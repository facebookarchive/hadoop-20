package org.apache.hadoop.hdfs;

import java.io.EOFException;
import java.io.IOException;
import java.net.URI;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.protocol.AlreadyBeingCreatedException;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.protocol.FSConstants.DatanodeReportType;
import org.apache.hadoop.hdfs.protocol.FSConstants.SafeModeAction;
import org.apache.hadoop.hdfs.protocol.FSConstants.UpgradeAction;
import org.apache.hadoop.hdfs.server.common.UpgradeStatusReport;
import org.apache.hadoop.ipc.ProtocolSignature;
import org.apache.hadoop.ipc.RPC.VersionIncompatible;
import org.apache.hadoop.security.AccessControlException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.Stat;

public class DistributedAvatarFileSystem extends DistributedFileSystem {

  AvatarZooKeeperClient zk;
  /*
   * ReadLock is acquired by the clients performing operations WriteLock is
   * acquired when we need to failover and modify the proxy. Read and write
   * because of read and write access to the namenode object.
   */
  ReentrantReadWriteLock fsLock = new ReentrantReadWriteLock(true);
  /**
   *  The canonical URI representing the cluster we are connecting to
   *  dfs1.data.xxx.com:9000 for example
   */
  URI logicalName;
  /**
   * The full address of the node in ZooKeeper
   */
  String zNode;
  Stat activeStat = new Stat();

  Configuration conf;
  // Wrapper for NameNodeProtocol that handles failover
  FailoverClientProtocol failoverClient;
  // Should DAFS retry write operations on failures or not
  boolean alwaysRetryWrites;
  // Indicates whether subscription model is used for ZK communication 
  boolean watchZK;

  volatile boolean shutdown = false;
  // indicates that the DFS is used instead of DAFS
  volatile boolean fallback = false;

  // We need to keep track of the FS object we used for failover
  DistributedFileSystem failoverFS;
  // Will try for two minutes checking with ZK every 15 seconds
  // to see if the failover has happened in pull case
  // and just wait for two minutes in watch case
  public static final int FAILOVER_CHECK_PERIOD = 15000;
  public static final int FAILOVER_RETIES = 8;
  // Tolerate up to 5 retries connecting to the NameNode
  private static final int FAILURE_RETRY = 5;

  /**
   * HA FileSystem initialization
   */

  @Override
  public URI getUri() {
    return this.logicalName;
  }

  public void initialize(URI name, Configuration conf) throws IOException {
    // The list of ZK servers
    String connection = conf.get("fs.ha.zookeeper.quorum");
    // The timeout to ZK server
    int zkTimeout = conf.getInt("fs.ha.zookeeper.timeout", 3000);
    /*
     * If true clients holds a watch on the znode and acts on events If false -
     * failover is pull based. Client will call zookeeper exists()
     */
    watchZK = conf.getBoolean("fs.ha.zookeeper.watch", false);
    /*
     * If false - on Mutable call to the namenode we fail If true we try to make
     * the call go through by resolving conflicts
     */
    alwaysRetryWrites = conf.getBoolean("fs.ha.retrywrites", false);
    // The actual name of the filesystem e.g. dfs.data.xxx.com:9000
    this.logicalName = name;
    this.conf = conf;
    // Create AvatarZooKeeperClient
    Watcher watcher = null;
    if (watchZK) {
      watcher = new ZooKeeperFSWatcher();
    }
    zk = new AvatarZooKeeperClient(connection, zkTimeout, watchZK, watcher);
    
    String fsAddress = name.getAuthority();
    /*
     * ZNode address is formed by the logical name of the filesystem:
     * dfs.data.xxx.com:9000 will be represented by zNode
     * /dfs/data/xxx/com/9000 in ZooKeeper
     */
    zNode = "/" + fsAddress.replaceAll("[.:]", "/");

    initUnderlyingFileSystem(false);
  }

  private boolean initUnderlyingFileSystem(boolean failover) throws IOException {
    try {
      byte[] addrBytes = zk.getNodeData(zNode, activeStat);

      String addrString = new String(addrBytes, "UTF-8");

      String fsHost = addrString.substring(0, addrString.indexOf(":"));
      int port = Integer.parseInt(addrString
          .substring(addrString.indexOf(":") + 1));
      URI realName = new URI(logicalName.getScheme(),
          logicalName.getUserInfo(), fsHost, port, logicalName.getPath(),
          logicalName.getQuery(), logicalName.getFragment());

      if (failover) {
        if (failoverFS != null) {
          failoverFS.close();
        }
        failoverFS = new DistributedFileSystem();
        failoverFS.initialize(realName, conf);

        failoverClient.newNameNode(failoverFS.dfs.namenode);
      } else {
        super.initialize(realName, conf);
        failoverClient = new FailoverClientProtocol(this.dfs.namenode);
        this.dfs.namenode = failoverClient;
      }
      LOG.info("Initialized new filesystem pointing to " + this.getUri() + " with the actual address " + addrString);
    } catch (Exception ex) {
      if (failover) {
        // Succeed or die trying
        // Next thread will try to failover again
        failoverFS = null;
        return false;
      } else {
        LOG.error("Ecxeption initializing DAFS. " +
      		"Falling back to using DFS instead", ex);
        fallback = true;
        super.initialize(logicalName, conf);
      }
    }
    return true;
  }

  private class FailoverClientProtocol implements ClientProtocol {

    ClientProtocol namenode;

    public FailoverClientProtocol(ClientProtocol namenode) {
      this.namenode = namenode;
    }

    public synchronized void nameNodeDown() {
      namenode = null;
    }

    public synchronized void newNameNode(ClientProtocol namenode) {
      this.namenode = namenode;
    }

    public synchronized boolean isDown() {
      return this.namenode == null;
    }

    @Override
    public int getDataTransferProtocolVersion() throws IOException {
      return (new ImmutableFSCaller<Integer>() {
        @Override
        Integer call() throws IOException {
          return namenode.getDataTransferProtocolVersion();
        }    
      }).callFS();
    }

    @Override
    public void abandonBlock(final Block b, final String src,
        final String holder) throws IOException {
      (new MutableFSCaller<Boolean>() {

        @Override
        Boolean call(int retry) throws IOException {
          namenode.abandonBlock(b, src, holder);
          return true;
        }

      }).callFS();
    }

    @Override
    public void abandonFile(final String src, final String holder) 
        throws IOException {
      (new MutableFSCaller<Boolean>() {

        @Override
        Boolean call(int retry) throws IOException {
          namenode.abandonFile(src, holder);
          return true;
        }

      }).callFS();
    }

    @Override
    public LocatedBlock addBlock(final String src, final String clientName,
        final DatanodeInfo[] excludedNodes) throws IOException {
      return (new MutableFSCaller<LocatedBlock>() {
        @Override
        LocatedBlock call(int retries) throws IOException {
          if (retries > 0) {
            FileStatus info = namenode.getFileInfo(src);
            if (info != null) {
              LocatedBlocks blocks = namenode.getBlockLocations(src, 0, info
                  .getLen());
              LocatedBlock last = blocks.get(blocks.locatedBlockCount() - 1);
              if (last.getBlockSize() == 0) {
                // This one has not been written to
                namenode.abandonBlock(last.getBlock(), src, clientName);
              }
            }
          }
          return namenode.addBlock(src, clientName, excludedNodes);
        }

      }).callFS();
    }

    @Override
    public LocatedBlock addBlock(final String src, final String clientName)
        throws IOException {
      return (new MutableFSCaller<LocatedBlock>() {
        @Override
        LocatedBlock call(int retries) throws IOException {
          if (retries > 0) {
            FileStatus info = namenode.getFileInfo(src);
            if (info != null) {
              LocatedBlocks blocks = namenode.getBlockLocations(src, 0, info
                  .getLen());
              LocatedBlock last = blocks.get(blocks.locatedBlockCount() - 1);
              if (last.getBlockSize() == 0) {
                // This one has not been written to
                namenode.abandonBlock(last.getBlock(), src, clientName);
              }
            }
          }
          return namenode.addBlock(src, clientName);
        }

      }).callFS();
    }

    @Override
    public LocatedBlock append(final String src, final String clientName)
        throws IOException {
      return (new MutableFSCaller<LocatedBlock>() {
        @Override
        LocatedBlock call(int retries) throws IOException {
          if (retries > 0) {
            namenode.complete(src, clientName);
          }
          return namenode.append(src, clientName);
        }

      }).callFS();
    }

    @Override
    public boolean complete(final String src, final String clientName)
        throws IOException {
      // Treating this as Immutable even though it changes metadata
      // but the complete called on the file should result in completed file
      return (new MutableFSCaller<Boolean>() {
        Boolean call(int r) throws IOException {
          if (r > 0) {
            try {
              return namenode.complete(src, clientName);
            } catch (IOException ex) {
              if (namenode.getFileInfo(src) != null) {
                // This might mean that we closed that file
                // which is why namenode can no longer find it
                // in the list of UnderConstruction
                if (ex.getMessage()
                    .contains("Could not complete write to file")) {
                  // We guess that we closed this file before because of the
                  // nature of exception
                  return true;
                }
              }
              throw ex;
            }
          }
          return namenode.complete(src, clientName);
        }
      }).callFS();
    }

    @Override
    public void create(final String src, final FsPermission masked,
        final String clientName, final boolean overwrite,
        final short replication, final long blockSize) throws IOException {
      (new MutableFSCaller<Boolean>() {
        @Override
        Boolean call(int retries) throws IOException {
          if (retries > 0) {
            // This I am not sure about, because of lease holder I can tell if
            // it
            // was me or not with a high level of certainty
            FileStatus stat = namenode.getFileInfo(src);
            if (stat != null && !overwrite) {
              /*
               * Since the file exists already we need to perform a number of
               * checks to see if it was created by us before the failover
               */

              if (stat.getBlockSize() == blockSize
                  && stat.getReplication() == replication && stat.getLen() == 0
                  && stat.getPermission().equals(masked)) {
                // The file has not been written to and it looks exactly like
                // the file we were trying to create. Last check:
                // call create again and then parse the exception.
                // Two cases:
                // it was the same client who created the old file
                // or it was created by someone else - fail
                try {
                  namenode.create(src, masked, clientName, overwrite,
                      replication, blockSize);
                } catch (AlreadyBeingCreatedException aex) {
                  if (aex.getMessage().contains(
                      "current leaseholder is trying to recreate file")) {
                    namenode.delete(src, false);
                  } else {
                    throw aex;
                  }
                }
              }
            }
          }
          namenode.create(src, masked, clientName, overwrite, 
               replication, blockSize);
          return true;
        }
      }).callFS();
    }



    @Override
    public void create(final String src, final FsPermission masked,
        final String clientName, final boolean overwrite,
        final boolean createParent,
        final short replication, final long blockSize) throws IOException {
      (new MutableFSCaller<Boolean>() {
        @Override
        Boolean call(int retries) throws IOException {
          if (retries > 0) {
            // This I am not sure about, because of lease holder I can tell if
            // it
            // was me or not with a high level of certainty
            FileStatus stat = namenode.getFileInfo(src);
            if (stat != null && !overwrite) {
              /*
               * Since the file exists already we need to perform a number of
               * checks to see if it was created by us before the failover
               */

              if (stat.getBlockSize() == blockSize
                  && stat.getReplication() == replication && stat.getLen() == 0
                  && stat.getPermission().equals(masked)) {
                // The file has not been written to and it looks exactly like
                // the file we were trying to create. Last check:
                // call create again and then parse the exception.
                // Two cases:
                // it was the same client who created the old file
                // or it was created by someone else - fail
                try {
                  namenode.create(src, masked, clientName, overwrite,
                      createParent, replication, blockSize);
                } catch (AlreadyBeingCreatedException aex) {
                  if (aex.getMessage().contains(
                      "current leaseholder is trying to recreate file")) {
                    namenode.delete(src, false);
                  } else {
                    throw aex;
                  }
                }
              }
            }
          }
          namenode.create(src, masked, clientName, overwrite, 
               createParent, replication, blockSize);
          return true;
        }
      }).callFS();
    }

    @Override
    public boolean delete(final String src, final boolean recursive)
        throws IOException {
      return (new MutableFSCaller<Boolean>() {
        @Override
        Boolean call(int retries) throws IOException {
          if (retries > 0) {
            namenode.delete(src, recursive);
            return true;
          }
          return namenode.delete(src, recursive);
        }

      }).callFS();
    }

    @Override
    public boolean delete(final String src) throws IOException {
      return (new MutableFSCaller<Boolean>() {
        @Override
        Boolean call(int retries) throws IOException {
          if (retries > 0) {
            namenode.delete(src);
            return true;
          }
          return namenode.delete(src);
        }

      }).callFS();
    }

    @Override
    public UpgradeStatusReport distributedUpgradeProgress(
        final UpgradeAction action) throws IOException {
      return (new MutableFSCaller<UpgradeStatusReport>() {
        UpgradeStatusReport call(int r) throws IOException {
          return namenode.distributedUpgradeProgress(action);
        }
      }).callFS();
    }

    @Override
    public void finalizeUpgrade() throws IOException {
      (new MutableFSCaller<Boolean>() {
        Boolean call(int retry) throws IOException {
          namenode.finalizeUpgrade();
          return true;
        }
      }).callFS();

    }

    @Override
    public void fsync(final String src, final String client) throws IOException {
      // TODO Is it Mutable or Immutable
      (new ImmutableFSCaller<Boolean>() {

        @Override
        Boolean call() throws IOException {
          namenode.fsync(src, client);
          return true;
        }

      }).callFS();
    }

    @Override
    public LocatedBlocks getBlockLocations(final String src, final long offset,
        final long length) throws IOException {
      // TODO Make it cache values as per Dhruba's suggestion
      return (new ImmutableFSCaller<LocatedBlocks>() {
        LocatedBlocks call() throws IOException {
          return namenode.getBlockLocations(src, offset, length);
        }
      }).callFS();
    }

    @Override
    public ContentSummary getContentSummary(final String src) throws IOException {
      return (new ImmutableFSCaller<ContentSummary>() {
        ContentSummary call() throws IOException {
          return namenode.getContentSummary(src);
        }
      }).callFS();
    }

    @Override
    public FileStatus[] getCorruptFiles() throws AccessControlException,
        IOException {
      return (new ImmutableFSCaller<FileStatus[]>() {
        FileStatus[] call() throws IOException {
          return namenode.getCorruptFiles();
        }
      }).callFS();
    }

    @Override
    public DatanodeInfo[] getDatanodeReport(final DatanodeReportType type)
        throws IOException {
      return (new ImmutableFSCaller<DatanodeInfo[]>() {
        DatanodeInfo[] call() throws IOException {
          return namenode.getDatanodeReport(type);
        }
      }).callFS();
    }

    @Override
    public FileStatus getFileInfo(final String src) throws IOException {
      return (new ImmutableFSCaller<FileStatus>() {
        FileStatus call() throws IOException {
          return namenode.getFileInfo(src);
        }
      }).callFS();
    }

    @Override
    public FileStatus[] getListing(final String src) throws IOException {
      return (new ImmutableFSCaller<FileStatus[]>() {
        FileStatus[] call() throws IOException {
          return namenode.getListing(src);
        }
      }).callFS();
    }

    @Override
    public long getPreferredBlockSize(final String filename) throws IOException {
      return (new ImmutableFSCaller<Long>() {
        Long call() throws IOException {
          return namenode.getPreferredBlockSize(filename);
        }
      }).callFS();
    }

    @Override
    public long[] getStats() throws IOException {
      return (new ImmutableFSCaller<long[]>() {
        long[] call() throws IOException {
          return namenode.getStats();
        }
      }).callFS();
    }

    @Override
    public void metaSave(final String filename) throws IOException {
      (new ImmutableFSCaller<Boolean>() {
        Boolean call() throws IOException {
          namenode.metaSave(filename);
          return true;
        }
      }).callFS();
    }

    @Override
    public boolean mkdirs(final String src, final FsPermission masked)
        throws IOException {
      return (new ImmutableFSCaller<Boolean>() {
        Boolean call() throws IOException {
          return namenode.mkdirs(src, masked);
        }
      }).callFS();
    }

    @Override
    public void refreshNodes() throws IOException {
      (new ImmutableFSCaller<Boolean>() {
        Boolean call() throws IOException {
          namenode.refreshNodes();
          return true;
        }
      }).callFS();
    }

    @Override
    public boolean rename(final String src, final String dst) throws IOException {
      return (new MutableFSCaller<Boolean>() {

        @Override
        Boolean call(int retries) throws IOException {
          if (retries > 0) {
            /*
             * Because of the organization of the code in the NameNode if the
             * source is still there then the rename did not happen
             * 
             * If it doesn't exist then if the rename happened, the dst exists
             * otherwise rename did not happen because there was an error return
             * false
             * 
             * This is of course a subject to races between clients but with
             * certain assumptions about a system we can make the call succeed
             * on failover
             */
            if (namenode.getFileInfo(src) != null)
              return namenode.rename(src, dst);
            return namenode.getFileInfo(dst) != null;
          }
          return namenode.rename(src, dst);
        }

      }).callFS();
    }

    @Override
    public void renewLease(final String clientName) throws IOException {
      // Treating this as immutable
      (new ImmutableFSCaller<Boolean>() {
        Boolean call() throws IOException {
          namenode.renewLease(clientName);
          return true;
        }
      }).callFS();
    }

    @Override
    public void reportBadBlocks(final LocatedBlock[] blocks) throws IOException {
      // TODO this might be a good place to send it to both namenodes
      (new MutableFSCaller<Boolean>() {
        Boolean call(int r) throws IOException {
          namenode.reportBadBlocks(blocks);
          return true;
        }
      }).callFS();
    }

    @Override
    public void saveNamespace() throws IOException {
      (new MutableFSCaller<Boolean>() {
        Boolean call(int r) throws IOException {
          namenode.saveNamespace();
          return true;
        }
      }).callFS();
    }

    @Override
    public void saveNamespace(final boolean force) throws IOException {
      (new MutableFSCaller<Boolean>() {
        Boolean call(int r) throws IOException {
          namenode.saveNamespace(force);
          return true;
        }
      }).callFS();
    }

    @Override
    public void setOwner(final String src, final String username,
        final String groupname) throws IOException {
      (new MutableFSCaller<Boolean>() {
        Boolean call(int r) throws IOException {
          namenode.setOwner(src, username, groupname);
          return true;
        }
      }).callFS();
    }

    @Override
    public void setPermission(final String src, final FsPermission permission)
        throws IOException {
      (new MutableFSCaller<Boolean>() {
        Boolean call(int r) throws IOException {
          namenode.setPermission(src, permission);
          return true;
        }
      }).callFS();
    }

    @Override
    public void setQuota(final String path, final long namespaceQuota,
        final long diskspaceQuota) throws IOException {
      (new MutableFSCaller<Boolean>() {
        Boolean call(int retry) throws IOException {
          namenode.setQuota(path, namespaceQuota, diskspaceQuota);
          return true;
        }
      }).callFS();
    }

    @Override
    public boolean setReplication(final String src, final short replication)
        throws IOException {
      return (new MutableFSCaller<Boolean>() {
        Boolean call(int retry) throws IOException {
          return namenode.setReplication(src, replication);
        }
      }).callFS();
    }

    @Override
    public boolean setSafeMode(final SafeModeAction action) throws IOException {
      return (new MutableFSCaller<Boolean>() {
        Boolean call(int r) throws IOException {
          return namenode.setSafeMode(action);
        }
      }).callFS();
    }

    @Override
    public void setTimes(final String src, final long mtime, final long atime)
        throws IOException {
      (new MutableFSCaller<Boolean>() {

        @Override
        Boolean call(int retry) throws IOException {
          namenode.setTimes(src, mtime, atime);
          return true;
        }

      }).callFS();
    }

    @Override
    public long getProtocolVersion(final String protocol,
        final long clientVersion) throws VersionIncompatible, IOException {
      return (new ImmutableFSCaller<Long>() {

        @Override
        Long call() throws IOException {
          return namenode.getProtocolVersion(protocol, clientVersion);
        }

      }).callFS();
    }

    @Override
    public ProtocolSignature getProtocolSignature(final String protocol,
        final long clientVersion, final int clientMethodsHash) throws IOException {
      return (new ImmutableFSCaller<ProtocolSignature>() {

        @Override
        ProtocolSignature call() throws IOException {
          return namenode.getProtocolSignature(
              protocol, clientVersion, clientMethodsHash);
        }

      }).callFS();
    }
  
    @Override
    public void recoverLease(final String src, final String clientName)
    throws IOException {
      // Treating this as immutable
      (new ImmutableFSCaller<Boolean>() {
        Boolean call() throws IOException {
          namenode.recoverLease(src, clientName);
          return true;
        }
      }).callFS(); 
    }

    @Override
    public boolean closeRecoverLease(final String src, final String clientName)
       throws IOException {
      // Treating this as immutable
      return (new ImmutableFSCaller<Boolean>() {
        Boolean call() throws IOException {
          return namenode.closeRecoverLease(src, clientName);
        }
      }).callFS(); 
    }

  }
  private boolean shouldHandleException(IOException ex) {
    if (ex.getMessage().contains("java.io.EOFException")) {
      return true;
    }
    return ex.getMessage().toLowerCase().contains("connection");
  }

  private void handleFailure(IOException ex, int failures) throws IOException {
    LOG.debug("Handle failure", ex);
    // Check if the exception was thrown by the network stack
    if (shutdown || !shouldHandleException(ex)) {
      throw ex;
    }

    if (failures > FAILURE_RETRY) {
      throw ex;
    }
    try {
      // This might've happened because we are failing over
      if (!watchZK) {
        LOG.debug("Not watching ZK, so checking explicitly");
        // Check with zookeeper
        fsLock.readLock().unlock();
        fsLock.writeLock().lock();
        try {
          Stat stat = null;
          if ((stat = zk.getNodeStats(zNode)) == null) {
            failoverClient.nameNodeDown();
            return;
          }
          LOG.debug("File is in ZK");
          LOG.debug("Checking mod time: " + stat.getMtime() + " > " + activeStat.getMtime());
          if (stat.getMtime() > activeStat.getMtime()) {
            // Failover has happened happened already
            failoverClient.nameNodeDown();
            return;
          }
        } catch (Exception x) {
          // just swallow for now
          LOG.error(x);
        } finally {
          fsLock.writeLock().unlock();
          fsLock.readLock().lock();
        }
      }
      Thread.sleep(1000);
    } catch (InterruptedException iex) {
      LOG.error("Interrupted while waiting for a failover", iex);
      Thread.currentThread().interrupt();
    }

  }

  @Override
  public void close() throws IOException {
    shutdown = true;
    if (fallback) {
      // no need to lock resources
      super.close();
      return;
    }
    readLock();
    try {
      super.close();
      if (failoverFS != null) {
        failoverFS.close();
      }
      try {
        zk.shutdown();
      } catch (InterruptedException e) {
        LOG.error("Error shutting down ZooKeeper client", e);
      }
    } finally {
      readUnlock();
    }
  }

  /**
   * ZooKeeper communication
   */

  private class ZooKeeperFSWatcher implements Watcher {
    @Override
    public void process(WatchedEvent event) {
      if (Event.EventType.NodeCreated == event.getType()
          && event.getPath().equals(zNode)) {
        fsLock.writeLock().lock();
        try {
          initUnderlyingFileSystem(true);
        } catch (IOException ex) {
          LOG.error("Error initializing fs", ex);
        } finally {
          fsLock.writeLock().unlock();
        }
        return;
      }
      if (Event.EventType.NodeDeleted == event.getType()
          && event.getPath().equals(zNode)) {
        fsLock.writeLock().lock();
        failoverClient.nameNodeDown();
        try {
          // Subscribe for changes
          if (zk.getNodeStats(zNode) != null) {
            // Failover already happened - initialize
            initUnderlyingFileSystem(true);
          }
        } catch (Exception iex) {
          LOG.error(iex);
        } finally {
          fsLock.writeLock().unlock();
        }
      }
    }
  }

  private void readUnlock() {
    fsLock.readLock().unlock();
  }

  private void readLock() throws IOException {
    for (int i = 0; i < FAILOVER_RETIES; i++) {
      fsLock.readLock().lock();

      if (failoverClient.isDown()) {
        // This means the underlying filesystem is not initialized
        // and there is no way to make a call
        // Failover might be in progress, so wait for it
        // Since we do not want to miss the notification on failoverMonitor
        fsLock.readLock().unlock();
        try {
          boolean failedOver = false;
          fsLock.writeLock().lock();
          if (!watchZK && failoverClient.isDown()) {
            LOG.debug("No Watch ZK Failover");
            // We are in pull failover mode where clients are asking ZK
            // if the failover is over instead of ZK telling watchers
            // however another thread in this FS Instance could've done
            // the failover for us.
            try {
              if (zk.getNodeStats(zNode) != null) {
                failedOver = initUnderlyingFileSystem(true);
                // Notify other threads waiting on this FS instance
              }
            } catch (Exception ex) {
              // Just swallow exception since we are retrying in any event
            }
          }
          fsLock.writeLock().unlock();
          if (!failedOver)
            Thread.sleep(FAILOVER_CHECK_PERIOD);
        } catch (InterruptedException ex) {
          LOG.error("Got interrupted waiting for failover", ex);
          Thread.currentThread().interrupt();
        }

      } else {
        // The client is up and we are holding a readlock.
        return;
      }
    }
    // We retried FAILOVER_RETIES times with no luck - fail the call
    throw new IOException("No FileSystem for " + logicalName);
  }

  /**
   * File System implementation
   */


  private abstract class ImmutableFSCaller<T> {
    
    abstract T call() throws IOException;
    
    public T callFS() throws IOException {
      int failures = 0;
      while (true) {
        readLock();
        try {
          return this.call();
        } catch (IOException ex) {
          handleFailure(ex, failures);
          failures++;
        } finally {
          readUnlock();
        }
      }
    }
  }

  private abstract class MutableFSCaller<T> {

    abstract T call(int retry) throws IOException;
    
    public T callFS() throws IOException {
      int retries = 0;
      while (true) {
        readLock();
        try {
          return this.call(retries);
        } catch (IOException ex) {
          if (!alwaysRetryWrites)
            throw ex;
          handleFailure(ex, retries);
          retries++;
        } finally {
          readUnlock();
        }
      }
    }
  }

}
