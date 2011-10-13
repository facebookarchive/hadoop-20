package org.apache.hadoop.hdfs;

import java.io.EOFException;
import java.io.IOException;
import java.io.FileNotFoundException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.protocol.AlreadyBeingCreatedException;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.protocol.CorruptFileBlocks;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.DirectoryListing;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.protocol.LocatedDirectoryListing;
import org.apache.hadoop.hdfs.protocol.VersionedLocatedBlock;
import org.apache.hadoop.hdfs.protocol.VersionedLocatedBlocks;
import org.apache.hadoop.hdfs.protocol.FSConstants.DatanodeReportType;
import org.apache.hadoop.hdfs.protocol.FSConstants.SafeModeAction;
import org.apache.hadoop.hdfs.protocol.FSConstants.UpgradeAction;
import org.apache.hadoop.hdfs.server.common.UpgradeStatusReport;
import org.apache.hadoop.ipc.ProtocolSignature;
import org.apache.hadoop.ipc.RPC.VersionIncompatible;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.util.StringUtils;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.Stat;

public class DistributedAvatarFileSystem extends DistributedFileSystem {

  CachingAvatarZooKeeperClient zk;
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
  long lastPrimaryUpdate = 0;

  Configuration conf;
  // Wrapper for NameNodeProtocol that handles failover
  FailoverClientProtocol failoverClient;
  // Should DAFS retry write operations on failures or not
  boolean alwaysRetryWrites;
  // Indicates whether subscription model is used for ZK communication 
  boolean watchZK;
  boolean cacheZKData = false;
  // number of milliseconds to wait between successive attempts
  // to initialize standbyFS
  long standbyFSInitInterval;

  // time at which we last attempted to initialize standbyFS
  long lastStandbyFSInit = 0L;

  // number of milliseconds before we should check if a failover has occurred
  // (used when making calls to the standby avatar)
  long standbyFSCheckInterval;

  // time at which last check for failover was performed
  long lastStandbyFSCheck = 0L;

  // number of requests to standbyFS between checks for failover
  int standbyFSCheckRequestInterval;

  // number of requests to standbyFS since last last failover check
  AtomicInteger standbyFSCheckRequestCount = new AtomicInteger(0);

  volatile boolean shutdown = false;
  // indicates that the DFS is used instead of DAFS
  volatile boolean fallback = false;

  // We need to keep track of the FS object we used for failover
  DistributedFileSystem failoverFS;

  // a filesystem object that points to the standby avatar
  StandbyFS standbyFS = null;
  // URI of primary and standby avatar
  URI primaryURI;
  URI standbyURI;

  // Will try for two minutes checking with ZK every 15 seconds
  // to see if the failover has happened in pull case
  // and just wait for two minutes in watch case
  public static final int FAILOVER_CHECK_PERIOD = 15000;
  public static final int FAILOVER_RETRIES = 8;
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
    zk = new CachingAvatarZooKeeperClient(conf, watcher);
    cacheZKData = zk.isCacheEnabled();
    // default interval between standbyFS initialization attempts is 10 mins
    standbyFSInitInterval = conf.getLong("fs.avatar.standbyfs.initinterval",
                                         10 * 60 * 1000);

    // default interval between failover checks is 5 min
    standbyFSCheckInterval = conf.getLong("fs.avatar.standbyfs.checkinterval",
                                         5 * 60 * 1000);

    // default interval between failover checks is 5000 requests
    standbyFSCheckRequestInterval =
      conf.getInt("fs.avatar.standbyfs.checkrequests", 5000);

    initUnderlyingFileSystem(false);
  }

  private URI addrToURI(String addrString) throws URISyntaxException {
    if (addrString.startsWith(logicalName.getScheme())) {
      return new URI(addrString);
    } else {
      if (addrString.indexOf(":") == -1) {
        // This is not a valid addr string
        return null;
      }
      String fsHost = addrString.substring(0, addrString.indexOf(":"));
      int port = Integer.parseInt(addrString
                                  .substring(addrString.indexOf(":") + 1));
      return new URI(logicalName.getScheme(),
                     logicalName.getUserInfo(),
                     fsHost, port, logicalName.getPath(),
                     logicalName.getQuery(), logicalName.getFragment());
    }
  }

  private class StandbyFS extends DistributedFileSystem {
    @Override
    public URI getUri() {
      return DistributedAvatarFileSystem.this.logicalName;
    }
  }

  /**
   * Try to initialize standbyFS. Must hold writelock to call this method.
   */
  private void initStandbyFS() {
    lastStandbyFSInit = System.currentTimeMillis();
    try {
      if (standbyFS != null) {
        standbyFS.close();
      }

      LOG.info("DAFS initializing standbyFS");
      LOG.info("DAFS primary=" + primaryURI.toString() +
               " standby=" + standbyURI.toString());
      standbyFS = new StandbyFS();
      standbyFS.initialize(standbyURI, conf);

    } catch (Exception e) {
      LOG.info("DAFS cannot initialize standbyFS: " +
               StringUtils.stringifyException(e));
      standbyFS = null;
    }
  }

  private boolean initUnderlyingFileSystem(boolean failover) throws IOException {
    try {
      boolean firstAttempt = true;

      while (true) {
        Stat stat = new Stat();
        String primaryAddr = zk.getPrimaryAvatarAddress(logicalName, stat,
            true, firstAttempt);
        lastPrimaryUpdate = stat.getMtime();
        primaryURI = addrToURI(primaryAddr);

        URI uri0 = addrToURI(conf.get("fs.default.name0", ""));
        // if the uri is null the configuration is broken.
        // no need to try to initialize a standby
        if (uri0 != null) {

          // the standby avatar is whichever one is not the primary
          // note that standbyFS connects to the datanode port of the standby
          // avatar
          // since the client port is not available while in safe mode
          if (uri0.equals(primaryURI)) {
            standbyURI = addrToURI(conf.get("dfs.namenode.dn-address1", ""));
          } else {
            standbyURI = addrToURI(conf.get("dfs.namenode.dn-address0", ""));
          }
          if (standbyURI != null) {
            initStandbyFS();
          } else {
            LOG.warn("Not initializing standby filesystem because the needed " +
                "configuration parameters " +
                "dfs.namenode.dn-address{0|1} are missing");
          }
        } else {
          LOG.warn("Not initializing standby filesystem because the needed "
              + "configuration parameters fs.default.name{0|1} are missing.");
        }

        try {
          if (failover) {
            if (failoverFS != null) {
              failoverFS.close();
            }
            failoverFS = new DistributedFileSystem();
            failoverFS.initialize(primaryURI, conf);

            failoverClient.newNameNode(failoverFS.dfs.namenode);

          } else {
            super.initialize(primaryURI, conf);
            failoverClient = new FailoverClientProtocol(this.dfs.namenode);
            this.dfs.namenode = failoverClient;
          }
        } catch (IOException ex) {
          if (firstAttempt && cacheZKData) {
            firstAttempt = false;
            continue;
          } else {
            throw ex;
          }
        }
        LOG.info("Initialized new filesystem pointing to " + this.getUri()
            + " with the actual address " + primaryAddr);
        return true;
      }
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
    public void abandonFile(final String src,
        final String holder) throws IOException {
      (new MutableFSCaller<Boolean>() {

        @Override
        Boolean call(int retry) throws IOException {
          namenode.abandonFile(src, holder);
          return true;
        }

      }).callFS();
    }

    @Override
    public LocatedDirectoryListing getLocatedPartialListing(final String src,
        final byte[] startAfter) throws IOException {
      return (new ImmutableFSCaller<LocatedDirectoryListing>() {

        @Override
        LocatedDirectoryListing call() throws IOException {
          return namenode.getLocatedPartialListing(src, startAfter);
        }

      }).callFS();
    }

    public LocatedBlock addBlock(final String src, final String clientName,
        final DatanodeInfo[] excludedNodes, final DatanodeInfo[] favoredNodes,
        final boolean wait) throws IOException {
      return (new MutableFSCaller<LocatedBlock>() {
        @Override
        LocatedBlock call(int retries) throws IOException {
          if (retries > 0) {
            FileStatus info = namenode.getFileInfo(src);
            if (info != null) {
              LocatedBlocks blocks = namenode.getBlockLocations(src, 0, info
                .getLen());
              if (blocks.locatedBlockCount() > 0 ) {
                LocatedBlock last = blocks.get(blocks.locatedBlockCount() - 1);
                if (last.getBlockSize() == 0) {
                  // This one has not been written to
                  namenode.abandonBlock(last.getBlock(), src, clientName);
                }
              }
            }
          }
          return namenode.addBlock(src, clientName, excludedNodes, favoredNodes,
            wait);
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
              // If atleast one block exists.
              if (blocks.locatedBlockCount() > 0) {
                LocatedBlock last = blocks.get(blocks.locatedBlockCount() - 1);
                if (last.getBlockSize() == 0) {
                  // This one has not been written to
                  namenode.abandonBlock(last.getBlock(), src, clientName);
                }
              }
            }
          }
          return namenode.addBlock(src, clientName, excludedNodes);
        }

      }).callFS();
    }

    @Override
    public VersionedLocatedBlock addBlockAndFetchVersion(
        final String src, final String clientName,
        final DatanodeInfo[] excludedNodes) throws IOException {
      return (new MutableFSCaller<VersionedLocatedBlock>() {
        @Override
        VersionedLocatedBlock call(int retries) throws IOException {
          if (retries > 0) {
            FileStatus info = namenode.getFileInfo(src);
            if (info != null) {
              LocatedBlocks blocks = namenode.getBlockLocations(src, 0, info
                  .getLen());
              // If atleast one block exists.
              if (blocks.locatedBlockCount() > 0) {
                LocatedBlock last = blocks.get(blocks.locatedBlockCount() - 1);
                if (last.getBlockSize() == 0) {
                  // This one has not been written to
                  namenode.abandonBlock(last.getBlock(), src, clientName);
                }
              }
            }
          }
          return namenode.addBlockAndFetchVersion(src, clientName, excludedNodes);
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
              // If atleast one block exists.
              if (blocks.locatedBlockCount() > 0) {
                LocatedBlock last = blocks.get(blocks.locatedBlockCount() - 1);
                if (last.getBlockSize() == 0) {
                  // This one has not been written to
                  namenode.abandonBlock(last.getBlock(), src, clientName);
                }
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
          namenode.create(src, masked, clientName, overwrite, createParent, replication,
              blockSize);
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
    public VersionedLocatedBlocks open(final String src, final long offset,
        final long length) throws IOException {
      // TODO Make it cache values as per Dhruba's suggestion
      return (new ImmutableFSCaller<VersionedLocatedBlocks>() {
        VersionedLocatedBlocks call() throws IOException {
          return namenode.open(src, offset, length);
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

    @Deprecated @Override
    public FileStatus[] getCorruptFiles() throws AccessControlException,
        IOException {
      return (new ImmutableFSCaller<FileStatus[]>() {
        FileStatus[] call() throws IOException {
          return namenode.getCorruptFiles();
        }
      }).callFS();
    }

    @Override
    public CorruptFileBlocks
      listCorruptFileBlocks(final String path, final String cookie)
      throws IOException {
      return (new ImmutableFSCaller<CorruptFileBlocks> () {
                CorruptFileBlocks call() 
                  throws IOException {
                  return namenode.listCorruptFileBlocks(path, cookie);
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
    public HdfsFileStatus getHdfsFileInfo(final String src) throws IOException {
      return (new ImmutableFSCaller<HdfsFileStatus>() {
        HdfsFileStatus call() throws IOException {
          return namenode.getHdfsFileInfo(src);
        }
      }).callFS();
    }

    @Override
    public HdfsFileStatus[] getHdfsListing(final String src) throws IOException {
      return (new ImmutableFSCaller<HdfsFileStatus[]>() {
        HdfsFileStatus[] call() throws IOException {
          return namenode.getHdfsListing(src);
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

    public DirectoryListing getPartialListing(final String src,
        final byte[] startAfter) throws IOException {
      return (new ImmutableFSCaller<DirectoryListing>() {
        DirectoryListing call() throws IOException {
          return namenode.getPartialListing(src, startAfter);
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
    public void saveNamespace(final boolean force, final boolean uncompressed) throws IOException {
      (new MutableFSCaller<Boolean>() {
        Boolean call(int r) throws IOException {
          namenode.saveNamespace(force, uncompressed);
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
    @Deprecated
    public void concat(final String trg, final String[] srcs
        ) throws IOException {
      concat(trg, srcs, true);
    }
    
    @Override
    public void concat(final String trg, final String[] srcs,
        final boolean restricted) throws IOException {
      (new MutableFSCaller<Boolean>() {
        Boolean call(int r) throws IOException {
          namenode.concat(trg, srcs, restricted);
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
          return namenode.closeRecoverLease(src, clientName, false);
        }
      }).callFS();
    }

    @Override
    public boolean closeRecoverLease(final String src, final String clientName,
                                     final boolean discardLastBlock)
       throws IOException {
      // Treating this as immutable
      return (new ImmutableFSCaller<Boolean>() {
        Boolean call() throws IOException {
          return namenode.closeRecoverLease(src, clientName, discardLastBlock);
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

  /**
   * @return true if a failover has happened, false otherwise
   * requires write lock
   */
  private boolean zkCheckFailover() {
    try {
      long registrationTime = zk.getPrimaryRegistrationTime(logicalName);
      LOG.debug("File is in ZK");
      LOG.debug("Checking mod time: " + registrationTime + 
                " > " + lastPrimaryUpdate);
      if (registrationTime > lastPrimaryUpdate) {
        // Failover has happened happened already
        failoverClient.nameNodeDown();
        return true;
      }
    } catch (Exception x) {
      // just swallow for now
      LOG.error(x);
    }
    return false;
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
        boolean failover = zkCheckFailover();
        fsLock.writeLock().unlock();
        fsLock.readLock().lock();
        if (failover) {
          return;
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
      if (standbyFS != null) {
        standbyFS.close();
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
      /**
       * This is completely inaccurate by now since we
       * switched from deletion and recreation of the node
       * to updating the node information.
       * I am commenting it out for now. Will revisit once we
       * decide to go to the watchers approach.
       */
//      if (Event.EventType.NodeCreated == event.getType()
//          && event.getPath().equals(zNode)) {
//        fsLock.writeLock().lock();
//        try {
//          initUnderlyingFileSystem(true);
//        } catch (IOException ex) {
//          LOG.error("Error initializing fs", ex);
//        } finally {
//          fsLock.writeLock().unlock();
//        }
//        return;
//      }
//      if (Event.EventType.NodeDeleted == event.getType()
//          && event.getPath().equals(zNode)) {
//        fsLock.writeLock().lock();
//        failoverClient.nameNodeDown();
//        try {
//          // Subscribe for changes
//          if (zk.getNodeStats(zNode) != null) {
//            // Failover already happened - initialize
//            initUnderlyingFileSystem(true);
//          }
//        } catch (Exception iex) {
//          LOG.error(iex);
//        } finally {
//          fsLock.writeLock().unlock();
//        }
//      }
    }
  }

  private void readUnlock() {
    fsLock.readLock().unlock();
  }

  private void readLock() throws IOException {
    for (int i = 0; i < FAILOVER_RETRIES; i++) {
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
              failedOver = initUnderlyingFileSystem(true);
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
    // We retried FAILOVER_RETRIES times with no luck - fail the call
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

  /**
   * Used to direct a call either to the standbyFS or to the underlying DFS.
   */
  private abstract class StandbyCaller<T> {
    abstract T call(DistributedFileSystem fs) throws IOException;

    private T callPrimary() throws IOException {
      return call(DistributedAvatarFileSystem.this);
    }

    private T callStandby() throws IOException {
      boolean primaryCalled = false;
      try {
        // grab the read lock but don't check for failover yet
        fsLock.readLock().lock();

        if (System.currentTimeMillis() >
            lastStandbyFSCheck + standbyFSCheckInterval ||
            standbyFSCheckRequestCount.get() >=
            standbyFSCheckRequestInterval) {
          // check if a failover has happened
          LOG.debug("DAFS checking for failover time=" +
                    System.currentTimeMillis() +
                    " last=" + lastStandbyFSCheck +
                    " t_interval=" + standbyFSCheckInterval +
                    " count=" + (standbyFSCheckRequestCount.get()) +
                    " r_interval=" + standbyFSCheckRequestInterval);

          // release read lock, grab write lock
          fsLock.readLock().unlock();
          fsLock.writeLock().lock();
          boolean failover = zkCheckFailover();
          if (failover) {
            LOG.info("DAFS failover has happened");
            failoverClient.nameNodeDown();
          } else {
            LOG.debug("DAFS failover has not happened");
          }
        
          standbyFSCheckRequestCount.set(0);
          lastStandbyFSCheck = System.currentTimeMillis();

          // release write lock
          fsLock.writeLock().unlock();

          // now check for failover
          readLock();
        } else if (standbyFS == null && (System.currentTimeMillis() >
                                         lastStandbyFSInit +
                                         standbyFSInitInterval)) {
          // try to initialize standbyFS

          // release read lock, grab write lock
          fsLock.readLock().unlock();
          fsLock.writeLock().lock();
          initStandbyFS();

          fsLock.writeLock().unlock();
          fsLock.readLock().lock();
        }

        standbyFSCheckRequestCount.incrementAndGet();

        if (standbyFS == null) {
          // if there is still no standbyFS, use the primary
          LOG.info("DAFS Standby avatar not available, using primary.");
          primaryCalled = true;
          fsLock.readLock().unlock();
          return callPrimary();
        }
        return call(standbyFS);
      } catch (FileNotFoundException fe) {
        throw fe;
      } catch (IOException ie) {
        if (primaryCalled) {
          throw ie;
        } else {
          LOG.error("DAFS Request to standby avatar failed, trying primary.\n" +
                    "Standby exception:\n" +
                    StringUtils.stringifyException(ie));
          primaryCalled = true;
          fsLock.readLock().unlock();
          return callPrimary();
        }
      } finally {
        if (!primaryCalled) {
          fsLock.readLock().unlock();
        }
      }
    }

    T callFS(boolean useStandby) throws IOException {
      if (!useStandby) {
        LOG.debug("DAFS using primary");
        return callPrimary();
      } else {
        LOG.debug("DAFS using standby");
        return callStandby();
      }
    }
  }

  /**
   * Return the stat information about a file.
   * @param f path
   * @param useStandby flag indicating whether to read from standby avatar
   * @throws FileNotFoundException if the file does not exist.
   */
  public FileStatus getFileStatus(final Path f, final boolean useStandby)
    throws IOException {
    return new StandbyCaller<FileStatus>() {
      @Override
      FileStatus call(DistributedFileSystem fs) throws IOException {
        return fs.getFileStatus(f);
      }
    }.callFS(useStandby);
  }

  /**
   * List files in a directory.
   * @param f path
   * @param useStandby flag indicating whether to read from standby avatar
   * @throws FileNotFoundException if the file does not exist.
   */
  public FileStatus[] listStatus(final Path f, final boolean useStandby)
    throws IOException {
    return new StandbyCaller<FileStatus[]>() {
      @Override
      FileStatus[] call(DistributedFileSystem fs) throws IOException {
        return fs.listStatus(f);
      }
    }.callFS(useStandby);
  }

  /**
   * {@inheritDoc}
   */
  public ContentSummary getContentSummary(final Path f,
                                          final boolean useStandby)
    throws IOException {
    return new StandbyCaller<ContentSummary>() {
      @Override
      ContentSummary call(DistributedFileSystem fs) throws IOException {
        return fs.getContentSummary(f);
      }
    }.callFS(useStandby);
  }

  /**
   * {@inheritDoc}
   *
   * This will only work if the standby avatar is
   * set up to populate its underreplicated
   * block queues while still in safe mode.
   */
  public RemoteIterator<Path> listCorruptFileBlocks(final Path path,
                                                    final boolean useStandby)
    throws IOException {
    return new StandbyCaller<RemoteIterator<Path>>() {
      @Override
      RemoteIterator<Path> call(DistributedFileSystem fs) throws IOException {
        return fs.listCorruptFileBlocks(path);
      }
    }.callFS(useStandby);
  }

  /**
   * Return statistics for each datanode.
   * @return array of data node reports
   * @throw IOException
   */
  public DatanodeInfo[] getDataNodeStats(final boolean useStandby)
    throws IOException {
    return new StandbyCaller<DatanodeInfo[]>() {
      @Override
      DatanodeInfo[] call(DistributedFileSystem fs) throws IOException {
        return fs.getDataNodeStats();
      }
    }.callFS(useStandby);
  }

}
