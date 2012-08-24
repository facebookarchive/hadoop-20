package org.apache.hadoop.hdfs;

import java.io.IOException;
import java.io.FileNotFoundException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.protocol.CorruptFileBlocks;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.ipc.VersionedProtocol;
import org.apache.hadoop.util.StringUtils;
import org.apache.zookeeper.data.Stat;

public class DistributedAvatarFileSystem extends DistributedFileSystem
    implements FailoverClient {

  static {
    Configuration.addDefaultResource("avatar-default.xml");
    Configuration.addDefaultResource("avatar-site.xml");
  }
  
  /**
   *  The canonical URI representing the cluster we are connecting to
   *  dfs1.data.xxx.com:9000 for example
   */
  URI logicalName;
  Configuration conf;
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

  FailoverClientProtocol failoverClient;
  FailoverClientHandler failoverHandler;

  /**
   * HA FileSystem initialization
   */

  @Override
  public URI getUri() {
    return this.logicalName;
  }

  public void initialize(URI name, Configuration conf) throws IOException {
    // The actual name of the filesystem e.g. dfs.data.xxx.com:9000
    this.logicalName = name;
    this.conf = conf;
    failoverHandler = new FailoverClientHandler(conf, logicalName, this);
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

  @Override
  public boolean tryFailover() throws IOException {
    return initUnderlyingFileSystem(true);
  }

  private boolean initUnderlyingFileSystem(boolean failover) throws IOException {
    try {
      boolean firstAttempt = true;

      while (true) {
        Stat stat = new Stat();
        String primaryAddr = failoverHandler.getPrimaryAvatarAddress(
            logicalName,
            stat,
            true, firstAttempt);
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
            if (LOG.isDebugEnabled()) {
              LOG.debug("Not initializing standby filesystem because the needed " +
                  "configuration parameters " +
                  "dfs.namenode.dn-address{0|1} are missing");
            }
          }
        } else {
          if (LOG.isDebugEnabled()) {
            LOG.debug("Not initializing standby filesystem because the needed "
                + "configuration parameters fs.default.name{0|1} are missing.");
          }
        }

        try {
          if (failover) {
            if (failoverFS != null) {
              failoverFS.close();
            }
            failoverFS = new DistributedFileSystem();
            failoverFS.initialize(primaryURI, conf);

            newNamenode(failoverFS.dfs.namenode);

          } else {
            super.initialize(primaryURI, conf);
            failoverClient = new FailoverClientProtocol(this.dfs.namenode,
                failoverHandler);
            this.dfs.namenode = failoverClient;
          }
        } catch (IOException ex) {
          if (firstAttempt && failoverHandler.isZKCacheEnabled()) {
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

  @Override
  public boolean isFailoverInProgress() {
    return failoverClient.getNameNode() == null;
  }

  @Override
  public void nameNodeDown() {
    failoverClient.setNameNode(null);
  }

  @Override
  public void newNamenode(VersionedProtocol namenode) {
    failoverClient.setNameNode((ClientProtocol) namenode);
  }

  @Override
  public boolean isShuttingdown() {
    return shutdown;
  }

  @Override
  public void close() throws IOException {
    shutdown = true;
    if (fallback) {
      // no need to lock resources
      super.close();
      return;
    }
    failoverHandler.readLock();
    try {
      super.close();
      if (failoverFS != null) {
        failoverFS.close();
      }
      if (standbyFS != null) {
        standbyFS.close();
      }
      try {
        failoverHandler.shutdown();
      } catch (InterruptedException e) {
        LOG.error("Error shutting down ZooKeeper client", e);
      }
    } finally {
      failoverHandler.readUnlock();
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
        failoverHandler.readLockSimple();

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
          failoverHandler.readUnlock();
          failoverHandler.writeLock();
          boolean failover = failoverHandler.zkCheckFailover();
          if (failover) {
            LOG.info("DAFS failover has happened");
            nameNodeDown();
          } else {
            LOG.debug("DAFS failover has not happened");
          }
        
          standbyFSCheckRequestCount.set(0);
          lastStandbyFSCheck = System.currentTimeMillis();

          // release write lock
          failoverHandler.writeUnLock();

          // now check for failover
          failoverHandler.readLock();
        } else if (standbyFS == null && (System.currentTimeMillis() >
                                         lastStandbyFSInit +
                                         standbyFSInitInterval)) {
          // try to initialize standbyFS

          // release read lock, grab write lock
          failoverHandler.readUnlock();
          failoverHandler.writeLock();
          initStandbyFS();

          failoverHandler.writeUnLock();
          failoverHandler.readLockSimple();
        }

        standbyFSCheckRequestCount.incrementAndGet();

        if (standbyFS == null) {
          // if there is still no standbyFS, use the primary
          LOG.info("DAFS Standby avatar not available, using primary.");
          primaryCalled = true;
          failoverHandler.readUnlock();
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
          failoverHandler.readUnlock();
          return callPrimary();
        }
      } finally {
        if (!primaryCalled) {
          failoverHandler.readUnlock();
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

  /**
   * Return statistics for datanodes that are alive.
   * @return array of data node reports
   * @throw IOException
   */
  public DatanodeInfo[] getLiveDataNodeStats(final boolean useStandby)
    throws IOException {
    return new StandbyCaller<DatanodeInfo[]>() {
      @Override
      DatanodeInfo[] call(DistributedFileSystem fs) throws IOException {
        return fs.getLiveDataNodeStats();
      }
    }.callFS(useStandby);
  }

}
