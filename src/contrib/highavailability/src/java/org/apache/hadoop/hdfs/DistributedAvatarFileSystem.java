package org.apache.hadoop.hdfs;

import java.io.IOException;
import java.io.FileNotFoundException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.OpenFileInfo;
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
import org.apache.hadoop.hdfs.protocol.LocatedBlockWithMetaInfo;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.protocol.LocatedBlocksWithMetaInfo;
import org.apache.hadoop.hdfs.protocol.LocatedBlockWithFileName;
import org.apache.hadoop.hdfs.protocol.LocatedDirectoryListing;
import org.apache.hadoop.hdfs.protocol.VersionedLocatedBlock;
import org.apache.hadoop.hdfs.protocol.VersionedLocatedBlocks;
import org.apache.hadoop.hdfs.protocol.FSConstants.DatanodeReportType;
import org.apache.hadoop.hdfs.protocol.FSConstants.SafeModeAction;
import org.apache.hadoop.hdfs.protocol.FSConstants.UpgradeAction;
import org.apache.hadoop.hdfs.server.common.UpgradeStatusReport;
import org.apache.hadoop.ipc.ProtocolSignature;
import org.apache.hadoop.ipc.RPC.VersionIncompatible;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.ipc.VersionedProtocol;
import org.apache.hadoop.security.AccessControlException;
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
            failoverClient = new FailoverClientProtocol(this.dfs.namenode);
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
    return failoverClient.namenode == null;
  }

  @Override
  public void nameNodeDown() {
    failoverClient.namenode = null;
  }

  @Override
  public void newNamenode(VersionedProtocol namenode) {
    failoverClient.namenode = (ClientProtocol) namenode;
  }

  @Override
  public boolean isShuttingdown() {
    return shutdown;
  }

  private class FailoverClientProtocol implements ClientProtocol {

    protected ClientProtocol namenode;

    public FailoverClientProtocol(ClientProtocol namenode) {
      this.namenode = namenode;
    }

    @Override
    public int getDataTransferProtocolVersion() throws IOException {
      return (failoverHandler.new ImmutableFSCaller<Integer>() {
        @Override
        Integer call() throws IOException {
          return namenode.getDataTransferProtocolVersion();
        }    
      }).callFS();
    }

    @Override
    public void abandonBlock(final Block b, final String src,
        final String holder) throws IOException {
      (failoverHandler.new MutableFSCaller<Boolean>() {

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
      (failoverHandler.new MutableFSCaller<Boolean>() {

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
      return (failoverHandler.new ImmutableFSCaller<LocatedDirectoryListing>() {

        @Override
        LocatedDirectoryListing call() throws IOException {
          return namenode.getLocatedPartialListing(src, startAfter);
        }

      }).callFS();
    }

    public LocatedBlock addBlock(final String src, final String clientName,
        final DatanodeInfo[] excludedNodes, final DatanodeInfo[] favoredNodes)
        throws IOException {
      return (failoverHandler.new MutableFSCaller<LocatedBlock>() {
        @Override
        LocatedBlock call(int retries) throws IOException {
          if (retries > 0) {
            FileStatus info = namenode.getFileInfo(src);
            if (info != null) {
              LocatedBlocks blocks = namenode.getBlockLocations(src, 0,
                  info
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
          return namenode
              .addBlock(src, clientName, excludedNodes,
            favoredNodes);
        }
      }).callFS();
    }

    @Override
    public LocatedBlock addBlock(final String src, final String clientName,
        final DatanodeInfo[] excludedNodes) throws IOException {
      return (failoverHandler.new MutableFSCaller<LocatedBlock>() {
        @Override
        LocatedBlock call(int retries) throws IOException {
          if (retries > 0) {
            FileStatus info = namenode.getFileInfo(src);
            if (info != null) {
              LocatedBlocks blocks = namenode.getBlockLocations(src, 0,
                  info
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
      return (failoverHandler.new MutableFSCaller<VersionedLocatedBlock>() {
        @Override
        VersionedLocatedBlock call(int retries) throws IOException {
          if (retries > 0) {
            FileStatus info = namenode.getFileInfo(src);
            if (info != null) {
              LocatedBlocks blocks = namenode.getBlockLocations(src, 0,
                  info
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
          return namenode
              .addBlockAndFetchVersion(src, clientName, excludedNodes);
        }

      }).callFS();
    }

    @Override
    public LocatedBlockWithMetaInfo addBlockAndFetchMetaInfo(
        final String src, final String clientName,
        final DatanodeInfo[] excludedNodes) throws IOException {
      return (failoverHandler.new MutableFSCaller<LocatedBlockWithMetaInfo>() {
        @Override
        LocatedBlockWithMetaInfo call(int retries) throws IOException {
          if (retries > 0) {
            FileStatus info = namenode.getFileInfo(src);
            if (info != null) {
              LocatedBlocks blocks = namenode.getBlockLocations(src, 0,
                  info
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
          return namenode.addBlockAndFetchMetaInfo(src, clientName,
              excludedNodes);
        }
      }).callFS();
    }

    @Override
    public LocatedBlockWithMetaInfo addBlockAndFetchMetaInfo(
        final String src, final String clientName,
        final DatanodeInfo[] excludedNodes,
        final long startPos) throws IOException {
      return (failoverHandler.new MutableFSCaller<LocatedBlockWithMetaInfo>() {
        @Override
        LocatedBlockWithMetaInfo call(int retries) throws IOException {
          if (retries > 0) {
            FileStatus info = namenode.getFileInfo(src);
            if (info != null) {
              LocatedBlocks blocks = namenode.getBlockLocations(src, 0,
                  info
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
          return namenode.addBlockAndFetchMetaInfo(src, clientName,
              excludedNodes, startPos);
        }

      }).callFS();
    }
    
    @Override
    public LocatedBlockWithMetaInfo addBlockAndFetchMetaInfo(final String src,
        final String clientName, final DatanodeInfo[] excludedNodes,
        final DatanodeInfo[] favoredNodes)
        throws IOException {
      return (failoverHandler.new MutableFSCaller<LocatedBlockWithMetaInfo>() {
        @Override
        LocatedBlockWithMetaInfo call(int retries) throws IOException {
          if (retries > 0) {
            FileStatus info = namenode.getFileInfo(src);
            if (info != null) {
              LocatedBlocks blocks = namenode.getBlockLocations(src, 0,
                  info
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
          return namenode.addBlockAndFetchMetaInfo(src, clientName,
              excludedNodes, favoredNodes);
        }

      }).callFS();
    }

    @Override
    public LocatedBlockWithMetaInfo addBlockAndFetchMetaInfo(final String src,
        final String clientName, final DatanodeInfo[] excludedNodes,
	final DatanodeInfo[] favoredNodes, final long startPos)
        throws IOException {
      return (failoverHandler.new MutableFSCaller<LocatedBlockWithMetaInfo>() {
        @Override
        LocatedBlockWithMetaInfo call(int retries) throws IOException {
          if (retries > 0) {
            FileStatus info = namenode.getFileInfo(src);
            if (info != null) {
              LocatedBlocks blocks = namenode.getBlockLocations(src, 0,
                  info
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
          return namenode.addBlockAndFetchMetaInfo(src, clientName,
              excludedNodes, favoredNodes, startPos);
        }

      }).callFS();
    }

    @Override
    public LocatedBlockWithMetaInfo addBlockAndFetchMetaInfo(final String src,
        final String clientName, final DatanodeInfo[] excludedNodes,
       	final DatanodeInfo[] favoredNodes, final long startPos,
        final Block lastBlock)
        throws IOException {
      return (failoverHandler.new MutableFSCaller<LocatedBlockWithMetaInfo>() {
        @Override
        LocatedBlockWithMetaInfo call(int retries) throws IOException {
          if (retries > 0 && lastBlock == null) {
            FileStatus info = namenode.getFileInfo(src);
            if (info != null) {
              LocatedBlocks blocks = namenode.getBlockLocations(src, 0,
                  info
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
          return namenode.addBlockAndFetchMetaInfo(src, clientName,
       	      excludedNodes, favoredNodes, startPos, lastBlock);
        }

      }).callFS();
    }

    @Override
    public LocatedBlock addBlock(final String src, final String clientName)
        throws IOException {
      return (failoverHandler.new MutableFSCaller<LocatedBlock>() {
        @Override
        LocatedBlock call(int retries) throws IOException {
          if (retries > 0) {
            FileStatus info = namenode.getFileInfo(src);
            if (info != null) {
              LocatedBlocks blocks = namenode.getBlockLocations(src, 0,
                  info
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
      return (failoverHandler.new MutableFSCaller<LocatedBlock>() {
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
    public LocatedBlockWithMetaInfo appendAndFetchMetaInfo(final String src,
        final String clientName) throws IOException {
      return (failoverHandler.new MutableFSCaller<LocatedBlockWithMetaInfo>() {
        @Override
        LocatedBlockWithMetaInfo call(int retries) throws IOException {
          if (retries > 0) {
            namenode.complete(src, clientName);
          }
          return namenode.appendAndFetchMetaInfo(src, clientName);
        }
      }).callFS();
    }

    @Override
    public boolean complete(final String src, final String clientName)
        throws IOException {
      // Treating this as Immutable even though it changes metadata
      // but the complete called on the file should result in completed file
      return (failoverHandler.new MutableFSCaller<Boolean>() {
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
    public boolean complete(final String src, final String clientName,
       	final long fileLen)
        throws IOException {
      // Treating this as Immutable even though it changes metadata
      // but the complete called on the file should result in completed file
      return (failoverHandler.new MutableFSCaller<Boolean>() {
        Boolean call(int r) throws IOException {
          if (r > 0) {
            try {
              return namenode.complete(src, clientName, fileLen);
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
          return namenode.complete(src, clientName, fileLen);
        }
      }).callFS();
    }

    @Override
    public boolean complete(final String src, final String clientName,
       	final long fileLen, final Block lastBlock)
        throws IOException {
      // Treating this as Immutable even though it changes metadata
      // but the complete called on the file should result in completed file
      return (failoverHandler.new MutableFSCaller<Boolean>() {
        Boolean call(int r) throws IOException {
          if (r > 0 && lastBlock == null) {
            try {
              return namenode.complete(src, clientName, fileLen, lastBlock);
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
          return namenode.complete(src, clientName, fileLen, lastBlock);
        }
      }).callFS();
    }

    @Override
    public void create(final String src, final FsPermission masked,
        final String clientName, final boolean overwrite,
        final short replication, final long blockSize) throws IOException {
      (failoverHandler.new MutableFSCaller<Boolean>() {
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
      (failoverHandler.new MutableFSCaller<Boolean>() {
        @Override
        Boolean call(int retries) throws IOException {
          if (retries > 0) {
            // This I am not sure about, because of lease holder I can tell if
            // it
            // was me or not with a high level of certainty
            FileStatus stat = namenode.getFileInfo(src);
            if (stat != null) {
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
                } catch (RemoteException re) {
                  if (re.unwrapRemoteException() instanceof AlreadyBeingCreatedException) {
                    AlreadyBeingCreatedException aex = (AlreadyBeingCreatedException) re
                        .unwrapRemoteException();
                    if (aex.getMessage().contains(
                        "current leaseholder is trying to recreate file")) {
                      namenode.delete(src, false);
                    } else {
                      throw re;
                    }
                  } else {
                    throw re;
                  }
                }
              }
            }
          }
          namenode.create(src, masked, clientName, overwrite, createParent,
              replication,
              blockSize);
          return true;
        }
      }).callFS();
    }

    @Override
    public boolean delete(final String src, final boolean recursive)
        throws IOException {
      return (failoverHandler.new MutableFSCaller<Boolean>() {
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
      return (failoverHandler.new MutableFSCaller<Boolean>() {
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
      return (failoverHandler.new MutableFSCaller<UpgradeStatusReport>() {
        UpgradeStatusReport call(int r) throws IOException {
          return namenode.distributedUpgradeProgress(action);
        }
      }).callFS();
    }

    @Override
    public void finalizeUpgrade() throws IOException {
      (failoverHandler.new MutableFSCaller<Boolean>() {
        Boolean call(int retry) throws IOException {
          namenode.finalizeUpgrade();
          return true;
        }
      }).callFS();

    }

    @Override
    public void fsync(final String src, final String client) throws IOException {
      // TODO Is it Mutable or Immutable
      (failoverHandler.new ImmutableFSCaller<Boolean>() {

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
      return (failoverHandler.new ImmutableFSCaller<LocatedBlocks>() {
        LocatedBlocks call() throws IOException {
          return namenode.getBlockLocations(src, offset, length);
        }
      }).callFS();
    }

    @Override
    public VersionedLocatedBlocks open(final String src, final long offset,
        final long length) throws IOException {
      // TODO Make it cache values as per Dhruba's suggestion
      return (failoverHandler.new ImmutableFSCaller<VersionedLocatedBlocks>() {
        VersionedLocatedBlocks call() throws IOException {
          return namenode.open(src, offset, length);
        }
      }).callFS();
    }

    @Override
    public LocatedBlocksWithMetaInfo openAndFetchMetaInfo(final String src, final long offset,
        final long length) throws IOException {
      // TODO Make it cache values as per Dhruba's suggestion
      return (failoverHandler.new ImmutableFSCaller<LocatedBlocksWithMetaInfo>() {
        LocatedBlocksWithMetaInfo call() throws IOException {
          return namenode.openAndFetchMetaInfo(src, offset, length);
        }
      }).callFS();
    }

    @Override
    public ContentSummary getContentSummary(final String src) throws IOException {
      return (failoverHandler.new ImmutableFSCaller<ContentSummary>() {
        ContentSummary call() throws IOException {
          return namenode.getContentSummary(src);
        }
      }).callFS();
    }

    @Override
    public String getClusterName() throws IOException {
      return (failoverHandler.new ImmutableFSCaller<String>() {
        String call() throws IOException {
          return namenode.getClusterName();
        }
      }).callFS();
    }

    @Override
    public void recount() throws IOException {
      (failoverHandler.new ImmutableFSCaller<Boolean>() {
        Boolean call() throws IOException {
          namenode.recount();
          return true;
        }
      }).callFS();
    }
    
    @Deprecated @Override
    public FileStatus[] getCorruptFiles() throws AccessControlException,
        IOException {
      return (failoverHandler.new ImmutableFSCaller<FileStatus[]>() {
        FileStatus[] call() throws IOException {
          return namenode.getCorruptFiles();
        }
      }).callFS();
    }

    @Override
    public CorruptFileBlocks
      listCorruptFileBlocks(final String path, final String cookie)
      throws IOException {
      return (failoverHandler.new ImmutableFSCaller<CorruptFileBlocks>() {
                CorruptFileBlocks call() 
                  throws IOException {
          return namenode.listCorruptFileBlocks(path, cookie);
                }
              }).callFS();
    }

    @Override
    public DatanodeInfo[] getDatanodeReport(final DatanodeReportType type)
        throws IOException {
      return (failoverHandler.new ImmutableFSCaller<DatanodeInfo[]>() {
        DatanodeInfo[] call() throws IOException {
          return namenode.getDatanodeReport(type);
        }
      }).callFS();
    }

    @Override
    public FileStatus getFileInfo(final String src) throws IOException {
      return (failoverHandler.new ImmutableFSCaller<FileStatus>() {
        FileStatus call() throws IOException {
          return namenode.getFileInfo(src);
        }
      }).callFS();
    }

    @Override
    public HdfsFileStatus getHdfsFileInfo(final String src) throws IOException {
      return (failoverHandler.new ImmutableFSCaller<HdfsFileStatus>() {
        HdfsFileStatus call() throws IOException {
          return namenode.getHdfsFileInfo(src);
        }
      }).callFS();
    }

    @Override
    public HdfsFileStatus[] getHdfsListing(final String src) throws IOException {
      return (failoverHandler.new ImmutableFSCaller<HdfsFileStatus[]>() {
        HdfsFileStatus[] call() throws IOException {
          return namenode.getHdfsListing(src);
        }
      }).callFS();
    }

    @Override
    public FileStatus[] getListing(final String src) throws IOException {
      return (failoverHandler.new ImmutableFSCaller<FileStatus[]>() {
        FileStatus[] call() throws IOException {
          return namenode.getListing(src);
        }
      }).callFS();
    }

    public DirectoryListing getPartialListing(final String src,
        final byte[] startAfter) throws IOException {
      return (failoverHandler.new ImmutableFSCaller<DirectoryListing>() {
        DirectoryListing call() throws IOException {
          return namenode.getPartialListing(src, startAfter);
        }
      }).callFS();
    }

    @Override
    public long getPreferredBlockSize(final String filename) throws IOException {
      return (failoverHandler.new ImmutableFSCaller<Long>() {
        Long call() throws IOException {
          return namenode.getPreferredBlockSize(filename);
        }
      }).callFS();
    }

    @Override
    public long[] getStats() throws IOException {
      return (failoverHandler.new ImmutableFSCaller<long[]>() {
        long[] call() throws IOException {
          return namenode.getStats();
        }
      }).callFS();
    }

    @Override
    public void metaSave(final String filename) throws IOException {
      (failoverHandler.new ImmutableFSCaller<Boolean>() {
        Boolean call() throws IOException {
          namenode.metaSave(filename);
          return true;
        }
      }).callFS();
    }

    @Override
    public OpenFileInfo[] iterativeGetOpenFiles(
      final String path, final int millis, final String start) throws IOException {
      return (failoverHandler.new ImmutableFSCaller<OpenFileInfo[]>() {
        OpenFileInfo[] call() throws IOException {
          return namenode.iterativeGetOpenFiles(path, millis, start);
        }
      }).callFS();
    }

    @Override
    public boolean mkdirs(final String src, final FsPermission masked)
        throws IOException {
      return (failoverHandler.new ImmutableFSCaller<Boolean>() {
        Boolean call() throws IOException {
          return namenode.mkdirs(src, masked);
        }
      }).callFS();
    }

    @Override
    public void refreshNodes() throws IOException {
      (failoverHandler.new ImmutableFSCaller<Boolean>() {
        Boolean call() throws IOException {
          namenode.refreshNodes();
          return true;
        }
      }).callFS();
    }

    @Override
    public boolean hardLink(final String src, final String dst) throws IOException {
      return (failoverHandler.new MutableFSCaller<Boolean>() {

        @Override
        Boolean call(int retries) throws IOException {
          return namenode.hardLink(src, dst);
        }
      }).callFS();
    }
    
    @Override
    public boolean rename(final String src, final String dst) throws IOException {
      return (failoverHandler.new MutableFSCaller<Boolean>() {

        @Override
        Boolean call(int retries) throws IOException {
          if (retries > 0) {
            /*
             * Because of the organization of the code in the namenode if the
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
      (failoverHandler.new ImmutableFSCaller<Boolean>() {
        Boolean call() throws IOException {
          namenode.renewLease(clientName);
          return true;
        }
      }).callFS();
    }

    @Override
    public void reportBadBlocks(final LocatedBlock[] blocks) throws IOException {
      // TODO this might be a good place to send it to both namenodes
      (failoverHandler.new MutableFSCaller<Boolean>() {
        Boolean call(int r) throws IOException {
          namenode.reportBadBlocks(blocks);
          return true;
        }
      }).callFS();
    }

    @Override
    public void saveNamespace() throws IOException {
      (failoverHandler.new MutableFSCaller<Boolean>() {
        Boolean call(int r) throws IOException {
          namenode.saveNamespace();
          return true;
        }
      }).callFS();
    }

    @Override
    public void saveNamespace(final boolean force, final boolean uncompressed) throws IOException {
      (failoverHandler.new MutableFSCaller<Boolean>() {
        Boolean call(int r) throws IOException {
          namenode.saveNamespace(force, uncompressed);
          return true;
        }
      }).callFS();
    }

    @Override
    public void setOwner(final String src, final String username,
        final String groupname) throws IOException {
      (failoverHandler.new MutableFSCaller<Boolean>() {
        Boolean call(int r) throws IOException {
          namenode.setOwner(src, username, groupname);
          return true;
        }
      }).callFS();
    }

    @Override
    public void setPermission(final String src, final FsPermission permission)
        throws IOException {
      (failoverHandler.new MutableFSCaller<Boolean>() {
        Boolean call(int r) throws IOException {
          namenode.setPermission(src, permission);
          return true;
        }
      }).callFS();
    }

    @Override
    public void setQuota(final String path, final long namespaceQuota,
        final long diskspaceQuota) throws IOException {
      (failoverHandler.new MutableFSCaller<Boolean>() {
        Boolean call(int retry) throws IOException {
          namenode.setQuota(path, namespaceQuota, diskspaceQuota);
          return true;
        }
      }).callFS();
    }

    @Override
    public boolean setReplication(final String src, final short replication)
        throws IOException {
      return (failoverHandler.new MutableFSCaller<Boolean>() {
        Boolean call(int retry) throws IOException {
          return namenode.setReplication(src, replication);
        }
      }).callFS();
    }

    @Override
    public boolean setSafeMode(final SafeModeAction action) throws IOException {
      return (failoverHandler.new MutableFSCaller<Boolean>() {
        Boolean call(int r) throws IOException {
          return namenode.setSafeMode(action);
        }
      }).callFS();
    }

    @Override
    public void setTimes(final String src, final long mtime, final long atime)
        throws IOException {
      (failoverHandler.new MutableFSCaller<Boolean>() {

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
      (failoverHandler.new MutableFSCaller<Boolean>() {
        Boolean call(int r) throws IOException {
          namenode.concat(trg, srcs, restricted);
          return true;
        }
      }).callFS();
    }
    
    @Override
    public long getProtocolVersion(final String protocol,
        final long clientVersion) throws VersionIncompatible, IOException {
      return (failoverHandler.new ImmutableFSCaller<Long>() {

        @Override
        Long call() throws IOException {
          return namenode.getProtocolVersion(protocol, clientVersion);
        }

      }).callFS();
    }

    @Override
    public ProtocolSignature getProtocolSignature(final String protocol,
        final long clientVersion, final int clientMethodsHash) throws IOException {
      return (failoverHandler.new ImmutableFSCaller<ProtocolSignature>() {

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
      (failoverHandler.new ImmutableFSCaller<Boolean>() {
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
      return (failoverHandler.new ImmutableFSCaller<Boolean>() {
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
      return (failoverHandler.new ImmutableFSCaller<Boolean>() {
        Boolean call() throws IOException {
          return namenode
              .closeRecoverLease(src, clientName, discardLastBlock);
        }
      }).callFS();
    }
   
    @Override
    public LocatedBlockWithFileName getBlockInfo(final long blockId) 
    		throws IOException {
    	
      return (failoverHandler.new ImmutableFSCaller<LocatedBlockWithFileName>() {
    		LocatedBlockWithFileName call() throws IOException {
          return namenode.getBlockInfo(blockId);
        }
      }).callFS();
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
