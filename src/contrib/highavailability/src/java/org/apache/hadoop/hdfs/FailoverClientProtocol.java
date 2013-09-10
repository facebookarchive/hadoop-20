/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hdfs;

import java.io.IOException;

import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.OpenFileInfo;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.protocol.AlreadyBeingCreatedException;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.protocol.CorruptFileBlocks;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.DirectoryListing;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlockWithMetaInfo;
import org.apache.hadoop.hdfs.protocol.LocatedBlockWithOldGS;
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
import org.apache.hadoop.hdfs.util.InjectionEvent;
import org.apache.hadoop.ipc.ProtocolSignature;
import org.apache.hadoop.ipc.RPC.VersionIncompatible;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.util.InjectionHandler;


/**
 * Implementation of the {@link ClientProtocol} so that Avatar clients are
 * able to talk to the NameNode correctly even after an Avatar failover.
 */
public class FailoverClientProtocol implements ClientProtocol {

  private ClientProtocol namenode;
  private final FailoverClientHandler failoverHandler;

  public FailoverClientProtocol(ClientProtocol namenode, FailoverClientHandler failoverHandler) {
    this.namenode = namenode;
    this.failoverHandler = failoverHandler;
  }

  public ClientProtocol getNameNode() {
    return this.namenode;
  }

  public void setNameNode(ClientProtocol namenode) {
    this.namenode = namenode;
  }

  @Override
  public int getDataTransferProtocolVersion() throws IOException {
    return (failoverHandler.new ImmutableFSCaller<Integer>() {
      @Override
      public Integer call() throws IOException {
        return namenode.getDataTransferProtocolVersion();
      }
    }).callFS();
  }

  @Override
  public void abandonBlock(final Block b, final String src,
      final String holder) throws IOException {
    (failoverHandler.new MutableFSCaller<Boolean>() {

      @Override
      public Boolean call(int retry) throws IOException {
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
      public Boolean call(int retry) throws IOException {
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
      public LocatedDirectoryListing call() throws IOException {
        return namenode.getLocatedPartialListing(src, startAfter);
      }

    }).callFS();
  }

  public LocatedBlock addBlock(final String src, final String clientName,
      final DatanodeInfo[] excludedNodes, final DatanodeInfo[] favoredNodes)
  throws IOException {
    return (failoverHandler.new MutableFSCaller<LocatedBlock>() {
      @Override
      public LocatedBlock call(int retries) throws IOException {
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
      public LocatedBlock call(int retries) throws IOException {
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
      public VersionedLocatedBlock call(int retries) throws IOException {
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
      public LocatedBlockWithMetaInfo call(int retries) throws IOException {
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
      public LocatedBlockWithMetaInfo call(int retries) throws IOException {
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
      public LocatedBlockWithMetaInfo call(int retries) throws IOException {
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
      public LocatedBlockWithMetaInfo call(int retries) throws IOException {
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
      public LocatedBlockWithMetaInfo call(int retries) throws IOException {
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
      public LocatedBlock call(int retries) throws IOException {
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
      public LocatedBlock call(int retries) throws IOException {
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
      public LocatedBlockWithMetaInfo call(int retries) throws IOException {
        if (retries > 0) {
          namenode.complete(src, clientName);
        }
        return namenode.appendAndFetchMetaInfo(src, clientName);
      }
    }).callFS();
  }
  
  @Override
  public LocatedBlockWithOldGS appendAndFetchOldGS(final String src,
      final String clientName) throws IOException {
    return (failoverHandler.new MutableFSCaller<LocatedBlockWithOldGS>() {
      @Override
      public LocatedBlockWithOldGS call(int retries) throws IOException {
        if (retries > 0) {
          namenode.complete(src, clientName);
        }
        return namenode.appendAndFetchOldGS(src, clientName);
      }
    }).callFS();
  }

  @Override
  public boolean complete(final String src, final String clientName)
  throws IOException {
    // Treating this as Immutable even though it changes metadata
    // but the complete called on the file should result in completed file
    return (failoverHandler.new MutableFSCaller<Boolean>() {
      public Boolean call(int r) throws IOException {
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
      public Boolean call(int r) throws IOException {
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
      public Boolean call(int r) throws IOException {
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
      public Boolean call(int retries) throws IOException {
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
      public Boolean call(int retries) throws IOException {
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
                InjectionHandler
                    .processEvent(InjectionEvent.FAILOVERCLIENTPROTOCOL_AFTER_CREATE_FILE);
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
      public Boolean call(int retries) throws IOException {
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
      public Boolean call(int retries) throws IOException {
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
      public UpgradeStatusReport call(int r) throws IOException {
        return namenode.distributedUpgradeProgress(action);
      }
    }).callFS();
  }

  @Override
  public void finalizeUpgrade() throws IOException {
    (failoverHandler.new MutableFSCaller<Boolean>() {
      public Boolean call(int retry) throws IOException {
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
      public Boolean call() throws IOException {
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
      public LocatedBlocks call() throws IOException {
        return namenode.getBlockLocations(src, offset, length);
      }
    }).callFS();
  }

  @Override
  public VersionedLocatedBlocks open(final String src, final long offset,
      final long length) throws IOException {
    // TODO Make it cache values as per Dhruba's suggestion
    return (failoverHandler.new ImmutableFSCaller<VersionedLocatedBlocks>() {
      public VersionedLocatedBlocks call() throws IOException {
        return namenode.open(src, offset, length);
      }
    }).callFS();
  }

  @Override
  public LocatedBlocksWithMetaInfo openAndFetchMetaInfo(final String src, final long offset,
      final long length) throws IOException {
    // TODO Make it cache values as per Dhruba's suggestion
    return (failoverHandler.new ImmutableFSCaller<LocatedBlocksWithMetaInfo>() {
      public LocatedBlocksWithMetaInfo call() throws IOException {
        return namenode.openAndFetchMetaInfo(src, offset, length);
      }
    }).callFS();
  }

  @Override
  public ContentSummary getContentSummary(final String src) throws IOException {
    return (failoverHandler.new ImmutableFSCaller<ContentSummary>() {
      public ContentSummary call() throws IOException {
        return namenode.getContentSummary(src);
      }
    }).callFS();
  }

  @Override
  public String getClusterName() throws IOException {
    return (failoverHandler.new ImmutableFSCaller<String>() {
      public String call() throws IOException {
        return namenode.getClusterName();
      }
    }).callFS();
  }

  @Override
  public void recount() throws IOException {
    (failoverHandler.new ImmutableFSCaller<Boolean>() {
      public Boolean call() throws IOException {
        namenode.recount();
        return true;
      }
    }).callFS();
  }

  @Deprecated @Override
  public FileStatus[] getCorruptFiles() throws AccessControlException,
  IOException {
    return (failoverHandler.new ImmutableFSCaller<FileStatus[]>() {
      public FileStatus[] call() throws IOException {
        return namenode.getCorruptFiles();
      }
    }).callFS();
  }

  @Override
  public CorruptFileBlocks
  listCorruptFileBlocks(final String path, final String cookie)
  throws IOException {
    return (failoverHandler.new ImmutableFSCaller<CorruptFileBlocks>() {
      public CorruptFileBlocks call()
      throws IOException {
        return namenode.listCorruptFileBlocks(path, cookie);
      }
    }).callFS();
  }

  @Override
  public DatanodeInfo[] getDatanodeReport(final DatanodeReportType type)
  throws IOException {
    return (failoverHandler.new ImmutableFSCaller<DatanodeInfo[]>() {
      public DatanodeInfo[] call() throws IOException {
        return namenode.getDatanodeReport(type);
      }
    }).callFS();
  }

  @Override
  public FileStatus getFileInfo(final String src) throws IOException {
    return (failoverHandler.new ImmutableFSCaller<FileStatus>() {
      public FileStatus call() throws IOException {
        return namenode.getFileInfo(src);
      }
    }).callFS();
  }

  @Override
  public HdfsFileStatus getHdfsFileInfo(final String src) throws IOException {
    return (failoverHandler.new ImmutableFSCaller<HdfsFileStatus>() {
      public HdfsFileStatus call() throws IOException {
        return namenode.getHdfsFileInfo(src);
      }
    }).callFS();
  }

  @Override
  public HdfsFileStatus[] getHdfsListing(final String src) throws IOException {
    return (failoverHandler.new ImmutableFSCaller<HdfsFileStatus[]>() {
      public HdfsFileStatus[] call() throws IOException {
        return namenode.getHdfsListing(src);
      }
    }).callFS();
  }

  @Override
  public FileStatus[] getListing(final String src) throws IOException {
    return (failoverHandler.new ImmutableFSCaller<FileStatus[]>() {
      public FileStatus[] call() throws IOException {
        return namenode.getListing(src);
      }
    }).callFS();
  }

  public DirectoryListing getPartialListing(final String src,
      final byte[] startAfter) throws IOException {
    return (failoverHandler.new ImmutableFSCaller<DirectoryListing>() {
      public DirectoryListing call() throws IOException {
        return namenode.getPartialListing(src, startAfter);
      }
    }).callFS();
  }

  @Override
  public long getPreferredBlockSize(final String filename) throws IOException {
    return (failoverHandler.new ImmutableFSCaller<Long>() {
      public Long call() throws IOException {
        return namenode.getPreferredBlockSize(filename);
      }
    }).callFS();
  }

  @Override
  public long[] getStats() throws IOException {
    return (failoverHandler.new ImmutableFSCaller<long[]>() {
      public long[] call() throws IOException {
        return namenode.getStats();
      }
    }).callFS();
  }

  @Override
  public void metaSave(final String filename) throws IOException {
    (failoverHandler.new ImmutableFSCaller<Boolean>() {
      public Boolean call() throws IOException {
        namenode.metaSave(filename);
        return true;
      }
    }).callFS();
  }

  @Override
  public void blockReplication(final boolean isEnable) throws IOException {
    (failoverHandler.new ImmutableFSCaller<Boolean>() {
      public Boolean call() throws IOException {
        namenode.blockReplication(isEnable);
        return true;
      }
    }).callFS();
  }

  @Override
  public OpenFileInfo[] iterativeGetOpenFiles(
      final String path, final int millis, final String start) throws IOException {
    return (failoverHandler.new ImmutableFSCaller<OpenFileInfo[]>() {
      public OpenFileInfo[] call() throws IOException {
        return namenode.iterativeGetOpenFiles(path, millis, start);
      }
    }).callFS();
  }

  @Override
  public boolean mkdirs(final String src, final FsPermission masked)
  throws IOException {
    return (failoverHandler.new ImmutableFSCaller<Boolean>() {
      public Boolean call() throws IOException {
        return namenode.mkdirs(src, masked);
      }
    }).callFS();
  }

  @Override
  public void refreshNodes() throws IOException {
    (failoverHandler.new ImmutableFSCaller<Boolean>() {
      public Boolean call() throws IOException {
        namenode.refreshNodes();
        return true;
      }
    }).callFS();
  }

  @Override
  public boolean hardLink(final String src, final String dst) throws IOException {
    return (failoverHandler.new MutableFSCaller<Boolean>() {

      @Override
      public Boolean call(int retries) throws IOException {
        return namenode.hardLink(src, dst);
      }
    }).callFS();
  }

  @Override
  public String[] getHardLinkedFiles(final String src) throws IOException {
    return (failoverHandler.new ImmutableFSCaller<String[]>() {
      @Override
      public String[] call() throws IOException {
        return namenode.getHardLinkedFiles(src);
      }
    }).callFS();
  }

  @Override
  public boolean rename(final String src, final String dst) throws IOException {
    return (failoverHandler.new MutableFSCaller<Boolean>() {

      @Override
      public Boolean call(int retries) throws IOException {
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
      public Boolean call() throws IOException {
        namenode.renewLease(clientName);
        return true;
      }
    }).callFS();
  }

  @Override
  public void reportBadBlocks(final LocatedBlock[] blocks) throws IOException {
    // TODO this might be a good place to send it to both namenodes
    (failoverHandler.new MutableFSCaller<Boolean>() {
      public Boolean call(int r) throws IOException {
        namenode.reportBadBlocks(blocks);
        return true;
      }
    }).callFS();
  }

  @Override
  public void saveNamespace() throws IOException {
    (failoverHandler.new MutableFSCaller<Boolean>() {
      public Boolean call(int r) throws IOException {
        namenode.saveNamespace();
        return true;
      }
    }).callFS();
  }
  
  @Override
  public void rollEditLogAdmin() throws IOException {
    (failoverHandler.new MutableFSCaller<Boolean>() {
      public Boolean call(int r) throws IOException {
        namenode.rollEditLogAdmin();
        return true;
      }
    }).callFS();
  }

  @Override
  public void saveNamespace(final boolean force, final boolean uncompressed) throws IOException {
    (failoverHandler.new MutableFSCaller<Boolean>() {
      public Boolean call(int r) throws IOException {
        namenode.saveNamespace(force, uncompressed);
        return true;
      }
    }).callFS();
  }

  @Override
  public void setOwner(final String src, final String username,
      final String groupname) throws IOException {
    (failoverHandler.new MutableFSCaller<Boolean>() {
      public Boolean call(int r) throws IOException {
        namenode.setOwner(src, username, groupname);
        return true;
      }
    }).callFS();
  }

  @Override
  public void setPermission(final String src, final FsPermission permission)
  throws IOException {
    (failoverHandler.new MutableFSCaller<Boolean>() {
      public Boolean call(int r) throws IOException {
        namenode.setPermission(src, permission);
        return true;
      }
    }).callFS();
  }

  @Override
  public void setQuota(final String path, final long namespaceQuota,
      final long diskspaceQuota) throws IOException {
    (failoverHandler.new MutableFSCaller<Boolean>() {
      public Boolean call(int retry) throws IOException {
        namenode.setQuota(path, namespaceQuota, diskspaceQuota);
        return true;
      }
    }).callFS();
  }

  @Override
  public boolean setReplication(final String src, final short replication)
  throws IOException {
    return (failoverHandler.new MutableFSCaller<Boolean>() {
      public Boolean call(int retry) throws IOException {
        return namenode.setReplication(src, replication);
      }
    }).callFS();
  }

  @Override
  public boolean setSafeMode(final SafeModeAction action) throws IOException {
    return (failoverHandler.new MutableFSCaller<Boolean>() {
      public Boolean call(int r) throws IOException {
        return namenode.setSafeMode(action);
      }
    }).callFS();
  }

  @Override
  public void setTimes(final String src, final long mtime, final long atime)
  throws IOException {
    (failoverHandler.new MutableFSCaller<Boolean>() {

      @Override
      public Boolean call(int retry) throws IOException {
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
      public Boolean call(int r) throws IOException {
        namenode.concat(trg, srcs, restricted);
        return true;
      }
    }).callFS();
  }
  
  @Override
  public void merge(final String parity, final String source, 
      final String codecId, final int[] checksums) throws IOException {
    (failoverHandler.new MutableFSCaller<Boolean>() {
      @Override
      public Boolean call(int retries) throws IOException {
        namenode.merge(parity, source, codecId, checksums);
        return true;
      }
    }).callFS();
  }

  @Override
  public long getProtocolVersion(final String protocol,
      final long clientVersion) throws VersionIncompatible, IOException {
    return (failoverHandler.new ImmutableFSCaller<Long>() {

      @Override
      public Long call() throws IOException {
        return namenode.getProtocolVersion(protocol, clientVersion);
      }

    }).callFS();
  }

  @Override
  public ProtocolSignature getProtocolSignature(final String protocol,
      final long clientVersion, final int clientMethodsHash) throws IOException {
    return (failoverHandler.new ImmutableFSCaller<ProtocolSignature>() {

      @Override
      public ProtocolSignature call() throws IOException {
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
      public Boolean call() throws IOException {
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
      public Boolean call() throws IOException {
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
      public Boolean call() throws IOException {
        return namenode
        .closeRecoverLease(src, clientName, discardLastBlock);
      }
    }).callFS();
  }

  @Override
  public LocatedBlockWithFileName getBlockInfo(final long blockId)
  throws IOException {

    return (failoverHandler.new ImmutableFSCaller<LocatedBlockWithFileName>() {
      public LocatedBlockWithFileName call() throws IOException {
        return namenode.getBlockInfo(blockId);
      }
    }).callFS();
  }

  @Override
  public void updatePipeline(final String clientName, final Block oldBlock, 
      final Block newBlock, final DatanodeID[] newNodes) throws IOException {
    
    (failoverHandler.new MutableFSCaller<Boolean>() {
      @Override
      public Boolean call(int retry) throws IOException {
        namenode.updatePipeline(clientName, oldBlock, newBlock, newNodes);
        return true;
      }
    }).callFS();
  }

  @Override
  public long nextGenerationStamp(final Block block, final boolean fromNN)
      throws IOException {
    return (failoverHandler.new ImmutableFSCaller<Long>() {
      public Long call() throws IOException {
        return namenode.nextGenerationStamp(block, fromNN);
      }
    }).callFS();
  }

  @Override
  public void commitBlockSynchronization(final Block block,
      final long newgenerationstamp, final long newlength,
      final boolean closeFile, final boolean deleteblock,
      final DatanodeID[] newtargets) throws IOException {
    (failoverHandler.new MutableFSCaller<Boolean>() {
      @Override
      public Boolean call(int retry) throws IOException {
        namenode.commitBlockSynchronization(block, newgenerationstamp,
            newlength, closeFile, deleteblock, newtargets);
        return true;
      }
    }).callFS();
  }
  
  @Override
  public boolean raidFile(final String source, final String codecId,
      final short expectedSourceRepl) throws IOException {
    return (failoverHandler.new MutableFSCaller<Boolean>() {
      @Override
      public Boolean call(int retries) throws IOException {
        return namenode.raidFile(source, codecId, expectedSourceRepl);
      } 
    }).callFS();
  }
}
