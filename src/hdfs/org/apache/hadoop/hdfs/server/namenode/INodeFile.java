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
package org.apache.hadoop.hdfs.server.namenode;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.server.namenode.BlocksMap.BlockInfo;
import org.apache.hadoop.hdfs.server.namenode.INodeRaidStorage.RaidBlockInfo;
import org.apache.hadoop.hdfs.server.namenode.INodeStorage.StorageType;
import org.apache.hadoop.raid.RaidCodec;

public class INodeFile extends INode {
  static final FsPermission UMASK = FsPermission.createImmutable((short)0111);

  //Number of bits for Block size
  static final short BLOCKBITS = 48;

  //Header mask 64-bit representation
  //Format: [16 bits for replication][48 bits for PreferredBlockSize]
  static final long HEADERMASK = 0xffffL << BLOCKBITS;

  protected long header;
  
  protected INodeStorage storage = null;

  INodeFile(long id, PermissionStatus permissions,  
            int nrBlocks, short replication, long modificationTime,
            long atime, long preferredBlockSize) {
    this(id, permissions, new BlockInfo[nrBlocks], replication,
        modificationTime, atime, preferredBlockSize, null);
  }

  protected INodeFile() {
    storage = null;
    header = 0;
  }

  protected INodeFile(long id, PermissionStatus permissions, BlockInfo[] blklist,
                      short replication, long modificationTime,
                      long atime, long preferredBlockSize, RaidCodec codec) {
    super(id, permissions, modificationTime, atime);
    this.setReplication(replication);
    this.setPreferredBlockSize(preferredBlockSize);
    try {
      this.storage = (codec == null)? new INodeRegularStorage(blklist):
                     new INodeRaidStorage(blklist, codec);
    } catch (IOException ioe) {
      LOG.error("Fail to initialize storage", ioe);
    }
  }
  
  protected INodeFile(INodeFile inodeFile) {  
    super(inodeFile); 
    this.setReplication(inodeFile.getReplication());  
    this.setPreferredBlockSize(inodeFile.getPreferredBlockSize());
    this.storage = inodeFile.storage;
  }

  protected void updateFile(PermissionStatus permissions, BlockInfo[] blklist,
      short replication, long modificationTime, long atime,
      long preferredBlockSize) throws IOException {
    INode.enforceRegularStorageINode(this, 
        "Namenode doesn't support updateFile for empty file or raided file"); 
    super.updateINode(permissions, modificationTime, atime);
    this.setReplication(replication);
    this.setPreferredBlockSize(preferredBlockSize);
    this.storage.blocks = blklist;
  }

  /**
   * Set the {@link FsPermission} of this {@link INodeFile}.
   * Since this is a file,
   * the {@link FsAction#EXECUTE} action, if any, is ignored.
   */
  protected void setPermission(FsPermission permission) {
    super.setPermission(permission.applyUMask(UMASK));
  }

  public boolean isDirectory() {
    return false;
  }

  /**
   * Get block replication for the file
   * @return block replication value
   */
  public short getReplication() {
    return (short) ((header & HEADERMASK) >> BLOCKBITS);
  }
  
  // return block replication. Parity block and Source block 
  // may have different replication
  public short getBlockReplication(BlockInfo block) {
    if (storage.isSourceBlock(block)) {
      return getReplication();
    } else {
      if (storage.getStorageType() == StorageType.RAID_STORAGE) {
        return ((INodeRaidStorage)storage).getCodec().parityReplication;
      } else {
        throw new IllegalStateException("parity block " + block +
            " belongs to a non-raid file");
      }
    }
  }

  public void setReplication(short replication) {
    if(replication <= 0)
       throw new IllegalArgumentException("Unexpected value for the replication");
    header = ((long)replication << BLOCKBITS) | (header & ~HEADERMASK);
  }

  /**
   * Get preferred block size for the file
   * @return preferred block size in bytes
   */
  public long getPreferredBlockSize() {
        return header & ~HEADERMASK;
  }

  public void setPreferredBlockSize(long preferredBlkSize)
  {
    if((preferredBlkSize < 0) || (preferredBlkSize > ~HEADERMASK ))
       throw new IllegalArgumentException("Unexpected value for the block size");
    header = (header & HEADERMASK) | (preferredBlkSize & ~HEADERMASK);
  }
  
  public StorageType getStorageType() {
    return this.storage == null? null: this.storage.getStorageType();
  }

  /**
   * Get file blocks
   * @return file blocks
   */
  public BlockInfo[] getBlocks() {
    return this.storage == null? null: this.storage.getBlocks();
  }
  
  public INodeStorage getStorage() {
    return this.storage;
  }

  /**
   * Return the last block in this file, or null
   * if there are no blocks.
   */
  public Block getLastBlock() {
    return this.storage == null? null: this.storage.getLastBlock();
  }

  /**
   * Set file block
   */
  void setBlock(int idx, BlockInfo blk) {
    this.storage.blocks[idx] = blk;
  }

  int collectSubtreeBlocksAndClear(List<BlockInfo> v, int blocksLimit,
      List<INode> removedINodes) {
    parent = null;
    name = null;
    BlockInfo[] blocks = getBlocks();
    if(blocks != null && v != null) {
      for (BlockInfo blk : blocks) {
        v.add(blk);
      }
    }
    this.storage = null;
    removedINodes.add(this);
    return 1;
  }

  /** {@inheritDoc} */
  long[] computeContentSummary(long[] summary) {
    summary[0] += getFileSize();
    summary[1]++;
    summary[3] += diskspaceConsumed();
    return summary;
  }
  
  long getFileSize() {
    return this.storage == null? 0L: this.storage.getFileSize();
  }
  
  int getBlockIndex(Block blk, String file) throws IOException {
    BlockInfo[] blocks = getBlocks();
    if (blocks == null) {
      throw new IOException("blocks is null for file " + file);
    }
    // null indicates that this block is currently added. Return size() 
    // as the index in this case
    if (blk == null) {
      return blocks.length;
    }
    for (int curBlk = 0; curBlk < blocks.length; curBlk++) {
      if (blocks[curBlk].equals(blk)) {
        return curBlk;
      }
    }
    throw new IOException("Cannot locate " + blk + " in file " + file);
  }

  @Override
  DirCounts spaceConsumedInTree(DirCounts counts) {
    counts.nsCount += 1;
    counts.dsCount += diskspaceConsumed();
    return counts;
  }

  long diskspaceConsumed() {
    return this.storage == null? 0:  this.storage.diskspaceConsumed(this);
  }
  
  long diskspaceConsumed(Block[] blocks) {
    return this.storage == null? 0: this.storage.diskspaceConsumed(blocks, this);
  }

  /**
   * Check if the given block the last block of the file
   * @param block a block
   * @return true if the given block the last block of the file
   */
  boolean isLastBlock(BlockInfo block) {
    return this.storage == null? false: this.storage.getLastBlock() == block;
  }
  
  RaidBlockInfo getFirstBlockInStripe(Block block, int index) throws IOException{
    return this.storage == null ? null : this.storage.getFirstBlockInStripe(block, index);
  }

  public boolean isHardlinkFile() {
    return false;
  }
}
