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

import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.server.namenode.BlocksMap.BlockInfo;
import org.apache.hadoop.hdfs.server.namenode.INodeRaidStorage.RaidBlockInfo;
import org.apache.hadoop.raid.RaidCodec;

public class INodeRegularStorage extends INodeStorage {
  public INodeRegularStorage() {
    super();
  }
  
  public INodeRegularStorage(BlockInfo[] blklist) {
    super(blklist);
  }
  
  @Override
  public StorageType getStorageType() {
    return StorageType.REGULAR_STORAGE;
  }
  
  @Override
  public BlockInfo[] getBlocks() {
    return this.blocks;
  }

  @Override
  public void appendBlocks(INodeFile [] inodes, int totalAddedBlocks, INodeFile inode) {
    int size = this.blocks.length;
  
    BlockInfo[] newlist = new BlockInfo[size + totalAddedBlocks];
    System.arraycopy(this.blocks, 0, newlist, 0, size);
  
    for(INodeFile in: inodes) {
      BlockInfo[] blks = in.storage.getBlocks();
      System.arraycopy(blks, 0, newlist, size, blks.length);
      size += blks.length;
    }
  
    this.blocks = newlist;
  
    for(BlockInfo bi: this.blocks) {
      bi.setINode(inode);
    }
  }
  
  @Override
  public Block getLastBlock() {
    if (this.blocks == null ||
        this.blocks.length == 0)
      return null;
    return this.blocks[this.blocks.length - 1];
  }
  
  /**
   * Return the penultimate allocated block for this file.
   */
  @Override
  public Block getPenultimateBlock() {
    if (blocks == null || blocks.length <= 1) {
      return null;
    }
    return blocks[blocks.length - 2];
  }
  
  @Override
  public long diskspaceConsumed(INodeFile inode) {
    return diskspaceConsumed(this.blocks, inode);
  }
  
  @Override
  public long diskspaceConsumed(Block[] blocks, INodeFile inode) {
    long size = 0;
    if(blocks == null) {
      return 0;
    }
    for (Block blk : blocks) {
      if (blk != null) {
        size += blk.getNumBytes();
      }
    }
    /* If the last block is being written to, use prefferedBlockSize
     * rather than the actual block size.
     */
    if (blocks.length > 0 && blocks[blocks.length-1] != null &&
        inode.isUnderConstruction()) {
      size += inode.getPreferredBlockSize() - blocks[blocks.length-1].getNumBytes();
    }
    return size * inode.getReplication();
  }
  
  @Override
  public long getFileSize() {
    if (blocks == null) {
      return 0L;
    }
    long fileSize = 0L;
    for (Block blk: blocks) {
      fileSize += blk.getNumBytes();
    }
    return fileSize; 
  }
  
  /**
   * add a block to the block list
   */
  @Override
  public void addBlock(BlockInfo newblock) {
    if (this.blocks == null) {
      this.blocks = new BlockInfo[1];
      this.blocks[0] = newblock;
    } else {
      int size = this.blocks.length;
      BlockInfo[] newlist = new BlockInfo[size + 1];
      System.arraycopy(this.blocks, 0, newlist, 0, size);
      newlist[size] = newblock;
      this.blocks = newlist;
    }
  }
  
  @Override
  public void removeBlock(Block oldblock) throws IOException {
    if (blocks == null) {
      throw new IOException("Trying to delete non-existant block " + oldblock);
    }
    int size_1 = blocks.length - 1;
    if (!blocks[size_1].equals(oldblock)) {
      throw new IOException("Trying to delete non-last block " + oldblock);
    }

    //copy to a new list
    BlockInfo[] newlist = new BlockInfo[size_1];
    System.arraycopy(blocks, 0, newlist, 0, size_1);
    blocks = newlist;
  }
  
  @Override
  public void checkLastBlockId(long blockId) throws IOException {
    Block oldLast = this.getLastBlock();
    if (oldLast == null) {
      throw new IOException("Doesn't exist the last block"); 
    }
    if (oldLast.getBlockId() != blockId) {
      // This should not happen - this means that we're performing recovery
      // on an internal block in the file!
      NameNode.stateChangeLog.error(
        "Trying to commit block synchronization for an internal block on"
          + " inode=" + this
          + " newblockId=" + blockId + " oldLast=" + oldLast);
      throw new IOException("Trying to update an internal block of " +
        "pending file " + this);
    }
  }
  
  @Override
  public void setLastBlock(BlockInfo newblock) throws IOException {
    checkLastBlockId(newblock.getBlockId());

    BlockInfo oldLast = blocks[blocks.length - 1];
    if (oldLast.getGenerationStamp() > newblock.getGenerationStamp()) {
      NameNode.stateChangeLog.warn(
        "Updating last block " + oldLast + " of inode " +
          "under construction " + this + " with a block that " +
          "has an older generation stamp: " + newblock);
    } 

    blocks[blocks.length - 1] = newblock;
  }
 
  @Override
  public RaidBlockInfo getFirstBlockInStripe(Block block, int index) throws IOException {
    return (RaidBlockInfo)notSupported("getFirstBlockInStripe");
  }
  
  /**
   * Only used by merge, it puts parity file's blocks and source file's blocks
   * together into a block array to create an INodeRaidStorage  
   */
  @Override
  public INodeRaidStorage convertToRaidStorage(BlockInfo[] parityBlocks, 
      RaidCodec codec, int[] checksums, BlocksMap blocksMap, short replication,
      INodeFile inode) throws IOException {
    if (codec == null) {
      throw new IOException("Codec is null");
    } else {
      return new INodeRaidStorage(codec.convertToRaidStorage(parityBlocks,
          blocks, checksums, blocksMap, replication, inode), codec);
    }
  }
  
  @Override
  public boolean isSourceBlock(BlockInfo block) {
    return true;  
  }
}