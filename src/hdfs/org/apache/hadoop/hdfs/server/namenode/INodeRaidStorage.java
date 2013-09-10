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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.server.namenode.BlocksMap.BlockInfo;
import org.apache.hadoop.raid.RaidCodec;

/**
 * Define Raid File Format
 * In RS(10, 4), the 14n, 14n+1, ... 14n+3th blocks in the blocks array 
 * are parity blocks, the 14n+4,...,14n+13th blocks are data blocks. 
 * The last stripe could have partial data blocks
 */
public class INodeRaidStorage extends INodeStorage {
  private final RaidCodec codec;
  public static final Log LOG = LogFactory.getLog(INodeRaidStorage.class);
  
  /**
   * Raid Block Info records the index of the block in the blocks array
   * for fast lookup
   */
  static public class RaidBlockInfo extends BlockInfo {
    private int index = -1;
    public RaidBlockInfo(Block blk, int replication, int newIndex) {
      super(blk, replication);
      this.index = newIndex;
    }
    
    public RaidBlockInfo(int replication, int newIndex) {
      super();
      this.setReplication(replication);
      this.index = newIndex;
    }
    
    public int getIndex() {
      return this.index;
    }
  }
  
  public INodeRaidStorage(BlockInfo[] blkList, RaidCodec newCodec)
    throws IOException {
    super(blkList);
    if (newCodec == null) {
      throw new IOException("codec shouldn't be null for raid storage");
    }
    this.codec = newCodec;
  }
  
  @Override
  public StorageType getStorageType() {
    return StorageType.RAID_STORAGE;
  }
  
  @Override
  public BlockInfo[] getBlocks() {
    return this.blocks;
  }
  
  // Return only the source blocks of the raided file
  public BlockInfo[] getSourceBlocks() {
    return codec.getSourceBlocks(blocks); 
  }
  
  @Override
  public Block getLastBlock() {
    return codec.getLastBlock(blocks);
  }
  
  @Override
  public long getFileSize() {
    return codec.getFileSize(blocks); 
  }
  
  @Override
  public long diskspaceConsumed(INodeFile inode) {
    return diskspaceConsumed(this.blocks, inode);
  }
  
  @Override
  public long diskspaceConsumed(Block[] blocks, INodeFile inode) {
    return codec.diskspaceConsumed(blocks, inode.isUnderConstruction(), 
        inode.getPreferredBlockSize(), inode.getReplication()); 
  }
  
  @Override
  public void checkLastBlockId(long blockId) throws IOException {
    notSupported("checkLastBlockId");
  }
  
  @Override
  public void setLastBlock(BlockInfo newblock) throws IOException {
    notSupported("setLastBlock");
  }
  
  @Override
  public void appendBlocks(INodeFile [] inodes, int totalAddedBlocks
      , INodeFile inode) throws IOException {
    notSupported("appendBlocks");
  }
  
  @Override
  public Block getPenultimateBlock() throws IOException {
    return (Block)notSupported("getPenultimateBlock");
  }
  
  @Override
  public void addBlock(BlockInfo newblock) throws IOException {
    notSupported("addBlock");
  }
  
  @Override
  public boolean isSourceBlock(BlockInfo block) {
    int index = 0;
    if (block instanceof RaidBlockInfo) {
      RaidBlockInfo rbi = (RaidBlockInfo)block;
      index = rbi.index; 
    } else {
      if (LOG.isDebugEnabled()) {
        LOG.debug("block: " + block + " is not raid block info");
      }
      for (index = 0; index < blocks.length; index++) {
        if (blocks[index].equals(block)) {
          break;
        }
      }
      if (index == blocks.length) {
        return false; 
      }
    }
    return index % codec.numStripeBlocks >= codec.numParityBlocks;
  }
  
  @Override
  public void removeBlock(Block oldblock) throws IOException {
    notSupported("removeBlock");
  }
 
  @Override
  public RaidBlockInfo getFirstBlockInStripe(Block block, int index) throws IOException{
    for (int i = 0; i < blocks.length; i+= codec.numStripeBlocks) {
      if (i + codec.numStripeBlocks > index ) {
        // the first block must be a parity block
        return new RaidBlockInfo(blocks[i], codec.parityReplication, i);
      }
    }
    return null;
  }
  
  @Override
  public INodeRaidStorage convertToRaidStorage(BlockInfo[] parityBlocks, 
      RaidCodec codec, int[] checksums, BlocksMap blocksMap, short replication,
      INodeFile inode) throws IOException {
    return (INodeRaidStorage)notSupported("convertToRaidStorage");
  }
  
  public RaidCodec getCodec() {
    return codec;
  }
}
