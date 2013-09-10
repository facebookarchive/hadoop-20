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
import org.apache.hadoop.hdfs.server.namenode.INodeRaidStorage.RaidBlockInfo;
import org.apache.hadoop.raid.RaidCodec;

public abstract class INodeStorage {
  public static final Log LOG = LogFactory.getLog(INodeStorage.class);
  public enum StorageType {
    REGULAR_STORAGE,
    RAID_STORAGE,
  };
  protected BlockInfo blocks[] = null;
  
  public INodeStorage() {
    this.blocks = null;
  }
  
  public INodeStorage(BlockInfo[] blklist) {
    this.blocks = blklist;
  }
  
  public static Object notSupported(String method) throws IOException {
    String message = method + " is not supported";
    LOG.warn(message);
    throw new IOException(message);
  }
  
  public abstract StorageType getStorageType();
  
  public abstract BlockInfo[] getBlocks();
  
  public abstract Block getLastBlock();
  
  public abstract long diskspaceConsumed(INodeFile inode);
  
  public abstract long diskspaceConsumed(Block[] blocks, INodeFile inode);
  
  public abstract long getFileSize();
  
  public abstract void checkLastBlockId(long blockId) throws IOException;
  
  public abstract void setLastBlock(BlockInfo newblock) throws IOException;
  
  public abstract void appendBlocks(INodeFile [] inodes, int totalAddedBlocks
      , INodeFile inode) throws IOException ; 
  
  public abstract Block getPenultimateBlock() throws IOException;
  
  public abstract void addBlock(BlockInfo newblock) throws IOException;
  
  public abstract void removeBlock(Block oldblock) throws IOException;
  
  public abstract RaidBlockInfo getFirstBlockInStripe(Block block, int index) throws IOException;
  
  public abstract INodeRaidStorage convertToRaidStorage(BlockInfo[] parityBlocks, 
      RaidCodec codec, int[] checksums, BlocksMap blocksMap,
      short replication, INodeFile inode) throws IOException;
  
  public abstract boolean isSourceBlock(BlockInfo block);
}
