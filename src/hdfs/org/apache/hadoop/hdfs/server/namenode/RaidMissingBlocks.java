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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.server.namenode.BlocksMap.BlockInfo;
import org.apache.hadoop.hdfs.server.namenode.INodeRaidStorage.RaidBlockInfo;
import org.apache.hadoop.raid.RaidCodec;

/**
 * This class manages the priority queues for each RaidCodec.
 *
 */
class RaidMissingBlocks {
  static final Log LOG = LogFactory.getLog(RaidMissingBlocks.class);
  final HashMap<RaidCodec, RaidMissingBlocksPerCodec> queues
        = new HashMap<RaidCodec, RaidMissingBlocksPerCodec> ();
  
  RaidMissingBlocks() {
    for (RaidCodec codec : RaidCodec.getCodecs()) {
      queues.put(codec, new RaidMissingBlocksPerCodec(codec));
    }
  }
  
  /**
   * Empty the queues.
   */
  void clear() {
    for (RaidMissingBlocksPerCodec codecQueue : queues.values()) {
      codecQueue.clear();
    }
  }
  
  /**
   * Get the total count of the missing blocks
   */
  int getTotalCount() {
    int ret = 0; 
    for (RaidMissingBlocksPerCodec queue : queues.values()) {
      ret += queue.getTotalCount();
    }
    return ret;
  }
  
  /**
   * Add a missing Raided block to its codec queue.
   */
  boolean add(BlockInfo blockInfo, RaidCodec codec) {
    if(queues.get(codec).add(blockInfo)) {
      if (NameNode.stateChangeLog.isDebugEnabled()) {
        NameNode.stateChangeLog.debug(
                                      "BLOCK* NameSystem.RaidMissingBlocks.add:"
                                      + blockInfo
                                      + " file "+ blockInfo.getINode()
                                      + " codec " + codec.id);
      }
      return true;
    }
    return false;
  }
  
  /**
   * Remove a missing Raided block from its codec queue.
   */
  boolean remove(BlockInfo blockInfo, RaidCodec codec) {
    if(queues.get(codec).remove(blockInfo)) {
      if (NameNode.stateChangeLog.isDebugEnabled()) {
        NameNode.stateChangeLog.debug(
                                      "BLOCK* NameSystem.RaidMissingBlocks.remove:"
                                      + blockInfo
                                      + " file "+ blockInfo.getINode()
                                      + " codec " + codec.id);
      }
      return true;
    }
    return false;
  }
  
  /**
   * This class maintains a priority queue for one RaidCodec. 
   * 
   * Not thread-safe.
   * 
   * Each entity in the queue points to the missing blocks in a stripe, 
   * we use the first parity block to represent the stripe and has a hashset
   * to store all the indexes of the missing blocks in the stripe.
   * 
   * We prioritize the stripe according to number of missing blocks in the stripe.
   * We calculate the priorities like this:
   * num_missing_blocks       priorities
   *      1                       0
   *      2                       1
   *     ...                     ...
   *   numParityBlocks         numParityBlocks - 1
   *     more                  numParityBlocks(corruptLevel)
   *                            maxLevel
   *  
   */
  private static class RaidMissingBlocksPerCodec {
    
    private final RaidCodec codec;
    // the maxLevel, will be codec.numPrityBlock + 1
    private final int maxLevel;
    
    private final List<HashMap<RaidBlockInfo, HashSet<Integer>>> priorityQueues 
          = new ArrayList<HashMap<RaidBlockInfo, HashSet<Integer>>> ();
    
    RaidMissingBlocksPerCodec(RaidCodec codec) {
      this.codec = codec;
      this.maxLevel = codec.numParityBlocks + 1;
      
      for (int i = 0; i < maxLevel; i++) {
        priorityQueues.add(new HashMap<RaidBlockInfo, HashSet<Integer>>());
      }
    }
    
    /**
     * Empty the queues.
     */
    void clear() {
      for(int i = 0; i < maxLevel; i++) {
        priorityQueues.get(i).clear();
      }
    }
    
    /** Return the number of Raid missing blocks of priority */
    int size(int priority) {
      if (priority < 0 || priority >= maxLevel) {
        throw new IllegalArgumentException("Unsupported priority: " + priority);
      }
      
      if (priority < maxLevel - 1) {
        return priorityQueues.get(priority).size() * (priority + 1);
      } else {
        int ret = 0;
        for (HashSet<Integer> missingBlocks : priorityQueues.get(priority).values()) {
          ret += missingBlocks.size();
        }
        return ret;
      }
    }
    
    /**
     * get total count of the missing blocks in this queue.
     */
    int getTotalCount() {
      int ret = 0;
      for (int priority = 0; priority < maxLevel; priority ++) {
        ret += size(priority);
      }
      return ret;
    }
    
    private int getBlockIndex(BlockInfo blockInfo) 
                      throws IOException {
      if (blockInfo instanceof RaidBlockInfo) {
        return ((RaidBlockInfo) blockInfo).getIndex();
      } else {
        return blockInfo.getINode().getBlockIndex(blockInfo, blockInfo.getINode().toString());
      }
    }
    
    /**
     * Add a missing block to the queue.
     * 
     */
    boolean add(BlockInfo blockInfo) {
      INodeFile fileINode = blockInfo.getINode();
      try {
        int blockIndex = getBlockIndex(blockInfo);
        RaidBlockInfo firstBlock = fileINode.getFirstBlockInStripe(blockInfo, blockIndex);
        HashSet<Integer> missingBlkIdxs = null;
        int i = 0;
        for (; i < maxLevel; i++) {
          HashMap<RaidBlockInfo, HashSet<Integer>> queue = priorityQueues.get(i);
          if (queue.containsKey(firstBlock)) {
            missingBlkIdxs = queue.get(firstBlock);
            if (missingBlkIdxs.contains(blockIndex)) {
              return false;
            }
            
            if (i == maxLevel - 1) {
              missingBlkIdxs.add(blockIndex);
              return true;
            }
            queue.remove(firstBlock);
            break;
          }
        }
        
        if (missingBlkIdxs == null) {
          // no other missing blocks in this stripe
          missingBlkIdxs = new HashSet<Integer>(1);
          missingBlkIdxs.add(blockIndex);
          priorityQueues.get(0).put(firstBlock, missingBlkIdxs);
        } else {
          // there are other missing blocks in this stripe
          missingBlkIdxs.add(blockIndex);
          priorityQueues.get(i + 1).put(firstBlock, missingBlkIdxs);
        }
        
        return true;
      } catch (IOException ex) {
        LOG.warn("Failed to add block into Raid missing blocks queue: " +
                                "block: " + blockInfo +
                                "file: " + fileINode + 
                                "codec: " + codec.id,
                                ex);
        return false;
      }
    }
    
    /**
     * remove a missing block from the queue.
     */
    boolean remove(BlockInfo blockInfo) {
      INodeFile fileINode = blockInfo.getINode();
      try {
        int blockIndex = getBlockIndex(blockInfo);
        RaidBlockInfo firstBlock = fileINode.getFirstBlockInStripe(blockInfo, blockIndex);
        HashSet<Integer> missingBlkIdxs = null;
        int i = 0; 
        for (; i < maxLevel; i++) {
          HashMap<RaidBlockInfo, HashSet<Integer>> queue = priorityQueues.get(i);
          if (queue.containsKey(firstBlock)) {
            missingBlkIdxs = queue.get(firstBlock);
            if (!missingBlkIdxs.contains(blockIndex)) {
              return false;
            }
            break;
          }
        }

        if (missingBlkIdxs == null) {
          // can not find any missing blocks in this stripe
          return false;
        } else {
          missingBlkIdxs.remove(blockIndex);
          if (missingBlkIdxs.size() == 0) {
            priorityQueues.get(i).remove(firstBlock);
          } else if (missingBlkIdxs.size() < (i + 1)) {
            priorityQueues.get(i).remove(firstBlock);
            priorityQueues.get(i - 1).put(firstBlock, missingBlkIdxs);
          }
        }
        return true;
      } catch (Exception ex) {
        LOG.warn("Failed to remove block from Raid missing blocks queue: " +
                                    "block: " + blockInfo +
                                    "file: " + fileINode + 
                                    "codec: " + codec.id,
                                    ex);
        return false;
      }
    }
  }
}
