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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.hdfs.server.namenode.BlocksMap.BlockInfo;
import org.apache.hadoop.hdfs.server.namenode.INodeStorage.StorageType;
import org.apache.hadoop.hdfs.util.LightWeightLinkedSet;
import org.apache.hadoop.raid.RaidCodec;

/* Class for keeping track of under replication blocks
 * Blocks have replication priority, with priority 0 indicating the highest
 * Blocks have only one replicas has the highest
 */
class UnderReplicatedBlocks implements Iterable<BlockInfo> {
  static final int LEVEL = 4;
  static public final int QUEUE_WITH_CORRUPT_BLOCKS = LEVEL-1;
  private List<LightWeightLinkedSet<BlockInfo>> priorityQueues
      = new ArrayList<LightWeightLinkedSet<BlockInfo>>();
  
  private final RaidMissingBlocks raidQueue;
      
  /* constructor */
  UnderReplicatedBlocks() {
    for(int i=0; i<LEVEL; i++) {
      priorityQueues.add(new LightWeightLinkedSet<BlockInfo>());
    }
    raidQueue =  new RaidMissingBlocks();
  }

  /**
   * Empty the queues.
   */
  void clear() {
    for(int i=0; i<LEVEL; i++) {
      priorityQueues.get(i).clear();
    }
    raidQueue.clear();
  }

  /* Return the number of under replication blocks excluding corrupt blocks */
  synchronized int getNonCorruptUnderReplicatedBlocksCount() {
    int size = 0;
    for (int i=0; i<QUEUE_WITH_CORRUPT_BLOCKS; i++) {
      size += priorityQueues.get(i).size();
    }
    return size;
  }

  /** Return the number of corrupt blocks */
  synchronized int getCorruptBlocksCount() {
    return priorityQueues.get(QUEUE_WITH_CORRUPT_BLOCKS).size();
  }
  
  /**
   * return the number of raid missing blocks
   */
  synchronized int getRaidMissingBlocksCount() {
    return raidQueue.getTotalCount();
  }
  
  /** Return the number of under replication blocks of priority */
  synchronized int size( int priority) {
    if (priority < 0 || priority >= LEVEL) {
      throw new IllegalArgumentException("Unsupported priority: " + priority);
    }
    return priorityQueues.get(priority).size();
  }
  
  /** get the number of under replicated blocks with equal or higher priority */
  synchronized int getSize( int priority ) {
    int size = 0;
    for (int i=priority; i<LEVEL; i++) {
      size += priorityQueues.get(i).size();
    }
    return size;
  }
  
  /* Return the total number of under replication blocks */
  synchronized int size() {
    return getCorruptBlocksCount() + getNonCorruptUnderReplicatedBlocksCount();
  }
        
  /* Check if a block is in the neededReplication queue */
  synchronized boolean contains(BlockInfo block) {
    for(LightWeightLinkedSet<BlockInfo> set:priorityQueues) {
      if(set.contains(block)) { return true; }
    }
    return false;
  }
  
  private boolean isRaidedBlock(BlockInfo block) {
    INodeFile fileINode = block.getINode();
    return fileINode != null && fileINode.getStorageType().equals(StorageType.RAID_STORAGE);
  }
      
  /* Return the priority of a block
   * 
   * If this is a Raided block and still has 1 replica left, not assign the highest priority.
   * 
   * @param block a under replication block
   * @param curReplicas current number of replicas of the block
   * @param expectedReplicas expected number of replicas of the block
   */
  private int getPriority(BlockInfo block, 
                          int curReplicas, 
                          int decommissionedReplicas,
                          int expectedReplicas) {
    if (curReplicas<0 || curReplicas>=expectedReplicas) {
      return LEVEL; // no need to replicate
    } else if(curReplicas==0) {
      // If there are zero non-decommissioned replica but there are
      // some decommissioned replicas, then assign them highest priority
      if (decommissionedReplicas > 0) {
        return 0;
      }
      return QUEUE_WITH_CORRUPT_BLOCKS; // keep these blocks in needed replication.
    } else if(curReplicas==1) {
      return isRaidedBlock(block) ? 1 : 0; // highest priority
    } else if(curReplicas*3<expectedReplicas) {
      return 1;
    } else {
      return 2;
    }
  }
      
  /* add a block to a under replication queue according to its priority
   * @param block a under replication block
   * @param curReplicas current number of replicas of the block
   * @param expectedReplicas expected number of replicas of the block
   */
  synchronized boolean add(
                           BlockInfo blockInfo,
                           int curReplicas, 
                           int decomissionedReplicas,
                           int expectedReplicas) {
    if(curReplicas<0 || expectedReplicas <= curReplicas) {
      return false;
    }
    int priLevel = getPriority(blockInfo, curReplicas, decomissionedReplicas,
                               expectedReplicas);
    INodeFile fileINode = blockInfo.getINode();
    if (priLevel == QUEUE_WITH_CORRUPT_BLOCKS && fileINode != null &&
        fileINode.getStorageType().equals(StorageType.RAID_STORAGE)) {
      RaidCodec codec = ((INodeRaidStorage) fileINode.getStorage()).getCodec();
      return raidQueue.add(blockInfo, codec);
    }
    
    if(priLevel != LEVEL && priorityQueues.get(priLevel).add(blockInfo)) {
      if (NameNode.stateChangeLog.isDebugEnabled()) {
        NameNode.stateChangeLog.debug(
                                      "BLOCK* NameSystem.UnderReplicationBlock.add:"
                                      + blockInfo
                                      + " has only "+curReplicas
                                      + " replicas and need " + expectedReplicas
                                      + " replicas so is added to neededReplications"
                                      + " at priority level " + priLevel);
      }
      return true;
    }
    return false;
  }

  /* remove a block from a under replication queue */
  synchronized boolean remove(BlockInfo blockInfo, 
                              int oldReplicas, 
                              int decommissionedReplicas,
                              int oldExpectedReplicas) {
    int priLevel = getPriority(blockInfo, oldReplicas, 
                               decommissionedReplicas,
                               oldExpectedReplicas);
    return remove(blockInfo, priLevel);
  }
      
  /* remove a block from a under replication queue given a priority*/
  synchronized boolean remove(BlockInfo blockInfo, int priLevel) {
    INodeFile fileINode = blockInfo.getINode();
    if (priLevel == QUEUE_WITH_CORRUPT_BLOCKS && fileINode != null &&
        fileINode.getStorageType().equals(StorageType.RAID_STORAGE)) {
      RaidCodec codec = ((INodeRaidStorage) fileINode.getStorage()).getCodec();
      return raidQueue.remove(blockInfo, codec);
    } 
    
    if(priLevel >= 0 && priLevel < LEVEL 
        && priorityQueues.get(priLevel).remove(blockInfo)) {
      if (NameNode.stateChangeLog.isDebugEnabled()) {
        NameNode.stateChangeLog.debug(
                                      "BLOCK* NameSystem.UnderReplicationBlock.remove: "
                                      + "Removing block " + blockInfo
                                      + " from priority queue "+ priLevel);
      }
      return true;
    } else {
      for(int i=0; i<LEVEL; i++) {
        if(i!=priLevel && priorityQueues.get(i).remove(blockInfo)) {
          if (NameNode.stateChangeLog.isDebugEnabled()) {
            NameNode.stateChangeLog.debug(
                                          "BLOCK* NameSystem.UnderReplicationBlock.remove: "
                                          + "Removing block " + blockInfo
                                          + " from priority queue "+ i);
          }
          return true;
        }
      }
    }
    return false;
  }
      
  /* update the priority level of a block */
  synchronized void update(BlockInfo blockInfo, int curReplicas, 
                           int decommissionedReplicas,
                           int curExpectedReplicas,
                           int curReplicasDelta, int expectedReplicasDelta) {
    int oldReplicas = curReplicas-curReplicasDelta;
    int oldExpectedReplicas = curExpectedReplicas-expectedReplicasDelta;
    int curPri = getPriority(blockInfo, curReplicas, decommissionedReplicas, curExpectedReplicas);
    int oldPri = getPriority(blockInfo, oldReplicas, decommissionedReplicas, oldExpectedReplicas);
    if(NameNode.stateChangeLog.isDebugEnabled()) {
      NameNode.stateChangeLog.debug("UnderReplicationBlocks.update " + 
                                    blockInfo +
                                    " curReplicas " + curReplicas +
                                    " curExpectedReplicas " + curExpectedReplicas +
                                    " oldReplicas " + oldReplicas +
                                    " oldExpectedReplicas  " + oldExpectedReplicas +
                                    " curPri  " + curPri +
                                    " oldPri  " + oldPri);
    }
    if(oldPri != LEVEL && oldPri != curPri) {
      remove(blockInfo, oldPri);
    }
    if(curPri != LEVEL && priorityQueues.get(curPri).add(blockInfo)) {
      if (NameNode.stateChangeLog.isDebugEnabled()) {
        NameNode.stateChangeLog.debug(
                                      "BLOCK* NameSystem.UnderReplicationBlock.update:"
                                      + blockInfo
                                      + " has only "+curReplicas
                                      + " replicas and need " + curExpectedReplicas
                                      + " replicas so is added to neededReplications"
                                      + " at priority level " + curPri);
      }
    }
  }

  /* returns an iterator of all blocks in a given priority queue */
  synchronized BlockIterator iterator(int level) {
    return new BlockIterator(level);
  }

  
  /* return an iterator of all the under replication blocks */
  public synchronized BlockIterator iterator() {
    return new BlockIterator();
  }

  class BlockIterator implements Iterator<BlockInfo> {
    private int level;
    private boolean isIteratorForLevel = false;
    private List<Iterator<BlockInfo>> iterators = 
                  new ArrayList<Iterator<BlockInfo>>();
    
    BlockIterator() {
      level=0;
      for(int i=0; i<LEVEL; i++) {
        iterators.add(priorityQueues.get(i).iterator());
      }
      
    }
 
    BlockIterator(int l) {
      level = l;
      isIteratorForLevel = true;
      iterators.add(priorityQueues.get(level).iterator());
    }
 
    private void update() {
      if (isIteratorForLevel)
        return;
      while(level< LEVEL-1 && !iterators.get(level).hasNext()) {
        level++;
      }
    }
 
    public BlockInfo next() {
      if (isIteratorForLevel)
        return iterators.get(0).next();
      update();
         return iterators.get(level).next();
    }
    
    public boolean hasNext() {
      if (isIteratorForLevel)
        return iterators.get(0).hasNext();
      update();
      return iterators.get(level).hasNext();
    }
 
    public void remove() {
      if (isIteratorForLevel) 
        iterators.get(0).remove();
      else
        iterators.get(level).remove();
    }
 
    public int getPriority() {
      return level;
    }
  }  
  
}
