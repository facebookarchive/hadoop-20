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
package org.apache.hadoop.hdfs.server.datanode;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.server.datanode.FSDataset.ActiveFile;
import org.apache.hadoop.hdfs.server.datanode.FSDataset.FSVolume;


/**
 * A class to maintain a namespace's block->blockInfo map and block->ongoing
 * create map
 */
public class NamespaceMap {
  int NUM_BUCKETS = 64;
  
  /**
   * A bucket of blocks.
   */
  public class BlockBucket {
    int bucketId;
    Map<Block, DatanodeBlockInfo> blockInfoMap;
    
    BlockBucket() {
      blockInfoMap = new HashMap<Block, DatanodeBlockInfo>();      
    }
    
    synchronized int removeUnhealthyVolumes(Collection<FSVolume> failed_vols) {
      int removed_blocks = 0;

      Iterator<Entry<Block, DatanodeBlockInfo>> dbi = blockInfoMap.entrySet()
          .iterator();
      while (dbi.hasNext()) {
        Entry<Block, DatanodeBlockInfo> entry = dbi.next();
        for (FSVolume v : failed_vols) {
          if (entry.getValue().getVolume() == v) {
            DataNode.LOG.warn("removing block " + entry.getKey().getBlockId()
                + " from vol " + v.toString() + ", form namespace: "
                + namespaceId);
            dbi.remove();
            removed_blocks++;
            break;
          }
        }
      }
      return removed_blocks;
    }
    
    synchronized DatanodeBlockInfo getBlockInfo(Block block) {
      return blockInfoMap.get(block);
    }

    synchronized DatanodeBlockInfo addBlockInfo(Block block,
        DatanodeBlockInfo replicaInfo) {
      return blockInfoMap.put(block, replicaInfo);
    }
    
    synchronized DatanodeBlockInfo removeBlockInfo(Block block) {
      return blockInfoMap.remove(block);
    }
    
    /**
     * Get the size of the map for given namespace
     * 
     * @return the number of replicas in the map
     */
    synchronized int size() {
      return blockInfoMap.size();
    }
    
    public synchronized String toString() {
      return blockInfoMap.toString();
    }
    
    synchronized void getBlockCrcPerVolume(
        Map<FSVolume, List<DatanodeBlockInfo>> fsVolumeMap) {
      for (DatanodeBlockInfo binfo : blockInfoMap.values()) {
        if (fsVolumeMap.containsKey(binfo.getVolume())
            && binfo.hasBlockCrcInfo()) {
          fsVolumeMap.get(binfo.getVolume()).add(binfo);
        }
      }
    }

  }
  
  // Map of block Id to DatanodeBlockInfo
  final private int numBucket;
  final private int namespaceId;
  final private Map<Block, ActiveFile> ongoingCreates;
  final private BlockBucket[] blockBuckets;
  
  NamespaceMap(int namespaceId) {
    numBucket = NUM_BUCKETS;
    this.namespaceId = namespaceId;
    ongoingCreates = new ConcurrentHashMap<Block, ActiveFile>();
    blockBuckets = new BlockBucket[numBucket];
    for (int i = 0; i < numBucket; i++) {
      blockBuckets[i] = new BlockBucket();
    }
  }

  public int getNumBucket() {
    return numBucket;
  }
  
  public BlockBucket getBucket(int i) {
    if (i < 0 || i >= blockBuckets.length) {
      return null;
    }
    return blockBuckets[i];
  }
  
  int getBucketId(Block block) {
    int bucketId = (int) (block.getBlockId() % numBucket);
    if (bucketId < 0) {
      bucketId += numBucket;
    }
    return bucketId;
  }
  
  public BlockBucket getBlockBucket(Block block) {
    return blockBuckets[getBucketId(block)];
  }

  int removeUnhealthyVolumes(Collection<FSVolume> failed_vols) {
    int removed_blocks = 0;
    
    for (BlockBucket blockBucket : blockBuckets) {
      removed_blocks += blockBucket.removeUnhealthyVolumes(failed_vols);
    }
    return removed_blocks;
  }

  /**
   * Get the meta information of the replica that matches both block id and
   * generation stamp
   * 
   * @param block
   *          block with its id as the key
   * @return the replica's meta information
   * @throws IllegalArgumentException
   *           if the input block or block pool is null
   */
  DatanodeBlockInfo getBlockInfo(Block block) {
    return getBlockBucket(block).getBlockInfo(block);
  }

  /**
   * Add a replica's meta information into the map
   * 
   * @param replicaInfo
   *          a replica's meta information
   * @return previous meta information of the replica
   * @throws IllegalArgumentException
   *           if the input parameter is null
   */
  DatanodeBlockInfo addBlockInfo(Block block,
      DatanodeBlockInfo replicaInfo) {
    return getBlockBucket(block).addBlockInfo(block, replicaInfo);
  }
  
  DatanodeBlockInfo updateBlockInfo(Block oldB, Block newB) {
    int bidOld = this.getBucketId(oldB);
    int bidNew = this.getBucketId(newB);
    BlockBucket bOld = blockBuckets[bidOld];
    BlockBucket bNew = blockBuckets[bidNew];
    BlockBucket lock1 = bOld;
    BlockBucket lock2 = bNew;
    
    // make sure we acquire the locks in order
    if (bidOld > bidNew) {
      BlockBucket t = lock1;
      lock1 = lock2;
      lock2 = t;
    }
    
    synchronized (lock1) {
      synchronized (lock2) {
        DatanodeBlockInfo bi = bOld.removeBlockInfo(oldB);
        if (bi != null) {
          bNew.addBlockInfo(newB, bi);
        }
        return bi;
      }
    }
    
  }
  
  /**
   * Remove the replica's meta information from the map that matches the input
   * block's id and generation stamp
   * 
   * @param namespaceId
   * @param block
   *          block with its id as the key
   * @return the removed replica's meta information
   * @throws IllegalArgumentException
   *           if the input block is null
   */
  DatanodeBlockInfo removeBlockInfo(Block block) {
    return getBlockBucket(block).removeBlockInfo(block);
  }
  
  /**
   * Get the size of the map for given namespace
   * 
   * @return the number of replicas in the map
   */
  int size() {
    int ret = 0;
    for (BlockBucket bb : blockBuckets) {
      ret += bb.size();
    }
    return ret;
  }
  
  // for ongoing creates

  ActiveFile getOngoingCreates(Block block) {
    return ongoingCreates.get(block);
  }
  
  ActiveFile removeOngoingCreates(Block block) { 
    return ongoingCreates.remove(block);
  }

  ActiveFile addOngoingCreates(Block block, ActiveFile af) {
    return ongoingCreates.put(block, af);
  }
  
  /**
   * If there is an ActiveFile object for the block, create a copy of the
   * old one and replace the old one. This is to make sure that the VisibleLength
   * applied to the old object will have no impact to the local map. In
   * that way, BlockReceiver can directly update visible length without
   * holding the lock.
   * 
   * @param block
   * @throws CloneNotSupportedException 
   */
  void copyOngoingCreates(Block block) throws CloneNotSupportedException {
    ActiveFile af = ongoingCreates.get(block);
    if (af == null) {
      return;
    }

    ongoingCreates.put(block, af.getClone());
  }
  
  /**
   * get a list of block info with CRC information per FS volume.
   * 
   * @param volumes
   *          Volumes are interested in get the list
   * @return a map from FSVolume to a list of DatanodeBlockInfo in the volume
   *         and has CRC information.
   */
  Map<FSVolume, List<DatanodeBlockInfo>> getBlockCrcPerVolume(
      List<FSVolume> volumes) {
    Map<FSVolume, List<DatanodeBlockInfo>> retMap = new HashMap<FSVolume, List<DatanodeBlockInfo>>();
    for (FSVolume volume : volumes) {
      retMap.put(volume, new ArrayList<DatanodeBlockInfo>());
    }
    for (BlockBucket bb : blockBuckets) {
      bb.getBlockCrcPerVolume(retMap);
    }
    return retMap;
  }
  
  public String toString() {
    StringBuilder sb = new StringBuilder();
    for (BlockBucket bb : blockBuckets) {
      sb.append(bb.toString() + "\n");
    }
    return sb.toString() + "\n---\n" + ongoingCreates.toString();
  }
}
