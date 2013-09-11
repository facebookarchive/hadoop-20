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

import java.io.IOException;
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
import org.apache.hadoop.hdfs.server.datanode.FSDataset.FSDatasetDeltaInterface;
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
    
    BlockBucket(int bucketId) {
      blockInfoMap = new HashMap<Block, DatanodeBlockInfo>();      
      this.bucketId = bucketId;
    }
    
    synchronized int removeUnhealthyVolumes(Collection<FSVolume> failed_vols,
        FSDatasetDeltaInterface datasetDelta) {
      int removed_blocks = 0;

      Iterator<Entry<Block, DatanodeBlockInfo>> dbi = blockInfoMap.entrySet()
          .iterator();
      while (dbi.hasNext()) {
        Entry<Block, DatanodeBlockInfo> entry = dbi.next();
        for (FSVolume v : failed_vols) {
          if (entry.getValue().getBlockDataFile().getVolume() == v) {
            DataNode.LOG.warn("removing block " + entry.getKey().getBlockId()
                + " from vol " + v.toString() + ", form namespace: "
                + namespaceId);
            dbi.remove();
            if (datasetDelta != null) {
              datasetDelta.removeBlock(namespaceId, entry.getKey());
            }
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
    
    synchronized void getBlockReport(List<Block> ret) {
      for (Entry<Block, DatanodeBlockInfo> e : blockInfoMap.entrySet()) {
        if (e.getValue().isFinalized()) {
          ret.add(e.getKey());
        }
      }
    }
    
    synchronized List<DatanodeBlockInfo> getBlockInfosForTesting() {
      List<DatanodeBlockInfo> blockInfos = new ArrayList<DatanodeBlockInfo>();
      for (Entry<Block, DatanodeBlockInfo> e : blockInfoMap.entrySet()) {
        blockInfos.add(e.getValue());
      }
      return blockInfos;
    }

    synchronized DatanodeBlockInfo addBlockInfo(Block block,
        DatanodeBlockInfo replicaInfo) {
      DatanodeBlockInfo oldInfo = blockInfoMap.put(block, replicaInfo);
      if (oldInfo != null) {
        oldInfo.getBlockDataFile().closeFileChannel();
      }
      return oldInfo;
    }
    
    synchronized DatanodeBlockInfo removeBlockInfo(Block block) {
      DatanodeBlockInfo removedInfo = blockInfoMap.remove(block);
      if (removedInfo != null) {
        removedInfo.getBlockDataFile().closeFileChannel();
      }
      return removedInfo;
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
        Map<FSVolume, List<Map<Block, DatanodeBlockInfo>>> fsVolumeMap) {
      for (Map.Entry<Block, DatanodeBlockInfo> entry: blockInfoMap.entrySet()) {
        Block block = entry.getKey();
        DatanodeBlockInfo binfo = entry.getValue();
        if (fsVolumeMap.containsKey(binfo.getBlockDataFile().getVolume())
            && binfo.hasBlockCrcInfo()) {
          fsVolumeMap.get(binfo.getBlockDataFile().getVolume()).get(bucketId)
              .put(block, binfo);
        }
      }
    }

    /**
     * @param blockCrcInfos
     * @return number of blocks whose CRCs were updated.
     * @throws IOException
     */
    synchronized int updateBlockCrc(List<BlockCrcInfoWritable> blockCrcInfos)
        throws IOException {
      int updatedCount = 0;
      Block tmpBlock = new Block();
      for (BlockCrcInfoWritable blockCrcInfo : blockCrcInfos) {
        tmpBlock.set(blockCrcInfo.blockId, 0, blockCrcInfo.blockGenStamp);
        DatanodeBlockInfo info = getBlockInfo(tmpBlock);
        if (info != null && !info.hasBlockCrcInfo()) {
          updatedCount++;
          info.setBlockCrc(blockCrcInfo.blockCrc);
        }
      }
      return updatedCount;
    }
    
    /**
     * 
     * @param reader
     * @return number of blocks whose CRCs were updated.
     * @throws IOException
     */
    int updateBlockCrc(BlockCrcFileReader reader) throws IOException {
      int updatedCount = 0;
      int batchCount = 0;
      List<BlockCrcInfoWritable> listCrcInfo = new ArrayList<BlockCrcInfoWritable>();
      while (reader.moveToNextRecordAndGetItsBucketId() == this.bucketId) {
        BlockCrcInfoWritable blockCrcInfo = reader.getNextRecord();
        if (blockCrcInfo == null) {
          DataNode.LOG.warn("Connot get next block crc record from file.");
          return updatedCount;
        } else {
          listCrcInfo.add(blockCrcInfo);
          batchCount++;
          if (batchCount >= 5000) {
            // We don't want to hold the lock for too long.
            updateBlockCrc(listCrcInfo);
            listCrcInfo.clear();
            batchCount = 0;
            try {
              Thread.sleep(1);
            } catch (InterruptedException e) {
              DataNode.LOG.warn("thread interrupted");
            }
          }
        }
      }
      if (listCrcInfo.size() > 0) {
        updatedCount += updateBlockCrc(listCrcInfo);        
      }
      return updatedCount;
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
      blockBuckets[i] = new BlockBucket(i);
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

  int removeUnhealthyVolumes(Collection<FSVolume> failed_vols,
      FSDatasetDeltaInterface datasetDelta) {
    int removed_blocks = 0;
    
    for (BlockBucket blockBucket : blockBuckets) {
      removed_blocks += blockBucket.removeUnhealthyVolumes(failed_vols,
          datasetDelta);
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
          bi.setBlock(newB);
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
   * @return a map from FSVolume to buckets -> (Block -> DatanodeBlockInfo) in
   *         the volume and has CRC information. The first level value is a
   *         list, each one on the list is for a bucket. The order on the list
   *         is the bucket ID. The third level is a map from block to datablock
   *         info.
   */
  Map<FSVolume, List<Map<Block, DatanodeBlockInfo>>> getBlockCrcPerVolume(
      List<FSVolume> volumes) {
    Map<FSVolume, List<Map<Block, DatanodeBlockInfo>>> retMap =
        new HashMap<FSVolume, List<Map<Block, DatanodeBlockInfo>>>();
    for (FSVolume volume : volumes) {
      List<Map<Block, DatanodeBlockInfo>> newSubMap = new ArrayList<Map<Block, DatanodeBlockInfo>>(
          numBucket);
      for (int i = 0; i < numBucket; i++) {
        newSubMap.add(new HashMap<Block, DatanodeBlockInfo>());
      }
      retMap.put(volume, newSubMap);
    }
    for (BlockBucket bb : blockBuckets) {
      bb.getBlockCrcPerVolume(retMap);
    }
    return retMap;
  }
  
  /**
   * @param reader
   * @return number of blocks whose CRCs were updated.
   * @throws IOException
   */
  int updateBlockCrc(BlockCrcFileReader reader) throws IOException {
    int retValue = 0;
    for (BlockBucket bucket : blockBuckets) {
      retValue += bucket.updateBlockCrc(reader);
    }
    return retValue;
  }
  
  public String toString() {
    StringBuilder sb = new StringBuilder();
    for (BlockBucket bb : blockBuckets) {
      sb.append(bb.toString() + "\n");
    }
    return sb.toString() + "\n---\n" + ongoingCreates.toString();
  }
}
