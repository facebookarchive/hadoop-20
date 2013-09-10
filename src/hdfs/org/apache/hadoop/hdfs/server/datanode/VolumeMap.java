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
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.server.datanode.FSDataset.ActiveFile;
import org.apache.hadoop.hdfs.server.datanode.FSDataset.FSVolume;
import org.apache.hadoop.hdfs.server.datanode.FSDataset.FSDatasetDeltaInterface;

/**
 * Maintains the replicas map.
 */
class VolumeMap {

  private Map<Integer, NamespaceMap> nsMap;
  
  private FSDatasetDeltaInterface datasetDelta;

  synchronized void setDatasetDelta(FSDatasetDeltaInterface stateChangeCallback) {
    this.datasetDelta = stateChangeCallback;
  }
  
  VolumeMap(int numNamespaces) {
    nsMap = new HashMap<Integer, NamespaceMap>(numNamespaces);
  }

  /**
   * Return the namespace map object for the namespace.
   * 
   * @param namespaceID
   *          the namespaceID for which the namespace map should be returned.
   */
  public synchronized NamespaceMap getNamespaceMap(int namespaceID) {
    return nsMap.get(namespaceID);
  }

  synchronized Integer[] getNamespaceList() {
    return nsMap.keySet().toArray(new Integer[nsMap.keySet().size()]);
  }

  /**
   * @param namespaceId
   * @param reader
   * @return number of blocks whose CRCs were updated.
   * @throws IOException
   */
  int updateBlockCrc(int namespaceId, BlockCrcFileReader reader)
      throws IOException {
    NamespaceMap nm = getNamespaceMap(namespaceId);
    if (nm != null) {
      return nm.updateBlockCrc(reader);
    } else {
      return 0;
    }
  }
  
  int getNumBuckets(int namespaceId) {
    NamespaceMap nm = getNamespaceMap(namespaceId);
    if (nm != null) {
      return nm.getNumBucket();
    } else {
      return 0;
    }
  }
  
  static private void checkBlock(Block b) {
    if (b == null) {
      throw new IllegalArgumentException("Block is null");
    }
  }

  synchronized int removeUnhealthyVolumes(Collection<FSVolume> failed_vols) {
    int removed_blocks = 0;

    for (Integer namespaceId : nsMap.keySet()) {
      removed_blocks += nsMap.get(namespaceId).removeUnhealthyVolumes(
          failed_vols, datasetDelta);
    }
    return removed_blocks;
  }
  

  /**
   * Get the meta information of the replica that matches both block id and
   * generation stamp
   * 
   * @param namespaceId
   * @param block
   *          block with its id as the key
   * @return the replica's meta information
   * @throws IllegalArgumentException
   *           if the input block or block pool is null
   */
  DatanodeBlockInfo get(int namespaceId, Block block) {
    checkBlock(block);
    NamespaceMap nm = getNamespaceMap(namespaceId);
    if (nm == null) {
      return null;
    }
    return nm.getBlockInfo(block);
  }

  /**
   * Add a replica's meta information into the map
   * 
   * @param namespaceId
   * @param replicaInfo
   *          a replica's meta information
   * @return previous meta information of the replica
   * @throws IllegalArgumentException
   *           if the input parameter is null
   */
  DatanodeBlockInfo add(int namespaceId, Block block,
      DatanodeBlockInfo replicaInfo) {
    checkBlock(block);
    NamespaceMap nm = getNamespaceMap(namespaceId);
    if (nm == null) {
      return null;
    }
    replicaInfo.setBlock(block);
    if (datasetDelta != null) {
      datasetDelta.addBlock(namespaceId, block);
    }
    return nm.addBlockInfo(block, replicaInfo);
  }

  DatanodeBlockInfo update(int namespaceId, Block oldB, Block newB) {
    NamespaceMap nm = getNamespaceMap(namespaceId);
    if (nm == null) {
      return null;
    }
    if (datasetDelta != null) {
      datasetDelta.updateBlock(namespaceId, oldB, newB);
    }
    return nm.updateBlockInfo(oldB, newB);
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
  DatanodeBlockInfo remove(int namespaceId, Block block) {
    NamespaceMap nm = getNamespaceMap(namespaceId);
    if (nm == null) {
      return null;
    }
    if (datasetDelta != null) {
      datasetDelta.removeBlock(namespaceId, block);
    }
    return nm.removeBlockInfo(block);
  }

  /**
   * Get the size of the map for given namespace
   * 
   * @param namespaceId
   * @return the number of replicas in the map
   */
  int size(int namespaceId) {
    NamespaceMap nm = getNamespaceMap(namespaceId);
    if (nm == null) {
      return 0;
    }
    return nm.size();
  }

  synchronized void initNamespace(int namespaceId) {
    NamespaceMap nm = nsMap.get(namespaceId);
    if(nm != null){
      return; 
    }
    nsMap.put(namespaceId, new NamespaceMap(namespaceId));
  }

  
  synchronized void removeNamespace(int namespaceId){
    nsMap.remove(namespaceId);
  }

  // for ongoing creates

  ActiveFile getOngoingCreates(int namespaceId, Block block) {
    checkBlock(block);
    NamespaceMap nm = getNamespaceMap(namespaceId);
    if (nm == null) {
      return null;
    }
    return nm.getOngoingCreates(block);
  }

  ActiveFile removeOngoingCreates(int namespaceId, Block block) { 
    checkBlock(block);
    NamespaceMap nm = getNamespaceMap(namespaceId);
    if (nm == null) {
      return null;
    }
    return nm.removeOngoingCreates(block);
  }

  ActiveFile addOngoingCreates(int namespaceId, Block block, ActiveFile af) {
    checkBlock(block);
    NamespaceMap nm = getNamespaceMap(namespaceId);
    if (nm == null) {
      return null;
    }
    return nm.addOngoingCreates(block, af);
  }

  /**
   * If there is an ActiveFile object for the block, create a copy of the
   * old one and replace the old one. This is to make sure that the VisibleLength
   * applied to the old object will have no impact to the local map. In
   * that way, BlockReceiver can directly update visible length without
   * holding the lock.
   * 
   * @param namespaceId
   * @param block
   * @throws CloneNotSupportedException 
   */
  void copyOngoingCreates(int namespaceId, Block block) throws CloneNotSupportedException {
    checkBlock(block);
    NamespaceMap nm = getNamespaceMap(namespaceId);
    if (nm != null) {
      nm.copyOngoingCreates(block);
    }
  }

  public synchronized String toString() {
    String ret = "VolumeMap: ";
    for (Integer namespaceId : nsMap.keySet()) {
      ret += nsMap.get(namespaceId).toString();
    }
    return ret;
  }
}
