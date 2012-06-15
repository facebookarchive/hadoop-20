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

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.server.datanode.FSDataset.ActiveFile;
import org.apache.hadoop.hdfs.server.datanode.FSDataset.FSVolume;

/**
 * Maintains the replicas map.
 */
class VolumeMap {

  // Map of block Id to DatanodeBlockInfo, indexed by namespace
  private Map<Integer, Map<Block, DatanodeBlockInfo>> namespaceMap;
  private Map<Integer, Map<Block, ActiveFile>> ongoingCreates;

  VolumeMap(int numNamespaces) {
    namespaceMap = new HashMap<Integer, Map<Block, DatanodeBlockInfo>>(numNamespaces);
    ongoingCreates = new HashMap<Integer, Map<Block, ActiveFile>>(numNamespaces);
  }

  /**
   * Returns an immutable instance of the volume map for a given namespace.
   * @param namespaceID the namespaceID for which the volume map needs to be
   * returned.
   */
  public Map <Block, DatanodeBlockInfo> getNamespaceMap(int namespaceID) {
    return Collections.unmodifiableMap(namespaceMap.get(namespaceID));
  }

  synchronized Integer[] getNamespaceList() {
    return namespaceMap.keySet().toArray(
        new Integer[namespaceMap.keySet().size()]);
  }

  private void checkBlock(Block b) {
    if (b == null) {
      throw new IllegalArgumentException("Block is null");
    }
  }

  synchronized int removeUnhealthyVolumes(Collection<FSVolume> failed_vols) {
    int removed_blocks = 0;

    for (Integer namespaceId : namespaceMap.keySet()) {
      Map<Block, DatanodeBlockInfo> m = namespaceMap.get(namespaceId);
      Iterator<Entry<Block, DatanodeBlockInfo>> dbi = m.entrySet().iterator();
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
    synchronized(this){
      Map<Block, DatanodeBlockInfo> m = namespaceMap.get(namespaceId);
      return m != null ? m.get(block) : null;
    }
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
    synchronized(this){
      Map<Block, DatanodeBlockInfo> m = namespaceMap.get(namespaceId);
      return m.put(block, replicaInfo);
    }
  }

  DatanodeBlockInfo update(int namespaceId, Block oldB, Block newB) {
    Map<Block, DatanodeBlockInfo> m = namespaceMap.get(namespaceId);
    if (m == null) {
      return null;
    }

    DatanodeBlockInfo bi = m.remove(oldB);
    if (bi != null) {
      m.put(newB, bi);
    }
    return bi;
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
    checkBlock(block);
    synchronized(this){
      Map<Block, DatanodeBlockInfo> m = namespaceMap.get(namespaceId);
      return m != null ? m.remove(block) : null;
    }
  }

  /**
   * Get the size of the map for given namespace
   * 
   * @param namespaceId
   * @return the number of replicas in the map
   */
  synchronized int size(int namespaceId) {
    Map<Block, DatanodeBlockInfo> m = namespaceMap.get(namespaceId);
    return m != null ? m.size() : 0;
  }

  synchronized void initNamespace(int namespaceId) {
    Map<Block, DatanodeBlockInfo> m = namespaceMap.get(namespaceId);
    if(m != null){
      return; 
    }
    m = new HashMap<Block, DatanodeBlockInfo>();
    namespaceMap.put(namespaceId, m);    
    Map<Block, ActiveFile> oc = new HashMap<Block, ActiveFile>();
    ongoingCreates.put(namespaceId, oc);
  }

  
  synchronized void removeNamespace(int namespaceId){
    namespaceMap.remove(namespaceId);
    ongoingCreates.remove(namespaceId);
  }

  // for ongoing creates

  ActiveFile getOngoingCreates(int namespaceId, Block block) {
    checkBlock(block);
    synchronized(this){
      Map<Block, ActiveFile> m = ongoingCreates.get(namespaceId);
      return m != null ? m.get(block) : null;
    }
  }

  ActiveFile removeOngoingCreates(int namespaceId, Block block) { 
    checkBlock(block);
    synchronized(this){
      Map<Block, ActiveFile> m = ongoingCreates.get(namespaceId);
      return m != null ? m.remove(block) : null;
    }
  }

  ActiveFile addOngoingCreates(int namespaceId, Block block, ActiveFile af) {
    checkBlock(block);
    synchronized(this){
      Map<Block, ActiveFile> m = ongoingCreates.get(namespaceId);
      return m.put(block, af);
    }
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
    synchronized(this){
      Map<Block, ActiveFile> m = ongoingCreates.get(namespaceId);
      ActiveFile af = m.get(block);
      if (af == null) {
        return;
      }
      
      m.put(block, af.getClone());
    }
  }


  public synchronized String toString() {
    String ret = "VolumeMap: ";
    for (Integer namespaceId : namespaceMap.keySet()) {
      ret += namespaceMap.get(namespaceId).toString();
      ret += "\n---\n";
      ret += ongoingCreates.get(namespaceId).toString();
    }
    return ret;
  }
}
