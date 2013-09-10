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

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.hdfs.protocol.Block;
import org.mortbay.log.Log;

/**
 * 
 * This class is used by {@link DirectoryScanner} to observe changes
 * that happen to the volumeMap during the scanning process. 
 *
 */
class FSDatasetDelta implements FSDataset.FSDatasetDeltaInterface {
  static enum BlockOperation {
    ADD,
    REMOVE,
    UPDATE;
  }

  private Map<Integer, Map<Long, FSDatasetDelta.BlockOperation>> delta;
  
  private volatile boolean shouldRecordDelta = false;
  
  FSDatasetDelta() {
    resetDelta();
  }
  
  synchronized void startRecordingDelta() {
    shouldRecordDelta = true;
  }
  
  synchronized void stopRecordingDelta() {
    shouldRecordDelta = false;
  }

  synchronized void resetDelta() {
    this.delta = new HashMap<Integer, Map<Long, FSDatasetDelta.BlockOperation>>();
  }
  
  synchronized private void updateBlockOperation(int namespaceId, long blockId, 
      FSDatasetDelta.BlockOperation operation) {
    if (!shouldRecordDelta) {
      return;
    }
    Map<Long, FSDatasetDelta.BlockOperation> namespaceDelta = delta.get(namespaceId);
    if (namespaceDelta == null) {
      namespaceDelta = new HashMap<Long, FSDatasetDelta.BlockOperation>();
      delta.put(namespaceId, namespaceDelta);
    }
    namespaceDelta.put(blockId, operation);
  }
  
  @Override
  public synchronized void addBlock(int namespaceId, Block block) {
    updateBlockOperation(namespaceId, block.getBlockId(), FSDatasetDelta.BlockOperation.ADD);
  }

  @Override
  public synchronized void removeBlock(int namespaceId, Block block) {
    updateBlockOperation(namespaceId, block.getBlockId(), FSDatasetDelta.BlockOperation.REMOVE);
  }

  @Override
  public synchronized void updateBlock(int namespaceId, Block oldBlock,
      Block newBlock) {
    assert oldBlock.getBlockId() == newBlock.getBlockId();
    updateBlockOperation(namespaceId, oldBlock.getBlockId(), FSDatasetDelta.BlockOperation.UPDATE);
  }
  
  synchronized FSDatasetDelta.BlockOperation get(int namespaceId, long blockId) {
    Map<Long, FSDatasetDelta.BlockOperation> namespaceDelta = delta.get(namespaceId);
    return namespaceDelta == null ? null : namespaceDelta.get(blockId);
  }
  
  FSDatasetDelta.BlockOperation get(int namespaceId, Block block) {
    return get(namespaceId, block.getBlockId());
  }    
  
  /* This method is used for testing purposes */
  synchronized int size(int nsid) {
    Map<Long, FSDatasetDelta.BlockOperation> ns = delta.get(nsid);
    return ns == null ? 0 : ns.size();
  }
}