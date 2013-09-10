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
import java.util.List;

import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.server.namenode.BlocksMap.BlockInfo;


class INodeFileUnderConstruction extends INodeFile {
  String clientName;         // lease holder
  private String clientMachine;
  private final DatanodeDescriptor clientNode; // if client is a cluster node too.

  private int primaryNodeIndex = -1; //the node working on lease recovery
  // targets and targetGSs should either both be null or have the same length
  private DatanodeDescriptor[] targets = null;   //locations for last block
  private long[] targetGSs = null; // generation stamp for each replica
  private long lastRecoveryTime = 0;
  private boolean lastBlockReplicated = false; // if scheduled to replicate last block
  
  INodeFileUnderConstruction(long id, PermissionStatus permissions,
                             short replication,
                             long preferredBlockSize,
                             long modTime,
                             String clientName,
                             String clientMachine,
                             DatanodeDescriptor clientNode) {
  this(id, permissions, replication, preferredBlockSize, modTime, modTime, 
     clientName, clientMachine, clientNode);
  }
  
  INodeFileUnderConstruction(long id, 
                             PermissionStatus permissions,
                             short replication,
                             long preferredBlockSize,
                             long modTime,
                             long accessTime,
                             String clientName,
                             String clientMachine,
                             DatanodeDescriptor clientNode) {
    super(id, permissions.applyUMask(UMASK), 0, replication, modTime, accessTime,
          preferredBlockSize);
    this.clientName = clientName;
    this.clientMachine = clientMachine;
    this.clientNode = clientNode;
  }

  public INodeFileUnderConstruction(long id,
                             byte[] name,
                             short blockReplication,
                             long modificationTime,
                             long preferredBlockSize,
                             BlockInfo[] blocks,
                             PermissionStatus perm,
                             String clientName,
                             String clientMachine,
                             DatanodeDescriptor clientNode) {
    this(id, name, blockReplication, modificationTime, modificationTime,preferredBlockSize, 
         blocks, perm, clientName, clientMachine,clientNode);
  }
  
  public INodeFileUnderConstruction(long id, 
                                    byte[] name,
                                    short blockReplication,
                                    long modificationTime,
                                    long accessTime,
                                    long preferredBlockSize,
                                    BlockInfo[] blocks,
                                    PermissionStatus perm,
                                    String clientName,
                                    String clientMachine,
                                    DatanodeDescriptor clientNode) {
    super(id, perm, blocks, blockReplication, modificationTime, accessTime,
          preferredBlockSize, null);
    setLocalName(name);
    this.clientName = clientName;
    this.clientMachine = clientMachine;
    this.clientNode = clientNode;
  }

  String getClientName() {
    return clientName;
  }

  void setClientMachine(String clientMachine) {
    this.clientMachine = clientMachine;
  }

  public void setClientName(String clientName) {
    this.clientName = clientName;
  }

  String getClientMachine() {
    return clientMachine;
  }

  DatanodeDescriptor getClientNode() {
    return clientNode;
  }

  /**
   * Is this inode being constructed?
   */
  @Override
  boolean isUnderConstruction() {
    return true;
  }

  DatanodeDescriptor[] getTargets() {
    return targets;
  }
  
  /** Return the targets with generation stamp matching that of the last block */
  DatanodeDescriptor[] getValidTargets() {
    if (targetGSs == null) {
      return null;
    }
    int count = 0;
    long lastBlockGS = this.getLastBlock().getGenerationStamp();
    for (long targetGS : targetGSs) {
      if (lastBlockGS == targetGS) {
        count++;
      }
    }
    if (count == 0) {
      return null;
    } if (count == targets.length) {
      return targets;
    } else {
      DatanodeDescriptor[] validTargets = new DatanodeDescriptor[count];
      for (int i=0, numOfValidTargets=0; i<targets.length; i++) {
        if (lastBlockGS == targetGSs[i]) {
          validTargets[numOfValidTargets++] = targets[i];
          if (numOfValidTargets == count) {
            return validTargets;
          }
        }
      }
      return validTargets;
    }
  }
  
  void clearTargets() {
    if (targets != null) {
      for (DatanodeDescriptor node : targets) {
        node.removeINode(this);
      }
    }
    this.targets = null;
    this.targetGSs = null;
  }

  /**
   * Set targets for list of replicas all sharing the same generationStamp
   * 
   * @param locs location of replicas
   * @param generationStamp shared generation stamp
   */
  void setTargets(DatanodeDescriptor[] locs, long generationStamp) {
    setTargets(locs);
    if (locs == null) {
      targetGSs = null;
      return;
    }
    long[] targetGSs = new long[locs.length];
    for (int i=0; i<targetGSs.length; i++) {
      targetGSs[i] = generationStamp;
    }
    this.targetGSs = targetGSs;
  }
  
  private void setTargets(DatanodeDescriptor[] targets) {
    // remove assoc of this with previous Datanodes
    removeINodeFromDatanodeDescriptors(this.targets);
    // add new assoc
    addINodeToDatanodeDescriptors(targets);

    this.targets = targets;
    this.primaryNodeIndex = -1;
  }

  /**
   * add this target if it does not already exists. Returns true if the target
   * was added.
   * 
   * @param node
   *          data node having the block
   * @param generationStamp
   *          the generation of the block on the data node
   * @return true if the data node is added to the target list, or previous
   *         generation stamp for the datanode is updated. Otherwise, false,
   *         which means the data node is already in the target list with
   *         the same generation stamp.
   */
  boolean addTarget(DatanodeDescriptor node, long generationStamp) {
    
    if (this.targets == null) {
      this.targets = new DatanodeDescriptor[0];
    }

    for (int i=0; i<targets.length; i++) {
      if (targets[i].equals(node)) {
        if (generationStamp != targetGSs[i]) {
          targetGSs[i] = generationStamp;
          return true;
        }
        return false;
      }
    }
    
    if (node != null) {
      node.addINode(this);
    }

    // allocate new data structure to store additional target
    DatanodeDescriptor[] newt = new DatanodeDescriptor[targets.length + 1];
    long[] newgs = new long[targets.length + 1];
    for (int i = 0; i < targets.length; i++) {
      newt[i] = this.targets[i];
      newgs[i] = this.targetGSs[i];
    }
    newt[targets.length] = node;
    newgs[targets.length] = generationStamp;

    this.targets = newt;
    this.targetGSs = newgs;
    this.primaryNodeIndex = -1;
    return true;
  }

  void removeTarget(DatanodeDescriptor node) {
    if (targets != null) {
      int index = -1;
      for (int j = 0; j < this.targets.length; j++) {
        if (this.targets[j].equals(node)) {
          index = j;
          break;
        }
      }
      
      if (index == -1) {
        StringBuilder sb = new StringBuilder();
        for (DatanodeDescriptor datanode : this.targets) {
          sb.append(datanode.getName() + ":" + datanode.getStorageID() + " ");
        }
        NameNode.stateChangeLog.error(
            "Node is not in the targets of INodeFileUnderConstruction: "
            + " node=" + node.getName() + ":" + node.getStorageID()
            + " inode=" + this 
            + " targets=" + sb);
        return;
      }
      
      DatanodeDescriptor[] newt = new DatanodeDescriptor[targets.length - 1];
      long[] newgs = new long[targets.length - 1];
      for (int i = 0, j = 0; i < targets.length; i++) {
        if (i != index) {
          newt[j] = this.targets[i];
          newgs[j++] = this.targetGSs[i];
        }
      }

      setTargets(newt);
      this.targetGSs = newgs;
    }
  }

  //
  // converts a INodeFileUnderConstruction into a INodeFile
  // use the modification time as the access time
  //
  INodeFile convertToInodeFile(boolean changeAccessTime) {
    INodeFile obj = new INodeFile(getId(),
                                  getPermissionStatus(),
                                  getBlocks(),
                                  getReplication(),
                                  getModificationTime(),
                                  changeAccessTime ? getModificationTime() : getAccessTime(),
                                  getPreferredBlockSize(),
                                  null);
    return obj;
    
  }
 
  INodeFile convertToInodeFile() {
    return this.convertToInodeFile(true);
  }

  /**
   * remove a block from the block list. This block should be
   * the last one on the list.
   */
  void removeBlock(Block oldblock) throws IOException {
    this.storage.removeBlock(oldblock);
    setTargets(null, -1); // reset targets to be null
  }
  
  /**
   * This function throws exception if the last block of the file
   * is not for blockId.
   * @param blockId
   * @throws IOException
   */
  synchronized void checkLastBlockId(long blockId) throws IOException {
    this.storage.checkLastBlockId(blockId);
  }

  synchronized void setLastBlock(BlockInfo newblock, DatanodeDescriptor[] newtargets
      ) throws IOException {
    this.storage.setLastBlock(newblock);
    setTargets(newtargets, newblock.getGenerationStamp());
    lastRecoveryTime = 0;
  }

  /**
   * Initialize lease recovery for this object
   */
  void assignPrimaryDatanode() {
    //assign the first alive datanode as the primary datanode

    if (targets.length == 0) {
      NameNode.stateChangeLog.warn("BLOCK*"
        + " INodeFileUnderConstruction.initLeaseRecovery:"
        + " No blocks found, lease removed.");
    }

    int previous = primaryNodeIndex;
    Block lastBlock = this.getLastBlock();
    // find an alive datanode beginning from previous.
    // This causes us to cycle through the targets on successive retries.
    for(int i = 1; i <= targets.length; i++) {
      int j = (previous + i)%targets.length;
      if (targets[j].isAlive) {
        DatanodeDescriptor primary = targets[primaryNodeIndex = j]; 
        primary.addBlockToBeRecovered(lastBlock, targets);
        NameNode.stateChangeLog.info("BLOCK* " + lastBlock
          + " recovery started, primary=" + primary);
        return;
      }
    }
  }
  
  /**
   * Update lastRecoveryTime if expired.
   * @return true if lastRecoveryTimeis updated. 
   */
  synchronized boolean setLastRecoveryTime(long now) {
    boolean expired = now - lastRecoveryTime > NameNode.LEASE_RECOVER_PERIOD;
    if (expired) {
      lastRecoveryTime = now;
    }
    return expired;
  }
  
  /** Check if the last block has scheduled to be replicated */
  synchronized boolean isLastBlockReplicated() {
    return this.lastBlockReplicated;
  }
  
  /** Mark that last block has been scheduled to be replicated */
  synchronized void setLastBlockReplicated() {
    this.lastBlockReplicated = true;
  }
  
  /**
   * When deleting an open file, we should remove it from the list
   * of its targets.
   */
  int collectSubtreeBlocksAndClear(List<BlockInfo> v, 
                                   int blocksLimit, 
                                   List<INode> removedINodes) {
    clearTargets();
    return super.collectSubtreeBlocksAndClear(v, blocksLimit, removedINodes);
  }
  
  /**
   * Set local file name. Since the name, and hence hash value, changes,
   * we need to reinsert this inode into the list of it's targets.
   */
  @Override
  void setLocalName(byte[] name) {
    removeINodeFromDatanodeDescriptors(targets);
    this.name = name;
    addINodeToDatanodeDescriptors(targets);
  }
  
  /**
   * Remove this INodeFileUnderConstruction from the list of datanodes.
   */
  private void removeINodeFromDatanodeDescriptors(DatanodeDescriptor[] targets) {
    if (targets != null) {
      for (DatanodeDescriptor node : targets) {
        node.removeINode(this);
      }
    }
  }
  
  /**
   * Add this INodeFileUnderConstruction to the list of datanodes.
   */
  private void addINodeToDatanodeDescriptors(DatanodeDescriptor[] targets) {
    if (targets != null) {
      for (DatanodeDescriptor node : targets) {
        node.addINode(this);
      }
    }
  } 
}
