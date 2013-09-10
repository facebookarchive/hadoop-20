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
import java.util.Iterator;
import java.util.LinkedList;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.server.namenode.BlocksMap.BlockInfo;
import org.apache.hadoop.hdfs.server.namenode.DatanodeDescriptor.DecommissioningStatus;

/**
 * Manage node decommissioning.
 */
class DecommissionManager {
  static final Log LOG = LogFactory.getLog(DecommissionManager.class);

  private final FSNamesystem fsnamesystem;

  DecommissionManager(FSNamesystem namesystem) {
    this.fsnamesystem = namesystem;
  }

  /** Periodically check decommission status. */
  class Monitor implements Runnable {
    /** recheckInterval is how often namenode checks
     *  if a node has finished decommission
     */
    private final long recheckInterval;
    /** The number of decommission nodes to check for each interval */
    private final int numNodesPerCheck;
    private final DecommissioningStatus nodeStatus = new DecommissioningStatus();

    // datanodes that just started decomission,
    // which has higher priority to be checked next
    private final LinkedList<DatanodeDescriptor> newlyStarted =
      new LinkedList<DatanodeDescriptor>();
    // datanodes that needs to be checked next
    private LinkedList<DatanodeDescriptor> toBeChecked =
      new LinkedList<DatanodeDescriptor>();
    // datanodes that just finished check
    private LinkedList<DatanodeDescriptor> checked =
      new LinkedList<DatanodeDescriptor>();

    // the node is under check
    private volatile DatanodeDescriptor nodeBeingCheck;
    // if there was an attempt to stop nodeBeingCheck from decommission
    private volatile boolean pendingToStopDecommission = false;
    
    Monitor(int recheckIntervalInSecond, int numNodesPerCheck) {
      this.recheckInterval = recheckIntervalInSecond * 1000L;
      this.numNodesPerCheck = numNodesPerCheck;
    }

    /**
     * Add a datanode that is just marked to start decommission
     * @param datanode a newly marked decommissioned node
     * @return true if the node is added
     */
    synchronized boolean startDecommision(DatanodeDescriptor datanode) {
      if (datanode == null) {
        throw new IllegalArgumentException(
            "datanode to be decomissioned can not be null");
      }
      if (nodeBeingCheck == datanode) {
        pendingToStopDecommission = false;
        return false;
      } 
      if (!newlyStarted.contains(datanode) && 
          !toBeChecked.contains(datanode) && !checked.contains(datanode)) {
        newlyStarted.offer(datanode);
        notifyAll();
        return true;
      }
      return false;
    }
    
    /**
     * Stop a node from decommission by removing it from the queue
     * @param datanode a datanode
     * @return true if decommission is stopped; false if it is pending
     */
    synchronized boolean stopDecommission(DatanodeDescriptor datanode)
    throws IOException {
      if (datanode == null) {
        throw new IllegalArgumentException(
        "datanode to be removed can not be null");
      }
      if (datanode == nodeBeingCheck) {
        // the node to be stopped decommission is under check
        // so waiting for it to be done
        pendingToStopDecommission = true;
        return false;
      }
      if (newlyStarted.remove(datanode) ||
               toBeChecked.remove(datanode)) {
        checked.remove(datanode);
      }
      datanode.decommissioningStatus.set(0, 0, 0);
      return true;
    }
    
    synchronized private void handlePendingStopDecommission() {
      if (pendingToStopDecommission) {
        LOG.info("Stop (delayed) Decommissioning node " + 
            nodeBeingCheck.getName());
        nodeBeingCheck.stopDecommission();
        nodeBeingCheck.decommissioningStatus.set(0, 0, 0);
        pendingToStopDecommission = false;
      }
    }

    /**
     * Change, if appropriate, the admin state of a datanode to 
     * decommission completed. Return true if decommission is complete.
     */
    private boolean checkDecommissionStateInternal(boolean newlyStartedNode) {
      LOG.info("Decommission started checking the progress of " +
          nodeBeingCheck.getName());
      fsnamesystem.readLock();
      int numOfBlocks;
      try {
        if (!nodeBeingCheck.isDecommissionInProgress()) {
          return true;
        }
        // initialize decominssioning status
        numOfBlocks = nodeBeingCheck.numBlocks();
      } finally {
        fsnamesystem.readUnlock();
      }
      
      nodeStatus.set(0, 0, 0);
      if (newlyStartedNode) {
        // add all nodes to needed replication queue
        // limit the number of scans
        final int numOfBlocksToCheck = Math.max(10000, numOfBlocks/5);
        int numCheckedBlocks = 0;
        boolean hasMore;
        do {
          fsnamesystem.writeLock();
          try {
            Iterator<BlockInfo> it = nodeBeingCheck.getBlockIterator();
            // scan to the last checked position
            for(int i=0; i<numCheckedBlocks && it.hasNext(); i++, it.next()) ;
            // start checking blocks
            for (int i=0; i<numOfBlocksToCheck && it.hasNext(); i++) {
              // put the block into neededReplication queue
              fsnamesystem.isReplicationInProgress(
                  nodeStatus, nodeBeingCheck, it.next(), true);
              numCheckedBlocks++;
            }
            hasMore = it.hasNext();
          } finally {
            fsnamesystem.writeUnlock();
          }
        } while (hasMore);
      } else {
        // Check to see if all blocks in this decommissioned
        // node has reached their target replication factor.
        ArrayList<BlockInfo> needReplicationBlocks = new ArrayList<BlockInfo>();
        fsnamesystem.readLock();
        try {
          for( Iterator<BlockInfo> it = nodeBeingCheck.getBlockIterator();
                                    it.hasNext(); ) {
            final BlockInfo block = fsnamesystem.isReplicationInProgress(
                nodeStatus, nodeBeingCheck, it.next(), false);
            if (block != null) {
              needReplicationBlocks.add(block);
            }
          }
        } finally {
          fsnamesystem.readUnlock();
        }
        
        if (!needReplicationBlocks.isEmpty()) {
          // re-insert them into needReplication queue
          LOG.info("Decommission found " +
              needReplicationBlocks.size() + " under-replicated blocks");
          fsnamesystem.writeLock();
          try {
            for (BlockInfo block : needReplicationBlocks) {
              fsnamesystem.isReplicationInProgress(
                  null, nodeBeingCheck, block, true);
            }
          } finally {
            fsnamesystem.writeUnlock();
          }
        }
      }
      
      fsnamesystem.writeLock();
      nodeBeingCheck.decommissioningStatus.set(
          nodeStatus.getUnderReplicatedBlocks(),
          nodeStatus.getDecommissionOnlyReplicas(),
          nodeStatus.getUnderReplicatedInOpenFiles());
      try {
        handlePendingStopDecommission();
        if (!nodeBeingCheck.isDecommissionInProgress()) {
          return true;
        }
        if (!newlyStartedNode && nodeBeingCheck.decommissioningStatus.
            getUnderReplicatedBlocks() == 0) {
         nodeBeingCheck.setDecommissioned();
         LOG.info("Decommission complete for node " + nodeBeingCheck.getName());
         return true;
        }
      } finally {
        fsnamesystem.writeUnlock();
      }
      
      LOG.info("Decommission finished checking the progress of " +
          nodeBeingCheck.getName());
      return false;
    }

    /**
     * Wait for more work to do
     * @return true if more work to do; false if gets interrupted
     */
    private boolean waitForWork() {
      try {
        synchronized (this) {
          if (newlyStarted.isEmpty() && toBeChecked.isEmpty() &&
              checked.isEmpty()) {
            do {
              wait();
            } while (newlyStarted.isEmpty() && toBeChecked.isEmpty() &&
                checked.isEmpty());
            return true;
          }
        }
        // the queue is not empty
        Thread.sleep(recheckInterval);
        return true;
      } catch (InterruptedException ie) {
        LOG.info("Interrupted " + this.getClass().getSimpleName(), ie);
        return false;
      }
    }
    
    /**
     * Check decommission status of numNodesPerCheck nodes;
     * sleep if there is no decommission node;
     * otherwise wakeup for every recheckInterval milliseconds.
     */
    public void run() {
      while (fsnamesystem.isRunning() && !Thread.interrupted()) {
        try {
          if (waitForWork()) {
            check();
          } else {
            break;
          }
        } catch (Exception e) {
          LOG.warn("DecommissionManager encounters an error: ", e);
        }
      }
    }
    
    /**
     * Get the next datanode that's decommission in progress
     * 
     * @return true if this is a newly started decommission node
     */
    synchronized private boolean getDecommissionInProgressNode() {
      do {
        nodeBeingCheck = newlyStarted.poll();
      } while (nodeBeingCheck != null && !nodeBeingCheck.isAlive);
      if (nodeBeingCheck != null)
        return true;
      
      do {
        nodeBeingCheck = toBeChecked.poll();
      } while (nodeBeingCheck != null && !nodeBeingCheck.isAlive);
      if (nodeBeingCheck == null) {
        // all datanodes have been checked; preparing for the next iteration
        LinkedList<DatanodeDescriptor> tmp = toBeChecked;
        toBeChecked = checked;
        checked = tmp;
      }
      return false;
    }
    
    /**
     * Mark the given datanode as just checked
     * @param datanode
     */
    synchronized private void doneCheck(final boolean isDecommissioned) {
      if (!isDecommissioned) {
        // put to checked for next iteration of check
        checked.add(nodeBeingCheck);
      }
      nodeBeingCheck = null;
    }
    
    /**
     * Check up to numNodesPerCheck decommissioning in progress datanodes to
     * see if all their blocks are replicated.
     */
    private void check() {
      if (fsnamesystem.isInSafeMode()) {
        return;  // not to check when NN in safemode
      }
      for (int i=0; i<numNodesPerCheck; i++) {
        boolean isNewNode = getDecommissionInProgressNode();
        if (nodeBeingCheck == null) {
          break;
        }
        try {
          boolean isDecommissioned =
            checkDecommissionStateInternal(isNewNode);
          doneCheck(isDecommissioned);
        } catch(Exception e) {
          LOG.warn("entry=" + nodeBeingCheck, e);
        }
      }
    }
  }
}
