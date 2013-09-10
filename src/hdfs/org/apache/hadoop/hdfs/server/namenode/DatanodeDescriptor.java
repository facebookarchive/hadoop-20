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

import java.io.DataInput;
import java.io.IOException;
import java.util.*;

import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.BlockListAsLongs;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.server.common.GenerationStamp;
import org.apache.hadoop.hdfs.server.namenode.BlocksMap.BlockInfo;
import org.apache.hadoop.hdfs.server.protocol.BlockCommand;
import org.apache.hadoop.hdfs.server.protocol.DatanodeProtocol;
import org.apache.hadoop.hdfs.server.protocol.RaidTask;
import org.apache.hadoop.hdfs.server.protocol.RaidTaskCommand;
import org.apache.hadoop.hdfs.util.LightWeightHashSet;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.UTF8;
import org.apache.hadoop.io.WritableUtils;

/**************************************************
 * DatanodeDescriptor tracks stats on a given DataNode,
 * such as available storage capacity, last update time, etc.,
 * and maintains a set of blocks stored on the datanode. 
 *
 * This data structure is a data structure that is internal
 * to the namenode. It is *not* sent over-the-wire to the Client
 * or the Datnodes. Neither is it stored persistently in the
 * fsImage.

 **************************************************/
public class DatanodeDescriptor extends DatanodeInfo {
  
  /**
   * Helper class for storing indices of datanodes
   */
  protected static class DatanodeIndex{
    int currentIndex;
    int headIndex;
  }

  // Stores status of decommissioning.
  // If node is not decommissioning, do not use this object for anything.
  DecommissioningStatus decommissioningStatus = new DecommissioningStatus();
  volatile long startTime = FSNamesystem.now();

  /** Block and targets pair */
  public static class BlockTargetPair {
    public final Block block;
    public final DatanodeDescriptor[] targets;    
    
    BlockTargetPair(Block block, DatanodeDescriptor[] targets) {
      this.block = block;
      this.targets = targets;
    }
  }

  /** A BlockTargetPair queue. */
  private static class BlockQueue {
    private final Queue<BlockTargetPair> blockq = new LinkedList<BlockTargetPair>();

    /** Size of the queue */
    synchronized int size() {return blockq.size();}

    /** Enqueue */
    synchronized boolean offer(Block block, DatanodeDescriptor[] targets) { 
      return blockq.offer(new BlockTargetPair(block, targets));
    }

    /** Dequeue */
    synchronized List<BlockTargetPair> poll(int numBlocks) {
      if (numBlocks <= 0 || blockq.isEmpty()) {
        return null;
      }

      List<BlockTargetPair> results = new ArrayList<BlockTargetPair>();
      for(; !blockq.isEmpty() && numBlocks > 0; numBlocks--) {
        results.add(blockq.poll());
      }
      return results;
    }
  }

  private volatile BlockInfo blockList = null;
  private int numOfBlocks = 0;  // number of block this DN has
  
  // used to determine whether a block report should be 
  // 1) discarded in startup safemode
  // 2) processed without computing the diff between a report and in-memory state
  private boolean receivedFirstFullBlockReport = false;
  
  boolean receivedFirstFullBlockReport() {
    return receivedFirstFullBlockReport;
  }
  
  void setReceivedFirstFullBlockReport() {
    receivedFirstFullBlockReport = true;
  }

  // isAlive == heartbeats.contains(this)
  // This is an optimization, because contains takes O(n) time on Arraylist
  protected volatile boolean isAlive = false;

  /** A queue of blocks to be replicated by this datanode */
  private BlockQueue replicateBlocks = new BlockQueue();
  /** A queue of blocks to be recovered by this datanode */
  private BlockQueue recoverBlocks = new BlockQueue();
  /** A set of blocks to be invalidated by this datanode */
  private LightWeightHashSet<Block> invalidateBlocks 
    = new LightWeightHashSet<Block>();
  /** A set of raid encoding tasks to be done by this datanode */
  private LightWeightHashSet<RaidTask> raidEncodingTasks
    = new LightWeightHashSet<RaidTask> ();
  /** A set of raid decoding (block fixing) tasks to be done by this datanode */
  private LightWeightHashSet<RaidTask> raidDecodingTasks
    = new LightWeightHashSet<RaidTask> ();
  /** A set of INodeFileUnderConstruction that this datanode is part of */
  private Set<INodeFileUnderConstruction> openINodes
    = new HashSet<INodeFileUnderConstruction>();

  /* Variables for maintaning number of blocks scheduled to be written to
   * this datanode. This count is approximate and might be slightly higger
   * in case of errors (e.g. datanode does not report if an error occurs 
   * while writing the block).
   */
  private int currApproxBlocksScheduled = 0;
  private int prevApproxBlocksScheduled = 0;
  private long lastBlocksScheduledRollTime = 0;
  private static final int BLOCKS_SCHEDULED_ROLL_INTERVAL = 600*1000; //10min
  /**
   * When set to true, the node is not in include list and is not allowed
   * to communicate with the namenode
   */
  private boolean disallowed = false;
  
  /** Default constructor */
  public DatanodeDescriptor() {}
  
  /** DatanodeDescriptor constructor
   * @param nodeID id of the data node
   */
  public DatanodeDescriptor(DatanodeID nodeID) {
    this(nodeID, 0L, 0L, 0L, 0L, 0);
  }

  /** DatanodeDescriptor constructor
   * 
   * @param nodeID id of the data node
   * @param networkLocation location of the data node in network
   */
  public DatanodeDescriptor(DatanodeID nodeID, 
                            String networkLocation) {
    this(nodeID, networkLocation, null);
  }
  
  /** DatanodeDescriptor constructor
   * 
   * @param nodeID id of the data node
   * @param networkLocation location of the data node in network
   * @param hostName it could be different from host specified for DatanodeID
   */
  public DatanodeDescriptor(DatanodeID nodeID, 
                            String networkLocation,
                            String hostName) {
    this(nodeID, networkLocation, hostName, 0L, 0L, 0L, 0L, 0);
  }
  
  /** DatanodeDescriptor constructor
   * 
   * @param nodeID id of the data node
   * @param capacity capacity of the data node
   * @param dfsUsed space used by the data node
   * @param remaining remaing capacity of the data node
   * @param namespace space used by the data node
   * @param xceiverCount # of data transfers at the data node
   */
  public DatanodeDescriptor(DatanodeID nodeID, 
                            long capacity,
                            long dfsUsed,
                            long remaining,
                            long namespaceUsed,
                            int xceiverCount) {
    super(nodeID);
    updateHeartbeat(capacity, dfsUsed, remaining, namespaceUsed, xceiverCount);
  }

  /** DatanodeDescriptor constructor
   * 
   * @param nodeID id of the data node
   * @param networkLocation location of the data node in network
   * @param capacity capacity of the data node, including space used by non-dfs
   * @param dfsUsed the used space by dfs datanode
   * @param remaining remaing capacity of the data node
   * @param namespace space used by the data node
   * @param xceiverCount # of data transfers at the data node
   */
  public DatanodeDescriptor(DatanodeID nodeID,
                            String networkLocation,
                            String hostName,
                            long capacity,
                            long dfsUsed,
                            long remaining,
                            long namespaceUsed, 
                            int xceiverCount) {
    super(nodeID, networkLocation, hostName);
    updateHeartbeat(capacity, dfsUsed, remaining, namespaceUsed, xceiverCount);
  }

  /**
   * adds an open INodeFile association to this datanode
   *
   * @param iNodeFile - file to associate with this datanode
   * @return
   */
  boolean addINode(INodeFileUnderConstruction iNodeFile) {
    return openINodes.add(iNodeFile);
  }

  /**
   * removes an open INodeFile association to this datanode
   *
   * @param iNodeFile - file disassociate with this datanode
   * @return
   */
  boolean removeINode(INodeFileUnderConstruction iNodeFile) {
    return openINodes.remove(iNodeFile);
  }

  Set<INodeFileUnderConstruction> getOpenINodes() {
    return Collections.unmodifiableSet(openINodes);
  }

  /**
   * Add data-node to the block.
   * Add block to the head of the list of blocks belonging to the data-node.
   */
  boolean addBlock(BlockInfo b) {
    int dnIndex = b.addNode(this);
    if(dnIndex < 0)
      return false;
    // add to the head of the data-node list
    blockList = b.listInsert(blockList, this, dnIndex);
    numOfBlocks++;
    return true;
  }
  
  /**
   * Adds blocks already connected into list, to this descriptor's blocks.
   * The blocks in the input list already have this descriptor inserted to them.
   * Used for parallel initial block reports.
   */
  void insertIntoList(BlockInfo head, int headIndex, BlockInfo tail, int tailIndex, int count) {
    if (head == null)
      return;
    
    // connect tail to now-head
    tail.setNext(tailIndex, blockList);
    if (blockList != null)
      blockList.setPrevious(blockList.findDatanode(this), tail);
    
    // create new head
    blockList = head;
    blockList.setPrevious(headIndex, null);
    
    // add new blocks to the count
    numOfBlocks += count;
  }
  
  /**
   * Adds datanode descriptor to stored block.
   * Ensures that next & previous are null when insert
   * succeeds (DN not already in block info)
   */
  int addBlockWithoutInsertion(BlockInfo b) {
    return b.addNode(this);
  }
  
  /**
   * Remove block from the list of blocks belonging to the data-node.
   * Remove data-node from the block.
   */
  boolean removeBlock(BlockInfo b) {
    blockList = b.listRemove(blockList, this);
    if ( b.removeNode(this) ) {
      numOfBlocks--;
      return true;
    } else {
      return false;
    }
  }

  /**
   * Move block to the head of the list of blocks belonging to the data-node.
   */
  void moveBlockToHead(BlockInfo b) {
    blockList = b.listRemove(blockList, this);
    blockList = b.listInsert(blockList, this, -1);
  }

  /**
   * Remove block from the list and insert
   * into the head of the list of blocks
   * related to the specified DatanodeDescriptor.
   * If the head is null then form a new list.
   * @return current block as the new head of the list.
   */
  protected BlockInfo listMoveToHead(BlockInfo block, BlockInfo head,
      DatanodeIndex indexes) {
    assert head != null : "Head can not be null";
    if (head == block) {
      return head;
    }
    BlockInfo next = block.getSetNext(indexes.currentIndex, head);
    BlockInfo prev = block.getSetPrevious(indexes.currentIndex, null);

    head.setPrevious(indexes.headIndex, block);
    indexes.headIndex = indexes.currentIndex;
    prev.setNext(prev.findDatanode(this), next);
    if (next != null)
      next.setPrevious(next.findDatanode(this), prev);
    return block;
  }

  void resetBlocks() {
    this.capacity = 0;
    this.remaining = 0;
    this.dfsUsed = 0;
    this.namespaceUsed = 0;
    this.xceiverCount = 0;
    this.blockList = null;
    this.numOfBlocks = 0;
    this.invalidateBlocks.clear();
    clearRaidEncodingTasks();
    clearRaidDecodingTasks();
    this.receivedFirstFullBlockReport = false;
  }
  
  /**
   * Raid Encoding functions
   */
  private int clearRaidEncodingTasks() {
    int size = 0;
    synchronized (raidEncodingTasks) {
      size = raidEncodingTasks.size();
      raidEncodingTasks.clear();
    }
    return size;
  }
  
  void addRaidEncodingTask(RaidTask task) {
    synchronized (raidEncodingTasks) {
      raidEncodingTasks.add(task);
    }
  }
  
  int getNumberOfRaidEncodingTasks() {
    synchronized (raidEncodingTasks) {
      return raidEncodingTasks.size();
    }
  }
  
  /**
   * Raid Decoding (block fixing) functions
   */
  private int clearRaidDecodingTasks() {
    int size = 0;
    synchronized (raidDecodingTasks) {
      size = raidDecodingTasks.size();
      raidDecodingTasks.clear();
    }
    return size;
  }
  
  void addRaidDecodingTask(RaidTask task) {
    synchronized (raidDecodingTasks) {
      raidDecodingTasks.add(task);
    }
  }
  
  int getNumberOfRaidDecodingTasks() {
    synchronized (raidDecodingTasks) {
      return raidDecodingTasks.size();
    }
  }
  
  RaidTaskCommand getRaidCommand(int maxEncodingTasks, int maxDecodingTasks) {
    List<RaidTask> tasks = new ArrayList<RaidTask>();
    
    synchronized (raidEncodingTasks) {
      tasks.addAll(raidEncodingTasks.pollN (
          Math.min(raidEncodingTasks.size(), maxEncodingTasks)));
    }
    
    synchronized (raidDecodingTasks) {
      tasks.addAll(raidDecodingTasks.pollN(
          Math.min(raidDecodingTasks.size(), maxDecodingTasks)));
    }
    
    return (tasks.size() == 0) ?
        null : new RaidTaskCommand(DatanodeProtocol.DNA_RAIDTASK, 
                        tasks.toArray(new RaidTask[tasks.size()]));
  }
  
  public int numBlocks() {
    return numOfBlocks;
  }
  
  void updateLastHeard() {
    this.lastUpdate = System.currentTimeMillis();
  }

  /**
   */
  void updateHeartbeat(long capacity, long dfsUsed, long remaining, long namespaceUsed, 
      int xceiverCount) {
    this.capacity = capacity;
    this.dfsUsed = dfsUsed;
    this.remaining = remaining;
    this.namespaceUsed = namespaceUsed;
    this.lastUpdate = System.currentTimeMillis();
    this.xceiverCount = xceiverCount;
    rollBlocksScheduled(lastUpdate);
  }

  /**
   * Iterates over the list of blocks belonging to the data-node.
   */
  static private class BlockIterator implements Iterator<BlockInfo> {
    private BlockInfo current;
    private DatanodeDescriptor node;
      
    BlockIterator(BlockInfo head, DatanodeDescriptor dn) {
      this.current = head;
      this.node = dn;
    }

    public boolean hasNext() {
      return current != null;
    }

    public BlockInfo next() {
      BlockInfo res = current;
      current = current.getNext(current.findDatanode(node));
      return res;
    }

    public void remove()  {
      throw new UnsupportedOperationException("Sorry. can't remove.");
    }
  }

  Iterator<BlockInfo> getBlockIterator() {
    return new BlockIterator(this.blockList, this);
  }
  
  /**
   * Store block replication work.
   */
  void addBlockToBeReplicated(Block block, DatanodeDescriptor[] targets) {
    assert(block != null && targets != null && targets.length > 0);
    replicateBlocks.offer(block, targets);
  }

  /**
   * Store block recovery work.
   */
  void addBlockToBeRecovered(Block block, DatanodeDescriptor[] targets) {
    assert(block != null && targets != null && targets.length > 0);
    recoverBlocks.offer(block, targets);
  }

  /**
   * Store block invalidation work.
   */
  void addBlocksToBeInvalidated(List<Block> blocklist) {
    assert(blocklist != null && blocklist.size() > 0);
    synchronized (invalidateBlocks) {
      for(Block blk : blocklist) {
        invalidateBlocks.add(blk);
      }
    }
  }
  
  /**
   * The number of work items that are pending to be replicated
   */
  int getNumberOfBlocksToBeReplicated() {
    return replicateBlocks.size();
  }

  /**
   * The number of block invalidation items that are pending to 
   * be sent to the datanode
   */
  int getNumberOfBlocksToBeInvalidated() {
    synchronized (invalidateBlocks) {
      return invalidateBlocks.size();
    }
  }
  
  BlockCommand getReplicationCommand(int maxTransfers) {
    List<BlockTargetPair> blocktargetlist = replicateBlocks.poll(maxTransfers);
    return blocktargetlist == null? null:
        new BlockCommand(DatanodeProtocol.DNA_TRANSFER, blocktargetlist);
  }

  BlockCommand getLeaseRecoveryCommand(int maxTransfers) {
    List<BlockTargetPair> blocktargetlist = recoverBlocks.poll(maxTransfers);
    return blocktargetlist == null? null:
        new BlockCommand(DatanodeProtocol.DNA_RECOVERBLOCK, blocktargetlist);
  }

  /**
   * Remove the specified number of blocks to be invalidated
   */
  BlockCommand getInvalidateBlocks(int maxblocks) {
    Block[] deleteList = null;
    synchronized (invalidateBlocks) {
      deleteList = invalidateBlocks.pollToArray(new Block[Math.min(
          invalidateBlocks.size(), maxblocks)]);
    }
    return (deleteList == null || deleteList.length == 0) ? 
        null: new BlockCommand(DatanodeProtocol.DNA_INVALIDATE, deleteList);
  }

  void reportDiff(BlocksMap blocksMap,
                  BlockListAsLongs newReport,
                  Collection<Block> toAdd,
                  Collection<Block> toRemove,
                  Collection<Block> toInvalidate,
                  Collection<Block> toRetry,
                  FSNamesystem namesystem) {
    // place a deilimiter in the list which separates blocks 
    // that have been reported from those that have not
    BlockInfo delimiter = new BlockInfo(new Block(), 1);
    boolean added = this.addBlock(delimiter);
    assert added : "Delimiting block cannot be present in the node";
    // currently the delimiter is the head
    DatanodeIndex indexes = new DatanodeIndex();
    indexes.headIndex = 0;

    if(newReport == null)
      newReport = new BlockListAsLongs( new long[0]);
    // scan the report and collect newly reported blocks
    // Note we are taking special precaution to limit tmp blocks allocated
    // as part this block report - which why block list is stored as longs
    Block iblk = new Block(); // a fixed new'ed block to be reused with index i
    Block oblk = new Block(); // for fixing genstamps
    for (int i = 0; i < newReport.getNumberOfBlocks(); ++i) {
      newReport.getBlock(iblk, i);
      BlockInfo storedBlock = blocksMap.getStoredBlock(iblk);
      if(storedBlock == null) {
        // if the block with a WILDCARD generation stamp matches 
        // then accept this block.
        // This block has a diferent generation stamp on the datanode 
        // because of a lease-recovery-attempt.
        oblk.set(newReport.getBlockId(i), newReport.getBlockLen(i),
                 GenerationStamp.WILDCARD_STAMP);
        storedBlock = blocksMap.getStoredBlock(oblk);
        if (storedBlock != null && storedBlock.getINode() != null &&
            (storedBlock.getGenerationStamp() <= iblk.getGenerationStamp() ||
             storedBlock.getINode().isUnderConstruction())) {
          // accept block. It wil be cleaned up on cluster restart.
        } else {
          storedBlock = null;
        }
      }
      if (storedBlock == null) {
        // If block is not in blocksMap it does not belong to any file
        if (namesystem.getNameNode().shouldRetryAbsentBlock(iblk, storedBlock)) {
          toRetry.add(new Block(iblk));
        } else {
          toInvalidate.add(new Block(iblk));
        }
        continue;
      }
      int index = storedBlock.findDatanode(this);
      if(index < 0) {// Known block, but not on the DN
        // if the size/GS differs from what is in the blockmap, then return
        // the new block. addStoredBlock will then pick up the right size/GS of 
        // this block and will update the block object in the BlocksMap
        if (storedBlock.getNumBytes() != iblk.getNumBytes()
            || storedBlock.getGenerationStamp() != iblk.getGenerationStamp()) {
          toAdd.add(new Block(iblk));
        } else {
          toAdd.add(storedBlock);
        }
        continue;
      }
      indexes.currentIndex = index;
      // move block to the head of the list
      blockList = listMoveToHead(storedBlock, blockList, indexes);
    }
    // collect blocks that have not been reported
    // all of them are next to the delimiter
    Iterator<BlockInfo> it = new BlockIterator(delimiter.getNext(0), this);
    while(it.hasNext()) {
      BlockInfo storedBlock = it.next();
      INodeFile file = storedBlock.getINode();
      if (file == null || !file.isUnderConstruction()) {
        toRemove.add(storedBlock);
      }
    }
    this.removeBlock(delimiter);
  }

  /** Serialization for FSEditLog */
  void readFieldsFromFSEditLog(DataInput in) throws IOException {
    this.name = UTF8.readString(in);
    this.storageID = UTF8.readString(in);
    this.infoPort = in.readShort() & 0x0000ffff;

    this.capacity = in.readLong();
    this.dfsUsed = in.readLong();
    this.remaining = in.readLong();
    this.lastUpdate = in.readLong();
    this.xceiverCount = in.readInt();
    this.location = Text.readString(in);
    this.hostName = Text.readString(in);
    setAdminState(WritableUtils.readEnum(in, AdminStates.class));
  }
  
  /**
   * @return Approximate number of blocks currently scheduled to be written 
   * to this datanode.
   */
  public int getBlocksScheduled() {
    return currApproxBlocksScheduled + prevApproxBlocksScheduled;
  }
  
  /**
   * Increments counter for number of blocks scheduled. 
   */
  void incBlocksScheduled() {
    currApproxBlocksScheduled++;
  }
  
  /**
   * Decrements counter for number of blocks scheduled.
   */
  void decBlocksScheduled() {
    if (prevApproxBlocksScheduled > 0) {
      prevApproxBlocksScheduled--;
    } else if (currApproxBlocksScheduled > 0) {
      currApproxBlocksScheduled--;
    } 
    // its ok if both counters are zero.
  }

  void setStartTime(long time) {
    startTime = time;
  }

  long getStartTime() {
    return startTime;
  }
  
  /**
   * Adjusts curr and prev number of blocks scheduled every few minutes.
   */
  private void rollBlocksScheduled(long now) {
    if ((now - lastBlocksScheduledRollTime) > 
        BLOCKS_SCHEDULED_ROLL_INTERVAL) {
      prevApproxBlocksScheduled = currApproxBlocksScheduled;
      currApproxBlocksScheduled = 0;
      lastBlocksScheduledRollTime = now;
    }
  }
  
  static class DecommissioningStatus {
    int underReplicatedBlocks;
    int decommissionOnlyReplicas;
    int underReplicatedInOpenFiles;

    synchronized void set(int underRep, int onlyRep, int underConstruction) {
      underReplicatedBlocks = underRep;
      decommissionOnlyReplicas = onlyRep;
      underReplicatedInOpenFiles = underConstruction;
    }

    synchronized int getUnderReplicatedBlocks() {
      return underReplicatedBlocks;
    }

    synchronized int getDecommissionOnlyReplicas() {
      return decommissionOnlyReplicas;
    }

    synchronized int getUnderReplicatedInOpenFiles() {
      return underReplicatedInOpenFiles;
    }
  } // End of class DecommissioningStatus
  
  /**
   * Set the flag to indicate if this datanode is disallowed from communicating
   * with the namenode.
   */
  void setDisallowed(boolean flag) {
    disallowed = flag;
  }
  
  boolean isDisallowed() {
    return disallowed;
  }
}
