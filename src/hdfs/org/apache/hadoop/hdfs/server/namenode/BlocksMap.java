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
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.server.namenode.INodeRaidStorage.RaidBlockInfo;
import org.apache.hadoop.hdfs.util.GSet;
import org.apache.hadoop.hdfs.util.LightWeightGSet;

/**
 * This class maintains the map from a block to its metadata.
 * block's metadata currently includes INode it belongs to and
 * the datanodes that store the block.
 */
public class BlocksMap {

  /**
   * Internal class for block metadata.
   */
  static public class BlockInfo extends Block implements LightWeightGSet.LinkedElement {
    public static final int NO_BLOCK_CHECKSUM = 0;
    
    private INodeFile          inode;

    /** For implementing {@link LightWeightGSet.LinkedElement} interface */
    private LightWeightGSet.LinkedElement nextLinkedElement;

    /**
     * This array contains triplets of references.
     * For each i-th data-node the block belongs to
     * triplets[3*i] is the reference to the DatanodeDescriptor
     * and triplets[3*i+1] and triplets[3*i+2] are references 
     * to the previous and the next blocks, respectively, in the 
     * list of blocks belonging to this data-node.
     */
    private Object[] triplets;
    
    private int checksum;

    BlockInfo(Block blk, int replication) {
      super(blk);
      triplets = new Object[3*replication];
      inode = null;
      checksum = NO_BLOCK_CHECKSUM;
    }
    
    public BlockInfo() {
      super();
    }
    
    public void setChecksum(int checksum) {
      this.checksum = checksum;
    }
    
    public int getChecksum() {
      return checksum;
    }
    
    public void setReplication(int replication) {
      triplets = new Object[3*replication];
    }
    
    INodeFile getINode() {
      return inode;
    }

    public void setINode(INodeFile inode) {
      this.inode = inode;
    }     

    DatanodeDescriptor getDatanode(int index) {
      DatanodeDescriptor node = (DatanodeDescriptor)triplets[index*3];
      return node;
    }

    BlockInfo getPrevious(int index) {
      BlockInfo info = (BlockInfo)triplets[index*3+1];
      return info;
    }

    BlockInfo getNext(int index) {
      BlockInfo info = (BlockInfo)triplets[index*3+2];
      return info;
    }

    void setDatanode(int index, DatanodeDescriptor node) {
      triplets[index*3] = node;
    }

    void setPrevious(int index, BlockInfo to) {
      triplets[index*3+1] = to;
    }

    void setNext(int index, BlockInfo to) {
      triplets[index*3+2] = to;
    }

    BlockInfo getSetPrevious(int index, BlockInfo to) {
      BlockInfo info = (BlockInfo)triplets[index*3+1];
      triplets[index*3+1] = to;
      return info;
    }

    BlockInfo getSetNext(int index, BlockInfo to) {
      BlockInfo info = (BlockInfo)triplets[index*3+2];
      triplets[index*3+2] = to;
      return info;
    }

    private int getCapacity() {
      assert this.triplets != null : "BlockInfo is not initialized";
      assert triplets.length % 3 == 0 : "Malformed BlockInfo";
      return triplets.length / 3;
    }
    
    /**
     * Please be cautious when trying to implement this function, it doesn't write checksum whereas
     * checksum is written separately in FSEditsLog and FSImage
     */
    @Override
    public void write(DataOutput out) throws IOException {
      super.write(out);
    }

    /**
     * Please be cautious when trying to implement this function, it doesn't write checksum whereas
     * checksum is read separately in FSEditsLog and FSImage
     */
    @Override
    public void readFields(DataInput in) throws IOException {
      super.readFields(in);
    }

    /**
     * Ensure that there is enough  space to include num more triplets.
     *      * @return first free triplet index.
     */
    private int ensureCapacity(int num) {
      assert this.triplets != null : "BlockInfo is not initialized";
      int last = numNodes();
      if(triplets.length >= (last+num)*3)
        return last;
      /* Not enough space left. Create a new array. Should normally 
       * happen only when replication is manually increased by the user. */
      Object[] old = triplets;
      triplets = new Object[(last+num)*3];
      for(int i=0; i < last*3; i++) {
        triplets[i] = old[i];
      }
      return last;
    }

    /**
     * Count the number of data-nodes the block belongs to.
     */
    int numNodes() {
      assert this.triplets != null : "BlockInfo is not initialized";
      assert triplets.length % 3 == 0 : "Malformed BlockInfo";
      for(int idx = getCapacity()-1; idx >= 0; idx--) {
        if(getDatanode(idx) != null)
          return idx+1;
      }
      return 0;
    }

    /**
     * Add data-node this block belongs to.
     */
    int addNode(DatanodeDescriptor node) {
      int lastNode = findDatanode(node);
      if(lastNode >= 0) // the node is already there
        return -1;
      // find the last null node
      lastNode = ensureCapacity(1);
      setDatanode(lastNode, node);
      setNext(lastNode, null);
      setPrevious(lastNode, null);
      return lastNode;
    }

    /**
     * Remove data-node from the block.
     */
    boolean removeNode(DatanodeDescriptor node) {
      int dnIndex = findDatanode(node);
      if(dnIndex < 0) // the node is not found
        return false;
      assert getPrevious(dnIndex) == null && getNext(dnIndex) == null : 
        "Block is still in the list and must be removed first.";
      // find the last not null node
      int lastNode = numNodes()-1; 
      // replace current node triplet by the lastNode one 
      setDatanode(dnIndex, getDatanode(lastNode));
      setNext(dnIndex, getNext(lastNode)); 
      setPrevious(dnIndex, getPrevious(lastNode)); 
      // set the last triplet to null
      setDatanode(lastNode, null);
      setNext(lastNode, null); 
      setPrevious(lastNode, null); 
      return true;
    }

    /**
     * Find specified DatanodeDescriptor.
     * @param dn
     * @return index or -1 if not found.
     */
    int findDatanode(DatanodeDescriptor dn) {
      int len = getCapacity();
      for(int idx = 0; idx < len; idx++) {
        DatanodeDescriptor cur = getDatanode(idx);
        if(cur == dn)
          return idx;
        if(cur == null)
          break;
      }
      return -1;
    }

    /**
     * Insert this block into the head of the list of blocks 
     * related to the specified DatanodeDescriptor.
     * If the head is null then form a new list.
     * @return current block as the new head of the list.
     */
    BlockInfo listInsert(BlockInfo head, DatanodeDescriptor dn, int dnIndex) {
      if(dnIndex < 0){
        dnIndex = this.findDatanode(dn);
      }
      assert dnIndex >= 0 : "Data node is not found: current";
      assert getPrevious(dnIndex) == null && getNext(dnIndex) == null : 
              "Block is already in the list and cannot be inserted.";
      this.setPrevious(dnIndex, null);
      this.setNext(dnIndex, head);
      if(head != null)
        head.setPrevious(head.findDatanode(dn), this);
      return this;
    }

    /**
     * Remove this block from the list of blocks 
     * related to the specified DatanodeDescriptor.
     * If this block is the head of the list then return the next block as 
     * the new head.
     * @return the new head of the list or null if the list becomes
     * empy after deletion.
     */
    BlockInfo listRemove(BlockInfo head, DatanodeDescriptor dn) {
      if(head == null)
        return null;
      int dnIndex = this.findDatanode(dn);
      if(dnIndex < 0) // this block is not on the data-node list
        return head;

      BlockInfo next = this.getNext(dnIndex);
      BlockInfo prev = this.getPrevious(dnIndex);
      this.setNext(dnIndex, null);
      this.setPrevious(dnIndex, null);
      if(prev != null)
        prev.setNext(prev.findDatanode(dn), next);
      if(next != null)
        next.setPrevious(next.findDatanode(dn), prev);
      if(this == head)  // removing the head
        head = next;
      return head;
    }

    int listCount(DatanodeDescriptor dn) {
      int count = 0;
      for(BlockInfo cur = this; cur != null;
            cur = cur.getNext(cur.findDatanode(dn)))
        count++;
      return count;
    }

    boolean listIsConsistent(DatanodeDescriptor dn) {
      // going forward
      int count = 0;
      BlockInfo next, nextPrev;
      BlockInfo cur = this;
      while(cur != null) {
        next = cur.getNext(cur.findDatanode(dn));
        if(next != null) {
          nextPrev = next.getPrevious(next.findDatanode(dn));
          if(cur != nextPrev) {
            System.out.println("Inconsistent list: cur->next->prev != cur");
            return false;
          }
        }
        cur = next;
        count++;
      }
      return true;
    }

    @Override
    public LightWeightGSet.LinkedElement getNext() {
      return nextLinkedElement;
    }

    @Override
    public void setNext(LightWeightGSet.LinkedElement next) {
      this.nextLinkedElement = next;
    }
  }

  private static class NodeIterator implements Iterator<DatanodeDescriptor> {
    private BlockInfo blockInfo;
    private int nextIdx = 0;
      
    NodeIterator(BlockInfo blkInfo) {
      this.blockInfo = blkInfo;
    }

    @Override
    public boolean hasNext() {
      return blockInfo != null && nextIdx < blockInfo.getCapacity()
              && blockInfo.getDatanode(nextIdx) != null;
    }

    @Override
    public DatanodeDescriptor next() {
      return blockInfo.getDatanode(nextIdx++);
    }

    @Override
    public void remove()  {
      throw new UnsupportedOperationException("Sorry. can't remove.");
    }
  }

  /** Constant {@link LightWeightGSet} capacity. */
  private final int capacity;
  
  private GSet<Block, BlockInfo> blocks;
  private final FSNamesystem ns;

  BlocksMap(int initialCapacity, float loadFactor, FSNamesystem ns) {
    // Use 2% of total memory
    this.capacity = LightWeightGSet.computeCapacity(2.0, "BlocksMap");
    this.blocks = new LightWeightGSet<Block, BlockInfo>(capacity);
    this.ns = ns;
  }

  public void close() {
    blocks = null;
  }

  /**
   * All removals from the blocks map goes through this function.
   * 
   * @param b
   *          block to be removed
   * @param decrementSafeBlockCount
   *          whether need to decrement safe block count when needed
   * @return the {@link BlockInfo} for the removed block
   */
  private BlockInfo removeBlockFromMap(Block b) {
    if (b == null) {
      return null;
    }
     ns.decrementSafeBlockCountForBlockRemoval(b);
    return blocks.remove(b);
  }

  /**
   * Add BlockInfo if mapping does not exist.
   */
  private BlockInfo checkBlockInfo(Block b, int replication) {
    // regular update always checks if the block is already in the map!
    return checkBlockInfo(b, replication, true);
  }
  
  private BlockInfo checkBlockInfo(Block b, int replication,
      boolean checkExistence) {
    // when loading regular files, we do not need to check if the blocks are
    // already in the map - just allocate and insert
    // for hardlink files, and outside of loading, we need to always check
    BlockInfo info = checkExistence ? blocks.get(b) : null;
    if (info == null) {
      if (b instanceof RaidBlockInfo) {
        info = new RaidBlockInfo(b, replication, ((RaidBlockInfo)b).getIndex());
      } else {
        info = new BlockInfo(b, replication);
      }
      blocks.put(info);
    }
    return info;
  }
  
  INodeFile getINode(Block b) {
    BlockInfo info = blocks.get(b);
    return (info != null) ? info.inode : null;
  }

  BlockInfo getBlockInfo(Block b) {
    return blocks.get(b);
  }
  
  /**
   * Add block b belonging to the specified file inode to the map
   */
  BlockInfo addINode(Block b, INodeFile iNode, short replication) {
    BlockInfo info = checkBlockInfo(b, replication);
    info.inode = iNode;
    return info;
  }
  
  /**
   * Add block b belonging to the specified file inode to the map.
   * Does not check for block existence, for non-hardlinked files.
   */
  BlockInfo addINodeForLoading(Block b, INodeFile iNode) {
    // allocate new block when loading the image
    // for hardlinked files, we need to check if the blocks are already there
    BlockInfo info = checkBlockInfo(b, iNode.getReplication(),
        iNode.isHardlinkFile());
    info.inode = iNode;
    return info;
  }

  /**
   * Add block b belonging to the specified file inode to the map, this
   * overwrites the map with the new block information.
   */
  public BlockInfo updateINode(BlockInfo oldBlock, Block newBlock, INodeFile iNode,
      short replication, boolean forceUpdate) throws IOException {
    // If the old block is not same as the new block, probably the GS was
    // bumped up, hence update the block with new GS/size.
    // If forceUpdate is true, we will always remove the old block and 
    // update with new block, it's used by raid
    List<DatanodeDescriptor> locations = null;
    if (oldBlock != null && (!oldBlock.equals(newBlock) || forceUpdate)) {
      if (oldBlock.getBlockId() != newBlock.getBlockId()) {
        throw new IOException("block ids don't match : " + oldBlock + ", "
            + newBlock);
      }
      if (forceUpdate) {
        // save locations of the old block
        locations = new ArrayList<DatanodeDescriptor>();
        for (int i=0; i<oldBlock.numNodes(); i++) {
          locations.add(oldBlock.getDatanode(i));
        }
      } else {
        if (!iNode.isUnderConstruction()) {
          throw new IOException(
              "Try to update generation of a finalized block old block: "
                  + oldBlock + ", new block: " + newBlock);
        }
      }
      removeBlock(oldBlock);
    }
    BlockInfo info = checkBlockInfo(newBlock, replication);
    info.set(newBlock.getBlockId(), newBlock.getNumBytes(), newBlock.getGenerationStamp());
    info.inode = iNode;
    if (locations != null) {
      // add back the locations if needed
      if (locations != null) {
        for (DatanodeDescriptor d : locations) {
          d.addBlock(info);
        }
      }
    }
    return info;
  }

  /**
   * Remove INode reference from block b.
   * If it does not belong to any file and data-nodes,
   * then remove the block from the block map.
   */
  void removeINode(Block b) {
    BlockInfo info = blocks.get(b);
    if (info != null) {
      info.inode = null;
      if (info.getDatanode(0) == null) {  // no datanodes left
        removeBlockFromMap(b); // remove block from the map
      }
    }
  }

  /**
   * Remove the block from the block map;
   * remove it from all data-node lists it belongs to;
   * and remove all data-node locations associated with the block.
   */
  void removeBlock(Block block) {
    BlockInfo blockInfo = removeBlockFromMap(block);
    if (blockInfo == null)
      return;
    blockInfo.inode = null;
    for(int idx = blockInfo.numNodes()-1; idx >= 0; idx--) {
      DatanodeDescriptor dn = blockInfo.getDatanode(idx);
      dn.removeBlock(blockInfo); // remove from the list and wipe the location
    }
  }

  /** Returns the block object it it exists in the map. */
  BlockInfo getStoredBlock(Block b) {
    return blocks.get(b);
  }

  /** Return the block object without matching against generation stamp. */
  BlockInfo getStoredBlockWithoutMatchingGS(Block b) {
    return blocks.get(new Block(b.getBlockId()));
  }

  /** Returned Iterator does not support. */
  Iterator<DatanodeDescriptor> nodeIterator(Block b) {
    return new NodeIterator(blocks.get(b));
  }

  /** counts number of containing nodes. Better than using iterator. */
  int numNodes(Block b) {
    BlockInfo info = blocks.get(b);
    return info == null ? 0 : info.numNodes();
  }

  /** returns true if the node does not already exists and is added.
   * false if the node already exists.*/
  boolean addNode(Block b, DatanodeDescriptor node, int replication) {
    // insert into the map if not there yet
    BlockInfo info = checkBlockInfo(b, replication);
    // add block to the data-node list and the node to the block info
    return node.addBlock(info);
  }

  /**
   * Remove data-node reference from the block.
   * Remove the block from the block map
   * only if it does not belong to any file and data-nodes.
   */
  boolean removeNode(Block b, DatanodeDescriptor node) {
    BlockInfo info = blocks.get(b);
    if (info == null)
      return false;

    // remove block from the data-node list and the node from the block info
    boolean removed = node.removeBlock(info);

    if (info.getDatanode(0) == null     // no datanodes left
              && info.inode == null) {  // does not belong to a file
      removeBlockFromMap(b); // remove block from the map
    }
    return removed;
  }

  int size() {
    return (blocks == null) ? 0 : blocks.size();
  }

  Iterable<BlockInfo> getBlocks() {
    return blocks;
  }
  
  /**
   * Get a list of shard iterators. Each iterator will travers only a part
   * of the blocks map.
   * @param numShards desired number of shards
   * @return list of iterators (size might be smaller than
   *          numShards if blocks map has fewer buckets)
   */
  List<Iterator<BlockInfo>> getBlocksIterarors(int numShards) {
    List<Iterator<BlockInfo>> iterators = new ArrayList<Iterator<BlockInfo>>();
    if (numShards <= 0) {
      throw new IllegalArgumentException("Number of shards must be greater than 0");
    }
    for (int i = 0; i < numShards; i++) {
      Iterator<BlockInfo> iterator = blocks.shardIterator(i, numShards);
      if (iterator != null) {
        iterators.add(iterator);
      }
    }   
    return iterators;
  }
  /**
   * Check if the block exists in map
   */
  boolean contains(Block block) {
    return blocks.contains(block);
  }
  
  /**
   * Check if the replica at the given datanode exists in map
   */
  boolean contains(Block block, DatanodeDescriptor datanode) {
    BlockInfo info = blocks.get(block);
    if (info == null)
      return false;
    
    if (-1 == info.findDatanode(datanode))
      return false;
    
    return true;
  }
  
  /** Get the capacity of the HashMap that stores blocks */
  public int getCapacity() {
    return capacity;
  }
}
