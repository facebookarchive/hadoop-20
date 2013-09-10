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
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.server.common.GenerationStamp;
import org.apache.hadoop.hdfs.server.namenode.BlocksMap.BlockInfo;

import static org.junit.Assert.*;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;

public class TestBlocksMap {
  
  private class MyNamesystem extends FSNamesystem {
    @Override
    void decrementSafeBlockCountForBlockRemoval(Block b) {
    }
  }

  private static final Log LOG = LogFactory.getLog(TestBlocksMap.class
      .getName());

  INodeFile iNode;
  Set<Block> blockList;
  BlocksMap map;
  static Runtime runtime;

  @BeforeClass
  public static void setUpBeforeClass() {
    // simulate less memory to create fewer buckets for blocks map
    runtime = spy(Runtime.getRuntime());
    FSEditLog.setRuntimeForTesting(runtime);
  }

  private void insertBlocks(int numBlocks, boolean underConstruction) {
    Random r = new Random();
    map = new BlocksMap(1000, 0.75f, new MyNamesystem());
    Set<Long> ids = new HashSet<Long>(numBlocks);

    blockList = new HashSet<Block>(numBlocks);
    if (underConstruction) {
      INodeFile node = new INodeFile();
      iNode = new INodeFileUnderConstruction(node.getId(),
          node.getLocalNameBytes(), (short) 2,
          node.getModificationTime(), 0, node.getPreferredBlockSize(),
          node.getBlocks(), node.getPermissionStatus(), "", "", null);
    } else {
      iNode= new INodeFile();
    }
    int inserted = 0;

    while (inserted < numBlocks) {
      long id;
      while (ids.contains((id = r.nextLong())))
        ;
      ids.add(id);
      Block b = new Block(id, 0, GenerationStamp.FIRST_VALID_STAMP);
      blockList.add(b);
      BlockInfo info = map.addINode(b, iNode, iNode.getReplication());
      
      // create 2 datanode descriptors
      DatanodeDescriptor dd; 
      
      dd = new DatanodeDescriptor();
      dd.addBlock(info);
      dd = new DatanodeDescriptor();
      dd.addBlock(info);
      
      inserted++;
    }
  }
  
  @Test
  public void testBlockInfoPlaceUpdate() throws IOException {
    insertBlocks(100, true);

    for (Block b : blockList) {
      BlockInfo oldBlock = map.getBlockInfo(b);
      
      // get current locations
      DatanodeDescriptor loc0 = oldBlock.getDatanode(0);
      DatanodeDescriptor loc1 = oldBlock.getDatanode(1);
      assertNotNull(loc0);
      assertNotNull(loc1);
      assertEquals(2, oldBlock.numNodes());

      // prepare new block with different size and GS
      long newGS = b.getGenerationStamp() + 1;
      b.setGenerationStamp(newGS);
      b.setNumBytes(b.getNumBytes() + 10);
      BlockInfo newBlock = new BlockInfo(b, iNode.getReplication());

      LOG.info("Updating block: " + oldBlock + " to: " + newBlock);
      // do update
      newBlock = map.updateINode(oldBlock, newBlock, iNode, 
          iNode.getReplication(), false);
      // preserved new generation stamp
      assertEquals(map.getStoredBlockWithoutMatchingGS(newBlock)
          .getGenerationStamp(), newGS);
      // check locations
      assertEquals(0, newBlock.numNodes());

      // when id is mismatched, the block should not be updated
      newBlock.setBlockId(newBlock.getBlockId() + 1);
      try {
        map.updateINode(oldBlock, newBlock, iNode, iNode.getReplication(), false);
        fail("Should fail here");
      } catch (IOException e) {
        LOG.info("Can't update " + oldBlock + " to: " + newBlock + " "
            + e.getMessage());
      }
    }
  }

  @Test
  public void testFullIteratorWithEmptyBuckets() {
    // number of buckets is more than blocks
    testFullIterator(5000, 10 * 1024 * 1024);
  }

  @Test
  public void testFullIteratorWithNoEmptyBuckets() {
    // number of buckets is much lower than blocks
    // little chances that there will be empty buckets
    testFullIterator(100000, 1 * 1024 * 1024);
  }

  @Test
  public void testShardedIteratorWithEmptyBuckets() {
    // number of buckets is more than blocks
    testShardedIterator(5000, 10 * 1024 * 1024);
  }

  @Test
  public void testShardedIteratorWithNoEmptyBuckets() {
    // number of buckets is much lower than blocks
    // little chances that there will be empty buckets
    testShardedIterator(100000, 1 * 1024 * 1024);
  }

  @SuppressWarnings("unused")
  @Test
  public void testEmpty() {
    // test correct behaviour when the map is empty

    doReturn(new Long(1 * 1024 * 1024)).when(runtime).maxMemory();
    insertBlocks(0, false);

    for (Block b : map.getBlocks()) {
      fail("There should be no blocks in the map");
    }

    // get sharded iterators
    List<Iterator<BlockInfo>> iterators = map.getBlocksIterarors(16);
    assertEquals(16, iterators.size());
    for (Iterator<BlockInfo> iterator : iterators) {
      LOG.info("Next sharded iterator");
      while (iterator.hasNext()) {
        fail("There should be no block in any iterator");
      }
    }

  }

  private void testFullIterator(int numBlocks, long memSize) {

    // make the map have very few buckets
    doReturn(new Long(memSize)).when(runtime).maxMemory();

    insertBlocks(numBlocks, false);
    assertEquals(map.size(), numBlocks);
    assertEquals(blockList.size(), numBlocks);

    LOG.info("Starting iteration...");
    long start = System.currentTimeMillis();
    Set<Block> iteratedBlocks = new HashSet<Block>();
    for (Block b : map.getBlocks()) {
      // no block should be seen more than once
      assertFalse(iteratedBlocks.contains(b));
      iteratedBlocks.add(b);
    }
    long stop = System.currentTimeMillis();
    
    // each block should be seen once
    assertEquals(blockList, iteratedBlocks);
    LOG.info("Iterated : " + numBlocks + " in: " + (stop - start));
  }

  private void testShardedIterator(int numBlocks, long memSize) {

    // make the map have very few buckets
    doReturn(new Long(memSize)).when(runtime).maxMemory();

    insertBlocks(numBlocks, false);
    assertEquals(map.size(), numBlocks);
    assertEquals(blockList.size(), numBlocks);

    LOG.info("Starting iteration...");
    long start = System.currentTimeMillis();
    Set<Block> iteratedBlocks = new HashSet<Block>();

    // get sharded iterators
    List<Iterator<BlockInfo>> iterators = map.getBlocksIterarors(16);
    assertEquals(16, iterators.size());
    for (Iterator<BlockInfo> iterator : iterators) {
      LOG.info("Next sharded iterator");
      while (iterator.hasNext()) {
        Block b = new Block(iterator.next());
        // no block should be seen more than once
        assertFalse(iteratedBlocks.contains(b));
        iteratedBlocks.add(b);
      }
    }

    long stop = System.currentTimeMillis();
    
    // each block should be seen once
    assertEquals(blockList, iteratedBlocks);
    LOG.info("Iterated : " + numBlocks + " in: " + (stop - start));
  }
}
