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

import junit.framework.TestCase;

import java.util.ArrayList;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.server.common.GenerationStamp;
import org.apache.hadoop.hdfs.server.namenode.BlocksMap.BlockInfo;
import org.apache.hadoop.hdfs.server.namenode.DatanodeDescriptor.DatanodeIndex;

/**
 * This class provides tests for BlockInfo class, which is used in BlocksMap.
 * The test covers BlockList.listMoveToHead, used for faster block report
 * processing in DatanodeDescriptor.reportDiff.
 *
 * @author tomasz
 *
 */

public class TestBlockInfo extends TestCase {

  private static final Log LOG = LogFactory
      .getLog("org.apache.hadoop.hdfs.TestBlockInfo");

  public void testBlockListMoveToHead() throws Exception {
    LOG.info("BlockInfo moveToHead tests...");

    final int MAX_BLOCKS = 10;

    DatanodeDescriptor dd = new DatanodeDescriptor();
    ArrayList<Block> blockList = new ArrayList<Block>(MAX_BLOCKS);
    ArrayList<BlockInfo> blockInfoList = new ArrayList<BlockInfo>();

    BlockInfo head = null;

    LOG.info("Building block list...");
    for (int i = 0; i < MAX_BLOCKS; i++) {
      blockList.add(new Block(i, 0, GenerationStamp.FIRST_VALID_STAMP));
      blockInfoList.add(new BlockInfo(blockList.get(i), 3));
      blockInfoList.get(i).addNode(dd);
      head = blockInfoList.get(i).listInsert(head, dd, -1);

      // index of the datanode should be 0
      assertEquals("Find datanode should be 0", 0, blockInfoList.get(i)
          .findDatanode(dd));
    }

    // list length should be equal to the number of blocks we inserted
    LOG.info("Checking list length...");
    assertEquals("Length should be MEX_BLOCK", MAX_BLOCKS, head.listCount(dd));

    DatanodeIndex ind = new DatanodeIndex();
    ind.headIndex = head.findDatanode(dd);

    LOG.info("Moving each block to the head of the list...");
    for (int i = 0; i < MAX_BLOCKS; i++) {
      ind.currentIndex = blockInfoList.get(i).findDatanode(dd);
      head = dd.listMoveToHead(blockInfoList.get(i), head, ind);
      // the moved element must be at the head of the list
      assertEquals("Block should be at the head of the list now.",
          blockInfoList.get(i), head);
      // list length must not change
      assertEquals("List size should not change", MAX_BLOCKS,
          head.listCount(dd));
    }

    // move head of the list to the head - this should not change the list
    LOG.info("Moving head to the head...");
    BlockInfo temp = head;
    ind.currentIndex = 0;
    ind.headIndex = 0;
    head = dd.listMoveToHead(head, head, ind);
    assertEquals(
        "Moving head to the head of the list shopuld not change the list",
        temp, head);

    // check all elements of the list against the original blockInfoList
    LOG.info("Checking elements of the list...");
    BlockInfo it = head;
    assertNotNull("Head should not be null", head);
    int c = MAX_BLOCKS - 1;
    while (it != null) {
      assertEquals("Expected element is not on the list",
          blockInfoList.get(c--), it);
      it = it.getNext(0);
    }

    ind.headIndex = head.findDatanode(dd);

    LOG.info("Moving random blocks to the head of the list...");
    Random rand = new Random();
    for (int i = 0; i < MAX_BLOCKS; i++) {
      int j = rand.nextInt(MAX_BLOCKS);
      ind.currentIndex = blockInfoList.get(j).findDatanode(dd);
      head = dd.listMoveToHead(blockInfoList.get(j), head, ind);
      // the moved element must be at the head of the list
      assertEquals("Block should be at the head of the list now.",
          blockInfoList.get(j), head);
      // list length must not change
      assertEquals("List size should not change", MAX_BLOCKS,
          head.listCount(dd));
    }

  }

  private void printContents(BlockInfo head) {
    BlockInfo it = head;
    while (it != null) {
      LOG.info("Block: " + it.toString());
      it = it.getNext(0);
    }
  }

}
