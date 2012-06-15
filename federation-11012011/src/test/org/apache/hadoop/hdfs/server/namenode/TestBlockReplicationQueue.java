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

import java.util.*;
import junit.framework.TestCase;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.server.common.GenerationStamp;
import org.apache.hadoop.hdfs.server.namenode.BlocksMap.BlockInfo;
import org.apache.hadoop.hdfs.util.LightWeightLinkedSet;

public class TestBlockReplicationQueue extends TestCase {

  private final int MAX_BLOCKS = 100000;
  private int milisecondsToSleep = 1 * 1000;
  private static final Log LOG = LogFactory
      .getLog("org.apache.hadoop.hdfs.TestBlockReplicationQueue");
  private HashSet<Block> blockList = new HashSet<Block>(MAX_BLOCKS);
  private Set<Block> queue;
  private LightWeightLinkedSet<Block> queueL;
  Runtime r = Runtime.getRuntime();

  protected void setUp(){
    blockList.clear();
    LOG.info("Generating blocks...");
    for (int i = 0; i < MAX_BLOCKS; i++) {
      blockList.add(new BlockInfo(new Block(i, 0,
          GenerationStamp.FIRST_VALID_STAMP), 3));
    }
  }

  private void freeAndSleep(long m){
    r.freeMemory();
    try{
      Thread.sleep(m);
    } catch (Exception e) {
    }
  }

  private void insertBlocks(boolean l){
    LOG.info("Insert blocks...");
    freeAndSleep(milisecondsToSleep);
    long freeBefore = r.freeMemory();
    long start = System.currentTimeMillis();
    for (Block b : blockList) {
      if(!l)
        assertTrue(queue.add(b));
      else
        assertTrue(queueL.add(b));
    }
    long stop = System.currentTimeMillis();
    freeAndSleep(milisecondsToSleep);
    long freeAfter = r.freeMemory();
    LOG.info("Insert blocks... DONE. TIME: "+(stop-start));
    LOG.info("Approximate structure size: " + (freeBefore-freeAfter));
  }

  private void containsBlocks(boolean l){
    LOG.info("Contains blocks...");
    long start = System.currentTimeMillis();
    for (Block b : blockList) {
      if(!l)
        assertTrue(queue.contains(b));
      else
        assertTrue(queueL.contains(b));
    }
    long stop = System.currentTimeMillis();
    LOG.info("Contains blocks... DONE. TIME: "+(stop-start));
  }

  private void removeBlocks(boolean l){
    LOG.info("Remove blocks...");
    long start = System.currentTimeMillis();
    for (Block b : blockList) {
      if(!l)
        assertTrue(queue.remove(b));
      else
        assertTrue(queueL.remove(b));
    }
    long stop = System.currentTimeMillis();
    LOG.info("Remove blocks... DONE. TIME: "+(stop-start));
  }

  public void testHashSetBenchmark() {
    LOG.info("Test HASH_SET");
    queue = new HashSet<Block>();
    insertBlocks(false);
    containsBlocks(false);
    removeBlocks(false);
  }

  public void testLinkedHashSetBenchmark() {
    LOG.info("Test LINKED_HASH_SET");
    queue = new LinkedHashSet<Block>();
    insertBlocks(false);
    containsBlocks(false);
    removeBlocks(false);
  }

  public void testTreeSetBenchmark() {
    LOG.info("Test TREE_SET");
    queue = new TreeSet<Block>();
    insertBlocks(false);
    containsBlocks(false);
    removeBlocks(false);
  }

  public void testLightWeightLinkedSetBenchmark() {
    LOG.info("Test LIGHTWEIGHT_LINKED_SET");
    queueL = new LightWeightLinkedSet<Block>();
    insertBlocks(true);
    containsBlocks(true);
    removeBlocks(true);
  }
}
