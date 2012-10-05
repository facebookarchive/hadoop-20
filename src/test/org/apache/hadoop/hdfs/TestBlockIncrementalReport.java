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
package org.apache.hadoop.hdfs;

import java.util.Random;

import junit.framework.TestCase;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.BlockListAsLongs;
import org.apache.hadoop.hdfs.server.protocol.IncrementalBlockReport;
import org.apache.hadoop.hdfs.server.protocol.ReceivedBlockInfo;
import org.apache.hadoop.hdfs.util.LightWeightBitSet;

public class TestBlockIncrementalReport extends TestCase {

  private static final Log LOG = LogFactory
      .getLog("org.apache.hadoop.hdfs.TestBlockIncrementalReport");
  private Block[] blocks;
  private boolean[] isBlockReceived;
  private int numReceived;

  private final int NUM = 100;
  private Random rand;

  protected void setUp() {
    rand = new Random(System.currentTimeMillis());
    blocks = new Block[NUM];
    isBlockReceived = new boolean[NUM];
    numReceived = 0;

    for (int i = 0; i < NUM; i++) {
      if (!rand.nextBoolean()) {
        blocks[i] = new Block(rand.nextLong(), rand.nextLong(), rand.nextLong());
        isBlockReceived[i] = false;
      } else {
        ReceivedBlockInfo bri = new ReceivedBlockInfo(new Block(
            rand.nextLong(), rand.nextLong(), rand.nextLong()), "delHints" + i);
        blocks[i] = bri;
        isBlockReceived[i] = true;
        numReceived++;
      }
    }
  }

  public void testEmpty() {
    LOG.info("Test empty");

    IncrementalBlockReport ibrSource = new IncrementalBlockReport(new Block[0]);
    long[] sourceBlocks = ibrSource.getBlockReportInLongs();
    long[] sourceHintsMap = ibrSource.getHintsMap();
    String[] sourceHints = ibrSource.getHints();

    assertEquals(0, sourceBlocks.length);
    assertEquals(0, sourceHintsMap.length);
    assertEquals(0, sourceHints.length);

    // initialize the block report on the NN side

    assertFalse(ibrSource.hasNext());

    LOG.info("Test empty -- DONE");
  }

  public void testReport() {
    LOG.info("Test multi basic");

    IncrementalBlockReport ibrSource = new IncrementalBlockReport(blocks);
    long[] sourceBlocks = ibrSource.getBlockReportInLongs();
    long[] sourceHintsMap = ibrSource.getHintsMap();
    String[] sourceHints = ibrSource.getHints();

    // check if the arrays are of correct length
    assertEquals(NUM * BlockListAsLongs.LONGS_PER_BLOCK, sourceBlocks.length);
    assertEquals(LightWeightBitSet.getBitArraySize(NUM), sourceHintsMap.length);
    assertEquals(numReceived, LightWeightBitSet.cardinality(sourceHintsMap));
    assertEquals(numReceived, sourceHints.length);

    // check the contents
    int i = 0;
    String delHint = null;
    Block blk = new Block();

    while (ibrSource.hasNext()) {
      delHint = ibrSource.getNext(blk);
      // check if received/deleted
      if (isBlockReceived[i]) {
        ReceivedBlockInfo source = (ReceivedBlockInfo) blocks[i];
        // check if delHints match
        assertEquals(source.getDelHints(), delHint);
      }
      // check if id/size/genstamp match 
      // discard delHint (checked above)
      assertEquals(new Block(blocks[i]), blk);
      i++;
    }
    
    //check the length function
    assertEquals(i, ibrSource.getLength());

    // check if we've got all the blocks
    assertEquals(NUM, i);

    LOG.info("Test multi - DONE");
  }

}
