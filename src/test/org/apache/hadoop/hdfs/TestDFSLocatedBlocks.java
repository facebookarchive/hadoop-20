/*
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

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;

import static org.apache.hadoop.hdfs.DFSClient.checkBlockRange;
import static org.junit.Assert.*;

import org.junit.Test;

public class TestDFSLocatedBlocks {

  private static final Log LOG = LogFactory.getLog(
      TestDFSLocatedBlocks.class.getName());

  private Random rand = new Random(12983719287L);

  private static final int OFFSET_RANGE = 100;

  private static final long[][] BLOCK_RANGES = new long[][] {
    // These arrays define blocks, e.g. the following corresponds to four
    // blocks with offsets 100, 200, 500, 700 and sizes 100, 300, 200, 300
    new long[] { 100, 200, 500, 700, 1000 },
    new long[] { 150, 750, 1,
                 200, 750, 0,
                 199, 701, 1 },
    new long[] { 50, 120, 1000, 5000, 5600 },
    new long[] { 50, 5600, 1,
                 51, 5599, 1,
                 120, 5600, 0,
                 119, 5600, 1,
                 50, 5000, 0,
                 50, 5001, 1 }
  };

  /** The number of blocks for an exhaustive test with 2^(2N) complexity. */
  private static final int EXHAUSTIVE_NUM_BLOCKS = 8;
  private static final int NUM_BLOCK_SUBSETS = (1 << EXHAUSTIVE_NUM_BLOCKS);

  private static final int NUM_BLOCKS = 100;
  private static final int BLOCKS_IN_SUBRANGE = 5;
  private static final int NUM_THREADS = 10;

  private static final int NUM_ITERATIONS = 50;
  private static final int NUM_INSERTS = 1000;

  static {
    // Validate the test set.
    assert BLOCK_RANGES.length % 2 == 0;
    for (int i = 0; i < BLOCK_RANGES.length / 2; ++i) {
      long[] offsets = BLOCK_RANGES[i * 2];
      assert offsets.length > 0;
      for (int j = 1; j < offsets.length; ++j)
        assert offsets[j - 1] < offsets[j];

      long[] testCases = BLOCK_RANGES[i * 2 + 1];
      assert testCases.length % 3 == 0;
      for (int j = 0; j < testCases.length / 3; ++j) {
        assert testCases[j * 3] < testCases[j * 3 + 1];
        long correctAnswer = testCases[j * 3 + 2];
        assert correctAnswer == 0 || correctAnswer == 1;
      }
    }
  }

  private static List<LocatedBlock> createBlockRange(long[] offsets,
      long blockSizeDelta) {
    List<LocatedBlock> blocks =
        new ArrayList<LocatedBlock>(offsets.length - 1);
    File f = new File("/no/such/dir/blk_0");
    for (int i = 0; i < offsets.length - 1; ++i) {
      LocatedBlock blk = new LocatedBlock(
          new Block(f, offsets[i + 1] - offsets[i] + blockSizeDelta, 0),
          null, offsets[i]);
      blocks.add(blk);
    }
    return blocks;
  }

  private List<LocatedBlock> createRandomBlockRange(int n,
      long blockSizeDelta) {
    if (n == 0)
      return new ArrayList<LocatedBlock>();

    long[] blockBoundaries = randomBlockBoundaries(n, blockSizeDelta);
    return createBlockRange(blockBoundaries, blockSizeDelta);
  }

  private long[] randomBlockBoundaries(int n, long blockSizeDelta) {
    long[] blockBoundaries = new long[n + 1];
    blockBoundaries[0] = 0;
    for (int i = 1; i < n + 1; ++i) {
      long blockSize = rand.nextInt(OFFSET_RANGE - 1) + 1;
      if (blockSize + blockSizeDelta <= 0) {
        // Make sure we don't end up with zero or negative-size blocks.
        blockSize = 1 + Math.abs(blockSizeDelta);
      }
      blockBoundaries[i] = blockBoundaries[i - 1] + blockSize;
    }
    return blockBoundaries;
  }

  private static long getLastBlockEnd(List<LocatedBlock> blocks) {
    LocatedBlock lastBlk = blocks.get(blocks.size() - 1);
    return lastBlk.getStartOffset() + lastBlk.getBlockSize();
  }

  private DFSLocatedBlocks randomDFSLocatedBlocks(int n, long blockSizeDelta) {
    List<LocatedBlock> blocks = createRandomBlockRange(n,
        blockSizeDelta);
    assertEquals(n, blocks.size());
    return createDFSLocatedBlocks(blocks);
  }
  
  private static DFSLocatedBlocks createDFSLocatedBlocks(
      List<LocatedBlock> blocks) {
    return new DFSLocatedBlocks(new LocatedBlocks(
        blocks.size() > 0 ? getLastBlockEnd(blocks) : Integer.MAX_VALUE,
            blocks, false), 60000);
  }

  private static List<LocatedBlock> randomBlockSubrange(Random rand,
      List<LocatedBlock> allBlocks) {
    int nBlocks = rand.nextInt(BLOCKS_IN_SUBRANGE) + 1;
    assertTrue(1 <= nBlocks && nBlocks <= BLOCKS_IN_SUBRANGE);

    int firstBlockIdx = rand.nextInt(NUM_BLOCKS - nBlocks + 1);
    List<LocatedBlock> blockSubrange = new ArrayList<LocatedBlock>(nBlocks);
    for (int i = firstBlockIdx; i < firstBlockIdx + nBlocks; ++i) {
      blockSubrange.add(allBlocks.get(i));
    }

    assertEquals(nBlocks, blockSubrange.size());
    return blockSubrange;
  }

  private static boolean isValidBlockRange(List<LocatedBlock> blockRange,
      long offset, long length) {
    try {
      checkBlockRange(blockRange, offset, length);
      return true;
    } catch (IOException ex) {
      return false;
    }
  }

  @Test
  public void testCheckBlockRange() {
    for (int i = 0; i < BLOCK_RANGES.length / 2; ++i) {
      long[] offsets = BLOCK_RANGES[i * 2];
      long[] testCases = BLOCK_RANGES[i * 2 + 1];
      for (int j = 0; j < testCases.length / 3; ++j) {
        long offset = testCases[j * 3];
        long length = testCases[j * 3 + 1] - testCases[j * 3];
        long correctAnswer = testCases[j * 3 + 2];
        for (int blockSizeDelta = -1; blockSizeDelta <= 1; ++blockSizeDelta) {
          List<LocatedBlock> blockRange = createBlockRange(offsets,
              blockSizeDelta);
          // We expect the block range to be valid only if the test case says
          // so and if we did not mess with the block size.
          assertEquals(blockSizeDelta == 0 && correctAnswer == 1,
              isValidBlockRange(blockRange, offset, length));
        }
      }
    }
  }

  private class InsertRangeThread implements Callable<Boolean>{
    private List<LocatedBlock> allBlocks;
    private DFSLocatedBlocks locatedBlocks;

    public InsertRangeThread(List<LocatedBlock> allBlocks,
        DFSLocatedBlocks dfsLocatedBlocks) {
      this.allBlocks = allBlocks;
      this.locatedBlocks = dfsLocatedBlocks;
    }

    @Override
    public Boolean call() throws Exception {
      for (int i = 0; i < NUM_INSERTS; ++i) {
        List<LocatedBlock> newBlocks = randomBlockSubrange(rand, allBlocks);
        locatedBlocks.insertRange(newBlocks);
        for (LocatedBlock blk : newBlocks) {
          LocatedBlock blockFromArr = locatedBlocks.getBlockContainingOffset(
              blk.getStartOffset());
          assertEquals(blockFromArr.getBlockSize(), blk.getBlockSize());
        }

        List<LocatedBlock> locBlocksCopy =
            locatedBlocks.getLocatedBlocksCopy();
        for (int j = 1; j < locBlocksCopy.size(); ++j) {
          assertTrue(locBlocksCopy.get(j - 1).getStartOffset() <
              locBlocksCopy.get(j).getStartOffset());
        }
      }
      return true;
    }
  }

  @Test
  public void testInsertRangeConcurrent() throws Exception {
    ExecutorService exec = Executors.newFixedThreadPool(NUM_THREADS);
    for (int iteration = 0; iteration < NUM_ITERATIONS; ++iteration) {
      List<LocatedBlock> allBlocks = randomDFSLocatedBlocks(
          NUM_BLOCKS, 0).getLocatedBlocks();
      DFSLocatedBlocks dfsLocatedBlocks = randomDFSLocatedBlocks(0, 0);

      List<Future<Boolean>> results = new ArrayList<Future<Boolean>>();
      for (int iThread = 0; iThread < NUM_THREADS; ++iThread) {
        results.add(exec.submit(new InsertRangeThread(allBlocks,
            dfsLocatedBlocks)));
      }

      for (Future<Boolean> f : results) {
        assertTrue(f.get());
      }
      LOG.info("# located blocks: " +
          dfsLocatedBlocks.getLocatedBlocks().size());
    }
    exec.shutdown();
  }

  private static List<LocatedBlock> blockSubset(
      List<LocatedBlock> allBlocks, int subsetMask) {
    List<LocatedBlock> blkList = new ArrayList<LocatedBlock>();
    for (int i = 0; i < EXHAUSTIVE_NUM_BLOCKS; ++i) {
      if ((subsetMask & 1) != 0)
        blkList.add(allBlocks.get(i));
      subsetMask >>= 1;
    }
    return blkList;
  }

  @Test
  public void testInsertRangesExhaustive() {
    long[] blockBoundaries = randomBlockBoundaries(EXHAUSTIVE_NUM_BLOCKS, 0);
    List<LocatedBlock> allBlocks1 = createBlockRange(blockBoundaries, 0);
    DFSLocatedBlocks dfsAllBlocks1 = createDFSLocatedBlocks(allBlocks1);

    // Another identical block list to test replacement.
    List<LocatedBlock> allBlocks2 = createBlockRange(blockBoundaries, 0);
    DFSLocatedBlocks dfsAllBlocks2 = createDFSLocatedBlocks(allBlocks2);

    for (int subset1 = 0; subset1 < NUM_BLOCK_SUBSETS; ++subset1)
      for (int subset2 = 0; subset2 < NUM_BLOCK_SUBSETS; ++subset2) {
        DFSLocatedBlocks lbs = createDFSLocatedBlocks(blockSubset(allBlocks1,
            subset1));
        lbs.insertRange(blockSubset(allBlocks2, subset2));

        // Test that offsets and sizes are the same.
        List<LocatedBlock> unionBlocks = blockSubset(allBlocks1, subset1
            | subset2);
        assertEquals(unionBlocks.toString(),
            lbs.getLocatedBlocks().toString());

        for (int i = 0; i < EXHAUSTIVE_NUM_BLOCKS; ++i) {
          int blockMask = (1 << i);
          boolean isInFirst = (subset1 & blockMask) != 0;
          boolean isInSecond = (subset2 & blockMask) != 0;
          long offset = allBlocks1.get(i).getStartOffset();
          LocatedBlock lb = lbs.getBlockContainingOffset(offset);
          LocatedBlock lb1 = dfsAllBlocks1.getBlockContainingOffset(offset);
          LocatedBlock lb2 = dfsAllBlocks2.getBlockContainingOffset(offset);
          if (isInSecond) {
            assertTrue(lb == lb2); 
          } else if (isInFirst) {
            assertTrue(lb == lb1);
          } else {
            assertTrue(lb == null);
          }
        }
      }
  }

  private List<LocatedBlock> selectBlocks(List<LocatedBlock> allBlocks,
      int... blockIndexes) {
    List<LocatedBlock> someBlocks = new ArrayList<LocatedBlock>();
    for (int i = 0; i < blockIndexes.length; ++i)
      someBlocks.add(allBlocks.get(blockIndexes[i]));
    return someBlocks;
  }

  @Test
  public void testJumpOverBlocks() {
    List<LocatedBlock> allBlocks = 
        randomDFSLocatedBlocks(10, 0).getLocatedBlocks();
    DFSLocatedBlocks locatedBlocks = randomDFSLocatedBlocks(0, 0);

    locatedBlocks.insertRange(selectBlocks(allBlocks, 1, 3));
    assertEquals(selectBlocks(allBlocks, 1, 3).toString(),
        locatedBlocks.toString());

    locatedBlocks.insertRange(selectBlocks(allBlocks, 2, 4));
    assertEquals(selectBlocks(allBlocks, 1, 2, 3, 4).toString(),
        locatedBlocks.toString());
  }

  @Test
  public void testFirstLastBlockOverlap() {
    List<LocatedBlock> allBlocks =
        randomDFSLocatedBlocks(10, 0).getLocatedBlocks();
    DFSLocatedBlocks locatedBlocks = randomDFSLocatedBlocks(0, 0);
    locatedBlocks.insertRange(selectBlocks(allBlocks, 1, 2, 3));
    assertEquals(selectBlocks(allBlocks, 1, 2, 3).toString(),
        locatedBlocks.toString());

    locatedBlocks.insertRange(selectBlocks(allBlocks, 6, 7, 8));
    assertEquals(selectBlocks(allBlocks, 1, 2, 3, 6, 7, 8).toString(),
        locatedBlocks.toString());

    locatedBlocks.insertRange(selectBlocks(allBlocks, 8, 9));
    assertEquals(selectBlocks(allBlocks, 1, 2, 3, 6, 7, 8, 9).toString(),
        locatedBlocks.toString());

    locatedBlocks.insertRange(selectBlocks(allBlocks, 0, 1));
    assertEquals(selectBlocks(allBlocks, 0, 1, 2, 3, 6, 7, 8, 9).toString(),
        locatedBlocks.toString());
  }

  @Test
  public void testBlockContainingOffset() {
    for (long blockSizeDelta = -1; blockSizeDelta <= 0; ++blockSizeDelta) {
      DFSLocatedBlocks locatedBlocks =
          randomDFSLocatedBlocks(1000, blockSizeDelta);
      LOG.info("Located blocks: " + locatedBlocks);
      List<LocatedBlock> allBlocks = locatedBlocks.getLocatedBlocks();
      for (LocatedBlock b : allBlocks) {
        long startOffset = b.getStartOffset();
        long endOffset = startOffset + b.getBlockSize();
        assertTrue(
            locatedBlocks.getBlockContainingOffset(startOffset - 1) != b);
        assertTrue(locatedBlocks.getBlockContainingOffset(startOffset) == b);
        assertTrue(locatedBlocks.getBlockContainingOffset(endOffset - 1) == b);
        assertTrue(locatedBlocks.getBlockContainingOffset(endOffset) != b);

        if (blockSizeDelta < 0) {
          // We have left gaps between blocks. Check that the byte immediately
          // before and the byte immediately after the block are not in any
          // block.
          assertTrue("b=" + b,
              locatedBlocks.getBlockContainingOffset(startOffset - 1) == null);
          assertTrue("b=" + b,
              locatedBlocks.getBlockContainingOffset(endOffset) == null);
        }
      }
    }
  }

}
