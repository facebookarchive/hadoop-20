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
package org.apache.hadoop.raid;

import java.io.File;
import java.io.IOException;
import junit.framework.TestCase;
import org.junit.Test;
import org.junit.After;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.TestRaidDfs;
import org.apache.hadoop.raid.RaidNode;

public class TestDirectoryRaidShellFsck  extends TestCase {
  final static Log LOG =
    LogFactory.getLog("org.apache.hadoop.raid.TestDirectoryRaidShellFsck");
  final static String TEST_DIR = 
    new File(System.
             getProperty("test.build.data", "build/contrib/raid/test/data")).
    getAbsolutePath();
  final static int NUM_DATANODES = 4;
  final static int STRIPE_BLOCKS = 3; // number of blocks per stripe
  final static long BASIC_BLOCK_SIZE = 8192L; // size of block in byte
  final static long PARITY_BLOCK_SIZE = BASIC_BLOCK_SIZE * 2;
  final short targetReplication = 1;
  final short metaReplication = 1;
  final long[] fileSizes =
      new long[]{BASIC_BLOCK_SIZE + BASIC_BLOCK_SIZE/2, // block 0, 1
      3*BASIC_BLOCK_SIZE,                               // block 2, 3
      BASIC_BLOCK_SIZE + BASIC_BLOCK_SIZE/2 + 1};       // block 4, 5, 6, 7
  final long[] blockSizes = new long[]{BASIC_BLOCK_SIZE, 2*BASIC_BLOCK_SIZE,
      BASIC_BLOCK_SIZE/2};
  
  final long[] fileSizes1 = new long[] {
      BASIC_BLOCK_SIZE * STRIPE_BLOCKS,
      BASIC_BLOCK_SIZE * STRIPE_BLOCKS,
      BASIC_BLOCK_SIZE * STRIPE_BLOCKS,
  };
  final long[] blockSizes1 = new long[] {
      BASIC_BLOCK_SIZE, BASIC_BLOCK_SIZE, BASIC_BLOCK_SIZE
  };
      
  final Path srcDir = new Path("/user/dhruba/raidtestrs");
  final Path parityFile = new Path("/destraidrs/user/dhruba/raidtestrs");
  Configuration conf = null;
  Configuration clientConf = null;
  MiniDFSCluster cluster = null;
  DistributedFileSystem dfs = null;
  RaidShell shell = null;
  String[] args = null;
  Path[] files = null;
  FileStatus[] srcStats = null;
  FileStatus parityStat = null;

  public void setUpCluster(int rsParityLength) 
      throws IOException, ClassNotFoundException {
    setUpCluster(rsParityLength, fileSizes, blockSizes);
  }
  
  /**
   * creates a MiniDFS instance with a raided file in it
   */
  public void setUpCluster(int rsPairtyLength, long[] fileSizes,
      long[] blockSizes) throws IOException, ClassNotFoundException {
    new File(TEST_DIR).mkdirs(); // Make sure data directory exists
    conf = new Configuration();
    Utils.loadTestCodecs(conf, STRIPE_BLOCKS, STRIPE_BLOCKS, 1, rsPairtyLength,
        "/destraid", "/destraidrs", false, true);
    conf.setBoolean("dfs.permissions", false);
    cluster = new MiniDFSCluster(conf, NUM_DATANODES, true, null);
    cluster.waitActive();
    dfs = (DistributedFileSystem) cluster.getFileSystem();
    TestDirectoryRaidDfs.setupStripeStore(conf, dfs);    
    String namenode = dfs.getUri().toString();
    FileSystem.setDefaultUri(conf, namenode);
    Codec dirRS = Codec.getCodec("rs");
    long[] crcs = new long[fileSizes.length];
    int[] seeds = new int[fileSizes.length];
    files = TestRaidDfs.createTestFiles(srcDir, fileSizes,
      blockSizes, crcs, seeds, (FileSystem)dfs, (short)1);
    assertTrue(RaidNode.doRaid(conf, dfs.getFileStatus(srcDir),
      new Path(dirRS.parityDirectory), dirRS,
      new RaidNode.Statistics(),
      RaidUtils.NULL_PROGRESSABLE,
      false, 1, 1));
    srcStats = new FileStatus[files.length];
    for (int i = 0 ; i < files.length; i++) {
      srcStats[i] = dfs.getFileStatus(files[i]);
    }
    parityStat = dfs.getFileStatus(parityFile);
    clientConf = new Configuration(conf);
    clientConf.set("fs.hdfs.impl",
                   "org.apache.hadoop.hdfs.DistributedRaidFileSystem");
    clientConf.set("fs.raid.underlyingfs.impl",
                   "org.apache.hadoop.hdfs.DistributedFileSystem");
    // prepare shell and arguments
    shell = new RaidShell(clientConf);
    args = new String[2];
    args[0] = "-fsck";
    args[1] = "/";
  }

  /**
   * removes a specified block from MiniDFS storage and reports it as corrupt
   */
  public static void removeAndReportBlock(MiniDFSCluster cluster, 
                                    FileStatus fsStat,
                                    int[] blockNums) 
    throws IOException {
    DistributedFileSystem blockDfs = (DistributedFileSystem)cluster.getFileSystem();
    Path filePath = fsStat.getPath();
    LocatedBlocks lbs = blockDfs.getClient().namenode.
        getBlockLocations(filePath.toUri().getPath(), 0, fsStat.getLen());
    for (int blockNum: blockNums) {
      assert blockNum < lbs.getLocatedBlocks().size();
      LocatedBlock block = lbs.get(blockNum);
      TestRaidDfs.corruptBlock(filePath, block.getBlock(), 
          NUM_DATANODES, true, cluster);
      // report deleted block to the name node
      LocatedBlock[] toReport = { block };
      blockDfs.getClient().namenode.reportBadBlocks(toReport);
    }
  }
  
  /**
   * checks fsck with files whose size is one stripe 
   */
  @Test
  public void testFilesWithStripeSize() throws Exception {
    LOG.info("testFilesWithStripeSize");
    int rsParityLength = 3;
    setUpCluster(rsParityLength, fileSizes1, blockSizes1);
    TestRaidShellFsck.waitUntilCorruptFileCount(dfs, 0);
    LOG.info("Corrupt the last file");
    removeAndReportBlock(cluster, srcStats[2], new int[]{0, 1, 2});
    TestRaidShellFsck.waitUntilCorruptFileCount(dfs, 1);
    LOG.info(" Corrupt 1 blocks of parity file in stripe 2 ");
    removeAndReportBlock(cluster, parityStat, new int[]{6});
    TestRaidShellFsck.waitUntilCorruptFileCount(dfs, 2);
    assertEquals(0, ToolRunner.run(shell, args));
    int result = shell.getCorruptCount();
    assertEquals("fsck should return 1", 1, result);
  }

  /**
   * checks fsck with no missing blocks
   */
  @Test
  public void testClean() throws Exception {
    LOG.info("testClean");
    setUpCluster(3);
    assertEquals(0, ToolRunner.run(shell, args));
    int result = shell.getCorruptCount();
    assertEquals("fsck should return 0", 0, result);
  }

  /**
   * checks fsck with missing all blocks in files but not in parity files 
   * Because parity stripe length is 3, we don't corrupt any files.
   */
  @Test
  public void testFileBlockMissing() throws Exception {
    LOG.info("testFileBlockMissing");
    int rsParityLength = 3;
    setUpCluster(rsParityLength);
    TestRaidShellFsck.waitUntilCorruptFileCount(dfs, 0);
    LOG.info("Corrupt all blocks in all source files");
    for (int i = 0; i < files.length; i++) {
      long blockNum = RaidNode.getNumBlocks(srcStats[i]);
      for (int j = 0; j < blockNum; j++) {
        removeAndReportBlock(cluster, srcStats[i], new int[]{j});
      }
    }
    TestRaidShellFsck.waitUntilCorruptFileCount(dfs, files.length);
    assertEquals(0, ToolRunner.run(shell, args));
    int result = shell.getCorruptCount();
    assertEquals("fsck should return 0", 0, result);
  }

  /**
   * checks fsck with missing all blocks in parity block but not in file block
   * Raid fsck actually skips all parity files.
   */
  @Test
  public void testParityBlockMissing() throws Exception {
    LOG.info("testParityBlockMissing");
    int rsParityLength = 3;
    setUpCluster(rsParityLength);
    TestRaidShellFsck.waitUntilCorruptFileCount(dfs, 0);
    long blockNum = RaidNode.getNumBlocks(parityStat);
    LOG.info("Corrupt all blocks in parity file");
    for (int i = 0; i < blockNum; i++) {
      removeAndReportBlock(cluster, parityStat, new int[]{i});
    }
    TestRaidShellFsck.waitUntilCorruptFileCount(dfs, 1);

    assertEquals(0, ToolRunner.run(shell, args));
    int result = shell.getCorruptCount();

    assertEquals("fsck should return 0", 0, result);
  }

  /**
   * checks fsck with missing block in both file block and parity block
   * in different stripes
   */
  @Test
  public void testFileBlockAndParityBlockMissingInDifferentStripes() 
    throws Exception {
    LOG.info("testFileBlockAndParityBlockMissingInDifferentStripes");
    int rsParityLength = 3;
    setUpCluster(rsParityLength);
    TestRaidShellFsck.waitUntilCorruptFileCount(dfs, 0);
    LOG.info("Corrupt all blocks of source files in the first stripe");
    removeAndReportBlock(cluster, srcStats[0], new int[]{0, 1});
    removeAndReportBlock(cluster, srcStats[1], new int[]{0});
    TestRaidShellFsck.waitUntilCorruptFileCount(dfs, 2);
    LOG.info("Corrupt all blocks of parity file in the second and third stripe");
    removeAndReportBlock(cluster, parityStat, new int[]{3, 4, 5, 6, 7, 8});
    TestRaidShellFsck.waitUntilCorruptFileCount(dfs, 3);

    assertEquals(0, ToolRunner.run(shell, args));
    int result = shell.getCorruptCount();

    assertEquals("fsck should return 0", 0, result);
  }

  /**
   * checks fsck with missing blocks in both file block and parity block
   * in same stripe
   */
  @Test
  public void testFileBlockAndParityBlockMissingInSameStripe() 
    throws Exception {
    LOG.info("testFileBlockAndParityBlockMissingInSameStripe");
    int rsParityLength = 3;
    setUpCluster(rsParityLength);
    TestRaidShellFsck.waitUntilCorruptFileCount(dfs, 0);
    LOG.info("Corrupt all blocks of parity file in the first stripe");
    removeAndReportBlock(cluster, parityStat, new int[]{0, 1, 2});
    TestRaidShellFsck.waitUntilCorruptFileCount(dfs, 1);
    LOG.info("Corrupt the second block in the first stripe");
    removeAndReportBlock(cluster, srcStats[0], new int[]{1});
    TestRaidShellFsck.waitUntilCorruptFileCount(dfs, 2);
    LOG.info("Corrupt the first block in the second stripe");
    removeAndReportBlock(cluster, srcStats[1], new int[]{1});
    TestRaidShellFsck.waitUntilCorruptFileCount(dfs, 3);

    assertEquals(0, ToolRunner.run(shell, args));
    int result = shell.getCorruptCount();
    // only the fist file is corrupt 
    assertEquals("fsck should return 1", 1, result);
  }

  /**
   * checks fsck with 3 missing blocks in same stripe
   * Because rsParityLength is 2, we corrupt a file.
   */
  @Test
  public void test3FileBlocksMissingInSameStripe() 
    throws Exception {
    LOG.info("test3FileBlocksMissingInSameStripe");
    int rsParityLength = 2;
    setUpCluster(rsParityLength);
    TestRaidShellFsck.waitUntilCorruptFileCount(dfs, 0);
    LOG.info("Corrupt the all blocks of source files in the second " +
        "stripe and thrid stripe, the second stripe is corrupt, so " + 
        " file 1 and file 2 are corrupt");
    removeAndReportBlock(cluster, srcStats[1], new int[]{1});
    removeAndReportBlock(cluster, srcStats[2], new int[]{0, 1, 2, 3});
    TestRaidShellFsck.waitUntilCorruptFileCount(dfs, 2);

    assertEquals(0, ToolRunner.run(shell, args));
    int result = shell.getCorruptCount();

    assertEquals("fsck should return 2", 2, result);
  }
  
  /**
   * Corrupt blocks in all stripes  
   */
  @Test
  public void testCorruptionInAllStripes() throws Exception {
    LOG.info("testCorruptionInAllStripes");
    int rsParityLength = 3;
    setUpCluster(rsParityLength);
    TestRaidShellFsck.waitUntilCorruptFileCount(dfs, 0);
    LOG.info(" Corrupt 2 blocks of source file in stripe 0, 1, 2 ");
    removeAndReportBlock(cluster, srcStats[0], new int[]{0, 1});
    removeAndReportBlock(cluster, srcStats[1], new int[]{1});
    removeAndReportBlock(cluster, srcStats[2], new int[]{2, 3});

    TestRaidShellFsck.waitUntilCorruptFileCount(dfs, 3);
    LOG.info(" Corrupt 2 blocks of parity file in stripe 0, 2 ");
    removeAndReportBlock(cluster, parityStat, new int[]{0, 1, 6, 7});
    TestRaidShellFsck.waitUntilCorruptFileCount(dfs, 4);
    assertEquals(0, ToolRunner.run(shell, args));
    int result = shell.getCorruptCount();
    
    // the second file is not recoverable
    assertEquals("fsck should return 2", 2, result);
  }
  
  @After
  public void tearDown() throws Exception {
    if (cluster != null) {
      cluster.shutdown();
      cluster = null;
    }
    dfs = null;
    LOG.info("Test cluster shut down");
  }

}