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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotSame;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import junit.framework.TestCase;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.RaidDFSUtil;
import org.apache.hadoop.hdfs.TestDatanodeBlockScanner;
import org.apache.hadoop.hdfs.TestRaidDfs;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.mapred.MiniMRCluster;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.raid.DistBlockIntegrityMonitor.Worker.LostFileInfo;
import org.apache.hadoop.raid.LogUtils.LOGRESULTS;
import org.apache.hadoop.raid.LogUtils.LOGTYPES;
import org.apache.hadoop.raid.StripeStore.StripeInfo;
import org.apache.hadoop.util.StringUtils;
import org.junit.Test;


public class TestDirectoryBlockFixer extends TestCase {
  final static Log LOG = LogFactory.getLog(
                            "org.apache.hadoop.raid.TestDirectoryBlockFixer");
  final static String TEST_DIR = new File(System.getProperty("test.build.data",
      "build/contrib/raid/test/data")).getAbsolutePath();
  final static String CHECKSUM_STORE_DIR = new File(TEST_DIR,
      "ckm_store." + System.currentTimeMillis()).getAbsolutePath();
  final static String STRIPE_STORE_DIR = new File(TEST_DIR,
      "stripe_store." + System.currentTimeMillis()).getAbsolutePath();
  final static String CONFIG_FILE = new File(TEST_DIR, 
      "test-raid.xml").getAbsolutePath();
  final static long RELOAD_INTERVAL = 1000;
  final static int NUM_DATANODES = 3;
  final long blockSize = 8192L;
  final long[] fileSizes =
      new long[]{blockSize + blockSize/2, // block 0, 1
      3*blockSize,                  // block 2, 3
      blockSize + blockSize/2 + 1}; // block 4, 5, 6, 7
  final long[] blockSizes = new long[]{blockSize, 2*blockSize, blockSize/2};
  final Integer[] rsCorruptFileIdx1 = new Integer[]{0, 1, 2, 3, 5, 6, 7};
  final int[] rsNumCorruptBlocksInFiles1 = new int[] {2, 2, 3};
  final Integer[] rsCorruptFileIdx2 = new Integer[]{1, 3, 4, 5, 6};
  final int[] rsNumCorruptBlocksInFiles2 = new int[] {1, 1, 3};
  Configuration conf;
  String namenode = null;
  MiniDFSCluster dfsCluster = null;
  String hftp = null;
  MiniMRCluster mr = null;
  FileSystem fileSys = null;
  RaidNode cnode = null;
  String jobTrackerName = null;
  Random rand = new Random();
  static {
    ParityFilePair.disableCacheUsedInTestOnly();
  }

  public Configuration getRaidNodeConfig(Configuration conf, boolean local) {
    // create an instance of the RaidNode
    Configuration localConf = new Configuration(conf);
    localConf.setInt("raid.blockfix.interval", 1000);
    if (local) {
      localConf.set("raid.blockfix.classname",
                    "org.apache.hadoop.raid.LocalBlockIntegrityMonitor");
    } else {
      localConf.set("raid.blockfix.classname",
                    "org.apache.hadoop.raid.DistBlockIntegrityMonitor");
    }
    localConf.setLong("raid.blockfix.filespertask", 2L);
    return localConf;
  }

  @Test
  public void testDirectoryFilterUnfixableFiles() throws Exception {
    conf = new Configuration();
    dfsCluster = new MiniDFSCluster(conf, NUM_DATANODES, true, null);
    dfsCluster.waitActive();
    FileSystem fs = dfsCluster.getFileSystem();

    Utils.loadTestCodecs(conf, 3, 5, 1,
        3, "/destraid", "/destraidrs", false, true);
    try {
      Configuration testConf = fs.getConf();
      BlockIntegrityMonitor blockFixer = new
          LocalBlockIntegrityMonitor(testConf, false);

      String p1 = "/user/foo/f1";
      String p2 = "/user/foo/f2";
      String p3 = "/user1/foo/bar/f1";
      String p4 = "/a/b";
      String p5 = "/c";
      String p6 = "/destraidrs/user";
      String p7 = "/destraid/user1/foo";
      
      fs.mkdirs(new Path(p6));

      List<String> fileList = new ArrayList<String>();
      fileList.add(p1);
      fileList.add(p2);
      fileList.add(p3);
      fileList.add(p4);
      fileList.add(p5);

      blockFixer.filterUnreconstructableSourceFiles(fs, fileList.iterator());
      // p3 and p5 should be filtered out.
      assertEquals(3, fileList.size());

      Set<String> filtered = new HashSet<String>();
      for (String p: fileList) filtered.add(p);
      assertFalse("File not filtered", filtered.contains(p3));
      assertFalse("File not filtered", filtered.contains(p5));

      fileList.add(p3);
      fs.mkdirs(new Path(p7));
      blockFixer.filterUnreconstructableSourceFiles(fs, fileList.iterator());
      // Nothing is filtered.
      assertEquals(4, fileList.size());
    } finally {
      dfsCluster.shutdown();
    }
  }
  
  @Test
  public void testDirBlockFixLocal() throws Exception {
    implDirBlockFix(true, true, false);
  }
  
  @Test
  public void testDirBlockFixDist() throws Exception {
    implDirBlockFix(false, true, false);
  }
 
  @Test
  public void testDirBlockFixDistWithoutStripeInfo() throws Exception {
    implDirBlockFix(false, false, false);
  }
  
  @Test
  public void testDirBlockFixDistWithStripeVerificationFailure()
      throws Exception {
    implDirBlockFix(false, true, true);
  }
  
  private long getTotal(int[] elements) {
    long totalCorruptBlocks = 0;
    for (int numBlk: elements) {
      totalCorruptBlocks += numBlk;
    }
    return totalCorruptBlocks;
  }
  
  /**
   * Create a file with three stripes, corrupt a block each in two stripes,
   * and wait for the the file to be fixed.
   */
  private void implDirBlockFix(boolean local, boolean hasStripeInfo,
      boolean corruptStripe) throws Exception {
    LOG.info("Test testDirBlockFix started. local:" + local + 
        " hasStripeInfo:" + hasStripeInfo + " corruptStripe:" + corruptStripe);
    int stripeLength = 3;
    mySetup(stripeLength);
    long[] crcs = new long[3];
    int[] seeds = new int[3];
    Path dirPath = new Path("/user/dhruba/raidtestrs");
    Path[] files = TestRaidDfs.createTestFiles(dirPath,
        fileSizes, blockSizes, crcs, seeds, fileSys, (short)1);
    Path destPath = new Path("/destraidrs/user/dhruba");
    LOG.info("Test testDirBlockFix created test files");
    Configuration localConf = this.getRaidNodeConfig(conf, local);
    // Not allow multiple running jobs
    localConf.setLong("raid.blockfix.maxpendingjobs", 1L);

    try {
      cnode = RaidNode.createRaidNode(null, localConf);
      TestRaidDfs.waitForDirRaided(LOG, fileSys, dirPath, destPath);
      cnode.stop(); cnode.join();
      
      DistributedFileSystem dfs = (DistributedFileSystem)fileSys;
      String[] corruptFiles = DFSUtil.getCorruptFiles(dfs);
      assertEquals("no corrupt files expected", 0, corruptFiles.length);
      assertEquals("filesFixed() should return 0 before fixing files",
                   0, cnode.blockIntegrityMonitor.getNumFilesFixed());
      if (!hasStripeInfo) {
        // clear out all stripes
        LocalStripeStore lss = new LocalStripeStore();
        lss.initialize(localConf, false, dfs);
        lss.clear();
      }
      if (corruptStripe) {
        LocalStripeStore lss = new LocalStripeStore();
        lss.initialize(localConf, false, dfs);
        Set<List<Block>> corruptCandidates = 
            new HashSet<List<Block>>(lss.stripeSet.keySet());
        for (List<Block> lb : corruptCandidates) {
          for (Codec codec : Codec.getCodecs()) {
            StripeInfo si = lss.getStripe(codec, lb.get(0));
            if (si == null) {
              continue;
            }
            String oldSi = si.toString();
            Collections.rotate(si.parityBlocks, 1);
            Collections.rotate(si.srcBlocks, 1);
            lss.putStripe(codec, si.parityBlocks, si.srcBlocks);
            String newSi = lss.getStripe(codec, lb.get(0)).toString();
            LOG.info("Corrupt the stripe info old : " + oldSi +
                     " new : " + newSi);
          }
        }
      }
      
      this.corruptFiles(dirPath, crcs, rsCorruptFileIdx1, dfs, files, 
          rsNumCorruptBlocksInFiles1);
      cnode = RaidNode.createRaidNode(null, localConf);
      long start = System.currentTimeMillis();
      while (cnode.blockIntegrityMonitor.getNumFilesFixed() < 3 && 
             cnode.blockIntegrityMonitor.getNumFileFixFailures() < 3 &&
             System.currentTimeMillis() - start < 120000) {
        LOG.info("Test testDirBlockFix waiting for files to be fixed.");
        Thread.sleep(1000);
      }
      long totalCorruptBlocks = getTotal(rsNumCorruptBlocksInFiles1);
      if (hasStripeInfo) {
        if (!corruptStripe) {
         TestBlockFixer.verifyMetrics(fileSys, cnode, local,
              3L, totalCorruptBlocks);
      
          dfs = getDFS(conf, dfs);
          for (int i = 0; i < fileSizes.length; i++) {
            assertTrue("file " + files[i] + " not fixed",
                TestRaidDfs.validateFile(dfs, files[i], fileSizes[i],
                crcs[i]));
          }
        } else {
          TestBlockFixer.verifyMetrics(fileSys, cnode, local, 0L, 0L);
          assertTrue("should fail to fix more than 3 files", 
              cnode.blockIntegrityMonitor.getNumFileFixFailures() >= 3L);
          TestBlockFixer.verifyMetrics(fileSys, cnode, 
              LOGTYPES.OFFLINE_RECONSTRUCTION_FILE, LOGRESULTS.FAILURE, 3L, true);
          // Will throw stripe mismatch exception for the first blocks of 3 files
          TestBlockFixer.verifyMetrics(fileSys, cnode,
              LOGTYPES.OFFLINE_RECONSTRUCTION_STRIPE_VERIFICATION,
              LOGRESULTS.FAILURE, 3L, true);
        }
      } else {
        TestBlockFixer.verifyMetrics(fileSys, cnode, local, 0L, 0L);
        assertTrue("should fail to fix more than 3 files",  
            cnode.blockIntegrityMonitor.getNumFileFixFailures() >= 3L);
        TestBlockFixer.verifyMetrics(fileSys, cnode,
            LOGTYPES.OFFLINE_RECONSTRUCTION_GET_STRIPE, LOGRESULTS.FAILURE,
            totalCorruptBlocks, true);
        TestBlockFixer.verifyMetrics(fileSys, cnode, 
            LOGTYPES.OFFLINE_RECONSTRUCTION_FILE, LOGRESULTS.FAILURE,
            3L, true);
      }
    } catch (Exception e) {
      LOG.info("Test testDirBlockFix Exception " + e, e);
      throw e;
    } finally {
      myTearDown();
    }
    LOG.info("Test testDirBlockFix completed.");
  }

  /**
   * Tests integrity of generated block.
   * Create a file and delete a block entirely. Wait for the block to be
   * regenerated. Now stop RaidNode and corrupt the generated block.
   * Test that corruption in the generated block can be detected by clients.
   */
  private void generatedBlockTestCommon(String testName, int blockToCorrupt,
                                        boolean local) throws Exception {
    LOG.info("Test " + testName + " started.");
    int stripeLength = 3;
    mySetup(stripeLength);
    long[] crcs = new long[3];
    int[] seeds = new int[3];
    Path dirPath = new Path("/user/dhruba/raidtest");
    Path[] files = TestRaidDfs.createTestFiles(dirPath,
        fileSizes, blockSizes, crcs, seeds, fileSys, (short)1);
    Path destPath = new Path("/destraid/user/dhruba");
    LOG.info("Test " + testName + " created test files");
    Configuration localConf = this.getRaidNodeConfig(conf, local);

    try {
      cnode = RaidNode.createRaidNode(null, localConf);
      TestRaidDfs.waitForDirRaided(LOG, fileSys, dirPath, destPath);
      cnode.stop(); cnode.join();
      
      DistributedFileSystem dfs = (DistributedFileSystem)fileSys;
      String[] corruptFiles = DFSUtil.getCorruptFiles(dfs);
      assertEquals("no corrupt files expected", 0, corruptFiles.length);
      assertEquals("filesFixed() should return 0 before fixing files",
                   0, cnode.blockIntegrityMonitor.getNumFilesFixed());
      
      Integer[] corruptBlockIdxs = new Integer[]{blockToCorrupt};
      TestDirectoryRaidDfs.corruptBlocksInDirectory(conf, dirPath,
          crcs, corruptBlockIdxs, fileSys, dfsCluster, false, true);
      
      corruptFiles = DFSUtil.getCorruptFiles(dfs);
      assertEquals("files not corrupted", corruptBlockIdxs.length,
          corruptFiles.length);
      int corruptFileIdx = -1;
      for (int i = 0; i < files.length; i++) {
        if (files[i].toUri().getPath().equals(corruptFiles[0])) {
          corruptFileIdx = i;
          break;
        }
      }
      assertNotSame("Wrong corrupt file", -1, corruptFileIdx);
      cnode = RaidNode.createRaidNode(null, localConf);
      long start = System.currentTimeMillis();
      while (cnode.blockIntegrityMonitor.getNumFilesFixed() < 1 &&
             System.currentTimeMillis() - start < 120000) {
        LOG.info("Test testDirBlockFix waiting for files to be fixed.");
        Thread.sleep(1000);
      }
      TestBlockFixer.verifyMetrics(fileSys, cnode, local, 1L,
          corruptBlockIdxs.length);
      // Stop RaidNode
      cnode.stop(); cnode.join(); cnode = null;

      // The block has successfully been reconstructed.
      dfs = getDFS(conf, dfs);
      assertTrue("file not fixed",
                 TestRaidDfs.validateFile(dfs, files[corruptFileIdx], 
                     fileSizes[corruptFileIdx], crcs[corruptFileIdx]));

      // Now corrupt the generated block.
      TestDirectoryRaidDfs.corruptBlocksInDirectory(conf, dirPath,
          crcs, corruptBlockIdxs, dfs, dfsCluster, false, false);
      try {
        TestRaidDfs.validateFile(dfs, files[corruptFileIdx], 
            fileSizes[corruptFileIdx], crcs[corruptFileIdx]);
        fail("Expected exception not thrown");
      } catch (org.apache.hadoop.fs.ChecksumException ce) {
      } catch (org.apache.hadoop.fs.BlockMissingException bme) {
      }
    } catch (Exception e) {
      LOG.info("Test " + testName + " Exception " + e, e);
      throw e;
    } finally {
      myTearDown();
    }
    LOG.info("Test " + testName + " completed.");
  }

  /**
   * Tests integrity of generated block.
   * Create a file and delete a block entirely. Wait for the block to be
   * regenerated. Now stop RaidNode and corrupt the generated block.
   * Test that corruption in the generated block can be detected by clients.
   */
  @Test
  public void testGeneratedBlockLocal() throws Exception {
    generatedBlockTestCommon("testGeneratedBlock", 2, true);
  }

  /**
   * Tests integrity of generated block.
   * Create a file and delete a block entirely. Wait for the block to be
   * regenerated. Now stop RaidNode and corrupt the generated block.
   * Test that corruption in the generated block can be detected by clients.
   */
  @Test
  public void testGeneratedBlockDist() throws Exception {
    generatedBlockTestCommon("testGeneratedBlock", 2, false);
  }

  /**
   * Tests integrity of generated last block.
   * Create a file and delete a block entirely. Wait for the block to be
   * regenerated. Now stop RaidNode and corrupt the generated block.
   * Test that corruption in the generated block can be detected by clients.
   */
  @Test
  public void testGeneratedLastBlockLocal() throws Exception {
    generatedBlockTestCommon("testGeneratedLastBlock", -1, true);
  }

  /**
   * Tests integrity of generated last block.
   * Create a file and delete a block entirely. Wait for the block to be
   * regenerated. Now stop RaidNode and corrupt the generated block.
   * Test that corruption in the generated block can be detected by clients.
   */
  @Test
  public void testGeneratedLastBlockDist() throws Exception {
    generatedBlockTestCommon("testGeneratedLastBlock", -1, false);
  }
  
  @Test
  public void testParityBlockFixLocal() throws Exception {
    implParityBlockFix("testParityBlockFixLocal", true);
  }

  @Test
  public void testParityBlockFixDist() throws Exception {
    implParityBlockFix("testParityBlockFixDist", false);
  }
  
  /**
   * Corrupt a parity file and wait for it to get fixed.
   */
  private void implParityBlockFix(String testName, boolean local)
    throws Exception {
    LOG.info("Test " + testName + " started.");
    int stripeLength = 3;
    mySetup(stripeLength); 
    long[] crcs = new long[3];
    int[] seeds = new int[3];
    Path dirPath = new Path("/user/dhruba/raidtest");
    Path[] files = TestRaidDfs.createTestFiles(dirPath,
        fileSizes, blockSizes, crcs, seeds, fileSys, (short)1);
    Path destPath = new Path("/destraid/user/dhruba");
    Path parityFile = new Path("/destraid/user/dhruba/raidtest");
    LOG.info("Test " + testName + " created test files");
    Configuration localConf = this.getRaidNodeConfig(conf, local);

    try {
      cnode = RaidNode.createRaidNode(null, localConf);
      TestRaidDfs.waitForDirRaided(LOG, fileSys, dirPath, destPath);
      cnode.stop(); cnode.join();

      long parityCRC = RaidDFSUtil.getCRC(fileSys, parityFile);

      FileStatus parityStat = fileSys.getFileStatus(parityFile);
      DistributedFileSystem dfs = (DistributedFileSystem)fileSys;
      LocatedBlocks locs = RaidDFSUtil.getBlockLocations(
        dfs, parityFile.toUri().getPath(), 0, parityStat.getLen());
      String[] corruptFiles = DFSUtil.getCorruptFiles(dfs);
      
      assertEquals("no corrupt files expected", 0, corruptFiles.length);
      assertEquals("filesFixed() should return 0 before fixing files",
                   0, cnode.blockIntegrityMonitor.getNumFilesFixed());

      // Corrupt parity blocks for different stripes.
      int[] corruptBlockIdxs = new int[]{0, 1, 2};
      for (int idx: corruptBlockIdxs)
        corruptBlock(locs.get(idx).getBlock(), dfsCluster);
      RaidDFSUtil.reportCorruptBlocks(dfs, parityFile, corruptBlockIdxs,
          2*blockSize);

      corruptFiles = DFSUtil.getCorruptFiles(dfs);
      assertEquals("file not corrupted",
                   1, corruptFiles.length);
      assertEquals("wrong file corrupted",
                   corruptFiles[0], parityFile.toUri().getPath());

      cnode = RaidNode.createRaidNode(null, localConf);
      long start = System.currentTimeMillis();
      while (cnode.blockIntegrityMonitor.getNumFilesFixed() < 1 &&
             System.currentTimeMillis() - start < 120000) {
        LOG.info("Test " + testName + " waiting for files to be fixed.");
        Thread.sleep(3000);
      }
      TestBlockFixer.verifyMetrics(fileSys, cnode, local, 1L, 
          corruptBlockIdxs.length);

      long checkCRC = RaidDFSUtil.getCRC(fileSys, parityFile);

      assertEquals("file not fixed",
                   parityCRC, checkCRC);

    } catch (Exception e) {
      LOG.info("Test " + testName + " Exception " + e +
               StringUtils.stringifyException(e));
      throw e;
    } finally {
      myTearDown();
    }
    LOG.info("Test " + testName + " completed.");
  }
  
  private void corruptFiles(Path dirPath, long[] crcs, 
      Integer[] corruptBlockIdxs, DistributedFileSystem dfs,
      Path[] files, int[] numCorruptBlocksInFiles) throws IOException {
    int totalCorruptFiles = DFSUtil.getCorruptFiles(dfs).length;
    TestDirectoryRaidDfs.corruptBlocksInDirectory(conf, dirPath,
        crcs, corruptBlockIdxs, fileSys, dfsCluster, false, true);
    
    String[] corruptFiles = DFSUtil.getCorruptFiles(dfs);
    for (int i = 0; i < numCorruptBlocksInFiles.length; i++) {
      if (numCorruptBlocksInFiles[i] > 0)
        totalCorruptFiles++;
    }
    assertEquals("files not corrupted", totalCorruptFiles,
        corruptFiles.length);
    for (int i = 0; i< fileSizes.length; i++) {
      assertEquals("wrong number of corrupt blocks for file " + 
          files[i], numCorruptBlocksInFiles[i],
          RaidDFSUtil.corruptBlocksInFile(dfs,
          files[i].toUri().getPath(), 0, fileSizes[i]).size());
    }
  }

  /**
   * tests that we can have 2 concurrent jobs fixing files 
   * (dist block fixer)
   */
  @Test
  public void testConcurrentJobs() throws Exception {
    LOG.info("Test testConcurrentJobs started.");
    int stripeLength = 3;
    mySetup(stripeLength); 
    long[] crcs1 = new long[3];
    int[] seeds1 = new int[3];
    long[] crcs2 = new long[3];
    int[] seeds2 = new int[3];
    Path dirPath1 = new Path("/user/dhruba/raidtestrs/1");
    Path[] files1 = TestRaidDfs.createTestFiles(dirPath1,
        fileSizes, blockSizes, crcs1, seeds1, fileSys, (short)1);
    Path dirPath2 = new Path("/user/dhruba/raidtestrs/2");
    Path[] files2 = TestRaidDfs.createTestFiles(dirPath2,
        fileSizes, blockSizes, crcs2, seeds2, fileSys, (short)1);
    Path destPath = new Path("/destraidrs/user/dhruba/raidtestrs");
    
    LOG.info("Test testConcurrentJobs created test files");
    Configuration localConf = this.getRaidNodeConfig(conf, false);
    localConf.setLong(BlockIntegrityMonitor.BLOCKCHECK_INTERVAL, 15000L);
    localConf.setLong(
        DistBlockIntegrityMonitor.RAIDNODE_BLOCK_FIX_SUBMISSION_INTERVAL_KEY,
        15000L);
    localConf.setLong(
        DistBlockIntegrityMonitor.RAIDNODE_BLOCK_FIX_SCAN_SUBMISSION_INTERVAL_KEY,
        3600000);
    
    try {
      cnode = RaidNode.createRaidNode(null, localConf);
      TestRaidDfs.waitForDirRaided(LOG, fileSys, dirPath1, destPath);
      TestRaidDfs.waitForDirRaided(LOG, fileSys, dirPath2, destPath);
      cnode.stop(); cnode.join();
      
      DistributedFileSystem dfs = (DistributedFileSystem)fileSys;
      String[] corruptFiles = DFSUtil.getCorruptFiles(dfs);
      assertEquals("no corrupt files expected", 0, corruptFiles.length);
      assertEquals("filesFixed() should return 0 before fixing files",
                   0, cnode.blockIntegrityMonitor.getNumFilesFixed());
      //corrupt directory 1
      this.corruptFiles(dirPath1, crcs1, rsCorruptFileIdx1, dfs, files1, 
          rsNumCorruptBlocksInFiles1);

      cnode = RaidNode.createRaidNode(null, localConf);
      DistBlockIntegrityMonitor blockFixer =
          (DistBlockIntegrityMonitor) cnode.blockIntegrityMonitor;
      long start = System.currentTimeMillis();

      // All files are HIGH-PRI corrupt files
      while (blockFixer.jobsRunning() < 1 &&
             System.currentTimeMillis() - start < 60000) {
        LOG.info("Test testDirBlockFix waiting for fixing job 1 to start");
        Thread.sleep(1000);
      }
      assertEquals("job 1 not running", 1, blockFixer.jobsRunning());
      
      // Corrupt directory 2
      this.corruptFiles(dirPath2, crcs2, rsCorruptFileIdx2, dfs, files2,
          rsNumCorruptBlocksInFiles2);
      
      // 1 LOW-PRI file and 2 HIGH-PRI files 
      while (blockFixer.jobsRunning() < 3 &&
             System.currentTimeMillis() - start < 60000) {
        LOG.info("Test testDirBlockFix waiting for fixing job 2 and 3 to start");
        Thread.sleep(1000);
      }
      assertTrue("more than 3 jobs are running", blockFixer.jobsRunning() >= 3);

      while (blockFixer.getNumFilesFixed() < 6 &&
             System.currentTimeMillis() - start < 240000) {
        LOG.info("Test testDirBlockFix waiting for files to be fixed.");
        Thread.sleep(1000);
      }
      TestBlockFixer.verifyMetrics(fileSys, cnode, false, 6L,
          getTotal(rsNumCorruptBlocksInFiles1) + 
          getTotal(rsNumCorruptBlocksInFiles2));
      dfs = getDFS(conf, dfs);
      for (int i = 0; i < fileSizes.length; i++) {
        assertTrue("file " + files1[i] + " not fixed",
                 TestRaidDfs.validateFile(dfs, files1[i], fileSizes[i], crcs1[i]));
      }
      for (int i = 0; i < fileSizes.length; i++) {
        assertTrue("file " + files2[i] + " not fixed",
                 TestRaidDfs.validateFile(dfs, files2[i], fileSizes[i], crcs2[i]));
      }
    } catch (Exception e) {
      LOG.info("Test testConcurrentJobs exception " + e, e);
      throw e;
    } finally {
      myTearDown();
    }
  }

  /**
   * tests that the distributed block fixer obeys
   * the limit on how many jobs to submit simultaneously.
   */
  @Test
  public void testMaxPendingJobs() throws Exception {
    LOG.info("Test testMaxPendingJobs started.");
    int stripeLength = 3;
    mySetup(stripeLength); 
    long[] crcs1 = new long[3];
    int[] seeds1 = new int[3];
    long[] crcs2 = new long[3];
    int[] seeds2 = new int[3];
    Path dirPath1 = new Path("/user/dhruba/raidtestrs/1");
    Path[] files1 = TestRaidDfs.createTestFiles(dirPath1,
        fileSizes, blockSizes, crcs1, seeds1, fileSys, (short)1);
    Path dirPath2 = new Path("/user/dhruba/raidtestrs/2");
    Path[] files2 = TestRaidDfs.createTestFiles(dirPath2,
        fileSizes, blockSizes, crcs2, seeds2, fileSys, (short)1);
    Path destPath = new Path("/destraidrs/user/dhruba/raidtestrs");
    LOG.info("Test testMaxPendingJobs created test files");
    Configuration localConf = this.getRaidNodeConfig(conf, false);
    localConf.setLong("raid.blockfix.maxpendingjobs", 1L);

    try {
      cnode = RaidNode.createRaidNode(null, localConf);
      TestRaidDfs.waitForDirRaided(LOG, fileSys, dirPath1, destPath);
      TestRaidDfs.waitForDirRaided(LOG, fileSys, dirPath2, destPath);
      cnode.stop(); cnode.join();

      DistributedFileSystem dfs = (DistributedFileSystem)fileSys;
      String[] corruptFiles = DFSUtil.getCorruptFiles(dfs);
      assertEquals("no corrupt files expected", 0, corruptFiles.length);
      assertEquals("filesFixed() should return 0 before fixing files",
                   0, cnode.blockIntegrityMonitor.getNumFilesFixed());
      //corrupt directory 1
      this.corruptFiles(dirPath1, crcs1, rsCorruptFileIdx1, dfs, files1, 
          rsNumCorruptBlocksInFiles1);

      cnode = RaidNode.createRaidNode(null, localConf);
      DistBlockIntegrityMonitor blockFixer = (DistBlockIntegrityMonitor) cnode.blockIntegrityMonitor;
      long start = System.currentTimeMillis();

      while (blockFixer.jobsRunning() < 1 &&
             System.currentTimeMillis() - start < 60000) {
        LOG.info("Test testDirBlockFix waiting for fixing job 1 to start");
        Thread.sleep(1000);
      }
      assertEquals("job not running", 1, blockFixer.jobsRunning());

      //corrupt directory 2
      this.corruptFiles(dirPath2, crcs2, rsCorruptFileIdx2, dfs, files2,
          rsNumCorruptBlocksInFiles2);
      
      // wait until both files are fixed
      while (blockFixer.getNumFilesFixed() < 6 &&
             System.currentTimeMillis() - start < 240000) {
        // make sure the block fixer does not start a second job while
        // the first one is still running
        assertTrue("too many jobs running", blockFixer.jobsRunning() <= 1);
        Thread.sleep(1000);
      }
      TestBlockFixer.verifyMetrics(fileSys, cnode, false, 6L, 
          getTotal(rsNumCorruptBlocksInFiles1) + 
          getTotal(rsNumCorruptBlocksInFiles2));
      dfs = getDFS(conf, dfs);
      for (int i = 0; i < fileSizes.length; i++) {
        assertTrue("file " + files1[i] + " not fixed",
                 TestRaidDfs.validateFile(dfs, files1[i], fileSizes[i], crcs1[i]));
      }
      for (int i = 0; i < fileSizes.length; i++) {
        assertTrue("file " + files2[i] + " not fixed",
                 TestRaidDfs.validateFile(dfs, files2[i], fileSizes[i], crcs2[i]));
      }
    } catch (Exception e) {
      LOG.info("Test testMaxPendingJobs exception " + e +
               StringUtils.stringifyException(e));
      throw e;
    } finally {
      myTearDown();
    }
  }

  static class FakeDistBlockIntegrityMonitor extends DistBlockIntegrityMonitor {
    Map<String, List<String>> submittedJobs =
      new HashMap<String, List<String>>();
    FakeDistBlockIntegrityMonitor(Configuration conf) throws Exception {
      super(conf);
    }

    @Override
    void submitJob(Job job, List<String> filesInJob, Priority priority,
        Map<Job, List<LostFileInfo>> jobIndex, 
        Map<JobID, TrackingUrlInfo> idToTrackingUrlMap) {
      LOG.info("Job " + job.getJobName() + " was submitted ");
      submittedJobs.put(job.getJobName(), filesInJob);
    }
  }

  public void testMultiplePriorities() throws Exception {
    long[] crcs = new long[3];
    int[] seeds = new int[3];
    Path dirPath = new Path("/home/test");
    int stripeLength = 3;
    short repl = 1;
    mySetup(stripeLength); 
    Codec codec = Codec.getCodec("rs");
    LOG.info("Starting testMultiplePriorities");
    try {
      // Create test file and raid it.
      Path[] files = TestRaidDfs.createTestFiles(dirPath,
          fileSizes, blockSizes, crcs, seeds, fileSys, (short)1);
      FileStatus stat = fileSys.getFileStatus(dirPath);
      RaidNode.doRaid(conf, stat, new Path(codec.parityDirectory), codec,
          new RaidNode.Statistics(), RaidUtils.NULL_PROGRESSABLE,
        false, repl, repl);
      
      Integer[] corruptBlockIdxs = new Integer[]{0, 2};
      LOG.info("Corrupt block " + corruptBlockIdxs + " of directory " + dirPath);
      TestDirectoryRaidDfs.corruptBlocksInDirectory(conf, dirPath,
          crcs, corruptBlockIdxs, fileSys, dfsCluster, false, true);

      // Create Block Fixer and fix.
      FakeDistBlockIntegrityMonitor distBlockFixer = new FakeDistBlockIntegrityMonitor(conf);
      assertEquals(0, distBlockFixer.submittedJobs.size());
      // waiting for one job to submit
      long startTime = System.currentTimeMillis();
      while (System.currentTimeMillis() - startTime < 120000 &&
             distBlockFixer.submittedJobs.size() == 0) { 
        distBlockFixer.getCorruptionMonitor().checkAndReconstructBlocks();
        LOG.info("Waiting for jobs to submit");
        Thread.sleep(10000);
      }
      int submittedJob = distBlockFixer.submittedJobs.size();
      LOG.info("Already Submitted " + submittedJob + " jobs");
      assertTrue("Should submit more than 1 jobs", submittedJob >= 1);
      

      // Corrupt two more blocks
      corruptBlockIdxs = new Integer[]{4, 5};
      LOG.info("Corrupt block " + corruptBlockIdxs + " of directory " + dirPath);
      TestDirectoryRaidDfs.corruptBlocksInDirectory(conf, dirPath,
          crcs, corruptBlockIdxs, fileSys, dfsCluster, false, true);
      
      // A new job should be submitted since two blocks are corrupt.
      startTime = System.currentTimeMillis();
      while (System.currentTimeMillis() - startTime < 120000 &&
             distBlockFixer.submittedJobs.size() == submittedJob) { 
        distBlockFixer.getCorruptionMonitor().checkAndReconstructBlocks();
        LOG.info("Waiting for more jobs to submit");
        Thread.sleep(10000);
      }
      LOG.info("Already Submitted " + distBlockFixer.submittedJobs.size()  + " jobs");
      assertTrue("should submit more than 1 jobs",
          distBlockFixer.submittedJobs.size() - submittedJob >= 1);
    } finally {
      myTearDown();
    }
  }

  public static DistributedFileSystem getDFS(
        Configuration conf, FileSystem dfs) throws IOException {
    Configuration clientConf = new Configuration(conf);
    clientConf.set("fs.hdfs.impl",
                   "org.apache.hadoop.hdfs.DistributedFileSystem");
    clientConf.setBoolean("fs.hdfs.impl.disable.cache", true);
    URI dfsUri = dfs.getUri();
    FileSystem.closeAll();
    return (DistributedFileSystem) FileSystem.get(dfsUri, clientConf);
  }

  private void mySetup(int stripeLength) throws Exception {
    if (System.getProperty("hadoop.log.dir") == null) {
      String base = new File(".").getAbsolutePath();
      System.setProperty("hadoop.log.dir", new Path(base).toString() + "/logs");
    }

    new File(TEST_DIR).mkdirs(); // Make sure data directory exists
    conf = new Configuration();

    conf.set("raid.config.file", CONFIG_FILE);
    conf.setBoolean("raid.config.reload", true);
    conf.setLong("raid.config.reload.interval", RELOAD_INTERVAL);

    // scan all policies once every 5 second
    conf.setLong("raid.policy.rescan.interval", 5000);

    // do not use map-reduce cluster for Raiding
    conf.set("raid.classname", "org.apache.hadoop.raid.LocalRaidNode");
    conf.set("raid.server.address", "localhost:" + MiniDFSCluster.getFreePort());
    conf.set("mapred.raid.http.address", "localhost:0");

    Utils.loadTestCodecs(conf, stripeLength, stripeLength, 1, 3, "/destraid",
        "/destraidrs", false, true);

    conf.setBoolean("dfs.permissions", false);
    // Make sure initial repl is smaller than NUM_DATANODES
    conf.setInt(RaidNode.RAID_PARITY_INITIAL_REPL_KEY, 1);

    dfsCluster = new MiniDFSCluster(conf, NUM_DATANODES, true, null);
    dfsCluster.waitActive();
    fileSys = dfsCluster.getFileSystem();
    namenode = fileSys.getUri().toString();

    FileSystem.setDefaultUri(conf, namenode);
    mr = new MiniMRCluster(4, namenode, 3);
    jobTrackerName = "localhost:" + mr.getJobTrackerPort();
    hftp = "hftp://localhost.localdomain:" + dfsCluster.getNameNodePort();

    FileSystem.setDefaultUri(conf, namenode);
    conf.set("mapred.job.tracker", jobTrackerName);
    conf.set(RaidNode.RAID_CHECKSUM_STORE_CLASS_KEY,
        "org.apache.hadoop.raid.LocalChecksumStore");
    conf.setBoolean(RaidNode.RAID_CHECKSUM_STORE_REQUIRED_KEY, true);
    conf.set(LocalChecksumStore.LOCAL_CHECK_STORE_DIR_KEY, CHECKSUM_STORE_DIR);
    conf.set(RaidNode.RAID_STRIPE_STORE_CLASS_KEY,
        "org.apache.hadoop.raid.LocalStripeStore");
    conf.set(LocalStripeStore.LOCAL_STRIPE_STORE_DIR_KEY, STRIPE_STORE_DIR);
    ConfigBuilder cb = new ConfigBuilder(CONFIG_FILE);
    cb.addPolicy("RaidTest1", "/user/dhruba/raidtest",
        1, 1);
    cb.addPolicy("RaidTest2", "/user/dhruba/raidtestrs",
        1, 1, "rs");
    cb.persist();
  }

  private void myTearDown() throws Exception {
    if (cnode != null) { cnode.stop(); cnode.join(); }
    if (mr != null) { mr.shutdown(); }
    if (dfsCluster != null) { dfsCluster.shutdown(); }
  }

  static void corruptBlock(Block block, MiniDFSCluster dfs) throws IOException {
    boolean corrupted = false;
    for (int i = 0; i < NUM_DATANODES; i++) {
      corrupted |= TestDatanodeBlockScanner.corruptReplica(block, i, dfs);
    }
    assertTrue("could not corrupt block", corrupted);
  }
}

