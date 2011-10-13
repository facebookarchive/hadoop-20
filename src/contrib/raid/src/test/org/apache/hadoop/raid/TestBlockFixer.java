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
import java.io.FileWriter;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.net.URI;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.zip.CRC32;

import junit.framework.TestCase;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.RaidDFSUtil;
import org.apache.hadoop.hdfs.TestRaidDfs;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.mapred.MiniMRCluster;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.raid.DistBlockIntegrityMonitor.Priority;
import org.apache.hadoop.raid.DistBlockIntegrityMonitor.Worker.LostFileInfo;
import org.apache.hadoop.util.StringUtils;
import org.junit.Test;


public class TestBlockFixer extends TestCase {
  final static Log LOG = LogFactory.getLog(
                            "org.apache.hadoop.raid.TestBlockFixer");
  final static String TEST_DIR = new File(System.getProperty("test.build.data",
      "build/contrib/raid/test/data")).getAbsolutePath();
  final static String CONFIG_FILE = new File(TEST_DIR, 
      "test-raid.xml").getAbsolutePath();
  final static long RELOAD_INTERVAL = 1000;
  final static int NUM_DATANODES = 3;
  Configuration conf;
  String namenode = null;
  MiniDFSCluster dfs = null;
  String hftp = null;
  MiniMRCluster mr = null;
  FileSystem fileSys = null;
  RaidNode cnode = null;
  String jobTrackerName = null;
  Random rand = new Random();
  static {
    ParityFilePair.disableCacheUsedInTestOnly();
  }
  
  @Test
  public void testPriorityEnum() {
    // Verify proper ordering of in-place values 
    assertTrue(Priority.HIGH.higherThan(Priority.LOW));
    assertTrue(Priority.LOW.higherThan(Priority.LOWEST));
    
    // Verify ordering of higherThan (guards against bad additions)
    int i = 0;
    for (Priority p : Priority.values()) {
      int j = 0;
      for (Priority q : Priority.values()) {
        boolean gt = p.higherThan(q);
        
        if (i > j) {
          assertTrue("Priority.gt() returned a bad value", gt);
        } else {
          assertFalse("Priority.gt() returned a bad value", gt);
        }
        
        j--;
      }
      i--;
    }
  }

  @Test
  public void testFilterUnfixableFiles() throws IOException {
    conf = new Configuration();
    dfs = new MiniDFSCluster(conf, NUM_DATANODES, true, null);
    dfs.waitActive();
    FileSystem fs = dfs.getFileSystem();

    try {
      Configuration testConf = new Configuration();
      testConf.set("hdfs.raid.locations", "/raid");
      testConf.set("hdfs.raidrs.locations", "/raidrs");
      BlockIntegrityMonitor blockFixer = new LocalBlockIntegrityMonitor(testConf);

      String p1 = "/user/foo/f1";
      String p2 = "/user/foo/f2";
      String p3 = "/user/foo/bar/f1";
      String p4 = "/raid/user/foo";
      String p5 = "/raidrs/user/foo/bar";
      fs.mkdirs(new Path(p4));

      List<String> fileList = new ArrayList<String>();
      fileList.add(p1);
      fileList.add(p2);
      fileList.add(p3);

      blockFixer.filterUnreconstructableSourceFiles(fs, fileList.iterator());
      // p3 should be filtered out.
      assertEquals(2, fileList.size());

      Set<String> filtered = new HashSet<String>();
      for (String p: fileList) filtered.add(p);
      assertFalse("File not filtered", filtered.contains(p3));

      fileList.add(p3);
      fs.mkdirs(new Path(p5));
      blockFixer.filterUnreconstructableSourceFiles(fs, fileList.iterator());
      // Nothing is filtered.
      assertEquals(3, fileList.size());
    } finally {
      dfs.shutdown();
    }
  }

  /**
   * Tests isXorParityFile and isRsParityFile
   */
  @Test
  public void testIsParityFile() throws IOException {
    Configuration testConf = new Configuration();
    testConf.set("hdfs.raid.locations", "/raid");
    testConf.set("hdfs.raidrs.locations", "/raidrs");

    BlockReconstructor.CorruptBlockReconstructor helper =
      new BlockReconstructor.CorruptBlockReconstructor(testConf);

    assertFalse("incorrectly identified rs parity file as xor parity file",
                helper.isXorParityFile(new Path("/raidrs/test/test")));
    assertTrue("could not identify rs parity file",
               helper.isRsParityFile(new Path("/raidrs/test/test")));
    assertTrue("could not identify xor parity file",
               helper.isXorParityFile(new Path("/raid/test/test")));
    assertFalse("incorrectly identified xor parity file as rs parity file",
                helper.isRsParityFile(new Path("/raid/test/test")));
  }


  /**
   * Test the filtering of trash files from the list of corrupt files.
   */
  @Test
  public void testTrashFilter() {
    List<String> files = new LinkedList<String>();
    // Paths that do not match the trash pattern.
    String p1 = "/user/raid/raidtest/f1";
    String p2 = "/user/.Trash/";
    // Paths that match the trash pattern.
    String p3 = "/user/raid/.Trash/raidtest/f1";
    String p4 = "/user/raid/.Trash/";
    String p5 = "/tmp/foo";
    files.add(p1);
    files.add(p3);
    files.add(p4);
    files.add(p2);
    files.add(p5);

    Configuration conf = new Configuration();
    RaidUtils.filterTrash(conf, files);

    assertEquals("expected 2 non-trash files but got " + files.size(),
                 2, files.size());
    for (String p: files) {
      assertTrue("wrong file returned by filterTrash",
                 p == p1 || p == p2);
    }
  }

  @Test
  public void testBlockFixDist() throws Exception {
    implBlockFix(false);
  }

  @Test
  public void testBlockFixLocal() throws Exception {
    implBlockFix(true);
  }

  /**
   * Create a file with three stripes, corrupt a block each in two stripes,
   * and wait for the the file to be fixed.
   */
  private void implBlockFix(boolean local) throws Exception {
    LOG.info("Test testBlockFix started.");
    long blockSize = 8192L;
    int stripeLength = 3;
    mySetup(stripeLength, -1); // never har
    Path file1 = new Path("/user/dhruba/raidtest/file1");
    Path destPath = new Path("/destraid/user/dhruba/raidtest");
    long crc1 = TestRaidDfs.createTestFilePartialLastBlock(fileSys, file1,
                                                          1, 7, blockSize);
    long file1Len = fileSys.getFileStatus(file1).getLen();
    LOG.info("Test testBlockFix created test files");

    // create an instance of the RaidNode
    Configuration localConf = new Configuration(conf);
    localConf.set(RaidNode.RAID_LOCATION_KEY, "/destraid");
    localConf.setInt("raid.blockfix.interval", 1000);
    if (local) {
      localConf.set("raid.blockfix.classname",
                    "org.apache.hadoop.raid.LocalBlockIntegrityMonitor");
    } else {
      localConf.set("raid.blockfix.classname",
                    "org.apache.hadoop.raid.DistBlockIntegrityMonitor");
    }
    localConf.setLong("raid.blockfix.filespertask", 2L);

    try {
      cnode = RaidNode.createRaidNode(null, localConf);
      TestRaidDfs.waitForFileRaided(LOG, fileSys, file1, destPath);
      cnode.stop(); cnode.join();
      
      FileStatus srcStat = fileSys.getFileStatus(file1);
      DistributedFileSystem dfs = (DistributedFileSystem)fileSys;
      LocatedBlocks locs = RaidDFSUtil.getBlockLocations(
        dfs, file1.toUri().getPath(), 0, srcStat.getLen());

      String[] corruptFiles = DFSUtil.getCorruptFiles(dfs);
      assertEquals("no corrupt files expected", 0, corruptFiles.length);
      assertEquals("filesFixed() should return 0 before fixing files",
                   0, cnode.blockIntegrityMonitor.getNumFilesFixed());
      
      // Corrupt blocks in two different stripes. We can fix them.
      int[] corruptBlockIdxs = new int[]{0, 4, 6};
      for (int idx: corruptBlockIdxs)
        corruptBlock(locs.get(idx).getBlock().getBlockName());
      reportCorruptBlocks(dfs, file1, corruptBlockIdxs, blockSize);
      
      corruptFiles = DFSUtil.getCorruptFiles(dfs);
      assertEquals("file not corrupted", 1, corruptFiles.length);
      assertEquals("wrong file corrupted",
                   corruptFiles[0], file1.toUri().getPath());
      assertEquals("wrong number of corrupt blocks", 3,
        RaidDFSUtil.corruptBlocksInFile(dfs, file1.toUri().getPath(), 0,
          srcStat.getLen()).size());

      cnode = RaidNode.createRaidNode(null, localConf);
      long start = System.currentTimeMillis();
      while (cnode.blockIntegrityMonitor.getNumFilesFixed() < 1 &&
             System.currentTimeMillis() - start < 120000) {
        LOG.info("Test testBlockFix waiting for files to be fixed.");
        Thread.sleep(1000);
      }
      assertEquals("file not fixed", 1, cnode.blockIntegrityMonitor.getNumFilesFixed());
      
      dfs = getDFS(conf, dfs);
      assertTrue("file not fixed",
                 TestRaidDfs.validateFile(dfs, file1, file1Len, crc1));

    } catch (Exception e) {
      LOG.info("Test testBlockFix Exception " + e +
               StringUtils.stringifyException(e));
      throw e;
    } finally {
      myTearDown();
    }
    LOG.info("Test testBlockFix completed.");
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
    long blockSize = 8192L;
    int stripeLength = 3;
    mySetup(stripeLength, -1); // never har
    Path file1 = new Path("/user/dhruba/raidtest/file1");
    Path destPath = new Path("/destraid/user/dhruba/raidtest");
    long crc1 = TestRaidDfs.createTestFile(fileSys, file1, 1, 7, blockSize);
    long file1Len = fileSys.getFileStatus(file1).getLen();
    LOG.info("Test " + testName + " created test files");

    // create an instance of the RaidNode
    Configuration localConf = new Configuration(conf);
    localConf.set(RaidNode.RAID_LOCATION_KEY, "/destraid");
    localConf.setInt("raid.blockfix.interval", 1000);
    if (local) {
      localConf.set("raid.blockfix.classname",
                    "org.apache.hadoop.raid.LocalBlockIntegrityMonitor");
    } else {
      localConf.set("raid.blockfix.classname",
                    "org.apache.hadoop.raid.DistBlockIntegrityMonitor");
    }
    localConf.setLong("raid.blockfix.filespertask", 2L);
    try {
      cnode = RaidNode.createRaidNode(null, localConf);
      TestRaidDfs.waitForFileRaided(LOG, fileSys, file1, destPath);
      cnode.stop(); cnode.join();
      
      FileStatus srcStat = fileSys.getFileStatus(file1);
      DistributedFileSystem dfs = (DistributedFileSystem)fileSys;
      LocatedBlocks locs = RaidDFSUtil.getBlockLocations(
        dfs, file1.toUri().getPath(), 0, srcStat.getLen());

      String[] corruptFiles = DFSUtil.getCorruptFiles(dfs);
      assertEquals("no corrupt files expected", 0, corruptFiles.length);
      assertEquals("filesFixed() should return 0 before fixing files",
                   0, cnode.blockIntegrityMonitor.getNumFilesFixed());
      
      corruptBlock(locs.get(0).getBlock().getBlockName());
      reportCorruptBlocks(dfs, file1, new int[]{0}, blockSize);
      
      corruptFiles = DFSUtil.getCorruptFiles(dfs);
      assertEquals("file not corrupted",
                   1, corruptFiles.length);
      assertEquals("wrong file corrupted",
                   corruptFiles[0], file1.toUri().getPath());
      
      cnode = RaidNode.createRaidNode(null, localConf);
      long start = System.currentTimeMillis();
      while (cnode.blockIntegrityMonitor.getNumFilesFixed() < 1 &&
             System.currentTimeMillis() - start < 120000) {
        LOG.info("Test " + testName + " waiting for files to be fixed.");
        Thread.sleep(1000);
      }
      assertEquals("file not fixed",
                   1, cnode.blockIntegrityMonitor.getNumFilesFixed());
      
      // Stop RaidNode
      cnode.stop(); cnode.join(); cnode = null;

      // The block has successfully been reconstructed.
      dfs = getDFS(conf, dfs);
      assertTrue("file not fixed",
                 TestRaidDfs.validateFile(dfs, file1, file1Len, crc1));

      // Now corrupt the generated block.
      locs = RaidDFSUtil.getBlockLocations(
        dfs, file1.toUri().getPath(), 0, srcStat.getLen());
      corruptBlock(locs.get(0).getBlock().getBlockName());
      reportCorruptBlocks(dfs, file1, new int[]{0}, blockSize);

      try {
        Thread.sleep(5*1000);
      } catch (InterruptedException ignore) {
      }
      try {
        TestRaidDfs.validateFile(dfs, file1, file1Len, crc1);
        fail("Expected exception not thrown");
      } catch (org.apache.hadoop.fs.ChecksumException ce) {
      } catch (org.apache.hadoop.fs.BlockMissingException bme) {
      }
    } catch (Exception e) {
      LOG.info("Test " + testName + " Exception " + e +
               StringUtils.stringifyException(e));
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
  public void testGeneratedBlockDist() throws Exception {
    generatedBlockTestCommon("testGeneratedBlock", 3, false);
  }

  /**
   * Tests integrity of generated block.
   * Create a file and delete a block entirely. Wait for the block to be
   * regenerated. Now stop RaidNode and corrupt the generated block.
   * Test that corruption in the generated block can be detected by clients.
   */
  @Test
  public void testGeneratedBlockLocal() throws Exception {
    generatedBlockTestCommon("testGeneratedBlock", 3, true);
  }

  /**
   * Tests integrity of generated last block.
   * Create a file and delete a block entirely. Wait for the block to be
   * regenerated. Now stop RaidNode and corrupt the generated block.
   * Test that corruption in the generated block can be detected by clients.
   */
  @Test
  public void testGeneratedLastBlockDist() throws Exception {
    generatedBlockTestCommon("testGeneratedLastBlock", 6, false);
  }

  /**
   * Tests integrity of generated last block.
   * Create a file and delete a block entirely. Wait for the block to be
   * regenerated. Now stop RaidNode and corrupt the generated block.
   * Test that corruption in the generated block can be detected by clients.
   */
  @Test
  public void testGeneratedLastBlockLocal() throws Exception {
    generatedBlockTestCommon("testGeneratedLastBlock", 6, true);
  }

  @Test
  public void testParityBlockFixDist() throws Exception {
    implParityBlockFix("testParityBlockFixDist", false);
  }

  @Test
  public void testParityBlockFixLocal() throws Exception {
    implParityBlockFix("testParityBlockFixLocal", true);
  }

  /**
   * Corrupt a parity file and wait for it to get fixed.
   */
  private void implParityBlockFix(String testName, boolean local)
    throws Exception {
    LOG.info("Test " + testName + " started.");
    long blockSize = 8192L;
    int stripeLength = 3;
    mySetup(stripeLength, -1); // never har
    Path file1 = new Path("/user/dhruba/raidtest/file1");
    Path destPath = new Path("/destraid/user/dhruba/raidtest");
    Path parityFile = new Path("/destraid/user/dhruba/raidtest/file1");
    TestRaidDfs.createTestFilePartialLastBlock(fileSys, file1,
                                                          1, 7, blockSize);
    long file1Len = fileSys.getFileStatus(file1).getLen();
    LOG.info("Test " + testName + " created test files");

    // create an instance of the RaidNode
    Configuration localConf = new Configuration(conf);
    localConf.set(RaidNode.RAID_LOCATION_KEY, "/destraid");
    localConf.setInt("raid.blockfix.interval", 1000);
    if (local) {
      localConf.set("raid.blockfix.classname",
                    "org.apache.hadoop.raid.LocalBlockIntegrityMonitor");
    } else {
      localConf.set("raid.blockfix.classname",
                    "org.apache.hadoop.raid.DistBlockIntegrityMonitor");
    }
    localConf.setLong("raid.blockfix.filespertask", 2L);

    try {
      cnode = RaidNode.createRaidNode(null, localConf);
      TestRaidDfs.waitForFileRaided(LOG, fileSys, file1, destPath);
      cnode.stop(); cnode.join();

      long parityCRC = getCRC(fileSys, parityFile);

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
        corruptBlock(locs.get(idx).getBlock().getBlockName());
      reportCorruptBlocks(dfs, parityFile, corruptBlockIdxs, blockSize);

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
        Thread.sleep(1000);
      }
      assertEquals("file not fixed",
                   1, cnode.blockIntegrityMonitor.getNumFilesFixed());

      long checkCRC = getCRC(fileSys, parityFile);

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

  @Test
  public void testParityHarBlockFixDist() throws Exception {
    implParityHarBlockFix("testParityHarBlockFixDist", false);
  }

  @Test
  public void testParityHarBlockFixLocal() throws Exception {
    implParityHarBlockFix("testParityHarBlockFixLocal", true);
  }

  private void implParityHarBlockFix(String testName, boolean local)
    throws Exception {
    LOG.info("Test " + testName + " started.");
    long blockSize = 8192L;
    int stripeLength = 3;
    mySetup(stripeLength, 0); // Time before har = 0 days.
    Path file1 = new Path("/user/dhruba/raidtest/file1");
    Path destPath = new Path("/destraid/user/dhruba/raidtest");
    // Parity file will have 7 blocks.
    TestRaidDfs.createTestFilePartialLastBlock(fileSys, file1,
                                               1, 20, blockSize);
    long file1Len = fileSys.getFileStatus(file1).getLen();
    LOG.info("Test " + testName + " created test files");

    // create an instance of the RaidNode
    Configuration localConf = new Configuration(conf);
    localConf.set(RaidNode.RAID_LOCATION_KEY, "/destraid");
    localConf.setInt("raid.blockfix.interval", 1000);
    if (local) {
      localConf.set("raid.blockfix.classname",
                    "org.apache.hadoop.raid.LocalBlockIntegrityMonitor");
    } else {
      localConf.set("raid.blockfix.classname",
                    "org.apache.hadoop.raid.DistBlockIntegrityMonitor");
    }
    localConf.setLong("raid.blockfix.filespertask", 2L);

    try {
      cnode = RaidNode.createRaidNode(null, localConf);
      Path harDirectory =
        new Path("/destraid/user/dhruba/raidtest/raidtest" +
                 RaidNode.HAR_SUFFIX);
      long start = System.currentTimeMillis();
      while (System.currentTimeMillis() - start < 1000 * 120) {
        if (fileSys.exists(harDirectory)) {
          break;
        }
        LOG.info("Test " + testName + " waiting for har");
        Thread.sleep(1000);
      }

      Path partFile = new Path(harDirectory, "part-0");
      long partCRC = getCRC(fileSys, partFile);
      FileStatus partStat = fileSys.getFileStatus(partFile);
      DistributedFileSystem dfs = (DistributedFileSystem)fileSys;
      LocatedBlocks locs = RaidDFSUtil.getBlockLocations(
        dfs, partFile.toUri().getPath(), 0, partStat.getLen());
      assertEquals("wrong number of har blocks",
                   7, locs.getLocatedBlocks().size());
      cnode.stop(); cnode.join();

      String[] corruptFiles = DFSUtil.getCorruptFiles(dfs);
      assertEquals("no corrupt files expected", 0, corruptFiles.length);
      assertEquals("filesFixed() should return 0 before fixing files",
                   0, cnode.blockIntegrityMonitor.getNumFilesFixed());

      // Corrupt parity blocks for different stripes.
      int[] corruptBlockIdxs = new int[]{0, 1, 6};
      for (int idx: corruptBlockIdxs)
        corruptBlock(locs.get(idx).getBlock().getBlockName());
      reportCorruptBlocks(dfs, partFile, corruptBlockIdxs,
        partStat.getBlockSize());

      corruptFiles = DFSUtil.getCorruptFiles(dfs);
      assertEquals("file not corrupted", 1, corruptFiles.length);
      assertEquals("wrong file corrupted",
                   corruptFiles[0], partFile.toUri().getPath());

      cnode = RaidNode.createRaidNode(null, localConf);
      start = System.currentTimeMillis();
      while (cnode.blockIntegrityMonitor.getNumFilesFixed() < 1 &&
             System.currentTimeMillis() - start < 120000) {
        LOG.info("Test " + testName + " waiting for files to be fixed.");
        Thread.sleep(1000);
      }
      assertEquals("file not fixed",
                   1, cnode.blockIntegrityMonitor.getNumFilesFixed());

      long checkCRC = getCRC(fileSys, partFile);

      assertEquals("file not fixed",
                   partCRC, checkCRC);
    } catch (Exception e) {
      LOG.info("Test " + testName + " Exception " + e +
               StringUtils.stringifyException(e));
      throw e;
    } finally {
      myTearDown();
    }
    LOG.info("Test " + testName + " completed.");
  }


  /**
   * tests that we can have 2 concurrent jobs fixing files 
   * (dist block fixer)
   */
  @Test
  public void testConcurrentJobs() throws Exception {
    LOG.info("Test testConcurrentJobs started.");
    long blockSize = 8192L;
    int stripeLength = 3;
    mySetup(stripeLength, -1); // never har
    Path file1 = new Path("/user/dhruba/raidtest/file1");
    Path file2 = new Path("/user/dhruba/raidtest/file2");
    Path destPath = new Path("/destraid/user/dhruba/raidtest");
    long crc1 = TestRaidDfs.createTestFilePartialLastBlock(fileSys, file1,
                                                          1, 20, blockSize);
    long crc2 = TestRaidDfs.createTestFilePartialLastBlock(fileSys, file2,
                                                          1, 20, blockSize);
    long file1Len = fileSys.getFileStatus(file1).getLen();
    long file2Len = fileSys.getFileStatus(file2).getLen();
    LOG.info("Test testConcurrentJobs created test files");

    // create an instance of the RaidNode
    Configuration localConf = new Configuration(conf);
    localConf.set(RaidNode.RAID_LOCATION_KEY, "/destraid");
    localConf.setInt("raid.blockfix.interval", 1000);
    localConf.set("raid.blockfix.classname", 
                  "org.apache.hadoop.raid.DistBlockIntegrityMonitor");
    localConf.setLong("raid.blockfix.filespertask", 2L);

    try {
      cnode = RaidNode.createRaidNode(null, localConf);
      TestRaidDfs.waitForFileRaided(LOG, fileSys, file1, destPath);
      TestRaidDfs.waitForFileRaided(LOG, fileSys, file2, destPath);
      cnode.stop(); cnode.join();

      FileStatus file1Stat = fileSys.getFileStatus(file1);
      FileStatus file2Stat = fileSys.getFileStatus(file2);
      DistributedFileSystem dfs = (DistributedFileSystem)fileSys;
      LocatedBlocks file1Loc =
        RaidDFSUtil.getBlockLocations(dfs, file1.toUri().getPath(),
                                      0, file1Stat.getLen());
      LocatedBlocks file2Loc =
        RaidDFSUtil.getBlockLocations(dfs, file2.toUri().getPath(),
                                      0, file2Stat.getLen());
      
      String[] corruptFiles = DFSUtil.getCorruptFiles(dfs);
      assertEquals("no corrupt files expected", 0, corruptFiles.length);
      assertEquals("filesFixed() should return 0 before fixing files",
                   0, cnode.blockIntegrityMonitor.getNumFilesFixed());

      // corrupt file1
      int[] corruptBlockIdxs = new int[]{0, 4, 6};
      for (int idx: corruptBlockIdxs)
        corruptBlock(file1Loc.get(idx).getBlock().getBlockName());
      reportCorruptBlocks(dfs, file1, corruptBlockIdxs, blockSize);

      cnode = RaidNode.createRaidNode(null, localConf);
      DistBlockIntegrityMonitor blockFixer = (DistBlockIntegrityMonitor) cnode.blockIntegrityMonitor;
      long start = System.currentTimeMillis();

      while (blockFixer.jobsRunning() < 1 &&
             System.currentTimeMillis() - start < 240000) {
        LOG.info("Test testBlockFix waiting for fixing job 1 to start");
        Thread.sleep(10);
      }
      assertEquals("job 1 not running", 1, blockFixer.jobsRunning());

      // corrupt file2
      for (int idx: corruptBlockIdxs)
        corruptBlock(file2Loc.get(idx).getBlock().getBlockName());
      reportCorruptBlocks(dfs, file2, corruptBlockIdxs, blockSize);
      
      while (blockFixer.jobsRunning() < 2 &&
             System.currentTimeMillis() - start < 240000) {
        LOG.info("Test testBlockFix waiting for fixing job 2 to start");
        Thread.sleep(10);
      }
      assertTrue(blockFixer.jobsRunning() >= 2);

      while (blockFixer.getNumFilesFixed() < 2 &&
             System.currentTimeMillis() - start < 240000) {
        LOG.info("Test testBlockFix waiting for files to be fixed.");
        Thread.sleep(10);
      }
      assertEquals("files not fixed", 2, blockFixer.getNumFilesFixed());

      dfs = getDFS(conf, dfs);
      
      try {
        Thread.sleep(5*1000);
      } catch (InterruptedException ignore) {
      }
      assertTrue("file not fixed",
                 TestRaidDfs.validateFile(dfs, file1, file1Len, crc1));
      assertTrue("file not fixed",
                 TestRaidDfs.validateFile(dfs, file2, file2Len, crc2));
    } catch (Exception e) {
      LOG.info("Test testConcurrentJobs exception " + e +
               StringUtils.stringifyException(e));
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
    long blockSize = 8192L;
    int stripeLength = 3;
    mySetup(stripeLength, -1); // never har
    Path file1 = new Path("/user/dhruba/raidtest/file1");
    Path file2 = new Path("/user/dhruba/raidtest/file2");
    Path destPath = new Path("/destraid/user/dhruba/raidtest");
    long crc1 = TestRaidDfs.createTestFilePartialLastBlock(fileSys, file1,
                                                          1, 20, blockSize);
    long crc2 = TestRaidDfs.createTestFilePartialLastBlock(fileSys, file2,
                                                          1, 20, blockSize);
    long file1Len = fileSys.getFileStatus(file1).getLen();
    long file2Len = fileSys.getFileStatus(file2).getLen();
    LOG.info("Test testMaxPendingJobs created test files");

    // create an instance of the RaidNode
    Configuration localConf = new Configuration(conf);
    localConf.set(RaidNode.RAID_LOCATION_KEY, "/destraid");
    localConf.setInt("raid.blockfix.interval", 1000);
    localConf.set("raid.blockfix.classname", 
                  "org.apache.hadoop.raid.DistBlockIntegrityMonitor");
    localConf.setLong("raid.blockfix.filespertask", 2L);
    localConf.setLong("raid.blockfix.maxpendingjobs", 1L);

    try {
      cnode = RaidNode.createRaidNode(null, localConf);
      TestRaidDfs.waitForFileRaided(LOG, fileSys, file1, destPath);
      TestRaidDfs.waitForFileRaided(LOG, fileSys, file2, destPath);
      cnode.stop(); cnode.join();

      FileStatus file1Stat = fileSys.getFileStatus(file1);
      FileStatus file2Stat = fileSys.getFileStatus(file2);
      DistributedFileSystem dfs = (DistributedFileSystem)fileSys;
      LocatedBlocks file1Loc =
        RaidDFSUtil.getBlockLocations(dfs, file1.toUri().getPath(),
                                      0, file1Stat.getLen());
      LocatedBlocks file2Loc =
        RaidDFSUtil.getBlockLocations(dfs, file2.toUri().getPath(),
                                      0, file2Stat.getLen());

      String[] corruptFiles = DFSUtil.getCorruptFiles(dfs);
      assertEquals("no corrupt files expected", 0, corruptFiles.length);
      assertEquals("filesFixed() should return 0 before fixing files",
                   0, cnode.blockIntegrityMonitor.getNumFilesFixed());

      // corrupt file1
      int[] corruptBlockIdxs = new int[]{0, 4, 6};
      for (int idx: corruptBlockIdxs)
        corruptBlock(file1Loc.get(idx).getBlock().getBlockName());
      reportCorruptBlocks(dfs, file1, corruptBlockIdxs, blockSize);
      corruptFiles = DFSUtil.getCorruptFiles(dfs);

      cnode = RaidNode.createRaidNode(null, localConf);
      DistBlockIntegrityMonitor blockFixer = (DistBlockIntegrityMonitor) cnode.blockIntegrityMonitor;
      long start = System.currentTimeMillis();

      while (blockFixer.jobsRunning() < 1 &&
             System.currentTimeMillis() - start < 240000) {
        LOG.info("Test testBlockFix waiting for fixing job 1 to start");
        Thread.sleep(10);
      }
      assertEquals("job not running", 1, blockFixer.jobsRunning());

      // corrupt file2
      for (int idx: corruptBlockIdxs)
        corruptBlock(file2Loc.get(idx).getBlock().getBlockName());
      reportCorruptBlocks(dfs, file2, corruptBlockIdxs, blockSize);
      corruptFiles = DFSUtil.getCorruptFiles(dfs);
      
      // wait until both files are fixed
      while (blockFixer.getNumFilesFixed() < 2 &&
             System.currentTimeMillis() - start < 240000) {
        // make sure the block fixer does not start a second job while
        // the first one is still running
        assertTrue("too many jobs running", blockFixer.jobsRunning() <= 1);
        Thread.sleep(10);
      }
      assertEquals("files not fixed", 2, blockFixer.getNumFilesFixed());

      dfs = getDFS(conf, dfs);
      
      try {
        Thread.sleep(5*1000);
      } catch (InterruptedException ignore) {
      }
      assertTrue("file not fixed",
                 TestRaidDfs.validateFile(dfs, file1, file1Len, crc1));
      assertTrue("file not fixed",
                 TestRaidDfs.validateFile(dfs, file2, file2Len, crc2));
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
    FakeDistBlockIntegrityMonitor(Configuration conf) {
      super(conf);
    }

    @Override
    void submitJob(Job job, List<String> filesInJob, Priority priority,
        Map<Job, List<LostFileInfo>> jobIndex) {

      LOG.info("Job " + job.getJobName() + " was submitted ");
      submittedJobs.put(job.getJobName(), filesInJob);
    }
  }
  


  public void testMultiplePriorities() throws Exception {
    Path srcFile = new Path("/home/test/file1");
    int repl = 1;
    int numBlocks = 8;
    long blockSize = 16384;
    int stripeLength = 3;
    Path destPath = new Path("/raidrs");
    ErasureCodeType code = ErasureCodeType.RS;
    mySetup(stripeLength, -1); // never har
    try {
      // Create test file and raid it.
      TestRaidDfs.createTestFilePartialLastBlock(
        fileSys, srcFile, repl, numBlocks, blockSize);
      FileStatus stat = fileSys.getFileStatus(srcFile);
      RaidNode.doRaid(conf, stat,
        destPath, code, new RaidNode.Statistics(), RaidUtils.NULL_PROGRESSABLE,
        false, repl, repl, stripeLength);

      // Corrupt first block of file.
      int blockIdxToCorrupt = 1;
      LOG.info("Corrupt block " + blockIdxToCorrupt + " of file " + srcFile);
      LocatedBlocks locations = getBlockLocations(srcFile, stat.getLen());
      corruptBlock(locations.get(blockIdxToCorrupt).getBlock().getBlockName());
      reportCorruptBlocks(fileSys, srcFile, new int[]{1}, blockSize);

      // Create Block Fixer and fix.
      FakeDistBlockIntegrityMonitor distBlockFixer = new FakeDistBlockIntegrityMonitor(conf);
      assertEquals(0, distBlockFixer.submittedJobs.size());

      // One job should be submitted.
      distBlockFixer.getCorruptionMonitor().checkAndReconstructBlocks();
      assertEquals(1, distBlockFixer.submittedJobs.size());

      // No new job should be submitted since we already have one.
      distBlockFixer.getCorruptionMonitor().checkAndReconstructBlocks();
      assertEquals(1, distBlockFixer.submittedJobs.size());

      // Corrupt one more block.
      blockIdxToCorrupt = 4;
      LOG.info("Corrupt block " + blockIdxToCorrupt + " of file " + srcFile);
      locations = getBlockLocations(srcFile, stat.getLen());
      corruptBlock(locations.get(blockIdxToCorrupt).getBlock().getBlockName());
      reportCorruptBlocks(fileSys, srcFile, new int[]{4}, blockSize);

      // A new job should be submitted since two blocks are corrupt.
      distBlockFixer.getCorruptionMonitor().checkAndReconstructBlocks();
      assertEquals(2, distBlockFixer.submittedJobs.size());
    } finally {
      myTearDown();
    }
  }

  private static DistributedFileSystem getDFS(
        Configuration conf, FileSystem dfs) throws IOException {
    Configuration clientConf = new Configuration(conf);
    clientConf.set("fs.hdfs.impl",
                   "org.apache.hadoop.hdfs.DistributedFileSystem");
    clientConf.setBoolean("fs.hdfs.impl.disable.cache", true);
    URI dfsUri = dfs.getUri();
    FileSystem.closeAll();
    return (DistributedFileSystem) FileSystem.get(dfsUri, clientConf);
  }

  private void mySetup(int stripeLength, int timeBeforeHar) throws Exception {

    new File(TEST_DIR).mkdirs(); // Make sure data directory exists
    conf = new Configuration();

    conf.set("raid.config.file", CONFIG_FILE);
    conf.setBoolean("raid.config.reload", true);
    conf.setLong("raid.config.reload.interval", RELOAD_INTERVAL);

    // scan all policies once every 5 second
    conf.setLong("raid.policy.rescan.interval", 5000);

    // do not use map-reduce cluster for Raiding
    conf.set("raid.classname", "org.apache.hadoop.raid.LocalRaidNode");
    conf.set("raid.server.address", "localhost:0");
    conf.setInt("hdfs.raid.stripeLength", stripeLength);
    conf.set("hdfs.raid.locations", "/destraid");

    conf.setBoolean("dfs.permissions", false);

    dfs = new MiniDFSCluster(conf, NUM_DATANODES, true, null);
    dfs.waitActive();
    fileSys = dfs.getFileSystem();
    namenode = fileSys.getUri().toString();

    FileSystem.setDefaultUri(conf, namenode);
    mr = new MiniMRCluster(4, namenode, 3);
    jobTrackerName = "localhost:" + mr.getJobTrackerPort();
    hftp = "hftp://localhost.localdomain:" + dfs.getNameNodePort();

    FileSystem.setDefaultUri(conf, namenode);
    conf.set("mapred.job.tracker", jobTrackerName);
    
    FileWriter fileWriter = new FileWriter(CONFIG_FILE);
    fileWriter.write("<?xml version=\"1.0\"?>\n");
    String str = "<configuration> " +
                   "<srcPath prefix=\"/user/dhruba/raidtest\"> " +
                     "<policy name = \"RaidTest1\"> " +
                        "<erasureCode>xor</erasureCode> " +
                        "<destPath> /destraid</destPath> " +
                        "<property> " +
                          "<name>targetReplication</name> " +
                          "<value>1</value> " + 
                          "<description>after RAIDing, decrease the replication factor of a file to this value." +
                          "</description> " + 
                        "</property> " +
                        "<property> " +
                          "<name>metaReplication</name> " +
                          "<value>1</value> " + 
                          "<description> replication factor of parity file" +
                          "</description> " + 
                        "</property> " +
                        "<property> " +
                          "<name>modTimePeriod</name> " +
                          "<value>2000</value> " + 
                          "<description> time (milliseconds) after a file is modified to make it " +
                                         "a candidate for RAIDing " +
                          "</description> " + 
                        "</property> ";
    if (timeBeforeHar >= 0) {
      str +=
                        "<property> " +
                          "<name>time_before_har</name> " +
                          "<value>" + timeBeforeHar + "</value> " +
                          "<description> amount of time waited before har'ing parity files" +
                          "</description> " + 
                        "</property> ";
    }

    str +=
                     "</policy>" +
                   "</srcPath>" +
                 "</configuration>";
    fileWriter.write(str);
    fileWriter.close();
  }

  private void myTearDown() throws Exception {
    if (cnode != null) { cnode.stop(); cnode.join(); }
    if (mr != null) { mr.shutdown(); }
    if (dfs != null) { dfs.shutdown(); }
  }

  private LocatedBlocks getBlockLocations(Path file, long length)
    throws IOException {
    DistributedFileSystem dfs = (DistributedFileSystem) fileSys;
    return dfs.getClient().namenode.getBlockLocations(file.toString(),
                                                      0, length);
  }

  private long getCRC(FileSystem fs, Path p) throws IOException {
    CRC32 crc = new CRC32();
    FSDataInputStream stm = fs.open(p);
    for (int b = 0; b > 0; b = stm.read()) {
      crc.update(b);
    }
    stm.close();
    return crc.getValue();
  }

  static boolean corruptReplica(String blockName, int replica)
    throws IOException {
    Random random = new Random();
    String testDir = System.getProperty("test.build.data", "build/test/data") +
      "/dfs/";
    File baseDir = new File(testDir, "data");
    boolean corrupted = false;
    for (int i=replica*2; i<replica*2+2; i++) {
      File blockFile = new File(baseDir, "data" + (i+1) + 
                                "/current/" + blockName);
      if (blockFile.exists()) {
        // Corrupt replica by writing random bytes into replica
        RandomAccessFile raFile = new RandomAccessFile(blockFile, "rw");
        FileChannel channel = raFile.getChannel();
        String badString = "BADBAD";
        int rand = random.nextInt((int)channel.size()/2);
        raFile.seek(rand);
        raFile.write(badString.getBytes());
        raFile.close();
        corrupted = true;
      }
    }
    return corrupted;
  }

  static void corruptBlock(String blockName) throws IOException {
    boolean corrupted = false;
    for (int i = 0; i < NUM_DATANODES; i++) {
      corrupted |= corruptReplica(blockName, i);
    }
    assertTrue("could not corrupt block", corrupted);
  }
  
  static void reportCorruptBlocks(FileSystem fs, Path file, int[] idxs,
    long blockSize) throws IOException {

    FSDataInputStream in = fs.open(file);
    try {
      for (int idx: idxs) {
        long offset = idx * blockSize;
        LOG.info("Reporting corrupt block " + file + ":" + offset);
        in.seek(offset);
        try {
          in.readFully(new byte[(int)blockSize]);
          fail("Expected exception not thrown for " + file + ":" + offset);
        } catch (org.apache.hadoop.fs.ChecksumException e) {
        } catch (org.apache.hadoop.fs.BlockMissingException bme) {
        }
      }
    } finally {
      in.close();
    }
  }
}

