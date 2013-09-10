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
import java.lang.management.ManagementFactory;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import junit.framework.TestCase;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.RaidDFSUtil;
import org.apache.hadoop.hdfs.TestDatanodeBlockScanner;
import org.apache.hadoop.hdfs.TestRaidDfs;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.util.InjectionEvent;
import org.apache.hadoop.mapred.MiniMRCluster;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.metrics.util.MetricsTimeVaryingLong;
import org.apache.hadoop.raid.DistBlockIntegrityMonitor.Priority;
import org.apache.hadoop.raid.DistBlockIntegrityMonitor.Worker.LostFileInfo;
import org.apache.hadoop.raid.LogUtils.LOGRESULTS;
import org.apache.hadoop.raid.LogUtils.LOGTYPES;
import org.apache.hadoop.raid.RaidHistogram.Point;
import org.apache.hadoop.util.InjectionEventI;
import org.apache.hadoop.util.InjectionHandler;
import org.apache.hadoop.util.StringUtils;
import org.junit.Test;


public class TestBlockFixer extends TestCase {
  final static Log LOG = LogFactory.getLog(
                            "org.apache.hadoop.raid.TestBlockFixer");
  final static String TEST_DIR = new File(System.getProperty("test.build.data",
      "build/contrib/raid/test/data")).getAbsolutePath();
  final static String CHECKSUM_STORE_DIR = new File(TEST_DIR,
      "ckm_store." + System.currentTimeMillis()).getAbsolutePath();
  final static String CONFIG_FILE = new File(TEST_DIR, 
      "test-raid.xml").getAbsolutePath();
  final static long RELOAD_INTERVAL = 1000;
  final static int NUM_DATANODES = 4;
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
  
  class TestSendRecoveryTimeInjectionHandler extends InjectionHandler {
    @Override
    public void _processEvent(InjectionEventI event, Object... args) {
      if (event == InjectionEvent.RAID_SEND_RECOVERY_TIME) {
        if (cnode == null) {
          return;
        }
        RaidHistogram histogram = (RaidHistogram)args[0];
        String p = (String)args[1];
        Long value = (Long)args[2];
        ArrayList<Point> points =
            histogram.getPointsWithGivenRecoveryTime(value);
        boolean match = false;
        for (Point pt: points) {
          if (pt.path.equals(p)) {
            match = true;
            assertEquals(value, (Long)pt.value);
            String trackingUrl = CorruptFileCounterServlet.getTrackingUrl(
                (String)args[3], cnode);
            assertTrue("Should get a tracking url", trackingUrl.length() > 0);
            break;
          }
        }
        assertTrue("We should find path " + p + " in the histogram", match);
      }
    }
  }
  
  public static void verifyMetrics(FileSystem fileSys, 
      RaidNode cnode, boolean local, long expectedFixedFiles,
      long expectedFixBlocks) {
    assertTrue("Fewer expected fixed files", 
        cnode.blockIntegrityMonitor.getNumFilesFixed() >= expectedFixedFiles);
    if (!local) {
      verifyMetrics(fileSys, cnode, LOGTYPES.OFFLINE_RECONSTRUCTION_BLOCK,
          LOGRESULTS.SUCCESS, null, expectedFixBlocks, true);
    }
  }
  
  public static void verifyMetrics(FileSystem fileSys, 
      RaidNode cnode, LOGTYPES type, LOGRESULTS result, long expected, 
      boolean greater) {
    verifyMetrics(fileSys, cnode, type, result, null, expected, greater);
  }
  
  public static void verifyMetrics(FileSystem fileSys, 
      RaidNode cnode, LOGTYPES type, LOGRESULTS result, String tag, long expected, 
      boolean greater) {
    String counterName = LogUtils.getCounterName(fileSys, type, result, tag);
    Map<String, MetricsTimeVaryingLong> logMetrics = RaidNodeMetrics.getInstance(
        RaidNodeMetrics.DEFAULT_NAMESPACE_ID).logMetrics;
    String message = "expect " + expected + (greater? " >= ": " = ") + counterName;
    long actual = 0L;
    synchronized(logMetrics) {
      if (expected == 0L) {
        if (greater == false) {
          assertTrue(message, !logMetrics.containsKey(counterName));
        } else {
          actual = logMetrics.containsKey(counterName)?logMetrics.get(counterName).getCurrentIntervalValue():
                0;
          assertTrue(message + " but " + actual, actual >= 0L);
        }
      } else {
        actual = logMetrics.get(counterName).getCurrentIntervalValue();
        if (greater == false) {
          assertEquals(message + " but " + actual, new Long(expected),
              new Long(actual));
        } else {
          assertTrue(message + " but " + actual, actual >= expected);
        }
      }
    }
  }
  
  public static void setChecksumStoreConfig(Configuration conf) {
    conf.set(RaidNode.RAID_CHECKSUM_STORE_CLASS_KEY,
        "org.apache.hadoop.raid.LocalChecksumStore");
    conf.setBoolean(RaidNode.RAID_CHECKSUM_STORE_REQUIRED_KEY, true);
    conf.set(LocalChecksumStore.LOCAL_CHECK_STORE_DIR_KEY, CHECKSUM_STORE_DIR);
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
  public void testFilterUnfixableFiles() throws Exception {
    conf = new Configuration();
    dfsCluster = new MiniDFSCluster(conf, NUM_DATANODES, true, null);
    dfsCluster.waitActive();
    FileSystem fs = dfsCluster.getFileSystem();

    Utils.loadTestCodecs(conf);
    try {
      Configuration testConf = fs.getConf();
      BlockIntegrityMonitor blockFixer =
          new LocalBlockIntegrityMonitor(testConf, false);

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
      dfsCluster.shutdown();
    }
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
    implBlockFix(false, false);
  }
  
  @Test
  public void testBlockFixLocal() throws Exception {
    implBlockFix(true, false);
  }
  
  private void verifyMXBean(RaidNode cnode) throws Exception {
    MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
    ObjectName mxbeanName = new ObjectName(
        "hadoop:service=RaidNode,name=RaidNodeState");
    Long timeSinceLastSuccessfulFix = 
        (Long) mbs.getAttribute(mxbeanName, "TimeSinceLastSuccessfulFix");
    assertNotNull(timeSinceLastSuccessfulFix);
    if (timeSinceLastSuccessfulFix == 0) {
      assertEquals("No files need to fix", 0,
          cnode.blockIntegrityMonitor.approximateNumRecoverableFiles);
    }
    LOG.info("timeSinceLastSuccessfulFix:" + timeSinceLastSuccessfulFix +
             "\t" + "approximateNumRecoverableFiles:" +
             cnode.blockIntegrityMonitor.approximateNumRecoverableFiles + 
             "\t" + "lastSuccessfulFixTime:" + 
             cnode.blockIntegrityMonitor.lastSuccessfulFixTime);
  }

  /**
   * Create a file with three stripes, corrupt a block each in two stripes,
   * and wait for the the file to be fixed.
   */
  public void implBlockFix(boolean local, boolean hasChecksumStore) throws Exception {
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
    localConf.setInt("raid.blockfix.interval", 1000);
    if (local) {
      localConf.set("raid.blockfix.classname",
                    "org.apache.hadoop.raid.LocalBlockIntegrityMonitor");
    } else {
      localConf.set("raid.blockfix.classname",
                    "org.apache.hadoop.raid.DistBlockIntegrityMonitor");
      InjectionHandler h = new TestSendRecoveryTimeInjectionHandler();
      InjectionHandler.set(h);
    }
    localConf.setLong("raid.blockfix.filespertask", 2L);
    if (hasChecksumStore) {
      TestBlockFixer.setChecksumStoreConfig(localConf);
    }
    
    try {
      cnode = RaidNode.createRaidNode(null, localConf);
      TestRaidDfs.waitForFileRaided(LOG, fileSys, file1, destPath);
      LOG.info("Startup raidnode");
      verifyMXBean(cnode);
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
        corruptBlock(locs.get(idx).getBlock(), dfsCluster);
      RaidDFSUtil.reportCorruptBlocks(dfs, file1, corruptBlockIdxs, blockSize);
      
      corruptFiles = DFSUtil.getCorruptFiles(dfs);
      assertEquals("file not corrupted", 1, corruptFiles.length);
      assertEquals("wrong file corrupted",
                   corruptFiles[0], file1.toUri().getPath());
      assertEquals("wrong number of corrupt blocks", 3,
        RaidDFSUtil.corruptBlocksInFile(dfs, file1.toUri().getPath(), 0,
          srcStat.getLen()).size());

      cnode = RaidNode.createRaidNode(null, localConf);
      long start = System.currentTimeMillis();
      while ((cnode.blockIntegrityMonitor.getNumFilesFixed() < 1 || 
              cnode.blockIntegrityMonitor.getNumberOfPoints("/") < 1) && 
             System.currentTimeMillis() - start < 120000) {
        LOG.info("Test testBlockFix waiting for files to be fixed.");
        verifyMXBean(cnode);
        Thread.sleep(1000);
      }
      LOG.info("Files should be fixed");
      verifyMXBean(cnode);
      assertTrue("Raidnode should record more than 1 point", 
          cnode.blockIntegrityMonitor.getNumberOfPoints("/") >= 1);
      verifyMetrics(fileSys, cnode, local, 1L, corruptBlockIdxs.length);
      
      dfs = getDFS(conf, dfs);
      assertTrue("file not fixed",
                 TestRaidDfs.validateFile(dfs, file1, file1Len, crc1));
      LOG.info("Finish checking");
      verifyMXBean(cnode);
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
  public void generatedBlockTestCommon(String testName, int blockToCorrupt,
                                        boolean local, boolean hasChecksumStore) throws Exception {
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
    localConf.setInt("raid.blockfix.interval", 1000);
    if (local) {
      localConf.set("raid.blockfix.classname",
                    "org.apache.hadoop.raid.LocalBlockIntegrityMonitor");
    } else {
      localConf.set("raid.blockfix.classname",
                    "org.apache.hadoop.raid.DistBlockIntegrityMonitor");
    }
    if (hasChecksumStore) {
      TestBlockFixer.setChecksumStoreConfig(localConf);
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
      
      corruptBlock(locs.get(0).getBlock(), dfsCluster);
      RaidDFSUtil.reportCorruptBlocks(dfs, file1, new int[]{0}, blockSize);
      
      corruptFiles = DFSUtil.getCorruptFiles(dfs);
      assertEquals("file not corrupted",
                   1, corruptFiles.length);
      assertEquals("wrong file corrupted",
                   corruptFiles[0], file1.toUri().getPath());
      
      cnode = RaidNode.createRaidNode(null, localConf);
      long start = System.currentTimeMillis();
      while ((cnode.blockIntegrityMonitor.getNumFilesFixed() < 1 ||
              cnode.blockIntegrityMonitor.getNumberOfPoints("/") < 1) &&
             System.currentTimeMillis() - start < 120000) {
        LOG.info("Test " + testName + " waiting for files to be fixed.");
        Thread.sleep(1000);
      }
      verifyMetrics(fileSys, cnode, local, 1L, 1L);
      assertTrue("Raidnode should record more than 1 point",
          cnode.blockIntegrityMonitor.getNumberOfPoints("/") >= 1);
      
      // Stop RaidNode
      cnode.stop(); cnode.join(); cnode = null;

      // The block has successfully been reconstructed.
      dfs = getDFS(conf, dfs);
      assertTrue("file not fixed",
                 TestRaidDfs.validateFile(dfs, file1, file1Len, crc1));

      // Now corrupt the generated block.
      locs = RaidDFSUtil.getBlockLocations(
        dfs, file1.toUri().getPath(), 0, srcStat.getLen());
      corruptBlock(locs.get(0).getBlock(), dfsCluster);
      RaidDFSUtil.reportCorruptBlocks(dfs, file1, new int[]{0}, blockSize);

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
    generatedBlockTestCommon("testGeneratedBlock", 3, false, false);
  }

  /**
   * Tests integrity of generated block.
   * Create a file and delete a block entirely. Wait for the block to be
   * regenerated. Now stop RaidNode and corrupt the generated block.
   * Test that corruption in the generated block can be detected by clients.
   */
  @Test
  public void testGeneratedBlockLocal() throws Exception {
    generatedBlockTestCommon("testGeneratedBlock", 3, true, false);
  }

  /**
   * Tests integrity of generated last block.
   * Create a file and delete a block entirely. Wait for the block to be
   * regenerated. Now stop RaidNode and corrupt the generated block.
   * Test that corruption in the generated block can be detected by clients.
   */
  @Test
  public void testGeneratedLastBlockDist() throws Exception {
    generatedBlockTestCommon("testGeneratedLastBlock", 6, false, false);
  }

  /**
   * Tests integrity of generated last block.
   * Create a file and delete a block entirely. Wait for the block to be
   * regenerated. Now stop RaidNode and corrupt the generated block.
   * Test that corruption in the generated block can be detected by clients.
   */
  @Test
  public void testGeneratedLastBlockLocal() throws Exception {
    generatedBlockTestCommon("testGeneratedLastBlock", 6, true, false);
  }
  
  @Test
  public void testParityBlockFixDist() throws Exception {
    implParityBlockFix("testParityBlockFixDist", false, false);
  }

  @Test
  public void testParityBlockFixLocal() throws Exception {
    implParityBlockFix("testParityBlockFixLocal", true, false);
  }

  /**
   * Corrupt a parity file and wait for it to get fixed.
   */
  public void implParityBlockFix(String testName, boolean local, boolean hasChecksumStore)
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
    LOG.info("Test " + testName + " created test files");

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
    if (hasChecksumStore) {
      TestBlockFixer.setChecksumStoreConfig(localConf);
    }
    localConf.setLong("raid.blockfix.filespertask", 2L);

    try {
      cnode = RaidNode.createRaidNode(null, localConf);
      TestRaidDfs.waitForFileRaided(LOG, fileSys, file1, destPath);
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
      RaidDFSUtil.reportCorruptBlocks(dfs, parityFile, corruptBlockIdxs, blockSize);

      corruptFiles = DFSUtil.getCorruptFiles(dfs);
      assertEquals("file not corrupted",
                   1, corruptFiles.length);
      assertEquals("wrong file corrupted",
                   corruptFiles[0], parityFile.toUri().getPath());
      if (!local) {
         assertFalse(dfs.exists(new Path("blockfixer")));
      }
      cnode = RaidNode.createRaidNode(null, localConf);
      long start = System.currentTimeMillis();
      while ((cnode.blockIntegrityMonitor.getNumFilesFixed() < 1 || 
              cnode.blockIntegrityMonitor.getNumberOfPoints("/") < 1) &&
             System.currentTimeMillis() - start < 120000) {
        LOG.info("Test " + testName + " waiting for files to be fixed.");
        Thread.sleep(1000);
      }
      assertTrue("Raidnode should record more than 1 point",
          cnode.blockIntegrityMonitor.getNumberOfPoints("/") >= 1);

      long checkCRC = RaidDFSUtil.getCRC(fileSys, parityFile);

      assertEquals("file not fixed",
                   parityCRC, checkCRC);
      cnode.stop(); cnode.join();
      if (!local) {
        assertTrue("blockfixer will create /user/username/blockfixer", 
            dfs.exists(new Path("blockfixer")));
      }
      cnode = RaidNode.createRaidNode(null, localConf);
      if (!local) {
        assertFalse("Restarting raidnode will cleanup job dir", 
            dfs.exists(new Path("blockfixer")));
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
    // Parity file will have 7 blocks.
    TestRaidDfs.createTestFilePartialLastBlock(fileSys, file1,
                                               1, 20, blockSize);
    LOG.info("Test " + testName + " created test files");

    // create an instance of the RaidNode
    Configuration localConf = new Configuration(conf);
    localConf.setInt("raid.blockfix.interval", 1000);
    localConf.setInt(RaidNode.RAID_PARITY_HAR_THRESHOLD_DAYS_KEY, 0);
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
      assertEquals(true, fileSys.exists(harDirectory));

      Path partFile = new Path(harDirectory, "part-0");
      long partCRC = RaidDFSUtil.getCRC(fileSys, partFile);
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
        corruptBlock(locs.get(idx).getBlock(), dfsCluster);
      RaidDFSUtil.reportCorruptBlocks(dfs, partFile, corruptBlockIdxs,
        partStat.getBlockSize());

      corruptFiles = DFSUtil.getCorruptFiles(dfs);
      assertEquals("file not corrupted", 1, corruptFiles.length);
      assertEquals("wrong file corrupted",
                   corruptFiles[0], partFile.toUri().getPath());

      cnode = RaidNode.createRaidNode(null, localConf);
      start = System.currentTimeMillis();
      while ((cnode.blockIntegrityMonitor.getNumFilesFixed() < 1 || 
              cnode.blockIntegrityMonitor.getNumberOfPoints("/") < 1) &&
             System.currentTimeMillis() - start < 120000) {
        LOG.info("Test " + testName + " waiting for files to be fixed.");
        Thread.sleep(1000);
      }
      assertTrue("Raidnode should record more than 1 point", 
          cnode.blockIntegrityMonitor.getNumberOfPoints("/") >= 1);

      long checkCRC = RaidDFSUtil.getCRC(fileSys, partFile);

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
    Path file1 = new Path("/user/dhruba/raidtest/concurrentjobfile1");
    Path file2 = new Path("/user/dhruba/raidtest/concurrentjobfile2");
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
    localConf.setInt("raid.blockfix.interval", 1000);
    localConf.set("raid.blockfix.classname", 
                  "org.apache.hadoop.raid.DistBlockIntegrityMonitor");
    localConf.setLong("raid.blockfix.filespertask", 2L);
    TestBlockFixer.setChecksumStoreConfig(localConf);
    
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
      for (int idx: corruptBlockIdxs) {
        corruptBlock(file1Loc.get(idx).getBlock(), dfsCluster);
        RaidDFSUtil.reportCorruptBlocksToNN(dfs, 
            new LocatedBlock[] {file1Loc.get(idx)});
      }
      RaidDFSUtil.reportCorruptBlocks(dfs, file1, corruptBlockIdxs, blockSize);
          
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
      for (int idx: corruptBlockIdxs) {
        corruptBlock(file2Loc.get(idx).getBlock(), dfsCluster);
        RaidDFSUtil.reportCorruptBlocksToNN(dfs, 
            new LocatedBlock[] {file2Loc.get(idx)});
      }
          
      corruptFiles = DFSUtil.getCorruptFiles(dfs);
      assertEquals("2 corrupt files expected", 2, corruptFiles.length);
      
      while (blockFixer.jobsRunning() < 2 &&
             System.currentTimeMillis() - start < 240000) {
        LOG.info("Test testBlockFix waiting for fixing job 2 to start");
        Thread.sleep(10);
      }
      assertTrue(blockFixer.jobsRunning() >= 2);
      
      while ((blockFixer.getNumFilesFixed() < 2 ||
              blockFixer.getNumberOfPoints("/") < 2) &&
             System.currentTimeMillis() - start < 240000) {
        LOG.info("Test testBlockFix waiting for files to be fixed.");
        Thread.sleep(10);
      }
      
      // wait for all the jobs to finish
      while (blockFixer.jobsRunning() > 0 &&
          System.currentTimeMillis() - start < 240000) {
        LOG.info("Test testBlockFix waiting for block fixer jobs to finish.");
        Thread.sleep(10);
      }
      dfs = getDFS(conf, dfs);
      
      try {
        Thread.sleep(5*1000);
      } catch (InterruptedException ignore) {
      }
      assertTrue("files not fixed", blockFixer.getNumFilesFixed() >= 2);
      assertTrue("fixed files not recorded",
          blockFixer.getNumberOfPoints("/") >= 2);
      verifyMetrics(fileSys, cnode, LOGTYPES.OFFLINE_RECONSTRUCTION_BLOCK,
          LOGRESULTS.SUCCESS, corruptBlockIdxs.length * 2, true);
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
    localConf.setInt("raid.blockfix.interval", 1000);
    localConf.set("raid.blockfix.classname", 
                  "org.apache.hadoop.raid.DistBlockIntegrityMonitor");
    localConf.setLong("raid.blockfix.filespertask", 2L);
    localConf.setLong("raid.blockfix.maxpendingjobs", 1L);
    TestBlockFixer.setChecksumStoreConfig(localConf);
    
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
        corruptBlock(file1Loc.get(idx).getBlock(), dfsCluster);
      RaidDFSUtil.reportCorruptBlocks(dfs, file1, corruptBlockIdxs, blockSize);
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
        corruptBlock(file2Loc.get(idx).getBlock(), dfsCluster);
      RaidDFSUtil.reportCorruptBlocks(dfs, file2, corruptBlockIdxs, blockSize);
      corruptFiles = DFSUtil.getCorruptFiles(dfs);
      
      // wait until both files are fixed
      while ((blockFixer.getNumFilesFixed() < 2 || 
              blockFixer.getNumberOfPoints("/") < 2) &&
             System.currentTimeMillis() - start < 240000) {
        // make sure the block fixer does not start a second job while
        // the first one is still running
        assertTrue("too many jobs running", blockFixer.jobsRunning() <= 1);
        Thread.sleep(10);
      }
      assertTrue("files not fixed", blockFixer.getNumFilesFixed() >= 2);
      assertTrue("files fixed not record",
          blockFixer.getNumberOfPoints("/") >= 2);
      verifyMetrics(fileSys, cnode, LOGTYPES.OFFLINE_RECONSTRUCTION_BLOCK,
          LOGRESULTS.SUCCESS, corruptBlockIdxs.length * 2, true);
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
    LOG.info("Test testMultiplePriorities started.");
    Path srcFile = new Path("/home/test/file1");
    int repl = 1;
    int numBlocks = 8;
    long blockSize = 16384;
    int stripeLength = 3;
    Path destPath = new Path("/destraidrs");
    mySetup(stripeLength, -1); // never har
    Codec codec = Codec.getCodec("rs");
    LOG.info("Starting testMultiplePriorities");
    try {
      // Create test file and raid it.
      TestRaidDfs.createTestFilePartialLastBlock(
        fileSys, srcFile, repl, numBlocks, blockSize);
      FileStatus stat = fileSys.getFileStatus(srcFile);
      RaidNode.doRaid(conf, stat,
        destPath, codec, new RaidNode.Statistics(), RaidUtils.NULL_PROGRESSABLE,
        false, repl, repl);

      // Corrupt first block of file.
      int blockIdxToCorrupt = 1;
      LOG.info("Corrupt block " + blockIdxToCorrupt + " of file " + srcFile);
      LocatedBlocks locations = getBlockLocations(srcFile, stat.getLen());
      corruptBlock(locations.get(blockIdxToCorrupt).getBlock(),
          dfsCluster);
      RaidDFSUtil.reportCorruptBlocks(fileSys, srcFile, new int[]{1}, blockSize);

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

      // Corrupt one more block.
      blockIdxToCorrupt = 4;
      LOG.info("Corrupt block " + blockIdxToCorrupt + " of file " + srcFile);
      locations = getBlockLocations(srcFile, stat.getLen());
      corruptBlock(locations.get(blockIdxToCorrupt).getBlock(),
          dfsCluster);
      RaidDFSUtil.reportCorruptBlocks(fileSys, srcFile, new int[]{4}, blockSize);

      // A new job should be submitted since two blocks are corrupt.
      startTime = System.currentTimeMillis();
      while (System.currentTimeMillis() - startTime < 120000 &&
             distBlockFixer.submittedJobs.size() == submittedJob) { 
        distBlockFixer.getCorruptionMonitor().checkAndReconstructBlocks();
        LOG.info("Waiting for more jobs to submit");
        Thread.sleep(10000);
      }
      LOG.info("Already Submitted " + distBlockFixer.submittedJobs.size()  + " jobs");
      assertTrue("Should submit more than 1 jobs",
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

  private void mySetup(int stripeLength, int timeBeforeHar) throws Exception {
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
    // Make sure initial repl is smaller than NUM_DATANODES
    conf.setInt(RaidNode.RAID_PARITY_INITIAL_REPL_KEY, 1);
    
    Utils.loadTestCodecs(conf, stripeLength, 1, 3, "/destraid", "/destraidrs");

    conf.setBoolean("dfs.permissions", false);

    dfsCluster = new MiniDFSCluster(conf, NUM_DATANODES, true, null);
    dfsCluster.waitActive();
    fileSys = dfsCluster.getFileSystem();
    namenode = fileSys.getUri().toString();

    FileSystem.setDefaultUri(conf, namenode);
    mr = new MiniMRCluster(4, namenode, 3);
    jobTrackerName = "localhost:" + mr.getJobTrackerPort();
    hftp = "hftp://localhost.localdomain:" + dfsCluster.getNameNodePort();

    FileSystem.setDefaultUri(conf, namenode);
    conf.set("mapred.job.tracker" + "." + DistBlockIntegrityMonitor.BLOCKFIXER, jobTrackerName);
    
    FileWriter fileWriter = new FileWriter(CONFIG_FILE);
    fileWriter.write("<?xml version=\"1.0\"?>\n");
    String str = "<configuration> " +
                     "<policy name = \"RaidTest1\"> " +
                        "<srcPath prefix=\"/user/dhruba/raidtest\"/> " +
                        "<codecId>xor</codecId> " +
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
                 "</configuration>";
    fileWriter.write(str);
    fileWriter.close();
  }

  private void myTearDown() throws Exception {
    if (cnode != null) { cnode.stop(); cnode.join(); }
    if (mr != null) { mr.shutdown(); }
    if (dfsCluster != null) { dfsCluster.shutdown(); }
    InjectionHandler.clear();
  }

  private LocatedBlocks getBlockLocations(Path file, long length)
    throws IOException {
    DistributedFileSystem dfs = (DistributedFileSystem) fileSys;
    return dfs.getClient().namenode.getBlockLocations(file.toString(),
                                                      0, length);
  }

  static void corruptBlock(Block block, MiniDFSCluster dfs) throws IOException {
    boolean corrupted = false;
    for (int i = 0; i < NUM_DATANODES; i++) {
      corrupted |= TestDatanodeBlockScanner.corruptReplica(block, i, dfs);
    }
    assertTrue("could not corrupt block", corrupted);
  }
}

