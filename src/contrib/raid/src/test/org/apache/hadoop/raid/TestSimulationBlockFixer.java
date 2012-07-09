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
import java.util.Random;

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
import org.apache.hadoop.hdfs.TestRaidDfs;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.mapred.MiniMRCluster;
import org.apache.hadoop.util.StringUtils;


import junit.framework.TestCase;

public class TestSimulationBlockFixer  extends TestCase {
  final static Log LOG = LogFactory.getLog(
      "org.apache.hadoop.raid.TestSimulationBlockFixer");
  final static String TEST_DIR = new File(System.getProperty("test.build.data",
      "build/contrib/raid/test/data")).getAbsolutePath();
  final static String CONFIG_FILE = new File(TEST_DIR, 
      "test-raid.xml").getAbsolutePath();
  final static long RELOAD_INTERVAL = 1000;
  final static int NUM_DATANODES = 3;
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
  
  private void mySetup(int stripeLength, int timeBeforeHar, 
      String xorCode) throws Exception {

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

    Utils.loadTestCodecs(conf, stripeLength, stripeLength, 1, 3, "/destraid",
        "/destraidrs", true, xorCode, "org.apache.hadoop.raid.ReedSolomonCode",
        false);

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
    conf.set("mapred.job.tracker", jobTrackerName);
    
    ConfigBuilder cb = new ConfigBuilder(CONFIG_FILE);
    cb.addPolicy("RaidTest1", "/user/dhruba/raidtest", 1, 1);
    cb.persist();
  }

  private void myTearDown() throws Exception {
    if (cnode != null) { cnode.stop(); cnode.join(); }
    if (mr != null) { mr.shutdown(); }
    if (dfsCluster != null) { dfsCluster.shutdown(); }
  }
  
  public void testGoodBlockFixer() throws Exception {
    implSimulationBlockFixer(true);
  }
  
  public void testBadBlockFixer() throws Exception {
    implSimulationBlockFixer(false);
  }
  
  public void implSimulationBlockFixer(boolean isGoodFixer) throws Exception {
    LOG.info("Test TestSimulationBlockFixer started.");
    long blockSize = 8192L;
    int stripeLength = 3;
    if (isGoodFixer) {
      mySetup(stripeLength, -1, "org.apache.hadoop.raid.XORCode"); 
    } else {
      mySetup(stripeLength, -1,
          "org.apache.hadoop.raid.BadXORCode"); 
    }
    Path file1 = new Path("/user/dhruba/raidtest/file1");
    Path destPath = new Path("/destraid/user/dhruba/raidtest");
    long crc1 = TestRaidDfs.createTestFilePartialLastBlock(fileSys, file1,
                                                          1, 7, blockSize);
    long file1Len = fileSys.getFileStatus(file1).getLen();
    LOG.info("Test testBlockFix created test files");

    // create an instance of the RaidNode
    Configuration localConf = new Configuration(conf);
    localConf.setInt("raid.blockfix.interval", 1000);
    localConf.set("raid.blockfix.classname",
                  "org.apache.hadoop.raid.DistBlockIntegrityMonitor");
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
      int[] corruptBlockIdxs = new int[]{1, 4, 6};
      for (int idx: corruptBlockIdxs)
        TestBlockFixer.corruptBlock(locs.get(idx).getBlock().getBlockName(), dfsCluster);
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
      while (cnode.blockIntegrityMonitor.getNumFilesFixed() < 1 &&
             System.currentTimeMillis() - start < 120000) {
        LOG.info("Test TestSimulationBlockFixer waiting for files to be fixed.");
        Thread.sleep(1000);
      }
      assertEquals("file not fixed", 1, cnode.blockIntegrityMonitor.getNumFilesFixed());
      
      dfs = TestBlockFixer.getDFS(conf, dfs);
      assertTrue("file not fixed",
                 TestRaidDfs.validateFile(dfs, file1, file1Len, crc1));
      // Verify the counters are right
      long expectedNumFailures = isGoodFixer? 0: corruptBlockIdxs.length;
      assertEquals(expectedNumFailures,
          cnode.blockIntegrityMonitor.getNumBlockFixSimulationFailures());
      assertEquals(3 - expectedNumFailures,
          cnode.blockIntegrityMonitor.getNumBlockFixSimulationSuccess());
      
      long expectedNumFailedJobs = isGoodFixer? 0: 1;
      assertEquals("Number of simulated failed jobs should be " +
          expectedNumFailedJobs, expectedNumFailedJobs, 
          ((DistBlockIntegrityMonitor.Worker)cnode.
              blockIntegrityMonitor.getCorruptionMonitor()).simFailJobIndex.
              size());
    } catch (Exception e) {
      LOG.info("Test TestSimulationBlockFixer Exception " + e, e);
      throw e;
    } finally {
      myTearDown();
    }
    LOG.info("Test TestSimulationBlockFixer completed.");
  }

}
