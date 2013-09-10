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
import java.util.Map;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileChecksum;
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
import org.apache.hadoop.raid.LogUtils.LOGRESULTS;
import org.apache.hadoop.raid.LogUtils.LOGTYPES;
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
  
  protected void mySetup(int stripeLength, int timeBeforeHar, 
      String xorCode, String rsCode, String code, boolean hasSimulation)
          throws Exception {
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
    conf.set("mapred.raid.http.address", "localhost:0");
    conf.setInt(RaidNode.RAID_PARITY_INITIAL_REPL_KEY, 1);

    // do not use map-reduce cluster for Raiding
    conf.set("raid.classname", "org.apache.hadoop.raid.LocalRaidNode");
    conf.set("raid.server.address", "localhost:" + MiniDFSCluster.getFreePort());
    conf.setLong("raid.blockfix.maxpendingjobs", 1L);
    conf.setLong(BlockIntegrityMonitor.BLOCKCHECK_INTERVAL, 1000L);
    conf.setLong(DistBlockIntegrityMonitor.RAIDNODE_BLOCK_FIX_SUBMISSION_INTERVAL_KEY,
        15000L);
    conf.setLong(DistBlockIntegrityMonitor.RAIDNODE_BLOCK_FIX_SCAN_SUBMISSION_INTERVAL_KEY,
        3600000);
    Utils.loadTestCodecs(conf, stripeLength, stripeLength, 1, 3, "/destraid",
        "/destraidrs", hasSimulation, xorCode, rsCode,
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
    cb.addPolicy("RaidTest1", "/user/dhruba/raidtest", 1, 1, code);
    cb.persist();
  }

  protected void myTearDown() throws Exception {
    if (cnode != null) { cnode.stop(); cnode.join(); }
    if (mr != null) { mr.shutdown(); }
    if (dfsCluster != null) { dfsCluster.shutdown(); }
  }
  
  public void testBadBlockFixerWithoutChecksums() throws Exception {
    implSimulationBlockFixer(false, "xor", true, false, true);
    implSimulationBlockFixer(false, "rs", true, false, true);
  }
  
  public void testBadBlockFixerWithoutSimulation() throws Exception {
    implSimulationBlockFixer(false, "xor", true, true, false);
    implSimulationBlockFixer(false, "rs", true, true, false);
  }
  
  public void testGoodBlockFixer() throws Exception {
    implSimulationBlockFixer(true, "xor", true);
    implSimulationBlockFixer(true, "rs", true);
  }
  
  public void testBadBlockFixer() throws Exception {
    implSimulationBlockFixer(false, "xor", true);
    implSimulationBlockFixer(false, "rs", true);
  }
  
  public void implSimulationBlockFixer(boolean isGoodFixer, String code, 
      boolean fixSource) throws Exception {
    implSimulationBlockFixer(isGoodFixer, code, fixSource, true, true);
  }
  
  public void implSimulationBlockFixer(boolean isGoodFixer, String code, 
      boolean fixSource, boolean hasChecksum, boolean hasSimulation)
          throws Exception {
    String testMessage = "TestSimulationBlockFixer started:" + 
          (isGoodFixer?"Good":"Bad") + " fixer," + 
          " Code "+ code + " " + (fixSource?"source":"parity") + " " +
          (hasChecksum?"has-checksum":"") + " " +
          (hasSimulation?"hasSimulation":"");
    LOG.info("Test started :" + testMessage);
    long blockSize = 8192L;
    int stripeLength = 3;
    if (isGoodFixer) {
      mySetup(stripeLength, -1, "org.apache.hadoop.raid.XORCode",
          "org.apache.hadoop.raid.ReedSolomonCode", code, hasSimulation); 
    } else {
      mySetup(stripeLength, -1,
          "org.apache.hadoop.raid.BadXORCode",
          "org.apache.hadoop.raid.BadReedSolomonCode", code, hasSimulation); 
    }
    Path file1 = new Path("/user/dhruba/raidtest/file1");
    Path destPath;
    if (code.equals("xor")) {
      destPath = new Path("/destraid/user/dhruba/raidtest");
    } else {
      // equals ("rs")
      destPath = new Path("/destraidrs/user/dhruba/raidtest");
    }
    long crc1 = TestRaidDfs.createTestFilePartialLastBlock(fileSys, file1,
                                                          1, 7, blockSize);
    LOG.info("Test testBlockFix created test files");
    FileStatus statFile1 = fileSys.getFileStatus(file1);

    // create an instance of the RaidNode
    Configuration localConf = new Configuration(conf);
    localConf.set("raid.blockfix.classname",
                  "org.apache.hadoop.raid.DistBlockIntegrityMonitor");
    localConf.setLong("raid.blockfix.filespertask", 2L);
    // Add checksum store
    TestBlockFixer.setChecksumStoreConfig(localConf);

    Codec codec = Codec.getCodec(code);
    
    try {
      cnode = RaidNode.createRaidNode(null, localConf);
      TestRaidDfs.waitForFileRaided(LOG, fileSys, file1, destPath);
      cnode.stop(); cnode.join();
      
      if (!hasChecksum) {
        // Clear checksums 
        LocalChecksumStore lcs = new LocalChecksumStore();
        lcs.initialize(localConf, false);
        lcs.clear();
      }
      
      ParityFilePair pfPair = ParityFilePair.getParityFile(codec, statFile1, 
          localConf);
      assertNotNull(pfPair);
      Path parity = pfPair.getPath();
      
      FileStatus stat;
      DistributedFileSystem dfs = (DistributedFileSystem)fileSys;
      
      String[] corruptFiles = DFSUtil.getCorruptFiles(dfs);
      assertEquals("no corrupt files expected", 0, corruptFiles.length);
      assertEquals("filesFixed() should return 0 before fixing files",
                   0, cnode.blockIntegrityMonitor.getNumFilesFixed());
      
      Path corruptFile;
      int [] corruptBlockIdxs;
      if (fixSource) {
        stat = fileSys.getFileStatus(file1);
        LocatedBlocks locs = RaidDFSUtil.getBlockLocations(
            dfs, file1.toUri().getPath(), 0, stat.getLen());
        // Corrupt blocks in two different stripes. We can fix them.
        corruptBlockIdxs = new int[]{1, 4, 6};
        for (int idx: corruptBlockIdxs) {
          TestBlockFixer.corruptBlock(locs.get(idx).getBlock(), dfsCluster);
        }
        RaidDFSUtil.reportCorruptBlocks(dfs, file1, corruptBlockIdxs, blockSize);
        corruptFile = file1;
      } else {
        crc1 = RaidDFSUtil.getCRC(fileSys, parity);
        stat = fileSys.getFileStatus(parity);
        LocatedBlocks locs = RaidDFSUtil.getBlockLocations(
            dfs, parity.toUri().getPath(), 0, stat.getLen());
        corruptBlockIdxs = new int[] {0, 1, 2};
        for (int idx : corruptBlockIdxs) {
          TestBlockFixer.corruptBlock(locs.get(idx).getBlock(), dfsCluster);
        }
        RaidDFSUtil.reportCorruptBlocks(dfs, parity, corruptBlockIdxs, blockSize);
        corruptFile = parity;
      }
      
      corruptFiles = DFSUtil.getCorruptFiles(dfs);
      assertEquals("file not corrupted", 1, corruptFiles.length);
      assertEquals("wrong file corrupted",
                   corruptFiles[0], corruptFile.toUri().getPath());
      assertEquals("wrong number of corrupt blocks", 3,
        RaidDFSUtil.corruptBlocksInFile(dfs, corruptFile.toUri().getPath(), 0,
          stat.getLen()).size());

      cnode = RaidNode.createRaidNode(null, localConf);
      long start = System.currentTimeMillis();
      while (cnode.blockIntegrityMonitor.getNumFilesFixed() < 1 &&
             cnode.blockIntegrityMonitor.getNumFileFixFailures() < 1 &&
             System.currentTimeMillis() - start < 120000) {
        LOG.info("Test TestSimulationBlockFixer waiting for files to be fixed.");
        Thread.sleep(1000);
      }
        
      if (hasSimulation || isGoodFixer) {
        assertEquals("file not fixed", 1,
            cnode.blockIntegrityMonitor.getNumFilesFixed());
        TestBlockFixer.verifyMetrics(fileSys, cnode,
            LOGTYPES.OFFLINE_RECONSTRUCTION_BLOCK,
            LOGRESULTS.SUCCESS, 3L, true);
        boolean fixed = TestRaidDfs.validateFile(dfs, corruptFile, stat.getLen(), crc1);
        assertTrue("file not fixed: " + corruptFile.toString(), fixed);
        // Verify the counters are right
        long expectedNumFailures = isGoodFixer? 0: corruptBlockIdxs.length;
        assertEquals(expectedNumFailures,
            cnode.blockIntegrityMonitor.getNumBlockFixSimulationFailures());
        assertEquals(3 - expectedNumFailures,
            cnode.blockIntegrityMonitor.getNumBlockFixSimulationSuccess());
        if (!hasChecksum) {
          TestBlockFixer.verifyMetrics(fileSys, cnode,
              LOGTYPES.OFFLINE_RECONSTRUCTION_GET_CHECKSUM,
              LOGRESULTS.FAILURE, expectedNumFailures, true);
        }
        TestBlockFixer.verifyMetrics(fileSys, cnode,
            LOGTYPES.OFFLINE_RECONSTRUCTION_CHECKSUM_VERIFICATION,
            LOGRESULTS.FAILURE, 0L, false);
        TestBlockFixer.verifyMetrics(fileSys, cnode,
            LOGTYPES.OFFLINE_RECONSTRUCTION_SIMULATION,
            LOGRESULTS.FAILURE, expectedNumFailures, true);
        
        long expectedNumFailedJobs = isGoodFixer? 0: 1;
        assertEquals("Number of simulated failed jobs should be " +
            String.valueOf(expectedNumFailedJobs) +
            " file: " + corruptFile, expectedNumFailedJobs, 
            ((DistBlockIntegrityMonitor.Worker)cnode.
                blockIntegrityMonitor.getCorruptionMonitor()).simFailJobIndex.
                size());
      } else {
        assertEquals(0L, cnode.blockIntegrityMonitor.
            getNumBlockFixSimulationFailures());
        assertEquals(0L, cnode.blockIntegrityMonitor.
            getNumBlockFixSimulationSuccess());
        TestBlockFixer.verifyMetrics(fileSys, cnode,
            LOGTYPES.OFFLINE_RECONSTRUCTION_SIMULATION,
            LOGRESULTS.FAILURE, 0L, false);
        assertEquals(1L, cnode.blockIntegrityMonitor.getNumFileFixFailures());
        if (hasChecksum) {
          // One mismatch checksum will cancel the whole file reconstruction
          TestBlockFixer.verifyMetrics(fileSys, cnode,
              LOGTYPES.OFFLINE_RECONSTRUCTION_CHECKSUM_VERIFICATION,
              LOGRESULTS.FAILURE, 1L, true);
        } else {
          // all checksums are lost
          TestBlockFixer.verifyMetrics(fileSys, cnode,
              LOGTYPES.OFFLINE_RECONSTRUCTION_GET_CHECKSUM,
              LOGRESULTS.FAILURE, 3L, true);
        }
      }
      LOG.info(
      ((DistBlockIntegrityMonitor.Worker)cnode.
          blockIntegrityMonitor.getCorruptionMonitor()).getStatus().toHtml(500));
    } catch (Exception e) {
      LOG.info("Test TestSimulationBlockFixer Exception " + e, e);
      throw e;
    } finally {
      myTearDown();
    }
    LOG.info("Test completed: " + testMessage);
  }

}
