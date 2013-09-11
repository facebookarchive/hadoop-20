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

import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.RaidDFSUtil;
import org.apache.hadoop.hdfs.TestRaidDfs;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.raid.DistBlockIntegrityMonitor.CorruptFileStatus;
import org.apache.hadoop.util.StringUtils;
import org.junit.Test;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Random;
import junit.framework.TestCase;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.TestDatanodeBlockScanner;
import org.apache.hadoop.mapred.MiniMRCluster;

public class TestFileCorruptions  extends TestCase {
  final static Log LOG = LogFactory.getLog(
      "org.apache.hadoop.raid.TestFileCorruptions");
  final static String TEST_DIR = new File(System.getProperty("test.build.data",
      "build/contrib/raid/test/data")).getAbsolutePath();
  final static String CONFIG_FILE = new File(TEST_DIR, "test-raid.xml").getAbsolutePath();
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
  
  /**
   * tests that the distributed block fixer obeys
   * the limit on how many jobs to submit simultaneously.
   */
  @Test
  public void testCorruptFileCounter() throws Exception {
    
    LOG.info("Test testCorruptFileCounter started.");
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
    LOG.info("Test testCorruptFileCounter created test files");

    // create an instance of the RaidNode
    Configuration localConf = new Configuration(conf);
    localConf.setInt("raid.blockfix.interval", 1000);
    localConf.set("raid.blockfix.classname", 
                  "org.apache.hadoop.raid.DistBlockIntegrityMonitor");
    localConf.setLong("raid.blockfix.filespertask", 2L);
    localConf.setLong("raid.blockfix.maxpendingjobs", 1L);
    localConf.set("raid.corruptfile.counter.dirs", "/user/dhruba/raidtest,/user/dhruba1");
    localConf.setInt("raid.corruptfilecount.interval", 1000);
    localConf.set("mapred.raid.http.address", "localhost:0");
    localConf.setInt(
        DistBlockIntegrityMonitor.RAIDNODE_MAX_NUM_DETECTION_TIME_COLLECTED_KEY,
        1);

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
      int[] corruptBlockIdxs = new int[]{0, 1, 2, 3, 4, 6};
      for (int idx: corruptBlockIdxs)
        corruptBlock(file1Loc.get(idx).getBlock(), dfsCluster);
      RaidDFSUtil.reportCorruptBlocks(dfs, file1, corruptBlockIdxs, blockSize);
      cnode = RaidNode.createRaidNode(null, localConf);
      long startTime = System.currentTimeMillis();
      Map<String, Map<CorruptFileStatus, Long>> result = null;
      while (System.currentTimeMillis() - startTime < 120000) { 
        result = cnode.getCorruptFilesCounterMap();
        Long counter = result.get("/user/dhruba/raidtest").get(
            CorruptFileStatus.RAID_UNRECOVERABLE);
        if (counter != null && counter > 0) {
          break;
        }
        LOG.info("Waiting for 1 corrupt file");
        Thread.sleep(1000);
      }
      assertEquals("We expect 1 corrupt files", result.get(
          "/user/dhruba/raidtest").get(CorruptFileStatus.RAID_UNRECOVERABLE)
          , new Long(1L));
      assertEquals("We expect 0 corrupt files", result.get(
          "/user/dhruba1").get(CorruptFileStatus.RAID_UNRECOVERABLE), 
          new Long(0L));
      // corrupt file2
      for (int idx: corruptBlockIdxs)
        corruptBlock(file2Loc.get(idx).getBlock(), dfsCluster);
      RaidDFSUtil.reportCorruptBlocks(dfs, file2, corruptBlockIdxs, blockSize);
      startTime = System.currentTimeMillis();
      while (System.currentTimeMillis() - startTime < 120000) { 
        result = cnode.getCorruptFilesCounterMap();
        Long counter = result.get("/user/dhruba/raidtest").get(
            CorruptFileStatus.RAID_UNRECOVERABLE);
        if (counter != null && counter > 1) {
          break;
        }
        LOG.info("Waiting for 2 corrupt files");
        Thread.sleep(1000);
      }
      LOG.info("Handle " + cnode.getNumDetectionsPerSec() + " files per second");
      assertTrue(cnode.getNumDetectionsPerSec() > 0);
      assertEquals("We expect 2 corrupt files", result.get(
          "/user/dhruba/raidtest").get(CorruptFileStatus.RAID_UNRECOVERABLE),
          new Long(2L));
      assertEquals("We expect 0 corrupt files", result.get(
          "/user/dhruba1").get(CorruptFileStatus.RAID_UNRECOVERABLE), 
          new Long(0L));
    } catch (Exception e) {
      LOG.info("Test testCorruptFileCounter exception " + e.getMessage(),
               e);
      throw e;
    } finally {
      myTearDown();
    }

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
    conf.set("mapred.raid.http.address", "localhost:0");
    conf.setInt(BlockIntegrityMonitor.BLOCKCHECK_INTERVAL, 3000);
    
    conf.setBoolean("dfs.permissions", false);
    Utils.loadTestCodecs(conf, 5, 1, 3, "/destraid", "/destraidrs");

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
  }
  
  static void corruptBlock(Block block, MiniDFSCluster dfs) throws IOException {
    boolean corrupted = false;
    for (int i = 0; i < NUM_DATANODES; i++) {
      corrupted |= TestDatanodeBlockScanner.corruptReplica(block, i, dfs);
    }
    assertTrue("could not corrupt block", corrupted);
  }
}
