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
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
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
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.TestRaidDfs;
import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapred.MiniMRCluster;
import org.apache.hadoop.raid.protocol.PolicyInfo;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.hdfs.protocol.DataTransferProtocol;
import org.apache.hadoop.raid.DistRaid.Counter;
import org.apache.hadoop.raid.LogUtils.LOGRESULTS;
import org.apache.hadoop.raid.LogUtils.LOGTYPES;
import org.apache.hadoop.raid.PlacementMonitor.BlockInfo;
import org.apache.hadoop.raid.Utils.Builder;

/**
  * Test the generation of parity blocks for files with different block
  * sizes. Also test that a data block can be regenerated from a raid stripe
  * using the parity block
  */
public class TestRaidNode extends TestCase {
  final static String TEST_DIR = new File(System.getProperty("test.build.data",
      "build/contrib/raid/test/data")).getAbsolutePath();
  final static String CONFIG_FILE = new File(TEST_DIR, 
      "test-raid.xml").getAbsolutePath();
  final static long RELOAD_INTERVAL = 1000;
  final static Log LOG = LogFactory.getLog("org.apache.hadoop.raid.TestRaidNode");
  final static Random rand = new Random();

  Configuration conf;
  String namenode = null;
  String hftp = null;
  MiniDFSCluster dfs = null;
  MiniMRCluster mr = null;
  FileSystem fileSys = null;
  String jobTrackerName = null;
  
  static public void loadTestCodecs(Configuration conf, int xorStripeLength, int rsStripeLength, 
      int xorParityLength, int rsParityLength) 
    throws IOException {
    Utils.loadTestCodecs(conf, new Builder[] {
        // priority 100
        Utils.getXORBuilder().setStripeLength(xorStripeLength).setParityLength(
            xorParityLength),
        Utils.getXORBuilder().dirRaid(true).setParityDir(
            "/dir-raid").setStripeLength(xorStripeLength).setParityLength(
            xorParityLength).setCodeId("dir-xor").setPriority(400),
        // priority 300
        Utils.getRSBuilder().setStripeLength(rsStripeLength).setParityLength(
            rsParityLength), 
        Utils.getRSBuilder().dirRaid(true).setParityDir(
            "/dir-raidrs").setStripeLength(rsStripeLength).setParityLength(
            rsParityLength).setCodeId("dir-rs").setPriority(600)
    });
  }

  /**
   * create mapreduce and dfs clusters
   */
  private void createClusters(boolean local, boolean rackAware) throws Exception {
    if (System.getProperty("hadoop.log.dir") == null) {
      String base = new File(".").getAbsolutePath();
      System.setProperty("hadoop.log.dir", new Path(base).toString() + "/logs");
    }

    new File(TEST_DIR).mkdirs(); // Make sure data directory exists
    conf = new Configuration();
    conf.set("raid.config.file", CONFIG_FILE);
    conf.setBoolean("raid.config.reload", true);
    conf.setLong("raid.config.reload.interval", RELOAD_INTERVAL);
    conf.setLong(JobMonitor.JOBMONITOR_INTERVAL_KEY, 20000L);
    conf.setLong(RaidNode.TRIGGER_MONITOR_SLEEP_TIME_KEY, 3000L);
    conf.setLong(RaidNode.MINIMUM_RAIDABLE_FILESIZE_KEY, 1L);
    conf.setLong(RaidNode.RAID_MOD_TIME_PERIOD_KEY, 0);
    // Report block deletion quickly
    conf.setLong("dfs.blockreport.intervalMsec", 8000L);
    conf.setBoolean(StatisticsCollector.STATS_COLLECTOR_SUBMIT_JOBS_CONFIG, false);
    conf.set("mapred.raid.http.address", "localhost:0");
    //disable purge monitor
    conf.setLong(PurgeMonitor.PURGE_MONITOR_SLEEP_TIME_KEY, 3600000L);

    // scan every policy every 5 seconds
    conf.setLong("raid.policy.rescan.interval", 5 * 1000L);

    // the RaidNode does the raiding inline (instead of submitting to map/reduce)
    if (local) {
      conf.set("raid.classname", "org.apache.hadoop.raid.LocalRaidNode");
    } else {
      conf.set("raid.classname", "org.apache.hadoop.raid.DistRaidNode");
    }

    // use local block fixer
    conf.set("raid.blockfix.classname", 
             "org.apache.hadoop.raid.LocalBlockIntegrityMonitor");
    conf.set("dfs.block.replicator.classname",
        "org.apache.hadoop.hdfs.server.namenode.BlockPlacementPolicyRaid");

    conf.set("raid.server.address", "localhost:" + MiniDFSCluster.getFreePort());
    loadTestCodecs(conf, 3, 10, 1, 5);

    // create a dfs and map-reduce cluster
    final int taskTrackers = 4;
    final int jobTrackerPort = 60050;

    if (rackAware) {
      // Because BlockPlacementPolicyRaid only allows one replica in each rack,
      // spread 6 nodes into 6 racks to make sure chooseTarget function could pick
      // more than one node. 
      String[] racks = {"/rack1", "/rack2", "/rack3", "/rack4", "/rack5", "/rack6"};
      dfs = new MiniDFSCluster(conf, 6, true, racks);
    } else {
      dfs = new MiniDFSCluster(conf, 3, true, null);
    }
    dfs.waitActive();
    fileSys = dfs.getFileSystem();
    TestDirectoryRaidDfs.setupStripeStore(conf, fileSys);
    namenode = fileSys.getUri().toString();
    mr = new MiniMRCluster(taskTrackers, namenode, 3);
    jobTrackerName = "localhost:" + mr.getJobTrackerPort();
    hftp = "hftp://localhost.localdomain:" + dfs.getNameNodePort();

    FileSystem.setDefaultUri(conf, namenode);
    conf.set("mapred.job.tracker", jobTrackerName);
  }

    
  /**
   * stop clusters created earlier
   */
  private void stopClusters() throws Exception {
    if (mr != null) { mr.shutdown(); }
    if (dfs != null) { dfs.shutdown(); }
  }
  
  public void testFileListDirRaidPolicy() throws Exception {
    LOG.info("Test testFileListDirRaidPolicy started.");
    short targetReplication = 2;
    short metaReplication   = 2;
    short srcReplication = 3;

    createClusters(false, false);
    conf.setInt(ConfigManager.DIRRAID_BLOCK_LIMIT_KEY, 14);
    // avoid raidnode start raiding files
    conf.setLong(RaidNode.TRIGGER_MONITOR_SLEEP_TIME_KEY, 3600000);
    ConfigBuilder cb = new ConfigBuilder(CONFIG_FILE);
    cb.addAbstractPolicy("abstractPolicy2", targetReplication, metaReplication,
        "dir-xor");
    cb.addFileListPolicy("policy3", "/user/rvadali/raiddirlist.txt", "abstractPolicy2");
    cb.persist();
    RaidNode cnode = null;

    Path dirListPath = new Path("/user/rvadali/raiddirlist.txt");
    try {
      createTestFiles(fileSys, "/user/rvadali/dir-raidtest/1/",
          "/dir-raid/user/rvadali/dir-raidtest/1", 2, 8, srcReplication);
      createTestFiles(fileSys, "/user/rvadali/dir-raidtest/2/",
          "/dir-raid/user/rvadali/dir-raidtest/2", 2, 7, targetReplication);
      createTestFiles(fileSys, "/user/rvadali/dir-raidtest/3/",
          "/dir-raid/user/rvadali/dir-raidtest/3", 2, 3, targetReplication);
      createTestFiles(fileSys, "/user/rvadali/dir-raidtest/4/",
          "/dir-raid/user/rvadali/dir-raidtest/4", 2, 3, targetReplication);
      createTestFiles(fileSys, "/user/rvadali/dir-raidtest/5/",
          "/dir-raid/user/rvadali/dir-raidtest/5", 2, 3, targetReplication);
      LOG.info("Test testFileListDirRaidPolicy created test files");
      FSDataOutputStream dirOut = fileSys.create(dirListPath);
      FileStatus[] dirs = fileSys.listStatus(new Path("/user/rvadali/dir-raidtest"));
      for (FileStatus dir: dirs) {
        dirOut.write(dir.getPath().toString().getBytes());
        dirOut.write("\n".getBytes());
      }
      dirOut.close();

      cnode = RaidNode.createRaidNode(conf);
      PolicyInfo[] infos = cnode.getAllPolicies();
      assertEquals("we should have only one policy", 1, infos.length);
      PolicyInfo info = infos[0];
      cnode.triggerMonitor.putPolicyInfo(info);
      List<FileStatus> list1 = cnode.triggerMonitor.readFileList(info);
      assertEquals("Only one directory is selected", 1, list1.size());
      assertEquals("/user/rvadali/dir-raidtest/1",
          list1.get(0).getPath().toUri().getPath());
      List<FileStatus> list2 = cnode.triggerMonitor.readFileList(info);
      assertEquals("Only one directory is selected", 1, list2.size());
      assertEquals("/user/rvadali/dir-raidtest/2",
          list2.get(0).getPath().toUri().getPath());
      List<FileStatus> list3 = cnode.triggerMonitor.readFileList(info);
      assertEquals("Only 3 directories are selected", 3, list3.size());
      assertEquals("/user/rvadali/dir-raidtest/3",
          list3.get(0).getPath().toUri().getPath());
      assertEquals("/user/rvadali/dir-raidtest/4",
          list3.get(1).getPath().toUri().getPath());
      assertEquals("/user/rvadali/dir-raidtest/5",
          list3.get(2).getPath().toUri().getPath());
      List<FileStatus> list4 = cnode.triggerMonitor.readFileList(info);
      assertEquals("None is selected", 0, list4.size());
      LOG.info("Test testFileListDirRaidPolicy successful.");
    } catch (Exception e) {
      LOG.info("testFileListDirRaidPolicy Exception ", e);
      throw e;
    } finally {
      if (cnode != null) { cnode.stop(); cnode.join(); }
      stopClusters();
    }
    LOG.info("Test testFileListDirRaidPolicy completed.");
  }

  /**
   * Test to run a filter
   */
  public void testPathFilter() throws Exception {
    LOG.info("Test testPathFilter started.");

    long blockSizes    []  = {1024L};
    int stripeLengths []  = {5, 6, 10, 11, 12};
    int targetReplication = 1;
    int metaReplication   = 1;
    int  numBlock          = 11;
    int  iter = 0;

    createClusters(true, false);
    try {
      for (long blockSize : blockSizes) {
        for (int stripeLength : stripeLengths) {
           this.loadTestCodecs(conf, stripeLength, stripeLength, 1, 3);
           doTestPathFilter(iter, targetReplication, metaReplication,
                                              stripeLength, blockSize, numBlock);
           iter++;
        }
      }
      doCheckPolicy();
    } finally {
      stopClusters();
    }
    LOG.info("Test testPathFilter completed.");
  }
  
  private void simulateErrors(RaidShell shell, Path file1, long crc, long blockSize,
      long numBlock, long stripeLength) throws IOException {
    if (numBlock >= 1) {
      LOG.info("doTestPathFilter Check error at beginning of file.");
      simulateError(shell, fileSys, file1, crc, 0);
    }

    // check for error at the beginning of second block
    if (numBlock >= 2) {
      LOG.info("doTestPathFilter Check error at beginning of second block.");
      simulateError(shell, fileSys, file1, crc, blockSize + 1);
    }

    // check for error at the middle of third block
    if (numBlock >= 3) {
      LOG.info("doTestPathFilter Check error at middle of third block.");
      simulateError(shell, fileSys, file1, crc, 2 * blockSize + 10);
    }

    // check for error at the middle of second stripe
    if (numBlock >= stripeLength + 1) {
      LOG.info("doTestPathFilter Check error at middle of second stripe.");
      simulateError(shell, fileSys, file1, crc,
                                          stripeLength * blockSize + 100);
    }
  }

  /**
   * Test to run a filter
   */
  private void doTestPathFilter(int iter, long targetReplication,
                          long metaReplication, long stripeLength,
                          long blockSize, int numBlock) throws Exception {
    LOG.info("doTestPathFilter started---------------------------:" + 
             " iter " + iter + " blockSize=" + blockSize + " stripeLength=" +
             stripeLength);
    ConfigBuilder cb = new ConfigBuilder(CONFIG_FILE);
    cb.addPolicy("policy1", "/user/dhruba/raidtest", 
        targetReplication, metaReplication);
    cb.addPolicy("policy2", "/user/dhruba/dir-raidtest", 
        targetReplication, metaReplication, "dir-xor");
    cb.persist();

    RaidShell shell = null;
    Path dir = new Path("/user/dhruba/raidtest/");
    Path file1 = new Path(dir + "/file" + iter);
    Path dir1 = new Path("/user/dhruba/dir-raidtest/1");
    Path file2 = new Path(dir1 + "/file2");
    Path file3 = new Path(dir1 + "/file3");
    RaidNode cnode = null;
    try {
      Path destPath = new Path("/raid/user/dhruba/raidtest");
      Path destPath1 = new Path("/dir-raid/user/dhruba/dir-raidtest");
      fileSys.delete(dir, true);
      fileSys.delete(destPath, true);
      fileSys.delete(dir1, true);
      fileSys.delete(destPath1, true);
      long crc1 = createOldFile(fileSys, file1, 1, numBlock, blockSize);
      long crc2 = createOldFile(fileSys, file2, 1, numBlock, blockSize);
      long crc3 = createOldFile(fileSys, file3, 1, numBlock, blockSize);
      LOG.info("doTestPathFilter created test files for iteration " + iter);

      // create an instance of the RaidNode
      Configuration localConf = new Configuration(conf);
      cnode = RaidNode.createRaidNode(null, localConf);
      TestRaidDfs.waitForFileRaided(LOG, fileSys, file1, destPath);
      TestRaidDfs.waitForDirRaided(LOG, fileSys, dir1, destPath1);
      
      LOG.info("doTestPathFilter all files found in Raid.");
      // check for error at beginning of file
      shell = new RaidShell(conf);
      shell.initializeRpc(conf, cnode.getListenerAddress());
      this.simulateErrors(shell, file1, crc1, blockSize,
          numBlock, stripeLength);
      this.simulateErrors(shell, file2, crc2, blockSize,
          numBlock, stripeLength);
      this.simulateErrors(shell, file3, crc3, blockSize,
          numBlock, stripeLength);
    } catch (Exception e) {
      LOG.info("doTestPathFilter Exception ", e);
      throw e;
    } finally {
      if (shell != null) shell.close();
      if (cnode != null) { cnode.stop(); cnode.join(); }
      LOG.info("doTestPathFilter clean up" );
      fileSys.delete(dir, true);
      fileSys.delete(new Path("/raid"), true);
      fileSys.delete(dir1, true);
      fileSys.delete(new Path("/dir-raid"), true);
    }
    LOG.info("doTestPathFilter completed:" + " blockSize=" + blockSize +
                                             " stripeLength=" + stripeLength);
  }

  // Check that raid occurs only on files that have a replication factor
  // greater than or equal to the specified value
  private void doCheckPolicy() throws Exception {
    LOG.info("doCheckPolicy started---------------------------:"); 
    short srcReplication = 3;
    short targetReplication = 2;
    long metaReplication = 1;
    long stripeLength = 2;
    long blockSize = 1024;
    int numBlock = 3;
    ConfigBuilder cb = new ConfigBuilder(CONFIG_FILE);
    cb.addPolicy("policy1", "/user/dhruba/policytest", targetReplication,
        metaReplication);
    cb.addPolicy("policy2", "/user/dhruba/dir-policytest", 
        targetReplication, metaReplication, "dir-xor");
    cb.persist();
    Path dir = new Path("/user/dhruba/policytest/");
    Path dir1 = new Path("/user/dhruba/dir-policytest/1");
    Path file1 = new Path(dir + "/file1");
    Path file2 = new Path(dir1 + "/file2");
    Path file3 = new Path(dir1 + "/file3");
    Path file4 = new Path(dir1 + "/file4");
    RaidNode cnode = null;
    try {
      Path destPath = new Path("/raid/user/dhruba/policytest");
      Path destPath1 = new Path("/dir-raid/user/dhruba/dir-policytest");
      fileSys.delete(dir, true);
      fileSys.delete(destPath, true);
      fileSys.delete(dir1, true);
      fileSys.delete(destPath1, true);

      // create an instance of the RaidNode
      Configuration localConf = new Configuration(conf);
      cnode = RaidNode.createRaidNode(null, localConf);

      // this file should be picked up RaidNode
      createOldFile(fileSys, file1, 3, numBlock, blockSize);
      createOldFile(fileSys, file2, 3, numBlock, blockSize);
      createOldFile(fileSys, file3, 3, numBlock, blockSize);
      TestRaidDfs.waitForFileRaided(LOG, fileSys, file1, destPath, targetReplication);
      TestRaidDfs.waitForDirRaided(LOG, fileSys, dir1, destPath1, targetReplication);

      LOG.info("doCheckPolicy all files found in Raid the first time.");

      LOG.info("doCheckPolicy: recreating source file");
      long firstmodetime1 = fileSys.getFileStatus(file1).getModificationTime();
      createOldFile(fileSys, file1, 3, numBlock, blockSize);
      assertTrue(fileSys.getFileStatus(file1).getModificationTime() > firstmodetime1);
      
      LOG.info("Change the modification time of directory");
      long firstmodetime2 = fileSys.getFileStatus(dir1).getModificationTime();
      createOldFile(fileSys, file4, 3, numBlock, blockSize);
      assertTrue(fileSys.getFileStatus(dir1).getModificationTime() > firstmodetime2);
      TestRaidDfs.waitForFileRaided(LOG, fileSys, file1, destPath, targetReplication);
      TestRaidDfs.waitForDirRaided(LOG, fileSys, dir1, destPath1, targetReplication);

      LOG.info("doCheckPolicy: file got re-raided as expected.");
      
    } catch (Exception e) {
      LOG.info("doCheckPolicy Exception ", e);
      throw e;
    } finally {
      if (cnode != null) { cnode.stop(); cnode.join(); }
      LOG.info("doTestPathFilter clean up");
      fileSys.delete(dir, true);
      fileSys.delete(new Path("/raid"), true);
      fileSys.delete(dir1, true);
      fileSys.delete(new Path("/dir-raid"), true);
    }
    LOG.info("doCheckPolicy completed:");
  }

  static public void createTestFiles(FileSystem fileSys, 
      String path, String destpath, int nfile,
      int nblock) throws IOException {
    createTestFiles(fileSys, path, destpath, nfile, nblock, (short)1);
  }

  static void createTestFiles(FileSystem fileSys, String path, String destpath, int nfile,
      int nblock, short repl) throws IOException {
    long blockSize         = 1024L;
    Path dir = new Path(path);
    Path destPath = new Path(destpath);
    fileSys.delete(dir, true);
    fileSys.delete(destPath, true);
   
    for(int i = 0 ; i < nfile; i++){
      Path file = new Path(dir, "file" + i);
      createOldFile(fileSys, file, repl, nblock, blockSize);
    }
  }
  
  private void checkTestFiles(String srcDir, String parityDir, int stripeLength, 
      short targetReplication, short metaReplication, PlacementMonitor pm, 
      Codec codec, int nfiles) throws IOException, InterruptedException {
    for(int i = 0 ; i < nfiles; i++){
      Path srcPath = new Path(srcDir, "file" + i);
      Path parityPath = null;
      if (codec.isDirRaid) {
        parityPath = new Path(parityDir);
        TestRaidDfs.waitForDirRaided(LOG, fileSys, srcPath.getParent(),
            parityPath.getParent(), targetReplication);
      } else {
        parityPath = new Path(parityDir, "file" + i);
        TestRaidDfs.waitForFileRaided(LOG, fileSys, srcPath, parityPath.getParent(),
            targetReplication);
      }
      TestRaidDfs.waitForReplicasReduction(fileSys, parityPath,
          targetReplication);
      FileStatus srcFile = fileSys.getFileStatus(srcPath);
      FileStatus parityStat = fileSys.getFileStatus(parityPath);
      assertEquals(srcFile.getReplication(), targetReplication);
      assertEquals(parityStat.getReplication(), metaReplication);
      List<BlockInfo> parityBlocks = pm.getBlockInfos(fileSys, parityStat);
      
      int parityLength = codec.parityLength; 
      if (parityLength == 1) { 
        continue;
      }
      if (codec.isDirRaid && i > 0) {
        // One directory has one parity, just need to check once
        continue;
      }
      long numBlocks;
      if (codec.isDirRaid) {
        List<FileStatus> lfs = RaidNode.listDirectoryRaidFileStatus(conf, fileSys, 
            new Path(srcDir));
        numBlocks = DirectoryStripeReader.getBlockNum(lfs);
      } else {
        numBlocks = RaidNode.numBlocks(srcFile);
      }
      int numStripes = (int)RaidNode.numStripes(numBlocks, stripeLength);

      Map<String, Integer> nodeToNumBlocks = new HashMap<String, Integer>();
      Set<String> nodesInThisStripe = new HashSet<String>();
      for (int stripeIndex = 0; stripeIndex < numStripes; ++stripeIndex) {
        List<BlockInfo> stripeBlocks = new ArrayList<BlockInfo>();
        // Adding parity blocks
        int stripeStart = parityLength * stripeIndex;
        int stripeEnd = Math.min(
            stripeStart + parityLength, parityBlocks.size());
        if (stripeStart < stripeEnd) {
          stripeBlocks.addAll(parityBlocks.subList(stripeStart, stripeEnd));
        }
        PlacementMonitor.countBlocksOnEachNode(stripeBlocks, nodeToNumBlocks, nodesInThisStripe);
        LOG.info("file: " + parityPath + " stripe: " + stripeIndex);
        int max = 0;
        for (String node: nodeToNumBlocks.keySet()) {
          int count = nodeToNumBlocks.get(node);
          LOG.info("node:" + node + " count:" + count);
          if (max < count) {
            max = count; 
          }
        }
        assertTrue("pairty blocks in a stripe cannot live in the same node", max<parityLength);
      }
    }
  }

  /**
   * Test dist Raid
   */
  public void testDistRaid() throws Exception {
    LOG.info("Test testDistRaid started.");
    short targetReplication = 2;
    short metaReplication   = 2;
    short rstargetReplication = 1;
    short rsmetaReplication   = 1;
    short xorstripeLength = 3;
    int rsstripeLength      = 10;

    createClusters(false, true);
    ConfigBuilder cb = new ConfigBuilder(CONFIG_FILE);
    cb.addPolicy("policy1", "/user/dhruba/raidtest", 
        targetReplication, metaReplication);
    cb.addAbstractPolicy("abstractPolicy", targetReplication, 
        metaReplication, "xor");
    cb.addPolicy("policy2", "/user/dhruba/raidtest2", "abstractPolicy");
    cb.addPolicy("policy3", "/user/dhruba/raidtest3", 
        rstargetReplication, rsmetaReplication, "rs");
    cb.addPolicy("policy4", "/user/dhruba/dir-raidtest/", 
        targetReplication, metaReplication, "dir-xor");
    cb.addPolicy("policy5", "/user/dhruba/dir-raidtestrs/", 
        rstargetReplication, rsmetaReplication, "dir-rs");
    cb.persist();

    RaidNode cnode = null;
    try {
      createTestFiles(fileSys, "/user/dhruba/raidtest/",
          "/raid/user/dhruba/raidtest", 5, 7);
      createTestFiles(fileSys, "/user/dhruba/raidtest2/",
          "/raid/user/dhruba/raidtest2", 5, 7);
      createTestFiles(fileSys, "/user/dhruba/raidtest3/",
          "/raidrs/user/dhruba/raidtest3", 1, 10);
      createTestFiles(fileSys, "/user/dhruba/dir-raidtest/1/",
          "/dir-raid/user/dhruba/dir-raidtest/1", 5, 7);
      createTestFiles(fileSys, "/user/dhruba/dir-raidtestrs/2/",
          "/dir-raidrs/user/dhruba/dir-raidtestrs/2", 2, 8);
      LOG.info("Test testDistRaid created test files");

      Configuration localConf = new Configuration(conf);
      //Avoid block mover to move blocks
      localConf.setInt(PlacementMonitor.BLOCK_MOVE_QUEUE_LENGTH_KEY, 0);
      localConf.setInt(PlacementMonitor.NUM_MOVING_THREADS_KEY, 1);
      // don't allow rescan, make sure only one job is submitted.
      localConf.setLong("raid.policy.rescan.interval", 3600 * 1000L);
      cnode = RaidNode.createRaidNode(null, localConf);
      // Verify the policies are parsed correctly
      for (PolicyInfo p: cnode.getAllPolicies()) {
          if (p.getName().equals("policy1")) {
            Path srcPath = new Path("/user/dhruba/raidtest");
            assertTrue(p.getSrcPath().equals(
                srcPath.makeQualified(srcPath.getFileSystem(conf))));
          } else if (p.getName().equals("policy2")) {
            Path srcPath = new Path("/user/dhruba/raidtest2");
            assertTrue(p.getSrcPath().equals(
                srcPath.makeQualified(srcPath.getFileSystem(conf))));
          } else if (p.getName().equals("policy3")){
            Path srcPath = new Path("/user/dhruba/raidtest3");
            assertTrue(p.getSrcPath().equals(
                srcPath.makeQualified(srcPath.getFileSystem(conf))));
          } else if (p.getName().equals("policy4")) {
            Path srcPath = new Path("/user/dhruba/dir-raidtest/");
            assertTrue(p.getSrcPath().equals(
                srcPath.makeQualified(srcPath.getFileSystem(conf))));
          } else {
            assertEquals(p.getName(), "policy5");
            Path srcPath = new Path("/user/dhruba/dir-raidtestrs/");
            assertTrue(p.getSrcPath().equals(
                srcPath.makeQualified(srcPath.getFileSystem(conf))));
          }
          if (p.getName().equals("policy3") || p.getName().equals("policy5")) {
            if (p.getName().equals("policy3")) {
              assertTrue(p.getCodecId().equals("rs"));
            } else {
              assertTrue(p.getCodecId().equals("dir-rs"));
            }
            assertEquals(rstargetReplication,
                         Integer.parseInt(p.getProperty("targetReplication")));
            assertEquals(rsmetaReplication,
                         Integer.parseInt(p.getProperty("metaReplication")));
          } else {
            if (p.getName().equals("policy4")) {
              assertTrue(p.getCodecId().equals("dir-xor"));
            } else {
              assertTrue(p.getCodecId().equals("xor"));
            }
            assertEquals(targetReplication,
                         Integer.parseInt(p.getProperty("targetReplication")));
            assertEquals(metaReplication,
                         Integer.parseInt(p.getProperty("metaReplication")));
          }
      }

      long start = System.currentTimeMillis();
      final int MAX_WAITTIME = 120000;
      
      assertTrue("cnode is not DistRaidNode", cnode instanceof DistRaidNode);
      DistRaidNode dcnode = (DistRaidNode) cnode;
      checkTestFiles("/user/dhruba/raidtest/", "/raid/user/dhruba/raidtest", 
          xorstripeLength, targetReplication, metaReplication, dcnode.placementMonitor,
          Codec.getCodec("xor"), 5);
      checkTestFiles("/user/dhruba/raidtest2/", "/raid/user/dhruba/raidtest2", 
          xorstripeLength, targetReplication, metaReplication, dcnode.placementMonitor,
          Codec.getCodec("xor"), 5);
      checkTestFiles("/user/dhruba/raidtest3/", "/raidrs/user/dhruba/raidtest3", 
          rsstripeLength, rstargetReplication, rsmetaReplication, dcnode.placementMonitor,
          Codec.getCodec("rs"), 1);
      checkTestFiles("/user/dhruba/dir-raidtest/1/", "/dir-raid/user/dhruba/dir-raidtest/1", 
          xorstripeLength, targetReplication, metaReplication, dcnode.placementMonitor,
          Codec.getCodec("dir-xor"), 5);
      checkTestFiles("/user/dhruba/dir-raidtestrs/2/", "/dir-raidrs/user/dhruba/dir-raidtestrs/2", 
          rsstripeLength, rstargetReplication, rsmetaReplication, dcnode.placementMonitor,
          Codec.getCodec("dir-rs"), 2);
      while (dcnode.jobMonitor.runningJobsCount() > 0 &&
          System.currentTimeMillis() - start < MAX_WAITTIME) {
        LOG.info("Waiting for zero running jobs: " +
            dcnode.jobMonitor.runningJobsCount());
        Thread.sleep(1000);
      }
      TestBlockFixer.verifyMetrics(fileSys, cnode, LOGTYPES.ENCODING,
          LOGRESULTS.SUCCESS, 5 + 5 + 1 + 1 + 1, true); 

      LOG.info("Test testDistRaid successful.");
    } catch (Exception e) {
      LOG.info("testDistRaid Exception " + e + StringUtils.stringifyException(e));
      throw e;
    } finally {
      if (cnode != null) { cnode.stop(); cnode.join(); }
      stopClusters();
    }
    LOG.info("Test testDistRaid completed.");
  }
  
  //
  // simulate a corruption at specified offset and verify that eveyrthing is good
  //
  void simulateError(RaidShell shell, FileSystem fileSys, Path file1, 
                     long crc, long corruptOffset) throws IOException {
    // recover the file assuming that we encountered a corruption at offset 0
    String[] args = new String[3];
    args[0] = "-recover";
    args[1] = file1.toString();
    args[2] = Long.toString(corruptOffset);
    Path recover1 = shell.recover(args[0], args, 1)[0];

    // compare that the recovered file is identical to the original one
    LOG.info("Comparing file " + file1 + " with recovered file " + recover1);
    validateFile(fileSys, file1, recover1, crc);
    fileSys.delete(recover1, false);
  }

  //
  // creates a file and populate it with random data. Returns its crc.
  //
  static long createOldFile(FileSystem fileSys, Path name, int repl, int numBlocks, long blocksize)
    throws IOException {
    CRC32 crc = new CRC32();
    FSDataOutputStream stm = fileSys.create(name, true,
                                            fileSys.getConf().getInt("io.file.buffer.size", 4096),
                                            (short)repl, blocksize);
    // fill random data into file
    byte[] b = new byte[(int)blocksize];
    for (int i = 0; i < numBlocks; i++) {
      if (i == (numBlocks-1)) {
        b = new byte[(int)blocksize/2]; 
      }
      rand.nextBytes(b);
      stm.write(b);
      crc.update(b);
    }
    
    stm.close();
    return crc.getValue();
  }

  //
  // validates that file matches the crc.
  //
  private void validateFile(FileSystem fileSys, Path name1, Path name2, long crc) 
    throws IOException {

    FileStatus stat1 = fileSys.getFileStatus(name1);
    FileStatus stat2 = fileSys.getFileStatus(name2);
    assertTrue(" Length of file " + name1 + " is " + stat1.getLen() + 
               " is different from length of file " + name1 + " " + stat2.getLen(),
               stat1.getLen() == stat2.getLen());

    CRC32 newcrc = new CRC32();
    FSDataInputStream stm = fileSys.open(name2);
    final byte[] b = new byte[4192];
    int num = 0;
    while (num >= 0) {
      num = stm.read(b);
      if (num < 0) {
        break;
      }
      newcrc.update(b, 0, num);
    }
    stm.close();
    if (newcrc.getValue() != crc) {
      fail("CRC mismatch of files " + name1 + " with file " + name2);
    }
  }

  public void testSuspendTraversal() throws Exception {
    LOG.info("Test testSuspendTraversal started.");
    long targetReplication = 2;
    long metaReplication   = 2;

    createClusters(false, false);
    ConfigBuilder cb = new ConfigBuilder(CONFIG_FILE);
    cb.addPolicy("policy1", "/user/dhruba/raidtest", targetReplication, metaReplication);
    cb.persist();

    RaidNode cnode = null;
    try {
      for(int i = 0; i < 6; i++){
        Path file = new Path("/user/dhruba/raidtest/dir" + i + "/file" + i);
        createOldFile(fileSys, file, 1, 7, 1024L);
      }

      LOG.info("Test testSuspendTraversal created test files");

      Configuration localConf = new Configuration(conf);
      localConf.setInt("raid.distraid.max.jobs", 3);
      localConf.setInt("raid.distraid.max.files", 2);
      localConf.setInt("raid.directorytraversal.threads", 1);
      localConf.setBoolean(StatisticsCollector.STATS_COLLECTOR_SUBMIT_JOBS_CONFIG, false);
      // don't allow rescan, make sure only one job is submitted.
      localConf.setLong("raid.policy.rescan.interval", 3600 * 1000L);
      // 6 test files: 3 jobs with 2 files each.
      final int numJobsExpected = 3;
      cnode = RaidNode.createRaidNode(null, localConf);

      long start = System.currentTimeMillis();
      final int MAX_WAITTIME = 300000;

      assertTrue("cnode is not DistRaidNode", cnode instanceof DistRaidNode);
      DistRaidNode dcnode = (DistRaidNode) cnode;

      start = System.currentTimeMillis();
      while (dcnode.jobMonitor.jobsSucceeded() < numJobsExpected &&
             System.currentTimeMillis() - start < MAX_WAITTIME) {
        LOG.info("Waiting for num jobs succeeded " + dcnode.jobMonitor.jobsSucceeded() + 
         " to reach " + numJobsExpected);
        Thread.sleep(3000);
      }
      // Wait for any running jobs to finish.
      start = System.currentTimeMillis();
      while (dcnode.jobMonitor.runningJobsCount() > 0 &&
             System.currentTimeMillis() - start < MAX_WAITTIME) {
        LOG.info("Waiting for zero running jobs: " +
             dcnode.jobMonitor.runningJobsCount());
        Thread.sleep(1000);
      }
      assertEquals(numJobsExpected, dcnode.jobMonitor.jobsMonitored());
      assertEquals(numJobsExpected, dcnode.jobMonitor.jobsSucceeded());

      LOG.info("Test testSuspendTraversal successful.");
      TestBlockFixer.verifyMetrics(fileSys, cnode, LOGTYPES.ENCODING,
          LOGRESULTS.SUCCESS, 6L, true); 
    } catch (Exception e) {
      LOG.info("testSuspendTraversal Exception " + e + StringUtils.stringifyException(e));
      throw e;
    } finally {
      if (cnode != null) { cnode.stop(); cnode.join(); }
      stopClusters();
    }
    LOG.info("Test testSuspendTraversal completed.");
  }
  
  public void testFileListPolicy() throws Exception {
    LOG.info("Test testFileListPolicy started.");
    short targetReplication = 2;
    short metaReplication   = 2;
    long stripeLength      = 3;
    short srcReplication = 3;

    createClusters(false, false);
    ConfigBuilder cb = new ConfigBuilder(CONFIG_FILE);
    cb.addAbstractPolicy("abstractPolicy",  targetReplication, metaReplication,
        "xor");
    cb.addFileListPolicy("policy2", "/user/rvadali/raidfilelist.txt", "abstractPolicy");
    cb.addAbstractPolicy("abstractPolicy2", targetReplication, metaReplication,
        "dir-xor");
    cb.addFileListPolicy("policy3", "/user/rvadali/raiddirlist.txt", "abstractPolicy2");
    cb.persist();

    RaidNode cnode = null;
    Path fileListPath = new Path("/user/rvadali/raidfilelist.txt");
    Path dirListPath = new Path("/user/rvadali/raiddirlist.txt");
    try {
      createTestFiles(fileSys, "/user/rvadali/raidtest/",
          "/raid/user/rvadali/raidtest", 5, 7, srcReplication);
      createTestFiles(fileSys, "/user/rvadali/dir-raidtest/1/",
          "/dir-raid/user/rvadali/dir-raidtest/1", 2, 7, srcReplication);
      // althought the files reach the target replication, because
      // they don't have parities, we are still going to raid them.
      createTestFiles(fileSys, "/user/rvadali/dir-raidtest/2/",
          "/dir-raid/user/rvadali/dir-raidtest/2", 2, 7, targetReplication);
      createTestFiles(fileSys, "/user/dhruba/unraidable/",
          "/raid/user/dhruba/unradiable/", 2, 1, srcReplication);
      // generate parities files for the following directory, raidnode
      // won't raid them again
      Path srcDir3 = new Path("/user/rvadali/dir-raidtest/3/");
      Path destPath3 = new Path("/dir-raid/user/rvadali/dir-raidtest/3");
      createTestFiles(fileSys, srcDir3.toString(), destPath3.toString(), 2, 4,
          targetReplication);
      Codec dirCode = Codec.getCodec("dir-xor");
      FileStatus srcStat3 = fileSys.getFileStatus(srcDir3);
      assertTrue(RaidNode.doRaid(conf, srcStat3,
          new Path(dirCode.parityDirectory), dirCode, new RaidNode.Statistics(),
          RaidUtils.NULL_PROGRESSABLE, false, targetReplication, metaReplication));
      FileStatus parityStat3 = fileSys.getFileStatus(destPath3);
      assertEquals(parityStat3.getModificationTime(), srcStat3.getModificationTime());
      assertEquals(parityStat3.getReplication(), metaReplication);
      LOG.info("Test testFileListPolicy created test files");

      // Create list of files to raid.
      FSDataOutputStream out = fileSys.create(fileListPath);
      FileStatus[] files = fileSys.listStatus(new Path("/user/rvadali/raidtest"));
      for (FileStatus f: files) {
        out.write(f.getPath().toString().getBytes());
        out.write("\n".getBytes());
      }
      // write directory, we should filter it.
      out.write("/user/rvadali/raidtest/\n".getBytes());
      // small file, we should filter it
      out.write("/user/dhruba/unraidable/file0\n".getBytes());
      out.write("/user/dhruba/unraidable/file1\n".getBytes());
      out.close();
      
      FSDataOutputStream dirOut = fileSys.create(dirListPath);
      FileStatus[] dirs = fileSys.listStatus(new Path("/user/rvadali/dir-raidtest"));
      for (FileStatus dir: dirs) {
        dirOut.write(dir.getPath().toString().getBytes());
        dirOut.write("\n".getBytes());
      }
      // write file, we should filter it
      dirOut.write("/user/rvadali/raidtest/file0\n".getBytes());
      // small directory, we should filter it
      out.write("/user/dhruba/unraidable\n".getBytes());
      dirOut.close();

      Configuration localConf = new Configuration(conf);
      // don't allow rescan, make sure only one job is submitted.
      localConf.setLong("raid.policy.rescan.interval", 3600 * 1000L);
      cnode = RaidNode.createRaidNode(localConf);
      final int MAX_WAITTIME = 120000;
      DistRaidNode dcnode = (DistRaidNode) cnode;

      long start = System.currentTimeMillis();
      int numJobsExpected = 2;
      while (dcnode.jobMonitor.jobsSucceeded() < numJobsExpected &&
             System.currentTimeMillis() - start < MAX_WAITTIME) {
        LOG.info("Waiting for num jobs succeeded " + dcnode.jobMonitor.jobsSucceeded() + 
         " to reach " + numJobsExpected);
        Thread.sleep(1000);
      }
      assertEquals(numJobsExpected, dcnode.jobMonitor.jobsMonitored());
      assertEquals(numJobsExpected, dcnode.jobMonitor.jobsSucceeded());
      
      Path destPath = new Path("/raid/user/rvadali/raidtest");
      for (FileStatus file : files) {
        TestRaidDfs.waitForFileRaided(LOG, fileSys, file.getPath(), destPath,
            targetReplication);
      }
      Path destPath1 = new Path("/dir-raid/user/rvadali/dir-raidtest");
      for (FileStatus dir : dirs) {
        TestRaidDfs.waitForDirRaided(LOG, fileSys, dir.getPath(), destPath1,
            targetReplication);
      }
      assertTrue(!fileSys.exists(new Path("/raid/user/dhruba/unraidable/file0")));
      assertTrue(!fileSys.exists(new Path("/raid/user/dhruba/unraidable/file1")));
      assertTrue(!fileSys.exists(new Path("/dir-raid/user/dhruba/unraidable")));
      
      TestBlockFixer.verifyMetrics(fileSys, cnode, LOGTYPES.ENCODING,
          LOGRESULTS.SUCCESS, 7L, true); 
      Map<String, Counters> raidProgress = dcnode.jobMonitor.getRaidProgress();
      long succeedFiles = 0;
      for (Counters ctrs: raidProgress.values()) {
        Counters.Counter ctr = ctrs.findCounter(Counter.FILES_SUCCEEDED);
        succeedFiles += ctr.getValue();
      }
      // We have one raided directory, so it's total - 1;
      assertEquals(succeedFiles, files.length + dirs.length -1); 
      LOG.info("Test testFileListPolicy successful.");
    } catch (Exception e) {
      LOG.info("testFileListPolicy Exception " + e + StringUtils.stringifyException(e));
      throw e;
    } finally {
      if (cnode != null) { cnode.stop(); cnode.join(); }
      stopClusters();
    }
    LOG.info("Test testFileListPolicy completed.");
  }
}
