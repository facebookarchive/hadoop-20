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
import org.apache.hadoop.mapred.MiniMRCluster;
import org.apache.hadoop.raid.protocol.PolicyInfo;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.hdfs.protocol.DataTransferProtocol;
import org.apache.hadoop.raid.PlacementMonitor.BlockInfo;

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

    Utils.loadTestCodecs(conf, 3, 10, 1, 5, "/raid", "/raidrs", false, false);

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

    conf.set("raid.server.address", "localhost:0");

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
      assertEquals(DataTransferProtocol.DATA_TRANSFER_VERSION,
                   RaidUtils.getDataTransferProtocolVersion(conf));
      
      for (long blockSize : blockSizes) {
        for (int stripeLength : stripeLengths) {
           Utils.loadTestCodecs(conf, stripeLength, 1, 3, "/raid", "/raidrs");
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

  /**
   * Test to run a filter
   */
  private void doTestPathFilter(int iter, long targetReplication,
                          long metaReplication, long stripeLength,
                          long blockSize, int numBlock) throws Exception {
    LOG.info("doTestPathFilter started---------------------------:" +  " iter " + iter +
             " blockSize=" + blockSize + " stripeLength=" + stripeLength);
    ConfigBuilder cb = new ConfigBuilder(CONFIG_FILE);
    cb.addPolicy("policy1", "/user/dhruba/raidtest", (short)1, targetReplication, metaReplication);
    cb.persist();

    RaidShell shell = null;
    Path dir = new Path("/user/dhruba/raidtest/");
    Path file1 = new Path(dir + "/file" + iter);
    RaidNode cnode = null;
    try {
      Path destPath = new Path("/raid/user/dhruba/raidtest");
      fileSys.delete(dir, true);
      fileSys.delete(destPath, true);
      long crc1 = createOldFile(fileSys, file1, 1, numBlock, blockSize);
      LOG.info("doTestPathFilter created test files for iteration " + iter);

      // create an instance of the RaidNode
      Configuration localConf = new Configuration(conf);
      cnode = RaidNode.createRaidNode(null, localConf);
      FileStatus[] listPaths = null;

      // wait till file is raided
      while (true) {
        try {
          listPaths = fileSys.listStatus(destPath);
          int count = 0;
          if (listPaths != null && listPaths.length == 1) {
            for (FileStatus s : listPaths) {
              LOG.info("doTestPathFilter found path " + s.getPath());
              if (!s.getPath().toString().endsWith(".tmp") &&
                  fileSys.getFileStatus(file1).getReplication() ==
                  targetReplication) {
                count++;
              }
            }
          }
          if (count > 0) {
            break;
          }
        } catch (FileNotFoundException e) {
          //ignore
        }
        LOG.info("doTestPathFilter waiting for files to be raided. Found " + 
                 (listPaths == null ? "none" : listPaths.length));
        Thread.sleep(1000);                  // keep waiting
      }
      // assertEquals(listPaths.length, 1); // all files raided
      LOG.info("doTestPathFilter all files found in Raid.");

      // check for error at beginning of file
      shell = new RaidShell(conf);
      shell.initializeRpc(conf, cnode.getListenerAddress());
      if (numBlock >= 1) {
        LOG.info("doTestPathFilter Check error at beginning of file.");
        simulateError(shell, fileSys, file1, crc1, 0);
      }

      // check for error at the beginning of second block
      if (numBlock >= 2) {
        LOG.info("doTestPathFilter Check error at beginning of second block.");
        simulateError(shell, fileSys, file1, crc1, blockSize + 1);
      }

      // check for error at the middle of third block
      if (numBlock >= 3) {
        LOG.info("doTestPathFilter Check error at middle of third block.");
        simulateError(shell, fileSys, file1, crc1, 2 * blockSize + 10);
      }

      // check for error at the middle of second stripe
      if (numBlock >= stripeLength + 1) {
        LOG.info("doTestPathFilter Check error at middle of second stripe.");
        simulateError(shell, fileSys, file1, crc1,
                                            stripeLength * blockSize + 100);
      }

    } catch (Exception e) {
      LOG.info("doTestPathFilter Exception " + e +
                                          StringUtils.stringifyException(e));
      throw e;
    } finally {
      if (shell != null) shell.close();
      if (cnode != null) { cnode.stop(); cnode.join(); }
      LOG.info("doTestPathFilter delete file " + file1);
      fileSys.delete(file1, true);
    }
    LOG.info("doTestPathFilter completed:" + " blockSize=" + blockSize +
                                             " stripeLength=" + stripeLength);
  }

  // Check that raid occurs only on files that have a replication factor
  // greater than or equal to the specified value
  private void doCheckPolicy() throws Exception {
    LOG.info("doCheckPolicy started---------------------------:"); 
    short srcReplication = 3;
    long targetReplication = 2;
    long metaReplication = 1;
    long stripeLength = 2;
    long blockSize = 1024;
    int numBlock = 3;
    ConfigBuilder cb = new ConfigBuilder(CONFIG_FILE);
    cb.addPolicy("policy1", "/user/dhruba/policytest", (short)1, targetReplication,
        metaReplication);
    cb.persist();
    Path dir = new Path("/user/dhruba/policytest/");
    Path file2 = new Path(dir + "/file2");
    RaidNode cnode = null;
    try {
      Path destPath = new Path("/raid/user/dhruba/policytest");
      fileSys.delete(dir, true);
      fileSys.delete(destPath, true);

      // create an instance of the RaidNode
      Configuration localConf = new Configuration(conf);
      cnode = RaidNode.createRaidNode(null, localConf);

      // this file should be picked up RaidNode
      long crc2 = createOldFile(fileSys, file2, 2, numBlock, blockSize);
      FileStatus[] listPaths = null;

      long firstmodtime = 0;
      // wait till file is raided
      while (true) {
        Thread.sleep(1000L);                  // waiting
        listPaths = fileSys.listStatus(destPath);
        int count = 0;
        if (listPaths != null && listPaths.length == 1) {
          for (FileStatus s : listPaths) {
            LOG.info("doCheckPolicy found path " + s.getPath());
            if (!s.getPath().toString().endsWith(".tmp") &&
                fileSys.getFileStatus(file2).getReplication() ==
                targetReplication) {
              count++;
              firstmodtime = s.getModificationTime();
            }
          }
        }
        if (count > 0) {
          break;
        }
        LOG.info("doCheckPolicy waiting for files to be raided. Found " + 
                 (listPaths == null ? "none" : listPaths.length));
      }
      assertEquals(listPaths.length, 1);

      LOG.info("doCheckPolicy all files found in Raid the first time.");

      LOG.info("doCheckPolicy: recreating source file");
      crc2 = createOldFile(fileSys, file2, 2, numBlock, blockSize);

      FileStatus st = fileSys.getFileStatus(file2);
      assertTrue(st.getModificationTime() > firstmodtime);
      
      // wait till file is raided
      while (true) {
        Thread.sleep(1000L);                  // waiting
        listPaths = fileSys.listStatus(destPath);
        int count = 0;
        if (listPaths != null && listPaths.length == 1) {
          for (FileStatus s : listPaths) {
            LOG.info("doCheckPolicy found path " + s.getPath() + " " + s.getModificationTime());
            if (!s.getPath().toString().endsWith(".tmp") &&
                s.getModificationTime() > firstmodtime &&
                fileSys.getFileStatus(file2).getReplication() ==
                targetReplication) {
              count++;
            }
          }
        }
        if (count > 0) {
          break;
        }
        LOG.info("doCheckPolicy waiting for files to be raided. Found " + 
                 (listPaths == null ? "none" : listPaths.length));
      } 
      assertEquals(listPaths.length, 1);

      LOG.info("doCheckPolicy: file got re-raided as expected.");
      
    } catch (Exception e) {
      LOG.info("doCheckPolicy Exception " + e +
                                          StringUtils.stringifyException(e));
      throw e;
    } finally {
      if (cnode != null) { cnode.stop(); cnode.join(); }
      LOG.info("doTestPathFilter delete file " + file2);
      fileSys.delete(file2, false);
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
      Path file = new Path(path, "file" + i);
      createOldFile(fileSys, file, repl, nblock, blockSize);
    }
  }
  
  private void checkTestFiles(String srcDir, String parityDir, int stripeLength, 
      short targetReplication, short metaReplication, PlacementMonitor pm, 
      Codec codec, int nfiles) throws IOException {
    for(int i = 0 ; i < nfiles; i++){
      Path srcPath = new Path(srcDir, "file" + i);
      Path parityPath = new Path(parityDir, "file" + i);
      FileStatus srcFile = fileSys.getFileStatus(srcPath);
      FileStatus parityStat = fileSys.getFileStatus(parityPath);
      assertEquals(srcFile.getReplication(), targetReplication);
      assertEquals(parityStat.getReplication(), metaReplication);
      List<BlockInfo> parityBlocks = pm.getBlockInfos(fileSys, parityStat);
      
      int parityLength = codec.parityLength; 
      if (parityLength == 1) { 
        continue;
      }
      int numBlocks = (int)Math.ceil(1D * srcFile.getLen() /
                                     srcFile.getBlockSize());
      int numStripes = (int)Math.ceil(1D * (numBlocks) / stripeLength);

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
        LOG.info("file: " + srcPath + " stripe: " + stripeIndex);
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
    short srcReplication = 1;
    short rstargetReplication = 1;
    short rsmetaReplication   = 1;
    int rsstripeLength      = 10;
    short rssrcReplication = 1;
    

    createClusters(false, true);
    ConfigBuilder cb = new ConfigBuilder(CONFIG_FILE);
    cb.addPolicy("policy1", "/user/dhruba/raidtest", srcReplication, 
        targetReplication, metaReplication);
    cb.addPolicy("abstractPolicy", srcReplication, targetReplication, 
        metaReplication);
    cb.addPolicy("policy2", "/user/dhruba/raidtest2", "abstractPolicy");
    cb.addPolicy("policy3", "/user/dhruba/raidtest3", rssrcReplication, 
        rstargetReplication, rsmetaReplication, "rs");
    cb.persist();

    RaidNode cnode = null;
    try {
      createTestFiles(fileSys, "/user/dhruba/raidtest/",
          "/raid/user/dhruba/raidtest", 5, 7);
      createTestFiles(fileSys, "/user/dhruba/raidtest2/",
          "/raid/user/dhruba/raidtest2", 5, 7);
      createTestFiles(fileSys, "/user/dhruba/raidtest3/",
          "/raidrs/user/dhruba/raidtest3", 1, 10);
      LOG.info("Test testDistRaid created test files");

      Configuration localConf = new Configuration(conf);
      //Avoid block mover to move blocks
      localConf.setInt(PlacementMonitor.BLOCK_MOVE_QUEUE_LENGTH_KEY, 0);
      localConf.setInt(PlacementMonitor.NUM_MOVING_THREADS_KEY, 1);
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
          } else {
            assertTrue(p.getName().equals("policy3"));
            Path srcPath = new Path("/user/dhruba/raidtest3");
            assertTrue(p.getSrcPath().equals(
                srcPath.makeQualified(srcPath.getFileSystem(conf))));
          }
          if (p.getName().equals("policy3")) {
            assertTrue(p.getCodecId().equals("rs"));
            assertEquals(rstargetReplication,
                         Integer.parseInt(p.getProperty("targetReplication")));
            assertEquals(rsmetaReplication,
                         Integer.parseInt(p.getProperty("metaReplication")));
          } else {
            assertTrue(p.getCodecId().equals("xor"));
            assertEquals(targetReplication,
                         Integer.parseInt(p.getProperty("targetReplication")));
            assertEquals(metaReplication,
                         Integer.parseInt(p.getProperty("metaReplication")));
          }
      }

      long start = System.currentTimeMillis();
      final int MAX_WAITTIME = 300000;
      
      assertTrue("cnode is not DistRaidNode", cnode instanceof DistRaidNode);
      DistRaidNode dcnode = (DistRaidNode) cnode;

      while (dcnode.jobMonitor.jobsMonitored() < 3 &&
             System.currentTimeMillis() - start < MAX_WAITTIME) {
        Thread.sleep(1000);
      }

      start = System.currentTimeMillis();
      while (dcnode.jobMonitor.jobsSucceeded() < dcnode.jobMonitor.jobsMonitored() &&
             System.currentTimeMillis() - start < MAX_WAITTIME) {
        Thread.sleep(1000);
      }
      assertEquals(dcnode.jobMonitor.jobsSucceeded(), dcnode.jobMonitor.jobsMonitored());
      checkTestFiles("/user/dhruba/raidtest/", "/raid/user/dhruba/raidtest", 
          rsstripeLength, targetReplication, metaReplication, dcnode.placementMonitor,
          Codec.getCodec("xor"), 5);
      checkTestFiles("/user/dhruba/raidtest2/", "/raid/user/dhruba/raidtest2", 
          rsstripeLength, targetReplication, metaReplication, dcnode.placementMonitor,
          Codec.getCodec("xor"), 5);
      checkTestFiles("/user/dhruba/raidtest3/", "/raidrs/user/dhruba/raidtest3", 
          rsstripeLength, rstargetReplication, rsmetaReplication, dcnode.placementMonitor,
          Codec.getCodec("rs"), 1);

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
    cb.addPolicy("policy1", "/user/dhruba/raidtest", (short)1, targetReplication, metaReplication);
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
    long targetReplication = 2;
    long metaReplication   = 2;
    long stripeLength      = 3;
    short srcReplication = 3;

    createClusters(false, false);
    // don't allow rescan, make sure only one job is submitted.
    conf.setLong("raid.policy.rescan.interval", 60 * 1000L);
    ConfigBuilder cb = new ConfigBuilder(CONFIG_FILE);
    cb.addPolicy("abstractPolicy", (short)1, targetReplication, metaReplication);
    cb.addFileListPolicy("policy2", "/user/rvadali/raidfilelist.txt", "abstractPolicy");
    cb.persist();

    RaidNode cnode = null;
    Path fileListPath = new Path("/user/rvadali/raidfilelist.txt");
    try {
      createTestFiles(fileSys, "/user/rvadali/raidtest/",
          "/raid/user/rvadali/raidtest", 5, 7, srcReplication);
      LOG.info("Test testFileListPolicy created test files");

      // Create list of files to raid.
      FSDataOutputStream out = fileSys.create(fileListPath);
      FileStatus[] files = fileSys.listStatus(new Path("/user/rvadali/raidtest"));
      for (FileStatus f: files) {
        out.write(f.getPath().toString().getBytes());
        out.write("\n".getBytes());
      }
      out.close();

      cnode = RaidNode.createRaidNode(conf);
      final int MAX_WAITTIME = 300000;
      DistRaidNode dcnode = (DistRaidNode) cnode;

      long start = System.currentTimeMillis();
      int numJobsExpected = 1;
      while (dcnode.jobMonitor.jobsSucceeded() < numJobsExpected &&
             System.currentTimeMillis() - start < MAX_WAITTIME) {
        LOG.info("Waiting for num jobs succeeded " + dcnode.jobMonitor.jobsSucceeded() + 
         " to reach " + numJobsExpected);
        Thread.sleep(1000);
      }
      assertEquals(numJobsExpected, dcnode.jobMonitor.jobsMonitored());
      assertEquals(numJobsExpected, dcnode.jobMonitor.jobsSucceeded());

      FileStatus[] parityFiles = fileSys.listStatus(
        new Path("/raid/user/rvadali/raidtest"));
      assertEquals(files.length, parityFiles.length);
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
