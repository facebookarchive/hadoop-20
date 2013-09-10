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
import java.util.List;
import java.util.Random;
import junit.framework.TestCase;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.impl.Log4JLogger;
import org.apache.log4j.Level;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.mapred.MiniMRCluster;
import org.apache.hadoop.raid.protocol.PolicyInfo;
import org.apache.hadoop.hdfs.TestRaidDfs;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.raid.Utils.Builder;

/**
 * If a file gets deleted, then verify that the parity file gets deleted too.
 */
public class TestRaidPurge extends TestCase {
  final static String TEST_DIR = new File(System.getProperty("test.build.data",
      "build/contrib/raid/test/data")).getAbsolutePath();
  final static String CONFIG_FILE = new File(TEST_DIR, 
      "test-raid.xml").getAbsolutePath();
  final static long RELOAD_INTERVAL = 1000;
  final static Log LOG = LogFactory.getLog("org.apache.hadoop.raid.TestRaidPurge");
  final Random rand = new Random();

  {
    ((Log4JLogger)RaidNode.LOG).getLogger().setLevel(Level.ALL);
  }


  Configuration conf;
  String namenode = null;
  String hftp = null;
  MiniDFSCluster dfs = null;
  MiniMRCluster mr = null;
  FileSystem fileSys = null;
  String jobTrackerName = null;

  public void createClusters(boolean local, int numNodes) throws Exception {
    createClusters(local, numNodes, null, null);
  }
  
  /**
   * create mapreduce and dfs clusters
   */
  public void createClusters(boolean local, int numNodes, 
      String[] racks, String[] hosts) throws Exception {
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
    conf.set("mapred.raid.http.address", "localhost:0");
    conf.setLong("dfs.blockreport.intervalMsec", 1000L);
    // create a dfs and map-reduce cluster
    final int taskTrackers = numNodes;

    dfs = new MiniDFSCluster(conf, numNodes, true, racks, hosts);
    dfs.waitActive();
    fileSys = dfs.getFileSystem();
    TestDirectoryRaidDfs.setupStripeStore(conf, fileSys);
    
    namenode = fileSys.getUri().toString();
    mr = new MiniMRCluster(taskTrackers, namenode, numNodes);
    jobTrackerName = "localhost:" + mr.getJobTrackerPort();
    hftp = "hftp://localhost.localdomain:" + dfs.getNameNodePort();

    FileSystem.setDefaultUri(conf, namenode);
    conf.set("mapred.job.tracker", jobTrackerName);
    //Don't allow empty file to be raid
    conf.setLong(RaidNode.MINIMUM_RAIDABLE_FILESIZE_KEY, 1L);
  }
    
  public void mySetup(long targetReplication,
    long metaReplication) throws Exception {
    // Initialize Raid Policy config
    ConfigBuilder cb = new ConfigBuilder(CONFIG_FILE);
    cb.addPolicy("policy1", "/user/dhruba/raidtest", 
        targetReplication, metaReplication);
    cb.addPolicy("policy2", "/user/dhruba/dirraidtest", 
        targetReplication, metaReplication, "dir-xor");
    cb.addPolicy("policy3", "/user/dhruba/dirraidrstest", 
        targetReplication, metaReplication, "dir-rs");
    cb.persist();
  }

  /**
   * stop clusters created earlier
   */
  public void stopClusters() throws Exception {
    if (mr != null) { mr.shutdown(); }
    if (dfs != null) { dfs.shutdown(); }
  }

  /**
   * Test that parity files that do not have an associated master file
   * get deleted.
   */
  public void testPurge() throws Exception {
    LOG.info("Test testPurge  started.");

    long blockSizes    []  = {1024L};
    long stripeLengths []  = {5};
    long targetReplication = 1;
    long metaReplication   = 1;
    int  numBlock          = 9;
    int  iter = 0;

    createClusters(true, 3);
    try {
      for (long blockSize : blockSizes) {
        for (long stripeLength : stripeLengths) {
           doTestPurge(iter, targetReplication, metaReplication,
                       stripeLength, blockSize, numBlock);
           iter++;
        }
      }
    } finally {
      stopClusters();
    }
    LOG.info("Test testPurge completed.");
  }
  
  private void waitFilesDelete(List<Path> destPaths) throws IOException,
    InterruptedException {
    // wait till parity file and directory are automatically deleted
    for (Path destPath: destPaths) {
      while (fileSys.listStatus(destPath).length > 0) {
        LOG.info("doTestPurge waiting for parity files to be removed.");
        Thread.sleep(1000);                  // keep waiting
      }
    }
  }
  
  /**
   * Create parity file, delete original file and then validate that
   * parity file is automatically deleted.
   */
  private void doTestPurge(int iter, long targetReplication,
                          long metaReplication, long stripeLength,
                          long blockSize, int numBlock) throws Exception {
    LOG.info("doTestPurge started---------------------------:" +  " iter " + iter +
             " blockSize=" + blockSize + " stripeLength=" + stripeLength);
    mySetup(targetReplication, metaReplication);
    Utils.loadTestCodecs(conf, new Builder[] {
        Utils.getXORBuilder().setStripeLength(stripeLength),
        Utils.getXORBuilder().setStripeLength(stripeLength).dirRaid(
            true).setParityDir("/dir-raid").setCodeId("dir-xor")
    });
    
    Path dir = new Path("/user/dhruba/raidtest/");
    Path file1 = new Path(dir + "/file" + iter);
    Path dir1 = new Path("/user/dhruba/dirraidtest/" + iter);
    Path file2 = new Path(dir1 + "/file1");
    Path file3 = new Path(dir1 + "/file2");
    RaidNode cnode = null;
    try {
      List<Path> destPaths = new ArrayList<Path>();
      Path destPath1 = new Path("/raid/user/dhruba/raidtest");
      destPaths.add(destPath1);
      Path destPath2 = new Path("/dir-raid/user/dhruba/dirraidtest");
      destPaths.add(destPath2);
      fileSys.delete(dir, true);
      fileSys.delete(dir1, true);
      fileSys.delete(destPath1, true);
      fileSys.delete(destPath2, true);
      TestRaidNode.createOldFile(fileSys, file1, 1, numBlock, blockSize);
      TestRaidNode.createOldFile(fileSys, file2, 1, numBlock, blockSize);
      TestRaidNode.createOldFile(fileSys, file3, 1, numBlock, blockSize);
      LOG.info("doTestPurge created test files for iteration " + iter);

      // create an instance of the RaidNode
      Configuration localConf = new Configuration(conf);
      cnode = RaidNode.createRaidNode(null, localConf);
      
      TestRaidDfs.waitForFileRaided(LOG, fileSys, file1, destPath1);
      TestRaidDfs.waitForDirRaided(LOG, fileSys, dir1, destPath2);
      LOG.info("doTestPurge all files found in Raid.");

      // delete original file
      assertTrue("Unable to delete original file " + file1 ,
                 fileSys.delete(file1, true));
      LOG.info("deleted file " + file1);

      // delete original directory
      assertTrue("Unable to delete original directory " + dir1,
                 fileSys.delete(dir1, true));
      LOG.info("deleted directory " + dir1);
      waitFilesDelete(destPaths);
    } catch (Exception e) {
      LOG.info("doTestPurge Exception " + e +
                                          StringUtils.stringifyException(e));
      throw e;
    } finally {
      if (cnode != null) { cnode.stop(); cnode.join(); }
      LOG.info("doTestPurge delete file " + file1);
      fileSys.delete(file1, true);
      fileSys.delete(dir1, true);
    }
    LOG.info("doTestPurge completed:" + " blockSize=" + blockSize +
             " stripeLength=" + stripeLength);
  }

  /**
   * Create a file, wait for parity file to get HARed. Then modify the file,
   * wait for the HAR to get purged.
   */
  public void testPurgeHar() throws Exception {
    LOG.info("testPurgeHar started");
    createClusters(true, 3);
    mySetup(1, 1);
    Utils.loadTestCodecs(conf, new Builder[] {
        Utils.getXORBuilder().setStripeLength(5),
    });
    Path dir = new Path("/user/dhruba/raidtest/");
    Path destPath = new Path("/raid/user/dhruba/raidtest");
    Path file1 = new Path(dir + "/file");
    RaidNode cnode = null;
    try {
      TestRaidNode.createOldFile(fileSys, file1, 1, 8, 8192L);
      LOG.info("testPurgeHar created test files");

      // create an instance of the RaidNode
      Configuration localConf = new Configuration(conf);
      localConf.setInt(RaidNode.RAID_PARITY_HAR_THRESHOLD_DAYS_KEY, 0);
      cnode = RaidNode.createRaidNode(null, localConf);

      // Wait till har is created.
      while (true) {
        try {
          FileStatus[] listPaths = fileSys.listStatus(destPath);
          if (listPaths != null && listPaths.length == 1) {
            FileStatus s = listPaths[0];
            LOG.info("testPurgeHar found path " + s.getPath());
            if (s.getPath().toString().endsWith(".har")) {
              break;
            }
          }
        } catch (FileNotFoundException e) {
          //ignore
        }
        Thread.sleep(1000);                  // keep waiting
      }

      // Set an old timestamp.
      fileSys.setTimes(file1, 0, 0);

      boolean found = false;
      FileStatus[] listPaths = null;
      while (!found || listPaths == null || listPaths.length > 1) {
        listPaths = fileSys.listStatus(destPath);
        if (listPaths != null) {
          for (FileStatus s: listPaths) {
            LOG.info("testPurgeHar waiting for parity file to be recreated" +
              " and har to be deleted found " + s.getPath());
            if (s.getPath().toString().endsWith("file") &&
                s.getModificationTime() == 0) {
              found = true;
            }
          }
        }
        Thread.sleep(1000);
      }
    } catch (Exception e) {
      LOG.info("testPurgeHar Exception " + e +
          StringUtils.stringifyException(e));
      throw e;
    } finally {
      if (cnode != null) { cnode.stop(); cnode.join(); }
      fileSys.delete(dir, true);
      fileSys.delete(destPath, true);
      stopClusters();
    }
  }

  /**
   * Create parity file, delete original file's directory and then validate that
   * parity directory is automatically deleted.
   */
  public void testPurgeDirectory() throws Exception {
    long stripeLength = 5;
    long blockSize = 8192;
    long targetReplication = 1;
    long metaReplication   = 1;
    int  numBlock          = 9;

    createClusters(true, 3);
    mySetup(targetReplication, metaReplication);
    Utils.loadTestCodecs(conf, new Builder[] {
        Utils.getXORBuilder().setStripeLength(stripeLength),
        Utils.getXORBuilder().setStripeLength(stripeLength).dirRaid(
            true).setParityDir("/dir-raid").setCodeId("dir-xor")
    });
    Path dir = new Path("/user/dhruba/raidtest/");
    Path file1 = new Path(dir + "/file1");
    Path dir1 = new Path("/user/dhruba/dirraidtest/1");
    Path file2 = new Path(dir1 + "/file2");
    Path file3 = new Path(dir1 + "/file3");
    RaidNode cnode = null;
    try {
      List<Path> destPaths = new ArrayList<Path>();
      Path destPath1 = new Path("/raid/user/dhruba/raidtest");
      destPaths.add(destPath1);
      Path destPath2 = new Path("/dir-raid/user/dhruba/dirraidtest");
      destPaths.add(destPath2);
      TestRaidNode.createOldFile(fileSys, file1, 1, numBlock, blockSize);
      TestRaidNode.createOldFile(fileSys, file2, 1, numBlock, blockSize);
      TestRaidNode.createOldFile(fileSys, file3, 1, numBlock, blockSize);
      
      // create an instance of the RaidNode
      Configuration localConf = new Configuration(conf);
      cnode = RaidNode.createRaidNode(null, localConf);
      TestRaidDfs.waitForFileRaided(LOG, fileSys, file1, destPath1);
      TestRaidDfs.waitForDirRaided(LOG, fileSys, dir1, destPath2);

      // delete original directory.
      assertTrue("Unable to delete original directory " + file1 ,
                 fileSys.delete(file1.getParent(), true));
      LOG.info("deleted directory " + file1.getParent());
      
      // delete original directory
      assertTrue("Unable to delete original direcotry" + dir1, 
          fileSys.delete(dir1.getParent(), true));
      LOG.info("deleted directory " + dir1.getParent());

      // wait till parity file and directory are automatically deleted
      long start = System.currentTimeMillis();
      while ((fileSys.exists(destPath1) ||
              fileSys.exists(destPath2)) &&
            System.currentTimeMillis() - start < 120000) {
        LOG.info("testPurgeDirectory waiting for parity files to be removed.");
        Thread.sleep(1000);                  // keep waiting
      }
      assertFalse(fileSys.exists(destPath1));
      assertFalse(fileSys.exists(destPath2));

    } catch (Exception e) {
      LOG.info("testPurgeDirectory Exception " + e +
                                          StringUtils.stringifyException(e));
      throw e;
    } finally {
      if (cnode != null) { cnode.stop(); cnode.join(); }
      LOG.info("testPurgeDirectory delete file " + file1);
      fileSys.delete(file1, true);
      fileSys.delete(dir1, true);
      stopClusters();
    }
  }

  /**
   * Test that higher-pri codecs could purge lower-pri codecs 
   */
  public void testPurgePreference() throws Exception {
    LOG.info("Test testPurgePreference started");
    createClusters(true, 3);
    Utils.loadTestCodecs(conf, new Builder[] {
        Utils.getXORBuilder(), // priority 100
        Utils.getRSBuilder(),  // priority 300
        Utils.getDirXORBuilder(), // priority 400
        Utils.getDirRSBuilder(), // priority 600
        Utils.getRSBuilder().setParityDir("/test-raidrs").setCodeId(
            "testrs").simulatedBlockFixed(true)
    });
    mySetup(1, 1);
    Path dir = new Path("/user/test/raidtest");
    Path file1 = new Path(dir + "/file1");
    HashMap<String, PolicyInfo> infos = new HashMap<String, PolicyInfo>();
    for (Codec code: Codec.getCodecs()) {
      PolicyInfo pi = new PolicyInfo("testPurgePreference", conf);
      pi.setSrcPath("/user/test/raidtest");
      pi.setCodecId(code.id);
      pi.setDescription("test policy");
      pi.setProperty("targetReplication", "1");
      pi.setProperty("metaReplication", "1");
      infos.put(code.id, pi);
    }
    
    try {
      LOG.info("Create a old file");
      TestRaidNode.createOldFile(fileSys, file1, 1, 9, 8192L);
      FileStatus stat = fileSys.getFileStatus(file1);
      FileStatus dirStat = fileSys.getFileStatus(dir);
      HashMap<String, Path> parityFiles = new HashMap<String, Path>();
      // Create the parity files.
      LOG.info("Start Raiding");
      for (PolicyInfo pi: infos.values()){
        Codec code = Codec.getCodec(pi.getCodecId());
        FileStatus fsStat = (code.isDirRaid)? dirStat: stat;
        RaidNode.doRaid(
          conf, pi, fsStat, new RaidNode.Statistics(), Reporter.NULL);
        Path parity = RaidNode.getOriginalParityFile(new Path(code.parityDirectory),
              fsStat.getPath());
        assertTrue(fileSys.exists(parity));
        parityFiles.put(pi.getCodecId(), parity);
      }
      LOG.info("Finished Raiding");
      // Check purge of a single parity file.
      PurgeMonitor purgeMonitor = new PurgeMonitor(conf, null, null);
      LOG.info("Purge dir-rs");
      purgeMonitor.purgeCode(Codec.getCodec("dir-rs"));
      // Calling purge under the Dir-RS path has no effect.
      assertTrue(fileSys.exists(parityFiles.get("testrs")));
      assertTrue(fileSys.exists(parityFiles.get("dir-rs")));
      assertTrue(fileSys.exists(parityFiles.get("rs")));
      assertTrue(fileSys.exists(parityFiles.get("dir-xor")));
      assertTrue(fileSys.exists(parityFiles.get("xor")));
      
      LOG.info("Purge rs");
      purgeMonitor.purgeCode(Codec.getCodec("rs"));
      // Calling purge under the rs path will delete rs
      assertTrue(fileSys.exists(parityFiles.get("testrs")));
      assertTrue(fileSys.exists(parityFiles.get("dir-rs")));
      assertFalse(fileSys.exists(parityFiles.get("rs")));
      assertTrue(fileSys.exists(parityFiles.get("dir-xor")));
      assertTrue(fileSys.exists(parityFiles.get("xor")));
      
      LOG.info("Purge dir-xor");
      purgeMonitor.purgeCode(Codec.getCodec("dir-xor"));
      // Calling purge under the Dir-xor path will delete dir-xor
      assertTrue(fileSys.exists(parityFiles.get("testrs")));
      assertTrue(fileSys.exists(parityFiles.get("dir-rs")));
      assertFalse(fileSys.exists(parityFiles.get("dir-xor")));
      assertTrue(fileSys.exists(parityFiles.get("xor")));
      
      LOG.info("Purge xor");
      purgeMonitor.purgeCode(Codec.getCodec("xor"));
      assertFalse(fileSys.exists(parityFiles.get("xor")));
      assertTrue(fileSys.exists(parityFiles.get("testrs")));
      assertTrue(fileSys.exists(parityFiles.get("dir-rs")));
      
      LOG.info("delete dir-rs parity file");
      fileSys.delete(parityFiles.get("dir-rs"), true);
      assertFalse(fileSys.exists(parityFiles.get("dir-rs")));
      
      //Recreate RS and Dir-XOR
      LOG.info("Raid rs");
      RaidNode.doRaid(
          conf, infos.get("rs"), stat, new RaidNode.Statistics(),
          Reporter.NULL);
      assertTrue(fileSys.exists(parityFiles.get("rs")));
      LOG.info("Raid dir-xor");
      RaidNode.doRaid(
          conf, infos.get("dir-xor"), dirStat, new RaidNode.Statistics(),
          Reporter.NULL);
      assertTrue(fileSys.exists(parityFiles.get("dir-xor")));
      LOG.info("Raid xor");
      RaidNode.doRaid(
          conf, infos.get("xor"), stat, new RaidNode.Statistics(),
          Reporter.NULL);
      assertTrue(fileSys.exists(parityFiles.get("xor")));
      
      LOG.info("Purge dir-xor");
      purgeMonitor.purgeCode(Codec.getCodec("dir-xor"));
      // Calling purge under the Dir-XOR path succeeds
      assertTrue(fileSys.exists(parityFiles.get("testrs")));
      assertTrue(fileSys.exists(parityFiles.get("rs")));
      assertTrue(fileSys.exists(parityFiles.get("dir-xor")));
      assertTrue(fileSys.exists(parityFiles.get("xor")));
      
      LOG.info("Purge rs");
      purgeMonitor.purgeCode(Codec.getCodec("rs"));
      // Calling purge under the Dir-XOR path succeeds
      assertTrue(fileSys.exists(parityFiles.get("testrs")));
      assertFalse(fileSys.exists(parityFiles.get("rs")));
      assertTrue(fileSys.exists(parityFiles.get("dir-xor")));
      assertTrue(fileSys.exists(parityFiles.get("xor")));
      
      LOG.info("Purge testrs");
      purgeMonitor.purgeCode(Codec.getCodec("testrs"));
      // Calling purge under the Dir-XOR path succeeds
      assertFalse(fileSys.exists(parityFiles.get("testrs")));
      assertTrue(fileSys.exists(parityFiles.get("dir-xor")));
      assertTrue(fileSys.exists(parityFiles.get("xor")));
      
      LOG.info("delete dir-xor parity file");
      fileSys.delete(parityFiles.get("dir-xor"), true);
      assertFalse(fileSys.exists(parityFiles.get("dir-xor")));
      
      LOG.info("Raid rs");
      RaidNode.doRaid(
          conf, infos.get("rs"), stat, new RaidNode.Statistics(),
          Reporter.NULL);
      assertTrue(fileSys.exists(parityFiles.get("rs")));
      
      LOG.info("Purge xor");
      purgeMonitor.purgeCode(Codec.getCodec("xor"));
      assertTrue(fileSys.exists(parityFiles.get("rs")));
      assertFalse(fileSys.exists(parityFiles.get("xor")));
      
      LOG.info("delete rs");
      fileSys.delete(parityFiles.get("rs"), true);
      assertFalse(fileSys.exists(parityFiles.get("testrs")));
      LOG.info("Raid testrs");
      RaidNode.doRaid(
          conf, infos.get("testrs"), stat, new RaidNode.Statistics(),
          Reporter.NULL);
      assertTrue(fileSys.exists(parityFiles.get("testrs")));
      LOG.info("Raid xor");
      RaidNode.doRaid(
          conf, infos.get("xor"), stat, new RaidNode.Statistics(),
          Reporter.NULL);
      assertTrue(fileSys.exists(parityFiles.get("xor")));
      LOG.info("Purge xor");
      purgeMonitor.purgeCode(Codec.getCodec("xor"));
      assertTrue(fileSys.exists(parityFiles.get("testrs")));
      assertFalse(fileSys.exists(parityFiles.get("xor")));
      LOG.info("delete testrs");
      fileSys.delete(parityFiles.get("testrs"), true);
      
      // The following is har related stuff
       
      Path rsParity = parityFiles.get("rs");
      Path xorParity = parityFiles.get("xor");
      PolicyInfo infoXor = infos.get("xor");
      PolicyInfo infoRs = infos.get("rs");
      // Now check the purge of a parity har.
      // Delete the RS parity for now.
      fileSys.delete(rsParity, true);
      // Recreate the XOR parity.
      Path xorHar = new Path("/raid", "user/test/raidtest/raidtest" +
          RaidNode.HAR_SUFFIX);
      RaidNode.doRaid(
        conf, infoXor, stat, new RaidNode.Statistics(), Reporter.NULL);
      assertTrue(fileSys.exists(xorParity));
      assertFalse(fileSys.exists(xorHar));

      // Create the har.
      long cutoff = System.currentTimeMillis();
      // create an instance of the RaidNode
      Configuration localConf = new Configuration(conf);
      RaidNode cnode = RaidNode.createRaidNode(localConf);
      FileStatus raidStat =
         fileSys.getFileStatus(new Path("/raid"));
      cnode.recurseHar(Codec.getCodec("xor"), fileSys, raidStat,
        "/raid", fileSys, cutoff,
        Codec.getCodec(infoXor.getCodecId()).tmpHarDirectory);

      // Call purge to get rid of the parity file. The har should remain.
      purgeMonitor.purgeCode(Codec.getCodec("xor"));
      // XOR har should exist but xor parity file should have been purged.
      assertFalse(fileSys.exists(xorParity));
      assertTrue(fileSys.exists(xorHar));

      // Now create the RS parity.
      RaidNode.doRaid(
        conf, infoRs, stat, new RaidNode.Statistics(), Reporter.NULL);
      purgeMonitor.purgeCode(Codec.getCodec("xor"));
      // XOR har should get deleted.
      assertTrue(fileSys.exists(rsParity));
      assertFalse(fileSys.exists(xorParity));
      assertFalse(fileSys.exists(xorHar));
      LOG.info("Test testPurgePreference completed");
    } finally {
      stopClusters();
    }
  }
}
