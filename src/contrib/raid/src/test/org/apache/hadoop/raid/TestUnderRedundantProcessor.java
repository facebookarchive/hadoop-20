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
import java.util.HashMap;
import java.util.Random;

import junit.framework.TestCase;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.TestRaidDfs;
import org.apache.hadoop.mapred.MiniMRCluster;
import org.apache.hadoop.raid.RaidNode.UnderRedundantFilesProcessor;

public class TestUnderRedundantProcessor extends TestCase {
  public TestUnderRedundantProcessor(String name) {
    super(name);
    // TODO Auto-generated constructor stub
  }

  final static String TEST_DIR = new File(System.getProperty("test.build.data",
      "build/contrib/raid/test/data")).getAbsolutePath();
  final static String CONFIG_FILE = new File(TEST_DIR, 
      "test-raid.xml").getAbsolutePath();
  final static long RELOAD_INTERVAL = 1000;
  final static Log LOG = LogFactory.getLog(TestUnderRedundantProcessor.class);
  final Random rand = new Random();
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
    ConfigBuilder cb = new ConfigBuilder(CONFIG_FILE);
    cb.persist();
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
    conf.set(RaidNode.RAID_UNDER_REDUNDANT_FILES, "/under_redundant.txt");
    conf.setLong(RaidNode.MINIMUM_RAIDABLE_FILESIZE_KEY, 1L);
    conf.setLong(UnderRedundantFilesProcessor.INCREASE_REPLICATION_BATCH_SIZE_KEY,
        2);
    conf.setLong(RaidNode.UNDER_REDUNDANT_FILES_PROCESSOR_SLEEP_TIME_KEY, 1000);
    conf.set(BlockIntegrityMonitor.RAIDNODE_CORRUPT_FILE_COUNTER_DIRECTORIES_KEY,
        "/0,/1");
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
  public void testUnderRedundantProcessor() throws Exception {
    LOG.info("Test TestUnderRedundantProcessor started.");
    createClusters(true, 3);
    RaidNode cnode = null;
    try {
      Path fileListPath = new Path("/under_redundant.txt");
      FSDataOutputStream out = fileSys.create(fileListPath);
      // Create 
      HashMap<Path, Integer> expectRepl = new HashMap<Path, Integer>();
      for (int i = 0; i < 3; i++) {
        for (int repl = 1; repl < 5; repl++) {
          String fileName = "/" + i + "/file" + repl; 
          Path newFile = new Path(fileName);
          expectRepl.put(newFile, repl > 2? repl: 3);
          TestRaidDfs.createTestFile(fileSys, newFile, repl, 1, 4096L);
          out.write((fileName + "\n").getBytes());
        }
        out.write(("/" + i + "/nonexist\n").getBytes());
      }
      out.close();
      LOG.info("Files created");
      // create an instance of the RaidNode
      Configuration localConf = new Configuration(conf);
      cnode = RaidNode.createRaidNode(null, localConf);
      long startTime = System.currentTimeMillis();
      boolean good = false;
      while (System.currentTimeMillis() - startTime < 120000 && !good) {
        good = true;
        for (Path file : expectRepl.keySet()){
          FileStatus stat = fileSys.getFileStatus(file);
          if (stat.getReplication() != expectRepl.get(file)) {
            good = false;
            break;
          }
        }
        Thread.sleep(1000);
      }
      for (Path file : expectRepl.keySet()){
        FileStatus stat = fileSys.getFileStatus(file);
        assertEquals("file " + file + " should has " + expectRepl.get(file) +
            " repl but get " + stat.getReplication(), expectRepl.get(file),
            new Integer(stat.getReplication()));
      }
      assertEquals("failed files should be 0", 0, 
          cnode.getNumUnderRedundantFilesFailedIncreaseReplication());
      assertEquals("succeeded files should be 6", 6, 
          cnode.getNumUnderRedundantFilesSucceededIncreaseReplication());
      RaidNodeMetrics rnm = RaidNodeMetrics.getInstance(
          RaidNodeMetrics.DEFAULT_NAMESPACE_ID);
      assertEquals(5, rnm.underRedundantFiles.get("/0").get());
      assertEquals(5, rnm.underRedundantFiles.get("/1").get());
      assertEquals(5, rnm.underRedundantFiles.get(BlockIntegrityMonitor.OTHERS).get());
      Thread.sleep(3000);
      assertEquals(5, rnm.underRedundantFiles.get("/0").get());
      assertEquals(5, rnm.underRedundantFiles.get("/1").get());
      assertEquals(5, rnm.underRedundantFiles.get(BlockIntegrityMonitor.OTHERS).get());
      out = fileSys.create(fileListPath);
      for (int i = 1; i < 3; i ++) {
        out.write(("/" + i + "/nonexist\n").getBytes());
      }
      out.close();
      Thread.sleep(3000);
      assertEquals(0, rnm.underRedundantFiles.get("/0").get());
      assertEquals(1, rnm.underRedundantFiles.get("/1").get());
      assertEquals(1, rnm.underRedundantFiles.get(BlockIntegrityMonitor.OTHERS).get());
    } catch (Exception e) {
      LOG.info("TestUnderRedundantProcessor Exception ", e);
      throw e; 
    } finally {
      if (cnode != null) { cnode.stop(); cnode.join(); }
      stopClusters();
    }
    LOG.info("Test TestUnderRedundantProcessor completed.");
  }
}
