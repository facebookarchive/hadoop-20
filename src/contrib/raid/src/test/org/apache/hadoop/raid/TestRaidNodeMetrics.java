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
import java.io.FileWriter;
import java.io.IOException;
import java.util.Random;

import junit.framework.Assert;
import junit.framework.TestCase;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.TestRaidDfs;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.server.namenode.BlockPlacementPolicyRaid;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.mapred.MiniMRCluster;
import org.junit.After;

public class TestRaidNodeMetrics extends TestCase {
  final static String TEST_DIR = 
      new File(System.
               getProperty("test.build.data", "build/contrib/raid/test/data")).
      getAbsolutePath();
    final static String CONFIG_FILE = new File(TEST_DIR, "test-raid.xml").
      getAbsolutePath();
    final static long RELOAD_INTERVAL = 1000;
    final static int NUM_DATANODES = 6;
    final static int STRIPE_BLOCKS = 5; // number of file blocks per stripe
    final static int PARITY_BLOCKS = 3; // number of file blocks per stripe for RS

    final static int FILE_BLOCKS = 6; // number of blocks that file consists of
    final static short REPL = 1; // replication factor before raiding
    final static long BLOCK_SIZE = 8192L; // size of block in byte
    final static String DIR_PATH = "/user/rashmikv/raidtest";
    final static Path FILE_PATH0 =
      new Path("/user/rashmikv/raidtest/file0.test");
    final static String MONITOR_DIRS = "/";
    /*to test RS:
     * Confirm that codec is set to "rs" in the setUp function
     */
    final static Path RAID_PATH = new Path("/raidrs/user/rashmikv/raidtest");
    final static String RAID_DIR = "/raidrs";
    final static String CODE_USED ="rs";
    
    String[] racks = {"/rack1", "/rack2", "/rack3", "/rack4", "/rack5", "/rack6"};
    String[] hosts= {"host1.rack1.com", "host2.rack2.com", "host3.rack3.com", 
        "host4.rack4.com", "host5.rack5.com", "host6.rack6.com"};
    final int taskTrackers = 4;

       
    
    Configuration conf = null;
    Configuration raidConf = null;
    Configuration clientConf = null;
    MiniDFSCluster cluster = null;
    DistributedFileSystem dfs = null;
    RaidNode rnode = null; 
    MiniMRCluster mr = null;
    String jobTrackerName = null;
    String hftp = null;
    Codec codec;


  final static Log LOG = LogFactory.getLog(
      "org.apache.hadoop.raid.TestRaidNodeMetrics");
  
  private void setUp(boolean doHar) throws IOException, ClassNotFoundException {
    LOG.info("Setting up");
    new File(TEST_DIR).mkdirs(); // Make sure data directory exists
    conf = new Configuration();
    Utils.loadTestCodecs(conf, STRIPE_BLOCKS, 1, PARITY_BLOCKS, "/raid", "/raidrs");
    codec = Codec.getCodec("rs");
    conf.set("raid.config.file", CONFIG_FILE);
    conf.setBoolean("raid.config.reload", true);
    conf.setLong("raid.config.reload.interval", RELOAD_INTERVAL);

    // scan all policies once every 5 second
    conf.setLong("raid.policy.rescan.interval", 5000);

    // do not use map-reduce cluster for Raiding
    conf.set("raid.classname", "org.apache.hadoop.raid.LocalRaidNode");

    conf.set("raid.blockfix.classname", 
        "org.apache.hadoop.raid.DistBlockIntegrityMonitor");

    conf.set("raid.server.address", "localhost:" + MiniDFSCluster.getFreePort());
    conf.set("mapred.raid.http.address", "localhost:0");

    conf.setInt("dfs.corruptfilesreturned.max", 500);

    conf.setBoolean("dfs.permissions", false);
    conf.set("raid.corruptfile.counter.dirs", MONITOR_DIRS);
    conf.setInt("raid.corruptfilecount.interval", 1000);

    cluster = new MiniDFSCluster(conf, 6, true, racks, hosts);
    cluster.waitActive();

    dfs = (DistributedFileSystem) cluster.getFileSystem();
    String namenode = dfs.getUri().toString();

    mr = new MiniMRCluster(taskTrackers, namenode, 6);
    jobTrackerName = "localhost:" + mr.getJobTrackerPort();
    hftp = "hftp://localhost.localdomain:" + cluster.getNameNodePort();
    FileSystem.setDefaultUri(conf, namenode);
    conf.set("mapred.job.tracker", jobTrackerName);
    
    FileWriter fileWriter = new FileWriter(CONFIG_FILE);
    fileWriter.write("<?xml version=\"1.0\"?>\n");
    //Change the codec name: rs of xor here:
    String str =
      "<configuration> " +
      "    <policy name = \"RaidTest1\"> " +
      "      <srcPath prefix=\"" + DIR_PATH + "\"/> " +
      "      <codecId>rs</codecId> " +
      "      <destPath> " + RAID_DIR + " </destPath> " +
      "      <property> " +
      "        <name>targetReplication</name> " +
      "        <value>1</value> " +
      "        <description>after RAIDing, decrease the replication " +
      "factor of a file to this value.</description> " +
      "      </property> " +
      "      <property> " +
      "        <name>metaReplication</name> " +
      "        <value>1</value> " +
      "        <description> replication factor of parity file</description> " +
      "      </property> " +
      "      <property> " +
      "        <name>modTimePeriod</name> " +
      "        <value>2000</value> " +
      "        <description>time (milliseconds) after a file is modified " + 
      "to make it a candidate for RAIDing</description> " +
      "      </property> ";
   
    str +=
      "    </policy>" +
      "</configuration>";

    fileWriter.write(str);
    fileWriter.close();

    TestRaidDfs.createTestFile(dfs, FILE_PATH0, REPL, FILE_BLOCKS, BLOCK_SIZE);
       
    //Path[] filePaths = { FILE_PATH0 };
    
    RaidNode.doRaid(conf, dfs.getFileStatus(FILE_PATH0),
        new Path(RAID_DIR), codec, new RaidNode.Statistics(), RaidUtils.NULL_PROGRESSABLE,
        false, REPL, REPL);
    try {
      TestRaidDfs.waitForFileRaided(LOG, dfs, FILE_PATH0, RAID_PATH);
    } catch (FileNotFoundException ignore) { 
    } catch (InterruptedException ignore) {
    }
 
     //raidTestFiles(RAID_PATH, filePaths, doHar);

    clientConf = new Configuration(conf);
    clientConf.set("fs.hdfs.impl",
                   "org.apache.hadoop.hdfs.DistributedRaidFileSystem");
    clientConf.set("fs.raid.underlyingfs.impl",
                   "org.apache.hadoop.hdfs.DistributedFileSystem"); 
    
  }
  
  /**
   * removes a file block in the specified stripe
   */
  private void removeFileBlock(Path filePath, int stripe, int blockInStripe)
    throws IOException {
    LocatedBlocks fileBlocks = dfs.getClient().namenode.
      getBlockLocations(filePath.toString(), 0, FILE_BLOCKS * BLOCK_SIZE);
    if (fileBlocks.locatedBlockCount() != FILE_BLOCKS) {
      throw new IOException("expected " + FILE_BLOCKS + 
                            " file blocks but found " + 
                            fileBlocks.locatedBlockCount());
    }
    if (blockInStripe >= STRIPE_BLOCKS) {
      throw new IOException("blockInStripe is " + blockInStripe +
                            " but must be smaller than " + STRIPE_BLOCKS);
    }
    LocatedBlock block = fileBlocks.get(stripe * STRIPE_BLOCKS + blockInStripe);
    removeAndReportBlock(dfs, filePath, block);
    LOG.info("removed file " + filePath.toString() + " block " +
             stripe * STRIPE_BLOCKS + " in stripe " + stripe);
  }

  /**
   * removes a specified block from MiniDFS storage and reports it as corrupt
   */
  private void removeAndReportBlock(DistributedFileSystem blockDfs,
                                    Path filePath,
                                    LocatedBlock block) 
    throws IOException {
    TestRaidDfs.corruptBlock(filePath, block.getBlock(), 
        NUM_DATANODES, true, cluster);
   
    // report deleted block to the name node
    LocatedBlock[] toReport = { block };
    blockDfs.getClient().namenode.reportBadBlocks(toReport);

  }
  /**
   * sleeps for up to 20s until the number of corrupt files 
   * in the file system is equal to the number specified
   */
  private void waitUntilCorruptFileCount(DistributedFileSystem dfs,
                                         int corruptFiles)
    throws IOException {
    int initialCorruptFiles = DFSUtil.getCorruptFiles(dfs).length;
    long waitStart = System.currentTimeMillis();
    while (DFSUtil.getCorruptFiles(dfs).length != corruptFiles) {
      try {
        Thread.sleep(1000);
      } catch (InterruptedException ignore) {
        
      }

      if (System.currentTimeMillis() > waitStart + 20000L) {
        break;
      }
    }
    
    long waited = System.currentTimeMillis() - waitStart;

    int corruptFilesFound = DFSUtil.getCorruptFiles(dfs).length;
    if (corruptFilesFound != corruptFiles) {
      throw new IOException("expected " + corruptFiles + 
                            " corrupt files but got " +
                            corruptFilesFound);
    }
  }
  
  @After
  public void tearDown() throws Exception {
    if (rnode != null) {
      rnode.stop();
      rnode.join();
      rnode = null;
    }

    if (cluster != null) {
      cluster.shutdown();
      cluster = null;
    }
    
    dfs = null;

    LOG.info("Test cluster shut down");
  }
  public void testRaidNodeMetrics() {
    LOG.info("testRaidNodeMetrics starting");
    RaidNodeMetrics inst = RaidNodeMetrics.getInstance(RaidNodeMetrics.DEFAULT_NAMESPACE_ID);
    inst.filesFixed.inc();
    inst.filesRaided.set(2);
    inst.raidFailures.inc(3);
    inst.doUpdates(inst.context);
    LOG.info("testRaidNodeMetrics succeeded");
  }
  
  public void testRaidNodeMetricsBytesTransferred() throws Exception{
    LOG.info("testRaidNodeMetricsBytesTransferred starting");
    setUp(false);
    
    try {
      waitUntilCorruptFileCount(dfs, 0);
      removeFileBlock(FILE_PATH0, 0, 0);
      waitUntilCorruptFileCount(dfs, 1);
      
      Configuration localConf = new Configuration(conf);
      localConf.setInt("raid.blockfix.interval", 1000);
      localConf.setInt("raid.blockcheck.interval", 1000);
      localConf.set("raid.blockfix.classname", 
                    "org.apache.hadoop.raid.DistBlockIntegrityMonitor");
   
      localConf.setLong("raid.blockfix.filespertask", 2L);
      localConf.setLong("raid.blockfix.maxpendingjobs", 2L);
      rnode = RaidNode.createRaidNode(null, localConf);
      long start = System.currentTimeMillis();
      while (rnode.blockIntegrityMonitor.getNumFilesFixed() < 1 &&
             System.currentTimeMillis() - start < 120000) {
        LOG.info("Test testBlockFix waiting for files to be fixed.");
        Thread.sleep(1000);
      }
      assertTrue("file not fixed", 1 <= rnode.blockIntegrityMonitor.getNumFilesFixed());
      LOG.info("Checking Raid Node Metrics") ;
      RaidNodeMetrics inst = RaidNodeMetrics.getInstance(RaidNodeMetrics.DEFAULT_NAMESPACE_ID);
      LOG.info("Number of bytes transferred across rack for repair in the current interval is : "+inst.numFileFixReadBytesRemoteRack.getCurrentIntervalValue());
      LOG.info("Number of bytes transferred across rack for repair in the previous interval is : "+inst.numFileFixReadBytesRemoteRack.getPreviousIntervalValue());
    } catch (Exception e) {
      LOG.info("Test TestSimulationBlockFixer Exception " + e, e);
      throw e;
  } finally {
    tearDown();
  } 
  }
}
   

