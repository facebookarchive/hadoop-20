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

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.URI;
import java.util.Random;
import java.util.UUID;
import java.util.zip.CRC32;

import junit.framework.TestCase;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.TestRaidDfs;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.mapred.MiniMRCluster;
import org.apache.hadoop.raid.RaidNode;
import org.apache.hadoop.raid.tools.FastFileCheck;


public class TestRaidShell extends TestCase {
  final static Log LOG = LogFactory.getLog(
                            "org.apache.hadoop.raid.TestRaidShell");
  final static String TEST_DIR = new File(System.getProperty("test.build.data",
      "build/contrib/raid/test/data")).getAbsolutePath();
  final static String CONFIG_FILE = new File(TEST_DIR,
      "test-raid.xml").getAbsolutePath();
  final static private String RAID_SRC_PATH = "/user/raidtest";
  final static private String RAID_POLICY_NAME = "RaidTest1";
  final static long RELOAD_INTERVAL = 1000;
  final static int NUM_DATANODES = 3;
  Configuration conf;
  String namenode = null;
  MiniDFSCluster dfsCluster = null;
  String hftp = null;
  FileSystem fileSys = null;
  RaidNode cnode = null;
  Random rand = new Random();
  
  private void doRaid(Path srcPath, Codec codec) throws IOException {
    RaidNode.doRaid(conf, fileSys.getFileStatus(srcPath),
              new Path("/raid"), codec, 
              new RaidNode.Statistics(), 
                RaidUtils.NULL_PROGRESSABLE,
                false, 1, 1);
  }
  
  public void testFileCheck() throws Exception {
    LOG.info("Test FileCheck started.");
    
    mySetup(3, -1);
    File fileList = null;
    try {
      MiniMRCluster mr = new MiniMRCluster(4, namenode, 3);
      String jobTrackerName = "localhost:" + mr.getJobTrackerPort();
      conf.set("mapred.job.tracker", jobTrackerName);
      
      Path srcPath = new Path("/user/dikang/raidtest/file0");
      TestRaidDfs.createTestFilePartialLastBlock(fileSys, srcPath, 
          1, 8, 8192L);
      Codec codec = Codec.getCodec("xor");
      doRaid(srcPath, codec);
      FileStatus stat = fileSys.getFileStatus(srcPath);
      ParityFilePair pfPair = ParityFilePair.getParityFile(codec, stat, conf);
      assertNotNull(pfPair);
      
      // write the filelist
      fileList = new File(TEST_DIR + "/" + UUID.randomUUID().toString());
      BufferedWriter writer = new BufferedWriter(new FileWriter(fileList));
      writer.write(fileList.getPath() + "\n");
      writer.close();
      
      // Create RaidShell
      RaidShell shell = new RaidShell(conf);
      String[] args = new String[4];
      args[0] = "-fileCheck";
      args[1] = "-filesPerJob";
      args[2] = "1";
      args[3] = fileList.getPath();
      
      assertEquals(0, ToolRunner.run(shell, args));
      
      // test check source only
      // delete the parity file
      fileSys.delete(pfPair.getPath());
      args = new String[5];
      args[0] = "-fileCheck";
      args[1] = "-filesPerJob";
      args[2] = "1";
      args[3] = "-sourceOnly";
      args[4] = fileList.getPath();
      assertEquals(0, ToolRunner.run(shell, args));
      
    } finally {
      if (null != fileList) {
        fileList.delete();
      }
      myTearDown();
    }
  }

  /**
   * Test distRaid command
   * @throws Exception
   */
  public void testDistRaid() throws Exception {
    LOG.info("TestDist started.");
    // create a dfs and map-reduce cluster
    mySetup(3, -1);
    MiniMRCluster mr = new MiniMRCluster(4, namenode, 3);
    String jobTrackerName = "localhost:" + mr.getJobTrackerPort();
    conf.set("mapred.job.tracker", jobTrackerName);

    try {
      // Create files to be raided
      TestRaidNode.createTestFiles(fileSys, RAID_SRC_PATH,
          "/raid" + RAID_SRC_PATH, 1, 3, (short)3);
      String subDir = RAID_SRC_PATH + "/subdir";
      TestRaidNode.createTestFiles(
          fileSys, subDir, "/raid" + subDir, 1, 3, (short)3);
      
      // Create RaidShell and raid the files.
      RaidShell shell = new RaidShell(conf);
      String[] args = new String[3];
      args[0] = "-distRaid";
      args[1] = RAID_POLICY_NAME;
      args[2] = RAID_SRC_PATH;
      assertEquals(0, ToolRunner.run(shell, args));

      // Check files are raided
      checkIfFileRaided(new Path(RAID_SRC_PATH, "file0"));
      checkIfFileRaided(new Path(subDir, "file0"));
    } finally {
      mr.shutdown();
      myTearDown();
    }
  }
  
  // check if a file has been raided
  private void checkIfFileRaided(Path srcPath) throws IOException {
    FileStatus srcStat = fileSys.getFileStatus(srcPath);
    assertEquals(1, srcStat.getReplication());
    
    Path parityPath = new Path("/raid", srcPath);
    FileStatus parityStat = fileSys.getFileStatus(parityPath);
    assertEquals(1, parityStat.getReplication());
  }
  
  /**
   * Create a file with three stripes, corrupt a block each in two stripes,
   * and wait for the the file to be fixed.
   */
  public void testBlockFix() throws Exception {
    LOG.info("Test testBlockFix started.");
    long blockSize = 8192L;
    int stripeLength = 3;
    mySetup(stripeLength, -1);
    Path file1 = new Path(RAID_SRC_PATH, "file1");
    Path destPath = new Path("/raid"+RAID_SRC_PATH);
    long crc1 = TestRaidDfs.createTestFilePartialLastBlock(fileSys, file1,
                                                          1, 7, blockSize);
    long file1Len = fileSys.getFileStatus(file1).getLen();
    LOG.info("Test testBlockFix created test files");

    // create an instance of the RaidNode
    Configuration localConf = new Configuration(conf);
    localConf.setInt("raid.blockfix.interval", 1000);
    localConf.set("raid.blockfix.classname",
                  "org.apache.hadoop.raid.LocalBlockIntegrityMonitor");
    // the RaidNode does the raiding inline (instead of submitting to map/reduce)
    conf.set("raid.classname", "org.apache.hadoop.raid.LocalRaidNode");
    // use local block fixer
    conf.set("raid.blockfix.classname", 
             "org.apache.hadoop.raid.LocalBlockIntegrityMonitor");

    cnode = RaidNode.createRaidNode(null, localConf);

    try {
      TestRaidDfs.waitForFileRaided(LOG, fileSys, file1, destPath);
      cnode.stop();
      cnode.join();
      cnode = null;

      FileStatus srcStat = fileSys.getFileStatus(file1);
      LocatedBlocks locations = getBlockLocations(file1, srcStat.getLen());

      DistributedFileSystem dfs = (DistributedFileSystem)fileSys;
      ClientProtocol namenode = dfs.getClient().namenode;

      String[] corruptFiles = DFSUtil.getCorruptFiles(dfs);
      assertEquals(corruptFiles.length, 0);

      // Corrupt blocks in two different stripes. We can fix them.
      TestRaidDfs.corruptBlock(file1, locations.get(0).getBlock(),
               NUM_DATANODES, true, dfsCluster); // delete block
      TestRaidDfs.corruptBlock(file1, locations.get(4).getBlock(),
               NUM_DATANODES, false, dfsCluster); // corrupt block
      TestRaidDfs.corruptBlock(file1, locations.get(6).getBlock(),
               NUM_DATANODES, true, dfsCluster); // delete last (partial) block
      LocatedBlock[] toReport = new LocatedBlock[3];
      toReport[0] = locations.get(0);
      toReport[1] = locations.get(4);
      toReport[2] = locations.get(6);
      namenode.reportBadBlocks(toReport);

      corruptFiles = DFSUtil.getCorruptFiles(dfs);
      assertEquals(corruptFiles.length, 1);
      assertEquals(corruptFiles[0], file1.toString());

      // Create RaidShell and fix the file.
      RaidShell shell = new RaidShell(conf);
      String[] args = new String[2];
      args[0] = "-recoverBlocks";
      args[1] = file1.toUri().getPath();
      assertEquals(0, ToolRunner.run(shell, args));

      long start = System.currentTimeMillis();
      do {
        LOG.info("Test testBlockFix waiting for files to be fixed.");
        Thread.sleep(1000);
        corruptFiles = DFSUtil.getCorruptFiles(dfs);
      } while (corruptFiles.length != 0 &&
             System.currentTimeMillis() - start < 120000);

      assertEquals(0, corruptFiles.length);

      dfs = getDFS(conf, dfs);
      assertTrue(TestRaidDfs.validateFile(dfs, file1, file1Len, crc1));

    } catch (Exception e) {
      LOG.info("Test testBlockFix Exception " + e + StringUtils.stringifyException(e));
      throw e;
    } finally {
      myTearDown();
    }
    LOG.info("Test testBlockFix completed.");
  }

  private static DistributedFileSystem getDFS(
        Configuration conf, FileSystem dfs) throws IOException {
    Configuration clientConf = new Configuration(conf);
    clientConf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
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
    conf.set("raid.server.address", "localhost:" + MiniDFSCluster.getFreePort());

    conf.setBoolean("dfs.permissions", false);

    dfsCluster = new MiniDFSCluster(conf, NUM_DATANODES, true, null);
    dfsCluster.waitActive();
    fileSys = dfsCluster.getFileSystem();
    namenode = fileSys.getUri().toString();

    FileSystem.setDefaultUri(conf, namenode);
    hftp = "hftp://localhost.localdomain:" + dfsCluster.getNameNodePort();

    FileSystem.setDefaultUri(conf, namenode);

    FileWriter fileWriter = new FileWriter(CONFIG_FILE);
    fileWriter.write("<?xml version=\"1.0\"?>\n");
    String str = "<configuration> " +
                   "<policy name = \"" + RAID_POLICY_NAME + "\"> " +
                        "<srcPath prefix=\"" + RAID_SRC_PATH + "\"/> " +
                        "<codecId>xor</codecId> " +
                        "<destPath> /raid</destPath> " +
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
                          "<value>0</value> " +
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

    Utils.loadTestCodecs(conf, stripeLength, 1, 3, "/raid", "/raidrs");
  }

  private void myTearDown() throws Exception {
    if (cnode != null) { cnode.stop(); cnode.join(); }
    if (dfsCluster != null) { dfsCluster.shutdown(); }
  }

  private LocatedBlocks getBlockLocations(Path file, long length)
    throws IOException {
    DistributedFileSystem dfs = (DistributedFileSystem) fileSys;
    return dfs.getClient().namenode.getBlockLocations(file.toString(), 0, length);
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
}

