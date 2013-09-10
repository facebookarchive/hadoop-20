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
import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.FSConstants;
import org.apache.hadoop.mapred.MiniMRCluster;
import org.apache.hadoop.util.ToolRunner;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestRaidSmokeTest {
  final static Log LOG = LogFactory.getLog(TestRaidSmokeTest.class);
  final static String TEST_DIR = new File(System.getProperty("test.build.data",
      "build/contrib/raid/test/data")).getAbsolutePath();
  final static String CONFIG_FILE = new File(TEST_DIR, 
      "test-raid.xml").getAbsolutePath();
  Configuration conf;
  String namenode = null;
  MiniDFSCluster dfsCluster = null;
  MiniMRCluster mr = null;
  String jobTrackerName = null;
  String hftp = null;
  FileSystem fileSys = null;
  RaidNode cnode = null;
  
  @Test
  public void testSmokeTestFailedToRaid() throws Exception {
    Configuration localConf = new Configuration(conf);
    // Set to a wrong mapreduce cluster, shouldn't be able to submit job
    localConf.set("mapred.job.tracker", "localhost:0");
    cnode = RaidNode.createRaidNode(null, localConf);
    // Wait for raidnode to start up.
    Thread.sleep(3000);
    // Create RaidShell and raid the files.
    RaidShell shell = new RaidShell(conf);
    String[] args = new String[3];
    args[0] = "-smoketest";
    assertEquals("Smoke test should fail", 
        -1, ToolRunner.run(shell, args));
  }

  @Test
  public void testSuccessfulSmokeTest() throws Exception {
    Configuration localConf = new Configuration(conf);
    cnode = RaidNode.createRaidNode(null, localConf);
    // Wait for raidnode to start up.
    Thread.sleep(3000);
    // Create RaidShell and raid the files.
    RaidShell shell = new RaidShell(conf);
    String[] args = new String[3];
    args[0] = "-smoketest";
    assertEquals("Smoke test should succeed", 
        0, ToolRunner.run(shell, args));
  }

  @Before
  public void setUp() throws IOException {
    if (System.getProperty("hadoop.log.dir") == null) {
      String base = new File(".").getAbsolutePath();
      System.setProperty("hadoop.log.dir", new Path(base).toString() + "/logs");
    }
    new File(TEST_DIR).mkdirs(); // Make sure data directory exists
    conf = new Configuration();
    conf.set("raid.config.file", CONFIG_FILE);
    // do not use map-reduce cluster for Raiding
    conf.set("raid.classname", "org.apache.hadoop.raid.DistRaidNode");
    conf.set("raid.blockfix.classname",
        "org.apache.hadoop.raid.DistBlockIntegrityMonitor");
    conf.set("raid.server.address", "localhost:" + MiniDFSCluster.getFreePort());
    String raidHttpAddress = "localhost:" + MiniDFSCluster.getFreePort();
    conf.set("mapred.raid.http.address", raidHttpAddress);
    conf.set(FSConstants.DFS_RAIDNODE_HTTP_ADDRESS_KEY, raidHttpAddress);
    conf.setInt("raid.blockfix.interval", 1000);
    conf.set("raid.server.address", "localhost:" + MiniDFSCluster.getFreePort());
    conf.setBoolean("dfs.permissions", false);
    // set max pending jobs to 0, disallow raidnode to fix the file by itself
    conf.setLong(DistBlockIntegrityMonitor.MAX_PENDING_JOBS, 0L);
    Utils.loadTestCodecs(conf, 3, 1, 3, "/destraid", "/destraidrs");
    dfsCluster = new MiniDFSCluster(conf, 2, true, null);
    dfsCluster.waitActive();
    fileSys = dfsCluster.getFileSystem();
    namenode = fileSys.getUri().toString();
    mr = new MiniMRCluster(4, namenode, 3);
    jobTrackerName = "localhost:" + mr.getJobTrackerPort();
    conf.set("mapred.job.tracker", jobTrackerName);    
    FileSystem.setDefaultUri(conf, namenode);
    TestBlockFixer.setChecksumStoreConfig(conf);
    ConfigBuilder cb = new ConfigBuilder(CONFIG_FILE);
    cb.persist(); 
  }

  @After
  public void tearDown() throws IOException {
    if (cnode != null) { cnode.stop(); cnode.join(); }
    if (dfsCluster != null) {dfsCluster.shutdown();}
  }
}