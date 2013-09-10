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
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Random;
import java.util.TreeMap;

import javax.security.auth.login.LoginException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.mapred.MiniMRCluster;
import org.apache.hadoop.raid.RaidHistogram.BlockFixStatus;
import org.apache.hadoop.raid.protocol.RaidProtocol;
import org.apache.hadoop.security.UnixUserGroupInformation;

import junit.framework.TestCase;

public class TestRaidHistogram extends TestCase {
  public TestRaidHistogram(String name) {
    super(name);
    // TODO Auto-generated constructor stub
  }

  final static Log LOG = LogFactory.getLog(TestRaidHistogram.class);
  final static String TEST_DIR = new File(System.getProperty("test.build.data",
      "build/contrib/raid/test/data")).getAbsolutePath();
  final static String CONFIG_FILE = new File(TEST_DIR, 
      "test-raid.xml").getAbsolutePath();
  Configuration conf;
  String namenode = null;
  MiniDFSCluster dfsCluster = null;
  String hftp = null;
  MiniMRCluster mr = null;
  FileSystem fileSys = null;
  RaidNode cnode = null;
  String jobTrackerName = null;
  Random rand;
  int nPercents = 10;
  String monitorDirStr = "/a";
  String[] monitorDirs = monitorDirStr.split(",");
  public volatile boolean running = true;
  
  private void mySetup() throws Exception {
    if (System.getProperty("hadoop.log.dir") == null) {
      String base = new File(".").getAbsolutePath();
      System.setProperty("hadoop.log.dir", new Path(base).toString() + "/logs");
    }
    new File(TEST_DIR).mkdirs(); // Make sure data directory exists
    conf = new Configuration();
    long seed = (new Random()).nextLong();
    LOG.info("Random seed is " + seed);
    rand = new Random(seed);
    conf.set("raid.config.file", CONFIG_FILE);
    conf.setBoolean("raid.config.reload", true);

    // scan all policies once every 5 second
    conf.setLong("raid.policy.rescan.interval", 5000);

    // do not use map-reduce cluster for Raiding
    conf.set("raid.classname", "org.apache.hadoop.raid.LocalRaidNode");
    conf.set("raid.server.address", "localhost:" + MiniDFSCluster.getFreePort());
    conf.set("mapred.raid.http.address", "localhost:0");
    // Make sure initial repl is smaller than NUM_DATANODES
    conf.setInt(RaidNode.RAID_PARITY_INITIAL_REPL_KEY, 1);
    
    Utils.loadTestCodecs(conf, 3, 1, 3, "/destraid", "/destraidrs");

    conf.setBoolean("dfs.permissions", false);
    conf.set(BlockIntegrityMonitor.RAIDNODE_CORRUPT_FILE_COUNTER_DIRECTORIES_KEY,
        monitorDirStr);
    // initialize as 1hour and 2hours windows
    conf.set(BlockIntegrityMonitor.MONITOR_SECONDS_KEY, "3600,120");

    dfsCluster = new MiniDFSCluster(conf, 1, true, null);
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
    cb.addPolicy("RaidTest1", "/user/dhruba/raidtest",
        1, 1);
    cb.addPolicy("RaidTest2", "/user/dhruba/raidtestrs",
        1, 1, "rs");
    cb.persist();
  }

  private void myTearDown() throws Exception {
    if (cnode != null) { cnode.stop(); cnode.join(); }
    if (dfsCluster != null) {dfsCluster.shutdown();}
    if (mr != null) {mr.shutdown(); }
  }
  
  public class FakeBlockFixerThread extends Thread {
    public RaidProtocol rpcRaidnode;
    public RaidProtocol raidnode;
    public int start;
    public int range;
    public long round;
    
    FakeBlockFixerThread(int newStart, int newRange, long newRound)
        throws Exception {
      UnixUserGroupInformation ugi;
      try {
        ugi = UnixUserGroupInformation.login(conf, true);
      } catch (LoginException e) {
        throw (IOException)(new IOException().initCause(e));
      }
      rpcRaidnode = RaidShell.createRPCRaidnode(RaidNode.getAddress(conf),
          conf, ugi);
      raidnode = RaidShell.createRaidnode(rpcRaidnode);
      start = newStart;
      range = newRange;
      round = newRound;
    }
    
    public void run() {
      try {
        for (int i = 0 ;i < round; i++) {
          int value = rand.nextInt(range);
          // Make sure the minimum and maximum values are sent. 
          if (i == 0) {
            value = start;
          } else if (i == 1) {
            value = start + range - 1;
          } else {
            value += start;
          }
          String path1;
          String path2;
          for (int j = 0 ;j <= monitorDirs.length; j++) {
            if (j == monitorDirs.length) {
              path1 = "/others/" + rand.nextInt();
              path2 = "/others/failed" + start + "_" + i;
            } else {
              path1 = monitorDirs[j] + "/" + rand.nextInt();
              path2 = monitorDirs[j] + "/" + start + "_" + i;
            }
            try {
              raidnode.sendRecoveryTime(path1, value, null);
            } catch (IOException ioe) {
              LOG.error(ioe);
            }
            try {
              raidnode.sendRecoveryTime(path2, Integer.MAX_VALUE, null);
            } catch (IOException ioe) {
              LOG.error(ioe);
            }
          }
        }
      } finally {
        RPC.stopProxy(rpcRaidnode);
      }
    }
  }
  
  public void sendRecoveryTimes(int nPercents, int start,
      int range, int rounds) throws Exception {
    FakeBlockFixerThread[] threads = new FakeBlockFixerThread[nPercents];
    for (int i = 0; i < nPercents; i++, start += range) {
      threads[i] = new FakeBlockFixerThread(start, range, rounds);
      threads[i].start();
    }
    for (int i = 0; i < nPercents; i++) {
      threads[i].join();
    }
  }
  
  public void printBlockFixStatus(TreeMap<Long, BlockFixStatus> status) {
    for (Long window: status.keySet()) {
      LOG.info("Window: " + window);
      BlockFixStatus bfs = status.get(window);
      LOG.info("failedPaths: " + bfs.failedPaths);
      String values = "";
      for (Long val: bfs.percentValues) {
        values += "/" + val;
      }
      LOG.info("percentValues: " + values);
    }
  }
  
  // send recovery time multiple times
  public void testRepeatSendingRecoveryTime() throws Exception {
    int rounds = 4;
    int nPercents = 2;
    int range = 1000000;
    int dividedRange = range / 1000;
    float step = 1.0f / nPercents;
    long gapTime = 3000L;
    ArrayList<Long> windows = new ArrayList<Long>();
    windows.add(gapTime);
    windows.add(3600000L);
    int sendRound = 2;
    try {
      mySetup();
      Configuration localConf = new Configuration(conf);
      localConf.set(BlockIntegrityMonitor.MONITOR_SECONDS_KEY,
          gapTime/1000 + ",3600");
      cnode = RaidNode.createRaidNode(null, localConf);
      ArrayList<Float> percents = new ArrayList<Float>();
      
      for (int i = 0 ; i <= 2; i++) {
        percents.add(step * i);
      }
      Collections.shuffle(percents);
      for (int r = 0; r < rounds; r++) {
        // submit some data
        long sTime = System.currentTimeMillis();
        sendRecoveryTimes(2, 0, range, sendRound);       
        LOG.info("Get blockFixStatus");
        String monitorDir = monitorDirs[0];
        TreeMap<Long, BlockFixStatus> status = 
            cnode.blockIntegrityMonitor.getBlockFixStatus(
            monitorDir, nPercents, percents, sTime + gapTime - 1000);
        printBlockFixStatus(status);
        assertTrue(status.containsKey(windows.get(0)));
        assertTrue(status.containsKey(windows.get(1)));
        BlockFixStatus bfs = status.get(windows.get(0));
        // Verify failed recovered files for the first window
        assertEquals("The number of failed recovery files should match",
            sendRound*nPercents, bfs.failedPaths);
        // Verify percent values for the first window
        assertEquals(nPercents + 1, bfs.percentValues.length);
        assertEquals(0, bfs.percentValues[0]);
        for (int j = 1; j <= nPercents; j++) {
          assertEquals(dividedRange * j - 1, bfs.percentValues[j]);
        }
        bfs = status.get(windows.get(1));
        // Verify failed recovered files for the second window
        assertEquals("The number of failed recovery files should match",
            sendRound*nPercents, bfs.failedPaths);
        // Verify percent values for the second window
        assertEquals(nPercents + 1, bfs.percentValues.length);
        assertEquals(0, bfs.percentValues[0]);
        for (int j = 1; j <= nPercents; j++) {
          assertEquals(dividedRange * j - 1, bfs.percentValues[j]);
        }
        Thread.sleep(gapTime + 1000);
        status = cnode.blockIntegrityMonitor.getBlockFixStatus(
            monitorDir, nPercents, percents, System.currentTimeMillis());
        printBlockFixStatus(status);
        assertTrue(status.containsKey(windows.get(0)));
        assertTrue(status.containsKey(windows.get(1)));
        bfs = status.get(windows.get(0));
        // Verify failed recovered files for the first window
        assertEquals("The number of failed recovery files should be 0",
            0, bfs.failedPaths);
        // Verify percent values for the first window, they should all be -1
        assertEquals(nPercents + 1, bfs.percentValues.length);
        assertEquals(-1, bfs.percentValues[0]);
        for (int j = 1; j <= nPercents; j++) {
          assertEquals(-1, bfs.percentValues[j]);
        }
      }
    } finally {
      myTearDown();
    }
  }
  
  /**
   * Have three stages. Each stage spawns nPercents threads.
   * Each thread iterate $rounds rounds and send random number for 
   * each monitor dir to raidnode including succeed files and failed files. 
   * Set two windows: The first window covers stage3 only.
   * The second window covers stage2 and stage3 only.
   * Calling getBlockFixStatus should be able to filter out all stage1 points
   * The histogram counts for the second window should be double as the of 
   * the first window.
   */
  public void testHistograms() throws Exception {
    int rounds = 10000;
    int range = 1000000;
    int dividedRange = range / 1000;
    float step = 1.0f / nPercents;
    try {
      mySetup();
      cnode = RaidNode.createRaidNode(null, conf);
      ArrayList<Float> percents = new ArrayList<Float>();
      
      for (int i = 0 ; i <= nPercents; i++) {
        percents.add(step * i);
      }
      Collections.shuffle(percents);
      // submit some old data
      sendRecoveryTimes(nPercents, range*(nPercents + 1), range, rounds);
      Thread.sleep(100);
      long ckpTime1 = System.currentTimeMillis();
      
      sendRecoveryTimes(nPercents, 0, range, rounds);
      Thread.sleep(100);
      long ckpTime2 = System.currentTimeMillis();
      
      sendRecoveryTimes(nPercents, 0, range, rounds);
      long endTime = System.currentTimeMillis();
      ArrayList<Long> newWindows = new ArrayList<Long>();
      newWindows.add(endTime - ckpTime2);
      newWindows.add(endTime - ckpTime1);
      HashMap<String, RaidHistogram> recoveryTimes = 
          cnode.blockIntegrityMonitor.getRecoveryTimes();
      for (RaidHistogram histogram: recoveryTimes.values()) {
        histogram.setNewWindows(newWindows);
      }
      for (int i = 0; i <= monitorDirs.length; i++) {
        String monitorDir;
        if (i < monitorDirs.length) {
          monitorDir = monitorDirs[i];
        } else {
          monitorDir = BlockIntegrityMonitor.OTHERS;
        }
        assertEquals("Stale entries are not filtered", rounds*nPercents*3*2, 
            cnode.blockIntegrityMonitor.getNumberOfPoints(monitorDir));
        TreeMap<Long, BlockFixStatus> status = 
            cnode.blockIntegrityMonitor.getBlockFixStatus(
            monitorDir, nPercents, percents, endTime);
        assertTrue(status.containsKey(newWindows.get(0)));
       	assertTrue(status.containsKey(newWindows.get(1)));
       	BlockFixStatus bfs = status.get(newWindows.get(0));
        assertEquals("Stale entries are not filtered", rounds*nPercents*2*2, 
            cnode.blockIntegrityMonitor.getNumberOfPoints(monitorDir));
        // Verify failed recovered files for the first window
        assertEquals("The number of failed recovery files should match",
            rounds*nPercents, bfs.failedPaths);
       	// Verify histogram for the first window
       	assertEquals(nPercents, bfs.counters.length);
       	for (int j = 0; j < nPercents; j++) {
       	  assertEquals(rounds, bfs.counters[j]);
       	}
       	// Verify percent values for the first window
       	assertEquals(nPercents + 1, bfs.percentValues.length);
       	assertEquals(0, bfs.percentValues[0]);
       	for (int j = 1; j <= nPercents; j++) {
       	  assertEquals(dividedRange * j - 1, bfs.percentValues[j]);
       	}
       	bfs = status.get(newWindows.get(1));
        // Verify failed recovered files for the second window
        assertEquals("The number of failed recovery files should match",
            rounds*nPercents, bfs.failedPaths);
       	// Verify histogram for the second window
       	assertEquals(nPercents, bfs.counters.length);
        for (int j = 0; j < nPercents; j++) {
          assertEquals(rounds*2, bfs.counters[j]);
        }
       	// Verify percent values for the second window
        assertEquals(nPercents + 1, bfs.percentValues.length);
        assertEquals(0, bfs.percentValues[0]);
        for (int j = 1; j <= nPercents; j++) {
          assertEquals(dividedRange * j - 1, bfs.percentValues[j]);
        }
      }      
    } finally {
      myTearDown();
    }
  }
} 