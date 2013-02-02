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

package org.apache.hadoop.hdfs;

import java.io.FileNotFoundException;
import java.io.IOException;

import org.junit.Test;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.AfterClass;
import static org.junit.Assert.*;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.util.InjectionEvent;
import org.apache.hadoop.util.InjectionEventI;
import org.apache.hadoop.util.InjectionHandler;

import static org.apache.hadoop.hdfs.server.namenode.TestListCorruptFileBlocks.
  countPaths;

public class TestAvatarAPI {
  final static Log LOG = LogFactory.getLog(TestAvatarAPI.class);

  private static final String FILE_PATH = "/dir1/dir2/myfile";
  private static final String DIR_PATH = "/dir1/dir2";
  private static final long FILE_LEN = 512L * 1024L;
  private static final long MAX_WAIT = 30000L;

  private Configuration conf;
  private MiniAvatarCluster cluster;
  private DistributedAvatarFileSystem dafs;
  private Path path;
  private Path dirPath;

  @BeforeClass
  public static void setUpClass() throws Exception {
    MiniAvatarCluster.createAndStartZooKeeper();
  }

  /**
   * Set up MiniAvatarCluster.
   */
  public void setUp(boolean federation, String name) throws Exception {
    LOG.info("------------------- test: " + name + " START ----------------");
    conf = new Configuration();
    // populate repl queues on standby (in safe mode)
    conf.setFloat("dfs.namenode.replqueue.threshold-pct", 0f);
    conf.setLong("fs.avatar.standbyfs.initinterval", 1000);
    conf.setLong("fs.avatar.standbyfs.checkinterval", 1000);
    if (!federation) {
      cluster = new MiniAvatarCluster(conf, 3, true, null, null);
      dafs = cluster.getFileSystem();
    } else {
      cluster = new MiniAvatarCluster(conf, 3, true, null, null, 1, true);
      dafs = cluster.getFileSystem(0);
    }

    
    path = new Path(FILE_PATH);
    dirPath = new Path(DIR_PATH);
    DFSTestUtil.createFile(dafs, path, FILE_LEN, (short) 1, 0L);
    DFSTestUtil.waitReplication(dafs, path, (short) 1);
    try {
      Thread.sleep(4000);
    } catch (InterruptedException ignore) {
    }
  }

  private void checkPrimary() throws Exception {
    FileStatus fs = dafs.getFileStatus(path, false);
    FileStatus[] dir = dafs.listStatus(dirPath, false);
    RemoteIterator<Path> cfb =
      dafs.listCorruptFileBlocks(dirPath, false);
    assertTrue("DAFS file status has the wrong length",
               fs != null && fs.getLen() == FILE_LEN);
    assertTrue("DAFS directory listing has the wrong length",
               dir != null && dir.length == 1);
    assertTrue("DAFS expected 0 corrupt file blocks",
               countPaths(cfb) == 0);

    ContentSummary cs = dafs.getContentSummary(path, false);
    DatanodeInfo[] di = dafs.getDataNodeStats(false);
    assertTrue("DAFS datanode info should contain 3 data nodes",
               di.length == 3);
  }

  private void checkStandby() throws Exception {
    FileStatus fs = null;
    FileStatus[] dir = null;
    long startTime = System.currentTimeMillis();
    while ((fs == null || dir == null || dir.length == 0 ||
            dafs.getDataNodeStats(true).length != 3) &&
           (System.currentTimeMillis() < startTime + MAX_WAIT)) {
      try {
        fs = dafs.getFileStatus(path, true);
        dir = dafs.listStatus(dirPath, true);
      } catch (FileNotFoundException fe) {
        LOG.info("DAFS File not found on standby avatar, retrying.");
      }

      if (fs == null) {
        LOG.info("DAFS file " + path.toString() + " not found");
      }
      if (dir == null) {
        LOG.info("DAFS dir " + dirPath.toString() + " not found");
      } else {
        if (dir.length == 0) {
          LOG.info("DAFS dir " + dirPath.toString() +
                   " is empty");
        }
      }

      try {
        Thread.sleep(100);
      } catch (InterruptedException ignore) {
      }
    }
    assertTrue("DAFS file status has the wrong length",
               fs != null && fs.getLen() == FILE_LEN);
    assertTrue("DAFS directory listing has the wrong length",
               dir != null && dir.length == 1);

    ContentSummary cs = dafs.getContentSummary(path, true);
    DatanodeInfo[] di = dafs.getDataNodeStats(true);
    assertEquals("DAFS datanode info should contain 3 data nodes",
               3, di.length);
  }
  
  @Test
  public void testPrimaryShutdownFailure() throws Exception {
    setUp(false, "testPrimaryShutdownFailure");
    InjectionHandler.set(new TestAvatarAPIHandler());
    LOG.info("DAFS testPrimary");
    try {
      cluster.killPrimary();
      fail("Shutting down the node should fail");
    } catch (IOException e) {
      assertTrue(e.toString().contains("Number of required edit streams"));
    }
    InjectionHandler.clear();
  }
  
  /**
   * Read from primary avatar.
   */
  @Test
  public void testPrimary() throws Exception {
    setUp(false, "testPrimary");
    LOG.info("DAFS testPrimary");
    cluster.killStandby();
    checkPrimary();
  }
  
  /**
   * Read from standby avatar.
   */
  @Test
  public void testStandby() throws Exception {
    setUp(false, "testStandby");
    LOG.info("DAFS testStandby");
    long lastTxId = getLastTxId();
    cluster.killPrimary(false);
    // make sure to consume all transactions.
    cluster.getStandbyAvatar(0).avatar.quiesceStandby(lastTxId);
    checkStandby();
  }
  
  /**
   * Test if we can still read standby after failover.
   */
  @Test
  public void testFailOver() throws Exception {
    setUp(false, "testFailOver");
    LOG.info("DAFS testFailOver");
    long lastTxId = getLastTxId();
    cluster.killPrimary();
    cluster.failOver();
    cluster.restartStandby();
    cluster.waitAvatarNodesActive();
    cluster.waitDataNodesActive();
    // make sure to consume all transactions.
    cluster.getStandbyAvatar(0).avatar.quiesceStandby(lastTxId);
    
    try {
      Thread.sleep(2000);
    } catch (InterruptedException ignore) {
    }

    checkStandby();
    checkPrimary();
  }
  
  @Test
  public void testStandbyWithFederation() throws Exception {
    setUp(true, "testStandbyWithFederation");
    LOG.info("DAFS testStandby");
    long lastTxId = getLastTxId();
    cluster.killPrimary(0, false);
    // make sure to consume all transactions
    cluster.getStandbyAvatar(0).avatar.quiesceStandby(lastTxId);
    checkStandby();
  }
  
  @Test
  public void testPrimaryWithFederation() throws Exception {
    setUp(true, "testPrimaryWithFederation");
    LOG.info("DAFS testPrimary");
    cluster.killStandby(0);
    checkPrimary();
  }
  
  @Test
  public void testFailOverWithFederation() throws Exception {
    setUp(true, "testFailoverWithFederation");
    LOG.info("DAFS testFailOver");
    long lastTxId = getLastTxId();
    cluster.killPrimary(0);
    cluster.failOver(0);
    cluster.restartStandby(0);
    cluster.waitAvatarNodesActive(0);
    cluster.waitDataNodesActive(0);
    // make sure to consume all transactions.
    cluster.getStandbyAvatar(0).avatar.quiesceStandby(lastTxId);

    try {
      Thread.sleep(2000);
    } catch (InterruptedException ignore) {
    }

    checkStandby();
    checkPrimary();
  }
  
  private long getLastTxId(){
    return cluster.getPrimaryAvatar(0).avatar.getFSImage().getEditLog()
        .getCurrentTxId() - 1;
  }

  @After
  public void shutDown() throws Exception {
    dafs.close();
    cluster.shutDown();
    InjectionHandler.clear();
  }

  @AfterClass
  public static void shutDownClass() throws Exception {
    MiniAvatarCluster.shutDownZooKeeper();
  }
  
  private static class TestAvatarAPIHandler extends InjectionHandler {

    @Override
    public boolean _trueCondition(InjectionEventI event, Object... args) {
      if (event == InjectionEvent.AVATARNODE_CHECKEDITSTREAMS) {
        return false;
      }
      return true;
    }
  }
}
