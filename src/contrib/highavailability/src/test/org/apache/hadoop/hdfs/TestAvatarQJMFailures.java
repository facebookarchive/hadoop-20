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

import java.io.IOException;
import java.net.InetSocketAddress;

import java.net.URL;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.impl.Log4JLogger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.qjournal.MiniJournalCluster;
import org.apache.hadoop.hdfs.qjournal.protocol.JournalConfigKeys;
import org.apache.hadoop.hdfs.qjournal.server.Journal;
import org.apache.hadoop.hdfs.qjournal.server.JournalNode;
import org.apache.hadoop.hdfs.server.namenode.AvatarNode;
import org.apache.hadoop.hdfs.server.namenode.FSEditLog;
import org.apache.hadoop.hdfs.server.namenode.FSImageTestUtil.CheckpointTrigger;
import org.apache.hadoop.hdfs.util.InjectionEvent;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.InjectionEventI;
import org.apache.hadoop.util.InjectionHandler;
import org.apache.log4j.Level;

import static org.mockito.Matchers.anyInt;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import org.junit.After;
import org.junit.AfterClass;
import static org.junit.Assert.*;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestAvatarQJMFailures {

  {
    ((Log4JLogger)Journal.LOG).getLogger().setLevel(Level.ALL);
  }
  
  private static final Log LOG = LogFactory.getLog(TestAvatarQJMFailures.class);
  
  private static final int WRITE_TXID_TIMEOUT = 1000;
  
  private Configuration conf;
  private MiniAvatarCluster cluster;
  private FileSystem fs;
  private MiniJournalCluster journalCluster;
  
  private TestAvatarQJMFailuresHandler handler;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    MiniAvatarCluster.createAndStartZooKeeper();
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    MiniAvatarCluster.shutDownZooKeeper();
  }

  public void setUp(String name) throws Exception {
    setUp(new Configuration(), null, name);
  }

  public void setUp(Configuration confg, MiniJournalCluster jCluster,
      String name)
      throws Exception {
    LOG.info("START TEST : " + name);
    
    handler = new TestAvatarQJMFailuresHandler();
    InjectionHandler.set(handler);

    FSEditLog.setRuntimeForTesting(Runtime.getRuntime());
    conf = confg;
    if (jCluster == null) {
      cluster = new MiniAvatarCluster.Builder(conf).numDataNodes(1)
          .enableQJM(true).build();
    } else {
      cluster = new MiniAvatarCluster.Builder(conf).numDataNodes(1)
          .enableQJM(true).setJournalCluster(jCluster).build();
    }
    fs = cluster.getFileSystem();
    journalCluster = cluster.getJournalCluster();
    
  }

  @After
  public void tearDown() throws Exception {
    if (fs != null)
      fs.close();
    if (cluster != null)
      cluster.shutDown();
    InjectionHandler.clear();
  }

  public static class TestAvatarQJMFailuresHandler extends InjectionHandler {
    
    int currentJournalHttpPort = -1;
    volatile boolean simulateCrash = false;
    volatile boolean instantiatedIngest = false;
    final CheckpointTrigger checkpointTrigger = new CheckpointTrigger();
    volatile boolean simulateJNFailureForWrite = false;
    volatile int journalFailures = 0;
    volatile int currentJournalRPCPort = -1;
    volatile boolean simulateJournalHang = false;

    @Override
    protected void _processEvent(InjectionEventI event, Object... args) {
      if (event == InjectionEvent.QJM_URLLOGEDITLOGSTREAM_NEXTOP) {
        currentJournalHttpPort = ((URL) args[0]).getPort();
      } else if (event == InjectionEvent.INGEST_BEFORE_LOAD_EDIT) {
        instantiatedIngest = true;
      }
      checkpointTrigger.checkpointDone(event, args);
    }

    @Override
    protected void _processEventIO(InjectionEventI event, Object... args)
        throws IOException {
      if (event == InjectionEvent.QJM_JOURNALNODE_JOURNAL) {
        int port = ((InetSocketAddress) args[1]).getPort();
        if (port == currentJournalRPCPort) {
          if (simulateJNFailureForWrite) {
            journalFailures++;
            throw new IOException("Simulating journal failure");
          } else if (simulateJournalHang) {
            journalFailures++;
            // Hang long enough for the timeout.
            DFSTestUtil.waitNMilliSecond(WRITE_TXID_TIMEOUT * 5);
          }
        }
      }
    }

    @Override
    protected boolean _trueCondition(InjectionEventI event, Object... args) {
      if (event == InjectionEvent.FSNAMESYSTEM_CLOSE_DIRECTORY) {
        return !simulateCrash;
      }
      return true;
    }

    @Override
    protected boolean _falseCondition(InjectionEventI event, Object... args) {
      return checkpointTrigger.triggerCheckpoint(event);
    }
    
    void doCheckpoint() throws IOException {
      checkpointTrigger.doCheckpoint();
    }
  }

  /**
   * Tests that if a single journal node hangs, we can still proceed normally
   * with all operations.
   */
  @Test
  public void testJournalNodeHang() throws Exception {
    // Set a timeout of 1 second.
    Configuration conf = new Configuration();
    conf.setInt(JournalConfigKeys.DFS_QJOURNAL_WRITE_TXNS_TIMEOUT_KEY,
        WRITE_TXID_TIMEOUT);
    setUp(conf, null, "testJournalNodeHang");

    // Get the current journal port.
    JournalNode jn = getStandbyTailingJN();

    DFSTestUtil.createFile(fs, new Path("/test"), 1024, (short) 1, 0);
    handler.simulateJournalHang = true;

    // Ensure we can still write.
    DFSTestUtil.createFile(fs, new Path("/test1"), 1024, (short) 1, 0);

    // Ensure we can still do checkpoints.
    handler.doCheckpoint();
    DFSTestUtil.createFile(fs, new Path("/test2"), 1024, (short) 1, 0);

    // Verify the JN is lagging.
    assertFalse(cluster.getPrimaryAvatar(0).avatar.getLastWrittenTxId() == jn
        .getJournal((MiniAvatarCluster.JID + "/zero").getBytes())
        .getHighestWrittenTxId());

    // Ensure we can failover.
    cluster.failOver();
    cluster.restartStandby();

    // Ensure we can write after failover.
    DFSTestUtil.createFile(fs, new Path("/test3"), 1024, (short) 1, 0);

    // Ensure we can still do checkpoints.
    handler.doCheckpoint();

    // Ensure that we did end up hanging a journal node.
    DFSTestUtil.createFile(fs, new Path("/test4"), 1024, (short) 1, 0);
    assertTrue(handler.journalFailures > 0);
    assertFalse(cluster.getPrimaryAvatar(0).avatar.getLastWrittenTxId() == jn
        .getJournal((MiniAvatarCluster.JID + "/one").getBytes())
        .getHighestWrittenTxId());

    handler.simulateJournalHang = false;
  }


  private JournalNode getStandbyTailingJN() {
    assertTrue(handler.currentJournalHttpPort != -1);
    // Find the journal node the Standby is tailing from.
    JournalNode jn = null;
    for (JournalNode j : journalCluster.getJournalNodes()) {
      if (j.getBoundHttpAddress().getPort() == handler.currentJournalHttpPort) {
        jn = j;
        break;
      }
    }
    handler.currentJournalRPCPort = jn.getBoundIpcAddress().getPort();
    return jn;
  }

  /**
   * Tests that if the JournalNode from which the standby is tailing fails, the
   * Standby can still continue tailing from another JournalNode without issues.
   */
  @Test
  public void testStandbyTailingJNFailure() throws Exception {
    setUp("testStandbyTailingJNFailure");

    DFSTestUtil.createFile(fs, new Path("/test"), 1024, (short) 1, 0);
    AvatarNode standby = cluster.getStandbyAvatar(0).avatar;
    handler.doCheckpoint();

    // Find the journal node the Standby is tailing from.
    JournalNode jn = getStandbyTailingJN();
    assertNotNull(jn);

    jn.stopAndJoin(0);
    handler.instantiatedIngest = false;

    LOG.info("Stopped JournalNode with port "
        + jn.getBoundHttpAddress().getPort());
    LOG.info("current journal port : " + handler.currentJournalHttpPort);

    DFSTestUtil.createFile(fs, new Path("/test1"), 1024, (short) 1, 0);

    // Wait for ingest.
    long start = System.currentTimeMillis();
    while (!handler.instantiatedIngest
        && System.currentTimeMillis() - start < 10000) {
      DFSTestUtil.waitSecond();
    }

    assertTrue(handler.instantiatedIngest);

    LOG.info("Ingest failures : "
        + standby.getAvatarNodeMetrics().numIngestFailures.get());
    assertTrue(standby.getAvatarNodeMetrics().numIngestFailures
        .get() <= 1);
  }

  /**
   * This tests that if the journal node that the Standby is tailing from fails
   * only from the perspective of the primary avatar, the Standby should still
   * be able to switch to other journal nodes and continue ingesting.
   */
  @Test
  public void testStandbyTailingJNFailureForWrite() throws Exception {
    setUp("testStandbyTailingJNFailureForWrite");

    // Create some files and ensure we know which JN the Standby is tailing
    // from.
    DFSTestUtil.createFile(fs, new Path("/test"), 1024, (short) 1, 0);
    getStandbyTailingJN();

    // Now ensure the Primary excludes the JN the standby is tailing from.
    handler.simulateJNFailureForWrite = true;
    DFSTestUtil.createFile(fs, new Path("/test1"), 1024, (short) 1, 0);
    assertTrue(handler.journalFailures > 0);

    // Now ensure we can do a failover safely.
    cluster.failOver();
  }

  /**
   * Tests that we can still continue running if the journal cluster fails.
   */
  @Test
  public void testJournalClusterFailure() throws Exception {
    setUp("testJournalClusterFailure");
    DFSTestUtil.createFile(fs, new Path("/test"), 1024, (short) 1, 0);
    // Ensure standby is down, otherwise it would cause a System.exit() because
    // of failed checkpointing.
    cluster.killStandby();
    journalCluster.shutdown();
    DFSTestUtil.createFile(fs, new Path("/test1"), 1024, (short) 1, 0);
  }

  /**
   * Tests that if the journal cluster is marked as required, we cannot
   * tolerate its failure.
   */
  @Test
  public void testJournalClusterFailureWhenRequired() throws Exception {
    Configuration conf = new Configuration();
    journalCluster = new MiniJournalCluster.Builder(conf).numJournalNodes(
        3).build();
    String journalURI = journalCluster.getQuorumJournalURI(
        MiniAvatarCluster.JID).toString();
    conf.set("dfs.name.edits.dir.required", journalURI + "/zero," + journalURI
        + "/one");

    setUp(conf, journalCluster, "testJournalClusterFailureWhenRequired");

    // spy the runtime
    Runtime runtime = Runtime.getRuntime();
    runtime = spy(runtime);
    doNothing().when(runtime).exit(anyInt());
    FSEditLog.setRuntimeForTesting(runtime);


    // Kill standby to ensure only 1 runtime.exit();
    cluster.killStandby();
    journalCluster.shutdown();
    fs.create(new Path("/test1"));

    // verify failure.
    verify(runtime, times(1)).exit(anyInt());
  }

  /**
   * Tests that if the primary crashes multiple times and restarts resulting in
   * multiple logs with no END_LOG_SEGMENT, then the Standby can still
   * successfully ingest these logs.
   */
  @Test
  public void testMultiplePrimaryCrash() throws Exception {
    setUp("testMultiplePrimaryCrash");
    handler.simulateCrash = true;

    for (int i = 0; i < 3; i++) {
      // Create a few unfinalized segments.
      DFSTestUtil.createFile(fs, new Path("/test" + i), 1024, (short) 1, 0);
      cluster.killPrimary();
      cluster.getNameNode(0).unlockStorageDirectory("zero");
      cluster.getNameNode(0).unlockStorageDirectory("one");
      cluster.restartAvatarNodes();
    }

    DFSTestUtil.createFile(fs, new Path("/finaltest"), 1024, (short) 1, 0);

    long lastTxid = cluster.getPrimaryAvatar(0).avatar.getLastWrittenTxId();

    // Force failover since Primary is in simulate crash mode.
    cluster.failOver(true);

    // Standby will have one more txid after failover due to START_LOG_SEGMENT.
    assertEquals(lastTxid + 1,
        cluster.getPrimaryAvatar(0).avatar.getLastWrittenTxId());
  }
}
