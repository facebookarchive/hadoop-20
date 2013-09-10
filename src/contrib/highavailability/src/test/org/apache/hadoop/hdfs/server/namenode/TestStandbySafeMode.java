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
package org.apache.hadoop.hdfs.server.namenode;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Random;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniAvatarCluster;
import org.apache.hadoop.hdfs.server.datanode.AvatarDataNode;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.util.InjectionEvent;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.protocol.FSConstants.DatanodeReportType;
import org.apache.hadoop.hdfs.server.namenode.LeaseManager;
import org.apache.hadoop.hdfs.server.namenode.StandbySafeMode;
import org.apache.hadoop.hdfs.server.namenode.StandbySafeMode.SafeModeState;
import org.apache.hadoop.util.InjectionEventI;
import org.apache.hadoop.util.InjectionHandler;

import org.junit.After;
import org.junit.AfterClass;
import static org.junit.Assert.*;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestStandbySafeMode {
  private static MiniAvatarCluster cluster;
  private static Configuration conf;
  private static FileSystem fs;
  private static int BLOCK_SIZE = 1024;
  private static int MAX_BLOCKS = 20;
  private static int MAX_FILE_SIZE = MAX_BLOCKS * BLOCK_SIZE;
  private static int MAX_WAIT_TIME = 30 * 1000;
  private static Log LOG = LogFactory.getLog(TestStandbySafeMode.class);
  TestStandbySafeModeHandler h;
  private static boolean pass = true;
  private static Random random = new Random();
  private static final int LEASE_PERIOD = 5000;
  private static int clearPrimaryCount = 0;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    MiniAvatarCluster.createAndStartZooKeeper();
  }

  public void setUp(boolean shortlease, String name) throws Exception {
    setUp(shortlease, name, 3);
  }

  public void setUp(boolean shortlease, String name, int numDatanodes)
      throws Exception {
    LOG.info("------------------- test: " + name + " START ----------------");
    h = new TestStandbySafeModeHandler();
    InjectionHandler.set(h);
    conf = new Configuration();
    conf.setInt("dfs.block.size", BLOCK_SIZE);
    conf.setFloat("dfs.namenode.replqueue.threshold-pct", 0.1f);
    conf.setInt("dfs.heartbeat.interval", 1);
    conf.setBoolean("fs.checkpoint.enabled", true);
    if (shortlease) {
      conf.setInt("dfs.softlease.period", LEASE_PERIOD);
      conf.setBoolean("fs.ha.retrywrites", true);
      conf.setInt("fs.avatar.failover.checkperiod", 200);
    }
    cluster = new MiniAvatarCluster.Builder(conf).numDataNodes(numDatanodes).enableQJM(false).build();
    fs = cluster.getFileSystem();
    pass = true;
    clearPrimaryCount = 0;
  }

  @After
  public void tearDown() throws Exception {
    if(h != null) {
      h.stallBlockReport = false;
    }
    cluster.shutDown();
    InjectionHandler.clear();
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    MiniAvatarCluster.shutDownZooKeeper();
  }
  
  private void waitAndVerifyBlocks() throws Exception {
    waitAndVerifyBlocks(0);
  }

  private void waitAndVerifyBlocks(int safeCountDiff) throws Exception {
    FSNamesystem standbyNS = cluster.getStandbyAvatar(0).avatar.namesystem;
    FSNamesystem primaryNS = cluster.getPrimaryAvatar(0).avatar.namesystem;
    assertTrue(standbyNS.isInSafeMode());
    long primaryBlocks = primaryNS.getBlocksTotal();
    long standbyBlocks = standbyNS.getBlocksTotal();
    long start = System.currentTimeMillis();
    long standbySafeBlocks = 0;

    // Wait for standby safe mode to catch up to all blocks.
    while (System.currentTimeMillis() - start <= MAX_WAIT_TIME
        && (primaryBlocks != standbyBlocks
        || standbySafeBlocks - safeCountDiff != primaryBlocks)) {
      LOG.info("Verifying blocks...");
      primaryBlocks = primaryNS.getBlocksTotal();
      standbyBlocks = standbyNS.getBlocksTotal();
      standbySafeBlocks = standbyNS.getSafeBlocks();
      DFSTestUtil.waitSecond();
    }

    // Final verification of block counts.
    assertEquals(primaryBlocks, standbyBlocks);
    assertEquals(primaryBlocks - safeCountDiff, standbySafeBlocks);
  }

  private void createTestFiles(String topDir) throws Exception {
    DFSTestUtil util = new DFSTestUtil(topDir, 10, 1, MAX_FILE_SIZE);
    util.createFiles(fs, topDir);
    util.checkFiles(fs, topDir);
  }

  @Test
  public void testStandbyFullBlockReport() throws Exception {
    setUp(false, "testStandbyFullBlockReport");
    h.setIgnoreDatanodes(false);
    // Create test files.
    createTestFiles("/testStandbySafeMode");
    LOG.info("before restart");
    cluster.killStandby();
    cluster.restartStandby();
    LOG.info("after restart");
    DFSTestUtil.waitSecond();

    long lastTxid = cluster.getPrimaryAvatar(0).avatar.getLastWrittenTxId();

    cluster.getStandbyAvatar(0).avatar.quiesceStandby(lastTxid);

    waitAndVerifyBlocks();
  }

  @Test
  public void testStandbySafeMode() throws Exception {
    setUp(false, "testStandbySafeMode");
    h.setIgnoreDatanodes(false);
    // Create test files.
    createTestFiles("/testStandbySafeMode");

    // Sync all data to the edit log.
    cluster.getPrimaryAvatar(0).avatar.getFSImage().getEditLog().logSyncAll();
    long lastTxid = cluster.getPrimaryAvatar(0).avatar.getLastWrittenTxId();

    // Need to quiesce to ingest edits.new since checkpointing is disabled.
    cluster.getStandbyAvatar(0).avatar.quiesceStandby(lastTxid);

    waitAndVerifyBlocks();
  }

  private long getDatanodeBlocks() throws Exception {
    long blocks = 0;
    for (AvatarDataNode dn : cluster.getDataNodes()) {
      blocks += dn.data.getBlockReport(cluster.getPrimaryAvatar(0).avatar
          .getNamespaceID()).length;
    }
    return blocks;
  }

  @Test
  public void testActiveDeletes() throws Exception {
    setUp(false, "testActiveDeletes");
    h.setIgnoreDatanodes(false);
    // Create test files.
    String fileName = "/testActiveDeletes";
    Path p = new Path(fileName);
    DFSTestUtil.createFile(fs, p, (long) MAX_FILE_SIZE, (short) 3,
        System.currentTimeMillis());
    fs.delete(p, false);

    long start = System.currentTimeMillis();
    while (System.currentTimeMillis() - start <= MAX_WAIT_TIME
        && getDatanodeBlocks() != 0) {
      DFSTestUtil.waitSecond();
    }

    // All blocks on the datanode should be deleted.
    assertEquals(0, getDatanodeBlocks());
  }

  @Test
  public void testClearDatanodeRetryList() throws Exception {
    setUp(false, "testClearDatanodeRetryList");
    cluster.restartAvatarNodes();
    h.setIgnoreDatanodes(true);
    // Create test files.
    String fileName = "/testClearDatanodeRetryList";
    Path p = new Path(fileName);
    DFSTestUtil.createFile(fs, p, (long) MAX_FILE_SIZE, (short) 3,
        System.currentTimeMillis());

    h.forceblockreport = true;
    // Force full block reports, incremental reports still get backed off.
    for (AvatarDataNode dn : cluster.getDataNodes()) {
      dn.scheduleNSBlockReport(0);
    }


    long blockReports =
      NameNode.getNameNodeMetrics().numBlockReport.getCurrentIntervalValue();

    // Wait for full block reports.
    long start = System.currentTimeMillis();
    while (System.currentTimeMillis() - start <= MAX_WAIT_TIME
        && NameNode.getNameNodeMetrics().numBlockReport
            .getCurrentIntervalValue() - 3 < blockReports) {
      Thread.sleep(500);
    }

    // Full block reports should have been processed and the incremental list
    // should be cleared.

    assertTrue(NameNode.getNameNodeMetrics()
        .numBlockReport.getCurrentIntervalValue() - 3 >= blockReports);

    // Disable BACKOFF so that incremental reports now go through.
    h.setIgnoreDatanodes(false);

    long lastTxid = cluster.getPrimaryAvatar(0).avatar.getLastWrittenTxId();

    // Need to quiesce to ingest edits.new since checkpointing is disabled.
    cluster.getStandbyAvatar(0).avatar.quiesceStandby(lastTxid);
    waitAndVerifyBlocks();
  }

  class TestStandbySafeModeHandler extends InjectionHandler {
    volatile boolean forceheartbeat = false;
    volatile boolean forceblockreport = false;
    boolean ignoreDatanodes;
    public volatile boolean stallIngest = false;
    volatile boolean stallOfferService = false;
    volatile boolean leaveSafeModeinCheckMode = false;
    volatile boolean ignoreClearPrimary = false;
    volatile boolean stallBlockReport = false;

    public void setIgnoreDatanodes(boolean v) {
      ignoreDatanodes = v;
    }

    @Override
    protected boolean _falseCondition(InjectionEventI event, Object... args) {
      if (event == InjectionEvent.STANDBY_FELL_BEHIND) {
        return ignoreDatanodes;
      }
      if (event == InjectionEvent.OFFERSERVICE_SCHEDULE_HEARTBEAT) {
        return forceheartbeat;
      }
      if (event == InjectionEvent.OFFERSERVICE_SCHEDULE_BR) {
        return forceblockreport;
      }
      if (event == InjectionEvent.STANDBY_SAFEMODE_CHECKMODE) {
        return leaveSafeModeinCheckMode;
      }
      return false;
    }

    @Override
    protected void _processEventIO(InjectionEventI event, Object... args)
        throws IOException {
      if (event == InjectionEvent.OFFERSERVICE_BEFORE_CLEARPRIMARY
          && ignoreClearPrimary) {
        throw new IOException("Ignoring clear primary");
      }
    }

    @Override
    protected void _processEvent(InjectionEventI event, Object... args) {

      if (event == InjectionEvent.AVATARNODE_BLOCKRECEIVED_AND_DELETED_NEW
          && stallBlockReport) {
        try {
          while (stallBlockReport) {
            LOG.info("Stalling block report");
            DFSTestUtil.waitSecond();
          }
        } catch (Exception e) {
          LOG.info("Stall block report failed", e);
          pass = false;
        }

      } else if (event == InjectionEvent.INGEST_BEFORE_LOAD_EDIT) {
        while (stallIngest) {
          DFSTestUtil.waitSecond();
        }
      } else if (event == InjectionEvent.OFFERSERVICE_START && stallOfferService) {
        try {
          while (cluster.getPrimaryAvatar(0) == null) {
            Set<DatanodeID> beats = cluster.getStandbyAvatar(0).avatar
              .getStandbySafeMode().getOutStandingHeartbeats();
            // If the failover is waiting on the datanode that we just
            // started, then perform failover.
            if (beats.size() == 1) {
              DatanodeID node = beats.iterator().next();
              if (node.getPort() == (Integer) args[0]
                  && cluster.getStandbyAvatar(0).avatar.getNameNodeAddress()
                  .equals(args[1])) {
                break;
                  }
            }
            LOG.info("Waiting for failover");
            DFSTestUtil.waitSecond();
          }
        } catch (Throwable e) {
          LOG.warn("Exception waiting for failover", e);
          pass = false;
        }
      } else if (event == InjectionEvent.OFFERSERVICE_CLEAR_PRIMARY) {
        clearPrimaryCount++;
      } else if (event == InjectionEvent.STANDBY_FAILOVER_INPROGRESS
          && stallOfferService) {
        try {
          cluster.startDataNodes(1, null, null, conf);
          ArrayList<AvatarDataNode> dns = cluster.getDataNodes();
          cluster.waitDataNodeInitialized(dns.get(dns.size() - 1));
        } catch (Exception e) {
          LOG.warn("Start datanode failed", e);
          pass = false;
        }
      }

    }
  }

  @Test
  public void testDeadDatanodeFailover() throws Exception {
    setUp(false, "testDeadDatanodeFailover");
    h.setIgnoreDatanodes(false);
    // Create test files.
    createTestFiles("/testDeadDatanodeFailover");
    cluster.shutDownDataNode(0);
    FSNamesystem ns = cluster.getStandbyAvatar(0).avatar.namesystem;
    StandbySafeMode safeMode = cluster.getStandbyAvatar(0).avatar.getStandbySafeMode();
    new ExitSafeMode(safeMode, ns).start();
    cluster.failOver();
    // One datanode should be removed after failover
    assertEquals(2,
        cluster.getPrimaryAvatar(0).avatar.namesystem
            .datanodeReport(DatanodeReportType.LIVE).length);
    assertTrue(pass);
  }

  private class ExitSafeMode extends Thread {
    private final StandbySafeMode safeMode;
    private final FSNamesystem namesystem;
    public int expectedHeartbeatSize = 1;
    public int expectedReportSize = 0;

    public ExitSafeMode(StandbySafeMode safeMode, FSNamesystem namesystem) {
      this.safeMode = safeMode;
      this.namesystem = namesystem;
    }

    public void run() {
     try {
        while (true) {
          if (safeMode.failoverInProgress()
              && safeMode.getOutStandingHeartbeats().size() == expectedHeartbeatSize
              && safeMode.getOutStandingReports().size() == expectedReportSize
              && namesystem.getBlocksTotal() == namesystem.getSafeBlocks()) {
            safeMode.leave(false);
            break;
          }
          safeMode.canLeave();
          DFSTestUtil.waitSecond();
        }
     } catch (Exception e) {
        LOG.info("SafeMode exit failed", e);
        pass = false;
     }
    }
  }

  @Test
  public void testLeaseExpiry() throws Exception {
    setUp(true, "testLeaseExpiry");
    h.setIgnoreDatanodes(false);
    LeaseManager leaseManager = cluster.getStandbyAvatar(0).avatar.namesystem.leaseManager;
    // Set low lease periods.
    leaseManager.setLeasePeriod(LEASE_PERIOD, LEASE_PERIOD);
    String src = "/testLeaseExpiry";

    // Create some data.
    FSDataOutputStream out = fs.create(new Path(src));
    byte[] buffer = new byte[BLOCK_SIZE * 2];
    random.nextBytes(buffer);
    out.write(buffer);
    out.sync();

    // Wait for the hard lease time to expire.
    Thread.sleep(LEASE_PERIOD * 2);

    cluster.failOver();
    LOG.info("Failover done");

    // Renew lease.
    String clientName = ((DistributedFileSystem)fs).getClient().getClientName();
    cluster.getPrimaryAvatar(0).avatar.renewLease(clientName);
    LOG.info("Lease renewal done");

    // Wait to see whether lease expires.
    long start = System.currentTimeMillis();
    while (System.currentTimeMillis() - start < MAX_WAIT_TIME
        && leaseManager.getLeaseByPath(src) != null) {
      DFSTestUtil.waitSecond();
    }
    LOG.info("Wait for lease done");

    // Now try to write to the file.
    out.write(buffer);
    out.sync();
  }

  private void blocksEqual() throws Exception {
    long primaryBlocks = cluster.getPrimaryAvatar(0).avatar.namesystem
        .getBlocksTotal();
    long standbyBlocks = cluster.getStandbyAvatar(0).avatar.namesystem
        .getBlocksTotal();
    while (primaryBlocks != standbyBlocks) {
      primaryBlocks = cluster.getPrimaryAvatar(0).avatar.namesystem
          .getBlocksTotal();
      standbyBlocks = cluster.getStandbyAvatar(0).avatar.namesystem
          .getBlocksTotal();
      Thread.sleep(300);
    }
  }

  private void syncEditLog() throws Exception {
    // Sync all data to the edit log.
    long lastTxid = cluster.getPrimaryAvatar(0).avatar.getLastWrittenTxId();
    cluster.getPrimaryAvatar(0).avatar.getFSImage().getEditLog().logSync();

    cluster.getStandbyAvatar(0).avatar.quiesceStandby(lastTxid);
  }

  @Test
  public void testStandbySafeModeDel() throws Exception {
    setUp(false, "testStandbySafeModeDel");
    h.setIgnoreDatanodes(false);
    // Create test files.
    String topDir = "/testStandbySafeModeDel";
    DFSTestUtil util = new DFSTestUtil(topDir, 1, 1, MAX_FILE_SIZE);
    util.createFiles(fs, topDir);

    syncEditLog();

    // Force full block reports, incremental reports still get backed off.
    h.forceheartbeat = true;
    Thread.sleep(3000);
    for (AvatarDataNode dn : cluster.getDataNodes()) {
      dn.scheduleNSBlockReport(0);
    }
    waitAndVerifyBlocks();
    h.forceheartbeat = false;

    cluster.restartAvatarNodes();
    fs = cluster.getFileSystem();

    // Wait for ingest.
    blocksEqual();
    h.forceheartbeat = true;
    Thread.sleep(3000);
    for (AvatarDataNode dn : cluster.getDataNodes()) {
      dn.scheduleNSBlockReport(0);
    }
    waitAndVerifyBlocks();
    h.forceheartbeat = false;
    // Stall ingest so that incremental block reports are retried.
    h.stallIngest = true;

    for (String fileName : util.getFileNames(topDir)) {
        fs.delete(new Path(fileName), false);
    }

    DFSTestUtil util1 = new DFSTestUtil(topDir, 1, 1, MAX_FILE_SIZE);
    util1.createFiles(fs, topDir);

    // Restart ingest.
    h.stallIngest = false;

    // Wait for ingest.
    blocksEqual();

    waitAndVerifyBlocks();
  }

  @Test
  public void testStandbySafeModeDel1() throws Exception {
    setUp(false, "testStandbySafeModeDel1");
    h.setIgnoreDatanodes(false);
    // Create test files.
    String topDir = "/testStandbySafeModeDel";
    DFSTestUtil util = new DFSTestUtil(topDir, 1, 1, MAX_FILE_SIZE);
    util.createFiles(fs, topDir);

    syncEditLog();
    h.forceheartbeat = true;
    Thread.sleep(3000);
    // Force full block reports, incremental reports still get backed off.
    for (AvatarDataNode dn : cluster.getDataNodes()) {
      dn.scheduleNSBlockReport(0);
    }

    waitAndVerifyBlocks();
    h.forceheartbeat = false;

    cluster.restartAvatarNodes();
    fs = cluster.getFileSystem();

    // Wait for ingest.
    blocksEqual();
    h.forceheartbeat = true;
    Thread.sleep(3000);
    for (AvatarDataNode dn : cluster.getDataNodes()) {
      dn.scheduleNSBlockReport(0);
    }
    waitAndVerifyBlocks();
    h.forceheartbeat = false;
    h.setIgnoreDatanodes(true);

    for (String fileName : util.getFileNames(topDir)) {
      fs.delete(new Path(fileName), false);
    }

    DFSTestUtil util1 = new DFSTestUtil(topDir, 1, 1, MAX_FILE_SIZE);
    util1.createFiles(fs, topDir);

    // Wait for ingest.
    blocksEqual();

    h.setIgnoreDatanodes(false);

    waitAndVerifyBlocks();
  }

  @Test
  public void testDatanodeRegisterDuringFailover() throws Exception {
    String name = "testDatanodeRegisterDuringFailover";
    setUp(false, name);
    String topDir = "/" + name;
    createTestFiles(topDir);
    h.stallOfferService = true;
    cluster.failOver();
    long start = System.currentTimeMillis();
    while (clearPrimaryCount != 4 &&
        System.currentTimeMillis() - start < 30000) {
      LOG.info("Waiting for clearPrimaryCount : " + clearPrimaryCount +
          " to reach 4");
      Thread.sleep(1000);
    }
    assertEquals(4, clearPrimaryCount);
    assertTrue(pass);
  }

  @Test
  /**
   * Ensures that a single thread calls leave() only once in StandbySafeMode.
   */
  public void testSafeModeDuplicateLeave() throws Exception {
    String name = "testSafeModeDuplicateLeave";
    setUp(false, name, 1);
    String topDir = "/" + name;
    createTestFiles(topDir);
    h.leaveSafeModeinCheckMode = true;
    h.ignoreClearPrimary = true;
    ExitSafeMode s = new ExitSafeMode(
        cluster.getStandbyAvatar(0).avatar.getStandbySafeMode(),
        cluster.getStandbyAvatar(0).avatar.namesystem);
    s.expectedHeartbeatSize = 0;
    s.expectedReportSize = 1;
    s.start();
    cluster.failOver();
    assertTrue(pass);
  }

  /**
   * Tests that if we receive a RBW report and a finalized block report
   * (in that order) when the Standby is processing RBW reports, we still
   * get the correct safe block count. Also makes sure that the standby 
   * ingestion clears targets from previous blocks.
   */
  @Test
  public void testSafeModeDuplicateRbwAndFinalizedBlockReport()
      throws Throwable {
    // Setup
    String name = "testSafeModeDuplicateRbwAndFinalizedBlockReport";
    setUp(false, name, 3);
    
    // Create some RBW blocks.
    FSDataOutputStream out = cluster.getFileSystem().create(
        new Path("/" + name), (short) 3);
    byte[] buffer = new byte[BLOCK_SIZE];
    random.nextBytes(buffer);
    out.write(buffer, 0, BLOCK_SIZE);
    out.sync();
    
    // wait for all finalized reports
    // INodeFUC.targets has at least one node
    waitAndVerifyBlocks();
    
    h.stallBlockReport = true;
    
    // allocation of a new block will clear targets at standby
    out.write(buffer, 0, BLOCK_SIZE / 2);
    out.sync();
    
    // make sure there is no stray INodesUC in DatanodeDescriptors
    // wait until standby consumes block allocation
    while (cluster.getStandbyAvatar(0).avatar.namesystem.getTotalBlocks() < 2) {
      DFSTestUtil.waitNMilliSecond(10);
    }
    // datanode descriptors at standby at this moment should not have any open
    // files
    for (DatanodeDescriptor d : cluster.getStandbyAvatar(0).avatar.namesystem.heartbeats
        .toArray(new DatanodeDescriptor[3])) {
      assertEquals(0, d.getOpenINodes().size());
    }
    
    cluster.shutDownDataNode(0);

    // Now finalize the last block
    // finalized reports from the 2 alive nodes are not processed
    // because of injection 
    out.write(buffer, BLOCK_SIZE / 2, BLOCK_SIZE / 2);
    out.sync();

    // Make sure we receive a RBW report first.
    // this should be added to targets (1 target)
    cluster.getStandbyAvatar(0).avatar.getStandbySafeMode()
        .setSafeModeStateForTesting(SafeModeState.LEAVING_SAFEMODE);
    cluster.restartDataNode(true, 0);
    waitAndVerifyBlocks(1);

    FSNamesystem ns = cluster.getStandbyAvatar(0).avatar.namesystem;
    
    // Now enable finalized block reports and verify counts.
    h.stallBlockReport = false;
    long start = System.currentTimeMillis();
    while (ns.getReportingNodes() != 3 &&
        System.currentTimeMillis() - start < 10000) {
      DFSTestUtil.waitSecond();
    }
    assertEquals(1, ns.getSafeBlocks());
    
    // close the file
    out.close();

    // wait until the file is closed on standby
    start = System.currentTimeMillis();
    while (System.currentTimeMillis() - start < 10000) {
      if (!cluster.getStandbyAvatar(0).avatar.namesystem.dir.getFileINode(
          "/" + name).isUnderConstruction()) {
        break;
      }
      DFSTestUtil.waitNMilliSecond(500);
    }
    
    // check if standby does not have any targets
    // within the datanode descriptors
    for (DatanodeDescriptor d : cluster.getStandbyAvatar(0).avatar.namesystem.heartbeats
        .toArray(new DatanodeDescriptor[3])) {
      assertEquals(0, d.getOpenINodes().size());
    }
  }
}
