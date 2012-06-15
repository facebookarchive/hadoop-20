package org.apache.hadoop.hdfs;

import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.server.datanode.AvatarDataNode;
import org.apache.hadoop.hdfs.server.datanode.AvatarDataNode.ServicePair;
import org.apache.hadoop.hdfs.server.datanode.NamespaceService;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.util.InjectionEvent;
import org.apache.hadoop.hdfs.util.InjectionHandler;
import org.apache.hadoop.hdfs.protocol.FSConstants.DatanodeReportType;
import org.apache.hadoop.hdfs.server.namenode.LeaseManager;
import org.apache.hadoop.hdfs.server.namenode.StandbySafeMode;

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
  private static final int LEASE_PERIOD = 10000;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    MiniAvatarCluster.createAndStartZooKeeper();
  }

  public void setUp(boolean shortlease) throws Exception {
    setUp(shortlease, true);
  }

  public void setUp(boolean shortlease, boolean shortFBR) throws Exception {
    h = new TestStandbySafeModeHandler();
    InjectionHandler.set(h);
    conf = new Configuration();
    conf.setInt("dfs.block.size", BLOCK_SIZE);
    conf.setFloat("dfs.namenode.replqueue.threshold-pct", 0.1f);
    conf.setInt("dfs.datanode.blockreceived.retry.internval", 200);
    conf.setInt("dfs.heartbeat.interval", 1);
    if (shortFBR) {
      conf.setInt("dfs.datanode.fullblockreport.delay", 1000);
    }
    if (shortlease) {
      conf.setInt("dfs.softlease.period", LEASE_PERIOD);
      conf.setBoolean("fs.ha.retrywrites", true);
      conf.setInt("fs.avatar.failover.checkperiod", 200);
    }
    cluster = new MiniAvatarCluster(conf, 3, true, null, null);
    fs = cluster.getFileSystem();
    pass = true;
  }

  @After
  public void tearDown() throws Exception {
    cluster.shutDown();
    InjectionHandler.clear();
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    MiniAvatarCluster.shutDownZooKeeper();
  }

  private void waitAndVerifyBlocks() throws Exception {
    FSNamesystem standbyNS = cluster.getStandbyAvatar(0).avatar.namesystem;
    FSNamesystem primaryNS = cluster.getPrimaryAvatar(0).avatar.namesystem;
    assertTrue(standbyNS.isInSafeMode());
    long primaryBlocks = primaryNS.getBlocksTotal();
    long standbyBlocks = standbyNS.getBlocksTotal();
    long start = System.currentTimeMillis();
    long standbySafeBlocks = 0;

    // Wait for standby safe mode to catch up to all blocks.
    while (System.currentTimeMillis() - start <= MAX_WAIT_TIME
        && (primaryBlocks != standbyBlocks || standbySafeBlocks != primaryBlocks)) {
      primaryBlocks = primaryNS.getBlocksTotal();
      standbyBlocks = standbyNS.getBlocksTotal();
      standbySafeBlocks = standbyNS.getSafeBlocks();
      Thread.sleep(1000);
    }

    // Final verification of block counts.
    assertEquals(primaryBlocks, standbyBlocks);
    assertEquals(primaryBlocks, standbySafeBlocks);
  }

  private void createTestFiles(String topDir) throws Exception {
    DFSTestUtil util = new DFSTestUtil(topDir, 10, 1, MAX_FILE_SIZE);
    util.createFiles(fs, topDir);
    util.checkFiles(fs, topDir);
  }

  @Test
  public void testStandbyFullBlockReport() throws Exception {
    setUp(false);
    h.setIgnoreDatanodes(false);
    // Create test files.
    createTestFiles("/testStandbySafeMode");
    LOG.info("before restart");
    cluster.killStandby();
    cluster.restartStandby();
    LOG.info("after restart");

    long lastTxid = cluster.getPrimaryAvatar(0).avatar.getLastWrittenTxId();

    // Need to quiesce to ingest edits.new since checkpointing is disabled.
    cluster.getStandbyAvatar(0).avatar.quiesceStandby(lastTxid);

    waitAndVerifyBlocks();
  }

  @Test
  public void testStandbySafeMode() throws Exception {
    setUp(false);
    h.setIgnoreDatanodes(false);
    // Create test files.
    createTestFiles("/testStandbySafeMode");

    // Sync all data to the edit log.
    long lastTxid = cluster.getPrimaryAvatar(0).avatar.getLastWrittenTxId();
    cluster.getPrimaryAvatar(0).avatar.getFSImage().getEditLog().logSync();

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
    setUp(false);
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
      Thread.sleep(1000);
    }

    // All blocks on the datanode should be deleted.
    assertEquals(0, getDatanodeBlocks());
  }

  @Test
  public void testClearDatanodeRetryList() throws Exception {
    setUp(false);
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
    
    public void setIgnoreDatanodes(boolean v) {
      ignoreDatanodes = v;
    }
    
    @Override
    protected boolean _falseCondition(InjectionEvent event, Object... args) {
      if (event == InjectionEvent.STANDBY_FELL_BEHIND) {
        return ignoreDatanodes;
      }
      if (event == InjectionEvent.OFFERSERVICE_SCHEDULE_HEARTBEAT) {
        return forceheartbeat;
      }
      if (event == InjectionEvent.OFFERSERVICE_SCHEDULE_BR) {
        return forceblockreport;
      }
      return false;
    }

    @Override
    protected void _processEvent(InjectionEvent event, Object... args) {
      if (event == InjectionEvent.INGEST_BEFORE_LOAD_EDIT) {
        while (stallIngest) {
          try {
            Thread.sleep(1000);
          } catch (InterruptedException ie) {

          }
        }
      }
    }
  }

  @Test
  public void testDeadDatanodeFailover() throws Exception {
    setUp(false);
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

    public ExitSafeMode(StandbySafeMode safeMode, FSNamesystem namesystem) {
      this.safeMode = safeMode;
      this.namesystem = namesystem;
    }

    public void run() {
     try {
        while (true) {
          if (safeMode.failoverInProgress()
              && safeMode.getOutStandingHeartbeats().size() == 1
              && safeMode.getOutStandingReports().size() == 0
              && namesystem.getBlocksTotal() == namesystem.getSafeBlocks()) {
            safeMode.leave(false);
            break;
          }
          safeMode.canLeave();
          Thread.sleep(1000);
        }
     } catch (Exception e) {
        LOG.info("SafeMode exit failed", e);
        pass = false;
     }
    }
  }

  @Test
  public void testLeaseExpiry() throws Exception {
    setUp(true);
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
    ((DistributedFileSystem)fs).getClient().leasechecker.renew();
    LOG.info("Lease renewal done");

    // Wait to see whether lease expires.
    long start = System.currentTimeMillis();
    while (System.currentTimeMillis() - start < MAX_WAIT_TIME
        && leaseManager.getLeaseByPath(src) != null) {
      Thread.sleep(1000);
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
    setUp(false, false);
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

    // Wait for incremental block reports.
    Thread.sleep(10000);

    // Restart ingest.
    h.stallIngest = false;

    // Wait for ingest.
    blocksEqual();

    waitAndVerifyBlocks();
  }

  @Test
  public void testStandbySafeModeDel1() throws Exception {
    setUp(false, false);
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
}
