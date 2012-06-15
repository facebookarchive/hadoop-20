package org.apache.hadoop.hdfs;

import java.io.IOException;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.server.namenode.AvatarNode;
import org.apache.hadoop.hdfs.server.namenode.ZookeeperTxId;
import org.apache.hadoop.hdfs.util.InjectionEvent;
import org.apache.hadoop.hdfs.util.InjectionHandler;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.zookeeper.KeeperException.NoNodeException;

import org.junit.After;
import static org.junit.Assert.*;
import org.junit.Before;
import org.junit.Test;

public class TestAvatarSyncLastTxid {

  private static MiniAvatarCluster cluster;
  private static Configuration conf;
  private static FileSystem fs;
  private static Random random = new Random();
  private static AvatarZooKeeperClient zkClient;
  private static Log LOG = LogFactory.getLog(TestAvatarSyncLastTxid.class);

  @Before
  public void setUp() throws Exception {
    MiniAvatarCluster.createAndStartZooKeeper();
    conf = new Configuration();
    conf.setBoolean("fs.ha.retrywrites", true);
    conf.setInt("dfs.block.size", 1024);
    cluster = new MiniAvatarCluster(conf, 3, true, null, null);
    fs = cluster.getFileSystem();
    zkClient = new AvatarZooKeeperClient(conf, null);
  }

  @After
  public void tearDown() throws Exception {
    zkClient.shutdown();
    cluster.shutDown();
    MiniAvatarCluster.clearZooKeeperData();
    MiniAvatarCluster.shutDownZooKeeper();
  }

  private void createEditsNotSynced(int nEdits) throws IOException {
    for (int i = 0; i < nEdits; i++) {
      // This creates unsynced edits by calling generation stamp.
      cluster.getPrimaryAvatar(0).avatar.namesystem
          .nextGenerationStampForTesting();
    }
  }

  private void createEdits(int nEdits) throws IOException {
    for (int i = 0; i < nEdits / 2; i++) {
      // Create file ends up logging two edits to the edit log, one for create
      // file and one for bumping up the generation stamp
      fs.create(new Path("/" + random.nextInt()));
    }
  }

  private ZookeeperTxId getLastTxid() throws Exception {
    AvatarNode primaryAvatar = cluster.getPrimaryAvatar(0).avatar;
    String address = AvatarNode.getClusterAddress(primaryAvatar
        .getStartupConf());
    return zkClient.getPrimaryLastTxId(address);
  }

  private long getSessionId() throws Exception {
    AvatarNode primaryAvatar = cluster.getPrimaryAvatar(0).avatar;
    String address = AvatarNode.getClusterAddress(primaryAvatar
        .getStartupConf());
    return zkClient.getPrimarySsId(address);
  }

  private void verifyState(long expectedTxid) throws Exception {
    ZookeeperTxId lastTxId = getLastTxid();
    assertNotNull(lastTxId);
    long sessionId = getSessionId();
    assertEquals(sessionId, lastTxId.getSessionId());
    assertEquals(expectedTxid - 1, lastTxId.getTransactionId());
  }

  @Test
  public void testBasic() throws Exception {
    createEdits(20);
    // fs.close() creates 10 more edits due to close file ops.
    fs.close();
    cluster.shutDown();
    verifyState(30);
  }

  @Test
  public void testPrimaryCrash() throws Exception {
    createEdits(20);
    fs.close();
    InjectionHandler.set(new TestAvatarSyncLastTxidInjectionHandler());
    cluster.getPrimaryAvatar(0).avatar.shutdownAvatar();
    InjectionHandler.clear();
    ZookeeperTxId lastTxId = null;
    try {
      lastTxId = getLastTxid();
    } catch (NoNodeException e) {
      LOG.info("Expected exception", e);
      assertNull(lastTxId);
      return;
    }
    fail("Did not throw : " + NoNodeException.class);
  }

  @Test
  public void testPrimaryCrashWithExistingZkNode() throws Exception {
    // First do a clean shutdown.
    createEdits(20);
    // fs.close() creates 10 more edits due to close file ops.
    fs.close();
    cluster.shutDown();
    verifyState(30);

    // Now we have an existing znode with the last transaction id, now do an
    // unclean shutdown and verify session ids don't match.
    cluster = new MiniAvatarCluster(conf, 3, true, null, null);
    fs = cluster.getFileSystem();
    createEdits(20);
    fs.close();
    InjectionHandler.set(new TestAvatarSyncLastTxidInjectionHandler());
    cluster.getPrimaryAvatar(0).avatar.shutdownAvatar();
    InjectionHandler.clear();
    ZookeeperTxId lastTxId = null;
    try {
      lastTxId = getLastTxid();
    } catch (NoNodeException e) {
      LOG.info("Expected exception", e);
      assertNull(lastTxId);
      return;
    }
    fail("Did not throw : " + NoNodeException.class);
  }

  @Test
  public void testWithFailover() throws Exception {
    createEdits(20);
    cluster.failOver();
    createEdits(20);
    // 20 edits since we are closing 20 files.
    fs.close();
  }

  @Test
  public void testWithDoubleFailover() throws Exception {
    // Perform first failover
    createEdits(20);
    cluster.failOver();
    createEdits(20);
    cluster.restartStandby();

    // Perform second failover.
    createEdits(20);
    cluster.failOver();
    createEdits(20);

    // 40 edits since we are closing 40 files.
    fs.close();
  }

  @Test
  public void testFailoverWithPrimaryCrash() throws Exception {
    createEdits(20);
    fs.close();
    
    InjectionHandler.set(new TestAvatarSyncLastTxidInjectionHandler());
    cluster.killPrimary(0, true);
    InjectionHandler.clear();
    
    try {
      cluster.failOver();
    } catch (Exception e) {
      LOG.info("Expected exception", e);
      return;
    }
    fail("Did not throw exception");
  }

  @Test
  public void testDoubleFailoverWithPrimaryCrash() throws Exception {
    // First failover
    createEdits(20);
    cluster.failOver();
    cluster.restartStandby();
    createEdits(20);
    fs.close();

    // Second failover.
    InjectionHandler.set(new TestAvatarSyncLastTxidInjectionHandler());
    cluster.killPrimary(0, true);
    InjectionHandler.clear();
    
    try {
      cluster.failOver();
    } catch (Exception e) {
      LOG.info("Expected exception : ", e);
      return;
    }
    fail("Did not throw : " + NoNodeException.class);
  }

  @Test
  public void testFailoverAfterUnsuccessfulFailover() throws Exception {
    createEdits(20);
    fs.close();
    
    InjectionHandler.set(new TestAvatarSyncLastTxidInjectionHandler());
    cluster.killPrimary(0, true);
    InjectionHandler.clear();
    
    try {
      cluster.failOver();
    } catch (Exception e) {
      LOG.info("Expected exception : ", e);
      // Now restart avatarnodes and do a clean failover.
      cluster.restartAvatarNodes();
      fs = cluster.getFileSystem();
      createEdits(20);
      cluster.failOver();
      return;
    }
    fail("Did not throw exception");
  }

  @Test
  public void testEditLogCrash() throws Exception {
    cluster.shutDown();
    conf.setBoolean("dfs.simulate.editlog.crash", true);
    conf.setBoolean("fs.ha.retrywrites", true);
    cluster = new MiniAvatarCluster(conf, 3, true, null, null);
    fs = cluster.getFileSystem();
    zkClient = new AvatarZooKeeperClient(conf, null);
    createEdits(20);
    // These 20 edits should not be persisted, since edit log is going to crash.
    createEditsNotSynced(20);
    try {
      cluster.failOver();
    } catch (Exception e) {
      LOG.info("Expected exception : ", e);
      assertEquals(19, cluster.getStandbyAvatar(0).avatar.getLastWrittenTxId());
      return;
    }
    fail("Did not throw exception");
  }
  
  @Test
  public void testBlocksMisMatch() throws Exception {
    int totalBlocks = 50;
    DFSTestUtil.createFile(fs, new Path("/testBlocksMisMatch"),
        (long) totalBlocks * 1024, (short) 3, System.currentTimeMillis());
    AvatarNode standby = cluster.getStandbyAvatar(0).avatar;
    standby.quiesceStandby(cluster
        .getPrimaryAvatar(0).avatar.getLastWrittenTxId());
    // This sets the standby number of blocks to 0.
    standby.namesystem.close();
    try {
      cluster.failOver();
    } catch (Exception e) {
      LOG.info("Expected exception : ", e);
      return;
    }
    fail("Did not throw exception");
  }
  
  class TestAvatarSyncLastTxidInjectionHandler extends InjectionHandler {

    public boolean _falseCondition(InjectionEvent event, Object... args) {
      if (event == InjectionEvent.AVATARNODE_SHUTDOWN) {
        // used to simulate a situation where zk 
        // is not updated at primary shutdown
        LOG.info("Skipping the write to ZK");
        return true;
      }
      return false;
    }
  }
}
