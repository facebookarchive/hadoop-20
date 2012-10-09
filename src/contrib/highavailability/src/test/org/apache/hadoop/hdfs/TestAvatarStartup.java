package org.apache.hadoop.hdfs;

import java.io.IOException;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.MiniAvatarCluster.NameNodeInfo;
import org.apache.hadoop.hdfs.protocol.AvatarConstants;
import org.apache.hadoop.hdfs.protocol.AvatarConstants.StartupOption;
import org.apache.hadoop.hdfs.server.namenode.AvatarNode;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import static org.junit.Assert.*;
import org.junit.Test;

public class TestAvatarStartup extends FailoverLoadTestUtil {
  private static MiniAvatarCluster cluster;
  private static Configuration conf;
  private static AvatarZooKeeperClient zkClient;
  private static final Random r = new Random();
  private static final Log LOG = LogFactory
      .getLog(TestAvatarStartup.class);

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    MiniAvatarCluster.createAndStartZooKeeper();
  }

  public void setUp(boolean federation, String testName) throws Exception {
    setUp(federation, testName, false);
  }

  public void setUp(boolean federation, String testName, boolean retrywrites)
      throws Exception {
    LOG.info("------------------- test: " + testName
        + " START ----------------");
    conf = new Configuration();
    conf.setBoolean("fs.ha.retrywrites", retrywrites);
    if (federation) {
      cluster = new MiniAvatarCluster(conf, 1, true, null, null,
          2 + r.nextInt(4), true);
    } else {
      cluster = new MiniAvatarCluster(conf, 1, true, null, null);
    }
    zkClient = new AvatarZooKeeperClient(conf, null);
  }

  @After
  public void tearDown() throws Exception {
    zkClient.shutdown();
    cluster.shutDown();
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    MiniAvatarCluster.shutDownZooKeeper();
  }

  private long getSessionId(int index) throws Exception {
    AvatarNode primaryAvatar = cluster.getPrimaryAvatar(index).avatar;
    String address = AvatarNode.getClusterAddress(primaryAvatar
        .getStartupConf());
    return zkClient.getPrimarySsId(address);
  }

  private void verifyStartup(boolean federation, int index,
      boolean singleStartup, boolean standby) throws Exception {
    verifyStartup(federation, index, singleStartup, standby, false);
  }

  private void verifyStartup(boolean federation, int index,
      boolean singleStartup, boolean standby, boolean forceStartup)
      throws Exception {
    MiniAvatarCluster.instantiationRetries = 2;
    NameNodeInfo nnInfo = cluster.getNameNode(index);
    String instance = (!standby) ? AvatarConstants.StartupOption.NODEZERO
        .getName() : AvatarConstants.StartupOption.NODEONE.getName();
    String force = (forceStartup) ? AvatarConstants.StartupOption.FORCE
        .getName() : instance;
    String[] primaryArgs = { instance, force };
    String[] standbyArgs = { instance, force,
        AvatarConstants.StartupOption.STANDBY.getName() };
    String[] normalArgs = (!standby) ? primaryArgs : standbyArgs;
    String[] federationPrimaryArgs = { instance, force,
        StartupOption.SERVICE.getName(), nnInfo.nameserviceId };
    String[] federationStandbyArgs = { instance, force,
        StartupOption.SERVICE.getName(), nnInfo.nameserviceId,
        AvatarConstants.StartupOption.STANDBY.getName() };
    String[] federationArgs = (!standby) ? federationPrimaryArgs
        : federationStandbyArgs;
    String[] args = (federation) ? federationArgs : normalArgs;
    AvatarNode primary1 = MiniAvatarCluster.instantiateAvatarNode(
        args,
        MiniAvatarCluster.getServerConf(instance, nnInfo));
    if (singleStartup) {
      if (!standby) {
        assertEquals(primary1.getSessionId(), getSessionId(index));
      }
      return;
    }
    try {      
      MiniAvatarCluster.instantiateAvatarNode(args, MiniAvatarCluster.getServerConf(
              instance, nnInfo));
      fail("Did not throw exception");
    } catch (Exception e) {
      LOG.info("Expected exception : ", e);
      if (!standby) {
        assertEquals(primary1.getSessionId(), getSessionId(index));
      }
    }
  }

  @Test
  public void testStartup() throws Exception {
    setUp(false, "testStartup");
    cluster.shutDownAvatarNodes();
    int nameNodes = cluster.getNumNameNodes();
    for (int i = 0; i < nameNodes; i++) {
      verifyStartup(false, i, false, false);
    }
  }

  @Test
  public void testStartupFederation() throws Exception {
    setUp(true, "testStartupFederation");
    cluster.shutDownAvatarNodes();
    int nameNodes = cluster.getNumNameNodes();
    for (int i = 0; i < nameNodes; i++) {
      verifyStartup(true, i, false, false);
    }
  }

  @Test(expected = IOException.class)
  public void testNullZKStartupPrimary() throws Exception {
    setUp(false, "testNullZKStartupPrimary");
    cluster.shutDownAvatarNodes();
    cluster.clearZooKeeperNode(0);
    verifyStartup(false, 0, true, false);
  }

  @Test(expected = IOException.class)
  public void testNullZKStartupStandby() throws Exception {
    setUp(false, "testNullZKStartupStandby");
    cluster.shutDownAvatarNodes();
    cluster.clearZooKeeperNode(0);
    verifyStartup(false, 0, true, true);
  }

  @Test()
  public void testNullZKStartupForce() throws Exception {
    setUp(false, "testNullZKStartupForce");
    cluster.shutDownAvatarNodes();
    cluster.clearZooKeeperNode(0);
    verifyStartup(false, 0, true, false, true);
    verifyStartup(false, 0, true, true, true);
  }

  @Test()
  public void testNullZKStartupForceWithLoad() throws Exception {
    setUp(false, "testNullZKStartupForceWithLoad", true);
    LoadThread loadThread = new LoadThread(cluster.getFileSystem());
    loadThread.start();
    Thread.sleep(3000);
    cluster.shutDownAvatarNodes();
    cluster.clearZooKeeperNode(0);
    verifyStartup(false, 0, true, false, true);
    verifyStartup(false, 0, true, true, true);
    cluster.registerZooKeeperNodes();
    Thread.sleep(3000);
    loadThread.cancel();
    loadThread.join(30000);
    assertTrue(pass);
  }
}
