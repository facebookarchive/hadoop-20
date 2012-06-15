package org.apache.hadoop.hdfs;

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

public class TestAvatarMultipleStartup {
  private static MiniAvatarCluster cluster;
  private static Configuration conf;
  private static AvatarZooKeeperClient zkClient;
  private static final Random r = new Random();
  private static final Log LOG = LogFactory
      .getLog(TestAvatarMultipleStartup.class);

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    MiniAvatarCluster.createAndStartZooKeeper();
  }

  public void setUp(boolean federation) throws Exception {
    conf = new Configuration();
    conf.setBoolean("dfs.avatarnode.startup.testing", true);
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

  private void verifyStartup(boolean federation, int index)
      throws Exception {
    NameNodeInfo nnInfo = cluster.getNameNode(index);
    String[] normalArgs = { AvatarConstants.StartupOption.NODEZERO.getName() };
    String[] federationArgs = {
        AvatarConstants.StartupOption.NODEZERO.getName(),
        StartupOption.SERVICE.getName(), nnInfo.nameserviceId };
    String[] args = (federation) ? federationArgs : normalArgs;
    AvatarNode primary1 = AvatarNode.createAvatarNode(
        args,
        MiniAvatarCluster.getServerConf(
            AvatarConstants.StartupOption.NODEZERO.getName(), nnInfo));
    try {
      AvatarNode.createAvatarNode(args, MiniAvatarCluster.getServerConf(
              AvatarConstants.StartupOption.NODEONE.getName(), nnInfo));
      fail("Did not throw exception");
    } catch (Exception e) {
      LOG.info("Expected exception : ", e);
      assertEquals(primary1.getSessionId(), getSessionId(index));
    }
  }

  @Test
  public void testStartup() throws Exception {
    setUp(false);
    cluster.shutDownAvatarNodes();
    int nameNodes = cluster.getNumNameNodes();
    for (int i = 0; i < nameNodes; i++) {
      verifyStartup(false, i);
    }
  }

  @Test
  public void testStartupFederation() throws Exception {
    setUp(true);
    cluster.shutDownAvatarNodes();
    int nameNodes = cluster.getNumNameNodes();
    for (int i = 0; i < nameNodes; i++) {
      verifyStartup(true, i);
    }
  }
}
