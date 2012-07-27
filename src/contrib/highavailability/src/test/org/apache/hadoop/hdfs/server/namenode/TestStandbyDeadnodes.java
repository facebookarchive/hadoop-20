package org.apache.hadoop.hdfs.server.namenode;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.MiniAvatarCluster;

import org.junit.AfterClass;
import static org.junit.Assert.*;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestStandbyDeadnodes {

  private static MiniAvatarCluster cluster;
  private static Configuration conf;
  private static final int recheck = 100;
  private static final int interval = 1000;
  private static final int expire = 2 * recheck + 10 * interval;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    MiniAvatarCluster.createAndStartZooKeeper();
    conf = new Configuration();
    conf.setLong("dfs.heartbeat.interval", interval / 1000);
    conf.setInt("heartbeat.recheck.interval", recheck);
    cluster = new MiniAvatarCluster(conf, 3, true, null, null);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    cluster.shutDown();
    MiniAvatarCluster.shutDownZooKeeper();
  }

  @Test
  public void testDeadDatanode() throws Exception {
    cluster.shutDownDataNodes();
    // Wait for all heartbeats to expire;
    Thread.sleep(2 * expire);
    assertEquals(0,
        cluster.getPrimaryAvatar(0).avatar.namesystem.heartbeats.size());
    assertEquals(0,
        cluster.getStandbyAvatar(0).avatar.namesystem.heartbeats.size());
  }
}
