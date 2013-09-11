package org.apache.hadoop.hdfs.server.namenode;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.MiniAvatarCluster;
import org.apache.hadoop.hdfs.util.InjectionEvent;
import org.apache.hadoop.util.InjectionEventI;
import org.apache.hadoop.util.InjectionHandler;

import org.junit.After;
import org.junit.AfterClass;
import static org.junit.Assert.*;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestStandbyDeadnodes {

  private static MiniAvatarCluster cluster;
  private static Configuration conf;
  private static final int recheck = 100;
  private static final int interval = 1000;
  private static final int expire = 2 * recheck + 10 * interval;
  private static final Log LOG = LogFactory.getLog(TestStandbyDeadnodes.class);
  private static boolean pass = true;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    MiniAvatarCluster.createAndStartZooKeeper();
  }

  @Before
  public void setup() throws Exception {
    pass = true;
    conf = new Configuration();
    conf.setLong("dfs.heartbeat.interval", interval / 1000);
    conf.setInt("heartbeat.recheck.interval", recheck);
    cluster = new MiniAvatarCluster(conf, 3, true, null, null);
    new DFSTestUtil("/test", 1, 1, 1024).createFiles(cluster.getFileSystem(),
        "/test");
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    MiniAvatarCluster.shutDownZooKeeper();
  }

  @After
  public void tearDown() throws Exception {
    InjectionHandler.clear();
    cluster.shutDown();
  }

  private static class TestHandler extends InjectionHandler {

    @Override
    protected void _processEvent(InjectionEventI event, Object... args) {
      if (event == InjectionEvent.STANDBY_ENTER_SAFE_MODE) {
        try {
          long starttime = System.currentTimeMillis();
          FSNamesystem ns = cluster.getStandbyAvatar(0).avatar.namesystem;
          boolean reached = (ns.getSafeBlocks() == ns.getBlocksTotal());
          while (System.currentTimeMillis() - starttime < 30000 && !reached) {
            reached = (ns.getSafeBlocks() == ns.getBlocksTotal());
            LOG.info("Waiting for blocks");
            Thread.sleep(1000);
          }
          if (!reached) {
            throw new Exception("Did not reach threshold");
          }
          for (int i = 0; i < 2; i++) {
            cluster.shutDownDataNode(i);
          }
        } catch (Exception e) {
          pass = false;
          LOG.warn("Shutdown failed : ", e);
        }
      }
    }
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

  @Test
  public void testDeadDatanodeDuringFailover() throws Exception {
    InjectionHandler.set(new TestHandler());
    cluster.failOver();
    assertTrue(pass);
  }
}
