package org.apache.hadoop.hdfs.server.balancer;

import java.io.*;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DistributedAvatarFileSystem;
import org.apache.hadoop.hdfs.MiniAvatarCluster;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.FSConstants.DatanodeReportType;
import org.apache.hadoop.hdfs.server.balancer.AvatarBalancer;

import org.junit.After;
import org.junit.AfterClass;
import static org.junit.Assert.*;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestAvatarBalancer {
  private static MiniAvatarCluster cluster;
  private static FileSystem fs;
  private static Configuration conf;
  private static int BLOCK_SIZE = 1024;
  private static int TOTAL_BLOCKS = 20;
  private static int MAX_FILE_SIZE = BLOCK_SIZE * TOTAL_BLOCKS;
  private static int MAX_FILES = 5;
  private static int CAPACITY = MAX_FILES * MAX_FILE_SIZE;
  private static boolean pass = true;
  private static Log LOG = LogFactory.getLog(TestAvatarBalancer.class);

  @Before
  public void setUp() throws Exception {
    conf = new Configuration();
    conf.setInt("dfs.block.size", 1024);
    conf.setLong("dfs.heartbeat.interval", 1L);
    conf.setClass("dfs.balancer.impl", AvatarBalancer.class, Balancer.class);
    cluster = new MiniAvatarCluster(conf, 2, true, null, null, 1, false,
        new long[] { CAPACITY, CAPACITY });
    conf.setLong("dfs.balancer.movedWinWidth", 2000L);
    fs = cluster.getFileSystem();
    new DFSTestUtil("/testBasic", MAX_FILES, 5, MAX_FILE_SIZE).createFiles(fs,
        "/", (short) 2);
    Balancer.setBlockMoveWaitTime(1000);
    pass = true;
  }

  @After
  public void tearDown() throws Exception {
    cluster.shutDown();
  }

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    MiniAvatarCluster.createAndStartZooKeeper();
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    MiniAvatarCluster.shutDownZooKeeper();
  }

  public void waitForHeartbeats() throws Exception {
    DatanodeInfo[] dns = cluster.getPrimaryAvatar(0).avatar
        .getDatanodeReport(DatanodeReportType.ALL);
    while (true) {
      int count = 0;
      for (DatanodeInfo dn : dns) {
        if (dn.getRemaining() == 5 * MAX_FILE_SIZE || dn.getRemaining() == 0) {
          LOG.info("Bad dn : " + dn.getName() + " remaining : "
              + dn.getRemaining());
          count++;
        }
      }
      dns = cluster.getPrimaryAvatar(0).avatar
        .getDatanodeReport(DatanodeReportType.ALL);
      if (count == 1)
        break;
      LOG.info("Waiting for heartbeats");
      Thread.sleep(1000);
    }
  }

  private static void runBalancer() throws Exception {
    Configuration bconf = new Configuration(conf);
    bconf.setClass("fs.hdfs.impl", DistributedAvatarFileSystem.class,
        FileSystem.class);
    Balancer b = new AvatarBalancer(bconf);
    assertEquals(0, b.run(new String[] { "-threshold", "1" }));
  }

  @Test
  public void testBasic() throws Exception {
    cluster.startDataNodes(new long[] { CAPACITY }, 1, null, null, conf);
    cluster.waitDataNodesActive();
    waitForHeartbeats();
    runBalancer();
  }

  @Test
  public void testAfterFailover() throws Exception {
    cluster.startDataNodes(new long[] { CAPACITY }, 1, null, null, conf);
    cluster.waitDataNodesActive();
    cluster.failOver();
    waitForHeartbeats();
    runBalancer();
  }

  private static class BalancerThread extends Thread {
    public void run() {
      try {
        runBalancer();
      } catch (Throwable e) {
        LOG.error("Balancer failed : ", e);
        pass = false;
      }
    }
  }

  @Test
  public void testDuringFailover() throws Exception {
    cluster.startDataNodes(new long[] { CAPACITY }, 1, null, null, conf);
    cluster.waitDataNodesActive();
    waitForHeartbeats();
    Thread t = new BalancerThread();
    t.start();
    cluster.failOver();
    t.join();
    assertTrue(pass);
  }

  @Test
  public void testAfterFailoverShell() throws Exception {
    cluster.startDataNodes(new long[] { CAPACITY }, 1, null, null, conf);
    cluster.waitDataNodesActive();
    cluster.failOver();
    waitForHeartbeats();
    File confFile = new File(new File(MiniAvatarCluster.TEST_DIR).getParent(),
        "core-site.xml");
    conf.unset("dfs.balancer.impl");
    FileOutputStream out = new FileOutputStream(confFile);
    conf.writeXml(out);
    out.close();
    confFile.deleteOnExit();
    assertEquals(0,
        AvatarBalancer.runBalancer(new String[] { "-threshold", "1" }));
  }
}
