package org.apache.hadoop.hdfs;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

import org.junit.AfterClass;
import static org.junit.Assert.*;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestAvatarContinuousFailover {
  private static MiniAvatarCluster cluster;
  private static Configuration conf;
  private static int BLOCK_SIZE = 1024;
  private static int MAX_BLOCKS = 50;
  private static int MAX_FILE_SIZE = MAX_BLOCKS * BLOCK_SIZE;
  private static int FAILOVERS = 5;
  private static volatile boolean pass = true;
  private static Random random = new Random();
  private static Log LOG = LogFactory
      .getLog(TestAvatarContinuousFailover.class);
  private static int THREADS = 10;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    MiniAvatarCluster.createAndStartZooKeeper();
    conf = new Configuration();
    conf.setInt("dfs.block.size", BLOCK_SIZE);
    conf.setBoolean("fs.ha.retrywrites", true);
    conf.setInt("fs.avatar.failover.checkperiod", 200);
    cluster = new MiniAvatarCluster(conf, 3, true, null, null);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    MiniAvatarCluster.shutDownZooKeeper();
  }

  @Test
  public void testContinuousFailover() throws Exception {
    String topDir = "" + random.nextInt();
    DFSTestUtil util = new DFSTestUtil(topDir, 10, 3, MAX_FILE_SIZE);
    util.createFiles(cluster.getFileSystem(), topDir);
    List<LoadThread> threads = new ArrayList<LoadThread>();
    for (int i = 0; i < THREADS; i++) {
      threads.add(new LoadThread());
    }

    for (int i = 0; i < FAILOVERS; i++) {
      cluster.failOver();
      cluster.restartStandby();
      Thread.sleep(15000);
    }

    for (LoadThread thread : threads) {
      thread.cancel();
    }

    for (LoadThread thread : threads) {
      thread.join();
    }

    assertTrue(pass);
  }

  private static class LoadThread extends Thread {

    private volatile boolean running = true;

    public void cancel() {
      running = false;
    }

    public void run() {
      while (running) {
        try {
          String topDir = "" + random.nextInt();
          DFSTestUtil util = new DFSTestUtil(topDir, 10, 3, MAX_FILE_SIZE);
          util.createFiles(cluster.getFileSystem(), topDir);
          pass = util.checkFiles(cluster.getFileSystem(), topDir);
        } catch (Exception e) {
          LOG.warn("Copy failed : " + e);
          pass = false;
          running = false;
        }
      }
    }
  }

}
