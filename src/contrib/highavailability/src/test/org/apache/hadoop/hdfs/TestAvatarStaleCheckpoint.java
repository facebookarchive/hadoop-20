package org.apache.hadoop.hdfs;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.util.InjectionEvent;
import org.apache.hadoop.hdfs.util.InjectionHandler;

import org.junit.AfterClass;
import static org.junit.Assert.*;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestAvatarStaleCheckpoint {
  private static Configuration conf;
  private static MiniAvatarCluster cluster;
  private static boolean pass = false;
  private static boolean done = false;
  private static boolean staleCheckpoint = false;
  private static Log LOG = LogFactory.getLog(TestAvatarStaleCheckpoint.class);

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    MiniAvatarCluster.createAndStartZooKeeper();
    conf = new Configuration();
    conf.setBoolean("fs.checkpoint.enabled", true);
    conf.setLong("fs.checkpoint.period", 1);
    cluster = new MiniAvatarCluster(conf, 3, true, null, null);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    MiniAvatarCluster.shutDownZooKeeper();
  }

  private static class FailoverThread extends Thread {

    public void run() {
      try {
        cluster.failOver(true);
        pass = true;
      } catch (Throwable e) {
        pass = false;
        LOG.warn("Failover failed", e);
      } finally {
        done = true;
      }
    }
  }

  private static class TestHandler extends InjectionHandler {
    protected void _processEvent(InjectionEvent event, Object... args) {
      if (event == InjectionEvent.STANDBY_AFTER_DO_CHECKPOINT) {
        new FailoverThread().start();
        long start = System.currentTimeMillis();
        while (System.currentTimeMillis() - start <= 30000 && !staleCheckpoint) {
          try {
            Thread.sleep(1000);
          } catch (InterruptedException e) {

          }
        }
      } else if (event == InjectionEvent.STANDBY_AFTER_DO_CHECKPOINT) {
        staleCheckpoint = true;
      }
    }
  }

  @Test
  public void testStaleCheckpoint() throws Exception {
    long start = System.currentTimeMillis();
    DFSTestUtil.createFile(cluster.getFileSystem(), new Path("/abc"),
        (long) 1024, (short) 3, 0);
    InjectionHandler.set(new TestHandler());
    while (System.currentTimeMillis() - start <= 120000 && !done) {
      Thread.sleep(1000);
    }
    assertTrue(done);
    assertTrue(pass);
  }
}
