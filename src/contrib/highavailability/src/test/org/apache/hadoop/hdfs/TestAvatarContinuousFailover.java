package org.apache.hadoop.hdfs;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hdfs.util.InjectionHandler;

import static org.junit.Assert.*;
import org.junit.Test;

public class TestAvatarContinuousFailover extends FailoverLoadTestUtil {
  private static int FAILOVERS = 3;
  private static int THREADS = 5;

  @Test
  public void testContinuousFailover() throws Exception {
    List<LoadThread> threads = new ArrayList<LoadThread>();
    InjectionHandler.set(new TestHandler());
    for (int i = 0; i < THREADS; i++) {
      LoadThread T = new LoadThread();
      T.start();
      threads.add(T);
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

    LOG.info("GETADDR : " + get_addr + " GETSTAT : " + get_stats);
    assertTrue(FAILOVERS >= get_addr);
    assertTrue(FAILOVERS >= get_stats);
    assertTrue(pass);
  }
}
