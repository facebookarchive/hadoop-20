package org.apache.hadoop.hdfs;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.util.InjectionEvent;
import org.apache.hadoop.util.InjectionEventI;
import org.apache.hadoop.util.InjectionHandler;

import static org.junit.Assert.*;
import org.junit.Test;

public class TestAvatarFailoverCaching extends FailoverLoadTestUtil {

  private static int THREADS = 2;
  private static volatile AtomicInteger check_failovers = new AtomicInteger();

  @Test
  public void testCaching() throws Exception {
    FileSystem fs = cluster.getFileSystem();
    List<LoadThread> threads = new ArrayList<LoadThread>();
    InjectionHandler.set(new TestCacheHandler());
    for (int i = 0; i < THREADS; i++) {
      LoadThread T = new LoadThread(fs);
      T.setName("LoadThread" + i);
      T.start();
      threads.add(T);
    }

    Thread.sleep(3000);
    cluster.failOver();

    for (LoadThread thread : threads) {
      thread.cancel();
    }

    for (LoadThread thread : threads) {
      thread.join();
    }

    LOG.info("GETADDR : " + get_addr + " GETSTAT : " + get_stats);
    assertTrue(pass);
    assertEquals(1, get_stats);
    assertEquals(1, get_addr);
  }

  protected static class TestCacheHandler extends TestHandler {
    @Override
    public void _processEvent(InjectionEventI event, Object... args) {
      super._processEvent(event, args);
      if (event == InjectionEvent.DAFS_CHECK_FAILOVER) {
        check_failovers.incrementAndGet();
        while (check_failovers.get() < 2) {
          try {
            LOG.info("Waiting for check_failovers : " + check_failovers.get());
            Thread.sleep(1000);
          } catch (InterruptedException ie) {
          }
        }
      }
    }
  }
}
