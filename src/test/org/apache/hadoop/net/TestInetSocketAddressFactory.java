package org.apache.hadoop.net;

import junit.framework.TestCase;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Test;

import java.net.InetSocketAddress;
import java.util.concurrent.atomic.*;

public class TestInetSocketAddressFactory extends TestCase {
  private static final Log LOG =
    LogFactory.getLog(TestInetSocketAddressFactory.class);


  public void testBadHost() {
    final AtomicBoolean testResult = new AtomicBoolean(false);

    runWithTimeout(new Runnable() {
      @Override
      public void run() {
        InetSocketAddress socketAddress =
          InetSocketAddressFactory.createWithResolveRetry(
          "_notavalidhost_",
          8080
          );

        testResult.set(socketAddress.isUnresolved());
      }
    }, 20000);

    assertTrue("expected invalid host", testResult.get());
  }

  public void testValidHost() {
    final AtomicBoolean testResult = new AtomicBoolean(false);

    runWithTimeout(new Runnable() {
      @Override
      public void run() {
        InetSocketAddress socketAddress =
          InetSocketAddressFactory.createWithResolveRetry(
          "www.apache.org",
          8080
          );

        testResult.set(!socketAddress.isUnresolved());
      }
    }, 20000);

    assertTrue("expected valid host", testResult.get());
  }

  private static void runWithTimeout(Runnable runnable, long maxRuntimeMillis) {
    Thread t = new Thread(runnable);
    long sleepInterval = Math.min(1000, maxRuntimeMillis);
    long timeSlept = 0;

    t.start();

    while (timeSlept < maxRuntimeMillis &&
      t.getState() != Thread.State.TERMINATED) {
      try {
        Thread.sleep(sleepInterval);

        timeSlept += sleepInterval;
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }

    if (t.getState() != Thread.State.TERMINATED) {
      fail(String.format(
          "test did not complete within the allotted time %d ms",
          maxRuntimeMillis
      ));
    }
  }
}
