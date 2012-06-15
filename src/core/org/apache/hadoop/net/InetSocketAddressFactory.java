package org.apache.hadoop.net;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.net.InetSocketAddress;

public class InetSocketAddressFactory {
  private static final Log LOG = LogFactory.getLog(InetSocketAddressFactory.class);
  private static final int DEFAULT_DELAY_MILLIS = 100;
  private static final int DEFAULT_MAX_ATTEMPTS = 2;

  /**
   * Utility function to create an InetSocketAddress that has been resolved.
   * Retries once and sleeps 100ms between the failure and the retry
   *
   * @param hostname
   * @param port
   * @return InetSocketAddress, may be unresolved if both attempts fail
   */
  public static InetSocketAddress createWithResolveRetry(
    String hostname,
    int port
  ) {
    return createWithResolveRetry(
      hostname,
      port,
      DEFAULT_DELAY_MILLIS,
      DEFAULT_MAX_ATTEMPTS
    );
  }

  /**
   * Utility function to create an InetSocketAddress that has been resolved.
   * Retries with a small sleep in between
   *
   * @param hostname - host
   * @param port - port
   * @param delayMillis - millis to sleep between attempts
   * @param maxAttempt - max total attempts to retry
   * @return InetSocketAddress, may be unresolved if all attempts fail
   */
  public static InetSocketAddress createWithResolveRetry(
    String hostname,
    int port,
    int delayMillis,
    int maxAttempt
  ) {
    InetSocketAddress socketAddress;
    int attempts = 0;

    do {
      socketAddress = new InetSocketAddress(hostname, port);
      // if dns failed, try one more time
      if (socketAddress.isUnresolved()) {
        attempts++;
        LOG.info(String.format(
          "failed to resolve host %s, attempt %d",
          hostname,
          attempts
        ));

        try {
          Thread.sleep(delayMillis);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      } else if (attempts > 0) {
        LOG.info(
          String.format("successful resolution on attempt %d", attempts)
        );
      }
    } while (socketAddress.isUnresolved() && attempts < maxAttempt);

    return socketAddress;
  }
}
