package org.apache.hadoop.mapred;

import java.io.IOException;
import java.net.ConnectException;
import java.net.InetSocketAddress;

import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.mapred.CoronaSessionInfo.InetSocketAddressWritable;

import com.sun.org.apache.commons.logging.Log;
import com.sun.org.apache.commons.logging.LogFactory;

/**
 * Generic caller that tries to reconnect to new job tracker on error in RPC
 * call. New job tracker address is obtained from
 * @param <T> return type of called function
 */
@SuppressWarnings("deprecation")
public abstract class CoronaJTFallbackCaller<T> {
  /** Default wait time between queries to secondary tracker until remote JT is
   * completely restarted, in msec */
  private static final int SECONDARY_TRACKER_QUERIES_INTERVAL = 10000;
  /** Default timeout for connecting to secondary tracker address */
  private static final long SECONDARY_TRACKER_CONNECT_TIMEOUT = 30000;
  /** Max number of times we can ask for new JT address and get response to back
   * off after single crash */
  private static final int SECONDARY_TRACKER_MAX_BACKOFF = 600;
  /** Logger */
  private static final Log LOG = LogFactory
      .getLog(CoronaJTFallbackCaller.class);
  /** Max number the Fallback caller can connect the new address
   *  In some cases, when the Fallback caller get the new job tracker address
   *  and try to connect the new job tracker, it will find the new job tracker get
   *  lost again. So we need to call reconnectToNewJobTracker () recursively, This 
   *  number limit max number we do recursion.
   * */
  private static final int CONNECT_MAX_NUMBER = 8;

  /**
   * Perform the call. Must be overridden by a sub-class.
   * @return The generic return value.
   * @throws IOException
   */
  protected abstract T call() throws IOException;

  /**
   * Prediticate determining if should try again after getting information, that
   * remote JT is during restarting process.
   * @param retryNum numbet of performed retry retry (zeroed after call failure)
   * @return true if should retry
   */
  protected boolean predRetry(int retryNum) {
    return retryNum <= SECONDARY_TRACKER_MAX_BACKOFF;
  }

  /**
   * Provides implementation of wait mechanism between quering secondary tracker
   * for new remote JT address.
   */
  protected void waitRetry() throws InterruptedException {
    synchronized (this) {
      this.wait(SECONDARY_TRACKER_QUERIES_INTERVAL);
    }
  }

  /**
   * Opens client with provided address
   * @param address
   * @throws IOException
   */
  protected abstract void connect(InetSocketAddress address) throws IOException;

  /**
   * Closes RPC client
   */
  protected abstract void shutdown();

  /**
   * Get current RPC address
   * @return current address of RPC clients destination
   */
  protected abstract InetSocketAddress getCurrentClientAddress();

  /**
   * Returns job configuration
   * @return job conf
   */
  protected abstract JobConf getConf();

  /**
   * Gets secondary tracker address
   * @return secondary fallback address
   */
  protected abstract InetSocketAddress getSecondaryTracker();
  
  /**
   * When IO Exception happened, call this function to handle it
   */
  protected abstract void handleIOException(IOException e) throws IOException;

  /**
   * Template function to make the call. Throws if can not fallback.
   * @return The generic return value.
   * @throws IOException
   */
  public final T makeCall() throws IOException {
    while (true) {
      try {
        return call();
      } catch (ConnectException e) {
        // We fall back only after ConnectException
        try {
          // Fall back to secondary tracker and reconnect to new JT
          reconnectToNewJobTracker(0);
        } catch (IOException f) {
          LOG.error("Fallback process failed with ", f);
          // Re-throw original exception
          throw e;
        }
      } catch (IOException e) {
        // the subclass of fallback caller should provide
        // logic here. We will retry in most cases
        handleIOException(e);
      }
    }
  }

  /**
   * Reconnects to new address obtained from secondary address via
   * InterCoronaTrackerProtocol
   * @throws IOException
   */
  private final void reconnectToNewJobTracker(int connectNum) throws IOException {
    if (connectNum >= CONNECT_MAX_NUMBER) {
      LOG.error("reconnectToNewJobTracker has reached its max number.");
      throw new IOException("reconnectToNewJobTracker has reached its max number.");
    }
    
    InetSocketAddress secondaryTracker = getSecondaryTracker();
    JobConf conf = getConf();
    InetSocketAddress oldAddress = getCurrentClientAddress();

    LOG.info("Falling back from " + oldAddress + " to secondary tracker at "
        + secondaryTracker + " with " + connectNum + " try");
    if (secondaryTracker == null)
      throw new IOException("Secondary address not provided.");

    shutdown();
    InterCoronaJobTrackerProtocol secondaryClient = RPC.waitForProxy(
        InterCoronaJobTrackerProtocol.class,
        InterCoronaJobTrackerProtocol.versionID, secondaryTracker, conf,
        SECONDARY_TRACKER_CONNECT_TIMEOUT);
    // Obtain new address
    InetSocketAddressWritable oldAddrWritable = new InetSocketAddressWritable(
        oldAddress);
    InetSocketAddressWritable newAddress = null;
    int retryNum = 0;
    do {
      newAddress = secondaryClient.getNewJobTrackerAddress(oldAddrWritable);
      try {
        waitRetry();
      } catch (InterruptedException e) {
        LOG.error("Fallback interrupted, taking next retry.");
      }
      ++retryNum;
    } while (newAddress == null && predRetry(retryNum));

    if (newAddress == null || newAddress.getAddress() == null)
      throw new IOException("Failed to obtain new job tracker address.");

    RPC.stopProxy(secondaryClient);
    try {
      connect(newAddress.getAddress());
      LOG.info("Fallback process successful: " + newAddress.getAddress());
    } catch (IOException e) {
      LOG.error("Fallback connect to " + newAddress.getAddress() + " failed for ", e);
      reconnectToNewJobTracker(++connectNum);
    }
  }

}
