package org.apache.hadoop.hdfs;

import java.io.IOException;
import java.net.URI;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.util.InjectionEvent;
import org.apache.hadoop.hdfs.util.InjectionHandler;
import org.apache.hadoop.ipc.VersionedProtocol;
import org.apache.zookeeper.data.Stat;

/**
 * Handles failovers for clients talking to a namenode.
 */
public class FailoverClientHandler {

  CachingAvatarZooKeeperClient zk;
  /*
   * ReadLock is acquired by the clients performing operations WriteLock is
   * acquired when we need to failover and modify the proxy. Read and write
   * because of read and write access to the namenode object.
   */
  ReentrantReadWriteLock fsLock = new ReentrantReadWriteLock(true);

  /**
   * The full address of the node in ZooKeeper
   */
  long lastPrimaryUpdate = 0;

  // Should DAFS retry write operations on failures or not
  boolean alwaysRetryWrites;
  // Indicates whether subscription model is used for ZK communication
  boolean watchZK;

  private int failoverCheckPeriod;

  VersionedProtocol namenode;

  // Will try for two minutes checking with ZK every 15 seconds
  // to see if the failover has happened in pull case
  // and just wait for two minutes in watch case
  public static final int FAILOVER_CHECK_PERIOD = 15000;
  public static final int FAILOVER_RETRIES = 8;
  // Tolerate up to 5 retries connecting to the NameNode
  private static final int FAILURE_RETRY = 5;

  private static final Log LOG = LogFactory.getLog(FailoverClientHandler.class);
  private final URI logicalName;
  private final FailoverClient failoverClient;

  static {
    Configuration.addDefaultResource("avatar-default.xml");
    Configuration.addDefaultResource("avatar-site.xml");
  }

  public FailoverClientHandler(Configuration conf, URI logicalName,
      FailoverClient failoverClient) {
    /*
     * If false - on Mutable call to the namenode we fail If true we try to make
     * the call go through by resolving conflicts
     */
    alwaysRetryWrites = conf.getBoolean("fs.ha.retrywrites", false);

    // Create AvatarZooKeeperClient
    zk = new CachingAvatarZooKeeperClient(conf, null);

    failoverCheckPeriod = conf.getInt("fs.avatar.failover.checkperiod",
        FAILOVER_CHECK_PERIOD);
    this.logicalName = logicalName;
    this.failoverClient = failoverClient;
  }

  public String getPrimaryAvatarAddress(URI logicalName, Stat stat,
      boolean retry,
      boolean firstAttempt) throws Exception {
    String primaryAddr = zk.getPrimaryAvatarAddress(logicalName, stat, true,
        firstAttempt);
    lastPrimaryUpdate = stat.getMtime();
    return primaryAddr;
  }

  public boolean isZKCacheEnabled() {
    return zk.isCacheEnabled();
  }

  /**
   * @return true if a failover has happened, false otherwise requires write
   *         lock
   */
  boolean zkCheckFailover() {
    try {
      long registrationTime = zk.getPrimaryRegistrationTime(logicalName);
      LOG.debug("File is in ZK");
      LOG.debug("Checking mod time: " + registrationTime + " > "
          + lastPrimaryUpdate);
      if (registrationTime > lastPrimaryUpdate) {
        // Failover has happened happened already
        failoverClient.nameNodeDown();
        return true;
      }
    } catch (Exception x) {
      // just swallow for now
      LOG.error(x);
    }
    return false;
  }

  void handleFailure(IOException ex, int failures)
      throws IOException {
    // Check if the exception was thrown by the network stack
    if (failoverClient.isShuttingdown() || !shouldHandleException(ex)) {
      throw ex;
    }

    if (failures > FAILURE_RETRY) {
      throw ex;
    }
    try {
      // This might've happened because we are failing over
      if (!watchZK) {
        LOG.debug("Not watching ZK, so checking explicitly");
        // Check with zookeeper
        fsLock.readLock().unlock();
        InjectionHandler.processEvent(InjectionEvent.DAFS_CHECK_FAILOVER);
        fsLock.writeLock().lock();
        boolean failover = zkCheckFailover();
        fsLock.writeLock().unlock();
        fsLock.readLock().lock();
        if (failover) {
          return;
        }
      }
      Thread.sleep(1000);
    } catch (InterruptedException iex) {
      LOG.error("Interrupted while waiting for a failover", iex);
      Thread.currentThread().interrupt();
    }

  }

  private boolean shouldHandleException(IOException ex) {
    if (ex.getMessage().contains("java.io.EOFException")) {
      return true;
    }
    return ex.getMessage().toLowerCase().contains("connection");
  }

  void shutdown() throws IOException, InterruptedException {
    zk.shutdown();
  }

  void readUnlock() {
    fsLock.readLock().unlock();
  }

  void readLockSimple() {
    fsLock.readLock().lock();
  }

  void writeLock() {
    fsLock.writeLock().lock();
  }

  void writeUnLock() {
    fsLock.writeLock().unlock();
  }

  void readLock() throws IOException {
    for (int i = 0; i < FAILOVER_RETRIES; i++) {
      fsLock.readLock().lock();

      if (failoverClient.isFailoverInProgress()) {
        // This means Failover might be in progress, so wait for it
        fsLock.readLock().unlock();
        try {
          boolean failedOver = false;
          fsLock.writeLock().lock();
          if (!watchZK && failoverClient.isFailoverInProgress()) {
            // We are in pull failover mode where clients are asking ZK
            // if the failover is over instead of ZK telling watchers
            // however another thread in this Instance could've done
            // the failover for us.
            try {
              failedOver = failoverClient.tryFailover();
            } catch (Exception ex) {
              // Just swallow exception since we are retrying in any event
            }
          }
          fsLock.writeLock().unlock();
          if (!failedOver)
            Thread.sleep(failoverCheckPeriod);
        } catch (InterruptedException ex) {
          LOG.error("Got interrupted waiting for failover", ex);
          Thread.currentThread().interrupt();
        }

      } else {
        // The client is up and we are holding a readlock.
        return;
      }
    }
    // We retried FAILOVER_RETRIES times with no luck - fail the call
    throw new IOException("No namenode for " + logicalName);
  }

  /**
   * File System implementation
   */

  public abstract class ImmutableFSCaller<T> {

    public abstract T call() throws IOException;

    public T callFS() throws IOException {
      int failures = 0;
      while (true) {
        readLock();
        try {
          return this.call();
        } catch (IOException ex) {
          handleFailure(ex, failures);
          failures++;
        } finally {
          readUnlock();
        }
      }
    }
  }

  public abstract class MutableFSCaller<T> {

    public abstract T call(int retry) throws IOException;

    public T callFS() throws IOException {
      int retries = 0;
      while (true) {
        readLock();
        try {
          return this.call(retries);
        } catch (IOException ex) {
          if (!alwaysRetryWrites)
            throw ex;
          handleFailure(ex, retries);
          retries++;
        } finally {
          readUnlock();
        }
      }
    }
  }
}
