package org.apache.hadoop.hdfs;

import java.io.IOException;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

public class AvatarZooKeeperClient {
  private String connection;
  private int timeout;
  private boolean watch;
  private Watcher watcher;
  private ZooKeeper zk;

  // Making it large enough to be sure that the cluster is down
  // these retries go one after another so they do not take long
  public static final int ZK_CONNECTION_RETRIES = 100;

  public AvatarZooKeeperClient(String connection, int timeout, boolean watch,
      Watcher watcher) {
    super();
    this.connection = connection;
    this.timeout = timeout;
    this.watch = watch;
    if (watch) {
      this.watcher = watcher;
    } else {
      this.watcher = (new Watcher() {

        @Override
        public void process(WatchedEvent event) {
          // This is a stub for ZK compatibility
          // if it is not there ZK will keep writing errors to the log
        }
      });
    }

  }

  /**
   * Tries to connect to ZooKeeper. To be used when we need to test if the
   * ZooKeeper cluster is available and the config is correct
   * 
   * @throws IOException
   * @throws InterruptedException
   */
  public synchronized void primeConnection() throws IOException,
      InterruptedException {
    initZK();
    if (!watch) {
      stopZK();
    }
  }

  private void initZK() throws IOException {
    if (zk == null)
      zk = new ZooKeeper(connection, timeout, watcher);
  }

  private void stopZK() throws InterruptedException {
    if (zk == null)
      return;
    zk.close();
    zk = null;
  }

  /**
   * Gets the information stored in node of zookeeper. Will create and destroy
   * connection if the AvatarZooKeeperClient is not configured to set watchers
   * The result returned will never be null. Since this is an Avatar client
   * the semantics is: if the zNode contains null - the failover is in progress
   * and we need to wait
   * 
   * @param node
   *          the path of zNode to fetch the data from
   * @return byte[] the data that was in this zNode
   * @throws IOException
   * @throws KeeperException
   * @throws InterruptedException
   */
  public synchronized byte[] getNodeData(String node, Stat stat)
      throws IOException, KeeperException, InterruptedException {
    int failures = 0;
    
    byte[] data = null;
    while (data == null) {
      initZK();
      try {
        data = zk.getData(node, watch, stat);
        if (!watch) {
          stopZK();
        }
        if (data == null) {
          // Failover is in progress
          // reset the failures
          failures = 0;
          DistributedAvatarFileSystem.LOG.info("Failover is in progress. Waiting");
          try {
            Thread.sleep(DistributedAvatarFileSystem.FAILOVER_CHECK_PERIOD);
          } catch (InterruptedException iex) {
            Thread.currentThread().interrupt();
          }
        } else {
          return data;
        }
      } catch (KeeperException kex) {
        if (KeeperException.Code.CONNECTIONLOSS == kex.code()
            && failures < ZK_CONNECTION_RETRIES) {
          failures++;
          // This means there was a failure connecting to zookeeper
          // we should retry since some nodes might be down.
          continue;
        }
        throw kex;
      }
    }
    return data;
  }

  /**
   * Gets the {@link Stat} of the node. Will create and destroy connection if
   * the AvatarZooKeeperClient is not configured to set watchers
   * 
   * @param node
   *          the path of zNode to get {@link Stat} for
   * @return {@link Stat} of the node
   * @throws IOException
   * @throws KeeperException
   * @throws InterruptedException
   */

  public synchronized Stat getNodeStats(String node) throws IOException,
      KeeperException, InterruptedException {
    
    int failures = 0;
    boolean gotStats = false;
    Stat res = null;
    while (!gotStats) {
      initZK();
      try {
        res = zk.exists(node, watch);
        // Since stats can be null we have to control the execution with a flag
        gotStats = true;
        if (!watch) {
          stopZK();
        }
      } catch (KeeperException kex) {
        if (KeeperException.Code.CONNECTIONLOSS == kex.code()
            && failures < ZK_CONNECTION_RETRIES) {
          failures++;
          continue;
        }
        throw kex;
      }
    }
    return res;
  }

  public synchronized void shutdown() throws InterruptedException {
    stopZK();
  }
}
