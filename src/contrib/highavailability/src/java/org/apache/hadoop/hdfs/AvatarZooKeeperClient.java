package org.apache.hadoop.hdfs;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.server.namenode.ZookeeperTxId;
import org.apache.hadoop.hdfs.util.InjectionEvent;
import org.apache.hadoop.util.InjectionHandler;
import org.apache.hadoop.util.SerializableUtils;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileSystem;
import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooDefs.Perms;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Id;
import org.apache.zookeeper.data.Stat;

public class AvatarZooKeeperClient {
  
  private static Log LOG = LogFactory.getLog(AvatarZooKeeperClient.class.getName());
  
  private String connection;
  private int timeout;
  private int connectTimeout;
  private boolean watch;
  // Prefix under which the data for this client will be stored
  private String prefix;
  // The directory under which the session id for the current avatar is stored.
  private static final String ssid = "ssid";
  // The directory under which the last transaction id for the primary avatar is
  // stored.
  private static final String txid = "txid";
  private Watcher watcher;
  private ZooKeeper zk;
  private final int failoverCheckPeriod;
  private final boolean closeConnOnEachOp;

  // Making it large enough to be sure that the cluster is down
  // these retries go one after another so they do not take long
  public static final int ZK_CONNECTION_RETRIES = 10;
  private static final int ZK_INIT_RETRIES = 5;
  public static final int ZK_CONNECT_TIMEOUT_DEFAULT = 10000; // 10 seconds
  
  public AvatarZooKeeperClient(Configuration conf, Watcher watcher) {
    this(conf, watcher, true);
  }

  public AvatarZooKeeperClient(Configuration conf, Watcher watcher,
      boolean closeConnOnEachOp) {
    this.connection = conf.get("fs.ha.zookeeper.quorum");
    this.timeout = conf.getInt("fs.ha.zookeeper.timeout", 3000);
    this.connectTimeout = conf.getInt("fs.ha.zookeeper.connect.timeout",
        ZK_CONNECT_TIMEOUT_DEFAULT);
    this.watch = conf.getBoolean("fs.ha.zookeeper.watch", false);
    this.prefix = conf.get("fs.ha.zookeeper.prefix", "/hdfs");
    this.watcher = new ProxyWatcher(watcher);
    if (watcher == null) {
      // If there was no watcher regardless of the watch policy in the conf
      // set it to false. Since there is no watcher being set
      watch = false;
    }
    this.failoverCheckPeriod = conf.getInt("fs.avatar.failover.checkperiod",
        FailoverClientHandler.FAILOVER_CHECK_PERIOD);
    this.closeConnOnEachOp = closeConnOnEachOp;
  }

  private static class ProxyWatcher implements Watcher {
    private Watcher impl;
    ProxyWatcher(Watcher impl) {
      this.impl = impl;
    }

    public void process(WatchedEvent event) {
      if (event.getType() == Event.EventType.None
              && event.getState() == Event.KeeperState.SyncConnected) {
        // The ZooKeeper client is connected
        synchronized (this) {
          this.notifyAll();
        }
      }
    }
  }
  
  public synchronized void clearPrimary(String address) throws IOException {
    String node = getRegistrationNode(address);
    zkCreateRecursively(node, null, true, null);
  }

  /**
   * Creates a node in zookeeper denoting the current session id of the primary
   * avatarnode of the cluster. The primary avatarnode always syncs this
   * information to zookeeper when it starts.
   * 
   * @param address
   *          the address of the cluster, used to create the path name for the
   *          znode
   * @param ssid
   *          the session id of the primary avatarnode
   * @throws IOException
   */
  public synchronized void registerPrimarySsId(String address, Long ssid)
      throws IOException {
    String node = getSsIdNode(address);
    zkCreateRecursively(node, SerializableUtils.toBytes(ssid), true,
        ssid.toString());
  }

  /**
   * Creates a node in zookeeper denoting the current session id and the last
   * transaction id processed by the primary avatarnode. This is used by the
   * primary avatarnode when it shuts down cleanly.
   * 
   * @param address
   *          the address of the cluster, used to create the path name for the
   *          znode
   * @param lastTxid
   *          the last transaction id in the primary avatarnode
   * @throws IOException
   */
  public synchronized void registerLastTxId(String address,
      ZookeeperTxId lastTxid)
      throws IOException {
    String node = getLastTxIdNode(address);
    zkCreateRecursively(node, lastTxid.toBytes(), true, lastTxid.toString());
  }

  public synchronized void registerPrimary(String address, String realAddress, 
      boolean overwrite) 
    throws UnsupportedEncodingException, IOException {
    String node = getRegistrationNode(address);
    zkCreateRecursively(node, realAddress.getBytes("UTF-8"), overwrite,
        realAddress);
  }
  
  private void zkCreateRecursively(String zNode, byte[] data,
      boolean overwrite, String strData) throws IOException {
    try {
    initZK();
    } catch (InterruptedException ie) {
      throw new IOException(ie);
    }
    System.out.println("updating (" + zNode + ")-> (" + strData + ")");
    String[] parts = zNode.split("/");
    String path = "";
    byte[] payLoad = new byte[0];
    List<ACL> acls = new ArrayList<ACL>(1);
    acls.add(new ACL(Perms.ALL, new Id("world", "anyone")));
    
    try {
      for (int i = 0; i < parts.length; i++) {
        if (parts[i].isEmpty())
          continue;
        path += "/" + parts[i];
        if (i == parts.length - 1) {
          payLoad = data;
        }
        boolean created = false;
        while (!created) {
          // While loop to keep trying through the ConnectionLoss exceptions
          try {
            if ((zk.exists(path, false)) != null) {
              // -1 indicates that we should update zNode regardless of its
              // version
              // since we are not utilizing versions in zNode - this is the best
              if (i == parts.length - 1 && !overwrite) {
                throw new FileAlreadyExistsException("ZNode " + path + " already exists.");
              }
              zk.setData(path, payLoad, -1);
            } else {
              zk.create(path, payLoad, acls, CreateMode.PERSISTENT);
            }            
            created = true;
          } catch (KeeperException ex) {
            ex.printStackTrace();
            if (KeeperException.Code.CONNECTIONLOSS != ex.code()) {
              throw ex;
            }
          }
        }
      }
      FileSystem.LOG.info("Wrote zNode " + zNode);
    } catch (KeeperException e) {
      e.printStackTrace();
    } catch (InterruptedException e) {
      e.printStackTrace();
      Thread.currentThread().interrupt();
    } finally {
      try {
        if (closeConnOnEachOp) {
          stopZK();
        }
      } catch (InterruptedException ex) {
        Thread.currentThread().interrupt();
      }
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

  private void initZK() throws IOException, InterruptedException {
    synchronized (watcher) {
      for (int i = 0; i < ZK_INIT_RETRIES; i++) {
        try {
          if (zk == null || zk.getState() == ZooKeeper.States.CLOSED) {
            zk = new ZooKeeper(connection, timeout, watcher);
          }
          break;
        } catch (IOException ie) {
          if (i == ZK_INIT_RETRIES - 1) {
            throw ie;
          }
          FileSystem.LOG.info("Connection to zookeeper could not be"
              + "established retrying....", ie);
          Thread.sleep(3000);
        }
      }
      if (zk.getState() != ZooKeeper.States.CONNECTED) {
        watcher.wait(this.connectTimeout);
      }
      if (zk.getState() != ZooKeeper.States.CONNECTED) {
        // Close any open connections to avoid fd leak.
        zk.close();
        throw new IOException("Timed out trying to connect to ZooKeeper");
      }
    }
  }

  private void stopZK() throws InterruptedException {
    if (zk == null)
      return;
    zk.close();
    zk = null;
  }

  /**
   * Get the information stored in the node of zookeeper. If retry is set
   * to true it will keep retrying until the data in that node is available
   * (failover case). If the retry is set to false it will return the first
   * value that it gets from the zookeeper.
   * 
   * @param node the path of zNode in zookeeper
   * @param stat {@link Stat} object that will contain stats of the node
   * @param retry if true will retry until the data in znode is not null
   * @return byte[] the data in the znode
   * @throws IOException
   * @throws KeeperException
   * @throws InterruptedException
   */
  private synchronized byte[] getNodeData(String node, Stat stat,
      boolean retry, boolean sync)
      throws IOException, KeeperException, InterruptedException {
    int failures = 0;
    
    byte[] data = null;
    while (data == null) {
      initZK();
      try {
        if (sync) {
          SyncUtil su = new SyncUtil();
          su.sync(zk, node);
        }
        data = zk.getData(node, watch, stat);
        if (data == null && retry) {
          // Failover is in progress
          // reset the failures
          failures = 0;
          DistributedAvatarFileSystem.LOG.info("Failover is in progress. Waiting");
          try {
            Thread.sleep(failoverCheckPeriod);
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
      } finally {
        if (closeConnOnEachOp) {
          stopZK();
        }
      }
    }
    return data;
  }

  /**
   * Retrieves the current session id for the cluster from zookeeper.
   * 
   * @param address
   *          the address of the cluster
   * @param sync
   *          whether or not to perform a sync before read
   * @throws IOException
   * @throws KeeperException
   * @throws InterruptedException
   */
  public Long getPrimarySsId(String address, boolean sync) throws IOException,
      KeeperException, InterruptedException, ClassNotFoundException {
    Stat stat = new Stat();
    String node = getSsIdNode(address);
    byte[] data = getNodeData(node, stat, false, sync);
    if (data == null) {
      return null;
    }
    return (Long) SerializableUtils.getFromBytes(data, Long.class);
  }

  /**
   * Retrieves the last transaction id of the primary from zookeeper.
   * 
   * @param address
   *          the address of the cluster
   * @param sync
   *          whether or not to perform a sync before read
   * @throws IOException
   * @throws KeeperException
   * @throws InterruptedException
   */
  public ZookeeperTxId getPrimaryLastTxId(String address, boolean sync)
      throws IOException,
      KeeperException, InterruptedException, ClassNotFoundException {
    Stat stat = new Stat();
    String node = getLastTxIdNode(address);
    byte[] data = getNodeData(node, stat, false, sync);
    if (data == null) {
      return null;
    }
    return ZookeeperTxId.getFromBytes(data);
  }

  /**
   * Retrieves the primary address for the cluster, this does not perform a
   * sync before it reads the znode.
   */
  public String getPrimaryAvatarAddress(String address, Stat stat, boolean retry)
      throws IOException, KeeperException, InterruptedException {
    return getPrimaryAvatarAddress(address, stat, retry, false);
  }

  public String getPrimaryAvatarAddress(String address, Stat stat,
      boolean retry, boolean sync)
    throws IOException, KeeperException, InterruptedException {
    String node = getRegistrationNode(address);
    byte[] data = getNodeData(node, stat, retry, sync);
    if (data == null) {
      return null;
    }
    return new String(data, "UTF-8");
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
  private synchronized Stat getNodeStats(String node) throws IOException,
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
      } catch (KeeperException kex) {
        if (KeeperException.Code.CONNECTIONLOSS == kex.code()
            && failures < ZK_CONNECTION_RETRIES) {
          failures++;
          continue;
        }
        throw kex;
      } finally {
      }
    }
    return res;
  }

  public long getPrimaryRegistrationTime(URI address) throws IOException,
      KeeperException, InterruptedException {
    InjectionHandler
        .processEvent(InjectionEvent.AVATARZK_GET_REGISTRATION_TIME);
    String node = getRegistrationNode(address.getAuthority());
    return getNodeStats(node).getMtime();
  }
  /*
   * ZNode address is formed by the logical name of the filesystem:
   * dfs.data.xxx.com:9000 will be represented by zNode
   * /prefix/dfs.data.xxx.com/9000 in ZooKeeper
   */
  private String getRegistrationNode(String clusterAddress) {
    return prefix + "/" + clusterAddress.replaceAll("[:]", "/").toLowerCase();
  }

  /**
   * Computes the znode for the session id of the primary avatar, the format is
   * /prefix/ssid/dfs.data.xxx.com/9000
   * 
   * @param clusterAddress
   *          the address of the cluster
   * @return the znode to store the ssid in the following format :
   *         /prefix/ssid/dfs.data.xxx.com/9000
   */
  private String getSsIdNode(String clusterAddress) {
    return prefix + "/" + ssid + "/"
        + clusterAddress.replaceAll("[:]", "/").toLowerCase();
  }

  /**
   * Computes the znode for the session id of the primary avatar, the format is
   * /prefix/txid/dfs.data.xxx.com/9000
   * 
   * @param clusterAddress
   *          the address of the cluster
   * @return the znode to store the ssid in the following format :
   *         /prefix/txid/dfs.data.xxx.com/9000
   */
  private String getLastTxIdNode(String clusterAddress) {
    return prefix + "/" + txid + "/"
        + clusterAddress.replaceAll("[:]", "/").toLowerCase();
  }

  public synchronized void shutdown() throws InterruptedException {
    stopZK();
  }
  
  /**
   * Helper class for syncing data to ZK.
   */
  static class SyncUtil implements AsyncCallback.VoidCallback {
    private static final int MAX_SYNC_WAIT_TIME = 5 * 1000; // 5 sec
    volatile int rc = -1;

    @Override
    public synchronized void processResult(int rc, String path, Object ctx) {
      this.rc = rc;
      this.notify();
    }

    synchronized boolean sync(ZooKeeper zk, String path)
        throws InterruptedException {
      zk.sync(path, this, null);
      this.wait(MAX_SYNC_WAIT_TIME);
      if (rc != KeeperException.Code.OK.intValue()) {
        LOG.info("Cannot sync ZK for path: " + path + " return code: " + rc);
        return false;
      }
      LOG.info("Synced ZK for path: " + path);
      return true;
    }
  }
}
