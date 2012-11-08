/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdfs.server.namenode;

import java.io.IOException;
import java.net.InetSocketAddress;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.AvatarZooKeeperClient;
import org.apache.zookeeper.data.Stat;

public class AvatarNodeZkUtil {

  private static final Log LOG = LogFactory.getLog(AvatarNodeZkUtil.class);

  static boolean zkIsEmpty(Configuration startupConf, Configuration confg)
      throws Exception {
    AvatarZooKeeperClient zk = null;
    InetSocketAddress defaultAddr = NameNode
        .getClientProtocolAddress(startupConf);
    String fsname = defaultAddr.getHostName() + ":" + defaultAddr.getPort();

    int maxTries = startupConf.getInt("dfs.avatarnode.zk.retries", 3);
    for (int i = 0; i < maxTries; i++) {
      try {
        zk = new AvatarZooKeeperClient(confg, null);
        String zkRegistry = zk.getPrimaryAvatarAddress(fsname, new Stat(),
            false);
        return zkRegistry == null;
      } catch (Exception e) {
        LOG.error("Got Exception reading primary node registration "
            + "from ZooKeeper. Will retry...", e);
      } finally {
        shutdownZkClient(zk);
      }
    }
    throw new IOException("Cannot connect to zk");
  }

  /**
   * Obtain last transaction id of the primary form zk.
   */
  static ZookeeperTxId getLastTransactionId(Configuration startupConf,
      Configuration confg) throws IOException {
    AvatarZooKeeperClient zk = null;
    int maxTries = startupConf.getInt("dfs.avatarnode.zk.retries", 3);
    for (int i = 0; i < maxTries; i++) {
      try {
        zk = new AvatarZooKeeperClient(confg, null);
        // Gather session id and transaction id data.
        String address = AvatarNode.getClusterAddress(startupConf);
        long sessionId = zk.getPrimarySsId(address);
        ZookeeperTxId zkTxId = zk.getPrimaryLastTxId(address);
        if (sessionId != zkTxId.getSessionId()) {
          throw new IOException("Session Id in the ssid node : " + sessionId
              + " does not match the session Id in the txid node : "
              + zkTxId.getSessionId());
        }
        return zkTxId;
      } catch (Exception e) {
        LOG.error("Got Exception reading last transaction id of the primary "
            + "from ZooKeeper. Will retry...", e);
      } finally {
        shutdownZkClient(zk);
      }
    }
    throw new IOException("Cannot connect to zk");
  }

  /**
   * Register given node as a primary to zk.
   */
  static void registerAsPrimaryToZK(Configuration startupConf,
      Configuration confg) throws IOException {
    AvatarZooKeeperClient zk = null;
    // Register client port address.
    String address = AvatarNode.getClusterAddress(startupConf);
    String realAddress = AvatarNode.getClusterAddress(confg);

    int maxTries = startupConf.getInt("dfs.avatarnode.zk.retries", 3);
    for (int i = 0; i < maxTries; i++) {
      try {
        zk = new AvatarZooKeeperClient(confg, null);
        zk.registerPrimary(address, realAddress, true);

        // Register dn protocol address
        registerAddressToZK(zk, "dfs.namenode.dn-address", startupConf, confg);
        // Register http address
        registerAddressToZK(zk, "dfs.http.address", startupConf, confg);
        // Register rpc address
        registerAddressToZK(zk, AvatarNode.DFS_NAMENODE_RPC_ADDRESS_KEY,
            startupConf, confg);
        return;
      } catch (Exception e) {
        LOG.error("Got Exception registering the new primary "
            + "with ZooKeeper. Will retry...", e);
      } finally {
        shutdownZkClient(zk);
      }
    }
    throw new IOException("Cannot connect to zk");
  }

  /**
   * Writes the last transaction id of the primary avatarnode to zookeeper.
   */
  static void writeLastTxidToZookeeper(long lastTxid, long totalBlocks,
      long totalInodes, long ssid, Configuration startupConf,
      Configuration confg) throws IOException {
    AvatarZooKeeperClient zk = null;
    LOG.info("Writing lastTxId: " + lastTxid + ", total blocks: " + totalBlocks
        + ", total inodes: " + totalInodes);
    if (lastTxid < 0) {
      LOG.warn("Invalid last transaction id : " + lastTxid
          + " skipping write to zookeeper.");
      return;
    }
    ZookeeperTxId zkTxid = new ZookeeperTxId(ssid, lastTxid, totalBlocks,
        totalInodes);
    int maxTries = startupConf.getInt("dfs.avatarnode.zk.retries", 3);
    for (int i = 0; i < maxTries; i++) {
      try {
        zk = new AvatarZooKeeperClient(confg, null);
        zk.registerLastTxId(AvatarNode.getClusterAddress(startupConf), zkTxid);
        return;
      } catch (Exception e) {
        LOG.error("Got Exception when syncing last txid to zk. Will retry...",
            e);
      } finally {
        shutdownZkClient(zk);
      }
    }
    throw new IOException("Cannot connect to zk");
  }

  /**
   * Generates a new session id for the cluster and writes it to zookeeper. Some
   * other data in zookeeper (like the last transaction id) is written to
   * zookeeper with the sessionId so that we can easily determine in which
   * session was this data written. The sessionId is unique since it uses the
   * current time.
   * 
   * @return the session id that it wrote to ZooKeeper
   * @throws IOException
   */
  static long writeSessionIdToZK(Configuration conf) throws IOException {
    AvatarZooKeeperClient zk = null;
    long ssid = -1;
    int maxTries = conf.getInt("dfs.avatarnode.zk.retries", 3);
    boolean mismatch = false;
    Long ssIdInZk = -1L;

    for (int i = 0; i < maxTries; i++) {
      try {
        zk = new AvatarZooKeeperClient(conf, null);
        ssid = AvatarNode.now();
        zk.registerPrimarySsId(AvatarNode.getClusterAddress(conf), ssid);
        // Be extra careful and verify the data was synced to zk.
        ssIdInZk = zk.getPrimarySsId(AvatarNode.getClusterAddress(conf));
        if (ssid != ssIdInZk) {
          mismatch = true;
          break;
        }
        return ssid;
      } catch (Exception e) {
        LOG.error("Got Exception when writing session id to zk. Will retry...",
            e);
      } finally {
        shutdownZkClient(zk);
      }
    }
    if (mismatch)
      throw new IOException("Session Id in the NameNode : " + ssid
          + " does not match the session Id in Zookeeper : " + ssIdInZk);
    throw new IOException("Cannot connect to zk");
  }

  /**
   * Obtain the registration of the primary from zk.
   */
  static String getPrimaryRegistration(Configuration startupConf,
      Configuration conf, String fsname) throws IOException {
    AvatarZooKeeperClient zk = null;
    int maxTries = startupConf.getInt("dfs.avatarnode.zk.retries", 3);
    for (int i = 0; i < maxTries; i++) {
      try {
        zk = new AvatarZooKeeperClient(conf, null);
        Stat stat = new Stat();
        String zkRegistry = zk.getPrimaryAvatarAddress(fsname, stat, false);
        return zkRegistry;
      } catch (Exception e) {
        LOG.error(
            "Got Exception when reading primary registration. Will retry...", e);
      } finally {
        shutdownZkClient(zk);
      }
    }
    throw new IOException("Cannot connect to zk");
  }

  // helpers

  private static void registerAddressToZK(AvatarZooKeeperClient zk,
      String confParam, Configuration startupConf, Configuration confg)
      throws IOException {
    String address = startupConf.get(confParam);
    String realAddress = confg.get(confParam);
    if (address != null && realAddress != null) {
      zk.registerPrimary(address, realAddress, true);
    }
  }

  static void shutdownZkClient(AvatarZooKeeperClient zk) {
    if (zk != null) {
      try {
        zk.shutdown();
      } catch (InterruptedException e) {
        LOG.error("Error shutting down ZooKeeper client", e);
      }
    }
  }
}
