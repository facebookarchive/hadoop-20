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
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.net.InetSocketAddress;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.AvatarZKShell;
import org.apache.hadoop.hdfs.AvatarZooKeeperClient;
import org.apache.hadoop.hdfs.protocol.AvatarConstants.Avatar;
import org.apache.hadoop.hdfs.protocol.AvatarConstants.StartupOption;
import org.apache.hadoop.hdfs.protocol.AvatarConstants.ZookeeperKey;
import org.apache.hadoop.hdfs.protocol.FSConstants;
import org.apache.hadoop.net.NetUtils;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;

public class AvatarNodeZkUtil {

  private static final Log LOG = LogFactory.getLog(AvatarNodeZkUtil.class);
 
  /**
   * Verifies whether we are in a consistent state before we perform a failover
   *
   * @param startupConf
   *          the startup configuration
   * @param confg
   *          the current configuration
   * @param noverification
   *          whether or not to skip some zookeeper based verification
   * @return the session id and last transaction id information from zookeeper
   * @throws IOException
   */
  static ZookeeperTxId checkZooKeeperBeforeFailover(Configuration startupConf,
      Configuration confg, boolean noverification) throws IOException {
    AvatarZooKeeperClient zk = null;
    String fsname = startupConf.get(NameNode.DFS_NAMENODE_RPC_ADDRESS_KEY);
    
    int maxTries = startupConf.getInt("dfs.avatarnode.zk.retries", 3);
    Exception lastException = null;
    for (int i = 0; i < maxTries; i++) {
      try {
        zk = new AvatarZooKeeperClient(confg, null, false);
        LOG.info("Failover: Checking if the primary is empty");
        String zkRegistry = zk.getPrimaryAvatarAddress(fsname, new Stat(),
            false, i > 0);
        if (zkRegistry != null) {
          throw new IOException(
              "Can't switch the AvatarNode to primary since "
                  + "zookeeper record is not clean. Either use shutdownAvatar to kill "
                  + "the current primary and clean the ZooKeeper entry, "
                  + "or clear out the ZooKeeper entry if the primary is dead");
        }

        if (noverification) {
          return null;
        }

        LOG.info("Failover: Obtaining last transaction id from ZK");
        String address = startupConf.get(NameNode.DFS_NAMENODE_RPC_ADDRESS_KEY);
        long sessionId = zk.getPrimarySsId(address, i > 0);
        ZookeeperTxId zkTxId = zk.getPrimaryLastTxId(address, i > 0);
        if (sessionId != zkTxId.getSessionId()) {
          throw new IOException("Session Id in the ssid node : " + sessionId
              + " does not match the session Id in the txid node : "
              + zkTxId.getSessionId());
        }
        return zkTxId;
      } catch (Exception e) {
        LOG.error("Got Exception reading primary node registration "
            + "from ZooKeeper. Will retry...", e);
        lastException = e;
      } finally {
        shutdownZkClient(zk);
      }
    }
    throw new IOException(lastException);
  }

  /**
   * Performs some operations after failover such as writing a new session id
   * and registering to zookeeper as the new primary.
   *
   * @param startupConf
   *          the startup configuration
   * @param confg
   *          the current configuration
   * @return the session id for the new node after failover
   * @throws IOException
   */
  static long writeToZooKeeperAfterFailover(Configuration startupConf,
      Configuration confg) throws IOException {
    AvatarZooKeeperClient zk = null;
    // Register client port address.
    String address = startupConf.get(NameNode.DFS_NAMENODE_RPC_ADDRESS_KEY);
    String realAddress = confg.get(NameNode.DFS_NAMENODE_RPC_ADDRESS_KEY);

    int maxTries = startupConf.getInt("dfs.avatarnode.zk.retries", 3);
    for (int i = 0; i < maxTries; i++) {
      try {
        zk = new AvatarZooKeeperClient(confg, null, false);
        LOG.info("Failover: Registering to ZK as primary");
        final boolean toOverwrite = true;
        zk.registerPrimary(address, realAddress, toOverwrite);
        registerClientProtocolAddress(zk, startupConf, confg, toOverwrite);
        registerDnProtocolAddress(zk, startupConf, confg, toOverwrite);
        registerHttpAddress(zk, startupConf, confg, toOverwrite);
        LOG.info("Failover: Writting session id to ZK");
        return writeSessionIdToZK(startupConf, zk);
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
        zk = new AvatarZooKeeperClient(confg, null, false);
        zk.registerLastTxId(startupConf.get(NameNode.DFS_NAMENODE_RPC_ADDRESS_KEY), zkTxid);
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

  static long writeSessionIdToZK(Configuration conf, AvatarZooKeeperClient zk)
      throws IOException {
    long ssid = AvatarNode.now();
    zk.registerPrimarySsId(conf.get(NameNode.DFS_NAMENODE_RPC_ADDRESS_KEY), ssid);
    return ssid;
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
        zk = new AvatarZooKeeperClient(conf, null, false);
        ssid = writeSessionIdToZK(conf, zk);
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
        zk = new AvatarZooKeeperClient(conf, null, false);
        String zkRegistry = zk.getPrimaryAvatarAddress(fsname, new Stat(),
            false);
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
  static void shutdownZkClient(AvatarZooKeeperClient zk) {
    if (zk != null) {
      try {
        zk.shutdown();
      } catch (InterruptedException e) {
        LOG.error("Error shutting down ZooKeeper client", e);
      }
    }
  }
  
  /**
   * This method tries to update the information in ZooKeeper For every address
   * of the NameNode it is being run for (fs.default.name,
   * dfs.namenode.dn-address, dfs.namenode.http.address) if they are present. It
   * also creates information for aliases in ZooKeeper for lists of strings in
   * fs.default.name.aliases, dfs.namenode.dn-address.aliases and
   * dfs.namenode.http.address.aliases
   *
   * Each address it transformed to the address of the zNode to be created by
   * substituting all . and : characters to /. The slash is also added in the
   * front to make it a valid zNode address. So dfs.domain.com:9000 will be
   * /dfs/domain/com/9000
   *
   * If any part of the path does not exist it is created automatically
   */
  public static void updateZooKeeper(Configuration originalConf, Configuration conf,
      boolean toOverwrite, String serviceName, String primaryInstance) throws IOException {
    String connection = conf.get(FSConstants.FS_HA_ZOOKEEPER_QUORUM);
    if (connection == null)
      return;
    AvatarZooKeeperClient zk = new AvatarZooKeeperClient(conf, null);
    if (registerClientProtocolAddress(zk, originalConf, conf, toOverwrite)) {
      return;
    }
    registerDnProtocolAddress(zk, originalConf, conf, toOverwrite);
    registerHttpAddress(zk, originalConf, conf, toOverwrite);
    for (ZookeeperKey key : ZookeeperKey.values()) {
      zk.registerPrimary(getZnodeName(conf, serviceName, Avatar.ACTIVE, key),
          key.getIpPortString(conf), true);
    }
    if(primaryInstance.equalsIgnoreCase(StartupOption.NODEZERO.getName())){
      primaryInstance = StartupOption.NODEONE.getName();
    } else {
      primaryInstance = StartupOption.NODEZERO.getName();
    }
    Configuration tempConf = AvatarZKShell.updateConf(primaryInstance, originalConf);
    for (ZookeeperKey key : ZookeeperKey.values()) {
      zk.registerPrimary(getZnodeName(tempConf, serviceName, Avatar.STANDBY, key),
          key.getIpPortString(tempConf), true);
    }
  }

  private static String getZnodeName(Configuration conf, String serviceName,
      Avatar primaryOrStandby, ZookeeperKey typeOfAddress) {
    return (conf.get(FSConstants.DFS_CLUSTER_NAME, "no-cluster") + "/"
        + (serviceName == null ? "no-service" : serviceName) + "/"
        + primaryOrStandby.toString() + typeOfAddress).toLowerCase();
  }

  /**
   * Registers namenode's address in zookeeper
   */
  private static boolean registerClientProtocolAddress(AvatarZooKeeperClient zk,
      Configuration originalConf, Configuration conf, boolean toOverwrite)
      throws UnsupportedEncodingException, IOException {
    LOG.info("Updating Client Address information in ZooKeeper");
    InetSocketAddress addr = NameNode.getClientProtocolAddress(conf);
    if (addr == null) {
      LOG.error(FSConstants.DFS_NAMENODE_RPC_ADDRESS_KEY
          + " for primary service is not defined");
      return true;
    }
    InetSocketAddress defaultAddr = NameNode.getClientProtocolAddress(originalConf);
    if (defaultAddr == null) {
      LOG.error(FSConstants.DFS_NAMENODE_RPC_ADDRESS_KEY
          + " for default service is not defined");
      return true;
    }
    registerSocketAddress(zk,
        originalConf.get(NameNode.DFS_NAMENODE_RPC_ADDRESS_KEY),
        conf.get(NameNode.DFS_NAMENODE_RPC_ADDRESS_KEY), toOverwrite);
    /** TODO later: need to handle alias leave it as it is now */
    registerAliases(zk, conf, FSConstants.FS_NAMENODE_ALIASES,
        conf.get(NameNode.DFS_NAMENODE_RPC_ADDRESS_KEY), toOverwrite);
    return false;
  }

  /**
   * Registers the datanode protocol address in the zookeeper
   */
  private static void registerDnProtocolAddress(AvatarZooKeeperClient zk,
      Configuration originalConf, Configuration conf, boolean toOverwrite)
      throws UnsupportedEncodingException, IOException {
    LOG.info("Updating Service Address information in ZooKeeper");
    registerSocketAddress(zk,
        originalConf.get(NameNode.DATANODE_PROTOCOL_ADDRESS),
        conf.get(NameNode.DATANODE_PROTOCOL_ADDRESS), toOverwrite);
    registerAliases(zk, conf, FSConstants.DFS_NAMENODE_DN_ALIASES,
        conf.get(NameNode.DATANODE_PROTOCOL_ADDRESS), toOverwrite);
  }

  /**
   * Registers the http address of the namenode in the zookeeper
   */
  private static void registerHttpAddress(AvatarZooKeeperClient zk, Configuration originalConf,
      Configuration conf, boolean toOverwrite) throws UnsupportedEncodingException, IOException {
    LOG.info("Updating Http Address information in ZooKeeper");
    String addr = conf.get(FSConstants.DFS_NAMENODE_HTTP_ADDRESS_KEY);
    String defaultAddr = originalConf
        .get(FSConstants.DFS_NAMENODE_HTTP_ADDRESS_KEY);
    registerSocketAddress(zk, defaultAddr, addr, toOverwrite);
    registerAliases(zk, conf, FSConstants.DFS_HTTP_ALIASES, addr, toOverwrite);
  }

  private static void registerAliases(AvatarZooKeeperClient zk, Configuration conf, String key,
      String value, boolean toOverwrite) throws UnsupportedEncodingException,
      IOException {
    String[] aliases = conf.getStrings(key);
    if (aliases == null) {
      return;
    }
    for (String alias : aliases) {
      zk.registerPrimary(alias, value, toOverwrite);
    }
  }
  
  public static String toIpPortString(InetSocketAddress addr) {
    return addr.getAddress().getHostAddress() + ":" + addr.getPort(); 
  }

  private static void registerSocketAddress(AvatarZooKeeperClient zk,
      String key, String value, boolean toOverwrite)
      throws UnsupportedEncodingException, IOException {
    if (key == null || value == null) {
      return;
    }
    zk.registerPrimary(key, value, toOverwrite);
  }
  
  public static void clearZookeeper(Configuration originalConf, Configuration conf,
      String serviceName) throws IOException {
    String connection = conf.get(FSConstants.FS_HA_ZOOKEEPER_QUORUM);
    if (connection == null) {
      return;
    }
    AvatarZooKeeperClient zk = new AvatarZooKeeperClient(conf, null);

    // Clear NameNode address in ZK
    InetSocketAddress defaultAddr;
    String[] aliases;

    defaultAddr = NameNode.getClientProtocolAddress(originalConf);
    String defaultName = defaultAddr.getHostName() + ":"
        + defaultAddr.getPort();
    LOG.info("Clear Client Address information in ZooKeeper: " + defaultName);
    zk.clearPrimary(defaultName);

    aliases = conf.getStrings(FSConstants.FS_NAMENODE_ALIASES);
    if (aliases != null) {
      for (String alias : aliases) {
        zk.clearPrimary(alias);
      }
    }
    
    
    LOG.info("Clear Service Address information in ZooKeeper");
    defaultAddr = NameNode.getDNProtocolAddress(originalConf);
    if (defaultAddr != null) {
      String defaultServiceName = defaultAddr.getHostName() + ":"
          + defaultAddr.getPort();
      zk.clearPrimary(defaultServiceName);
    }
    
    aliases = conf.getStrings(FSConstants.DFS_NAMENODE_DN_ALIASES);
    if (aliases != null) {
      for (String alias : aliases) {
        zk.clearPrimary(alias);
      }
    }
    
    
    LOG.info("Clear Http Address information in ZooKeeper");
    // Clear http address in ZK
    // Stolen from NameNode so we have the same code in both places
    defaultAddr = NetUtils.createSocketAddr(originalConf
        .get(FSConstants.DFS_NAMENODE_HTTP_ADDRESS_KEY));
    String defaultHttpAddress = defaultAddr.getHostName() + ":"
        + defaultAddr.getPort();
    zk.clearPrimary(defaultHttpAddress);
    
    aliases = conf.getStrings(FSConstants.DFS_HTTP_ALIASES);
    if (aliases != null) {
      for (String alias : aliases) {
        zk.clearPrimary(alias);
      }
    }

    for(Avatar avatar : Avatar.avatars) {
      for (ZookeeperKey key : ZookeeperKey.values()) {
        zk.clearPrimary(getZnodeName(conf, serviceName, avatar, key));
      }
    }
  }

  public static void printZookeeperEntries(Configuration originalConf, Configuration conf,
      String serviceName, PrintStream outputStream) throws IOException, KeeperException,
      InterruptedException {
    String connection = conf.get(FSConstants.FS_HA_ZOOKEEPER_QUORUM);
    if (connection == null)
      return;
    AvatarZooKeeperClient zk = new AvatarZooKeeperClient(conf, null);
    outputStream.println("ZooKeeper entries:");

    // client protocol
    InetSocketAddress defaultAddr = NameNode.getClientProtocolAddress(originalConf);
    String defaultName = defaultAddr.getHostName() + ":" + defaultAddr.getPort();
    outputStream.println("Default name is " + defaultName);
    String registration = zk.getPrimaryAvatarAddress(defaultName, new Stat(), false);
    outputStream.println("Primary node according to ZooKeeper: " + registration);

    // datanode protocol
    defaultAddr = NameNode.getDNProtocolAddress(originalConf);
    defaultName = defaultAddr.getHostName() + ":" + defaultAddr.getPort();
    registration = zk.getPrimaryAvatarAddress(defaultName, new Stat(), false);
    outputStream.println("Primary node DN protocol     : " + registration);

    // http address
    defaultAddr = NetUtils.createSocketAddr(originalConf.get("dfs.http.address"));
    defaultName = defaultAddr.getHostName() + ":" + defaultAddr.getPort();
    registration = zk.getPrimaryAvatarAddress(defaultName, new Stat(), false);
    outputStream.println("Primary node http address    : " + registration);

    for (Avatar anAvatar : Avatar.avatars) {
      outputStream.println(anAvatar + " entries: ");
      for (ZookeeperKey key : ZookeeperKey.values()) {
        String keyInZookeeper = getZnodeName(conf, serviceName, anAvatar, key);
        outputStream.println(keyInZookeeper + " : "
            + zk.getPrimaryAvatarAddress(keyInZookeeper, new Stat(), false));
      }
    }
  }
}
