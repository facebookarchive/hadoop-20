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

package org.apache.hadoop.hdfs;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import javax.security.auth.login.LoginException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.protocol.AvatarProtocol;
import org.apache.hadoop.hdfs.protocol.AvatarConstants.Avatar;
import org.apache.hadoop.hdfs.protocol.AvatarConstants.StartupOption;
import org.apache.hadoop.hdfs.server.namenode.AvatarNode;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.io.retry.RetryPolicies;
import org.apache.hadoop.io.retry.RetryPolicy;
import org.apache.hadoop.io.retry.RetryProxy;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.UnixUserGroupInformation;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooDefs.Perms;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Id;
import org.apache.zookeeper.data.Stat;

/**
 * A {@link AvatarZKShell} that allows browsing configured avatar policies.
 */
public class AvatarZKShell extends Configured implements Tool {
  public static final Log LOG = LogFactory
      .getLog("org.apache.hadoop.AvatarShell");

  // AvatarShell deals with hdfs configuration so need to add these
  static {
    Configuration.addDefaultResource("hdfs-default.xml");
    Configuration.addDefaultResource("hdfs-site.xml");
    Configuration.addDefaultResource("avatar-site.xml");
  }

  public AvatarProtocol avatarnode;
  AvatarProtocol rpcAvatarnode;
  private UnixUserGroupInformation ugi;
  volatile boolean clientRunning = true;
  private Configuration conf;
  // We need to keep the default configuration around with
  // Avatar specific fields unmodified
  private Configuration originalConf;

  /**
   * Start AvatarShell.
   * <p>
   * The AvatarShell connects to the specified AvatarNode and performs basic
   * configuration options.
   * 
   * @throws IOException
   */
  public AvatarZKShell() {
    this(new Configuration());
  }

  /**
   * The AvatarShell connects to the specified AvatarNode and performs basic
   * configuration options.
   * 
   * @param conf
   *          The Hadoop configuration
   * @throws IOException
   */
  public AvatarZKShell(Configuration conf) {
    super(conf);
    this.conf = conf;
    this.originalConf = new Configuration(conf);
  }

  public void initAvatarRPC() throws IOException {
    try {
      this.ugi = UnixUserGroupInformation.login(conf, true);
    } catch (LoginException e) {
      throw (IOException) (new IOException().initCause(e));
    }

    this.rpcAvatarnode = createRPCAvatarnode(AvatarNode.getAddress(conf), conf,
        ugi);
    this.avatarnode = createAvatarnode(rpcAvatarnode);
  }

  public static AvatarProtocol createAvatarnode(Configuration conf)
      throws IOException {
    return createAvatarnode(AvatarNode.getAddress(conf), conf);
  }

  public static AvatarProtocol createAvatarnode(
      InetSocketAddress avatarNodeAddr, Configuration conf) throws IOException {
    try {
      return createAvatarnode(createRPCAvatarnode(avatarNodeAddr, conf,
          UnixUserGroupInformation.login(conf, true)));
    } catch (LoginException e) {
      throw (IOException) (new IOException().initCause(e));
    }
  }

  private static AvatarProtocol createRPCAvatarnode(
      InetSocketAddress avatarNodeAddr, Configuration conf,
      UnixUserGroupInformation ugi) throws IOException {
    LOG.info("AvatarShell connecting to " + avatarNodeAddr);
    return (AvatarProtocol) RPC.getProxy(AvatarProtocol.class,
        AvatarProtocol.versionID, avatarNodeAddr, ugi, conf,
        NetUtils.getSocketFactory(conf, AvatarProtocol.class));
  }

  private static AvatarProtocol createAvatarnode(AvatarProtocol rpcAvatarnode)
      throws IOException {
    RetryPolicy createPolicy = RetryPolicies
        .retryUpToMaximumCountWithFixedSleep(5, 5000, TimeUnit.MILLISECONDS);

    Map<Class<? extends Exception>, RetryPolicy> remoteExceptionToPolicyMap = new HashMap<Class<? extends Exception>, RetryPolicy>();

    Map<Class<? extends Exception>, RetryPolicy> exceptionToPolicyMap = new HashMap<Class<? extends Exception>, RetryPolicy>();
    exceptionToPolicyMap.put(RemoteException.class, RetryPolicies
        .retryByRemoteException(RetryPolicies.TRY_ONCE_THEN_FAIL,
            remoteExceptionToPolicyMap));
    RetryPolicy methodPolicy = RetryPolicies.retryByException(
        RetryPolicies.TRY_ONCE_THEN_FAIL, exceptionToPolicyMap);
    Map<String, RetryPolicy> methodNameToPolicyMap = new HashMap<String, RetryPolicy>();

    methodNameToPolicyMap.put("create", methodPolicy);

    return (AvatarProtocol) RetryProxy.create(AvatarProtocol.class,
        rpcAvatarnode, methodNameToPolicyMap);
  }

  private void checkOpen() throws IOException {
    if (!clientRunning) {
      IOException result = new IOException("AvatarNode closed");
      throw result;
    }
  }

  /**
   * Close the connection to the avatarNode.
   */
  public synchronized void close() throws IOException {
    if (clientRunning) {
      clientRunning = false;
      RPC.stopProxy(rpcAvatarnode);
    }
  }

  /**
   * Displays format of commands.
   */
  private static void printUsage(String cmd) {
    String prefix = "Usage: java " + AvatarZKShell.class.getSimpleName();
    if ("-updateZK".equals(cmd)) {
      System.err
          .println("Usage: java AvatarShell" + " [-updateZK -{zero|one}]");
    } else if ("-clearZK".equals(cmd)) {
      System.err.println("Usage: java AvatarShell" + " [-clearZK]");
    } else {
      System.err.println("Usage: java AvatarZKShell");
      System.err.println("           [-updateZK -{zero|one} [-force]]");
      System.err.println("           [-clearZK -force]");
      System.err.println("           [-createZK -{zero|one}]");
      System.err.println("           [-getZK]");
      System.err.println();
      ToolRunner.printGenericCommandUsage(System.err);
    }
  }

  private int updateConf(String instance) {
    if (instance.equalsIgnoreCase(StartupOption.NODEONE.getName())) {
      FileSystem.setDefaultUri(conf, conf.get("fs.default.name1"));
      if (originalConf.get("dfs.namenode.dn-address1") != null) {
        conf.set("dfs.namenode.dn-address",
            originalConf.get("dfs.namenode.dn-address1"));
      }
      conf.set("dfs.http.address", originalConf.get("dfs.http.address1"));
    } else if (instance.equalsIgnoreCase(StartupOption.NODEZERO.getName())) {
      FileSystem.setDefaultUri(conf, conf.get("fs.default.name0"));
      if (originalConf.get("dfs.namenode.dn-address0") != null) {
        conf.set("dfs.namenode.dn-address",
            originalConf.get("dfs.namenode.dn-address0"));
      }
      conf.set("dfs.http.address", originalConf.get("dfs.http.address0"));
    } else {
      return -1;
    }
    return 0;
  }

  /**
   * run
   */
  public int run(String argv[]) throws Exception {

    if (argv.length < 1) {
      printUsage("");
      return -1;
    }

    int exitCode = -1;
    int i = 0;

    String cmd = argv[i++];
    //
    // verify that we have enough command line parameters
    //
    if ("-updateZK".equals(cmd)) {
      if (argv.length < 2) {
        printUsage(cmd);
        return exitCode;
      }
      String instance = argv[i++];
      if (updateConf(instance) == -1) {
        printUsage(cmd);
        return exitCode;
      }

    } else if ("-clearZK".equals(cmd)) {
      if (argv.length > 1) {
        printUsage(cmd);
        return exitCode;
      }
    } else if ("-createZK".equals(cmd)) {
      if (argv.length < 2) {
        printUsage(cmd);
        return exitCode;
      }
      String instance = argv[i++];
      if (updateConf(instance) == -1) {
        printUsage(cmd);
        return exitCode;
      }
    } else if ("-getZK".equals(cmd)) {
      if (argv.length > 1) {
        printUsage(cmd);
        return exitCode;
      }
    }

    try {
      if ("-updateZK".equals(cmd)) {
        boolean force = false;
        if (argv.length == 3 && argv[i++].equals("-force"))
          force = true;
        updateZooKeeper(force);
      } else if ("-clearZK".equals(cmd)) {
        clearZooKeeper();
      } else if ("-createZK".equals(cmd)) {
        updateZooKeeper(true, false);
      } else if ("-getZK".equals(cmd)) {
        getZooKeeper();
      } else {
        exitCode = -1;
        System.err.println(cmd.substring(1) + ": Unknown command");
        printUsage("");
      }
    } catch (IllegalArgumentException arge) {
      exitCode = -1;
      arge.printStackTrace();
      System.err.println(cmd.substring(1) + ": " + arge.getLocalizedMessage());
      printUsage(cmd);
    } catch (RemoteException e) {
      exitCode = -1;
      try {
        String[] content;
        content = e.getLocalizedMessage().split("\n");
        System.err.println(cmd.substring(1) + ": " + content[0]);
      } catch (Exception ex) {
        System.err.println(cmd.substring(1) + ": " + ex.getLocalizedMessage());
      }
    } catch (IOException e) {
      //
      // IO exception encountered locally.
      //
      exitCode = -1;
      System.err.println(cmd.substring(1) + ": " + e.getLocalizedMessage());
    } catch (Throwable re) {
      exitCode = -1;
      System.err.println(cmd.substring(1) + ": " + re.getLocalizedMessage());
    } finally {
    }
    return exitCode;
  }

  public void getZooKeeper() throws IOException, KeeperException, InterruptedException {
    String connection = conf.get("fs.ha.zookeeper.quorum");
    if (connection == null)
      return;
    AvatarZooKeeperClient zk = new AvatarZooKeeperClient(conf, null);
    InetSocketAddress defaultAddr = NameNode.getAddress(originalConf);
    String defaultName = defaultAddr.getHostName() + ":"
        + defaultAddr.getPort();
    String registration = zk.getPrimaryAvatarAddress(defaultName, new Stat(), false);
    
    System.out.println("Primary node according to ZooKeeper: " + registration);
  }
  public void clearZooKeeper() throws IOException {
    String connection = conf.get("fs.ha.zookeeper.quorum");
    if (connection == null)
      return;
    AvatarZooKeeperClient zk = new AvatarZooKeeperClient(conf, null);

    // Clear NameNode address in ZK
    System.out.println("Clear Client Address");
    LOG.info("Clear Client Address information in ZooKeeper");
    InetSocketAddress defaultAddr;
    String[] aliases;

    defaultAddr = NameNode.getAddress(originalConf);

    String defaultName = defaultAddr.getHostName() + ":"
        + defaultAddr.getPort();

    zk.clearPrimary(defaultName);

    aliases = conf.getStrings("fs.default.name.aliases");
    if (aliases != null) {
      for (String alias : aliases) {
        zk.clearPrimary(alias);
      }
    }
    System.out.println("Clear Service Address");
    LOG.info("Clear Service Address information in ZooKeeper");
    // Clear service address in ZK

    defaultAddr = NameNode.getDNProtocolAddress(originalConf);
    if (defaultAddr != null) {
      String defaultServiceName = defaultAddr.getHostName() + ":"
          + defaultAddr.getPort();
      zk.clearPrimary(defaultServiceName);
    }
    aliases = conf.getStrings("dfs.namenode.dn-address.aliases");
    if (aliases != null) {
      for (String alias : aliases) {
        zk.clearPrimary(alias);
      }
    }
    System.out.println("Clear Http Address");
    LOG.info("Clear Http Address information in ZooKeeper");
    // Clear http address in ZK
    // Stolen from NameNode so we have the same code in both places
    defaultAddr = NetUtils.createSocketAddr(originalConf
        .get("dfs.http.address"));

    String defaultHttpAddress = defaultAddr.getHostName() + ":"
        + defaultAddr.getPort();

    zk.clearPrimary(defaultHttpAddress);

    aliases = conf.getStrings("dfs.http.address.aliases");
    if (aliases != null) {
      for (String alias : aliases) {
        zk.clearPrimary(alias);
      }
    }
  }
  
  public void updateZooKeeper(boolean force) throws IOException {
    updateZooKeeper(force, true);
  }

  /*
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
  public void updateZooKeeper(boolean force, boolean overwrite)
      throws IOException {
    if (!force) {
      initAvatarRPC();
      Avatar avatar = avatarnode.getAvatar();
      if (avatar != Avatar.ACTIVE) {
        throw new IOException(
            "Cannot update ZooKeeper information to point to "
                + "the AvatarNode in Standby mode");
      }
    }
    String connection = conf.get("fs.ha.zookeeper.quorum");
    if (connection == null)
      return;
    AvatarZooKeeperClient zk = new AvatarZooKeeperClient(conf, null);

    // Update NameNode address in ZK
    System.out.println("Update Client Address");
    LOG.info("Update Client Address information in ZooKeeper");
    InetSocketAddress defaultAddr;
    String[] aliases;
    InetSocketAddress addr = NameNode.getAddress(conf);

    String primaryAddress = addr.getHostName() + ":" + addr.getPort();
    defaultAddr = NameNode.getAddress(originalConf);

    String defaultName = defaultAddr.getHostName() + ":"
        + defaultAddr.getPort();
    zk.registerPrimary(defaultName, primaryAddress, overwrite);
    aliases = conf.getStrings("fs.default.name.aliases");
    if (aliases != null) {
      for (String alias : aliases) {
        zk.registerPrimary(alias, primaryAddress, overwrite);
      }
    }
    System.out.println("Update Service Address");
    LOG.info("Update Service Address information in ZooKeeper");
    // Update service address in ZK
    addr = NameNode.getDNProtocolAddress(conf);

    defaultAddr = NameNode.getDNProtocolAddress(originalConf);
    if (defaultAddr != null) {
      String primaryServiceAddress = addr.getHostName() + ":" + addr.getPort();
      String defaultServiceName = defaultAddr.getHostName() + ":"
          + defaultAddr.getPort();
      zk.registerPrimary(defaultServiceName, primaryServiceAddress, overwrite);
    }
    aliases = conf.getStrings("dfs.namenode.dn-address.aliases");
    if (aliases != null) {
      String primaryServiceAddress = addr.getHostName() + ":" + addr.getPort();
      for (String alias : aliases) {
        zk.registerPrimary(alias, primaryServiceAddress, overwrite);
      }
    }
    System.out.println("Update Http Address");
    LOG.info("Update Http Address information in ZooKeeper");
    // Update http address in ZK
    // Stolen from NameNode so we have the same code in both places
    addr = NetUtils.createSocketAddr(conf.get("dfs.http.address"));
    String primaryHttpAddress = addr.getHostName() + ":" + addr.getPort();
    defaultAddr = NetUtils.createSocketAddr(originalConf
        .get("dfs.http.address"));

    String defaultHttpAddress = defaultAddr.getHostName() + ":"
        + defaultAddr.getPort();
    zk.registerPrimary(defaultHttpAddress, primaryHttpAddress, overwrite);

    aliases = conf.getStrings("dfs.http.address.aliases");
    if (aliases != null) {
      for (String alias : aliases) {
        zk.registerPrimary(alias, primaryHttpAddress, overwrite);
      }
    }
  }

  public static class DummyWatcher implements Watcher {

    @Override
    public void process(WatchedEvent event) {
      // This is a dummy watcher since we are only doing creates and deletes
    }
  }

  /**
   * main() has some simple utility methods
   */
  public static void main(String argv[]) throws Exception {
    AvatarZKShell shell = null;

    shell = new AvatarZKShell();
    int res;
    try {
      res = ToolRunner.run(shell, argv);
    } finally {
      shell.close();
    }
    System.exit(res);
  }
}
