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
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import javax.security.auth.login.LoginException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hdfs.protocol.AvatarProtocol;
import org.apache.hadoop.hdfs.protocol.AvatarConstants.Avatar;
import org.apache.hadoop.hdfs.protocol.AvatarConstants.InstanceId;
import org.apache.hadoop.hdfs.protocol.AvatarConstants.StartupOption;
import org.apache.hadoop.hdfs.server.namenode.AvatarNode;
import org.apache.hadoop.hdfs.server.namenode.AvatarNodeZkUtil;
import org.apache.hadoop.io.retry.RetryPolicies;
import org.apache.hadoop.io.retry.RetryPolicy;
import org.apache.hadoop.io.retry.RetryProxy;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.UnixUserGroupInformation;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;

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
    Configuration.addDefaultResource("avatar-default.xml");
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
    this.conf = this.originalConf = conf;
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
    if ("-updateZK".equals(cmd)) {
      System.err.println("Usage: java AvatarZKShell [-service serviceName]"
          + " [-updateZK -{zero|one}]");
    } else if ("-clearZK".equals(cmd)) {
      System.err.println("Usage: java AvatarZKShell [-service serviceName]"
          + " [-clearZK]");
    } else {
      System.err.println("Usage: java AvatarZKShell [-service serviceName]");
      System.err.println("           [-updateZK -{zero|one} [-force]]");
      System.err.println("           [-clearZK]");
      System.err.println("           [-createZK -{zero|one}]");
      System.err.println("           [-getZK]");
      System.err.println();
      ToolRunner.printGenericCommandUsage(System.err);
    }
  }

  public static Configuration updateConf(String instance, Configuration conf) {
    InstanceId instanceId = InstanceId.NODEZERO;

    // Some instances might use fs.default.name0 and some might use
    // dfs.namenode.rpc-address.
    if (instance.equalsIgnoreCase(StartupOption.NODEONE.getName())) {
      if (conf.get(AvatarNode.DFS_NAMENODE_RPC_ADDRESS1_KEY) == null
          && conf.get("fs.default.name1") == null) {
        LOG.error("Configuration error: Cannot find dfs.namenode.rpc-address1 or fs.default.name1");
        return null;
      }
      instanceId = InstanceId.NODEONE;
    } else if (instance.equalsIgnoreCase(StartupOption.NODEZERO.getName())) {
      if (conf.get(AvatarNode.DFS_NAMENODE_RPC_ADDRESS0_KEY) == null
          && conf.get("fs.default.name0") == null) {
        LOG.error("Configuration error: Cannot find dfs.namenode.rpc-address0 or fs.default.name0");
        return null;
      }
      instanceId = InstanceId.NODEZERO;
    } else {
      return null;
    }

    return AvatarNode.updateAddressConf(conf, instanceId);
  }

  /**
   * run
   */
  public int run(String argv[]) throws Exception {

    if (argv.length < 1) {
      printUsage("");
      return -1;
    }

    int exitCode = 0;
    String zkCmd = null;
    String serviceName = null;
    String instance = null;
    boolean force = false;
    
    // enforce to specify service if configured
    if (!AvatarShell.isServiceSpecified("AvatarZKShell", conf, argv)) {
      return -1;
    }

    for(int i=0; i < argv.length; i++) {
      String cmd = argv[i];

      //
      // verify that we have enough command line parameters
      //
      if ("-service".equals(cmd)) {
        if (i+1 == argv.length ) {
          printUsage(cmd);
          return -1;
        }
        serviceName = argv[++i];
      } else if ("-updateZK".equals(cmd)) {
        zkCmd = cmd;
        if (i+1 == argv.length ) {
          printUsage(cmd);
          return -1;
        }
        instance = argv[++i];
        if (i+1 < argv.length && argv[i+1].equals("-force")) {
          force = true;
          i++;
        }
      } else if ("-clearZK".equals(cmd)) {
        zkCmd = cmd;
      } else if ("-createZK".equals(cmd)) {
        zkCmd = cmd;
        if (i+1 == argv.length) {
          printUsage(cmd);
          return -1;
        }
        instance = argv[++i];
      } else if ("-getZK".equals(cmd)) {
        zkCmd = cmd;
      }  else {
        exitCode = -1;
        System.err.println(cmd.substring(1) + ": Unknown command");
        printUsage("");
      }
    }

    // make sure that command is provided
    if (zkCmd == null ) {
      printUsage("");
      return -1;
    }

    Collection<String> services;
    if (serviceName != null) { // for one service
      if (!AvatarNode.validateServiceName(conf, serviceName)) {
        return -1;
      }
      services = new ArrayList<String>(1);
      services.add(serviceName);
    } else { // for all configured services
      services = DFSUtil.getNameServiceIds(conf);
      if (services.size() == 0) {
        //non-federation
        services.add("");
      }
    }

    try {
      for (String service : services) {
        AvatarNode.initializeGenericKeys(originalConf, service);
        if ("-updateZK".equals(zkCmd) || "-createZK".equals(zkCmd)) {
          if (instance == null || (conf=updateConf(instance, originalConf)) == null) {
            printUsage(zkCmd);
            return -1;
          }
        }
        
        if ("-updateZK".equals(zkCmd)) {
          updateZooKeeper(force, service, instance);
        } else if ("-clearZK".equals(zkCmd)) {
          clearZooKeeper(service);
        } else if ("-createZK".equals(zkCmd)) {
          updateZooKeeper(true, false, service, instance);
        } else if ("-getZK".equals(zkCmd)) {
          getZooKeeper(service);
        }
      }
    } catch (IllegalArgumentException arge) {
      exitCode = -1;
      arge.printStackTrace();
      System.err.println(zkCmd.substring(1) + ": " + arge.getLocalizedMessage());
      printUsage(zkCmd);
    } catch (RemoteException e) {
      exitCode = -1;
      try {
        String[] content;
        content = e.getLocalizedMessage().split("\n");
        System.err.println(zkCmd.substring(1) + ": " + content[0]);
      } catch (Exception ex) {
        System.err.println(zkCmd.substring(1) + ": " + ex.getLocalizedMessage());
      }
    } catch (IOException e) {
      //
      // IO exception encountered locally.
      //
      exitCode = -1;
      System.err.println(zkCmd.substring(1) + ": " + e.getLocalizedMessage());
      e.printStackTrace();
    } catch (Throwable re) {
      exitCode = -1;
      System.err.println(zkCmd.substring(1) + ": " + re.getLocalizedMessage());
      re.printStackTrace();
    } finally {
    }
    return exitCode;
  }

  public void getZooKeeper(String serviceName) throws IOException, KeeperException, InterruptedException {
    AvatarNodeZkUtil.printZookeeperEntries(originalConf, conf, serviceName, System.out);
  }

  public void clearZooKeeper(String serviceName) throws IOException {
    AvatarNodeZkUtil.clearZookeeper(originalConf, conf, serviceName);
  }

  public void updateZooKeeper(boolean force, String serviceName, String instance) throws IOException {
    updateZooKeeper(force, true, serviceName, instance);
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
  public void updateZooKeeper(boolean force, boolean toOverwrite, String serviceName, String primary)
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
    AvatarNodeZkUtil.updateZooKeeper(originalConf, conf, toOverwrite, serviceName, primary);
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
    DnsMonitorSecurityManager.setTheManager();
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
