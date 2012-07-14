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
import org.apache.hadoop.hdfs.protocol.AvatarConstants.InstanceId;
import org.apache.hadoop.hdfs.protocol.AvatarProtocol;
import org.apache.hadoop.hdfs.protocol.AvatarConstants.Avatar;
import org.apache.hadoop.hdfs.protocol.AvatarConstants.StartupOption;
import org.apache.hadoop.hdfs.protocol.FSConstants.SafeModeAction;
import org.apache.hadoop.hdfs.server.namenode.AvatarNode;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.server.namenode.ZookeeperTxId;
import org.apache.hadoop.io.retry.RetryPolicies;
import org.apache.hadoop.io.retry.RetryPolicy;
import org.apache.hadoop.io.retry.RetryProxy;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.UnixUserGroupInformation;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.Stat;

/**
 * A {@link AvatarShell} that allows browsing configured avatar policies.
 */
public class AvatarShell extends Configured implements Tool {
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
  public AvatarShell() throws IOException {
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
  public AvatarShell(Configuration conf) {
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
        AvatarProtocol.versionID, avatarNodeAddr, ugi, conf, NetUtils
            .getSocketFactory(conf, AvatarProtocol.class));
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
    if ("-showAvatar".equals(cmd)) {
      System.err.println("Usage: java AvatarShell"
          + " [-{zero|one} -showAvatar] [-service serviceName]");
    } else if ("-setAvatar".equals(cmd)) {
      System.err.println("Usage: java AvatarShell"
              + " [-{zero|one} -setAvatar {primary|standby}] [-force] [-service serviceName]");
    } else if ("-shutdownAvatar".equals(cmd)) {
      System.err.println("Usage: java AvatarShell" +
          " [-{zero|one} -shutdownAvatar] [-service serviceName]");
    } else if ("-failover".equals(cmd)) {
      System.err.println("Usage: java AvatarShell" +
          " [-failover] [-service serviceName]");
    }  else if ("-isInitialized".equals(cmd)) {
        System.err.println("Usage: java AvatarShell" +
            " [-{zero|one} -isInitialized] [-service serviceName]");
    } else if ("-waittxid".equals(cmd)) {
      System.err.println("Usage: java AvatarShell"
          + " [-waittxid] [-service serviceName]");
    } else {
      System.err.println("Usage: java AvatarShell");
      System.err.println("           [-{zero|one} -showAvatar] [-service serviceName]");
      System.err.println("           [-{zero|one} -setAvatar {primary|standby}] [-force] [-service serviceName]");
      System.err.println("           [-{zero|one} -shutdownAvatar] [-service serviceName]");
      System.err.println("           [-{zero|one} -leaveSafeMode] [-service serviceName]");
      System.err.println("           [-failover] [-service serviceName]");
      System.err.println("           [-{zero|one} -isInitialized] [-service serviceName]");
      System.err.println("           [-waittxid] [-service serviceName]");
      System.err.println();
      ToolRunner.printGenericCommandUsage(System.err);
    }
  }

  private boolean isPrimary(Configuration conf, String zkRegistration) {
    InetSocketAddress actualAddr = NameNode.getClientProtocolAddress(conf);
    String actualName = actualAddr.getHostName() + ":" + actualAddr.getPort();
    return actualName.equals(zkRegistration);
  }

  protected long getMaxWaitTimeForWaitTxid() {
    return 1000 * 60 * 10; // 10 minutes.
  }

  /**
   * Waits till the last txid node appears in Zookeeper, such that it matches
   * the ssid node.
   */
  private void waitForLastTxIdNode(AvatarZooKeeperClient zk, Configuration conf)
      throws Exception {
    // Gather session id and transaction id data.
    String address = AvatarNode.getClusterAddress(conf);
    long maxWaitTime = this.getMaxWaitTimeForWaitTxid();
    long start = System.currentTimeMillis();
    while (true) {
      if (System.currentTimeMillis() - start > maxWaitTime) {
        throw new IOException("No valid last txid znode found");
      }
      try {
        long sessionId = zk.getPrimarySsId(address);
        ZookeeperTxId zkTxId = zk.getPrimaryLastTxId(address);
        if (sessionId != zkTxId.getSessionId()) {
          LOG.warn("Session Id in the ssid node : " + sessionId
              + " does not match the session Id in the txid node : "
              + zkTxId.getSessionId() + " retrying...");
          Thread.sleep(10000);
          continue;
        }
      } catch (Throwable e) {
        LOG.warn("Caught exception : " + e + " retrying ...");
        Thread.sleep(10000);
        continue;
      }
      break;
    }
  }

  private String[] getAvatarCommand(String serviceName, String... args) {
    List<String> cmdlist = new ArrayList<String>();
    for (String arg : args) {
      cmdlist.add(arg);
    }
    if (serviceName != null) {
      cmdlist.add("-service");
      cmdlist.add(serviceName);
    }
    return cmdlist.toArray(new String[cmdlist.size()]);
  }

  private int failover(String serviceName) throws Exception {
    AvatarZooKeeperClient zk = new AvatarZooKeeperClient(conf, null);
    try {
      InetSocketAddress defaultAddr = NameNode.getClientProtocolAddress(conf);
      String defaultName = defaultAddr.getHostName() + ":"
          + defaultAddr.getPort();
      String registration = zk.getPrimaryAvatarAddress(defaultName, new Stat(),
          false);

      if (registration == null) {
        throw new IOException("No node found in zookeeper");
      }

      Configuration zeroConf = AvatarNode.updateAddressConf(conf,
          InstanceId.NODEZERO);
      Configuration oneConf = AvatarNode.updateAddressConf(conf,
          InstanceId.NODEONE);

      boolean onePrimary = isPrimary(oneConf, registration);
      boolean zeroPrimary = isPrimary(zeroConf, registration);
      if (!onePrimary && !zeroPrimary) {
        throw new IOException(
            "None of the -zero or -one instances are the primary in zk, zk registration : "
                + registration);
      }

      AvatarShell shell = new AvatarShell(originalConf);

      String[] cmd = null;
      if (zeroPrimary) {
        cmd = getAvatarCommand(serviceName, "-one", "-isInitialized");
        if (shell.run(cmd) != 0) {
          throw new IOException("-one is not initialized");
        }
        cmd = getAvatarCommand(serviceName, "-zero", "-shutdownAvatar");
        if (shell.run(cmd) != 0) {
          throw new IOException("-zero shutdownAvatar failed");
        }
        waitForLastTxIdNode(zk, originalConf);
        cmd = getAvatarCommand(serviceName, "-one", "-setAvatar", "primary");
        return shell.run(cmd);
      } else {
        cmd = getAvatarCommand(serviceName, "-zero", "-isInitialized");
        if (shell.run(cmd) != 0) {
          throw new IOException("-zero is not initialized");
        }
        cmd = getAvatarCommand(serviceName, "-one", "-shutdownAvatar");
        if (shell.run(cmd) != 0) {
          throw new IOException("-one shutdownAvatar failed");
        }
        waitForLastTxIdNode(zk, originalConf);
        cmd = getAvatarCommand(serviceName, "-zero", "-setAvatar", "primary");
        return shell.run(cmd);
      }
    } finally {
      zk.shutdown();
    }
  }

  private boolean processServiceName(String serviceName) {
    // validate service name
    if (serviceName != null) {
      if (!AvatarNode.validateServiceName(conf, serviceName)) {
        return false;
      }

      // remove the service name suffix
      AvatarNode.initializeGenericKeys(conf, serviceName);
    }
    return true;
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
    
    if ("-waittxid".equals(argv[0])) {
      AvatarZooKeeperClient zk = new AvatarZooKeeperClient(conf, null);
      try {
        String serviceName = null;
        if (argv.length == 3 && "-service".equals(argv[1])) {
          serviceName = argv[2];
        }
        if (!processServiceName(serviceName)) {
          return -1;
        }
        waitForLastTxIdNode(zk, originalConf);
      } catch (Exception e) {
        exitCode = -1;
        System.err.println(argv[0].substring(1) + ": "
            + e.getLocalizedMessage());
      } finally {
        zk.shutdown();
      }
      return exitCode;
    }

    if ("-failover".equals(argv[0])) {
      try {
        String serviceName = null;
        if (argv.length == 3 && "-service".equals(argv[1])) {
          serviceName = argv[2];
        }
        if (!processServiceName(serviceName)) {
          return -1;
        }
        exitCode = failover(serviceName);
      } catch (Exception e) {
        exitCode = -1;
        System.err.println(argv[0].substring(1) + ": "
            + e.getLocalizedMessage());
      }
      return exitCode;
    }
    
    int i = 0;
    String instance = argv[i++];
    String cmd = argv[i++];

    // Get the role
    String role = null;
    boolean forceSetAvatar = false;
    if ("-setAvatar".equals(cmd)) {
      if (argv.length < 3) {
        printUsage(cmd);
        return -1;
      }
      role = argv[i++];
      if (i != argv.length && "-force".equals(argv[i])) {
        forceSetAvatar = true;
        i++;
      }
    }

    String serviceName = null;
    if (i != argv.length) {
      if (i+2 != argv.length || !"-service".equals(argv[i])) {
        printUsage(cmd);
        return -1;
      }
      serviceName = argv[i+1];
    }
    
    if (!processServiceName(serviceName)) {
      return -1;
    }

    // remove 0/1 suffix
    if ((conf = AvatarZKShell.updateConf(instance, originalConf)) == null) {
      printUsage(cmd);
      return -1;
    }

    initAvatarRPC();

    try {
      if ("-showAvatar".equals(cmd)) {
        exitCode = showAvatar();
      } else if ("-setAvatar".equals(cmd)) {
        exitCode = setAvatar(role, forceSetAvatar);
      } else if ("-isInitialized".equals(cmd)) {
        exitCode = isInitialized();
      } else if ("-shutdownAvatar".equals(cmd)) {
        shutdownAvatar();
      } else if ("-leaveSafeMode".equals(cmd)) {
        leaveSafeMode();
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
      //
      // This is a error returned by avatarnode server. Print
      // out the first line of the error mesage, ignore the stack trace.
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

  /**
   * Apply operation specified by 'cmd' on all parameters starting from
   * argv[startindex].
   */
  private int showAvatar()
      throws IOException {
    int exitCode = 0;
    Avatar avatar = avatarnode.reportAvatar();
    System.out.println("The current avatar of " + AvatarNode.getAddress(conf)
        + " is " + avatar);
    return exitCode;
  }
  
  private int isInitialized()
      throws IOException {
    int exitCode = avatarnode.isInitialized() ? 0 : -1;
    if (exitCode == 0) {
      LOG.info("Standby has been successfully initialized");
    } else {
      LOG.error("Standby has not been initialized yet");
    }
    return exitCode;
  }

  /**
   * Sets the avatar to the specified value
   */
  public int setAvatar(String role, boolean forceSetAvatar)
      throws IOException {
    Avatar dest;
    if (Avatar.ACTIVE.toString().equalsIgnoreCase(role)) {
      dest = Avatar.ACTIVE;
    } else if (Avatar.STANDBY.toString().equalsIgnoreCase(role)) {
      throw new IOException("setAvatar Command only works to switch avatar" +
      		" from Standby to Primary");
    } else {
      throw new IOException("Unknown avatar type " + role);
    }
    Avatar current = avatarnode.getAvatar();
    if (current == dest) {
      System.out.println("This instance is already in " + current + " avatar.");
    } else {
      avatarnode.setAvatar(dest, forceSetAvatar);
      updateZooKeeper();
    }
    return 0;
  }
  
  public void shutdownAvatar() throws IOException {
    clearZooKeeper();
    avatarnode.shutdownAvatar();
  }
  
  public void leaveSafeMode() throws IOException {
    avatarnode.setSafeMode(SafeModeAction.SAFEMODE_LEAVE);
  }

  public void clearZooKeeper() throws IOException {
    Avatar avatar = avatarnode.getAvatar();
    if (avatar != Avatar.ACTIVE) {
      throw new IOException("Cannot clear zookeeper because the node " +
      		" provided is not Primary");
    }
    String connection = conf.get("fs.ha.zookeeper.quorum");
    if (connection == null)
      return;
    AvatarZooKeeperClient zk = new AvatarZooKeeperClient(conf, null);

    // Clear NameNode address in ZK
    System.out.println("Clear Client Address");
    LOG.info("Clear Client Address information in ZooKeeper");
    InetSocketAddress defaultAddr;
    String[] aliases;

    defaultAddr = NameNode.getClientProtocolAddress(originalConf);

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
    defaultAddr = 
      NetUtils.createSocketAddr(originalConf.get("dfs.http.address"));

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

  /*
   * This method tries to update the information in ZooKeeper
   * For every address of the NameNode it is being run for
   * (fs.default.name, dfs.namenode.dn-address, dfs.namenode.http.address)
   * if they are present.
   * It also creates information for aliases in ZooKeeper for lists of strings
   * in fs.default.name.aliases, dfs.namenode.dn-address.aliases and 
   * dfs.namenode.http.address.aliases
   * 
   * Each address it transformed to the address of the zNode to be created by
   * substituting all . and : characters to /. The slash is also added in the 
   * front to make it a valid zNode address.
   * So dfs.domain.com:9000 will be /dfs/domain/com/9000
   * 
   * If any part of the path does not exist it is created automatically
   * 
   */
  public void updateZooKeeper() throws IOException {
    Avatar avatar = avatarnode.getAvatar();
    if (avatar != Avatar.ACTIVE) {
      throw new IOException("Cannot update ZooKeeper information to point to " +
      		"the AvatarNode in Standby mode");
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
    InetSocketAddress addr = NameNode.getClientProtocolAddress(conf);

    String primaryAddress = addr.getHostName() + ":" + addr.getPort();
    defaultAddr = NameNode.getClientProtocolAddress(originalConf);

    String defaultName = defaultAddr.getHostName() + ":"
        + defaultAddr.getPort();
    zk.registerPrimary(defaultName, primaryAddress);
    aliases = conf.getStrings("fs.default.name.aliases");
    if (aliases != null) {
      for (String alias : aliases) {
        zk.registerPrimary(alias, primaryAddress);
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
      zk.registerPrimary(defaultServiceName, primaryServiceAddress);
    }
    aliases = conf.getStrings("dfs.namenode.dn-address.aliases");
    if (aliases != null) {
      String primaryServiceAddress = addr.getHostName() + ":" + addr.getPort();
      for (String alias : aliases) {
        zk.registerPrimary(alias, primaryServiceAddress);
      }
    }
    System.out.println("Update Http Address");
    LOG.info("Update Http Address information in ZooKeeper");
    // Update http address in ZK
    // Stolen from NameNode so we have the same code in both places
    addr = NetUtils.createSocketAddr(conf.get("dfs.http.address"));
    String primaryHttpAddress = addr.getHostName() + ":" + addr.getPort();
    defaultAddr = 
      NetUtils.createSocketAddr(originalConf.get("dfs.http.address"));

    String defaultHttpAddress = defaultAddr.getHostName() + ":"
        + defaultAddr.getPort();
    zk.registerPrimary(defaultHttpAddress, primaryHttpAddress);

    aliases = conf.getStrings("dfs.http.address.aliases");
    if (aliases != null) {
      for (String alias : aliases) {
        zk.registerPrimary(alias, primaryHttpAddress);
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
    AvatarShell shell = null;
    try {
      shell = new AvatarShell();
    } catch (RPC.VersionMismatch v) {
      System.err.println("Version Mismatch between client and server"
          + "... command aborted.");
      System.exit(-1);
    } catch (IOException e) {
      System.err.println("Bad connection to AvatarNode. command aborted.");
      System.exit(-1);
    }

    int res;
    try {
      res = ToolRunner.run(shell, argv);
    } finally {
      shell.close();
    }
    System.exit(res);
  }
}
