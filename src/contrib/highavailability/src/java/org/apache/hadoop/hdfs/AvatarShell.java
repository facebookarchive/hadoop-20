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

import java.io.BufferedReader;
import java.io.InputStreamReader;
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
import org.apache.hadoop.hdfs.protocol.AvatarConstants.InstanceId;
import org.apache.hadoop.hdfs.protocol.AvatarConstants.StartupOption;
import org.apache.hadoop.hdfs.protocol.AvatarProtocol;
import org.apache.hadoop.hdfs.protocol.AvatarConstants.Avatar;
import org.apache.hadoop.hdfs.protocol.FSConstants;
import org.apache.hadoop.hdfs.protocol.FSConstants.SafeModeAction;
import org.apache.hadoop.hdfs.server.namenode.AvatarNode;
import org.apache.hadoop.hdfs.server.namenode.AvatarNodeZkUtil;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.server.namenode.StandbyStateException;
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
  
  // used when waiting for last txid from primary, 
  // by default polling zk every second
  public static long retrySleep = 1 * 1000;

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

  public void initAvatarRPC(String address) throws IOException {
    InetSocketAddress addr = null;
    if (address != null) {
      addr = NetUtils.createSocketAddr(address);
    } else {
      addr = AvatarNode.getAddress(conf);
    }
    
    try {
      this.ugi = UnixUserGroupInformation.login(conf, true);
    } catch (LoginException e) {
      throw (IOException) (new IOException().initCause(e));
    }

    this.rpcAvatarnode = createRPCAvatarnode(addr, conf, ugi);
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
  static void printUsage() {
    System.err.println("Usage: java AvatarShell");
    System.err.println("           [-waittxid] [-service serviceName]");
    System.err.println("           [-failover] [-service serviceName]");
    System.err.println("           [-prepfailover] [-service serviceName]");
    System.err.println("           [-{zero|one} -showAvatar] [-service serviceName]");
    System.err.println("           [-{zero|one} -setAvatar primary [force]] [-service serviceName]");
    System.err.println("           [-{zero|one} -shutdownAvatar] [-service serviceName]");
    System.err.println("           [-{zero|one} -safemode enter|leave|get|wait|initqueues] [-service serviceName]");
    System.err.println("           [-{zero|one} -metasave filename] [-service serviceName]");    
    System.err.println("           [-{zero|one} -isInitialized] [-service serviceName]");
    System.err.println("           [-{zero|one} -saveNamespace [force] [uncompressed]] [-service serviceName]");
    System.err.println();
    ToolRunner.printGenericCommandUsage(System.err);
  }

  private boolean isPrimary(Configuration conf, String zkRegistration) {
    String actualName = conf.get(NameNode.DFS_NAMENODE_RPC_ADDRESS_KEY);
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
    String address = conf.get(NameNode.DFS_NAMENODE_RPC_ADDRESS_KEY);
    long maxWaitTime = this.getMaxWaitTimeForWaitTxid();
    long start = System.currentTimeMillis();
    while (true) {
      if (System.currentTimeMillis() - start > maxWaitTime) {
        throw new IOException("No valid last txid znode found");
      }
      try {
        long sessionId = zk.getPrimarySsId(address, false);
        ZookeeperTxId zkTxId = zk.getPrimaryLastTxId(address, false);
        if (sessionId != zkTxId.getSessionId()) {
          LOG.warn("Session Id in the ssid node : " + sessionId
              + " does not match the session Id in the txid node : "
              + zkTxId.getSessionId() + " retrying...");
          Thread.sleep(retrySleep);
          continue;
        }
      } catch (Throwable e) {
        LOG.warn("Caught exception : " + e + " retrying ...");
        Thread.sleep(retrySleep);
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

  private int failover(String serviceName, boolean prepareOnly) throws Exception {
    AvatarZooKeeperClient zk = new AvatarZooKeeperClient(conf, null);
    
    String prefix = "Failover" + (prepareOnly ? " (prepare):" : ":");
    System.out.println(prefix + " START");
    LOG.info(prefix + " START");
    
    long start = System.currentTimeMillis();
    try {
      String defaultName = conf.get(NameNode.DFS_NAMENODE_RPC_ADDRESS_KEY);
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
      
      // perform pre-failover health check
      cmd = getAvatarCommand(serviceName, "-zero", "-isInitialized");
      runCommand(shell, cmd, "-zero is not initialized");
      cmd = getAvatarCommand(serviceName, "-one", "-isInitialized");
      runCommand(shell, cmd, "-one is not initialized");
      
      String primary = zeroPrimary ? "-zero" : "-one";
      String standby = zeroPrimary ? "-one" : "-zero";
      
      if (prepareOnly) {
        // instruct standby that we are about to failover
        cmd = getAvatarCommand(serviceName, standby, "-safemode", "prepfailover");
        runCommand(shell, cmd, standby + " prepare failover failed");
        // initialize replication queues on standby
        cmd = getAvatarCommand(serviceName, standby, "-safemode", "initqueues");
        runCommand(shell, cmd, standby
            + " standby replication queues initialization failed");
        return 0;
      } else {      
        // perform actual failover
        cmd = getAvatarCommand(serviceName, primary, "-shutdownAvatar");
        runCommand(shell, cmd, primary + " shutdownAvatar failed");
        waitForLastTxIdNode(zk, originalConf);
        cmd = getAvatarCommand(serviceName, standby, "-setAvatar", "primary");
        return shell.run(cmd);
      }
      
    } finally {
      zk.shutdown();
      long stop = System.currentTimeMillis();
      String msg = prefix + " DONE - Time taken: " + ((stop - start) / 1000.0f)
          + " sec.";
      System.out.println(msg);
      LOG.info(msg);
    }
  }
  
  private void runCommand(AvatarShell shell, String[] cmd, String failureMessage)
      throws Exception {
    if (shell.run(cmd) != 0) {
      throw new IOException(failureMessage);
    }
  }

  private boolean processServiceName(String serviceName, boolean failOnError)
      throws IOException {
    // validate service name
    if (serviceName != null) {
      if (!AvatarNode.validateServiceName(conf, serviceName)) {
        if (failOnError) {
          throw new IOException("Wrong service name");
        }
        return false;
      }

      // remove the service name suffix
      AvatarNode.initializeGenericKeys(conf, serviceName);
    }
    return true;
  }
  
  private void printError(Throwable e) {
    System.err.println(e.getLocalizedMessage());
  }

  /**
   * run
   */
  public int run(String argv[]) throws Exception {
    if (argv.length < 1) {
      printUsage();
      return -1;
    }
    
    AvatarShellCommand cmd = AvatarShellCommand.parseCommand(argv);
    if (cmd == null) {
      printUsage();
      return -1;
    }
    
    int exitCode = 0;
  
    if (conf.get(FSConstants.DFS_FEDERATION_NAMESERVICES) != null
        && (!cmd.isServiceCommand) && (!cmd.isAddressCommand)) {
      printServiceErrorMessage("AvatarShell", conf);
      return -1;
    }
    
    String serviceName = null;
    if (cmd.isServiceCommand) {
      serviceName = cmd.serviceArgs[0];
    }
    
    // commands without -{zero|one} prefix
    if (cmd.isWaitTxIdCommand) {
      AvatarZooKeeperClient zk = new AvatarZooKeeperClient(conf, null);
      try {
        processServiceName(serviceName, true);
        waitForLastTxIdNode(zk, originalConf);
      } catch (Exception e) {
        exitCode = -1;
        printError(e);
      } finally {
        zk.shutdown();
      }
      if (exitCode == 0) {
        LOG.info("Primary shutdown was successful!");
      }
      return exitCode;
    }

    if (cmd.isFailoverCommand || cmd.isPrepfailoverCommand) {
      boolean prep = cmd.isPrepfailoverCommand;
      try {
        processServiceName(serviceName, true);
        exitCode = failover(serviceName, prep);
      } catch (Exception e) {
        exitCode = -1;
        printError(e);
      }
      String prefix = prep ? "Prep" : "";
      if (exitCode == 0) {
        LOG.info(prefix + "Failover was successful!");
        if (prep) {
          System.out
              .println("WARNING: Standby is in pre-failover state! If the failover "
                  + "is not performed, the standby needs to be restarted to "
                  + "continue checkpointing.");
        }
      } else {
        LOG.error(prefix + "Failover failed!");
        if (prep) {
          System.out
              .println("WARNING: Standby is in bad state! Restart the standby node!");
        }
      }
      return exitCode;
    }
    
    /////////////////////// direct commands (-zero -one -address)
    
    String address = cmd.isAddressCommand ? cmd.addressArgs[0] : null;
    String instance = cmd.isZeroCommand ? StartupOption.NODEZERO.getName()
        : (cmd.isOneCommand ? StartupOption.NODEONE.getName() : null);

    if (!processServiceName(serviceName, false)) {
      return -1;
    }

    // remove 0/1 suffix
    if (instance != null) {
      if ((conf = AvatarZKShell.updateConf(instance, originalConf)) == null) {
        printUsage();
        return -1;
      }
    } 
    initAvatarRPC(address);

    try {
      if (cmd.isShowAvatarCommand) {
        exitCode = showAvatar();
      } else if (cmd.isSetAvatarCommand) {
        exitCode = setAvatar("primary", contains(cmd.setAvatarArgs, "force"), serviceName, instance);
      } else if (cmd.isIsInitializedCommand) {
        exitCode = isInitialized();
      } else if (cmd.isMetasaveCommand) {
        exitCode = metasave(cmd.metasageArgs[0]);
      } else if (cmd.isSaveNamespaceCommand) {
        exitCode = saveNamespace(cmd.getSaveNamespaceArgs());
      } else if (cmd.isShutdownAvatarCommand) {
        shutdownAvatar(serviceName);
      } else if (cmd.isSafemodeCommand) {
        processSafeMode(cmd.safemodeArgs[0]);
      } else {
        exitCode = -1;
        System.err.println("Unknown command");
        printUsage();
      }
    } catch (IllegalArgumentException arge) {
      exitCode = -1;
      arge.printStackTrace();
      printError(arge);
      printUsage();
    } catch (RemoteException e) {
      //
      // This is a error returned by avatarnode server. Print
      // out the first line of the error mesage, ignore the stack trace.
      exitCode = -1;
      try {
        String[] content;
        content = e.getLocalizedMessage().split("\n");
        System.err.println(content[0]);
      } catch (Exception ex) {
        System.err.println(ex.getLocalizedMessage());
      }
    } catch (IOException e) {
      //
      // IO exception encountered locally.
      // 
      exitCode = -1;
      printError(e);
    } catch (Throwable re) {
      exitCode = -1;
      printError(re);
    } finally {
    }
    if (exitCode == 0) {
      LOG.info("Command was successful!");
    }
    return exitCode;
  }
  
  private boolean contains(String[] args, String arg) {
    if (args == null)
      return false;
    for(String s : args) {
      if (s.equalsIgnoreCase(arg))
        return true;
    }
    return false;
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
  
  private int metasave(String filename)
      throws IOException {
    try {
      avatarnode.metaSave(filename);
      return 0;
    } catch (Exception e) {
      LOG.error("Exception when saving metadata", e);
      return 1;
    }
  }
  
  public int saveNamespace(List<String> args) throws IOException {
    int exitCode = -1;
    boolean force = false;
    boolean uncompressed = false;
    for(String arg : args) {
      if (arg.equals("force")) {
        force = true;
      } else if (arg.equals("uncompressed")) {
        uncompressed = true;
      } else {
        printUsage();
        return exitCode;
      }
    }
    avatarnode.saveNamespace(force, uncompressed);
    return 0;
  }

  public static void handleRemoteException(RemoteException re) throws IOException {
    IOException ie = re.unwrapRemoteException();
    if (!(ie instanceof StandbyStateException)) {
      throw re;
    }
    BufferedReader in = new BufferedReader(new InputStreamReader(
          System.in));
    String input = null;
    do {
      System.out.println("The Standby's state is incorrect : " + ie
          + "\n. You can still force a failover after some manual "
          + "verification. This is an EXTEREMELY DANGEROUS operation if "
          + "you don't know what you are doing. Do you wish to "
          + "continue with forcing the failover ? (Y/N)");
      input = in.readLine();
    } while (input == null || (!input.equalsIgnoreCase("Y") && !input
          .equalsIgnoreCase("N")));
    if (input.equalsIgnoreCase("N")) {
      throw re;
    }
  }

  /**
   * Sets the avatar to the specified value
   */
  public int setAvatar(String role, boolean noverification, String serviceName, String instance)
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
      try {
        avatarnode.quiesceForFailover(noverification);
      } catch (RemoteException re) {
        handleRemoteException(re);
      }
      avatarnode.performFailover();
      updateZooKeeper(serviceName , instance);
    }
    return 0;
  }
  
  public void shutdownAvatar(String serviceName) throws IOException {
    clearZooKeeper(serviceName);
    avatarnode.shutdownAvatar();
  }
  
  public void processSafeMode(String safeModeAction) throws IOException {
    SafeModeAction action = null;
    boolean waitExitSafe = false;
    
    if (safeModeAction.equals("leave")) {
      action = SafeModeAction.SAFEMODE_LEAVE;
    } else if (safeModeAction.equals("get")) {
      action = SafeModeAction.SAFEMODE_GET;
    } else if (safeModeAction.equals("enter")) {
      action = SafeModeAction.SAFEMODE_ENTER; 
    } else if (safeModeAction.equals("initqueues")) {
      action = SafeModeAction.SAFEMODE_INITQUEUES; 
    } else if (safeModeAction.equals("prepfailover")) {
      action = SafeModeAction.SAFEMODE_PREP_FAILOVER;
    } else if (safeModeAction.equals("wait")) {
      action = SafeModeAction.SAFEMODE_GET; 
      waitExitSafe = true;
    }
    
    if (action == null) {
      System.err.println("Invalid safemode action : " + safeModeAction);
      printUsage();
      return;
    }
    
    boolean inSafeMode = avatarnode.setSafeMode(action);
    
    //
    // If we are waiting for safemode to exit, then poll and
    // sleep till we are out of safemode.
    //
    while (inSafeMode && waitExitSafe) {
      System.out.println("Safe mode is " + (inSafeMode ? "ON" : "OFF")
          + ". Waiting for safemode to be OFF.");
      try {
        Thread.sleep(5000);
      } catch (java.lang.InterruptedException e) {
        throw new IOException("Wait Interrupted");
      }
      inSafeMode = avatarnode.setSafeMode(action);
    }

    System.out.println("Safe mode is " + (inSafeMode ? "ON" : "OFF"));
  }

  public void clearZooKeeper(String serviceName) throws IOException {
    Avatar avatar = avatarnode.getAvatar();
    if (avatar != Avatar.ACTIVE) {
      throw new IOException("Cannot clear zookeeper because the node " +
      		" provided is not Primary");
    }
    AvatarNodeZkUtil.clearZookeeper(originalConf, conf, serviceName);
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
  public void updateZooKeeper(String serviceName, String instance) throws IOException {
    Avatar avatar = avatarnode.getAvatar();
    if (avatar != Avatar.ACTIVE) {
      throw new IOException("Cannot update ZooKeeper information to point to " +
      		"the AvatarNode in Standby mode");
    }
    AvatarNodeZkUtil.updateZooKeeper(originalConf, conf, true, serviceName, instance);
  }
  
  public static void printServiceErrorMessage(String command, Configuration conf) {
    System.err.println(command
        + " must specify a service to operate on when "
        + "dfs.federation.nameservices is set in the cluster config\n"
        + "Nameservices available: "
        + conf.get(FSConstants.DFS_FEDERATION_NAMESERVICES));
  }
  
  /**
   * Checks if the service argument is specified in the command arguments.
   */
  public static boolean isServiceSpecified(String command, Configuration conf,
      String[] argv) {
    if (conf.get(FSConstants.DFS_FEDERATION_NAMESERVICES) != null) {
      for (int i = 0; i < argv.length; i++) {
        if (argv[i].equals("-service")) {
          // found service specs
          return true;
        }
      }
      // no service specs
      printServiceErrorMessage(command, conf);
      return false;
    }
    return true;
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
