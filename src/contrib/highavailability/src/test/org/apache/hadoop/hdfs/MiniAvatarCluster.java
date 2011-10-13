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

import java.io.FileOutputStream;
import java.io.File;
import java.io.PrintWriter;
import java.io.IOException;
import java.util.Properties;
import java.util.ArrayList;
import java.net.InetSocketAddress;
import java.net.InetAddress;
import java.net.ServerSocket;

import org.apache.zookeeper.server.ZooKeeperServer;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig.ConfigException;
import org.apache.zookeeper.server.ZKDatabase;
import org.apache.zookeeper.server.persistence.FileTxnSnapLog;
import org.apache.zookeeper.server.NIOServerCnxn;
import org.apache.zookeeper.server.ServerConfig;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.protocol.FSConstants.DatanodeReportType;
import org.apache.hadoop.hdfs.protocol.AvatarConstants;
import org.apache.hadoop.hdfs.protocol.FSConstants;
import org.apache.hadoop.hdfs.server.common.HdfsConstants;
import org.apache.hadoop.hdfs.server.namenode.AvatarNode;
import org.apache.hadoop.hdfs.server.datanode.AvatarDataNode;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.net.StaticMapping;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.net.DNSToSwitchMapping;
import org.apache.hadoop.util.StringUtils;

/**
 * This class manages a Avatar/HDFS cluster with all nodes running
 * locally.
 * To synchronize the AvatarNodes, it uses a local ZooKeeper
 * server.
 */
public class MiniAvatarCluster {

  // start handing out ports from this number
  private static final int PORT_START = 10000;

  // the next port that will be handed out (if it is free)
  private volatile static int nextPort = PORT_START;

  /**
   * Check whether a port is free.
   */ 
  static boolean isPortFree(int port) {
    ServerSocket socket = null;
    try {
      socket = new ServerSocket();
      socket.bind(new InetSocketAddress(port));
    } catch (IOException e) {
      return false;
    } finally {
      try {
        if (socket != null) {
          socket.close();
        }
      } catch (IOException ignore) {
        // do nothing
      }
    }
    return true;
  }

  /**
   * Get a free port.
   */
  private static int getFreePort() {
    return getFreePorts(1);
  }

  /**
   * Get the specified number of consecutive free ports.
   * @return the first free port of the range
   */
  private static int getFreePorts(int num) {
    int port = nextPort;

    boolean found = true;
    do {
      for (int i = port; i < port + num; i++) {
        if (!isPortFree(i)) {
          port = i + 1;
          found = false;
          break; // from for loop
        }
      }
    } while (!found);

    nextPort = port + num;
    LOG.info("using free port " + port + "(+" + (num - 1) + ")");
    return port;
  }

  private static class DataNodeProperties {
    AvatarDataNode datanode;
    Configuration conf;
    String[] dnArgs;

    DataNodeProperties(AvatarDataNode node, Configuration conf, String[] args) {
      this.datanode = node;
      this.conf = conf;
      this.dnArgs = args;
    }
  }

  private static enum AvatarState {
    ACTIVE,
    STANDBY,
    DEAD
  }

  private static class AvatarInfo {
    AvatarNode avatar;
    AvatarState state;
    int nnPort;
    int nnDnPort;
    int httpPort;
    String startupOption;
    
    AvatarInfo(AvatarNode avatar, AvatarState state,
               int nnPort, int nnDnPort, int httpPort,
               String startupOption) {
      this.avatar = avatar;
      this.state = state;
      this.nnPort = nnPort;
      this.nnDnPort = nnDnPort;
      this.httpPort = httpPort;
      this.startupOption = startupOption;
    }
  }

  private static final Log LOG = LogFactory.getLog(MiniAvatarCluster.class);

  private static final String DEFAULT_TEST_DIR = 
    "build/contrib/highavailability/test/data";
  private static final String TEST_DIR =
    new File(System.getProperty("test.build.data", DEFAULT_TEST_DIR)).
    getAbsolutePath();

  private static final String ZK_DATA_DIR = TEST_DIR + "/zk.data";
  private static final String ZK_CONF_FILE = TEST_DIR + "/zk.conf";

  private static final int zkClientPort = getFreePort();

  private final String avatarDir;
  private final String dataDir;
  private final String fsimagelocal0Dir;
  private final String fsimagelocal1Dir;
  private final String fsimage0Dir;
  private final String fsimage1Dir;
  private final String fseditslocal0Dir;
  private final String fseditslocal1Dir;
  private final String fsedits0Dir;
  private final String fsedits1Dir;

  private final int nnPort;
  private final int nn0Port;
  private final int nn1Port;
  private final int nnDnPort;
  private final int nnDn0Port;
  private final int nnDn1Port;
  private final int httpPort;
  private final int http0Port;
  private final int http1Port;

  private Configuration conf;
  private Configuration a0Conf;
  private Configuration a1Conf;
  private int numDataNodes;
  private boolean format;
  private String[] racks;
  private String[] hosts;

  private static ZooKeeperServer zooKeeper;
  private static NIOServerCnxn.Factory cnxnFactory;

  ArrayList<AvatarInfo> avatars = new ArrayList<AvatarInfo>(2);

  private ArrayList<DataNodeProperties> dataNodes = 
    new ArrayList<DataNodeProperties>();

  /**
   * Modify the config and start up the servers.  The rpc and info ports for
   * servers are guaranteed to use free ports.
   * <p>
   * NameNode and DataNode directory creation and configuration will be
   * managed by this class.
   *
   * @param conf the base configuration to use in starting the servers.  This
   *          will be modified as necessary.
   * @param numDataNodes Number of DataNodes to start; may be zero
   * @param format if true, format the NameNode and DataNodes before starting up
   * @param racks array of strings indicating the rack that each DataNode is on
   * @param hosts array of strings indicating the hostname of each DataNode
   */
  public MiniAvatarCluster(Configuration conf,
                           int numDataNodes,
                           boolean format,
                           String[] racks,
                           String[] hosts) 
    throws IOException, ConfigException, InterruptedException {

    final String testDir = TEST_DIR + "/" + System.currentTimeMillis();
    avatarDir = testDir + "/avatar";
    dataDir = testDir + "/data";
    fsimagelocal0Dir = avatarDir + "/fsimagelocal0";
    fsimagelocal1Dir = avatarDir + "/fsimagelocal1";
    fsimage0Dir = avatarDir + "/fsimage0";
    fsimage1Dir = avatarDir + "/fsimage1";
    fseditslocal0Dir = avatarDir + "/fseditslocal0";
    fseditslocal1Dir = avatarDir + "/fseditslocal1";
    fsedits0Dir = avatarDir + "/fsedits0";
    fsedits1Dir = avatarDir + "/fsedits1";

    nnPort = getFreePort();
    nnDnPort = getFreePort();
    httpPort = getFreePort();
    nn0Port = getFreePorts(2);
    nnDn0Port = getFreePort();
    http0Port = getFreePort();
    nn1Port = getFreePorts(2);
    nnDn1Port = getFreePort();
    http1Port = getFreePort();

    this.conf = conf;
    this.numDataNodes = numDataNodes;
    this.format = format;
    this.racks = racks;
    this.hosts = hosts;

    configureAvatar();
    createAvatarDirs();
  
    startAvatarNodes();
    waitAvatarNodesActive();

    startDataNodes();
    waitDataNodesActive();

    waitExitSafeMode();
  }

  private static ServerConfig createZooKeeperConf() 
    throws IOException, ConfigException {
    
    // create conf file
    File zkConfDir = new File(TEST_DIR);
    zkConfDir.mkdirs();
    File zkConfFile = new File(ZK_CONF_FILE);
    zkConfFile.delete();
    zkConfFile.createNewFile();

    Properties zkConfProps = new Properties();
    zkConfProps.setProperty("tickTime", "2000");
    zkConfProps.setProperty("dataDir", ZK_DATA_DIR);
    zkConfProps.setProperty("clientPort", new Integer(zkClientPort).toString());
    zkConfProps.store(new FileOutputStream(zkConfFile), "");

    // create config object
    ServerConfig zkConf = new ServerConfig();
    zkConf.parse(ZK_CONF_FILE);

    return zkConf;
  }
    
  public static void createAndStartZooKeeper() 
    throws IOException, ConfigException, InterruptedException {
    ServerConfig zkConf = createZooKeeperConf();

    zooKeeper = new ZooKeeperServer();
    FileTxnSnapLog ftxn = new 
      FileTxnSnapLog(new File(zkConf.getDataLogDir()),
                     new File(zkConf.getDataDir()));
    zooKeeper.setTxnLogFactory(ftxn);
    zooKeeper.setTickTime(zkConf.getTickTime());
    zooKeeper.setMinSessionTimeout(zkConf.getMinSessionTimeout());
    zooKeeper.setMaxSessionTimeout(zkConf.getMaxSessionTimeout());

    cnxnFactory =
      new NIOServerCnxn.Factory(zkConf.getClientPortAddress(),
                                zkConf.getMaxClientCnxns());
    cnxnFactory.startup(zooKeeper);

  }

  private void registerZooKeeperNode(int nnPrimaryPort, int nnDnPrimaryPort,
                                     int httpPrimaryPort) throws IOException {
    AvatarZooKeeperClient zkClient = new AvatarZooKeeperClient(conf, null);
    zkClient.registerPrimary("localhost:" + nnPort, 
                             "localhost:" + nnPrimaryPort);
    zkClient.registerPrimary("localhost:" + nnDnPort, 
                             "localhost:" + nnDnPrimaryPort);
    zkClient.registerPrimary("localhost:" + httpPort,
                             "localhost:" + httpPrimaryPort);
  }

  private void clearZooKeeperNode() throws IOException {
    AvatarZooKeeperClient zkClient = new AvatarZooKeeperClient(conf, null);
    zkClient.clearPrimary("localhost:" + httpPort);
    zkClient.clearPrimary("localhost:" + nnPort);
    zkClient.clearPrimary("localhost:" + nnDnPort);
  }

  private void createAvatarDirs() {
    new File(fsimagelocal0Dir).mkdirs();
    new File(fsimagelocal1Dir).mkdirs();
    new File(fsimage0Dir).mkdirs();
    new File(fsimage1Dir).mkdirs();
    new File(fseditslocal0Dir).mkdirs();
    new File(fseditslocal1Dir).mkdirs();
    new File(fsedits0Dir).mkdirs();
    new File(fsedits1Dir).mkdirs();
  }

  private void configureAvatar() throws IOException {
    // overwrite relevant settings

    // avatar nodes
    conf.setInt("dfs.secondary.info.port", 0);
    conf.set("fs.default.name", "hdfs://localhost:" + nnPort);
    conf.set("fs.default.name0", "hdfs://localhost:" + nn0Port);
    conf.set("fs.default.name1", "hdfs://localhost:" + nn1Port);
    conf.set("dfs.namenode.dn-address", "localhost:" + nnDnPort);
    conf.set("dfs.namenode.dn-address0", "localhost:" + nnDn0Port);
    conf.set("dfs.namenode.dn-address1", "localhost:" + nnDn1Port);
    conf.set("dfs.http.address", "localhost:" + httpPort);
    conf.set("dfs.http.address0", "localhost:" + http0Port);
    conf.set("dfs.http.address1", "localhost:" + http1Port);
    conf.set("fs.ha.zookeeper.prefix", "/hdfs");
    conf.set("fs.ha.zookeeper.quorum", "localhost:" + zkClientPort);
    conf.set("dfs.name.dir.shared0", fsimage0Dir);
    conf.set("dfs.name.dir.shared1", fsimage1Dir);
    conf.set("dfs.name.edits.dir.shared0", fsedits0Dir);
    conf.set("dfs.name.edits.dir.shared1", fsedits1Dir);

    LOG.info("conf fs.default.name0=hdfs://localhost:" + nn0Port);
    LOG.info("conf fs.default.name1=hdfs://localhost:" + nn1Port);
    LOG.info("conf dfs.namenode.dn-address0=localhost:" + nnDn0Port);
    LOG.info("conf dfs.namenode.dn-address1=localhost:" + nnDn1Port);

    // datanodes
    conf.set("dfs.datanode.address", "localhost:0");
    conf.set("dfs.datanode.http.address", "localhost:0");
    conf.set("dfs.datanode.ipc.address", "localhost:0");
    conf.set("dfs.datanode.dns.interface", "lo");
    conf.set("dfs.namenode.dns.interface", "lo");

    // other settings
    conf.setBoolean("dfs.permissions", false);
    conf.setBoolean("dfs.persist.blocks", true);
    conf.set("fs.hdfs.impl",
             "org.apache.hadoop.hdfs.DistributedAvatarFileSystem");
    conf.setLong("dfs.blockreport.initialDelay", 0);
    conf.setClass("topology.node.switch.mapping.impl", 
                  StaticMapping.class, DNSToSwitchMapping.class);

    // never automatically exit from safe mode
    conf.setFloat("dfs.safemode.threshold.pct", 1.5f);

    // server config for avatar nodes
    a0Conf = new Configuration(conf);
    a1Conf = new Configuration(conf);

    a0Conf.set("fs.hdfs.impl",
               "org.apache.hadoop.hdfs.DistributedFileSystem");
    a0Conf.set("dfs.name.dir", fsimagelocal0Dir);
    a0Conf.set("dfs.name.edits.dir", fseditslocal0Dir);
    a0Conf.set("fs.checkpoint.dir", avatarDir + "/checkpoint0");

    a1Conf.set("fs.hdfs.impl",
               "org.apache.hadoop.hdfs.DistributedFileSystem");
    a1Conf.set("dfs.name.dir", fsimagelocal1Dir);
    a1Conf.set("dfs.name.edits.dir", fseditslocal1Dir);
    a1Conf.set("fs.checkpoint.dir", avatarDir + "/checkpoint1");


  }

  private Configuration getServerConf(String startupOption) {
    // namenode should use DFS, not DAFS

    if (startupOption.
               equals(AvatarConstants.StartupOption.NODEZERO.getName())) {
      return new Configuration(a0Conf);
    } else if (startupOption.
               equals(AvatarConstants.StartupOption.NODEONE.getName())) {
      return new Configuration(a1Conf);
    } else {
      throw new IllegalArgumentException("invalid avatar");
    }
  }

  private void startAvatarNodes() throws IOException {
    registerZooKeeperNode(nn0Port, nnDn0Port, http0Port);

    if (format) {
      LOG.info("formatting");
      String[] a0FormatArgs = { AvatarConstants.StartupOption.
                                NODEZERO.getName(),
                                AvatarConstants.StartupOption.
                                FORMATFORCE.getName() };
      AvatarNode.createAvatarNode(a0FormatArgs, 
                                  getServerConf(AvatarConstants.StartupOption.
                                                NODEZERO.getName()));
    }

    {
      LOG.info("starting avatar 0");
      String[] a0Args = { AvatarConstants.StartupOption.NODEZERO.getName() };
      AvatarNode a0 = AvatarNode.
        createAvatarNode(a0Args, 
                         getServerConf(AvatarConstants.
                                       StartupOption.
                                       NODEZERO.
                                       getName()));
      // leave safe mode manually
      a0.setSafeMode(FSConstants.SafeModeAction.SAFEMODE_LEAVE);

      avatars.add(new AvatarInfo(a0,
                                 AvatarState.ACTIVE,
                                 nn0Port, nnDn0Port, http0Port,
                                 AvatarConstants.StartupOption.NODEZERO.
                                 getName()));

    }

    {
      LOG.info("starting avatar 1");
      String[] a1Args = { AvatarConstants.StartupOption.NODEONE.getName(),
                          AvatarConstants.StartupOption.STANDBY.getName(),
                          AvatarConstants.StartupOption.SYNC.getName() };
      avatars.add(new AvatarInfo(AvatarNode.
                                 createAvatarNode(a1Args, 
                                                  getServerConf(AvatarConstants.
                                                                StartupOption.
                                                                NODEONE.
                                                                getName())),
                                 AvatarState.STANDBY,
                                 nn1Port, nnDn1Port, http1Port,
                                 AvatarConstants.StartupOption.NODEONE.
                                 getName()));
    }

    for (AvatarInfo avatar: avatars) {
      if (avatar.avatar == null) {
        throw new IOException("Cannot create avatar nodes");
      }
    }
    
  }

  private void shutDownDataNodes() throws IOException, InterruptedException {
    int i = 0;
    for (DataNodeProperties dn : dataNodes) {
      i++;
      LOG.info("shutting down data node " + i);
      dn.datanode.shutdown();
      LOG.info("data node " + i + " shut down");
    }
  }

  private void shutDownAvatarNodes() throws IOException, InterruptedException {
    for (AvatarInfo avatar: avatars) {
      if (avatar.state == AvatarState.ACTIVE || 
          avatar.state == AvatarState.STANDBY) {
        LOG.info("shutdownAvatar");
        avatar.avatar.shutdownAvatar();
        avatar.avatar.stopRPC();
      }
    }

    try {
      Thread.sleep(1000);
    } catch (InterruptedException ignore) {
      // do nothing
    }
  }

  public static void shutDownZooKeeper() throws IOException, InterruptedException {
    cnxnFactory.shutdown();
    cnxnFactory.join();
    if (zooKeeper.isRunning()) {
      zooKeeper.shutdown();
    }
  }
  
  /**
   * Shut down the cluster
   */
  public void shutDown() throws IOException, InterruptedException {
    System.out.println("Shutting down the Mini Avatar Cluster");
    
    shutDownAvatarNodes(); 

    // this doesn't work, so just leave the datanodes running,
    // they won't interfere with the next run
    // shutDownDataNodes();
  }

  private void startDataNodes() throws IOException {
    if (racks != null && numDataNodes > racks.length ) {
      throw new IllegalArgumentException( "The length of racks [" + 
                                          racks.length +
                                          "] is less than the number " +
                                          "of datanodes [" +
                                          numDataNodes + "].");
    }
    if (hosts != null && numDataNodes > hosts.length ) {
      throw new IllegalArgumentException( "The length of hosts [" + 
                                          hosts.length +
                                          "] is less than the number " +
                                          "of datanodes [" +
                                          numDataNodes + "].");
    }

    //Generate some hostnames if required
    if (racks != null && hosts == null) {
      LOG.info("Generating host names for datanodes");
      hosts = new String[numDataNodes];
      for (int i = 0; i < numDataNodes; i++) {
        hosts[i] = "host" + i + ".foo.com";
      }
    }
    
    
    String[] dnArgs = { HdfsConstants.StartupOption.REGULAR.getName() };
    
    for (int i = 0; i < numDataNodes; i++) {
      Configuration dnConf = new Configuration(conf);

      File dir1 = new File(dataDir, "data"+(2*i+1));
      File dir2 = new File(dataDir, "data"+(2*i+2));
      dir1.mkdirs();
      dir2.mkdirs();
      if (!dir1.isDirectory() || !dir2.isDirectory()) { 
        throw new IOException("Mkdirs failed to create directory for DataNode "
                              + i + ": " + dir1 + " or " + dir2);
      }
      dnConf.set("dfs.data.dir", dir1.getPath() + "," + dir2.getPath()); 

      LOG.info("Starting DataNode " + i + " with dfs.data.dir: " 
                         + dnConf.get("dfs.data.dir"));
      
      if (hosts != null) {
        dnConf.set("slave.host.name", hosts[i]);
        LOG.info("Starting DataNode " + i + " with hostname set to: " 
                           + dnConf.get("slave.host.name"));
      }

      if (racks != null) {
        String name = hosts[i];
        LOG.info("Adding node with hostname : " + name + " to rack "+
                           racks[i]);
        StaticMapping.addNodeToRack(name,
                                    racks[i]);
      }
      Configuration newconf = new Configuration(dnConf); // save config
      if (hosts != null) {
        NetUtils.addStaticResolution(hosts[i], "localhost");
      }
      AvatarDataNode dn = AvatarDataNode.instantiateDataNode(dnArgs, dnConf);
      //since the HDFS does things based on IP:port, we need to add the mapping
      //for IP:port to rackId
      
      String ipAddr = dn.getSelfAddr().getAddress().getHostAddress();
      if (racks != null) {
        int port = dn.getSelfAddr().getPort();
        System.out.println("Adding node with IP:port : " + ipAddr + ":" + port+
                            " to rack " + racks[i]);
        StaticMapping.addNodeToRack(ipAddr + ":" + port,
                                  racks[i]);
      }
      AvatarDataNode.runDatanodeDaemon(dn);
      dataNodes.add(new DataNodeProperties(dn, newconf, dnArgs));

    }

  }

  private void waitAvatarNodesActive() {
    for (AvatarInfo avatar: avatars) {
      while (avatar.avatar.getNameNodeDNAddress() == null) {
        try {
          LOG.info("waiting for avatar");
          Thread.sleep(200);
        } catch (InterruptedException ignore) {
          // do nothing
        }
      }
    }
  }

  private void waitDataNodesActive() throws IOException {
    InetSocketAddress addr = new InetSocketAddress("localhost", nn0Port);
    DFSClient client = new DFSClient(addr, conf);

    // make sure all datanodes are alive
    while(client.datanodeReport(DatanodeReportType.LIVE).length
        != numDataNodes) {
      try {
        LOG.info("waiting for data nodes");
        Thread.sleep(200);
      } catch (InterruptedException ignore) {
        // do nothing
      }
    }
    client.close();
  }

  private AvatarInfo getPrimaryAvatar() {
    return getAvatarByState(AvatarState.ACTIVE);
  }

  private AvatarInfo getStandbyAvatar() {
    return getAvatarByState(AvatarState.STANDBY);
  }

  private AvatarInfo getDeadAvatar() {
    return getAvatarByState(AvatarState.DEAD);
  }

  private AvatarInfo getAvatarByState(AvatarState state) {
    for (AvatarInfo avatar: avatars) {
      if (avatar.state == state) {
        return avatar;
      }
    }
    return null;
  }

  /**
   * Return true if primary avatar has left safe mode
   */
  private boolean hasLeftSafeMode() throws IOException {
    AvatarInfo primary = getPrimaryAvatar();

    return (primary != null && !primary.avatar.isInSafeMode() && 
            primary.avatar.getStats()[0] != 0);
  }

  private void waitExitSafeMode() throws IOException {
    // make sure all datanodes are alive
    while(!hasLeftSafeMode()) {
      try {
        LOG.info("waiting until avatar0 has left safe mode");
        Thread.sleep(50);
      } catch (InterruptedException ignore) {
        // do nothing
      }
    }
  }

  /**
   * Get DAFS.
   */
  public DistributedAvatarFileSystem getFileSystem() throws IOException {
    FileSystem fs = FileSystem.get(conf);

    if (!(fs instanceof DistributedAvatarFileSystem)) {
      throw new IOException("fs is not avatar fs");
    }

    return (DistributedAvatarFileSystem) fs;
  }

  /**
   * Kill the primary avatar node.
   * @param updateZK clear zookeeper?
   */
  public void killPrimary() throws IOException {
    killPrimary(true);
  }

  /**
   * Kill the primary avatar node.
   * @param clearZK clear zookeeper?
   */
  public void killPrimary(boolean clearZK) throws IOException {
    AvatarInfo primary = getPrimaryAvatar();
    if (primary != null) {
      if (clearZK) {
        clearZooKeeperNode();
      }

      primary.avatar.shutdownAvatar();
      primary.avatar.stopRPC();

      primary.avatar = null;
      primary.state = AvatarState.DEAD;

      try {
        Thread.sleep(1000);
      } catch (InterruptedException ignore) {
        // do nothing
      }
      
    } else {
      throw new IOException("can't kill primary avatar, already dead");
    }
  }

  /**
   * Kill the standby avatar node.
   */
  public void killStandby() throws IOException {
    AvatarInfo standby = getStandbyAvatar();
    if (standby != null) {
      standby.avatar.shutdownAvatar();
      standby.avatar.stopRPC();

      standby.avatar = null;
      standby.state = AvatarState.DEAD;

      try {
        Thread.sleep(1000);
      } catch (InterruptedException ignore) {
        // do nothing
      }
      
    } else {
      LOG.info("can't kill standby avatar, already dead");
    }
  }


  /**
   * Make standby avatar the new primary avatar. Kill the old
   * primary avatar first if necessary.
   */
  public void failOver() throws IOException {
    if (getPrimaryAvatar() != null) {
      LOG.info("killing primary avatar before failover");
      killPrimary();
    }

    AvatarInfo standby = getStandbyAvatar();
    if (standby == null) {
      throw new IOException("no standby avatar running");
    }

    standby.avatar.setAvatar(AvatarConstants.Avatar.ACTIVE);
    standby.state = AvatarState.ACTIVE;
    registerZooKeeperNode(standby.nnPort, standby.nnDnPort, standby.httpPort);
  }

  /**
   * Restart a dead avatar node as a standby avatar.
   */
  public void restartStandby() throws IOException {
    AvatarInfo dead = getDeadAvatar();
    if (getPrimaryAvatar() == null || dead == null) {
      throw new IOException("cannot start standby avatar: " +
                            "primary or dead avatar not found");
      
    }
    LOG.info("restarting " + dead.startupOption + " as standby");

    String[] args = { dead.startupOption,
                      AvatarConstants.StartupOption.STANDBY.getName(),
                      AvatarConstants.StartupOption.SYNC.getName() };
    dead.avatar = AvatarNode.createAvatarNode(args, getServerConf(dead.startupOption));
    dead.state = AvatarState.STANDBY;

    if (dead.avatar == null) {
      throw new IOException("cannot start avatar node");
    }
  }
}
