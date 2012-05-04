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
import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.Properties;
import java.util.ArrayList;
import java.util.Set;
import java.net.InetSocketAddress;
import java.net.ServerSocket;

import org.apache.zookeeper.server.ZooKeeperServer;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig.ConfigException;
import org.apache.zookeeper.server.persistence.FileTxnSnapLog;
import org.apache.zookeeper.server.NIOServerCnxn;
import org.apache.zookeeper.server.ServerConfig;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.MiniDFSCluster.DataNodeProperties;
import org.apache.hadoop.hdfs.MiniDFSCluster.NameNodeInfo;
import org.apache.hadoop.hdfs.protocol.FSConstants.DatanodeReportType;
import org.apache.hadoop.hdfs.protocol.AvatarConstants;
import org.apache.hadoop.hdfs.protocol.FSConstants;
import org.apache.hadoop.hdfs.server.common.HdfsConstants;
import org.apache.hadoop.hdfs.server.common.HdfsConstants.StartupOption;
import org.apache.hadoop.hdfs.server.namenode.AvatarNode;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.server.datanode.AvatarDataNode;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.tools.DFSAdmin;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.net.StaticMapping;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.net.DNSToSwitchMapping;

/**
 * This class manages a Avatar/HDFS cluster with all nodes running
 * locally.
 * To synchronize the AvatarNodes, it uses a local ZooKeeper
 * server.
 */
public class MiniAvatarCluster {

  // start handing out ports from this number
  private static final int PORT_START = 10000;
  public static final String NAMESERVICE_ID_PREFIX = "nameserviceId";
  public static int currNSId = 0;

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

  public static class DataNodeProperties {
    public AvatarDataNode datanode;
    public Configuration conf;
    public String[] dnArgs;

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
  
  public static class AvatarInfo {
    public AvatarNode avatar;
    AvatarState state;
    int nnPort;
    int nnDnPort;
    int httpPort;
    int rpcPort;
    String startupOption;
    
    AvatarInfo(AvatarNode avatar, AvatarState state,
               int nnPort, int nnDnPort, int httpPort,
               int rpcPort, String startupOption) {
      this.avatar = avatar;
      this.state = state;
      this.nnPort = nnPort;
      this.nnDnPort = nnDnPort;
      this.httpPort = httpPort;
      this.rpcPort = rpcPort;
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
  private static String baseAvatarDir;
  private static String dataDir;
  private int numDataNodes;
  private boolean format;
  private String[] racks;
  private String[] hosts;
  private boolean federation;
  private NameNodeInfo[] nameNodes;
  private Configuration conf;
  
  public class NameNodeInfo {
    Configuration conf;
    public ArrayList<AvatarInfo> avatars = null;
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
    private final int rpcPort;
    private final int rpc0Port;
    private final int rpc1Port;

    private Configuration clientConf;
    private Configuration a0Conf;
    private Configuration a1Conf;
    private final String avatarDir;
    private String nameserviceId;
    
    NameNodeInfo(int nnIndex) {
      avatarDir = baseAvatarDir;
      fsimagelocal0Dir = avatarDir + "/fsimagelocal0";
      fsimagelocal1Dir = avatarDir + "/fsimagelocal1";
      fsimage0Dir = avatarDir + "/fsimage0";
      fsimage1Dir = avatarDir + "/fsimage1";
      fseditslocal0Dir = avatarDir + "/fseditslocal0";
      fseditslocal1Dir = avatarDir + "/fseditslocal1";
      fsedits0Dir = avatarDir + "/fsedits0";
      fsedits1Dir = avatarDir + "/fsedits1";

      rpcPort = nnPort = getFreePort();
      nnDnPort = getFreePort();
      httpPort = getFreePort();
      rpc0Port = nn0Port = getFreePorts(2);
      nnDn0Port = getFreePort();
      http0Port = getFreePort();
      rpc1Port = nn1Port = getFreePorts(2);
      nnDn1Port = getFreePort();
      http1Port = getFreePort();
    }
    
    public void setAvatarNodes(ArrayList<AvatarInfo> avatars) {
      this.avatars = avatars;
    }
    
    public void initClientConf(Configuration conf) {
      clientConf = new Configuration(conf);
      clientConf.set("fs.default.name", "hdfs://localhost:" + nnPort);
      clientConf.set("fs.default.name0", "hdfs://localhost:" + nn0Port);
      clientConf.set("fs.default.name1", "hdfs://localhost:" + nn1Port);
      clientConf.set("dfs.namenode.dn-address", "localhost:" + nnDnPort);
      clientConf.set("dfs.namenode.dn-address0", "localhost:" + nnDn0Port);
      clientConf.set("dfs.namenode.dn-address1", "localhost:" + nnDn1Port);
      clientConf.set("fs.hdfs.impl",
          "org.apache.hadoop.hdfs.DistributedAvatarFileSystem");
      clientConf.setBoolean("fs.hdfs.impl.disable.cache", true);
    }
    
    public void initGeneralConf(Configuration conf, String nameserviceId) {
      // overwrite relevant settings
      initClientConf(conf);
      this.nameserviceId = nameserviceId;
      // avatar nodes
      if (federation) {
        conf.set("dfs.namenode.rpc-address0", "localhost:" + rpc0Port);
        conf.set("dfs.namenode.rpc-address1", "localhost:" + rpc1Port);
      } else {
        conf.set("fs.default.name", "hdfs://localhost:" + nnPort);
        conf.set("fs.default.name0", "hdfs://localhost:" + nn0Port);
        conf.set("fs.default.name1", "hdfs://localhost:" + nn1Port);
        conf.set("dfs.namenode.dn-address", "localhost:" + nnDnPort);
        conf.set("dfs.http.address", "localhost:" + httpPort);
      }
      conf.set("dfs.namenode.dn-address0", "localhost:" + nnDn0Port);
      conf.set("dfs.namenode.dn-address1", "localhost:" + nnDn1Port);
      conf.set("dfs.http.address0", "localhost:" + http0Port);
      conf.set("dfs.http.address1", "localhost:" + http1Port);
      
      conf.set("dfs.name.dir.shared0", fsimage0Dir);
      conf.set("dfs.name.dir.shared1", fsimage1Dir);
      conf.set("dfs.name.edits.dir.shared0", fsedits0Dir);
      conf.set("dfs.name.edits.dir.shared1", fsedits1Dir);
      // We need to disable the filesystem cache so that unit tests and
      // MiniAvatarCluster don't end up sharing FileSystem objects.
      if (federation) {
        for (String key: AvatarNode.AVATARSERVICE_SPECIFIC_KEYS) {
          String value = conf.get(key);
          if (value != null) {
            String newKey = DFSUtil.getNameServiceIdKey(key, nameserviceId);
            conf.set(newKey, value);
            conf.set(key, "");
          }
        }
        String rpcKey = DFSUtil.getNameServiceIdKey(
            AvatarNode.DFS_NAMENODE_RPC_ADDRESS_KEY, nameserviceId);
        conf.set(rpcKey, "localhost:" + rpcPort);
        String dnKey = DFSUtil.getNameServiceIdKey(
            NameNode.DATANODE_PROTOCOL_ADDRESS, nameserviceId);
        conf.set(dnKey, "localhost:" + nnDnPort);
        String httpKey = DFSUtil.getNameServiceIdKey(
            NameNode.DFS_NAMENODE_HTTP_ADDRESS_KEY, nameserviceId);
        conf.set(httpKey, "localhost:" + httpPort);
      }
    }
    
    public void updateAvatarConf(Configuration newConf) {
      conf = new Configuration(newConf);
      if (federation) {
        conf.set(FSConstants.DFS_FEDERATION_NAMESERVICE_ID, nameserviceId);
      }
      
      // server config for avatar nodes
      a0Conf = new Configuration(conf);
      a1Conf = new Configuration(conf);

      a0Conf.set("dfs.name.dir", fsimagelocal0Dir);
      a0Conf.set("dfs.name.edits.dir", fseditslocal0Dir);
      a0Conf.set("fs.checkpoint.dir", avatarDir + "/checkpoint0");

      a1Conf.set("dfs.name.dir", fsimagelocal1Dir);
      a1Conf.set("dfs.name.edits.dir", fseditslocal1Dir);
      a1Conf.set("fs.checkpoint.dir", avatarDir + "/checkpoint1");
    }
    
    public void createAvatarDirs() {
      new File(fsimagelocal0Dir).mkdirs();
      new File(fsimagelocal1Dir).mkdirs();
      new File(fsimage0Dir).mkdirs();
      new File(fsimage1Dir).mkdirs();
      new File(fseditslocal0Dir).mkdirs();
      new File(fseditslocal1Dir).mkdirs();
      new File(fsedits0Dir).mkdirs();
      new File(fsedits1Dir).mkdirs();
    }
    
    public void cleanupAvatarDirs() throws IOException {
      String[] files = new String[] {fsimagelocal0Dir, fsimagelocal1Dir,
          fsimage0Dir, fsimage1Dir, fseditslocal0Dir, fseditslocal1Dir,
          fsedits0Dir, fsedits1Dir
      };
      for (String filename : files) {
        FileUtil.fullyDelete(new File(filename));
      }
    }
  }

  private static ZooKeeperServer zooKeeper;
  private static NIOServerCnxn.Factory cnxnFactory;

  private ArrayList<DataNodeProperties> dataNodes = 
    new ArrayList<DataNodeProperties>();

  public MiniAvatarCluster(Configuration conf,
                           int numDataNodes,
                           boolean format,
                           String[] racks,
                           String[] hosts) 
    throws IOException, ConfigException, InterruptedException {
    this(conf, numDataNodes, format, racks, hosts, 1, false);
  }
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
   * @param numNameNodes Number of NameNodes to start; 
   * @param federation if true, we start it with federation configure;
   */
  public MiniAvatarCluster(Configuration conf,
                           int numDataNodes,
                           boolean format,
                           String[] racks,
                           String[] hosts,
                           int numNameNodes,
                           boolean federation) 
    throws IOException, ConfigException, InterruptedException {

    final String testDir = TEST_DIR + "/" + conf.get(MiniDFSCluster.DFS_CLUSTER_ID, "");
    baseAvatarDir = testDir + "/avatar";
    dataDir = testDir + "/data";
    this.conf = conf;
    this.numDataNodes = numDataNodes;
    this.format = format;
    this.racks = racks;
    this.hosts = hosts;
    
    conf.setInt("dfs.secondary.info.port", 0);
    conf.set("fs.ha.zookeeper.prefix", "/hdfs");
    conf.set("fs.ha.zookeeper.quorum", "localhost:" + zkClientPort);
    
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

    this.federation = federation;
    Collection<String> nameserviceIds = DFSUtil.getNameServiceIds(conf);
    if(nameserviceIds.size() > 1)  
      this.federation = true;
    if (!federation && numNameNodes != 1) {
      throw new IOException("Only 1 namenode is allowed in non-federation cluster.");
    }
    nameNodes = new NameNodeInfo[numNameNodes];
    for (int nnIndex = 0; nnIndex < numNameNodes; nnIndex++) {
      nameNodes[nnIndex] = new NameNodeInfo(nnIndex);
      if (format)
        nameNodes[nnIndex].cleanupAvatarDirs();
      nameNodes[nnIndex].createAvatarDirs();
    }
    if (!federation) {
      nameNodes[0].initGeneralConf(conf, null);
    } else {
      if (nameserviceIds.isEmpty()) {
        for (int i = 0; i < nameNodes.length; i++) {
          nameserviceIds.add(NAMESERVICE_ID_PREFIX + getNSId());
        }
      }
      initFederationConf(conf, nameserviceIds);
    }

    if (this.format) {
      File data_dir = new File(dataDir);
      if (data_dir.exists() && !FileUtil.fullyDelete(data_dir)) {
        throw new IOException("Cannot remove data directory: " + data_dir);
      }
    }
  
    // Need to start datanodes before avatarnodes, since the primary starts up
    // in safemode and when the standby starts up, it waits for the primary to
    // exit safemode. So if we start avatarnodes first with non-empty FSImage
    // and FSEdits, the primary avatar would wait for datanode block reports and
    // the standby would wait for the primary to exit safemode and since we
    // wouldn't return from the standby initialization we would never start the
    // datanodes and hence we enter a deadlock.
    startDataNodes();
    startAvatarNodes();
    waitAvatarNodesActive();

    waitDataNodesActive();

    waitExitSafeMode();
  }
  
  private void initFederationConf(Configuration conf,
      Collection<String> nameserviceIds) {
    String nameserviceIdList = "";
    int nnIndex = 0;
    for (String nameserviceId : nameserviceIds) {
      // Create comma separated list of nameserviceIds
      if (nameserviceIdList.length() > 0) {
        nameserviceIdList += ",";
      }
      nameserviceIdList += nameserviceId;
      nameNodes[nnIndex].initGeneralConf(conf, nameserviceId);
      nnIndex++;
    }
    conf.set(FSConstants.DFS_FEDERATION_NAMESERVICES, nameserviceIdList);
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
    zkConfProps.setProperty("maxClientCnxns", "30");
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
      int httpPrimaryPort, int rpcPrimaryPort, NameNodeInfo nni) throws IOException {
    AvatarZooKeeperClient zkClient = new AvatarZooKeeperClient(nni.conf, null);
    zkClient.registerPrimary("localhost:" + nni.nnPort, 
    "localhost:" + nnPrimaryPort);
    zkClient.registerPrimary("localhost:" + nni.nnDnPort, 
    "localhost:" + nnDnPrimaryPort);
    zkClient.registerPrimary("localhost:" + nni.httpPort,
    "localhost:" + httpPrimaryPort);
    zkClient.registerPrimary("localhost:" + nni.rpcPort,
    "localhost:" + rpcPrimaryPort);
    try {
      zkClient.shutdown();
    } catch (InterruptedException ie) {
      throw new IOException("zkClient.shutdown() interrupted");
    }
    LOG.info("Closed zk client connection for registerZookeeper");
  }

  private void clearZooKeeperNode(int nnIndex) throws IOException {
    NameNodeInfo nni = this.nameNodes[nnIndex];
    AvatarZooKeeperClient zkClient = new AvatarZooKeeperClient(nni.conf, null);
    zkClient.clearPrimary("localhost:" + nni.httpPort);
    zkClient.clearPrimary("localhost:" + nni.nnPort);
    zkClient.clearPrimary("localhost:" + nni.nnDnPort);
    zkClient.clearPrimary("localhost:" + nni.rpcPort);
    try {
      zkClient.shutdown();
    } catch (InterruptedException ie) {
      throw new IOException("zkClient.shutdown() interrupted");
    }
    LOG.info("Closed zk client connection for clearZKNode");
  }

  private Configuration getServerConf(String startupOption, NameNodeInfo nni) {
    // namenode should use DFS, not DAFS

    if (startupOption.
               equals(AvatarConstants.StartupOption.NODEZERO.getName())) {
      return new Configuration(nni.a0Conf);
    } else if (startupOption.
               equals(AvatarConstants.StartupOption.NODEONE.getName())) {
      return new Configuration(nni.a1Conf);
    } else {
      throw new IllegalArgumentException("invalid avatar");
    }
  }
  
  private void startAvatarNodes() throws IOException {
    for (NameNodeInfo nni: this.nameNodes) {
      nni.updateAvatarConf(this.conf);
      startAvatarNode(nni, null);
    }
  }

  private void startAvatarNode(NameNodeInfo nni, StartupOption operation) throws IOException {
    registerZooKeeperNode(nni.nn0Port, nni.nnDn0Port, nni.http0Port, nni.rpc0Port, nni);

    if (format) {
      LOG.info("formatting");
      // Start the NameNode
      String[] a0FormatArgs; 
      ArrayList<String> argList = new ArrayList<String>();
      argList.add(AvatarConstants.StartupOption.
          NODEZERO.getName());
      argList.add(AvatarConstants.StartupOption.
          FORMATFORCE.getName());
      if (federation) {
        argList.add(StartupOption.SERVICE.getName());
        argList.add(nni.nameserviceId);
      }
      a0FormatArgs = new String[argList.size()];
      argList.toArray(a0FormatArgs);
      AvatarNode.createAvatarNode(a0FormatArgs, 
                                  getServerConf(AvatarConstants.StartupOption.
                                                NODEZERO.getName(), nni));
    }
    ArrayList<AvatarInfo> avatars = new ArrayList<AvatarInfo>(2);
    {
      LOG.info("starting avatar 0");
      String[] a0Args; 
      ArrayList<String> argList = new ArrayList<String>();
      if (operation != null) {
        argList.add(operation.getName());
      }
      argList.add(AvatarConstants.StartupOption.NODEZERO.getName());
      if (federation) {
        argList.add(StartupOption.SERVICE.getName());
        argList.add(nni.nameserviceId);
      }
      a0Args = new String[argList.size()];
      argList.toArray(a0Args);
      AvatarNode a0 = AvatarNode.
        createAvatarNode(a0Args, 
                         getServerConf(AvatarConstants.
                                       StartupOption.
                                       NODEZERO.
                                       getName(), nni));

      avatars.add(new AvatarInfo(a0,
                                 AvatarState.ACTIVE,
                                 nni.nn0Port, nni.nnDn0Port, nni.http0Port, nni.rpc0Port,
                                 AvatarConstants.StartupOption.NODEZERO.
                                 getName()));

    }

    {
      LOG.info("starting avatar 1");
      String[] a1Args; 
      ArrayList<String> argList = new ArrayList<String>();
      argList.add(AvatarConstants.StartupOption.NODEONE.getName());
      argList.add(AvatarConstants.StartupOption.STANDBY.getName());
      argList.add(AvatarConstants.StartupOption.REGULAR.getName());
      if (federation) {
        argList.add(StartupOption.SERVICE.getName());
        argList.add(nni.nameserviceId);
      }
      a1Args = new String[argList.size()];
      argList.toArray(a1Args);
      avatars.add(new AvatarInfo(AvatarNode.
                                 createAvatarNode(a1Args, 
                                                  getServerConf(AvatarConstants.
                                                                StartupOption.
                                                                NODEONE.
                                                                getName(), nni)),
                                 AvatarState.STANDBY,
                                 nni.nn1Port, nni.nnDn1Port, nni.http1Port, nni.rpc1Port,
                                 AvatarConstants.StartupOption.NODEONE.
                                 getName()));
    }

    for (AvatarInfo avatar: avatars) {
      if (avatar.avatar == null) {
        throw new IOException("Cannot create avatar nodes");
      }
    }
    nni.setAvatarNodes(avatars);
    DFSUtil.setGenericConf(nni.conf, nni.nameserviceId, 
        AvatarNode.AVATARSERVICE_SPECIFIC_KEYS);
    nni.updateAvatarConf(nni.conf);
  }

  public void restartAvatarNodes() throws Exception {
    shutDownAvatarNodes();
    for (NameNodeInfo nni : this.nameNodes) {
      nni.avatars.clear();
    }
    this.format = false;
    Thread.sleep(10000);
    startAvatarNodes();
    waitAvatarNodesActive();

    waitDataNodesActive();

    waitExitSafeMode();
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

  public void shutDownAvatarNodes() throws IOException, InterruptedException {
    for (NameNodeInfo nni : this.nameNodes) {
      for (AvatarInfo avatar: nni.avatars) {
        if (avatar.state == AvatarState.ACTIVE || 
            avatar.state == AvatarState.STANDBY) {
          LOG.info("shutdownAvatar");
          avatar.avatar.shutdownAvatar();
        }
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
    LOG.info("Zookeeper Connection Factory shutdown");
    if (zooKeeper.isRunning()) {
      zooKeeper.shutdown();
    }
    LOG.info("Zookeepr Server shutdown");
  }
  
  /**
   * Shut down the cluster
   */
  public void shutDown() throws IOException, InterruptedException {
    System.out.println("Shutting down the Mini Avatar Cluster");
    // this doesn't work, so just leave the datanodes running,
    // they won't interfere with the next run
    shutDownDataNodes();
    
    shutDownAvatarNodes(); 

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
      dn.runDatanodeDaemon();
      dataNodes.add(new DataNodeProperties(dn, newconf, dnArgs));

    }

  }

  public void waitAvatarNodesActive() {
    for (int nnIndex = 0; nnIndex < this.nameNodes.length; nnIndex++) {
      waitAvatarNodesActive(nnIndex);
    }
  }

  public void waitAvatarNodesActive(int nnIndex) {
    NameNodeInfo nni = this.nameNodes[nnIndex];
    for (AvatarInfo avatar: nni.avatars) {
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

  /* wait Datanodes active for all namespaces */
  public void waitDataNodesActive() throws IOException {
    for (int nnIndex = 0; nnIndex < this.nameNodes.length; nnIndex++) {
      waitDataNodesActive(nnIndex);
    }
  }
  
  /* wait Datanodes active for specific namespaces */
  public void waitDataNodesActive(int nnIndex) throws IOException {
    NameNodeInfo nni = this.nameNodes[nnIndex];
    InetSocketAddress addr = new InetSocketAddress("localhost", nni.rpc0Port);
    DistributedAvatarFileSystem dafs = getFileSystem(nnIndex);

    int liveDataNodes = 0;
    // make sure all datanodes are alive
    while(liveDataNodes != numDataNodes) {
      try {
        LOG.info("waiting for data nodes... ");
        Thread.sleep(200);
        LOG.info("waiting for data nodes : live=" + liveDataNodes + ", total=" + numDataNodes);
        liveDataNodes = dafs.getLiveDataNodeStats(false).length;
      } catch (Exception e) {
        LOG.warn("Exception waiting for datanodes : ", e);
      }
    }
    dafs.close();
  }
  
  private void checkSingleNameNode() {
    if (nameNodes.length != 1) {
      throw new IllegalArgumentException("It's not a single namenode cluster, use index instead.");
    }
  }

  public AvatarInfo getPrimaryAvatar(int nnIndex) {
    return getAvatarByState(nnIndex, AvatarState.ACTIVE);
  }
  
  private AvatarInfo getStandbyAvatar(int nnIndex) {
    return getAvatarByState(nnIndex, AvatarState.STANDBY);
  }
  
  private AvatarInfo getDeadAvatar(int nnIndex) {
    return getAvatarByState(nnIndex, AvatarState.DEAD);
  }

  private AvatarInfo getAvatarByState(int nnIndex, AvatarState state) {
    for (AvatarInfo avatar: this.nameNodes[nnIndex].avatars) {
      if (avatar.state == state) {
        return avatar;
      }
    }
    return null;
  }

  /**
   * Return true if primary avatar has left safe mode
   */
  private boolean hasLeftSafeMode(int nnIndex) throws IOException {
    AvatarInfo primary = getPrimaryAvatar(nnIndex);

    return (primary != null && !primary.avatar.isInSafeMode() && 
            primary.avatar.getStats()[0] != 0);
  }

  private void waitExitSafeMode() throws IOException {
    for (int nnIndex=0; nnIndex < this.nameNodes.length; nnIndex++) {
      // make sure all datanodes are alive
      while(!hasLeftSafeMode(nnIndex)) {
        try {
          LOG.info("waiting until avatar0 has left safe mode");
          Thread.sleep(50);
        } catch (InterruptedException ignore) {
          // do nothing
        }
      }
    }
  }
  
  public DistributedAvatarFileSystem getFileSystem() throws IOException {
    checkSingleNameNode();
    return getFileSystem(0);
  }

  /**
   * Get DAFS.
   */
  public DistributedAvatarFileSystem getFileSystem(int nnIndex) throws IOException {
    FileSystem fs = FileSystem.get(this.nameNodes[nnIndex].clientConf);

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
    checkSingleNameNode();
    killPrimary(0, true);
  }
  
  public void killPrimary(int nnIndex) throws IOException {
    killPrimary(nnIndex, true);
  }
  
  public void killPrimary(boolean clearZK) throws IOException {
    checkSingleNameNode();
    killPrimary(0, clearZK);
  }

  /**
   * Kill the primary avatar node.
   * @param clearZK clear zookeeper?
   */
  public void killPrimary(int nnIndex, boolean clearZK) throws IOException {
    AvatarInfo primary = getPrimaryAvatar(nnIndex);
    if (primary != null) {
      if (clearZK) {
        clearZooKeeperNode(nnIndex);
      }

      primary.avatar.shutdownAvatar();

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
  
  public void killStandby() throws IOException {
    checkSingleNameNode();
    killStandby(0);
  }

  /**
   * Kill the standby avatar node.
   */
  public void killStandby(int nnIndex) throws IOException {
    AvatarInfo standby = getStandbyAvatar(nnIndex);
    if (standby != null) {
      standby.avatar.shutdownAvatar();

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

  public void failOver() throws IOException {
    checkSingleNameNode();
    failOver(0);
  }

  /**
   * Make standby avatar the new primary avatar. Kill the old
   * primary avatar first if necessary.
   */
  public void failOver(int nnIndex) throws IOException {
    if (getPrimaryAvatar(nnIndex) != null) {
      LOG.info("killing primary avatar before failover");
      killPrimary(nnIndex);
    }

    AvatarInfo standby = getStandbyAvatar(nnIndex);
    if (standby == null) {
      throw new IOException("no standby avatar running");
    }

    standby.avatar.setAvatar(AvatarConstants.Avatar.ACTIVE);
    standby.state = AvatarState.ACTIVE;
    registerZooKeeperNode(standby.nnPort, standby.nnDnPort, standby.httpPort,
        standby.rpcPort, this.nameNodes[nnIndex]);
  }
  
  public void restartStandby() throws IOException {
    checkSingleNameNode();
    restartStandby(0);
  }
  /**
   * Restart a dead avatar node as a standby avatar.
   */
  public void restartStandby(int nnIndex) throws IOException {
    AvatarInfo dead = getDeadAvatar(nnIndex);
    if (getPrimaryAvatar(nnIndex) == null || dead == null) {
      throw new IOException("cannot start standby avatar: " +
                            "primary or dead avatar not found");
      
    }
    LOG.info("restarting " + dead.startupOption + " as standby");
    NameNodeInfo nni = this.nameNodes[nnIndex];
    String[] args; 
    ArrayList<String> argList = new ArrayList<String>();
    argList.add(dead.startupOption);
    argList.add(AvatarConstants.StartupOption.STANDBY.getName());
    argList.add(AvatarConstants.StartupOption.REGULAR.getName());
    if (federation) {
      argList.add(StartupOption.SERVICE.getName());
      argList.add(nni.nameserviceId);
    }
    args = new String[argList.size()];
    argList.toArray(args);
    dead.avatar = AvatarNode.createAvatarNode(args, 
        getServerConf(dead.startupOption, nni));
    dead.state = AvatarState.STANDBY;

    if (dead.avatar == null) {
      throw new IOException("cannot start avatar node");
    }
  }
  
  /**
   * return NameNodeInfo 
   */
  public NameNodeInfo getNameNode(int nnIndex) {
    return this.nameNodes[nnIndex];
  }
  
  public ArrayList<DataNodeProperties> getDataNodeProperties() {
    return dataNodes;
  }
  
  /**
   * Gets a list of the started DataNodes.  May be empty.
   */
  public ArrayList<AvatarDataNode> getDataNodes() {
    ArrayList<AvatarDataNode> list = new ArrayList<AvatarDataNode>();
    for (int i = 0; i < dataNodes.size(); i++) {
      AvatarDataNode node = dataNodes.get(i).datanode;
      list.add(node);
    }
    return list;
  }
  
  /*
   * return number of namenodes
   */
  public int getNumNameNodes() {
    return this.nameNodes.length;
  }
  
  /**
   * Add a namenode to cluster and start it. Configuration of datanodes
   * in the cluster is refreshed to register with the new namenode.
   * @return newly started namenode
   */
  public NameNodeInfo addNameNode(Configuration conf)
      throws IOException {
    if(!federation) {
      throw new IOException("cannot add namenode to non-federated cluster");
    }
    int nnIndex = nameNodes.length;
    int numNameNodes = nameNodes.length + 1;
    NameNodeInfo[] newlist = new NameNodeInfo[numNameNodes];
    System.arraycopy(nameNodes, 0, newlist, 0, nameNodes.length);
    nameNodes = newlist;
    nameNodes[nnIndex] = new NameNodeInfo(nnIndex);
    
    NameNodeInfo nni = nameNodes[nnIndex];
    nni.createAvatarDirs();
    String nameserviceId = NAMESERVICE_ID_PREFIX + getNSId();
    String nameserviceIds = conf.get(FSConstants.DFS_FEDERATION_NAMESERVICES);
    nameserviceIds += "," + nameserviceId;
    nni.initGeneralConf(conf, nameserviceId);
    conf.set(FSConstants.DFS_FEDERATION_NAMESERVICES, nameserviceIds);
    
    nni.updateAvatarConf(conf);
    startAvatarNode(nni, null);

    // Refresh datanodes with the newly started namenode
    for (DataNodeProperties dn : dataNodes) {
      DataNode datanode = dn.datanode;
      datanode.refreshNamenodes(conf);
    }
    // Wait for new namenode to get registrations from all the datanodes
    waitDataNodesActive(nnIndex);
    return nni;
  }
  
  private void updateAvatarConfWithServiceId(Configuration dstConf, Configuration srcConf,
      String nameserviceId) {
    for (String key: AvatarNode.AVATARSERVICE_SPECIFIC_KEYS) {
      String federationKey = DFSUtil.getNameServiceIdKey(
          key, nameserviceId);
      String value = srcConf.get(federationKey);
      if (value != null) {
        dstConf.set(federationKey, value);
      }
    }
    for (String key: NameNode.NAMESERVICE_SPECIFIC_KEYS) {
      String federationKey = DFSUtil.getNameServiceIdKey(
          key, nameserviceId);
      String value = srcConf.get(federationKey);
      if (value != null) {
        dstConf.set(federationKey, value);
      }
    }
  }
  
  /**
   * Add another cluster to current cluster and start it. Configuration of datanodes
   * in the cluster is refreshed to register with the new namenodes;
   */
  public void addCluster(MiniAvatarCluster cluster, boolean format)
      throws IOException, InterruptedException{
    if(!federation || !cluster.federation) {
      throw new IOException("Cannot handle non-federated cluster");
    }
    if (cluster.dataNodes.size() > this.dataNodes.size()) {
      throw new IOException("Cannot merge: new cluster has more datanodes the old one.");
    }
    this.shutDown();
    cluster.shutDown();
    
    int nnIndex = nameNodes.length;
    int numNameNodes = nameNodes.length + cluster.nameNodes.length;
    NameNodeInfo[] newlist = new NameNodeInfo[numNameNodes];
    System.arraycopy(nameNodes, 0, newlist, 0, nameNodes.length);
    System.arraycopy(cluster.nameNodes, 0, newlist, nameNodes.length, 
        cluster.nameNodes.length);
    nameNodes = newlist;
    String newNameserviceIds = cluster.conf.get(FSConstants.DFS_FEDERATION_NAMESERVICES);
    String nameserviceIds = conf.get(FSConstants.DFS_FEDERATION_NAMESERVICES);
    nameserviceIds += "," + newNameserviceIds;
    this.format = format;
    conf.set(FSConstants.DFS_FEDERATION_NAMESERVICES, nameserviceIds);

    int i;
    for (i = 0; i < nameNodes.length; i++) {
      NameNodeInfo nni = nameNodes[i];
      String nameserviceId = nni.nameserviceId;
      nni.initGeneralConf(nni.conf, nni.nameserviceId);
      nni.updateAvatarConf(nni.conf);
      for (int dnIndex = 0; dnIndex < dataNodes.size(); dnIndex++) {
        Configuration dstConf = dataNodes.get(dnIndex).conf;
        if (i >= nnIndex) {
          String dataStr = cluster.dataNodes.get(dnIndex).conf.get("dfs.data.dir");
          dstConf.set("dfs.merge.data.dir." + nameserviceId, dataStr);
        }
        updateAvatarConfWithServiceId(dstConf, nni.conf, nameserviceId);
      }
    }

    for (DataNodeProperties dn : dataNodes) {
      dn.conf.set(FSConstants.DFS_FEDERATION_NAMESERVICES, nameserviceIds);
      dn.datanode = AvatarDataNode.instantiateDataNode(dn.dnArgs, 
          new Configuration(dn.conf));
      dn.datanode.runDatanodeDaemon();
    }

    for (i = 0; i < nameNodes.length; i++) {
      NameNodeInfo nni = nameNodes[i];

      if (i < nnIndex) {
        startAvatarNode(nni, StartupOption.UPGRADE);
      } else {
        startAvatarNode(nni, null);
      }
    }
    waitAvatarNodesActive();
    waitDataNodesActive();
    waitExitSafeMode();
  }
  
  /*
   * Restart all datanodes
   */
  public synchronized boolean restartDataNodes() throws IOException, InterruptedException{
    shutDownDataNodes();
    int i = 0;
    for (DataNodeProperties dn : dataNodes) {
      i++;
      LOG.info("Restart Datanode " + i);
      dn.datanode = AvatarDataNode.instantiateDataNode(dn.dnArgs, 
          new Configuration(dn.conf));
      dn.datanode.runDatanodeDaemon();
      waitDataNodeInitialized(dn.datanode);
    }
    waitDataNodesActive();
    return true;
  }
  
  /**
   * Wait until the Datanode is initialized, or it throws an IOException
   * @param AvatarDataNode dn;
   * @throws IOException when some ServicePair threads are dead. 
   */
  public synchronized void waitDataNodeInitialized(AvatarDataNode dn) throws IOException {
    if (dn == null) {
      return ;
    }
    boolean initialized = false;
    while (!initialized) {
      initialized = true;
      for (int i = 0; i<nameNodes.length; i++) { 
        InetSocketAddress nameNodeAddr = getNameNode(i).avatars.
            get(0).avatar.getNameNodeDNAddress();
        if (!dn.initialized(nameNodeAddr)) {
          initialized = false;
          break;
        }
      }
      try {
        Thread.sleep(100);
      } catch (Exception e) {
      }
    }
  }
  
  public int getNamespaceId(int index) {
    return this.nameNodes[index].avatars.get(0).avatar.getNamespaceID();
  }
  
  static public int getNSId() {
    return MiniAvatarCluster.currNSId++;
  }
}
