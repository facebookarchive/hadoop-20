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

import java.io.File;
import java.io.IOException;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.nio.channels.FileChannel;
import java.util.Random;
import java.io.RandomAccessFile;
import javax.security.auth.login.LoginException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.net.*;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.FSConstants;
import org.apache.hadoop.hdfs.protocol.FSConstants.DatanodeReportType;
import org.apache.hadoop.hdfs.server.common.HdfsConstants.StartupOption;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.datanode.DataStorage;
import org.apache.hadoop.hdfs.server.datanode.FSDatasetInterface;
import org.apache.hadoop.hdfs.server.datanode.NameSpaceSliceStorage;
import org.apache.hadoop.hdfs.server.datanode.SimulatedFSDataset;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.server.protocol.NamespaceInfo;
import org.apache.hadoop.hdfs.tools.DFSAdmin;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.security.*;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.util.StringUtils;

/**
 * This class creates a single-process DFS cluster for junit testing.
 * The data directories for non-simulated DFS are under the testing directory.
 * For simulated data nodes, no underlying fs storage is used.
 */
public class MiniDFSCluster {
  static final Log LOG = LogFactory.getLog(MiniDFSCluster.class);
  public static final String NAMESERVICE_ID_PREFIX = "nameserviceId";
  public static int currNSId = 0;
  
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
  public static int getFreePort() {
    return getFreePorts(1);
  }

  /**
   * Get the specified number of consecutive free ports.
   * @return the first free port of the range
   */
  public static int getFreePorts(int num) {
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

  public class DataNodeProperties {
    public DataNode datanode;
    Configuration conf;
    String[] dnArgs;

    DataNodeProperties(DataNode node, Configuration conf, String[] args) {
      this.datanode = node;
      this.conf = conf;
      this.dnArgs = args;
    }
  }
  
  boolean federation = false;
  private Configuration conf;
  private NameNodeInfo[] nameNodes;
  
  /**
   * Stores the information related to a namenode in the cluster
   */
  static class NameNodeInfo {
    final NameNode nameNode;
    final Configuration conf;
    NameNodeInfo(NameNode nn, Configuration conf) {
      this.nameNode = nn;
      this.conf = new Configuration(conf);
    }
  }

  //private Configuration conf;
  private int numDataNodes;
  private ArrayList<DataNodeProperties> dataNodes = 
                         new ArrayList<DataNodeProperties>();
  private File base_dir;
  private File data_dir;

  public final static String FINALIZED_DIR_NAME = "/current/finalized/";
  public final static String RBW_DIR_NAME = "/current/rbw/";
  public final static String CURRENT_DIR_NAME = "/current";
  public final static String DFS_CLUSTER_ID = "dfs.clsuter.id";

  // wait until namenode has left safe mode?
  private boolean waitSafeMode = true;  
  
  /**
   * This null constructor is used only when wishing to start a data node cluster
   * without a name node (ie when the name node is started elsewhere).
   */
  public MiniDFSCluster() {
    nameNodes = new NameNodeInfo[0]; // No namenode in the cluster
  }
  
  /**
   * Modify the config and start up the servers with the given operation.
   * Servers will be started on free ports.
   * <p>
   * The caller must manage the creation of NameNode and DataNode directories
   * and have already set dfs.name.dir and dfs.data.dir in the given conf.
   * 
   * @param conf the base configuration to use in starting the servers.  This
   *          will be modified as necessary.
   * @param numDataNodes Number of DataNodes to start; may be zero
   * @param nameNodeOperation the operation with which to start the servers.  If null
   *          or StartupOption.FORMAT, then StartupOption.REGULAR will be used.
   */
  public MiniDFSCluster(Configuration conf,
                        int numDataNodes,
                        StartupOption nameNodeOperation) throws IOException {
    this(0, conf, numDataNodes, false, false, false, nameNodeOperation, 
          null, null, null);
  }
  
  public MiniDFSCluster(Configuration conf,
                        int numDataNodes,
                        StartupOption nameNodeOperation,
                        boolean manageDfsDirs,
                        int numNameNodes) throws IOException {
    this(0, conf, numDataNodes, false, manageDfsDirs,
        manageDfsDirs, nameNodeOperation, null, null, null, true, false, 
        numNameNodes, true);
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
   */
  public MiniDFSCluster(Configuration conf,
                        int numDataNodes,
                        boolean format,
                        String[] racks) throws IOException {
    this(0, conf, numDataNodes, format, true, true,  null, racks, null, null);
  }
  
  public MiniDFSCluster(Configuration conf,
                        int numDataNodes,
                        String[] racks,
                        String[] hosts,
                        boolean setupHostsFile,
                        boolean waitActive) throws IOException {
    this(0, conf, numDataNodes, true, true, true, null, racks, hosts, null,
        true, setupHostsFile, 1, false, waitActive);
  }

  public MiniDFSCluster(Configuration conf, int numDataNodes, boolean format,
                        String[] racks,
                        int numNameNodes) throws IOException {
    this(0, conf, numDataNodes, format, true, true, null, racks, null, null, 
        true, false, numNameNodes, true);
  }
  
  public MiniDFSCluster(int nameNodePort,
                        Configuration conf, 
                        int numDataNodes,
                        boolean format,
                        String[] racks,
                        int numNameNodes) throws IOException {
    this(nameNodePort, conf, numDataNodes, format, true, true, null, racks, null, null, 
        true, false, numNameNodes, true);
  }
  
  public MiniDFSCluster(int nameNodePort,
                        Configuration conf, 
                        int numDataNodes,
                        boolean format,
                        boolean manageDfsDirs,
                        String[] racks,
                        int numNameNodes) throws IOException {
    this(nameNodePort, conf, numDataNodes, format, manageDfsDirs, 
        manageDfsDirs, null, racks, null, null, true, false, numNameNodes, true);
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
   * @param wait until namenode has left safe mode?
   */
  public MiniDFSCluster(Configuration conf,
                        int numDataNodes,
                        boolean format,
                        String[] racks,
                        boolean waitSafeMode) throws IOException {
    this(0, conf, numDataNodes, format, true, true,  null, racks, null, null,
         waitSafeMode, false, 1, false);
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
   * @param hosts array of strings indicating the hostname for each DataNode
   */
  public MiniDFSCluster(Configuration conf,
                        int numDataNodes,
                        boolean format,
                        String[] racks, String[] hosts) throws IOException {
    this(0, conf, numDataNodes, format, true, true, null, racks, hosts, null);
  }
  
  /**
   * NOTE: if possible, the other constructors that don't have nameNode port 
   * parameter should be used as they will ensure that the servers use free ports.
   * <p>
   * Modify the config and start up the servers.  
   * 
   * @param nameNodePort suggestion for which rpc port to use.  caller should
   *          use getNameNodePort() to get the actual port used.
   * @param conf the base configuration to use in starting the servers.  This
   *          will be modified as necessary.
   * @param numDataNodes Number of DataNodes to start; may be zero
   * @param format if true, format the NameNode and DataNodes before starting up
   * @param manageDfsDirs if true, the data directories for servers will be
   *          created and dfs.name.dir and dfs.data.dir will be set in the conf
   * @param operation the operation with which to start the servers.  If null
   *          or StartupOption.FORMAT, then StartupOption.REGULAR will be used.
   * @param racks array of strings indicating the rack that each DataNode is on
   */
  public MiniDFSCluster(int nameNodePort, 
                        Configuration conf,
                        int numDataNodes,
                        boolean format,
                        boolean manageDfsDirs,
                        StartupOption operation,
                        String[] racks) throws IOException {
    this(nameNodePort, conf, numDataNodes, format, manageDfsDirs, manageDfsDirs,
         operation, racks, null, null);
  }

  /**
   * NOTE: if possible, the other constructors that don't have nameNode port 
   * parameter should be used as they will ensure that the servers use free ports.
   * <p>
   * Modify the config and start up the servers.  
   * 
   * @param nameNodePort suggestion for which rpc port to use.  caller should
   *          use getNameNodePort() to get the actual port used.
   * @param conf the base configuration to use in starting the servers.  This
   *          will be modified as necessary.
   * @param numDataNodes Number of DataNodes to start; may be zero
   * @param format if true, format the NameNode and DataNodes before starting up
   * @param manageDfsDirs if true, the data directories for servers will be
   *          created and dfs.name.dir and dfs.data.dir will be set in the conf
   * @param operation the operation with which to start the servers.  If null
   *          or StartupOption.FORMAT, then StartupOption.REGULAR will be used.
   * @param racks array of strings indicating the rack that each DataNode is on
   * @param simulatedCapacities array of capacities of the simulated data nodes
   */
  public MiniDFSCluster(int nameNodePort, 
                        Configuration conf,
                        int numDataNodes,
                        boolean format,
                        boolean manageDfsDirs,
                        StartupOption operation,
                        String[] racks,
                        long[] simulatedCapacities) throws IOException {
    this(nameNodePort, conf, numDataNodes, format, manageDfsDirs, manageDfsDirs,
          operation, racks, null, simulatedCapacities);
  }
  
  /**
   * NOTE: if possible, the other constructors that don't have nameNode port 
   * parameter should be used as they will ensure that the servers use free ports.
   * <p>
   * Modify the config and start up the servers.  
   * 
   * @param nameNodePort suggestion for which rpc port to use.  caller should
   *          use getNameNodePort() to get the actual port used.
   * @param conf the base configuration to use in starting the servers.  This
   *          will be modified as necessary.
   * @param numDataNodes Number of DataNodes to start; may be zero
   * @param format if true, format the NameNode and DataNodes before starting up
   * @param manageNameDfsDirs if true, the data directories for servers will be
   *          created and dfs.name.dir and dfs.data.dir will be set in the conf
   * @param manageDataDfsDirs if true, the data directories for datanodes will
   *          be created and dfs.data.dir set to same in the conf
   * @param operation the operation with which to start the servers.  If null
   *          or StartupOption.FORMAT, then StartupOption.REGULAR will be used.
   * @param racks array of strings indicating the rack that each DataNode is on
   * @param hosts array of strings indicating the hostnames of each DataNode
   * @param simulatedCapacities array of capacities of the simulated data nodes
   */
  public MiniDFSCluster(int nameNodePort, 
                        Configuration conf,
                        int numDataNodes,
                        boolean format,
                        boolean manageNameDfsDirs,
                        boolean manageDataDfsDirs,
                        StartupOption operation,
                        String[] racks, String hosts[],
                        long[] simulatedCapacities) throws IOException {
    this(nameNodePort, conf, numDataNodes, format, manageNameDfsDirs,
         manageDataDfsDirs, operation, racks, hosts, simulatedCapacities, true, false, 1, false);
  }

  public MiniDFSCluster(int nameNodePort, 
                        Configuration conf,
                        int numDataNodes,
                        boolean format,
                        boolean manageNameDfsDirs,
                        boolean manageDataDfsDirs,
                        StartupOption operation,
                        String[] racks, String hosts[],
                        long[] simulatedCapacities,
                        boolean waitSafeMode,
                        boolean setupHostsFile,
                        int numNameNodes,
                        boolean federation) throws IOException {
    this(nameNodePort, conf, numDataNodes, format, manageNameDfsDirs,
        manageDataDfsDirs, operation, racks, hosts, simulatedCapacities,
        waitSafeMode, setupHostsFile, numNameNodes, federation, true);
  }


  /**
   * NOTE: if possible, the other constructors that don't have nameNode port 
   * parameter should be used as they will ensure that the servers use free ports.
   * <p>
   * Modify the config and start up the servers.  
   * 
   * @param nameNodePort suggestion for which rpc port to use.  caller should
   *          use getNameNodePort() to get the actual port used.
   * @param conf the base configuration to use in starting the servers.  This
   *          will be modified as necessary.
   * @param numDataNodes Number of DataNodes to start; may be zero
   * @param format if true, format the NameNode and DataNodes before starting up
   * @param manageNameDfsDirs if true, the data directories for servers will be
   *          created and dfs.name.dir and dfs.data.dir will be set in the conf
   * @param manageDataDfsDirs if true, the data directories for datanodes will
   *          be created and dfs.data.dir set to same in the conf
   * @param operation the operation with which to start the servers.  If null
   *          or StartupOption.FORMAT, then StartupOption.REGULAR will be used.
   * @param racks array of strings indicating the rack that each DataNode is on
   * @param hosts array of strings indicating the hostnames of each DataNode
   * @param simulatedCapacities array of capacities of the simulated data nodes
   * @param wait until namenode has left safe mode?
   * @param setup the host file with datanode address
   * @param numNameNodes
   */
  public MiniDFSCluster(int nameNodePort, 
                        Configuration conf,
                        int numDataNodes,
                        boolean format,
                        boolean manageNameDfsDirs,
                        boolean manageDataDfsDirs,
                        StartupOption operation,
                        String[] racks, String hosts[],
                        long[] simulatedCapacities,
                        boolean waitSafeMode,
                        boolean setupHostsFile,
                        int numNameNodes,
                        boolean federation,
                        boolean waitActive) throws IOException {
    this.conf = conf;
    this.waitSafeMode = waitSafeMode;
    try {
      UserGroupInformation.setCurrentUser(UnixUserGroupInformation.login(conf));
    } catch (LoginException e) {
      IOException ioe = new IOException();
      ioe.initCause(e);
      throw ioe;
    }
    base_dir = getBaseDirectory();
    data_dir = new File(base_dir, "data");
    
    // Setup the NameNode configuration
    int replication = conf.getInt("dfs.replication", 3);
    conf.setInt("dfs.replication", Math.min(replication, numDataNodes));
    conf.setInt("dfs.safemode.extension", 0);
    conf.setInt("dfs.namenode.decommission.interval", 3); // 3 second
    conf.setClass("topology.node.switch.mapping.impl", 
                   StaticMapping.class, DNSToSwitchMapping.class);
    
    
    this.federation = federation;
    
    this.nameNodes = new NameNodeInfo[numNameNodes];
    if (!federation) {
      conf.set(FileSystem.FS_DEFAULT_NAME_KEY, "127.0.0.1:" + nameNodePort);
      conf.set("dfs.http.address", "127.0.0.1:0");
      FileSystem.setDefaultUri(conf, "hdfs://localhost:" +
                               Integer.toString(nameNodePort));
      NameNode nn = createNameNode(0, conf, numDataNodes, manageNameDfsDirs,
          format, operation);
      nameNodes[0] = new NameNodeInfo(nn, conf);
    } else {
      Collection<String> nameserviceIds = conf.getStringCollection(
          FSConstants.DFS_FEDERATION_NAMESERVICES);
      if (nameserviceIds == null || nameserviceIds.size() == 0) {
        nameserviceIds = new ArrayList<String>();
        for (int i = 0; i < nameNodes.length; i++) {
          nameserviceIds.add(NAMESERVICE_ID_PREFIX + getNSId());
        }
      } else if (nameserviceIds.size() != nameNodes.length) {
        throw new IOException("number of nameservices " + 
          conf.get(FSConstants.DFS_FEDERATION_NAMESERVICES) + 
          "doesn't match number of namenodes");
      }
      initFederationConf(conf, nameserviceIds, numDataNodes, nameNodePort);
      createFederationNamenodes(conf, nameserviceIds, manageNameDfsDirs, format,
          operation);
    }
    // Format and clean out DataNode directories
    if (format) {
      if (data_dir.exists() && !FileUtil.fullyDelete(data_dir)) {
        throw new IOException("Cannot remove data directory: " + data_dir);
      }
    }

    // Start the DataNodes
    if (numDataNodes > 0) {
      startDataNodes(conf, numDataNodes, manageDataDfsDirs, operation, racks,
          hosts, simulatedCapacities, setupHostsFile, waitActive);
    }
    if (waitActive) {
      waitClusterUp();
    }
  }
  
  /** Initialize configuration for federation cluster */
  private static void initFederationConf(Configuration conf,
      Collection<String> nameserviceIds, int numDataNodes, int nnPort) {
    String nameserviceIdList = "";
    for (String nameserviceId : nameserviceIds) {
      // Create comma separated list of nameserviceIds
      if (nameserviceIdList.length() > 0) {
        nameserviceIdList += ",";
      }
      nameserviceIdList += nameserviceId;
      initFederatedNamenodeAddress(conf, nameserviceId, nnPort);
      nnPort = nnPort == 0 ? 0 : nnPort + 2;
    }
    conf.set(FSConstants.DFS_FEDERATION_NAMESERVICES, nameserviceIdList);
  }

  /* For federated namenode initialize the address:port */
  private static void initFederatedNamenodeAddress(Configuration conf,
      String nameserviceId, int nnPort) {
    // Set nameserviceId specific key
    String key = DFSUtil.getNameServiceIdKey(
        FSConstants.DFS_NAMENODE_HTTP_ADDRESS_KEY, nameserviceId);
    conf.set(key, "127.0.0.1:0");

    key = DFSUtil.getNameServiceIdKey(
        FSConstants.DFS_NAMENODE_RPC_ADDRESS_KEY, nameserviceId);
    conf.set(key, "127.0.0.1:" + nnPort);
    
    key = DFSUtil.getNameServiceIdKey(
        NameNode.DATANODE_PROTOCOL_ADDRESS, nameserviceId);
    conf.set(key, "127.0.0.1:0");
  }
  
  private void createFederationNamenodes(Configuration conf,
      Collection<String> nameserviceIds, boolean manageNameDfsDirs,
      boolean format, StartupOption operation)
      throws IOException {
    // Create namenodes in the cluster
    int nnCounter = 0;
    for (String nameserviceId : nameserviceIds) {
      createFederatedNameNode(nnCounter++, conf, numDataNodes, manageNameDfsDirs,
          format, operation, nameserviceId);
    }
  }
  
  private NameNode createNameNode(int nnIndex, Configuration conf,
      int numDataNodes,
      boolean manageNameDfsDirs,
      boolean format,
      StartupOption operation) throws IOException {
    return createNameNode(nnIndex, conf, numDataNodes, manageNameDfsDirs,
        format, operation, null); 
  }
  
  private NameNode createNameNode(int nnIndex, Configuration conf,
      int numDataNodes,
      boolean manageNameDfsDirs,
      boolean format,
      StartupOption operation,
      String nameServiceId) throws IOException {
    // Setup the NameNode configuration
    if (manageNameDfsDirs) {
      if (this.nameNodes[nnIndex] != null) {
        Configuration nnconf = this.nameNodes[nnIndex].conf;
        conf.set("dfs.name.dir", nnconf.get("dfs.name.dir"));
        conf.set("fs.checkpoint.dir", nnconf.get("fs.checkpoint.dir"));
      } else {
        conf.set("dfs.name.dir", new File(base_dir, "name" + (2*nnIndex + 1)).getPath()+","+
                 new File(base_dir, "name" + (2*nnIndex + 2)).getPath());
        conf.set("fs.checkpoint.dir", new File(base_dir, "namesecondary" + (2*nnIndex + 1)).
                  getPath()+"," + new File(base_dir, "namesecondary" + (2*nnIndex + 2)).getPath());
      }
    }

    
    // Format and clean out DataNode directories
    if (format) {
      Configuration newConf = conf;
      if (federation) {
        newConf = new Configuration(conf);
        NameNode.initializeGenericKeys(newConf, nameServiceId);
      }
      NameNode.format(newConf);
    }
    // Start the NameNode
    String[] args;
    ArrayList<String> argList = new ArrayList<String>();
    if (!(operation == null ||
          operation == StartupOption.FORMAT ||
          operation == StartupOption.REGULAR)) {
      argList.add(operation.getName());
    } 
    if (federation) {
      argList.add(StartupOption.SERVICE.getName());
      argList.add(nameServiceId);
      conf = new Configuration(conf);
    }
    args = new String[argList.size()];
    argList.toArray(args);
    return NameNode.createNameNode(args, conf);
  }
  
  private void createFederatedNameNode(int nnIndex, Configuration conf,
      int numDataNodes, boolean manageNameDfsDirs, boolean format,
      StartupOption operation, String nameserviceId)
      throws IOException {
    conf.set(FSConstants.DFS_FEDERATION_NAMESERVICE_ID, nameserviceId);
    NameNode nn = createNameNode(nnIndex, conf, numDataNodes, manageNameDfsDirs,
        format, operation, nameserviceId);
    DFSUtil.setGenericConf(conf, nameserviceId, 
        NameNode.NAMESERVICE_SPECIFIC_KEYS);
    conf.set(DFSUtil.getNameServiceIdKey(
        FSConstants.DFS_NAMENODE_HTTP_ADDRESS_KEY, nameserviceId), NameNode
        .getHostPortString(nn.getHttpAddress()));
    conf.set(DFSUtil.getNameServiceIdKey(
        NameNode.DATANODE_PROTOCOL_ADDRESS, nameserviceId), NameNode
        .getHostPortString(nn.getNameNodeDNAddress()));
    nameNodes[nnIndex] = new NameNodeInfo(nn, new Configuration(conf));
  }

  /**
   * wait for the cluster to get out of 
   * safemode.
   */
  public void waitClusterUp() {
    if (numDataNodes > 0) {
      while (!isClusterUp()) {
        try {
          System.err.println("Waiting for the Mini HDFS Cluster to start...");
          Thread.sleep(1000);
        } catch (InterruptedException e) {
        }
      }
    }
  }
  
  public void startDataNodes(Configuration conf, int numDataNodes, 
      boolean manageDfsDirs, StartupOption operation, 
      String[] racks, String[] hosts,
      long[] simulatedCapacities) throws IOException {
    startDataNodes(conf, numDataNodes, manageDfsDirs, operation,
        racks, hosts, simulatedCapacities, false);
  }
  
  public synchronized void startDataNodes(Configuration conf, int numDataNodes, 
                             boolean manageDfsDirs, StartupOption operation, 
                             String[] racks, String[] hosts,
                             long[] simulatedCapacities,
                             boolean setupHostsFile) throws IOException {
    startDataNodes(conf, numDataNodes, manageDfsDirs, operation,
        racks, hosts, simulatedCapacities, false, true);
  }

  /**
   * Modify the config and start up additional DataNodes.  The info port for
   * DataNodes is guaranteed to use a free port.
   *  
   *  Data nodes can run with the name node in the mini cluster or
   *  a real name node. For example, running with a real name node is useful
   *  when running simulated data nodes with a real name node.
   *  If minicluster's name node is null assume that the conf has been
   *  set with the right address:port of the name node.
   *
   * @param conf the base configuration to use in starting the DataNodes.  This
   *          will be modified as necessary.
   * @param numDataNodes Number of DataNodes to start; may be zero
   * @param manageDfsDirs if true, the data directories for DataNodes will be
   *          created and dfs.data.dir will be set in the conf
   * @param operation the operation with which to start the DataNodes.  If null
   *          or StartupOption.FORMAT, then StartupOption.REGULAR will be used.
   * @param racks array of strings indicating the rack that each DataNode is on
   * @param hosts array of strings indicating the hostnames for each DataNode
   * @param simulatedCapacities array of capacities of the simulated data nodes
   * @param setup the host file with datanode address
   *
   * @throws IllegalStateException if NameNode has been shutdown
   */
  public synchronized void startDataNodes(Configuration conf, int numDataNodes, 
                             boolean manageDfsDirs, StartupOption operation, 
                             String[] racks, String[] hosts,
                             long[] simulatedCapacities,
                             boolean setupHostsFile,
                             boolean waitActive) throws IOException {

    int curDatanodesNum = dataNodes.size();
    // for mincluster's the default initialDelay for BRs is 0
    if (conf.get("dfs.blockreport.initialDelay") == null) {
      conf.setLong("dfs.blockreport.initialDelay", 0);
    }
    
    if (racks != null && numDataNodes > racks.length ) {
      throw new IllegalArgumentException( "The length of racks [" + racks.length
          + "] is less than the number of datanodes [" + numDataNodes + "].");
    }
    if (hosts != null && numDataNodes > hosts.length ) {
      throw new IllegalArgumentException( "The length of hosts [" + hosts.length
          + "] is less than the number of datanodes [" + numDataNodes + "].");
    }
    //Generate some hostnames if required
    if (racks != null && hosts == null) {
      System.out.println("Generating host names for datanodes");
      hosts = new String[numDataNodes];
      for (int i = curDatanodesNum; i < curDatanodesNum + numDataNodes; i++) {
        hosts[i - curDatanodesNum] = "host" + i + ".foo.com";
      }
    }

    if (simulatedCapacities != null 
        && numDataNodes > simulatedCapacities.length) {
      throw new IllegalArgumentException( "The length of simulatedCapacities [" 
          + simulatedCapacities.length
          + "] is less than the number of datanodes [" + numDataNodes + "].");
    }

    String [] dnArgs = (operation == null ||
                        operation != StartupOption.ROLLBACK) ?
        null : new String[] {operation.getName()};
    
    for (int i = curDatanodesNum; i < curDatanodesNum+numDataNodes; i++) {
      Configuration dnConf = new Configuration(conf);
      // Set up datanode address
      setupDatanodeAddress(dnConf, setupHostsFile);
      if (manageDfsDirs) {
        File dir1 = new File(data_dir, "data"+(2*i+1));
        File dir2 = new File(data_dir, "data"+(2*i+2));
        dir1.mkdirs();
        dir2.mkdirs();
        if (!dir1.isDirectory() || !dir2.isDirectory()) { 
          throw new IOException("Mkdirs failed to create directory for DataNode "
                                + i + ": " + dir1 + " or " + dir2);
        }
        dnConf.set("dfs.data.dir", dir1.getPath() + "," + dir2.getPath()); 
      }
      if (simulatedCapacities != null) {
        dnConf.setBoolean("dfs.datanode.simulateddatastorage", true);
        dnConf.setLong(SimulatedFSDataset.CONFIG_PROPERTY_CAPACITY,
            simulatedCapacities[i-curDatanodesNum]);
      }
      System.out.println("Starting DataNode " + i + " with dfs.data.dir: " 
                         + dnConf.get("dfs.data.dir"));
      if (hosts != null) {
        dnConf.set("slave.host.name", hosts[i - curDatanodesNum]);
        System.out.println("Starting DataNode " + i + " with hostname set to: " 
                           + dnConf.get("slave.host.name"));
      }
      if (racks != null) {
        String name = hosts[i - curDatanodesNum];
        System.out.println("Adding node with hostname : " + name + " to rack "+
                            racks[i-curDatanodesNum]);
        StaticMapping.addNodeToRack(name,
                                    racks[i-curDatanodesNum]);
      }
      Configuration newconf = new Configuration(dnConf); // save config
      if (hosts != null) {
        NetUtils.addStaticResolution(hosts[i - curDatanodesNum], "localhost");
      }
      DataNode dn = DataNode.instantiateDataNode(dnArgs, dnConf);
      if(dn == null)
        throw new IOException("Cannot start DataNode in " +
          conf.get("dfs.data.dir"));
      //since the HDFS does things based on IP:port, we need to add the mapping
      //for IP:port to rackId
      String ipAddr = dn.getSelfAddr().getAddress().getHostAddress();
      if (racks != null) {
        int port = dn.getSelfAddr().getPort();
        System.out.println("Adding node with IP:port : " + ipAddr + ":" + port+
                            " to rack " + racks[i-curDatanodesNum]);
        StaticMapping.addNodeToRack(ipAddr + ":" + port,
                                  racks[i-curDatanodesNum]);
      }
      dn.runDatanodeDaemon();
      waitDataNodeInitialized(dn);
      dataNodes.add(new DataNodeProperties(dn, newconf, dnArgs));
    }
    curDatanodesNum += numDataNodes;
    this.numDataNodes += numDataNodes;
    if (waitActive) {
      waitActive();
    }
  }
  
  
  
  /**
   * Modify the config and start up the DataNodes.  The info port for
   * DataNodes is guaranteed to use a free port.
   *
   * @param conf the base configuration to use in starting the DataNodes.  This
   *          will be modified as necessary.
   * @param numDataNodes Number of DataNodes to start; may be zero
   * @param manageDfsDirs if true, the data directories for DataNodes will be
   *          created and dfs.data.dir will be set in the conf
   * @param operation the operation with which to start the DataNodes.  If null
   *          or StartupOption.FORMAT, then StartupOption.REGULAR will be used.
   * @param racks array of strings indicating the rack that each DataNode is on
   *
   * @throws IllegalStateException if NameNode has been shutdown
   */
  
  public void startDataNodes(Configuration conf, int numDataNodes, 
      boolean manageDfsDirs, StartupOption operation, 
      String[] racks) throws IOException {
    startDataNodes( conf,  numDataNodes, manageDfsDirs,  operation, racks, null, null, false);
  }
  
  /**
   * Modify the config and start up additional DataNodes.  The info port for
   * DataNodes is guaranteed to use a free port.
   *  
   *  Data nodes can run with the name node in the mini cluster or
   *  a real name node. For example, running with a real name node is useful
   *  when running simulated data nodes with a real name node.
   *  If minicluster's name node is null assume that the conf has been
   *  set with the right address:port of the name node.
   *
   * @param conf the base configuration to use in starting the DataNodes.  This
   *          will be modified as necessary.
   * @param numDataNodes Number of DataNodes to start; may be zero
   * @param manageDfsDirs if true, the data directories for DataNodes will be
   *          created and dfs.data.dir will be set in the conf
   * @param operation the operation with which to start the DataNodes.  If null
   *          or StartupOption.FORMAT, then StartupOption.REGULAR will be used.
   * @param racks array of strings indicating the rack that each DataNode is on
   * @param simulatedCapacities array of capacities of the simulated data nodes
   *
   * @throws IllegalStateException if NameNode has been shutdown
   */
  public void startDataNodes(Configuration conf, int numDataNodes, 
                             boolean manageDfsDirs, StartupOption operation, 
                             String[] racks,
                             long[] simulatedCapacities) throws IOException {
    startDataNodes(conf, numDataNodes, manageDfsDirs, operation, racks, null,
                   simulatedCapacities, false);
    
  }
  
  
  /**
   * Finalize one namenode with its configuration
   * @param nn
   * @param conf
   * @throws Exception
   */
  public void finalizeNameNode(NameNode nn, Configuration conf) throws Exception{
    if (nn == null) {
      throw new IllegalStateException("Attempting to finalize "
                                    + "Namenode but it is not running");
    }
    ToolRunner.run(new DFSAdmin(conf), new String[] {"-finalizeUpgrade"});
  }
  
  /**
   * If the NameNode is running, attempt to finalize a previous upgrade.
   * When this method return, the NameNode should be finalized, but
   * DataNodes may not be since that occurs asynchronously.
   *
   * @throws IllegalStateException if the Namenode is not running.
   */
  public void finalizeCluster(Configuration conf) throws Exception {
    for (NameNodeInfo nnInfo : nameNodes) {
      if (nnInfo == null) {
        throw new IllegalStateException("Attempting to finalize "
                                        + "Namenode but it is not running");
      }
      finalizeNameNode(nnInfo.nameNode, nnInfo.conf);
    }
  }
  
  /**
   * Gets the started NameNode.  May be null.
   */
  public NameNode getNameNode() {
    checkSingleNameNode();
    return getNameNode(0);
  }
  
  public NameNode getNameNode(int nnIndex) {
    return nameNodes[nnIndex].nameNode;
  }
  
  private void checkSingleNameNode() {
    if (nameNodes.length != 1) {
      throw new IllegalArgumentException("It's not a single namenode cluster, use index instead.");
    }
  }
  
  /**
   * Gets a list of the started DataNodes.  May be empty.
   */
  public ArrayList<DataNode> getDataNodes() {
    ArrayList<DataNode> list = new ArrayList<DataNode>();
    for (int i = 0; i < dataNodes.size(); i++) {
      DataNode node = dataNodes.get(i).datanode;
      list.add(node);
    }
    return list;
  }
  
  /** @return the datanode having the ipc server listen port */
  public DataNode getDataNode(int ipcPort) {
    for(DataNode dn : getDataNodes()) {
      if (dn.ipcServer.getListenerAddress().getPort() == ipcPort) {
        return dn;
      }
    }
    return null;
  }

  /**
   * Gets the rpc port used by the NameNode, because the caller 
   * supplied port is not necessarily the actual port used.
   */     
  public int getNameNodePort() {
    checkSingleNameNode();
    return getNameNodePort(0);
  }
  
  public int getNameNodePort(int nnIndex) {
    return nameNodes[nnIndex].nameNode.getNameNodeAddress().getPort();
  }
    
  public void shutdown() {
    shutdown(true);
  }
  /**
   * Shut down all the servers that are up.
   * @param remove: remove datanode information from the dataNodes or not
   */
  public void shutdown(boolean remove) {
    System.out.println("Shutting down the Mini HDFS Cluster");
    shutdownDataNodes(remove);
    for (NameNodeInfo nnInfo : nameNodes) {
      NameNode nameNode = nnInfo.nameNode;
      if (nameNode != null) {
        nameNode.stop();
        nameNode.join();
        nameNode = null;
      }
    }
  }
  
  public void shutdownDataNodes() {
    shutdownDataNodes(true);
  }
  /**
   * Shutdown all DataNodes started by this class.  The NameNode
   * is left running so that new DataNodes may be started.
   */
  public void shutdownDataNodes(boolean remove) {
    for (int i = dataNodes.size()-1; i >= 0; i--) {
      shutdownDataNode(i, remove);
    }
  }
  
  public void shutdownDataNode(int index, boolean remove) {
    System.out.println("Shutting down DataNode " + index);
    DataNode dn = remove ? dataNodes.remove(index).datanode : dataNodes
        .get(index).datanode;
    dn.shutdown();
    numDataNodes--;
  }

  public synchronized void shutdownNameNode() {
    checkSingleNameNode();
    shutdownNameNodes();
  }

  public synchronized void restartNameNode() throws IOException {
    checkSingleNameNode();
    restartNameNodes();
  }

  /**
   * Shutdown one namenode
   */
  public synchronized void shutdownNameNode(int nnIndex) {
    NameNode nn = nameNodes[nnIndex].nameNode;
    if (nn != null) {
      System.out.println("Shutting down the namenode");
      nn.stop();
      nn.join();
      Configuration conf = nameNodes[nnIndex].conf;
      nameNodes[nnIndex] = new NameNodeInfo(null, conf);
    }
  }

  /**
   * Shutdown namenodes.
   */
  public synchronized void shutdownNameNodes() {
    for (int i = 0; i<nameNodes.length; i++) {
      shutdownNameNode(i);
    }
  }

  /**
   * Restart namenodes.
   */
  public synchronized void restartNameNodes() throws IOException {
    for (int i = 0; i<nameNodes.length; i++) {
      restartNameNode(i);
    }
  }

  public synchronized void restartNameNode(int nnIndex) throws IOException {
    restartNameNode(nnIndex, new String[] {});
  }

  public synchronized void restartNameNode(int nnIndex, String[] argv) throws IOException {
    restartNameNode(nnIndex, argv, true);
  }

  public synchronized void restartNameNode(int nnIndex, String[] argv, boolean waitActive) throws IOException {
    shutdownNameNode(nnIndex);
    Configuration conf = nameNodes[nnIndex].conf;
    NameNode nn = NameNode.createNameNode(argv, conf);
    nameNodes[nnIndex] = new NameNodeInfo(nn, conf);
    if (!waitActive) {
      return;
    }
    waitClusterUp();
    System.out.println("Restarted the namenode");
    int failedCount = 0;
    while (true) {
      try {
        waitActive();
        break;
      } catch (IOException e) {
        failedCount++;
        // Cached RPC connection to namenode, if any, is expected to fail once
        if (failedCount > 5) {
          System.out.println("Tried waitActive() " + failedCount
              + " time(s) and failed, giving up.  "
                             + StringUtils.stringifyException(e));
          throw e;
        }
      }
    }
    System.out.println("Cluster is active");
  }


  /*
   * Corrupt a block on all datanode
   */
  void corruptBlockOnDataNodes(String blockName) throws Exception{
    for (int i=0; i < dataNodes.size(); i++)
      corruptBlockOnDataNode(i,blockName);
  }

  /*
   * Corrupt a block on a particular datanode
   */
  boolean corruptBlockOnDataNode(int i, String blockName) throws Exception {
    Random random = new Random();
    boolean corrupted = false;
    if (i < 0 || i >= dataNodes.size())
      return false;
    for (int dn = i*2; dn < i*2+2; dn++) {
      File blockFile = new File(getBlockDirectory("data" + (dn+1)),
                                blockName);
      System.out.println("Corrupting for: " + blockFile);
      if (blockFile.exists()) {
        // Corrupt replica by writing random bytes into replica
        RandomAccessFile raFile = new RandomAccessFile(blockFile, "rw");
        FileChannel channel = raFile.getChannel();
        String badString = "BADBAD";
        int rand = random.nextInt((int)channel.size()/2);
        raFile.seek(rand);
        raFile.write(badString.getBytes());
        raFile.close();
      }
      corrupted = true;
    }
    return corrupted;
  }

  /*
   * Shutdown a particular datanode
   */
  public DataNodeProperties stopDataNode(int i) {
    if (i < 0 || i >= dataNodes.size()) {
      return null;
    }
    DataNodeProperties dnprop = dataNodes.remove(i);
    DataNode dn = dnprop.datanode;
    System.out.println("MiniDFSCluster Stopping DataNode " + 
                       dn.getDatanodeInfo() +
                       " from a total of " + (dataNodes.size() + 1) + 
                       " datanodes.");
    dn.shutdown();
    numDataNodes--;
    return dnprop;
  }

  /**
   * Restart a datanode
   * @param dnprop datanode's property
   * @return true if restarting is successful
   * @throws IOException
   */
  public synchronized boolean restartDataNode(DataNodeProperties dnprop)
  throws IOException {
    Configuration conf = dnprop.conf;
    String[] args = dnprop.dnArgs;
    Configuration newconf = new Configuration(conf); // save cloned config
    dataNodes.add(new DataNodeProperties(
                     DataNode.createDataNode(args, conf), 
                     newconf, args));
    waitDataNodeInitialized(dataNodes.get(numDataNodes).datanode);
    numDataNodes++;
    return true;
  }
  /*
   * Restart a particular datanode
   */
  public synchronized boolean restartDataNode(int i) throws IOException {
    DataNodeProperties dnprop = stopDataNode(i);
    if (dnprop == null) {
      return false;
    } else {
      return restartDataNode(dnprop);
    }
  }

  /*
   * Restart all datanodes
   */
  public synchronized boolean restartDataNodes() throws IOException {
    for (int i = dataNodes.size()-1; i >= 0; i--) {
      System.out.println("Restarting DataNode " + i);
      if (!restartDataNode(i))
        return false;
    }
    return true;
  }

  /*
   * Shutdown a datanode by name.
   */
  public synchronized DataNodeProperties stopDataNode(String name) {
    int i = findDataNodeIndex(name);
    if (i == -1) return null;
    return stopDataNode(i);
  }
  
  public synchronized int findDataNodeIndex(String name) {
    int i;
    int namespaceId = getNameNode(0).getNamespaceID();
    try {
      for (i = 0; i < dataNodes.size(); i++) {
        DataNode dn = dataNodes.get(i).datanode;
        if (dn.getDNRegistrationForNS(namespaceId).getName().equals(name)) {
          break;
        }
      }
    } catch (IOException e){
      LOG.error(e);
      return -1;
    }
    return i;
  }
  
  /**
   * Returns true if the NameNode is running and is out of Safe Mode
   * or if waiting for safe mode is disabled.
   */
  public boolean isNameNodeUp(int nnIndex) {
    NameNode nn = nameNodes[nnIndex].nameNode;
    if (nn == null) {
      return false;
    }
    try {
      long[] sizes = nn.getStats();
      boolean isUp = false;
      synchronized (this) {
        isUp = ((!nn.isInSafeMode() || !waitSafeMode) && sizes[0] != 0);
      }
      return isUp;
    } catch (IOException ie) {
      return false;
    }
  }
  
  public boolean isClusterUp() {
    for (int i = 0; i < nameNodes.length; i++) {
      if (!isNameNodeUp(i)) {
        return false;
      }
    }
    return true;
  }
  
  /**
   * Returns true if there is at least one DataNode running.
   */
  public boolean isDataNodeUp() {
    if (dataNodes == null || dataNodes.size() == 0) {
      return false;
    }
    for (DataNodeProperties dn : dataNodes) {
      if (dn.datanode.isDatanodeUp()) {
        return true;
      }
    }
    return false;
  }
  
  /**
   * Get a client handle to the DFS cluster.
   */
  public FileSystem getFileSystem() throws IOException {
    checkSingleNameNode();
    return getFileSystem(0);
  }
  
  public FileSystem getFileSystem(int nnIndex) throws IOException{
    return FileSystem.get(getURI(nnIndex), nameNodes[nnIndex].conf);
  }

  /**
   * Get a client handle to the DFS cluster.
   */
  public FileSystem getUniqueFileSystem() throws IOException {
    checkSingleNameNode();
    return FileSystem.newInstance(nameNodes[0].conf);
  }

  /**
   * Get the directories where the namenode stores its image.
   */
  public Collection<File> getNameDirs() {
    checkSingleNameNode();
    return getNameDirs(0);
  }
  
  public Collection<File> getNameDirs(int nnIndex) {
    return FSNamesystem.getNamespaceDirs(nameNodes[nnIndex].conf);
  }

  /**
   * Get the directories where the namenode stores its edits.
   */
  public Collection<File> getNameEditsDirs() {
    checkSingleNameNode();
    return getNameEditsDirs(0);
  }
  
  public Collection<File> getNameEditsDirs(int nnIndex) {
    return FSNamesystem.getNamespaceEditsDirs(nameNodes[nnIndex].conf);
  }

  /**
   * Wait until the cluster is active and running.
   */
  public void waitActive() throws IOException {
    waitActive(true);
  }
  
  public void waitActive(boolean waitHeartbeats) throws IOException {
    for (int i = 0; i < nameNodes.length; i++) {
      waitActive(waitHeartbeats, i);
    }
  }

  /**
   * Wait until the cluster is active.
   * @param waitHeartbeats if true, will wait until all DNs have heartbeat
   */
  public void waitActive(boolean waitHeartbeats, int nnIndex) throws IOException {
    if (nnIndex < 0 || nnIndex >= nameNodes.length || nameNodes[nnIndex] == null) {
      return;
    }
    NameNode nn = nameNodes[nnIndex].nameNode;
    if (nn == null) {
      return;
    }
    InetSocketAddress addr = nn.getNameNodeAddress();
    // Wait for the client server to start if we have two configured
    while (addr == null) {
      try {
        Thread.sleep(100);
      } catch (Exception e) {
      }
      addr = nn.getNameNodeAddress();
    }
    addr = nn.getNameNodeDNAddress();
    DFSClient client = new DFSClient(addr, nn.getConf());

    // make sure all datanodes are alive and sent heartbeat
    while (shouldWait(client.datanodeReport(DatanodeReportType.LIVE),
                      waitHeartbeats, addr)) {
      try {
        Thread.sleep(100);
      } catch (InterruptedException e) {
      }
    }

    client.close();
  }

  /**
   * Wait until the Datanode is initialized, or it throws an IOException
   * @param DataNode dn;
   * @throws IOException when some NSOfferService threads are dead. 
   */
  public synchronized void waitDataNodeInitialized(DataNode dn) throws IOException {
    if (dn == null) {
      return ;
    }
    boolean initialized = false;
    while (!initialized) {
      initialized = true;
      for (int i = 0; i<nameNodes.length; i++) { 
        InetSocketAddress nameNodeAddr = nameNodes[i].nameNode.getNameNodeDNAddress();
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
  
  private synchronized boolean shouldWait(DatanodeInfo[] dnInfo,
                                          boolean waitHeartbeats,
                                          InetSocketAddress addr){
    for (DataNodeProperties dn : dataNodes) {
      if (!dn.datanode.isNamespaceAlive(addr)) {
        return false;
      }
    }
    
    if (dnInfo.length != numDataNodes) {
      return true;
    }
    
    // if one of the data nodes is not fully started, continue to wait
    for (DataNodeProperties dn : dataNodes) {
      if (!dn.datanode.isInitialized()) {
        return true;
      }
    }

    // If we don't need heartbeats, we're done.
    if (!waitHeartbeats) {
      return false;
    }

    // make sure all datanodes have sent first heartbeat to namenode,
    // using (capacity == 0) as proxy.
    for (DatanodeInfo dn : dnInfo) {
      if (dn.getCapacity() == 0) {
        return true;
      }
    }
    return false;
  }

  /**
   * Wait for the given datanode to heartbeat once.
   */
  public void waitForDNHeartbeat(int dnIndex, long timeoutMillis)
    throws IOException, InterruptedException {
    DataNode dn = getDataNodes().get(dnIndex);
    for (int i = 0; i<nameNodes.length; i++) {
      waitForDNHeartbeat(dn, timeoutMillis, i);
    }
  }
  
  private void waitForDNHeartbeat(DataNode dn, long timeoutMillis, int nnIndex)
    throws IOException, InterruptedException {
    InetSocketAddress addr = new InetSocketAddress("localhost",
                                                   getNameNodePort(nnIndex));
    DFSClient client = new DFSClient(addr, nameNodes[nnIndex].conf);
    int namespaceId = getNameNode(nnIndex).getNamespaceID();
    long startTime = System.currentTimeMillis();
    while (System.currentTimeMillis() < startTime + timeoutMillis) {
      DatanodeInfo report[] = client.datanodeReport(DatanodeReportType.LIVE);
 
      for (DatanodeInfo thisReport : report) {
        if (thisReport.getStorageID().equals(
              dn.getDNRegistrationForNS(namespaceId).getStorageID())) {
          if (thisReport.getLastUpdate() > startTime)
            return;
        }
      }

      Thread.sleep(500);
    }
  }
  
  public void formatDataNodeDirs() throws IOException {
    base_dir = getBaseDirectory();
    data_dir = new File(base_dir, "data");
    if (data_dir.exists() && !FileUtil.fullyDelete(data_dir)) {
      throw new IOException("Cannot remove data directory: " + data_dir);
    }
  }
  
  /**
   * 
   * @param dataNodeIndex - data node whose block report is desired - the index is same as for getDataNodes()
   * @return the block report for the specified data node
   */
  public Block[] getBlockReport(int dataNodeIndex, int namespaceId) throws IOException{
    if (dataNodeIndex < 0 || dataNodeIndex > dataNodes.size()) {
      throw new IndexOutOfBoundsException();
    }
    return dataNodes.get(dataNodeIndex).datanode.getFSDataset().getBlockReport(namespaceId);
  }
  
  
  /**
   * 
   * @return block reports from all data nodes
   *    Block[] is indexed in the same order as the list of datanodes returned by getDataNodes()
   */
  public Block[][] getAllBlockReports(int nsId) throws IOException{
    int numDataNodes = dataNodes.size();
    Block[][] result = new Block[numDataNodes][];
    for (int i = 0; i < numDataNodes; ++i) {
     result[i] = getBlockReport(i, nsId);
    }
    return result;
  }
  
  
  /**
   * This method is valid only if the data nodes have simulated data
   * @param dataNodeIndex - data node i which to inject - the index is same as for getDataNodes()
   * @param blocksToInject - the blocks
   * @throws IOException
   *              if not simulatedFSDataset
   *             if any of blocks already exist in the data node
   *   
   */
  public void injectBlocks(int dataNodeIndex, Block[] blocksToInject) throws IOException {
    if (dataNodeIndex < 0 || dataNodeIndex > dataNodes.size()) {
      throw new IndexOutOfBoundsException();
    }
    FSDatasetInterface dataSet = dataNodes.get(dataNodeIndex).datanode.getFSDataset();
    if (!(dataSet instanceof SimulatedFSDataset)) {
      throw new IOException("injectBlocks is valid only for SimilatedFSDataset");
    }
    SimulatedFSDataset sdataset = (SimulatedFSDataset) dataSet;
    sdataset.injectBlocks(getNameNode().getNamespaceID(), blocksToInject);
    dataNodes.get(dataNodeIndex).datanode.scheduleNSBlockReport(0);
  }
  
  /**
   * This method is valid only if the data nodes have simulated data
   * @param blocksToInject - blocksToInject[] is indexed in the same order as the list 
   *             of datanodes returned by getDataNodes()
   * @throws IOException
   *             if not simulatedFSDataset
   *             if any of blocks already exist in the data nodes
   *             Note the rest of the blocks are not injected.
   */
  public void injectBlocks(Block[][] blocksToInject) throws IOException {
    if (blocksToInject.length >  dataNodes.size()) {
      throw new IndexOutOfBoundsException();
    }
    for (int i = 0; i < blocksToInject.length; ++i) {
     injectBlocks(i, blocksToInject[i]);
    }
  }

  /**
   * Set the softLimit and hardLimit of client lease periods
   */
  void setLeasePeriod(long soft, long hard) {
    checkSingleNameNode();
    NameNode nn = getNameNode(0);
    nn.namesystem.leaseManager.setLeasePeriod(soft, hard);
    nn.namesystem.lmthread.interrupt();
  }

  /**
   * Returns the current set of datanodes
   */
  DataNode[] listDataNodes() {
    DataNode[] list = new DataNode[dataNodes.size()];
    for (int i = 0; i < dataNodes.size(); i++) {
      list[i] = dataNodes.get(i).datanode;
    }
    return list;
  }

  /**
   * Access to the data directory used for Datanodes
   * @throws IOException 
   */
  public String getDataDirectory() {
    return getDataDirectory(conf).getAbsolutePath();
  }
  
  public static File getDataDirectory(Configuration conf) {
    File base_dir = getBaseDirectory(conf);
    return new File(base_dir, "data");
  }

  private File getBaseDirectory() {
    return getBaseDirectory(conf);
  }

  public static File getBaseDirectory(Configuration conf) {
    return new File(System.getProperty("test.build.data", "build/test/data"),
        "dfs/" + conf.get(DFS_CLUSTER_ID, ""));
  }

  /**
   * Get the base data directory
   * @return the base data directory
   */
  public File getBaseDataDir() {
    return new File(getBaseDirectory(), "data");
  }
  
  private int getFreeSocketPort() {
    int port = 0;
    try {
      ServerSocket s = new ServerSocket(0);
      port = s.getLocalPort();
      s.close();
      return port;
    } catch (IOException e) {
      // Could not get a free port. Return default port 0.
    }
    return port;
  }
  
  private void setupDatanodeAddress(Configuration conf, boolean setupHostsFile) throws IOException {
    if (setupHostsFile) {
      String hostsFile = conf.get("dfs.hosts", "").trim();
      if (hostsFile.length() == 0) {
        throw new IOException("Parameter dfs.hosts is not setup in conf");
      }
      // Setup datanode in the include file, if it is defined in the conf
      String address = "127.0.0.1:" + getFreeSocketPort();
      conf.set("dfs.datanode.address", address);
      conf.set("dfs.datanode.http.address", "127.0.0.1:0");
      conf.set("dfs.datanode.ipc.address", "127.0.0.1:0");
      addToFile(hostsFile, address);
      System.out.println("Adding datanode " + address + " to hosts file " + hostsFile);
    } else {
      conf.set("dfs.datanode.address", "127.0.0.1:0");
      conf.set("dfs.datanode.http.address", "127.0.0.1:0");
      conf.set("dfs.datanode.ipc.address", "127.0.0.1:0");
    }
  }
  
  private void addToFile(String p, String address) throws IOException {
    File f = new File(p);
    if (!f.exists()) {
      f.createNewFile();
    }
    PrintWriter writer = new PrintWriter(new FileWriter(f, true));
    try {
      writer.println(address);
    } finally {
      writer.close();
    }
  }

  public ArrayList<DataNodeProperties> getDataNodeProperties() {
    return dataNodes;
  }

  /**
   * Get the directory for data node  
   * @return the base data directory
   */
  public File getBlockDirectory(String dirName) {
    checkSingleNameNode();
    int nsId = getNameNode(0).getNamespaceID();
    File curDataDir = new File(getBaseDataDir(), dirName + "/current/");
    return new File(NameSpaceSliceStorage.getNsRoot(
        nsId, curDataDir), FINALIZED_DIR_NAME);
  }
  
  /**
   * @return URI of the given namenode in MiniDFSCluster
   */
  public URI getURI(int nnIndex) {
    InetSocketAddress addr = nameNodes[nnIndex].nameNode.getNameNodeAddress();
    String hostPort = NameNode.getHostPortString(addr);
    URI uri = null;
    try {
      uri = new URI("hdfs://" + hostPort);
    } catch (URISyntaxException e) {
      NameNode.LOG.warn("unexpected URISyntaxException: " + e );
    }
    return uri;
  }

  
  /**
   * Add a namenode to cluster and start it. Configuration of datanodes
   * in the cluster is refreshed to register with the new namenode.
   * @return newly started namenode
   */
  public NameNode addNameNode(Configuration conf, int namenodePort)
      throws IOException {
    if(!federation) {
      throw new IOException("cannot add namenode to non-federated cluster");
    }
    int nnIndex = nameNodes.length;
    int numNameNodes = nameNodes.length + 1;
    NameNodeInfo[] newlist = new NameNodeInfo[numNameNodes];
    System.arraycopy(nameNodes, 0, newlist, 0, nameNodes.length);
    nameNodes = newlist;
    String nameserviceId = NAMESERVICE_ID_PREFIX + getNSId();
    String nameserviceIds = conf.get(FSConstants.DFS_FEDERATION_NAMESERVICES);
    nameserviceIds += "," + nameserviceId;
    conf.set(FSConstants.DFS_FEDERATION_NAMESERVICES, nameserviceIds);
    
    initFederatedNamenodeAddress(conf, nameserviceId, namenodePort); 
    createFederatedNameNode(nnIndex, conf, numDataNodes, true, true,
        null, nameserviceId);

    // Refresh datanodes with the newly started namenode
    for (DataNodeProperties dn : dataNodes) {
      DataNode datanode = dn.datanode;
      datanode.refreshNamenodes(conf);
    }

    // Wait for new namenode to get registrations from all the datanodes
    waitActive(true, nnIndex);
    return nameNodes[nnIndex].nameNode;
  }
  
  /**
   * Add another cluster to current cluster and start it. Configuration of datanodes
   * in the cluster is refreshed to register with the new namenodes;
   */
  public void addCluster(MiniDFSCluster cluster, boolean format)
      throws IOException, InterruptedException {
    if(!federation || !cluster.federation) {
      throw new IOException("Cannot handle non-federated cluster");
    }
    if (cluster.dataNodes.size() > this.dataNodes.size()) {
      throw new IOException("Cannot merge: new cluster has more datanodes the old one.");
    }
    LOG.info("Shutdown both clusters");
    this.shutdown(false);
    cluster.shutdown(false);
    this.numDataNodes = this.dataNodes.size();
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
    conf.set(FSConstants.DFS_FEDERATION_NAMESERVICES, nameserviceIds);
    int i;
    for (i = 0; i < nameNodes.length; i++) {
      NameNodeInfo nni = nameNodes[i];
      String nameserviceId = nni.conf.get(FSConstants.DFS_FEDERATION_NAMESERVICE_ID);
      initFederatedNamenodeAddress(nni.conf, nameserviceId, 0);
      if (i < nnIndex) {
        // Start with upgrade
        createFederatedNameNode(i, nni.conf, numDataNodes, false, format,
            StartupOption.UPGRADE, nameserviceId);
      } else {
        // Start with regular
        createFederatedNameNode(i, nni.conf, numDataNodes, false, format,
            null, nameserviceId);
      }
      for (int dnIndex = 0; dnIndex < dataNodes.size(); dnIndex++) {
        Configuration dstConf = dataNodes.get(dnIndex).conf;
        if (i >= nnIndex) {
          String dataStr = cluster.dataNodes.get(dnIndex).conf.get("dfs.data.dir");
          dstConf.set("dfs.merge.data.dir." + nameserviceId, dataStr);
        }
        String key = DFSUtil.getNameServiceIdKey(NameNode.DATANODE_PROTOCOL_ADDRESS, 
            nameserviceId);
        dstConf.set(key, nni.conf.get(key));
      }
    }
    //restart Datanode
    for (i = 0; i < dataNodes.size(); i++) {
      DataNodeProperties dn = dataNodes.get(i);
      dn.conf.set(FSConstants.DFS_FEDERATION_NAMESERVICES, nameserviceIds);
      dn.datanode = DataNode.createDataNode(dn.dnArgs, dn.conf);
    }
    waitClusterUp();
  }
  
  public int getNumNameNodes() {
    return nameNodes.length;
  }
  
  static public int getNSId() {
    return MiniDFSCluster.currNSId++;
  }
}
