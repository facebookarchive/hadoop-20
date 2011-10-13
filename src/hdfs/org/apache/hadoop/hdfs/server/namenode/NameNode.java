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

import org.apache.commons.logging.*;

import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.Trash;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.hdfs.HDFSPolicyProvider;
import org.apache.hadoop.hdfs.protocol.*;
import org.apache.hadoop.hdfs.server.common.HdfsConstants.StartupOption;
import org.apache.hadoop.hdfs.server.common.IncorrectVersionException;
import org.apache.hadoop.hdfs.server.common.UpgradeStatusReport;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem.CompleteFileStatus;
import org.apache.hadoop.hdfs.server.namenode.metrics.NameNodeMetrics;
import org.apache.hadoop.hdfs.server.protocol.BlockReport;
import org.apache.hadoop.hdfs.server.protocol.BlocksWithLocations;
import org.apache.hadoop.hdfs.server.protocol.DatanodeCommand;
import org.apache.hadoop.hdfs.server.protocol.DatanodeProtocol;
import org.apache.hadoop.hdfs.server.protocol.DatanodeRegistration;
import org.apache.hadoop.hdfs.server.protocol.NamenodeProtocol;
import org.apache.hadoop.hdfs.server.protocol.NamespaceInfo;
import org.apache.hadoop.hdfs.server.protocol.ReceivedDeletedBlockInfo;
import org.apache.hadoop.hdfs.server.protocol.UpgradeCommand;
import org.apache.hadoop.http.HttpServer;
import org.apache.hadoop.ipc.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.net.NetworkTopology;
import org.apache.hadoop.net.Node;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.AuthorizationException;
import org.apache.hadoop.security.authorize.ConfiguredPolicy;
import org.apache.hadoop.security.authorize.PolicyProvider;
import org.apache.hadoop.security.authorize.RefreshAuthorizationPolicyProtocol;
import org.apache.hadoop.security.authorize.ServiceAuthorizationManager;

import java.io.*;
import java.net.*;
import java.util.Collection;
import java.util.Iterator;
import java.util.Set;
import java.util.HashSet;
import java.util.List;
import java.util.ArrayList;
import java.util.Arrays;

/**********************************************************
 * NameNode serves as both directory namespace manager and
 * "inode table" for the Hadoop DFS.  There is a single NameNode
 * running in any DFS deployment.  (Well, except when there
 * is a second backup/failover NameNode.)
 *
 * The NameNode controls two critical tables:
 *   1)  filename->blocksequence (namespace)
 *   2)  block->machinelist ("inodes")
 *
 * The first table is stored on disk and is very precious.
 * The second table is rebuilt every time the NameNode comes
 * up.
 *
 * 'NameNode' refers to both this class as well as the 'NameNode server'.
 * The 'FSNamesystem' class actually performs most of the filesystem
 * management.  The majority of the 'NameNode' class itself is concerned
 * with exposing the IPC interface and the http server to the outside world,
 * plus some configuration management.
 *
 * NameNode implements the ClientProtocol interface, which allows
 * clients to ask for DFS services.  ClientProtocol is not
 * designed for direct use by authors of DFS client code.  End-users
 * should instead use the org.apache.nutch.hadoop.fs.FileSystem class.
 *
 * NameNode also implements the DatanodeProtocol interface, used by
 * DataNode programs that actually store DFS data blocks.  These
 * methods are invoked repeatedly and automatically by all the
 * DataNodes in a DFS deployment.
 *
 * NameNode also implements the NamenodeProtocol interface, used by
 * secondary namenodes or rebalancing processes to get partial namenode's
 * state, for example partial blocksMap etc.
 **********************************************************/
public class NameNode extends ReconfigurableBase
  implements ClientProtocol, DatanodeProtocol,
             NamenodeProtocol, FSConstants,
             RefreshAuthorizationPolicyProtocol {
  static{
    Configuration.addDefaultResource("hdfs-default.xml");
    Configuration.addDefaultResource("hdfs-site.xml");
  }
  
  private static final String CONF_SERVLET_PATH = "/nnconfchange";

  public long getProtocolVersion(String protocol, 
                                 long clientVersion) throws IOException {
    InetSocketAddress requestAddress = Server.get().getListenerAddress();
    boolean dnRequest = false, clientRequest = false;
    // If dnProtocolAddress is null - there is only one server running
    // otherwise check the address of the incoming request.
    if (dnProtocolAddress == null ||
        dnProtocolAddress.equals(requestAddress)) {
      dnRequest = true;
    }
    if (dnProtocolAddress == null || requestAddress.equals(serverAddress)) {
      clientRequest = true;
    }
    if (protocol.equals(ClientProtocol.class.getName())) {
      long namenodeVersion = ClientProtocol.versionID;
      if (namenodeVersion > clientVersion && 
          !ProtocolCompatible.isCompatibleClientProtocol(
              clientVersion, namenodeVersion)) {
        throw new RPC.VersionIncompatible(
            protocol, clientVersion, namenodeVersion);
      }
      return namenodeVersion; 
    } else if (protocol.equals(DatanodeProtocol.class.getName()) && dnRequest){
      return DatanodeProtocol.versionID;
    } else if (protocol.equals(NamenodeProtocol.class.getName()) && clientRequest){
      return NamenodeProtocol.versionID;
    } else if (protocol.equals(RefreshAuthorizationPolicyProtocol.class.getName()) && clientRequest){
      return RefreshAuthorizationPolicyProtocol.versionID;
    } else {
      throw new IOException("Unknown protocol to name node: " + protocol);
    }
  }

  @Override
  public ProtocolSignature getProtocolSignature(String protocol,
      long clientVersion, int clientMethodsHash) throws IOException {
    return ProtocolSignature.getProtocolSignature(
        this, protocol, clientVersion, clientMethodsHash);
  }

  // The number of handlers to be used to process datanode requests
  public static final String DATANODE_PROTOCOL_HANDLERS =
                                "dfs.namenode.dn-handlers";
  // The address where the server processing datanode requests is running
  public static final String DATANODE_PROTOCOL_ADDRESS =
                                "dfs.namenode.dn-address";
  public static final int DEFAULT_PORT = 8020;

  public static final Log LOG = LogFactory.getLog(NameNode.class.getName());
  public static final Log stateChangeLog = LogFactory.getLog("org.apache.hadoop.hdfs.StateChange");
  public FSNamesystem namesystem; // TODO: This should private. Use getNamesystem() instead. 
  /** RPC server */
  private Server server;
  /** RPC server for datanodes */
  private Server dnProtocolServer;
  /** RPC server address */
  private InetSocketAddress serverAddress = null;
  /** RPC server for datanodes address */
  private InetSocketAddress dnProtocolAddress = null;
  /** httpServer */
  private HttpServer httpServer;
  /** HTTP server address */
  private InetSocketAddress httpAddress = null;
  private Thread emptier;
  /** only used for testing purposes  */
  private boolean stopRequested = false;
  /** Is service level authorization enabled? */
  private boolean serviceAuthEnabled = false;
  
  /** Format a new filesystem.  Destroys any filesystem that may already
   * exist at this location.  **/
  public static void format(Configuration conf) throws IOException {
    format(conf, false);
  }

  static NameNodeMetrics myMetrics;

  public FSNamesystem getNamesystem() {
    return namesystem;
  }

  public static NameNodeMetrics getNameNodeMetrics() {
    return myMetrics;
  }
  
  public static InetSocketAddress getAddress(String address) {
    return NetUtils.createSocketAddr(address, DEFAULT_PORT);
  }
  
  /**
   * Set the datanode server address property in the configuration
   * @param conf Configuration to modify
   * @param address the address in the form "hostname:port"
   */
  public static void setDNProtocolAddress(Configuration conf, String address) {
    conf.set(DATANODE_PROTOCOL_ADDRESS, address);
  }
  
  /**
   * Get the configured address of the datanode to run the RPC server
   * processing requests from datanodes. Returns the address if it is
   * configured, otherwise will return null.
   * 
   * @param conf
   * @return the address object or null if it is not configured
   */
  public static InetSocketAddress getDNProtocolAddress(Configuration conf) {
    String dnAddressString = conf.get(DATANODE_PROTOCOL_ADDRESS);
    if (dnAddressString == null || dnAddressString.isEmpty())
      return null;
    return getAddress(dnAddressString);
  }

  public static InetSocketAddress getAddress(Configuration conf) {
    return getAddress(FileSystem.getDefaultUri(conf).getAuthority());
  }

  public static URI getUri(InetSocketAddress namenode) {
    int port = namenode.getPort();
    String portString = port == DEFAULT_PORT ? "" : (":"+port);
    return URI.create("hdfs://"+ namenode.getHostName()+portString);
  }

  /**
   * Initialize name-node.
   * 
   */
  private void initialize() throws IOException {
    // set service-level authorization security policy
    if (serviceAuthEnabled =
        getConf().getBoolean(
            ServiceAuthorizationManager.SERVICE_AUTHORIZATION_CONFIG, false)) {
      PolicyProvider policyProvider = 
        (PolicyProvider)(ReflectionUtils.newInstance(
            getConf().getClass(PolicyProvider.POLICY_PROVIDER_CONFIG,
                HDFSPolicyProvider.class, PolicyProvider.class), 
            getConf()));
      SecurityUtil.setPolicy(new ConfiguredPolicy(getConf(), policyProvider));
    }

    // This is a check that the port is free
    // create a socket and bind to it, throw exception if port is busy
    // This has to be done before we are reading Namesystem not to waste time and fail fast
    InetSocketAddress clientSocket = NameNode.getAddress(getConf());
    ServerSocket socket = new ServerSocket();
    socket.bind(clientSocket);
    socket.close();
    InetSocketAddress dnSocket = NameNode.getDNProtocolAddress(getConf());
    if (dnSocket != null) {
      socket = new ServerSocket();
      socket.bind(dnSocket);
      socket.close();
      //System.err.println("Tested " + dnSocket);
    }
    
    
    myMetrics = new NameNodeMetrics(getConf(), this);

    this.namesystem = new FSNamesystem(this, getConf());
    // HACK: from removal of FSNamesystem.getFSNamesystem().
    JspHelper.fsn = this.namesystem;

    this.startDNServer();
    startHttpServer(getConf());
  }

  private static FileSystem getTrashFileSystem(Configuration conf) throws IOException {
    InetSocketAddress serviceAddress = NameNode.getDNProtocolAddress(conf);
    if (serviceAddress != null) {
      URI defaultUri = FileSystem.getDefaultUri(conf);
      URI serviceUri = null;
      try {
        serviceUri = new URI(defaultUri.getScheme(), defaultUri.getUserInfo(),
            serviceAddress.getHostName(), serviceAddress.getPort(),
            defaultUri.getPath(), defaultUri.getQuery(),
            defaultUri.getFragment());
      } catch (URISyntaxException uex) {
        throw new IOException("Failed to initialize a uri for trash FS");
      }
      Path trashFsPath = new Path(serviceUri.toString());
      return trashFsPath.getFileSystem(conf);
    } else {
      return FileSystem.get(conf);
    }
  }

  private void startTrashEmptier(Configuration conf) throws IOException {
    if (conf.getInt("fs.trash.interval", 0) == 0) {
      return;
    }
    FileSystem fs = NameNode.getTrashFileSystem(conf);
    this.emptier = new Thread(new Trash(fs, conf).getEmptier(), "Trash Emptier");
    this.emptier.setDaemon(true);
    this.emptier.start();
  }

  private void startHttpServer(Configuration conf) throws IOException {
    String infoAddr = 
      NetUtils.getServerAddress(conf, "dfs.info.bindAddress", 
                                "dfs.info.port", "dfs.http.address");
    InetSocketAddress infoSocAddr = NetUtils.createSocketAddr(infoAddr);
    String infoHost = infoSocAddr.getHostName();
    int infoPort = infoSocAddr.getPort();
    this.httpServer = new HttpServer("hdfs", infoHost, infoPort, 
        infoPort == 0, conf);
    if (conf.getBoolean("dfs.https.enable", false)) {
      boolean needClientAuth = conf.getBoolean("dfs.https.need.client.auth", false);
      InetSocketAddress secInfoSocAddr = NetUtils.createSocketAddr(conf.get(
          "dfs.https.address", infoHost + ":" + 0));
      Configuration sslConf = new Configuration(false);
      sslConf.addResource(conf.get("dfs.https.server.keystore.resource",
          "ssl-server.xml"));
      this.httpServer.addSslListener(secInfoSocAddr, sslConf, needClientAuth);
      // assume same ssl port for all datanodes
      InetSocketAddress datanodeSslPort = NetUtils.createSocketAddr(conf.get(
          "dfs.datanode.https.address", infoHost + ":" + 50475));
      this.httpServer.setAttribute("datanode.https.port", datanodeSslPort
          .getPort());
    }
    this.httpServer.setAttribute("name.node", this);
    this.httpServer.setAttribute("name.node.address", getNameNodeAddress());
    this.httpServer.setAttribute("name.system.image", getFSImage());
    this.httpServer.setAttribute("name.conf", conf);
    this.httpServer.addInternalServlet("fsck", "/fsck", FsckServlet.class);
    this.httpServer.addInternalServlet("getimage", "/getimage", GetImageServlet.class);
    this.httpServer.addInternalServlet("listPaths", "/listPaths/*", ListPathsServlet.class);
    this.httpServer.addInternalServlet("data", "/data/*", FileDataServlet.class);
    this.httpServer.addInternalServlet("checksum", "/fileChecksum/*",
        FileChecksumServlets.RedirectServlet.class);
    httpServer.setAttribute(ReconfigurationServlet.
                            CONF_SERVLET_RECONFIGURABLE_PREFIX +
                            CONF_SERVLET_PATH, NameNode.this);
    httpServer.addInternalServlet("nnconfchange", CONF_SERVLET_PATH,
                                  ReconfigurationServlet.class);

    this.httpServer.start();

    // The web-server port can be ephemeral... ensure we have the correct info
    infoPort = this.httpServer.getPort();
    this.httpAddress = new InetSocketAddress(infoHost, infoPort);
    conf.set("dfs.http.address", infoHost + ":" + infoPort);
    LOG.info("Web-server up at: " + infoHost + ":" + infoPort);
  }

  /**
   * Start NameNode.
   * <p>
   * The name-node can be started with one of the following startup options:
   * <ul> 
   * <li>{@link StartupOption#REGULAR REGULAR} - normal name node startup</li>
   * <li>{@link StartupOption#FORMAT FORMAT} - format name node</li>
   * <li>{@link StartupOption#UPGRADE UPGRADE} - start the cluster  
   * upgrade and create a snapshot of the current file system state</li> 
   * <li>{@link StartupOption#ROLLBACK ROLLBACK} - roll the  
   *            cluster back to the previous state</li>
   * </ul>
   * The option is passed via configuration field: 
   * <tt>dfs.namenode.startup</tt>
   * 
   * The conf will be modified to reflect the actual ports on which 
   * the NameNode is up and running if the user passes the port as
   * <code>zero</code> in the conf.
   * 
   * @param conf  confirguration
   * @throws IOException
   */
  public NameNode(Configuration conf) throws IOException {
    super(conf);
    try {
      initialize();
    } catch (IOException e) {
      this.stop();
      throw e;
    }
  }

  /**
   * Wait for service to finish.
   * (Normally, it runs forever.)
   */
  public void join() {
    try {
      if (this.dnProtocolServer != null) {
        this.dnProtocolServer.join();
      }
      if (this.server != null) {
        this.server.join();
      }
    } catch (InterruptedException ie) {
    }
  }
  
  public void startServerForClientRequests() throws IOException {
    if (this.server == null) {
      InetSocketAddress socAddr = NameNode.getAddress(getConf());
      int handlerCount = getConf().getInt("dfs.namenode.handler.count", 10); 
      
      // create rpc server 
      this.server = RPC.getServer(this, socAddr.getHostName(), socAddr.getPort(),
                                  handlerCount, false, getConf());
      // The rpc-server port can be ephemeral... ensure we have the correct info
      this.serverAddress = this.server.getListenerAddress();
      FileSystem.setDefaultUri(getConf(), getUri(serverAddress));
      if (this.httpServer != null) {
        // This means the server is being started once out of safemode
        // and jetty is initialized already
        this.httpServer.setAttribute("name.node.address", getNameNodeAddress());
      }
      LOG.info("Namenode up at: " + this.serverAddress);
      
      
      this.server.start();
    }
  }
  
  public void startDNServer() throws IOException {
    InetSocketAddress dnAddr = NameNode.getDNProtocolAddress(getConf());
    int handlerCount = getConf().getInt("dfs.namenode.handler.count", 10);
    
    if (dnAddr != null) {
      int dnHandlerCount =
        getConf().getInt(DATANODE_PROTOCOL_HANDLERS, handlerCount);
      this.dnProtocolServer = RPC.getServer(this, dnAddr.getHostName(),
                                            dnAddr.getPort(), dnHandlerCount,
                                            false, getConf());
      this.dnProtocolAddress = dnProtocolServer.getListenerAddress();
      NameNode.setDNProtocolAddress(getConf(), 
          dnProtocolAddress.getHostName() + ":" + dnProtocolAddress.getPort());
      LOG.info("Datanodes endpoint is up at: " + this.dnProtocolAddress);
    }
    
    if (this.dnProtocolServer != null) {
      this.dnProtocolServer.start();

    } else {
      this.startServerForClientRequests();
    }
    startTrashEmptier(getConf()); 
  }

  /**
   * Stop all NameNode threads and wait for all to finish.
   */
  public void stop() {
    if (stopRequested)
      return;
    stopRequested = true;
    try {
      if (httpServer != null) httpServer.stop();
    } catch (Exception e) {
      LOG.error(StringUtils.stringifyException(e));
    }
    if(namesystem != null) namesystem.close();
    if(emptier != null) emptier.interrupt();
    if(server != null) server.stop();
    if (dnProtocolServer != null) dnProtocolServer.stop();
    if (myMetrics != null) {
      myMetrics.shutdown();
    }
    if (namesystem != null) {
      namesystem.shutdown();
    }
  }
  
  /////////////////////////////////////////////////////
  // NamenodeProtocol
  /////////////////////////////////////////////////////
  /**
   * return a list of blocks & their locations on <code>datanode</code> whose
   * total size is <code>size</code>
   * 
   * @param datanode on which blocks are located
   * @param size total size of blocks
   */
  public BlocksWithLocations getBlocks(DatanodeInfo datanode, long size)
  throws IOException {
    if(size <= 0) {
      throw new IllegalArgumentException(
        "Unexpected not positive size: "+size);
    }

    return namesystem.getBlocks(datanode, size); 
  }

  public long[] getBlockLengths(long[] blockIds) {
    long[] lengths = new long[blockIds.length];

    for (int i = 0; i < blockIds.length; i++) {
      Block block = namesystem.getBlockInfo(new Block(blockIds[i]));
      if (block == null) {
        lengths[i] = -1; // length could not be found
      } else {
        lengths[i] = block.getNumBytes();
      }
    }
    return lengths;
  }

  public CheckpointSignature getCheckpointSignature() {
    return new CheckpointSignature(namesystem.dir.fsImage);
  }

  public LocatedBlocks updateDatanodeInfo(LocatedBlocks locatedBlocks) throws IOException {
    return namesystem.updateDatanodeInfo(locatedBlocks);
  }
  
  /////////////////////////////////////////////////////
  // ClientProtocol
  /////////////////////////////////////////////////////

  /** {@inheritDoc} */
  public LocatedBlocks   getBlockLocations(String src, 
                                          long offset, 
                                          long length) throws IOException {
    myMetrics.numGetBlockLocations.inc();
    return namesystem.getBlockLocations(getClientMachine(), 
                                        src, offset, length, false);
  }
  
  public VersionedLocatedBlocks open(String src, 
                                     long offset, 
                                     long length) throws IOException {
    myMetrics.numGetBlockLocations.inc();
    return (VersionedLocatedBlocks)namesystem.getBlockLocations(getClientMachine(), 
                           src, offset, length, true);
  }

  private static String getClientMachine() {
    String clientMachine = Server.getRemoteAddress();
    if (clientMachine == null) {
      clientMachine = "";
    }
    return clientMachine;
  }

  @Deprecated
  public void create(String src, 
                     FsPermission masked,
                             String clientName, 
                             boolean overwrite,
                             short replication,
                             long blockSize
                             ) throws IOException {
    create(src,masked,clientName,overwrite,true,replication,blockSize);
  }

  /** {@inheritDoc} */
  public void create(String src, 
                     FsPermission masked,
                             String clientName, 
                             boolean overwrite,
                             boolean createParent,
                             short replication,
                             long blockSize
                             ) throws IOException {
    String clientMachine = getClientMachine();
    if (stateChangeLog.isDebugEnabled()) {
      stateChangeLog.debug("*DIR* NameNode.create: file "
                         +src+" for "+clientName+" at "+clientMachine);
    }
    if (!checkPathLength(src)) {
      throw new IOException("create: Pathname too long.  Limit " 
                            + MAX_PATH_LENGTH + " characters, " + MAX_PATH_DEPTH + " levels.");
    }
    namesystem.startFile(src,
        new PermissionStatus(UserGroupInformation.getCurrentUGI().getUserName(),
            null, masked),
        clientName, clientMachine, overwrite, createParent, replication, blockSize);
    myMetrics.numFilesCreated.inc();
    myMetrics.numCreateFileOps.inc();
  }

  /** {@inheritDoc} */
  public LocatedBlock append(String src, String clientName) throws IOException {
    String clientMachine = getClientMachine();
    if (stateChangeLog.isDebugEnabled()) {
      stateChangeLog.debug("*DIR* NameNode.append: file "
          +src+" for "+clientName+" at "+clientMachine);
    }
    LocatedBlock info = namesystem.appendFile(src, clientName, clientMachine);
    myMetrics.numFilesAppended.inc();
    return info;
  }

  /** {@inheritDoc} */
  public void recoverLease(String src, String clientName) throws IOException {
    String clientMachine = getClientMachine();
    namesystem.recoverLease(src, clientName, clientMachine, false);
  }

  @Override
  public boolean closeRecoverLease(String src, String clientName)
    throws IOException {
    return closeRecoverLease(src, clientName, Boolean.valueOf(false));
  }

  @Override
  public boolean closeRecoverLease(String src, String clientName,
                                   boolean discardLastBlock) throws IOException {
    String clientMachine = getClientMachine();
    return namesystem.recoverLease(src, clientName, clientMachine,
                                   discardLastBlock);
  }

  /** {@inheritDoc} */
  public boolean setReplication(String src,
                                short replication
                                ) throws IOException {
    boolean value = namesystem.setReplication(src, replication);
    if (value) {
      myMetrics.numSetReplication.inc();
    }
    return value;
  }
    
  /** {@inheritDoc} */
  public void setPermission(String src, FsPermission permissions
      ) throws IOException {
    namesystem.setPermission(src, permissions);
    myMetrics.numSetPermission.inc();
  }

  /** {@inheritDoc} */
  public void setOwner(String src, String username, String groupname
      ) throws IOException {
    namesystem.setOwner(src, username, groupname);
    myMetrics.numSetOwner.inc();
  }

  /**
   * Stub for 0.20 clients that don't support HDFS-630
   */
  public LocatedBlock addBlock(String src, 
                               String clientName) throws IOException {
    return addBlock(src, clientName, null);
  }

  @Override
  public LocatedBlock addBlock(String src, String clientName,
      DatanodeInfo[] excludedNodes) throws IOException {
    return addBlock(src, clientName, excludedNodes, null, true);
  }

  @Override
  public VersionedLocatedBlock addBlockAndFetchVersion(String src, String clientName,
      DatanodeInfo[] excludedNodes) throws IOException {
    return (VersionedLocatedBlock)addBlock(src, clientName, excludedNodes, null, true, true);
  }

  @Override
  public LocatedBlock addBlock(String src, String clientName,
      DatanodeInfo[] excludedNodes, DatanodeInfo[] favoredNodes, boolean wait)
  throws IOException {
    return addBlock(src, clientName, excludedNodes, favoredNodes,
        wait, false);
  }
  
  public LocatedBlock addBlock(String src,
        String clientName,
        DatanodeInfo[] excludedNodes,
        DatanodeInfo[] favoredNodes,
        boolean wait,
        boolean needVersion)
    throws IOException {
    List<Node> excludedNodeList = null;
    if (excludedNodes != null) {
      // We must copy here, since this list gets modified later on
      // in ReplicationTargetChooser
      excludedNodeList = new ArrayList<Node>(
        Arrays.<Node>asList(excludedNodes));
    }

    stateChangeLog.debug("*BLOCK* NameNode.addBlock: file "
                         +src+" for "+clientName);
    List<DatanodeInfo> favoredNodesList = (favoredNodes == null) ? null
        : Arrays.asList(favoredNodes);
    LocatedBlock locatedBlock = namesystem.getAdditionalBlock(src, clientName,
        excludedNodeList, favoredNodesList, wait, needVersion);
    if (locatedBlock != null)
      myMetrics.numAddBlockOps.inc();
    return locatedBlock;
  }

  /**
   * The client needs to give up on the block.
   */
  public void abandonBlock(Block b, String src, String holder
      ) throws IOException {
    stateChangeLog.debug("*BLOCK* NameNode.abandonBlock: "
                         +b+" of file "+src);
    if (!namesystem.abandonBlock(b, src, holder)) {
      throw new IOException("Cannot abandon block during write to " + src);
    }
    myMetrics.numAbandonBlock.inc();
  }

  @Override
  public void abandonFile(String src, String holder) throws IOException {
    stateChangeLog.debug("*FILE* NameNode.abandonFile: " + src);
    if (!namesystem.abandonFile(src, holder)) {
      throw new IOException("Cannot abandon write to file " + src);
    }
  }

  /** {@inheritDoc} */
  public boolean complete(String src, String clientName) throws IOException {
    stateChangeLog.debug("*DIR* NameNode.complete: " + src + " for " + clientName);
    CompleteFileStatus returnCode = namesystem.completeFile(src, clientName);
    if (returnCode == CompleteFileStatus.STILL_WAITING) {
      return false;
    } else if (returnCode == CompleteFileStatus.COMPLETE_SUCCESS) {
      myMetrics.numCompleteFile.inc();
      return true;
    } else {
      throw new IOException("Could not complete write to file " + src + " by " + clientName);
    }
  }

  /**
   * The client has detected an error on the specified located blocks 
   * and is reporting them to the server.  For now, the namenode will 
   * mark the block as corrupt.  In the future we might 
   * check the blocks are actually corrupt. 
   */
  public void reportBadBlocks(LocatedBlock[] blocks) throws IOException {
    stateChangeLog.info("*DIR* NameNode.reportBadBlocks");
    myMetrics.numReportBadBlocks.inc();
    for (int i = 0; i < blocks.length; i++) {
      Block blk = blocks[i].getBlock();
      DatanodeInfo[] nodes = blocks[i].getLocations();
      for (int j = 0; j < nodes.length; j++) {
        myMetrics.numReportedCorruptReplicas.inc();
        DatanodeInfo dn = nodes[j];
        namesystem.markBlockAsCorrupt(blk, dn);
      }
    }
  }

  /** {@inheritDoc} */
  public long nextGenerationStamp(Block block, boolean fromNN) throws IOException{
    myMetrics.numNextGenerationStamp.inc();
    return namesystem.nextGenerationStampForBlock(block, fromNN);
  }

  /** {@inheritDoc} */
  public void commitBlockSynchronization(Block block,
      long newgenerationstamp, long newlength,
      boolean closeFile, boolean deleteblock, DatanodeID[] newtargets
      ) throws IOException {
    namesystem.commitBlockSynchronization(block,
        newgenerationstamp, newlength, closeFile, deleteblock, newtargets);
  }
  
  public long getPreferredBlockSize(String filename) throws IOException {
    return namesystem.getPreferredBlockSize(filename);
  }

  /** 
   * {@inheritDoc}
   */
  @Deprecated
  public void concat(String trg, String[] src)
      throws IOException {
    concat(trg, src, true);
  }
  
  /** 
   * {@inheritDoc}
   */
  public void concat(String trg, String[] src, boolean restricted)
      throws IOException {
    namesystem.concat(trg, src, restricted);
  }
    
  /**
   */
  public boolean rename(String src, String dst) throws IOException {
    stateChangeLog.debug("*DIR* NameNode.rename: " + src + " to " + dst);
    if (!checkPathLength(dst)) {
      throw new IOException("rename: Pathname too long.  Limit " 
                            + MAX_PATH_LENGTH + " characters, " + MAX_PATH_DEPTH + " levels.");
    }
    boolean ret = namesystem.renameTo(src, dst);
    if (ret) {
      myMetrics.numFilesRenamed.inc();
    }
    return ret;
  }

  /**
   */
  @Deprecated
  public boolean delete(String src) throws IOException {
    return delete(src, true);
  }

  /** {@inheritDoc} */
  public boolean delete(String src, boolean recursive) throws IOException {
    if (stateChangeLog.isDebugEnabled()) {
      stateChangeLog.debug("*DIR* Namenode.delete: src=" + src
          + ", recursive=" + recursive);
    }
    boolean ret = namesystem.delete(src, recursive);
    if (ret) 
      myMetrics.numDeleteFileOps.inc();
    return ret;
  }

  /**
   * Check path length does not exceed maximum.  Returns true if
   * length and depth are okay.  Returns false if length is too long 
   * or depth is too great.
   * 
   */
  private boolean checkPathLength(String src) {
    Path srcPath = new Path(src);
    return (src.length() <= MAX_PATH_LENGTH &&
            srcPath.depth() <= MAX_PATH_DEPTH);
  }
    
  /** {@inheritDoc} */
  public boolean mkdirs(String src, FsPermission masked) throws IOException {
    stateChangeLog.debug("*DIR* NameNode.mkdirs: " + src);
    if (!checkPathLength(src)) {
      throw new IOException("mkdirs: Pathname too long.  Limit " 
                            + MAX_PATH_LENGTH + " characters, " + MAX_PATH_DEPTH + " levels.");
    }
    boolean value =  namesystem.mkdirs(src,
        new PermissionStatus(UserGroupInformation.getCurrentUGI().getUserName(),
            null, masked));
    if (value) {
      myMetrics.numMkdirs.inc();
    }
    return value;
  }

  /**
   */
  public void renewLease(String clientName) throws IOException {
    myMetrics.numRenewLease.inc();
    namesystem.renewLease(clientName);        
  }

  /**
   */
  public FileStatus[] getListing(String src) throws IOException {
    FileStatus[] files = namesystem.getListing(src);
    if (files != null) {
      myMetrics.numGetListingOps.inc();
    }
    return files;
  }

  @Override
  public HdfsFileStatus[] getHdfsListing(String src) throws IOException {
    HdfsFileStatus[] files = namesystem.getHdfsListing(src);
    if (files != null) {
      myMetrics.numGetListingOps.inc();
    }
    return files;
  }

  @Override
  public DirectoryListing getPartialListing(String src, byte[] startAfter)
  throws IOException {
    DirectoryListing files = namesystem.getPartialListing(
        src, startAfter, false);
    if (files != null) {
      myMetrics.numGetListingOps.inc();
    }
    return files;
  }

  @Override
  public LocatedDirectoryListing getLocatedPartialListing(
      String src, byte[] startAfter)
  throws IOException {
    DirectoryListing files = namesystem.getPartialListing(
        src, startAfter, true);
    if (files != null) {
      myMetrics.numGetListingOps.inc();
    }
    return (LocatedDirectoryListing)files;
  }
  
  /**
   * Get the file info for a specific file.
   * @param src The string representation of the path to the file
   * @throws IOException if permission to access file is denied by the system
   * @return object containing information regarding the file
   *         or null if file not found
   */
  public FileStatus getFileInfo(String src)  throws IOException {
    myMetrics.numFileInfoOps.inc();
    return namesystem.getFileInfo(src);
  }

  @Override
  public HdfsFileStatus getHdfsFileInfo(String src)  throws IOException {
    HdfsFileStatus value = namesystem.getHdfsFileInfo(src);
    myMetrics.numFileInfoOps.inc();
    return value;
  }

  /** @inheritDoc */
  public long[] getStats() throws IOException {
    return namesystem.getStats();
  }

  /**
   */
  public DatanodeInfo[] getDatanodeReport(DatanodeReportType type)
  throws IOException {
    DatanodeInfo results[] = namesystem.datanodeReport(type);
    if (results == null ) {
      throw new IOException("Cannot find datanode report");
    }
    return results;
  }
    
  /**
   * @inheritDoc
   */
  public boolean setSafeMode(SafeModeAction action) throws IOException {
    return namesystem.setSafeMode(action);
  }

  /**
   * Is the cluster currently in safe mode?
   */
  public boolean isInSafeMode() {
    return namesystem.isInSafeMode();
  }

  /**
   * @inheritDoc
   */
  public void saveNamespace() throws IOException {
    saveNamespace(false, false);
  }

  @Override
  public void saveNamespace(boolean force, boolean uncompressed)
  throws IOException {
    namesystem.saveNamespace(force, uncompressed);
    myMetrics.numSaveNamespace.inc();
  }

  /**
   * Refresh the list of datanodes that the namenode should allow to  
   * connect.  Re-reads conf by creating new Configuration object and 
   * uses the files list in the configuration to update the list. 
   */
  public void refreshNodes() throws IOException {
    namesystem.refreshNodes(new Configuration());
    myMetrics.numRefreshNodes.inc();
  }

  /**
   * Returns the size of the current edit log.
   */
  public long getEditLogSize() throws IOException {
    return namesystem.getEditLogSize();
  }

  /**
   * Roll the edit log.
   */
  public CheckpointSignature rollEditLog() throws IOException {
    return namesystem.rollEditLog();
  }

  /**
   * Roll the image 
   */
  @Override
  public void rollFsImage(CheckpointSignature newImageSignature) throws IOException {
    namesystem.rollFSImage(newImageSignature);
  }
    
  public void finalizeUpgrade() throws IOException {
    namesystem.finalizeUpgrade();
  }

  public UpgradeStatusReport distributedUpgradeProgress(UpgradeAction action
                                                        ) throws IOException {
    return namesystem.distributedUpgradeProgress(action);
  }

  /**
   * Dumps namenode state into specified file
   */
  public void metaSave(String filename) throws IOException {
    namesystem.metaSave(filename);
  }

  /**
   * {@inheritDoc}
   *
   * implement old API for backwards compatibility
   */
  @Override @Deprecated
  public FileStatus[] getCorruptFiles() throws IOException {
    CorruptFileBlocks corruptFileBlocks = listCorruptFileBlocks("/", null);
    Set<String> filePaths = new HashSet<String>();
    for (String file : corruptFileBlocks.getFiles()) {
      filePaths.add(file);
    }
    
    List<FileStatus> fileStatuses = new ArrayList<FileStatus>(filePaths.size());
    for (String f: filePaths) {
      FileStatus fs = getFileInfo(f);
      if (fs != null) 
        LOG.info("found fs for " + f);
      else
        LOG.info("found no fs for " + f);
      fileStatuses.add(fs);
    }
    return fileStatuses.toArray(new FileStatus[fileStatuses.size()]);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public CorruptFileBlocks
    listCorruptFileBlocks(String path,
                          String cookie) 
    throws IOException {
    
    String[] cookieTab = new String[] { cookie };
    Collection<FSNamesystem.CorruptFileBlockInfo> fbs =
      namesystem.listCorruptFileBlocks(path, cookieTab);

    String[] files = new String[fbs.size()];
    int i = 0;
    for(FSNamesystem.CorruptFileBlockInfo fb: fbs) {
      files[i++] = fb.path;
    }
    return new CorruptFileBlocks(files, cookieTab[0]);
  }

/** {@inheritDoc} */
  public ContentSummary getContentSummary(String path) throws IOException {
    return namesystem.getContentSummary(path);
  }

  /** {@inheritDoc} */
  public void setQuota(String path, long namespaceQuota, long diskspaceQuota) 
                       throws IOException {
    namesystem.setQuota(path, namespaceQuota, diskspaceQuota);
    myMetrics.numSetQuota.inc();
  }
  
  /** {@inheritDoc} */
  public void fsync(String src, String clientName) throws IOException {
    namesystem.fsync(src, clientName);
    myMetrics.numFsync.inc();
  }

  /** @inheritDoc */
  public void setTimes(String src, long mtime, long atime) throws IOException {
    namesystem.setTimes(src, mtime, atime);
    myMetrics.numSetTimes.inc();
  }

  ////////////////////////////////////////////////////////////////
  // DatanodeProtocol
  ////////////////////////////////////////////////////////////////
  /** 
   */
  public DatanodeRegistration register(DatanodeRegistration nodeReg
                                       ) throws IOException {
    verifyVersion(nodeReg.getVersion());
    namesystem.registerDatanode(nodeReg);
    myMetrics.numRegister.inc();
      
    return nodeReg;
  }

  public void keepAlive(DatanodeRegistration nodeReg) throws IOException {
    namesystem.handleKeepAlive(nodeReg);
  }

  /**
   * Data node notify the name node that it is alive 
   * Return an array of block-oriented commands for the datanode to execute.
   * This will be either a transfer or a delete operation.
   */
  public DatanodeCommand[] sendHeartbeat(DatanodeRegistration nodeReg,
                                       long capacity,
                                       long dfsUsed,
                                       long remaining,
                                       int xmitsInProgress,
                                       int xceiverCount) throws IOException {
    verifyRequest(nodeReg);
    myMetrics.numHeartbeat.inc();
    return namesystem.handleHeartbeat(nodeReg, capacity, dfsUsed, remaining,
        xceiverCount, xmitsInProgress);
  }
  
  /**
  * add new replica blocks to the Inode to target mapping
  * also add the Inode file to DataNodeDesc
  */
  public void blocksBeingWrittenReport(DatanodeRegistration nodeReg,
      BlockReport blocks) throws IOException {
    verifyRequest(nodeReg);
    long[] blocksAsLong = blocks.getBlockReportInLongs();
    BlockListAsLongs blist = new BlockListAsLongs(blocksAsLong);
    namesystem.processBlocksBeingWrittenReport(nodeReg, blist);
        
    stateChangeLog.info("*BLOCK* NameNode.blocksBeingWrittenReport: "
        +"from "+nodeReg.getName()+" "+blist.getNumberOfBlocks() +" blocks");
  }

  @Override
  public DatanodeCommand blockReport(DatanodeRegistration nodeReg,
      BlockReport blocks) throws IOException {
    return blockReport(nodeReg, blocks.getBlockReportInLongs());
  }
  
  @Override
  public DatanodeCommand blockReport(DatanodeRegistration nodeReg,
                                     long[] blocks) throws IOException {
    verifyRequest(nodeReg);
    myMetrics.numBlockReport.inc();
    BlockListAsLongs blist = new BlockListAsLongs(blocks);
    stateChangeLog.debug("*BLOCK* NameNode.blockReport: "
           +"from "+nodeReg.getName()+" "+blist.getNumberOfBlocks() +" blocks");

    namesystem.processReport(nodeReg, blist);
    if (getFSImage().isUpgradeFinalized())
      return DatanodeCommand.FINALIZE;
    return null;
  }

  @Override
  public void blockReceivedAndDeleted(DatanodeRegistration nodeReg,
                                      Block receivedAndDeletedBlocks[])
                                      throws IOException {
    verifyRequest(nodeReg);
    myMetrics.numBlockReceived.inc();
    namesystem.blockReceivedAndDeleted(nodeReg, receivedAndDeletedBlocks);
  }

  @Override
  public void blockReceivedAndDeleted(DatanodeRegistration nodeReg,
                                      ReceivedDeletedBlockInfo receivedAndDeletedBlocks[])
                                      throws IOException {
    verifyRequest(nodeReg);
    myMetrics.numBlockReceived.inc();
    namesystem.blockReceivedAndDeleted(nodeReg, receivedAndDeletedBlocks);
  }

  /**
   */
  public void errorReport(DatanodeRegistration nodeReg,
                          int errorCode, 
                          String msg) throws IOException {
    // Log error message from datanode
    String dnName = (nodeReg == null ? "unknown DataNode" : nodeReg.getName());
    LOG.info("Error report from " + dnName + ": " + msg);
    if (errorCode == DatanodeProtocol.NOTIFY) {
      return;
    }
    verifyRequest(nodeReg);
    if (errorCode == DatanodeProtocol.DISK_ERROR) {
      LOG.warn("Volume failed on " + dnName); 
    } else if (errorCode == DatanodeProtocol.FATAL_DISK_ERROR) {
      namesystem.removeDatanode(nodeReg);            
    }
  }
    
  public NamespaceInfo versionRequest() throws IOException {
    myMetrics.numVersionRequest.inc();
    return namesystem.getNamespaceInfo();
  }

  public UpgradeCommand processUpgradeCommand(UpgradeCommand comm) throws IOException {
    return namesystem.processDistributedUpgradeCommand(comm);
  }

  /** 
   * Verify request.
   * 
   * Verifies correctness of the datanode version, registration ID, and 
   * if the datanode does not need to be shutdown.
   * 
   * @param nodeReg data node registration
   * @throws IOException
   */
  public void verifyRequest(DatanodeRegistration nodeReg) throws IOException {
    verifyVersion(nodeReg.getVersion());
    if (!namesystem.getRegistrationID().equals(nodeReg.getRegistrationID()))
      throw new UnregisteredDatanodeException(nodeReg);
    myMetrics.numVersionRequest.inc();
  }
    
  /**
   * Verify version.
   * 
   * @param version
   * @throws IOException
   */
  public void verifyVersion(int version) throws IOException {
    if (version != LAYOUT_VERSION)
      throw new IncorrectVersionException(version, "data node");
  }

  /**
   * Returns the name of the fsImage file
   */
  public File getFsImageName() throws IOException {
    return getFSImage().getFsImageName();
  }
    
  public FSImage getFSImage() {
    return namesystem.dir.fsImage;
  }

  /**
   * Returns the name of the fsImage file uploaded by periodic
   * checkpointing
   */
  public File[] getFsImageNameCheckpoint() throws IOException {
    return getFSImage().getFsImageNameCheckpoint();
  }

  /**
   * Returns the address on which the NameNodes is listening to.
   * @return the address on which the NameNodes is listening to.
   */
  public InetSocketAddress getNameNodeAddress() {
    return serverAddress;
  }
  
  public InetSocketAddress getNameNodeDNAddress() {
    if (dnProtocolAddress == null) {
      return serverAddress;
    }
    return dnProtocolAddress;
  }

  /**
   * Returns the address of the NameNodes http server, 
   * which is used to access the name-node web UI.
   * 
   * @return the http address.
   */
  public InetSocketAddress getHttpAddress() {
    return httpAddress;
  }

  NetworkTopology getNetworkTopology() {
    return this.namesystem.clusterMap;
  }

  BlockPlacementPolicy getBlockPlacementPolicy() {
    return this.namesystem.replicator;
  }

  /**
   * Verify that configured directories exist, then
   * Interactively confirm that formatting is desired 
   * for each existing directory and format them.
   * 
   * @param conf
   * @param isConfirmationNeeded
   * @return true if formatting was aborted, false otherwise
   * @throws IOException
   */
  private static boolean format(Configuration conf,
                                boolean isConfirmationNeeded
                                ) throws IOException {
    boolean allowFormat = conf.getBoolean("dfs.namenode.support.allowformat", 
                                          true);
    if (!allowFormat) {
      throw new IOException("The option dfs.namenode.support.allowformat is "
                            + "set to false for this filesystem, so it "
                            + "cannot be formatted. You will need to set "
                            + "dfs.namenode.support.allowformat parameter "
                            + "to true in order to format this filesystem");
    }
    Collection<File> dirsToFormat = FSNamesystem.getNamespaceDirs(conf);
    Collection<File> editDirsToFormat = 
                 FSNamesystem.getNamespaceEditsDirs(conf);
    for(Iterator<File> it = dirsToFormat.iterator(); it.hasNext();) {
      File curDir = it.next();
      if (!curDir.exists())
        continue;
      if (isConfirmationNeeded) {
        System.err.print("Re-format filesystem in " + curDir +" ? (Y or N) ");
        if (!(System.in.read() == 'Y')) {
          System.err.println("Format aborted in "+ curDir);
          return true;
        }
        while(System.in.read() != '\n'); // discard the enter-key
      }
    }

    FSNamesystem nsys = new FSNamesystem(new FSImage(dirsToFormat,
                                         editDirsToFormat), conf);
    nsys.dir.fsImage.format();
    return false;
  }

  private static boolean finalize(Configuration conf,
                               boolean isConfirmationNeeded
                               ) throws IOException {
    Collection<File> dirsToFormat = FSNamesystem.getNamespaceDirs(conf);
    Collection<File> editDirsToFormat = 
                               FSNamesystem.getNamespaceEditsDirs(conf);
    FSNamesystem nsys = new FSNamesystem(new FSImage(dirsToFormat,
                                         editDirsToFormat), conf);
    System.err.print(
        "\"finalize\" will remove the previous state of the files system.\n"
        + "Recent upgrade will become permanent.\n"
        + "Rollback option will not be available anymore.\n");
    if (isConfirmationNeeded) {
      System.err.print("Finalize filesystem state ? (Y or N) ");
      if (!(System.in.read() == 'Y')) {
        System.err.println("Finalize aborted.");
        return true;
      }
      while(System.in.read() != '\n'); // discard the enter-key
    }
    nsys.dir.fsImage.finalizeUpgrade();
    return false;
  }

  @Override
  public void refreshServiceAcl() throws IOException {
    if (!serviceAuthEnabled) {
      throw new AuthorizationException("Service Level Authorization not enabled!");
    }

    SecurityUtil.getPolicy().refresh();
  }

  private static void printUsage() {
    System.err.println(
      "Usage: java NameNode [" +
      StartupOption.FORMAT.getName() + "] | [" +
      StartupOption.UPGRADE.getName() + "] | [" +
      StartupOption.ROLLBACK.getName() + "] | [" +
      StartupOption.FINALIZE.getName() + "] | [" +
      StartupOption.IMPORT.getName() + "]");
  }

  private static StartupOption parseArguments(String args[]) {
    int argsLen = (args == null) ? 0 : args.length;
    StartupOption startOpt = StartupOption.REGULAR;
    for(int i=0; i < argsLen; i++) {
      String cmd = args[i];
      if (StartupOption.FORMAT.getName().equalsIgnoreCase(cmd)) {
        startOpt = StartupOption.FORMAT;
      } else if (StartupOption.REGULAR.getName().equalsIgnoreCase(cmd)) {
        startOpt = StartupOption.REGULAR;
      } else if (StartupOption.UPGRADE.getName().equalsIgnoreCase(cmd)) {
        startOpt = StartupOption.UPGRADE;
      } else if (StartupOption.ROLLBACK.getName().equalsIgnoreCase(cmd)) {
        startOpt = StartupOption.ROLLBACK;
      } else if (StartupOption.FINALIZE.getName().equalsIgnoreCase(cmd)) {
        startOpt = StartupOption.FINALIZE;
      } else if (StartupOption.IMPORT.getName().equalsIgnoreCase(cmd)) {
        startOpt = StartupOption.IMPORT;
      } else
        return null;
    }
    return startOpt;
  }

  private static void setStartupOption(Configuration conf, StartupOption opt) {
    conf.set("dfs.namenode.startup", opt.toString());
  }

  static StartupOption getStartupOption(Configuration conf) {
    return StartupOption.valueOf(conf.get("dfs.namenode.startup",
                                          StartupOption.REGULAR.toString()));
  }

  public static NameNode createNameNode(String argv[], 
                                 Configuration conf) throws IOException {
    if (conf == null)
      conf = new Configuration();
    StartupOption startOpt = parseArguments(argv);
    if (startOpt == null) {
      printUsage();
      return null;
    }
    setStartupOption(conf, startOpt);

    switch (startOpt) {
      case FORMAT:
        boolean aborted = format(conf, true);
        System.exit(aborted ? 1 : 0);
      case FINALIZE:
        aborted = finalize(conf, true);
        System.exit(aborted ? 1 : 0);
      default:
    }

    NameNode namenode = new NameNode(conf);
    return namenode;
  }
    
  /**
   */
  public static void main(String argv[]) throws Exception {
    try {
      StringUtils.startupShutdownMessage(NameNode.class, argv, LOG);
      NameNode namenode = createNameNode(argv, null);
      if (namenode != null)
        namenode.join();
    } catch (Throwable e) {
      LOG.error(StringUtils.stringifyException(e));
      System.exit(-1);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void reconfigurePropertyImpl(String property, String newVal) 
    throws ReconfigurationException {
    // just pass everything to the namesystem
    if (namesystem.isPropertyReconfigurable(property)) {
      namesystem.reconfigureProperty(property, newVal);
    } else {
      throw new ReconfigurationException(property, newVal,
                                         getConf().get(property));
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public List<String> getReconfigurableProperties() {
    // only allow reconfiguration of namesystem's reconfigurable properties
    return namesystem.getReconfigurableProperties();
  }
  
  @Override
  public int getDataTransferProtocolVersion() throws IOException {
    return DataTransferProtocol.DATA_TRANSFER_VERSION;
  }
}
