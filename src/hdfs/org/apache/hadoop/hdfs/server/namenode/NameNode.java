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
import org.apache.hadoop.fs.OpenFileInfo;
import org.apache.hadoop.fs.TrashPolicyDefault;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.FastProtocolHDFS;
import org.apache.hadoop.hdfs.FastWritableHDFS;
import org.apache.hadoop.hdfs.HDFSPolicyProvider;
import org.apache.hadoop.hdfs.FileStatusExtended;
import org.apache.hadoop.hdfs.protocol.*;
import org.apache.hadoop.hdfs.protocol.ClientProxyRequests.*;
import org.apache.hadoop.hdfs.protocol.ClientProxyResponses.*;
import org.apache.hadoop.hdfs.server.common.HdfsConstants.StartupOption;
import org.apache.hadoop.hdfs.server.common.IncorrectVersionException;
import org.apache.hadoop.hdfs.server.common.UpgradeStatusReport;
import org.apache.hadoop.hdfs.server.namenode.ClusterJspHelper.NameNodeKey;
import org.apache.hadoop.hdfs.server.namenode.BlocksMap.BlockInfo;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem.CompleteFileStatus;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem.BlockMetaInfoType;
import org.apache.hadoop.hdfs.server.namenode.metrics.NameNodeMetrics;
import org.apache.hadoop.hdfs.server.protocol.AutoEditsRollerInterface;
import org.apache.hadoop.hdfs.server.protocol.BlockReport;
import org.apache.hadoop.hdfs.server.protocol.BlocksWithLocations;
import org.apache.hadoop.hdfs.server.protocol.DatanodeCommand;
import org.apache.hadoop.hdfs.server.protocol.DatanodeProtocol;
import org.apache.hadoop.hdfs.server.protocol.DatanodeRegistration;
import org.apache.hadoop.hdfs.server.protocol.IncrementalBlockReport;
import org.apache.hadoop.hdfs.server.protocol.NamenodeProtocol;
import org.apache.hadoop.hdfs.server.protocol.NamespaceInfo;
import org.apache.hadoop.hdfs.server.protocol.RemoteEditLogManifest;
import org.apache.hadoop.hdfs.server.protocol.UpgradeCommand;
import org.apache.hadoop.hdfs.util.InjectionEvent;
import org.apache.hadoop.http.HttpServer;
import org.apache.hadoop.ipc.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.util.InjectionHandler;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.net.NetworkTopology;
import org.apache.hadoop.net.Node;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.authorize.AuthorizationException;
import org.apache.hadoop.security.authorize.ConfiguredPolicy;
import org.apache.hadoop.security.authorize.PolicyProvider;
import org.apache.hadoop.security.authorize.RefreshAuthorizationPolicyProtocol;
import org.apache.hadoop.security.authorize.ServiceAuthorizationManager;

import java.io.*;
import java.net.*;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
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
             RefreshAuthorizationPolicyProtocol,
             ClientProxyProtocol,
             AutoEditsRollerInterface {
  static{
    Configuration.addDefaultResource("hdfs-default.xml");
    Configuration.addDefaultResource("hdfs-site.xml");
  }

  private static final String CONF_SERVLET_PATH = "/nnconfchange";
  private boolean currentEnableHftpProperty = true;

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
    } else if (protocol.equals(ClientProxyProtocol.class.getName())) {
      return ClientProxyProtocol.versionID;
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
  public String clusterName;
  public FSNamesystem namesystem; // TODO: This should private. Use getNamesystem() instead.
  /** RPC server */
  volatile private Server server;
  /** RPC server for datanodes */
  private Server dnProtocolServer;
  /** RPC server address */
  volatile private InetSocketAddress serverAddress = null;
  /** RPC server for datanodes address */
  private InetSocketAddress dnProtocolAddress = null;
  /** httpServer */
  volatile protected HttpServer httpServer;
  /** HTTP server address */
  private InetSocketAddress httpAddress = null;
  private Thread emptier;
  private Trash trash;
  /** only used for testing purposes  */
  private volatile boolean stopRequested = false;
  /** Is service level authorization enabled? */
  private boolean serviceAuthEnabled = false;
  /** only used for federation, nameservice ID */
  protected String nameserviceId = null;
  /** Method fingerprint for ClientProtocol */
  private int clientProtocolMethodsFingerprint;
  /**
   * Determines whether or not the namenode should fail when there is a txid
   * mismatch.
   */
  private final boolean failOnTxIdMismatch;

  /**
   * HDFS federation configuration can have two types of parameters:
   * <ol>
   * <li>Parameter that is common for all the name services in the cluster.</li>
   * <li>Parameters that are specific to a name service. This keys are suffixed
   * with nameserviceId in the configuration. For example,
   * "dfs.namenode.rpc-address.nameservice1".</li>
   * </ol>
   *
   * Following are nameservice specific keys.
   */
  public static final String[] NAMESERVICE_SPECIFIC_KEYS = {
    DFS_NAMENODE_RPC_ADDRESS_KEY,
    DATANODE_PROTOCOL_ADDRESS,
    DFS_NAMENODE_HTTP_ADDRESS_KEY,
    DFS_NAMENODE_SECONDARY_HTTP_ADDRESS_KEY,
    DFS_RAIDNODE_HTTP_ADDRESS_KEY,
    FS_NAMENODE_ALIASES,
    DFS_NAMENODE_DN_ALIASES,
    DFS_HTTP_ALIASES,
    "dfs.permissions.audit.log",
    "fs.trash.interval",
    "dfs.permissions",
    "dfs.permissions.checking.paths"
  };

  /** Format a new filesystem.  Destroys any filesystem that may already
   * exist at this location.  **/
  public static void format(Configuration conf) throws IOException {
    format(conf, true, false);
  }

  static NameNodeMetrics myMetrics;

  public FSNamesystem getNamesystem() {
    return namesystem;
  }

  @Override
  public boolean isNamesystemRunning() {
    if (namesystem == null) {
      return false;
    }
    return namesystem.isRunning();
  }

  @Override
  public boolean isNamesystemInitialized() {
    return namesystem != null;
  }

  public HttpServer getHttpServer() {
    return this.httpServer;
  }

  public static NameNodeMetrics getNameNodeMetrics() {
    return myMetrics;
  }

  /**
   * Compose a "host:port" string from the address.
   * @deprecated because it might use reverse DNS.
   */
  @Deprecated
  public static String getHostPortString(InetSocketAddress addr) {
    return addr.getHostName() + ":" + addr.getPort();
  }

  public int getClientProtocolMethodsFingerprint() {
    return clientProtocolMethodsFingerprint;
  }

  public void setClientProtocolMethodsFingerprint(
      int clientProtocolMethodsFingerprint) {
    this.clientProtocolMethodsFingerprint = clientProtocolMethodsFingerprint;
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
    return getAddress(conf, DATANODE_PROTOCOL_ADDRESS);
  }

  public static InetSocketAddress getHttpServerAddress(Configuration conf) {
    return getAddress(conf, DFS_NAMENODE_HTTP_ADDRESS_KEY);
  }

  public static InetSocketAddress getClientProtocolAddress(Configuration conf) {
    InetSocketAddress rpcAddr = getAddress(conf, DFS_NAMENODE_RPC_ADDRESS_KEY);
    if(rpcAddr == null) {
      LOG.warn(DFS_NAMENODE_RPC_ADDRESS_KEY + " isn't configured. falling back to fs.default.name (Deprecated)");
    }
    return rpcAddr == null ? getAddress(conf) : rpcAddr;
  }

  private static InetSocketAddress getAddress(Configuration conf, String property) {
    String dnAddressString = conf.get(property);
    if (dnAddressString == null || dnAddressString.isEmpty())
      return null;
    return getAddress(dnAddressString);
  }

  /**
   * @deprecated Use {@link #getClientProtocolAddress(Configuration)}
   */
  @Deprecated
  private static InetSocketAddress getAddress(Configuration conf) {
    return getAddress(getDefaultAddress(conf));
  }

  /**
   * Returns the fs.default name from the configuration as a string for znode name retrieval
   * without a DNS lookup.
   * @param conf
   * @return
   */
  public static String getDefaultAddress(Configuration conf) {
    URI uri = FileSystem.getDefaultUri(conf);
    String authority = uri.getAuthority();
    if (authority == null) {
      throw new IllegalArgumentException(String.format(
          "Invalid URI for NameNode address (check %s): %s has no authority.",
          FileSystem.FS_DEFAULT_NAME_KEY, uri.toString()));
    }
    return authority;
  }

  public static URI getUri(InetSocketAddress namenode) {
    int port = namenode.getPort();
    return URI.create("hdfs://"+ namenode.getHostName()+ ":" + port);
  }

  /**
   * Initialize name-node.
   *
   */
  protected void initialize() throws IOException {
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
    NetUtils.isSocketBindable(getClientProtocolAddress(getConf()));
    NetUtils.isSocketBindable(getDNProtocolAddress(getConf()));
    NetUtils.isSocketBindable(getHttpServerAddress(getConf()));

    long serverVersion = ClientProtocol.versionID;
    this.clientProtocolMethodsFingerprint = ProtocolSignature
        .getMethodsSigFingerPrint(ClientProtocol.class, serverVersion);

    myMetrics = new NameNodeMetrics(getConf(), this);

    this.clusterName = getConf().get(FSConstants.DFS_CLUSTER_NAME);
    this.namesystem = new FSNamesystem(this, getConf());
    // HACK: from removal of FSNamesystem.getFSNamesystem().
    JspHelper.fsn = this.namesystem;

    this.startDNServer();
    startHttpServer(getConf());
  }

  private static FileSystem getTrashFileSystem(Configuration conf) throws IOException {
    conf = new Configuration(conf);
    InetSocketAddress serviceAddress = NameNode.getDNProtocolAddress(conf);
    if (serviceAddress != null) {
      URI defaultUri = FileSystem.getDefaultUri(conf);
      URI serviceUri = null;
      try {
        serviceUri = new URI(defaultUri.getScheme(), defaultUri.getUserInfo(),
            serviceAddress.getAddress().getHostAddress(), serviceAddress.getPort(),
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
    this.trash = new Trash(fs, conf);
    this.emptier = new Thread(trash.getEmptier(), "Trash Emptier");
    this.emptier.setDaemon(true);
    this.emptier.start();
  }

  private void startHttpServer(Configuration conf) throws IOException {
    String infoAddr =
      NetUtils.getServerAddress(conf, "dfs.info.bindAddress",
                                "dfs.info.port", "dfs.http.address");
    InetSocketAddress infoSocAddr = NetUtils.createSocketAddr(infoAddr);
    String infoHostIp = infoSocAddr.getAddress().getHostAddress();
    int infoPort = infoSocAddr.getPort();
    this.httpServer = new HttpServer("hdfs", infoHostIp, infoPort,
        infoPort == 0, conf);
    if (conf.getBoolean("dfs.https.enable", false)) {
      boolean needClientAuth = conf.getBoolean("dfs.https.need.client.auth", false);
      InetSocketAddress secInfoSocAddr = NetUtils.createSocketAddr(conf.get(
          "dfs.https.address", infoHostIp + ":" + 0));
      Configuration sslConf = new Configuration(false);
      sslConf.addResource(conf.get("dfs.https.server.keystore.resource",
          "ssl-server.xml"));
      this.httpServer.addSslListener(secInfoSocAddr, sslConf, needClientAuth);
      // assume same ssl port for all datanodes
      InetSocketAddress datanodeSslPort = NetUtils.createSocketAddr(conf.get(
          "dfs.datanode.https.address", infoHostIp + ":" + 50475));
      this.httpServer.setAttribute("datanode.https.port", datanodeSslPort
          .getPort());
    }
    this.httpServer.setAttribute("name.node", this);
    this.httpServer.setAttribute("name.node.address", this.serverAddress);
    this.httpServer.setAttribute("name.system.image", getFSImage());
    this.httpServer.setAttribute("name.conf", conf);
    this.httpServer.addInternalServlet("fsck", "/fsck", FsckServlet.class);
    this.httpServer.addInternalServlet("getimage", "/getimage", GetImageServlet.class);
    this.httpServer.addInternalServlet("latestimage", "/latestimage", LatestImageServlet.class);
    if (conf.getBoolean("dfs.enableHftp",true)) {
      this.httpServer.addInternalServlet("listPaths", "/listPaths/*",
          ListPathsServlet.class);
      this.httpServer.addInternalServlet("data", "/data/*",
          FileDataServlet.class);
      this.httpServer.addInternalServlet("checksum", "/fileChecksum/*",
          FileChecksumServlets.RedirectServlet.class);
      this.currentEnableHftpProperty = true;
    } else {
      this.currentEnableHftpProperty = false;
    }

    this.httpServer.addInternalServlet("namenodeMXBean", "/namenodeMXBean",
        NameNodeMXBeanServlet.class);
    httpServer.setAttribute(ReconfigurationServlet.
                            CONF_SERVLET_RECONFIGURABLE_PREFIX +
                            CONF_SERVLET_PATH, NameNode.this);
    httpServer.addInternalServlet("nnconfchange", CONF_SERVLET_PATH,
                                  ReconfigurationServlet.class);

    this.httpServer.start();

    // The web-server port can be ephemeral... ensure we have the correct info
    infoPort = this.httpServer.getPort();
    this.httpAddress = new InetSocketAddress(infoHostIp, infoPort);
    conf.set("dfs.http.address", infoHostIp + ":" + infoPort);
    LOG.info("Web-server up at: " + infoHostIp + ":" + infoPort);
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
  public NameNode(Configuration conf, boolean failOnTxIdMismatch)
    throws IOException {
    super(conf);
    this.failOnTxIdMismatch = failOnTxIdMismatch;
    boolean initialized = false;
    try {
      initialize();
      initialized = true;
    } finally {
      if (!initialized) {
        this.stop();
      }
    }
  }

  public NameNode(Configuration conf) throws IOException {
    this(conf, true);
  }

  /**
   * In federation configuration is set for a set of
   * namenode and secondary namenode/backup/checkpointer, which are
   * grouped under a logical nameservice ID. The configuration keys specific
   * to them have suffix set to configured nameserviceId.
   *
   * This method copies the value from specific key of format key.nameserviceId
   * to key, to set up the generic configuration. Once this is done, only
   * generic version of the configuration is read in rest of the code, for
   * backward compatibility and simpler code changes.
   *
   * @param conf
   *          Configuration object to lookup specific key and to set the value
   *          to the key passed. Note the conf object is modified
   * @see DFSUtil#setGenericConf(Configuration, String, String...)
   */
  public static void initializeGenericKeys(Configuration conf, String serviceKey) {
    if ((serviceKey == null) || serviceKey.isEmpty()) {
      return;
    }

    // adjust meta directory names by service key
    adjustMetaDirectoryNames(conf, serviceKey);

    DFSUtil.setGenericConf(conf, serviceKey, NAMESERVICE_SPECIFIC_KEYS);
  }

  /** Append service name to each meta directory name
   *
   * @param conf configuration of NameNode
   * @param serviceKey the non-empty name of the name node service
   */
  protected static void adjustMetaDirectoryNames(Configuration conf, String serviceKey) {
    adjustMetaDirectoryName(conf, DFS_NAMENODE_NAME_DIR_KEY, serviceKey);
    adjustMetaDirectoryName(conf, DFS_NAMENODE_EDITS_DIR_KEY, serviceKey);
    adjustMetaDirectoryName(conf, DFS_NAMENODE_CHECKPOINT_DIR_KEY, serviceKey);
    adjustMetaDirectoryName(
        conf, DFS_NAMENODE_CHECKPOINT_EDITS_DIR_KEY, serviceKey);
  }

  protected static void adjustMetaDirectoryName(
      Configuration conf, String key, String serviceDirName) {
    Collection<String> dirNames = conf.getStringCollection(key);
    if (dirNames.isEmpty()) {
      dirNames.add("/tmp/hadoop/dfs/name");
    }
    String newValues = "";
    for (String dir : dirNames) {
      // check if the configured path has the trailing separator
      newValues += dir + (dir.endsWith(Path.SEPARATOR) ? "" : Path.SEPARATOR)
          + serviceDirName + ",";
    }
    // remove trailing comma
    conf.set(key, newValues.substring(0, newValues.length() - 1));
  }

  public static void setupDefaultURI(Configuration conf) {
    String rpcAddress = conf.get(DFS_NAMENODE_RPC_ADDRESS_KEY);
    if ( rpcAddress != null) {
      URI defaultUri = URI.create("hdfs://" + rpcAddress);
      conf.set(FileSystem.FS_DEFAULT_NAME_KEY, defaultUri.toString());
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
      InetSocketAddress socAddr = NameNode.getClientProtocolAddress(getConf());
      int handlerCount = getConf().getInt("dfs.namenode.handler.count", 10);

      // create rpc server 
      // job conf won't be sent to namenode
      this.server = RPC.getServer(this, socAddr.getAddress().getHostAddress(), socAddr.getPort(),
                                  handlerCount, false, getConf(), false);
      // The rpc-server port can be ephemeral... ensure we have the correct info
      this.serverAddress = this.server.getListenerAddress();

      FileSystem.setDefaultUri(getConf(), getUri(serverAddress));
      if (this.httpServer != null) {
        // This means the server is being started once out of safemode
        // and jetty is initialized already
        this.httpServer.setAttribute("name.node.address", this.serverAddress);
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
      // Datanode won't send job conf object through RPC
      this.dnProtocolServer = RPC.getServer(this, dnAddr.getAddress().getHostAddress(),
          dnAddr.getPort(), dnHandlerCount, false, getConf(), false);
      this.dnProtocolAddress = dnProtocolServer.getListenerAddress();
      NameNode.setDNProtocolAddress(getConf(), dnProtocolAddress.getAddress().getHostAddress()
          + ":" + dnProtocolAddress.getPort());
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
   * Quiescess all communication to namenode cleanly.
   * Ensures all RPC handlers have exited.
   *
   * @param interruptClientHandlers should the handlers be interrupted
   */
  protected void stopRPC(boolean interruptClientHandlers)
      throws IOException, InterruptedException {
    // stop client handlers
    stopRPCInternal(server, "client", interruptClientHandlers);

    // stop datanode handlers
    stopRPCInternal(dnProtocolServer, "datanode", interruptClientHandlers);

    // waiting for the ongoing requests to complete
    stopWaitRPCInternal(server, "client");
    stopWaitRPCInternal(dnProtocolServer, "datanode");
  }

  protected static void stopRPCInternal(Server server, String name,
      boolean interruptClientHandlers) throws IOException, InterruptedException {
    if (server != null) {
      LOG.info("stopRPC: Stopping " + name + " server");
      server.stop(interruptClientHandlers);
      InjectionHandler.processEvent(InjectionEvent.NAMENODE_STOP_RPC);
    }
  }

  protected static void stopWaitRPCInternal(Server server, String name)
      throws IOException, InterruptedException {
    if (server != null) {
      server.waitForHandlers();
      server.stopResponder();
      LOG.info("stopRPC: Stopping " + name + " server - DONE");
    }
  }

  /**
   * Stop all NameNode threads and wait for all to finish.
   */
  public void stop() {
    if (stopRequested)
      return;
    stopRequested = true;
    LOG.info("Stopping http server");
    try {
      if (httpServer != null) httpServer.stop();
    } catch (Exception e) {
      LOG.error(StringUtils.stringifyException(e));
    }

    LOG.info("Stopping namesystem");
    if(namesystem != null) namesystem.close();

    LOG.info("Stopping emptier");
    if(emptier != null) emptier.interrupt();

    LOG.info("Stopping rpc servers");
    if(server != null) server.stop();
    if (dnProtocolServer != null) dnProtocolServer.stop();

    LOG.info("Stopping metrics");
    if (myMetrics != null) {
      myMetrics.shutdown();
    }

    LOG.info("Stopping namesystem mbeans");
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

  public CheckpointSignature getCheckpointSignature() throws IOException {
    return namesystem.getCheckpointSignature();
  }

  public LocatedBlocksWithMetaInfo updateDatanodeInfo(
      LocatedBlocks locatedBlocks) throws IOException {
    return namesystem.updateDatanodeInfo(locatedBlocks);
  }

  /////////////////////////////////////////////////////
  // ClientProtocol
  /////////////////////////////////////////////////////

  /** {@inheritDoc} */
  public LocatedBlocks   getBlockLocations(String src,
                                          long offset,
                                          long length) throws IOException {
    return getBlockLocationsInternal(src, offset, length, BlockMetaInfoType.NONE);
  }

  public VersionedLocatedBlocks open(String src,
                                     long offset,
                                     long length) throws IOException {
    return (VersionedLocatedBlocks) getBlockLocationsInternal(src, offset, length,
        BlockMetaInfoType.VERSION);
  }

  public LocatedBlocksWithMetaInfo openAndFetchMetaInfo(String src,
                                                        long offset,
                                                        long length)
  throws IOException {
    return (LocatedBlocksWithMetaInfo) getBlockLocationsInternal(src, offset, length,
        BlockMetaInfoType.VERSION_AND_NAMESPACEID);
  }

  private LocatedBlocks getBlockLocationsInternal(String src, long offset, long length,
      BlockMetaInfoType metaInfoType) throws IOException {
    myMetrics.numGetBlockLocations.inc();
    return namesystem.getBlockLocations(getClientMachine(), src, offset, length, metaInfoType);
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
    createInternal(src, masked, clientName, overwrite, createParent, replication, blockSize);
  }

  private void createInternal(String src,
                      FsPermission masked,
                      String clientName,
                      boolean overwrite,
                      boolean createParent,
                      short replication,
                      long blockSize) throws IOException {
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
        new PermissionStatus(FSNamesystem.getCurrentUGI().getUserName(),
            null, masked),
        clientName, clientMachine, overwrite, createParent, replication, blockSize);
    InjectionHandler.processEventIO(InjectionEvent.NAMENODE_AFTER_CREATE_FILE);
    myMetrics.numFilesCreated.inc();
    myMetrics.numCreateFileOps.inc();
  }

  /** {@inheritDoc} */
  public LocatedBlock append(String src, String clientName) throws IOException {
    return appendInternal(src, clientName, BlockMetaInfoType.NONE);
  }

  public LocatedBlockWithMetaInfo appendAndFetchMetaInfo(String src, String clientName) throws IOException {
    return (LocatedBlockWithMetaInfo)appendInternal(src, clientName, BlockMetaInfoType.VERSION_AND_NAMESPACEID);
  }

  public LocatedBlockWithOldGS appendAndFetchOldGS(String src, String clientName)
          throws IOException {
    return (LocatedBlockWithOldGS)appendInternal(src, clientName,
        BlockMetaInfoType.VERSION_AND_NAMESPACEID_AND_OLD_GS);
  }

  private LocatedBlock appendInternal(String src, String clientName, BlockMetaInfoType type) throws IOException {
    String clientMachine = getClientMachine();
    if (stateChangeLog.isDebugEnabled()) {
      stateChangeLog.debug("*DIR* NameNode.append: file "
          +src+" for "+clientName+" at "+clientMachine);
    }
    LocatedBlock info = namesystem.appendFile(src, clientName, clientMachine, type);
    myMetrics.numFilesAppended.inc();
    return info;
  }

  /** {@inheritDoc} */
  public void recoverLease(String src, String clientName) throws IOException {
    namesystem.recoverLease(src, clientName, getClientMachine(), false);
  }

  @Override
  public boolean closeRecoverLease(String src, String clientName)
    throws IOException {
    return closeRecoverLease(src, clientName, Boolean.valueOf(false));
  }

  @Override
  public boolean closeRecoverLease(String src, String clientName,
                                   boolean discardLastBlock) throws IOException {
    return namesystem.recoverLease(src, clientName, getClientMachine(), discardLastBlock);
  }

  /** {@inheritDoc} */
  public boolean setReplication(String src,
                                short replication
                                ) throws IOException {
    return setReplicationInternal(src, replication);
  }

  private boolean setReplicationInternal(String src, short replication) throws IOException {
    boolean value = namesystem.setReplication(src, replication);
    if (value) {
      myMetrics.numSetReplication.inc();
    }
    return value;
  }

  /** {@inheritDoc} */
  public void setPermission(String src, FsPermission permissions
      ) throws IOException {
    setPermissionInternal(src, permissions);
  }

  private void setPermissionInternal(String src, FsPermission permissions) throws IOException {
    namesystem.setPermission(src, permissions);
    myMetrics.numSetPermission.inc();
  }

  /** {@inheritDoc} */
  public void setOwner(String src, String username, String groupname
      ) throws IOException {
    setOwnerInternal(src, username, groupname);
  }

  private void setOwnerInternal(String src, String username, String groupname) throws IOException {
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

  public LocatedBlock addBlock(String src,
                               String clientName,
                               DatanodeInfo[] excludedNodes)
    throws IOException {
    return addBlockInternal(src, clientName, excludedNodes, null, -1, null, BlockMetaInfoType.NONE);
  }

  @Override
  public VersionedLocatedBlock addBlockAndFetchVersion(String src, String clientName,
      DatanodeInfo[] excludedNodes) throws IOException {
    return (VersionedLocatedBlock) addBlockInternal(src, clientName, excludedNodes, null, -1, null,
        BlockMetaInfoType.VERSION);
  }

  @Override
  public LocatedBlock addBlock(String src, String clientName,
      DatanodeInfo[] excludedNodes, DatanodeInfo[] favoredNodes)
      throws IOException {
    return addBlockInternal(src, clientName, excludedNodes, favoredNodes, -1, null,
        BlockMetaInfoType.NONE);
  }

  @Override
  public LocatedBlockWithMetaInfo addBlockAndFetchMetaInfo(String src,
      String clientName, DatanodeInfo[] excludedNodes) throws IOException {
    return (LocatedBlockWithMetaInfo) addBlockInternal(src, clientName, excludedNodes, null, -1,
        null, BlockMetaInfoType.VERSION_AND_NAMESPACEID);
  }

  @Override
  public LocatedBlockWithMetaInfo addBlockAndFetchMetaInfo(String src,
      String clientName, DatanodeInfo[] excludedNodes, long startPos)
      throws IOException {
    return (LocatedBlockWithMetaInfo) addBlockInternal(src, clientName, excludedNodes, null,
        startPos, null, BlockMetaInfoType.VERSION_AND_NAMESPACEID);
  }

  @Override
  public LocatedBlockWithMetaInfo addBlockAndFetchMetaInfo(
      String src, String clientName,
      DatanodeInfo[] excludedNodes, DatanodeInfo[] favoredNodes)
      throws IOException {
    return (LocatedBlockWithMetaInfo) addBlockInternal(src, clientName, excludedNodes, favoredNodes,
        -1, null, BlockMetaInfoType.VERSION_AND_NAMESPACEID);
  }

  @Override
  public LocatedBlockWithMetaInfo addBlockAndFetchMetaInfo(
      String src, String clientName,
      DatanodeInfo[] excludedNodes, DatanodeInfo[] favoredNodes,
      long startPos)
      throws IOException {
    return (LocatedBlockWithMetaInfo) addBlockInternal(src, clientName, excludedNodes, favoredNodes,
        startPos, null, BlockMetaInfoType.VERSION_AND_NAMESPACEID);
  }

  @Override
  public LocatedBlockWithMetaInfo addBlockAndFetchMetaInfo(String src,
      String clientName, DatanodeInfo[] excludedNodes,
      DatanodeInfo[] favoredNodes, long startPos, Block lastBlock)
      throws IOException {
    return (LocatedBlockWithMetaInfo) addBlockInternal(src, clientName, excludedNodes, favoredNodes,
        startPos, lastBlock, BlockMetaInfoType.VERSION_AND_NAMESPACEID);
  }

  private LocatedBlock addBlockInternal(String src, String clientName, DatanodeInfo[] excludedNodes,
      DatanodeInfo[] favoredNodes, long startPos, Block lastBlock, BlockMetaInfoType type) throws
      IOException {
    return addBlockInternal(src, clientName, safeAsList(excludedNodes), safeAsList(favoredNodes),
        startPos, lastBlock, type);
  }

  private LocatedBlock addBlockInternal(String src, String clientName,
      List<? extends Node> excludedNodes, List<DatanodeInfo> favoredNodes, long startPos,
      Block lastBlock, BlockMetaInfoType type) throws IOException {
    List<Node> excludedNodesList = null;
    if (excludedNodes != null && !excludedNodes.isEmpty()) {
      // We must copy here, since this list gets modified later on in ReplicationTargetChooser
      excludedNodesList = new ArrayList<Node>(excludedNodes.size());
      for (Node node : excludedNodes) {
        excludedNodesList.add(node);
      }
    }
    if (favoredNodes != null && favoredNodes.isEmpty()) {
      favoredNodes = null;
    }
    if (stateChangeLog.isDebugEnabled()) {
      stateChangeLog.debug("*BLOCK* NameNode.addBlock: file " + src + " for " + clientName);
    }
    LocatedBlock locatedBlock = namesystem.getAdditionalBlock(src, clientName, excludedNodesList,
        favoredNodes, startPos, lastBlock, type);
    if (locatedBlock != null) {
      myMetrics.numAddBlockOps.inc();
    }
    return locatedBlock;
  }

  /**
   * The client needs to give up on the block.
   */
  public void abandonBlock(Block b, String src, String holder
      ) throws IOException {
    abandonBlockInternal(b, src, holder);
  }

  private void abandonBlockInternal(Block b, String src, String holder) throws IOException {
    if (stateChangeLog.isDebugEnabled()) {
      stateChangeLog.debug("*BLOCK* NameNode.abandonBlock: "
                           +b+" of file "+src);
    }
    if (!namesystem.abandonBlock(b, src, holder)) {
      throw new IOException("Cannot abandon block during write to " + src);
    }
    myMetrics.numAbandonBlock.inc();
  }

  @Override
  public void abandonFile(String src, String holder) throws IOException {
    abandonFileInternal(src, holder);
  }

  private void abandonFileInternal(String src, String holder) throws IOException {
    if (stateChangeLog.isDebugEnabled()) {
      stateChangeLog.debug("*FILE* NameNode.abandonFile: " + src);
    }
    if (!namesystem.abandonFile(src, holder)) {
      throw new IOException("Cannot abandon write to file " + src);
    }
  }

  @Override
  public boolean raidFile(String source, String codecId, short expectedSourceRepl)
    throws IOException {
    return raidFileInternal(source, codecId, expectedSourceRepl);
  }

  private boolean raidFileInternal(String source, String codecId, short expectedSourceRepl) throws
      IOException {
    boolean status = namesystem.raidFile(source, codecId, expectedSourceRepl);
    myMetrics.numRaidFileOps.inc();
    if (status) {
      myMetrics.numFilesRaided.inc();
    }
    return status;
  }

  /** {@inheritDoc} */
  public boolean complete(String src, String clientName) throws IOException {
    return complete(src, clientName, -1);
  }

  @Override
  public boolean complete(String src, String clientName, long fileLen)
      throws IOException {
    return complete(src, clientName, fileLen, null);
  }

  @Override
  public boolean complete(String src, String clientName, long fileLen, Block lastBlock)
      throws IOException {
    return completeInternal(src, clientName, fileLen, lastBlock);
  }

  private boolean completeInternal(String src, String clientName, long fileLen,
      Block lastBlock) throws IOException {
    if (stateChangeLog.isDebugEnabled()) {
      stateChangeLog.debug("*DIR* NameNode.complete: " + src + " for " + clientName);
    }
    CompleteFileStatus returnCode = namesystem.completeFile(src, clientName,
        fileLen, lastBlock);
    if (returnCode == CompleteFileStatus.STILL_WAITING) {
      return false;
    } else if (returnCode == CompleteFileStatus.COMPLETE_SUCCESS) {
      myMetrics.numCompleteFile.inc();
      return true;
    } else {
      // do not change message in this exception
      // failover client relies on this message
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
    reportBadBlocksInternal(safeAsList(blocks));
  }

  private void reportBadBlocksInternal(List<LocatedBlock> blocks) throws IOException {
    stateChangeLog.info("*DIR* NameNode.reportBadBlocks");
    myMetrics.numReportBadBlocks.inc();
    for (LocatedBlock locatedBlock : blocks) {
      Block blk = locatedBlock.getBlock();
      DatanodeInfo[] nodes = locatedBlock.getLocations();
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
   * {@inheritDoc}
   */
  public void merge(String parity, String source, String codecId,
      int[] checksums) throws IOException {
    namesystem.merge(parity, source, codecId, checksums);
    myMetrics.numFilesMerged.inc();
  }

  /**
   * {@inheritDoc}
   */
  public boolean hardLink(String src, String dst) throws IOException {
    return hardLinkInternal(src, dst);
  }

  private boolean hardLinkInternal(String src, String dst) throws IOException {
    if (stateChangeLog.isDebugEnabled()) {
      stateChangeLog.debug("*DIR* NameNode.hardlink: " + src + " to " + dst);
    }
    if (!checkPathLength(dst)) {
      throw new IOException("hardlink: Pathname too long.  Limit "
          + MAX_PATH_LENGTH + " characters, " + MAX_PATH_DEPTH + " levels.");
    }
    return namesystem.hardLinkTo(src, dst);
  }

  /**
   * {@inheritDoc}
   */
  public String[] getHardLinkedFiles(String src) throws IOException {
    return namesystem.getHardLinkedFiles(src);
  }

  /**
   */
  public boolean rename(String src, String dst) throws IOException {
    return renameInternal(src, dst);
  }

  private boolean renameInternal(String src, String dst) throws IOException {
    if (stateChangeLog.isDebugEnabled()) {
      stateChangeLog.debug("*DIR* NameNode.rename: " + src + " to " + dst);
    }
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
    return deleteInternal(src, recursive);
  }

  private boolean deleteInternal(String src, boolean recursive) throws IOException {
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
    return (src.length() <= MAX_PATH_LENGTH);
  }

  /** {@interitDoc} */
  public OpenFileInfo[] iterativeGetOpenFiles(
    String prefix, int millis, String start) throws IOException {
    return namesystem.iterativeGetOpenFiles(prefix, millis, start);
  }

  /** {@inheritDoc} */
  public boolean mkdirs(String src, FsPermission masked) throws IOException {
    return mkdirsInternal(src, masked);
  }

  private boolean mkdirsInternal(String src, FsPermission masked) throws IOException {
    if (stateChangeLog.isDebugEnabled()) {
      stateChangeLog.debug("*DIR* NameNode.mkdirs: " + src);
    }
    if (!checkPathLength(src)) {
      throw new IOException("mkdirs: Pathname too long.  Limit "
                            + MAX_PATH_LENGTH + " characters, " + MAX_PATH_DEPTH + " levels.");
    }
    boolean value =  namesystem.mkdirs(src,
        new PermissionStatus(FSNamesystem.getCurrentUGI().getUserName(),
            null, masked));
    if (value) {
      myMetrics.numMkdirs.inc();
    }
    return value;
  }

  /**
   */
  public void renewLease(String clientName) throws IOException {
    renewLeaseInternal(clientName);
  }

  private void renewLeaseInternal(String clientName) throws IOException {
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
    return getPartialListingInternal(src, startAfter, false);
  }

  @Override
  public LocatedDirectoryListing getLocatedPartialListing(
      String src, byte[] startAfter)
  throws IOException {
    return (LocatedDirectoryListing) getPartialListingInternal(src, startAfter, true);
  }

  private DirectoryListing getPartialListingInternal(String src, byte[] startAfter,
      boolean needLocation) throws IOException {
    DirectoryListing files = namesystem.getPartialListing(src, startAfter, needLocation);
    if (files != null) {
      myMetrics.numGetListingOps.inc();
    }
    return files;
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
    return getHdfsFileInfoInternal(src);
  }

  private HdfsFileStatus getHdfsFileInfoInternal(String src) throws IOException {
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
   * Determines whether or not we should check for heartbeats.
   */
  protected boolean shouldCheckHeartbeat() {
    return !namesystem.isInSafeMode();
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
   * @return The most recent transaction ID that has been synced to
   * persistent storage.
   * @throws IOException
   */
  public long getTransactionID() throws IOException {
    return namesystem.getTransactionID();
  }

  /**
   * Roll the edit log.
   */
  public CheckpointSignature rollEditLog() throws IOException {
    return namesystem.rollEditLog();
  }

  /**
   * Roll the edit log manually.
   */
  public void rollEditLogAdmin() throws IOException {
    rollEditLog();
  }

  /**
   * Roll the image
   */
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
   * Enable/Disable Block Replication
   */
  public void blockReplication(boolean isEnable) throws IOException {
    namesystem.blockReplication(isEnable);
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
    return listCorruptFileBlocksInternal(path, cookie);
  }

  private  CorruptFileBlocks listCorruptFileBlocksInternal(String path,
      String cookie) throws IOException {
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
    return getContentSummaryInternal(path);
  }

  private ContentSummary getContentSummaryInternal(String path) throws IOException {
    myMetrics.numGetContentSummary.inc();
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
    fsyncInternal(src, clientName);
  }

  private void fsyncInternal(String src, String clientName) throws IOException {
    namesystem.fsync(src, clientName);
    myMetrics.numFsync.inc();
  }

  /** @inheritDoc */
  public void setTimes(String src, long mtime, long atime) throws IOException {
    setTimesInternal(src, mtime, atime);
  }

  private void setTimesInternal(String src, long mtime, long atime) throws IOException {
    namesystem.setTimes(src, mtime, atime);
    myMetrics.numSetTimes.inc();
  }

  ////////////////////////////////////////////////////////////////
  // DatanodeProtocol
  ////////////////////////////////////////////////////////////////
  /**
   */
  @Override
  public DatanodeRegistration register(DatanodeRegistration nodeReg
                                       ) throws IOException {
    verifyVersion(nodeReg.getVersion(), LAYOUT_VERSION, "layout");
    namesystem.registerDatanode(nodeReg);
    myMetrics.numRegister.inc();
    return nodeReg;
  }

  @Override
  public DatanodeRegistration register(DatanodeRegistration nodeReg,
      int dataTransferVersion) throws IOException {
    verifyVersion(dataTransferVersion,
        DataTransferProtocol.DATA_TRANSFER_VERSION, "data transfer");
    return register(nodeReg);

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
                                       long namespaceUsed,
                                       int xmitsInProgress,
                                       int xceiverCount) throws IOException {
    verifyRequest(nodeReg);
    myMetrics.numHeartbeat.inc();
    return namesystem.handleHeartbeat(nodeReg, capacity, dfsUsed, remaining, namespaceUsed,
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
    boolean processed = namesystem.processBlocksBeingWrittenReport(nodeReg, blist);

    String message = "*BLOCK* NameNode.blocksBeingWrittenReport: "
        +"from "+nodeReg.getName()+" "+blist.getNumberOfBlocks() +" blocks";
    if (!processed) {
      message += " was discarded.";
    }
    stateChangeLog.info(message);
  }

  protected Collection<Block> blockReportWithRetries(
      DatanodeRegistration nodeReg, BlockReport blocks) throws IOException {
    verifyRequest(nodeReg);
    myMetrics.numBlockReport.inc();
    BlockListAsLongs blist =
      new BlockListAsLongs(blocks.getBlockReportInLongs());
    stateChangeLog.debug("*BLOCK* NameNode.blockReport: " + "from "
        + nodeReg.getName() + " " + blist.getNumberOfBlocks() + " blocks");

    return namesystem.processReport(nodeReg, blist);
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
    if (stateChangeLog.isDebugEnabled()) {
      stateChangeLog.debug("*BLOCK* NameNode.blockReport: "
             +"from "+nodeReg.getName()+" "+blist.getNumberOfBlocks() +" blocks");
    }

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
      IncrementalBlockReport receivedAndDeletedBlocks)
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

  long getCTime() {
    return namesystem.dir.fsImage.storage.cTime;
  }

  /**
   * Verify request.
   *
   * Verifies correctness of the datanode layout version, namespace ID and cTime
   * . We support a higher layout version for the datanode and a lower or equal
   * cTime for the datanode, but the namespace Id has to match.
   *
   * @param nodeReg data node registration
   * @throws IOException
   */
  public void verifyRequest(DatanodeRegistration nodeReg) throws IOException {
    verifyVersion(nodeReg.getVersion(), LAYOUT_VERSION, "layout");
    // The ctime of the namenode has to be greater than or equal to the ctime of
    // the datanode. The ctime of the namenode is updated when it is upgraded
    // and only then the ctime of the datanode is updated. Hence the datanode's
    // ctime should never be greater than the namenode's ctime.
    if (getNamespaceID() != nodeReg.storageInfo.namespaceID
        || getCTime() < nodeReg.storageInfo.cTime) {
      LOG.warn("Invalid Request : NN namespaceId, cTime : " + getNamespaceID()
          + ", " + getCTime() + " DN namespaceId, cTime : "
          + nodeReg.storageInfo.namespaceID + ", " + nodeReg.storageInfo.cTime);
      throw new UnregisteredDatanodeException(nodeReg);
    }
    myMetrics.numVersionRequest.inc();
  }

  /**
   * Verify version.
   *
   * @param reportedVersion version reported by datanode
   * @param expectedVersion version expected by namenode
   * @param annotation explanation of the given version
   * @throws IncorrectVersionException
   */
  public static void verifyVersion(int reportedVersion,
      int expectedVersion, String annotation) throws IOException {
    if ((reportedVersion ^ expectedVersion) < 0) {
      throw new IOException("reportedVersion and expectedVersion have" +
          " different signs : " + reportedVersion + ", " + expectedVersion);
    }
    // layout_version is negative and data_transfer is positive, so we need to
    // look at the absolute.
    if (Math.abs(reportedVersion) < Math.abs(expectedVersion))
      throw new IncorrectVersionException(
          reportedVersion, "data node " + annotation, expectedVersion);
  }

  public FSImage getFSImage() {
    return namesystem.dir.fsImage;
  }

  public long getLastWrittenTxId() {
    if (getFSImage() != null && getFSImage().getEditLog() != null) {
      return getFSImage().getEditLog().getLastWrittenTxId();
    } else {
      return -1;
    }
  }

  /**
   * Get a sample of the total files in the FileSystem. The sampling is done
   * randomly.
   *
   * @param percentage
   *          the percentage of files to sample, value should be between (0 -
   *          1.0]
   * @return the list of files
   */
  public List<FileStatusExtended> getRandomFilesSample(double percentage) {
    if (!(percentage > 0 && percentage <= 1.0)) {
      throw new IllegalArgumentException("Invalid percentage : " + percentage +
          " value should be between (0 - 1.0]");
    }
    LOG.info("Sampling : " + (percentage * 100) + " percent of files");
    return namesystem.getRandomFiles(percentage);
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
   * Denotes whether the RPC server is running and accepting connections from
   * clients.
   * @return true if the RPC server for clients is running, false otherwise
   */
  protected boolean isRpcServerRunning() {
    return (this.server != null && this.server.isAlive());
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
  static boolean format(Configuration conf,
                                boolean force,
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
    Collection<URI> dirsToFormat = NNStorageConfiguration.getNamespaceDirs(conf);
    Collection<URI> editDirsToFormat =
        NNStorageConfiguration.getNamespaceEditsDirs(conf);

    FSNamesystem nsys = new FSNamesystem(new FSImage(conf, dirsToFormat,
                                         editDirsToFormat, null), conf);
    try {
      if (!nsys.dir.fsImage.confirmFormat(force, isConfirmationNeeded)) {
        return true; // aborted
      }
      nsys.dir.fsImage.format();
      return false;
    } finally {
      nsys.close();
    }
  }

  static boolean finalize(Configuration conf,
                               boolean isConfirmationNeeded
                               ) throws IOException {
    Collection<URI> dirsToFormat = NNStorageConfiguration.getNamespaceDirs(conf);
    Collection<URI> editDirsToFormat =
        NNStorageConfiguration.getNamespaceEditsDirs(conf);
    FSNamesystem nsys = new FSNamesystem(new FSImage(conf, dirsToFormat,
                                         editDirsToFormat, null), conf);
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

  private static class StartupOptionAndService {
    private StartupOptionAndService(
        StartupOption startupOption, String serviceName,
        boolean failOnTxIdMismatch) {
      this.startupOption = startupOption;
      this.serviceName = serviceName;
      this.failOnTxIdMismatch = failOnTxIdMismatch;
    }
    private final StartupOption startupOption;
    private final String serviceName;
    private final boolean failOnTxIdMismatch;
  }

  private static StartupOptionAndService parseArguments(String args[]) {
    int argsLen = (args == null) ? 0 : args.length;
    StartupOption startOpt = StartupOption.REGULAR;
    String serviceName = null;
    boolean failOnTxIdMismatch = true;
    for(int i=0; i < argsLen; i++) {
      String cmd = args[i];
      if (StartupOption.SERVICE.getName().equalsIgnoreCase(cmd)) {
        if (++i < argsLen) {
          serviceName = args[i];
        } else {
          return null;
        }
      } else if (StartupOption.IGNORETXIDMISMATCH.getName().equalsIgnoreCase(cmd)) {
        failOnTxIdMismatch = false;
      } else if (StartupOption.FORMAT.getName().equalsIgnoreCase(cmd)) {
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
      } else {
        return null;
      }
    }
    return new StartupOptionAndService(startOpt, serviceName,
        failOnTxIdMismatch);
  }

  private static void setStartupOption(Configuration conf, StartupOption opt) {
    conf.set("dfs.namenode.startup", opt.toString());
  }

  static StartupOption getStartupOption(Configuration conf) {
    return StartupOption.valueOf(conf.get("dfs.namenode.startup",
                                          StartupOption.REGULAR.toString()));
  }

  /**
   * Valide if the input service name is valid
   * @param conf configuration
   * @param nameServiceId name service id
   * @return true if valid; false otherwise
   */
  public static boolean validateServiceName(Configuration conf,
      String nameServiceId) {
    Collection<String> nameserviceIds = DFSUtil.getNameServiceIds(conf);
    if (nameserviceIds != null && !nameserviceIds.isEmpty()) {
      if (nameServiceId == null) {
        System.err.println("Need to input a nameservice id");
        return false;
      } else if (!nameserviceIds.contains(nameServiceId)) {
        System.err.println("An invalid nameservice id: " + nameServiceId);
        return false;
      }
    } else if (nameServiceId != null) {
      System.err.println("An invalid nameservice id: " + nameServiceId);
      return false;
    }
    return true;
  }

  /**
   * Valide if the input service name is valid
   * @param conf configuration
   * @param nameServiceId name service id
   * @throws exception if service not present in the configuration
   */
  public static void checkServiceName(Configuration conf, String nameServiceId) {
    if (!validateServiceName(conf, nameServiceId)) {
      throw new IllegalArgumentException("Service Id doesn't match the config");
    }
  }

  public static NameNode createNameNode(String argv[],
                                 Configuration conf) throws IOException {
    if (conf == null)
      conf = new Configuration();
    StartupOptionAndService startOpt = parseArguments(argv);
    if (startOpt == null) {
      printUsage();
      return null;
    }

    if (!validateServiceName(conf, startOpt.serviceName)) {
      return null;
    }

    initializeGenericKeys(conf, startOpt.serviceName);
    setupDefaultURI(conf);

    setStartupOption(conf, startOpt.startupOption);

    switch (startOpt.startupOption) {
      case FORMAT:
        boolean aborted = format(conf, false, true);
        System.exit(aborted ? 1 : 0);
      case FINALIZE:
        aborted = finalize(conf, true);
        System.exit(aborted ? 1 : 0);
      default:
    }

    NameNode namenode = new NameNode(conf, startOpt.failOnTxIdMismatch);
    namenode.nameserviceId = startOpt.serviceName;
    return namenode;
  }

  /**
   */
  public static void main(String argv[]) throws Exception {
    org.apache.hadoop.hdfs.DnsMonitorSecurityManager.setTheManager();
    try {
      StringUtils.startupShutdownMessage(NameNode.class, argv, LOG);
      FastWritableHDFS.init();
      FastProtocolHDFS.init();
      NameNode namenode = createNameNode(argv, null);
      if (namenode != null)
        namenode.join();
    } catch (Throwable e) {
      LOG.error(StringUtils.stringifyException(e));
      System.exit(-1);
    }
  }

  private void reconfigureTrashIntervalProperty(String property, String newVal)
    throws ReconfigurationException{
    try {
      if (newVal == null) {
        // set to default
        trash.setDeleteInterval(60L * TrashPolicyDefault.MSECS_PER_MINUTE);
      } else {
        trash.setDeleteInterval((long)(
            Float.valueOf(newVal) * TrashPolicyDefault.MSECS_PER_MINUTE));
      }
      LOG.info("RECONFIGURE* changed trash deletion interval to " +
          newVal);
    } catch (NumberFormatException e) {
      throw new ReconfigurationException(property, newVal,
          getConf().get(property));
    }
  }

  private synchronized void reconfigureEnableHftp(String property, String newVal)
    throws ReconfigurationException {
    boolean newProperty = Boolean.valueOf(newVal);
    if(newProperty == this.currentEnableHftpProperty) {
      return;
    } else {
      this.currentEnableHftpProperty = newProperty;
      if(this.currentEnableHftpProperty) {
        InjectionHandler.processEvent(InjectionEvent.NAMENODE_RECONFIG_HFTP, true);
        this.httpServer.addInternalServlet("listPaths", "/listPaths/*",
            ListPathsServlet.class);
        this.httpServer.addInternalServlet("data", "/data/*",
            FileDataServlet.class);
        this.httpServer.addInternalServlet("checksum", "/fileChecksum/*",
            FileChecksumServlets.RedirectServlet.class);
      } else {
        InjectionHandler.processEvent(InjectionEvent.NAMENODE_RECONFIG_HFTP, false);
        this.httpServer.removeInternalServlet("listPaths", "/listPaths/*",
            ListPathsServlet.class);
        this.httpServer.removeInternalServlet("data", "/data/*",
            FileDataServlet.class);
        this.httpServer.removeInternalServlet("checksum", "/fileChecksum/*",
            FileChecksumServlets.RedirectServlet.class);
      }
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
    } else if ("fs.trash.interval".equals(property)) {
      reconfigureTrashIntervalProperty(property, newVal);
    } else if ("dfs.enableHftp".equals(property)) {
      reconfigureEnableHftp(property, newVal);
    } else {
      throw new ReconfigurationException(property, newVal,
                                         getConf().get(property));
    }
  }

  @Override
  /**
   * Removes NameServiceId from keys is the config.
   */
  public void preProcessConfiguration(Configuration config) {
    String serviceId = this.getNameserviceID();
    DFSUtil.setGenericConf(config, serviceId, NameNode.NAMESERVICE_SPECIFIC_KEYS);
  }

  @Override
  /**
   * Returns a configuration with NameServiceId suffixes removed.
   */
  public String preProcessKey(String key) {
    String serviceId = this.getNameserviceID();
    for (String nsSpecifyKey : NAMESERVICE_SPECIFIC_KEYS)
      if (key.equals(nsSpecifyKey + "." + serviceId))
        return nsSpecifyKey;
    return key;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public List<String> getReconfigurableProperties() {
    // only allow reconfiguration of namesystem's reconfigurable properties
    List<String> properties = namesystem.getReconfigurableProperties();
    properties.add("fs.trash.interval");
    properties.add("dfs.enableHftp");
    properties.add(FSConstants.DFS_NAMENODE_RPC_ADDRESS_KEY + "0");
    properties.add(FSConstants.DFS_NAMENODE_RPC_ADDRESS_KEY + "1");
    return properties;
  }

  @Override
  public void updatePipeline(String clientName, Block oldBlock,
      Block newBlock, DatanodeID[] datanodes) throws IOException {
    namesystem.updatePipeline(clientName, oldBlock, newBlock, safeAsList(datanodes));
  }

  @Override
  public int getDataTransferProtocolVersion() throws IOException {
    return DataTransferProtocol.DATA_TRANSFER_VERSION;
  }

  /**
   * For unit testing only, get Namespace ID
   * TODO temporary
   */
  public int getNamespaceID() {
    return namesystem.getNamespaceInfo().getNamespaceID();
  }

  /**
   * get the nameservice Id used for federation
   */
  public String getNameserviceID() {
    return this.nameserviceId;
  }

  /**
   * Whether or not we should fail on a txid mismatch.
   */
  public boolean failOnTxIdMismatch() {
    return this.failOnTxIdMismatch;
  }

  protected Map<NameNodeKey, String> getNameNodeSpecificKeys(){
    return new HashMap<NameNodeKey, String>();
  }

  protected boolean getIsPrimary() {
    return true;
  }

	@Override
	public LocatedBlockWithFileName getBlockInfo(long blockId)
			throws IOException {
    return getBlockInfoInternal(blockId);
  }

  private LocatedBlockWithFileName getBlockInfoInternal(long blockId)
      throws IOException {
		Block block = new Block(blockId);
		BlockInfo blockInfo = namesystem.blocksMap.getBlockInfo(block);
		if (null == blockInfo) {
			return null;
		}

		INodeFile inode = blockInfo.getINode();
		if (null == inode) {
			return null;
		}

		String fileName = inode.getFullPathName();
		// get the location info
		List<DatanodeInfo> diList = new ArrayList<DatanodeInfo>();
		for (Iterator<DatanodeDescriptor> it
				= namesystem.blocksMap.nodeIterator(block); it.hasNext();) {
			diList.add(it.next());
		}
		return new LocatedBlockWithFileName(block,
				diList.toArray(new DatanodeInfo[] {}), fileName);
	}

  @Override
  public String getClusterName() throws IOException {
    return this.clusterName;
  }

  @Override
  public void recount() throws IOException {
    namesystem.recount();
  }

  public boolean shouldRetryAbsentBlocks() {
    return false;
  }

  public boolean shouldRetryAbsentBlock(Block b, Block storedBlock) {
    return false;
  }

  @Override
  public RemoteEditLogManifest getEditLogManifest(long fromTxId)
      throws IOException {
    return this.namesystem.getEditLog().getEditLogManifest(fromTxId);
  }

  @Override
  public int register() throws IOException {
    InetAddress configuredRemoteAddress = getAddress(getConf().get(
        "dfs.secondary.http.address")).getAddress();
    validateCheckpointerAddress(configuredRemoteAddress);
    return DataTransferProtocol.DATA_TRANSFER_VERSION;
  }

  /**
   * Checks if the ip of the caller is equal to the given configured address.
   *
   * @param configuredRemoteAddress
   * @throws IOException
   */
  protected void validateCheckpointerAddress(InetAddress configuredRemoteAddress)
      throws IOException {
    InetAddress remoteAddress = Server.getRemoteIp();
    InjectionHandler.processEvent(InjectionEvent.NAMENODE_VERIFY_CHECKPOINTER,
        remoteAddress);

    LOG.info("Verify: received request from: " + remoteAddress);

    if (remoteAddress == null) {
      LOG.info("Verify: Remote address is NULL");
      throw new IOException("Verify: Remote address is null");
    }

    // if the address is not configured then skip checking
    if (configuredRemoteAddress == null
        || configuredRemoteAddress.equals(new InetSocketAddress("0.0.0.0", 0)
            .getAddress())) {
      LOG.info("Verify: Skipping check since the configured address is: "
          + configuredRemoteAddress);
      return;
    }

    // compare addresses
    if (!remoteAddress.equals(configuredRemoteAddress)) {
      String msg = "Verify: Configured standby is :"
          + configuredRemoteAddress + ", not allowing: " + remoteAddress
          + " to register";
      LOG.warn(msg);
      throw new IOException(msg);
    }
  }

  public long getTrashDeletionInterval() {
    return trash.getDeletionInterval();
  }

  void clearOutstandingNodes() {
    // Do nothing
  }

  public static <T> T[] safeToArray(List<T> list, T[] array) {
    return (list == null) ? null : list.toArray(array);
  }

  public static <T> List<T> safeAsList(T[] array) {
    return (array == null) ? null : Arrays.asList(array);
  }

  ///////////////////////////////////////
  // ClientProxyProtocol
  ///////////////////////////////////////

  @Override
  public BlockLocationsResponse getBlockLocations(GetBlockLocationsRequest req) throws IOException {
    Server.setOrignalCaller(req.getOriginalCaller());
    return new BlockLocationsResponse(getBlockLocationsInternal(req.getSrc(), req.getOffset(),
        req.getLength(), BlockMetaInfoType.NONE));
  }

  @Override
  public OpenResponse open(OpenRequest req) throws IOException {
    Server.setOrignalCaller(req.getOriginalCaller());
    return new OpenResponse((LocatedBlocksWithMetaInfo) getBlockLocationsInternal(req.getSrc(),
        req.getOffset(), req.getLength(), BlockMetaInfoType.VERSION_AND_NAMESPACEID));
  }

  @Override
  public void create(CreateRequest req) throws IOException {
    Server.setOrignalCaller(req.getOriginalCaller());
    createInternal(req.getSrc(), req.getMasked(), req.getClientName(), req.isOverwrite(),
        req.isCreateParent(), req.getReplication(), req.getBlockSize());
  }

  @Override
  public AppendResponse append(AppendRequest req) throws IOException {
    Server.setOrignalCaller(req.getOriginalCaller());
    return new AppendResponse((LocatedBlockWithOldGS) appendInternal(req.getSrc(),
        req.getClientName(), BlockMetaInfoType.VERSION_AND_NAMESPACEID_AND_OLD_GS));
  }

  @Override
  public void recoverLease(RecoverLeaseRequest req) throws IOException {
    Server.setOrignalCaller(req.getOriginalCaller());
    namesystem.recoverLease(req.getSrc(), req.getClientName(), NameNode.getClientMachine(), false);
  }

  @Override
  public boolean closeRecoverLease(CloseRecoverLeaseRequest req) throws IOException {
    Server.setOrignalCaller(req.getOriginalCaller());
    return namesystem.recoverLease(req.getSrc(), req.getClientName(), NameNode.getClientMachine(),
        req.isDiscardLastBlock());
  }

  @Override
  public boolean setReplication(SetReplicationRequest req) throws IOException {
    Server.setOrignalCaller(req.getOriginalCaller());
    return setReplicationInternal(req.getSrc(), req.getReplication());
  }

  @Override
  public void setPermission(SetPermissionRequest req) throws IOException {
    Server.setOrignalCaller(req.getOriginalCaller());
    setPermissionInternal(req.getSrc(), req.getPermission());
  }

  @Override
  public void setOwner(SetOwnerRequest req) throws IOException {
    Server.setOrignalCaller(req.getOriginalCaller());
    setOwnerInternal(req.getSrc(), req.getUsername(), req.getGroupname());
  }

  @Override
  public void abandonBlock(AbandonBlockRequest req) throws IOException {
    Server.setOrignalCaller(req.getOriginalCaller());
    abandonBlockInternal(req.getBlock(), req.getSrc(), req.getClientName());
  }

  @Override
  public void abandonFile(AbandonFileRequest req) throws IOException {
    Server.setOrignalCaller(req.getOriginalCaller());
    abandonFileInternal(req.getSrc(), req.getClientName());
  }

  @Override
  public AddBlockResponse addBlock(AddBlockRequest req) throws IOException {
    Server.setOrignalCaller(req.getOriginalCaller());
    return new AddBlockResponse((LocatedBlockWithMetaInfo) addBlockInternal(req.getSrc(),
        req.getClientName(), req.getExcludedNodes(), req.getFavoredNodes(), req.getStartPos(),
        req.getLastBlock(), BlockMetaInfoType.VERSION_AND_NAMESPACEID));
  }

  @Override
  public boolean complete(CompleteRequest req) throws IOException {
    Server.setOrignalCaller(req.getOriginalCaller());
    return completeInternal(req.getSrc(), req.getClientName(), req.getFileLen(),
        req.getLastBlock());
  }

  @Override
  public void reportBadBlocks(ReportBadBlocksRequest req) throws IOException {
    Server.setOrignalCaller(req.getOriginalCaller());
    reportBadBlocksInternal(req.getBlocks());
  }

  @Override
  public boolean hardLink(HardLinkRequest req) throws IOException {
    Server.setOrignalCaller(req.getOriginalCaller());
    return hardLinkInternal(req.getSrc(), req.getDst());
  }

  @Override
  public HardLinkedFilesResponse getHardLinkedFiles(GetHardLinkedFilesRequest req) throws
      IOException {
    Server.setOrignalCaller(req.getOriginalCaller());
    return new HardLinkedFilesResponse(safeAsList(namesystem.getHardLinkedFiles(req.getSrc())));
  }

  @Override
  public boolean rename(RenameRequest req) throws IOException {
    Server.setOrignalCaller(req.getOriginalCaller());
    return renameInternal(req.getSrc(), req.getDst());
  }

  @Override
  public void concat(ConcatRequest req) throws IOException {
    Server.setOrignalCaller(req.getOriginalCaller());
    namesystem.concat(req.getTrg(), safeToArray(req.getSrcs(), new String[0]), req.isRestricted());
  }

  @Override
  public boolean delete(DeleteRequest req) throws IOException {
    Server.setOrignalCaller(req.getOriginalCaller());
    return deleteInternal(req.getSrc(), req.isRecursive());
  }

  @Override
  public boolean mkdirs(MkdirsRequest req) throws IOException {
    Server.setOrignalCaller(req.getOriginalCaller());
    return mkdirsInternal(req.getSrc(), req.getMasked());
  }

  @Override
  public IterativeGetOpenFilesResponse iterativeGetOpenFiles(
      IterativeGetOpenFilesRequest req) throws IOException {
    Server.setOrignalCaller(req.getOriginalCaller());
    return new IterativeGetOpenFilesResponse(safeAsList(namesystem.iterativeGetOpenFiles(
        req.getSrc(), req.getMillis(), req.getStart())));
  }

  @Override
  public PartialListingResponse getPartialListing(GetPartialListingRequest req) throws IOException {
    Server.setOrignalCaller(req.getOriginalCaller());
    return new PartialListingResponse(getPartialListingInternal(req.getSrc(), req.getStartAfter(),
        false));
  }

  @Override
  public LocatedPartialListingResponse getLocatedPartialListing(
      GetLocatedPartialListingRequest req) throws IOException {
    Server.setOrignalCaller(req.getOriginalCaller());
    return new LocatedPartialListingResponse((LocatedDirectoryListing) getPartialListingInternal(
        req.getSrc(), req.getStartAfter(), true));
  }

  @Override
  public void renewLease(RenewLeaseRequest req) throws IOException {
    Server.setOrignalCaller(req.getOriginalCaller());
    renewLeaseInternal(req.getClientName());
  }

  @Override
  public StatsResponse getStats(GetStatsRequest req) throws IOException {
    Server.setOrignalCaller(req.getOriginalCaller());
    return new StatsResponse(namesystem.getStats());
  }

  @Override
  public long getPreferredBlockSize(GetPreferredBlockSizeRequest req) throws IOException {
    Server.setOrignalCaller(req.getOriginalCaller());
    return namesystem.getPreferredBlockSize(req.getSrc());
  }

  @Override
  public CorruptFileBlocksResponse listCorruptFileBlocks(ListCorruptFileBlocksRequest req) throws
      IOException {
    Server.setOrignalCaller(req.getOriginalCaller());
    return new CorruptFileBlocksResponse(listCorruptFileBlocksInternal(req.getSrc(),
        req.getCookie()));
  }

  @Override
  public FileInfoResponse getFileInfo(GetFileInfoRequest req) throws IOException {
    Server.setOrignalCaller(req.getOriginalCaller());
    return new FileInfoResponse(getHdfsFileInfoInternal(req.getSrc()));
  }

  @Override
  public ContentSummaryResponse getContentSummary(GetContentSummaryRequest req) throws IOException {
    Server.setOrignalCaller(req.getOriginalCaller());
    return new ContentSummaryResponse(getContentSummaryInternal(req.getSrc()));
  }

  @Override
  public void fsync(FSyncRequest req) throws IOException {
    Server.setOrignalCaller(req.getOriginalCaller());
    fsyncInternal(req.getSrc(), req.getClientName());
  }

  @Override
  public void setTimes(SetTimesRequest req) throws IOException {
    Server.setOrignalCaller(req.getOriginalCaller());
    setTimesInternal(req.getSrc(), req.getMtime(), req.getAtime());
  }

  @Override
  public void updatePipeline(UpdatePipelineRequest req) throws IOException {
    Server.setOrignalCaller(req.getOriginalCaller());
    namesystem.updatePipeline(req.getClientName(), req.getOldBlock(), req.getNewBlock(),
        req.getNewNodes());
  }

  @Override
  public int getDataTransferProtocolVersion(GetDataTransferProtocolVersionRequest req) throws
      IOException {
    Server.setOrignalCaller(req.getOriginalCaller());
    return DataTransferProtocol.DATA_TRANSFER_VERSION;
  }

  @Override
  public BlockInfoResponse getBlockInfo(GetBlockInfoRequest req) throws IOException {
    Server.setOrignalCaller(req.getOriginalCaller());
    return new BlockInfoResponse(getBlockInfoInternal(req.getBlockId()));
  }

  @Override
  public boolean raidFile(RaidFileRequest req) throws IOException {
    Server.setOrignalCaller(req.getOriginalCaller());
    return raidFileInternal(req.getSrc(), req.getCodecId(), req.getExpectedSourceReplication());
  }

  @Override
  public PingResponse ping(PingRequest req) throws IOException {
    return new PingResponse(System.currentTimeMillis());
  }
}
