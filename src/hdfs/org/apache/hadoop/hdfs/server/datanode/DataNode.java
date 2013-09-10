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
package org.apache.hadoop.hdfs.server.datanode;

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStream;
import java.lang.reflect.Proxy;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.net.URI;
import java.net.UnknownHostException;
import java.nio.channels.ClosedByInterruptException;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.AbstractList;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.management.ObjectName;
import javax.security.auth.login.LoginException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.ReconfigurableBase;
import org.apache.hadoop.conf.ReconfigurationException;
import org.apache.hadoop.conf.ReconfigurationServlet;
import org.apache.hadoop.hdfs.BlockRecoveryCoordinator;
import org.apache.hadoop.hdfs.BlockSyncer;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.FastProtocolHDFS;
import org.apache.hadoop.hdfs.FastWritableHDFS;
import org.apache.hadoop.hdfs.HDFSPolicyProvider;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.BlockPathInfo;
import org.apache.hadoop.hdfs.protocol.BlockListAsLongs;
import org.apache.hadoop.hdfs.protocol.ClientDatanodeProtocol;
import org.apache.hadoop.hdfs.protocol.DataTransferProtocol;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.FSConstants;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.WriteBlockHeader;
import org.apache.hadoop.hdfs.protocol.UnregisteredDatanodeException;
import org.apache.hadoop.hdfs.protocol.ProtocolCompatible;
import org.apache.hadoop.hdfs.server.common.CountingLogger;
import org.apache.hadoop.hdfs.server.common.CountingLogger.ErrorCounter;
import org.apache.hadoop.hdfs.server.common.HdfsConstants.StartupOption;
import org.apache.hadoop.hdfs.server.common.HdfsConstants;
import org.apache.hadoop.hdfs.server.common.GenerationStamp;
import org.apache.hadoop.hdfs.server.common.IncorrectVersionException;
import org.apache.hadoop.hdfs.server.common.Storage;
import org.apache.hadoop.hdfs.server.common.Storage.StorageDirectory;
import org.apache.hadoop.hdfs.server.common.Util;
import org.apache.hadoop.hdfs.server.datanode.FSDataset.FSVolume;
import org.apache.hadoop.hdfs.server.datanode.metrics.DataNodeMetrics;
import org.apache.hadoop.hdfs.server.datanode.metrics.DatanodeThreadLivenessReporter;
import org.apache.hadoop.hdfs.server.datanode.metrics.DatanodeThreadLivenessReporter.BackgroundThread;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.namenode.FileChecksumServlets;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.server.namenode.StreamFile;
import org.apache.hadoop.hdfs.server.protocol.BlockCommand;
import org.apache.hadoop.hdfs.server.protocol.BlockMetaDataInfo;
import org.apache.hadoop.hdfs.server.protocol.BlockRecoveryInfo;
import org.apache.hadoop.hdfs.server.protocol.BlockReport;
import org.apache.hadoop.hdfs.server.protocol.DatanodeCommand;
import org.apache.hadoop.hdfs.server.protocol.DatanodeProtocol;
import org.apache.hadoop.hdfs.server.protocol.DatanodeRegistration;
import org.apache.hadoop.hdfs.server.protocol.DisallowedDatanodeException;
import org.apache.hadoop.hdfs.server.protocol.InterDatanodeProtocol;
import org.apache.hadoop.hdfs.server.protocol.NamespaceInfo;
import org.apache.hadoop.hdfs.server.protocol.RaidTaskCommand;
import org.apache.hadoop.hdfs.server.protocol.ReceivedBlockInfo;
import org.apache.hadoop.hdfs.server.protocol.UpgradeCommand;
import org.apache.hadoop.hdfs.util.InjectionEvent;
import org.apache.hadoop.http.HttpServer;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.ipc.ProtocolSignature;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.metrics.util.MBeanUtil;
import org.apache.hadoop.net.DNS;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.ConfiguredPolicy;
import org.apache.hadoop.security.authorize.PolicyProvider;
import org.apache.hadoop.security.authorize.ServiceAuthorizationManager;
import org.apache.hadoop.util.Daemon;
import org.apache.hadoop.util.DataTransferThrottler;
import org.apache.hadoop.util.DiskChecker;
import org.apache.hadoop.util.InjectionHandler;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.DiskChecker.DiskErrorException;
import org.apache.hadoop.util.DiskChecker.DiskOutOfSpaceException;
import org.apache.hadoop.util.PulseChecker;
import org.apache.hadoop.util.PulseCheckable;
import org.apache.hadoop.util.VersionInfo;
import org.apache.hadoop.util.DataDirFileReader;
import org.mortbay.util.ajax.JSON;


/**********************************************************
 * DataNode is a class (and program) that stores a set of
 * blocks for a DFS deployment.  A single deployment can
 * have one or many DataNodes.  Each DataNode communicates
 * regularly with a single NameNode.  It also communicates
 * with client code and other DataNodes from time to time.
 *
 * DataNodes store a series of named blocks.  The DataNode
 * allows client code to read these blocks, or to write new
 * block data.  The DataNode may also, in response to instructions
 * from its NameNode, delete blocks or copy blocks to/from other
 * DataNodes.
 *
 * The DataNode maintains just one critical table:
 *   block-> stream of bytes (of BLOCK_SIZE or less)
 *
 * This info is stored on a local disk.  The DataNode
 * reports the table's contents to the NameNode upon startup
 * and every so often afterwards.
 *
 * DataNodes spend their lives in an endless loop of asking
 * the NameNode for something to do.  A NameNode cannot connect
 * to a DataNode directly; a NameNode simply returns values from
 * functions invoked by a DataNode.
 *
 * DataNodes maintain an open server socket so that client code 
 * or other DataNodes can read/write data.  The host/port for
 * this server is reported to the NameNode, which then sends that
 * information to clients or other DataNodes that might be interested.
 *
 **********************************************************/
public class DataNode extends ReconfigurableBase 
    implements InterDatanodeProtocol, ClientDatanodeProtocol, FSConstants, PulseCheckable,
    DataNodeMXBean {
  public static final CountingLogger LOG = new CountingLogger(LogFactory.getLog(DataNode.class));
  
  static{
    Configuration.addDefaultResource("hdfs-default.xml");
    Configuration.addDefaultResource("hdfs-site.xml");
  }

  public static final String DN_CLIENTTRACE_FORMAT =
        "src: %s" +      // src IP
        ", dest: %s" +   // dst IP
        ", bytes: %s" +  // byte count
        ", op: %s" +     // operation
        ", cliID: %s" +  // DFSClient id
        ", offset: %s" + // offset
        ", srvID: %s" +  // DatanodeRegistration
        ", blockid: %s" + // block id
        ", duration: %s"; // duration time

  public static final Log ClientTraceLog =
    LogFactory.getLog(DataNode.class.getName() + ".clienttrace");

  private static Random cachedSecureRandom; 
  
  // get an instance of a SecureRandom for creating storageid
  public synchronized static Random getSecureRandom() {
    if (cachedSecureRandom != null)
      return cachedSecureRandom;
    try {
      return SecureRandom.getInstance("SHA1PRNG");
    } catch (NoSuchAlgorithmException e) {
      return R;
    }
  }
  
  // used for testing to speed-up startup
  public synchronized static void setSecureRandom(Random rand) {
    cachedSecureRandom = rand;
  }
  
  /**
   * Use {@link NetUtils#createSocketAddr(String)} instead.
   */
  @Deprecated
  public static InetSocketAddress createSocketAddr(String target
                                                   ) throws IOException {
    return NetUtils.createSocketAddr(target);
  }

  /** Sets given ErrorCounter in all CountingLoggers in DataNode's proximity */
  private static void setCountingLoggers(ErrorCounter metrics) {
    DataNode.LOG.setCounter(metrics);
  }

  public FSDatasetInterface data = null;
  
  //TODO this should be not used at all
  private static InetSocketAddress nameNodeAddr;
  public static int NAMESPACE_ID = 12345678;

  volatile boolean shouldRun = true;
  boolean isAlive = false;
  
  protected NamespaceManager namespaceManager;
  
  /** list of blocks being recovered */
  private final Map<Block, Block> ongoingRecovery = new HashMap<Block, Block>();
  AtomicInteger xmitsInProgress = new AtomicInteger();
  AtomicBoolean shuttingDown = new AtomicBoolean(false);
  AtomicBoolean checkingDisk = new AtomicBoolean(false);
  volatile long timeLastCheckDisk = 0;
  long minDiskCheckIntervalMsec;
  Daemon dataXceiverServer = null;
  ThreadGroup threadGroup = null;
  long blockReportInterval;
  long deletedReportInterval;
  long initialBlockReportDelay = BLOCKREPORT_INITIAL_DELAY * 1000L;
  long heartBeatInterval;
  DataStorage storage = null;
  HttpServer infoServer = null;
  DataNodeMetrics myMetrics;
  DatanodeThreadLivenessReporter threadLivenessReporter;
  
  protected InetSocketAddress selfAddr;
  String machineName;
  static String dnThreadName;
  int socketTimeout;
  int socketReadExtentionTimeout;
  int socketWriteTimeout = 0;
  int socketWriteExtentionTimeout = 0;  
  boolean transferToAllowed = true;
  boolean ignoreChecksumWhenRead = false;
  boolean updateBlockCrcWhenRead = true;
  boolean updateBlockCrcWhenWrite = true;
  long thresholdLogReadDuration;
  int writePacketSize = 0;
  boolean syncOnClose;
  boolean supportAppends;
  public boolean useInlineChecksum;
  long heartbeatExpireInterval;
  final long dataTransferMaxRate;
  long blkRecoveryTimeout;
  // heartbeatExpireInterval is how long namenode waits for datanode to report
  
  /**
   * Testing hook that allows tests to delay the sending of blockReceived
   * RPCs to the namenode. This can help find bugs in append.
   */
  int artificialBlockReceivedDelay = 0;

  public DataBlockScannerSet blockScanner = null;
  public DirectoryScanner directoryScanner = null;
  
  private static final String CONF_SERVLET_PATH = "/dnconf";
  
  private static final Random R = new Random();
  
  // For InterDataNodeProtocol
  public Server ipcServer;

  private final ExecutorService blockCopyExecutor;
  public static final int BLOCK_COPY_THREAD_POOL_SIZE = 10;
  
  public static final String DFS_DATANODE_SOCKET_REUSE_KEEPALIVE_KEY = 
        "dfs.datanode.socket.reuse.keepalive";
  public static final int DFS_DATANODE_SOCKET_REUSE_KEEPALIVE_DEFAULT = 1000;

  private final int blockCopyRPCWaitTime;
  AbstractList<File> dataDirs;
  Configuration conf;
  private PulseChecker pulseChecker;

  /**
   * Current system time.
   * @return current time in msec.
   */
  static long now() {
    return System.currentTimeMillis();
  }

  /**
   * Create the DataNode given a configuration and an array of dataDirs.
   * 'dataDirs' is where the blocks are stored.
   */
  DataNode(Configuration conf, 
          AbstractList<File> dataDirs) throws IOException {
   super(conf);
   supportAppends = conf.getBoolean("dfs.support.append", false);
   useInlineChecksum = conf.getBoolean(DFS_USE_INLINE_CHECKSUM_KEY, true);
   // Update Block CRC if needed when read
   updateBlockCrcWhenRead = conf.getBoolean("dfs.update.blockcrc.when.read", true);
   updateBlockCrcWhenWrite = conf.getBoolean("dfs.update.blockcrc.when.write", true);
   thresholdLogReadDuration = conf.getLong(
        "dfs.threshold.log.read.latency.ms", 200);
    dataTransferMaxRate = conf.getLong(
        "dfs.data.transfer.max.bytes.per.sec", -1);
    
   // TODO(pritam): Integrate this into a threadpool for all operations of the
   // datanode.
   blockCopyExecutor = Executors.newCachedThreadPool();

   // Time that the blocking version of  RPC for copying block between
   // datanodes should wait for. Default is 5 minutes.
   blockCopyRPCWaitTime = conf.getInt("dfs.datanode.blkcopy.wait_time",
       5 * 60);
   try {
     startDataNode(this.getConf(), dataDirs);
   } catch (IOException ie) {
     LOG.info("Failed to start datanode " + StringUtils.stringifyException(ie));
     shutdown();
     throw ie;
   }
 }

  /**
   * Initialize global settings for DN
   */
  protected void initGlobalSetting(Configuration conf,
      AbstractList<File> dataDirs) throws IOException {
    this.dataDirs = dataDirs;
    this.conf = conf;
    storage = new DataStorage(this);
    
    // global DN settings
    initConfig(conf);
    registerMXBean();
    initDataXceiver(conf);
    startInfoServer(conf);
    initIpcServer(conf);

    myMetrics = new DataNodeMetrics(conf, storage.getStorageID());
    setCountingLoggers(myMetrics);
    threadLivenessReporter = new DatanodeThreadLivenessReporter(conf.getLong(
        "dfs.datanode.thread.liveness.threshold", 240 * 1000),
        myMetrics.threadActiveness);
  }
  
  /**
   * Initialize dataset and block scanner
   * 
   * @param conf  Configuration
   * @param dataDirs data directories
   * @param numOfNameSpaces number of name spaces
   * @throws IOException
   */
  protected void initDataSetAndScanner(Configuration conf,
      AbstractList<File> dataDirs, int numOfNameSpaces) throws IOException {
    initFsDataSet(conf, dataDirs, numOfNameSpaces);
    initDataBlockScanner(conf); 
    initDirectoryScanner(conf);
  }
  
  private synchronized void initDirectoryScanner(Configuration conf) {
    if (directoryScanner != null) {
      return;
    }
    String reason = null;
    if (conf.getInt(DirectoryScanner.DFS_DATANODE_DIRECTORYSCAN_INTERVAL_KEY, 
        DirectoryScanner.DFS_DATANODE_DIRECTORYSCAN_INTERVAL_DEFAULT) < 0) {
      reason = "verification is turned off by configuration";
    } else if (!(data instanceof FSDataset)) {
      reason = "verification is supported only with FSDataset";
    } 
    if (reason == null) {
      directoryScanner = new DirectoryScanner(this, (FSDataset) data, conf);
      directoryScanner.start();
    } else {
      LOG.info("Periodic Directory Tree Verification scan is disabled because " +
               reason + ".");
    }
  }
  
  private synchronized void shutdownDirectoryScanner() {
    if (directoryScanner != null) {
      directoryScanner.shutdown();
    }
  }
  
  /**
   * This method starts the data node with the specified conf.
   * 
   * @param conf - the configuration
   *  if conf's CONFIG_PROPERTY_SIMULATED property is set
   *  then a simulated storage based data node is created.
   * 
   * @param dataDirs - only for a non-simulated storage data node
   * @throws IOException
   */
  void startDataNode(Configuration conf, 
                     AbstractList<File> dataDirs
                     ) throws IOException {
    initGlobalSetting(conf, dataDirs);
    
    /* Initialize namespace manager */
    List<InetSocketAddress> nameNodeAddrs = DFSUtil.getNNServiceRpcAddresses(conf);
    
    //TODO this will be no longer valid, since we will have multiple namenodes
    // We might want to keep it and assign the first NN to it.
    DataNode.nameNodeAddr = nameNodeAddrs.get(0); 
    namespaceManager = new NamespaceManager(conf, nameNodeAddrs);
  
    initDataSetAndScanner(conf, dataDirs, nameNodeAddrs.size());
  }
  
  private void initConfig(Configuration conf) throws IOException {
    if (conf.get(FSConstants.SLAVE_HOST_NAME) != null) {
      machineName = conf.get(FSConstants.SLAVE_HOST_NAME);
    }
    if (machineName == null) {
      machineName = DNS.getDefaultIP(conf.get(FSConstants.DFS_DATANODE_DNS_INTERFACE, "default"));
    } else {
      // Ensuring it's an IP address
      machineName = NetUtils.normalizeHostName(machineName);
    }
    // Allow configuration to delay block reports to find bugs
    artificialBlockReceivedDelay = conf.getInt(
      "dfs.datanode.artificialBlockReceivedDelay", 0);
    if (conf.getBoolean(
        ServiceAuthorizationManager.SERVICE_AUTHORIZATION_CONFIG, false)) {
      PolicyProvider policyProvider = (PolicyProvider) (ReflectionUtils
          .newInstance(conf.getClass(PolicyProvider.POLICY_PROVIDER_CONFIG,
              HDFSPolicyProvider.class, PolicyProvider.class), conf));
      SecurityUtil.setPolicy(new ConfiguredPolicy(conf, policyProvider));
    }
    this.socketTimeout = conf.getInt("dfs.socket.timeout",
        HdfsConstants.READ_TIMEOUT);
    this.socketReadExtentionTimeout = conf.getInt(
        HdfsConstants.DFS_DATANODE_READ_EXTENSION,
        HdfsConstants.READ_TIMEOUT_EXTENSION);
    this.socketWriteTimeout = conf.getInt("dfs.datanode.socket.write.timeout",
        HdfsConstants.WRITE_TIMEOUT);
    this.socketWriteExtentionTimeout = conf.getInt(
        HdfsConstants.DFS_DATANODE_WRITE_EXTENTSION,
        HdfsConstants.WRITE_TIMEOUT_EXTENSION);
    
    /* Based on results on different platforms, we might need set the default 
     * to false on some of them. */
    this.transferToAllowed = conf.getBoolean("dfs.datanode.transferTo.allowed",
                                             true);

    // TODO: remove the global setting and change data protocol to support
    // per session setting for this value.
    this.ignoreChecksumWhenRead = conf.getBoolean("dfs.datanode.read.ignore.checksum",
        false);

    this.writePacketSize = conf.getInt("dfs.write.packet.size",
        HdfsConstants.DEFAULT_PACKETSIZE);
    
    this.deletedReportInterval =
      conf.getLong("dfs.blockreport.intervalMsec", BLOCKREPORT_INTERVAL);
    // Calculate the full block report interval
    int fullReportMagnifier = conf.getInt("dfs.fullblockreport.magnifier", 2);
    this.blockReportInterval = fullReportMagnifier * deletedReportInterval;
    this.heartBeatInterval = conf.getLong("dfs.heartbeat.interval", HEARTBEAT_INTERVAL) * 1000L;
    long heartbeatRecheckInterval = conf.getInt(
        "heartbeat.recheck.interval", 5 * 60 * 1000); // 5 minutes
    this.heartbeatExpireInterval = 2 * heartbeatRecheckInterval +
        10 * heartBeatInterval;
    this.blkRecoveryTimeout = conf.getLong(
        DFS_NAMENODE_CHECKPOINT_EDITS_DIR_KEY,
        DFS_BLK_RECOVERY_BY_NN_TIMEOUT_DEFAULT);
    
    this.initialBlockReportDelay = conf.getLong("dfs.blockreport.initialDelay",
        BLOCKREPORT_INITIAL_DELAY) * 1000L;
    if (this.initialBlockReportDelay >= blockReportInterval) {
      this.initialBlockReportDelay = 0;
      LOG.info("dfs.blockreport.initialDelay is greater than "
          + "dfs.blockreport.intervalMsec."
          + " Setting initial delay to 0 msec:");
    }

    // do we need to sync block file contents to disk when blockfile is closed?
    this.syncOnClose = conf.getBoolean("dfs.datanode.synconclose", false);
    
    this.minDiskCheckIntervalMsec = conf.getLong(
        "dfs.datnode.checkdisk.mininterval",
        FSConstants.MIN_INTERVAL_CHECK_DIR_MSEC);
  }
  
  /**
   * Used only for testing.
   * 
   * @param name
   *          the new name for datanode registration.
   */
  public void setRegistrationName(String name) {
    NamespaceService[] nsos = namespaceManager.getAllNamenodeThreads();
    for (NamespaceService ns : nsos) {
      ((NSOfferService) ns).setRegistrationName(name);
    }
  }

  
  private void initDataXceiver(Configuration conf) throws IOException {
    String address = 
      NetUtils.getServerAddress(conf,
                        "dfs.datanode.bindAddress",
                        "dfs.datanode.port",
                        FSConstants.DFS_DATANODE_ADDRESS_KEY);
    InetSocketAddress socAddr = NetUtils.createSocketAddr(address);
    // find free port
    ServerSocket ss = (socketWriteTimeout > 0) ? 
          ServerSocketChannel.open().socket() : new ServerSocket();
    Server.bind(ss, socAddr, 
        conf.getInt("dfs.datanode.xceiver.listen.queue.size", 128));
    ss.setReceiveBufferSize(DEFAULT_DATA_SOCKET_SIZE); 
    // adjust machine name with the actual port
    int tmpPort = ss.getLocalPort();
    selfAddr = new InetSocketAddress(ss.getInetAddress().getHostAddress(),
                                     tmpPort);
    LOG.info("Opened info server at " + tmpPort);
      
    this.threadGroup = new ThreadGroup("dataXceiverServer");
    this.dataXceiverServer = new Daemon(threadGroup, 
        new DataXceiverServer(ss, conf, this));
    this.threadGroup.setDaemon(true); // auto destroy when empty
  }
  
  private void startInfoServer(Configuration conf) throws IOException {
    String infoAddr = 
      NetUtils.getServerAddress(conf, 
                              "dfs.datanode.info.bindAddress", 
                              "dfs.datanode.info.port",
                              "dfs.datanode.http.address");
    InetSocketAddress infoSocAddr = NetUtils.createSocketAddr(infoAddr);
    String infoHostIp = infoSocAddr.getAddress().getHostAddress();
    int tmpInfoPort = infoSocAddr.getPort();
    this.infoServer = new HttpServer("datanode", infoHostIp, tmpInfoPort, tmpInfoPort == 0, conf);
    if (conf.getBoolean("dfs.https.enable", false)) {
      boolean needClientAuth = conf.getBoolean("dfs.https.need.client.auth", false);
      InetSocketAddress secInfoSocAddr = NetUtils.createSocketAddr(conf.get(
          "dfs.datanode.https.address", infoHostIp + ":" + 0));
      Configuration sslConf = new Configuration(false);
      sslConf.addResource(conf.get("dfs.https.server.keystore.resource",
          "ssl-server.xml"));
      this.infoServer.addSslListener(secInfoSocAddr, sslConf, needClientAuth);
      // assume same ssl port for all datanodes
      InetSocketAddress datanodeSslPort = NetUtils.createSocketAddr(conf.get(
          "dfs.datanode.https.address", infoHostIp + ":" + 50475));
      this.infoServer.setAttribute("datanode.https.port", datanodeSslPort
          .getPort());
    }
    this.infoServer.addInternalServlet(null, "/streamFile/*", StreamFile.class);
    this.infoServer.addInternalServlet(null, "/getFileChecksum/*",
        FileChecksumServlets.GetServlet.class);
    this.infoServer.setAttribute("datanode", this);
    this.infoServer.addServlet(null, "/blockScannerReport", 
                               DataBlockScannerSet.Servlet.class);

    this.infoServer.setAttribute(ReconfigurationServlet.CONF_SERVLET_RECONFIGURABLE_PREFIX +
    CONF_SERVLET_PATH, DataNode.this);
    this.infoServer.addServlet("dnConf", CONF_SERVLET_PATH, ReconfigurationServlet.class);
    this.infoServer.start();
  }
  
  private void initIpcServer(Configuration conf) throws IOException {
    //init ipc server
    InetSocketAddress ipcAddr = NetUtils.createSocketAddr(
        conf.get(DFS_DATANODE_IPC_ADDRESS_KEY));
    ipcServer = RPC.getServer(this, ipcAddr.getAddress().getHostAddress(), ipcAddr.getPort(),
        conf.getInt("dfs.datanode.handler.count", 3), false, conf, false);
    ipcServer.start();
  }

  /**
   * Creates either NIO or regular depending on socketWriteTimeout.
   */
  protected Socket newSocket() throws IOException {
   return (socketWriteTimeout > 0) ?
          SocketChannel.open().socket() : new Socket();
  }

  public boolean isSupportAppends() {
    return supportAppends;
  }

  public static InterDatanodeProtocol createInterDataNodeProtocolProxy(
      DatanodeID datanodeid, Configuration conf, final int socketTimeout)
    throws IOException {
    InetSocketAddress addr = NetUtils.createSocketAddr(
	datanodeid.getHost() + ":" + datanodeid.getIpcPort());
    if (InterDatanodeProtocol.LOG.isDebugEnabled()) {
      InterDatanodeProtocol.LOG.info("InterDatanodeProtocol addr=" + addr);
    }
    UserGroupInformation ugi;
    try {
      ugi = UserGroupInformation.login(conf);
    } catch (LoginException le) {
      throw new RuntimeException("Couldn't login!");
    }
    return (InterDatanodeProtocol)RPC.getProxy(InterDatanodeProtocol.class,
        InterDatanodeProtocol.versionID, addr,
        ugi, conf,
        NetUtils.getDefaultSocketFactory(conf), socketTimeout);
  }

  /**
   * This method returns the address namenode uses to communicate with
   * datanodes. If this address is not configured the default NameNode
   * address is used, as it is running only one RPC server.
   * If it is running multiple servers this address cannot be used by clients!!
   * 
   * Note: If DataNodeProtocolAddress isn't configured,  we fallback to 
   * ClientProtocolAddress.
   * @param conf
   */
  public static InetSocketAddress getNameNodeAddress(Configuration conf) {
    InetSocketAddress addr = null;
    addr = NameNode.getDNProtocolAddress(conf);
    if (addr != null) {
      return addr;
    }
    return NameNode.getClientProtocolAddress(conf);
  }

  //TODO this should not be there -> it affects StreamFile class
  public InetSocketAddress getNameNodeAddr() {
    return nameNodeAddr;
  }
  
  /**
   * Get namenode corresponding to a namespace
   * @param namespaceId 
   * @return Namenode corresponding to the namespace
   * @throws IOException
   */
  public DatanodeProtocol getNSNamenode(int namespaceId) throws IOException {
    NamespaceService nsos = namespaceManager.get(namespaceId);
    if(nsos == null || nsos.getDatanodeProtocol() == null) {
      throw new IOException("cannot find a namnode proxy for namespaceId=" + namespaceId);
    }
    return nsos.getDatanodeProtocol();
  }

  public InetSocketAddress getSelfAddr() {
    return selfAddr;
  }
  
  public int getPort() {
    return selfAddr.getPort();
  }

  DataNodeMetrics getMetrics() {
    return myMetrics;
  }
  
  /**
   * get datanode registration by namespace id
   * @param namespaceId
   * @return datanode registration object
   * @throws IOException
   */
  public DatanodeRegistration getDNRegistrationForNS(int namespaceId) 
  throws IOException {
    NamespaceService nsos = namespaceManager.get(namespaceId);
    if(nsos==null || nsos.getNsRegistration()==null) {
      throw new IOException("cannot find NSOfferService for namespaceId="+namespaceId);
    }
    return nsos.getNsRegistration();
  }

  /**
   * Return the namenode's identifier
   */
  public String getNamenode() {
    //return namenode.toString();
    return "<namenode>";
  }
  
  public static void setNewStorageID(DatanodeRegistration dnReg) {
    LOG.info("Datanode is " + dnReg);
    dnReg.storageID = createNewStorageId(dnReg.getPort());
  }

  public static String createNewStorageId(int port) {
    /* Return 
     * "DS-randInt-ipaddr-currentTimeMillis"
     * It is considered extermely rare for all these numbers to match
     * on a different machine accidentally for the following 
     * a) SecureRandom(INT_MAX) is pretty much random (1 in 2 billion), and
     * b) Good chance ip address would be different, and
     * c) Even on the same machine, Datanode is designed to use different ports.
     * d) Good chance that these are started at different times.
     * For a confict to occur all the 4 above have to match!.
     * The format of this string can be changed anytime in future without
     * affecting its functionality.
     */
    String ip = "unknownIP";
    try {
      ip = DNS.getDefaultIP("default");
    } catch (UnknownHostException ignored) {
      LOG.warn("Could not find ip address of \"default\" inteface.");
    }

    int rand = getSecureRandom().nextInt(Integer.MAX_VALUE);
    return "DS-" + rand + "-"+ ip + "-" + port + "-" + 
                      System.currentTimeMillis();
  }

  /**
   * Shut down this instance of the datanode.
   * Returns only after shutdown is complete.
   * This method can only be called by the offerService thread.
   * Otherwise, deadlock might occur.
   */
  public void shutdown() {
    if (this.shuttingDown.getAndSet(true)) {
      // Already being shut down
      LOG.warn("DataNode.shutdown() was called while shutting down.");
      return;
    }
    if (infoServer != null) {
      try {
        infoServer.stop();
      } catch (Exception e) {
        LOG.warn("Exception shutting down DataNode", e);
      }
    }
    if (ipcServer != null) {
      ipcServer.stop();
    }
    this.shouldRun = false;
    if (dataXceiverServer != null) {
      ((DataXceiverServer) this.dataXceiverServer.getRunnable()).kill();
      this.dataXceiverServer.interrupt();

      // wait for all data receiver threads to exit
      if (this.threadGroup != null) {
        int retries = 0;
        while (true) {
          this.threadGroup.interrupt();
          LOG.info("Waiting for threadgroup to exit, active threads is " +
                   this.threadGroup.activeCount());
          if (this.threadGroup.activeCount() == 0) {
            break;
          }
          try {
            if (++retries > 600) {
              Thread[] activeThreads = new Thread[this.threadGroup.activeCount()];
              this.threadGroup.enumerate(activeThreads, true);
              LOG.info("Active Threads: " + Arrays.toString(activeThreads));
              LOG.warn("Waited for ThreadGroup to be empty for 10 minutes." + 
                        " SHUTTING DOWN NOW");
              break;
            }
            Thread.sleep(1000);
          } catch (InterruptedException e) {}
        }
      }
      // wait for dataXceiveServer to terminate
      try {
        this.dataXceiverServer.join();
      } catch (InterruptedException ie) {
      }
    }
    
    if (blockCopyExecutor != null && !blockCopyExecutor.isShutdown()) {
      blockCopyExecutor.shutdownNow();
    }
    
    if (namespaceManager != null) {
      namespaceManager.shutDownAll();
    }
    
    if (blockScanner != null) { 
      blockScanner.shutdown();
    }
    if(directoryScanner != null) {
      shutdownDirectoryScanner();
    }
    if (storage != null) {
      try {
        this.storage.unlockAll();
      } catch (IOException ie) {
      }
    }
    if (data != null) {
      data.shutdown();
    }
    if (myMetrics != null) {
      setCountingLoggers(null);
      myMetrics.shutdown();
    }
    this.shutdownMXBean();
  }
  

  /** Check if there is no space in disk 
   *  @param e that caused this checkDiskError call
   **/
  protected void checkDiskError(Exception e ) throws IOException {
    if (e instanceof ClosedByInterruptException
        || e instanceof java.io.InterruptedIOException) {
      return;
    }
    LOG.warn("checkDiskError: exception: ", e);
    
    if (e.getMessage() != null &&
        e.getMessage().startsWith("No space left on device")) {
      throw new DiskOutOfSpaceException("No space left on device");
    } else {
      checkDiskError();
    }
  }
  
  /**
   *  Check if there is a disk failure and if so, handle the error
   *
   **/
  protected void checkDiskError( ) throws IOException{
    // We disallow concurrent disk checks as it doesn't help
    // but can significantly impact performance and reliability of
    // the system.
    //
    boolean setSuccess = checkingDisk.compareAndSet(false, true);
    if (!setSuccess) {
      LOG.info("checkDiskError is already running.");
      return;
    }

    try {
      // We don't check disks if it's not long since last check.
      //
      long curTime = System.currentTimeMillis();
      if (curTime - timeLastCheckDisk < minDiskCheckIntervalMsec) {
        LOG.info("checkDiskError finished within "
            + minDiskCheckIntervalMsec + " mses. Skip this one.");
        return;
      }
      data.checkDataDir();
      timeLastCheckDisk = System.currentTimeMillis();
    } catch(DiskErrorException de) {
      handleDiskError(de.getMessage());
    } finally {
      checkingDisk.set(false);
    }
  }
  
  private void handleDiskError(String errMsgr) throws IOException{
    boolean hasEnoughResource = data.hasEnoughResource();
    myMetrics.volumeFailures.inc();
    for(Integer namespaceId : namespaceManager.getAllNamespaces()){
      DatanodeProtocol nn = getNSNamenode(namespaceId);
      LOG.warn("DataNode.handleDiskError: Keep Running: " + hasEnoughResource);
      
      //if hasEnoughtResource = true - more volumes are available, so we don't want 
      // to shutdown DN completely and don't want NN to remove it.
      int dp_error = DatanodeProtocol.DISK_ERROR;
      if(hasEnoughResource == false) {
        // DN will be shutdown and NN should remove it
        dp_error = DatanodeProtocol.FATAL_DISK_ERROR;
      }
      //inform NameNode
      try {
        nn.errorReport(getDNRegistrationForNS(namespaceId), dp_error, errMsgr);
      } catch(IOException ignored) {              
      }
      
      
      if(hasEnoughResource) {
        for (NamespaceService nsos : namespaceManager.getAllNamenodeThreads()) {
          nsos.scheduleBlockReport(0);
        }
        return; // do not shutdown
      }
    }
    
    LOG.warn("DataNode is shutting down.\n" + errMsgr);
    shouldRun = false; 
  }
  
  private void refreshVolumes(String confVolumes) throws Exception {
    if( !(data instanceof FSDataset)) {
      throw new UnsupportedOperationException("Only FSDataset support refresh volumes operation");
    }

    // Dirs described by conf file
    Configuration conf = getConf();

    //temporary set dfs.data.dir for get storageDirs
    String oldVolumes = conf.get("dfs.data.dir");
    conf.set("dfs.data.dir", confVolumes);
    Collection<URI> dataDirs = getStorageDirs(conf);
    conf.set("dfs.data.dir", oldVolumes);
    
    ArrayList<File> newDirs = getDataDirsFromURIs(dataDirs);
    //This is used to pass a list of directories for the datanode to decommission.
    ArrayList<File> decomDirs = new ArrayList<File>();
    //Used to store storage directories that are no longer needed.
    ArrayList<StorageDirectory> decomStorage = new ArrayList<StorageDirectory>();
    
    for (Iterator<StorageDirectory> storageIter = this.storage.dirIterator();
        storageIter.hasNext();) {
      StorageDirectory dir = storageIter.next();
      
      // Delete volumes not in service from DataStorage
      if (!((FSDataset)data).isValidVolume(dir.getCurrentDir())) {
        LOG.info("This dir is listed in conf, but not in service " + dir.getRoot());
        storageIter.remove();
        continue;
      }
  
      if (newDirs.contains(dir.getRoot().getAbsoluteFile())){
        // remove the dir already in-service in newDirs list
        LOG.info("This conf dir has already been in service " + dir.getRoot().getAbsoluteFile());
        newDirs.remove(dir.getRoot().getAbsoluteFile());
      } else {
        LOG.info("This dir should not be in service and is not in the config. Removing: " + 
          dir.getRoot());
        // add the dirs not described in conf files to decomDirs
        decomDirs.add(dir.getRoot().getAbsoluteFile());
        storageIter.remove();
        decomStorage.add(dir);
      }
    }
    if (newDirs.isEmpty()){
      LOG.info("All the configured dirs are already in service");
    } else {
      for (int namespaceId: namespaceManager.getAllNamespaces()) {
        // Load new volumes via DataStorage
        NamespaceInfo nsInfo = getNSNamenode(namespaceId).versionRequest();
        String nameserviceId = this.namespaceManager.get(namespaceId).getNameserviceId();
        Collection<StorageDirectory> newStorageDirectories =
          storage.recoverTransitionAdditionalRead(nsInfo, newDirs, getStartupOption(conf));
        storage.recoverTransitionRead(this, namespaceId, nsInfo, newDirs, 
          getStartupOption(conf), nameserviceId);
       
        // add new volumes in FSDataSet
        ((FSDataset)data).addVolumes(conf, namespaceId, 
          storage.getNameSpaceDataDir(namespaceId), newStorageDirectories);
      }
    }
    if (!decomDirs.isEmpty()) {
      LOG.info("Removing decommissioned directories from FSDataset");
      try {
        ((FSDataset)data).removeVolumes(conf, decomDirs);
      } catch (DiskErrorException de) {
        handleDiskError(de.getMessage());
      }
      for (StorageDirectory sDir : decomStorage) {
        try {
          LOG.info("Unlocking Storage Directory: " + sDir.getRoot());
          sDir.unlock();
        } catch (IOException e) {
          LOG.error("Unable to unlock Storage Directory: " + sDir.getRoot());
        }
      }
    }
    LOG.info("Finished refreshing volumes");  
  }
    
  /** Number of concurrent xceivers per node. */
  int getXceiverCount() {
    return threadGroup == null ? 0 : threadGroup.activeCount();
  }

  static Collection<URI> getStorageDirs(Configuration conf) {
    Collection<String> dirNames =
      conf.getStringCollection("dfs.data.dir");
    return Util.stringCollectionAsURIs(dirNames);
  }

  static ArrayList<File> getDataDirsFromURIs(Collection<URI> dataDirs) {
    ArrayList<File> dirs = new ArrayList<File>();
    for (URI dirURI : dataDirs) {
      if (!"file".equalsIgnoreCase(dirURI.getScheme())) {
        LOG.warn("Unsupported URI schema in " + dirURI + ". Ignoring ...");
        continue;
      }
      // drop any (illegal) authority in the URI for backwards compatibility
      File data = new File(dirURI.getPath());
      try {
        DiskChecker.checkDir(data);
        dirs.add(data);
      } catch (IOException e) {
        LOG.warn("Invalid directory in dfs.data.dir: "
                 + e.getMessage());
      }
    }
    return dirs;
  }

  /**
   * A thread per namenode to perform:
   * <ul>
   * <li> Pre-registration handshake with namenode</li>
   * <li> Registration with namenode</li>
   * <li> Send periodic heartbeats to the namenode</li>
   * <li> Handle commands received from the datanode</li>
   * </ul>
   */
  class NSOfferService extends NamespaceService {
    final InetSocketAddress nnAddr;
    DatanodeRegistration nsRegistration;
    NamespaceInfo nsInfo;
    long lastBlockReport = 0;
    private Thread nsThread;
    private DatanodeProtocol nsNamenode;
    int namespaceId;
    String nameserviceId;
    private long lastHeartbeat = 0;
    private long lastDeletedReport = 0;
    boolean resetBlockReportTime = true;
    private volatile boolean initialized = false;
    private final LinkedList<Block> receivedAndDeletedBlockList 
      = new LinkedList<Block>();
    private int pendingReceivedRequests = 0;
    private volatile boolean shouldServiceRun = true;
    UpgradeManagerDatanode upgradeManager = null;
    private ScheduledFuture keepAliveRun = null;
    private volatile BlockSyncer brc = null;
    private ScheduledExecutorService keepAliveSender = null;
    private boolean firstBlockReportSent = false;
    volatile long lastBeingAlive = now();

    NSOfferService(InetSocketAddress isa, String nameserviceId) {
      this.nsRegistration = new DatanodeRegistration(getMachineName());
      this.nnAddr = isa;
      this.nameserviceId = nameserviceId;
    }
    
    public DatanodeProtocol getDatanodeProtocol() {
      return nsNamenode;
    }

    /**
     * Used only for testing.
     * 
     * @param name
     *          the new registration name for the datanode
     */
    public void setRegistrationName(String name) {
      this.nsRegistration.setName(name);
    }

    /**
     * Main loop for each NS thread. Run until shutdown,
     * forever calling remote NameNode functions.
     */
    private void offerService() throws Exception {

      LOG.info("using BLOCKREPORT_INTERVAL of " + blockReportInterval + "msec" + 
          " Initial delay: " + initialBlockReportDelay + "msec");
      LOG.info("using DELETEREPORT_INTERVAL of " + deletedReportInterval + "msec");
      LOG.info("using HEARTBEAT_INTERVAL of " + heartBeatInterval + "msec");
      LOG.info("using HEARTBEAT_EXPIRE_INTERVAL of " + heartbeatExpireInterval + "msec");

      //
      // Now loop for a long time....
      //

      while (shouldRun && shouldServiceRun) {
        try {
          long startTime = now();

          //
          // Every so often, send heartbeat or block-report
          //

          if (startTime - lastHeartbeat > heartBeatInterval) {
            //
            // All heartbeat messages include following info:
            // -- Datanode name
            // -- data transfer port
            // -- Total capacity
            // -- Bytes remaining
            //
            lastHeartbeat = startTime;
            DatanodeCommand[] cmds = nsNamenode.sendHeartbeat(nsRegistration,
                                                         data.getCapacity(),
                                                         data.getDfsUsed(),
                                                         data.getRemaining(),
                                                         data.getNSUsed(namespaceId),
                                                         xmitsInProgress.get(),
                                                         getXceiverCount());
            long cmdTime = now();
            this.lastBeingAlive = cmdTime;
            LOG.debug("Sent heartbeat at " + this.lastBeingAlive);
            myMetrics.heartbeats.inc(cmdTime - startTime);
            //LOG.info("Just sent heartbeat, with name " + localName);
            if (!processCommand(cmds, cmdTime))
              continue;
          }

          // check if there are newly received blocks (pendingReceivedRequeste > 0
          // or if the deletedReportInterval passed.
          if (firstBlockReportSent && (pendingReceivedRequests > 0
              || (startTime - lastDeletedReport > deletedReportInterval))) {
            Block[] receivedAndDeletedBlockArray = null;
            int currentReceivedRequestsCounter = pendingReceivedRequests;
            synchronized (receivedAndDeletedBlockList) {
                lastDeletedReport = startTime;

                int numBlocksReceivedAndDeleted = receivedAndDeletedBlockList
                    .size();
                if (numBlocksReceivedAndDeleted > 0) {
                  receivedAndDeletedBlockArray = receivedAndDeletedBlockList
                      .toArray(new Block[numBlocksReceivedAndDeleted]);
                }
            }
            if (receivedAndDeletedBlockArray != null) {
              long rpcStartTime = 0;
              if (LOG.isDebugEnabled()) {
                rpcStartTime = System.nanoTime();
                LOG.debug("sending blockReceivedAndDeleted "
                    + receivedAndDeletedBlockArray.length + " blocks to " + nnAddr);
              }
              nsNamenode.blockReceivedAndDeleted(nsRegistration, receivedAndDeletedBlockArray);
              if (LOG.isDebugEnabled()) {
                LOG.debug("finshed blockReceivedAndDeleted to " + nnAddr
                    + " time: " + (System.nanoTime() - rpcStartTime) + " ns");
              }
              synchronized (receivedAndDeletedBlockList) {
                for (int i = 0; i < receivedAndDeletedBlockArray.length; i++) {
                  receivedAndDeletedBlockList
                      .remove(receivedAndDeletedBlockArray[i]);
                }
                pendingReceivedRequests-=currentReceivedRequestsCounter;
              }
            }
          }


          // send block report
          if (startTime - lastBlockReport > blockReportInterval) {
            //
            // Send latest blockinfo report if timer has expired.
            // Get back a list of local block(s) that are obsolete
            // and can be safely GC'ed.
            //
            long brStartTime = now();
            Block[] bReport = data.getBlockReport(namespaceId);

            DatanodeCommand cmd = nsNamenode.blockReport(nsRegistration,
                    new BlockReport(BlockListAsLongs.convertToArrayLongs(bReport)));
            firstBlockReportSent = true;
            long cmdTime = now();
            long brTime = cmdTime - brStartTime;
            myMetrics.blockReports.inc(brTime);
            LOG.info("BlockReport of " + bReport.length +
                " blocks got processed in " + brTime + " msecs");
            //
            // If we have sent the first block report, then wait a random
            // time before we start the periodic block reports.
            //
            if (resetBlockReportTime) {
              lastBlockReport = startTime - R.nextInt((int)(blockReportInterval));
              resetBlockReportTime = false;
            } else {
               /* say the last block report was at 8:20:14. The current report
               * should have started around 9:20:14 (default 1 hour interval). 
               * If current time is :
               *   1) normal like 9:20:18, next report should be at 10:20:14
               *   2) unexpected like 11:35:43, next report should be at 12:20:14
               */
              lastBlockReport += (now() - lastBlockReport) / 
                                 blockReportInterval * blockReportInterval;
            }
            processCommand(cmd, cmdTime);
          }

          //
          // There is no work to do;  sleep until hearbeat timer elapses, 
          // or work arrives, and then iterate again.
          //
          long waitTime = heartBeatInterval - (System.currentTimeMillis() - lastHeartbeat);
          synchronized(receivedAndDeletedBlockList) {
            if (waitTime > 0 && pendingReceivedRequests == 0) {
              try {
                receivedAndDeletedBlockList.wait(waitTime);
              } catch (InterruptedException ie) {
              }
              delayBeforeBlockReceived();
            }
          } // synchronized
        } catch(RemoteException re) {
          String reClass = re.getClassName();
          if (UnregisteredDatanodeException.class.getName().equals(reClass) ||
              DisallowedDatanodeException.class.getName().equals(reClass) ||
              IncorrectVersionException.class.getName().equals(reClass)) {
            LOG.warn("DataNode is shutting down: " + 
                     StringUtils.stringifyException(re));
            shouldRun = false;
            shutdown();
            return;
          }
          try {
            Thread.sleep(1000);
          } catch (InterruptedException ie) {
            // NOTE: common case should be doing this instead of ignoring ie
            Thread.currentThread().interrupt();
          }
          LOG.warn(StringUtils.stringifyException(re));
        } catch (IOException e) {
          LOG.warn(StringUtils.stringifyException(e));
        }
      } // while (shouldRun)
    } // offerService
    
    /**
     * When a block has been received, we can delay some period of time before
     * reporting it to the DN, for the purpose of testing. This simulates
     * the actual latency of blockReceived on a real network (where the client
     * may be closer to the NN than the DNs).
     */
    private void delayBeforeBlockReceived() {
      if (artificialBlockReceivedDelay > 0 && !receivedAndDeletedBlockList.isEmpty()) {
        try {
          long sleepFor = (long)R.nextInt(artificialBlockReceivedDelay);
          LOG.debug("DataNode " + nsRegistration + " sleeping for " +
                    "artificial delay: " + sleepFor + " ms");
          Thread.sleep(sleepFor);
        } catch (InterruptedException ie) {
          Thread.currentThread().interrupt();
        }
      }
    }
    
    /**
     * Process an array of datanode commands
     * 
     * @param cmds an array of datanode commands
     * @return true if further processing may be required or false otherwise. 
     */
    private boolean processCommand(DatanodeCommand[] cmds, long processStartTime) {
      if (cmds != null) {
        for (DatanodeCommand cmd : cmds) {
          try {
            if (processCommand(cmd, processStartTime) == false) {
              return false;
            }
          } catch (IOException ioe) {
            LOG.warn("Error processing datanode Command", ioe);
          }
        }
      }
      return true;
    }
    
    /**
    *
    * @param cmd
    * @return true if further processing may be required or false otherwise.
    * @throws IOException
    */
    private boolean processCommand(DatanodeCommand cmd, long processStartTime)
        throws IOException {
     if (cmd == null)
       return true;
     final BlockCommand bcmd = cmd instanceof BlockCommand? (BlockCommand)cmd: null;

     boolean retValue = true;
     long startTime = System.currentTimeMillis();

     switch(cmd.getAction()) {
     case DatanodeProtocol.DNA_TRANSFER:
       // Send a copy of a block to another datanode
       transferBlocks(namespaceId,
           bcmd.getBlocks(), bcmd.getTargets());
       myMetrics.blocksReplicated.inc(bcmd.getBlocks().length);
       break;
     case DatanodeProtocol.DNA_INVALIDATE:
       //
       // Some local block(s) are obsolete and can be 
       // safely garbage-collected.
       //
       Block toDelete[] = bcmd.getBlocks();
       try {
         if (blockScanner != null) {
           blockScanner.deleteBlocks(namespaceId, toDelete);
         }        
         data.invalidate(namespaceId, toDelete);
       } catch(IOException e) {
         checkDiskError();
         throw e;
       }
       myMetrics.blocksRemoved.inc(toDelete.length);
       break;
     case DatanodeProtocol.DNA_SHUTDOWN:
       // shut down the data node
       shouldServiceRun = false;
       retValue = false;
       break;
     case DatanodeProtocol.DNA_REGISTER:
       // namenode requested a registration - at start or if NN lost contact
       LOG.info("DatanodeCommand action: DNA_REGISTER");
       if (shouldRun) {
         register();
         firstBlockReportSent = false;
       }
       break;
     case DatanodeProtocol.DNA_FINALIZE:
        storage.finalizedUpgrade(namespaceId);
       break;
     case UpgradeCommand.UC_ACTION_START_UPGRADE:
       // start distributed upgrade here
       processDistributedUpgradeCommand((UpgradeCommand)cmd);
       break;
     case DatanodeProtocol.DNA_RECOVERBLOCK:
        recoverBlocks(namespaceId, bcmd.getBlocks(), bcmd.getTargets(),
            processStartTime);
       break;
     case DatanodeProtocol.DNA_RAIDTASK:
       processRaidTaskCommand((RaidTaskCommand) cmd);
       break;
     default:
       LOG.warn("Unknown DatanodeCommand action: " + cmd.getAction());
     }
     long endTime = System.currentTimeMillis();
     if (endTime - startTime > 1000) {
       LOG.info("processCommand() took " + (endTime - startTime)
           + " msec to process command " + cmd.getAction() + " from " + nnAddr);
     } else if (LOG.isDebugEnabled()) {
       LOG.debug("processCommand() took " + (endTime - startTime)
           + " msec to process command " + cmd.getAction() + " from " + nnAddr);
     }
     return retValue;
   }

    /**
     * returns true if NS thread has completed initialization of storage
     * and has registered with the corresponding namenode
     * @return true if initialized
     */
   @Override
    public boolean initialized() {
      return initialized;
    }
    
   @Override
    public boolean isAlive() {
      return shouldServiceRun && nsThread.isAlive();
    }
    
   @Override
    public int getNamespaceId() {
      return namespaceId;
    }
   
   @Override
   public String getNameserviceId() {
     return this.nameserviceId;
   }
    
   @Override
    public InetSocketAddress getNNSocketAddress() {
      return nnAddr;
    }
 
    void setNamespaceInfo(NamespaceInfo nsinfo) {
      this.nsInfo = nsinfo;
      this.namespaceId = nsinfo.getNamespaceID();
      namespaceManager.addNamespace(this);
    }

    void setNameNode(DatanodeProtocol dnProtocol) {
      nsNamenode = dnProtocol;
    }

    private NamespaceInfo handshake() throws IOException {
      NamespaceInfo nsInfo = new NamespaceInfo();
      while (shouldRun && shouldServiceRun) {
        try {
          nsInfo = nsNamenode.versionRequest();
          break;
        } catch(SocketTimeoutException e) {  // namenode is busy
          LOG.info("Problem connecting to server: " + nnAddr);
          try {
            Thread.sleep(1000);
          } catch (InterruptedException ie) {}
        }
      }
      String errorMsg = null;
      // verify build version
      if( ! nsInfo.getBuildVersion().equals( Storage.getBuildVersion() )) {
        errorMsg = "Incompatible build versions: namenode BV = "
          + nsInfo.getBuildVersion() + "; datanode BV = "
          + Storage.getBuildVersion();
        LOG.warn( errorMsg );
        try {
          nsNamenode.errorReport( nsRegistration,
                                DatanodeProtocol.NOTIFY, errorMsg );
        } catch( SocketTimeoutException e ) {  // namenode is busy
          LOG.info("Problem connecting to server: " + nnAddr.toString());
        }
      }
      assert FSConstants.LAYOUT_VERSION == nsInfo.getLayoutVersion() :
        "Data-node and name-node layout versions must be the same."
        + "Expected: "+ FSConstants.LAYOUT_VERSION + " actual "+ nsInfo.getLayoutVersion();
      return nsInfo;
    }

    void setupNS(Configuration conf, AbstractList<File> dataDirs) 
    throws IOException {
      // get NN proxy
      DatanodeProtocol dnp = 
        (DatanodeProtocol)RPC.waitForProxy(DatanodeProtocol.class,
            DatanodeProtocol.versionID, nnAddr, conf);
      setNameNode(dnp);

      // handshake with NN
      NamespaceInfo nsInfo = handshake();
      setNamespaceInfo(nsInfo);
      synchronized(DataNode.this){
        setupNSStorage();
      }
      
      nsRegistration.setIpcPort(ipcServer.getListenerAddress().getPort());
      nsRegistration.setInfoPort(infoServer.getPort());
    }
    
    void setupNSStorage() throws IOException {
      StartupOption startOpt = getStartupOption(conf);
      assert startOpt != null : "Startup option must be set.";

      boolean simulatedFSDataset = 
        conf.getBoolean("dfs.datanode.simulateddatastorage", false);
      
      if (simulatedFSDataset) {
        nsRegistration.setStorageID(storage.getStorageID()); //same as DN
        nsRegistration.storageInfo.layoutVersion = FSConstants.LAYOUT_VERSION;
        nsRegistration.storageInfo.namespaceID = nsInfo.namespaceID;
      } else {
        // read storage info, lock data dirs and transition fs state if necessary      
        // first do it at the top level dataDirs
        // This is done only once when among all namespaces
        storage
            .recoverTransitionRead(DataNode.this, nsInfo, dataDirs, startOpt);
        // Then do it for this namespace's directory
        storage.recoverTransitionRead(DataNode.this, nsInfo.namespaceID,
            nsInfo, dataDirs, startOpt, nameserviceId);
        
        LOG.info("setting up storage: namespaceId="
            + namespaceId + ";lv=" + storage.layoutVersion + ";nsInfo="
            + nsInfo);

        nsRegistration.setStorageInfo(
            storage.getNStorage(nsInfo.namespaceID), storage.getStorageID());
        data.initialize(storage);
        
      }
      data.addNamespace(namespaceId, storage.getNameSpaceDataDir(namespaceId), conf);
      if (blockScanner != null) {
        blockScanner.start();
        blockScanner.addNamespace(namespaceId);
      }
    }
    

    /**
     * This methods  arranges for the data node to send the block report at 
     * the next heartbeat.
     */
    @Override
    public void scheduleBlockReport(long delay) {
      if (delay > 0) { // send BR after random delay
        lastBlockReport = System.currentTimeMillis()
        - ( blockReportInterval - R.nextInt((int)(delay)));
      } else { // send at next heartbeat
        lastBlockReport = lastHeartbeat - blockReportInterval;
      }
      resetBlockReportTime = true; // reset future BRs for randomness
    }
    
    /**
     * This method control the occurrence of blockReceivedAndDeleted 
     * only use for testing 
     */
    @Override
    public void scheduleBlockReceivedAndDeleted(long delay) {
      if (delay > 0) {
        lastDeletedReport = System.currentTimeMillis()
            - deletedReportInterval + delay;
      } else {
        lastDeletedReport = 0;
      }
    }

    @Override
    public void reportBadBlocks(LocatedBlock[] blocks) throws IOException{
      try {
        nsNamenode.reportBadBlocks(blocks);  
      } catch (IOException e){
        /* One common reason is that NameNode could be in safe mode.
         * Should we keep on retrying in that case?
         */
        LOG.warn("Failed to report bad block to namenode : " +
                 " Exception : " + StringUtils.stringifyException(e));
        throw e;
      }
      
    }

    /*
     * Informing the name node could take a long long time! Should we wait
     * till namenode is informed before responding with success to the
     * client? For now we don't.
     */
    @Override
    public void notifyNamenodeReceivedBlock(Block block, String delHint) {
      if (block == null ) {
        throw new IllegalArgumentException("Block is null");
      }

      if (delHint != null && !delHint.isEmpty()) {
        block = new ReceivedBlockInfo(block, delHint);
      }

      synchronized (receivedAndDeletedBlockList) {
        receivedAndDeletedBlockList.add(block);
        pendingReceivedRequests++;
        receivedAndDeletedBlockList.notifyAll();
      }
    }

    @Override
    public void notifyNamenodeDeletedBlock(Block block) {
      if (block == null) {
        throw new IllegalArgumentException(block == null ? "Block is null"
            : "delHint is null");
      }

      // mark it as a deleted block
      DFSUtil.markAsDeleted(block);

      synchronized (receivedAndDeletedBlockList) {
        receivedAndDeletedBlockList.add(block);
      }
    }
    
    //This must be called only by namespaceManager
    @Override
    public void start() {
      if ((nsThread != null) && (nsThread.isAlive())) {
        //Thread is started already
        return;
      }
      nsThread = new Thread(this, dnThreadName);
      nsThread.setDaemon(true); // needed for JUnit testing
      nsThread.start();
    }
    
    @Override
    //This must be called only by namespaceManager.
    public void stop() {
      shouldServiceRun = false;
      if (keepAliveRun != null) {
        keepAliveRun.cancel(false);
      }
      if (keepAliveSender != null) {
        keepAliveSender.shutdownNow();
      }
      if (nsThread != null) {
        nsThread.interrupt();
      }
    }
    
    //This must be called only by namespaceManager
    @Override
    public void join() {
      try {
        if (nsThread != null) {
          nsThread.join();
        }
      } catch (InterruptedException ie) { }
    }
    
    //Cleanup method to be called by current thread before exiting.
    private void cleanUp() {
      
      if(upgradeManager != null)
        upgradeManager.shutdownUpgrade();
      
      namespaceManager.remove(this.nnAddr);
      if (keepAliveRun != null) {
        keepAliveRun.cancel(false);
      }
      if (keepAliveSender != null) {
        keepAliveSender.shutdownNow();
      }
      shouldServiceRun = false;
      RPC.stopProxy(nsNamenode);
      if (blockScanner != null) {
        blockScanner.removeNamespace(this.getNamespaceId());
      }
      if (data != null) { 
        data.removeNamespace(this.getNamespaceId());
      }
      if (storage != null) {
        storage.removeNamespaceStorage(this.getNamespaceId());
      }
    }



    /**
     * Register one namespace with the corresponding NameNode
     * <p>
     * The nsDatanode needs to register with the namenode on startup in order
     * 1) to report which storage it is serving now and 
     * 2) to receive a registrationID
     *  
     * issued by the namenode to recognize registered datanodes.
     * 
     * @see FSNamesystem#registerDatanode(DatanodeRegistration)
     * @throws IOException
     */
    void register() throws IOException {
      if (nsRegistration.getStorageID().equals("")) {
        nsRegistration.storageID = createNewStorageId(nsRegistration.getPort());
      }
      while(shouldRun && shouldServiceRun) {
        try {
          // reset name to machineName. Mainly for web interface.
          nsRegistration.setName(machineName + ":" + nsRegistration.getPort());        
          nsRegistration = nsNamenode.register(nsRegistration,
              DataTransferProtocol.DATA_TRANSFER_VERSION);
          break;
        } catch(RemoteException re) {
          String reClass = re.getClassName();
          if (UnregisteredDatanodeException.class.getName().equals(reClass) ||
              DisallowedDatanodeException.class.getName().equals(reClass) ||
              IncorrectVersionException.class.getName().equals(reClass)) {
            LOG.warn("DataNode is shutting down: " +
                     StringUtils.stringifyException(re));
            break;
          }
        } catch(Exception e) {  // namenode cannot be contacted
          LOG.info("Problem connecting to server: " + nnAddr.toString() +
                    StringUtils.stringifyException(e));
        }
        try {
          Thread.sleep(1000);
        } catch (InterruptedException ie) {}
      }
      assert ("".equals(storage.getStorageID()) 
              && !"".equals(nsRegistration.getStorageID()))
              || storage.getStorageID().equals(nsRegistration.getStorageID()) :
              "New storageID can be assigned only if data-node is not formatted";
              
      if (storage.getStorageID().equals("")) {
        storage.setStorageID(nsRegistration.getStorageID());
        storage.writeAll();
        LOG.info("New storage id " + nsRegistration.getStorageID()
            + " is assigned to data-node " + nsRegistration.getName());
      }
      if(! storage.getStorageID().equals(nsRegistration.getStorageID())) {
        throw new IOException("Inconsistent storage IDs. Name-node returned "
            + nsRegistration.getStorageID() 
            + ". Expecting " + storage.getStorageID());
      }
      
      sendBlocksBeingWrittenReport(nsNamenode, namespaceId, nsRegistration);
      // random short delay - helps scatter the BR from all DNs
      scheduleBlockReport(initialBlockReportDelay);
    }


    /**
     * No matter what kind of exception we get, keep retrying to offerService().
     * That's the loop that connects to the NameNode and provides basic DataNode
     * functionality.
     *
     * Only stop when "shouldRun" or "shouldServiceRun" is turned off, which can
     * happen either at shutdown or due to refreshNamenodes.
     */
    @Override
    public void run() {
      LOG.info(nsRegistration + "In NSOfferService.run, data = " + data
          + ";ns=" + namespaceId);
      try {
        // init stuff
        try {
          // setup storage
          setupNS(conf, dataDirs);
          register();
          
          KeepAliveHeartbeater keepAliveTask =
              new KeepAliveHeartbeater(nsNamenode, nsRegistration, this);
          keepAliveSender = Executors.newSingleThreadScheduledExecutor();
          keepAliveRun = keepAliveSender.scheduleAtFixedRate(keepAliveTask, 0,
                                                             heartBeatInterval,
                                                             TimeUnit.MILLISECONDS);
          
        } catch (IOException ioe) {
          // Initial handshake, storage recovery or registration failed
          // End NSOfferService thread
          LOG.info("--------- " + StringUtils.stringifyException(ioe));
          LOG.fatal(nsRegistration + " initialization failed for namespaceId "
              + namespaceId, ioe);
          return;
        }

        initialized = true;
        while (shouldRun && shouldServiceRun) {
          try {
            startDistributedUpgradeIfNeeded();
            offerService();
          } catch (Exception ex) {
            LOG.error("Exception: " + StringUtils.stringifyException(ex));
            if (shouldRun && shouldServiceRun) {
              try {
                Thread.sleep(5000);
              } catch (InterruptedException ie) {
                LOG.warn("Received exception: ", ie);
              }
            }
          }
        }
      } catch (Throwable ex) {
        LOG.warn("Unexpected exception " + StringUtils.stringifyException(ex));
      } finally {
        LOG.warn(nsRegistration + " ending namespace service for: " 
            + namespaceId);
        cleanUp();
      }
    }  
    
    private void processDistributedUpgradeCommand(UpgradeCommand comm
                                                 ) throws IOException {
      assert upgradeManager != null : "DataNode.upgradeManager is null.";
      upgradeManager.processUpgradeCommand(comm);
    }

    @Override
    public synchronized UpgradeManagerDatanode getUpgradeManager() {
      if(upgradeManager == null)
        upgradeManager = 
          new UpgradeManagerDatanode(DataNode.this, namespaceId);
      
      return upgradeManager;
    }
    
    /**
     * Start distributed upgrade if it should be initiated by the data-node.
     */
    private void startDistributedUpgradeIfNeeded() throws IOException {
      UpgradeManagerDatanode um = getUpgradeManager();
      
      if(!um.getUpgradeState())
        return;
      um.setUpgradeState(false, um.getUpgradeVersion());
      um.startUpgrade();
      return;
    }
    
    /** Block synchronization */
    @Override
    public LocatedBlock syncBlock(
      Block block, List<BlockRecord> syncList,
      boolean closeFile, List<InterDatanodeProtocol> datanodeProxies,
      long deadline
    ) 
      throws IOException {
      if (brc == null) {
        brc = new BlockSyncer(namespaceId, nsNamenode, DataNode.LOG);
      }
      return brc.syncBlock(block, syncList, closeFile, datanodeProxies, deadline);
    }

    @Override
    public DatanodeRegistration getNsRegistration() {
      return nsRegistration;
    }
    

  }
  
  /**
   * Manages the NSOfferService objects for the data node.
   * Creation, removal, starting, stopping, shutdown on NSOfferService
   * objects must be done via APIs in this class.
   */
  class NamespaceManager {
    private final Map<Integer, NamespaceService> nsMapping =
      new HashMap<Integer, NamespaceService>();
    protected final Map<InetSocketAddress, NamespaceService> nameNodeThreads =
      new HashMap<InetSocketAddress, NamespaceService>();
    //This lock is only used for refreshNamenodes method
    private final Object refreshNamenodesLock = new Object();
 
    NamespaceManager() {
    }
    
    NamespaceManager(Configuration conf, List<InetSocketAddress> nameNodeAddrs)
        throws IOException {
      Collection<String> nameserviceIds = DFSUtil.getNameServiceIds(conf);
      Iterator<String> it = nameserviceIds.iterator();
      for(InetSocketAddress nnAddr : nameNodeAddrs){
        String nameserviceId = it.hasNext()? it.next(): null;
        NSOfferService nsos = new NSOfferService(nnAddr, nameserviceId);
        nameNodeThreads.put(nsos.getNNSocketAddress(), nsos);
      }
    }
    
    public boolean initailized() {
      for(NamespaceService nsos : nameNodeThreads.values()){
        if(!nsos.initialized()){
          return false;
        }
      }
      return true;
    }
    
    public boolean isAlive(int namespaceId) {
      NamespaceService nsos = nsMapping.get(namespaceId);
      if(nsos == null){
        return false;
      }
      return nsos.isAlive();
    }
    
    synchronized void addNamespace(NamespaceService t) {
      if (nameNodeThreads.get(t.getNNSocketAddress()) == null) {
        throw new IllegalArgumentException(
            "Unknown NSOfferService thread for namenode address:"
                + t.getNNSocketAddress());
      }
      nsMapping.put(t.getNamespaceId(), t);
    }
    
    /**
     * Returns the array of NSOfferService objects. 
     * Caution: The NSOfferService returned could be shutdown any time.
     */
    synchronized NamespaceService[] getAllNamenodeThreads() {
      NamespaceService[] nsosArray = new NamespaceService[nameNodeThreads.values()
          .size()];
      return nameNodeThreads.values().toArray(nsosArray);
    }
    
    synchronized NamespaceService get(int namespaceId) {
      return nsMapping.get(namespaceId);
    }
    
    synchronized NamespaceService get(String nameserviceId) {
      for (NamespaceService namespaceService : nameNodeThreads.values()) {
        if (namespaceService.getNameserviceId().equals(nameserviceId)) {
          return namespaceService;
        }
      }
      return null;
    }
    
    synchronized NamespaceService get(InetSocketAddress nameNodeAddr) {
      return nameNodeThreads.get(nameNodeAddr);
    }
    
    public synchronized NamespaceService remove(InetSocketAddress nnAddr) {
      NamespaceService ns = nameNodeThreads.remove(nnAddr);
      if (ns != null) {
        nsMapping.remove(ns.getNamespaceId());
      }
      return ns;
    }
    
    /** The service's address gets updated from oldNNAddr to newNNAddr,
     * so we need to update the key of the nameNodeTreads map
     * 
     * @param oldNNAddr old service namenode address
     * @param newNNAddr new service namenode address
     * @throws IOException if the service does not exist
     */
    protected synchronized void remapNameservice(InetSocketAddress oldNNAddr,
        InetSocketAddress newNNAddr) throws IOException {
      NamespaceService service = nameNodeThreads.remove(oldNNAddr);
      if (service == null) {
        throw new IOException("Service for " + oldNNAddr + " does not exist!");
      }
      nameNodeThreads.put(newNNAddr, service);
    }
    
    synchronized Integer[] getAllNamespaces(){
      return nsMapping.keySet().toArray(
          new Integer[nsMapping.keySet().size()]);
    }
    
    void shutDownAll() {

      NamespaceService[] nsosArray = this.getAllNamenodeThreads();
        
      for (NamespaceService nsos : nsosArray) {
        nsos.stop(); //interrupts the threads
      }
      //now join
      for (NamespaceService nsos : nsosArray) {
        nsos.join();
      }
    }
    
    void startAll() throws IOException {
      for (NamespaceService nsos : getAllNamenodeThreads()) {
        nsos.start();
      }
      isAlive = true;
    }
    
    void stopAll() {
      for (NamespaceService nsos : getAllNamenodeThreads()) {
        nsos.stop();
      }      
    }
    
    void joinAll() throws InterruptedException {
      for (NamespaceService nsos : getAllNamenodeThreads()) {
        nsos.join();
      }
    }
    
    void refreshNamenodes(List<InetSocketAddress> nameNodeAddrs, Configuration conf)
        throws IOException, InterruptedException{
      List<InetSocketAddress> toStart = new ArrayList<InetSocketAddress>();
      List<NamespaceService> toStop = new ArrayList<NamespaceService>();
      Collection<String> nameserviceIds = DFSUtil.getNameServiceIds(conf);
      List<String> toStartServiceIds = new ArrayList<String>();
      synchronized (refreshNamenodesLock) {
        synchronized (this) {
          List<InetSocketAddress> toStopNNs = new ArrayList<InetSocketAddress>();
          for (InetSocketAddress nnAddr : nameNodeThreads.keySet()) {
            if (!nameNodeAddrs.contains(nnAddr)){
              LOG.info("To remove service at " + nnAddr);
              toStopNNs.add(nnAddr);
            }
          }
          for (InetSocketAddress nnAddr : toStopNNs) {
            NamespaceService ns = nameNodeThreads.remove(nnAddr);
            if (ns != null) {
              LOG.info("Removing service: " + nnAddr);
              toStop.add(ns);
            }
          }
          Iterator<String> it = nameserviceIds.iterator();
          for (InetSocketAddress nnAddr : nameNodeAddrs) {
            String nameserviceId = it.hasNext()? it.next(): null;
            if (!nameNodeThreads.containsKey(nnAddr)) {
              LOG.info("Adding service: " + nnAddr);
              toStart.add(nnAddr);
              toStartServiceIds.add(nameserviceId);
            }
          }
          
          it = toStartServiceIds.iterator();
          for (InetSocketAddress nnAddr : toStart) {
            NSOfferService nsos = new NSOfferService(nnAddr, it.next());
            nameNodeThreads.put(nsos.getNNSocketAddress(), nsos);
          }
        }
        for (NamespaceService nsos : toStop) {
          nsos.stop();
        }
        for (NamespaceService nsos : toStop) {
          nsos.join();
        }
        startAll();
      }
    }
  }
  
  /* ********************************************************************
  Protocol when a client reads data from Datanode (Cur Ver: 23):
  
  Client's Request :
  =================
   
     Processed in DataXceiver:
     +----------------------------------------------+
     | Common Header   | 1 byte OP == OP_READ_BLOCK |
     +----------------------------------------------+
     
     Processed in readBlock() :
     +------------------------------------------------------------------+
     | 4 byte NS ID |     8 byte Block ID     |     8 byte genstamp     |
     +------------------------------------------------------------------+
     |   8 byte start offset     |        8 byte length       |
     +--------------------------------------------------------+
     |   vInt length   |  <DFSClient id> |
     +-----------------------------------+
     
     Client sends optional response only at the end of receiving data.
       
  DataNode Response :
  ===================
   
    In readBlock() :
    If there is an error while initializing BlockSender :
       +---------------------------+
       | 2 byte OP_STATUS_ERROR    | and connection will be closed.
       +---------------------------+
    Otherwise
       +---------------------------+
       | 2 byte OP_STATUS_SUCCESS  |
       +---------------------------+

    Actual data, sent by BlockSender.sendBlock() :

      ChecksumHeader :
      +--------------------------------------------------+
      | 1 byte CHECKSUM_TYPE | 4 byte BYTES_PER_CHECKSUM |
      +--------------------------------------------------+
      Followed by actual data in the form of PACKETS:
      +------------------------------------+
      | Sequence of data PACKETs ....      |
      +------------------------------------+

    If it is used for sending data from data nodes to clients, two extra
    fields are sent in the end:

      +--------------------------+
      | Boolean isBlockFinalized |
      +--------------------------+-------------------------------+
      |                 8 byte Block length                      |
      +----------------------------------------------------------+

    A "PACKET" is defined further below.

    The client reads data until it receives a packet with
    "LastPacketInBlock" set to true or with a zero length. It then replies
    to DataNode with one of the status codes:
    - CHECKSUM_OK:      All the chunk checksums have been verified
    - SUCCESS:          Data received; checksums not verified
    - ERROR_CHECKSUM:   (Currently not used) Detected invalid checksums

       +----------------+
       | 2 byte Status  |
       +----------------+
    
    The DataNode expects all well behaved clients to send the 2 byte 
    status code. And if the client doesn't, the DN will close the 
    connection. So the status code is optional in the sense that it
    does not affect the correctness of the data. (And the client can
    always reconnect.)

    PACKET : Contains a packet header, checksum and data. Amount of data
    ======== carried is set by BUFFER_SIZE.

      +-----------------------------------------------------+
      | 4 byte packet length (excluding packet header)      |
      +-----------------------------------------------------+
      | 8 byte offset in the block | 8 byte sequence number |
      +-----------------------------------------------------+
      | 1 byte boolean set: isLastPacketInBlock | forceSync |
      +-----------------------------------------------------+
      | 4 byte Length of actual data                        |
      +-----------------------------------------------------+
      | x byte checksum data. x is defined below            |
      +-----------------------------------------------------+
      | actual data ......                                  |
      +-----------------------------------------------------+
      
      x = (length of data + BYTE_PER_CHECKSUM - 1)/BYTES_PER_CHECKSUM *
          CHECKSUM_SIZE
          
      CHECKSUM_SIZE depends on CHECKSUM_TYPE (usually, 4 for CRC32)
      
      The above packet format is used while writing data to DFS also.
      Not all the fields might be used while reading.
    
   ************************************************************************ */
  
  /** Header size for a packet */
  public static int PKT_HEADER_LEN = ( 4 + /* Packet payload length */
                                      8 + /* offset in block */
                                      8 + /* seqno */
                                      1  /* up to 8 boolean values field */);
  public static byte isLastPacketInBlockMask = 0x01;
  public static byte forceSyncMask = 0x02;
  
  public static int getPktVersionSize(boolean pktIncludeVersion) {
    return pktIncludeVersion ? SIZE_OF_INTEGER : 0;
  }
  
  public static int getPacketHeaderLen(boolean pktIncludeVersion) {
    return DataNode.PKT_HEADER_LEN + getPktVersionSize(pktIncludeVersion);
  }
  
  /**
   * Used for transferring a block of data.  This class
   * sends a piece of data to another DataNode.
   */
  class DataTransfer implements Callable<Boolean> {
    DatanodeInfo targets[];
    Block b;
    Block destinationBlock;
    DataNode datanode;
    private int srcNamespaceId;
    private int dstNamespaceId;

    /**
     * Connect to the first item in the target list.  Pass along the 
     * entire target list, the block, and the data.
     */
    public DataTransfer(int namespaceId, DatanodeInfo targets[], Block b, DataNode datanode) throws IOException {
      // the source and destination blocks are the same for block replication
      this(targets, namespaceId, b, namespaceId, b, datanode);
    }

    /**
     * Connect to the first item in the target list.  Pass along the 
     * entire target list, the block, and the data.
     */
    public DataTransfer(DatanodeInfo targets[], int srcNamespaceId, Block b,
                        int dstNamespaceId, Block destinationBlock,
                        DataNode datanode) throws IOException {
      this.targets = targets;
      this.b = b;
      this.destinationBlock = destinationBlock;
      this.datanode = datanode;
      this.srcNamespaceId = srcNamespaceId;
      this.dstNamespaceId = dstNamespaceId;
    }

    /**
     * Do the deed, write the bytes
     */
    public Boolean call() throws Exception {
      xmitsInProgress.getAndIncrement();
      Socket sock = null;
      DataOutputStream out = null;
      BlockSender blockSender = null;
      
      int dataTransferVersion = DataTransferProtocol.DATA_TRANSFER_VERSION;

      try {
        InetSocketAddress curTarget =
          NetUtils.createSocketAddr(targets[0].getName());
        sock = newSocket();
        NetUtils.connect(sock, curTarget, socketTimeout);
        sock.setSoTimeout(targets.length * socketTimeout);

        long writeTimeout = socketWriteTimeout + socketWriteExtentionTimeout
            * (targets.length - 1);
        OutputStream baseStream = NetUtils.getOutputStream(sock, writeTimeout);
        out = new DataOutputStream(new BufferedOutputStream(baseStream, 
                                                            SMALL_BUFFER_SIZE));

        blockSender = new BlockSender(srcNamespaceId, b, 0, b.getNumBytes(), false,
            false, false, false,
            dataTransferVersion >=
                DataTransferProtocol.PACKET_INCLUDE_VERSION_VERSION,
            true, datanode, null);
        DatanodeInfo srcNode = new DatanodeInfo(getDNRegistrationForNS(srcNamespaceId));

        //
        // Header info
        //
        WriteBlockHeader header = new WriteBlockHeader(
            dataTransferVersion, dstNamespaceId,
            destinationBlock.getBlockId(),
            destinationBlock.getGenerationStamp(), 0, false, true, srcNode,
            targets.length - 1, targets, "");
        header.writeVersionAndOpCode(out);
        header.write(out);

        // send data & checksum
        DataTransferThrottler trottler = null;
        if (dataTransferMaxRate > 0) {
          trottler = new DataTransferThrottler(dataTransferMaxRate);
        }
        blockSender.sendBlock(out, baseStream, trottler);

        // no response necessary
        LOG.info(getDatanodeInfo() + ":Transmitted block " + b + " at " + srcNamespaceId + " to " + curTarget);

      } catch (IOException ie) {
        LOG.warn(getDatanodeInfo() + ":Failed to transfer " + b + " at " + srcNamespaceId + " to " + targets[0].getName()
            + " got " + StringUtils.stringifyException(ie));
        // check if there are any disk problem
        try{
          datanode.checkDiskError();
        } catch (IOException e) {
          LOG.warn("Error when checking disks : " + StringUtils.stringifyException(e));
          throw e;
        }
        throw ie;
      } finally {
        xmitsInProgress.getAndDecrement();
        IOUtils.closeStream(blockSender);
        IOUtils.closeStream(out);
        IOUtils.closeSocket(sock);
      }
      return true;
    }
  }

  /**
   * Initializes the {@link #data}. The initialization is done only once, when
   * handshake with the the first namenode is completed.
   */
  private synchronized void initFsDataSet(Configuration conf,
      AbstractList<File> dataDirs, int numNamespaces) throws IOException {
    if (data != null) { // Already initialized
      return;
    }

    // get version and id info from the name-node
    boolean simulatedFSDataset = 
      conf.getBoolean("dfs.datanode.simulateddatastorage", false);

    if (simulatedFSDataset) {
      storage.createStorageID(selfAddr.getPort());
      // it would have been better to pass storage as a parameter to
      // constructor below - need to augment ReflectionUtils used below.
      conf.set("dfs.datanode.StorageId", storage.getStorageID());
      try {
        data = (FSDatasetInterface) ReflectionUtils.newInstance(
            Class.forName(
            "org.apache.hadoop.hdfs.server.datanode.SimulatedFSDataset"),
            conf);
      } catch (ClassNotFoundException e) {
        throw new IOException(StringUtils.stringifyException(e));
      }
    } else {
      data = new FSDataset(this, conf, numNamespaces);
    }
  }

  public static class KeepAliveHeartbeater implements Runnable {

   private DatanodeProtocol namenode;
   private DatanodeRegistration dnRegistration;
   private NamespaceService ns;

   public KeepAliveHeartbeater(DatanodeProtocol namenode,
       DatanodeRegistration dnRegistration,
       NamespaceService ns) {
     this.namenode = namenode;
     this.dnRegistration = dnRegistration;
     this.ns = ns;
   }
   
   public void run() {
     try {
       namenode.keepAlive(dnRegistration);
       ns.lastBeingAlive = now();
       LOG.debug("Sent heartbeat at " + ns.lastBeingAlive);
     } catch (Throwable ex) {
       LOG.error("Error sending keepAlive to the namenode", ex);
     }
   }
  }
  
  /** Start a single datanode daemon and wait for it to finish.
   *  If this thread is specifically interrupted, it will stop waiting.
   */
  public void runDatanodeDaemon() throws IOException {
    namespaceManager.startAll();
    // start dataXceiveServer
    dataXceiverServer.start();
    ipcServer.start();
  }

  public static boolean isDatanodeUp(DataNode dn) {
    return dn.isDatanodeUp();
  }
  
  /**
   * @return true if any namespace thread is alive 
   */
  public boolean isDatanodeUp() {
    for (NamespaceService nsos: namespaceManager.getAllNamenodeThreads()) {
      if (nsos != null && nsos.isAlive()) {
        return true;
      }
    }
    return false;
  }
  
  /**
   * @return true if any namespace thread has heartbeat with namenode recently
   */
  public boolean isDataNodeBeingAlive() {
    for (NamespaceService nsos: namespaceManager.getAllNamenodeThreads()) {
      if (nsos != null && 
          nsos.lastBeingAlive >= now() - heartbeatExpireInterval) {
        return true;
      }
    }
    return false;
  }
  
  /**
  * @return true - if the data node is initialized 
  */
  public boolean isInitialized() {
    for (NamespaceService nsos : namespaceManager.getAllNamenodeThreads()) {
      if (!nsos.initialized() || !nsos.isAlive()) {
        return false;
      }
    }
    return true;
  }
  
  /**
   * @param namenode addr  
   * @return true if the NSOfferService thread for given namespaceID is initialized
   * @throws IOException when the NSOfferService is dead
   */
  public synchronized boolean initialized(InetSocketAddress nameNodeAddr) throws IOException{
    NamespaceService nsos = namespaceManager.get(nameNodeAddr);
    if (nsos == null) {
      throw new IOException("NSOfferService for namenode " +
          nameNodeAddr.getAddress() + " is dead.");
    }
    return nsos.initialized(); 
  }
  
  /** Instantiate a single datanode object. This must be run by invoking
   *  {@link DataNode#runDatanodeDaemon(DataNode)} subsequently. 
   */
  public static DataNode instantiateDataNode(String args[],
                                      Configuration conf) throws IOException {
    if (conf == null)
      conf = new Configuration();
    if (!parseArguments(args, conf)) {
      printUsage();
      return null;
    }
    if (conf.get("dfs.network.script") != null) {
      LOG.error("This configuration for rack identification is not supported" +
          " anymore. RackID resolution is handled by the NameNode.");
      System.exit(-1);
    }
    String[] dataDirs = getListOfDataDirs(conf);
        dnThreadName = "DataNode: [" +
                        StringUtils.arrayToString(dataDirs) + "]";
    return makeInstance(dataDirs, conf);
  }

  /** Returns a list of data directories from the file provided by the
   * dfs.datadir.confpath. If it cannot get the list of data directories
   * then the method will return the default dataDirs from dfs.data.dir.
   */
  public static String[] getListOfDataDirs(Configuration conf) {
    String[] configFilePath = conf.getStrings("dfs.datadir.confpath");
    String[] dataDirs = null;
    if(configFilePath != null && (configFilePath.length != 0)) {
      try {
        DataDirFileReader reader = new DataDirFileReader(configFilePath[0]);
        dataDirs = reader.getArrayOfCurrentDataDirectories();
        if(dataDirs == null) {
          LOG.warn("File is empty, using dfs.data.dir directories");
        }
      } catch (Exception e) {
        LOG.warn("Could not read file, using directories from dfs.data.dir" +
                                                         " Exception: ", e);
      }
    } else {
      LOG.warn("No dfs.datadir.confpath not defined, now using default " +
                                                              "directories");
    }
    if(dataDirs == null) {
      dataDirs = conf.getStrings("dfs.data.dir");
    }
    return dataDirs;

  }

  /** Instantiate & Start a single datanode daemon and wait for it to finish.
   *  If this thread is specifically interrupted, it will stop waiting.
   */
  public static DataNode createDataNode(String args[], Configuration conf) 
    throws IOException {
    DataNode dn = instantiateDataNode(args, conf);
    if (dn != null) {
      dn.runDatanodeDaemon();
    }
    return dn;
  }

  void join() {
    while (shouldRun) {
      try {
        namespaceManager.joinAll();
        NamespaceService[] namespaceServices = namespaceManager.getAllNamenodeThreads();
        if (namespaceServices == null || (namespaceServices != null
            && namespaceServices.length == 0)) {
          shouldRun = false;
          isAlive = false;
        }
        Thread.sleep(2000);
      } catch (InterruptedException ex) {
        LOG.warn("Received exception in Datanode#join: " + ex);
      }
    }
  }

  /**
   * Make an instance of DataNode after ensuring that at least one of the
   * given data directories (and their parent directories, if necessary)
   * can be created.
   * @param dataDirs List of directories, where the new DataNode instance should
   * keep its files.
   * @param conf Configuration instance to use.
   * @return DataNode instance for given list of data dirs and conf, or null if
   * no directory from this directory list can be created.
   * @throws IOException
   */
  public static DataNode makeInstance(String[] dataDirs, Configuration conf)
    throws IOException {
    ArrayList<File> dirs = new ArrayList<File>();
    for (int i = 0; i < dataDirs.length; i++) {
      File data = new File(dataDirs[i]);
      try {
        DiskChecker.checkDir(data);
        dirs.add(data);
      } catch(DiskErrorException e) {
        LOG.warn("Invalid directory in dfs.data.dir: " + e.getMessage());
      }
    }
    if (dirs.size() > 0)
      return new DataNode(conf, dirs);
    LOG.error("All directories in dfs.data.dir are invalid.");
    return null;
  }

  @Override
  public String toString() {
    return "DataNode{" +
      "data=" + data +
      ", localName='" + getDatanodeInfo() + "'" +
      ", xmitsInProgress=" + xmitsInProgress.get() +
      "}";
  }

  private static void printUsage() {
    System.err.println("Usage: java DataNode");
    System.err.println("           [-rollback]");
  }

  /**
   * Parse and verify command line arguments and set configuration parameters.
   *
   * @return false if passed argements are incorrect
   */
  private static boolean parseArguments(String args[],
                                        Configuration conf) {
    int argsLen = (args == null) ? 0 : args.length;
    StartupOption startOpt = StartupOption.REGULAR;
    for(int i=0; i < argsLen; i++) {
      String cmd = args[i];
      if ("-r".equalsIgnoreCase(cmd) || "--rack".equalsIgnoreCase(cmd)) {
        LOG.error("-r, --rack arguments are not supported anymore. RackID " +
            "resolution is handled by the NameNode.");
        System.exit(-1);
      } else if ("-rollback".equalsIgnoreCase(cmd)) {
        startOpt = StartupOption.ROLLBACK;
      } else if ("-regular".equalsIgnoreCase(cmd)) {
        startOpt = StartupOption.REGULAR;
      } else if ("-d".equalsIgnoreCase(cmd)) {
        ++i;
        if(i >= argsLen) {
          LOG.error("-D option requires following argument.");
          System.exit(-1);
        }
        String[] keyval = args[i].split("=", 2);
        if (keyval.length == 2) {
          conf.set(keyval[0], keyval[1]);
        } else {
          LOG.error("-D option invalid (expected =): " + args[i]);
          System.exit(-1);
        }
      } else
        return false;
    }
    setStartupOption(conf, startOpt);
    return true;
  }

  private static void setStartupOption(Configuration conf, StartupOption opt) {
    conf.set("dfs.datanode.startup", opt.toString());
  }

  static StartupOption getStartupOption(Configuration conf) {
    return StartupOption.valueOf(conf.get("dfs.datanode.startup",
                                          StartupOption.REGULAR.toString()));
  }


  /**
   * This method is used for testing. 
   * Examples are adding and deleting blocks directly.
   * The most common usage will be when the data node's storage is similated.
   * 
   * @return the fsdataset that stores the blocks
   */
  public FSDatasetInterface getFSDataset() {
    return data;
  }

  /** Wait for the datanode to exit and clean up all its resources */
  public void waitAndShutdown() {
    join();
    // make sure all other threads have exited even if 
    // offerservice thread died abnormally
    shutdown();
  }

  /**
   */
  public static void main(String args[]) {
    org.apache.hadoop.hdfs.DnsMonitorSecurityManager.setTheManager();
    try {
      StringUtils.startupShutdownMessage(DataNode.class, args, LOG);
      FastWritableHDFS.init();
      FastProtocolHDFS.init();
      DataNode datanode = createDataNode(args, null);
      if (datanode != null) {
        datanode.waitAndShutdown();
      }
    } catch (Throwable e) {
      LOG.error(StringUtils.stringifyException(e));
      System.exit(-1);
    }
  }
  
  private void transferBlock(int namespaceId, Block block,
      DatanodeInfo xferTargets[]) throws IOException {
    DatanodeProtocol nn = getNSNamenode(namespaceId);
    DatanodeRegistration nsReg = getDNRegistrationForNS(namespaceId);

    if (!data.isValidBlock(namespaceId, block, true)) {
      // block does not exist or is under-construction
      String errStr = "Can't send invalid block " + block;
      LOG.info(errStr);
      nn.errorReport(nsReg, DatanodeProtocol.INVALID_BLOCK, errStr);
      return;
    }

    // Check if NN recorded length matches on-disk length
    long onDiskLength = data.getFinalizedBlockLength(namespaceId, block);
    if (block.getNumBytes() > onDiskLength) {
      // Shorter on-disk len indicates corruption so report NN the corrupt block
      nn.reportBadBlocks(new LocatedBlock[] { new LocatedBlock(block,
          new DatanodeInfo[] { new DatanodeInfo(nsReg) }) });
      LOG.info("Can't replicate block " + block + " because on-disk length "
          + onDiskLength + " is shorter than NameNode recorded length "
          + block.getNumBytes());
      return;
    }

    int numTargets = xferTargets.length;
    if (numTargets > 0) {
      if (LOG.isInfoEnabled()) {
        StringBuilder xfersBuilder = new StringBuilder();
        for (int i = 0; i < numTargets; i++) {
          xfersBuilder.append(xferTargets[i].getName());
          xfersBuilder.append(" ");
        }
        LOG.info(nsReg + " Starting thread to transfer block " + block + " to "
            + xfersBuilder);
      }

      blockCopyExecutor.submit(new DataTransfer(namespaceId, xferTargets, block, this));
    }
  }

  void transferBlocks(int namespaceId, Block blocks[],
      DatanodeInfo xferTargets[][]) {
    for (int i = 0; i < blocks.length; i++) {
      try {
        transferBlock(namespaceId, blocks[i], xferTargets[i]);
      } catch (IOException ie) {
        LOG.warn("Failed to transfer block " + blocks[i], ie);
      }
    }
  }

  protected void notifyNamenodeReceivedBlock(int namespaceId, Block block,
      String delHint) throws IOException {
    if (block == null) {
      throw new IllegalArgumentException("Block is null");
    }
    NamespaceService nsos = namespaceManager.get(namespaceId);
    if (nsos == null || nsos.getDatanodeProtocol() == null) {
      throw new IOException("Cannot locate OfferService thread for namespace="
          + namespaceId);
    }
    nsos.notifyNamenodeReceivedBlock(block, delHint);
  }

  protected void notifyNamenodeDeletedBlock(int namespaceId, Block block)
      throws IOException {
    if (block == null) {
      throw new IllegalArgumentException("Block is null");
    }
    NamespaceService nsos = namespaceManager.get(namespaceId);
    if (nsos == null || nsos.getDatanodeProtocol() == null) {
      throw new IOException("Cannot locate OfferService thread for namespace="
          + namespaceId);
    }
    nsos.notifyNamenodeDeletedBlock(block);
  }

  // InterDataNodeProtocol implementation
  // THIS METHOD IS ONLY USED FOR UNIT TESTS
  /** {@inheritDoc} */
  public BlockMetaDataInfo getBlockMetaDataInfo(int namespaceId, Block block
      ) throws IOException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("block=" + block);
    }

    Block stored = data.getStoredBlock(namespaceId, block.getBlockId());

    if (stored == null) {
      return null;
    }
    BlockMetaDataInfo info = new BlockMetaDataInfo(stored,
                                 blockScanner.getLastScanTime(namespaceId, stored));
    if (LOG.isDebugEnabled()) {
      LOG.debug("getBlockMetaDataInfo successful block=" + stored +
                " length " + stored.getNumBytes() +
                " genstamp " + stored.getGenerationStamp());
    }

    // paranoia! verify that the contents of the stored block
    // matches the block file on disk.
    
    data.validateBlockMetadata(namespaceId, stored);
    return info;
  }

  @Override
  public BlockRecoveryInfo startBlockRecovery(int namespaceId, Block block) throws IOException {
    InjectionHandler.processEvent(InjectionEvent.DATANODE_BEFORE_RECOVERBLOCK,
        this);
    return data.startBlockRecovery(namespaceId, block.getBlockId());
  }

  public Daemon recoverBlocks(final int namespaceId, final Block[] blocks,
      final DatanodeInfo[][] targets, long processStartTime) {
    final long deadline = processStartTime + blkRecoveryTimeout;
    Daemon d = new Daemon(threadGroup, new Runnable() {
      /** Recover a list of blocks. It is run by the primary datanode. */
      public void run() {
        for(int i = 0; i < blocks.length; i++) {
          try {
            logRecoverBlock("NameNode", namespaceId, blocks[i], targets[i]);
            recoverBlock(namespaceId, blocks[i], false, targets[i], true, deadline);
          } catch (IOException e) {
            LOG.warn("recoverBlocks FAILED, blocks[" + i + "]=" + blocks[i], e);
          }
        }
      }
    });
    d.start();
    return d;
  }
  
  public void processRaidTaskCommand(RaidTaskCommand cmd) throws IOException {
    // TODO: do actual encoding/blockfixing work here.
    // If all parity blocks are missing, this is a encoding task,
    // Otherwise, this is a decoding task.
    InjectionHandler.processEventIO(InjectionEvent.DATANODE_PROCESS_RAID_TASK,
        this, cmd);
  }

    /** {@inheritDoc} */
  public void updateBlock(int namespaceId, Block oldblock, Block newblock, boolean finalize) throws IOException {
    LOG.info("namespaceId: " + namespaceId 
        + ", oldblock=" + oldblock + "(length=" + oldblock.getNumBytes()
        + "), newblock=" + newblock + "(length=" + newblock.getNumBytes()
        + "), datanode=" + getDatanodeInfo());
    data.updateBlock(namespaceId, oldblock, newblock);
    if (finalize) {
      data.finalizeBlockIfNeeded(namespaceId, newblock);
      myMetrics.blocksWritten.inc();
      notifyNamenodeReceivedBlock(namespaceId, newblock, null);
      LOG.info("Received block " + newblock +
                " of size " + newblock.getNumBytes() +
                " as part of lease recovery.");
    }
  }

  /** {@inheritDoc} */
  public long getProtocolVersion(String protocol, long clientVersion
      ) throws IOException {
    if (protocol.equals(InterDatanodeProtocol.class.getName())) {
      return InterDatanodeProtocol.versionID;
    } else if (protocol.equals(ClientDatanodeProtocol.class.getName())) {
      checkVersion(protocol, clientVersion, ClientDatanodeProtocol.versionID);
      return ClientDatanodeProtocol.versionID;
    }
    throw new IOException("Unknown protocol to " + getClass().getSimpleName()
        + ": " + protocol);
  }

  /** {@inheritDoc} */
  public BlockPathInfo getBlockPathInfo(Block block) throws IOException {
    return getBlockPathInfo(getAllNamespaces()[0], block);
  }
  
  @Override
  public BlockPathInfo getBlockPathInfo(int namespaceId, Block block)
      throws IOException {
    ReplicaToRead replica = data.getReplicaToRead(namespaceId, block);
    if (replica == null) {
      throw new IOException("Cannot find block to read. namespace: "
          + namespaceId + " block: " + block);
    }
    String metafilePath = "";
    if (!replica.isInlineChecksum()) {
      metafilePath = BlockWithChecksumFileWriter.getMetaFile(
          replica.getDataFileToRead(), block).getAbsolutePath();
    }
    BlockPathInfo info = new BlockPathInfo(block, replica.getDataFileToRead()
        .getAbsolutePath(), metafilePath);
    if (LOG.isDebugEnabled()) {
      LOG.debug("getBlockPathInfo successful block=" + block +
                " blockfile " + replica.getDataFileToRead().getAbsolutePath() +
                " metafile " + metafilePath);
    }
    return info;
  }

  public ProtocolSignature getProtocolSignature(String protocol,
      long clientVersion, int clientMethodsHash) throws IOException {
    return ProtocolSignature.getProtocolSignature(
        this, protocol, clientVersion, clientMethodsHash);
  }

  private void checkVersion(String protocol, long clientVersion, 
    long serverVersion) throws IOException {
    if (serverVersion > clientVersion &&
       !ProtocolCompatible.isCompatibleClientDatanodeProtocol(
              clientVersion, serverVersion)) {
      throw new RPC.VersionIncompatible(protocol, clientVersion, serverVersion);
    }
  }

  static public class BlockRecoveryTimeoutException extends IOException {
    /**
     * 
     */
    private static final long serialVersionUID = 7887035511587861524L;

    public BlockRecoveryTimeoutException (String msg) {
      super (msg);
    }
  }

  static public void throwIfAfterTime(long timeoutTime) throws IOException {
    if (timeoutTime > 0 && System.currentTimeMillis() > timeoutTime) {
      throw new BlockRecoveryTimeoutException("The client have timed out.");
    }
  }
  
  /** Recover a block
   * @param keepLength if true, will only recover replicas that have the same length
   * as the block passed in. Otherwise, will calculate the minimum length of the
   * replicas and truncate the rest to that length.
   **/
  private LocatedBlock recoverBlock(int namespaceId, Block block, boolean keepLength,
      DatanodeID[] datanodeids, boolean closeFile, long deadline) throws IOException {
    InjectionHandler.processEvent(InjectionEvent.DATANODE_BEFORE_RECOVERBLOCK,
        this);

    // If the block is already being recovered, then skip recovering it.
    // This can happen if the namenode and client start recovering the same
    // file at the same time.
    synchronized (ongoingRecovery) {
      Block tmp = new Block();
      tmp.set(block.getBlockId(), block.getNumBytes(), GenerationStamp.WILDCARD_STAMP);
      if (ongoingRecovery.get(tmp) != null) {
        String msg = "Block " + block + " is already being recovered, " +
                     " ignoring this request to recover it.";
        LOG.info(msg);
        throw new IOException(msg);
      }
      ongoingRecovery.put(block, block);
    }
    try {
      BlockRecoveryCoordinator brc = new BlockRecoveryCoordinator(LOG,
          getConf(), socketTimeout, this, namespaceManager.get(namespaceId),
          getDNRegistrationForNS(namespaceId));
      
      return brc.recoverBlock(namespaceId, block, keepLength, datanodeids, closeFile,
          deadline);
    } finally {
      synchronized (ongoingRecovery) {
        ongoingRecovery.remove(block);
      }
    }
  }

  // ClientDataNodeProtocol implementation
  /** {@inheritDoc} */
  public LocatedBlock recoverBlock(Block block, boolean keepLength,
      DatanodeInfo[] targets) throws IOException {
    // old client: use default namespace
    return recoverBlock(getAllNamespaces()[0], block, keepLength, targets);
  }

  @Override
  public LocatedBlock recoverBlock(int namespaceId, Block block,
      boolean keepLength, DatanodeInfo[] targets, long deadline)
      throws IOException {
    
    logRecoverBlock("Client", namespaceId, block, targets);
    long myDeadline = Math.min(deadline, this.blkRecoveryTimeout);
    return recoverBlock(namespaceId, block, keepLength, targets, false,
        myDeadline);
  }

  @Override
  public LocatedBlock recoverBlock(int namespaceId, Block block,
      boolean keepLength, DatanodeInfo[] targets) throws IOException {
    logRecoverBlock("Client", namespaceId, block, targets);
    return recoverBlock(namespaceId, block, keepLength, targets, false, 0);
  }

  /** {@inheritDoc} */
  public Block getBlockInfo(Block block) throws IOException {
    return getBlockInfo(DataNode.PKT_HEADER_LEN, block);
  }
  
  @Override
  public Block getBlockInfo(int namespaceId, Block block) throws IOException {
    
    Block stored = data.getStoredBlock(namespaceId, block.getBlockId());
    return stored;
  }

  @Override
  public void copyBlockLocal(String srcFileSystem,
      int srcNamespaceId, Block srcBlock,
      int dstNamespaceId, Block dstBlock, String srcBlockFilePath)
      throws IOException {
    File srcBlockFile = new File(srcBlockFilePath);
    if (!srcBlockFile.exists()) {
      throw new FileNotFoundException("File " + srcBlockFilePath
          + " could not be found");
    }
    blockCopyExecutor.submit(new LocalBlockCopy(srcFileSystem,
        srcNamespaceId, srcBlock,
        dstNamespaceId, dstBlock, true, srcBlockFile));
  }

  @Override
  public void copyBlock(Block srcBlock, Block destinationBlock,
      DatanodeInfo target) throws IOException {
    copyBlock(srcBlock, destinationBlock, target, true);
  }

  @Override
  public void copyBlock(Block srcBlock, Block destinationBlock,
      DatanodeInfo target, boolean async) throws IOException {
    throw new IOException(
        "Please upgrade your fastcopy tool to work with federated " +
        "HDFS clusters.");
  }
  
  @Override
  public void copyBlock(int srcNamespaceId, Block srcBlock, int dstNamespaceId,
      Block destinationBlock, DatanodeInfo target)
    throws IOException {
    copyBlock(srcNamespaceId, srcBlock,
        dstNamespaceId, destinationBlock, target, true);
  }
  
  @Override
  public void copyBlock(int srcNamespaceId, Block srcBlock, int dstNamespaceId,
      Block destinationBlock, DatanodeInfo target, boolean async)
      throws IOException {

    if (!data.isValidBlock(srcNamespaceId, srcBlock, true)) {
      // block does not exist or is under-construction
      String errStr = "copyBlock: Can't send invalid block " + srcBlock 
                    + " at " + srcNamespaceId;
      LOG.info(errStr);
      throw new IOException(errStr);
    }

    // Check if specified length matches on-disk length 
    long onDiskLength = data.getFinalizedBlockLength(srcNamespaceId, srcBlock);
    if (srcBlock.getNumBytes() > onDiskLength) {
      // Shorter on-disk len indicates corruption so report NN the corrupt block
      String msg = "copyBlock: Can't replicate block " + srcBlock
          + " at " + srcNamespaceId
          + " because on-disk length " + onDiskLength
          + " is shorter than provided length " + srcBlock.getNumBytes();
      LOG.info(msg);
      throw new IOException(msg);
    }

    LOG.info(getDatanodeInfo() + " copyBlock: Starting thread to transfer: " +
             "srcNamespaceId: " + srcNamespaceId + " block: " +
             srcBlock + " to " + target.getName());
    DatanodeInfo[] targets = new DatanodeInfo[1];
    targets[0] = target;

    // Use IP Address and port number to determine locality. Relying on the
    // DatanodeID of both the target machine and the local machine to
    // determine locality. This guarantees uniformity in comparison.
    String targetMachine = target.getHost();
    int targetPort = target.getPort();
    DatanodeRegistration dnRegistration = getDNRegistrationForNS(srcNamespaceId);
    int localPort = dnRegistration.getPort();
    String localMachine = dnRegistration.getHost();

    Future<Boolean> result;
    // If the target datanode is our datanode itself, then perform local copy.
    if (targetMachine.equals(localMachine) && targetPort == localPort) {
      LOG.info("Performing local block copy since source and "
          + "destination datanodes are same for  block "
          + srcBlock.getBlockName());
      result = blockCopyExecutor.submit(new LocalBlockCopy(srcNamespaceId,
          srcBlock, dstNamespaceId, destinationBlock));
    } else if (targetMachine.equals(localMachine)) {
      LOG.info("Performing cross datanode local block copy since source " +
          "and destination hosts are same for block "
          + srcBlock.getBlockName());
      result = blockCopyExecutor.submit(new CrossDatanodeLocalBlockCopy(
          srcNamespaceId, srcBlock, dstNamespaceId, destinationBlock, target));
    } else {
      result = blockCopyExecutor.submit(new DataTransfer(targets, srcNamespaceId, srcBlock,
          dstNamespaceId, destinationBlock, this));
    }

    // If this is not an async request, wait for the task to complete, if the
    // task fails this will throw an exception and will be propogated to the
    // client.
    if (!async) {
      try {
        // Wait for 5 minutes.
        result.get(this.blockCopyRPCWaitTime, TimeUnit.SECONDS);
      } catch (Exception e) {
        LOG.error(e);
        throw new IOException(e);
      }
    }
  }

  private static void logRecoverBlock(String who, int namespaceId,
      Block block, DatanodeID[] targets) {
    StringBuilder msg = new StringBuilder(targets[0].getName());
    for (int i = 1; i < targets.length; i++) {
      msg.append(", " + targets[i].getName());
    }
    LOG.info(who + " calls recoverBlock(namespace_id =" + namespaceId +
        " block=" + block
        + ", targets=[" + msg + "])");
  }

  class CrossDatanodeLocalBlockCopy implements Callable<Boolean> {
    private final int srcNamespaceId;
    private final Block srcBlock;
    private final int dstNamespaceId;
    private Block dstBlock;
    private final DatanodeInfo target;
    private final String srcFileSystem;

    public CrossDatanodeLocalBlockCopy(int srcNamespaceId, Block srcBlock,
        int dstNamespaceId, Block dstBlock,
        DatanodeInfo target) throws IOException {
      this.srcNamespaceId = srcNamespaceId;
      this.srcBlock = srcBlock;
      this.dstNamespaceId = dstNamespaceId;
      this.dstBlock = dstBlock;
      this.target = target;
      this.srcFileSystem = data.getFileSystemForBlock(srcNamespaceId, srcBlock);
    }

    public Boolean call() throws Exception {
      InterDatanodeProtocol remoteDatanode = null;
      try {
        File srcBlockFile = data.getBlockFile(srcNamespaceId, srcBlock);
        remoteDatanode = DataNode
            .createInterDataNodeProtocolProxy(target, getConf(), socketTimeout);
        remoteDatanode.copyBlockLocal(srcFileSystem, srcNamespaceId, srcBlock,
            dstNamespaceId, dstBlock,
            srcBlockFile.getAbsolutePath());
      } catch (IOException e) {
        LOG.warn("Cross datanode local block copy failed", e);
        throw e;
      } finally {
        if (remoteDatanode != null) {
          stopDatanodeProxy(remoteDatanode);
        }
      }
      return true;
    }
  }

  class LocalBlockCopy implements Callable<Boolean> {
    private final Block srcBlock;
    private final Block dstBlock;
    private final int srcNamespaceId;
    private final int dstNamespaceId;
    // Whether or not this copy is a copy across two datanodes on the same host.
    private final boolean crossDatanode;
    private final File srcBlockFile;
    private final String srcFileSystem;

    public LocalBlockCopy(int srcNamespaceId, Block srcBlock,
        int dstNamespaceId, Block dstBlock) throws IOException {
      this(null, srcNamespaceId, srcBlock, dstNamespaceId, dstBlock, false, null);
    }

    public LocalBlockCopy(String srcFileSystem,
        int srcNamespaceId, Block srcBlock,
        int dstNamespaceId, Block dstBlock,
        boolean crossDatanode, File srcBlockFile) throws IOException {
      this.srcBlock = srcBlock;
      this.dstBlock = dstBlock;
      this.srcNamespaceId = srcNamespaceId;
      this.dstNamespaceId = dstNamespaceId;
      this.crossDatanode = crossDatanode;
      this.srcBlockFile = srcBlockFile;
      this.srcFileSystem = (srcFileSystem != null) ? srcFileSystem :
        data.getFileSystemForBlock(srcNamespaceId, srcBlock);
    }

    public Boolean call() throws Exception {
      try {
        if (crossDatanode) {
          data.copyBlockLocal(srcFileSystem, srcBlockFile,
              srcNamespaceId, srcBlock, dstNamespaceId, dstBlock);
        } else {
          data.copyBlockLocal(srcFileSystem,
              data.getBlockFile(srcNamespaceId, srcBlock),
              srcNamespaceId, srcBlock, dstNamespaceId, dstBlock);
        }
        dstBlock.setNumBytes(srcBlock.getNumBytes());
        notifyNamenodeReceivedBlock(dstNamespaceId, dstBlock, null);
        blockScanner.addBlock(dstNamespaceId, dstBlock);
      } catch (Exception e) {
        LOG.warn("Local block copy for src : " + srcBlock.getBlockName()
            + ", dst : " + dstBlock.getBlockName() + " failed", e);
        throw e;
      }
      return true;
    }
  }

  public void reportBadBlocks(int namespaceId, LocatedBlock[] blocks)
      throws IOException {
    NamespaceService nsos = namespaceManager.get(namespaceId);
    if(nsos == null) {
      throw new IOException("cannot locate OfferService thread for namespace=" + namespaceId);
    }
    nsos.reportBadBlocks(blocks);
  }
  
  public UpgradeManagerDatanode getUpgradeManager(int namespaceId) {
    NamespaceService nsos = namespaceManager.get(namespaceId);
    return nsos == null ? null : nsos.getUpgradeManager();
  }
  
  public void completeUpgrade() throws IOException{
    for(int namespaceId : namespaceManager.getAllNamespaces()){
      UpgradeManagerDatanode manager = namespaceManager.get(namespaceId).getUpgradeManager();
      manager.completeUpgrade();
    }
  }
  
  /**
   * See {@link DataBlockScanner}
   */
  private synchronized void initDataBlockScanner(Configuration conf) {
    if (blockScanner != null) {
      return;
    }
    //initialize periodic block scanner
    String reason = null;
    if (conf.getInt("dfs.datanode.scan.period.hours", 0) < 0) {
      reason = "verification is turned off by configuration";
    } else if ( !(data instanceof FSDataset) ) {
      reason = "verifcation is supported only with FSDataset";
    } 

    if ( reason == null ) {
      blockScanner = new DataBlockScannerSet(this, (FSDataset)data, conf);      
    } else {
      LOG.info("Periodic Block Verification is disabled because " +
               reason + ".");
    }
  }
  
  /** 
   * Get host:port with host set to Datanode host and port set to the
   * port {@link DataXceiver} is serving.
   * @return host:port string
   */
  public String getMachineName() {
    return machineName + ":" + selfAddr.getPort();
  }

  public long getCTime(int namespaceId) {
    return storage.getNStorage(namespaceId).getCTime();
  }
  
  public String getStorageID() {
    return storage.getStorageID();
  }

  /** 
   * Get DataNode info - used primarily for logging
   */
  public String getDatanodeInfo() {
    return machineName + ":" + selfAddr.getPort() 
      + "; storageID= " + storage.getStorageID();
  }
  
  /**
   * Return true if the given namespace is alive.
   * @param namespaceId
   * @return true if the namespace is alive, false otherwise
   */
  public boolean isNamespaceAlive(int namespaceId) {
    return namespaceManager.isAlive(namespaceId);
  }
  
  /**
   * Return true if the given namespace is alive.
   * @param addr 
   * @return true if the namespace is alive, false otherwise
   */
  public boolean isNamespaceAlive(InetSocketAddress addr) {
    return namespaceManager.get(addr).isAlive();
  }
  
  public Integer[] getAllNamespaces(){
    return namespaceManager.getAllNamespaces();
  }
  
  public NamespaceService[] getAllNamespaceServices() {
    return namespaceManager.getAllNamenodeThreads();
  }

  /**
   * This method makes data node to send block report
   */
  public void scheduleNSBlockReport(long delay) {
    for (NamespaceService nsos : namespaceManager.getAllNamenodeThreads()) {
      nsos.scheduleBlockReport(delay);
    }
  }
  
  /**
   * This method makes data node to send blockReceivedAndDelete report
   */
  public void scheduleNSBlockReceivedAndDeleted(long delay) {
    for (NamespaceService nsos : namespaceManager.getAllNamenodeThreads()) {
      nsos.scheduleBlockReceivedAndDeleted(delay);
    }
  }

  public void refreshNamenodes(Configuration conf) throws IOException {
    LOG.info("refresh namenodes");
    try {
      List<InetSocketAddress> nameNodeAddrs = DFSUtil.getNNServiceRpcAddresses(conf);
      namespaceManager.refreshNamenodes(nameNodeAddrs, conf);
    } catch (InterruptedException e) {
      throw new IOException(e.getCause());
    }
  }

  //ClientDataNodeProtocol implementation
  /* {@inheritDoc} */
  /**
   * This method refreshes all name nodes served by the datanode
   */
  public void refreshNamenodes() throws IOException {
    conf = new Configuration();
    refreshNamenodes(conf);
  }
  
  @Override
  public void refreshOfferService(String serviceName) throws IOException {
    throw new IOException("Datanode doesn't support refresh of offerservices");
  }
  
  public void refreshDataDirs(String confVolumes) throws IOException {
    try{
      //Refresh the volumes using default configuration path
      if (confVolumes.equals("--defaultPath")) {
        Configuration conf = getConf();
        confVolumes = conf.get("dfs.datadir.confpath");
      }
      DataDirFileReader reader = new DataDirFileReader(confVolumes);
      this.refreshVolumes(reader.getNewDirectories()); 
    } catch (Exception e) {
      LOG.error("Cannot refresh the data dirs of the node Exception: " + e);      
      return;
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void reconfigurePropertyImpl(String property, String newVal) 
    throws ReconfigurationException {
    if (property.equals("dfs.data.dir")) {
      try {
        LOG.info("Reconfigure " + property + " to " + newVal);
        this.refreshVolumes(newVal);
      } catch (Exception e) {
        throw new ReconfigurationException(property, 
        newVal, getConf().get(property), e);
      }
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
    List<String> changeable = 
      Arrays.asList("dfs.data.dir");
    return changeable;
  }
  
  //@Override PulseCheckable
  public Boolean isAlive() {
    return isDatanodeUp() && isDataNodeBeingAlive();
  }
  
  private ObjectName datanodeMXBeanName;
  
  /**
   * Register DataNodeMXBean
   */
  private void registerMXBean() {   
    this.pulseChecker = PulseChecker.create(this, "DataNode");
    datanodeMXBeanName = MBeanUtil.registerMBean("DataNode", "DataNodeInfo", this);
  }
  
  private void shutdownMXBean() {
    if (datanodeMXBeanName != null) {
      MBeanUtil.unregisterMBean(datanodeMXBeanName);
    }
    if (pulseChecker != null) {
      pulseChecker.shutdown();
    }
  }
  
  @Override // DataNodeMXBean
  public String getVersion() {
    return VersionInfo.getVersion();
  }

  @Override // DataNodeMXBean
  public String getRpcPort(){
    return Integer.toString(this.ipcServer.getListenerAddress().getPort());
  }

  @Override // DataNodeMXBean
  public String getHttpPort(){
    return Integer.toString(this.infoServer.getPort());
  }

  /**
   * Returned information is a JSON representation of a map with
   * name node host name as the key and block pool Id as the value
   */
  @Override // DataNodeMXBean
  public String getNamenodeAddresses() {
    final Map<String, Integer> info = new HashMap<String, Integer>();
    for (NamespaceService ns : namespaceManager.getAllNamenodeThreads()) {
      if (ns != null && ns.initialized()) {
        info.put(ns.getNNSocketAddress().getAddress().getHostAddress(), ns.getNamespaceId());
      }
    }
    return JSON.toString(info);
  }

  /**
   * Returned information is a JSON representation of a map with
   * volume name as the key and value is a map of volume attribute
   * keys to its values
   */
  @Override // DataNodeMXBean
  public String getVolumeInfo() {
    final Map<String, Object> info = new HashMap<String, Object>();
    try {
      FSVolume[] volumes = ((FSDataset)this.data).volumes.getVolumes();
      for (FSVolume v : volumes) {
        final Map<String, Object> innerInfo = new HashMap<String, Object>();
        innerInfo.put("usedSpace", v.getDfsUsed());
        innerInfo.put("freeSpace", v.getAvailable());
        innerInfo.put("reservedSpace", v.getReserved());
        info.put(v.getDir().toString(), innerInfo);
      }
      return JSON.toString(info);
    } catch (IOException e) {
      LOG.info("Cannot get volume info.", e);
      return "ERROR";
    }
  }

  @Override // DataNodeMXBean
  public String getServiceIds() {
    String nameserviceIdList = "";
    for (NamespaceService ns : namespaceManager.getAllNamenodeThreads()) {
      if (ns != null && ns.initialized()) {
        String nameserviceId = ns.getNameserviceId();
        if (nameserviceIdList.length() > 0) {
          nameserviceIdList += ",";
        }
        if (nameserviceId == null) {
          // Non-federation version, should be only one namespace
          nameserviceId = "NONFEDERATION";
        }
        nameserviceIdList += nameserviceId;
      }
    }
    return nameserviceIdList;
  }

  /**
   * Sends a 'Blocks Being Written' report to the given node.
   *
   * @param node the node to send the report to
   * @throws IOException
   */
  public void sendBlocksBeingWrittenReport(DatanodeProtocol node,
      int namespaceId, DatanodeRegistration nsRegistration) throws IOException {
    Block[] blocks = data.getBlocksBeingWrittenReport(namespaceId);
    if (blocks != null && blocks.length != 0) {
      long[] blocksAsLong =
        BlockListAsLongs.convertToArrayLongs(blocks);
      BlockReport bbwReport = new BlockReport(blocksAsLong);
      node.blocksBeingWrittenReport(nsRegistration, bbwReport);
    }
  }
  
  public void updateAndReportThreadLiveness(BackgroundThread thread) {
    if (threadLivenessReporter != null) {
      threadLivenessReporter.reportLiveness(thread);
    }
  }

  @Override
  public void removeNamespace(String nameserviceId) throws IOException {
    NamespaceService ns = namespaceManager.get(nameserviceId);
    if (ns != null) {
      namespaceManager.remove(ns.getNNSocketAddress());
      ns.stop();
    } else {
      throw new IOException("Service with id " + nameserviceId +
          " does not exist");
    }
  }

  static public void stopAllProxies(List<InterDatanodeProtocol> datanodeProxies) {
    // safe to stop proxies now
    for (InterDatanodeProtocol proxy : datanodeProxies) {
      stopDatanodeProxy(proxy);
    }
  }
  
  static public void stopDatanodeProxy(InterDatanodeProtocol datanode) {
    // if this is a proxy instance, close it
    if (Proxy.isProxyClass(datanode.getClass())) {
      RPC.stopProxy(datanode);
    }
  }
  
  /**
   * Only used for testing
   */
  public void finalizeAndNotifyNamenode(int namespaceId, Block blk)
      throws IOException {
    this.getFSDataset().finalizeBlock(namespaceId, blk);
    this.notifyNamenodeReceivedBlock(namespaceId, blk, null);
  }
}
