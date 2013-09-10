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

import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.WeakHashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Random;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.*;

import javax.net.SocketFactory;
import javax.security.auth.login.LoginException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedBlockFileStatus;
import org.apache.hadoop.fs.OpenFileInfo;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.MD5MD5CRC32FileChecksum;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DistributedFileSystem.DiskStatus;
import org.apache.hadoop.hdfs.metrics.DFSClientMetrics;
import org.apache.hadoop.hdfs.metrics.DFSQuorumReadMetrics;
import org.apache.hadoop.hdfs.profiling.DFSReadProfilingData;
import org.apache.hadoop.hdfs.profiling.DFSWriteProfilingData;
import org.apache.hadoop.hdfs.protocol.AlreadyBeingCreatedException;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.BlockChecksumHeader;
import org.apache.hadoop.hdfs.protocol.BlockCrcHeader;
import org.apache.hadoop.hdfs.protocol.ClientDatanodeProtocol;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.protocol.CorruptFileBlocks;
import org.apache.hadoop.io.ReadOptions;
import org.apache.hadoop.io.WriteOptions;
import org.apache.hadoop.hdfs.protocol.DSQuotaExceededException;
import org.apache.hadoop.hdfs.protocol.DataTransferProtocol;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.DirectoryListing;
import org.apache.hadoop.hdfs.protocol.FSConstants;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlockWithFileName;
import org.apache.hadoop.hdfs.protocol.LocatedBlockWithOldGS;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.protocol.LocatedBlockWithMetaInfo;
import org.apache.hadoop.hdfs.protocol.LocatedBlocksWithMetaInfo;
import org.apache.hadoop.hdfs.protocol.LocatedDirectoryListing;
import org.apache.hadoop.hdfs.protocol.NSQuotaExceededException;
import org.apache.hadoop.hdfs.protocol.ProtocolCompatible;
import org.apache.hadoop.hdfs.server.common.HdfsConstants;
import org.apache.hadoop.hdfs.server.common.UpgradeStatusReport;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.namenode.LeaseExpiredException;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.util.DaemonThreadFactory;
import org.apache.hadoop.hdfs.util.InjectionEvent;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.MD5Hash;
import org.apache.hadoop.io.retry.RetryPolicies;
import org.apache.hadoop.io.retry.RetryPolicy;
import org.apache.hadoop.io.retry.RetryProxy;
import org.apache.hadoop.ipc.Client;
import org.apache.hadoop.ipc.ProtocolProxy;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.net.DNSToSwitchMapping;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.net.ScriptBasedMapping;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.CrcConcat;
import org.apache.hadoop.util.Daemon;
import org.apache.hadoop.util.InjectionHandler;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.StringUtils;

/********************************************************
 * DFSClient can connect to a Hadoop Filesystem and
 * perform basic file tasks.  It uses the ClientProtocol
 * to communicate with a NameNode daemon, and connects
 * directly to DataNodes to read/write block data.
 *
 * Hadoop DFS users should obtain an instance of
 * DistributedFileSystem, which uses DFSClient to handle
 * filesystem tasks.
 *
 ********************************************************/
public class DFSClient implements FSConstants, java.io.Closeable {

  public static final Log LOG = LogFactory.getLog(DFSClient.class);
  public static final int MAX_BLOCK_ACQUIRE_FAILURES = 3;
  static final int TCP_WINDOW_SIZE = 128 * 1024; // 128 KB
  static byte[] emptyByteArray = new byte[0];
  
  public static final String DFS_CLIENT_SOCKET_CACHE_CAPACITY_KEY = 
      "dfs.client.socketcache.capacity";
  public static final int DFS_CLIENT_SOCKET_CACHE_CAPACITY_DEFAULT = 16;
  
  public static final String DFS_CLIENT_CACHED_CONN_RETRY_KEY = 
      "dfs.client.cached.conn.retry";
  public static final int DFS_CLIENT_CACHED_CONN_RETRY_DEFAULT = 3;
  
  public ClientProtocol namenode;
  private ClientProtocol rpcNamenode;
  // Namenode proxy that supports method-based compatibility
  public ProtocolProxy<ClientProtocol> namenodeProtocolProxy = null;
  public Object namenodeProxySyncObj = new Object();
  final UserGroupInformation ugi;
  volatile boolean clientRunning = true;
  static Random r = new Random();
  final String clientName;
  final LeaseChecker leasechecker;
  Configuration conf;
  long defaultBlockSize;
  private short defaultReplication;
  final private int preferredPacketVersion;
  SocketFactory socketFactory;
  int namenodeRPCSocketTimeout;
  int socketTimeout;
  int socketReadExtentionTimeout;
  int datanodeWriteTimeout;
  int datanodeWriteExtentionTimeout;
  int timeoutValue;  // read timeout for the socket
  final int writePacketSize;
  final long bytesToCheckReadSpeed;
  final long minReadSpeedBps;
  final boolean enableThrowForSlow;
  final FileSystem.Statistics stats;
  int maxBlockAcquireFailures;
  final int hdfsTimeout;    // timeout value for a DFS operation.
  // The amount of time to wait before aborting a close file.
  final SocketCache socketCache;
  
  private final long closeFileTimeout;
  private long namenodeVersion = ClientProtocol.versionID;
  DFSClientMetrics metrics;
  protected Integer dataTransferVersion = -1;
  protected volatile int namespaceId = 0;
  boolean shortCircuitLocalReads = false;
  boolean shortcircuitDisableWhenFail = true;
  final InetAddress localHost;
  InetSocketAddress nameNodeAddr;
  private DatanodeInfo pseuDatanodeInfoForLocalhost;
  private String localhostNetworkLocation = null;
  DNSToSwitchMapping dnsToSwitchMapping = null;
  int ipTosValue = NetUtils.NOT_SET_IP_TOS;
  
  final BlockLocationRenewal blockLocationRenewal;

  private final static ThreadLocal<DFSReadProfilingData> dfsReadContext = 
      new ThreadLocal<DFSReadProfilingData>();

  private final static ThreadLocal<DFSWriteProfilingData> profileForNextOutputStream =
      new ThreadLocal<DFSWriteProfilingData>();
  
  protected long quorumReadThresholdMillis;
  public DFSQuorumReadMetrics quorumReadMetrics;
  
  private FileStatusCache fileStatusCache;
  
  public void setQuorumReadTimeout(long timeoutMillis) {
    this.quorumReadThresholdMillis = timeoutMillis;
  }
  
  public volatile boolean allowParallelReads = false;
  
  private final DistributedFileSystem dfs;

  public void enableParallelReads() {
    allowParallelReads = true;
  }
  public void disableParallelReads() {
    allowParallelReads = false;
  }
  
  protected ThreadPoolExecutor parallelReadsThreadPool = null;

  public void setNumParallelThreadsForReads(int num) {
    if (this.parallelReadsThreadPool == null) {
      this.parallelReadsThreadPool = new ThreadPoolExecutor(1,
          num, 60, TimeUnit.SECONDS,
        new SynchronousQueue<Runnable>(),
        new DaemonThreadFactory("dfsclient-reader-thread-"),
        new ThreadPoolExecutor.CallerRunsPolicy() {
        
          public void rejectedExecution(Runnable r, ThreadPoolExecutor e) {
            // increment metrics
            LOG.debug("Execution rejected, Executing in current thread");
            quorumReadMetrics.incParallelReadOpsInCurThread();
            
            // will run in the current thread
            super.rejectedExecution(r, e);
          }
          
      });
      parallelReadsThreadPool.allowCoreThreadTimeOut(true);
    } else {
      this.parallelReadsThreadPool.setMaximumPoolSize(num);
    }
  }
  
  public static void startReadProfiling() {
    dfsReadContext.set(new DFSReadProfilingData());
  }
  
  public static void stopReadProfiling() {
    dfsReadContext.remove();
  }
  
  public static DFSReadProfilingData getDFSReadProfilingData() {
    return dfsReadContext.get();
  }

  public static void setProfileDataForNextOutputStream(
      DFSWriteProfilingData profile) {
    profileForNextOutputStream.set(profile);
  }

  public static DFSWriteProfilingData getAndResetProfileDataForNextOutputStream() {
    DFSWriteProfilingData profile = profileForNextOutputStream.get();
    profileForNextOutputStream.remove();
    return profile;
  }
  
  /**
   * This variable tracks the number of failures for each thread of 
   * dfs input stream since the start of the most recent user-facing operation. 
   * That is to say, it should be reset
   * whenever the user makes a call on this stream, and if at any point
   * during the retry logic, the failure count exceeds a threshold,
   * the errors will be thrown back to the operation.
   *
   * Specifically this counts the number of times the client has gone
   * back to the namenode to get a new list of block locations, and is
   * capped at maxBlockAcquireFailures
   * 
   */
  static ThreadLocal<Integer> dfsInputStreamfailures = 
    new ThreadLocal<Integer>();

  /**
   * The locking hierarchy is to first acquire lock on DFSClient object, followed by
   * lock on leasechecker, followed by lock on an individual DFSOutputStream.
   */
  public static ClientProtocol createNamenode(Configuration conf) throws IOException {
    return createNamenode(NameNode.getClientProtocolAddress(conf), conf);
  }

  public static ClientProtocol createNamenode( InetSocketAddress nameNodeAddr,
      Configuration conf) throws IOException {
    try {
      return createNamenode(
          createRPCNamenode(nameNodeAddr, conf,
              UserGroupInformation.getUGI(conf),
              conf.getInt(DFS_CLIENT_NAMENODE_SOCKET_TIMEOUT, 0)).getProxy(),
          conf);
    } catch (LoginException e) {
      throw (IOException)(new IOException().initCause(e));
    }
  }
  
  /**
   * Create a NameNode proxy for the client if the client and NameNode
   * are compatible
   *
   * @param nameNodeAddr NameNode address
   * @param conf configuration
   * @param ugi ticket
   * @return a NameNode proxy that's compatible with the client
   */
  private void createRPCNamenodeIfCompatible(
      InetSocketAddress nameNodeAddr,
      Configuration conf,
      UserGroupInformation ugi) throws IOException {
    try {
      this.namenodeProtocolProxy = createRPCNamenode(nameNodeAddr, conf, ugi,
          namenodeRPCSocketTimeout);
      this.rpcNamenode = namenodeProtocolProxy.getProxy();
    } catch (RPC.VersionMismatch e) {
      long clientVersion = e.getClientVersion();
      namenodeVersion = e.getServerVersion();
      if (clientVersion > namenodeVersion &&
          !ProtocolCompatible.isCompatibleClientProtocol(
              clientVersion, namenodeVersion)) {
        throw new RPC.VersionIncompatible(
            ClientProtocol.class.getName(), clientVersion, namenodeVersion);
      }
      this.rpcNamenode = (ClientProtocol)e.getProxy();
    }
  }

  public static ProtocolProxy<ClientProtocol> createRPCNamenode(
      Configuration conf) throws IOException {
    try {
      return createRPCNamenode(NameNode.getClientProtocolAddress(conf), conf,
          UserGroupInformation.getUGI(conf),
          conf.getInt(DFS_CLIENT_NAMENODE_SOCKET_TIMEOUT, 0));
    } catch (LoginException e) {
      throw new IOException(e);
    }   
  }

  public static ProtocolProxy<ClientProtocol> createRPCNamenode(InetSocketAddress nameNodeAddr,
      Configuration conf, UserGroupInformation ugi, int rpcTimeout)
    throws IOException {
    return RPC.getProtocolProxy(ClientProtocol.class,
        ClientProtocol.versionID, nameNodeAddr, ugi, conf,
        NetUtils.getSocketFactory(conf, ClientProtocol.class), rpcTimeout);
  }

  static ClientProtocol createNamenode(ClientProtocol rpcNamenode,
      Configuration conf)
    throws IOException {
    long sleepTime = conf.getLong("dfs.client.rpc.retry.sleep",
        LEASE_SOFTLIMIT_PERIOD);
    RetryPolicy createPolicy = RetryPolicies.retryUpToMaximumCountWithFixedSleep(
        5, sleepTime, TimeUnit.MILLISECONDS);

    Map<Class<? extends Exception>,RetryPolicy> remoteExceptionToPolicyMap =
      new HashMap<Class<? extends Exception>, RetryPolicy>();
    remoteExceptionToPolicyMap.put(AlreadyBeingCreatedException.class, createPolicy);

    Map<Class<? extends Exception>,RetryPolicy> exceptionToPolicyMap =
      new HashMap<Class<? extends Exception>, RetryPolicy>();
    exceptionToPolicyMap.put(RemoteException.class,
        RetryPolicies.retryByRemoteException(
            RetryPolicies.TRY_ONCE_THEN_FAIL, remoteExceptionToPolicyMap));
    RetryPolicy methodPolicy = RetryPolicies.retryByException(
        RetryPolicies.TRY_ONCE_THEN_FAIL, exceptionToPolicyMap);
    Map<String,RetryPolicy> methodNameToPolicyMap = new HashMap<String,RetryPolicy>();

    methodNameToPolicyMap.put("create", methodPolicy);

    return (ClientProtocol) RetryProxy.create(ClientProtocol.class,
        rpcNamenode, methodNameToPolicyMap);
  }

  public static ClientDatanodeProtocol createClientDatanodeProtocolProxy(
      DatanodeID datanodeid, Configuration conf, int socketTimeout)
      throws IOException {
    return createClientDNProtocolProxy(datanodeid, conf, socketTimeout).getProxy();
  }
  
  static ProtocolProxy<ClientDatanodeProtocol> createClientDNProtocolProxy (
      DatanodeID datanodeid, Configuration conf, int socketTimeout)
      throws IOException {
    InetSocketAddress addr = NetUtils.createSocketAddr(
      datanodeid.getHost() + ":" + datanodeid.getIpcPort());
    if (ClientDatanodeProtocol.LOG.isDebugEnabled()) {
      ClientDatanodeProtocol.LOG.info("ClientDatanodeProtocol addr=" + addr);
    }
    UserGroupInformation ugi;
    try {
      ugi = UserGroupInformation.login(conf);
    } catch (LoginException le) {
      throw new RuntimeException("Couldn't login!");
    }

    return RPC.getProtocolProxy(ClientDatanodeProtocol.class,
        ClientDatanodeProtocol.versionID, addr, ugi, conf,
        NetUtils.getDefaultSocketFactory(conf), socketTimeout);
  }

  /**
   * Same as this(NameNode.getClientProtocolAddress(conf), conf);
   * @see #DFSClient(InetSocketAddress, Configuration)
   */
  public DFSClient(Configuration conf) throws IOException {
    this(NameNode.getClientProtocolAddress(conf), conf);
  }

  /**
   * Same as this(nameNodeAddr, conf, null);
   * @see #DFSClient(InetSocketAddress, Configuration, org.apache.hadoop.fs.FileSystem.Statistics)
   */
  public DFSClient(InetSocketAddress nameNodeAddr, Configuration conf
      ) throws IOException {
    this(nameNodeAddr, conf, null);
  }

  /**
   * Same as this(nameNodeAddr, null, conf, stats);
   * @see #DFSClient(InetSocketAddress, ClientProtocol, Configuration, org.apache.hadoop.fs.FileSystem.Statistics)
   */
  public DFSClient(InetSocketAddress nameNodeAddr, Configuration conf,
                   FileSystem.Statistics stats)
    throws IOException {
    this(nameNodeAddr, null, conf, stats);
  }

  /**
   * Create a new DFSClient connected to the given nameNodeAddr or rpcNamenode.
   * Exactly one of nameNodeAddr or rpcNamenode must be null.
   */
  DFSClient(InetSocketAddress nameNodeAddr, ClientProtocol rpcNamenode,
      Configuration conf, FileSystem.Statistics stats)
    throws IOException {
    this(nameNodeAddr, rpcNamenode, conf, stats, 0, null);
  }
  /**
   * Create a new DFSClient connected to the given nameNodeAddr or rpcNamenode.
   * Exactly one of nameNodeAddr or rpcNamenode must be null.
   */
  DFSClient(InetSocketAddress nameNodeAddr, ClientProtocol rpcNamenode,
      Configuration conf, FileSystem.Statistics stats, long uniqueId,
      DistributedFileSystem dfs)
    throws IOException {
    this.dfs = dfs;
    this.conf = conf;
    this.metrics = new DFSClientMetrics(conf.getBoolean(
        "dfs.client.metrics.enable", false));
    this.stats = stats;
    this.socketTimeout = conf.getInt("dfs.socket.timeout",
                                     HdfsConstants.READ_TIMEOUT);
    this.socketReadExtentionTimeout = conf.getInt(
        HdfsConstants.DFS_DATANODE_READ_EXTENSION,
        HdfsConstants.READ_TIMEOUT_EXTENSION);
    this.namenodeRPCSocketTimeout = conf.getInt(
        DFS_CLIENT_NAMENODE_SOCKET_TIMEOUT, 0);
    this.timeoutValue = this.socketTimeout;
    this.datanodeWriteTimeout = conf.getInt("dfs.datanode.socket.write.timeout",
                                            HdfsConstants.WRITE_TIMEOUT);
    this.datanodeWriteExtentionTimeout = conf.getInt(
        HdfsConstants.DFS_DATANODE_WRITE_EXTENTSION,
        HdfsConstants.WRITE_TIMEOUT_EXTENSION);    
    this.socketFactory = NetUtils.getSocketFactory(conf, ClientProtocol.class);
    // dfs.write.packet.size is an internal config variable
    this.writePacketSize = conf.getInt("dfs.write.packet.size",
        HdfsConstants.DEFAULT_PACKETSIZE);
    this.bytesToCheckReadSpeed = conf.getLong("dfs.bytes.to.check.read.speed", 2 * 1024 * 1024);

    int fileStatusCacheSize = conf.getInt("dfs.filestatus.cache.size", 0);
    int fileStatusCacheExpireTime = conf.getInt("dfs.filestatus.cache.expiretime", 1000);
    this.fileStatusCache = fileStatusCacheSize != 0 ? new FileStatusCache(
        fileStatusCacheExpireTime, fileStatusCacheSize) : null;
    	
    // We determine which packet version to use (inline checksum or not) based
    // on whether inline cehcksum is enabled.
    // Notice that, datanode doesn't have to share the same configuration as the
    // client configuration here.
    // 
    // If the client set the inline checksum to be disable, the packets will
    // have checksums first. Datanodes can always accept the data, no matter whether
    // inline checksum is enabled or not.
    //
    // If the client set the inline checksum to be enabled, the packets will have
    // checksums inlined. Only data nodes enabled inline checksum are able to
    // accept those packets. Data nodes with inline checksum disabled will fail
    // to accept data.
    //
    // If a user is not sure the data node has inline checksum enabled, it
    // should set inline checksum to be disabled in client side.
    //
    if (conf.getBoolean(DFS_USE_INLINE_CHECKSUM_KEY, false)) {
      // non-inline checksum doesn't support this option.
      preferredPacketVersion = DataTransferProtocol.PACKET_VERSION_CHECKSUM_INLINE;
    } else {
      preferredPacketVersion = DataTransferProtocol.PACKET_VERSION_CHECKSUM_FIRST;
    }
    this.minReadSpeedBps = conf.getLong("dfs.min.read.speed.bps", -1);
    this.enableThrowForSlow = conf.getBoolean("dfs.read.switch.for.slow", true);
    this.maxBlockAcquireFailures = getMaxBlockAcquireFailures(conf);
    this.localHost = InetAddress.getLocalHost();
    
    // fetch network location of localhost
    this.pseuDatanodeInfoForLocalhost = new DatanodeInfo(new DatanodeID(
        this.localHost.getHostAddress()));
    this.dnsToSwitchMapping = ReflectionUtils.newInstance(
        conf.getClass("topology.node.switch.mapping.impl", ScriptBasedMapping.class,
          DNSToSwitchMapping.class), conf);
    ArrayList<String> tempList = new ArrayList<String>();
    tempList.add(this.localHost.getHostAddress());
    List<String> retList = dnsToSwitchMapping.resolve(tempList);
    if (retList != null && retList.size() > 0) {
      localhostNetworkLocation = retList.get(0);
      this.pseuDatanodeInfoForLocalhost.setNetworkLocation(localhostNetworkLocation);
    }

    this.quorumReadThresholdMillis = conf.getLong(
        HdfsConstants.DFS_DFSCLIENT_QUORUM_READ_THRESHOLD_MILLIS,
        HdfsConstants.DEFAULT_DFSCLIENT_QUORUM_READ_THRESHOLD_MILLIS);    
    int numThreads = conf.getInt(
        HdfsConstants.DFS_DFSCLIENT_QUORUM_READ_THREADPOOL_SIZE,
        HdfsConstants.DEFAULT_DFSCLIENT_QUORUM_READ_THREADPOOL_SIZE);    
    if (numThreads > 0) {
      this.enableParallelReads();
      this.setNumParallelThreadsForReads(numThreads);
    }
    this.quorumReadMetrics = new DFSQuorumReadMetrics();
    
    // The hdfsTimeout is currently the same as the ipc timeout
    this.hdfsTimeout = Client.getTimeout(conf);

    this.closeFileTimeout = conf.getLong("dfs.client.closefile.timeout", this.hdfsTimeout);

    try {
      this.ugi = UserGroupInformation.getUGI(conf);
    } catch (LoginException e) {
      throw (IOException)(new IOException().initCause(e));
    }

    String taskId = conf.get("mapred.task.id");
    if (taskId != null) {
      this.clientName = "DFSClient_" + taskId + "_" + r.nextInt()
                      + "_" + Thread.currentThread().getId();
    } else {
      this.clientName = "DFSClient_" + r.nextInt()
          + ((uniqueId == 0) ? "" : "_" + uniqueId);
    }
    defaultBlockSize = conf.getLong("dfs.block.size", DEFAULT_BLOCK_SIZE);
    defaultReplication = (short) conf.getInt("dfs.replication", 3);
    this.socketCache = new SocketCache(
        conf.getInt(DFS_CLIENT_SOCKET_CACHE_CAPACITY_KEY, 
            DFS_CLIENT_SOCKET_CACHE_CAPACITY_DEFAULT));

    if (nameNodeAddr != null && rpcNamenode == null) {
      this.nameNodeAddr = nameNodeAddr;
      getNameNode();
    } else if (nameNodeAddr == null && rpcNamenode != null) {
      //This case is used for testing.
      if (rpcNamenode instanceof NameNode) {
        this.namenodeProtocolProxy = createRPCNamenode(
            ((NameNode) rpcNamenode).getNameNodeAddress(), conf, ugi,
            namenodeRPCSocketTimeout);
      }
      this.namenode = this.rpcNamenode = rpcNamenode;
    } else {
      throw new IllegalArgumentException(
          "Expecting exactly one of nameNodeAddr and rpcNamenode being null: "
          + "nameNodeAddr=" + nameNodeAddr + ", rpcNamenode=" + rpcNamenode);
    }
    // read directly from the block file if configured.
    this.shortCircuitLocalReads = conf.getBoolean("dfs.read.shortcircuit", false);
    if (this.shortCircuitLocalReads) {
      LOG.debug("Configured to shortcircuit reads to " + localHost);
    }
    this.shortcircuitDisableWhenFail = conf.getBoolean(
        "dfs.read.shortcircuit.fallbackwhenfail", true);
    this.leasechecker = new LeaseChecker(this.clientName, this.conf);
    if (conf.getBoolean("dfs.client.block.location.renewal.enabled", false)) {
      this.blockLocationRenewal = new BlockLocationRenewal();
    } else {
      this.blockLocationRenewal = null;
    }
    // by default, if the ipTosValue is less than 0(for example -1), 
    // we will not set it in the socket.
    this.ipTosValue = conf.getInt(NetUtils.DFS_CLIENT_TOS_CONF, 
    							  NetUtils.NOT_SET_IP_TOS);
    if (this.ipTosValue > NetUtils.IP_TOS_MAX_VALUE) {
    	LOG.warn(NetUtils.DFS_CLIENT_TOS_CONF + " " + ipTosValue + 
    			 " exceeds the max allowed value " + NetUtils.IP_TOS_MAX_VALUE + 
    			 ", will not take affect");
    	this.ipTosValue = NetUtils.NOT_SET_IP_TOS;
    }
  }

  public ClientProtocol getNameNodeRPC() {
    return this.namenode;
  }

  private void getNameNode() throws IOException {
    if (nameNodeAddr != null) {
      // The lock is to make sure namenode, namenodeProtocolProxy
      // and rpcNamenode are consistent ultimately. There is still
      // a small window where another thread can see inconsistent
      // version of namenodeProtocolProxy and namenode. But it will
      // only happen during the transit time when name-node upgrade
      // and the exception will likely to be resolved after a retry.
      //
      synchronized (namenodeProxySyncObj) {
        createRPCNamenodeIfCompatible(nameNodeAddr, conf, ugi);
        this.namenode = (dfs == null) ? createNamenode(this.rpcNamenode, conf)
            : dfs.getNewNameNode(rpcNamenode, conf);
      }
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("Name node signature is refreshed. Fingerprint: "
          + namenodeProtocolProxy.getMethodsFingerprint());
    }
  }

  public String getClientName() {
    return clientName;
  }

  public void getNewNameNodeIfNeeded(int serverMethodFingerprint)
      throws IOException {
    if (serverMethodFingerprint != namenodeProtocolProxy
        .getMethodsFingerprint()
        || InjectionHandler
            .falseCondition(InjectionEvent.DFSCLIENT_FINGERPRINT_MISMATCH)) {
      LOG.info(String.format(
          "Different Namenode methods' fingerprint: client %s server %s ",
          namenodeProtocolProxy.getMethodsFingerprint(),
          serverMethodFingerprint));
      getNameNode();
      LOG.info("Namenode methods updated. New fingerprint: "
          + namenodeProtocolProxy.getMethodsFingerprint());
    }
  }
  
  static int getMaxBlockAcquireFailures(Configuration conf) {
    return conf.getInt("dfs.client.max.block.acquire.failures",
                       MAX_BLOCK_ACQUIRE_FAILURES);
  }
  
  public boolean isOpen() {
    return clientRunning;
  }

  protected void checkOpen() throws IOException {
    if (!clientRunning) {
      IOException result = new IOException("Filesystem closed");
      throw result;
    }
  }

  /**
   * Close the file system, abandoning all of the leases and files being
   * created and close connections to the namenode.
   */
  public synchronized void close() throws IOException {
    if(clientRunning) {
      leasechecker.close();
      leasechecker.closeRenewal();
      if (blockLocationRenewal != null) {
        blockLocationRenewal.stop();
      }
      clientRunning = false;
      try {
        leasechecker.interruptAndJoin();
        if (blockLocationRenewal != null) {
          blockLocationRenewal.join();
        }
      } catch (InterruptedException ie) {
      }

      // close connections to the namenode
      RPC.stopProxy(rpcNamenode);
    }
  }

  /**
   * Get DFSClientMetrics
   */
  
  public DFSClientMetrics getDFSClientMetrics(){
	  return metrics;
  }
  
  /**
   * Get the default block size for this cluster
   * @return the default block size in bytes
   */
  public long getDefaultBlockSize() {
    return defaultBlockSize;
  }

  public long getBlockSize(String f) throws IOException {
    try {
      return namenode.getPreferredBlockSize(f);
    } catch (IOException ie) {
      LOG.warn("Problem getting block size: " +
          StringUtils.stringifyException(ie));
      throw ie;
    }
  }

  /**
   * Report corrupt blocks that were discovered by the client.
   */
  public void reportBadBlocks(LocatedBlock[] blocks) throws IOException {
    namenode.reportBadBlocks(blocks);
  }

  public short getDefaultReplication() {
    return defaultReplication;
  }

  /**
   *  @deprecated Use getBlockLocations instead
   *
   * Get hints about the location of the indicated block(s).
   *
   * getHints() returns a list of hostnames that store data for
   * a specific file region.  It returns a set of hostnames for
   * every block within the indicated region.
   *
   * This function is very useful when writing code that considers
   * data-placement when performing operations.  For example, the
   * MapReduce system tries to schedule tasks on the same machines
   * as the data-block the task processes.
   */
  @Deprecated
  public String[][] getHints(String src, long start, long length)
    throws IOException {
    BlockLocation[] blkLocations = getBlockLocations(src, start, length);
    if ((blkLocations == null) || (blkLocations.length == 0)) {
      return new String[0][];
    }
    int blkCount = blkLocations.length;
    String[][]hints = new String[blkCount][];
    for (int i=0; i < blkCount ; i++) {
      String[] hosts = blkLocations[i].getHosts();
      hints[i] = new String[hosts.length];
      hints[i] = hosts;
    }
    return hints;
  }

  public static boolean isMetaInfoSuppoted(ProtocolProxy<ClientProtocol> proxy)
  throws IOException {
    return proxy != null && proxy.isMethodSupported(
        "openAndFetchMetaInfo", String.class, long.class, long.class);
  }
  
  private static LocatedBlocks callGetBlockLocations(
      ClientProtocol namenode,
      String src, long start, long length, boolean supportMetaInfo) throws IOException {
    try {
      if (supportMetaInfo) {
        return namenode.openAndFetchMetaInfo(src, start, length);
      }
      return namenode.getBlockLocations(src, start, length);
    } catch(RemoteException re) {
      throw re.unwrapRemoteException(AccessControlException.class,
                                    FileNotFoundException.class);
    }
  }

  /**
   * Get block location info about file
   *
   * getBlockLocations() returns a list of hostnames that store
   * data for a specific file region.  It returns a set of hostnames
   * for every block within the indicated region.
   *
   * This function is very useful when writing code that considers
   * data-placement when performing operations.  For example, the
   * MapReduce system tries to schedule tasks on the same machines
   * as the data-block the task processes.
   */
  public BlockLocation[] getBlockLocations(String src, long start,
    long length) throws IOException {
    LocatedBlocks blocks = callGetBlockLocations(namenode, src, start, length,
        isMetaInfoSuppoted(namenodeProtocolProxy));
    return DFSUtil.locatedBlocks2Locations(blocks);
  }
  
  public LocatedBlocks getLocatedBlocks(String src, long start,
      long length) throws IOException {
    return callGetBlockLocations(namenode, src, start, length,
        isMetaInfoSuppoted(namenodeProtocolProxy));
  }

  public DFSInputStream open(String src) throws IOException {
    return open(src, conf.getInt("io.file.buffer.size", 4096), true, null,
        false, new ReadOptions());
  }

  /*
   * This method is only used by SnapshotClient
   */
  DFSInputStream open(LocatedBlocksWithMetaInfo blocks) throws IOException {
    checkOpen();
    incFileReadToStats();
    return new DFSInputStream(this, blocks,
    		conf.getInt("io.file.buffer.size", 4096), true);
  }


  /**
   * Create an input stream that obtains a nodelist from the
   * namenode, and then reads from all the right places.  Creates
   * inner subclass of InputStream that does the right out-of-band
   * work.
   */
  DFSInputStream open(String src, int buffersize, boolean verifyChecksum,
      FileSystem.Statistics stats, boolean clearOsBuffer,
      ReadOptions options
      ) throws IOException {
    checkOpen();

    incFileReadToStats();
    // Get block info from namenode
    DFSInputStream stream = new DFSInputStream(this, src, buffersize,
        verifyChecksum, clearOsBuffer, options);
    if (blockLocationRenewal != null) {
      blockLocationRenewal.add(stream);
    }
    return stream;
  }

  /**
   * Create a new dfs file and return an output stream for writing into it.
   *
   * @param src stream name
   * @param overwrite do not check for file existence if true
   * @return output stream
   * @throws IOException
   */
  public OutputStream create(String src,
                             boolean overwrite
                             ) throws IOException {
    return create(src, overwrite, defaultReplication, defaultBlockSize, null);
  }

  /**
   * Create a new dfs file and return an output stream for writing into it
   * with write-progress reporting.
   *
   * @param src stream name
   * @param overwrite do not check for file existence if true
   * @return output stream
   * @throws IOException
   */
  public OutputStream create(String src,
                             boolean overwrite,
                             Progressable progress
                             ) throws IOException {
    return create(src, overwrite, defaultReplication, defaultBlockSize, null);
  }

  /**
   * Create a new dfs file with the specified block replication
   * and return an output stream for writing into the file.
   *
   * @param src stream name
   * @param overwrite do not check for file existence if true
   * @param replication block replication
   * @return output stream
   * @throws IOException
   */
  public OutputStream create(String src,
                             boolean overwrite,
                             short replication,
                             long blockSize
                             ) throws IOException {
    return create(src, overwrite, replication, blockSize, null);
  }


  /**
   * Create a new dfs file with the specified block replication
   * with write-progress reporting and return an output stream for writing
   * into the file.
   *
   * @param src stream name
   * @param overwrite do not check for file existence if true
   * @param replication block replication
   * @return output stream
   * @throws IOException
   */
  public OutputStream create(String src,
                             boolean overwrite,
                             short replication,
                             long blockSize,
                             Progressable progress
                             ) throws IOException {
    return create(src, overwrite, replication, blockSize, progress,
        conf.getInt("io.file.buffer.size", 4096));
  }
  /**
   * Call
   * {@link #create(String,FsPermission,boolean,short,long,Progressable,int)}
   * with default permission.
   * @see FsPermission#getDefault()
   */
  public OutputStream create(String src,
      boolean overwrite,
      short replication,
      long blockSize,
      Progressable progress,
      int buffersize
      ) throws IOException {
    return create(src, FsPermission.getDefault(),
        overwrite, replication, blockSize, progress, buffersize);
  }

  /**
   * Call
   * {@link #create(String,FsPermission,boolean,boolean,short,long,Progressable,int)}
   * with createParent set to true.
   */
  public OutputStream create(String src,
      FsPermission permission,
      boolean overwrite,
      short replication,
      long blockSize,
      Progressable progress,
      int buffersize
      ) throws IOException {
    return create(src, permission, overwrite, true,
        replication, blockSize, progress, buffersize);
  }

  /**
   * Create a new dfs file with the specified block replication
   * with write-progress reporting and return an output stream for writing
   * into the file.
   *
   * @param src stream name
   * @param permission The permission of the directory being created.
   * If permission == null, use {@link FsPermission#getDefault()}.
   * @param overwrite do not check for file existence if true
   * @param createParent create missing parent directory if true
   * @param replication block replication
   * @return output stream
   * @throws IOException
   * @see ClientProtocol#create(String, FsPermission, String, boolean, short, long)
   */
  public OutputStream create(String src,
                             FsPermission permission,
                             boolean overwrite,
                             boolean createParent,
                             short replication,
                             long blockSize,
                             Progressable progress,
                             int buffersize
                             ) throws IOException {
    return create(src, permission, overwrite, createParent, replication, blockSize,
		progress, buffersize, conf.getInt("io.bytes.per.checksum", 512));
  }

  /**
   * Create a new dfs file with the specified block replication
   * with write-progress reporting and return an output stream for writing
   * into the file.
   *
   * @param src stream name
   * @param permission The permission of the directory being created.
   * If permission == null, use {@link FsPermission#getDefault()}.
   * @param overwrite do not check for file existence if true
   * @param replication block replication
   * @return output stream
   * @throws IOException
   * @see ClientProtocol#create(String, FsPermission, String, boolean, short, long)
   */
  public OutputStream create(String src,
                             FsPermission permission,
                             boolean overwrite,
                             boolean createParent,
                             short replication,
                             long blockSize,
                             Progressable progress,
                             int buffersize,
                             int bytesPerChecksum)  throws IOException {
  return create(src, permission, overwrite, createParent, replication, blockSize,
      progress, buffersize, bytesPerChecksum, false, false, null);
  }

  /**
   * Create a new dfs file with the specified block replication
   * with write-progress reporting and return an output stream for writing
   * into the file.
   *
   * @param src stream name
   * @param permission The permission of the directory being created.
   * If permission == null, use {@link FsPermission#getDefault()}.
   * @param overwrite do not check for file existence if true
   * @param createParent create missing parent directory if true
   * @param replication block replication
   * @param forceSync a hdfs sync() operation invokes local filesystem sync
   * 				on datanodes.
   * @param doParallelWrites write replicas in parallel
   * @return output stream
   * @throws IOException
   * @see ClientProtocol#create(String, FsPermission, String, boolean, short, long)
   */
  public OutputStream create(String src,
                             FsPermission permission,
                             boolean overwrite,
                             boolean createParent,
                             short replication,
                             long blockSize,
                             Progressable progress,
                             int buffersize,
                             boolean forceSync,
                             boolean doParallelWrites) throws IOException {
    return create(src, permission, overwrite, createParent, replication,
        blockSize,progress, buffersize,
        conf.getInt("io.bytes.per.checksum", 512),
        forceSync, doParallelWrites, null);
  }

  /**
   * Create a new dfs file with the specified block replication
   * with write-progress reporting and return an output stream for writing
   * into the file.
   *
   * @param src stream name
   * @param permission The permission of the directory being created.
   * If permission == null, use {@link FsPermission#getDefault()}.
   * @param overwrite do not check for file existence if true
   * @param replication block replication
   * @param forceSync a hdfs sync() operation invokes local filesystem sync
   * 				on datanodes.
   * @param doParallelWrites write replicas in parallel
   * @return output stream
   * @throws IOException
   * @see ClientProtocol#create(String, FsPermission, String, boolean, short, long)
   */
  public OutputStream create(String src,
                             FsPermission permission,
                             boolean overwrite,
                             boolean createParent,
                             short replication,
                             long blockSize,
                             Progressable progress,
                             int buffersize,
                             int bytesPerChecksum,
                             boolean forceSync,
                             boolean doParallelWrites) throws IOException {
    return create(src, permission, overwrite, createParent, replication,
        blockSize, progress, buffersize, bytesPerChecksum, forceSync,
        doParallelWrites, null);
  }

  public OutputStream create(String src, FsPermission permission,
      boolean overwrite, boolean createParent, short replication,
      long blockSize, Progressable progress, int buffersize,
      int bytesPerChecksum, boolean forceSync, boolean doParallelWrites,
      InetSocketAddress[] favoredNodes) throws IOException {
    return create(src, permission, overwrite, createParent, replication,
        blockSize, progress, buffersize, bytesPerChecksum, forceSync,
        doParallelWrites, favoredNodes, new WriteOptions());
  }

  /**
   * Create a new dfs file with the specified block replication
   * with write-progress reporting and return an output stream for writing
   * into the file.
   *
   * @param src stream name
   * @param permission The permission of the directory being created.
   * If permission == null, use {@link FsPermission#getDefault()}.
   * @param overwrite do not check for file existence if true
   * @param replication block replication
   * @param forceSync a hdfs sync() operation invokes local filesystem sync
   * 				on datanodes.
   * @param doParallelWrites write replicas in parallel
   * @param favoredNodes nodes on which to place replicas if possible
   * @return output stream
   * @throws IOException
   * @see ClientProtocol#create(String, FsPermission, String, boolean, short, long)
   */
  public OutputStream create(String src,
                             FsPermission permission,
                             boolean overwrite,
                             boolean createParent,
                             short replication,
                             long blockSize,
                             Progressable progress,
                             int buffersize,
                             int bytesPerChecksum,
                             boolean forceSync,
                             boolean doParallelWrites,
      InetSocketAddress[] favoredNodes, WriteOptions options)
  throws IOException {
    checkOpen();
    clearFileStatusCache();
    
    if (permission == null) {
      permission = FsPermission.getDefault();
    }
    boolean success = false;
    try {
      FsPermission masked = permission.applyUMask(FsPermission.getUMask(conf));
      LOG.debug(src + ": masked=" + masked);

      // For each of the favored nodes, mock up a DatanodeInfo with the IP
      // address and port of that node.
      DatanodeInfo[] favoredNodeInfos = null;
      if (favoredNodes != null) {
        favoredNodeInfos = new DatanodeInfo[favoredNodes.length];
        for (int i = 0; i < favoredNodes.length; i++) {
          favoredNodeInfos[i] = new DatanodeInfo(new DatanodeID(
              favoredNodes[i].getAddress().getHostAddress() + ":" +
              favoredNodes[i].getPort()));
        }
      }

      OutputStream result = new DFSOutputStream(this, src, masked,
          overwrite, createParent, replication, blockSize, progress, buffersize,
          bytesPerChecksum, forceSync, doParallelWrites, favoredNodeInfos,
          options);
      leasechecker.put(src, result);
      metrics.incNumCreateFileOps();
      if (stats != null) {
        stats.incrementFilesCreated();
      }
      success = true;
      return result;
    } finally {
      if (!success  && namenodeProtocolProxy.isMethodSupported(
          "abandonFile", String.class, String.class)) {
        try {
          namenode.abandonFile(src, clientName);
        } catch (RemoteException e) {
          if (e.unwrapRemoteException() instanceof LeaseExpiredException) {
            LOG.debug(String.format(
              "client %s attempting to abandon file %s which it does not own",
              clientName, src),
              e
            );
          } else {
            throw e;
          }
        }
      }
    }
  }
  
  /**
   * Raid a file with given codec 
   * No guarantee file will be raided when the call returns.
   * Namenode will schedule raiding asynchronously. 
   * If raiding is done when raidFile is called again, namenode will set 
   * replication of source blocks to expectedSourceRepl 
   * 
   * @param source 
   * @param codecId
   * @param expectedSourceRepl 
   * @throws IOException
   */
  public boolean raidFile(String source,
                       String codecId,
                       short expectedSourceRepl) throws IOException {
    checkOpen();
    try {
      return namenode.raidFile(source, codecId, expectedSourceRepl);
    } catch(RemoteException re) {
      throw re.unwrapRemoteException(AccessControlException.class,
                                     NSQuotaExceededException.class,
                                     DSQuotaExceededException.class);
    }
  }

  /**
   * Recover a file's lease
   *
   * @param src a file's path
   * @return if lease recovery completes
   * @throws IOException
   */
  boolean recoverLease(String src, boolean discardLastBlock) throws IOException {
    checkOpen();
    // We remove the file from local lease checker. Usually it is already been
    // removed by the client but we want to be extra safe.
    leasechecker.remove(src);

    if (this.namenodeProtocolProxy == null) {
      return versionBasedRecoverLease(src);
    }
    return methodBasedRecoverLease(src, discardLastBlock);
  }

  /** recover lease based on version */
  private boolean versionBasedRecoverLease(String src) throws IOException {

    if (namenodeVersion < ClientProtocol.RECOVER_LEASE_VERSION) {
      OutputStream out;
      try {
        out = append(src, conf.getInt("io.file.buffer.size", 4096), null);
      } catch (RemoteException re) {
        IOException e = re.unwrapRemoteException(AlreadyBeingCreatedException.class);
        if (e instanceof AlreadyBeingCreatedException) {
          return false;
        }
        throw re;
      }
      out.close();
      return true;
    } else if (namenodeVersion < ClientProtocol.CLOSE_RECOVER_LEASE_VERSION){
      try {
        namenode.recoverLease(src, clientName);
      } catch (RemoteException re) {
        throw re.unwrapRemoteException(FileNotFoundException.class,
                                       AccessControlException.class);
      }
      return !namenode.getBlockLocations(src, 0, Long.MAX_VALUE).isUnderConstruction();
    } else {
      try {
        return namenode.closeRecoverLease(src, clientName, false);
      } catch (RemoteException re) {
        throw re.unwrapRemoteException(FileNotFoundException.class,
                                       AccessControlException.class);
      }
    }
  }

  /** recover lease based on method name */
  private boolean methodBasedRecoverLease(String src, boolean discardLastBlock)
    throws IOException {
    // check if closeRecoverLease(discardLastBlock) is supported
    if (namenodeProtocolProxy.isMethodSupported(
        "closeRecoverLease", String.class, String.class, boolean.class)) {
      try {
        return namenode.closeRecoverLease(src, clientName, discardLastBlock);
      } catch (RemoteException re) {
        throw re.unwrapRemoteException(FileNotFoundException.class,
                                       AccessControlException.class);
      }
    }
    // check if closeRecoverLease is supported
    else if (namenodeProtocolProxy.isMethodSupported(
        "closeRecoverLease", String.class, String.class)) {
      try {
        return namenode.closeRecoverLease(src, clientName);
      } catch (RemoteException re) {
        throw re.unwrapRemoteException(FileNotFoundException.class,
                                       AccessControlException.class);
      }
    }
    // check if recoverLease is supported
    if (namenodeProtocolProxy.isMethodSupported(
        "recoverLease", String.class, String.class)) {
      try {
        namenode.recoverLease(src, clientName);
      } catch (RemoteException re) {
        throw re.unwrapRemoteException(FileNotFoundException.class,
                                       AccessControlException.class);
      }
      return !namenode.getBlockLocations(src, 0, Long.MAX_VALUE).isUnderConstruction();
    }
    // now use append
    OutputStream out;
    try {
      out = append(src, conf.getInt("io.file.buffer.size", 4096), null);
    } catch (RemoteException re) {
      IOException e = re.unwrapRemoteException(AlreadyBeingCreatedException.class);
      if (e instanceof AlreadyBeingCreatedException) {
        return false;
      }
      throw re;
    }
    out.close();
    return true;
  }

  private boolean closeFileOnNameNode(String src, long fileLen,
      Block lastBlockId) throws IOException {
    boolean fileComplete;
    if (namenodeProtocolProxy != null
        && namenodeProtocolProxy.isMethodSupported("complete", String.class,
            String.class, long.class, Block.class)) {
      fileComplete = namenode.complete(src, clientName, fileLen, lastBlockId);
    } else if (namenodeProtocolProxy != null
        && namenodeProtocolProxy.isMethodSupported("complete", String.class,
            String.class, long.class)) {
      fileComplete = namenode.complete(src, clientName, fileLen);
    } else {
      fileComplete = namenode.complete(src, clientName);
    }
    return fileComplete;
  }

  public void closeFile(String src, long fileLen, Block lastBlockId) throws IOException {
    long localstart = System.currentTimeMillis();
    boolean fileComplete = false;
    boolean retried = false;
    IOException lastException = null;
    // These are the close file semantics for retry that we need :
    //
    // 1) If we have exhausted the close file time out but have tried only once, retry one more time.
    //
    // 2) If we have exhausted the close file time otherwise, just abort.
    while (!fileComplete) {
      try {
        fileComplete = closeFileOnNameNode(src, fileLen, lastBlockId);
      } catch (RemoteException re) {
        // If the Namenode throws an exception, we need to rethrow the
        // exception.
        throw re;
      } catch (IOException e) {
        // Record exception so that we re-throw when we fail.
        if (closeFileTimeout <= 0) {
          // If the closeFileTimeout is not positive, we should throw the
          // exception since otherwise we would retry indefinitely.
          throw e;
        }
        lastException = e;
        LOG.warn("Exception closing file on namenode", e);
      }

      boolean timedOut = (closeFileTimeout > 0 &&
          localstart + closeFileTimeout < System.currentTimeMillis());
      // Verify the close file timeout has not elapsed.
      if (!fileComplete) {
        if (!clientRunning || (timedOut && retried)) {
          if (lastException != null) {
            throw lastException;
          }
          String msg = "Unable to close file because dfsclient " +
            " was unable to contact the HDFS servers." +
            " clientRunning " + clientRunning +
            " closeFileTimeout " + closeFileTimeout;
          LOG.info(msg);
          throw new IOException(msg);
        }
        try {
          retried = true;
          Thread.sleep(400);
          if (System.currentTimeMillis() - localstart > 5000) {
            LOG.info("Could not complete file " + src + " retrying...");
          }
        } catch (InterruptedException ie) {
        }
      }
    }
  }

  /**
   * Append to an existing HDFS file.
   *
   * @param src file name
   * @param buffersize buffer size
   * @param progress for reporting write-progress
   * @return an output stream for writing into the file
   * @throws IOException
   * @see ClientProtocol#append(String, String)
   */
  OutputStream append(String src, int buffersize, Progressable progress
      ) throws IOException {
    checkOpen();
    clearFileStatusCache();

    FileStatus stat = null;
    LocatedBlock lastBlock = null;
    boolean success = false;
    
    try {
      stat = getFileInfo(src);
      if (namenodeProtocolProxy != null
            && namenodeProtocolProxy.isMethodSupported(
                "appendAndFetchOldGS", String.class, String.class)) {
        LocatedBlockWithOldGS loc = namenode.appendAndFetchOldGS(src, 
            clientName);
        lastBlock = loc;
        if (loc != null) {
          updateNamespaceIdIfNeeded(loc.getNamespaceID());
          updateDataTransferProtocolVersionIfNeeded(loc.getDataProtocolVersion());
          getNewNameNodeIfNeeded(loc.getMethodFingerPrint());
        }
      } else if (namenodeProtocolProxy != null 
               && dataTransferVersion >= DataTransferProtocol.APPEND_BLOCK_VERSION) {
        // fail the request if the data transfer version support the new append 
        // protocol, but the namenode method is not supported.
        // This should not happen unless there is a bug.
        throw new IOException ("DataTransferVersion " + dataTransferVersion +
              "requires the method appendAndFetchOldGS is supported in Namenode");
      } else if (namenodeProtocolProxy != null
          && namenodeProtocolProxy.isMethodSupported(
              "appendAndFetchMetaInfo", String.class, String.class)) {
        LocatedBlockWithMetaInfo loc = namenode.appendAndFetchMetaInfo(src,
            clientName);
        lastBlock = loc;
        if (loc != null) {
          updateNamespaceIdIfNeeded(loc.getNamespaceID());
          updateDataTransferProtocolVersionIfNeeded(loc.getDataProtocolVersion());
          getNewNameNodeIfNeeded(loc.getMethodFingerPrint());
        }
      } else {
        lastBlock = namenode.append(src, clientName);
      }

      OutputStream result = new DFSOutputStream(this, src, buffersize, progress,
          lastBlock, stat, conf.getInt("io.bytes.per.checksum", 512), namespaceId);
      leasechecker.put(src, result);
      success = true;

      return result;
    } catch(RemoteException re) {
      throw re.unwrapRemoteException(FileNotFoundException.class,
                                     AccessControlException.class,
                                     NSQuotaExceededException.class,
                                     DSQuotaExceededException.class);
    } finally {
      if (!success) {
        try {
          namenode.abandonFile(src, clientName);
        } catch (RemoteException e) {
          if (e.unwrapRemoteException() instanceof LeaseExpiredException) {
            LOG.debug(String.format(
              "client %s attempting to abandon file %s which it does not own",
              clientName, src), 
              e
            );
          } else {
            throw e;
          }
        }
      }
    }
  }

  /**
   * Set replication for an existing file.
   *
   * @see ClientProtocol#setReplication(String, short)
   * @param replication
   * @throws IOException
   * @return true is successful or false if file does not exist
   */
  public boolean setReplication(String src,
                                short replication
                                ) throws IOException {
    try {
      return namenode.setReplication(src, replication);
    } catch(RemoteException re) {
      throw re.unwrapRemoteException(AccessControlException.class,
                                     NSQuotaExceededException.class,
                                     DSQuotaExceededException.class);
    }
  }

  /**
   * Move blocks from src to trg and delete src
   * See {@link ClientProtocol#concat(String, String [])}.
   */
  public void concat(String trg, String[] srcs, boolean restricted)
      throws IOException {
    checkOpen();
    clearFileStatusCache();

    try {
      if (namenodeProtocolProxy != null
          && namenodeProtocolProxy.isMethodSupported("concat", String.class,
              String[].class, boolean.class)) {
        namenode.concat(trg, srcs, restricted);
      } else if (!restricted){
        throw new UnsupportedOperationException(
            "Namenode does not support variable length blocks");
      } else {
        namenode.concat(trg, srcs);
      }
    } catch(RemoteException re) {
      throw re.unwrapRemoteException(AccessControlException.class,
                                     NSQuotaExceededException.class,
                                     DSQuotaExceededException.class);
    }
  }
  
  /**
   * Merge parity file and source file together into a raided file
   */
  public void merge(String parity, String source, String codecId, 
      int[] checksums) throws IOException {
    checkOpen();
    try {
      namenode.merge(parity, source, codecId, checksums);
    } catch (RemoteException re) {
      throw re.unwrapRemoteException(AccessControlException.class,
                                     NSQuotaExceededException.class,
                                     DSQuotaExceededException.class);
    }
  }
  
  /** 
   * See {@link ClientProtocol#hardLink(String, String)}. 
   */ 
  public boolean hardLink(String src, String dst) throws IOException {  
    checkOpen();  
    try { 
      return namenode.hardLink(src, dst); 
    } catch(RemoteException re) { 
      throw re.unwrapRemoteException(AccessControlException.class,  
                                     NSQuotaExceededException.class,  
                                     DSQuotaExceededException.class); 
    } 
  }
  
  /**
   * See {@link ClientProtocol#getHardLinkedFiles(String)}.
   */
  public String[] getHardLinkedFiles(String src) throws IOException {
    checkOpen();
    try {
      return namenode.getHardLinkedFiles(src);
    } catch (RemoteException re) {
      throw re.unwrapRemoteException(AccessControlException.class,
          NSQuotaExceededException.class, DSQuotaExceededException.class);
    }
  }

  /**
   * Rename file or directory.
   * See {@link ClientProtocol#rename(String, String)}.
   */
  public boolean rename(String src, String dst) throws IOException {
    checkOpen();
    clearFileStatusCache();
    
    try {
      return namenode.rename(src, dst);
    } catch(RemoteException re) {
      throw re.unwrapRemoteException(AccessControlException.class,
                                     NSQuotaExceededException.class,
                                     DSQuotaExceededException.class);
    }
  }

  /**
   * Delete file or directory.
   * See {@link ClientProtocol#delete(String)}.
   */
  @Deprecated
  public boolean delete(String src) throws IOException {
    checkOpen();
    clearFileStatusCache();
     
    return namenode.delete(src, true);
  }

  /**
   * delete file or directory.
   * delete contents of the directory if non empty and recursive
   * set to true
   */
  public boolean delete(String src, boolean recursive) throws IOException {
    checkOpen();
    clearFileStatusCache();
    
    try {
      return namenode.delete(src, recursive);
    } catch(RemoteException re) {
      throw re.unwrapRemoteException(AccessControlException.class);
    }
  }

  /** Implemented using getFileInfo(src)
   */
  public boolean exists(String src) throws IOException {
    checkOpen();
    return getFileInfo(src) != null;
  }

  /** @deprecated Use getFileStatus() instead */
  @Deprecated
  public boolean isDirectory(String src) throws IOException {
    FileStatus fs = getFileInfo(src);
    if (fs != null)
      return fs.isDir();
    else
      throw new FileNotFoundException("File does not exist: " + src);
  }

  /**
   * Get a listing of the indicated directory
   */
  public FileStatus[] listPaths(String src) throws IOException {
    checkOpen();
    metrics.incLsCalls();
    try {
      if (namenodeProtocolProxy == null) {
        return versionBasedListPath(src);
      }
      return methodBasedListPath(src);
    } catch(RemoteException re) {
      throw re.unwrapRemoteException(AccessControlException.class);
    }
  }

  private FileStatus[] versionBasedListPath(String src) throws IOException {
    if (namenodeVersion >= ClientProtocol.ITERATIVE_LISTING_VERSION) {
      return iterativeListing(src);
    } else if (namenodeVersion >= ClientProtocol.OPTIMIZE_FILE_STATUS_VERSION) {
      HdfsFileStatus[] hdfsStats = namenode.getHdfsListing(src);
      if (hdfsStats == null) {
        return null;
      }
      FileStatus[] stats = new FileStatus[hdfsStats.length];
      for (int i=0; i<stats.length; i++) {
        stats[i] = HdfsFileStatus.toFileStatus(hdfsStats[i], src);
      }
      return stats;
    } else {
      return namenode.getListing(src);
    }
  }

  private FileStatus[] methodBasedListPath(String src) throws IOException {
    if (namenodeProtocolProxy.isMethodSupported(
        "getPartialListing", String.class, byte[].class)) {
      return iterativeListing(src);
    } else if (namenodeProtocolProxy.isMethodSupported(
        "getHdfsListing", String.class)) {
      HdfsFileStatus[] hdfsStats = namenode.getHdfsListing(src);
      if (hdfsStats == null) {
        return null;
      }
      FileStatus[] stats = new FileStatus[hdfsStats.length];
      for (int i=0; i<stats.length; i++) {
        stats[i] = HdfsFileStatus.toFileStatus(hdfsStats[i], src);
      }
      return stats;
    } else {
      return namenode.getListing(src);
    }
  }
  
  public boolean isConcatAvailable() throws IOException {
    if(namenodeProtocolProxy == null) {
      if(namenodeVersion >= ClientProtocol.CONCAT_VERSION)
        return true;
    }
    else {
      return namenodeProtocolProxy.isMethodSupported(
          "concat", String.class, String[].class);
    }
      return false;
  }

  /**
   * Get a partial listing of the indicated directory
   *
   * Recommend to use HdfsFileStatus.EMPTY_NAME as startAfter
   * if the application wants to fetch a listing starting from
   * the first entry in the directory
   *
   * @see ClientProtocol#getLocatedPartialListing(String, byte[])
   */
  public RemoteIterator<LocatedBlockFileStatus> listPathWithBlockLocation(
      final String src) throws IOException {
    checkOpen();
    try {
      return new RemoteIterator<LocatedBlockFileStatus>() {
        private LocatedDirectoryListing thisListing;
        private int i;

        { // initializer
          // fetch the first batch of entries in the directory

          thisListing = namenode.getLocatedPartialListing(
              src, HdfsFileStatus.EMPTY_NAME);
          if (thisListing == null) { // the directory does not exist
            throw new FileNotFoundException("File " + src + " does not exist.");
          }
        }

        @Override
        public boolean hasNext() throws IOException {
          if (i>=thisListing.getPartialListing().length
              && thisListing.hasMore()) {
            // current listing is exhausted & fetch a new listing
            thisListing = namenode.getLocatedPartialListing(
                src, thisListing.getLastName());
            if (thisListing == null) {
              throw new FileNotFoundException("File " + src + " does not exist.");
            }
            i = 0;
          }
          return i < thisListing.getPartialListing().length;
        }

        @Override
        public LocatedBlockFileStatus next() throws IOException {
          if (!hasNext()) {
            throw new java.util.NoSuchElementException("No more entry in " + src);
          }
          return HdfsFileStatus.toLocatedBlockFileStatus(thisListing.getPartialListing()[i],
              thisListing.getBlockLocations()[i++], src);
        }
      };
    } catch(RemoteException re) {
      throw re.unwrapRemoteException(AccessControlException.class);
    }
  }

  /**
   * Get a partial listing of the indicated directory
   *
   * Recommend to use HdfsFileStatus.EMPTY_NAME as startAfter
   * if the application wants to fetch a listing starting from
   * the first entry in the directory
   *
   * @see ClientProtocol#getLocatedPartialListing(String, byte[])
   */
  public RemoteIterator<LocatedFileStatus> listPathWithLocation(
      final String src) throws IOException {
    checkOpen();
    try {
      if (namenodeProtocolProxy == null) {
        return versionBasedListPathWithLocation(src);
      }
      return methodBasedListPathWithLocation(src);
    } catch(RemoteException re) {
      throw re.unwrapRemoteException(AccessControlException.class);
    }
  }

  /** List a directory with location based on version */
  private RemoteIterator<LocatedFileStatus> versionBasedListPathWithLocation(
      final String src) throws IOException {
    if (namenodeVersion >= ClientProtocol.BULK_BLOCK_LOCATIONS_VERSION) {
      return iteratorListing(src);
    } else {
      return arrayListing(src);
    }
  }

  /** List a directory with location based on method */
  private RemoteIterator<LocatedFileStatus> methodBasedListPathWithLocation(
      final String src) throws IOException {
    if (namenodeProtocolProxy.isMethodSupported(
        "getLocatedPartialListing", String.class, byte[].class)) {
      return iteratorListing(src);
    } else {
      return arrayListing(src);
    }
  }

  /** create the iterator from an array of file status */
  private RemoteIterator<LocatedFileStatus> arrayListing(final String src)
  throws IOException {
    return new RemoteIterator<LocatedFileStatus>() {
      private FileStatus[] stats;
      private int i = 0;

      { //initializer
        stats = listPaths(src);
        if (stats == null) {
          throw new FileNotFoundException("File " + src + " does not exist.");
        }
      }

      @Override
      public boolean hasNext() throws IOException {
        return i<stats.length;
      }

      @Override
      public LocatedFileStatus next() throws IOException {
        if (!hasNext()) {
          throw new NoSuchElementException("No more entry in " + src);
        }
        FileStatus result = stats[i++];
        BlockLocation[] locs = result.isDir() ? null :
            getBlockLocations(
                result.getPath().toUri().getPath(), 0, result.getLen());
        return new LocatedFileStatus(result, locs);
      }
    };
  }

  /** create the iterator from the iterative listing with block locations */
  private RemoteIterator<LocatedFileStatus> iteratorListing(final String src)
  throws IOException {
    return new RemoteIterator<LocatedFileStatus>() {
      private LocatedDirectoryListing thisListing;
      private int i;

      { // initializer
        // fetch the first batch of entries in the directory

        thisListing = namenode.getLocatedPartialListing(
            src, HdfsFileStatus.EMPTY_NAME);
        if (thisListing == null) { // the directory does not exist
          throw new FileNotFoundException("File " + src + " does not exist.");
        }
      }

      @Override
      public boolean hasNext() throws IOException {
        if (i>=thisListing.getPartialListing().length
            && thisListing.hasMore()) {
          // current listing is exhausted & fetch a new listing
          thisListing = namenode.getLocatedPartialListing(
              src, thisListing.getLastName());
          if (thisListing == null) {
            throw new FileNotFoundException("File " + src + " does not exist.");
          }
          i = 0;
        }
        return i < thisListing.getPartialListing().length;
      }

      @Override
      public LocatedFileStatus next() throws IOException {
        if (!hasNext()) {
          throw new java.util.NoSuchElementException("No more entry in " + src);
        }
        return HdfsFileStatus.toLocatedFileStatus(thisListing.getPartialListing()[i],
            thisListing.getBlockLocations()[i++], src);
      }
    };

  }
  /**
   * List the given path iteratively if the directory is large
   *
   * @param src a path
   * @return a listing of the path
   * @throws IOException if any IO error is occurred
   */
  private FileStatus[] iterativeListing(String src) throws IOException {
    // fetch the first batch of entries in the directory
    DirectoryListing thisListing = namenode.getPartialListing(
        src, HdfsFileStatus.EMPTY_NAME);

    if (thisListing == null) { // the directory does not exist
      return null;
     }
    HdfsFileStatus[] partialListing = thisListing.getPartialListing();
    if (!thisListing.hasMore()) { // got all entries of the directory
      FileStatus[] stats = new FileStatus[partialListing.length];
      for (int i = 0; i < partialListing.length; i++) {
        stats[i] = HdfsFileStatus.toFileStatus(partialListing[i], src);
      }
      return stats;
    }

    // The directory size is too big that it needs to fetch more
    // estimate the total number of entries in the directory
    int totalNumEntries =
      partialListing.length + thisListing.getRemainingEntries();
    ArrayList<FileStatus> listing =
      new ArrayList<FileStatus>(totalNumEntries);
    // add the first batch of entries to the array list
    for (HdfsFileStatus fileStatus : partialListing) {
      listing.add(HdfsFileStatus.toFileStatus(fileStatus, src));
    }

    // now fetch more entries
    do {
      thisListing = namenode.getPartialListing(src, thisListing.getLastName());

      if (thisListing == null) {
        return null; // the directory is deleted
      }

      partialListing = thisListing.getPartialListing();
      for (HdfsFileStatus fileStatus : partialListing) {
        listing.add(HdfsFileStatus.toFileStatus(fileStatus, src));
      }
    } while (thisListing.hasMore());

    return listing.toArray(new FileStatus[listing.size()]);
  }

  public FileStatus getFileInfo(String src) throws IOException {
    FileStatus fileStatus;

    checkOpen();
    try {
      if (fileStatusCache != null) {
        fileStatus = fileStatusCache.get(src);
        if (fileStatus != FileStatusCache.nullFileStatus) {
          return fileStatus;
        }    
      }
      fileStatus = namenodeProtocolProxy == null ? versionBasedGetFileInfo(src)
          : methodBasedGetFileInfo(src);
  	  if (fileStatusCache != null) {
  	    fileStatusCache.set(src, fileStatus);
  	  }
  	  
  	  return fileStatus;
    } catch(RemoteException re) {
      throw re.unwrapRemoteException(AccessControlException.class);
    }
  }

  /** Get file info: decide which rpc to call based on protocol version */
  private FileStatus versionBasedGetFileInfo(String src) throws IOException {
    if (namenodeVersion >= ClientProtocol.OPTIMIZE_FILE_STATUS_VERSION) {
      return HdfsFileStatus.toFileStatus(namenode.getHdfsFileInfo(src), src);
    } else {
      return namenode.getFileInfo(src);
    }
  }

  /** Get file info: decide which rpc to call based on server methods*/
  private FileStatus methodBasedGetFileInfo(String src) throws IOException {
    if (namenodeProtocolProxy.isMethodSupported(
        "getHdfsFileInfo", String.class)) {
      return HdfsFileStatus.toFileStatus(namenode.getHdfsFileInfo(src), src);
    } else {
      return namenode.getFileInfo(src);
    }
  }
  
  /**
   * Get the CRC32 Checksum of a file.
   * @param src The file path
   * @return The checksum
   * @see DistributedFileSystem#getFileCrc(Path)
   */
  int getFileCrc(String src) throws IOException {
    checkOpen();
    return getFileCrc(dataTransferVersion,
        src, namenode, namenodeProtocolProxy, socketFactory, socketTimeout);
  }

  /**
   * Get the CRC Checksum of a file.
   * @param src The file path
   * @return The checksum
   */
  public static int getFileCrc(
      int dataTransferVersion, String src,
      ClientProtocol namenode, ProtocolProxy<ClientProtocol> namenodeProxy,
      SocketFactory socketFactory, int socketTimeout
      ) throws IOException {
    //get all block locations
    final LocatedBlocks locatedBlocks = callGetBlockLocations(
        namenode, src, 0, Long.MAX_VALUE, isMetaInfoSuppoted(namenodeProxy));
    if (locatedBlocks == null) {
      throw new IOException("Null block locations, mostly because non-existent file " + src);
    }
    int namespaceId = 0;
    if (locatedBlocks instanceof LocatedBlocksWithMetaInfo) {
      LocatedBlocksWithMetaInfo lBlocks = (LocatedBlocksWithMetaInfo)locatedBlocks;
      dataTransferVersion = lBlocks.getDataProtocolVersion();
      namespaceId = lBlocks.getNamespaceID();
    } else if (dataTransferVersion == -1) {
      dataTransferVersion = namenode.getDataTransferProtocolVersion();
    }
    final List<LocatedBlock> locatedblocks  = locatedBlocks.getLocatedBlocks();
    int fileCrc = 0;

    // get block CRC for each block
    for(int i = 0; i < locatedblocks.size(); i++) {
      LocatedBlock lb = locatedblocks.get(i);
      final Block block = lb.getBlock();
      final DatanodeInfo[] datanodes = lb.getLocations();

      // try each datanode location of the block
      final int timeout = (socketTimeout > 0) ? (socketTimeout +
        HdfsConstants.READ_TIMEOUT_EXTENSION * datanodes.length) : 0;

      boolean done = false;
      for(int j = 0; !done && j < datanodes.length; j++) {
        final Socket sock = socketFactory.createSocket();
        DataOutputStream out = null;
        DataInputStream in = null;

        try {
          // connect to a datanode
          NetUtils.connect(sock,
              NetUtils.createSocketAddr(datanodes[j].getName()), timeout);
          sock.setSoTimeout(timeout);

          out = new DataOutputStream(new BufferedOutputStream(
              NetUtils.getOutputStream(sock), DataNode.SMALL_BUFFER_SIZE));
          in = new DataInputStream(NetUtils.getInputStream(sock));

          if (LOG.isDebugEnabled()) {
            LOG.debug("read block CRC from" + datanodes[j].getName() + ": "
                + DataTransferProtocol.OP_BLOCK_CRC + ", block=" + block);
          }
          
          /* Write the header */
          BlockCrcHeader blockCrcHeader = new BlockCrcHeader(
              dataTransferVersion, namespaceId, block.getBlockId(),
              block.getGenerationStamp());
          blockCrcHeader.writeVersionAndOpCode(out);
          blockCrcHeader.write(out);
          out.flush();

          final short reply = in.readShort();
          if (reply != DataTransferProtocol.OP_STATUS_SUCCESS) {
            throw new IOException("Bad response " + reply + " for block "
                + block + " from datanode " + datanodes[j].getName());
          }

          //read block CRC
          final int blockCrc = (int) in.readLong();
          if (i == 0) {
            fileCrc = blockCrc;
          } else {
            // Concatenate to CRC of previous bytes.
            fileCrc = CrcConcat.concatCrc(fileCrc, blockCrc, (int) lb.getBlockSize());
          }

          done = true;

          if (LOG.isDebugEnabled()) {
            LOG.debug("got reply from " + datanodes[j].getName()
                + ": crc=" + blockCrcHeader);
          }
        } catch (IOException ie) {
          LOG.warn("src=" + src + ", datanodes[" + j + "].getName()="
              + datanodes[j].getName(), ie);
        } finally {
          IOUtils.closeStream(in);
          IOUtils.closeStream(out);
          IOUtils.closeSocket(sock);
        }
      }

      if (!done) {
        throw new IOException("Fail to get block CRC for " + block);
      }
    }

    return fileCrc;
  }

  /**
   * Get the checksum of a file.
   * @param src The file path
   * @return The checksum
   * @see DistributedFileSystem#getFileChecksum(Path)
   */
  MD5MD5CRC32FileChecksum getFileChecksum(String src) throws IOException {
    checkOpen();
    return getFileChecksum(dataTransferVersion,
        src, namenode, namenodeProtocolProxy, socketFactory, socketTimeout);
  }

  /**
   * Get the checksum of a file.
   * @param src The file path
   * @return The checksum
   */
  public static MD5MD5CRC32FileChecksum getFileChecksum(
      int dataTransferVersion, String src,
      ClientProtocol namenode, ProtocolProxy<ClientProtocol> namenodeProxy,
      SocketFactory socketFactory, int socketTimeout
      ) throws IOException {
    //get all block locations
    final LocatedBlocks locatedBlocks = callGetBlockLocations(
        namenode, src, 0, Long.MAX_VALUE, isMetaInfoSuppoted(namenodeProxy));
    if (locatedBlocks == null) {
    	throw new IOException("Null block locations, mostly because non-existent file " + src);
    }
    int namespaceId = 0;
    if (locatedBlocks instanceof LocatedBlocksWithMetaInfo) {
      LocatedBlocksWithMetaInfo lBlocks = (LocatedBlocksWithMetaInfo)locatedBlocks;
      dataTransferVersion = lBlocks.getDataProtocolVersion();
      namespaceId = lBlocks.getNamespaceID();
    } else if (dataTransferVersion == -1) {
      dataTransferVersion = namenode.getDataTransferProtocolVersion();
    }
    final List<LocatedBlock> locatedblocks  = locatedBlocks.getLocatedBlocks();
    final DataOutputBuffer md5out = new DataOutputBuffer();
    int bytesPerCRC = 0;
    long crcPerBlock = 0;

    //get block checksum for each block
    for(int i = 0; i < locatedblocks.size(); i++) {
      LocatedBlock lb = locatedblocks.get(i);
      final Block block = lb.getBlock();
      final DatanodeInfo[] datanodes = lb.getLocations();

      //try each datanode location of the block
      final int timeout = (socketTimeout > 0) ? (socketTimeout +
        HdfsConstants.READ_TIMEOUT_EXTENSION * datanodes.length) : 0;

      boolean done = false;
      for(int j = 0; !done && j < datanodes.length; j++) {
        final Socket sock = socketFactory.createSocket();
        DataOutputStream out = null;
        DataInputStream in = null;

        try {
          // connect to a datanode
          NetUtils.connect(sock,
              NetUtils.createSocketAddr(datanodes[j].getName()), timeout);
          sock.setSoTimeout(timeout);

          out = new DataOutputStream(new BufferedOutputStream(
              NetUtils.getOutputStream(sock), DataNode.SMALL_BUFFER_SIZE));
          in = new DataInputStream(NetUtils.getInputStream(sock));

          // get block MD5
          if (LOG.isDebugEnabled()) {
            LOG.debug("write to " + datanodes[j].getName() + ": "
                + DataTransferProtocol.OP_BLOCK_CHECKSUM +
                ", block=" + block);
          }
          
          /* Write the header */
          BlockChecksumHeader blockChecksumHeader = new BlockChecksumHeader(
              dataTransferVersion, namespaceId, block.getBlockId(),
              block.getGenerationStamp());
          blockChecksumHeader.writeVersionAndOpCode(out);
          blockChecksumHeader.write(out);
          out.flush();

          final short reply = in.readShort();
          if (reply != DataTransferProtocol.OP_STATUS_SUCCESS) {
            throw new IOException("Bad response " + reply + " for block "
                + block + " from datanode " + datanodes[j].getName());
          }

          //read byte-per-checksum
          final int bpc = in.readInt();
          if (i == 0) { //first block
            bytesPerCRC = bpc;
          }
          else if (bpc != bytesPerCRC) {
            throw new IOException("Byte-per-checksum not matched: bpc=" + bpc
                + " but bytesPerCRC=" + bytesPerCRC);
          }

          //read crc-per-block
          final long cpb = in.readLong();
          if (locatedblocks.size() > 1 && i == 0) {
            crcPerBlock = cpb;
          }

          //read md5
          final MD5Hash md5 = MD5Hash.read(in);
          md5.write(md5out);

          done = true;

          if (LOG.isDebugEnabled()) {
            if (i == 0) {
              LOG.debug("set bytesPerCRC=" + bytesPerCRC
                  + ", crcPerBlock=" + crcPerBlock);
            }
            LOG.debug("got reply from " + datanodes[j].getName()
                + ": md5=" + md5);
          }
        } catch (IOException ie) {
          LOG.warn("src=" + src + ", datanodes[" + j + "].getName()="
              + datanodes[j].getName(), ie);
        } finally {
          IOUtils.closeStream(in);
          IOUtils.closeStream(out);
          IOUtils.closeSocket(sock);
        }
      }

      if (!done) {
        throw new IOException("Fail to get block MD5 for " + block);
      }
    }

    //compute file MD5
    final MD5Hash fileMD5 = MD5Hash.digest(md5out.getData());
    return new MD5MD5CRC32FileChecksum(bytesPerCRC, crcPerBlock, fileMD5);
  }

  /**
   * Set permissions to a file or directory.
   * @param src path name.
   * @param permission
   * @throws <code>FileNotFoundException</code> is file does not exist.
   */
  public void setPermission(String src, FsPermission permission
                            ) throws IOException {
    checkOpen();
    clearFileStatusCache();
    
    try {
      namenode.setPermission(src, permission);
    } catch(RemoteException re) {
      throw re.unwrapRemoteException(AccessControlException.class,
                                     FileNotFoundException.class);
    }
  }

  /**
   * Set file or directory owner.
   * @param src path name.
   * @param username user id.
   * @param groupname user group.
   * @throws <code>FileNotFoundException</code> is file does not exist.
   */
  public void setOwner(String src, String username, String groupname
                      ) throws IOException {
    checkOpen();
    clearFileStatusCache();
    
    try {
      namenode.setOwner(src, username, groupname);
    } catch(RemoteException re) {
      throw re.unwrapRemoteException(AccessControlException.class,
                                     FileNotFoundException.class);
    }
  }

  public DiskStatus getDiskStatus() throws IOException {
    long rawNums[] = namenode.getStats();
    return new DiskStatus(rawNums[0], rawNums[1], rawNums[2]);
  }
  
  /*
   * Return the Disk status for current namespace
   */
  public DiskStatus getNSDiskStatus() throws IOException {
    long rawNums[] = namenode.getStats();
    // rawNums[6] should be capacityNamespaceUsed
    long dfsUsed = (rawNums.length > 6)? rawNums[6]: rawNums[1];
    return new DiskStatus(rawNums[0], dfsUsed, rawNums[2]);
  }
  
  /**
   */
  public long totalRawCapacity() throws IOException {
    long rawNums[] = namenode.getStats();
    return rawNums[0];
  }

  /**
   */
  public long totalRawUsed() throws IOException {
    long rawNums[] = namenode.getStats();
    return rawNums[1];
  }

  /**
   * Returns count of blocks with no good replicas left. Normally should be
   * zero.
   * @throws IOException
   */
  public long getMissingBlocksCount() throws IOException {
    return namenode.getStats()[ClientProtocol.GET_STATS_MISSING_BLOCKS_IDX];
  }

  /**
   * Returns count of blocks with one of more replica missing.
   * @throws IOException
   */
  public long getUnderReplicatedBlocksCount() throws IOException {
    return namenode.getStats()[ClientProtocol.GET_STATS_UNDER_REPLICATED_IDX];
  }

  /**
   * Returns count of blocks with at least one replica marked corrupt.
   * @throws IOException
   */
  public long getCorruptBlocksCount() throws IOException {
    return namenode.getStats()[ClientProtocol.GET_STATS_CORRUPT_BLOCKS_IDX];
  }

  /**
   * @return a list in which each entry describes a corrupt file/block
   * @throws AccessControlException
   * @throws IOException
   */
  public CorruptFileBlocks listCorruptFileBlocks(String path,
                                                 String cookie)
    throws IOException {
    if (namenodeProtocolProxy == null) {
      return versionBasedListCorruptFileBlocks(path, cookie);
    }
    return methodBasedListCorruptFileBlocks(path, cookie);
  }

  /** Version based list corrupt file blocks */
  private CorruptFileBlocks versionBasedListCorruptFileBlocks(String path,
      String cookie) throws IOException {
    if (namenodeVersion < ClientProtocol.LIST_CORRUPT_FILEBLOCKS_VERSION) {
      LOG.info("NameNode version is " + namenodeVersion +
               " Using older version of getCorruptFiles.");
      if (cookie != null ) {
        return new CorruptFileBlocks(new String[0], "");
      }
      ArrayList<String> str = new ArrayList<String>();
      for (FileStatus stat : namenode.getCorruptFiles()) {
        String filename = stat.getPath().toUri().getPath();
        if (filename.startsWith(path)) {
          str.add(filename);
        }
      }
      return new CorruptFileBlocks(str.toArray(new String[str.size()]), "");
    }
    return namenode.listCorruptFileBlocks(path, cookie);
  }

  /** Method based listCorruptFileBlocks */
  private CorruptFileBlocks methodBasedListCorruptFileBlocks(String path,
      String cookie) throws IOException {
    if (!namenodeProtocolProxy.isMethodSupported("listCorruptFileBlocks",
        String.class, String.class)) {
      LOG.info("NameNode version is " + namenodeVersion +
               " Using older version of getCorruptFiles.");
      if (cookie != null ) {
        return new CorruptFileBlocks(new String[0], "");
      }
      ArrayList<String> str = new ArrayList<String>();
      for (FileStatus stat : namenode.getCorruptFiles()) {
        String filename = stat.getPath().toUri().getPath();
        if (filename.startsWith(path)) {
          str.add(filename);
        }
      }
      return new CorruptFileBlocks(str.toArray(new String[str.size()]), "");
    }
    return namenode.listCorruptFileBlocks(path, cookie);
  }

  public DatanodeInfo[] datanodeReport(DatanodeReportType type)
  throws IOException {
    return namenode.getDatanodeReport(type);
  }

  /**
   * Enter, leave or get safe mode.
   * See {@link ClientProtocol#setSafeMode(FSConstants.SafeModeAction)}
   * for more details.
   *
   * @see ClientProtocol#setSafeMode(FSConstants.SafeModeAction)
   */
  public boolean setSafeMode(SafeModeAction action) throws IOException {
    return namenode.setSafeMode(action);
  }

  /**
   * Save namespace image.
   * See {@link ClientProtocol#saveNamespace()}
   * for more details.
   *
   * @see ClientProtocol#saveNamespace()
   */
  void saveNamespace(boolean force, boolean uncompressed)
  throws AccessControlException, IOException {
    try {
      if (namenodeProtocolProxy == null) {
        versionBasedSaveNamespace(force, uncompressed);
      } else {
        methodBasedSaveNamespace(force, uncompressed);
      }
    } catch(RemoteException re) {
      throw re.unwrapRemoteException(AccessControlException.class);
    }
  }

  /** Version-based save namespace */
  private void versionBasedSaveNamespace(boolean force, boolean uncompressed)
  throws AccessControlException, IOException {
    if (namenodeVersion >= ClientProtocol.SAVENAMESPACE_FORCE) {
      namenode.saveNamespace(force, uncompressed);
    } else {
      namenode.saveNamespace();
    }
  }

  /** Method-based save namespace */
  private void methodBasedSaveNamespace(boolean force, boolean uncompressed)
  throws AccessControlException, IOException {
    if (namenodeProtocolProxy.isMethodSupported(
        "saveNamespace", boolean.class, boolean.class)) {
      namenode.saveNamespace(force, uncompressed);
    } else {
      namenode.saveNamespace();
    }
  }

  /**
   * Refresh the hosts and exclude files.  (Rereads them.)
   * See {@link ClientProtocol#refreshNodes()}
   * for more details.
   *
   * @see ClientProtocol#refreshNodes()
   */
  public void refreshNodes() throws IOException {
    namenode.refreshNodes();
  }
  
  /**
   * Roll edit log at the namenode manually.
   */
  public void rollEditLog() throws IOException {
    namenode.rollEditLogAdmin();
  }

  /**
   * Enable/Disable Block Replication.
   */
  public void blockReplication(boolean isEnable) throws IOException {
    namenode.blockReplication(isEnable);
  }

  /**
   * Dumps DFS data structures into specified file.
   * See {@link ClientProtocol#metaSave(String)}
   * for more details.
   *
   * @see ClientProtocol#metaSave(String)
   */
  public void metaSave(String pathname) throws IOException {
    namenode.metaSave(pathname);
  }

  /**
   * @see ClientProtocol#finalizeUpgrade()
   */
  public void finalizeUpgrade() throws IOException {
    namenode.finalizeUpgrade();
  }

  /**
   * @see ClientProtocol#distributedUpgradeProgress(FSConstants.UpgradeAction)
   */
  public UpgradeStatusReport distributedUpgradeProgress(UpgradeAction action
                                                        ) throws IOException {
    return namenode.distributedUpgradeProgress(action);
  }
  
  public String getClusterName() throws IOException {
    if (namenodeProtocolProxy.isMethodSupported(
        "getClusterName")) {
      return namenode.getClusterName();
    } else {
      return null;
    }
  }

  /** Re-populate the namespace and diskspace count of every node with quota */
  public void recount() throws IOException {
    if (namenodeProtocolProxy.isMethodSupported("recount")) {
      namenode.recount();
    }
  }
  /**
   * Fetch the list of files that have been open longer than a
   * specified amount of time.
   * @param prefix path prefix specifying subset of files to examine
   * @param millis select files that have been open longer that this
   * @param where to start searching when there are large numbers of
   * files returned. pass null the first time, then pass the last
   * value returned by the previous call for subsequent calls.
   * @return array of OpenFileInfo objects
   * @throw IOException
   */
  public OpenFileInfo[] iterativeGetOpenFiles(
    Path prefix, int millis, String start) throws IOException {
    checkOpen();
    try {
      return namenode.iterativeGetOpenFiles(prefix.toString(), millis, start);
    } catch (RemoteException re) {
      throw re.unwrapRemoteException(AccessControlException.class,
                                     NSQuotaExceededException.class,
                                     DSQuotaExceededException.class);
    }
  }

  /**
   */
  public boolean mkdirs(String src) throws IOException {
    return mkdirs(src, null);
  }

  /**
   * Create a directory (or hierarchy of directories) with the given
   * name and permission.
   *
   * @param src The path of the directory being created
   * @param permission The permission of the directory being created.
   * If permission == null, use {@link FsPermission#getDefault()}.
   * @return True if the operation success.
   * @see ClientProtocol#mkdirs(String, FsPermission)
   */
  public boolean mkdirs(String src, FsPermission permission)throws IOException{
    checkOpen();
    clearFileStatusCache();
     
    if (permission == null) {
      permission = FsPermission.getDefault();
    }
    FsPermission masked = permission.applyUMask(FsPermission.getUMask(conf));
    LOG.debug(src + ": masked=" + masked);
    try {
      metrics.incNumCreateDirOps();
      return namenode.mkdirs(src, masked);
    } catch(RemoteException re) {
      throw re.unwrapRemoteException(AccessControlException.class,
                                     NSQuotaExceededException.class,
                                     DSQuotaExceededException.class);
    }
  }

  public ContentSummary getContentSummary(String src) throws IOException {
    try {
      return namenode.getContentSummary(src);
    } catch(RemoteException re) {
      throw re.unwrapRemoteException(AccessControlException.class,
                                     FileNotFoundException.class);
    }
  }

  /**
   * Sets or resets quotas for a directory.
   * @see org.apache.hadoop.hdfs.protocol.ClientProtocol#setQuota(String, long, long)
   */
  void setQuota(String src, long namespaceQuota, long diskspaceQuota)
                                                 throws IOException {
    // sanity check
    if ((namespaceQuota <= 0 && namespaceQuota != FSConstants.QUOTA_DONT_SET &&
         namespaceQuota != FSConstants.QUOTA_RESET) ||
        (diskspaceQuota <= 0 && diskspaceQuota != FSConstants.QUOTA_DONT_SET &&
         diskspaceQuota != FSConstants.QUOTA_RESET)) {
      throw new IllegalArgumentException("Invalid values for quota : " +
                                         namespaceQuota + " and " +
                                         diskspaceQuota);

    }

    try {
      namenode.setQuota(src, namespaceQuota, diskspaceQuota);
    } catch(RemoteException re) {
      throw re.unwrapRemoteException(AccessControlException.class,
                                     FileNotFoundException.class,
                                     NSQuotaExceededException.class,
                                     DSQuotaExceededException.class);
    }
  }

  /**
   * set the modification and access time of a file
   * @throws FileNotFoundException if the path is not a file
   */
  public void setTimes(String src, long mtime, long atime) throws IOException {
    checkOpen();
    clearFileStatusCache();

    try {
      namenode.setTimes(src, mtime, atime);
    } catch(RemoteException re) {
      throw re.unwrapRemoteException(AccessControlException.class,
                                     FileNotFoundException.class);
    }
  }

  protected int numNodeLeft(DatanodeInfo nodes[],
      ConcurrentHashMap<DatanodeInfo, Long> deadNodes) {
    int nodesLeft = 0;
    if (nodes != null) {
      for (int i = 0; i < nodes.length; i++) {
        if (!deadNodes.containsKey(nodes[i])) {
          nodesLeft++;
        }
      }
    }
    return nodesLeft;
  }
  
  boolean isLeaseCheckerStarted() {
    return leasechecker.daemon != null;
  }

  /**
   * Background thread to refresh block information for cached blocks.
   */
  class BlockLocationRenewal implements Runnable {

    private final Map<DFSInputStream, Boolean> inputStreams = Collections
      .synchronizedMap(new WeakHashMap<DFSInputStream, Boolean>());
    private volatile boolean isRunning = true;
    private final long blockRenewalCheckInterval;
    private static final long DEFAULT_RENEWAL_INTERVAL = 60 * 60 * 1000L; // 60 minutes.
    private long lastRenewal = 0;
    private Daemon daemon = null;

    public BlockLocationRenewal() {
      this.blockRenewalCheckInterval = conf.getLong(
          "dfsclient.block.location.renewal.interval",
          DEFAULT_RENEWAL_INTERVAL);
    }

    @Override
      public void run() {
        try {
          while (isRunning && clientRunning) {
            long elapsedTime = System.currentTimeMillis() - lastRenewal;
            try {
              if (elapsedTime >= this.blockRenewalCheckInterval) {
                lastRenewal = System.currentTimeMillis();
                renewLocations();
                elapsedTime = 0;
                InjectionHandler
                  .processEvent(InjectionEvent.DFSCLIENT_BLOCK_RENEWAL_DONE);
              }
              if (isRunning && clientRunning) {
                Thread.sleep(blockRenewalCheckInterval - elapsedTime);
              }
            } catch (InterruptedException ie) {
              if (LOG.isDebugEnabled()) {
                LOG.debug("BlockLocationRenewal thread received interrupted exception",
                    ie);
              }
              break;
            } catch (InterruptedIOException ioe) {
              if (LOG.isDebugEnabled()) {
                LOG.debug("BlockLocationRenewal thread received interrupted exception",
                    ioe);
              }
              break;
            } catch (IOException e) {
              // Ignore exception since its already logged.
            }
          }
        } finally {
          if (isRunning && clientRunning) {
            LOG.warn("BlockLocationRenewal thread exiting unexpectedly!");
          }
        }
      }

    public boolean populateCache(String src, long offset, long len,
        DFSLocatedBlocks locatedBlocks) throws IOException, InterruptedException {
      if (!isRunning || !clientRunning) {
        return false;
      }
      LocatedBlocks newBlocks = namenode.getBlockLocations(src, offset,
          len);
      if (newBlocks == null) {
        LOG.warn("Null blocks retrieved for : " + src + ", offset : "
            + offset + ", len : " + len);
        return true;
      }
      locatedBlocks.insertRange(newBlocks.getLocatedBlocks());
      if (LOG.isDebugEnabled()) {
        LOG.debug("Updated blocks for file : " + src + ", offset : " + offset +
            ", len : " + len);
      }
      // Wait sometime so that we don't hammer the namenode.
      Thread.sleep(1000);
      return true;
    }

    public void renewLocations() throws IOException, InterruptedException {
      // Create a copy instead of synchronizing on inputStreams and blocking other
      // operations.
      Map<DFSInputStream, Boolean> streams = null;
      synchronized (inputStreams) {
        streams = new HashMap<DFSInputStream, Boolean>(inputStreams);
      }
      for (DFSInputStream stream : streams.keySet()) {
        // Be extra careful since with weak hash map certain streams might have
        // been garbage collected.
        if (stream == null) {
          continue;
        }
        DFSLocatedBlocks locatedBlocks = stream.fetchLocatedBlocks();
        // Do not process files under construction since the client has more
        // upto date information about the size of the last block.
        if (locatedBlocks == null || locatedBlocks.isUnderConstruction()) {
          continue;
        }
        List<LocatedBlock> blocks = locatedBlocks.getLocatedBlocksCopy();
        if (blocks == null || blocks.isEmpty()) {
          continue;
        }
        long offset = blocks.get(0).getStartOffset();
        long len = blocks.get(0).getBlockSize();
        try {
          for (int i = 1; i < blocks.size(); i++) {
            if (blocks.get(i).getStartOffset() == offset + len) {
              // Have only a single NN RPC for contiguous blocks.
              len += blocks.get(i).getBlockSize();
            } else {
              if (!populateCache(stream.getFileName(), offset, len,
                    locatedBlocks)) {
                return;
              }
              // Start a new block range.
              offset = blocks.get(i).getStartOffset();
              len = blocks.get(i).getBlockSize();
            }
          }
          if(!populateCache(stream.getFileName(), offset, len,
                locatedBlocks)) {
            return;
          }
        } catch (InterruptedException ie) {
          throw ie;
        } catch (InterruptedIOException ioe) {
          throw ioe;
        } catch (Throwable t) {
          InjectionHandler
            .processEvent(InjectionEvent.DFSCLIENT_BLOCK_RENEWAL_EXCEPTION);
          LOG.warn("Could not get block locations for : " + stream.getFileName(),
              t);
          throw new IOException(t);
        }
      }
    }

    public synchronized void add(DFSInputStream stream) {
      if (daemon == null) {
        daemon = new Daemon(this);
        daemon.start();
      }
      inputStreams.put(stream, true);
    }

    public synchronized void remove(DFSInputStream stream) {
      inputStreams.remove(stream);
    }

    public synchronized void join() throws InterruptedException {
      if (daemon != null) {
        LOG.debug("Waiting for BlockLocationRenewal thread to exit");
        daemon.join();
      }
    }

    public synchronized void stop() {
      if (daemon != null) {
        daemon.interrupt();
      }
      isRunning = false;
    }
  }

  /** Lease management*/
  class LeaseChecker extends LeaseRenewal {
    /** A map from src -> DFSOutputStream of files that are currently being
     * written by this client.
     */
    private final SortedMap<String, OutputStream> pendingCreates
        = new TreeMap<String, OutputStream>();

    private Daemon daemon = null;

    public LeaseChecker(String clientName, Configuration conf) {
      super(clientName, conf);
    }

    synchronized void put(String src, OutputStream out) {
      if (clientRunning) {
        if (daemon == null) {
          daemon = new Daemon(this);
          daemon.start();
        }
        pendingCreates.put(src, out);
      }
    }

    synchronized void remove(String src) {
      pendingCreates.remove(src);
    }

    void interruptAndJoin() throws InterruptedException {
      Daemon daemonCopy = null;
      synchronized (this) {
        if (daemon != null) {
          daemon.interrupt();
          daemonCopy = daemon;
        }
      }

      if (daemonCopy != null) {
        LOG.debug("Wait for lease checker to terminate");
        daemonCopy.join();
      }
    }

    synchronized void close() {
      while (!pendingCreates.isEmpty()) {
        String src = pendingCreates.firstKey();
        OutputStream out = pendingCreates.remove(src);
        if (out != null) {
          try {
            out.close();
          } catch (IOException ie) {
            LOG.error("Exception closing file " + src+ " : " + ie, ie);
          }
        }
      }
    }

    /**
     * Abort all open files. Release resources held. Ignore all errors.
     */
    @Override
    protected synchronized void abort() {
      super.closeRenewal();
      clientRunning = false;
      while (!pendingCreates.isEmpty()) {
        String src = pendingCreates.firstKey();
        DFSOutputStream out = (DFSOutputStream)pendingCreates.remove(src);
        if (out != null) {
          try {
            out.abort();
          } catch (IOException ie) {
            LOG.error("Exception aborting file " + src+ ": ", ie);
          }
        }
      }
      RPC.stopProxy(rpcNamenode); // close connections to the namenode
    }

    @Override
    protected void renew() throws IOException {
      synchronized(this) {
        if (pendingCreates.isEmpty()) {
          return;
        }
      }
      namenode.renewLease(clientName);
    }

    /** {@inheritDoc} */
    public String toString() {
      String s = getClass().getSimpleName();
      if (LOG.isTraceEnabled()) {
        return s + "@" + DFSClient.this + ": "
               + StringUtils.stringifyException(new Throwable("for testing"));
      }
      return s;
    }
  }

  /**
   * Checks that the given block range covers the given file segment and
   * consists of contiguous blocks. This function assumes that the length
   * of the queried segment is non-zero, and a non-empty block list is
   * expected.
   * @param blockRange the set of blocks obtained for the given file segment
   * @param offset the start offset of the file segment
   * @param length the length of the file segment. Assumed to be positive.
   */
  static void checkBlockRange(List<LocatedBlock> blockRange,
      long offset, long length) throws IOException {
    boolean isValid = false;

    if (!blockRange.isEmpty()) {
      int numBlocks = blockRange.size();
      LocatedBlock firstBlock = blockRange.get(0);
      LocatedBlock lastBlock = blockRange.get(numBlocks - 1);
      long segmentEnd = offset + length;

      // Check that the queried segment is between the beginning of the first
      // block and the end of the last block in the block range.
      if (firstBlock.getStartOffset() <= offset &&
          (segmentEnd <=
           lastBlock.getStartOffset() + lastBlock.getBlockSize())) {
        isValid = true;  // There is a chance the block list is valid
        LocatedBlock prevBlock = firstBlock;
        for (int i = 1; i < numBlocks; ++i) {
          // In this loop, prevBlock is always the block #(i - 1) and curBlock
          // is the block #i.
          long prevBlkEnd = prevBlock.getStartOffset() +
              prevBlock.getBlockSize();
          LocatedBlock curBlock = blockRange.get(i);
          long curBlkOffset = curBlock.getStartOffset();
          if (prevBlkEnd != curBlkOffset ||  // Blocks are not contiguous
              prevBlkEnd <= offset ||        // Previous block is redundant
              segmentEnd <= curBlkOffset) {  // Current block is redundant
            isValid = false;
            break;
          }
          prevBlock = curBlock;
        }
      }
    }

    if (!isValid) {
      throw new IOException("Got incorrect block range for " +
          "offset=" + offset + ", length=" + length + ": " +
          blockRange);
    }
  }

  public static class DFSDataInputStream extends FSDataInputStream {
    DFSDataInputStream(DFSInputStream in)
      throws IOException {
      super(in);
    }

    /**
     * Returns the datanode from which the stream is currently reading.
     */
    public DatanodeInfo getCurrentDatanode() {
      return ((DFSInputStream)in).getCurrentDatanode();
    }

    /**
     * Returns the block containing the target position.
     */
    public Block getCurrentBlock() {
      return ((DFSInputStream)in).getCurrentBlock();
    }

    /**
     * Return collection of blocks that has already been located.
     */
    public synchronized List<LocatedBlock> getAllBlocks() throws IOException {
      return ((DFSInputStream)in).getAllBlocks();
    }
    
    public void setUnfavoredNodes(Collection<DatanodeInfo> unfavoredNodes) {
      ((DFSInputStream)in).setUnfavoredNodes(unfavoredNodes);
    }

    @Override
    public boolean isUnderConstruction() throws IOException {
      return ((DFSInputStream)in).isUnderConstruction();
    }

    public long getFileLength() {
      return ((DFSInputStream)in).getFileLength();
    }

    public int read(long position, byte[] buffer, int offset, int length,
        ReadOptions options) throws IOException {
      return ((DFSInputStream) in).read(position, buffer, offset, length,
          options);
    }
  }

  /**
   * Encapsulate multiple output streams into one object.
   */
  class MultiDataOutputStream {
    DataOutputStream[] streams;
    volatile int errorSlot;

    MultiDataOutputStream(DataOutputStream[] outs) {
      this.streams = outs;
      this.errorSlot = -1;       // no errors so far
    }

    DataOutputStream get(int i) {
      return streams[i];
    }

    void set(int i, DataOutputStream st) {
      streams[i] = st;
    }

    void write(byte[] buf, int off, int len) throws IOException {
      for (int i = 0; i < streams.length; i++) {
        try {
          streams[i].write(buf, off, len);
        } catch (IOException e) {
          errorSlot = i;
          throw e;
        }
      }
    }

    void writeInt(int v) throws IOException  {
      for (int i = 0; i < streams.length; i++) {
        try {
          streams[i].writeInt(v);
        } catch (IOException e) {
          errorSlot = i;
          throw e;
        }
      }
    }

    void flush() throws IOException {
      for (int i = 0; i < streams.length; i++) {
        try {
          streams[i].flush();
        } catch (IOException e) {
          errorSlot = i;
          throw e;
        }
      }
    }

    void close() throws IOException {
      for (int i = 0; i < streams.length; i++) {
        try {
          streams[i].close();
        } catch (IOException e) {
          errorSlot = i;
          throw e;
        }
      }
    }

    /** Returns the slot number of the file descriptor that encountered
     * an error. Returns -1 if there were no error.
     */
    int getErrorIndex() {
      return errorSlot;
    }
  }

  /**
   * Encapsulate multiple input streams into one object.
   */
  class MultiDataInputStream {
    DataInputStream[] streams;

    MultiDataInputStream(DataInputStream[] ins) {
      this.streams = ins;
    }

    DataInputStream get(int i) {
      return streams[i];
    }

    int size() {
      return streams.length;
    }

    void set(int i, DataInputStream st) {
      streams[i] = st;
    }

    void close() throws IOException {
      for (int i = 0; i < streams.length; i++) {
        streams[i].close();
      }
    }
  }

  void reportChecksumFailure(String file, Block blk, DatanodeInfo dn) {
    DatanodeInfo [] dnArr = { dn };
    LocatedBlock [] lblocks = { new LocatedBlock(blk, dnArr) };
    reportChecksumFailure(file, lblocks);
  }

  // just reports checksum failure and ignores any exception during the report.
  void reportChecksumFailure(String file, LocatedBlock lblocks[]) {
    try {
      reportBadBlocks(lblocks);
    } catch (IOException ie) {
      LOG.info("Found corruption while reading " + file
               + ".  Error repairing corrupt blocks.  Bad blocks remain. "
               + StringUtils.stringifyException(ie));
    }
  }

  /** Return the namespaceId known so far
   * Call this method with caution!!!
   * Must be called after either listLocatedStatus, open, 
   * create, or append is called */
  public int getNamespaceId() {
    return this.namespaceId;
  }
  
  /** set the namespaceId to the new value if different from the old value */
  public void updateNamespaceIdIfNeeded(int namespaceId) {
    if (this.namespaceId != namespaceId) {
      this.namespaceId = namespaceId;
    }
  }
  
  /**
   * Get the data transfer protocol version supported in the cluster
   * assuming all the datanodes have the same version.
   *
   * @return the data transfer protocol version supported in the cluster
   */
  public int getDataTransferProtocolVersion() throws IOException {
    synchronized (dataTransferVersion) {
      if (dataTransferVersion == -1) {
        // Get the version number from NN
        try {
          int remoteDataTransferVersion = namenode.getDataTransferProtocolVersion();
          updateDataTransferProtocolVersionIfNeeded(remoteDataTransferVersion);
        } catch (RemoteException re) {
          IOException ioe = re.unwrapRemoteException(IOException.class);
          if (ioe.getMessage().startsWith(IOException.class.getName() + ": " +
              NoSuchMethodException.class.getName())) {
            dataTransferVersion = 14; // last version not supportting this RPC
          } else {
            throw ioe;
          }
        }
        if (LOG.isDebugEnabled()) {
		LOG.debug("Data Transfer Protocal Version is "+ dataTransferVersion);
        }
      }
      return dataTransferVersion;
    }
  }
  
  void updateDataTransferProtocolVersionIfNeeded(int remoteDataTransferVersion) {
    int newDataTransferVersion = 0;
    if (remoteDataTransferVersion < DataTransferProtocol.DATA_TRANSFER_VERSION) {
      // client is newer than server
      newDataTransferVersion = remoteDataTransferVersion;
    } else {
      // client is older or the same as server
      newDataTransferVersion = DataTransferProtocol.DATA_TRANSFER_VERSION;
    }
    synchronized (dataTransferVersion) {
      if (dataTransferVersion != newDataTransferVersion) {
        dataTransferVersion = newDataTransferVersion;
      }
    }    
  }
  
  /**
   * @return whether we should include packet version in the packet, based on
   *         the data transfer version.
   */
  boolean ifPacketIncludeVersion() throws IOException {
    return (getDataTransferProtocolVersion() >=
        DataTransferProtocol.PACKET_INCLUDE_VERSION_VERSION);
  }
  
  int getOutPacketVersion() throws IOException {
    if (ifPacketIncludeVersion()) {
      return this.preferredPacketVersion;
    } else {
      // If the server side runs on an older version that doesn't support
      // packet version, the older format that checksum is in the first
      // is used.
      //
      return DataTransferProtocol.PACKET_VERSION_CHECKSUM_FIRST;
    }
  }
  
  /**
   * If stats object is not null, increment the read exception count
   */
  void incReadExpCntToStats() {
    if (stats != null) {
      stats.incrementCntReadException();
    }
  }

  /**
   * If stats object is not null, increment the read exception count
   */
  void incWriteExpCntToStats() {
    if (stats != null) {
      stats.incrementCntWriteException();
    }
  }
  
  /**
   * If stats object is not null, increment the files read count
   */
  void incFileReadToStats() {
    if (stats != null) {
      stats.incrementFilesRead();
    }
  }
  
  /**
   * Determine whether the input address is in the same rack as local machine
   */
  public boolean isInLocalRack(InetSocketAddress addr) {
    if (dnsToSwitchMapping == null || this.localhostNetworkLocation == null) {
      return false;
    }
    ArrayList<String> tempList = new ArrayList<String>();
    tempList.add(addr.getAddress().getHostAddress());
    List<String> retList = dnsToSwitchMapping.resolve(tempList);
    if (retList != null && retList.size() > 0) {
      return retList.get(0).equals(this.localhostNetworkLocation);
    } else {
      return false;
    }
  }
  
  public LocatedBlockWithFileName getBlockInfo(final long blockId) 
  		throws IOException {
  	return namenode.getBlockInfo(blockId);
  }

  static void sleepForUnitTest(long artificialSlowdown) {
    // This is used by unit test to trigger race conditions.
    if (artificialSlowdown > 0) {
      LOG.debug("Sleeping for artificial slowdown of " +
          artificialSlowdown + "ms");
      try {
        Thread.sleep(artificialSlowdown);
      } catch (InterruptedException e) {}
    }    
  }

  /** {@inheritDoc} */
  public String toString() {
    return getClass().getSimpleName() + "[clientName=" + clientName
        + ", ugi=" + ugi + "]";
  }
  
  static class DataNodeSlowException extends IOException {
	  public DataNodeSlowException(String msg) {
	    super(msg);
	 }
  }
  
  private void clearFileStatusCache() {
    if (fileStatusCache != null) {
      fileStatusCache.clear();
    }
  }

  public InetSocketAddress getNameNodeAddr() {
    return nameNodeAddr;
  }
}
