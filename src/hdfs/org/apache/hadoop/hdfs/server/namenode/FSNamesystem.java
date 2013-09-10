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

import java.io.BufferedWriter;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.io.PrintWriter;
import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.Formatter;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import javax.management.NotCompliantMBeanException;
import javax.management.ObjectName;
import javax.management.StandardMBean;
import javax.security.auth.login.LoginException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.ReconfigurableBase;
import org.apache.hadoop.conf.ReconfigurationException;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.OpenFileInfo;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.FileStatusExtended;
import org.apache.hadoop.hdfs.OpenFilesInfo;
import org.apache.hadoop.hdfs.protocol.AlreadyBeingCreatedException;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.BlockListAsLongs;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.protocol.DataTransferProtocol;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.DirectoryListing;
import org.apache.hadoop.hdfs.protocol.FSConstants;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlockWithMetaInfo;
import org.apache.hadoop.hdfs.protocol.LocatedBlockWithOldGS;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.protocol.LocatedBlocksWithMetaInfo;
import org.apache.hadoop.hdfs.protocol.UnregisteredDatanodeException;
import org.apache.hadoop.hdfs.protocol.VersionedLocatedBlock;
import org.apache.hadoop.hdfs.server.common.GenerationStamp;
import org.apache.hadoop.hdfs.server.common.HdfsConstants;
import org.apache.hadoop.hdfs.server.common.HdfsConstants.StartupOption;
import org.apache.hadoop.hdfs.server.common.Storage;
import org.apache.hadoop.hdfs.server.common.UpgradeStatusReport;
import org.apache.hadoop.hdfs.server.namenode.BlocksMap.BlockInfo;
import org.apache.hadoop.hdfs.server.namenode.ClusterJspHelper.NameNodeKey;
import org.apache.hadoop.hdfs.server.namenode.DatanodeDescriptor.DecommissioningStatus;
import org.apache.hadoop.hdfs.server.namenode.DecommissionManager.Monitor;
import org.apache.hadoop.hdfs.server.namenode.INodeRaidStorage.RaidBlockInfo;
import org.apache.hadoop.hdfs.server.namenode.INodeStorage.StorageType;
import org.apache.hadoop.hdfs.server.namenode.LeaseManager.Lease;
import org.apache.hadoop.hdfs.server.namenode.UnderReplicatedBlocks.BlockIterator;
import org.apache.hadoop.hdfs.server.namenode.ValidateNamespaceDirPolicy.NNStorageLocation;
import org.apache.hadoop.hdfs.server.namenode.metrics.FSNamesystemMBean;
import org.apache.hadoop.hdfs.server.namenode.metrics.FSNamesystemMetrics;
import org.apache.hadoop.hdfs.server.protocol.BlockAlreadyCommittedException;
import org.apache.hadoop.hdfs.server.protocol.BlockFlags;
import org.apache.hadoop.hdfs.server.protocol.BlocksWithLocations;
import org.apache.hadoop.hdfs.server.protocol.BlocksWithLocations.BlockWithLocations;
import org.apache.hadoop.hdfs.server.protocol.DatanodeCommand;
import org.apache.hadoop.hdfs.server.protocol.DatanodeRegistration;
import org.apache.hadoop.hdfs.server.protocol.DisallowedDatanodeException;
import org.apache.hadoop.hdfs.server.protocol.IncrementalBlockReport;
import org.apache.hadoop.hdfs.server.protocol.NamespaceInfo;
import org.apache.hadoop.hdfs.server.protocol.RaidTask;
import org.apache.hadoop.hdfs.server.protocol.UpgradeCommand;
import org.apache.hadoop.hdfs.util.InjectionEvent;
import org.apache.hadoop.hdfs.util.LightWeightHashSet;
import org.apache.hadoop.hdfs.util.LightWeightLinkedSet;
import org.apache.hadoop.hdfs.util.PathValidator;
import org.apache.hadoop.hdfs.util.UserNameValidator;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.metrics.util.MBeanUtil;
import org.apache.hadoop.net.CachedDNSToSwitchMapping;
import org.apache.hadoop.net.DNSToSwitchMapping;
import org.apache.hadoop.net.NetworkTopology;
import org.apache.hadoop.net.Node;
import org.apache.hadoop.net.NodeBase;
import org.apache.hadoop.net.ScriptBasedMapping;
import org.apache.hadoop.raid.RaidCodec;
import org.apache.hadoop.net.StaticMapping;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.UnixUserGroupInformation;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.Daemon;
import org.apache.hadoop.util.FlushableLogger;
import org.apache.hadoop.util.HostsFileReader;
import org.apache.hadoop.util.InjectionEventI;
import org.apache.hadoop.util.InjectionHandler;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.Shell;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.VersionInfo;
import org.mortbay.util.ajax.JSON;

/**
 * ************************************************
 * FSNamesystem does the actual bookkeeping work for the
 * DataNode.
 * <p/>
 * It tracks several important tables.
 * <p/>
 * 1)  valid fsname --> blocklist  (kept on disk, logged)
 * 2)  Set of all valid blocks (inverted #1)
 * 3)  block --> machinelist (kept in memory, rebuilt dynamically from reports)
 * 4)  machine --> blocklist (inverted #2)
 * 5)  LRU cache of updated-heartbeat machines
 * *************************************************
 */
public class FSNamesystem extends ReconfigurableBase
  implements FSConstants, FSNamesystemMBean, FSClusterStats,
  NameNodeMXBean {
  public static final Log LOG = LogFactory.getLog(FSNamesystem.class);
  // immediate flush logger
  private static final Log FLOG = FlushableLogger.getLogger(LOG);
  
  public static int BLOCK_DELETION_INCREMENT = 1000;  
  public static final String AUDIT_FORMAT =
    "ugi=%s\t" +  // ugi
      "ip=%s\t" +   // remote IP
      "cmd=%s\t" +  // command
      "src=%s\t" +  // src path
      "dst=%s\t" +  // dst path (optional)
      "perm=%s";    // permissions (optional)
  
  private static final ThreadLocal<Formatter> auditFormatter =
    new ThreadLocal<Formatter>() {
      protected Formatter initialValue() {
        return new Formatter(new StringBuilder(AUDIT_FORMAT.length() * 4));
      }
    };

  private static final ThreadLocal<StringBuilder> auditStringBuilder =
    new ThreadLocal<StringBuilder>() {
    protected StringBuilder initialValue() {
      return new StringBuilder(AUDIT_FORMAT.length() * 4);
    }
  };

  private static final void logAuditEvent(UserGroupInformation ugi,
                                          InetAddress addr, String cmd, String src, String dst,
                                          INode node) {
    final StringBuilder builder = auditStringBuilder.get();
    builder.setLength(0);
    builder.append("ugi=").append(ugi).append("\t").
            append("ip=").append(addr).append("\t").
            append("cmd=").append(cmd).append("\t").
            append("src=").append(src).append("\t").
            append("dst=").append(dst).append("\t").
            append("perm=");
    if (node == null) {
      builder.append("null");
    } else {
      builder.append(node.getUserName() + ':' +
          node.getGroupName() + ':' + node.getFsPermission());
    }
    auditLog.info(builder.toString());
  }

  public static final Log auditLog = LogFactory.getLog(
    FSNamesystem.class.getName() + ".audit");

  // Default initial capacity and load factor of map
  public static final int DEFAULT_INITIAL_MAP_CAPACITY = 16;
  public static final float DEFAULT_MAP_LOAD_FACTOR = 0.75f;
  public static final int DEFAULT_MAX_CORRUPT_FILEBLOCKS_RETURNED = 500;
  private boolean isPermissionEnabled;
  private INode[] permissionEnabled;
  private boolean persistBlocks;
  private UserGroupInformation fsOwner;
  private String supergroup;
  private PermissionStatus defaultPermission;
  // FSNamesystemMetrics counter variables
  private FSNamesystemMetrics myFSMetrics;
  private long capacityTotal = 0L, capacityUsed = 0L, capacityRemaining = 0L, capacityNamespaceUsed = 0L;
  private int totalLoad = 0;

  volatile long pendingReplicationBlocksCount = 0L;
  volatile long corruptReplicaBlocksCount = 0L;
  volatile long underReplicatedBlocksCount = 0L;
  volatile long numInvalidFilePathOperations = 0L;
  volatile long scheduledReplicationBlocksCount = 0L;
  volatile long excessBlocksCount = 0L;
  volatile long pendingDeletionBlocksCount = 0L;
  volatile long upgradeStartTime = 0L;
  
  //
  // Stores the correct file name hierarchy
  //
  public FSDirectory dir;

  //
  // Mapping: Block -> { INode, datanodes, self ref }
  // Updated only in response to client-sent information.
  //
  public final BlocksMap blocksMap = new BlocksMap(
      DEFAULT_INITIAL_MAP_CAPACITY,
      DEFAULT_MAP_LOAD_FACTOR, this);

  //
  // Store blocks-->datanodedescriptor(s) map of corrupt replicas
  //
  public CorruptReplicasMap corruptReplicas = new CorruptReplicasMap();

  /**
   * Stores the datanode -> block map.
   * <p/>
   * Done by storing a set of {@link DatanodeDescriptor} objects, sorted by
   * storage id. In order to keep the storage map consistent it tracks
   * all storages ever registered with the namenode.
   * A descriptor corresponding to a specific storage id can be
   * <ul>
   * <li>added to the map if it is a new storage id;</li>
   * <li>updated with a new datanode started as a replacement for the old one
   * with the same storage id; and </li>
   * <li>removed if and only if an existing datanode is restarted to serve a
   * different storage id.</li>
   * </ul> <br>
   * The list of the {@link DatanodeDescriptor}s in the map is checkpointed
   * in the namespace image file. Only the {@link DatanodeInfo} part is
   * persistent, the list of blocks is restored from the datanode block
   * reports.
   * <p/>
   * Mapping: StorageID -> DatanodeDescriptor
   */
  NavigableMap<String, DatanodeDescriptor> datanodeMap =
    new TreeMap<String, DatanodeDescriptor>();

  //
  // Keeps a Collection for every named machine containing
  // blocks that have recently been invalidated and are thought to live
  // on the machine in question.
  // Mapping: StorageID -> ArrayList<Block>
  //
  private Map<String, LightWeightHashSet<Block>> recentInvalidateSets =
    new TreeMap<String, LightWeightHashSet<Block>>();
  
  //
  // Keeps a TreeSet for every named node.  Each treeset contains
  // a list of the blocks that are "extra" at that location.  We'll
  // eventually remove these extras.
  // Mapping: StorageID -> TreeSet<Block>
  //
  Map<String, LightWeightHashSet<Block>> excessReplicateMap =
    new HashMap<String, LightWeightHashSet<Block>>();

  Random r = new Random();

  /**
   * Stores a set of DatanodeDescriptor objects.
   * This is a subset of {@link #datanodeMap}, containing nodes that are
   * considered alive.
   * The {@link HeartbeatMonitor} periodically checks for outdated entries,
   * and removes them from the list.
   */
  ArrayList<DatanodeDescriptor> heartbeats = new ArrayList<DatanodeDescriptor>();

  //
  // Store set of Blocks that need to be replicated 1 or more times.
  // We also store pending replication-orders.
  // Set of: Block
  //
  private UnderReplicatedBlocks neededReplications = new UnderReplicatedBlocks();
  private PendingReplicationBlocks pendingReplications;

  private volatile long delayOverreplicationMonitorTime = 0;
  // list of blocks that need to be checked for possible overreplication
  LightWeightLinkedSet<Block> overReplicatedBlocks = new LightWeightLinkedSet<Block>();
  LightWeightLinkedSet<RaidBlockInfo> raidEncodingTasks = 
      new LightWeightLinkedSet<RaidBlockInfo>();

  public LeaseManager leaseManager = new LeaseManager(this);

  //
  // Threaded object that checks to see if we have been
  // getting heartbeats from all clients.
  //
  Daemon hbthread = null;   // HeartbeatMonitor thread
  private LeaseManager.Monitor lmmonitor;
  public Daemon lmthread = null;   // LeaseMonitor thread
  Daemon smmthread = null;  // SafeModeMonitor thread

  public Daemon underreplthread = null;  // Replication thread for under replicated blocks
  public Daemon overreplthread = null;  // Replication thread for over replicated blocks
  public Daemon raidEncodingTaskThread = null;  // Raid task thread for raiding

  
  Daemon automaticEditsRollingThread = null;
  AutomaticEditsRoller automaticEditsRoller;
  
  private volatile boolean fsRunning = true;
  long systemStart = 0;

  //  The maximum number of replicates we should allow for a single block
  private int maxReplication;
  //  How many outgoing replication streams a given node should have at one time
  private int maxReplicationStreams;
  // MIN_REPLICATION is how many copies we need in place or else we disallow the write
  private int minReplication;
  // Min replication for closing a file
  private int minCloseReplication;
  // Default replication
  private int defaultReplication;
  // Variable to stall new replication checks for testing purposes
  private volatile boolean stallReplicationWork = false;
  // How many entries are returned by getCorruptInodes()
  int maxCorruptFilesReturned;
  // heartbeat interval from configuration
  volatile long heartbeatInterval;
  // heartbeatRecheckInterval is how often namenode checks for expired datanodes
  volatile long heartbeatRecheckInterval;
  // heartbeatExpireInterval is how long namenode waits for datanode to report
  // heartbeat
  volatile private long heartbeatExpireInterval;
  // default block size of a file
  private long defaultBlockSize = 0;
  // allow appending to hdfs files
  private boolean supportAppends = true;
  private boolean accessTimeTouchable = true;
  // enable block replication
  private volatile boolean blockReplicationEnabled = true;

  /**
   * Last block index used for replication work per priority
   */
  private int[] replIndex = new int[UnderReplicatedBlocks.LEVEL];

  /**
   * NameNode RPC address
   */
  private InetSocketAddress nameNodeAddress = null; // TODO: name-node has this field, it should be removed here
  final private NameNode nameNode;
  
  // fast initial block reports and safemode exit
  int parallelProcessingThreads = 16; // number of threads used
  int parallelRQblocksPerShard = 1000000; // blocks per shard for repl
                                                  // queues
  int parallelBRblocksPerShard = 10000; // blocks per shard for
                                                  // initial block reports
  boolean parallelBRenabled = true; //fast initial block reports enabled
  private SafeModeInfo safeMode; // safe mode information
  private Host2NodesMap host2DataNodeMap = new Host2NodesMap();

  // datanode networktoplogy
  NetworkTopology clusterMap = null;
  DNSToSwitchMapping dnsToSwitchMapping;

  // for block replicas placement
  BlockPlacementPolicy replicator;
  private boolean syncAddBlock;

  private HostsFileReader hostsReader;
  private Daemon dnthread = null;

  private long maxFsObjects = 0;          // maximum number of fs objects

  /**
   * The global generation stamp for this file system.
   */
  private final GenerationStamp generationStamp = new GenerationStamp();

  // precision of access times.
  private long accessTimePrecision = 0;

  // lock to protect FSNamesystem.
  private ReentrantReadWriteLock fsLock;
  boolean hasRwLock = false; // shall we use read/write locks?

  // do not use manual override to exit safemode
  volatile boolean manualOverrideSafeMode = false;

  private PathValidator pathValidator;

  private UserNameValidator userNameValidator;

  // Permission violations only result in entries in the namenode log.
  // The operation does not actually fail.
  private boolean permissionAuditOnly = false;

  // set of absolute path names that cannot be deleted
  Set<String> neverDeletePaths = new TreeSet<String>();

  // dynamic loading of config files
  private ConfigManager configManager;

  /** Denotes the number of safe blocks during safe mode */
  protected long blocksSafe = 0;

  /** flag indicating whether replication queues have been initialized */
  volatile protected boolean initializedReplQueues = false;
  
  volatile private boolean isInitialized = false;
  
  // executor for processing initial block reports
  ExecutorService initialBlockReportExecutor = null;
  
  
  void setInitializedReplicationQueues(boolean populatingReplQueues) {
    initializedReplQueues = populatingReplQueues;
  }
  
  //entering/leaving safemode will start/stop the executor for initial
  // block reports
  void setupInitialBlockReportExecutor(boolean populatingReplQueues) {
    if (populatingReplQueues && initialBlockReportExecutor != null) {
      // first blocks reports no longer will be processed in parallel
      initialBlockReportExecutor.shutdown();
      initialBlockReportExecutor = null;
    } else if (!populatingReplQueues) {
      // first block reports can be processed in parallel
      initialBlockReportExecutor = Executors
          .newFixedThreadPool(parallelProcessingThreads);
    }
  }
  
  /**
   * FSNamesystem constructor.
   */
  FSNamesystem(NameNode nn, Configuration conf) throws IOException {
    super(conf);
    try {
      clusterMap = new NetworkTopology(conf);
      this.nameNode = nn;
      if (nn == null) {
        automaticEditsRoller = null;
        automaticEditsRollingThread = null;
      } else {
        automaticEditsRoller = new AutomaticEditsRoller(nn);
        automaticEditsRollingThread = new Daemon(automaticEditsRoller);
      }
      initialize(nn, getConf());
      pathValidator = new PathValidator(conf);
      userNameValidator = new UserNameValidator(conf);
    } catch (IOException e) {
      LOG.error(getClass().getSimpleName() + " initialization failed.", e);
      close();
      throw e;
    }
  }

  /**
   * For testing.
   */
  FSNamesystem() {
    this.nameNode = null;
  }

  /**
   * Initialize FSNamesystem.
   */
  private void initialize(NameNode nn, Configuration conf) throws IOException {
    // Register MBean first since pedingReplication refers to myFSMetrics
    this.registerMBean(conf); // register the MBean for the FSNamesystemStutus
    // This needs to be initialized first, since it is referenced by other
    // operations in the initialization.
    
    // Validate the Namespace Directory policy before loading them
    // get the location map (mounts/shared dirs/etc
    Map<URI, NNStorageLocation> locationMap = 
        ValidateNamespaceDirPolicy.validate(conf);
    
    pendingReplications = new PendingReplicationBlocks(
        conf.getInt("dfs.replication.pending.timeout.sec",
          -1) * 1000L, myFSMetrics);
    this.systemStart = now();
    this.fsLock = new ReentrantReadWriteLock(); // non-fair
    configManager = new ConfigManager(this, conf);
    setConfigurationParameters(conf);

    // This can be null if two ports are running. Should not rely on the value.
    // The getter for this is deprecated
    this.nameNodeAddress = nn.getNameNodeAddress();
    
    // Creating root for name node
    this.dir = new FSDirectory(this, conf, 
        NNStorageConfiguration.getNamespaceDirs(conf), 
        NNStorageConfiguration.getNamespaceEditsDirs(conf),
        locationMap);
    StartupOption startOpt = NameNode.getStartupOption(conf);
    
    this.dir.loadFSImage(startOpt, conf);
    long timeTakenToLoadFSImage = now() - systemStart;
    LOG.info("Finished loading FSImage in " + timeTakenToLoadFSImage + " msecs");
    LOG.info("Total number of inodes : " + getFilesAndDirectoriesTotal());
    NameNode.getNameNodeMetrics().fsImageLoadTime.set(
      (int) timeTakenToLoadFSImage);
    
    // load directories that enabled permission checking
    loadEnabledPermissionCheckingDirs(conf);
    
    // fast RQ and BR prcessing
    // make sure we have at least one thread, by default 1
    this.parallelProcessingThreads = Math.max(1,
        getConf().getInt("dfs.safemode.processing.threads", 16));
    this.parallelRQblocksPerShard = getConf().getInt(
        "dfs.safemode.fast.rq.blocks", 1000000);
    this.parallelBRblocksPerShard = getConf().getInt(
        "dfs.safemode.fast.br.blocks", 10000);
    this.parallelBRenabled = getConf().getBoolean("dfs.safemode.fast.br.enabled", true);
    
    this.safeMode = SafeModeUtil.getInstance(conf, this);
    this.safeMode.checkMode();
    if ("true".equals(conf.get("dfs.namenode.initialize.counting"))) {
      LOG.info("Start counting items in the file tree");
      dir.rootDir.countItems();
      LOG.info("Finish counting items in the file tree");
      INodeDirectory.ItemCounts counts = dir.rootDir.getItemCounts();
      LOG.info("Counting result: " +
        counts.numDirectories + " directories, " +
        counts.numFiles + " files, and " +
        counts.numBlocks + " blocks in file tree. " +
        blocksMap.size() + " blocks in block map.");
    } else {
      LOG.info("Skip counting items in the file tree");
    }

    // initialize heartbeat & leasemanager threads
    this.hbthread = new Daemon(new HeartbeatMonitor(conf));
    this.lmmonitor = leaseManager.new Monitor();
    this.lmthread = new Daemon(lmmonitor);

    // start heartbeat & leasemanager threads
    hbthread.start();
    lmthread.start();

    // initialize replication threads
    this.underreplthread = new Daemon(new UnderReplicationMonitor());
    this.overreplthread = new Daemon(new OverReplicationMonitor());
    this.raidEncodingTaskThread = new Daemon(new RaidEncodingTaskMonitor());

    // start replication threds
    underreplthread.start();
    overreplthread.start();
    raidEncodingTaskThread.start();
    

    this.hostsReader =
      new HostsFileReader(
        conf.get(FSConstants.DFS_HOSTS, ""),
        conf.get("dfs.hosts.exclude", ""),
        !conf.getBoolean("dfs.hosts.ignoremissing", false));

    this.dnthread = new Daemon(new DecommissionManager(this).new Monitor(
      conf.getInt("dfs.namenode.decommission.interval", 30),
      conf.getInt("dfs.namenode.decommission.nodes.per.interval", 5)));
    dnthread.start();
    
    if (this.automaticEditsRollingThread != null) {
      LOG.info("Starting Automatic Edits Rolling Thread...");
      this.automaticEditsRollingThread.start();
    }

    this.dnsToSwitchMapping = ReflectionUtils.newInstance(
      conf.getClass("topology.node.switch.mapping.impl", ScriptBasedMapping.class,
        DNSToSwitchMapping.class), conf);

    /* If the dns to swith mapping supports cache, resolve network
    * locations of those hosts in the include list,
    * and store the mapping in the cache; so future calls to resolve
    * will be fast.
    */
    if (dnsToSwitchMapping instanceof CachedDNSToSwitchMapping) {
      dnsToSwitchMapping.resolve(new ArrayList<String>(hostsReader.getHostNames()));
    }

    this.replicator = BlockPlacementPolicy.getInstance(
        conf,
        this,
        this.clusterMap,
        this.hostsReader,
        this.dnsToSwitchMapping,
        this);
    this.registerMXBean();
    // Whether or not to sync each addBlock() operation to the edit log.
    this.syncAddBlock = conf.getBoolean("dfs.sync.on.every.addblock", false);
    this.isInitialized = true;
  }

  /**
   * dirs is a list of directories where the filesystem directory state
   * is stored
   */
  FSNamesystem(FSImage fsImage, Configuration conf) throws IOException {
    super(conf);
    this.nameNode = null;
    clusterMap = new NetworkTopology(conf);
    fsLock = new ReentrantReadWriteLock();
    setConfigurationParameters(conf);
    dir = new FSDirectory(fsImage, this, conf);
    loadEnabledPermissionCheckingDirs(conf);
  }
  
  /**
   * Load the predefined paths that should enable permission checking,
   * each of which represents the root of a subtree 
   * whose nodes should check permission
   * 
   * @param conf configuration
   * @throws IOException if a predefine path does not exist
   */
  private void loadEnabledPermissionCheckingDirs(Configuration conf) 
      throws IOException {
    if (this.isPermissionEnabled) {
      String[] permissionCheckingDirs = 
          conf.getStrings("dfs.permissions.checking.paths", "/");
      int numDirs = permissionCheckingDirs.length;
      if (numDirs == 0) {
        return;
      }
      this.permissionEnabled = new INode[numDirs];
      int i = 0;
      for (String src : permissionCheckingDirs) {
        INode permissionEnabledNode = this.dir.getINode(src);
        if (permissionEnabledNode == null) {
          throw new IOException(
              "Non-existent path for disabling permission Checking: " + src);
        }
        permissionEnabled[i++] = permissionEnabledNode;
      }
    }
  }
  
  /** Check if a path is predefined to enable permission checking */
  private boolean isPermissionCheckingEnabled(INode[] pathNodes) {
    if (this.isPermissionEnabled) {
      if (permissionEnabled == null) {
        return false;
      }
      for (INode enableDir : this.permissionEnabled) {
        for (INode pathNode : pathNodes) {
          if (pathNode == enableDir) {
            return true;
          }
        }
      }
      return false;
    }
    return false;
  }

  // utility methods to acquire and release read lock and write lock
  // If hasRwLock is false, then a readLock actually turns into  write lock.

  void readLock() {
    if (this.hasRwLock) {
      this.fsLock.readLock().lock();
    } else {
      writeLock();
    }
  }

  void readUnlock() {
    if (this.hasRwLock) {
      this.fsLock.readLock().unlock();
    } else {
      writeUnlock();
    }
  }

  void writeLock() {
    this.fsLock.writeLock().lock();
  }

  void writeUnlock() {
    this.fsLock.writeLock().unlock();
  }

  boolean hasWriteLock() {
    return this.fsLock.isWriteLockedByCurrentThread();
  }
  
  /**
   * Set parameters derived from heartbeat interval.
   */
  private void setHeartbeatInterval(long heartbeatInterval,
                                    long heartbeatRecheckInterval) {
    this.heartbeatInterval = heartbeatInterval;
    this.heartbeatRecheckInterval = heartbeatRecheckInterval;
    this.heartbeatExpireInterval = 2 * heartbeatRecheckInterval +
      10 * heartbeatInterval;
    ReplicationConfigKeys.blockInvalidateLimit =
      Math.max(ReplicationConfigKeys.blockInvalidateLimit,
               20 * (int) (heartbeatInterval/1000L));
    
  }
  
  long getHeartbeatExpireInterval() {
    return heartbeatExpireInterval;
  }


  /**
   * Initializes some of the members from configuration
   */
  private void setConfigurationParameters(Configuration conf)
    throws IOException {
    if (conf.getBoolean("hadoop.disable.shell", false)) {
      conf.setStrings(UnixUserGroupInformation.UGI_PROPERTY_NAME, new String[]{"hadoop", "hadoop"});
      Shell.setDisabled(true);
    }

    try {
      fsOwner = UnixUserGroupInformation.login(conf);
    } catch (LoginException e) {
      throw new IOException(StringUtils.stringifyException(e));
    }

    LOG.info("fsOwner=" + fsOwner);

    this.hasRwLock = conf.getBoolean("dfs.rwlock", false);
    this.supergroup = conf.get("dfs.permissions.supergroup", "supergroup");
    this.isPermissionEnabled = conf.getBoolean("dfs.permissions", true);
    this.setPersistBlocks(conf.getBoolean("dfs.persist.blocks", false));
    LOG.info("supergroup=" + supergroup);
    LOG.info("isPermissionEnabled=" + isPermissionEnabled);
    short filePermission = (short) conf.getInt("dfs.upgrade.permission", 0777);
    this.defaultPermission = PermissionStatus.createImmutable(
      fsOwner.getUserName(), supergroup, new FsPermission(filePermission));


    this.maxCorruptFilesReturned = conf.getInt("dfs.corruptfilesreturned.max",
      DEFAULT_MAX_CORRUPT_FILEBLOCKS_RETURNED);
    this.defaultReplication = conf.getInt("dfs.replication", 3);
    this.maxReplication = conf.getInt("dfs.replication.max", 512);
    this.minReplication = conf.getInt("dfs.replication.min", 1);
    this.minCloseReplication = conf.getInt("dfs.close.replication.min",
        minReplication);
    if (minReplication <= 0) {
      throw new IOException(
        "Unexpected configuration parameters: dfs.replication.min = "
          + minReplication
          + " must be greater than 0");
    }
    if (maxReplication >= (int) Short.MAX_VALUE) {
      throw new IOException(
        "Unexpected configuration parameters: dfs.replication.max = "
          + maxReplication + " must be less than " + (Short.MAX_VALUE));
    }
    if (maxReplication < minReplication) {
      throw new IOException(
        "Unexpected configuration parameters: dfs.replication.min = "
          + minReplication
          + " must be less than dfs.replication.max = "
          + maxReplication);
    }
    if (minCloseReplication < minReplication) {
      throw new IOException(
          "Unexpected configuration parameters: dfs.close.replication.min = "
          + minCloseReplication
          + " must be no less than dfs.replication.min = "
          + minReplication);
    }
    if (minCloseReplication > maxReplication) {
      throw new IOException(
          "Unexpected configuration paramerters: dfs.close.replication.min = "
          + minCloseReplication
          + " must be no greater than dfs.replication.max = "
          + maxReplication);
    }
    ReplicationConfigKeys.updateConfigKeys(conf);
    this.maxReplicationStreams = conf.getInt("dfs.max-repl-streams", 2);
    long heartbeatInterval = conf.getLong("dfs.heartbeat.interval", 3) * 1000;
    long heartbeatRecheckInterval = conf.getInt(
      "heartbeat.recheck.interval", 5 * 60 * 1000); // 5 minutes
    setHeartbeatInterval(heartbeatInterval, heartbeatRecheckInterval);
    this.defaultBlockSize = conf.getLong("dfs.block.size", DEFAULT_BLOCK_SIZE);
    this.maxFsObjects = conf.getLong("dfs.max.objects", 0);
    this.accessTimePrecision = conf.getLong("dfs.access.time.precision", 0);
    this.supportAppends = conf.getBoolean("dfs.support.append", false);
    this.accessTimeTouchable = conf.getBoolean("dfs.access.time.touchable", true);

    long editPreallocateSize = conf.getLong("dfs.edit.preallocate.size",
      HdfsConstants.DEFAULT_EDIT_PREALLOCATE_SIZE);
    FSEditLog.setPreallocateSize(editPreallocateSize);
    int editBufferSize = conf.getInt("dfs.edit.buffer.size",
      HdfsConstants.DEFAULT_EDIT_BUFFER_SIZE);
    FSEditLog.setBufferCapacity(editBufferSize);
    int maxBufferedTransactions = conf.getInt("dfs.max.buffered.transactions",
      HdfsConstants.DEFAULT_MAX_BUFFERED_TRANSACTIONS);
    FSEditLog.setMaxBufferedTransactions(maxBufferedTransactions);

    // Permission violations are logged in the namenode logs. The operation
    // does not fail.
    this.permissionAuditOnly = conf.getBoolean("dfs.permissions.audit.log", false);

    // set soft and hard lease period
    long hardLeaseLimit = conf.getLong(FSConstants.DFS_HARD_LEASE_KEY,
        FSConstants.LEASE_HARDLIMIT_PERIOD);
    long softLeaseLimit = conf.getLong(FSConstants.DFS_SOFT_LEASE_KEY,
        FSConstants.LEASE_SOFTLIMIT_PERIOD);
    this.leaseManager.setLeasePeriod(
        Math.min(hardLeaseLimit, softLeaseLimit), hardLeaseLimit);
  }
  
  /**
   * Return the default path permission when upgrading from releases with no
   * permissions (<=0.15) to releases with permissions (>=0.16)
   */
  protected PermissionStatus getUpgradePermission() {
    return defaultPermission;
  }

  NamespaceInfo getNamespaceInfo() {
    writeLock();
    try {
      NamespaceInfo ni = new NamespaceInfo(dir.fsImage.getNamespaceID(),
          dir.fsImage.storage.getCTime(), getDistributedUpgradeVersion());
      InjectionHandler.processEvent(
          InjectionEvent.FSNAMESYSTEM_VERSION_REQUEST, ni);
      return ni;
    } finally {
      writeUnlock();
    }
  }

  /**
   * Retrieves a list of random files with some information.
   * 
   * @param percent
   *          the percent of files to return
   * @return the list of files
   */
  public List<FileStatusExtended> getRandomFiles(double percent) {
    return dir.getRandomFileStats(percent);
  }

  public OpenFilesInfo getOpenFiles() throws IOException {
    List <FileStatusExtended> openFiles = new ArrayList <FileStatusExtended>();
    for (Lease lease : leaseManager.getSortedLeases()) {
      for (String path : lease.getPaths()) {
        FileStatusExtended stat = this.getFileInfoExtended(path,
            lease.getHolder());
        if (stat != null) {
          openFiles.add(stat);
        }
      }
    }
    return new OpenFilesInfo(openFiles, this.getGenerationStamp());
  }
  
  /**
   * Stops lease monitor thread.
   */
  public void stopLeaseMonitor() throws InterruptedException {
    if (lmmonitor != null) {
      lmmonitor.stop();
      InjectionHandler
          .processEvent(InjectionEvent.FSNAMESYSTEM_STOP_LEASEMANAGER);
    }
    if (lmthread != null) {
      // interrupt the lease monitor thread. We need to make sure the
      // interruption does not happen during
      // LeaseManager.checkLeases(), which would possibly interfere with writing
      // to edit log, and hence with clean shutdown. It essential during
      // failover, as it could exclude edit log streams!!!
      writeLock();
      try {
        lmthread.interrupt();
      } finally {
        writeUnlock();
      }
      lmthread.join();
    }
  }

  /**
   * Close down this file system manager.
   * Causes heartbeat and lease daemons to stop; waits briefly for
   * them to finish, but a short timeout returns control back to caller.
   */
  public void close() {
    fsRunning = false;
    try {
      if (pendingReplications != null) {
        pendingReplications.stop();
      }
      if (hbthread != null) {
        hbthread.interrupt();
      }      
      if (underreplthread != null) {
        underreplthread.interrupt();
      }
      if (overreplthread != null) {
        overreplthread.interrupt();
      }
      if (raidEncodingTaskThread != null) {
        raidEncodingTaskThread.interrupt();
      }
      if (dnthread != null) {
        dnthread.interrupt();
      }
      if (automaticEditsRollingThread != null) {
        automaticEditsRoller.stop();
        // We cannot interrupt roller thread. For manual failover, we want
        // the edits file operations to finish.
        automaticEditsRollingThread.join();
      }
      if (safeMode != null) {
        safeMode.shutdown();
      }
    } catch (Exception e) {
      LOG.warn("Exception shutting down FSNamesystem", e);
    } finally {
      // using finally to ensure we also wait for lease daemon
      try {
        LOG.info("Stopping LeaseManager");
        stopLeaseMonitor();
        if (InjectionHandler
            .trueCondition(InjectionEvent.FSNAMESYSTEM_CLOSE_DIRECTORY)) {
          if (dir != null) {
            LOG.info("Stopping directory (fsimage, fsedits)");
            dir.close();
          }
        }
      } catch (InterruptedException ie) {
      } catch (IOException ie) {
        LOG.error("Error closing FSDirectory", ie);
        IOUtils.cleanup(LOG, dir);
      }
    }
  }

  /**
   * Is this name system running?
   */
  boolean isRunning() {
    return fsRunning;
  }
  
  /**
   * Enable/Disable Block Replication
   */
  void blockReplication(boolean isEnable) throws AccessControlException {
    checkSuperuserPrivilege();
    if (isEnable) {
      blockReplicationEnabled = true;
    } else {
      blockReplicationEnabled = false;
    }
  }

  /**
   * Dump all metadata into specified file
   */
  void metaSave(String filename) throws IOException {    
    readLock();
    try {
      checkSuperuserPrivilege();
      File file = new File(System.getProperty("hadoop.log.dir"),
        filename);
      PrintWriter out = new PrintWriter(new BufferedWriter(
        new FileWriter(file, true)));


      //
      // Dump contents of neededReplication
      //
      synchronized (neededReplications) {
        out.println("Metasave: Blocks waiting for replication: " +
          neededReplications.size());
        for (Block block : neededReplications) {
          List<DatanodeDescriptor> containingNodes =
            new ArrayList<DatanodeDescriptor>();
          NumberReplicas numReplicas = new NumberReplicas();
          // source node returned is not used
          chooseSourceDatanode(block, containingNodes, numReplicas);
          int usableReplicas = numReplicas.liveReplicas() +
            numReplicas.decommissionedReplicas();
          // l: == live:, d: == decommissioned c: == corrupt e: == excess
          out.print(block + ((usableReplicas > 0) ? "" : " MISSING") +
            " size: " + block.getNumBytes() + 
            " (replicas:" +
            " l: " + numReplicas.liveReplicas() +
            " d: " + numReplicas.decommissionedReplicas() +
            " c: " + numReplicas.corruptReplicas() +
            " e: " + numReplicas.excessReplicas() + ") ");

          Collection<DatanodeDescriptor> corruptNodes =
            corruptReplicas.getNodes(block);

          for (Iterator<DatanodeDescriptor> jt = blocksMap.nodeIterator(block);
               jt.hasNext();) {
            DatanodeDescriptor node = jt.next();
            String state = "";
            if (corruptNodes != null && corruptNodes.contains(node)) {
              state = "(corrupt)";
            } else if (node.isDecommissioned() ||
              node.isDecommissionInProgress()) {
              state = "(decommissioned)";
            }
            out.print(" " + node + state + " : ");
          }
          out.println("");
        }
      }

      //
      // Dump blocks from pendingReplication
      //
      pendingReplications.metaSave(out);

      //
      // Dump blocks that are waiting to be deleted
      //
      dumpRecentInvalidateSets(out);
      
      //
      // Dump blocks that are excess and waiting to be deleted
      //
      dumpExcessReplicasSets(out);

      //
      // Dump all datanodes
      //
      datanodeDump(out);

      out.flush();
      out.close();
    } finally {
      readUnlock();
    }
  }

  long getDefaultBlockSize() {
    return defaultBlockSize;
  }

  long getAccessTimePrecision() {
    return accessTimePrecision;
  }

  boolean isAccessTimeSupported() {
    return accessTimePrecision > 0;
  }

  /* get replication factor of a block */

  int getReplication(Block block) {
    BlockInfo storedBlock = blocksMap.getBlockInfo(block);
    INodeFile fileINode = (storedBlock == null) ? null : storedBlock.getINode();
    if (fileINode == null) { // block does not belong to any file
      return 0;
    }
    assert !fileINode.isDirectory() : "Block cannot belong to a directory.";
    return fileINode.getBlockReplication(storedBlock);
  }

  /* updates a block in under replication queue */

  void updateNeededReplications(BlockInfo blockInfo,
                                int curReplicasDelta, 
                                int expectedReplicasDelta) {
    writeLock();
    try {
      NumberReplicas repl = countNodes(blockInfo);
      int curExpectedReplicas = getReplication(blockInfo);
      neededReplications.update(blockInfo,
        repl.liveReplicas(),
        repl.decommissionedReplicas(),
        curExpectedReplicas,
        curReplicasDelta, expectedReplicasDelta);
    } finally {
      writeUnlock();
    }
  }

  /////////////////////////////////////////////////////////
  //
  // These methods are called by secondary namenodes
  //
  /////////////////////////////////////////////////////////

  /**
   * return a list of blocks & their locations on <code>datanode</code> whose
   * total size is <code>size</code>
   *
   * @param datanode on which blocks are located
   * @param size     total size of blocks
   */
  BlocksWithLocations getBlocks(DatanodeID datanode, long size)
    throws IOException {
    readLock();
    try {
      checkSuperuserPrivilege();

      DatanodeDescriptor node = getDatanode(datanode);
      if (node == null) {
        NameNode.stateChangeLog.warn("BLOCK* NameSystem.getBlocks: "
          + "Asking for blocks from an unrecorded node " + datanode.getName());
        throw new IllegalArgumentException(
          "Unexpected exception.  Got getBlocks message for datanode " +
            datanode.getName() + ", but there is no info for it");
      }

      int numBlocks = node.numBlocks();
      if (numBlocks == 0) {
        return new BlocksWithLocations(new BlockWithLocations[0]);
      }
      Iterator<BlockInfo> iter = node.getBlockIterator();
      int startBlock = r.nextInt(numBlocks); // starting from a random block
      // skip blocks
      for (int i = 0; i < startBlock; i++) {
        iter.next();
      }
      List<BlockWithLocations> results = new ArrayList<BlockWithLocations>();
      long totalSize = 0;
      while (totalSize < size && iter.hasNext()) {
        totalSize += addBlock(iter.next(), results);
      }
      if (totalSize < size) {
        iter = node.getBlockIterator(); // start from the beginning
        for (int i = 0; i < startBlock && totalSize < size; i++) {
          totalSize += addBlock(iter.next(), results);
        }
      }

      return new BlocksWithLocations(
        results.toArray(new BlockWithLocations[results.size()]));
    } finally {
      readUnlock();
    }
  }

  /**
   * Get all valid locations of the block & add the block to results
   * return the length of the added block; 0 if the block is not added
   */
  private long addBlock(Block block, List<BlockWithLocations> results) {
    ArrayList<String> machineSet =
      new ArrayList<String>(blocksMap.numNodes(block));
    for (Iterator<DatanodeDescriptor> it =
      blocksMap.nodeIterator(block); it.hasNext();) {
      String storageID = it.next().getStorageID();
      // filter invalidate replicas
      LightWeightHashSet<Block> blocks = recentInvalidateSets.get(storageID);
      if (blocks == null || !blocks.contains(block)) {
        machineSet.add(storageID);
      }
    }
    if (machineSet.size() == 0) {
      return 0;
    } else {
      results.add(new BlockWithLocations(block,
        machineSet.toArray(new String[machineSet.size()])));
      return block.getNumBytes();
    }
  }

  /////////////////////////////////////////////////////////
  //
  // These methods are called by HadoopFS clients
  //
  /////////////////////////////////////////////////////////

  /**
   * Set permissions for an existing file.
   *
   * @throws IOException
   */
  public void setPermission(String src, FsPermission permission
  ) throws IOException {
    INode[] inodes = null;
    writeLock();
    try {
      if (isInSafeMode()) {
        throw new SafeModeException("Cannot set permission for " + src, safeMode);
      }
      inodes = dir.getExistingPathINodes(src);
      if (isPermissionCheckingEnabled(inodes)) {
        checkOwner(src, inodes);
      }
      dir.setPermission(src, permission);
    } finally {
      writeUnlock();
    }
    getEditLog().logSync(false);
    if (auditLog.isInfoEnabled()) {
      logAuditEvent(getCurrentUGI(),
        Server.getRemoteIp(),
        "setPermission", src, null, getLastINode(inodes));
    }
  }

  /**
   * Set owner for an existing file.
   *
   * @throws IOException
   */
  public void setOwner(String src, String username, String group
  ) throws IOException {
    INode[] inodes = null;
    writeLock();
    try {
      if (isInSafeMode()) {
        throw new SafeModeException("Cannot set permission for " + src, safeMode);
      }
      inodes = dir.getExistingPathINodes(src);
      if (isPermissionCheckingEnabled(inodes)) {
        FSPermissionChecker pc = checkOwner(src, inodes);
        if (!pc.isSuper) {
          if (username != null && !pc.user.equals(username)) {
            if (this.permissionAuditOnly) {
              // do not throw the exception, we would like to only log.
              LOG.warn("PermissionAudit failed on " + src + 
                  ": non-super user cannot change owner.");
            } else {
              throw new AccessControlException("Non-super user cannot change owner.");
            }
          }
          if (group != null && !pc.containsGroup(group)) {
            if (this.permissionAuditOnly) {
              // do not throw the exception, we would like to only log.
              LOG.warn("PermissionAudit failed on " + src + 
                  ": user does not belong to " + group + " .");
            } else {
              throw new AccessControlException("User does not belong to " + group
                  + " .");
            }
          }
        }
      }
      dir.setOwner(src, username, group);
    } finally {
      writeUnlock();
    }
    getEditLog().logSync(false);
    if (auditLog.isInfoEnabled()) {
      logAuditEvent(getCurrentUGI(),
        Server.getRemoteIp(),
        "setOwner", src, null, getLastINode(inodes));
    }
  }

  enum BlockMetaInfoType {
    NONE,                               // no meta info
    VERSION,                            // data transfer version#
    VERSION_AND_NAMESPACEID,            // namespaceid + data transfer version#
    VERSION_AND_NAMESPACEID_AND_OLD_GS  // namespaceid + data transfer version + old generation stamp#
  }

  /**
   * Updates DatanodeInfo for each LocatedBlock in locatedBlocks.
   */
  LocatedBlocksWithMetaInfo updateDatanodeInfo(LocatedBlocks locatedBlocks)
      throws IOException {
    if (locatedBlocks.getLocatedBlocks().size() == 0)
      return new LocatedBlocksWithMetaInfo(locatedBlocks.getFileLength(),
          locatedBlocks.getLocatedBlocks(), false,
          DataTransferProtocol.DATA_TRANSFER_VERSION, getNamespaceId(),
          this.nameNode.getClientProtocolMethodsFingerprint());
    List<LocatedBlock> newBlocks = new ArrayList<LocatedBlock>();

    readLock();
    try {
      for (LocatedBlock locBlock: locatedBlocks.getLocatedBlocks()) {
        Block block = locBlock.getBlock();
        int numNodes = blocksMap.numNodes(block);
        int numCorruptNodes = countNodes(block).corruptReplicas();
        int numCorruptReplicas = corruptReplicas.numCorruptReplicas(block); 

        if (numCorruptNodes != numCorruptReplicas) {
          LOG.warn("Inconsistent number of corrupt replicas for " + 
                   block + "blockMap has " + numCorruptNodes + 
                   " but corrupt replicas map has " + numCorruptReplicas);
        }

        boolean blockCorrupt = numCorruptNodes == numNodes;
        int numMachineSet = blockCorrupt ? numNodes : (numNodes - numCorruptNodes);
        DatanodeDescriptor[] machineSet = new DatanodeDescriptor[numMachineSet];

        if (numMachineSet > 0) {
          numNodes = 0;
          for(Iterator<DatanodeDescriptor> it = blocksMap.nodeIterator(block); 
              it.hasNext();) {
            DatanodeDescriptor dn = it.next();
            boolean replicaCorrupt = corruptReplicas.isReplicaCorrupt(block, dn);
            if (blockCorrupt || (!blockCorrupt && !replicaCorrupt))
              machineSet[numNodes++] = dn;
          }
        }

        // We need to make a copy of the block object before releasing the lock
        // to prevent the state of block is changed after that and before the
        // object is serialized to clients, to avoid potential inconsistency.
        // Further optimization is possible to avoid some object copy. Since it
        // is so far not a critical path. We leave a safe approach here.
        //
        Block blockCopy  = null;
        if (block != null) {
          blockCopy = new Block(block);
        }
        LocatedBlock newBlock = new LocatedBlock(blockCopy, machineSet, 0,
            blockCorrupt);
  newBlocks.add(newBlock);
      }
    } finally {
      readUnlock();
    }

    return new LocatedBlocksWithMetaInfo(locatedBlocks.getFileLength(),
        newBlocks, false, DataTransferProtocol.DATA_TRANSFER_VERSION,
        getNamespaceId(), this.nameNode.getClientProtocolMethodsFingerprint());
  }

  /**
   * Get block locations within the specified range.
   *
   * @see #getBlockLocations(String, long, long)
   */
  LocatedBlocks getBlockLocations(String clientMachine, String src,
                                  long offset, long length,
                                  BlockMetaInfoType type) throws IOException {
    INode[] inodes = dir.getExistingPathINodes(src);
    if (isPermissionEnabled && 
        isPermissionCheckingEnabled(inodes)) {
      checkPathAccess(src, inodes, FsAction.READ);
    }

    LocatedBlocks blocks = getBlockLocations(src, inodes[inodes.length-1],
        offset, length, false, type);
    if (blocks != null) {
      //sort the blocks
      DatanodeDescriptor client = host2DataNodeMap.getDatanodeByHost(
        clientMachine);
      for (LocatedBlock b : blocks.getLocatedBlocks()) {
        clusterMap.pseudoSortByDistance(client, b.getLocations());
        // Move decommissioned datanodes to the bottom
        Arrays.sort(b.getLocations(), DFSUtil.DECOM_COMPARATOR);
      }
    }
    return blocks;
  }

  /**
   * Get block locations within the specified range.
   *
   * @see ClientProtocol#getBlockLocations(String, long, long)
   */
  public LocatedBlocks getBlockLocations(String src, long offset, long length) throws IOException {
    INode[] inodes = dir.getExistingPathINodes(src);
    return getBlockLocations(src, inodes[inodes.length-1],
        offset, length, false, BlockMetaInfoType.NONE);
  }

  /**
   * Get block locations within the specified range.
   *
   * @see ClientProtocol#getBlockLocations(String, long, long)
   */
  public LocatedBlocks getBlockLocations(String src, INode inode, long offset, long length,
                                         boolean doAccessTime, BlockMetaInfoType type)
    throws IOException {
    if (offset < 0) {
      throw new IOException("Negative offset is not supported. File: " + src);
    }
    if (length < 0) {
      throw new IOException("Negative length is not supported. File: " + src);
    }
    final LocatedBlocks ret = getBlockLocationsInternal(src, inode,
      offset, length, Integer.MAX_VALUE, doAccessTime, type);
    if (auditLog.isInfoEnabled()) {
      logAuditEvent(getCurrentUGI(),
        Server.getRemoteIp(),
        "open", src, null, null);
    }
    return ret;
  }

  private LocatedBlocks getBlockLocationsInternal(String src,
                                                  INode inode,
                                                  long offset,
                                                  long length,
                                                  int nrBlocksToReturn,
                                                  boolean doAccessTime,
                                                  BlockMetaInfoType type)
    throws IOException {
    boolean doneSetTimes = false;

    for (int attempt = 0; attempt < 2; attempt++) {
      if (attempt == 0) { // first attempt is with readlock
        readLock();
      }  else { // second attempt is with  write lock
        writeLock(); // writelock is needed to set accesstime
        // We need to refetch the node in case it was deleted
        // while the lock was not held
        INode[] inodes = dir.getExistingPathINodes(src);
        inode = inodes[inodes.length - 1];
      }

      // if the namenode is in safemode, then do not update access time
      if (isInSafeMode()) {
        doAccessTime = false;
      }
      try {
        long now = now();
        if (inode == null || inode.isDirectory()) {
          return null;
        }
        INodeFile fileInode = (INodeFile)inode;
        if (doAccessTime && isAccessTimeSupported()) {
          if (now > inode.getAccessTime() + getAccessTimePrecision()) {
            // if we have to set access time but we only have the readlock, then
            // restart this entire operation with the writeLock.
            if (attempt == 0) {
              continue;
            }
            dir.setTimes(src, fileInode, -1, now, false);
            doneSetTimes = true; // successful setTime call
          }
        }
        return getBlockLocationsInternal(fileInode, offset, length,
            nrBlocksToReturn, type);
      } finally {
        if (attempt == 0) {
          readUnlock();
        } else {
          writeUnlock();
        }
        if (doneSetTimes) {
          getEditLog().logSyncIfNeeded(); // sync if too many transactions in buffer
        }
      }
    }
    return null; // can never reach here
  }

  LocatedBlocks getBlockLocationsInternal(INodeFile inode,
      long offset, long length,
      int nrBlocksToReturn) throws IOException {
    return getBlockLocationsInternal(inode, offset, length, 
        nrBlocksToReturn, BlockMetaInfoType.NONE);
  }

  LocatedBlocks getBlockLocationsInternal(INodeFile inode,
                                          long offset, long length,
                                          int nrBlocksToReturn,
                                          BlockMetaInfoType type)
    throws IOException {
    readLock();
    try {
      Block[] blocks = (inode.getStorageType() == StorageType.RAID_STORAGE)?
          ((INodeRaidStorage)inode.getStorage()).getSourceBlocks(): inode.getBlocks();

      if (blocks == null) {
        return null;
      }
      if (blocks.length == 0) {
        return inode.createLocatedBlocks(
            new ArrayList<LocatedBlock>(blocks.length), 
            type, this.getFSImage().storage.namespaceID,
            this.nameNode.getClientProtocolMethodsFingerprint());
      }
      List<LocatedBlock> results;
      results = new ArrayList<LocatedBlock>(blocks.length);

      int curBlk = 0;
      long curPos = 0, blkSize = 0;
      int nrBlocks = (blocks[0].getNumBytes() == 0) ? 0 : blocks.length;
      for (curBlk = 0; curBlk < nrBlocks; curBlk++) {
        blkSize = blocks[curBlk].getNumBytes();
        assert blkSize > 0 : "Block of size 0";
        if (curPos + blkSize > offset) {
          break;
        }
        curPos += blkSize;
      }

      if (nrBlocks > 0 && curBlk == nrBlocks)   // offset >= end of file
      {
        return null;
      }

      long endOff = offset + length;

      do {
        // get block locations
        int numNodes = blocksMap.numNodes(blocks[curBlk]);
        int numCorruptNodes = countCorruptNodes(blocks[curBlk]);
        int numCorruptReplicas = corruptReplicas.numCorruptReplicas(blocks[curBlk]);
        if (numCorruptNodes != numCorruptReplicas) {
          LOG.warn("Inconsistent number of corrupt replicas for " +
            blocks[curBlk] + "blockMap has " + numCorruptNodes +
            " but corrupt replicas map has " + numCorruptReplicas);
        }
        DatanodeDescriptor[] machineSet = null;
        boolean blockCorrupt = false;
        if (inode.isUnderConstruction() && curBlk == blocks.length - 1
            && blocksMap.numNodes(blocks[curBlk]) == 0) {
          // get unfinished block locations
          INodeFileUnderConstruction cons = (INodeFileUnderConstruction)inode;
          machineSet = cons.getTargets();
          blockCorrupt = false;
        } else {
          blockCorrupt = (numCorruptNodes == numNodes);
          int numMachineSet = blockCorrupt ? numNodes : 
                              (numNodes - numCorruptNodes);
          machineSet = new DatanodeDescriptor[numMachineSet];
          if (numMachineSet > 0) {
            numNodes = 0;
            for(Iterator<DatanodeDescriptor> it = 
                blocksMap.nodeIterator(blocks[curBlk]); it.hasNext();) {
              DatanodeDescriptor dn = it.next();
              boolean replicaCorrupt = corruptReplicas.isReplicaCorrupt(blocks[curBlk], dn);
              if (blockCorrupt || (!blockCorrupt && !replicaCorrupt))
                machineSet[numNodes++] = dn;
            }
          }
        }
        
        // We need to make a copy of the block object before releasing the lock
        // to prevent the state of block is changed after that and before the
        // object is serialized to clients, to avoid the file length LocatedBlocks of
        // is consistent as block length in LocatedBlock.
        //
        Block blockForLb = null;
        if (!inode.isUnderConstruction() || curBlk < blocks.length - 2) {
          // The block size is not changed if the file is finalized unless append() happens.
          // We want to keep the inconsistency in append() case for performance reason.
          // The block size of block that is not one of the last two will never change even
          // if the file is unfinalized. We can assume the block object is immutable.
          //
          blockForLb = blocks[curBlk];
        } else if (blocks[curBlk] != null) {
          blockForLb = new Block(blocks[curBlk]);
        }
        results.add(new LocatedBlock(blockForLb, machineSet, curPos,
            blockCorrupt));
        curPos += blocks[curBlk].getNumBytes();
        curBlk++;
      } while (curPos < endOff
        && curBlk < blocks.length
        && results.size() < nrBlocksToReturn);

      return inode.createLocatedBlocks(results, type,
          this.getFSImage().storage.namespaceID, this.nameNode
              .getClientProtocolMethodsFingerprint());
    } finally {
      readUnlock();
    }
  }
  
  /**
   * Merge parity file into source file to convert source file's storage format
   * from INodeRegularStorage into INodeRaidStorage. 
   * checksums will be added to each block of the source file
   * return true if merge succeeds
   */
  public void merge(String parity, String source, String codecId, int[] checksums) 
    throws IOException {
    if(FSNamesystem.LOG.isDebugEnabled()) {
      FSNamesystem.LOG.debug("merge " + parity + " to " + source);
    }
    if (isInSafeMode()) {
      throw new SafeModeException("merge: cannot merge " + parity + " to " 
                                  + source, safeMode);
    }
    // Verify parity and source 
    if (source == null || source.isEmpty() || parity == null || parity.isEmpty()) {
      throw new IOException(
          "merge: source file name or parity file name is empty");
    }
    // Verify checksums
    if (checksums == null || checksums.length == 0) {
      throw new IOException("merge: checksum array is empty or null");
    }
    // Verify codec
    RaidCodec codec = RaidCodec.getCodec(codecId);
    if (codec == null) {
      throw new IOException("merge: codec " + codecId + " doesn't exist");
    }
    
    INode[] sourceINodes = dir.getExistingPathINodes(source);
    INode[] parityINodes = dir.getExistingPathINodes(parity);
    writeLock();
    try {
      // write permissions for the source
      if (isPermissionEnabled) {
        if (isPermissionCheckingEnabled(sourceINodes)) {
          checkPathAccess(source, sourceINodes, FsAction.WRITE); 
        }
        if (isPermissionCheckingEnabled(parityINodes)) {
          checkPathAccess(parity, parityINodes, FsAction.READ); // read the file
          checkParentAccess(parity, parityINodes, FsAction.WRITE); // for delete
        }
      }
      INode sinode = sourceINodes[sourceINodes.length - 1];
      INode pinode = parityINodes[parityINodes.length - 1];
      
      if (sinode == null || pinode == null) {
        throw new IOException(
            "merge: source file or parity file doesn't exist");
      }
      
      if (sinode.isUnderConstruction() || pinode.isUnderConstruction()) {
        throw new IOException(
            "merge: source file or parity file is under construction");
      }
      
      if (sinode.isDirectory() || pinode.isDirectory()) {
        throw new IOException(
            "merge: source file or parity file is a directory");
      }
      
      if (sinode instanceof INodeHardLinkFile ||
          pinode instanceof INodeHardLinkFile) { 
        throw new IOException("merge: source file or parity file is hardlinked");  
      }

      INodeFile sourceINode = (INodeFile) sinode;
      INodeFile parityINode = (INodeFile) pinode;

      if (sourceINode.getStorageType() != StorageType.REGULAR_STORAGE
          || parityINode.getStorageType() != StorageType.REGULAR_STORAGE) {
        throw new IOException(
            "merge: source file or parity file doesn't support merge");
      }
      if (sourceINode.getModificationTime() != parityINode.getModificationTime()) {
        throw new IOException(
            "merge: source file and parity file doesn't have the same modification time");
      }
      if (parityINode.getReplication() != codec.parityReplication) {
        throw new IOException(
            "merge: parity file's replication doesn't match codec's parity replication");
      }
      
      BlockInfo[] sourceBlks = sourceINode.getBlocks();
      BlockInfo[] parityBlks = parityINode.getBlocks();
      
      if (sourceBlks == null || sourceBlks.length == 0) {
        throw new IOException("merge: " + source + " is empty");
      }
      if (parityBlks == null || parityBlks.length == 0) {
        throw new IOException("merge: " + parity + " is empty");
      }
      if (checksums.length != sourceBlks.length) {
        throw new IOException("merge: checksum length " + checksums.length +
            " doesn't match number of source blocks " + sourceBlks.length);
      }
      int expectedParityBlocks = codec.getNumStripes(sourceBlks.length) 
          * codec.numParityBlocks;
      if (expectedParityBlocks != parityBlks.length) {
        throw new IOException("merge: expect parity blocks " + 
            expectedParityBlocks + " doesn't match number of parity blocks " + 
            parityBlks.length);
      }

      if(NameNode.stateChangeLog.isDebugEnabled()) {
        NameNode.stateChangeLog.debug("DIR* NameSystem.merge: " +
            parity + " to " + source);
      }
      
      dir.mergeInternal(parityINodes, sourceINodes, parity, source, codec,
                        checksums);
    } finally {
      writeUnlock();
    }
    getEditLog().logSync();

    if (auditLog.isInfoEnabled()) {
      logAuditEvent(getCurrentUGI(),
                    Server.getRemoteIp(),
                    "merge", parity, source, 
                    getLastINode(sourceINodes));
    }
  }
  
  /**
   * Moves all the blocks from srcs and appends them to trg
   * To avoid rollbacks we will verify validitity of ALL of the args
   * before we start actual move.
   * @param target
   * @param srcs
   * @param restricted - true if equal block sizes are to be enforced, 
   *        false otherwise
   * @throws IOException
   */
  public void concat(String target, String [] srcs, boolean restricted)
    throws IOException {
    if(FSNamesystem.LOG.isDebugEnabled()) {
      FSNamesystem.LOG.debug("concat " + Arrays.toString(srcs) +
          " to " + target);
    }
    // check safe mode
    if (isInSafeMode()) {
      throw new SafeModeException("concat: cannot concat " + target, safeMode);
    }

    // verify args
    if(target.isEmpty()) {
      throw new IllegalArgumentException("concat: trg file name is empty");
    }
    if(srcs == null || srcs.length == 0) {
      throw new IllegalArgumentException("concat: srcs list is empty or null");
    }

    // currently we require all the files to be in the same dir
    String trgParent =
      target.substring(0, target.lastIndexOf(Path.SEPARATOR_CHAR));
    for(String s : srcs) {
      String srcParent = s.substring(0, s.lastIndexOf(Path.SEPARATOR_CHAR));
      if(! srcParent.equals(trgParent)) {
        throw new IllegalArgumentException
           ("concat:  srcs and target shoould be in same dir");
      }
    }

    INode[] targetInodes = null;
    writeLock();
    try {
      // write permission for the target
      if (isPermissionEnabled) {
        targetInodes = dir.getExistingPathINodes(target);
        if (isPermissionCheckingEnabled(targetInodes)) {
          checkPathAccess(target, targetInodes, FsAction.WRITE);
        }

        // and srcs
        for(String aSrc: srcs) {
          INode[] srcNodes = dir.getExistingPathINodes(aSrc);
          if (isPermissionCheckingEnabled(srcNodes)) {
            checkPathAccess(aSrc, srcNodes, FsAction.READ); // read the file
            checkParentAccess(aSrc, srcNodes, FsAction.WRITE); // for delete
          }
        }
      }


      // to make sure no two files are the same
      Set<INode> si = new HashSet<INode>();

      // we put the following prerequisite for the operation
      // replication and blocks sizes should be the same for ALL the blocks
      // check the target
      INode inode = dir.getINode(target);

      if(inode == null) {
        throw new IllegalArgumentException("concat: trg file doesn't exist");
      }
      if(inode.isUnderConstruction()) {
        throw new IllegalArgumentException("concat: trg file is uner construction");
      }

      INodeFile trgInode = (INodeFile) inode;

      INode.enforceRegularStorageINode(trgInode, "target file " + target +
          " doesn't support concatenation");
      
      BlockInfo[] blks = trgInode.getBlocks();
      // per design trg shouldn't be empty and all the blocks same size
      if(blks.length == 0) {
        throw new IllegalArgumentException("concat: "+ target + " file is empty");
      }

      long blockSize = trgInode.getPreferredBlockSize();
      
      // check the end block to be full
      if(restricted && blockSize != blks[blks.length-1].getNumBytes()) {
        throw new IllegalArgumentException(target
            + " file: last block should be full. " + " PreferredBlockSize is "
            + blockSize + " block size is: "
            + blks[blks.length - 1].getNumBytes());
      }

      si.add(trgInode);
      short repl = trgInode.getReplication();

      // now check the srcs
      boolean endSrc = false; // final src file doesn't have to have full end block
      for(int i=0; i<srcs.length; i++) {
        String src = srcs[i];
        if(i==srcs.length-1)    
          endSrc=true;
        
        INodeFile srcInode = dir.getFileINode(src);
        INode.enforceRegularStorageINode(srcInode, "concat: file " + src +
              " doesn't support concatenation");
        BlockInfo[] srcBlks = srcInode.getBlocks();
        if(src.isEmpty()
            || srcInode == null
            || srcInode.isUnderConstruction()
            || srcBlks == null
            || srcBlks.length == 0) {
          throw new IllegalArgumentException("concat: file " + src +
          " is invalid or empty or underConstruction");
        }

        // check replication and blocks size
        if(repl != srcInode.getReplication()) {
          throw new IllegalArgumentException(src + " and " + target + " " +
              "should have same replication: "
              + repl + " vs. " + srcInode.getReplication());
        }

        // verify that all the blocks are of the same length as target
        // should be enough to check the end blocks
        if (restricted) {
          int idx = srcBlks.length-1; 
          if(endSrc)    
            idx = srcBlks.length-2; // end block of endSrc is OK not to be full   
          if(idx >= 0 && srcBlks[idx].getNumBytes() != blockSize) {
            throw new IllegalArgumentException("concat: blocks sizes of " +
                src + " and " + target + " should all be the same");
          }
        }

        si.add(srcInode);
      }

      // make sure no two files are the same
      if(si.size() < srcs.length+1) { // trg + srcs
        // it means at least two files are the same
        throw new IllegalArgumentException("at least two files are the same");
      }

      if(NameNode.stateChangeLog.isDebugEnabled()) {
        NameNode.stateChangeLog.debug("DIR* NameSystem.concat: " +
            Arrays.toString(srcs) + " to " + target);
      }

      dir.concatInternal(target,srcs);
    } finally {
      writeUnlock();
    }
    getEditLog().logSync();


    if (auditLog.isInfoEnabled()) {
      logAuditEvent(getCurrentUGI(),
                    Server.getRemoteIp(),
                    "concat", Arrays.toString(srcs), target, 
                    getLastINode(targetInodes));
    }

  }

  /**
   * stores the modification and access time for this inode.
   * The access time is precise upto an hour. The transaction, if needed, is
   * written to the edits log but is not flushed.
   */
  public void setTimes(String src, long mtime, long atime) throws IOException {
    if ( !accessTimeTouchable && atime != -1) {
      throw new AccessTimeException("setTimes is not allowed for accessTime");
	}    
    setTimesInternal(src, mtime, atime);
    getEditLog().logSync(false);
  }

  private void setTimesInternal(String src, long mtime, long atime)
    throws IOException {
    writeLock();
    try {
      if (isInSafeMode()) {
        throw new SafeModeException("Cannot set accesstimes  for " + src, safeMode);
      }
      //
      // The caller needs to have write access to set access & modification times.
      if (isPermissionEnabled) {
        INode[] inodes = dir.getExistingPathINodes(src);
        if (isPermissionCheckingEnabled(inodes)) {
          checkPathAccess(src, inodes, FsAction.WRITE);
        }
      }
      INode inode = dir.getINode(src);
      if (inode != null) {
        dir.setTimes(src, inode, mtime, atime, true);
        if (auditLog.isInfoEnabled()) {
          logAuditEvent(getCurrentUGI(),
            Server.getRemoteIp(),
            "setTimes", src, null, inode);
        }
      } else {
        throw new FileNotFoundException("File/Directory " + src + " does not exist.");
      }
    } finally {
      writeUnlock();
    }
  }

  /**
   * Set replication for an existing file.
   * <p/>
   * The NameNode sets new replication and schedules either replication of
   * under-replicated data blocks or removal of the eccessive block copies
   * if the blocks are over-replicated.
   *
   * @param src         file name
   * @param replication new replication
   * @return true if successful;
   *         false if file does not exist or is a directory
   * @see ClientProtocol#setReplication(String, short)
   */
  public boolean setReplication(String src, short replication)
    throws IOException {
    boolean status = setReplicationInternal(src, replication);
    getEditLog().logSync(false);
    if (status && auditLog.isInfoEnabled()) {
      logAuditEvent(getCurrentUGI(),
        Server.getRemoteIp(),
        "setReplication", src, null, null);
    }
    return status;
  }

  private boolean setReplicationInternal(String src,
                                         short replication
  ) throws IOException {
    writeLock();
    try {
      if (isInSafeMode()) {
        throw new SafeModeException("Cannot set replication for " + src, safeMode);
      }
      verifyReplication(src, replication, null);
      if (isPermissionEnabled) {
        INode[] inodes = dir.getExistingPathINodes(src);
        if (this.isPermissionCheckingEnabled(inodes)) {
          checkPathAccess(src, inodes, FsAction.WRITE);
        }
      }

      int[] oldReplication = new int[1];
      BlockInfo[] fileBlocks;
      fileBlocks = dir.setReplication(src, replication, oldReplication);
      if (fileBlocks == null)  // file not found or is a directory
      {
        return false;
      }
      int oldRepl = oldReplication[0];
      if (oldRepl == replication) // the same replication
      {
        return true;
      }

      // update needReplication priority queues
      for (int idx = 0; idx < fileBlocks.length; idx++) {
        updateNeededReplications(fileBlocks[idx], 0, replication - oldRepl);
      }

      if (oldRepl > replication) {
        // old replication > the new one; need to remove copies
        LOG.info("Reducing replication for file " + src
          + ". New replication is " + replication);
        for (int idx = 0; idx < fileBlocks.length; idx++) {
          overReplicatedBlocks.add(fileBlocks[idx]);
        }
      } else { // replication factor is increased
        LOG.info("Increasing replication for file " + src
          + ". New replication is " + replication);
      }
      return true;
    } finally {
      writeUnlock();
    }
  }

  long getPreferredBlockSize(String filename) throws IOException {
    if (isPermissionEnabled) {
      INode[] inodes = dir.getExistingPathINodes(filename);
      if (isPermissionCheckingEnabled(inodes)) {
        checkTraverse(filename, inodes);
      }
    }
    return dir.getPreferredBlockSize(filename);
  }

  /**
   * Check whether the replication parameter is within the range
   * determined by system configuration.
   */
  private void verifyReplication(String src,
                                 short replication,
                                 String clientName
  ) throws IOException {
    String text = "file " + src
      + ((clientName != null) ? " on client " + clientName : "")
      + ".\n"
      + "Requested replication " + replication;

    if (replication > maxReplication) {
      throw new IOException(text + " exceeds maximum " + maxReplication);
    }

    if (replication < minReplication) {
      throw new IOException(
        text + " is less than the required minimum " + minReplication);
    }
  }

  /*
   * Verify that parent dir exists
   */
  private void verifyParentDir(String src) throws FileAlreadyExistsException,
      FileNotFoundException {
    Path parent = new Path(src).getParent();
    if (parent != null) {
      INode[] pathINodes = dir.getExistingPathINodes(parent.toString());
      if (pathINodes[pathINodes.length - 1] == null) {
        throw new FileNotFoundException("Parent directory doesn't exist: "
            + parent.toString());
      } else if (!pathINodes[pathINodes.length - 1].isDirectory()) {
        throw new FileAlreadyExistsException("Parent path is not a directory: "
            + parent.toString());
      }
    }
  }

  /**
   * Create a new file entry in the namespace.
   *
   * @throws IOException if file name is invalid
   *                     {@link FSDirectory#isValidToCreate(String)}.
   * @see ClientProtocol#create(String, FsPermission, String, boolean, short, long)
   */
  void startFile(String src, PermissionStatus permissions,
                 String holder, String clientMachine,
                 boolean overwrite, boolean createParent, short replication, long blockSize
  ) throws IOException {
    INodeFileUnderConstruction file =
      startFileInternal(src, permissions, holder, clientMachine, overwrite, false,
      createParent, replication, blockSize);
    getEditLog().logSync(false);
    if (auditLog.isInfoEnabled()) {
      logAuditEvent(getCurrentUGI(),
        Server.getRemoteIp(),
        "create", src, null, file);
    }
  }

  private INodeFileUnderConstruction startFileInternal(String src,
                                 PermissionStatus permissions,
                                 String holder,
                                 String clientMachine,
                                 boolean overwrite,
                                 boolean append,
                                 boolean createParent,
                                 short replication,
                                 long blockSize
  ) throws IOException {
    // tokenize the filename into components
    String[] names = INodeDirectory.getPathNames(src);
    if (!pathValidator.isValidName(src, names)) {
      numInvalidFilePathOperations++;
      throw new IOException("Invalid file name: " + src);
    }
    
    // check validity of the username
    checkUserName(permissions);
    
    // convert names to array of bytes w/o holding lock
    byte[][] components = INodeDirectory.getPathComponents(names);
    
    if (NameNode.stateChangeLog.isDebugEnabled()) {
      NameNode.stateChangeLog.debug("DIR* NameSystem.startFile: src=" + src
        + ", holder=" + holder
        + ", clientMachine=" + clientMachine
        + ", createParent=" + createParent
        + ", replication=" + replication
        + ", overwrite=" + overwrite
        + ", append=" + append);
    }
    
    writeLock();
    try {
      if (isInSafeMode()) {
        throw new SafeModeException("Cannot create file" + src, safeMode);
      }
      INode[] inodes = new INode[components.length];
      dir.rootDir.getExistingPathINodes(components, inodes);
      INode inode = inodes[inodes.length-1];
      boolean pathExists = inode != null &&
          (inode.isDirectory() || ((INodeFile)inode).getBlocks() != null);
      if (pathExists && inode.isDirectory()) {
        throw new IOException("Cannot create file " + src + "; already exists as a directory.");
      }

      if (isPermissionEnabled 
          && isPermissionCheckingEnabled(inodes)) {
        if (append || (overwrite && pathExists)) {
          checkPathAccess(src, inodes, FsAction.WRITE);
        } else {
          checkAncestorAccess(src, inodes, FsAction.WRITE);
        }
      }

      if (!createParent) {
        verifyParentDir(src);
      }

      try {
        INode myFile = (INodeFile) inode;
        recoverLeaseInternal(myFile, src, holder, clientMachine, false, false);
        long oldAccessTime = -1;
        
        try {
          verifyReplication(src, replication, clientMachine);
        } catch (IOException e) {
          throw new IOException("failed to create " + e.getMessage());
        }
        if (append) {
          if (myFile == null) {
            throw new FileNotFoundException("failed to append to non-existent file "
              + src + " on client " + clientMachine);
          } else if (myFile.isDirectory()) {
            throw new IOException("failed to append to directory " + src
              + " on client " + clientMachine);
          }
        } else if (myFile != null) {
          if (overwrite) {
        	// if the accessTime is not changable, keep the previous time
            if (!isAccessTimeSupported() && (inodes.length > 0)) {
              oldAccessTime = inodes[inodes.length - 1].getAccessTime();
            }
            deleteInternal(src, inodes, false, true);
          } else {
            throw new IOException("failed to create file " + src
              + " on client " + clientMachine
              + " either because the filename is invalid or the file exists");
          }
        }

        DatanodeDescriptor clientNode = getNode(clientMachine);

        if (append) {
          //
          // Replace current node with a INodeUnderConstruction.
          // Recreate in-memory lease record.
          //
          INodeFile node = (INodeFile) myFile;
          // cannot append the hard linked file if its reference cnt is more than 1 
          if (node instanceof INodeHardLinkFile) { 
            throw new IOException("failed to append file " + src  
                + " on client " + clientMachine 
                + " because the file is hard linked !");  
          }
          
          INodeFileUnderConstruction cons = new INodeFileUnderConstruction(
            node.getId(),
            node.getLocalNameBytes(),
            node.getReplication(),
            node.getModificationTime(),
            isAccessTimeSupported() ? node.getModificationTime() : node.getAccessTime(),
            node.getPreferredBlockSize(),
            node.getBlocks(),
            node.getPermissionStatus(),
            holder,
            clientMachine,
            clientNode);
          dir.replaceNode(src, inodes, node, cons, true);
          leaseManager.addLease(cons.clientName, src,
                                cons.getModificationTime());
          // indicate in the log that the file is reopened
          getFSImage().getEditLog().logAppendFile(src, cons);
          return cons;
        } else {
          // Now we can add the name to the filesystem. This file has no
          // blocks associated with it.
          //
          checkFsObjectLimit();

          // increment global generation stamp
          long genstamp = nextGenerationStamp();
          INodeFileUnderConstruction newNode = dir.addFile(
            src, names, components, inodes, permissions,
            replication, blockSize, holder, clientMachine, clientNode, genstamp, oldAccessTime);
          if (newNode == null) {
            throw new IOException("DIR* NameSystem.startFile: " +
              "Unable to add file to namespace.");
          }
          leaseManager.addLease(newNode.clientName, src,
                                newNode.getModificationTime());
          if (NameNode.stateChangeLog.isDebugEnabled()) {
            NameNode.stateChangeLog.debug("DIR* NameSystem.startFile: "
              + "add " + src + " to namespace for " + holder);
          }
          return newNode;
        }
      } catch (IOException ie) {
        NameNode.stateChangeLog.warn("DIR* NameSystem.startFile: "
          + ie.getMessage());
        throw ie;
      }
    } finally {
      writeUnlock();
    }
  }

  /**
   * Recover lease;
   * Immediately revoke the lease of the current lease holder and start lease
   * recovery so that the file can be forced to be closed.
   *
   * @param src the path of the file to start lease recovery
   * @param holder the lease holder's name
   * @param clientMachine the client machine's name
   * @param discardLastBlock if true, then drop last block if hsync not
   *                         yet invoked on last block
   * @return if the lease recovery completes or not
   * @throws IOException
   */
  boolean recoverLease(String src, String holder, String clientMachine,
                       boolean discardLastBlock) throws IOException {
    
    // convert names to array of bytes w/o holding lock
    byte[][] components = INodeDirectory.getPathComponents(src);
    
    writeLock();
    try {
      if (isInSafeMode()) {
        throw new SafeModeException(
            "Cannot recover the lease of " + src, safeMode);
      }
      INode inode = dir.getFileINode(components);
      if (inode == null) {
        throw new FileNotFoundException("File not found " + src);
      }

      if (!inode.isUnderConstruction()) {
        return true;
      }

      if (isPermissionEnabled) {
        INode[] inodes = dir.getExistingPathINodes(src);
        if (isPermissionCheckingEnabled(inodes)) {
          checkPathAccess(src, inodes, FsAction.WRITE);
        }
      }

      return recoverLeaseInternal(inode, src, holder, clientMachine,
                           true, discardLastBlock);
    } finally {
      writeUnlock();
    }
  }

  private boolean recoverLeaseInternal(INode fileInode,
      String src, String holder, String clientMachine, boolean force,
      boolean discardLastBlock)
  throws IOException {
    if (fileInode != null && fileInode.isUnderConstruction()) {
      INodeFileUnderConstruction pendingFile = (INodeFileUnderConstruction) fileInode;
      //
      // If the file is under construction , then it must be in our
      // leases. Find the appropriate lease record.
      //
      Lease lease = leaseManager.getLease(holder);
      //
      // We found the lease for this file. And surprisingly the original
      // holder is trying to recreate this file. This should never occur.
      //
      if (!force && lease != null) {
        Lease leaseFile = leaseManager.getLeaseByPath(src);
        if (leaseFile != null && leaseFile.equals(lease)) {
          // do not change message in this exception
          // failover client relies on this message
          throw new AlreadyBeingCreatedException(
                    "failed to create file " + src + " for " + holder +
                    " on client " + clientMachine +
                    " because current leaseholder is trying to recreate file.");
        }
      }
      //
      // Find the original holder.
      //
      lease = leaseManager.getLease(pendingFile.clientName);
      if (lease == null) {
        throw new AlreadyBeingCreatedException(
                    "failed to create file " + src + " for " + holder +
                    " on client " + clientMachine +
                    " because pendingCreates is non-null but no leases found.");
      }
      if (force) {
        // close now: no need to wait for soft lease expiration and
        // close only the file src
        LOG.info("recoverLease: recover lease " + lease + ", src=" + src +
                 " from client " + pendingFile.clientName +
                 " discardLastBloc = " + discardLastBlock);
        // We have to tell the client that the lease recovery has succeeded e.g. in case the file
        // has no blocks. Otherwise, the client will get stuck in an infinite loop.
        return internalReleaseLeaseOne(lease, src, pendingFile, discardLastBlock);
      } else {
        //
        // If the original holder has not renewed in the last SOFTLIMIT
        // period, then start lease recovery.
        //
        if (lease.expiredSoftLimit()) {
          LOG.info("startFile: recover lease " + lease + ", src=" + src +
              " from client " + pendingFile.clientName);
          internalReleaseLease(lease, src, pendingFile);
        }
        throw new AlreadyBeingCreatedException(
            "failed to create file " + src + " for " + holder +
            " on client " + clientMachine +
            ", because this file is already being created by " +
            pendingFile.getClientName() +
            " on " + pendingFile.getClientMachine());
      }
    }
    // This does not necessarily mean lease recovery has failed, but we will
    // have the client retry.
    return false;
  }

  /** 
   * Append to an existing file in the namespace.
   */
  public LocatedBlock appendFile(String src, String holder, String clientMachine
      ) throws IOException {
    return appendFile(src, holder, clientMachine, BlockMetaInfoType.NONE);
  }
  /**
   * Append to an existing file in the namespace.
   */
  public LocatedBlock appendFile(String src, String holder, String clientMachine,
      BlockMetaInfoType type) throws IOException {
    if (supportAppends == false) {
      throw new IOException("Append to hdfs not supported." +
        " Please refer to dfs.support.append configuration parameter.");
    }
    startFileInternal(src, null, holder, clientMachine, false, true,
      false, (short) maxReplication, (long) 0);
    getEditLog().logSync();

    //
    // Create a LocatedBlock object for the last block of the file
    // to be returned to the client. Return null if the file does not
    // have a partial block at the end.
    //
    LocatedBlock lb = null;
    writeLock();
    try {
      // Need to re-check existence here, since the file may have been deleted
      // in between the synchronized blocks
      INodeFileUnderConstruction file = checkLease(src, holder);
      INode.enforceRegularStorageINode(file,
          "Append to hdfs not supported for " + src);

      Block[] blocks = file.getBlocks();
      if (blocks != null && blocks.length > 0) {
        Block last = blocks[blocks.length - 1];
        BlockInfo storedBlock = blocksMap.getStoredBlock(last);
        if (file.getPreferredBlockSize() > storedBlock.getNumBytes()) {
          long fileLength = file.computeContentSummary().getLength();          
          DatanodeDescriptor[] targets = new DatanodeDescriptor[blocksMap.numNodes(last)];
          Iterator<DatanodeDescriptor> it = blocksMap.nodeIterator(last);
          for (int i = 0; it != null && it.hasNext(); i++) {
            targets[i] = it.next();
          }
          // remove the replica locations of this block from the blocksMap
          for (int i = 0; i < targets.length; i++) {
            targets[i].removeBlock(storedBlock);
          }
          // set the locations of the last block in the lease record
          file.setLastBlock(storedBlock, targets);

          // We make a copy of block object to prevent a data race that after
          // releasing the lock, before serializing the result object to clients,
          // the block object is changed. It is unlikely for this append() case
          // so potentially we can avoid the object copying. Since append() doesn't
          // happen frequently, now we make a copy to be extra safe.
          Block lastCopy = null;
          if (last != null) {
            lastCopy = new Block(last);
            // bump up the generation stamp for the lastcopy.
            if (type.equals(
                  BlockMetaInfoType.VERSION_AND_NAMESPACEID_AND_OLD_GS)) {
              lastCopy.setGenerationStamp(this.nextGenerationStamp());
            }
          }
          lb = createLocatedBlock(lastCopy, targets, fileLength
              - storedBlock.getNumBytes(),
              DataTransferProtocol.DATA_TRANSFER_VERSION, 
              last.getGenerationStamp(), type);
          
          // Remove block from replication queue.
          neededReplications.remove(storedBlock, -1);

          // remove this block from the list of pending blocks to be deleted.
          // This reduces the possibility of triggering HADOOP-1349.
          //
          for (DatanodeDescriptor dd : targets) {
            String datanodeId = dd.getStorageID();
            LightWeightHashSet<Block> v = recentInvalidateSets.get(datanodeId);
            if (v != null && v.remove(last)) {
              if (v.isEmpty()) {
                recentInvalidateSets.remove(datanodeId);
              }
              pendingDeletionBlocksCount--;
            }
          }
        }
      }
    } finally {
      writeUnlock();
    }
    if (lb != null) {
      if (NameNode.stateChangeLog.isDebugEnabled()) {
        NameNode.stateChangeLog.debug("DIR* NameSystem.appendFile: file "
          + src + " for " + holder + " at " + clientMachine
          + " block " + lb.getBlock()
          + " block size " + lb.getBlock().getNumBytes());
      }
    }

    if (auditLog.isInfoEnabled()) {
      logAuditEvent(getCurrentUGI(),
        Server.getRemoteIp(),
        "append", src, null, null);
    }
    return lb;
  }

  /**
   * Stub for old callers pre-HDFS-630
   */
  public LocatedBlock getAdditionalBlock(String src,
                                         String clientName
                                         ) throws IOException {
    return getAdditionalBlock(src, clientName, null);
  }
  
  private static String toHostPort(DatanodeID d) {
    return d.getHost() + ":" + d.getPort();
  }

  /**
   * Given information about an array of datanodes, 
   * returns an array of DatanodeDescriptors for the
   * same, or if it doesn't find the datanode, it looks for a machine local and
   * then rack local datanode, if a rack local datanode is not possible either,
   * it returns the DatanodeDescriptor of any random node in the cluster.
   *
   * @param info
   *          the array of datanodes to look for
   * @param replication number of targets to return
   * @return array of the best matches for the given datanodes
   * @throws IOException
   */
  private DatanodeDescriptor[] findBestDatanodeInCluster(
      List<DatanodeInfo> infos, int replication)
      throws IOException {
    int targetReplication = Math.min(infos.size(), replication);
    DatanodeDescriptor[] dns = new DatanodeDescriptor[targetReplication];
    boolean[] changedRacks = new boolean[targetReplication];
    boolean isOnSameRack = 
      (clusterMap.getNumOfRacks() > 1 && targetReplication > 1);
    for (int i=0; i<targetReplication; i++) {
      DatanodeInfo info = infos.get(i);
      DatanodeDescriptor node = getDatanode(info);
      if (node == null) {
        node = host2DataNodeMap.getDataNodeByIpPort(toHostPort(info));
      }
      if (node == null
          && InjectionHandler.trueCondition(
              InjectionEvent.FSNAMESYSTEM_STOP_LEASEMANAGER, info.getHost())) {
        node = host2DataNodeMap.getDatanodeByHost(info.getHost());
      }
      if (node == null) {
        if (info.getNetworkLocation() == null
            || info.getNetworkLocation().equals(NetworkTopology.DEFAULT_RACK)) {
          resolveNetworkLocation(info);
        }
        // If the current cluster doesn't contain the node, fallback to
        // something machine local and then rack local.
        List<Node> rackNodes = clusterMap.getDatanodesInRack(info
            .getNetworkLocation());
        if (rackNodes != null) {
          // Try something machine local.
          for (Node rackNode : rackNodes) {
            if (((DatanodeDescriptor) rackNode).getHost().equals(info.getHost())) {
              node = (DatanodeDescriptor) rackNode;
              break;
            }
          }

          // Try something rack local.
          if (node == null && !rackNodes.isEmpty()) {
            node = (DatanodeDescriptor) (rackNodes
                .get(r.nextInt(rackNodes.size())));
          }
        }

        // If we can't even choose rack local, just choose any node in the
        // cluster.
        if (node == null) {
          node = (DatanodeDescriptor) clusterMap.chooseRandom(NodeBase.ROOT);
          LOG.info("ChooseTarget for favored nodes: " + toString(infos)
                 + ". Node " + info + " changed its rack location to " + node);
          changedRacks[i] = true;
        } else {
          changedRacks[i] = false;
        }
      }

      if (node == null) {
        throw new IOException("Could not find any node in the cluster for : "
            + info);
      }
      dns[i] = node;
      if (i!=0 && isOnSameRack && !clusterMap.isOnSameRack(dns[i], dns[i-1])) {
        isOnSameRack = false;
      }
    }
    
    // Make sure that the returning nodes are not on the same rack
    if (isOnSameRack) {
      for (int i=0; i<targetReplication; i++) {
        if (changedRacks[i]) {
          dns[i] = (DatanodeDescriptor) clusterMap.chooseRandom(NodeBase.ROOT,
              dns[i].getNetworkLocation());
        }
      }
    }
    return dns;
  }

  /** util funcation to print a list of nodes */
  private String toString(List<DatanodeInfo> nodes) {
    StringBuilder sb = new StringBuilder();
    for (DatanodeInfo node : nodes) {
      sb.append(node).append(" ");
    }
    return sb.toString();

  }

  public DatanodeDescriptor[] computeTargets(String src, int replication,
      DatanodeDescriptor clientNode, List<Node> excludedNodes, long blockSize,
      List<DatanodeInfo> favoredNodes) throws IOException {
    DatanodeDescriptor targets[];

    // If excluded nodes null, then instantiate for future use in this method.
    if (excludedNodes == null) {
      excludedNodes = new ArrayList<Node>();
    }

    if (favoredNodes == null) {
      // Favored nodes not specified, fall back to regular block placement.
      targets = replicator.chooseTarget(src, replication, clientNode,
          new ArrayList<DatanodeDescriptor>(), excludedNodes, blockSize);
    } else {
      // If favored nodes are specified choose a target for each.
      List<DatanodeDescriptor> results = new ArrayList<DatanodeDescriptor>();
      DatanodeDescriptor[] favoredDNs =
        findBestDatanodeInCluster(favoredNodes, replication);

      List<Boolean> changedRacks = new ArrayList<Boolean>(favoredDNs.length);
      boolean isOnSameRack = 
        (clusterMap.getNumOfRacks() > 1 && favoredDNs.length > 1);

      for (int i=0; i< favoredDNs.length; i++) {
        // Choose a single node which is local to favoredNode.
        DatanodeDescriptor[] locations;
        locations = replicator.chooseTarget(src, 1,
            favoredDNs[i], new ArrayList<DatanodeDescriptor>(0),
            excludedNodes, blockSize);
        if (locations.length != 1) {
          LOG.warn("Could not find a target for file " + src
              + " with favored node " + favoredDNs[i]);
          continue;
        }
        DatanodeDescriptor target = locations[0];
        if (target != favoredDNs[i] && 
            !clusterMap.isOnSameRack(target, favoredDNs[i])) {
          LOG.info("ChooseTarget for favored nodes " + toString(favoredNodes)
                 + ". Node " + favoredDNs[i] 
                 + " changed its rack location to " + target);
          changedRacks.add(Boolean.TRUE);
        } else {
          changedRacks.add(Boolean.FALSE);
        }
        if (!isOnSameRack && !results.isEmpty() && 
            !clusterMap.isOnSameRack(target, results.get(results.size()-1))) {
          isOnSameRack = false;
        }
        results.add(target);
        excludedNodes.add(target);
      }
      
      if (isOnSameRack && results.size() == replication){
        for (int i = 0; i<changedRacks.size(); i++) {
          if (changedRacks.get(i)) {
            results.remove(i);
            break;
          }
        }
      }

      // Not enough nodes computed, place the remaining replicas.
      if (results.size() < replication) {
        // Not enough favored nodes, use replicator to choose other nodes.
        replication -= results.size();
        DatanodeDescriptor[] remainingTargets = replicator.chooseTarget(src,
            replication, clientNode,
            results, excludedNodes, blockSize);
        for (int i = 0; i < remainingTargets.length; i++) {
          results.add(remainingTargets[i]);
        }
      }
      targets = new DatanodeDescriptor[results.size()];
      results.toArray(targets);
      clusterMap.getPipeline(clientNode, targets);
    }
    return targets;
  }

  /**
   * The client would like to obtain an additional block for the indicated
   * filename (which is being written-to).  Return an array that consists
   * of the block, plus a set of machines.  The first on this list should
   * be where the client writes data.  Subsequent items in the list must
   * be provided in the connection to the first datanode.
   * <p/>
   * Make sure the previous blocks have been reported by datanodes and
   * are replicated.  Will return an empty 2-elt array if we want the
   * client to "try again later".
   */
  public LocatedBlock getAdditionalBlock(String src,
                                                     String clientName,
                                                     List<Node> excludedNodes)
  throws IOException {
    return getAdditionalBlock(src, clientName, excludedNodes, null,
        -1, null, BlockMetaInfoType.NONE);
  }
  
  public LocatedBlock getAdditionalBlock(String src,
                                         String clientName,
                                         List<Node> excludedNodes,
                                         List<DatanodeInfo> favoredNodes,                                         
                                         long startPos,
                                         Block lastBlock,
                                         BlockMetaInfoType type)
  throws IOException {
    long fileLength = -1, blockSize = -1;
    int replication = 0;
    DatanodeDescriptor clientNode = null;
    Block newBlock = null;
    DatanodeDescriptor[] targets = null;

    // convert path string into an array of bytes w/o holding lock
    byte[][] components = INodeDirectory.getPathComponents(src);
    INode[] pathINodes = new INode[components.length];

    if (NameNode.stateChangeLog.isDebugEnabled()) {
      NameNode.stateChangeLog.debug("BLOCK* NameSystem.getAdditionalBlock: file "
        + src + " for " + clientName);
    }

    boolean returnLastBlock = false;
    INodeFileUnderConstruction pendingFile;
    readLock();
    try {
      // have we exceeded the configured limit of fs objects.
      checkFsObjectLimit();

      dir.rootDir.getExistingPathINodes(components, pathINodes);
      pendingFile =
         checkLease(src, clientName, pathINodes[pathINodes.length - 1]);

      fileLength = pendingFile.computeContentSummary().getLength();
      int numExistingBlocks = pendingFile.getBlocks().length;
      if (startPos != numExistingBlocks * pendingFile.getPreferredBlockSize()
          && startPos != -1) {
        // If the file size from client size matches the the size
        // before the last block, there is a possible that it is a
        // duplicated RPC from client, when the previous operation
        // succeeded but the client failed to receive the suceess
        // ack. If the last block is empty from namenode's knowlege
        // and last block the client knows is the second last one,
        // name-node will just return the last block. 
        //
        if (lastBlock != null
            && startPos == (numExistingBlocks - 1)
                * pendingFile.getPreferredBlockSize()
            && pendingFile.getLastBlock().getNumBytes() == 0
            && numExistingBlocks > 1
            && pendingFile.getBlocks()[numExistingBlocks - 2]
                .equals(lastBlock)) {
          returnLastBlock = true;
          newBlock = pendingFile.getLastBlock();
          targets = pendingFile.getTargets();
          if (getPersistBlocks()) {
            dir.persistBlocks(src, pendingFile);
          }
        } else {
          throw new IOException(
              "File size from client side doesn't match name-node. Pos: "
                  + startPos + " #block: " + numExistingBlocks
                  + " block size: " + pendingFile.getPreferredBlockSize());
        }
      } else {
        if (!checkFileProgress(pendingFile, false)) {
          throw new NotReplicatedYetException("Not replicated yet:" + src);
        }

        blockSize = pendingFile.getPreferredBlockSize();
        clientNode = pendingFile.getClientNode();
        replication = (int) pendingFile.getReplication();
      }
    } finally {
      readUnlock();
    }
    if (returnLastBlock) {
      if (getPersistBlocks()) {
        if (syncAddBlock) {
          getEditLog().logSync(); // sync every block.
        } else {
          getEditLog().logSyncIfNeeded(); // sync if too many transactions in buffer
        }
      }
      // There is a race condition of block length change of newBlock after releasing
      // the lock. If it happens, block size in LocatedBlock and fileLength can be inconsistent
      // Since after getAdditionalBlock(), before client comsumes the result,
      // newBlock is nearly always immutable. We just keep the codes to avoid an extra
      // object copy.
      return createLocatedBlock(newBlock,
          targets, fileLength,
          DataTransferProtocol.DATA_TRANSFER_VERSION, 
          GenerationStamp.WILDCARD_STAMP, type);
    }
    
    targets = computeTargets(src, replication, clientNode,
        excludedNodes, blockSize, favoredNodes);

    if (targets.length < this.minReplication) {
      throw new IOException("File " + src + " could only be replicated to " +
        targets.length + " nodes, instead of " +
        minReplication);
    }

    // Allocate a new block and record it in the INode.
    writeLock();
    try {
      if (isInSafeMode()) {
        throw new SafeModeException("Cannot add block to " + src, safeMode);
      }
      Arrays.fill((Object[])pathINodes, null);
      dir.rootDir.getExistingPathINodes(components, pathINodes);
      pendingFile =
        checkLease(src, clientName, pathINodes[pathINodes.length - 1]);


      if (!checkFileProgress(pendingFile, false)) {
        throw new NotReplicatedYetException("Not replicated yet:" + src);
      }

      // set current last (soon to be second to last) block's 
      // size to be the file's default block size
      setLastBlockSize(pendingFile);
      
      // replicate current last (soon to be second to last) block
      // if under-replicated
      replicateLastBlock(src, pendingFile);
      
      // allocate new block record block locations in INode.
      newBlock = allocateBlock(src, pathINodes);
      pendingFile.setTargets(targets, newBlock.getGenerationStamp());

      for (DatanodeDescriptor dn : targets) {
        dn.incBlocksScheduled();
      }
      if (getPersistBlocks()) {
        dir.persistBlocks(src, pendingFile);
      }
    } finally {
      writeUnlock();
    }

    if (getPersistBlocks()) {
      if (syncAddBlock) {
        getEditLog().logSync(); // sync every block.
      } else {
        getEditLog().logSyncIfNeeded(); // sync if too many transactions in buffer
      }
    }

    // log the newly created block
    StringBuilder msg = new StringBuilder();
    msg.append("BLOCK* NameSystem.allocateBlock: " + src + ". "
        + newBlock + " allocated to " + targets.length + " datanodes:");
    for (DatanodeDescriptor dn : targets) {
      msg.append(" " + dn);
    }
    NameNode.stateChangeLog.info(msg.toString());

    // Create next block
    // There is a race condition of block length change of newBlock after releasing
    // the lock. If it happens, block size in LocatedBlock and fileLength can be inconsistent
    // Since after getAdditionalBlock(), before client comsumes the result,
    // newBlock is nearly always immutable. We just keep the codes to avoid an extra
    // object copy.
    return createLocatedBlock(newBlock, targets, fileLength,
        DataTransferProtocol.DATA_TRANSFER_VERSION, GenerationStamp.WILDCARD_STAMP, type);
  }

  private LocatedBlock createLocatedBlock(Block block,
      DatanodeDescriptor[] targets,
      long fileLen, int transferProtocolVersion, long oldGenerationStamp, 
      BlockMetaInfoType type) {
    switch (type) {
    case VERSION_AND_NAMESPACEID_AND_OLD_GS:
      return new LocatedBlockWithOldGS(block, targets, fileLen,
          transferProtocolVersion, this.getFSImage().storage.namespaceID,
          this.nameNode.getClientProtocolMethodsFingerprint(), oldGenerationStamp);
    case VERSION_AND_NAMESPACEID:
      return new LocatedBlockWithMetaInfo(block, targets, fileLen,
          transferProtocolVersion, this.getFSImage().storage.namespaceID, this.nameNode
              .getClientProtocolMethodsFingerprint());
    case VERSION:
      return new VersionedLocatedBlock(block, targets, fileLen,
          transferProtocolVersion);
    default:
      return new LocatedBlock(block, targets, fileLen);
    }
  }

  /**
   * Set last block's block size to be the file's default block size
   * @param file file inode under construction
   */
  private void setLastBlockSize(INodeFileUnderConstruction pendingFile) {
    Block block = pendingFile.getLastBlock();
    if (block != null) {
      block.setNumBytes(pendingFile.getPreferredBlockSize());
    }
  }
  
  /** 
   * Check last block of the file under construction
   * Replicate it if it is under replicated
   * @param src the file name
   * @param file the file's inode
   */
  private void replicateLastBlock(String src, INodeFileUnderConstruction file) {
    BlockInfo[] blks = file.getBlocks();
    if (blks == null || blks.length == 0)
      return;
    BlockInfo block = blks[blks.length-1];
    DatanodeDescriptor[] targets = file.getValidTargets();
    final int numOfTargets = targets == null ? 0 : targets.length;
    NumberReplicas status = countNodes(block);
    int totalReplicas = status.getTotal();
    if (numOfTargets > totalReplicas) {
      pendingReplications.add(block, numOfTargets-totalReplicas);
    }
    int expectedReplicas = file.getReplication();
    if (numOfTargets < expectedReplicas || 
        status.decommissionedReplicas != 0 ||
        status.corruptReplicas != 0) {
      LOG.info("Add " + block + " of " + src + " to needReplication queue: " + 
          " numOfTargets = " + numOfTargets +
          " decomissionedReplicas = " + status.decommissionedReplicas +
          " corruptReplicas = " + status.corruptReplicas);
      neededReplications.add(block, status.liveReplicas, 
          status.decommissionedReplicas, expectedReplicas);
    }
    
    // update metrics
    if (numOfTargets < expectedReplicas) {
      if (numOfTargets == 1) {
        myFSMetrics.numNewBlocksWithOneReplica.inc();
      }
    } else {
      myFSMetrics.numNewBlocksWithoutFailure.inc();
    }
    myFSMetrics.numNewBlocks.inc();
  }
  
  /**
   * The client would like to let go of the given block
   */
  public boolean abandonBlock(Block b, String src, String holder
  ) throws IOException {
    writeLock();
    try {
      //
      // Remove the block from the pending creates list
      //
      if (NameNode.stateChangeLog.isDebugEnabled()) {
        NameNode.stateChangeLog.debug("BLOCK* NameSystem.abandonBlock: "
          + b + "of file " + src);
      }
      if (isInSafeMode()) {
        throw new SafeModeException("Cannot abandon block " + b +
          " for fle" + src, safeMode);
      }
      INodeFileUnderConstruction file = checkLease(src, holder);
      dir.removeBlock(src, file, b);
      if (NameNode.stateChangeLog.isDebugEnabled()) {
        NameNode.stateChangeLog.debug("BLOCK* NameSystem.abandonBlock: "
          + b
          + " is removed from pendingCreates");
      }
      return true;
    } finally {
      writeUnlock();
    }
  }
  
  /**
   * Raid a file with given codec 
   * No guarantee file will be raided.
   * Namenode will schedule raiding asynchronously. The actual raiding will 
   * happen in the datanode sides
   * Namenode put toRaidStripes to a raid queue
   * A raid queue monitor will fetch to-raid stripes and schedule datanodes 
   * to raid 
   * If raiding is done when raidFile is called again, namenode will set 
   * replication of source blocks to expectedSourceRepl 
   */
  public boolean raidFile(String source, String codecId, short expectedSourceRepl)
    throws IOException {
    if (FSNamesystem.LOG.isDebugEnabled()) {
      FSNamesystem.LOG.debug("raidFile " + source + " with given codec " +
        codecId + " to replication " + expectedSourceRepl);
    }
    // Verify source
    if (source == null || source.isEmpty()) {
      throw new IOException("raidFile: source file name is empty");
    }
    // Verify codec
    RaidCodec codec = RaidCodec.getCodec(codecId);
    if (codec == null) {
      throw new IOException("raidFile: codec " + codecId + 
          " doesn't exist");
    }
    // Verify expectedSourceRepl
    if (codec.minSourceReplication > expectedSourceRepl) {
      throw new IOException(
          "raidFile: expectedSourceRepl is smaller than " + 
          codec.minSourceReplication);
    }
    INode[] sourceINodes = dir.getExistingPathINodes(source); 
    boolean status = false;
    writeLock();
    try {
      if (isInSafeMode()) {
        throw new SafeModeException("raidFile: Cannot raid file " + source, safeMode);
      }
      verifyReplication(source, expectedSourceRepl, null);
     
      // write permissions for the source
      if (isPermissionEnabled && isPermissionCheckingEnabled(sourceINodes)) {
        checkPathAccess(source, sourceINodes, FsAction.WRITE);
      }
      INode sinode = sourceINodes[sourceINodes.length - 1]; 
      if (sinode == null) {
        throw new IOException("raidFile: source file doesn't exist");
      }
      
      if (sinode.isUnderConstruction()) {
        throw new IOException(
            "raidFile: source file is under-construction"); 
      }
      
      if (sinode.isDirectory()) {
        throw new IOException("raidFile: source file is a directory");
      }
      
      INodeFile sourceINode = (INodeFile)sinode;
      BlockInfo[] blocks = sourceINode.getBlocks();
      if (blocks == null || blocks.length == 0) {
        throw new IOException("raidFile: source file is empty");
      }
      
      if (sourceINode instanceof INodeHardLinkFile) {
        throw new IOException("raidFile: cannot raid a hardlinked file");
      }
      
      if (sourceINode.getStorageType() == StorageType.RAID_STORAGE) {
        INodeRaidStorage storage = (INodeRaidStorage)sourceINode.getStorage();
        if (!storage.getCodec().id.equals(codec.id)) {
          throw new IOException("raidFile: couldn't raid a raided file");
        }
        // check parity blocks and schedule raiding or set replication
        if (codec.checkRaidProgress(sourceINode, raidEncodingTasks, this, false)) { 
          setReplicationInternal(source, expectedSourceRepl);
          status = true;
        } 
      } else if (sourceINode.getStorageType() == StorageType.REGULAR_STORAGE) {
        // allocate parity blocks
        checkFsObjectLimit();
        // Verify all source blocks have checksums
        boolean allHasChecksums = true;
        for (int i = 0; i < blocks.length; i++) {
          if (blocks[i].getChecksum() == BlockInfo.NO_BLOCK_CHECKSUM) {
            allHasChecksums = false;
            break;
          }
        }
        if (!allHasChecksums) {
          throw new IOException("raidFile: not all source blocks have checksums");
        }
        int numParityBlocks = codec.getNumParityBlocks(sourceINode.getBlocks().length);
        Block[] parityBlocks = allocateParityBlocks(numParityBlocks);
        // Convert to Raid format
        dir.raidFile(sourceINodes, source, codec, expectedSourceRepl, parityBlocks);
        // schedule raid tasks
        codec.checkRaidProgress(sourceINode, raidEncodingTasks, this, true);
      } else {
        throw new IllegalArgumentException("raidFile: storage is not valid");
      }
    } finally {
      writeUnlock();
    }
    getEditLog().logSync();
    if (auditLog.isInfoEnabled()) {
      logAuditEvent(getCurrentUGI(),
                    Server.getRemoteIp(),
                    "raidFile", source, null,
                    getLastINode(sourceINodes));
    }
    return status;
  }

  /**
 *    * The client would like to let go of a given file
 *       */
  public boolean abandonFile(String src, String holder
      ) throws IOException {
    writeLock();
    try {
      if (NameNode.stateChangeLog.isDebugEnabled()) {
        NameNode.stateChangeLog.debug("FILE* NameSystem.abandonFile: " + src);
      }
      if (isInSafeMode()) {
        throw new SafeModeException("Cannot abandon file " + src, safeMode);
      }
      checkLease(src, holder);
      internalReleaseLeaseOne(leaseManager.getLease(holder), src);
      if (NameNode.stateChangeLog.isDebugEnabled()) {
        NameNode.stateChangeLog.debug("FILE* NameSystem.abandonFile: "
                                        + " has been scheduled for lease recovery");
      }
      return true;
    } finally {
      writeUnlock();
    }
  }

  // make sure that we still have the lease on this file.

  private INodeFileUnderConstruction checkLease(String src, String holder)
    throws IOException {
    INodeFile file = dir.getFileINode(src);
    return checkLease(src, holder, file);
  }

  private INodeFileUnderConstruction checkLease(String src, String holder, INode file)
    throws IOException {

    if (file == null || file.isDirectory()) {
      Lease lease = leaseManager.getLease(holder);
      throw new LeaseExpiredException("No lease on " + src +
        " File does not exist. " +
        (lease != null ? lease.toString() :
          "Holder " + holder +
            " does not have any open files."));
    }
    if (!file.isUnderConstruction()) {
      Lease lease = leaseManager.getLease(holder);
      throw new LeaseExpiredException("No lease on " + src +
        " File is not open for writing. " +
        (lease != null ? lease.toString() :
          "Holder " + holder +
            " does not have any open files."));
    }
    INodeFileUnderConstruction pendingFile = (INodeFileUnderConstruction) file;
    if (holder != null && !pendingFile.getClientName().equals(holder)) {
      throw new LeaseExpiredException("Lease mismatch on " + src + " owned by "
        + pendingFile.getClientName() + " but is accessed by " + holder);
    }
    return pendingFile;
  }

  /**
   * The FSNamesystem will already know the blocks that make up the file.
   * Before we return, we make sure that all the file's blocks have
   * been reported by datanodes and are replicated correctly.
   */

  enum CompleteFileStatus {
    OPERATION_FAILED,
    STILL_WAITING,
    COMPLETE_SUCCESS
  }

  public CompleteFileStatus completeFile(String src, String holder,
      long fileLen, Block lastBlock) throws IOException {
    CompleteFileStatus status = completeFileInternal(src, holder, fileLen,
        lastBlock);
    getEditLog().logSync();
    return status;
  }

  private void checkFilePath(String src, INode[] inodes) throws IOException {
    if (inodes.length == 0 || inodes.length == 1) {
      throw new IOException("Illegal file path: " + src +
            "! Must start from root.");
    }
    for (int i=0; i<inodes.length-1; i++) {
      if (inodes[i]==null || !inodes[i].isDirectory()) {
        throw new IOException("Path doesn't exist: " + src);
      }
    }
  }
  
  private CompleteFileStatus completeFileInternal(String src,
                                                  String holder,
                                                  long fileLen,
                                                  Block lastBlock)
    throws IOException {
    // convert path string into an array of bytes w/o holding lock
    byte[][] components = INodeDirectory.getPathComponents(src);

    writeLock();
    try {
      if (NameNode.stateChangeLog.isDebugEnabled()) {
        NameNode.stateChangeLog.debug("DIR* NameSystem.completeFile: " + src
            + " for " + holder
            + ((fileLen != -1) ? (" file length " + fileLen) : "")
            + ((lastBlock != null) ? (" last block: " + lastBlock) : ""));
      }
      if (isInSafeMode()) {
        throw new SafeModeException("Cannot complete file " + src, safeMode);
      }
      INode[] inodes = new INode[components.length];
      dir.rootDir.getExistingPathINodes(components, inodes);
      checkFilePath(src, inodes);
      
      INode inode = inodes[inodes.length-1];
      if (inode != null && !inode.isUnderConstruction() && !inode.isDirectory()
          && fileLen != -1 && lastBlock != null && inode instanceof INodeFile) {
        // We assume it is a duplicate call if the file is not under
        // construction. In that case, we'll compare file size and
        // the last block ID that the client knows, instead of checking
        // leases. Then it'll just recommit the edit log and return
        // success. If client doesn't send the last block metadata
        // (most likely because it uses older protocol), we'll simply
        // fail the call.
        //
        INodeFile inf = (INodeFile) inode;
        long sumBlockLen = inf.getFileSize(); 
        if (sumBlockLen != fileLen) {
          throw new IOException(
              "Try close a closed file: file size in client side "
                  + "doesn't match name-node. client: " + fileLen
                  + " name-node: " + sumBlockLen);
        }
        
        if (!inf.getLastBlock().equals(lastBlock)) {
          throw new IOException(
              "Try close a closed file: last block from client side "
                  + "doesn't match name-node. client: " + lastBlock
                  + " name-node: " + inf.getLastBlock());          
        }

        // we need to write logs again in case the log was not synced
        // for the previous request.
        dir.closeFile(src, inf);
        NameNode.stateChangeLog
            .info("DIR* NameSystem.completeFile: process duplicate request: file "
                + src
                + " is closed by "
                + holder
                + ((fileLen != -1) ? (" file length " + fileLen) : "")
                + ((lastBlock != null) ? (" last block: " + lastBlock) : ""));
        return CompleteFileStatus.COMPLETE_SUCCESS;
      }
      
      INodeFileUnderConstruction pendingFile = checkLease(
          src, holder, inode);
      Block[] fileBlocks = pendingFile.getBlocks();
      if (fileBlocks == null) {
        NameNode.stateChangeLog.warn("DIR* NameSystem.completeFile: "
          + "failed to complete " + src
          + " because dir.getFileBlocks() is null " +
          " and pendingFile is " +
          ((pendingFile == null) ? "null" :
            ("from " + pendingFile.getClientMachine()))
        );
        return CompleteFileStatus.OPERATION_FAILED;
      }
      
      if (!pendingFile.isLastBlockReplicated()) {
        if (!checkBlockProgress(
            pendingFile, pendingFile.getLastBlock(), 1)) { 
          // check last block has at least one replica
          return CompleteFileStatus.STILL_WAITING;
        }
        if (fileLen != -1) {
          long sumBlockLen = pendingFile.getFileSize();
          if (sumBlockLen != fileLen) {
            throw new IOException(
                "file size in client side doesn't match the closed file. client: "
                + fileLen + " name-node: " + sumBlockLen);
          }
        }

        // replicate last block if under-replicated
        try {
        replicateLastBlock(src, pendingFile);
        } catch (Exception e) {
          LOG.info(e);
        }
        
        // mark last block has been scheduled to be replicated
        pendingFile.setLastBlockReplicated();
      }

      // make sure all blocks of the file meet the min close replication factor
      if (!checkFileProgress(pendingFile, true)) { 
        return CompleteFileStatus.STILL_WAITING;
      }
      
      finalizeINodeFileUnderConstruction(src, inodes, pendingFile);
      
    } finally {
      writeUnlock();
    }
    if (NameNode.stateChangeLog.isDebugEnabled()) {
      NameNode.stateChangeLog.info("DIR* NameSystem.completeFile: file " + src
          + " is closed by " + holder
          + ((fileLen != -1) ? (" file length " + fileLen) : "")
          + ((lastBlock != null) ? (" last block: " + lastBlock) : ""));
    } else {
      NameNode.stateChangeLog.info("DIR* NameSystem.completeFile: file " + src
          + " is closed by " + holder + " - lease removed");
    }

    return CompleteFileStatus.COMPLETE_SUCCESS;
  }

  private static Random randBlockId = new Random();
  
  /**
   * For now, we will allow only negative block ids
   */
  private static long generateBlockId() {
    long id = randBlockId.nextLong();
    return id < 0 ? id : -1 * id;
  }

  /**
   * Allocate a block at the given pending filename
   *
   * @param src    path to the file
   * @param inodes INode representing each of the components of src.
   *               <code>inodes[inodes.length-1]</code> is the INode for the file.
   */
  private Block allocateBlock(String src, INode[] inodes) throws IOException {
    Block b = new Block(generateBlockId(), 0, 0);
    while (isValidBlock(b)) {
      b.setBlockId(generateBlockId());
    }
    b.setGenerationStamp(getGenerationStamp());
    b = dir.addBlock(src, inodes, b);
    return b;
  }
  
  /**
   * Allocate a number of parity blocks
   * Require a write lock
   * @param numParityBlocks 
   */
  private Block[] allocateParityBlocks(int numParityBlocks) throws IOException {
    Block[] blocks = new Block[numParityBlocks];
    for (int i = 0; i < numParityBlocks; i++) {
      Block b = new Block(generateBlockId(), 0, 0);
      while (isValidBlock(b)) {
        b.setBlockId(generateBlockId());
      }
      b.setGenerationStamp(getGenerationStamp());
      blocks[i] = b;
    }
    return blocks;
  }

  /**
   * Check that the indicated file's blocks are present and
   * replicated.  If not, return false. If checkall is true, then check
   * all blocks, otherwise check only penultimate block.
   */
  boolean checkFileProgress(INodeFile v, boolean checkall) 
    throws IOException {
    INode.enforceRegularStorageINode(v, 
        "checkFileProgress is not supported for non-regular files");
    if (checkall) {
      //
      // check all blocks of the file.
      //
      int closeFileReplicationMin =
        Math.min(v.getReplication(), this.minCloseReplication);
      for (Block block : v.getBlocks()) {
        if (!checkBlockProgress(v, block, closeFileReplicationMin)) {
          return false;
        }
      }
      return true;
    } else {
      //
      // check the penultimate block of this file
      //
      Block b = v.getStorage().getPenultimateBlock();
      return checkBlockProgress(v, b, minReplication);
    }
  }

  private boolean checkBlockProgress(
      INodeFile v, Block b, int expectedReplication) {
    if (b != null) {
      int numNodes = blocksMap.numNodes(b);
      if (numNodes < expectedReplication) {
        LOG.info(
          "INodeFile " + v + " block " + b + " has replication " +
            numNodes + " and requires " + this.minReplication
        );

        return false;
      }
    }
    return true;
    
  }
  /**
   * Remove a datanode from the invalidatesSet
   *
   * @param n datanode
   */
  void removeFromInvalidates(String storageID) {
    LightWeightHashSet<Block> blocks = recentInvalidateSets.remove(storageID);
    if (blocks != null) {
      pendingDeletionBlocksCount -= blocks.size();
    }
  }

  /**
   * Adds block to list of blocks which will be invalidated on
   * specified datanode and log the move
   *
   * @param b block
   * @param n datanode
   */
  void addToInvalidates(Block b, DatanodeInfo n, boolean ackRequired) {
    addToInvalidatesNoLog(b, n, ackRequired);
    if (isInitialized && !isInSafeModeInternal()) {
      // do not log in startup phase
      NameNode.stateChangeLog.info("BLOCK* NameSystem.addToInvalidates: "
        + b.getBlockName() + " is added to invalidSet of " + n.getName());
    }
  }

  /**
   * Adds block to list of blocks which will be invalidated on
   * specified datanode
   *
   * @param b block
   * @param n datanode
   */
  void addToInvalidatesNoLog(Block b, DatanodeInfo n, boolean ackRequired) {
    // We are the standby avatar and we don't want to add blocks to the
    // invalidates list.
    if (this.getNameNode().shouldRetryAbsentBlocks()) {
      return;
    }

    LightWeightHashSet<Block> invalidateSet = recentInvalidateSets.get(n
        .getStorageID());
    if (invalidateSet == null) {
      invalidateSet = new LightWeightHashSet<Block>();
      recentInvalidateSets.put(n.getStorageID(), invalidateSet);
    }
    if(!ackRequired){
      b.setNumBytes(BlockFlags.NO_ACK);
    }
    if (invalidateSet.add(b)) {
      pendingDeletionBlocksCount++;
    }
  }

  /**
   * Adds block to list of blocks which will be invalidated on
   * all its datanodes.
   */
  private void addToInvalidates(Block b, boolean ackRequired) {
    StringBuilder sb = new StringBuilder();
    for (Iterator<DatanodeDescriptor> it =
      blocksMap.nodeIterator(b); it.hasNext();) {
      DatanodeDescriptor node = it.next();
      addToInvalidatesNoLog(b, node, ackRequired);
      sb.append(node.getName());
      sb.append(' ');
    }
    if (isInitialized && !isInSafeMode()) {
      // do not log in startup phase
      NameNode.stateChangeLog.info("BLOCK* NameSystem.addToInvalidates: "
              + b.getBlockName() + " is added to invalidSet of " + sb);
    }
  }
  
  /**
   * dumps the contents of recentInvalidateSets
   */
  void dumpExcessReplicasSets(PrintWriter out) {
    int size = excessReplicateMap.values().size();
    out.println("Metasave: Excess blocks " + excessBlocksCount
      + " waiting deletion from " + size + " datanodes.");
    if (size == 0) {
      return;
    }
    for (Map.Entry<String, LightWeightHashSet<Block>> entry : excessReplicateMap
        .entrySet()) {
      LightWeightHashSet<Block> blocks = entry.getValue();
      if (blocks.size() > 0) {
        out.println(datanodeMap.get(entry.getKey()).getName());
        blocks.printDetails(out);
      }
    }
  }

  /**
   * dumps the contents of recentInvalidateSets
   */
  private void dumpRecentInvalidateSets(PrintWriter out) {
    int size = recentInvalidateSets.values().size();
    out.println("Metasave: Blocks " + pendingDeletionBlocksCount
      + " waiting deletion from " + size + " datanodes.");
    if (size == 0) {
      return;
    }
    for (Map.Entry<String, LightWeightHashSet<Block>> entry : recentInvalidateSets
        .entrySet()) {
      LightWeightHashSet<Block> blocks = entry.getValue();
      if (blocks.size() > 0) {
        out.println(datanodeMap.get(entry.getKey()).getName() + blocks);
      }
    }
  }
  
  public void markBlockAsCorrupt(Block blk, DatanodeInfo dn) throws IOException {
    markBlockAsCorrupt(blk, dn, false);
  }

  /**
   * Mark the block belonging to datanode as corrupt
   *
   * @param blk Block to be marked as corrupt
   * @param dn  Datanode which holds the corrupt replica
   * @param parallelInitialBlockReport indicates that this call 
   *        is a result of parallel initial block report
   */
  public void markBlockAsCorrupt(Block blk, DatanodeInfo dn,
      final boolean parallelInitialBlockReport) throws IOException {
    if (!parallelInitialBlockReport) {
      // regular call, not through parallel block report
      writeLock(); 
    }
    lockParallelBRLock(parallelInitialBlockReport);
    
    try {
      DatanodeDescriptor node = getDatanode(dn);
      if (node == null) {
        throw new IOException("Cannot mark block" + blk.getBlockName() +
          " as corrupt because datanode " + dn.getName() +
          " does not exist. ");
      }

      final BlockInfo storedBlockInfo = blocksMap.getStoredBlock(blk);
      if (storedBlockInfo == null) {
        // Check if the replica is in the blockMap, if not
        // ignore the request for now. This could happen when BlockScanner
        // thread of Datanode reports bad block before Block reports are sent
        // by the Datanode on startup
        NameNode.stateChangeLog.info("BLOCK NameSystem.markBlockAsCorrupt: " +
          "block " + blk + " could not be marked " +
          "as corrupt as it does not exists in " +
          "blocksMap");
      } else {
        INodeFile inode = storedBlockInfo.getINode();
        if (inode == null) {
          NameNode.stateChangeLog.info("BLOCK NameSystem.markBlockAsCorrupt: " +
            "block " + blk + " could not be marked " +
            "as corrupt as it does not belong to " +
            "any file");
          addToInvalidates(storedBlockInfo, node, false);
          return;
        }
        // Add this replica to corruptReplicas Map
        if (!corruptReplicas.addToCorruptReplicasMap(storedBlockInfo, node)) {
          return;
        }
        NumberReplicas num = countNodes(storedBlockInfo);
        short blockReplication = inode.getBlockReplication(storedBlockInfo);
        if (num.liveReplicas() > blockReplication) {
          // the block is over-replicated so invalidate the replicas immediately
          invalidateBlock(storedBlockInfo, node, true);
        } else if (isPopulatingReplQueuesInternal()) {
          // add the block to neededReplication
          int numCurrentReplicas = num.liveReplicas() +
            pendingReplications.getNumReplicas(storedBlockInfo);
          updateNeededReplicationQueue(storedBlockInfo, -1, numCurrentReplicas,
              num.decommissionedReplicas, node, blockReplication);
        }
      }
    } finally {
      if (!parallelInitialBlockReport) {
        writeUnlock();
      }
      unlockParallelBRLock(parallelInitialBlockReport);
    }
  }

  /**
   * Invalidates the given block on the given datanode.
   */
  private void invalidateBlock(Block blk, DatanodeInfo dn, boolean ackRequired)
    throws IOException {
    NameNode.stateChangeLog.info("DIR* NameSystem.invalidateBlock: "
      + blk + " on "
      + dn.getName());
    DatanodeDescriptor node = getDatanode(dn);
    if (node == null) {
      throw new IOException("Cannot invalidate block " + blk +
        " because datanode " + dn.getName() +
        " does not exist.");
    }

    // Check how many copies we have of the block.  If we have at least one
    // copy on a live node, then we can delete it.
    int count = countNodes(blk).liveReplicas();
    if (count > 1) {
      // delete with ACK
      addToInvalidates(blk, dn, ackRequired);
      removeStoredBlock(blk, node);
      if (NameNode.stateChangeLog.isDebugEnabled()) {
        NameNode.stateChangeLog.debug("BLOCK* NameSystem.invalidateBlocks: "
          + blk + " on "
          + dn.getName() + " listed for deletion.");
      }
    } else {
      NameNode.stateChangeLog.info("BLOCK* NameSystem.invalidateBlocks: "
        + blk + " on "
        + dn.getName() + " is the only copy and was not deleted.");
    }
  }

  ////////////////////////////////////////////////////////////////
  // Here's how to handle block-copy failure during client write:
  // -- As usual, the client's write should result in a streaming
  // backup write to a k-machine sequence.
  // -- If one of the backup machines fails, no worries.  Fail silently.
  // -- Before client is allowed to close and finalize file, make sure
  // that the blocks are backed up.  Namenode may have to issue specific backup
  // commands to make up for earlier datanode failures.  Once all copies
  // are made, edit namespace and return to client.
  ////////////////////////////////////////////////////////////////

  /**
   * See {@link ClientProtocol#getHardLinkedFiles(String)}.
   */
  public String[] getHardLinkedFiles(String src) throws IOException {
    return dir.getHardLinkedFiles(src);
  }

  /** 
   * Create the hard link from src file to the dest file. 
   */ 
  public boolean hardLinkTo(String src, String dst) throws IOException {  
    INode dstNode = hardLinkToInternal(src, dst); 
    getEditLog().logSync(false);  
    if (dstNode != null && auditLog.isInfoEnabled()) {  
			logAuditEvent(getCurrentUGI(), Server.getRemoteIp(),
        "hardlink", src, dst, dstNode);
    } 
    return dstNode != null; 
  } 
    
  private INode hardLinkToInternal(String src, String dst) throws IOException {
    // Check the dst name 
    String[] dstNames = INode.getPathNames(dst);  
    if (!pathValidator.isValidName(dst, dstNames)) {  
      numInvalidFilePathOperations++; 
      throw new IOException("Invalid name: " + dst);  
    } 
      
    // Get the src and dst components 
    String[] srcNames = INode.getPathNames(src);  
    byte[][] srcComponents = INode.getPathComponents(srcNames); 
    byte[][] dstComponents = INode.getPathComponents(dstNames); 
      
    if (NameNode.stateChangeLog.isDebugEnabled()) { 
      NameNode.stateChangeLog.debug("DIR* NameSystem.hardLinkTo: src: " + src + " dst: " + dst);  
    } 
      
    writeLock();  
    try { 
      if (isInSafeMode()) { 
        throw new SafeModeException("Cannot hard link to src: " + src, safeMode); 
      } 
      INode[] srcInodes = new INode[srcComponents.length];  
      dir.rootDir.getExistingPathINodes(srcComponents, srcInodes);  
      INode[] dstInodes = new INode[dstComponents.length];  
      dir.rootDir.getExistingPathINodes(dstComponents, dstInodes);  
  
      // Check permission 
      if (isPermissionEnabled) {
        if (isPermissionCheckingEnabled(srcInodes)) {
          checkParentAccess(src, srcInodes, FsAction.EXECUTE);
        }
        if (isPermissionCheckingEnabled(dstInodes)) {
          checkParentAccess(dst, dstInodes, FsAction.WRITE_EXECUTE);
        }
      } 
        
      // Create the hard link 
      if (dir.hardLinkTo(src, srcNames, srcComponents, srcInodes,  
                         dst, dstNames, dstComponents, dstInodes)) {  
        return dstInodes[dstInodes.length-1]; 
      } 
      return null;  
    } finally { 
      writeUnlock();  
    } 
  }
  
  /**
   * Change the indicated filename.
   */
  public boolean renameTo(String src, String dst) throws IOException {
    INode dstNode = renameToInternal(src, dst);
    getEditLog().logSync(false);
    if (dstNode != null && auditLog.isInfoEnabled()) {
      logAuditEvent(getCurrentUGI(),
        Server.getRemoteIp(),
        "rename", src, dst, dstNode);
    }
    return dstNode != null;
  }

  private INode renameToInternal(String src, String dst
  ) throws IOException {
  
    String[] dstNames = INode.getPathNames(dst);
    if (!pathValidator.isValidName(dst, dstNames)) {
      numInvalidFilePathOperations++;
      throw new IOException("Invalid name: " + dst);
    }
    
    String[] srcNames = INode.getPathNames(src);
    byte[][] srcComponents = INode.getPathComponents(srcNames);
    byte[][] dstComponents = INode.getPathComponents(dstNames);
    
    if (NameNode.stateChangeLog.isDebugEnabled()) {
      NameNode.stateChangeLog.debug("DIR* NameSystem.renameTo: " + src + " to " + dst);
    }
    
    writeLock();
    try {
      if (isInSafeMode()) {
        throw new SafeModeException("Cannot rename " + src, safeMode);
      }
      INode[] srcInodes = new INode[srcComponents.length];
      dir.rootDir.getExistingPathINodes(srcComponents, srcInodes);
      INode[] dstInodes = new INode[dstComponents.length];
      dir.rootDir.getExistingPathINodes(dstComponents, dstInodes);
      INode dstNode = dstInodes[dstInodes.length-1];
      String actualDst = dst;
      byte[] lastDstComponent = dstComponents[dstComponents.length-1];
      if (isPermissionEnabled) {
        //We should not be doing this.  This is move() not renameTo().
        //but for now,
        if (isPermissionCheckingEnabled(srcInodes)) {
          checkParentAccess(src, srcInodes, FsAction.WRITE);
        }

        INode[] actualDstInodes = dstInodes;
        if (dstNode != null && dstNode.isDirectory()) {
          actualDst = dst + Path.SEPARATOR + srcNames[srcNames.length-1];
          actualDstInodes = Arrays.copyOf(dstInodes, dstInodes.length+1);
          lastDstComponent = srcComponents[srcComponents.length-1];
          actualDstInodes[actualDstInodes.length-1] = ((INodeDirectory)dstNode).
                       getChildINode(lastDstComponent);
        }
        if (isPermissionCheckingEnabled(actualDstInodes)) {
          checkAncestorAccess(actualDst, actualDstInodes, FsAction.WRITE);
        }
      }
      if (neverDeletePaths.contains(src)) {
        NameNode.stateChangeLog.warn("DIR* NameSystem.delete: " +
          " Trying to rename a whitelisted path " + src +
          " by user " + getCurrentUGI() +
          " from server " + Server.getRemoteIp());
        throw new IOException("Rename a whitelisted directory is not allowed " + src);
      }

      HdfsFileStatus dinfo = dstNode==null ? null : FSDirectory.getHdfsFileInfo(dstNode);
      if (dir.renameTo(src, srcNames[srcNames.length-1],
                       srcComponents[srcComponents.length-1], srcInodes,
                       dst, dstInodes, lastDstComponent)) {
        changeLease(src, dst, dinfo);     // update lease with new filename
        return dstInodes[dstInodes.length-1];
      }
      return null;
    } finally {
      writeUnlock();
    }
  }

  /**
   * Remove the indicated filename from namespace. If the filename
   * is a directory (non empty) and recursive is set to false then throw exception.
   */
  public boolean delete(String src, boolean recursive) throws IOException {
    boolean status = deleteInternal(src, null, recursive, true);
    getEditLog().logSync(false);
    if (status && auditLog.isInfoEnabled()) {
      logAuditEvent(getCurrentUGI(),
        Server.getRemoteIp(),
        "delete", src, null, null);
    }
    return status;
  }

  static void incrDeletedFileCount(FSNamesystem fsnamesystem, int count) {
    if (fsnamesystem != null && NameNode.getNameNodeMetrics() != null)
      NameNode.getNameNodeMetrics().numFilesDeleted.inc(count);
  }
    
  /**
   * Remove the indicated filename from the namespace.  This may
   * invalidate some blocks that make up the file.
   */
  boolean deleteInternal(String src, INode[] inodes, boolean recursive,
                         boolean enforcePermission) throws IOException {
    ArrayList<BlockInfo> collectedBlocks = new ArrayList<BlockInfo>();
    INode targetNode = null;

    byte[][] components = inodes == null ?
      INodeDirectory.getPathComponents(src) : null;

    writeLock();
    try {
      if (NameNode.stateChangeLog.isDebugEnabled()) {
        NameNode.stateChangeLog.debug("DIR* NameSystem.delete: " + src);
      }
      if (isInSafeMode()) {
        throw new SafeModeException("Cannot delete " + src, safeMode);
      }
      if (inodes == null) {
        inodes = new INode[components.length];
        dir.rootDir.getExistingPathINodes(components, inodes);
      }
      if (enforcePermission && isPermissionEnabled
          && isPermissionCheckingEnabled(inodes)) {
        checkPermission(src, inodes, false, null, FsAction.WRITE, null, FsAction.ALL);
      }
      if (neverDeletePaths.contains(src)) {
        NameNode.stateChangeLog.warn("DIR* NameSystem.delete: " +
          " Trying to delete a whitelisted path " + src +
          " by user " + getCurrentUGI() +
          " from server " + Server.getRemoteIp());
        throw new IOException("Deleting a whitelisted directory is not allowed. " + src);
      }

      if ((!recursive) && (!dir.isDirEmpty(inodes[inodes.length-1]))) {
        throw new IOException(src + " is non empty");
      }

      targetNode = dir.delete(src, inodes, collectedBlocks,
          BLOCK_DELETION_INCREMENT);
      if (targetNode == null) {
        return false;
      }
    } finally {
      writeUnlock();
    }
    List<INode> removedINodes = new ArrayList<INode>();
    while (targetNode.name != null) {
      // Interatively Remove blocks
      collectedBlocks.clear();
      // sleep to make sure that the lock can be grabbed by another thread
      try {
        Thread.sleep(1);
      } catch (InterruptedException e) {
        throw new InterruptedIOException(e.getMessage());
      }
      writeLock();
      try {
        int filesRemoved = targetNode.collectSubtreeBlocksAndClear(
            collectedBlocks, BLOCK_DELETION_INCREMENT, removedINodes);
        incrDeletedFileCount(this, filesRemoved);
        removeBlocks(collectedBlocks);
        // remove from inodeMap
        dir.removeFromInodeMap(removedINodes);
      } finally {
        writeUnlock();
      }   
    }
       
    collectedBlocks.clear();
    removedINodes.clear();
    
    return true;
  }
                   
  /**
   * From the given list, incrementally remove the blocks. Add the blocks
   * to invalidates, and set a flag that explicit ACK from DataNode is not
   * required. This function should be used only for deleting entire files.
   */
  private void removeBlocks(List<BlockInfo> blocks) {
    if (blocks == null) {
      return;
    }
    for (BlockInfo b : blocks) {
      removeFromExcessReplicateMap(b);
      neededReplications.remove(b, -1);
      corruptReplicas.removeFromCorruptReplicasMap(b);
      if (pendingReplications != null) {
        int replicas = pendingReplications.getNumReplicas(b);
        for (int i = 0; i < replicas; i++) {
          pendingReplications.remove(b);
        }    
      }    
      addToInvalidates(b, false);
      blocksMap.removeBlock(b);
    }
  }

  /**
   * Remove the blocks from the given list. Also, remove the path. Add the
   * blocks to invalidates, and set a flag that explicit ACK from DataNode is
   * not required. This function should be used only for deleting entire files.
   */
  void removePathAndBlocks(String src, List<BlockInfo> blocks) throws IOException {
    // No need for lock until we start accepting requests from clients.
    assert (!nameNode.isRpcServerRunning() || hasWriteLock());
    leaseManager.removeLeaseWithPrefixPath(src);
    removeBlocks(blocks);
  }

  /**
   * Get the file info for a specific file.
   *
   * @param src The string representation of the path to the file
   * @return object containing information regarding the file
   *         or null if file not found
   * @throws IOException if permission to access file is denied by the system
   */
  FileStatus getFileInfo(String src) throws IOException {
    src = dir.normalizePath(src);
    INode[] inodes = dir.getExistingPathINodes(src);

    if (isPermissionEnabled && 
        isPermissionCheckingEnabled(inodes)) {
      checkTraverse(src, inodes);
    }
    return dir.getFileInfo(src, inodes[inodes.length-1]);
  }

  public FileStatusExtended getFileInfoExtended(String src) throws IOException {
    Lease lease = leaseManager.getLeaseByPath(src);
    String leaseHolder = (lease == null) ? "" : lease.getHolder();
    return getFileInfoExtended(src, leaseHolder);
  }

  /**
   * Get the extended file info for a specific file.
   * 
   * @param src
   *          The string representation of the path to the file
   * @return object containing information regarding the file or null if file
   *         not found
   * @throws IOException
   *           if permission to access file is denied by the system
   */
  FileStatusExtended getFileInfoExtended(String src, String leaseHolder)
      throws IOException {
    src = dir.normalizePath(src);
    INode[] inodes = dir.getExistingPathINodes(src);
    return dir.getFileInfoExtended(src, inodes[inodes.length - 1], leaseHolder);
  }

  /**
   * Get the file info for a specific file.
   *
   * @param src The string representation of the path to the file
   * @return object containing information regarding the file
   *         or null if file not found
   * @throws IOException if permission to access file is denied by the system
   */
  HdfsFileStatus getHdfsFileInfo(String src) throws IOException {
    if (isPermissionEnabled) {
      INode[] inodes = dir.getExistingPathINodes(src);
      if (isPermissionCheckingEnabled(inodes)) {
        checkTraverse(src, inodes);
      }
    }
    return dir.getHdfsFileInfo(src);
  }

  /** Get the block info for a specific block id.
   * @param id The id of the block for which info is requested
   * @return object containing information regarding the block
   * or null if block not found
   */
  BlockInfo getBlockInfo(Block b) {
    return blocksMap.getBlockInfo(b);
  }

  /**
   * Fetch the list of files that have been open longer than a
   * specified amount of time.
   * @param prefix path prefix specifying subset of files to examine
   * @param millis select files that have been open longer that this
   * @param where to start searching, or null
   * @return array of OpenFileInfo objects
   * @throw IOException
   */
  public OpenFileInfo[] iterativeGetOpenFiles(
    String prefix, int millis, String start) {
    return leaseManager.iterativeGetOpenFiles(prefix, millis, start);
  }

  /**
   * Create all the necessary directories
   */
  public boolean mkdirs(String src, PermissionStatus permissions
  ) throws IOException {
    INode newNode = mkdirsInternal(src, permissions);
    getEditLog().logSync(false);
    if (newNode != null && auditLog.isInfoEnabled()) {
      logAuditEvent(getCurrentUGI(),
        Server.getRemoteIp(),
        "mkdirs", src, null, newNode);
    }
    return newNode != null;
  }

  /**
   * Create all the necessary directories
   */
  private INode mkdirsInternal(String src,
                                 PermissionStatus permissions)
    throws IOException {
    src = dir.normalizePath(src);
    
    // tokenize the src into components
    String[] names = INodeDirectory.getPathNames(src);
    
    if (!pathValidator.isValidName(src, names)) {
      numInvalidFilePathOperations++;
      throw new IOException("Invalid directory name: " + src);
    }

    // check validity of the username
    checkUserName(permissions);

    // convert the names into an array of bytes w/o holding lock
    byte[][] components = INodeDirectory.getPathComponents(names);
    INode[] inodes = new INode[components.length];

    writeLock();
    try {
      if (NameNode.stateChangeLog.isDebugEnabled()) {
        NameNode.stateChangeLog.debug("DIR* NameSystem.mkdirs: " + src);
      }
      dir.rootDir.getExistingPathINodes(components, inodes);
      if (isPermissionEnabled
          && isPermissionCheckingEnabled(inodes)) {
        checkTraverse(src, inodes);
      }
      INode lastINode = inodes[inodes.length-1];
      if (lastINode !=null && lastINode.isDirectory()) {
        // all the users of mkdirs() are used to expect 'true' even if
        // a new directory is not created.
        return lastINode;
      }
      if (isInSafeMode()) {
        throw new SafeModeException("Cannot create directory " + src, safeMode);
      }
      if (isPermissionEnabled
          && isPermissionCheckingEnabled(inodes)) {
        checkAncestorAccess(src, inodes, FsAction.WRITE);
      }

      // validate that we have enough inodes. This is, at best, a
      // heuristic because the mkdirs() operation migth need to
      // create multiple inodes.
      checkFsObjectLimit();

      if (!dir.mkdirs(src, names, components, inodes, inodes.length, permissions, false, now())) {
        throw new IOException("Invalid directory name: " + src);
      }
      return inodes[inodes.length-1];
    } finally {
      writeUnlock();
    }
  }

  ContentSummary getContentSummary(String src) throws IOException {
    INode[] inodes = dir.getExistingPathINodes(src);
    if (isPermissionEnabled
        && isPermissionCheckingEnabled(inodes)) {
      checkPermission(src, inodes, false, null, null, null, FsAction.READ_EXECUTE);
    }
    readLock();
    try {
      if (auditLog.isInfoEnabled()) {
        logAuditEvent(getCurrentUGI(),
          Server.getRemoteIp(),
          "getContentSummary", src, null, 
          getLastINode(inodes));
      }
      return dir.getContentSummary(src);
    } finally {
      readUnlock();
    }
  }

  /**
   * Set the namespace quota and diskspace quota for a directory.
   * See {@link ClientProtocol#setQuota(String, long, long)} for the
   * contract.
   */
  void setQuota(String path, long nsQuota, long dsQuota) throws IOException {
    writeLock();
    try {
      if (isInSafeMode()) {
        throw new SafeModeException("Cannot setQuota " + path, safeMode);
      }
      INode[] inodes = this.dir.getExistingPathINodes(path);
      if (isPermissionEnabled
          && isPermissionCheckingEnabled(inodes)) {
        checkSuperuserPrivilege();
      }

      dir.setQuota(path, nsQuota, dsQuota);
    } finally {
      writeUnlock();
    }
    getEditLog().logSync(false);
  }

  /**
   * Persist all metadata about this file.
   *
   * @param src        The string representation of the path
   * @param clientName The string representation of the client
   * @throws IOException if path does not exist
   */
  void fsync(String src, String clientName) throws IOException {

    NameNode.stateChangeLog.info("BLOCK* NameSystem.fsync: file "
      + src + " for " + clientName);
    writeLock();
    try {
      if (isInSafeMode()) {
        throw new SafeModeException("Cannot fsync file " + src, safeMode);
      }
      INodeFileUnderConstruction pendingFile = checkLease(src, clientName);

      // If the block has a length of zero, set it to size 1 so 
      // that lease recovery will not discard it.
      Block last = pendingFile.getLastBlock();
      if (last.getNumBytes() == 0) {
        last.setNumBytes(1);
      }
      dir.persistBlocks(src, pendingFile);
    } finally {
      writeUnlock();
    }
    getEditLog().logSync();
  }

  /**
   * This is invoked when a lease expires. On lease expiry, 
   * all the files that were written from that dfsclient should be
   * recovered.
   */
  void internalReleaseLease(Lease lease, String src,
      INodeFileUnderConstruction pendingFile) throws IOException {
    if (lease.hasPath()) {
      // make a copy of the paths because internalReleaseLeaseOne removes
      // pathnames from the lease record.
      String[] leasePaths = new String[lease.getPaths().size()];
      lease.getPaths().toArray(leasePaths);
      LOG.info("Recovering lease: " + lease + " for paths "
          + Arrays.toString(leasePaths));
      for (String p: leasePaths) {
        internalReleaseLeaseOne(lease, p);
      }
    } else {
      internalReleaseLeaseOne(lease, src, pendingFile, false);
    }
  }

  /**
   * Move a file that is being written to be immutable.
   *
   * @param src   The filename
   * @param lease The lease for the client creating the file
   */
  void internalReleaseLeaseOne(Lease lease, String src) throws IOException {
    internalReleaseLeaseOne(lease, src, false);
  }

  void internalReleaseLeaseOne(Lease lease, String src,
                                       boolean discardLastBlock) throws IOException {
    assert hasWriteLock();

    INodeFile iFile = dir.getFileINode(src);
    if (iFile == null) {
      final String message = "DIR* NameSystem.internalReleaseCreate: "
        + "attempt to release a create lock on "
        + src + " file does not exist.";
      NameNode.stateChangeLog.warn(message);
      throw new IOException(message);
    }
    if (!iFile.isUnderConstruction()) {
      final String message = "DIR* NameSystem.internalReleaseCreate: "
        + "attempt to release a create lock on "
        + src + " but file is already closed.";
      NameNode.stateChangeLog.warn(message);
      throw new IOException(message);
    }
    internalReleaseLeaseOne(lease, src, (INodeFileUnderConstruction)iFile, discardLastBlock);
  }

  /**
   * Move a file that is being written to be immutable.
   *
   * @param src   The filename
   * @param lease The lease for the client creating the file
   * @return true if lease recovery has completed, false if the client has to retry
   */
  boolean internalReleaseLeaseOne(Lease lease, String src,
        INodeFileUnderConstruction pendingFile, boolean discardLastBlock) throws IOException {
    // if it is safe to discard the last block, then do so
    if (discardLastBlock && discardDone(pendingFile, src)) {
      // Have client retry lease recovery.
      return false;
    }

    // Initialize lease recovery for pendingFile. If there are no blocks
    // associated with this file, then reap lease immediately. Otherwise
    // renew the lease and trigger lease recovery.
    if (pendingFile.getTargets() == null ||
      pendingFile.getTargets().length == 0) {
      if (pendingFile.getBlocks().length == 0) {
        finalizeINodeFileUnderConstruction(src, pendingFile);
        NameNode.stateChangeLog.warn("BLOCK*"
          + " internalReleaseLease: No blocks found, lease removed for " +  src);
        // Tell the client that lease recovery has succeeded.
        return true;
      }
      // setup the Inode.targets for the last block from the blocksMap
      //
      Block[] blocks = pendingFile.getBlocks();
      Block last = blocks[blocks.length - 1];
      DatanodeDescriptor[] targets =
        new DatanodeDescriptor[blocksMap.numNodes(last)];
      Iterator<DatanodeDescriptor> it = blocksMap.nodeIterator(last);
      for (int i = 0; it != null && it.hasNext(); i++) {
        targets[i] = it.next();
      }
      pendingFile.setTargets(targets, last.getGenerationStamp());
    }
    // start lease recovery of the last block for this file.
    pendingFile.assignPrimaryDatanode();
    Lease reassignedLease = reassignLease(
      lease, src, HdfsConstants.NN_RECOVERY_LEASEHOLDER, pendingFile
    );
    getEditLog().logOpenFile(src, pendingFile);
    leaseManager.renewLease(reassignedLease);
    return false;  // Have the client retry lease recovery.
  }

  // If we are part of recoverLease call and no fsync has
  // been called on the last block so far, then it is safe to drop
  // the last block completely.
  private boolean discardDone(INodeFileUnderConstruction pendingFile,
                              String src) throws IOException {
    Block[] blocks = pendingFile.getBlocks();
    if (blocks == null || blocks.length == 0) {
      return false;
    }
    Block last = blocks[blocks.length-1];
    if (last.getNumBytes() == 0) {
      dir.removeBlock(src, pendingFile, last);
      finalizeINodeFileUnderConstruction(src, pendingFile);
      NameNode.stateChangeLog.warn("BLOCK*" +
          " internalReleaseLease: discarded last block " + last +
          " , lease removed for " +  src);
      return true;
    }
    return false;
  }

  Lease reassignLease(Lease lease, String src, String newHolder,
                      INodeFileUnderConstruction pendingFile) {
    if(newHolder == null)
      return lease;
    pendingFile.setClientName(newHolder);
    return leaseManager.reassignLease(lease, src, newHolder);
  }
  
  private void finalizeINodeFileUnderConstruction(String src,
      INodeFileUnderConstruction pendingFile) throws IOException {
    // Put last block in needed replication queue if uder replicated
    replicateLastBlock(src, pendingFile);

    finalizeINodeFileUnderConstruction(src, null, pendingFile);
  }

  private void finalizeINodeFileUnderConstruction(String src,
      INode[] inodes, INodeFileUnderConstruction pendingFile) throws IOException {
    leaseManager.removeLease(pendingFile.getClientName(), src);

    DatanodeDescriptor[] descriptors = pendingFile.getTargets();

    if (descriptors != null) {
      for (DatanodeDescriptor node : descriptors) {
        node.removeINode(pendingFile);
      }
    }

    // The file is no longer pending.
    // Create permanent INode, update blockmap
    INodeFile newFile = pendingFile.convertToInodeFile(isAccessTimeSupported());
    dir.replaceNode(src, inodes, pendingFile, newFile, true);

    // close file and persist block allocations for this file
    dir.closeFile(src, newFile);
  }

  /**
   * corrupts a file by:
   * 1. removing all targets of the last block
   */
  void corruptFileForTesting(String src) throws IOException {
    INodeFile inode = dir.getFileINode(src);

    if (inode.isUnderConstruction()) {
      INodeFileUnderConstruction pendingFile =
        (INodeFileUnderConstruction) inode;
      BlockInfo[] blocks = pendingFile.getBlocks();
      if (blocks != null && blocks.length >= 1) {
        BlockInfo lastBlockInfo = blocks[blocks.length - 1];

        pendingFile.setLastBlock(
          lastBlockInfo,
          new DatanodeDescriptor[0]
        );
      }
    }
  }

  // public only for testing
  public void commitBlockSynchronization(Block lastblock,
                                  long newgenerationstamp, long newlength,
                                  boolean closeFile, boolean deleteblock, DatanodeID[] newtargets
  ) throws IOException {
    LOG.info("commitBlockSynchronization(lastblock=" + lastblock
      + ", newgenerationstamp=" + newgenerationstamp
      + ", newlength=" + newlength
      + ", newtargets=" + Arrays.asList(newtargets)
      + ", closeFile=" + closeFile
      + ", deleteBlock=" + deleteblock
      + ")");
    String src = null;
    writeLock();
    try {
      if (isInSafeMode()) {
        throw new SafeModeException("Cannot commitBlockSynchronization "
          + lastblock, safeMode);
      }      
      Block blockWithWildcardGenstamp = new Block(lastblock.getBlockId());
      final BlockInfo oldblockinfo = blocksMap.getStoredBlock(blockWithWildcardGenstamp);
      if (oldblockinfo == null) {
        throw new IOException("Block (=" + lastblock + ") not found");
      }
      if (!deleteblock
          && lastblock.getGenerationStamp() != oldblockinfo
              .getGenerationStamp()
          && oldblockinfo.getGenerationStamp() >= newgenerationstamp) {
        throw new IOException("Try to update block " + oldblockinfo
            + " to generation stamp " + newgenerationstamp);
      }
      INodeFile iFile = oldblockinfo.getINode();
      if (!iFile.isUnderConstruction()) {
        throw new IOException("Unexpected block (=" + lastblock
          + ") since the file (=" + iFile.getLocalName()
          + ") is not under construction");
      }
      INodeFileUnderConstruction pendingFile = (INodeFileUnderConstruction) iFile;
      // Fail the request if the block being synchronized is not the last block
      // of the file.
      pendingFile.checkLastBlockId(lastblock.getBlockId());

      // Remove old block from blocks map. This always have to be done
      // because the generation stamp of this block is changing.
      blocksMap.removeBlock(oldblockinfo);

      if (deleteblock) {
        pendingFile.removeBlock(lastblock);
      } else {
        // update last block, construct newblockinfo and add it to the blocks map
        lastblock.set(lastblock.getBlockId(), newlength, newgenerationstamp);
        final BlockInfo newblockinfo = 
            blocksMap.addINode(lastblock, pendingFile, 
                               pendingFile.getReplication());

        // find the DatanodeDescriptor objects
        // There should be no locations in the blocksMap till now because the
        // file is underConstruction
        DatanodeDescriptor[] descriptors = null;
        List<DatanodeDescriptor> descriptorsList =
          new ArrayList<DatanodeDescriptor>(newtargets.length);
        for(int i = 0; i < newtargets.length; i++) {
          // We don't use getDatanode here since that method can
          // throw. If we were to throw an exception during this commit
          // process, we'd fall out of sync since DNs have already finalized
          // the block with the new GS.
          DatanodeDescriptor node =
            datanodeMap.get(newtargets[i].getStorageID());
          if (node != null) {
            if (closeFile) {
              // If we aren't closing the file, we shouldn't add it to the
              // block list for the node, since the block is still under
              // construction there. (in getAdditionalBlock, for example
              // we don't add to the block map for the targets)
              node.addBlock(newblockinfo);
            }
            descriptorsList.add(node);
          } else {
          LOG.error("commitBlockSynchronization included a target DN " +
            newtargets[i] + " which is not known to NN. Ignoring.");
          }
        }
        if (!descriptorsList.isEmpty()) {
          descriptors = descriptorsList.toArray(new DatanodeDescriptor[0]);
        }
        // add locations into the INodeUnderConstruction
        pendingFile.setLastBlock(newblockinfo, descriptors);
      }

      // Get the file's full name; make sure that the file is valid
      src = pendingFile.getFullPathName();
      
      // If this commit does not want to close the file, persist
      // blocks only if append is supported and return
      if (!closeFile) {
        // persist the blocks if we support appends, 
        // or the block was deleted from the file and we need to persist it
        if (supportAppends || persistBlocks) {
          dir.persistBlocks(src, pendingFile);
        }
      } else {
        // remove lease, close file
        // this will persist the blocks
        finalizeINodeFileUnderConstruction(src, pendingFile);
      }
    } finally {// end of synchronized section
      writeUnlock();
    }

    if (closeFile || supportAppends) {
      getEditLog().logSync();
    }
    LOG.info("commitBlockSynchronization(newblock=" + lastblock
      + ", file=" + src + ") successful");
  }
  
  private INodeFileUnderConstruction checkUCBlock(Block block, String clientName)
      throws IOException {
    // check safe mode
    if (isInSafeMode()) {
      throw new SafeModeException("Cannot get nextGenStamp for " + block, 
                                  safeMode);
    }
    
    // check stored block state
    Block blockWithWildcardGenstamp = new Block(block.getBlockId());
    BlockInfo storedBlock = blocksMap.getStoredBlock(blockWithWildcardGenstamp);
    if (storedBlock == null) {
      String msg = block + " is already commited, storedBlock == null.";
      LOG.info(msg);
      throw new BlockAlreadyCommittedException(msg);
    }
    
    // check file node
    INodeFile fileINode = storedBlock.getINode();
    if (!fileINode.isUnderConstruction()) {
      String msg = block + " is already commited, !fileINode.isUnderConstruction().";
      LOG.info(msg);
      throw new BlockAlreadyCommittedException(msg);
    }
    
    // check lease
    INodeFileUnderConstruction pendingFile = 
                    (INodeFileUnderConstruction) fileINode;
    
    if (clientName == null || !clientName.equals(pendingFile.getClientName())) {
      String msg = "Lease mismatch: " + block + 
          " is accessed by a non lease holder " + clientName;
      LOG.info(msg);
      throw new LeaseExpiredException(msg);
    }
    return pendingFile;
  }

  /**
   * Update a pipeline for a block under construction
   * 
   * @param clientName  the name of the client
   * @param oldBlock    an old block
   * @param newBlock    a new block with a new generation stamp and length
   * @param newNodes    if any error occurs
   * @throws IOException 
   */
  void updatePipeline(String clientName, Block oldBlock, 
      Block newBlock, List<DatanodeID> newNodes) throws IOException {
    LOG.info("updatePipeline(block=" + oldBlock
        + ", newGenerationStamp=" + newBlock.getGenerationStamp()
        + ", newLength=" + newBlock.getNumBytes()
        + ", newNodes=" + newNodes
        + ")");

    writeLock();
    try {
      // check the vadility of the block and lease holder name
      final INodeFileUnderConstruction pendingFile = 
          checkUCBlock(oldBlock, clientName);
      final Block oldBlockInfo = pendingFile.getLastBlock();

      // check new GS & length: this is not expected
      if (newBlock.getGenerationStamp() == oldBlockInfo.getGenerationStamp()) {
        // we will do nothing if the GS is the same, to make this method 
        // idempotent, this should come from the avatar failover retry.
        String msg = "Update " + oldBlock + " (len = " + 
            oldBlockInfo.getNumBytes() + ") to a same generation stamp: " 
            + newBlock + " (len = " + newBlock.getNumBytes() + ")";
        LOG.warn(msg);
        return;
      }
      
      if (newBlock.getGenerationStamp() < oldBlockInfo.getGenerationStamp() ||
          newBlock.getNumBytes() < oldBlockInfo.getNumBytes()) {
        String msg = "Update " + oldBlock + " (len = " + 
            oldBlockInfo.getNumBytes() + ") to an older state: " + newBlock +
            " (len = " + newBlock.getNumBytes() + ")";
        LOG.warn(msg);
        throw new IOException(msg);
      }

      // Remove old block from blocks map. This alawys have to be done 
      // because the generation stamp of this block is changing.
      blocksMap.removeBlock(oldBlockInfo);

      // update Last block, add it to the blocks map
      BlockInfo newBlockInfo = 
          blocksMap.addINode(newBlock, pendingFile, pendingFile.getReplication());

      // find the Datanode Descriptor objects
      DatanodeDescriptor[] descriptors = null;
      if (!newNodes.isEmpty()) {
        descriptors = new DatanodeDescriptor[newNodes.size()];
        for (int i = 0; i < newNodes.size(); i++) {
          descriptors[i] = getDatanode(newNodes.get(i));
        }
      }

      // add locations into the the INodeUnderConstruction
      pendingFile.setLastBlock(newBlockInfo, descriptors);
      // persist blocks only if append is supported
      String src = leaseManager.findPath(pendingFile);
      if (supportAppends) {
        dir.persistBlocks(src, pendingFile);
      }
    } finally {
      writeUnlock();
    }
    
    if (supportAppends) {
      getEditLog().logSync();
    }
    
    
    LOG.info("updatePipeline(" + oldBlock + ") successfully to " + newBlock);
  }

  /**
   * Renew the lease(s) held by the given client
   */
  void renewLease(String holder) throws IOException {
    readLock();
    try {
      if (isInSafeMode()) {
        throw new SafeModeException("Cannot renew lease for " + holder, safeMode);
      }

      leaseManager.renewLease(holder);
    } finally {
      readUnlock();
    }
  }

  private void getListingCheck(String src) throws IOException {
    getListingCheck(src, dir.getExistingPathINodes(src));
  }

  private void getListingCheck(String src, INode[] inodes) throws IOException {
    if (isPermissionEnabled
        && isPermissionCheckingEnabled(inodes)) {
      if (FSDirectory.isDir(inodes[inodes.length-1])) {
        checkPathAccess(src, inodes, FsAction.READ_EXECUTE);
      } else {
        checkTraverse(src, inodes);
      }
    }
  }

  /**
   * Get a listing of all files at 'src'.  The Object[] array
   * exists so we can return file attributes (soon to be implemented)
   */
  public FileStatus[] getListing(String src) throws IOException {
    getListingCheck(src);
    FileStatus[] stats =  dir.getListing(src);
    if (auditLog.isInfoEnabled()) {
      logAuditEvent(getCurrentUGI(),
        Server.getRemoteIp(),
        "listStatus", src, null, null);
    }
    return stats;
  }

  /**
   * Get a listing of all files at 'src'.  The Object[] array
   * exists so we can return file attributes (soon to be implemented)
   */
  public HdfsFileStatus[] getHdfsListing(String src) throws IOException {
    getListingCheck(src);
    HdfsFileStatus[] stats = dir.getHdfsListing(src);
    if (auditLog.isInfoEnabled()) {
      logAuditEvent(getCurrentUGI(),
        Server.getRemoteIp(),
        "listStatus", src, null, null);
    }
    return stats;
  }

  /**
   * Get a partial listing of the indicated directory
   *
   * @param src          the directory name
   * @param startAfter   the name to start after
   * @param needLocation if blockLocations need to be returned
   * @return a partial listing starting after startAfter
   */
  public DirectoryListing getPartialListing(String src, byte[] startAfter,
                                            boolean needLocation)
    throws IOException {
    DirectoryListing stats;
    src = dir.normalizePath(src);
    byte[][] names = INode.getPathComponents(src);
    INode[] inodes = new INode[names.length];
    readLock();
    try {
      dir.rootDir.getExistingPathINodes(names, inodes);
      getListingCheck(src, inodes);
      stats = dir.getPartialListing(src, inodes[inodes.length-1],
          startAfter, needLocation);
    } finally {
      readUnlock();
    }
    if (auditLog.isInfoEnabled()) {
      logAuditEvent(getCurrentUGI(),
        Server.getRemoteIp(),
        "listStatus", src, null, null);
    }
    return stats;
  }

  /////////////////////////////////////////////////////////
  //
  // These methods are called by datanodes
  //
  /////////////////////////////////////////////////////////

  /**
   * Register Datanode.
   * <p/>
   * The purpose of registration is to identify whether the new datanode
   * serves a new data storage, and will report new data block copies,
   * which the namenode was not aware of; or the datanode is a replacement
   * node for the data storage that was previously served by a different
   * or the same (in terms of host:port) datanode.
   * The data storages are distinguished by their storageIDs. When a new
   * data storage is reported the namenode issues a new unique storageID.
   * <p/>
   * Finally, the namenode returns its namespaceID as the registrationID
   * for the datanodes.
   * namespaceID is a persistent attribute of the name space.
   * The registrationID is checked every time the datanode is communicating
   * with the namenode.
   * Datanodes with inappropriate registrationID are rejected.
   * If the namenode stops, and then restarts it can restore its
   * namespaceID and will continue serving the datanodes that has previously
   * registered with the namenode without restarting the whole cluster.
   *
   * @see org.apache.hadoop.hdfs.server.datanode.DataNode#register()
   */
  public void registerDatanode(DatanodeRegistration nodeReg
  ) throws IOException {
    writeLock();
    try {
      String dnAddress = Server.getRemoteAddress();
      if (dnAddress == null) {
        // Mostly called inside an RPC.
        // But if not, use address passed by the data-node.
        dnAddress = nodeReg.getHost();
      }

      // check if the datanode is allowed to be connect to the namenode
      if (!verifyNodeRegistration(nodeReg, dnAddress)) {
        throw new DisallowedDatanodeException(nodeReg);
      }

      String hostName = nodeReg.getHost();

      // update the datanode's name with ip:port
      DatanodeID dnReg = new DatanodeID(dnAddress + ":" + nodeReg.getPort(),
        nodeReg.getStorageID(),
        nodeReg.getInfoPort(),
        nodeReg.getIpcPort());
      nodeReg.updateRegInfo(dnReg);

      NameNode.stateChangeLog.info(
        "BLOCK* NameSystem.registerDatanode: "
          + "node registration from " + nodeReg.getName()
          + " storage " + nodeReg.getStorageID());

      DatanodeDescriptor nodeS = datanodeMap.get(nodeReg.getStorageID());
      DatanodeDescriptor nodeN = host2DataNodeMap.getDatanodeByName(nodeReg
          .getHost() + ":" + nodeReg.getPort());

      if (nodeN != null && nodeN != nodeS) {
        NameNode.LOG.info("BLOCK* NameSystem.registerDatanode: "
          + "node from name: " + nodeN.getName());
        // nodeN previously served a different data storage,
        // which is not served by anybody anymore.
        removeDatanode(nodeN);
        // physically remove node from datanodeMap
        wipeDatanode(nodeN);
        nodeN = null;
      }

      if (nodeS != null) {
        if (nodeN == nodeS) {
          // The same datanode has been just restarted to serve the same data
          // storage. We do not need to remove old data blocks, the delta will
          // be calculated on the next block report from the datanode
          if (NameNode.stateChangeLog.isDebugEnabled()) {
            NameNode.stateChangeLog.debug("BLOCK* NameSystem.registerDatanode: "
              + "node restarted.");
          }
        } else {
          // nodeS is found
          /* The registering datanode is a replacement node for the existing
             data storage, which from now on will be served by a new node.
             If this message repeats, both nodes might have same storageID
             by (insanely rare) random chance. User needs to restart one of the
             nodes with its data cleared (or user can just remove the StorageID
             value in "VERSION" file under the data directory of the datanode,
             but this is might not work if VERSION file format has changed
          */
          NameNode.stateChangeLog.info("BLOCK* NameSystem.registerDatanode: "
            + "node " + nodeS.getName()
            + " is replaced by " + nodeReg.getName() +
            " with the same storageID " +
            nodeReg.getStorageID());
        }
        // update cluster map
        clusterMap.remove(nodeS);
        nodeS.updateRegInfo(nodeReg);
        nodeS.setHostName(hostName);
        nodeS.setDisallowed(false); // Node is in the include list

        // resolve network location
        resolveNetworkLocation(nodeS);
        clusterMap.add(nodeS);

        // also treat the registration message as a heartbeat
        synchronized (heartbeats) {
          if (!heartbeats.contains(nodeS)) {
            heartbeats.add(nodeS);
            //update its timestamp
            nodeS.updateHeartbeat(0L, 0L, 0L, 0L, 0);
            nodeS.isAlive = true;
          }
        }
        checkDecommissioning(nodeS, dnAddress);
        return;
      }

      // this is a new datanode serving a new data storage
      if (nodeReg.getStorageID().equals("")) {
        // this data storage has never been registered
        // it is either empty or was created by pre-storageID version of DFS
        nodeReg.storageID = newStorageID();
        if (NameNode.stateChangeLog.isDebugEnabled()) {
          NameNode.stateChangeLog.debug(
            "BLOCK* NameSystem.registerDatanode: "
              + "new storageID " + nodeReg.getStorageID() + " assigned.");
        }
      }
      // register new datanode
      DatanodeDescriptor nodeDescr
        = new DatanodeDescriptor(nodeReg, NetworkTopology.DEFAULT_RACK, hostName);
      resolveNetworkLocation(nodeDescr);
      unprotectedAddDatanode(nodeDescr);
      clusterMap.add(nodeDescr);
      checkDecommissioning(nodeDescr, dnAddress);

      // also treat the registration message as a heartbeat
      synchronized (heartbeats) {
        heartbeats.add(nodeDescr);
        nodeDescr.isAlive = true;
        // no need to update its timestamp
        // because its is done when the descriptor is created
      }
      return;
    } finally {
      writeUnlock();
    }
  }

  /* Resolve a node's network location */

  void resolveNetworkLocation(DatanodeInfo node) {
    List<String> names = new ArrayList<String>(1);
    if (dnsToSwitchMapping instanceof CachedDNSToSwitchMapping) {
      // get the node's IP address
      names.add(node.getHost());
    } else if (dnsToSwitchMapping instanceof StaticMapping) {
      names.add(toHostPort(node));
    } else {
      // get the node's host name
      String hostName = node.getHostName();
      int colon = hostName.indexOf(":");
      hostName = (colon == -1) ? hostName : hostName.substring(0, colon);
      names.add(hostName);
    }

    // resolve its network location
    List<String> rName = dnsToSwitchMapping.resolve(names);
    String networkLocation;
    if (rName == null) {
      LOG.error("The resolve call returned null! Using " +
        NetworkTopology.DEFAULT_RACK + " for host " + names);
      networkLocation = NetworkTopology.DEFAULT_RACK;
    } else {
      networkLocation = rName.get(0);
    }
    node.setNetworkLocation(networkLocation);
  }

  /**
   * Get registrationID for datanodes based on the namespaceID.
   *
   * @return registration ID
   * @see #registerDatanode(DatanodeRegistration)
   * @see FSImage#newNamespaceID()
   */
  public String getRegistrationID() {
    return Storage.getRegistrationID(dir.fsImage.storage);
  }

  /**
   * Generate new storage ID.
   *
   * @return unique storage ID
   *         <p/>
   *         Note: that collisions are still possible if somebody will try
   *         to bring in a data storage from a different cluster.
   */
  private String newStorageID() {
    String newID = null;
    while (newID == null) {
      newID = "DS" + Integer.toString(r.nextInt());
      if (datanodeMap.get(newID) != null) {
        newID = null;
      }
    }
    return newID;
  }

  boolean isDatanodeDead(DatanodeDescriptor node) {
    return (node.getLastUpdate() <
      (now() - heartbeatExpireInterval));
  }

  public void setDatanodeDead(DatanodeDescriptor node) throws IOException {
    node.setLastUpdate(0);
  }

  /**
   * The given node has reported in.  This method should:
   * 1) Record the heartbeat, so the datanode isn't timed out
   * 2) Adjust usage stats for future block allocation
   * <p/>
   * If a substantial amount of time passed since the last datanode
   * heartbeat then request an immediate block report.
   *
   * @return an array of datanode commands
   * @throws IOException
   */
  DatanodeCommand[] handleHeartbeat(DatanodeRegistration nodeReg,
                                    long capacity, long dfsUsed, long remaining, long namespaceUsed,
                                    int xceiverCount, int xmitsInProgress)
    throws IOException {
    DatanodeCommand cmd = null;
    synchronized (heartbeats) {
      synchronized (datanodeMap) {
        DatanodeDescriptor nodeinfo = null;
        try {
          nodeinfo = getDatanode(nodeReg);
        } catch (UnregisteredDatanodeException e) {
          return new DatanodeCommand[]{DatanodeCommand.REGISTER};
        }

        // Check if this datanode should actually be shutdown instead.
        if (nodeinfo != null && nodeinfo.isDisallowed()) {
          setDatanodeDead(nodeinfo);
          throw new DisallowedDatanodeException(nodeinfo);
        }

        if (nodeinfo == null || !nodeinfo.isAlive) {
          return new DatanodeCommand[]{DatanodeCommand.REGISTER};
        }

        updateStats(nodeinfo, false);
        nodeinfo.updateHeartbeat(capacity, dfsUsed, remaining, namespaceUsed, xceiverCount);
        updateStats(nodeinfo, true);

        //check lease recovery
        cmd = nodeinfo.getLeaseRecoveryCommand(Integer.MAX_VALUE);
        if (cmd != null) {
          return new DatanodeCommand[]{cmd};
        }

        ArrayList<DatanodeCommand> cmds = new ArrayList<DatanodeCommand>(2);
        //check pending replication
        cmd = nodeinfo.getReplicationCommand(maxReplicationStreams -
                                             xmitsInProgress);
        if (cmd != null) {
          cmds.add(cmd);
        }
        //check block invalidation
        cmd = nodeinfo.getInvalidateBlocks(ReplicationConfigKeys.blockInvalidateLimit);
        if (cmd != null) {
          cmds.add(cmd);
        }
        // check raid tasks
        cmd = nodeinfo.getRaidCommand(ReplicationConfigKeys.raidEncodingTaskLimit, 
                                      ReplicationConfigKeys.raidDecodingTaskLimit);
        if (cmd != null) {
          cmds.add(cmd);
        }
        
        if (!cmds.isEmpty()) {
          return cmds.toArray(new DatanodeCommand[cmds.size()]);
        }
      }
    }

    //check distributed upgrade
    cmd = getDistributedUpgradeCommand();
    if (cmd != null) {
      return new DatanodeCommand[]{cmd};
    }
    return null;
  }

  void handleKeepAlive(DatanodeRegistration nodeReg) throws IOException {
    synchronized (heartbeats) {
      synchronized (datanodeMap) {
        DatanodeDescriptor nodeinfo = null;
        try {
          nodeinfo = getDatanode(nodeReg);
        } catch (UnregisteredDatanodeException e) {
          return;
        }
        if (nodeinfo == null || !nodeinfo.isAlive) {
          return;
        }
        nodeinfo.updateLastHeard();
      }
    }
  }

  private void updateStats(DatanodeDescriptor node, boolean isAdded) {
    //
    // The statistics are protected by the heartbeat lock
    // For decommissioning/decommissioned nodes, only used capacity
    // is counted.
    //
    assert (Thread.holdsLock(heartbeats));
    if (isAdded) {
      capacityUsed += node.getDfsUsed();
      capacityNamespaceUsed += node.getNamespaceUsed();
      totalLoad += node.getXceiverCount();
      if (!(node.isDecommissionInProgress() || node.isDecommissioned())) {
        capacityTotal += node.getCapacity();
        capacityRemaining += node.getRemaining();
      } else {
        capacityTotal += node.getDfsUsed();
      }
    } else {
      capacityUsed -= node.getDfsUsed();
      capacityNamespaceUsed -= node.getNamespaceUsed();
      totalLoad -= node.getXceiverCount();
      if (!(node.isDecommissionInProgress() || node.isDecommissioned())) {
        capacityTotal -= node.getCapacity();
        capacityRemaining -= node.getRemaining();
      } else {
        capacityTotal -= node.getDfsUsed();
      }
    }
  }

  /**
   * Periodically calls heartbeatCheck() and updateSuspectStatus().
   */
  class HeartbeatMonitor implements Runnable {
    private long reevaluateSuspectFailNodesIntervalMs;
    private long suspectFailUponHeartBeatMissTimeoutMs;
    private int maxSuspectNodesAllowed;

    public HeartbeatMonitor(Configuration conf) {
      suspectFailUponHeartBeatMissTimeoutMs = conf.getLong(
          "dfs.heartbeat.timeout.millis", 120000); // 2 min, default
      reevaluateSuspectFailNodesIntervalMs = 
          suspectFailUponHeartBeatMissTimeoutMs / 2; // 1 min, default
      maxSuspectNodesAllowed = conf.getInt( "dfs.heartbeat.timeout.suspect-fail.max", 
          25); // more than 1 rack. but less than 2.
    }

    /**
     */
    public void run() {
      long now = now();
      long nextHeartBeatCheckTime = now + heartbeatRecheckInterval;
      long nextUpdateSuspectTime = now + reevaluateSuspectFailNodesIntervalMs;
      long sleepTime;
      
      while (fsRunning) {
        try {
          // 5 mins too slow for suspect?
          sleepTime = Math.min(nextUpdateSuspectTime, nextHeartBeatCheckTime) - now;
          Thread.sleep(sleepTime);
          // For short heartbeatRecheckInterval, FSNamesystem object might not
          // have been initialized yet.
          if (getNameNode().namesystem == null) {
            LOG.warn("FSNamesystem not initialized yet!");
            continue;
          }
        } catch (InterruptedException ie) {
        }
        
        now = now();
       
        if (now > nextUpdateSuspectTime) {
          nextUpdateSuspectTime = now + reevaluateSuspectFailNodesIntervalMs;
          updateSuspectStatus(now);
        }
        
        if (now > nextHeartBeatCheckTime) {
          try {
            InjectionHandler.processEvent(InjectionEvent.FSNAMESYSTEM_STOP_MONITOR, hbthread);
            nextHeartBeatCheckTime = now + heartbeatRecheckInterval;
            heartbeatCheck();
          } catch (Exception e) {
            FSNamesystem.LOG.error("Error in heartbeatCheck: ", e);
            myFSMetrics.numHeartBeatMonitorExceptions.inc();
          }
        }
      }
    }

    boolean isDatanodeSuspectFail(long now, DatanodeDescriptor node) {
      return (node.getLastUpdate() <
        (now - suspectFailUponHeartBeatMissTimeoutMs));
    }
    
    void updateSuspectStatus(long now) {
      int numSuspectNodes = 0;
      synchronized (heartbeats) {
        for (Iterator<DatanodeDescriptor> it = heartbeats.iterator();
             it.hasNext();) {
          DatanodeDescriptor nodeInfo = it.next();
          if (isDatanodeSuspectFail(now, nodeInfo)) {
            nodeInfo.setSuspectFail(true);
            if (++numSuspectNodes > maxSuspectNodesAllowed) {
              break;
            }
          } else {
            nodeInfo.setSuspectFail(false);
          }
        }
        
        if (numSuspectNodes <= maxSuspectNodesAllowed) {
          DatanodeInfo.enableSuspectNodes();
        } else { // Too many suspects. Disable failure detection
          DatanodeInfo.disableSuspectNodes();
        }
      }
    }
  }

  /**
   * Periodically calls computeDatanodeWork().
   */
  class UnderReplicationMonitor implements Runnable {
    
    public void run() {
      while (fsRunning) {
        try {
          InjectionHandler.processEvent(InjectionEvent.FSNAMESYSTEM_STOP_MONITOR, underreplthread);
          if (blockReplicationEnabled) {
            computeDatanodeWork();
            processPendingReplications();
          }
          Thread.sleep(ReplicationConfigKeys.replicationRecheckInterval);
        } catch (InterruptedException ie) {
          LOG.warn("UnderReplicationMonitor thread received InterruptedException.", ie);
          break;
        } catch (Exception ie) {
          LOG.warn("UnderReplicationMonitor thread received exception. ", ie);
          myFSMetrics.numUnderReplicationMonitorExceptions.inc();
        } catch (Throwable t) {
          LOG.warn("UnderReplicationMonitor thread received Runtime exception. ", t);
          myFSMetrics.numUnderReplicationMonitorExceptions.inc();
          Runtime.getRuntime().exit(-1);
        }
      }
    }
  }
  
  class OverReplicatedBlock extends Block {
    public DatanodeDescriptor addedNode;
    public DatanodeDescriptor delNodeHint;
    OverReplicatedBlock(Block b, DatanodeDescriptor newAddedNode,
        DatanodeDescriptor newDelNodeHint) {
      super(b);
      this.addedNode = newAddedNode;
      this.delNodeHint = newDelNodeHint;
    }
  }

  /**
   * Periodically calls processOverReplicatedBlocksAsync().
   */
  class OverReplicationMonitor implements Runnable {
    
    public void run() {
      while (fsRunning) {
        try {
          InjectionHandler.processEvent(InjectionEvent.FSNAMESYSTEM_STOP_MONITOR, overreplthread);
          processOverReplicatedBlocksAsync();
          configManager.reloadConfigIfNecessary();
          Thread.sleep(ReplicationConfigKeys.replicationRecheckInterval);
        } catch (InterruptedException ie) {
          LOG.warn("OverReplicationMonitor thread received InterruptedException.", ie);
          break;
        } catch (Exception e) {
          LOG.warn("OverReplicationMonitor thread received exception. ", e);
          myFSMetrics.numOverReplicationMonitorExceptions.inc();
        } catch (Throwable t) {
          LOG.warn("OverReplicationMonitor thread received Runtime exception. ", t);
          myFSMetrics.numOverReplicationMonitorExceptions.inc();
          Runtime.getRuntime().exit(-1);
        }
      }
    }
  }
 
  /**
   * Periodically calls processRaidEncodingTaskAsync() 
   */
  class RaidEncodingTaskMonitor implements Runnable {
    public void run() {
      while (fsRunning) {
        try {
          InjectionHandler.processEvent(InjectionEvent.FSNAMESYSTEM_STOP_MONITOR,
              raidEncodingTaskThread);
          processRaidEncodingTaskAsync();
          configManager.reloadConfigIfNecessary();
          Thread.sleep(ReplicationConfigKeys.replicationRecheckInterval);
        } catch (InterruptedException ie) {
          LOG.warn("RaidEncodingTaskMonitor thread received InterruptedException.",
              ie);
          break;
        } catch (Exception e) {
          LOG.warn("RaidEncodingTaskMonitor thread received exception. ", e);
          myFSMetrics.numRaidEncodingTaskMonitorExceptions.inc();
        } catch (Throwable t) {
          LOG.warn("RaidEncodingTaskMonitor thread received Runtime exception. ", t);
          myFSMetrics.numRaidEncodingTaskMonitorExceptions.inc();
          Runtime.getRuntime().exit(-1);
        }
      }
    }
  }
  
  /**
   * delay OverReplicationMonitor until the given time
   * @param until
   */
  void delayOverreplicationProcessing(long until) {
    delayOverreplicationMonitorTime = until;
  }

  /////////////////////////////////////////////////////////
  //
  // These methods are called by the Namenode system, to see
  // if there is any work for registered datanodes.
  //
  /////////////////////////////////////////////////////////

  /**
   * Compute block replication and block invalidation work
   * that can be scheduled on data-nodes.
   * The datanode will be informed of this work at the next heartbeat.
   *
   * @return number of blocks scheduled for replication or removal.
   */
  public int computeDatanodeWork() throws IOException {
    int workFound = 0;
    int blocksToProcess = 0;
    int nodesToProcess = 0;
    // blocks should not be replicated or removed if safe mode is on
    if (isInSafeMode()) {
      updateReplicationCounts(workFound);
      return workFound;
    }
    synchronized (heartbeats) {
      blocksToProcess = (int) (heartbeats.size()
        * ReplicationConfigKeys.replicationWorkMultiplier);
      nodesToProcess = (int) Math.ceil((double) heartbeats.size()
        * ReplicationConfigKeys.INVALIDATE_WORK_PCT_PER_ITERATION / 100);
    }

    workFound = computeReplicationWork(blocksToProcess);

    // Update FSNamesystemMetrics counters
    updateReplicationCounts(workFound);

    workFound += computeInvalidateWork(nodesToProcess);
    return workFound;
  }
  
  private void updateReplicationCounts(int workFound) {
    writeLock();
    try {
      pendingReplicationBlocksCount = pendingReplications.size();
      underReplicatedBlocksCount = neededReplications.size();
      scheduledReplicationBlocksCount = workFound;
      corruptReplicaBlocksCount = corruptReplicas.size();
    } finally {
      writeUnlock();
    }
  }

  /**
   * Schedule blocks for deletion at datanodes
   *
   * @param nodesToProcess number of datanodes to schedule deletion work
   * @return total number of block for deletion
   */
  int computeInvalidateWork(int nodesToProcess) {
    int numOfNodes = 0;
    ArrayList<String> keyArray = null;

    readLock();
    try {
      numOfNodes = recentInvalidateSets.size();
      // get an array of the keys
      keyArray = new ArrayList<String>(recentInvalidateSets.keySet());
    } finally {
      readUnlock();
    }

    nodesToProcess = Math.min(numOfNodes, nodesToProcess);

    // randomly pick up <i>nodesToProcess</i> nodes
    // and put them at [0, nodesToProcess)
    int remainingNodes = numOfNodes - nodesToProcess;
    if (nodesToProcess < remainingNodes) {
      for (int i = 0; i < nodesToProcess; i++) {
        int keyIndex = r.nextInt(numOfNodes - i) + i;
        Collections.swap(keyArray, keyIndex, i); // swap to front
      }
    } else {
      for (int i = 0; i < remainingNodes; i++) {
        int keyIndex = r.nextInt(numOfNodes - i);
        Collections.swap(keyArray, keyIndex, numOfNodes - i - 1); // swap to end
      }
    }

    int blockCnt = 0;
    for (int nodeCnt = 0; nodeCnt < nodesToProcess; nodeCnt++) {
      blockCnt += invalidateWorkForOneNode(keyArray.get(nodeCnt));
    }
    return blockCnt;
  }

  /**
   * Scan blocks in {@link #neededReplications} and assign replication
   * work to data-nodes they belong to.
   * <p/>
   * The number of process blocks equals either twice the number of live
   * data-nodes or the number of under-replicated blocks whichever is less.
   *
   * @return number of blocks scheduled for replication during this iteration.
   */
  private int computeReplicationWork(
    int blocksToProcess) throws IOException {
    // stall only useful for unit tests (see TestFileAppend4.java)
    if (stallReplicationWork)  {
      return 0;
    }

    // Choose the blocks to be replicated
    List<List<BlockInfo>> blocksToReplicate =
      chooseUnderReplicatedBlocks(blocksToProcess);

    // replicate blocks
    return computeReplicationWorkForBlocks(blocksToReplicate);
  }

  /** Decide the number of blocks to replicate on for this priority.
   * The heuristic is that allocate at most 20% the quota for 
   * lower priority blocks
   * 
   * @param totalQuota max number of blocks can be replicated
   * @param blocksForThisPriority
   *                   number of needed replication blocks for this priority
   * @param blocksForLowerPriorities
   *                   number of lower priority needed replication blocks
   * @return the number blocks
   */
  private int getQuotaForThisPriority(int totalQuota, int blocksForThisPriority,
      int blocksForLowerPriorities) {
    // reserve at most 20% for lower priority blocks
    int quotaForLowerPriorities = 
                     Math.min(totalQuota/5, blocksForLowerPriorities);
    return Math.min( blocksForThisPriority,
                     totalQuota-quotaForLowerPriorities);
  }
  
  /**
   * Get a list of block lists to be replicated
   * The index of block lists represents the
   *
   * @param blocksToProcess
   * @return Return a list of block lists to be replicated.
   *         The block list index represents its replication priority.
   */
  List<List<BlockInfo>> chooseUnderReplicatedBlocks(int blocksToProcess) {
    // initialize data structure for the return value
    List<List<BlockInfo>> blocksToReplicate =
      new ArrayList<List<BlockInfo>>(UnderReplicatedBlocks.LEVEL);
    for (int i = 0; i < UnderReplicatedBlocks.LEVEL; i++) {
      blocksToReplicate.add(new ArrayList<BlockInfo>());
    }

    writeLock();
    try {
      synchronized (neededReplications) {
        if (neededReplications.size() == 0) {
          return blocksToReplicate;
        }

        for (int priority = 0; priority<UnderReplicatedBlocks.LEVEL; priority++) {
        // Go through all blocks that need replications of priority
        BlockIterator neededReplicationsIterator = neededReplications.iterator(priority);
        int numBlocks = neededReplications.size(priority);
        if (replIndex[priority] > numBlocks) {
          replIndex[priority] = 0;
        }
        // skip to the first unprocessed block, which is at replIndex
        for (int i = 0; i < replIndex[priority] && neededReplicationsIterator.hasNext(); i++) {
          neededReplicationsIterator.next();
        }
        // # of blocks to process for this priority
        int blocksToProcessIter = getQuotaForThisPriority(blocksToProcess,
            numBlocks, neededReplications.getSize(priority+1));
        blocksToProcess -= blocksToProcessIter;

        for (int blkCnt = 0; blkCnt < blocksToProcessIter; blkCnt++, replIndex[priority]++) {
          if (!neededReplicationsIterator.hasNext()) {
            // start from the beginning
            replIndex[priority] = 0;
            neededReplicationsIterator = neededReplications.iterator(priority);
            assert neededReplicationsIterator.hasNext() :
              "neededReplications should not be empty.";
          }

          BlockInfo block = neededReplicationsIterator.next();
          blocksToReplicate.get(priority).add(block);
        } // end for
        }
      } // end try
      return blocksToReplicate;
    } finally {
      writeUnlock();
    }
  }

  /**
   * Replicate a set of blocks
   *
   * @param blocksToReplicate blocks to be replicated, for each priority
   * @return the number of blocks scheduled for replication
   */
  int computeReplicationWorkForBlocks(List<List<BlockInfo>> blocksToReplicate) {
    int requiredReplication, numEffectiveReplicas, priority;
    List<DatanodeDescriptor> containingNodes;
    DatanodeDescriptor srcNode;
    INodeFile fileINode = null;

    int scheduledWork = 0;
    List<ReplicationWork> work = new LinkedList<ReplicationWork>();

    writeLock();
    try {
      synchronized (neededReplications) {
        for (priority = 0; priority < blocksToReplicate.size(); priority++) {
          for (BlockInfo block : blocksToReplicate.get(priority)) {
            // block should belong to a file
            BlockInfo storedBlock = blocksMap.getBlockInfo(block);
            fileINode = (storedBlock == null) ? null : storedBlock.getINode();
            // abandoned block not belong to a file
            if (fileINode == null) {
              neededReplications.remove(block, priority); // remove from neededReplications
              replIndex[priority]--;
              continue;
            }
            requiredReplication = fileINode.getBlockReplication(storedBlock);

            // get a source data-node
            containingNodes = new ArrayList<DatanodeDescriptor>();
            NumberReplicas numReplicas = new NumberReplicas();
            srcNode = chooseSourceDatanode(block, containingNodes, numReplicas);
            if (srcNode == null) // block can not be replicated from any node
            {
              continue;
            }

            // do not schedule more if enough replicas is already pending
            numEffectiveReplicas = numReplicas.liveReplicas() +
              pendingReplications.getNumReplicas(block);
            if (numEffectiveReplicas >= requiredReplication) {
              neededReplications.remove(block, priority); // remove from neededReplications
              replIndex[priority]--;
              continue;
            }
            work.add(new ReplicationWork(block, fileINode, requiredReplication
                - numEffectiveReplicas, srcNode, containingNodes, priority));
          }
        }
      }
    } finally {
      writeUnlock();
    }

    // choose replication targets: NOT HODING THE GLOBAL LOCK
    for(ReplicationWork rw : work){
      DatanodeDescriptor targets[] = chooseTarget(rw);
      rw.targets = targets;
    }

    writeLock();
    try {
      for(ReplicationWork rw : work){
        DatanodeDescriptor[] targets = rw.targets;
        if(targets == null || targets.length == 0){
          rw.targets = null;
          continue;
        }
        if (targets.length == 1 && 
            !clusterMap.isOnSameRack(rw.srcNode, targets[0])) {
          // in case src & target are not on the same rack
          // see if there is an alternate valid source in the same rack 
          // as the target that we can use instead.
          for (DatanodeDescriptor node : rw.containingNodes) {
            if (node != rw.srcNode && 
                clusterMap.isOnSameRack(node, targets[0]) &&
                isGoodReplica(node, rw.block)) {
              rw.srcNode = node;
              break;
            }
          }
        }
        synchronized (neededReplications) {
          BlockInfo block = rw.block;
          priority = rw.priority;
          // Recheck since global lock was released
          // block should belong to a file
          BlockInfo storedBlock = blocksMap.getBlockInfo(block);
          fileINode = (storedBlock == null) ? null : storedBlock.getINode();
          // abandoned block not belong to a file
          if (fileINode == null ) {
            neededReplications.remove(block, priority); // remove from neededReplications
            rw.targets = null;
            replIndex[priority]--;
            continue;
          }
          requiredReplication = fileINode.getBlockReplication(storedBlock);

          // do not schedule more if enough replicas is already pending
          NumberReplicas numReplicas = countNodes(block);
          numEffectiveReplicas = numReplicas.liveReplicas() +
            pendingReplications.getNumReplicas(block);
          if (numEffectiveReplicas >= requiredReplication) {
            neededReplications.remove(block, priority); // remove from neededReplications
            replIndex[priority]--;
            rw.targets = null;
            continue;
          }

          // Add block to the to be replicated list
          rw.srcNode.addBlockToBeReplicated(block, targets);
          
          scheduledWork++;

          for (DatanodeDescriptor dn : targets) {
            dn.incBlocksScheduled();
          }

          // Move the block-replication into a "pending" state.
          // The reason we use 'pending' is so we can retry
          // replications that fail after an appropriate amount of time.
          pendingReplications.add(block, targets.length);
          if (NameNode.stateChangeLog.isDebugEnabled()) {
            NameNode.stateChangeLog.debug(
              "BLOCK* block " + block
                + " is moved from neededReplications to pendingReplications");
          }

          // remove from neededReplications
          if (numEffectiveReplicas + targets.length >= requiredReplication) {
            neededReplications.remove(block, priority); // remove from neededReplications
            replIndex[priority]--;
          }
        }
      }
    } finally {
      writeUnlock();
    }

    // update metrics
    updateReplicationMetrics(work);
    
    // print debug information
    if(NameNode.stateChangeLog.isInfoEnabled()){
      // log which blocks have been scheduled for replication
      for(ReplicationWork rw : work){
        // report scheduled blocks
        DatanodeDescriptor[] targets = rw.targets;
        if (targets != null && targets.length != 0) {
          StringBuffer targetList = new StringBuffer("datanode(s)");
          for (int k = 0; k < targets.length; k++) {
            targetList.append(' ');
            targetList.append(targets[k].getName());
          }
          NameNode.stateChangeLog.info(
            "BLOCK* ask "
              + rw.srcNode.getName() + " to replicate "
              + rw.block + " to " + targetList);
        }
      }
    }

    // log once per call
    if (NameNode.stateChangeLog.isDebugEnabled()) {
      NameNode.stateChangeLog.debug("BLOCK* neededReplications = "
          + neededReplications.size() + " pendingReplications = "
          + pendingReplications.size());
    }
    return scheduledWork;
  }

  /**
   * Update replication metrics
   * 
   * @param work record for all scheduled replication
   */
  private void updateReplicationMetrics(List<ReplicationWork> work) {
    for(ReplicationWork rw : work){
      DatanodeDescriptor[] targets = rw.targets;
      if (targets == null) continue;
      for (DatanodeDescriptor target : targets) {
        if (clusterMap.isOnSameRack(rw.srcNode, target)) {
          myFSMetrics.numLocalRackReplications.inc();
        } else {
          myFSMetrics.numAcrossRackReplications.inc();
        }
      }
    }
  }
  
  /**
   * Get a full path name of a node
   * @param inode
   * @return its full path name; null if invalid
   */
  static String getFullPathName(FSInodeInfo inode) {
    try {
      return inode.getFullPathName();
    } catch (IOException ioe) {
      return null;
    }
  }
  
  /**
   * Wrapper function for choosing targets for replication.
   */
  private DatanodeDescriptor[] chooseTarget(ReplicationWork work) {
    if (!neededReplications.contains(work.block)) {
      return null;
    }
    if (work.blockSize == BlockFlags.NO_ACK) {
      LOG.warn("Block " + work.block.getBlockId() + 
          " of the file " + getFullPathName(work.fileINode) + 
          " is invalidated and cannot be replicated.");
      return null;
    }
    if (work.blockSize == BlockFlags.DELETED) {
      LOG.warn("Block " + work.block.getBlockId() + 
          " of the file " + getFullPathName(work.fileINode) + 
          " is a deleted block and cannot be replicated.");
      return null;
    }
    return replicator.chooseTarget(work.fileINode,
        work.numOfReplicas, work.srcNode,
        work.containingNodes, null, work.blockSize);
  }

  /**
   * Parse the data-nodes the block belongs to and choose one,
   * which will be the replication source.
   * <p/>
   * We prefer nodes that are in DECOMMISSION_INPROGRESS state to other nodes
   * since the former do not have write traffic and hence are less busy.
   * We do not use already decommissioned nodes as a source.
   * Otherwise we choose a random node among those that did not reach their
   * replication limit.
   * <p/>
   * In addition form a list of all nodes containing the block
   * and calculate its replication numbers.
   */
  private DatanodeDescriptor chooseSourceDatanode(
    Block block,
    List<DatanodeDescriptor> containingNodes,
    NumberReplicas numReplicas) {
    containingNodes.clear();
    DatanodeDescriptor srcNode = null;
    int live = 0;
    int decommissioned = 0;
    int corrupt = 0;
    int excess = 0;
    Iterator<DatanodeDescriptor> it = blocksMap.nodeIterator(block);
    Collection<DatanodeDescriptor> nodesCorrupt = corruptReplicas.getNodes(block);
    DatanodeDescriptor excessReplica = null;
    while (it.hasNext()) {
      DatanodeDescriptor node = it.next();
      Collection<Block> excessBlocks =
        excessReplicateMap.get(node.getStorageID());
      if ((nodesCorrupt != null) && (nodesCorrupt.contains(node))) {
        corrupt++;
      } else if (node.isDecommissionInProgress() || node.isDecommissioned()) {
        decommissioned++;
      } else if (excessBlocks != null && excessBlocks.contains(block)) {
        excess++;
      } else {
        live++;
      }
      containingNodes.add(node);
      // Check if this replica is corrupt
      // If so, do not select the node as src node
      if ((nodesCorrupt != null) && nodesCorrupt.contains(node)) {
        continue;
      }
      if (node.getNumberOfBlocksToBeReplicated() >= maxReplicationStreams) {
        continue;
      } // already reached replication limit
      // the block must not be scheduled for removal on srcNode
      if (excessBlocks != null && excessBlocks.contains(block)) {
        excessReplica = node;
        continue;
      }
      // never use already decommissioned nodes
      if (node.isDecommissioned()) {
        continue;
      }
      // we prefer nodes that are in DECOMMISSION_INPROGRESS state
      if (node.isDecommissionInProgress() || srcNode == null) {
        srcNode = node;
        continue;
      }
      if (srcNode.isDecommissionInProgress()) {
        continue;
      }
      // switch to a different node randomly
      // this to prevent from deterministically selecting the same node even
      // if the node failed to replicate the block on previous iterations
      if (r.nextBoolean()) {
        srcNode = node;
      }
    }
    if (numReplicas != null) {
      numReplicas.initialize(live, decommissioned, corrupt, excess);
    }
    if (srcNode == null && live == 0 && excessReplica != null) {
      // try excessReplica if there is no other choice
      srcNode = excessReplica;
    }
    return srcNode;
  }

  /**
   * Decide if a replica is valid
   * @param node datanode the block is located
   * @param block a block
   * @return true if a replica is valid
   */
  private boolean isGoodReplica(DatanodeDescriptor node, Block block) {
    Collection<Block> excessBlocks =
      excessReplicateMap.get(node.getStorageID());
    Collection<DatanodeDescriptor> nodesCorrupt = corruptReplicas.getNodes(block);
    return (nodesCorrupt == null || !nodesCorrupt.contains(node)) // not corrupt
        // not over scheduling for replication
        && (node.getNumberOfBlocksToBeReplicated() < maxReplicationStreams)
        // not alredy scheduled for removal
        && (excessBlocks == null || !excessBlocks.contains(block))
        && !node.isDecommissioned(); // not decommissioned
  }
  
  /**
   * Get blocks to invalidate for <i>nodeId</i>
   * in {@link #recentInvalidateSets}.
   *
   * @return number of blocks scheduled for removal during this iteration.
   */
  private int invalidateWorkForOneNode(String nodeId) {
    List<Block> blocksToInvalidate = null;
    DatanodeDescriptor dn = null;
    writeLock();
    try {
      // blocks should not be replicated or removed if safe mode is on
      if (isInSafeMode()) {
        return 0;
      }
      // get blocks to invalidate for the nodeId
      assert nodeId != null;
      dn = datanodeMap.get(nodeId);
      if (dn == null) {
        recentInvalidateSets.remove(nodeId);
        return 0;
      }

      LightWeightHashSet<Block> invalidateSet = recentInvalidateSets
          .get(nodeId);
      if (invalidateSet == null) {
        return 0;
      }
      // # blocks that can be sent in one message is limited
      blocksToInvalidate = invalidateSet.pollN(ReplicationConfigKeys.blockInvalidateLimit);

      // If we send everything in this message, remove this node entry
      if (invalidateSet.isEmpty()) {
        recentInvalidateSets.remove(nodeId);
      }

      dn.addBlocksToBeInvalidated(blocksToInvalidate);
      pendingDeletionBlocksCount -= blocksToInvalidate.size();
    } finally {
      writeUnlock();
      if (NameNode.stateChangeLog.isInfoEnabled() && blocksToInvalidate != null
          && blocksToInvalidate.size() != 0) {
        StringBuffer blockList = new StringBuffer();
        for (Block blk : blocksToInvalidate) {
          blockList.append(' ');
          blockList.append(blk);
        }
        NameNode.stateChangeLog.info("BLOCK* ask "
          + dn.getName() + " to delete " + blockList);
      }
    }
    return blocksToInvalidate.size();
  }

  public void setNodeReplicationLimit(int limit) {
    this.maxReplicationStreams = limit;
  }

  /**
   * If there were any replication requests that timed out, reap them
   * and put them back into the neededReplication queue
   */
  void processPendingReplications() {
    BlockInfo[] timedOutItems = pendingReplications.getTimedOutBlocks();
    if (timedOutItems != null) {
      writeLock();
      try {
        for (int i = 0; i < timedOutItems.length; i++) {
          NumberReplicas num = countNodes(timedOutItems[i]);
          neededReplications.add(
            timedOutItems[i], 
            num.liveReplicas(),
            num.decommissionedReplicas(),
            getReplication(timedOutItems[i]));
        }
      } finally {
        writeUnlock();
      }
      /* If we know the target datanodes where the replication timedout,
       * we could invoke decBlocksScheduled() on it. Its ok for now.
       */
    }
  }

  /**
   * remove a datanode descriptor
   *
   * @param nodeID datanode ID
   */
  public void removeDatanode(DatanodeID nodeID)
    throws IOException {
    writeLock();
    try {
      DatanodeDescriptor nodeInfo = getDatanode(nodeID);
      if (nodeInfo != null) {
        removeDatanode(nodeInfo);
      } else {
        NameNode.stateChangeLog.warn("BLOCK* NameSystem.removeDatanode: "
          + nodeID.getName() + " does not exist");
      }
    } finally {
      writeUnlock();
    }
  }
  
  /**
   * Clear replication queues. This is used by standby avatar to reclaim memory.
   */
  void clearReplicationQueues() {
    writeLock();
    try {
      synchronized (neededReplications) {
        neededReplications.clear();
      }
      underReplicatedBlocksCount = 0;

      corruptReplicas.clear();
      corruptReplicaBlocksCount = 0;

      overReplicatedBlocks.clear();
      raidEncodingTasks.clear();

      excessReplicateMap = new HashMap<String, LightWeightHashSet<Block>>();
      excessBlocksCount = 0;
    } finally {
      writeUnlock();
    }
  }

  /**
   * remove a datanode descriptor
   *
   * @param nodeInfo datanode descriptor
   */
  private void removeDatanode(DatanodeDescriptor nodeInfo) {
    synchronized (heartbeats) {
      if (nodeInfo.isAlive) {
        updateStats(nodeInfo, false);
        heartbeats.remove(nodeInfo);
        nodeInfo.isAlive = false;
      }
    }

    for (Iterator<BlockInfo> it = nodeInfo.getBlockIterator(); it.hasNext();) {
      removeStoredBlock(it.next(), nodeInfo);
    }
    unprotectedRemoveDatanode(nodeInfo);
    clusterMap.remove(nodeInfo);

    // remove this datanode from any INodeFileUnderConstruction so
    // concurrent readers don't get down datanodes
    // (we need a copy of the Set since removeTargets may modify it)
    Set<INodeFileUnderConstruction> iNodeFileSet =
      new HashSet<INodeFileUnderConstruction>(nodeInfo.getOpenINodes());
    for (INodeFileUnderConstruction iNodeFile : iNodeFileSet) {
      iNodeFile.removeTarget(nodeInfo);
    }
  }

  void unprotectedRemoveDatanode(DatanodeDescriptor nodeDescr) {
    nodeDescr.resetBlocks();
    removeFromInvalidates(nodeDescr.getStorageID());
    if (NameNode.stateChangeLog.isDebugEnabled()) {
      NameNode.stateChangeLog.debug(
        "BLOCK* NameSystem.unprotectedRemoveDatanode: "
          + nodeDescr.getName() + " is out of service now.");
    }
  }

  void unprotectedAddDatanode(DatanodeDescriptor nodeDescr) {
    /* To keep host2DataNodeMap consistent with datanodeMap,
       remove  from host2DataNodeMap the datanodeDescriptor removed
       from datanodeMap before adding nodeDescr to host2DataNodeMap.
    */
    host2DataNodeMap.remove(
      datanodeMap.put(nodeDescr.getStorageID(), nodeDescr));
    host2DataNodeMap.add(nodeDescr);

    if (NameNode.stateChangeLog.isDebugEnabled()) {
      NameNode.stateChangeLog.debug(
        "BLOCK* NameSystem.unprotectedAddDatanode: "
          + "node " + nodeDescr.getName() + " is added to datanodeMap.");
    }
  }

  /**
   * Physically remove node from datanodeMap.
   *
   * @param nodeID node
   */
  void wipeDatanode(DatanodeID nodeID) throws IOException {
    String key = nodeID.getStorageID();
    host2DataNodeMap.remove(datanodeMap.remove(key));
    if (NameNode.stateChangeLog.isDebugEnabled()) {
      NameNode.stateChangeLog.debug(
        "BLOCK* NameSystem.wipeDatanode: "
          + nodeID.getName() + " storage " + key
          + " is removed from datanodeMap.");
    }
  }

  public FSImage getFSImage() {
    return dir.fsImage;
  }

  public FSEditLog getEditLog() {
    return getFSImage().getEditLog();
  }

  /**
   * Check if there are any expired heartbeats, and if so,
   * whether any blocks have to be re-replicated.
   * While removing dead datanodes, make sure that only one datanode is marked
   * dead at a time within the synchronized section. Otherwise, a cascading
   * effect causes more datanodes to be declared dead.
   */
  void heartbeatCheck() {
    if (!getNameNode().shouldCheckHeartbeat()) {
      // not to check dead nodes.
      return;
    }
    boolean allAlive = false;
    while (!allAlive) {
      boolean foundDead = false;
      DatanodeID nodeID = null;

      // locate the first dead node.
      synchronized (heartbeats) {
        for (Iterator<DatanodeDescriptor> it = heartbeats.iterator();
             it.hasNext();) {
          DatanodeDescriptor nodeInfo = it.next();
          if (isDatanodeDead(nodeInfo)) {
            foundDead = true;
            nodeID = nodeInfo;
            break;
          }
        }
      }

      // acquire the fsnamesystem lock, and then remove the dead node.
      if (foundDead) {
        writeLock();
        try {
          synchronized (heartbeats) {
            synchronized (datanodeMap) {
              DatanodeDescriptor nodeInfo = null;
              try {
                nodeInfo = getDatanode(nodeID);
              } catch (IOException e) {
                nodeInfo = null;
              }
              if (nodeInfo != null && isDatanodeDead(nodeInfo)) {
                NameNode.stateChangeLog.info("BLOCK* NameSystem.heartbeatCheck: "
                  + "lost heartbeat from " + nodeInfo.getName());
                removeDatanode(nodeInfo);
                nodeInfo.setStartTime(now());
              }
            }
          }
        } finally {
          writeUnlock();
        }
      }
      allAlive = !foundDead;
    }
  }
  
  public boolean processBlocksBeingWrittenReport(DatanodeID nodeID, 
      BlockListAsLongs blocksBeingWritten) 
  throws IOException {
    // check if we can discard the report
    if (safeMode != null && !safeMode.shouldProcessRBWReports()) {
      return false;
    }
    writeLock();
    try {
      DatanodeDescriptor dataNode = getDatanode(nodeID);
      if (dataNode == null) {
        throw new IOException("ProcessReport from unregisterted node: "
            + nodeID.getName());
      }
      
      Block block = new Block();

      for (int i = 0; i < blocksBeingWritten.getNumberOfBlocks(); i ++) {
        block.set(blocksBeingWritten.getBlockId(i), 
            blocksBeingWritten.getBlockLen(i), 
            blocksBeingWritten.getBlockGenStamp(i));

        BlockInfo storedBlock = blocksMap.getStoredBlockWithoutMatchingGS(block);

        if (storedBlock == null) {
          rejectAddStoredBlock(
              new Block(block), dataNode,
              "Block not in blockMap with any generation stamp",
              true, false);
          continue;
        }

        INodeFile inode = storedBlock.getINode();
        if (inode == null) {
          rejectAddStoredBlock(
              new Block(block), dataNode,
              "Block does not correspond to any file",
              true, false);
          continue;
        }

        boolean underConstruction = inode.isUnderConstruction();
        boolean isLastBlock = inode.getLastBlock() != null &&
                   inode.getLastBlock().getBlockId() == block.getBlockId();

        // Must be the last block of a file under construction,
        if (!underConstruction) {
          rejectAddStoredBlock(
              new Block(block), dataNode,
              "Reported as block being written but is a block of closed file.",
              true, false);
          continue;
        }

        if (!isLastBlock) {
          rejectAddStoredBlock(
              new Block(block), dataNode,
              "Reported as block being written but not the last block of " +
              "an under-construction file.",
              true, false);
          continue;
        }

        INodeFileUnderConstruction pendingFile = 
                            (INodeFileUnderConstruction)inode;
        boolean added = pendingFile.addTarget(dataNode, block.getGenerationStamp());
        if (added) {
          // Increment only once for each datanode.
          DatanodeDescriptor[] validDNs = pendingFile.getValidTargets();
          if (validDNs != null) {
            incrementSafeBlockCount(validDNs.length, true);            
          }
        }
      }
    } finally {
      writeUnlock();
      checkSafeMode();
    }
    return true;
  }
  
  // lock for initial in-safemode block reports
  private final ReentrantLock parallelBRLock = new ReentrantLock();
  
  void lockParallelBRLock(boolean parallelInitialBlockReport) {
    if (parallelInitialBlockReport) {
      parallelBRLock.lock();
    }
  }

  void unlockParallelBRLock(boolean parallelInitialBlockReport) {
    if (parallelInitialBlockReport) {
      parallelBRLock.unlock();
    }
  }
  
  /**
   * The given node is reporting all its blocks.  Use this info to
   * update the (machine-->blocklist) and (block-->machinelist) tables.
   */
  public Collection<Block> processReport(DatanodeID nodeID,
      BlockListAsLongs newReport
  ) throws IOException {
    // To minimize startup time, we discard any second (or later) block reports
    // that we receive while still in startup phase.
    // Checking this without holding fsnamesystem so that duplicate 
    // block reports won't wait for too long time thus getting into old gen
    if (isInStartupSafeMode()) {
      synchronized (datanodeMap) {
        DatanodeDescriptor node = getDatanode(nodeID);
        if (node != null && node.receivedFirstFullBlockReport()) {
          NameNode.stateChangeLog.info("BLOCK* NameSystem.processReport: "
              + "discarded non-initial block report from " + nodeID.getName()
              + " because namenode still in startup phase");
          return new LinkedList<Block>();
        }
      }
    }

    int processTime;
    Collection<Block> toAdd = null, toRemove = null, toInvalidate = null, toRetry = null;
    boolean firstReport = false;
    writeLock();
    try {
      long startTime = now();
      if (NameNode.stateChangeLog.isDebugEnabled()) {
         NameNode.stateChangeLog.debug("BLOCK* NameSystem.processReport: "
          + "from " + nodeID.getName() + " " +
          newReport.getNumberOfBlocks() + " blocks");
      }
      DatanodeDescriptor node = getDatanode(nodeID);
      if (node == null || !node.isAlive) {
        throw new IOException("ProcessReport from dead or unregistered node: "
          + nodeID.getName());
      }
      
      if (this.getNameNode().shouldRetryAbsentBlocks()) {
        toRetry = new LinkedList<Block>();
      }

      // check the case when the NN does not know of any replicas on this
      // datanode. This typically happens when the NN restarts and the first
      // report from this datanode is being processed. Short-circuit the
      // processing in this case: just add all these replicas to this
      // datanode. This helps NN restart times tremendously.
      firstReport = !node.receivedFirstFullBlockReport();
      if (firstReport) {   
        if (!isPopulatingReplQueuesInternal()
            && newReport.getNumberOfBlocks() > parallelBRblocksPerShard
            && parallelBRenabled) {
          // first report, with no replication queues initialized
          InitialReportWorker.processReport(this, toRetry, newReport, node,
              initialBlockReportExecutor);
        } else {
          // first report, with replication queues initialized
          // or queues not initialized but the report has too few blocks to
          // be processed in parallel
          Block iblk = new Block(); // a fixed new'ed block to be reused with index i
          for (int i = 0; i < newReport.getNumberOfBlocks(); ++i) {
            newReport.getBlock(iblk, i);
            if (!addStoredBlockInternal(iblk, node, null, true, null)
                && this.getNameNode().shouldRetryAbsentBlock(iblk, null)) {
              toRetry.add(new Block(iblk));
            }
          }
        }
        node.setReceivedFirstFullBlockReport();
      } else {

        //
        // Modify the (block-->datanode) map, according to the difference
        // between the old and new block report.
        //
        toAdd = new LinkedList<Block>();
        toRemove = new LinkedList<Block>();
        toInvalidate = new LinkedList<Block>();
        node.reportDiff(blocksMap, newReport, toAdd, toRemove, toInvalidate,
            toRetry, this);

        for (Block b : toRemove) {
          removeStoredBlock(b, node);
        }
        for (Block b : toAdd) {
          addStoredBlock(b, node, null);
        }
        for (Block b : toInvalidate) {
          addToInvalidatesNoLog(b, node, false);
        }
      }
      processTime = (int)(now() - startTime);
      NameNode.getNameNodeMetrics().blockReport.inc(processTime);
    } finally {
      InjectionHandler.processEvent(InjectionEvent.FSNAMESYSTEM_BLOCKREPORT_COMPLETED);
      writeUnlock();
      checkSafeMode();
    }
    if (toInvalidate != null) {
      for (Block b : toInvalidate) {
        NameNode.stateChangeLog.info("BLOCK* NameSystem.processReport: block "
          + b + " on " + nodeID.getName() + " size " + b.getNumBytes()
          + " does not belong to any file.");
      }
    }
    String shortCircuit = firstReport ? " (shortCircuit first report)" : "";
    NameNode.stateChangeLog.info("BLOCK* NameSystem.processReport"+ shortCircuit 
        + ": from " + nodeID.getName() + " with " + newReport.getNumberOfBlocks()
        + " blocks took " + processTime + "ms"
        + (toAdd == null ? "." : (": #toAdd = " + toAdd.size() +
                                 " #toRemove = " + toRemove.size() +
                                 " #toInvalidate = " + toInvalidate.size() +
                                 ".")));
    if(firstReport && isInSafeMode()) {
      LOG.info("BLOCK* NameSystem.processReport: " + getReportingNodes() +
          " data nodes reporting, " +
          getSafeBlocks() + "/" + getBlocksTotal() +
          " blocks safe (" + getSafeBlockRatio() + ")");
    }
    return toRetry;
  }

  /**
   * Return true if the block size number is valid
   */
  private boolean checkBlockSize(Block block, INodeFile inode) {
    if (block.getNumBytes() < 0) {
      return false;
    }
    BlockInfo[] blocks = inode.getBlocks();
    if (blocks.length == 0) {
      return false;
    }
    return block.getNumBytes() <= inode.getPreferredBlockSize();
  }
  
  /**
   * Modify (block-->datanode) map. Remove block from set of needed replications
   * if this takes care of the problem.
   * 
   * @return whether or not the block was stored.
   * @throws IOException 
   */
  private boolean addStoredBlock(Block block,
                               DatanodeDescriptor node,
                               DatanodeDescriptor delNodeHint) throws IOException {
    return addStoredBlockInternal(block, node, delNodeHint, false, null);
  }

  /**
   * Modify (block-->datanode) map.  Remove block from set of
   * needed replications if this takes care of the problem.
   *
   * @return the block that is stored in blockMap.
   * @throws IOException 
   */
  final boolean addStoredBlockInternal(Block block,
                               DatanodeDescriptor node,
                               DatanodeDescriptor delNodeHint,
                               boolean initialBlockReport,
                               InitialReportWorker worker) throws IOException {
    InjectionHandler.processEvent(InjectionEvent.FSNAMESYSTEM_ADDSTORED_BLOCK,
        node);
    // either this is a direct call protected by writeLock, or
    // this is parallel initial block report
    assert (hasWriteLock() || (worker != null));
    
    // the call is invoked by the initial block report worker thread
    final boolean parallelInitialBlockReport = (worker != null);
    
    BlockInfo storedBlock = blocksMap.getStoredBlock(block);
    
    // handle the case for standby when we receive a block
    // that we don't know about yet - this is for block reports,
    // where we do not explicitly check this beforehand
    if (storedBlock == null && nameNode.shouldRetryAbsentBlocks() // standby
        && nameNode.shouldRetryAbsentBlock(block, storedBlock)) {
      // block should be retried
      return false;
    }
    
    if (storedBlock == null) {
      // then we need to do some special processing.
      storedBlock = blocksMap.getStoredBlockWithoutMatchingGS(block);

      if (storedBlock == null) {
        rejectAddStoredBlock(
          new Block(block), node,
          "Block not in blockMap with any generation stamp",
          initialBlockReport, parallelInitialBlockReport);
        return false;
      }

      INodeFile inode = storedBlock.getINode();
      if (inode == null) {
        rejectAddStoredBlock(
          new Block(block), node,
          "Block does not correspond to any file",
          initialBlockReport, parallelInitialBlockReport);
        return false;
      }

      boolean reportedOldGS = block.getGenerationStamp() < storedBlock.getGenerationStamp();
      boolean reportedNewGS = block.getGenerationStamp() > storedBlock.getGenerationStamp();
      boolean underConstruction = inode.isUnderConstruction();
      boolean isLastBlock = inode.getLastBlock() != null &&
        inode.getLastBlock().getBlockId() == block.getBlockId();

      // Don't add blocks to the DN when they're part of the in-progress last block
      // and have an inconsistent generation stamp. Instead just add them to targets
      // for recovery purposes. They will get added to the node when
      // commitBlockSynchronization runs
      if (reportedOldGS || reportedNewGS) {  // mismatched generation stamp
        if (underConstruction && isLastBlock) {
          NameNode.stateChangeLog.info(
              "BLOCK* NameSystem.addStoredBlock: "
              + "Targets updated: block " + block + " on " + node.getName() +
              " is added as a target for block " + storedBlock + " with size " +
              block.getNumBytes());
          
          lockParallelBRLock(parallelInitialBlockReport);
          try {
            ((INodeFileUnderConstruction) inode).addTarget(node,
                block.getGenerationStamp());
          } finally {
            unlockParallelBRLock(parallelInitialBlockReport);
          }
        } else {
          rejectAddStoredBlock(
              new Block(block), node,
              "Reported block has mismatched generation stamp " +
              "but is not the last block of " +
              "an under-construction file. (current generation is " +
              storedBlock.getGenerationStamp() + ")",
              initialBlockReport, parallelInitialBlockReport);
        }
        return false;
      }
    }

    INodeFile fileINode = storedBlock.getINode();
    if (fileINode == null) {
      rejectAddStoredBlock(
          new Block(block), node,
          "Block does not correspond to any file",
          initialBlockReport, parallelInitialBlockReport);
      return false;
    }

    assert storedBlock != null : "Block must be stored by now";

    // add block to the data-node
    boolean added;
    if (!parallelInitialBlockReport) {
      // full insert
      added = node.addBlock(storedBlock);
    } else {
      // insert DN descriptor into stored block
      int index = node.addBlockWithoutInsertion(storedBlock);
      added = index >= 0;
      // inform the worker so it can insert it into its local list
      worker.setCurrentStoredBlock(storedBlock, index);
    }
    
    // Is the block being reported the last block of an underconstruction file?
    boolean blockUnderConstruction = false;
    
    if (fileINode.isUnderConstruction()) {
      Block last = fileINode.getLastBlock();
      if (last == null) {
        // This should never happen, but better to handle it properly than to throw
        // an NPE below.
        LOG.error("Null blocks for reported block=" + block + " stored=" + storedBlock +
          " inode=" + fileINode);
        return false;
      }
      blockUnderConstruction = last.equals(storedBlock);
    }

    // block == storedBlock when this addStoredBlock is the result of a block report
    if (block.getNumBytes() != storedBlock.getNumBytes()) {
      if (!checkBlockSize(block, storedBlock.getINode())) {
        try {
          // New replica has an invalid block size. Mark it as corrupted.
          LOG.warn("Mark new replica " + block + " from " + node.getName() +
            "as corrupt because its length " + block.getNumBytes() +
            " is not valid");
          markBlockAsCorrupt(block, node, parallelInitialBlockReport);
        } catch (IOException e) {
          LOG.warn("Error in deleting bad block " + block + e);
        }
      } else {
        long cursize = storedBlock.getNumBytes();
        if (cursize == 0) {
          storedBlock.setNumBytes(block.getNumBytes());
        } else if (cursize != block.getNumBytes()) {
          String logMsg = "Inconsistent size for block " + block + 
            " reported from " + node.getName() +
            " current size is " + cursize +
            " reported size is " + block.getNumBytes();
          // If the block is still under construction this isn't likely
          // to be a problem, so just log at INFO level.
          if (blockUnderConstruction) {
            if (cursize != 1) { // cursize == 1 implies block was fsynced
              LOG.info(logMsg);
            }
          } else {
            LOG.warn(logMsg);
          }

          try {
            if (cursize > block.getNumBytes() && !blockUnderConstruction) {
              // new replica is smaller in size than existing block.
              // Mark the new replica as corrupt.
              LOG.warn("Mark new replica " + block + " from " + node.getName() +
                "as corrupt because its length is shorter than existing ones");
              markBlockAsCorrupt(block, node, parallelInitialBlockReport);
            } else {
              // new replica is larger in size than existing block.
              if (!blockUnderConstruction) {
                // Mark pre-existing replicas as corrupt.
                int numNodes = blocksMap.numNodes(block);
                int count = 0;
                DatanodeDescriptor nodes[] = new DatanodeDescriptor[numNodes];
                Iterator<DatanodeDescriptor> it = blocksMap.nodeIterator(block);
                for (; it != null && it.hasNext(); ) {
                  DatanodeDescriptor dd = it.next();
                  if (!dd.equals(node)) {
                    nodes[count++] = dd;
                  }
                }
                for (int j = 0; j < count; j++) {
                  LOG.warn("Mark existing replica " + block + " from " + node.getName() +
                  " as corrupt because its length is shorter than the new one");
                  markBlockAsCorrupt(block, nodes[j], parallelInitialBlockReport);
                }
              }
              //
              // change the size of block in blocksMap
              //
              storedBlock.setNumBytes(block.getNumBytes());
            }
          } catch (IOException e) {
            LOG.warn("Error in deleting bad block " + block + e);
          }
        }
      }
      block = storedBlock;
    } else {
      block = storedBlock;
    }
    assert storedBlock == block : "Block must be stored by now";

    int curReplicaDelta = 0;

    if (added) {
      curReplicaDelta = 1;
    } else {
      NameNode.stateChangeLog.info("BLOCK* NameSystem.addStoredBlock: "
        + "Redundant addStoredBlock request received for "
        + block + " on " + node.getName()
        + " size " + block.getNumBytes());
    }

    // filter out containingNodes that are marked for decommission.
    NumberReplicas num = countNodes(storedBlock);
    int numCurrentReplica = 0;
    int numLiveReplicas = num.liveReplicas();

    boolean popReplQueuesBefore = isPopulatingReplQueuesInternal();

    if (!popReplQueuesBefore) {
      // if we haven't populated the replication queues
      // then use a cheaper method to count
      // we only need live and decommissioned replicas.
      numCurrentReplica = numLiveReplicas + num.decommissionedReplicas();
    } else {
      // count live & decommissioned replicas
      numCurrentReplica = numLiveReplicas + pendingReplications.getNumReplicas(block);
    }

    if (blockUnderConstruction) {
      lockParallelBRLock(parallelInitialBlockReport);
      try {
        added = ((INodeFileUnderConstruction) fileINode).addTarget(node,
            block.getGenerationStamp());
      } finally {
        unlockParallelBRLock(parallelInitialBlockReport);
      }
    }

    // check whether safe replication is reached for the block. Do not increment
    // safe block count if this block has already been reported by the same
    // datanode before. We used the check for 'added' to achieve this.
    if (added && isInSafeModeInternal()) {
      int numSafeReplicas = numLiveReplicas + num.decommissionedReplicas();
      if (blockUnderConstruction) {
        // If this is the last block under construction then all the replicas so
        // far have been added to the "targets" field of
        // INodeFileUnderConstruction and hence lookup the replication factor
        // from there.
        // In this code path, a "valid" replica is added to the block, and we
        // only count "valid" replica to be safe replica. It is possible that
        // earlier a replica with higher generation stamp has been added and a
        // false was returned. In that case, if we only count number of targets
        // we will miss to count in the case that we first accepted more than
        // minReplication's number of higher generation stamp blocks but later
        // blocks with "valid" generation stamps themselves reached minimum
        // replication again.
        DatanodeDescriptor[] validDNs = ((INodeFileUnderConstruction) fileINode)
            .getValidTargets();
        numSafeReplicas = (validDNs != null) ? validDNs.length : 0;
      }
      
      if (!parallelInitialBlockReport && added) {
        // regular add stored block
        incrementSafeBlockCount(numSafeReplicas, initialBlockReport);
      } else if (parallelInitialBlockReport && added) {
        // for parallel initial block report increment local worker variable
        worker.incrementSafeBlockCount(numSafeReplicas);
      }
    }

    if (!popReplQueuesBefore && isPopulatingReplQueuesInternal()) {
      // we have just initialized the repl queues
      // must recompute num
      num = countNodes(storedBlock);
      numLiveReplicas = num.liveReplicas();
      numCurrentReplica = numLiveReplicas +
        pendingReplications.getNumReplicas(block);
    }

    //
    // if file is being actively written to and it is the last block, 
    // then do not check replication-factor here.
    //
    if (blockUnderConstruction) {
      return true;
    }

    // do not handle mis-replicated blocks during start up
    if (!isPopulatingReplQueuesInternal()) {
      return true;
    }

    if (curReplicaDelta == 0) {
      return true;
    }
    
    // handle underReplication
    short blockReplication = fileINode.getBlockReplication(storedBlock);
    updateNeededReplicationQueue(storedBlock, curReplicaDelta, numCurrentReplica,
        num.decommissionedReplicas, node, blockReplication);
    // handle over-replication
    if (numCurrentReplica > blockReplication) {
      // Put block into a queue and handle excess block asyncly
      if (delNodeHint == null || node == delNodeHint) {
        overReplicatedBlocks.add(block);
      } else {
        overReplicatedBlocks.add(new OverReplicatedBlock(block, node, delNodeHint));
      }
    }
    // If the file replication has reached desired value
    // we can remove any corrupt replicas the block may have
    int corruptReplicasCount = corruptReplicas.numCorruptReplicas(block);
    int numCorruptNodes = num.corruptReplicas();
    if (numCorruptNodes != corruptReplicasCount) {
      LOG.warn("Inconsistent number of corrupt replicas for " +
        block + "blockMap has " + numCorruptNodes +
        " but corrupt replicas map has " + corruptReplicasCount);
    }
    if ((corruptReplicasCount > 0) && (numLiveReplicas >= blockReplication)) {
      invalidateCorruptReplicas(block);
    }
    return true;
  }

  /**
   * Log a rejection of an addStoredBlock RPC, invalidate the reported block.
   * 
   * @param block
   * @param node
   * @param msg error message
   * @param ignoreInfoLogs should the logs be printed (used for fast startup)
   * @param parallelInitialBlockReport indicates that this call
   *        is a result of parallel initial block report
   */
  private void rejectAddStoredBlock(Block block,
                                     DatanodeDescriptor node,
                                     String msg,
                                     boolean ignoreInfoLogs,
                                     final boolean parallelInitialBlockReport) {
      if ((!isInSafeModeInternal()) && (!ignoreInfoLogs)) {
        NameNode.stateChangeLog.info("BLOCK* NameSystem.addStoredBlock: "
                                     + "addStoredBlock request received for "
                                     + block + " size " + block.getNumBytes()
                                     + " but was rejected and added to invalidSet of " 
                                     + node.getName() + " : " + msg);
      }
      lockParallelBRLock(parallelInitialBlockReport);
      try {
        addToInvalidatesNoLog(block, node, false);
        // we do not need try finally, the lock is unlocked by the worker if locked
      } finally {
        unlockParallelBRLock(parallelInitialBlockReport);
      }
  }

  /**
   * Invalidate corrupt replicas.
   * <p/>
   * This will remove the replicas from the block's location list,
   * add them to {@link #recentInvalidateSets} so that they could be further
   * deleted from the respective data-nodes,
   * and remove the block from corruptReplicasMap.
   * <p/>
   * This method should be called when the block has sufficient
   * number of live replicas.
   *
   * @param blk Block whose corrupt replicas need to be invalidated
   */
  void invalidateCorruptReplicas(Block blk) {
    Collection<DatanodeDescriptor> nodes = corruptReplicas.getNodes(blk);
    boolean gotException = false;
    if (nodes == null) {
      return;
    }
    // Make a copy of this list, since calling invalidateBlock will modify
    // the original (avoid CME)
    nodes = new ArrayList<DatanodeDescriptor>(nodes);
    if (NameNode.stateChangeLog.isDebugEnabled()) {
      NameNode.stateChangeLog.debug("NameNode.invalidateCorruptReplicas: " +
          "invalidating corrupt replicas on " + nodes.size() + "nodes");
    }
    for (Iterator<DatanodeDescriptor> it = nodes.iterator(); it.hasNext();) {
      DatanodeDescriptor node = it.next();
      try {
        invalidateBlock(blk, node, true);
      } catch (IOException e) {
        NameNode.stateChangeLog.info("NameNode.invalidateCorruptReplicas " +
          "error in deleting bad block " + blk +
          " on " + node + e);
        gotException = true;
      }
    }
    // Remove the block from corruptReplicasMap
    if (!gotException) {
      corruptReplicas.removeFromCorruptReplicasMap(blk);
    }
  }
  
  /**
   * A worker thread for processing mis-replicated blocks. Each worker is
   * responsible for traversing a single shard of blocks, and update replication
   * queues. The only modified objects are the replication queues, and the
   * updates are sychronized across workes by "lock" object. The workers are
   * spawned in processMisreplicatedBlocks(), which holds the write lock.
   */
  class MisReplicatedBlocksWorker implements Runnable {
    private final int shardId;
    private final AtomicLong count;
    private final Iterator<BlockInfo> blockIterator;
    private final long totalBlocks;
    private final Object lock;

    private long lastReportTime;       
    private long nrInvalid = 0;
    private long missingUnderConstructionBlocks;
    
    /**
     * Constructor
     * @param lock object on which the updates to replications queues are synchronized
     * @param shardId id of the shard
     * @param count counter of processed blocks used for reporting 
     * @param blockIterator blcks to be processed by this workes
     * @param totalBlocks total block count used for reporting
     */
    public MisReplicatedBlocksWorker(Object lock, int shardId,
        AtomicLong count, Iterator<BlockInfo> blockIterator, long totalBlocks) {
      this.shardId = shardId;
      this.count = count;
      this.blockIterator = blockIterator;
      this.totalBlocks = totalBlocks;
      this.lock = lock;     
      this.lastReportTime = now();
    }
    
    @Override
    public void run() {
      String logPrefix = "Processing mis-replicated blocks: ";
      FLOG.info(logPrefix + "starting worker for shard: " + shardId);
      int increment = 0;
      
      while(blockIterator.hasNext()) {
        BlocksMap.BlockInfo block = blockIterator.next();
        if (++increment == 1000) {
          // increment the global counter every 1000 blocks
          // to avoid lock contention
          count.addAndGet(increment);
          increment = 0;
        }
        if (shardId == 0 && lastReportTime < now() - 5000) {
          // only "first" thread is reporting every 5 seconds
          lastReportTime = now();
          FLOG.info(logPrefix + "Percent completed: " +
              (count.get() * 100 / totalBlocks) );
        }
        
        INodeFile fileINode = block.getINode();
        if (fileINode == null) {
          // block does not belong to any file
          nrInvalid++;
          synchronized (lock) {
            addToInvalidates(block, false);
          }         
          continue;
        }
        
        // If this is a last block of a file under construction ignore it.
        if (block.equals(fileINode.getLastBlock())
            && fileINode.isUnderConstruction()) {
          INodeFileUnderConstruction cons =
            (INodeFileUnderConstruction) fileINode;
          int replicas = cons.getTargets() != null ?
            cons.getTargets().length : 0;
          if (replicas < minReplication) {
            missingUnderConstructionBlocks++;
          }
          LOG.warn("Skipping under construction block : " + block +
              " with replicas : " + replicas);
          continue;
        }

        // calculate current replication
        short expectedReplication = fileINode.getBlockReplication(block);
        NumberReplicas num = countNodes(block);
        int numCurrentReplica = num.liveReplicas();
        
        // add to under-replicated queue if need to be
        if (numCurrentReplica >= 0 && numCurrentReplica < expectedReplication) {
          // perform this check to decrease lock contention
          synchronized (lock) {
            neededReplications.add(block,
              numCurrentReplica,
              num.decommissionedReplicas(),
              expectedReplication);
          }
          continue;
        }

        if (numCurrentReplica > expectedReplication) {
          // over-replicated block
          synchronized (lock) {
            overReplicatedBlocks.add(block);
          }
        }
      }
      FLOG.info(logPrefix + " worker for shard: " + shardId + " done");
      // add last block count
      count.addAndGet(increment);
    }
  }
  
  /**
   * For each block in the name-node verify whether it belongs to any file,
   * over or under replicated. Place it into the respective queue.
   */
  protected void processMisReplicatedBlocks() {
    writeLock();
    try {
      if (this.initializedReplQueues) {
        return;
      }
      String logPrefix = "Processing mis-replicated blocks: ";
      long nrInvalid = 0;
      
      // clear queues
      neededReplications.clear();
      overReplicatedBlocks.clear();
      raidEncodingTasks.clear();
      
      int totalBlocks = blocksMap.size();
      
      // shutdown initial block report executor
      setupInitialBlockReportExecutor(true);
      
      // simply return if there are no blocks
      if (totalBlocks == 0) {
        // indicate that we are populating replication queues
        setInitializedReplicationQueues(true);
        LOG.info(logPrefix + "Blocks map empty. Nothing to do.");
        return;
      }
      
      AtomicLong count = new AtomicLong(0);
      
      // shutdown initial block report thread executor
      setupInitialBlockReportExecutor(true);
      
      FLOG.info(logPrefix + "Starting, total number of blocks: " + totalBlocks);
      long start = now();
      
      // have one thread process at least 1M blocks
      // no more than configured number of threads
      // at least one thread if the number of blocks < 1M
      int numShards = Math.min(parallelProcessingThreads,
          ((totalBlocks + parallelRQblocksPerShard - 1) / parallelRQblocksPerShard));
      
      // get shard iterators
      List<Iterator<BlockInfo>> iterators = blocksMap
          .getBlocksIterarors(numShards);

      // sanity, as the number can be lower in corner cases
      numShards = iterators.size();
      
      FLOG.info(logPrefix + "Using " + numShards + " threads");
      
      List<MisReplicatedBlocksWorker> misReplicationWorkers 
        = new ArrayList<MisReplicatedBlocksWorker>();
      List<Thread> misReplicationThreads = new ArrayList<Thread>();
      
      // workers will synchronize on this object to insert into 
      // replication queues
      Object lock = new Object();
      
      // initialize mis-replication worker threads
      for (int i = 0; i < numShards; i++) {
        MisReplicatedBlocksWorker w = new MisReplicatedBlocksWorker(lock, i, count,
            iterators.get(i), totalBlocks);
        Thread t = new Thread(w);
        t.start();
        misReplicationWorkers.add(w);
        misReplicationThreads.add(t);
      }
      
      // join all threads
      for (Thread t : misReplicationThreads) {
        try {
          t.join();
        } catch (InterruptedException e) { 
          Thread.currentThread().interrupt();
        }
      }
      
      // get number of invalidated blocks
      for (MisReplicatedBlocksWorker w : misReplicationWorkers) {
        nrInvalid += w.nrInvalid;
      }
      
      // update safeblock count (finalized blocks)
      this.blocksSafe = blocksMap.size() - getMissingBlocksCount();
      // update safeblock count (under construction blocks)
      for (MisReplicatedBlocksWorker w : misReplicationWorkers) {
        this.blocksSafe -= w.missingUnderConstructionBlocks;
      }
      
      // indicate that we are populating replication queues
      setInitializedReplicationQueues(true);

      long stop = now();
      
      FLOG.info(logPrefix + "Total number of blocks = " + blocksMap.size());
      FLOG.info(logPrefix + "Number of invalid blocks = " + nrInvalid);
      FLOG.info(logPrefix + "Number of under-replicated blocks = " +
          neededReplications.size());
      FLOG.info(logPrefix + "Number of  over-replicated blocks = " +
          overReplicatedBlocks.size());
      FLOG.info(logPrefix + "Number of safe blocks = " + this.blocksSafe);
      FLOG.info(logPrefix + "Finished in " + (stop - start) + " ms");
    } finally {
      writeUnlock();
    }
  }
  
  /**
   * This is called from the RaidEncodingTaskEncodingMonitor to process
   *  encoding tasks
   * Each call will poll (raidEncodingTaskMultiplier*nodes) stripes from
   * raidEncodingTasks.
   * Then for each stripe, it will compute necessary information to encode 
   * the stripe and add encoding tasks to datanodes' queue. 
   * If it fails to compute, it will add back to the raidEncodingTasks
   * for the next round
   */
  private void processRaidEncodingTaskAsync() {
    // Should not schedule raiding during safe mode
    if (isInSafeMode()) {
      return;
    }
    
    final int nodes = heartbeats.size();
    List<RaidBlockInfo> tasksToProcess = new ArrayList<RaidBlockInfo>(Math.min(
        raidEncodingTasks.size(),
        ReplicationConfigKeys.raidEncodingTaskMultiplier * nodes));
    
    for (int i = 0; i < ReplicationConfigKeys.raidEncodingTaskMultiplier; i++) {
      writeLock();
      try {
        NameNode.getNameNodeMetrics().numRaidEncodingTasks.set(
            raidEncodingTasks.size());
        raidEncodingTasks.pollNToList(nodes, tasksToProcess);
        if (raidEncodingTasks.isEmpty()) {
          break;
        }
      } finally {
        writeUnlock();
      }
    }

    ArrayList<RaidBlockInfo> tasksToAdd = new ArrayList<RaidBlockInfo>();
    for (RaidBlockInfo rbi : tasksToProcess) {
      if (NameNode.stateChangeLog.isDebugEnabled()) {
        NameNode.stateChangeLog
            .debug("BLOCK* NameSystem.processRaidEncodingTaskAsync: " + rbi);
      }
      if (processRaidEncodingTask(rbi)) {
        tasksToAdd.add(rbi);
      }
    }
    if (!tasksToAdd.isEmpty()) {
      writeLock();
      try {
        for (RaidBlockInfo rbi: tasksToAdd) {
          raidEncodingTasks.add(rbi);
        }
      } finally {
        writeUnlock();
      }
    }
  }

  /**
   * This is called from the ReplicationMonitor to process over
   * replicated blocks.
   */
  private void processOverReplicatedBlocksAsync() {

    // blocks should not be scheduled for deletion during safemode
    if (isInSafeMode()) {
      return;
    }
    
    if (delayOverreplicationMonitorTime > now()) {
      LOG.info("Overreplication monitor delayed for "
          + ((delayOverreplicationMonitorTime - now()) / 1000) + " seconds");
      return;
    }
    nameNode.clearOutstandingNodes();
    
    final int nodes = heartbeats.size();
    List<Block> blocksToProcess = new ArrayList<Block>(Math.min(
        overReplicatedBlocks.size(),
        ReplicationConfigKeys.overreplicationWorkMultiplier * nodes));
    
    for (int i = 0; i < ReplicationConfigKeys.overreplicationWorkMultiplier; i++) {
      writeLock();
      try {
        NameNode.getNameNodeMetrics().numOverReplicatedBlocks.set(overReplicatedBlocks.size());
        overReplicatedBlocks.pollNToList(nodes, blocksToProcess);
        if (overReplicatedBlocks.isEmpty()) {
          break;
        }
      } finally {
        writeUnlock();
      }
    }

    for (Block block : blocksToProcess) {
      if (NameNode.stateChangeLog.isDebugEnabled()) {
        NameNode.stateChangeLog
            .debug("BLOCK* NameSystem.processOverReplicatedBlocksAsync: " + block);
      }
      if (block instanceof OverReplicatedBlock) {
        OverReplicatedBlock opb = (OverReplicatedBlock)block;
        processOverReplicatedBlock(block, (short) -1, opb.addedNode, opb.delNodeHint);
      } else {
        processOverReplicatedBlock(block, (short) -1, null, null);
      }
        
    }
  }
  
  /**
   * Find out datanodes to store parity blocks and schedule raiding on one of them
   * @return whether we want to add the raid task back to the queue 
   */
  private boolean processRaidEncodingTask(RaidBlockInfo rbi) {
    ArrayList<ArrayList<DatanodeInfo>> stripeDatanodes =
        new ArrayList<ArrayList<DatanodeInfo>>();
    BlockInfo[] stripeBlocks = null;
    RaidCodec codec = null;
    StringBuilder sb = new StringBuilder();
    StringBuilder dnSb = new StringBuilder();
    readLock();
    try {
      INodeFile inode = blocksMap.getINode(rbi);
      if (inode == null) {
        return false; // file has been deleted already, nothing to do.
      }
      if (inode.getStorageType() != StorageType.RAID_STORAGE) {
        LOG.error("File for block " + rbi + " is not raidable");
        return false;
      }
      INodeRaidStorage storage = (INodeRaidStorage)inode.getStorage();
      codec = storage.getCodec();
      // Find out related blocks
      stripeBlocks = codec.getBlocksInOneStripe(
          inode.getBlocks(), rbi);
      // Find out datanode locations
      for (int i = 0; i < stripeBlocks.length; i++) {
        ArrayList<DatanodeInfo> liveNodes = 
            new ArrayList<DatanodeInfo>();
        stripeDatanodes.add(liveNodes);
        Collection<DatanodeDescriptor> nodesCorrupt = 
            corruptReplicas.getNodes(stripeBlocks[i]);
        StringBuilder dn = new StringBuilder();
        dn.append("[");
        for (Iterator<DatanodeDescriptor> it =
            blocksMap.nodeIterator(stripeBlocks[i]); 
            it.hasNext();) {
          DatanodeDescriptor node = it.next();
          if (nodesCorrupt != null && nodesCorrupt.contains(node)) {
            // do nothing
          } else {
            liveNodes.add(node);
            dn.append(node);
            dn.append(" ");
          }
        }
        dn.append("]");
        if (i >= codec.numParityBlocks && liveNodes.size() == 0) {
          LOG.error("File for block " + rbi +
              " is not raidable, not all source blocks have replicas");
          return true;
        }
        sb.append(stripeBlocks[i]);
        sb.append(" ");
        dnSb.append(dn.toString());
        dnSb.append(" ");
      }
    } finally {
      readUnlock();
    }
    DatanodeDescriptor scheduleNode = null; 
    int[] toRaidIdxs = new int[codec.numParityBlocks];
    // Randomly pick datanodes first
    for (int i = 0; i < codec.numParityBlocks; i++) {
      toRaidIdxs[i] = i;
      int size = stripeDatanodes.get(i).size();
      stripeDatanodes.get(i).clear();
      if (size < codec.parityReplication) {
        // If not enough replicas, assign targets left
        for (int j = 0; j < codec.parityReplication - size; j++) {
          stripeDatanodes.get(i).add(getRandomDatanode());
        }
        if (scheduleNode == null) {
          scheduleNode = (DatanodeDescriptor)stripeDatanodes.get(i).get(0);
        }
      }
    }
    // Assign raiding task to scheduledNode
    if (scheduleNode == null) {
      // All parity blocks are generated, no need to raid
      return false;
    }
    RaidTask rt = new RaidTask(codec, stripeBlocks, stripeDatanodes, toRaidIdxs);
    writeLock();
    try {
      scheduleNode.addRaidEncodingTask(rt);
      if (NameNode.stateChangeLog.isDebugEnabled()) {
        NameNode.stateChangeLog.debug("Stripe* NameSystem: Blocks " + sb.toString()
            + " Datanodes: " + dnSb.toString()
            + " are added to raidEncodingTasks of" + scheduleNode);
      }
      return false;
    } finally {
      writeUnlock();
    }
  }
  
  /**
   * Find how many of the containing nodes are "extra", if any.
   * If there are any extras, call findOverReplicatedReplicas() to
   * insert them into excessReplicateTmp.
   */
  private void processOverReplicatedBlock(Block block, short replication,
                                          DatanodeDescriptor addedNode, DatanodeDescriptor delNodeHint) {

    List<DatanodeID> excessReplicateTmp = new ArrayList<DatanodeID>();
    List<DatanodeID> originalDatanodes = new ArrayList<DatanodeID>();

    // find all replicas that can possibly be deleted.
    // The results are returned in excessReplicateTmp.
    findOverReplicatedReplicas(block, replication,
      addedNode, delNodeHint,
      excessReplicateTmp, originalDatanodes);
    if (excessReplicateTmp.size() <= 0) {
      return;
    }

    writeLock();                     // acquire write lock,
    try {
      BlockInfo storedBlock = blocksMap.getBlockInfo(block);
      INodeFile inode = (storedBlock == null) ? null : storedBlock.getINode(); 
      if (inode == null) {
        return; // file has been deleted already, nothing to do.
      }

      //
      // if the state of replicas of this block has changed since the time
      // when we released and reacquired the lock, then all the decisions
      // that we have made so far might not be correct. Do not delete excess
      // replicas in this case.

      int live = 0;
      Collection<DatanodeDescriptor> nodesCorrupt = corruptReplicas.getNodes(block);
      for (Iterator<DatanodeDescriptor> it = blocksMap.nodeIterator(block);
           it.hasNext();) {
        DatanodeDescriptor node = it.next();
        if (((nodesCorrupt != null) && (nodesCorrupt.contains(node))) ||
          node.isDecommissionInProgress() || node.isDecommissioned()) {
          // do nothing
        } else {
          live++;            // number of live nodes
          originalDatanodes.remove(node);
        }
      }

      if (originalDatanodes.size() > 0) {
        NameNode.stateChangeLog.info("Unable to delete excess replicas for block " +
          block +
          " because the state of the original replicas have changed." +
          " Will retry later.");
        overReplicatedBlocks.add(block);
        return;
      }
      
      short blockReplication = inode.getBlockReplication(storedBlock);

      // loop through datanodes that have excess-replicas of this block
      for (ListIterator<DatanodeID> iter = excessReplicateTmp.listIterator();
           iter.hasNext();) {
        DatanodeID datanodeId = iter.next();

        // re-check that block still has excess replicas.
        // If not, then there is nothing more to do.
        if (live <= blockReplication) {
          break;
        }

        // find the DatanodeDescriptor for this datanode
        DatanodeDescriptor datanode = null;
        try {
          datanode = getDatanode(datanodeId);
        } catch (IOException e) {
        }
        if (datanode == null) {
          NameNode.stateChangeLog.info("No datanode found while processing " +
            "overreplicated block " + block);
          continue;              // dead datanode?
        }

        // insert into excessReplicateMap
        LightWeightHashSet<Block> excessBlocks = excessReplicateMap.get(datanodeId.getStorageID());
        if (excessBlocks == null) {
          excessBlocks = new LightWeightHashSet<Block>();
          excessReplicateMap.put(datanodeId.getStorageID(), excessBlocks);
        }

        if (excessBlocks.add(block)) {
          excessBlocksCount++;
          if (NameNode.stateChangeLog.isDebugEnabled()) {
            NameNode.stateChangeLog.debug("BLOCK* NameSystem.chooseExcessReplicates: "
              + "(" + datanodeId.getName() + ", " + block
              + ") is added to excessReplicateMap");
          }
        }
        //
        // The 'excessblocks' tracks blocks until we get confirmation
        // that the datanode has deleted them; the only way we remove them
        // is when we get a "removeBlock" message.
        //
        // The 'invalidate' list is used to inform the datanode the block
        // should be deleted.  Items are removed from the invalidate list
        // upon giving instructions to the namenode.
        //
        addToInvalidatesNoLog(block, datanode, true);
        live--;
        if (NameNode.stateChangeLog.isDebugEnabled()) {
          NameNode.stateChangeLog.debug("BLOCK* NameSystem.chooseExcessReplicates: "
            + "(" + datanode.getName() + ", " + block + ") is added to recentInvalidateSets");
        }
      }
    } finally {
      writeUnlock();
    }
  }

  /**
   * Find how many of the containing nodes are "extra", if any.
   * If there are any extras, call chooseExcessReplicates() to
   * mark them in the excessReplicateMap.
   *
   * @param excessReplicateMapTmp replicas that can possibly be in excess
   * @param originalDatanodes     all currently valid replicas of this block
   */
  private void findOverReplicatedReplicas(Block block, short replication,
                                          DatanodeDescriptor addedNode, DatanodeDescriptor delNodeHint,
                                          List<DatanodeID> excessReplicateMapTmp,
                                          List<DatanodeID> originalDatanodes) {

    Collection<DatanodeDescriptor> nonExcess;
    INodeFile inode;

    readLock();
    try {
      BlockInfo storedBlock = blocksMap.getBlockInfo(block);
      inode = (storedBlock == null) ? null : storedBlock.getINode();
      if (inode == null) {
        return; // file has been deleted already, nothing to do.
      }

      // if the caller did not specify what the target replication factor
      // of the file, then fetch it from the inode. This happens when invoked
      // by the ReplicationMonitor thread.
      if (replication < 0) {
        replication = inode.getBlockReplication(storedBlock);
      }

      if (addedNode == delNodeHint) {
        delNodeHint = null;
      }
      nonExcess = new ArrayList<DatanodeDescriptor>();
      Collection<DatanodeDescriptor> corruptNodes = corruptReplicas.getNodes(block);
      for (Iterator<DatanodeDescriptor> it = blocksMap.nodeIterator(block);
           it.hasNext();) {
        DatanodeDescriptor cur = it.next();
        Collection<Block> excessBlocks = excessReplicateMap.get(cur.getStorageID());
        if (excessBlocks == null || !excessBlocks.contains(block)) {
          if (!cur.isDecommissionInProgress() && !cur.isDecommissioned()) {
            // exclude corrupt replicas
            if (corruptNodes == null || !corruptNodes.contains(cur)) {
              nonExcess.add(cur);
              originalDatanodes.add(cur);
            }
          }
        }
      }
    } finally {
      readUnlock();
    }

    // this can be called without the FSnamesystem lock because it does not
    // use any global data structures. Also, the inode is passed as it is to
    // the Pluggable blockplacement policy.
    chooseExcessReplicates(nonExcess, block, replication,
      addedNode, delNodeHint, inode, excessReplicateMapTmp);
  }

  /**
   * We want "replication" replicates for the block, but we now have too many.
   * In this method, copy enough nodes from 'srcNodes' into 'dstNodes' such that:
   * <p/>
   * srcNodes.size() - dstNodes.size() == replication
   * <p/>
   * We pick node that make sure that replicas are spread across racks and
   * also try hard to pick one with least free space.
   */
  void chooseExcessReplicates(Collection<DatanodeDescriptor> nonExcess,
                              Block b, short replication,
                              DatanodeDescriptor addedNode,
                              DatanodeDescriptor delNodeHint,
                              INodeFile inode,
                              List<DatanodeID> excessReplicateMapTmp) {
    // first form a rack to datanodes map and
    HashMap<String, ArrayList<DatanodeDescriptor>> rackMap =
      new HashMap<String, ArrayList<DatanodeDescriptor>>();
    for (Iterator<DatanodeDescriptor> iter = nonExcess.iterator();
         iter.hasNext();) {
      DatanodeDescriptor node = iter.next();
      String rackName = node.getNetworkLocation();
      ArrayList<DatanodeDescriptor> datanodeList = rackMap.get(rackName);
      if (datanodeList == null) {
        datanodeList = new ArrayList<DatanodeDescriptor>();
      }
      datanodeList.add(node);
      rackMap.put(rackName, datanodeList);
    }

    // split nodes into two sets
    // priSet contains nodes on rack with more than one replica
    // remains contains the remaining nodes
    // It may be useful for the corresponding BlockPlacementPolicy.
    ArrayList<DatanodeDescriptor> priSet = new ArrayList<DatanodeDescriptor>();
    ArrayList<DatanodeDescriptor> remains = new ArrayList<DatanodeDescriptor>();
    for (Iterator<Entry<String, ArrayList<DatanodeDescriptor>>> iter =
      rackMap.entrySet().iterator(); iter.hasNext();) {
      Entry<String, ArrayList<DatanodeDescriptor>> rackEntry = iter.next();
      ArrayList<DatanodeDescriptor> datanodeList = rackEntry.getValue();
      if (datanodeList.size() == 1) {
        remains.add(datanodeList.get(0));
      } else {
        priSet.addAll(datanodeList);
      }
    }

    // pick one node to delete that favors the delete hint
    // otherwise follow the strategy of corresponding BlockPlacementPolicy.
    boolean firstOne = true;
    while (nonExcess.size() - replication > 0) {
      DatanodeInfo cur = null;
      long minSpace = Long.MAX_VALUE;

      // check if we can del delNodeHint
      if (firstOne && delNodeHint != null && nonExcess.contains(delNodeHint) &&
        (priSet.contains(delNodeHint) || (addedNode != null && !priSet.contains(addedNode)))) {
        cur = delNodeHint;
      } else { // regular excessive replica removal
        cur = replicator.chooseReplicaToDelete(inode, b, replication, priSet, remains);
      }

      firstOne = false;
      // adjust rackmap, priSet, and remains
      String rack = cur.getNetworkLocation();
      ArrayList<DatanodeDescriptor> datanodes = rackMap.get(rack);
      datanodes.remove(cur);
      if (datanodes.isEmpty()) {
        rackMap.remove(rack);
      }
      if (priSet.remove(cur)) {
        if (datanodes.size() == 1) {
          priSet.remove(datanodes.get(0));
          remains.add(datanodes.get(0));
        }
      } else {
        remains.remove(cur);
      }
      nonExcess.remove(cur);

      excessReplicateMapTmp.add(cur);
      if (NameNode.stateChangeLog.isDebugEnabled()) {
        NameNode.stateChangeLog.debug("BLOCK* NameSystem.chooseExcessReplicates: "
          + "(" + cur.getName() + ", " + b
          + ") is added to excessReplicateMapTmp");
      }
    }
  }

  public void blockReceivedAndDeleted(DatanodeRegistration nodeReg,
      Block receivedAndDeletedBlocks[]) throws IOException {
  int received = 0;
  int deleted = 0;
  int processTime = 0;
  writeLock();
  long startTime = now();
    try {
      DatanodeDescriptor node = getDatanode(nodeReg);
      if (node == null || !node.isAlive) {
        NameNode.stateChangeLog.warn("BLOCK* NameSystem.blockReceivedDeleted" +
          " is received from dead or unregistered node " + nodeReg.getName());
        throw new IOException(
          "Got blockReceivedDeleted message from unregistered or dead node " + nodeReg.getName());
      }

      if (NameNode.stateChangeLog.isDebugEnabled()) {
        NameNode.stateChangeLog.debug("BLOCK* NameSystem.blockReceivedDeleted"
           + " is received from " + nodeReg.getName());
      }

      for (int i = 0; i < receivedAndDeletedBlocks.length; i++) {
        //avatar datanode mighs send nulls in the array
        if (receivedAndDeletedBlocks[i] == null) {
          continue;
        }
        if (DFSUtil.isDeleted(receivedAndDeletedBlocks[i])) {
          removeStoredBlock(receivedAndDeletedBlocks[i], node);
          // only leave received block in the array
          receivedAndDeletedBlocks[i] = null; 
          deleted++;
        } else {
          if(!blockReceived(receivedAndDeletedBlocks[i],
              receivedAndDeletedBlocks[i].getDelHints(),node)) {
            // block was rejected
            receivedAndDeletedBlocks[i] = null;
            continue;
          }
          received++;
        }
      }
    } finally {
      processTime = (int)(now() - startTime);
      writeUnlock();
      if (received + deleted > 10) {
        // Only log for bigger incremental block reports.
        // This will cut a lot of logging.
        NameNode.stateChangeLog.info("*BLOCK* NameNode.blockReceivedAndDeleted: "
          + "from " + nodeReg.getName() + " took " + processTime
          + "ms: (#received = " + received + ", "
          + " #deleted = " + deleted + ")");
      }
      if(!isInStartupSafeMode()){
        int count = 0;
        // At startup time, because too many new blocks come in
        // they take up lots of space in the log file.
        // So, we log only when namenode is out of safemode.
        // We log only blockReceived messages.
        for (int i = 0; i < receivedAndDeletedBlocks.length; i++) {
          // log only the ones that have been processed
          if (count > received) {
            break;
          }
          Block blk = receivedAndDeletedBlocks[i];
          if (blk == null) 
              continue;
          ++count;
          if (blk.getNumBytes() != BlockFlags.NO_ACK) {       
            NameNode.stateChangeLog.info("BLOCK* NameSystem.blockReceived: "
                + "blockMap updated: " + nodeReg.getName() + " is added to "
                + receivedAndDeletedBlocks[i] + " size "
                + receivedAndDeletedBlocks[i].getNumBytes());
          } 
        }
      }
    }
  }

  public void blockReceivedAndDeleted(DatanodeRegistration nodeReg,
      IncrementalBlockReport receivedAndDeletedBlocks)
      throws IOException {
    int received = 0;
    int deleted = 0;
    int processTime = 0;
    
    Block blk = new Block(); 
    int numberOfAcks = receivedAndDeletedBlocks.getLength();
    
    writeLock();
    long startTime = now();
    try {
      DatanodeDescriptor node = getDatanode(nodeReg);
      if (node == null || !node.isAlive) {
        NameNode.stateChangeLog.warn("BLOCK* NameSystem.blockReceivedDeleted" +
          " is received from dead or unregistered node " + nodeReg.getName());
        throw new IOException(
          "Got blockReceivedDeleted message from unregistered or dead node " + nodeReg.getName());
      }

      if (NameNode.stateChangeLog.isDebugEnabled()) {
        NameNode.stateChangeLog.debug("BLOCK* NameSystem.blockReceivedDeleted"
           + " is received from " + nodeReg.getName());
      }

      receivedAndDeletedBlocks.resetIterator();
      for (int currentBlock = 0; currentBlock < numberOfAcks; currentBlock++) {
        String hint = receivedAndDeletedBlocks.getNext(blk);
        //avatar datanode mighs send nulls in the array
        if (blk.getNumBytes() == BlockFlags.IGNORE) {
          continue;
        }

        if (DFSUtil.isDeleted(blk)) {
          removeStoredBlock(blk, node); 
          deleted++;
        } else {
          if(!blockReceived(blk, hint, node)) {
            // block was rejected, and deleted
            blk.setNumBytes(BlockFlags.DELETED);
            receivedAndDeletedBlocks.setBlock(blk, currentBlock);
            continue;
          }
          received++;
        }
      }
    } finally {
      processTime = (int)(now() - startTime);
      writeUnlock();
      if (received + deleted > 10) {
        // Only log for bigger incremental block reports.
        // This will cut a lot of logging.
        NameNode.stateChangeLog.info("*BLOCK* NameNode.blockReceivedAndDeleted: "
          + "from " + nodeReg.getName() + " took " + processTime
          + "ms: (#received = " + received + ", "
          + " #deleted = " + deleted + ")");
      }
      if(!isInStartupSafeMode()){
        int count = 0;
        // At startup time, because too many new blocks come in
        // they take up lots of space in the log file.
        // So, we log only when namenode is out of safemode.
        // We log only blockReceived messages.
        receivedAndDeletedBlocks.resetIterator();
        while(receivedAndDeletedBlocks.hasNext()) {
          receivedAndDeletedBlocks.getNext(blk);
          if (count > received) {
            break;
          }
          if (DFSUtil.isDeleted(blk) || blk.getNumBytes() == BlockFlags.IGNORE)
            continue;
          ++count;
          if (blk.getNumBytes() != BlockFlags.NO_ACK) {       
            NameNode.stateChangeLog.info("BLOCK* NameSystem.blockReceived: "
                + "blockMap updated: " + nodeReg.getName() + " is added to "
                + blk + " size "
                + blk.getNumBytes());
          } 
        }
      }
    }
  }
  
  /**
   * Modify (block-->datanode) map.  Possibly generate
   * replication tasks, if the removed block is still valid.
   */
  private void removeStoredBlock(Block block, DatanodeDescriptor node) {
    if (NameNode.stateChangeLog.isDebugEnabled()) {
      NameNode.stateChangeLog.debug("BLOCK* NameSystem.removeStoredBlock: "
        + block + " from " + node.getName());
    }
    if (!blocksMap.removeNode(block, node)) {
      if (NameNode.stateChangeLog.isDebugEnabled()) {
        NameNode.stateChangeLog.debug("BLOCK* NameSystem.removeStoredBlock: "
          + block + " has already been removed from node " + node);
      }
      return;
    }

    //
    // if file is being actively written to and it is the last block, 
    // then do not check replication-factor here.
    //
    BlockInfo storedBlock = blocksMap.getStoredBlock(block);
    INodeFile fileINode = (storedBlock == null) ? null : storedBlock.getINode();
    if (fileINode != null && 
        fileINode.isUnderConstruction() && fileINode.isLastBlock(storedBlock)) {
      decrementSafeBlockCount(block);
      return;
    }      

    //
    // It's possible that the block was removed because of a datanode
    // failure.  If the block is still valid, check if replication is
    // necessary.  In that case, put block on a possibly-will-
    // be-replicated list.
    //
    if (fileINode != null) {
      decrementSafeBlockCount(block);
      // handle under replication
      // Use storedBlock here because block maybe a deleted block with size DELETED
      if (isPopulatingReplQueuesInternal()) {
        NumberReplicas num = countNodes(storedBlock);
        int numCurrentReplicas = num.liveReplicas()
            +
        pendingReplications.getNumReplicas(storedBlock);
        updateNeededReplicationQueue(storedBlock, -1, numCurrentReplicas,
            num.decommissionedReplicas, node, 
            fileINode.getBlockReplication(storedBlock));
      }
    }

    //
    // We've removed a block from a node, so it's definitely no longer
    // in "excess" there.
    //
    removeFromExcessReplicateMap(block, node);

    // Remove the replica from corruptReplicas
    corruptReplicas.removeFromCorruptReplicasMap(block, node);
  }
  
  /**
   * Update a block's priority queue in neededReplicaiton queues
   * 
   * @param blockInfo blockInfo
   * @param delta the change of number of replicas
   * @param numCurrentReplicas current number of replicas
   * @param numCurrentDecommissionedReplicas current number of decommissioed replicas
   * @param node the node where the replica resides
   * @param fileReplication expected number of replicas
   */
  private void updateNeededReplicationQueue(BlockInfo blockInfo, int delta,
      int numCurrentReplicas, int numCurrentDecommissionedReplicas,
       DatanodeDescriptor node, short fileReplication) {
     int numOldReplicas = numCurrentReplicas;
     int numOldDecommissionedReplicas = numCurrentDecommissionedReplicas;
     if (node.isDecommissioned() || node.isDecommissionInProgress()) {
       numOldDecommissionedReplicas -= delta;
     } else {
       numOldReplicas -= delta;
     }   
     if (fileReplication > numOldReplicas) {
       neededReplications.remove(blockInfo, numOldReplicas,
           numOldDecommissionedReplicas, fileReplication);
     }
     if (fileReplication > numCurrentReplicas) {
       neededReplications.add(blockInfo, numCurrentReplicas,
           numCurrentDecommissionedReplicas, fileReplication); 
     }   
   }   

  private void removeFromExcessReplicateMap(Block block, DatanodeDescriptor node){
    // No need for lock until we start accepting requests from clients.
    assert (!nameNode.isRpcServerRunning() || hasWriteLock());
    Collection<Block> excessBlocks = excessReplicateMap.get(node.getStorageID());
    if (excessBlocks != null) {
      if (excessBlocks.remove(block)) {
        excessBlocksCount--;
        if (NameNode.stateChangeLog.isDebugEnabled()) {
          NameNode.stateChangeLog.debug("BLOCK* NameSystem.removeFromExcessReplicateMap: "
            + block + " is removed from excessBlocks");
        }
        if (excessBlocks.size() == 0) {
          excessReplicateMap.remove(node.getStorageID());
        }
      }
    }
  }
  
  private void removeFromExcessReplicateMap(Block block){
    // No need for lock until we start accepting requests from clients.
    assert (!nameNode.isRpcServerRunning() || hasWriteLock());
    for (Iterator<DatanodeDescriptor> it =
      blocksMap.nodeIterator(block); it.hasNext();) {
      DatanodeDescriptor node = it.next();
      removeFromExcessReplicateMap(block, node);
    }
  }

  /**
   * The given node is reporting that it received a certain block.
   */
  private boolean blockReceived(Block block,
                            String delHint,
                            DatanodeDescriptor node) throws IOException {
    assert (hasWriteLock());

    // decrement number of blocks scheduled to this datanode.
    node.decBlocksScheduled();

    // get the deletion hint node
    DatanodeDescriptor delHintNode = null;
    if (delHint != null && delHint.length() != 0) {
      delHintNode = datanodeMap.get(delHint);
      if (delHintNode == null) {
        NameNode.stateChangeLog.warn("BLOCK* NameSystem.blockReceived: "
          + block
          + " is expected to be removed from an unrecorded node "
          + delHint);
      }
    }
    //
    // Modify the blocks->datanode map and node's map.
    //
    pendingReplications.remove(block);
    return addStoredBlock(block, node, delHintNode);
  }

  public long getMissingBlocksCount() {
    // not locking
    return neededReplications.getCorruptBlocksCount();
  }
  
  public long getRaidMissingBlocksCount() {
    return neededReplications.getRaidMissingBlocksCount();
  }
  
  public String getRaidHttpUrl() {
    return this.getConf().get(
        FSConstants.DFS_RAIDNODE_HTTP_ADDRESS_KEY, null);
  }

  long[] getStats() throws IOException {
    synchronized (heartbeats) {
      return new long[]{this.capacityTotal, this.capacityUsed,
        this.capacityRemaining,
        this.underReplicatedBlocksCount,
        this.corruptReplicaBlocksCount,
        getMissingBlocksCount(),
        this.capacityNamespaceUsed};
    }
  }

  /**
   * Total raw bytes including non-dfs used space.
   */
  public long getCapacityTotal() {
    synchronized (heartbeats) {
      return this.capacityTotal;
    }
  }

  /**
   * Total used space by data nodes
   */
  public long getCapacityUsed() {
    synchronized (heartbeats) {
      return this.capacityUsed;
    }
  }

  /**
   * Total used space by data nodes as percentage of total capacity
   */
  public float getCapacityUsedPercent() {
    synchronized (heartbeats) {
      if (capacityTotal <= 0) {
        return 100;
      }

      return ((float) capacityUsed * 100.0f) / (float) capacityTotal;
    }
  }
  
  /**
   * Total namespace used space by data nodes
   */
  public long getCapacityNamespaceUsed() {
    synchronized (heartbeats) {
      return this.capacityNamespaceUsed;
    }
  }

  /**
   * Total used space by data nodes as percentage of total capacity
   */
  public float getCapacityNamespaceUsedPercent() {
    synchronized (heartbeats) {
      if (capacityTotal <= 0) {
        return 100;
      }

      return ((float) capacityNamespaceUsed * 100.0f) / (float) capacityTotal;
    }
  }

  /**
   * Total used space by data nodes for non DFS purposes such
   * as storing temporary files on the local file system
   */
  public long getCapacityUsedNonDFS() {
    long nonDFSUsed = 0;
    synchronized (heartbeats) {
      nonDFSUsed = capacityTotal - capacityRemaining - capacityUsed;
    }
    return nonDFSUsed < 0 ? 0 : nonDFSUsed;
  }

  /**
   * Total non-used raw bytes.
   */
  public long getCapacityRemaining() {
    synchronized (heartbeats) {
      return this.capacityRemaining;
    }
  }

  /**
   * Total remaining space by data nodes as percentage of total capacity
   */
  public float getCapacityRemainingPercent() {
    synchronized (heartbeats) {
      if (capacityTotal <= 0) {
        return 0;
      }

      return ((float) capacityRemaining * 100.0f) / (float) capacityTotal;
    }
  }

  /**
   * Total number of connections.
   */
  public int getTotalLoad() {
    synchronized (heartbeats) {
      return this.totalLoad;
    }
  }

  int getNumberOfDatanodes(DatanodeReportType type) {
    return getDatanodeListForReport(type).size();
  }
 
  ArrayList<DatanodeDescriptor> getDatanodeListForReport(
    DatanodeReportType type) {
    try {
      HashSet<String> mustList = new HashSet<String>();
      synchronized (hostsReader) {
        boolean listLiveNodes = type == DatanodeReportType.ALL ||
          type == DatanodeReportType.LIVE;
        boolean listDeadNodes = type == DatanodeReportType.ALL ||
          type == DatanodeReportType.DEAD;
  
        if (listDeadNodes) {
          //first load all the nodes listed in include and exclude files.
          for (Iterator<String> it = hostsReader.getHosts().iterator();
               it.hasNext();) {
            mustList.add(it.next());
          }
          for (Iterator<String> it = hostsReader.getExcludedHosts().iterator();
               it.hasNext();) {
            mustList.add(it.next());
          }
        }
  
        ArrayList<DatanodeDescriptor> nodes = null;
  
        synchronized (datanodeMap) {
          nodes = new ArrayList<DatanodeDescriptor>(datanodeMap.size() +
            mustList.size());
  
          for (Iterator<DatanodeDescriptor> it = datanodeMap.values().iterator();
               it.hasNext();) {
            DatanodeDescriptor dn = it.next();
            boolean isDead = isDatanodeDead(dn);
            //Remove any form of the this datanode in include/exclude lists.
            mustList.remove(dn.getName());
            mustList.remove(dn.getHost());
            mustList.remove(dn.getHostName());
            mustList.remove(dn.getHostName() + ":" + dn.getPort());
            
            // for mixed situation in which includes/excludes have names
            // and datanodes are using IPs
            String hostname = getHostNameForIp(dn.getHost());
            if (hostname != null) {
              mustList.remove(hostname);
              mustList.remove(hostname + ".");
            }
            if (!isDead && listLiveNodes && this.inHostsList(dn, null)) {
              nodes.add(dn);
            } else if (isDead && listDeadNodes &&
                (inHostsList(dn, null) || inExcludedHostsList(dn, null))) {
              nodes.add(dn);
            }
          }
        }
  
        if (listDeadNodes) {
          for (Iterator<String> it = mustList.iterator(); it.hasNext();) {
            DatanodeDescriptor dn =
              new DatanodeDescriptor(new DatanodeID(it.next()));
            //This node has never been alive, set starttime to the 0
            dn.setStartTime(0);
            dn.setLastUpdate(0);
            nodes.add(dn);
          }
        }
        return nodes;
      }
    } catch (Exception e) {
      return new ArrayList<DatanodeDescriptor>();
    }
  }

  public DatanodeInfo[] datanodeReport(DatanodeReportType type
  ) throws AccessControlException {
    // needs to hold FSNamesystem.lock()
    readLock();
    try {
      checkSuperuserPrivilege();
      return getDatanodes(type);
    } finally {
      readUnlock();
    }
  }
  
  /**
   * Get all the datanodes of the given type
   * @param type type of the datanodes
   * @return a list of datanodes
   */
  DatanodeInfo[] getDatanodes(DatanodeReportType type) {
    ArrayList<DatanodeDescriptor> results = getDatanodeListForReport(type);
    DatanodeInfo[] arr = new DatanodeInfo[results.size()];
    for (int i = 0; i < arr.length; i++) {
      arr[i] = new DatanodeInfo(results.get(i));
    }
    return arr;
  }

  /**
   * Save namespace image.
   * This will save current namespace into fsimage file and empty edits file.
   * Requires superuser privilege and safe mode.
   *
   * @param force if true, then the namenode need not already be in safemode.
   * @throws AccessControlException if superuser privilege is violated.
   * @throws IOException            if
   */
  void saveNamespace(boolean force, boolean uncompressed) throws AccessControlException, IOException {
    LOG.info("Saving namespace");
    writeLock();
    try {
      checkSuperuserPrivilege();
      if(!force && !isInSafeMode()) {
        throw new IOException("Safe mode should be turned ON " +
          "in order to create namespace image.");
      }
      getFSImage().saveNamespace(uncompressed);
    } finally {
      writeUnlock();
    }
    LOG.info("Saving namespace - DONE");
  }

  /**
   */
  public void DFSNodesStatus(ArrayList<DatanodeDescriptor> live,
                                          ArrayList<DatanodeDescriptor> dead) {
    try {
      ArrayList<DatanodeDescriptor> results =
        getDatanodeListForReport(DatanodeReportType.ALL);
      for (Iterator<DatanodeDescriptor> it = results.iterator(); it.hasNext();) {
        DatanodeDescriptor node = it.next();
        if (!node.isAlive) {
          dead.add(node);
        } else {
          live.add(node);
        }
      }
    } catch (Exception e) {
      LOG.info("Exception: ", e);
    }
  }

  /**
   * Prints information about all datanodes.
   */
  private void datanodeDump(PrintWriter out) {
    readLock();
    try {
      synchronized (datanodeMap) {
        out.println("Metasave: Number of datanodes: " + datanodeMap.size());
        for (Iterator<DatanodeDescriptor> it = datanodeMap.values().iterator(); it.hasNext();) {
          DatanodeDescriptor node = it.next();
          out.println(node.dumpDatanode());
        }
      }
    } finally {
      readUnlock();
    }
  }

  /**
   * Start decommissioning the specified datanode.
   */
  void startDecommission(DatanodeDescriptor node)
    throws IOException {
    if (!node.isDecommissionInProgress() && !node.isDecommissioned()) {
      LOG.info("Start Decommissioning node " + node.getName() + " with " + 
          node.numBlocks() +  " blocks.");
      
      synchronized (heartbeats) {
        updateStats(node, false);
        node.startDecommission();
        updateStats(node, true);
      }
      if (((Monitor) dnthread.getRunnable()).startDecommision(node)) {
        node.setStartTime(now());
      }
    } else if (node.isDecommissionInProgress()) {
      if (((Monitor) dnthread.getRunnable()).startDecommision(node)) {
        node.setStartTime(now());
      }
    }
  }
  
  /**
   * Stop decommissioning the specified datanodes.
   */
  void stopDecommission(DatanodeDescriptor node)
    throws IOException {
    if ((node.isDecommissionInProgress() &&
      ((Monitor) dnthread.getRunnable()).stopDecommission(node)) ||
      node.isDecommissioned()) {
      LOG.info("Stop Decommissioning node " + node.getName());
      synchronized (heartbeats) {
        updateStats(node, false);
        node.stopDecommission();
        updateStats(node, true);
      }

      // Make sure we process over replicated blocks.
      writeLock();
      try {
        Iterator<BlockInfo> it = node.getBlockIterator();
        while (it.hasNext()) {
          Block b = it.next();
          if (countLiveNodes(b) > getReplication(b)) {
            overReplicatedBlocks.add(b);
          }
        }
      } finally {
        writeUnlock();
      }
    }
  }

  /**
   */
  public DatanodeInfo getDataNodeInfo(String name) {
    return datanodeMap.get(name);
  }

  /**
   * returns the namenode object that was used to initialize this namesystem
   */
  public NameNode getNameNode() {
    return this.nameNode;
  }

  /**
   * Whether or not we should fail on a txid mismatch.
   */
  public boolean failOnTxIdMismatch() {
    return this.nameNode.failOnTxIdMismatch();
  }

  /**
   * @deprecated use {@link NameNode#getNameNodeAddress()} instead.
   */
  @Deprecated
  public InetSocketAddress getDFSNameNodeAddress() {
    return nameNodeAddress;
  }

  /**
   */
  public Date getStartTime() {
    return new Date(systemStart);
  }

  short getMaxReplication() {
    return (short) maxReplication;
  }

  short getMinReplication() {
    return (short) minReplication;
  }

  short getDefaultReplication() {
    return (short) defaultReplication;
  }

  public void stallReplicationWork() {
    stallReplicationWork = true;
  }

  public void restartReplicationWork() {
    stallReplicationWork = false;
  }

  /**
   * A immutable object that stores the number of live replicas and
   * the number of decommissined Replicas.
   */
  static class NumberReplicas {
    private int liveReplicas;
    private int decommissionedReplicas;
    private int corruptReplicas;
    private int excessReplicas;

    NumberReplicas() {
      initialize(0, 0, 0, 0);
    }

    NumberReplicas(int live, int decommissioned, int corrupt, int excess) {
      initialize(live, decommissioned, corrupt, excess);
    }

    void initialize(int live, int decommissioned, int corrupt, int excess) {
      liveReplicas = live;
      decommissionedReplicas = decommissioned;
      corruptReplicas = corrupt;
      excessReplicas = excess;
    }

    int liveReplicas() {
      return liveReplicas;
    }

    int decommissionedReplicas() {
      return decommissionedReplicas;
    }

    int corruptReplicas() {
      return corruptReplicas;
    }

    int excessReplicas() {
      return excessReplicas;
    }
    
    int getTotal() {
      return liveReplicas + decommissionedReplicas + 
             corruptReplicas + excessReplicas;
    }
  }

  /**
   * Counts the number of nodes in the given list into active and
   * decommissioned counters.
   */
  private NumberReplicas countNodes(Block b,
                                    Iterator<DatanodeDescriptor> nodeIter) {
    int count = 0;
    int live = 0;
    int corrupt = 0;
    int excess = 0;
    Collection<DatanodeDescriptor> nodesCorrupt = corruptReplicas.getNodes(b);
    while (nodeIter.hasNext()) {
      DatanodeDescriptor node = nodeIter.next();
      if ((nodesCorrupt != null) && (nodesCorrupt.contains(node))) {
        corrupt++;
      } else if (node.isDecommissionInProgress() || node.isDecommissioned()) {
        count++;
      } else {
        Collection<Block> blocksExcess = initializedReplQueues ?
          excessReplicateMap.get(node.getStorageID()) : null;
        if (blocksExcess != null && blocksExcess.contains(b)) {
          excess++;
        } else {
          live++;
        }
      }
    }
    return new NumberReplicas(live, count, corrupt, excess);
  }

  /**
   * Counts the number of live nodes in the given list
   */
  private int countLiveNodes(Block b, Iterator<DatanodeDescriptor> nodeIter) {
    int live = 0;
    Collection<DatanodeDescriptor> nodesCorrupt = null;
    if (corruptReplicas.size() != 0) {
      nodesCorrupt = corruptReplicas.getNodes(b);
    }
    while (nodeIter.hasNext()) {
      DatanodeDescriptor node = nodeIter.next();
      if (((nodesCorrupt != null) && (nodesCorrupt.contains(node))) ||
        node.isDecommissionInProgress() || node.isDecommissioned()) {
        // do nothing
      } else {
        live++;
      }
    }
    return live;
  }
  
  /**
   * Counts the number of nodes in the given list into active and decommissioned
   * counters.
   */
  private int countCorruptNodes(Block b, 
                                    Iterator<DatanodeDescriptor> nodeIter) {
    int corrupt = 0;
    Collection<DatanodeDescriptor> nodesCorrupt = corruptReplicas.getNodes(b);
    while (nodeIter.hasNext()) {
      DatanodeDescriptor node = nodeIter.next();
      if ((nodesCorrupt != null) && (nodesCorrupt.contains(node))) {
        corrupt++;
      }
    }
    return corrupt;
  }

  /**
   * Return the number of nodes that are corrupt
   */
  int countCorruptNodes(Block b) {
    return countCorruptNodes(b, blocksMap.nodeIterator(b));
  }

  /**
   * Return the number of nodes that are live
   */
  public int countLiveNodes(Block b) {
    return countLiveNodes(b, blocksMap.nodeIterator(b));
  }

  /**
   * Return the number of nodes that are live and decommissioned.
   */
  NumberReplicas countNodes(Block b) {
    return countNodes(b, blocksMap.nodeIterator(b));
  }

  private void logBlockReplicationInfo(Block block, DatanodeDescriptor srcNode,
                                       NumberReplicas num) {
    int curReplicas = num.liveReplicas();
    int curExpectedReplicas = getReplication(block);
    INode fileINode = blocksMap.getINode(block);
    Iterator<DatanodeDescriptor> nodeIter = blocksMap.nodeIterator(block);
    StringBuffer nodeList = new StringBuffer();
    while (nodeIter.hasNext()) {
      DatanodeDescriptor node = nodeIter.next();
      nodeList.append(node.name);
      nodeList.append(" ");
    }
    FSNamesystem.LOG.info("Block: " + block + ", Expected Replicas: "
      + curExpectedReplicas + ", live replicas: " + curReplicas
      + ", corrupt replicas: " + num.corruptReplicas()
      + ", decommissioned replicas: " + num.decommissionedReplicas()
      + ", excess replicas: " + num.excessReplicas() + ", Is Open File: "
      + fileINode.isUnderConstruction() + ", Datanodes having this block: "
      + nodeList + ", Current Datanode: " + srcNode.name
      + ", Is current datanode decommissioning: "
      + srcNode.isDecommissionInProgress());
  }

  /**
   * Check if a block on srcNode has reached its replication factor or not
   *
   *@param status decomission status to update; null means no update
   * @param srcNode a datanode
   * @param block   a block
   * @param addToNeeded if the block needs to add to needReplication queue
   * @return block if not addToNeeded and the block needes to added later
   */
  BlockInfo isReplicationInProgress(final DecommissioningStatus status,
      final DatanodeDescriptor srcNode, final BlockInfo block,
      boolean addToNeeded) {
    INode fileINode = blocksMap.getINode(block);
    if (fileINode == null) {
      return null;
    }
    NumberReplicas num = countNodes(block);
    int curReplicas = num.liveReplicas();
    int curExpectedReplicas = getReplication(block);
    if (curExpectedReplicas > curReplicas) {
      if (status!= null) {
        //Log info about one block for this node which needs replication
        if (status.underReplicatedBlocks == 0) {
          logBlockReplicationInfo(block, srcNode, num);
        }
        status.underReplicatedBlocks++;
        if ((curReplicas == 0) && (num.decommissionedReplicas() > 0)) {
          status.decommissionOnlyReplicas++;
        }
        if (fileINode.isUnderConstruction()) {
          status.underReplicatedInOpenFiles++;
        }
      }
      if (!neededReplications.contains(block) &&
        pendingReplications.getNumReplicas(block) == 0) {
        //
        // These blocks have been reported from the datanode
        // after the startDecommission method has been executed. These
        // blocks were in flight when the decommissioning was started.
        //
        if (addToNeeded) {
          neededReplications.add(block, 
              curReplicas,
              num.decommissionedReplicas(),
              curExpectedReplicas);
        } else {
          return block;
        }
      }
    }
    return null;
  }
  
  /**
   * Best effort reverse DNS resolution.
   * Returns null on error.
   */
  private String getHostNameForIp(String ipAddr) {
    try {
      // this handles the case where the hostlist contains names
      // and the datanodes are using IPs
      InetAddress addr = InetAddress.getByName(ipAddr);
      return addr.getHostName();
    } catch (Exception e) { 
      // this can be safely ignored
    }
    return null;
  }

  /**
   * Keeps track of which datanodes/ipaddress are allowed to connect to the namenode.
   */
  private boolean inHostsList(DatanodeID node, String ipAddr) {
    Set<String> hostsList = hostsReader.getHosts();
    try {
      if (hostsList.isEmpty()
          || (ipAddr != null && hostsList.contains(ipAddr))
          || hostsList.contains(node.getHost())
          || hostsList.contains(node.getName())
          || ((node instanceof DatanodeInfo) && (hostsList.contains(((DatanodeInfo) node)
              .getHostName()) 
          || hostsList.contains(((DatanodeInfo) node).getHostName() + ":"
              + node.getPort()))) 
          // this is for the mixed mode in which the hostnames in the includes file
          // are followed with a "." and the datanodes are using IP addresses
          // during registration
          || (ipAddr != null && 
              hostsList.contains(InetAddress.getByName(ipAddr).getHostName() 
              + "."))) {
        return true;
      }
      
      // for several calls we pass ipAddr = null, but the datanode name
      // is an IP address, for this we need to do best effort reverse DNS 
      // resolution
      String host = getHostNameForIp(node.getHost());
      if (host != null) {
        return hostsList.contains(host + ".") || hostsList.contains(host);
      }
      
      // host is not in the list
      return false;
    } catch (UnknownHostException e) {
      LOG.error(e);
    }
    return false;
  }

  public boolean inExcludedHostsList(DatanodeID node, String ipAddr) {
    Set<String> excludeList = hostsReader.getExcludedHosts();
    try {
      if (
          (ipAddr != null && excludeList.contains(ipAddr)) ||
        excludeList.contains(node.getHost()) ||
        excludeList.contains(node.getName()) ||
        ((node instanceof DatanodeInfo) &&
          excludeList.contains(((DatanodeInfo) node).getHostName())) ||
          // this is for the mixed mode in which the hostnames in the excludes file
          // are followed with a "." and the datanodes are using IP addresses
          (ipAddr != null  && excludeList.contains(InetAddress.getByName(ipAddr).getHostName()
              + "."))) {
        return true;
      }
      
      // for several calls we pass ipAddr = null, but the datanode name
      // is an IP address, for this we need to do best effort reverse DNS 
      // resolution
      String host = getHostNameForIp(node.getHost());
      if (host != null) {
        return excludeList.contains(host + ".") || excludeList.contains(host);
      }
      
      // host is not in the list
      return false;
    } catch (UnknownHostException e) {
      LOG.error(e);
    }
    return false;
  }

  /**
   * Rereads the config to get hosts and exclude list file names.
   * Rereads the files to update the hosts and exclude lists.  It
   * checks if any of the hosts have changed states:
   * 1. Added to hosts  --> no further work needed here.
   * 2. Removed from hosts --> mark AdminState as decommissioned.
   * 3. Added to exclude --> start decommission.
   * 4. Removed from exclude --> stop decommission.
   */
  public void refreshNodes(Configuration conf) throws IOException {
    LOG.info("Refreshing nodes.");
    checkSuperuserPrivilege();
    // Reread the config to get dfs.hosts and dfs.hosts.exclude filenames.
    // Update the file names and refresh internal includes and excludes list
    if (conf == null) {
      conf = new Configuration();
    }
    hostsReader.updateFileNames(conf.get(FSConstants.DFS_HOSTS, ""),
      conf.get("dfs.hosts.exclude", ""), true);
    Set<String> includes = hostsReader.getNewIncludes();
    Set<String> excludes = hostsReader.getNewExcludes();
    Set<String> prevIncludes = hostsReader.getHosts();
    Set<String> prevExcludes = hostsReader.getExcludedHosts();
    // We need to syncrhonize on hostsReader here so that no thread reads from
    // hostsReader between the two updates to hostsReader in this function.
    synchronized (hostsReader) {
      hostsReader.switchFiles(includes, excludes);
      try {
        replicator.hostsUpdated();
      } catch (DefaultRackException e) {
      // We want to update the includes and excludes files only if hostsUpdated,
      // doesn't throw an exception. This avoids inconsitency between the
      // replicator and the fnamesystem especially for
      // BlockPlacementPolicyConfigurable.
        hostsReader.switchFiles(prevIncludes, prevExcludes);
        throw e;
      }
    }
    writeLock();
    try {
      for (Iterator<DatanodeDescriptor> it = datanodeMap.values().iterator();
           it.hasNext();) {
        DatanodeDescriptor node = it.next();
        // Check if not include.
        if (!inHostsList(node, null)) {
          node.setDisallowed(true);  // case 2.
        } else {
          if (inExcludedHostsList(node, null)) {
            startDecommission(node);   // case 3.
          } else {
            stopDecommission(node);   // case 4.
          }
        }
      }
    } finally {
      writeUnlock();
    }
    LOG.info("Refreshing nodes - DONE");
  }

  void finalizeUpgrade() throws IOException {
    checkSuperuserPrivilege();
    getFSImage().finalizeUpgrade();
  }

  /**
   * Checks if the node is not on the hosts list.  If it is not, then
   * it will be disallowed from registering.  
   */
  private boolean verifyNodeRegistration(DatanodeRegistration nodeReg, String ipAddr)
    throws IOException {
    assert (hasWriteLock());
    return inHostsList(nodeReg, ipAddr);
  }

  /**
   * Decommission the node if it is in exclude list.
   */
  private void checkDecommissioning(DatanodeDescriptor nodeReg, String ipAddr) 
    throws IOException {
    // If the registered node is in exclude list, then decommission it
    if (inExcludedHostsList(nodeReg, ipAddr)) {
      startDecommission(nodeReg);
    }
  }

  /**
   * Get data node by storage ID.
   *
   * @param nodeID
   * @return DatanodeDescriptor or null if the node is not found.
   * @throws IOException
   */
  public DatanodeDescriptor getDatanode(DatanodeID nodeID) throws IOException {
    UnregisteredDatanodeException e = null;
    DatanodeDescriptor node = datanodeMap.get(nodeID.getStorageID());
    if (node == null) {
      return null;
    }
    if (!node.getName().equals(nodeID.getName())) {
      e = new UnregisteredDatanodeException(nodeID, node);
      NameNode.stateChangeLog.fatal("BLOCK* NameSystem.getDatanode: "
        + e.getLocalizedMessage());
      throw e;
    }
    return node;
  }

  /**
   * Stop at and return the datanode at index (used for content browsing)
   */
  @Deprecated
  private DatanodeDescriptor getDatanodeByIndex(int index) {
    int i = 0;
    for (DatanodeDescriptor node : datanodeMap.values()) {
      if (i == index) {
        return node;
      }
      i++;
    }
    return null;
  }

  @Deprecated
  public String randomDataNode() {
    int size = datanodeMap.size();
    int index = 0;
    if (size != 0) {
      index = r.nextInt(size);
      for (int i = 0; i < size; i++) {
        DatanodeDescriptor d = getDatanodeByIndex(index);
        if (d != null && !d.isDecommissioned() && !isDatanodeDead(d) &&
          !d.isDecommissionInProgress()) {
          return d.getHost() + ":" + d.getInfoPort();
        }
        index = (index + 1) % size;
      }
    }
    return null;
  }

  public DatanodeDescriptor getRandomDatanode() {
    return replicator.chooseTarget("", 1, null,
      new ArrayList<DatanodeDescriptor>(), 0)[0];
  }

  /**
   * Current system time.
   *
   * @return current time in msec.
   */
  static long now() {
    return System.currentTimeMillis();
  }

  boolean setSafeMode(SafeModeAction action) throws IOException {
    if (action != SafeModeAction.SAFEMODE_GET) {
      checkSuperuserPrivilege();
      switch (action) {
        case SAFEMODE_LEAVE: // leave safe mode
          if (!manualOverrideSafeMode) {
            leaveSafeMode(false);
          } else {
            LOG.warn("Leaving safemode is not allowed. " + manualOverrideSafeMode);
          }
          break;
        case SAFEMODE_ENTER: // enter safe mode
          enterSafeMode();
          break;
        case SAFEMODE_INITQUEUES:
          initializeReplQueues();
          break;
        case SAFEMODE_PREP_FAILOVER:
          // do nothing
          break;
      }
    }
    return isInSafeMode();
  }

  /**
   * Allow the ability to let an external API manually override exiting safemode
   */
  void setSafeModeManualOverride(boolean flag) {
    this.manualOverrideSafeMode = flag;
  }

  /**
   * Check whether the name node is in safe mode.
   *
   * @return true if safe mode is ON, false otherwise
   */
  public boolean isInSafeMode() {
    readLock();
    try {
      return isInSafeModeInternal();
    } finally {
      readUnlock();
    }
  }
  
  private boolean isInSafeModeInternal() {
    try {
      return safeMode != null && safeMode.isOn();
    } catch (Exception e) {
      // safemode could be not null and become null in between
      // so we should return false, the other way is safe
      return false;
    }
  }

  /**
   * Check whether replication queues are populated.
   */
  boolean isPopulatingReplQueues() {
    readLock();
    try {
      return isPopulatingReplQueuesInternal();
    } finally {
      readUnlock();
    }
  }
  
  /**
   * Check whether replication queues are populated.
   * Raw check with no write lock.
   */
  boolean isPopulatingReplQueuesInternal() {
    return (!isInSafeModeInternal() || initializedReplQueues);
  }

  /**
   * Check whether the name node is in startup mode.
   */
  boolean isInStartupSafeMode() {
    readLock();
    try {
      return safeMode != null && safeMode.isOn() && !safeMode.isManual();
    } finally {
      readUnlock();
    }
  }

  /**
   * Increment number of blocks that reached minimal replication.
   *
   * @param replication current replication
   * @param skipCheck if true the safemode will not be checked -
   *        used for processing initial block reports to skip the 
   *        check for every block - at the end checkSafeMode() must be called
   */
  void incrementSafeBlockCount(int replication, boolean skipCheck) {
    if (safeMode != null && safeMode.isOn()) {
      if ((int) replication == minReplication) {
        this.blocksSafe++;
        if(!skipCheck) {
          safeMode.checkMode();
        }
      }
    }
  }
  
  /**
   * Unconditionally increments safe block count. 
   * This should be used ONLY for initial block reports.
   */
  void incrementSafeBlockCount(int count) {
    this.blocksSafe += count;
  }
  
  /**
   * Check the safemode. Used after processing a batch of blocks
   * to avoid multiple check() calls.
   */
  void checkSafeMode() {
    writeLock();
    try {
      if (safeMode != null && safeMode.isOn()) {
        safeMode.checkMode();
      }
    } finally {
      writeUnlock();
    }
  }

  /**
   * This method is invoked just before removing a block from the
   * {@link #blocksMap}. It is used to correctly update the safe block count
   * when a block is removed from the {@link #blocksMap}. If the current
   * replication for the block is greater than {@link #minReplication} this
   * means that the safe block count has not already been decremented for this
   * block and hence we should decrement the safe block count.
   * 
   * @param b
   *          the block that has to be removed.
   */
  void decrementSafeBlockCountForBlockRemoval(Block b) {
    if (safeMode != null && safeMode.isOn()) {
      NumberReplicas num = countNodes(b);
      // Count decommissioned replicas as well since you can still read from
      // them.
      int replication = num.liveReplicas() + num.decommissionedReplicas();
      if (replication >= minReplication) {
        this.blocksSafe--;
        safeMode.checkMode();
      }
    }
  }

  /**
   * Decrement number of blocks that reached minimal replication.
   */
  void decrementSafeBlockCount(Block b) {
    if (safeMode != null && safeMode.isOn()) {
      NumberReplicas num = countNodes(b);
      // Count decommissioned replicas as well since you can still read from
      // them.
      int replication = num.liveReplicas() + num.decommissionedReplicas();
      if (replication == minReplication - 1) {
        this.blocksSafe--;
        safeMode.checkMode();
      }
    }
  }

  /**
   * Retrieve the total number of safe blocks in the file system.
   * 
   * @return a long denoting the total number of safe blocks
   */
  public long getSafeBlocks() {
    return this.blocksSafe;
  }

  public SafeModeInfo getSafeModeInstance() {
    return this.safeMode;
  }

  /**
   * Ratio of the number of safe blocks to the total number of blocks.
   */
  public double getSafeBlockRatio() {
    long blockTotal = getBlocksTotal();
    if (blockTotal == 0) {
      return 1;
    }
    return (blockTotal == this.blocksSafe ? 1 : (double) this.blocksSafe
        / (double) blockTotal);
  }

  /**
   * Get the total number of blocks in the system.
   */
  public long getBlocksTotal() {
    return blocksMap.size();
  }

  /**
   * Enter safe mode manually.
   *
   * @throws IOException
   */
  void enterSafeMode() throws IOException {
    writeLock();
    try {
      // Ensure that any concurrent operations have been fully synced
      // before entering safe mode. This ensures that the FSImage
      // is entirely stable on disk as soon as we're in safe mode.
      getEditLog().logSyncAll();
      if (!isInSafeMode()) {
        safeMode = SafeModeUtil.getInstance(this);
        safeMode.setManual();
        return;
      }
      safeMode.setManual();
      getEditLog().logSyncAll();
      NameNode.stateChangeLog.info("STATE* Safe mode is ON. "
        + safeMode.getTurnOffTip());
    } finally {
      writeUnlock();
    }
  }

  /**
   * Leave safe mode.
   *
   * @throws IOException
   */
  void leaveSafeMode(boolean checkForUpgrades) throws SafeModeException {
    writeLock();
    try {
      if (!isInSafeMode()) {
        NameNode.stateChangeLog.info("STATE* Safe mode is already OFF.");
        return;
      }
      if (getDistributedUpgradeState()) {
        throw new SafeModeException("Distributed upgrade is in progress",
          safeMode);
      }
      safeMode.leave(checkForUpgrades);
      safeMode = null;
    } finally {
      writeUnlock();
    }
  }
  
  /**
   * Manually initialize replication queues when in safemode.
   */
  void initializeReplQueues() throws SafeModeException {
    writeLock();
    try {
      if (isPopulatingReplQueues()) {
        NameNode.stateChangeLog.info("STATE* Safe mode is already OFF."
            + " Replication queues are initialized");
        return;
      }
      safeMode.initializeReplicationQueues();
    } finally {
      writeUnlock();
    }
  }

  String getSafeModeTip() {
    if (!isInSafeMode()) {
      return "";
    }
    return safeMode.getTurnOffTip();
  }

  /**
   * @return The most recent transaction ID that has been synced to
   * persistent storage.
   * @throws IOException
   */
  public long getTransactionID() throws IOException {
    return getEditLog().getLastSyncedTxId();
  }

  CheckpointSignature rollEditLog() throws IOException {
    writeLock();
    try {
      if (isInSafeMode()) {
        throw new SafeModeException("Checkpoint not created",
          safeMode);
      }
      LOG.info("Roll Edit Log from " + Server.getRemoteAddress() +
        " starting stransaction id " + getFSImage().getEditLog().getCurSegmentTxId() +
        " ending transaction id: " + (getFSImage().getEditLog().getLastWrittenTxId() + 1));
      return getFSImage().rollEditLog();
    } finally {
      writeUnlock();
    }
  }
  
  CheckpointSignature getCheckpointSignature() throws IOException {
    writeLock();
    try {
      if (isInSafeMode()) {
        throw new SafeModeException(
            "Safemode is ON. Cannot obtain checkpoint signature", safeMode);
      }
      return new CheckpointSignature(getFSImage());
    } finally {
      writeUnlock();
    }
  }

  /**
   * @param newImageSignature the signature of the new image
   */
  void rollFSImage(CheckpointSignature newImageSignature) throws IOException {
    writeLock();
    try {
      if (isInSafeMode()) {
        throw new SafeModeException("Checkpoint not created",
          safeMode);
      }
      LOG.info("Roll FSImage from " + Server.getRemoteAddress());
      getFSImage().rollFSImage(newImageSignature);
    } finally {
      writeUnlock();
    }
    // Now that we have a new checkpoint, we might be able to
    // remove some old ones.          
    getFSImage().purgeOldStorage();
  }

  /**
   * Returns whether the given block is one pointed-to by a file.
   */
  private boolean isValidBlock(Block b) {
    return (blocksMap.getINode(b) != null);
  }

  // Distributed upgrade manager
  final UpgradeManagerNamenode upgradeManager = new UpgradeManagerNamenode(this);

  UpgradeStatusReport distributedUpgradeProgress(UpgradeAction action
  ) throws IOException {
    return upgradeManager.distributedUpgradeProgress(action);
  }

  UpgradeCommand processDistributedUpgradeCommand(UpgradeCommand comm)
    throws IOException {
    return upgradeManager.processUpgradeCommand(comm);
  }

  int getDistributedUpgradeVersion() {
    return upgradeManager.getUpgradeVersion();
  }

  UpgradeCommand getDistributedUpgradeCommand() throws IOException {
    return upgradeManager.getBroadcastCommand();
  }

  boolean getDistributedUpgradeState() {
    return upgradeManager.getUpgradeState();
  }

  short getDistributedUpgradeStatus() {
    return upgradeManager.getUpgradeStatus();
  }

  boolean startDistributedUpgradeIfNeeded() throws IOException {
    return upgradeManager.startUpgrade();
  }

  PermissionStatus createFsOwnerPermissions(FsPermission permission) {
    return new PermissionStatus(fsOwner.getUserName(), supergroup, permission);
  }

  private FSPermissionChecker checkOwner(String path, INode[] inodes)
    throws AccessControlException {
    return checkPermission(path, inodes, true, null, null, null, null);
  }

  private FSPermissionChecker checkPathAccess(String path, INode[] inodes, FsAction access
  ) throws AccessControlException {
    return checkPermission(path, inodes, false, null, null, access, null);
  }

  private FSPermissionChecker checkParentAccess(String path, INode[] inodes, FsAction access
  ) throws AccessControlException {
    return checkPermission(path, inodes, false, null, access, null, null);
  }

  private FSPermissionChecker checkAncestorAccess(String path, INode[] inodes, FsAction access
  ) throws AccessControlException {
    return checkPermission(path, inodes, false, access, null, null, null);
  }

  private FSPermissionChecker checkTraverse(String path, INode[] inodes
  ) throws AccessControlException {
    return checkPermission(path, inodes, false, null, null, null, null);
  }

  void checkSuperuserPrivilege() throws AccessControlException {
    if (isPermissionEnabled) {
      try {
        FSPermissionChecker.checkSuperuserPrivilege(fsOwner, supergroup);
      } catch (AccessControlException e) {
        if (this.permissionAuditOnly) {
          // do not throw the exception, we would like to only log.
          LOG.warn("PermissionAudit superuser failed for user: " + fsOwner +
            " group:" + supergroup);
        } else {
          throw e;
        }
      }
    }
  }

  /**
   * Check whether current user have permissions to access the path.
   * For more details of the parameters, see
   * {@link FSPermissionChecker#checkPermission(String, INodeDirectory, boolean, FsAction, FsAction, FsAction, FsAction)}.
   */
  private FSPermissionChecker checkPermission(String path, INode[] inodes, boolean doCheckOwner,
                                              FsAction ancestorAccess, FsAction parentAccess, FsAction access,
                                              FsAction subAccess)
    throws AccessControlException {
    boolean permissionCheckFailed = false;
    FSPermissionChecker pc = new FSPermissionChecker(
      fsOwner.getUserName(), supergroup, this.getCurrentUGI());
    if (!pc.isSuper) {
      dir.waitForReady();
      readLock();
      try {
        pc.checkPermission(path, inodes, doCheckOwner,
          ancestorAccess, parentAccess, access, subAccess);
      } catch (AccessControlException e) {
        if (!this.permissionAuditOnly) {
          throw e;
        }
        permissionCheckFailed = true;
      } finally {
        readUnlock();
      }
    }
    if (permissionCheckFailed) {
      // do not throw the exception, we would like to only log.
      LOG.warn("PermissionAudit failed on " + path +
        " checkOwner:" + doCheckOwner +
        " ancestor:" + ancestorAccess +
        " parent:" + parentAccess +
        " access:" + access +
        " subaccess:" + subAccess);
    }
    return pc;
  }

  /**
   * Check to see if we have exceeded the limit on the number
   * of inodes.
   */
  void checkFsObjectLimit() throws IOException {
    if (maxFsObjects != 0 &&
      maxFsObjects <= dir.totalInodes() + getBlocksTotal()) {
      throw new IOException("Exceeded the configured number of objects " +
        maxFsObjects + " in the filesystem.");
    }
  }

  /**
   * Get the total number of objects in the system.
   */
  long getMaxObjects() {
    return maxFsObjects;
  }

  public long getFilesAndDirectoriesTotal() {
    return this.dir.totalInodes();
  }

  public long getDiskSpaceTotal() {
    return this.dir.totalDiskSpace();
  }
  
  public long getPendingReplicationBlocks() {
    return pendingReplicationBlocksCount;
  }

  public long getUnderReplicatedBlocks() {
    return underReplicatedBlocksCount;
  }

  public int getNumDeadMonitoringThread() {
    int numDeadMonitoringThread = 0;
    if (hbthread != null && hbthread.isAlive() == false) {
      numDeadMonitoringThread += 1;
    }
    if (lmthread != null && lmthread.isAlive() == false) {
      numDeadMonitoringThread += 1;
    }
    if (underreplthread != null && underreplthread.isAlive() == false) {
      numDeadMonitoringThread += 1;
    }
    if (overreplthread != null && overreplthread.isAlive() == false) {
      numDeadMonitoringThread += 1;
    }
    return numDeadMonitoringThread;
  }

  /**
   * Return number of under-replicatedBlocks excluding missing/corrupt blocks
   */
  public long getNonCorruptUnderReplicatedBlocks() {
    return neededReplications.getNonCorruptUnderReplicatedBlocksCount();
  }

  /**
   * Returns number of blocks with corrupt replicas
   */
  public long getCorruptReplicaBlocks() {
    return corruptReplicaBlocksCount;
  }

  public long getScheduledReplicationBlocks() {
    return scheduledReplicationBlocksCount;
  }

  public long getPendingDeletionBlocks() {
    return pendingDeletionBlocksCount;
  }

  public long getExcessBlocks() {
    return excessBlocksCount;
  }

  public int getBlockCapacity() {
    readLock();
    try {
      return blocksMap.getCapacity();
    } finally {
      readUnlock();
    }
  }

  public long getNumInvalidFilePathOperations() {
    return numInvalidFilePathOperations;
  }

  public String getFSState() {
    return isInSafeMode() ? "safeMode" : "Operational";
  }

  private ObjectName mbeanName;
  private ObjectName versionBeanName;
  private ObjectName namenodeMXBeanName;

  /**
   * Register the FSNamesystem MBean using the name
   * "hadoop:service=NameNode,name=FSNamesystemState"
   */
  void registerMBean(Configuration conf) {
    // We wrap to bypass standard mbean naming convention.
    // This wraping can be removed in java 6 as it is more flexible in
    // package naming for mbeans and their impl.
    StandardMBean bean;
    try {
      versionBeanName = VersionInfo.registerJMX("NameNode");
      myFSMetrics = new FSNamesystemMetrics(conf, this);
      bean = new StandardMBean(this, FSNamesystemMBean.class);
      mbeanName = MBeanUtil.registerMBean("NameNode", "FSNamesystemState", bean);
    } catch (NotCompliantMBeanException e) {
      e.printStackTrace();
    }

    LOG.info("Registered FSNamesystemStatusMBean");
  }

  /**
   * get FSNamesystemMetrics
   */
  final public FSNamesystemMetrics getFSNamesystemMetrics() {
    return myFSMetrics;
  }

  /**
   * shutdown FSNamesystem
   */
  public void shutdown() {
    if (mbeanName != null) {
      MBeanUtil.unregisterMBean(mbeanName);
    }
    if (versionBeanName != null) {
      MBeanUtil.unregisterMBean(versionBeanName);
    }
    if (namenodeMXBeanName != null) {
      MBeanUtil.unregisterMBean(namenodeMXBeanName);
    }
  }

  /**
   * Sets the generation stamp for this filesystem
   */
  public void setGenerationStamp(long stamp) {
    generationStamp.setStamp(stamp);
  }

  /**
   * Gets the generation stamp for this filesystem
   */
  public long getGenerationStamp() {
    return generationStamp.getStamp();
  }

  /**
   * Increments, logs and then returns the stamp. This is used only for testing.
   */
  public long nextGenerationStampForTesting() {
    return nextGenerationStamp();
  }

  /**
   * Increments, logs and then returns the stamp
   */
  private long nextGenerationStamp() {
    long gs = generationStamp.nextStamp();
    getEditLog().logGenerationStamp(gs);
    return gs;
  }

  /**
   * Verifies that the block is associated with a file that has a lease.
   * Increments, logs and then returns the stamp
   *
   * @param block block
   * @param fromNN if it is for lease recovery initiated by NameNode
   * @return a new generation stamp
   */
  public long nextGenerationStampForBlock(Block block, boolean fromNN) throws IOException {
    writeLock();
    try {
      if (isInSafeMode()) {
        throw new SafeModeException("Cannot get nextGenStamp for " + block, safeMode);
      }
      Block blockWithWildcardGenstamp = new Block(block.getBlockId());
      BlockInfo storedBlock = blocksMap.getStoredBlock(blockWithWildcardGenstamp);
      if (storedBlock == null) {
        String msg = block + " is already commited, storedBlock == null.";
        LOG.info(msg);
        throw new BlockAlreadyCommittedException(msg);
      }
      INodeFile fileINode = storedBlock.getINode();
      if (!fileINode.isUnderConstruction()) {
        String msg = block + " is already commited, !fileINode.isUnderConstruction().";
        LOG.info(msg);
        throw new BlockAlreadyCommittedException(msg);
      }
      // Disallow client-initiated recovery once
      // NameNode initiated lease recovery starts
      String path = null;
      try {
        path = fileINode.getFullPathName();
      } catch (IOException ioe) {
        throw (BlockAlreadyCommittedException)
          new BlockAlreadyCommittedException(
              block + " is already deleted").initCause(ioe);
      }
      if (!fromNN && HdfsConstants.NN_RECOVERY_LEASEHOLDER.equals(
          leaseManager.getLeaseByPath(path).getHolder())) {
        String msg = block +
          "is being recovered by NameNode, ignoring the request from a client";
        LOG.info(msg);
        throw new IOException(msg);
      }
      if (!((INodeFileUnderConstruction) fileINode).setLastRecoveryTime(now())) {
        String msg = block + " is being recovered, ignoring this request.";
        LOG.info(msg);
        throw new IOException(msg);
      }
      return nextGenerationStamp();
    } finally {
      writeUnlock();
    }
  }

  // rename was successful. If any part of the renamed subtree had
  // files that were being written to, update with new filename.
  //

  void changeLease(String src, String dst, HdfsFileStatus dinfo)
    throws IOException {
    String overwrite;
    String replaceBy;

    boolean destinationExisted = true;
    if (dinfo == null) {
      destinationExisted = false;
    }

    if (destinationExisted && dinfo.isDir()) {
      Path spath = new Path(src);
      String srcParent = spath.getParent().toString();
      overwrite = Path.SEPARATOR.equals(srcParent) ? srcParent 
                                                   : srcParent + Path.SEPARATOR;
      replaceBy = dst.endsWith(Path.SEPARATOR) ? dst : dst + Path.SEPARATOR;
    } else {
      overwrite = src;
      replaceBy = dst;
    }

    leaseManager.changeLease(src, dst, overwrite, replaceBy);
  }

  /**
   * Serializes leases.
   */
  void saveFilesUnderConstruction(SaveNamespaceContext ctx, DataOutputStream out) throws IOException {
    synchronized (leaseManager) {
      int pathsToSave = 0;
      Iterator<Lease> itrl = leaseManager.getSortedLeases().iterator();
      while (itrl.hasNext()) {
        Lease lease = itrl.next();
        for (String path : lease.getPaths()) {
          // verify that path exists in namespace
          INode node = dir.getFileINode(path);
          if (node != null && node.isUnderConstruction()) {
            pathsToSave++;
          } else if (node == null) {
            // ignore the path and continue.
            String msg = "saveLeases - counting - found path " + path
                + " but no matching entry in namespace.";
            LOG.warn(msg);
            continue;
          } else {
            throw new IOException("saveLeases found path " + path
                + " but is not under construction.");
          }
        }
      }
      
      if (pathsToSave != leaseManager.countPath()) {
        LOG.warn("Number of leases mismatch: " + pathsToSave
            + " are valid, lease manager indicated: "
            + leaseManager.countPath());
      }
      
      out.writeInt(pathsToSave); // write the size
      int pathsSaved = 0;

      LightWeightLinkedSet<Lease> sortedLeases = leaseManager.getSortedLeases();
      Iterator<Lease> itr = sortedLeases.iterator();
      while (itr.hasNext()) {
        ctx.checkCancelled();
        Lease lease = itr.next();
        for (String path : lease.getPaths()) {
          // verify that path exists in namespace
          INode node = dir.getFileINode(path);
          if (node == null) {
            // ignore the path and continue.
            String msg = "saveLeases found path " + path + " but no matching entry in namespace.";
            LOG.warn(msg);
            continue;
          }
          if (!node.isUnderConstruction()) {
            throw new IOException("saveLeases found path " + path +
              " but is not under construction.");
          }
          INodeFileUnderConstruction cons = (INodeFileUnderConstruction) node;
          FSImageSerialization.writeINodeUnderConstruction(out, cons, path);
          pathsSaved++;
        }
      }
      if (pathsSaved != pathsToSave) {
        String msg = "Saved paths: " + pathsSaved
            + " is not equal to what we thought we would save: " + pathsToSave;
        LOG.error(msg);
        throw new IOException(msg);
      }
    }
  }

  /**
   * Return an iterator over the set of blocks for which there are no replicas.
   */
  BlockIterator getCorruptReplicaBlockIterator() {
    return neededReplications
      .iterator(UnderReplicatedBlocks.QUEUE_WITH_CORRUPT_BLOCKS);
  }

  public static class CorruptFileBlockInfo {
    String path;
    Block block;

    public CorruptFileBlockInfo(String p, Block b) {
      path = p;
      block = b;
    }

    public String toString() {
      return block.getBlockName() + "\t" + path;
    }

    public String getPath() {
      return path;
    }
  }
  
  /**
   * @return Minutes in upgrade time
   */
  public int getUpgradeTime() {
    if (0 == upgradeStartTime) {
      return 0;
    }

    long curTime = now();
    if (curTime <= upgradeStartTime) {
      return 0;
    }

    return (int) ((curTime - upgradeStartTime) / (1000 * 60));
  }

  public void setUpgradeStartTime(final long startTime) {
    upgradeStartTime = startTime;
  }

  /**
   * @param path Restrict corrupt files to this portion of namespace.
   * @param startBlockAfter Support for continuation; the set of files we return
   *  back is ordered by blockid; startBlockAfter tells where to start from
   * @return a list in which each entry describes a corrupt file/block
   * @throws AccessControlException
   * @throws IOException
   */
  Collection<CorruptFileBlockInfo>
    listCorruptFileBlocks(String path,
                          String[] cookieTab)
    throws IOException {
    return listCorruptFileBlocks(path, cookieTab, false);
  }

  /**
   * @param path Restrict corrupt files to this portion of namespace.
   * @param startBlockAfter Support for continuation; the set of files we return
   *  back is ordered by blockid; startBlockAfter tells where to start from
   * @param decommissioningOnly if set the blocks returned will be the ones that
   *  only have replicas on the nodes that are being decommissioned
   * @return a list in which each entry describes a corrupt file/block
   * @throws AccessControlException
   * @throws IOException
   */
  Collection<CorruptFileBlockInfo>
    listCorruptFileBlocks(String path,
                          String[] cookieTab,
                          boolean decommissioningOnly) 
    throws IOException {
    readLock();
    synchronized (neededReplications) {
      try {
        if (!isPopulatingReplQueues()) {
          throw new IOException("Cannot run listCorruptFileBlocks because "
              + "replication queues have not been initialized.");
        }

        // print a limited # of corrupt files per call
        int count = 0;
        ArrayList<CorruptFileBlockInfo> corruptFiles = 
          new ArrayList<CorruptFileBlockInfo>();

        BlockIterator blkIterator = null;
        if (decommissioningOnly) {
          blkIterator = neededReplications.iterator(0);
        } else {
          blkIterator = getCorruptReplicaBlockIterator();
        }
        
        if (cookieTab == null) {
          cookieTab = new String[] { null };
        }
        int skip = getIntCookie(cookieTab[0]);
        for(int i = 0; i < skip && blkIterator.hasNext(); i++) {
          blkIterator.next();
        }
        
        while (blkIterator.hasNext()) {
          Block blk = blkIterator.next();
          INode inode = blocksMap.getINode(blk);
          skip++;
          if (inode != null) {
            try {
              String src = FSDirectory.getFullPathName(inode);
              if (src != null && src.startsWith(path)) {
                NumberReplicas num = countNodes(blk);
                if (num.liveReplicas == 0) {
                  if (decommissioningOnly && num.decommissionedReplicas > 0 ||
                      !decommissioningOnly && num.decommissionedReplicas == 0) {
                    corruptFiles.add(new CorruptFileBlockInfo(src, blk));
                    count++;
                    if (count >= maxCorruptFilesReturned)
                      break;
                  }
                }
              }
            } catch (IOException ioe) {
              // the node may have already been deleted; ingore it
              LOG.info("Invalid inode", ioe);
            }
          }
        }
        cookieTab[0] = String.valueOf(skip);
        LOG.info("list corrupt file blocks under " + path  + ": " + count);
        return corruptFiles;
      } finally {
        readUnlock();
      }
    }
  }
  
  public static int getIntCookie(String cookie){
    int c;
    if(cookie == null){
      c = 0;
    } else {
      try{
        c = Integer.parseInt(cookie);
      }catch (NumberFormatException e) {
        c = 0;
      }
    }
    c = Math.max(0, c);
    return c;
  }

  void setPersistBlocks(boolean persistBlocks) {
    this.persistBlocks = persistBlocks;
  }

  boolean getPersistBlocks() {
    return persistBlocks;
  }

  void setPermissionAuditLog(boolean permissionAuditOnly) {
    this.permissionAuditOnly = permissionAuditOnly;
  }

  boolean getPermissionAuditLog() {
    return permissionAuditOnly;
  }

  /**
   * for debugging
   */
  String getBlockPlacementPolicyClassName() {
    String name = replicator.getClass().getCanonicalName();
    return name;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void reconfigurePropertyImpl(String property, String newVal)
    throws ReconfigurationException {
    if (property.equals("dfs.block.replicator.classname")) {
      try {
        writeLock();
        Configuration replConf = new Configuration(getConf());
        if (newVal == null) {
          replConf.unset(property);
        } else {
          replConf.set(property, newVal);
        }
        replicator =
          BlockPlacementPolicy.getInstance(replConf, this, clusterMap,
                               hostsReader, dnsToSwitchMapping, this);
      } finally {
        writeUnlock();
      }
      LOG.info("RECONFIGURE* changed block placement policy to " +
               getBlockPlacementPolicyClassName());
    } else if (property.equals("dfs.persist.blocks")) {
      try {
        writeLock();
        if (newVal == null) {
          // set to default
          setPersistBlocks(false);
        } else if (newVal.equals("true")) {
          setPersistBlocks(true);
        } else if (newVal.equals("false")) {
          setPersistBlocks(false);
        } else {
          throw new ReconfigurationException(property, newVal,
                                             getConf().get(property));
        }
        LOG.info("RECONFIGURE* changed persist blocks to " +
                 getPersistBlocks());
      } finally {
        writeUnlock();
      }
    } else if (property.equals("dfs.permissions.audit.log")) {
      try {
        writeLock();
        if (newVal == null) {
          // set to default
          setPermissionAuditLog(false);
        } else if (newVal.equals("true")) {
          setPermissionAuditLog(true);
        } else if (newVal.equals("false")) {
          setPermissionAuditLog(false);
        } else {
          throw new ReconfigurationException(property, newVal,
                                             getConf().get(property));
        }
        LOG.info("RECONFIGURE* changed permission audit log to " +
                 getPermissionAuditLog());
      } finally {
        writeUnlock();
      }
    } else if (property.equals("dfs.heartbeat.interval")) {
      writeLock();
      try {
        if (newVal == null) {
          // set to default
          setHeartbeatInterval(3L * 1000L, this.heartbeatRecheckInterval);
        } else {
          setHeartbeatInterval(Long.valueOf(newVal) * 1000L,
                               this.heartbeatRecheckInterval);
        }
      } catch (NumberFormatException e) {
        throw new ReconfigurationException(property, newVal,
                                           getConf().get(property));
      } finally {
        writeUnlock();
      }
      LOG.info("RECONFIGURE* changed heartbeatInterval to " +
               this.heartbeatInterval);
    } else if (property.equals("heartbeat.recheck.interval")) {
      try {
        writeLock();
        if (newVal == null) {
          // set to default
          setHeartbeatInterval(this.heartbeatInterval, 5 * 60 * 1000);
        } else {
          setHeartbeatInterval(this.heartbeatInterval, Integer.valueOf(newVal));
        }
      } catch (NumberFormatException e) {
        throw new ReconfigurationException(property, newVal,
                                           getConf().get(property));
      } finally {
        writeUnlock();
      }
      LOG.info("RECONFIGURE* changed heartbeatRecheckInterval to " +
               this.heartbeatRecheckInterval);
    } else if (property.equals("dfs.access.time.precision")) {
      try {
        writeLock();
        if (newVal == null) {
          this.accessTimePrecision = 3600L;
        } else {
          this.accessTimePrecision = Long.parseLong(newVal);
        }
      } catch  (NumberFormatException e) {
        throw new ReconfigurationException(property, newVal,
            getConf().get(property));
      } finally {
        writeUnlock();
      }
      LOG.info("RECONFIGURE* changed access time precision to " +
          this.accessTimePrecision);
    } else if (property.equals("dfs.close.replication.min")) {
      try {
        writeLock();
        if (newVal == null) {
          this.minCloseReplication = this.minReplication;
        } else {
          int newCloseMinReplication = Integer.parseInt(newVal);
          if (newCloseMinReplication < minReplication ||
              newCloseMinReplication > maxReplication) {
            throw new ReconfigurationException(
                property, newVal, getConf().get(property));
          }
        }
      } catch (NumberFormatException e) {
        throw new ReconfigurationException(property, newVal,
            getConf().get(property));
      } finally {
        writeUnlock();
      }
      LOG.info("RECONFIGURE* changed new close min replication to " +
          this.minCloseReplication);
    } else if (property.equals("dfs.max-repl-streams")) {
      try {
        writeLock();
        if (newVal == null) {
          this.maxReplicationStreams = 2;
        } else {
          this.maxReplicationStreams = Integer.parseInt(newVal);
        }
      } catch (NumberFormatException e) {
        throw new ReconfigurationException(property, newVal,
            getConf().get(property));
      } finally {
        writeUnlock();
      }
      LOG.info("RECONFIGURE* changed max replication streams to " +
          this.maxReplicationStreams);
    } else if (property.equals("dfs.replication.iteration.multiplier")) {
      try {
        writeLock();
        if (newVal == null) {
          ReplicationConfigKeys.replicationWorkMultiplier =
              ReplicationConfigKeys.REPLICATION_WORK_MULTIPLIER_PER_ITERATION;
        } else {
          ReplicationConfigKeys.replicationWorkMultiplier = Float.parseFloat(newVal);
        }
      } catch (NumberFormatException e) {
        throw new ReconfigurationException(property, newVal,
            getConf().get(property));
      } finally {
        writeUnlock();
      }
      LOG.info("RECONFIGURE* changed replication work multiplier to " +
          ReplicationConfigKeys.replicationWorkMultiplier);
    } else if (property.equals("dfs.overreplication.iteration.multiplier")) {
        try {
          writeLock();
          if (newVal == null) {
            ReplicationConfigKeys.overreplicationWorkMultiplier =
                ReplicationConfigKeys.OVERREPLICATION_WORK_MULTIPLIER_PER_ITERATION;
          } else {
            ReplicationConfigKeys.overreplicationWorkMultiplier = Integer.parseInt(newVal);
          }
        } catch (NumberFormatException e) {
          throw new ReconfigurationException(property, newVal,
              getConf().get(property));
        } finally {
          writeUnlock();
        }
        LOG.info("RECONFIGURE* changed per iteration overreplication work to " +
            ReplicationConfigKeys.overreplicationWorkMultiplier);
    } else if (property.equals("dfs.block.invalidate.limit")) {
      try {
        writeLock();
        if (newVal == null) {
          ReplicationConfigKeys.blockInvalidateLimit =
              FSConstants.BLOCK_INVALIDATE_CHUNK;
        } else {
          ReplicationConfigKeys.blockInvalidateLimit = Integer.parseInt(newVal);
        }
      } catch (NumberFormatException e) {
        throw new ReconfigurationException(property, newVal,
            getConf().get(property));
      } finally {
        writeUnlock();
      }
      LOG.info("RECONFIGURE* block invalidate limit to " +
          ReplicationConfigKeys.blockInvalidateLimit);
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
    ArrayList<String> propertyList = new ArrayList<String>();
    propertyList.add("dfs.block.replicator.classname");
    propertyList.add("dfs.persist.blocks");
    propertyList.add("dfs.permissions.audit.log");
    propertyList.add("dfs.heartbeat.interval");
    propertyList.add("heartbeat.recheck.interval");
    propertyList.add("dfs.access.time.precision");
    propertyList.add("dfs.close.replication.min");
    propertyList.add("dfs.max-repl-streams");
    propertyList.add("dfs.replication.iteration.multiplier");
    return propertyList;
  }

  private static class ReplicationWork {

    private BlockInfo block;
    private long blockSize;
    private INodeFile fileINode;

    private int numOfReplicas;
    private DatanodeDescriptor srcNode;
    private List<DatanodeDescriptor> containingNodes;

    private DatanodeDescriptor targets[];
    private int priority;

    public ReplicationWork(BlockInfo block,
                            INodeFile fileINode,
                            int numOfReplicas,
                            DatanodeDescriptor srcNode,
                            List<DatanodeDescriptor> containingNodes,
                            int priority){
      this.block = block;
      this.blockSize = block.getNumBytes();
      this.fileINode = fileINode;
      this.numOfReplicas = numOfReplicas;
      this.srcNode = srcNode;
      this.containingNodes = containingNodes;
      this.priority = priority;

      this.targets = null;
    }
  }

  /**
   * Register NameNodeMXBean
   */
  private void registerMXBean() {
    this.namenodeMXBeanName = MBeanUtil.registerMBean("NameNode", "NameNodeInfo", this);
  }

  /**
   * Class representing Namenode information for JMX interfaces
   */
  @Override // NameNodeMXBean
  public String getVersion() {
    return VersionInfo.getVersion();
  }

  @Override // NameNodeMXBean
  public long getUsed() {
    return this.getCapacityUsed();
  }

  @Override // NameNodeMXBean
  public long getFree() {
    return this.getCapacityRemaining();
  }

  @Override // NameNodeMXBean
  public long getTotal() {
    return this.getCapacityTotal();
  }

  @Override // NameNodeMXBean
  public String getSafemode() {
    if (!this.isInSafeMode())
      return "";
    return "Safe mode is ON." + this.getSafeModeTip();
  }

  @Override // NameNodeMXBean
  public boolean isUpgradeFinalized() {
    return this.getFSImage().isUpgradeFinalized();
  }

  @Override // NameNodeMXBean
  public long getNonDfsUsedSpace() {
    return getCapacityUsedNonDFS();
  }

  @Override // NameNodeMXBean
  public float getPercentUsed() {
    return getCapacityUsedPercent();
  }

  @Override // NameNodeMXBean
  public long getNamespaceUsed() {
    return this.getCapacityNamespaceUsed();
  }

  @Override // NameNodeMXBean
  public float getPercentNamespaceUsed() {
    return this.getCapacityNamespaceUsedPercent();
  }

  @Override // NameNodeMXBean
  public float getPercentRemaining() {
    return getCapacityRemainingPercent();
  }

  @Override // NameNodeMXBean
  public long getTotalBlocks() {
    return getBlocksTotal();
  }

  @Override // NameNodeMXBean
  public long getTotalFilesAndDirectories() {
    return getFilesAndDirectoriesTotal();
  }

  @Override // NameNodeMXBean
  public long getNumberOfMissingBlocks() {
    return getMissingBlocksCount();
  }
  
  @Override // NameNodeMXBean
  public int getThreads() {
    return ManagementFactory.getThreadMXBean().getThreadCount();
  }

  /**
   * Returned information is a JSON representation of map with host name as the
   * key and value is a map of live node attribute keys to its values
   */
  @Override // NameNodeMXBean
  public String getLiveNodes() {
    final Map<String, Map<String,Object>> info = 
      new HashMap<String, Map<String,Object>>();
    try {
      final ArrayList<DatanodeDescriptor> liveNodeList = 
        new ArrayList<DatanodeDescriptor>();
      final ArrayList<DatanodeDescriptor> deadNodeList =
        new ArrayList<DatanodeDescriptor>();
      DFSNodesStatus(liveNodeList, deadNodeList);
      removeDecommissionedNodeFromList(liveNodeList);
      for (DatanodeDescriptor node : liveNodeList) {
        final Map<String, Object> innerinfo = new HashMap<String, Object>();
        innerinfo.put("lastContact", getLastContact(node));
        innerinfo.put("usedSpace", getDfsUsed(node));
        innerinfo.put("adminState", node.getAdminState().toString());
        innerinfo.put("excluded", this.inExcludedHostsList(node, null));
        info.put(node.getHostName() + ":" + node.getPort(), innerinfo);
      }
    } catch (Exception e) {
      LOG.error("Exception:", e);
    }
    return JSON.toString(info);
  }

  /**
   * Returned information is a JSON representation of map with host name as the
   * key and value is a map of dead node attribute keys to its values
   */
  @Override // NameNodeMXBean
  public String getDeadNodes() {
    final Map<String, Map<String, Object>> info = 
      new HashMap<String, Map<String, Object>>();
    try {
      final ArrayList<DatanodeDescriptor> liveNodeList =
      new ArrayList<DatanodeDescriptor>();
      final ArrayList<DatanodeDescriptor> deadNodeList =
      new ArrayList<DatanodeDescriptor>();
      // we need to call DFSNodeStatus to filter out the dead data nodes
      DFSNodesStatus(liveNodeList, deadNodeList);
      removeDecommissionedNodeFromList(deadNodeList);
      for (DatanodeDescriptor node : deadNodeList) {
        final Map<String, Object> innerinfo = new HashMap<String, Object>();
        innerinfo.put("lastContact", getLastContact(node));
        innerinfo.put("decommissioned", node.isDecommissioned());
        innerinfo.put("excluded", this.inExcludedHostsList(node, null));
        info.put(node.getHostName() + ":" + node.getPort(), innerinfo);
      }
    } catch (Exception e) {
      LOG.error("Exception:", e);
    }
    return JSON.toString(info);
  }

  /**
   * Returned information is a JSON representation of map with host name as the
   * key and value is a map of decomisioning node attribute keys to its values
   */
  @Override // NameNodeMXBean
  public String getDecomNodes() {
    final Map<String, Map<String, Object>> info = 
      new HashMap<String, Map<String, Object>>();
    try {
      final ArrayList<DatanodeDescriptor> decomNodeList = 
        this.getDecommissioningNodesList();
      for (DatanodeDescriptor node : decomNodeList) {
        final Map<String, Object> innerinfo = new HashMap<String, Object>();
        innerinfo.put("underReplicatedBlocks", node.decommissioningStatus
            .getUnderReplicatedBlocks());
        innerinfo.put("decommissionOnlyReplicas", node.decommissioningStatus
            .getDecommissionOnlyReplicas());
        innerinfo.put("underReplicateInOpenFiles", node.decommissioningStatus
            .getUnderReplicatedInOpenFiles());
        info.put(node.getHostName() + ":" + node.getPort(), innerinfo);
      }
    } catch (Exception e) {
      LOG.error("Exception:", e);
    }
    return JSON.toString(info);
  }
  
  /**
   * @return the live datanodes.
   */
  public List<DatanodeDescriptor> getLiveNodesList() {
    try {
      return getDatanodeListForReport(DatanodeReportType.LIVE);
    } catch (Exception e) {
      return new ArrayList<DatanodeDescriptor>();
    }
  }

  /*
   * @return list of datanodes where decommissioning is in progress
   */
  public ArrayList<DatanodeDescriptor> getDecommissioningNodesList() {

    try {
      return getDecommissioningNodesList(getDatanodeListForReport(DatanodeReportType.LIVE));
    } catch (Exception e) {
      return new ArrayList<DatanodeDescriptor>();
    }
  }

  public ArrayList<DatanodeDescriptor> getDecommissioningNodesList(
      List<DatanodeDescriptor> live) {
    ArrayList<DatanodeDescriptor> decommissioningNodes = new ArrayList<DatanodeDescriptor>();
    for (Iterator<DatanodeDescriptor> it = live.iterator(); it.hasNext();) {
      DatanodeDescriptor node = it.next();
      if (node.isDecommissionInProgress()) {
        decommissioningNodes.add(node);
      }
    }
    return decommissioningNodes;
  }
  
  /**
   * Number of live data nodes
   *
   * @return Number of live data nodes
   */
  public int getNumLiveDataNodes() {
    return getDatanodeListForReport(DatanodeReportType.LIVE).size();
  }


  /**
   * Number of dead data nodes
   *
   * @return Number of dead data nodes
   */
  public int getNumDeadDataNodes() {
    return getDatanodeListForReport(DatanodeReportType.DEAD).size();
  }

  private long getLastContact(DatanodeDescriptor alivenode) {
    return (System.currentTimeMillis() - alivenode.getLastUpdate())/1000;
  }

  private long getDfsUsed(DatanodeDescriptor alivenode) {
    return alivenode.getDfsUsed();
  }
  
  @Override  // NameNodeMXBean
  public int getNamespaceId() {
    return getNamespaceInfo().getNamespaceID();
  }
  
  @Override // NameNodeMXBean
  public String getNameserviceId() {
    return getNameNode().getNameserviceID();
  }
  
  @Override //NameNodeMXBean
  public String getSafeModeText() {
    if (!this.isInSafeMode())
      return "";
    return "Safe mode is ON. <em>" + this.getSafeModeTip() + "</em><br>";
  }
  
  @Override // NameNodeMXBean
  public Map<NameNodeKey, String> getNNSpecificKeys() {
    return getNameNode().getNameNodeSpecificKeys();
  }
  
  public Map<String, String> getJsonFriendlyNNSpecificKeys() {
    Map<String, String> clone = new HashMap<String, String>();
    Map<NameNodeKey, String> original = this.getNNSpecificKeys();
    for (NameNodeKey nnk : original.keySet()) {
      clone.put(nnk.toString(), original.get(nnk));
    }
    return clone;
  }
  
  @Override // NameNodeMXBean
  public boolean getIsPrimary() {
    return getNameNode().getIsPrimary();
  }
  
  public String getNameNodeStatus() {
    Map<String, Object> result = new HashMap<String, Object>();
    result.put(ClusterJspHelper.TOTAL_FILES_AND_DIRECTORIES,
        Long.toString(this.getTotalFilesAndDirectories()));
    result.put(ClusterJspHelper.TOTAL, Long.toString(this.getTotal()));
    result.put(ClusterJspHelper.FREE, Long.toString(this.getFree()));
    result.put(ClusterJspHelper.NAMESPACE_USED,
        Long.toString(this.getNamespaceUsed()));
    result.put(ClusterJspHelper.NON_DFS_USEDSPACE,
        Long.toString(this.getNonDfsUsedSpace()));
    result.put(ClusterJspHelper.TOTAL_BLOCKS,
        Long.toString(this.getTotalBlocks()));
    result.put(ClusterJspHelper.NUMBER_MISSING_BLOCKS,
        Long.toString(this.getNumberOfMissingBlocks()));
    result.put(ClusterJspHelper.SAFE_MODE_TEXT, this.getSafeModeText());
    result.put(ClusterJspHelper.LIVE_NODES, this.getLiveNodes());
    result.put(ClusterJspHelper.DEAD_NODES, this.getDeadNodes());
    result.put(ClusterJspHelper.DECOM_NODES, this.getDecomNodes());
    result.put(ClusterJspHelper.NNSPECIFIC_KEYS,
        JSON.toString(this.getJsonFriendlyNNSpecificKeys()));
    result.put(ClusterJspHelper.IS_PRIMARY,
        Boolean.toString(this.getIsPrimary()));
    return JSON.toString(result);
  }
  
  /**
   * Remove an already decommissioned data node who is neither in include nor
   * exclude hosts lists from the the list of live or dead nodes.  This is used
   * to not display an already decommssioned data node to the operators.
   * The operation procedure of making a already decommissioned data node not
   * to be displayed is as following:
   * <ol>
   *   <li> 
   *   Host must have been in the include hosts list and the include hosts list
   *   must not be empty.
   *   </li>
   *   <li>
   *   Host is decommissioned by remaining in the include hosts list and added
   *   into the exclude hosts list. Name node is updated with the new 
   *   information by issuing dfsadmin -refreshNodes command.
   *   </li>
   *   <li>
   *   Host is removed from both include hosts and exclude hosts lists.  Name 
   *   node is updated with the new informationby issuing dfsamin -refreshNodes 
   *   command.
   *   <li>
   * </ol>
   * 
   * @param nodeList
   *          , array list of live or dead nodes.
   */
  void removeDecommissionedNodeFromList(ArrayList<DatanodeDescriptor> nodeList) {
    // If the include list is empty, any nodes are welcomed and it does not
    // make sense to exclude any nodes from the cluster. Therefore, no remove.
    if (hostsReader.getHosts().isEmpty()) {
      return;
    }
    
    for (Iterator<DatanodeDescriptor> it = nodeList.iterator(); it.hasNext();) {
      DatanodeDescriptor node = it.next();
      if ((!inHostsList(node, null)) && (!inExcludedHostsList(node, null))
          && node.isDecommissioned()) {
        // Include list is not empty, an existing datanode does not appear
        // in both include or exclude lists and it has been decommissioned.
        // Remove it from the node list.
        it.remove();
      }
    }
  }
  
  public void cancelSaveNamespace(String reason) {
    this.dir.fsImage.cancelSaveNamespace(reason);
  }
  
  public void clearCancelSaveNamespace() {
    this.dir.fsImage.clearCancelSaveNamespace();
  }

  public HostsFileReader getHostReader() {
    return this.hostsReader;
  }

  public int getReportingNodes() {
    synchronized (heartbeats) {
      return getReportingNodesUnsafe();
    }
  }
  
  /**
   * This should be used with caution as it can throw
   * a concurrent modification exception.
   */
  public int getReportingNodesUnsafe() {
    int reportingNodes = 0;
    for (DatanodeDescriptor dn : heartbeats) {
      reportingNodes += (dn.receivedFirstFullBlockReport()) ? 1 : 0;
    }
    return reportingNodes;
  }
  
  static final UserGroupInformation getCurrentUGI() {
    UserGroupInformation ugi = Server.getCurrentUGI();
    if (ugi == null)
      ugi = UserGroupInformation.getCurrentUGI();
    return ugi;
  }
  
  private INode getLastINode(INode[] nodes) {
    if (nodes == null || nodes.length == 0)
      return null;
    return nodes[nodes.length-1];
  }
  
  /**
   * Clamp the specified replication between the minimum and the maximum
   * replication levels.
   */
  public short adjustReplication(short replication) {
    short r = (short) (replication < minReplication? minReplication
        : replication > maxReplication? maxReplication: replication);
    return r;
  }
  
  /** Re-populate the namespace and diskspace count of every node with quota */
  void recount() {
    writeLock();
    try {
      dir.updateCountForINodeWithQuota();
    } finally {
      writeUnlock();
    }
  }
  
  private void checkUserName(PermissionStatus permissions) throws IOException {
    if (permissions == null) {
      return;
    }
    if (!userNameValidator.isValidName(permissions.getUserName())) {
      throw new IOException("Invalid user : " + permissions.getUserName());
    }
  }
  
  DatanodeDescriptor getNode(String host) {
    return host2DataNodeMap.getDatanodeByHost(host);
  }
}

