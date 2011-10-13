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

import org.apache.hadoop.conf.*;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.util.LightWeightHashSet;
import org.apache.hadoop.hdfs.util.PathValidator;
import org.apache.hadoop.hdfs.protocol.*;
import org.apache.hadoop.hdfs.server.common.GenerationStamp;
import org.apache.hadoop.hdfs.server.common.HdfsConstants;
import org.apache.hadoop.hdfs.server.common.HdfsConstants.StartupOption;
import org.apache.hadoop.hdfs.server.common.Storage;
import org.apache.hadoop.hdfs.server.common.UpgradeStatusReport;
import org.apache.hadoop.hdfs.server.namenode.BlocksMap.BlockInfo;
import org.apache.hadoop.hdfs.server.namenode.DatanodeDescriptor.DecommissioningStatus;
import org.apache.hadoop.hdfs.server.namenode.DecommissionManager.Monitor;
import org.apache.hadoop.hdfs.server.namenode.metrics.FSNamesystemMBean;
import org.apache.hadoop.hdfs.server.namenode.metrics.FSNamesystemMetrics;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.PermissionChecker;
import org.apache.hadoop.security.UnixUserGroupInformation;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.*;
import org.apache.hadoop.metrics.util.MBeanUtil;
import org.apache.hadoop.net.CachedDNSToSwitchMapping;
import org.apache.hadoop.net.DNSToSwitchMapping;
import org.apache.hadoop.net.NetworkTopology;
import org.apache.hadoop.net.Node;
import org.apache.hadoop.net.ScriptBasedMapping;
import org.apache.hadoop.hdfs.server.namenode.LeaseManager.Lease;
import org.apache.hadoop.hdfs.server.namenode.UnderReplicatedBlocks.BlockIterator;
import org.apache.hadoop.hdfs.server.protocol.BlockCommand;
import org.apache.hadoop.hdfs.server.protocol.BlocksWithLocations;
import org.apache.hadoop.hdfs.server.protocol.BlocksWithLocations.BlockWithLocations;
import org.apache.hadoop.hdfs.server.protocol.DatanodeCommand;
import org.apache.hadoop.hdfs.server.protocol.DatanodeRegistration;
import org.apache.hadoop.hdfs.server.protocol.DisallowedDatanodeException;
import org.apache.hadoop.hdfs.server.protocol.NamespaceInfo;
import org.apache.hadoop.hdfs.server.protocol.ReceivedDeletedBlockInfo;
import org.apache.hadoop.hdfs.server.protocol.UpgradeCommand;
import org.apache.hadoop.hdfs.util.LightWeightLinkedSet;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.*;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.io.IOUtils;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.DataOutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.*;
import java.util.Map.Entry;

import javax.management.NotCompliantMBeanException;
import javax.management.ObjectName;
import javax.management.StandardMBean;
import javax.security.auth.login.LoginException;
import java.util.concurrent.locks.ReentrantReadWriteLock;

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
  implements FSConstants, FSNamesystemMBean, FSClusterStats {
  public static final Log LOG = LogFactory.getLog(FSNamesystem.class);
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
                                          HdfsFileStatus stat) {
    final StringBuilder builder = auditStringBuilder.get();
    builder.setLength(0);
    builder.append("ugi=").append(ugi).append("\t").
            append("ip=").append(addr).append("\t").
            append("cmd=").append(cmd).append("\t").
            append("src=").append(src).append("\t").
            append("dst=").append(dst).append("\t").
            append("perm=");
    if (stat == null) {
      builder.append("null").toString();
    } else {
      builder.append(stat.getOwner() + ':' +
          stat.getGroup() + ':' + stat.getPermission());
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
  private boolean persistBlocks;
  private UserGroupInformation fsOwner;
  private String supergroup;
  private PermissionStatus defaultPermission;
  // FSNamesystemMetrics counter variables
  private FSNamesystemMetrics myFSMetrics;
  private long capacityTotal = 0L, capacityUsed = 0L, capacityRemaining = 0L;
  private int totalLoad = 0;

  // number of datanodes that have reported (used during safemode only)
  private int dnReporting = 0;

  volatile long pendingReplicationBlocksCount = 0L;
  volatile long corruptReplicaBlocksCount = 0L;
  volatile long underReplicatedBlocksCount = 0L;
  volatile long numInvalidFilePathOperations = 0L;
  volatile long scheduledReplicationBlocksCount = 0L;
  volatile long excessBlocksCount = 0L;
  volatile long pendingDeletionBlocksCount = 0L;
  //
  // Stores the correct file name hierarchy
  //
  public FSDirectory dir;

  //
  // Mapping: Block -> { INode, datanodes, self ref }
  // Updated only in response to client-sent information.
  //
  final BlocksMap blocksMap = new BlocksMap(DEFAULT_INITIAL_MAP_CAPACITY,
    DEFAULT_MAP_LOAD_FACTOR);

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
  Map<String, Collection<Block>> excessReplicateMap =
    new TreeMap<String, Collection<Block>>();

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

  // list of blocks that need to be checked for possible overreplication
  private LightWeightLinkedSet<Block> overReplicatedBlocks = new LightWeightLinkedSet<Block>();

  public LeaseManager leaseManager = new LeaseManager(this);

  //
  // Threaded object that checks to see if we have been
  // getting heartbeats from all clients.
  //
  Daemon hbthread = null;   // HeartbeatMonitor thread
  public Daemon lmthread = null;   // LeaseMonitor thread
  Daemon smmthread = null;  // SafeModeMonitor thread

  public Daemon underreplthread = null;  // Replication thread for under replicated blocks
  public Daemon overreplthread = null;  // Replication thread for over replicated blocks

  private volatile boolean fsRunning = true;
  long systemStart = 0;

  //  The maximum number of replicates we should allow for a single block
  private int maxReplication;
  //  How many outgoing replication streams a given node should have at one time
  private int maxReplicationStreams;
  // MIN_REPLICATION is how many copies we need in place or else we disallow the write
  private int minReplication;
  // Default replication
  private int defaultReplication;
  // Variable to stall new replication checks for testing purposes
  private volatile boolean stallReplicationWork = false;
  // How many entries are returned by getCorruptInodes()
  int maxCorruptFilesReturned;
  // heartbeat interval from configuration
  long heartbeatInterval;
  // heartbeatRecheckInterval is how often namenode checks for expired datanodes
  long heartbeatRecheckInterval;
  // heartbeatExpireInterval is how long namenode waits for datanode to report
  // heartbeat
  private long heartbeatExpireInterval;
  // underReplicationRecheckInterval is how often namenode checks for new under replication work
  private long underReplicationRecheckInterval;
  // overReplicationRecheckInterval is how often namenode checks for new over replication work
  private long overReplicationRecheckInterval;
  // default block size of a file
  private long defaultBlockSize = 0;
  // allow appending to hdfs files
  private boolean supportAppends = true;

  /**
   * Last block index used for replication work per priority
   */
  private int[] replIndex = new int[UnderReplicatedBlocks.LEVEL];

  /**
   * NameNode RPC address
   */
  private InetSocketAddress nameNodeAddress = null; // TODO: name-node has this field, it should be removed here
  private NameNode nameNode = null;
  private SafeModeInfo safeMode;  // safe mode information
  private Host2NodesMap host2DataNodeMap = new Host2NodesMap();

  // datanode networktoplogy
  NetworkTopology clusterMap = null;
  DNSToSwitchMapping dnsToSwitchMapping;

  // for block replicas placement
  BlockPlacementPolicy replicator;

  private HostsFileReader hostsReader;
  private Daemon dnthread = null;

  private long maxFsObjects = 0;          // maximum number of fs objects

  /**
   * The global generation stamp for this file system.
   */
  private final GenerationStamp generationStamp = new GenerationStamp();

  // Ask Datanode only up to this many blocks to delete.
  int blockInvalidateLimit = FSConstants.BLOCK_INVALIDATE_CHUNK;

  // precision of access times.
  private long accessTimePrecision = 0;

  // lock to protect FSNamesystem.
  private ReentrantReadWriteLock fsLock;
  boolean hasRwLock = false; // shall we use read/write locks?

  // do not use manual override to exit safemode
  volatile boolean manualOverrideSafeMode = false;

  private PathValidator pathValidator;

  // Permission violations only result in entries in the namenode log.
  // The operation does not actually fail.
  private boolean permissionAuditOnly = false;

  // set of absolute path names that cannot be deleted
  Set<String> neverDeletePaths = new TreeSet<String>();

  // dynamic loading of config files
  private ConfigManager configManager;

  /**
   * FSNamesystem constructor.
   */
  FSNamesystem(NameNode nn, Configuration conf) throws IOException {
    super(conf);
    try {
      clusterMap = new NetworkTopology(conf);
      initialize(nn, getConf());
      pathValidator = new PathValidator(conf);
    } catch (IOException e) {
      LOG.error(getClass().getSimpleName() + " initialization failed.", e);
      close();
      throw e;
    }
  }

  /**
   * Initialize FSNamesystem.
   */
  private void initialize(NameNode nn, Configuration conf) throws IOException {
    this.systemStart = now();
    this.fsLock = new ReentrantReadWriteLock(); // non-fair
    configManager = new ConfigManager(this, conf);
    setConfigurationParameters(conf);

    // This can be null if two ports are running. Should not rely on the value.
    // The getter for this is deprecated
    this.nameNodeAddress = nn.getNameNodeAddress();
    this.nameNode = nn;
    this.registerMBean(conf); // register the MBean for the FSNamesystemStutus
    this.dir = new FSDirectory(this, conf);
    StartupOption startOpt = NameNode.getStartupOption(conf);
    this.dir.loadFSImage(getNamespaceDirs(conf),
      getNamespaceEditsDirs(conf), startOpt);
    long timeTakenToLoadFSImage = now() - systemStart;
    LOG.info("Finished loading FSImage in " + timeTakenToLoadFSImage + " msecs");
    NameNode.getNameNodeMetrics().fsImageLoadTime.set(
      (int) timeTakenToLoadFSImage);
    this.safeMode = new SafeModeInfo(conf);
    setBlockTotal();
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
    pendingReplications = new PendingReplicationBlocks(
      conf.getInt("dfs.replication.pending.timeout.sec",
        -1) * 1000L);

    // initialize heartbeat & leasemanager threads
    this.hbthread = new Daemon(new HeartbeatMonitor());
    this.lmthread = new Daemon(leaseManager.new Monitor());

    // start heartbeat & leasemanager threads
    hbthread.start();
    lmthread.start();

    // initialize replication threads
    this.underreplthread = new Daemon(new UnderReplicationMonitor());
    this.overreplthread = new Daemon(new OverReplicationMonitor());

    // start replication threds
    underreplthread.start();
    overreplthread.start();

    this.hostsReader = new HostsFileReader(conf.get("dfs.hosts", ""),
      conf.get("dfs.hosts.exclude", ""));
    this.dnthread = new Daemon(new DecommissionManager(this).new Monitor(
      conf.getInt("dfs.namenode.decommission.interval", 30),
      conf.getInt("dfs.namenode.decommission.nodes.per.interval", 5)));
    dnthread.start();

    this.dnsToSwitchMapping = ReflectionUtils.newInstance(
      conf.getClass("topology.node.switch.mapping.impl", ScriptBasedMapping.class,
        DNSToSwitchMapping.class), conf);

    /* If the dns to swith mapping supports cache, resolve network
    * locations of those hosts in the include list,
    * and store the mapping in the cache; so future calls to resolve
    * will be fast.
    */
    if (dnsToSwitchMapping instanceof CachedDNSToSwitchMapping) {
      dnsToSwitchMapping.resolve(new ArrayList<String>(hostsReader.getHosts()));
    }

    this.replicator = BlockPlacementPolicy.getInstance(
        conf,
        this,
        this.clusterMap,
        this.hostsReader,
        this.dnsToSwitchMapping,
        this);
  }

  public static Collection<File> getNamespaceDirs(Configuration conf) {
    Collection<String> dirNames = conf.getStringCollection("dfs.name.dir");
    if (dirNames.isEmpty()) {
      dirNames.add("/tmp/hadoop/dfs/name");
    }
    Collection<File> dirs = new ArrayList<File>(dirNames.size());
    for (String name : dirNames) {
      dirs.add(new File(name));
    }
    return dirs;
  }

  public static Collection<File> getNamespaceEditsDirs(Configuration conf) {
    Collection<String> editsDirNames =
      conf.getStringCollection("dfs.name.edits.dir");
    if (editsDirNames.isEmpty()) {
      editsDirNames.add("/tmp/hadoop/dfs/name");
    }
    Collection<File> dirs = new ArrayList<File>(editsDirNames.size());
    for (String name : editsDirNames) {
      dirs.add(new File(name));
    }
    return dirs;
  }

  /**
   * dirs is a list of directories where the filesystem directory state
   * is stored
   */
  FSNamesystem(FSImage fsImage, Configuration conf) throws IOException {
    super(conf);
    this.clusterMap = new NetworkTopology(conf);
    this.fsLock = new ReentrantReadWriteLock();
    setConfigurationParameters(conf);
    this.dir = new FSDirectory(fsImage, this, conf);
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
    this.blockInvalidateLimit =
      Math.max(this.blockInvalidateLimit,
               20 * (int) (heartbeatInterval/1000L));
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
    this.maxReplicationStreams = conf.getInt("dfs.max-repl-streams", 2);
    long heartbeatInterval = conf.getLong("dfs.heartbeat.interval", 3) * 1000;
    long heartbeatRecheckInterval = conf.getInt(
      "heartbeat.recheck.interval", 5 * 60 * 1000); // 5 minutes
    setHeartbeatInterval(heartbeatInterval, heartbeatRecheckInterval);
    this.underReplicationRecheckInterval =
      conf.getInt("dfs.replication.interval", 3) * 1000L;
    this.overReplicationRecheckInterval = 2 * underReplicationRecheckInterval;
    this.defaultBlockSize = conf.getLong("dfs.block.size", DEFAULT_BLOCK_SIZE);
    this.maxFsObjects = conf.getLong("dfs.max.objects", 0);
    this.accessTimePrecision = conf.getLong("dfs.access.time.precision", 0);
    this.supportAppends = conf.getBoolean("dfs.support.append", false);

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
      return new NamespaceInfo(dir.fsImage.getNamespaceID(),
        dir.fsImage.getCTime(),
        getDistributedUpgradeVersion());
    } finally {
      writeUnlock();
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
      if (dnthread != null) {
        dnthread.interrupt();
      }
      if (smmthread != null) {
        smmthread.interrupt();
      }
    } catch (Exception e) {
      LOG.warn("Exception shutting down FSNamesystem", e);
    } finally {
      // using finally to ensure we also wait for lease daemon
      try {
        if (lmthread != null) {
          lmthread.interrupt();
          lmthread.join(3000);
        }
        dir.close();
        blocksMap.close();
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

  private boolean isAccessTimeSupported() {
    return accessTimePrecision > 0;
  }

  /* get replication factor of a block */

  private int getReplication(Block block) {
    INodeFile fileINode = blocksMap.getINode(block);
    if (fileINode == null) { // block does not belong to any file
      return 0;
    }
    assert !fileINode.isDirectory() : "Block cannot belong to a directory.";
    return fileINode.getReplication();
  }

  /* updates a block in under replication queue */

  void updateNeededReplications(Block block,
                                int curReplicasDelta, int expectedReplicasDelta) {
    writeLock();
    try {
      NumberReplicas repl = countNodes(block);
      int curExpectedReplicas = getReplication(block);
      neededReplications.update(block,
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
      Iterator<Block> iter = node.getBlockIterator();
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
    writeLock();
    try {
      if (isInSafeMode()) {
        throw new SafeModeException("Cannot set permission for " + src, safeMode);
      }
      INode[] inodes = dir.getExistingPathINodes(src);
      checkOwner(src, inodes);
      dir.setPermission(src, permission);
    } finally {
      writeUnlock();
    }
    getEditLog().logSync(false);
    if (auditLog.isInfoEnabled()) {
      final HdfsFileStatus stat = dir.getHdfsFileInfo(src);
      logAuditEvent(UserGroupInformation.getCurrentUGI(),
        Server.getRemoteIp(),
        "setPermission", src, null, stat);
    }
  }

  /**
   * Set owner for an existing file.
   *
   * @throws IOException
   */
  public void setOwner(String src, String username, String group
  ) throws IOException {
    writeLock();
    try {
      if (isInSafeMode()) {
        throw new SafeModeException("Cannot set permission for " + src, safeMode);
      }
      INode[] inodes = dir.getExistingPathINodes(src);
      FSPermissionChecker pc = checkOwner(src, inodes);
      if (!pc.isSuper) {
        if (username != null && !pc.user.equals(username)) {
          throw new AccessControlException("Non-super user cannot change owner.");
        }
        if (group != null && !pc.containsGroup(group)) {
          throw new AccessControlException("User does not belong to " + group
            + " .");
        }
      }
      dir.setOwner(src, username, group);
    } finally {
      writeUnlock();
    }
    getEditLog().logSync(false);
    if (auditLog.isInfoEnabled()) {
      final HdfsFileStatus stat = dir.getHdfsFileInfo(src);
      logAuditEvent(UserGroupInformation.getCurrentUGI(),
        Server.getRemoteIp(),
        "setOwner", src, null, stat);
    }
  }

  /**
   * Updates DatanodeInfo for each LocatedBlock in locatedBlocks.
   */
  LocatedBlocks updateDatanodeInfo(LocatedBlocks locatedBlocks) throws IOException {
    if (locatedBlocks.getLocatedBlocks().size() == 0) return locatedBlocks;

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

	LocatedBlock newBlock = new LocatedBlock(block, machineSet, 0, blockCorrupt);
	newBlocks.add(newBlock);
      }
    } finally {
      readUnlock();
    }

    return new LocatedBlocks(locatedBlocks.getFileLength(), newBlocks, false);
  }

  /**
   * Get block locations within the specified range.
   *
   * @see #getBlockLocations(String, long, long)
   */
  LocatedBlocks getBlockLocations(String clientMachine, String src,
                                  long offset, long length,
                                  boolean needVersion) throws IOException {
    INode[] inodes = dir.getExistingPathINodes(src);
    if (isPermissionEnabled) {
      checkPathAccess(src, inodes, FsAction.READ);
    }

    LocatedBlocks blocks = getBlockLocations(src, inodes[inodes.length-1],
        offset, length, true, needVersion);
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
        offset, length, false, false);
  }

  /**
   * Get block locations within the specified range.
   *
   * @see ClientProtocol#getBlockLocations(String, long, long)
   */
  public LocatedBlocks getBlockLocations(String src, INode inode, long offset, long length,
                                         boolean doAccessTime, boolean needVersion)
    throws IOException {
    if (offset < 0) {
      throw new IOException("Negative offset is not supported. File: " + src);
    }
    if (length < 0) {
      throw new IOException("Negative length is not supported. File: " + src);
    }
    final LocatedBlocks ret = getBlockLocationsInternal(src, inode,
      offset, length, Integer.MAX_VALUE, doAccessTime, needVersion);
    if (auditLog.isInfoEnabled()) {
      logAuditEvent(UserGroupInformation.getCurrentUGI(),
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
                                                  boolean needVersion)
    throws IOException {
    boolean doneSetTimes = false;

    for (int attempt = 0; attempt < 2; attempt++) {
      if (attempt == 0) { // first attempt is with readlock
        readLock();
      }  else { // second attempt is with  write lock
        writeLock(); // writelock is needed to set accesstime
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
          if (now <= inode.getAccessTime() + getAccessTimePrecision()) {
            // if we have to set access time but we only have the readlock, then
            // restart this entire operation with the writeLock.
            if (attempt == 0) {
              continue;
            }
          }
          dir.setTimes(src, fileInode, -1, now, false);
          doneSetTimes = true; // successful setTime call
        }
        return getBlockLocationsInternal(fileInode, offset, length,
            nrBlocksToReturn, needVersion);
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
        nrBlocksToReturn, false);
  }

  LocatedBlocks getBlockLocationsInternal(INodeFile inode,
                                          long offset, long length,
                                          int nrBlocksToReturn,
                                          boolean needVersion)
    throws IOException {
    readLock();
    try {
      Block[] blocks = inode.getBlocks();
      if (blocks == null) {
        return null;
      }
      if (blocks.length == 0) {
        return inode.createLocatedBlocks(
            new ArrayList<LocatedBlock>(blocks.length), needVersion);
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
        int numCorruptNodes = countNodes(blocks[curBlk]).corruptReplicas();
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
        results.add(new LocatedBlock(blocks[curBlk], machineSet, curPos,
          blockCorrupt));
        curPos += blocks[curBlk].getNumBytes();
        curBlk++;
      } while (curPos < endOff
        && curBlk < blocks.length
        && results.size() < nrBlocksToReturn);

      return inode.createLocatedBlocks(results, needVersion);
    } finally {
      readUnlock();
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

    writeLock();
    try {
      // write permission for the target
      if (isPermissionEnabled) {
        INode[] targetInodes = dir.getExistingPathINodes(target);
        checkPathAccess(target, targetInodes, FsAction.WRITE);

        // and srcs
        for(String aSrc: srcs) {
          INode[] srcNodes = dir.getExistingPathINodes(aSrc);
          checkPathAccess(aSrc, srcNodes, FsAction.READ); // read the file
          checkParentAccess(aSrc, srcNodes, FsAction.WRITE); // for delete
        }
      }


      // to make sure no two files are the same
      Set<INode> si = new HashSet<INode>();

      // we put the following prerequisite for the operation
      // replication and blocks sizes should be the same for ALL the blocks
      // check the target
      INode inode = dir.getInode(target);

      if(inode == null) {
        throw new IllegalArgumentException("concat: trg file doesn't exist");
      }
      if(inode.isUnderConstruction()) {
        throw new IllegalArgumentException("concat: trg file is uner construction");
      }

      INodeFile trgInode = (INodeFile) inode;

      // per design trg shouldn't be empty and all the blocks same size
      if(trgInode.blocks.length == 0) {
        throw new IllegalArgumentException("concat: "+ target + " file is empty");
      }

      long blockSize = trgInode.getPreferredBlockSize();

      // check the end block to be full
      if(restricted && blockSize != trgInode.blocks[trgInode.blocks.length-1].getNumBytes()) {
        throw new IllegalArgumentException(target
            + " file: last block should be full. " + " PreferredBlockSize is "
            + blockSize + " block size is: "
            + trgInode.blocks[trgInode.blocks.length - 1].getNumBytes());
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

        if(src.isEmpty()
            || srcInode == null
            || srcInode.isUnderConstruction()
            || srcInode.blocks.length == 0) {
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
          int idx = srcInode.blocks.length-1; 
          if(endSrc)    
            idx = srcInode.blocks.length-2; // end block of endSrc is OK not to be full   
          if(idx >= 0 && srcInode.blocks[idx].getNumBytes() != blockSize) {
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
      final HdfsFileStatus stat = dir.getHdfsFileInfo(target);
      logAuditEvent(UserGroupInformation.getCurrentUGI(),
                    Server.getRemoteIp(),
                    "concat", Arrays.toString(srcs), target, stat);
    }

  }

  /**
   * stores the modification and access time for this inode.
   * The access time is precise upto an hour. The transaction, if needed, is
   * written to the edits log but is not flushed.
   */
  public void setTimes(String src, long mtime, long atime) throws IOException {
    setTimesInternal(src, mtime, atime);
    getEditLog().logSync(false);
  }

  private void setTimesInternal(String src, long mtime, long atime)
    throws IOException {
    writeLock();
    try {
      if (!isAccessTimeSupported() && atime != -1) {
        atime = -1;  // not to update access time since not supported
      }
      if (isInSafeMode()) {
        throw new SafeModeException("Cannot set accesstimes  for " + src, safeMode);
      }
      //
      // The caller needs to have write access to set access & modification times.
      if (isPermissionEnabled) {
        INode[] inodes = dir.getExistingPathINodes(src);
        checkPathAccess(src, inodes, FsAction.WRITE);
      }
      INodeFile inode = dir.getFileINode(src);
      if (inode != null) {
        dir.setTimes(src, inode, mtime, atime, true);
        if (auditLog.isInfoEnabled()) {
          final HdfsFileStatus stat = dir.getHdfsFileInfo(src);
          logAuditEvent(UserGroupInformation.getCurrentUGI(),
            Server.getRemoteIp(),
            "setTimes", src, null, stat);
        }
      } else {
        throw new FileNotFoundException("File " + src + " does not exist.");
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
      logAuditEvent(UserGroupInformation.getCurrentUGI(),
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
        checkPathAccess(src, inodes, FsAction.WRITE);
      }

      int[] oldReplication = new int[1];
      Block[] fileBlocks;
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
      checkTraverse(filename, inodes);
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
      final HdfsFileStatus stat = FSDirectory.getHdfsFileInfo(file);
      logAuditEvent(UserGroupInformation.getCurrentUGI(),
        Server.getRemoteIp(),
        "create", src, null, stat);
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
    if (!pathValidator.isValidName(src)) {
      numInvalidFilePathOperations++;
      throw new IOException("Invalid file name: " + src);
    }

    // convert path string to array of bytes w/o holding lock
    String[] names = INodeDirectory.getPathNames(src);
    byte[][] components = INodeDirectory.getPathComponents(names);

    writeLock();
    try {
      if (NameNode.stateChangeLog.isDebugEnabled()) {
        NameNode.stateChangeLog.debug("DIR* NameSystem.startFile: src=" + src
          + ", holder=" + holder
          + ", clientMachine=" + clientMachine
          + ", createParent=" + createParent
          + ", replication=" + replication
          + ", overwrite=" + overwrite
          + ", append=" + append);
      }

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

      if (isPermissionEnabled) {
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
            deleteInternal(src, inodes, false, true);
          } else {
            throw new IOException("failed to create file " + src
              + " on client " + clientMachine
              + " either because the filename is invalid or the file exists");
          }
        }

        DatanodeDescriptor clientNode =
          host2DataNodeMap.getDatanodeByHost(clientMachine);

        if (append) {
          //
          // Replace current node with a INodeUnderConstruction.
          // Recreate in-memory lease record.
          //
          INodeFile node = (INodeFile) myFile;
          INodeFileUnderConstruction cons = new INodeFileUnderConstruction(
            node.getLocalNameBytes(),
            node.getReplication(),
            node.getModificationTime(),
            node.getPreferredBlockSize(),
            node.getBlocks(),
            node.getPermissionStatus(),
            holder,
            clientMachine,
            clientNode);
          dir.replaceNode(src, inodes, node, cons, true);
          leaseManager.addLease(cons.clientName, src);
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
            replication, blockSize, holder, clientMachine, clientNode, genstamp);
          if (newNode == null) {
            throw new IOException("DIR* NameSystem.startFile: " +
              "Unable to add file to namespace.");
          }
          leaseManager.addLease(newNode.clientName, src);
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
    writeLock();
    try {
      if (isInSafeMode()) {
        throw new SafeModeException(
            "Cannot recover the lease of " + src, safeMode);
      }
      if (!pathValidator.isValidName(src)) {
        throw new IOException("Invalid file name: " + src);
      }

      INode inode = dir.getFileINode(src);
      if (inode == null) {
        throw new FileNotFoundException("File not found " + src);
      }

      if (!inode.isUnderConstruction()) {
        return true;
      }

      if (isPermissionEnabled) {
        INode[] inodes = dir.getExistingPathINodes(src);
        checkPathAccess(src, inodes, FsAction.WRITE);
      }

      recoverLeaseInternal(inode, src, holder, clientMachine,
                           true, discardLastBlock);
      return false;
    } finally {
      writeUnlock();
    }
  }

  private void recoverLeaseInternal(INode fileInode,
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
        internalReleaseLeaseOne(lease, src, pendingFile, discardLastBlock);
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
  }

  /**
   * Append to an existing file in the namespace.
   */
  LocatedBlock appendFile(String src, String holder, String clientMachine
  ) throws IOException {
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

          lb = new LocatedBlock(last, targets,
            fileLength - storedBlock.getNumBytes());

          // Remove block from replication queue.
          updateNeededReplications(last, 0, 0);

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
      logAuditEvent(UserGroupInformation.getCurrentUGI(),
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

  public LocatedBlock getAdditionalBlock(String src, String clientName,
      List<Node> excludedNodes) throws IOException {
    return getAdditionalBlock(src, clientName, excludedNodes, null, true, false);
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
      for (int i = 0; i < Math.min(favoredNodes.size(), replication); i++) {
        DatanodeDescriptor favoredNode = getDatanode(favoredNodes
            .get(i));
        // Choose a single node which is local to favoredNodes(i).
        DatanodeDescriptor[] locations = replicator.chooseTarget(src, 1,
            favoredNode, new ArrayList<DatanodeDescriptor>(),
            excludedNodes, blockSize);
        if (locations.length != 1) {
          LOG.warn("Could not find a target for file " + src
              + " with favored node " + favoredNode);
          continue;
        }
        DatanodeDescriptor target = locations[0];
        results.add(target);
        excludedNodes.add(target);
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
      List<Node> excludedNodes, List<DatanodeInfo> favoredNodes,
      boolean wait, boolean needVersion)
    throws IOException {
    long fileLength, blockSize;
    int replication;
    DatanodeDescriptor clientNode = null;
    Block newBlock = null;

    // convert path string into an array of bytes w/o holding lock
    byte[][] components = INodeDirectory.getPathComponents(src);
    INode[] pathINodes = new INode[components.length];

    NameNode.stateChangeLog.debug("BLOCK* NameSystem.getAdditionalBlock: file "
      + src + " for " + clientName);

    readLock();
    try {
      // have we exceeded the configured limit of fs objects.
      checkFsObjectLimit();

      dir.rootDir.getExistingPathINodes(components, pathINodes);
      INodeFileUnderConstruction pendingFile =
         checkLease(src, clientName, pathINodes[pathINodes.length - 1]);

      //
      // If we fail this, bad things happen!
      // check for pending file blocks only if 'wait' is set to true.
      if (wait && !checkFileProgress(pendingFile, false)) {
        throw new NotReplicatedYetException("Not replicated yet:" + src);
      }
      fileLength = pendingFile.computeContentSummary().getLength();
      blockSize = pendingFile.getPreferredBlockSize();
      clientNode = pendingFile.getClientNode();
      replication = (int) pendingFile.getReplication();
    } finally {
      readUnlock();
    }

    DatanodeDescriptor[] targets = computeTargets(src, replication, clientNode,
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
      INodeFileUnderConstruction pendingFile =
        checkLease(src, clientName, pathINodes[pathINodes.length - 1]);


      if (wait && !checkFileProgress(pendingFile, false)) {
        throw new NotReplicatedYetException("Not replicated yet:" + src);
      }

      // replicate last block if under-replicated
      replicateLastBlock(src, pendingFile);
      
      // allocate new block record block locations in INode.
      newBlock = allocateBlock(src, pathINodes);
      pendingFile.setTargets(targets);

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
      getEditLog().logSyncIfNeeded(); // sync if too many transactions in buffer
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
    if (needVersion) {
    return new VersionedLocatedBlock(newBlock, targets, fileLength,
        DataTransferProtocol.DATA_TRANSFER_VERSION);
    }
    return new LocatedBlock(newBlock, targets, fileLength);
  }

  /** 
   * Check last block of the file under construction
   * Replicate it if it is under replicated
   * @param src the file name
   * @param file the file's inode
   */
  private void replicateLastBlock(String src, INodeFileUnderConstruction file) {
    if (file.blocks == null || file.blocks.length == 0)
      return;
    BlockInfo block = file.blocks[file.blocks.length-1];
    DatanodeDescriptor[] targets = file.getTargets();
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
      NameNode.stateChangeLog.debug("BLOCK* NameSystem.abandonBlock: "
        + b + "of file " + src);
      if (isInSafeMode()) {
        throw new SafeModeException("Cannot abandon block " + b +
          " for fle" + src, safeMode);
      }
      INodeFileUnderConstruction file = checkLease(src, holder);
      dir.removeBlock(src, file, b);
      NameNode.stateChangeLog.debug("BLOCK* NameSystem.abandonBlock: "
        + b
        + " is removed from pendingCreates");
      return true;
    } finally {
      writeUnlock();
    }
  }

  /**
 *    * The client would like to let go of a given file
 *       */
  public boolean abandonFile(String src, String holder
      ) throws IOException {
    writeLock();
    try {
      NameNode.stateChangeLog.debug("FILE* NameSystem.abandonFile: " + src);
      if (isInSafeMode()) {
        throw new SafeModeException("Cannot abandon file " + src, safeMode);
      }
      checkLease(src, holder);
      internalReleaseLeaseOne(leaseManager.getLease(holder), src);
      NameNode.stateChangeLog.debug("FILE* NameSystem.abandonFile: "
                                      + " has been scheduled for lease recovery");
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

  public CompleteFileStatus completeFile(String src, String holder)
    throws IOException {
    CompleteFileStatus status = completeFileInternal(src, holder);
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
                                                  String holder)
    throws IOException {
    // convert path string into an array of bytes w/o holding lock
    byte[][] components = INodeDirectory.getPathComponents(src);

    writeLock();
    try {
      NameNode.stateChangeLog.debug("DIR* NameSystem.completeFile: " + src + " for " + holder);
      if (isInSafeMode()) {
        throw new SafeModeException("Cannot complete file " + src, safeMode);
      }
      INode[] inodes = new INode[components.length];
      dir.rootDir.getExistingPathINodes(components, inodes);
      checkFilePath(src, inodes);
      INodeFileUnderConstruction pendingFile = checkLease(
          src, holder, inodes[inodes.length-1]);
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
      } else if (!checkFileProgress(pendingFile, true)) {
        return CompleteFileStatus.STILL_WAITING;
      }

      finalizeINodeFileUnderConstruction(src, inodes, pendingFile);

    } finally {
      writeUnlock();
    }
    NameNode.stateChangeLog.info("DIR* NameSystem.completeFile: file " + src
      + " is closed by " + holder);
    return CompleteFileStatus.COMPLETE_SUCCESS;
  }

  static Random randBlockId = new Random();

  /**
   * Allocate a block at the given pending filename
   *
   * @param src    path to the file
   * @param inodes INode representing each of the components of src.
   *               <code>inodes[inodes.length-1]</code> is the INode for the file.
   */
  private Block allocateBlock(String src, INode[] inodes) throws IOException {
    Block b = new Block(FSNamesystem.randBlockId.nextLong(), 0, 0);
    while (isValidBlock(b)) {
      b.setBlockId(FSNamesystem.randBlockId.nextLong());
    }
    b.setGenerationStamp(getGenerationStamp());
    b = dir.addBlock(src, inodes, b);
    return b;
  }

  /**
   * Check that the indicated file's blocks are present and
   * replicated.  If not, return false. If checkall is true, then check
   * all blocks, otherwise check only penultimate block.
   */
  boolean checkFileProgress(INodeFile v, boolean checkall) {
    if (checkall) {
      //
      // check all blocks of the file.
      //
      for (Block block : v.getBlocks()) {
        if (blocksMap.numNodes(block) < this.minReplication) {
          LOG.info(
            "INodeFile " + v + " block " + block + " has replication " +
              blocksMap.numNodes(block) + " and requires " + this.minReplication
          );

          return false;
        }
      }
    } else {
      //
      // check the penultimate block of this file
      //
      Block b = v.getPenultimateBlock();
      if (b != null) {
        if (blocksMap.numNodes(b) < this.minReplication) {
          LOG.info(
            "INodeFile " + v + " block " + b + " has replication " +
              blocksMap.numNodes(b) + " and requires " + this.minReplication
          );

          return false;
        }
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
    NameNode.stateChangeLog.info("BLOCK* NameSystem.addToInvalidates: "
      + b.getBlockName() + " is added to invalidSet of " + n.getName());
  }

  /**
   * Adds block to list of blocks which will be invalidated on
   * specified datanode
   *
   * @param b block
   * @param n datanode
   */
  void addToInvalidatesNoLog(Block b, DatanodeInfo n, boolean ackRequired) {
    LightWeightHashSet<Block> invalidateSet = recentInvalidateSets.get(n
        .getStorageID());
    if (invalidateSet == null) {
      invalidateSet = new LightWeightHashSet<Block>();
      recentInvalidateSets.put(n.getStorageID(), invalidateSet);
    }
    if(!ackRequired){
      b.setNumBytes(BlockCommand.NO_ACK);
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
    for (Iterator<DatanodeDescriptor> it =
      blocksMap.nodeIterator(b); it.hasNext();) {
      DatanodeDescriptor node = it.next();
      addToInvalidates(b, node, ackRequired);
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

  /**
   * Mark the block belonging to datanode as corrupt
   *
   * @param blk Block to be marked as corrupt
   * @param dn  Datanode which holds the corrupt replica
   */
  public void markBlockAsCorrupt(Block blk, DatanodeInfo dn)
    throws IOException {
    writeLock();
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
        if (countNodes(storedBlockInfo).liveReplicas() > inode.getReplication()) {
          // the block is over-replicated so invalidate the replicas immediately
          invalidateBlock(storedBlockInfo, node, true);
        } else if (isPopulatingReplQueues()) {
          // add the block to neededReplication
          updateNeededReplications(storedBlockInfo, -1, 0);
        }
      }
    } finally {
      writeUnlock();
    }
  }

  /**
   * Invalidates the given block on the given datanode.
   */
  private void invalidateBlock(Block blk, DatanodeInfo dn, boolean ackRequired)
    throws IOException {
    assert (hasWriteLock());
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
      NameNode.stateChangeLog.debug("BLOCK* NameSystem.invalidateBlocks: "
        + blk + " on "
        + dn.getName() + " listed for deletion.");
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
   * Change the indicated filename.
   */
  public boolean renameTo(String src, String dst) throws IOException {
    INode dstNode = renameToInternal(src, dst);
    getEditLog().logSync(false);
    if (dstNode != null && auditLog.isInfoEnabled()) {
      final HdfsFileStatus stat = FSDirectory.getHdfsFileInfo(dstNode);
      logAuditEvent(UserGroupInformation.getCurrentUGI(),
        Server.getRemoteIp(),
        "rename", src, dst, stat);
    }
    return dstNode != null;
  }

  private INode renameToInternal(String src, String dst
  ) throws IOException {
    // convernt string to bytes outside of write lock
    String[] srcNames = INode.getPathNames(src);
    byte[][] srcComponents = INode.getPathComponents(srcNames);
    byte[][] dstComponents = INode.getPathComponents(dst);
    writeLock();
    try {
      NameNode.stateChangeLog.debug("DIR* NameSystem.renameTo: " + src + " to " + dst);
      if (isInSafeMode()) {
        throw new SafeModeException("Cannot rename " + src, safeMode);
      }
      if (!pathValidator.isValidName(dst)) {
        numInvalidFilePathOperations++;
        throw new IOException("Invalid name: " + dst);
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
        checkParentAccess(src, srcInodes, FsAction.WRITE);

        INode[] actualDstInodes = dstInodes;
        if (dstNode != null && dstNode.isDirectory()) {
          actualDst = dst + Path.SEPARATOR + srcNames[srcNames.length-1];
          actualDstInodes = Arrays.copyOf(dstInodes, dstInodes.length+1);
          lastDstComponent = srcComponents[srcComponents.length-1];
          actualDstInodes[actualDstInodes.length-1] = ((INodeDirectory)dstNode).
                       getChildINode(lastDstComponent);
        }
        checkAncestorAccess(actualDst, actualDstInodes, FsAction.WRITE);
      }
      if (neverDeletePaths.contains(src)) {
        NameNode.stateChangeLog.warn("DIR* NameSystem.delete: " +
          " Trying to rename a whitelisted path " + src +
          " by user " + UserGroupInformation.getCurrentUGI() +
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
      logAuditEvent(UserGroupInformation.getCurrentUGI(),
        Server.getRemoteIp(),
        "delete", src, null, null);
    }
    return status;
  }

  /**
   * Remove the indicated filename from the namespace.  This may
   * invalidate some blocks that make up the file.
   */
  boolean deleteInternal(String src, INode[] inodes, boolean recursive,
                         boolean enforcePermission) throws IOException {
    ArrayList<Block> collectedBlocks = new ArrayList<Block>();
    boolean deleteNow = false;

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
      if (enforcePermission && isPermissionEnabled) {
        checkPermission(src, inodes, false, null, FsAction.WRITE, null, FsAction.ALL);
      }
      if (neverDeletePaths.contains(src)) {
        NameNode.stateChangeLog.warn("DIR* NameSystem.delete: " +
          " Trying to delete a whitelisted path " + src +
          " by user " + UserGroupInformation.getCurrentUGI() +
          " from server " + Server.getRemoteIp());
        throw new IOException("Deleting a whitelisted directory is not allowed. " + src);
      }

      if ((!recursive) && (!dir.isDirEmpty(inodes[inodes.length-1]))) {
        throw new IOException(src + " is non empty");
      }

      if (dir.delete(src, inodes, collectedBlocks) == null) {
        return false;
      }
      deleteNow = collectedBlocks.size() <= BLOCK_DELETION_INCREMENT;
      if (deleteNow) {
        removeBlocks(collectedBlocks);
      }
    } finally {
      writeUnlock();
    }
    if (!deleteNow) {
      removeBlocks(collectedBlocks);
    }
    collectedBlocks.clear();
    return true;
  }

  /**
   * From the given list, incrementally remove the blocks. Add the blocks
   * to invalidates, and set a flag that explicit ACK from DataNode is not
   * required. This function should be used only for deleting entire files.
   */
  private void removeBlocks(List<Block> blocks) {
    int start = 0;
    int end = 0;
    while (start < blocks.size()) {
      end = BLOCK_DELETION_INCREMENT + start;
      end = end > blocks.size() ? blocks.size() : end;
      writeLock();
      try {
        for (int i = start; i < end; i++) {
          Block b = blocks.get(i);
          addToInvalidates(b, false);
          removeFromExcessReplicateMap(b);
          neededReplications.remove(b, -1);
          corruptReplicas.removeFromCorruptReplicasMap(b);
          blocksMap.removeBlock(b);
        }
      } finally {
        writeUnlock();
      }
      start = end;
    }
  }

  /**
   * Remove the blocks from the given list. Also, remove the path. Add the
   * blocks to invalidates, and set a flag that explicit ACK from DataNode is
   * not required. This function should be used only for deleting entire files.
   */
  void removePathAndBlocks(String src, List<Block> blocks) throws IOException {
    leaseManager.removeLeaseWithPrefixPath(src);
    if (blocks == null) {
      return;
    }
    for (Block b : blocks) {
      addToInvalidates(b, false);
      removeFromExcessReplicateMap(b);
      neededReplications.remove(b, -1);
      corruptReplicas.removeFromCorruptReplicasMap(b);
      blocksMap.removeBlock(b);
    }
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
    INode[] inodes = dir.rootDir.getExistingPathINodes(src);

    if (isPermissionEnabled) {
      checkTraverse(src, inodes);
    }
    return dir.getFileInfo(src, inodes[inodes.length-1]);
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
      checkTraverse(src, inodes);
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
   * Create all the necessary directories
   */
  public boolean mkdirs(String src, PermissionStatus permissions
  ) throws IOException {
    INode newNode = mkdirsInternal(src, permissions);
    getEditLog().logSync(false);
    if (newNode != null && auditLog.isInfoEnabled()) {
      final HdfsFileStatus stat = FSDirectory.getHdfsFileInfo(newNode);
      logAuditEvent(UserGroupInformation.getCurrentUGI(),
        Server.getRemoteIp(),
        "mkdirs", src, null, stat);
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
    if (!pathValidator.isValidName(src)) {
      numInvalidFilePathOperations++;
      throw new IOException("Invalid directory name: " + src);
    }
    // convert path string into an array of bytes w/o holding lock
    String[] names = INodeDirectory.getPathNames(src);
    byte[][] components = INodeDirectory.getPathComponents(names);
    INode[] inodes = new INode[components.length];

    writeLock();
    try {
      NameNode.stateChangeLog.debug("DIR* NameSystem.mkdirs: " + src);
      dir.rootDir.getExistingPathINodes(components, inodes);
      if (isPermissionEnabled) {
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
      if (isPermissionEnabled) {
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
    if (isPermissionEnabled) {
      INode[] inodes = dir.getExistingPathINodes(src);
      checkPermission(src, inodes, false, null, null, null, FsAction.READ_EXECUTE);
    }
    readLock();
    try {
      if (auditLog.isInfoEnabled()) {
        final HdfsFileStatus stat = dir.getHdfsFileInfo(src);
        logAuditEvent(UserGroupInformation.getCurrentUGI(),
          Server.getRemoteIp(),
          "getContentSummary", src, null, stat);
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
      if (isPermissionEnabled) {
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
      dir.persistBlocks(src, pendingFile);

      // record the lastBlockId until which fsync has been called
      Block last = pendingFile.getLastBlock();
      pendingFile.setLastSyncBlock(last);
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

  private void internalReleaseLeaseOne(Lease lease, String src,
                                       boolean discardLastBlock) throws IOException {
    assert hasWriteLock();

    LOG.info("Recovering lease=" + lease + ", src=" + src +
             " discardLastBlock=" + discardLastBlock);

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
   */
  void internalReleaseLeaseOne(Lease lease, String src,
        INodeFileUnderConstruction pendingFile, boolean discardLastBlock) throws IOException {
    // if it is safe to discard the last block, then do so
    if (discardLastBlock && discardDone(pendingFile, src)) {
      return;
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
        return;
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
      pendingFile.setTargets(targets);
    }
    // start lease recovery of the last block for this file.
    pendingFile.assignPrimaryDatanode();
    Lease reassignedLease = reassignLease(
      lease, src, HdfsConstants.NN_RECOVERY_LEASEHOLDER, pendingFile
    );
    leaseManager.renewLease(reassignedLease);
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
    Block lastSync = pendingFile.getLastSyncBlock();
    if (lastSync == null || lastSync.compareTo(last) != 0) {
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
    finalizeINodeFileUnderConstruction(src, null, pendingFile);
  }

  private void finalizeINodeFileUnderConstruction(String src,
      INode[] inodes, INodeFileUnderConstruction pendingFile) throws IOException {
    // Put last block in needed replication queue if uder replicated
    replicateLastBlock(src, pendingFile);

    NameNode.stateChangeLog.info("Removing lease on  file " + src + 
                                 " from client " + pendingFile.getClientName());
    leaseManager.removeLease(pendingFile.getClientName(), src);

    DatanodeDescriptor[] descriptors = pendingFile.getTargets();

    if (descriptors != null) {
      for (DatanodeDescriptor node : descriptors) {
        node.removeINode(pendingFile);
      }
    }

    // The file is no longer pending.
    // Create permanent INode, update blockmap
    INodeFile newFile = pendingFile.convertToInodeFile();
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
      final BlockInfo oldblockinfo = blocksMap.getStoredBlock(lastblock);
      if (oldblockinfo == null) {
        throw new IOException("Block (=" + lastblock + ") not found");
      }
      INodeFile iFile = oldblockinfo.getINode();
      if (!iFile.isUnderConstruction()) {
        throw new IOException("Unexpected block (=" + lastblock
          + ") since the file (=" + iFile.getLocalName()
          + ") is not under construction");
      }
      INodeFileUnderConstruction pendingFile = (INodeFileUnderConstruction) iFile;


      // Remove old block from blocks map. This always have to be done
      // because the generation stamp of this block is changing.
      blocksMap.removeBlock(oldblockinfo);

      if (deleteblock) {
        pendingFile.removeBlock(lastblock);
      } else {
        // update last block, construct newblockinfo and add it to the blocks map
        lastblock.set(lastblock.getBlockId(), newlength, newgenerationstamp);
        final BlockInfo newblockinfo = blocksMap.addINode(lastblock, pendingFile);

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

      // If this commit does not want to close the file, persist
      // blocks only if append is supported and return
      src = leaseManager.findPath(pendingFile);
      if (!closeFile) {
        if (supportAppends) {
          dir.persistBlocks(src, pendingFile);
          getEditLog().logSync();
        }
        LOG.info("commitBlockSynchronization(" + lastblock + ") successful");
        return;
      }

      //remove lease, close file
      finalizeINodeFileUnderConstruction(src, pendingFile);
    } finally {// end of synchronized section
      writeUnlock();
    }

    getEditLog().logSync();
    LOG.info("commitBlockSynchronization(newblock=" + lastblock
      + ", file=" + src
      + ", newgenerationstamp=" + newgenerationstamp
      + ", newlength=" + newlength
      + ", newtargets=" + Arrays.asList(newtargets) + ") successful");
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
    if (isPermissionEnabled) {
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
      logAuditEvent(UserGroupInformation.getCurrentUGI(),
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
      logAuditEvent(UserGroupInformation.getCurrentUGI(),
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
      logAuditEvent(UserGroupInformation.getCurrentUGI(),
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
      DatanodeDescriptor nodeN = host2DataNodeMap.getDatanodeByName(nodeReg.getName());

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
          NameNode.stateChangeLog.debug("BLOCK* NameSystem.registerDatanode: "
            + "node restarted.");
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
            nodeS.updateHeartbeat(0L, 0L, 0L, 0);
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
        NameNode.stateChangeLog.debug(
          "BLOCK* NameSystem.registerDatanode: "
            + "new storageID " + nodeReg.getStorageID() + " assigned.");
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

  private void resolveNetworkLocation(DatanodeDescriptor node) {
    List<String> names = new ArrayList<String>(1);
    if (dnsToSwitchMapping instanceof CachedDNSToSwitchMapping) {
      // get the node's IP address
      names.add(node.getHost());
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
    return Storage.getRegistrationID(dir.fsImage);
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

  private boolean isDatanodeDead(DatanodeDescriptor node) {
    return (node.getLastUpdate() <
      (now() - heartbeatExpireInterval));
  }

  private void setDatanodeDead(DatanodeDescriptor node) throws IOException {
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
                                    long capacity, long dfsUsed, long remaining,
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
        nodeinfo.updateHeartbeat(capacity, dfsUsed, remaining, xceiverCount);
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
        cmd = nodeinfo.getInvalidateBlocks(blockInvalidateLimit);
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
      totalLoad += node.getXceiverCount();
      if (!(node.isDecommissionInProgress() || node.isDecommissioned())) {
        capacityTotal += node.getCapacity();
        capacityRemaining += node.getRemaining();
      } else {
        capacityTotal += node.getDfsUsed();
      }
    } else {
      capacityUsed -= node.getDfsUsed();
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
   * Periodically calls heartbeatCheck().
   */
  class HeartbeatMonitor implements Runnable {
    /**
     */
    public void run() {
      while (fsRunning) {
        try {
          heartbeatCheck();
        } catch (Exception e) {
          FSNamesystem.LOG.error(StringUtils.stringifyException(e));
        }
        try {
          Thread.sleep(heartbeatRecheckInterval);
        } catch (InterruptedException ie) {
        }
      }
    }
  }

  /**
   * Periodically calls computeDatanodeWork().
   */
  class UnderReplicationMonitor implements Runnable {
    static final int INVALIDATE_WORK_PCT_PER_ITERATION = 32;
    static final float REPLICATION_WORK_MULTIPLIER_PER_ITERATION = 2;

    public void run() {
      while (fsRunning) {
        try {
          computeDatanodeWork();
          processPendingReplications();
          Thread.sleep(underReplicationRecheckInterval);
        } catch (InterruptedException ie) {
          LOG.warn("UnderReplicationMonitor thread received InterruptedException." + ie);
          break;
        } catch (IOException ie) {
          LOG.warn("UnderReplicationMonitor thread received exception. " + ie);
        } catch (Throwable t) {
          LOG.warn("UnderReplicationMonitor thread received Runtime exception. " + t);
          Runtime.getRuntime().exit(-1);
        }
      }
    }
  }

  /**
   * Periodically calls processOverReplicatedBlocksAsync().
   */
  class OverReplicationMonitor implements Runnable {

    public void run() {
      while (fsRunning) {
        try {
          processOverReplicatedBlocksAsync();
          configManager.reloadConfigIfNecessary();
          Thread.sleep(overReplicationRecheckInterval);
        } catch (InterruptedException ie) {
          LOG.warn("OverReplicationMonitor thread received InterruptedException." + ie);
          break;
        } catch (Throwable t) {
          LOG.warn("OverReplicationMonitor thread received Runtime exception. " + t);
          Runtime.getRuntime().exit(-1);
        }
      }
    }
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
      return workFound;
    }
    synchronized (heartbeats) {
      blocksToProcess = (int) (heartbeats.size()
        * UnderReplicationMonitor.REPLICATION_WORK_MULTIPLIER_PER_ITERATION);
      nodesToProcess = (int) Math.ceil((double) heartbeats.size()
        * UnderReplicationMonitor.INVALIDATE_WORK_PCT_PER_ITERATION / 100);
    }

    workFound = computeReplicationWork(blocksToProcess);

    // Update FSNamesystemMetrics counters
    writeLock();
    try {
      pendingReplicationBlocksCount = pendingReplications.size();
      underReplicatedBlocksCount = neededReplications.size();
      scheduledReplicationBlocksCount = workFound;
      corruptReplicaBlocksCount = corruptReplicas.size();
    } finally {
      writeUnlock();
    }

    workFound += computeInvalidateWork(nodesToProcess);
    return workFound;
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
    List<List<Block>> blocksToReplicate =
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
  List<List<Block>> chooseUnderReplicatedBlocks(int blocksToProcess) {
    // initialize data structure for the return value
    List<List<Block>> blocksToReplicate =
      new ArrayList<List<Block>>(UnderReplicatedBlocks.LEVEL);
    for (int i = 0; i < UnderReplicatedBlocks.LEVEL; i++) {
      blocksToReplicate.add(new ArrayList<Block>());
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

          Block block = neededReplicationsIterator.next();
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
  int computeReplicationWorkForBlocks(List<List<Block>> blocksToReplicate) {
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
          for (Block block : blocksToReplicate.get(priority)) {
            // block should belong to a file
            fileINode = blocksMap.getINode(block);
            // abandoned block not belong to a file
            if (fileINode == null ) {
              neededReplications.remove(block, priority); // remove from neededReplications
              replIndex[priority]--;
              continue;
            }
            requiredReplication = fileINode.getReplication();

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
        synchronized (neededReplications) {
          Block block = rw.block;
          priority = rw.priority;
          // Recheck since global lock was released
          // block should belong to a file
          fileINode = blocksMap.getINode(block);
          // abandoned block not belong to a file
          if (fileINode == null ) {
            neededReplications.remove(block, priority); // remove from neededReplications
            rw.targets = null;
            replIndex[priority]--;
            continue;
          }
          requiredReplication = fileINode.getReplication();

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
          NameNode.stateChangeLog.debug(
            "BLOCK* block " + block
              + " is moved from neededReplications to pendingReplications");

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
   * Wrapper function for choosing targets for replication.
   */
  private DatanodeDescriptor[] chooseTarget(ReplicationWork work) {
    return replicator.chooseTarget(work.fileINode,
        work.numOfReplicas, work.srcNode,
        work.containingNodes, null, work.block.getNumBytes());
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
      blocksToInvalidate = invalidateSet.pollN(blockInvalidateLimit);

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
    Block[] timedOutItems = pendingReplications.getTimedOutBlocks();
    if (timedOutItems != null) {
      writeLock();
      try {
        for (int i = 0; i < timedOutItems.length; i++) {
          NumberReplicas num = countNodes(timedOutItems[i]);
          neededReplications.add(timedOutItems[i],
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

    for (Iterator<Block> it = nodeInfo.getBlockIterator(); it.hasNext();) {
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
    NameNode.stateChangeLog.debug(
      "BLOCK* NameSystem.unprotectedRemoveDatanode: "
        + nodeDescr.getName() + " is out of service now.");
  }

  void unprotectedAddDatanode(DatanodeDescriptor nodeDescr) {
    /* To keep host2DataNodeMap consistent with datanodeMap,
       remove  from host2DataNodeMap the datanodeDescriptor removed
       from datanodeMap before adding nodeDescr to host2DataNodeMap.
    */
    host2DataNodeMap.remove(
      datanodeMap.put(nodeDescr.getStorageID(), nodeDescr));
    host2DataNodeMap.add(nodeDescr);

    NameNode.stateChangeLog.debug(
      "BLOCK* NameSystem.unprotectedAddDatanode: "
        + "node " + nodeDescr.getName() + " is added to datanodeMap.");
  }

  /**
   * Physically remove node from datanodeMap.
   *
   * @param nodeID node
   */
  void wipeDatanode(DatanodeID nodeID) throws IOException {
    String key = nodeID.getStorageID();
    host2DataNodeMap.remove(datanodeMap.remove(key));
    NameNode.stateChangeLog.debug(
      "BLOCK* NameSystem.wipeDatanode: "
        + nodeID.getName() + " storage " + key
        + " is removed from datanodeMap.");
  }

  FSImage getFSImage() {
    return dir.fsImage;
  }

  FSEditLog getEditLog() {
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
    if (isInSafeMode()) {
      // not to check dead nodes if in safemode 
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
  
  public void processBlocksBeingWrittenReport(DatanodeID nodeID, 
      BlockListAsLongs blocksBeingWritten) 
  throws IOException {
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
              "Block not in blockMap with any generation stamp");
          continue;
        }

        INodeFile inode = storedBlock.getINode();
        if (inode == null) {
          rejectAddStoredBlock(
              new Block(block), dataNode,
              "Block does not correspond to any file");
          continue;
        }

        boolean underConstruction = inode.isUnderConstruction();
        boolean isLastBlock = inode.getLastBlock() != null &&
                   inode.getLastBlock().getBlockId() == block.getBlockId();

        // Must be the last block of a file under construction,
        if (!underConstruction) {
          rejectAddStoredBlock(
              new Block(block), dataNode,
              "Reported as block being written but is a block of closed file.");
          continue;
        }

        if (!isLastBlock) {
          rejectAddStoredBlock(
              new Block(block), dataNode,
              "Reported as block being written but not the last block of " +
              "an under-construction file.");
          continue;
        }

        INodeFileUnderConstruction pendingFile = 
                            (INodeFileUnderConstruction)inode;
        pendingFile.addTarget(dataNode);
        incrementSafeBlockCount(pendingFile.getTargets().length);
      }
    } finally {
      writeUnlock();
    }
  }
  
  /**
   * The given node is reporting all its blocks.  Use this info to
   * update the (machine-->blocklist) and (block-->machinelist) tables.
   */
  public void processReport(DatanodeID nodeID,
                            BlockListAsLongs newReport
  ) throws IOException {
    int processTime;
    Collection<Block> toAdd = null, toRemove = null, toInvalidate = null;
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
      
      // To minimize startup time, we discard any second (or later) block reports
      // that we receive while still in startup phase.
      if (isInStartupSafeMode() && node.reportReceived()) {
        NameNode.stateChangeLog.info("BLOCK* NameSystem.processReport: "
            + "discarded non-initial block report from " + nodeID.getName()
            + " because namenode still in startup phase");
        return;
      }
      
      // check the case when the NN does not know of any replicas on this
      // datanode. This typically happens when the NN restarts and the first
      // report from this datanode is being processed. Short-circuit the
      // processing in this case: just add all these replicas to this
      // datanode. This helps NN restart times tremendously.
      node.setReportReceived(true);
      if (node.numBlocks() == 0) {
        NameNode.stateChangeLog.info("BLOCK* NameSystem.processReport: "
          + "from " + nodeID.getName() + " " +
          newReport.getNumberOfBlocks() + " blocks" +
          " shortCircuit first report.");
        Block iblk = new Block(); // a fixed new'ed block to be reused with index i
        for (int i = 0; i < newReport.getNumberOfBlocks(); ++i) {
          iblk.set(newReport.getBlockId(i), newReport.getBlockLen(i),
            newReport.getBlockGenStamp(i));
          addStoredBlock(iblk, node, null);
        }
        
        dnReporting++;

        if(isInSafeMode()) {
          LOG.info("BLOCK* NameSystem.processReport:" + dnReporting +
                   " data nodes reporting, " +
                   safeMode.blockSafe + "/" + safeMode.blockTotal +
                   " blocks safe (" + safeMode.getSafeBlockRatio() + ")");
        }
      } else {

        //
        // Modify the (block-->datanode) map, according to the difference
        // between the old and new block report.
        //
        toAdd = new LinkedList<Block>();
        toRemove = new LinkedList<Block>();
        toInvalidate = new LinkedList<Block>();
        node.reportDiff(blocksMap, newReport, toAdd, toRemove, toInvalidate);

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
      writeUnlock();
    }
    if (toInvalidate != null) {
      for (Block b : toInvalidate) {
        NameNode.stateChangeLog.info("BLOCK* NameSystem.processReport: block "
          + b + " on " + nodeID.getName() + " size " + b.getNumBytes()
          + " does not belong to any file.");
      }
    }
    NameNode.stateChangeLog.info("BLOCK* NameSystem.processReport: from "
        + nodeID.getName() + " with " + newReport.getNumberOfBlocks()
        + " blocks took " + processTime + "ms"
        + (toAdd == null ? "." : (": #toAdd = " + toAdd.size() +
                                 " #toRemove = " + toRemove.size() +
                                 " #toInvalidate = " + toInvalidate.size() +
                                 ".")));
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
   * Modify (block-->datanode) map.  Remove block from set of
   * needed replications if this takes care of the problem.
   *
   * @return the block that is stored in blockMap.
   */
  private Block addStoredBlock(Block block,
                               DatanodeDescriptor node,
                               DatanodeDescriptor delNodeHint) {
    assert (hasWriteLock());
    BlockInfo storedBlock = blocksMap.getStoredBlock(block);
    if (storedBlock == null) {
      // then we need to do some special processing.
      storedBlock = blocksMap.getStoredBlockWithoutMatchingGS(block);

      if (storedBlock == null) {
        return rejectAddStoredBlock(
          new Block(block), node,
          "Block not in blockMap with any generation stamp");
      }

      INodeFile inode = storedBlock.getINode();
      if (inode == null) {
        return rejectAddStoredBlock(
          new Block(block), node,
          "Block does not correspond to any file");
      }

      boolean reportedOldGS = block.getGenerationStamp() < storedBlock.getGenerationStamp();
      boolean reportedNewGS = block.getGenerationStamp() > storedBlock.getGenerationStamp();
      boolean underConstruction = inode.isUnderConstruction();
      boolean isLastBlock = inode.getLastBlock() != null &&
        inode.getLastBlock().getBlockId() == block.getBlockId();

      // We can report a stale generation stamp for the last block under construction,
      // we just need to make sure it ends up in targets.
      if (reportedOldGS && !(underConstruction && isLastBlock)) {
        return rejectAddStoredBlock(
          new Block(block), node,
          "Reported block has old generation stamp but is not the last block of " +
          "an under-construction file. (current generation is " +
          storedBlock.getGenerationStamp() + ")");
      }

      // Don't add blocks to the DN when they're part of the in-progress last block
      // and have an inconsistent generation stamp. Instead just add them to targets
      // for recovery purposes. They will get added to the node when
      // commitBlockSynchronization runs
      if (underConstruction && isLastBlock && (reportedOldGS || reportedNewGS)) {
        NameNode.stateChangeLog.info(
          "BLOCK* NameSystem.addStoredBlock: "
          + "Targets updated: block " + block + " on " + node.getName() +
          " is added as a target for block " + storedBlock + " with size " +
          block.getNumBytes());
        ((INodeFileUnderConstruction)inode).addTarget(node);
        return block;
      }
    }

    INodeFile fileINode = storedBlock.getINode();
    if (fileINode == null) {
      return rejectAddStoredBlock(
          new Block(block), node,
          "Block does not correspond to any file");
    }

    assert storedBlock != null : "Block must be stored by now";

    // add block to the data-node
    boolean added = node.addBlock(storedBlock);
    
    // Is the block being reported the last block of an underconstruction file?
    boolean blockUnderConstruction = false;
    if (fileINode.isUnderConstruction()) {
      INodeFileUnderConstruction cons = (INodeFileUnderConstruction) fileINode;
      Block last = fileINode.getLastBlock();
      if (last == null) {
        // This should never happen, but better to handle it properly than to throw
        // an NPE below.
        LOG.error("Null blocks for reported block=" + block + " stored=" + storedBlock +
          " inode=" + fileINode);
        return block;
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
          markBlockAsCorrupt(block, node);
        } catch (IOException e) {
          LOG.warn("Error in deleting bad block " + block + e);
        }
      } else {
        long cursize = storedBlock.getNumBytes();
        INodeFile file = storedBlock.getINode();
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
            LOG.info(logMsg);
          } else {
            LOG.warn(logMsg);
          }

          try {
            if (cursize > block.getNumBytes() && !blockUnderConstruction) {
              // new replica is smaller in size than existing block.
              // Mark the new replica as corrupt.
              LOG.warn("Mark new replica " + block + " from " + node.getName() +
                "as corrupt because its length is shorter than existing ones");
              markBlockAsCorrupt(block, node);
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
                  markBlockAsCorrupt(block, nodes[j]);
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

        //Updated space consumed if required.
        long diff = (file == null) ? 0 :
          (file.getPreferredBlockSize() - storedBlock.getNumBytes());

        if (diff > 0 && file.isUnderConstruction() &&
          cursize < storedBlock.getNumBytes()) {
          try {
            String path = /* For finding parents */
              leaseManager.findPath((INodeFileUnderConstruction) file);
            dir.updateSpaceConsumed(path, 0, -diff * file.getReplication());
          } catch (IOException e) {
            LOG.warn("Unexpected exception while updating disk space : " +
              e.getMessage());
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
    NumberReplicas num = null;
    int numCurrentReplica = 0;
    int numLiveReplicas = 0;

    boolean popReplQueuesBefore = isPopulatingReplQueues();

    if (!popReplQueuesBefore) {
      // if we haven't populated the replication queues
      // then use a cheaper method to count
      // only live replicas, that's all we need.
      numLiveReplicas = countLiveNodes(storedBlock);
      numCurrentReplica = numLiveReplicas;
    } else {
      // count live & decommissioned replicas
      num = countNodes(storedBlock);
      numLiveReplicas = num.liveReplicas();
      numCurrentReplica = numLiveReplicas + pendingReplications.getNumReplicas(block);
    }

    // check whether safe replication is reached for the block
    if (!node.isDecommissionInProgress() && !node.isDecommissioned()) {
      incrementSafeBlockCount(numCurrentReplica);
    }

    if (!popReplQueuesBefore && isPopulatingReplQueues()) {
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
    if (blockUnderConstruction && fileINode.isLastBlock(storedBlock)) {
      INodeFileUnderConstruction cons = (INodeFileUnderConstruction)fileINode;
      cons.addTarget(node);
      return block;
    }

    // do not handle mis-replicated blocks during start up
    if (!isPopulatingReplQueues()) {
      return block;
    }

    // handle underReplication/overReplication
    short fileReplication = fileINode.getReplication();
    if (numCurrentReplica >= fileReplication) {
      neededReplications.remove(block, numCurrentReplica-1,
        num.decommissionedReplicas, fileReplication);
    } else {
      updateNeededReplications(block, curReplicaDelta, 0);
    }
    if (numCurrentReplica > fileReplication) {
      processOverReplicatedBlock(block, fileReplication, node, delNodeHint);
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
    if ((corruptReplicasCount > 0) && (numLiveReplicas >= fileReplication)) {
      invalidateCorruptReplicas(block);
    }
    return block;
  }

  /**
   * Log a rejection of an addStoredBlock RPC, invalidate the reported block,
   * and return it.
   */
  private Block rejectAddStoredBlock(Block block,
                                     DatanodeDescriptor node,
                                     String msg) {
      if (!isInSafeMode()) {
        NameNode.stateChangeLog.info("BLOCK* NameSystem.addStoredBlock: "
                                     + "addStoredBlock request received for "
                                     + block + " on " + node.getName()
                                     + " size " + block.getNumBytes()
                                     + " but was rejected: " + msg);
      }
      addToInvalidates(block, node, false);
      return block;
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
    NameNode.stateChangeLog.debug("NameNode.invalidateCorruptReplicas: " +
        "invalidating corrupt replicas on " + nodes.size() + "nodes");
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
   * For each block in the name-node verify whether it belongs to any file,
   * over or under replicated. Place it into the respective queue.
   */
  private void processMisReplicatedBlocks() {
    writeLock();
    try {
      long nrInvalid = 0, nrOverReplicated = 0, nrUnderReplicated = 0;
      neededReplications.clear();
      for (BlocksMap.BlockInfo block : blocksMap.getBlocks()) {
        INodeFile fileINode = block.getINode();
        if (fileINode == null) {
          // block does not belong to any file
          nrInvalid++;
          addToInvalidates(block, false);
          continue;
        }
        // If this is a last block of a file under construction ignore it.
        if (fileINode.getLastBlock().equals(block)
            && fileINode.isUnderConstruction()) {
          continue;
        }

        // calculate current replication
        short expectedReplication = fileINode.getReplication();
        NumberReplicas num = countNodes(block);
        int numCurrentReplica = num.liveReplicas();
        // add to under-replicated queue if need to be
        if (neededReplications.add(block,
          numCurrentReplica,
          num.decommissionedReplicas(),
          expectedReplication)) {
          nrUnderReplicated++;
        }

        if (numCurrentReplica > expectedReplication) {
          // over-replicated block
          nrOverReplicated++;
          overReplicatedBlocks.add(block);
        }
      }
      LOG.info("Total number of blocks = " + blocksMap.size());
      LOG.info("Number of invalid blocks = " + nrInvalid);
      LOG.info("Number of under-replicated blocks = " + nrUnderReplicated);
      LOG.info("Number of  over-replicated blocks = " + nrOverReplicated);
    } finally {
      writeUnlock();
    }
  }

  /**
   * This is called from the ReplicationMonitor to process over
   * replicated blocks.
   */
  private void processOverReplicatedBlocksAsync() {

    List<Block> blocksToProcess = new LinkedList<Block>();
    writeLock();
    try {
      int size = Math.min(overReplicatedBlocks.size(), 1000);
      NameNode.getNameNodeMetrics().numOverReplicatedBlocks.set(overReplicatedBlocks.size());
      blocksToProcess = overReplicatedBlocks.pollN(size);
    } finally {
      writeUnlock();
    }

    for (Block block : blocksToProcess) {
      NameNode.stateChangeLog
          .debug("BLOCK* NameSystem.processOverReplicatedBlocksAsync: " + block);
      processOverReplicatedBlock(block, (short) -1, null, null);
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

      INodeFile inode = blocksMap.getINode(block);
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

      // loop through datanodes that have excess-replicas of this block
      for (ListIterator<DatanodeID> iter = excessReplicateTmp.listIterator();
           iter.hasNext();) {
        DatanodeID datanodeId = iter.next();

        // re-check that block still has excess replicas.
        // If not, then there is nothing more to do.
        if (live <= inode.getReplication()) {
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
        Collection<Block> excessBlocks = excessReplicateMap.get(datanodeId.getStorageID());
        if (excessBlocks == null) {
          excessBlocks = new TreeSet<Block>();
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
      inode = blocksMap.getINode(block);
      if (inode == null) {
        return; // file has been deleted already, nothing to do.
      }

      // if the caller did not specify what the target replication factor
      // of the file, then fetch it from the inode. This happens when invoked
      // by the ReplicationMonitor thread.
      if (replication < 0) {
        replication = inode.getReplication();
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
   * The algorithm is first to pick a node with least free space from nodes
   * that are on a rack holding more than one replicas of the block.
   * So removing such a replica won't remove a rack.
   * If no such a node is available,
   * then pick a node with least free space
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
    // otherwise pick one with least space from priSet if it is not empty
    // otherwise one node with least space from remains
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
          blockReceived(receivedAndDeletedBlocks[i],
              receivedAndDeletedBlocks[i].getDelHints(),node);
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
        if (blk.getNumBytes() != BlockCommand.NO_ACK) {       
          NameNode.stateChangeLog.info("BLOCK* NameSystem.blockReceived: "
              + "blockMap updated: " + nodeReg.getName() + " is added to "
              + receivedAndDeletedBlocks[i] + " size "
              + receivedAndDeletedBlocks[i].getNumBytes());
        } else {
          NameNode.stateChangeLog.info("BLOCK* NameSystem.blockReceived: "
              + "for block "
              + receivedAndDeletedBlocks[i] + " was rejected");
        }
      }
    }
  }

  public void blockReceivedAndDeleted(DatanodeRegistration nodeReg,
      ReceivedDeletedBlockInfo receivedAndDeletedBlocks[]) throws IOException {
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
        if (receivedAndDeletedBlocks[i].isDeletedBlock()) {
          removeStoredBlock(receivedAndDeletedBlocks[i].getBlock(), node);
          deleted++;
        } else {
          blockReceived(receivedAndDeletedBlocks[i].getBlock(),
              receivedAndDeletedBlocks[i].getDelHints(),node);
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
      int count = 0;
      // At startup time, because too many new blocks come in
      // they take up lots of space in the log file.
      // So, we log only when namenode is out of safemode.
      // We log only blockReceived messages.
      for (int i = 0; i < receivedAndDeletedBlocks.length; i++) {
        // log only the ones that have been processed
        if (++count > received) {
          break;
        }
        ReceivedDeletedBlockInfo blk = receivedAndDeletedBlocks[i];
        if ((blk != null) && !blk.isDeletedBlock()
            && blk.getBlock().getNumBytes() != BlockCommand.NO_ACK) {       
          NameNode.stateChangeLog.info("BLOCK* NameSystem.blockReceived: "
              + "blockMap updated: " + nodeReg.getName() + " is added to "
              + receivedAndDeletedBlocks[i].getBlock() + " size "
              + receivedAndDeletedBlocks[i].getBlock().getNumBytes());
        } else if ((blk != null) && !blk.isDeletedBlock()
            && blk.getBlock().getNumBytes() == BlockCommand.NO_ACK) {
          NameNode.stateChangeLog.info("BLOCK* NameSystem.blockReceived: "
              + "for block "
              + receivedAndDeletedBlocks[i].getBlock() + " was rejected");
        }
      }
    }
  }
  
  /**
   * Modify (block-->datanode) map.  Possibly generate
   * replication tasks, if the removed block is still valid.
   */
  private void removeStoredBlock(Block block, DatanodeDescriptor node) {
    assert (hasWriteLock());
    NameNode.stateChangeLog.debug("BLOCK* NameSystem.removeStoredBlock: "
      + block + " from " + node.getName());
    if (!blocksMap.removeNode(block, node)) {
      NameNode.stateChangeLog.debug("BLOCK* NameSystem.removeStoredBlock: "
        + block + " has already been removed from node " + node);
      return;
    }

    //
    // if file is being actively written to and it is the last block, 
    // then do not check replication-factor here.
    //
    BlockInfo storedBlock = blocksMap.getStoredBlock(block);
    INodeFile fileINode = storedBlock == null ? null : storedBlock.getINode();
    if (fileINode != null && 
        fileINode.isUnderConstruction() && fileINode.isLastBlock(storedBlock)
        && !node.isDecommissionInProgress() && !node.isDecommissioned()) {          
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
      updateNeededReplications(block, -1, 0);
    }

    //
    // We've removed a block from a node, so it's definitely no longer
    // in "excess" there.
    //
    removeFromExcessReplicateMap(block, node);

    // Remove the replica from corruptReplicas
    corruptReplicas.removeFromCorruptReplicasMap(block, node);
  }
  
  private void removeFromExcessReplicateMap(Block block, DatanodeDescriptor node){
    assert (hasWriteLock());
    Collection<Block> excessBlocks = excessReplicateMap.get(node.getStorageID());
    if (excessBlocks != null) {
      if (excessBlocks.remove(block)) {
        excessBlocksCount--;
        NameNode.stateChangeLog.debug("BLOCK* NameSystem.removeFromExcessReplicateMap: "
          + block + " is removed from excessBlocks");
        if (excessBlocks.size() == 0) {
          excessReplicateMap.remove(node.getStorageID());
        }
      }
    }
  }
  
  private void removeFromExcessReplicateMap(Block block){
    assert (hasWriteLock());
    for (Iterator<DatanodeDescriptor> it =
      blocksMap.nodeIterator(block); it.hasNext();) {
      DatanodeDescriptor node = it.next();
      removeFromExcessReplicateMap(block, node);
    }
  }

  /**
   * The given node is reporting that it received a certain block.
   */
  private void blockReceived(Block block,
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
    addStoredBlock(block, node, delHintNode);
  }

  public long getMissingBlocksCount() {
    // not locking
    return neededReplications.getCorruptBlocksCount();
  }

  long[] getStats() throws IOException {
    checkSuperuserPrivilege();
    synchronized (heartbeats) {
      return new long[]{this.capacityTotal, this.capacityUsed,
        this.capacityRemaining,
        this.underReplicatedBlocksCount,
        this.corruptReplicaBlocksCount,
        getMissingBlocksCount()};
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

  private ArrayList<DatanodeDescriptor> getDatanodeListForReport(
    DatanodeReportType type) {
    readLock();
    try {

      boolean listLiveNodes = type == DatanodeReportType.ALL ||
        type == DatanodeReportType.LIVE;
      boolean listDeadNodes = type == DatanodeReportType.ALL ||
        type == DatanodeReportType.DEAD;

      HashSet<String> mustList = new HashSet<String>();

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
          dn.setLastUpdate(0);
          nodes.add(dn);
        }
      }

      return nodes;
    } finally {
      readUnlock();
    }
  }

  public DatanodeInfo[] datanodeReport(DatanodeReportType type
  ) throws AccessControlException {
    readLock();
    try {
      checkSuperuserPrivilege();

      ArrayList<DatanodeDescriptor> results = getDatanodeListForReport(type);
      DatanodeInfo[] arr = new DatanodeInfo[results.size()];
      for (int i = 0; i < arr.length; i++) {
        arr[i] = new DatanodeInfo(results.get(i));
      }
      return arr;
    } finally {
      readUnlock();
    }
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
    writeLock();
    try {
      checkSuperuserPrivilege();
      if(!force && !isInSafeMode()) {
        throw new IOException("Safe mode should be turned ON " +
          "in order to create namespace image.");
      }
      getFSImage().saveNamespace(uncompressed, true);
      LOG.info("New namespace image has been created.");
    } finally {
      writeUnlock();
    }
  }

  /**
   */
  public void DFSNodesStatus(ArrayList<DatanodeDescriptor> live,
                                          ArrayList<DatanodeDescriptor> dead) {
    readLock();
    try {
      ArrayList<DatanodeDescriptor> results =
        getDatanodeListForReport(DatanodeReportType.ALL);
      for (Iterator<DatanodeDescriptor> it = results.iterator(); it.hasNext();) {
        DatanodeDescriptor node = it.next();
        if (isDatanodeDead(node)) {
          dead.add(node);
        } else {
          live.add(node);
        }
      }
    } finally {
      readUnlock();
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
  private void startDecommission(DatanodeDescriptor node)
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
    }
  }
  
  /**
   * Stop decommissioning the specified datanodes.
   */
  private void stopDecommission(DatanodeDescriptor node)
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
        Collection<Block> blocksExcess =
          excessReplicateMap.get(node.getStorageID());
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
   * Return the number of nodes that are live
   */
  int countLiveNodes(Block b) {
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
  Block isReplicationInProgress(final DecommissioningStatus status,
      final DatanodeDescriptor srcNode, final Block block,
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
   * Keeps track of which datanodes/ipaddress are allowed to connect to the namenode.
   */
  private boolean inHostsList(DatanodeID node, String ipAddr) {
    Set<String> hostsList = hostsReader.getHosts();
    return (hostsList.isEmpty() ||
      (ipAddr != null && hostsList.contains(ipAddr)) ||
      hostsList.contains(node.getHost()) ||
      hostsList.contains(node.getName()) ||
      ((node instanceof DatanodeInfo) &&
        hostsList.contains(((DatanodeInfo) node).getHostName())));
  }

  public boolean inExcludedHostsList(DatanodeID node, String ipAddr) {
    Set<String> excludeList = hostsReader.getExcludedHosts();
    return ((ipAddr != null && excludeList.contains(ipAddr)) ||
      excludeList.contains(node.getHost()) ||
      excludeList.contains(node.getName()) ||
      ((node instanceof DatanodeInfo) &&
        excludeList.contains(((DatanodeInfo) node).getHostName())));
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
    checkSuperuserPrivilege();
    // Reread the config to get dfs.hosts and dfs.hosts.exclude filenames.
    // Update the file names and refresh internal includes and excludes list
    if (conf == null) {
      conf = new Configuration();
    }
    hostsReader.updateFileNames(conf.get("dfs.hosts", ""),
      conf.get("dfs.hosts.exclude", ""));
    hostsReader.refresh();
    replicator.hostsUpdated();
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
   * SafeModeInfo contains information related to the safe mode.
   * <p/>
   * An instance of {@link SafeModeInfo} is created when the name node
   * enters safe mode.
   * <p/>
   * During name node startup {@link SafeModeInfo} counts the number of
   * <em>safe blocks</em>, those that have at least the minimal number of
   * replicas, and calculates the ratio of safe blocks to the total number
   * of blocks in the system, which is the size of
   * {@link FSNamesystem#blocksMap}. When the ratio reaches the
   * {@link #threshold} it starts the {@link SafeModeMonitor} daemon in order
   * to monitor whether the safe mode {@link #extension} is passed.
   * Then it leaves safe mode and destroys itself.
   * <p/>
   * If safe mode is turned on manually then the number of safe blocks is
   * not tracked because the name node is not intended to leave safe mode
   * automatically in the case.
   *
   * @see ClientProtocol#setSafeMode(FSConstants.SafeModeAction)
   * @see SafeModeMonitor
   */
  class SafeModeInfo {
    // configuration fields
    /**
     * Safe mode threshold condition %.
     */
    private double threshold;
    /**
     * Safe mode extension after the threshold.
     */
    private long extension;
    /**
     * Min replication required by safe mode.
     */
    private int safeReplication;
    /** threshold for populating needed replication queues */
    private double replQueueThreshold;

    // internal fields
    /**
     * Time when threshold was reached.
     * <p/>
     * <br>-1 safe mode is off
     * <br> 0 safe mode is on, but threshold is not reached yet
     */
    private long reached = -1;
    /**
     * Total number of blocks.
     */
    int blockTotal;
    /**
     * Number of safe blocks.
     */
    private int blockSafe;
    /** Number of blocks needed before populating replication queues */
    private int blockReplQueueThreshold;
    /**
     * time of the last status printout
     */
    private long lastStatusReport = 0;
    /** flag indicating whether replication queues have been initialized */
    private boolean initializedReplQueues = false;

    /**
     * Creates SafeModeInfo when the name node enters
     * automatic safe mode at startup.
     *
     * @param conf configuration
     */
    SafeModeInfo(Configuration conf) {
      this.threshold = conf.getFloat("dfs.safemode.threshold.pct", 0.95f);
      this.extension = conf.getLong("dfs.safemode.extension", 0);
      // default to safe mode threshold
      // (i.e., don't populate queues before leaving safe mode)
      this.replQueueThreshold =
        conf.getFloat("dfs.namenode.replqueue.threshold-pct",
                      (float) threshold);
      this.safeReplication = conf.getInt("dfs.replication.min", 1);
      this.blockTotal = 0;
      this.blockSafe = 0;
    }

    /**
     * Creates SafeModeInfo when safe mode is entered manually.
     * <p/>
     * The {@link #threshold} is set to 1.5 so that it could never be reached.
     * {@link #blockTotal} is set to -1 to indicate that safe mode is manual.
     *
     * @see SafeModeInfo
     */
    private SafeModeInfo() {
      this.threshold = 1.5f;  // this threshold can never be reached
      this.extension = Long.MAX_VALUE;
      this.safeReplication = Short.MAX_VALUE + 1; // more than maxReplication
      this.replQueueThreshold = 1.5f; // can never be reached
      this.blockTotal = -1;
      this.blockSafe = -1;
      this.reached = -1;
      enter();
      reportStatus("STATE* Safe mode is ON.", true);
    }

    /**
     * Check if safe mode is on.
     *
     * @return true if in safe mode
     */
    synchronized boolean isOn() {
      try {
        assert isConsistent() : " SafeMode: Inconsistent filesystem state: "
          + "Total num of blocks, active blocks, or "
          + "total safe blocks don't match.";
      } catch (IOException e) {
        System.err.print(StringUtils.stringifyException(e));
      }
      return this.reached >= 0;
    }

    /**
     * Check if we are populating replication queues.
     */
    synchronized boolean isPopulatingReplQueues() {
      return initializedReplQueues;
    }

    /**
     * Enter safe mode.
     */
    void enter() {
      this.reached = 0;
    }

    /**
     * Leave safe mode.
     * <p/>
     * Switch to manual safe mode if distributed upgrade is required.<br>
     * Check for invalid, under- & over-replicated blocks in the end of startup.
     */
    synchronized void leave(boolean checkForUpgrades) {
      if (checkForUpgrades) {
        // verify whether a distributed upgrade needs to be started
        boolean needUpgrade = false;
        try {
          needUpgrade = startDistributedUpgradeIfNeeded();
        } catch (IOException e) {
          FSNamesystem.LOG.error(StringUtils.stringifyException(e));
        }
        if (needUpgrade) {
          // switch to manual safe mode
          safeMode = new SafeModeInfo();
          return;
        }
      }
      // if not done yet, initialize replication queues
      if (!isPopulatingReplQueues()) {
        initializeReplQueues();
      }
      long timeInSafemode = now() - systemStart;
      NameNode.stateChangeLog.info("STATE* Leaving safe mode after "
        + timeInSafemode / 1000 + " secs.");
      NameNode.getNameNodeMetrics().safeModeTime.set((int) timeInSafemode);

      if (reached >= 0) {
        NameNode.stateChangeLog.info("STATE* Safe mode is OFF.");
      }
      reached = -1;
      safeMode = null;
      try {
        nameNode.startServerForClientRequests();
      } catch (IOException ex) {
        nameNode.stop();
      }
      NameNode.stateChangeLog.info("STATE* Network topology has "
        + clusterMap.getNumOfRacks() + " racks and "
        + clusterMap.getNumOfLeaves() + " datanodes");
      NameNode.stateChangeLog.info("STATE* UnderReplicatedBlocks has "
        + neededReplications.size() + " blocks");
    }

    /**
     * Initialize replication queues.
     */
    synchronized void initializeReplQueues() {
      LOG.info("initializing replication queues");
      if (isPopulatingReplQueues()) {
        LOG.warn("Replication queues already initialized.");
      }
      processMisReplicatedBlocks();
      initializedReplQueues = true;
    }

    /**
     * Check whether we have reached the threshold for
     * initializing replication queues.
     */
    synchronized boolean canInitializeReplQueues() {
      return blockSafe >= blockReplQueueThreshold;
    }

    /**
     * Safe mode can be turned off iff
     * the threshold is reached and
     * the extension time have passed.
     *
     * @return true if can leave or false otherwise.
     */
    synchronized boolean canLeave() {
      if (reached == 0) {
        return false;
      }
      if (now() - reached < extension) {
        reportStatus("STATE* Safe mode ON.", false);
        return false;
      }
      return !needEnter();
    }

    /**
     * There is no need to enter safe mode
     * if DFS is empty or {@link #threshold} == 0
     */
    boolean needEnter() {
      return getSafeBlockRatio() < threshold;
    }

    /**
     * Ratio of the number of safe blocks to the total number of blocks
     * to be compared with the threshold.
     */
    private double getSafeBlockRatio() {
      return (blockTotal == blockSafe ? 1 : (double) blockSafe / (double) blockTotal);
    }

    /**
     * Check and trigger safe mode if needed.
     */
    private void checkMode() {
      if (needEnter()) {
        enter();
        // check if we are ready to initialize replication queues
        if (canInitializeReplQueues() && !isPopulatingReplQueues()) {
          initializeReplQueues();
        }
        reportStatus("STATE* Safe mode ON.", false);
        return;
      }
      // the threshold is reached
      if (!isOn() ||                           // safe mode is off
        extension <= 0 || threshold <= 0) {  // don't need to wait
        this.leave(true); // leave safe mode
        return;
      }
      if (reached > 0) {  // threshold has already been reached before
        reportStatus("STATE* Safe mode ON.", false);
        return;
      }
      // start monitor
      reached = now();
      smmthread = new Daemon(new SafeModeMonitor());
      smmthread.start();
      reportStatus("STATE* Safe mode extension entered.", true);

      // check if we are ready to initialize replication queues
      if (canInitializeReplQueues() && !isPopulatingReplQueues()) {
        initializeReplQueues();
      }
    }

    /**
     * Set total number of blocks.
     */
    synchronized void setBlockTotal(int total) {
      this.blockTotal = total;
      this.blockReplQueueThreshold =
        (int) (((double) blockTotal) * replQueueThreshold);
      checkMode();
    }

    /**
     * Increment number of safe blocks if current block has
     * reached minimal replication.
     *
     * @param replication current replication
     */
    synchronized void incrementSafeBlockCount(short replication) {
      if ((int) replication == safeReplication) {
        this.blockSafe++;
        checkMode();
      }
    }

    /**
     * Decrement number of safe blocks if current block has
     * fallen below minimal replication.
     *
     * @param replication current replication
     */
    synchronized void decrementSafeBlockCount(short replication) {
      if (replication == safeReplication - 1) {
        this.blockSafe--;
        checkMode();
      }
    }

    /**
     * Check if safe mode was entered manually or at startup.
     */
    boolean isManual() {
      return extension == Long.MAX_VALUE;
    }

    /**
     * Set manual safe mode.
     */
    void setManual() {
      extension = Long.MAX_VALUE;
    }

    /**
     * A tip on how safe mode is to be turned off: manually or automatically.
     */
    String getTurnOffTip() {
      String leaveMsg = "Safe mode will be turned off automatically";
      if (reached < 0) {
        return "Safe mode is OFF.";
      }
      if (isManual()) {
        if (getDistributedUpgradeState()) {
          return leaveMsg + " upon completion of " +
            "the distributed upgrade: upgrade progress = " +
            getDistributedUpgradeStatus() + "%";
        }
        leaveMsg = "Use \"hadoop dfsadmin -safemode leave\" to turn safe mode off";
      }
      if (blockTotal < 0) {
        return leaveMsg + ".";
      }
      String safeBlockRatioMsg =
        String.format("The ratio of reported blocks %.8f has " +
          (reached == 0 ? "not " : "") + "reached the threshold %.8f. ",
          getSafeBlockRatio(), threshold) +
          "Safe blocks = " + blockSafe +
          ", Total blocks = " + blockTotal +
          ", Remaining blocks = " + (blockTotal-blockSafe) + ". " +
          "Reporting nodes = " + dnReporting + ". " +
          leaveMsg;
      if (reached == 0 || isManual())  // threshold is not reached or manual
      {
        return safeBlockRatioMsg + ".";
      }
      // extension period is in progress
      return safeBlockRatioMsg + " in "
        + Math.abs(reached + extension - now()) / 1000 + " seconds.";
    }

    /**
     * Print status every 20 seconds.
     */
    private void reportStatus(String msg, boolean rightNow) {
      long curTime = now();
      if (!rightNow && (curTime - lastStatusReport < 20 * 1000)) {
        return;
      }
      NameNode.stateChangeLog.info(msg + " \n" + getTurnOffTip());
      lastStatusReport = curTime;
    }

    /**
     * Returns printable state of the class.
     */
    public String toString() {
      String resText = "Current safe block ratio = "
        + getSafeBlockRatio()
        + ". Safe blocks = " + blockSafe
        + ". Total blocks = " + blockTotal
        + ". Target threshold = " + threshold
        + ". Minimal replication = " + safeReplication + ".";
      if (reached > 0) {
        resText += " Threshold was reached " + new Date(reached) + ".";
      }
      return resText;
    }

    /**
     * Checks consistency of the class state.
     * This is costly and currently called only in assert.
     */
    boolean isConsistent() throws IOException {
      if (blockTotal == -1 && blockSafe == -1) {
        return true; // manual safe mode
      }
      int activeBlocks = blocksMap.size() - (int) pendingDeletionBlocksCount;
      return (blockTotal == activeBlocks) ||
        (blockSafe >= 0 && blockSafe <= blockTotal);
    }
  }

  /**
   * Periodically check whether it is time to leave safe mode.
   * This thread starts when the threshold level is reached.
   */
  class SafeModeMonitor implements Runnable {
    /**
     * interval in msec for checking safe mode: {@value}
     */
    private static final long recheckInterval = 1000;

    /**
     */
    public void run() {
      while (fsRunning && (safeMode != null && !safeMode.canLeave())) {
        try {
          Thread.sleep(recheckInterval);
        } catch (InterruptedException ie) {
        }
      }
      // if we stopped namenode while still in safemode, then exit here
      if (!fsRunning) {
        LOG.info("Quitting SafeModeMonitor thread. ");
        return;
      }

      // leave safe mode and stop the monitor
      try {
        leaveSafeMode(true);
      } catch (SafeModeException es) { // should never happen
        String msg = "SafeModeMonitor may not run during distributed upgrade.";
        assert false : msg;
        throw new RuntimeException(msg, es);
      }
      smmthread = null;
    }
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
  synchronized boolean isInSafeMode() {
    if (safeMode == null) {
      return false;
    }
    return safeMode.isOn();
  }

  /**
   * Check whether replication queues are populated.
   */
  synchronized boolean isPopulatingReplQueues() {
    return (!isInSafeMode() ||
            safeMode.isPopulatingReplQueues());
  }

  /**
   * Check whether the name node is in startup mode.
   */
  synchronized boolean isInStartupSafeMode() {
    if (safeMode == null)
      return false;
    return safeMode.isOn() && !safeMode.isManual();
  }

  /**
   * Increment number of blocks that reached minimal replication.
   *
   * @param replication current replication
   */
  void incrementSafeBlockCount(int replication) {
    if (safeMode == null) {
      return;
    }
    safeMode.incrementSafeBlockCount((short) replication);
  }

  /**
   * Decrement number of blocks that reached minimal replication.
   */
  void decrementSafeBlockCount(Block b) {
    if (safeMode == null) // mostly true
    {
      return;
    }
    safeMode.decrementSafeBlockCount((short) countNodes(b).liveReplicas());
  }

  /**
   * Set the total number of blocks in the system.
   */
  void setBlockTotal() {
    if (safeMode == null) {
      return;
    }
    safeMode.setBlockTotal(blocksMap.size());
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
      if (!isInSafeMode()) {
        safeMode = new SafeModeInfo();
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

  long getEditLogSize() throws IOException {
    return getEditLog().getEditLogSize();
  }

  CheckpointSignature rollEditLog() throws IOException {
    writeLock();
    try {
      if (isInSafeMode()) {
        throw new SafeModeException("Checkpoint not created",
          safeMode);
      }
      LOG.info("Roll Edit Log from " + Server.getRemoteAddress() +
        " editlog file " + getFSImage().getEditLog().getFsEditName() +
        " editlog timestamp " + getFSImage().getEditLog().getFsEditTime());
      return getFSImage().rollEditLog();
    } finally {
      writeUnlock();
    }
  }

  /**
   * Moves fsimage.ckpt to fsImage and edits.new to edits
   * Reopens the new edits file.
   *
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

  private void checkSuperuserPrivilege() throws AccessControlException {
    if (isPermissionEnabled) {
      try {
        PermissionChecker.checkSuperuserPrivilege(fsOwner, supergroup);
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
      fsOwner.getUserName(), supergroup);
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

  public long getFilesTotal() {
    return this.dir.totalInodes();
  }

  public long getPendingReplicationBlocks() {
    return pendingReplicationBlocksCount;
  }

  public long getUnderReplicatedBlocks() {
    return underReplicatedBlocksCount;
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
  public FSNamesystemMetrics getFSNamesystemMetrics() {
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
  }


  /**
   * Number of live data nodes
   *
   * @return Number of live data nodes
   */
  public int getNumLiveDataNodes() {
    int numLive = 0;
    synchronized (datanodeMap) {
      for (Iterator<DatanodeDescriptor> it = datanodeMap.values().iterator();
           it.hasNext();) {
        DatanodeDescriptor dn = it.next();
        if (!isDatanodeDead(dn)) {
          numLive++;
        }
      }
    }
    return numLive;
  }


  /**
   * Number of dead data nodes
   *
   * @return Number of dead data nodes
   */
  public int getNumDeadDataNodes() {
    int numDead = 0;
    synchronized (datanodeMap) {
      for (Iterator<DatanodeDescriptor> it = datanodeMap.values().iterator();
           it.hasNext();) {
        DatanodeDescriptor dn = it.next();
        if (isDatanodeDead(dn)) {
          numDead++;
        }
      }
    }
    return numDead;
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
      BlockInfo storedBlock = blocksMap.getStoredBlock(block);
      if (storedBlock == null) {
        String msg = block + " is already commited, storedBlock == null.";
        LOG.info(msg);
        throw new IOException(msg);
      }
      INodeFile fileINode = storedBlock.getINode();
      if (!fileINode.isUnderConstruction()) {
        String msg = block + " is already commited, !fileINode.isUnderConstruction().";
        LOG.info(msg);
        throw new IOException(msg);
      }
      // Disallow client-initiated recovery once
      // NameNode initiated lease recovery starts
      if (!fromNN && HdfsConstants.NN_RECOVERY_LEASEHOLDER.equals(
          leaseManager.getLeaseByPath(fileINode.getFullPathName()).getHolder())) {
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
      overwrite = spath.getParent().toString() + Path.SEPARATOR;
      replaceBy = dst + Path.SEPARATOR;
    } else {
      overwrite = src;
      replaceBy = dst;
    }

    leaseManager.changeLease(src, dst, overwrite, replaceBy);
  }

  /**
   * Serializes leases.
   */
  void saveFilesUnderConstruction(DataOutputStream out) throws IOException {
    synchronized (leaseManager) {
      out.writeInt(leaseManager.countPath()); // write the size

      for (Lease lease : leaseManager.getSortedLeases()) {
        for (String path : lease.getPaths()) {
          // verify that path exists in namespace
          INode node = dir.getFileINode(path);
          if (node == null) {
            throw new IOException("saveLeases found path " + path +
              " but no matching entry in namespace.");
          }
          if (!node.isUnderConstruction()) {
            throw new IOException("saveLeases found path " + path +
              " but is not under construction.");
          }
          INodeFileUnderConstruction cons = (INodeFileUnderConstruction) node;
          FSImage.writeINodeUnderConstruction(out, cons, path);
        }
      }
    }
  }

  /*
   * @return list of datanodes where decommissioning is in progress 
   */
  public ArrayList<DatanodeDescriptor> getDecommissioningNodes() {
    readLock();
    try {
      ArrayList<DatanodeDescriptor> decommissioningNodes = new ArrayList<DatanodeDescriptor>();
      ArrayList<DatanodeDescriptor> results = getDatanodeListForReport(DatanodeReportType.LIVE);
      for (Iterator<DatanodeDescriptor> it = results.iterator(); it.hasNext();) {
        DatanodeDescriptor node = it.next();
        if (node.isDecommissionInProgress()) {
          decommissioningNodes.add(node);
        }
      }
      return decommissioningNodes;
    } finally {
      readUnlock();
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

        checkSuperuserPrivilege();
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
            String src = FSDirectory.getFullPathName(inode);
            if ((src.startsWith(path))
                && (!decommissioningOnly || countLiveNodes(blk) == 0)) {
              corruptFiles.add(new CorruptFileBlockInfo(src, blk));
              count++;
              if (count >= maxCorruptFilesReturned)
                break;
            }
          }
        }
        cookieTab[0] = String.valueOf(skip);
        LOG.info("list corrupt file blocks returned: " + count);
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
    return Arrays.asList("dfs.block.replicator.classname",
                         "dfs.persist.blocks",
                         "dfs.permissions.audit.log",
                         "dfs.heartbeat.interval",
                         "heartbeat.recheck.interval",
                         "dfs.access.time.precision");
  }

  private static class ReplicationWork {

    private Block block;
    private INodeFile fileINode;

    private int numOfReplicas;
    private DatanodeDescriptor srcNode;
    private List<DatanodeDescriptor> containingNodes;

    private DatanodeDescriptor targets[];
    private int priority;

    public ReplicationWork(Block block,
                            INodeFile fileINode,
                            int numOfReplicas,
                            DatanodeDescriptor srcNode,
                            List<DatanodeDescriptor> containingNodes,
                            int priority){
      this.block = block;
      this.fileINode = fileINode;
      this.numOfReplicas = numOfReplicas;
      this.srcNode = srcNode;
      this.containingNodes = containingNodes;
      this.priority = priority;

      this.targets = null;
    }
  }
}
