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

package org.apache.hadoop.raid;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.net.InetSocketAddress;
import java.net.URISyntaxException;
import java.text.SimpleDateFormat;
import java.text.DateFormat;
import java.util.Date;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.management.NotCompliantMBeanException;
import javax.management.ObjectName;
import javax.management.StandardMBean;
import javax.xml.parsers.ParserConfigurationException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.util.InjectionEvent;
import org.apache.hadoop.util.InjectionHandler;
import org.apache.hadoop.http.HttpServer;
import org.apache.hadoop.ipc.ProtocolSignature;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.raid.Decoder.DecoderInputStream;
import org.apache.hadoop.raid.DistBlockIntegrityMonitor.CorruptFile;
import org.apache.hadoop.raid.DistBlockIntegrityMonitor.CorruptFileStatus;
import org.apache.hadoop.raid.DistBlockIntegrityMonitor.Worker;
import org.apache.hadoop.raid.DistBlockIntegrityMonitor.CorruptFileCounter;
import org.apache.hadoop.raid.DistBlockIntegrityMonitor.CorruptionWorker;
import org.apache.hadoop.raid.DistRaid.EncodingCandidate;
import org.apache.hadoop.raid.LogUtils.LOGRESULTS;
import org.apache.hadoop.raid.LogUtils.LOGTYPES;
import org.apache.hadoop.raid.StripeStore.StripeInfo;
import org.apache.hadoop.raid.protocol.PolicyInfo;
import org.apache.hadoop.raid.protocol.RaidProtocol;
import org.apache.hadoop.raid.StripeStore;
import org.apache.hadoop.tools.HadoopArchives;
import org.apache.hadoop.util.Daemon;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.util.VersionInfo;
import org.json.JSONException;
import org.xml.sax.SAXException;
import org.apache.hadoop.metrics.util.MBeanUtil;

/**
 * A base class that implements {@link RaidProtocol}.
 *
 * use raid.classname to specify which implementation to use
 */
public abstract class RaidNode implements RaidProtocol, RaidNodeStatusMBean {

  static{
    Configuration.addDefaultResource("hdfs-default.xml");
    Configuration.addDefaultResource("mapred-default.xml");
    Configuration.addDefaultResource("raid-default.xml");
    Configuration.addDefaultResource("hdfs-site.xml");
    Configuration.addDefaultResource("mapred-site.xml");
    Configuration.addDefaultResource("raid-site.xml");
  }
  
  // The modification time of the raid candidate should be 
  // at least (1 day) older.
  public static final long RAID_MOD_TIME_PERIOD_DEFAULT = 24 * 3600 * 1000;
  public static final String RAID_MOD_TIME_PERIOD_KEY = "raid.mod.time.period";

  public static final Log LOG = LogFactory.getLog(RaidNode.class);
  public static final long SLEEP_TIME = 10000L; // 10 seconds
  public static final String TRIGGER_MONITOR_SLEEP_TIME_KEY = 
      "hdfs.raid.trigger.monitor.sleep.time";
  public static final String UNDER_REDUNDANT_FILES_PROCESSOR_SLEEP_TIME_KEY=
      "hdfs.raid.under.redundant.files.processor.sleep.time";
  public static final int DEFAULT_PORT = 60000;
  // we don't raid too small files
  public static final long MINIMUM_RAIDABLE_FILESIZE = 10*1024L;
  public static final String MINIMUM_RAIDABLE_FILESIZE_KEY = 
      "hdfs.raid.min.filesize";

  public static final String RAID_RECOVERY_LOCATION_KEY =
      "hdfs.raid.local.recovery.location";
  public static final String DEFAULT_RECOVERY_LOCATION = "/tmp/raidrecovery";

  public static final String RAID_PARITY_HAR_THRESHOLD_DAYS_KEY =
      "raid.parity.har.threshold.days";
  public static final int DEFAULT_RAID_PARITY_HAR_THRESHOLD_DAYS = 3;

  public static final String RAID_DIRECTORYTRAVERSAL_SHUFFLE =
      "raid.directorytraversal.shuffle";
  public static final String RAID_DIRECTORYTRAVERSAL_THREADS =
      "raid.directorytraversal.threads";

  public static final String RAID_UNDER_REDUNDANT_FILES = 
      "raid.under.redundant.files";
  public static final String RAID_MAPREDUCE_UPLOAD_CLASSES = 
      "raid.mapreduce.upload.classes";
  public static final String RAID_DISABLE_CORRUPT_BLOCK_FIXER_KEY = 
      "raid.blockreconstruction.corrupt.disable";
  public static final String RAID_DISABLE_DECOMMISSIONING_BLOCK_COPIER_KEY = 
      "raid.blockreconstruction.decommissioning.disable";
  public static final String RAID_DISABLE_CORRUPTFILE_COUNTER_KEY = 
      "raid.corruptfile.counter.disable";
  public static final String RAID_CHECKSUM_STORE_CLASS_KEY =
      "hdfs.raid.checksum.store.class";
  // by default we don't require to use checksum store, when it's set, we will throw an exception
  // if checksum store is null
  public static final String RAID_CHECKSUM_STORE_REQUIRED_KEY =
      "hdfs.raid.checksum.store.required";
  public static final String RAID_CHECKSUM_VERIFICATION_REQUIRED_KEY =
      "hdfs.raid.checksum.verification.required";
  public static final String RAID_STRIPE_STORE_CLASS_KEY =
      "hdfs.raid.stripe.store.class";
  public static final String RAID_ENCODING_STRIPES_KEY = 
      "hdfs.raid.stripe.encoding";
  public static final int DEFAULT_RAID_ENCODING_STRIPES = 1;
  public static final String RAID_PARITY_INITIAL_REPL_KEY = 
      "hdfs.raid.parity.initial.repl";
  public static final int DEFAULT_RAID_PARITY_INITIAL_REPL = 3;
  public static final String JOBUSER = "raid";
  
  public static final String HAR_SUFFIX = "_raid.har";
  public static final Pattern PARITY_HAR_PARTFILE_PATTERN =

      Pattern.compile(".*" + HAR_SUFFIX + "/part-.*");

  public static final String RAIDNODE_CLASSNAME_KEY = "raid.classname";  
  public static final DateFormat df = new SimpleDateFormat("yyyy-MM-dd-HH-mm-ss-SSS");
  public static Random rand = new Random();

  /** RPC server */
  private Server server;
  /** RPC server address */
  private InetSocketAddress serverAddress = null;
  /** only used for testing purposes  */
  protected boolean stopRequested = false;

  /** Configuration Manager */
  private ConfigManager configMgr;

  private HttpServer infoServer;
  private String infoBindAddress;
  private long startTime;

  /** hadoop configuration */
  protected Configuration conf;

  protected boolean initialized = false;  // Are we initialized?
  protected volatile boolean running = true; // Are we running?
  
  private static long modTimePeriod = RAID_MOD_TIME_PERIOD_DEFAULT;

  /** Deamon thread to trigger policies */
  Daemon triggerThread = null;
  Daemon urfThread = null;
  public static long triggerMonitorSleepTime = SLEEP_TIME;
  public static long underRedundantFilesProcessorSleepTime = SLEEP_TIME;

  /** Deamon thread to delete obsolete parity files */
  PurgeMonitor purgeMonitor = null;
  Daemon purgeThread = null;

  /** Deamon thread to har raid directories */
  Daemon harThread = null;

  /** Daemon thread to fix corrupt files */
  BlockIntegrityMonitor blockIntegrityMonitor = null;
  Daemon blockFixerThread = null;
  Daemon blockCopierThread = null;
  Daemon corruptFileCounterThread = null;

  /** Daemon thread to collecting statistics */
  StatisticsCollector statsCollector = null;
  Daemon statsCollectorThread = null;

  PlacementMonitor placementMonitor = null;
  Daemon placementMonitorThread = null;
  
  TriggerMonitor triggerMonitor = null;
  UnderRedundantFilesProcessor urfProcessor = null;

  private int directoryTraversalThreads;
  private boolean directoryTraversalShuffle;
  private ObjectName beanName;
  private ObjectName raidnodeMXBeanName;

  // statistics about RAW hdfs blocks. This counts all replicas of a block.
  public static class Statistics {
    long numProcessedBlocks; // total blocks encountered in namespace
    long processedSize;   // disk space occupied by all blocks
    long remainingSize;      // total disk space post RAID

    long numMetaBlocks;      // total blocks in metafile
    long metaSize;           // total disk space for meta files

    public void clear() {
      numProcessedBlocks = 0;
      processedSize = 0;
      remainingSize = 0;
      numMetaBlocks = 0;
      metaSize = 0;
    }
    public String toString() {
      long save = processedSize - (remainingSize + metaSize);
      long savep = 0;
      if (processedSize > 0) {
        savep = (save * 100)/processedSize;
      }
      String msg = " numProcessedBlocks = " + numProcessedBlocks +
          " processedSize = " + processedSize +
          " postRaidSize = " + remainingSize +
          " numMetaBlocks = " + numMetaBlocks +
          " metaSize = " + metaSize +
          " %save in raw disk space = " + savep;
      return msg;
    }
  }
  
  // Startup options
  static public enum StartupOption{
    TEST ("-test"),
    REGULAR ("-regular");

    private String name = null;
    private StartupOption(String arg) {this.name = arg;}
    public String getName() {return name;}
  }

  // For unit test
  RaidNode() {}

  /**
   * Start RaidNode.
   * <p>
   * The raid-node can be started with one of the following startup options:
   * <ul> 
   * <li>{@link StartupOption#REGULAR REGULAR} - normal raid node startup</li>
   * </ul>
   * The option is passed via configuration field: 
   * <tt>fs.raidnode.startup</tt>
   * 
   * The conf will be modified to reflect the actual ports on which 
   * the RaidNode is up and running if the user passes the port as
   * <code>zero</code> in the conf.
   * 
   * @param conf  confirguration
   * @throws IOException
   */
  RaidNode(Configuration conf) throws IOException {
    try {
      initialize(conf);
    } catch (IOException e) {
      LOG.error(StringUtils.stringifyException(e));
      this.stop();
      throw e;
    } catch (Exception e) {
      this.stop();
      throw new IOException(e);
    }
  }

  public long getProtocolVersion(String protocol,
      long clientVersion) throws IOException {
    if (protocol.equals(RaidProtocol.class.getName())) {
      return RaidProtocol.versionID;
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

  /**
   * Wait for service to finish.
   * (Normally, it runs forever.)
   */
  public void join() {
    try {
      if (server != null) server.join();
      if (triggerThread != null) triggerThread.join();
      if (urfThread != null) urfThread.join();
      if (blockFixerThread != null) blockFixerThread.join();
      if (blockCopierThread != null) blockCopierThread.join();
      if (corruptFileCounterThread != null) corruptFileCounterThread.join();
      if (purgeThread != null) purgeThread.join();
      if (statsCollectorThread != null) statsCollectorThread.join();
    } catch (InterruptedException ie) {
      // do nothing
    }
  }

  /**
   * Stop all RaidNode threads and wait for all to finish.
   */
  public void stop() {
    if (stopRequested) {
      return;
    }
    stopRequested = true;
    running = false;
    if (server != null) server.stop();
    if (triggerThread != null) {
      triggerThread.interrupt();
      triggerMonitor = null;
    }
    if (urfThread != null) {
      urfThread.interrupt();
      urfProcessor = null;
    }
    if (blockIntegrityMonitor != null) blockIntegrityMonitor.running = false;
    if (blockFixerThread != null) blockFixerThread.interrupt();
    if (blockCopierThread != null) blockCopierThread.interrupt();
    if (corruptFileCounterThread != null) corruptFileCounterThread.interrupt();
    if (purgeMonitor != null) purgeMonitor.running = false;
    if (purgeThread != null) purgeThread.interrupt();
    if (placementMonitor != null) placementMonitor.stop();
    if (statsCollector != null) statsCollector.stop();
    if (statsCollectorThread != null) statsCollectorThread.interrupt();
    if (infoServer != null) {
      try {
        infoServer.stop();
      } catch (Exception e) {
        LOG.warn("Exception shutting down " + RaidNode.class, e);
      }
    }
    this.unregisterMBean();
  }

  private static InetSocketAddress getAddress(String address) {
    return NetUtils.createSocketAddr(address);
  }

  public static InetSocketAddress getAddress(Configuration conf) {
    String nodeport = conf.get("raid.server.address");
    if (nodeport == null) {
      nodeport = "localhost:" + DEFAULT_PORT;
    }
    return getAddress(nodeport);
  }

  public InetSocketAddress getListenerAddress() {
    return server.getListenerAddress();
  }

  private void cleanUpDirectory(String dir, Configuration conf) 
      throws IOException {
    Path pdir = new Path(dir);
    FileSystem fs = pdir.getFileSystem(conf);
    if (fs.exists(pdir)) {
      fs.delete(pdir);
    }
  }

  private void cleanUpTempDirectory(Configuration conf) throws IOException {
    for (Codec codec: Codec.getCodecs()) {
      cleanUpDirectory(codec.tmpParityDirectory, conf);
      cleanUpDirectory(codec.tmpHarDirectory, conf);
    }
  }
  
  private void addTmpJars(Configuration conf) throws URISyntaxException {
    StringBuilder jarLocations = new StringBuilder();
    String[] uploadClassNames = conf.getStrings(RAID_MAPREDUCE_UPLOAD_CLASSES);
    if (uploadClassNames == null || uploadClassNames.length == 0) {
      LOG.warn("Key " + RAID_MAPREDUCE_UPLOAD_CLASSES + " is not defined");
      return;
    }
    boolean first = true;
    for (String uploadClassName: uploadClassNames) {
      try {
        String jarLocation = Class.forName(uploadClassName).getProtectionDomain().
            getCodeSource().getLocation().toURI().toString();
        if (!first) {
          jarLocations.append(",");
        }
        jarLocations.append(jarLocation);
        first = false;
      } catch (ClassNotFoundException cnfe) {
        LOG.warn("Class " + uploadClassName + " is not found", cnfe);
      }
    }
    LOG.info("Load jars " + jarLocations.toString());
    conf.set("tmpjars", jarLocations.toString());
  }

  private void initialize(Configuration conf) 
      throws IOException, SAXException, InterruptedException, RaidConfigurationException,
      ClassNotFoundException, ParserConfigurationException, URISyntaxException, JSONException {
    this.startTime = RaidNode.now();
    this.conf = conf;
    modTimePeriod = conf.getLong(RAID_MOD_TIME_PERIOD_KEY, 
        RAID_MOD_TIME_PERIOD_DEFAULT);
    LOG.info("modTimePeriod: " + modTimePeriod);
    InetSocketAddress socAddr = RaidNode.getAddress(conf);
    int handlerCount = conf.getInt("fs.raidnode.handler.count", 10);
    addTmpJars(this.conf);
    // clean up temporay directory
    cleanUpTempDirectory(conf);

    // read in the configuration
    configMgr = new ConfigManager(conf);

    // create rpc server
    this.server = RPC.getServer(this, socAddr.getAddress().getHostAddress(), socAddr.getPort(),
        handlerCount, false, conf);

    // create checksum store if not exist  
    RaidNode.createChecksumStore(conf, true);

    // create stripe store if not exist
    RaidNode.createStripeStore(conf, true, FileSystem.get(conf));

    // The rpc-server port can be ephemeral... ensure we have the correct info
    this.serverAddress = this.server.getListenerAddress();
    LOG.info("RaidNode up at: " + this.serverAddress);
    // Instantiate the metrics singleton.
    RaidNodeMetrics.getInstance(RaidNodeMetrics.DEFAULT_NAMESPACE_ID);

    this.server.start(); // start RPC server

    // Create a block integrity monitor and start its thread(s)
    this.blockIntegrityMonitor = BlockIntegrityMonitor.createBlockIntegrityMonitor(conf);

    boolean useBlockFixer = 
        !conf.getBoolean(RAID_DISABLE_CORRUPT_BLOCK_FIXER_KEY, false);
    boolean useBlockCopier = 
        !conf.getBoolean(RAID_DISABLE_DECOMMISSIONING_BLOCK_COPIER_KEY, true);
    boolean useCorruptFileCounter = 
        !conf.getBoolean(RAID_DISABLE_CORRUPTFILE_COUNTER_KEY, false);

    Runnable fixer = blockIntegrityMonitor.getCorruptionMonitor();
    if (useBlockFixer && (fixer != null)) {
      this.blockFixerThread = new Daemon(fixer);
      this.blockFixerThread.setName("Block Fixer");
      this.blockFixerThread.start();
    }

    Runnable copier = blockIntegrityMonitor.getDecommissioningMonitor();
    if (useBlockCopier && (copier != null)) {
      this.blockCopierThread = new Daemon(copier);
      this.blockCopierThread.setName("Block Copier");
      this.blockCopierThread.start();
    }

    Runnable counter = blockIntegrityMonitor.getCorruptFileCounter();
    if (useCorruptFileCounter && counter != null) {
      this.corruptFileCounterThread = new Daemon(counter);
      this.corruptFileCounterThread.setName("Corrupt File Counter");
      this.corruptFileCounterThread.start();
    }

    // start the deamon thread to fire polcies appropriately
    RaidNode.triggerMonitorSleepTime = conf.getLong(
        TRIGGER_MONITOR_SLEEP_TIME_KEY, 
        SLEEP_TIME);
    RaidNode.underRedundantFilesProcessorSleepTime = conf.getLong(
        UNDER_REDUNDANT_FILES_PROCESSOR_SLEEP_TIME_KEY,
        SLEEP_TIME);
    this.triggerMonitor = new TriggerMonitor();
    this.triggerThread = new Daemon(this.triggerMonitor);
    this.triggerThread.setName("Trigger Thread");
    this.triggerThread.start();
    
    this.urfProcessor = new UnderRedundantFilesProcessor(conf);
    this.urfThread = new Daemon(this.urfProcessor);
    this.urfThread.setName("UnderRedundantFilesProcessor Thread");
    this.urfThread.start();

    // start the thread that monitor and moves blocks
    this.placementMonitor = new PlacementMonitor(conf);
    this.placementMonitor.start();

    // start the thread that deletes obsolete parity files
    this.purgeMonitor = new PurgeMonitor(conf, placementMonitor, this);
    this.purgeThread = new Daemon(purgeMonitor);
    this.purgeThread.setName("Purge Thread");
    this.purgeThread.start();

    // start the thread that creates HAR files
    this.harThread = new Daemon(new HarMonitor());
    this.harThread.setName("HAR Thread");
    this.harThread.start();

    // start the thread that collects statistics
    this.statsCollector = new StatisticsCollector(this, configMgr, conf);
    this.statsCollectorThread = new Daemon(statsCollector);
    this.statsCollectorThread.setName("Stats Collector");
    this.statsCollectorThread.start();

    this.directoryTraversalShuffle =
        conf.getBoolean(RAID_DIRECTORYTRAVERSAL_SHUFFLE, true);
    this.directoryTraversalThreads =
        conf.getInt(RAID_DIRECTORYTRAVERSAL_THREADS, 4);

    startHttpServer();
    this.registerMBean();

    initialized = true;
  }
  
  public void registerMBean() {
    StandardMBean bean;
    try {
      beanName = VersionInfo.registerJMX("RaidNode");
      bean = new StandardMBean(this, RaidNodeStatusMBean.class);
      raidnodeMXBeanName = MBeanUtil.registerMBean("RaidNode", "RaidNodeState", bean);
    } catch (NotCompliantMBeanException e) {
      e.printStackTrace();
    }
    LOG.info("Registered RaidNodeStatusMBean");
  }
  
  public void unregisterMBean() {
    if (this.raidnodeMXBeanName != null) {
      MBeanUtil.unregisterMBean(raidnodeMXBeanName);
    }
    if (this.beanName != null) {
      MBeanUtil.unregisterMBean(beanName);
    }
    LOG.info("Unregistered RaidNodeStatusMBean");
  }

  private void startHttpServer() throws IOException {
    String infoAddr = conf.get("mapred.raid.http.address", "localhost:50091");
    InetSocketAddress infoSocAddr = NetUtils.createSocketAddr(infoAddr);
    this.infoBindAddress = infoSocAddr.getAddress().getHostAddress();
    int tmpInfoPort = infoSocAddr.getPort();
    this.infoServer = new HttpServer("raid", this.infoBindAddress, tmpInfoPort,
        tmpInfoPort == 0, conf);
    this.infoServer.setAttribute("raidnode", this);
    this.infoServer.addInternalServlet("corruptfilecounter", "/corruptfilecounter", 
        CorruptFileCounterServlet.class);
    this.infoServer.start();
    LOG.info("Web server started at port " + this.infoServer.getPort());
  }

  public StatisticsCollector getStatsCollector() {
    return this.statsCollector;
  }

  public HttpServer getInfoServer() {
    return infoServer;
  }

  public PlacementMonitor getPlacementMonitor() {
    return this.placementMonitor;
  }
  
  public TriggerMonitor getTriggerMonitor() {
    return this.triggerMonitor;
  }

  public UnderRedundantFilesProcessor getURFProcessor() {
    return this.urfProcessor;
  }
  
  public PurgeMonitor getPurgeMonitor() {
    return this.purgeMonitor;
  }
  
  public BlockIntegrityMonitor getBlockIntegrityMonitor() {
    return blockIntegrityMonitor;
  }

  public BlockIntegrityMonitor.Status getBlockIntegrityMonitorStatus() {
    return blockIntegrityMonitor.getAggregateStatus();
  }

  public BlockIntegrityMonitor.Status getBlockFixerStatus() {
    return ((Worker)blockIntegrityMonitor.getCorruptionMonitor()).getStatus();
  }

  public BlockIntegrityMonitor.Status getBlockCopierStatus() {
    return ((Worker)blockIntegrityMonitor.getDecommissioningMonitor()).getStatus();
  }
  
  public double getNumDetectionsPerSec() {
    return ((CorruptionWorker)blockIntegrityMonitor.getCorruptionMonitor()).
        getNumDetectionsPerSec();
  }
  
  public ArrayList<CorruptFile> getCorruptFileList(String monitorDir, 
      CorruptFileStatus cfs) {
    return ((CorruptionWorker)blockIntegrityMonitor.getCorruptionMonitor()).
        getCorruptFileList(monitorDir, cfs);
  }

  // Return the counter map where key is the check directories, 
  // the value is counters of different types of corrupt files 
  public Map<String, Map<CorruptFileStatus, Long>> getCorruptFilesCounterMap() {
    return ((CorruptionWorker)blockIntegrityMonitor.getCorruptionMonitor()).
        getCorruptFilesCounterMap();
  }
  
  public long getNumFilesWithMissingBlks() {
    return ((CorruptFileCounter)blockIntegrityMonitor.
        getCorruptFileCounter()).getFilesWithMissingBlksCnt();
  }
  
  public long[] getNumStrpWithMissingBlksRS(){
    return ((CorruptFileCounter)blockIntegrityMonitor.
        getCorruptFileCounter()).getNumStrpWithMissingBlksRS();
  }
  
  public CorruptFileCounter getCorruptFileCounter() {
    return (CorruptFileCounter) blockIntegrityMonitor.getCorruptFileCounter();
  }

  public String getHostName() {
    return this.infoBindAddress;
  }

  public long getStartTime() {
    return this.startTime;
  }

  public Thread.State getStatsCollectorState() {
    return this.statsCollectorThread.getState();
  }

  public Configuration getConf() {
    return this.conf;
  }
  
  /**
   * Determine a PolicyInfo from the codec, to re-generate the parity files
   * of modified source files.
   * 
   * @param codec
   * @return
   */
  public PolicyInfo determinePolicy(Codec codec) {
    for (PolicyInfo info : configMgr.getAllPolicies()) {
      if (!info.getShouldRaid()) {
        continue;
      }
      
      if (info.getCodecId().equals(codec.id)) {
        return info;
      }
    }
    
    return null;
  }

  /**
   * Implement RaidProtocol methods
   */

  /** {@inheritDoc} */
  public PolicyInfo[] getAllPolicies() throws IOException {
    Collection<PolicyInfo> list = configMgr.getAllPolicies();
    return list.toArray(new PolicyInfo[list.size()]);
  }

  /** {@inheritDoc} */
  public String recoverFile(String inStr, long corruptOffset) throws IOException {
    throw new IOException("Not supported");
  }
  
  /** {@inheritDoc} */
  public void sendRecoveryTime(String path, long recoveryTime, String taskId)
      throws IOException {
    this.blockIntegrityMonitor.sendRecoveryTime(path, recoveryTime, taskId);
  }
  
  public boolean startSmokeTest() throws Exception {
    return startSmokeTest(true);
  }
  
  public boolean startSmokeTest(boolean wait) throws Exception {
    Runnable worker = this.blockIntegrityMonitor.getCorruptionMonitor();
    if (worker == null || !(worker instanceof CorruptionWorker)) {
      throw new IOException("CorruptionWorker is not found");
    }
    if (!(this instanceof DistRaidNode)) {
      throw new IOException("Current Raid daemon is not DistRaidNode");
    }
    if (!(this.blockIntegrityMonitor instanceof DistBlockIntegrityMonitor)) {
      throw new IOException("Current BlockFix daemon is not DistBlockIntegrityMonitor");
    }
    SmokeTestThread.LOG.info("[SMOKETEST] Start Raid Smoke Test");
    long startTime = System.currentTimeMillis();
    SmokeTestThread str = new SmokeTestThread(this);
    ExecutorService executor = Executors.newSingleThreadExecutor();
    Future<Boolean> future = executor.submit(str);
    boolean result = false;
    if (wait) {
      try {
        result = future.get(1200, TimeUnit.SECONDS);
      } catch (Throwable exp) {
        SmokeTestThread.LOG.info("[SMOKETEST] Get Exception ", exp);
      } finally {
        executor.shutdownNow();
        SmokeTestThread.LOG.info("[SMOKETEST] Finish Raid Smoke Test (" + 
            (result? "succeed": "fail") + ") using " + 
            (System.currentTimeMillis() - startTime) + "ms");
        if (str.ioe != null) {
          throw str.ioe;
        }
      }
    } 
    return result;
  }
  
  /**
   * returns the number of raid jobs running for a particular policy
   */
  abstract int getRunningJobsForPolicy(String policyName);
  
  class IncreaseReplicationRunnable implements Runnable {
    List<String> files = null;
    FileSystem fs = null;
    AtomicLong failFilesCount = null;
    AtomicLong succeedFilesCount = null;
    IncreaseReplicationRunnable(List<String> newFiles, FileSystem newFs,
        AtomicLong newFailFilesCount, AtomicLong newSucceedFilesCount) {
      files = newFiles;
      fs = newFs;
      failFilesCount = newFailFilesCount;
      succeedFilesCount = newSucceedFilesCount;
    }
    public void run() {
      short repl = 3;
      int failCount = 0;
      int succeedCount = 0;
      try {
        for (String file: files) {
          FileStatus stat = null;
          Path p = new Path(file);
          try {
            stat = fs.getFileStatus(p);
          } catch (FileNotFoundException fnfe) {
            // File doesn't exist, skip it
            continue;
          }
          if (stat.isDir() || stat.getReplication() >= repl) {
            // It's a directory or it already has enough replication, skip it
            continue;
          }
          if (!fs.setReplication(new Path(file), repl)) {
            failCount++;
            LOG.warn("Fail to increase replication for " + file);
          } else {
            succeedCount++;
          }
        }
      } catch (Throwable th) {
        LOG.error("Fail to increase replication", th);
      } finally {
        failFilesCount.addAndGet(failCount);
        succeedFilesCount.addAndGet(succeedCount);
      }
    }
  }
  
  public class UnderRedundantFilesProcessor implements Runnable {
    public static final String UNDER_REDUNDANT_FILES_PROCESSOR_THREADS_NUM_KEY = 
        "raid.under.redundant.files.processor.threads.num";
    public static final String INCREASE_REPLICATION_BATCH_SIZE_KEY = 
        "raid.increase.replication.batch.size";
    public static final int DEFAULT_UNDER_REDUNDANT_FILES_PROCESSOR_THREADS_NUM = 5;
    public static final int DEFAULT_INCREASE_REPLICATION_BATCH_SIZE = 50;
    int numThreads = DEFAULT_UNDER_REDUNDANT_FILES_PROCESSOR_THREADS_NUM;
    int incReplBatch = DEFAULT_INCREASE_REPLICATION_BATCH_SIZE;
    long lastFileModificationTime = 0;
    AtomicLong failedFilesCount = new AtomicLong(0);
    AtomicLong succeedFilesCount = new AtomicLong(0);
    String[] monitorDirs = null;
    int[] counters = null;
    int others = 0;
    
    public UnderRedundantFilesProcessor(Configuration conf) {
      numThreads = conf.getInt(UNDER_REDUNDANT_FILES_PROCESSOR_THREADS_NUM_KEY,
          DEFAULT_UNDER_REDUNDANT_FILES_PROCESSOR_THREADS_NUM);
      incReplBatch = conf.getInt(INCREASE_REPLICATION_BATCH_SIZE_KEY,
          DEFAULT_INCREASE_REPLICATION_BATCH_SIZE);
    }
    
    public void processUnderRedundantFiles(ExecutorService executor, int counterLen)
        throws IOException {
      String underRedundantFile = conf.get(RAID_UNDER_REDUNDANT_FILES);
      if (underRedundantFile == null) {
        return;
      }
      Path fileListPath = new Path(underRedundantFile);
      BufferedReader fileListReader;
      final FileSystem fs = fileListPath.getFileSystem(conf);
      FileStatus stat = null;
      try {
        stat = fs.getFileStatus(fileListPath);
        if (stat.isDir() || stat.getModificationTime() == lastFileModificationTime) {
          // Skip directory and already-scan files
          return;
        }
      } catch (FileNotFoundException fnfe) {
        return;
      }
      try {
        InputStream in = fs.open(fileListPath);
        fileListReader = new BufferedReader(new InputStreamReader(in));
      } catch (IOException e) {
        LOG.warn("Could not create reader for " + fileListPath, e);
        return;
      }
      int[] newCounters = new int[counterLen];
      int newOthers = 0;
      String l = null;
      List<String> files = new ArrayList<String>();
      try {
        while ((l = fileListReader.readLine()) != null) {
          if (LOG.isDebugEnabled()) {
            LOG.debug("checking file " + l);
          }
          for (Codec codec: Codec.getCodecs()) {
            if (l.startsWith(codec.tmpParityDirectory)) {
              // Skip tmp parity files 
              continue;
            }
          }
          boolean match = false;
          for (int i = 0; i < newCounters.length; i++) {
            if (l.startsWith(monitorDirs[i])) {
              newCounters[i]++;
              match = true;
            }
          }
          if (!match) {
            newOthers++;
          }
          files.add(l);
          if (files.size() == incReplBatch) {
            Runnable work = new IncreaseReplicationRunnable(files, fs,
                failedFilesCount, succeedFilesCount);
            executor.submit(work);
            files = new ArrayList<String>();
          }
        }
        if (files.size() > 0) {
          Runnable work = new IncreaseReplicationRunnable(files, fs,
              failedFilesCount, succeedFilesCount);
          executor.submit(work);
        }
        counters = newCounters;
        others = newOthers;
        lastFileModificationTime = stat.getModificationTime();
      } catch (IOException e) {
        LOG.error("Encountered error in processUnderRedundantFiles", e);
      }
    }
    
    public void run() {
      RaidNodeMetrics rnm = RaidNodeMetrics.getInstance(
          RaidNodeMetrics.DEFAULT_NAMESPACE_ID);
      rnm.initUnderRedundantFilesMetrics(conf);
      ExecutorService executor = null;
      ThreadFactory factory = new ThreadFactory() {
        final AtomicInteger tnum = new AtomicInteger();
        public Thread newThread(Runnable r) {
          Thread t = new Thread(r);
          t.setName("IncReplication-" + tnum.incrementAndGet());
          return t;
        }
      };
      executor = Executors.newFixedThreadPool(numThreads, factory);
      monitorDirs = BlockIntegrityMonitor.getCorruptMonitorDirs(conf);
      while (running) {
        LOG.info("Start process UnderRedundantFiles");
        try {
          processUnderRedundantFiles(executor, monitorDirs.length);
          LOG.info("Update UnderRedundantFiles Metrics");
          if (counters != null) {
            for (int i = 0; i < counters.length; i++) {
              rnm.underRedundantFiles.get(monitorDirs[i]).set(counters[i]);
            }
            rnm.underRedundantFiles.get(BlockIntegrityMonitor.OTHERS).set(others);
          }
        } catch (Throwable e) {
          LOG.error(e);
        }
        try {
          Thread.sleep(RaidNode.underRedundantFilesProcessorSleepTime);
        } catch (InterruptedException ie) {
          break;
        }
      }
      executor.shutdown();
    }    
  }

  /**
   * Periodically checks to see which policies should be fired.
   */
  class TriggerMonitor implements Runnable {

    class PolicyState {
      long startTime = 0;
      // A policy may specify either a path for directory traversal
      // or a file with the list of files to raid.
      DirectoryTraversal pendingTraversal = null;
      BufferedReader fileListReader = null;

      PolicyState() {}

      boolean isFileListReadInProgress() {
        return fileListReader != null;
      }
      void resetFileListRead() throws IOException {
        if (fileListReader != null) {
          fileListReader.close();
          fileListReader = null;
        }
      }

      boolean isScanInProgress() {
        return pendingTraversal != null;
      }
      void resetTraversal() {
        pendingTraversal = null;
      }
      void setTraversal(DirectoryTraversal pendingTraversal) {
        this.pendingTraversal = pendingTraversal;
      }
    }

    private Map<String, PolicyState> policyStateMap =
        new HashMap<String, PolicyState>();
    private volatile long lastTriggerTime = 0;
    
    public long getLastTriggerTime() {
      return lastTriggerTime;
    }
    
    // only used for testing
    public void putPolicyInfo(PolicyInfo info) {
      if (!policyStateMap.containsKey(info.getName())) {
        policyStateMap.put(info.getName(), new PolicyState());
      }
    }

    public void run() {
      while (running) {
        try {
          doProcess();
        } catch (Exception e) {
          LOG.error(StringUtils.stringifyException(e));
        } finally {
          LOG.info("Trigger thread continuing to run...");
        }
      }
    }

    private boolean shouldReadFileList(PolicyInfo info) {
      if (info.getFileListPath() == null || !info.getShouldRaid()) {
        return false;
      }
      String policyName = info.getName();
      PolicyState scanState = policyStateMap.get(policyName);
      if (scanState.isFileListReadInProgress()) {
        int maxJobsPerPolicy = configMgr.getMaxJobsPerPolicy();
        int runningJobsCount = getRunningJobsForPolicy(policyName);

        // If there is a scan in progress for this policy, we can have
        // upto maxJobsPerPolicy running jobs.
        return (runningJobsCount < maxJobsPerPolicy);
      } else {
        long lastReadStart = scanState.startTime;
        return (now() > lastReadStart + configMgr.getPeriodicity());
      }
    }

    /**
     * Should we select more files for a policy.
     */
    private boolean shouldSelectFiles(PolicyInfo info) {
      if (!info.getShouldRaid()) {
        return false;
      }
      String policyName = info.getName();
      int runningJobsCount = getRunningJobsForPolicy(policyName);
      PolicyState scanState = policyStateMap.get(policyName);
      if (scanState.isScanInProgress()) {
        int maxJobsPerPolicy = configMgr.getMaxJobsPerPolicy();

        // If there is a scan in progress for this policy, we can have
        // upto maxJobsPerPolicy running jobs.
        return (runningJobsCount < maxJobsPerPolicy);
      } else {

        // Check the time of the last full traversal before starting a fresh
        // traversal.
        long lastScan = scanState.startTime;
        return (now() > lastScan + configMgr.getPeriodicity());
      }
    }

    /**
     * Returns a list of pathnames that needs raiding.
     * The list of paths could be obtained by resuming a previously suspended
     * traversal.
     * The number of paths returned is limited by raid.distraid.max.jobs.
     */
    private List<FileStatus> selectFiles(
        PolicyInfo info, ArrayList<PolicyInfo> allPolicies) throws IOException {
      String policyName = info.getName();

      // Max number of files returned.
      int selectLimit = configMgr.getMaxFilesPerJob();

      PolicyState scanState = policyStateMap.get(policyName);

      List<FileStatus> returnSet = new ArrayList<FileStatus>(selectLimit);
      DirectoryTraversal traversal;
      if (scanState.isScanInProgress()) {
        LOG.info("Resuming traversal for policy " + policyName);
        traversal = scanState.pendingTraversal;
      } else {
        LOG.info("Start new traversal for policy " + policyName);
        scanState.startTime = now();
        if (!Codec.getCodec(info.getCodecId()).isDirRaid) {
          traversal = DirectoryTraversal.raidFileRetriever(
              info, info.getSrcPathExpanded(), allPolicies, conf,
              directoryTraversalThreads, directoryTraversalShuffle,
              true);
        } else {
          traversal = DirectoryTraversal.raidLeafDirectoryRetriever(
              info, info.getSrcPathExpanded(), allPolicies, conf,
              directoryTraversalThreads, directoryTraversalShuffle,
              true);
        }
        scanState.setTraversal(traversal);
      }

      FileStatus f;
      while ((f = traversal.next()) != DirectoryTraversal.FINISH_TOKEN) {
        returnSet.add(f);
        if (returnSet.size() == selectLimit) {
          return returnSet;
        }
      }
      scanState.resetTraversal();
      return returnSet;
    }

    public List<FileStatus> readFileList(PolicyInfo info) throws IOException {
      Path fileListPath = info.getFileListPath();
      List<FileStatus> list = new ArrayList<FileStatus>();
      long dirRaidNumBlocks = 0L;
      if (fileListPath == null) {
        return list;
      }

      int targetReplication = Integer.parseInt(info.getProperty("targetReplication"));

      String policyName = info.getName();
      PolicyState scanState = policyStateMap.get(policyName);
      if (!scanState.isFileListReadInProgress()) {
        scanState.startTime = now();
        try {
          InputStream in = fileListPath.getFileSystem(conf).open(fileListPath);
          scanState.fileListReader = new BufferedReader(new InputStreamReader(in));
        } catch (IOException e) {
          LOG.warn("Could not create reader for " + fileListPath, e);
          return list;
        }
      }

      Codec codec = Codec.getCodec(info.getCodecId());
      // Max number of blocks/files returned.
      int selectLimit = codec.isDirRaid? configMgr.getMaxBlocksPerDirRaidJob():
        configMgr.getMaxFilesPerJob();
      String l = null;
      try {
        while ((l = scanState.fileListReader.readLine()) != null) {
          if (LOG.isDebugEnabled()) {
            LOG.debug("Select files to raid, check: " + l);
          }
          Path p = new Path(l);
          FileSystem fs = p.getFileSystem(conf);
          p = fs.makeQualified(p);
          FileStatus stat = null;
          try {
            stat = ParityFilePair.FileStatusCache.get(fs, p);
          } catch (FileNotFoundException e) {
            LOG.warn("Path " + p  + " does not exist", e);
          }
          if (stat == null) {
            continue;
          }
          short repl = 0;
          List<FileStatus> lfs = null;
          if (codec.isDirRaid) {
            if (!stat.isDir()) {
              continue;
            }
            lfs = RaidNode.listDirectoryRaidFileStatus(conf, fs, p);
            if (lfs == null) {
              continue;
            }
            repl = DirectoryStripeReader.getReplication(lfs);
          } else {
            repl = stat.getReplication();
          }
          
          // if should not raid, will not put the file into the write list.
          if (!RaidNode.shouldRaid(conf, fs, stat, codec, lfs)) {
            if (LOG.isDebugEnabled()) {
              LOG.debug("Should not raid file: " + l);
            }
            continue;
          }
          
          // check the replication.
          if ((repl > targetReplication) ||
              (repl == targetReplication &&
                  !ParityFilePair.parityExists(stat, codec, conf))) {
            list.add(stat);
            if (codec.isDirRaid) {
              dirRaidNumBlocks += DirectoryStripeReader.getBlockNum(lfs);;
            }
          }
          // for dir-raid, we judge from number of blocks rather than
          // that of directories
          if (codec.isDirRaid && dirRaidNumBlocks >= selectLimit ||
              !codec.isDirRaid && list.size() >= selectLimit) { 
            break;
          }
        }
        if (l == null) {
          scanState.resetFileListRead();
        }
      } catch (IOException e) {
        LOG.error("Encountered error in file list read ", e);
        scanState.resetFileListRead();
      }
      return list;
    }

    /**
     * Keep processing policies.
     * If the config file has changed, then reload config file and start afresh.
     */
    private void doProcess() throws IOException, InterruptedException {
      ArrayList<PolicyInfo> allPolicies = new ArrayList<PolicyInfo>();
      ArrayList<PolicyInfo> allPoliciesWithSrcPath = new ArrayList<PolicyInfo>();
      for (PolicyInfo info : configMgr.getAllPolicies()) {
        allPolicies.add(info);
        if (info.getSrcPath() != null) {
          allPoliciesWithSrcPath.add(info);
        }
      }
      while (running) {
        Thread.sleep(RaidNode.triggerMonitorSleepTime);

        boolean reloaded = configMgr.reloadConfigsIfNecessary();
        if (reloaded) {
          allPolicies.clear();
          allPoliciesWithSrcPath.clear();
          for (PolicyInfo info : configMgr.getAllPolicies()) {
            allPolicies.add(info);
            if (info.getSrcPath() != null) {
              allPoliciesWithSrcPath.add(info);
            }
          }
        }
        LOG.info("TriggerMonitor.doProcess " + allPolicies.size());

        for (PolicyInfo info: allPolicies) {
          this.putPolicyInfo(info);

          List<FileStatus> filteredPaths = null;
          if (shouldReadFileList(info)) {
            filteredPaths = readFileList(info);
          } else if (shouldSelectFiles(info)) {
            LOG.info("Triggering Policy Filter " + info.getName() +
                " " + info.getSrcPath());
            try {
              filteredPaths = selectFiles(info, allPoliciesWithSrcPath);
            } catch (Exception e) {
              LOG.info("Exception while invoking filter on policy " + info.getName() +
                  " srcPath " + info.getSrcPath() + 
                  " exception " + StringUtils.stringifyException(e));
              continue;
            }
          } else {
            continue;
          }

          if (filteredPaths == null || filteredPaths.size() == 0) {
            LOG.info("No filtered paths for policy " + info.getName());
            continue;
          }

          // Apply the action on accepted paths
          LOG.info("Triggering Policy Action " + info.getName() +
              " " + filteredPaths.size() + " files");
          try {
            raidFiles(info, filteredPaths);
          } catch (Throwable e) {
            LOG.info("Exception while invoking action on policy " + info.getName() +
                " srcPath " + info.getSrcPath() + 
                " exception " + StringUtils.stringifyException(e), e);
            continue;
          }
        }
        
        lastTriggerTime = System.currentTimeMillis();
      }
    }
  }

  /**
   * raid a list of files, this will be overridden by subclasses of RaidNode
   */
  abstract void raidFiles(PolicyInfo info, List<FileStatus> paths) throws IOException;

  public abstract String raidJobsHtmlTable(JobMonitor.STATUS st);

  static Path getOriginalParityFile(Path destPathPrefix, Path srcPath) {
    return (srcPath == null || srcPath.getParent() == null)? destPathPrefix:
               new Path(destPathPrefix, makeRelative(srcPath));
  }

  public static long numBlocks(FileStatus stat) {
    return (long) Math.ceil(stat.getLen() * 1.0 / stat.getBlockSize());
  }

  public static long numStripes(long numBlocks, int stripeSize) {
    return (long) Math.ceil(numBlocks * 1.0 / stripeSize);
  }

  static long savingFromRaidingFile(
      EncodingCandidate ec, int stripeSize, int paritySize,
      int targetReplication, int parityReplication) {
    if (ec.startStripe != 0)
      return 0;
    FileStatus stat = ec.srcStat;
    long currentReplication = stat.getReplication();
    if (currentReplication > targetReplication) {
      long numBlocks = numBlocks(stat);
      long numStripes = numStripes(numBlocks, stripeSize);
      long sourceSaving =
          stat.getLen() * (currentReplication - targetReplication);
      long parityBlocks = numStripes * paritySize;
      return sourceSaving - parityBlocks * parityReplication * stat.getBlockSize();
    }
    return 0;
  }
  
  static public List<EncodingCandidate> splitPaths(Configuration conf,
      Codec codec, FileStatus path) throws IOException {
    List<FileStatus> lfs = new ArrayList<FileStatus>();
    lfs.add(path);
    return splitPaths(conf, codec, lfs);
  }
  
  static public List<EncodingCandidate> splitPaths(Configuration conf,
      Codec codec, List<FileStatus> paths) throws IOException {
    List<EncodingCandidate> lec = new ArrayList<EncodingCandidate>();
    long encodingUnit = conf.getLong(RAID_ENCODING_STRIPES_KEY, 
        DEFAULT_RAID_ENCODING_STRIPES);
    FileSystem srcFs = FileSystem.get(conf);
    for (FileStatus s : paths) {
      if (codec.isDirRaid != s.isDir()) {
        continue;
      }
      long numBlocks = 0L;
      if (codec.isDirRaid) {
        List<FileStatus> lfs = RaidNode.listDirectoryRaidFileStatus(
            conf, srcFs, s.getPath());
        if (lfs == null) {
          continue;
        }
        for (FileStatus stat : lfs) {
          numBlocks += RaidNode.numBlocks(stat);
        }
      } else {
        numBlocks = RaidNode.numBlocks(s);
      }
      long numStripes = RaidNode.numStripes(numBlocks, codec.stripeLength);
      String encodingId = System.currentTimeMillis() + "." + rand.nextLong();
      for (long startStripe = 0; startStripe < numStripes;
           startStripe += encodingUnit) {
        lec.add(new EncodingCandidate(s, startStripe, encodingId, encodingUnit,
            s.getModificationTime()));
      }
    }
    return lec;
  }

  /**
   * RAID a list of files / directories
   * @throws InterruptedException 
   */
  void doRaid(Configuration conf, PolicyInfo info, List<EncodingCandidate> paths)
      throws IOException {
    int targetRepl = Integer.parseInt(info.getProperty("targetReplication"));
    int metaRepl = Integer.parseInt(info.getProperty("metaReplication"));
    Codec codec = Codec.getCodec(info.getCodecId());
    Path destPref = new Path(codec.parityDirectory);
    String simulate = info.getProperty("simulate");
    boolean doSimulate = simulate == null ? false : Boolean
        .parseBoolean(simulate);

    Statistics statistics = new Statistics();
    int count = 0;

    for (EncodingCandidate ec : paths) {
      doRaid(conf, ec, destPref, codec, statistics, 
            RaidUtils.NULL_PROGRESSABLE, doSimulate,
            targetRepl, metaRepl);
      if (count % 1000 == 0) {
        LOG.info("RAID statistics " + statistics.toString());
      }
      count++;
    }
    LOG.info("RAID statistics " + statistics.toString());
  }

  static public boolean doRaid(Configuration conf, PolicyInfo info,
      FileStatus src, Statistics statistics, Progressable reporter) 
          throws IOException {
    List<EncodingCandidate> lec =
        splitPaths(conf, Codec.getCodec(info.getCodecId()), src); 
    boolean succeed = false;
    for (EncodingCandidate ec: lec) {
      succeed = succeed || doRaid(conf, info, ec, statistics, reporter);
    }
    return succeed;
  }

  /**
   * RAID an individual file/directory
   * @throws InterruptedException 
   */
  static public boolean doRaid(Configuration conf, PolicyInfo info,
      EncodingCandidate src, Statistics statistics, 
      Progressable reporter) 
          throws IOException {
    int targetRepl = Integer.parseInt(info.getProperty("targetReplication"));
    int metaRepl = Integer.parseInt(info.getProperty("metaReplication"));

    Codec codec = Codec.getCodec(info.getCodecId());
    Path destPref = new Path(codec.parityDirectory);

    String simulate = info.getProperty("simulate");
    boolean doSimulate = simulate == null ? false : Boolean
        .parseBoolean(simulate);

    return doRaid(conf, src, destPref, codec, statistics,
          reporter, doSimulate, targetRepl, metaRepl);
  }
  
  public static List<FileStatus> listDirectoryRaidFileStatus(Configuration conf,
      FileSystem srcFs, Path p) throws IOException {
    long minFileSize = conf.getLong(MINIMUM_RAIDABLE_FILESIZE_KEY,
        MINIMUM_RAIDABLE_FILESIZE);
    List<FileStatus> lfs = new ArrayList<FileStatus>();
    FileStatus[] files =  srcFs.listStatus(p);
    if (null == files) {
      return null;
    }
    for (FileStatus stat : files) {
      if (stat.isDir()) {
        return null;
      } 
      // We don't raid too small files
      if (stat.getLen() < minFileSize) {
        continue;
      }
      lfs.add(stat);
    }
    if (lfs.size() == 0)
      return null;
    return lfs;
  }
  
  public static List<LocatedFileStatus> listDirectoryRaidLocatedFileStatus(
      Configuration conf, FileSystem srcFs, Path p) throws IOException {
    long minFileSize = conf.getLong(MINIMUM_RAIDABLE_FILESIZE_KEY,
        MINIMUM_RAIDABLE_FILESIZE);
    List<LocatedFileStatus> lfs = new ArrayList<LocatedFileStatus>();
    RemoteIterator<LocatedFileStatus> iter = srcFs.listLocatedStatus(p);
    while (iter.hasNext()) {
      LocatedFileStatus stat = iter.next();
      if (stat.isDir()) {
        return null;
      }
      // We don't raid too small files
      if (stat.getLen() < minFileSize) {
        continue;
      }
      lfs.add(stat);
    }
    if (lfs.size() == 0)
      return null;
    return lfs;
  }
  
  // only used by test
  public static boolean doRaid(Configuration conf, FileStatus stat,
      Path destPath, Codec codec, Statistics statistics,
      Progressable reporter, boolean doSimulate, int targetRepl, int metaRepl)
          throws IOException {
    boolean succeed = false;
    for (EncodingCandidate ec : RaidNode.splitPaths(conf, codec, stat)) {
      succeed = succeed || doRaid(conf, ec, destPath, codec, statistics,
          reporter, doSimulate, targetRepl, metaRepl);
    }
    return succeed;
  }
  
  public static boolean doRaid(Configuration conf, EncodingCandidate ec,
      Path destPath, Codec codec, Statistics statistics,
      Progressable reporter, boolean doSimulate, int targetRepl, int metaRepl)
        throws IOException {
    long startTime = System.currentTimeMillis();
    LOGRESULTS result = LOGRESULTS.FAILURE;
    Throwable ex = null;
    try {
      if (codec.isDirRaid) {
        result = doDirRaid(conf, ec, destPath, codec, statistics, reporter,
            doSimulate, targetRepl, metaRepl);
      } else {
        result = doFileRaid(conf, ec, destPath, codec, statistics, reporter,
            doSimulate, targetRepl, metaRepl);
      }
      return result == LOGRESULTS.SUCCESS;
    } catch (IOException ioe) {
      ex = ioe;
      throw ioe;
    } catch (InterruptedException e) {
      ex = e;
      throw new IOException(e);
    } finally {
      long delay = System.currentTimeMillis() - startTime;
      long savingBytes = statistics.processedSize - 
          statistics.remainingSize - statistics.metaSize;
      FileStatus stat = ec.srcStat;
      FileSystem srcFs = stat.getPath().getFileSystem(conf);
      if (result != LOGRESULTS.NOACTION) {
        LogUtils.logRaidEncodingMetrics(result, codec,
            delay, statistics.processedSize, statistics.numProcessedBlocks,
            statistics.numMetaBlocks, statistics.metaSize, 
            savingBytes, stat.getPath(), LOGTYPES.ENCODING, srcFs, ex,
            reporter);
      }
    }
  }
  
  /**
   * check if the file is already raided by high priority codec
   */
  public static boolean raidedByOtherHighPriCodec(Configuration conf,
        FileStatus stat, Codec codec) throws IOException {
    
    for (Codec tcodec : Codec.getCodecs()) {
      if (tcodec.priority > codec.priority) {
        if (stat.isDir() && !tcodec.isDirRaid) {
          // A directory could not be raided by a file level codec.
          continue;
        }
        
        // check if high priority parity file exists.
        if (ParityFilePair.parityExists(stat, tcodec, conf)) {
          InjectionHandler.processEvent(InjectionEvent.RAID_ENCODING_SKIP_PATH);
          return true;
        }
      }
    }
    
    return false;
  }
  
  private static boolean tooNewForRaid(FileStatus stat) {
    
    if (System.currentTimeMillis() - stat.getModificationTime() < modTimePeriod) {
      InjectionHandler.processEvent(InjectionEvent.RAID_ENCODING_SKIP_PATH_TOO_NEW_MOD);
      if (LOG.isDebugEnabled()) {
        LOG.debug("Skip file: " + stat.getPath() + 
            " with too new modification time: " + stat.getModificationTime());
      }
      return true;
    }
    
    return false;
  }
  
  /**
   * Decide whether a file/directory is too small to raid.
   */
  public static boolean shouldRaid(Configuration conf, FileSystem srcFs, 
      FileStatus stat, Codec codec, List<FileStatus> lfs) throws IOException {
    Path p = stat.getPath();
    long blockNum = 0L;
    if (stat.isDir() != codec.isDirRaid) {
      return false;
    }
    
    if (tooNewForRaid(stat)) {
      return false;
    }
    
    blockNum = codec.isDirRaid ? 
        DirectoryStripeReader.getBlockNum(lfs) : numBlocks(stat);
    
    // if the file/directory has fewer than 2 blocks, then nothing to do
    if (blockNum <= RaidState.TOO_SMALL_NOT_RAID_NUM_BLOCKS) {
      return false;
    }
    
    return !raidedByOtherHighPriCodec(conf, stat, codec);
  }
  
  public static boolean shouldRaid(Configuration conf, FileSystem srcFs, 
      FileStatus stat, Codec codec) throws IOException {
    Path p = stat.getPath();
    if (stat.isDir() != codec.isDirRaid) {
      return false;
    }
    
    if (tooNewForRaid(stat)) {
      return false;
    }
    
    List<FileStatus> lfs = null;
    if (codec.isDirRaid) {
      // add up the total number of blocks in the directory
      lfs = RaidNode.listDirectoryRaidFileStatus(conf, srcFs, p);
      if (null == lfs) {
        return false;
      }
    }
    
    return shouldRaid(conf, srcFs, stat, codec, lfs);
  }
  
  public static long getNumBlocks(FileStatus stat) {
    long numBlocks = stat.getLen() / stat.getBlockSize();
    if (stat.getLen() % stat.getBlockSize() == 0)
      return numBlocks;
    else 
      return numBlocks + 1;
  }
  
  /**
   * RAID an individual directory
   * @throws InterruptedException 
   */
  private static LOGRESULTS doDirRaid(Configuration conf, EncodingCandidate ec,
      Path destPath, Codec codec, Statistics statistics,
      Progressable reporter, boolean doSimulate, int targetRepl, int metaRepl)
        throws IOException {
    FileStatus stat = ec.srcStat;
    Path p = stat.getPath();
    FileSystem srcFs = p.getFileSystem(conf);
    List<FileStatus> lfs = RaidNode.listDirectoryRaidFileStatus(conf, srcFs, p);
    if (lfs == null) {
      return LOGRESULTS.NOACTION;
    }
    
    // add up the total number of blocks in the directory
    long blockNum = DirectoryStripeReader.getBlockNum(lfs);
    
    // if the directory has fewer than 2 blocks, then nothing to do
    if (blockNum <= 2) {
      return LOGRESULTS.NOACTION;
    }
    // add up the raw disk space occupied by this directory 
    long diskSpace = 0; 
    // we use the maximum replication
    int srcRepl = 0;
    for (FileStatus fsStat: lfs) {
      diskSpace += (fsStat.getLen() * fsStat.getReplication());
      if (fsStat.getReplication() > srcRepl) {
        srcRepl = fsStat.getReplication();
      }
    }
    long parityBlockSize = DirectoryStripeReader.getParityBlockSize(conf, lfs); 


    statistics.numProcessedBlocks += blockNum;
    statistics.processedSize += diskSpace;

    boolean parityGenerated = false;
    // generate parity file
    try {
      parityGenerated = generateParityFile(conf, ec, targetRepl, reporter, srcFs,
          destPath, codec, blockNum, srcRepl, metaRepl, parityBlockSize, lfs);
    } catch (InterruptedException e) {
      throw new IOException (e);
    }
    if (!parityGenerated) 
      return LOGRESULTS.NOACTION;
    if (!doSimulate) {
      for (FileStatus fsStat: lfs) {
        if (srcFs.setReplication(fsStat.getPath(), (short)targetRepl) == false) {
          LOG.info("Error in reducing replication of " +
            fsStat.getPath() + " to " + targetRepl);
          statistics.remainingSize += diskSpace;
          return LOGRESULTS.FAILURE;
        }
      };
    }

    diskSpace = 0;
    for (FileStatus fsStat: lfs) {
      diskSpace += (fsStat.getLen() * targetRepl);
    }
    statistics.remainingSize += diskSpace;

    // the metafile will have this many number of blocks
    long numMeta = blockNum / codec.stripeLength;
    if (blockNum % codec.stripeLength != 0) {
      numMeta++;
    }

    // we create numMeta blocks. This metablock has metaRepl # replicas.
    // the last block of the metafile might not be completely filled up, but we
    // ignore that for now.
    statistics.numMetaBlocks += (numMeta * metaRepl);
    statistics.metaSize += (numMeta * metaRepl * parityBlockSize);
    return LOGRESULTS.SUCCESS;
  }

  /**
   * RAID an individual file
   * @throws InterruptedException 
   */
  private static LOGRESULTS doFileRaid(Configuration conf, EncodingCandidate ec,
      Path destPath, Codec codec, Statistics statistics,
      Progressable reporter, boolean doSimulate, int targetRepl, int metaRepl)
        throws IOException, InterruptedException {
    FileStatus stat = ec.srcStat;
    Path p = stat.getPath();
    FileSystem srcFs = p.getFileSystem(conf);

    // extract block locations from File system
    BlockLocation[] locations = srcFs.getFileBlockLocations(stat, 0, stat.getLen());
    // if the file has fewer than 2 blocks, then nothing to do
    if (locations.length <= 2) {
      return LOGRESULTS.NOACTION;
    }

    // add up the raw disk space occupied by this file
    long diskSpace = 0;
    for (BlockLocation l: locations) {
      diskSpace += (l.getLength() * stat.getReplication());
    }
    statistics.numProcessedBlocks += locations.length;
    statistics.processedSize += diskSpace;

    // generate parity file
    boolean parityGenerated = generateParityFile(conf, ec, targetRepl, reporter,
        srcFs, destPath, codec, locations.length, stat.getReplication(),
        metaRepl, stat.getBlockSize(), null);
    if (!parityGenerated) {
      return LOGRESULTS.NOACTION;
    }
    if (!doSimulate) {
      if (srcFs.setReplication(p, (short)targetRepl) == false) {
        LOG.info("Error in reducing replication of " + p + " to " + targetRepl);
        statistics.remainingSize += diskSpace;
        return LOGRESULTS.FAILURE;
      };
    }

    diskSpace = 0;
    for (BlockLocation l: locations) {
      diskSpace += (l.getLength() * targetRepl);
    }
    statistics.remainingSize += diskSpace;

    // the metafile will have this many number of blocks
    int numMeta = locations.length / codec.stripeLength;
    if (locations.length % codec.stripeLength != 0) {
      numMeta++;
    }

    // we create numMeta for every file. This metablock has metaRepl # replicas.
    // the last block of the metafile might not be completely filled up, but we
    // ignore that for now.
    statistics.numMetaBlocks += (numMeta * metaRepl);
    statistics.metaSize += (numMeta * metaRepl * stat.getBlockSize());
    return LOGRESULTS.SUCCESS;
  }

  /**
   * Generate parity file
   * @throws InterruptedException 
   */
  static private boolean generateParityFile(Configuration conf, EncodingCandidate ec,
                                  int targetRepl,
                                  Progressable reporter,
                                  FileSystem inFs,
                                  Path destPathPrefix,
                                  Codec codec,
                                  long blockNum,
                                  int srcRepl,
                                  int metaRepl, 
                                  long blockSize,
                                  List<FileStatus> lfs)
                                      throws IOException, InterruptedException {
    FileStatus stat = ec.srcStat;
    Path inpath = stat.getPath();
    Path outpath =  getOriginalParityFile(destPathPrefix, inpath);
    FileSystem outFs = inFs;

    // If the parity file is already upto-date and source replication is set
    // then nothing to do.
    try {
      FileStatus stmp = outFs.getFileStatus(outpath);
      if (stmp.getModificationTime() == stat.getModificationTime() &&
          srcRepl == targetRepl) {
        LOG.info("Parity file for " + inpath + "(" + blockNum +
            ") is " + outpath + " already upto-date and " +
            "file is at target replication . Nothing more to do.");
        return false;
      }
    } catch (IOException e) {
      // ignore errors because the raid file might not exist yet.
    }

    Encoder encoder = new Encoder(conf, codec);
    encoder.verifyStore();
    StripeReader sReader = null;
    boolean parityGenerated = false;
    if (codec.isDirRaid) {
      long numStripes = (blockNum % codec.stripeLength == 0) ?
          (blockNum / codec.stripeLength) :
          ((blockNum / codec.stripeLength) + 1);
      
      sReader = new DirectoryStripeReader(conf, codec, inFs,
          ec.startStripe, ec.encodingUnit, inpath, lfs);
      parityGenerated = encoder.encodeFile(conf, inFs, outFs, outpath, 
          (short)metaRepl, numStripes, blockSize, reporter, sReader, ec);
    } else {
      FileStatus srcStat = inFs.getFileStatus(inpath);
      long srcSize = srcStat.getLen();
      long numBlocks = (srcSize % blockSize == 0) ?
                        (srcSize / blockSize) :
                        ((srcSize / blockSize) + 1);
      long numStripes = (numBlocks % codec.stripeLength == 0) ?
                        (numBlocks / codec.stripeLength) :
                        ((numBlocks / codec.stripeLength) + 1);
      sReader = new FileStripeReader(conf, blockSize, 
          codec, inFs, ec.startStripe, ec.encodingUnit, inpath, srcSize);
      parityGenerated = encoder.encodeFile(conf, inFs, outFs, outpath,
          (short)metaRepl, numStripes, blockSize, reporter, sReader, ec);
    }
    if (!parityGenerated) {
      return false;
    }

    // set the modification time of the RAID file. This is done so that the modTime of the
    // RAID file reflects that contents of the source file that it has RAIDed. This should
    // also work for files that are being appended to. This is necessary because the time on
    // on the destination namenode may not be synchronised with the timestamp of the 
    // source namenode.
    outFs.setTimes(outpath, stat.getModificationTime(), -1);

    FileStatus outstat = outFs.getFileStatus(outpath);
    FileStatus inStat = inFs.getFileStatus(inpath);
    if (stat.getModificationTime() != inStat.getModificationTime()) {
      String msg = "Source file changed mtime during raiding from " +
          stat.getModificationTime() + " to " + inStat.getModificationTime();
      throw new IOException(msg);
    }
    if (outstat.getModificationTime() != inStat.getModificationTime()) {
      String msg = "Parity file mtime " + outstat.getModificationTime() +
          " does not match source mtime " + inStat.getModificationTime();
      throw  new IOException(msg);
    }
    LOG.info("Source file " + inpath + " of size " + inStat.getLen() +
        " Parity file " + outpath + " of size " + outstat.getLen() +
        " src mtime " + stat.getModificationTime()  +
        " parity mtime " + outstat.getModificationTime());
    return true;
  }

  
  public static DecoderInputStream unRaidCorruptInputStream(Configuration conf,
              Path srcPath, Codec codec, ParityFilePair parityFilePair, 
              Block lostBlock,
              long blockSize,
              long corruptOffset,
              long limit,
              boolean useStripeStore) 
              throws IOException {
    boolean recoverFromStripeInfo = false;
    StripeInfo si = null;
    FileSystem srcFs = srcPath.getFileSystem(conf);
    Decoder decoder = new Decoder(conf, codec);
    // Test if parity file exists
    if (parityFilePair == null) {
      if (codec.isDirRaid && useStripeStore) {
        recoverFromStripeInfo = true;
        decoder.connectToStore(srcPath);
        si = decoder.retrieveStripe(lostBlock, srcPath, -1, srcFs, null, true);
        if (si == null) {
          LOG.warn("Could not find " + codec.id + " parity file for " + srcPath
              + ", including the stripe store");
          return null;
        }
      } else {
        LOG.warn("Could not find " + codec.id + " parity file for " + srcPath
              + " without stripe store");
        return null;
      }
    }
    
    return decoder.generateAlternateStream(srcFs, srcPath, 
        parityFilePair == null ? srcFs : parityFilePair.getFileSystem(), 
        parityFilePair == null ? null : parityFilePair.getPath(), 
        blockSize, corruptOffset, limit, lostBlock, si, recoverFromStripeInfo, null);
  }
  
  private void doHar() throws IOException, InterruptedException {
    long prevExec = 0;
    while (running) {

      // The config may be reloaded by the TriggerMonitor. 
      // This thread uses whatever config is currently active.
      while(now() < prevExec + configMgr.getPeriodicity()){
        Thread.sleep(SLEEP_TIME);
      }

      LOG.info("Started archive scan");
      prevExec = now();

      // fetch all categories
      for (Codec codec : Codec.getCodecs()) {
        if (codec.isDirRaid) {
          // Disable har for directory raid
          continue;
        }
        try {
          String tmpHarPath = codec.tmpHarDirectory;
          int harThresold = conf.getInt(RAID_PARITY_HAR_THRESHOLD_DAYS_KEY,
              DEFAULT_RAID_PARITY_HAR_THRESHOLD_DAYS);
          long cutoff = now() - ( harThresold * 24L * 3600000L );

          Path destPref = new Path(codec.parityDirectory);
          FileSystem destFs = destPref.getFileSystem(conf);
          FileStatus destStat = null;
          try {
            destStat = destFs.getFileStatus(destPref);
          } catch (FileNotFoundException e) {
            continue;
          }

          LOG.info("Haring parity files in " + destPref);
          recurseHar(codec, destFs, destStat, destPref.toUri().getPath(),
            destFs, cutoff, tmpHarPath);
        } catch (Exception e) {
          LOG.warn("Ignoring Exception while haring ", e);
        }
      }
    }
    return;
  }
  
  void recurseHar(Codec codec, FileSystem destFs, FileStatus dest,
      String destPrefix, FileSystem srcFs, long cutoff, String tmpHarPath)
          throws IOException {

    if (!dest.isDir()) {
      return;
    }

    Path destPath = dest.getPath(); // pathname, no host:port
    String destStr = destPath.toUri().getPath();

    // If the source directory is a HAR, do nothing.
    if (destStr.endsWith(".har")) {
      return;
    }

    // Verify if it already contains a HAR directory
    if ( destFs.exists(new Path(destPath, destPath.getName()+HAR_SUFFIX)) ) {
      return;
    }

    boolean shouldHar = false;
    FileStatus[] files = destFs.listStatus(destPath);
    long harBlockSize = -1;
    short harReplication = -1;
    if (files != null) {
      shouldHar = files.length > 0;
      for (FileStatus one: files) {
        if (one.isDir()){
          recurseHar(codec, destFs, one, destPrefix, srcFs, cutoff, tmpHarPath);
          shouldHar = false;
        } else if (one.getModificationTime() > cutoff ) {
          if (shouldHar) {
            LOG.debug("Cannot archive " + destPath + 
                " because " + one.getPath() + " was modified after cutoff");
            shouldHar = false;
          }
        } else {
          if (harBlockSize == -1) {
            harBlockSize = one.getBlockSize();
          } else if (harBlockSize != one.getBlockSize()) {
            LOG.info("Block size of " + one.getPath() + " is " +
                one.getBlockSize() + " which is different from " + harBlockSize);
            shouldHar = false;
          }
          if (harReplication == -1) {
            harReplication = one.getReplication();
          } else if (harReplication != one.getReplication()) {
            LOG.info("Replication of " + one.getPath() + " is " +
                one.getReplication() + " which is different from " + harReplication);
            shouldHar = false;
          }
        }
      }

      if (shouldHar) {
        String src = destStr.replaceFirst(destPrefix, "");
        Path srcPath = new Path(src);
        FileStatus[] statuses = srcFs.listStatus(srcPath);
        Path destPathPrefix = new Path(destPrefix).makeQualified(destFs);
        if (statuses != null) {
          for (FileStatus status : statuses) {
            if (ParityFilePair.getParityFile(codec,
                status, conf) == null ) {
              LOG.debug("Cannot archive " + destPath + 
                  " because it doesn't contain parity file for " +
                  status.getPath().makeQualified(srcFs) + " on destination " +
                  destPathPrefix);
              shouldHar = false;
              break;
            }
          }
        }
      }

    }

    if ( shouldHar ) {
      LOG.info("Archiving " + dest.getPath() + " to " + tmpHarPath );
      singleHar(codec, destFs, dest, tmpHarPath, harBlockSize, harReplication);
    }
  } 

  
  private void singleHar(Codec codec, FileSystem destFs,
       FileStatus dest, String tmpHarPath, long harBlockSize,
       short harReplication) throws IOException {

    Random rand = new Random();
    Path root = new Path("/");
    Path qualifiedPath = dest.getPath().makeQualified(destFs);
    String harFileDst = qualifiedPath.getName() + HAR_SUFFIX;
    String harFileSrc = qualifiedPath.getName() + "-" + 
        rand.nextLong() + "-" + HAR_SUFFIX;

    // HadoopArchives.HAR_PARTFILE_LABEL is private, so hard-coding the label.
    conf.setLong("har.partfile.size", configMgr.getHarPartfileSize());
    conf.setLong("har.block.size", harBlockSize);
    HadoopArchives har = new HadoopArchives(conf);
    String[] args = new String[7];
    args[0] = "-Ddfs.replication=" + harReplication;
    args[1] = "-archiveName";
    args[2] = harFileSrc;
    args[3] = "-p"; 
    args[4] = root.makeQualified(destFs).toString();
    args[5] = qualifiedPath.toUri().getPath().substring(1);
    args[6] = tmpHarPath.toString();
    int ret = 0;
    Path tmpHar = new Path(tmpHarPath + "/" + harFileSrc);
    try {
      ret = ToolRunner.run(har, args);
      if (ret == 0 && !destFs.rename(tmpHar,
          new Path(qualifiedPath, harFileDst))) {
        LOG.info("HAR rename didn't succeed from " + tmpHarPath+"/"+harFileSrc +
            " to " + qualifiedPath + "/" + harFileDst);
        ret = -2;
      }
    } catch (Exception exc) {
      throw new IOException("Error while creating archive " + ret, exc);
    } finally {
      destFs.delete(tmpHar, true);
    }

    if (ret != 0){
      throw new IOException("Error while creating archive " + ret);
    }
    return;
  }

  /**
   * Periodically generates HAR files
   */
  class HarMonitor implements Runnable {

    public void run() {
      while (running) {
        try {
          doHar();
        } catch (Exception e) {
          LOG.error(StringUtils.stringifyException(e));
        } finally {
          LOG.info("Har parity files thread continuing to run...");
        }
      }
      LOG.info("Leaving Har thread.");
    }
  }

  static boolean isParityHarPartFile(Path p) {
    Matcher m = PARITY_HAR_PARTFILE_PATTERN.matcher(p.toUri().getPath());
    return m.matches();
  }

  /**
   * Returns current time.
   */
  static long now() {
    return System.currentTimeMillis();
  }

  /**                       
   * Make an absolute path relative by stripping the leading /
   */   
  static Path makeRelative(Path path) {
    if (!path.isAbsolute()) {
      return path;
    }          
    String p = path.toUri().getPath();
    String relative = p.substring(1, p.length());
    return new Path(relative);
  } 

  private static void printUsage() {
    System.err.println("Usage: java RaidNode ");
  }

  private static StartupOption parseArguments(String args[]) {
    StartupOption startOpt = StartupOption.REGULAR;
    return startOpt;
  }

  /**
   * Convert command line options to configuration parameters
   */
  private static void setStartupOption(Configuration conf, StartupOption opt) {
    conf.set("fs.raidnode.startup", opt.toString());
  }

  /**
   * Create an instance of the appropriate subclass of RaidNode 
   */
  public static RaidNode createRaidNode(Configuration conf)
      throws ClassNotFoundException {
    try {
      // default to distributed raid node
      Class<?> raidNodeClass =
          conf.getClass(RAIDNODE_CLASSNAME_KEY, DistRaidNode.class);
      if (!RaidNode.class.isAssignableFrom(raidNodeClass)) {
        throw new ClassNotFoundException("not an implementation of RaidNode");
      }
      Constructor<?> constructor =
          raidNodeClass.getConstructor(new Class[] {Configuration.class} );
      return (RaidNode) constructor.newInstance(conf);
    } catch (NoSuchMethodException e) {
      throw new ClassNotFoundException("cannot construct raidnode", e);
    } catch (InstantiationException e) {
      throw new ClassNotFoundException("cannot construct raidnode", e);
    } catch (IllegalAccessException e) {
      throw new ClassNotFoundException("cannot construct raidnode", e);
    } catch (InvocationTargetException e) {
      throw new ClassNotFoundException("cannot construct raidnode", e);
    }
  }
  
  static public ChecksumStore createChecksumStore(Configuration conf,
      boolean createStore) {
    Class<?> checksumStoreClass = null;
    checksumStoreClass = conf.getClass(RAID_CHECKSUM_STORE_CLASS_KEY, null);
    if (checksumStoreClass == null) {
      return null;
    }
    ChecksumStore checksumStore = (ChecksumStore) ReflectionUtils.newInstance(
        checksumStoreClass, conf);
    try {
      checksumStore.initialize(conf, createStore);
    } catch (IOException ioe) {
      LOG.error("Fail to initialize checksum store", ioe);
      checksumStore = null;
    }
    return checksumStore;
  }
  
  static public StripeStore createStripeStore(Configuration conf
      , boolean createStore, FileSystem fs) {
    Class<?> stripeStoreClass = null;
    stripeStoreClass = conf.getClass(RAID_STRIPE_STORE_CLASS_KEY, null);
    if (stripeStoreClass == null) {
      return null;
    }
    StripeStore stripeStore = (StripeStore) ReflectionUtils.newInstance(
        stripeStoreClass, conf);
    try {
      stripeStore.initialize(conf, createStore, fs);
    } catch (IOException ioe) {
      LOG.error("Fail to initialize stripe store", ioe);
      stripeStore = null;
    }
    return stripeStore;
  }

  /**
   * Create an instance of the RaidNode 
   */
  public static RaidNode createRaidNode(String argv[], Configuration conf)
      throws IOException, ClassNotFoundException {
    if (conf == null) {
      conf = new Configuration();
    }
    StartupOption startOpt = parseArguments(argv);
    if (startOpt == null) {
      printUsage();
      return null;
    }
    setStartupOption(conf, startOpt);
    RaidNode node = createRaidNode(conf);
    return node;
  }

  /**
   * Get the job id from the configuration
   */
  public static String getJobID(Configuration conf) {
    String jobId = conf.get("mapred.job.id", null);
    if (jobId == null) {
      jobId = "localRaid" + df.format(new Date());
      conf.set("mapred.job.id", jobId);
    }
    return jobId;
  }
  
  public String getReadReconstructionMetricsUrl() {
    return configMgr.getReadReconstructionMetricsUrl();
  }
  
  @Override //RaidNodeStatusMBean
  public long getTimeSinceLastSuccessfulFix() {
    return this.blockIntegrityMonitor.getTimeSinceLastSuccessfulFix();
  }
  
  @Override //RaidNodeStatusMBean
  public long getNumUnderRedundantFilesFailedIncreaseReplication() {
    return this.urfProcessor.failedFilesCount.get();
  }
  
  @Override //RaidNodeStatusMBean
  public long getNumUnderRedundantFilesSucceededIncreaseReplication() {
    return this.urfProcessor.succeedFilesCount.get();
  }

  public static void main(String argv[]) throws Exception {
    try {
      StringUtils.startupShutdownMessage(RaidNode.class, argv, LOG);
      RaidNode raid = createRaidNode(argv, null);
      if (raid != null) {
        raid.join();
      }
    } catch (Throwable e) {
      LOG.error(StringUtils.stringifyException(e));
      System.exit(-1);
    }
  }
}
