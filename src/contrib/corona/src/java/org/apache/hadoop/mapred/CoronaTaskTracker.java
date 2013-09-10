package org.apache.hadoop.mapred;

import java.io.File;
import java.io.IOException;
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.util.Collection;
import java.util.EnumMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.corona.ClusterManagerAvailabilityChecker;
import org.apache.hadoop.corona.ClusterManagerService;
import org.apache.hadoop.corona.ClusterNode;
import org.apache.hadoop.corona.ClusterNodeInfo;
import org.apache.hadoop.corona.ComputeSpecs;
import org.apache.hadoop.corona.CoronaConf;
import org.apache.hadoop.corona.CoronaTaskTrackerService;
import org.apache.hadoop.corona.DisallowedNode;
import org.apache.hadoop.corona.InetAddress;
import org.apache.hadoop.corona.InvalidSessionHandle;
import org.apache.hadoop.corona.ResourceType;
import org.apache.hadoop.corona.SafeModeException;
import org.apache.hadoop.corona.NodeHeartbeatResponse;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.ipc.ProtocolSignature;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RPC.Server;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.util.CoronaFailureEventInjector;
import org.apache.hadoop.util.DiskChecker.DiskErrorException;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.ResourceCalculatorPlugin;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.VersionInfo;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

public class CoronaTaskTracker extends TaskTracker
    implements CoronaTaskTrackerProtocol, CoronaTaskTrackerService.Iface {

  public static final Log LOG = LogFactory.getLog(CoronaTaskTracker.class);
  public static final String CORONA_TASK_TRACKER_SERVER_CLIENTTIMEOUT_KEY = "corona.task.tracker.server.clienttimeout";
  public static final String CORONA_TASK_TRACKER_HANDLER_COUNT_KEY = "corona.task.tracker.handler.count";
  public static final String HEART_BEAT_INTERVAL_KEY = "corona.clustermanager.heartbeat.interval";
  public static final String JT_CONNECT_TIMEOUT_MSEC_KEY = "corona.jobtracker.connect.timeout.msec";
  /**
   * Multiplier for the number of slots to simulate on Corona to allow task
   * overlapping (1 means no task overlapping) key
   */
  public static final String SLOT_MULTIPLIER_KEY = "corona.slot.multiplier";
  /**
   * Default multiplier for the number of slots to simulate on Corona to allow
   * task overlapping, 10 seems to be enough.
   */
  public static final int SLOT_MULTIPLIER_DEFAULT = 10;
  private static final int MAX_CM_CONNECT_RETRIES = 10;

  private ClusterManagerService.Client client = null;
  private TTransport transport = null;
  // Thrift server to serve ClusterManager
  private TServer clusterManagerCallbackServer = null;
  private TServerThread clusterManagerCallbackServerThread = null;
  InetAddress clusterManagerCallbackServerAddr = null;
  InetSocketAddress actionServerAddr = null;
  ConcurrentHashMap<String, String> blacklistedSessions =
      new ConcurrentHashMap<String, String>();
  private final long heartbeatCMInterval;
  private volatile long lastCMHeartbeat = 0;
  Server actionServer;
  ConcurrentHashMap<JobID, JobTrackerReporter> jobTrackerReporters;
  long jtConnectTimeoutMsec = 0;
  private int clusterManagerConnectRetries;
  private CoronaReleaseManager crReleaseManager;
  /**
   * Multiplier for the number of slots to simulate on Corona to allow
   * task overlapping (1 means no task overlapping)
   */
  private final int slotMultiplier;

  /**
   * Purge old Corona Job Tracker logs.
   */
  private final Thread cjtLogCleanupThread =
    new Thread(
      new LogCleanupThread(
        TaskLog.getLogDir(CoronaTaskTracker.jobTrackerLogDir())),
      "CJTLogCleanup");
  
  // for failure emulation
  CoronaFailureEventInjector jtFailureEventInjector = null;
  public void setJTFailureEventInjector(CoronaFailureEventInjector jtFailureEventInjector) {
    this.jtFailureEventInjector = jtFailureEventInjector;
  }
  
  public CoronaTaskTracker(JobConf conf) throws IOException {
    slotMultiplier = conf.getInt(SLOT_MULTIPLIER_KEY, SLOT_MULTIPLIER_DEFAULT);
    // Default is to use netty over jetty
    boolean useNetty = conf.getBoolean(NETTY_MAPOUTPUT_USE, true);
    this.shuffleServerMetrics = new ShuffleServerMetrics(conf);
    if (useNetty) {
      initNettyMapOutputHttpServer(conf);
    }
    initHttpServer(conf, useNetty);
    LOG.info("Http port " + httpPort +
        ", netty map output http port " + nettyMapOutputHttpPort +
        ", use netty = " + useNetty);
    super.initialize(conf);
    initializeTaskActionServer();
    initializeClusterManagerCallbackServer();
    initializeCleanupThreads();
    heartbeatCMInterval = conf.getLong(HEART_BEAT_INTERVAL_KEY, 3000L);
    jtConnectTimeoutMsec = conf.getLong(JT_CONNECT_TIMEOUT_MSEC_KEY, 60000L);
    crReleaseManager = new CoronaReleaseManager(conf);
    crReleaseManager.start();
  }

  private synchronized void initializeTaskActionServer() throws IOException {
    // Create Hadoop RPC to serve JobTrackers
    actionServerAddr = NetUtils.createSocketAddr(getLocalHostname(), 0);
    int handlerCount = fConf.getInt(CORONA_TASK_TRACKER_HANDLER_COUNT_KEY, 10);
    this.actionServer = RPC.getServer
      (this, actionServerAddr.getHostName(), 0, handlerCount, false, fConf);
    this.actionServer.start();
    actionServerAddr = actionServer.getListenerAddress();
    LOG.info("TaskActionServer up at " +
      actionServerAddr.getHostName() + ":" + actionServerAddr.getPort());
    jobTrackerReporters = new ConcurrentHashMap<JobID, JobTrackerReporter>();
    String dir = fConf.get(JobTracker.MAPRED_SYSTEM_DIR_KEY,
        JobTracker.DEFAULT_MAPRED_SYSTEM_DIR);
    if (dir == null) {
      throw new IOException("Failed to get system directory");
    }
    systemDirectory = new Path(dir);
    systemFS = systemDirectory.getFileSystem(fConf);
  }

  private synchronized void initializeClusterManagerCallbackServer()
      throws IOException {
    // Create thrift RPC to serve ClusterManager
    int soTimeout = fConf.getInt(
        CORONA_TASK_TRACKER_SERVER_CLIENTTIMEOUT_KEY, 30 * 1000);
    ServerSocket serverSocket = new ServerSocket();
    serverSocket.setReuseAddress(true);
    serverSocket.bind(new InetSocketAddress(0));
    TServerSocket tSocket = new TServerSocket(serverSocket, soTimeout);
    CoronaTaskTrackerService.Processor proc =
        new CoronaTaskTrackerService.Processor(this);
    TBinaryProtocol.Factory protocolFactory =
        new TBinaryProtocol.Factory(true, true);
    TThreadPoolServer.Args args = new TThreadPoolServer.Args(tSocket);
    args.processor(proc);
    args.protocolFactory(protocolFactory);
    clusterManagerCallbackServer = new TThreadPoolServer(args);
    clusterManagerCallbackServerThread =
        new TServerThread(clusterManagerCallbackServer);
    clusterManagerCallbackServerThread.start();
    clusterManagerCallbackServerAddr = new InetAddress(
        getLocalHostname(), serverSocket.getLocalPort());
    LOG.info("SessionServer up at " + serverSocket.getLocalSocketAddress());
  }

  private synchronized void initializeClusterManagerClient()
      throws IOException {
    // Connect to cluster manager thrift service
    String target = CoronaConf.getClusterManagerAddress(fConf);
    LOG.info("Connecting to Cluster Manager at " + target);
    InetSocketAddress address = NetUtils.createSocketAddr(target);
    transport = new TFramedTransport(
      new TSocket(address.getHostName(), address.getPort()));
    TProtocol protocol = new TBinaryProtocol(transport);
    client = new ClusterManagerService.Client(protocol);
    try {
      transport.open();
    } catch (TTransportException e) {
      throw new IOException(e);
    }
  }

  private synchronized void closeClusterManagerClient() {
    client = null;
    if (transport != null) {
      transport.close();
      transport = null;
    }
  }

  private synchronized void initializeCleanupThreads() {
    cjtLogCleanupThread.setDaemon(true);
    cjtLogCleanupThread.start();
  }

  class TServerThread extends Thread {
    TServer server;
    TServerThread(TServer server) {
      this.server = server;
    }
    @Override
    public void run() {
      server.serve();
    }
  }

  /**
   * The server retry loop.
   * This while-loop attempts to connect to the JobTracker.
   */
  @Override
  public void run() {
    try {
      startCleanupThreads();
      try {
        while (running && !shuttingDown) {
          try {
            heartbeatToClusterManager();
          } catch (IOException e) {
            LOG.error("Error initializing heartbeat to Cluster Manager", e);
            try {
              Thread.sleep(5000L);
            } catch (InterruptedException ie) {
            }
          }
          if (shuttingDown) {
            return;
          }
        }
      } finally {
        shutdown();
      }
    } catch (IOException iex) {
      LOG.error("Got fatal exception while initializing TaskTracker", iex);
      return;
    }
  }

  /**
   * Main service loop.  Will stay in this loop forever.
   */
  private void heartbeatToClusterManager() throws IOException {
    CoronaConf coronaConf = new CoronaConf(fConf);
    int numCpu = coronaConf.getInt("mapred.coronatasktracker.num.cpus",
      resourceCalculatorPlugin.getNumProcessors());
    if (numCpu == ResourceCalculatorPlugin.UNAVAILABLE) {
      numCpu = 1;
    }
    LOG.info("Will report " + numCpu + " CPUs");
    int totalMemoryMB = (int) (resourceCalculatorPlugin.getPhysicalMemorySize() / 1024D / 1024);
    ComputeSpecs total = new ComputeSpecs((short)numCpu);
    total.setNetworkMBps((short)100);
    total.setMemoryMB(totalMemoryMB);
    total.setDiskGB(
       (int)(getDiskSpace(false) / 1024D / 1024 / 1024));
    String appInfo = null;
    if (getLocalHostAddress() != null) {
      appInfo = getLocalHostAddress() + ":" + actionServerAddr.getPort();
    }
    else {
      appInfo = getLocalHostname() + ":" + actionServerAddr.getPort();
    }
    Map<ResourceType, String> resourceInfos =
        new EnumMap<ResourceType, String>(ResourceType.class);
    resourceInfos.put(ResourceType.MAP, appInfo);
    resourceInfos.put(ResourceType.REDUCE, appInfo);
    resourceInfos.put(ResourceType.JOBTRACKER, appInfo);

    while (running && !shuttingDown) {
      try {
        long now = System.currentTimeMillis();
        Thread.sleep(heartbeatCMInterval);

        float cpuUsage = resourceCalculatorPlugin.getCpuUsage();
        if (cpuUsage == ResourceCalculatorPlugin.UNAVAILABLE) {
          cpuUsage = 0;
        }
        ComputeSpecs free = new ComputeSpecs((short)(numCpu * cpuUsage / 100D));
        // TODO find free network.
        free.setNetworkMBps((short)100);
        int availableMemoryMB =
            (int)(resourceCalculatorPlugin.
                getAvailablePhysicalMemorySize() / 1024D / 1024);
        free.setMemoryMB(availableMemoryMB);
        long freeDiskSpace = getDiskSpace(true);
        long freeLogDiskSpace = getLogDiskFreeSpace();
        free.setDiskGB((int)(
          Math.min(freeDiskSpace, freeLogDiskSpace) / 1024D / 1024 / 1024));
        // TT puts it's MR specific host:port tuple here
        ClusterNodeInfo node = new ClusterNodeInfo
          (this.getName(), clusterManagerCallbackServerAddr, total);
        node.setFree(free);
        node.setResourceInfos(resourceInfos);

        LOG.debug("ClusterManager heartbeat: " + node.toString());
        if (client == null) {
          initializeClusterManagerClient();
        }
        NodeHeartbeatResponse nodeHeartbeatResponse =
          client.nodeHeartbeat(node);
        if (nodeHeartbeatResponse.restartFlag) {
          LOG.fatal("Get CM notice to exit");
          System.exit(0);
        }
        clusterManagerConnectRetries = 0;
        lastCMHeartbeat = System.currentTimeMillis();

        markUnresponsiveTasks();
        killOverflowingTasks();

        //we've cleaned up, resume normal operation
        if (!acceptNewTasks && isIdle()) {
          acceptNewTasks=true;
        }
        //The check below may not be required every iteration but we are
        //erring on the side of caution here. We have seen many cases where
        //the call to jetty's getLocalPort() returns different values at
        //different times. Being a real paranoid here.
        checkJettyPort();
      } catch (InterruptedException ie) {
        LOG.info("Interrupted. Closing down.");
        return;
      } catch (DisallowedNode ex) {
        LOG.error("CM has excluded node, shutting down TT");
        shutdown();
      } catch (SafeModeException e) {
        LOG.info("Cluster Manager is in Safe Mode");
        try {
          ClusterManagerAvailabilityChecker.
            waitWhileClusterManagerInSafeMode(coronaConf);
        } catch (IOException ie) {
          LOG.error("Could not wait while Cluster Manager is in Safe Mode ",
                    ie);
        }
      } catch (TException ex) {
        if (!shuttingDown) {
          LOG.error("Error connecting to CM. " + clusterManagerConnectRetries
            + "th retry. Retry in 10 seconds.", ex);
          closeClusterManagerClient();
          if (++clusterManagerConnectRetries >= MAX_CM_CONNECT_RETRIES) {
            LOG.error("Cannot connect to CM " + clusterManagerConnectRetries +
              " times. Shutting down TT");
            shutdown();
          }
          try {
            Thread.sleep(10000L);
          } catch (InterruptedException ie) {
          }
          ClusterManagerAvailabilityChecker.
            waitWhileClusterManagerInSafeMode(coronaConf);
        }
      }
    }
  }

  /**
   * Send heartbeats to a JobTracker to report task status
   */
  class JobTrackerReporter extends Thread {
    private static final long SLOW_HEARTBEAT_INTERVAL = 3 * 60 * 1000;
    private InetSocketAddress jobTrackerAddr;
    final InetSocketAddress secondaryTrackerAddr;
    final String sessionHandle;
    final RunningJob rJob;
    InterTrackerProtocol jobClient = null;
    boolean justInited = true;
    long lastJTHeartbeat = -1;
    long previousCounterUpdate = -1;
    long heartbeatJTInterval = 3000L;
    short heartbeatResponseId = -1;
    TaskTrackerStatus status = null;
    final String name;
    int errorCount = 0;
    // Can make configurable later, 10 is the count used for connection errors.
    final int maxErrorCount = 10;
    JobTrackerReporter(RunningJob rJob, InetSocketAddress jobTrackerAddr,
        InetSocketAddress secondaryTrackerAddr, String sessionHandle) {
      this.rJob = rJob;
      this.jobTrackerAddr = jobTrackerAddr;
      this.secondaryTrackerAddr = secondaryTrackerAddr;
      this.sessionHandle = sessionHandle;
      this.name = "JobTrackerReporter(" + rJob.getJobID() + ")";
    }
    volatile boolean shuttingDown = false;
    @Override
    public void run() {
      try {
        if (CoronaTaskTracker.this.running &&
            !CoronaTaskTracker.this.shuttingDown) {
          connect();
        }
        while (CoronaTaskTracker.this.running &&
            !CoronaTaskTracker.this.shuttingDown &&
            !this.shuttingDown) {
          long now = System.currentTimeMillis();
          synchronized (finishedCount) {
            if (finishedCount.get() == 0) {
              finishedCount.wait(heartbeatJTInterval);
            }
            finishedCount.set(0);
          }
          // If the reporter is just starting up, verify the buildVersion
          if(justInited) {
            String jobTrackerBV = jobClient.getBuildVersion();
            if(doCheckBuildVersion() &&
                !VersionInfo.getBuildVersion().equals(jobTrackerBV)) {
              String msg = name + " shutting down. Incompatible buildVersion." +
              "\nJobTracker's: " + jobTrackerBV +
              "\nTaskTracker's: "+ VersionInfo.getBuildVersion();
              LOG.error(msg);
              try {
                jobClient.reportTaskTrackerError(taskTrackerName, null, msg);
              } catch(Exception e ) {
                LOG.warn(name + " problem reporting to jobtracker: " + e);
              }
              shuttingDown = true;
              return;
            }
            justInited = false;
          }

          Collection<TaskInProgress> tipsInSession = new LinkedList<TaskInProgress>();
          boolean doHeartbeat = false;
          synchronized (CoronaTaskTracker.this) {
            for (TaskTracker.TaskInProgress tip : runningTasks.values()) {
              if (rJob.getJobID().equals(tip.getTask().getJobID())) {
                tipsInSession.add(tip);
              }
            }
            if (!tipsInSession.isEmpty() ||
                now - lastJTHeartbeat > SLOW_HEARTBEAT_INTERVAL) {
              doHeartbeat = true;
              // We need slow heartbeat to check if the JT is still alive
              boolean sendCounters = false;
              if (now > (previousCounterUpdate + COUNTER_UPDATE_INTERVAL)) {
                sendCounters = true;
                previousCounterUpdate = now;
              }
              status = updateTaskTrackerStatus(
                  sendCounters, null, tipsInSession, jobTrackerAddr);
            }
          }
          if (doHeartbeat) {
            // Send heartbeat only when there is at least one running tip in
            // this session, or we have reached the slow heartbeat interval.

            LOG.info(name + " heartbeat:" + jobTrackerAddr.toString() +
              " hearbeatId:" + heartbeatResponseId + " " + status.toString());

            try {
              HeartbeatResponse heartbeatResponse =
                  (new Caller<HeartbeatResponse>() {
                @Override
                protected HeartbeatResponse call() throws IOException {
                  return transmitHeartBeat(jobClient, heartbeatResponseId,
                      status);
                }
              }).makeCall();

              // The heartbeat got through successfully!
              // Reset error count after a successful heartbeat.
              errorCount = 0;
              heartbeatResponseId = heartbeatResponse.getResponseId();
              heartbeatJTInterval = heartbeatResponse.getHeartbeatInterval();
              // Note the time when the heartbeat returned, use this to decide when to send the
              // next heartbeat
              lastJTHeartbeat = System.currentTimeMillis();

              // resetting heartbeat interval from the response.
              justStarted = false;
            } catch (ConnectException e) {
              // JobTracker is dead. Purge the job.
              // Or it will timeout this task.
              // Treat the task as killed
              LOG.error(name + " connect error in reporting to " + jobTrackerAddr, e);
              throw e;
            } catch (IOException e) {
              handleIOException(e);
            }
          }
        }
      } catch (DiskErrorException de) {
        String msg = name + " exiting for disk error:\n" +
          StringUtils.stringifyException(de);
        LOG.error(msg);
        try {
          jobClient.reportTaskTrackerError(taskTrackerName,
              "DiskErrorException", msg);
        } catch (IOException exp) {
          LOG.error(name + " cannot report TaskTracker failure");
        }
      } catch (IOException e) {
        purgeSession(this.sessionHandle);
      } catch (InterruptedException e) {
        LOG.info(name + " interrupted");
      }
    }

    private void connect() throws IOException {
      try {
        LOG.info(name + " connecting to " + this.jobTrackerAddr);
        jobClient = RPC.waitForProtocolProxy(
            InterTrackerProtocol.class,
            InterTrackerProtocol.versionID,
            this.jobTrackerAddr,
            CoronaTaskTracker.this.fConf,
            jtConnectTimeoutMsec).getProxy();
        rJob.setJobClient(jobClient);
      } catch (IOException e) {
        LOG.error(name + " failed to connect to " + jobTrackerAddr, e);
        throw e;
      }
    }
    public void shutdown() {
      LOG.info(name + " shutting down");
      // shutdown RPC connections
      RPC.stopProxy(jobClient);
      shuttingDown = true;
    }
    
    private void handleIOException(IOException e) throws IOException {
      errorCount++;
      if (errorCount >= maxErrorCount) {
        LOG.error(name + " too many errors " + maxErrorCount +
          " in reporting to " + jobTrackerAddr, e);
        throw e;
      } else {
        long backoff = errorCount * heartbeatJTInterval;
        LOG.warn(
          name + " error " + errorCount + " in reporting to " + jobTrackerAddr +
          " will wait " + backoff + " msec", e);
        try {
          Thread.sleep(backoff);
        } catch (InterruptedException ie) {
        }
      }
    } 
    
    /**
     * Handles fallback process and connecting to new job tracker
     * @param <T> return type of called function
     */
    private abstract class Caller<T> extends CoronaJTFallbackCaller<T> {

      @Override
      protected void handleIOException(IOException e) throws IOException {
        JobTrackerReporter.this.handleIOException(e);
      }

      @Override
      protected void connect(InetSocketAddress address) throws IOException {
        JobTrackerReporter.this.jobTrackerAddr = address;
        JobTrackerReporter.this.connect();
      }

      @Override
      protected void shutdown() {
        RPC.stopProxy(JobTrackerReporter.this.jobClient);
      }

      @Override
      protected InetSocketAddress getCurrentClientAddress() {
        return JobTrackerReporter.this.jobTrackerAddr;
      }

      @Override
      protected JobConf getConf() {
        return CoronaTaskTracker.this.fConf;
      }

      @Override
      protected boolean predRetry(int retryNum) {
        return super.predRetry(retryNum)
            && (CoronaTaskTracker.this.running
                && !CoronaTaskTracker.this.shuttingDown
                && !JobTrackerReporter.this.shuttingDown);
      }

      @Override
      protected void waitRetry() throws InterruptedException {
        synchronized (finishedCount) {
          finishedCount.wait(heartbeatJTInterval);
        }
      }

      @Override
      protected InetSocketAddress getSecondaryTracker() {
        return JobTrackerReporter.this.secondaryTrackerAddr;
      }
    }
    
  }

  @Override
  public Boolean isAlive() {
    long timeSinceHeartbeat = System.currentTimeMillis() - lastCMHeartbeat;
    CoronaConf cConf = new CoronaConf(fConf);
    long expire = cConf.getNodeExpiryInterval();
    if (timeSinceHeartbeat > expire) {
      return false;
    }
    return true;
  }

  @Override
  public void submitActions(TaskTrackerAction[] actions) throws IOException,
      InterruptedException {
    if (actions != null){
      for(TaskTrackerAction action: actions) {
        CoronaSessionInfo info = (CoronaSessionInfo)(action.getExtensible());
        if (info == null ||
            info.getSessionHandle() == null ||
            info.getJobTrackerAddr() == null) {
          LOG.warn("Received a " + action + " from unkown JobTracker. Ignored.");
          continue;
        }
        if (blacklistedSessions.contains(info.getSessionHandle())) {
          LOG.warn("Received a " + action + " from blacklisted session " +
              info.getSessionHandle() + ". Ignored.");
          continue;
        }
        switch (action.getActionId()) {
        case LAUNCH_TASK:
          LaunchTaskAction launchAction = (LaunchTaskAction)action;
          LOG.info("Received launch task action for " +
              launchAction.getTask().getTaskID());
          addToTaskQueue(launchAction);
          break;
        case COMMIT_TASK:
          CommitTaskAction commitAction = (CommitTaskAction)action;
          if (!commitResponses.contains(commitAction.getTaskID())) {
            LOG.info("Received commit task action for " +
                commitAction.getTaskID());
            commitResponses.add(commitAction.getTaskID());
          }
          break;
        case KILL_JOB:
          JobID jobId = ((KillJobAction)action).getJobID();
          LOG.info("Received kill job action for " + jobId);
          List<TaskAttemptID> running = getRunningTasksForJob(jobId);
          for (TaskAttemptID attemptID : running) {
            removeRunningTask(attemptID);
          }
          tasksToCleanup.put(action);
          break;
        case KILL_TASK:
          LOG.info("Received kill task action for " +
              ((KillTaskAction)action).getTaskID());
          tasksToCleanup.put(action);
          break;
        case REINIT_TRACKER:
          LOG.error("Recieved unsupport RenitTrackerAction from " +
              info.getJobTrackerAddr() + " Ignored.");
        }
      }
    }
  }

  @SuppressWarnings("deprecation")
  @Override
  public void startCoronaJobTracker(Task jobTask, CoronaSessionInfo info)
      throws IOException {
    // The "client" should already have submitted the
    // job.xml file and the split file to the system directory.
    LOG.info("Processing startCoronaJobTracker request for "
        + jobTask.getJobID() + " from " + info.getJobTrackerAddr());
    TaskTracker.TaskInProgress tip = new TaskInProgress(jobTask, fConf, null,
        null);
    String releasePath = crReleaseManager.getRelease(jobTask.getJobID());
    String originalPath = crReleaseManager.getOriginal();
    CoronaJobTrackerRunner runner =
      new CoronaJobTrackerRunner(tip, jobTask, this, new JobConf(this.getJobConf()), info,
        originalPath, releasePath);
    runner.start();
  }

  void stopActionServer() {
    if (actionServer != null) {
      actionServer.stop();
      actionServer = null;
    }
  }

  @Override
  public synchronized void close() throws IOException {
    super.close();
    LOG.info(CoronaTaskTracker.class + " closed.");
    closeClusterManagerClient();
    stopActionServer();
    if (transport != null) {
      transport.close();
    }
    if (clusterManagerCallbackServerThread != null) {
      clusterManagerCallbackServerThread.interrupt();
    }
    if (clusterManagerCallbackServer != null) {
      clusterManagerCallbackServer.stop();
    }
    for (JobTrackerReporter reporter : jobTrackerReporters.values()) {
      reporter.shutdown();
    }
    if (crReleaseManager != null){
      crReleaseManager.shutdown();
    }
  }

  @Override
  protected int getMaxSlots(JobConf conf, int numCpuOnTT, TaskType type) {
    int ret = getMaxActualSlots(conf, numCpuOnTT, type);
    // Use a large value of slots if desired. This effectively removes slots as
    // a concept and lets the Cluster Manager manage the resources.
    return ret * slotMultiplier;
  }

  @Override
  int getMaxActualSlots(JobConf conf, int numCpuOnTT, TaskType type) {
    Map<Integer, Map<ResourceType, Integer>> cpuToResourcePartitioning =
      CoronaConf.getUncachedCpuToResourcePartitioning(conf);
    if (numCpuOnTT == ResourceCalculatorPlugin.UNAVAILABLE) {
      numCpuOnTT = 1;
    }
    Map<ResourceType, Integer> resourceTypeToCountMap =
      ClusterNode.getResourceTypeToCountMap(numCpuOnTT,
                                            cpuToResourcePartitioning);
    switch (type) {
    case MAP:
      return resourceTypeToCountMap.get(ResourceType.MAP);
    case REDUCE:
      return resourceTypeToCountMap.get(ResourceType.REDUCE);
    default:
      throw new RuntimeException("getMaxActualSlots: Illegal type " + type);
    }
  }

  /**
   * Override this method to create the proper jobClient and the thread that
   * sends jobTracker heartbeat.
   */
  @Override
  protected RunningJob createRunningJob(JobID jobId, TaskInProgress tip)
      throws IOException {
    CoronaSessionInfo info = (CoronaSessionInfo)(tip.getExtensible());
    // JobClient will be set by JobTrackerReporter thread later
    RunningJob rJob = new RunningJob(jobId, null, info);
    JobTrackerReporter reporter = new JobTrackerReporter(rJob,
        info.getJobTrackerAddr(), info.getSecondaryTracker(),
        info.getSessionHandle());
    reporter.setName("JobTrackerReporter for " + jobId);
    // Start the heartbeat to the jobtracker
    reporter.start();
    jobTrackerReporters.put(jobId, reporter);
    return rJob;
  }

  /**
   * Override this to shutdown the heartbeat the the corresponding jobtracker
   */
  @Override
  protected synchronized void purgeJob(KillJobAction action) throws IOException {
    JobID jobId = action.getJobID();
    JobTrackerReporter reporter = jobTrackerReporters.remove(jobId);
    if (reporter != null) {
      reporter.shutdown();
    }
    super.purgeJob(action);
    crReleaseManager.returnRelease(jobId);
  }

  @Override
  public long getProtocolVersion(String protocol,
                                 long clientVersion) throws IOException {
    if (protocol.equals(CoronaTaskTrackerProtocol.class.getName())) {
      return CoronaTaskTrackerProtocol.versionID;
    }
    return super.getProtocolVersion(protocol, clientVersion);
  }

  @Override
  public ProtocolSignature getProtocolSignature(String protocol,
      long clientVersion, int clientMethodsHash) throws IOException {
    return ProtocolSignature.getProtocolSignature(this, protocol,
        clientVersion, clientMethodsHash);
  }

  /**
   * Start the TaskTracker, point toward the indicated JobTracker
   */
  public static void main(String argv[]) throws Exception {
    StringUtils.startupShutdownMessage(CoronaTaskTracker.class, argv, LOG);
    if (argv.length != 0) {
      System.out.println("usage: CoronaTaskTracker");
      System.exit(-1);
    }
    JobConf conf=new JobConf();
    // enable the server to track time spent waiting on locks
    ReflectionUtils.setContentionTracing
      (conf.getBoolean("tasktracker.contention.tracking", false));
    try {
      new CoronaTaskTracker(conf).run();
    } catch (Throwable t) {
      LOG.fatal("Error running CoronaTaskTracker", t);
      System.exit(-2);
    }
  }

  @Override
  public void purgeSession(String handle) {
    synchronized (runningJobs) {
      for (TaskTracker.RunningJob job : this.runningJobs.values()) {
        CoronaSessionInfo info = (CoronaSessionInfo)(job.getExtensible());
        if (info.getSessionHandle().equals(handle)) {
          tasksToCleanup.add(new KillJobAction(job.getJobID()));
        }
      }
    }
  }

  @Override
  public void blacklistSession(String handle) throws InvalidSessionHandle,
      TException {
    blacklistedSessions.put(handle, handle);
  }

  @Override
  protected void reconfigureLocalJobConf(JobConf localJobConf,
      Path localJobFile, TaskInProgress tip, boolean changed)
      throws IOException {
    localJobConf.set(JobConf.TASK_RUNNER_CHILD_CLASS_CONF,
        CoronaChild.class.getName());
    CoronaSessionInfo info = (CoronaSessionInfo) (tip.getExtensible());
    InetSocketAddress directAddress = CoronaDirectTaskUmbilical.getAddress(
        localJobConf, CoronaDirectTaskUmbilical.DIRECT_UMBILICAL_JT_ADDRESS);
    if (directAddress == null
        || !directAddress.equals(info.getJobTrackerAddr())) {
      CoronaDirectTaskUmbilical.setAddress(localJobConf,
          CoronaDirectTaskUmbilical.DIRECT_UMBILICAL_JT_ADDRESS,
          info.getJobTrackerAddr());
      CoronaDirectTaskUmbilical.setAddress(localJobConf,
          CoronaDirectTaskUmbilical.DIRECT_UMBILICAL_FALLBACK_ADDRESS,
          info.getSecondaryTracker());
      changed = true;
    }
    super.reconfigureLocalJobConf(localJobConf, localJobFile, tip, changed);
  }

  @Override
  protected TaskUmbilicalProtocol getUmbilical(TaskInProgress tip)
    throws IOException {
    CoronaSessionInfo info = (CoronaSessionInfo)(tip.getExtensible());
    if (info != null) {
      return CoronaDirectTaskUmbilical.createDirectUmbilical(
        this, info.getJobTrackerAddr(), info.getSecondaryTracker(), fConf);
    }
    return this;
  }

  @Override
  protected void cleanupUmbilical(TaskUmbilicalProtocol t) {
    if (t instanceof CoronaDirectTaskUmbilical) {
      ((CoronaDirectTaskUmbilical) t).close();
    }
  }

  public static String jobTrackerLogDir() {
    return new File(
      System.getProperty("hadoop.log.dir"), "jtlogs").getAbsolutePath();
  }
}
