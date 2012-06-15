package org.apache.hadoop.mapred;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.util.Collection;
import java.util.LinkedList;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.corona.ClusterManagerService;
import org.apache.hadoop.corona.ClusterNodeInfo;
import org.apache.hadoop.corona.ComputeSpecs;
import org.apache.hadoop.corona.CoronaConf;
import org.apache.hadoop.corona.CoronaTaskTrackerService;
import org.apache.hadoop.corona.InetAddress;
import org.apache.hadoop.corona.InvalidSessionHandle;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RPC.Server;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.net.NetUtils;
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
  public static final int SLOT_MULTIPLIER = 10;
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
  private final long clusterHeartbeatInterval;
  Server actionServer;
  ConcurrentHashMap<JobID, JobTrackerReporter> jobTrackerReporters;
  long jtConnectTimeoutMsec = 0;
  private int clusterManagerConnectRetries;

  public CoronaTaskTracker(JobConf conf) throws IOException {
    initHttpServer(conf);
    super.initialize(conf);
    initializeTaskActionServer();
    initializeClusterManagerCallbackServer();
    initializeClusterManagerClient();
    clusterHeartbeatInterval = conf.getLong(HEART_BEAT_INTERVAL_KEY, 3000L);
    jtConnectTimeoutMsec = conf.getLong(JT_CONNECT_TIMEOUT_MSEC_KEY, 60000L);
  }

  private String getLocalHostname() throws IOException {
    String localHostname;
    if (fConf.get("slave.host.name") != null) {
      localHostname = fConf.get("slave.host.name");
    } else {
      localHostname =
          java.net.InetAddress.getLocalHost().getCanonicalHostName();
    }
    return localHostname;
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
    InetSocketAddress address = NetUtils.createSocketAddr(target);
    transport = new TSocket(address.getHostName(), address.getPort());
    TProtocol protocol = new TBinaryProtocol(transport);
    client = new ClusterManagerService.Client(protocol);
    try {
      transport.open();
    } catch (TTransportException e) {
      throw new IOException(e);
    }
  }

  class TServerThread extends Thread {
    TServer server;
    TServerThread(TServer server) {
      this.server = server;
    }
    public void run() {
      while (running) {
        try {
          server.serve();
        } catch (Exception e) {
          LOG.error("Caught exception " + e);
        }
      }
    }
  }

  /**
   * The server retry loop.
   * This while-loop attempts to connect to the JobTracker.
   */
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
    long lastHeartbeat = 0;

    int numCpu = resourceCalculatorPlugin.getNumProcessors();
    if (numCpu == ResourceCalculatorPlugin.UNAVAILABLE) {
      numCpu = 1;
    }
    ComputeSpecs total = new ComputeSpecs((short)numCpu);
    total.setNetworkMBps((short)100);
    total.setMemoryMB(
      (int)(resourceCalculatorPlugin.getPhysicalMemorySize() / 1024D / 1024));
    total.setDiskGB(
       (int)(getDiskSpace(false) / 1024D / 1024 / 1024));
    String appInfo = actionServerAddr.getHostName() + ":" +
       actionServerAddr.getPort();

    while (running && !shuttingDown) {
      try {
        long now = System.currentTimeMillis();

        long waitTime = clusterHeartbeatInterval - (now - lastHeartbeat);
        if (waitTime > 0) {
          // sleeps for the wait time or 
          // until there are empty slots to schedule tasks
          synchronized (finishedCount) {
            if (finishedCount.get() == 0) {
              finishedCount.wait(waitTime);
            }
            finishedCount.set(0);
          }
        }

        float cpuUsage = resourceCalculatorPlugin.getCpuUsage();
        if (cpuUsage == ResourceCalculatorPlugin.UNAVAILABLE) {
          cpuUsage = 0;
        }
        ComputeSpecs used = new ComputeSpecs((short)(numCpu * cpuUsage / 100D));
        used.setNetworkMBps((short)10);
        used.setMemoryMB(
            (int)(resourceCalculatorPlugin.
                  getAvailablePhysicalMemorySize() / 1024D / 1024));
        used.setDiskGB(
            (int)(getDiskSpace(true) / 1024D / 1024 / 1024));
        // TT puts it's MR specific host:port tuple here
        ClusterNodeInfo node = new ClusterNodeInfo
          (this.getName(), clusterManagerCallbackServerAddr, total);
        node.setUsed(used);
        node.setAppInfo(appInfo);

        LOG.info("ClusterManager heartbeat: " + node.toString());
        client.nodeHeartbeat(node);
        clusterManagerConnectRetries = 0;
        lastHeartbeat = System.currentTimeMillis();

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
      } catch (TException ex) {
        if (!shuttingDown) {
          LOG.error("Error connecting to CM. " + clusterManagerConnectRetries
              + "th retry. Retry in 10 seconds.", ex);
          if (++clusterManagerConnectRetries >= MAX_CM_CONNECT_RETRIES) {
            LOG.error("Cannot connect to CM " + clusterManagerConnectRetries +
                " times. Shutting down TT");
            shutdown();
          }
          try {
            Thread.sleep(10000L);
          } catch (InterruptedException ie) {
          }
        }
      }
    }
  }

  /**
   * Send heartbeats to a JobTracker to report task status
   */
  class JobTrackerReporter extends Thread {
    private static final long SLOW_HEARTBEAT_INTERVAL = 3 * 60 * 1000;
    final InetSocketAddress jobTrackerAddr;
    final String sessionHandle;
    final RunningJob rJob;
    InterTrackerProtocol jobClient = null;
    boolean justInited = true;
    long lastHeartbeat = -1;
    long previousCounterUpdate = -1;
    long heartbeatInterval = 3000L;
    short heartbeatResponseId = -1;
    TaskTrackerStatus status = null;
    JobTrackerReporter(RunningJob rJob, InetSocketAddress jobTrackerAddr,
        String sessionHandle) {
      this.rJob = rJob;
      this.jobTrackerAddr = jobTrackerAddr;
      this.sessionHandle = sessionHandle;
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
          long waitTime = heartbeatInterval - (now - lastHeartbeat);
          if (waitTime > 0) {
            // sleeps for the wait time or 
            // until there are empty slots to schedule tasks
            synchronized (finishedCount) {
              if (finishedCount.get() == 0) {
                finishedCount.wait(waitTime);
              }
              finishedCount.set(0);
            }
          }
          // If the reporter is just starting up, verify the buildVersion
          if(justInited) {
            String jobTrackerBV = jobClient.getBuildVersion();
            if(doCheckBuildVersion() &&
                !VersionInfo.getBuildVersion().equals(jobTrackerBV)) {
              String msg = "Shutting down. Incompatible buildVersion." +
              "\nJobTracker's: " + jobTrackerBV + 
              "\nTaskTracker's: "+ VersionInfo.getBuildVersion();
              LOG.error(msg);
              try {
                jobClient.reportTaskTrackerError(taskTrackerName, null, msg);
              } catch(Exception e ) {
                LOG.info("Problem reporting to jobtracker: " + e);
              }
              shuttingDown = true;
              return;
            }
          }

          Collection<TaskInProgress> tipsInSession = new LinkedList<TaskInProgress>();
          synchronized (CoronaTaskTracker.this) {
            for (TaskTracker.TaskInProgress tip : runningTasks.values()) {
              CoronaSessionInfo info = (CoronaSessionInfo)(tip.getExtensible());
              if (info.getSessionHandle().equals(sessionHandle)) {
                tipsInSession.add(tip);
              }
            }
            if (!tipsInSession.isEmpty() ||
                now - lastHeartbeat > SLOW_HEARTBEAT_INTERVAL) {
              // We need slow heartbeat to check if the JT is still alive
              boolean sendCounters = false;
              if (now > (previousCounterUpdate + COUNTER_UPDATE_INTERVAL)) {
                sendCounters = true;
                previousCounterUpdate = now;
              }
              status = updateTaskTrackerStatus(
                  sendCounters, status, tipsInSession, jobTrackerAddr);
            }
          }
          if (!tipsInSession.isEmpty()) {
            // Send heartbeat only when there is at least one running tip in
            // this session

            LOG.info("JobTracker heartbeat:" + jobTrackerAddr.toString() +
                " hearbeatId:" + heartbeatResponseId + " " + status.toString());

            HeartbeatResponse heartbeatResponse = transmitHeartBeat(
                jobClient, heartbeatResponseId, status);

            // The heartbeat got through successfully!
            // Force a rebuild of 'status' on the next iteration
            status = null;
            heartbeatResponseId = heartbeatResponse.getResponseId();
            heartbeatInterval = heartbeatResponse.getHeartbeatInterval();
          }

          // Note the time when the heartbeat returned, use this to decide when to send the
          // next heartbeat   
          lastHeartbeat = System.currentTimeMillis();

          // resetting heartbeat interval from the response.
          justStarted = false;
          justInited = false;

        }
      } catch (DiskErrorException de) {
        String msg = "Exiting task tracker for disk error:\n" +
          StringUtils.stringifyException(de);
        LOG.error(msg);
        try {
          jobClient.reportTaskTrackerError(taskTrackerName, 
              "DiskErrorException", msg);
        } catch (Exception exp) {
          LOG.error("Cannot report TaskTracker failure");
        }
      } catch (IOException e) {
        LOG.error("Error report to JobTracker:" + jobTrackerAddr +
            " sessionHandle:" + sessionHandle, e);
        // JobTracker is dead. Purge the job.
        try {
          purgeSession(this.sessionHandle);
        } catch (Exception exp) {
          // should not happen
          LOG.error("Purge session failure");
        }
      } catch (InterruptedException e) {
        LOG.info("JobTrackerReporter interrupted");
      } catch (Exception except) {
        LOG.error("JobTrackerReporter exist", except);
      }
    }
    private void connect() throws IOException {
      try {
        jobClient = (InterTrackerProtocol) RPC.waitForProtocolProxy(
            InterTrackerProtocol.class,
            InterTrackerProtocol.versionID,
            this.jobTrackerAddr,
            CoronaTaskTracker.this.fConf,
            jtConnectTimeoutMsec).getProxy();
        rJob.setJobClient(jobClient);
      } catch (IOException e) {
        LOG.error("Failed to connect to JobTracker:" +
            jobTrackerAddr + " sessionHandle:" + sessionHandle, e);
        throw e;
      }
    }
    public void shutdown() {
      LOG.info("Shutting down reporter to JobTracker " + this.jobTrackerAddr);
      // shutdown RPC connections
      RPC.stopProxy(jobClient);
      shuttingDown = true;
    }
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
          LOG.info("Received kill job action for " +
              ((KillJobAction)action).getJobID());
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

  @Override
  public synchronized void close() throws IOException {
    super.close();
    LOG.info(CoronaTaskTracker.class + " closed.");
    if (actionServer != null) {
      actionServer.stop();
      actionServer = null;
    }
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
  }

  @Override
  protected int getMaxSlots(JobConf conf, int numCpuOnTT, TaskType type) {
    int ret = super.getMaxSlots(conf, numCpuOnTT, type);
    // Use a large value of slots. This effectively removes slots as a concept
    // and lets the Cluster Manager manage the resources.
    return ret * SLOT_MULTIPLIER;
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
    JobTrackerReporter reporter = new JobTrackerReporter(
        rJob, info.getJobTrackerAddr(), info.getSessionHandle());
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
  }

  @Override
  public long getProtocolVersion(String protocol,
                                 long clientVersion) throws IOException {
    if (protocol.equals(CoronaTaskTrackerProtocol.class.getName())) {
      return CoronaTaskTrackerProtocol.versionID;
    }
    return super.getProtocolVersion(protocol, clientVersion);
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
    try {
      JobConf conf=new JobConf();
      // enable the server to track time spent waiting on locks
      ReflectionUtils.setContentionTracing
        (conf.getBoolean("tasktracker.contention.tracking", false));
      new CoronaTaskTracker(conf).run();
    } catch (Throwable e) {
      LOG.error("Can not start corona task tracker because "+
                StringUtils.stringifyException(e));
      System.exit(-1);
    }
  }

  @Override
  public void purgeSession(String handle) throws InvalidSessionHandle,
      TException {
    for (TaskTracker.RunningJob job : this.runningJobs.values()) {
      CoronaSessionInfo info = (CoronaSessionInfo)(job.getExtensible());
      if (info.getSessionHandle().equals(handle)) {
        tasksToCleanup.add(new KillJobAction(job.getJobID()));
      }
    }
  }

  @Override
  public void blacklistSession(String handle) throws InvalidSessionHandle,
      TException {
    blacklistedSessions.put(handle, handle);
  }

  @Override
  protected void reconfigureLocalJobConf(
      JobConf localJobConf, Path localJobFile, TaskInProgress tip, boolean changed)
      throws IOException {
    CoronaSessionInfo info = (CoronaSessionInfo)(tip.getExtensible());
    localJobConf.set(DirectTaskUmbilical.MAPRED_DIRECT_TASK_UMBILICAL_ADDRESS,
        info.getJobTrackerAddr().getHostName() + ":" + info.getJobTrackerAddr().getPort());
    super.reconfigureLocalJobConf(localJobConf, localJobFile, tip, true);
  }
}
