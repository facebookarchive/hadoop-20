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
package org.apache.hadoop.mapred;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.corona.InetAddress;
import org.apache.hadoop.corona.CoronaConf;
import org.apache.hadoop.corona.ResourceGrant;
import org.apache.hadoop.corona.ResourceRequest;
import org.apache.hadoop.corona.ResourceType;
import org.apache.hadoop.corona.SessionDriver;
import org.apache.hadoop.corona.SessionDriverService;
import org.apache.hadoop.corona.SessionPriority;
import org.apache.hadoop.corona.SessionStatus;
import org.apache.hadoop.corona.Utilities;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.http.HttpServer;
import org.apache.hadoop.ipc.ProtocolSignature;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.VersionInfo;

/**
 * The Corona Job Tracker (CJT) can work in one of three modes
 * - In-process: In this mode, the CJT performs its entire functionality in
 * the same process as the JobClient
 * - Forwarding: In this case, the CJT just forwards the calls to a remote CJT.
 * - Standalone: This is the remote CJT that is serving the calls from the
 * forwarding CJT.
 * The CoronaJobTracker (CJT) is responsible for running a single map-reduce
 * job in Corona. It is similar to the classic Map-Reduce JobTracker (JT) class,
 * except that it deals with only one job. Unlike the JT, the CJT does not
 * track/manage the nodes that run the map/reduce tasks. The CJT gets all that
 * functionality from the ClusterManager (CM). It communicates the resource
 * needs of its job to the CM, and uses the resources provided by the CM to
 * launch tasks.
 * <p/>
 * Cluster Resource Flow in CJT
 * <p/>
 * When the CJT starts, it obtains a session ID in the constructor.
 * This session ID is used to derive the job ID and that does not change during
 * the lifetime of the CJT. When the job is started through
 * JobSubmissionProtocol#submitJob call, the resource flow is started. First the
 * job is initialized through CoronaJobInProgress#initTasks(). Then
 * CoronaJobTracker#startJob() does the work to create the initial set of
 * resource requests to be sent to the CM. CoronaJobTracker#updateResources is
 * responsible for actually sending the resource requests to the CM, and it is
 * invoked periodically to update the CM with requested and released resources.
 * <p/>
 * Apart from the initial set of resource requests, the CJT may send additional
 * resource requests. This is needed to run speculative task attempts and to
 * re-run task attempts that have failed and need to be run on a different
 * machine. In these cases, the machine that ran the original attempt is
 * specified as an excluded host in the resource request.
 * <p/>
 * The process of releasing resources back to the CM is a little involved.
 * The resources given by the CM to the CJT are for the CJT to use for as long
 * as needed, except if the resource is revoked by the CM through
 * SessionDriverService#Iface#revokeResource. So once a task is finished on the
 * granted machine, the CJT is allowed to reuse the machine to run other tasks.
 * The decision of reusing a resource vs not is done in
 * CoronaJobTracker#processTaskResource, which does the following:
 * - if the task succeeded: reuse the resource if possible, otherwise release it
 * - if the task failed: get a new request for running the task, and mark the
 *   resource as bad so that it can be excluded from future requests.
 * <p/>
 * When the job finishes, the resources active at that point are not explicitly
 * returned the CM, instead, a session-end notification is sent to the CM which
 * effectively releases the resources for the job. Also a job end notification
 * is sent to the task trackers that ran tasks, so that they can clean up their
 * state (see CoronaJobTracker#close)
 */
@SuppressWarnings("deprecation")
public class CoronaJobTracker
  extends JobTrackerTraits
  implements JobSubmissionProtocol, SessionDriverService.Iface,
 InterTrackerProtocol,
             ResourceTracker.ResourceProcessor, TaskStateChangeListener {
  public static final int UNHANDLED_EXCEPTION_KILLED_CJT = 1;
  public static final int HEARTBEAT_THREAD_KILLED_CJT = 2;
  static {
    Thread.setDefaultUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler() {
      @Override
      public void uncaughtException(Thread t, Throwable e) {
        LOG.error("UNCAUGHT: Thread " + t.getName() + " got an uncaught exception", e);
        System.exit(UNHANDLED_EXCEPTION_KILLED_CJT);
      }
    });
  }

  public static final String STANDALONE_CJT_THRESHOLD_CONF = "mapred.coronajobtracker.remote.threshold";
  public static final int STANDALONE_CJT_THRESHOLD_DEFAULT = 1000;
  public static final int MAX_REQUESTS_PER_TASK = 1 + TaskInProgress.MAX_TASK_EXECS;

  public static final Log LOG = LogFactory.getLog(CoronaJobTracker.class);

  public static final String TT_CONNECT_TIMEOUT_MSEC_KEY = "corona.tasktracker.connect.timeout.msec";
  public static final String HEART_BEAT_INTERVAL_KEY = "corona.jobtracker.heartbeat.interval";

  JobConf conf; // JT conf.
  FileSystem fs;

  final boolean isStandalone;
  volatile RemoteJTProxy remoteJT;

  // Handles the session with the cluster manager.
  SessionDriver sessionDriver;
  String sessionId;
  String sessionLogPath;
  SessionStatus sessionEndStatus = null;

  // Variable to atomically check if more than one job is attempted to be
  // launched via this jobtracker
  AtomicInteger jobCounter = new AtomicInteger();

  // Identifier for the current job.
  final JobID jobId;
  CoronaJobInProgress job;

  ResourceTracker resourceTracker;
  List<ResourceGrant> grantsToRevoke = new ArrayList<ResourceGrant>();
  List<String> deadNodes = new ArrayList<String>();

  volatile boolean running = true;
  volatile boolean closed = false;
  Thread assignTasksThread;

  InetSocketAddress jobTrackerAddress;
  volatile Server interTrackerServer;
  HttpServer infoServer;
  long startTime = System.currentTimeMillis();

  TaskLookupTable taskLookupTable = new TaskLookupTable();

  Map<String, TaskTrackerStatus> taskTrackerStatus =
    new ConcurrentHashMap<String, TaskTrackerStatus>();

  final TrackerStats trackerStats;

  static class TaskContext {
    List<ResourceRequest> resourceRequests;
    Set<String> excludedHosts;

    TaskContext(ResourceRequest req) {
      resourceRequests = new ArrayList<ResourceRequest>();
      resourceRequests.add(req);
      excludedHosts = new HashSet<String>();
    }
  }

  // This provides information about the resource needs of each task (TIP).
  HashMap<TaskInProgress, TaskContext> taskToContextMap =
    new HashMap<TaskInProgress, TaskContext>();
  // Maintains the inverse of taskToContextMap.
  HashMap<Integer, TaskInProgress> requestToTipMap =
    new HashMap<Integer, TaskInProgress>();

  // Keeping track of the speculated TIPs
  HashSet<TaskInProgress> speculatedMaps = new HashSet<TaskInProgress>();
  HashSet<TaskInProgress> speculatedReduces = new HashSet<TaskInProgress>();

  /** The task launch expiry logic. */
  private ExpireLaunchingTasks expireLaunchingTasks;

  /**
   * An Attempt and it's corresponding TaskInProgress
   * There is a unique TIP per Attempt. Hence the attempt
   * can be used as the unique key to identify this tuple
   * (in a Collection for example)
   */
  public static final class TaskAttemptIDWithTip
    implements Comparable<TaskAttemptIDWithTip> {
    public final TaskAttemptID attemptId;
    public final TaskInProgress tip;

    public TaskAttemptIDWithTip(TaskAttemptID attemptId, TaskInProgress tip) {
      this.attemptId = attemptId;
      this.tip = tip;
    }

    @Override
    public boolean equals(Object o) {
      TaskAttemptIDWithTip that = (TaskAttemptIDWithTip)o;
      return this.attemptId.equals(that.attemptId);
    }

    @Override
    public int hashCode() {
      return attemptId.hashCode();
    }

    @Override
    public int compareTo(TaskAttemptIDWithTip that) {
      return this.attemptId.compareTo(that.attemptId);
    }
  }

  class TaskLookupTable {
    Map<TaskAttemptID, String> taskIdToTrackerMap =
      new HashMap<TaskAttemptID, String>();
    Map<TaskAttemptID, TaskInProgress> taskIdToTIPMap =
      new HashMap<TaskAttemptID, TaskInProgress>();
    Map<String, Set<TaskAttemptIDWithTip>> trackerToTaskMap =
      new HashMap<String, Set<TaskAttemptIDWithTip>>();
    Map<String, Set<TaskAttemptID>> trackerToSucessfulTaskMap =
      new HashMap<String, Set<TaskAttemptID>>();
    Map<TaskAttemptID, Integer> taskIdToGrantMap =
      new HashMap<TaskAttemptID, Integer>();

    public void createTaskEntry(
        TaskAttemptID taskId, String taskTracker, TaskInProgress tip,
        Integer grant) {
      LOG.info("Adding task (" + tip.getAttemptType(taskId) + ") " +
        "'"  + taskId + "' to tip " +
        tip.getTIPId() + ", for tracker '" + taskTracker + "' grant:" + grant);

      synchronized(lockObject) {
        // taskId --> tracker
        taskIdToTrackerMap.put(taskId, taskTracker);

        // tracker --> taskId
        Set<TaskAttemptIDWithTip> taskset = trackerToTaskMap.get(taskTracker);
        if (taskset == null) {
          taskset = new HashSet<TaskAttemptIDWithTip>();
          trackerToTaskMap.put(taskTracker, taskset);
        }
        taskset.add(new TaskAttemptIDWithTip(taskId, tip));
        // taskId --> TIP
        // We never remove this entry.
        taskIdToTIPMap.put(taskId, tip);

        taskIdToGrantMap.put(taskId, grant);
      }
    }

    public List<TaskAttemptID> getSuccessfulTasksForNode(String node) {
      List<TaskAttemptID> attempts = new ArrayList<TaskAttemptID>();
      synchronized (lockObject) {
        Set<TaskAttemptID> set = trackerToSucessfulTaskMap.get(node);
        if (set != null) {
          attempts.addAll(set);
        }
      }
      return attempts;
    }
    
    public void addSuccessfulTaskEntry(TaskAttemptID taskId, String node) {
      synchronized (lockObject) {
        Set<TaskAttemptID> attempts = trackerToSucessfulTaskMap.get(node);
        if (attempts == null) {
          attempts = new HashSet<TaskAttemptID>();
          trackerToSucessfulTaskMap.put(node, attempts);
        }

        attempts.add(taskId);
      }
    }

    public void removeSuccessfulTaskEntry(TaskAttemptID taskId, String node) {
      synchronized (lockObject) {
        Set<TaskAttemptID> attempts = trackerToSucessfulTaskMap.get(node);
        if (attempts != null) {
          attempts.remove(taskId);
        }
      }
    }

    public void removeTaskEntry(TaskAttemptID taskId) {
      LOG.info("Removing task '" + taskId + "'");
      synchronized(lockObject) {
        // taskId --> tracker
        String tracker = taskIdToTrackerMap.get(taskId);

        // tracker --> taskId
        if (tracker != null) {
          Set<TaskAttemptIDWithTip> taskset = trackerToTaskMap.get(tracker);
          if (taskset != null) {
            // TaskAttemptIDWithTip.equals() uses attemptId equality.
            taskset.remove(new TaskAttemptIDWithTip(taskId, null));
          }
        }

        taskIdToGrantMap.remove(taskId);
      }
    }

    public TaskInProgress getTIP(TaskAttemptID taskId) {
      synchronized(lockObject) {
        return taskIdToTIPMap.get(taskId);
      }
    }

    public TaskAttemptID taskForGrant(ResourceGrant grant) {
      return taskForGrantId(grant.getId());
    }

    public TaskAttemptID taskForGrantId(Integer grantId) {
      synchronized(lockObject) {
        for (Map.Entry<TaskAttemptID, Integer> entry: taskIdToGrantMap.entrySet()) {
          if (entry.getValue().equals(grantId)) {
            return entry.getKey();
          }
        }
      }
      return null;
    }
    
    public Set<Integer> grantsInUseOnTracker(
          String trackerName) {
      synchronized(lockObject) {
        Set<Integer> grants = new HashSet<Integer>();
        for (TaskAttemptIDWithTip tip : trackerToTaskMap.get(trackerName)) {
          grants.add(taskIdToGrantMap.get(tip.attemptId));
        }
        return grants;
      }
    }

    List<KillTaskAction> getTasksToKill(String taskTracker) {
      synchronized(lockObject) {
        Set<TaskAttemptIDWithTip> taskset = trackerToTaskMap.get(taskTracker);
        List<KillTaskAction> killList = new ArrayList<KillTaskAction>();
        if (taskset != null) {
          for (TaskAttemptIDWithTip onetask : taskset) {
            TaskAttemptID killTaskId = onetask.attemptId;
            TaskInProgress tip = onetask.tip;

            if (tip == null) {
              continue;
            }
            if (tip.shouldClose(killTaskId)) {
              //
              // This is how the JobTracker ends a task at the TaskTracker.
              // It may be successfully completed, or may be killed in
              // mid-execution.
              //
              if (job != null && !job.getStatus().isJobComplete()) {
                killList.add(new KillTaskAction(killTaskId));
                LOG.debug(taskTracker + " -> KillTaskAction: " + killTaskId);
              }
            }
          }
        }
        return killList;
      }
    }

    public Integer getGrantIdForTask(TaskAttemptID taskId) {
      synchronized(lockObject) {
        return taskIdToGrantMap.get(taskId);
      }
    }

    public String getAssignedTracker(TaskAttemptID attempt) {
      synchronized(lockObject) {
        return taskIdToTrackerMap.get(attempt);
      }
    }
  }

  private void failTask(TaskAttemptID taskId, String reason,
      boolean isFailed) {
    TaskInProgress tip = taskLookupTable.getTIP(taskId);
    Integer grantId = taskLookupTable.getGrantIdForTask(taskId);
    ResourceGrant grant = resourceTracker.getGrant(grantId);
    synchronized (lockObject) {
      if (!tip.isAttemptRunning(taskId)) {
        /*
         * This attempt is not running so we should not be killing/failing it
         * The reason we might try to fail the task that is not running is if it
         * has finished and was preempted at the same time.
         */
        return;
      }
    }
    assert grant != null : "Task " + taskId +
      " is running but has no associated resource";
    String trackerName = grant.getNodeName();
    TaskTrackerStatus trackerStatus =
      getTaskTrackerStatus(trackerName);

    TaskStatus.Phase phase =
      tip.isMapTask()? TaskStatus.Phase.MAP:
        TaskStatus.Phase.STARTING;
    CoronaJobTracker.this.job.failedTask(
        tip, taskId,reason, phase,
        isFailed, trackerName, trackerStatus);
  }

  TrackerClientCache trackerClientCache;

  ResourceUpdater resourceUpdater = new ResourceUpdater();
  Thread resourceUpdaterThread;

  private int infoPort;

  private final Object lockObject = new Object();
  private final Object closeLock = new Object();

  CoronaJobHistory jobHistory;

  private final int heartbeatInterval;

  private volatile boolean fullTrackerStarted = false;
  private CoronaTaskLauncher taskLauncher;

  // For testing.
  CoronaJobTracker(JobConf conf, String sessionId,
      TrackerClientCache cache) throws IOException {
    this.isStandalone = false;
    this.conf = conf;
    this.trackerStats = new TrackerStats(conf);
    this.fs = FileSystem.get(conf);
    this.sessionId = sessionId;
    this.trackerClientCache = cache;
    LOG.info("ResourceTracker initialized");
    this.resourceTracker = new ResourceTracker(lockObject);
    this.taskLookupTable = new TaskLookupTable();
    this.jobId = new JobID(sessionId, 1);
    this.jobHistory = new CoronaJobHistory(conf, jobId, null);
    this.heartbeatInterval = conf.getInt(HEART_BEAT_INTERVAL_KEY, 100);
    this.remoteJT = null;
  }

  public CoronaJobTracker(JobConf conf, JobID jobId,
      TaskAttemptID attemptId, InetSocketAddress parentAddr) throws IOException {
    this.isStandalone = true;
    this.heartbeatInterval = conf.getInt(HEART_BEAT_INTERVAL_KEY, 3000);
    this.remoteJT = null;
    // This is already a standalone (remote) CJT, unset the flag.
    conf.setBoolean("mapred.coronajobtracker.forceremote", false);
    this.conf = conf;
    this.trackerStats = new TrackerStats(conf);
    this.fs = FileSystem.get(conf);
    this.jobId = jobId;

    startSession();
    startFullTracker();

    // In remote mode, we have a parent JT that we need to communicate with.
    ParentHeartbeat parentHeartbeat = new ParentHeartbeat(
      conf, attemptId, jobTrackerAddress, parentAddr, sessionId);
    try {
      // Perform an initial heartbeat to confirm that we can go ahead.
      // If this throws an exception, the rest of the threads are daemon
      // threads, so the stand-alone CJT will exit.
      parentHeartbeat.initialHeartbeat();
      // Start the thread to do periodic heartbeats.
      // This thread is not a daemon thread, so the process will hang around
      // while it is alive.
      Thread parentHeartbeatThread = new Thread(parentHeartbeat);
      parentHeartbeatThread.setDaemon(false);
      parentHeartbeatThread.setName("Parent Heartbeat");
      parentHeartbeatThread.start();
    } catch (IOException e) {
      LOG.error("Closing CJT after initial heartbeat error" , e);
      close(false);
    }
  }

  public CoronaJobTracker(JobConf conf) throws IOException {
    this.isStandalone = false;
    this.heartbeatInterval = conf.getInt(HEART_BEAT_INTERVAL_KEY, 3000);
    this.conf = conf;
    this.trackerStats = new TrackerStats(conf);
    this.fs = FileSystem.get(conf);

    startSession();

    // the jobtracker can run only a single job. it's jobid is fixed based
    // on the sessionId.
    this.jobId = new JobID(sessionId, 1);
  }

  private void startSession() throws IOException {
    // Create the session driver. This will contact the cluster manager.
    sessionDriver = new SessionDriver(conf, this);
    sessionId = sessionDriver.getSessionId();
    sessionLogPath = sessionDriver.getSessionLog();
  }

  private void startFullTracker() throws IOException {
    if (fullTrackerStarted) {
      return;
    }
    this.resourceTracker = new ResourceTracker(lockObject);
    this.trackerClientCache = new TrackerClientCache(conf);

    startRPCServer(this);
    startInfoServer();

    this.taskLookupTable = new TaskLookupTable();

    assignTasksThread = new Thread(new AssignTasksThread());
    assignTasksThread.setName("assignTasks Thread");
    assignTasksThread.setDaemon(true);
    assignTasksThread.start();

    resourceUpdaterThread = new Thread(resourceUpdater);
    resourceUpdaterThread.setName("Resource Updater");
    resourceUpdaterThread.setDaemon(true);
    resourceUpdaterThread.start();

    expireLaunchingTasks = new ExpireLaunchingTasks(lockObject, this);
    expireLaunchingTasks.setName("Expire launching tasks");
    expireLaunchingTasks.setDaemon(true);
    expireLaunchingTasks.start();

    taskLauncher = new CoronaTaskLauncher(conf, this, expireLaunchingTasks);

    jobHistory = new CoronaJobHistory(conf, jobId, sessionLogPath);

    // Initialize history DONE folder
    if (!jobHistory.isDisabled()) {
      String historyLogDir =
        jobHistory.getCompletedJobHistoryLocation().toString();
      infoServer.setAttribute("historyLogDir", historyLogDir);
      infoServer.setAttribute("conf", conf);
    }

    fullTrackerStarted = true;
  }

  private void startRestrictedTracker(JobID jobId, JobConf jobConf)
      throws IOException {
    this.resourceTracker = new ResourceTracker(lockObject);
    this.trackerClientCache = new TrackerClientCache(conf);
    remoteJT = new RemoteJTProxy(this, jobId, jobConf);
    startRPCServer(remoteJT);
  }

  private void startRPCServer(Object instance) throws IOException {
    if (interTrackerServer != null) {
      return;
    }
    int handlerCount = conf.getInt("mapred.job.tracker.handler.count", 10);

    // Use the DNS hostname so that Task Trackers can connect to JT.
    jobTrackerAddress = NetUtils.createSocketAddr(
      java.net.InetAddress.getLocalHost().getCanonicalHostName(),
      0);
    interTrackerServer = RPC.getServer(instance,
       jobTrackerAddress.getHostName(), jobTrackerAddress.getPort(),
       handlerCount, false, conf);
    interTrackerServer.start();
    jobTrackerAddress = new InetSocketAddress(
      jobTrackerAddress.getHostName(),
      interTrackerServer.getListenerAddress().getPort());
    LOG.info("CoronaJobTracker up at " + jobTrackerAddress);
  }

  private void startInfoServer() throws IOException {
    InetSocketAddress infoSocAddr = NetUtils.createSocketAddr(
      java.net.InetAddress.getLocalHost().getCanonicalHostName(),
      0);
    String infoBindAddress = infoSocAddr.getHostName();
    int tmpInfoPort = infoSocAddr.getPort();
    infoServer = new HttpServer("jt", infoBindAddress, tmpInfoPort,
        tmpInfoPort == 0, conf);
    infoServer.setAttribute("job.tracker", this);
    infoServer.start();
    this.infoPort = this.infoServer.getPort();

    String hostname = java.net.InetAddress.getLocalHost().getCanonicalHostName();
    this.conf.set("mapred.job.tracker.http.address", hostname + ":" + this.infoPort);
    this.conf.setInt("mapred.job.tracker.info.port", this.infoPort);
    this.conf.set("mapred.job.tracker.info.bindAddress", hostname);

    LOG.info("JobTracker webserver: " + this.infoPort);
  }

  public String getJobTrackerMachine() {
    return jobTrackerAddress.getHostName();
  }

  public String getUrl() throws IOException {
    String jobHistoryFileLocation = jobHistory.getCompletedJobHistoryPath();
    String encodedJobHistoryFileLocation =
      URLEncoder.encode(jobHistoryFileLocation, "UTF-8");

    String url = getProxyUrl(conf,
                             "coronajobdetails.jsp?jobid=" + jobId +
                             "&jobhistoryfileloc=" +
                             encodedJobHistoryFileLocation);
    return url;
  }

  public SessionStatus getSessionEndStatus(int jobState) {
    if (sessionEndStatus != null) {
      return sessionEndStatus;
    }
    switch (jobState) {
      case JobStatus.PREP:
      case JobStatus.RUNNING:
        return SessionStatus.RUNNING;
      case JobStatus.SUCCEEDED:
        return SessionStatus.SUCCESSFUL;
      case JobStatus.FAILED:
        return SessionStatus.FAILED;
      case JobStatus.KILLED:
        return SessionStatus.KILLED;
      default:
      throw new RuntimeException("Unknown job state: " + jobState);
    }
  }

  protected void closeIfComplete(boolean closeFromWebUI) throws IOException {
    // Prevent multiple simultaneous executions of this function. We could have
    // the Web UI and JobSubmissionProtocol.killJob() call this, for example.
    if (this.job.getStatus().isJobComplete()) {
      close(closeFromWebUI);
    }
  }

  void close(boolean closeFromWebUI) throws IOException {
    synchronized (closeLock) {
      if (!running) {
        return;
      }
      running = false;
      reportJobStats();
      if (jobHistory != null) {
        try {
          jobHistory.markCompleted();
        } catch (IOException ioe) {
          LOG.warn("Failed to mark job " + jobId + " as completed!", ioe);
        }
        jobHistory.shutdown();
      }

      if (sessionDriver != null) {
        int jobState = 0;
        if (job == null) {
          // The remote JT will have the real status.
          jobState = JobStatus.SUCCEEDED;
          sessionDriver.stop(getSessionEndStatus(jobState));
        } else {
          jobState = job.getStatus().getRunState();
          if (jobState != JobStatus.SUCCEEDED) {
            // We will report task failure counts only if the job succeeded.
            trackerStats.resetFailedCount();
          }
          sessionDriver.stop(
            getSessionEndStatus(jobState),
            ResourceTracker.resourceTypes(),
            trackerStats.getNodeUsageReports());
        }
      }

      // Stop the RPC server only if this job tracker is not operating
      // in "remote" mode and it did not start a remote job tracker.
      // We need the RPC server to hang around so that status
      // calls from local JT -> remote JT and heartbeats from
      // remote JT -> local JT continue to work after the job has completed.
      if (!this.isStandalone && remoteJT == null && interTrackerServer != null) {
        interTrackerServer.stop();
      }

      if (expireLaunchingTasks != null) {
        expireLaunchingTasks.shutdown();
        expireLaunchingTasks.interrupt();
        try {
          expireLaunchingTasks.join();
        } catch (InterruptedException e) {}
        expireLaunchingTasks = null;
      }
      if (resourceUpdaterThread != null) {
        resourceUpdaterThread.interrupt();
        try {
          resourceUpdaterThread.join();
        } catch (InterruptedException e) {}
        resourceUpdaterThread = null;
      }
      if (assignTasksThread != null) {
        assignTasksThread.interrupt();
        try {
          assignTasksThread.join();
        } catch (InterruptedException e) {}
        assignTasksThread = null;
      }
      if (sessionDriver != null) {
        try {
          sessionDriver.join();
        } catch (InterruptedException e) {}
        sessionDriver = null;
      }

      if (taskLauncher != null) {
        taskLauncher.killJob(jobId, resourceTracker.allTrackers());
      }

      if (infoServer != null) {
        if (closeFromWebUI) {
          // If we are being called from the web UI, this function is executing
          // in a web-server thread. Give some time to the web-server to
          // clean up.
          infoServer.setGracefulShutdown(1000);
        }
        try {
          infoServer.stop();
        } catch (Exception ex) {
          LOG.warn("Exception shutting down web server ", ex);
        }
      }
      synchronized (lockObject) {
        closed = true;
        lockObject.notifyAll();
      }
    }
  }

  private void reportJobStats() {
    if (job == null) {
      return;
    }
    Counters jobCounters = job.getJobCounters();
    JobStats jobStats = job.getJobStats();
    String pool = null;
    if (sessionDriver != null) {
      pool = sessionDriver.getPoolName();
    }
    String jobId = job.getProfile().getJobId().toString();
    try {
      CoronaConf coronaConf = new CoronaConf(conf);
      InetSocketAddress aggregatorAddr = NetUtils.createSocketAddr(
        coronaConf.getProxyJobTrackerAddress());
      long timeout = 5000; // Can make configurable later.
      CoronaJobAggregator aggregator = RPC.waitForProxy(
        CoronaJobAggregator.class,
        CoronaJobAggregator.versionID,
        aggregatorAddr,
        conf,
        timeout);
      LOG.info("Reporting job stats with jobId=" + jobId +
        ", pool=" + pool + ", jobStats=" + jobStats + ", " +
        "jobCounters=" + jobCounters);
      aggregator.reportJobStats(jobId, pool, jobStats, jobCounters);
    } catch (IOException e) {
      LOG.warn("Ignoring error in reportJobStats ", e);
    }
  }

  class AssignTasksThread implements Runnable {
    @Override
    public void run() {
      while(running) {
        try {
          assignTasks();
        } catch (InterruptedException e) {
          // ignore and let loop check running flag
        } catch (Throwable t) {
          LOG.fatal("assignTasks thread dying because of " +
                    StringUtils.stringifyException(t));
          return;
        }
      }
      LOG.info ("Terminating AssignTasksThread");
    }
  }

  /**
   * This thread performs heartbeats to the parent CJT. It has two purposes -
   * notify the parent of the RPC host:port information of this CJT - detect if
   * the parent has died, and terminate this CJT in that case.
   */
  class ParentHeartbeat implements Runnable {
    final InetSocketAddress myAddr;
    final InetSocketAddress parentAddr;
    final InterCoronaJobTrackerProtocol parent;
    private final TaskAttemptID attemptId;
    private final String sessionId;

    public ParentHeartbeat(
      Configuration conf,
      TaskAttemptID attemptId,
      InetSocketAddress myAddr,
      InetSocketAddress parentAddr,
      String sessionId) throws IOException {
      this.attemptId = attemptId;
      this.myAddr = myAddr;
      this.parentAddr = parentAddr;
      this.sessionId = sessionId;
      long connectTimeout = RemoteJTProxy.getRemotJTTimeout(conf);
      parent = RPC.waitForProxy(
          InterCoronaJobTrackerProtocol.class,
          InterCoronaJobTrackerProtocol.versionID,
          parentAddr,
          conf,
          connectTimeout);
    }

    public void initialHeartbeat() throws IOException {
      parent.reportRemoteCoronaJobTracker(
          attemptId.toString(),
          myAddr.getHostName(),
          myAddr.getPort(),
          sessionId);
    }

    @Override
    public void run() {
      while (true) {
        try {
          parent.reportRemoteCoronaJobTracker(
              attemptId.toString(),
              myAddr.getHostName(),
              myAddr.getPort(),
              sessionId);
          LOG.info("Performed heartbeat to parent at " + parentAddr);
          Thread.sleep(1000);
        } catch (IOException e) {
          LOG.error("Could not communicate with parent, closing this CJT ", e);
          CoronaJobTracker jt = CoronaJobTracker.this;
          try {
            jt.killJob(jt.jobId);
          } catch (IOException e1) {
            LOG.error("Error in closing on timeout ", e1);
          } finally {
            System.exit(HEARTBEAT_THREAD_KILLED_CJT);
          }
        } catch (InterruptedException e) {
          // Ignore and check running flag.
        }
      }
    }
  }


  @Override
  public boolean processAvailableResource(ResourceGrant grant) {
    if (isBadResource(grant)) {
      LOG.info("Resource " + grant.getId() + " is bad");
      processBadResource(grant.getId(), true);
      // return true since this request was bad and will be returned
      // so it should no longer be available
      return true;
    } else if (!isResourceNeeded(grant)) {
      // This resource is no longer needed, but it is not a fault
      // of the host
      LOG.info("Resource " + grant.getId() + " is not needed");
      processBadResource(grant.getId(), false);
      return true;
    }
    InetAddress addr =
      Utilities.appInfoToAddress(grant.appInfo);
    String trackerName = grant.getNodeName();
    boolean isMapGrant =
        grant.getType().equals(ResourceType.MAP);
    Task task = getSetupAndCleanupTasks(trackerName, addr.host, isMapGrant);
    if (task == null) {
      TaskInProgress tip = null;
      synchronized (lockObject) {
        tip = requestToTipMap.get(grant.getId());
      }
      if (tip.isMapTask()) {
        task = job.obtainNewMapTaskForTip(trackerName, addr.host, tip);
      } else {
        task = job.obtainNewReduceTaskForTip(trackerName, addr.host, tip);
      }
    }
    if (task != null) {
      TaskAttemptID taskId = task.getTaskID();
      taskLookupTable.createTaskEntry(taskId, trackerName,
          job.getTaskInProgress(taskId.getTaskID()), grant.getId());
      taskLauncher.launchTask(task, trackerName, addr);
      trackerStats.recordTask(trackerName);
      return true;
    }
    return false;
  }

  public boolean isBadResource(ResourceGrant grant) {
    InetAddress addr = grant.address;
    String trackerName = grant.getNodeName();
    TaskInProgress tip = requestToTipMap.get(grant.getId());
    return trackerStats.isFaulty(trackerName) ||
           !job.canTrackerBeUsed(trackerName, addr.host, tip) ||
           job.isBadSpeculativeResource(tip, trackerName, addr.host);
  }

  public boolean isResourceNeeded(ResourceGrant grant) {
    InetAddress addr = grant.address;
    String trackerName = grant.getNodeName();
    TaskInProgress tip = requestToTipMap.get(grant.getId());
    // 1. If the task is running and we can speculate
    // 2. If the task is not running, but is runnable
    // 3. If we are about to reuse a tip for something else
    return (tip.isRunning() &&
            job.confirmSpeculativeTask(tip, trackerName, addr.host)) ||
            (!tip.isRunning() && tip.isRunnable()) ||
            (job.needsTaskCleanup(tip)) ||
            job.shouldReuseTaskResource(tip);
  }

  /**
   * Return this grant and request a different one.
   * This can happen because the task has failed, was killed
   * or the job tracker decided that the resource is bad
   *
   * @param grant
   * @param abandonHost - if true then this host will be excluded
   * from the list of possibilities for this request
   */
  public void processBadResource(int grant, boolean abandonHost) {
    synchronized (lockObject) {
      Set<String> excludedHosts = null;
      TaskInProgress tip = requestToTipMap.get(grant);
      if (!tip.isRunnable() ||
          (tip.isRunning() &&
              !(speculatedMaps.contains(tip) ||
                  speculatedReduces.contains(tip)))) {
        // The task is not runnable anymore. Job is done/killed/failed or the
        // task has finished and this is a speculative resource
        // Or the task is running and this is a speculative resource
        // but the speculation is no longer needed
        resourceTracker.releaseResource(grant);
        return;
      }
      if (abandonHost) {
        ResourceGrant resource = resourceTracker.getGrant(grant);
        String hostToExlcude = resource.getAddress().getHost();
        taskToContextMap.get(tip).excludedHosts.add(hostToExlcude);
        excludedHosts = taskToContextMap.get(tip).excludedHosts;
      }
      ResourceRequest newReq = resourceTracker.releaseAndRequestResource(grant,
          excludedHosts);
      requestToTipMap.put(newReq.getId(), tip);
      TaskContext context = taskToContextMap.get(tip);
      if (context == null) {
        context = new TaskContext(newReq);
      } else {
        context.resourceRequests.add(newReq);
      }
      taskToContextMap.put(tip, context);
    }
  }

  /**
   * One iteration of core logic.
   */
  void assignTasks() throws InterruptedException {
    resourceTracker.processAvailableGrants(this);
  }

  void processDeadNodes() {
    if (job == null) return;
    synchronized(lockObject) {
      for (String deadNode : deadNodes) {
        trackerStats.recordDeadTracker(deadNode);
        List<TaskAttemptID> attempts = 
          taskLookupTable.getSuccessfulTasksForNode(deadNode);
        for (TaskAttemptID attempt : attempts) {
          TaskInProgress tip = taskLookupTable.getTIP(attempt);
          if (tip.isMapTask()) {
            // Only the map task needs to be rerun if there was a failure
            job.failedTask(tip, attempt, "Lost task tracker",
                TaskStatus.Phase.MAP, false, deadNode, null);
          }
        }
        Set<Integer> grantIds = taskLookupTable.grantsInUseOnTracker(deadNode);
        for (int grantId : grantIds) {
          TaskAttemptID attempt = taskLookupTable.taskForGrantId(grantId);
          // We are just failing the tasks, since if they are still 
          // to be launched the launcher will check with the trackerStats
          // see that the tracker is dead and not launch them in the first
          failTask(attempt, "TaskTracker is dead", false);
        }
      }
      deadNodes.clear();
    }
  }

  void processGrantsToRevoke() {
    if (job == null) return;
    Map<ResourceGrant, TaskAttemptID> processed =
        new HashMap<ResourceGrant, TaskAttemptID>();
    Set<String> nodesOfGrants = new HashSet<String>();
    synchronized(lockObject) {
      for (ResourceGrant grant : grantsToRevoke) {
        TaskAttemptID attemptId = taskLookupTable.taskForGrant(grant);
        if (attemptId != null) {
          if (taskLauncher.removeLaunchingTask(attemptId)) {
            // Kill the task in the job since it never got launched
            expireLaunchingTasks.failedLaunch(attemptId);
            continue;
          }
          killTaskUnprotected(attemptId, false,
            "Request received to kill" +
            " task '" + attemptId + "' by cluster manager (grant revoked)");
          processed.put(grant, attemptId);
          nodesOfGrants.add(grant.getNodeName());
          // Grant will get removed from the resource tracker
          // when the kill takes effect and we get a response from TT.
        }
      }
      for (String ttNode : nodesOfGrants) {
        queueKillActions(ttNode);
      }
    }
    for (Map.Entry<ResourceGrant, TaskAttemptID> entry : processed.entrySet()) {
      LOG.info("Revoking resource " + entry.getKey().getId() +
               " task: " + entry.getValue());
      grantsToRevoke.remove(entry.getKey());
    }
  }

  static class TrackerClientCache {
    Map<InetSocketAddress, CoronaTaskTrackerProtocol> trackerClients =
      new HashMap<InetSocketAddress, CoronaTaskTrackerProtocol>();
    Configuration conf;

    TrackerClientCache(Configuration conf) {
      this.conf = conf;
    }

    public synchronized CoronaTaskTrackerProtocol getClient(InetSocketAddress s)
        throws IOException {
      CoronaTaskTrackerProtocol client = trackerClients.get(s);
      if (client == null) {
        client = createClient(s);
        trackerClients.put(s, client);
      }
      return client;
    }

    public synchronized void resetClient(InetSocketAddress s) {
      trackerClients.remove(s);
    }

    protected CoronaTaskTrackerProtocol createClient(InetSocketAddress s)
        throws IOException {
      LOG.info("Creating client to " + s.getHostName() + ":" + s.getPort());
      long connectTimeout = conf.getLong(TT_CONNECT_TIMEOUT_MSEC_KEY, 10000L);
      return RPC.waitForProxy(
            CoronaTaskTrackerProtocol.class,
            CoronaTaskTrackerProtocol.versionID, s, conf, connectTimeout);
    }

    public synchronized void clearClient(InetSocketAddress s) {
      CoronaTaskTrackerProtocol client = trackerClients.get(s);
      if (client != null) {
        trackerClients.remove(s);
      }
    }
  }

  void processTaskLaunchError(TaskTrackerAction ttAction) {
    if (ttAction instanceof LaunchTaskAction) {
      LaunchTaskAction launchTaskAction = (LaunchTaskAction) ttAction;
      TaskAttemptID attempt = launchTaskAction.getTask().getTaskID();
      expireLaunchingTasks.failedLaunch(attempt);
    } else if (ttAction instanceof KillTaskAction) {
      KillTaskAction killTaskAction = (KillTaskAction) ttAction;
      TaskAttemptID attempt = killTaskAction.getTaskID();
      failTask(attempt, "TaskTracker is dead", false);
    }
  }

  /**
   * A thread to update resource requests/releases.
   */
  protected class ResourceUpdater implements Runnable {
    void notifyThread() {
      synchronized(this) {
        this.notify();
      }
    }

    void waitToBeNotified() throws InterruptedException {
      synchronized(this) {
        this.wait(1000L);
      }
    }

    @Override
    public void run() {
      while (running) {
        try {
          // Check if session had errors in heartbeating.
          // We need this to detect lost sessions early.
          if (sessionDriver != null) {
            IOException sessionException = sessionDriver.getFailed();
            if (sessionException != null) {
              killJobOnSessionError(sessionException, SessionStatus.KILLED);
              return;
            }
          }
          waitToBeNotified();
          processGrantsToRevoke();
          updateSpeculativeResources();
          processDeadNodes();
          try {
            updateResources();
          } catch (IOException e) {
            killJobOnSessionError(e, SessionStatus.KILLED_ABORTED);
            return;
          }
        } catch (InterruptedException ie) {
          // ignore. if shutting down, while cond. will catch it
        }
      }
    }

    private void killJobOnSessionError(IOException e, SessionStatus s) {
      sessionEndStatus = s;
      // Just log the exception name, the stack trace would have been logged
      // earlier.
      LOG.error("Killing job because session indicated error " + e);
      // Kill the job in a new thread, since killJob() will call
      // close() eventually, and that will try to join() all the
      // existing threads, including the thread calling this function.
      new Thread(new Runnable() {
        @Override
        public void run() {
          try {
            killJob(CoronaJobTracker.this.jobId);
          } catch (IOException ignored) {
            LOG.warn("Ignoring exception while killing job", ignored);
          }
        }
      }).start();
    }

    public void updateResources() throws IOException {
      if (job == null) return;

      if (sessionDriver != null) {
        List<ResourceRequest> newRequests =
          resourceTracker.getWantedResources();
        if (!newRequests.isEmpty()) {
          sessionDriver.requestResources(newRequests);
         }
        List<ResourceRequest> toRelease =
          resourceTracker.getResourcesToRelease();
        if (!toRelease.isEmpty()) {
          sessionDriver.releaseResources(toRelease);
        }
      }

      // Check that all resources make sense

      checkTasksResource(TaskType.MAP);
      checkTasksResource(TaskType.REDUCE);
    }

    /**
     * This method copies the requests and adds all the hosts
     * currently used to run the attempts of the TIP to the list
     * of excluded and removes them from the list of requested.
     * This way when we request a resource for speculation it will
     * not be given on the host that is already running an attempt
     *
     * @param req the request to copy
     * @param tip the task in progress of this request. It is being used
     * to figure out which hosts are running attempts of this task.
     */
    private void excludeHostsUnprotected(ResourceRequest req,
        TaskInProgress tip) {
      Set<String> excludedHosts = new HashSet<String>();
      excludedHosts.addAll(taskToContextMap.get(tip).excludedHosts);
      for (TaskAttemptID tid : tip.getAllTaskAttemptIDs()) {
        Integer runningGrant = taskLookupTable.getGrantIdForTask(tid);
        if (runningGrant == null) {
          // This task attempt is no longer running
          continue;
        }
        ResourceGrant resource = resourceTracker.getGrant(runningGrant);
        String tidHost = resource.getAddress().getHost();
        excludedHosts.add(tidHost);
      }
      req.setExcludeHosts(new ArrayList<String>(excludedHosts));
      List<String> newHosts = new ArrayList<String>();
      if (req.getHosts() != null) {
        for (String host : req.getHosts()) {
          if (!excludedHosts.contains(host)) {
            newHosts.add(host);
          }
        }
        req.setHosts(newHosts);
      }
    }

    public void updateSpeculativeResources() {
      if (job == null) return;
      // Update resource requests based on speculation.
      if (job.getStatus().getRunState() == JobStatus.RUNNING) {
        job.updateSpeculationCandidates();
      }

      synchronized (lockObject) {
        List<TaskInProgress> maps = job.getSpeculativeCandidates(TaskType.MAP);
        if (maps != null) {
          for (TaskInProgress tip : maps) {
            if (!speculatedMaps.contains(tip)) {
              // Speculate the tip
              ResourceRequest req =
                resourceTracker.newMapRequest(tip.getSplitLocations());
              excludeHostsUnprotected(req, tip);
              registerNewRequestForTip(tip, req);
            }
          }
          speculatedMaps.clear();
          speculatedMaps.addAll(maps);
        }
        List<TaskInProgress> reduces = job
            .getSpeculativeCandidates(TaskType.REDUCE);
        if (reduces != null) {
          for (TaskInProgress tip : reduces) {
            if (!speculatedReduces.contains(tip)) {
              // Speculate the tip
              ResourceRequest req = resourceTracker.newReduceRequest();
              excludeHostsUnprotected(req, tip);
              registerNewRequestForTip(tip, req);
            }
          }
          speculatedReduces.clear();
          speculatedReduces.addAll(reduces);
        }
      }
    }

    private void checkTasksResource(TaskType type) throws IOException {
      synchronized (lockObject) {
        if (!job.inited()) {
          return;
        }
        if (type == TaskType.REDUCE && !job.areReducersInitialized()) {
          return;
        }
        TaskInProgress[] tasks = job.getTasks(type);
        for (TaskInProgress tip : tasks) {
          // Check that tip is either:
          if (tip.isRunnable()) {
            // There should be requests for this tip since it is not done yet
            List<ResourceRequest> requestIds =
              taskToContextMap.get(tip).resourceRequests;
            if (requestIds == null || requestIds.size() == 0) {
              // This task should be runnable, but it doesn't
              // have requests which means it will never run
              throw new IOException("Tip " + tip.getTIPId() +
                " doesn't have resources " + "requested");
            }
          }
        }
      }
    }
  }

  Task getSetupAndCleanupTasks(String taskTrackerName, String hostName,
        boolean isMapGrant) {
    Task t = null;
    t = job.obtainJobCleanupTask(taskTrackerName, hostName, isMapGrant);

    if (t == null) {
      t = job.obtainJobSetupTask(taskTrackerName, hostName, isMapGrant);
    }
    return t;
  }

  void updateTaskStatuses(TaskTrackerStatus status) {
    String trackerName = status.getTrackerName();
    for (TaskStatus report : status.getTaskReports()) {
      report.setTaskTracker(trackerName);
      TaskAttemptID taskId = report.getTaskID();

      // Remove it from the expired task list
      if (report.getRunState() != TaskStatus.State.UNASSIGNED) {
        expireLaunchingTasks.removeTask(taskId);
      }

      if (!this.jobId.equals(taskId.getJobID())) {
        LOG.warn("Task " + taskId +
            " belongs to unknown job " + taskId.getJobID());
        continue;
      }

      TaskInProgress tip = taskLookupTable.getTIP(taskId);
      if (tip == null) {
        continue;
      }
      // Clone TaskStatus object here, because CoronaJobInProgress
      // or TaskInProgress can modify this object and
      // the changes should not get reflected in TaskTrackerStatus.
      // An old TaskTrackerStatus is used later in countMapTasks, etc.
      job.updateTaskStatus(tip, (TaskStatus)report.clone(), status);
      setupReduceRequests(job);
      List<TaskInProgress> failedTips = processFetchFailures(report);
    }
  }

  @Override
  public void taskStateChange(TaskStatus.State state, TaskInProgress tip,
      TaskAttemptID taskid) {
    LOG.info("The state of " + taskid + " changed to " + state);
    processTaskResource(state, tip, taskid);
  }
  private void processTaskResource(TaskStatus.State state, TaskInProgress tip,
      TaskAttemptID taskid) {
    if (!TaskStatus.TERMINATING_STATES.contains(state)) {
      return;
    }
    Integer grantId = taskLookupTable.getGrantIdForTask(taskid);
    // The TIP that this grant was issued for originally
    // if tip is not equal to assignedTip then the grant was borrowed
    TaskInProgress assignedTip = requestToTipMap.get(grantId);
    taskLookupTable.removeTaskEntry(taskid);

    ResourceGrant grant = resourceTracker.getGrant(grantId);
    String trackerName = null;
    if (grant != null) {
      trackerName = grant.nodeName;
    }
    if (trackerName != null) {
      if (state == TaskStatus.State.SUCCEEDED) {
        trackerStats.recordSucceededTask(trackerName);
      } else if (state == TaskStatus.State.FAILED_UNCLEAN) {
        trackerStats.recordFailedTask(trackerName);
      } else if (state == TaskStatus.State.KILLED_UNCLEAN) {
        trackerStats.recordKilledTask(trackerName);
      }
    }

    if (state == TaskStatus.State.SUCCEEDED) {
      assert (grantId != null) : "Grant for task id " + taskid + " is null!";
      TaskType taskType = tip.getAttemptType(taskid);
      if (taskType == TaskType.MAP || taskType == TaskType.REDUCE) {
        // Ignore cleanup tasks types.
        taskLookupTable.addSuccessfulTaskEntry(taskid, trackerName);
      }
      if (job.shouldReuseTaskResource(tip) || !assignedTip.equals(tip)) {
        resourceTracker.reuseGrant(grantId);
      } else {
        resourceTracker.releaseResource(grantId);
      }
    } else {
      if (grantId == null) {
        // grant could be null if the task reached a terminating state twice,
        // e.g. succeeded then failed due to a fetch failure. Or if a TT
        // dies after after a success
        if (tip.isMapTask()) {
          registerNewRequestForTip(tip,
              resourceTracker.newMapRequest(tip.getSplitLocations()));
        } else {
          registerNewRequestForTip(tip, resourceTracker.newReduceRequest());
        }

      } else {
        boolean excludeResource = state != TaskStatus.State.KILLED
            && state != TaskStatus.State.KILLED_UNCLEAN;
        processBadResource(grantId, excludeResource);
      }
    }
  }

  private List<TaskInProgress> processFetchFailures(TaskStatus taskStatus) {
    List<TaskInProgress> failedMaps = new ArrayList<TaskInProgress>();
    List<TaskAttemptID> failedFetchMaps = taskStatus.getFetchFailedMaps();
    if (failedFetchMaps != null) {
      TaskAttemptID reportingAttempt = taskStatus.getTaskID();
      for (TaskAttemptID mapTaskId : failedFetchMaps) {
        TaskInProgress failedFetchMap = taskLookupTable.getTIP(mapTaskId);

        if (failedFetchMap != null) {
          // Gather information about the map which has to be failed, if need be
          String failedFetchTrackerName =
            taskLookupTable.getAssignedTracker(mapTaskId);
          if (failedFetchTrackerName == null) {
            failedFetchTrackerName = "Lost task tracker";
          }
          if (((CoronaJobInProgress)failedFetchMap.getJob()).fetchFailureNotification(
            reportingAttempt, failedFetchMap, mapTaskId, failedFetchTrackerName)) {
            failedMaps.add(failedFetchMap);
          }
        } else {
          LOG.warn("Could not find TIP for " + failedFetchMap);
        }
      }
    }
    return failedMaps;
  }

  /**
   * A tracker wants to know if any of its Tasks can be committed
   */
  List<CommitTaskAction> getCommitActions(TaskTrackerStatus tts) {
    synchronized(lockObject) {
      List<CommitTaskAction> saveList = new ArrayList<CommitTaskAction>();
      List<TaskStatus> taskStatuses = tts.getTaskReports();
      if (taskStatuses != null) {
        for (TaskStatus taskStatus : taskStatuses) {
          if (taskStatus.getRunState() == TaskStatus.State.COMMIT_PENDING) {
            TaskAttemptID taskId = taskStatus.getTaskID();
            TaskInProgress tip = taskLookupTable.getTIP(taskId);
            if (tip == null) {
              continue;
            }
            if (tip.shouldCommit(taskId)) {
              Integer grant = taskLookupTable.getGrantIdForTask(taskId);
              if (grant != null) {
                InetAddress addr = Utilities.appInfoToAddress(
                    resourceTracker.getGrant(grant).getAppInfo());
                CommitTaskAction commitAction = new CommitTaskAction(taskId);
                saveList.add(commitAction);
                LOG.debug(tts.getTrackerName() +
                    " -> CommitTaskAction: " + taskId);
              }
            }
          }
        }
      }
      return saveList;
    }
  }

  CoronaJobInProgress createJob(JobID jobId, JobConf defaultConf) throws IOException {
    checkJobId(jobId);

    return new CoronaJobInProgress(lockObject, jobId, new Path(getSystemDir()),
                                   defaultConf, taskLookupTable, this, jobHistory, getUrl());
  }
  private void registerNewRequestForTip(TaskInProgress tip, ResourceRequest req) {
    requestToTipMap.put(req.getId(), tip);
    TaskContext context = taskToContextMap.get(tip);
    if (context == null) {
      context = new TaskContext(req);
    } else {
      context.resourceRequests.add(req);
    }
    taskToContextMap.put(tip, context);
    resourceTracker.recordRequest(req);
  }

  private void setupMapRequests(CoronaJobInProgress jip) {
    synchronized (lockObject) {
      TaskInProgress[] maps = jip.getTasks(TaskType.MAP);
      for (TaskInProgress map : maps) {
        ResourceRequest req =
          resourceTracker.newMapRequest(map.getSplitLocations());
        registerNewRequestForTip(map, req);
      }
    }
  }

  private void setupReduceRequests(CoronaJobInProgress jip) {
    synchronized (lockObject) {
      if (jip.scheduleReducesUnprotected() && !jip.initializeReducers()) {

        TaskInProgress[] reduces = jip.getTasks(TaskType.REDUCE);
        for (TaskInProgress reduce : reduces) {
          ResourceRequest req = resourceTracker.newReduceRequest();
          registerNewRequestForTip(reduce, req);
        }
      }
    }
  }

  JobStatus startJob(CoronaJobInProgress jip, SessionDriver driver)
      throws IOException {
    synchronized(lockObject) {
      this.job = jip;
    }
    if (job.isJobEmpty()) {
      job.completeEmptyJob();
      closeIfComplete(false);
      return job.getStatus();
    } else if (!job.isSetupCleanupRequired()) {
      job.completeSetup();
    }

    setupMapRequests(job);
    setupReduceRequests(job);
    resourceUpdater.notifyThread();

    return job.getStatus();
  }

  CoronaJobInProgress getJob() {
    return job;
  }

  @Override
  public JobInProgressTraits getJobInProgress(JobID jobId) {
    checkJobId(jobId);
    return this.job;
  }


  @Override
  public long getProtocolVersion(String protocol, long clientVersion)
      throws IOException {
    if (protocol.equals(JobSubmissionProtocol.class.getName())) {
      return JobSubmissionProtocol.versionID;
    } else if (protocol.equals(InterTrackerProtocol.class.getName())) {
      return InterTrackerProtocol.versionID;
    } else {
      throw new IOException("Unknown protocol " + protocol);
    }
  }

  public void killJobFromWebUI(JobID jobId) throws IOException {
    checkJobId(jobId);
    LOG.info("Killing job from Web UI " + jobId);
    job.kill();
    closeIfComplete(true);
  }

  private boolean canStartLocalJT(JobConf jobConf) {
    boolean forceRemote = jobConf.getBoolean(
        "mapred.coronajobtracker.forceremote", false);
    if (isStandalone) {
      // If we are running in standalone (remote) mode, start the tracker.
      return true;
    } else {
      // We are running in the client process.
      if (forceRemote) {
        // If remote mode is forced, should not start tracker.
        return false;
      } else {
        // Remote mode is not forced, go remote if there are too many
        // map tasks.
        return jobConf.getNumMapTasks() <=
              jobConf.getInt(STANDALONE_CJT_THRESHOLD_CONF,
                  STANDALONE_CJT_THRESHOLD_DEFAULT);
      }
    }
  }


  //////////////////////////////////////////////////////////////////////////////
  // JobSubmissionProtocol
  //////////////////////////////////////////////////////////////////////////////

  /**
   * Returns a unique JobID for a new job.
   * CoronaJobTracker can only run a single job and it's id is fixed a-priori
   *
   */
  @Override
  public JobID getNewJobId() throws IOException {
    int value = jobCounter.incrementAndGet();
    if (value > 1)
      throw new RuntimeException ("CoronaJobTracker can only run one job! (value=" + value + ")");

    return jobId;
  }

  @Override
  public JobStatus submitJob(JobID jobId) throws IOException {
    JobConf jobConf = new JobConf(conf);
    if (canStartLocalJT(jobConf)) {
      startFullTracker();
      CoronaJobInProgress jip = createJob(jobId, jobConf);
      if (sessionDriver != null) {
        sessionDriver.setName(jobConf.getJobName());
        sessionDriver.setUrl(getUrl());
        sessionDriver.setPriority(jip.getPriority());
      }
      jip.initTasks();
      return startJob(jip, sessionDriver);
    } else {
      if (sessionDriver != null) {
        sessionDriver.setName("Launch pending for " + jobConf.getJobName());
      }
      CoronaJobInProgress.uploadCachedSplits(jobId, jobConf, getSystemDir());
      startRestrictedTracker(jobId, jobConf);
      remoteJT.waitForJTStart(jobConf);
      JobStatus status = remoteJT.submitJob(jobId);
      String url = remoteJT.getJobProfile(jobId).getURL().toString();
      if (sessionDriver != null) {
        sessionDriver.setName("Launched session " +
            remoteJT.getRemoteSessionId());
        sessionDriver.setUrl(url);
      }
      return status;
    }
  }

  @Override
  public ClusterStatus getClusterStatus(boolean detailed) throws IOException {
    throw new UnsupportedOperationException(
        "getClusterStatus is not supported by CoronaJobTracker");
  }

  @Override
  public void killJob(JobID jobId) throws IOException {
    checkJobId(jobId);
    LOG.info("Killing job " + jobId);
    if (remoteJT == null) {
      job.kill();
      closeIfComplete(false);
    } else {
      remoteJT.killJob(jobId);
      LOG.info("Successfully killed " + jobId + " on remote JT, closing");
      close(false);
    }
  }

  @Override
  public void setJobPriority(JobID jobId, String priority) throws IOException {
    if (!this.jobId.equals(jobId))
      throw new IOException("JobId " + jobId + " does not match the expected id of: " + this.jobId);

    SessionPriority newPrio = SessionPriority.valueOf(priority);
    sessionDriver.setPriority(newPrio);
    job.setPriority(newPrio);
  }

  @Override
  public boolean killTask(TaskAttemptID taskId, boolean shouldFail)
      throws IOException {
    if (remoteJT == null) {
      synchronized (lockObject) {
        return killTaskUnprotected(taskId, shouldFail,"" +
          "Request received to " + (shouldFail ? "fail" : "kill") +
          " task '" + taskId + "' by user");
      }
    } else {
      return remoteJT.killTask(taskId, shouldFail);
    }
  }

  private boolean killTaskUnprotected(TaskAttemptID taskId, boolean shouldFail,
                                      String diagnosticInfo) {
    TaskInProgress tip = taskLookupTable.getTIP(taskId);
    return tip.killTask(taskId, shouldFail, diagnosticInfo);
  }

  @Override
  public JobProfile getJobProfile(JobID jobId) throws IOException {
    if (!this.jobId.equals(jobId)) {
      return null;
    } else {
      if (remoteJT == null) {
        return this.job.getProfile();
      } else {
        return remoteJT.getJobProfile(jobId);
      }
    }
  }

  @Override
  public JobStatus getJobStatus(JobID jobId) throws IOException {
    JobStatus status = null;
    if (this.jobId.equals(jobId)) {
      if (remoteJT == null) {
        status = this.job.getStatus();
        if (status.isJobComplete()) {
          synchronized (lockObject) {
            while (!closed) {
              try {
                lockObject.wait();
              } catch (InterruptedException iex) {
              }
            }
          }
        }
      } else {
        status = remoteJT.getJobStatus(jobId);
        if (status.isJobComplete()) {
          close(false);
        }
      }
    }
    return status;
  }

  @Override
  public Counters getJobCounters(JobID jobId) throws IOException {
    if (!this.jobId.equals(jobId)) {
      return null;
    } else {
      if (remoteJT == null) {
        return this.job.getCounters();
      } else {
        return remoteJT.getJobCounters(jobId);
      }
    }
  }

  @Override
  public TaskReport[] getMapTaskReports(JobID jobId) throws IOException {
    checkJobId(jobId);
    if (remoteJT == null) {
      synchronized (lockObject) {
        return super.getMapTaskReportsImpl(jobId);
      }
    } else {
      return remoteJT.getMapTaskReports(jobId);
    }
  }

  @Override
  public TaskReport[] getReduceTaskReports(JobID jobId) throws IOException {
    checkJobId(jobId);
    if (remoteJT == null) {
      synchronized (lockObject) {
        return super.getReduceTaskReportsImpl(jobId);
      }
    } else {
      return remoteJT.getReduceTaskReports(jobId);
    }
  }

  @Override
  public TaskReport[] getCleanupTaskReports(JobID jobId) throws IOException {
    checkJobId(jobId);
    if (remoteJT == null) {
      synchronized (lockObject) {
        return super.getCleanupTaskReportsImpl(jobId);
      }
    } else {
      return remoteJT.getCleanupTaskReports(jobId);
    }
  }

  @Override
  public TaskReport[] getSetupTaskReports(JobID jobId) throws IOException {
    checkJobId(jobId);
    if (remoteJT == null) {
      synchronized (lockObject) {
        return super.getSetupTaskReportsImpl(jobId);
      }
    } else {
      return remoteJT.getSetupTaskReports(jobId);
    }
  }

  @Override
  public String getFilesystemName() throws IOException {
    return null;
  }

  @Override
  public JobStatus[] jobsToComplete() { return null; }

  @Override
  public JobStatus[] getAllJobs() { return null; }

  @Override
  public TaskCompletionEvent[] getTaskCompletionEvents(JobID jobid,
      int fromEventId, int maxEvents) throws IOException {
    if (!this.jobId.equals(jobId)) {
      return TaskCompletionEvent.EMPTY_ARRAY;
    } else {
      if (remoteJT == null) {
        return job.getTaskCompletionEvents(fromEventId, maxEvents);
      } else {
        return remoteJT.getTaskCompletionEvents(jobid,
              fromEventId, maxEvents);
      }
    }
  }

  @Override
  public String[] getTaskDiagnostics(TaskAttemptID taskId) throws IOException {
    if (remoteJT == null) {
      synchronized (lockObject) {
        return super.getTaskDiagnosticsImpl(taskId);
      }
    } else {
      return remoteJT.getTaskDiagnostics(taskId);
    }
  }

  @Override
  public String getSystemDir() {
    Path sysDir = new Path(conf.get("mapred.system.dir", "/tmp/hadoop/mapred/system"));
    java.net.URI uri = sysDir.toUri();
    if (uri.getScheme() != null && uri.getAuthority() != null) {
      return sysDir.toString();
    } else {
      return fs.makeQualified(sysDir).toString();
    }
  }

  @Override
  public JobQueueInfo[] getQueues() { return null; }

  @Override
  public JobQueueInfo getQueueInfo(String queue) { return null; }

  @Override
  public JobStatus[] getJobsFromQueue(String queue) { return null; }

  @Override
  public QueueAclsInfo[] getQueueAclsForCurrentUser() throws IOException {
    return null;
  }

  //////////////////////////////////////////////////////////////////////////////
  // SessionDriverService.Iface
  //////////////////////////////////////////////////////////////////////////////
  @Override
  public void grantResource(String handle, List<ResourceGrant> granted) {
    LOG.info("Received " + granted.size() + " new grants:" +
         granted.toString());
    resourceTracker.addNewGrants(granted);
  }

  @Override
  public void revokeResource(String handle,
      List<ResourceGrant> revoked, boolean force) {
    synchronized(lockObject) {
      grantsToRevoke.addAll(revoked);
    }
    LOG.info("Giving up " + revoked.size() + " grants: " +
        revoked.toString());
  }

  @Override
  public void processDeadNode(String handle, String deadNode) {
    // CM declared the node as lost so we can process it quickly
    synchronized(lockObject) {
      deadNodes.add(deadNode);
    }
    LOG.info("Node " + deadNode + " declared dead by the CM");
  }

  /////////////////////////////////////////////////////////////////////////////
  // InterTrackerProtocol
  /////////////////////////////////////////////////////////////////////////////
  @Override
  public String getBuildVersion() throws IOException {
    return VersionInfo.getBuildVersion();
  }

  @Override
  public HeartbeatResponse heartbeat(TaskTrackerStatus status,
      boolean restarted, boolean initialContact, boolean acceptNewTasks,
      short responseId) throws IOException {
    updateTaskStatuses(status);

    String trackerName = status.getTrackerName();

    // remember the last known status of this task tracker
    // This is a ConcurrentHashMap, so no lock required.
    taskTrackerStatus.put(trackerName, status);

    // Check for tasks whose outputs can be saved
    List<CommitTaskAction> commitActions = getCommitActions(status);
    for (CommitTaskAction action: commitActions) {
      taskLauncher.commitTask(
          trackerName, resourceTracker.getTrackerAddr(trackerName), action);
    }


    // Return an empty response since the actions are sent separately.
    short newResponseId = (short)(responseId + 1);
    HeartbeatResponse response =
      new HeartbeatResponse(newResponseId, new TaskTrackerAction[0]);

    response.setHeartbeatInterval(getNextHeartbeatInterval());

    queueKillActions(trackerName);

    closeIfComplete(false);

    return response;
  }

  private void queueKillActions(String trackerName) {
    List<KillTaskAction> killActions =
        taskLookupTable.getTasksToKill(trackerName);
    InetAddress addr =
        resourceTracker.getTrackerAddr(trackerName);
    taskLauncher.killTasks(trackerName, addr, killActions);
  }

  private int getNextHeartbeatInterval() {
    return heartbeatInterval;
  }

  @Override
  public void reportTaskTrackerError(String taskTrackerName, String errorClass,
      String errorMessage) throws IOException {
    LOG.warn("reportTaskTrackerError is not implemented in Corona JT, " +
      "params are " + taskTrackerName + "," + errorClass + "," + errorMessage);
  }

  @Override
  public ProtocolSignature getProtocolSignature(String protocol,
      long clientVersion, int clientMethodsHash) throws IOException {
    return ProtocolSignature.getProtocolSignature(
        this, protocol, clientVersion, clientMethodsHash);
  }

  public int getInfoPort() {
    return infoPort;
  }

  public TaskTrackerStatus getTaskTrackerStatus(String trackerID) {
    synchronized(lockObject) {
      return taskTrackerStatus.get(trackerID);
    }
  }

  /**
   * Based on the resource type, get a resource report of the grant # and
   * task #.  Used by coronajobresources.jsp for debugging which resources are
   * being used
   *
   * @param resourceType Map or reduce type
   * @return List of the resource reports for the appropriate type sorted by id.
   */
  public List<ResourceReport> getResourceReportList(String resourceType) {
    Map<Integer, ResourceReport> resourceReportMap =
        new TreeMap<Integer, ResourceReport>();
    synchronized(lockObject) {
      for (Map.Entry<TaskAttemptID, Integer> entry :
          taskLookupTable.taskIdToGrantMap.entrySet()) {
        if ((resourceType.equals("map") && entry.getKey().isMap()) ||
            (resourceType.equals("reduce") && !entry.getKey().isMap())) {
          resourceReportMap.put(entry.getValue(),
              new ResourceReport(entry.getValue(), entry.getKey().toString()));
        }
      }
      for (Integer grantId : resourceTracker.availableResources) {
        if (!resourceReportMap.containsKey(grantId)) {
          resourceReportMap.put(grantId,
              new ResourceReport(grantId, "Available (currently not in use)"));
        }
      }
    }
    return new ArrayList<ResourceReport>(resourceReportMap.values());
  }

  public String getProxyUrl(String relativeUrl) {
    return getProxyUrl(conf, relativeUrl);
  }

  public String getProxyJTAddr() {
    return getProxyJTAddr(conf);
  }

  public static String getProxyJTAddr(Configuration conf) {
    return conf.get("mapred.job.tracker.corona.proxyaddr", "localhost");
  }

  public static String getProxyUrl(Configuration conf, String relativeUrl) {
    String proxyJtAddr = getProxyJTAddr(conf);

    if ((proxyJtAddr != null) && (proxyJtAddr.length() > 0)) {
      String ret  = "http://" + proxyJtAddr + "/proxy?host=" +
        conf.get("mapred.job.tracker.info.bindAddress") + "&port=" +
        conf.get("mapred.job.tracker.info.port") + "&path=";

      int qIndex = relativeUrl.indexOf('?');
      String path = (qIndex == -1) ? relativeUrl : relativeUrl.substring(0, qIndex);
      String params = (qIndex == -1) ? null :
        ( (qIndex == (relativeUrl.length()-1)) ? null : relativeUrl.substring(qIndex+1));

      return ret + path + ((params == null) ? "" : ("&" + params));
    } else {
      return relativeUrl;
    }
  }

  public String getClusterManagerUrl() {
    String httpConf = conf.get("cm.server.http.address");
    if (httpConf != null) {
      return "http://" + httpConf;
    } else {
      return "NONE";
    }
  }

  public TrackerStats getStats() {
    return trackerStats;
  }

  private void checkJobId(JobID jobId) {
    if (!this.jobId.equals(jobId))
      throw new RuntimeException("JobId " + jobId
          + " does not match the expected id of: " + this.jobId);
  }

  /**
   * Gets the resource usage (snapshot), mainly for displaying on the web
   * server.
   *
   * @return Snapshot of resource usage
   */
  public ResourceUsage getResourceUsage() {
    return resourceTracker.getResourceUsage();
  }

  public static void main(String[] args) throws IOException, InterruptedException {
    if (args.length < 4) {
      System.err.println(
          "Usage: java CoronaJobTracker JOBID ATTEMPTID PARENTHOST PARENTPORT");
      System.exit(-1);
    }
    JobID jobId = JobID.forName(args[0]);
    TaskAttemptID attemptId = TaskAttemptID.forName(args[1]);
    InetSocketAddress parentAddr =
      new InetSocketAddress(args[2], Integer.parseInt(args[3]));

    // Use the localized configuration in the working directory.
    JobConf conf = new JobConf(new Path(jobId + ".xml"));
    conf.set("mapred.system.dir", System.getProperty("mapred.system.dir"));

    CoronaJobTracker cjt = new CoronaJobTracker(conf, jobId, attemptId, parentAddr);
    while (cjt.running) {
      Thread.sleep(1000);
    }
  }

  /**
   * Handle a task that could not be launched.
   * @param taskId The task attempt ID.
   */
  public void expiredLaunchingTask(TaskAttemptID taskId) {
    Integer grantId = taskLookupTable.getGrantIdForTask(taskId);
    if (grantId != null) {
      ResourceGrant grant = resourceTracker.getGrant(grantId);
      if (grant != null) {
        trackerStats.recordTimeout(grant.getNodeName());
      }
    }
    failTask(taskId, "Error launching task", false);
  }
}
