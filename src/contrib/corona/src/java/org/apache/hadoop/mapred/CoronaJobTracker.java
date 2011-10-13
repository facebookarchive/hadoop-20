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
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.corona.InetAddress;
import org.apache.hadoop.corona.ResourceGrant;
import org.apache.hadoop.corona.ResourceRequest;
import org.apache.hadoop.corona.SessionDriver;
import org.apache.hadoop.corona.SessionDriverService;
import org.apache.hadoop.corona.SessionStatus;
import org.apache.hadoop.corona.Utilities;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.http.HttpServer;
import org.apache.hadoop.ipc.ProtocolSignature;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.VersionInfo;

/**
 * Tracker for a single job. This can run jobs one-at-a-time.
 */
@SuppressWarnings("deprecation")
public class CoronaJobTracker
  extends JobTrackerTraits
  implements JobSubmissionProtocol, SessionDriverService.Iface,
             InterTrackerProtocol, ResourceTracker.ResourceProcessor {

  public static final Log LOG = LogFactory.getLog(CoronaJobTracker.class);

  static long TASKTRACKER_EXPIRY_INTERVAL = 10 * 60 * 1000;
  public static final String HEART_BEAT_INTERVAL_KEY = "corona.jobtracker.heartbeat.interval";

  JobConf conf; // JT conf.
  FileSystem fs;

  // Handles the session with the cluster manager.
  SessionDriver sessionDriver;
  String sessionId;

  // Variable to atomically check if more than one job is attempted to be
  // launched via this jobtracker
  AtomicInteger jobCounter = new AtomicInteger();

  // Identifier for the current job.
  final JobID jobId;
  CoronaJobInProgress job;

  ResourceTracker resourceTracker;
  List<ResourceGrant> grantsToRevoke = new ArrayList<ResourceGrant>();

  volatile boolean running = true;
  Thread assignTasksThread;

  InetSocketAddress jobTrackerAddress;
  Server interTrackerServer;
  HttpServer infoServer;
  long startTime = System.currentTimeMillis();

  TaskLookupTable taskLookupTable = new TaskLookupTable();

  Map<String, TaskTrackerStatus> taskTrackerStatus =
    new ConcurrentHashMap<String, TaskTrackerStatus>();

  ExpireLaunchingTasks expireLaunchingTasks = new ExpireLaunchingTasks();
  Thread expireLaunchingTasksThread;

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

    public boolean equals(Object o) {
      TaskAttemptIDWithTip that = (TaskAttemptIDWithTip)o;
      return this.attemptId.equals(that.attemptId);
    }

    public int hashCode() {
      return attemptId.hashCode();
    }

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
      synchronized(lockObject) {
        for (Map.Entry<TaskAttemptID, Integer> entry: taskIdToGrantMap.entrySet()) {
          if (entry.getValue().equals(grant.getId())) {
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
        for (TaskAttemptIDWithTip tip: trackerToTaskMap.get(trackerName)) {
          grants.add(taskIdToGrantMap.get(tip.attemptId));
        }
        return grants;
      }
    }

    List<TaskTrackerAction> getTasksToKill(String taskTracker) {
      synchronized(lockObject) {
        Set<TaskAttemptIDWithTip> taskset = trackerToTaskMap.get(taskTracker);
        List<TaskTrackerAction> killList = new ArrayList<TaskTrackerAction>();
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

  class ExpireLaunchingTasks implements Runnable {
    /**
     * This is a map of the tasks that have been assigned to task trackers,
     * but that have not yet been seen in a status report.
     * map: task-id -> time-assigned
     */
    Map<TaskAttemptID, Long> launchingTasks =
      new LinkedHashMap<TaskAttemptID, Long>();

    public void run() {
      while (running) {
        try {
          // Every 3 minutes check for any tasks that are overdue
          Thread.sleep(TASKTRACKER_EXPIRY_INTERVAL/3);
          expireLaunchingTasks();
        } catch (InterruptedException ie) {
          // ignore. if shutting down, while cond. will catch it
        } catch (Exception e) {
          LOG.error("Expire Launching Task Thread got exception: ", e);
        }
      }
    }

    void expireLaunchingTasks() {
      if (job == null) {
        return;
      }
      long now = JobTracker.getClock().getTime();
      LOG.debug("Starting launching task sweep");
      synchronized (lockObject) {
        Iterator<Map.Entry<TaskAttemptID, Long>> itr =
          launchingTasks.entrySet().iterator();
        while (itr.hasNext()) {
          Map.Entry<TaskAttemptID, Long> pair = itr.next();
          TaskAttemptID taskId = pair.getKey();
          long age = now - (pair.getValue()).longValue();
          if (age > TASKTRACKER_EXPIRY_INTERVAL) {
            LOG.info("Launching task " + taskId + " timed out.");
            TaskInProgress tip = taskLookupTable.getTIP(taskId);
            if (tip != null) {
              Integer grantId = taskLookupTable.getGrantIdForTask(taskId);
              ResourceGrant grant = resourceTracker.getGrant(grantId);
              if (grant != null) {
                String trackerName = grant.getNodeName();
                TaskTrackerStatus trackerStatus =
                  getTaskTrackerStatus(trackerName);

                TaskStatus.Phase phase =
                  tip.isMapTask()? TaskStatus.Phase.MAP:
                    TaskStatus.Phase.STARTING;
                boolean isFailed = true;
                CoronaJobTracker.this.job.failedTask(
                    tip, taskId, "Error launching task", phase,
                    isFailed, trackerName, trackerStatus);
              } else {
                LOG.error("Task " + taskId + " is running but has no associated resource");
              }
            }
            itr.remove();
          }
        }
      }
    }

    public void failedLaunch(TaskAttemptID attempt) {
      synchronized (lockObject) {
        // Check if the attempt exists in the map.
        // It might have expired already.
        if (launchingTasks.containsKey(attempt)) {
          // Set the launch time to a very old value.
          launchingTasks.put(attempt, (long)0);
          // Make the expire task logic run immediately.
          expireLaunchingTasksThread.interrupt();
          lockObject.notify();
        }
      }
    }

    public void addNewTask(TaskAttemptID taskName) {
      synchronized (lockObject) {
        launchingTasks.put(taskName,
                           JobTracker.getClock().getTime());
      }
    }

    public void removeTask(TaskAttemptID taskName) {
      synchronized (lockObject) {
        launchingTasks.remove(taskName);
      }
    }
  }

  static class ActionToSend {
    String trackerHost;
    int port;
    TaskTrackerAction action;
    ActionToSend(String trackerHost, int port, TaskTrackerAction action) {
      this.trackerHost = trackerHost;
      this.action = action;
      this.port = port;
    }
  }
  List<ActionToSend> actionsToSend = new LinkedList<ActionToSend>();
  Thread taskLauncherThread;
  TrackerClientCache trackerClientCache;

  ResourceUpdater resourceUpdater = new ResourceUpdater();
  Thread resourceUpdaterThread;

  private int infoPort;

  private Object lockObject = new Object();
  private Object closeLock = new Object();

  CoronaJobHistory jobHistory;

  private int heartbeatInterval;

  // For testing.
  CoronaJobTracker(JobConf conf, String sessionId,
      TrackerClientCache cache) throws IOException {
    this.conf = conf;
    fs = FileSystem.get(conf);
    this.sessionId = sessionId;
    this.trackerClientCache = cache;
    this.resourceTracker = new ResourceTracker(lockObject);
    this.taskLookupTable = new TaskLookupTable();
    this.jobId = new JobID(sessionId, 1);
    this.jobHistory = new CoronaJobHistory(conf, jobId);
    this.heartbeatInterval = conf.getInt(HEART_BEAT_INTERVAL_KEY, 100);
  }

  public CoronaJobTracker(JobConf conf) throws IOException {
    this.conf = conf;
    fs = FileSystem.get(conf);
    this.resourceTracker = new ResourceTracker(lockObject);
    this.taskLookupTable = new TaskLookupTable();

    // Use the DNS hostname so that Task Trackers can connect to JT.
    jobTrackerAddress = NetUtils.createSocketAddr(
      java.net.InetAddress.getLocalHost().getCanonicalHostName(),
      0);
    int handlerCount = conf.getInt("mapred.job.tracker.handler.count", 10);
    this.heartbeatInterval = conf.getInt(HEART_BEAT_INTERVAL_KEY, 3000);
    interTrackerServer = RPC.getServer((InterTrackerProtocol)this,
       jobTrackerAddress.getHostName(), jobTrackerAddress.getPort(),
       handlerCount, false, conf);
    interTrackerServer.start();
    jobTrackerAddress = new InetSocketAddress(
      jobTrackerAddress.getHostName(),
      interTrackerServer.getListenerAddress().getPort());
    LOG.info("CoronaJobTracker up at " + jobTrackerAddress);

    String infoAddr =
      NetUtils.getServerAddress(conf, "mapred.job.tracker.info.bindAddress",
                                "mapred.job.tracker.info.port",
                                "mapred.job.tracker.http.address");
    InetSocketAddress infoSocAddr = NetUtils.createSocketAddr(infoAddr);
    String infoBindAddress = infoSocAddr.getHostName();
    int tmpInfoPort = infoSocAddr.getPort();
    infoServer = new HttpServer("jt", infoBindAddress, tmpInfoPort,
        tmpInfoPort == 0, conf);
    infoServer.setAttribute("job.tracker", this);
    infoServer.start();
    this.infoPort = this.infoServer.getPort();

    // TODO: we may want to bind the jobtracker to a specific interface?
    String hostname = java.net.InetAddress.getLocalHost().getCanonicalHostName();
    this.conf.set("mapred.job.tracker.http.address", hostname + ":" + this.infoPort);
    this.conf.setInt("mapred.job.tracker.info.port", this.infoPort);
    this.conf.set("mapred.job.tracker.info.bindAddress", hostname);

    LOG.info("JobTracker webserver: " + this.infoPort);

    assignTasksThread = new Thread(new AssignTasksThread());
    assignTasksThread.setName("assignTasks Thread");
    assignTasksThread.setDaemon(true);
    assignTasksThread.start();

    taskLauncherThread = new Thread(new TaskLauncherThread());
    taskLauncherThread.setName("Task Launcher Thread");
    taskLauncherThread.setDaemon(true);
    taskLauncherThread.start();

    resourceUpdaterThread = new Thread(resourceUpdater);
    resourceUpdaterThread.setName("Resource Updater");
    resourceUpdaterThread.setDaemon(true);
    resourceUpdaterThread.start();

    expireLaunchingTasksThread = new Thread(expireLaunchingTasks);
    expireLaunchingTasksThread.setName("Expire launching tasks");
    expireLaunchingTasksThread.setDaemon(true);
    expireLaunchingTasksThread.start();

    // Create the session driver. This will contact the cluster manager.
    sessionDriver = new SessionDriver(conf, this);
    sessionId = sessionDriver.getSessionId();

    // the jobtracker can run only a single job. it's jobid is fixed based
    // on the sessionId.
    jobId = new JobID(sessionId, 1);
    jobHistory = new CoronaJobHistory(conf, jobId);

    // Initialize history DONE folder
    if (!jobHistory.isDisabled()) {
      String historyLogDir =
        jobHistory.getCompletedJobHistoryLocation().toString();
      infoServer.setAttribute("historyLogDir", historyLogDir);
      infoServer.setAttribute("conf", conf);
    }

    sessionDriver.setUrl(getUrl());

    this.trackerClientCache = new TrackerClientCache(conf);
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

  public static SessionStatus jobToSessionStatus(JobStatus jobStatus) {
    switch (jobStatus.getRunState()) {
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
        throw new RuntimeException("Unknown job status: " +  jobStatus);
      }
  }

  protected void closeIfComplete(boolean closeFromWebUI) throws IOException {
    // Prevent multiple simultaneous executions of this function. We could have
    // the Web UI and JobSubmissionProtocol.killJob() call this, for example.
    synchronized(closeLock) {
      if (this.job.getStatus().isJobComplete()) {
        if (running) {
          running = false;
          close(closeFromWebUI);
        }
      }
    }
  }

  void close(boolean closeFromWebUI) throws IOException {
    try {
      jobHistory.markCompleted();
    } catch (IOException ioe) {
      LOG.warn("Failed to mark job " + jobId + " as completed!", ioe);
    }
    jobHistory.shutdown();

    if (sessionDriver != null) {
      sessionDriver.stop(jobToSessionStatus(job.getStatus()));
    }

    if (interTrackerServer != null) {
      interTrackerServer.stop();
    }
    if (expireLaunchingTasksThread != null) {
      expireLaunchingTasksThread.interrupt();
      try {
        expireLaunchingTasksThread.join();
      } catch (InterruptedException e) {}
      expireLaunchingTasksThread = null;
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

    // to stop the taskLauncher thread - we queue some additional actions and wake it up
    // the taskLauncher will wakeup, dispatch those actions and then terminate (because
    // running = false. if there are no actions, interrupt the launcher thread.
    synchronized (actionsToSend) {
      for(org.apache.hadoop.corona.InetAddress addr: resourceTracker.allTrackers()) {
        LOG.info("Sending kill job to " + addr.host + ":" + addr.port);
        ActionToSend action = new ActionToSend(addr.host, addr.port, new KillJobAction(this.jobId));
        actionsToSend.add(action);
      }
      if (actionsToSend.size() > 0) {
        actionsToSend.notify();
      } else {
        if (taskLauncherThread != null) {
          taskLauncherThread.interrupt();
        }
      }
    }

    if (infoServer != null) {
      if (closeFromWebUI) {
        // If we are being called from the web UI, this function is executing in a
        // web-server thread. Give some time to the web-server to clean up.
        infoServer.setGracefulShutdown(1000);
      }
      try {
        infoServer.stop();
      } catch (Exception ex) {
        LOG.warn("Exception shutting down web server ", ex);
      }
    }
  }

  class AssignTasksThread implements Runnable {
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

  @Override
  public boolean processAvailableResource(ResourceGrant grant) {
    org.apache.hadoop.corona.InetAddress addr =
      Utilities.appInfoToAddress(grant.appInfo);
    String trackerName = grant.getNodeName();

    Task task = getSetupAndCleanupTasks(trackerName, addr.host);
    if (task == null) {
      TaskInProgress tip = resourceTracker.findTipForGrant(grant);
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
      expireLaunchingTasks.addNewTask(task.getTaskID());
      queueTaskForLaunch(task, trackerName, addr);
      return true;
    }
    return false;
  }
  
  @Override
  public boolean isBadResource(ResourceGrant grant, TaskInProgress tip) {
    org.apache.hadoop.corona.InetAddress addr = grant.address;
    String trackerName = grant.getNodeName();
    return !job.canTrackerBeUsed(trackerName, addr.host, tip);
  }
  
  /**
   * One iteration of core logic.
   */
  void assignTasks() throws InterruptedException {
    resourceTracker.processAvailableGrants(this);
  }

  void processGrantsToRevoke() {
    Map<Integer, TaskAttemptID> processed = new HashMap<Integer, TaskAttemptID>();
    synchronized(lockObject) {
      for (ResourceGrant grant: grantsToRevoke) {
        TaskAttemptID attemptId = taskLookupTable.taskForGrant(grant);
        if (attemptId != null) {
          boolean shouldFail = false;
          boolean killed = killTaskUnprotected(attemptId, shouldFail);
          processed.put(grant.getId(), attemptId);
          // Grant will get removed from the resource tracker
          // when the kill takes effect and we get a response from TT.
          queueKillActions(grant.getNodeName());
          taskLookupTable.removeTaskEntry(attemptId);
        }
      }
    }
    for (Map.Entry<Integer, TaskAttemptID> entry: processed.entrySet()) {
      LOG.info("Revoking resource " + entry.getKey() +
               " task: " + entry.getValue());
    }
  }

  void queueTaskForLaunch(Task task, String trackerName,
      org.apache.hadoop.corona.InetAddress addr) {
    CoronaSessionInfo info =
      new CoronaSessionInfo(sessionId, jobTrackerAddress);
    TaskTrackerAction action = new LaunchTaskAction(task, info);
    ActionToSend actionToSend =
      new ActionToSend(addr.host, addr.port, action);
    LOG.info("Queueing a launch task action for " + trackerName + "(" +
      addr.host + ":" + addr.port);
    synchronized(actionsToSend) {
      actionsToSend.add(actionToSend);
      actionsToSend.notify();
    }
  }

  private void queueKillActions(String trackerName) {
    List<TaskTrackerAction> killActions =
      taskLookupTable.getTasksToKill(trackerName);
    org.apache.hadoop.corona.InetAddress addr =
      resourceTracker.getTrackerAddr(trackerName);
    synchronized(actionsToSend) {
      for (TaskTrackerAction killAction: killActions) {
        ActionToSend actionToSend =
          new ActionToSend(addr.host, addr.port, killAction);
        actionsToSend.add(actionToSend);
      }
      actionsToSend.notify();
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
      return (CoronaTaskTrackerProtocol) RPC.waitForProxy(
            CoronaTaskTrackerProtocol.class,
            CoronaTaskTrackerProtocol.versionID, s, conf);
    }

    public synchronized void clearClient(InetSocketAddress s) {
      CoronaTaskTrackerProtocol client = trackerClients.get(s);
      if (client != null) {
        trackerClients.remove(s);
      }
    }
  }

  class TaskLauncherThread implements Runnable {
    public void run() {
      while (running) {
        try {
          launchTasks();
        } catch (InterruptedException e) {
          // ignore, check running flag for termination
        } catch (Throwable t) {
          LOG.fatal("LaunchTaskThread dying because of " +
                    StringUtils.stringifyException(t));
          return;
        }
      }
      LOG.info("Terminating TaskLauncher thread");
    }
  }

  void launchTasks() throws InterruptedException {
    List<ActionToSend> actions = new ArrayList<ActionToSend>();
    synchronized(actionsToSend) {
      while (actionsToSend.isEmpty()) {
        actionsToSend.wait();
      }
      actions.addAll(actionsToSend);
      actionsToSend.clear();
    }
    for (ActionToSend actionToSend: actions) {
      // Get the tracker address.
      InetSocketAddress trackerAddress =
        new InetSocketAddress(actionToSend.trackerHost, actionToSend.port);
      // Fill in the job tracker information.
      CoronaSessionInfo info = new CoronaSessionInfo(sessionId, jobTrackerAddress);
      actionToSend.action.setExtensible(info);

      try {
        CoronaTaskTrackerProtocol client =
          trackerClientCache.getClient(trackerAddress);
        client.submitActions(new TaskTrackerAction[]{actionToSend.action});
      } catch (IOException e) {
        LOG.error("Could not send " + actionToSend.action.getClass() +
          " action to " + actionToSend.trackerHost, e);
        trackerClientCache.resetClient(trackerAddress);
        if (actionToSend.action instanceof LaunchTaskAction) {
          LaunchTaskAction launchTaskAction = (LaunchTaskAction) actionToSend.action;
          TaskAttemptID attempt = launchTaskAction.getTask().getTaskID();
          expireLaunchingTasks.failedLaunch(attempt);
        }
      }
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
 
    public void run() {
      while (running) {
        try {
          waitToBeNotified();
          updateResources();
        } catch (InterruptedException ie) {
          // ignore. if shutting down, while cond. will catch it
        } catch (Exception e) {
          LOG.error("Resource Updater Thread got exception: ", e);
        }
      }
    }

    public void updateResources() throws IOException {
      if (job == null) return;
      processGrantsToRevoke();

      // Update resource requests based on speculation.
      if (job.getStatus().getRunState() == JobStatus.RUNNING) {
        job.updateSpeculationRequests();
      }

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
    }
  }

  Task getSetupAndCleanupTasks(String taskTrackerName, String hostName) {
    Task t = null;
    t = job.obtainJobCleanupTask(taskTrackerName, hostName, true);
    if (t == null) {
      t = job.obtainTaskCleanupTask(taskTrackerName, true);
    }
    
    if (t == null) {
      t = job.obtainJobSetupTask(taskTrackerName, hostName, true);
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

      processFetchFailures(report);
    }
  }

  private void processFetchFailures(TaskStatus taskStatus) {
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
          ((CoronaJobInProgress)failedFetchMap.getJob()).fetchFailureNotification(
            reportingAttempt, failedFetchMap, mapTaskId, failedFetchTrackerName);
        } else {
          LOG.warn("Could not find TIP for " + failedFetchMap);
        }
      }
    }
  }

  /**
   * A tracker wants to know if any of its Tasks can be committed
   */
  List<ActionToSend> getCommitActionsToSend(TaskTrackerStatus tts) {
    synchronized(lockObject) {
      List<ActionToSend> saveList = new ArrayList<ActionToSend>();
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
                TaskTrackerAction commitAction = new CommitTaskAction(taskId);
                ActionToSend commitActionToSend = new ActionToSend(
                    addr.getHost(), addr.getPort(), commitAction);
                saveList.add(commitActionToSend);
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
    if (!this.jobId.equals(jobId))
      throw new RuntimeException("JobId " + jobId + " does not match the expected id of: " + this.jobId);

    return new CoronaJobInProgress(lockObject, jobId, new Path(getSystemDir()),
                                   defaultConf, taskLookupTable, resourceTracker, jobHistory, getUrl());
  }

  JobStatus startJob(CoronaJobInProgress jip, SessionDriver driver)
      throws IOException {
    synchronized(lockObject) {
      this.job = jip;
    }
    if (job.isJobEmpty()) {
      job.completeEmptyJob();
    } else if (!job.isSetupCleanupRequired()) {
      job.completeSetup();
    }
    
    resourceUpdater.notifyThread();

    return job.getStatus();
  }

  CoronaJobInProgress getJob() {
    return job;
  }

  public JobInProgressTraits getJobInProgress(JobID jobId) {
    if (!this.jobId.equals(jobId))
      throw new RuntimeException("JobId " + jobId + " does not match the expected id of: " + this.jobId);

    return (JobInProgressTraits)this.job;
  }


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
    if (!this.jobId.equals(jobId))
      throw new RuntimeException("JobId " + jobId + " does not match the expected id of: " + this.jobId);

    LOG.info("Killing job from Web UI " + jobId);
    job.kill();
    closeIfComplete(true);
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
    CoronaJobInProgress jip = createJob(jobId, jobConf);
    if (sessionDriver != null) {
      sessionDriver.setName(jobConf.getJobName());
    }
    jip.initTasks();
    return startJob(jip, sessionDriver);
  }

  @Override
  public ClusterStatus getClusterStatus(boolean detailed) throws IOException {
    // TODO
    return null;
  }

  @Override
  public void killJob(JobID jobId) throws IOException {
    if (!this.jobId.equals(jobId))
      throw new RuntimeException("JobId " + jobId + " does not match the expected id of: " + this.jobId);

    LOG.info("Killing job " + jobId);
    job.kill();
    closeIfComplete(false);
  }

  @Override
  public void setJobPriority(JobID jobId, String priority) throws IOException {
    if (!this.jobId.equals(jobId))
      throw new IOException("JobId " + jobId + " does not match the expected id of: " + this.jobId);

    throw new UnsupportedOperationException(
      "Changing job priority in CoronaJobTracker is not supported");
  }

  @Override
  public boolean killTask(TaskAttemptID taskId, boolean shouldFail)
      throws IOException {
    synchronized(lockObject) {
      return killTaskUnprotected(taskId, shouldFail);
    }
  }

  private boolean killTaskUnprotected(TaskAttemptID taskId, boolean shouldFail) {
    TaskInProgress tip = taskLookupTable.getTIP(taskId);
    return tip.killTask(taskId, shouldFail,
        "Request received to " + (shouldFail ? "fail" : "kill") 
        + " task '" + taskId + "' by user" );
  }

  @Override
  public JobProfile getJobProfile(JobID jobId) throws IOException {
    if (!this.jobId.equals(jobId)) {
      return null;
    } else {
      return this.job.getProfile();
    }
  }

  @Override
  public JobStatus getJobStatus(JobID jobId) throws IOException {
    if (!this.jobId.equals(jobId)) {
      return null;
    } else {
      return this.job.getStatus();
    }
  }

  @Override
  public Counters getJobCounters(JobID jobId) throws IOException {
    if (!this.jobId.equals(jobId)) {
      return null;
    } else {
      return this.job.getCounters();
    }
  }


  @Override
  public String getFilesystemName() throws IOException {
    // TODO:
    return null;
  }

  @Override
  public JobStatus[] jobsToComplete() { return null; }

  @Override
  public JobStatus[] getAllJobs() { return null; }

  @Override
  public TaskCompletionEvent[] getTaskCompletionEvents(JobID jobid
      , int fromEventId, int maxEvents) {
    if (!this.jobId.equals(jobId)) {
      return TaskCompletionEvent.EMPTY_ARRAY;
    } else {
      return job.getTaskCompletionEvents(fromEventId, maxEvents);
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
    List<ActionToSend> commitActionsToSend = getCommitActionsToSend(status);
    if (commitActionsToSend.size() > 0) {
      synchronized(actionsToSend) {
        actionsToSend.addAll(commitActionsToSend);
        actionsToSend.notify();
      }
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

  public TaskReport[] getMapTaskReports(JobID jobId) {
    if (!this.jobId.equals(jobId))
      throw new RuntimeException("JobId " + jobId + " does not match the expected id of: " + this.jobId);

    synchronized(lockObject) {
      return super.getMapTaskReportsImpl(jobId);
    }
  }

  public TaskReport[] getReduceTaskReports(JobID jobId) {
    if (!this.jobId.equals(jobId))
      throw new RuntimeException("JobId " + jobId + " does not match the expected id of: " + this.jobId);

    synchronized(lockObject) {
      return super.getReduceTaskReportsImpl(jobId);
    }
  }

  public TaskReport[] getCleanupTaskReports(JobID jobId) {
    if (!this.jobId.equals(jobId))
      throw new RuntimeException("JobId " + jobId + " does not match the expected id of: " + this.jobId);

    synchronized(lockObject) {
      return super.getCleanupTaskReportsImpl(jobId);
    }
  }

  public TaskReport[] getSetupTaskReports(JobID jobId) {
    if (!this.jobId.equals(jobId))
      throw new RuntimeException("JobId " + jobId + " does not match the expected id of: " + this.jobId);

    synchronized(lockObject) {
      return super.getSetupTaskReportsImpl(jobId);
    }
  }

  public String[] getTaskDiagnostics(TaskAttemptID taskId)
    throws IOException {
    synchronized(lockObject) {
      return super.getTaskDiagnosticsImpl(taskId);
    }
  }

  public String getProxyUrl(String relativeUrl) {
    return getProxyUrl(conf, relativeUrl);
  }

  public String getProxyJTAddr() {
    return getProxyJTAddr(conf);
  }

  public static String getProxyJTAddr(Configuration conf) {
    return conf.get("mapred.job.tracker.corona.proxyaddr", null);
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
}
