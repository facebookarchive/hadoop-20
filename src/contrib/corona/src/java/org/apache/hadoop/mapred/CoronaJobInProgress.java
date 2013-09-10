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

import java.io.DataInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.Vector;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.corona.SessionPriority;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapred.TaskStatus;
import org.apache.hadoop.mapred.JobHistory.Values;
import org.apache.hadoop.mapred.JobInProgress.Counter;
import org.apache.hadoop.mapred.TaskStatus.Phase;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.net.NetworkTopology;
import org.apache.hadoop.net.Node;
import org.apache.hadoop.net.TopologyCache;
import org.apache.hadoop.util.StringUtils;


public class CoronaJobInProgress extends JobInProgressTraits {
  static final Log LOG = LogFactory.getLog(CoronaJobInProgress.class);

  Clock clock;

  @SuppressWarnings("deprecation")
  JobID jobId;
  SessionPriority priority;
  JobProfile profile;
  JobStatus status;
  private final JobStats jobStats = new JobStats();
  long startTime;
  long launchTime;
  long finishTime;
  long deadline;

  String user;
  @SuppressWarnings("deprecation")
  JobConf jobConf;

  Path jobFile;  // non-local
  Path localJobFile;

  // XXX: Do not limit number of tasks ourselves. Let Cluster Manager handle it.
  int maxTasks = 0;
  int numMapTasks;
  int numReduceTasks;
  long memoryPerMap;
  long memoryPerReduce;
  volatile int numSlotsPerMap = 1;
  volatile int numSlotsPerReduce = 1;
  List<TaskCompletionEvent> taskCompletionEvents;
  private int taskCompletionEventCounter = 0;
  static final float DEFAULT_COMPLETED_MAPS_PERCENT_FOR_REDUCE_SLOWSTART = 0.05f;
  public static final String SPECULATIVE_MAP_UNFINISHED_THRESHOLD_KEY =
      "mapred.map.tasks.speculation.unfinished.threshold";
  public static final String SPECULATIVE_REDUCE_UNFINISHED_THRESHOLD_KEY =
      "mapred.reduce.tasks.speculation.unfinished.threshold";

  static final int NUM_SLOTS_PER_MAP = 1;
  static final int NUM_SLOTS_PER_REDUCE = 1;

  private final boolean jobSetupCleanupNeeded;
  private final boolean jobFinishWhenReducesDone;
  private final boolean taskCleanupNeeded;
  private volatile boolean launchedSetup = false;
  private volatile boolean tasksInited = false;
  private final AtomicBoolean reduceResourcesRequested = new AtomicBoolean(false);
  private volatile boolean launchedCleanup = false;
  private volatile boolean jobKilled = false;
  private volatile boolean jobFailed = false;

  String[][] mapLocations;  // Has an array of locations for each map.
  List<TaskInProgress> nonRunningMaps;
  List<TaskInProgress> nonRunningReduces = new LinkedList<TaskInProgress>();
  int runningMapTasks = 0;
  int runningReduceTasks = 0;
  int failedMapTasks = 0;  // includes killed
  int failedReduceTasks = 0;  // includes killed
  int killedMapTasks = 0;
  int killedReduceTasks = 0;
  int failedMapTIPs = 0;
  int failedReduceTIPs = 0;
  int finishedMapTasks = 0;
  int finishedReduceTasks = 0;
  int completedMapsForReduceSlowstart = 0;
  int rushReduceReduces = 5;
  int rushReduceMaps = 5;
  float speculativeMapUnfininshedThreshold = 0.001F;
  float speculativeReduceUnfininshedThreshold = 0.001F;
  float speculativeMapLogRateThreshold = 0.001F;
  int speculativeMapLogNumThreshold = 3;
  float speculativeReduceLogRateThreshold = 0.001F;
  int speculativeReduceLogNumThreshold = 3;

  Set<TaskInProgress> runningMaps = new LinkedHashSet<TaskInProgress>();
  Set<TaskInProgress> runningReduces = new LinkedHashSet<TaskInProgress>();
  @SuppressWarnings("deprecation")
  List<TaskAttemptID> mapCleanupTasks = new LinkedList<TaskAttemptID>();
  @SuppressWarnings("deprecation")
  List<TaskAttemptID> reduceCleanupTasks = new LinkedList<TaskAttemptID>();

  int maxLevel;
  int anyCacheLevel;
  private final LocalityStats localityStats;
  private final Thread localityStatsThread;

  // runningMapTasks include speculative tasks, so we need to capture
  // speculative tasks separately
  int speculativeMapTasks = 0;
  int speculativeReduceTasks = 0;

  int mapFailuresPercent = 0;
  int reduceFailuresPercent = 0;
  @SuppressWarnings("deprecation")
  Counters jobCounters = new Counters();

  TaskErrorCollector taskErrorCollector;

  // Maximum no. of fetch-failure notifications after which
  // the map task is killed
  private static final int MAX_FETCH_FAILURES_NOTIFICATIONS = 3;
  private static final int MAX_FETCH_FAILURES_PER_MAP_DEFAULT = 50;
  private static final String MAX_FETCH_FAILURES_PER_MAP_KEY =
     "mapred.job.per.map.maxfetchfailures";
  private int maxFetchFailuresPerMapper;

  // The key for the property holding the job deadline value
  private static final String JOB_DEADLINE_KEY = "mapred.job.deadline";
  // The default value of the job deadline property
  private static final long JOB_DEADLINE_DEFAULT_VALUE = 0L;
  // The key for the property holding the job priority
  private static final String SESSION_PRIORITY_KEY = "mapred.job.priority";
  // The default value of the job priority
  private static final String SESSION_PRIORITY_DEFAULT = "NORMAL";
  // The maximum percentage of fetch failures allowed for a map
  private static final double MAX_ALLOWED_FETCH_FAILURES_PERCENT = 0.5;
  // Map of mapTaskId -> no. of fetch failures
  private final Map<TaskAttemptID, Integer> mapTaskIdToFetchFailuresMap =
    new TreeMap<TaskAttemptID, Integer>();

  // Don't lower speculativeCap below one TT's worth (for small clusters)
  private static final int MIN_SPEC_CAP = 10;
  public static final String SPECULATIVE_SLOWTASK_THRESHOLD =
    "mapreduce.job.speculative.slowtaskthreshold";
  public static final String RUSH_REDUCER_MAP_THRESHOLD =
    "mapred.job.rushreduce.map.threshold";
  public static final String RUSH_REDUCER_REDUCE_THRESHOLD =
    "mapred.job.rushreduce.reduce.threshold";
  public static final String SPECULATIVECAP =
    "mapreduce.job.speculative.speculativecap";
  public static final String SPECULATIVE_SLOWNODE_THRESHOLD =
    "mapreduce.job.speculative.slownodethreshold";
  public static final String SPECULATIVE_REFRESH_TIMEOUT =
    "mapreduce.job.speculative.refresh.timeout";
  public static final String LOG_CANNOTSPECULATIVE_INTERVAL =
    "mapreduce.job.log.cannotspeculative.interval";
  public static final String SPECULATIVE_STDDEVMEANRATIO_MAX =
    "mapreduce.job.speculative.stddevmeanratio.max";

  // thresholds for speculative execution
  float slowTaskThreshold;
  float speculativeCap;
  float slowNodeThreshold;
  private long lastSpeculativeMapRefresh, lastSpeculativeReduceRefresh;
  private long lastTimeCannotspeculativeMapLog, lastTimeCannotspeculativeReduceLog;
  private final long speculativeRefreshTimeout;
  private final long logCannotspeculativeInterval;
  private final float speculativeStddevMeanRatioMax;
  volatile private boolean hasSpeculativeMaps;
  volatile private boolean hasSpeculativeReduces;
  private List<TaskInProgress> candidateSpeculativeMaps, candidateSpeculativeReduces;
  //Statistics are maintained for a couple of things
  //mapTaskStats is used for maintaining statistics about
  //the completion time of map tasks on the trackers. On a per
  //tracker basis, the mean time for task completion is maintained
  private final DataStatistics mapTaskStats = new DataStatistics();
  //reduceTaskStats is used for maintaining statistics about
  //the completion time of reduce tasks on the trackers. On a per
  //tracker basis, the mean time for task completion is maintained
  private final DataStatistics reduceTaskStats = new DataStatistics();
  //trackerMapStats used to maintain a mapping from the tracker to the
  //the statistics about completion time of map tasks
  private final Map<String,DataStatistics> trackerMapStats =
    new HashMap<String,DataStatistics>();
  //trackerReduceStats used to maintain a mapping from the tracker to the
  //the statistics about completion time of reduce tasks
  private final Map<String,DataStatistics> trackerReduceStats =
    new HashMap<String,DataStatistics>();
  //runningMapStats used to maintain the RUNNING map tasks' statistics
  private final DataStatistics runningMapTaskStats = new DataStatistics();
  //runningReduceStats used to maintain the RUNNING reduce tasks' statistics
  private final DataStatistics runningReduceTaskStats = new DataStatistics();
  private static final String JOB_KILLED_REASON = "Job killed";
  private static final String EMPTY_TRACKER_NAME =
    "tracker_:localhost.localdomain/127.0.0.1:";


  protected CoronaJobTracker.TaskLookupTable taskLookupTable;
  private final TaskStateChangeListener taskStateChangeListener;

  private final Object lockObject;
  private final CoronaJobHistory jobHistory;
  
  private String jobTrackerId;

  private int terminated = -1;


  @SuppressWarnings("deprecation")
  public CoronaJobInProgress(
    Object lockObject,
    JobID jobId, Path systemDir, JobConf jobConf,
    CoronaJobTracker.TaskLookupTable taskLookupTable,
    TaskStateChangeListener taskStateChangeListener,
    TopologyCache topologyCache,
    CoronaJobHistory jobHistory, String url, String jobTrackerId)
    throws IOException {
    this.lockObject = lockObject;
    this.clock = JobTracker.getClock();

    this.jobId = jobId;
    this.jobConf = jobConf;
    this.taskLookupTable = taskLookupTable;
    this.taskStateChangeListener = taskStateChangeListener;
    this.jobHistory = jobHistory;
    this.jobTrackerId = jobTrackerId;

    // Status.
    this.startTime = clock.getTime();
    this.status = new JobStatus(jobId, 0.0f, 0.0f, JobStatus.PREP);
    status.setStartTime(startTime);

    // Job file.
    this.jobFile = getJobFile(systemDir, jobId);

    this.user = jobConf.getUser();
    this.profile = new JobProfile(
      user, jobId, jobFile.toString(), url, jobConf.getJobName(),
      jobConf.getQueueName());


    this.numMapTasks = jobConf.getNumMapTasks();
    this.numReduceTasks = jobConf.getNumReduceTasks();
    this.memoryPerMap = jobConf.getMemoryForMapTask();
    this.memoryPerReduce = jobConf.getMemoryForReduceTask();
    this.taskCompletionEvents = new ArrayList<TaskCompletionEvent>
       (numMapTasks + numReduceTasks + 10);
    this.jobSetupCleanupNeeded = jobConf.getJobSetupCleanupNeeded();
    this.jobFinishWhenReducesDone = jobConf.getJobFinishWhenReducesDone();
    this.taskCleanupNeeded = jobConf.getTaskCleanupNeeded();

    this.mapFailuresPercent = jobConf.getMaxMapTaskFailuresPercent();
    this.reduceFailuresPercent = jobConf.getMaxReduceTaskFailuresPercent();

    this.maxLevel = jobConf.getInt("mapred.task.cache.levels",
        NetworkTopology.DEFAULT_HOST_LEVEL);
    this.anyCacheLevel = this.maxLevel+1;
    this.localityStats = new LocalityStats(
      jobConf, maxLevel, jobCounters, jobStats, topologyCache);
    localityStatsThread = new Thread(localityStats);
    localityStatsThread.setName("Locality Stats");
    localityStatsThread.setDaemon(true);
    localityStatsThread.start();

    this.taskErrorCollector = new TaskErrorCollector(
      jobConf, Integer.MAX_VALUE, 1);

    this.slowTaskThreshold = Math.max(0.0f,
        jobConf.getFloat(CoronaJobInProgress.SPECULATIVE_SLOWTASK_THRESHOLD,1.0f));
    this.speculativeCap = jobConf.getFloat(
        CoronaJobInProgress.SPECULATIVECAP,0.1f);
    this.slowNodeThreshold = jobConf.getFloat(
        CoronaJobInProgress.SPECULATIVE_SLOWNODE_THRESHOLD,1.0f);
    this.speculativeRefreshTimeout = jobConf.getLong(
        CoronaJobInProgress.SPECULATIVE_REFRESH_TIMEOUT, 5000L);
    this.logCannotspeculativeInterval = jobConf.getLong(
        CoronaJobInProgress.LOG_CANNOTSPECULATIVE_INTERVAL, 150000L);
    this.speculativeStddevMeanRatioMax = jobConf.getFloat(
        CoronaJobInProgress.SPECULATIVE_STDDEVMEANRATIO_MAX, 0.33f);

    this.speculativeMapUnfininshedThreshold = jobConf.getFloat(
        CoronaJobInProgress.SPECULATIVE_MAP_UNFINISHED_THRESHOLD_KEY,
        speculativeMapUnfininshedThreshold);
    this.speculativeReduceUnfininshedThreshold = jobConf.getFloat(
        CoronaJobInProgress.SPECULATIVE_REDUCE_UNFINISHED_THRESHOLD_KEY,
        speculativeReduceUnfininshedThreshold);
    this.deadline = jobConf.getLong(JOB_DEADLINE_KEY,
        JOB_DEADLINE_DEFAULT_VALUE);


    this.priority = SessionPriority.valueOf(
        jobConf.get(SESSION_PRIORITY_KEY, SESSION_PRIORITY_DEFAULT));
    hasSpeculativeMaps = jobConf.getMapSpeculativeExecution();
    hasSpeculativeReduces = jobConf.getReduceSpeculativeExecution();
    LOG.info(jobId + ": hasSpeculativeMaps = " + hasSpeculativeMaps +
             ", hasSpeculativeReduces = " + hasSpeculativeReduces);
  }

  public JobStats getJobStats() {
    return jobStats;
  }

  @Override
  public String getUser() { return user; }

  public JobProfile getProfile() {  return profile; }

  @Override
  public JobStatus getStatus() { return status; }

  public boolean isSetupCleanupRequired() { return jobSetupCleanupNeeded; }

  public SessionPriority getPriority() {
    return this.priority;
  }
  
  public void setPriority(SessionPriority priority) {
    this.priority = priority;
  }

  @Override
  DataStatistics getRunningTaskStatistics(boolean isMap) {
    if (isMap) {
      return runningMapTaskStats;
    } else {
      return runningReduceTaskStats;
    }
  }

  @Override
  public float getSlowTaskThreshold() { return slowTaskThreshold; }

  @Override
  public float getStddevMeanRatioMax() { return speculativeStddevMeanRatioMax; }

  @Override
  public int getNumRestarts() { return 0; }

  public long getLaunchTime() { return launchTime; }

  public long getStartTime() { return startTime; }

  public long getFinishTime() { return finishTime; }

  public long getJobDeadline() { return deadline; }

  public int getNumMapTasks() { return numMapTasks; }

  public int getNumReduceTasks() { return numReduceTasks; }

  @SuppressWarnings("deprecation")
  public Counters getJobCounters() { return jobCounters; }

  /**
   *  Returns map phase counters by summing over all map tasks in progress.
   */
  @SuppressWarnings("deprecation")
  public Counters getMapCounters() {
    synchronized (lockObject) {
      return incrementTaskCountersUnprotected(new Counters(), maps);
    }
  }

  /**
   *  Returns map phase counters by summing over all map tasks in progress.
   */
  @SuppressWarnings("deprecation")
  public Counters getReduceCounters() {
    synchronized (lockObject) {
      return incrementTaskCountersUnprotected(new Counters(), reduces);
    }
  }

  /**
   * Get all the tasks of the desired type in this job.
   */
  TaskInProgress[] getTasks(TaskType type) {
    TaskInProgress[] tasks = null;
    switch (type) {
    case MAP:
      tasks = maps;
      break;
    case REDUCE:
      tasks = reduces;
      break;
    case JOB_SETUP:
      tasks = setup;
      break;
    case JOB_CLEANUP:
      tasks = cleanup;
      break;
    default:
      tasks = new TaskInProgress[0];
      break;
    }

    return tasks;
  }

  public TaskCompletionEvent[] getTaskCompletionEvents(
        int fromEventId, int maxEvents) {
    TaskCompletionEvent[] events = TaskCompletionEvent.EMPTY_ARRAY;
    synchronized (lockObject) {
      if (!tasksInited) {
        return events;
      }
      if (taskCompletionEvents.size() > fromEventId) {
        int actualMax = Math.min(maxEvents,
                                 (taskCompletionEvents.size() - fromEventId));
        events = taskCompletionEvents.subList(fromEventId,
          actualMax + fromEventId).toArray(events);
      }
      return events;
    }
  }

  public int getTaskCompletionEventsSize() {
    synchronized (lockObject) {
      return taskCompletionEvents.size();
    }
  }

  boolean fetchFailureNotification(
        TaskAttemptID reportingAttempt,
        TaskInProgress tip, TaskAttemptID mapAttemptId, String trackerName) {
    jobStats.incNumMapFetchFailures();
    synchronized (lockObject) {
      Integer fetchFailures = mapTaskIdToFetchFailuresMap.get(mapAttemptId);
      fetchFailures = (fetchFailures == null) ? 1 : (fetchFailures + 1);
      mapTaskIdToFetchFailuresMap.put(mapAttemptId, fetchFailures);
      LOG.info("Failed fetch notification #" + fetchFailures + " by " + reportingAttempt +
          " for task " + mapAttemptId + " tracker " + trackerName);

      float failureRate = (float)fetchFailures / runningReduceTasks;
      // declare faulty if fetch-failures >= max-allowed-failures
      final boolean isMapFaulty = (failureRate >= MAX_ALLOWED_FETCH_FAILURES_PERCENT) ||
          fetchFailures > maxFetchFailuresPerMapper;
      if (fetchFailures >= MAX_FETCH_FAILURES_NOTIFICATIONS && isMapFaulty) {
        String reason = "Too many fetch-failures (" + fetchFailures + ") at " +
          new Date();
        LOG.info(reason + " for " + mapAttemptId + " ... killing it");

        final boolean isFailed = true;
        TaskTrackerInfo ttStatus = null;
        jobStats.incNumMapTasksFailedByFetchFailures();
        failedTask(tip, mapAttemptId, reason,
            (tip.isMapTask() ? TaskStatus.Phase.MAP : TaskStatus.Phase.REDUCE),
            isFailed, trackerName, ttStatus);

        mapTaskIdToFetchFailuresMap.remove(mapAttemptId);
        return true;
      }
    }
    return false;
  }

  @SuppressWarnings("deprecation")
  public static Path getJobFile(Path systemDir, JobID jobId) {
    Path systemDirForJob = new Path(systemDir, jobId.toString());
    return new Path(systemDirForJob, "job.xml");
  }

  public int getMaxTasksPerJob() {
    return 0; // No maximum.
  }

  public void close() {
    localityStats.stop();
    localityStatsThread.interrupt();
    // The thread may be stuck in DNS lookups. This thread is a daemon,
    // so it will not prevent process exit.
    try {
      localityStatsThread.join(1000);
    } catch (InterruptedException e) {
      LOG.warn("localityStatsThread.join interrupted");
    }
  }

  /**
   * Read input splits and create a map per split.
   */
  public void initTasks() throws IOException {
    // log job info
    jobHistory.logSubmitted(jobFile.toString(), this.startTime, this.jobTrackerId);
    // log the job priority
    JobClient.RawSplit[] splits = null;
    splits = JobClient.getAndRemoveCachedSplits(jobId);
    if (splits == null) {
      FileSystem fs = jobFile.getFileSystem(jobConf);
      Path splitFile = new Path(jobFile.getParent(), "job.split");
      LOG.info("Reading splits from " + splitFile);
      DataInputStream splitFileIn = fs.open(splitFile);
      try {
        splits = JobClient.readSplitFile(splitFileIn);
      } finally {
        splitFileIn.close();
      }
    }
    initTasksFromSplits(splits);
    jobHistory.logInited(this.launchTime,
                         numMapTasks, numReduceTasks);
  }

  /**
   * Used by test code.
   */
  void initTasksFromSplits(JobClient.RawSplit[] splits) throws IOException {
    synchronized (lockObject) {
      initTasksFromSplitsUnprotected(splits);
    }
  }

  private void initTasksFromSplitsUnprotected(JobClient.RawSplit[] splits)
      throws IOException {
    String jobFile = profile.getJobFile();

    numMapTasks = splits.length;
    if (maxTasks > 0 && numMapTasks + numReduceTasks > maxTasks) {
      throw new IOException(
                "The number of tasks for this job " +
                (numMapTasks + numReduceTasks) +
                " exceeds the configured limit " + maxTasks);
    }

    long inputLength = 0;
    maps = new TaskInProgress[numMapTasks];
    mapLocations = new String[numMapTasks][];
    nonRunningMaps = new ArrayList<TaskInProgress>(numMapTasks);
    for(int i = 0; i < numMapTasks; i++) {
      inputLength += splits[i].getDataLength();
      maps[i] = new TaskInProgress(jobId, jobFile, splits[i],
                                 jobConf, this, i, 1); // numSlotsPerMap = 1
      nonRunningMaps.add(maps[i]);
      mapLocations[i] = splits[i].getLocations();
    }
    LOG.info("Input size for job " + jobId + " = " + inputLength
        + ". Number of splits = " + splits.length);

    this.launchTime = clock.getTime();
    LOG.info("Number of splits for job " + jobId + " = " + splits.length);

    // Create reduce tasks
    this.reduces = new TaskInProgress[numReduceTasks];
    for (int i = 0; i < numReduceTasks; i++) {
      reduces[i] = new TaskInProgress(jobId, jobFile, numMapTasks, i,
                                  jobConf, this, 1); // numSlotsPerReduce = 1
      nonRunningReduces.add(reduces[i]);
    }
    LOG.info("Number of reduces for job " + jobId + " = " + reduces.length);

    // Calculate the minimum number of maps to be complete before
    // we should start scheduling reduces
    completedMapsForReduceSlowstart =
      (int)Math.ceil(
          (jobConf.getFloat("mapred.reduce.slowstart.completed.maps",
                         DEFAULT_COMPLETED_MAPS_PERCENT_FOR_REDUCE_SLOWSTART) *
           numMapTasks));
    // The thresholds of total maps and reduces for scheduling reducers
    // immediately.
    rushReduceMaps =
      jobConf.getInt(RUSH_REDUCER_MAP_THRESHOLD, rushReduceMaps);
    rushReduceReduces =
      jobConf.getInt(RUSH_REDUCER_REDUCE_THRESHOLD, rushReduceReduces);
    maxFetchFailuresPerMapper = jobConf.getInt(MAX_FETCH_FAILURES_PER_MAP_KEY,
      MAX_FETCH_FAILURES_PER_MAP_DEFAULT);

    // Proceed to Setup/Cleanup.
    if (jobSetupCleanupNeeded) {
      // create cleanup two cleanup tips, one map and one reduce.
      cleanup = new TaskInProgress[2];

      // cleanup map tip. This map doesn't use any splits. Just assign an empty
      // split.
      JobClient.RawSplit emptySplit = new JobClient.RawSplit();
      cleanup[0] = new TaskInProgress(jobId, jobFile, emptySplit,
              jobConf, this, numMapTasks, 1);
      cleanup[0].setJobCleanupTask();

      // cleanup reduce tip.
      cleanup[1] = new TaskInProgress(jobId, jobFile, numMapTasks,
                         numReduceTasks, jobConf, this, 1);
      cleanup[1].setJobCleanupTask();

      // create two setup tips, one map and one reduce.
      setup = new TaskInProgress[2];

      // setup map tip. This map doesn't use any split. Just assign an empty
      // split.
      setup[0] = new TaskInProgress(jobId, jobFile, emptySplit,
              jobConf, this, numMapTasks + 1, 1);
      setup[0].setJobSetupTask();

      // setup reduce tip.
      setup[1] = new TaskInProgress(jobId, jobFile, numMapTasks,
                         numReduceTasks + 1, jobConf, this, 1);
      setup[1].setJobSetupTask();
    }
    tasksInited = true;
  }

  boolean scheduleReducesUnprotected() {
    // Start scheduling reducers if we have enough maps finished or
    // if the job has very few mappers or reducers.
    return numMapTasks <= rushReduceMaps ||
           numReduceTasks <= rushReduceReduces ||
           finishedMapTasks >= completedMapsForReduceSlowstart;
  }

  /**
   * Signals that the reduce resources are being requested
   * as soon as one process starts this nobody else should be
   * trying to request reduce resources, so get current and set to true
   *
   * @return false if the resources have not been requested yet,
   * true if they have
   */
  boolean initializeReducers() {
    return this.reduceResourcesRequested.getAndSet(true);
  }

  boolean areReducersInitialized() {
    return this.reduceResourcesRequested.get();
  }
  public Task obtainNewMapTaskForTip(
      String taskTrackerName, String hostName, TaskInProgress intendedTip) {
    synchronized (lockObject) {
      Task result = obtainTaskCleanupTask(taskTrackerName, intendedTip);
      if (result != null) {
        return result;
      }

      if (status.getRunState() != JobStatus.RUNNING) {
        return null;
      }

      TaskInProgress tip = removeMatchingTipUnprotected(nonRunningMaps, hostName, intendedTip);
      if (tip != null) {
        LOG.info("Running task " + tip.getTIPId() + " on " + taskTrackerName +
            "(" + hostName + ")");
      }

      if (tip == null && hasSpeculativeMaps) {
        SpeculationStatus speculationStatus = confirmSpeculativeTaskUnprotected(
            candidateSpeculativeMaps, intendedTip,
            taskTrackerName, hostName, TaskType.MAP);
        if (speculationStatus == SpeculationStatus.CAN_BE_SPECULATED) {
          tip = intendedTip;
          LOG.info("Speculating task " + tip.getTIPId() + " on " + taskTrackerName +
              "(" + hostName + ")");
        } else {
          LOG.warn("Cant speculate for given resource " +
            taskTrackerName + " because " + speculationStatus);
        }
      }

      if (tip == null) {
        return null;
      } else  if (tip != intendedTip) {
        throw new RuntimeException("Logic error:" + tip.getTIPId() +
            " was chosen instead of " + intendedTip.getTIPId());
      }
      scheduleMapUnprotected(tip);

      result = tip.getTaskToRun(taskTrackerName);
      if (result != null) {
        addRunningTaskToTIPUnprotected(tip, result.getTaskID(),
            taskTrackerName, hostName, true);
      }
      return result;
    }
  }
  
  /**
   * Registers new task attempt for given task
   * @param taskTrackerName name of destination tracker
   * @param hostName hostname of tracker
   * @param forcedTip task in progress to run
   * @return task to run
   */
  public Task forceNewMapTaskForTip(String taskTrackerName, String hostName,
      TaskInProgress forcedTip) {
    synchronized (lockObject) {
      Task result = obtainTaskCleanupTask(taskTrackerName, forcedTip);
      if (result != null) {
        return result;
      }

      removeMatchingTipUnprotectedUnconditional(nonRunningMaps, forcedTip);
      LOG.info("Running task " + forcedTip.getTIPId() + " on "
          + taskTrackerName + "(" + hostName + ")");
      scheduleMapUnprotected(forcedTip);
      result = forcedTip.getTaskToRun(taskTrackerName);
      if (result != null) {
        addRunningTaskToTIPUnprotected(forcedTip, result.getTaskID(),
            taskTrackerName, hostName, true);
        // Handle cleanup task
        setJobCleanupTaskState(result);
      }
      return result;
    }
  }


  @Override
  public boolean hasSpeculativeMaps() { return hasSpeculativeMaps; }

  @Override
  public boolean hasSpeculativeReduces() { return hasSpeculativeReduces; }

  private void refreshCandidateSpeculativeMapsUnprotected() {
    long now = clock.getTime();
    if ((now - lastSpeculativeMapRefresh) > speculativeRefreshTimeout) {
      // update the progress rates of all the candidate tips ..
      for(TaskInProgress tip: runningMaps) {
        tip.updateProgressRate(now);
      }
      candidateSpeculativeMaps =
        findSpeculativeTaskCandidatesUnprotected(runningMaps);

      int cap = getSpeculativeCap(TaskType.MAP);
      cap = Math.max(0, cap - speculativeMapTasks);
      cap = Math.min(candidateSpeculativeMaps.size(), cap);
      candidateSpeculativeMaps = candidateSpeculativeMaps.subList(0, cap);
      lastSpeculativeMapRefresh = now;
    }
  }

  public int getSpeculativeCap(TaskType type) {
    int cap = 0;
    synchronized (lockObject) {
      int numRunningTasks = (type == TaskType.MAP) ?
          (runningMapTasks - speculativeMapTasks) :
            (runningReduceTasks - speculativeReduceTasks);
      cap = (int) Math.max(MIN_SPEC_CAP, speculativeCap * numRunningTasks);
    }
    return cap;
  }

  public List<TaskInProgress> getSpeculativeCandidates(TaskType type) {
    synchronized (lockObject) {
      if (TaskType.MAP == type) {
        return candidateSpeculativeMaps;
      } else {
        return candidateSpeculativeReduces;
      }
    }
  }

  public void updateSpeculationCandidates() {
    synchronized (lockObject) {
      if (hasSpeculativeMaps()) {
        refreshCandidateSpeculativeMapsUnprotected();
      }
      if (hasSpeculativeReduces()) {
        refreshCandidateSpeculativeReducesUnprotected();
      }
    }
  }

  /**
   * Given a candidate set of tasks, find and order the ones that
   * can be speculated and return the same.
   */
  protected List<TaskInProgress> findSpeculativeTaskCandidatesUnprotected(
      Collection<TaskInProgress> list) {
    ArrayList<TaskInProgress> candidates = new ArrayList<TaskInProgress>();

    long now = clock.getTime();
    Iterator<TaskInProgress> iter = list.iterator();
    while (iter.hasNext()) {
      TaskInProgress tip = iter.next();
      if (tip.canBeSpeculated(now)) {
        candidates.add(tip);
      }
    }
    if (candidates.size() > 0 ) {
      Comparator<TaskInProgress> LateComparator =
        new JobInProgress.EstimatedTimeLeftComparator(now);

      Collections.sort(candidates, LateComparator);
    }
    return candidates;
  }

  private enum SpeculationStatus {
    CAN_BE_SPECULATED,
    CAN_NO_LONGER_BE_SPECULATED,
    HAS_RUN_ON_MACHINE,
    MACHINE_IS_SLOW
  };

  public boolean confirmSpeculativeTask(TaskInProgress tip,
      String taskTrackerName, String taskTrackerHost) {
    synchronized (lockObject) {
      if (tip.isMapTask()) {
        return confirmSpeculativeTaskUnprotected(candidateSpeculativeMaps,
            tip, taskTrackerName, taskTrackerHost, TaskType.MAP)
            == SpeculationStatus.CAN_BE_SPECULATED;
      } else {
        return confirmSpeculativeTaskUnprotected(candidateSpeculativeReduces,
            tip, taskTrackerName, taskTrackerHost, TaskType.REDUCE)
            == SpeculationStatus.CAN_BE_SPECULATED;
      }
    }
  }

  public boolean isBadSpeculativeResource(TaskInProgress tip,
      String taskTrackerName, String taskTrackerHost) {
    synchronized (lockObject) {
      SpeculationStatus status = null;
      if (tip.isMapTask()) {
        status = confirmSpeculativeTaskUnprotected(candidateSpeculativeMaps,
            tip, taskTrackerName, taskTrackerHost, TaskType.MAP);

      } else {
        status = confirmSpeculativeTaskUnprotected(candidateSpeculativeReduces,
            tip, taskTrackerName, taskTrackerHost, TaskType.REDUCE);
      }
      return status == SpeculationStatus.HAS_RUN_ON_MACHINE ||
      status == SpeculationStatus.MACHINE_IS_SLOW;
    }
  }

  protected SpeculationStatus confirmSpeculativeTaskUnprotected(
      List<TaskInProgress> candidates, TaskInProgress intendedTip,
      String taskTrackerName, String taskTrackerHost, TaskType taskType) {
    if ((candidates == null) || candidates.isEmpty()) {
      return null;
    }
    if (isSlowTrackerUnprotected(taskTrackerName)) {
      // TODO: request another resource if this happens.
      return SpeculationStatus.MACHINE_IS_SLOW;
    }

    if (!candidates.contains(intendedTip)) {
      return SpeculationStatus.CAN_NO_LONGER_BE_SPECULATED;
    }
    if (intendedTip.hasRunOnMachine(taskTrackerHost, taskTrackerName)) {
      return SpeculationStatus.HAS_RUN_ON_MACHINE;
    }

    long now = clock.getTime();
    if (intendedTip.canBeSpeculated(now)) {
      return SpeculationStatus.CAN_BE_SPECULATED;
    } else {
      // if it can't be speculated, then:
      // A. it has completed/failed etc. - in which case makes sense to never
      //    speculate again
      // B. it's relative progress does not allow speculation. in this case
      //    it's fair to treat it as if it was never eligible for speculation
      //    to begin with.
      return SpeculationStatus.CAN_NO_LONGER_BE_SPECULATED;
    }
  }

  /**
   * Compares the ave progressRate of tasks that have finished on this
   * taskTracker to the ave of all succesfull tasks thus far to see if this
   * TT one is too slow for speculating.
   * slowNodeThreshold is used to determine the number of standard deviations
   * @param taskTracker the name of the TaskTracker we are checking
   * @return is this TaskTracker slow
   */
  protected boolean isSlowTrackerUnprotected(String taskTracker) {
    // TODO - use statistics here.
    return false;
  }

  private TaskInProgress removeMatchingTipUnprotected(
      List<TaskInProgress> taskList, String hostName,
      TaskInProgress intendedTip) {
    for (Iterator<TaskInProgress> iter = taskList.iterator();
                            iter.hasNext();) {
      TaskInProgress t = iter.next();
      if (t == intendedTip &&
          t.isRunnable() && !t.isRunning() &&
          !t.hasFailedOnMachine(hostName)) {
        iter.remove();
        return t;
      }
    }
    return null;
  }
  
  /**
   * Removes matching TIP without checking any conditions
   * @param taskList list of tasks to remove from
   * @param intendedTip tip to remove
   * @return removed tip
   */
  private TaskInProgress removeMatchingTipUnprotectedUnconditional(
      List<TaskInProgress> taskList, TaskInProgress intendedTip) {
    for (Iterator<TaskInProgress> iter = taskList.iterator(); iter.hasNext();) {
      TaskInProgress t = iter.next();
      if (t.getTIPId().equals(intendedTip.getTIPId())) {
        iter.remove();
        return t;
      }
    }
    return null;
  }

  /**
   * Find a non-running task in the passed list of TIPs
   * @param tips a collection of TIPs
   * @param hostName the host name of tracker that has requested a task to run
   * @param removeFailedTip whether to remove the failed tips
   */
  private static TaskInProgress findTaskFromList(
      Collection<TaskInProgress> tips, String hostName,
      boolean removeFailedTip) {
    Iterator<TaskInProgress> iter = tips.iterator();
    while (iter.hasNext()) {
      TaskInProgress tip = iter.next();

      // Select a tip if
      //   1. runnable   : still needs to be run and is not completed
      //   2. ~running   : no other node is running it
      //   3. earlier attempt failed : has not failed on this host
      //                               and has failed on all the other hosts
      // A TIP is removed from the list if
      // (1) this tip is scheduled
      // (2) if the passed list is a level 0 (host) cache
      // (3) when the TIP is non-schedulable (running, killed, complete)
      if (tip.isRunnable() && !tip.isRunning()) {
        // check if the tip has failed on this host
        if (!tip.hasFailedOnMachine(hostName)) {
          // TODO: check if the tip has failed on all the nodes
          iter.remove();
          return tip;
        } else if (removeFailedTip) {
          // the case where we want to remove a failed tip from the host cache
          // point#3 in the TIP removal logic above
          iter.remove();
        }
      } else {
        // see point#3 in the comment above for TIP removal logic
        iter.remove();
      }
    }
    return null;
  }

  public Task obtainNewReduceTaskForTip(String taskTrackerName, String hostName,
      TaskInProgress intendedTip) {
    synchronized (lockObject) {
      // TODO: check for resource constraints.
      Task result = obtainTaskCleanupTask(taskTrackerName, intendedTip);
      if (result != null) {
        return result;
      }
      TaskInProgress tip = removeMatchingTipUnprotected(
          nonRunningReduces, hostName, intendedTip);
      if (tip != null) {
        LOG.info("Running task " + tip.getTIPId() + " on " + taskTrackerName +
            "(" + hostName + ")");
      }
      // 2. check for a reduce tip to be speculated
      if (tip == null && hasSpeculativeReduces) {
        SpeculationStatus speculationStatus = confirmSpeculativeTaskUnprotected(
            candidateSpeculativeReduces, intendedTip,
            taskTrackerName, hostName, TaskType.REDUCE);
        if (speculationStatus == SpeculationStatus.CAN_BE_SPECULATED) {
          tip = intendedTip;
          LOG.info("Speculating task " + tip.getTIPId() + " on " + taskTrackerName +
              "(" + hostName + ")");
        } else {
          LOG.warn("Cant speculate for given resource " +
            taskTrackerName + " because " + speculationStatus);
        }
      }

      if (tip == null) {
        return null;
      } else  if (tip != intendedTip) {
        throw new RuntimeException("Logic error:" + tip.getTIPId() +
            " was chosen instead of " + intendedTip.getTIPId());
      }

      scheduleReduceUnprotected(tip);
      result = tip.getTaskToRun(taskTrackerName);
      if (result != null) {
        addRunningTaskToTIPUnprotected(
            tip, result.getTaskID(), taskTrackerName, hostName, true);
      }
      return result;
    }
  }
  
  /**
   * Registers new task attempt for given task
   * @param taskTrackerName name of destination tracker
   * @param hostName hostname of tracker
   * @param forcedTip task in progress to run
   * @return task to run
   */
  public Task forceNewReduceTaskForTip(String taskTrackerName, String hostName,
      TaskInProgress forcedTip) {
    synchronized (lockObject) {
      Task result = obtainTaskCleanupTask(taskTrackerName, forcedTip);
      if (result != null) {
        return result;
      }

      removeMatchingTipUnprotectedUnconditional(nonRunningMaps, forcedTip);
      LOG.info("Running task " + forcedTip.getTIPId() + " on "
          + taskTrackerName + "(" + hostName + ")");
      scheduleReduceUnprotected(forcedTip);
      result = forcedTip.getTaskToRun(taskTrackerName);
      if (result != null) {
        addRunningTaskToTIPUnprotected(forcedTip, result.getTaskID(),
            taskTrackerName, hostName, true);
        // Handle cleanup task
        setJobCleanupTaskState(result);
      }
      return result;
    }
  }

  private void refreshCandidateSpeculativeReducesUnprotected() {
    long now = clock.getTime();
    if ((now - lastSpeculativeReduceRefresh) > speculativeRefreshTimeout) {
      // update the progress rates of all the candidate tips ..
      for(TaskInProgress tip: runningReduces) {
        tip.updateProgressRate(now);
      }
      candidateSpeculativeReduces =
        findSpeculativeTaskCandidatesUnprotected(runningReduces);
      int cap = getSpeculativeCap(TaskType.REDUCE);
      cap = Math.max(0, cap - speculativeReduceTasks);
      cap = Math.min(candidateSpeculativeReduces.size(), cap);
      candidateSpeculativeReduces =
        candidateSpeculativeReduces.subList(0, cap);
      lastSpeculativeReduceRefresh = now;
    }
  }

  /**
   * Can a tracker be used for a TIP?
   */
  public boolean canTrackerBeUsed(
      String taskTracker, String trackerHost, TaskInProgress tip) {
    synchronized (lockObject) {
      return !tip.hasFailedOnMachine(trackerHost);
    }
  }

  /**
   * Return a CleanupTask, if appropriate, to run on the given tasktracker
   */
  public Task obtainJobCleanupTask(
      String taskTrackerName, String hostName, boolean isMapSlot) {
    synchronized (lockObject) {
      if(!tasksInited || !jobSetupCleanupNeeded) {
        return null;
      }

      if (!canLaunchJobCleanupTaskUnprotected()) {
        return null;
      }

      List<TaskInProgress> cleanupTaskList = new ArrayList<TaskInProgress>();
      if (isMapSlot) {
        cleanupTaskList.add(cleanup[0]);
      } else {
        cleanupTaskList.add(cleanup[1]);
      }
      TaskInProgress tip =
        findTaskFromList(cleanupTaskList, hostName, false);
      if (tip == null) {
        return null;
      }

      // Now launch the cleanupTask
      Task result = tip.getTaskToRun(taskTrackerName);

      if (result != null) {
        addRunningTaskToTIPUnprotected(tip, result.getTaskID(), taskTrackerName, hostName,
            true);
        // Handle cleanup task
        setJobCleanupTaskState(result);
      }
      return result;
    }
  }
  
  /**
   * Sets task state according to job state if given task is cleanup one
   * @param task task to handle
   */
  private void setJobCleanupTaskState(Task task) {
    if (task.isJobCleanupTask()) {
      if (jobFailed) {
        task.setJobCleanupTaskState(
            org.apache.hadoop.mapreduce.JobStatus.State.FAILED);
      } else if (jobKilled) {
        task.setJobCleanupTaskState(
            org.apache.hadoop.mapreduce.JobStatus.State.KILLED);
      } else {
        task.setJobCleanupTaskState(
            org.apache.hadoop.mapreduce.JobStatus.State.SUCCEEDED);
      }
    }
  }
 
  @SuppressWarnings("deprecation")
  public boolean needsTaskCleanup(TaskInProgress tip) {
    synchronized (lockObject) {
      Iterator<TaskAttemptID> cleanupCandidates;
      if (tip.isMapTask()) {
        cleanupCandidates = mapCleanupTasks.iterator();
      } else {
        cleanupCandidates = reduceCleanupTasks.iterator();
      }
      
      while (cleanupCandidates.hasNext()) {
        if (cleanupCandidates.next().getTaskID().equals(tip.getTIPId())) {
          return true;
        }
      }
    }
    return false;
  }

  @SuppressWarnings("deprecation")
  public Task obtainTaskCleanupTask(String taskTracker, TaskInProgress tip) {
    synchronized (lockObject) {
      if (!tasksInited) {
        return null;
      }
      if (this.status.getRunState() != JobStatus.RUNNING ||
          jobFailed || jobKilled) {
        return null;
      }

      if (tip.isMapTask()) {
        if (mapCleanupTasks.isEmpty())
          return null;
      } else {
        if (reduceCleanupTasks.isEmpty())
          return null;
      }

      if (this.status.getRunState() != JobStatus.RUNNING ||
          jobFailed || jobKilled) {
        return null;
      }
      TaskAttemptID taskid = null;
      Iterator<TaskAttemptID> cleanupCandidates = null;
      boolean foundCleanup = false;
      if (tip.isMapTask()) {
        if (!mapCleanupTasks.isEmpty()) {
          cleanupCandidates = mapCleanupTasks.iterator();
        }
      } else {
        if (!reduceCleanupTasks.isEmpty()) {
          cleanupCandidates = reduceCleanupTasks.iterator();
        }
      }
      
      while (cleanupCandidates.hasNext()) {
         taskid = cleanupCandidates.next();
        
        if (taskid.getTaskID().equals(tip.getTIPId())) {
          // The task requires a cleanup so we are going to do that right now
          cleanupCandidates.remove();
          foundCleanup = true;
          break;
        }
      }
      if (foundCleanup) {
        return tip.addRunningTask(taskid, taskTracker, true);
      }
      return null;
    }
  }

  /**
   * Return a SetupTask, if appropriate, to run on the given tasktracker
   */
  public Task obtainJobSetupTask(
      String taskTrackerName, String hostName, boolean isMapSlot) {
    synchronized (lockObject) {
      if (!tasksInited || !jobSetupCleanupNeeded) {
        return null;
      }

      if (!(tasksInited && status.getRunState() == JobStatus.PREP &&
           !launchedSetup && !jobKilled && !jobFailed)) {
        return null;
      }
      List<TaskInProgress> setupTaskList = new ArrayList<TaskInProgress>();
      if (isMapSlot) {
        setupTaskList.add(setup[0]);
      } else {
        setupTaskList.add(setup[1]);
      }
      TaskInProgress tip = findTaskFromList(setupTaskList, hostName, false);
      if (tip == null) {
        return null;
      }

      // Now launch the setupTask
      Task result = tip.getTaskToRun(taskTrackerName);
      if (result != null) {
        addRunningTaskToTIPUnprotected(tip, result.getTaskID(), taskTrackerName, hostName,
            true);
      }
      return result;
    }
  }

  public void updateTaskStatus(TaskInProgress tip, TaskStatus status,
      TaskTrackerInfo ttStatus) {
    synchronized (lockObject) {
      updateTaskStatusUnprotected(tip, status, ttStatus);
    }
  }
  
  private boolean isTaskKilledHighMemory(TaskStatus status, String keyword) {
    String diagnosticInfo = status.getDiagnosticInfo();
    if (diagnosticInfo == null) {
      return false;
    }
    String[] splitdiagnosticInfo = diagnosticInfo.split("\\s+");
    for (String info : splitdiagnosticInfo) { 
      if (keyword.equals(info)) {
        return true;
      }
    }
    return false;
  }
  
  private boolean isTaskKilledWithHighMemory(TaskStatus status) {
    return isTaskKilledHighMemory(status,
      TaskMemoryManagerThread.HIGH_MEMORY_KEYWORD);
  }
  
  private boolean isTaskKilledWithCGroupMemory(TaskStatus status) {
    return isTaskKilledHighMemory(status,
      CGroupMemoryWatcher.CGROUPHIGH_MEMORY_KEYWORD);
  }
  
  private void updateCGResourceCounters(TaskStatus status, boolean isMap) {
    Counters taskCounters = status.getCounters();
    long maxMem = taskCounters.getCounter(Task.Counter.MAX_MEMORY_BYTES);
    long rssMem = taskCounters.getCounter(Task.Counter.MAX_RSS_MEMORY_BYTES);
    long instMem = taskCounters.getCounter(Task.Counter.INST_MEMORY_BYTES);
    
    if (isMap) {
     if (jobCounters.getCounter(Counter.MAX_MAP_MEM_BYTES) < maxMem) {
       jobCounters.findCounter(Counter.MAX_MAP_MEM_BYTES).setValue(maxMem);
     }
     if (jobCounters.getCounter(Counter.MAX_MAP_RSS_MEM_BYTES) < rssMem) {
       jobCounters.findCounter(Counter.MAX_MAP_RSS_MEM_BYTES).setValue(rssMem);
     }
     if (jobCounters.getCounter(Counter.MAX_MAP_INST_MEM_BYTES) < instMem) {
       jobCounters.findCounter(Counter.MAX_MAP_INST_MEM_BYTES).setValue(instMem);
     }
    } else {
      if (jobCounters.getCounter(Counter.MAX_REDUCE_MEM_BYTES) < maxMem) {
        jobCounters.findCounter(Counter.MAX_REDUCE_MEM_BYTES).setValue(maxMem);
      }
      if (jobCounters.getCounter(Counter.MAX_REDUCE_RSS_MEM_BYTES) < rssMem) {
        jobCounters.findCounter(Counter.MAX_REDUCE_RSS_MEM_BYTES).setValue(rssMem);
      }
      if (jobCounters.getCounter(Counter.MAX_REDUCE_INST_MEM_BYTES) < instMem) {
        jobCounters.findCounter(Counter.MAX_REDUCE_INST_MEM_BYTES).setValue(instMem);
      }
    }
  }
  
  @SuppressWarnings("deprecation")
  private void updateTaskStatusUnprotected(TaskInProgress tip, TaskStatus status,
      TaskTrackerInfo ttStatus) {
    double oldProgress = tip.getProgress();   // save old progress
    boolean wasRunning = tip.isRunning();
    boolean wasComplete = tip.isComplete();
    boolean wasPending = tip.isOnlyCommitPending();
    TaskAttemptID taskid = status.getTaskID();
    boolean wasAttemptRunning = tip.isAttemptRunning(taskid);
    
    // If the TIP is already completed and the task reports as SUCCEEDED then
    // mark the task as KILLED.
    // In case of task with no promotion the task tracker will mark the task
    // as SUCCEEDED.
    // User has requested to kill the task, but TT reported SUCCEEDED,
    // mark the task KILLED.
    if ((wasComplete || tip.wasKilled(taskid)) &&
        (status.getRunState() == TaskStatus.State.SUCCEEDED)) {
      status.setRunState(TaskStatus.State.KILLED);
    }

    // When a task has just reported its state as FAILED_UNCLEAN/KILLED_UNCLEAN,
    // if the job is complete or cleanup task is switched off,
    // make the task's state FAILED/KILLED without launching cleanup attempt.
    // Note that if task is already a cleanup attempt,
    // we don't change the state to make sure the task gets a killTaskAction
    if ((this.status.isJobComplete() || jobFailed ||
         jobKilled || !taskCleanupNeeded) && !tip.isCleanupAttempt(taskid)) {
      if (status.getRunState() == TaskStatus.State.FAILED_UNCLEAN) {
        status.setRunState(TaskStatus.State.FAILED);
      } else if (status.getRunState() == TaskStatus.State.KILLED_UNCLEAN) {
        status.setRunState(TaskStatus.State.KILLED);
      }
    }
    
    // aggregate the task tracker reported cgroup resource counters in job
    updateCGResourceCounters(status, tip.isMapTask());
    
    boolean change = tip.updateStatus(status);
    if (change) {
      TaskStatus.State state = status.getRunState();
      String httpTaskLogLocation = null; // TODO fix this
      if (ttStatus != null){
        String host;
        if (NetUtils.getStaticResolution(ttStatus.getHost()) != null) {
          host = NetUtils.getStaticResolution(ttStatus.getHost());
        } else {
          host = ttStatus.getHost();
        }
        httpTaskLogLocation = "http://" + host + ":" + ttStatus.getHttpPort();
      }
      TaskCompletionEvent taskEvent = null;
      if (state == TaskStatus.State.SUCCEEDED) {
        taskEvent = new TaskCompletionEvent(
                                            taskCompletionEventCounter,
                                            taskid,
                                            tip.idWithinJob(),
                                            status.getIsMap() &&
                                            !tip.isJobCleanupTask() &&
                                            !tip.isJobSetupTask(),
                                            TaskCompletionEvent.Status.SUCCEEDED,
                                            httpTaskLogLocation
                                           );
        taskEvent.setTaskRunTime((int)(status.getFinishTime()
                                       - status.getStartTime()));
        tip.setSuccessEventNumber(taskCompletionEventCounter);
      } else if (state == TaskStatus.State.COMMIT_PENDING) {
        // If it is the first attempt reporting COMMIT_PENDING
        // ask the task to commit.
        if (!wasComplete && !wasPending) {
          tip.doCommit(taskid);
        }
        return;
      } else if (state == TaskStatus.State.FAILED_UNCLEAN ||
                 state == TaskStatus.State.KILLED_UNCLEAN) {
        tip.incompleteSubTask(taskid, this.status);
        // add this task, to be rescheduled as cleanup attempt
        if (tip.isMapTask()) {
          mapCleanupTasks.add(taskid);
        } else {
          reduceCleanupTasks.add(taskid);
        }
        if (isTaskKilledWithCGroupMemory(status)) {
          //Increment the High Memory killed count for Reduce and Map Tasks
          if (status.getIsMap()) {
            jobCounters.incrCounter(
              Counter.TOTAL_CGROUP_MEMORY_MAP_TASK_KILLED, 1);
          }
          else {
            jobCounters.incrCounter(
              Counter.TOTAL_CGROUP_MEMORY_REDUCE_TASK_KILLED, 1);
          }
        }
      }
      //For a failed task update the JT datastructures.
      else if (state == TaskStatus.State.FAILED ||
               state == TaskStatus.State.KILLED) {
        if (isTaskKilledWithHighMemory(status)) {
          //Increment the High Memory killed count for Reduce and Map Tasks
          if (status.getIsMap()) {
            jobCounters.incrCounter(Counter.TOTAL_HIGH_MEMORY_MAP_TASK_KILLED, 1);
          }
          else {
            jobCounters.incrCounter(Counter.TOTAL_HIGH_MEMORY_REDUCE_TASK_KILLED, 1);
          }
        }
        if (isTaskKilledWithCGroupMemory(status)) {
          //Increment the High Memory killed count for Reduce and Map Tasks
          if (status.getIsMap()) {
            jobCounters.incrCounter(
              Counter.TOTAL_CGROUP_MEMORY_MAP_TASK_KILLED, 1);
          }
          else {
            jobCounters.incrCounter(
              Counter.TOTAL_CGROUP_MEMORY_REDUCE_TASK_KILLED, 1);
          }
        }
        // Get the event number for the (possibly) previously successful
        // task. If there exists one, then set that status to OBSOLETE
        int eventNumber;
        if ((eventNumber = tip.getSuccessEventNumber()) != -1) {
          TaskCompletionEvent t =
            this.taskCompletionEvents.get(eventNumber);
          if (t.getTaskAttemptId().equals(taskid))
            t.setTaskStatus(TaskCompletionEvent.Status.OBSOLETE);
        }

        // Tell the job to fail the relevant task
        failedTask(tip, taskid, status, ttStatus, wasRunning,
                  wasComplete, wasAttemptRunning);

        // Did the task failure lead to tip failure?
        TaskCompletionEvent.Status taskCompletionStatus =
          (state == TaskStatus.State.FAILED ) ?
              TaskCompletionEvent.Status.FAILED :
              TaskCompletionEvent.Status.KILLED;
        if (tip.isFailed()) {
          taskCompletionStatus = TaskCompletionEvent.Status.TIPFAILED;
        }
        taskEvent = new TaskCompletionEvent(taskCompletionEventCounter,
                                            taskid,
                                            tip.idWithinJob(),
                                            status.getIsMap() &&
                                            !tip.isJobCleanupTask() &&
                                            !tip.isJobSetupTask(),
                                            taskCompletionStatus,
                                            httpTaskLogLocation
                                           );
      }

      // Add the 'complete' task i.e. successful/failed
      // It _is_ safe to add the TaskCompletionEvent.Status.SUCCEEDED
      // *before* calling TIP.completedTask since:
      // a. One and only one task of a TIP is declared as a SUCCESS, the
      //    other (speculative tasks) are marked KILLED by the TaskCommitThread
      // b. TIP.completedTask *does not* throw _any_ exception at all.
      if (taskEvent != null) {
        this.taskCompletionEvents.add(taskEvent);
        taskCompletionEventCounter++;
        if (state == TaskStatus.State.SUCCEEDED) {
          completedTask(tip, status, ttStatus);
        }
      }
      taskStateChangeListener.taskStateChange(state, tip, taskid,
          (ttStatus == null ? "null" : ttStatus.getHost()));
    }

    //
    // Update CoronaJobInProgress status
    //
    if(LOG.isDebugEnabled()) {
      LOG.debug("Taking progress for " + tip.getTIPId() + " from " +
                 oldProgress + " to " + tip.getProgress());
    }

    if (!tip.isJobCleanupTask() && !tip.isJobSetupTask()) {
      double progressDelta = tip.getProgress() - oldProgress;
      if (tip.isMapTask()) {
          this.status.setMapProgress((float) (this.status.mapProgress() +
                                              progressDelta / maps.length));
      } else {
        this.status.setReduceProgress((float) (this.status.reduceProgress() +
                                           (progressDelta / reduces.length)));
      }
    }
  }


  /**
   * Should we reuse the resource of this succeeded task attempt
   * return true if we should reuse
   */
  boolean shouldReuseTaskResource(TaskInProgress tip) {
    synchronized (lockObject) {
      return tip.isJobSetupTask() || canLaunchJobCleanupTaskUnprotected()
          || tip.isJobCleanupTask();
    }
    // TIP is a job setup/cleanup task or job is ready for cleanup.
    //
    // Since job setup/cleanup does not get an explicit resource, reuse
    // the resource. For Map/Reduce tasks, we would want normally want
    // to release the resource. But if the job is ready for cleanup,
    // releasing the resource could mean that the job cleanup task can't
    // run, so reuse the resource.
  }

  public boolean completedTask(TaskInProgress tip, TaskStatus status,
      TaskTrackerInfo ttStatus) {
    synchronized (lockObject) {
      return completedTaskUnprotected(tip, status, ttStatus);
    }
  }

  /**
   * A taskId assigned to this CoronaJobInProgress has reported in successfully.
   */
  @SuppressWarnings("deprecation")
  private boolean completedTaskUnprotected(TaskInProgress tip, TaskStatus status,
      TaskTrackerInfo ttStatus) {
    int oldNumAttempts = tip.getActiveTasks().size();
    // Metering
    meterTaskAttemptUnprotected(tip, status);

    // It _is_ safe to not decrement running{Map|Reduce}Tasks and
    // finished{Map|Reduce}Tasks variables here because one and only
    // one task-attempt of a TIP gets to completedTask. This is because
    // the TaskCommitThread in the JobTracker marks other, completed,
    // speculative tasks as _complete_.
    TaskAttemptID taskId = status.getTaskID();
    if (tip.isComplete()) {
      // Mark this task as KILLED
      tip.alreadyCompletedTask(taskId);
      return false;
    }

    LOG.info("Task '" + taskId + "' has completed " + tip.getTIPId() +
           " successfully.");
    // Mark the TIP as complete
    tip.completed(taskId);

    // Update jobhistory
    String taskType = getTaskType(tip);
    if (status.getIsMap()){
      jobHistory.logMapTaskStarted(status.getTaskID(), status.getStartTime(),
                                       status.getTaskTracker(),
                                       ttStatus.getHttpPort(),
                                       taskType);
      jobHistory.logMapTaskFinished(status.getTaskID(), status.getFinishTime(),
                                        ttStatus.getHost(), taskType,
                                        status.getStateString(),
                                        status.getCounters());
    }else{
      jobHistory.logReduceTaskStarted( status.getTaskID(), status.getStartTime(),
                                          status.getTaskTracker(),
                                          ttStatus.getHttpPort(),
                                          taskType);
      jobHistory.logReduceTaskFinished(status.getTaskID(), status.getShuffleFinishTime(),
                                           status.getSortFinishTime(), status.getFinishTime(),
                                           ttStatus.getHost(),
                                           taskType,
                                           status.getStateString(),
                                           status.getCounters());
    }
    jobHistory.logTaskFinished(tip.getTIPId(),
                                taskType,
                                tip.getExecFinishTime(),
                                status.getCounters());

    int newNumAttempts = tip.getActiveTasks().size();
    if (tip.isJobSetupTask()) {
      // setup task has finished. kill the extra setup tip
      killSetupTipUnprotected(!tip.isMapTask());
      setupCompleteUnprotected();
    } else if (tip.isJobCleanupTask()) {
      // cleanup task has finished. Kill the extra cleanup tip
      if (tip.isMapTask()) {
        // kill the reduce tip
        cleanup[1].kill();
      } else {
        cleanup[0].kill();
      }

      if (jobFailed) {
        terminateJob(JobStatus.FAILED);
      } else if (jobKilled) {
        terminateJob(JobStatus.KILLED);
      } else {
        jobCompleteUnprotected();
      }
    } else if (tip.isMapTask()) {
      runningMapTasks--;
      // Update locality counters.
      long inputBytes = tip.getCounters()
                .getGroup("org.apache.hadoop.mapred.Task$Counter")
                .getCounter("Map input bytes");
      jobStats.incTotalMapInputBytes(inputBytes);
      localityStats.record(tip, ttStatus.getHost(), inputBytes);
      // check if this was a speculative task.
      if (oldNumAttempts > 1) {
        speculativeMapTasks -= (oldNumAttempts - newNumAttempts);
        jobStats.incNumSpeculativeSucceededMaps();
      }
      finishedMapTasks += 1;
      jobStats.incNumMapTasksCompleted();
      if (!tip.isJobSetupTask() && hasSpeculativeMaps) {
        updateTaskTrackerStats(tip,ttStatus,trackerMapStats,mapTaskStats);
      }
      // remove the completed map from the resp running caches
      retireMapUnprotected(tip);
      if ((finishedMapTasks + failedMapTIPs) == (numMapTasks)) {
        this.status.setMapProgress(1.0f);
      }
    } else {
      runningReduceTasks -= 1;
      if (oldNumAttempts > 1) {
        speculativeReduceTasks -= (oldNumAttempts - newNumAttempts);
        jobStats.incNumSpeculativeSucceededReduces();
      }
      finishedReduceTasks += 1;
      jobStats.incNumReduceTasksCompleted();
      if (!tip.isJobSetupTask() && hasSpeculativeReduces) {
        updateTaskTrackerStats(tip,ttStatus,trackerReduceStats,reduceTaskStats);
      }
      // remove the completed reduces from the running reducers set
      retireReduceUnprotected(tip);
      if ((finishedReduceTasks + failedReduceTIPs) == (numReduceTasks)) {
        this.status.setReduceProgress(1.0f);
      }
    }

    // is job complete?
    if (!jobSetupCleanupNeeded && canLaunchJobCleanupTaskUnprotected()) {
      jobCompleteUnprotected();
    }

    return true;
  }

  /**
   * Fail a task with a given reason, but without a status object.
   */
  @SuppressWarnings("deprecation")
  public void failedTask(
    TaskInProgress tip,
    TaskAttemptID taskid,
    String reason,
    TaskStatus.Phase phase,
    boolean isFailed,
    String trackerName,
    TaskTrackerInfo ttStatus) {
    TaskStatus.State state =
      isFailed ? TaskStatus.State.FAILED: TaskStatus.State.KILLED;
    TaskStatus status = TaskStatus.createTaskStatus(tip.isMapTask(),
                                                    taskid,
                                                    0.0f,
                                                    1,
                                                    state,
                                                    reason,
                                                    reason,
                                                    trackerName, phase,
                                                    new Counters());
    synchronized (lockObject) {
      // update the actual start-time of the attempt
      TaskStatus oldStatus = tip.getTaskStatus(taskid);
      long startTime = oldStatus == null
      ? JobTracker.getClock().getTime()
          : oldStatus.getStartTime();
      if (startTime < 0) {
        startTime = JobTracker.getClock().getTime();
      }
      status.setStartTime(startTime);
      status.setFinishTime(JobTracker.getClock().getTime());
      boolean wasComplete = tip.isComplete();
      updateTaskStatus(tip, status, ttStatus);
      boolean isComplete = tip.isComplete();
      if (wasComplete && !isComplete) { // mark a successful tip as failed
        String taskType = getTaskType(tip);
        JobHistory.Task.logFailed(tip.getTIPId(), taskType,
            tip.getExecFinishTime(), reason, taskid);
      }
    }
  }


  @SuppressWarnings("deprecation")
  public void failedTask(TaskInProgress tip, TaskAttemptID taskid,
                          TaskStatus status, TaskTrackerInfo taskTrackerStatus,
                          boolean wasRunning, boolean wasComplete,
                          boolean wasAttemptRunning) {
    synchronized (lockObject) {
      failedTaskUnprotected(tip, taskid, status, taskTrackerStatus,
          wasRunning, wasComplete, wasAttemptRunning);
    }
  }

  /**
   * A task assigned to this CoronaJobInProgress has reported in as failed.
   * Most of the time, we'll just reschedule execution.  However, after
   * many repeated failures we may instead decide to allow the entire
   * job to fail or succeed if the user doesn't care about a few tasks failing.
   *
   * Even if a task has reported as completed in the past, it might later
   * be reported as failed.  That's because the TaskTracker that hosts a map
   * task might die before the entire job can complete.  If that happens,
   * we need to schedule reexecution so that downstream reduce tasks can
   * obtain the map task's output.
   */
  @SuppressWarnings("deprecation")
  private void failedTaskUnprotected(TaskInProgress tip, TaskAttemptID taskid,
                          TaskStatus status, TaskTrackerInfo taskTrackerStatus,
                          boolean wasRunning, boolean wasComplete,
                          boolean wasAttemptRunning) {
    taskErrorCollector.collect(tip, taskid, clock.getTime());
    // check if the TIP is already failed
    boolean wasFailed = tip.isFailed();

    // Mark the taskid as FAILED or KILLED
    tip.incompleteSubTask(taskid, this.status);

    boolean isRunning = tip.isRunning();
    boolean isComplete = tip.isComplete();

    if (wasAttemptRunning) {
      if (!tip.isJobCleanupTask() && !tip.isJobSetupTask()) {
        long timeSpent = clock.getTime() - status.getStartTime();
        boolean isSpeculative = tip.isSpeculativeAttempt(taskid);
        if (tip.isMapTask()) {
          runningMapTasks -= 1;
          if (wasFailed) {
            jobStats.incNumMapTasksFailed();
            jobStats.incFailedMapTime(timeSpent);
          } else {
            jobStats.incNumMapTasksKilled();
            jobStats.incKilledMapTime(timeSpent);
            if (isSpeculative) {
              jobStats.incNumSpeculativeWasteMaps();
              jobStats.incSpeculativeMapTimeWaste(timeSpent);
            }
          }
        } else {
          runningReduceTasks -= 1;
          if (wasFailed) {
            jobStats.incNumReduceTasksFailed();
            jobStats.incFailedReduceTime(timeSpent);
          } else {
            jobStats.incNumReduceTasksKilled();
            jobStats.incKilledReduceTime(timeSpent);
            if (isSpeculative) {
              jobStats.incNumSpeculativeWasteReduces();
              jobStats.incSpeculativeReduceTimeWaste(timeSpent);
            }
          }
        }
      }

      // Metering
      meterTaskAttemptUnprotected(tip, status);
    }

    //update running  count on task failure.
    if (wasRunning && !isRunning) {
      if (tip.isJobCleanupTask()) {
        launchedCleanup = false;
      } else if (tip.isJobSetupTask()) {
        launchedSetup = false;
      } else if (tip.isMapTask()) {
        // remove from the running queue and put it in the non-running cache
        // if the tip is not complete i.e if the tip still needs to be run
        if (!isComplete) {
          failMapUnprotected(tip);
        }
      } else {
        // remove from the running queue and put in the failed queue if the tip
        // is not complete
        if (!isComplete) {
          failReduceUnprotected(tip);
        }
      }
    }

    // The case when the map was complete but the task tracker went down.
    // However, we don't need to do any metering here...
    if (wasComplete && !isComplete) {
      if (tip.isMapTask()) {
        // Put the task back in the cache. This will help locality for cases
        // where we have a different TaskTracker from the same rack/switch
        // asking for a task.
        // We bother about only those TIPs that were successful
        // earlier (wasComplete and !isComplete)
        // (since they might have been removed from the cache of other
        // racks/switches, if the input split blocks were present there too)
        failMapUnprotected(tip);
        finishedMapTasks -= 1;
      }
    }

    // update job history
    // get taskStatus from tip
    TaskStatus taskStatus = tip.getTaskStatus(taskid);
    String taskTrackerName = taskStatus.getTaskTracker();
    String taskTrackerHostName = convertTrackerNameToHostName(taskTrackerName);
    int taskTrackerPort = -1;
    if (taskTrackerStatus != null) {
      taskTrackerPort = taskTrackerStatus.getHttpPort();
    }
    long startTime = taskStatus.getStartTime();
    long finishTime = taskStatus.getFinishTime();
    List<String> taskDiagnosticInfo = tip.getDiagnosticInfo(taskid);
    String diagInfo = taskDiagnosticInfo == null ? "" :
      StringUtils.arrayToString(taskDiagnosticInfo.toArray(new String[0]));
    String taskType = getTaskType(tip);
    if (taskStatus.getIsMap()) {
      jobHistory.logMapTaskStarted(taskid, startTime,
        taskTrackerName, taskTrackerPort, taskType);
      if (taskStatus.getRunState() == TaskStatus.State.FAILED) {
        jobHistory.logMapTaskFailed(taskid, finishTime,
          taskTrackerHostName, diagInfo, taskType);
      } else {
        jobHistory.logMapTaskKilled(taskid, finishTime,
          taskTrackerHostName, diagInfo, taskType);
      }
    } else {
      jobHistory.logReduceTaskStarted(taskid, startTime,
        taskTrackerName, taskTrackerPort, taskType);
      if (taskStatus.getRunState() == TaskStatus.State.FAILED) {
        jobHistory.logReduceTaskFailed(taskid, finishTime,
          taskTrackerHostName, diagInfo, taskType);
      } else {
        jobHistory.logReduceTaskKilled(taskid, finishTime,
          taskTrackerHostName, diagInfo, taskType);
      }
    }

    // After this, try to assign tasks with the one after this, so that
    // the failed task goes to the end of the list.
    if (!tip.isJobCleanupTask() && !tip.isJobSetupTask()) {
      if (tip.isMapTask()) {
        failedMapTasks++;
        if (taskStatus.getRunState() != TaskStatus.State.FAILED) {
          killedMapTasks++;
        }
      } else {
        failedReduceTasks++;
        if (taskStatus.getRunState() != TaskStatus.State.FAILED) {
          killedReduceTasks++;
        }
      }
    }


    //
    // Check if we need to kill the job because of too many failures or
    // if the job is complete since all component tasks have completed

    // We do it once per TIP and that too for the task that fails the TIP
    if (!wasFailed && tip.isFailed()) {
      //
      // Allow upto 'mapFailuresPercent' of map tasks to fail or
      // 'reduceFailuresPercent' of reduce tasks to fail
      //
      boolean killJob = tip.isJobCleanupTask() || tip.isJobSetupTask() ? true :
                        tip.isMapTask() ?
            ((++failedMapTIPs*100) > (mapFailuresPercent*numMapTasks)) :
            ((++failedReduceTIPs*100) > (reduceFailuresPercent*numReduceTasks));

      if (killJob) {
        LOG.info("Aborting job " + profile.getJobID());
        jobHistory.logTaskFailed(tip.getTIPId(),
                                  taskType,
                                  finishTime,
                                  diagInfo);
        if (tip.isJobCleanupTask()) {
          // kill the other tip
          if (tip.isMapTask()) {
            cleanup[1].kill();
          } else {
            cleanup[0].kill();
          }
          terminateJob(JobStatus.FAILED);
        } else {
          if (tip.isJobSetupTask()) {
            // kill the other tip
            killSetupTipUnprotected(!tip.isMapTask());
          }
          fail();
        }
      }

      //
      // Update the counters
      //
      if (!tip.isJobCleanupTask() && !tip.isJobSetupTask()) {
        if (tip.isMapTask()) {
          jobCounters.incrCounter(Counter.NUM_FAILED_MAPS, 1);
        } else {
          jobCounters.incrCounter(Counter.NUM_FAILED_REDUCES, 1);
        }
      }
    }
  }

  /**
   * Get the task type for logging it to {@link JobHistory}.
   */
  private String getTaskType(TaskInProgress tip) {
    if (tip.isJobCleanupTask()) {
      return Values.CLEANUP.name();
    } else if (tip.isJobSetupTask()) {
      return Values.SETUP.name();
    } else if (tip.isMapTask()) {
      return Values.MAP.name();
    } else {
      return Values.REDUCE.name();
    }
  }

  /**
   * Adds the failed TIP in the front of the list for non-running maps
   * @param tip the tip that needs to be failed
   */
  private void failMapUnprotected(TaskInProgress tip) {
    if (nonRunningMaps == null) {
      LOG.warn("Non-running cache for maps missing!! "
               + "Job details are missing.");
      return;
    }
    nonRunningMaps.add(tip);
  }

  /**
   * Adds a failed TIP in the front of the list for non-running reduces
   * @param tip the tip that needs to be failed
   */
  private void failReduceUnprotected(TaskInProgress tip) {
    if (nonRunningReduces == null) {
      LOG.warn("Failed cache for reducers missing!! "
               + "Job details are missing.");
      return;
    }
    nonRunningReduces.add(0, tip);
  }

  @SuppressWarnings("deprecation")
  private void clearUncleanTasksUnprotected() {
    TaskAttemptID taskid = null;
    TaskInProgress tip = null;
    while (!mapCleanupTasks.isEmpty()) {
      taskid = mapCleanupTasks.remove(0);
      tip = maps[taskid.getTaskID().getId()];
      updateTaskStatus(tip, tip.getTaskStatus(taskid), null);
    }
    while (!reduceCleanupTasks.isEmpty()) {
      taskid = reduceCleanupTasks.remove(0);
      tip = reduces[taskid.getTaskID().getId()];
      updateTaskStatus(tip, tip.getTaskStatus(taskid), null);
    }
  }

  private void killSetupTipUnprotected(boolean isMap) {
    if (isMap) {
      setup[0].kill();
    } else {
      setup[1].kill();
    }
  }

  private void setupCompleteUnprotected() {
    status.setSetupProgress(1.0f);
    if (this.status.getRunState() == JobStatus.PREP) {
      changeStateTo(JobStatus.RUNNING);
      jobHistory.logStarted();
    }
  }

  private void jobComplete() {
    synchronized (lockObject) {
      jobCompleteUnprotected();
    }
  }
  /**
   * The job is done since all it's component tasks are either
   * successful or have failed.
   */
  @SuppressWarnings("deprecation")
  private void jobCompleteUnprotected() {
    //
    // All tasks are complete, then the job is done!
    
    if(this.terminated == JobStatus.FAILED 
        || this.terminated == JobStatus.KILLED) {
      terminate(this.terminated);
    }
    if (this.status.getRunState() == JobStatus.RUNNING ||
        this.status.getRunState() == JobStatus.PREP) {
      changeStateTo(JobStatus.SUCCEEDED);
      this.status.setCleanupProgress(1.0f);
      if (maps.length == 0) {
        this.status.setMapProgress(1.0f);
      }
      if (reduces.length == 0) {
        this.status.setReduceProgress(1.0f);
      }
      this.finishTime = clock.getTime();
      LOG.info("Job " + this.status.getJobID() +
               " has completed successfully.");

      // Log the job summary (this should be done prior to logging to
      // job-history to ensure job-counters are in-sync
      JobSummary.logJobSummary(this);

      Counters counters = getCounters();
      // Log job-history
      jobHistory.logFinished(finishTime,
                             this.finishedMapTasks,
                             this.finishedReduceTasks, failedMapTasks,
                             failedReduceTasks, killedMapTasks,
                             killedReduceTasks, getMapCounters(),
                             getReduceCounters(), counters);
    }
  }

  /**
   * Job state change must happen thru this call
   */
  private void changeStateTo(int newState) {
    synchronized (lockObject) {
      int oldState = this.status.getRunState();
      if (oldState == newState) {
        return; //old and new states are same
      }
      this.status.setRunState(newState);
    }
  }

  /**
   * Metering: Occupied Slots * (Finish - Start)
   * @param tip {@link TaskInProgress} to be metered which just completed,
   *            cannot be <code>null</code>
   * @param status {@link TaskStatus} of the completed task, cannot be
   *               <code>null</code>
   */
  @SuppressWarnings("deprecation")
  private void meterTaskAttemptUnprotected(TaskInProgress tip, TaskStatus status) {
    Counter slotCounter =
      (tip.isMapTask()) ? Counter.SLOTS_MILLIS_MAPS :
                          Counter.SLOTS_MILLIS_REDUCES;
    jobCounters.incrCounter(slotCounter,
                            tip.getNumSlotsRequired() *
                            (status.getFinishTime() - status.getStartTime()));
    if (!tip.isMapTask()) {
      jobCounters.incrCounter(Counter.SLOTS_MILLIS_REDUCES_COPY,
                  tip.getNumSlotsRequired() *
                  (status.getShuffleFinishTime() - status.getStartTime()));
      jobCounters.incrCounter(Counter.SLOTS_MILLIS_REDUCES_SORT,
                  tip.getNumSlotsRequired() *
                  (status.getSortFinishTime() - status.getShuffleFinishTime()));
      jobCounters.incrCounter(Counter.SLOTS_MILLIS_REDUCES_REDUCE,
                  tip.getNumSlotsRequired() *
                  (status.getFinishTime() - status.getSortFinishTime()));
    }
  }

  /**
   * Populate the data structures as a task is scheduled.
   *
   * Assuming {@link JobTracker} is locked on entry.
   *
   * @param tip The tip for which the task is added
   * @param id The attempt-id for the task
   * @param taskTracker task tracker name
   * @param hostName host name for the task tracker
   * @param isScheduled Whether this task is scheduled from the JT or has
   *        joined back upon restart
   */
  @SuppressWarnings("deprecation")
  private void addRunningTaskToTIPUnprotected(
      TaskInProgress tip, TaskAttemptID id, String taskTracker,
      String hostName, boolean isScheduled) {
    // Make an entry in the tip if the attempt is not scheduled i.e externally
    // added
    if (!isScheduled) {
      tip.addRunningTask(id, taskTracker);
    }

    // keeping the earlier ordering intact
    String name;
    String splits = "";
    Enum<Counter> counter = null;
    if (tip.isJobSetupTask()) {
      launchedSetup = true;
      name = Values.SETUP.name();
    } else if (tip.isJobCleanupTask()) {
      launchedCleanup = true;
      name = Values.CLEANUP.name();
    } else if (tip.isMapTask()) {
      ++runningMapTasks;
      name = Values.MAP.name();
      counter = Counter.TOTAL_LAUNCHED_MAPS;
      splits = tip.getSplitNodes();
      if (tip.getActiveTasks().size() > 1) {
        speculativeMapTasks++;
        jobStats.incNumSpeculativeMaps();
      }
      jobStats.incNumMapTasksLaunched();
    } else {
      ++runningReduceTasks;
      name = Values.REDUCE.name();
      counter = Counter.TOTAL_LAUNCHED_REDUCES;
      if (tip.getActiveTasks().size() > 1) {
        speculativeReduceTasks++;
        jobStats.incNumSpeculativeReduces();
      }
      jobStats.incNumReduceTasksLaunched();
    }
    // Note that the logs are for the scheduled tasks only. Tasks that join on
    // restart has already their logs in place.
    if (tip.isFirstAttempt(id)) {
      jobHistory.logTaskStarted(tip.getTIPId(), name,
                                 tip.getExecStartTime(), splits);
    }
    if (!tip.isJobSetupTask() && !tip.isJobCleanupTask()) {
      jobCounters.incrCounter(counter, 1);
    }

    if (tip.isMapTask() && !tip.isJobSetupTask() && !tip.isJobCleanupTask()) {
      localityStats.record(tip, hostName, -1);
    }
  }

  /**
   * Kill the job and all its component tasks.
   */
  public void kill() {
    terminate(JobStatus.KILLED);
  }

  void fail() {
    terminate(JobStatus.FAILED);
  }

  private void terminate(int jobTerminationState) {
    this.terminated = jobTerminationState;
    synchronized (lockObject) {
      terminateUnprotected(jobTerminationState);
    }
  }

  /**
   * Terminate the job and all its component tasks.
   * Calling this will lead to marking the job as failed/killed. Cleanup
   * tip will be launched. If the job has not inited, it will directly call
   * terminateJob as there is no need to launch cleanup tip.
   * This method is reentrant.
   * @param jobTerminationState job termination state
   */
  private void terminateUnprotected(int jobTerminationState) {
    this.terminated = jobTerminationState;
    if(!tasksInited) {
      //init could not be done, we just terminate directly.
      terminateJob(jobTerminationState);
      return;
    }

    if ((status.getRunState() == JobStatus.RUNNING) ||
         (status.getRunState() == JobStatus.PREP)) {
      LOG.info("Killing job '" + this.status.getJobID() + "'");
      if (jobTerminationState == JobStatus.FAILED) {
        if(jobFailed) {//reentrant
          return;
        }
        jobFailed = true;
      } else if (jobTerminationState == JobStatus.KILLED) {
        if(jobKilled) {//reentrant
          return;
        }
        jobKilled = true;
      }
      // clear all unclean tasks
      clearUncleanTasksUnprotected();
      //
      // kill all TIPs.
      //
      for (int i = 0; i < setup.length; i++) {
        setup[i].kill();
      }
      for (int i = 0; i < maps.length; i++) {
        maps[i].kill();
        TreeMap<TaskAttemptID, String> activeTasks = maps[i].getActiveTasksCopy();
        for (TaskAttemptID attempt : activeTasks.keySet()) {
          TaskStatus status = maps[i].getTaskStatus(attempt);
          if (status != null) {
            failedTask(maps[i], attempt, JOB_KILLED_REASON,
              status.getPhase(), false, status.getTaskTracker(), null);
          } else {
            failedTask(maps[i], attempt, JOB_KILLED_REASON,
              Phase.MAP, false, EMPTY_TRACKER_NAME, null);
          }
        }
      }
      for (int i = 0; i < reduces.length; i++) {
        reduces[i].kill();
        TreeMap<TaskAttemptID, String> activeTasks = reduces[i].getActiveTasksCopy();
        for (TaskAttemptID attempt : activeTasks.keySet()) {
          TaskStatus status = reduces[i].getTaskStatus(attempt);
          if (status != null) {
            failedTask(reduces[i], attempt, JOB_KILLED_REASON,
              status.getPhase(), false, status.getTaskTracker(), null);
          } else {
            failedTask(reduces[i], attempt, JOB_KILLED_REASON,
              Phase.REDUCE, false, EMPTY_TRACKER_NAME, null);
          }
        }
      }

      // Moved job to a terminal state if no job cleanup is needed. In case the
      // job is killed, we do not perform cleanup. This is because cleanup
      // cannot be guaranteed - the process running Corona JT could just be killed.
      if (!jobSetupCleanupNeeded || jobTerminationState == JobStatus.KILLED) {
        terminateJobUnprotected(jobTerminationState);
      }
    }
  }

  private void terminateJob(int jobTerminationState) {
    synchronized (lockObject) {
      terminateJobUnprotected(jobTerminationState);
    }
  }

  @SuppressWarnings("deprecation")
  private void terminateJobUnprotected(int jobTerminationState) {
    if ((status.getRunState() == JobStatus.RUNNING) ||
        (status.getRunState() == JobStatus.PREP)) {
      this.finishTime = clock.getTime();
      this.status.setMapProgress(1.0f);
      this.status.setReduceProgress(1.0f);
      this.status.setCleanupProgress(1.0f);

      Counters counters = getCounters();
      setExtendedMetricsCountersUnprotected(counters);
      if (jobTerminationState == JobStatus.FAILED) {
        changeStateTo(JobStatus.FAILED);

        // Log the job summary
        JobSummary.logJobSummary(this);

        // Log to job-history
        jobHistory.logFailed(finishTime,
                             this.finishedMapTasks,
                             this.finishedReduceTasks, counters);
      } else {
        changeStateTo(JobStatus.KILLED);

        // Log the job summary
        JobSummary.logJobSummary(this);

        // Log to job-history
        jobHistory.logKilled(finishTime,
                             this.finishedMapTasks,
                             this.finishedReduceTasks, counters);
      }
    }
  }

  @SuppressWarnings("deprecation")
  private void setExtendedMetricsCountersUnprotected(Counters counters) {
    counters.incrCounter("extMet", "submit_time",
        getLaunchTime() - getStartTime());
    for (int i = 0; i < setup.length; i++) {
      if (setup[i].isComplete()) {
        counters.incrCounter("extMet", "setup_time",
            setup[i].getExecFinishTime() - setup[i].getStartTime());
        break;
      }
    }
    for (int i = cleanup.length - 1; i >= 0; i--) {
      if (cleanup[i].isComplete()) {
        counters.incrCounter("extMet", "cleanup_time",
            cleanup[i].getExecFinishTime() - cleanup[i].getStartTime());
        break;
      }
    }
    long totalMapWaitTime = 0;
    long maxMapWaitTime = 0;
    long totalMaps = 0;
    for (int i = 0; i < maps.length; i++) {
      if (maps[i].isComplete()) {
        long waitTime = maps[i].getExecStartTime() - getLaunchTime();
        if (waitTime > maxMapWaitTime)
          maxMapWaitTime = waitTime;
        totalMapWaitTime += waitTime;
        ++totalMaps;
      }
    }
    counters.incrCounter("extMet", "avg_map_wait_time",
        totalMaps > 0 ? (totalMapWaitTime / totalMaps) : 0);
    counters.incrCounter("extMet", "max_map_wait_time",
        maxMapWaitTime);
  }

  /**
   * Remove a map TIP from the lists for running maps.
   * Called when a map fails/completes (note if a map is killed,
   * it won't be present in the list since it was completed earlier)
   * @param tip the tip that needs to be retired
   */
  private void retireMapUnprotected(TaskInProgress tip) {
    if (runningMaps == null) {
      LOG.warn("Running cache for maps missing!! "
               + "Job details are missing.");
      return;
    }
    runningMaps.remove(tip);
  }

  /**
   * Remove a reduce TIP from the list for running-reduces
   * Called when a reduce fails/completes
   * @param tip the tip that needs to be retired
   */
  private void retireReduceUnprotected(TaskInProgress tip) {
    if (runningReduces == null) {
      LOG.warn("Running list for reducers missing!! "
               + "Job details are missing.");
      return;
    }
    runningReduces.remove(tip);
  }

  protected void scheduleMapUnprotected(TaskInProgress tip) {
    runningMapTaskStats.add(0.0f);
    runningMaps.add(tip);
  }

  protected void scheduleReduceUnprotected(TaskInProgress tip) {
    runningReduceTaskStats.add(0.0f);
    runningReduces.add(tip);
  }

  /**
   * Check if the job needs the JobCleanup task launched.
   * @return true if the job needs a cleanup task launched, false otherwise
   */
  public boolean canLaunchJobCleanupTask() {
    synchronized (lockObject) {
      return canLaunchJobCleanupTaskUnprotected();
    }
  }

  /**
   * Check whether cleanup task can be launched for the job.
   *
   * Cleanup task can be launched if it is not already launched
   * or job is Killed
   * or all maps and reduces are complete
   * @return true/false
   */
  private boolean canLaunchJobCleanupTaskUnprotected() {
    // check if the job is running
    if (status.getRunState() != JobStatus.RUNNING &&
        status.getRunState() != JobStatus.PREP) {
      return false;
    }
    // check if cleanup task has been launched already or if setup isn't
    // launched already. The later check is useful when number of maps is
    // zero.
    if (launchedCleanup || !isSetupFinishedUnprotected()) {
      return false;
    }
    // check if job has failed or killed
    if (jobKilled || jobFailed) {
      return true;
    }

    boolean mapsDone = ((finishedMapTasks + failedMapTIPs) == (numMapTasks));
    boolean reducesDone = ((finishedReduceTasks + failedReduceTIPs) == numReduceTasks);
    boolean mapOnlyJob = (numReduceTasks == 0);

    if (mapOnlyJob) {
      return mapsDone;
    }
    if (jobFinishWhenReducesDone) {
      return reducesDone;
    }
    return mapsDone && reducesDone;
  }

  boolean isSetupFinishedUnprotected() {
    // if there is no setup to be launched, consider setup is finished.
    if ((tasksInited && setup.length == 0) ||
        setup[0].isComplete() || setup[0].isFailed() || setup[1].isComplete()
        || setup[1].isFailed()) {
      return true;
    }
    return false;
  }


  /**
   *  Returns the total job counters, by adding together the job,
   *  the map and the reduce counters.
   */
  @SuppressWarnings("deprecation")
  public Counters getCounters() {
    synchronized (lockObject) {
      Counters result = new Counters();
      result.incrAllCounters(getJobCounters());

      incrementTaskCountersUnprotected(result, maps);
      return incrementTaskCountersUnprotected(result, reduces);
    }
  }

  public Counters getErrorCounters() {
    return taskErrorCollector.getErrorCountsCounters();
  }

  public Object getSchedulingInfo() {
    return null; // TODO
  }

  boolean isJobEmpty() {
    return maps.length == 0 && reduces.length == 0 && !jobSetupCleanupNeeded;
  }

  void completeEmptyJob() {
    jobComplete();
  }

  void completeSetup() {
    synchronized (lockObject) {
      setupCompleteUnprotected();
    }
  }

  /**
   * Increments the counters with the counters from each task.
   * @param counters the counters to increment
   * @param tips the tasks to add in to counters
   * @return counters the same object passed in as counters
   */
  @SuppressWarnings("deprecation")
  private Counters incrementTaskCountersUnprotected(Counters counters,
                                         TaskInProgress[] tips) {
    for (TaskInProgress tip : tips) {
      counters.incrAllCounters(tip.getCounters());
    }
    return counters;
  }

  static class JobSummary {
    static final Log LOG = LogFactory.getLog(JobSummary.class);

    // Escape sequences
    static final char EQUALS = '=';
    static final char[] charsToEscape =
      {StringUtils.COMMA, EQUALS, StringUtils.ESCAPE_CHAR};

    /**
     * Log a summary of the job's runtime.
     *
     * @param job {@link JobInProgress} whose summary is to be logged, cannot
     *            be <code>null</code>.
     */
    @SuppressWarnings("deprecation")
    public static void logJobSummary(CoronaJobInProgress job) {
      JobStatus status = job.getStatus();
      JobProfile profile = job.getProfile();
      String user = StringUtils.escapeString(profile.getUser(),
                                             StringUtils.ESCAPE_CHAR,
                                             charsToEscape);
      String queue = StringUtils.escapeString(profile.getQueueName(),
                                              StringUtils.ESCAPE_CHAR,
                                              charsToEscape);
      Counters jobCounters = job.getJobCounters();
      long mapSlotSeconds =
        (jobCounters.getCounter(Counter.SLOTS_MILLIS_MAPS) +
         jobCounters.getCounter(Counter.FALLOW_SLOTS_MILLIS_MAPS)) / 1000;
      long reduceSlotSeconds =
        (jobCounters.getCounter(Counter.SLOTS_MILLIS_REDUCES) +
         jobCounters.getCounter(Counter.FALLOW_SLOTS_MILLIS_REDUCES)) / 1000;

      LOG.info("jobId=" + profile.getJobID() + StringUtils.COMMA +
               "submitTime" + EQUALS + job.getStartTime() + StringUtils.COMMA +
               "launchTime" + EQUALS + job.getLaunchTime() + StringUtils.COMMA +
               "finishTime" + EQUALS + job.getFinishTime() + StringUtils.COMMA +
               "numMaps" + EQUALS + job.getTasks(TaskType.MAP).length +
                           StringUtils.COMMA +
               "numSlotsPerMap" + EQUALS + NUM_SLOTS_PER_MAP +
                                  StringUtils.COMMA +
               "numReduces" + EQUALS + job.getTasks(TaskType.REDUCE).length +
                              StringUtils.COMMA +
               "numSlotsPerReduce" + EQUALS + NUM_SLOTS_PER_REDUCE +
                                     StringUtils.COMMA +
               "user" + EQUALS + user + StringUtils.COMMA +
               "queue" + EQUALS + queue + StringUtils.COMMA +
               "status" + EQUALS +
                          JobStatus.getJobRunState(status.getRunState()) +
                          StringUtils.COMMA +
               "mapSlotSeconds" + EQUALS + mapSlotSeconds + StringUtils.COMMA +
               "reduceSlotsSeconds" + EQUALS + reduceSlotSeconds  +
                                      StringUtils.COMMA
      );
    }
  }

  @Override
  public boolean inited() {
    return tasksInited;
  }

  private void updateTaskTrackerStats(TaskInProgress tip, TaskTrackerInfo ttStatus,
      Map<String,DataStatistics> trackerStats, DataStatistics overallStats) {
    synchronized (lockObject) {
      float tipDuration = tip.getExecFinishTime() -
      tip.getDispatchTime(tip.getSuccessfulTaskid());
      DataStatistics ttStats =
        trackerStats.get(ttStatus.getTrackerName());
      double oldMean = 0.0d;
      //We maintain the mean of TaskTrackers' means. That way, we get a single
      //data-point for every tracker (used in the evaluation in isSlowTracker)
      if (ttStats != null) {
        oldMean = ttStats.mean();
        ttStats.add(tipDuration);
        overallStats.updateStatistics(oldMean, ttStats.mean());
      } else {
        trackerStats.put(ttStatus.getTrackerName(),
            (ttStats = new DataStatistics(tipDuration)));
        overallStats.add(tipDuration);
      }
      if (LOG.isDebugEnabled()) {
        LOG.debug("Added mean of " +ttStats.mean() + " to trackerStats of type "+
            (tip.isMapTask() ? "Map" : "Reduce") +
            " on "+ttStatus.getTrackerName()+". DataStatistics is now: " +
            trackerStats.get(ttStatus.getTrackerName()));
      }
    }
  }

  @Override
  public Vector<TaskInProgress> reportTasksInProgress(boolean shouldBeMap,
                                                      boolean shouldBeComplete) {
    synchronized (lockObject) {
      return super.reportTasksInProgress(shouldBeMap, shouldBeComplete);
    }
  }

  @Override
  public Vector<TaskInProgress> reportCleanupTIPs(boolean shouldBeComplete) {
    synchronized (lockObject) {
      return super.reportCleanupTIPs(shouldBeComplete);
    }
  }

  @Override
  public Vector<TaskInProgress> reportSetupTIPs(boolean shouldBeComplete) {
    synchronized (lockObject) {
      return super.reportSetupTIPs(shouldBeComplete);
    }
  }

  @Override
  public TaskInProgress getTaskInProgress(TaskID tipid) {
    synchronized (lockObject) {
      return super.getTaskInProgress(tipid);
    }
  }

  public Configuration getConf() {
    return jobConf;
  }

  @Override
  public boolean shouldLogCannotspeculativeMaps() {
    long now = clock.getTime();
    if ((now - lastTimeCannotspeculativeMapLog) <= logCannotspeculativeInterval)
        return false;
    int unfinished = numMapTasks - finishedMapTasks;
    if (unfinished <= numMapTasks * speculativeMapLogRateThreshold ||
        unfinished <= speculativeMapLogNumThreshold) {
        lastTimeCannotspeculativeMapLog = now; 
        return true;
    }
    return false;
  }

  @Override
  public boolean shouldLogCannotspeculativeReduces() {
    long now = clock.getTime();
    if ((now - lastTimeCannotspeculativeReduceLog) <= logCannotspeculativeInterval)
      return false;
    int unfinished = numReduceTasks - finishedReduceTasks;
    if (unfinished <= numReduceTasks * speculativeReduceLogRateThreshold ||
        unfinished <= speculativeReduceLogNumThreshold) {
        lastTimeCannotspeculativeReduceLog = now; 
        return true;
    }
    return false;
  }

  @Override
  public boolean shouldSpeculateAllRemainingMaps() {
    int unfinished = numMapTasks - finishedMapTasks;
    if (unfinished < numMapTasks * speculativeMapUnfininshedThreshold ||
        unfinished == 1) {
      return true;
    }
    return false;
  }

  @Override
  public boolean shouldSpeculateAllRemainingReduces() {
    int unfinished = numReduceTasks - finishedReduceTasks;
    if (unfinished < numReduceTasks * speculativeReduceUnfininshedThreshold ||
        unfinished == 1) {
      return true;
    }
    return false;
  }

  @Override
  DataStatistics getRunningTaskStatistics(Phase phase) {
    throw new RuntimeException("Not yet implemented.");
  }

  public static void uploadCachedSplits(JobID jobId, JobConf jobConf,
      String systemDir) throws IOException {
    Path jobDir = new Path(systemDir, jobId.toString());
    Path splitFile = new Path(jobDir, "job.split");
    LOG.info("Uploading splits file for " + jobId + " to " + splitFile);
    List<JobClient.RawSplit> splits = Arrays.asList(JobClient
        .getAndRemoveCachedSplits(jobId));
    JobClient.writeComputedSplits(jobConf, splits, splitFile);
  }
}
