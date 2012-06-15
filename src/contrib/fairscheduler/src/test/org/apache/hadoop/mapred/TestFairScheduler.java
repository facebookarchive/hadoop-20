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

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import junit.framework.TestCase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapred.FairScheduler.JobComparator;
import org.apache.hadoop.mapred.FairScheduler.JobInfo;
import org.apache.hadoop.mapred.PoolManager;
import org.apache.hadoop.mapred.FairScheduler.LocalityLevel;
import org.apache.hadoop.mapred.FairScheduler.LocalityLevelManager;
import org.apache.hadoop.mapred.JobInProgress.KillInterruptedException;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.mapreduce.server.jobtracker.TaskTracker;
import org.apache.hadoop.net.Node;
import org.junit.Assert;

public class TestFairScheduler extends TestCase {
  final static String TEST_DIR = new File(System.getProperty("test.build.data",
      "build/contrib/streaming/test/data")).getAbsolutePath();
  final static String ALLOC_FILE = new File(TEST_DIR,
      "test-pools").getAbsolutePath();

  private static final String POOL_PROPERTY = "pool";
  private static final int LOCALITY_DELAY = 10000;
  private static final int LOCALITY_DELAY_NODE_LOCAL = 5000;
  private static final int DEFAULT_MAP_SLOTS = 5;
  private static final int DEFAULT_REDUCE_SLOTS = 6;
  private static final double ALLOW_ERROR = 0.1;
  private static final double ALLOW_DEFICIT_ERROR = 10;

  private static int jobCounter;
  private FakeLocalityLevelManager localManager;

  static class FakeLocalityLevelManager extends LocalityLevelManager {
    LocalityLevel fakeLocalityLevel = LocalityLevel.NODE;
    public void setFakeLocalityLevel(LocalityLevel level) {
      this.fakeLocalityLevel = level;
    }
    /**
     * Obtain LocalityLevel of a task from its job and tasktracker.
     */
    @Override
    public FairScheduler.LocalityLevel taskToLocalityLevel(JobInProgress job,
        Task mapTask, TaskTrackerStatus tracker) {
      return fakeLocalityLevel;
    }
  }

  class FakeJobInProgress extends JobInProgress {

    private FakeTaskTrackerManager taskTrackerManager;
    // Locality used in obtainNewMapTasks
    private int fakeCacheLevel;
    private int mapCounter = 0;
    private int reduceCounter = 0;
    private boolean initialized = false;

    public FakeJobInProgress(JobConf jobConf,
        FakeTaskTrackerManager taskTrackerManager,
        JobTracker jt) throws IOException {
      super(new JobID("test", ++jobCounter), jobConf, jt);
      this.taskTrackerManager = taskTrackerManager;
      this.startTime = System.currentTimeMillis();
      this.status = new JobStatus();
      this.status.setRunState(JobStatus.PREP);
      this.setFakeCacheLevel(LocalityLevel.NODE);
      this.nonLocalMaps = new LinkedList<TaskInProgress>();
      this.nonLocalRunningMaps = new LinkedHashSet<TaskInProgress>();
      this.runningMapCache = new IdentityHashMap<Node, Set<TaskInProgress>>();
      this.nonRunningReduces = new LinkedList<TaskInProgress>();
      this.runningReduces = new LinkedHashSet<TaskInProgress>();
    }

    @Override
    public synchronized void initTasks() throws IOException {
      // initTasks is needed to create non-empty cleanup and setup TIP
      // arrays, otherwise calls such as job.getTaskInProgress will fail
      JobID jobId = getJobID();
      JobConf conf = getJobConf();
      String jobFile = "";
      // create two cleanup tips, one map and one reduce.
      cleanup = new TaskInProgress[2];
      // cleanup map tip.
      cleanup[0] = new TaskInProgress(jobId, jobFile, null,
              conf, this, 1, 1);
      cleanup[0].setJobCleanupTask();
      // cleanup reduce tip.
      cleanup[1] = new TaskInProgress(jobId, jobFile, null,
              conf, this, 1, 1);
      cleanup[1].setJobCleanupTask();
      // create two setup tips, one map and one reduce.
      setup = new TaskInProgress[2];
      // setup map tip.
      setup[0] = new TaskInProgress(jobId, jobFile, null,
              conf, this, 1, 1);
      setup[0].setJobSetupTask();
      // setup reduce tip.
      setup[1] = new TaskInProgress(jobId, jobFile, null,
              conf, this, 1, 1);
      setup[1].setJobSetupTask();
      // create maps
      numMapTasks = conf.getNumMapTasks();
      maps = new TaskInProgress[numMapTasks];
      for (int i = 0; i < numMapTasks; i++) {
        maps[i] = new FakeTaskInProgress(getJobID(),
            getJobConf(), true, this);
      }
      // create reduces
      numReduceTasks = conf.getNumReduceTasks();
      reduces = new TaskInProgress[numReduceTasks];
      for (int i = 0; i < numReduceTasks; i++) {
        reduces[i] = new FakeTaskInProgress(getJobID(),
            getJobConf(), false, this);
      }
      refreshIfNecessary();
      initialized = true;
    }

    public boolean isInitialized() {
      return initialized;
    }

    /*
     * Set the best locality in obtainNewMapTask
     */
    public void setFakeCacheLevel(LocalityLevel level) {
      switch (level) {
      case NODE: fakeCacheLevel = 1;
                 break;
      case RACK: fakeCacheLevel = 2;
                 break;
      default: fakeCacheLevel = Integer.MAX_VALUE;
      }
    }

    @Override
    public Task obtainNewMapTask(final TaskTrackerStatus tts, int clusterSize,
        int numUniqueHosts, int maxCacheLevel) throws IOException {
      if (maxCacheLevel < fakeCacheLevel) {
        // If the best locality is still worse than maxCacheLevel, return null.
        return null;
      }
      if (runningMapTasks == maps.length) {
        return null;
      }
      TaskAttemptID attemptId = getTaskAttemptID(true);
      Task task = new MapTask("", attemptId, 0, "", new BytesWritable(), 1, getJobConf().getUser()) {
        @Override
        public String toString() {
          return String.format("%s on %s", getTaskID(), tts.getTrackerName());
        }
      };
      runningMapTasks++;
      FakeTaskInProgress tip =
        (FakeTaskInProgress) maps[attemptId.getTaskID().getId()];
      tip.createTaskAttempt(task, tts.getTrackerName());
      nonLocalRunningMaps.add(tip);
      taskTrackerManager.startTask(tts.getTrackerName(), task, tip);
      return task;
    }

    @Override
    public Task obtainNewReduceTask(final TaskTrackerStatus tts,
        int clusterSize, int ignored) throws IOException {
      TaskAttemptID attemptId = getTaskAttemptID(false);
      Task task = new ReduceTask("", attemptId, 0, 10, 1, getJobConf().getUser()) {
        @Override
        public String toString() {
          return String.format("%s on %s", getTaskID(), tts.getTrackerName());
        }
      };
      if (runningReduceTasks == reduces.length) {
        return null;
      }
      runningReduceTasks++;
      FakeTaskInProgress tip =
        (FakeTaskInProgress) reduces[attemptId.getTaskID().getId()];
      tip.createTaskAttempt(task, tts.getTrackerName());
      runningReduces.add(tip);
      taskTrackerManager.startTask(tts.getTrackerName(), task, tip);
      return task;
    }

    public void mapTaskFinished(TaskInProgress tip) {
      runningMapTasks--;
      finishedMapTasks++;
      nonLocalRunningMaps.remove(tip);
    }

    public void reduceTaskFinished(TaskInProgress tip) {
      runningReduceTasks--;
      finishedReduceTasks++;
      runningReduces.remove(tip);
    }

    private TaskAttemptID getTaskAttemptID(boolean isMap) {
      JobID jobId = getJobID();
      TaskType t = TaskType.REDUCE;
      if (isMap) {
        t = TaskType.MAP;
        return new TaskAttemptID(jobId.getJtIdentifier(),
            jobId.getId(), isMap, mapCounter++, 0);
      } else {
        return new TaskAttemptID(jobId.getJtIdentifier(),
            jobId.getId(), isMap, reduceCounter++, 0);
      }
    }
  }

  class FakeTaskInProgress extends TaskInProgress {
    private boolean isMap;
    private FakeJobInProgress fakeJob;
    private TreeMap<TaskAttemptID, String> activeTasks;
    private TaskStatus taskStatus;
    private boolean isComplete = false;

    FakeTaskInProgress(JobID jId, JobConf jobConf, boolean isMap,
                       FakeJobInProgress job) {
      super(jId, "", new JobClient.RawSplit(), jobConf, job, 0, 1);
      this.isMap = isMap;
      this.fakeJob = job;
      activeTasks = new TreeMap<TaskAttemptID, String>();
      taskStatus = TaskStatus.createTaskStatus(isMap);
      taskStatus.setRunState(TaskStatus.State.UNASSIGNED);
    }

    private void createTaskAttempt(Task task, String taskTracker) {
      activeTasks.put(task.getTaskID(), taskTracker);
      taskStatus = TaskStatus.createTaskStatus(isMap, task.getTaskID(),
                                  0.5f, 1, TaskStatus.State.RUNNING, "", "", "",
                                  TaskStatus.Phase.STARTING, new Counters());
      taskStatus.setStartTime(clock.getTime());
    }

    @Override
    TreeMap<TaskAttemptID, String> getActiveTasks() {
      return activeTasks;
    }

    public synchronized boolean isComplete() {
      return isComplete;
    }

    public boolean isRunning() {
      return activeTasks.size() > 0;
    }

    @Override
    public TaskStatus getTaskStatus(TaskAttemptID taskid) {
      return taskStatus;
    }

    void killAttempt() {
      if (isMap) {
        fakeJob.mapTaskFinished(this);
      }
      else {
        fakeJob.reduceTaskFinished(this);
      }
      activeTasks.clear();
      taskStatus.setRunState(TaskStatus.State.UNASSIGNED);
    }

    void finishAttempt() {
      isComplete = true;
      if (isMap) {
        fakeJob.mapTaskFinished(this);
      }
      else {
        fakeJob.reduceTaskFinished(this);
      }
      activeTasks.clear();
      taskStatus.setRunState(TaskStatus.State.UNASSIGNED);
    }
  }

  static class FakeQueueManager extends QueueManager {
    private Set<String> queues = null;
    FakeQueueManager() {
      super(new Configuration());
    }
    void setQueues(Set<String> queues) {
      this.queues = queues;
    }
    public synchronized Set<String> getQueues() {
      return queues;
    }
  }

  static class FakeTaskTrackerManager implements TaskTrackerManager {
    int maps = 0;
    int reduces = 0;
    int maxMapTasksPerTracker = 2;
    int maxReduceTasksPerTracker = 2;
    List<JobInProgressListener> listeners =
      new ArrayList<JobInProgressListener>();

    private Map<String, TaskTracker> trackers =
      new HashMap<String, TaskTracker>();
    private Map<String, TaskStatus> statuses =
      new HashMap<String, TaskStatus>();
    private Map<String, FakeTaskInProgress> tips =
      new HashMap<String, FakeTaskInProgress>();
    private Map<String, TaskTrackerStatus> trackerForTip =
      new HashMap<String, TaskTrackerStatus>();
    private Map<JobID, JobInProgress> jobs =
      new HashMap<JobID, JobInProgress>();

    public FakeTaskTrackerManager(int numTrackers) {
      for (int i = 1; i <= numTrackers; i++) {
        TaskTracker tt = new TaskTracker("tt" + i);
        tt.setStatus(new TaskTrackerStatus("tt" + i,  "host" + i, i,
            new ArrayList<TaskStatus>(), 0,
            maxMapTasksPerTracker, maxReduceTasksPerTracker));
        trackers.put("tt" + i, tt);
      }
    }

    @Override
    public ClusterStatus getClusterStatus() {
      int numTrackers = trackers.size();
      return new ClusterStatus(numTrackers, maps, reduces,
          numTrackers * maxMapTasksPerTracker,
          numTrackers * maxReduceTasksPerTracker,
          JobTracker.State.RUNNING);
    }

    @Override
    public QueueManager getQueueManager() {
      return null;
    }

    @Override
    public int getNumberOfUniqueHosts() {
      return 0;
    }

    @Override
    public Collection<TaskTrackerStatus> taskTrackers() {
      List<TaskTrackerStatus> statuses = new ArrayList<TaskTrackerStatus>();
      for (TaskTracker tt : trackers.values()) {
        statuses.add(tt.getStatus());
      }
      return statuses;
    }


    @Override
    public void addJobInProgressListener(JobInProgressListener listener) {
      listeners.add(listener);
    }

    @Override
    public void removeJobInProgressListener(JobInProgressListener listener) {
      listeners.remove(listener);
    }

    @Override
    public int getNextHeartbeatInterval() {
      return MRConstants.HEARTBEAT_INTERVAL_MIN;
    }

    @Override
    public void killJob(JobID jobid) {
      return;
    }

    @Override
    public JobInProgress getJob(JobID jobid) {
      return jobs.get(jobid);
    }

    public void initJob (JobInProgress job) {
      try {
        job.initTasks();
      } catch (KillInterruptedException e) {
      } catch (IOException e) {
      }
      job.status.setRunState(JobStatus.RUNNING);
    }

    public void failJob (JobInProgress job) {
      // do nothing
    }

    // Test methods

    public void submitJob(JobInProgress job) throws IOException {
      for (JobInProgressListener listener : listeners) {
        listener.jobAdded(job);
      }
      jobs.put(job.getJobID(), job);
    }

    public TaskTracker getTaskTracker(String trackerID) {
      return trackers.get(trackerID);
    }

    public void startTask(String trackerName, Task t, FakeTaskInProgress tip) {
      final boolean isMap = t.isMapTask();
      if (isMap) {
        maps++;
      } else {
        reduces++;
      }
      String attemptId = t.getTaskID().toString();
      TaskStatus status = tip.getTaskStatus(t.getTaskID());
      TaskTrackerStatus trackerStatus = trackers.get(trackerName).getStatus();
      tips.put(attemptId, tip);
      statuses.put(attemptId, status);
      trackerForTip.put(attemptId, trackerStatus);
      statuses.put(t.getTaskID().toString(), status);
      status.setRunState(TaskStatus.State.RUNNING);
      trackerStatus.getTaskReports().add(status);
    }

    public void finishTask(String taskTrackerName, String attemptId) {
      FakeTaskInProgress tip = tips.get(attemptId);
      if (tip.isMapTask()) {
        maps--;
      } else {
        reduces--;
      }
      tip.finishAttempt();
      TaskStatus status = statuses.get(attemptId);
      trackers.get(taskTrackerName).getStatus().getTaskReports().remove(status);
    }

    @Override
    public boolean killTask(TaskAttemptID attemptId, boolean shouldFail, String reason) {
      String attemptIdStr = attemptId.toString();
      FakeTaskInProgress tip = tips.get(attemptIdStr);
      if (tip.isMapTask()) {
        maps--;
      } else {
        reduces--;
      }
      tip.killAttempt();
      TaskStatus status = statuses.get(attemptIdStr);
      trackerForTip.get(attemptIdStr).getTaskReports().remove(status);
      return true;
    }
  }

  protected class FakeClock extends FairScheduler.Clock {
    private long time = 1000; // Set it to something > 0 to avoid exception

    public void advance(long millis) {
      time += millis;
    }

    @Override
    long getTime() {
      return time;
    }
  }

  protected JobConf conf;
  protected FairScheduler scheduler;
  private FakeTaskTrackerManager taskTrackerManager;
  private FakeClock clock;
  private JobTracker jobTracker;

  @Override
  protected void setUp() throws Exception {
    jobCounter = 0;
    new File(TEST_DIR).mkdirs(); // Make sure data directory exists
    // Create an empty pools file (so we can add/remove pools later)
    FileWriter fileWriter = new FileWriter(ALLOC_FILE);
    fileWriter.write("<?xml version=\"1.0\"?>\n");
    fileWriter.write("<allocations />\n");
    fileWriter.close();
    setUpCluster(2);
  }

  private void setUpCluster(int numTaskTrackers) {
    conf = new JobConf();
    conf.set("mapred.fairscheduler.allocation.file", ALLOC_FILE);
    conf.set("mapred.fairscheduler.poolnameproperty", POOL_PROPERTY);
    conf.set("mapred.fairscheduler.assignmultiple", "false");
    conf.set("mapred.fairscheduler.server.address", "localhost:0");
    conf.setInt("mapred.fairscheduler.map.tasks.maximum", DEFAULT_MAP_SLOTS);
    conf.setInt("mapred.fairscheduler.reduce.tasks.maximum", DEFAULT_REDUCE_SLOTS);
    taskTrackerManager = new FakeTaskTrackerManager(numTaskTrackers);
    // Manually set locality delay because we aren't using a JobTracker so
    // we can't auto-compute it from the heartbeat interval.
    conf.setLong("mapred.fairscheduler.locality.delay", LOCALITY_DELAY);
    conf.setLong("mapred.fairscheduler.locality.delay.nodelocal",
                 LOCALITY_DELAY_NODE_LOCAL);
    taskTrackerManager = new FakeTaskTrackerManager(numTaskTrackers);
    clock = new FakeClock();
    localManager = new FakeLocalityLevelManager();
    scheduler = new FairScheduler(clock, false, localManager);
    scheduler.waitForMapsBeforeLaunchingReduces = false;
    scheduler.setConf(conf);
    scheduler.setTaskTrackerManager(taskTrackerManager);
    scheduler.start();
    jobTracker = UtilsForTests.getJobTracker();
  }

  @Override
  protected void tearDown() throws Exception {
    new File(TEST_DIR).delete();
    if (scheduler != null) {
      scheduler.terminate();
    }
    if (jobTracker != null) {
      jobTracker.infoServer.stop();
    }
  }

  private JobInProgress submitJob(int state, int maps, int reduces)
      throws IOException {
    return submitJob(state, maps, reduces, null);
  }

  private JobInProgress submitJobNoInitialization(
      int state, int maps, int reduces) throws IOException {
    return submitJobNoInitialization(state, maps, reduces, null);
  }

  private JobInProgress submitJobNoInitialization(
      int state, int maps, int reduces, String pool) throws IOException {
    JobConf jobConf = new JobConf(conf);
    jobConf.setNumMapTasks(maps);
    jobConf.setNumReduceTasks(reduces);
    if (pool != null)
      jobConf.set(POOL_PROPERTY, pool);
    JobInProgress job = new FakeJobInProgress(jobConf, taskTrackerManager,
        jobTracker);
    job.getStatus().setRunState(state);
    taskTrackerManager.submitJob(job);
    job.startTime = clock.time;
    return job;
  }

  private JobInProgress submitJob(int state, int maps, int reduces, String pool)
      throws IOException {
    JobInProgress job = submitJobNoInitialization(state, maps, reduces, pool);
    try {
      job.initTasks();
    } catch (KillInterruptedException e) {
    }
    scheduler.update();
    return job;
  }

  protected void submitJobs(int number, int state, int maps, int reduces)
    throws IOException {
    for (int i = 0; i < number; i++) {
      submitJob(state, maps, reduces);
    }
  }

  public void testAllocationFileParsing() throws Exception {
    PrintWriter out = new PrintWriter(new FileWriter(ALLOC_FILE));
    out.println("<?xml version=\"1.0\"?>");
    out.println("<allocations>");
    // Give pool A a minimum of 1 map, 2 reduces
    out.println("<pool name=\"pool_a\">");
    out.println("<minMaps>1</minMaps>");
    out.println("<minReduces>2</minReduces>");
    out.println("<maxTotalInitedTasks>100000</maxTotalInitedTasks>");
    out.println("</pool>");
    // Give pool B a minimum of 2 maps, 1 reduce
    out.println("<pool name=\"pool_b\">");
    out.println("<minMaps>2</minMaps>");
    out.println("<minReduces>1</minReduces>");
    out.println("</pool>");
    // Give pool B a maximum of 4 maps, 2 reduce
    out.println("<pool name=\"pool_b\">");
    out.println("<maxMaps>4</maxMaps>");
    out.println("<maxReduces>2</maxReduces>");
    out.println("<maxTotalInitedTasks>20000</maxTotalInitedTasks>");
    out.println("</pool>");
    // Give pool C min maps but no min reduces
    out.println("<pool name=\"pool_c\">");
    out.println("<minMaps>2</minMaps>");
    out.println("</pool>");
    // Give pool C a maximum of 4 maps, no maximum reduces
    out.println("<pool name=\"pool_c\">");
    out.println("<maxMaps>4</maxMaps>");
    out.println("</pool>");
    // Give pool D a limit of 3 running jobs
    out.println("<pool name=\"pool_d\">");
    out.println("<maxRunningJobs>3</maxRunningJobs>");
    out.println("</pool>");
    // Give pool E a preemption timeout of one minute
    out.println("<pool name=\"pool_e\">");
    out.println("<minSharePreemptionTimeout>60</minSharePreemptionTimeout>");
    out.println("</pool>");
    // Set default limit of jobs per user to 5
    out.println("<userMaxJobsDefault>5</userMaxJobsDefault>");
    // Give user1 a limit of 10 jobs
    out.println("<user name=\"user1\">");
    out.println("<maxRunningJobs>10</maxRunningJobs>");
    out.println("</user>");
    // Set default min share preemption timeout to 2 minutes
    out.println("<defaultMinSharePreemptionTimeout>120"
        + "</defaultMinSharePreemptionTimeout>");
    // Set fair share preemption timeout to 5 minutes
    out.println("<fairSharePreemptionTimeout>300</fairSharePreemptionTimeout>");
    out.println("<defaultMaxTotalInitedTasks>50000" +
                "</defaultMaxTotalInitedTasks>");
    out.println("</allocations>");
    out.close();

    PoolManager poolManager = scheduler.getPoolManager();
    poolManager.reloadAllocs();

    assertEquals(6, poolManager.getPools().size()); // 5 in file + default pool
    assertEquals(0, poolManager.getMinSlots(Pool.DEFAULT_POOL_NAME,
        TaskType.MAP));
    assertEquals(0, poolManager.getMinSlots(Pool.DEFAULT_POOL_NAME,
        TaskType.REDUCE));
    assertEquals(1, poolManager.getMinSlots("pool_a", TaskType.MAP));
    assertEquals(2, poolManager.getMinSlots("pool_a", TaskType.REDUCE));
    assertEquals(2, poolManager.getMinSlots("pool_b", TaskType.MAP));
    assertEquals(1, poolManager.getMinSlots("pool_b", TaskType.REDUCE));
    assertEquals(2, poolManager.getMinSlots("pool_c", TaskType.MAP));
    assertEquals(0, poolManager.getMinSlots("pool_c", TaskType.REDUCE));
    assertEquals(0, poolManager.getMinSlots("pool_d", TaskType.MAP));
    assertEquals(0, poolManager.getMinSlots("pool_d", TaskType.REDUCE));
    assertEquals(0, poolManager.getMinSlots("pool_e", TaskType.MAP));
    assertEquals(0, poolManager.getMinSlots("pool_e", TaskType.REDUCE));
    assertEquals(10, poolManager.getUserMaxJobs("user1"));
    assertEquals(5, poolManager.getUserMaxJobs("user2"));
    assertEquals(4, poolManager.getMaxSlots("pool_b", TaskType.MAP));
    assertEquals(2, poolManager.getMaxSlots("pool_b", TaskType.REDUCE));
    assertEquals(4, poolManager.getMaxSlots("pool_c", TaskType.MAP));
    assertEquals(100000, poolManager.getPoolMaxInitedTasks("pool_a"));
    assertEquals(20000, poolManager.getPoolMaxInitedTasks("pool_b"));
    assertEquals(50000, poolManager.getPoolMaxInitedTasks("pool_c"));
    assertEquals(50000, poolManager.getPoolMaxInitedTasks("pool_d"));
    assertEquals(50000, poolManager.getPoolMaxInitedTasks("pool_e"));
    assertEquals(Integer.MAX_VALUE,
                 poolManager.getMaxSlots("pool_c", TaskType.REDUCE));
    assertEquals(120000, poolManager.getMinSharePreemptionTimeout(
        Pool.DEFAULT_POOL_NAME));
    assertEquals(120000, poolManager.getMinSharePreemptionTimeout("pool_a"));
    assertEquals(120000, poolManager.getMinSharePreemptionTimeout("pool_b"));
    assertEquals(120000, poolManager.getMinSharePreemptionTimeout("pool_c"));
    assertEquals(120000, poolManager.getMinSharePreemptionTimeout("pool_d"));
    assertEquals(120000, poolManager.getMinSharePreemptionTimeout("pool_a"));
    assertEquals(60000, poolManager.getMinSharePreemptionTimeout("pool_e"));
    assertEquals(300000, poolManager.getFairSharePreemptionTimeout());
  }

  public void testTaskNotAssignedWhenNoJobsArePresent() throws IOException {
    assertNull(scheduler.assignTasks(tracker("tt1")));
  }

  public void testNonRunningJobsAreIgnored() throws IOException {
    submitJobs(1, JobStatus.SUCCEEDED, 10, 10);
    submitJobs(1, JobStatus.FAILED, 10, 10);
    submitJobs(1, JobStatus.KILLED, 10, 10);
    assertNull(scheduler.assignTasks(tracker("tt1")));
    advanceTime(100); // Check that we still don't assign jobs after an update
    assertNull(scheduler.assignTasks(tracker("tt1")));
  }

  /**
   * This test contains two jobs with fewer required tasks than there are slots.
   * We check that all tasks are assigned, but job 1 gets them first because it
   * was submitted earlier.
   */
  public void testSmallJobs() throws IOException {
    JobInProgress job1 = submitJob(JobStatus.RUNNING, 2, 1);
    JobInfo info1 = scheduler.infos.get(job1);

    // Check scheduler variables
    assertEquals(0,    info1.runningMaps);
    assertEquals(0,    info1.runningReduces);
    assertEquals(2,    info1.neededMaps);
    assertEquals(1,    info1.neededReduces);
    assertEquals(0,    info1.mapDeficit, ALLOW_DEFICIT_ERROR);
    assertEquals(0,    info1.reduceDeficit, ALLOW_DEFICIT_ERROR);
    assertEquals(2.0,  info1.mapFairShare, ALLOW_ERROR);
    assertEquals(1.0,  info1.reduceFairShare, ALLOW_ERROR);

    // Advance time before submitting another job j2, to make j1 run before j2
    // deterministically.
    advanceTime(100);
    JobInProgress job2 = submitJob(JobStatus.RUNNING, 1, 2);
    JobInfo info2 = scheduler.infos.get(job2);

    // Check scheduler variables; the fair shares should now have been allocated
    // equally between j1 and j2, but j1 should have (2 slots)*(100 ms) map
    // deficit and (1 slots) * (100 ms) reduce deficit
    assertEquals(0,    info1.runningMaps);
    assertEquals(0,    info1.runningReduces);
    assertEquals(2,    info1.neededMaps);
    assertEquals(1,    info1.neededReduces);
    assertEquals(200,  info1.mapDeficit, ALLOW_DEFICIT_ERROR);
    assertEquals(100,  info1.reduceDeficit, ALLOW_DEFICIT_ERROR);
    assertEquals(2.0,  info1.mapFairShare, ALLOW_ERROR);
    assertEquals(1.0,  info1.reduceFairShare, ALLOW_ERROR);
    assertEquals(0,    info2.runningMaps);
    assertEquals(0,    info2.runningReduces);
    assertEquals(1,    info2.neededMaps);
    assertEquals(2,    info2.neededReduces);
    assertEquals(0,    info2.mapDeficit, ALLOW_DEFICIT_ERROR);
    assertEquals(0,    info2.reduceDeficit, ALLOW_DEFICIT_ERROR);
    assertEquals(1.0,  info2.mapFairShare, ALLOW_ERROR);
    assertEquals(2.0,  info2.reduceFairShare, ALLOW_ERROR);

    // Assign tasks and check that all slots are filled with j1, then j2
    checkAssignment("tt1", "attempt_test_0001_m_000000_0 on tt1");
    checkAssignment("tt1", "attempt_test_0001_m_000001_0 on tt1");
    checkAssignment("tt1", "attempt_test_0001_r_000000_0 on tt1");
    checkAssignment("tt1", "attempt_test_0002_r_000000_0 on tt1");
    checkAssignment("tt2", "attempt_test_0002_m_000000_0 on tt2");
    checkAssignment("tt2", "attempt_test_0002_r_000001_0 on tt2");
    assertNull(scheduler.assignTasks(tracker("tt2")));

    // Check that the scheduler has started counting the tasks as running
    // as soon as it launched them.
    assertEquals(2,  info1.runningMaps);
    assertEquals(1,  info1.runningReduces);
    assertEquals(0,  info1.neededMaps);
    assertEquals(0,  info1.neededReduces);
    assertEquals(1,  info2.runningMaps);
    assertEquals(2,  info2.runningReduces);
    assertEquals(0, info2.neededMaps);
    assertEquals(0, info2.neededReduces);
  }

  /**
   * This test begins by submitting two jobs with 10 maps and reduces each.
   * The first job is submitted 100ms after the second, during which time no
   * tasks run. After this, we assign tasks to all slots, which should all be
   * from job 1. These run for 200ms, at which point job 2 now has a deficit
   * of 400 while job 1 is down to a deficit of 0. We then finish all tasks and
   * assign new ones, which should all be from job 2. These run for 50 ms,
   * which is not enough time for job 2 to make up its deficit (it only makes up
   * 100 ms of deficit). Finally we assign a new round of tasks, which should
   * all be from job 2 again.
   */
  public void testLargeJobs() throws IOException {
    JobInProgress job1 = submitJob(JobStatus.RUNNING, 10, 10);
    JobInfo info1 = scheduler.infos.get(job1);

    // Check scheduler variables
    assertEquals(0,    info1.runningMaps);
    assertEquals(0,    info1.runningReduces);
    assertEquals(10,   info1.neededMaps);
    assertEquals(10,   info1.neededReduces);
    assertEquals(0,    info1.mapDeficit, ALLOW_DEFICIT_ERROR);
    assertEquals(0,    info1.reduceDeficit, ALLOW_DEFICIT_ERROR);
    assertEquals(4.0,  info1.mapFairShare, ALLOW_ERROR);
    assertEquals(4.0,  info1.reduceFairShare, ALLOW_ERROR);

    // Advance time before submitting another job j2, to make j1 run before j2
    // deterministically.
    advanceTime(100);
    JobInProgress job2 = submitJob(JobStatus.RUNNING, 10, 10);
    JobInfo info2 = scheduler.infos.get(job2);

    // Check scheduler variables; the fair shares should now have been allocated
    // equally between j1 and j2, but j1 should have (4 slots)*(100 ms) deficit
    assertEquals(0,    info1.runningMaps);
    assertEquals(0,    info1.runningReduces);
    assertEquals(10,   info1.neededMaps);
    assertEquals(10,   info1.neededReduces);
    assertEquals(400,  info1.mapDeficit, ALLOW_DEFICIT_ERROR);
    assertEquals(400,  info1.reduceDeficit, ALLOW_DEFICIT_ERROR);
    assertEquals(2.0,  info1.mapFairShare, ALLOW_ERROR);
    assertEquals(2.0,  info1.reduceFairShare, ALLOW_ERROR);
    assertEquals(0,    info2.runningMaps);
    assertEquals(0,    info2.runningReduces);
    assertEquals(10,   info2.neededMaps);
    assertEquals(10,   info2.neededReduces);
    assertEquals(0,    info2.mapDeficit, ALLOW_DEFICIT_ERROR);
    assertEquals(0,    info2.reduceDeficit, ALLOW_DEFICIT_ERROR);
    assertEquals(2.0,  info2.mapFairShare, ALLOW_ERROR);
    assertEquals(2.0,  info2.reduceFairShare, ALLOW_ERROR);

    // Assign tasks and check that all slots are initially filled with job 1
    checkAssignment("tt1", "attempt_test_0001_m_000000_0 on tt1");
    checkAssignment("tt1", "attempt_test_0001_m_000001_0 on tt1");
    checkAssignment("tt1", "attempt_test_0001_r_000000_0 on tt1");
    checkAssignment("tt1", "attempt_test_0001_r_000001_0 on tt1");
    checkAssignment("tt2", "attempt_test_0001_m_000002_0 on tt2");
    checkAssignment("tt2", "attempt_test_0001_m_000003_0 on tt2");
    checkAssignment("tt2", "attempt_test_0001_r_000002_0 on tt2");
    checkAssignment("tt2", "attempt_test_0001_r_000003_0 on tt2");

    // Check that no new tasks can be launched once the tasktrackers are full
    assertNull(scheduler.assignTasks(tracker("tt1")));
    assertNull(scheduler.assignTasks(tracker("tt2")));

    // Check that the scheduler has started counting the tasks as running
    // as soon as it launched them.
    assertEquals(4,  info1.runningMaps);
    assertEquals(4,  info1.runningReduces);
    assertEquals(6,  info1.neededMaps);
    assertEquals(6,  info1.neededReduces);
    assertEquals(0,  info2.runningMaps);
    assertEquals(0,  info2.runningReduces);
    assertEquals(10, info2.neededMaps);
    assertEquals(10, info2.neededReduces);

    // Finish up the tasks and advance time again. Note that we must finish
    // the task since FakeJobInProgress does not properly maintain running
    // tasks, so the scheduler will always get an empty task list from
    // the JobInProgress's getTasks(TaskType.MAP)/getTasks(TaskType.REDUCE) and
    // think they finished.
    taskTrackerManager.finishTask("tt1", "attempt_test_0001_m_000000_0");
    taskTrackerManager.finishTask("tt1", "attempt_test_0001_m_000001_0");
    taskTrackerManager.finishTask("tt1", "attempt_test_0001_r_000000_0");
    taskTrackerManager.finishTask("tt1", "attempt_test_0001_r_000001_0");
    taskTrackerManager.finishTask("tt2", "attempt_test_0001_m_000002_0");
    taskTrackerManager.finishTask("tt2", "attempt_test_0001_m_000003_0");
    taskTrackerManager.finishTask("tt2", "attempt_test_0001_r_000002_0");
    taskTrackerManager.finishTask("tt2", "attempt_test_0001_r_000003_0");
    advanceTime(200);
    assertEquals(0,   info1.runningMaps);
    assertEquals(0,   info1.runningReduces);
    assertEquals(0,   info1.mapDeficit, ALLOW_DEFICIT_ERROR);
    assertEquals(0,   info1.reduceDeficit, ALLOW_DEFICIT_ERROR);
    assertEquals(0,   info2.runningMaps);
    assertEquals(0,   info2.runningReduces);
    assertEquals(400, info2.mapDeficit, ALLOW_DEFICIT_ERROR);
    assertEquals(400, info2.reduceDeficit, ALLOW_DEFICIT_ERROR);

    // Assign tasks and check that all slots are now filled with job 2
    checkAssignment("tt1", "attempt_test_0002_m_000000_0 on tt1");
    checkAssignment("tt1", "attempt_test_0002_m_000001_0 on tt1");
    checkAssignment("tt1", "attempt_test_0002_r_000000_0 on tt1");
    checkAssignment("tt1", "attempt_test_0002_r_000001_0 on tt1");
    checkAssignment("tt2", "attempt_test_0002_m_000002_0 on tt2");
    checkAssignment("tt2", "attempt_test_0002_m_000003_0 on tt2");
    checkAssignment("tt2", "attempt_test_0002_r_000002_0 on tt2");
    checkAssignment("tt2", "attempt_test_0002_r_000003_0 on tt2");

    // Finish up the tasks and advance time again, but give job 2 only 50ms.
    taskTrackerManager.finishTask("tt1", "attempt_test_0002_m_000000_0");
    taskTrackerManager.finishTask("tt1", "attempt_test_0002_m_000001_0");
    taskTrackerManager.finishTask("tt1", "attempt_test_0002_r_000000_0");
    taskTrackerManager.finishTask("tt1", "attempt_test_0002_r_000001_0");
    taskTrackerManager.finishTask("tt2", "attempt_test_0002_m_000002_0");
    taskTrackerManager.finishTask("tt2", "attempt_test_0002_m_000003_0");
    taskTrackerManager.finishTask("tt2", "attempt_test_0002_r_000002_0");
    taskTrackerManager.finishTask("tt2", "attempt_test_0002_r_000003_0");
    advanceTime(50);
    assertEquals(0,   info1.runningMaps);
    assertEquals(0,   info1.runningReduces);
    assertEquals(100, info1.mapDeficit, ALLOW_DEFICIT_ERROR);
    assertEquals(100, info1.reduceDeficit, ALLOW_DEFICIT_ERROR);
    assertEquals(0,   info2.runningMaps);
    assertEquals(0,   info2.runningReduces);
    assertEquals(300, info2.mapDeficit, ALLOW_DEFICIT_ERROR);
    assertEquals(300, info2.reduceDeficit, ALLOW_DEFICIT_ERROR);

    // Assign tasks and check that all slots are now still with job 2
    checkAssignment("tt1", "attempt_test_0002_m_000004_0 on tt1");
    checkAssignment("tt1", "attempt_test_0002_m_000005_0 on tt1");
    checkAssignment("tt1", "attempt_test_0002_r_000004_0 on tt1");
    checkAssignment("tt1", "attempt_test_0002_r_000005_0 on tt1");
    checkAssignment("tt2", "attempt_test_0002_m_000006_0 on tt2");
    checkAssignment("tt2", "attempt_test_0002_m_000007_0 on tt2");
    checkAssignment("tt2", "attempt_test_0002_r_000006_0 on tt2");
    checkAssignment("tt2", "attempt_test_0002_r_000007_0 on tt2");
  }


  /**
   * We submit two jobs such that one has 2x the priority of the other, wait
   * for 100 ms, and check that the weights/deficits are okay and that the
   * tasks all go to the high-priority job.
   */
  public void testJobsWithPriorities() throws IOException {
    JobInProgress job1 = submitJob(JobStatus.RUNNING, 10, 10);
    JobInfo info1 = scheduler.infos.get(job1);
    JobInProgress job2 = submitJob(JobStatus.RUNNING, 10, 10);
    JobInfo info2 = scheduler.infos.get(job2);
    job2.setPriority(JobPriority.HIGH);
    scheduler.update();

    // Check scheduler variables
    assertEquals(0,    info1.runningMaps);
    assertEquals(0,    info1.runningReduces);
    assertEquals(10,   info1.neededMaps);
    assertEquals(10,   info1.neededReduces);
    assertEquals(0,    info1.mapDeficit, ALLOW_DEFICIT_ERROR);
    assertEquals(0,    info1.reduceDeficit, ALLOW_DEFICIT_ERROR);
    assertEquals(1.33, info1.mapFairShare, ALLOW_ERROR);
    assertEquals(1.33, info1.reduceFairShare, ALLOW_ERROR);
    assertEquals(0,    info2.runningMaps);
    assertEquals(0,    info2.runningReduces);
    assertEquals(10,   info2.neededMaps);
    assertEquals(10,   info2.neededReduces);
    assertEquals(0,    info2.mapDeficit, ALLOW_DEFICIT_ERROR);
    assertEquals(0,    info2.reduceDeficit, ALLOW_DEFICIT_ERROR);
    assertEquals(2.66, info2.mapFairShare, ALLOW_ERROR);
    assertEquals(2.66, info2.reduceFairShare, ALLOW_ERROR);

    // Advance time and check deficits
    advanceTime(100);
    assertEquals(133,  info1.mapDeficit, 1.0);
    assertEquals(133,  info1.reduceDeficit, 1.0);
    assertEquals(266,  info2.mapDeficit, 1.0);
    assertEquals(266,  info2.reduceDeficit, 1.0);

    // Assign tasks and check that all slots are filled with j1, then j2
    checkAssignment("tt1", "attempt_test_0002_m_000000_0 on tt1");
    checkAssignment("tt1", "attempt_test_0002_m_000001_0 on tt1");
    checkAssignment("tt1", "attempt_test_0002_r_000000_0 on tt1");
    checkAssignment("tt1", "attempt_test_0002_r_000001_0 on tt1");
    checkAssignment("tt2", "attempt_test_0002_m_000002_0 on tt2");
    checkAssignment("tt2", "attempt_test_0002_m_000003_0 on tt2");
    checkAssignment("tt2", "attempt_test_0002_r_000002_0 on tt2");
    checkAssignment("tt2", "attempt_test_0002_r_000003_0 on tt2");
  }

  /**
   * We submit two jobs at interval of 200 such that job2 has 2x the priority
   * of the job1, then wait for 100 ms, and check that all slots are assigned
   * to job 1 even though job 2 has higher priority and fair scheduler would
   * have allocated atleast a few slots to job 2
   */
  public void testFifoJobScheduler() throws Exception {
     // Set up pools file
    PrintWriter out = new PrintWriter(new FileWriter(ALLOC_FILE));
    out.println("<?xml version=\"1.0\"?>");
    out.println("<allocations>");
    out.println("<pool name=\"pool_a\">");
    out.println("<minMaps>2</minMaps>");
    out.println("<minReduces>2</minReduces>");
    // enable fifo
    out.println("<fifo>true</fifo>");
    out.println("</pool>");
    out.println("</allocations>");
    out.close();
    scheduler.getPoolManager().reloadAllocs();

    JobInProgress job1 = submitJob(JobStatus.RUNNING, 10, 10, "pool_a");
    JobInfo info1 = scheduler.infos.get(job1);

    // Advance time 200ms and submit job 2
    advanceTime(200);
    JobInProgress job2 = submitJob(JobStatus.RUNNING, 10, 10, "pool_a");
    JobInfo info2 = scheduler.infos.get(job2);
    job2.setPriority(JobPriority.HIGH);
    // Advance time 100ms
    advanceTime(100);

    // Assign tasks and check that all slots are given to job1
    checkAssignment("tt1", "attempt_test_0001_m_000000_0 on tt1");
    checkAssignment("tt1", "attempt_test_0001_m_000001_0 on tt1");
    checkAssignment("tt1", "attempt_test_0001_r_000000_0 on tt1");
    checkAssignment("tt1", "attempt_test_0001_r_000001_0 on tt1");
    checkAssignment("tt2", "attempt_test_0001_m_000002_0 on tt2");
    checkAssignment("tt2", "attempt_test_0001_m_000003_0 on tt2");
    checkAssignment("tt2", "attempt_test_0001_r_000002_0 on tt2");
    checkAssignment("tt2", "attempt_test_0001_r_000003_0 on tt2");
  }

  /**
   * This test starts by submitting three large jobs:
   * - job1 in the default pool, at time 0
   * - job2 in pool_a, with an allocation of 1 map / 2 reduces, at time 200
   * - job3 in pool_b, with an allocation of 2 maps / 1 reduce, at time 200
   *
   * After this, we sleep 100ms, until time 300. At this point, job1 has the
   * highest map deficit, job3 the second, and job2 the third. This is because
   * job3 has more maps in its min share than job2, but job1 has been around
   * a long time at the beginning. The reduce deficits are similar, except job2
   * comes before job3 because it had a higher reduce minimum share.
   *
   * Finally, assign tasks to all slots. The maps should be assigned in the
   * order job3, job2, job1 because 3 and 2 both have guaranteed slots and 3
   * has a higher deficit. The reduces should be assigned as job2, job3, job1.
   */
  public void testLargeJobsWithPools() throws Exception {
    // Set up pools file
    PrintWriter out = new PrintWriter(new FileWriter(ALLOC_FILE));
    out.println("<?xml version=\"1.0\"?>");
    out.println("<allocations>");
    // Give pool A a minimum of 1 map, 2 reduces
    out.println("<pool name=\"pool_a\">");
    out.println("<minMaps>1</minMaps>");
    out.println("<minReduces>2</minReduces>");
    out.println("</pool>");
    // Give pool B a minimum of 2 maps, 1 reduce
    out.println("<pool name=\"pool_b\">");
    out.println("<minMaps>2</minMaps>");
    out.println("<minReduces>1</minReduces>");
    out.println("</pool>");
    out.println("</allocations>");
    out.close();
    scheduler.getPoolManager().reloadAllocs();

    JobInProgress job1 = submitJob(JobStatus.RUNNING, 10, 10);
    JobInfo info1 = scheduler.infos.get(job1);

    // Check scheduler variables
    assertEquals(0,    info1.runningMaps);
    assertEquals(0,    info1.runningReduces);
    assertEquals(10,   info1.neededMaps);
    assertEquals(10,   info1.neededReduces);
    assertEquals(0,    info1.mapDeficit, ALLOW_DEFICIT_ERROR);
    assertEquals(0,    info1.reduceDeficit, ALLOW_DEFICIT_ERROR);
    assertEquals(4.0,  info1.mapFairShare, ALLOW_ERROR);
    assertEquals(4.0,  info1.reduceFairShare, ALLOW_ERROR);

    // Advance time 200ms and submit jobs 2 and 3
    advanceTime(200);
    assertEquals(800,  info1.mapDeficit, ALLOW_DEFICIT_ERROR);
    assertEquals(800,  info1.reduceDeficit, ALLOW_DEFICIT_ERROR);
    JobInProgress job2 = submitJob(JobStatus.RUNNING, 10, 10, "pool_a");
    JobInfo info2 = scheduler.infos.get(job2);
    JobInProgress job3 = submitJob(JobStatus.RUNNING, 10, 10, "pool_b");
    JobInfo info3 = scheduler.infos.get(job3);

    // Check that minimum and fair shares have been allocated
    assertEquals(0,    info1.minMaps);
    assertEquals(0,    info1.minReduces);
    assertEquals(1.0,  info1.mapFairShare, ALLOW_ERROR);
    assertEquals(1.0,  info1.reduceFairShare, ALLOW_ERROR);
    assertEquals(1,    info2.minMaps);
    assertEquals(2,    info2.minReduces);
    assertEquals(1.0,  info2.mapFairShare, ALLOW_ERROR);
    assertEquals(2.0,  info2.reduceFairShare, ALLOW_ERROR);
    assertEquals(2,    info3.minMaps);
    assertEquals(1,    info3.minReduces);
    assertEquals(2.0,  info3.mapFairShare, ALLOW_ERROR);
    assertEquals(1.0,  info3.reduceFairShare, ALLOW_ERROR);

    // Advance time 100ms and check deficits
    advanceTime(100);
    assertEquals(900,  info1.mapDeficit, ALLOW_DEFICIT_ERROR);
    assertEquals(900,  info1.reduceDeficit, ALLOW_DEFICIT_ERROR);
    assertEquals(100,  info2.mapDeficit, ALLOW_DEFICIT_ERROR);
    assertEquals(200,  info2.reduceDeficit, ALLOW_DEFICIT_ERROR);
    assertEquals(200,  info3.mapDeficit, ALLOW_DEFICIT_ERROR);
    assertEquals(100,  info3.reduceDeficit, ALLOW_DEFICIT_ERROR);

    // Assign tasks and check that slots are first given to needy jobs
    checkAssignment("tt1", "attempt_test_0003_m_000000_0 on tt1");
    checkAssignment("tt1", "attempt_test_0003_m_000001_0 on tt1");
    checkAssignment("tt1", "attempt_test_0002_r_000000_0 on tt1");
    checkAssignment("tt1", "attempt_test_0002_r_000001_0 on tt1");
    checkAssignment("tt2", "attempt_test_0002_m_000000_0 on tt2");
    checkAssignment("tt2", "attempt_test_0001_m_000000_0 on tt2");
    checkAssignment("tt2", "attempt_test_0003_r_000000_0 on tt2");
    checkAssignment("tt2", "attempt_test_0001_r_000000_0 on tt2");
  }

  /**
   * This test starts by submitting three large jobs:
   * - job1 in the default pool, at time 0
   * - job2 in pool_a, with an allocation of 2 maps / 2 reduces, at time 200
   * - job3 in pool_a, with an allocation of 2 maps / 2 reduces, at time 300
   *
   * After this, we sleep 100ms, until time 400. At this point, job1 has the
   * highest deficit, job2 the second, and job3 the third. The first two tasks
   * should be assigned to job2 and job3 since they are in a pool with an
   * allocation guarantee, but the next two slots should be assigned to job 3
   * because the pool will no longer be needy.
   */
  public void testLargeJobsWithExcessCapacity() throws Exception {
    // Set up pools file
    PrintWriter out = new PrintWriter(new FileWriter(ALLOC_FILE));
    out.println("<?xml version=\"1.0\"?>");
    out.println("<allocations>");
    // Give pool A a minimum of 2 maps, 2 reduces
    out.println("<pool name=\"pool_a\">");
    out.println("<minMaps>2</minMaps>");
    out.println("<minReduces>2</minReduces>");
    out.println("</pool>");
    out.println("</allocations>");
    out.close();
    scheduler.getPoolManager().reloadAllocs();

    JobInProgress job1 = submitJob(JobStatus.RUNNING, 10, 10);
    JobInfo info1 = scheduler.infos.get(job1);

    // Check scheduler variables
    assertEquals(0,    info1.runningMaps);
    assertEquals(0,    info1.runningReduces);
    assertEquals(10,   info1.neededMaps);
    assertEquals(10,   info1.neededReduces);
    assertEquals(0,    info1.mapDeficit, ALLOW_DEFICIT_ERROR);
    assertEquals(0,    info1.reduceDeficit, ALLOW_DEFICIT_ERROR);
    assertEquals(4.0,  info1.mapFairShare, ALLOW_ERROR);
    assertEquals(4.0,  info1.reduceFairShare, ALLOW_ERROR);

    // Advance time 200ms and submit job 2
    advanceTime(200);
    assertEquals(800,  info1.mapDeficit, ALLOW_DEFICIT_ERROR);
    assertEquals(800,  info1.reduceDeficit, ALLOW_DEFICIT_ERROR);
    JobInProgress job2 = submitJob(JobStatus.RUNNING, 10, 10, "pool_a");
    JobInfo info2 = scheduler.infos.get(job2);

    // Check that minimum and fair shares have been allocated
    assertEquals(0,    info1.minMaps);
    assertEquals(0,    info1.minReduces);
    assertEquals(2.0,  info1.mapFairShare, ALLOW_ERROR);
    assertEquals(2.0,  info1.reduceFairShare, ALLOW_ERROR);
    assertEquals(2,    info2.minMaps);
    assertEquals(2,    info2.minReduces);
    assertEquals(2.0,  info2.mapFairShare, ALLOW_ERROR);
    assertEquals(2.0,  info2.reduceFairShare, ALLOW_ERROR);

    // Advance time 100ms and submit job 3
    advanceTime(100);
    assertEquals(1000, info1.mapDeficit, ALLOW_DEFICIT_ERROR);
    assertEquals(1000, info1.reduceDeficit, ALLOW_DEFICIT_ERROR);
    assertEquals(200,  info2.mapDeficit, ALLOW_DEFICIT_ERROR);
    assertEquals(200,  info2.reduceDeficit, ALLOW_DEFICIT_ERROR);
    JobInProgress job3 = submitJob(JobStatus.RUNNING, 10, 10, "pool_a");
    JobInfo info3 = scheduler.infos.get(job3);

    // Check that minimum and fair shares have been allocated
    assertEquals(0,    info1.minMaps);
    assertEquals(0,    info1.minReduces);
    assertEquals(2,    info1.mapFairShare, ALLOW_ERROR);
    assertEquals(2,    info1.reduceFairShare, ALLOW_ERROR);
    assertEquals(1,    info2.minMaps);
    assertEquals(1,    info2.minReduces);
    assertEquals(1,    info2.mapFairShare, ALLOW_ERROR);
    assertEquals(1,    info2.reduceFairShare, ALLOW_ERROR);
    assertEquals(1,    info3.minMaps);
    assertEquals(1,    info3.minReduces);
    assertEquals(1,    info3.mapFairShare, ALLOW_ERROR);
    assertEquals(1,    info3.reduceFairShare, ALLOW_ERROR);

    // Advance time 100ms and check deficits
    advanceTime(100);
    assertEquals(1200, info1.mapDeficit, ALLOW_DEFICIT_ERROR);
    assertEquals(1200, info1.reduceDeficit, ALLOW_DEFICIT_ERROR);
    assertEquals(300,  info2.mapDeficit, ALLOW_DEFICIT_ERROR);
    assertEquals(300,  info2.reduceDeficit, ALLOW_DEFICIT_ERROR);
    assertEquals(100,  info3.mapDeficit, ALLOW_DEFICIT_ERROR);
    assertEquals(100,  info3.reduceDeficit, ALLOW_DEFICIT_ERROR);

    // Assign tasks and check that slots are first given to needy jobs, but
    // that job 1 gets two tasks after due to having a larger deficit.
    checkAssignment("tt1", "attempt_test_0002_m_000000_0 on tt1");
    checkAssignment("tt1", "attempt_test_0003_m_000000_0 on tt1");
    checkAssignment("tt1", "attempt_test_0002_r_000000_0 on tt1");
    checkAssignment("tt1", "attempt_test_0003_r_000000_0 on tt1");
    checkAssignment("tt2", "attempt_test_0001_m_000000_0 on tt2");
    checkAssignment("tt2", "attempt_test_0001_m_000001_0 on tt2");
    checkAssignment("tt2", "attempt_test_0001_r_000000_0 on tt2");
    checkAssignment("tt2", "attempt_test_0001_r_000001_0 on tt2");
  }

  /**
   * This test starts by submitting two jobs at time 0:
   * - job1 in the default pool
   * - job2, with 1 map and 1 reduce, in pool_a, which has an alloc of 4
   *   maps and 4 reduces
   *
   * When we assign the slots, job2 should only get 1 of each type of task.
   *
   * The fair share for job 2 should be 2.0 however, because even though it is
   * running only one task, it accumulates deficit in case it will have failures
   * or need speculative tasks later. (TODO: This may not be a good policy.)
   */
  public void testSmallJobInLargePool() throws Exception {
    // Set up pools file
    PrintWriter out = new PrintWriter(new FileWriter(ALLOC_FILE));
    out.println("<?xml version=\"1.0\"?>");
    out.println("<allocations>");
    // Give pool A a minimum of 4 maps, 4 reduces
    out.println("<pool name=\"pool_a\">");
    out.println("<minMaps>4</minMaps>");
    out.println("<minReduces>4</minReduces>");
    out.println("</pool>");
    out.println("</allocations>");
    out.close();
    scheduler.getPoolManager().reloadAllocs();

    JobInProgress job1 = submitJob(JobStatus.RUNNING, 10, 10);
    JobInfo info1 = scheduler.infos.get(job1);
    JobInProgress job2 = submitJob(JobStatus.RUNNING, 1, 1, "pool_a");
    JobInfo info2 = scheduler.infos.get(job2);

    // Check scheduler variables
    assertEquals(0,    info1.runningMaps);
    assertEquals(0,    info1.runningReduces);
    assertEquals(10,   info1.neededMaps);
    assertEquals(10,   info1.neededReduces);
    assertEquals(0,    info1.mapDeficit, ALLOW_DEFICIT_ERROR);
    assertEquals(0,    info1.reduceDeficit, ALLOW_DEFICIT_ERROR);
    assertEquals(3.0,  info1.mapFairShare, ALLOW_ERROR);
    assertEquals(3.0,  info1.reduceFairShare, ALLOW_ERROR);
    assertEquals(0,    info2.runningMaps);
    assertEquals(0,    info2.runningReduces);
    assertEquals(1,    info2.neededMaps);
    assertEquals(1,    info2.neededReduces);
    assertEquals(0,    info2.mapDeficit, ALLOW_DEFICIT_ERROR);
    assertEquals(0,    info2.reduceDeficit, ALLOW_DEFICIT_ERROR);
    assertEquals(1.0,  info2.mapFairShare, ALLOW_ERROR);
    assertEquals(1.0,  info2.reduceFairShare, ALLOW_ERROR);

    // Assign tasks and check that slots are first given to needy jobs
    checkAssignment("tt1", "attempt_test_0002_m_000000_0 on tt1");
    checkAssignment("tt1", "attempt_test_0001_m_000000_0 on tt1");
    checkAssignment("tt1", "attempt_test_0002_r_000000_0 on tt1");
    checkAssignment("tt1", "attempt_test_0001_r_000000_0 on tt1");
    checkAssignment("tt2", "attempt_test_0001_m_000001_0 on tt2");
    checkAssignment("tt2", "attempt_test_0001_m_000002_0 on tt2");
    checkAssignment("tt2", "attempt_test_0001_r_000001_0 on tt2");
    checkAssignment("tt2", "attempt_test_0001_r_000002_0 on tt2");
  }

  /**
   * This test starts by submitting four jobs in the default pool. However, the
   * maxRunningJobs limit for this pool has been set to two. We should see only
   * the first two jobs get scheduled, each with half the total slots.
   */
  public void testPoolMaxJobs() throws Exception {
    // Set up pools file
    PrintWriter out = new PrintWriter(new FileWriter(ALLOC_FILE));
    out.println("<?xml version=\"1.0\"?>");
    out.println("<allocations>");
    out.println("<pool name=\"default\">");
    out.println("<maxRunningJobs>2</maxRunningJobs>");
    out.println("</pool>");
    out.println("</allocations>");
    out.close();
    scheduler.getPoolManager().reloadAllocs();

    // Submit jobs, advancing time in-between to make sure that they are
    // all submitted at distinct times.
    JobInProgress job1 = submitJobNoInitialization(JobStatus.PREP, 10, 10);
    JobInfo info1 = scheduler.infos.get(job1);
    advanceTime(10);
    JobInProgress job2 = submitJobNoInitialization(JobStatus.PREP, 10, 10);
    JobInfo info2 = scheduler.infos.get(job2);
    advanceTime(10);
    JobInProgress job3 = submitJobNoInitialization(JobStatus.PREP, 10, 10);
    JobInfo info3 = scheduler.infos.get(job3);
    advanceTime(10);
    JobInProgress job4 = submitJobNoInitialization(JobStatus.PREP, 10, 10);
    JobInfo info4 = scheduler.infos.get(job4);

    Thread.sleep(1000L); // Let JobInitializaer to finish the work

    // Only two of the jobs should be initialized.
    assertTrue(((FakeJobInProgress)job1).isInitialized());
    assertTrue(((FakeJobInProgress)job2).isInitialized());
    assertFalse(((FakeJobInProgress)job3).isInitialized());
    assertFalse(((FakeJobInProgress)job4).isInitialized());

    // Check scheduler variables
    assertEquals(2.0,  info1.mapFairShare, ALLOW_ERROR);
    assertEquals(2.0,  info1.reduceFairShare, ALLOW_ERROR);
    assertEquals(2.0,  info2.mapFairShare, ALLOW_ERROR);
    assertEquals(2.0,  info2.reduceFairShare, ALLOW_ERROR);
    assertEquals(0.0,  info3.mapFairShare, ALLOW_ERROR);
    assertEquals(0.0,  info3.reduceFairShare, ALLOW_ERROR);
    assertEquals(0.0,  info4.mapFairShare, ALLOW_ERROR);
    assertEquals(0.0,  info4.reduceFairShare, ALLOW_ERROR);

    // Assign tasks and check that slots are first to jobs 1 and 2
    checkAssignment("tt1", "attempt_test_0001_m_000000_0 on tt1");
    checkAssignment("tt1", "attempt_test_0001_m_000001_0 on tt1");
    checkAssignment("tt1", "attempt_test_0001_r_000000_0 on tt1");
    checkAssignment("tt1", "attempt_test_0001_r_000001_0 on tt1");
    advanceTime(100);
    checkAssignment("tt2", "attempt_test_0002_m_000000_0 on tt2");
    checkAssignment("tt2", "attempt_test_0002_m_000001_0 on tt2");
    checkAssignment("tt2", "attempt_test_0002_r_000000_0 on tt2");
    checkAssignment("tt2", "attempt_test_0002_r_000001_0 on tt2");
  }

  /**
   * This test configures a pool pool_a, and redirects the default to it.
   */
  public void testPoolRedirect() throws Exception {
    // Set up pools file
    // pool_a has 0 totalInitedTasks, default does not have that restriction.
    // The redirect from default -> pool_a should enforce 0 total inited tasks.
    PrintWriter out = new PrintWriter(new FileWriter(ALLOC_FILE));
    out.println("<?xml version=\"1.0\"?>");
    out.println("<allocations>");
    out.println("<pool name=\"default\">");
    out.println("<maxTotalInitedTasks>100</maxTotalInitedTasks>");
    out.println("<redirect>pool_a</redirect>");
    out.println("</pool>");
    out.println("<pool name=\"pool_a\">");
    out.println("<maxTotalInitedTasks>0</maxTotalInitedTasks>");
    out.println("</pool>");
    out.println("</allocations>");
    out.close();
    scheduler.getPoolManager().reloadAllocs();

    // Submit a job.
    JobInProgress job1 = submitJobNoInitialization(JobStatus.PREP, 10, 10);
    JobInfo info1 = scheduler.infos.get(job1);
    advanceTime(10);
    Thread.sleep(1000L); // Let JobInitializaer to finish the work

    // Should have gone to pool_a, not default
    assertEquals(info1.poolName, "pool_a");
  }

  /**
   * This test configures a pool pool_a, tries the submit a job
   * before and after blacklisting of pool_a.
   */
  public void testpool_blacklisted() throws Exception {
    // Set up pools file
    PrintWriter out = new PrintWriter(new FileWriter(ALLOC_FILE));
    out.println("<?xml version=\"1.0\"?>");
    out.println("<allocations>");
    out.println("<pool name=\"default\">");
    out.println("<maxTotalInitedTasks>100</maxTotalInitedTasks>");
    out.println("</pool>");
    out.println("<pool name=\"pool_a\">");
    out.println("<maxTotalInitedTasks>0</maxTotalInitedTasks>");
    out.println("</pool>");
    out.println("</allocations>");
    out.close();
    scheduler.getPoolManager().reloadAllocs();

    // Submit a job to the not blacklisted pool_a
    JobInProgress job1 =
        submitJobNoInitialization(JobStatus.PREP, 10, 10, "pool_a");
    JobInfo info1 = scheduler.infos.get(job1);
    advanceTime(10);
    Thread.sleep(1000L); // Let JobInitializaer to finish the work

    out = new PrintWriter(new FileWriter(ALLOC_FILE));
    out.println("<?xml version=\"1.0\"?>");
    out.println("<allocations>");
    out.println("<pool name=\"default\">");
    out.println("<maxTotalInitedTasks>100</maxTotalInitedTasks>");
    out.println("</pool>");
    out.println("<pool name=\"pool_a\">");
    out.println("<maxTotalInitedTasks>0</maxTotalInitedTasks>");
    out.println("<blacklisted/>");
    out.println("</pool>");
    out.println("</allocations>");
    out.close();
    scheduler.getPoolManager().reloadAllocs();

    // Submit a job to the newly blacklisted pool_a
    JobInProgress job2 =
        submitJobNoInitialization(JobStatus.PREP, 10, 10, "pool_a");
    JobInfo info2 = scheduler.infos.get(job2);
    advanceTime(10);
    Thread.sleep(1000L); // Let JobInitializaer to finish the work

    // pool_a is not blacklisted, so goes to pool_a
    assertEquals(info1.poolName, "pool_a");
    // pool_a is blacklisted, so goes to default
    assertEquals(info2.poolName, "default");
  }

  /**
   * This test starts by submitting four jobs in the default pool. However, the
   * maxTotalInitedTasks for this pool has been set to 40. We should see only
   * the first two jobs get initialized, each with half the total slots.
   */
  public void testPoolMaxTasks() throws Exception {
    // Set up pools file
    PrintWriter out = new PrintWriter(new FileWriter(ALLOC_FILE));
    out.println("<?xml version=\"1.0\"?>");
    out.println("<allocations>");
    out.println("<defaultMaxTotalInitedTasks>10</defaultMaxTotalInitedTasks>");
    out.println("<pool name=\"default\">");
    out.println("<maxTotalInitedTasks>40</maxTotalInitedTasks>");
    out.println("</pool>");
    out.println("</allocations>");
    out.close();
    scheduler.getPoolManager().reloadAllocs();

    // Submit jobs, advancing time in-between to make sure that they are
    // all submitted at distinct times.
    JobInProgress job1 = submitJobNoInitialization(JobStatus.PREP, 10, 10);
    JobInfo info1 = scheduler.infos.get(job1);
    advanceTime(10);
    JobInProgress job2 = submitJobNoInitialization(JobStatus.PREP, 10, 10);
    JobInfo info2 = scheduler.infos.get(job2);
    advanceTime(10);
    JobInProgress job3 = submitJobNoInitialization(JobStatus.PREP, 10, 10);
    JobInfo info3 = scheduler.infos.get(job3);
    advanceTime(10);
    JobInProgress job4 = submitJobNoInitialization(JobStatus.PREP, 10, 10);
    JobInfo info4 = scheduler.infos.get(job4);
    advanceTime(10);
    JobInProgress job5 =
        submitJobNoInitialization(JobStatus.PREP, 10, 10, "pool_a");
    JobInfo info5 = scheduler.infos.get(job5);
    advanceTime(10);
    JobInProgress job6 =
        submitJobNoInitialization(JobStatus.PREP, 10, 10, "pool_a");
    JobInfo info6 = scheduler.infos.get(job6);
    advanceTime(10);

    Thread.sleep(1000L); // Let JobInitializaer to finish the work

    // Only three of the jobs should be initialized.
    assertTrue(((FakeJobInProgress)job1).isInitialized());
    assertTrue(((FakeJobInProgress)job2).isInitialized());
    assertFalse(((FakeJobInProgress)job3).isInitialized());
    assertFalse(((FakeJobInProgress)job4).isInitialized());
    assertTrue(((FakeJobInProgress)job5).isInitialized());
    assertFalse(((FakeJobInProgress)job6).isInitialized());

    scheduler.update();
    // Check scheduler variables
    assertEquals(1.0, info1.mapFairShare, ALLOW_ERROR);
    assertEquals(1.0, info1.reduceFairShare, ALLOW_ERROR);
    assertEquals(110, info1.mapDeficit, ALLOW_DEFICIT_ERROR);
    assertEquals(110, info1.reduceDeficit, ALLOW_DEFICIT_ERROR);
    assertEquals(1.0, info2.mapFairShare, ALLOW_ERROR);
    assertEquals(1.0, info2.reduceFairShare, ALLOW_ERROR);
    assertEquals(70,  info2.mapDeficit, ALLOW_DEFICIT_ERROR);
    assertEquals(70,  info2.reduceDeficit, ALLOW_DEFICIT_ERROR);
    assertEquals(0.0, info3.mapFairShare, ALLOW_ERROR);
    assertEquals(0.0, info3.reduceFairShare, ALLOW_ERROR);
    assertEquals(0,   info3.mapDeficit, ALLOW_DEFICIT_ERROR);
    assertEquals(0,   info3.reduceDeficit, ALLOW_DEFICIT_ERROR);
    assertEquals(0.0, info4.mapFairShare, ALLOW_ERROR);
    assertEquals(0.0, info4.reduceFairShare, ALLOW_ERROR);
    assertEquals(0,   info4.mapDeficit, ALLOW_DEFICIT_ERROR);
    assertEquals(0,   info4.reduceDeficit, ALLOW_DEFICIT_ERROR);
    assertEquals(2.0, info5.mapFairShare, ALLOW_ERROR);
    assertEquals(2.0, info5.reduceFairShare, ALLOW_ERROR);
    assertEquals(20,  info5.mapDeficit, ALLOW_DEFICIT_ERROR);
    assertEquals(20,  info5.reduceDeficit, ALLOW_DEFICIT_ERROR);
    assertEquals(0.0, info6.mapFairShare, ALLOW_ERROR);
    assertEquals(0.0, info6.reduceFairShare, ALLOW_ERROR);
    assertEquals(0,   info6.mapDeficit, ALLOW_DEFICIT_ERROR);
    assertEquals(0,   info6.reduceDeficit, ALLOW_DEFICIT_ERROR);

    // Assign tasks and check that slots are first to jobs 1 and 2
    checkAssignment("tt1", "attempt_test_0001_m_000000_0 on tt1");
    checkAssignment("tt1", "attempt_test_0001_m_000001_0 on tt1");
    checkAssignment("tt1", "attempt_test_0001_r_000000_0 on tt1");
    checkAssignment("tt1", "attempt_test_0001_r_000001_0 on tt1");
    advanceTime(100);
    checkAssignment("tt2", "attempt_test_0005_m_000000_0 on tt2");
    checkAssignment("tt2", "attempt_test_0005_m_000001_0 on tt2");
    checkAssignment("tt2", "attempt_test_0005_r_000000_0 on tt2");
    checkAssignment("tt2", "attempt_test_0005_r_000001_0 on tt2");
  }

  /**
   * This test starts by submitting two jobs by user "user1" to the default
   * pool, and two jobs by "user2". We set user1's job limit to 1. We should
   * see one job from user1 and two from user2.
   */
  public void testUserMaxJobs() throws Exception {
    // Set up pools file
    PrintWriter out = new PrintWriter(new FileWriter(ALLOC_FILE));
    out.println("<?xml version=\"1.0\"?>");
    out.println("<allocations>");
    out.println("<user name=\"user1\">");
    out.println("<maxRunningJobs>1</maxRunningJobs>");
    out.println("</user>");
    out.println("</allocations>");
    out.close();
    scheduler.getPoolManager().reloadAllocs();

    // Submit jobs, advancing time in-between to make sure that they are
    // all submitted at distinct times.
    JobInProgress job1 = submitJob(JobStatus.RUNNING, 10, 10);
    job1.getJobConf().set("user.name", "user1");
    JobInfo info1 = scheduler.infos.get(job1);
    advanceTime(10);
    JobInProgress job2 = submitJob(JobStatus.RUNNING, 10, 10);
    job2.getJobConf().set("user.name", "user1");
    JobInfo info2 = scheduler.infos.get(job2);
    advanceTime(10);
    JobInProgress job3 = submitJob(JobStatus.RUNNING, 10, 10);
    job3.getJobConf().set("user.name", "user2");
    JobInfo info3 = scheduler.infos.get(job3);
    advanceTime(10);
    JobInProgress job4 = submitJob(JobStatus.RUNNING, 10, 10);
    job4.getJobConf().set("user.name", "user2");
    JobInfo info4 = scheduler.infos.get(job4);

    // Check scheduler variables
    assertEquals(1.33,  info1.mapFairShare, ALLOW_ERROR);
    assertEquals(1.33,  info1.reduceFairShare, ALLOW_ERROR);
    assertEquals(0.0,   info2.mapFairShare, ALLOW_ERROR);
    assertEquals(0.0,   info2.reduceFairShare, ALLOW_ERROR);
    assertEquals(1.33,  info3.mapFairShare, ALLOW_ERROR);
    assertEquals(1.33,  info3.reduceFairShare, ALLOW_ERROR);
    assertEquals(1.33,  info4.mapFairShare, ALLOW_ERROR);
    assertEquals(1.33,  info4.reduceFairShare, ALLOW_ERROR);

    // Assign tasks and check that slots are first to jobs 1 and 3
    checkAssignment("tt1", "attempt_test_0001_m_000000_0 on tt1");
    checkAssignment("tt1", "attempt_test_0001_m_000001_0 on tt1");
    checkAssignment("tt1", "attempt_test_0001_r_000000_0 on tt1");
    checkAssignment("tt1", "attempt_test_0001_r_000001_0 on tt1");
    advanceTime(100);
    checkAssignment("tt2", "attempt_test_0003_m_000000_0 on tt2");
    checkAssignment("tt2", "attempt_test_0003_m_000001_0 on tt2");
    checkAssignment("tt2", "attempt_test_0003_r_000000_0 on tt2");
    checkAssignment("tt2", "attempt_test_0003_r_000001_0 on tt2");
  }

  /**
   * Test a combination of pool job limits and user job limits, the latter
   * specified through both the userMaxJobsDefaults (for some users) and
   * user-specific &lt;user&gt; elements in the allocations file.
   */
  public void testComplexJobLimits() throws Exception {
    // Set up pools file
    PrintWriter out = new PrintWriter(new FileWriter(ALLOC_FILE));
    out.println("<?xml version=\"1.0\"?>");
    out.println("<allocations>");
    out.println("<pool name=\"pool_a\">");
    out.println("<maxRunningJobs>1</maxRunningJobs>");
    out.println("</pool>");
    out.println("<user name=\"user1\">");
    out.println("<maxRunningJobs>1</maxRunningJobs>");
    out.println("</user>");
    out.println("<user name=\"user2\">");
    out.println("<maxRunningJobs>10</maxRunningJobs>");
    out.println("</user>");
    out.println("<userMaxJobsDefault>2</userMaxJobsDefault>");
    out.println("</allocations>");
    out.close();
    scheduler.getPoolManager().reloadAllocs();

    // Submit jobs, advancing time in-between to make sure that they are
    // all submitted at distinct times.

    // Two jobs for user1; only one should get to run
    JobInProgress job1 = submitJob(JobStatus.RUNNING, 10, 10);
    job1.getJobConf().set("user.name", "user1");
    JobInfo info1 = scheduler.infos.get(job1);
    advanceTime(10);
    JobInProgress job2 = submitJob(JobStatus.RUNNING, 10, 10);
    job2.getJobConf().set("user.name", "user1");
    JobInfo info2 = scheduler.infos.get(job2);
    advanceTime(10);

    // Three jobs for user2; all should get to run
    JobInProgress job3 = submitJob(JobStatus.RUNNING, 10, 10);
    job3.getJobConf().set("user.name", "user2");
    JobInfo info3 = scheduler.infos.get(job3);
    advanceTime(10);
    JobInProgress job4 = submitJob(JobStatus.RUNNING, 10, 10);
    job4.getJobConf().set("user.name", "user2");
    JobInfo info4 = scheduler.infos.get(job4);
    advanceTime(10);
    JobInProgress job5 = submitJob(JobStatus.RUNNING, 10, 10);
    job5.getJobConf().set("user.name", "user2");
    JobInfo info5 = scheduler.infos.get(job5);
    advanceTime(10);

    // Three jobs for user3; only two should get to run
    JobInProgress job6 = submitJob(JobStatus.RUNNING, 10, 10);
    job6.getJobConf().set("user.name", "user3");
    JobInfo info6 = scheduler.infos.get(job6);
    advanceTime(10);
    JobInProgress job7 = submitJob(JobStatus.RUNNING, 10, 10);
    job7.getJobConf().set("user.name", "user3");
    JobInfo info7 = scheduler.infos.get(job7);
    advanceTime(10);
    JobInProgress job8 = submitJob(JobStatus.RUNNING, 10, 10);
    job8.getJobConf().set("user.name", "user3");
    JobInfo info8 = scheduler.infos.get(job8);
    advanceTime(10);

    // Two jobs for user4, in pool_a; only one should get to run
    JobInProgress job9 = submitJob(JobStatus.RUNNING, 10, 10, "pool_a");
    job9.getJobConf().set("user.name", "user4");
    JobInfo info9 = scheduler.infos.get(job9);
    advanceTime(10);
    JobInProgress job10 = submitJob(JobStatus.RUNNING, 10, 10, "pool_a");
    job10.getJobConf().set("user.name", "user4");
    JobInfo info10 = scheduler.infos.get(job10);
    advanceTime(10);

    // Check scheduler variables. The jobs in pool_a should get half
    // the total share, while those in the default pool should get
    // the other half. This works out to 2 slots each for the jobs
    // in pool_a and 1/3 each for the jobs in the default pool because
    // there are 2 runnable jobs in pool_a and 6 jobs in the default pool.
    assertEquals(0.33,   info1.mapFairShare, ALLOW_ERROR);
    assertEquals(0.33,   info1.reduceFairShare, ALLOW_ERROR);
    assertEquals(0.0,    info2.mapFairShare, ALLOW_ERROR);
    assertEquals(0.0,    info2.reduceFairShare, ALLOW_ERROR);
    assertEquals(0.33,   info3.mapFairShare, ALLOW_ERROR);
    assertEquals(0.33,   info3.reduceFairShare, ALLOW_ERROR);
    assertEquals(0.33,   info4.mapFairShare, ALLOW_ERROR);
    assertEquals(0.33,   info4.reduceFairShare, ALLOW_ERROR);
    assertEquals(0.33,   info5.mapFairShare, ALLOW_ERROR);
    assertEquals(0.33,   info5.reduceFairShare, ALLOW_ERROR);
    assertEquals(0.33,   info6.mapFairShare, ALLOW_ERROR);
    assertEquals(0.33,   info6.reduceFairShare, ALLOW_ERROR);
    assertEquals(0.33,   info7.mapFairShare, ALLOW_ERROR);
    assertEquals(0.33,   info7.reduceFairShare, ALLOW_ERROR);
    assertEquals(0.0,    info8.mapFairShare, ALLOW_ERROR);
    assertEquals(0.0,    info8.reduceFairShare, ALLOW_ERROR);
    assertEquals(2.0,    info9.mapFairShare, ALLOW_ERROR);
    assertEquals(2.0,    info9.reduceFairShare, ALLOW_ERROR);
    assertEquals(0.0,    info10.mapFairShare, ALLOW_ERROR);
    assertEquals(0.0,    info10.reduceFairShare, ALLOW_ERROR);
  }

  public void testSizeBasedWeight() throws Exception {
    scheduler.sizeBasedWeight = true;
    JobInProgress job1 = submitJob(JobStatus.RUNNING, 2, 10);
    JobInProgress job2 = submitJob(JobStatus.RUNNING, 20, 1);
    assertTrue(scheduler.infos.get(job2).mapFairShare >
               scheduler.infos.get(job1).mapFairShare);
    assertTrue(scheduler.infos.get(job1).reduceFairShare >
               scheduler.infos.get(job2).reduceFairShare);
  }

  /**
   * This test submits jobs in three pools: pool_a, which has a weight
   * of 2.0; pool_b, which has a weight of 0.5; and the default pool, which
   * should have a weight of 1.0. It then checks that the map and reduce
   * fair shares are given out accordingly. We then submit a second job to
   * pool B and check that each gets half of the pool (weight of 0.25).
   */
  public void testPoolWeights() throws Exception {
    // Set up pools file
    PrintWriter out = new PrintWriter(new FileWriter(ALLOC_FILE));
    out.println("<?xml version=\"1.0\"?>");
    out.println("<allocations>");
    out.println("<pool name=\"pool_a\">");
    out.println("<weight>2.0</weight>");
    out.println("</pool>");
    out.println("<pool name=\"pool_b\">");
    out.println("<weight>0.5</weight>");
    out.println("</pool>");
    out.println("</allocations>");
    out.close();
    scheduler.getPoolManager().reloadAllocs();

    // Submit jobs, advancing time in-between to make sure that they are
    // all submitted at distinct times.
    JobInProgress job1 = submitJob(JobStatus.RUNNING, 10, 10);
    JobInfo info1 = scheduler.infos.get(job1);
    JobInProgress job2 = submitJob(JobStatus.RUNNING, 10, 10, "pool_a");
    JobInfo info2 = scheduler.infos.get(job2);
    JobInProgress job3 = submitJob(JobStatus.RUNNING, 10, 10, "pool_b");
    JobInfo info3 = scheduler.infos.get(job3);
    advanceTime(10);

    assertEquals(1.14,  info1.mapFairShare, ALLOW_ERROR);
    assertEquals(1.14,  info1.reduceFairShare, ALLOW_ERROR);
    assertEquals(2.28,  info2.mapFairShare, ALLOW_ERROR);
    assertEquals(2.28,  info2.reduceFairShare, ALLOW_ERROR);
    assertEquals(0.57,  info3.mapFairShare, ALLOW_ERROR);
    assertEquals(0.57,  info3.reduceFairShare, ALLOW_ERROR);

    JobInProgress job4 = submitJob(JobStatus.RUNNING, 10, 10, "pool_b");
    JobInfo info4 = scheduler.infos.get(job4);
    advanceTime(10);

    assertEquals(1.14,  info1.mapFairShare, ALLOW_ERROR);
    assertEquals(1.14,  info1.reduceFairShare, ALLOW_ERROR);
    assertEquals(2.28,  info2.mapFairShare, ALLOW_ERROR);
    assertEquals(2.28,  info2.reduceFairShare, ALLOW_ERROR);
    assertEquals(0.28,  info3.mapFairShare, ALLOW_ERROR);
    assertEquals(0.28,  info3.reduceFairShare, ALLOW_ERROR);
    assertEquals(0.28,  info4.mapFairShare, ALLOW_ERROR);
    assertEquals(0.28,  info4.reduceFairShare, ALLOW_ERROR);
  }

  /**
   * This test submits jobs in two pools, pool_a and pool_b. None of the
   * jobs in pool_a have maps, but this should not affect their reduce
   * share.
   */
  public void testPoolWeightsWhenNoMaps() throws Exception {
    // Set up pools file
    PrintWriter out = new PrintWriter(new FileWriter(ALLOC_FILE));
    out.println("<?xml version=\"1.0\"?>");
    out.println("<allocations>");
    out.println("<pool name=\"pool_a\">");
    out.println("<weight>2.0</weight>");
    out.println("</pool>");
    out.println("<pool name=\"pool_b\">");
    out.println("<weight>1.0</weight>");
    out.println("</pool>");
    out.println("</allocations>");
    out.close();
    scheduler.getPoolManager().reloadAllocs();

    // Submit jobs, advancing time in-between to make sure that they are
    // all submitted at distinct times.
    JobInProgress job1 = submitJob(JobStatus.RUNNING, 0, 10, "pool_a");
    JobInfo info1 = scheduler.infos.get(job1);
    JobInProgress job2 = submitJob(JobStatus.RUNNING, 0, 10, "pool_a");
    JobInfo info2 = scheduler.infos.get(job2);
    JobInProgress job3 = submitJob(JobStatus.RUNNING, 10, 10, "pool_b");
    JobInfo info3 = scheduler.infos.get(job3);
    advanceTime(10);

    assertEquals(0,     info1.mapWeight, 0.01);
    assertEquals(1.0,   info1.reduceWeight, 0.01);
    assertEquals(0,     info2.mapWeight, 0.01);
    assertEquals(1.0,   info2.reduceWeight, 0.01);
    assertEquals(1.0,   info3.mapWeight, 0.01);
    assertEquals(1.0,   info3.reduceWeight, 0.01);

    assertEquals(0,     info1.mapFairShare, ALLOW_ERROR);
    assertEquals(1.33,  info1.reduceFairShare, ALLOW_ERROR);
    assertEquals(0,     info2.mapFairShare, ALLOW_ERROR);
    assertEquals(1.33,  info2.reduceFairShare, ALLOW_ERROR);
    assertEquals(4,     info3.mapFairShare, ALLOW_ERROR);
    assertEquals(1.33,  info3.reduceFairShare, ALLOW_ERROR);
  }

  /**
   * Tests that max-running-tasks per node are set by assigning load
   * equally accross the cluster in CapBasedLoadManager.
   */
  public void testCapBasedLoadManager() {
    CapBasedLoadManager loadMgr = new CapBasedLoadManager();
    // Arguments to getCap: totalRunnableTasks, nodeCap, totalSlots
    // Desired behavior: return ceil(nodeCap * min(1, runnableTasks/totalSlots))
    assertEquals(1, loadMgr.getCap(1, 1, 100));
    assertEquals(1, loadMgr.getCap(1, 2, 100));
    assertEquals(1, loadMgr.getCap(1, 10, 100));
    assertEquals(1, loadMgr.getCap(200, 1, 100));
    assertEquals(1, loadMgr.getCap(1, 5, 100));
    assertEquals(3, loadMgr.getCap(50, 5, 100));
    assertEquals(5, loadMgr.getCap(100, 5, 100));
    assertEquals(5, loadMgr.getCap(200, 5, 100));

    Configuration conf = new Configuration();
    conf.setFloat("mapred.fairscheduler.load.max.diff", 0.2f);
    loadMgr.setConf(conf);
    assertEquals(1, loadMgr.getCap(1, 1, 100));
    assertEquals(1, loadMgr.getCap(1, 2, 100));
    assertEquals(3, loadMgr.getCap(1, 10, 100));
    assertEquals(1, loadMgr.getCap(200, 1, 100));
    assertEquals(2, loadMgr.getCap(1, 5, 100));
    assertEquals(4, loadMgr.getCap(50, 5, 100));
    assertEquals(5, loadMgr.getCap(100, 5, 100));
    assertEquals(5, loadMgr.getCap(200, 5, 100));
  }

  /**
   * Tests that tasks cannot be assigned if there is not enough memory on TT.
   */
  public void testMemBasedLoadManager() {
    MemBasedLoadManager loadMgr = new MemBasedLoadManager();
    Configuration conf = new Configuration();
    conf.setLong(MemBasedLoadManager.RESERVED_PHYSICAL_MEMORY_ON_TT_STRING,
                 4 * 1024L);
    loadMgr.setConf(conf);

    // Not enough free memory on the TaskTracker
    TaskTrackerStatus tracker = new TaskTrackerStatus();
    tracker.getResourceStatus().
            setAvailablePhysicalMemory(3 * 1024 * 1024 * 1024L);
    assertFalse(loadMgr.canAssign(tracker));

    // Enough memory on the TaskTracker
    tracker.getResourceStatus().
            setAvailablePhysicalMemory(5 * 1024 * 1024 * 1024L);
    assertTrue(loadMgr.canAssign(tracker));
  }

  /**
   * This test starts by launching a job in the default pool that takes
   * all the slots in the cluster. We then submit a job in a pool with
   * min share of 2 maps and 1 reduce task. After the min share preemption
   * timeout, this job should be allowed to preempt tasks.
   */
  public void testMinSharePreemption() throws Exception {
    // Enable preemption in scheduler
    scheduler.preemptionEnabled = true;
    // Set up pools file
    PrintWriter out = new PrintWriter(new FileWriter(ALLOC_FILE));
    out.println("<?xml version=\"1.0\"?>");
    out.println("<allocations>");
    // Give pool A a min share of 2 maps and 1 reduce, and a preemption
    // timeout of 1 minute
    out.println("<pool name=\"pool_a\">");
    out.println("<minMaps>2</minMaps>");
    out.println("<minReduces>1</minReduces>");
    out.println("<minSharePreemptionTimeout>60</minSharePreemptionTimeout>");
    out.println("</pool>");
    out.println("</allocations>");
    out.close();
    scheduler.getPoolManager().reloadAllocs();

    // Submit job 1 and assign all slots to it. Sleep a bit before assigning
    // tasks on tt1 and tt2 to ensure that the ones on tt2 get preempted first.
    JobInProgress job1 = submitJob(JobStatus.RUNNING, 10, 10);
    checkAssignment("tt1", "attempt_test_0001_m_000000_0 on tt1");
    checkAssignment("tt1", "attempt_test_0001_m_000001_0 on tt1");
    checkAssignment("tt1", "attempt_test_0001_r_000000_0 on tt1");
    checkAssignment("tt1", "attempt_test_0001_r_000001_0 on tt1");
    advanceTime(100);
    checkAssignment("tt2", "attempt_test_0001_m_000002_0 on tt2");
    checkAssignment("tt2", "attempt_test_0001_m_000003_0 on tt2");
    checkAssignment("tt2", "attempt_test_0001_r_000002_0 on tt2");
    checkAssignment("tt2", "attempt_test_0001_r_000003_0 on tt2");

    // Ten seconds later, submit job 2.
    advanceTime(10000);
    JobInProgress job2 = submitJob(JobStatus.RUNNING, 10, 10, "pool_a");

    // Ten seconds later, check that job 2 is not able to preempt tasks.
    advanceTime(10000);
    assertEquals(0, scheduler.tasksToPreempt(job2, TaskType.MAP,
        clock.getTime()));
    assertEquals(0, scheduler.tasksToPreempt(job2, TaskType.REDUCE,
        clock.getTime()));

    // Advance time by 49 more seconds, putting us at 59s after the
    // submission of job 2. It should still not be able to preempt.
    advanceTime(49000);
    assertEquals(0, scheduler.tasksToPreempt(job2, TaskType.MAP,
        clock.getTime()));
    assertEquals(0, scheduler.tasksToPreempt(job2, TaskType.REDUCE,
        clock.getTime()));

    // Advance time by 2 seconds, putting us at 61s after the submission
    // of job 2. It should now be able to preempt 2 maps and 1 reduce.
    advanceTime(2000);
    assertEquals(2, scheduler.tasksToPreempt(job2, TaskType.MAP,
        clock.getTime()));
    assertEquals(1, scheduler.tasksToPreempt(job2, TaskType.REDUCE,
        clock.getTime()));

    // Test that the tasks actually get preempted and we can assign new ones
    scheduler.preemptTasksIfNecessary();
    scheduler.update();
    assertEquals(2, scheduler.runningTasks(job1, TaskType.MAP));
    assertEquals(3, scheduler.runningTasks(job1, TaskType.REDUCE));
    checkAssignment("tt2", "attempt_test_0002_m_000000_0 on tt2");
    checkAssignment("tt2", "attempt_test_0002_m_000001_0 on tt2");
    checkAssignment("tt2", "attempt_test_0002_r_000000_0 on tt2");
    assertNull(scheduler.assignTasks(tracker("tt1")));
    assertNull(scheduler.assignTasks(tracker("tt2")));
  }

  /**
   * This test starts by launching a job in the default pool that takes
   * all the slots in the cluster. We then submit a job in a pool with
   * min share of 3 maps and 3 reduce tasks, but which only actually
   * needs 1 map and 2 reduces. We check that this job does not prempt
   * more than this many tasks despite its min share being higher.
   */
  public void testMinSharePreemptionWithSmallJob() throws Exception {
    // Enable preemption in scheduler
    scheduler.preemptionEnabled = true;
    // Set up pools file
    PrintWriter out = new PrintWriter(new FileWriter(ALLOC_FILE));
    out.println("<?xml version=\"1.0\"?>");
    out.println("<allocations>");
    // Give pool A a min share of 2 maps and 1 reduce, and a preemption
    // timeout of 1 minute
    out.println("<pool name=\"pool_a\">");
    out.println("<minMaps>3</minMaps>");
    out.println("<minReduces>3</minReduces>");
    out.println("<minSharePreemptionTimeout>60</minSharePreemptionTimeout>");
    out.println("</pool>");
    out.println("</allocations>");
    out.close();
    scheduler.getPoolManager().reloadAllocs();

    // Submit job 1 and assign all slots to it. Sleep a bit before assigning
    // tasks on tt1 and tt2 to ensure that the ones on tt2 get preempted first.
    JobInProgress job1 = submitJob(JobStatus.RUNNING, 10, 10);
    checkAssignment("tt1", "attempt_test_0001_m_000000_0 on tt1");
    checkAssignment("tt1", "attempt_test_0001_m_000001_0 on tt1");
    checkAssignment("tt1", "attempt_test_0001_r_000000_0 on tt1");
    checkAssignment("tt1", "attempt_test_0001_r_000001_0 on tt1");
    advanceTime(100);
    checkAssignment("tt2", "attempt_test_0001_m_000002_0 on tt2");
    checkAssignment("tt2", "attempt_test_0001_m_000003_0 on tt2");
    checkAssignment("tt2", "attempt_test_0001_r_000002_0 on tt2");
    checkAssignment("tt2", "attempt_test_0001_r_000003_0 on tt2");

    // Ten seconds later, submit job 2.
    advanceTime(10000);
    JobInProgress job2 = submitJob(JobStatus.RUNNING, 1, 2, "pool_a");

    // Advance time by 59 seconds and check that no preemption occurs.
    advanceTime(59000);
    assertEquals(0, scheduler.tasksToPreempt(job2, TaskType.MAP,
        clock.getTime()));
    assertEquals(0, scheduler.tasksToPreempt(job2, TaskType.REDUCE,
        clock.getTime()));

    // Advance time by 2 seconds, putting us at 61s after the submission
    // of job 2. Job 2 should now preempt 1 map and 2 reduces.
    advanceTime(2000);
    assertEquals(1, scheduler.tasksToPreempt(job2, TaskType.MAP,
        clock.getTime()));
    assertEquals(2, scheduler.tasksToPreempt(job2, TaskType.REDUCE,
        clock.getTime()));

    // Test that the tasks actually get preempted and we can assign new ones
    scheduler.preemptTasksIfNecessary();
    scheduler.update();
    assertEquals(3, scheduler.runningTasks(job1, TaskType.MAP));
    assertEquals(2, scheduler.runningTasks(job1, TaskType.REDUCE));
    checkAssignment("tt2", "attempt_test_0002_m_000000_0 on tt2");
    checkAssignment("tt2", "attempt_test_0002_r_000000_0 on tt2");
    checkAssignment("tt2", "attempt_test_0002_r_000001_0 on tt2");
    assertNull(scheduler.assignTasks(tracker("tt1")));
    assertNull(scheduler.assignTasks(tracker("tt2")));
  }

  /**
   * This test runs on a 4-node (8-slot) cluster to allow 3 jobs with fair
   * shares greater than 2 slots to coexist (which makes the half-fair-share
   * of each job more than 1 so that fair share preemption can kick in).
   *
   * The test first launches job 1, which takes 6 map slots and 6 reduce slots.
   * We then submit job 2, which takes 2 slots of each type. Finally, we submit
   * a third job, job 3, which gets no slots. At this point the fair share
   * of each job will be 8/3 ~= 2.7 slots. Job 1 will be above its fair share,
   * job 2 will be below it but at half fair share, and job 3 will
   * be below half fair share. Therefore job 3 should be allowed to
   * preempt a task (after a timeout) but jobs 1 and 2 shouldn't.
   */
  public void testFairSharePreemption() throws Exception {
    // Create a bigger cluster than normal (4 tasktrackers instead of 2)
    setUpCluster(4);
    // Enable preemption in scheduler
    scheduler.preemptionEnabled = true;
    // Set up pools file with a fair share preemtion timeout of 1 minute
    PrintWriter out = new PrintWriter(new FileWriter(ALLOC_FILE));
    out.println("<?xml version=\"1.0\"?>");
    out.println("<allocations>");
    out.println("<fairSharePreemptionTimeout>60</fairSharePreemptionTimeout>");
    out.println("</allocations>");
    out.close();
    scheduler.getPoolManager().reloadAllocs();

    // Submit jobs 1 and 2. We advance time by 100 between each task tracker
    // assignment stage to ensure that the tasks from job1 on tt3 are the ones
    // that are deterministically preempted first (being the latest launched
    // tasks in an over-allocated job).
    JobInProgress job1 = submitJob(JobStatus.RUNNING, 6, 6);
    advanceTime(200); // Makes job 1 deterministically launch before job 2
    JobInProgress job2 = submitJob(JobStatus.RUNNING, 10, 10);
    checkAssignment("tt1", "attempt_test_0001_m_000000_0 on tt1");
    checkAssignment("tt1", "attempt_test_0001_m_000001_0 on tt1");
    checkAssignment("tt1", "attempt_test_0001_r_000000_0 on tt1");
    checkAssignment("tt1", "attempt_test_0001_r_000001_0 on tt1");
    advanceTime(100);
    checkAssignment("tt2", "attempt_test_0001_m_000002_0 on tt2");
    checkAssignment("tt2", "attempt_test_0001_m_000003_0 on tt2");
    checkAssignment("tt2", "attempt_test_0001_r_000002_0 on tt2");
    checkAssignment("tt2", "attempt_test_0001_r_000003_0 on tt2");
    advanceTime(100);
    checkAssignment("tt3", "attempt_test_0001_m_000004_0 on tt3");
    checkAssignment("tt3", "attempt_test_0001_m_000005_0 on tt3");
    checkAssignment("tt3", "attempt_test_0001_r_000004_0 on tt3");
    checkAssignment("tt3", "attempt_test_0001_r_000005_0 on tt3");
    advanceTime(100);
    checkAssignment("tt4", "attempt_test_0002_m_000000_0 on tt4");
    checkAssignment("tt4", "attempt_test_0002_m_000001_0 on tt4");
    checkAssignment("tt4", "attempt_test_0002_r_000000_0 on tt4");
    checkAssignment("tt4", "attempt_test_0002_r_000001_0 on tt4");

    // Submit job 3.
    JobInProgress job3 = submitJob(JobStatus.RUNNING, 10, 10);

    // Check that after 59 seconds, neither job can preempt
    advanceTime(59000);
    long now = clock.getTime();
    assertEquals(0, scheduler.tasksToPreempt(job2, TaskType.MAP, now));
    assertEquals(0, scheduler.tasksToPreempt(job2, TaskType.REDUCE, now));
    assertEquals(0, scheduler.tasksToPreempt(job3, TaskType.MAP, now));
    assertEquals(0, scheduler.tasksToPreempt(job3, TaskType.REDUCE, now));

    // Wait 2 more seconds, so that job 3 has now been in the system for 61s.
    // Now job 3 should be able to preempt 2 task but job 2 shouldn't.
    advanceTime(2000);

    JobInfo info1 = scheduler.infos.get(job1);
    JobInfo info2 = scheduler.infos.get(job2);
    JobInfo info3 = scheduler.infos.get(job3);

    final double ALLOWED_ERROR = 0.00001;
    assertEquals(8.0 / 3, info1.mapFairShare, ALLOW_ERROR);
    assertEquals(8.0 / 3, info2.mapFairShare, ALLOW_ERROR);
    assertEquals(8.0 / 3, info3.mapFairShare, ALLOW_ERROR);
    assertEquals(8.0 / 3, info1.reduceFairShare, ALLOW_ERROR);
    assertEquals(8.0 / 3, info2.reduceFairShare, ALLOW_ERROR);
    assertEquals(8.0 / 3, info3.reduceFairShare, ALLOW_ERROR);

    now = clock.getTime();
    assertEquals(0, scheduler.tasksToPreempt(job2, TaskType.MAP, now));
    assertEquals(0, scheduler.tasksToPreempt(job2, TaskType.REDUCE, now));
    assertEquals(2, scheduler.tasksToPreempt(job3, TaskType.MAP, now));
    assertEquals(2, scheduler.tasksToPreempt(job3, TaskType.REDUCE, now));

    // Test that the tasks actually get preempted and we can assign new ones
    scheduler.preemptTasksIfNecessary();
    scheduler.update();
    assertEquals(4, scheduler.runningTasks(job1, TaskType.MAP));
    assertEquals(4, scheduler.runningTasks(job1, TaskType.REDUCE));
    checkAssignment("tt3", "attempt_test_0003_m_000000_0 on tt3");
    checkAssignment("tt3", "attempt_test_0003_m_000001_0 on tt3");
    checkAssignment("tt3", "attempt_test_0003_r_000000_0 on tt3");
    checkAssignment("tt3", "attempt_test_0003_r_000001_0 on tt3");
    assertNull(scheduler.assignTasks(tracker("tt1")));
    assertNull(scheduler.assignTasks(tracker("tt2")));
    assertNull(scheduler.assignTasks(tracker("tt3")));
    assertNull(scheduler.assignTasks(tracker("tt4")));
  }

  /**
   * This test submits a job that takes all 4 slots, and then a second
   * job that has both a min share of 2 slots with a 60s timeout and a
   * fair share timeout of 60s. After 60 seconds, this job will be starved
   * of both min share (2 slots of each type) and fair share (2 slots of each
   * type), and we test that it does not kill more than 2 tasks of each type
   * in total.
   */
  public void testMinAndFairSharePreemption() throws Exception {
    // Enable preemption in scheduler
    scheduler.preemptionEnabled = true;
    // Set up pools file
    PrintWriter out = new PrintWriter(new FileWriter(ALLOC_FILE));
    out.println("<?xml version=\"1.0\"?>");
    out.println("<allocations>");
    // Give pool A a min share of 2 maps and 1 reduce, and a preemption
    // timeout of 1 minute
    out.println("<pool name=\"pool_a\">");
    out.println("<minMaps>2</minMaps>");
    out.println("<minReduces>2</minReduces>");
    out.println("<minSharePreemptionTimeout>60</minSharePreemptionTimeout>");
    out.println("</pool>");
    out.println("<fairSharePreemptionTimeout>60</fairSharePreemptionTimeout>");
    out.println("</allocations>");
    out.close();
    scheduler.getPoolManager().reloadAllocs();

    // Submit job 1 and assign all slots to it. Sleep a bit before assigning
    // tasks on tt1 and tt2 to ensure that the ones on tt2 get preempted first.
    JobInProgress job1 = submitJob(JobStatus.RUNNING, 10, 10);
    checkAssignment("tt1", "attempt_test_0001_m_000000_0 on tt1");
    checkAssignment("tt1", "attempt_test_0001_m_000001_0 on tt1");
    checkAssignment("tt1", "attempt_test_0001_r_000000_0 on tt1");
    checkAssignment("tt1", "attempt_test_0001_r_000001_0 on tt1");
    advanceTime(100);
    checkAssignment("tt2", "attempt_test_0001_m_000002_0 on tt2");
    checkAssignment("tt2", "attempt_test_0001_m_000003_0 on tt2");
    checkAssignment("tt2", "attempt_test_0001_r_000002_0 on tt2");
    checkAssignment("tt2", "attempt_test_0001_r_000003_0 on tt2");

    // Ten seconds later, submit job 2.
    advanceTime(10000);
    JobInProgress job2 = submitJob(JobStatus.RUNNING, 10, 10, "pool_a");

    // Ten seconds later, check that job 2 is not able to preempt tasks.
    advanceTime(10000);
    assertEquals(0, scheduler.tasksToPreempt(job2, TaskType.MAP,
        clock.getTime()));
    assertEquals(0, scheduler.tasksToPreempt(job2, TaskType.REDUCE,
        clock.getTime()));

    // Advance time by 49 more seconds, putting us at 59s after the
    // submission of job 2. It should still not be able to preempt.
    advanceTime(49000);
    assertEquals(0, scheduler.tasksToPreempt(job2, TaskType.MAP,
        clock.getTime()));
    assertEquals(0, scheduler.tasksToPreempt(job2, TaskType.REDUCE,
        clock.getTime()));

    // Advance time by 2 seconds, putting us at 61s after the submission
    // of job 2. It should now be able to preempt 2 maps and 1 reduce.
    advanceTime(2000);
    assertEquals(2, scheduler.tasksToPreempt(job2, TaskType.MAP,
        clock.getTime()));
    assertEquals(2, scheduler.tasksToPreempt(job2, TaskType.REDUCE,
        clock.getTime()));

    // Test that the tasks actually get preempted and we can assign new ones
    scheduler.preemptTasksIfNecessary();
    scheduler.update();
    assertEquals(2, scheduler.runningTasks(job1, TaskType.MAP));
    assertEquals(2, scheduler.runningTasks(job1, TaskType.REDUCE));
    checkAssignment("tt2", "attempt_test_0002_m_000000_0 on tt2");
    checkAssignment("tt2", "attempt_test_0002_m_000001_0 on tt2");
    checkAssignment("tt2", "attempt_test_0002_r_000000_0 on tt2");
    checkAssignment("tt2", "attempt_test_0002_r_000001_0 on tt2");
    assertNull(scheduler.assignTasks(tracker("tt1")));
    assertNull(scheduler.assignTasks(tracker("tt2")));
  }

  /**
   * This is a copy of testMinAndFairSharePreemption that turns preemption
   * off and verifies that no tasks get killed.
   */
  public void testNoPreemptionIfDisabled() throws Exception {
    // Set up pools file
    PrintWriter out = new PrintWriter(new FileWriter(ALLOC_FILE));
    out.println("<?xml version=\"1.0\"?>");
    out.println("<allocations>");
    // Give pool A a min share of 2 maps and 1 reduce, and a preemption
    // timeout of 1 minute
    out.println("<pool name=\"pool_a\">");
    out.println("<minMaps>2</minMaps>");
    out.println("<minReduces>2</minReduces>");
    out.println("<minSharePreemptionTimeout>60</minSharePreemptionTimeout>");
    out.println("</pool>");
    out.println("<fairSharePreemptionTimeout>60</fairSharePreemptionTimeout>");
    out.println("</allocations>");
    out.close();
    scheduler.getPoolManager().reloadAllocs();

    // Submit job 1 and assign all slots to it. Sleep a bit before assigning
    // tasks on tt1 and tt2 to ensure that the ones on tt2 get preempted first.
    JobInProgress job1 = submitJob(JobStatus.RUNNING, 10, 10);
    checkAssignment("tt1", "attempt_test_0001_m_000000_0 on tt1");
    checkAssignment("tt1", "attempt_test_0001_m_000001_0 on tt1");
    checkAssignment("tt1", "attempt_test_0001_r_000000_0 on tt1");
    checkAssignment("tt1", "attempt_test_0001_r_000001_0 on tt1");
    advanceTime(100);
    checkAssignment("tt2", "attempt_test_0001_m_000002_0 on tt2");
    checkAssignment("tt2", "attempt_test_0001_m_000003_0 on tt2");
    checkAssignment("tt2", "attempt_test_0001_r_000002_0 on tt2");
    checkAssignment("tt2", "attempt_test_0001_r_000003_0 on tt2");

    // Ten seconds later, submit job 2.
    advanceTime(10000);
    JobInProgress job2 = submitJob(JobStatus.RUNNING, 10, 10, "pool_a");

    // Advance time by 61s, putting us past the preemption timeout,
    // and check that no tasks get preempted.
    advanceTime(61000);
    scheduler.preemptTasksIfNecessary();
    scheduler.update();
    assertEquals(4, scheduler.runningTasks(job1, TaskType.MAP));
    assertEquals(4, scheduler.runningTasks(job1, TaskType.REDUCE));
    assertNull(scheduler.assignTasks(tracker("tt1")));
    assertNull(scheduler.assignTasks(tracker("tt2")));
  }

  private void advanceTime(long time) {
    clock.advance(time);
    for (JobInProgress job : scheduler.infos.keySet()) {
      job.refresh(clock.getTime());
    }
    scheduler.update();
  }

  protected TaskTracker tracker(String taskTrackerName) {
    return taskTrackerManager.getTaskTracker(taskTrackerName);
  }

  protected void checkAssignment(String taskTrackerName,
      String expectedTaskString) throws IOException {
    List<Task> tasks = scheduler.assignTasks(tracker(taskTrackerName));
    assertNotNull(expectedTaskString, tasks);
    assertEquals(expectedTaskString, 1, tasks.size());
    assertEquals(expectedTaskString, tasks.get(0).toString());
  }

  public void testPoolMaxMapsReduces() throws Exception {
    PrintWriter out = new PrintWriter(new FileWriter(ALLOC_FILE));
    out.println("<?xml version=\"1.0\"?>");
    out.println("<allocations>");
    // Pool with upper bound
    out.println("<pool name=\"pool_limited\">");
    out.println("<weight>1.0</weight>");
    out.println("<maxMaps>2</maxMaps>");
    out.println("<maxReduces>1</maxReduces>");
    out.println("</pool>");
    out.println("</allocations>");
    out.close();
    scheduler.getPoolManager().reloadAllocs();
    // Create two jobs with ten maps
    String poolName = "pool_limited";
    JobInProgress job1 = submitJob(JobStatus.RUNNING, 10, 5, poolName);
    advanceTime(10);

    PoolManager poolMgr = scheduler.getPoolManager();
    assertEquals(2, poolMgr.getMaxSlots(poolName, TaskType.MAP));
    assertEquals(1, poolMgr.getMaxSlots(poolName, TaskType.REDUCE));

    // Check scheduler variables
    JobInfo info1 = scheduler.infos.get(job1);
    assertEquals(0,    info1.runningMaps);
    assertEquals(0,    info1.runningReduces);
    assertEquals(10,   info1.neededMaps);
    assertEquals(5,    info1.neededReduces);
    assertEquals(20,   info1.mapDeficit, ALLOW_DEFICIT_ERROR);
    assertEquals(10,   info1.reduceDeficit, ALLOW_DEFICIT_ERROR);
    assertEquals(2.0,  info1.mapFairShare, ALLOW_ERROR);
    assertEquals(1.0,  info1.reduceFairShare, ALLOW_ERROR);
    assertEquals(0,    info1.minMaps);
    assertEquals(0,    info1.minReduces);
    assertEquals(2,    info1.maxMaps);
    assertEquals(1,    info1.maxReduces);

    JobInProgress job2 = submitJob(JobStatus.RUNNING, 10, 5);

    // Check scheduler variables
    scheduler.update();
    JobInfo info2 = scheduler.infos.get(job2);
    assertEquals(0,    info2.runningMaps);
    assertEquals(0,    info2.runningReduces);
    assertEquals(10,   info2.neededMaps);
    assertEquals(5,    info2.neededReduces);
    assertEquals(0,    info2.mapDeficit, ALLOW_DEFICIT_ERROR);
    assertEquals(0,    info2.reduceDeficit, ALLOW_DEFICIT_ERROR);
    assertEquals(2.0,  info2.mapFairShare, ALLOW_ERROR);
    assertEquals(3.0,  info2.reduceFairShare, ALLOW_ERROR);
    assertEquals(0,    info2.minMaps);
    assertEquals(0,    info2.minReduces);
    assertEquals(10,   info2.maxMaps);
    assertEquals(5,    info2.maxReduces);

    checkAssignment("tt1", "attempt_test_0001_m_000000_0 on tt1");
    checkAssignment("tt1", "attempt_test_0001_m_000001_0 on tt1");
    checkAssignment("tt1", "attempt_test_0001_r_000000_0 on tt1");
    checkAssignment("tt1", "attempt_test_0002_r_000000_0 on tt1");
    checkAssignment("tt2", "attempt_test_0002_m_000000_0 on tt2");
    checkAssignment("tt2", "attempt_test_0002_m_000001_0 on tt2");
    checkAssignment("tt2", "attempt_test_0002_r_000001_0 on tt2");
    checkAssignment("tt2", "attempt_test_0002_r_000002_0 on tt2");
    // Verify
    assertEquals(2, scheduler.poolMgr.
                    getRunningTasks("pool_limited", TaskType.MAP));
    assertEquals(1, scheduler.poolMgr.
                    getRunningTasks("pool_limited", TaskType.REDUCE));
    assertEquals(2, scheduler.infos.get(job1).runningMaps);
    assertEquals(1, scheduler.infos.get(job1).runningReduces);
    assertEquals(2, scheduler.infos.get(job2).runningMaps);
    assertEquals(3, scheduler.infos.get(job2).runningReduces);
    assertEquals(2, job1.runningMapTasks);
    assertEquals(1, job1.runningReduceTasks);
    assertEquals(2, job2.runningMapTasks);
    assertEquals(3, job2.runningReduceTasks);
  }

  /**
   * Schedule two jobs. Set the locality level of all the tasks to RACK.
   * Verify the delay scheduling behavior.
   */
  public void testDelayScheduling() throws Exception {
    JobInProgress job1 = submitJob(JobStatus.RUNNING, 2, 1);
    JobInfo info1 = scheduler.infos.get(job1);

    // Check scheduler variables
    assertEquals(0, info1.timeWaitedForLocalMap);
    assertEquals(LocalityLevel.NODE, info1.lastMapLocalityLevel);
    assertFalse(info1.skippedAtLastHeartbeat);

    // Advance time before submitting another job j2, to make j1 run before j2
    // deterministically.
    advanceTime(100);
    JobInProgress job2 = submitJob(JobStatus.RUNNING, 2, 2);
    JobInfo info2 = scheduler.infos.get(job2);

    // Check scheduler variables
    assertEquals(0, info2.timeWaitedForLocalMap);
    assertEquals(LocalityLevel.NODE, info2.lastMapLocalityLevel);
    assertFalse(info2.skippedAtLastHeartbeat);

    // Set all the tasks to have rake local on all tasktrackers
    localManager.setFakeLocalityLevel(LocalityLevel.RACK);
    ((FakeJobInProgress) job1).setFakeCacheLevel(LocalityLevel.RACK);
    ((FakeJobInProgress) job2).setFakeCacheLevel(LocalityLevel.RACK);

    // Check that only reducers are scheduled because we only get RACK local
    checkAssignment("tt1", "attempt_test_0001_r_000000_0 on tt1");
    checkAssignment("tt1", "attempt_test_0002_r_000000_0 on tt1");
    checkAssignment("tt2", "attempt_test_0002_r_000001_0 on tt2");

    // Check scheduler variables
    assertEquals(0, info1.timeWaitedForLocalMap);
    assertEquals(LocalityLevel.NODE, info1.lastMapLocalityLevel);
    assertTrue(info1.skippedAtLastHeartbeat);
    assertEquals(0, info2.timeWaitedForLocalMap);
    assertEquals(LocalityLevel.NODE, info2.lastMapLocalityLevel);
    assertTrue(info2.skippedAtLastHeartbeat);

    advanceTime(LOCALITY_DELAY_NODE_LOCAL);
    // Now the allowed locality level should go up to RACK
    checkAssignment("tt1", "attempt_test_0001_m_000000_0 on tt1");

    // Check scheduler variables
    assertEquals(0, info1.timeWaitedForLocalMap);
    assertEquals(LocalityLevel.RACK, info1.lastMapLocalityLevel);
    assertFalse(info1.skippedAtLastHeartbeat);
    assertEquals(LOCALITY_DELAY_NODE_LOCAL, info2.timeWaitedForLocalMap);
    assertEquals(LocalityLevel.NODE, info2.lastMapLocalityLevel);
    assertFalse(info2.skippedAtLastHeartbeat);

    // Now the allowed locality level should go up to RACK
    checkAssignment("tt1", "attempt_test_0001_m_000001_0 on tt1");
    checkAssignment("tt2", "attempt_test_0002_m_000000_0 on tt2");

    // Check scheduler variables
    assertEquals(0, info1.timeWaitedForLocalMap);
    assertEquals(LocalityLevel.RACK, info1.lastMapLocalityLevel);
    assertFalse(info1.skippedAtLastHeartbeat);
    assertEquals(0, info2.timeWaitedForLocalMap);
    assertEquals(LocalityLevel.RACK, info2.lastMapLocalityLevel);
    assertFalse(info2.skippedAtLastHeartbeat);
  }

  /**
   * Verifies that the tasks are assigned based on
   * current running tasks instead of deficit.
   */
  public void testFairComparator() throws IOException {
    JobInProgress job1 = submitJob(JobStatus.RUNNING, 2, 1);
    JobInfo info1 = scheduler.infos.get(job1);
    scheduler.setJobComparator(JobComparator.FAIR);

    // Check scheduler variables
    assertEquals(0,    info1.runningMaps);
    assertEquals(0,    info1.runningReduces);
    assertEquals(2,    info1.neededMaps);
    assertEquals(1,    info1.neededReduces);
    assertEquals(0,    info1.mapDeficit, ALLOW_DEFICIT_ERROR);
    assertEquals(0,    info1.reduceDeficit, ALLOW_DEFICIT_ERROR);
    assertEquals(2.0,  info1.mapFairShare, ALLOW_ERROR);
    assertEquals(1.0,  info1.reduceFairShare, ALLOW_ERROR);

    // Advance time before submitting another job j2, to make j1 run before j2
    // deterministically.
    advanceTime(100);
    JobInProgress job2 = submitJob(JobStatus.RUNNING, 1, 2);
    JobInfo info2 = scheduler.infos.get(job2);

    // Check scheduler variables; the fair shares should now have been allocated
    // equally between j1 and j2, but j1 should have (2 slots)*(100 ms) map
    // deficit and (1 slots) * (100 ms) reduce deficit.
    assertEquals(0,    info1.runningMaps);
    assertEquals(0,    info1.runningReduces);
    assertEquals(2,    info1.neededMaps);
    assertEquals(1,    info1.neededReduces);
    assertEquals(200,  info1.mapDeficit, ALLOW_DEFICIT_ERROR);
    assertEquals(100,  info1.reduceDeficit, ALLOW_DEFICIT_ERROR);
    assertEquals(2.0,  info1.mapFairShare, ALLOW_ERROR);
    assertEquals(1.0,  info1.reduceFairShare, ALLOW_ERROR);
    assertEquals(0,    info2.runningMaps);
    assertEquals(0,    info2.runningReduces);
    assertEquals(1,    info2.neededMaps);
    assertEquals(2,    info2.neededReduces);
    assertEquals(0,    info2.mapDeficit, ALLOW_DEFICIT_ERROR);
    assertEquals(0,    info2.reduceDeficit, ALLOW_DEFICIT_ERROR);
    assertEquals(1.0,  info2.mapFairShare, ALLOW_ERROR);
    assertEquals(2.0,  info2.reduceFairShare, ALLOW_ERROR);

    // Assign tasks and check slots are assigned based on fairshare
    checkAssignment("tt1", "attempt_test_0001_m_000000_0 on tt1");
    checkAssignment("tt1", "attempt_test_0002_m_000000_0 on tt1");
    checkAssignment("tt1", "attempt_test_0001_r_000000_0 on tt1");
    checkAssignment("tt1", "attempt_test_0002_r_000000_0 on tt1");
    checkAssignment("tt2", "attempt_test_0001_m_000001_0 on tt2");
    checkAssignment("tt2", "attempt_test_0002_r_000001_0 on tt2");
    assertNull(scheduler.assignTasks(tracker("tt2")));

    // Check that the scheduler has started counting the tasks as running
    // as soon as it launched them.
    assertEquals(2,  info1.runningMaps);
    assertEquals(1,  info1.runningReduces);
    assertEquals(0,  info1.neededMaps);
    assertEquals(0,  info1.neededReduces);
    assertEquals(1,  info2.runningMaps);
    assertEquals(2,  info2.runningReduces);
    assertEquals(0, info2.neededMaps);
    assertEquals(0, info2.neededReduces);
  }

  /**
   * Test Fair Comparator when there are minimum slots setup. Tasks
   * should be assigned alternatively based on the running slots and also
   * assigned to the pools with minimum slots first.
   */
  public void testFairComparatorWithMinSlots() throws Exception {
    // Set the job comparator to RunningTasksComparator
    scheduler.setJobComparator(JobComparator.FAIR);

    // Set up pools file
    PrintWriter out = new PrintWriter(new FileWriter(ALLOC_FILE));
    out.println("<?xml version=\"1.0\"?>");
    out.println("<allocations>");
    // Give pool A a minimum of 1 map, 2 reduces
    out.println("<pool name=\"pool_a\">");
    out.println("<minMaps>1</minMaps>");
    out.println("<minReduces>2</minReduces>");
    out.println("</pool>");
    // Give pool B a minimum of 2 maps, 1 reduce
    out.println("<pool name=\"pool_b\">");
    out.println("<minMaps>2</minMaps>");
    out.println("<minReduces>1</minReduces>");
    out.println("</pool>");
    out.println("</allocations>");
    out.close();
    scheduler.getPoolManager().reloadAllocs();

    JobInProgress job1 = submitJob(JobStatus.RUNNING, 10, 10);
    JobInfo info1 = scheduler.infos.get(job1);

    // Check scheduler variables
    assertEquals(0,    info1.runningMaps);
    assertEquals(0,    info1.runningReduces);
    assertEquals(10,   info1.neededMaps);
    assertEquals(10,   info1.neededReduces);
    assertEquals(0,    info1.mapDeficit, ALLOW_DEFICIT_ERROR);
    assertEquals(0,    info1.reduceDeficit, ALLOW_DEFICIT_ERROR);
    assertEquals(4.0,  info1.mapFairShare, ALLOW_ERROR);
    assertEquals(4.0,  info1.reduceFairShare, ALLOW_ERROR);

    // Advance time 200ms and submit jobs 2 and 3
    advanceTime(200);
    assertEquals(800,  info1.mapDeficit, ALLOW_DEFICIT_ERROR);
    assertEquals(800,  info1.reduceDeficit, ALLOW_DEFICIT_ERROR);
    JobInProgress job2 = submitJob(JobStatus.RUNNING, 10, 10, "pool_a");
    JobInfo info2 = scheduler.infos.get(job2);
    JobInProgress job3 = submitJob(JobStatus.RUNNING, 10, 10, "pool_b");
    JobInfo info3 = scheduler.infos.get(job3);

    // Check that minimum and fair shares have been allocated
    assertEquals(0,    info1.minMaps);
    assertEquals(0,    info1.minReduces);
    assertEquals(1.0,  info1.mapFairShare, ALLOW_ERROR);
    assertEquals(1.0,  info1.reduceFairShare, ALLOW_ERROR);
    assertEquals(1,    info2.minMaps);
    assertEquals(2,    info2.minReduces);
    assertEquals(1.0,  info2.mapFairShare, ALLOW_ERROR);
    assertEquals(2.0,  info2.reduceFairShare, ALLOW_ERROR);
    assertEquals(2,    info3.minMaps);
    assertEquals(1,    info3.minReduces);
    assertEquals(2.0,  info3.mapFairShare, ALLOW_ERROR);
    assertEquals(1.0,  info3.reduceFairShare, ALLOW_ERROR);

    // Advance time 100ms and check deficits
    advanceTime(100);
    assertEquals(900,  info1.mapDeficit, ALLOW_DEFICIT_ERROR);
    assertEquals(900,  info1.reduceDeficit, ALLOW_DEFICIT_ERROR);
    assertEquals(100,  info2.mapDeficit, ALLOW_DEFICIT_ERROR);
    assertEquals(200,  info2.reduceDeficit, ALLOW_DEFICIT_ERROR);
    assertEquals(200,  info3.mapDeficit, ALLOW_DEFICIT_ERROR);
    assertEquals(100,  info3.reduceDeficit, ALLOW_DEFICIT_ERROR);

    // Assign tasks and check slots are assigned alternatively based on the
    // minimum slots and running tasks
    checkAssignment("tt1", "attempt_test_0002_m_000000_0 on tt1");
    checkAssignment("tt1", "attempt_test_0003_m_000000_0 on tt1");
    checkAssignment("tt1", "attempt_test_0002_r_000000_0 on tt1");
    checkAssignment("tt1", "attempt_test_0003_r_000000_0 on tt1");
    checkAssignment("tt2", "attempt_test_0003_m_000001_0 on tt2");
    checkAssignment("tt2", "attempt_test_0001_m_000000_0 on tt2");
    checkAssignment("tt2", "attempt_test_0002_r_000001_0 on tt2");
    checkAssignment("tt2", "attempt_test_0001_r_000000_0 on tt2");
  }

  /**
   * Test that we can control the maximum slots using FairScheduler.
   * @throws Exception
   */
  public void testSetAndGetFSMaxSlots() throws Exception {
    assertEquals(scheduler.getFSMaxSlots("tt1", TaskType.MAP), Integer.MAX_VALUE);
    assertEquals(scheduler.getFSMaxSlots("tt1", TaskType.REDUCE), Integer.MAX_VALUE);
    assertEquals(scheduler.getFSMaxSlots("tt2", TaskType.MAP), Integer.MAX_VALUE);
    assertEquals(scheduler.getFSMaxSlots("tt2", TaskType.REDUCE), Integer.MAX_VALUE);

    TaskTrackerStatus tts1 = new TaskTrackerStatus(
        "tt1", "tt1.net", 80, new ArrayList<TaskStatus>(), 0, 4, 5);
    TaskTrackerStatus tts2 = new TaskTrackerStatus(
        "tt1", "tt1.net", 80, new ArrayList<TaskStatus>(), 0, 1, 2);
    assertEquals(scheduler.getMaxSlots(tts1, TaskType.MAP), 4);
    assertEquals(scheduler.getMaxSlots(tts1, TaskType.REDUCE), 5);
    assertEquals(scheduler.getMaxSlots(tts2, TaskType.MAP), 1);
    assertEquals(scheduler.getMaxSlots(tts2, TaskType.REDUCE), 2);

    scheduler.setFSMaxSlots("tt1", TaskType.MAP, 1);
    scheduler.setFSMaxSlots("tt1", TaskType.REDUCE, 2);
    scheduler.setFSMaxSlots("tt2", TaskType.MAP, 3);
    scheduler.setFSMaxSlots("tt2", TaskType.REDUCE, 4);
    assertEquals(scheduler.getFSMaxSlots("tt1", TaskType.MAP), 1);
    assertEquals(scheduler.getFSMaxSlots("tt1", TaskType.REDUCE), 2);
    assertEquals(scheduler.getFSMaxSlots("tt2", TaskType.MAP), 3);
    assertEquals(scheduler.getFSMaxSlots("tt2", TaskType.REDUCE), 4);
    assertEquals(scheduler.getMaxSlots(tts1, TaskType.MAP), 1);
    assertEquals(scheduler.getMaxSlots(tts1, TaskType.REDUCE), 2);
    assertEquals(scheduler.getMaxSlots(tts2, TaskType.MAP), 1);
    assertEquals(scheduler.getMaxSlots(tts2, TaskType.REDUCE), 2);

    scheduler.setFSMaxSlots("tt1", TaskType.MAP, 1);
    scheduler.setFSMaxSlots("tt1", TaskType.REDUCE, 1);
    scheduler.setFSMaxSlots("tt2", TaskType.MAP, 3);
    scheduler.setFSMaxSlots("tt2", TaskType.REDUCE, 3);
    submitJob(JobStatus.RUNNING, 10, 10);
    advanceTime(100);

    checkAssignment("tt1", "attempt_test_0001_m_000000_0 on tt1");
    checkAssignment("tt1", "attempt_test_0001_r_000000_0 on tt1");
    assertNull(scheduler.assignTasks(tracker("tt1")));

    checkAssignment("tt2", "attempt_test_0001_m_000001_0 on tt2");
    checkAssignment("tt2", "attempt_test_0001_m_000002_0 on tt2");
    checkAssignment("tt2", "attempt_test_0001_r_000001_0 on tt2");
    checkAssignment("tt2", "attempt_test_0001_r_000002_0 on tt2");
    assertNull(scheduler.assignTasks(tracker("tt2")));
  }

  /**
   * Test that {@link FairSchedulerShell} get and set maximum slots correctly.
   * @throws Exception
   */
  public void testShell() throws Exception {
    InetSocketAddress addr = scheduler.server.getListenerAddress();
    Configuration conf = new Configuration();
    conf.set("mapred.fairscheduler.server.address", "localhost:" + addr.getPort());
    FairSchedulerShell shell = new FairSchedulerShell();
    shell.setConf(conf);
    shell.setFSMaxSlots("tt1", TaskType.MAP, 1);
    shell.setFSMaxSlots("tt1", TaskType.REDUCE, 2);
    shell.setFSMaxSlots("tt2", TaskType.MAP, 3);
    shell.setFSMaxSlots("tt2", TaskType.REDUCE, 4);
    assertEquals(shell.getFSMaxSlots("tt1", TaskType.MAP), 1);
    assertEquals(shell.getFSMaxSlots("tt1", TaskType.REDUCE), 2);
    assertEquals(shell.getFSMaxSlots("tt2", TaskType.MAP), 3);
    assertEquals(shell.getFSMaxSlots("tt2", TaskType.REDUCE), 4);
    submitJob(JobStatus.RUNNING, 2, 1, "pool_a");
    advanceTime(10);
    Assert.assertArrayEquals(new int[]{Integer.MAX_VALUE, Integer.MAX_VALUE},
                             shell.getPoolMaxTasks("pool_a"));
    Assert.assertArrayEquals(new int[]{0, 0},
                             shell.getPoolRunningTasks("pool_a"));
  }

  /**
   * Test that the cpus to tasks configuration is loaded correctly
   */
  public void testCpuToMaxSlots() {
    Configuration conf = new Configuration();
    conf.set("mapred.fairscheduler.cpus.to.maptasks", "1:2, 4:6, 8:10");
    String config = conf.get("mapred.fairscheduler.cpus.to.maptasks");
    conf.set("mapred.fairscheduler.cpus.to.reducetasks", "1:3, 4:5, 8:11");
    CapBasedLoadManager loadMgr = new CapBasedLoadManager();
    loadMgr.setConf(conf);
    TaskTrackerStatus tts = new TaskTrackerStatus(
        "tt1", "", 0, new ArrayList<TaskStatus>(), 0, 20, 20);
    tts.getResourceStatus().setNumProcessors(1);
    assertEquals(2, loadMgr.getMaxSlots(tts, TaskType.MAP));
    assertEquals(3, loadMgr.getMaxSlots(tts, TaskType.REDUCE));
    tts.getResourceStatus().setNumProcessors(4);
    assertEquals(6, loadMgr.getMaxSlots(tts, TaskType.MAP));
    assertEquals(5, loadMgr.getMaxSlots(tts, TaskType.REDUCE));
    tts.getResourceStatus().setNumProcessors(8);
    assertEquals(10, loadMgr.getMaxSlots(tts, TaskType.MAP));
    assertEquals(11, loadMgr.getMaxSlots(tts, TaskType.REDUCE));
  }

  /**
   * Test that the mininum slots and fairshare is correctly distributed to each job
   */
  public void testMinMaxSlotsAndFairShareDistribution() throws Exception {
    // Create a bigger cluster than normal (4 tasktrackers instead of 2)
    // So now we have 16 tasks for both types
    setUpCluster(8);

    // Set up pools file
    PrintWriter out = new PrintWriter(new FileWriter(ALLOC_FILE));
    out.println("<?xml version=\"1.0\"?>");
    out.println("<allocations>");
    // Give pool A a minimum of 1 map, 2 reduces
    out.println("<pool name=\"pool_a\">");
    out.println("<minMaps>4</minMaps>");
    out.println("<minReduces>4</minReduces>");
    out.println("<maxMaps>6</maxMaps>");
    out.println("<maxReduces>6</maxReduces>");
    out.println("</pool>");
    // Give pool B a minimum of 2 maps, 1 reduce
    out.println("<pool name=\"pool_b\">");
    out.println("<minMaps>7</minMaps>");
    out.println("<minReduces>9</minReduces>");
    out.println("<maxMaps>8</maxMaps>");
    out.println("</pool>");
    out.println("</allocations>");
    out.close();

    scheduler.getPoolManager().reloadAllocs();
    JobInProgress job1 = submitJob(JobStatus.RUNNING, 10, 10, "pool_a");
    JobInProgress job2 = submitJob(JobStatus.RUNNING, 10, 10, "pool_a");
    JobInProgress job3 = submitJob(JobStatus.RUNNING, 10, 10, "pool_b");
    JobInProgress job4 = submitJob(JobStatus.RUNNING, 10, 10, "pool_b");
    JobInProgress job5 = submitJob(JobStatus.RUNNING, 10, 10, "pool_b");
    scheduler.update();
    JobInfo info1 = scheduler.infos.get(job1);
    JobInfo info2 = scheduler.infos.get(job2);
    JobInfo info3 = scheduler.infos.get(job3);
    JobInfo info4 = scheduler.infos.get(job4);
    JobInfo info5 = scheduler.infos.get(job5);

    assertEquals(2, info1.minMaps);
    assertEquals(2, info2.minMaps);
    assertEquals(3, info3.minMaps);
    assertEquals(2, info4.minMaps);
    assertEquals(2, info5.minMaps);
    assertEquals(2, info1.minReduces);
    assertEquals(2, info2.minReduces);
    assertEquals(3, info3.minReduces);
    assertEquals(3, info4.minReduces);
    assertEquals(3, info5.minReduces);

    assertEquals(3, info1.maxMaps);
    assertEquals(3, info2.maxMaps);
    assertEquals(3, info3.maxMaps);
    assertEquals(3, info4.maxMaps);
    assertEquals(2, info5.maxMaps);
    assertEquals(3, info1.maxReduces);
    assertEquals(3, info2.maxReduces);
    assertEquals(10, info3.maxReduces);
    assertEquals(10, info4.maxReduces);
    assertEquals(10, info5.maxReduces);

    assertEquals(3.0, info1.mapFairShare, ALLOW_ERROR);
    assertEquals(3.0, info2.mapFairShare, ALLOW_ERROR);
    assertEquals(3.0, info3.mapFairShare, ALLOW_ERROR);
    assertEquals(3.0, info4.mapFairShare, ALLOW_ERROR);
    assertEquals(2.0, info5.mapFairShare, ALLOW_ERROR);
    assertEquals(3.0, info1.reduceFairShare, ALLOW_ERROR);
    assertEquals(3.0, info2.reduceFairShare, ALLOW_ERROR);
    assertEquals(10.0 / 3, info3.reduceFairShare, ALLOW_ERROR);
    assertEquals(10.0 / 3, info4.reduceFairShare, ALLOW_ERROR);
    assertEquals(10.0 / 3, info5.reduceFairShare, ALLOW_ERROR);
  }

  /**
   * Verify the FIFO pool weight adjust
   */
  public void testPoolFifoWeight() throws Exception {
    // Set up pools file
    PrintWriter out = new PrintWriter(new FileWriter(ALLOC_FILE));
    out.println("<?xml version=\"1.0\"?>");
    out.println("<allocations>");
    out.println("<pool name=\"pool_a\">");
    out.println("<fifo>true</fifo>");
    out.println("</pool>");
    out.println("</allocations>");
    out.close();

    scheduler.getPoolManager().reloadAllocs();
    JobInProgress job1 = submitJob(JobStatus.RUNNING, 10, 10, "pool_a");
    advanceTime(1L);
    JobInProgress job2 = submitJob(JobStatus.RUNNING, 10, 10, "pool_a");
    advanceTime(2L);
    JobInProgress job3 = submitJob(JobStatus.RUNNING, 10, 10, "pool_a");
    advanceTime(3L);
    JobInProgress job4 = submitJob(JobStatus.RUNNING, 10, 10, "pool_a");
    scheduler.update();

    JobInfo info1 = scheduler.infos.get(job1);
    JobInfo info2 = scheduler.infos.get(job2);
    JobInfo info3 = scheduler.infos.get(job3);
    JobInfo info4 = scheduler.infos.get(job4);

    final double ALLOWED_ERROR = 0.00001;
    assertEquals(8.0 / 15, info1.mapWeight, ALLOWED_ERROR);
    assertEquals(4.0 / 15, info2.mapWeight, ALLOWED_ERROR);
    assertEquals(2.0 / 15, info3.mapWeight, ALLOWED_ERROR);
    assertEquals(1.0 / 15, info4.mapWeight, ALLOWED_ERROR);
  }

  /**
   * Verify the min slots of FIFO pools
   */
  public void testPoolFifoMin() throws Exception {
    // Set up pools file
    PrintWriter out = new PrintWriter(new FileWriter(ALLOC_FILE));
    out.println("<?xml version=\"1.0\"?>");
    out.println("<allocations>");
    out.println("<pool name=\"pool_a\">");
    out.println("<fifo>true</fifo>");
    out.println("<minMaps>12</minMaps>");
    out.println("<minReduces>12</minReduces>");
    out.println("</pool>");
    out.println("</allocations>");
    out.close();

    scheduler.getPoolManager().reloadAllocs();
    JobInProgress job1 = submitJob(JobStatus.RUNNING, 5, 5, "pool_a");
    advanceTime(1L);
    JobInProgress job2 = submitJob(JobStatus.RUNNING, 5, 5, "pool_a");
    advanceTime(2L);
    JobInProgress job3 = submitJob(JobStatus.RUNNING, 5, 5, "pool_a");
    advanceTime(3L);
    JobInProgress job4 = submitJob(JobStatus.RUNNING, 5, 5, "pool_a");
    scheduler.update();

    JobInfo info1 = scheduler.infos.get(job1);
    JobInfo info2 = scheduler.infos.get(job2);
    JobInfo info3 = scheduler.infos.get(job3);
    JobInfo info4 = scheduler.infos.get(job4);

    assertEquals(5, info1.minMaps);
    assertEquals(5, info2.minMaps);
    assertEquals(2, info3.minMaps);
    assertEquals(0, info4.minMaps);

    assertEquals(5, info1.minReduces);
    assertEquals(5, info2.minReduces);
    assertEquals(2, info3.minReduces);
    assertEquals(0, info4.minReduces);
  }

  /**
   * Verify the max slots of FIFO pools
   */
  public void testPoolFifoMax() throws Exception {
    // Set up pools file
    PrintWriter out = new PrintWriter(new FileWriter(ALLOC_FILE));
    out.println("<?xml version=\"1.0\"?>");
    out.println("<allocations>");
    out.println("<pool name=\"pool_a\">");
    out.println("<fifo>true</fifo>");
    out.println("<maxMaps>12</maxMaps>");
    out.println("<maxReduces>12</maxReduces>");
    out.println("</pool>");
    out.println("</allocations>");
    out.close();

    scheduler.getPoolManager().reloadAllocs();
    JobInProgress job1 = submitJob(JobStatus.RUNNING, 5, 5, "pool_a");
    advanceTime(1L);
    JobInProgress job2 = submitJob(JobStatus.RUNNING, 5, 5, "pool_a");
    advanceTime(2L);
    JobInProgress job3 = submitJob(JobStatus.RUNNING, 5, 5, "pool_a");
    advanceTime(3L);
    JobInProgress job4 = submitJob(JobStatus.RUNNING, 5, 5, "pool_a");
    scheduler.update();

    JobInfo info1 = scheduler.infos.get(job1);
    JobInfo info2 = scheduler.infos.get(job2);
    JobInfo info3 = scheduler.infos.get(job3);
    JobInfo info4 = scheduler.infos.get(job4);

    assertEquals(5, info1.maxMaps);
    assertEquals(5, info2.maxMaps);
    assertEquals(2, info3.maxMaps);
    assertEquals(0, info4.maxMaps);

    assertEquals(5, info1.maxReduces);
    assertEquals(5, info2.maxReduces);
    assertEquals(2, info3.maxReduces);
    assertEquals(0, info4.maxReduces);
  }

  public void testPoolNames() throws IOException {
    String[] goodPoolNames = { "--good_pool_9876--" ,
                               "abcefghijklmnopqrstuvwxyz" +
                                 "0123456789_-" };
    String[] badPoolNames = { "fds_a!fsda--__",
                              "0123\n4567",
                              "ABCDEFGHIJKLMNOPQRSTUVWXYZ",
                              "pool@root",
                              "ứnicȍḏe",
                              "\0",
                              "" };

    PoolManager mgr = scheduler.getPoolManager();
    JobConf jobConf = new JobConf(conf);
    JobInProgress job = new FakeJobInProgress(jobConf, taskTrackerManager,
        jobTracker);

    // Check that each good pool name is permitted
    for (String goodPoolName : goodPoolNames) {
      job.conf.set(PoolManager.EXPLICIT_POOL_PROPERTY, goodPoolName);
      mgr.addJob(job);
      assertEquals("PoolManager rejected good pool name",
          goodPoolName, mgr.getPoolName(job));
    }

    // Check that each bad pool name is rejected
    for (String badPoolName : badPoolNames) {
      job.conf.set(PoolManager.EXPLICIT_POOL_PROPERTY, badPoolName);
      mgr.addJob(job);
      assertFalse("PoolManager accepted bad pool name",
          badPoolName.equals(mgr.getPoolName(job)));
    }
  }
}
