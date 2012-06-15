/*
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

import java.util.List;

import org.apache.hadoop.mapred.Counters.Group;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.metrics.MetricsContext;
import org.apache.hadoop.metrics.MetricsRecord;
import org.apache.hadoop.metrics.MetricsUtil;
import org.apache.hadoop.metrics.Updater;
import org.apache.hadoop.metrics.jvm.JvmMetrics;

class JobTrackerMetricsInst extends JobTrackerInstrumentation implements Updater {
  private final MetricsRecord metricsRecord;

  private int numJobsSubmitted = 0;
  private int numJobsCompleted = 0;
  private int numWaitingMaps = 0;
  private int numWaitingReduces = 0;

  /**
   * Helper class that makes it easier to keep track of additional stats for
   * speculated tasks. Needed to evaluate the performance of speculating by
   * processing (bytes/s) vs progress (%/s) rate.
   */
  private static class SpecStats {
    public static enum TaskType {MAP, REDUCE};
    // The type of speculation. Whether it's by progress rate (e.g. %/sec) or
    // processing rate (e.g. bytes/sec)
    public static enum SpecType {PROGRESS, PROCESSING};
    // Wasted time is in ms
    public static enum StatType {WASTED_TASKS, WASTED_TIME, LAUNCHED_TASKS,
      SUCCESFUL_TASKS};

    private final int taskTypeSize = TaskType.values().length;
    private final int specTypeSize = SpecType.values().length;
    private final int statTypeSize = StatType.values().length;

    private final long[][][] values =
      new long[taskTypeSize][specTypeSize][statTypeSize];

    public SpecStats() {}

    public void incStat(TaskType taskType, SpecType specType, StatType statType,
                        long value) {
      values[taskType.ordinal()][specType.ordinal()][statType.ordinal()] +=
        value;
    }

    public long getStat(TaskType taskType, SpecType specType,
                        StatType statType) {
      return values[taskType.ordinal()][specType.ordinal()][statType.ordinal()];
    }
  }

  private final SpecStats specStats = new SpecStats();

  private final JobStats aggregateJobStats = new JobStats();

  private final Counters countersToMetrics = new Counters();

  //Cluster status fields.
  private volatile int numMapSlots = 0;
  private volatile int numReduceSlots = 0;
  private int numBlackListedMapSlots = 0;
  private int numBlackListedReduceSlots = 0;

  private int numReservedMapSlots = 0;
  private int numReservedReduceSlots = 0;
  private int numOccupiedMapSlots = 0;
  private int numOccupiedReduceSlots = 0;

  private int numJobsFailed = 0;
  private int numJobsKilled = 0;

  private int numJobsPreparing = 0;
  private int numJobsRunning = 0;

  private int numRunningMaps = 0;
  private int numRunningReduces = 0;

  private int numTrackers = 0;
  private int numTrackersBlackListed = 0;
  private int numTrackersDecommissioned = 0;
  private int numTrackersExcluded = 0;
  private int numTrackersDead = 0;

  private int numTasksInMemory = 0;

  //Extended JobTracker Metrics
  private long totalSubmitTime = 0;
  private long numJobsLaunched = 0;

  public JobTrackerMetricsInst(JobTracker tracker, JobConf conf) {
    super(tracker, conf);
    String sessionId = conf.getSessionId();
    // Initiate JVM Metrics
    JvmMetrics.init("JobTracker", sessionId);
    // Create a record for map-reduce metrics
    MetricsContext context = MetricsUtil.getContext("mapred");
    metricsRecord = MetricsUtil.createRecord(context, "jobtracker");
    metricsRecord.setTag("sessionId", sessionId);
    context.registerUpdater(this);
  }

  /**
   * Since this object is a registered updater, this method will be called
   * periodically, e.g. every 5 seconds.
   */
  public void doUpdates(MetricsContext unused) {
    // In case of running in LocalMode tracker == null
    if (tracker != null) {
      synchronized (tracker) {
        synchronized (this) {
          numRunningMaps = 0;
          numRunningReduces = 0;

          numWaitingMaps = 0;
          numWaitingReduces = 0;
          numTasksInMemory = 0;

          List<JobInProgress> jobs = tracker.getRunningJobs();
          for (JobInProgress jip : jobs) {
            for (TaskInProgress tip : jip.maps) {
              if (tip.isRunning()) {
                numRunningMaps++;
              } else if (tip.isRunnable()) {
                numWaitingMaps++;
              }
            }
            for (TaskInProgress tip : jip.reduces) {
              if (tip.isRunning()) {
                numRunningReduces++;
              } else if (tip.isRunnable()) {
                numWaitingReduces++;
              }

            }
            numTasksInMemory += jip.getTasks(TaskType.MAP).length;
            numTasksInMemory += jip.getTasks(TaskType.REDUCE).length;
          }

          // Get tracker metrics
          numTrackersDead = tracker.getDeadNodes().size();
          ClusterStatus cs = tracker.getClusterStatus(false);
          numTrackersExcluded = cs.getNumExcludedNodes();
        }
      }
    }
    synchronized (this) {
      metricsRecord.setMetric("map_slots", numMapSlots);
      metricsRecord.setMetric("reduce_slots", numReduceSlots);
      metricsRecord.incrMetric("blacklisted_maps", numBlackListedMapSlots);
      metricsRecord.incrMetric("blacklisted_reduces",
          numBlackListedReduceSlots);
      metricsRecord.incrMetric("jobs_submitted", numJobsSubmitted);
      metricsRecord.incrMetric("jobs_completed", numJobsCompleted);
      metricsRecord.setMetric("waiting_maps", numWaitingMaps);
      metricsRecord.setMetric("waiting_reduces", numWaitingReduces);

      metricsRecord.incrMetric("reserved_map_slots", numReservedMapSlots);
      metricsRecord.incrMetric("reserved_reduce_slots", numReservedReduceSlots);
      metricsRecord.incrMetric("occupied_map_slots", numOccupiedMapSlots);
      metricsRecord.incrMetric("occupied_reduce_slots", numOccupiedReduceSlots);

      metricsRecord.incrMetric("jobs_failed", numJobsFailed);
      metricsRecord.incrMetric("jobs_killed", numJobsKilled);

      metricsRecord.incrMetric("jobs_preparing", numJobsPreparing);
      metricsRecord.incrMetric("jobs_running", numJobsRunning);

      metricsRecord.setMetric("running_maps", numRunningMaps);
      metricsRecord.setMetric("running_reduces", numRunningReduces);

      metricsRecord.setMetric("num_tasks_in_memory", numTasksInMemory);

      metricsRecord.setMetric("trackers", numTrackers);
      metricsRecord.setMetric("trackers_blacklisted", numTrackersBlackListed);
      metricsRecord.setMetric("trackers_decommissioned",
          numTrackersDecommissioned);
      metricsRecord.setMetric("trackers_excluded", numTrackersExcluded);
      metricsRecord.setMetric("trackers_dead", numTrackersDead);

      metricsRecord.incrMetric("num_launched_jobs", numJobsLaunched);
      metricsRecord.incrMetric("total_submit_time", totalSubmitTime);

      aggregateJobStats.incrementMetricsAndReset(metricsRecord);

      // Update additional speculation stats for measuring the performance
      // of different kinds of speculation
      for (SpecStats.TaskType taskType : SpecStats.TaskType.values()) {
        for (SpecStats.SpecType specType : SpecStats.SpecType.values()) {
          for(SpecStats.StatType statType : SpecStats.StatType.values()) {
            String key = "speculation_by_" + specType.toString().toLowerCase()  
                + "_" + "rate_" + taskType.toString().toLowerCase() + "_" + 
                statType.toString().toLowerCase();
            long value = specStats.getStat(taskType, specType, statType);
            metricsRecord.setMetric(key, value);
          }
        }
      }

      
      for (Group group: countersToMetrics) {
        String groupName = group.getName();
        for (Counter counter : group) {
          String name = groupName + "_" + counter.getName();
          name = name.replaceAll("[^a-zA-Z_]", "_").toLowerCase();
          metricsRecord.incrMetric(name, counter.getValue());
        }
      }
      clearCounters();

      numJobsSubmitted = 0;
      numJobsCompleted = 0;
      numWaitingMaps = 0;
      numWaitingReduces = 0;
      numBlackListedMapSlots = 0;
      numBlackListedReduceSlots = 0;

      numReservedMapSlots = 0;
      numReservedReduceSlots = 0;
      numOccupiedMapSlots = 0;
      numOccupiedReduceSlots = 0;

      numJobsFailed = 0;
      numJobsKilled = 0;

      numJobsPreparing = 0;
      numJobsRunning = 0;

      numRunningMaps = 0;
      numRunningReduces = 0;

      numTrackers = 0;
      numTrackersBlackListed = 0;

      totalSubmitTime = 0;
      numJobsLaunched = 0;
    }
    metricsRecord.update();
  }

  @Override
  public synchronized void launchMap(TaskAttemptID taskAttemptID) {
    aggregateJobStats.incNumMapTasksLaunched();
    decWaitingMaps(taskAttemptID.getJobID(), 1);
  }
  @Override
  public void launchDataLocalMap(TaskAttemptID taskAttemptID) {
    aggregateJobStats.incNumDataLocalMaps();
  }
  @Override
  public void launchRackLocalMap(TaskAttemptID taskAttemptID) {
    aggregateJobStats.incNumRackLocalMaps();
  }

  @Override
  public void completeMap(TaskAttemptID taskAttemptID) {
    aggregateJobStats.incNumMapTasksCompleted();
  }

  @Override
  public synchronized void speculateMap(TaskAttemptID taskAttemptID, 
      boolean isUsingProcessingRate) {
    aggregateJobStats.incNumSpeculativeMaps();
    SpecStats.SpecType specType = isUsingProcessingRate ?
        SpecStats.SpecType.PROCESSING : SpecStats.SpecType.PROGRESS;
    specStats.incStat(SpecStats.TaskType.MAP, specType, 
        SpecStats.StatType.LAUNCHED_TASKS, 1);
  }

  @Override
  public synchronized void speculativeSucceededMap(
          TaskAttemptID taskAttemptID, boolean isUsingProcessingRate) {
    aggregateJobStats.incNumSpeculativeSucceededMaps();
    SpecStats.SpecType specType = isUsingProcessingRate ?
        SpecStats.SpecType.PROCESSING : SpecStats.SpecType.PROGRESS;
    specStats.incStat(SpecStats.TaskType.REDUCE, specType, 
        SpecStats.StatType.SUCCESFUL_TASKS, 1);
  }

  @Override
  public synchronized void speculativeSucceededReduce(
          TaskAttemptID taskAttemptID, boolean isUsingProcessingRate) {
    aggregateJobStats.incNumSpeculativeSucceededMaps();
    SpecStats.SpecType specType = isUsingProcessingRate ?
        SpecStats.SpecType.PROCESSING : SpecStats.SpecType.PROGRESS;
    specStats.incStat(SpecStats.TaskType.REDUCE, specType, 
        SpecStats.StatType.SUCCESFUL_TASKS, 1);
  }

  @Override
  public synchronized void failedMap(TaskAttemptID taskAttemptID,
      boolean wasFailed, boolean isSpeculative,
      boolean isUsingProcessingRate, long taskStartTime) {
    long timeSpent = JobTracker.getClock().getTime() - taskStartTime;
    if (wasFailed) {
      aggregateJobStats.incNumMapTasksFailed();
      aggregateJobStats.incFailedMapTime(timeSpent);
    } else {
      aggregateJobStats.incNumMapTasksKilled();
      aggregateJobStats.incKilledMapTime(timeSpent);
	    if (isSpeculative) {
        aggregateJobStats.incNumSpeculativeWasteMaps();
	      aggregateJobStats.incSpeculativeMapTimeWaste(timeSpent);
	      // More detailed stats
	      SpecStats.SpecType specType = isUsingProcessingRate ? 
	          SpecStats.SpecType.PROCESSING : SpecStats.SpecType.PROGRESS;
	      specStats.incStat(SpecStats.TaskType.MAP, specType, 
	          SpecStats.StatType.WASTED_TASKS, 1);
	      specStats.incStat(SpecStats.TaskType.MAP, specType, 
	          SpecStats.StatType.WASTED_TIME, timeSpent);
	    }
    }
    addWaitingMaps(taskAttemptID.getJobID(), 1);
  }

  @Override
  public synchronized void launchReduce(TaskAttemptID taskAttemptID) {
    aggregateJobStats.incNumReduceTasksLaunched();
    decWaitingReduces(taskAttemptID.getJobID(), 1);
  }

  @Override
  public void completeReduce(TaskAttemptID taskAttemptID) {
    aggregateJobStats.incNumReduceTasksCompleted();
  }

  @Override
  public synchronized void speculateReduce(TaskAttemptID taskAttemptID,
      boolean isUsingProcessingRate) {
    aggregateJobStats.incNumSpeculativeReduces();
    SpecStats.SpecType specType = isUsingProcessingRate ? 
        SpecStats.SpecType.PROCESSING : SpecStats.SpecType.PROGRESS;
    specStats.incStat(SpecStats.TaskType.REDUCE, specType, 
        SpecStats.StatType.LAUNCHED_TASKS, 1);
  }

  @Override
  public synchronized void failedReduce(TaskAttemptID taskAttemptID,
      boolean wasFailed, boolean isSpeculative, boolean isUsingProcessingRate, 
      long taskStartTime) {
    long timeSpent = JobTracker.getClock().getTime() - taskStartTime;
    if (wasFailed) {
      aggregateJobStats.incNumReduceTasksFailed();
      aggregateJobStats.incFailedReduceTime(timeSpent);
    } else {
      aggregateJobStats.incNumReduceTasksKilled();
      aggregateJobStats.incFailedReduceTime(timeSpent);
	    if (isSpeculative) {
	      aggregateJobStats.incNumSpeculativeWasteReduces();
	      aggregateJobStats.incSpeculativeReduceTimeWaste(timeSpent);
	       // More detailed stats
        SpecStats.SpecType specType = isUsingProcessingRate ? 
            SpecStats.SpecType.PROCESSING : SpecStats.SpecType.PROGRESS;
        specStats.incStat(SpecStats.TaskType.MAP, specType, 
            SpecStats.StatType.WASTED_TASKS, 1);
        specStats.incStat(SpecStats.TaskType.MAP, specType, 
            SpecStats.StatType.WASTED_TIME, timeSpent);
	    }
    }
    addWaitingReduces(taskAttemptID.getJobID(), 1);
  }

  @Override
  public void mapFailedByFetchFailures() {
    aggregateJobStats.incNumMapTasksFailedByFetchFailures();
  }

  @Override
  public void mapFetchFailure() {
    aggregateJobStats.incNumMapFetchFailures();
  }

  @Override
  public synchronized void submitJob(JobConf conf, JobID id) {
    ++numJobsSubmitted;
  }

  @Override
  public synchronized void completeJob(JobConf conf, JobID id) {
    collectJobCounters(id);
    ++numJobsCompleted;
  }

  @Override
  public synchronized void addWaitingMaps(JobID id, int task) {
  }

  @Override
  public synchronized void decWaitingMaps(JobID id, int task) {
  }

  @Override
  public synchronized void addWaitingReduces(JobID id, int task) {
  }

  @Override
  public synchronized void decWaitingReduces(JobID id, int task){
  }

  @Override
  public synchronized void setMapSlots(int slots) {
    numMapSlots = slots;
  }

  @Override
  public synchronized void setReduceSlots(int slots) {
    numReduceSlots = slots;
  }

  @Override
  public synchronized void addBlackListedMapSlots(int slots){
    numBlackListedMapSlots += slots;
  }

  @Override
  public synchronized void decBlackListedMapSlots(int slots){
    numBlackListedMapSlots -= slots;
  }

  @Override
  public synchronized void addBlackListedReduceSlots(int slots){
    numBlackListedReduceSlots += slots;
  }

  @Override
  public synchronized void decBlackListedReduceSlots(int slots){
    numBlackListedReduceSlots -= slots;
  }

  @Override
  public synchronized void addReservedMapSlots(int slots)
  {
    numReservedMapSlots += slots;
  }

  @Override
  public synchronized void decReservedMapSlots(int slots)
  {
    numReservedMapSlots -= slots;
  }

  @Override
  public synchronized void addReservedReduceSlots(int slots)
  {
    numReservedReduceSlots += slots;
  }

  @Override
  public synchronized void decReservedReduceSlots(int slots)
  {
    numReservedReduceSlots -= slots;
  }

  @Override
  public synchronized void addOccupiedMapSlots(int slots)
  {
    numOccupiedMapSlots += slots;
  }

  @Override
  public synchronized void decOccupiedMapSlots(int slots)
  {
    numOccupiedMapSlots -= slots;
  }

  @Override
  public synchronized void addOccupiedReduceSlots(int slots)
  {
    numOccupiedReduceSlots += slots;
  }

  @Override
  public synchronized void decOccupiedReduceSlots(int slots)
  {
    numOccupiedReduceSlots -= slots;
  }

  @Override
  public synchronized void failedJob(JobConf conf, JobID id)
  {
    numJobsFailed++;
  }

  @Override
  public synchronized void killedJob(JobConf conf, JobID id)
  {
    numJobsKilled++;
  }

  @Override
  public synchronized void addPrepJob(JobConf conf, JobID id)
  {
    numJobsPreparing++;
  }

  @Override
  public synchronized void decPrepJob(JobConf conf, JobID id)
  {
    numJobsPreparing--;
  }

  @Override
  public synchronized void addRunningJob(JobConf conf, JobID id)
  {
    numJobsRunning++;
  }

  @Override
  public synchronized void decRunningJob(JobConf conf, JobID id)
  {
    numJobsRunning--;
  }

  @Override
  public synchronized void addRunningMaps(int task)
  {
  }

  @Override
  public synchronized void decRunningMaps(int task)
  {
  }

  @Override
  public synchronized void addRunningReduces(int task)
  {
  }

  @Override
  public synchronized void decRunningReduces(int task)
  {
  }

  @Override
  public synchronized void killedMap(TaskAttemptID taskAttemptID)
  {
  }

  @Override
  public synchronized void killedReduce(TaskAttemptID taskAttemptID)
  {
  }

  @Override
  public synchronized void addTrackers(int trackers)
  {
    numTrackers += trackers;
  }

  @Override
  public synchronized void decTrackers(int trackers)
  {
    numTrackers -= trackers;
  }

  @Override
  public synchronized void addBlackListedTrackers(int trackers)
  {
    numTrackersBlackListed += trackers;
  }

  @Override
  public synchronized void decBlackListedTrackers(int trackers)
  {
    numTrackersBlackListed -= trackers;
  }

  @Override
  public synchronized void setDecommissionedTrackers(int trackers)
  {
    numTrackersDecommissioned = trackers;
  }

  @Override
  public synchronized void addLaunchedJobs(long submitTime)
  {
    ++numJobsLaunched;
    totalSubmitTime += submitTime;
  }

  @Override
  public void addMapInputBytes(long size) {
    aggregateJobStats.incTotalMapInputBytes(size);
  }

  @Override
  public void addLocalMapInputBytes(long size) {
    aggregateJobStats.incLocalMapInputBytes(size);
    addMapInputBytes(size);
  }

  @Override
  public void addRackMapInputBytes(long size) {
    aggregateJobStats.incRackMapInputBytes(size);
    addMapInputBytes(size);
  }

  @Override
  public void terminateJob(JobConf conf, JobID id) {
    collectJobCounters(id);
  }

  private synchronized void collectJobCounters(JobID id) {
    JobInProgress job = tracker.jobs.get(id);
    if (job == null) {
      return;
    }
    Counters jobCounter = job.getCounters();
    for (JobInProgress.Counter key : JobInProgress.Counter.values()) {
      countersToMetrics.findCounter(key).
      increment(jobCounter.findCounter(key).getValue());
    }
    for (Task.Counter key : Task.Counter.values()) {
      countersToMetrics.findCounter(key).
      increment(jobCounter.findCounter(key).getValue());
    }
    for (Counter counter : jobCounter.getGroup(Task.FILESYSTEM_COUNTER_GROUP)) {
      countersToMetrics.incrCounter(
          Task.FILESYSTEM_COUNTER_GROUP, counter.getName(), counter.getValue());
    }
  }
  /*
   *  Set everything in the counters to zero
   */
  private void clearCounters() {
    for (Group g : countersToMetrics) {
      for (Counter c : g) {
        c.setValue(0);
      }
    }
  }
}
