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

  private int numMapTasksLaunched = 0;
  private int numMapTasksCompleted = 0;
  private int numMapTasksFailed = 0;
  private int numReduceTasksLaunched = 0;
  private int numReduceTasksCompleted = 0;
  private int numReduceTasksFailed = 0;
  private int numJobsSubmitted = 0;
  private int numJobsCompleted = 0;
  private int numWaitingMaps = 0;
  private int numWaitingReduces = 0;

  private int numSpeculativeMaps = 0;
  private int numSpeculativeReduces = 0;
  private int numSpeculativeSucceededMaps = 0;
  private int numSpeculativeSucceededReduces = 0;
  private int numSpeculativeWasteMaps = 0;
  private int numSpeculativeWasteReduces = 0;
  private int numDataLocalMaps = 0;
  private int numRackLocalMaps = 0;

  private long killedMapTime = 0L;
  private long killedReduceTime = 0L;
  private long failedMapTime = 0L;
  private long failedReduceTime = 0L;
  private long speculativeMapTimeWaste = 0L;
  private long speculativeReduceTimeWaste = 0L;

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

  private int numMapTasksKilled = 0;
  private int numReduceTasksKilled = 0;

  private int numTrackers = 0;
  private int numTrackersBlackListed = 0;
  private int numTrackersDecommissioned = 0;
  private int numTrackersExcluded = 0;
  private int numTrackersDead = 0;
  
  private int numTasksInMemory = 0;

  //Extended JobTracker Metrics
  private long totalSubmitTime = 0;
  private long numJobsLaunched = 0;
  private long totalMapInputBytes = 0;
  private long localMapInputBytes = 0;
  private long rackMapInputBytes = 0;
  
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
      metricsRecord.incrMetric("maps_launched", numMapTasksLaunched);
      metricsRecord.incrMetric("maps_completed", numMapTasksCompleted);
      metricsRecord.incrMetric("maps_failed", numMapTasksFailed);
      metricsRecord.incrMetric("reduces_launched", numReduceTasksLaunched);
      metricsRecord.incrMetric("reduces_completed", numReduceTasksCompleted);
      metricsRecord.incrMetric("reduces_failed", numReduceTasksFailed);
      metricsRecord.incrMetric("jobs_submitted", numJobsSubmitted);
      metricsRecord.incrMetric("jobs_completed", numJobsCompleted);
      metricsRecord.setMetric("waiting_maps", numWaitingMaps);
      metricsRecord.setMetric("waiting_reduces", numWaitingReduces);
      metricsRecord.incrMetric("num_speculative_maps", numSpeculativeMaps);
      metricsRecord.incrMetric("num_speculative_reduces", numSpeculativeReduces);
      metricsRecord.incrMetric("num_speculative_succeeded_maps",
          numSpeculativeSucceededMaps);
      metricsRecord.incrMetric("num_speculative_succeeded_reduces",
          numSpeculativeSucceededReduces);
      metricsRecord.incrMetric("num_speculative_wasted_maps", numSpeculativeWasteMaps);
      metricsRecord.incrMetric("num_speculative_wasted_reduces", numSpeculativeWasteReduces);
      metricsRecord.incrMetric("speculative_map_time_waste", speculativeMapTimeWaste);
      metricsRecord.incrMetric("speculative_reduce_time_waste", speculativeReduceTimeWaste);
      metricsRecord.incrMetric("killed_tasks_map_time", killedMapTime);
      metricsRecord.incrMetric("killed_tasks_reduce_time", killedReduceTime);
      metricsRecord.incrMetric("failed_tasks_map_time", failedMapTime);
      metricsRecord.incrMetric("failed_tasks_reduce_time", failedReduceTime);
      metricsRecord.incrMetric("num_dataLocal_maps", numDataLocalMaps);
      metricsRecord.incrMetric("num_rackLocal_maps", numRackLocalMaps);

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

      metricsRecord.incrMetric("maps_killed", numMapTasksKilled);
      metricsRecord.incrMetric("reduces_killed", numReduceTasksKilled);

      metricsRecord.setMetric("trackers", numTrackers);
      metricsRecord.setMetric("trackers_blacklisted", numTrackersBlackListed);
      metricsRecord.setMetric("trackers_decommissioned",
          numTrackersDecommissioned);
      metricsRecord.setMetric("trackers_excluded", numTrackersExcluded);
      metricsRecord.setMetric("trackers_dead", numTrackersDead);

      metricsRecord.incrMetric("num_launched_jobs", numJobsLaunched);
      metricsRecord.incrMetric("total_submit_time", totalSubmitTime);
      
      metricsRecord.incrMetric("total_map_input_bytes", totalMapInputBytes);
      metricsRecord.incrMetric("local_map_input_bytes", localMapInputBytes);
      metricsRecord.incrMetric("rack_map_input_bytes", rackMapInputBytes);

      for (Group group: countersToMetrics) {
        String groupName = group.getName();
        for (Counter counter : group) {
          String name = groupName + "_" + counter.getName();
          name = name.replaceAll("[^a-zA-Z_]", "_").toLowerCase();
          metricsRecord.incrMetric(name, counter.getValue());
        }
      }
      clearCounters();

      numMapTasksLaunched = 0;
      numMapTasksCompleted = 0;
      numMapTasksFailed = 0;
      numReduceTasksLaunched = 0;
      numReduceTasksCompleted = 0;
      numReduceTasksFailed = 0;
      numJobsSubmitted = 0;
      numJobsCompleted = 0;
      numWaitingMaps = 0;
      numWaitingReduces = 0;
      numBlackListedMapSlots = 0;
      numBlackListedReduceSlots = 0;
      numSpeculativeMaps = 0;
      numSpeculativeReduces = 0;
      numSpeculativeSucceededMaps = 0;
      numSpeculativeSucceededReduces = 0;
      numSpeculativeWasteMaps = 0;
      numSpeculativeWasteReduces = 0;
      speculativeMapTimeWaste = 0L;
      speculativeReduceTimeWaste = 0L;
      killedMapTime = 0;
      killedReduceTime = 0;
      failedMapTime = 0;
      failedReduceTime = 0;
      numDataLocalMaps = 0;
      numRackLocalMaps = 0;

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

      numMapTasksKilled = 0;
      numReduceTasksKilled = 0;

      numTrackers = 0;
      numTrackersBlackListed = 0;

      totalSubmitTime = 0;
      numJobsLaunched = 0;
      
      totalMapInputBytes = 0;
      localMapInputBytes = 0;
      rackMapInputBytes = 0;      
    }
    metricsRecord.update();
  }

  @Override
  public synchronized void launchMap(TaskAttemptID taskAttemptID) {
    ++numMapTasksLaunched;
    decWaitingMaps(taskAttemptID.getJobID(), 1);
  }
  @Override
  public synchronized void launchDataLocalMap(TaskAttemptID taskAttemptID) {
    ++numDataLocalMaps;
  }
  @Override
  public synchronized void launchRackLocalMap(TaskAttemptID taskAttemptID) {
    ++numRackLocalMaps;
  }

  @Override
  public synchronized void completeMap(TaskAttemptID taskAttemptID) {
    ++numMapTasksCompleted;
  }

  @Override
  public synchronized void speculateMap(TaskAttemptID taskAttemptID) {
    ++numSpeculativeMaps;
  }

  public synchronized void speculativeSucceededMap(
          TaskAttemptID taskAttemptID) {
    ++numSpeculativeSucceededMaps;
  }

  public synchronized void speculativeSucceededReduce(
          TaskAttemptID taskAttemptID) {
    ++numSpeculativeSucceededReduces;
  }

  @Override
  public synchronized void failedMap(TaskAttemptID taskAttemptID,
      boolean wasFailed, boolean isSpeculative, long taskStartTime) {
    long timeSpent = JobTracker.getClock().getTime() - taskStartTime;
    if (wasFailed) {
      ++numMapTasksFailed;
      failedMapTime += timeSpent;
    } else {
      ++numMapTasksKilled;
      killedMapTime += timeSpent;
	    if (isSpeculative) {
	      ++numSpeculativeWasteMaps;
	      speculativeMapTimeWaste += timeSpent;
	    }
    }
    addWaitingMaps(taskAttemptID.getJobID(), 1);
  }

  @Override
  public synchronized void launchReduce(TaskAttemptID taskAttemptID) {
    ++numReduceTasksLaunched;
    decWaitingReduces(taskAttemptID.getJobID(), 1);
  }

  @Override
  public synchronized void completeReduce(TaskAttemptID taskAttemptID) {
    ++numReduceTasksCompleted;
  }

  @Override
  public synchronized void speculateReduce(TaskAttemptID taskAttemptID) {
    ++numSpeculativeReduces;
  }

  @Override
  public synchronized void failedReduce(TaskAttemptID taskAttemptID,
      boolean wasFailed, boolean isSpeculative, long taskStartTime) {
    long timeSpent = JobTracker.getClock().getTime() - taskStartTime;
    if (wasFailed) {
      ++numReduceTasksFailed;
      failedReduceTime += timeSpent;
    } else {
      ++numReduceTasksKilled;
      failedReduceTime += timeSpent;
	    if (isSpeculative) {
	      ++numSpeculativeWasteReduces;
	      speculativeReduceTimeWaste += timeSpent;
	    }
    }
    addWaitingReduces(taskAttemptID.getJobID(), 1);
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
  public synchronized void addMapInputBytes(long size) {
    totalMapInputBytes += size;
  }

  @Override
  public synchronized void addLocalMapInputBytes(long size) {
    localMapInputBytes += size;
    addMapInputBytes(size);
  }

  @Override
  public synchronized void addRackMapInputBytes(long size) {
    rackMapInputBytes += size;
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
