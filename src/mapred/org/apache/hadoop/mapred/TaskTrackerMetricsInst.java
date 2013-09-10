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

import java.util.Collection;
import org.apache.hadoop.metrics.MetricsContext;
import org.apache.hadoop.metrics.MetricsRecord;
import org.apache.hadoop.metrics.MetricsUtil;
import org.apache.hadoop.metrics.Updater;
import org.apache.hadoop.metrics.jvm.JvmMetrics;
import org.apache.hadoop.metrics.util.MetricsBase;
import org.apache.hadoop.metrics.util.MetricsRegistry;
import org.apache.hadoop.metrics.util.MetricsTimeVaryingRate;
import org.apache.hadoop.util.ProcfsBasedProcessTree;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

class TaskTrackerMetricsInst extends TaskTrackerInstrumentation
                             implements Updater {
  /** Configuration variable for extra jvms */
  public static final String EXTRA_JVMS = "mapred.extraJvms";
  private static final Log LOG =
      LogFactory.getLog(TaskTrackerMetricsInst.class);
  /** Registry of a subset of metrics */
  private final MetricsRegistry registry = new MetricsRegistry();
  private final MetricsTimeVaryingRate taskLaunchMsecs =
      new MetricsTimeVaryingRate("taskLaunchMsecs", registry,
          "Msecs to launch a task after getting request.", true);
  /** All metrics are put here */
  private final MetricsRecord metricsRecord;
  /**
   * Number of completed map tasks (subset of numCompletedTasks),
   * includes setup/clean up tasks
   */
  private int numCompletedMapTasks = 0;
  /** Number of completed reduce tasks (subset of numCompletedTasks) */
  private int numCompletedReduceTasks = 0;
  private int numCompletedTasks = 0;
  private int timedoutTasks = 0;
  private int tasksFailedPing = 0;
  private long unaccountedMemory = 0;
  private int  numDiskOutOfSpaceTasks = 0;

  @Override
  public synchronized void diskOutOfSpaceTask(TaskAttemptID t) {
    ++numDiskOutOfSpaceTasks;
  }

  /** Tree for checking the proc fs */
  private ProcfsBasedProcessTree processTree =
      new ProcfsBasedProcessTree("-1", false, -1);
  /** Extra JVMs allowed beyond the maps + reduces before dumping procs */
  private final int extraJvms;
  private final boolean checkJvms = ProcfsBasedProcessTree.isAvailable();

  public TaskTrackerMetricsInst(TaskTracker t) {
    super(t);
    JobConf conf = tt.getJobConf();
    extraJvms = conf.getInt(EXTRA_JVMS, 16);
    String sessionId = conf.getSessionId();
    // Initiate Java VM Metrics
    JvmMetrics.init("TaskTracker", sessionId);
    // Create a record for Task Tracker metrics
    MetricsContext context = MetricsUtil.getContext("mapred");
    metricsRecord = MetricsUtil.createRecord(context, "tasktracker"); //guaranteed never null
    metricsRecord.setTag("sessionId", sessionId);
    context.registerUpdater(this);
  }

  @Override
  public synchronized void completeTask(TaskAttemptID t) {
    if (t.isMap()) {
      ++numCompletedMapTasks;
    } else {
      ++numCompletedReduceTasks;
    }
    ++numCompletedTasks;
  }

  @Override
  public synchronized void timedoutTask(TaskAttemptID t) {
    ++timedoutTasks;
  }

  @Override
  public synchronized void taskFailedPing(TaskAttemptID t) {
    ++tasksFailedPing;
  }

  @Override
  public synchronized void unaccountedMemory(long memory) {
    unaccountedMemory = memory;
  }

  @Override
  public synchronized void addTaskLaunchMsecs(long msecs) {
    taskLaunchMsecs.inc(msecs);
  }

  @Override
  public MetricsTimeVaryingRate getTaskLaunchMsecs() {
    return taskLaunchMsecs;
  }

  /**
   * Check the number of jvms running on this node and also set the metric
   * For use in doUpdates().
   */
  private void checkAndSetJvms() {
    Collection<String> jvmProcs =
        processTree.getProcessNameContainsCount("java ");
    metricsRecord.setMetric("all_node_jvms", jvmProcs.size());
    int maxExpected = tt.getMaxActualMapTasks() + tt.getMaxActualReduceTasks() +
        extraJvms;
    if (maxExpected < jvmProcs.size()) {
      LOG.warn("checkAndSetJvms: Expected up to " + maxExpected + " jvms, " +
          "but got " + jvmProcs.size());
      for (String jvmProc : jvmProcs) {
        LOG.warn(jvmProc);
      }
    }
  }

  /**
   * Since this object is a registered updater, this method will be called
   * periodically, e.g. every 5 seconds.
   */
  @Override
  public void doUpdates(MetricsContext unused) {
    synchronized (this) {
      for (MetricsBase metricsBase : registry.getMetricsList()) {
        metricsBase.pushMetric(metricsRecord);
      }

      metricsRecord.setMetric("aveMapSlotRefillMsecs",
        tt.getAveMapSlotRefillMsecs());
      metricsRecord.setMetric("aveReduceSlotRefillMsecs",
        tt.getAveReduceSlotRefillMsecs());
      if (checkJvms) {
        checkAndSetJvms();
      }

      metricsRecord.setMetric("cgroup_memory_oom", tt.getCGroupOOM());
      metricsRecord.setMetric("maps_running", tt.getRunningMaps());
      metricsRecord.setMetric("reduces_running", tt.getRunningReduces());
      metricsRecord.setMetric("mapTaskSlots", (short)tt.getMaxActualMapTasks());
      metricsRecord.setMetric("reduceTaskSlots",
                                   (short)tt.getMaxActualReduceTasks());
      metricsRecord.incrMetric("map_tasks_completed",
          numCompletedMapTasks);
      metricsRecord.incrMetric("reduce_tasks_completed",
          numCompletedReduceTasks);
      metricsRecord.incrMetric("tasks_completed", numCompletedTasks);
      metricsRecord.incrMetric("tasks_failed_timeout", timedoutTasks);
      metricsRecord.incrMetric("tasks_failed_ping", tasksFailedPing);
      metricsRecord.incrMetric("tasks_disk_out_of_space", 
          numDiskOutOfSpaceTasks);
      metricsRecord.setMetric("unaccounted_memory", unaccountedMemory);
      metricsRecord.incrMetric("tasks_failed_to_add_cgroup", tt.getAndResetNumFailedToAddTaskToCGroup());
      metricsRecord.incrMetric("tasks_saved_by_cgroup", tt.getAndResetAliveTaskNumInCGroup());
      metricsRecord.setMetric("rss_memory_usage", tt.getTaskTrackerRSSMem());
      metricsRecord.incrMetric("tasks_cpu_saved_by_cgroup", tt.getAndResetAliveTasksCPUMSecs());
      
      for (int index = 0; index <tt.getKilledTaskRssBucketsNum(); ++ index) {
        String metricKey = "tasks_killed_group_by_rss_" + index;
        metricsRecord.incrMetric(metricKey, tt.getAndRestNumOfKilledTasksByRssBucket(index));
      }

      numCompletedMapTasks = 0;
      numCompletedReduceTasks = 0;
      numCompletedTasks = 0;
      timedoutTasks = 0;
      tasksFailedPing = 0;
      numDiskOutOfSpaceTasks = 0;
    }
    
    metricsRecord.update();
  }
}
