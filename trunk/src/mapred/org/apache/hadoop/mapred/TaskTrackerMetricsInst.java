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

import org.apache.hadoop.metrics.MetricsContext;
import org.apache.hadoop.metrics.MetricsRecord;
import org.apache.hadoop.metrics.MetricsUtil;
import org.apache.hadoop.metrics.Updater;
import org.apache.hadoop.metrics.jvm.JvmMetrics;
import org.apache.hadoop.metrics.util.MetricsBase;
import org.apache.hadoop.metrics.util.MetricsRegistry;
import org.apache.hadoop.metrics.util.MetricsTimeVaryingRate;

class TaskTrackerMetricsInst extends TaskTrackerInstrumentation
                             implements Updater {
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

  public TaskTrackerMetricsInst(TaskTracker t) {
    super(t);
    JobConf conf = tt.getJobConf();
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
      metricsRecord.setMetric("unaccounted_memory", unaccountedMemory);

      numCompletedMapTasks = 0;
      numCompletedReduceTasks = 0;
      numCompletedTasks = 0;
      timedoutTasks = 0;
      tasksFailedPing = 0;
    }
      metricsRecord.update();
  }
}
