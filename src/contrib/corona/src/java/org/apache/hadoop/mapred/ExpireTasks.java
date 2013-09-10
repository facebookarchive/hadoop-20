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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Times out tasks that take too long to start up
 * or do not heartbeat for a while
 */
public class ExpireTasks extends Thread {
  /** Logger. */
  private static final Log LOG = LogFactory.getLog(ExpireTasks.class);
  /**
   * This is a map of the tasks that have been assigned to task trackers,
   * but that have not yet been seen in a status report.
   * map: task-id -> time-assigned
   */
  private final Map<TaskAttemptID, Long> runningTasks =
      new LinkedHashMap<TaskAttemptID, Long>();
  /**
   * This is a map of the tasks that have been assigned to task trackers,
   * but that have not yet been seen in a status report.
   * map: task-id -> time-assigned
   */
  private final Map<TaskAttemptID, Long> launchingTasks =
      new LinkedHashMap<TaskAttemptID, Long>();

  /** Running flag. */
  private volatile boolean running = true;
  /** The Corona Job Tracker. */
  private final CoronaJobTracker cjt;

  /**
   * Constructor.
   *
   * @param cjt
   *          The Corona Job Tracker.
   */
  public ExpireTasks(CoronaJobTracker cjt) {
    this.cjt = cjt;
  }

  @Override
  public void run() {
    final long sleepTime = Math.max(10000, Math.min(
        cjt.getLaunchExpiryInterval(), cjt.getTaskExpiryInterval()) / 3);
    while (running) {
      try {
        long now = JobTracker.getClock().getTime();
        LOG.info("Starting launching task sweep");
        List<TaskAttemptID> expiredLaunchingTasks =
          new ArrayList<TaskAttemptID>();
        List<TaskAttemptID> expiredRunningTasks =
          new ArrayList<TaskAttemptID>();
        synchronized (this) {
          Iterator<Map.Entry<TaskAttemptID, Long>> itr =
              launchingTasks.entrySet().iterator();
          while (itr.hasNext()) {
            Map.Entry<TaskAttemptID, Long> pair = itr.next();
            TaskAttemptID taskId = pair.getKey();
            long age = now - (pair.getValue()).longValue();
            if (age > cjt.getLaunchExpiryInterval()) {
              LOG.error("Launching task " + taskId + " timed out after " +
                (age / 1000) + " secs.");
              expiredLaunchingTasks.add(taskId);
              itr.remove();
            }
          }

          itr = runningTasks.entrySet().iterator();
          while (itr.hasNext()) {
            Map.Entry<TaskAttemptID, Long> pair = itr.next();
            TaskAttemptID taskId = pair.getKey();
            long age = now - (pair.getValue()).longValue();
            if (age > cjt.getTaskExpiryInterval()) {
              LOG.error("Running task " + taskId + " timed out after " +
                (age / 1000) + " secs.");
              expiredRunningTasks.add(taskId);
              itr.remove();
            }
          }
        }
        // Call into job tracker outside of the local lock.
        for (TaskAttemptID taskId : expiredLaunchingTasks) {
          cjt.expiredLaunchingTask(taskId);
        }

        for (TaskAttemptID taskId : expiredRunningTasks) {
          cjt.expiredRunningTask(taskId);
        }
        // Check for any tasks that are overdue every sleepTime msec
        Thread.sleep(sleepTime);
      } catch (InterruptedException ie) {
        // Ignore. if shutting down, while condition will catch it.
        LOG.info(ExpireTasks.class + " interrupted");
      }
    }
  }

  /**
   * Add new task to the list of launch pending tasks.
   * @param taskName The task attempt.
   */
  public void addNewTask(TaskAttemptID taskName) {
    synchronized (this) {
      launchingTasks.put(taskName,
          JobTracker.getClock().getTime());
    }
  }

  /**
   * If we heard from this task - update the timestamp
   * @param taskName the id of the task updated
   */
  public void updateTask(TaskAttemptID taskName) {
    synchronized (this) {
      runningTasks.put(taskName, JobTracker.getClock().getTime());
    }
  }

  /**
   * When the task is finished remove it completely
   * @param taskName the id of the task finished
   */
  public void finishedTask(TaskAttemptID taskName) {
    synchronized (this) {
      runningTasks.remove(taskName);
    }
  }

  /**
   * Remove a task from the list of launch pending tasks.
   * @param taskName The task attempt.
   */
  public void removeTask(TaskAttemptID taskName) {
    synchronized (this) {
      launchingTasks.remove(taskName);
    }
  }

  /**
   * Trigger the shutdown.
   */
  public void shutdown() {
    running = false;
  }
}
