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

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Times out tasks that take too long to start up after being sent to a Corona
 * Task Tracker.
 */
public class ExpireLaunchingTasks extends Thread {
  /** Logger. */
  private static final Log LOG = LogFactory.getLog(ExpireLaunchingTasks.class);
  /** Expiry interval. */
  private static final long TASKTRACKER_EXPIRY_INTERVAL = 10 * 60 * 1000;
  /**
   * This is a map of the tasks that have been assigned to task trackers,
   * but that have not yet been seen in a status report.
   * map: task-id -> time-assigned
   */
  private final Map<TaskAttemptID, Long> launchingTasks =
      new LinkedHashMap<TaskAttemptID, Long>();

  /** Running flag. */
  private volatile boolean running = true;
  /** Lock object. */
  private final Object lockObject;
  /** The Corona Job Tracker. */
  private final CoronaJobTracker cjt;

  /**
   * Constructor.
   *
   * @param lockObject
   *          The shared lock.
   * @param cjt
   *          The Corona Job Tracker.
   */
  public ExpireLaunchingTasks(Object lockObject, CoronaJobTracker cjt) {
    this.lockObject = lockObject;
    this.cjt = cjt;
  }

  @Override
  public void run() {
    while (running) {
      try {
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
              cjt.expiredLaunchingTask(taskId);
              itr.remove();
            }
          }
        }
        // Every 3 minutes check for any tasks that are overdue
        Thread.sleep(TASKTRACKER_EXPIRY_INTERVAL / 3);
      } catch (InterruptedException ie) {
        // Ignore. if shutting down, while condition will catch it.
        LOG.info(ExpireLaunchingTasks.class + " interrupted");
      }
    }
  }

  /**
   * Force the failure of a launching attempt.
   * @param attempt The task attempt.
   */
  public void failedLaunch(TaskAttemptID attempt) {
    synchronized (lockObject) {
      // Check if the attempt exists in the map.
      // It might have expired already.
      if (launchingTasks.containsKey(attempt)) {
        // Set the launch time to a very old value.
        launchingTasks.put(attempt, (long) 0);
        // Make the expire task logic run immediately.
        this.interrupt();
        lockObject.notify();
      }
    }
  }

  /**
   * Add new task to the list of launch pending tasks.
   * @param taskName The task attempt.
   */
  public void addNewTask(TaskAttemptID taskName) {
    synchronized (lockObject) {
      launchingTasks.put(taskName,
          JobTracker.getClock().getTime());
    }
  }

  /**
   * Remove a task from the list of launch pending tasks.
   * @param taskName The task attempt.
   */
  public void removeTask(TaskAttemptID taskName) {
    synchronized (lockObject) {
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
