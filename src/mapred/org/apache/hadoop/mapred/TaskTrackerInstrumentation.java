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

import java.io.File;
import org.apache.hadoop.metrics.util.MetricsTimeVaryingRate;

/**
 * TaskTrackerInstrumentation defines a number of instrumentation points
 * associated with TaskTrackers.  By default, the instrumentation points do
 * nothing, but subclasses can do arbitrary instrumentation and monitoring at
 * these points.
 * 
 * TaskTrackerInstrumentation interfaces are associated uniquely with a
 * TaskTracker.  We don't want an inner class here, because then subclasses
 * wouldn't have direct access to the associated TaskTracker.
 *  
 **/
class TaskTrackerInstrumentation  {

  protected final TaskTracker tt;
  
  public TaskTrackerInstrumentation(TaskTracker t) {
    tt = t;
  }
  
  /**
   * invoked when task attempt t succeeds
   * @param t
   */
  public void completeTask(TaskAttemptID t) { }
  
  public void timedoutTask(TaskAttemptID t) { }
  
  public void taskFailedPing(TaskAttemptID t) { }
  
  public void diskOutOfSpaceTask(TaskAttemptID t) {}

  /**
   * Called just before task attempt t starts.
   * @param stdout the file containing standard out of the new task
   * @param stderr the file containing standard error of the new task 
   */
  public void reportTaskLaunch(TaskAttemptID t, File stdout, File stderr)  { }
  
  /**
   * called when task t has just finished.
   * @param t
   */
  public void reportTaskEnd(TaskAttemptID t) {}
   
  /**
   * Called when a task changes status. 
   * @param task the task whose status changed
   * @param taskStatus the new status of the task
   */
  public void statusUpdate(Task task, TaskStatus taskStatus) {}

  /**
   * Called to record the amount of used memory that is not accounted for by the
   * TaskTracker.
   * 
   * @param memory
   *          The unaccounted memory.
   */
  public void unaccountedMemory(long memory) {}

  /**
   * Add the task launch msecs time to the metrics.
   *
   * @param msecs Msecs to launch the task after the command
   */
  public void addTaskLaunchMsecs(long msecs) { }

  /**
   * Get the metrics for the task launch msecs.
   *
   * @return Metrics for task launch msecs.  Returns null if no such metric
   * exists.
   */
  public MetricsTimeVaryingRate getTaskLaunchMsecs() {
    return null;
  }
}
