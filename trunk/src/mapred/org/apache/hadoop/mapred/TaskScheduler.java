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
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.mapreduce.server.jobtracker.TaskTracker;

/**
 * Used by a {@link JobTracker} to schedule {@link Task}s on
 * {@link TaskTracker}s.
 * <p>
 * {@link TaskScheduler}s typically use one or more
 * {@link JobInProgressListener}s to receive notifications about jobs.
 * <p>
 * It is the responsibility of the {@link TaskScheduler}
 * to initialize tasks for a job, by calling {@link JobInProgress#initTasks()}
 * between the job being added (when
 * {@link JobInProgressListener#jobAdded(JobInProgress)} is called)
 * and tasks for that job being assigned (by
 * {@link #assignTasks(TaskTracker)}).
 * @see EagerTaskInitializationListener
 */
abstract class TaskScheduler implements Configurable {

  protected Configuration conf;
  protected TaskTrackerManager taskTrackerManager;

  public Configuration getConf() {
    return conf;
  }

  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  public synchronized void setTaskTrackerManager(
      TaskTrackerManager taskTrackerManager) {
    this.taskTrackerManager = taskTrackerManager;
  }

  /**
   * Lifecycle method to allow the scheduler to start any work in separate
   * threads.
   * @throws IOException
   */
  public void start() throws IOException {
    // do nothing
  }

  /**
   * Lifecycle method to allow the scheduler to stop any work it is doing.
   * @throws IOException
   */
  public void terminate() throws IOException {
    // do nothing
  }

  /**
   * Returns the tasks we'd like the TaskTracker to execute right now.
   *
   * @param taskTracker The TaskTracker for which we're looking for tasks.
   * @return A list of tasks to run on that TaskTracker, possibly empty.
   */
  public abstract List<Task> assignTasks(TaskTracker taskTracker)
  throws IOException;

  /**
   * Returns a collection of jobs in an order which is specific to
   * the particular scheduler.
   * @param queueName
   * @return
   */
  public abstract Collection<JobInProgress> getJobs(String queueName);

  /**
   * Obtain the total number of slots for the tasktracker.
   * @param status The status of the tasktracker
   * @param type The task type we want to know about the capacity
   * @return The number of total slots on the tasktracker.
   */
  public int getMaxSlots(TaskTrackerStatus status, TaskType type) {
    return type == TaskType.MAP ?
        status.getMaxMapSlots() : status.getMaxReduceSlots();
  }

  /**
   * Returns a user readable string about the scheduling status of a job
   * @param job The job to query
   * @return The schedule information of the job
   */
  public String jobScheduleInfo(JobInProgress job) {
    return "n/a";
  }

  /**
   * Is this job configuration valid?
   * @param job Job to check
   * @throws InvalidJobConfException When the configuration is not valid
   */
  public void checkJob(JobInProgress job) throws InvalidJobConfException {
    // do nothing
  }
}
