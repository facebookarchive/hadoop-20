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

package org.apache.hadoop.mapred.protocal;

import java.io.IOException;

import org.apache.hadoop.ipc.VersionedProtocol;
import org.apache.hadoop.mapred.TaskTrackerStatus;
import org.apache.hadoop.mapreduce.ClusterMetrics;
import org.apache.hadoop.mapreduce.TaskType;

public interface FairSchedulerProtocol extends VersionedProtocol {

  /**
   * Compared to the previous version the following changes have been introduced: * Only the latest change is reflected.
   * 1: new protocol introduced
   */
  public static final long versionID = 1L;

  /**
   * Obtain the total number of slots for the tasktracker.
   * @param status The status of the tasktracker
   * @param type The task type we want to know about the capacity
   * @return The number of total slots on the tasktracker.
   */
  public int getMaxSlots(TaskTrackerStatus status, TaskType type)
      throws IOException;
  
  /**
   * Obtain the number of the slots limit of a tasktracker on FairScheduler
   * @param trackerName The name of the task tracker
   * @param type The type of the task
   * @return The number of maximum slots
   */
  public int getFSMaxSlots(String trackerName, TaskType type)
      throws IOException;

  /**
   * Set the number of maximum slots on a tasktracker
   * @param trackerName The name of the tasktracker to set
   * @param type The type of the task to set
   * @param slots New number of maximum slots
   * @throws IOException
   */
  public void setFSMaxSlots(String trackerName, TaskType type, int slots)
      throws IOException;
  
  /**
   * Set the number of the maximum slots to the configured number
   * @throws IOException
   */
  public void resetFSMaxSlots() throws IOException;

  /**
   * Get a list of the status of the TaskTrackers being managed
   * @return The status of TaskTrackers
   * @throws IOException
   */
  public TaskTrackerStatus[] getTaskTrackerStatus() throws IOException;

  /**
   * Return the number of runnable tasks in the cluster
   * @param type The type of the runnable task
   * @return The number of the runnable tasks
   * @throws IOException
   */
  public int getRunnableTasks(TaskType type) throws IOException;

  /**
   * Return the number of running mappers and reducers
   * @param pool The name of the pool
   * @return The array contains number of running mappers and reducers
   * @throws IOException
   */
  public int[] getPoolRunningTasks(String pool) throws IOException;

  /**
   * Return the number of maximum mappers and reducers
   * @param pool The name of the pool
   * @return The array contains number of maximum mappers and reducers
   * @throws IOException
   */
  public int[] getPoolMaxTasks(String pool) throws IOException;
}
