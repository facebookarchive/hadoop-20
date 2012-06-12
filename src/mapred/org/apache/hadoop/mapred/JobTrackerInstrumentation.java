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

class JobTrackerInstrumentation {

  protected final JobTracker tracker;

  public JobTrackerInstrumentation(JobTracker jt, JobConf conf) {
    tracker = jt;
  }

  public void launchMap(TaskAttemptID taskAttemptID)
  { }

  public void completeMap(TaskAttemptID taskAttemptID)
  { }

  /**
   * Record a failed map task
   * 
   * @param taskAttemptID the task attempt to mark failed
   * @param wasFailed whether it's a failed TIP
   * @param isSpeculative if the task was launched as a speculated task
   * @param isUsingSpeculationByProcessingRate - if using processing rate
   * (bytes/s) instead of (%/s) to determine whether to speculate.
   * @param taskStartTime time 
   */
  public void failedMap(TaskAttemptID taskAttemptID,
      boolean wasFailed, boolean isSpeculative, 
      boolean isUsingSpeculationByProcessingRate, long taskStartTime)
  { }

  public void launchReduce(TaskAttemptID taskAttemptID)
  { }

  public void completeReduce(TaskAttemptID taskAttemptID)
  { }

  /**
   * Record a failed map task
   * 
   * @param taskAttemptID the task attempt to mark failed
   * @param wasFailed whether it's a failed TIP
   * @param isSpeculative if the task was launched as a speculated task
   * @param isUsingSpeculationByProcessingRate - if using processing rate
   * (bytes/s) instead of (%/s) to determine whether to speculate.
   * @param taskStartTime
   */
  public synchronized void failedReduce(TaskAttemptID taskAttemptID,
      boolean wasFailed, boolean isSpeculative, 
      boolean isUsingSpeculationByProcessingRate, long taskStartTime)
  { }

  public void mapFailedByFetchFailures()
  { }

  public void mapFetchFailure()
  { }

  public void submitJob(JobConf conf, JobID id)
  { }

  public void completeJob(JobConf conf, JobID id)
  { }

  public void terminateJob(JobConf conf, JobID id)
  { }

  public void finalizeJob(JobConf conf, JobID id)
  { }

  public void addWaitingMaps(JobID id, int task)
  { }

  public void decWaitingMaps(JobID id, int task)
  { }

  public void addWaitingReduces(JobID id, int task)
  { }

  public void decWaitingReduces(JobID id, int task)
  { }

  public void setMapSlots(int slots)
  { }

  public void setReduceSlots(int slots)
  { }

  public void addBlackListedMapSlots(int slots)
  { }

  public void decBlackListedMapSlots(int slots)
  { }

  public void addBlackListedReduceSlots(int slots)
  { }

  public void decBlackListedReduceSlots(int slots)
  { }

  public void addReservedMapSlots(int slots)
  { }

  public void decReservedMapSlots(int slots)
  { }

  public void addReservedReduceSlots(int slots)
  { }

  public void decReservedReduceSlots(int slots)
  { }

  public void addOccupiedMapSlots(int slots)
  { }

  public void decOccupiedMapSlots(int slots)
  { }

  public void addOccupiedReduceSlots(int slots)
  { }

  public void decOccupiedReduceSlots(int slots)
  { }

  public void failedJob(JobConf conf, JobID id)
  { }

  public void killedJob(JobConf conf, JobID id)
  { }

  public void addPrepJob(JobConf conf, JobID id)
  { }

  public void decPrepJob(JobConf conf, JobID id)
  { }

  public void addRunningJob(JobConf conf, JobID id)
  { }

  public void decRunningJob(JobConf conf, JobID id)
  { }

  public void addRunningMaps(int tasks)
  { }

  public void decRunningMaps(int tasks)
  { }

  public void addRunningReduces(int tasks)
  { }

  public void decRunningReduces(int tasks)
  { }

  public void killedMap(TaskAttemptID taskAttemptID)
  { }

  public void killedReduce(TaskAttemptID taskAttemptID)
  { }

  public void addTrackers(int trackers)
  { }

  public void decTrackers(int trackers)
  { }

  public void addBlackListedTrackers(int trackers)
  { }

  public void decBlackListedTrackers(int trackers)
  { }

  public void setDecommissionedTrackers(int trackers)
  { }

  public synchronized void addLaunchedJobs(long submitTime)
  { }

  /**
   * Record when a map task was launched speculatively
   * 
   * @param taskAttemptID id of the task that was speculated
   * @param isUsingProcessingRate - if it used processing rate (e.g.
   * bytes/sec) to determine whether to speculate
   */
  public synchronized void speculateMap(TaskAttemptID taskAttemptID,
      boolean isUsingProcessingRate)
  { }

  /**
   * Record when a reduce task was launched speculatively
   * 
   * @param taskAttemptID id of the task that was speculated
   * @param isUsingProcessingRate if it is used processing rate (e.g.
   * bytes/sec) to determine whether to speculate
   */
  public synchronized void speculateReduce(TaskAttemptID taskAttemptID,
      boolean isUsingProcessingRate)
  { }

  /**
   * Record when a speculated map task succeeded
   * 
   * @param taskAttemptID id of the task that was speculated
   * @param isUsingProcessingRateif it used processing rate (e.g.
   * bytes/sec) to determine whether to speculate
   */
  public synchronized void speculativeSucceededMap(TaskAttemptID taskAttemptID,
      boolean isUsingProcessingRate)
  { }

  /**
   * Record when a speculated reduce task succeeded
   * 
   * @param taskAttemptID id of the task that was speculated
   * @param isUsingProcessingRate if it used processing rate (e.g.
   * bytes/sec) to determine whether to speculate
   */
  public synchronized void speculativeSucceededReduce(TaskAttemptID
    taskAttemptID, boolean isUsingProcessingRate)
  { }

  public synchronized void launchDataLocalMap(TaskAttemptID taskAttemptID)
  { }

  public synchronized void launchRackLocalMap(TaskAttemptID taskAttemptID)
  { }

  public synchronized void addMapInputBytes(long size)
  { }

  public synchronized void addLocalMapInputBytes(long size)
  { }

  public synchronized void addRackMapInputBytes(long size)
  { }
}
