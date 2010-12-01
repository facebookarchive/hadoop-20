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

import org.apache.hadoop.ipc.VersionedProtocol;

/**********************************************************************
 * CollectorProtocol is used by user code
 * {@link UtilizationReporter} class to communicate
 * with the Collector.
 **********************************************************************/

public interface UtilizationCollectorProtocol extends VersionedProtocol {
  /**
   * Compared to the previous version the following changes have been introduced:
   * Only the latest change is reflected.
   * 1: new protocol introduced
   * 2: change cumulative CPU time in JobUtilization to double instead of long
   * 3: add a new field cpu giga-cycle in JobUtilization
   */
  public static final long versionID = 3L;

  /**
   * Get the resource utilization of a job
   * @throws IOException
   * @param hostName
   * @return {@link JobUtilization} which contains the utilization information
   */
  public TaskTrackerUtilization getTaskTrackerUtilization(String hostName)
          throws IOException;

  /**
   * Get the resource utilization of a job.
   * @throws IOException
   * @param jobID a String contains the job's id
   * @return a class contains information about the job
   */
  public JobUtilization getJobUtilization(String jobId) throws IOException;

  /**
   * Get resource utilization information of all TaskTrackers
   * @throws IOException
   * @return an array contains recourse utilization information
   */
  public TaskTrackerUtilization[] getAllTaskTrackerUtilization()
          throws IOException;

  /**
   * Get resource utilization information of all job
   * @throws IOException
   * @return an array contains job utilization information
   */
  public JobUtilization[] getAllRunningJobUtilization() throws IOException;

  /**
   * Get the real-time cluster information
   * @throws IOException
   * @return a class contains running information about the cluster
   */
  public ClusterUtilization getClusterUtilization() throws IOException;

  /**
   * Report the real-time resource utilization information of a TaskTracker
   * @throws IOException
   */
  public void reportTaskTrackerUtilization(
          TaskTrackerUtilization ttUtil, LocalJobUtilization[] localJobUtil)
                                    throws IOException;
}
