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

import org.apache.hadoop.ipc.VersionedProtocol;

/**
 * Interface for aggregation information about corona map-reduce jobs.
 */
public interface CoronaJobAggregator extends VersionedProtocol {
  public static final int versionID = 1;

  /**
   * Report a job's counters after the job is completed.
   * @param jobId The job identifier.
   * @param pool The pool that the job ran in.
   * @param stats The job statistics.
   * @param counters Counters for the job.
   */
  public void reportJobStats(
    String jobId,
    String pool,
    JobStats stats,
    Counters counters);

  /**
   * Report a job's error statistics.
   */
  public void reportJobErrorCounters(Counters counters);
}
