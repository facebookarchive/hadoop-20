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

import java.io.IOException;

import org.apache.hadoop.ipc.VersionedProtocol;
import org.apache.hadoop.mapred.CoronaSessionInfo.InetSocketAddressWritable;

/**
 * The protocol used in communication between the job tracker running inside of
 * the cluster and the parent job tracker running on the client
 */
@SuppressWarnings("deprecation")
public interface InterCoronaJobTrackerProtocol extends VersionedProtocol {

  /** Version of the protocol */
  public static final long versionID = 2;

  /**
   * Used by a Corona Job Tracker to report its RPC host:port information to the
   * parent Corona Job Tracker that started it. This also serves as heartbeat to
   * the parent.
   * 
   * @param attempt The Job Tracker task attempt that this remote Corona Job
   *        Tracker represents.
   * @param host The host running the remote Corona Job Tracker
   * @param port The RPC port of the remote Corona Job Tracker
   * @param sessionId The sessionId of the remote Corona Job Tracker.
   * @throws IOException
   */
  void reportRemoteCoronaJobTracker(String attemptId, String host,
      int port, String sessionId) throws IOException;

  /**
   * Used by Corona Task Tracker to obtain new Job Tracker address after JT
   * failure. Returns null if new JT is not ready and throws if JT can't
   * restarted.
   * @param failedTracker address of job tracker
   * @return address of new job tracker
   * @throws IOException
   */
  InetSocketAddressWritable getNewJobTrackerAddress(
      InetSocketAddressWritable failedTracker) throws IOException;

  /**
   * Sends status update. Either of arguments can be null.
   * @param attempt task attempt associated with remote job tracker
   * @param statuses array of task statuses to record
   * @param launch describes launched task
   * @throws IOException
   */
  void pushCoronaJobTrackerStateUpdate(TaskAttemptID attempt,
      CoronaStateUpdate[] updates) throws IOException;

  /**
   * Returns saved state
   * @param attempt task attempt associated with remote job tracker
   * @return saved state
   * @throws IOException
   */
  CoronaJTState getCoronaJobTrackerState(TaskAttemptID attempt)
      throws IOException;

  /**
   * Given array of task attempts that caller wants to commit returns array of
   * task attempts matching tasks of provided attempts, that were last
   * committing
   * @param attempt task attempt of calling remote JT
   * @param toCommit attempts that we want to commit
   * @return task attempts that were/are committing
   * @throws IOException
   */
  TaskAttemptID[] getAndSetCommitting(TaskAttemptID attempt,
      TaskAttemptID[] attempts) throws IOException;

}
