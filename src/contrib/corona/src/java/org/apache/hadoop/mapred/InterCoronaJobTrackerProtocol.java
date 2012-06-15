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

/**
 * The protocol used in communication between the job tracker running
 * inside of the cluster and the parent job tracker running on the client
 */
public interface InterCoronaJobTrackerProtocol extends VersionedProtocol {

  /** Version of the protocol */
  public static final long versionID = 1;

  /**
   * Used by a Corona Job Tracker to report its RPC host:port information to the
   * parent Corona Job Tracker that started it. This also serves as heartbeat to
   * the parent.
   *
   * @param attempt
   *          The Job Tracker attempt that this remote Corona Job Tracker
   *          represents.
   * @param host
   *          The host running the remote Corona Job Tracker
   * @param port
   *          The RPC port of the remote Corona Job Tracker
   * @param sessionId
   *          The sessionId of the remote Corona Job Tracker.
   * @throws IOException
   */
  void reportRemoteCoronaJobTracker(String attempt, String host, int port,
      String sessionId)
    throws IOException;
}
