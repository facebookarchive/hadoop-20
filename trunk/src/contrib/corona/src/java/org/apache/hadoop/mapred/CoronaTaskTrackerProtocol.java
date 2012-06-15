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
 * Protocol that allows TaskTracker receives commands from ClusterManager and
 * JobTracker
 */
public interface CoronaTaskTrackerProtocol extends VersionedProtocol {

  /** The version of the protocol */
  public static final long versionID = 2L;

  /**
   * Tell TaskTracker to perform a list of actions
   * @param actions The actions to perform
   * @throws IOException
   */
  public void submitActions(TaskTrackerAction[] actions) throws IOException,
      InterruptedException;

  /**
   * Used to start a Corona Job Tracker
   *
   * @param jobTask
   *          A fake task that provides information about jobid, taskid etc.
   * @param info
   *          Information about the corona session that is starting this job
   *          tracker.
   */
  public void startCoronaJobTracker(Task jobTask, CoronaSessionInfo info)
    throws IOException;
}
