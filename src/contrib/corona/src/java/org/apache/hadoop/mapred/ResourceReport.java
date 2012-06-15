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

/**
 * Data transfer object for resource reports that is passed to
 * coronajobresources.jsp (Corona Proxy Job Tracker).
 */
public class ResourceReport {
  /** The id of the grant */
  private final Integer grantId;
  /** The task attempt being run using this grant*/
  private final String taskAttemptString;

  /**
   * Construct resource report given a grant id and a task attempt
   * being run using this grant
   * @param grantId the id of the grant
   * @param taskAttemptString the task attempt being run
   */
  public ResourceReport(Integer grantId, String taskAttemptString) {
    this.grantId = grantId;
    this.taskAttemptString = taskAttemptString;
  }

  public Integer getGrantId() {
    return grantId;
  }

  public String getTaskAttemptString() {
    return taskAttemptString;
  }
}
