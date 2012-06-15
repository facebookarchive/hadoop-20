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

import java.util.ArrayList;
import java.util.Collection;

/**
 * A schedulable pool of jobs.
 */
public class Pool {
  /** Name of the default pool, where jobs with no pool parameter go. */
  public static final String DEFAULT_POOL_NAME = "default";

  /** Pool name. */
  private String name;

  /** Jobs in this specific pool; does not include children pools' jobs. */
  private Collection<JobInProgress> jobs = new ArrayList<JobInProgress>();

  /** Is this configured in pools.xml? */
  private boolean isConfigured;

  public Pool(String name, boolean isConfigured) {
    this.name = name;
    this.isConfigured = isConfigured;
  }

  public Collection<JobInProgress> getJobs() {
    return jobs;
  }

  public void addJob(JobInProgress job) {
    jobs.add(job);
  }

  /**
   * Remove the job from the pool
   *
   * @param job Job to remove from the pool
   * @return True if the job was removed
   */
  public boolean removeJob(JobInProgress job) {
    return jobs.remove(job);
  }

  public String getName() {
    return name;
  }

  public boolean isDefaultPool() {
    return Pool.DEFAULT_POOL_NAME.equals(name);
  }

  public boolean isConfiguredPool() {
    return isConfigured;
  }
}
