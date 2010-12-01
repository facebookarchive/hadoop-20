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

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.JobID;

/**
 * A pluggable for obtaining cluster and job resource information
 */
abstract class ResourceReporter implements Configurable {
  static public int UNAVAILABLE = -1;
  
  protected Configuration conf;
  
  
  public Configuration getConf() {
    return conf;
  }

  public void setConf(Configuration conf) {
    this.conf = conf;
  }
  
  /**
   * @param jobid Job Id
   * @return CPU percentage of this job on the cluster
   */
  public abstract double getJobCpuPercentageOnCluster(JobID jobid);
  
  /**
   * @param jobid Job Id
   * @return Memory percentage of this job on the cluster
   */
  public abstract double getJobMemPercentageOnCluster(JobID jobid);
  
  /**
   * @param jobid Job Id
   * @return Maximum current value of CPU percentage per node
   */
  public abstract double getJobCpuMaxPercentageOnBox(JobID jobid);
  
  /**
   * @param jobid Job Id
   * @return Maximum current value of Memory percentage per node
   */
  public abstract double getJobMemMaxPercentageOnBox(JobID jobid);
  
  /**
   * @param jobid Job Id
   * @return Maximum current value of Memory percentage per node
   */
  public abstract double getJobMemMaxPercentageOnBoxAllTime(JobID jobid);
  
  /**
   * @param jobid Job Id
   * @return Cumulated CPU time (cluster-millisecond)
   */
  public abstract double getJobCpuCumulatedUsageTime(JobID jobid);
  
  /**
   * @param jobid Job Id
   * @return Cumulated memory usage time (cluster-millisecond)
   */
  public abstract double getJobMemCumulatedUsageTime(JobID jobid);
  
  /**
   * @param jobid Job Id
   * @return Total CPU giga-cycle used by this job
   */
  public abstract double getJobCpuCumulatedGigaCycles(JobID jobid);
  
  /**
   * @return Total CPU clock in GHz of the cluster
   */
  public abstract double getClusterCpuTotalGHz();
  
  /**
   * @return Total memory in GB of the cluster
   */
  public abstract double getClusterMemTotalGB();
  
  /**
   * @return Total CPU usage in GHz of the cluster
   */
  public abstract double getClusterCpuUsageGHz();
  
  /**
   * @return Total memory usage in GB of the cluster
   */
  public abstract double getClusterMemUsageGB();
  
  /**
   * @return Total memory usage in GB of the cluster
   */
  public abstract int getReportedTaskTrackers();
}
