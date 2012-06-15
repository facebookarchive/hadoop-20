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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.ResourceReporter;
import org.apache.hadoop.mapreduce.JobID;


public class HmonResourceReporter extends ResourceReporter {
  UtilizationCollectorCached collectorCached;

  @Override
  public void setConf(Configuration conf) {
    super.setConf(conf);
    collectorCached = UtilizationCollectorCached.getInstance(conf);
  }

  @Override
  public double getClusterCpuTotalGHz() {
    try {
      return collectorCached.getClusterUtilization().getCpuTotalGHz();
    } catch (Exception e) {
      return UNAVAILABLE;
    }
  }

  @Override
  public double getClusterCpuUsageGHz() {
    try {
      return collectorCached.getClusterUtilization().getCpuUsageGHz();
    } catch (Exception e) {
      return UNAVAILABLE;
    }
  }

  @Override
  public double getClusterMemTotalGB() {
    try {
      return collectorCached.getClusterUtilization().getMemTotalGB();
    } catch (Exception e) {
      return UNAVAILABLE;
    }
  }

  @Override
  public double getClusterMemUsageGB() {
    try {
      return collectorCached.getClusterUtilization().getMemUsageGB();
    } catch (Exception e) {
      return UNAVAILABLE;
    }
  }

  @Override
  public double getJobCpuCumulatedGigaCycles(JobID jobid) {
    try {
      return collectorCached.getJobUtilization(jobid.toString())
               .getCpuGigaCycles();
    } catch (Exception e) {
      return UNAVAILABLE;
    }
  }

  @Override
  public double getJobCpuCumulatedUsageTime(JobID jobid) {
    try {
      if (collectorCached == null || jobid == null) {
        return UNAVAILABLE;
      }
      return collectorCached.getJobUtilization(jobid.toString())
               .getCpuCumulatedUsageTime();
    } catch (Exception e) {
      return UNAVAILABLE;
    }
  }

  @Override
  public double getJobCpuMaxPercentageOnBox(JobID jobid) {
    try {
      return collectorCached.getJobUtilization(jobid.toString())
               .getCpuMaxPercentageOnBox();
    } catch (Exception e) {
      return UNAVAILABLE;
    }
  }

  @Override
  public double getJobCpuPercentageOnCluster(JobID jobid) {
    try {
      return collectorCached.getJobUtilization(jobid.toString())
               .getCpuPercentageOnCluster();
    } catch (Exception e) {
      return UNAVAILABLE;
    }
  }

  @Override
  public double getJobMemCumulatedUsageTime(JobID jobid) {
    try {
      return collectorCached.getJobUtilization(jobid.toString())
               .getMemCumulatedUsageTime();
    } catch (Exception e) {
      return UNAVAILABLE;
    }
  }

  @Override
  public double getJobMemMaxPercentageOnBox(JobID jobid) {
    try {
      return collectorCached.getJobUtilization(jobid.toString())
               .getMemMaxPercentageOnBox();
    } catch (Exception e) {
      return UNAVAILABLE;
    }
  }

  @Override
  public double getJobMemMaxPercentageOnBoxAllTime(JobID jobid) {
    try {
      return collectorCached.getJobUtilization(jobid.toString())
               .getMemMaxPercentageOnBoxAllTime();
    } catch (Exception e) {
      return UNAVAILABLE;
    }
  }

  @Override
  public double getJobMemPercentageOnCluster(JobID jobid) {
    try {
      return collectorCached.getJobUtilization(jobid.toString())
               .getMemPercentageOnCluster();
    } catch (Exception e) {
      return UNAVAILABLE;
    }
  }
  
  @Override
  public int getReportedTaskTrackers() {
    try {
      return collectorCached.getClusterUtilization().getNumTaskTrackers();
    } catch (Exception e) {
      return UNAVAILABLE;
    }
  }

}
