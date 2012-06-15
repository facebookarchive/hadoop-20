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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

/**
 * Contains job utilization information on one TaskTracker
 */
public class LocalJobUtilization implements Writable {
  private String jobId;         // jobid of the Hadoop job
  private double cpuUsageGHz;   // CPU usage (GHz) on the local machine
  private double memUsageGB;    // Memory usage (GB) on the local machine

  static public final String contentFormat = "%-25s%-8.2f%-8.2f\n";
  static public final String legendFormat = "%-25s%-8s%-8s\n";
  static public final String legendString =
          String.format(legendFormat, "jobID", "CPU", "Memory");
  static public final String unitString =
          String.format(legendFormat, "", "GHz", "GB");

  @Override
  public String toString() {
    return String.format(contentFormat,
                         getJobId(),
                         getCpuUsageGHz(),
                         getMemUsageGB());
  }

  //////////////////////////////////////////////////
  // Writable
  //////////////////////////////////////////////////
  @Override
  public void write(DataOutput out) throws IOException {
    Text.writeString(out, getJobId());
    out.writeDouble(getCpuUsageGHz());
    out.writeDouble(getMemUsageGB());
  }
  @Override
  public void readFields(DataInput in) throws IOException {
    setJobId(Text.readString(in));
    setCpuUsageGHz(in.readDouble());
    setMemUsageGB(in.readDouble());
  }

  /**
   * @return the jobId
   */
  public String getJobId() {
    return jobId;
  }

  /**
   * @param jobId the jobId to set
   */
  public void setJobId(String jobId) {
    this.jobId = jobId;
  }

  /**
   * @return the cpuUsageGHz
   */
  public double getCpuUsageGHz() {
    return cpuUsageGHz;
  }

  /**
   * @param cpuUsageGHz the cpuUsageGHz to set
   */
  public void setCpuUsageGHz(double cpuUsageGHz) {
    this.cpuUsageGHz = cpuUsageGHz;
  }

  /**
   * @return the memUsageGB
   */
  public double getMemUsageGB() {
    return memUsageGB;
  }

  /**
   * @param memUsageGB the memUsageGB to set
   */
  public void setMemUsageGB(double memUsageGHz) {
    this.memUsageGB = memUsageGHz;
  }

}
