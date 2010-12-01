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
 * Contains the resource utilization information of a job on the cluster
 */
public class JobUtilization implements Writable {
  private String jobId;                  // jobid of this Hadoop job
  private double cpuPercentageOnCluster; // CPU usage % on the cluster
  private double memPercentageOnCluster; // memory usage % on the cluster
  private double cpuMaxPercentageOnBox;  // maximum CPU usage on a machine
  private double memMaxPercentageOnBox;  // maximum memory usage on a machine
  private double memMaxPercentageOnBoxAllTime; // peak max memory over time
  private double cpuCumulatedUsageTime;  // cumulated CPU time of the cluster
  private double cpuGigaCycles;          // cumulated CPU cycles
  private double memCumulatedUsageTime;  // memory usage time of the cluster
  private long runningTime;            // how long the job is actually running
  private long stoppedTime;               // how long the job has stopped
  private Boolean isRunning;             // is this job currently running
  // in seconds

  public static final String contentFormat =
          "%-25s%-8.2f%-8.2f%-8.2f%-8.2f%-8.2f%-8.2f%-8.2f%-8.2f%-8.2f\n";
  static public final String legendFormat =
          "%-25s%-8s%-8s%-8s%-8s%-8s%-8s%-8s%-8s\n";
  static public final String legendString =
          String.format(legendFormat, "jobID", "%CPU", "%MEM", "MAXCPU",
                        "MAXMEM", "PEAKMEM", "CTIME", "MTIME", "STIME", "RTIME");
  static public final String unitString =
          String.format(legendFormat, "", "%CLU", "%CLU", "%BOX", "%BOX", "%BOX"
                        , "Sec", "Sec", "Sec", "Sec");

  public JobUtilization() {
    jobId = "";
    cpuPercentageOnCluster = 0D;
    memPercentageOnCluster = 0D;
    cpuMaxPercentageOnBox = 0D;
    memMaxPercentageOnBox = 0D;
    cpuCumulatedUsageTime = 0D;
    setCpuGigaCycles(0D);
    memCumulatedUsageTime = 0D;
    runningTime = 0L;
    stoppedTime = 0L;
    isRunning = true;
  }

  public JobUtilization(String jobId,
                 double cpuPercentageOnCluster,
                 double memPercentageOnCluster,
                 double cpuMaxPercentageOnBox,
                 double memMaxPercentageOnBox,
                 double cpuCumulatedUsageTime,
                 double memCumulatedUsageTime,
                 long runningTime,
                 long stoppedTime,
                 Boolean isRunning) {
    this.jobId = jobId;
    this.cpuPercentageOnCluster = cpuPercentageOnCluster;
    this.memPercentageOnCluster = memPercentageOnCluster;
    this.cpuMaxPercentageOnBox = cpuMaxPercentageOnBox;
    this.memMaxPercentageOnBox = memMaxPercentageOnBox;
    this.cpuCumulatedUsageTime = cpuCumulatedUsageTime;
    this.memCumulatedUsageTime = memCumulatedUsageTime;
    this.runningTime = runningTime;
    this.stoppedTime = stoppedTime;
    this.isRunning = isRunning;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == this) {
      return true;
    }
    if (obj == null || obj.getClass() != this.getClass()) {
      return false;
    }
    JobUtilization job = (JobUtilization)obj;
    if (!job.jobId.equals(jobId) ||
        job.cpuPercentageOnCluster != cpuPercentageOnCluster ||
        job.memPercentageOnCluster != memPercentageOnCluster ||
        job.cpuMaxPercentageOnBox != cpuMaxPercentageOnBox ||
        job.memMaxPercentageOnBox != memMaxPercentageOnBox ||
        job.cpuCumulatedUsageTime != cpuCumulatedUsageTime ||
        job.memCumulatedUsageTime != memCumulatedUsageTime ||
        job.runningTime != runningTime ||
        job.stoppedTime != stoppedTime ||
        job.isRunning != isRunning) {
      return false;
    }
    return true;
  }

  @Override
  public int hashCode() {
    int hash = 7;
    hash = 37 * hash + (this.jobId != null ? this.jobId.hashCode() : 0);
    hash = 37 * hash + (int) (Double.doubleToLongBits(this.cpuPercentageOnCluster) ^ (Double.doubleToLongBits(this.cpuPercentageOnCluster) >>> 32));
    hash = 37 * hash + (int) (Double.doubleToLongBits(this.memPercentageOnCluster) ^ (Double.doubleToLongBits(this.memPercentageOnCluster) >>> 32));
    hash = 37 * hash + (int) (Double.doubleToLongBits(this.cpuMaxPercentageOnBox) ^ (Double.doubleToLongBits(this.cpuMaxPercentageOnBox) >>> 32));
    hash = 37 * hash + (int) (Double.doubleToLongBits(this.memMaxPercentageOnBox) ^ (Double.doubleToLongBits(this.memMaxPercentageOnBox) >>> 32));
    hash = 37 * hash + (int) (Double.doubleToLongBits(this.cpuCumulatedUsageTime) ^ (Double.doubleToLongBits(this.cpuMaxPercentageOnBox) >>> 32));
    hash = 37 * hash + (int) (Double.doubleToLongBits(this.memCumulatedUsageTime) ^ (Double.doubleToLongBits(this.cpuMaxPercentageOnBox) >>> 32));
    hash = 37 * hash + (int) (this.runningTime ^ (this.runningTime >>> 32));
    hash = 37 * hash + (int) (this.stoppedTime ^ (this.stoppedTime >>> 32));
    hash = 37 * hash + (this.isRunning != null ? this.isRunning.hashCode() : 0);
    return hash;
  }

  @Override
  public String toString() {
    return String.format(
            contentFormat,
            getJobId(),
            getCpuPercentageOnCluster(),
            getMemPercentageOnCluster(),
            getCpuMaxPercentageOnBox(),
            getMemMaxPercentageOnBox(),
            getMemMaxPercentageOnBoxAllTime(),
            getCpuCumulatedUsageTime() / 1000D,  // Display the time in seconds
            getMemCumulatedUsageTime() / 1000D,
            getStoppedTime() / 1000D,
            getRunningTime() / 1000D
            );
  }

  /**
   * Clear the old information from last aggregation
   */
  public void clear() {
    setCpuPercentageOnCluster(0);
    setCpuMaxPercentageOnBox(0);
    setMemMaxPercentageOnBox(0);
    setMemPercentageOnCluster(0);
  }


  //////////////////////////////////////////////////
  // Writable
  //////////////////////////////////////////////////
  @Override
  public void write(DataOutput out) throws IOException {
    Text.writeString(out,getJobId());
    out.writeDouble(getCpuMaxPercentageOnBox());
    out.writeDouble(getCpuPercentageOnCluster());
    out.writeDouble(getMemMaxPercentageOnBox());
    out.writeDouble(getMemMaxPercentageOnBoxAllTime());
    out.writeDouble(getMemPercentageOnCluster());
    out.writeDouble(getCpuCumulatedUsageTime());
    out.writeDouble(getMemCumulatedUsageTime());
    out.writeLong(getRunningTime());
    out.writeLong(getStoppedTime());
    out.writeDouble(getCpuGigaCycles());
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    setJobId(Text.readString(in));
    setCpuMaxPercentageOnBox(in.readDouble());
    setCpuPercentageOnCluster(in.readDouble());
    setMemMaxPercentageOnBox(in.readDouble());
    setMemMaxPercentageOnBoxAllTime(in.readDouble());
    setMemPercentageOnCluster(in.readDouble());
    setCpuCumulatedUsageTime(in.readDouble());
    setMemCumulatedUsageTime(in.readDouble());
    setRunningTime(in.readLong());
    setStoppedTime(in.readLong());
    setCpuGigaCycles(in.readDouble());
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
  public void setJobId(String jobID) {
    this.jobId = jobID;
  }

  /**
   * @return the cpuPercentageOnCluster
   */
  public double getCpuPercentageOnCluster() {
    return cpuPercentageOnCluster;
  }

  /**
   * @param cpuPercentageOnCluster the cpuPercentageOnCluster to set
   */
  public void setCpuPercentageOnCluster(double cpuPercentageOnCluster) {
    this.cpuPercentageOnCluster = cpuPercentageOnCluster;
  }

  /**
   * @return the memPercentageOnCluster
   */
  public double getMemPercentageOnCluster() {
    return memPercentageOnCluster;
  }

  /**
   * @param memPercentageOnCluster the memPercentageOnCluster to set
   */
  public void setMemPercentageOnCluster(double memPercentageOnCluster) {
    this.memPercentageOnCluster = memPercentageOnCluster;
  }

  /**
   * @return the cpuMaxPercentageOnBox
   */
  public double getCpuMaxPercentageOnBox() {
    return cpuMaxPercentageOnBox;
  }

  /**
   * @param cpuMaxPercentageOnBox the cpuMaxPercentageOnBox to set
   */
  public void setCpuMaxPercentageOnBox(double cpuMaxPercentageOnBox) {
    this.cpuMaxPercentageOnBox = cpuMaxPercentageOnBox;
  }

  /**
   * @return the memMaxPercentageOnBox
   */
  public double getMemMaxPercentageOnBox() {
    return memMaxPercentageOnBox;
  }

  /**
   * @param memMaxPercentageOnBox the memMaxPercentageOnBox to set
   */
  public void setMemMaxPercentageOnBox(double memMaxPercentageOnBox) {
    this.memMaxPercentageOnBox = memMaxPercentageOnBox;
  }

  /**
   * @return the isRunning
   */
  public Boolean getIsRunning() {
    return isRunning;
  }

  /**
   * @param isRunning the isRunning to set
   */
  public void setIsRunning(Boolean isRunning) {
    this.isRunning = isRunning;
  }

  /**
   * @return the cpuCumulatedUsageTime in millisecond
   */
  public double getCpuCumulatedUsageTime() {
    return cpuCumulatedUsageTime;
  }

  /**
   * @param cpuCumulatedUsageTime the cpuCumulatedUsageTime to set
   */
  public void setCpuCumulatedUsageTime(double cpuCumulatedUsageTime) {
    this.cpuCumulatedUsageTime = cpuCumulatedUsageTime;
  }

  /**
   * @return the memCumulatedUsageTime in millisecond
   */
  public double getMemCumulatedUsageTime() {
    return memCumulatedUsageTime;
  }

  /**
   * @param memCumulatedUsageTime the memCumulatedUsageTime to set
   */
  public void setMemCumulatedUsageTime(double memCumulatedUsageTime) {
    this.memCumulatedUsageTime = memCumulatedUsageTime;
  }

  /**
   * @return the runningTime
   */
  public long getRunningTime() {
    return runningTime;
  }

  /**
   * @param runningTime the runningTime to set
   */
  public void setRunningTime(long runningTime) {
    this.runningTime = runningTime;
  }

  /**
   * @return the stoppedTime
   */
  public long getStoppedTime() {
    return stoppedTime;
  }

  /**
   * @param stoppedTime the stoppedTime to set
   */
  public void setStoppedTime(long stopTime) {
    this.stoppedTime = stopTime;
  }

  /**
   * @return the peak maximum memory over time
   */
  public double getMemMaxPercentageOnBoxAllTime() {
    return memMaxPercentageOnBoxAllTime;
  }

  /**
   * Set the peak maximum memory over time
   * @param memMaxPercentageOnBoxAllTime
   */
  public void setMemMaxPercentageOnBoxAllTime(double memMaxPercentageOnBoxAllTime) {
    this.memMaxPercentageOnBoxAllTime = memMaxPercentageOnBoxAllTime;
  }

  /**
   * Set the # of CPU cycles (in Giga)
   * @param cpuCycles # of CPU cycles
   */
  public void setCpuGigaCycles(double cpuGigaCycles) {
    this.cpuGigaCycles = cpuGigaCycles;
  }

  /**
   * @return # of CPU cycles (in Giga)
   */
  public double getCpuGigaCycles() {
    return cpuGigaCycles;
  }
}