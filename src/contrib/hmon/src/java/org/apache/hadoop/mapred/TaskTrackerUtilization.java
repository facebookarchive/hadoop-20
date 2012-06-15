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
 * Contains the resource utilization information of a task tracker
 */
public class TaskTrackerUtilization implements Writable {
  private String hostName;    // hostname of the TaskTracker
  private int numCpu;         // number of CPU on the TaskTracker
  private double memTotalGB;  // total amount of memory in GB
  private double cpuTotalGHz; // total CPU cycles on the TaskTracker
  private double memUsageGB;  // memory usage (GB) on the TaskTracker
  private double cpuUsageGHz; // CPU usage (GHz) on the TaskTracker

  //////////////////////////////////////////////////
  // Writable
  //////////////////////////////////////////////////
  @Override
  public void write(DataOutput out) throws IOException {
    Text.writeString(out, getHostName());
    out.writeInt(getNumCpu());
    out.writeDouble(getCpuTotalGHz());
    out.writeDouble(getMemTotalGB());
    out.writeDouble(getCpuUsageGHz());
    out.writeDouble(getMemUsageGB());
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    setHostName(Text.readString(in));
    setNumCpu(in.readInt());
    setCpuTotalGHz(in.readDouble());
    setMemTotalGB(in.readDouble());
    setCpuUsageGHz(in.readDouble());
    setMemUsageGB(in.readDouble());
  }

  public TaskTrackerUtilization() {
    this.hostName = "";
    this.numCpu = 0;
    this.memTotalGB = 0D;
    this.cpuTotalGHz = 0D;
    this.memUsageGB = 0D;
    this.cpuUsageGHz = 0D;
  }

  public TaskTrackerUtilization(String hostName,
                         int numCpu,
                         double memTotalGB,
                         double cpuTotalGHz,
                         double memUsageGB,
                         double cpuUsageGHz) {
    this.hostName = hostName;
    this.numCpu = numCpu;
    this.memTotalGB = memTotalGB;
    this.cpuTotalGHz = cpuTotalGHz;
    this.memUsageGB = memUsageGB;
    this.cpuUsageGHz = cpuUsageGHz;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == this) {
      return true;
    }
    if (obj == null || obj.getClass() != this.getClass()) {
      return false;
    }
    TaskTrackerUtilization tasktracker = (TaskTrackerUtilization)obj;
    if (!tasktracker.hostName.equals(hostName) ||
        tasktracker.numCpu != numCpu ||
        tasktracker.memTotalGB != memTotalGB ||
        tasktracker.cpuTotalGHz != cpuTotalGHz ||
        tasktracker.memUsageGB != memUsageGB ||
        tasktracker.cpuUsageGHz != cpuUsageGHz) {
      return false;
    }
    return true;
  }

  @Override
  public int hashCode() {
    int hash = 7;
    hash = 59 * hash + (this.hostName != null ? this.hostName.hashCode() : 0);
    hash = 59 * hash + this.numCpu;
    hash = 59 * hash + (int) (Double.doubleToLongBits(this.memTotalGB) ^ (Double.doubleToLongBits(this.memTotalGB) >>> 32));
    hash = 59 * hash + (int) (Double.doubleToLongBits(this.cpuTotalGHz) ^ (Double.doubleToLongBits(this.cpuTotalGHz) >>> 32));
    hash = 59 * hash + (int) (Double.doubleToLongBits(this.memUsageGB) ^ (Double.doubleToLongBits(this.memUsageGB) >>> 32));
    hash = 59 * hash + (int) (Double.doubleToLongBits(this.cpuUsageGHz) ^ (Double.doubleToLongBits(this.cpuUsageGHz) >>> 32));
    return hash;
  }

  public static final String legendFormat =
          "%-42s%-8s%-8s%-8s%-8s%-8s\n";
  public static final String contentFormat =
          "%-42s%-8s%-8.2f%-8.2f%-8.2f%-8.2f\n";
  public static final String legendString;
  public static final String unitString;
  static {
    legendString = String.format(legendFormat,
            "Host Name", "#CPU", "CPU", "MEM", "%CPU", "%MEM");
    unitString = String.format(legendFormat, "", "#", "GHz", "GB", "%", "%");
  }
  @Override
  public String toString() {
    return String.format(contentFormat,
                         getHostName(),
                         getNumCpu(),
                         getCpuTotalGHz(),
                         getMemTotalGB(),
                         getCpuUsagePercentage(),
                         getMemUsagePercentage());
  }

  /**
   * @return the memory usage % on the tasktracker
   */
  public double getMemUsagePercentage() {
    return getMemUsageGB() / getMemTotalGB() * 100;
  }

  /**
   * @return the cpu usage in % on the tasktracker
   */
  public double getCpuUsagePercentage() {
    return getCpuUsageGHz() / getCpuTotalGHz() * 100;
  }

  /**
   * @return the memTotalGB
   */
  public double getMemTotalGB() {
    return memTotalGB;
  }

  /**
   * @param memTotalGB the memTotalGB to set
   */
  public void setMemTotalGB(double memTotalGB) {
    this.memTotalGB = memTotalGB;
  }

  /**
   * @return the cpuTotalGHz
   */
  public double getCpuTotalGHz() {
    return cpuTotalGHz;
  }

  /**
   * @param cpuTotalGHz the cpuTotalGHz to set
   */
  public void setCpuTotalGHz(double cpuTotalGHz) {
    this.cpuTotalGHz = cpuTotalGHz;
  }

  /**
   * @return the numCpu
   */
  public int getNumCpu() {
    return numCpu;
  }

  /**
   * @param numCpu the numCpu to set
   */
  public void setNumCpu(int numCpu) {
    this.numCpu = numCpu;
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
  public void setMemUsageGB(double memUsageGB) {
    this.memUsageGB = memUsageGB;
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
   * @return the hostName
   */
  public String getHostName() {
    return hostName;
  }

  /**
   * @param hostName the hostName to set
   */
  public void setHostName(String hostName) {
    this.hostName = hostName;
  }
}