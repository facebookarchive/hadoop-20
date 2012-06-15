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
import org.apache.hadoop.io.Writable;

/**
 * Contains the resource utilization information of the cluster
 */
public class ClusterUtilization implements Writable {
  private int numCpu;           // total number of CPUs on the cluster
  private double cpuTotalGHz;   // total CPU cycles (GHz) on the cluster
  private double memTotalGB;    // total memory (GB) on the cluster
  private int numTaskTrackers;  // number of TaskTrackers on the cluster
  private int numRunningJobs;   // number of reported Hadoop jobs
  private double cpuUsageGHz;   // CPU usage (GHz) on the cluster
  private double memUsageGB;    // Memory usage (GB) on the cluster

  public ClusterUtilization() {
    this.numCpu = 0;
    this.cpuTotalGHz = 0D;
    this.memTotalGB = 0D;
    this.numTaskTrackers = 0;
    this.numRunningJobs = 0;
    this.cpuUsageGHz = 0D;
    this.memUsageGB = 0D;
  }

  public ClusterUtilization(int numCpu,
                     double cpuTotalGHz,
                     double memTotalGB,
                     int numTaskTrackers,
                     int numRunningJobs,
                     double cpuUsageGHz,
                     double memUsageGB) {
    this.numCpu = numCpu;
    this.cpuTotalGHz = cpuTotalGHz;
    this.memTotalGB = memTotalGB;
    this.numTaskTrackers = numTaskTrackers;
    this.numRunningJobs = numRunningJobs;
    this.cpuUsageGHz = cpuUsageGHz;
    this.memUsageGB = memUsageGB;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == this) {
      return true;
    }
    if (obj == null || obj.getClass() != this.getClass()) {
      return false;
    }
    ClusterUtilization clu = (ClusterUtilization)obj;
    if (clu.numCpu != numCpu ||
        clu.cpuTotalGHz != cpuTotalGHz ||
        clu.memTotalGB != memTotalGB ||
        clu.numTaskTrackers != numTaskTrackers ||
        clu.numRunningJobs != numRunningJobs ||
        clu.cpuUsageGHz != cpuUsageGHz ||
        clu.memUsageGB != memUsageGB ) {
      return false;
    }
    return true;
  }

  @Override
  public int hashCode() {
    int hash = 7;
    hash = 13 * hash + this.numCpu;
    hash = 13 * hash + (int) (Double.doubleToLongBits(this.cpuTotalGHz) ^ (Double.doubleToLongBits(this.cpuTotalGHz) >>> 32));
    hash = 13 * hash + (int) (Double.doubleToLongBits(this.memTotalGB) ^ (Double.doubleToLongBits(this.memTotalGB) >>> 32));
    hash = 13 * hash + this.numTaskTrackers;
    hash = 13 * hash + this.numRunningJobs;
    hash = 13 * hash + (int) (Double.doubleToLongBits(this.cpuUsageGHz) ^ (Double.doubleToLongBits(this.cpuUsageGHz) >>> 32));
    hash = 13 * hash + (int) (Double.doubleToLongBits(this.memUsageGB) ^ (Double.doubleToLongBits(this.memUsageGB) >>> 32));
    return hash;
  }

  @Override
  public String toString() {
    String result = "";
    result += String.format("Nodes: %d, #Jobs: %d, #CPU: %d, CPU GHz: %.2f\n",
            getNumTaskTrackers(), getNumRunningJobs(), getNumCpu(), getCpuTotalGHz());
    result += String.format("Mem GB: %.2f, %%CPU: %.2f, %%Mem: %.2f\n",
            getMemTotalGB(), cpuPercentage(), memPercentage());

    return result;
  }

  /**
   * Set all the status to zeros
   */
  public synchronized void clear() {
    setNumCpu(0);
    setCpuTotalGHz(0);
    setMemTotalGB(0);
    setNumTaskTrackers(0);
    setNumRunningJobs(0);
    cpuUsageGHz = 0;
    memUsageGB = 0;
  }

  //////////////////////////////////////////////////
  // Writable
  //////////////////////////////////////////////////
  @Override
  public synchronized void write(DataOutput out) throws IOException {
    out.writeDouble(getCpuTotalGHz());
    out.writeDouble(getMemTotalGB());
    out.writeInt(getNumCpu());
    out.writeInt(getNumRunningJobs());
    out.writeInt(getNumTaskTrackers());
    out.writeDouble(getMemUsageGB());
    out.writeDouble(getCpuUsageGHz());
  }

  @Override
  public synchronized void readFields(DataInput in) throws IOException {
    setCpuTotalGHz(in.readDouble());
    setMemTotalGB(in.readDouble());
    setNumCpu(in.readInt());
    setNumRunningJobs(in.readInt());
    setNumTaskTrackers(in.readInt());
    setMemUsageGB(in.readDouble());
    setCpuUsageGHz(in.readDouble());
  }

  /**
   * @return cpu usage in %
   */
  public synchronized double cpuPercentage() {
    return cpuUsageGHz / cpuTotalGHz * 100;
  }

  /**
   * @return mem usage in %
   */
  public synchronized double memPercentage() {
    return memUsageGB / memTotalGB * 100;
  }

  /**
   * @return the numCpu
   */
  public synchronized int getNumCpu() {
    return numCpu;
  }

  /**
   * @param numCpu the numCpu to set
   */
  public synchronized void setNumCpu(int numCpu) {
    this.numCpu = numCpu;
  }

  /**
   * @return the cpuTotalGHz
   */
  public synchronized double getCpuTotalGHz() {
    return cpuTotalGHz;
  }

  /**
   * @param cpuTotalGHz the cpuTotalGHz to set
   */
  public synchronized void setCpuTotalGHz(double cpuTotalGHz) {
    this.cpuTotalGHz = cpuTotalGHz;
  }

  /**
   * @return the memTotalGB
   */
  public synchronized double getMemTotalGB() {
    return memTotalGB;
  }

  /**
   * @param memTotalGB the memTotalGB to set
   */
  public synchronized void setMemTotalGB(double memTotalGB) {
    this.memTotalGB = memTotalGB;
  }

  /**
   * @return the numTaskTrackers
   */
  public synchronized int getNumTaskTrackers() {
    return numTaskTrackers;
  }

  /**
   * @param numTaskTrackers the numTaskTrackers to set
   */
  public synchronized void setNumTaskTrackers(int numTaskTrackers) {
    this.numTaskTrackers = numTaskTrackers;
  }

  /**
   * @return the numRunningJobs
   */
  public synchronized int getNumRunningJobs() {
    return numRunningJobs;
  }

  /**
   * @param numRunningJobs the numRunningJobs to set
   */
  public synchronized void setNumRunningJobs(int numRunningJobs) {
    this.numRunningJobs = numRunningJobs;
  }

  /**
   * @return the cpuUsageGHz
   */
  public synchronized double getCpuUsageGHz() {
    return cpuUsageGHz;
  }

  /**
   * @param cpuUsageGHz the cpuUsageGHz to set
   */
  public synchronized void setCpuUsageGHz(double cpuUsageGHz) {
    this.cpuUsageGHz = cpuUsageGHz;
  }

  /**
   * @return the memUsageGB
   */
  public synchronized double getMemUsageGB() {
    return memUsageGB;
  }

  /**
   * @param memUsageGB the memUsageGB to set
   */
  public synchronized void setMemUsageGB(double memUsageGB) {
    this.memUsageGB = memUsageGB;
  }
}
