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

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.metrics.MetricsRecord;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Job Statistics.
 */
public class JobStats implements Writable {
  private int numMapTasksLaunched;
  private int numMapTasksCompleted;
  private int numMapTasksFailed;
  private int numMapTasksFailedByFetchFailures;
  private int numMapFetchFailures;
  private int numMapTasksKilled;
  private int numReduceTasksLaunched;
  private int numReduceTasksCompleted;
  private int numReduceTasksFailed;
  private int numReduceTasksKilled;
  private int numSpeculativeMaps;
  private int numSpeculativeReduces;
  private int numSpeculativeSucceededMaps;
  private int numSpeculativeSucceededReduces;
  private int numSpeculativeWasteMaps;
  private int numSpeculativeWasteReduces;
  private int numDataLocalMaps;
  private int numRackLocalMaps;
  private long totalMapInputBytes;
  private long localMapInputBytes;
  private long rackMapInputBytes;
  private long killedMapTime;
  private long killedReduceTime;
  private long failedMapTime;
  private long failedReduceTime;
  private long speculativeMapTimeWaste;
  private long speculativeReduceTimeWaste;

  public synchronized void incNumMapTasksLaunched() {
    numMapTasksLaunched++;
  }

  public synchronized int getNumMapTasksLaunched() {
    return numMapTasksLaunched;
  }

  public synchronized void incNumMapTasksCompleted() {
    numMapTasksCompleted++;
  }

  public synchronized int getNumMapTasksCompleted() {
    return numMapTasksCompleted;
  }

  public synchronized void incNumMapTasksFailed() {
    numMapTasksFailed++;
  }

  public synchronized int getNumMapTasksFailed() {
    return numMapTasksFailed;
  }

  public synchronized void incNumMapTasksFailedByFetchFailures() {
    numMapTasksFailedByFetchFailures++;
  }

  public synchronized int getNumMapTasksFailedByFetchFailures() {
    return numMapTasksFailedByFetchFailures;
  }

  public synchronized void incNumMapFetchFailures() {
    numMapFetchFailures++;
  }

  public synchronized int getNumMapFetchFailures() {
    return numMapFetchFailures;
  }

  public synchronized void incNumMapTasksKilled() {
    numMapTasksKilled++;
  }

  public synchronized int getNumMapTasksKilled() {
    return numMapTasksKilled;
  }

  public synchronized void incNumReduceTasksLaunched() {
    numReduceTasksLaunched++;
  }

  public synchronized int getNumReduceTasksLaunched() {
    return numReduceTasksLaunched;
  }

  public synchronized void incNumReduceTasksCompleted() {
    numReduceTasksCompleted++;
  }

  public synchronized int getNumReduceTasksCompleted() {
    return numReduceTasksCompleted;
  }

  public synchronized void incNumReduceTasksFailed() {
    numReduceTasksFailed++;
  }

  public synchronized int getNumReduceTasksFailed() {
    return numReduceTasksFailed;
  }

  public synchronized void incNumReduceTasksKilled() {
    numReduceTasksKilled++;
  }

  public synchronized int getNumReduceTasksKilled() {
    return numReduceTasksKilled;
  }

  public synchronized void incNumSpeculativeMaps() {
    numSpeculativeMaps++;
  }

  public synchronized int getNumSpeculativeMaps() {
    return numSpeculativeMaps;
  }

  public synchronized void incNumSpeculativeReduces() {
    numSpeculativeReduces++;
  }

  public synchronized int getNumSpeculativeReduces() {
    return numSpeculativeReduces;
  }

  public synchronized void incNumSpeculativeSucceededMaps() {
    numSpeculativeSucceededMaps++;
  }

  public synchronized int getNumSpeculativeSucceededMaps() {
    return numSpeculativeSucceededMaps;
  }

  public synchronized void incNumSpeculativeSucceededReduces() {
    numSpeculativeSucceededReduces++;
  }

  public synchronized int getNumSpeculativeSucceededReduces() {
    return numSpeculativeSucceededReduces;
  }

  public synchronized void incNumSpeculativeWasteMaps() {
    numSpeculativeWasteMaps++;
  }

  public synchronized int getNumSpeculativeWasteMaps() {
    return numSpeculativeWasteMaps;
  }

  public synchronized void incNumSpeculativeWasteReduces() {
    numSpeculativeWasteReduces++;
  }

  public synchronized int getNumSpeculativeWasteReduces() {
    return numSpeculativeWasteReduces;
  }

  public synchronized void incNumDataLocalMaps() {
    numDataLocalMaps++;
  }

  public synchronized int getNumDataLocalMaps() {
    return numDataLocalMaps;
  }

  public synchronized void incNumRackLocalMaps() {
    numRackLocalMaps++;
  }

  public synchronized int getNumRackLocalMaps() {
    return numRackLocalMaps;
  }

  public synchronized void incTotalMapInputBytes(long v) {
    totalMapInputBytes += v;
  }

  public synchronized long getTotalMapInputBytes() {
    return totalMapInputBytes;
  }

  public synchronized void incLocalMapInputBytes(long v) {
    localMapInputBytes += v;
  }

  public synchronized long getLocalMapInputBytes() {
    return localMapInputBytes;
  }

  public synchronized void incRackMapInputBytes(long v) {
    rackMapInputBytes += v;
  }

  public synchronized long getRackMapInputBytes() {
    return rackMapInputBytes;
  }

  public synchronized void incKilledMapTime(long v) {
    killedMapTime += v;
  }

  public synchronized long getKilledMapTime() {
    return killedMapTime;
  }

  public synchronized void incKilledReduceTime(long v) {
    killedReduceTime += v;
  }

  public synchronized long getKilledReduceTime() {
    return killedReduceTime;
  }

  public synchronized void incFailedMapTime(long v) {
    failedMapTime += v;
  }

  public synchronized long getFailedMapTime() {
    return failedMapTime;
  }

  public synchronized void incFailedReduceTime(long v) {
    failedReduceTime += v;
  }

  public synchronized long getFailedReduceTime() {
    return failedReduceTime;
  }

  public synchronized void incSpeculativeMapTimeWaste(long v) {
    speculativeMapTimeWaste += v;
  }

  public synchronized long getSpeculativeMapTimeWaste() {
    return speculativeMapTimeWaste;
  }

  public synchronized void incSpeculativeReduceTimeWaste(long v) {
    speculativeReduceTimeWaste += v;
  }

  public synchronized long getSpeculativeReduceTimeWaste() {
    return speculativeReduceTimeWaste;
  }

  public synchronized void reset() {
    numMapTasksLaunched = 0;
    numMapTasksCompleted = 0;
    numMapTasksFailed = 0;
    numMapTasksFailedByFetchFailures = 0;
    numMapFetchFailures = 0;
    numMapTasksKilled = 0;
    numReduceTasksLaunched = 0;
    numReduceTasksCompleted = 0;
    numReduceTasksFailed = 0;
    numReduceTasksKilled = 0;
    numSpeculativeMaps = 0;
    numSpeculativeReduces = 0;
    numSpeculativeSucceededMaps = 0;
    numSpeculativeSucceededReduces = 0;
    numSpeculativeWasteMaps = 0;
    numSpeculativeWasteReduces = 0;
    numDataLocalMaps = 0;
    numRackLocalMaps = 0;
    totalMapInputBytes = 0;
    localMapInputBytes = 0;
    rackMapInputBytes = 0;
    killedMapTime = 0;
    killedReduceTime = 0;
    failedMapTime = 0;
    failedReduceTime = 0;
    speculativeMapTimeWaste = 0;
    speculativeReduceTimeWaste = 0;
  }

  public synchronized void accumulate(JobStats other) {
    numMapTasksLaunched += other.numMapTasksLaunched;
    numMapTasksCompleted += other.numMapTasksCompleted;
    numMapTasksFailed += other.numMapTasksFailed;
    numMapTasksFailedByFetchFailures += other.numMapTasksFailedByFetchFailures;
    numMapFetchFailures += other.numMapFetchFailures;
    numMapTasksKilled += other.numMapTasksKilled;
    numReduceTasksLaunched += other.numReduceTasksLaunched;
    numReduceTasksCompleted += other.numReduceTasksCompleted;
    numReduceTasksFailed += other.numReduceTasksFailed;
    numReduceTasksKilled += other.numReduceTasksKilled;
    numSpeculativeMaps += other.numSpeculativeMaps;
    numSpeculativeReduces += other.numSpeculativeReduces;
    numSpeculativeSucceededMaps += other.numSpeculativeSucceededMaps;
    numSpeculativeSucceededReduces += other.numSpeculativeSucceededReduces;
    numSpeculativeWasteMaps += other.numSpeculativeWasteMaps;
    numSpeculativeWasteReduces += other.numSpeculativeWasteReduces;
    numDataLocalMaps += other.numDataLocalMaps;
    numRackLocalMaps += other.numRackLocalMaps;
    totalMapInputBytes += other.totalMapInputBytes;
    localMapInputBytes += other.localMapInputBytes;
    rackMapInputBytes += other.rackMapInputBytes;
    killedMapTime += other.killedMapTime;
    killedReduceTime += other.killedReduceTime;
    failedMapTime += other.failedMapTime;
    failedReduceTime += other.failedReduceTime;
    speculativeMapTimeWaste += other.speculativeMapTimeWaste;
    speculativeReduceTimeWaste += other.speculativeReduceTimeWaste;
  }

  public synchronized void incrementMetricsAndReset(
    MetricsRecord  metricsRecord) {
    metricsRecord.incrMetric("maps_launched", getNumMapTasksLaunched());
    metricsRecord.incrMetric("maps_completed", getNumMapTasksCompleted());
    metricsRecord.incrMetric("maps_failed", getNumMapTasksFailed());
    metricsRecord.incrMetric("reduces_launched", getNumReduceTasksLaunched());
    metricsRecord.incrMetric("reduces_completed", getNumReduceTasksCompleted());
    metricsRecord.incrMetric("reduces_failed", getNumReduceTasksFailed());
    metricsRecord.incrMetric("num_speculative_maps", getNumSpeculativeMaps());
    metricsRecord.incrMetric("num_speculative_reduces", getNumSpeculativeReduces());
    metricsRecord.incrMetric("num_speculative_succeeded_maps",
      getNumSpeculativeSucceededMaps());
    metricsRecord.incrMetric("num_speculative_succeeded_reduces",
      getNumSpeculativeSucceededReduces());
    metricsRecord.incrMetric("num_speculative_wasted_maps",
      getNumSpeculativeWasteMaps());
    metricsRecord.incrMetric("num_speculative_wasted_reduces",
      getNumSpeculativeWasteReduces());
    metricsRecord.incrMetric("speculative_map_time_waste",
      getSpeculativeMapTimeWaste());
    metricsRecord.incrMetric("speculative_reduce_time_waste",
      getSpeculativeMapTimeWaste());
    metricsRecord.incrMetric("killed_tasks_map_time", getKilledMapTime());
    metricsRecord.incrMetric("killed_tasks_reduce_time", getKilledReduceTime());
    metricsRecord.incrMetric("failed_tasks_map_time", getFailedMapTime());
    metricsRecord.incrMetric("failed_tasks_reduce_time", getFailedReduceTime());
    metricsRecord.incrMetric("num_dataLocal_maps", getNumDataLocalMaps());
    metricsRecord.incrMetric("num_rackLocal_maps", getNumRackLocalMaps());
    metricsRecord.incrMetric("maps_killed", getNumMapTasksKilled());
    metricsRecord.incrMetric("reduces_killed", getNumReduceTasksKilled());
    metricsRecord.incrMetric("total_map_input_bytes", getTotalMapInputBytes());
    metricsRecord.incrMetric("local_map_input_bytes", getLocalMapInputBytes());
    metricsRecord.incrMetric("rack_map_input_bytes", getRackMapInputBytes());
    metricsRecord.incrMetric("maps_failed_by_fetch_failures",
      getNumMapTasksFailedByFetchFailures());
    metricsRecord.incrMetric("map_fetches_failed", getNumMapFetchFailures());

    reset();
  }

  @Override
  public synchronized void write(DataOutput out) throws IOException {
    out.writeInt(numMapTasksLaunched);
    out.writeInt(numMapTasksCompleted);
    out.writeInt(numMapTasksFailed);
    out.writeInt(numMapTasksFailedByFetchFailures);
    out.writeInt(numMapFetchFailures);
    out.writeInt(numMapTasksKilled);
    out.writeInt(numReduceTasksLaunched);
    out.writeInt(numReduceTasksCompleted);
    out.writeInt(numReduceTasksFailed);
    out.writeInt(numReduceTasksKilled);
    out.writeInt(numSpeculativeMaps);
    out.writeInt(numSpeculativeReduces);
    out.writeInt(numSpeculativeSucceededMaps);
    out.writeInt(numSpeculativeSucceededReduces);
    out.writeInt(numSpeculativeWasteMaps);
    out.writeInt(numSpeculativeWasteReduces);
    out.writeInt(numDataLocalMaps);
    out.writeInt(numRackLocalMaps);
    out.writeLong(totalMapInputBytes);
    out.writeLong(localMapInputBytes);
    out.writeLong(rackMapInputBytes);
    out.writeLong(killedMapTime);
    out.writeLong(killedReduceTime);
    out.writeLong(failedMapTime);
    out.writeLong(failedReduceTime);
    out.writeLong(speculativeMapTimeWaste);
    out.writeLong(speculativeReduceTimeWaste);
  }

  @Override
  public synchronized void readFields(DataInput in) throws IOException {
    numMapTasksLaunched = in.readInt();
    numMapTasksCompleted = in.readInt();
    numMapTasksFailed = in.readInt();
    numMapTasksFailedByFetchFailures = in.readInt();
    numMapFetchFailures = in.readInt();
    numMapTasksKilled = in.readInt();
    numReduceTasksLaunched = in.readInt();
    numReduceTasksCompleted = in.readInt();
    numReduceTasksFailed = in.readInt();
    numReduceTasksKilled = in.readInt();
    numSpeculativeMaps = in.readInt();
    numSpeculativeReduces = in.readInt();
    numSpeculativeSucceededMaps = in.readInt();
    numSpeculativeSucceededReduces = in.readInt();
    numSpeculativeWasteMaps = in.readInt();
    numSpeculativeWasteReduces = in.readInt();
    numDataLocalMaps = in.readInt();
    numRackLocalMaps = in.readInt();
    totalMapInputBytes = in.readLong();
    localMapInputBytes = in.readLong();
    rackMapInputBytes = in.readLong();
    killedMapTime = in.readLong();
    killedReduceTime = in.readLong();
    failedMapTime = in.readLong();
    failedReduceTime = in.readLong();
    speculativeMapTimeWaste = in.readLong();
    speculativeReduceTimeWaste = in.readLong();
  }


}
