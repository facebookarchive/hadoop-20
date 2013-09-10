/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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

import org.apache.hadoop.mapred.Task.Counter;
import org.apache.hadoop.mapred.Task.TaskReporter;
import org.apache.hadoop.util.ResourceCalculatorPlugin.ProcResourceValues;

public class MapSpillSortCounters {

  private long numSpillsVal;
  private long mapSpillCPUVal;
  private long mapSpillWallClockVal;
  private long mapSpillBytesVal;
  private long mapMemSortCPUVal;
  private long mapMemSortWallClockVal;
  private long mapMergeCPUVal;
  private long mapMergeWallClockVal;
  private long mapSpillSingleRecordNum;
  
  // for new accurate metrics
  private long mapSpillJVMCPUVal;
  private long mapMemSortJVMCPUVal;
  private long mapMergeJVMCPUVal;

  private TaskReporter reporter;

  public MapSpillSortCounters(TaskReporter taskReporter) {
    this.reporter = taskReporter;
    numSpillsVal = 0;
    mapSpillCPUVal = 0;
    mapSpillWallClockVal = 0;
    mapSpillBytesVal = 0;
    mapMemSortCPUVal = 0;
    mapMemSortWallClockVal = 0;
    mapMergeCPUVal = 0;
    mapMergeWallClockVal = 0;
    mapSpillSingleRecordNum = 0;
    
    mapSpillJVMCPUVal = 0;
    mapMemSortJVMCPUVal = 0;
    mapMergeJVMCPUVal = 0;
  }
  
  public void incJVMCPUPerSpill(long spillStart, long spillEnd) {
    mapSpillJVMCPUVal = 
        mapSpillJVMCPUVal + (spillEnd - spillStart);
  }
  
  public void incJVMCPUPerSort(long sortStart, long sortEnd) {
    mapMemSortJVMCPUVal =
        mapMemSortJVMCPUVal + (sortEnd - sortStart);
  }
  
  public void incJVMCPUMerge(long mergeStart, long mergeEnd) {
    mapMergeJVMCPUVal =
        mapMergeJVMCPUVal + (mergeEnd - mergeStart);
  }
  
  public void incCountersPerSpill(ProcResourceValues spillStartProcVals,
      ProcResourceValues spillEndProcVals, long wallClockVal,
      long spillBytesVal) {
    numSpillsVal += 1;
    long cpuUsedBySpill = getCPUVal(spillStartProcVals, spillEndProcVals);
    mapSpillCPUVal += cpuUsedBySpill;
    mapSpillWallClockVal += wallClockVal;
    mapSpillBytesVal += spillBytesVal;
  }
  
  public void incCountersPerSort(ProcResourceValues sortStartProcVals,
      ProcResourceValues sortEndProcVals, long wallClockVal) {
    long cpuUsedBySort = getCPUVal(sortStartProcVals, sortEndProcVals);
    mapMemSortCPUVal += cpuUsedBySort;
    mapMemSortWallClockVal += wallClockVal;
  }
  
  public void incMergeCounters(ProcResourceValues mergeStartProcVals,
      ProcResourceValues mergeEndProcVals, long wallClockVal) {
    long cpuUsedByMerge = this
        .getCPUVal(mergeStartProcVals, mergeEndProcVals);
    mapMergeCPUVal += cpuUsedByMerge;
    this.mapMergeWallClockVal += wallClockVal;
  }
  
  public void incSpillSingleRecord() {
    mapSpillSingleRecordNum++;
  }
  
  public void finalCounterUpdate() {
    setCounterValue(Counter.MAP_SPILL_NUMBER, numSpillsVal);
    setCounterValue(Counter.MAP_SPILL_CPU, mapSpillCPUVal);
    setCounterValue(Counter.MAP_SPILL_WALLCLOCK, mapSpillWallClockVal);
    setCounterValue(Counter.MAP_SPILL_BYTES, mapSpillBytesVal);
    setCounterValue(Counter.MAP_MEM_SORT_CPU, mapMemSortCPUVal);
    setCounterValue(Counter.MAP_MEM_SORT_WALLCLOCK, mapMemSortWallClockVal);
    setCounterValue(Counter.MAP_MERGE_CPU, mapMergeCPUVal);
    setCounterValue(Counter.MAP_MERGE_WALLCLOCK, mapMergeWallClockVal);
    setCounterValue(Counter.MAP_SPILL_SINGLERECORD_NUM, mapSpillSingleRecordNum);
    
    setCounterValue(Counter.MAP_SPILL_CPU_JVM, mapSpillJVMCPUVal);
    setCounterValue(Counter.MAP_MEM_SORT_CPU_JVM, mapMemSortJVMCPUVal);
    setCounterValue(Counter.MAP_MERGE_CPU_JVM, mapMergeJVMCPUVal);
  }
  
  private void setCounterValue(Counter counter, long value) {
    Counters.Counter counterObj = reporter.getCounter(counter);
    if (counterObj != null) {
      counterObj.setValue(value);
    }
  }
  
  private long getCPUVal(ProcResourceValues startProcVals,
      ProcResourceValues endProcVals) {
    long cpuUsed = 0;
    if (startProcVals != null &&  endProcVals != null) {
      long cpuStartVal = startProcVals.getCumulativeCpuTime();
      long cpuEndVal = endProcVals.getCumulativeCpuTime();
      if (cpuEndVal > cpuStartVal) {
        cpuUsed = cpuEndVal - cpuStartVal;
      }
    }
    return cpuUsed;
  }
}
