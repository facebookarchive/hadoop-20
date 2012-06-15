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


import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.TaskType;


/**
 * {@link MemBasedLoadManager} implements {@link CapBasedLoadManager} for use by
 * the {@link FairScheduler} that checks the available memory on tasktracker to
 * determine whether to launch a task.
 * 
 * Check the memory usage information of the Jobs and TaskTrackers to
 * determine whether a task can be launched on a particular node.
 *
 * To use this class, set the following configurations
 * 1. mapred.fairscheduler.loadmanager 
 *    org.apache.hadoop.mapred.MemBasedLoadManager
 *    If this is not set up, FairScheduler will use the default LoadManager
 * 2. mapred.membasedloadmanager.reservedvmem.mb
 *    mb of the desired reserved memory (not be used by tasks) on the machine
 *    If this is not set up, the default of 2GB will be used
 */
public class MemBasedLoadManager extends CapBasedLoadManager {

  private long reservedPhysicalMemoryOnTT; // in mb
  private static final long DEFAULT_RESERVED_PHYSICAL_MEMORY_ON_TT = 2 * 1024L;
  public static final Log LOG = LogFactory.getLog(MemBasedLoadManager.class);

  public static final String RESERVED_PHYSICAL_MEMORY_ON_TT_STRING =
          "mapred.membasedloadmanager.reserved.physicalmemory.mb";

	@Override
	public void setConf(Configuration conf) {
		super.setConf(conf);
    setReservedPhysicalMemoryOnTT(conf.getLong(
            RESERVED_PHYSICAL_MEMORY_ON_TT_STRING,
            DEFAULT_RESERVED_PHYSICAL_MEMORY_ON_TT));
  }

  @Override
  public void start() {
    LOG.info("MemBasedLoadManager started");
  }

  @Override
  public boolean canAssignMap(TaskTrackerStatus tracker,
      int totalRunnableMaps, int totalMapSlots) {
    return super.canAssignMap(tracker, totalRunnableMaps, totalMapSlots) &&
           canAssign(tracker);
  }

  @Override
  public boolean canAssignReduce(TaskTrackerStatus tracker,
      int totalRunnableReduces, int totalReduceSlots) {
    return super.canAssignReduce(tracker, totalRunnableReduces, totalReduceSlots)
           && canAssign(tracker);
  }

  @Override
  public boolean canLaunchTask(TaskTrackerStatus tracker,
      JobInProgress job,  TaskType type) {
    long taskMemory = (type == TaskType.MAP ? job.getMemoryForMapTask() :
        job.getMemoryForReduceTask());
    if (taskMemory == JobConf.DISABLED_MEMORY_LIMIT) {
      return true;
    }
    taskMemory *= 1024 * 1024L; // Convert from MB to bytes
    long availablePhysicalMemory =
        tracker.getResourceStatus().getAvailablePhysicalMemory();
    long reservedPhysicalMemory =
        getReservedPhysicalMemoryOnTT() * 1024 * 1024L;

    if (availablePhysicalMemory < reservedPhysicalMemory + taskMemory) {
      String msg = String.format(
          "Cannot assign tasks from %s to %s. Not enough memory." +
          " Available:%s, Reserved:%s, Task:%s",
          job.getJobID(),
          tracker.getTrackerName(),
          availablePhysicalMemory,
          reservedPhysicalMemory,
          taskMemory);
      LOG.warn(msg);
      return false;
    }
    return true;
  }

  boolean canAssign(TaskTrackerStatus tracker) {
    long availablePhysicalMemory =
            tracker.getResourceStatus().getAvailablePhysicalMemory();
    long reservedPhysicalMemory =
            getReservedPhysicalMemoryOnTT() * 1024 * 1024L;

    if (availablePhysicalMemory < reservedPhysicalMemory) {
      String msg = String.format(
          "Cannot assign tasks to %s. Not enough memory." +
          " Available:%s, Reserved %s", tracker.getTrackerName(),
          availablePhysicalMemory, reservedPhysicalMemory);
      LOG.warn(msg);
      return false;
    }
    if (LOG.isDebugEnabled()) {
      String msg = String.format(
          "Can assign tasks to %s." +
          " Available:%s, Reserved %s", tracker.getTrackerName(),
          availablePhysicalMemory, reservedPhysicalMemory);
      LOG.debug(msg);
    }
    return true;
  }

  /**
   * Get reserved physical memory on TaskTracker in mb
   * @return reservedPhysicalMemoryOnTT
   */
  public long getReservedPhysicalMemoryOnTT() {
    return reservedPhysicalMemoryOnTT;
  }

  /**
   * Set reserved physical memory on TaskTracker in mb
   * @param reservedPhyscialMemoryOnTT the reservedPhysicalMemoryOnTT to set
   */
  public void setReservedPhysicalMemoryOnTT(long reservedPhysicalMemoryOnTT) {
    this.reservedPhysicalMemoryOnTT = reservedPhysicalMemoryOnTT;
  }
}
