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

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.TaskType;

/**
 * A {@link LoadManager} for use by the {@link FairScheduler} that allocates
 * tasks evenly across nodes up to their per-node maximum, using the default
 * load management algorithm in Hadoop.
 */
public class CapBasedLoadManager extends LoadManager {
  
  float maxDiff = 0.0f;

  public static final Log LOG =
    LogFactory.getLog(CapBasedLoadManager.class);

  // Stores the maximum slot limit set by the RPC
  Map<String, Integer> trackerNameToMaxMapSlots = 
      new HashMap<String, Integer>();
  Map<String, Integer> trackerNameToMaxReduceSlots =
      new HashMap<String, Integer>();

  // Stores the initial maximum slot limit loaded from the conf
  int defaultMaxMapSlots;
  int defaultMaxReduceSlots;

  // Stores the initial #CPU to maximum slot limit loaded from the conf
  Map<Integer, Integer> defaultCpuToMaxMapSlots = null;
  Map<Integer, Integer> defaultCpuToMaxReduceSlots = null;

  public void setConf(Configuration conf) {
    super.setConf(conf);
    maxDiff = conf.getFloat("mapred.fairscheduler.load.max.diff", 0.0f);
    defaultMaxMapSlots = conf.getInt(
        "mapred.fairscheduler.map.tasks.maximum", Integer.MAX_VALUE);
    defaultMaxReduceSlots = conf.getInt(
        "mapred.fairscheduler.reduce.tasks.maximum", Integer.MAX_VALUE);
    defaultCpuToMaxMapSlots = loadCpuToMaxSlots(TaskType.MAP);
    defaultCpuToMaxReduceSlots = loadCpuToMaxSlots(TaskType.REDUCE);
    LOG.info("Allowed load difference between TaskTrackers = " + maxDiff);
    LOG.info("Default slots per node: Maps:" + defaultMaxMapSlots +
        " Reduces:" + defaultMaxReduceSlots);
  }

  public Map<Integer, Integer> loadCpuToMaxSlots(TaskType type) {
    String config = type == TaskType.MAP ?
        conf.get("mapred.fairscheduler.cpus.to.maptasks") :
        conf.get("mapred.fairscheduler.cpus.to.reducetasks");
    Map<Integer, Integer> defaultCpuToMaxSlots =
        new HashMap<Integer, Integer>();
    if (config != null) {
      for (String s : config.replaceAll("\\s", "").split(",")) {
        String pair[] = s.split(":");
        int cpus = Integer.parseInt(pair[0]);
        int tasks = Integer.parseInt(pair[1]);
        LOG.info(String.format(
            "Number of CPUs to tasks. %s CPU : %s %s", cpus, tasks, type));
        defaultCpuToMaxSlots.put(cpus, tasks);
      }
    }
    return defaultCpuToMaxSlots;
  }

  /**
   * Determine how many tasks of a given type we want to run on a TaskTracker. 
   * This cap is chosen based on how many tasks of that type are outstanding in
   * total, so that when the cluster is used below capacity, tasks are spread
   * out uniformly across the nodes rather than being clumped up on whichever
   * machines sent out heartbeats earliest.
   */
  int getCap(int totalRunnableTasks, int localMaxTasks, int totalSlots) {
    double load = maxDiff + ((double)totalRunnableTasks) / totalSlots;
    int cap = (int) Math.min(localMaxTasks, Math.ceil(load * localMaxTasks));
    if (LOG.isDebugEnabled()) {
      LOG.debug("load:" + load + " maxDiff:" + maxDiff +
          " totalRunnable:" + totalRunnableTasks + " totalSlots:" + totalSlots +
          " localMaxTasks:" + localMaxTasks +
          " cap:" + cap);
    }
    return cap;
  }

  @Override
  public boolean canAssignMap(TaskTrackerStatus tracker,
      int totalRunnableMaps, int totalMapSlots) {
    int maxSlots = getFSMaxSlots(tracker.getTrackerName(), TaskType.MAP);
    if (LOG.isDebugEnabled()) {
      LOG.debug("fsMaxSlots:" + maxSlots +
          " ttMaxSlots:" + tracker.getMaxMapSlots() +
          " ttOccupied:" + tracker.countOccupiedMapSlots());
    }
    maxSlots = Math.min(maxSlots, tracker.getMaxMapSlots());
    return tracker.countOccupiedMapSlots() < getCap(totalRunnableMaps,
        maxSlots, totalMapSlots);
  }

  @Override
  public boolean canAssignReduce(TaskTrackerStatus tracker,
      int totalRunnableReduces, int totalReduceSlots) {
    int maxSlots = getFSMaxSlots(tracker.getTrackerName(), TaskType.REDUCE);
    if (LOG.isDebugEnabled()) {
      LOG.debug("fsMaxSlots:" + maxSlots +
          " ttMaxSlots:" + tracker.getMaxReduceSlots() +
          " ttOccupied:" + tracker.countOccupiedReduceSlots());
    }
    maxSlots = Math.min(maxSlots, tracker.getMaxReduceSlots());
    return tracker.countOccupiedReduceSlots() < getCap(totalRunnableReduces,
        maxSlots, totalReduceSlots);
  }

  @Override
  public boolean canLaunchTask(TaskTrackerStatus tracker,
      JobInProgress job,  TaskType type) {
    return true;
  }

  @Override
  public synchronized int getMaxSlots(
      TaskTrackerStatus status, TaskType type) {
    String trackerName = status.getTrackerName();
    // 1. Check if it is set by calling setFSMaxSlots
    Map<String, Integer> trackerToSlots = (type == TaskType.MAP) ?
        trackerNameToMaxMapSlots : trackerNameToMaxReduceSlots;
    Integer slots = trackerToSlots.get(trackerName);
    // 2. Check if it is set by cpuToMaxSlots in the configuration
    if (slots == null) {
      Map<Integer, Integer> defaultCpuToMaxSlots = (type == TaskType.MAP) ?
          defaultCpuToMaxMapSlots : defaultCpuToMaxReduceSlots;
      int cpus = status.getResourceStatus().getNumProcessors();
      slots = defaultCpuToMaxSlots.get(cpus);
    }
    // 3. If everything else is not set, use the default slots
    if (slots == null) {
      slots = (type == TaskType.MAP) ?
          defaultMaxMapSlots : defaultMaxReduceSlots;
    }
    int taskTrackerSlots = (type == TaskType.MAP) ? status.getMaxMapSlots() :
        status.getMaxReduceSlots();
    // 4. Return the minimum of TT slots and FS slots
    return Math.min(slots, taskTrackerSlots);
  }

  @Override
  public synchronized int getFSMaxSlots(String trackerName, TaskType type) {
    Map<String, Integer> trackerToSlots = (type == TaskType.MAP) ?
        trackerNameToMaxMapSlots : trackerNameToMaxReduceSlots;
    Integer slots = trackerToSlots.get(trackerName);
    if (slots == null) {
      return Integer.MAX_VALUE;
    }
    return slots;
  }

  @Override
  public synchronized void setFSMaxSlots(String trackerName,
      TaskType type, int slots) throws IOException {
    Map<String, Integer> trackerToSlots = (type == TaskType.MAP) ?
        trackerNameToMaxMapSlots : trackerNameToMaxReduceSlots;
    int oldSlots = getFSMaxSlots(trackerName, type);
    trackerToSlots.put(trackerName, slots);
    LOG.info("Change the " + TaskType.MAP + " of " + trackerName +
        " from " + oldSlots + " to " + slots);
  }
  
  @Override
  public synchronized void resetFSMaxSlots() throws IOException {
    // Allow changing the configuration and reloading them
    conf.reloadConfiguration();
    setConf(conf);
    trackerNameToMaxMapSlots.clear();
    trackerNameToMaxReduceSlots.clear();
    LOG.info("Reset the maximum slots to configuration");
  }
}
