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
import java.util.ArrayList;
import java.util.Collection;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;


import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapreduce.TaskType;

/**
 * Status information on the current state of the Map-Reduce cluster.
 * 
 * <p><code>ClusterStatus</code> provides clients with information such as:
 * <ol>
 *   <li>
 *   Size of the cluster. 
 *   </li>
 *   <li>
 *   Name of the trackers. 
 *   </li>
 *   <li>
 *   Task capacity of the cluster. 
 *   </li>
 *   <li>
 *   The number of currently running map & reduce tasks.
 *   </li>
 *   <li>
 *   State of the <code>JobTracker</code>.
 *   </li>
 * </ol></p>
 * 
 * <p>Clients can query for the latest <code>ClusterStatus</code>, via 
 * {@link JobClient#getClusterStatus()}.</p>
 * 
 * @see JobClient
 */
public class ClusterStatus implements Writable {

  private int numActiveTrackers;
  private Collection<String> activeTrackers = new ArrayList<String>();
  private Collection<String> blacklistedTrackers = new ArrayList<String>();
  private int numBlacklistedTrackers;
  private int numExcludedNodes;
  private long ttExpiryInterval;
  private int map_tasks;
  private int reduce_tasks;
  private int max_map_tasks;
  private int max_reduce_tasks;
  private int total_map_tasks;
  private int total_reduce_tasks;
  private JobTracker.State state;
  private long used_memory;
  private long max_memory;
  private Collection<TaskTrackerStatus> taskTrackersDetails =
         new ArrayList<TaskTrackerStatus>();
  // This is the map between task tracker name and the list of all tasks
  // that belong to currently running jobs and are/were executed
  // on this tasktracker
  private Map<String, Collection<TaskStatus>> taskTrackerExtendedTasks =
         new HashMap<String, Collection<TaskStatus>>();

  ClusterStatus() {}
  
  /**
   * Construct a new cluster status.
   * 
   * @param trackers no. of tasktrackers in the cluster
   * @param maps no. of currently running map-tasks in the cluster
   * @param reduces no. of currently running reduce-tasks in the cluster
   * @param maxMaps the maximum no. of map tasks in the cluster
   * @param maxReduces the maximum no. of reduce tasks in the cluster
   * @param state the {@link JobTracker.State} of the <code>JobTracker</code>
   * @deprecated 
   */
  @Deprecated
  ClusterStatus(int trackers, int maps, int reduces, int maxMaps,
                int maxReduces, JobTracker.State state) {
    this(trackers, 0, JobTracker.TASKTRACKER_EXPIRY_INTERVAL, maps, reduces,
        maxMaps, maxReduces, state);
  }
  
  /**
   * Construct a new cluster status.
   * 
   * @param trackers no. of tasktrackers in the cluster
   * @param blacklists no of blacklisted task trackers in the cluster
   * @param ttExpiryInterval the tasktracker expiry interval
   * @param maps no. of currently running map-tasks in the cluster
   * @param reduces no. of currently running reduce-tasks in the cluster
   * @param maxMaps the maximum no. of map tasks in the cluster
   * @param maxReduces the maximum no. of reduce tasks in the cluster
   * @param state the {@link JobTracker.State} of the <code>JobTracker</code>
   */
  ClusterStatus(int trackers, int blacklists, long ttExpiryInterval, 
                int maps, int reduces,
                int maxMaps, int maxReduces, JobTracker.State state) {
    this(trackers, blacklists, ttExpiryInterval, maps, reduces, maxMaps, 
         maxReduces, state, 0);
  }

  /**
   * @param numDecommissionedNodes number of decommission trackers
   */
  ClusterStatus(int trackers, int blacklists, long ttExpiryInterval, 
                int maps, int reduces, int maxMaps, int maxReduces, 
                JobTracker.State state, int numDecommissionedNodes) {
    numActiveTrackers = trackers;
    numBlacklistedTrackers = blacklists;
    this.numExcludedNodes = numDecommissionedNodes;
    this.ttExpiryInterval = ttExpiryInterval;
    map_tasks = maps;
    reduce_tasks = reduces;
    max_map_tasks = maxMaps;
    max_reduce_tasks = maxReduces;
    this.state = state;

    MemoryMXBean memoryMXBean = ManagementFactory.getMemoryMXBean();
    MemoryUsage status = memoryMXBean.getHeapMemoryUsage();
    used_memory = status.getUsed();
    max_memory = status.getMax();
  }

  /**
   * Construct a new cluster status.
   * 
   * @param activeTrackers active tasktrackers in the cluster
   * @param blacklistedTrackers blacklisted tasktrackers in the cluster
   * @param ttExpiryInterval the tasktracker expiry interval
   * @param maps no. of currently running map-tasks in the cluster
   * @param reduces no. of currently running reduce-tasks in the cluster
   * @param maxMaps the maximum no. of map tasks in the cluster
   * @param maxReduces the maximum no. of reduce tasks in the cluster
   * @param state the {@link JobTracker.State} of the <code>JobTracker</code>
   */
  ClusterStatus(Collection<String> activeTrackers, 
      Collection<String> blacklistedTrackers,
      long ttExpiryInterval,
      int maps, int reduces, int maxMaps, int maxReduces, 
      JobTracker.State state) {
    this(activeTrackers, blacklistedTrackers, ttExpiryInterval, maps, reduces, 
         maxMaps, maxReduces, state, 0);
  }

  /**
   * @param numDecommissionNodes number of decommission trackers
   */
  ClusterStatus(Collection<String> activeTrackers, 
                Collection<String> blacklistedTrackers, long ttExpiryInterval,
                int maps, int reduces, int maxMaps, int maxReduces, 
                JobTracker.State state, int numDecommissionNodes) {
    this(activeTrackers.size(), blacklistedTrackers.size(), ttExpiryInterval, 
        maps, reduces, maxMaps, maxReduces, state, numDecommissionNodes);
    this.activeTrackers = activeTrackers;
    this.blacklistedTrackers = blacklistedTrackers;
  }


  /**
   * Get the number of task trackers in the cluster.
   * 
   * @return the number of task trackers in the cluster.
   */
  public int getTaskTrackers() {
    return numActiveTrackers;
  }
  
  /**
   * Get the names of task trackers in the cluster.
   * 
   * @return the active task trackers in the cluster.
   */
  public Collection<String> getActiveTrackerNames() {
    return activeTrackers;
  }

  /**
   * Get the names of task trackers in the cluster.
   * 
   * @return the blacklisted task trackers in the cluster.
   */
  public Collection<String> getBlacklistedTrackerNames() {
    return blacklistedTrackers;
  }
  
  /**
   * Get the number of blacklisted task trackers in the cluster.
   * 
   * @return the number of blacklisted task trackers in the cluster.
   */
  public int getBlacklistedTrackers() {
    return numBlacklistedTrackers;
  }
  
  /**
   * Get the number of excluded hosts in the cluster.
   * @return the number of excluded hosts in the cluster.
   */
  public int getNumExcludedNodes() {
    return numExcludedNodes;
  }
  
  /**
   * Get the tasktracker expiry interval for the cluster
   * @return the expiry interval in msec
   */
  public long getTTExpiryInterval() {
    return ttExpiryInterval;
  }
  
  /**
   * Get the number of currently running map tasks in the cluster.
   * 
   * @return the number of currently running map tasks in the cluster.
   */
  public int getMapTasks() {
    return map_tasks;
  }
  
  /**
   * Get the number of currently running reduce tasks in the cluster.
   * 
   * @return the number of currently running reduce tasks in the cluster.
   */
  public int getReduceTasks() {
    return reduce_tasks;
  }


  public int getTotalMapTasks() {
    return total_map_tasks;
  }

  public int getTotalReduceTasks() {
    return total_reduce_tasks;
  }
  
  /**
   * Get the maximum capacity for running map tasks in the cluster.
   * 
   * @return the maximum capacity for running map tasks in the cluster.
   */
  public int getMaxMapTasks() {
    return max_map_tasks;
  }

  /**
   * Get the maximum capacity for running reduce tasks in the cluster.
   * 
   * @return the maximum capacity for running reduce tasks in the cluster.
   */
  public int getMaxReduceTasks() {
    return max_reduce_tasks;
  }
  
  /**
   * Get the current state of the <code>JobTracker</code>, 
   * as {@link JobTracker.State}
   * 
   * @return the current state of the <code>JobTracker</code>.
   */
  public JobTracker.State getJobTrackerState() {
    return state;
  }

  /**
   * Get the total heap memory used by the <code>JobTracker</code>
   * 
   * @return the size of heap memory used by the <code>JobTracker</code>
   */
  public long getUsedMemory() {
    return used_memory;
  }

  ClusterStatus(Collection<String> activeTrackers,
          Collection<String> blackListedTrackerInfo,
          Collection<TaskTrackerStatus> taskTrackersInfo,
          Collection<JobInProgress> runningJobs,
          long ttExpiryInterval,
          int maps, int reduces, int maxMaps, int maxReduces,
          JobTracker.State state, int numDecommissionNodes) {
    this(activeTrackers, blackListedTrackerInfo, ttExpiryInterval,
            maps, reduces, maxMaps, maxReduces, state, numDecommissionNodes);
    this.taskTrackersDetails = taskTrackersInfo;
    initTrackersToTasksMap(runningJobs);
  }


  /**
   * Get the TaskTrackerStatus for each task tracker in the cluster
   *
   * @return the collection of all task tracker statuses
   */
  public Collection<TaskTrackerStatus> getTaskTrackersDetails() {
    return taskTrackersDetails;
  }

  /**
   * Goes through the list of TaskStatus objects for each of the running jobs
   * on the cluster and associates them with the name of the task tracker
   * they are or were running on.
   */
  private void initTrackersToTasksMap(Collection<JobInProgress> jobsInProgress) {
    for (TaskTrackerStatus tracker : taskTrackersDetails) {
      taskTrackerExtendedTasks.put(tracker.getTrackerName(),
              new ArrayList<TaskStatus>());
    }
    for (JobInProgress job : jobsInProgress) {
      total_map_tasks += job.getTasks(TaskType.MAP).length;
      total_reduce_tasks += job.getTasks(TaskType.REDUCE).length;
      for (TaskInProgress task : job.getTasks(TaskType.REDUCE)) {

        TaskStatus[] taskStatuses = task.getTaskStatuses();
        for (TaskStatus status : taskStatuses) {
          Collection<TaskStatus> trackerTasks =
                  taskTrackerExtendedTasks.get(status.getTaskTracker());
          if (trackerTasks == null) {
            trackerTasks = new ArrayList<TaskStatus>();
            taskTrackerExtendedTasks.put(status.getTaskTracker(),
                    trackerTasks);
          }
          trackerTasks.add(status);
        }
      }
      for (TaskInProgress task : job.getTasks(TaskType.MAP)) {
        TaskStatus[] taskStatuses = task.getTaskStatuses();
        for (TaskStatus status : taskStatuses) {
          Collection<TaskStatus> trackerTasks =
                  taskTrackerExtendedTasks.get(status.getTaskTracker());
          if (trackerTasks == null) {
            trackerTasks = new ArrayList<TaskStatus>();
            taskTrackerExtendedTasks.put(status.getTaskTracker(),
                    trackerTasks);
          }
          trackerTasks.add(status);
        }
      }
    }
  }

  /**
   * Get the collection of TaskStatus for all tasks that are running on this
   * task tracker or have run on this task tracker and are part of the still
   * running job.
   *
   * @param ttName TaskTracker name
   * @return Collection of TaskStatus objects
   */
  public Collection<TaskStatus> getTaskTrackerTasksStatuses(String ttName) {
    return taskTrackerExtendedTasks.get(ttName);
  }

  /**
   * Get the maximum configured heap memory that can be used by the <code>JobTracker</code>
   *
   * @return the configured size of max heap memory that can be used by the <code>JobTracker</code>
   */
  public long getMaxMemory() {
    return max_memory;
  }

  public void write(DataOutput out) throws IOException {
    if (activeTrackers.size() == 0) {
      out.writeInt(numActiveTrackers);
      out.writeInt(0);
    } else {
      out.writeInt(activeTrackers.size());
      out.writeInt(activeTrackers.size());
      for (String tracker : activeTrackers) {
        Text.writeString(out, tracker);
      }
    }
    if (blacklistedTrackers.size() == 0) {
      out.writeInt(numBlacklistedTrackers);
      out.writeInt(0);
    } else {
      out.writeInt(blacklistedTrackers.size());
      out.writeInt(blacklistedTrackers.size());
      for (String tracker : blacklistedTrackers) {
        Text.writeString(out, tracker);
      }
    }
    out.writeInt(numExcludedNodes);
    out.writeLong(ttExpiryInterval);
    out.writeInt(map_tasks);
    out.writeInt(reduce_tasks);
    out.writeInt(max_map_tasks);
    out.writeInt(max_reduce_tasks);
    out.writeLong(used_memory);
    out.writeLong(max_memory);
    WritableUtils.writeEnum(out, state);
    out.writeInt(total_map_tasks);
    out.writeInt(total_reduce_tasks);
    out.writeInt(taskTrackersDetails.size());
    for (TaskTrackerStatus status : taskTrackersDetails) {
      status.write(out);
    }

    out.writeInt(taskTrackerExtendedTasks.size());
    for (Map.Entry<String, Collection<TaskStatus>> trackerTasks :
            taskTrackerExtendedTasks.entrySet()) {
      Text.writeString(out, trackerTasks.getKey());

      Collection<TaskStatus> tasks = trackerTasks.getValue();
      out.writeInt(tasks.size());

      for (TaskStatus task : tasks) {
        TaskStatus.writeTaskStatus(out, task);
      }
    }
  }

  public void readFields(DataInput in) throws IOException {
    numActiveTrackers = in.readInt();
    int numTrackerNames = in.readInt();
    if (numTrackerNames > 0) {
      for (int i = 0; i < numTrackerNames; i++) {
        String name = Text.readString(in);
        activeTrackers.add(name);
      }
    }
    numBlacklistedTrackers = in.readInt();
    numTrackerNames = in.readInt();
    if (numTrackerNames > 0) {
      for (int i = 0; i < numTrackerNames; i++) {
        String name = Text.readString(in);
        blacklistedTrackers.add(name);
      }
    }
    numExcludedNodes = in.readInt();
    ttExpiryInterval = in.readLong();
    map_tasks = in.readInt();
    reduce_tasks = in.readInt();
    max_map_tasks = in.readInt();
    max_reduce_tasks = in.readInt();
    used_memory = in.readLong();
    max_memory = in.readLong();
    state = WritableUtils.readEnum(in, JobTracker.State.class);

    total_map_tasks = in.readInt();
    total_reduce_tasks = in.readInt();
    int taskTrackers = in.readInt();
    for (int i = 0; i < taskTrackers; i++) {
      TaskTrackerStatus status = new TaskTrackerStatus();
      status.readFields(in);
      taskTrackersDetails.add(status);
    }
    int mapSize = in.readInt();
    for (int i = 0; i < mapSize; i++) {
      String trackerName = Text.readString(in);
      int numTasks = in.readInt();
      Collection<TaskStatus> tasks = new ArrayList<TaskStatus>(numTasks);
      for (int j = 0; j < numTasks; j++) {
        TaskStatus status = TaskStatus.readTaskStatus(in);
        tasks.add(status);
      }

      taskTrackerExtendedTasks.put(trackerName, tasks);
    }
  }
}
