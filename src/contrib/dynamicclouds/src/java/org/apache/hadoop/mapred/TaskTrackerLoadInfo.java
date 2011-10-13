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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.mapred.TaskStatus.Phase;
import org.apache.hadoop.mapred.TaskStatus.State;

public class TaskTrackerLoadInfo {

  private String taskTrackerHost;
  private boolean active;
  private long lastSeen;
  private int maxMapTasks;
  private int runningMapTasks;
  private int totalMapTasks;
  private int maxReduceTasks;
  private int runningReduceTasks;
  private List<TaskInfo> localTasksInfo = new ArrayList<TaskInfo>();

  public TaskTrackerLoadInfo(String taskTrackerHost) {
    this.taskTrackerHost = taskTrackerHost;
  }

  public TaskTrackerLoadInfo(String taskTrackerHost,
          boolean active,
          long lastSeen, int maxMapTasks,
          int runningMapTasks, int maxReduceTasks,
          int runningReduceTasks,
          float[] mapTasksProgress,
          float[] reduceTasksProgress,
          long[] mapsRunningTimes,
          long[] reducesRunningTimes) {

    this.taskTrackerHost = taskTrackerHost;
    this.active = active;
    this.lastSeen = lastSeen;
    this.maxMapTasks = maxMapTasks;
    this.runningMapTasks = runningMapTasks;
    this.maxReduceTasks = maxReduceTasks;
    this.runningReduceTasks = runningReduceTasks;
  }

  public String getHostName() {
    return taskTrackerHost;
  }
  
  public boolean isActive() {
    return active;
  }

  public long getLastSeen() {
    return lastSeen;
  }

  public int getMaxMapTasks() {
    return maxMapTasks;
  }

  public int getMaxReduceTasks() {
    return maxReduceTasks;
  }

  public int getTotalMapTasks() {
    return totalMapTasks;
  }

  private boolean isTaskRunning(TaskStatus.State state) {
    return (state == State.RUNNING || state == State.UNASSIGNED);
  }

  private boolean isTaskCleanup(TaskStatus.Phase phase,
          TaskStatus.State runState) {
    return (phase == TaskStatus.Phase.CLEANUP &&
            (runState == TaskStatus.State.FAILED_UNCLEAN ||
            runState == TaskStatus.State.KILLED_UNCLEAN));
  }

  public int getRunningMapTasks() {
    int running = 0;
    for (TaskInfo task : localTasksInfo) {
      if (task.isMap() &&
              isTaskRunning(task.getTaskState()) ||
              isTaskCleanup(task.getTaskPhase(), task.getTaskState())) {
        running++;
      }
    }
    return running;
  }

  public int getRunningReduceTasks() {
    int running = 0;
    for (TaskInfo task : localTasksInfo) {
      if (!task.isMap() &&
              task.getTaskState() == State.RUNNING) {
        running++;
      }
    }
    return running;
  }

  public String getTaskTrackerName() {
    return taskTrackerHost;
  }

  public String getTaskTrackerHost() {
    return taskTrackerHost;
  }

  public List<TaskInfo> getLocalTasksInfo() {
    return localTasksInfo;
  }

  public long getTotalWastedTime() {
    long total = 0;
    for (TaskInfo task : localTasksInfo) {
      if (task.isMap() ||
              (task.getTaskState() == State.RUNNING &&
              task.getTaskProgress() > 0)) {
        // The reduces that did not yet progress can be considered not started
        total += task.getRunningTime();
      }
    }
    return total;
  }

  public long getRunningTimeWasted() {
    long runningTimeWasted = 0;
    for (TaskInfo task : localTasksInfo) {
      if (task.getTaskState() == State.RUNNING &&
              (task.isMap() || task.getTaskProgress() > 0)) {
        runningTimeWasted += task.getRunningTime();
      }
    }

    // Another level of complexity here would be to count the time it would
    // take to rerun all the map tasks that were not fetched yet.
    return runningTimeWasted;
  }

  public long getEstTimeToFinish() {
    // The biggest problem introduced here is the reducers which are hard to
    // predict. The maps on the other hand usually are very fast.
    return 0;
  }

  public void parseMap(Map<String, Object> trackerInfo) {
    active = (Boolean) trackerInfo.get("active");
    lastSeen = (Long) trackerInfo.get("last_seen");
    maxMapTasks = ((Long) trackerInfo.get("map_tasks_max")).intValue();

    maxReduceTasks = ((Long) trackerInfo.get("reduce_tasks_max")).intValue();

    Object[] tasks = (Object[]) trackerInfo.get("tasks");
    for (Object task : tasks) {
      Map<String, Object> taskMap = (Map<String, Object>) task;
      int jobId = ((Long) taskMap.get("job_id")).intValue();
      int taskId = ((Long) taskMap.get("task_id")).intValue();
      int attempt = ((Long) taskMap.get("attempt")).intValue();
      boolean map = taskMap.get("type").equals("map");

      double taskProgress = (Double) taskMap.get("progress");
      long startTime = (Long) taskMap.get("start_time");
      long runningTime = (Long) taskMap.get("running_time");

      TaskStatus.State taskState =
              TaskStatus.State.valueOf(taskMap.get("state").toString());
      TaskStatus.Phase taskPhase =
              TaskStatus.Phase.valueOf(taskMap.get("phase").toString());
      TaskInfo taskInfo = new TaskInfo(jobId, taskId, attempt, map,
              startTime, runningTime, taskProgress, taskPhase, taskState);
      if (map && 
							taskState == TaskStatus.State.SUCCEEDED || 
							taskState == TaskStatus.State.RUNNING) {
				totalMapTasks++;
      }

      localTasksInfo.add(taskInfo);
    }
  }

  @Override
  public String toString() {
    return this.taskTrackerHost;
  }



  public static class TaskInfo {

    private int jobId;
    private int taskId;
    private int attempt;
    private boolean map;
    private long startTime;
    private long runningTime;
    private double taskProgress;
    TaskStatus.Phase taskPhase;
    TaskStatus.State taskState;

    public TaskInfo(int jobId, int taskId, int attempt, boolean map,
            long startTime, long runningTime, double taskProgress,
            Phase taskPhase, State taskState) {

      this.jobId = jobId;
      this.taskId = taskId;
      this.attempt = attempt;
      this.map = map;
      this.startTime = startTime;
      this.taskProgress = taskProgress;
      this.taskPhase = taskPhase;
      this.taskState = taskState;
      this.runningTime = runningTime;
    }

    public int getAttempt() {
      return attempt;
    }

    public int getJobId() {
      return jobId;
    }

    public boolean isMap() {
      return map;
    }

    public long getRunningTime() {
      return runningTime;
    }

    public int getTaskId() {
      return taskId;
    }

    public Phase getTaskPhase() {
      return taskPhase;
    }

    public double getTaskProgress() {
      return taskProgress;
    }

    public State getTaskState() {
      return taskState;
    }
  }
}
