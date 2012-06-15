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
import java.util.Collection;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import javax.security.auth.login.LoginException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobClient.RawSplit;
import org.apache.hadoop.mapred.TaskStatus.Phase;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.net.Node;

/** 
 * Utilities used in unit test.
 *  
 */
public class FakeObjectUtilities {

  static final Log LOG = LogFactory.getLog(FakeObjectUtilities.class);

  private static String jtIdentifier = "test";
  private static int jobCounter;
  
  /**
   * A Fake JobTracker class for use in Unit Tests
   */
  static class FakeJobTracker extends JobTracker {
    
    int totalSlots;
    private String[] trackers;

    FakeJobTracker(JobConf conf, Clock clock, String[] tts) throws IOException, 
    InterruptedException, LoginException {
      super(conf, clock);
      this.trackers = tts;
      //initialize max{Map/Reduce} task capacities to twice the clustersize
      totalSlots = trackers.length * 4;
    }
    @Override
    public ClusterStatus getClusterStatus(boolean detailed) {
      return new ClusterStatus(
          taskTrackers().size() - getBlacklistedTrackerCount(),
          getBlacklistedTrackerCount(), 0, 0, 0, totalSlots/2, totalSlots/2, 
           JobTracker.State.RUNNING, 0);
    }

    public void setNumSlots(int totalSlots) {
      this.totalSlots = totalSlots;
    }
  }

  static class FakeJobInProgress extends JobInProgress {
    @SuppressWarnings("deprecation")
    FakeJobInProgress(JobConf jobConf, JobTracker tracker) throws IOException {
      super(new JobID(jtIdentifier, ++jobCounter), jobConf, tracker);
      Path jobFile = new Path("Dummy");
      this.profile = new JobProfile(jobConf.getUser(), getJobID(), 
          jobFile.toString(), null, jobConf.getJobName(),
          jobConf.getQueueName());
    }

    @Override
    public synchronized void initTasks() throws IOException {
     
      RawSplit[] splits = createSplits();
      numMapTasks = splits.length;
      createMapTasks(splits);
      nonRunningMapCache = createCache(splits, maxLevel);
      createReduceTasks();
      tasksInited.set(true);
      this.status.setRunState(JobStatus.RUNNING);
    }

    RawSplit[] createSplits(){
      RawSplit[] splits = new RawSplit[numMapTasks];
      for (int i = 0; i < numMapTasks; i++) {
        splits[i] = new RawSplit();
        splits[i].setLocations(new String[0]);
      }
      return splits;
    }
    
    protected void createMapTasks(RawSplit[] splits) {
      maps = new TaskInProgress[numMapTasks];
      for (int i = 0; i < numMapTasks; i++) {
        maps[i] = new TaskInProgress(getJobID(), "test", 
            splits[i], getJobConf(), this, i, 1);
      }
    }

    protected void createReduceTasks() {
      reduces = new TaskInProgress[numReduceTasks];
      for (int i = 0; i < numReduceTasks; i++) {
        reduces[i] = new TaskInProgress(getJobID(), "test", 
            numMapTasks, i, 
            getJobConf(), this, 1);
        nonRunningReduces.add(reduces[i]);
      }
    }

    private TaskAttemptID findTask(String trackerName, String trackerHost,
        Collection<TaskInProgress> nonRunningTasks, 
        Collection<TaskInProgress> runningTasks, TaskType taskType)
    throws IOException {
      TaskInProgress tip = null;
      Iterator<TaskInProgress> iter = nonRunningTasks.iterator();
      long now = JobTracker.getClock().getTime();
      //look for a non-running task first
      while (iter.hasNext()) {
        TaskInProgress t = iter.next();
        if (t.isRunnable() && !t.isRunning()) {
          runningTasks.add(t);
          iter.remove();
          tip = t;
          break;
        }
      }
      if (tip == null) {
        if (getJobConf().getSpeculativeExecution()) {
          // update the progress rates of all the candidate tips ..
          for(TaskInProgress rtip: runningTasks) {
            rtip.updateProgressRate(now);
            rtip.updateProcessingRate(now);
          }

          tip = findSpeculativeTask(findSpeculativeTaskCandidates(runningTasks),
                                    trackerName, trackerHost,
                                    taskType);
        }
      }
      if (tip != null) {
        TaskAttemptID tId = tip.getTaskToRun(trackerName).getTaskID();
        if (tip.isMapTask()) {
          scheduleMap(tip);
        } else {
          scheduleReduce(tip);
        }
        //Set it to RUNNING
        makeRunning(tId, tip, trackerName, now);
        jobtracker.createTaskEntry(tId, trackerName, tip);
        return tId;
      }
      return null;
    }

    public TaskAttemptID findMapTask(String trackerName)
    throws IOException {
      return findTask(trackerName, 
          JobInProgress.convertTrackerNameToHostName(trackerName),
          nonLocalMaps, nonLocalRunningMaps, TaskType.MAP);
    }

    public TaskAttemptID findReduceTask(String trackerName) 
    throws IOException {
      return findTask(trackerName, 
          JobInProgress.convertTrackerNameToHostName(trackerName),
          nonRunningReduces, runningReduces, TaskType.REDUCE);
    }

    public void finishTask(TaskAttemptID taskId) {
      TaskInProgress tip = jobtracker.taskidToTIPMap.get(taskId);
      TaskStatus status = TaskStatus.createTaskStatus(tip.isMapTask(), taskId, 
          1.0f, 1, TaskStatus.State.SUCCEEDED, "", "", 
          tip.machineWhereTaskRan(taskId), 
          tip.isMapTask() ? Phase.MAP : Phase.REDUCE, new Counters());
      updateTaskStatus(tip, status);
    }
  
    private void makeRunning(TaskAttemptID taskId, TaskInProgress tip, 
        String taskTracker) {
      makeRunning( taskId,  tip, taskTracker, 0);
    }
    
    private void makeRunning(TaskAttemptID taskId, TaskInProgress tip, 
        String taskTracker,  long startTime) {
      Phase phase = tip.isMapTask() ? Phase.MAP : Phase.REDUCE;
      addRunningTaskToTIP(tip, taskId, new TaskTrackerStatus(taskTracker,
          JobInProgress.convertTrackerNameToHostName(taskTracker)), true);

      TaskStatus status = TaskStatus.createTaskStatus(tip.isMapTask(), taskId, 
          0.0f, 1, TaskStatus.State.RUNNING, "", "", taskTracker,
          phase, new Counters());
      status.setStartTime(startTime);
      updateTaskStatus(tip, status);
    }

    public void progressMade(TaskAttemptID taskId, float progress) {
      TaskInProgress tip = jobtracker.taskidToTIPMap.get(taskId);
      TaskStatus status = TaskStatus.createTaskStatus(tip.isMapTask(), taskId, 
          progress, 1, TaskStatus.State.RUNNING, "", "", 
          tip.machineWhereTaskRan(taskId), 
          tip.isMapTask() ? Phase.MAP : Phase.REDUCE, new Counters());
      updateTaskStatus(tip, status);
    }
    
    public void processingRate(TaskAttemptID taskId, Task.Counter counterName,
        long counterValue, float progress, Phase p) {
      TaskInProgress tip = jobtracker.taskidToTIPMap.get(taskId);
      Counters counters = tip.getCounters();
      if(tip.isMapTask()) {
        assert p == Phase.MAP : "Map task but phase is " + p;
      } else {
        assert ((p != Phase.SHUFFLE) && 
            (p != Phase.SORT) && 
            (p != Phase.REDUCE)) : "Reduce task, but phase is " + p;
      }
      TaskStatus status = TaskStatus.createTaskStatus(tip.isMapTask(), taskId,
          progress, 1, TaskStatus.State.RUNNING, "", "", 
          tip.machineWhereTaskRan(taskId), p , counters);
      //need to keep the time
      TaskStatus oldStatus = tip.getTaskStatus(taskId);
      status.setStartTime(oldStatus.getStartTime());
      if(!tip.isMapTask()) {
        status.setShuffleFinishTime(oldStatus.getShuffleFinishTime());
        status.setSortFinishTime(oldStatus.getSortFinishTime());
      }
      tip.getCounters().findCounter(counterName).setValue(counterValue);
      updateTaskStatus(tip, status);      
      LOG.info(tip.getCounters().toString());
    }
    
    public void finishCopy(TaskAttemptID taskId, long time, long bytesCopied) {

      TaskInProgress tip = jobtracker.taskidToTIPMap.get(taskId);
      assert !tip.isMapTask() : "Task ID " + taskId + " should be a reduce";
      Counters counters = tip.getCounters();  
      TaskStatus status = TaskStatus.createTaskStatus(tip.isMapTask(), taskId,
          1.0f/3.0f, 1, TaskStatus.State.RUNNING, "", "", 
          tip.machineWhereTaskRan(taskId), 
          Phase.SORT, counters);
      TaskStatus oldStatus = tip.getTaskStatus(taskId);
      status.setStartTime(oldStatus.getStartTime());
      status.setShuffleFinishTime(time);
      updateTaskStatus(tip, status);
      tip.getCounters().findCounter(Task.Counter.REDUCE_SHUFFLE_BYTES).setValue(bytesCopied);
      LOG.info(tip.getCounters().toString());
    }
    
    public void finishSort(TaskAttemptID taskId, long time) {
      TaskInProgress tip = jobtracker.taskidToTIPMap.get(taskId);
      assert !tip.isMapTask() : "Task ID " + taskId + "should be a reduce";
      if(tip.isMapTask())
        LOG.error("The task should be a reduce task");
      //need to keep the counter value
      Counters counters = tip.getCounters();  
      TaskStatus status = TaskStatus.createTaskStatus(tip.isMapTask(), taskId,
          2.0f/3.0f, 1, TaskStatus.State.RUNNING, "", "", 
          tip.machineWhereTaskRan(taskId), 
          Phase.REDUCE, counters);
      //need to keep the time
      TaskStatus oldStatus = tip.getTaskStatus(taskId);
      status.setStartTime(oldStatus.getStartTime());
      status.setSortFinishTime(finishTime);
      status.setShuffleFinishTime(oldStatus.getShuffleFinishTime());
      updateTaskStatus(tip, status);
      LOG.info(tip.getCounters().toString());
    }
    
    public void failTask(TaskAttemptID taskId) {
      TaskInProgress tip = jobtracker.taskidToTIPMap.get(taskId);
      TaskStatus status = TaskStatus.createTaskStatus(tip.isMapTask(), taskId,
          1.0f, 1, TaskStatus.State.FAILED, "", "", tip
              .machineWhereTaskRan(taskId), tip.isMapTask() ? Phase.MAP
              : Phase.REDUCE, new Counters());
      updateTaskStatus(tip, status);
      
    }

    public void killTask(TaskAttemptID taskId) {
      TaskInProgress tip = jobtracker.taskidToTIPMap.get(taskId);
      TaskStatus status = TaskStatus.createTaskStatus(tip.isMapTask(), taskId,
          1.0f, 1, TaskStatus.State.KILLED, "", "", tip
              .machineWhereTaskRan(taskId), tip.isMapTask() ? Phase.MAP
              : Phase.REDUCE, new Counters());
      updateTaskStatus(tip, status);
    }

    public void cleanUpMetrics() {
    }
    
    public void setClusterSize(int clusterSize) {
      super.setClusterSize(clusterSize);
    }
  }
  
  static short sendHeartBeat(JobTracker jt, TaskTrackerStatus status, 
		  boolean initialContact, boolean acceptNewTasks,
                                             String tracker, short responseId) 
    throws IOException {
    if (status == null) {
      status = new TaskTrackerStatus(tracker, 
          JobInProgress.convertTrackerNameToHostName(tracker));

    }
      jt.heartbeat(status, false, initialContact, acceptNewTasks, responseId);
      return ++responseId ;
  }
  
  static void establishFirstContact(JobTracker jt, String tracker) 
    throws IOException {
    sendHeartBeat(jt, null, true, false, tracker, (short) 0);
  }

  static class FakeTaskInProgress extends TaskInProgress {

    public FakeTaskInProgress(JobID jobId, String jobFile, int numMaps,
        int partition, JobTracker jobTracker, JobConf conf, JobInProgress job,
        int numSlotsRequired) {
      super(jobId, jobFile, numMaps, partition, conf, job,
          numSlotsRequired);
    }

    public FakeTaskInProgress(JobID jobId, String jobFile, RawSplit emptySplit,
        JobTracker jobTracker, JobConf jobConf,
        JobInProgress job, int partition, int numSlotsRequired) {
      super(jobId, jobFile, emptySplit, jobConf, job,
            partition, numSlotsRequired);
    }

    @Override
    synchronized boolean updateStatus(TaskStatus status) {
      TaskAttemptID taskid = status.getTaskID();
      taskStatuses.put(taskid, status);
      return false;
    }
  }
}
