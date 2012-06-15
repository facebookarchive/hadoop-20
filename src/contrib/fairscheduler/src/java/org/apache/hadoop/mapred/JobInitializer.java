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
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.FairSchedulerMetricsInst.AdmissionControlData;

/**
 * Used by {@link FairScheduler} to initialize jobs asynchronously
 */
class JobInitializer {

  public static final Log LOG = LogFactory.getLog(JobInitializer.class);

  private static final int DEFAULT_NUM_THREADS = 1;

  private volatile boolean running = true;
  private int taskLimit = Integer.MAX_VALUE;
  private int totalTasks = 0;

  final private List<JobInProgress> waitingQueue =
      new ArrayList<JobInProgress>();
  final private TaskTrackerManager taskTrackerManager;
  final private Thread workerThreads[];
  final private Map<JobInProgress, Integer> jobToRank =
      new HashMap<JobInProgress, Integer>();
  /** Percent of total tasks that use the soft limit [0,1.0] */
  private final double softTaskLimitPercent;
  /** Number of hard admission items in the queue */
  public static final String MAX_HARD_ADMISSION_JOB_SIZE =
      "fairscheduler.maxHardAdmissionJobSize";
  private final int maxHardAdmissionJobSize;
  /** Keep up to the last MAX_HARD_ADMISSION_JOB_SIZE entries */
  private final Queue<Long> hardAdmissionMillisQueue =
      new LinkedList<Long>();

  JobInitializer(Configuration conf, TaskTrackerManager taskTrackerManager) {
    this.taskTrackerManager = taskTrackerManager;
    int numThreads = conf.getInt("mapred.jobinit.threads",
                                   DEFAULT_NUM_THREADS);
    maxHardAdmissionJobSize = conf.getInt(MAX_HARD_ADMISSION_JOB_SIZE, 10);
    this.workerThreads = new Thread[numThreads];
    for (int i = 0; i < workerThreads.length; ++i) {
      workerThreads[i] = new Thread(new Worker());
    }

    softTaskLimitPercent =
        conf.getFloat(FairScheduler.SOFT_TASK_LIMIT_PERCENT,
                      FairScheduler.DEFAULT_SOFT_TASK_LIMIT_PERCENT);
  }

  void start() {
    for (Thread t : workerThreads) {
      t.start();
    }
  }

  void terminate() {
    running = false;
    for (Thread t : workerThreads) {
      t.interrupt();
    }
  }

  synchronized void notifyTotalTasks(int totalTasks) {
    this.totalTasks = totalTasks;
    this.notifyAll();
  }

  synchronized void notifyTaskLimit(int taskLimit) {
    this.taskLimit = taskLimit;
    this.notifyAll();
  }

  synchronized void addJob(JobInProgress job) {
    waitingQueue.add(job);
    jobToRank.put(job, waitingQueue.size());
    this.notifyAll();
  }

  private class Worker implements Runnable {
    @Override
    public void run() {
      while (running) {
        try {

          JobInProgress job = takeOneJob();

          // Note that initJob maybe slow. It cannot be in a synchronized block.
          taskTrackerManager.initJob(job);

          // There may be more than one worker finishes initJob and increases
          // the totalTasks. So the totalTasks may be exceeding taskLimit a lot
          // if there are too many Workers.
          incTotalTasks(job);

        } catch (InterruptedException e) {
          LOG.info("JobInitializer interrupted");
        }
      }
    }
  }

  synchronized private void incTotalTasks(JobInProgress job) {
    totalTasks += job.getTasks(org.apache.hadoop.mapreduce.TaskType.MAP).length;
    totalTasks += job.getTasks(org.apache.hadoop.mapreduce.TaskType.REDUCE).length;
  }

  synchronized private JobInProgress takeOneJob() throws InterruptedException {
    long startTime = -1;
    while (running && (waitingQueue.isEmpty() || exceedTaskLimit())) {
      if (!waitingQueue.isEmpty() && exceedTaskLimit() && (startTime == -1)) {
        startTime = System.currentTimeMillis();
      }
      this.wait();
    }

    // If there is a start time, the total time waiting is the time to admit
    // a job from hard admission control
    if (startTime != -1) {
      hardAdmissionMillisQueue.add(System.currentTimeMillis() - startTime);
      if (hardAdmissionMillisQueue.size() > maxHardAdmissionJobSize) {
        hardAdmissionMillisQueue.remove();
      }
    }

    JobInProgress job = waitingQueue.remove(0);
    jobToRank.clear();
    for (int i = 0; i < waitingQueue.size(); ++i) {
      jobToRank.put(waitingQueue.get(i), i + 1);
    }
    return job;
  }

  /**
   * Get the average waiting msecs per hard admission job entrance.
   *
   * @return Average waiting msecs per hard admission job entrance
   */
  synchronized float getAverageWaitMsecsPerHardAdmissionJob() {
    float averageWaitMsecsPerHardAdmissionJob = -1f;
    if (!hardAdmissionMillisQueue.isEmpty()) {
      long totalWait = 0;
      for (Long waitMillis : hardAdmissionMillisQueue) {
        totalWait += waitMillis;
      }
      averageWaitMsecsPerHardAdmissionJob =
        ((float) totalWait) / hardAdmissionMillisQueue.size();
    }
    return averageWaitMsecsPerHardAdmissionJob;
  }

  /**
   * Get the job admission wait info for a particular job.
   *
   * @param job Job to look for in the waiting queue
   * @return Admission wait info for the job
   */
  synchronized JobAdmissionWaitInfo getJobAdmissionWaitInfo(JobInProgress job) {
    Integer rank = jobToRank.get(job);
    int position = (rank == null) ? -1 : rank;
    float averageWaitMsecsPerHardAdmissionJob =
        getAverageWaitMsecsPerHardAdmissionJob();
    return new JobAdmissionWaitInfo(
        exceedTaskLimit(), position, waitingQueue.size(),
        averageWaitMsecsPerHardAdmissionJob, hardAdmissionMillisQueue.size());
  }

  /**
   * Check to see if the cluster-wide hard limit was exceeded
   *
   * @return True if the cluster-wide hard limit was exceeded, false otherwise
   */
  private boolean exceedTaskLimit() {
    return totalTasks > taskLimit;
  }

  /**
   * Get the admission control data for metrics.
   *
   * @return All the admission control data for metrics
   */
  synchronized AdmissionControlData getAdmissionControlData() {
    return new AdmissionControlData(
        totalTasks, (int) (taskLimit * softTaskLimitPercent), taskLimit);
  }
}
