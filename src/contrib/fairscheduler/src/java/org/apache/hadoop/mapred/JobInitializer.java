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
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

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

  JobInitializer(Configuration conf, TaskTrackerManager taskTrackerManager) {
    this.taskTrackerManager = taskTrackerManager;
    int numThreads = conf.getInt("mapred.jobinit.threads",
                                   DEFAULT_NUM_THREADS);
    this.workerThreads = new Thread[numThreads];
    for (int i = 0; i < workerThreads.length; ++i) {
      workerThreads[i] = new Thread(new Worker());
    }
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
    while (running && (waitingQueue.isEmpty() || exceedTaskLimit())) {
      this.wait();
    }
    JobInProgress job =  waitingQueue.remove(0);
    jobToRank.clear();
    for (int i = 0; i < waitingQueue.size(); ++i) {
      jobToRank.put(waitingQueue.get(i), i + 1);
    }
    return job;
  }

  synchronized int getWaitingRank(JobInProgress job) {
    Integer rank = jobToRank.get(job);
    return rank == null ? -1 : rank;
  }

  synchronized int getTotalWaiting() {
    return waitingQueue.size();
  }

  private boolean exceedTaskLimit() {
    return totalTasks > taskLimit;
  }
}
