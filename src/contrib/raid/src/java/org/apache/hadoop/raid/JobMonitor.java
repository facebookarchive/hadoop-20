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

package org.apache.hadoop.raid;

import java.io.IOException;
import java.text.SimpleDateFormat; 
import java.util.Collections;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.LinkedList;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapred.Counters.Group;
import org.apache.hadoop.mapreduce.CounterGroup;
import org.apache.hadoop.metrics.util.MetricsRegistry;
import org.apache.hadoop.metrics.util.MetricsTimeVaryingLong;
import org.apache.hadoop.raid.DistRaid.Counter;

/**
 * Periodically monitors the status of jobs registered with it.
 *
 * Jobs that are submitted for the same policy name are kept in the same list,
 * and the list itself is kept in a map that has the policy name as the key and
 * the list as value.
 */
public class JobMonitor implements Runnable {
  public static final Log LOG = LogFactory.getLog(
                                  "org.apache.hadoop.raid.JobMonitor");

  volatile boolean running = true;

  private Map<String, List<DistRaid>> jobs;
  private Map<String, List<DistRaid>> history;
  private Map<String, Counters> raidProgress; 
  private long jobMonitorInterval;
  private long maximumRunningTime;
  private volatile long jobsMonitored = 0;
  private volatile long jobsSucceeded = 0;
  private static final SimpleDateFormat dateForm = new SimpleDateFormat("yyyy-MM-dd");
  private static final Counter[] INT_CTRS =
    {Counter.FILES_SUCCEEDED, Counter.PROCESSED_SIZE, Counter.SAVING_SIZE};
  public enum STATUS {
    RUNNING, FINISHED, RAIDED
  }
  public static final String JOBMONITOR_INTERVAL_KEY = "raid.jobmonitor.interval";
  public static final String JOBMONITOR_MAXIMUM_RUNNINGTIME_KEY =
      "raid.jobmonitor.max.runningtime";
  public static final long DEFAULT_MAXIMUM_RUNNING_TIME = 24L * 3600L * 1000L;

  public JobMonitor(Configuration conf) {
    jobMonitorInterval = conf.getLong(JOBMONITOR_INTERVAL_KEY, 60000);
    maximumRunningTime = conf.getLong(JOBMONITOR_MAXIMUM_RUNNINGTIME_KEY,
        DEFAULT_MAXIMUM_RUNNING_TIME);
    jobs = new java.util.HashMap<String, List<DistRaid>>();
    history = new java.util.HashMap<String, List<DistRaid>>();
    raidProgress = new java.util.HashMap<String, Counters>();
  }

  public void run() {
    while (running) {
      try {
        LOG.info("JobMonitor thread continuing to run...");
        doMonitor();
      } catch (Throwable e) {
        LOG.error("JobMonitor encountered exception " +
          StringUtils.stringifyException(e));
        // All expected exceptions are caught by doMonitor(). It is better
        // to exit now, this will prevent RaidNode from submitting more jobs
        // since the number of running jobs will never decrease.
        return;
      }
    }
  }

  /**
   * Periodically checks status of running map-reduce jobs.
   */
  public void doMonitor() {
    while (running) {
      String[] keys = null;
      // Make a copy of the names of the current jobs.
      synchronized(jobs) {
        keys = jobs.keySet().toArray(new String[0]);
      }

      // Check all the jobs. We do not want to block access to `jobs`
      // because that will prevent new jobs from being added.
      // This is safe because JobMonitor.run is the only code that can
      // remove a job from `jobs`. Thus all elements in `keys` will have
      // valid values.
      Map<String, List<DistRaid>> finishedJobs =
        new HashMap<String, List<DistRaid>>();

      for (String key: keys) {
        // For each policy being monitored, get the list of jobs running.
        DistRaid[] jobListCopy = null;
        synchronized(jobs) {
          List<DistRaid> jobList = jobs.get(key);
          synchronized(jobList) {
            jobListCopy = jobList.toArray(new DistRaid[jobList.size()]);
          }
        }
        // The code that actually contacts the JobTracker is not synchronized,
        // it uses copies of the list of jobs.
        for (DistRaid job: jobListCopy) {
          // Check each running job.
          try {
            boolean complete = job.checkComplete();
            if (complete) {
              addJob(finishedJobs, key, job);
              if (job.successful()) {
                jobsSucceeded++;
              }
            } else if (System.currentTimeMillis() -
                job.getStartTime() > maximumRunningTime){
              // If the job is running for more than one day
              throw new Exception("Job " + job.getJobID() + 
                  " is hanging more than " + maximumRunningTime/1000
                  + " seconds. Kill it");
            }
          } catch (Exception e) {
            // If there was an error, consider the job finished.
            addJob(finishedJobs, key, job);
            try {
              job.killJob();
            } catch (Exception ee) {
              LOG.error(ee);
            }
          }
        }
      }

      if (finishedJobs.size() > 0) {
        for (String key: finishedJobs.keySet()) {
          List<DistRaid> finishedJobList = finishedJobs.get(key);
          // Iterate through finished jobs and remove from jobs.
          // removeJob takes care of locking.
          for (DistRaid job: finishedJobList) {
            addCounter(raidProgress, job, INT_CTRS);
            removeJob(jobs, key, job);
            addJob(history, key, job);
            // delete the temp directory 
            job.cleanUp();
          }
        }
      }

      try {
        Thread.sleep(jobMonitorInterval);
      } catch (InterruptedException ie) {
      }
    }
  }

  // For test code
  int runningJobsCount() {
    int total = 0;
    synchronized(jobs) {
      for (String key: jobs.keySet()) {
        total += jobs.get(key).size();
      }
    }
    return total;
  }

  public int runningJobsCount(String key) {
    int count = 0;
    synchronized(jobs) {
      if (jobs.containsKey(key)) {
        List<DistRaid> jobList = jobs.get(key);
        synchronized(jobList) {
          count = jobList.size();
        }
      }
    }
    return count;
  }
  
  // for test
  public List<DistRaid> getRunningJobs() {
    List<DistRaid> list = new LinkedList<DistRaid>();
    synchronized(jobs) {
      for (List<DistRaid> jobList : jobs.values()) {
        synchronized(jobList) {
          list.addAll(jobList);
        }
      }
    }
    return list;
  }
  
  // for test
  public Map<String, Counters> getRaidProgress() {
    synchronized (raidProgress) {
      return Collections.unmodifiableMap(this.raidProgress);
    }
  }

  public void monitorJob(String key, DistRaid job) {
    addJob(jobs, key, job);
    jobsMonitored++;
  }

  public long jobsMonitored() {
    return this.jobsMonitored;
  }
  
  public long jobsSucceeded() {
    return this.jobsSucceeded;
  }

  private static void addJob(Map<String, List<DistRaid>> jobsMap,
                              String jobName, DistRaid job) {
    synchronized(jobsMap) {
      List<DistRaid> list = null;
      if (jobsMap.containsKey(jobName)) {
        list = jobsMap.get(jobName);
      } else {
        list = new LinkedList<DistRaid>();
        jobsMap.put(jobName, list);
      }
      synchronized(list) {
        list.add(job);
      }
    }
  }
  
  private static void addCounter(Map<String, Counters> countersMap,
                              DistRaid job, Counter[] ctrNames) {
    Counters total_ctrs = null;
    Counters ctrs = null;
    try {
      ctrs = job.getCounters();
      if (ctrs == null) {
        LOG.warn("No counters for " + job.getJobID());
        return;
      }
    } catch (Exception e) {
      LOG.error(e);
      return;
    }
    
    //Adding to logMetrics
    Group counterGroup = ctrs.getGroup(LogUtils.LOG_COUNTER_GROUP_NAME);
    MetricsRegistry registry = RaidNodeMetrics.getInstance(
        RaidNodeMetrics.DEFAULT_NAMESPACE_ID).getMetricsRegistry();
    Map<String, MetricsTimeVaryingLong> logMetrics = RaidNodeMetrics.getInstance(
        RaidNodeMetrics.DEFAULT_NAMESPACE_ID).logMetrics;
    synchronized(logMetrics) {
      for (Counters.Counter ctr: counterGroup) {
        if (!logMetrics.containsKey(ctr.getName())) {
          logMetrics.put(ctr.getName(),
              new MetricsTimeVaryingLong(ctr.getName(), registry));
        }
        ((MetricsTimeVaryingLong)logMetrics.get(ctr.getName())).inc(ctr.getValue());
      }
    }
    
    String currDate = dateForm.format(new Date(RaidNode.now()));
    synchronized(countersMap) {
      if (countersMap.containsKey(currDate)) {
        total_ctrs = countersMap.get(currDate);
      } else {
        total_ctrs = new Counters();
        countersMap.put(currDate, total_ctrs);
      }
      for (Counter ctrName : ctrNames) {
        Counters.Counter ctr = ctrs.findCounter(ctrName);
        if (ctr != null) {
          total_ctrs.incrCounter(ctrName, ctr.getValue());
          LOG.info(ctrName + " " + ctr.getValue() + ": " +
              total_ctrs.getCounter(ctrName));
        }
      }
    }
  } 

  private static void removeJob(Map<String, List<DistRaid>> jobsMap,
                                  String jobName, DistRaid job) {
    synchronized(jobsMap) {
      if (jobsMap.containsKey(jobName)) {
        List<DistRaid> list = jobsMap.get(jobName);
        synchronized(list) {
          for (Iterator<DistRaid> it = list.iterator(); it.hasNext(); ) {
            DistRaid val = it.next();
            if (val == job) {
              it.remove();
            }
          }
          if (list.size() == 0) {
            jobsMap.remove(jobName);
          }
        }
      }
    }
  }
  
  public String toHtml(STATUS st) {
    StringBuilder sb = new StringBuilder();
    if (st == STATUS.RUNNING) {
      sb.append(DistRaid.htmlRowHeader());
      synchronized(jobs) {
        for (List<DistRaid> jobList: jobs.values()) {
          for (DistRaid job: jobList) {
            sb.append(job.toHtmlRow());
          }
        }
      }
    } else if (st == STATUS.FINISHED){
      sb.append(DistRaid.htmlRowHeader());
      synchronized(history) {
        for (List<DistRaid> jobList: history.values()) {
          for (DistRaid job: jobList) {
            sb.append(job.toHtmlRow());
          }
        }
      }
    } else if (st == STATUS.RAIDED) {
      sb.append(raidProgressRowHeader());
      synchronized(raidProgress) {
        for (String dateStr: raidProgress.keySet()) {
          sb.append(toRaidProgressHtmlRow(dateStr, 
              raidProgress.get(dateStr)));
        }
      }
    }
    return JspUtils.table(sb.toString());
  }
  
  private static String raidProgressRowHeader() {
    return JspUtils.tr(
        JspUtils.td("Date") +
        JspUtils.td("File Processed") +
        JspUtils.td("Size Processed") +
        JspUtils.td("Saved"));
  }
  
  private String toRaidProgressHtmlRow(String dateStr, Counters ctrs) {
    StringBuilder sb = new StringBuilder();
    sb.append(td(dateStr));
    sb.append(td(Long.toString(ctrs.getCounter(Counter.FILES_SUCCEEDED))));
    sb.append(td(StringUtils.humanReadableInt(ctrs.getCounter(Counter.PROCESSED_SIZE))));
    sb.append(td(StringUtils.humanReadableInt(ctrs.getCounter(Counter.SAVING_SIZE))));
    return tr(sb.toString());
  }
  
  private static String td(String s) {
    return JspUtils.td(s);
  }

  private static String tr(String s) {
    return JspUtils.tr(s);
  }
}
