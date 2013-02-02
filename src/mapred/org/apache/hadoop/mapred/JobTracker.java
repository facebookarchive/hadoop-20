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


import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.Writer;
import java.net.BindException;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Queue;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import javax.management.ObjectName;
import javax.security.auth.login.LoginException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Reconfigurable;
import org.apache.hadoop.conf.ReconfigurationServlet;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalDirAllocator;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.http.HttpServer;
import org.apache.hadoop.ipc.ProtocolSignature;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RPC.VersionMismatch;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.mapred.CleanupQueue.PathDeletionContext;
import org.apache.hadoop.mapred.JobInProgress.KillInterruptedException;
import org.apache.hadoop.mapred.JobStatusChangeEvent.EventType;
import org.apache.hadoop.mapred.TaskTrackerStatus.TaskTrackerHealthStatus;
import org.apache.hadoop.mapreduce.ClusterMetrics;
import org.apache.hadoop.mapreduce.CounterNames;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.mapreduce.server.jobtracker.TaskTracker;
import org.apache.hadoop.metrics.util.MBeanUtil;
import org.apache.hadoop.net.DNSToSwitchMapping;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.net.NetworkTopology;
import org.apache.hadoop.net.Node;
import org.apache.hadoop.net.NodeBase;
import org.apache.hadoop.net.ScriptBasedMapping;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.PermissionChecker;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UnixUserGroupInformation;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.AuthorizationException;
import org.apache.hadoop.security.authorize.ConfiguredPolicy;
import org.apache.hadoop.security.authorize.PolicyProvider;
import org.apache.hadoop.security.authorize.RefreshAuthorizationPolicyProtocol;
import org.apache.hadoop.security.authorize.ServiceAuthorizationManager;
import org.apache.hadoop.util.HostsFileReader;
import org.apache.hadoop.util.MRAsyncDiskService;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.VersionInfo;

/*******************************************************
 * JobTracker is the central location for submitting and
 * tracking MR jobs in a network environment.
 *
 *******************************************************/
public class JobTracker extends JobTrackerTraits implements MRConstants,
    InterTrackerProtocol,
    JobSubmissionProtocol, TaskTrackerManager,
    RefreshAuthorizationPolicyProtocol, AdminOperationsProtocol,
    JobHistoryObserver {

  public static final String DEFAULT_MAPRED_SYSTEM_DIR = "/tmp/hadoop/mapred/system";
  public static final String MAPRED_SYSTEM_DIR_KEY = "mapred.system.dir";
  public static final String MAPRED_SYSTEM_DIR_CLEAN_KEY = "mapred.system.dir.clean";

  static{
    Configuration.addDefaultResource("mapred-default.xml");
    Configuration.addDefaultResource("mapred-site.xml");
  }

  static long TASKTRACKER_EXPIRY_INTERVAL = 10 * 60 * 1000;
  static long RETIRE_JOB_INTERVAL;
  static long RETIRE_JOB_CHECK_INTERVAL;


  // The interval after which one fault of a tracker will be discarded,
  // if there are no faults during this.
  private long UpdateFaultyTrackerInterval;
  // The maximum percentage of trackers in cluster added
  // to the 'blacklist' across all the jobs.
  private final static double MAX_BLACKLIST_PERCENT = 0.50;
  // A tracker is blacklisted across jobs only if number of
  // blacklists are X% above the average number of blacklists.
  // X is the blacklist threshold here.
  private double AverageBlacklistThreshold;

  // Approximate number of heartbeats that could arrive JobTracker
  // in a second
  static final String JT_HEARTBEATS_IN_SECOND = "mapred.heartbeats.in.second";
  private int NUM_HEARTBEATS_IN_SECOND;
  private final int DEFAULT_NUM_HEARTBEATS_IN_SECOND = 100;
  private final int MIN_NUM_HEARTBEATS_IN_SECOND = 1;

  // Scaling factor for heartbeats, used for testing only
  static final String JT_HEARTBEATS_SCALING_FACTOR =
    "mapreduce.jobtracker.heartbeats.scaling.factor";
  private float HEARTBEATS_SCALING_FACTOR;
  private final float MIN_HEARTBEATS_SCALING_FACTOR = 0.01f;
  private final float DEFAULT_HEARTBEATS_SCALING_FACTOR = 1.0f;

  public static enum State { INITIALIZING, RUNNING }
  State state = State.INITIALIZING;
  private static final int FS_ACCESS_RETRY_PERIOD = 10000;

  private DNSToSwitchMapping dnsToSwitchMapping;
  NetworkTopology clusterMap = new NetworkTopology();
  private int numTaskCacheLevels; // the max level to which we cache tasks

  static Clock clock = null;

  static final Clock DEFAULT_CLOCK = new Clock();
  /**
   * {@link #nodesAtMaxLevel} is using the keySet from {@link ConcurrentHashMap}
   * so that it can be safely written to and iterated on via 2 separate threads.
   * Note: It can only be iterated from a single thread which is feasible since
   *       the only iteration is done in {@link JobInProgress} under the
   *       {@link JobTracker} lock.
   */
  private Set<Node> nodesAtMaxLevel =
    Collections.newSetFromMap(new ConcurrentHashMap<Node, Boolean>());
  final TaskScheduler taskScheduler;
  private final ResourceReporter resourceReporter;
  private final List<JobInProgressListener> jobInProgressListeners =
    new CopyOnWriteArrayList<JobInProgressListener>();

  private static final LocalDirAllocator lDirAlloc =
                              new LocalDirAllocator("mapred.local.dir");
  // system directories are world-wide readable and owner readable
  final static FsPermission SYSTEM_DIR_PERMISSION =
    FsPermission.createImmutable((short) 0733); // rwx-wx-wx

  // system files should have 700 permission
  final static FsPermission SYSTEM_FILE_PERMISSION =
    FsPermission.createImmutable((short) 0700); // rwx------

  private MRAsyncDiskService asyncDiskService;

  // set before starting JobTracker shutdown
  private volatile boolean shutdown = false;

  /**
   * The maximum no. of 'completed' (successful/failed/killed)
   * jobs kept in memory per-user.
   */
  final int MAX_COMPLETE_USER_JOBS_IN_MEMORY;

   /**
    * The minimum time (in ms) that a job's information has to remain
    * in the JobTracker's memory before it is retired.
    */
  static int MIN_TIME_BEFORE_RETIRE = 0;

  /**
   * If this is set, then the next iteration of RetireJob thread will
   * retire most completed jobs
   */
  static volatile boolean RETIRE_COMPLETED_JOBS = false;


  private final AtomicInteger nextJobId = new AtomicInteger(1);

  public static final Log LOG = LogFactory.getLog(JobTracker.class);

  private final TaskErrorCollector taskErrorCollector;

  /**
   * Returns JobTracker's clock. Note that the correct clock implementation will
   * be obtained only when the JobTracker is initialized. If the JobTracker is
   * not initialized then the default clock i.e {@link Clock} is returned.
   */
  static Clock getClock() {
    return clock == null ? DEFAULT_CLOCK : clock;
  }

  /**
   * Start the JobTracker with given configuration.
   *
   * The conf will be modified to reflect the actual ports on which
   * the JobTracker is up and running if the user passes the port as
   * <code>zero</code>.
   *
   * @param conf configuration for the JobTracker.
   * @throws IOException
   */
  public static JobTracker startTracker(JobConf conf
                                        ) throws IOException,
                                                 InterruptedException {
    return startTracker(conf, generateNewIdentifier());
  }

  public static JobTracker startTracker(JobConf conf, String identifier)
  throws IOException, InterruptedException {
    JobTracker result = null;
    while (true) {
      try {
        result = new JobTracker(conf, identifier);
        result.taskScheduler.setTaskTrackerManager(result);
        break;
      } catch (VersionMismatch e) {
        throw e;
      } catch (BindException e) {
        throw e;
      } catch (UnknownHostException e) {
        throw e;
      } catch (AccessControlException ace) {
        // in case of jobtracker not having right access
        // bail out
        throw ace;
      } catch (IOException e) {
        LOG.warn("Error starting tracker: " +
                 StringUtils.stringifyException(e));
      }
      Thread.sleep(1000);
    }
    if (result != null) {
      JobEndNotifier.startNotifier();
    }
    return result;
  }

  public void stopTracker() throws IOException {
    JobEndNotifier.stopNotifier();
    close();
  }

  public long getProtocolVersion(String protocol,
                                 long clientVersion) throws IOException {
    if (protocol.equals(InterTrackerProtocol.class.getName())) {
      return InterTrackerProtocol.versionID;
    } else if (protocol.equals(JobSubmissionProtocol.class.getName())){
      return JobSubmissionProtocol.versionID;
    } else if (protocol.equals(RefreshAuthorizationPolicyProtocol.class.getName())){
      return RefreshAuthorizationPolicyProtocol.versionID;
    } else if (protocol.equals(AdminOperationsProtocol.class.getName())){
      return AdminOperationsProtocol.versionID;
    } else {
      throw new IOException("Unknown protocol to job tracker: " + protocol);
    }
  }

  public ProtocolSignature getProtocolSignature(String protocol,
      long clientVersion, int clientMethodsHash) throws IOException {
    return ProtocolSignature.getProtocolSignature(
        this, protocol, clientVersion, clientMethodsHash);
  }

  /**
   * A thread to update information for all Jobs
   */
  private class JobUpdater implements Runnable {

    public void run() {
      long iter = 0;
      while (!shutdown) {
        iter++;
        if (iter % 60 == 0) {
          // Every 60 iterations.
          CounterNames.clearIfNecessary(
              jobTrackerReconfigurable.getMaxUniqueCounterNames());
        }
        try {
          // Check every second if jobs speculation stats need update
          Thread.sleep(1000L);
          List<JobInProgress> cachedJobs = new ArrayList<JobInProgress> ();

          long startTime = JobTracker.getClock().getTime();

          // get a snapshot of all the jobs in the system
          synchronized (jobs) {
            cachedJobs.addAll(jobs.values());
          }

          for (JobInProgress job: cachedJobs) {
            job.refreshIfNecessary();
          }

          long runTime = JobTracker.getClock().getTime() - startTime;
          LOG.info("JobUpdater updated. runtime:" + runTime);
        } catch (InterruptedException ie) {
          // ignore. if shutting down, while cond. will catch it
        } catch (Exception e) {
          LOG.error("JobUpdater Thread got exception: " +
                    StringUtils.stringifyException(e));
        }
      }
    }
  }

  /**
   * A thread to timeout tasks that have been assigned to task trackers,
   * but that haven't reported back yet.
   * Note that I included a stop() method, even though there is no place
   * where JobTrackers are cleaned up.
   */
  private class ExpireLaunchingTasks implements Runnable {
    /**
     * This is a map of the tasks that have been assigned to task trackers,
     * but that have not yet been seen in a status report.
     * map: task-id -> time-assigned
     */
    private Map<TaskAttemptID, Long> launchingTasks =
      new LinkedHashMap<TaskAttemptID, Long>();

    public void run() {
      while (!shutdown) {
        try {
          // Every 3 minutes check for any tasks that are overdue
          Thread.sleep(TASKTRACKER_EXPIRY_INTERVAL/3);
          long now = getClock().getTime();
          LOG.debug("Starting launching task sweep");
          synchronized (JobTracker.this) {
            synchronized (launchingTasks) {
              Iterator<Map.Entry<TaskAttemptID, Long>> itr =
                launchingTasks.entrySet().iterator();
              while (itr.hasNext()) {
                Map.Entry<TaskAttemptID, Long> pair = itr.next();
                TaskAttemptID taskId = pair.getKey();
                long age = now - (pair.getValue()).longValue();
                LOG.info(taskId + " is " + age + " ms debug.");
                if (age > TASKTRACKER_EXPIRY_INTERVAL) {
                  LOG.info("Launching task " + taskId + " timed out.");
                  TaskInProgress tip = null;
                  tip = taskidToTIPMap.get(taskId);
                  if (tip != null) {
                    JobInProgress job = (JobInProgress) tip.getJob();
                    String trackerName = getAssignedTracker(taskId);
                    TaskTrackerStatus trackerStatus =
                      getTaskTrackerStatus(trackerName);

                    // This might happen when the tasktracker has already
                    // expired and this thread tries to call failedtask
                    // again. expire tasktracker should have called failed
                    // task!
                    if (trackerStatus != null)
                      job.failedTask(tip, taskId, "Error launching task",
                                     tip.isMapTask()? TaskStatus.Phase.MAP:
                                     TaskStatus.Phase.STARTING,
                                     TaskStatus.State.FAILED,
                                     trackerName);
                  }
                  itr.remove();
                } else {
                  // the tasks are sorted by start time, so once we find
                  // one that we want to keep, we are done for this cycle.
                  break;
                }
              }
            }
          }
        } catch (InterruptedException ie) {
          // ignore. if shutting down, while cond. will catch it
        } catch (Exception e) {
          LOG.error("Expire Launching Task Thread got exception: " +
                    StringUtils.stringifyException(e));
        }
      }
    }

    public void addNewTask(TaskAttemptID taskName) {
      synchronized (launchingTasks) {
        launchingTasks.put(taskName,
                           getClock().getTime());
      }
    }

    public void removeTask(TaskAttemptID taskName) {
      synchronized (launchingTasks) {
        launchingTasks.remove(taskName);
      }
    }
  }

  ///////////////////////////////////////////////////////
  // Used to expire TaskTrackers that have gone down
  ///////////////////////////////////////////////////////
  class ExpireTrackers implements Runnable {

    public ExpireTrackers() {
    }
    /**
     * The run method lives for the life of the JobTracker, and removes TaskTrackers
     * that have not checked in for some time.
     */
    public void run() {
      while (!shutdown) {
        try {
          //
          // Thread runs periodically to check whether trackers should be expired.
          // The sleep interval must be no more than half the maximum expiry time
          // for a task tracker.
          //
          Thread.sleep(TASKTRACKER_EXPIRY_INTERVAL / 3);

          //
          // Loop through all expired items in the queue
          //
          // Need to lock the JobTracker here since we are
          // manipulating it's data-structures via
          // ExpireTrackers.run -> JobTracker.lostTaskTracker ->
          // JobInProgress.failedTask -> JobTracker.markCompleteTaskAttempt
          // Also need to lock JobTracker before locking 'taskTracker' &
          // 'trackerExpiryQueue' to prevent deadlock:
          // @see {@link JobTracker.processHeartbeat(TaskTrackerStatus, boolean)}
          synchronized (JobTracker.this) {
            synchronized (taskTrackers) {
              synchronized (trackerExpiryQueue) {
                long now = getClock().getTime();
                TaskTrackerStatus leastRecent = null;
                while ((trackerExpiryQueue.size() > 0) &&
                       (leastRecent = trackerExpiryQueue.first()) != null &&
                       ((now - leastRecent.getLastSeen()) > TASKTRACKER_EXPIRY_INTERVAL)) {


                  // Remove profile from head of queue
                  trackerExpiryQueue.remove(leastRecent);
                  String trackerName = leastRecent.getTrackerName();

                  // Figure out if last-seen time should be updated, or if tracker is dead
                  TaskTracker current = getTaskTracker(trackerName);
                  TaskTrackerStatus newProfile =
                    (current == null ) ? null : current.getStatus();
                  // Items might leave the taskTracker set through other means; the
                  // status stored in 'taskTrackers' might be null, which means the
                  // tracker has already been destroyed.
                  if (newProfile != null) {
                    if ((now - newProfile.getLastSeen()) > TASKTRACKER_EXPIRY_INTERVAL) {
                      removeTracker(current);
                      // remove the mapping from the hosts list
                      String hostname = newProfile.getHost();
                      hostnameToTaskTracker.get(hostname).remove(trackerName);
                    } else {
                      // Update time by inserting latest profile
                      trackerExpiryQueue.add(newProfile);
                    }
                  }
                }
              }
            }
          }
        } catch (InterruptedException iex) {
          // ignore. if shutting down, while cond. will catch it
        } catch (Exception t) {
          LOG.error("Tracker Expiry Thread got exception: " +
                    StringUtils.stringifyException(t));
        }
      }
    }

  }

  @Override
  public synchronized void historyFileCopied(JobID jobid, String historyFile) {
    JobInProgress job = getJob(jobid);
    if (job != null) { //found in main cache
      job.setHistoryFileCopied();
      if (historyFile != null) {
        job.setHistoryFile(historyFile);
      }
      return;
    }
    RetireJobInfo jobInfo = retireJobs.get(jobid);
    if (jobInfo != null) { //found in retired cache
      if (historyFile != null) {
        jobInfo.setHistoryFile(historyFile);
      }
    }
  }

  /**
   * An Attempt and it's corresponding TaskInProgress
   * There is a unique TIP per Attempt. Hence the attempt
   * can be used as the unique key to identify this tuple
   * (in a Collection for example)
   */
  public static final class TaskAttemptIDWithTip
    implements Comparable<TaskAttemptIDWithTip> {
    public final TaskAttemptID attemptId;
    public final TaskInProgress tip;

    public TaskAttemptIDWithTip(TaskAttemptID attemptId, TaskInProgress tip) {
      this.attemptId = attemptId;
      this.tip = tip;
    }

    public boolean equals(Object o) {
      TaskAttemptIDWithTip that = (TaskAttemptIDWithTip)o;
      return this.attemptId.equals(that.attemptId);
    }

    public int hashCode() {
      return attemptId.hashCode();
    }

    public int compareTo(TaskAttemptIDWithTip that) {
      return this.attemptId.compareTo(that.attemptId);
    }
  }

  static class RetireJobInfo {
    final JobStatus status;
    final JobProfile profile;
    final long finishTime;
    private String historyFile;
    RetireJobInfo(JobStatus status, JobProfile profile, long finishTime,
        String historyFile) {
      this.status = status;
      this.profile = profile;
      this.finishTime = finishTime;
      this.historyFile = historyFile;
    }
    void setHistoryFile(String file) {
      this.historyFile = file;
    }
    String getHistoryFile() {
      return historyFile;
    }
  }
  ///////////////////////////////////////////////////////
  // Used to remove old finished Jobs that have been around for too long
  ///////////////////////////////////////////////////////
  class RetireJobs implements Runnable {
    private final Map<JobID, RetireJobInfo> jobIDStatusMap =
      new HashMap<JobID, RetireJobInfo>();
    private final LinkedList<RetireJobInfo> jobRetireInfoQ =
      new LinkedList<RetireJobInfo>();

    public RetireJobs() {
    }

    synchronized void addToCache(JobInProgress job) {
      RetireJobInfo info = new RetireJobInfo(job.getStatus(),
          job.getProfile(), job.getFinishTime(), job.getHistoryFile());
      jobRetireInfoQ.add(info);
      jobIDStatusMap.put(info.status.getJobID(), info);
      if (jobRetireInfoQ.size() > retiredJobsCacheSize) {
        RetireJobInfo removed = jobRetireInfoQ.remove();
        jobIDStatusMap.remove(removed.status.getJobID());
        LOG.info("Retired job removed from cache " + removed.status.getJobID());
      }
    }

    synchronized RetireJobInfo get(JobID jobId) {
      return jobIDStatusMap.get(jobId);
    }

    @SuppressWarnings("unchecked")
    synchronized LinkedList<RetireJobInfo> getAll() {
      return (LinkedList<RetireJobInfo>) jobRetireInfoQ.clone();
    }

    synchronized LinkedList<JobStatus> getAllJobStatus() {
      LinkedList<JobStatus> list = new LinkedList<JobStatus>();
      for (RetireJobInfo info : jobRetireInfoQ) {
        list.add(info.status);
      }
      return list;
    }

    private boolean minConditionToRetire(JobInProgress job, long now) {
      return job.getStatus().getRunState() != JobStatus.RUNNING &&
          job.getStatus().getRunState() != JobStatus.PREP &&
          (job.getFinishTime() + MIN_TIME_BEFORE_RETIRE < now) &&
          (job.isHistoryFileCopied() || JobHistory.isDisableHistory());
    }
    /**
     * The run method lives for the life of the JobTracker,
     * and removes Jobs that are not still running, but which
     * finished a long time ago.
     */
    public void run() {
      while (!shutdown) {
        try {
          Thread.sleep(RETIRE_JOB_CHECK_INTERVAL);
          List<JobInProgress> retiredJobs = new ArrayList<JobInProgress>();
          long now = getClock().getTime();
          long retireBefore = now - RETIRE_JOB_INTERVAL;

          synchronized (jobs) {
            for(JobInProgress job: jobs.values()) {
              if (minConditionToRetire(job, now) &&
                  (RETIRE_COMPLETED_JOBS || (job.getFinishTime()  < retireBefore))) {
                retiredJobs.add(job);
              }
            }
            RETIRE_COMPLETED_JOBS = false; // all completed jobs are now almost retired.
          }
          synchronized (userToJobsMap) {
            Iterator<Map.Entry<String, ArrayList<JobInProgress>>>
                userToJobsMapIt = userToJobsMap.entrySet().iterator();
            while (userToJobsMapIt.hasNext()) {
              Map.Entry<String, ArrayList<JobInProgress>> entry =
                userToJobsMapIt.next();
              ArrayList<JobInProgress> userJobs = entry.getValue();
              Iterator<JobInProgress> it = userJobs.iterator();
              while (it.hasNext() &&
                  userJobs.size() > MAX_COMPLETE_USER_JOBS_IN_MEMORY) {
                JobInProgress jobUser = it.next();
                if (retiredJobs.contains(jobUser)) {
                  LOG.info("Removing from userToJobsMap: " +
                      jobUser.getJobID());
                  it.remove();
                } else if (minConditionToRetire(jobUser, now)) {
                  LOG.info("User limit exceeded. Marking job: " +
                      jobUser.getJobID() + " for retire.");
                  retiredJobs.add(jobUser);
                  it.remove();
                }
              }
              if (userJobs.isEmpty()) {
                userToJobsMapIt.remove();
              }
            }
          }
          if (!retiredJobs.isEmpty()) {
            List<JobID> toBeDeleted = new ArrayList<JobID>();
            synchronized (JobTracker.this) {
              synchronized (jobs) {
                synchronized (taskScheduler) {
                  for (JobInProgress job: retiredJobs) {
                    removeJobTasks(job);
                    jobs.remove(job.getProfile().getJobID());
                    for (JobInProgressListener l : jobInProgressListeners) {
                      l.jobRemoved(job);
                    }
                    String jobUser = job.getProfile().getUser();
                    LOG.info("Retired job with id: '" +
                             job.getProfile().getJobID() + "' of user '" +
                             jobUser + "'");
                    toBeDeleted.add(job.getProfile().getJobID());
                    addToCache(job);
                  }
                }
              }
            }
            for (JobID id : toBeDeleted) {
              // clean up job files from the local disk
              JobHistory.JobInfo.cleanupJob(id);
            }
          }
        } catch (InterruptedException t) {
          // ignore. if shutting down, while cond. will catch it
        } catch (Throwable t) {
          LOG.error("Error in retiring job:\n" +
                    StringUtils.stringifyException(t));
        }
      }
    }
  }

  enum ReasonForBlackListing {
    EXCEEDING_FAILURES,
    NODE_UNHEALTHY
  }

  // The FaultInfo which indicates the number of faults of a tracker
  // and when the last fault occurred
  // and whether the tracker is blacklisted across all jobs or not
  protected static class FaultInfo {
    static final String FAULT_FORMAT_STRING =  "%d failures on the tracker";
    private Queue<JobFault> jobFaults;
    long lastUpdated;
    boolean blacklisted;

    private boolean isHealthy;

    private HashMap<ReasonForBlackListing, String>rfbMap;

    FaultInfo() {
      jobFaults = new LinkedList<JobFault>();
      lastUpdated = getClock().getTime();
      blacklisted = false;
      rfbMap = new  HashMap<ReasonForBlackListing, String>();
    }

    void addFault(JobFault jf) {
      jobFaults.add(jf);
    }

    JobFault forgiveOneFault() {
      return jobFaults.poll();
    }

    JobFault[] getFaults() {
      return jobFaults.toArray(new JobFault[0]);
    }

    void setLastUpdated(long timeStamp) {
      lastUpdated = timeStamp;
    }

    int getFaultCount() {
      return jobFaults.size();
    }

    long getLastUpdated() {
      return lastUpdated;
    }

    boolean isBlacklisted() {
      return blacklisted;
    }

    void setBlacklist(ReasonForBlackListing rfb,
        String trackerFaultReport) {
      blacklisted = true;
      this.rfbMap.put(rfb, trackerFaultReport);
    }

    public void setHealthy(boolean isHealthy) {
      this.isHealthy = isHealthy;
    }

    public boolean isHealthy() {
      return isHealthy;
    }

    public String getTrackerFaultReport() {
      StringBuffer sb = new StringBuffer();
      for(String reasons : rfbMap.values()) {
        sb.append(reasons);
        sb.append("\n");
      }
      return sb.toString();
    }

    Set<ReasonForBlackListing> getReasonforblacklisting() {
      return this.rfbMap.keySet();
    }

    public void unBlacklist() {
      this.blacklisted = false;
      this.rfbMap.clear();
    }

    public boolean removeBlackListedReason(ReasonForBlackListing rfb) {
      String str = rfbMap.remove(rfb);
      return str!=null;
    }

    public void addBlackListedReason(ReasonForBlackListing rfb, String reason) {
      this.rfbMap.put(rfb, reason);
    }

  }

  public static class JobFault {
    final String tasktracker;
    final String job;
    final String[] taskExceptions;

    JobFault (String tt, String job, String[] exs) {
      this.tasktracker = tt;
      this.job = job;
      this.taskExceptions = exs.clone();
    }
  }

  protected class FaultyTrackersInfo {
    // A map from hostName to its faults
    private Map<String, FaultInfo> potentiallyFaultyTrackers =
              new HashMap<String, FaultInfo>();
    // This count gives the number of blacklisted trackers in the cluster
    // at any time. This is maintained to avoid iteration over
    // the potentiallyFaultyTrackers to get blacklisted trackers. And also
    // this count doesn't include blacklisted trackers which are lost,
    // although the fault info is maintained for lost trackers.
    private volatile int numBlacklistedTrackers = 0;

    /**
     * Increments faults(blacklist by job) for the tracker by one.
     *
     * Adds the tracker to the potentially faulty list.
     * Assumes JobTracker is locked on the entry.
     *
     * @param hostName
     */
    void incrementFaults(String hostName, JobFault jf) {
      synchronized (potentiallyFaultyTrackers) {
        FaultInfo fi = getFaultInfo(hostName, true);
        fi.addFault(jf);
        fi.setLastUpdated(getClock().getTime());
        if (exceedsFaults(fi)) {
          LOG.info("Adding " + hostName + " to the blacklist"
              + " across all jobs");
          String reason = String.format(FaultInfo.FAULT_FORMAT_STRING,
              fi.getFaultCount());
          blackListTracker(hostName, reason,
              ReasonForBlackListing.EXCEEDING_FAILURES);
        }
      }
    }

    private void incrBlackListedTrackers(int count) {
      numBlacklistedTrackers += count;
      getInstrumentation().addBlackListedTrackers(count);
    }

    private void decrBlackListedTrackers(int count) {
      numBlacklistedTrackers -= count;
      getInstrumentation().decBlackListedTrackers(count);
    }

    private void blackListTracker(String hostName, String reason, ReasonForBlackListing rfb) {
      FaultInfo fi = getFaultInfo(hostName, true);
      boolean blackListed = fi.isBlacklisted();
      if(blackListed) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Adding blacklisted reason for tracker : " + hostName
              + " Reason for blacklisting is : " + rfb);
        }
        if (!fi.getReasonforblacklisting().contains(rfb)) {
        LOG.info("Adding blacklisted reason for tracker : " + hostName
            + " Reason for blacklisting is : " + rfb);
        }
        fi.addBlackListedReason(rfb, reason);
      } else {
        LOG.info("Blacklisting tracker : " + hostName
            + " Reason for blacklisting is : " + rfb);
        Set<TaskTracker> trackers =
          hostnameToTaskTracker.get(hostName);
        synchronized (trackers) {
          for (TaskTracker tracker : trackers) {
            tracker.cancelAllReservations();
          }
        }
        removeHostCapacity(hostName);
        fi.setBlacklist(rfb, reason);
      }
    }

    private boolean canUnBlackListTracker(String hostName,
        ReasonForBlackListing rfb) {
      FaultInfo fi = getFaultInfo(hostName, false);
      if(fi == null) {
        return false;
      }

      Set<ReasonForBlackListing> rfbSet = fi.getReasonforblacklisting();
      return fi.isBlacklisted() && rfbSet.contains(rfb);
    }

    private void unBlackListTracker(String hostName,
        ReasonForBlackListing rfb) {
      // check if you can black list the tracker then call this methods
      FaultInfo fi = getFaultInfo(hostName, false);
      if(fi.removeBlackListedReason(rfb)) {
        if(fi.getReasonforblacklisting().isEmpty()) {
          addHostCapacity(hostName);
          LOG.info("Unblacklisting tracker : " + hostName);
          fi.unBlacklist();
          //We have unBlackListed tracker, so tracker should
          //definitely be healthy. Check fault count if fault count
          //is zero don't keep it memory.
          if(fi.getFaultCount() == 0) {
            potentiallyFaultyTrackers.remove(hostName);
          }
        }
      }
    }

    // Assumes JobTracker is locked on the entry
    protected FaultInfo getFaultInfo(String hostName,
        boolean createIfNeccessary) {
      FaultInfo fi = null;
      synchronized (potentiallyFaultyTrackers) {
        fi = potentiallyFaultyTrackers.get(hostName);
        if (fi == null && createIfNeccessary) {
          fi = new FaultInfo();
          potentiallyFaultyTrackers.put(hostName, fi);
        }
      }
      return fi;
    }

    /**
     * Blacklists the tracker across all jobs if
     * <ol>
     * <li>#faults are more than
     *     MaxBlacklistsPerTracker (configurable) blacklists</li>
     * <li>#faults is 50% (configurable) above the average #faults</li>
     * <li>50% the cluster is not blacklisted yet </li>
     * </ol>
     */
    private boolean exceedsFaults(FaultInfo fi) {
      int faultCount = fi.getFaultCount();
      if (faultCount >= jobTrackerReconfigurable.getMaxBlacklistsPerTracker()) {
        // calculate avgBlackLists
        long clusterSize = getClusterStatus().getTaskTrackers();
        long sum = 0;
        for (FaultInfo f : potentiallyFaultyTrackers.values()) {
          sum += f.getFaultCount();
        }
        double avg = (double) sum / clusterSize;

        long totalCluster = clusterSize + numBlacklistedTrackers;
        if ((faultCount - avg) > (AverageBlacklistThreshold * avg) &&
            numBlacklistedTrackers < (totalCluster * MAX_BLACKLIST_PERCENT)) {
          return true;
        }
      }
      return false;
    }

    /**
     * Removes the tracker from blacklist and
     * from potentially faulty list, when it is restarted.
     *
     * Assumes JobTracker is locked on the entry.
     *
     * @param hostName
     */
    void markTrackerHealthy(String hostName) {
      synchronized (potentiallyFaultyTrackers) {
        FaultInfo fi = potentiallyFaultyTrackers.remove(hostName);
        if (fi != null && fi.isBlacklisted()) {
          LOG.info("Removing " + hostName + " from blacklist");
          addHostCapacity(hostName);
        }
      }
    }

    /**
     * Check whether tasks can be assigned to the tracker.
     *
     * One fault of the tracker is discarded if there
     * are no faults during one day. So, the tracker will get a
     * chance again to run tasks of a job.
     * Assumes JobTracker is locked on the entry.
     *
     * @param hostName The tracker name
     * @param now The current time
     *
     * @return true if the tracker is blacklisted
     *         false otherwise
     */
    boolean shouldAssignTasksToTracker(String hostName, long now) {
      synchronized (potentiallyFaultyTrackers) {
        FaultInfo fi = potentiallyFaultyTrackers.get(hostName);
        if (fi != null &&
            (now - fi.getLastUpdated()) > UpdateFaultyTrackerInterval) {
          fi.forgiveOneFault();
          fi.setLastUpdated(now);
          if (canUnBlackListTracker(hostName,
              ReasonForBlackListing.EXCEEDING_FAILURES)) {
            unBlackListTracker(hostName,
                ReasonForBlackListing.EXCEEDING_FAILURES);
          }
        }
        return (fi != null && fi.isBlacklisted());
      }
    }

    private void removeHostCapacity(String hostName) {
      synchronized (taskTrackers) {
        // remove the capacity of trackers on this host
        int numTrackersOnHost = 0;
        for (TaskTrackerStatus status : getStatusesOnHost(hostName)) {
          updateTotalTaskCapacity(status);
          removeTaskTrackerCapacity(status);
          int mapSlots = taskScheduler.getMaxSlots(status, TaskType.MAP);
          int reduceSlots = taskScheduler.getMaxSlots(status, TaskType.REDUCE);
          ++numTrackersOnHost;
          getInstrumentation().addBlackListedMapSlots(
              mapSlots);
          getInstrumentation().addBlackListedReduceSlots(
              reduceSlots);
        }
        uniqueHostsMap.remove(hostName);
        incrBlackListedTrackers(numTrackersOnHost);
      }
    }

    // This is called on tracker's restart or after a day of blacklist.
    private void addHostCapacity(String hostName) {
      synchronized (taskTrackers) {
        int numTrackersOnHost = 0;
        // add the capacity of trackers on the host
        for (TaskTrackerStatus status : getStatusesOnHost(hostName)) {
          updateTotalTaskCapacity(status);
          int mapSlots = taskScheduler.getMaxSlots(status, TaskType.MAP);
          int reduceSlots = taskScheduler.getMaxSlots(status, TaskType.REDUCE);
          numTrackersOnHost++;
          getInstrumentation().decBlackListedMapSlots(mapSlots);
          getInstrumentation().decBlackListedReduceSlots(reduceSlots);
        }
        uniqueHostsMap.put(hostName,
                           numTrackersOnHost);
        decrBlackListedTrackers(numTrackersOnHost);
      }
    }

    /**
     * Whether a host is blacklisted across all the jobs.
     *
     * Assumes JobTracker is locked on the entry.
     * @param hostName
     * @return
     */
    boolean isBlacklisted(String hostName) {
      synchronized (potentiallyFaultyTrackers) {
        FaultInfo fi = null;
        if ((fi = potentiallyFaultyTrackers.get(hostName)) != null) {
          return fi.isBlacklisted();
        }
      }
      return false;
    }

    // Assumes JobTracker is locked on the entry.
    int getFaultCount(String hostName) {
      synchronized (potentiallyFaultyTrackers) {
        FaultInfo fi = null;
        if ((fi = potentiallyFaultyTrackers.get(hostName)) != null) {
          return fi.getFaultCount();
        }
      }
      return 0;
    }

    // Assumes JobTracker is locked on the entry.
    Set<ReasonForBlackListing> getReasonForBlackListing(String hostName) {
      synchronized (potentiallyFaultyTrackers) {
        FaultInfo fi = null;
        if ((fi = potentiallyFaultyTrackers.get(hostName)) != null) {
          return fi.getReasonforblacklisting();
        }
      }
      return null;
    }


    // Assumes JobTracker is locked on the entry.
    void setNodeHealthStatus(String hostName, boolean isHealthy, String reason) {
      FaultInfo fi = null;
      // If tracker is not healthy, create a fault info object
      // blacklist it.
      if (!isHealthy) {
        fi = getFaultInfo(hostName, true);
        fi.setHealthy(isHealthy);
        synchronized (potentiallyFaultyTrackers) {
          blackListTracker(hostName, reason,
              ReasonForBlackListing.NODE_UNHEALTHY);
        }
      } else {
        fi = getFaultInfo(hostName, false);
        if (fi == null) {
          return;
        } else {
          if (canUnBlackListTracker(hostName,
              ReasonForBlackListing.NODE_UNHEALTHY)) {
            unBlackListTracker(hostName, ReasonForBlackListing.NODE_UNHEALTHY);
          }
        }
      }
    }
  }

  /**
   * Get all task tracker statuses on given host
   *
   * Assumes JobTracker is locked on the entry
   * @param hostName
   * @return {@link java.util.List} of {@link TaskTrackerStatus}
   */
  private List<TaskTrackerStatus> getStatusesOnHost(String hostName) {
    List<TaskTrackerStatus> statuses = new ArrayList<TaskTrackerStatus>();
    synchronized (taskTrackers) {
      for (TaskTracker tt : taskTrackers.values()) {
        TaskTrackerStatus status = tt.getStatus();
        if (hostName.equals(status.getHost())) {
          statuses.add(status);
        }
      }
    }
    return statuses;
  }

  private final JobTrackerInstrumentation myInstrumentation;
  private ObjectName versionBeanName;

  /////////////////////////////////////////////////////////////////
  // The real JobTracker
  ////////////////////////////////////////////////////////////////
  int port;
  String localMachine;
  private String trackerIdentifier;
  long startTime;
  int totalSubmissions = 0;
  private int totalMapTaskCapacity;
  private int totalReduceTaskCapacity;

  // Remember the last number of slots for updating total map and reduce
  // capacity
  private Map<String, Integer> trackerNameToMapSlots =
      new HashMap<String, Integer>();
  private Map<String, Integer> trackerNameToReduceSlots =
      new HashMap<String, Integer>();

  private HostsFileReader hostsReader;

  //
  // Properties to maintain while running Jobs and Tasks:
  //
  // 1.  Each Task is always contained in a single Job.  A Job succeeds when all its
  //     Tasks are complete.
  //
  // 2.  Every running or successful Task is assigned to a Tracker.  Idle Tasks are not.
  //
  // 3.  When a Tracker fails, all of its assigned Tasks are marked as failures.
  //
  // 4.  A Task might need to be reexecuted if it (or the machine it's hosted on) fails
  //     before the Job is 100% complete.  Sometimes an upstream Task can fail without
  //     reexecution if all downstream Tasks that require its output have already obtained
  //     the necessary files.
  //

  // All the known jobs.  (jobid->JobInProgress)
  Map<JobID, JobInProgress> jobs =
    Collections.synchronizedMap(new TreeMap<JobID, JobInProgress>());


  // (user -> list of JobInProgress)
  TreeMap<String, ArrayList<JobInProgress>> userToJobsMap =
    new TreeMap<String, ArrayList<JobInProgress>>();

  // (trackerID --> list of jobs to cleanup)
  Map<String, Set<JobID>> trackerToJobsToCleanup =
    new HashMap<String, Set<JobID>>();

  // (trackerID --> list of tasks to cleanup)
  Map<String, Set<TaskAttemptID>> trackerToTasksToCleanup =
    new HashMap<String, Set<TaskAttemptID>>();

  // All the known TaskInProgress items, mapped to by taskids (taskid->TIP)
  Map<TaskAttemptID, TaskInProgress> taskidToTIPMap =
    new TreeMap<TaskAttemptID, TaskInProgress>();
  // This is used to keep track of all trackers running on one host. While
  // decommissioning the host, all the trackers on the host will be lost.
  Map<String, Set<TaskTracker>> hostnameToTaskTracker =
    Collections.synchronizedMap(new TreeMap<String, Set<TaskTracker>>());


  // (taskid --> trackerID)
  TreeMap<TaskAttemptID, String> taskidToTrackerMap = new TreeMap<TaskAttemptID, String>();

  // (trackerID->TreeSet of taskids running at that tracker)
  HashMap<String, Set<TaskAttemptIDWithTip>> trackerToTaskMap =
    new HashMap<String, Set<TaskAttemptIDWithTip>>();

  // (trackerID -> TreeSet of completed taskids running at that tracker)
  TreeMap<String, Set<TaskAttemptID>> trackerToMarkedTasksMap =
    new TreeMap<String, Set<TaskAttemptID>>();

  // (trackerID --> last sent HeartBeatResponse)
  Map<String, HeartbeatResponse> trackerToHeartbeatResponseMap =
    new TreeMap<String, HeartbeatResponse>();

  // (hostname --> Node (NetworkTopology))
  Map<String, Node> hostnameToNodeMap =
    Collections.synchronizedMap(new TreeMap<String, Node>());

  // job-id->username during staging
  Map<JobID, String> jobToUserMap =
    Collections.synchronizedMap(new TreeMap<JobID, String>());

  // Number of resolved entries
  int numResolved;

  protected FaultyTrackersInfo faultyTrackers = new FaultyTrackersInfo();

  private JobTrackerStatistics statistics =
    new JobTrackerStatistics();
  //
  // Watch and expire TaskTracker objects using these structures.
  // We can map from Name->TaskTrackerStatus, or we can expire by time.
  //
  int totalMaps = 0;
  int totalReduces = 0;
  private int occupiedMapSlots = 0;
  private int occupiedReduceSlots = 0;
  private int reservedMapSlots = 0;
  private int reservedReduceSlots = 0;
  private HashMap<String, TaskTracker> taskTrackers =
    new HashMap<String, TaskTracker>();
  Map<String,Integer>uniqueHostsMap = new ConcurrentHashMap<String, Integer>();
  ExpireTrackers expireTrackers = new ExpireTrackers();
  Thread expireTrackersThread = null;
  RetireJobs retireJobs = new RetireJobs();
  Thread retireJobsThread = null;
  final int retiredJobsCacheSize;
  ExpireLaunchingTasks expireLaunchingTasks = new ExpireLaunchingTasks();
  JobUpdater jobUpdater = new JobUpdater();

  Thread expireLaunchingTaskThread = new Thread(expireLaunchingTasks,
                                                "expireLaunchingTasks");
  Thread jobUpdaterThread = new Thread(jobUpdater, "jobUpdater");

  CompletedJobStatusStore completedJobStatusStore = null;
  Thread completedJobsStoreThread = null;

  /**
   * It might seem like a bug to maintain a TreeSet of tasktracker objects,
   * which can be updated at any time.  But that's not what happens!  We
   * only update status objects in the taskTrackers table.  Status objects
   * are never updated once they enter the expiry queue.  Instead, we wait
   * for them to expire and remove them from the expiry queue.  If a status
   * object has been updated in the taskTracker table, the latest status is
   * reinserted.  Otherwise, we assume the tracker has expired.
   */
  TreeSet<TaskTrackerStatus> trackerExpiryQueue =
    new TreeSet<TaskTrackerStatus>(
                                   new Comparator<TaskTrackerStatus>() {
                                     public int compare(TaskTrackerStatus p1, TaskTrackerStatus p2) {
                                       if (p1.getLastSeen() < p2.getLastSeen()) {
                                         return -1;
                                       } else if (p1.getLastSeen() > p2.getLastSeen()) {
                                         return 1;
                                       } else {
                                         return (p1.getTrackerName().compareTo(p2.getTrackerName()));
                                       }
                                     }
                                   }
                                   );

  // Used to provide an HTML view on Job, Task, and TaskTracker structures
  final HttpServer infoServer;
  int infoPort;

  Server interTrackerServer;

  // Some jobs are stored in a local system directory.  We can delete
  // the files when we're done with the job.
  public static final String SUBDIR = "jobTracker";
  FileSystem fs = null;
  Path systemDir = null;
  JobConf conf;
  private JobTrackerReconfigurable jobTrackerReconfigurable;
  private final UserGroupInformation mrOwner;
  private final String supergroup;

  long limitMaxMemForMapTasks;
  long limitMaxMemForReduceTasks;
  long memSizeForMapSlotOnJT;
  long memSizeForReduceSlotOnJT;

  private QueueManager queueManager;

  /**
   * Start the JobTracker process, listen on the indicated port
   */
  JobTracker(JobConf conf) throws IOException, InterruptedException {
    this(conf, generateNewIdentifier());
  }

  /**
   * Start the JobTracker process, listen on the indicated port
   */
  JobTracker(JobConf conf, Clock clock)
    throws IOException, InterruptedException {
    this(conf, generateNewIdentifier());
    JobTracker.clock = clock;
  }

  JobTracker(JobConf conf, String identifier)
  throws IOException, InterruptedException {
    // find the owner of the process
    if (conf.getBoolean("hadoop.disable.shell",false)){
      conf.setStrings(UnixUserGroupInformation.UGI_PROPERTY_NAME, new String[]{"hadoop", "hadoop"});
    }

    try {
      mrOwner = UnixUserGroupInformation.login(conf);
    } catch (LoginException e) {
      throw new IOException(StringUtils.stringifyException(e));
    }

    supergroup = conf.get("mapred.permissions.supergroup", "supergroup");
    LOG.info("Starting jobtracker with owner as " + mrOwner.getUserName()
             + " and supergroup as " + supergroup);

    //
    // Grab some static constants
    //
    TASKTRACKER_EXPIRY_INTERVAL =
      conf.getLong("mapred.tasktracker.expiry.interval", 10 * 60 * 1000);
    RETIRE_JOB_INTERVAL = conf.getLong("mapred.jobtracker.retirejob.interval", 24 * 60 * 60 * 1000);
    RETIRE_JOB_CHECK_INTERVAL = conf.getLong("mapred.jobtracker.retirejob.check", 60 * 1000);
    retiredJobsCacheSize =
             conf.getInt("mapred.job.tracker.retiredjobs.cache.size", 1000);
    MAX_COMPLETE_USER_JOBS_IN_MEMORY = conf.getInt("mapred.jobtracker.completeuserjobs.maximum", 100);
    MIN_TIME_BEFORE_RETIRE = conf.getInt("mapred.jobtracker.mintime.before.retirejob", 0);

    UpdateFaultyTrackerInterval =
        conf.getInt("mapred.tasktracker.blacklist.reevaluation.interval", 24 * 60 * 60 * 1000);

    NUM_HEARTBEATS_IN_SECOND =
      conf.getInt(JT_HEARTBEATS_IN_SECOND, DEFAULT_NUM_HEARTBEATS_IN_SECOND);
    if (NUM_HEARTBEATS_IN_SECOND < MIN_NUM_HEARTBEATS_IN_SECOND) {
      NUM_HEARTBEATS_IN_SECOND = DEFAULT_NUM_HEARTBEATS_IN_SECOND;
    }

    HEARTBEATS_SCALING_FACTOR =
      conf.getFloat(JT_HEARTBEATS_SCALING_FACTOR,
                    DEFAULT_HEARTBEATS_SCALING_FACTOR);
    if (HEARTBEATS_SCALING_FACTOR < MIN_HEARTBEATS_SCALING_FACTOR) {
      HEARTBEATS_SCALING_FACTOR = DEFAULT_HEARTBEATS_SCALING_FACTOR;
    }

    //This configuration is there solely for tuning purposes and
    //once this feature has been tested in real clusters and an appropriate
    //value for the threshold has been found, this config might be taken out.
    AverageBlacklistThreshold =
      conf.getFloat("mapred.cluster.average.blacklist.threshold", 0.5f);

    // This is a directory of temporary submission files.  We delete it
    // on startup, and can delete any files that we're done with
    this.conf = conf;
    this.jobTrackerReconfigurable = new JobTrackerReconfigurable(conf);

    JobConf jobConf = new JobConf(conf);

    initializeTaskMemoryRelatedConfig();

    // Read the hosts/exclude files to restrict access to the jobtracker.
    this.hostsReader = new HostsFileReader(conf.get("mapred.hosts", ""),
                                           conf.get("mapred.hosts.exclude", ""));

    Configuration queuesConf = new Configuration(this.conf);
    queueManager = new QueueManager(queuesConf);

    // Create the scheduler
    Class<? extends TaskScheduler> schedulerClass
      = conf.getClass("mapred.jobtracker.taskScheduler",
          JobQueueTaskScheduler.class, TaskScheduler.class);
    taskScheduler = (TaskScheduler) ReflectionUtils.newInstance(schedulerClass, conf);

    // Create the resourceReporter
    Class<? extends ResourceReporter> reporterClass
      = conf.getClass("mapred.jobtracker.resourceReporter", null,
                      ResourceReporter.class);
    if (reporterClass != null) {
      resourceReporter =
        (ResourceReporter) ReflectionUtils.newInstance(reporterClass, conf);
      LOG.info("Resource reporter: " + reporterClass.getClass() +
               " is created");
    } else {
      resourceReporter = null;
      LOG.warn("Resource reporter is not configured. It will be disabled.");
    }

    // Set ports, start RPC servers, setup security policy etc.
    InetSocketAddress addr = getAddress(conf);
    this.localMachine = addr.getHostName();
    this.port = addr.getPort();

    // Set service-level authorization security policy
    if (conf.getBoolean(
          ServiceAuthorizationManager.SERVICE_AUTHORIZATION_CONFIG, false)) {
      PolicyProvider policyProvider =
        (PolicyProvider)(ReflectionUtils.newInstance(
            conf.getClass(PolicyProvider.POLICY_PROVIDER_CONFIG,
                MapReducePolicyProvider.class, PolicyProvider.class),
            conf));
      SecurityUtil.setPolicy(new ConfiguredPolicy(conf, policyProvider));
    }

    int handlerCount = conf.getInt("mapred.job.tracker.handler.count", 10);
    this.interTrackerServer = RPC.getServer(this, addr.getHostName(), addr.getPort(), handlerCount, false, conf);
    if (LOG.isDebugEnabled()) {
      Properties p = System.getProperties();
      for (Iterator it = p.keySet().iterator(); it.hasNext();) {
        String key = (String) it.next();
        String val = p.getProperty(key);
        LOG.debug("Property '" + key + "' is " + val);
      }
    }

    String infoAddr =
      NetUtils.getServerAddress(conf, "mapred.job.tracker.info.bindAddress",
                                "mapred.job.tracker.info.port",
                                "mapred.job.tracker.http.address");
    InetSocketAddress infoSocAddr = NetUtils.createSocketAddr(infoAddr);
    String infoBindAddress = infoSocAddr.getHostName();
    int tmpInfoPort = infoSocAddr.getPort();
    this.startTime = getClock().getTime();
    infoServer = new HttpServer("job", infoBindAddress, tmpInfoPort,
        tmpInfoPort == 0, conf);
    infoServer.setAttribute("job.tracker", this);
    // initialize history parameters.
    boolean historyInitialized = JobHistory.init(this, conf, this.localMachine,
                                                 this.startTime);

    infoServer.addServlet("reducegraph", "/taskgraph", TaskGraphServlet.class);

    jobTrackerReconfigurable = new JobTrackerReconfigurable(conf);
    infoServer.setAttribute(ReconfigurationServlet.
                            CONF_SERVLET_RECONFIGURABLE_PREFIX +
                            "/jtconfchange", jobTrackerReconfigurable);
    infoServer.addInternalServlet("jtconfchange", "/jtconfchange",
                                  ReconfigurationServlet.class);

    if (taskScheduler instanceof Reconfigurable) {
      infoServer.setAttribute(ReconfigurationServlet.
                              CONF_SERVLET_RECONFIGURABLE_PREFIX +
                              "/fsconfchange", taskScheduler);
      infoServer.addInternalServlet("fsconfchange", "/fsconfchange",
                                    ReconfigurationServlet.class);
    }

    infoServer.start();

    this.trackerIdentifier = identifier;

    // Initialize instrumentation
    JobTrackerInstrumentation tmp;
    Class<? extends JobTrackerInstrumentation> metricsInst =
      getInstrumentationClass(jobConf);
    try {
      java.lang.reflect.Constructor<? extends JobTrackerInstrumentation> c =
        metricsInst.getConstructor(new Class[] {JobTracker.class, JobConf.class} );
      tmp = c.newInstance(this, jobConf);
    } catch(Exception e) {
      //Reflection can throw lots of exceptions -- handle them all by
      //falling back on the default.
      LOG.error("failed to initialize job tracker metrics", e);
      tmp = new JobTrackerMetricsInst(this, jobConf);
    }
    myInstrumentation = tmp;
    versionBeanName = VersionInfo.registerJMX("JobTracker");

    int excludedAtStart = hostsReader.getExcludedHosts().size();
    myInstrumentation.setDecommissionedTrackers(excludedAtStart);

    // The rpc/web-server ports can be ephemeral ports...
    // ... ensure we have the correct info
    this.port = interTrackerServer.getListenerAddress().getPort();
    this.conf.set("mapred.job.tracker", (this.localMachine + ":" + this.port));
    LOG.info("JobTracker up at: " + this.port);
    this.infoPort = this.infoServer.getPort();
    this.conf.set("mapred.job.tracker.http.address",
        infoBindAddress + ":" + this.infoPort);
    LOG.info("JobTracker webserver: " + this.infoServer.getPort());

    // start async disk service for asynchronous deletion service
    asyncDiskService = new MRAsyncDiskService(FileSystem.getLocal(jobConf),
        jobConf.getLocalDirs(), conf);

    while (!Thread.currentThread().isInterrupted()) {
      try {
        // if we haven't contacted the namenode go ahead and do it
        if (fs == null) {
          fs = FileSystem.get(conf);
        }
        // clean up the system dir, which will only work if hdfs is out of
        // safe mode
        if(systemDir == null) {
          systemDir = new Path(getSystemDir());
        }
        if (conf.getBoolean(MAPRED_SYSTEM_DIR_CLEAN_KEY, false)) {
          LOG.info("Recreating system dir to start clean");
          if (fs.exists(systemDir)) {
            fs.delete(systemDir, true);
          }
        }
        // make the systemdir if it doesn't exist already
        if (!fs.isDirectory(systemDir)) {
          // if systemdir is a file, delete it
          if (fs.exists(systemDir)) {
            fs.delete(systemDir, true);
          }
          if (!FileSystem.mkdirs(fs, systemDir,
                                new FsPermission(SYSTEM_DIR_PERMISSION))) {
            LOG.error("Mkdirs failed to create " + systemDir);
          } else {
            // point of success
            break;
          }
        } else {

        // system dir exists
        // Make sure that the backup data is preserved
        FileStatus[] systemDirData = fs.listStatus(this.systemDir);
        LOG.info("Cleaning up the system directory");
        for (FileStatus status: systemDirData) {
          // spare the CAR directory - shared cache files are immutable and reusable
          if (status.isDir() &&
            status.getPath().getName().equals(JobSubmissionProtocol.CAR)) {
            LOG.info("Preserving shared cache in system directory");
            continue;
          }
          fs.delete(status.getPath(), true);
        }
        break;
        }
      } catch (AccessControlException ace) {
        LOG.warn("Failed to operate on mapred.system.dir (" + systemDir
                 + ") because of permissions.");
        LOG.warn("Manually delete the mapred.system.dir (" + systemDir
                 + ") and then start the JobTracker.");
        LOG.warn("Bailing out ... ");
        throw ace;
      } catch (IOException ie) {
        LOG.info("problem cleaning system directory: " + systemDir, ie);
      }
      Thread.sleep(FS_ACCESS_RETRY_PERIOD);
    }

    if (Thread.currentThread().isInterrupted()) {
      throw new InterruptedException();
    }

    asyncDiskService.moveAndDeleteFromEachVolume(SUBDIR);

    // Initialize history DONE folder
    if (historyInitialized) {
      JobHistory.initDone(conf, fs);
      String historyLogDir =
        JobHistory.getCompletedJobHistoryLocation().toString();
      infoServer.setAttribute("historyLogDir", historyLogDir);
      FileSystem historyFS = new Path(historyLogDir).getFileSystem(conf);
      infoServer.setAttribute("fileSys", historyFS);
    }

    this.dnsToSwitchMapping = ReflectionUtils.newInstance(
        conf.getClass("topology.node.switch.mapping.impl", ScriptBasedMapping.class,
            DNSToSwitchMapping.class), conf);
    this.numTaskCacheLevels = conf.getInt("mapred.task.cache.levels",
        NetworkTopology.DEFAULT_HOST_LEVEL);

    //initializes the job status store
    completedJobStatusStore = new CompletedJobStatusStore(conf);

    ExpireUnusedFilesInCache eufic = new  ExpireUnusedFilesInCache(
      conf, getClock(), new Path(getSystemDir()));


    taskErrorCollector = new TaskErrorCollector(conf);
  }

  private static SimpleDateFormat getDateFormat() {
    return new SimpleDateFormat("yyyyMMddHHmm");
  }

  private static String generateNewIdentifier() {
    return getDateFormat().format(new Date());
  }

  static boolean validateIdentifier(String id) {
    try {
      // the jobtracker id should be 'date' parseable
      getDateFormat().parse(id);
      return true;
    } catch (ParseException pe) {}
    return false;
  }

  static boolean validateJobNumber(String id) {
    try {
      // the job number should be integer parseable
      Integer.parseInt(id);
      return true;
    } catch (IllegalArgumentException pe) {}
    return false;
  }

  public static Class<? extends JobTrackerInstrumentation> getInstrumentationClass(Configuration conf) {
    return conf.getClass("mapred.jobtracker.instrumentation",
        JobTrackerMetricsInst.class, JobTrackerInstrumentation.class);
  }

  public static void setInstrumentationClass(Configuration conf, Class<? extends JobTrackerInstrumentation> t) {
    conf.setClass("mapred.jobtracker.instrumentation",
        t, JobTrackerInstrumentation.class);
  }

  JobTrackerInstrumentation getInstrumentation() {
    return myInstrumentation;
  }

  public static InetSocketAddress getAddress(Configuration conf) {
    String jobTrackerStr =
      conf.get("mapred.job.tracker", "localhost:8012");
    return NetUtils.createSocketAddr(jobTrackerStr);
  }

  JobTrackerReconfigurable getJobTrackerReconfigurable() {
    return jobTrackerReconfigurable;
  }

  /**
   * Run forever
   */
  public void offerService() throws InterruptedException, IOException {

    taskScheduler.start();

    // refresh the node list as the recovery manager might have added
    // disallowed trackers
    refreshHosts();

    this.expireTrackersThread = new Thread(this.expireTrackers,
                                          "expireTrackers");
    this.expireTrackersThread.setDaemon(true);
    this.expireTrackersThread.start();
    this.retireJobsThread = new Thread(this.retireJobs, "retireJobs");
    this.retireJobsThread.setDaemon(true);
    this.retireJobsThread.start();
    expireLaunchingTaskThread.setDaemon(true);
    expireLaunchingTaskThread.start();
    jobUpdaterThread.setDaemon(true);
    jobUpdaterThread.start();

    if (completedJobStatusStore.isActive()) {
      completedJobsStoreThread = new Thread(completedJobStatusStore,
                                            "completedjobsStore-housekeeper");
      completedJobsStoreThread.start();
    }

    // start the inter-tracker server once the jt is ready
    this.interTrackerServer.start();

    synchronized (this) {
      state = State.RUNNING;
    }
    LOG.info("Starting RUNNING");

    this.interTrackerServer.join();
    LOG.info("Stopped interTrackerServer");
  }

  private void closeThread(Thread t) {
    if (t != null && t.isAlive()) {
      LOG.info("Stopping " + t.getName());
      t.interrupt();
      try {
        t.join();
      } catch (InterruptedException ex) {
        ex.printStackTrace();
      }
    }
  }


  void close() throws IOException {
    if (this.infoServer != null) {
      LOG.info("Stopping infoServer");
      try {
        this.infoServer.stop();
      } catch (Exception ex) {
        LOG.warn("Exception shutting down JobTracker", ex);
      }
    }
    if (this.interTrackerServer != null) {
      LOG.info("Stopping interTrackerServer");
      this.interTrackerServer.stop();
    }

    shutdown = true;

    closeThread(this.expireTrackersThread);
    closeThread(this.retireJobsThread);
    closeThread(this.jobUpdaterThread);

    if (taskScheduler != null) {
      taskScheduler.terminate();
    }

    closeThread(this.expireLaunchingTaskThread);
    closeThread(this.completedJobsStoreThread);
    if (versionBeanName != null) {
      MBeanUtil.unregisterMBean(versionBeanName);
    }
    LOG.info("stopped all jobtracker services");
    return;
  }

  ///////////////////////////////////////////////////////
  // Maintain lookup tables; called by JobInProgress
  // and TaskInProgress
  ///////////////////////////////////////////////////////
  void createTaskEntry(TaskAttemptID taskid, String taskTracker, TaskInProgress tip) {
    LOG.info("Adding task (" + tip.getAttemptType(taskid) + ") " +
      "'"  + taskid + "' to tip " +
      tip.getTIPId() + ", for tracker '" + taskTracker + "'");

    // taskid --> tracker
    taskidToTrackerMap.put(taskid, taskTracker);

    // tracker --> taskid
    Set<TaskAttemptIDWithTip> taskset = trackerToTaskMap.get(taskTracker);
    if (taskset == null) {
      taskset = new HashSet<TaskAttemptIDWithTip>();
      trackerToTaskMap.put(taskTracker, taskset);
    }
    taskset.add(new TaskAttemptIDWithTip(taskid, tip));

    // taskid --> TIP
    taskidToTIPMap.put(taskid, tip);

  }

  void removeTaskEntry(TaskAttemptID taskid) {
    // taskid --> tracker
    String tracker = taskidToTrackerMap.remove(taskid);

    // tracker --> taskid
    if (tracker != null) {
      Set<TaskAttemptIDWithTip> taskset = trackerToTaskMap.get(tracker);
      if (taskset != null) {
        taskset.remove(new TaskAttemptIDWithTip(taskid, null));
      }
    }

    // taskid --> TIP
    if (taskidToTIPMap.remove(taskid) != null) {
      LOG.info("Removing task '" + taskid + "'");
    }
  }

  /**
   * Mark a 'task' for removal later.
   * This function assumes that the JobTracker is locked on entry.
   *
   * @param taskTracker the tasktracker at which the 'task' was running
   * @param taskid completed (success/failure/killed) task
   */
  void markCompletedTaskAttempt(String taskTracker, TaskAttemptID taskid) {
    // tracker --> taskid
    Set<TaskAttemptID> taskset = trackerToMarkedTasksMap.get(taskTracker);
    if (taskset == null) {
      taskset = new TreeSet<TaskAttemptID>();
      trackerToMarkedTasksMap.put(taskTracker, taskset);
    }
    taskset.add(taskid);

    LOG.debug("Marked '" + taskid + "' from '" + taskTracker + "'");
  }

  /**
   * Mark all 'non-running' jobs of the job for pruning.
   * This function assumes that the JobTracker is locked on entry.
   *
   * @param job the completed job
   */
  void markCompletedJob(JobInProgress job) {
    for (TaskInProgress tip : job.getTasks(TaskType.JOB_SETUP)) {
      for (TaskStatus taskStatus : tip.getTaskStatuses()) {
        if (taskStatus.getRunState() != TaskStatus.State.RUNNING &&
            taskStatus.getRunState() != TaskStatus.State.COMMIT_PENDING &&
            taskStatus.getRunState() != TaskStatus.State.UNASSIGNED) {
          markCompletedTaskAttempt(taskStatus.getTaskTracker(),
                                   taskStatus.getTaskID());
        }
      }
    }
    for (TaskInProgress tip : job.getTasks(TaskType.MAP)) {
      for (TaskStatus taskStatus : tip.getTaskStatuses()) {
        if (taskStatus.getRunState() != TaskStatus.State.RUNNING &&
            taskStatus.getRunState() != TaskStatus.State.COMMIT_PENDING &&
            taskStatus.getRunState() != TaskStatus.State.FAILED_UNCLEAN &&
            taskStatus.getRunState() != TaskStatus.State.KILLED_UNCLEAN &&
            taskStatus.getRunState() != TaskStatus.State.UNASSIGNED) {
          markCompletedTaskAttempt(taskStatus.getTaskTracker(),
                                   taskStatus.getTaskID());
        }
      }
    }
    for (TaskInProgress tip : job.getTasks(TaskType.REDUCE)) {
      for (TaskStatus taskStatus : tip.getTaskStatuses()) {
        if (taskStatus.getRunState() != TaskStatus.State.RUNNING &&
            taskStatus.getRunState() != TaskStatus.State.COMMIT_PENDING &&
            taskStatus.getRunState() != TaskStatus.State.FAILED_UNCLEAN &&
            taskStatus.getRunState() != TaskStatus.State.KILLED_UNCLEAN &&
            taskStatus.getRunState() != TaskStatus.State.UNASSIGNED) {
          markCompletedTaskAttempt(taskStatus.getTaskTracker(),
                                   taskStatus.getTaskID());
        }
      }
    }
  }

  /**
   * Remove all 'marked' tasks running on a given {@link TaskTracker}
   * from the {@link JobTracker}'s data-structures.
   * This function assumes that the JobTracker is locked on entry.
   *
   * @param taskTracker tasktracker whose 'non-running' tasks are to be purged
   */
  void removeMarkedTasks(String taskTracker) {
    // Purge all the 'marked' tasks which were running at taskTracker
    Set<TaskAttemptID> markedTaskSet =
      trackerToMarkedTasksMap.get(taskTracker);
    if (markedTaskSet != null) {
      for (TaskAttemptID taskid : markedTaskSet) {
        removeTaskEntry(taskid);
        if (LOG.isDebugEnabled()) {
          LOG.debug("Removed marked completed task '" + taskid + "' from '" +
                    taskTracker + "'");
        }
      }
      // Clear
      trackerToMarkedTasksMap.remove(taskTracker);
    }
  }

  /**
   * Call {@link #removeTaskEntry(org.apache.hadoop.mapred.TaskAttemptID)}
   * for each of the
   * job's tasks.
   * When the JobTracker is retiring the long-completed
   * job, either because it has outlived {@link #RETIRE_JOB_INTERVAL}
   * or the limit of {@link #MAX_COMPLETE_USER_JOBS_IN_MEMORY} jobs
   * has been reached, we can afford to nuke all it's tasks; a little
   * unsafe, but practically feasible.
   *
   * @param job the job about to be 'retired'
   */
  synchronized void removeJobTasks(JobInProgress job) {
    // iterate over all the task types
    for (TaskType type : TaskType.values()) {
      // iterate over all the tips of the type under consideration
      for (TaskInProgress tip : job.getTasks(type)) {
        // iterate over all the task-ids in the tip under consideration
        for (TaskAttemptID id : tip.getAllTaskAttemptIDs()) {
          // remove the task-id entry from the jobtracker
          removeTaskEntry(id);
        }
      }
    }
  }

  /**
   * Safe clean-up all data structures at the end of the
   * job (success/failure/killed).
   * Here we also ensure that for a given user we maintain
   * information for only MAX_COMPLETE_USER_JOBS_IN_MEMORY jobs
   * on the JobTracker.
   *
   * @param job completed job.
   */
  synchronized void finalizeJob(JobInProgress job) {
    // Mark the 'non-running' tasks for pruning
    markCompletedJob(job);

    JobEndNotifier.registerNotification(job.getJobConf(), job.getStatus());

    // start the merge of log files
    JobID id = job.getStatus().getJobID();

    // mark the job as completed
    try {
      JobHistory.JobInfo.markCompleted(id);
    } catch (IOException ioe) {
      LOG.info("Failed to mark job " + id + " as completed!", ioe);
    }

    final JobTrackerInstrumentation metrics = getInstrumentation();
    metrics.finalizeJob(conf, id);

    long now = getClock().getTime();

    // mark the job for cleanup at all the trackers
    addJobForCleanup(id);

    try {
      File userFileForJob =
        new File(lDirAlloc.getLocalPathToRead(SUBDIR + "/" + id,
                                              conf).toString());
      if (userFileForJob != null) {
        userFileForJob.delete();
      }
    } catch (IOException ioe) {
      LOG.info("Failed to delete job id mapping for job " + id, ioe);
    }

    // add the blacklisted trackers to potentially faulty list
    if (job.getStatus().getRunState() == JobStatus.SUCCEEDED) {
      if (job.getNoOfBlackListedTrackers() > 0) {
        for (Entry<String, List<String>> e : job.getBlackListedTrackers().entrySet()) {
          String tt = e.getKey();
          String jobName = job.getJobID().toString();
          String[] exceptions = e.getValue().toArray(new String[0]);

          faultyTrackers.incrementFaults(tt, new JobFault(tt, jobName, exceptions));
        }
      }
    }

    String jobUser = job.getProfile().getUser();
    //add to the user to jobs mapping
    synchronized (userToJobsMap) {
      ArrayList<JobInProgress> userJobs = userToJobsMap.get(jobUser);
      if (userJobs == null) {
        userJobs =  new ArrayList<JobInProgress>();
        userToJobsMap.put(jobUser, userJobs);
      }
      userJobs.add(job);
    }
  }

  ///////////////////////////////////////////////////////
  // Accessors for objects that want info on jobs, tasks,
  // trackers, etc.
  ///////////////////////////////////////////////////////
  public int getTotalSubmissions() {
    return totalSubmissions;
  }
  public String getJobTrackerMachine() {
    return localMachine;
  }

  /**
   * Get the unique identifier (ie. timestamp) of this job tracker start.
   * @return a string with a unique identifier
   */
  public String getTrackerIdentifier() {
    return trackerIdentifier;
  }

  public int getTrackerPort() {
    return port;
  }
  public int getInfoPort() {
    return infoPort;
  }
  public long getStartTime() {
    return startTime;
  }
  public Vector<JobInProgress> runningJobs() {
    Vector<JobInProgress> v = new Vector<JobInProgress>();
    for (Iterator it = jobs.values().iterator(); it.hasNext();) {
      JobInProgress jip = (JobInProgress) it.next();
      JobStatus status = jip.getStatus();
      if (status.getRunState() == JobStatus.RUNNING) {
        v.add(jip);
      }
    }
    return v;
  }
  /**
   * Version that is called from a timer thread, and therefore needs to be
   * careful to synchronize.
   */
  public synchronized List<JobInProgress> getRunningJobs() {
    synchronized (jobs) {
      return runningJobs();
    }
  }
  public Vector<JobInProgress> failedJobs() {
    Vector<JobInProgress> v = new Vector<JobInProgress>();
    for (Iterator it = jobs.values().iterator(); it.hasNext();) {
      JobInProgress jip = (JobInProgress) it.next();
      JobStatus status = jip.getStatus();
      if ((status.getRunState() == JobStatus.FAILED)
          || (status.getRunState() == JobStatus.KILLED)) {
        v.add(jip);
      }
    }
    return v;
  }

  public synchronized List<JobInProgress> getFailedJobs() {
    synchronized (jobs) {
      return failedJobs();
    }
  }

  public Vector<JobInProgress> completedJobs() {
    Vector<JobInProgress> v = new Vector<JobInProgress>();
    for (Iterator it = jobs.values().iterator(); it.hasNext();) {
      JobInProgress jip = (JobInProgress) it.next();
      JobStatus status = jip.getStatus();
      if (status.getRunState() == JobStatus.SUCCEEDED) {
        v.add(jip);
      }
    }
    return v;
  }

  public synchronized List<JobInProgress> getCompletedJobs() {
    synchronized (jobs) {
      return completedJobs();
    }
  }

  private Vector<JobInProgress> preparingJobs() {
    Vector<JobInProgress> v = new Vector<JobInProgress>();
    for (Iterator it = jobs.values().iterator(); it.hasNext();) {
      JobInProgress jip = (JobInProgress) it.next();
      JobStatus status = jip.getStatus();
      if (status.getRunState() == JobStatus.PREP) {
        v.add(jip);
      }
    }
    return v;
  }

  public synchronized List<JobInProgress> getPreparingJobs() {
    synchronized (jobs) {
      return preparingJobs();
    }
  }

  /**
   * Get all the task trackers in the cluster
   *
   * @return {@link Collection} of {@link TaskTrackerStatus}
   */
  // lock to taskTrackers should hold JT lock first.
  public synchronized Collection<TaskTrackerStatus> taskTrackers() {
    Collection<TaskTrackerStatus> ttStatuses;
    synchronized (taskTrackers) {
      ttStatuses =
        new ArrayList<TaskTrackerStatus>(taskTrackers.values().size());
      for (TaskTracker tt : taskTrackers.values()) {
        ttStatuses.add(tt.getStatus());
      }
    }
    return ttStatuses;
  }

  /**
   * Get the active task tracker statuses in the cluster
   *
   * @return {@link Collection} of active {@link TaskTrackerStatus}
   */
  // This method is synchronized to make sure that the locking order
  // "taskTrackers lock followed by faultyTrackers.potentiallyFaultyTrackers
  // lock" is under JobTracker lock to avoid deadlocks.
  synchronized public Collection<TaskTrackerStatus> activeTaskTrackers() {
    Collection<TaskTrackerStatus> activeTrackers =
      new ArrayList<TaskTrackerStatus>();
    synchronized (taskTrackers) {
      for ( TaskTracker tt : taskTrackers.values()) {
        TaskTrackerStatus status = tt.getStatus();
        if (!faultyTrackers.isBlacklisted(status.getHost())) {
          activeTrackers.add(status);
        }
      }
    }
    return activeTrackers;
  }

  /**
   * Get the active and blacklisted task tracker names in the cluster. The first
   * element in the returned list contains the list of active tracker names.
   * The second element in the returned list contains the list of blacklisted
   * tracker names.
   */
  // This method is synchronized to make sure that the locking order
  // "taskTrackers lock followed by faultyTrackers.potentiallyFaultyTrackers
  // lock" is under JobTracker lock to avoid deadlocks.
  synchronized public List<List<String>> taskTrackerNames() {
    List<String> activeTrackers =
      new ArrayList<String>();
    List<String> blacklistedTrackers =
      new ArrayList<String>();
    synchronized (taskTrackers) {
      for (TaskTracker tt : taskTrackers.values()) {
        TaskTrackerStatus status = tt.getStatus();
        if (!faultyTrackers.isBlacklisted(status.getHost())) {
          activeTrackers.add(status.getTrackerName());
        } else {
          blacklistedTrackers.add(status.getTrackerName());
        }
      }
    }
    List<List<String>> result = new ArrayList<List<String>>(2);
    result.add(activeTrackers);
    result.add(blacklistedTrackers);
    return result;
  }

  /**
   * Get the blacklisted task tracker statuses in the cluster
   *
   * @return {@link Collection} of blacklisted {@link TaskTrackerStatus}
   */
  // This method is synchronized to make sure that the locking order
  // "taskTrackers lock followed by faultyTrackers.potentiallyFaultyTrackers
  // lock" is under JobTracker lock to avoid deadlocks.
  synchronized public Collection<TaskTrackerStatus> blacklistedTaskTrackers() {
    Collection<TaskTrackerStatus> blacklistedTrackers =
      new ArrayList<TaskTrackerStatus>();
    synchronized (taskTrackers) {
      for (TaskTracker tt : taskTrackers.values()) {
        TaskTrackerStatus status = tt.getStatus();
        if (faultyTrackers.isBlacklisted(status.getHost())) {
          blacklistedTrackers.add(status);
        }
      }
    }
    return blacklistedTrackers;
  }

  synchronized int getFaultCount(String hostName) {
    return faultyTrackers.getFaultCount(hostName);
  }

  /**
   * Get the number of blacklisted trackers across all the jobs
   *
   * @return
   */
  int getBlacklistedTrackerCount() {
    return faultyTrackers.numBlacklistedTrackers;
  }

  /**
   * Whether the tracker is blacklisted or not
   *
   * @param trackerID
   *
   * @return true if blacklisted, false otherwise
   */
  synchronized public boolean isBlacklisted(String trackerID) {
    TaskTrackerStatus status = getTaskTrackerStatus(trackerID);
    if (status != null) {
      return faultyTrackers.isBlacklisted(status.getHost());
    }
    return false;
  }

  // lock to taskTrackers should hold JT lock first.
  synchronized public TaskTrackerStatus getTaskTrackerStatus(String trackerID) {
    TaskTracker taskTracker;
    synchronized (taskTrackers) {
      taskTracker = taskTrackers.get(trackerID);
    }
    return (taskTracker == null) ? null : taskTracker.getStatus();
  }

  // lock to taskTrackers should hold JT lock first.
  synchronized public TaskTracker getTaskTracker(String trackerID) {
    synchronized (taskTrackers) {
      return taskTrackers.get(trackerID);
    }
  }

  JobTrackerStatistics getStatistics() {
    return statistics;
  }
  /**
   * Adds a new node to the jobtracker. It involves adding it to the expiry
   * thread and adding it for resolution
   *
   * Assumes JobTracker, taskTrackers and trackerExpiryQueue is locked on entry
   *
   * @param taskTracker Task Tracker
   */
  void addNewTracker(TaskTracker taskTracker) {
    TaskTrackerStatus status = taskTracker.getStatus();
    trackerExpiryQueue.add(status);

    //  Register the tracker if its not registered
    String hostname = status.getHost();
    if (getNode(status.getTrackerName()) == null) {
      // Making the network location resolution inline ..
      resolveAndAddToTopology(hostname);
    }

    // add it to the set of tracker per host
    Set<TaskTracker> trackers = hostnameToTaskTracker.get(hostname);
    if (trackers == null) {
      trackers = Collections.synchronizedSet(new HashSet<TaskTracker>());
      hostnameToTaskTracker.put(hostname, trackers);
    }
    statistics.taskTrackerAdded(status.getTrackerName());
    getInstrumentation().addTrackers(1);
    LOG.info("Adding tracker " + status.getTrackerName() + " to host "
             + hostname);
    trackers.add(taskTracker);
  }

  public Node resolveAndAddToTopology(String name) {
    List <String> tmpList = new ArrayList<String>(1);
    tmpList.add(name);
    List <String> rNameList = dnsToSwitchMapping.resolve(tmpList);
    String rName = rNameList.get(0);
    String networkLoc = NodeBase.normalize(rName);
    return addHostToNodeMapping(name, networkLoc);
  }

  private Node addHostToNodeMapping(String host, String networkLoc) {
    Node node = null;
    synchronized (nodesAtMaxLevel) {
      if ((node = clusterMap.getNode(networkLoc+"/"+host)) == null) {
        node = new NodeBase(host, networkLoc);
        clusterMap.add(node);
        if (node.getLevel() < getNumTaskCacheLevels()) {
          LOG.fatal("Got a host whose level is: " + node.getLevel() + "."
              + " Should get at least a level of value: "
              + getNumTaskCacheLevels());
          try {
            stopTracker();
          } catch (IOException ie) {
            LOG.warn("Exception encountered during shutdown: "
                + StringUtils.stringifyException(ie));
            System.exit(-1);
          }
        }
        hostnameToNodeMap.put(host, node);
        // Make an entry for the node at the max level in the cache
        nodesAtMaxLevel.add(getParentNode(node, getNumTaskCacheLevels() - 1));
      }
    }
    return node;
  }

  /**
   * Returns a collection of nodes at the max level
   */
  public Collection<Node> getNodesAtMaxLevel() {
    return nodesAtMaxLevel;
  }

  public static Node getParentNode(Node node, int level) {
    for (int i = 0; i < level; ++i) {
      node = node.getParent();
    }
    return node;
  }

  /**
   * Return the Node in the network topology that corresponds to the hostname
   */
  public Node getNode(String name) {
    return hostnameToNodeMap.get(name);
  }
  public int getNumTaskCacheLevels() {
    return numTaskCacheLevels;
  }
  public int getNumResolvedTaskTrackers() {
    return numResolved;
  }

  public int getNumberOfUniqueHosts() {
    return uniqueHostsMap.size();
  }

  public void addJobInProgressListener(JobInProgressListener listener) {
    jobInProgressListeners.add(listener);
  }

  public void removeJobInProgressListener(JobInProgressListener listener) {
    jobInProgressListeners.remove(listener);
  }

  // Update the listeners about the job
  // Assuming JobTracker is locked on entry.
  void updateJobInProgressListeners(JobChangeEvent event) {
    for (JobInProgressListener listener : jobInProgressListeners) {
      listener.jobUpdated(event);
    }
  }

  /**
   * Return the {@link QueueManager} associated with the JobTracker.
   */
  public QueueManager getQueueManager() {
    return queueManager;
  }

  ////////////////////////////////////////////////////
  // InterTrackerProtocol
  ////////////////////////////////////////////////////

  public String getBuildVersion() throws IOException{
    return VersionInfo.getBuildVersion();
  }

  /**
   * The periodic heartbeat mechanism between the {@link TaskTracker} and
   * the {@link JobTracker}.
   *
   * The {@link JobTracker} processes the status information sent by the
   * {@link TaskTracker} and responds with instructions to start/stop
   * tasks or jobs, and also 'reset' instructions during contingencies.
   */
  public HeartbeatResponse heartbeat(TaskTrackerStatus status,
                                     boolean restarted,
                                     boolean initialContact,
                                     boolean acceptNewTasks,
                                     short responseId)
    throws IOException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Got heartbeat from: " + status.getTrackerName() +
                " (restarted: " + restarted +
                " initialContact: " + initialContact +
                " acceptNewTasks: " + acceptNewTasks + ")" +
                " with responseId: " + responseId);
    }

    short newResponseId;
    boolean shouldSchedule = false;
    TaskTrackerStatus taskTrackerStatus;
    String trackerName;

    synchronized (this) {

      // Make sure heartbeat is from a tasktracker allowed by the jobtracker.
      if (!acceptTaskTracker(status)) {
        throw new DisallowedTaskTrackerException(status);
      }

      // First check if the last heartbeat response got through
      trackerName = status.getTrackerName();
      long now = getClock().getTime();
      if (restarted) {
        faultyTrackers.markTrackerHealthy(status.getHost());
      } else {
        // This updates faulty tracker information.
        faultyTrackers.shouldAssignTasksToTracker(status.getHost(), now);
      }

      HeartbeatResponse prevHeartbeatResponse =
        trackerToHeartbeatResponseMap.get(trackerName);

      if (initialContact != true) {
        // If this isn't the 'initial contact' from the tasktracker,
        // there is something seriously wrong if the JobTracker has
        // no record of the 'previous heartbeat'; if so, ask the
        // tasktracker to re-initialize itself.
        if (prevHeartbeatResponse == null) {
          // Jobtracker might have restarted but no recovery is needed
          // otherwise this code should not be reached
          LOG.warn("Serious problem, cannot find record of 'previous' " +
              "heartbeat for '" + trackerName +
          "'; reinitializing the tasktracker");
          return new HeartbeatResponse(responseId,
              new TaskTrackerAction[] {new ReinitTrackerAction()});

        } else {

          // It is completely safe to not process a 'duplicate' heartbeat from a
          // {@link TaskTracker} since it resends the heartbeat when rpcs are
          // lost see {@link TaskTracker.transmitHeartbeat()};
          // acknowledge it by re-sending the previous response to let the
          // {@link TaskTracker} go forward.
          if (prevHeartbeatResponse.getResponseId() != responseId) {
            LOG.info("Ignoring 'duplicate' heartbeat from '" +
                     trackerName + "'; resending the previous 'lost' response");
            return prevHeartbeatResponse;
          }
        }
      }

      // Process this heartbeat
      newResponseId = (short)(responseId + 1);
      status.setLastSeen(now);
      if (!processHeartbeat(status, initialContact)) {
        if (prevHeartbeatResponse != null) {
          trackerToHeartbeatResponseMap.remove(trackerName);
        }
        return new HeartbeatResponse(newResponseId,
                                     new TaskTrackerAction[] {new ReinitTrackerAction()});
      }


      shouldSchedule = acceptNewTasks &&
        !faultyTrackers.isBlacklisted(status.getHost());

      taskTrackerStatus =
        shouldSchedule ? getTaskTrackerStatus(trackerName) : null;

    } // synchronized JobTracker

    // Initialize the response to be sent for the heartbeat
    HeartbeatResponse response = new HeartbeatResponse(newResponseId, null);
    List<TaskTrackerAction> actions = new ArrayList<TaskTrackerAction>();
    List<Task> setupCleanupTasks = null;

    // Check for setup/cleanup tasks to be executed on the tasktracker
    if (shouldSchedule) {
      if (taskTrackerStatus == null) {
        LOG.warn("Unknown task tracker polling; ignoring: " + trackerName);
      } else {
        setupCleanupTasks = getSetupAndCleanupTasks(taskTrackerStatus);
      }
    }

    synchronized (this) {

      // Check for tasks to be killed
      // we compute this first so that additional tasks can be scheduled
      // to compensate for the kill actions
      List<TaskTrackerAction> killTasksList = getTasksToKill(trackerName);
      if (killTasksList != null) {
        actions.addAll(killTasksList);
      }

      List<Task> tasks = null;

      // Check for map/reduce tasks to be executed on the tasktracker
      // ignore any contribution by setup/cleanup tasks - it's ok to try
      // and overschedule since setup/cleanup tasks are super fast
      if (taskTrackerStatus != null) {

        // This tells Scheduler how many MAP/REDUCE slots will be released after
        // heartbeat. So that the Scheduler can pre-schedule them.
        int mapsReleased = countSlotsReleased
          (killTasksList, setupCleanupTasks, taskTrackerStatus, TaskType.MAP);
        int reducesReleased = countSlotsReleased
          (killTasksList, setupCleanupTasks, taskTrackerStatus, TaskType.REDUCE);
        status.setMapsReleased(mapsReleased);
        status.setReducesReleased(reducesReleased);

        List<Task> assignedTasks = taskScheduler.assignTasks(taskTrackers.get(trackerName));

        if ((setupCleanupTasks != null) && (assignedTasks != null)) {
            // tasks is immutable. so merge the tasks and assignedTasks into a new list
            // make sure that the setup/cleanup tasks go first since we can be overscheduling
            // tasks here and we need to make sure that the setup/cleanup is run first
          tasks = new ArrayList<Task> (assignedTasks.size() +
                                       setupCleanupTasks.size());
          tasks.addAll(setupCleanupTasks);
          tasks.addAll(assignedTasks);
        } else {
          tasks = (setupCleanupTasks != null) ? setupCleanupTasks : assignedTasks;
        }
      }

      if (tasks != null) {
        for (Task task : tasks) {

          TaskAttemptID taskid = task.getTaskID();
          JobInProgress job = getJob(taskid.getJobID());

          if (job != null) {
            createTaskEntry (taskid, taskTrackerStatus.getTrackerName(),
                             job.getTaskInProgress(taskid.getTaskID()));
          } else {
            // because we do not hold the jobtracker lock throughout this
            // routine - there is a small chance that the job for the task
            // we are trying to schedule no longer exists. ignore such tasks
            LOG.warn("Unable to find job corresponding to task: " + taskid.toString());
          }

          expireLaunchingTasks.addNewTask(task.getTaskID());
          LOG.debug(trackerName + " -> LaunchTask: " + task.getTaskID());
          actions.add(new LaunchTaskAction(task));
        }
      }

      // Check for jobs to be killed/cleanedup
      List<TaskTrackerAction> killJobsList = getJobsForCleanup(trackerName);
      if (killJobsList != null) {
        actions.addAll(killJobsList);
      }

      // Check for tasks whose outputs can be saved
      List<TaskTrackerAction> commitTasksList = getTasksToSave(status);
      if (commitTasksList != null) {
        actions.addAll(commitTasksList);
      }

      // calculate next heartbeat interval and put in heartbeat response
      int nextInterval = getNextHeartbeatInterval();
      response.setHeartbeatInterval(nextInterval);
      response.setActions(
                          actions.toArray(new TaskTrackerAction[actions.size()]));

      // Update the trackerToHeartbeatResponseMap
      trackerToHeartbeatResponseMap.put(trackerName, response);

      // Done processing the hearbeat, now remove 'marked' tasks
      removeMarkedTasks(trackerName);

      return response;

    } // synchronized JobTracker
  }

  /**
   * Calculate how many MAP/REDUCE (non-setup/cleanup) slots will be released
   * right after the heartbeat
   *
   * @param killTasksList Kill actions the TT received by heartbeat
   * @param setupCleanupTasks SetupCleanup tasks TT received by heartbeat
   * @param taskTrackerStatus The status of the TT
   * @param type The type of the task (MAP/REDUCE)
   * @return Number of slots will be released
   */
  private int countSlotsReleased(List<TaskTrackerAction> killTasksList,
      Collection<Task> setupCleanupTasks, TaskTrackerStatus taskTrackerStatus,
      TaskType type) {
    Map<TaskAttemptID, TaskStatus.State> taskState =
      new HashMap<TaskAttemptID, TaskStatus.State>();

    if (taskTrackerStatus.getTaskReports() != null) {
      for (TaskStatus ts : taskTrackerStatus.getTaskReports()) {
        taskState.put(ts.getTaskID(), ts.getRunState());
      }
    }
    int released = 0;
    for (TaskTrackerAction action : killTasksList) {
      TaskAttemptID tid = ((KillTaskAction)action).getTaskID();
      if (type == TaskType.MAP && tid.isMap() ||
          type == TaskType.REDUCE && !tid.isMap()) {
        TaskStatus.State state = taskState.get(tid);
        if (TaskStatus.TERMINATING_STATES.contains(state)) {
          // If we are killing a tasks that's already failed, it doesn't count.
          continue;
        }
        released += 1;
      }
    }
    if (setupCleanupTasks != null) {
      released -= setupCleanupTasks.size();
    }
    return released;
  }

  /**
   * Calculates next heartbeat interval using cluster size.
   * Heartbeat interval is incremented by 1 second for every 100 nodes by default.
   * @return next heartbeat interval.
   */
  public int getNextHeartbeatInterval() {
    // get the no of task trackers
    int clusterSize = getClusterStatus().getTaskTrackers();
    int heartbeatInterval =  Math.max(
                                (int)(1000 * HEARTBEATS_SCALING_FACTOR *
                                      Math.ceil((double)clusterSize /
                                                NUM_HEARTBEATS_IN_SECOND)),
                                HEARTBEAT_INTERVAL_MIN) ;
    return heartbeatInterval;
  }

  /**
   * Return if the specified tasktracker is in the hosts list,
   * if one was configured.  If none was configured, then this
   * returns true.
   */
  private boolean inHostsList(TaskTrackerStatus status) {
    Set<String> hostsList = hostsReader.getHosts();
    return (hostsList.isEmpty() || hostsList.contains(status.getHost()));
  }

  /**
   * Return if the specified tasktracker is in the exclude list.
   */
  private boolean inExcludedHostsList(TaskTrackerStatus status) {
    Set<String> excludeList = hostsReader.getExcludedHosts();
    return excludeList.contains(status.getHost());
  }

  /**
   * Returns true if the tasktracker is in the hosts list and
   * not in the exclude list.
   */
  private boolean acceptTaskTracker(TaskTrackerStatus status) {
    return (inHostsList(status) && !inExcludedHostsList(status));
  }

  /**
   * Update the last recorded status for the given task tracker.
   * It assumes that the taskTrackers are locked on entry.
   * @param trackerName The name of the tracker
   * @param status The new status for the task tracker
   * @return Was an old status found?
   */
  boolean updateTaskTrackerStatus(String trackerName,
                                  TaskTrackerStatus status) {
    TaskTracker tt = getTaskTracker(trackerName);
    TaskTrackerStatus oldStatus = (tt == null) ? null : tt.getStatus();

    // update the total cluster capacity first

    if (status != null) {
      // we have a fresh tasktracker status
      if (!faultyTrackers.isBlacklisted(status.getHost())) {
        // if the task tracker host is not blacklisted - then
        // we update the cluster capacity with the capacity
        // reported by the tasktracker
        updateTotalTaskCapacity(status);
      } else {
        // if the tasktracker is blacklisted - then it's capacity
        // is already removed and will only be added back to the
        // cluster capacity when it's unblacklisted
      }
    } else {
      if (oldStatus != null) {
        // old status exists - but no new status. in this case
        // we are removing the tracker from the cluster.

        if (!faultyTrackers.isBlacklisted(oldStatus.getHost())) {

          // we update the total task capacity based on the old status
          // this seems redundant - but this call is idempotent - so just
          // make it. the danger of not making it is that we may accidentally
          // remove something we never had
          updateTotalTaskCapacity(oldStatus);
          removeTaskTrackerCapacity(oldStatus);

        } else {
          // if the host is blacklisted - then the tracker's capacity
          // has already been removed and there's nothing to do
        }
      }
    }


    if (oldStatus != null) {
      totalMaps -= oldStatus.countMapTasks();
      totalReduces -= oldStatus.countReduceTasks();
      occupiedMapSlots -= oldStatus.countOccupiedMapSlots();
      occupiedReduceSlots -= oldStatus.countOccupiedReduceSlots();
      getInstrumentation().decRunningMaps(oldStatus.countMapTasks());
      getInstrumentation().decRunningReduces(oldStatus.countReduceTasks());
      getInstrumentation().decOccupiedMapSlots(oldStatus.countOccupiedMapSlots());
      getInstrumentation().decOccupiedReduceSlots(oldStatus.countOccupiedReduceSlots());
      if (status == null) {
        taskTrackers.remove(trackerName);
        Integer numTaskTrackersInHost =
          uniqueHostsMap.get(oldStatus.getHost());
        if (numTaskTrackersInHost != null) {
          numTaskTrackersInHost --;
          if (numTaskTrackersInHost > 0)  {
            uniqueHostsMap.put(oldStatus.getHost(), numTaskTrackersInHost);
          }
          else {
            uniqueHostsMap.remove(oldStatus.getHost());
          }
        }
      }
    }
    if (status != null) {
      totalMaps += status.countMapTasks();
      totalReduces += status.countReduceTasks();
      occupiedMapSlots += status.countOccupiedMapSlots();
      occupiedReduceSlots += status.countOccupiedReduceSlots();
      getInstrumentation().addRunningMaps(status.countMapTasks());
      getInstrumentation().addRunningReduces(status.countReduceTasks());
      getInstrumentation().addOccupiedMapSlots(status.countOccupiedMapSlots());
      getInstrumentation().addOccupiedReduceSlots(status.countOccupiedReduceSlots());

      boolean alreadyPresent = false;
      TaskTracker taskTracker = taskTrackers.get(trackerName);
      if (taskTracker != null) {
        alreadyPresent = true;
      } else {
        taskTracker = new TaskTracker(trackerName);
      }

      taskTracker.setStatus(status);
      taskTrackers.put(trackerName, taskTracker);

      if (LOG.isDebugEnabled()) {
        int runningMaps = 0, runningReduces = 0;
        int commitPendingMaps = 0, commitPendingReduces = 0;
        int unassignedMaps = 0, unassignedReduces = 0;
        int miscMaps = 0, miscReduces = 0;
        List<TaskStatus> taskReports = status.getTaskReports();
        for (Iterator<TaskStatus> it = taskReports.iterator(); it.hasNext();) {
          TaskStatus ts = (TaskStatus) it.next();
          boolean isMap = ts.getIsMap();
          TaskStatus.State state = ts.getRunState();
          if (state == TaskStatus.State.RUNNING) {
            if (isMap) { ++runningMaps; }
            else { ++runningReduces; }
          } else if (state == TaskStatus.State.UNASSIGNED) {
            if (isMap) { ++unassignedMaps; }
            else { ++unassignedReduces; }
          } else if (state == TaskStatus.State.COMMIT_PENDING) {
            if (isMap) { ++commitPendingMaps; }
            else { ++commitPendingReduces; }
          } else {
            if (isMap) { ++miscMaps; }
            else { ++miscReduces; }
          }
        }
        LOG.debug(trackerName + ": Status -" +
                  " running(m) = " + runningMaps +
                  " unassigned(m) = " + unassignedMaps +
                  " commit_pending(m) = " + commitPendingMaps +
                  " misc(m) = " + miscMaps +
                  " running(r) = " + runningReduces +
                  " unassigned(r) = " + unassignedReduces +
                  " commit_pending(r) = " + commitPendingReduces +
                  " misc(r) = " + miscReduces);
      }

      if (!alreadyPresent)  {
        Integer numTaskTrackersInHost =
          uniqueHostsMap.get(status.getHost());
        if (numTaskTrackersInHost == null) {
          numTaskTrackersInHost = 0;
        }
        numTaskTrackersInHost ++;
        uniqueHostsMap.put(status.getHost(), numTaskTrackersInHost);
      }
    }
    getInstrumentation().setMapSlots(totalMapTaskCapacity);
    getInstrumentation().setReduceSlots(totalReduceTaskCapacity);
    return oldStatus != null;
  }

  // Increment the number of reserved slots in the cluster.
  // This method assumes the caller has JobTracker lock.
  void incrementReservations(TaskType type, int reservedSlots) {
    if (type.equals(TaskType.MAP)) {
      reservedMapSlots += reservedSlots;
    } else if (type.equals(TaskType.REDUCE)) {
      reservedReduceSlots += reservedSlots;
    }
  }

  // Decrement the number of reserved slots in the cluster.
  // This method assumes the caller has JobTracker lock.
  void decrementReservations(TaskType type, int reservedSlots) {
    if (type.equals(TaskType.MAP)) {
      reservedMapSlots -= reservedSlots;
    } else if (type.equals(TaskType.REDUCE)) {
      reservedReduceSlots -= reservedSlots;
    }
  }

  private void updateNodeHealthStatus(TaskTrackerStatus trackerStatus) {
    TaskTrackerHealthStatus status = trackerStatus.getHealthStatus();
    synchronized (faultyTrackers) {
      faultyTrackers.setNodeHealthStatus(trackerStatus.getHost(),
          status.isNodeHealthy(), status.getHealthReport());
    }
  }

  /**
   * Process incoming heartbeat messages from the task trackers.
   */
  synchronized boolean processHeartbeat(
                                 TaskTrackerStatus trackerStatus,
                                 boolean initialContact) {
    String trackerName = trackerStatus.getTrackerName();

    synchronized (taskTrackers) {
      synchronized (trackerExpiryQueue) {
        boolean seenBefore = updateTaskTrackerStatus(trackerName,
                                                     trackerStatus);
        TaskTracker taskTracker = getTaskTracker(trackerName);
        if (initialContact) {
          // If it's first contact, then clear out
          // any state hanging around
          if (seenBefore) {
            LOG.warn("initialContact but seenBefore from Tracker : " + trackerName);
            lostTaskTracker(taskTracker);
          }
        } else {
          // If not first contact, there should be some record of the tracker
          if (!seenBefore) {
            LOG.warn("Status from unknown Tracker : " + trackerName);
            updateTaskTrackerStatus(trackerName, null);
            return false;
          }
        }

        if (initialContact) {
          // if this is lost tracker that came back now, and if it blacklisted
          // increment the count of blacklisted trackers in the cluster
          if (isBlacklisted(trackerName)) {
            faultyTrackers.incrBlackListedTrackers(1);
          }
          addNewTracker(taskTracker);
        }
      }
    }

    updateTaskStatuses(trackerStatus);
    updateNodeHealthStatus(trackerStatus);

    return true;
  }

  /**
   * A tracker wants to know if any of its Tasks have been
   * closed (because the job completed, whether successfully or not)
   */
  synchronized List<TaskTrackerAction> getTasksToKill(String taskTracker) {

    Set<TaskAttemptIDWithTip> taskset = trackerToTaskMap.get(taskTracker);
    List<TaskTrackerAction> killList = new ArrayList<TaskTrackerAction>();
    if (taskset != null) {
      for (TaskAttemptIDWithTip onetask : taskset) {
        TaskAttemptID killTaskId = onetask.attemptId;
        TaskInProgress tip = onetask.tip;

        if (tip == null) {
          continue;
        }
        if (tip.shouldClose(killTaskId)) {
          //
          // This is how the JobTracker ends a task at the TaskTracker.
          // It may be successfully completed, or may be killed in
          // mid-execution.
          //
          if (!((JobInProgress)tip.getJob()).isComplete()) {
            killList.add(new KillTaskAction(killTaskId));
            LOG.debug(taskTracker + " -> KillTaskAction: " + killTaskId);
          }
        }
      }
    }

    // add the stray attempts for uninited jobs
    synchronized (trackerToTasksToCleanup) {
      Set<TaskAttemptID> set = trackerToTasksToCleanup.remove(taskTracker);
      if (set != null) {
        for (TaskAttemptID id : set) {
          killList.add(new KillTaskAction(id));
        }
      }
    }
    return killList;
  }

  /**
   * Add a job to cleanup for the tracker.
   */
  private void addJobForCleanup(JobID id) {
    for (String taskTracker : taskTrackers.keySet()) {
      LOG.debug("Marking job " + id + " for cleanup by tracker " + taskTracker);
      synchronized (trackerToJobsToCleanup) {
        Set<JobID> jobsToKill = trackerToJobsToCleanup.get(taskTracker);
        if (jobsToKill == null) {
          jobsToKill = new HashSet<JobID>();
          trackerToJobsToCleanup.put(taskTracker, jobsToKill);
        }
        jobsToKill.add(id);
      }
    }
  }

  /**
   * A tracker wants to know if any job needs cleanup because the job completed.
   */
  private List<TaskTrackerAction> getJobsForCleanup(String taskTracker) {
    Set<JobID> jobs = null;
    synchronized (trackerToJobsToCleanup) {
      jobs = trackerToJobsToCleanup.remove(taskTracker);
    }
    if (jobs != null) {
      // prepare the actions list
      List<TaskTrackerAction> killList = new ArrayList<TaskTrackerAction>();
      for (JobID killJobId : jobs) {
        killList.add(new KillJobAction(killJobId));
        LOG.debug(taskTracker + " -> KillJobAction: " + killJobId);
      }

      return killList;
    }
    return null;
  }

  /**
   * A tracker wants to know if any of its Tasks can be committed
   */
  synchronized List<TaskTrackerAction> getTasksToSave(
                                                 TaskTrackerStatus tts) {
    List<TaskStatus> taskStatuses = tts.getTaskReports();
    if (taskStatuses != null) {
      List<TaskTrackerAction> saveList = new ArrayList<TaskTrackerAction>();
      for (TaskStatus taskStatus : taskStatuses) {
        if (taskStatus.getRunState() == TaskStatus.State.COMMIT_PENDING) {
          TaskAttemptID taskId = taskStatus.getTaskID();
          TaskInProgress tip = taskidToTIPMap.get(taskId);
          if (tip == null) {
            continue;
          }
          if (tip.shouldCommit(taskId)) {
            saveList.add(new CommitTaskAction(taskId));
            LOG.info(tts.getTrackerName() +
                      " -> CommitTaskAction: " + taskId);
          }
        }
      }
      return saveList;
    }
    return null;
  }

  // returns cleanup tasks first, then setup tasks.
  List<Task> getSetupAndCleanupTasks(
    TaskTrackerStatus taskTracker) throws IOException {
    int maxMapTasks = taskScheduler.getMaxSlots(taskTracker, TaskType.MAP);
    int maxReduceTasks =
        taskScheduler.getMaxSlots(taskTracker, TaskType.REDUCE);
    int numMaps = taskTracker.countOccupiedMapSlots();
    int numReduces = taskTracker.countOccupiedReduceSlots();
    int numTaskTrackers = getClusterStatus().getTaskTrackers();
    int numUniqueHosts = getNumberOfUniqueHosts();

    List<JobInProgress> cachedJobs = new ArrayList<JobInProgress> ();

    // get a snapshot of all the jobs in the system
    synchronized (jobs) {
      cachedJobs.addAll(jobs.values());
    }

    Task t = null;

    if (numMaps < maxMapTasks) {
      for (JobInProgress job: cachedJobs) {
        t = job.obtainJobCleanupTask(taskTracker, numTaskTrackers,
                                     numUniqueHosts, true);
        if (t != null) {
          return Collections.singletonList(t);
        }
      }
      for (JobInProgress job: cachedJobs) {
        t = job.obtainTaskCleanupTask(taskTracker, true);
        if (t != null) {
          return Collections.singletonList(t);
        }
      }
      for (JobInProgress job: cachedJobs) {
        t = job.obtainJobSetupTask(taskTracker, numTaskTrackers,
                                   numUniqueHosts, true);
        if (t != null) {
          return Collections.singletonList(t);
        }
      }
    }
    if (numReduces < maxReduceTasks) {
      for (JobInProgress job: cachedJobs) {
        t = job.obtainJobCleanupTask(taskTracker, numTaskTrackers,
                                     numUniqueHosts, false);
        if (t != null) {
          return Collections.singletonList(t);
        }
      }
      for (JobInProgress job: cachedJobs) {
        t = job.obtainTaskCleanupTask(taskTracker, false);
        if (t != null) {
          return Collections.singletonList(t);
        }
      }
      for (JobInProgress job: cachedJobs) {
        t = job.obtainJobSetupTask(taskTracker, numTaskTrackers,
                                   numUniqueHosts, false);
        if (t != null) {
          return Collections.singletonList(t);
        }
      }
    }

    return null;
  }

  /**
   * Grab the local fs name
   */
  public synchronized String getFilesystemName() throws IOException {
    if (fs == null) {
      throw new IllegalStateException("FileSystem object not available yet");
    }
    return fs.getUri().toString();
  }


  public void reportTaskTrackerError(String taskTracker,
                                     String errorClass,
                                     String errorMessage) throws IOException {
    LOG.warn("Report from " + taskTracker + ": " + errorMessage);
  }

  /**
   * Remove the job_ from jobids to get the unique string.
   */
  static String getJobUniqueString(String jobid) {
    return jobid.substring(4);
  }

  ////////////////////////////////////////////////////
  // JobSubmissionProtocol
  ////////////////////////////////////////////////////

  /**
   * Allocates a new JobId string.
   */
  public JobID getNewJobId() throws IOException {
    JobID id = new JobID(getTrackerIdentifier(), nextJobId.getAndIncrement());

    // get the user group info
    UserGroupInformation ugi = UserGroupInformation.getCurrentUGI();

    // mark the user for this id
    jobToUserMap.put(id, ugi.getUserName());

    LOG.info("Job id " + id + " assigned to user " + ugi.getUserName());

    return id;
  }

  private File persistUserName(JobID jobId, UserGroupInformation ugi)
          throws IOException {
    // persist
    File userFileForJob = new File(
            lDirAlloc.getLocalPathForWrite(
            SUBDIR + "/" + jobId, conf).toString());
    if (userFileForJob == null) {
      LOG.info("Failed to create job-id file for job " +
              jobId + " at " + userFileForJob);
    } else {
      FileOutputStream fout = new FileOutputStream(userFileForJob);
      BufferedWriter writer = null;
      try {
        writer = new BufferedWriter(new OutputStreamWriter(fout));
        writer.write(ugi.getUserName() + "\n");
      } finally {
        if (writer != null) {
          writer.close();
        }
        fout.close();
      }
      LOG.info("Job " + jobId +
              " user info persisted to file : " + userFileForJob);
    }
    return userFileForJob;
  }

  /**
   * JobTracker.submitJob() kicks off a new job.
   *
   * Create a 'JobInProgress' object, which contains both JobProfile
   * and JobStatus.  Those two sub-objects are sometimes shipped outside
   * of the JobTracker.  But JobInProgress adds info that's useful for
   * the JobTracker alone.
   */
  public JobStatus submitJob(JobID jobId) throws IOException {
    final UserGroupInformation ugi;
    final Path jobDir;
    synchronized (this) {

      if (jobs.containsKey(jobId)) {
        //job already running, don't start twice
        return jobs.get(jobId).getStatus();
      }

      // check if the owner is uploding the splits or not
      // get the user group info
      ugi = UserGroupInformation.getCurrentUGI();

      // check if the user invoking this api is the owner of this job
      if (!jobToUserMap.get(jobId).equals(ugi.getUserName())) {
        throw new IOException("User " + ugi.getUserName() +
            " is not the owner of the job " + jobId);
      }

      jobDir = this.getSystemDirectoryForJob(jobId);
      jobToUserMap.remove(jobId);
    }

    File userFileForJob = persistUserName(jobId, ugi);
    JobInProgress.copyJobFileLocally(jobDir, jobId, this.conf);

    synchronized (this) {
      JobInProgress job = null;
      try {
        job = new JobInProgress(jobId, this,
          jobTrackerReconfigurable.getJobConf(), ugi.getUserName(), 0);
      } catch (Exception e) {
        if (userFileForJob != null) {
          userFileForJob.delete();
        }
        throw new IOException(e);
      }

      String queue = job.getProfile().getQueueName();
      if (!(queueManager.getQueues().contains(queue))) {
        new CleanupQueue().addToQueue(new PathDeletionContext(
            FileSystem.get(conf),
            getSystemDirectoryForJob(jobId).toUri().getPath()));
        job.fail();
        if (userFileForJob != null) {
          userFileForJob.delete();
        }
        throw new IOException("Queue \"" + queue + "\" does not exist");
      }

      // The task scheduler should validate the job configuration
      taskScheduler.checkJob(job);

      // check for access
      try {
        checkAccess(job, QueueManager.QueueOperation.SUBMIT_JOB);
      } catch (IOException ioe) {
        LOG.warn("Access denied for user " + job.getJobConf().getUser() +
            ". Ignoring job " + jobId, ioe);
        job.fail();
        if (userFileForJob != null) {
          userFileForJob.delete();
        }
        new CleanupQueue().addToQueue(new PathDeletionContext(
            FileSystem.get(conf),
            getSystemDirectoryForJob(jobId).toUri().getPath()));
        throw ioe;
      }

      // Check the job if it cannot run in the cluster because of invalid memory
      // requirements.
      try {
        checkMemoryRequirements(job);
      } catch (IOException ioe) {
        new CleanupQueue().addToQueue(new PathDeletionContext(
            FileSystem.get(conf),
            getSystemDirectoryForJob(jobId).toUri().getPath()));
        throw ioe;
      }

      return addJob(jobId, job);
    }
  }

  /**
   * Adds a job to the jobtracker. Make sure that the checks are inplace before
   * adding a job. This is the core job submission logic
   * @param jobId The id for the job submitted which needs to be added
   */
  protected synchronized JobStatus addJob(JobID jobId, JobInProgress job) {
    totalSubmissions++;

    synchronized (jobs) {
      synchronized (taskScheduler) {
        jobs.put(job.getProfile().getJobID(), job);
        for (JobInProgressListener listener : jobInProgressListeners) {
          try {
            listener.jobAdded(job);
          } catch (IOException ioe) {
            LOG.warn("Failed to add and so skipping the job : "
                + job.getJobID() + ". Exception : " + ioe);
          }
        }
      }
    }
    myInstrumentation.submitJob(job.getJobConf(), jobId);
    String jobName = job.getJobConf().getJobName();
    int jobNameLen = 64;
    if (jobName.length() > jobNameLen) {
      jobName = jobName.substring(0, jobNameLen); // Truncate for logging.
    }
    LOG.info("Job " + jobId + "(" + jobName +
             ") added successfully for user '" + job.getJobConf().getUser() +
             "' to queue '" + job.getJobConf().getQueueName() + "'" +
             ", source " + job.getJobConf().getJobSource());
    return job.getStatus();
  }

  // Check whether the specified operation can be performed
  // related to the job.
  private void checkAccess(JobInProgress job,
                                QueueManager.QueueOperation oper)
                                  throws IOException {
    // get the user group info
    UserGroupInformation ugi = UserGroupInformation.getCurrentUGI();
    checkAccess(job, oper, ugi);
  }

  // use the passed ugi for checking the access
  private void checkAccess(JobInProgress job, QueueManager.QueueOperation oper,
                           UserGroupInformation ugi) throws IOException {
    // get the queue
    String queue = job.getProfile().getQueueName();
    if (!queueManager.hasAccess(queue, job, oper, ugi)) {
      throw new AccessControlException("User "
                            + ugi.getUserName()
                            + " cannot perform "
                            + "operation " + oper + " on queue " + queue +
                            ".\n Please run \"hadoop queue -showacls\" " +
                            "command to find the queues you have access" +
                            " to .");
    }
  }

  /**@deprecated use {@link #getClusterStatus(boolean)}*/
  @Deprecated
  public synchronized ClusterStatus getClusterStatus() {
    return getClusterStatus(false);
  }

  public synchronized ClusterStatus getClusterStatus(boolean detailed) {
    synchronized (taskTrackers) {
      if (detailed) {
        List<List<String>> trackerNames = taskTrackerNames();
        return new ClusterStatus(trackerNames.get(0),
            trackerNames.get(1),
            taskTrackers(),
            getRunningJobs(),
            TASKTRACKER_EXPIRY_INTERVAL,
            totalMaps,
            totalReduces,
            totalMapTaskCapacity,
            totalReduceTaskCapacity,
            state, getExcludedNodes().size()
            );
      } else {
        return new ClusterStatus(taskTrackers.size() -
            getBlacklistedTrackerCount(),
            getBlacklistedTrackerCount(),
            TASKTRACKER_EXPIRY_INTERVAL,
            totalMaps,
            totalReduces,
            totalMapTaskCapacity,
            totalReduceTaskCapacity,
            state, getExcludedNodes().size());
      }
    }
  }

  public synchronized ClusterMetrics getClusterMetrics() {
    return new ClusterMetrics(totalMaps,
      totalReduces, occupiedMapSlots, occupiedReduceSlots,
      reservedMapSlots, reservedReduceSlots,
      totalMapTaskCapacity, totalReduceTaskCapacity,
      totalSubmissions,
      taskTrackers.size() - getBlacklistedTrackerCount(),
      getBlacklistedTrackerCount(), getExcludedNodes().size()) ;
  }

  public synchronized void killJob(JobID jobid) throws IOException {
    if (null == jobid) {
      LOG.info("Null jobid object sent to JobTracker.killJob()");
      return;
    }

    JobInProgress job = jobs.get(jobid);

    if (null == job) {
      LOG.info("killJob(): JobId " + jobid.toString() + " is not a valid job");
      return;
    }

    checkAccess(job, QueueManager.QueueOperation.ADMINISTER_JOBS);
    killJob(job);
  }

  private synchronized void killJob(JobInProgress job) {
    LOG.info("Killing job " + job.getJobID());
    JobStatus prevStatus = (JobStatus)job.getStatus().clone();
    job.kill();

    // Inform the listeners if the job is killed
    // Note :
    //   If the job is killed in the PREP state then the listeners will be
    //   invoked
    //   If the job is killed in the RUNNING state then cleanup tasks will be
    //   launched and the updateTaskStatuses() will take care of it
    JobStatus newStatus = (JobStatus)job.getStatus().clone();
    if (prevStatus.getRunState() != newStatus.getRunState()
        && newStatus.getRunState() == JobStatus.KILLED) {
      JobStatusChangeEvent event =
        new JobStatusChangeEvent(job, EventType.RUN_STATE_CHANGED, prevStatus,
            newStatus);
      updateJobInProgressListeners(event);
    }
  }

  public void initJob(JobInProgress job) {
    if (null == job) {
      LOG.info("Init on null job is not valid");
      return;
    }

    try {
      JobStatus prevStatus = (JobStatus)job.getStatus().clone();
      LOG.info("Initializing " + job.getJobID());
      job.initTasks();

      // Here the job *should* be in the PREP state.
      // From here there are 3 ways :
      //  - job requires setup : the job remains in PREP state and
      //    setup is launched to move the job in RUNNING state
      //  - job is complete (no setup required and no tasks) : complete
      //    the job and move it to SUCCEEDED
      //  - job has tasks but doesnt require setup : make the job RUNNING.
      if (job.isJobEmpty()) { // is the job empty?
        completeEmptyJob(job); // complete it
      } else if (!job.isSetupCleanupRequired()) { // setup/cleanup not required
        job.completeSetup(); // complete setup and make job running
      }

      // Inform the listeners if the job state has changed
      // Note : that the job will be in PREP state.
      JobStatus newStatus = (JobStatus)job.getStatus().clone();
      if (prevStatus.getRunState() != newStatus.getRunState()) {
        JobStatusChangeEvent event =
          new JobStatusChangeEvent(job, EventType.RUN_STATE_CHANGED, prevStatus,
              newStatus);
        synchronized (JobTracker.this) {
          updateJobInProgressListeners(event);
        }
      }
    } catch (KillInterruptedException kie) {
      //   If job was killed during initialization, job state will be KILLED
      LOG.error(job.getJobID() + ": Job initialization interrupted:\n" +
          StringUtils.stringifyException(kie));
      killJob(job);
    } catch (Throwable t) {
      // If the job initialization is failed, job state will be FAILED
      LOG.error(job.getJobID() + ": Job initialization failed:\n" +
          StringUtils.stringifyException(t));
      failJob(job);
    }
	 }

  private synchronized void completeEmptyJob(JobInProgress job) {
    job.completeEmptyJob();
  }

  /**
   * Fail a job and inform the listeners. Other components in the framework
   * should use this to fail a job.
   */
  public synchronized void failJob(JobInProgress job) {
    if (null == job) {
      LOG.info("Fail on null job is not valid");
      return;
    }

    JobStatus prevStatus = (JobStatus)job.getStatus().clone();
    LOG.info("Failing job " + job.getJobID());
    job.fail();

    // Inform the listeners if the job state has changed
    JobStatus newStatus = (JobStatus)job.getStatus().clone();
    if (prevStatus.getRunState() != newStatus.getRunState()) {
      JobStatusChangeEvent event =
        new JobStatusChangeEvent(job, EventType.RUN_STATE_CHANGED, prevStatus,
            newStatus);
      updateJobInProgressListeners(event);
    }
  }

  /**
   * Set the priority of a job
   * @param jobid id of the job
   * @param priority new priority of the job
   */
  public synchronized void setJobPriority(JobID jobid,
                                              String priority)
                                                throws IOException {
    JobInProgress job = jobs.get(jobid);
    if (null == job) {
        LOG.info("setJobPriority(): JobId " + jobid.toString()
            + " is not a valid job");
        return;
    }
    checkAccess(job, QueueManager.QueueOperation.ADMINISTER_JOBS);
    JobPriority newPriority = JobPriority.valueOf(priority);
    setJobPriority(jobid, newPriority);
  }

  void storeCompletedJob(JobInProgress job) {
    //persists the job info in DFS
    completedJobStatusStore.store(job);
  }

  /**
   * Check if the <code>job</code> has been initialized.
   *
   * @param job {@link JobInProgress} to be checked
   * @return <code>true</code> if the job has been initialized,
   *         <code>false</code> otherwise
   */
  private boolean isJobInited(JobInProgress job) {
    return job.inited();
  }

  public JobProfile getJobProfile(JobID jobid) {
    synchronized (this) {
      JobInProgress job = jobs.get(jobid);
      if (job != null) {
        // Safe to call JobInProgress.getProfile while holding the lock
        // on the JobTracker since it isn't a synchronized method
        return job.getProfile();
      }  else {
        RetireJobInfo info = retireJobs.get(jobid);
        if (info != null) {
          return info.profile;
        }
      }
    }
    return completedJobStatusStore.readJobProfile(jobid);
  }
  public JobStatus getJobStatus(JobID jobid) {
    if (null == jobid) {
      LOG.warn("JobTracker.getJobStatus() cannot get status for null jobid");
      return null;
    }
    synchronized (this) {
      JobInProgress job = jobs.get(jobid);
      if (job != null) {
        // Safe to call JobInProgress.getStatus while holding the lock
        // on the JobTracker since it isn't a synchronized method
        JobStatus stats = job.getStatus();
        if (stats == null) {
          LOG.warn("JobTracker.getJobStatus() returning null for jobid " +
              jobid + " after non-null job");
        }
        return stats;
      } else {

        RetireJobInfo info = retireJobs.get(jobid);
        if (info != null) {
          if (info.status == null) {
            LOG.warn("JobTracker.getJobStatus() returning null for jobid " +
                jobid + " after retrieving info from retireJobs");
          }
          return info.status;
        }
      }
    }
    JobStatus stats = completedJobStatusStore.readJobStatus(jobid);
    if (stats == null) {
      LOG.warn("JobTracker.getJobStatus() returning null for jobid " + jobid +
          " after getting status from completedJobStatusStore");
    }
    return stats;
  }
  private static final Counters EMPTY_COUNTERS
      = new Counters();

  public Counters getJobCounters(JobID jobid) {
    JobInProgress job;
    synchronized (this) {
      job = jobs.get(jobid);
    }
    if (job != null) {
      if (!isJobInited(job)) {
        return EMPTY_COUNTERS;
      }
      return job.getCounters();
    }
    return completedJobStatusStore.readCounters(jobid);
  }

  public TaskReport[] getMapTaskReports(JobID jobid) {
    return super.getMapTaskReportsImpl(jobid);
  }


  public TaskReport[] getReduceTaskReports(JobID jobid) {
    return super.getReduceTaskReportsImpl(jobid);
  }

  public TaskReport[] getCleanupTaskReports(JobID jobid) {
    return super.getCleanupTaskReportsImpl(jobid);
  }

  public TaskReport[] getSetupTaskReports(JobID jobid) {
    return super.getSetupTaskReportsImpl(jobid);
  }

  static final String MAPRED_CLUSTER_MAP_MEMORY_MB_PROPERTY =
      "mapred.cluster.map.memory.mb";
  static final String MAPRED_CLUSTER_REDUCE_MEMORY_MB_PROPERTY =
      "mapred.cluster.reduce.memory.mb";

  static final String MAPRED_CLUSTER_MAX_MAP_MEMORY_MB_PROPERTY =
      "mapred.cluster.max.map.memory.mb";
  static final String MAPRED_CLUSTER_MAX_REDUCE_MEMORY_MB_PROPERTY =
      "mapred.cluster.max.reduce.memory.mb";

  /*
   * Returns a list of TaskCompletionEvent for the given job,
   * starting from fromEventId.
   * @see org.apache.hadoop.mapred.JobSubmissionProtocol#getTaskCompletionEvents(java.lang.String, int, int)
   */
  public TaskCompletionEvent[] getTaskCompletionEvents(
    JobID jobid, int fromEventId, int maxEvents) throws IOException{

    JobInProgress job = this.jobs.get(jobid);
    if (null != job) {
      return job.inited() ? job.getTaskCompletionEvents(fromEventId, maxEvents)
         : TaskCompletionEvent.EMPTY_ARRAY;
    }

    return completedJobStatusStore.readJobTaskCompletionEvents(jobid, fromEventId, maxEvents);
  }

  public synchronized String[] getTaskDiagnostics(TaskAttemptID taskId)
    throws IOException {
    return super.getTaskDiagnosticsImpl(taskId);
  }


  /** Get all the TaskStatuses from the tipid. */
  TaskStatus[] getTaskStatuses(TaskID tipid) {
    TaskInProgress tip = getTip(tipid);
    return (tip == null ? new TaskStatus[0]
            : tip.getTaskStatuses());
  }

  /** Returns the TaskStatus for a particular taskid. */
  TaskStatus getTaskStatus(TaskAttemptID taskid) {
    TaskInProgress tip = getTip(taskid.getTaskID());
    return (tip == null ? null
            : tip.getTaskStatus(taskid));
  }

  /**
   * Returns the counters for the specified task in progress.
   */
  Counters getTipCounters(TaskID tipid) {
    TaskInProgress tip = getTip(tipid);
    return (tip == null ? null : tip.getCounters());
  }

  /**
   * Returns the configured task scheduler for this job tracker.
   * @return the configured task scheduler
   */
  TaskScheduler getTaskScheduler() {
    return taskScheduler;
  }


  @Override
  // This is for JobSubmissionProtocol
  public boolean killTask(TaskAttemptID taskid, boolean shouldFail) throws IOException{
    return this.killTask(taskid, shouldFail,
        "Request received to " + (shouldFail ? "fail" : "kill")
        + " task '" + taskid + "' from JobClient" );
  }

  @Override
  // This is for TaskTrackerManager
  public synchronized boolean killTask(
      TaskAttemptID taskid, boolean shouldFail, String reason) throws IOException{
    TaskInProgress tip = taskidToTIPMap.get(taskid);
    if(tip != null) {
      checkAccess((JobInProgress)tip.getJob(), QueueManager.QueueOperation.ADMINISTER_JOBS);
      return tip.killTask(taskid, shouldFail, reason);
    }
    else {
      LOG.info("Kill task attempt failed since task " + taskid + " was not found");
      return false;
    }
  }

  /**
   * Get tracker name for a given task id.
   * @param taskId the name of the task
   * @return The name of the task tracker
   */
  public synchronized String getAssignedTracker(TaskAttemptID taskId) {
    return taskidToTrackerMap.get(taskId);
  }

  public JobStatus[] jobsToComplete() {
    return getJobStatus(jobs.values(), true);
  }

  public JobStatus[] getAllJobs() {
    List<JobStatus> list = new ArrayList<JobStatus>();
    list.addAll(Arrays.asList(getJobStatus(jobs.values(),false)));
    list.addAll(retireJobs.getAllJobStatus());
    return list.toArray(new JobStatus[list.size()]);
  }

  /**
   * @see org.apache.hadoop.mapred.JobSubmissionProtocol#getSystemDir()
   */
  public String getSystemDir() {
    Path sysDir = new Path(conf.get(MAPRED_SYSTEM_DIR_KEY, DEFAULT_MAPRED_SYSTEM_DIR));
    return fs.makeQualified(sysDir).toString();
  }

  ///////////////////////////////////////////////////////////////
  // JobTracker methods
  ///////////////////////////////////////////////////////////////
  public JobInProgress getJob(JobID jobid) {
    return jobs.get(jobid);
  }

  public JobInProgressTraits getJobInProgress(JobID jobid) {
    return (JobInProgressTraits)jobs.get(jobid);
  }

  // Get the job directory in system directory
  Path getSystemDirectoryForJob(JobID id) {
    return new Path(getSystemDir(), id.toString());
  }

  /**
   * Change the run-time priority of the given job.
   * @param jobId job id
   * @param priority new {@link JobPriority} for the job
   */
  synchronized void setJobPriority(JobID jobId, JobPriority priority) {
    JobInProgress job = jobs.get(jobId);
    if (job != null) {
      synchronized (taskScheduler) {
        JobStatus oldStatus = (JobStatus)job.getStatus().clone();
        job.setPriority(priority);
        JobStatus newStatus = (JobStatus)job.getStatus().clone();
        JobStatusChangeEvent event =
          new JobStatusChangeEvent(job, EventType.PRIORITY_CHANGED, oldStatus,
                                   newStatus);
        updateJobInProgressListeners(event);
      }
    } else {
      LOG.warn("Trying to change the priority of an unknown job: " + jobId);
    }
  }

  ////////////////////////////////////////////////////
  // Methods to track all the TaskTrackers
  ////////////////////////////////////////////////////
  /**
   * Accept and process a new TaskTracker profile.  We might
   * have known about the TaskTracker previously, or it might
   * be brand-new.  All task-tracker structures have already
   * been updated.  Just process the contained tasks and any
   * jobs that might be affected.
   */
  void updateTaskStatuses(TaskTrackerStatus status) {
    String trackerName = status.getTrackerName();
    for (TaskStatus report : status.getTaskReports()) {
      report.setTaskTracker(trackerName);
      TaskAttemptID taskId = report.getTaskID();

      // Remove it from the expired task list
      if (report.getRunState() != TaskStatus.State.UNASSIGNED) {
        expireLaunchingTasks.removeTask(taskId);
      }

      JobInProgress job = getJob(taskId.getJobID());
      if (job == null) {
        // if job is not there in the cleanup list ... add it
        synchronized (trackerToJobsToCleanup) {
          Set<JobID> jobs = trackerToJobsToCleanup.get(trackerName);
          if (jobs == null) {
            jobs = new HashSet<JobID>();
            trackerToJobsToCleanup.put(trackerName, jobs);
          }
          jobs.add(taskId.getJobID());
        }
        continue;
      }

      if (!job.inited()) {
        // if job is not yet initialized ... kill the attempt
        synchronized (trackerToTasksToCleanup) {
          Set<TaskAttemptID> tasks = trackerToTasksToCleanup.get(trackerName);
          if (tasks == null) {
            tasks = new HashSet<TaskAttemptID>();
            trackerToTasksToCleanup.put(trackerName, tasks);
          }
          tasks.add(taskId);
        }
        continue;
      }

      TaskInProgress tip = taskidToTIPMap.get(taskId);
      // Check if the tip is known to the jobtracker. In case of a restarted
      // jt, some tasks might join in later
      if (tip != null) {
        // Update the job and inform the listeners if necessary
        JobStatus prevStatus = (JobStatus)job.getStatus().clone();
        // Clone TaskStatus object here, because JobInProgress
        // or TaskInProgress can modify this object and
        // the changes should not get reflected in TaskTrackerStatus.
        // An old TaskTrackerStatus is used later in countMapTasks, etc.
        job.updateTaskStatus(tip, (TaskStatus)report.clone());
        JobStatus newStatus = (JobStatus)job.getStatus().clone();

        // Update the listeners if an incomplete job completes
        if (prevStatus.getRunState() != newStatus.getRunState()) {
          JobStatusChangeEvent event =
            new JobStatusChangeEvent(job, EventType.RUN_STATE_CHANGED,
                                     prevStatus, newStatus);
          updateJobInProgressListeners(event);
        }
      } else {
        LOG.info("Serious problem.  While updating status, cannot find taskid "
                 + report.getTaskID());
      }

      // Process 'failed fetch' notifications
      List<TaskAttemptID> failedFetchMaps = report.getFetchFailedMaps();
      if (failedFetchMaps != null) {
        TaskAttemptID reportingAttempt = report.getTaskID();
        for (TaskAttemptID mapTaskId : failedFetchMaps) {
          TaskInProgress failedFetchMap = taskidToTIPMap.get(mapTaskId);

          if (failedFetchMap != null) {
            // Gather information about the map which has to be failed, if need be
            String failedFetchTrackerName = getAssignedTracker(mapTaskId);
            if (failedFetchTrackerName == null) {
              failedFetchTrackerName = "Lost task tracker";
            }
            ((JobInProgress)failedFetchMap.getJob()).fetchFailureNotification(
              reportingAttempt, failedFetchMap, mapTaskId, failedFetchTrackerName);
          }
        }
      }
    }
  }

  /**
   * We lost the task tracker!  All task-tracker structures have
   * already been updated.  Just process the contained tasks and any
   * jobs that might be affected.
   */
  void lostTaskTracker(TaskTracker taskTracker) {
    String trackerName = taskTracker.getTrackerName();
    LOG.info("Lost tracker '" + trackerName + "'");

    // remove the tracker from the local structures
    synchronized (trackerToJobsToCleanup) {
      trackerToJobsToCleanup.remove(trackerName);
    }

    synchronized (trackerToTasksToCleanup) {
      trackerToTasksToCleanup.remove(trackerName);
    }

    Set<TaskAttemptIDWithTip> lostTasks = trackerToTaskMap.get(trackerName);
    trackerToTaskMap.remove(trackerName);

    if (lostTasks != null) {
      // List of jobs which had any of their tasks fail on this tracker
      Set<JobInProgress> jobsWithFailures = new HashSet<JobInProgress>();
      for (TaskAttemptIDWithTip oneTask : lostTasks) {
        TaskAttemptID taskId = oneTask.attemptId;
        TaskInProgress tip = oneTask.tip;
        JobInProgress job = (JobInProgress) tip.getJob();

        // Completed reduce tasks never need to be failed, because
        // their outputs go to dfs
        // And completed maps with zero reducers of the job
        // never need to be failed.
        if (!tip.isComplete() ||
            (tip.isMapTask() && !tip.isJobSetupTask() &&
             job.desiredReduces() != 0)) {
          // if the job is done, we don't want to change anything
          if (job.getStatus().getRunState() == JobStatus.RUNNING ||
              job.getStatus().getRunState() == JobStatus.PREP) {
            // the state will be KILLED_UNCLEAN, if the task(map or reduce)
            // was RUNNING on the tracker
            TaskStatus.State killState = (tip.isRunningTask(taskId) &&
              !tip.isJobSetupTask() && !tip.isJobCleanupTask()) ?
              TaskStatus.State.KILLED_UNCLEAN : TaskStatus.State.KILLED;
            job.failedTask(tip, taskId,
                          ("Lost task tracker: " + trackerName +
                            " at " + new Date()),
                           (tip.isMapTask() ?
                               TaskStatus.Phase.MAP :
                               TaskStatus.Phase.REDUCE),
                            killState,
                            trackerName);
            jobsWithFailures.add(job);
          }
        } else {
          // Completed 'reduce' task and completed 'maps' with zero
          // reducers of the job, not failed;
          // only removed from data-structures.
          markCompletedTaskAttempt(trackerName, taskId);
        }
      }

      // Penalize this tracker for each of the jobs which
      // had any tasks running on it when it was 'lost'
      // Also, remove any reserved slots on this tasktracker
      for (JobInProgress job : jobsWithFailures) {
        String reason = "Tracker went down";
        job.addTrackerTaskFailure(trackerName, taskTracker, reason);
      }

      // Cleanup
      taskTracker.cancelAllReservations();

      // Purge 'marked' tasks, needs to be done
      // here to prevent hanging references!
      removeMarkedTasks(trackerName);
    }
  }

  /**
   * Rereads the config to get hosts and exclude list file names.
   * Rereads the files to update the hosts and exclude lists.
   */
  public synchronized void refreshNodes() throws IOException {
    // check access
    PermissionChecker.checkSuperuserPrivilege(mrOwner, supergroup);

    // call the actual api
    refreshHosts();
  }

  private synchronized void refreshHosts() throws IOException {
    // Reread the config to get mapred.hosts and mapred.hosts.exclude filenames.
    // Update the file names and refresh internal includes and excludes list
    LOG.info("Refreshing hosts information");
    Configuration conf = new Configuration();

    hostsReader.updateFileNames(conf.get("mapred.hosts",""),
                                conf.get("mapred.hosts.exclude", ""));
    hostsReader.refresh();

    Set<String> excludeSet = new HashSet<String>();
    for(Map.Entry<String, TaskTracker> eSet : taskTrackers.entrySet()) {
      String trackerName = eSet.getKey();
      TaskTrackerStatus status = eSet.getValue().getStatus();
      // Check if not include i.e not in host list or in hosts list but excluded
      if (!inHostsList(status) || inExcludedHostsList(status)) {
          excludeSet.add(status.getHost()); // add to rejected trackers
      }
    }
    decommissionNodes(excludeSet);
    int totalExcluded = hostsReader.getExcludedHosts().size();
    getInstrumentation().setDecommissionedTrackers(totalExcluded);
  }

  // Remove a tracker from the system
  private void removeTracker(TaskTracker tracker) {
    String trackerName = tracker.getTrackerName();
    // Remove completely after marking the tasks as 'KILLED'
    lostTaskTracker(tracker);
    TaskTrackerStatus status = tracker.getStatus();

    // tracker is lost, and if it is blacklisted, remove
    // it from the count of blacklisted trackers in the cluster
    if (isBlacklisted(trackerName)) {
     faultyTrackers.decrBlackListedTrackers(1);
    }
    updateTaskTrackerStatus(trackerName, null);
    statistics.taskTrackerRemoved(trackerName);
    getInstrumentation().decTrackers(1);
  }

  // main decommission
  synchronized void decommissionNodes(Set<String> hosts)
  throws IOException {
    LOG.info("Decommissioning " + hosts.size() + " nodes");
    // create a list of tracker hostnames
    synchronized (taskTrackers) {
      synchronized (trackerExpiryQueue) {
        int trackersDecommissioned = 0;
        for (String host : hosts) {
          LOG.info("Decommissioning host " + host);
          Set<TaskTracker> trackers = hostnameToTaskTracker.remove(host);
          if (trackers != null) {
            for (TaskTracker tracker : trackers) {
              LOG.info("Decommission: Losing tracker " + tracker.getTrackerName() +
                       " on host " + host);
              removeTracker(tracker);
            }
            trackersDecommissioned += trackers.size();
          }
          LOG.info("Host " + host + " is ready for decommissioning");
        }
      }
    }
  }

  /**
   * Returns a set of excluded nodes.
   */
  Collection<String> getExcludedNodes() {
    return hostsReader.getExcludedHosts();
  }

  /**
   * Returns a set of dead nodes. (nodes that are expected to be alive)
   */
  public Collection<String> getDeadNodes() {
    List<String> activeHosts = new ArrayList<String>();
    synchronized(taskTrackers) {
      for (TaskTracker tt : taskTrackers.values()) {
        activeHosts.add(tt.getStatus().getHost());
      }
    }
    // dead hosts are the difference between active and known hosts
    // We don't consider a blacklisted host to be dead.
    Set<String> knownHosts = new HashSet<String>(hostsReader.getHosts());
    knownHosts.removeAll(activeHosts);
    // Also remove the excluded nodes as getHosts() returns them as well
    knownHosts.removeAll(hostsReader.getExcludedHosts());
    Set<String> deadHosts = knownHosts;
    return deadHosts;
  }

  /**
   * Get the localized job file path on the job trackers local file system
   * @param jobId id of the job
   * @return the path of the job conf file on the local file system
   */
  public static String getLocalJobFilePath(JobID jobId){
    return JobHistory.JobInfo.getLocalJobFilePath(jobId);
  }
  ////////////////////////////////////////////////////////////
  // main()
  ////////////////////////////////////////////////////////////

  /**
   * Start the JobTracker process.  This is used only for debugging.  As a rule,
   * JobTracker should be run as part of the DFS Namenode process.
   */
  public static void main(String argv[]
                          ) throws IOException, InterruptedException {
    StringUtils.startupShutdownMessage(JobTracker.class, argv, LOG);

    try {
      if (argv.length == 0) {
        JobTracker tracker = startTracker(new JobConf());
        tracker.offerService();
        return;
      }
      if ("-instance".equals(argv[0]) && argv.length == 2) {
        int instance = Integer.parseInt(argv[1]);
        if (instance == 0 || instance == 1) {
          JobConf conf = new JobConf();
          JobConf.overrideConfiguration(conf, instance);
          JobTracker tracker = startTracker(conf);
          tracker.offerService();
          return;
        }
      }
      if ("-dumpConfiguration".equals(argv[0]) && argv.length == 1) {
        dumpConfiguration(new PrintWriter(System.out));
        return;
      }
      System.out.println("usage: JobTracker [-dumpConfiguration]");
      System.out.println("       JobTracker [-instance <0|1>]");
      System.exit(-1);

    } catch (Throwable e) {
      LOG.fatal(StringUtils.stringifyException(e));
      System.exit(-1);
    }
  }

  /**
   * Dumps the configuration properties in Json format
   * @param writer {@link}Writer object to which the output is written
   * @throws IOException
   */
  private static void dumpConfiguration(Writer writer) throws IOException {
    Configuration.dumpConfiguration(new JobConf(), writer);
    writer.write("\n");
    // get the QueueManager configuration properties
    QueueManager.dumpConfiguration(writer);
    writer.write("\n");
  }

  @Override
  public JobQueueInfo[] getQueues() throws IOException {
    return queueManager.getJobQueueInfos();
  }


  @Override
  public JobQueueInfo getQueueInfo(String queue) throws IOException {
    return queueManager.getJobQueueInfo(queue);
  }

  @Override
  public JobStatus[] getJobsFromQueue(String queue) throws IOException {
    Collection<JobInProgress> jips = taskScheduler.getJobs(queue);
    return getJobStatus(jips,false);
  }

  @Override
  public QueueAclsInfo[] getQueueAclsForCurrentUser() throws IOException{
    return queueManager.getQueueAcls(
            UserGroupInformation.getCurrentUGI());
  }
  private synchronized JobStatus[] getJobStatus(Collection<JobInProgress> jips,
      boolean toComplete) {
    if(jips == null || jips.isEmpty()) {
      return new JobStatus[]{};
    }
    ArrayList<JobStatus> jobStatusList = new ArrayList<JobStatus>();
    for(JobInProgress jip : jips) {
      JobStatus status = jip.getStatus();
      status.setStartTime(jip.getStartTime());
      status.setUsername(jip.getProfile().getUser());
      if(toComplete) {
        if(status.getRunState() == JobStatus.RUNNING ||
            status.getRunState() == JobStatus.PREP) {
          jobStatusList.add(status);
        }
      }else {
        jobStatusList.add(status);
      }
    }
    return (JobStatus[]) jobStatusList.toArray(
        new JobStatus[jobStatusList.size()]);
  }

  /**
   * Returns the confgiured maximum number of tasks for a single job
   */
  int getMaxTasksPerJob() {
    return conf.getInt("mapred.jobtracker.maxtasks.per.job", -1);
  }

  @Override
  public void refreshServiceAcl() throws IOException {
    if (!conf.getBoolean(
            ServiceAuthorizationManager.SERVICE_AUTHORIZATION_CONFIG, false)) {
      throw new AuthorizationException("Service Level Authorization not enabled!");
    }
    SecurityUtil.getPolicy().refresh();
  }

  private void initializeTaskMemoryRelatedConfig() {
    memSizeForMapSlotOnJT =
        JobConf.normalizeMemoryConfigValue(conf.getLong(
            JobTracker.MAPRED_CLUSTER_MAP_MEMORY_MB_PROPERTY,
            JobConf.DISABLED_MEMORY_LIMIT));
    memSizeForReduceSlotOnJT =
        JobConf.normalizeMemoryConfigValue(conf.getLong(
            JobTracker.MAPRED_CLUSTER_REDUCE_MEMORY_MB_PROPERTY,
            JobConf.DISABLED_MEMORY_LIMIT));

    if (conf.get(JobConf.UPPER_LIMIT_ON_TASK_VMEM_PROPERTY) != null) {
      LOG.warn(
        JobConf.deprecatedString(
          JobConf.UPPER_LIMIT_ON_TASK_VMEM_PROPERTY)+
          " instead use "+JobTracker.MAPRED_CLUSTER_MAX_MAP_MEMORY_MB_PROPERTY+
          " and " + JobTracker.MAPRED_CLUSTER_MAX_REDUCE_MEMORY_MB_PROPERTY
      );

      limitMaxMemForMapTasks = limitMaxMemForReduceTasks =
        JobConf.normalizeMemoryConfigValue(
          conf.getLong(
            JobConf.UPPER_LIMIT_ON_TASK_VMEM_PROPERTY,
            JobConf.DISABLED_MEMORY_LIMIT));
      if (limitMaxMemForMapTasks != JobConf.DISABLED_MEMORY_LIMIT &&
        limitMaxMemForMapTasks >= 0) {
        limitMaxMemForMapTasks = limitMaxMemForReduceTasks =
          limitMaxMemForMapTasks /
            (1024 * 1024); //Converting old values in bytes to MB
      }
    } else {
      limitMaxMemForMapTasks =
        JobConf.normalizeMemoryConfigValue(
          conf.getLong(
            JobTracker.MAPRED_CLUSTER_MAX_MAP_MEMORY_MB_PROPERTY,
            JobConf.DISABLED_MEMORY_LIMIT));
      limitMaxMemForReduceTasks =
        JobConf.normalizeMemoryConfigValue(
          conf.getLong(
            JobTracker.MAPRED_CLUSTER_MAX_REDUCE_MEMORY_MB_PROPERTY,
            JobConf.DISABLED_MEMORY_LIMIT));
    }

    LOG.info(new StringBuilder().append("Scheduler configured with ").append(
        "(memSizeForMapSlotOnJT, memSizeForReduceSlotOnJT,").append(
        " limitMaxMemForMapTasks, limitMaxMemForReduceTasks) (").append(
        memSizeForMapSlotOnJT).append(", ").append(memSizeForReduceSlotOnJT)
        .append(", ").append(limitMaxMemForMapTasks).append(", ").append(
            limitMaxMemForReduceTasks).append(")"));
  }

  private boolean perTaskMemoryConfigurationSetOnJT() {
    if (limitMaxMemForMapTasks == JobConf.DISABLED_MEMORY_LIMIT
        || limitMaxMemForReduceTasks == JobConf.DISABLED_MEMORY_LIMIT
        || memSizeForMapSlotOnJT == JobConf.DISABLED_MEMORY_LIMIT
        || memSizeForReduceSlotOnJT == JobConf.DISABLED_MEMORY_LIMIT) {
      return false;
    }
    return true;
  }

  /**
   * Check the job if it has invalid requirements and throw and IOException if does have.
   *
   * @param job
   * @throws IOException
   */
  private void checkMemoryRequirements(JobInProgress job)
      throws IOException {
    if (!perTaskMemoryConfigurationSetOnJT()) {
      LOG.debug("Per-Task memory configuration is not set on JT. "
          + "Not checking the job for invalid memory requirements.");
      return;
    }

    boolean invalidJob = false;
    String msg = "";
    long maxMemForMapTask = job.getMemoryForMapTask();
    long maxMemForReduceTask = job.getMemoryForReduceTask();

    if (maxMemForMapTask == JobConf.DISABLED_MEMORY_LIMIT
        || maxMemForReduceTask == JobConf.DISABLED_MEMORY_LIMIT) {
      invalidJob = true;
      msg = "Invalid job requirements.";
    }

    if (maxMemForMapTask > limitMaxMemForMapTasks
        || maxMemForReduceTask > limitMaxMemForReduceTasks) {
      invalidJob = true;
      msg = "Exceeds the cluster's max-memory-limit.";
    }

    if (invalidJob) {
      StringBuilder jobStr =
          new StringBuilder().append(job.getJobID().toString()).append("(")
              .append(maxMemForMapTask).append(" memForMapTasks ").append(
                  maxMemForReduceTask).append(" memForReduceTasks): ");
      LOG.warn(jobStr.toString() + msg);

      throw new IOException(jobStr.toString() + msg);
    }
  }

  @Override
  public void refreshQueueAcls() throws IOException{
    LOG.info("Refreshing queue acls. requested by : " +
        UserGroupInformation.getCurrentUGI().getUserName());
    this.queueManager.refreshAcls(new Configuration(this.conf));
  }

  synchronized String getReasonsForBlacklisting(String host) {
    FaultInfo fi = faultyTrackers.getFaultInfo(host, false);
    if (fi == null) {
      return "";
    }
    return fi.getTrackerFaultReport();
  }

  /** Test Methods */
  synchronized Set<ReasonForBlackListing> getReasonForBlackList(String host) {
    FaultInfo fi = faultyTrackers.getFaultInfo(host, false);
    if (fi == null) {
      return new HashSet<ReasonForBlackListing>();
    }
    return fi.getReasonforblacklisting();
  }

  public synchronized void retireCompletedJobs() {
    synchronized (jobs) {
      RETIRE_COMPLETED_JOBS = true;
    }
  }

  /**
   * @return the pluggable object for obtaining job resource information
   */
  public ResourceReporter getResourceReporter() {
    return resourceReporter;
  }

  /**
   * Update totalMapTaskCapacity and totalReduceTaskCapacity to resolve the
   * number of slots changed in a tasktracker. The change could be from
   * TaskScheduler or TaskTrackerStatus
   *
   * @param status The status of the tasktracker
   */
  private void updateTotalTaskCapacity(TaskTrackerStatus status) {
    int mapSlots = taskScheduler.getMaxSlots(status, TaskType.MAP);
    String trackerName = status.getTrackerName();
    Integer oldMapSlots = trackerNameToMapSlots.get(trackerName);
    if (oldMapSlots == null) {
      oldMapSlots = 0;
    }
    int delta = mapSlots - oldMapSlots;

    if (delta != 0) {
      totalMapTaskCapacity += delta;
      trackerNameToMapSlots.put(trackerName, mapSlots);

      LOG.info("Changing map slot count due to " + trackerName + " from " +
               oldMapSlots + " to " + mapSlots + ", totalMap = " + totalMapTaskCapacity);
    }

    int reduceSlots = taskScheduler.getMaxSlots(status, TaskType.REDUCE);
    Integer oldReduceSlots = trackerNameToReduceSlots.get(trackerName);
    if (oldReduceSlots == null) {
      oldReduceSlots = 0;
    }

    delta = reduceSlots - oldReduceSlots;

    if (delta != 0) {
      totalReduceTaskCapacity += delta;
      trackerNameToReduceSlots.put(trackerName, reduceSlots);

      LOG.info("Changing reduce slot count due to " + trackerName + " from " +
               oldReduceSlots + " to " + reduceSlots +
               ", totalReduce = " + totalReduceTaskCapacity);
    }
  }

  /**
   * Update totalMapTaskCapacity and totalReduceTaskCapacity for removing
   * a tasktracker
   *
   * @param status The status of the tasktracker
   */
  private void removeTaskTrackerCapacity(TaskTrackerStatus status) {
    Integer mapSlots = trackerNameToMapSlots.remove(status.getTrackerName());
    if (mapSlots == null) {
      mapSlots = 0;
    }
    totalMapTaskCapacity -= mapSlots;

    Integer reduceSlots = trackerNameToReduceSlots.remove(status.getTrackerName());
    if (reduceSlots == null) {
      reduceSlots = 0;
    }
    totalReduceTaskCapacity -= reduceSlots;

    LOG.info("Removing " + mapSlots + " map slots, " + reduceSlots +
             " reduce slots due to " + status.getTrackerName() +
             ", totalMap = " + totalMapTaskCapacity + ", totalReduce = " + totalReduceTaskCapacity);
  }

  public TaskErrorCollector getTaskErrorCollector() {
    return taskErrorCollector;
  }
}
