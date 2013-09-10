/*
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
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.corona.ResourceGrant;
import org.apache.hadoop.corona.ResourceRequest;
import org.apache.hadoop.corona.SessionDriver;
import org.apache.hadoop.corona.Utilities;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.ipc.ProtocolSignature;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.mapred.CoronaCommitPermission.CommitPermissionServer;
import org.apache.hadoop.mapred.CoronaSessionInfo.InetSocketAddressWritable;

/**
 * The Proxy used by the CoronaJobTracker in the client to communicate
 * with the CoronaJobTracker running on the TaskTracker in case of a
 * remote CoronaJobTracker
 */
@SuppressWarnings("deprecation")
public class RemoteJTProxy implements InterCoronaJobTrackerProtocol,
    JobSubmissionProtocol {
  /** Logger */
  public static final Log LOG = LogFactory.getLog(CoronaJobTracker.class);
  /** Amount of time to wait for remote JT to launch. */
  public static final String REMOTE_JT_TIMEOUT_SEC_CONF =
      "mapred.coronajobtracker.remotejobtracker.wait";
  /** Flag used for test, if to exclude the failed remote job tracker. */
  public static final String REMOTE_JT_EXCLUDE_FAILED =
      "mpared.coronajobtracker.remotejobtracker.exclude";
  /** Default amount of time to wait for remote JT to launch. */
  public static final int REMOTE_JT_TIMEOUT_SEC_DEFAULT = 60;
  /** Amount of time for a RPC call timeout to remote JT. */
  public static final String REMOTE_JT_RPC_TIMEOUT_SEC_CONF =
      "mapred.coronajobtracker.remotejobtracker.rpc.timeout";
  /** Default amount of a RPC call timeout to remote JT. */
  public static final int REMOTE_JT_RPC_TIMEOUT_SEC_DEFAULT = 3600;
  /** Boolean, determines whether remote JT restart should restore state */
  public static final String REMOTE_JT_STATE_RESTORING_CONF =
      "mapred.coronajobtracker.remote.state.restoring";
  /** Default use state restoring mechanism */
  public static final boolean REMOTE_JT_STATE_RESTORING_DEFAULT = true;
  
  /** The proxy object to the CoronaJobTracker running in the cluster */
  private volatile JobSubmissionProtocol client;
  /** JobSubmissionProtocol client lock */
  // We do need fair lock to enable early recovery after remote JT crash.
  private ReadWriteLock clientLock = new ReentrantReadWriteLock(true);
  // TODO Giving priority to writers will increase performance.
  /** The host where the remote Job Tracker is running. */
  private String remoteJTHost;
  /** The port where the remote Job Tracker is running. */
  private int remoteJTPort;
  /** The task id for the current attempt of running CJT */
  private TaskAttemptID currentAttemptId;
  /** The number of the current attempt */
  private int attempt;
  /** Job configuration */
  private final JobConf conf;
  /** Parent JobTracker */
  private final CoronaJobTracker jt;
  /** The remote JT resource grant. */
  private ResourceGrant remoteJTGrant;
  /** The id of the job */
  private final JobID jobId;
  /** Is true iff the job has been submitted */
  private boolean isJobSubmitted = false;
  /** The session id for the job tracker running in the cluster */
  private String remoteSessionId;
  /** The number of remote JT restart attempts. */
  private volatile int numRemoteJTFailures;
  /** The limit for remote JT restart attempts number. */
  private final int maxRemoteJTFailures;
  /** Current job attempt id */
  private JobID attemptJobId;
  /** Address of remote JT */
  private InetSocketAddress remoteJTAddr;
  /** Holds exceptions from restarting */
  public volatile IOException restartingException = null;
  /** Saved state updates from remote JT */
  private final CoronaJTState remoteJTState;
  /** Authority that gives permission to commit */
  private final CommitPermissionServer commmitPermissionServer;
  
  private enum RemoteJTStatus {
    UNINITIALIZED,
    SUCCESS,
    FAILURE
  };
  private RemoteJTStatus remoteJTStatus;
  
  //This variable is our internal logic control flag. 
  //It means when the RJT failover is enabled, which API call failure will cause failover. 
  //For API call like killJob, killTasks should not fire the RJT failover.
  protected volatile boolean isRestartable = true;

  /**
   * Construct a proxy for the remote job tracker
   * @param jt parent job tracker
   * @param jobId id of the job the proxy is created for
   * @param conf job configuration
   * @throws IOException
   */
  RemoteJTProxy(CoronaJobTracker jt, JobID jobId, JobConf conf) throws IOException {
    this.maxRemoteJTFailures = conf.getInt(CoronaJobTracker.MAX_JT_FAILURES_CONF,
        CoronaJobTracker.MAX_JT_FAILURES_DEFAULT);
    this.conf = conf;
    this.jt = jt;
    this.jobId = jobId;
    // Prepare first attempt.
    this.attemptJobId = jobId;
    attempt = 0;
    int partitionId = conf.getNumMapTasks() + 100000;
    currentAttemptId = new TaskAttemptID(new TaskID(attemptJobId, true,
        partitionId), attempt);
    remoteJTStatus = RemoteJTStatus.UNINITIALIZED;
    
    // Prepare stuff for restoring state if necessary
    if (isStateRestoringEnabled(conf)) {
      remoteJTState = new CoronaJTState();
      commmitPermissionServer = new CommitPermissionServer();
    } else {
      remoteJTState = null;
      commmitPermissionServer = null;
    }
  }

  public String getRemoteSessionId() {
    return remoteSessionId;
  }

  // ///////////////////////////////////////////////////////////////////////////
  // InterCoronaJobTrackerProtocol
  // ///////////////////////////////////////////////////////////////////////////
  @Override
  public void reportRemoteCoronaJobTracker(
      String attempt,
      String host,
      int port,
      String sessionId) throws IOException {
    TaskAttemptID attemptId = TaskAttemptID.forName(attempt);
    synchronized (this) {
      checkAttempt(attemptId);
      initializeClientUnprotected(host, port, sessionId);
      this.notifyAll();
    }
  }
  
  @Override
  public InetSocketAddressWritable getNewJobTrackerAddress(
      InetSocketAddressWritable failedTracker) throws IOException {
    // Die immediately if restarting is disabled
    if (maxRemoteJTFailures == 0) {
      throw new IOException("Restarting remote JT is disabled.");
    }
    assert remoteJTAddr != null : "Not started, but got request to restart.";
    if (clientLock.readLock().tryLock()) {
      // We're not restarting, check address
      InetSocketAddress seenAddr = remoteJTAddr;
      clientLock.readLock().unlock();
      // seenAddr is safe to use, because even if restarting takes place, this
      // is the address of JT that either is fully running or dead
      // (not currently restarting)
      if (seenAddr.equals(failedTracker.getAddress())) {
        // Not restarted yet
        return null;
      } else {
        LOG.info("Serving new job tracker address request with " + seenAddr
            + " old " + failedTracker.getAddress());
        return new InetSocketAddressWritable(seenAddr);
      }
    } else {
      // Currently restarting
      return null;
    }
  }

  @Override
  public void pushCoronaJobTrackerStateUpdate(TaskAttemptID attempt,
      CoronaStateUpdate[] updates) throws IOException {
    checkAttempt(attempt);
    if (remoteJTState == null) {
      throw new IOException(
          "Logic error: got state update but state restoring is disabled");
    } else {
      synchronized (remoteJTState) {
        for (CoronaStateUpdate update : updates) {
          remoteJTState.add(update);
        }
      }
    }
  }

  @Override
  public CoronaJTState getCoronaJobTrackerState(TaskAttemptID attemptId)
      throws IOException {
    checkAttempt(attemptId);
    if (remoteJTState == null) {
      throw new IOException("Logic error: asked for remote JT state but state "
          + "restoring is disabled");
    } else {
      synchronized (remoteJTState) {
        return remoteJTState.prepare();
      }
    }
  }

  @Override
  public TaskAttemptID[] getAndSetCommitting(TaskAttemptID attemptId,
      TaskAttemptID[] toCommit) throws IOException {
    checkAttempt(attemptId);
    if (commmitPermissionServer == null) {
      throw new IOException(
          "Logic error: got getAndSet for committing attempt "
              + "but commit permission server is down");
    } else {
      return commmitPermissionServer.getAndSetCommitting(toCommit);
    }
  }

  @Override
  public ProtocolSignature getProtocolSignature(String protocol,
      long clientVersion, int clientMethodsHash) throws IOException {
    return ProtocolSignature.getProtocolSignature(
        this, protocol, clientVersion, clientMethodsHash);
  }

  @Override
  public long getProtocolVersion(String protocol, long clientVersion)
    throws IOException {
    if (protocol.equals(InterCoronaJobTrackerProtocol.class.getName())) {
      return InterCoronaJobTrackerProtocol.versionID;
    } else {
      throw new IOException("Unknown protocol " + protocol);
    }
  }

  /**
   * Increment the attempt number for launching a remote corona job tracker.
   * Must be called only when holding the object lock.
   */
  private void incrementAttemptUnprotected() {
    attempt++;
    currentAttemptId = new TaskAttemptID(new TaskID(attemptJobId,
        currentAttemptId.isMap(), currentAttemptId.getTaskID().getId()),
        attempt);
  }

  /**
   * Checks whether provided attempt id of remote JT matches currently set,
   * throws if not
   * @param attempt attempt id to check
   * @throws IOException
   */
  private void checkAttempt(TaskAttemptID attemptId) throws IOException {
    if (!attemptId.equals(currentAttemptId)) {
      throw new IOException("Attempt " + attemptId
          + " does not match current attempt " + currentAttemptId);
    }
  }

  /**
   * Create the RPC client to the remote corona job tracker.
   * @param host The host running the remote corona job tracker.
   * @param port The port of the remote corona job tracker.
   * @param sessionId The session for the remote corona job tracker.
   * @throws IOException
   */
  void initializeClientUnprotected(String host, int port, String sessionId)
    throws IOException {
    if (client != null) {
      return;
    }
    LOG.info("Creating JT client to " + host + ":" + port);
    long connectTimeout = RemoteJTProxy.getRemotJTTimeout(conf);
    int rpcTimeout = RemoteJTProxy.getRemoteJTRPCTimeout(conf);
    remoteJTAddr = new InetSocketAddress(host, port);
    client = RPC.waitForProtocolProxy(
      JobSubmissionProtocol.class,
      JobSubmissionProtocol.versionID,
      remoteJTAddr,
      conf,
      connectTimeout,
      rpcTimeout
      ).getProxy();
    remoteJTStatus = RemoteJTStatus.SUCCESS;
    remoteJTHost = host;
    remoteJTPort = port;
    remoteSessionId = sessionId;
    
    if (remoteJTState != null) {
      remoteJTState.setSessionId(sessionId);
    }
  }

  private void reinitClientUnprotected() throws IOException {
    if (client != null) {
      RPC.stopProxy(client);
      client = null;
      remoteJTStatus = RemoteJTStatus.UNINITIALIZED;
    }
    
    try {
      initializeClientUnprotected(remoteJTHost, remoteJTPort, remoteSessionId);
    } finally {
      if (client == null) {
        remoteJTStatus = RemoteJTStatus.FAILURE;
      }
    }
  }

  /**
   * Waits for the remote Corona JT to be ready.
   * This involves
   *    - getting a JOBTRACKER resource from the cluster manager.
   *    - starting the remote job tracker by connecting to the corona task
   *      tracker on the machine.
   *    - waiting for the remote job tracker to report its port back to this
   *      process.
   * @param jobConf The job configuration to use.
   * @throws IOException
   */
  public void waitForJTStart(JobConf jobConf) throws IOException {
    int maxJTAttempts = jobConf.getInt(
        "mapred.coronajobtracker.remotejobtracker.attempts", 4);
    ResourceTracker resourceTracker = jt.getResourceTracker();
    SessionDriver sessionDriver = jt.getSessionDriver();
    List<ResourceGrant> excludeGrants = new ArrayList<ResourceGrant>();
    boolean toExcludeFailed = jobConf.getBoolean(REMOTE_JT_EXCLUDE_FAILED, true);
    // Release and blacklist failed JT grant.
    if (remoteJTGrant != null) {
      if (toExcludeFailed) {
        excludeGrants.add(remoteJTGrant);
      }
      resourceTracker.releaseResource(remoteJTGrant.getId());
      sessionDriver.releaseResources(resourceTracker.getResourcesToRelease());
    }
    for (int i = 0; i < maxJTAttempts; i++) {
      try {
        remoteJTGrant = waitForJTGrant(resourceTracker, sessionDriver,
            excludeGrants);
        boolean success = startRemoteJT(jobConf, remoteJTGrant);
        if (success) {
          return;
        } else {
          excludeGrants.add(remoteJTGrant);
          resourceTracker.releaseResource(remoteJTGrant.getId());
          List<ResourceRequest> released =
            resourceTracker.getResourcesToRelease();
          sessionDriver.releaseResources(released);
        }
      } catch (InterruptedException e) {
        throw new IOException(e);
      }
    }
    throw new IOException("Could not start remote JT after " + maxJTAttempts +
      " attempts");
  }

  /**
   * Wait for a JOBTRACKER grant.
   * @param resourceTracker The resource tracker object for getting the grant
   * @param sessionDriver The session driver for getting the grant
   * @param previousGrants Previous grants that could not be used successfully.
   * @return A new JOBTRACKER grant.
   * @throws IOException
   * @throws InterruptedException
   */
  private ResourceGrant waitForJTGrant(
      ResourceTracker resourceTracker,
      SessionDriver sessionDriver,
      List<ResourceGrant> previousGrants)
    throws IOException, InterruptedException {
    LOG.info("Waiting for JT grant for " + attemptJobId);
    ResourceRequest req = resourceTracker.newJobTrackerRequest();
    for (ResourceGrant prev: previousGrants) {
      LOG.info("Adding " + prev.getNodeName() + " to excluded hosts");
      req.addToExcludeHosts(prev.getAddress().getHost());
    }
    resourceTracker.recordRequest(req);
    List<ResourceRequest> newRequests = resourceTracker.getWantedResources();
    sessionDriver.requestResources(newRequests);
    final List<ResourceGrant> grants = new ArrayList<ResourceGrant>();
    ResourceTracker.ResourceProcessor proc =
      new ResourceTracker.ResourceProcessor() {
        @Override
        public boolean processAvailableResource(ResourceGrant resource) {
          grants.add(resource);
          final boolean consumed = true;
          return consumed;
        }
      };
    while (true) {
      // Try to get JT grant while periodically checking for session driver
      // exceptions.
      long timeout = 60 * 1000; // 1 min.
      resourceTracker.processAvailableGrants(proc, 1, timeout);
      IOException e = sessionDriver.getFailed();
      if (e != null) {
        throw e;
      }
      if (!grants.isEmpty()) {
        return grants.get(0);
      }
    }
  }

  /**
   * Start corona job tracker on the machine provided by using the corona
   * task tracker API.
   * @param jobConf The job configuration.
   * @param grant The grant that specifies the remote machine.
   * @return A boolean indicating success.
   * @throws InterruptedException
   */
  private boolean startRemoteJT(
    JobConf jobConf,
    ResourceGrant grant) throws InterruptedException {
    org.apache.hadoop.corona.InetAddress ttAddr =
      Utilities.appInfoToAddress(grant.appInfo);
    CoronaTaskTrackerProtocol coronaTT = null;
    try {
      coronaTT = jt.getTaskTrackerClient(ttAddr.getHost(), ttAddr.getPort());
    } catch (IOException e) {
      LOG.error("Error while trying to connect to TT at " + ttAddr.getHost() +
        ":" + ttAddr.getPort(), e);
      return false;
    }
    LOG.warn("Starting remote JT for " + attemptJobId
        + " on " + ttAddr.getHost());

    // Get a special map id for the JT task.
    Path systemDir = new Path(jt.getSystemDir());
    LOG.info("startRemoteJT:systemDir "+systemDir.toString());
    String jobFile = CoronaJobInProgress.getJobFile(systemDir, attemptJobId)
        .toString();
    LOG.info("startRemoteJT:jobFile " + jobFile);
    String splitClass = JobClient.RawSplit.class.getName();
    BytesWritable split = new BytesWritable();
    Task jobTask = new MapTask(
        jobFile, currentAttemptId, currentAttemptId.getTaskID().getId(),
        splitClass, split, 1, jobConf.getUser());
    CoronaSessionInfo info = new CoronaSessionInfo(jt.getSessionId(),
        jt.getJobTrackerAddress(), jt.getJobTrackerAddress());
    synchronized (this) {
      try {
        coronaTT.startCoronaJobTracker(jobTask, info);
      } catch (IOException e) {
        // Increment the attempt so that the older attempt will get an error
        // in reportRemoteCoronaJobTracker().
        incrementAttemptUnprotected();
        LOG.error("Error while performing RPC to TT at " + ttAddr.getHost() +
          ":" + ttAddr.getPort(), e);
        return false;
      }
    }

    // Now wait for the remote CJT to report its address.
    final long waitStart = System.currentTimeMillis();
    final long timeout = RemoteJTProxy.getRemotJTTimeout(jobConf);
    synchronized (this) {
      while (client == null) {
        LOG.warn("Waiting for remote JT to start on " + ttAddr.getHost());
        this.wait(1000);
        if (client == null &&
            System.currentTimeMillis() - waitStart > timeout) {
          // Increment the attempt so that the older attempt will get an error
          // in reportRemoteCoronaJobTracker().
          incrementAttemptUnprotected();
          LOG.warn("Could not start remote JT on " + ttAddr.getHost());
          return false;
        }
      }
    }
    return true;
  }

  /**
   * Returns the timeout in milliseconds after which we timeout the remote job
   * tracker.
   *
   * @param conf
   *          The configuration
   * @return The timeout in milliseconds.
   */
  public static long getRemotJTTimeout(Configuration conf) {
    return conf.getInt(RemoteJTProxy.REMOTE_JT_TIMEOUT_SEC_CONF,
        RemoteJTProxy.REMOTE_JT_TIMEOUT_SEC_DEFAULT) * 1000;
  }
  
  public static int getRemoteJTRPCTimeout(Configuration conf) {
    return conf.getInt(RemoteJTProxy.REMOTE_JT_RPC_TIMEOUT_SEC_CONF, 
        RemoteJTProxy.REMOTE_JT_RPC_TIMEOUT_SEC_DEFAULT) * 1000;
  }
  

  // ///////////////////////////////////////////////////////////////////////////
  // JobSubmissionProtocol
  // ///////////////////////////////////////////////////////////////////////////
  @Override
  public JobID getNewJobId() throws IOException {
    throw new UnsupportedOperationException(
        "getNewJobId not supported by proxy");
  }

  @Override
  public JobStatus submitJob(final JobID jobId) throws IOException {
    return (new Caller<JobStatus>() {
      @Override
      JobStatus call(JobSubmissionProtocol myClient) throws IOException {
        // This is first time job submission. Called only once
        isJobSubmitted = true;
        return myClient.submitJob(attemptJobId);
      }
    }).makeCall();
  }

  @Override
  public ClusterStatus getClusterStatus(boolean detailed) throws IOException {
    throw new UnsupportedOperationException(
        "getClusterStatus is not supported by proxy");
  }

  @Override
  public void killJob(final JobID jobId) throws IOException {
    (new Caller<JobID>() {
      
      // If the job tracker who hosting the job died,
      // will not do an automatic failover
      @Override
      protected boolean isRestartableCall() {
        return false;
      }

      @Override
      JobID call(JobSubmissionProtocol myClient) throws IOException {
        myClient.killJob(attemptJobId);
        return jobId;
      }
    }).makeCall();
  }

  @Override
  public void setJobPriority(JobID jobId, String priority) throws IOException {
    throw new UnsupportedOperationException(
        "setJobPriority is not supported by proxy");
  }

  @Override
  public boolean killTask(final TaskAttemptID taskId, final boolean shouldFail)
    throws IOException {
    return (new Caller<Boolean>() {
      // If the job tracker who hosting the task died,
      // will not do an automatic failover
      @Override
      protected boolean isRestartableCall() {
        return false;
      }
      
      @Override
      Boolean call(JobSubmissionProtocol myClient) throws IOException {
        return myClient.killTask(taskId, shouldFail);
      }
    }).makeCall();
  }

  @Override
  public JobProfile getJobProfile(final JobID jobId) throws IOException {
    return (new Caller<JobProfile>() {
      @Override
      JobProfile call(JobSubmissionProtocol myClient) throws IOException {
        return myClient.getJobProfile(attemptJobId);
      }
    }).makeCall();
  }

  @Override
  public JobStatus getJobStatus(final JobID jobId) throws IOException {
    return (new Caller<JobStatus>() {
      @Override
      JobStatus call(JobSubmissionProtocol myClient) throws IOException {
        return myClient.getJobStatus(attemptJobId);
      }
    }).makeCall();
  }

  @Override
  public Counters getJobCounters(final JobID jobId) throws IOException {
    return (new Caller<Counters>() {
      @Override
      Counters call(JobSubmissionProtocol myClient) throws IOException {
        return myClient.getJobCounters(attemptJobId);
      }
    }).makeCall();
  }

  @Override
  public TaskReport[] getMapTaskReports(final JobID jobId) throws IOException {
    return (new Caller<TaskReport[]>() {
      @Override
      TaskReport[] call(JobSubmissionProtocol myClient) throws IOException {
        return myClient.getMapTaskReports(attemptJobId);
      }
    }).makeCall();
  }

  @Override
  public TaskReport[] getReduceTaskReports(final JobID jobId)
    throws IOException {
    return (new Caller<TaskReport[]>() {
      @Override
      TaskReport[] call(JobSubmissionProtocol myClient) throws IOException {
        return myClient.getReduceTaskReports(attemptJobId);
      }
    }).makeCall();
  }

  @Override
  public TaskReport[] getCleanupTaskReports(final JobID jobId)
    throws IOException {
    return (new Caller<TaskReport[]>() {
      @Override
      TaskReport[] call(JobSubmissionProtocol myClient) throws IOException {
        return myClient.getCleanupTaskReports(attemptJobId);
      }
    }).makeCall();
  }

  @Override
  public TaskReport[] getSetupTaskReports(final JobID jobId)
    throws IOException {
    return (new Caller<TaskReport[]>() {
      @Override
      TaskReport[] call(JobSubmissionProtocol myClient) throws IOException {
        return myClient.getSetupTaskReports(attemptJobId);
      }
    }).makeCall();
  }

  @Override
  public String getFilesystemName() throws IOException {
    throw new UnsupportedOperationException(
        "getFilesystemName is not supported by proxy");
  }

  @Override
  public JobStatus[] jobsToComplete() {
    throw new UnsupportedOperationException(
        "jobsToComplete is not supported by proxy");
  }

  @Override
  public JobStatus[] getAllJobs() {
    throw new UnsupportedOperationException(
        "getAllJobs is not supported by proxy");
  }

  @Override
  public TaskCompletionEvent[] getTaskCompletionEvents(final JobID jobid,
      final int fromEventId, final int maxEvents) throws IOException {
    return (new Caller<TaskCompletionEvent[]>() {
      @Override
      TaskCompletionEvent[] call(JobSubmissionProtocol myClient) throws IOException {
        return myClient.getTaskCompletionEvents(attemptJobId, fromEventId, maxEvents);
      }
    }).makeCall();
  }

  @Override
  public String[] getTaskDiagnostics(final TaskAttemptID taskId)
    throws IOException {
    return (new Caller<String[]>() {
      @Override
      String[] call(JobSubmissionProtocol myClient) throws IOException {
        return myClient.getTaskDiagnostics(taskId);
      }
    }).makeCall();
  }

  @Override
  public String getSystemDir() {
    throw new UnsupportedOperationException(
        "getSystemDir not supported by proxy.");
  }

  @Override
  public JobQueueInfo[] getQueues() {
    throw new UnsupportedOperationException("getQueues method is " +
        "not supported by proxy.");
  }

  @Override
  public JobQueueInfo getQueueInfo(String queue) {
    throw new UnsupportedOperationException(
        "getQueueInfo not supported by proxy.");
  }

  @Override
  public JobStatus[] getJobsFromQueue(String queue) {
    throw new UnsupportedOperationException(
        "getJobsFromQueue not supported by proxy.");
  }

  @Override
  public QueueAclsInfo[] getQueueAclsForCurrentUser() throws IOException {
    throw new UnsupportedOperationException(
        "getQueueAclsForCurrentUser not supported by proxy.");
  }

  /**
   * Stop RPC client.
   */
  public void close() {
    clientLock.writeLock().lock();
    try {
      if (client != null) {
        RPC.stopProxy(client);
        client = null;
      }
    } finally {
      clientLock.writeLock().unlock();
    }
  }

  // ///////////////////////////////////////////////////////////////////////////
  // Remote CJT reincarnation.
  // ///////////////////////////////////////////////////////////////////////////
  /**
   * Generic caller interface.
   */
  private abstract class Caller<T> {
    /**
     * Perform the call. Must be overridden by a sub-class.
     * @param myClient the client to make the call with.
     * @return The generic return value.
     * @throws IOException
     */
    abstract T call(JobSubmissionProtocol myClient) throws IOException;
    
    /**
     * Overriding it to let the caller know if the current call is a
     * restartable one. It means if failed to call RJT, if we need to
     * do an automatic failover
     * 
     */
    protected boolean isRestartableCall() {
      return isRestartable;
    }

    /**
     * Template function to make the call.
     * @return The generic return value.
     * @throws IOException
     */
    public T makeCall() throws IOException {
      int curRestartNo;
      // If restart fails, exception will break this loop
      while(true) {
        clientLock.readLock().lock();
        curRestartNo = numRemoteJTFailures;
        try {
          try {
            return makeCallWithRetries();
          } finally {
            clientLock.readLock().unlock();
          }
        } catch (IOException e) {
          LOG.error("Error on remote call with retries", e);
          if (isRestartableCall()) {
            handleRemoteJTFailure(curRestartNo);
          } else {
            throw e;
          }
        }
      } 
    }
    
    /**
     * Handles remote JT failure.
     * @param failureRestartNo numRemoteJTFailures when failure that issued this
     * call has occurred
     * @return true iff remote call can be repeated
     * @throws IOException InterruptedException
     */
    private void handleRemoteJTFailure(int failureRestartNo)
        throws IOException {
      clientLock.writeLock().lock();
      try {
        if (failureRestartNo == numRemoteJTFailures) {
          try {
            LOG.warn("failureRestartNo " + failureRestartNo + 
                " maxRemoteFailures " + maxRemoteJTFailures + 
                " numFailure " + numRemoteJTFailures);
            
            ++numRemoteJTFailures;
            if (numRemoteJTFailures <= maxRemoteJTFailures) {
              LOG.warn("JobTracker died or is unreachable."
                  + " Restarting remote JT.");
              synchronized(RemoteJTProxy.this) {
                restartRemoteJTUnprotected();
              }
            } else {
              LOG.warn("JobTracker died or is unreachable."
                  + " Reached restart number limit."
                  + " Reporting to ClusterManager.");
              if (remoteSessionId != null) {
                // Kill remote session - it will release resources immediately
                jt.getSessionDriver().stopRemoteSession(remoteSessionId);
              }
              jt.close(false, true);
              throw new IOException("Reached remote JT restart limit.");
            }
          } catch (IOException e) {
            restartingException = e;
            throw restartingException;
          } catch (InterruptedException e) {
            restartingException = new IOException(e);
            throw restartingException;
          }
        } else {
          // Other thread restarted remote JT, check if successfully
          if (restartingException != null) {
            throw new IOException(restartingException);
          }
        }
      } finally {
        clientLock.writeLock().unlock();
      }
    }
    
    /**
     * Restarts remote JT if there was running job and resubmits this job.
     * @throws IOException
     */
    private void restartRemoteJTUnprotected() throws IOException {
      SessionDriver sessionDriver = jt.getSessionDriver();
      if (remoteSessionId != null) {
        // Kill remote session - new JT will acquire new session
        sessionDriver.stopRemoteSession(remoteSessionId);
      }
      if (!isStateRestoringEnabled(conf)) {
        // Change attempt id only if we're not restoring state
        attemptJobId = prepareNextAttempt(attemptJobId);
      } else {
        // notify the remote job tracker the number of 
        // remote job tracker get restarted
        remoteJTState.restartNum = numRemoteJTFailures;
      }
      // Stop RPC client.
      RPC.stopProxy(client);
      client = null;
      // Increment attempt to kill old JT on next connection attempt.
      incrementAttemptUnprotected();
      if (sessionDriver != null) {
        sessionDriver.setName("Launch pending for " + conf.getJobName());
      }
      // Restart remote JT, don't release client lock yet.
      waitForJTStart(conf);
      // Resubmit job directly.
      try {
        if (isJobSubmitted) {
          LOG.warn("Resubmitting job " + jobId.toString());
          client.submitJob(attemptJobId);
        }
        // Set our info server url in parent JT and CM.
        String url = getJobProfile(attemptJobId).getURL().toString();
        jt.setRemoteJTUrl(url);
        if (sessionDriver != null) {
          sessionDriver.setName("Launched session " + getRemoteSessionId());
        }
        // If reached this point assume success.
        LOG.warn("Successfully restarted remote JT.");
        if (remoteJTState != null) {
          if (LOG.isInfoEnabled()) {
            synchronized (remoteJTState) {
              LOG.warn(remoteJTState.getPrettyReport(attemptJobId));
            }
          }
        }
      } catch (IOException e) {
        // in case the new job tracker get failed when doing submitJob
        // or getJobProfile
        LOG.error("Exception happened when doing RJT restart, try it another time", 
             e);
        handleRemoteJTFailure(numRemoteJTFailures);
      }
    }
      
    /**
     * Prepares next attempt of job.
     * @param oldId a job id of last submitted attempt or id known by client
     * @return job id of next attempt
     * @throws IOException
     */
    private JobID prepareNextAttempt(final JobID oldId) throws IOException {
      JobID newId = CoronaJobTracker.nextJobID(oldId);
      // TODO copy only necessary files
      Path oldJobDir = new Path(jt.getSystemDir(), oldId.toString()),
           newJobDir = new Path(jt.getSystemDir(), newId.toString());
      FileSystem fs = FileSystem.get(conf);
      LOG.info("oldJobDir " + oldJobDir.toString() + " newJobDir " + newJobDir.toString() );
      // Copy job files.
      Path localTemp = new Path("file:///tmp/" + newId.toString());
      if (fs.exists(newJobDir)) {
        LOG.info("newJobDir "+ localTemp.toString() + " exists, delete it");
        fs.delete(newJobDir, true);
      }
      if (!oldJobDir.equals(newJobDir) && fs.exists(oldJobDir)) {
        fs.copyToLocalFile(oldJobDir, localTemp);
        fs.moveFromLocalFile(localTemp, newJobDir);
      }
      LOG.info("Job files copied to " + newJobDir.toString());
      return newId;
    }

    private T makeCallWithRetries() throws IOException {
      int errorCount = 0;
      final int maxErrorCount = 10; // can make configurable later
      IOException lastException = null;
      while (errorCount < maxErrorCount) {
        try {
          JobSubmissionProtocol myClient = checkClient();
          return call(myClient);
        } catch (ConnectException e) {
          throw e;
        } catch (IOException e) {
          lastException = e;
          errorCount++;
          if (errorCount == maxErrorCount) {
            break;
          } else {
            long backoff = errorCount * 1000;
            LOG.warn(
              "Retrying after error connecting to remote JT " +
                remoteJTHost + ":" + remoteJTPort +
                " will wait " + backoff + " msec ", e);
            try {
              Thread.sleep(backoff);
            } catch (InterruptedException ie) {
              throw new IOException(ie);
            }
            synchronized (RemoteJTProxy.this) {
              reinitClientUnprotected();
            }
          }
        }
      }
      LOG.error("Too many errors " + errorCount +
          " in connecting to remote JT " +
          remoteJTHost + ":" + remoteJTPort, lastException);
      throw lastException;
    }
  }
  
  /**
   * Check if the RPC client to the remote job tracker is ready, and wait if
   * not.
   * @throws IOException
   */
  private JobSubmissionProtocol checkClient() throws IOException {
    synchronized (this) {
      while (client == null) {
        try {
          if (remoteJTStatus == RemoteJTStatus.FAILURE) {
            throw new IOException("Remote Job Tracker is not available");
          }
          this.wait(1000);
        } catch (InterruptedException e) {
          throw new IOException(e);
        }
      }
      return client;
    }
  }
  
  /**
   * Check job configuration if state restoring is enabled
   * @param conf configuration of job
   * @return true iff enabled
   */
  public static boolean isStateRestoringEnabled(JobConf conf) {
    return isJTRestartingEnabled(conf)
        && conf.getBoolean(REMOTE_JT_STATE_RESTORING_CONF,
            REMOTE_JT_STATE_RESTORING_DEFAULT);
  }

  /**
   * Check job configuration if remote JT restarting is enabled
   * @param conf configuration of job
   * @return true iff enabled
   */
  public static boolean isJTRestartingEnabled(JobConf conf) {
    return (0 < conf.getInt(CoronaJobTracker.MAX_JT_FAILURES_CONF,
        CoronaJobTracker.MAX_JT_FAILURES_DEFAULT));
  }
}
