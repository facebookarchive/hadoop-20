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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.corona.ResourceGrant;
import org.apache.hadoop.corona.ResourceRequest;
import org.apache.hadoop.corona.SessionDriver;
import org.apache.hadoop.corona.Utilities;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.ipc.ProtocolSignature;
import org.apache.hadoop.ipc.RPC;

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
  /** Default amount of time to wait for remote JT to launch. */
  public static final int REMOTE_JT_TIMEOUT_SEC_DEFAULT = 60;
  /** The proxy object to the CoronaJobTracker running in the cluster */
  private JobSubmissionProtocol client;
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
  /** The id of the job */
  private final JobID jobId;
  /** The session id for the job tracker running in the cluster */
  private String remoteSessionId;

  private enum RemoteJTStatus {
    UNINITIALIZED,
    SUCCESS,
    FAILURE
  };
  private RemoteJTStatus remoteJTStatus;

  /**
   * Construct a proxy for the remote job tracker
   * @param jt parent job tracker
   * @param jobId id of the job the proxy is created for
   * @param conf job configuration
   */
  @SuppressWarnings("deprecation")
  RemoteJTProxy(CoronaJobTracker jt, JobID jobId, JobConf conf) {
    this.conf = conf;
    this.jt = jt;
    this.jobId = jobId;
    attempt = 0;
    int partitionId = conf.getNumMapTasks() + 100000;
    currentAttemptId = new TaskAttemptID(new TaskID(jobId, true, partitionId),
        attempt);
    remoteJTStatus = RemoteJTStatus.UNINITIALIZED;
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
      if (!attemptId.equals(currentAttemptId)) {
        throw new IOException("Attempt " + attempt +
            " does not match current attempt " + currentAttemptId);
      }
      initializeClientUnprotected(host, port, sessionId);
      this.notifyAll();
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
    currentAttemptId = new TaskAttemptID(currentAttemptId.getTaskID(), attempt);
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
    client = RPC.waitForProtocolProxy(
      JobSubmissionProtocol.class,
      JobSubmissionProtocol.versionID,
      new InetSocketAddress(host, port),
      conf,
      connectTimeout).getProxy();
    remoteJTStatus = RemoteJTStatus.SUCCESS;
    remoteJTHost = host;
    remoteJTPort = port;
    remoteSessionId = sessionId;
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
   * Wait for the remote corona job tracker to be ready.
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
    for (int i = 0; i < maxJTAttempts; i++) {
      try {
        ResourceGrant jtGrant = waitForJTGrant(resourceTracker, sessionDriver,
            excludeGrants);
        boolean success = startRemoteJT(jobConf, jtGrant);
        if (success) {
          return;
        } else {
          excludeGrants.add(jtGrant);
          resourceTracker.releaseResource(jtGrant.getId());
          List<ResourceRequest> released =
            resourceTracker.getResourcesToRelease();
          sessionDriver.releaseResources(released);
        }
      } catch (InterruptedException e) {
        throw new IOException(
          "Interrupted while waiting for remote JT start for " + jobId, e);
      }

    }
    throw new IOException("Could not start remote JT for " + jobId +
      " after " + maxJTAttempts + " attempts");
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
    LOG.info("Waiting for JT grant for " + jobId);
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
        throw new IOException("Session error for job " + jobId, e);
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
        ":" + ttAddr.getPort() + " for job " + jobId, e);
      return false;
    }
    LOG.info("Starting remote JT for " + jobId + " on " + ttAddr.getHost());

    // Get a special map id for the JT task.
    Path systemDir = new Path(jt.getSystemDir());
    String jobFile = CoronaJobInProgress.getJobFile(systemDir, jobId)
        .toString();
    String splitClass = JobClient.RawSplit.class.getName();
    BytesWritable split = new BytesWritable();
    Task jobTask = new MapTask(
        jobFile, currentAttemptId, currentAttemptId.getTaskID().getId(),
        splitClass, split, 1, jobConf.getUser());
    CoronaSessionInfo info = new CoronaSessionInfo(jt.getSessionId(),
        jt.getJobTrackerAddress());
    synchronized (this) {
      try {
        coronaTT.startCoronaJobTracker(jobTask, info);
      } catch (IOException e) {
        // Increment the attempt so that the older attempt will get an error
        // in reportRemoteCoronaJobTracker().
        incrementAttemptUnprotected();
        LOG.error("Error while performing RPC to TT at " + ttAddr.getHost() +
          ":" + ttAddr.getPort() + " for job " + jobId, e);
        return false;
      }
    }

    // Now wait for the remote CJT to report its address.
    final long waitStart = System.currentTimeMillis();
    final long timeout = RemoteJTProxy.getRemotJTTimeout(jobConf);
    synchronized (this) {
      while (client == null) {
        LOG.info("Waiting for remote JT to start on " + ttAddr.getHost());
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
        return myClient.submitJob(jobId);
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
      @Override
      JobID call(JobSubmissionProtocol myClient) throws IOException {
        myClient.killJob(jobId);
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
        return myClient.getJobProfile(jobId);
      }
    }).makeCall();
  }

  @Override
  public JobStatus getJobStatus(final JobID jobId) throws IOException {
    return (new Caller<JobStatus>() {
      @Override
      JobStatus call(JobSubmissionProtocol myClient) throws IOException {
        return myClient.getJobStatus(jobId);
      }
    }).makeCall();
  }

  @Override
  public Counters getJobCounters(final JobID jobId) throws IOException {
    return (new Caller<Counters>() {
      @Override
      Counters call(JobSubmissionProtocol myClient) throws IOException {
        return myClient.getJobCounters(jobId);
      }
    }).makeCall();
  }

  @Override
  public TaskReport[] getMapTaskReports(final JobID jobId) throws IOException {
    return (new Caller<TaskReport[]>() {
      @Override
      TaskReport[] call(JobSubmissionProtocol myClient) throws IOException {
        return myClient.getMapTaskReports(jobId);
      }
    }).makeCall();
  }

  @Override
  public TaskReport[] getReduceTaskReports(final JobID jobId)
    throws IOException {
    return (new Caller<TaskReport[]>() {
      @Override
      TaskReport[] call(JobSubmissionProtocol myClient) throws IOException {
        return myClient.getReduceTaskReports(jobId);
      }
    }).makeCall();
  }

  @Override
  public TaskReport[] getCleanupTaskReports(final JobID jobId)
    throws IOException {
    return (new Caller<TaskReport[]>() {
      @Override
      TaskReport[] call(JobSubmissionProtocol myClient) throws IOException {
        return myClient.getCleanupTaskReports(jobId);
      }
    }).makeCall();
  }

  @Override
  public TaskReport[] getSetupTaskReports(final JobID jobId)
    throws IOException {
    return (new Caller<TaskReport[]>() {
      @Override
      TaskReport[] call(JobSubmissionProtocol myClient) throws IOException {
        return myClient.getSetupTaskReports(jobId);
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
        return myClient.getTaskCompletionEvents(jobid, fromEventId, maxEvents);
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

  public void close() {
    synchronized (this) {
      if (client != null) {
        RPC.stopProxy(client);
      }
    }
  }

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
     * Template function to make the call.
     * @return The generic return value.
     * @throws IOException
     */
    public T makeCall() throws IOException {
      try {
        return makeCallWithRetries();
      } catch (IOException e) {
        LOG.error("Error on remote call for job " + jobId, e);
        handleCallFailure();
        throw e;
      }
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
   * Handle failures while making calls to the remote corona job tracker.
   * We need to close the local job tracker.
   * @throws IOException
   */
  private void handleCallFailure() throws IOException {
    try {
      jt.close(false, true);
    } catch (InterruptedException e) {
      throw new IOException(e);
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
}
