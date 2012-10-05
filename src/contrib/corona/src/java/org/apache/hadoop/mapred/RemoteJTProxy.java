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
import org.apache.hadoop.ipc.RPC.VersionIncompatible;

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
  throws VersionIncompatible, IOException {
    if (protocol.equals(InterCoronaJobTrackerProtocol.class.getName())) {
      return InterCoronaJobTrackerProtocol.versionID;
    } else {
      throw new IOException("Unknown protocol " + protocol);
    }
  }


  private void incrementAttemptUnprotected() {
    attempt++;
    currentAttemptId = new TaskAttemptID(currentAttemptId.getTaskID(), attempt);
  }

  void initializeClientUnprotected(String host, int port, String sessionId)
      throws IOException {
    if (client != null) {
      return;
    }
    LOG.info("Creating JT client to " + host + ":" + port);
    client = RPC.waitForProxy(JobSubmissionProtocol.class,
        JobSubmissionProtocol.versionID, new InetSocketAddress(host, port),
        conf);
    remoteSessionId = sessionId;
  }

  public void waitForJTStart(JobConf jobConf) throws IOException {
    int maxJTAttempts = jobConf.getInt(
        "mapred.coronajobtracker.remotejobtracker.attempts", 4);
    ResourceTracker resourceTracker = jt.resourceTracker;
    SessionDriver sessionDriver = jt.sessionDriver;
    List<ResourceGrant> excludeGrants = new ArrayList<ResourceGrant>();
    for (int i = 0; i < maxJTAttempts; i++) {
      ResourceGrant jtGrant = waitForJTGrant(resourceTracker, sessionDriver,
          excludeGrants);
      boolean success = startRemoteJT(jobConf, jtGrant, i);
      if (success) {
        return;
      } else {
        excludeGrants.add(jtGrant);
        resourceTracker.releaseResource(jtGrant.getId());
        List<ResourceRequest> released =
          resourceTracker.getResourcesToRelease();
        sessionDriver.releaseResources(released);
      }
    }
    throw new IOException("Could not start remote JT after " + maxJTAttempts
        + " attempts");
  }

  private ResourceGrant waitForJTGrant(
      ResourceTracker resourceTracker,
      SessionDriver sessionDriver,
      List<ResourceGrant> previousGrants) throws IOException {
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
    try {
      resourceTracker.processAvailableGrants(proc);
    } catch (InterruptedException e) {
      throw new IOException(e);
    }
    return grants.get(0);
  }

  private boolean startRemoteJT(
      JobConf jobConf, ResourceGrant grant, int attempt) throws IOException {
    org.apache.hadoop.corona.InetAddress ttAddr =
      Utilities.appInfoToAddress(grant.appInfo);
    CoronaTaskTrackerProtocol coronaTT = jt.trackerClientCache
        .getClient(
          new InetSocketAddress(ttAddr.getHost(), ttAddr.getPort()));
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
    CoronaSessionInfo info = new CoronaSessionInfo(jt.sessionId,
        jt.jobTrackerAddress);
    coronaTT.startCoronaJobTracker(jobTask, info);

    // Now wait for the remote CJT to report its address.
    final long waitStart = System.currentTimeMillis();
    final long timeout = RemoteJTProxy.getRemotJTTimeout(jobConf);
    synchronized (this) {
      while (client == null) {
        LOG.info("Waiting for remote JT to start on " + ttAddr.getHost());
        try {
          this.wait(1000);
        } catch (InterruptedException e) {
          throw new IOException("Interrupted while waiting for JT");
        }
        if (System.currentTimeMillis() - waitStart > timeout) {
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
      JobStatus call() throws IOException {
        return client.submitJob(jobId);
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
      JobID call() throws IOException {
        client.killJob(jobId);
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
      Boolean call() throws IOException {
        return client.killTask(taskId, shouldFail);
      }
    }).makeCall();
  }

  @Override
  public JobProfile getJobProfile(final JobID jobId) throws IOException {
    return (new Caller<JobProfile>() {
      @Override
      JobProfile call() throws IOException {
        return client.getJobProfile(jobId);
      }
    }).makeCall();
  }

  @Override
  public JobStatus getJobStatus(final JobID jobId) throws IOException {
    return (new Caller<JobStatus>() {
      @Override
      JobStatus call() throws IOException {
        return client.getJobStatus(jobId);
      }
    }).makeCall();
  }

  @Override
  public Counters getJobCounters(final JobID jobId) throws IOException {
    return (new Caller<Counters>() {
      @Override
      Counters call() throws IOException {
        return client.getJobCounters(jobId);
      }
    }).makeCall();
  }

  @Override
  public TaskReport[] getMapTaskReports(final JobID jobId) throws IOException {
    return (new Caller<TaskReport[]>() {
      @Override
      TaskReport[] call() throws IOException {
        return client.getMapTaskReports(jobId);
      }
    }).makeCall();
  }

  @Override
  public TaskReport[] getReduceTaskReports(final JobID jobId)
      throws IOException {
    return (new Caller<TaskReport[]>() {
      @Override
      TaskReport[] call() throws IOException {
        return client.getReduceTaskReports(jobId);
      }
    }).makeCall();
  }

  @Override
  public TaskReport[] getCleanupTaskReports(final JobID jobId)
      throws IOException {
    return (new Caller<TaskReport[]>() {
      @Override
      TaskReport[] call() throws IOException {
        return client.getCleanupTaskReports(jobId);
      }
    }).makeCall();
  }

  @Override
  public TaskReport[] getSetupTaskReports(final JobID jobId)
    throws IOException {
    return (new Caller<TaskReport[]>() {
      @Override
      TaskReport[] call() throws IOException {
        return client.getSetupTaskReports(jobId);
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
      TaskCompletionEvent[] call() throws IOException {
        return client.getTaskCompletionEvents(jobid, fromEventId, maxEvents);
      }
    }).makeCall();
  }

  @Override
  public String[] getTaskDiagnostics(final TaskAttemptID taskId)
      throws IOException {
    return (new Caller<String[]>() {
      @Override
      String[] call() throws IOException {
        return client.getTaskDiagnostics(taskId);
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

  private abstract class Caller<T> {
    abstract T call() throws IOException;

    public T makeCall() throws IOException {
      try {
        checkClient();
        return call();
      } catch (IOException e) {
        LOG.error("Error on remote call ", e);
        handleCallFailure();
        throw e;
      }
    }
  }

  private void handleCallFailure() throws IOException {
    jt.close(false);
  }

  private void checkClient() throws IOException {
    synchronized (this) {
      if (client == null) {
        try {
          this.wait();
        } catch (InterruptedException e) {
          throw new IOException(e);
        }
      }
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
