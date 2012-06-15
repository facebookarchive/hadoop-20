package org.apache.hadoop.mapred;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Collections;
import java.net.InetSocketAddress;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.ProtocolSignature;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RPC.VersionIncompatible;
import org.apache.hadoop.ipc.VersionedProtocol;
import org.apache.hadoop.mapred.SortedRanges.Range;

/**
 * TaskUmbilicalProtocol used by Task in Corona
 * In Corona we allow each individual Task to talk to JobTracker directly.
 */
class DirectTaskUmbilical implements TaskUmbilicalProtocol {

  public static final Log LOG = LogFactory.getLog(DirectTaskUmbilical.class);

  final private TaskUmbilicalProtocol taskTrackerUmbilical;
  final private InterTrackerProtocol jobTracker;
  final private List<TaskCompletionEvent> mapEventFetched;
  private int totalEventsFetched = 0;
  static final String MAPRED_DIRECT_TASK_UMBILICAL_ADDRESS = "mapred.direct.task.umbilical.address";

  public static DirectTaskUmbilical createDirectUmbilical(
    TaskUmbilicalProtocol taskTracker,
    InetSocketAddress jobTrackerAddress, JobConf conf) throws IOException {

    LOG.info("Creating direct umbilical to " + jobTrackerAddress.toString());
    long jtConnectTimeoutMsec = conf.getLong(
      "corona.jobtracker.connect.timeout.msec", 60000L);
    int rpcTimeout = (int) jtConnectTimeoutMsec;

    InterTrackerProtocol jobClient = RPC.waitForProxy(
      InterTrackerProtocol.class,
      InterTrackerProtocol.versionID,
      jobTrackerAddress,
      conf,
      jtConnectTimeoutMsec,
      rpcTimeout);

    return new DirectTaskUmbilical(taskTracker, jobClient);
  }

  public List<VersionedProtocol> getCreatedProxies() {
    return Collections.singletonList((VersionedProtocol)jobTracker);
  }

  public void close() {
    RPC.stopProxy(jobTracker);
  }

  DirectTaskUmbilical(TaskUmbilicalProtocol taskTrackerUmbilical,
      InterTrackerProtocol jobTracker) {
    this.taskTrackerUmbilical = taskTrackerUmbilical;
    this.jobTracker = jobTracker;
    this.mapEventFetched = new ArrayList<TaskCompletionEvent>();
  }

  @Override
  public long getProtocolVersion(String protocol, long clientVersion)
      throws VersionIncompatible, IOException {
    return taskTrackerUmbilical.getProtocolVersion(protocol, clientVersion);
  }

  @Override
  public ProtocolSignature getProtocolSignature(String protocol,
      long clientVersion, int clientMethodsHash) throws IOException {
    return taskTrackerUmbilical.getProtocolSignature(
        protocol, clientVersion, clientMethodsHash);
  }

  @Override
  public JvmTask getTask(JvmContext context) throws IOException {
    return taskTrackerUmbilical.getTask(context);
  }

  @Override
  public boolean statusUpdate(TaskAttemptID taskId, TaskStatus taskStatus)
      throws IOException, InterruptedException {
    return taskTrackerUmbilical.statusUpdate(taskId, taskStatus);
  }

  @Override
  public void reportDiagnosticInfo(TaskAttemptID taskid, String trace)
      throws IOException {
    taskTrackerUmbilical.reportDiagnosticInfo(taskid, trace);
  }

  @Override
  public void reportNextRecordRange(TaskAttemptID taskid, Range range)
      throws IOException {
    taskTrackerUmbilical.reportNextRecordRange(taskid, range);
  }

  @Override
  public boolean ping(TaskAttemptID taskid) throws IOException {
    return taskTrackerUmbilical.ping(taskid);
  }

  @Override
  public void done(TaskAttemptID taskid) throws IOException {
    taskTrackerUmbilical.done(taskid);
  }

  @Override
  public void commitPending(TaskAttemptID taskId, TaskStatus taskStatus)
      throws IOException, InterruptedException {
    taskTrackerUmbilical.commitPending(taskId, taskStatus);
  }

  @Override
  public boolean canCommit(TaskAttemptID taskid) throws IOException {
    return taskTrackerUmbilical.canCommit(taskid);
  }

  @Override
  public void shuffleError(TaskAttemptID taskId, String message)
      throws IOException {
    taskTrackerUmbilical.shuffleError(taskId, message);
  }

  @Override
  public void fsError(TaskAttemptID taskId, String message) throws IOException {
    taskTrackerUmbilical.fsError(taskId, message);
  }

  @Override
  public void fatalError(TaskAttemptID taskId, String message)
      throws IOException {
    taskTrackerUmbilical.fatalError(taskId, message);
  }

  @Override
  public MapTaskCompletionEventsUpdate getMapCompletionEvents(JobID jobId,
      int fromIndex, int maxLocs, TaskAttemptID id) throws IOException {
    TaskCompletionEvent[] recentEvents =
        jobTracker.getTaskCompletionEvents(jobId, totalEventsFetched, Integer.MAX_VALUE);
    totalEventsFetched += recentEvents.length;
    for (TaskCompletionEvent event : recentEvents) {
      if (event.isMapTask()) {
        mapEventFetched.add(event);
      }
    }
    int toIndex = fromIndex + maxLocs;
    toIndex = toIndex > mapEventFetched.size() ? mapEventFetched.size() : toIndex;
    TaskCompletionEvent[] result = mapEventFetched.subList(fromIndex, toIndex).
        toArray(new TaskCompletionEvent[toIndex - fromIndex]);
    return new MapTaskCompletionEventsUpdate(result, false);
  }
}
