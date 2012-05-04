package org.apache.hadoop.mapred;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import junit.framework.TestCase;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.corona.ClusterManager;
import org.apache.hadoop.corona.ResourceGrant;
import org.apache.hadoop.corona.ResourceRequest;
import org.apache.hadoop.corona.SessionDriver;
import org.apache.hadoop.corona.TstUtils;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.ipc.ProtocolSignature;
import org.apache.hadoop.ipc.RPC.VersionIncompatible;
import org.apache.hadoop.mapred.TaskStatus.Phase;
import org.apache.hadoop.mapred.UtilsForTests.FakeClock;

public class TestSingleJobTracker extends TestCase {
  final static Log LOG = LogFactory.getLog(TestSingleJobTracker.class);

  Configuration conf;
  String sessionId;
  FakeTrackerClientCache trackerClientCache;
  CoronaJobTracker jt;

  protected void setUp() {
    conf = new Configuration();
    sessionId = "session_1";

    trackerClientCache = new FakeTrackerClientCache(conf);
    // Create a SingleJT that does not talk have a session driver.
    try {
      jt = new CoronaJobTracker(
          new JobConf(conf), sessionId, trackerClientCache);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  protected void tearDown() {
    conf = null;
    sessionId = "";
    trackerClientCache = null;
    try {
      jt.close(false);
    } catch (IOException e) {
      e.printStackTrace();
    }
    jt = null;
  }


  private JobClient.RawSplit createDummySplit(String[] locations) {
    JobClient.RawSplit split = new JobClient.RawSplit();
    split.setClassName("dummy");
    split.setDataLength(0);
    byte dummy[] = new byte[0];
    split.setBytes(dummy, 0, 0);
    split.setLocations(locations);
    return split;
  }

  private void writeJobFile(String systemDir, JobConf jobConf, JobID jobId)
      throws IOException {
    Path sysDir = new Path(systemDir);
    FileSystem fs = sysDir.getFileSystem(conf);
    Path submitJobDir = new Path(systemDir, jobId.toString());
    Path submitJobFile = new Path(submitJobDir, "job.xml");
    FSDataOutputStream out = fs.create(submitJobFile);

    LOG.info("Writing job file " + submitJobFile);
    try {
      jobConf.writeXml(out);
    } finally {
      out.close();
    }
  }
  
  static class FakeCoronaTaskTracker implements CoronaTaskTrackerProtocol {
    List<TaskTrackerAction> submittedActions =
      new ArrayList<TaskTrackerAction>();

    @Override
    public void submitActions(TaskTrackerAction[] actions) throws IOException,
        InterruptedException {
      for (int i = 0; i < actions.length; i++) {
        submittedActions.add(actions[i]);
      }
    }

    @Override
    public long getProtocolVersion(String protocol, long clientVersion)
        throws VersionIncompatible, IOException {
      return CoronaTaskTrackerProtocol.versionID;
    }

    @Override
    public ProtocolSignature getProtocolSignature(String protocol,
        long clientVersion, int clientMethodsHash) throws IOException {
      return ProtocolSignature.getProtocolSignature(
          this, protocol, clientVersion, clientMethodsHash);
    }
  }

  static class FakeTrackerClientCache extends
      CoronaJobTracker.TrackerClientCache {
    FakeTrackerClientCache(Configuration conf) {
      super(conf);
    }

    public CoronaTaskTrackerProtocol createClient(InetSocketAddress s) {
      return new FakeCoronaTaskTracker();
    }
  }
  
  @SuppressWarnings("deprecation")
  private CoronaJobInProgress setupJob(CoronaJobTracker jt,
      JobClient.RawSplit[] splits, int numReduces) throws IOException {
    return setupJob(jt, new JobConf(conf), splits, numReduces);
  }
  @SuppressWarnings("deprecation")
  private CoronaJobInProgress setupJob(CoronaJobTracker jt,
      JobConf jobConf, JobClient.RawSplit[] splits, int numReduces) throws IOException {

    JobID jobId = jt.getNewJobId();
    LOG.info("Job ID: " + jobId);

    int numSplits = splits.length;
    for (int i = 0; i < splits.length; i++) {
      String[] locations = new String[1];
      locations[0] = TstUtils.getNodeHost(i);
      splits[i] = createDummySplit(locations);
    }

    jobConf.setNumMapTasks(numSplits);
    jobConf.setNumReduceTasks(numReduces);
    writeJobFile(jt.getSystemDir(), jobConf, jobId);

    // Simulate submitJob().
    CoronaJobInProgress jip = jt.createJob(jobId, jobConf);
    jip.initTasksFromSplits(splits);
    SessionDriver driver = null;
    jt.startJob(jip, driver);
    return jip;
  }

  private TaskStatus finishedMapStatus(TaskAttemptID taskId) {
    return new MapTaskStatus(taskId, 100.0f, 1, TaskStatus.State.SUCCEEDED,
        "", "", "", null, new Counters());
  }

  private TaskStatus runningMapStatus(TaskAttemptID taskId, String trackerName) {
    TaskStatus stat = new MapTaskStatus(taskId, 10.1f, 1, TaskStatus.State.RUNNING,
        "", "", trackerName, null, null);
    stat.setStartTime(System.currentTimeMillis());
    return stat;
  }

  private TaskStatus killedMapStatus(TaskAttemptID taskId, String trackerName) {
    return new MapTaskStatus(taskId, 100.0f, 1, TaskStatus.State.KILLED_UNCLEAN,
        "", "", trackerName, null, null);
  }

  private TaskStatus runningReduceStatus(TaskAttemptID taskId, String trackerName) {
    TaskStatus stat = new ReduceTaskStatus(taskId, 10.1f, 1, TaskStatus.State.RUNNING,
        "", "", trackerName, null, null);
    stat.setStartTime(System.currentTimeMillis());
    return stat;
  }

  /**
   * Move a job through setup->running->end.
   * @throws Exception
   */
  public void testStartEnd() throws Exception {
    final int numSplits = 10;
    final int numReduces = 2;
    JobClient.RawSplit[] splits = new JobClient.RawSplit[numSplits];
    CoronaJobInProgress jip = setupJob(jt, splits, numReduces);

    List<ResourceRequest> wanted =
      jt.resourceTracker.getWantedResources();
    // Verify wanted resources.
    assertEquals(numSplits + numReduces, wanted.size());
    assertEquals(0, jt.resourceTracker.grantedResources.size());
    assertEquals(0, jt.resourceTracker.availableResources.size());

    // Simulate a resource grant from cluster manager.
    ResourceRequest want = wanted.get(0);
    ResourceGrant grant = new ResourceGrant
        (want.getId(), TstUtils.getNodeHost(0), TstUtils.getNodeAddress(0),
        ClusterManager.clock.getTime(), want.getType());
    grant.setAppInfo(
        TstUtils.getNodeHost(0) + ":" + TstUtils.getNodeAddress(0));

    // Send a fake grant response. Check the resultant change.
    jt.grantResource(sessionId, Collections.singletonList(grant));
    assertEquals(1, jt.resourceTracker.grantedResources.size());
    assertEquals(1, jt.resourceTracker.availableResources.size());

    // Pump the tracker. Available grants should go down, pending actions
    // show go up.
    jt.assignTasks();
    assertEquals(1, jt.resourceTracker.grantedResources.size());
    assertEquals(0, jt.resourceTracker.availableResources.size());
    assertEquals(1, jt.actionsToSend.size());
    LOG.info("Actions to send: " + jt.actionsToSend.size());

    jt.launchTasks();
    assertEquals(1, trackerClientCache.trackerClients.size());
    TaskAttemptID taskId = null;
    int numSubmittedActions = 0;
    for (CoronaTaskTrackerProtocol client :
      trackerClientCache.trackerClients.values()) {
      FakeCoronaTaskTracker fakeClient = (FakeCoronaTaskTracker) client;
      numSubmittedActions++;
      LaunchTaskAction launchAction =
        (LaunchTaskAction) fakeClient.submittedActions.get(0);
      assertTrue("Found non job-setup task",
          launchAction.getTask().isJobSetupTask());
      taskId = launchAction.getTask().getTaskID();
    }
    assertEquals(1, numSubmittedActions);

    // Simulate a heartbeat from a tracker.
    List<TaskStatus> taskReports = new ArrayList<TaskStatus>();
    LOG.info("Reporting success for " + taskId);
    taskReports.add(finishedMapStatus(taskId));
    TaskTrackerStatus ttStatus = new TaskTrackerStatus(
        TstUtils.getNodeHost(0), TstUtils.getNodeHost(0),
        8080, taskReports,  0, 0, 0);
    jt.heartbeat(ttStatus, false, false, false, (short) 1);

  }

  public void testAssignment() throws Exception {
    LOG.info("Starting testAssignment");
    final int numSplits = 10;
    final int numReduces = 2;
    JobClient.RawSplit[] splits = new JobClient.RawSplit[numSplits];
    CoronaJobInProgress jip = setupJob(jt, splits, numReduces);

    List<ResourceRequest> wanted =
      jt.resourceTracker.getWantedResources();
    List<ResourceGrant> grants = new ArrayList<ResourceGrant>();
    int next = numSplits;
    for (ResourceRequest req: wanted) {
      String host = req.getHosts() != null ?
          req.getHosts().get(0) : TstUtils.getNodeHost(next++);
      ResourceGrant grant = new ResourceGrant(
          req.getId(), host, TstUtils.getNodeAddress(next++),
          ClusterManager.clock.getTime(), req.getType());
      grant.setAppInfo(grant.getAddress().host + ":" + grant.getAddress().getPort());
      grants.add(grant);
    }

    jt.grantResource(sessionId, grants);
    assertEquals(grants.size(), jt.resourceTracker.availableResources.size());
    jip.completeSetup(); // So that other tasks can be assigned.

    jt.assignTasks();
    // All grants are assigned.
    assertEquals(0, jt.resourceTracker.availableResources.size());
  }

  static String trackers[] = new String[] {"tracker_tracker1:1000", 
    "tracker_tracker2:1000", "tracker_tracker3:1000",
    "tracker_tracker4:1000", "tracker_tracker5:1000"};


  static class SpecFakeClock extends FakeClock {
    // assuming default map/reduce speculative lags are 
    // identical in value. just need to store one of them
    long speculative_lag;

    public SpecFakeClock(long speculative_lag) {
      this.speculative_lag = speculative_lag;
    }

    public void advanceBySpeculativeLag() {
      time += speculative_lag;
    }
  };
  
  @SuppressWarnings("deprecation")
  private void finishTask(
      CoronaJobInProgress job, TaskInProgress tip, TaskAttemptID taskId,
      String host) {
    TaskStatus status = TaskStatus.createTaskStatus(tip.isMapTask(), taskId, 
        1.0f, 1, TaskStatus.State.SUCCEEDED, "", "", 
        host,
        tip.isMapTask() ? Phase.MAP : Phase.REDUCE, new Counters());
    List<TaskStatus> taskReports = new ArrayList<TaskStatus>();
    taskReports.add(finishedMapStatus(taskId));
    TaskTrackerStatus ttStatus = new TaskTrackerStatus(
        TstUtils.getNodeHost(0), TstUtils.getNodeHost(0),
        8080, taskReports,  0, 0, 0);
    job.updateTaskStatus(tip, status, ttStatus);
  }  

  @SuppressWarnings("deprecation")
  public void testTaskToSpeculate() throws IOException {
    final int numSplits = 5;
    final int numReduces = 0;
    JobClient.RawSplit[] splits = new JobClient.RawSplit[numSplits];
    JobConf jobConf = new JobConf();
    jobConf.setSpeculativeExecution(true);
    jobConf.setFloat(JobInProgress.SPECULATIVE_SLOWNODE_THRESHOLD, 100f);
    jobConf.setFloat(JobInProgress.SPECULATIVE_SLOWTASK_THRESHOLD, 0.5f);
    SpecFakeClock clock = new SpecFakeClock(jobConf.getMapSpeculativeLag());
    JobTracker.clock = clock;
    try {
    CoronaJobInProgress jip = setupJob(jt, jobConf, splits, numReduces);
    List<ResourceRequest> wanted = jt.resourceTracker.getWantedResources();
    assertEquals(5, wanted.size());

    List<ResourceGrant> grants = new ArrayList<ResourceGrant>();
    for (int i = 0; i < 5; i++) {
      grants.add(new ResourceGrant(
          i + 1, TstUtils.getNodeHost(i), TstUtils.getNodeAddress(i),
          clock.getTime(), wanted.get(i).getType()));
      grants.get(i).setAppInfo("192.168.0.1:1234");
    }
    jt.resourceTracker.addNewGrants(grants);
    jip.completeSetup();
    Task[] mapTasks = new Task[5];
    for (int i = 0; i < 5; i++) {
      ResourceGrant grant = grants.get(i);
      mapTasks[i] = jip.obtainNewMapTaskForTip(grant.getNodeName(), grant.address.host, jip.maps[i]);
      assertFalse("map task is null", mapTasks[i] == null);
    }
    clock.advance(5000);
    finishTask(jip, jip.maps[0], mapTasks[0].getTaskID(), TstUtils.getNodeHost(0));
    clock.advance(1000);
    finishTask(jip, jip.maps[1], mapTasks[1].getTaskID(), TstUtils.getNodeHost(1));
    clock.advanceBySpeculativeLag();
    
    // Two maps have finished, and we have advanced the clock sufficiently.
    // The other 3 maps should be speculated.
    assertEquals(0, jt.resourceTracker.getWantedResources().size());
    jip.updateSpeculationRequests();
    assertEquals(3, jt.resourceTracker.getWantedResources().size());
    assertEquals(3, jt.resourceTracker.numSpeculativeRequests(
                                        ResourceTracker.RESOURCE_TYPE_MAP));
    assertEquals(0, jt.resourceTracker.numSpeculativeRequests(
                                        ResourceTracker.RESOURCE_TYPE_REDUCE));
    } finally {
      JobTracker.clock = JobTracker.DEFAULT_CLOCK;
    }
  }


  public void testPreemption() throws Exception {
    final int numSplits = 1;
    final int numReduces = 0;
    JobClient.RawSplit[] splits = new JobClient.RawSplit[numSplits];
    CoronaJobInProgress jip = setupJob(jt, splits, numReduces);

    List<ResourceRequest> wanted =
      jt.resourceTracker.getWantedResources();
    List<ResourceGrant> grants = new ArrayList<ResourceGrant>();
    int next = numSplits;
    for (ResourceRequest req: wanted) {
      String host = req.getHosts() != null ?
        req.getHosts().get(0) : TstUtils.getNodeHost(next++);
      ResourceGrant grant = new ResourceGrant(
                          req.getId(), host, TstUtils.getNodeAddress(next++),
                          ClusterManager.clock.getTime(), req.getType());
      grant.setAppInfo(grant.getAddress().host + ":" + grant.getAddress().getPort());
      grants.add(grant);
    }

    jt.grantResource(sessionId, grants);
    jip.completeSetup(); // So that other tasks can be assigned.

    assertEquals(0, jt.actionsToSend.size());
    jt.assignTasks();
    assertEquals(1, jt.actionsToSend.size());
    Task attempt = ((LaunchTaskAction)jt.actionsToSend.get(0).action).getTask();
    jt.actionsToSend.clear();

    TaskStatus runningStatus = runningMapStatus(attempt.getTaskID(), "tracker_foo");
    jt.job.maps[0].updateStatus(runningStatus);

    // Revoke the resource.
    jt.revokeResource("", grants, false);
    assertEquals(0, jt.resourceTracker.getResourcesToRelease().size());
    assertEquals(0, jt.actionsToSend.size());
    // Call the update function. We should have one killTask action created.
    jt.resourceUpdater.updateResources();
    assertEquals(0, jt.resourceTracker.getResourcesToRelease().size());
    assertEquals(1, jt.actionsToSend.size());

    // Update the JT with the killed task result.
    jt.actionsToSend.clear();
    LOG.info("Reporting killed for " + attempt.getTaskID());
    TaskStatus killedStatus = killedMapStatus(attempt.getTaskID(), "tracker_foo");
    TaskTrackerStatus ttStatus = new TaskTrackerStatus(
        TstUtils.getNodeHost(0), TstUtils.getNodeHost(0),
        8080, Collections.singletonList(killedStatus), 0, 0, 0);
    jip.updateTaskStatus(jt.job.maps[0], killedStatus, ttStatus);
    jt.grantResource(sessionId, grants);
    jt.assignTasks();
    assertEquals(1, jt.actionsToSend.size());
  }

  public void testExpireLaunching() throws Exception {
    final int numSplits = 1;
    final int numReduces = 0;
    JobClient.RawSplit[] splits = new JobClient.RawSplit[numSplits];
    CoronaJobInProgress jip = setupJob(jt, splits, numReduces);

    List<ResourceRequest> wanted =
      jt.resourceTracker.getWantedResources();
    List<ResourceGrant> grants = new ArrayList<ResourceGrant>();
    ResourceRequest req = wanted.get(0);
    String host = req.getHosts() != null ?
        req.getHosts().get(0) : TstUtils.getNodeHost(0);
    ResourceGrant grant = new ResourceGrant(
          req.getId(), host, TstUtils.getNodeAddress(0),
          ClusterManager.clock.getTime(), req.getType());
    grant.setAppInfo(grant.getAddress().host + ":" + grant.getAddress().getPort());
    grants.add(grant);

    jt.grantResource(sessionId, grants);
    jip.completeSetup(); // So that other tasks can be assigned.

    assertEquals(0, jt.actionsToSend.size());
    jt.assignTasks();
    assertEquals(1, jt.actionsToSend.size());
    TaskAttemptID taskId = ((LaunchTaskAction)(jt.actionsToSend.get(0).action)).getTask().getTaskID();
    jt.actionsToSend.clear();

    assertEquals(0, jt.resourceTracker.getResourcesToRelease().size());
    assertTrue(jt.expireLaunchingTasks.launchingTasks.containsKey(taskId));
    jt.expireLaunchingTasks.launchingTasks.put(taskId, new Long(0));
    assertEquals(0, jt.job.failedMapTasks);
    jt.expireLaunchingTasks.expireLaunchingTasks();
    assertEquals(1, jt.job.failedMapTasks);
    assertEquals(1, jt.resourceTracker.getResourcesToRelease().size());
  }
  
  public void testFetchFailures() throws Exception {
    final int numSplits = 1;
    final int numReduces = 1;
    JobClient.RawSplit[] splits = new JobClient.RawSplit[numSplits];
    CoronaJobInProgress jip = setupJob(jt, splits, numReduces);
 
    // Simulate a resource grant from cluster manager.
    List<ResourceRequest> wanted = jt.resourceTracker.getWantedResources();
    assertEquals(2, wanted.size());
 
    ResourceRequest mapRequest = null;
    ResourceRequest reduceRequest = null;
    for (ResourceRequest req: wanted) {
      if (req.getType() == ResourceTracker.RESOURCE_TYPE_MAP) {
        mapRequest = req;
      } else {
        reduceRequest = req;
      }
    }
    assertNotNull(mapRequest);
    assertNotNull(reduceRequest);
 
    ResourceGrant mapGrant = new ResourceGrant
      (mapRequest.getId(), TstUtils.getNodeHost(0), TstUtils.getNodeAddress(0),
      ClusterManager.clock.getTime(), mapRequest.getType());
    mapGrant.setAppInfo(
        TstUtils.getNodeHost(0) + ":" + TstUtils.getNodeAddress(0));
    ResourceGrant reduceGrant = new ResourceGrant(
      reduceRequest.getId(), TstUtils.getNodeHost(1), TstUtils.getNodeAddress(1),
      ClusterManager.clock.getTime(), reduceRequest.getType());
    reduceGrant.setAppInfo(
        TstUtils.getNodeHost(1) + ":" + TstUtils.getNodeAddress(1));

    // Grant the map resource.
    jt.grantResource(sessionId, Collections.singletonList(mapGrant));
    jip.completeSetup(); // So that other tasks can be assigned.
    jt.assignTasks();
    assertEquals(1, jt.actionsToSend.size());
    Task mapAttempt = ((LaunchTaskAction)jt.actionsToSend.get(0).action).getTask();
    jt.actionsToSend.clear();
    TaskStatus finishedMapStatus = finishedMapStatus(mapAttempt.getTaskID());
    // Make the map finished.
    TaskTrackerStatus ttStatus = new TaskTrackerStatus(
      TstUtils.getNodeHost(0), TstUtils.getNodeHost(0),
      8080, Collections.singletonList(finishedMapStatus),  0, 0, 0);
    jt.heartbeat(ttStatus, false, false, false, (short) 1);

    // Grant the reduce resource.
    jt.grantResource(sessionId, Collections.singletonList(reduceGrant));
    jt.assignTasks();
    assertEquals(1, jt.actionsToSend.size());
    Task reduceAttempt = ((LaunchTaskAction)jt.actionsToSend.get(0).action).getTask();
    jt.actionsToSend.clear();
    TaskStatus runningReduceStatus = runningReduceStatus(reduceAttempt.getTaskID(), "tracker_foo");
    runningReduceStatus.addFetchFailedMap(mapAttempt.getTaskID());

    // Send heartbeats with fetch failure notifications.
    ttStatus = new TaskTrackerStatus(
      TstUtils.getNodeHost(1), TstUtils.getNodeHost(1),
      8080, Collections.singletonList(runningReduceStatus),  0, 0, 0);
    assertEquals(0, jt.job.failedMapTasks);
    jt.heartbeat(ttStatus, false, false, false, (short) 2);
    assertEquals(0, jt.job.failedMapTasks);
    jt.heartbeat(ttStatus, false, false, false, (short) 3);
    assertEquals(0, jt.job.failedMapTasks);
    jt.heartbeat(ttStatus, false, false, false, (short) 4);
    // After 3 notifications, attempt is failed.
    assertEquals(1, jt.job.failedMapTasks);
  }
}
