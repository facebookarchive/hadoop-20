package org.apache.hadoop.mapred;

import java.util.ArrayList;
import java.util.List;

import junit.framework.TestCase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.corona.ClusterManager;
import org.apache.hadoop.corona.ResourceGrant;
import org.apache.hadoop.corona.ResourceRequest;
import org.apache.hadoop.corona.TstUtils;
import org.junit.Test;

public class TestResourceTracker extends TestCase {
  JobConf conf;
  JobInProgressTraits job;
  
  @SuppressWarnings("deprecation")
  protected void setUp() {
    conf = new JobConf(new Configuration());
    job = new DummyJobInProgress();
  }
  
  static class DummyJobInProgress extends JobInProgressTraits {

    @Override
    public boolean inited() { return true;}

    @Override
    public int getNumRestarts() { return 0; }

    @Override
    public DataStatistics getRunningTaskStatistics(boolean isMap) { return null; }

    @Override
    DataStatistics getRunningTaskStatistics(TaskStatus.Phase phase) { return null; }

    @Override
    public float getSlowTaskThreshold() { return 0; }

    @Override
    public JobStatus getStatus() { return null; }

    @Override
    public float getStddevMeanRatioMax() { return 0; }

    @Override
    public String getUser() { return null; }

    @Override
    public boolean hasSpeculativeMaps() { return false; }

    @Override
    public boolean hasSpeculativeReduces() { return false; }

    @Override
    public boolean shouldLogCannotspeculativeMaps() { return false; }

    @Override
    public boolean shouldLogCannotspeculativeReduces() { return false; }
    
    @Override
    public boolean shouldSpeculateAllRemainingMaps() { return false; }

    @Override
    public boolean shouldSpeculateAllRemainingReduces() { return false; }
  }
  
  @SuppressWarnings("deprecation")
  TaskInProgress newMapTask(int idx) {
    JobClient.RawSplit split = new JobClient.RawSplit();
    split.setLocations(new String[0]);
    return new TaskInProgress(new JobID(), null, split,
        new JobConf(conf), job, idx, 1); // numSlotsPerMap = 1
  }
  
  @SuppressWarnings("deprecation")
  TaskInProgress newReduceTask(int idx, int numMapTasks) {
    return new TaskInProgress(new JobID(), null, numMapTasks, idx,
                                  conf, job, 1); // numSlotsPerReduce = 1
    
  }
  
  @Test
  public void testAddRelease() {
    ResourceTracker rt = new ResourceTracker(this);
    
    TaskInProgress map0 = newMapTask(0);
    TaskInProgress map1 = newMapTask(1);
    TaskInProgress reduce0 = newReduceTask(0, 2);
    TaskInProgress reduce1 = newReduceTask(1, 2);
    ResourceRequest req = rt.newMapRequest(map0.getSplitLocations());
    rt.recordRequest(req);
    req = rt.newMapRequest(map1.getSplitLocations());
    rt.recordRequest(req);
    req = rt.newReduceRequest();
    rt.recordRequest(req);
    req = rt.newReduceRequest();
    rt.recordRequest(req);
    
    List<ResourceRequest> wanted = rt.getWantedResources();
    assertEquals(4, wanted.size());
    assertEquals(0, rt.getWantedResources().size());
    
    List<ResourceGrant> grants = new ArrayList<ResourceGrant>();
    
    for (int i = 1; i <= 4; i++) {
      ResourceGrant grant =
        new ResourceGrant(i, TstUtils.getNodeHost(i-1),
            TstUtils.getNodeAddress(i-1), ClusterManager.clock.getTime(),
            wanted.get(i - 1).getType());
      grant.setAppInfo("192.168.0.1:1234"); // some random app info.
      grants.add(grant);
    }
    
    rt.addNewGrants(grants);
    
    assertEquals(0, rt.getResourcesToRelease().size());
    rt.releaseResource(1);
    rt.releaseResource(2);
    rt.releaseResource(3);
    assertEquals(3, rt.getResourcesToRelease().size());
    assertEquals(0, rt.getResourcesToRelease().size());
    rt.releaseResource(4);
    assertEquals(1, rt.getResourcesToRelease().size());
    assertEquals(0, rt.getResourcesToRelease().size());
  }

  public void testReuse() {
    ResourceTracker rt = new ResourceTracker(this);
    
    TaskInProgress map0 = newMapTask(0);
    TaskInProgress map1 = newMapTask(1);
    TaskInProgress reduce0 = newReduceTask(0, 2);
    TaskInProgress reduce1 = newReduceTask(1, 2);
    
    ResourceRequest req = rt.newMapRequest(map0.getSplitLocations());
    rt.recordRequest(req);
    req = rt.newMapRequest(map1.getSplitLocations());
    rt.recordRequest(req);
    req = rt.newReduceRequest();
    rt.recordRequest(req);
    req = rt.newReduceRequest();
    rt.recordRequest(req);
    
    assertEquals(4, rt.requestMap.size());
    List<ResourceRequest> wanted = rt.getWantedResources();
    assertEquals(4, wanted.size());
    assertEquals(0, rt.getWantedResources().size());
    
    List<ResourceGrant> grants = new ArrayList<ResourceGrant>();
    for (int i = 0; i < 4; i++) {
      ResourceGrant grant =
        new ResourceGrant(i+1, TstUtils.getNodeHost(i),
            TstUtils.getNodeAddress(i), ClusterManager.clock.getTime(),
            wanted.get(i).getType());
      grant.setAppInfo("192.168.0.1:1234"); // some random app info.
      grants.add(grant);
    }
    // Grant the initial need.
    rt.addNewGrants(grants);

    assertEquals(4, rt.requestedResources.size());
    assertEquals(4, rt.requestMap.size());
    rt.releaseAndRequestResource(grants.get(0).getId(), null);
    assertEquals(3, rt.grantedResources.size());
    assertEquals(4, rt.requestMap.size());
    assertEquals(4, rt.requestedResources.size());
    rt.getResourcesToRelease();
    assertEquals(3, rt.requestedResources.size());
    rt.getWantedResources();
    assertEquals(4, rt.requestedResources.size());
  }
}
