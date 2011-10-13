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
    
    rt.addNewMapTask(map0);
    rt.addNewMapTask(map1);
    rt.addNewReduceTask(reduce0);
    rt.addNewReduceTask(reduce1);
    
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
    rt.taskDone(map0);
    rt.taskDone(map1);
    rt.taskDone(reduce0);
    assertEquals(3, rt.getResourcesToRelease().size());
    assertEquals(0, rt.getResourcesToRelease().size());
    rt.taskDone(reduce1);
    assertEquals(1, rt.getResourcesToRelease().size());
    assertEquals(0, rt.getResourcesToRelease().size());
  }
  
  public void testSpeculation() {
    ResourceTracker rt = new ResourceTracker(this);
    
    TaskInProgress map0 = newMapTask(0);
    TaskInProgress map1 = newMapTask(1);
    TaskInProgress reduce0 = newReduceTask(0, 2);
    TaskInProgress reduce1 = newReduceTask(1, 2);
    
    rt.addNewMapTask(map0);
    rt.addNewMapTask(map1);
    rt.addNewReduceTask(reduce0);
    rt.addNewReduceTask(reduce1);
    
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
    assertEquals(0, rt.maxGrantedResources(true));
    assertEquals(0, rt.maxGrantedResources(false));
    rt.addNewGrants(grants);
    assertEquals(2, rt.maxGrantedResources(true));
    assertEquals(2, rt.maxGrantedResources(false));
    
    // Speculate a map.
    rt.speculateTask(map1);
    // Wanted resources should go up.
    wanted = rt.getWantedResources();
    assertEquals(1, wanted.size());
    assertEquals(5, wanted.get(0).getId());
    assertEquals(1, rt.numSpeculativeRequests(ResourceTracker.RESOURCE_TYPE_MAP));
    assertEquals(0, rt.numSpeculativeRequests(ResourceTracker.RESOURCE_TYPE_REDUCE));
    List<TaskInProgress> spec = rt.tasksBeingSpeculated();
    assertEquals(1, spec.size());
    assertEquals(map1, spec.get(0));

    // Grant the speculative map request.
    grants.clear();
    grants.add(
      new ResourceGrant(5, TstUtils.getNodeHost(3), TstUtils.getNodeAddress(3),
          ClusterManager.clock.getTime(), wanted.get(0).getType()));
    for (ResourceGrant grant: grants) {
      grant.setAppInfo("192.168.0.1:1234"); // some random app info.
    }
    rt.addNewGrants(grants);
    assertEquals(3, rt.maxGrantedResources(true));
    assertEquals(2, rt.maxGrantedResources(false));
    
    // Speculate a task but release the request before granting.
    List<Integer> grantsInUse = new ArrayList<Integer>();
    List<ResourceRequest> map0Requests = rt.taskToContextMap.get(map0).resourceRequests;
    grantsInUse.add(map0Requests.get(0).getId());
    rt.speculateTask(map0);
    assertTrue(rt.requestToTipMap.containsKey(6));
    rt.releaseSpeculativeRequests(map0, grantsInUse);
    assertFalse(rt.requestToTipMap.containsKey(6));

    rt.taskDone(map0);
    rt.taskDone(map1);
    rt.taskDone(reduce0);
    rt.taskDone(reduce1);
    assertEquals(5, rt.getResourcesToRelease().size());
    assertEquals(0, rt.grantedResources.size());
    assertEquals(3, rt.maxGrantedResources(true));
    assertEquals(2, rt.maxGrantedResources(false));
  }

  public void testReuse() {
    ResourceTracker rt = new ResourceTracker(this);
    
    TaskInProgress map0 = newMapTask(0);
    TaskInProgress map1 = newMapTask(1);
    TaskInProgress reduce0 = newReduceTask(0, 2);
    TaskInProgress reduce1 = newReduceTask(1, 2);
    
    rt.addNewMapTask(map0);
    rt.addNewMapTask(map1);
    rt.addNewReduceTask(reduce0);
    rt.addNewReduceTask(reduce1);
    
    assertEquals(4, rt.requestToTipMap.size());
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
    assertEquals(4, rt.requestToTipMap.size());
    assertEquals(0, rt.taskToContextMap.get(map0).excludedHosts.size());
    rt.releaseAndRequestAnotherResource(grants.get(0).getId());
    assertEquals(1, rt.taskToContextMap.get(map0).excludedHosts.size());
    assertEquals(3, rt.grantedResources.size());
    assertEquals(4, rt.requestToTipMap.size());
    assertEquals(4, rt.requestedResources.size());
    rt.getResourcesToRelease();
    assertEquals(3, rt.requestedResources.size());
    rt.getWantedResources();
    assertEquals(4, rt.requestedResources.size());
  }
}
