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
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.corona.ComputeSpecs;
import org.apache.hadoop.corona.InetAddress;
import org.apache.hadoop.corona.ResourceGrant;
import org.apache.hadoop.corona.ResourceRequest;
import org.apache.hadoop.corona.Utilities;


public class ResourceTracker {
  public static final Log LOG = LogFactory.getLog(ResourceTracker.class);

  public static final String RESOURCE_TYPE_MAP = new String ("M");
  public static final String RESOURCE_TYPE_REDUCE = new String ("R");
  public static final int MAX_REQUESTS_PER_TASK = 1 + TaskInProgress.MAX_TASK_EXECS;

  static ComputeSpecs stdMapSpec() {
    short numCpus = 1;
    ComputeSpecs spec = new ComputeSpecs(numCpus);
    // Create Compute Spec.
    spec.setNetworkMBps((short)10);
    spec.setMemoryMB(1024);
    spec.setDiskGB(10);
    return spec;
  }

  static ComputeSpecs stdReduceSpec() {
    short numCpus = 1;
    ComputeSpecs spec = new ComputeSpecs(numCpus);
    spec.setNetworkMBps((short)50);
    spec.setMemoryMB(1024);
    spec.setDiskGB(10);
    return spec;
  }

  static class TaskContext {
    List<ResourceRequest> resourceRequests;
    Set<String> excludedHosts;
    TaskContext(ResourceRequest req) {
      resourceRequests = new ArrayList<ResourceRequest>();
      resourceRequests.add(req);
      excludedHosts = new HashSet<String>();
    }
  }

  AtomicInteger resourceRequestId = new AtomicInteger();

  // This provides information about the resource needs of each task (TIP).
  HashMap<TaskInProgress, TaskContext> taskToContextMap =
    new HashMap<TaskInProgress, TaskContext>();
  // Maintains the inverse of taskToContextMap.
  HashMap<Integer, TaskInProgress> requestToTipMap =
    new HashMap<Integer, TaskInProgress>();
  // This tracks all resource requests sent to the Cluster Manager.
  // New requests are sent by computing
  //   (Requests in taskToContextMap) - (Requests in requestedResources)
  Map<Integer, ResourceRequest> requestedResources =
    new HashMap<Integer, ResourceRequest>();

  // Lookup table for all granted resources.
  HashMap<Integer, ResourceGrant> grantedResources =
    new HashMap<Integer, ResourceGrant>();
  // Granted resources that are not already in use.
  Set<Integer> availableResources = new HashSet<Integer>();
  // Keep track of maximum granted resources. This is used for speculation.
  int maxReduceGrants = 0;
  int maxMapGrants = 0;
  // Map for address information of a tracker.
  Map<String, org.apache.hadoop.corona.InetAddress> trackerAddress =
    new HashMap<String, org.apache.hadoop.corona.InetAddress>();

  private Object lockObject;

  public ResourceTracker(Object lockObject) {
    this.lockObject = lockObject;
  }

  /**
   * Find what new requests need to be sent by finding out resources needed
   * by tasks but not sent to Cluster Manager.
   * @return
   */
  public List<ResourceRequest> getWantedResources() {
    List<ResourceRequest> wanted = new ArrayList<ResourceRequest>();
    synchronized(lockObject) {
      for (Integer requestId:
        setDifference(requestToTipMap.keySet(), requestedResources.keySet())) {
        TaskInProgress task = requestToTipMap.get(requestId);
        TaskContext taskContext = taskToContextMap.get(task);
        for (ResourceRequest req: taskContext.resourceRequests) {
          if (req.getId() == requestId) {

            LOG.info("Filing request for tip: " + task.getTIPId() + " requestId: " + requestId);

            // If not, add it to the wanted list and updated the map.
            wanted.add(req);
            // We update requestedResources right away. This assumes that the
            // caller will be able to send the new requests, retrying if needed.
            requestedResources.put(req.getId(), req);
            break;
          }
        }
      }
    }
    return wanted;
  }

  /**
   * Go through all the requested resources and find what needs to be released.
   * @return
   */
  public List<ResourceRequest> getResourcesToRelease() {
    List<ResourceRequest> release = new ArrayList<ResourceRequest>();
    synchronized(lockObject) {
      for (Integer requestId:
        setDifference(requestedResources.keySet(), requestToTipMap.keySet())) {
        // We update the data structures right away. This assumes that the
        // caller will be able to release the resources.
        ResourceRequest req = requestedResources.remove(requestId);
        if (req != null) {
          release.add(req);
          LOG.info("Filing release for requestId: " + req.getId());
        }
      }
    }
    return release;
  }

  public void addNewMapTask(TaskInProgress mapTask) {
    synchronized(lockObject) {
      recordRequestUnprotected(mapTask, newMapRequest(mapTask));
    }
  }

  public void addNewReduceTask(TaskInProgress reduceTask) {
    synchronized(lockObject) {
      recordRequestUnprotected(reduceTask, newReduceRequest(reduceTask));
    }
  }

  /**
   * Task failed on a host. Get another resource to run the task.
   * @param grantIdToRelease
   */
  public void releaseAndRequestAnotherResource(Integer grantIdToRelease) {
    TaskID tipId;

    synchronized(lockObject) {
      TaskInProgress task = requestToTipMap.get(grantIdToRelease);
      if (task == null) {
        LOG.info ("releaseAndRequest for grant: " + grantIdToRelease + " has no matching task");
        return;
      }
      tipId = task.getTIPId();
      ResourceGrant grantToRelease = grantedResources.get(grantIdToRelease);
      String excluded = grantToRelease.getAddress().getHost();
      Set<String> excludedByTip = new HashSet<String>();
      if (excluded != null) {
        excludedByTip = addExcludedHost(task, excluded);
      }
      ResourceRequest requestToRelease = requestedResources.get(grantIdToRelease);
      if (requestToRelease != null) {
        boolean updateTaskContext = true;
        removeRequestUnprotected(requestToRelease, updateTaskContext);
        ResourceRequest request = copyRequest(requestToRelease, excludedByTip);
        recordRequestUnprotected(task, request);
      } else {
        LOG.info ("releaseAndRequest for grant: " + grantIdToRelease + " not found");
      }
    }
    LOG.info ("releaseAndRequest for grant: " + grantIdToRelease + " tip: " + tipId + " completed");
  }

  public int speculateTask(TaskInProgress task) {
    LOG.info("speculateTask for tip: " + task.getTIPId());
    synchronized(lockObject) {
      TaskContext taskContext = taskToContextMap.get(task);
      if (taskContext != null) {
        if (taskContext.resourceRequests.size() < MAX_REQUESTS_PER_TASK) {
          // If we can request more resources for this task, create a resource
          // request with the same specs as an existing request.
          ResourceRequest existing = taskContext.resourceRequests.get(0);
          recordRequestUnprotected(task,
              copyRequest(existing, getGrantedHostsUnprotected(taskContext)));
          return 1;
        }
      }
    }
    return 0;
  }

  private Set<String> getGrantedHostsUnprotected(TaskContext taskContext) {
    Set<String> result = new HashSet<String>();
    for (ResourceRequest req : taskContext.resourceRequests) {
      ResourceGrant grant = this.grantedResources.get(req.getId());
      if (grant != null) {
        result.add(grant.getAddress().getHost());
      }
    }
    return result;
  }

  /**
   * A task is done. Release the resources that we had requested for it.
   * @param task
   */
  public void taskDone(TaskInProgress task) {
    TaskContext taskContext = null;

    LOG.info("taskDone: " + task.getTIPId());

    synchronized(lockObject) {
      taskContext = taskToContextMap.get(task);
      for (ResourceRequest req: taskContext.resourceRequests) {
        boolean updateTaskContext = false; // We update task context ourselves.
        removeRequestUnprotected(req, updateTaskContext);
      }
      taskContext.resourceRequests.clear();
    }
  }

  /**
   * Release speculative requests. The speculative requests are those other than
   * the ones indicated as in use.
   */
  public void releaseSpeculativeRequests(
      TaskInProgress task, List<Integer> grantsInUse) {
    List<ResourceRequest> toRelease = new ArrayList<ResourceRequest>();
    synchronized(lockObject) {
      TaskContext taskContext = taskToContextMap.get(task);
      for (ResourceRequest req: taskContext.resourceRequests) {
        if (!grantsInUse.contains(req.getId())) {
          toRelease.add(req);
        }
      }
      for (ResourceRequest req: toRelease) {
        LOG.info("Released speculative resource with requestId: " + req.getId());
        boolean updateTaskContext = true;
        removeRequestUnprotected(req, updateTaskContext);
      }
    }
  }

  public int numSpeculativeRequests(String resourceType) {
    int numSpeculative = 0;
    synchronized(lockObject) {
      for (Map.Entry<TaskInProgress, TaskContext> entry: taskToContextMap.entrySet()) {
        List<ResourceRequest> requests = entry.getValue().resourceRequests;
        if (!requests.isEmpty() &&
          requests.get(0).getType().equals(resourceType)) {
          numSpeculative += (requests.size() - 1);
        }
      }
    }
    return numSpeculative;
  }

  public List<TaskInProgress> tasksBeingSpeculated() {
    List<TaskInProgress> result = new ArrayList<TaskInProgress>();
    synchronized(lockObject) {
      for (Map.Entry<TaskInProgress, TaskContext> entry: taskToContextMap.entrySet()) {
        TaskContext taskContext = entry.getValue();
        if (taskContext.resourceRequests.size() > 1) {
          result.add(entry.getKey());
        }
      }
    }
    return result;
  }

  public void reuseGrant(Integer grantIdToReuse) {
    synchronized(lockObject) {
      if (grantedResources.containsKey(grantIdToReuse)) {
        availableResources.add(grantIdToReuse);
        lockObject.notify();
      }
    }
    LOG.info ("reuseGrant for grant: " + grantIdToReuse);
  }

  /**
   * Obtained new grants from Cluster Manager.
   * @param grants
   */
  public void addNewGrants(List<ResourceGrant> grants) {
    int numGranted = 0;
    int numAvailable = 0;
    synchronized(lockObject) {
      for (ResourceGrant grant: grants) {
        Integer requestId = grant.getId();
        if (!requestedResources.containsKey(requestId) ||
            !requestToTipMap.containsKey(requestId)) {
          LOG.info("Request for grant " + grant.getId() + " no longer exists");
          continue;
        }
        updateTrackerAddressUnprotected(grant);

        addGrantedResourceUnprotected(grant);
      }
      updateGrantStatsUnprotected();
      numGranted = grantedResources.size();
      numAvailable = availableResources.size();
      lockObject.notify();
    }
    LOG.info("Number of available grants: " + numAvailable +
        " out of " + numGranted);
  }

  public interface ResourceProcessor {
    public boolean processAvailableResource(ResourceGrant resource);

    public boolean isBadResource(ResourceGrant grant, TaskInProgress tip);
  }

  public void processAvailableGrants(
        ResourceProcessor processor) throws InterruptedException {
    synchronized(lockObject) {
      while (availableResources.isEmpty()) {
        lockObject.wait();
      }
      List<Integer> resourcesConsumed = new ArrayList<Integer>();
      List<Integer> badResources = new ArrayList<Integer>();
      for (Integer grantId: availableResources) {
        Integer requestId = grantId;
        ResourceGrant grant = grantedResources.get(grantId);
        TaskInProgress tip = requestToTipMap.get(requestId);
        if (processor.isBadResource(grant, tip)) {
          LOG.info("processed bad resource with requestId: " + requestId);
          badResources.add(grantId);
        } else if (processor.processAvailableResource(grant)) {
          LOG.info("processed available resource with requestId: " + requestId);
          resourcesConsumed.add(grantId);
        }
      }
      for (Integer badResource: badResources) {
        // This will modify availableResources.
        releaseAndRequestAnotherResource(badResource);
      }
      // Remove consumed resources from the available set.
      availableResources.removeAll(resourcesConsumed);
      lockObject.wait(500); // Wait for some time before next iteration.
    }
  }

  public Set<String> addExcludedHost(TaskInProgress tip, String hostName) {
    LOG.info("Excluding " + hostName + " for tip:" + tip.getTIPId());
    synchronized(lockObject) {
      TaskContext taskContext = taskToContextMap.get(tip);
      taskContext.excludedHosts.add(hostName);
      for (ResourceRequest request : taskContext.resourceRequests) {
        //prefered hosts will not contain any exclude hosts
        if (request.getHosts() != null) {
          request.getHosts().remove(hostName);
        }
      }
      return Collections.unmodifiableSet(taskContext.excludedHosts);
    }
  }

  public TaskInProgress findTipForGrant(ResourceGrant grant) {
    Integer requestId = grant.getId();
    synchronized(lockObject) {
      // This is modified only under when taskToContextMap is also modified.
      return requestToTipMap.get(requestId);
    }
  }

  public ResourceGrant getGrant(Integer grantId) {
    synchronized(lockObject) {
      return grantedResources.get(grantId);
    }
  }

  public int maxGrantedResources(boolean map) {
    synchronized(lockObject) {
      if (map) {
        return maxMapGrants;
      } else {
        return maxReduceGrants;
      }
    }
  }

  public List<org.apache.hadoop.corona.InetAddress> allTrackers() {
    List<org.apache.hadoop.corona.InetAddress> result =
      new ArrayList<org.apache.hadoop.corona.InetAddress>();
    synchronized(lockObject) {
      result.addAll(trackerAddress.values());
    }
    return result;
  }

  public InetAddress getTrackerAddr(String trackerName) {
    synchronized(lockObject) {
      return trackerAddress.get(trackerName);
    }
  }

  public int getTrackerPort(String trackerName) {
    synchronized(lockObject) {
      return trackerAddress.get(trackerName).getPort();
    }
  }

  private ResourceRequest copyRequest(ResourceRequest requestToCopy) {
    return copyRequest(requestToCopy, null);
  }

  private ResourceRequest copyRequest(ResourceRequest requestToCopy,
      Set<String> excludedByTip) {
    int requestId = resourceRequestId.incrementAndGet();
    ResourceRequest req = new ResourceRequest(requestId, requestToCopy.getType());
    req.setSpecs(requestToCopy.getSpecs());

    Set<String> excluded = new HashSet<String>();
    if (excludedByTip != null || requestToCopy.getExcludeHosts() != null) {
      if (requestToCopy.getExcludeHosts() != null) {
        excluded.addAll(requestToCopy.getExcludeHosts());
      }
      if (excludedByTip != null) {
        excluded.addAll(excludedByTip);
      }
    }
    req.setExcludeHosts(new ArrayList<String>(excluded));
    if (requestToCopy.getHosts() != null) {
      List<String> hosts = new ArrayList<String>();
      for (String host : requestToCopy.getHosts()) {
        if (excluded == null || excluded.contains(host)) {
          hosts.add(host);
        }
      }
	    req.setHosts(hosts);
    }
    return req;
  }

  private ResourceRequest newMapRequest(TaskInProgress mapTask) {
    int requestId = resourceRequestId.incrementAndGet();
    ResourceRequest req = new ResourceRequest(requestId, RESOURCE_TYPE_MAP);
    req.setSpecs(stdMapSpec());

    String[] splitLocations = mapTask.getSplitLocations();
    List<String> hosts = new ArrayList<String>();
    for (int j = 0; j < splitLocations.length; j++) {
      hosts.add(splitLocations[j]);
    }
    if (!hosts.isEmpty()) {
      req.setHosts(hosts);
    }
    return req;
  }

  private ResourceRequest newReduceRequest(TaskInProgress reduceTask) {
    int requestId = resourceRequestId.incrementAndGet();
    ResourceRequest req = new ResourceRequest(requestId, RESOURCE_TYPE_REDUCE);
    req.setSpecs(stdReduceSpec());
    return req;
  }

  private void recordRequestUnprotected(
      TaskInProgress task, ResourceRequest req) {
    TaskContext taskContext = taskToContextMap.get(task);
    if (taskContext == null) {
      taskContext = new TaskContext(req);
    } else {
      taskContext.resourceRequests.add(req);
    }
    taskToContextMap.put(task, taskContext);
    requestToTipMap.put(req.getId(), task);
  }

  private void removeGrantedResourceUnprotected(Integer id) {
    boolean wasResourceAvailable = availableResources.remove(id);
    Object granted = grantedResources.remove(id);
    if (wasResourceAvailable && granted == null) {
      throw new RuntimeException(
          "Resource " + id + " was available but not granted");
    }

    if (granted != null && !requestToTipMap.containsKey(id)) {
      throw new RuntimeException(
          "Resource " + id + " was granted but not requested");
    }
  }

  private void addGrantedResourceUnprotected(ResourceGrant grant) {
    Integer id = grant.getId();
    grantedResources.put(id, grant);
    availableResources.add(id);
  }

  private void updateGrantStatsUnprotected() {
    int numMapGrants = 0;
    int numReduceGrants = 0;
    for (ResourceGrant grant: grantedResources.values()) {
      Integer id = grant.getId();
      TaskInProgress tip = requestToTipMap.get(id);
      TaskContext taskContext = taskToContextMap.get(tip);
      String resourceType = resourceType(taskContext, id);
      if (resourceType.equals(RESOURCE_TYPE_MAP)) {
        numMapGrants++;
      } else {
        numReduceGrants++;
      }
    }
    maxMapGrants = Math.max(maxMapGrants, numMapGrants);
    maxReduceGrants = Math.max(maxReduceGrants, numReduceGrants);
  }

  private void updateTrackerAddressUnprotected(ResourceGrant grant) {
    String trackerName = grant.getNodeName();
    // Update address information for trackers.
    org.apache.hadoop.corona.InetAddress addr =
      Utilities.appInfoToAddress(grant.appInfo);
    trackerAddress.put(trackerName, addr);
  }

  /**
   * Removes the request from requestToTipMap, and if updateTaskContext is true,
   * also from taskContext map. It *does not* remove from requestedResources.
   * This lets getResourcesToRelease() figure out what needs to be released.
   */
  private void removeRequestUnprotected(
       ResourceRequest req, boolean updateTaskContext) {
    Integer requestId = req.getId();
    removeGrantedResourceUnprotected(requestId);
    TaskInProgress task = requestToTipMap.remove(requestId);
    if (updateTaskContext) {
      TaskContext taskContext = taskToContextMap.get(task);
      for (Iterator<ResourceRequest> reqIt = taskContext.resourceRequests.iterator();
      reqIt.hasNext(); ) {
        ResourceRequest one = reqIt.next();
        if (one.getId() == requestId) {
          reqIt.remove();
          break;
        }
      }
    }
  }

  private static <T> List<T> setDifference(Set<T> s1, Set<T> s2) {
    List<T> diff = new ArrayList<T>();
    for (T one: s1) {
      if (!s2.contains(one)) {
        diff.add(one);
      }
    }
    return diff;
  }

  private static String resourceType(TaskContext taskContext, Integer id) {
    for (ResourceRequest request: taskContext.resourceRequests) {
      if (request.getId() == id) {
        return request.getType();
      }
    }
    return null;
  }

}
