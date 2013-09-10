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

import java.util.ArrayList;
import java.util.Arrays;
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
import org.apache.hadoop.corona.ResourceType;
import org.apache.hadoop.corona.Utilities;


public class ResourceTracker {
  public static final Log LOG = LogFactory.getLog(ResourceTracker.class);

  /** Highest integer that won't be used for request/grant ids */
  public static final int START_REQUEST_ID = 0;
  /** Null grant id is an id of non-existing grant, for restoring JT state */
  public static Integer NONE_GRANT_ID = new Integer(START_REQUEST_ID - 1);

  /**
   * Determines whether given id represents non-existing resource grant.
   * @param id an id to check
   * @return true if resource with given id can't exist
   */
  public static boolean isNoneGrantId(Integer id) {
    return NONE_GRANT_ID.equals(id);
  }

  /**
   * Returns id representing non-existing resource grant.
   * @return id id of resource grant
   */
  public static Integer getNoneGrantId() {
    return NONE_GRANT_ID;
  }

  /** Keeps last (possibly) assigned resource id */
  AtomicInteger resourceRequestId = new AtomicInteger(START_REQUEST_ID);

  static ComputeSpecs stdMapSpec() {
    short numCpus = 1;
    ComputeSpecs spec = new ComputeSpecs(numCpus);
    // Create Compute Spec.
    spec.setNetworkMBps((short) 10);
    spec.setMemoryMB(1024);
    spec.setDiskGB(10);
    return spec;
  }

  static ComputeSpecs stdReduceSpec() {
    short numCpus = 1;
    ComputeSpecs spec = new ComputeSpecs(numCpus);
    spec.setNetworkMBps((short) 50);
    spec.setMemoryMB(1024);
    spec.setDiskGB(10);
    return spec;
  }

  //This tracks all resources registered and not released in resource tracker,
  // also those not sent to CM
  HashMap<Integer, ResourceRequest> requestMap =
    new HashMap<Integer, ResourceRequest>();
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

  int maxReduceGrants = 0;
  int maxMapGrants = 0;
  // Map for address information of a tracker.
  Map<String, InetAddress> trackerAddress = new HashMap<String, InetAddress>();

  private final Object lockObject;

  public ResourceTracker(Object lockObject) {
    this.lockObject = lockObject;
  }

  /**
   * Get a snapshot of the resource usage.
   *
   * @return Snapshot of resource usage
   */
  public ResourceUsage getResourceUsage() {
    int totalMapperGrants = 0;
    int totalReducerGrants = 0;
    synchronized (lockObject) {
      for (Map.Entry<Integer, ResourceGrant> entry :
          grantedResources.entrySet()) {
        switch(entry.getValue().getType()) {
        case MAP:
          ++totalMapperGrants;
          break;
        case REDUCE:
          ++totalReducerGrants;
          break;
        case JOBTRACKER:
          // Ignore for now
          break;
        default:
          throw new RuntimeException("Illegal type " +
                                     entry.getValue().getType());
        }
      }
    }
    return new ResourceUsage(totalMapperGrants, totalReducerGrants);
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
        setDifference(requestMap.keySet(), requestedResources.keySet())) {
        ResourceRequest req = requestMap.get(requestId);
        LOG.info("Filing request for resource " + requestId);
        requestedResources.put(requestId, req);
        wanted.add(req);
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
        setDifference(requestedResources.keySet(), requestMap.keySet())) {
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

  public ResourceRequest releaseAndRequestResource(Integer grantIdToRelease, Set<String> excludedHosts) {
    synchronized(lockObject) {
      ResourceRequest requestToRelease = requestedResources.get(grantIdToRelease);
      if (requestToRelease != null) {
        removeRequestUnprotected(requestToRelease);
        ResourceRequest request = copyRequest(requestToRelease, excludedHosts);
        recordRequestUnprotected(request);
        LOG.info ("releaseAndRequest for grant: " + grantIdToRelease + " completed " +
            (excludedHosts != null ? "excluding resource" : "") +
            ". Generated new request #" + request.getId());
        return request;
      } else {
        LOG.info ("releaseAndRequest for grant: " + grantIdToRelease + " not found");
        return null;
      }
    }
  }

  /**
   * Release the resource that was requested
   */
  public void releaseResource(int resourceId) {
    synchronized (lockObject) {
      ResourceRequest req = requestedResources.get(resourceId);
      removeRequestUnprotected(req);
    }
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
            !requestMap.containsKey(requestId)) {
          LOG.info("Request for grant " + grant.getId() + " no longer exists");
          continue;
        }
        assert !grantedResources.containsKey(grant.getId()) :
            "Grant " + grant.getId() + " has already been processed.";
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
  }

  public void processAvailableGrants(
        ResourceProcessor processor, int maxBatchSize) throws InterruptedException {
    processAvailableGrants(processor, maxBatchSize, Long.MAX_VALUE);
  }

  public void processAvailableGrants(
        ResourceProcessor processor,
        int maxBatchSize,
        long timeout) throws InterruptedException {
    synchronized(lockObject) {
      while (availableResources.isEmpty()) {
        lockObject.wait(timeout);
        if (availableResources.isEmpty()) {
          LOG.warn("No available resources after timeout of " + timeout);
          return;
        }
      }
      List<Integer> resourcesConsumed = new ArrayList<Integer>();
      List<Integer> stillAvailable = new ArrayList<Integer>();
      Iterator<Integer> grantIter = availableResources.iterator();
      int processed = 0;
      while (grantIter.hasNext() && processed < maxBatchSize) {
        processed++;
        Integer grantId = grantIter.next();
        grantIter.remove();
        Integer requestId = grantId;
        ResourceGrant grant = grantedResources.get(grantId);
        if (processor.processAvailableResource(grant)) {
          if (LOG.isDebugEnabled()) {
            LOG.info("processed available resource with requestId: " +
              requestId);
          }
          resourcesConsumed.add(grantId);
        } else {
          if (LOG.isDebugEnabled()) {
            LOG.debug("available resource with requestId: " + requestId +
              " is not processed and is still available");
          }
          stillAvailable.add(grantId);
        }
      }

      // Remove consumed resources from the available set.
      availableResources.addAll(stillAvailable);
      if (processed < maxBatchSize) {
        // We did not have enough to process, wait for some time before
        // next iteration. If more resources become available, the object
        // will be notified.
        lockObject.wait(500);
      } else {
        // We processed a batch of data, wait for a short time to yield
        // the lock.
        lockObject.wait(1);
      }
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

  public Map<String, InetAddress> allTrackers() {
    synchronized(lockObject) {
      return new HashMap<String, InetAddress>(trackerAddress);
    }
  }

  public InetAddress getTrackerAddr(String trackerName) {
    synchronized(lockObject) {
      return trackerAddress.get(trackerName);
    }
  }
  
  /**
   * Updates mapping between tracker names and adresses
   * @param trackerName name of tracker
   * @param addr address of the tracker
   */
  public void updateTrackerAddr(String trackerName, InetAddress addr) {
    synchronized (lockObject) {
      trackerAddress.put(trackerName, addr);
    }
  }

  public int getTrackerPort(String trackerName) {
    synchronized(lockObject) {
      return trackerAddress.get(trackerName).getPort();
    }
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
        if (excluded == null || !excluded.contains(host)) {
          hosts.add(host);
        }
      }
      if (!hosts.isEmpty()) {
	      req.setHosts(hosts);
      }
    }
    return req;
  }

  public int nextRequestId() {
    return resourceRequestId.incrementAndGet();
  }

  public ResourceRequest newMapRequest(String[] splitLocations) {
    int requestId = nextRequestId();
    ResourceRequest req = new ResourceRequest(requestId,
        ResourceType.MAP);
    req.setSpecs(stdMapSpec());

    List<String> hosts = new ArrayList<String>();
    for (int j = 0; j < splitLocations.length; j++) {
      hosts.add(splitLocations[j]);
    }
    if (!hosts.isEmpty()) {
      req.setHosts(hosts);
    }
    return req;
  }

  public ResourceRequest newReduceRequest() {
    int requestId = nextRequestId();
    ResourceRequest req = new ResourceRequest(requestId,
        ResourceType.REDUCE);
    req.setSpecs(stdReduceSpec());
    return req;
  }

  public ResourceRequest newJobTrackerRequest() {
    int requestId = nextRequestId();
    ResourceRequest req = new ResourceRequest(requestId,
        ResourceType.JOBTRACKER);
    req.setSpecs(stdReduceSpec());
    return req;
  }

  public void recordRequest(ResourceRequest req) {
    synchronized (lockObject) {
      recordRequestUnprotected(req);
    }
  }

  private void recordRequestUnprotected(ResourceRequest req) {
    requestMap.put(req.getId(), req);
  }

  private void removeGrantedResourceUnprotected(Integer id) {
    boolean wasResourceAvailable = false;
    if (availableResources.contains(id)) {
      LOG.info("Removing " + id + " from available " + availableResources);
      wasResourceAvailable = availableResources.remove(id);
    }
    Object granted = grantedResources.remove(id);
    if (wasResourceAvailable && granted == null) {
      throw new RuntimeException(
          "Resource " + id + " was available but not granted");
    }

    if (granted != null && !requestMap.containsKey(id)) {
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
      switch (grant.getType()) {
      case MAP:
        ++numMapGrants;
        break;
      case REDUCE:
        ++numReduceGrants;
        break;
      case JOBTRACKER:
        // Not tracked for now
        break;
      default:
        throw new RuntimeException("Unknown resource type " + grant.getType());
      }
    }
    maxMapGrants = Math.max(maxMapGrants, numMapGrants);
    maxReduceGrants = Math.max(maxReduceGrants, numReduceGrants);
  }

  private void updateTrackerAddressUnprotected(ResourceGrant grant) {
    String trackerName = grant.getNodeName();
    // Update address information for trackers.
    InetAddress addr = Utilities.appInfoToAddress(grant.appInfo);
    trackerAddress.put(trackerName, addr);
  }

  /**
   * Removes the request from requestToTipMap, and if updateTaskContext is true,
   * also from taskContext map. It *does not* remove from requestedResources.
   * This lets getResourcesToRelease() figure out what needs to be released.
   */
  private void removeRequestUnprotected(ResourceRequest req) {
    Integer requestId = req.getId();
    removeGrantedResourceUnprotected(requestId);
    requestMap.remove(requestId);
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

  public boolean hasAvailableResources() {
    synchronized (lockObject) {
      return !availableResources.isEmpty();
    }
  }

  public static List<ResourceType> resourceTypes() {
    return Arrays.asList(ResourceType.MAP, ResourceType.REDUCE);
  }
}
