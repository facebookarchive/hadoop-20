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

package org.apache.hadoop.corona;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Manages the communication between a client and the {@link ClusterManager}
 * and the resources given to it from the {@link ClusterManager}.
 */
public class Session {
  /** Class logger */
  private static final Log LOG = LogFactory.getLog(Session.class);
  /** The request vector received from the SessionDriver */
  protected HashMap<Integer, ResourceRequest> idToRequest
    = new HashMap<Integer, ResourceRequest>();
  /** The request grants that have been made for this session */
  protected HashMap<Integer, ResourceGrant> idToGrant
    = new HashMap<Integer, ResourceGrant>();
  /** Derived list of pending requests */
  protected HashMap<Integer, ResourceRequest> idToPendingRequests
    = new HashMap<Integer, ResourceRequest>();
  /** For pending requests, indexes to enable efficient matching */
  protected HashMap<String, List<ResourceRequest>> hostToPendingRequests
    = new HashMap<String, List<ResourceRequest>>();
  /** The list of requests that don't have specific hosts requested */
  protected List<ResourceRequest> anyHostRequests
    = new ArrayList<ResourceRequest>();
  /** Overall status of this session */
  private SessionStatus status = SessionStatus.RUNNING;
  /** Unique identifier for this session */
  private final String sessionId;
  /** {@link SessionManager} can delete this session. */
  private boolean deleted = false;
  /** Information about the session from the creator */
  private final SessionInfo info;
  /** Time that this session was started (in millis) */
  private final long startTime;
  /** Last time a heartbeat occurred */
  private long lastHeartbeatTime;
  /**
   * Map of resource types to the resource context.
   */
  private Map<ResourceType, Context> typeToContext =
      new EnumMap<ResourceType, Context>(ResourceType.class);

  /**
   * Constructor.
   *
   * @param id Should be a unique id
   * @param info Information about the session
   */
  public Session(String id, SessionInfo info) {
    this.sessionId = id;
    this.info = info;
    this.startTime = ClusterManager.clock.getTime();
    this.lastHeartbeatTime = startTime;
  }

  public SessionStatus getStatus() {
    return status;
  }

  public void setStatus(SessionStatus status) {
    this.status = status;
  }

  public boolean isDeleted() {
    return deleted;
  }

  public void setDeleted(boolean deleted) {
    this.deleted = deleted;
  }

  public String getSessionId() {
    return sessionId;
  }

  public String getHandle() {
    return sessionId;
  }

  public SessionInfo getInfo() {
    return info;
  }

  public long getLastHeartbeatTime() {
    return lastHeartbeatTime;
  }

  public InetAddress getAddress() {
    return info.address;
  }

  public String getName() {
    return info.name;
  }

  public String getUserId() {
    return info.userId;
  }

  public String getPoolId() {
    return info.poolId;
  }

  public int getPriority() {
    return info.priority.getValue();
  }

  public long getDeadline() {
    return info.deadline;
  }

  public boolean isNoPreempt() {
    return info.noPreempt;
  }

  public String getUrl() {
    return info.url;
  }

  /**
   * Only update the session info url and name.
   *
   * @param url New session info url
   * @param name New session name
   */
  public void updateInfoUrlAndName(String url, String name) {
    this.info.url = url;
    this.info.name = name;
  }

  /**
   * Keeps track of the resource requests (pending, granted, fulfilled,
   * revoked, etc.) for a session and a particular resource type.
   */
  public static class Context {
    /** Requests that are waiting to be fulfilled */
    private List<ResourceRequest> pendingRequests =
        new ArrayList<ResourceRequest>();
    /** Granted requests */
    private List<ResourceRequest> grantedRequests =
        new ArrayList<ResourceRequest>();

    /** Number of requests at any point in time == pending + granted */
    private int requestCount = 0;
    /**
     * The maximum number of concurrent requests across the entire session
     * lifetime
     */
    private int maxConcurrentRequestCount = 0;
    /**
     * The total number of resources granted and released (and not revoked)
     * this gives us a sense for the total resource utilization by session
     */
    private int fulfilledRequestCount = 0;
    /** How many times did we cancel a grant */
    private int revokedRequestCount = 0;

    /** A private Constructor */
    private Context() {
    }
  }

  /**
   * Get the context for a resource type.  Creates the context
   * on demand if it doesn't exist.
   *
   * @param type Type of resource
   * @return Appropriate resource context
   */
  protected Context getContext(ResourceType type) {
    Context c = typeToContext.get(type);
    if (c == null) {
      c = new Context();
      typeToContext.put(type, c);
    }
    return c;
  }

  /**
   * Increase a request count for a resource for this session.  It cannot
   * exceed the max concurrent request count.
   *
   * @param type Resource type
   * @param delta incremental request count
   */
  protected void incrementRequestCount(ResourceType type, int delta) {
    Context c = getContext(type);
    int newRequestCount = c.requestCount + delta;
    c.requestCount = newRequestCount;
    if (newRequestCount > c.maxConcurrentRequestCount) {
      c.maxConcurrentRequestCount = newRequestCount;
    }
  }

  /**
   * Add a pending request for a resource type.
   *
   * @param req Resource request to add
   */
  protected void addPendingRequestForType(ResourceRequest req) {
    getContext(req.type).pendingRequests.add(req);
  }

  /**
   * Remove a pending resource request for a resource type
   *
   * @param req Resource request to remove
   */
  protected void removePendingRequestForType(ResourceRequest req) {
    Utilities.removeReference(getContext(req.type).pendingRequests,
                              req);
  }

  /**
   * Add a granted request for a resource type.
   *
   * @param req Resource request to add
   */
  protected void addGrantedRequest(ResourceRequest req) {
    Context c = getContext(req.type);
    c.grantedRequests.add(req);

    // assume a granted resource is going to be fully used
    c.fulfilledRequestCount++;
  }

  /**
   * Remove a granted request for a resource type.
   *
   * @param req Resource request to remove
   * @param isRevoked Was this request revoked from the cluster manager?
   */
  protected void removeGrantedRequest(ResourceRequest req, boolean isRevoked) {
    Context c = getContext(req.type);
    Utilities.removeReference(c.grantedRequests, req);

    // if revoked - we didn't fulfill this request
    if (isRevoked) {
      c.fulfilledRequestCount--;
      c.revokedRequestCount++;
    }
  }

  /**
   * Get the granted resources for a resource type
   *
   * @param type Type of resource
   * @return List of granted resource requests
   */
  public List<ResourceRequest> getGrantedRequestForType(ResourceType type) {
    return getContext(type).grantedRequests;
  }

  /**
   * Get the pending resources for a resource type
   *
   * @param type Type of resource
   * @return List of pending resource requests
   */
  public List<ResourceRequest> getPendingRequestForType(ResourceType type) {
    return getContext(type).pendingRequests;
  }

  /**
   * Get the request count for a resource type
   *
   * @param type Type of resource
   * @return Request count
   */
  public int getRequestCountForType(ResourceType type) {
    return getContext(type).requestCount;
  }

  /**
   * Get the granted request count for a resource type
   *
   * @param type Type of resource
   * @return Granted request count
   */
  public int getGrantCountForType(ResourceType type) {
    return getGrantedRequestForType(type).size();
  }

  /**
   * Get the maximum concurrent request count for a resource type
   *
   * @param type Type of resource
   * @return Maximum concurrent request count
   */
  public int getMaxConcurrentRequestCountForType(ResourceType type) {
    return getContext(type).maxConcurrentRequestCount;
  }

  /**
   * Get the fulfilled request count for a resource type
   *
   * @param type Type of resource
   * @return Fulfilled request count
   */
  public int getFulfilledRequestCountForType(ResourceType type) {
    return getContext(type).fulfilledRequestCount;
  }

  /**
   * Get the revoked request count for a resource type
   *
   * @param type Type of resource
   * @return Revoked request count
   */
  public int getRevokedRequestCountForType(ResourceType type) {
    return getContext(type).revokedRequestCount;
  }

  public Set<ResourceType> getTypes() {
    return typeToContext.keySet();
  }

  /**
   * We expose the mutable map only once the session has been marked deleted and
   * all data structures are effectively immutable
   *
   * @return Collection of resource grants made for this session
   */
  public Collection<ResourceGrant> getGrants() {
    return idToGrant.values();
  }

  /**
   * Request a list of resources
   *
   * @param requestList List of resource requests
   */
  public void requestResource(List<ResourceRequest> requestList) {
    if (deleted) {
      throw new RuntimeException("Session: " + sessionId +
          " has been deleted");
    }

    for (ResourceRequest req : requestList) {
      boolean newRequest = idToRequest.put(req.id, req) == null;
      if (!newRequest) {
        LOG.warn("Duplicate request from Session: " + sessionId + "" +
            " request: " + req.id);
        continue;
      }

      incrementRequestCount(req.type, 1);
      addPendingRequest(req);
    }
  }

  /**
   * Add a request to the list of pending
   * @param req the request to add
   */
  private void addPendingRequest(ResourceRequest req) {
    idToPendingRequests.put(req.id, req);
    if (req.hosts != null) {
      for (String host : req.hosts) {
        List<ResourceRequest> hostReqs = hostToPendingRequests.get(host);
        if (hostReqs == null) {
          hostReqs = new ArrayList<ResourceRequest>();
          hostToPendingRequests.put(host, hostReqs);
        }
        hostReqs.add(req);
      }
    } else {
      anyHostRequests.add(req);
    }

    addPendingRequestForType(req);
  }

  /**
   * Removes the request from the list of pending
   * @param req the request to remove
   */
  private void removePendingRequest(ResourceRequest req) {
    idToPendingRequests.remove(req.id);
    if (req.hosts != null) {
      for (String host : req.hosts) {
        List<ResourceRequest> hostReqs = hostToPendingRequests.get(host);

        if (hostReqs != null) {
          Utilities.removeReference(hostReqs, req);
          if (hostReqs.isEmpty()) {
            hostToPendingRequests.remove(host);
          }
        } else {
          // TODO: this is a bug - the index is not consistent
          LOG.warn("There is a problem with the hostsToPendingRequests " +
                "and the list of hosts in requests. " + host +
                " is present in the list of hosts for " + req +
                " doesn't have any pending requests in the " +
                "hostsToPendingRequests");
        }
      }
    } else {
      Utilities.removeReference(anyHostRequests, req);
    }

    removePendingRequestForType(req);
  }


  /**
   * Release the resources that are no longer needed from the session
   * @param idList the list of resources to release
   * @return the list of resources that have been released
   */
  public List<ResourceGrant> releaseResource(List<Integer> idList) {
    if (deleted) {
      throw new RuntimeException("Session: " +
          sessionId + " has been deleted");
    }

    List<ResourceGrant> canceledGrants = new ArrayList<ResourceGrant>();
    for (Integer id : idList) {
      ResourceRequest req = idToRequest.get(id);

      if (req != null) {
        idToRequest.remove(id);
        ResourceGrant grant = idToGrant.remove(id);
        if (grant != null) {
          // we have previously granted this resource, return to caller
          canceledGrants.add(grant);
          removeGrantedRequest(req, false);
        } else {
          removePendingRequest(req);
        }

        incrementRequestCount(req.type, -1);
      }
    }
    return canceledGrants;
  }

  /**
   * Grant a resource to a session to satisfy a request
   * @param req the request being satisfied
   * @param grant the grant satisfying the request
   */
  public void grantResource(ResourceRequest req, ResourceGrant grant) {
    if (deleted) {
      throw new RuntimeException("Session: " +
          sessionId + " has been deleted");
    }

    removePendingRequest(req);
    idToGrant.put(req.id, grant);
    addGrantedRequest(req);
  }

  /**
   * Revoke a list of resources from a session
   * @param idList the list of resources to revoke
   * @return the list of resource grants that have been revoked
   */
  public List<ResourceGrant> revokeResource(List<Integer> idList) {
    if (deleted) {
      throw new RuntimeException("Session: " +
          sessionId + " has been deleted");
    }

    List<ResourceGrant> canceledGrants = new ArrayList<ResourceGrant>();
    for (Integer id : idList) {
      ResourceRequest req = idToRequest.get(id);
      ResourceGrant grant = idToGrant.remove(id);
      if (grant != null) {

        if (req == null) {
          throw new RuntimeException("Session: " + sessionId +
              ", requestId: " + id + " grant exists but request doesn't");
        }

        removeGrantedRequest(req, true);

        // we have previously granted this resource, return to caller
        canceledGrants.add(grant);
      }
    }
    return canceledGrants;
  }

  /**
   * Record a successfull heartbeat
   */
  public void heartbeat() {
    lastHeartbeatTime = ClusterManager.clock.getTime();
  }

  public int getPendingRequestCount() {
    return idToPendingRequests.size();
  }

  public Collection<ResourceRequest> getPendingRequests() {
    return new ArrayList<ResourceRequest>(idToPendingRequests.values());
  }

  public Collection<ResourceGrant> getGrantedRequests() {
    return new ArrayList<ResourceGrant>(idToGrant.values());
  }

  /**
   * Get a snapshot of the grants used for this session for the web server.
   * This method should be synchronized by the caller around the session.
   *
   * @return List of {@link GrantReport} objects for this session sorted by id.
   */
  public List<GrantReport> getGrantReportList() {
    Map<Integer, GrantReport> grantReportMap =
      new TreeMap<Integer, GrantReport>();
    for (Map.Entry<Integer, ResourceGrant> entry : idToGrant.entrySet()) {
      grantReportMap.put(entry.getKey(),
        new GrantReport(entry.getKey().intValue(),
                        entry.getValue().getAddress().toString(),
                        entry.getValue().getType()));
    }
    return new ArrayList<GrantReport>(grantReportMap.values());
  }

  public long getStartTime() {
    return startTime;
  }

  /**
   * Obtain a list of grants to be preempted
   * @param maxGrantsToPreempt The maximum number of grants to preempt
   * @param maxRunningTime The running time of the preempted grants cannot be
   *                      larger than this number (in ms)
   * @param type The task type of the grants
   * @return The list of grant id to be preempted
   */
  public List<Integer> getGrantsToPreempt(
      int maxGrantsToPreempt, long maxRunningTime, ResourceType type) {
    if (deleted) {
      return Collections.emptyList();
    }
    List<ResourceGrant> candidates = getGrantsYoungerThan(maxRunningTime, type);
    List<Integer> grantIds = new ArrayList<Integer>();
    if (candidates.size() <= maxGrantsToPreempt) {
      // In this case, we can return the whole list without sorting
      for (ResourceGrant grant : candidates) {
        grantIds.add(grant.id);
      }
      return grantIds;
    }
    sortGrantsByStartTime(candidates);
    for (ResourceGrant grant : candidates) {
      grantIds.add(grant.id);
      if (grantIds.size() == maxGrantsToPreempt) {
        break;
      }
    }
    LOG.info("Find " + grantIds.size() + " " + type +
        " grants younger than " + maxRunningTime + " ms to preempt");
    return grantIds;
  }

  /**
   * Get all the grants that have been running for less than maxRunningTime
   * @param maxRunningTime the threshold to check against
   * @param type the type of the grants to return
   * @return the list of grants of a given type that have been granted less
   * than maxRunningTime ago
   */
  private List<ResourceGrant> getGrantsYoungerThan(
      long maxRunningTime, ResourceType type) {
    long now = ClusterManager.clock.getTime();
    List<ResourceGrant> candidates = new ArrayList<ResourceGrant>();
    for (ResourceGrant grant : getGrants()) {
      if (now - grant.getGrantedTime() < maxRunningTime &&
          type.equals(grant.getType())) {
        candidates.add(grant);
      }
    }
    return candidates;
  }

  /**
   * Sort grants on granted time in descending order
   * @param grants the list of grants to sort
   */
  private void sortGrantsByStartTime(List<ResourceGrant> grants) {
    Collections.sort(grants, new Comparator<ResourceGrant>() {
      @Override
      public int compare(ResourceGrant g1, ResourceGrant g2) {
        if (g1.grantedTime < g2.grantedTime) {
          return 1;
        }
        if (g1.grantedTime > g2.grantedTime) {
          return -1;
        }
        return g2.id - g1.id;
      }
    });
  }
}
