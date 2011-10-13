package org.apache.hadoop.corona;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapred.JobPriority;

public class Session {

  public static final Log LOG = LogFactory.getLog(Session.class);
  public SessionStatus status = SessionStatus.RUNNING;

  public final String sessionId;
  public boolean deleted = false;
  protected final SessionInfo info;
  protected final long startTime;
  protected long lastHeartbeatTime;

  public Session(String id, SessionInfo info) {
    this.sessionId = id;
    this.info = info;
    this.lastHeartbeatTime = this.startTime = ClusterManager.clock.getTime();
  }

  public String getHandle() {
    return sessionId;
  }

  public SessionInfo getInfo() {
    return info;
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

  public JobPriority getPriority() {
    return JobPriority.values()[info.priority.getValue()];
  }

  public boolean isNoPreempt() {
    return info.noPreempt;
  }

  public String getUrl() {
    return info.url;
  }

  public void updateInfo(SessionInfo info) {
    this.info.url = info.url;
    this.info.name = info.name;
  }


  public static class Context {
    List<ResourceRequest>       pendingRequests = new ArrayList<ResourceRequest> ();
    List<ResourceRequest>       grantedRequests = new ArrayList<ResourceRequest> ();

    // the number of requests at any point in time == pending + granted
    int                        requestCount = 0;

    // the maximum number of concurrent requests across the entire session lifetime
    int                        maxConcurrentRequestCount = 0;

    // the total number of resources granted and released (and not revoked)
    // this gives us a sense for the total resource utilization by session
    int                        fulfilledRequestCount = 0;

    // how many times did we cancel a grant
    int                        revokedRequestCount = 0;
  }

  private HashMap<String, Context> typeToContext
    = new HashMap<String, Context> ();

  protected Context getContext(String type) {
    Context c = typeToContext.get(type);
    if (c == null) {
      c = new Context();
      typeToContext.put(type, c);
    }
    return c;
  }

  protected void incrementRequestCount(String type, int delta) {
    Context c = getContext(type);
    int newRequestCount = c.requestCount + delta;
    c.requestCount = newRequestCount;
    if (newRequestCount > c.maxConcurrentRequestCount)
      c.maxConcurrentRequestCount = newRequestCount;
  }

  protected void addPendingRequestForType(ResourceRequest req) {
    getContext(req.type).pendingRequests.add(req);
  }

  protected void removePendingRequestForType(ResourceRequest req) {
    Utilities.removeReference(getContext(req.type).pendingRequests,
                              req);
  }

  protected void addGrantedRequest(ResourceRequest req) {
    Context c = getContext(req.type);
    c.grantedRequests.add(req);

    // assume a granted resource is going to be fully used
    c.fulfilledRequestCount++;
  }

  protected void removeGrantedRequest(ResourceRequest req, boolean isRevoked) {
    Context c = getContext(req.type);
    Utilities.removeReference(c.grantedRequests, req);

    // if revoked - we didn't fulfill this request
    if (isRevoked) {
      c.fulfilledRequestCount--;
      c.revokedRequestCount++;
    }
  }

  public List<ResourceRequest> getGrantedRequestForType(String type) {
    return getContext(type).grantedRequests;
  }

  public List<ResourceRequest> getPendingRequestForType(String type) {
    return getContext(type).pendingRequests;
  }

  public int getRequestCountForType(String type) {
    return getContext(type).requestCount;
  }

  public int getGrantCountForType(String type) {
    return getRequestCountForType(type) - getPendingRequestForType(type).size();
  }

  public int getMaxConcurrentRequestCountForType(String type) {
    return getContext(type).maxConcurrentRequestCount;
  }

  public int getFulfilledRequestCountForType(String type) {
    return getContext(type).fulfilledRequestCount;
  }

  public int getRevokedRequestCountForType(String type) {
    return getContext(type).revokedRequestCount;
  }

  public Set<String> getTypes() { return typeToContext.keySet(); }


  // the request vector received from the SessionDriver
  protected HashMap<Integer, ResourceRequest> idToRequest 
    = new HashMap<Integer, ResourceRequest> ();

  // the request grants that have been made for this session
  protected HashMap<Integer, ResourceGrant> idToGrant
    = new HashMap<Integer, ResourceGrant> ();

  // derived list of pending requests
  protected HashMap<Integer, ResourceRequest> idToPendingRequests 
    = new HashMap<Integer, ResourceRequest> ();

  // for pending requests, indexes to enable efficient matching
  protected HashMap<String, List<ResourceRequest>> hostToPendingRequests
    = new HashMap<String, List<ResourceRequest>> ();

  protected List<ResourceRequest> anyHostRequests
    = new ArrayList<ResourceRequest> ();


  /**
   * We expose the mutable map only once the session has been marked deleted and
   * all data structures are effectively immutable
   */
  public Collection<ResourceGrant> getGrants() {
    return idToGrant.values();
  }

  public void requestResource(List<ResourceRequest> requestList) {
    if (deleted)
      throw new RuntimeException ("Session: " + sessionId + " has been deleted");

    for (ResourceRequest req: requestList) {
      boolean newRequest = (idToRequest.put(req.id, req) == null);
      if (!newRequest) {
        LOG.warn("Duplicate request from Session: " + sessionId + " request: " + req.id);
        continue;
      }

      incrementRequestCount(req.type, 1);
      addPendingRequest(req);
    }
  }

  private void addPendingRequest(ResourceRequest req) {
    idToPendingRequests.put(req.id, req);
    if (req.hosts != null) {
      for (String host: req.hosts) {
        List<ResourceRequest> hostReqs = hostToPendingRequests.get(host);
        if (hostReqs == null) {
          hostReqs = new ArrayList<ResourceRequest> ();
          hostToPendingRequests.put(host, hostReqs);
        }
        hostReqs.add(req);
      }
    } else {
      anyHostRequests.add(req);
    }

    addPendingRequestForType(req);
  }


  private void removePendingRequest(ResourceRequest req) {
    idToPendingRequests.remove(req.id);
    if (req.hosts != null) {
      for (String host: req.hosts) {
        List<ResourceRequest> hostReqs = hostToPendingRequests.get(host);

        if (hostReqs != null) {
          Utilities.removeReference(hostReqs, req);
          if (hostReqs.isEmpty()) {
            hostToPendingRequests.remove(host);
          }
        } else {
          // TODO: this is a bug - the index is not consistent
        }
      }
    } else {
      Utilities.removeReference(anyHostRequests, req);
    }

    removePendingRequestForType(req);
  }


  public List<ResourceGrant> releaseResource(List<Integer> idList) {

    if (deleted)
      throw new RuntimeException ("Session: " + sessionId + " has been deleted");

    List<ResourceGrant> canceledGrants = new ArrayList<ResourceGrant> ();
    for (Integer id: idList) {
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

  public void grantResource(ResourceRequest req, ResourceGrant grant) {
    if (deleted)
      throw new RuntimeException ("Session: " + sessionId + " has been deleted");

    removePendingRequest(req);
    idToGrant.put(req.id, grant);
    addGrantedRequest(req);
  }

  public List<ResourceGrant> revokeResource(List<Integer> idList) {
    if (deleted)
      throw new RuntimeException ("Session: " + sessionId + " has been deleted");

    List<ResourceGrant> canceledGrants = new ArrayList<ResourceGrant> ();
    for (Integer id: idList) {
      ResourceRequest req = idToRequest.get(id);
      ResourceGrant grant = idToGrant.remove(id);
      if (grant != null) {

        if (req == null)
          throw new RuntimeException("Session: " + sessionId + ", requestId: " + id
                                     + " grant exists but request doesn't");

        removeGrantedRequest(req, true);

        // the revoked grants are now pending again
        addPendingRequest(req);

        // we have previously granted this resource, return to caller
        canceledGrants.add(grant);
      }
    }
    return canceledGrants;
  }

  public void heartbeat () {
    lastHeartbeatTime = ClusterManager.clock.getTime();
  }

  public int getPendingRequestCount() {
    return idToPendingRequests.size();
  }

  public Collection<ResourceRequest> getPendingRequests() {
    return new ArrayList<ResourceRequest> (idToPendingRequests.values());
  }

  public Collection<ResourceGrant> getGrantedRequests() {
    return new ArrayList<ResourceGrant> (idToGrant.values());
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
      int maxGrantsToPreempt, long maxRunningTime, String type) {
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

  private List<ResourceGrant> getGrantsYoungerThan(
      long maxRunningTime, String type) {
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
   * @param grants
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
