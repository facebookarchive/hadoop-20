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


import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.net.Node;
import org.apache.hadoop.util.CoronaSerializer;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.JsonToken;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.ConcurrentModificationException;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.TreeMap;

/**
 * Manages the communication between a client and the {@link ClusterManager}
 * and the resources given to it from the {@link ClusterManager}.
 */
public class Session {
  /** Class logger */
  private static final Log LOG = LogFactory.getLog(Session.class);
  /** The request vector received from the SessionDriver */
  protected HashMap<Integer, ResourceRequestInfo> idToRequest =
      new HashMap<Integer, ResourceRequestInfo>();
  /** Derived list of pending requests */
  protected HashMap<Integer, ResourceRequestInfo> idToPendingRequests
    = new HashMap<Integer, ResourceRequestInfo>();
  /** The request grants that have been made for this session */
  private final HashMap<Integer, ResourceGrant> idToGrant =
    new HashMap<Integer, ResourceGrant>();
  /** Overall status of this session */
  private SessionStatus status = SessionStatus.RUNNING;
  /** Unique identifier for this session */
  private final String sessionId;
  /** {@link SessionManager} can delete this session. */
  private boolean deleted = false;
  /** Deleted time */
  private long deletedTime = -1;
  /** Information about the session from the creator */
  private final SessionInfo info;
  /** Time that this session was started (in millis) */
  private final long startTime;
  /** Last time a heartbeat occurred */
  private long lastHeartbeatTime;
  /** expected heartbeat info to sync with JT, inclduing the last resource requestId
   *  from JT and the last grantId to JT */
  private HeartbeatArgs expectedInfo;
  /** the last time when heartbeat info from JT matches CM's
   *  or is different from its own previous one
   */ 
  private long lastSyncTime;
  /** If the heartbeat infos between CM and JT mismatch, this is the last heartbeat
   *  from JT 
   */
  private HeartbeatArgs lastHeartbeat;
  /** The max time for CM to wait for JT to catch up */
  private final long maxDelay;
  
  /**
   * Pool info (after possible redirection), info has the original pool info
   * strings.
   */
  private final PoolInfo poolInfo;
  /**
   * Map of resource types to the resource context.
   */
  private Map<ResourceType, Context> typeToContext =
      new EnumMap<ResourceType, Context>(ResourceType.class);
  /**
   * Keeps track of the first wait for all the resource types.  An entry
   * that exists with a null value indicates that the resource has been
   * requested, but has no first wait time.  Must be synchronized on self
   * to use.
   */
  private Map<ResourceType, Long> typeToFirstWait =
      new EnumMap<ResourceType, Long>(ResourceType.class);

  /**
   * We do not rebuild idToPendingRequests immediately after reading from the
   * CM state file. So we keep the list of the ids of pending requests, so as
   * to help us rebuild it later.
   */
  public List<Integer> pendingRequestsList = new ArrayList<Integer>();

  /**
   * Constructor.
   *
   * @param id Should be a unique id
   * @param info Information about the session
   */
  public Session(long maxDelay, String id, SessionInfo info, ConfigManager configManager) {
    this.maxDelay = maxDelay;
    this.sessionId = id;
    this.info = info;
    PoolInfo tmpPoolInfo = PoolInfo.createPoolInfo(info.getPoolInfoStrings());
    poolInfo = configManager.getRedirect(tmpPoolInfo);
    this.startTime = ClusterManager.clock.getTime();
    this.lastHeartbeatTime = startTime;
    this.expectedInfo = new HeartbeatArgs();
    this.expectedInfo.requestId = 0;
    this.expectedInfo.grantId = 0;
    this.lastSyncTime = System.currentTimeMillis();
    this.lastHeartbeat = new HeartbeatArgs();
    this.lastHeartbeat.requestId = 0;
    this.lastHeartbeat.grantId = 0;
  }

  /**
   * Constructor for Session, used when we are reading back the
   * ClusterManager state from the disk
   *
   * @param coronaSerializer The CoronaSerializer instance to be used to
   *                         read the JSON
   * @throws IOException
   */
  public Session(long maxDelay, CoronaSerializer coronaSerializer) throws IOException {
    this.maxDelay = maxDelay;
    // Expecting the START_OBJECT token for Session
    coronaSerializer.readStartObjectToken("Session");

    readIdToRequest(coronaSerializer);

    readIdToPendingRequests(coronaSerializer);

    readIdToGrant(coronaSerializer);

    coronaSerializer.readField("status");
    status = coronaSerializer.readValueAs(SessionStatus.class);

    coronaSerializer.readField("sessionId");
    this.sessionId = coronaSerializer.readValueAs(String.class);

    coronaSerializer.readField("deleted");
    deleted = coronaSerializer.readValueAs(Boolean.class);

    coronaSerializer.readField("deletedTime");
    deletedTime = coronaSerializer.readValueAs(Long.class);

    coronaSerializer.readField("info");
    this.info = coronaSerializer.readValueAs(SessionInfo.class);
    
    

    coronaSerializer.readField("startTime");
    this.startTime = coronaSerializer.readValueAs(Long.class);

    coronaSerializer.readField("poolInfo");
    this.poolInfo = new PoolInfo(coronaSerializer);

    readTypeToFirstWait(coronaSerializer);
    try {
      coronaSerializer.readField("expectedInfo");
      this.expectedInfo = coronaSerializer.readValueAs(HeartbeatArgs.class);
    
      coronaSerializer.readField("lastHeartbeat");
      this.lastHeartbeat= coronaSerializer.readValueAs(HeartbeatArgs.class);

      coronaSerializer.readField("lastSyncTime");
      this.lastSyncTime = coronaSerializer.readValueAs(Long.class);
    } catch (IOException e) {
      this.expectedInfo= new HeartbeatArgs();
      this.expectedInfo.requestId = 0;
      this.expectedInfo.grantId = 0;
      this.lastSyncTime = System.currentTimeMillis();
      this.lastHeartbeat = new HeartbeatArgs();
      this.lastHeartbeat.requestId = 0;
      this.lastHeartbeat.grantId = 0;
      return;
    }

    // Expecting the END_OBJECT token for Session
    coronaSerializer.readEndObjectToken("Session");
  }

  /**
   * Reads the idToRequest map from a JSON stream
   *
   * @param coronaSerializer The CoronaSerializer instance to be used to
   *                         read the JSON
   * @throws IOException
   */
  private void readIdToRequest(CoronaSerializer coronaSerializer)
    throws IOException {
    coronaSerializer.readField("idToRequest");
    // Expecting the START_OBJECT token for idToRequest
    coronaSerializer.readStartObjectToken("idToRequest");
    JsonToken current = coronaSerializer.nextToken();
    while (current != JsonToken.END_OBJECT) {
      Integer id = Integer.parseInt(coronaSerializer.getFieldName());
      idToRequest.put(id, new ResourceRequestInfo(coronaSerializer));
      current = coronaSerializer.nextToken();
    }
    // Done with reading the END_OBJECT token for idToRequest
  }

  /**
   * Reads the idToPendingRequests map from a JSON stream
   *
   * @param coronaSerializer The CoronaSerializer instance to be used to
   *                         read the JSON
   * @throws IOException
   */
  private void readIdToPendingRequests(CoronaSerializer coronaSerializer)
    throws IOException {
    coronaSerializer.readField("idToPendingRequests");
    // Expecting the START_ARRAY token for idToPendingRequests
    coronaSerializer.readStartArrayToken("idToPendingRequests");
    JsonToken current = coronaSerializer.nextToken();
    while (current != JsonToken.END_ARRAY) {
      pendingRequestsList.add(coronaSerializer.jsonParser.getIntValue());
      current = coronaSerializer.nextToken();
    }
    // Done with reading the END_ARRAY token for idToPendingRequests
  }

  /**
   * Reads the idToGrant map from a JSON stream
   *
   * @param coronaSerializer The CoronaSerializer instance to be used to
   *                         read the JSON
   * @throws IOException
   */
  private void readIdToGrant(CoronaSerializer coronaSerializer)
    throws IOException {
    coronaSerializer.readField("idToGrant");
    // Expecting the START_OBJECT token for idToGrant
    coronaSerializer.readStartObjectToken("idToGrant");
    JsonToken current = coronaSerializer.nextToken();
    while (current != JsonToken.END_OBJECT) {
      Integer id = Integer.parseInt(coronaSerializer.getFieldName());
      ResourceGrant resourceGrant =
        coronaSerializer.readValueAs(ResourceGrant.class);
      idToGrant.put(id, new ResourceGrant(resourceGrant));
      current = coronaSerializer.nextToken();
    }
    // Done with reading the END_OBJECT token for idToGrant
  }

  /**
   * Reads the typeToFirstWait map from the JSON stream
   *
   * @param coronaSerializer The CoronaSerializer instance to be used to
   *                         read the JSON
   * @throws IOException
   */
  private void readTypeToFirstWait(CoronaSerializer coronaSerializer)
    throws IOException {
    coronaSerializer.readField("typeToFirstWait");
    // Expecting the START_OBJECT token for typeToFirstWait
    coronaSerializer.readStartObjectToken("typeToFirstWait");
    JsonToken current = coronaSerializer.nextToken();
    while (current != JsonToken.END_OBJECT) {
      String resourceTypeStr = coronaSerializer.getFieldName();
      Long wait = coronaSerializer.readValueAs(Long.class);
      current = coronaSerializer.nextToken();
      if (wait == -1) {
        wait = null;
      }
      typeToFirstWait.put(ResourceType.valueOf(resourceTypeStr), wait);
    }
    // Done with reading the END_OBJECT token for typeToFirstWait
  }

  /**
   * Used to write the state of the Session instance to disk, when we are
   * persisting the state of the ClusterManager
   *
   * @param jsonGenerator The JsonGenerator instance being used to write JSON
   *                      to disk
   * @throws IOException
   */
  public void write(JsonGenerator jsonGenerator) throws IOException {
    jsonGenerator.writeStartObject();

    jsonGenerator.writeFieldName("idToRequest");
    jsonGenerator.writeStartObject();
    for (Integer id : idToRequest.keySet()) {
      jsonGenerator.writeFieldName(id.toString());
      idToRequest.get(id).write(jsonGenerator);
    }
    jsonGenerator.writeEndObject();

    // idToPendingRequests is the same, and we only need to persist the
    // array of pending request ids
    jsonGenerator.writeFieldName("idToPendingRequests");
    jsonGenerator.writeStartArray();
    for (Integer id : idToPendingRequests.keySet()) {
      jsonGenerator.writeNumber(id);
    }
    jsonGenerator.writeEndArray();

    jsonGenerator.writeFieldName("idToGrant");
    jsonGenerator.writeStartObject();
    for (Integer id : idToGrant.keySet()) {
      jsonGenerator.writeObjectField(id.toString(), idToGrant.get(id));
    }
    jsonGenerator.writeEndObject();

    jsonGenerator.writeObjectField("status", status);

    jsonGenerator.writeStringField("sessionId", sessionId);

    jsonGenerator.writeBooleanField("deleted", deleted);

    jsonGenerator.writeNumberField("deletedTime", deletedTime);

    jsonGenerator.writeObjectField("info", info);
    
    jsonGenerator.writeNumberField("startTime", startTime);

    jsonGenerator.writeFieldName("poolInfo");
    poolInfo.write(jsonGenerator);

    jsonGenerator.writeFieldName("typeToFirstWait");
    jsonGenerator.writeStartObject();
    for (ResourceType resourceType : typeToFirstWait.keySet()) {
      Long wait = typeToFirstWait.get(resourceType);
      if (wait == null) {
        wait = new Long(-1);
      }
      jsonGenerator.writeNumberField(resourceType.toString(), wait);
    }
    jsonGenerator.writeEndObject();

    jsonGenerator.writeObjectField("expectedInfo", expectedInfo);
    
    jsonGenerator.writeObjectField("lastHeartbeat", lastHeartbeat);
    
    jsonGenerator.writeNumberField("lastSyncTime", lastSyncTime);

    jsonGenerator.writeEndObject();
    // No need to serialize lastHeartbeatTime, it will be reset.
    // typeToContext can be rebuilt
  }

  /**
   *  This method rebuilds members related to the Session instance, which were
   *  not directly persisted themselves.
   */
  public void restoreAfterSafeModeRestart() {
    for (Integer pendingRequestId : pendingRequestsList) {
      ResourceRequestInfo request = idToRequest.get(pendingRequestId);
      incrementRequestCount(request.getType(), 1);
      addPendingRequest(request);
    }

    for (Integer grantedRequestId : idToGrant.keySet()) {
      ResourceRequestInfo request = idToRequest.get(grantedRequestId);
      incrementRequestCount(request.getType(), 1);
      addGrantedRequest(request);
    }
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

  /**
   * Mark this session deleted and record the time.
   */
  public void setDeleted() {
    this.deleted = true;
    this.deletedTime = ClusterManager.clock.getTime();
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

  /**
   * Get the pool info from the session info.  May have been redirected.
   *
   * @return Converted pool info from Thrift PoolInfoStrings
   */
  public PoolInfo getPoolInfo() {
    return poolInfo;
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
   * Update the priority of the running session.
   *
   * @param priority the new priority of the session
   */
  public void updateSessionPriority(SessionPriority priority) {
    this.info.priority = priority;
  }

  /**
   * Update the deadline of the running session.
   *
   * @param deadline the new deadline for the session
   */
  public void updateSessionDeadline(long deadline) {
    this.info.deadline = deadline;
  }

  /**
   * Find a pending request that wants to run on a given host.
   * @param host the host
   * @param type the type of the request
   * @return The resource request info for the matching request
   */
  public ResourceRequestInfo getPendingRequestOnHost(
    String host, ResourceType type) {
    Context c = getContext(type);
    List<ResourceRequestInfo> hostReqs = c.hostToPendingRequests.get(host);
    if (hostReqs != null) {
      // Returning the first element in the list makes the subsequent
      // call to Utilities.removeReference() very fast.
      return hostReqs.get(0);
    }
    return null;
  }

  /**
   * Find a pending request that wants to run on a given rack.
   * @param host The host we are considering
   * @param rack The rack of the host
   * @param type the type of the request
   * @return The resource request info for the matching request
   */
  public ResourceRequestInfo getPendingRequestOnRack(
    String host, Node rack, ResourceType type) {
    Context c = getContext(type);
    List<ResourceRequestInfo> rackReqs = c.rackToPendingRequests.get(rack);
    if (rackReqs != null) {
      for (ResourceRequestInfo req: rackReqs) {
        Set<String> excluded = req.getExcludeHosts();
        if (!excluded.contains(host)) {
          return req;
        }
      }
    }
    return null;
  }

  /**
   * Find a pending request that can run on any machine.
   * @param host The host we are considering
   * @param type the type of the request
   * @return The resource request info for the matching request
   */
  public ResourceRequestInfo getPendingRequestForAny(
    String host, ResourceType type) {
    Context c = getContext(type);
    for (ResourceRequestInfo req: c.anyHostRequests) {
      Set<String> excluded = req.getExcludeHosts();
      if (!excluded.contains(host)) {
        return req;
      }
    }
    return null;
  }

  /**
   * Keeps track of the resource requests (pending, granted, fulfilled,
   * revoked, etc.) for a session and a particular resource type.
   */
  public static class Context {
    /** Index from host to pending requests. */
    protected HashMap<String, List<ResourceRequestInfo>> hostToPendingRequests
      = new HashMap<String, List<ResourceRequestInfo>>();

    /** Index from rack to pending requests. */
    protected HashMap<Node, List<ResourceRequestInfo>> rackToPendingRequests =
      new HashMap<Node, List<ResourceRequestInfo>>();

    /** The list of requests that don't have specific hosts requested */
    protected List<ResourceRequestInfo> anyHostRequests =
      new LinkedList<ResourceRequestInfo>();

    /** Requests that are waiting to be fulfilled */
    private List<ResourceRequestInfo> pendingRequests =
      new ArrayList<ResourceRequestInfo>();
    /** Granted requests */
    private List<ResourceRequestInfo> grantedRequests =
      new ArrayList<ResourceRequestInfo>();
    /** Iterator over pending requests that remembers the last position. */
    private PendingRequestIterator pendingRequestIterator =
      new PendingRequestIterator(pendingRequests);
    /** Number of requests at any point in time == pending + granted */
    private int requestCount = 0;

    /** Number of pending requests. */
    private int pendingRequestCount = 0;
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
    
    private List<Long> resourceUsages;

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
  protected void addPendingRequestForType(ResourceRequestInfo req) {
    Context c = getContext(req.getType());
    c.pendingRequests.add(req);
    c.pendingRequestCount++;
  }

  /**
   * Remove a pending resource request for a resource type
   *
   * @param req Resource request to remove
   */
  protected void removePendingRequestForType(ResourceRequestInfo req) {
    Context c = getContext(req.getType());
    Object removed = Utilities.removeReference(c.pendingRequests, req);
    if (removed != null) {
      c.pendingRequestCount--;
    }
  }

  /**
   * Add a granted request for a resource type.
   *
   * @param req Resource request to add
   */
  protected void addGrantedRequest(ResourceRequestInfo req) {
    Context c = getContext(req.getType());
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
  protected void removeGrantedRequest(
    ResourceRequestInfo req, boolean isRevoked) {
    Context c = getContext(req.getType());
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
  public List<ResourceRequestInfo> getGrantedRequestForType(ResourceType type) {
    return getContext(type).grantedRequests;
  }

  /**
   * Get the pending resources for a resource type
   *
   * @param type Type of resource
   * @return List of pending resource requests
   */
  public List<ResourceRequestInfo> getPendingRequestForType(ResourceType type) {
    return getContext(type).pendingRequests;
  }

  /**
   * Get the iterator for pending requests of a certain type.
   * @param type The type.
   * @return The iterator.
   */
  public Iterator<ResourceRequestInfo> getPendingRequestIteratorForType(
    ResourceType type) {
    return getContext(type).pendingRequestIterator.startIteration();
  }

  /**
   * Stateful iterator on pending requests.
   */
  private static class PendingRequestIterator
      implements Iterator<ResourceRequestInfo> {
    /** The underlying list. */
    private final ArrayList<ResourceRequestInfo> list;
    /** Current position in the list. */
    private int currPos = 0;
    /** Position at the start of iteration. */
    private int startPos = 0;
    /** Flag to indicate start. */
    private boolean started = false;

    /**
     * Constructor
     * @param list The underlying list.
     */
    private PendingRequestIterator(List<ResourceRequestInfo> list) {
      this.list = (ArrayList<ResourceRequestInfo>) list;
    }

    /**
     * Adjusts the current position pointer based on size.
     */
    private void adjustPosition() {
      if (currPos >= list.size()) {
        // Possible if the list has shrunk between calls
        currPos = 0;
      }
    }

    /**
     * Start iteration.
     * @return the iterator.
     */
    private PendingRequestIterator startIteration() {
      adjustPosition();
      startPos = currPos;
      started = true;
      return this;
    }

    @Override
    public boolean hasNext() {
      if (currPos >= list.size() && list.size() > 0) {
        throw new ConcurrentModificationException();
      }
      return (currPos != startPos || started) && list.size() > 0;
    }

    @Override
    public ResourceRequestInfo next() {
      if (currPos >= list.size()) {
        throw new ConcurrentModificationException();
      }
      if (currPos == startPos && !started) {
        throw new NoSuchElementException();
      }
      started = false;
      ResourceRequestInfo ret = list.get(currPos);
      currPos++;
      if (currPos >= list.size()) {
        currPos = 0;
      }
      return ret;
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException("There was no need for this");
    }
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
   * Return the number pending requests for a type.
   * @param type the resource type.
   * @return The count
   */
  public int getPendingRequestCountForType(ResourceType type) {
    return getContext(type).pendingRequestCount;
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
  public void requestResource(List<ResourceRequestInfo> requestList) {
    if (deleted) {
      throw new RuntimeException("Session: " + sessionId +
          " has been deleted");
    }

    for (ResourceRequestInfo req : requestList) {
      boolean newRequest = idToRequest.put(req.getId(), req) == null;
      if (!newRequest) {
        LOG.warn("Duplicate request from Session: " + sessionId + "" +
            " request: " + req.getId());
        continue;
      }

      incrementRequestCount(req.getType(), 1);
      addPendingRequest(req);
      setTypeRequested(req.getType());
    }
  }

  /**
   * Set the resource type to requested for first wait metrics.  The value will
   * be set to null to denote the difference between waiting and requested and
   * not waiting.
   *
   * @param type Resource type that is will be set to requested for metrics.
   */
  private void setTypeRequested(ResourceType type) {
    synchronized (typeToFirstWait) {
      if (!typeToFirstWait.containsKey(type)) {
        typeToFirstWait.put(type, null);
      }
    }
  }

  /**
   * Add a request to the list of pending
   * @param req the request to add
   */
  private void addPendingRequest(ResourceRequestInfo req) {
    idToPendingRequests.put(req.getId(), req);
    if (req.getHosts() != null && req.getHosts().size() > 0) {
      Context c = getContext(req.getType());
      for (RequestedNode node: req.getRequestedNodes()) {
        String host = node.getHost();
        List<ResourceRequestInfo> hostReqs = c.hostToPendingRequests.get(host);
        if (hostReqs == null) {
          hostReqs = new LinkedList<ResourceRequestInfo>();
          c.hostToPendingRequests.put(host, hostReqs);
        }
        hostReqs.add(req);
        Node rack = node.getRack();
        List<ResourceRequestInfo> rackReqs = c.rackToPendingRequests.get(rack);
        if (rackReqs == null) {
          rackReqs = new LinkedList<ResourceRequestInfo>();
          c.rackToPendingRequests.put(rack, rackReqs);
        }
        rackReqs.add(req);
      }
    }
    // Always add to the "any" list.
    Context c = getContext(req.getType());
    c.anyHostRequests.add(req);
    addPendingRequestForType(req);
  }

  /**
   * Removes the request from the list of pending
   * @param req the request to remove
   */
  private void removePendingRequest(ResourceRequestInfo req) {
    ResourceRequestInfo removed = idToPendingRequests.remove(req.getId());
    if (removed != null) {
      if (req.getHosts() != null && req.getHosts().size() > 0) {
        Context c = getContext(req.getType());
        for (RequestedNode node : req.getRequestedNodes()) {
          String host = node.getHost();
          List<ResourceRequestInfo> hostReqs =
            c.hostToPendingRequests.get(host);
          Utilities.removeReference(hostReqs, req);
          if (hostReqs.isEmpty()) {
            c.hostToPendingRequests.remove(host);
          }
          Node rack = node.getRack();
          List<ResourceRequestInfo> rackReqs =
            c.rackToPendingRequests.get(rack);
          Utilities.removeReference(rackReqs, req);
          if (rackReqs.isEmpty()) {
            c.rackToPendingRequests.remove(rack);
          }
        }
      }
      Context c = getContext(req.getType());
      Utilities.removeReference(c.anyHostRequests, req);
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
      ResourceRequestInfo req = idToRequest.get(id);

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

        incrementRequestCount(req.getType(), -1);
      }
    }
    return canceledGrants;
  }

  /**
   * Grant a resource to a session to satisfy a request.  Update the finalized
   * first resource type times if not already set
   * @param req the request being satisfied
   * @param grant the grant satisfying the request
   */
  public void grantResource(ResourceRequestInfo req, ResourceGrant grant) {
    if (deleted) {
      throw new RuntimeException("Session: " +
          sessionId + " has been deleted");
    }

    removePendingRequest(req);
    idToGrant.put(req.getId(), grant);
    addGrantedRequest(req);

    // Handle the first wait metrics
    synchronized (typeToFirstWait) {
      if (!typeToFirstWait.containsKey(req.getType())) {
        throw new IllegalStateException(
            "Impossible to get a grant prior to requesting a resource.");
      }
      Long firstWait = typeToFirstWait.get(req.getType());
      if (firstWait == null) {
        firstWait = new Long(ClusterManager.clock.getTime() - startTime);
        typeToFirstWait.put(req.getType(), firstWait);
      }
    }
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
      ResourceRequestInfo req = idToRequest.get(id);
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

  public Collection<ResourceRequestInfo> getPendingRequests() {
    return new ArrayList<ResourceRequestInfo>(idToPendingRequests.values());
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
                        entry.getValue().getType(),
                        entry.getValue().getGrantedTime()));
    }
    return new ArrayList<GrantReport>(grantReportMap.values());
  }

  public long getStartTime() {
    return startTime;
  }

  public long getDeletedTime() {
    return deletedTime;
  }
 
  public void setResourceRequest(List<ResourceRequest> requestList) {
    int maxid = 0;
    for (ResourceRequest request: requestList) {
      if (maxid < request.id) {
        maxid = request.id;
      }
    }
    expectedInfo.requestId = maxid;
  }
  
  public void setResourceGrant(List<ResourceGrant> grantList) {
    int maxid = 0;
    for (ResourceGrant grant: grantList) {
      if (maxid < grant.id) {
        maxid = grant.id;
      }
    }
    synchronized (expectedInfo) {
      expectedInfo.grantId = maxid;
    }
  }

  /** Check if the heartbeatInfo between JT and CM are in sync. 
   *  If this method returns false, it means JT and CM are out
   *  of sync very badly, and the session will be killed.
   */
  public boolean checkHeartbeatInfo(HeartbeatArgs jtInfo) {
    if (expectedInfo.requestId != jtInfo.requestId){
      LOG.fatal("heartbeat out-of-sync:" + sessionId + 
        " CM:" + expectedInfo.requestId + " " + expectedInfo.grantId +
        " JT:" + jtInfo.requestId + " " + jtInfo.grantId);
      return false;
    }
    if (expectedInfo.grantId == jtInfo.grantId){
      // perfect match
      if (LOG.isDebugEnabled()) {
        LOG.debug("heartbeat match:" + sessionId); 
      }
      lastSyncTime = System.currentTimeMillis();
      lastHeartbeat.requestId = 0;
      lastHeartbeat.grantId = 0;
      return true;
    }
    // delay
    if ( jtInfo.grantId != lastHeartbeat.grantId) {
      LOG.info("heartbeat mismatch with progress:" + sessionId + 
        " CM:" + expectedInfo.requestId + " " + expectedInfo.grantId +
        " JT:" + jtInfo.requestId + " " + jtInfo.grantId);
      lastSyncTime = System.currentTimeMillis();
      lastHeartbeat.requestId = jtInfo.requestId;
      lastHeartbeat.grantId = jtInfo.grantId;
      return true;
    }
    if (System.currentTimeMillis() - lastSyncTime > maxDelay) {
      // no progress
      LOG.error("heartbeat out-of-sync:" + sessionId + 
        " CM:" + expectedInfo.requestId + " " + expectedInfo.grantId +
        " JT:" + jtInfo.requestId + " " + jtInfo.grantId);
      return true;
    }
    LOG.info("heartbeat mismatch with no progress:" + sessionId + 
      " CM:" + expectedInfo.requestId + " " + expectedInfo.grantId +
      " JT:" + jtInfo.requestId + " " + jtInfo.grantId);
    return true;
  }
  
  /**
   * Get the time to start (or waited so far) for the first resource of a type.
   * If there is no such request, this will return null.  This is thread safe.
   *
   * @param type Type to check
   * @return Time to start or waited so far in ms or null if not requested.
   */
  public Long getTypeFirstWaitMs(ResourceType type) {
    synchronized(typeToFirstWait) {
      // Was it ever requested?
      if (!typeToFirstWait.containsKey(type)) {
        return null;
      }
      Long firstWait = typeToFirstWait.get(type);
      if (firstWait == null) {
        return ClusterManager.clock.getTime() - startTime;
      }

      return firstWait;
    }
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
      LOG.warn("Attempt to preempt from deleted session " + getSessionId());
      return Collections.emptyList();
    }
    List<ResourceGrant> candidates = getGrantsYoungerThan(maxRunningTime, type);
    List<Integer> grantIds = new ArrayList<Integer>();
    if (candidates.size() <= maxGrantsToPreempt) {
      // In this case, we can return the whole list without sorting
      for (ResourceGrant grant : candidates) {
        grantIds.add(grant.id);
      }
    } else {
      sortGrantsByStartTime(candidates);
      for (ResourceGrant grant : candidates) {
        grantIds.add(grant.id);
        if (grantIds.size() == maxGrantsToPreempt) {
          break;
        }
      }
    }
    LOG.info("Found " + grantIds.size() + " " + type +
        " grants younger than " + maxRunningTime + " ms to preempt in " +
        getSessionId());
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
  
  public void storeResourceUsages(Map<ResourceType, List<Long>> resourceUsages) {
    synchronized(this.typeToContext) {
      for (ResourceType type: resourceUsages.keySet()) {
        this.getContext(type).resourceUsages = resourceUsages.get(type);
      }
    }
  }
  
  public List<Long> getResourceUsageForType(ResourceType type) {
    synchronized(this.typeToContext) {
      return this.getContext(type).resourceUsages;
    }
  }
}
