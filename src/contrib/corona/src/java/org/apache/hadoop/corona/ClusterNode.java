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

import java.io.IOException;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.net.Node;
import org.apache.hadoop.util.CoronaSerializer;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.JsonToken;

public class ClusterNode {
  /** Class logger */
  public static final Log LOG =
    LogFactory.getLog(ClusterNode.class);
  public long lastHeartbeatTime;
  public boolean deleted = false;
  // This is no longer a final because when we restart after an upgrade, we will
  // initialize the hostNode outside the constructor.
  public Node hostNode;
  private ClusterNodeInfo clusterNodeInfo;
  private volatile ComputeSpecs freeSpecs;
  private Map<ResourceType, Integer> resourceTypeToMaxCpu =
    new EnumMap<ResourceType, Integer>(ResourceType.class);
  private Map<ResourceType, Stats> resourceTypeToStatsMap =
    new EnumMap<ResourceType, Stats>(ResourceType.class);

  protected Map<GrantId, ResourceRequestInfo> grants =
    new HashMap<GrantId, ResourceRequestInfo>();

  protected ComputeSpecs granted =
    new ComputeSpecs(); // All integral fields get initialized to 0.

  public static class Stats {
    private volatile int allocatedCpu;
    private volatile int grantCount;
  }

  /**
   *  Metadata about a granted resource on a node.
   */
  public static class GrantId {
    /** Session identifier */
    private final String sessionId;
    /** Request identifier */
    private final int requestId;
    /** Unique name based on sessionId and requestId */
    private final String unique;

    /**
     * Constructor.
     *
     * @param sessionId Session id using this grant
     * @param requestId Grant id
     */
    public GrantId(String sessionId, int requestId) {
      this.sessionId = sessionId;
      this.requestId = requestId;
      this.unique = sessionId + requestId;
    }

    /**
     * Constructor for GrantId, used when we are reading back the state from
     * the disk
     * @param coronaSerializer The CoronaSerializer instance being used to read
     *                         the JSON from disk
     * @throws IOException
     */
    public GrantId(CoronaSerializer coronaSerializer) throws IOException {
      // Expecting the START_OBJECT token for GrantId
      coronaSerializer.readStartObjectToken("GrantId");

      coronaSerializer.readField("sessionId");
      this.sessionId = coronaSerializer.readValueAs(String.class);
      coronaSerializer.readField("requestId");
      this.requestId = coronaSerializer.readValueAs(Integer.class);

      // Expecting the END_OBJECT token for GrantId
      coronaSerializer.readEndObjectToken("GrantId");
      this.unique = this.sessionId + this.requestId;
    }

    public String getSessionId() {
      return sessionId;
    }

    public int getRequestId() {
      return requestId;
    }

    @Override
    public int hashCode() {
      return unique.hashCode();
    }

    @Override
    public boolean equals(Object that) {
      if (that == null) {
        return false;
      }
      if (that instanceof GrantId) {
        return this.equals((GrantId) that);
      }
      return false;
    }

    /**
     * Check if it equals another GrantId
     *
     * @param that Other GrandId
     * @return True if the same, false otherwise
     */
    public boolean equals(GrantId that) {
      if (that == null) {
        return false;
      }

      return this.unique.equals(that.unique);
    }

    /**
     * Used to write the state of the GrantId instance to disk, when we are
     * persisting the state of the NodeManager
     * @param jsonGenerator The JsonGenerator instance being used to write JSON
     *                      to disk
     * @throws IOException
     */
    public void write(JsonGenerator jsonGenerator) throws IOException {
      jsonGenerator.writeStartObject();
      jsonGenerator.writeObjectField("sessionId", sessionId);
      jsonGenerator.writeObjectField("requestId", requestId);
      jsonGenerator.writeEndObject();
    }
  }

  public ClusterNode(
    ClusterNodeInfo clusterNodeInfo, Node node,
    Map<Integer, Map<ResourceType, Integer>> cpuToResourcePartitioning) {
    clusterNodeInfo.address.host = clusterNodeInfo.address.host.intern();
    this.clusterNodeInfo = clusterNodeInfo;
    this.freeSpecs = clusterNodeInfo.getFree();
    lastHeartbeatTime = ClusterManager.clock.getTime();
    this.hostNode = node;
    resetResourceTypeToStatsMap();
    initResourceTypeToMaxCpuMap(cpuToResourcePartitioning);
  }

  /**
   * Constructor for ClusterNode, used when we are reading back the state from
   * the disk
   *  @param coronaSerializer The CoronaSerializer instance being used to read
   *                          the JSON from disk
   * @throws IOException
   */
  ClusterNode(CoronaSerializer coronaSerializer) throws IOException {
    // Initialize the resourceTypeToStatsMap map
    resetResourceTypeToStatsMap();

    // Expecting the START_OBJECT token for ClusterNode
    coronaSerializer.readStartObjectToken("ClusterNode");
    readClusterNodeInfo(coronaSerializer);
    coronaSerializer.readField("grants");
    readGrants(coronaSerializer);

    // Expecting the END_OBJECT token for ClusterManager
    coronaSerializer.readEndObjectToken("ClusterNode");

    // We will initialize the hostNode field later in the restoreClusterNode()
    // method in NodeManager, which is the last stage of restoring the
    // NodeManager state
    hostNode = null;

    // Done with reading the END_OBJECT token for ClusterNode
  }

  /**
   * Reads the clusterNodeInfo object from the JSON stream
   *  @param coronaSerializer The CoronaSerializer instance being used to read
   *                          the JSON from disk
   * @throws IOException
   */
  private void readClusterNodeInfo(CoronaSerializer coronaSerializer)
    throws IOException {
  coronaSerializer.readField("clusterNodeInfo");
    clusterNodeInfo = new ClusterNodeInfo();
    // Expecting the START_OBJECT token for clusterNodeInfo
    coronaSerializer.readStartObjectToken("clusterNodeInfo");

    coronaSerializer.readField("name");
    clusterNodeInfo.name = coronaSerializer.readValueAs(String.class);

    coronaSerializer.readField("address");
    clusterNodeInfo.address = coronaSerializer.readValueAs(InetAddress.class);

    coronaSerializer.readField("total");
    clusterNodeInfo.total = coronaSerializer.readValueAs(ComputeSpecs.class);

    coronaSerializer.readField("free");
    clusterNodeInfo.free = coronaSerializer.readValueAs(ComputeSpecs.class);

    coronaSerializer.readField("resourceInfos");
    clusterNodeInfo.resourceInfos = coronaSerializer.readValueAs(Map.class);

    // Expecting the END_OBJECT token for clusterNodeInfo
    coronaSerializer.readEndObjectToken("clusterNodeInfo");
  }

  /**
   * Reads the list of grants from the JSON stream
   *  @param coronaSerializer The CoronaSerializer instance being used to read
   *                          the JSON from disk
   * @throws IOException
   */
  private void readGrants(CoronaSerializer coronaSerializer)
    throws IOException {
    // Expecting the START_OBJECT token for grants
    coronaSerializer.readStartObjectToken("grants");
    JsonToken current = coronaSerializer.nextToken();
    while (current != JsonToken.END_OBJECT) {
      // We can access the key for the grant, but it is not required
      // Expecting the START_OBJECT token for the grant
      coronaSerializer.readStartObjectToken("grant");

      coronaSerializer.readField("grantId");
      GrantId grantId = new GrantId(coronaSerializer);
      coronaSerializer.readField("grant");
      ResourceRequestInfo resourceRequestInfo =
        new ResourceRequestInfo(coronaSerializer);

      // Expecting the END_OBJECT token for the grant
      coronaSerializer.readEndObjectToken("grant");

      // This will update the grants map and the resourceTypeToStatsMap map
      addGrant(grantId.getSessionId(), resourceRequestInfo);

      current = coronaSerializer.nextToken();
    }
  }

  /**
   * Used to write the state of the ClusterNode instance to disk, when we are
   * persisting the state of the NodeManager
   * @param jsonGenerator The JsonGenerator instance being used to write JSON
   *                      to disk
   * @throws IOException
   */
  public void write(JsonGenerator jsonGenerator) throws IOException {
    jsonGenerator.writeStartObject();

    // clusterNodeInfo begins
    jsonGenerator.writeFieldName("clusterNodeInfo");
    jsonGenerator.writeStartObject();
    jsonGenerator.writeStringField("name", clusterNodeInfo.name);
    jsonGenerator.writeObjectField("address", clusterNodeInfo.address);
    jsonGenerator.writeObjectField("total", clusterNodeInfo.total);
    jsonGenerator.writeObjectField("free", clusterNodeInfo.free);
    jsonGenerator.writeObjectField("resourceInfos",
      clusterNodeInfo.resourceInfos);
    jsonGenerator.writeEndObject();
    // clusterNodeInfo ends

    // grants begins
    jsonGenerator.writeFieldName("grants");
    jsonGenerator.writeStartObject();
    for (Map.Entry<GrantId, ResourceRequestInfo> entry : grants.entrySet()) {
      jsonGenerator.writeFieldName(entry.getKey().unique);
      jsonGenerator.writeStartObject();
      jsonGenerator.writeFieldName("grantId");
      entry.getKey().write(jsonGenerator);
      jsonGenerator.writeFieldName("grant");
      entry.getValue().write(jsonGenerator);
      jsonGenerator.writeEndObject();
    }
    jsonGenerator.writeEndObject();
    // grants ends

    jsonGenerator.writeEndObject();
    // We skip the hostNode and lastHeartbeatTime as they need not be persisted.
    // resourceTypeToMaxCpu and resourceTypeToStatsMap can be rebuilt using the
    // conf and the grants respectively.
  }

  /**
   * This method is used to reset the mapping of resource type to stats.
   */
  public void resetResourceTypeToStatsMap() {
    for (ResourceType type : ResourceType.values()) {
      resourceTypeToStatsMap.put(type, new Stats());
    }
  }

  /**
   * This method is used to initialize the resource type to max CPU mapping
   * based upon the cpuToResourcePartitioning instance given
   * @param cpuToResourcePartitioning Mapping of cpus to resources to be used
   */
  public void initResourceTypeToMaxCpuMap(Map<Integer, Map<ResourceType,
                                          Integer>> cpuToResourcePartitioning) {
    resourceTypeToMaxCpu =
      getResourceTypeToCountMap((int) clusterNodeInfo.total.numCpus,
        cpuToResourcePartitioning);
  }

  /**
   * Get a mapping of the resource type to amount of resources for a given
   * number of cpus.
   *
   * @param numCpus Number of cpus available
   * @param cpuToResourcePartitioning Mapping of number of cpus to resources
   * @return Resources for this amount of cpus
   */
  public static Map<ResourceType, Integer> getResourceTypeToCountMap(
      int numCpus,
      Map<Integer, Map<ResourceType, Integer>> cpuToResourcePartitioning) {
    Map<ResourceType, Integer> ret =
        cpuToResourcePartitioning.get(numCpus);
    if (ret == null) {
      Map<ResourceType, Integer> oneCpuMap = cpuToResourcePartitioning.get(1);
      if (oneCpuMap == null) {
        throw new RuntimeException(
          "No matching entry for cpu count: " + numCpus +
          " in node and no 1 cpu map");
      }

      ret = new EnumMap<ResourceType, Integer>(ResourceType.class);
      for (ResourceType key: oneCpuMap.keySet()) {
        ret.put(key, oneCpuMap.get(key).intValue() * numCpus);
      }
    }
    return ret;
  }

  public void addGrant(String sessionId, ResourceRequestInfo req) {
    if (deleted)
      throw new RuntimeException ("Node " + getName() + " has been deleted");

    grants.put(new GrantId(sessionId, req.getId()), req);
    incrementGrantCount(req.getType());

    // update allocated counts
    Utilities.incrComputeSpecs(granted,  req.getSpecs());
    Stats stats = resourceTypeToStatsMap.get(req.getType());
    stats.allocatedCpu += req.getSpecs().numCpus;

    //LOG.info("Node " +  getName() + " has granted " + granted.numCpus + " cpus");
  }

  public ResourceRequestInfo getRequestForGrant(String sessionId, int requestId) {
    return grants.get(new GrantId(sessionId, requestId));
  }

  public void cancelGrant(String sessionId, int requestId) {
    if (deleted)
      throw new RuntimeException ("Node " + getName() + " has been deleted");

    ResourceRequestInfo req = grants.remove(new GrantId(sessionId, requestId));
    if (req != null) {
      Utilities.decrComputeSpecs(granted,  req.getSpecs());
      Stats stats = resourceTypeToStatsMap.get(req.getType());
      stats.allocatedCpu -= req.getSpecs().numCpus;
      decrementGrantCount(req.getType());
    }
  }

  public boolean checkForGrant(
    ResourceRequest req, ResourceLimit resourceLimit) {
    if (deleted)
      throw new RuntimeException ("Node " + getName() + " has been deleted");

    int cpuAlloced = resourceTypeToStatsMap.get(req.type).allocatedCpu;
    Integer cpuMax = resourceTypeToMaxCpu.get(req.type);
    boolean enoughCpu = cpuMax.intValue() >= req.getSpecs().numCpus + cpuAlloced;
    boolean enoughMem = resourceLimit.hasEnoughResource(this);
    return enoughCpu && enoughMem;
  }

  public void heartbeat(ClusterNodeInfo newClusterNodeInfo) {
    lastHeartbeatTime = ClusterManager.clock.getTime();
    freeSpecs = newClusterNodeInfo.getFree();
  }

  public String getName() {
    return clusterNodeInfo.name;
  }

  public String getHost() {
    return clusterNodeInfo.address.host;
  }

  public InetAddress getAddress() {
    return clusterNodeInfo.address;
  }

  public ClusterNodeInfo getClusterNodeInfo() {
    return clusterNodeInfo;
  }

  public ComputeSpecs getFree() {
    return freeSpecs;
  }

  public ComputeSpecs getTotal() {
    return clusterNodeInfo.getTotal();
  }

  public Set<GrantId> getGrants() {
    HashSet<GrantId> ret = new HashSet<GrantId> ();
    ret.addAll(grants.keySet());
    return (ret);
  }

  private void incrementGrantCount(ResourceType type) {
    resourceTypeToStatsMap.get(type).grantCount++;
  }

  private void decrementGrantCount(ResourceType type) {
    resourceTypeToStatsMap.get(type).grantCount--;
  }

  public int getGrantCount(ResourceType type) {
    return resourceTypeToStatsMap.get(type).grantCount;
  }

  public Set<GrantId> getGrants(ResourceType type) {
    HashSet<GrantId> ret = new HashSet<GrantId> ();
    for (Map.Entry<GrantId, ResourceRequestInfo> entry :
        grants.entrySet()) {
      if (entry.getValue().getType().equals(type)) {
        ret.add(entry.getKey());
      }
    }
    return ret;
  }

  public int getMaxCpuForType(ResourceType type) {
    Integer i = resourceTypeToMaxCpu.get(type);
    if (i == null)
      return 0;
    else
      return i.intValue();
  }

  public int getAllocatedCpuForType(ResourceType type) {
    return resourceTypeToStatsMap.get(type).allocatedCpu;
  }
}

