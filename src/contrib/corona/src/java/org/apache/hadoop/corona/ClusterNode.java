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

import java.util.EnumMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.net.Node;

public class ClusterNode {
  /** Class logger */
  public static final Log LOG =
    LogFactory.getLog(ClusterNode.class);
  public ClusterNodeInfo clusterNodeInfo;
  public long lastHeartbeatTime;
  public boolean deleted = false;
  public final Node hostNode;
  public Map<ResourceType, Integer> resourceTypeToMaxCpu;
  public Map<ResourceType, IntWritable> resourceTypeToAllocatedCpu;

  protected Map<GrantId, ResourceRequest> grants =
    new HashMap<GrantId, ResourceRequest>();

  protected ComputeSpecs        granted =
    new ComputeSpecs(); // All integral fields get initialized to 0.

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
  }


  /**
   * Based on the mapping of cpus to resources, find the appropriate resources
   * for a given number of cpus and initialize the resource type to allocated
   * cpu mapping.
   *
   * @param cpuToResourcePartitioning Mapping of cpus to resources to be used
   */
  private void initResourceTypeToCpu(
      Map<Integer, Map<ResourceType, Integer>> cpuToResourcePartitioning) {
    resourceTypeToMaxCpu =
        getResourceTypeToCountMap((int) clusterNodeInfo.total.numCpus,
                                  cpuToResourcePartitioning);
    resourceTypeToAllocatedCpu =
        new EnumMap<ResourceType, IntWritable>(ResourceType.class);
    for (ResourceType key : resourceTypeToMaxCpu.keySet()) {
      resourceTypeToAllocatedCpu.put(key, new IntWritable(0));
    }
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


  public ClusterNode(
      ClusterNodeInfo clusterNodeInfo, Node node,
      Map<Integer, Map<ResourceType, Integer>> cpuToResourcePartitioning) {
    clusterNodeInfo.address.host = clusterNodeInfo.address.host.intern();
    this.clusterNodeInfo = clusterNodeInfo;
    lastHeartbeatTime = ClusterManager.clock.getTime();
    this.hostNode = node;
    initResourceTypeToCpu(cpuToResourcePartitioning);
  }

  public void addGrant(String sessionId, ResourceRequest req) {
    if (deleted)
      throw new RuntimeException ("Node " + getName() + " has been deleted");

    grants.put(new GrantId(sessionId, req.id), req);

    // update allocated counts
    Utilities.incrComputeSpecs(granted,  req.specs);
    IntWritable cpu = resourceTypeToAllocatedCpu.get(req.type);
    cpu.set(cpu.get() + req.specs.numCpus);

    //LOG.info("Node " +  getName() + " has granted " + granted.numCpus + " cpus");
  }

  public ResourceRequest getRequestForGrant(String sessionId, int requestId) {
    return grants.get(new GrantId(sessionId, requestId));
  }

  public void cancelGrant(String sessionId, int requestId) {
    if (deleted)
      throw new RuntimeException ("Node " + getName() + " has been deleted");

    ResourceRequest req = grants.remove(new GrantId(sessionId, requestId));
    if (req != null) {
      Utilities.decrComputeSpecs(granted,  req.specs);
      IntWritable cpu = resourceTypeToAllocatedCpu.get(req.type);
      cpu.set(cpu.get() - req.specs.numCpus);
    }
    //LOG.info("Node " +  getName() + " has granted " + granted.numCpus + " cpus");
  }

  public boolean checkForGrant(ResourceRequest req) {
    if (deleted)
      throw new RuntimeException ("Node " + getName() + " has been deleted");

    // only check for cpu availability for specific type right now
    IntWritable cpuAlloced = resourceTypeToAllocatedCpu.get(req.type);
    Integer cpuMax = resourceTypeToMaxCpu.get(req.type);
    return (cpuMax.intValue() >= req.specs.numCpus + cpuAlloced.get());
  }

  public void heartbeat() {
    lastHeartbeatTime = ClusterManager.clock.getTime();
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

  public Set<GrantId> getGrants() {
    HashSet<GrantId> ret = new HashSet<GrantId> ();
    ret.addAll(grants.keySet());
    return (ret);
  }

  public Set<GrantId> getGrants(ResourceType type) {
    HashSet<GrantId> ret = new HashSet<GrantId> ();
    for (Map.Entry<GrantId, ResourceRequest> entry: grants.entrySet()) {
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
    IntWritable i = resourceTypeToAllocatedCpu.get(type);
    if (i == null)
      return 0;
    else
      return i.get();
  }
}

