package org.apache.hadoop.corona;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.net.Node;

public class ClusterNode {

  public static final Log LOG =
    LogFactory.getLog(ClusterNode.class);

  public static class GrantId {
    public String       sessionId;
    public int          requestId;
    private final String      unique;

    public GrantId (String sessionId, int requestId) {
      this.sessionId = sessionId;
      this.requestId = requestId;
      this.unique = sessionId + requestId;
    }

    @Override
    public int hashCode() {
      return unique.hashCode();
    }

    @Override
    public boolean equals(Object that) {
      if (that == null)
        return false;
      if (that instanceof GrantId)
        return this.equals((GrantId)that);
      return false;
    }

    public boolean equals(GrantId that) {
      if (that == null)
        return false;

      return (this.unique.equals(that.unique));
    }
  }

  public ClusterNodeInfo        clusterNodeInfo;
  public long                   lastHeartbeatTime;
  public boolean                deleted = false;
  public final Node                   hostNode;
  public Map<String, Integer>   resourceTypeToMaxCpu;
  public Map<String, IntWritable>   resourceTypeToAllocatedCpu;

  protected Map<GrantId, ResourceRequest> grants =
    new HashMap<GrantId, ResourceRequest> ();

  protected ComputeSpecs        granted =
    new ComputeSpecs(); // All integral fields get initialized to 0.

  private void initResourceTypeToCpu(Map<Integer, Map<String, Integer>> cpuToResourcePartitioning) {

    Map<String, Integer> ret = cpuToResourcePartitioning.get((int)clusterNodeInfo.total.numCpus);

    if (ret == null) {
      Map<String, Integer> oneCpuMap = cpuToResourcePartitioning.get(1);
      if (oneCpuMap == null) {
        throw new RuntimeException ("No matching entry for cpu count: " + clusterNodeInfo.total.numCpus
                                    + " in node " + clusterNodeInfo.toString() + " and no 1 cpu map");
      }

      ret = new HashMap<String, Integer> ();
      for (String key: oneCpuMap.keySet()) {
        ret.put(key, oneCpuMap.get(key).intValue() * clusterNodeInfo.total.numCpus);
      }
    }

    resourceTypeToMaxCpu = ret;
    resourceTypeToAllocatedCpu = new HashMap<String, IntWritable> ();
    for (String key: ret.keySet()) {
      resourceTypeToAllocatedCpu.put(key, new IntWritable(0));
    }
  }

  public ClusterNode(ClusterNodeInfo clusterNodeInfo, Node node,
                     Map<Integer, Map<String, Integer>> cpuToResourcePartitioning) {
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

  public String getAppInfo() {
    return clusterNodeInfo.appInfo;
  }

  public Set<GrantId> getGrants() {
    HashSet<GrantId> ret = new HashSet<GrantId> ();
    ret.addAll(grants.keySet());
    return (ret);
  }

  public int getMaxCpuForType(String type) {
    Integer i = resourceTypeToMaxCpu.get(type);
    if (i == null)
      return 0;
    else
      return i.intValue();
  }

  public int getAllocatedCpuForType(String type) {
    IntWritable i = resourceTypeToAllocatedCpu.get(type);
    if (i == null)
      return 0;
    else
      return i.get();
  }
}

