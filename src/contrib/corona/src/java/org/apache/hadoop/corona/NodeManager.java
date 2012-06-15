package org.apache.hadoop.corona;

import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.net.Node;
import org.apache.hadoop.util.StringUtils;

public class NodeManager implements Configurable {

  public static final Log LOG = LogFactory.getLog(NodeManager.class);

  protected CoronaConf conf;
  protected ClusterManager clusterManager;
  volatile private int nodeReservedMemoryMB;
  volatile private int nodeReservedDiskGB;

  // secondary indices maintained for each resource type
  public class RunnableIndices {

    private static final int RACK_SHUFFLE_PERIOD = 100;
    private int getRunnableNodeForRackCounter = 0;

    protected ConcurrentMap<String, List<ClusterNode>> hostToRunnableNode
      = new ConcurrentHashMap<String, List<ClusterNode>> ();

    protected ConcurrentMap<Node, List<ClusterNode>> rackToRunnableNode
      = new ConcurrentHashMap<Node, List<ClusterNode>> ();

    String type;

    public RunnableIndices(String type) {
      this.type = type;
    }

    public ClusterNode getRunnableNodeForAny(Set<String> excluded) {
      for(Map.Entry<String, List<ClusterNode>> e: hostToRunnableNode.entrySet()) {
        String host = e.getKey();
        synchronized (topologyCache.getNode(host)) {
          List<ClusterNode> nlist = e.getValue();
          if (nlist == null) {
            return null;
          }
          for (ClusterNode node : nlist) {
            if (excluded == null || !excluded.contains(node.getHost())) {
              if (hasEnoughResource(node)) {
                return node;
              }
            }
          }
        }
      }
      return null;
    }

    public ClusterNode getRunnableNodeForHost(String host) {
      synchronized (topologyCache.getNode(host)) {
        // there should only be one node per host in the common case
        List<ClusterNode> nlist = hostToRunnableNode.get(host);
        if (nlist == null) {
          return null;
        }
        for (ClusterNode node : nlist) {
          if (hasEnoughResource(node)) {
            return node;
          }
        }
        return null;
      }
    }

    public ClusterNode getRunnableNodeForRack(Node rack, Set<String> excluded) {
      synchronized (rack) {
        List<ClusterNode> nlist = rackToRunnableNode.get(rack);
        getRunnableNodeForRackCounter += 1;
        if (nlist == null) {
          return null;
        }
        if (getRunnableNodeForRackCounter % RACK_SHUFFLE_PERIOD == 0) {
          // This balances more evenly across nodes in a rack
          Collections.shuffle(nlist);
        }
        for (ClusterNode node : nlist) {
          if (excluded == null || !excluded.contains(node.getHost())) {
            if (hasEnoughResource(node)) {
              return node;
            }
          }
        }
        return null;
      }
    }

    private boolean hasEnoughResource(ClusterNode node) {
      return hasEnoughMemory(node) && hasEnoughDiskSpace(node);
    }

    private boolean hasEnoughMemory(ClusterNode node) {
      int used = node.clusterNodeInfo.getUsed().memoryMB;
      int total = node.clusterNodeInfo.getTotal().memoryMB;
      int free = total - used;
      if (free < nodeReservedMemoryMB) {
        LOG.info(node.getHost() + " not enough memory." +
            " totalMB:" + total +
            " used:" + used +
            " free:" + free +
            " limit:" + nodeReservedMemoryMB);
        return false;
      }
      return true;
    }

    private boolean hasEnoughDiskSpace(ClusterNode node) {
      int used = node.clusterNodeInfo.getUsed().diskGB;
      int total = node.clusterNodeInfo.getTotal().diskGB;
      int free = total - used;
      if (free < nodeReservedDiskGB) {
        LOG.info(node.getHost() + " not enough disk space." +
            " totalMB:" + total +
            " used:" + used +
            " free:" + free +
            " limit:" + nodeReservedDiskGB);
        return false;
      }
      return true;
    }

    public boolean existRunnableNodes() {
      return (hostToRunnableNode.size() > 0);
    }

    public  void addRunnable(ClusterNode clusterNode) {
      String host = clusterNode.clusterNodeInfo.address.host;

      if (LOG.isDebugEnabled())
        LOG.debug(clusterNode.getName() + " added to runnable list for type: " + type);

      synchronized (clusterNode.hostNode) {
        List<ClusterNode> nlist = hostToRunnableNode.get(host);
        if (nlist == null) {
          nlist = new ArrayList<ClusterNode> (1);
          hostToRunnableNode.put(host, nlist);
        }
        nlist.add(clusterNode);
      }

      Node rack = clusterNode.hostNode.getParent();
      synchronized (rack) {
        List<ClusterNode> nlist = rackToRunnableNode.get(rack);
        if (nlist == null) {
          nlist = new ArrayList<ClusterNode> (1);
          rackToRunnableNode.put(rack, nlist);
        }
        nlist.add(clusterNode);
      }
    }

    public void deleteRunnable(ClusterNode node) {
      String host = node.getHost();

      if (LOG.isDebugEnabled())
        LOG.debug (node.getName() + " deleted from runnable list for type: " + type);

      synchronized (node.hostNode) {
        List<ClusterNode> nlist = hostToRunnableNode.get(host);
        if (nlist != null) {
          Utilities.removeReference(nlist, node);
          if (nlist.isEmpty())
            hostToRunnableNode.remove(host);
        } else {
          // this is expected if the host was not runnable
        }
      }

      Node rack = node.hostNode.getParent();
      synchronized (rack) {
        List<ClusterNode> nlist = rackToRunnableNode.get(rack);
        if (nlist != null) {
          Utilities.removeReference(nlist, node);
          if (nlist.isEmpty())
            rackToRunnableNode.remove(rack);
        } else {
          // this is expected if the host was not runnable
        }
      }
    }
  }

  // primary data structure mapping the unique name of the
  // node to the node object
  protected ConcurrentMap<String, ClusterNode> nameToNode
    = new ConcurrentHashMap<String, ClusterNode> ();

  // secondary indices maintained for each resource type
  protected IdentityHashMap<String, RunnableIndices> typeToIndices =
    new IdentityHashMap<String, RunnableIndices> ();

  protected TopologyCache       topologyCache;
  protected Map<Integer, Map<String, Integer>> cpuToResourcePartitioning;
  protected volatile boolean    shutdown = false;

  // monitoring for node death/hang
  protected static int nodeExpiryInterval;
  protected Thread expireNodesThread = null;
  ExpireNodes expireNodes = new ExpireNodes();

  public boolean existRunnableNodes() {
    for (Map.Entry<String, RunnableIndices> entry: typeToIndices.entrySet()) {
      RunnableIndices r = entry.getValue();
      if (r.existRunnableNodes())
        return true;
    }
    return false;
  }

  public boolean existRunnableNodes(String type) {
    RunnableIndices r = typeToIndices.get(type);
    return r.existRunnableNodes();
  }

  /**
   * Find the best matching node for this host subject to the maxLevel
   * constraint
   */
  public ClusterNode getRunnableNode(String host, LocalityLevel maxLevel,
      String type, Set<String> excluded) {

    ClusterNode node = null;
    RunnableIndices r = typeToIndices.get(type);

    // find host local
    if (host != null)
      node = r.getRunnableNodeForHost(host);

    // find rack local if required and allowed
    if (node == null) {
      if ((host != null) && (maxLevel.compareTo(LocalityLevel.NODE) > 0)) {
        Node rack = topologyCache.getNode(host).getParent();
        node = r.getRunnableNodeForRack(rack, excluded);
      }
    }

    // find any node if required and allowed
    if ((node == null) && (maxLevel.compareTo(LocalityLevel.RACK) > 0)) {
      node = r.getRunnableNodeForAny(excluded);
    }

    return node;
  }

  protected void addNode(ClusterNode node) {
    synchronized (node) {
      // 1: primary
      nameToNode.put(node.getName(), node);
      clusterManager.getMetrics().setAliveNodes(nameToNode.size());

      // 2: update runnable indices
      for (Map.Entry<String, RunnableIndices> entry: typeToIndices.entrySet()) {
        String type = entry.getKey();
        if (node.checkForGrant(Utilities.getUnitResourceRequest(type))) {
          RunnableIndices r = entry.getValue();
          r.addRunnable(node);
        }
      }
    }
  }

  public Set<ClusterNode.GrantId> deleteNode(String nodeName) {
    ClusterNode node = nameToNode.get(nodeName);
    if (node == null) {
      LOG.warn("Trying to delete non-existent node: " + nodeName);
      return null;
    }
    return deleteNode(node);
  }

  protected Set<ClusterNode.GrantId> deleteNode(ClusterNode node) {
    synchronized (node) {
      if (node.deleted)
        return null;

      node.deleted = true;
      // 1: primary
      nameToNode.remove(node.getName());
      clusterManager.getMetrics().setAliveNodes(nameToNode.size());

      // 2: update runnable index
      for (RunnableIndices r: typeToIndices.values()) {
        r.deleteRunnable(node);
      }
      return node.getGrants();
    }
  }

  public void cancelGrant(String nodeName, String sessionId, int requestId) {
    ClusterNode node = nameToNode.get(nodeName);
    if (node == null) {
      LOG.warn("Canceling grant for non-existent node: " + nodeName);
      return;
    }
    synchronized (node) {
      if (node.deleted) {
        LOG.warn("Canceling grant for deleted node: " + nodeName);
        return;
      }
      ResourceRequest req = node.getRequestForGrant(sessionId, requestId);
      if (req != null) {
        ResourceRequest unitReq = Utilities.getUnitResourceRequest(req.type);
        boolean previouslyRunnable = node.checkForGrant(unitReq);
        node.cancelGrant(sessionId, requestId);
        if (!previouslyRunnable && node.checkForGrant(unitReq)) {
          RunnableIndices r = typeToIndices.get(req.type);
          r.addRunnable(node);
        }
      }
    }
  }

  public boolean addGrant(ClusterNode node, String sessionId, ResourceRequest req) {
    synchronized (node) {
      if (node.deleted)
        return false;

      node.addGrant(sessionId, req);
      if (!node.checkForGrant(Utilities.getUnitResourceRequest(req.type))) {
        RunnableIndices r = typeToIndices.get(req.type);
        r.deleteRunnable(node);
      }
    }
    return true;
  }

  public NodeManager(ClusterManager clusterManager) {
    this.clusterManager = clusterManager;
    this.expireNodesThread = new Thread(this.expireNodes,
                                       "expireNodes");
    this.expireNodesThread.setDaemon(true);
    this.expireNodesThread.start();
  }

  @Override
  public void setConf(Configuration _conf) {
    this.conf = (CoronaConf) _conf;
    nodeExpiryInterval = conf.getNodeExpiryInterval();
    if (this.expireNodesThread != null)
      this.expireNodesThread.interrupt();

    topologyCache = new TopologyCache(conf);
    cpuToResourcePartitioning = conf.getCpuToResourcePartitioning();

    for(Map.Entry<Integer, Map<String, Integer>> entry:
          cpuToResourcePartitioning.entrySet()) {
      for (String type: entry.getValue().keySet()) {
        type = type.intern();
        if (typeToIndices.get(type) == null) {
          typeToIndices.put(type, new RunnableIndices(type));
        }
      }
    }
    nodeReservedMemoryMB = conf.getNodeReservedMemoryMB();
    nodeReservedDiskGB = conf.getNodeReservedDiskGB();
  }

  @Override
  public Configuration getConf() {
    return conf;
  }

  /**
   * return true if a new node has been added - else return false
   */
  public boolean heartbeat(ClusterNodeInfo clusterNodeInfo) {
    boolean newNode = false;
    ClusterNode node = nameToNode.get(clusterNodeInfo.name);

    if (node == null) {
      LOG.info("Adding node with heartbeat: " + clusterNodeInfo.toString());
      node = new ClusterNode(clusterNodeInfo, topologyCache.getNode(clusterNodeInfo.address.host),
                             cpuToResourcePartitioning);
      addNode(node);
      newNode = true;
    }
    node.heartbeat();
    return newNode;
  }
  
  class ExpireNodes implements Runnable {

    public ExpireNodes() {
    }

    @Override
    public void run() {
      while (!shutdown) {
        try {
          Thread.sleep(nodeExpiryInterval/2);

          long now = ClusterManager.clock.getTime();
          for(ClusterNode node: nameToNode.values()) {
            if (now - node.lastHeartbeatTime > nodeExpiryInterval) {
              LOG.warn("Timing out node: " + node.getName());
              clusterManager.nodeTimeout(node.getName());
            }
          }

        } catch (InterruptedException iex) {
          // ignore. if shutting down, while cond. will catch it
        } catch (Exception t) {
          LOG.error("Node Expiry Thread got exception: " +
                    StringUtils.stringifyException(t));
        }
      }
    }

  }

  public List<String> getResourceTypes() {
    List<String> ret = new ArrayList<String> ();
    ret.addAll(typeToIndices.keySet());
    return (ret);
  }

  public int getMaxCpuForType(String type) {
    int total = 0;

    for (ClusterNode node: nameToNode.values()) {
      synchronized (node) {
        if (node.deleted)
          continue;
        total += node.getMaxCpuForType(type);
      }
    }
    return total;
  }

  public int getAllocatedCpuForType(String type) {
    int total = 0;

    for (ClusterNode node: nameToNode.values()) {
      synchronized (node) {
        if (node.deleted)
          continue;
        total += node.getAllocatedCpuForType(type);
      }
    }
    return total;
  }

  public int getTotalNodeCount() {
    return nameToNode.size();
  }

  public void setNodeReservedMemoryMB(int mb) {
    LOG.info("nodeReservedMemoryMB changed" +
        " from " + nodeReservedMemoryMB +
        " to " + mb);
    this.nodeReservedMemoryMB = mb;
  }

  public void setNodeReservedDiskGB(int gb) {
    LOG.info("nodeReservedDiskGB changed" +
        " from " + nodeReservedDiskGB +
        " to " + gb);
    this.nodeReservedDiskGB = gb;
  }
}

