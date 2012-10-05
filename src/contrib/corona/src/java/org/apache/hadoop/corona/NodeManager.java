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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.HashSet;
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
import org.apache.hadoop.util.HostsFileReader;
import org.apache.hadoop.util.StringUtils;

/**
 * Manages all the nodes known in the cluster.
 */
public class NodeManager implements Configurable {
  /** Class logger */
  public static final Log LOG = LogFactory.getLog(NodeManager.class);

  /** Configuration. */
  protected CoronaConf conf;
  /** The Cluster Manager. */
  protected ClusterManager clusterManager;

  /**
   * Secondary index on nodes. This is an index of runnable nodes for a resource
   * type. There is one instance of this for each resource type.
   */
  public class RunnableIndices {
    /** Controls how frequently we shuffle the list of rack-runnable nodes. */
    private static final int RACK_SHUFFLE_PERIOD = 100;

    /** The lookup table of runnable nodes on hosts */
    protected ConcurrentMap<String, List<ClusterNode>> hostToRunnableNode
      = new ConcurrentHashMap<String, List<ClusterNode>>();

    /** The lookup table of runnable nodes in racks */
    protected ConcurrentMap<Node, List<ClusterNode>> rackToRunnableNode
      = new ConcurrentHashMap<Node, List<ClusterNode>>();

    /** The type of resource this RunnableIndices is tracking */
    private final ResourceType type;

    /**
     * Counter for checking if we need to shuffle the list of rack-runnable
     * nodes.
     */
    private int getRunnableNodeForRackCounter = 0;
    /**
     * Create a runnable indices for a given resource type
     * @param type the type of resource
     */
    public RunnableIndices(ResourceType type) {
      this.type = type;
    }

    /**
     * Get any runnable node that is not one of the excluded nodes
     * @param excluded the list of nodes to ignore
     * @return the runnable node, null if no runnable node can be found
     */
    public ClusterNode getRunnableNodeForAny(Set<String> excluded) {
      for (Map.Entry<String, List<ClusterNode>> e :
        hostToRunnableNode.entrySet()) {
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

    /**
     * Get runnable node local to the given host
     * @param host host that needs a local node
     * @return the node that is local to the host, null if
     * there are no runnable nodes local to the host
     */
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

    /**
     * Get a runnable node in the given rack that is not present in the
     * excluded list
     * @param rack the rack to get the node from
     * @param excluded the list of nodes to ignore
     * @return the runnable node from the rack satisfying conditions, null if
     * the node was not found
     */
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

    /**
     * Check if the node has enough resources to run tasks
     * @param node node to check
     * @return true if the node has enough resources, false otherwise
     */
    private boolean hasEnoughResource(ClusterNode node) {
      return hasEnoughMemory(node) && hasEnoughDiskSpace(node);
    }

    /**
     * Check if the node has enough memory to run tasks
     * @param node node to check
     * @return true if the node has enough memory, false otherwise
     */
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

    /**
     * Check if the ndoe has enough disk space to run tasks
     * @param node the node to check
     * @return true if the node has enough space, false otherwise
     */
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

    /**
     * Check if there are any runnable nodes
     * @return true if there are any runnable nodes, false otherwise
     */
    public boolean existRunnableNodes() {
      return hostToRunnableNode.size() > 0;
    }

    /**
     * Add a node to the runnable indices
     * @param clusterNode the node to add
     */
    public void addRunnable(ClusterNode clusterNode) {
      String host = clusterNode.clusterNodeInfo.address.host;

      if (LOG.isDebugEnabled()) {
        LOG.debug(clusterNode.getName() +
            " added to runnable list for type: " + type);
      }

      synchronized (clusterNode.hostNode) {
        List<ClusterNode> nlist = hostToRunnableNode.get(host);
        if (nlist == null) {
          nlist = new ArrayList<ClusterNode>(1);
          hostToRunnableNode.put(host, nlist);
        }
        nlist.add(clusterNode);
      }

      Node rack = clusterNode.hostNode.getParent();
      synchronized (rack) {
        List<ClusterNode> nlist = rackToRunnableNode.get(rack);
        if (nlist == null) {
          nlist = new ArrayList<ClusterNode>(1);
          rackToRunnableNode.put(rack, nlist);
        }
        nlist.add(clusterNode);
      }
    }

    /**
     * Remove the node from the runnable indices
     * @param node node to remove
     */
    public void deleteRunnable(ClusterNode node) {
      String host = node.getHost();

      if (LOG.isDebugEnabled()) {
        LOG.debug(node.getName() +
            " deleted from runnable list for type: " + type);
      }

      synchronized (node.hostNode) {
        List<ClusterNode> nlist = hostToRunnableNode.get(host);
        if (nlist != null) {
          Utilities.removeReference(nlist, node);
          if (nlist.isEmpty()) {
            hostToRunnableNode.remove(host);
          }
        }
      }

      Node rack = node.hostNode.getParent();
      synchronized (rack) {
        List<ClusterNode> nlist = rackToRunnableNode.get(rack);
        if (nlist != null) {
          Utilities.removeReference(nlist, node);
          if (nlist.isEmpty()) {
            rackToRunnableNode.remove(rack);
          }
        }
      }
    }
  }

  /** primary data structure mapping the unique name of the
   node to the node object */
  protected ConcurrentMap<String, ClusterNode> nameToNode =
    new ConcurrentHashMap<String, ClusterNode>();

  /** The registry of sessions running on the nodes */
  protected ConcurrentMap<ClusterNode, Set<String>> hostsToSessions
    = new ConcurrentHashMap<ClusterNode, Set<String>>();

  /** Tracks the applications active on the node. */
  protected ConcurrentMap<String, Map<ResourceType, String>> nameToApps =
    new ConcurrentHashMap<String, Map<ResourceType, String>>();

  /** Fault manager for the nodes */
  protected final FaultManager faultManager;

  /** secondary indices maintained for each resource type */
  protected Map<ResourceType, RunnableIndices> typeToIndices =
    new EnumMap<ResourceType, RunnableIndices>(ResourceType.class);

  /** The cache for local node lookups */
  protected TopologyCache topologyCache;
  /** The configuration of resources based on the CPUs */
  protected Map<Integer, Map<ResourceType, Integer>> cpuToResourcePartitioning;
  /** Shutdown flag */
  protected volatile boolean shutdown = false;

  /** The time before the node is declared dead if it doesn't heartbeat */
  protected int nodeExpiryInterval;
  /** A thread running expireNodes */
  protected Thread expireNodesThread = null;
  /** A runnable that is responsible for expiring nodes that don't heartbeat */
  private ExpireNodes expireNodes = new ExpireNodes();

  /** Hosts reader. */
  private final HostsFileReader hostsReader;
  /**
   * Amount of memory that must be free on a node before allocating resources on
   * it.
   */
  private volatile int nodeReservedMemoryMB;
  /**
   * Amount of disk (GB) that must be free on a node before allocating resources
   * on it.
   */
  private volatile int nodeReservedDiskGB;

  /**
   * NodeManager constructor given a cluster manager and a
   * {@link HostsFileReader} for includes/excludes lists
   * @param clusterManager the cluster manager
   * @param hostsReader the host reader for includes/excludes
   */
  public NodeManager(ClusterManager clusterManager,
      HostsFileReader hostsReader) {
    this.hostsReader = hostsReader;
    LOG.info("Included hosts: " + hostsReader.getHostNames().size() +
        " Excluded hosts: " + hostsReader.getExcludedHosts().size());
    this.clusterManager = clusterManager;
    this.expireNodesThread = new Thread(this.expireNodes,
                                       "expireNodes");
    this.expireNodesThread.setDaemon(true);
    this.expireNodesThread.start();
    this.faultManager = new FaultManager(this);
  }

  /**
   * Check if any runnable nodes are present in the cluster
   * @return true if runnable nodes are available, false otherwise
   */
  public boolean existRunnableNodes() {
    for (Map.Entry<ResourceType, RunnableIndices> entry :
        typeToIndices.entrySet()) {
      RunnableIndices r = entry.getValue();
      if (r.existRunnableNodes()) {
        return true;
      }
    }
    return false;
  }

  /**
   * See if there are any runnable nodes of a given type
   * @param type the type to look for
   * @return true if there are runnable nodes for this type, false otherwise
   */
  public boolean existRunnableNodes(ResourceType type) {
    RunnableIndices r = typeToIndices.get(type);
    return r.existRunnableNodes();
  }

  /**
   * Find the best matching node for this host subject to the maxLevel
   * constraint
   * @param host the host of the request
   * @param maxLevel the max locality level to consider
   * @param type the type of resource needed on the node
   * @param excluded the list of nodes to exclude from consideration
   * @return the runnable node satisfying the constraints
   */
  public ClusterNode getRunnableNode(String host, LocalityLevel maxLevel,
      ResourceType type, Set<String> excluded) {

    ClusterNode node = null;
    RunnableIndices r = typeToIndices.get(type);

    // find host local
    if (host != null) {
      node = r.getRunnableNodeForHost(host);
    }

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

    if (node == null && LOG.isDebugEnabled()) {
      LOG.debug("Could not find any node given: host = " + host +
          ", maxLevel = " + maxLevel + ", type = " + type +
          ", excludes = " + excluded);
    }

    return node;
  }

  /**
   * Add a node to be managed.
   *
   * @param node Node to be managed
   * @param resourceInfos Mapping of the resource type to runnable indices
   */
  protected void addNode(ClusterNode node,
                         Map<ResourceType, String> resourceInfos) {
    synchronized (node) {
      // 1: primary
      nameToNode.put(node.getName(), node);
      faultManager.addNode(node.getName(), resourceInfos.keySet());
      nameToApps.put(node.getName(), resourceInfos);
      hostsToSessions.put(node, new HashSet<String>());
      setAliveDeadMetrics();

      // 2: update runnable indices
      for (Map.Entry<ResourceType, RunnableIndices> entry :
          typeToIndices.entrySet()) {
        ResourceType type = entry.getKey();
        if (resourceInfos.containsKey(type)) {
          if (node.checkForGrant(Utilities.getUnitResourceRequest(type))) {
            RunnableIndices r = entry.getValue();
            r.addRunnable(node);
          }
        }
      }
    }
  }

  /**
   * Register a new application on the node
   * @param node the node to register on
   * @param type the type of an application
   * @param appInfo the appInfo string for the application
   */
  protected void addAppToNode(
      ClusterNode node, ResourceType type, String appInfo) {
    synchronized (node) {
      // Update primary index.
      Map<ResourceType, String> apps = nameToApps.get(node.getName());
      apps.put(type, appInfo);

      // Update runnable indices.
      for (Map.Entry<ResourceType, RunnableIndices> entry :
          typeToIndices.entrySet()) {
        if (type.equals(entry.getKey())) {
          if (node.checkForGrant(Utilities.getUnitResourceRequest(type))) {
            RunnableIndices r = entry.getValue();
            r.addRunnable(node);
          }
        }
      }
    }
  }

  /**
   * Get all the sessions that have grants on the node
   * @param nodeName the name of the node
   * @return the set of session ids that are running on the node
   */
  public Set<String> getNodeSessions(String nodeName) {
    ClusterNode node = nameToNode.get(nodeName);
    if (node == null) {
      LOG.warn("Trying to get the sessions for a non-existent node " +
        nodeName);
      return new HashSet<String>();
    }
    synchronized (node) {
      return new HashSet<String>(hostsToSessions.get(node));
    }
  }

  /**
   * Remove the references to the session
   * @param session the session to be deleted
   */
  public void deleteSession(String session) {
    for (Set<String> sessions : hostsToSessions.values()) {
      sessions.remove(session);
    }
  }
  /**
   * Delete the node from the cluster. This happens when the node times out
   * or is being decommissioned.
   * @param nodeName the name of the node to remove
   * @return the list of grants that are running on the node
   */
  public Set<ClusterNode.GrantId> deleteNode(String nodeName) {
    ClusterNode node = nameToNode.get(nodeName);
    if (node == null) {
      LOG.warn("Trying to delete non-existent node: " + nodeName);
      return null;
    }
    return deleteNode(node);
  }

  /**
   * Delete the node from the cluster. This happens when the node times out
   * or is being decommissioned.
   * @param node the node to remove
   * @return the list of grants that are running on the node
   */
  protected Set<ClusterNode.GrantId> deleteNode(ClusterNode node) {
    synchronized (node) {
      if (node.deleted) {
        return null;
      }

      node.deleted = true;
      // 1: primary
      nameToNode.remove(node.getName());
      faultManager.deleteNode(node.getName());
      nameToApps.remove(node.getName());
      hostsToSessions.remove(node);
      setAliveDeadMetrics();

      // 2: update runnable index
      for (RunnableIndices r : typeToIndices.values()) {
        r.deleteRunnable(node);
      }
      return node.getGrants();
    }
  }

  /**
   * Remove one application type from the node. Happens when the daemon
   * responsible for handling this application type on the node goes down
   * @param nodeName the name of the node
   * @param type the type of the resource
   * @return the list of grants that belonged to the application on this node
   */
  public Set<ClusterNode.GrantId> deleteAppFromNode(
      String nodeName, ResourceType type) {
    ClusterNode node = nameToNode.get(nodeName);
    if (node == null) {
      LOG.warn("Trying to delete type " + type +
        " from non-existent node: " + nodeName);
      return null;
    }
    return deleteAppFromNode(node, type);
  }

  /**
   * Remove one application type from the node. Happens when the daemon
   * responsible for handling this application type on the node goes down
   * @param node the node
   * @param type the type of the resource
   * @return the list of grants that belonged to the application on this node
   */
  protected Set<ClusterNode.GrantId> deleteAppFromNode(
      ClusterNode node, ResourceType type) {
    synchronized (node) {
      if (node.deleted) {
        return null;
      }

      nameToApps.remove(node.getName());
      RunnableIndices r = typeToIndices.get(type);
      r.deleteRunnable(node);

      return node.getGrants(type);
    }
  }

  /**
   * Cancel grant on a node
   * @param nodeName the node the grant is on
   * @param sessionId the session the grant was given to
   * @param requestId the request this grant satisfied
   */
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
          if (!faultManager.isBlacklisted(node.getName(), req.type)) {
            r.addRunnable(node);
          }
        }
      }
    }
  }

  /**
   * Add a grant to a node
   * @param node the node the grant is on
   * @param sessionId the session the grant is given to
   * @param req the request this grant satisfies
   * @return true if the grant can be added to the node, false otherwise
   */
  public boolean addGrant(
      ClusterNode node, String sessionId, ResourceRequest req) {
    synchronized (node) {
      if (node.deleted) {
        return false;
      }

      node.addGrant(sessionId, req);
      hostsToSessions.get(node).add(sessionId);
      if (!node.checkForGrant(Utilities.getUnitResourceRequest(req.type))) {
        RunnableIndices r = typeToIndices.get(req.type);
        r.deleteRunnable(node);
      }
    }
    return true;
  }

  @Override
  public void setConf(Configuration newConf) {
    this.conf = (CoronaConf) newConf;
    nodeExpiryInterval = conf.getNodeExpiryInterval();
    if (this.expireNodesThread != null) {
      this.expireNodesThread.interrupt();
    }

    topologyCache = new TopologyCache(conf);
    cpuToResourcePartitioning = conf.getCpuToResourcePartitioning();

    for (Map.Entry<Integer, Map<ResourceType, Integer>> entry :
          cpuToResourcePartitioning.entrySet()) {
      for (ResourceType type : entry.getValue().keySet()) {
        if (!typeToIndices.containsKey(type)) {
          typeToIndices.put(type, new RunnableIndices(type));
        }
      }
    }
    nodeReservedMemoryMB = conf.getNodeReservedMemoryMB();
    nodeReservedDiskGB = conf.getNodeReservedDiskGB();

    faultManager.setConf(conf);
  }

  @Override
  public Configuration getConf() {
    return conf;
  }

  /**
   * return true if a new node has been added - else return false
   * @param clusterNodeInfo the node that is heartbeating
   * @return true if this is a new node that has been added, false otherwise
   */
  public boolean heartbeat(ClusterNodeInfo clusterNodeInfo)
    throws DisallowedNode {
    if (!canAllowNode(clusterNodeInfo.getAddress().getHost())) {
      LOG.error("Host disallowed: "  + clusterNodeInfo.getAddress().getHost());
      throw new DisallowedNode(clusterNodeInfo.getAddress().getHost());
    }
    boolean newNode = false;
    ClusterNode node = nameToNode.get(clusterNodeInfo.name);
    Map<ResourceType, String> currentResources =
        clusterNodeInfo.getResourceInfos();
    if (currentResources == null) {
      currentResources = new EnumMap<ResourceType, String>(ResourceType.class);
    }

    if (node == null) {
      LOG.info("Adding node with heartbeat: " + clusterNodeInfo.toString());
      node = new ClusterNode(clusterNodeInfo,
          topologyCache.getNode(clusterNodeInfo.address.host),
          cpuToResourcePartitioning);
      addNode(node, currentResources);
      newNode = true;
    }

    node.heartbeat();

    boolean appsChanged = false;
    Map<ResourceType, String> prevResources =
        nameToApps.get(clusterNodeInfo.name);
    Set<ResourceType> deletedApps = null;
    for (Map.Entry<ResourceType, String> entry : prevResources.entrySet()) {
      String newAppInfo = currentResources.get(entry.getKey());
      String oldAppInfo = entry.getValue();
      if (newAppInfo == null || !newAppInfo.equals(oldAppInfo)) {
        if (deletedApps == null) {
          deletedApps = EnumSet.noneOf(ResourceType.class);
        }
        deletedApps.add(entry.getKey());
        appsChanged = true;
      }
    }
    Map<ResourceType, String> addedApps = null;
    for (Map.Entry<ResourceType, String> entry : currentResources.entrySet()) {
      String newAppInfo = entry.getValue();
      String oldAppInfo = prevResources.get(entry.getKey());
      if (oldAppInfo == null || !oldAppInfo.equals(newAppInfo)) {
        if (addedApps == null) {
          addedApps = new EnumMap<ResourceType, String>(ResourceType.class);
        }
        addedApps.put(entry.getKey(), entry.getValue());
        appsChanged = true;
      }
    }
    if (deletedApps != null) {
      for (ResourceType deleted : deletedApps) {
        clusterManager.nodeAppRemoved(clusterNodeInfo.name, deleted);
      }
    }
    if (addedApps != null) {
      for (Map.Entry<ResourceType, String> added: addedApps.entrySet()) {
        addAppToNode(node, added.getKey(), added.getValue());
      }
    }

    return newNode || appsChanged;
  }

  public String getAppInfo(ClusterNode node, ResourceType type) {
    Map<ResourceType, String> resourceInfos = nameToApps.get(node.getName());
    if (resourceInfos == null) {
      return null;
    } else {
      return resourceInfos.get(type);
    }
  }

  class ExpireNodes implements Runnable {

    @Override
    public void run() {
      while (!shutdown) {
        try {
          Thread.sleep(nodeExpiryInterval / 2);

          long now = ClusterManager.clock.getTime();
          for (ClusterNode node : nameToNode.values()) {
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

  /**
   * Used by the cm.jsp to get the list of resource types.
   *
   * @return Collection of resource types
   */
  public Collection<ResourceType> getResourceTypes() {
    return typeToIndices.keySet();
  }

  public int getMaxCpuForType(ResourceType type) {
    int total = 0;

    for (ClusterNode node: nameToNode.values()) {
      synchronized (node) {
        if (node.deleted) {
          continue;
        }
        total += node.getMaxCpuForType(type);
      }
    }
    return total;
  }

  public int getAllocatedCpuForType(ResourceType type) {
    int total = 0;

    for (ClusterNode node: nameToNode.values()) {
      synchronized (node) {
        if (node.deleted) {
          continue;
        }
        total += node.getAllocatedCpuForType(type);
      }
    }
    return total;
  }

  public int getTotalNodeCount() {
    return hostsReader.getHosts().size();
  }

  public Set<String> getAllNodes() {
    return hostsReader.getHostNames();
  }

  public int getExcludedNodeCount() {
    return hostsReader.getExcludedHosts().size();
  }

  public Set<String> getExcludedNodes() {
    return hostsReader.getExcludedHosts();
  }

  public int getAliveNodeCount() {
    return nameToNode.size();
  }

  public List<String> getAliveNodes() {
    return new ArrayList<String>(nameToNode.keySet());
  }

  public FaultManager getFaultManager() {
    return faultManager;
  }

  public synchronized void refreshNodes() throws IOException {
    hostsReader.refresh();
    LOG.info("After refresh Included hosts: " +
        hostsReader.getHostNames().size() +
        " Excluded hosts: " + hostsReader.getExcludedHosts().size());
    Set<String> newHosts = hostsReader.getHostNames();
    Set<String> newExcludes = hostsReader.getExcludedHosts();
    Set<String> hostsToDecommision = new HashSet<String>();
    for (Map.Entry<String, ClusterNode> entry : nameToNode.entrySet()) {
      String host = entry.getValue().getHost();
      // Check if not included or explicitly excluded.
      if (!newHosts.contains(host) || newExcludes.contains(host)) {
        hostsToDecommision.add(entry.getKey());
      }
    }
    for (String nodeName : hostsToDecommision) {
      clusterManager.nodeDecommisioned(nodeName);
    }
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

  public void nodeFeedback(
      String handle,
      List<ResourceType> resourceTypes,
      List<NodeUsageReport> reportList) {
    // Iterate over each report.
    for (NodeUsageReport usageReport : reportList) {
      faultManager.nodeFeedback(usageReport.getNodeName(), resourceTypes,
          usageReport);
    }
  }

  void blacklistNode(String nodeName, ResourceType resourceType) {
    LOG.info("Node " + nodeName + " has been blacklisted for resource " +
        resourceType);
    clusterManager.getMetrics().setBlacklistedNodes(
        faultManager.getBlacklistedNodeCount());
    deleteAppFromNode(nodeName, resourceType);
  }

  /**
   * Checks if a host is in the included hosts list. If the included hosts lists
   * is empty, the host is treated as included.
   *
   * @param host
   *          The host
   * @return a boolean indicating if the host is included.
   */
  private boolean inHostsList(String host) {
    Set<String> hosts = hostsReader.getHostNames();
    return (hosts.isEmpty() || hosts.contains(host));
  }

  /**
   * Checks if a host is in the excluded hosts list.
   *
   * @param host
   *          The host
   * @return a boolean indicating if the host is excluded.
   */
  private boolean inExcludedHostsList(String host) {
    return hostsReader.getExcludedHosts().contains(host);
  }

  /**
   * Checks if a host is allowed to communicate with the cluster manager.
   *
   * @param host
   *          The host
   * @return a boolean indicating if the host is allowed.
   */
  private boolean canAllowNode(String host) {
    return inHostsList(host) && !inExcludedHostsList(host);
  }

  /**
   * Update metrics for alive/dead nodes.
   */
  private void setAliveDeadMetrics() {
    clusterManager.getMetrics().setAliveNodes(nameToNode.size());
    int totalHosts = hostsReader.getHosts().size();
    if (totalHosts > 0) {
      clusterManager.getMetrics().setDeadNodes(
          totalHosts - nameToNode.size());
    }
  }
}
