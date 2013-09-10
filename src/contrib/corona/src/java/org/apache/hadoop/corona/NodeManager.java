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
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.net.Node;
import org.apache.hadoop.net.TopologyCache;
import org.apache.hadoop.util.CoronaSerializer;
import org.apache.hadoop.util.HostsFileReader;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.JsonToken;

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

    /** The lookup table of requested node for host */
    protected ConcurrentMap<String, RequestedNode> hostToRequestedNode =
      new ConcurrentHashMap<String, RequestedNode>();

    /** The lookup table of runnable nodes on hosts */
    protected ConcurrentMap<String, NodeContainer> hostToRunnableNodes =
      new ConcurrentHashMap<String, NodeContainer>();

    /** The lookup table of runnable nodes in racks */
    protected ConcurrentMap<Node, NodeContainer> rackToRunnableNodes
      = new ConcurrentHashMap<Node, NodeContainer>();

    /** Number of nodes that are still runnable */
    private AtomicInteger hostsWithRunnableNodes = new AtomicInteger(0);

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
      double avgLoad = loadManager.getAverageLoad(type);
      // Make two passes over the nodes. In the first pass, try to find a
      // node that has lower than average number of grants on it. If that does
      // not find a node, try looking at all nodes.
      for (int pass = 0; pass < 2; pass++) {
        for (Map.Entry<String, NodeContainer> e :
          hostToRunnableNodes.entrySet()) {
          NodeContainer nodeContainer = e.getValue();
          if (nodeContainer == null) {
            continue;
          }
          synchronized (nodeContainer) {
            if (nodeContainer.isEmpty()) {
              continue;
            }
            for (ClusterNode node : nodeContainer) {
              if (excluded == null || !excluded.contains(node.getHost())) {
                if (resourceLimit.hasEnoughResource(node)) {
                  // When pass == 0, try to average out the load.
                  if (pass == 0) {
                    if (node.getGrantCount(type) < avgLoad) {
                      return node;
                    }
                  } else {
                    return node;
                  }
                }
              }
            }
          }
        }
      }
      return null;
    }

    /**
     * Get runnable node local to the given host
     * @param requestedNode the requested node that needs local scheduling
     * @return the node that is local to the host, null if
     * there are no runnable nodes local to the host
     */
    public ClusterNode getRunnableNodeForHost(RequestedNode requestedNode) {
      // there should only be one node per host in the common case
      NodeContainer nodeContainer = requestedNode.getHostNodes();
      if (nodeContainer == null) {
        return null;
      }
      synchronized (nodeContainer) {
        if (nodeContainer.isEmpty()) {
          return null;
        }
        for (ClusterNode node : nodeContainer) {
          if (resourceLimit.hasEnoughResource(node)) {
            return node;
          }
        }
      }
      return null;

    }

    /**
     * Get a runnable node in the given rack that is not present in the
     * excluded list
     * @param requestedNode the node to look up rack locality for
     * @param excluded the list of nodes to ignore
     * @return the runnable node from the rack satisfying conditions, null if
     * the node was not found
     */
    public ClusterNode getRunnableNodeForRack(
      RequestedNode requestedNode, Set<String> excluded) {

      NodeContainer nodeContainer = requestedNode.getRackNodes();
      getRunnableNodeForRackCounter += 1;
      if (nodeContainer == null) {
        return null;
      }
      synchronized (nodeContainer) {
        if (nodeContainer.isEmpty()) {
          return null;
        }
        if (getRunnableNodeForRackCounter % RACK_SHUFFLE_PERIOD == 0) {
          // This balances more evenly across nodes in a rack
          nodeContainer.shuffle();
        }
        for (ClusterNode node : nodeContainer) {
          if (excluded == null || !excluded.contains(node.getHost())) {
            if (resourceLimit.hasEnoughResource(node)) {
              return node;
            }
          }
        }
      }
      return null;

    }

    /**
     * Check if there are any runnable nodes
     * @return true if there are any runnable nodes, false otherwise
     */
    public boolean existRunnableNodes() {
      return hostsWithRunnableNodes.get() > 0;
    }

    /**
     * Return an existing NodeContainer representing the node or if it
     * does not exist - create a new NodeContainer and return it.
     *
     * @param host the host to get the node container for
     * @return the node container representing this host
     */
    private NodeContainer getOrCreateHostRunnableNode(String host) {
      NodeContainer nodeContainer = hostToRunnableNodes.get(host);
      if (nodeContainer == null) {
        nodeContainer = new NodeContainer();
        NodeContainer oldList =
            hostToRunnableNodes.putIfAbsent(host, nodeContainer);
        if (oldList != null) {
          nodeContainer = oldList;
        }
      }
      return nodeContainer;
    }

    /**
     * Return an existing NodeContainer representing the rack or if it
     * does not exist - create a new NodeContainer and return it.
     *
     * @param rack the rack to return the node container for
     * @return the node container representing the rack
     */
    private NodeContainer getOrCreateRackRunnableNode(Node rack) {
      NodeContainer nodeContainer = rackToRunnableNodes.get(rack);
      if (nodeContainer == null) {
        nodeContainer = new NodeContainer();
        NodeContainer oldList =
            rackToRunnableNodes.putIfAbsent(rack, nodeContainer);
        if (oldList != null) {
          nodeContainer = oldList;
        }
      }
      return nodeContainer;
    }

    /**
     * Return a RequestedNode for a given host.
     * Returns a RequestedNode representing a given host by either getting
     * and existing RequestedNode or creating a new one.
     *
     * @param host the host to get the RequestedNode for
     * @return the RequestedNode object representing the host
     */
    private RequestedNode getOrCreateRequestedNode(String host) {
      RequestedNode node = hostToRequestedNode.get(host);
      if (node == null) {
        NodeContainer nodeRunnables = getOrCreateHostRunnableNode(host);
        Node rack = topologyCache.getNode(host).getParent();
        NodeContainer rackRunnables = getOrCreateRackRunnableNode(rack);
        node = new RequestedNode(
          type, host, rack, nodeRunnables, rackRunnables);
        RequestedNode oldNode = hostToRequestedNode.putIfAbsent(host, node);
        if (oldNode != null) {
          node = oldNode;
        }
      }
      return node;
    }

    /**
     * Add a node to the runnable indices
     * @param clusterNode the node to add
     */
    public void addRunnable(ClusterNode clusterNode) {
      String host = clusterNode.getHost();

      if (LOG.isDebugEnabled()) {
        LOG.debug(clusterNode.getName() +
            " added to runnable list for type: " + type);
      }


      NodeContainer nodeContainer = getOrCreateHostRunnableNode(host);
      synchronized (nodeContainer) {
        nodeContainer.addNode(clusterNode);
        hostsWithRunnableNodes.incrementAndGet();
      }

      Node rack = clusterNode.hostNode.getParent();
      nodeContainer = getOrCreateRackRunnableNode(rack);
      synchronized (nodeContainer) {
        nodeContainer.addNode(clusterNode);
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


      NodeContainer nodeContainer = hostToRunnableNodes.get(host);
      if (nodeContainer != null) {
        synchronized (nodeContainer) {
          if (nodeContainer.removeNode(node)) {
            /**
             * We are not removing the nodeContainer from runnable nodes map
             * since we are synchronizing operations with runnable indices
             * on it
             */
            hostsWithRunnableNodes.decrementAndGet();
          }
        }
      }


      Node rack = node.hostNode.getParent();

      nodeContainer = rackToRunnableNodes.get(rack);
      if (nodeContainer != null) {
        synchronized (nodeContainer) {
          /**
           * We are not removing the nodeContainer from runnable nodes map
           * since we are synchronizing operations with runnable indices
           * on it
           */
          nodeContainer.removeNode(node);
        }
      }
    }

    /**
     * Checks if a node is present as runnable in this index. Should be called
     * while holding the node lock.
     * @param clusterNode The node.
     * @return A boolean indicating if the node is present.
     */
    public boolean hasRunnable(ClusterNode clusterNode) {
      String host = clusterNode.getHost();
      NodeContainer nodeContainer = hostToRunnableNodes.get(host);
      return (nodeContainer != null) && !nodeContainer.isEmpty();
    }

    /**
     * Create a snapshot of runnable nodes.
     * @return The snapshot.
     */
    public NodeSnapshot getNodeSnapshot() {
      int nodeCount = 0;
      Map<String, NodeContainer> hostRunnables =
        new HashMap<String, NodeContainer>();
      for (Map.Entry<String, NodeContainer> entry :
        hostToRunnableNodes.entrySet()) {
        NodeContainer value = entry.getValue();
        synchronized (value) {
          if (!value.isEmpty()) {
            hostRunnables.put(entry.getKey(), value.copy());
            nodeCount += value.size();
          }
        }
      }
      Map<Node, NodeContainer> rackRunnables =
        new HashMap<Node, NodeContainer>();
      for (Map.Entry<Node, NodeContainer> entry :
        rackToRunnableNodes.entrySet()) {
        NodeContainer value = entry.getValue();
        synchronized (value) {
          if (!value.isEmpty()) {
            rackRunnables.put(entry.getKey(), value.copy());
          }
        }
      }
      return new NodeSnapshot(
        topologyCache, hostRunnables, rackRunnables, nodeCount);
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

  /** Track the load on nodes. */
  protected LoadManager loadManager;

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

  /** Resource limits. */
  private final ResourceLimit resourceLimit = new ResourceLimit();

  /** Hosts reader. */
  private final HostsFileReader hostsReader;


  /**
   * NodeManager constructor given a cluster manager and a
   * {@link HostsFileReader} for includes/excludes lists
   * @param clusterManager the cluster manager
   * @param hostsReader the host reader for includes/excludes
   */
  public NodeManager(
    ClusterManager clusterManager, HostsFileReader hostsReader) {
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
   * Constructor for the NodeManager, used when reading back the state of
   * NodeManager from disk.
   * @param clusterManager The ClusterManager instance
   * @param hostsReader The HostsReader instance
   * @param coronaSerializer The CoronaSerializer instance, which will be used
   *                         to read JSON from disk
   * @throws IOException
   */
  public NodeManager(ClusterManager clusterManager,
                     HostsFileReader hostsReader,
                     CoronaSerializer coronaSerializer)
    throws IOException {
    this(clusterManager, hostsReader);

    // Expecting the START_OBJECT token for nodeManager
    coronaSerializer.readStartObjectToken("nodeManager");
    readNameToNode(coronaSerializer);
    readHostsToSessions(coronaSerializer);
    readNameToApps(coronaSerializer);
    // Expecting the END_OBJECT token for ClusterManager
    coronaSerializer.readEndObjectToken("nodeManager");

    // topologyCache need not be serialized, it will eventually be rebuilt.
    // cpuToResourcePartitioning and resourceLimit need not be serialized,
    // they can be read from the conf.
  }

  /**
   * Reads the nameToNode map from the JSON stream
   * @param coronaSerializer The CoronaSerializer instance to be used to
   *                         read the JSON
   * @throws IOException
   */
  private void readNameToNode(CoronaSerializer coronaSerializer)
    throws IOException {
    coronaSerializer.readField("nameToNode");
    // Expecting the START_OBJECT token for nameToNode
    coronaSerializer.readStartObjectToken("nameToNode");
    JsonToken current = coronaSerializer.nextToken();
    while (current != JsonToken.END_OBJECT) {
      // nodeName is the key, and the ClusterNode is the value here
      String nodeName = coronaSerializer.getFieldName();
      ClusterNode clusterNode = new ClusterNode(coronaSerializer);
      if (!nameToNode.containsKey(nodeName)) {
        nameToNode.put(nodeName, clusterNode);
      }
      current = coronaSerializer.nextToken();
    }
    // Done with reading the END_OBJECT token for nameToNode
  }

  /**
   * Reads the hostsToSessions map from the JSON stream
   * @param coronaSerializer The CoronaSerializer instance to be used to
   *                         read the JSON
   * @throws java.io.IOException
   */
  private void readHostsToSessions(CoronaSerializer coronaSerializer)
    throws IOException {
    coronaSerializer.readField("hostsToSessions");
    // Expecting the START_OBJECT token for hostsToSessions
    coronaSerializer.readStartObjectToken("hostsToSessions");
    JsonToken current = coronaSerializer.nextToken();

    while (current != JsonToken.END_OBJECT) {
      String host = coronaSerializer.getFieldName();
      Set<String> sessionsSet = coronaSerializer.readValueAs(Set.class);
      hostsToSessions.put(nameToNode.get(host), sessionsSet);
      current = coronaSerializer.nextToken();
    }
  }

  /**
   * Reads the nameToApps map from the JSON stream
   * @param coronaSerializer The CoronaSerializer instance to be used to
   *                         read the JSON
   * @throws IOException
   */
  private void readNameToApps(CoronaSerializer coronaSerializer)
    throws IOException {
    coronaSerializer.readField("nameToApps");
    // Expecting the START_OBJECT token for nameToApps
    coronaSerializer.readStartObjectToken("nameToApps");
    JsonToken current = coronaSerializer.nextToken();

    while (current != JsonToken.END_OBJECT) {
      String nodeName = coronaSerializer.getFieldName();
      // Expecting the START_OBJECT token for the Apps
      coronaSerializer.readStartObjectToken(nodeName);
      Map<String, String> appMap = coronaSerializer.readValueAs(Map.class);
      Map<ResourceType, String> appsOnNode =
        new HashMap<ResourceType, String>();

      for (Map.Entry<String, String> entry : appMap.entrySet()) {
        appsOnNode.put(ResourceType.valueOf(entry.getKey()),
          entry.getValue());
      }

      nameToApps.put(nodeName, appsOnNode);
      current = coronaSerializer.nextToken();
    }
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
   * Create node snapshot of runnable nodes of a certain type.
   * @param type The resource type
   * @return The snapshot
   */
  public NodeSnapshot getNodeSnapshot(ResourceType type) {
    return typeToIndices.get(type).getNodeSnapshot();
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
    if (host == null) {
      RunnableIndices r = typeToIndices.get(type);
      return r.getRunnableNodeForAny(excluded);
    }
    RequestedNode node = resolve(host, type);
    return getRunnableNode(node, maxLevel, type, excluded);
  }

  /**
   * Get a runnable node.
   * @param requestedNode The request information.
   * @param maxLevel The maximum locality level that we can go to.
   * @param type The type of resource.
   * @param excluded The excluded nodes.
   * @return The runnable node that can be used.
   */
  public ClusterNode getRunnableNode(RequestedNode requestedNode,
                                     LocalityLevel maxLevel,
                                     ResourceType type,
                                     Set<String> excluded) {
    ClusterNode node = null;
    RunnableIndices r = typeToIndices.get(type);

    // find host local
    node = r.getRunnableNodeForHost(requestedNode);

    if (maxLevel == LocalityLevel.NODE || node != null) {
      return node;
    }
    node = r.getRunnableNodeForRack(requestedNode, excluded);

    if (maxLevel == LocalityLevel.RACK || node != null) {
      return node;
    }

    // find any node
    node = r.getRunnableNodeForAny(excluded);

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
      clusterManager.getMetrics().restartTaskTracker(1);
      setAliveDeadMetrics();

      // 2: update runnable indices
      for (Map.Entry<ResourceType, RunnableIndices> entry :
          typeToIndices.entrySet()) {
        ResourceType type = entry.getKey();
        if (resourceInfos.containsKey(type)) {
          if (node.checkForGrant(Utilities.getUnitResourceRequest(type),
                                 resourceLimit)) {
            RunnableIndices r = entry.getValue();
            r.addRunnable(node);
          }
        }
      }
    }
  }

  /**
   * Update the runnable status of a node based on resources available.
   * This checks both resources and slot availability.
   * @param node The node
   */
  private void updateRunnability(ClusterNode node) {
    synchronized (node) {
      for (Map.Entry<ResourceType, RunnableIndices> entry :
        typeToIndices.entrySet()) {
        ResourceType type = entry.getKey();
        RunnableIndices r = entry.getValue();
        ResourceRequest unitReq = Utilities.getUnitResourceRequest(type);
        boolean currentlyRunnable = r.hasRunnable(node);
        boolean shouldBeRunnable = node.checkForGrant(unitReq, resourceLimit);
        if (currentlyRunnable && !shouldBeRunnable) {
          LOG.info("Node " + node.getName() + " is no longer " +
            type + " runnable");
          r.deleteRunnable(node);
        } else if (!currentlyRunnable && shouldBeRunnable) {
          LOG.info("Node " + node.getName() + " is now " + type + " runnable");
          r.addRunnable(node);
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
          if (node.checkForGrant(Utilities.getUnitResourceRequest(type),
                                  resourceLimit)) {
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
      String hoststr = node.getClusterNodeInfo().getAddress().getHost();
      if (!canAllowNode(hoststr)) {
        LOG.warn("Canceling grant for excluded node: " + hoststr);
        return;
      }
      ResourceRequestInfo req = node.getRequestForGrant(sessionId, requestId);
      if (req != null) {
        ResourceRequest unitReq = Utilities.getUnitResourceRequest(
          req.getType());
        boolean previouslyRunnable = node.checkForGrant(unitReq, resourceLimit);
        node.cancelGrant(sessionId, requestId);
        loadManager.decrementLoad(req.getType());
        if (!previouslyRunnable && node.checkForGrant(unitReq, resourceLimit)) {
          RunnableIndices r = typeToIndices.get(req.getType());
          if (!faultManager.isBlacklisted(node.getName(), req.getType())) {
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
      ClusterNode node, String sessionId, ResourceRequestInfo req) {
    synchronized (node) {
      if (node.deleted) {
        return false;
      }
      if (!node.checkForGrant(Utilities.getUnitResourceRequest(
        req.getType()), resourceLimit)) {
        return false;
      }

      node.addGrant(sessionId, req);
      loadManager.incrementLoad(req.getType());
      hostsToSessions.get(node).add(sessionId);
      if (!node.checkForGrant(Utilities.getUnitResourceRequest(
        req.getType()), resourceLimit)) {
        RunnableIndices r = typeToIndices.get(req.getType());
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

    loadManager = new LoadManager(this);
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
    resourceLimit.setConf(conf);

    faultManager.setConf(conf);
  }

  /**
   *  This method rebuilds members related to the NodeManager instance, which
   *  were not directly persisted themselves.
   *  @throws IOException
   */
  public void restoreAfterSafeModeRestart() throws IOException {
    if (!clusterManager.safeMode) {
      throw new IOException("restoreAfterSafeModeRestart() called while the " +
        "Cluster Manager was not in Safe Mode");
    }
    // Restoring all the ClusterNode(s)
    for (ClusterNode clusterNode : nameToNode.values()) {
      restoreClusterNode(clusterNode);
    }

    // Restoring all the RequestedNodes(s)
    for (ClusterNode clusterNode : nameToNode.values()) {
      for (ResourceRequestInfo resourceRequestInfo :
        clusterNode.grants.values()) {
        // Fix the RequestedNode(s)
        restoreResourceRequestInfo(resourceRequestInfo);
        loadManager.incrementLoad(resourceRequestInfo.getType());
      }
    }
  }

  /**
   * This method rebuilds members related to a ResourceRequestInfo instance,
   * which were not directly persisted themselves.
   * @param resourceRequestInfo The ResourceRequestInfo instance to be restored
   */
  public void restoreResourceRequestInfo(ResourceRequestInfo
                                           resourceRequestInfo) {
    List<RequestedNode> requestedNodes = null;
    List<String> hosts = resourceRequestInfo.getHosts();
    if (hosts != null && hosts.size() > 0) {
      requestedNodes = new ArrayList<RequestedNode>(hosts.size());
      for (String host : hosts) {
        requestedNodes.add(resolve(host, resourceRequestInfo.getType()));
      }
    }
    resourceRequestInfo.nodes = requestedNodes;
  }

  private void restoreClusterNode(ClusterNode clusterNode) {
    clusterNode.hostNode = topologyCache.getNode(clusterNode.getHost());
    // This will reset the lastHeartbeatTime
    clusterNode.heartbeat(clusterNode.getClusterNodeInfo());
    clusterNode.initResourceTypeToMaxCpuMap(cpuToResourcePartitioning);
    updateRunnability(clusterNode);
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
    ClusterNode node = nameToNode.get(clusterNodeInfo.name);
    if (!canAllowNode(clusterNodeInfo.getAddress().getHost())) {
      if (node != null) {
        node.heartbeat(clusterNodeInfo);
      } else {
        throw new DisallowedNode(clusterNodeInfo.getAddress().getHost());
      }
      return false;
    }
    boolean newNode = false;
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

    node.heartbeat(clusterNodeInfo);

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

    updateRunnability(node);
    return newNode || appsChanged;
  }

  /**
   * Get information about applications running on a node.
   * @param node The node.
   * @param type The type of resources.
   * @return The application-specific information
   */
  public String getAppInfo(ClusterNode node, ResourceType type) {
    Map<ResourceType, String> resourceInfos = nameToApps.get(node.getName());
    if (resourceInfos == null) {
      return null;
    } else {
      return resourceInfos.get(type);
    }
  }

  /**
   * Check if a node has enough resources.
   * @param node The node
   * @return A boolean indicating if it has enough resources.
   */
  public boolean hasEnoughResource(ClusterNode node) {
    return resourceLimit.hasEnoughResource(node);
  }

  /**
   * Expires dead nodes.
   */
  class ExpireNodes implements Runnable {

    @Override
    public void run() {
      while (!shutdown) {
        try {
          Thread.sleep(nodeExpiryInterval / 2);

          if (clusterManager.safeMode) {
            // Do nothing but sleep
            continue;
          }

          long now = ClusterManager.clock.getTime();
          for (ClusterNode node : nameToNode.values()) {
            if (now - node.lastHeartbeatTime > nodeExpiryInterval) {
              LOG.warn("Timing out node: " + node.getName());
              clusterManager.nodeTimeout(node.getName());
            }
          }

        } catch (InterruptedException iex) {
          // ignore. if shutting down, while cond. will catch it
          continue;
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

  /**
   * Find capacity for a resource type.
   * @param type The resource type.
   * @return The capacity.
   */
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

  /**
   * Find allocation for a resource type.
   * @param type The resource type.
   * @return The allocation.
   */
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

  /**
   * Get a list nodes with free Cpu for a resource type
   */
  public List<String> getFreeNodesForType(ResourceType type) {
    ArrayList<String> freeNodes = new ArrayList<String>();
    for (Map.Entry<String, ClusterNode> entry: nameToNode.entrySet()) {
      ClusterNode node = entry.getValue();
      synchronized (node) {
        if (!node.deleted &&
            node.getMaxCpuForType(type) > node.getAllocatedCpuForType(type)) {
          freeNodes.add(entry.getKey() + ": " + node.getFree().toString());
        }
      }
    }
    return freeNodes;
  }

  /**
   * @return The total number of configured hosts.
   */
  public int getTotalNodeCount() {
    return hostsReader.getHosts().size();
  }

  /**
   * @return All the configured hosts.
   */
  public Set<String> getAllNodes() {
    return hostsReader.getHostNames();
  }

  /**
   * @return The number of excluded hosts.
   */
  public int getExcludedNodeCount() {
    return hostsReader.getExcludedHosts().size();
  }

  /**
   * @return The excluded hosts.
   */
  public Set<String> getExcludedNodes() {
    return hostsReader.getExcludedHosts();
  }

  /**
   * @return The number of alive nodes.
   */
  public int getAliveNodeCount() {
    return nameToNode.size();
  }

  /**
   * @return The alive nodes.
   */
  public List<String> getAliveNodes() {
    return new ArrayList<String>(nameToNode.keySet());
  }

  /**
   * @return The alive nodes.
   */
  public List<ClusterNode> getAliveClusterNodes() {
    return new ArrayList<ClusterNode>(nameToNode.values());
  }


  /**
   * @return The fault manager.
   */
  public FaultManager getFaultManager() {
    return faultManager;
  }

  /**
   * Refresh the includes/excludes information.
   * @throws IOException
   */
  public synchronized void refreshNodes() throws IOException {
    hostsReader.refresh();
    LOG.info("After refresh Included hosts: " +
        hostsReader.getHostNames().size() +
        " Excluded hosts: " + hostsReader.getExcludedHosts().size());
    Set<String> newHosts = hostsReader.getHostNames();
    Set<String> newExcludes = hostsReader.getExcludedHosts();
    Set<ClusterNode> hostsToExclude = new HashSet<ClusterNode>();
    for (ClusterNode tmpNode : nameToNode.values()) {
      String host = tmpNode.getHost();
      // Check if not included or explicitly excluded.
      if (!newHosts.contains(host) || newExcludes.contains(host)) {
        hostsToExclude.add(tmpNode);
      }
    }
    for (ClusterNode node: hostsToExclude) {
      synchronized (node) {
        for (Map.Entry<ResourceType, RunnableIndices> entry :
          typeToIndices.entrySet()) {
          ResourceType type = entry.getKey();
          RunnableIndices r = entry.getValue();
          if (r.hasRunnable(node)) {
            LOG.info("Node " + node.getName() + " is no longer " +
              type + " runnable because it is excluded");
            r.deleteRunnable(node);
          }
        }
      }
    }
  }

  /**
   * Process feedback about nodes.
   * @param handle The session handle.
   * @param resourceTypes The types of resource this feedback is about.
   * @param reportList The list of reports.
   */
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

  /**
   * Blacklist a resource on a node.
   * @param nodeName The node name
   * @param resourceType The resource type.
   */
  void blacklistNode(String nodeName, ResourceType resourceType) {
    LOG.info("Node " + nodeName + " has been blacklisted for resource " +
      resourceType);
    clusterManager.getMetrics().setBlacklistedNodes(
        faultManager.getBlacklistedNodeCount());
    deleteAppFromNode(nodeName, resourceType);
  }

  /**
   * Checks if a host is allowed to communicate with the cluster manager.
   *
   * @param host
   *          The host
   * @return a boolean indicating if the host is allowed.
   */
  private boolean canAllowNode(String host) {
    return hostsReader.isAllowedHost(host);
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

  /**
   * Resolve a host name.
   * @param host The host.
   * @param type The resource type.
   * @return The resolved form.
   */
  public RequestedNode resolve(String host, ResourceType type) {
    RunnableIndices indices = typeToIndices.get(type);
    return indices.getOrCreateRequestedNode(host);
  }

  public ResourceLimit getResourceLimit() {
    return resourceLimit;
  }

  /**
   * This is required when we come out of safe mode, and we need to reset
   * the lastHeartbeatTime for each node
   */
  public void resetNodesLastHeartbeatTime() {
    long now = ClusterManager.clock.getTime();
    for (ClusterNode node : nameToNode.values()) {
      node.lastHeartbeatTime = now;
    }
  }

  /**
   * This method writes the state of the NodeManager to disk
   * @param jsonGenerator The instance of JsonGenerator, which will be used to
   *                      write JSON to disk
   * @throws IOException
   */
  public void write(JsonGenerator jsonGenerator) throws IOException {
    jsonGenerator.writeStartObject();

    // nameToNode begins
    jsonGenerator.writeFieldName("nameToNode");
    jsonGenerator.writeStartObject();
    for (Map.Entry<String, ClusterNode> entry : nameToNode.entrySet()) {
      jsonGenerator.writeFieldName(entry.getKey());
      entry.getValue().write(jsonGenerator);
    }
    jsonGenerator.writeEndObject();
    // nameToNode ends

    // hostsToSessions begins
    // We create a new Map of type <ClusterNode.name, Set<SessionIds>>.
    // The original hostsToSessions map has the ClusterNode as its key, and
    // we do not need to persist the entire ClusterNode again, since we have
    // already done that with nameToNode.
    Map<String, Set<String>> hostsToSessionsMap =
      new HashMap<String, Set<String>>();
    for (Map.Entry<ClusterNode, Set<String>> entry :
      hostsToSessions.entrySet()) {
      hostsToSessionsMap.put(entry.getKey().getName(),
        entry.getValue());
    }
    jsonGenerator.writeObjectField("hostsToSessions", hostsToSessionsMap);
    // hostsToSessions ends

    jsonGenerator.writeObjectField("nameToApps", nameToApps);

    // faultManager is not required

    // We can rebuild the loadManager
    jsonGenerator.writeEndObject();
  }
}
