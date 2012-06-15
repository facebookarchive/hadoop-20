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
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Aggregates reports from sessions and decides if a node should be blacklisted.
 */
public class FaultManager {
  /** Logger. */
  private static final Log LOG = LogFactory.getLog(FaultManager.class);

  /** Configuration. */
  private CoronaConf conf;

  /** Reference to the Node Manager. */
  private final NodeManager nm;

  /** Map of nodeName -> list of fault statistics. */
  private final ConcurrentMap<String, List<FaultStatsForType>>
  nodeToFaultStats = new ConcurrentHashMap<String, List<FaultStatsForType>>();

  /** Lookup for blacklisted nodes: Node Name -> Resource Types */
  private final ConcurrentMap<String, List<ResourceType>> blacklistedNodes =
      new ConcurrentHashMap<String, List<ResourceType>>();

  /** Fault Statistics for a resource type. */
  public static class FaultStatsForType {
    /** Type of resource. */
    private final ResourceType type;
    /** Number of sessions with failed connections to the node. */
    private int numSessionsWithFailedConnections;
    /** Number of sessions that saw too many failures on the node. */
    private int numSessionsWithTooManyFailures;

    /**
     * Constructor.
     *
     * @param type
     *          The type of the resource.
     */
    public FaultStatsForType(ResourceType type) {
      this.type = type;
    }

    public ResourceType getType() {
      return type;
    }

    public int getNumSessionsWithFailedConnections() {
      return numSessionsWithFailedConnections;
    }

    public void setNumSessionsWithFailedConnections(int val) {
      numSessionsWithFailedConnections = val;
    }

    public int getNumSessionsWithTooManyFailures() {
      return numSessionsWithTooManyFailures;
    }

    public void setNumSessionsWithTooManyFailures(int val) {
      numSessionsWithTooManyFailures = val;
    }
  }

  /**
   * Constructor.
   *
   * @param nm The {@link NodeManager} that is using this FaultManager.
   */
  public FaultManager(NodeManager nm) {
    this.nm = nm;
  }

  /**
   * Sets the configuration.
   *
   * @param conf The configuration.
   */
  public void setConf(CoronaConf conf) {
    this.conf = conf;
  }

  /**
   * Notify the fault manager of a new node.
   *
   * @param name The node name.
   * @param resourceTypes The types of resource on this node.
   */
  public void addNode(String name, Set<ResourceType> resourceTypes) {
    List<FaultStatsForType> faultStats = new ArrayList<FaultStatsForType>(
        resourceTypes.size());
    for (ResourceType type : resourceTypes) {
      faultStats.add(new FaultStatsForType(type));
    }
    nodeToFaultStats.put(name, faultStats);
  }

  /**
   * Notify the fault manager that a node has been deleted.
   *
   * @param name The node name.
   */
  public void deleteNode(String name) {
    nodeToFaultStats.remove(name);
    blacklistedNodes.remove(name);
  }

  /**
   * Provide the fault manager with new feedback about a node.
   *
   * @param nodeName The node name.
   * @param resourceTypes The types of resources used on the node.
   * @param usageReport The {@link NodeUsageReport} for this node.
   */
  public void nodeFeedback(String nodeName, List<ResourceType> resourceTypes,
      NodeUsageReport usageReport) {
    List<FaultStatsForType> faultStats = nodeToFaultStats.get(nodeName);
    if (faultStats == null) {
      LOG.info("Received node feedback for deleted node " + nodeName);
      return;
    }
    boolean statsModified = false;
    synchronized (faultStats) {
      if (tooManyFailedConnectionsInSession(usageReport)) {
        for (FaultStatsForType stat : faultStats) {
          if (resourceTypes.contains(stat.type)) {
            stat.numSessionsWithFailedConnections++;
            statsModified = true;
          }
        }
      }
      if (tooManyFailuresInSession(usageReport)) {
        for (FaultStatsForType stat : faultStats) {
          if (resourceTypes.contains(stat.type)) {
            stat.numSessionsWithTooManyFailures++;
            statsModified = true;
          }
        }
      }
    }

    if (statsModified) {
      blacklistIfNeeded(nodeName, faultStats);
    }
  }

  /**
   * Gets the fault statistics for a node.
   * @param nodeName The node name.
   * @return The list of fault statistics for the node, one element per type.
   */
  public List<FaultStatsForType> getFaultStats(String nodeName) {
    synchronized (nodeToFaultStats) {
      return nodeToFaultStats.get(nodeName);
    }
  }

  /**
   * Check if a resource on a node is blacklisted.
   *
   * @param nodeName The node name.
   * @param type The type of resource to check for blacklisting.
   * @return A boolean value that is true if blacklisted, false if not.
   */
  public boolean isBlacklisted(String nodeName, ResourceType type) {
    List<ResourceType> blacklistedResourceTypes =
      blacklistedNodes.get(nodeName);
    if (blacklistedResourceTypes != null) {
      synchronized (blacklistedResourceTypes) {
        return blacklistedResourceTypes.contains(type);
      }
    } else {
      return false;
    }
  }

  /**
   * Return the number of blacklisted nodes.
   * @return The number of blacklisted nodes.
   */
  public int getBlacklistedNodeCount() {
    return blacklistedNodes.size();
  }

  /**
   * Return the list of blacklisted nodes.
   * @return The list of blacklisted nodes.
   */
  public List<String> getBlacklistedNodes() {
    List<String> ret = new ArrayList<String>();
    for (String nodeName : blacklistedNodes.keySet()) {
      ret.add(nodeName);
    }
    return ret;
  }

  /**
   * Checks if a node needs to be blacklisted and blacklists it.
   * @param nodeName The node name.
   * @param faultStats The fault statistics for the node.
   */
  private void blacklistIfNeeded(
        String nodeName, List<FaultStatsForType> faultStats) {
    for (FaultStatsForType stat : faultStats) {
      if (isBlacklisted(nodeName, stat.type)) {
        continue;
      }
      if (tooManyFailuresOnNode(stat) ||
          tooManyConnectionFailuresOnNode(stat)) {
        nm.blacklistNode(nodeName, stat.type);
        blacklist(nodeName, stat.type);
      }
    }
  }

  /**
   * Blacklists a resource on a node.
   * @param nodeName The node name.
   * @param type The type of the resource.
   */
  private void blacklist(String nodeName, ResourceType type) {
    List<ResourceType> blacklistedResourceTypes =
      blacklistedNodes.get(nodeName);
    if (blacklistedResourceTypes == null) {
      blacklistedResourceTypes = new ArrayList<ResourceType>();
      blacklistedNodes.put(nodeName, blacklistedResourceTypes);
    }
    synchronized (blacklistedResourceTypes) {
      if (!blacklistedResourceTypes.contains(type)) {
        blacklistedResourceTypes.add(type);
      }
    }
  }

  /**
   * Checks if there have been too many failures for a session on a node.
   *
   * @param usageReport The usage report.
   * @return A boolean value indicating if there were too many failures.
   */
  private boolean tooManyFailuresInSession(NodeUsageReport usageReport) {
    return usageReport.getNumFailed() > conf.getMaxFailuresPerSession();
  }

  /**
   * Checks if there have been too many failed connection attempts for a
   * session on a node.
   *
   * @param usageReport The usage report to check.
   * @return A boolean value indicating if there were too failed connections.
   */
  private boolean tooManyFailedConnectionsInSession(
      NodeUsageReport usageReport) {
    return usageReport.getNumFailedConnections() >
              conf.getMaxFailedConnectionsPerSession();
  }

  /**
   * Checks if there have been too many failures on a node across sessions.
   * @param stat Failure stats for the node.
   * @return A boolean value indicating if there were too many failures.
   */
  private boolean tooManyConnectionFailuresOnNode(FaultStatsForType stat) {
    return stat.numSessionsWithFailedConnections >
                conf.getMaxFailedConnections();
  }

  /**
   * Checks if there have been too many failed connection attempts on a node
   * across sessions.
   *
   * @param stat Failure stats for the node.
   * @return A boolean value indicating if there were too many failures.
   */
  private boolean tooManyFailuresOnNode(FaultStatsForType stat) {
    return stat.numSessionsWithTooManyFailures > conf.getMaxFailures();
  }
}
