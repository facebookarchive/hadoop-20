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

import org.apache.hadoop.net.Node;
import org.apache.hadoop.net.TopologyCache;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Respresents a snapshot of runnable nodes.
 * This class is not thread-safe.
 */
public class NodeSnapshot {

  /** Topology cache. */
  private TopologyCache topologyCache;

  /** The lookup table of runnable nodes on hosts */
  private Map<String, NodeContainer> hostToRunnableNode =
    new HashMap<String, NodeContainer>();

  /** The lookup table of runnable nodes in racks */
  private Map<Node, NodeContainer> rackToRunnableNode =
    new HashMap<Node, NodeContainer>();

  /** Number of runnable nodes in the snapshot. */
  private int runnableNodeCount;

  /**
   * Constructor
   * @param topologyCache The topology of the cluster.
   * @param hostToRunnableNode The runnable nodes on a hosts
   * @param rackToRunnableNode The runnable nodes on racks
   * @param runnableNodeCount The runnable node count
   */
  public NodeSnapshot(
    TopologyCache topologyCache,
    Map<String, NodeContainer> hostToRunnableNode,
    Map<Node, NodeContainer> rackToRunnableNode,
    int runnableNodeCount) {
    this.topologyCache = topologyCache;
    this.hostToRunnableNode = hostToRunnableNode;
    this.rackToRunnableNode = rackToRunnableNode;
    this.runnableNodeCount = runnableNodeCount;
  }

  /**
   * @return The hosts with runnable nodes.
   */
  public Set<Map.Entry<String, NodeContainer>> runnableHosts() {
    return hostToRunnableNode.entrySet();
  }

  /**
   * @return The racks with runnable nodes.
   */
  public Set<Map.Entry<Node, NodeContainer>> runnableRacks() {
    return rackToRunnableNode.entrySet();
  }

  /**
   * Remove a node from the snapshot.
   * @param node The node.
   */
  public void removeNode(ClusterNode node) {
    String host = node.getHost();
    NodeContainer container = hostToRunnableNode.get(host);
    if (container != null) {
      if (container.removeNode(node)) {
        runnableNodeCount--;
      }
      if (container.isEmpty()) {
        hostToRunnableNode.remove(host);
      }
    }
    Node rack = topologyCache.getNode(host).getParent();
    container = rackToRunnableNode.get(rack);
    if (container != null) {
      container.removeNode(node);
      if (container.isEmpty()) {
        rackToRunnableNode.remove(rack);
      }
    }
  }

  /**
   * @return The number of runnable hosts.
   */
  public int getRunnableHostCount() {
    return hostToRunnableNode.size();
  }
}
