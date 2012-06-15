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

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.LinkedList;

/**
 * NodeContainer holds the list of ClusterNodes.
 * It can represent either a host that is running one or more ClusterNodes
 * or a rack which has hosts running ClusterNodes
 */
public class NodeContainer implements Iterable<ClusterNode> {
  /** The list of nodes */
  private final LinkedList<ClusterNode> nodeList;

  /** Private constructor.
   * @param nodeList the node list */
  private NodeContainer(List<ClusterNode> nodeList) {
    this.nodeList = new LinkedList<ClusterNode>(nodeList);
  }

  /** Constructor */
  public NodeContainer() {
    nodeList = new LinkedList<ClusterNode>();
  }

  /** Add a node to the container
   *
   * @param node the node to add
   */
  public void addNode(ClusterNode node) {
    nodeList.add(node);
  }

  /**
   * Remove a node from the container
   * @param node the node to remove
   * @return A boolean indicating if the node was removed.
   */
  public boolean removeNode(ClusterNode node) {
    return Utilities.removeReference(nodeList, node) != null;
  }

  @Override
  /**
   * Iterator.
   */
  public Iterator<ClusterNode> iterator() {
    return nodeList.iterator();
  }

  /**
   * Returns true if there are no nodes in the container
   * @return true if there are no nodes in the container
   */
  public boolean isEmpty() {
    return nodeList.isEmpty();
  }

  /**
   * @return The number of nodes.
   */
  public int size() {
    return nodeList.size();
  }

  /**
   * Shuffle the nodes so that the iteration
   * order is different
   */
  public void shuffle() {
    Collections.shuffle(nodeList);
  }

  /**
   * Copy function for snapshotting.
   * @return A copy of this node container
   */
  public NodeContainer copy() {
    return new NodeContainer(nodeList);
  }
}
