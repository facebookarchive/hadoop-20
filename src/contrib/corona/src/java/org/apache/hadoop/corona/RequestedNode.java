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

/**
 * RequestedNode is a class that fully represents a request for a
 * resource on a node.
 * It has containers for the runnable nodes that are local and rack local
 * to the host requested.
 */
public class RequestedNode {
  /** The type of the resource */
  private ResourceType type;
  /** The hostname of the node requested*/
  private String host;
  /** The rack of node. */
  private Node rack;
  /** The container of the local cluster nodes */
  private NodeContainer hostNodes;
  /** The container of the rack local cluster nodes */
  private NodeContainer rackNodes;

  /**
   * Create a wrapper for the node
   *
   * @param type the type of the resource
   * @param host the hostname of the node
   * @param rack the rack of the node.
   * @param hostNodes the node container for the host of request
   * @param rackNodes the node container for the rack of the request
   */
  public RequestedNode(ResourceType type,
                       String host,
                       Node rack,
                       NodeContainer hostNodes,
                       NodeContainer rackNodes) {
    this.type = type;
    this.host = host;
    this.rack = rack;
    this.hostNodes = hostNodes;
    this.rackNodes = rackNodes;
  }

  public NodeContainer getHostNodes() {
    return hostNodes;
  }

  public NodeContainer getRackNodes() {
    return rackNodes;
  }

  public String getHost() {
    return host;
  }

  public Node getRack() {
    return rack;
  }
}
