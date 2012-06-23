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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Check resource limits.
 */
public class ResourceLimit {
  /** Class logger */
  public static final Log LOG = LogFactory.getLog(ResourceLimit.class);

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
   * Modify the reserved memory limit.
   * @param mb The reserved memory in megabytes.
   */
  public void setNodeReservedMemoryMB(int mb) {
    LOG.info("nodeReservedMemoryMB changed" +
      " from " + nodeReservedMemoryMB +
      " to " + mb);
    this.nodeReservedMemoryMB = mb;
  }

  /**
   * Modify the reserved disk limit.
   * @param gb The reserved disk space in gigabytes.
   */
  public void setNodeReservedDiskGB(int gb) {
    LOG.info("nodeReservedDiskGB changed" +
      " from " + nodeReservedDiskGB +
      " to " + gb);
    this.nodeReservedDiskGB = gb;
  }

  /**
   * Check if the node has enough resources to run tasks
   * @param node node to check
   * @return true if the node has enough resources, false otherwise
   */
  public boolean hasEnoughResource(ClusterNode node) {
    return hasEnoughMemory(node) && hasEnoughDiskSpace(node);
  }

  /**
   * Check if the node has enough memory to run tasks
   * @param node node to check
   * @return true if the node has enough memory, false otherwise
   */
  public boolean hasEnoughMemory(ClusterNode node) {
    int total = node.getTotal().memoryMB;
    int free = node.getFree().memoryMB;
    if (free < nodeReservedMemoryMB) {
      if (LOG.isDebugEnabled()) {
        LOG.debug(node.getHost() + " not enough memory." +
          " totalMB:" + total +
          " free:" + free +
          " limit:" + nodeReservedMemoryMB);
      }
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
    int total = node.getTotal().diskGB;
    int free = node.getFree().diskGB;
    if (free < nodeReservedDiskGB) {
      if (LOG.isDebugEnabled()) {
        LOG.debug(node.getHost() + " not enough disk space." +
          " totalMB:" + total +
          " free:" + free +
          " limit:" + nodeReservedDiskGB);
      }
      return false;
    }
    return true;
  }

  /**
   * Modify the limits based on new configuration.
   * @param conf The corona configuration.
   */
  public void setConf(CoronaConf conf) {
    nodeReservedMemoryMB = conf.getNodeReservedMemoryMB();
    nodeReservedDiskGB = conf.getNodeReservedDiskGB();
  }
}
