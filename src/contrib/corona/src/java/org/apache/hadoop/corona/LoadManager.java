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
import java.util.Map;

/**
 * This class tracks the load on the nodes.
 */
public class LoadManager {
  /** Track the load (running count) of each resource type. */
  private Map<ResourceType, Integer> typeToTotalLoad =
    new EnumMap<ResourceType, Integer>(ResourceType.class);

  /** The node manager. */
  private final NodeManager nodeManager;

  public LoadManager(NodeManager nodeManager) {
    this.nodeManager = nodeManager;
    for (ResourceType type: ResourceType.values()) {
      typeToTotalLoad.put(type, 0);
    }
  }

  /**
   * Computes average number of running resources of given type on nodes.
   * @param type the type of the resource
   * @return the average
   */
  public synchronized double getAverageLoad(ResourceType type) {
    return (typeToTotalLoad.get(type) * 1.0d) / nodeManager.getAliveNodeCount();
  }

  /**
   * Increment the number of running resources of a given type.
   * @param type the resource type.
   */
  public synchronized void incrementLoad(ResourceType type) {
    Integer load = typeToTotalLoad.get(type);
    assert(load != null);
    typeToTotalLoad.put(type, load + 1);
  }

  /**
   * Decrement the number of running resources of a given type.
   * @param type the resource type.
   */
  public synchronized void decrementLoad(ResourceType type) {
    Integer load = typeToTotalLoad.get(type);
    assert (load != null && load > 0);
    typeToTotalLoad.put(type, load - 1);
  }
}
