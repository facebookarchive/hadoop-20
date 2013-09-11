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
import java.util.HashMap;
import java.util.Map;

/**
 * Customized multikey map for ResourceType, PoolInfo to value.
 * Not thread-safe.
 *
 * @param <V> Value to map to.
 */
public class TypePoolInfoMap<V> {
  /** Internal map of map of map that stores everything */
  private final Map<ResourceType, Map<PoolInfo, V>> typePoolInfoMap =
      new EnumMap<ResourceType, Map<PoolInfo, V>>(ResourceType.class);

  /**
   * Put the value with the appropriate keys
   *
   * @param type Resource type
   * @param poolInfo Pool info
   * @param value Value to put
   * @return Previous value, can be null if wasn't set
   */
  public V put(ResourceType type, PoolInfo poolInfo, V value) {
    Map<PoolInfo, V> poolInfoMap = typePoolInfoMap.get(type);
    if (poolInfoMap == null) {
      poolInfoMap = new HashMap<PoolInfo, V>();
      typePoolInfoMap.put(type, poolInfoMap);
    }
    return poolInfoMap.put(poolInfo, value);
  }

  /**
   * Get the value with the appropriate keys
   *
   * @param type Resource type
   * @param poolInfo Pool info
   * @param value Value to put
   * @return Previous value, can be null if if wasn't set
   */
  public V get(ResourceType type, PoolInfo poolInfo) {
    Map<PoolInfo, V> poolInfoMap = typePoolInfoMap.get(type);
    if (poolInfoMap == null) {
      return null;
    }
    return poolInfoMap.get(poolInfo);
  }

  /**
   * Is the value set with the appropriate keys?
   *
   * @param type Resource type
   * @param poolInfo Pool info
   * @return True if this map contains a mapping for the specified key, false
   *         otherwise
   */
  public boolean containsKey(ResourceType type, PoolInfo poolInfo) {
    Map<PoolInfo, V> poolInfoMap = typePoolInfoMap.get(type);
    if (poolInfoMap == null) {
      return false;
    }
    return poolInfoMap.containsKey(poolInfo);
  }
}
