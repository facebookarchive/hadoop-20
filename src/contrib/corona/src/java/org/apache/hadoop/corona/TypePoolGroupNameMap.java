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
 * Customized multikey map for ResourceType, PoolGroup name to value.
 * Not thread-safe.
 *
 * @param <V> Value to map to.
 */
public class TypePoolGroupNameMap<V> {
  /** Internal map of map that stores everything */
  private final Map<ResourceType, Map<String, V>> typePoolGroupNameMap =
      new EnumMap<ResourceType, Map<String, V>>(ResourceType.class);

  /**
   * Put the value with the appropriate keys
   *
   * @param type Resource type
   * @param poolGroup Name of pool group
   * @param value Value to put
   * @return Previous value, can be null if wasn't set
   */
  public V put(ResourceType type, String poolGroupName, V value) {
    Map<String, V> poolGroupNameMap = typePoolGroupNameMap.get(type);
    if (poolGroupNameMap == null) {
      poolGroupNameMap = new HashMap<String, V>();
      typePoolGroupNameMap.put(type, poolGroupNameMap);
    }
    return poolGroupNameMap.put(poolGroupName, value);
  }

  /**
   * Get the value with the appropriate keys
   *
   * @param type Resource type
   * @param poolGroup Name of pool group
   * @param value Value to put
   * @return Previous value, can be null if if wasn't set
   */
  public V get(ResourceType type, String poolGroupName) {
    Map<String, V> poolGroupNameMap = typePoolGroupNameMap.get(type);
    if (poolGroupNameMap == null) {
      return null;
    }
    return poolGroupNameMap.get(poolGroupName);
  }

  /**
   * Is the value set with the appropriate keys?
   *
   * @param type Resource type
   * @param poolGroup Name of pool group
   * @return True if this map contains a mapping for the specified key, false
   *         otherwise
   */
  public boolean containsKey(ResourceType type, String poolGroupName) {
    Map<String, V> poolGroupNameMap = typePoolGroupNameMap.get(type);
    if (poolGroupNameMap == null) {
      return false;
    }
    return poolGroupNameMap.containsKey(poolGroupName);
  }
}
