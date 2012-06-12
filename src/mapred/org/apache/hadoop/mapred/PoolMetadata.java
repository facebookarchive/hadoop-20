/**
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

package org.apache.hadoop.mapred;

import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

/**
 * Metadata about a pool that is a snapshot of the actual state of the pool.
 * The fair scheduler for MapReduce and Corona can both use this helper class
 * for calculating fairness metrics.
 */
public class PoolMetadata {
  /** Name of the pool*/
  private final String poolName;
  /**
   * Mapping of the resource type to the resource metadata.  Uses string as
   * the resource type to work with MapReduce as well Corona.
   */
  private final Map<String, ResourceMetadata> resourceMetadataMap =
      new TreeMap<String, ResourceMetadata>();

  /**
   * Constructor.
   *
   * @param poolName Name of this pool
   */
  public PoolMetadata(String poolName) {
    this.poolName = poolName;
  }

  /**
   * Add resource metadata for this pool.
   *
   * @param resourceName Name of the resource
   * @param resourceMetadata Resource metadata associated with the name
   */
  public void addResourceMetadata(String resourceName,
      ResourceMetadata resourceMetadata) {
    if (resourceMetadataMap.put(resourceName, resourceMetadata) != null) {
      throw new RuntimeException("Resource name " + resourceName +
          " already exists!");
    }
  }

  /**
   * Get a set of the keys of all the resource types for this pool.
   *
   * @return Set of resource types as strings
   */
  public Set<String> getResourceMetadataKeys() {
    return resourceMetadataMap.keySet();
  }

  /**
   * Get resource metadata for a resource name
   *
   * @param resourceName Name of the resource
   * @return Resource metadata for this name
   * @throws RuntimException if the resource name doesn't exist
   */
  public ResourceMetadata getResourceMetadata(String resourceName) {
    if (!resourceMetadataMap.containsKey(resourceName)) {
      throw new RuntimeException("No resource metadata for " + resourceName);
    }
    return resourceMetadataMap.get(resourceName);
  }

  public String getPoolName() {
    return poolName;
  }
}