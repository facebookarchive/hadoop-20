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
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.mapred.ResourceMetadata;

/**
 * Keeps track of a pool's metrics.
 */
public class PoolMetrics {
  /** Identifies the pool */
  private final String poolId;
  /** Type of the resource of this pool */
  private final ResourceType type;
  /** Map of metric names to counters */
  private final Map<MetricName, Long> counters;

  /**
   * Various metrics names.
   */
  public enum MetricName {
    /** Sessions in a pool */
    SESSIONS("Sessions"),
    /** Granted resources in a pool */
    GRANTED("Granted"),
    /** Requested resources in a pool */
    REQUESTED("Requested"),
    /** Target share of resources in a pool */
    SHARE("Share"),
    /** Minimum resources this pool should get */
    MIN("Min"),
    /** Maximum resources this pool should get */
    MAX("Max"),
    /** Proportional to the resources this pool should get compared to others */
    WEIGHT("Weight"),
    /** Maximum starvation time metric for this pool */
    STARVING("StarvingTime");

    /** Title of the metric */
    private final String title;

    /**
     * Constructor to provide a title for every metric
     *
     * @param title Title of this metric
     */
    MetricName(String title) {
      this.title = title;
    }

    @Override
    public String toString() {
      return title;
    }
  }

  /**
   * Constructor for a pool of a specific resource.
   *
   * @param poolId Identifier of a pool
   * @param type Resource type
   */
  public PoolMetrics(String poolId, ResourceType type) {
    this.poolId = poolId;
    this.type = type;
    this.counters =
        Collections.synchronizedMap(new HashMap<MetricName, Long>());
  }

  /**
   * Atomically set the metric with a value.
   *
   * @param name Name of the metric
   * @param value Value of the metric
   */
  public void setCounter(MetricName name, long value) {
    counters.put(name, value);
  }

  /**
   * Atomically get the value of a metric.
   *
   * @param name Name of the metric
   * @return Value of the metric
   */
  public long getCounter(MetricName name) {
    return counters.get(name);
  }

  /**
   * Get a snapshot of the resource metadata for this pool.  Used for
   * collecting metrics.
   *
   * @return ResourceMetadata for this pool or null if the metrics are invalid.
   */
  public ResourceMetadata getResourceMetadata() {
    if (!counters.containsKey(MetricName.MIN) ||
        !counters.containsKey(MetricName.MAX) ||
        !counters.containsKey(MetricName.GRANTED) ||
        !counters.containsKey(MetricName.REQUESTED)) {
      return null;
    }

    return new ResourceMetadata(
        poolId,
        counters.get(MetricName.MIN).intValue(),
        counters.get(MetricName.MAX).intValue(),
        counters.get(MetricName.GRANTED).intValue(),
        counters.get(MetricName.REQUESTED).intValue());
  }
}
