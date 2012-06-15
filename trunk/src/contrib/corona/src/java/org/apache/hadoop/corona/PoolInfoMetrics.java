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
import org.apache.hadoop.metrics.MetricsRecord;

/**
 * Keeps track of a pool info's metrics for a resource type.
 */
public class PoolInfoMetrics {
  /** Identifies the pool info */
  private final PoolInfo poolInfo;
  /** Type of the resource of this pool */
  private final ResourceType type;
  /** Map of metric names to counters */
  private final Map<MetricName, Long> counters;
  /** Metrics record. */
  private final MetricsRecord record;

  /**
   * Various metrics names.
   */
  public enum MetricName {
    /** Sessions in a pool */
    SESSIONS("Sessions"),
    /** Requested resources in a pool */
    REQUESTED("Requested"),
    /** Granted resources in a pool */
    GRANTED("Granted"),
    /** Target share of resources in a pool */
    SHARE("Share"),
    /** Minimum resources this pool should get */
    MIN("Min"),
    /** Maximum resources this pool should get */
    MAX("Max"),
    /** Proportional to the resources this pool should get compared to others */
    WEIGHT("Weight"),
    /** Maximum starvation time metric for this pool */
    STARVING("StarvingTime"),
    /** Average first resource wait time in ms */
    AVE_FIRST_WAIT_MS("AverageFirstWaitMs");

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
   * Constructor for a pool info of a specific resource.
   *
   * @param poolInfo Pool info
   * @param type Resource type
   * @param record The metrics record for this object
   * @param
   */
  public PoolInfoMetrics(PoolInfo poolInfo, ResourceType type,
      MetricsRecord record) {
    this.poolInfo = poolInfo;
    this.type = type;
    this.counters =
        Collections.synchronizedMap(new HashMap<MetricName, Long>());
    this.record = record;
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
  public Long getCounter(MetricName name) {
    return counters.get(name);
  }

  /**
   * Get a snapshot of the resource metadata for this pool.  Used for
   * collecting metrics.  Will not collect resource metadata for PoolGroup
   * objects or if any counters are missing.
   *
   * @return ResourceMetadata for this pool or null if the metrics are invalid.
   */
  public ResourceMetadata getResourceMetadata() {
    if (poolInfo.getPoolName() == null ||
        !counters.containsKey(MetricName.MIN) ||
        !counters.containsKey(MetricName.MAX) ||
        !counters.containsKey(MetricName.GRANTED) ||
        !counters.containsKey(MetricName.REQUESTED)) {
      return null;
    }

    return new ResourceMetadata(
        PoolInfo.createStringFromPoolInfo(poolInfo),
        counters.get(MetricName.MIN).intValue(),
        counters.get(MetricName.MAX).intValue(),
        counters.get(MetricName.GRANTED).intValue(),
        counters.get(MetricName.REQUESTED).intValue());
  }

  /**
   * Update the metrics record associated with this object.
   */
  public void updateMetricsRecord() {
    for (Map.Entry<MetricName, Long> entry: counters.entrySet()) {
      String name = (entry.getKey() + "_" + type).toLowerCase();
      record.setMetric(name, entry.getValue());
    }
    record.update();
  }
}
