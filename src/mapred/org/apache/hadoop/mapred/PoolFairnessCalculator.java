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

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.metrics.MetricsRecord;

/**
 * Helper utilies to calculate the fairness of pools.
 */
public class PoolFairnessCalculator {
  /** Class logger */
  private static final Log LOG =
      LogFactory.getLog(PoolFairnessCalculator.class);

  /**
   * This is a utility class, do not construct.
   */
  private PoolFairnessCalculator() { }

  /** Prefix for metrics of fairness resource difference */
  private static final String FAIRNESS_DIFFERENCE_COUNT_PREFIX =
      "fairness_difference_count_";
  /** Prefix for metrics of average difference per pool */
  private static final String FAIRNESS_DIFFERENCE_PER_POOL_PREFIX =
      "fairness_difference_per_pool_";
  /** Prefix for metrics of unfairness percentage */
  private static final String FAIRNESS_PERCENT_UNFAIR_PREFIX =
      "fairness_percent_unfair_";
  /** Prefix of metrics for standard deviation of unfairness */
  private static final String FAIRNESS_UNFAIR_STD_DEV_PERFIX =
      "fairness_unfair_std_dev_";
  /** Prefix of total resources available for printing */
  private static final String TOTAL_RESOURCES_PREFIX = "total_resources_";

  /**
   * Helper object to keep track of the metadata for all resources of a
   * particular type.
   */
  private static class TotalResourceMetadata {
    /** Total resources that were allocated by the calculator */
    private int totalAllocated = 0;
    /** Total resources that were allocated by the scheduler */
    private int totalAvailable = 0;
    /**
     * Total difference between the expected and actual resources
     * allocated per pool
     */
    private int totalFairnessDifference = 0;
    /**
     * Differences between expected and actual resources squared for all pools
     */
    private int totalFairnessDifferenceSquared = 0;
    /** % of resources unfairly allocated for this resource type [0,100] */
    private float percentUnfair;
    /** Standard deviation of unfairly allocated resources */
    private float stdDevUnfair;
    /** Average resource unfairness per pool */
    private float averageUnfairPerPool;
    /**
     * Number of resources this type represents (usually one, except for
     * when this represents all the resource types)
     */
    private int resourceTypeCount = 1;
  }

  /**
   * A comparator for ResourceMetadata that sorts by the largest
   * min(min guaranteed, desired).
   */
  private static class GuaranteedDesiredComparator implements
      Comparator<ResourceMetadata> {
    @Override
    public int compare(ResourceMetadata left, ResourceMetadata right) {
      return right.getGuaranteedUsedAndDesired() -
          left.getGuaranteedUsedAndDesired();
    }
  }

  /**
   * A comparator for ResourceMetadata that sorts by the smallest expected
   * used.
   */
  private static class ExpectedUsedComparator implements
      Comparator<ResourceMetadata> {
    @Override
    public int compare(ResourceMetadata left, ResourceMetadata right) {
      return left.getExpectedUsed() - right.getExpectedUsed();
    }
  }

  /**
   * This method takes a list of {@link PoolMetadata} objects and calculates
   * fairness metrics of how well scheduling is doing.
   *
   * The goals of the fair scheduling are to insure that every pool is getting
   * an equal share.  The expected share of resources for each pool is
   * complicated by the pools not requiring an equal share
   * or pools that have a minimum or maximum allocation of resources.
   *
   * @param poolMetadataList List of all pool metadata
   * @param metricsRecord Where to write the metrics
   */
  public static void calculateFairness(
      final List<PoolMetadata> poolMetadataList,
      final MetricsRecord metricsRecord) {
    if (poolMetadataList == null || poolMetadataList.isEmpty()) {
      return;
    }

    // Find the total available usage and guaranteed resources by resource
    // type.  Add the resource metadata to the sorted set to schedule if
    // there is something to schedule (desiredAfterConstraints > 0)
    long startTime = System.currentTimeMillis();
    Map<String, TotalResourceMetadata> resourceTotalMap =
        new HashMap<String, TotalResourceMetadata>();
    Map<String, Set<ResourceMetadata>> resourceSchedulablePoolMap =
        new HashMap<String, Set<ResourceMetadata>>();
    for (PoolMetadata poolMetadata : poolMetadataList) {
      for (String resourceName : poolMetadata.getResourceMetadataKeys()) {
        ResourceMetadata resourceMetadata =
            poolMetadata.getResourceMetadata(resourceName);
        TotalResourceMetadata totalResourceMetadata =
            resourceTotalMap.get(resourceName);
        if (totalResourceMetadata == null) {
          totalResourceMetadata = new TotalResourceMetadata();
          resourceTotalMap.put(resourceName, totalResourceMetadata);
        }
        totalResourceMetadata.totalAvailable +=
            resourceMetadata.getCurrentlyUsed();

        Set<ResourceMetadata> schedulablePoolSet =
            resourceSchedulablePoolMap.get(resourceName);
        if (schedulablePoolSet == null) {
          schedulablePoolSet = new HashSet<ResourceMetadata>();
          resourceSchedulablePoolMap.put(resourceName, schedulablePoolSet);
        }
        if (resourceMetadata.getDesiredAfterConstraints() > 0) {
          if (!schedulablePoolSet.add(resourceMetadata)) {
            throw new RuntimeException("Duplicate resource metadata " +
                resourceMetadata + " in " + schedulablePoolSet);
          }
        }
      }
    }

    // First, allocate resources for all the min guaranteed resources
    // for the pools.  Ordering is done by the largest
    // min(min guaranteed, desired).
    GuaranteedDesiredComparator guarantedDesiredComparator =
        new GuaranteedDesiredComparator();
    List<ResourceMetadata> removePoolList = new ArrayList<ResourceMetadata>();
    for (Map.Entry<String, TotalResourceMetadata> entry :
        resourceTotalMap.entrySet()) {
      List<ResourceMetadata> resourceMetadataList =
          new ArrayList<ResourceMetadata>(
              resourceSchedulablePoolMap.get(entry.getKey()));
      TotalResourceMetadata totalResourceMetadata = entry.getValue();
      Collections.sort(resourceMetadataList, guarantedDesiredComparator);
      while ((totalResourceMetadata.totalAllocated <
          totalResourceMetadata.totalAvailable) &&
          !resourceMetadataList.isEmpty()) {
        removePoolList.clear();
        for (ResourceMetadata resourceMetadata : resourceMetadataList) {
          if (resourceMetadata.getExpectedUsed() ==
              resourceMetadata.getGuaranteedUsedAndDesired()) {
            removePoolList.add(resourceMetadata);
            continue;
          }
          resourceMetadata.incrExpectedUsed();
          ++totalResourceMetadata.totalAllocated;
        }
        resourceMetadataList.removeAll(removePoolList);
      }
      LOG.info("After allocating min guaranteed and desired - " +
      		"Resource type " + entry.getKey() +
          " totalAvailable=" + totalResourceMetadata.totalAvailable +
          ", totalAllocated=" + totalResourceMetadata.totalAllocated);
    }

    // At this point, all pools have been allocated their guaranteed used and
    // desired resources.  If there are any more resources to allocate, give
    // resources to lowest allocated pool that hasn't reached desired
    // until all the resources are gone
    ExpectedUsedComparator expectedUsedComparator =
        new ExpectedUsedComparator();
    PriorityQueue<ResourceMetadata> minHeap =
        new PriorityQueue<ResourceMetadata>(100, expectedUsedComparator);
    for (Map.Entry<String, TotalResourceMetadata> entry :
        resourceTotalMap.entrySet()) {
      minHeap.addAll(resourceSchedulablePoolMap.get(entry.getKey()));
      TotalResourceMetadata totalResourceMetadata = entry.getValue();
      while ((totalResourceMetadata.totalAllocated <
          totalResourceMetadata.totalAvailable) && !minHeap.isEmpty()) {
        ResourceMetadata resourceMetadata = minHeap.remove();
        if (resourceMetadata.getExpectedUsed() ==
            resourceMetadata.getDesiredAfterConstraints()) {
          continue;
        }
        resourceMetadata.incrExpectedUsed();
        ++totalResourceMetadata.totalAllocated;
        minHeap.add(resourceMetadata);
      }
      minHeap.clear();
    }

    // Now calculate the difference of the expected allocation and the
    // actual allocation to get the following metrics.  When calculating
    // the percent bad allocated divide by 2 because the difference double
    // counts a bad allocation
    // 1) total tasks difference between expected and actual allocation
    //    0 is totally fair, higher is less fair
    // 2) % of tasks incorrectly allocated
    //    0 is totally fair, higher is less fair
    // 3) average difference per pool
    //    0 is totally fair, higher is less fair
    // 4) standard deviation per pool
    //    0 is totally fair, higher is less fair
    for (PoolMetadata poolMetadata : poolMetadataList) {
      for (String resourceName : poolMetadata.getResourceMetadataKeys()) {
        ResourceMetadata resourceMetadata =
            poolMetadata.getResourceMetadata(resourceName);
        int diff = Math.abs(resourceMetadata.getExpectedUsed() -
            resourceMetadata.getCurrentlyUsed());
        LOG.info("Pool " + poolMetadata.getPoolName() +
            ", resourceName=" + resourceName +
            ", expectedUsed=" + resourceMetadata.getExpectedUsed() +
            ", currentUsed=" + resourceMetadata.getCurrentlyUsed() +
            ", maxAllowed=" + resourceMetadata.getMaxAllowed() +
            ", desiredAfterConstraints=" +
              resourceMetadata.getDesiredAfterConstraints() +
            ", guaranteedUsedAndDesired=" +
              resourceMetadata.getGuaranteedUsedAndDesired() +
            ", diff=" + diff);
        resourceTotalMap.get(resourceName).totalFairnessDifference +=
            diff;
        resourceTotalMap.get(resourceName).totalFairnessDifferenceSquared +=
            diff * diff;
      }
    }
    TotalResourceMetadata allResourceMetadata = new TotalResourceMetadata();
    allResourceMetadata.resourceTypeCount = resourceTotalMap.size();
    for (TotalResourceMetadata totalResourceMetadata :
        resourceTotalMap.values()) {
      allResourceMetadata.totalAvailable +=
          totalResourceMetadata.totalAvailable;
      allResourceMetadata.totalFairnessDifference +=
          totalResourceMetadata.totalFairnessDifference;
      allResourceMetadata.totalFairnessDifferenceSquared +=
          totalResourceMetadata.totalFairnessDifferenceSquared;
    }
    resourceTotalMap.put("all", allResourceMetadata);
    StringBuilder metricsBuilder = new StringBuilder();
    for (Map.Entry<String, TotalResourceMetadata> entry :
        resourceTotalMap.entrySet()) {
      TotalResourceMetadata totalResourceMetadata = entry.getValue();
      totalResourceMetadata.percentUnfair =
          (totalResourceMetadata.totalAvailable == 0) ? 0 :
              totalResourceMetadata.totalFairnessDifference * 100f / 2 /
              totalResourceMetadata.totalAvailable;
      totalResourceMetadata.stdDevUnfair = (float) Math.sqrt(
          (double) totalResourceMetadata.totalFairnessDifferenceSquared /
          poolMetadataList.size() / totalResourceMetadata.resourceTypeCount);
      totalResourceMetadata.averageUnfairPerPool =
          (float) totalResourceMetadata.totalFairnessDifference /
          poolMetadataList.size() / totalResourceMetadata.resourceTypeCount;

      metricsRecord.setMetric(
          FAIRNESS_DIFFERENCE_COUNT_PREFIX + entry.getKey(),
          totalResourceMetadata.totalFairnessDifference);
      metricsBuilder.append(
          FAIRNESS_DIFFERENCE_COUNT_PREFIX + entry.getKey() + "=" +
              totalResourceMetadata.totalFairnessDifference + "\n");
      metricsRecord.setMetric(
          FAIRNESS_PERCENT_UNFAIR_PREFIX + entry.getKey(),
          totalResourceMetadata.percentUnfair);
      metricsBuilder.append(
          FAIRNESS_PERCENT_UNFAIR_PREFIX + entry.getKey() + "=" +
              totalResourceMetadata.percentUnfair + "\n");
      metricsRecord.setMetric(
          FAIRNESS_DIFFERENCE_PER_POOL_PREFIX + entry.getKey(),
          totalResourceMetadata.averageUnfairPerPool);
      metricsBuilder.append(
          FAIRNESS_DIFFERENCE_PER_POOL_PREFIX + entry.getKey() + "=" +
              totalResourceMetadata.averageUnfairPerPool + "\n");
      metricsRecord.setMetric(
          FAIRNESS_UNFAIR_STD_DEV_PERFIX + entry.getKey(),
          totalResourceMetadata.stdDevUnfair);
      metricsBuilder.append(
          FAIRNESS_UNFAIR_STD_DEV_PERFIX + entry.getKey() + "=" +
              totalResourceMetadata.stdDevUnfair + "\n");
      metricsBuilder.append(TOTAL_RESOURCES_PREFIX + entry.getKey() + "=" +
          totalResourceMetadata.totalAvailable + "\n");
    }

    if (LOG.isInfoEnabled()) {
      LOG.info("calculateFairness took " +
          (System.currentTimeMillis() - startTime) +  " millisecond(s).");
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("\n" + metricsBuilder.toString());
    }
  }
}
