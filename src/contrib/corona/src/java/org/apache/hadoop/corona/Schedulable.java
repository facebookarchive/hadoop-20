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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.Vector;

/**
 * Maintain parameters for sorting
 */
public abstract class Schedulable {
  /** The number of resources requested */
  protected int requested;
  /** The number of requests pending. */
  protected int pending;
  /** The number of resources granted */
  protected int granted;

  /** The name of this schedulable */
  private final String name;
  /** The type of this schedulable */
  private final ResourceType type;
  /** The share of this schedulable */
  private double share;

  /**
   * Construct a Schedulable with a given name and for a given type
   * @param name the name of the new schedulable
   * @param type the type of the new schedulable
   */
  public Schedulable(String name, ResourceType type) {
    this.name = name;
    this.type = type;
  }

  /**
   * Get the name of this {@link Schedulable}
   *
   * @return Name of this {@link Schedulable}
   */
  public String getName() {
    return name;
  }

  public ResourceType getType() {
    return type;
  }

  /**
   * Take a snapshot of requested and granted
   */
  public abstract void snapshot();

  /**
   * Number of current requested resource at the last snapshot
   * @return the number of requested resources
   */
  public int getRequested() {
    return requested;
  }

  /** Number of requested resources that are not granted yet.
   * This can be different from requested - granted because of preemption.
   * @return the number of pending requests.
   */
  public int getPending() {
    return pending;
  }

  /**
   * Number of current granted resource at the last snapshot
   * @return the number of granted resources
   */
  public int getGranted() {
    return granted;
  }

  /**
   * Change the number of granted resource since last snapshot
   * @param diff the increment
   */
  public void incGranted(int diff) {
    granted += diff;
  }

  /**
   * Proportional to the number of resource this schedulable should get
   * @return the weight of this schedulable
   */
  public double getWeight() {
    return 1.0;
  }

  /**
   * The minimum number of resource this schedulable should get
   * @return the min share for this schedulable
   */
  public int getMinimum() {
    return 0;
  }

  /**
   * The maximum number of resource this schedulable should get
   * @return the max share cap for this schedulable
   */
  public int getMaximum() {
    return Integer.MAX_VALUE;
  }

  /**
   * Start time of this schedulable. Can be used for FIFO sort
   * @return the start time of this schedulable
   */
  public long getStartTime() {
    return -1L;
  }

  /**
   * Deadline for this Schedulable. Can be used for deadline based
   * scheduling when the job with the earlier deadline gets all
   * the resources
   *
   * @return the deadline of the schedulable
   */
  public abstract long getDeadline();


  /**
   * The priority for this schedulable. Used when sorting the sessions
   *
   * @return the priority of the schedulable
   */
  public abstract int getPriority();

  /**
   * The target number of granted resource should get in equilibrium
   * @return the share of this schedulable
   */
  public double getShare() {
    return share;
  }

  /**
   * Distribute the shares among the schedulables based on the comparator
   * @param total the total share to distribute
   * @param schedulables the list of schedulables
   * @param comparator the comparator to use when distributing the share
   */
  public static void distributeShare(
      double total, final Collection<? extends Schedulable> schedulables,
      ScheduleComparator comparator) {
    switch (comparator) {
    case FIFO:
    case DEADLINE:
      Schedulable.distributeShareSorted(total, schedulables, comparator);
      break;
    case FAIR:
      Schedulable.distributeShareFair(total, schedulables);
      break;
    case PRIORITY:
      Schedulable.distributeSharePriority(total, schedulables);
      break;
    default:
      throw new IllegalArgumentException("Unknown comparator");
    }
  }

  /**
   * Distribute the share among Schedulables in a greedy manner when
   * they are sorted based on some comparator and the first Schedulable
   * has to be fully satisfied before the next one can get any resources
   *
   * @param total the total amount of share to be distributed
   * @param schedulables a collection of schedulables to get the share
   * @param comparator a comparator to use when sorting schedulables
   */
  private static void distributeShareSorted(
      double total, final Collection<? extends Schedulable> schedulables,
      ScheduleComparator comparator) {
    List<Schedulable> sches = new ArrayList<Schedulable>(schedulables);
    Collections.sort(sches, comparator);
    for (Schedulable schedulable : sches) {
      int max = Math.min(schedulable.getRequested(), schedulable.getMaximum());
      if (total > max) {
        schedulable.share = max;
        total -= max;
      } else {
        schedulable.share = total;
        return;
      }
    }
  }

  /**
   * Distribute the total share among the list of schedulables according to the
   * FAIR model.
   * Finds a way to distribute the share in such a way that all the
   * min and max reservations of the schedulables are satisfied
   * @param total the share to be distributed
   * @param schedulables the list of schedulables
   */
  private static void distributeShareFair(
      double total, final Collection<? extends Schedulable> schedulables) {
    BinarySearcher searcher = new BinarySearcher() {
      @Override
      protected double targetFunction(double x) {
        return totalShareWithRatio(schedulables, x);
      }
    };
    double ratio = searcher.getSolution(total);
    for (Schedulable schedulable : schedulables) {
      schedulable.share = shareWithRatio(schedulable, ratio);
    }
  }

  /**
   * Compute the total share of the schedulables given a weightToShareRatio
   * @param schedulables the list of schedulables to compute the share for
   * @param weightToShareRatio the weightToShareRatio to compute it for
   * @return the total share of all the schedulables for a given
   * weightToShareRatio
   */
  private static double totalShareWithRatio(
      Collection<? extends Schedulable> schedulables,
      double weightToShareRatio) {
    double totalShare = 0;
    for (Schedulable schedulable : schedulables) {
      totalShare += shareWithRatio(schedulable, weightToShareRatio);
    }
    return totalShare;
  }

  /**
   * Get the share of the schedulable given a weightToShareRatio.
   * This takes into account the min and the max allocation and is used
   * to compute the weightToShareRatio globally
   * @param schedulable the schedulable to compute the share for
   * @param weightToShareRatio the multiplier for the weight of the schedulable
   * @return the share of the schedulable given a weightToShareRatio
   */
  private static double shareWithRatio(
      Schedulable schedulable, double weightToShareRatio) {
    double share = schedulable.getWeight() * weightToShareRatio;
    int min = schedulable.getMinimum();
    int max = schedulable.getMaximum();
    int requested = schedulable.getRequested();
    share = Math.max(min, share);
    share = Math.min(max, share);
    share = Math.min(requested, share);
    return share;
  }

  /**
   * Assign fair share if total share < min demand.
   *
   * @param totalShare   The share to be distributed.
   * @param schedulables The collection of schedulables
   * @return Excess share after assigning the min demand to each schedulable.
   */
  private static double assignShareIfUnderAllocated(
      double totalShare,
      final Collection<? extends Schedulable> schedulables) {

    double totalMinDemand = 0;
    for (Schedulable schedulable : schedulables) {
      schedulable.share = 0;
      totalMinDemand += Math.min(schedulable.getRequested(),
                                 schedulable.getMinimum());
    }

    if ((totalMinDemand > 0) && (totalMinDemand >= totalShare)) {
      distributeShareMin(schedulables);
    }

    return totalShare - totalMinDemand;
  }

  /**
   * Group a collection of schedulables into priority groups
   * sorted by priority.
   *
   * @param schedulables Collection of schedulables
   * @return Sorted map of priority to vector of schedulables
   *         (lowest priority first)
   */
  private static TreeMap<Integer, Vector<Schedulable>>
      generatePriorityGroupedSchedulables(
        final Collection<? extends Schedulable> schedulables) {
    TreeMap<Integer, Vector<Schedulable>> prioritizedSchedulableMap =
        new TreeMap<Integer, Vector<Schedulable>>();

    for (Schedulable schedulable : schedulables) {
      if (! prioritizedSchedulableMap.containsKey(schedulable.getPriority())) {
        prioritizedSchedulableMap.put(schedulable.getPriority(),
                                      new Vector<Schedulable>());
      }
      prioritizedSchedulableMap.get(schedulable.getPriority()).add(schedulable);
    }
    return prioritizedSchedulableMap;
  }

  /**
   * Does PRIORITY share distribution across priority groups.
   * Algorithm is as follows:
   * Starting from the highest priority,
   * try to allocate min(max_alloc, demand) to priority group.
   * If that's possible, then move to the next prty group in order.
   * If that's not possible, do fair share on that prty group
   * and then allocate min_alloc to all the other
   * priority groups.
   *
   * @param share The number of resources that need to be allocated.
   * @param prioritizedSchedulableMap The priority group map.
   */
  private static void trickleShareDownPriorityGroups(
      double share,
      TreeMap<Integer, Vector<Schedulable>> prioritizedSchedulableMap) {

    double surplusDemandForPriorityGroup, totalMinDemandForPriorityGroup;
    for (Map.Entry<Integer, Vector<Schedulable>> entry :
        prioritizedSchedulableMap.descendingMap().entrySet()) {
      Vector<Schedulable> schedulableVector = entry.getValue();
      if (share <= 0) {
        distributeShareMin(schedulableVector);
        continue;
      }

      surplusDemandForPriorityGroup = 0.0;
      totalMinDemandForPriorityGroup = 0.0;

      for (Schedulable schedulable : schedulableVector) {
        double d = Math.min(schedulable.getMaximum(),
                            schedulable.getRequested());
        totalMinDemandForPriorityGroup +=
            Math.min(schedulable.getMinimum(), d);
        surplusDemandForPriorityGroup +=
            Math.max(0, d - schedulable.getMinimum());
      }

      if (surplusDemandForPriorityGroup >= share) {
        distributeShareFair(share + totalMinDemandForPriorityGroup,
                            schedulableVector);
      } else {
        distributeShareMax(schedulableVector);
      }
      share -= surplusDemandForPriorityGroup;
    }
  }


  /**
   * Distribute the total share among the list of schedulables according to the
   * PRIORITY model. Note that the eventual intent is to determine if
   * preemption is necessary. So, we need to assign the most ideal share on
   * each schedulable beyond which preemption is necessary.
   * Finds a way to distribute the share in such a way that all the
   * min and max reservations of the schedulables are satisfied. In
   * the PRIORITY scheduling, MINs are always granted to all pools. Therefore,
   * irrespective of the demand of a pool, its share according to the
   * algorithm will be at least MIN.
   *
   * @param total the share to be distributed
   * @param schedulables the list of schedulables
   */
  private static void distributeSharePriority(
      double total, final Collection<? extends Schedulable> schedulables) {

    // 1) If share < sum(min(demand, min_alloc)) then do fair share and quit.
    double residualShare = assignShareIfUnderAllocated(total, schedulables);
    if (residualShare <= 0.0) {
      return;
    }

    // 2) Group schedulables according to priorities.
    TreeMap<Integer, Vector<Schedulable>> prioritizedSchedulableMap =
        generatePriorityGroupedSchedulables(schedulables);

    // 3) Trickle the share across priority groups.
    trickleShareDownPriorityGroups(residualShare, prioritizedSchedulableMap);
  }

  /**
   * Increment each of the schedulables' share by
   * the minimum demand. Note that each pool has to be guaranteed
   * at least its MIN value according to PRIORITY scheduling.
   *
   * @param schedulableVector: The collection of schedulables
   *                           (all with the same priority)
   */
  private static void distributeShareMin(
      Collection<? extends Schedulable> schedulableVector) {
    for (Schedulable schedulable : schedulableVector) {
      schedulable.share += schedulable.getMinimum();
    }
  }

  /**
   * Increment each of the schedulables' share by
   * the lesser of its min demand or its maximum.
   *
   * @param schedulableVector: The collection of schedulables
   *                           (all with the same priority)
   */
  private static void distributeShareMax(
      Collection<? extends Schedulable> schedulableVector) {
    for (Schedulable schedulable : schedulableVector) {
      double minShare = Math.max(schedulable.getMinimum(),
                                 schedulable.getRequested());
      schedulable.share += Math.min(schedulable.getMaximum(),
                                    minShare);
    }
  }
}
