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
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Represents a group of pools
 */
public class PoolGroupSchedulable extends Schedulable {
  /** The set of all pools into this group */
  private final ConcurrentHashMap<PoolInfo, PoolSchedulable> nameToMap =
      new ConcurrentHashMap<PoolInfo, PoolSchedulable>();
  /** Dynamic configuration */
  private final ConfigManager configManager;
  /** The list of pool snapshots used for scheduling */
  private Collection<PoolSchedulable> snapshotPools;
  /** The queue of pools for scheduling */
  private Queue<PoolSchedulable> scheduleQueue;
  /** The queue of pools for preemption */
  private Queue<PoolSchedulable> preemptQueue;
  /** The max allocation for the pool group */
  private int maximum;
  /** The minimum allocation for the pool group */
  private int minimum;
  /** A flag to indicate that the pool group needs its share redistributed */
  private boolean needRedistributed;
  /** Pool info or this pool group (null pool name) */
  private final PoolInfo poolInfo;

  /**
   * Constructs a pool group
   *
   * @param name Name of the pool group
   * @param type Type of the resource of this pool group
   * @param configManager Dynamic configuration
   */
  public PoolGroupSchedulable(
      String name, ResourceType type, ConfigManager configManager) {
    super(name, type);
    poolInfo = new PoolInfo(name, null);
    this.configManager = configManager;
    this.needRedistributed = true;
  }

  public PoolInfo getPoolInfo() {
    return poolInfo;
  }

  /**
   * Get the snapshot of the configuration from the configuration manager
   */
  private void snapshotConfig() {
    maximum = configManager.getPoolGroupMaximum(getName(), getType());
    minimum = configManager.getPoolGroupMinimum(getName(), getType());
  }

  @Override
  public void snapshot() {
    snapshotPools = new ArrayList<PoolSchedulable>(nameToMap.values());
    granted = 0;
    requested = 0;
    pending = 0;
    for (PoolSchedulable pool : snapshotPools) {
      pool.snapshot();
      granted += pool.getGranted();
      requested += pool.getRequested();
      pending += pool.getPending();
    }
    snapshotConfig();
    scheduleQueue = null;
    preemptQueue = null;
    needRedistributed = true;
  }

  /**
   * Get the queue of pools sorted for scheduling
   * @return the queue of pools sorted for scheduling
   */
  public Queue<PoolSchedulable> getScheduleQueue() {
    if (scheduleQueue == null) {
      ScheduleComparator sc = configManager.getPoolGroupComparator(getName());
      scheduleQueue = createPoolQueue(sc);
    }
    return scheduleQueue;
  }

  /**
   * Get the queue of the pool sorted for preemption
   * @return the queue of pool sorted for preemption
   */
  public Queue<PoolSchedulable> getPreemptQueue() {
    // TODO: For now, we support only one kind of scheduling.
    // Also note that FAIR is PRIORITY with equal priorities (by default)
    ScheduleComparator sPreempt = null;
    if (preemptQueue == null) {
      ScheduleComparator sc = configManager.getPoolGroupComparator(getName());
      if (sc == ScheduleComparator.PRIORITY) {
        sPreempt = ScheduleComparator.PRIORITY_PREEMPT;
      } else {
        throw new IllegalArgumentException("Unknown/misconfigured poolgroup");
      }
      preemptQueue = createPoolQueue(sPreempt);
    }
    return preemptQueue;
  }

  /**
   * Put all the pools into the priority queue sorted by a comparator
   * @param comparator the comparator to sort all the pools in the queue by
   * @return the queue of the pools sorted by a comparator
   */
  private Queue<PoolSchedulable> createPoolQueue(
      ScheduleComparator comparator) {
    int initCapacity =
        snapshotPools.size() == 0 ? 1 : snapshotPools.size();
    Queue<PoolSchedulable> poolQueue =
        new PriorityQueue<PoolSchedulable>(initCapacity, comparator);
    poolQueue.addAll(snapshotPools);
    return poolQueue;
  }

  @Override
  public long getDeadline() {
    // No deadline for pool groups
    return 0;
  }

  @Override
  public int getPriority() {
    // No priority for pool groups
    return 0;
  }

  @Override
  public int getMaximum() {
    return maximum;
  }

  @Override
  public int getMinimum() {
    return minimum;
  }

  /**
   * Check if the pool's share has reached its maximum share
   * @return true if the pool's share has reached its maximum, false
   * otherwise
   */
  public boolean reachedMaximum() {
    return granted >= getMaximum() || granted >= requested;
  }

  /**
   * Distribute the pool's share between the sessions in the pool
   */
  public void distributeShare() {
    if (needRedistributed) {
      ScheduleComparator sc = configManager.getPoolGroupComparator(getName());
      Schedulable.distributeShare(getShare(), snapshotPools, sc);
    }
    needRedistributed = false;
  }

  /**
   * Get a pool, creating it if it does not exist.  Note that these
   * pools are never removed.
   *
   * @param poolInfo Pool info used
   * @return Pool that existed or was created
   */
  public PoolSchedulable getPool(PoolInfo poolInfo) {
    PoolSchedulable pool = nameToMap.get(poolInfo);
    if (pool == null) {
      pool = new PoolSchedulable(poolInfo, getType(), configManager);
      PoolSchedulable prevPool = nameToMap.putIfAbsent(poolInfo, pool);
      if (prevPool != null) {
        pool = prevPool;
      }
    }
    return pool;
  }

  /**
   * Get a snapshot of the pools that is generated from snapshot().
   *
   * @return An unmodifiable snapshot of the pools.
   */
  public Collection<PoolSchedulable> getPools() {
    return Collections.unmodifiableCollection(snapshotPools);
  }
}
