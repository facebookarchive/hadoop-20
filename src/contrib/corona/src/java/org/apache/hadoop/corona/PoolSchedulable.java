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
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Manages sessions in a pool for a given type.
 */
public class PoolSchedulable extends Schedulable {
  /** Logger */
  private static final Log LOG = LogFactory.getLog(PoolSchedulable.class);
  /** The configuration manager */
  private final ConfigManager configManager;
  /** A lookup table from id to the session */
  private final Map<String, SessionSchedulable> idToSession;
  /** The snapshot of the SessionSchedulables */
  private final Collection<SessionSchedulable> snapshotSessions;
  /** The queue of sessions sorted for scheduling */
  private Queue<SessionSchedulable> scheduleQueue;
  /** The queue of sessions sorted for preemption */
  private Queue<SessionSchedulable> preemptQueue;
  /** Last time the pool was above minimum allocation */
  private long lastTimeAboveMinimum;
  /** Last time the pool was above starving share */
  private long lastTimeAboveStarvingShare;
  /** Last time the pool was preempted */
  private long lastPreemptTime;
  /** A flag to indicate that the pool needs its share redistributed */
  private boolean needRedistributed;
  /** The max allocation for the pool */
  private int maximum;
  /** The minimum allocation for the pool */
  private int minimum;
  /** The weight of the pool */
  private double weight;
  /** The ratio for starving */
  private double shareStarvingRatio;
  /** Is this pool preemptable or not */
  private boolean preemptable;
  /** The minimum amount of time to wait between preemptions */
  private long minPreemptPeriod;
  /** The amount of time the pool has been starving for the share */
  private long starvingTimeForShare;
  /** The amount of time the pool has been starving for the minimum */
  private long starvingTimeForMinimum;
  /** Pool info names for this pool */
  private final PoolInfo poolInfo;
  /** Priority for this pool */
  private int priority;

  /**
   * Create a PoolSchedulable for a given pool and a resource type with a
   * provided configuration manager
   * @param poolInfo Pool info for this pool
   * @param type the resource type of this pool
   * @param configManager the configuration manager for this pool
   */
  public PoolSchedulable(PoolInfo poolInfo,
      ResourceType type, ConfigManager configManager) {
    super(PoolInfo.createStringFromPoolInfo(poolInfo), type);
    this.poolInfo = poolInfo;
    this.configManager = configManager;
    this.scheduleQueue = null;
    this.preemptQueue = null;
    this.snapshotSessions = new ArrayList<SessionSchedulable>();
    this.lastTimeAboveMinimum = -1L;
    this.lastTimeAboveStarvingShare = -1L;
    this.needRedistributed = true;

    // This needs to be thread safe because addSession() and getSortedSessions()
    // are called from different threads
    idToSession = new ConcurrentHashMap<String, SessionSchedulable>();
  }

  public PoolInfo getPoolInfo() {
    return poolInfo;
  }

  @Override
  public void snapshot() {
    granted = 0;
    requested = 0;
    pending = 0;
    snapshotSessions.clear();
    Set<String> toBeDeleted = new HashSet<String>();
    for (Entry<String, SessionSchedulable> entry : idToSession.entrySet()) {
      SessionSchedulable schedulable = entry.getValue();
      Session session = entry.getValue().getSession();
      synchronized (session) {
        if (session.isDeleted()) {
          toBeDeleted.add(entry.getKey());
        } else {
          schedulable.snapshot();
          snapshotSessions.add(schedulable);
          granted += schedulable.getGranted();
          requested += schedulable.getRequested();
          pending += schedulable.getPending();
        }
      }
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("Snapshot for pool " + getName() + ":" +
          getType() + " {requested = " + requested +
        ", pending = " + pending +
        ", granted = " + granted + "}");
    }
    for (String id : toBeDeleted) {
      idToSession.remove(id);
    }
    snapshotConfig();
    scheduleQueue = null;
    preemptQueue = null;
    needRedistributed = true;
  }

  /**
   * Get the snapshot of the configuration from the configuration manager.
   * Synchronized on config manager to ensure that config is atomically updated
   * per pool.
   */
  private void snapshotConfig() {
    synchronized (configManager) {
      maximum = configManager.getPoolMaximum(poolInfo, getType());
      minimum = configManager.getPoolMinimum(poolInfo, getType());
      weight = configManager.getWeight(poolInfo);
      priority = configManager.getPriority(poolInfo);
      preemptable = configManager.isPoolPreemptable(poolInfo);
      shareStarvingRatio = configManager.getShareStarvingRatio();
      minPreemptPeriod = configManager.getMinPreemptPeriod();
      starvingTimeForShare = configManager.getStarvingTimeForShare();
      starvingTimeForMinimum = configManager.getStarvingTimeForMinimum();
    }
  }

  @Override
  public int getMaximum() {
    return maximum;
  }

  @Override
  public int getMinimum() {
    return minimum;
  }

  @Override
  public double getWeight() {
    return weight;
  }

  @Override
  public long getDeadline() {
    // Pools have no deadlines
    return -1L;
  }

  @Override
  public int getPriority() {
    return priority;
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
   * Add a session to the pool
   * @param id the id of the session
   * @param session the session to add to the pool
   */
  public void addSession(String id, Session session) {
    synchronized (session) {
      SessionSchedulable schedulable =
        new SessionSchedulable(session, getType());
      idToSession.put(id, schedulable);
    }
  }

  /**
   * Get the queue of sessions sorted for scheduling
   * @return the queue of sessions sorted for scheduling
   */
  public Queue<SessionSchedulable> getScheduleQueue() {
    if (scheduleQueue == null) {
      scheduleQueue =
          createSessionQueue(configManager.getPoolComparator(poolInfo));
    }
    return scheduleQueue;
  }

  /**
   * Get the queue of sessions sorted for preemption
   * @return the queue of sessions sorted for preemption
   */
  public Queue<SessionSchedulable> getPreemptQueue() {
    if (preemptQueue == null) {

      ScheduleComparator comparator = null;
      switch (configManager.getPoolComparator(poolInfo)) {
      case FIFO:
        comparator = ScheduleComparator.FIFO_PREEMPT;
        break;
      case FAIR:
        comparator = ScheduleComparator.FAIR_PREEMPT;
        break;
      case DEADLINE:
        comparator = ScheduleComparator.DEADLINE_PREEMPT;
        break;
      default:
        throw new IllegalArgumentException("Unknown comparator");
      }

      preemptQueue = createSessionQueue(comparator);
    }
    return preemptQueue;
  }

  /**
   * Get the queue of sessions in the pool sorted by comparator
   * @param comparator the comparator to use when sorting sessions
   * @return the queue of the sessions sorted by a given comparator
   */
  public Queue<SessionSchedulable> createSessionQueue(
      ScheduleComparator comparator) {
    int initCapacity = snapshotSessions.size() == 0 ?
        1 : snapshotSessions.size();
    Queue<SessionSchedulable> sessionQueue =
      new PriorityQueue<SessionSchedulable>(initCapacity, comparator);
    sessionQueue.addAll(snapshotSessions);
    return sessionQueue;
  }

  /**
   * Distribute the pool's share between the sessions in the pool
   */
  public void distributeShare() {
    if (needRedistributed) {
      ScheduleComparator comparator =
          configManager.getPoolComparator(poolInfo);
      Schedulable.distributeShare(getShare(), snapshotSessions, comparator);
    }
    needRedistributed = false;
  }

  /**
   * Check if the pool is starving now or not
   * @param now current timestampe
   * @return true if the pool is starving now, false otherwise
   */
  public boolean isStarving(long now) {
    double starvingShare = getShare() * shareStarvingRatio;
    if (getGranted() >= Math.ceil(starvingShare)) {
      lastTimeAboveStarvingShare = now;
    }

    if (getGranted() >= Math.min(getShare(), getMinimum())) {
      lastTimeAboveMinimum = now;
    }
    if (now - lastPreemptTime < getMinPreemptPeriod()) {
      // Prevent duplicate preemption
      return false;
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("Pool:" + getName() +
          " lastTimeAboveMinimum:" + lastTimeAboveMinimum +
          " lastTimeAboveStarvingShare:" + lastTimeAboveStarvingShare +
          " minimumStarvingTime:" + getMinimumStarvingTime(now) +
          " shareStarvingTime:" + getShareStarvingTime(now) +
          " starvingTime:" + getStarvingTime(now));
    }
    if (getMinimumStarvingTime(now) >= 0) {
      LOG.info("Pool:" + getName() + " for type:" + getType() +
          " is starving min:" + getMinimum() +
          " granted:" + getGranted());
      lastPreemptTime = now;
      return true;
    }
    if (getShareStarvingTime(now) >= 0) {
      LOG.info("Pool:" + getName() + " for type:" + getType() +
          " is starving share:" + getShare() +
          " starvingRatio:" + shareStarvingRatio +
          " starvingShare:" + starvingShare +
          " granted:" + getGranted());
      lastPreemptTime = now;
      return true;
    }
    return false;
  }

  /**
   * Get the amount of time the pool was starving for either its min share
   * or its share
   * @param now the current timestamp
   * @return the amount of time the pool was starving
   */
  public long getStarvingTime(long now) {
    long starvingTime = Math.max(
        getMinimumStarvingTime(now), getShareStarvingTime(now));
    return starvingTime;
  }

  /**
   * @return The minimum time between preemptions.
   */
  public long getMinPreemptPeriod() {
    return minPreemptPeriod;
  }

  /**
   * Get the amount of time the pool was starving for its min share
   * @param now the current timestamp
   * @return the amount of time the pool was starving for the min share
   */
  private long getMinimumStarvingTime(long now) {
    return (now - lastTimeAboveMinimum) - starvingTimeForMinimum;
  }

  /**
   * Get the amount of time the pool was starving for its share
   * @param now the current timestamp
   * @return the amount of time the pool was starving for share
   */
  private long getShareStarvingTime(long now) {
    return (now - lastTimeAboveStarvingShare) - starvingTimeForShare;
  }

  /**
   * Is the pool preemptable or not
   * @return true if the pool is preemptable false otherwise
   */
  public boolean isPreemptable() {
    return preemptable;
  }
}
