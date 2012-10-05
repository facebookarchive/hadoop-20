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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Manages pools for a given type. Needs to be thread safe because addSession()
 * and getSortedPools() can be called from different threads.
 */
public class PoolManager {
  /** Logger */
  public static final Log LOG = LogFactory.getLog(PoolManager.class);
  /** The name for the default pool */
  public static final String DEFAULT_POOL_NAME = "default";
  /** The resource being managed by this PoolMnanager */
  private final ResourceType type;
  /** A lookup table for the pools */
  private final ConcurrentHashMap<String, PoolSchedulable> nameToPool;
  /** Config manager */
  private final ConfigManager configManager;
  /** Configuration */
  private final CoronaConf conf;
  /** The list of pool snapshots used for scheduling */
  private Collection<PoolSchedulable> snapshotPools;
  /** The queue of pools for scheduling */
  private Queue<PoolSchedulable> scheduleQueue;
  /** The queue of pools for preemption */
  private Queue<PoolSchedulable> preemptQueue;

  /**
   * Create a PoolManager for a given {@link ResourceType}
   * with a given {@link ConfigManager}
   * @param type the type of resource to manage
   * @param configManager the configuration for this PoolManager
   * @param conf Static configuration
   */
  public PoolManager(ResourceType type, ConfigManager configManager,
                     CoronaConf conf) {
    this.type = type;
    this.configManager = configManager;
    this.conf = conf;

    // This needs to be thread safe because addSession() and getSortedPools()
    // are called from different threads
    this.nameToPool = new ConcurrentHashMap<String, PoolSchedulable>();
  }

  /**
   * Take snapshots for all pools and sessions
   */
  public void snapshot() {
    snapshotPools = new ArrayList<PoolSchedulable>(nameToPool.values());
    for (PoolSchedulable pool : snapshotPools) {
      pool.snapshot();
    }
    scheduleQueue = null;
    preemptQueue = null;
  }

  /**
   * Get the queue of pools sorted for scheduling
   * @return the queue of pools sorted for scheduling
   */
  public Queue<PoolSchedulable> getScheduleQueue() {
    if (scheduleQueue == null) {
      scheduleQueue = createPoolQueue(ScheduleComparator.FAIR);
    }
    return scheduleQueue;
  }

  /**
   * Get the queue of the pools sorted for preemption
   * @return the queue of pool sorted for preemption
   */
  public Queue<PoolSchedulable> getPreemptQueue() {
    if (preemptQueue == null) {
      preemptQueue = createPoolQueue(ScheduleComparator.FAIR_PREEMPT);
    }
    return preemptQueue;
  }

  /**
   * Put all the pools into the priority queue sorted by a comparator
   * @param comparator the comparator to sort all the pools in the queue by
   * @return the queue of the pools sorted by a comparator
   */
  public Queue<PoolSchedulable> createPoolQueue(ScheduleComparator comparator) {
    int initCapacity = snapshotPools.size() == 0 ? 1 : snapshotPools.size();
    Queue<PoolSchedulable> poolQueue = new PriorityQueue<PoolSchedulable>(
        initCapacity, comparator);
    poolQueue.addAll(snapshotPools);
    return poolQueue;
  }

  /**
   * Add a session to the scheduler
   * @param id the id of the session
   * @param session the session object to add
   */
  public void addSession(String id, Session session) {
    String poolName = getPoolName(session, configManager, conf);
    LOG.info("Session " + id + " added to pool " + poolName + " for " + type);
    getPoolSchedulable(poolName).addSession(id, session);
  }

  /**
   * Get the pool name for a given session, restricting to the configured
   * pools and the default pool.
   *
   * @param session the session to get the pool name for
   * @param configManager Config manager used to see if the pool is configured
   * @param conf Static configuration
   * @return the name of the pool the session is running in
   */
  public static String getPoolName(Session session,
                                   ConfigManager configManager,
                                   CoronaConf conf) {
    String poolName = session.getPoolId();
    // If there is no explicit pool name, take user name.
    if (poolName == null || poolName.equals("")) {
      poolName = session.getUserId();
    }
    // When only allowing configured pools, check the pool name to ensure
    // it is a configured pool name.
    if (poolName != null && conf.onlyAllowConfiguredPools() && !configManager
        .isConfiguredPool(poolName)) {
      poolName = null;
    }
    // If pool name still is not configured or was not set, use default.
    if (poolName == null || poolName.equals("")) {
      return DEFAULT_POOL_NAME;
    }
    if (!isLegalPoolName(poolName)) {
      LOG.warn("Bad poolName:" + poolName +
          " from session:" + session.getSessionId());
      return DEFAULT_POOL_NAME;
    }
    return poolName;
  }

  /**
   * Returns whether or not the given pool name is legal.
   *
   * Legal pool names are of nonzero length and are formed only of alphanumeric
   * characters, underscores (_), and hyphens (-).
   * @param poolName the name of the pool to check
   * @return true if the name is a valid pool name, false otherwise
   */
  private static boolean isLegalPoolName(String poolName) {
    return !poolName.matches(".*[^0-9a-z\\-\\_].*") &&
      (poolName.length() > 0);
  }

  /**
   * Get the Schedulable representing the pool with a given name
   * If it doesn't exist - create it and att it to the list
   *
   * @param name the name of the pool
   * @return the PoolSchedulable for a pool with a given name
   */
  private PoolSchedulable getPoolSchedulable(String name) {
    PoolSchedulable poolSchedulable = nameToPool.get(name);
    if (poolSchedulable == null) {
      poolSchedulable = new PoolSchedulable(name, type, configManager);
      // Note that we never remove pools from this map
      PoolSchedulable existingPool =
        nameToPool.putIfAbsent(name, poolSchedulable);
      if (existingPool != null) {
        // The pool was already created and added, just return it
        return existingPool;
      }
    }
    return poolSchedulable;
  }

  /**
   * Distribute the share between the pools
   * @param total the share to distribute
   */
  public void distributeShare(int total) {
    Schedulable.distributeShare(
        total, snapshotPools, ScheduleComparator.FAIR);
  }

  public Collection<PoolSchedulable> getPools() {
    return Collections.unmodifiableCollection(snapshotPools);
  }
}
