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
 * Manages pool groups and pools for a given type. Needs to be thread safe
 * because addSession() and getPools() can be called from different threads.
 */
public class PoolGroupManager {
  /** Default pool group */
  public static final String DEFAULT_POOL_GROUP = "default";
  /** Default pool */
  public static final String DEFAULT_POOL = "defaultpool";
  /** Default pool info */
  public static final PoolInfo DEFAULT_POOL_INFO =
      new PoolInfo(DEFAULT_POOL_GROUP, DEFAULT_POOL);
  /** Logger */
  private static final Log LOG = LogFactory.getLog(PoolGroupManager.class);
  /** The resource being managed by this PoolMnanager */
  private final ResourceType type;
  /**
   * A lookup table for the pool groups.  Thread-safe for addSession() and
   * getPoolGroups() by since it's a concurrent hash map and pool groups are
   * never deleted.
   */
  private final ConcurrentHashMap<String, PoolGroupSchedulable>
  nameToPoolGroup = new ConcurrentHashMap<String, PoolGroupSchedulable>();
  /** Config manager */
  private final ConfigManager configManager;
  /** Configuration */
  private final CoronaConf conf;
  /** The list of pool group snapshots used for scheduling */
  private Collection<PoolGroupSchedulable> snapshotPoolGroups;
  /** The queue of pool groups for scheduling */
  private Queue<PoolGroupSchedulable> scheduleQueue;
  /** The queue of pool groups for preemption */
  private Queue<PoolGroupSchedulable> preemptQueue;

  /**
   * Create a PoolGroupManager for a given {@link ResourceType}
   * with a given {@link ConfigManager}
   * @param type the type of resource to manage
   * @param configManager the configuration for this PoolManager
   * @param conf Static configuration
   */
  public PoolGroupManager(ResourceType type, ConfigManager configManager,
                          CoronaConf conf) {
    this.type = type;
    this.configManager = configManager;
    this.conf = conf;
  }

  /**
   * Take snapshots for all pools groups and sessions.
   */
  public void snapshot() {
    snapshotPoolGroups =
        new ArrayList<PoolGroupSchedulable>(nameToPoolGroup.values());
    for (PoolGroupSchedulable poolGroup : snapshotPoolGroups) {
      poolGroup.snapshot();
    }
    scheduleQueue = null;
    preemptQueue = null;

    // Load the configured pools for stats and cm.jsp
    // (needs to modify nameToPoolGroup)
    Collection<PoolInfo> configuredPoolInfos =
        configManager.getConfiguredPoolInfos();
    if (configuredPoolInfos != null) {
      for (PoolInfo poolInfo : configuredPoolInfos) {
        getPoolSchedulable(poolInfo);
      }
    }
  }

  /**
   * Get the queue of pool groups sorted for scheduling
   * @return the queue of pools sorted for scheduling
   */
  public Queue<PoolGroupSchedulable> getScheduleQueue() {
    if (scheduleQueue == null) {
      scheduleQueue = createPoolGroupQueue(ScheduleComparator.FAIR);
    }
    return scheduleQueue;
  }

  /**
   * Get the queue of the pool groups sorted for preemption
   * @return the queue of pool sorted for preemption
   */
  public Queue<PoolGroupSchedulable> getPreemptQueue() {
    if (preemptQueue == null) {
      preemptQueue = createPoolGroupQueue(ScheduleComparator.FAIR_PREEMPT);
    }
    return preemptQueue;
  }

  /**
   * Put all the pool groups into the priority queue sorted by a comparator
   * @param comparator the comparator to sort all the pool groups in the queue
   * @return the queue of the pool groups sorted by a comparator
   */
  private Queue<PoolGroupSchedulable> createPoolGroupQueue(
      ScheduleComparator comparator) {
    int initCapacity =
        snapshotPoolGroups.size() == 0 ? 1 : snapshotPoolGroups.size();
    Queue<PoolGroupSchedulable> poolGroupQueue =
        new PriorityQueue<PoolGroupSchedulable>(initCapacity, comparator);
    poolGroupQueue.addAll(snapshotPoolGroups);
    return poolGroupQueue;
  }

  /**
   * Add a session to the scheduler
   * @param id the id of the session
   * @param session the session object to add
   */
  public void addSession(String id, Session session) {
    PoolInfo poolInfo = getPoolInfo(session);
    LOG.info("Session " + id + " added to pool info " +
        poolInfo + " (originally " + session.getInfo().getPoolInfoStrings() +") for " + type);
    getPoolSchedulable(poolInfo).addSession(id, session);
  }

  /**
   * If the cluster is set to configured pools only, do not allow unset pool
   * information or pool info that doesn't match a valid pool info.  Throws
   * an InvalidSessionHandle exception in either of the failure cases.
   *
   * @param poolInfo Pool info to check
   * @param configManager Configuration to check
   * @param conf Corona configuration to check if configured pools
   * @throws InvalidSessionHandle
   */
  public static void checkPoolInfoIfStrict(PoolInfo poolInfo,
                                           ConfigManager configManager,
                                           CoronaConf conf)
      throws InvalidSessionHandle {
    if (!conf.onlyAllowConfiguredPools()) {
      return;
    }

    // When only allowing configured pools, check the pool name to ensure
    // it is a configured pool name.  Not setting the pool info is also
    // invalid.  A legal name must be specified.

    if (poolInfo == null) {
      throw new InvalidSessionHandle("This cluster is operating in " +
          "configured pools only mode.  The pool group " +
          "and pool was not specified.  Please use the Corona parameter " +
          CoronaConf.EXPLICIT_POOL_PROPERTY + " to set a valid poolgroup and " +
          "pool in the format '<poolgroup>.<pool>'");
    }

    if (!configManager.isConfiguredPoolInfo(poolInfo)) {
      throw new InvalidSessionHandle("This cluster is operating in " +
          "configured pools only mode.  The pool group " +
          "and pool was specified as '" + poolInfo.getPoolGroupName() +
          "." + poolInfo.getPoolName() +
          "' and is not part of this cluster.  " +
          "Please use the Corona parameter " +
          CoronaConf.EXPLICIT_POOL_PROPERTY + " to set a valid pool " +
          "group and pool in the format <poolgroup>.<pool>");
    }

    if (!PoolInfo.isLegalPoolInfo(poolInfo)) {
      throw new InvalidSessionHandle("This cluster is operating in " +
          "configured pools only mode.  The pool group " +
          "and pool was specified as '" + poolInfo.getPoolGroupName() +
          "." + poolInfo.getPoolName() +
          "' and has illegal characters (Something not in " +
          PoolInfo.INVALID_REGEX + ").  Please use the Corona parameter " +
          CoronaConf.EXPLICIT_POOL_PROPERTY + " to set a valid pool " +
          "group and pool in the format <poolgroup>.<pool>");
    }
  }

  /**
   * Get the pool name for a given session, using the default pool
   * information if the name is illegal.  Redirection should happen prior to
   * this.
   *
   * @param session the session to get the pool name for
   * @return the pool info that the session is running in
   */
  public static PoolInfo getPoolInfo(
      Session session) {
    PoolInfo poolInfo = session.getPoolInfo();

    // If there is no explicit pool info set, take user name.
    if (poolInfo == null || poolInfo.getPoolName().equals("")) {
      poolInfo = new PoolInfo(DEFAULT_POOL_GROUP, session.getUserId());
    }

    if (!PoolInfo.isLegalPoolInfo(poolInfo)) {
      LOG.warn("Illegal pool info :" + poolInfo +
          " from session " + session.getSessionId());
      return DEFAULT_POOL_INFO;
    }
    return poolInfo;
  }

  /**
   * Get the Schedulable representing the pool with a given name
   * If it doesn't exist - create it and add it to the list
   *
   * @param poolInfo Group and pool names
   * @return the PoolSchedulable for a pool with a given name
   */
  private PoolSchedulable getPoolSchedulable(PoolInfo poolInfo) {
    PoolGroupSchedulable poolGroup =
        nameToPoolGroup.get(poolInfo.getPoolGroupName());
    if (poolGroup == null) {
      poolGroup = new PoolGroupSchedulable(
          poolInfo.getPoolGroupName(), type, configManager);
      PoolGroupSchedulable prevPoolGroup =
          nameToPoolGroup.putIfAbsent(poolInfo.getPoolGroupName(), poolGroup);
      if (prevPoolGroup != null) {
        poolGroup = prevPoolGroup;
      }
    }

    return poolGroup.getPool(poolInfo);
  }

  /**
   * Distribute the share between the pool groups
   * @param total the share to distribute
   */
  public void distributeShare(int total) {
    Schedulable.distributeShare(
        total, snapshotPoolGroups, ScheduleComparator.FAIR);
  }

  /**
   * Get a snapshot of the pool groups that is generated from snapshot().
   *
   * @return An unmodifiable snapshot of the pool groups.
   */
  public Collection<PoolGroupSchedulable> getPoolGroups() {
    return Collections.unmodifiableCollection(snapshotPoolGroups);
  }
}
