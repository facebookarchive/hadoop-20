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
import java.util.EnumMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapred.PoolFairnessCalculator;
import org.apache.hadoop.mapred.PoolMetadata;
import org.apache.hadoop.mapred.ResourceMetadata;
import org.apache.hadoop.metrics.MetricsRecord;

/**
 * Schedules the sessions with the various resources available.
 */
public class Scheduler {
  /** Class logger */
  public static final Log LOG = LogFactory.getLog(Scheduler.class);
  /** Map of resource type to particular scheduler */
  private final Map<ResourceType, SchedulerForType> schedulersForTypes;
  /** Reloadable configuration manager */
  private final ConfigManager configManager;
  /** Types of resources */
  private final Collection<ResourceType> types;
  /** Static configuration */
  private CoronaConf conf;

  /**
   * Primary constructor.
   *
   * @param nodeManager Manages the nodes and their resources
   * @param sessionManager Manages a collection of sessions
   * @param sessionNotifier Delivers notifications to sessions asynchronously
   * @param types Types of resources
   * @param metrics Cluster Manager metrics
   * @param conf Static configuration
   */
  public Scheduler(
    NodeManager nodeManager,
    SessionManager sessionManager,
    SessionNotifier sessionNotifier,
    Collection<ResourceType> types,
    ClusterManagerMetrics metrics,
    CoronaConf conf) {
    this(nodeManager, sessionManager, sessionNotifier, types, metrics, conf,
        new ConfigManager(types, conf));
  }

  /**
   * Used by unit test to fake the ConfigManager
   *
   * @param nodeManager Manages the nodes and their resources
   * @param sessionManager Manages a collection of sessions
   * @param sessionNotifier Delivers notifications to sessions asynchronously
   * @param types Types of resources
   * @param metrics Cluster Manager metrics.
   * @param configManager Manages the reloadable configuration
   * @param conf Static configuration
   */
  public Scheduler(
      NodeManager nodeManager,
      SessionManager sessionManager,
      SessionNotifier sessionNotifier,
      Collection<ResourceType> types,
      ClusterManagerMetrics metrics,
      CoronaConf conf,
      ConfigManager configManager) {
    this.configManager = configManager;
    this.schedulersForTypes =
        new EnumMap<ResourceType, SchedulerForType>(ResourceType.class);
    this.types = types;
    for (ResourceType type : types) {
      SchedulerForType schedulerForType =
        new SchedulerForType(
          type,
          sessionManager,
          sessionNotifier,
          nodeManager,
          configManager,
          metrics,
          conf);
      schedulerForType.setDaemon(true);
      schedulerForType.setName("Scheduler-" + type);
      schedulersForTypes.put(type, schedulerForType);
    }
    this.conf = conf;
  }

  public ConfigManager getConfigManager() {
    return configManager;
  }

  /**
   * Add a session for scheduling.
   *
   * @param id Identifies the session
   * @param session Actual session to be scheduled
   */
  public void addSession(String id, Session session) {
    for (SchedulerForType scheduleThread : schedulersForTypes.values()) {
      scheduleThread.addSession(id, session);
    }
  }

  /**
   * Start the scheduling as well as the reloadable configuration manager.
   */
  public void start() {
    for (Thread schedulerForType : schedulersForTypes.values()) {
      LOG.info("Starting " + schedulerForType.getName());
      schedulerForType.start();
    }
    configManager.start();
  }

  public void setConf(CoronaConf conf) {
    this.conf = conf;
  }

  public CoronaConf getConf() {
    return conf;
  }

  /**
   * Stop the scheduling and disable the reloadable configuration manager.
   */
  public void close() {
    for (SchedulerForType scheduleThread : schedulersForTypes.values()) {
      scheduleThread.close();
    }
    for (Thread scheduleThread : schedulersForTypes.values()) {
      Utilities.waitThreadTermination(scheduleThread);
    }
    configManager.close();
  }

  /**
   * Notify the scheduler threads that the state has changed and
   * scheduling should occur.
   */
  public void notifyScheduler() {
    for (SchedulerForType scheduleThread : schedulersForTypes.values()) {
      synchronized (scheduleThread) {
        scheduleThread.notifyAll();
      }
    }
  }

  /**
   * Get an unmodifiable snapshot of the mapping from pool info to associated
   * metrics.
   *
   * @param type Type of resource.
   * @return Unmodifiable snapshot of mapping from pool info to metrics
   */
  public Map<PoolInfo, PoolInfoMetrics> getPoolInfoMetrics(ResourceType type) {
    return schedulersForTypes.get(type).getPoolInfoMetrics();
  }

  /**
   * Get a snapshot of the {@link PoolMetadata} objects for all the schedulers
   * for each resource type.  This is used for gathering metrics.
   *
   * @return List of snapshots for each pool and its resources
   */
  private List<PoolMetadata> getPoolMetadataList() {
    Map<String, PoolMetadata> poolNameMetadataMap =
        new HashMap<String, PoolMetadata>();
    for (Map.Entry<ResourceType, SchedulerForType> schedulerEntry :
        schedulersForTypes.entrySet()) {
      for (Map.Entry<PoolInfo, PoolInfoMetrics> poolEntry :
          schedulerEntry.getValue().getPoolInfoMetrics().entrySet()) {
        ResourceMetadata resourceMetadata =
            poolEntry.getValue().getResourceMetadata();
        // Ignore any invalid pool metrics
        if (resourceMetadata == null) {
          continue;
        }
        String stringifiedPoolInfo =
            PoolInfo.createStringFromPoolInfo(poolEntry.getKey());
        PoolMetadata poolMetadata =
            poolNameMetadataMap.get(stringifiedPoolInfo);
        if (poolMetadata == null) {
          poolMetadata = new PoolMetadata(stringifiedPoolInfo);
          poolNameMetadataMap.put(stringifiedPoolInfo, poolMetadata);
        }
        poolMetadata.addResourceMetadata(schedulerEntry.getKey().toString(),
                                         resourceMetadata);
      }
    }
    return new ArrayList<PoolMetadata>(poolNameMetadataMap.values());
  }

  /**
   * Get a snapshot of the pool infos that is sorted.
   *
   * @return Sorted snapshot of the pool infos.
   */
  public List<PoolInfo> getPoolInfos() {
    Set<PoolInfo> poolInfos = new HashSet<PoolInfo>();
    for (ResourceType type : types) {
      poolInfos.addAll(getPoolInfoMetrics(type).keySet());
    }
    List<PoolInfo> result = new ArrayList<PoolInfo>();
    result.addAll(poolInfos);
    Collections.sort(result);
    return result;
  }

  /**
   * Submit the metrics.
   *
   * @param metricsRecord Where the metrics will be submitted
   */
  public void submitMetrics(MetricsRecord metricsRecord) {
    List<PoolMetadata> poolMetadatas = getPoolMetadataList();
    PoolFairnessCalculator.calculateFairness(poolMetadatas, metricsRecord);
    for (SchedulerForType scheduler: schedulersForTypes.values()) {
      scheduler.submitMetrics();
    }
  }
}
