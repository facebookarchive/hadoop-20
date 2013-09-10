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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.hadoop.metrics.MetricsContext;
import org.apache.hadoop.metrics.MetricsRecord;
import org.apache.hadoop.metrics.MetricsUtil;
import org.apache.hadoop.metrics.Updater;
import org.apache.hadoop.metrics.util.MetricsBase;
import org.apache.hadoop.metrics.util.MetricsIntValue;
import org.apache.hadoop.metrics.util.MetricsLongValue;
import org.apache.hadoop.metrics.util.MetricsRegistry;
import org.apache.hadoop.metrics.util.MetricsTimeVaryingInt;
import org.apache.hadoop.metrics.util.MetricsTimeVaryingLong;

/**
 * Metrics for the Corona Cluster Manager.
 */
class ClusterManagerMetrics implements Updater {
  /** Metrics context name. */
  static final String CONTEXT_NAME = "clustermanager";
  /** The list of possible end states for a session */
  private static final List<SessionStatus> SESSION_END_STATES = getEndStates();
  /** Metrics context. */
  private final MetricsContext context;
  /** Metrics record. */
  private final MetricsRecord metricsRecord;
  /** Metrics registry. */
  private final MetricsRegistry registry = new MetricsRegistry();
  /** Number of requested resources by resource type. */
  private final Map<ResourceType, MetricsTimeVaryingLong>
  typeToResourceRequested;
  /** Number of granted resources by resource type. */
  private final Map<ResourceType, MetricsTimeVaryingLong> typeToResourceGranted;
  /** Number of revokes issued (preemption) by resource type. */
  private final Map<ResourceType, MetricsTimeVaryingLong> typeToResourceRevoked;
  /** Number of resources released by sessions, by resource type. */
  private final Map<ResourceType, MetricsTimeVaryingLong>
  typeToResourceReleased;
  /** Number of pending resource requests by resource type. */
  private final Map<ResourceType, MetricsIntValue> typeToPendingCount;
  /**
   * Number of running resource requests by resource type. This should be same
   * as the number of granted resources.
   */
  private final Map<ResourceType, MetricsIntValue> typeToRunningCount;
  /** Number of total slots by resource type. */
  private final Map<ResourceType, MetricsIntValue> typeToTotalSlots;
  /** Number of free slots by resource type. */
  private final Map<ResourceType, MetricsIntValue> typeToFreeSlots;
  /** Scheduler run time by resource type. */
  private final Map<ResourceType, MetricsIntValue> typeToSchedulerRunTime;
  /** The start time of the current run of the scheduler by resource type.
   * Contains a non 0 value if the cycle is in progress and 0 otherwise */
  private final Map<ResourceType, Long> typeToSchedulerCurrentCycleStart;
  /** Number of alive nodes. */
  private final MetricsIntValue aliveNodes;
  /** Number of dead nodes. */
  private final MetricsIntValue deadNodes;
  /** Number of blacklisted nodes. */
  private final MetricsIntValue blacklistedNodes;
  /** Breakdown of session by session status. */
  private final Map<SessionStatus, MetricsTimeVaryingInt>
  sessionStatusToMetrics;
  /** Number of running sessions. */
  private final MetricsIntValue numRunningSessions;
  /** Number of sessions since start of cluster manager. */
  private final MetricsTimeVaryingInt totalSessionCount;
  /** Number of pending calls to sessions. */
  private final MetricsIntValue pendingCallsCount;
  /** Number of CoronaJobTracker failures. */
  private final MetricsTimeVaryingInt numCJTFailures;
  /** Cluster manager scheduler for metrics */
  private Scheduler scheduler;
  /** Cluster manager session notifier for metrics. */
  private SessionNotifier sessionNotifier;
  /** Number of task tracker get restarted */
  private final MetricsIntValue numTaskTrackerRestarted;
  /** Number of remote job tracker timedout */
  private final MetricsIntValue numRemoteJTTimedout;

  /**
   * Constructor.
   * @param types The available resource types.
   */
  public ClusterManagerMetrics(Collection<ResourceType> types) {
    context = MetricsUtil.getContext(CONTEXT_NAME);
    metricsRecord = MetricsUtil.createRecord(context, CONTEXT_NAME);
    typeToResourceRequested = createTypeToResourceCountMap(types, "requested");
    typeToResourceGranted = createTypeToResourceCountMap(types, "granted");
    typeToResourceRevoked = createTypeToResourceCountMap(types, "revoked");
    typeToResourceReleased = createTypeToResourceCountMap(types, "released");
    typeToPendingCount = createTypeToCountMap(types, "pending");
    typeToRunningCount = createTypeToCountMap(types, "running");
    typeToTotalSlots = createTypeToCountMap(types, "total");
    typeToFreeSlots = createTypeToCountMap(types, "free");
    typeToSchedulerRunTime = createTypeToCountMap(types, "scheduler_runtime");
    typeToSchedulerCurrentCycleStart =
        new ConcurrentHashMap<ResourceType, Long>();
    sessionStatusToMetrics = createSessionStatusToMetricsMap();
    aliveNodes = new MetricsIntValue("alive_nodes", registry);
    deadNodes = new MetricsIntValue("dead_nodes", registry);
    blacklistedNodes = new MetricsIntValue("blacklisted_nodes", registry);
    numRunningSessions = new MetricsIntValue("num_running_sessions", registry);
    totalSessionCount = new MetricsTimeVaryingInt("total_sessions", registry);
    pendingCallsCount = new MetricsIntValue("num_pending_calls", registry);
    numCJTFailures = new MetricsTimeVaryingInt("num_cjt_failures", registry);
    numTaskTrackerRestarted = new MetricsIntValue("num_task_tracker_restarted", registry);
    numRemoteJTTimedout = new MetricsIntValue("num_remotejt_timedout", registry);
  }

  /**
   * Set the number of pending requests.
   * @param resourceType The resource type.
   * @param pending The number of pending requests.
   */
  public void setPendingRequestCount(ResourceType resourceType, int pending) {
    typeToPendingCount.get(resourceType).set(pending);
  }

  /**
   * Set the number of running resources.
   * @param resourceType The resource type.
   * @param running The number of running resources.
   */
  public void setRunningRequestCount(ResourceType resourceType, int running) {
    typeToRunningCount.get(resourceType).set(running);
  }

  /**
   * Set the number of total slots.
   * @param resourceType The resource type.
   * @param totalSlots The total number of slots.
   */
  public void setTotalSlots(ResourceType resourceType, int totalSlots) {
    typeToTotalSlots.get(resourceType).set(totalSlots);
  }

  /**
   * Set the number of free slots.
   * @param resourceType The resource type.
   * @param freeSlots The number of free slots.
   */
  public void setFreeSlots(ResourceType resourceType, int freeSlots) {
    typeToFreeSlots.get(resourceType).set(freeSlots);
  }

  public void setSchedulerRunTime(ResourceType resourceType, int runtime) {
    // This is called when the scheduling cycle is complete
    setSchedulerCurrentCycleStartTime(resourceType, 0);
    typeToSchedulerRunTime.get(resourceType).set(runtime);
  }

  public void setSchedulerCurrentCycleStartTime(ResourceType resourceType, long tstamp) {
    typeToSchedulerCurrentCycleStart.put(resourceType, tstamp);
  }

  /**
   * Record the request of a resource.
   * @param type The resource type.
   */
  public void requestResource(ResourceType type) {
    typeToResourceRequested.get(type).inc();
  }

  /**
   * Record the release of a resource.
   * @param type The resource type.
   */
  public void releaseResource(ResourceType type) {
    typeToResourceReleased.get(type).inc();
  }

  /**
   * Record the grant of a resource.
   * @param type The resource type.
   */
  public void grantResource(ResourceType type) {
    typeToResourceGranted.get(type).inc();
  }

  /**
   * Record the revoke of a resource.
   * @param type The resource type.
   */
  public void revokeResource(ResourceType type) {
    typeToResourceRevoked.get(type).inc();
  }

  /**
   * Set the number of alive nodes.
   * @param numAlive The number of alive nodes.
   */
  public void setAliveNodes(int numAlive) {
    aliveNodes.set(numAlive);
  }
  
  /**
   * num of task trackers get restarted
   * @param num The number of task trackers get restarted
   */
  public void restartTaskTracker(int num) {
    this.numTaskTrackerRestarted.inc(num);
  }

  /**
  /**
   * num of remote JT timedout
   * @param num The number of remote JT timedout
   */
  public void timeoutRemoteJT(int num) {
    this.numRemoteJTTimedout.inc(num);
  }

  /**
   * Set the number of dead nodes.
   * @param numDead The number of dead nodes.
   */
  public void setDeadNodes(int numDead) {
    deadNodes.set(numDead);
  }

  /**
   * Set the number of blacklisted nodes.
   * @param numBlacklisted The number of blacklisted nodes.
   */
  public void setBlacklistedNodes(int numBlacklisted) {
    blacklistedNodes.set(numBlacklisted);
  }

  /**
   * Set the number of running sessions.
   * @param num The number of running sessions.
   */
  public void setNumRunningSessions(int num) {
    numRunningSessions.set(num);
  }

  /**
   * Increment the number of sessions since the start of the cluster manager.
   */
  public void sessionStart() {
    totalSessionCount.inc();
  }

  /**
   * Record the end of a session.
   * @param finishState The state that the session finished in.
   */
  public void sessionEnd(SessionStatus finishState) {
    if (sessionStatusToMetrics.containsKey(finishState)) {
      sessionStatusToMetrics.get(finishState).inc();
    } else {
      throw new IllegalArgumentException("Invalid end state " + finishState);
    }
  }

  /**
   * Update the metric pending calls metric
   * @param numPendingCalls the number of calls in the queue
   * waiting to be sent out
   */
  public void setNumPendingCalls(int numPendingCalls) {
    pendingCallsCount.set(numPendingCalls);
  }

  /**
   * Records CoronaJobTracker failure.
   */
  public void recordCJTFailure() {
    numCJTFailures.inc();
  }

  /**
   * Set the scheduler and start the updating.  The metrics won't be reported
   * until this is called.
   *
   * @param scheduler Scheduler for this cluster manager.
   * @param sessionNotifier Session Notifier for this cluster manager.
   */
  public void registerUpdater(Scheduler scheduler,
      SessionNotifier sessionNotifier) {
    this.scheduler = scheduler;
    this.sessionNotifier = sessionNotifier;
    context.registerUpdater(this);
  }

  /**
   * Get all the possible end states (non running) of the session
   * @return The list of session status that are the non-running state
   */
  private static List<SessionStatus> getEndStates() {
    List<SessionStatus> endStatesRet = new ArrayList<SessionStatus>();
    for (SessionStatus s : SessionStatus.values()) {
      if (s != SessionStatus.RUNNING) {
        endStatesRet.add(s);
      }
    }
    return endStatesRet;
  }

  /**
   * Create a map of session status -> metrics.
   * @return the map.
   */
  private Map<SessionStatus, MetricsTimeVaryingInt>
  createSessionStatusToMetricsMap() {
    Map<SessionStatus, MetricsTimeVaryingInt> m =
      new HashMap<SessionStatus, MetricsTimeVaryingInt>();
    for (SessionStatus endState : SESSION_END_STATES) {
      String name = endState.toString().toLowerCase() + "_sessions";
      m.put(endState, new MetricsTimeVaryingInt(name, registry));
    }
    return m;
  }

  /**
   * Create a map of resource type -> current count.
   * @param resourceTypes The resource types.
   * @param actionType A string indicating pending, running etc.
   * @return The map.
   */
  private Map<ResourceType, MetricsIntValue> createTypeToCountMap(
      Collection<ResourceType> resourceTypes, String actionType) {
    Map<ResourceType, MetricsIntValue> m =
        new HashMap<ResourceType, MetricsIntValue>();
    for (ResourceType t : resourceTypes) {
      String name = (actionType + "_" + t).toLowerCase();
      MetricsIntValue value = new MetricsIntValue(name, registry);
      m.put(t, value);
    }
    return m;
  }

  /**
   * Create a map of resource type -> cumulative counts.
   * @param resourceTypes The resource types.
   * @param actionType A string indicating granted, revoked, etc.
   * @return The map.
   */
  private Map<ResourceType, MetricsTimeVaryingLong>
  createTypeToResourceCountMap(
      Collection<ResourceType> resourceTypes, String actionType) {
    Map<ResourceType, MetricsTimeVaryingLong> m =
        new HashMap<ResourceType, MetricsTimeVaryingLong>();
    for (ResourceType t : resourceTypes) {
      String name = (actionType + "_" + t).toLowerCase();
      MetricsTimeVaryingLong value = new MetricsTimeVaryingLong(name, registry);
      m.put(t, value);
    }
    return m;
  }

  @Override
  public void doUpdates(MetricsContext context) {
    // Get the fair scheduler metrics
    if (scheduler != null) {
      scheduler.submitMetrics(metricsRecord);
    }

    for (Map.Entry<ResourceType, Long> currStart :
        typeToSchedulerCurrentCycleStart.entrySet()) {
      long start = currStart.getValue();
      if (start > 0) {
        // This means that there's a scheduling cycle in progress.
        int currCycleRun = (int)(System.currentTimeMillis() - start);
        typeToSchedulerRunTime.get(currStart.getKey()).set(currCycleRun);
      }
    }

    // Get the number of pending calls.
    setNumPendingCalls(sessionNotifier.getNumPendingCalls());

    // Not synchronized on the ClusterManagerMetrics object.
    // The list of metrics in the registry is modified only in the constructor.
    // And pushMetrics() is thread-safe.
    for (MetricsBase m : registry.getMetricsList()) {
      m.pushMetric(metricsRecord);
    }

    metricsRecord.update();
  }

  public MetricsContext getContext() {
    return context;
  }
}
