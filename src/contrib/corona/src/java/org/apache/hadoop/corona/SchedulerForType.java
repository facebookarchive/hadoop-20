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

import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Queue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.corona.PoolInfoMetrics.MetricName;
import org.apache.hadoop.metrics.MetricsRecord;
import org.apache.hadoop.net.Node;

/**
 * Scheduler thread which matches requests to nodes for the given resource type
 */
public class SchedulerForType extends Thread {
  /** Maximum amount of milliseconds to wait before scheduling */
  public static final long SCHEDULE_PERIOD = 100L;
  /** Milliseconds to wait before preempting tasks */
  public static final long PREEMPTION_PERIOD = 1000L;
  /** Class logger */
  private static final Log LOG = LogFactory.getLog(SchedulerForType.class);
  /** Type of resource to schedule for */
  private final ResourceType type;
  /** Manages the pools groups for this resource type */
  private final PoolGroupManager poolGroupManager;
  /** Used to grant/revoke resources to the sessions */
  private final SessionManager sessionManager;
  /** Asynchronously notify the sessions about changes */
  private final SessionNotifier sessionNotifier;
  /** Find where to run the resources */
  private final NodeManager nodeManager;
  /** Get up-to-date dynamic configuration */
  private final ConfigManager configManager;
  /** Map of pool name to associated metrics. The map is replaced each time
   * scheduling happens, but once replaced, the map is not updated. */
  private volatile Map<PoolInfo, PoolInfoMetrics> poolInfoToMetrics =
      new HashMap<PoolInfo, PoolInfoMetrics>();
  /** Map of pool name -> metrics record. */
  private final Map<PoolInfo, MetricsRecord> poolInfoToMetricsRecord =
    new HashMap<PoolInfo, MetricsRecord>();
  /** A flag indicating that the scheduler has allocated all resources */
  private boolean fullyScheduled = true;
  /** Run until told to shutdown */
  private volatile boolean shutdown = false;
  /** Saved last preemption time, used with PREEMPTION_PREIOD */
  private long lastPreemptionTime = -1L;
  /** Do we want preemption. */
  private final boolean enablePreemption;
  /** Required locality levels for this type. */
  private final List<LocalityLevel> neededLocalityLevels;
  /** Cluster Manager metrics. */
  private final ClusterManagerMetrics metrics;
  /** The node snapshot. */
  private NodeSnapshot nodeSnapshot;

  /**
   * Constructor.
   *
   * @param type Type of resource to schedule
   * @param sessionManager Manager of sessions
   * @param sessionNotifier Asynchronous session notification
   * @param nodeManager Available resources on nodes
   * @param configManager Dynamic thread safe configuration
   * @param metrics Cluster Manager metrics
   * @param conf Static configuration
   */
  public SchedulerForType(
    ResourceType type,
    SessionManager sessionManager,
    SessionNotifier sessionNotifier,
    NodeManager nodeManager,
    ConfigManager configManager,
    ClusterManagerMetrics metrics,
    CoronaConf conf) {
    this.type = type;
    this.poolGroupManager = new PoolGroupManager(type, configManager, conf);
    this.sessionManager = sessionManager;
    this.sessionNotifier = sessionNotifier;
    this.nodeManager = nodeManager;
    this.configManager = configManager;
    this.enablePreemption = ResourceTypeProperties.canBePreempted(type);
    this.neededLocalityLevels =
      ResourceTypeProperties.neededLocalityLevels(type);
    this.metrics = metrics;
  }

  @Override
  public void run() {
    while (!shutdown) {
      try {
        if (nodeManager.clusterManager.safeMode) {
          // If the Cluster Manager is in Safe Mode, we will not change
          // anything
          Thread.sleep(1000);
          continue;
        }

        if (fullyScheduled) {
          waitForNotification();
        }

        long start = System.currentTimeMillis();
        metrics.setSchedulerCurrentCycleStartTime(type, start);
        poolGroupManager.snapshot();

        boolean scheduleFromNodeToSession =
          configManager.getScheduleFromNodeToSession();
        if (scheduleFromNodeToSession) {
          nodeSnapshot = nodeManager.getNodeSnapshot(type);
        } else {
          nodeSnapshot = null;
        }

        Map<String, List<ResourceGrant>> sessionIdToGranted = scheduleTasks();

        dispatchGrantedResource(sessionIdToGranted);

        if (enablePreemption && fullyScheduled) {
          doPreemption();
        }
        int runtime = (int) (System.currentTimeMillis() - start);
        if (runtime > 50) {
          int scheduled = 0;
          float speed = 0;
          for (List<?> grants : sessionIdToGranted.values()) {
            scheduled += grants.size();
          }
          if (scheduled > 0) {
            speed = ((float) runtime) / scheduled;
          }
          // Log if more than 50 msec.
          LOG.info("Scheduler runtime for " + type + " " + runtime + " ms / " +
              scheduled + " grants = " + speed);
        }
        metrics.setSchedulerRunTime(type, runtime);

        collectPoolInfoMetrics();

      } catch (InterruptedException e) {
        if (!shutdown) {
          LOG.warn("Unexpected InterruptedException");
        }
      }
    }
  }

  /**
   * Update the pool metrics. The update is atomic at the map level.
   */
  private void collectPoolInfoMetrics() {
    Map<PoolInfo, PoolInfoMetrics> newPoolNameToMetrics = new HashMap<PoolInfo,
      PoolInfoMetrics>();
    long now = ClusterManager.clock.getTime();

    Map<PoolInfo, Long> poolInfoAverageFirstWaitMs =
        sessionManager.getTypePoolInfoAveFirstWaitMs(type);

    // The gets + puts below are OK because only one thread is doing it.
    for (PoolGroupSchedulable poolGroup : poolGroupManager.getPoolGroups()) {
      int poolGroupSessions = 0;
      for (PoolSchedulable pool : poolGroup.getPools()) {
        MetricsRecord poolRecord =
            poolInfoToMetricsRecord.get(pool.getPoolInfo());
        if (poolRecord == null) {
          poolRecord = metrics.getContext().createRecord(
              "pool-" + pool.getName());
          poolInfoToMetricsRecord.put(pool.getPoolInfo(), poolRecord);
        }

        PoolInfoMetrics poolMetrics = new PoolInfoMetrics(pool.getPoolInfo(),
            type, poolRecord);
        poolMetrics.setCounter(
            MetricName.GRANTED, pool.getGranted());
        poolMetrics.setCounter(
            MetricName.REQUESTED, pool.getRequested());
        poolMetrics.setCounter(
            MetricName.SHARE, (long) pool.getShare());
        poolMetrics.setCounter(
            MetricName.MIN, pool.getMinimum());
        poolMetrics.setCounter(
            MetricName.MAX, pool.getMaximum());
        poolMetrics.setCounter(
            MetricName.WEIGHT, (long) pool.getWeight());
        poolMetrics.setCounter(
            MetricName.SESSIONS, pool.getScheduleQueue().size());
        poolMetrics.setCounter(
            MetricName.STARVING, pool.getStarvingTime(now) / 1000);
        Long averageFirstTypeMs =
            poolInfoAverageFirstWaitMs.get(pool.getPoolInfo());
        poolMetrics.setCounter(MetricName.AVE_FIRST_WAIT_MS,
            (averageFirstTypeMs == null) ?
                0 : averageFirstTypeMs.longValue());

        newPoolNameToMetrics.put(pool.getPoolInfo(), poolMetrics);
        poolGroupSessions += pool.getScheduleQueue().size();
      }

      MetricsRecord poolGroupRecord =
          poolInfoToMetricsRecord.get(poolGroup.getName());
      if (poolGroupRecord == null) {
        poolGroupRecord = metrics.getContext().createRecord(
            "poolgroup-" + poolGroup.getName());
        poolInfoToMetricsRecord.put(poolGroup.getPoolInfo(), poolGroupRecord);
      }

      PoolInfoMetrics poolGroupMetrics =
          new PoolInfoMetrics(poolGroup.getPoolInfo(), type, poolGroupRecord);
      poolGroupMetrics.setCounter(
          MetricName.GRANTED, poolGroup.getGranted());
      poolGroupMetrics.setCounter(
          MetricName.REQUESTED, poolGroup.getRequested());
      poolGroupMetrics.setCounter(
          MetricName.SHARE, (long) poolGroup.getShare());
      poolGroupMetrics.setCounter(
          MetricName.MIN, poolGroup.getMinimum());
      poolGroupMetrics.setCounter(
          MetricName.MAX, poolGroup.getMaximum());
      poolGroupMetrics.setCounter(
          MetricName.SESSIONS, poolGroupSessions);
      newPoolNameToMetrics.put(poolGroup.getPoolInfo(), poolGroupMetrics);
    }

    poolInfoToMetrics = newPoolNameToMetrics;
  }

  /**
   * Wait for SCHEDULE_PERIOD or another thread tells this thread to do
   * something.
   *
   * @throws InterruptedException
   */
  private void waitForNotification() throws InterruptedException {
    synchronized (this) {
      this.wait(SCHEDULE_PERIOD);
    }
  }

  /**
   * Informed the sessions about their newly granted resources via
   * {@link SessionNotifier}.
   *
   * @param sessionIdToGranted Map of session ids to granted resources
   */
  private void dispatchGrantedResource(
      Map<String, List<ResourceGrant>> sessionIdToGranted) {
    for (Map.Entry<String, List<ResourceGrant>> entry :
        sessionIdToGranted.entrySet()) {
      LOG.info("Assigning " + entry.getValue().size() + " " +
          type + " requests to Session: " + entry.getKey());
      try {
        Session session = sessionManager.getSession(entry.getKey());
        session.setResourceGrant(entry.getValue());
      } catch (InvalidSessionHandle e) {
        LOG.warn("Trying to add call for invalid session: " + entry.getKey());
      }
      if (LOG.isDebugEnabled()) {
        LOG.debug(Arrays.toString(entry.getValue().toArray()));
      }
      sessionNotifier.notifyGrantResource(entry.getKey(), entry.getValue());
    }
  }

  /**
   * Match requests to nodes.
   *
   * @return The list of granted resources for each session
   */
  private Map<String, List<ResourceGrant>> scheduleTasks() {
    fullyScheduled = false;
    long nodeWait = configManager.getLocalityWait(type, LocalityLevel.NODE);
    long rackWait = configManager.getLocalityWait(type, LocalityLevel.RACK);
    int tasksToSchedule = configManager.getGrantsPerIteration();
    Map<String, List<ResourceGrant>> sessionIdToGranted =
        new HashMap<String, List<ResourceGrant>>();
    for (int i = 0; i < tasksToSchedule; i++) {
      ScheduledPair scheduled = scheduleOneTask(nodeWait, rackWait);
      if (scheduled == null) {
        // Cannot find matched request-node anymore. We are done.
        fullyScheduled = true;
        break;
      }
      List<ResourceGrant> granted =
        sessionIdToGranted.get(scheduled.sessionId.toString());
      if (granted == null) {
        granted = new LinkedList<ResourceGrant>();
        sessionIdToGranted.put(scheduled.sessionId.toString(), granted);
      }
      granted.add(scheduled.grant);
    }
    return sessionIdToGranted;
  }

  /**
   * Try match one request to one node
   *
   * @param nodeWait Time to wait for node locality
   * @param rackWait Time to wait for rack locality
   * @return The pair contains a session id and a granted resource
   *         or null when no task can be scheduled
   */
  private ScheduledPair scheduleOneTask(long nodeWait, long rackWait) {
    if (!nodeManager.existRunnableNodes(type)) {
      return null;
    }

    Queue<PoolGroupSchedulable> poolGroupQueue =
        poolGroupManager.getScheduleQueue();
    while (!poolGroupQueue.isEmpty()) {
      PoolGroupSchedulable poolGroup = poolGroupQueue.poll();
      if (poolGroup.reachedMaximum()) {
        continue;
      }
      // Get the appropriate pool from the pool group to schedule, then
      // schedule the best session
      Queue<PoolSchedulable> poolQueue = poolGroup.getScheduleQueue();
      while (!poolQueue.isEmpty()) {
        PoolSchedulable pool = poolQueue.poll();
        if (pool.reachedMaximum()) {
          continue;
        }
        Queue<SessionSchedulable> sessionQueue = pool.getScheduleQueue();
        while (!sessionQueue.isEmpty()) {
          SessionSchedulable schedulable = sessionQueue.poll();
          Session session = schedulable.getSession();
          long now = ClusterManager.clock.getTime();
          MatchedPair pair = doMatch(
            schedulable, now, nodeWait, rackWait);
          synchronized (session) {
            if (session.isDeleted()) {
              continue;
            }
            if (pair != null) {
              ResourceGrant grant = commitMatchedResource(session, pair);
              if (grant != null) {
                poolGroup.incGranted(1);
                pool.incGranted(1);
                schedulable.incGranted(1);
                // Put back to the queue only if we scheduled successfully
                poolGroupQueue.add(poolGroup);
                poolQueue.add(pool);
                sessionQueue.add(schedulable);
                return new ScheduledPair(
                    session.getSessionId().toString(), grant);
              }
            }
          }
        }
      }
    }
    return null;
  }

  /**
   * Find a node to give resources to this schedulable session.
   *
   * @param schedulable Given a resource to this schedulable
   * @param now Current time
   * @param nodeWait Time to wait for node locality
   * @param rackWait Time to wait for rack locality
   * @return Pair of resource request and node or null if no node can be found.
   */
  private MatchedPair doMatch(
      SessionSchedulable schedulable, long now, long nodeWait, long rackWait) {
    schedulable.adjustLocalityRequirement(now, nodeWait, rackWait);
    for (LocalityLevel level : neededLocalityLevels) {
      if (level.isBetterThan(schedulable.getLastLocality())) {
        /**
         * This means that the last time we tried to schedule this session
         * we could not achieve the current LocalityLevel level.
         * Since this is the same iteration of the scheduler we do not need to
         * try this locality level.
         * The last locality level of the shcedulable is getting reset on every
         * iteration of the scheduler, so we will retry the better localities
         * in the next run of the scheduler.
         */
        continue;
      }
      if (needLocalityCheck(level, nodeWait, rackWait) &&
          !schedulable.isLocalityGoodEnough(level)) {
        break;
      }
      Session session = schedulable.getSession();
      synchronized (session) {
        if (session.isDeleted()) {
          return null;
        }
        int pendingRequestCount = session.getPendingRequestCountForType(type);
        MatchedPair matchedPair = null;
        if (nodeSnapshot == null ||
          pendingRequestCount < nodeSnapshot.getRunnableHostCount()) {
          matchedPair = matchNodeForSession(session, level);
        } else {
          matchedPair = matchSessionForNode(session, level);
        }
        if (matchedPair != null) {
          schedulable.setLocalityLevel(level);
          return matchedPair;
        }
      }
    }
    schedulable.startLocalityWait(now);
    if (LOG.isDebugEnabled()) {
      LOG.debug("Could not find a node for " +
        schedulable.getSession().getHandle());
    }
    return null;
  }

  /**
   * Find a matching pair of node, request by looping through the requests in
   * the session, looking at the hosts in each request and making look-ups
   * into the node manager.
   * @param session The session
   * @param level The locality level at which we are trying to schedule.
   * @return A match if found, null if not.
   */
  private MatchedPair matchNodeForSession(
    Session session, LocalityLevel level) {
    Iterator<ResourceRequestInfo> pendingRequestIterator =
        session.getPendingRequestIteratorForType(type);
    while (pendingRequestIterator.hasNext()) {
      ResourceRequestInfo req = pendingRequestIterator.next();
      Set<String> excluded = req.getExcludeHosts();
      if (req.getHosts() == null || req.getHosts().size() == 0) {
        // No locality requirement
        String host = null;
        ClusterNode node = nodeManager.getRunnableNode(
            host, LocalityLevel.ANY, type, excluded);
        if (node != null) {
          return new MatchedPair(node, req);
        }
        continue;
      }
      for (RequestedNode requestedNode : req.getRequestedNodes()) {
        ClusterNode node = nodeManager.getRunnableNode(
            requestedNode, level, type, excluded);
        if (node != null) {
          return new MatchedPair(node, req);
        }
      }
    }
    return null;
  }

  /**
   * Find a matching pair of node, request by looping through runnable nodes
   * in the node snapshot created earlier. For each node, we make lookups in the
   * session to find a suitable request.
   * @param session The session
   * @param level The locality level at which we are trying to schedule.
   * @return A match if found, null if not.
   */
  private MatchedPair matchSessionForNode(
    Session session, LocalityLevel level) {
    if (level == LocalityLevel.NODE || level == LocalityLevel.ANY) {
      Set<Map.Entry<String, NodeContainer>> hostNodesSet =
        nodeSnapshot.runnableHosts();
      for (Map.Entry<String, NodeContainer> hostNodes : hostNodesSet) {
        Iterator<ClusterNode> clusterNodeIt = hostNodes.getValue().iterator();
        while (clusterNodeIt.hasNext()) {
          ClusterNode node = clusterNodeIt.next();
          if (!nodeManager.hasEnoughResource(node)) {
            continue;
          }
          ResourceRequestInfo req = null;
          if (level == LocalityLevel.NODE) {
            req = session.getPendingRequestOnHost(node.getHost(), type);
          } else {
            req = session.getPendingRequestForAny(node.getHost(), type);
          }
          if (req != null) {
            return new MatchedPair(node, req);
          }
        }
      }
    } else if (level == LocalityLevel.RACK) {
      Set<Map.Entry<Node, NodeContainer>> rackNodesSet =
        nodeSnapshot.runnableRacks();
      for (Map.Entry<Node, NodeContainer> rackNodes: rackNodesSet) {
        Node rack = rackNodes.getKey();
        NodeContainer nodes = rackNodes.getValue();
        Iterator<ClusterNode> clusterNodeIt = nodes.iterator();
        while (clusterNodeIt.hasNext()) {
          ClusterNode node = clusterNodeIt.next();
          if (!nodeManager.hasEnoughResource(node)) {
            continue;
          }
          ResourceRequestInfo req = session.getPendingRequestOnRack(
            node.getHost(), rack, type);
          if (req != null) {
            return new MatchedPair(node, req);
          }
        }
      }
    }
    return null;
  }

  /**
   * If the locality wait time is zero, we don't need to check locality at all.
   * @param level The level
   * @param nodeWait The amount of time in msec to wait for node locality
   * @param rackWait The amount of time in msec to wait for rack locality
   * @return True if the locality check is needed, false otherwise.
   */
  private boolean needLocalityCheck(
      LocalityLevel level, long nodeWait, long rackWait) {
    if (level == LocalityLevel.NODE) {
      return nodeWait != 0;
    }
    if (level == LocalityLevel.RACK) {
      return rackWait != 0;
    }
    return false;
  }

  /**
   * Given a session and match of request-node, perform a "transaction commit"
   * @param session The session
   * @param pair The pair of (request, node)
   * @return The resource grant: non-null if the "commit" was successful, null
   *         otherwise.
   */
  private ResourceGrant commitMatchedResource(
      Session session, MatchedPair pair) {
    ResourceGrant grant = null;
    ResourceRequestInfo req = pair.req;
    ClusterNode node = pair.node;
    String appInfo = nodeManager.getAppInfo(node, type);
    if (appInfo != null) {
      if (nodeManager.addGrant(node, session.getSessionId(), req)) {
        // if the nodemanager can commit this grant - we are done
        // the commit can fail if the node has been deleted
        grant = new ResourceGrant(req.getId(), node.getName(),
          node.getAddress(), ClusterManager.clock.getTime(), req.getType());
        grant.setAppInfo(appInfo);
        sessionManager.grantResource(session, req, grant);
      }
    }
    if (nodeSnapshot != null) {
      synchronized (node) {
        if (node.deleted) {
          nodeSnapshot.removeNode(node);
        } else if (!node.checkForGrant(Utilities.getUnitResourceRequest(type),
          nodeManager.getResourceLimit())) {
          nodeSnapshot.removeNode(node);
        }
      }
    }
    return grant;
  }

  /**
   * Update metrics.
   */
  public void submitMetrics() {
    for (PoolInfoMetrics m : poolInfoToMetrics.values()) {
      m.updateMetricsRecord();
    }
  }

  /**
   * A pair of (request, node).
   */
  private class MatchedPair {
    /** Request */
    private final ResourceRequestInfo req;
    /** Node */
    private final ClusterNode node;

    /**
     * Constructor.
     * @param node The node.
     * @param req The request.
     */
    MatchedPair(ClusterNode node, ResourceRequestInfo req) {
      this.req = req;
      this.node = node;
    }
  }

  /**
   * Represents a resource grant given to a session.
   */
  private class ScheduledPair {
    /** The grant. */
    private final ResourceGrant grant;
    /** The session ID. */
    private final String sessionId;

    /**
     * Constructor.
     * @param sessionId The session ID.
     * @param grant The grant.
     */
    ScheduledPair(String sessionId, ResourceGrant grant) {
      this.grant = grant;
      this.sessionId = sessionId;
    }
  }

  /**
   * Performs preemption if it has been long enough since the last round.
   */
  private void doPreemption() {
    long now = ClusterManager.clock.getTime();
    if (now - lastPreemptionTime > PREEMPTION_PERIOD) {
      lastPreemptionTime = now;
      doPreemptionNow();
    }
  }

  /**
   * Performs the preemption.
   */
  private void doPreemptionNow() {
    int totalShare = nodeManager.getAllocatedCpuForType(type);
    poolGroupManager.distributeShare(totalShare);
    for (PoolGroupSchedulable poolGroup : poolGroupManager.getPoolGroups()) {
      poolGroup.distributeShare();
    }
    int tasksToPreempt = countTasksShouldPreempt();
    if (tasksToPreempt > 0) {
      LOG.info("Found " + tasksToPreempt + " " + type + " tasks to preempt");
      preemptTasks(tasksToPreempt);
    }
  }

  /**
   * Kill tasks from over-scheduled sessions
   * @param tasksToPreempt The number of tasks should kill
   */
  private void preemptTasks(int tasksToPreempt) {
    LOG.info("Start preempt " + tasksToPreempt + " for type " + type);
    long maxRunningTime = configManager.getPreemptedTaskMaxRunningTime();
    int rounds = configManager.getPreemptionRounds();
    while (tasksToPreempt > 0) {
      int preempted = preemptOneSession(tasksToPreempt, maxRunningTime);
      if (preempted == 0) {
        maxRunningTime *= 2;
        // Check for enough rounds or an overflow
        if (--rounds <= 0 || maxRunningTime <= 0) {
          LOG.warn("Cannot preempt enough " + type + " tasks " +
              " rounds " + configManager.getPreemptionRounds() +
              " maxRunningTime " + maxRunningTime +
              " tasks not preempted:" + tasksToPreempt);
          return;
        }
      }
      tasksToPreempt -= preempted;
    }
  }

  /**
   * Find the most over-scheduled session in the most over-scheduled pool.
   * Kill tasks from this session.
   * @param maxToPreemt The maximum number of tasks to kill
   * @param maxRunningTime The killed task cannot be older than this time
   * @return The number of tasks actually killed
   */
  private int preemptOneSession(int maxToPreemt, long maxRunningTime) {
    Queue<PoolGroupSchedulable> poolGroupQueue =
        poolGroupManager.getPreemptQueue();
    while (!poolGroupQueue.isEmpty()) {
      PoolGroupSchedulable poolGroup = poolGroupQueue.poll();
      poolGroup.distributeShare();
      Queue<PoolSchedulable> poolQueue = poolGroup.getPreemptQueue();
      while (!poolQueue.isEmpty()) {
        PoolSchedulable pool = poolQueue.poll();
        pool.distributeShare();
        if (!pool.isPreemptable()) {
          continue;
        }
        Queue<SessionSchedulable> sessionQueue = pool.getPreemptQueue();
        while (!sessionQueue.isEmpty()) {
          SessionSchedulable schedulable = sessionQueue.poll();
          try {
            int overScheduled =
                (int) (schedulable.getGranted() - schedulable.getShare());
            if (overScheduled <= 0) {
              continue;
            }
            maxToPreemt = Math.min(maxToPreemt, overScheduled);
            LOG.info("Trying to preempt " + maxToPreemt + " " + type +
                " from " + schedulable.getSession().getHandle());
            int preempted = preemptSession(
                schedulable, maxToPreemt, maxRunningTime);
            poolGroup.incGranted(-1 * preempted);
            pool.incGranted(-1 * preempted);
            schedulable.incGranted(-1 * preempted);
            return preempted;
          } catch (InvalidSessionHandle e) {
            LOG.warn("Invalid session handle:" +
                schedulable.getSession().getHandle() +
                " Session may be removed");
          } finally {
            // Add back the queue so it can be further preempt for other
            // sessions.
            poolGroupQueue.add(poolGroup);
            poolQueue.add(pool);
          }
        }
      }
    }
    return 0;
  }

  /**
   * Preempt a session.
   * @param schedulable The session.
   * @param maxToPreemt Maximum number of resources to preempt.
   * @param maxRunningTime Running time threshold for preemption.
   * @return The number of preempted resources.
   * @throws InvalidSessionHandle
   */
  private int preemptSession(SessionSchedulable schedulable,
      int maxToPreemt, long maxRunningTime) throws InvalidSessionHandle {
    Session session = schedulable.getSession();
    List<Integer> grantIds;
    synchronized (session) {
      grantIds = session.getGrantsToPreempt(maxToPreemt, maxRunningTime, type);
    }
    List<ResourceGrant> revokedGrants =
      sessionManager.revokeResource(session.getHandle(), grantIds);
    for (ResourceGrant grant : revokedGrants) {
      nodeManager.cancelGrant(
          grant.nodeName, session.getSessionId(), grant.getId());
    }
    sessionNotifier.notifyRevokeResource(
        session.getHandle(), revokedGrants, true);
    int preempted = revokedGrants.size();
    LOG.info("Preempt " + preempted + " " + type +
        " tasks for Session:" + session.getHandle());
    return preempted;
  }

  /**
   * Count how many tasks should preempt for the starving pools
   * @return The number of tasks should kill
   */
  private int countTasksShouldPreempt() {
    int tasksToPreempt = 0;
    long now = ClusterManager.clock.getTime();
    for (PoolGroupSchedulable poolGroup : poolGroupManager.getPoolGroups()) {
      for (PoolSchedulable pool : poolGroup.getPools()) {
        if (pool.isStarving(now)) {
          tasksToPreempt +=
            Math.min(pool.getPending(), pool.getShare() - pool.getGranted());
        }
      }
    }
    return tasksToPreempt;
  }

  /**
   * Add a session to this scheduler.
   * @param id The session ID.
   * @param session The session.
   */
  public void addSession(String id, Session session) {
    poolGroupManager.addSession(id, session);
    LOG.info("Session " + id +
        " has been added to " + type + " scheduler");
  }

  public Map<PoolInfo, PoolInfoMetrics> getPoolInfoMetrics() {
    return poolInfoToMetrics;
  }

  /**
   * Inform to stop scheduling.  This is not immediate, but will eventually
   * occur.
   */
  public void close() {
    shutdown = true;
  }
}
