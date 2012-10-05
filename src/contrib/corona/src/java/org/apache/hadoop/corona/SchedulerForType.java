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
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.corona.PoolMetrics.MetricName;

/**
 * Scheduler thread which matches requests to nodes for the given resource type
 */
public class SchedulerForType extends Thread {
  /** Class logger */
  public static final Log LOG = LogFactory.getLog(SchedulerForType.class);
  /** Maximum amount of milliseconds to wait before scheduling */
  public static final long SCHEDULE_PERIOD = 100L;
  /** Milliseconds to wait before preempting tasks */
  public static final long PREEMPTION_PERIOD = 1000L;
  /** Type of resource to schedule for */
  private final ResourceType type;
  /** Manages the pools for this resource type */
  private final PoolManager poolManager;
  /** Used to grant/revoke resources to the sessions */
  private final SessionManager sessionManager;
  /** Asynchronously notify the sessions about changes */
  private final SessionNotifier sessionNotifier;
  /** Find where to run the resources */
  private final NodeManager nodeManager;
  /** Get up-to-date dynamic configruation */
  private final ConfigManager configManager;
  /** Thread safe map of pool name to associaetd metrics */
  private final Map<String, PoolMetrics> poolNameToMetrics;
  /** Run until told to shutdown */
  private volatile boolean shutdown = false;
  /** Saved last preemption time, used with PREEMPTION_PREIOD */
  private long lastPreemptionTime = -1L;
  /** Do we want to avoid preemption. */
  private final boolean disablePreemption;
  /** Cluster Manager metrics. */
  private final ClusterManagerMetrics metrics;

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
    this.poolManager = new PoolManager(type, configManager, conf);
    this.sessionManager = sessionManager;
    this.sessionNotifier = sessionNotifier;
    this.nodeManager = nodeManager;
    this.configManager = configManager;
    this.poolNameToMetrics = new ConcurrentHashMap<String, PoolMetrics>();
    this.disablePreemption = type == ResourceType.JOBTRACKER;
    this.metrics = metrics;
  }

  @Override
  public void run() {
    while (!shutdown) {
      try {

        waitForNotification();

        long start = System.currentTimeMillis();

        poolManager.snapshot();

        Map<String, List<ResourceGrant>> sessionIdToGranted = scheduleTasks();

        dispatchGrantedResource(sessionIdToGranted);

        if (!disablePreemption) {
          doPreemption();
        }

        int runtime = (int) (System.currentTimeMillis() - start);
        metrics.setSchedulerRunTime(type, runtime);

        collectPoolMetrics();

      } catch (InterruptedException e) {
        if (!shutdown) {
          LOG.warn("Unexpected InterruptedException");
        }
      } catch (Throwable t) {
        LOG.error("SchedulerForType " + type, t);
      }
    }
  }

  /**
   * Update the pool metrics.  This update is atomic at the entry level, not
   * across the whole map.
   */
  private void collectPoolMetrics() {
    // TODO: Push these to Hadoop metrics
    poolNameToMetrics.clear();
    long now = ClusterManager.clock.getTime();
    for (PoolSchedulable pool : poolManager.getPools()) {
      PoolMetrics metrics = new PoolMetrics(pool.getName(), type);
      metrics.setCounter(MetricName.GRANTED, pool.getGranted());
      metrics.setCounter(MetricName.REQUESTED, pool.getRequested());
      metrics.setCounter(MetricName.SHARE, (long) pool.getShare());
      metrics.setCounter(MetricName.MIN, pool.getMinimum());
      metrics.setCounter(MetricName.MAX, pool.getMaximum());
      metrics.setCounter(MetricName.WEIGHT, (long) pool.getWeight());
      metrics.setCounter(MetricName.SESSIONS, pool.getScheduleQueue().size());
      metrics.setCounter(MetricName.STARVING, pool.getStarvingTime(now) / 1000);
      poolNameToMetrics.put(pool.getName(), metrics);
    }
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
      LOG.info("Assigning " +  entry.getValue().size() + " " +
          type + " requests to Session: " + entry.getKey() +
          " " + Arrays.toString(entry.getValue().toArray()));
      sessionNotifier.notifyGrantResource(entry.getKey(), entry.getValue());
    }
  }

  /**
   * Match requests to nodes.
   *
   * @return The list of granted resources for each session
   */
  private Map<String, List<ResourceGrant>> scheduleTasks() {

    long nodeWait = configManager.getLocalityWait(type, LocalityLevel.NODE);
    long rackWait = configManager.getLocalityWait(type, LocalityLevel.RACK);
    Map<String, List<ResourceGrant>> sessionIdToGranted =
        new HashMap<String, List<ResourceGrant>>();
    for (;;) {
      ScheduledPair scheduled = scheduleOneTask(nodeWait, rackWait);
      if (scheduled == null) {
        // Cannot find matched request-node anymore. We are done.
        break;
      }
      List<ResourceGrant> granted =
        sessionIdToGranted.get(scheduled.getSessionId().toString());
      if (granted == null) {
        granted = new ArrayList<ResourceGrant>();
        sessionIdToGranted.put(scheduled.getSessionId().toString(), granted);
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
    Queue<PoolSchedulable> poolQueue = poolManager.getScheduleQueue();
    if (!nodeManager.existRunnableNodes(type)) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Not scheduling because there are no runnable " +
            "nodes for type " + type);
      }
      return null;
    }

    if (LOG.isDebugEnabled()) {
      LOG.debug(poolQueue.size() + " pools in the pool queue for scheduling");
    }
    while (!poolQueue.isEmpty()) {
      PoolSchedulable pool = poolQueue.poll();
      if (pool.reachedMaximum()) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Pool " + pool.getName() + " has reached maximum");
        }
        continue;
      }
      Queue<SessionSchedulable> sessionQueue = pool.getScheduleQueue();
      if (LOG.isDebugEnabled()) {
        LOG.debug(sessionQueue.size() + " sessions in the pool " +
            pool.getName() + " for scheduling");
      }
      while (!sessionQueue.isEmpty()) {
        SessionSchedulable schedulable = sessionQueue.poll();
        Session session = schedulable.getSession();
        synchronized (session) {
          long now = ClusterManager.clock.getTime();
          MatchedPair pair = matchNodeForSession(
              schedulable, now, nodeWait, rackWait);
          if (pair != null) {
            ResourceGrant grant = commitMatchedResource(session, pair);
            if (grant != null) {
              pool.incGranted(1);
              schedulable.incGranted(1);
              // Put back to the queue only if we scheduled successfully
              poolQueue.add(pool);
              sessionQueue.add(schedulable);
              return new ScheduledPair(
                  session.getSessionId().toString(), grant);
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
  private MatchedPair matchNodeForSession(
      SessionSchedulable schedulable, long now, long nodeWait, long rackWait) {
    Session session = schedulable.getSession();
    if (session.isDeleted()) {
      return null;
    }
    schedulable.adjustLocalityRequirement(now, nodeWait, rackWait);
    for (LocalityLevel level : LocalityLevel.values()) {
      if (needLocalityCheck(level, nodeWait, rackWait) &&
          !schedulable.isLocalityGoodEnough(level)) {
        break;
      }
      for (ResourceRequest req : session.getPendingRequestForType(type)) {
        Set<String> excluded = null;
        if (req.getExcludeHosts() != null) {
          excluded = new HashSet<String>(req.getExcludeHosts());
        }
        if (req.getHosts() == null || req.getHosts().size() == 0) {
          // No locality requirement
          ClusterNode node = nodeManager.getRunnableNode(
              null, LocalityLevel.ANY, type, excluded);
          if (node != null) {
            return new MatchedPair(node, req);
          }
          continue;
        }
        for (String host : req.getHosts()) {
          ClusterNode node = nodeManager.getRunnableNode(
              host, level, type, excluded);
          if (node != null) {
            schedulable.setLocalityLevel(level);
            return new MatchedPair(node, req);
          }
        }
      }
    }
    schedulable.startLocalityWait(now);
    if (LOG.isDebugEnabled()) {
      LOG.debug("Could not find a node for " + session.getHandle());
    }
    return null;
  }

  /**
   * If the locality wait time is zero, we don't need to check locality at all.
   *
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

  private ResourceGrant commitMatchedResource(
      Session session, MatchedPair pair) {
    ResourceRequest req = pair.req;
    ClusterNode node = pair.node;
    String appInfo = nodeManager.getAppInfo(node, type);
    if (appInfo == null) {
      return null;
    }
    if (!nodeManager.addGrant(node, session.getSessionId(), req)) {
      return null;
    }
    // if the nodemanager can commit this grant - we are done
    // the commit can fail if the node has been deleted
    ResourceGrant grant = new ResourceGrant(req.getId(), node.getName(),
        node.getAddress(), ClusterManager.clock.getTime(), req.getType());
    grant.setAppInfo(appInfo);
    sessionManager.grantResource(session, req, grant);
    if (LOG.isDebugEnabled()) {
      LOG.debug("Assigning one " + type + " for session" +
          session.getSessionId() + " to " + node.getHost());
    }
    return grant;
  }

  private class MatchedPair {
    final ResourceRequest req;
    final ClusterNode node;
    MatchedPair(ClusterNode node, ResourceRequest req) {
      this.req = req;
      this.node = node;
    }
  }

  private class ScheduledPair {
    private final ResourceGrant grant;
    private final String sessionId;
    ScheduledPair(String sessionId, ResourceGrant grant) {
      this.grant = grant;
      this.sessionId = sessionId;
    }

    public ResourceGrant getGrant() {
      return grant;
    }

    public String getSessionId() {
      return sessionId;
    }
  }

  private void doPreemption() {
    long now = ClusterManager.clock.getTime();
    if (now - lastPreemptionTime > PREEMPTION_PERIOD) {
      lastPreemptionTime = now;
      doPreemptionNow();
    }
  }

  private void doPreemptionNow() {
    int totalShare = nodeManager.getAllocatedCpuForType(type);
    poolManager.distributeShare(totalShare);
    int tasksToPreempt = countTasksShouldPreempt();
    if (tasksToPreempt > 0) {
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
          LOG.warn("Cannot preempt enough tasks." +
              " Tasks not preempted:" + tasksToPreempt);
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
    Queue<PoolSchedulable> poolQueue = poolManager.getPreemptQueue();
    while (!poolQueue.isEmpty()) {
      PoolSchedulable pool = poolQueue.poll();
      pool.distributeShare();
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
              " from " + schedulable.getName());
          int preempted = preemptSession(
              schedulable, maxToPreemt, maxRunningTime);
          pool.incGranted(-1 * preempted);
          schedulable.incGranted(-1 * preempted);
          return preempted;
        } catch (InvalidSessionHandle e) {
          LOG.warn("Invalid session handle:" +
              schedulable.getSession().getHandle() +
              " Session may be removed");
        } finally {
          // Add back the queue so it can be further preempt for other sessions
          poolQueue.add(pool);
        }
      }
    }
    return 0;
  }

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
    for (PoolSchedulable pool : poolManager.getPools()) {
      if (pool.isStarving(now)) {
        tasksToPreempt += pool.getShare() - pool.getGranted();
      }
    }
    return tasksToPreempt;
  }

  public void addSession(String id, Session session) {
    poolManager.addSession(id, session);
    LOG.info("Session " + id +
        " has been added to " + type + " scheduler");
  }

  public Map<String, PoolMetrics> getPoolMetrics() {
    return Collections.unmodifiableMap(poolNameToMetrics);
  }

  /**
   * Inform to stop scheduling.  This is not immediate, but will eventually
   * occur.
   */
  public void close() {
    shutdown = true;
  }
}
