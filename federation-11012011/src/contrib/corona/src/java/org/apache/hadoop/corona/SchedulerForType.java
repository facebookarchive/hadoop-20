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
 * Scheduler thread which matches requests to nodes for the given type
 */
public class SchedulerForType extends Thread {

  public static final Log LOG = LogFactory.getLog(SchedulerForType.class);
  final static public long SCHEDULE_PERIOD = 100L;
  final static public long PREEMPTION_PERIOD = 1000L;
  final private String type;
  final private PoolManager poolManager;
  final private SessionManager sessionManager;
  final private SessionNotifier sessionNotifier;
  final private NodeManager nodeManager;
  final private ConfigManager configManager;
  final private Map<String, PoolMetrics> poolNameToMetrics;
  private volatile boolean shutdown = false;
  private long lastPreemptionTime = -1L;

  public SchedulerForType(String type, SessionManager sessionManager,
      SessionNotifier sessionNotifier, NodeManager nodeManager,
      ConfigManager configManager) {
    this.type = type;
    this.poolManager = new PoolManager(type, configManager);
    this.sessionManager = sessionManager;
    this.sessionNotifier = sessionNotifier;
    this.nodeManager = nodeManager;
    this.configManager = configManager;
    this.poolNameToMetrics = new ConcurrentHashMap<String, PoolMetrics>();
  }

  @Override
  public void run() {
    while (!shutdown) {
      try {

        waitForNotification();

        poolManager.snapshot();

        Map<String, List<ResourceGrant>> sessionIdToGranted = scheduleTasks();

        dispatchGrantedResource(sessionIdToGranted);

        doPreemption();

        collectPoolMetrics();

      } catch (InterruptedException e) {
        if (!shutdown) {
          LOG.warn ("Unexpected InterruptedException");
        }
      } catch (Throwable t) {
        LOG.error("SchedulerForType " + type, t);
      }
    }
  }

  private void collectPoolMetrics() {
    // TODO: Push these to Hadoop metrics
    poolNameToMetrics.clear();
    long now = ClusterManager.clock.getTime();
    for (PoolSchedulable pool : poolManager.getPools()) {
      PoolMetrics metrics = new PoolMetrics(pool.getName(), type);
      metrics.setCounter(MetricName.GRANTED, pool.getGranted());
      metrics.setCounter(MetricName.REQUESTED, pool.getRequested());
      metrics.setCounter(MetricName.SHARE, (long)pool.getShare());
      metrics.setCounter(MetricName.MIN, pool.getMinimum());
      metrics.setCounter(MetricName.MAX, pool.getMaximum());
      metrics.setCounter(MetricName.WEIGHT, (long)pool.getWeight());
      metrics.setCounter(MetricName.SESSIONS, pool.getScheduleQueue().size());
      metrics.setCounter(MetricName.STARVING, pool.getStarvingTime(now) / 1000);
      poolNameToMetrics.put(pool.getName(), metrics);
    }
  }

  private void waitForNotification() throws InterruptedException {
    synchronized (this) {
      this.wait(SCHEDULE_PERIOD);
    }
  }

  /**
   * Submit the granted resources to {@link SessionNotifier}
   */
  private void dispatchGrantedResource(
      Map<String, List<ResourceGrant>> sessionIdToGranted) {
    for (Map.Entry<String, List<ResourceGrant>> entry :
        sessionIdToGranted.entrySet()) {
      LOG.info ("Assigning " +  entry.getValue().size() + " "
          + type + " requests to Session: " + entry.getKey()
          + " " + Arrays.toString(entry.getValue().toArray()));
      sessionNotifier.notifyGrantResource(entry.getKey(), entry.getValue());
    }
  }

  /**
   * Match requests to nodes
   * @return The list of granted resources for each session
   */
  private Map<String, List<ResourceGrant>>  scheduleTasks() {

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
        sessionIdToGranted.get(scheduled.sessionId.toString());
      if (granted == null) {
        granted = new ArrayList<ResourceGrant>();
        sessionIdToGranted.put(scheduled.sessionId.toString(), granted);
      }
      granted.add(scheduled.grant);
    }
    return sessionIdToGranted;
  }

  /**
   * Try match one request to one node
   * @return The pair contains a session id and a granted resource
   *         or null when no task can be scheduled
   */
  private ScheduledPair scheduleOneTask(long nodeWait, long rackWait) {
    Queue<PoolSchedulable> poolQueue = poolManager.getScheduleQueue();
    if (!nodeManager.existRunnableNodes(type)) {
      return null;
    }

    while (!poolQueue.isEmpty()) {
      PoolSchedulable pool = poolQueue.poll();
      if (pool.reachedMaximum()) {
        continue;
      }
      Queue<SessionSchedulable> sessionQueue = pool.getScheduleQueue();
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
              return new ScheduledPair(session.sessionId.toString(), grant);
            }
          }
        }
      }
    }
    return null;
  }

  private MatchedPair matchNodeForSession(
      SessionSchedulable schedulable, long now, long nodeWait, long rackWait) {
    Session session = schedulable.getSession();
    if (session.deleted) {
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
    return null;
  }

  /**
   * If the locality wait time is zero, we don't need to check locality at all.
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
    if (!nodeManager.addGrant(node, session.sessionId, req)) {
      return null;
    }
    // if the nodemanager can commit this grant - we are done
    // the commit can fail if the node has been deleted
    ResourceGrant grant = new ResourceGrant(req.getId(), node.getName(),
        node.getAddress(), ClusterManager.clock.getTime(), req.getType());
    if (node.getAppInfo() != null) {
      grant.setAppInfo(node.getAppInfo());
    }
    sessionManager.grantResource(session, req, grant);
    if (LOG.isDebugEnabled())
      LOG.debug("Assigning one " + type + " for session" + session.sessionId
                          + " to " + node.getHost());
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
    final ResourceGrant grant;
    final String sessionId;
    ScheduledPair(String sessionId, ResourceGrant grant) {
      this.grant = grant;
      this.sessionId = sessionId;
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
    while (tasksToPreempt > 0) {
      int preempted = preemptOneSession(tasksToPreempt, maxRunningTime);
      if (preempted == 0) {
        LOG.warn("Cannot preempt enough tasks." +
            " Tasks not preempted:" + tasksToPreempt);
        return;
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
            (int)(schedulable.getGranted() - schedulable.getShare());
          maxToPreemt = Math.min(maxToPreemt, overScheduled);
          LOG.info("Trying to preempt " + maxToPreemt + " " + type +
              " from " + schedulable.getName());
          int preempted = preemptSession(
              schedulable, maxToPreemt, maxRunningTime);
          pool.incGranted(-1 * preempted);
          schedulable.incGranted(-1 * preempted);
          return preempted;
        } catch (InvalidSessionHandle e) {
          LOG.warn("Invalid session handle:"
              + schedulable.getSession().getHandle()
              + " Session may be removed");
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
          grant.nodeName, session.sessionId, grant.getId());
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

  public void close() {
    shutdown = true;
  }
}
