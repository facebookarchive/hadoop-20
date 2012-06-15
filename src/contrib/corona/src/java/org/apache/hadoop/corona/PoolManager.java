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

  public static final Log LOG = LogFactory.getLog(PoolManager.class);
  public final static String DEFAULT_POOL_NAME = "default";
  private final String type;
  private final ConcurrentHashMap<String, PoolSchedulable> nameToPool;
  private final ConfigManager configManager;
  private CoronaConf conf;
  private Collection<PoolSchedulable> snapshotPools;
  private Queue<PoolSchedulable> scheduleQueue;
  private Queue<PoolSchedulable> preemptQueue;

  public PoolManager(String type, ConfigManager configManager) {
    this.type = type;
    this.configManager = configManager;

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

  public Queue<PoolSchedulable> getScheduleQueue() {
    if (scheduleQueue == null) {
      scheduleQueue = createPoolQueue(ScheduleComparator.FAIR);
    }
    return scheduleQueue;
  }

  public Queue<PoolSchedulable> getPreemptQueue() {
    if (preemptQueue == null) {
      preemptQueue = createPoolQueue(ScheduleComparator.FAIR_PREEMPT);
    }
    return preemptQueue;
  }

  public Queue<PoolSchedulable> createPoolQueue(ScheduleComparator comparator) {
    int initCapacity = snapshotPools.size() == 0 ? 1 : snapshotPools.size();
    Queue<PoolSchedulable> poolQueue = new PriorityQueue<PoolSchedulable>(
        initCapacity, comparator);
    poolQueue.addAll(snapshotPools);
    return poolQueue;
  }

  public void addSession(String id, Session session) {
    String poolName = getPoolName(session);
    getPoolSchedulable(poolName).addSession(id, session);
  }

  public static String getPoolName(Session session) {
    String poolName = session.getPoolId();
    // If there is no explicit pool name, take user name.
    if (poolName == null || poolName.equals("")) {
      poolName = session.getUserId();
    }
    // If pool name still is bad, use default.
    if (poolName == null || poolName.equals("")) {
      return DEFAULT_POOL_NAME;
    }
    if (!isLegalPoolName(poolName)) {
      LOG.warn("Bad poolName:" + poolName +
          " from session:" + session.sessionId);
      return DEFAULT_POOL_NAME;
    }
    return poolName;
  }

  /**
   * Returns whether or not the given pool name is legal.
   * 
   * Legal pool names are of nonzero length and are formed only of alphanumeric 
   * characters, underscores (_), and hyphens (-).
   */
  private static boolean isLegalPoolName(String poolName) {
    return !poolName.matches(".*[^0-9a-zA-Z\\-\\_].*") 
            && (poolName.length() > 0);

  }

  private PoolSchedulable getPoolSchedulable(String name) {
    PoolSchedulable poolSchedulable = nameToPool.get(name);
    if (poolSchedulable == null) {
      poolSchedulable = new PoolSchedulable(name, type, configManager);
      // Note that we never remove pools from this map
      nameToPool.put(name, poolSchedulable);
    }
    return poolSchedulable;
  }

  public void distributeShare(int total) {
    Schedulable.distributeShare(
        total, snapshotPools, ScheduleComparator.FAIR);
  }

  public Collection<PoolSchedulable> getPools() {
    return Collections.unmodifiableCollection(snapshotPools);
  }

  public void setConf(CoronaConf conf) {
    this.conf = conf;
  }

  public CoronaConf getConf() {
    return conf;
  }
}
