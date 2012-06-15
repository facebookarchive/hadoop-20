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

  public static final Log LOG = LogFactory.getLog(PoolSchedulable.class);
  private static final long MIN_PREEMPT_PERIOD = 60 * 1000;
  private final ConfigManager configManager;
  private final Map<String, SessionSchedulable> idToSession;
  private final Collection<SessionSchedulable> snapshotSessions;
  private Queue<SessionSchedulable> scheduleQueue;
  private Queue<SessionSchedulable> preemptQueue;
  private long lastTimeAboveMinimum;
  private long lastTimeAboveStarvingShare;
  private long lastPreemptTime;
  private boolean needRedistributed;
  private int maximum;
  private int minimum;
  private double weight;
  private double shareStarvingRatio;
  private long starvingTimeForShare;
  private long starvingTimeForMinimum;

  public PoolSchedulable(String name, String type,
      ConfigManager configManager) {
    super(name, type);
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

  /**
   * Take snapshots of this pool and the sessions within
   */
  @Override
  public void snapshot() {
    granted = 0;
    requested = 0;
    snapshotSessions.clear();
    Set<String> toBeDeleted = new HashSet<String>();
    for (Entry<String, SessionSchedulable> entry : idToSession.entrySet()) {
      SessionSchedulable schedulable = entry.getValue();
      Session session = entry.getValue().getSession();
      synchronized (session) {
        if (session.deleted) {
          toBeDeleted.add(entry.getKey());
        } else {
          schedulable.snapshot();
          snapshotSessions.add(schedulable);
          granted += schedulable.getGranted();
          requested += schedulable.getRequested();
        }
      }
    }
    for (String id : toBeDeleted) {
      idToSession.remove(id);
    }
    snapshotConfig();
    scheduleQueue = null;
    preemptQueue = null;
    needRedistributed = true;
  }

  private void snapshotConfig() {
    synchronized (configManager) {
      maximum = configManager.getMaximum(getName(), getType());
      minimum = configManager.getMinimum(getName(), getType());
      weight = configManager.getWeight(getName());
      shareStarvingRatio = configManager.getShareStarvingRatio();
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

  public boolean reachedMaximum() {
    return granted >= getMaximum() || granted >= requested;
  }

  public void addSession(String id, Session session) {
    synchronized (session) {
      SessionSchedulable schedulable =
        new SessionSchedulable(session, getType());
      idToSession.put(id, schedulable);
    }
  }

  public Queue<SessionSchedulable> getScheduleQueue() {
    if (scheduleQueue == null) {
      scheduleQueue =
          createSessionQueue(configManager.getComparator(getName()));
    }
    return scheduleQueue;
  }

  public Queue<SessionSchedulable> getPreemptQueue() {
    if (preemptQueue == null) {
      ScheduleComparator comparator =
        configManager.getComparator(getName()) == ScheduleComparator.FIFO ?
            ScheduleComparator.FIFO_PREEMPT : ScheduleComparator.FAIR_PREEMPT;
      preemptQueue = createSessionQueue(comparator);
    }
    return preemptQueue;
  }

  public Queue<SessionSchedulable> createSessionQueue(
      ScheduleComparator comparator) {
    int initCapacity = snapshotSessions.size() == 0 ?
        1 : snapshotSessions.size();
    Queue<SessionSchedulable> sessionQueue =
      new PriorityQueue<SessionSchedulable>(initCapacity, comparator);
    sessionQueue.addAll(snapshotSessions);
    return sessionQueue;
  }

  public void distributeShare() {
    if (needRedistributed == true) {
      ScheduleComparator comparator = configManager.getComparator(getName());
      Schedulable.distributeShare(getShare(), snapshotSessions, comparator);
    }
    needRedistributed = false;
  }

  public boolean isStarving(long now) {
    double starvingShare = getShare() * shareStarvingRatio;
    if (getGranted() >= starvingShare) {
      lastTimeAboveStarvingShare = now;
    }
    if (getGranted() >= getMinimum()) {
      lastTimeAboveMinimum = now;
    }
    if (now - lastPreemptTime < MIN_PREEMPT_PERIOD) {
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

  public long getStarvingTime(long now) {
    long starvingTime = Math.max(
        getMinimumStarvingTime(now), getShareStarvingTime(now));
    return starvingTime;
  }

  private long getMinimumStarvingTime(long now) {
    return (now - lastTimeAboveMinimum) - starvingTimeForMinimum;
  }

  private long getShareStarvingTime(long now) {
    return (now - lastTimeAboveStarvingShare) - starvingTimeForShare;
  }
}
