package org.apache.hadoop.corona;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.Iterator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.StringUtils;

/**
 * Manages a collection of sessions
 */
public class SessionManager implements Configurable {
  public static final Log LOG = LogFactory.getLog(SessionManager.class);

  private static final String DATE_FORMAT_PATTERN = "yyyyMMddHHmm";

  protected CoronaConf conf;
  protected ClusterManager clusterManager;
  protected AtomicLong sessionCounter = new AtomicLong();
  protected static int sessionExpiryInterval;
  protected Thread expireSessionsThread = null;
  protected ExpireSessions expireSessions = new ExpireSessions();
  protected Thread metricsUpdaterThread;
  protected MetricsUpdater metricsUpdater = new MetricsUpdater();
  protected volatile boolean    shutdown = false;
  protected String startTime;

  // 1: primary data structure
  protected ConcurrentMap<String, Session> sessions
    = new ConcurrentHashMap<String, Session> ();

  // 2: list of all the sessions who need compute resources right now
  protected ConcurrentMap<String, Session> runnableSessions
    = new ConcurrentHashMap<String, Session> ();

  public Set<String> getSessions() {
    return sessions.keySet();
  }

  public Session getSession (String handle) throws InvalidSessionHandle {
    Session session = sessions.get(handle);
    if (session == null) {
      throw new InvalidSessionHandle (handle);
    }
    return session;
  }

  public List<Session> getRunnableSessions() {
    List<Session> ret = new ArrayList<Session> (runnableSessions.size());
    ret.addAll(runnableSessions.values());
    return ret;
  }

  public SessionManager(ClusterManager clusterManager) {
    DateFormat dateFormat = new SimpleDateFormat(DATE_FORMAT_PATTERN);
    this.startTime = dateFormat.format(new Date());
    this.clusterManager = clusterManager;
    this.expireSessionsThread = new Thread(this.expireSessions,
                                           "expireSessions");
    this.expireSessionsThread.setDaemon(true);
    this.expireSessionsThread.start();
    this.metricsUpdaterThread = new Thread(this.metricsUpdater,
                                          "SessionManager metrics");
    this.metricsUpdaterThread.setDaemon(true);
    this.metricsUpdaterThread.start();
  }

  public String addSession(SessionInfo info)  {
    String sessionId = startTime + "." + sessionCounter.incrementAndGet();
    Session session = new Session(sessionId, info);
    sessions.put(sessionId, session);
    clusterManager.getMetrics().sessionStart();
    clusterManager.getMetrics().setNumRunningSessions(sessions.size());
    clusterManager.getScheduler().addSession(sessionId.toString(), session);
    LOG.info("Add Session " +
      sessionId + " -> " +
      info.getName() + "@" +
      info.getAddress().getHost() + ":" +
      info.getAddress().getPort());
    return sessionId.toString();
  }

  public void updateInfo(String handle, SessionInfo info)
    throws InvalidSessionHandle {
    Session session = getSession(handle);

    synchronized (session) {
      if (session.deleted)
        throw new InvalidSessionHandle(handle);

      session.updateInfo(info);
    }
  }


  public Collection<ResourceGrant> deleteSession(String handle, SessionStatus status)
    throws InvalidSessionHandle {
    Session session = getSession(handle);

    synchronized (session) {
      if (session.deleted)
        throw new InvalidSessionHandle(handle);

      session.deleted = true;
      session.status = status;
      sessions.remove(session.sessionId);
      clusterManager.getMetrics().setNumRunningSessions(sessions.size());
      clusterManager.getMetrics().sessionEnd(status);
      runnableSessions.remove(session.sessionId);
      retireSession(session);
    }

    return session.getGrants();
  }

  public void heartbeat(String handle)  throws InvalidSessionHandle {
    Session session = getSession(handle);
    session.heartbeat();
  }

  public void requestResource(String handle, List<ResourceRequest> requestList)
    throws InvalidSessionHandle {
    Session session = getSession(handle);

    synchronized (session) {
      if (session.deleted)
        throw new InvalidSessionHandle(handle);

      int previousPending = session.getPendingRequestCount();
      session.requestResource(requestList);
      if (previousPending <= 0 && (session.getPendingRequestCount() > 0))
        runnableSessions.put(session.sessionId, session);
    }
  }

  public Collection<ResourceGrant> releaseResource(String handle, List<Integer> idList)
    throws InvalidSessionHandle {
    Session session = getSession(handle);

    synchronized (session) {
      if (session.deleted)
        throw new InvalidSessionHandle(handle);

      List<ResourceGrant> canceledGrants = session.releaseResource(idList);
      if (session.getPendingRequestCount() <= 0) {
        runnableSessions.remove(session.sessionId);
      }
      return canceledGrants;
    }
  }

  public List<ResourceGrant> revokeResource(String handle, List<Integer> idList)
    throws InvalidSessionHandle {
    Session session = getSession(handle);

    synchronized (session) {
      if (session.deleted)
        throw new InvalidSessionHandle(handle);

      int previousPending = session.getPendingRequestCount();

      List<ResourceGrant> canceledGrants = session.revokeResource(idList);

      if (previousPending <= 0 && (session.getPendingRequestCount() > 0))
        runnableSessions.put(session.sessionId, session);
      return canceledGrants;
    }
  }

  /**
   * Unlike other api's defined by the SessionManager - this one is invoked by
   * the scheduler when it already has a lock on the session and has a valid
   * session handle. The call is routed through the SessionManager to make sure
   * that any indices/views maintained on top of the sessions are maintained 
   * accurately
   */
  public void grantResource(Session session, ResourceRequest req, ResourceGrant grant) {
    session.grantResource(req, grant);
    if (session.getPendingRequestCount() <= 0) {
      runnableSessions.remove(session.sessionId);
    }
  }


  public void setConf(Configuration conf) {
    this.conf = (CoronaConf) conf;
    sessionExpiryInterval = this.conf.getSessionExpiryInterval();
    if (this.expireSessionsThread != null)
      this.expireSessionsThread.interrupt();

  }

  public Configuration getConf() {
    return conf;
  }

  public int getRequestCountForType(String type) {
    int total = 0;
    for (Session session: sessions.values()) {
      synchronized(session) {
        if (session.deleted)
          continue;
        total += session.getRequestCountForType(type);
      }
    }
    return total;
  }

  public int getPendingRequestCountForType(String type) {
    int total = 0;
    for (Session session: sessions.values()) {
      synchronized(session) {
        if (session.deleted)
          continue;
        total += session.getPendingRequestForType(type).size();
      }
    }
    return total;
  }

  public int getRunningSessionCount() {
    return sessions.size();
  }

  class MetricsUpdater implements Runnable {
    public void run() {
      while (!shutdown) {
        try {
          Thread.sleep(5000);
          NodeManager nm = clusterManager.getNodeManager();
          ClusterManagerMetrics metrics = clusterManager.getMetrics();
          for (String resourceType: clusterManager.getTypes()) {
            int pending = getPendingRequestCountForType(resourceType);
            int running = getRequestCountForType(resourceType) - pending;
            int totalSlots = nm.getMaxCpuForType(resourceType);
            int freeSlots = totalSlots - nm.getAllocatedCpuForType(resourceType);
            metrics.setPendingRequestCount(resourceType, pending);
            metrics.setRunningRequestCount(resourceType, running);
            metrics.setTotalSlots(resourceType, totalSlots);
            metrics.setFreeSlots(resourceType, freeSlots);
          }
        } catch (InterruptedException iex) {
          // ignore. if shutting down, while cond. will catch it
        } catch (Exception t) {
          LOG.error("Session Expiry Thread got exception: " +
                    StringUtils.stringifyException(t));
        }
      }
    }
  }

  class ExpireSessions implements Runnable {

    public ExpireSessions() {
    }

    public void run() {
      while (!shutdown) {
        try {
          Thread.sleep(sessionExpiryInterval/2);
          long now = ClusterManager.clock.getTime();
          for(Session session: sessions.values()) {
            long gap = now - session.lastHeartbeatTime;
            if (gap > sessionExpiryInterval) {
              LOG.warn("Timing out session: " + session.getName() +
                " after a heartbeat gap of " + gap + " msec");
              try {
                clusterManager.sessionEnd(session.getHandle(), SessionStatus.TIMED_OUT);
              } catch (Exception e) {}
            }
          }

        } catch (InterruptedException iex) {
          // ignore. if shutting down, while cond. will catch it
        } catch (Exception t) {
          LOG.error("Session Expiry Thread got exception: " +
                    StringUtils.stringifyException(t));
        }
      }
    }
  }

  public static final int MAX_RETIRED_SESSIONS = 1000;
  protected ArrayDeque<RetiredSession> retiredSessions =
    new ArrayDeque<RetiredSession>();

  protected void retireSession(Session session) {
    synchronized (retiredSessions) {
      while (retiredSessions.size() > MAX_RETIRED_SESSIONS) {
        retiredSessions.remove();
      }
      retiredSessions.add(new RetiredSession(session));
    }
  }

  public Collection<RetiredSession> getRetiredSessions() {
    return retiredSessions;
  }

}
