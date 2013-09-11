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

import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
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
import org.apache.hadoop.util.CoronaSerializer;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.JsonToken;

/**
 * Manages a collection of sessions
 */
public class SessionManager implements Configurable {
  private static final Log LOG = LogFactory.getLog(SessionManager.class);
  private static final String DATE_FORMAT_PATTERN = "yyyyMMddHHmm";

  private ArrayDeque<RetiredSession> retiredSessions =
    new ArrayDeque<RetiredSession>();

  private CoronaConf conf;
  private ClusterManager clusterManager;
  private AtomicLong sessionCounter = new AtomicLong();
  /** The number of resource requests/releases to process under the
   *  session lock. Not configurable for now */
  private int requestBatchSize = 1000;
  private int sessionExpiryInterval;
  private int numRetiredSessions;
  private Thread expireSessionsThread = null;
  private ExpireSessions expireSessions = new ExpireSessions();
  private Thread metricsUpdaterThread;
  private MetricsUpdater metricsUpdater = new MetricsUpdater();
  private volatile boolean shutdown = false;
  private String startTime;

  // 1: primary data structure
  private ConcurrentMap<String, Session> sessions
    = new ConcurrentHashMap<String, Session>();

  // 2: list of all the sessions who need compute resources right now
  private ConcurrentMap<String, Session> runnableSessions
    = new ConcurrentHashMap<String, Session>();

  /**
   * Constructor for SessionManager
   *
   * @param clusterManager The ClusterManager instance to be used
   */
  public SessionManager(ClusterManager clusterManager) {
    DateFormat dateFormat = new SimpleDateFormat(DATE_FORMAT_PATTERN);
    this.startTime = dateFormat.format(new Date(clusterManager.getStartTime()));
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

  /**
   * Constructor for SessionManager, used when we are reading back the
   * ClusterManager state from the disk
   *
   * @param clusterManager The ClusterManager instance to be used
   * @param coronaSerializer The CoronaSerializer instance, which will be used
   *                         to read JSON from disk
   * @throws IOException
   */
  public SessionManager(ClusterManager clusterManager,
                        CoronaSerializer coronaSerializer)
    throws  IOException {
    this(clusterManager);
    // Even though the expireSessions thread would be running now, it would
    // not expire any sessions we would be creating now, because the
    // ClusterManager would be in Safe Mode.

    // Expecting the START_OBJECT token for sessionManager
    coronaSerializer.readStartObjectToken("sessionManager");

    readSessions(coronaSerializer);

    coronaSerializer.readField("sessionCounter");
    sessionCounter = new AtomicLong(coronaSerializer.readValueAs(Long.class));

    // Expecting the END_OBJECT token for sessionManager
    coronaSerializer.readEndObjectToken("sessionManager");

    // Restoring the runnableSessions map
    for (String sessionId : sessions.keySet()) {
      Session session = sessions.get(sessionId);
      if (session.getPendingRequestCount() > 0) {
        runnableSessions.put(sessionId, session);
      }
    }
  }

  /**
   * Reads back the sessions map from a JSON stream
   *
   * @param coronaSerializer The CoronaSerializer instance to be used to
   *                         read the JSON
   * @throws IOException
   */
  private void readSessions(CoronaSerializer coronaSerializer)
    throws IOException {
    coronaSerializer.readField("sessions");
    // Expecting the START_OBJECT token for sessions
    coronaSerializer.readStartObjectToken("sessions");
    JsonToken current = coronaSerializer.nextToken();
    while (current != JsonToken.END_OBJECT) {
      String sessionId = coronaSerializer.getFieldName();
      Session session = new Session(clusterManager.conf.getCMHeartbeatDelayMax(),
        coronaSerializer);
      sessions.put(sessionId, session);
      current = coronaSerializer.nextToken();
    }
    // Done with reading the END_OBJECT token for sessions
  }

  /**
   * This method rebuilds members related to the SessionManager instance,
   * which were not directly persisted themselves.
   */
  public void restoreAfterSafeModeRestart() {
    if (!clusterManager.safeMode) {
      return;
    }

    for (Session session : sessions.values()) {
      for (ResourceRequestInfo resourceRequestInfo :
        session.idToRequest.values()) {

        // The helper method to restore the ResourceRequestInfo instances
        // is placed in NodeManager because it makes use of other members
        // of NodeManager
        clusterManager.nodeManager.
          restoreResourceRequestInfo(resourceRequestInfo);
      }
      session.restoreAfterSafeModeRestart();
      clusterManager.getScheduler().addSession(session.getSessionId(),
        session);
    }

    clusterManager.getMetrics().setNumRunningSessions(sessions.size());
  }


  /**
   * Used to write the state of the SessionManager instance to disk, when we
   * are persisting the state of the ClusterManager
   * @param jsonGenerator The JsonGenerator instance being used to write JSON
   *                      to disk
   * @throws IOException
   */
  public void write(JsonGenerator jsonGenerator) throws IOException {
    jsonGenerator.writeStartObject();
    // retiredSessions and numRetiredSessions need not be persisted

    // sessionCounter can be set to 0, when the SessionManager is instantiated

    // sessions begins
    jsonGenerator.writeFieldName("sessions");
    jsonGenerator.writeStartObject();
    for (String sessionId : sessions.keySet()) {
      jsonGenerator.writeFieldName(sessionId);
      sessions.get(sessionId).write(jsonGenerator);
    }
    jsonGenerator.writeEndObject();
    // sessions ends

    jsonGenerator.writeNumberField("sessionCounter",
                                    sessionCounter.longValue());

    jsonGenerator.writeEndObject();

    // We can rebuild runnableSessions
    // No need to write startTime and numRetiredSessions
  }

  public Set<String> getSessions() {
    return sessions.keySet();
  }

  /**
   * Helper class for getTypePoolInfoAveWaitMs().
   */
  private static class WaitCount {
    /** Total waited msecs */
    private long totalWaitMsecs;
    /** Number of entries */
    private int count;

    /**
     * Constructor.
     * @param intialWaitMsecs Initial waited msecs
     */
    WaitCount(long intialWaitMsecs) {
      totalWaitMsecs = intialWaitMsecs;
      count = 1;
    }

    /**
     * Add wait msecs
     * @param waitMsecs Waited msecs
     */
    void addWaitMsecs(long waitMsecs) {
      totalWaitMsecs += waitMsecs;
      ++count;
    }

    /**
     * Get the average wait.
     * @return total wait msecs / count
     */
    long getAverageWait() {
      return totalWaitMsecs / count;
    }
  }

  /**
   * Get a map of pool infos to average wait times for first
   * resource of a resource type.
   * @param type Resource type
   * @return Map of pools into average first resource time
   */
  public Map<PoolInfo, Long> getTypePoolInfoAveFirstWaitMs(ResourceType type) {
    Map<PoolInfo, WaitCount> poolInfoWaitCount =
        new HashMap<PoolInfo, WaitCount>();
    for (Session session : sessions.values()) {
      synchronized (session) {
        if (!session.isDeleted()) {
          Long wait = session.getTypeFirstWaitMs(type);
          if (wait == null) {
            continue;
          }

          WaitCount waitCount = poolInfoWaitCount.get(session.getPoolInfo());
          if (waitCount == null) {
            poolInfoWaitCount.put(session.getPoolInfo(),
                                  new WaitCount(wait));
          } else {
            waitCount.addWaitMsecs(wait);
          }
        }
      }
    }
    Map<PoolInfo, Long> poolInfoWaitMs =
        new HashMap<PoolInfo, Long>(poolInfoWaitCount.size());
    for (Map.Entry<PoolInfo, WaitCount> entry : poolInfoWaitCount.entrySet()) {
      poolInfoWaitMs.put(entry.getKey(), entry.getValue().getAverageWait());
    }
    return poolInfoWaitMs;
  }

  public Session getSession(String handle) throws InvalidSessionHandle {
    Session session = sessions.get(handle);
    if (session == null) {
      throw new InvalidSessionHandle(handle);
    }
    return session;
  }

  public List<Session> getRunnableSessions() {
    List<Session> ret = new ArrayList<Session>(runnableSessions.size());
    ret.addAll(runnableSessions.values());
    return ret;
  }

  public String getNextSessionId() {
    String sessionId = startTime + "." + sessionCounter.incrementAndGet();
    return sessionId;
  }

  public Session addSession(String sessionId, SessionInfo info)
    throws InvalidSessionHandle {
    if (!sessionId.startsWith(startTime)) {
      throw new InvalidSessionHandle(
        "Session belongs to a different start time " + sessionId);
    }
    if (sessions.containsKey(sessionId)) {
      throw new InvalidSessionHandle("Session already started " + sessionId);
    }

    Session session = new Session(conf.getCMHeartbeatDelayMax(), sessionId, info,
        clusterManager.getScheduler().getConfigManager());
    PoolGroupManager.checkPoolInfoIfStrict(
        session.getPoolInfo(),
        clusterManager.getScheduler().getConfigManager(),
        conf);
    sessions.put(sessionId, session);
    clusterManager.getMetrics().sessionStart();
    clusterManager.getMetrics().setNumRunningSessions(sessions.size());
    clusterManager.getScheduler().addSession(sessionId.toString(), session);
    LOG.info("Add Session " +
      sessionId + " -> " +
      info.getName() + "@" +
      info.getAddress().getHost() + ":" +
      info.getAddress().getPort());
    return session;
  }

  public void updateInfo(String handle, SessionInfo info)
    throws InvalidSessionHandle {
    Session session = getSession(handle);

    synchronized (session) {
      if (session.isDeleted()) {
        throw new InvalidSessionHandle(handle);
      }

      session.updateInfoUrlAndName(info.url, info.name);
      session.updateSessionPriority(info.priority);
      session.updateSessionDeadline(info.deadline);
    }
  }


  public Collection<ResourceGrant> deleteSession(String handle,
                                                 SessionStatus status)
    throws InvalidSessionHandle {
    Session session = getSession(handle);

    synchronized (session) {
      if (session.isDeleted()) {
        throw new InvalidSessionHandle(handle);
      }

      session.setDeleted();
      session.setStatus(status);
      sessions.remove(session.getSessionId());
      clusterManager.getNodeManager().deleteSession(handle);
      clusterManager.getMetrics().setNumRunningSessions(sessions.size());
      clusterManager.getMetrics().sessionEnd(status);
      runnableSessions.remove(session.getSessionId());
      retireSession(session);
    }

    return session.getGrants();
  }

  public void heartbeat(String handle)  throws InvalidSessionHandle {
    Session session = getSession(handle);
    session.heartbeat();
  }
  
  public void heartbeatV2(String handle, HeartbeatArgs jtInfo) throws InvalidSessionHandle {
    Session session = getSession(handle);
    
    session.heartbeat();
    session.storeResourceUsages(jtInfo.resourceUsages);
  }

  public void requestResource(
    String handle, List<ResourceRequestInfo> requestList)
    throws InvalidSessionHandle {
    Session session = getSession(handle);
    int listSize = requestList.size();
    // Limit the number of requests to process under the session lock.
    // This is required to prevent slow down of the scheduler threads, which
    // need to grab the session lock for all running sessions.
    for (int i = 0; i < listSize;) {
      int toIndex = Math.min(i + requestBatchSize, listSize);
      List<ResourceRequestInfo> toProcess = requestList.subList(i, toIndex);
      i += toIndex - i;
      synchronized (session) {
        if (session.isDeleted()) {
          throw new InvalidSessionHandle(handle);
        }

        int previousPending = session.getPendingRequestCount();
        session.requestResource(toProcess);
        if (previousPending <= 0 && (session.getPendingRequestCount() > 0)) {
          runnableSessions.put(session.getSessionId(), session);
        }
      }
    }
  }

  public Collection<ResourceGrant> releaseResource(
    String handle, List<Integer> idList)
    throws InvalidSessionHandle {
    Session session = getSession(handle);
    List<ResourceGrant> canceledGrants = null;

    int listSize = idList.size();
    // Limit the number of releases to process under the session lock.
    // This is required to prevent slow down of the scheduler threads, which
    // need to grab the session lock for all running sessions.
    for (int i = 0; i < listSize;) {
      int toIndex = Math.min(i + requestBatchSize, listSize);
      List<Integer> toProcess = idList.subList(i, toIndex);
      i += toIndex - i;
      synchronized (session) {
        if (session.isDeleted()) {
          throw new InvalidSessionHandle(handle);
        }

        if (canceledGrants == null) {
          canceledGrants = session.releaseResource(toProcess);
        } else {
          canceledGrants.addAll(session.releaseResource(toProcess));
        }
        if (session.getPendingRequestCount() <= 0) {
          runnableSessions.remove(session.getSessionId());
        }
      }
    }
    return canceledGrants;
  }

  public List<ResourceGrant> revokeResource(String handle, List<Integer> idList)
    throws InvalidSessionHandle {
    Session session = getSession(handle);

    synchronized (session) {
      if (session.isDeleted()) {
        throw new InvalidSessionHandle(handle);
      }

      int previousPending = session.getPendingRequestCount();

      List<ResourceGrant> canceledGrants = session.revokeResource(idList);

      if (previousPending <= 0 && (session.getPendingRequestCount() > 0)) {
        runnableSessions.put(session.getSessionId(), session);
      }
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
  public void grantResource(
    Session session, ResourceRequestInfo req, ResourceGrant grant) {
    session.grantResource(req, grant);
    if (session.getPendingRequestCount() <= 0) {
      runnableSessions.remove(session.getSessionId());
    }
  }


  public void setConf(Configuration conf) {
    this.conf = (CoronaConf) conf;
    sessionExpiryInterval = this.conf.getSessionExpiryInterval();
    numRetiredSessions = this.conf.getNumRetiredSessions();
    LOG.info("Will keep " + numRetiredSessions + " retired sessions in memory");
    if (this.expireSessionsThread != null) {
      this.expireSessionsThread.interrupt();
    }
  }

  public Configuration getConf() {
    return conf;
  }

  public int getRequestCountForType(ResourceType type) {
    int total = 0;
    for (Session session: sessions.values()) {
      synchronized (session) {
        if (session.isDeleted()) {
          continue;
        }
        total += session.getRequestCountForType(type);
      }
    }
    return total;
  }

  public int getGrantCountForType(ResourceType type) {
    int total = 0;
    for (Session session: sessions.values()) {
      synchronized (session) {
        if (!session.isDeleted()) {
          total += session.getGrantCountForType(type);
        }
      }
    }
    return total;
  }

  public int getPendingRequestCountForType(ResourceType type) {
    int total = 0;
    for (Session session: sessions.values()) {
      synchronized (session) {
        if (session.isDeleted()) {
          continue;
        }
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
          // If the ClusterManager is in Safe Mode, we do not need to update
          // the metrics
          if (clusterManager.safeMode) {
            continue;
          }
          NodeManager nm = clusterManager.getNodeManager();
          ClusterManagerMetrics metrics = clusterManager.getMetrics();
          for (ResourceType resourceType: clusterManager.getTypes()) {
            int pending = getPendingRequestCountForType(resourceType);
            int running = getRequestCountForType(resourceType) - pending;
            int totalSlots = nm.getMaxCpuForType(resourceType);
            int freeSlots =
              totalSlots - nm.getAllocatedCpuForType(resourceType);
            metrics.setPendingRequestCount(resourceType, pending);
            metrics.setRunningRequestCount(resourceType, running);
            metrics.setTotalSlots(resourceType, totalSlots);
            metrics.setFreeSlots(resourceType, freeSlots);
          }
        } catch (InterruptedException iex) {
          // ignore. if shutting down, while cond. will catch it
        }
      }
    }
  }

  class ExpireSessions implements Runnable {
    @Override
    public void run() {
      while (!shutdown) {
        try {
          Thread.sleep(sessionExpiryInterval / 2);
          /**
           * If we are in safe mode, we should not expire any sessions, and
           * reset the last seen time before we come out of safe mode.
           */
          if (clusterManager.safeMode) {
            continue;
          }
          long now = ClusterManager.clock.getTime();
          for (Session session: sessions.values()) {
            long gap = now - session.getLastHeartbeatTime();
            if (gap > sessionExpiryInterval) {
              LOG.warn("Timing out session: " + session.getHandle() +
                " (" + session.getName() + ") " +
                "after a heartbeat gap of " + gap + " msec");
              try {
                clusterManager.sessionEnd(
                  session.getHandle(), SessionStatus.TIMED_OUT);
              } catch (InvalidSessionHandle e) {
                LOG.warn(
                  "Ignoring error while expiring session " +
                  session.getHandle(), e);
              } catch (SafeModeException e) {
                // You could come here, if the safe mode is set while you are
                // in the for-loop.
                LOG.info(
                  "Got a SafeModeException in the Expire Sessions thread");
                // We need not loop any further.
                break;
              } catch (org.apache.thrift.TException e) {
                // Should not happen since we are making a function call,
                // not thrift call.
                LOG.warn(
                  "Ignoring error while expiring session " +
                    session.getHandle(), e);
              }
            }
          }

        } catch (InterruptedException iex) {
          // ignore. if shutting down, while cond. will catch it
        }
      }
    }
  }

  protected void retireSession(Session session) {
    synchronized (retiredSessions) {
      while (retiredSessions.size() > numRetiredSessions) {
        retiredSessions.remove();
      }
      retiredSessions.add(new RetiredSession(session));
    }
  }

  public Collection<RetiredSession> getRetiredSessions() {
    return retiredSessions;
  }

  /**
   * This is required when we come out of safe mode, and we need to reset
   * the lastHeartbeatTime for each session
   */
  public void resetSessionsLastHeartbeatTime() {
    for (Session session : sessions.values()) {
      session.heartbeat();
    }
  }

}
