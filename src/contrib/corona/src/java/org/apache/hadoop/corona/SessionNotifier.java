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
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.CoronaSerializer;
import org.apache.thrift.TBase;
import org.codehaus.jackson.JsonGenerator;

/**
 * Session Notifier accepts notifications for the session drivers in a
 * non-blocking manner and dispatches them asynchronously. This class maintains
 * a pool of threads and divides dispatch of session notifications
 * across this pool.
 *
 * If notifications to a session cannot be dispatched - then the notifier tells
 * the cluster manager to terminate the session.
 */
public class SessionNotifier implements Configurable {
  /** Class logger */
  public static final Log LOG = LogFactory.getLog(SessionNotifier.class);

  public final static String callToHandle(TBase call) {
    if (call instanceof SessionDriverService.grantResource_args) {
      return (( SessionDriverService.grantResource_args)call).handle;
    } else if (call instanceof  SessionDriverService.revokeResource_args) {
      return (( SessionDriverService.revokeResource_args)call).handle;
    } else if (call instanceof  SessionDriverService.processDeadNode_args) {
      return (( SessionDriverService.processDeadNode_args)call).handle;
    } else {
      throw new RuntimeException("Unknown Class: " + call.getClass().getName());
    }
  }

  protected final SessionManager sessionManager;
  protected final ClusterManager clusterManager;
  protected final ClusterManagerMetrics metrics;

  protected CoronaConf conf;

  protected int numNotifierThreads;
  protected SessionNotifierThread [] notifierThreads;

  protected int waitInterval;

  /**
   * The sessionsToCtx map that has been read from the disk. This will be
   * cleared when the SessionNotifierThread instances are fully restored
   */
  Map<String, SessionNotificationCtx> sessionsToCtxFromDisk;
  /** The deletedSessions set that has been read from the disk. This will be
   * cleared when the SessionNotifierThread instances are fully restored
   */
  Set<String> deletedSessionsFromDisk;

  /**
   * Constructor for SessionNotifier
   *
   * @param sessionManager The SessionManager instance
   * @param clusterManager The ClusterManager instance
   * @param metrics The ClusterManagerMetrics instance
   */
  public SessionNotifier(SessionManager sessionManager,
                         ClusterManager clusterManager,
                         ClusterManagerMetrics metrics) {
    this.sessionManager = sessionManager;
    this.clusterManager = clusterManager;
    this.metrics = metrics;
  }

  /**
   * Constructor for SessionNotifier, used when we are reading back the
   * ClusterManager state from the disk
   *
   * @param sessionManager The SessionManager instance
   * @param clusterManager The ClusterManager instance
   * @param metrics The ClusterManagerMetrics instance
   * @param coronaSerializer The CoronaSerializer instance, which will be used
   *                         to read JSON from disk
   * @throws IOException
   */
  public SessionNotifier(SessionManager sessionManager,
                         ClusterManager clusterManager,
                         ClusterManagerMetrics metrics,
                         CoronaSerializer coronaSerializer) throws IOException {
    this(sessionManager, clusterManager, metrics);
    sessionsToCtxFromDisk = new HashMap<String, SessionNotificationCtx>();
    deletedSessionsFromDisk = new TreeSet<String>();
    int totalSessionsToCtx, totalDeletedSessions;

    coronaSerializer.readStartObjectToken("SessionNotifier");

    coronaSerializer.readField("totalSessionsToCtx");
    totalSessionsToCtx = coronaSerializer.readValueAs(Integer.class);

    coronaSerializer.readField("sessionsToCtx");
    coronaSerializer.readStartObjectToken("sessionsToCtx");
    for (int i = 0; i < totalSessionsToCtx; i++) {
      String handle = coronaSerializer.readValueAs(String.class);
      try {
        SessionNotificationCtx sessionNotificationCtx =
          new SessionNotificationCtx(sessionManager, coronaSerializer);
        sessionsToCtxFromDisk.put(handle, sessionNotificationCtx);
       } catch (IOException e) {
         LOG.warn("Unable to serialize SessionNotificationCtx for " + handle);
       }
    }
    coronaSerializer.readEndObjectToken("sessionsToCtx");

    coronaSerializer.readField("totalDeletedSessions");
    totalDeletedSessions = coronaSerializer.readValueAs(Integer.class);

    coronaSerializer.readField("deletedSessions");
    coronaSerializer.readStartArrayToken("totalDeletedSessions");
    for (int i = 0; i < totalDeletedSessions; i++) {
      deletedSessionsFromDisk.add(coronaSerializer.readValueAs(String.class));
    }
    coronaSerializer.readEndArrayToken("deletedSessions");

    coronaSerializer.readEndObjectToken("SessionNotifier");
  }

  /**
   * Used to write the state of the SessionNotifier instance to disk, when we
   * are persisting the state of the ClusterManager
   *
   * @param jsonGenerator The JsonGenerator instance being used to write JSON
   *                      to disk
   * @throws IOException
   */
  public void write(JsonGenerator jsonGenerator) throws IOException {
    jsonGenerator.writeStartObject();

    int totalSessionsToCtx = 0, totalDeletedSessions = 0;
    for (int i = 0; i < numNotifierThreads; i++) {
      totalSessionsToCtx += notifierThreads[i].sessionsToCtx.size();
      totalDeletedSessions += notifierThreads[i].deletedSessions.size();
    }

    jsonGenerator.writeNumberField("totalSessionsToCtx",
                                    totalSessionsToCtx);

    jsonGenerator.writeFieldName("sessionsToCtx");
    jsonGenerator.writeStartObject();
    for (int i = 0; i < numNotifierThreads; i++) {
      for (ConcurrentMap.Entry<String, SessionNotificationCtx> entry :
        notifierThreads[i].sessionsToCtx.entrySet()) {
        jsonGenerator.writeFieldName(entry.getKey());
        entry.getValue().write(jsonGenerator);
      }
    }
    jsonGenerator.writeEndObject();

    jsonGenerator.writeNumberField("totalDeletedSessions",
                                    totalDeletedSessions);

    jsonGenerator.writeFieldName("deletedSessions");
    jsonGenerator.writeStartArray();
    for (int i = 0; i < numNotifierThreads; i++) {
      for (String deletedSessionHandle :
            notifierThreads[i].deletedSessions.keySet()) {
        jsonGenerator.writeString(deletedSessionHandle);
      }
    }
    jsonGenerator.writeEndArray();

    jsonGenerator.writeEndObject();
  }

  public int getNumPendingCalls() {
    int numQueued = 0;
    for (SessionNotifierThread notifier: notifierThreads) {
      for (SessionNotificationCtx ctx: notifier.sessionsToCtx.values()) {
        numQueued += ctx.getNumPendingCalls();
      }
    }
    return numQueued;
  }

  private class SessionNotifierThread extends Thread {

    // calls are queued into per session context for processing by
    // monitor thread
    ConcurrentMap<String, SessionNotificationCtx> sessionsToCtx
      = new ConcurrentHashMap<String, SessionNotificationCtx> ();

    // session deletions are also enqueued for processing by
    // monitor thread
    ConcurrentMap<String, Object> deletedSessions =
      new ConcurrentHashMap<String, Object> ();

    synchronized private void wakeupThread() {
      this.notify();
    }

    public void deleteSession(String handle) {
      SessionNotificationCtx ctx = sessionsToCtx.get(handle);
      if (ctx != null) {
        deletedSessions.put(handle, this);
        wakeupThread();
      }
    }

    public void addCall(TBase call) {
      String handle = callToHandle(call);
      try {
        Session session = sessionManager.getSession(handle);
        SessionNotificationCtx ctx;

        // make sure there's only one context per session
        synchronized (session) {
          ctx = sessionsToCtx.get(handle);
          if (ctx == null) {
            ctx = new SessionNotificationCtx(session, handle,
                                             session.getAddress().host,
                                             session.getAddress().port);
            ctx.setConf(getConf());
            sessionsToCtx.put(handle, ctx);
          }
        }

        ctx.addCall(call);
        wakeupThread();
      } catch (InvalidSessionHandle e) {
        // this seems impossible. notifications are only issued to
        // valid sessions. log and eat
        LOG.warn("Trying to add call for invalid session: " + handle);
      }
    }
    
    @Override
    public void run() {
      while (true) {
        synchronized (this) {
          // wait to be woken up or for timeout to expire
          // the timeout is used so that retries can be processed
          // interrupts can cause false wakeup - but the cost and
          // frequency should be low enough to ignore them
          try {
            this.wait(waitInterval);
          } catch (InterruptedException e) {
            LOG.error("Waiting was interrupted", e);
          }
        }

        // we have work (most likely)

        // first process deletions if any
        Set<String> handles = deletedSessions.keySet();
        for (String handle: handles) {
          SessionNotificationCtx ctx = sessionsToCtx.remove(handle);
          LOG.info("Close session " + handle);
          // close the session notifier to clear sockets
          if (ctx != null) {
            ctx.close();
          }

          // remove the session from the list of sessions to be deleted
          handles.remove(handle);
        }

        long now = ClusterManager.clock.getTime();
        for (SessionNotificationCtx ctx: sessionsToCtx.values()) {
          String clientInfo = "Notifier to " + ctx.getSessionHandle() +
            " (" + ctx.host + ":" + ctx.port + ")";
          Thread.currentThread().setName(clientInfo);
          if (!ctx.makeCalls(now)) {
            try {
              clusterManager.sessionEnd(ctx.getSessionHandle(),
                                        SessionStatus.TIMED_OUT);
            } catch (InvalidSessionHandle e) {
              LOG.warn(
                "Ignoring error while expiring session " +
                ctx.getSessionHandle(), e);
            } catch (SafeModeException e) {
              LOG.info("Cluster Manager in Safe Mode") ;
            } catch (org.apache.thrift.TException e) {
              // Should not happen since we are making a function call,
              // not thrift call.
              LOG.warn(
                "Ignoring error while expiring session " +
                  ctx.getSessionHandle(), e);
            }
          }
        }
      }
    }
  }

  private SessionNotifierThread handleToNotifier(String handle) {
    return notifierThreads[Math.abs(handle.hashCode()) % numNotifierThreads];
  }

  public void notifyGrantResource(String handle, List<ResourceGrant> granted) {
    reportGrantMetrics(granted);
    handleToNotifier(handle).addCall
      (new  SessionDriverService.grantResource_args(handle, granted));
  }

  public void notifyRevokeResource(String handle, List<ResourceGrant> revoked, boolean force) {
    reportRevokeMetrics(revoked);
    handleToNotifier(handle).addCall
      (new  SessionDriverService.revokeResource_args(handle, revoked, force));
  }

  public void notifyDeadNode(String handle, String nodeName) {
    handleToNotifier(handle).addCall(
       new SessionDriverService.processDeadNode_args(handle, nodeName));
  }

  protected void reportGrantMetrics(Collection<ResourceGrant> granted) {
    for (ResourceGrant grant : granted) {
      metrics.grantResource(grant.type);
    }
  }

  protected void reportRevokeMetrics(Collection<ResourceGrant> revoked) {
    for (ResourceGrant grant : revoked) {
      metrics.revokeResource(grant.type);
    }
  }

  public void deleteSession(String handle) {
    handleToNotifier(handle).deleteSession(handle);
  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = (CoronaConf) conf;
    waitInterval = this.conf.getNotifierPollInterval();
    numNotifierThreads = this.conf.getCMNotifierThreadCount();
    notifierThreads = new SessionNotifierThread [numNotifierThreads];
    for (int i = 0; i < numNotifierThreads; i++) {
      notifierThreads[i] = new SessionNotifierThread();
      notifierThreads[i].setDaemon(true);
      notifierThreads[i].setName("Session Notifier Thread #" + i);

      // If we are in Safe Mode, we haven't yet restored the SessionNotifier
      // completely, so we won't be starting the threads now. We would do that
      // in the restoreAfterSafeModeRestart() method.
      if (!clusterManager.safeMode) {
        notifierThreads[i].start();
      }
    }
  }

  /**
   * This method rebuilds members related to the SessionNotifier instance,
   * which were not directly persisted themselves.
   */
  public void restoreAfterSafeModeRestart() {
    // Put the sessionsToCtxFromDisk entries into their respective
    // SessionNotifierThreads instances
    for (Map.Entry<String, SessionNotificationCtx> entry :
          sessionsToCtxFromDisk.entrySet()) {
      // The conf and the conf related properties are missing in the
      // sessionsToCtx objects
      entry.getValue().setConf(conf);
      handleToNotifier(entry.getKey()).sessionsToCtx.put(entry.getKey(),
                                                          entry.getValue());
      sessionsToCtxFromDisk.remove(entry);
    }

    // Put the deletedSessions into the the respective SessionNotifierThreads
    for (String deletedSessionHandle : deletedSessionsFromDisk) {
      SessionNotifierThread notifierThread =
        handleToNotifier(deletedSessionHandle);
      if (notifierThread.sessionsToCtx.get(deletedSessionHandle) != null) {
        notifierThread.deletedSessions.put(deletedSessionHandle,
                                            notifierThread);
      }
      deletedSessionsFromDisk.remove(deletedSessionHandle);
    }

    // We can now start the notifier threads
    for (int i = 0; i < numNotifierThreads; i++) {
      notifierThreads[i].start();
    }
  }


  public Configuration getConf() {
    return conf;
  }
}
