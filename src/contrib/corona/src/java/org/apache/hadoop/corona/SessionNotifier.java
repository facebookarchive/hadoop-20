package org.apache.hadoop.corona;

import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.thrift.TBase;

/**
 * Session Notifier accepts notifications for the session drivers in a non-blocking
 * manner and dispatches them asynchronously. This class maintains a pool of threads
 * and divides dispatch of session notifications across this pool.
 *
 * If notifications to a session cannot be dispatched - then the notifier tells the
 * cluster manager to terminate the session.
 */
public class SessionNotifier implements Configurable {

  public static final Log LOG = LogFactory.getLog(SessionNotifier.class);

  public final static String callToHandle(TBase call) {
    if (call instanceof SessionDriverService.grantResource_args) {
      return (( SessionDriverService.grantResource_args)call).handle;
    } else if (call instanceof  SessionDriverService.revokeResource_args) {
      return (( SessionDriverService.revokeResource_args)call).handle;
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
            ctx = new SessionNotificationCtx(handle,
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
          }
        }

        // we have work (most likely)

        // first process deletions if any
        Set<String> handles = deletedSessions.keySet();
        for (String handle: handles) {
          SessionNotificationCtx ctx = sessionsToCtx.remove(handle);
          // close the session notifier to clear sockets
          if (ctx != null) {
            ctx.close();
          }

          // remove the session from the list of sessions to be deleted
          handles.remove(handle);
        }

        long now = ClusterManager.clock.getTime();
        for (SessionNotificationCtx ctx: sessionsToCtx.values()) {
          if (!ctx.makeCalls(now)) {
            try {
              clusterManager.sessionEnd(ctx.getSessionHandle(), SessionStatus.TIMED_OUT);
            } catch (Exception e) {}
          }
        }
      }
    }
  }

  private SessionNotifierThread handleToNotifier(String handle) {
    return notifierThreads[Math.abs(handle.hashCode()) % numNotifierThreads];
  }

  public SessionNotifier(SessionManager sessionManager,
      ClusterManager clusterManager, ClusterManagerMetrics metrics) {
    this.sessionManager = sessionManager;
    this.clusterManager = clusterManager;
    this.metrics = metrics;
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

  public void setConf(Configuration _conf) {
    this.conf = (CoronaConf) _conf;
    waitInterval = conf.getNotifierPollInterval();
    // TODO - get this from the conf
    numNotifierThreads = 17;
    notifierThreads = new SessionNotifierThread [numNotifierThreads];
    for (int i=0; i<numNotifierThreads; i++) {
      notifierThreads[i] = new SessionNotifierThread();
      notifierThreads[i].setDaemon(true);
      notifierThreads[i].setName("Session Notifier Thread #" + i);
      notifierThreads[i].start();
    }
    
  }

  public Configuration getConf() {
    return conf;
  }
}