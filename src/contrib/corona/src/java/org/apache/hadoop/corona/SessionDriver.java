package org.apache.hadoop.corona;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.InetSocketAddress;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Daemon;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.net.NetUtils;

import org.apache.thrift.*;
import org.apache.thrift.protocol.*;
import org.apache.thrift.transport.*;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;


/**
 * Handles sessions from client to cluster manager.
 */
public class SessionDriver {

  public static final Log LOG = LogFactory.getLog(SessionDriver.class);

  final CoronaConf conf;
  final SessionDriverService.Iface iface;

  String sessionId = "";
  SessionInfo sessionInfo;

  ServerSocket serverSocket; 
  TServer server;
  Thread serverThread;

  CMNotifierThread cmNotifier;
  IOException   failException = null;


  public SessionDriver(Configuration conf, SessionDriverService.Iface iface)
    throws IOException {
    this(new CoronaConf(conf), iface);
  }

  public SessionDriver(CoronaConf conf, SessionDriverService.Iface iface)
      throws IOException {
    this.conf = conf;
    this.iface = iface;

    serverSocket = initializeServer(conf);

    org.apache.hadoop.corona.InetAddress myAddress =
      new org.apache.hadoop.corona.InetAddress();
    myAddress.setHost(serverSocket.getInetAddress().getHostAddress());
    myAddress.setPort(serverSocket.getLocalPort());
    LOG.info("My serverSocketPort " + serverSocket.getLocalPort());
    LOG.info("My Address " + myAddress.getHost() + ":" + myAddress.getPort());

    String userName = System.getProperty("user.name");
    String sessionName = userName + "-" + new java.util.Date().toString();
    this.sessionInfo = new SessionInfo();
    this.sessionInfo.setAddress(myAddress);
    this.sessionInfo.setName(sessionName);
    this.sessionInfo.setUserId(userName);
    this.sessionInfo.setPoolId(conf.getPoolName());
    // TODO - set session priority.
    this.sessionInfo.setPriority(SessionPriority.NORMAL);
    this.sessionInfo.setNoPreempt(false);

    this.serverThread = new Daemon(new Thread() {
        public void run() {
          server.serve();
        }
      });
    this.serverThread.start();

    cmNotifier = new CMNotifierThread(conf, sessionInfo, this);
    cmNotifier.setDaemon(true);
    cmNotifier.start();
    sessionId = cmNotifier.getSessionRegistrationData().handle;
  }

  static java.net.InetAddress getLocalAddress() throws IOException {
    try {
      return java.net.InetAddress.getLocalHost();
    } catch (java.net.UnknownHostException e) {
      throw new IOException(e);
    }
  }

  private ServerSocket initializeServer(CoronaConf conf) throws IOException {
    // Choose any free port.
    ServerSocket serverSocket = new ServerSocket(0, 0, getLocalAddress());
    TServerSocket tServerSocket = new TServerSocket(serverSocket,
                                                    conf.getCMSoTimeout());

    TFactoryBasedThreadPoolServer.Args args =
      new TFactoryBasedThreadPoolServer.Args(tServerSocket);
    args.processor(new SessionDriverService.Processor(iface));
    args.transportFactory(new TTransportFactory());
    args.protocolFactory(new TBinaryProtocol.Factory(true, true));
    args.stopTimeoutVal = 0;
    server = new TFactoryBasedThreadPoolServer(
      args, new TFactoryBasedThreadPoolServer.DaemonThreadFactory());
    return serverSocket;
  }

  public IOException getFailed() {
    return failException;
  }

  public void setFailed(IOException e) {
    failException = e;
  }

  public String getSessionId() { return sessionId; }

  public SessionInfo getSessionInfo() { return sessionInfo; }

  public void setName(String name) throws IOException {
    if (failException != null) {
      throw failException;
    }

    if (name == null || name.length() == 0) {
      return;
    }

    sessionInfo.name = name;

    SessionInfo newInfo = new SessionInfo(sessionInfo);
    cmNotifier.addCall(
      new ClusterManagerService.sessionUpdateInfo_args(sessionId, newInfo));
  }

  public void setUrl(String url) throws IOException {
    if (failException != null) {
      throw failException;
    }

    sessionInfo.url = url;

    SessionInfo newInfo = new SessionInfo(sessionInfo);
    cmNotifier.addCall(
      new ClusterManagerService.sessionUpdateInfo_args(sessionId, newInfo));
  }

  /**
   * For test purposes. Abort a sessiondriver without sending a sessionEnd()
   * call to the CM. This allows testing for abnormal session termination
   */
  public void abort() {
    LOG.info("Aborting session driver");
    cmNotifier.clearCalls();
    cmNotifier.doShutdown();
    server.stop();
  }

  public void stop(SessionStatus status) {
    LOG.info("Stopping session driver");

    // clear all calls from the notifier and append a last call
    // to send the sessionEnd 
    cmNotifier.clearCalls();
    cmNotifier.addCall(new ClusterManagerService.sessionEnd_args(sessionId, status));
    cmNotifier.doShutdown();
    server.stop();
  }

  public void join() throws InterruptedException {
    serverThread.join();
    cmNotifier.join();
  }

  public void requestResources(List<ResourceRequest> wanted) throws IOException {
    if (failException != null)
      throw failException;

    cmNotifier.addCall(new ClusterManagerService.requestResource_args(sessionId, wanted));
  }

  public void releaseResources(List<ResourceRequest> released) throws IOException {
    if (failException != null)
      throw failException;

    List<Integer> releasedIds = new ArrayList<Integer>();
    for (ResourceRequest req: released) {
      releasedIds.add(req.getId());
    }
    cmNotifier.addCall(new ClusterManagerService.releaseResource_args(sessionId, releasedIds));
  }

  public static class CMNotifierThread extends Thread { 

    final List<TBase> pendingCalls = Collections.synchronizedList(new LinkedList<TBase> ());

    /**
     * starting retry interval
     */
    final int           retryIntervalStart;

    /**
     * multiplier between successive retry intervals
     */
    final int           retryIntervalFactor;

    /**
     * max number of retries
     */
    final int           retryCountMax;

    /**
     * period between polling for dispatches
     */
    final int           waitInterval;

    /**
     * intervals between heartbeats
     */
    final int           heartbeatInterval;


    final String                  host;
    final int                     port;
    final SessionInfo             sinfo;
    final SessionRegistrationData sreg;
    final SessionDriver           sessionDriver;

    /**
     * Time (in milliseconds) when to make the next RPC call
     * -1 means to make the call immediately
     */
    long                nextDispatchTime = -1;

    /**
     * Number of retries that have been made for the first call 
     * in the list
     */
    short               numRetries = 0;

    /**
     * current retry interval
     */
    int                 currentRetryInterval;

    /**
     * last time heartbeat was sent
     */
    long                lastHeartbeatTime = 0;

    TTransport                    transport = null;
    ClusterManagerService.Client  client;
    volatile boolean  shutdown = false;

    public void doShutdown() {
      shutdown = true;
      wakeupThread();
    }

    public CMNotifierThread(CoronaConf conf, SessionInfo sinfo, SessionDriver sdriver)
      throws IOException {
      waitInterval = conf.getNotifierPollInterval();
      retryIntervalFactor = conf.getNotifierRetryIntervalFactor();
      retryCountMax = conf.getNotifierRetryMax();
      retryIntervalStart = conf.getNotifierRetryIntervalStart();
      heartbeatInterval = Math.max(conf.getSessionExpiryInterval()/8, 1);
      sessionDriver = sdriver;

      String target = conf.getClusterManagerAddress();
      InetSocketAddress address = NetUtils.createSocketAddr(target);
      host = address.getHostName();
      port = address.getPort();
      this.sinfo = sinfo;

      try {
        LOG.info("Connecting to cluster manager at " + host + ":" + port);
        init();
        sreg = client.sessionStart(sinfo);
        close();
        LOG.info("Established session " + sreg.handle);
      } catch (TException e) {
        throw new IOException(e);
      }
    }

    public SessionRegistrationData getSessionRegistrationData() {
      return sreg;
    }

    synchronized private void wakeupThread() {
      this.notify();
    }

    public void addCall(TBase call) {
      pendingCalls.add(call);
      wakeupThread();
    }

    public void clearCalls() {
      pendingCalls.clear();
    }

    private void init () throws TException {
      if (transport == null) {
        transport = new TSocket(host, port);
        client = new ClusterManagerService.Client(new TBinaryProtocol(transport));
        transport.open();
      }
    }

    public void close() {
      if (transport != null) {
        transport.close();
        transport = null;
        client = null;
      }
    }

    private void resetRetryState() {
      nextDispatchTime = -1;
      numRetries = 0;
      currentRetryInterval = retryIntervalStart;
    }

    /**
     * get the first pending call if it exists, else null
     */
    private TBase getCall() {
      try {
        TBase ret = pendingCalls.get(0);
        return (ret);
      } catch (IndexOutOfBoundsException e) {
        return null;
      }
    }

    public void run() {
      while (!shutdown) {
        synchronized (this) {
          try {
            this.wait(waitInterval);
          } catch (InterruptedException e) {
          }
        }

        long now = ClusterManager.clock.getTime();

        try {

          // send heartbeat if one is due
          // if shutdown is ordered, don't send heartbeat
          if (!shutdown && ((now - lastHeartbeatTime) > heartbeatInterval)) {
            init();
            client.sessionHeartbeat(sreg.handle);
            resetRetryState();
            lastHeartbeatTime = now;
          }

          // except in the case of shutdown, wait correct time
          // before trying calls again (in case of failures)
          // in the case of shutdown - we try to send as many pending
          // calls as we can before terminating

          if (!shutdown && (now < nextDispatchTime))
            continue;

          // send pending requests/releases
          while (!pendingCalls.isEmpty()) {
            TBase call = getCall();

            init();
            dispatchCall(call);
            resetRetryState();

            // we can only remove the first element if 
            // it is the call we just dispatched
            TBase currentCall = getCall();
            if (currentCall == call)
              pendingCalls.remove(0);
          }


        } catch (TException e) {

          LOG.error("Call to CM, numRetry: " + numRetries +
                    " failed with exception: \n" + StringUtils.stringifyException(e));

          // close the transport/client on any exception
          // will be reopened on next try
          close();

          if (numRetries > retryCountMax) {
            LOG.error("All retries failed - closing CMNotifier");
            sessionDriver.setFailed(new IOException(e));
            break;
          }

          numRetries++;
          nextDispatchTime = now + currentRetryInterval;
          currentRetryInterval *= retryIntervalFactor;

        } catch (InvalidSessionHandle e) {
          LOG.error("InvalidSession exception - closing CMNotifier");
          sessionDriver.setFailed(new IOException(e));
          break;
        }

      } // while (true)
    } // run()


    private void dispatchCall(TBase call) throws TException, InvalidSessionHandle {
      if (LOG.isDebugEnabled())
        LOG.debug ("Begin dispatching call: " + call.toString());

      if (call instanceof  ClusterManagerService.requestResource_args) {
        ClusterManagerService.requestResource_args args =
          (ClusterManagerService.requestResource_args)call;

        client.requestResource(args.handle, args.requestList);
      } else if (call instanceof  ClusterManagerService.releaseResource_args) {
        ClusterManagerService.releaseResource_args args =
          (ClusterManagerService.releaseResource_args)call;

        client.releaseResource(args.handle, args.idList);
      } else if (call instanceof  ClusterManagerService.sessionEnd_args) {
        ClusterManagerService.sessionEnd_args args =
          (ClusterManagerService.sessionEnd_args)call;

        client.sessionEnd(args.handle, args.status);
      } else if (call instanceof  ClusterManagerService.sessionUpdateInfo_args) {
        ClusterManagerService.sessionUpdateInfo_args args =
          (ClusterManagerService.sessionUpdateInfo_args)call;

        client.sessionUpdateInfo(args.handle, args.info);
      } else {
        throw new RuntimeException("Unknown Class: " + call.getClass().getName());
      }

      LOG.debug ("End dispatch call");

    }
  }
}
