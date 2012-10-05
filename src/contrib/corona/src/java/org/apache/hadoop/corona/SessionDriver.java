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
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import javax.security.auth.login.LoginException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.UnixUserGroupInformation;
import org.apache.hadoop.util.Daemon;
import org.apache.hadoop.util.StringUtils;
import org.apache.thrift.TBase;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.server.TServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportFactory;


/**
 * Handles sessions from client to cluster manager.
 */
public class SessionDriver {

  /** Logger */
  public static final Log LOG = LogFactory.getLog(SessionDriver.class);

  /** The configuration of the session */
  private final CoronaConf conf;
  /** The processor for the callback server */
  private final SessionDriverService.Iface iface;

  /** The id of the underlying session */
  private String sessionId = "";
  /** The pool that the cluster manager assigns this session to. */
  private String poolName = "";
  /** The location of the log for the underlying session */
  private String sessionLog = "";
  /** The info of the underlying session */
  private SessionInfo sessionInfo;

  /** Callback server socket */
  private ServerSocket serverSocket;
  /** Callback Thrift Server */
  private TServer server;
  /** Callback server thread */
  private Thread serverThread;

  /** The thread that handles the dispatch of the calls to the CM */
  private CMNotifierThread cmNotifier;
  /** The exception this SessionDriver has failed with */
  private volatile IOException failException = null;


  /**
   * Construct a session driver given the configuration and the processor
   * for the thrift calls into this session
   * @param conf the configuration of the session
   * @param iface the processor for the thrift calls
   * @throws IOException
   */
  public SessionDriver(Configuration conf, SessionDriverService.Iface iface)
    throws IOException {
    this(new CoronaConf(conf), iface);
  }

  /**
   * Construct a session driver given the configuration and the processor
   * for the thrift calls into this session
   * @param conf the configuration of the session
   * @param iface the processor for the thrift calls
   * @throws IOException
   */
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

    UnixUserGroupInformation ugi = getUGI(conf);
    String userName = ugi.getUserName();
    String sessionName = userName +
        "@" + myAddress.getHost() + ":" + myAddress.getPort() +
        "-" + new java.util.Date().toString();
    this.sessionInfo = new SessionInfo();
    this.sessionInfo.setAddress(myAddress);
    this.sessionInfo.setName(sessionName);
    this.sessionInfo.setUserId(userName);
    this.sessionInfo.setPoolId(conf.getPoolName());
    this.sessionInfo.setPriority(SessionPriority.NORMAL);
    this.sessionInfo.setNoPreempt(false);

    this.serverThread = new Daemon(new Thread() {
        @Override
        public void run() {
          server.serve();
        }
      });
    this.serverThread.start();

    cmNotifier = new CMNotifierThread(conf, sessionInfo, this);
    cmNotifier.setDaemon(true);
    cmNotifier.start();
    sessionId = cmNotifier.getSessionRegistrationData().getHandle();
    sessionLog = cmNotifier.getSessionRegistrationData()
      .getClusterManagerInfo().getJobHistoryLocation();
    poolName = cmNotifier.getSessionRegistrationData().getPool();
    sessionInfo.setPoolId(poolName);
    LOG.info("Session " + sessionId + " job history location " + sessionLog +
      " pool " + poolName);
  }

  /**
   * A helper method to get the {@link UnixUserGroupInformation} for the
   * owner of the process
   * @param conf the configuration of the process
   * @return the {@link UnixUserGroupInformation} of the owner of the process
   * @throws IOException
   */
  private static UnixUserGroupInformation getUGI(Configuration conf)
    throws IOException {
    UnixUserGroupInformation ugi = null;
    try {
      ugi = UnixUserGroupInformation.login(conf, true);
    } catch (LoginException e) {
      throw (IOException) (new IOException(
          "Failed to get the current user's information.").initCause(e));
    }
    return ugi;
  }

  /**
   * A helper function to get the local address of the machine
   * @return the local address of the current machine
   * @throws IOException
   */
  static java.net.InetAddress getLocalAddress() throws IOException {
    try {
      return java.net.InetAddress.getLocalHost();
    } catch (java.net.UnknownHostException e) {
      throw new IOException(e);
    }
  }

  /**
   * Start the SessionDriver callback server
   * @param conf the corona configuration for this session
   * @return the server socket of the callback server
   * @throws IOException
   */
  private ServerSocket initializeServer(CoronaConf conf) throws IOException {
    // Choose any free port.
    ServerSocket sessionServerSocket =
      new ServerSocket(0, 0, getLocalAddress());
    TServerSocket tServerSocket = new TServerSocket(sessionServerSocket,
                                                    conf.getCMSoTimeout());

    TFactoryBasedThreadPoolServer.Args args =
      new TFactoryBasedThreadPoolServer.Args(tServerSocket);
    args.processor(new SessionDriverService.Processor(iface));
    args.transportFactory(new TTransportFactory());
    args.protocolFactory(new TBinaryProtocol.Factory(true, true));
    args.stopTimeoutVal = 0;
    server = new TFactoryBasedThreadPoolServer(
      args, new TFactoryBasedThreadPoolServer.DaemonThreadFactory());
    return sessionServerSocket;
  }

  public IOException getFailed() {
    return failException;
  }

  public void setFailed(IOException e) {
    failException = e;
  }

  public String getSessionId() { return sessionId; }

  public String getPoolName() { return poolName; }

  public String getSessionLog() { return sessionLog; }

  public SessionInfo getSessionInfo() { return sessionInfo; }

  /**
   * Set the name for this session in the ClusterManager
   * @param name the name of this session
   * @throws IOException
   */
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

  /**
   * Set the priority for this session in the ClusterManager
   * @param prio the priority of this session
   * @throws IOException
   */
  public void setPriority(SessionPriority prio) throws IOException {
    if (failException != null) {
      throw failException;
    }

    sessionInfo.priority = prio;

    SessionInfo newInfo = new SessionInfo(sessionInfo);
    cmNotifier.addCall(
        new ClusterManagerService.sessionUpdateInfo_args(sessionId, newInfo));
  }

  /**
   * Set the deadline for this session in the ClusterManager
   * @param sessionDeadline the deadline for the session
   * @throws IOException
   */
  public void setDeadline(long sessionDeadline) throws IOException {
    if (failException != null) {
      throw failException;
    }

    sessionInfo.deadline = sessionDeadline;

    SessionInfo newInfo = new SessionInfo(sessionInfo);
    cmNotifier.addCall(
        new ClusterManagerService.sessionUpdateInfo_args(sessionId, newInfo));
  }

  /**
   * Set the URL for this session in the ClusterManager
   * @param url the url of this session
   * @throws IOException
   */
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

  /**
   * Stop the session with setting the status and providing no usage report
   * @param status the terminating status of the session
   */
  public void stop(SessionStatus status) {
    stop(status, null, null);
  }

  /**
   * Stop the SessionDriver.
   * This sends the message to the ClusterManager indicating that the session
   * has ended.
   * If reportList is not null or empty it will send the report prior to
   * closing the session.
   *
   * @param status the terminating status of the session
   * @param resourceTypes the types of the resources to send the report for
   * @param reportList the report of node usage for this session
   */
  public void stop(SessionStatus status,
                   List<ResourceType> resourceTypes,
                   List<NodeUsageReport> reportList) {
    LOG.info("Stopping session driver");

    // clear all calls from the notifier and append the feedback and session
    // end.
    cmNotifier.clearCalls();
    if (reportList != null && !reportList.isEmpty()) {
      cmNotifier.addCall(
        new ClusterManagerService.nodeFeedback_args(
          sessionId, resourceTypes, reportList));
    }
    cmNotifier.addCall(
        new ClusterManagerService.sessionEnd_args(sessionId, status));
    cmNotifier.doShutdown();
    server.stop();
  }

  /**
   * Join the underlying threads of SessionDriver
   * @throws InterruptedException
   */
  public void join() throws InterruptedException {
    serverThread.join();
    cmNotifier.join();
  }

  /**
   * Request needed resources from the ClusterManager
   * @param wanted the list of resources requested
   * @throws IOException
   */
  public void requestResources(List<ResourceRequest> wanted)
    throws IOException {
    if (failException != null) {
      throw failException;
    }

    cmNotifier.addCall(
        new ClusterManagerService.requestResource_args(sessionId, wanted));
  }

  /**
   * Release the resources that are no longer used
   * @param released resources to be released
   * @throws IOException
   */
  public void releaseResources(List<ResourceRequest> released)
    throws IOException {
    if (failException != null) {
      throw failException;
    }

    List<Integer> releasedIds = new ArrayList<Integer>();
    for (ResourceRequest req : released) {
      releasedIds.add(req.getId());
    }
    cmNotifier.addCall(
        new ClusterManagerService.releaseResource_args(sessionId, releasedIds));
  }

  /**
   * This thread is responsible for dispatching calls to the ClusterManager
   * The session driver is adding the calls to the list of pending calls and
   * the {@link CMNotifierThread} is responsible for sending those to the
   * ClusterManager
   */
  public static class CMNotifierThread extends Thread {

    /** The queue for the calls to be sent to the CM */
    private final List<TBase> pendingCalls = Collections
        .synchronizedList(new LinkedList<TBase>());
    /** starting retry interval */
    private final int retryIntervalStart;
    /** multiplier between successive retry intervals */
    private final int retryIntervalFactor;
    /** max number of retries */
    private final int retryCountMax;
    /** period between polling for dispatches */
    private final int waitInterval;
    /** intervals between heartbeats */
    private final int heartbeatInterval;
    /** The host CM is listening on */
    private final String host;
    /** The port CM is listening on */
    private final int port;
    /** The SessionInfo of this driver's session */
    private final SessionInfo sinfo;
    /** The registration data for the session of this driver*/
    private final SessionRegistrationData sreg;
    /** The underlying SessionDriver for this notifier thread */
    private final SessionDriver sessionDriver;
    /**
     * Time (in milliseconds) when to make the next RPC call -1 means to make
     * the call immediately
     */
    private long nextDispatchTime = -1;
    /** Number of retries that have been made for the first call in the list */
    private short numRetries = 0;
    /** current retry interval */
    private int currentRetryInterval;
    /** last time heartbeat was sent */
    private long lastHeartbeatTime = 0;
    /** Underlying transport for the thrift client */
    private TTransport transport = null;
    /** The ClusterManager thrift client */
    private ClusterManagerService.Client client;
    /** Gets set when the SessionDriver is shutting down */
    private volatile boolean shutdown = false;

    /**
     * Construct a CMNotifier given a Configuration, SessionInfo and for a
     * given SessionDriver
     * @param conf the configuration
     * @param sinfo SessionInfo
     * @param sdriver SessionDriver
     * @throws IOException
     */
    public CMNotifierThread(CoronaConf conf, SessionInfo sinfo,
        SessionDriver sdriver)
      throws IOException {
      waitInterval = conf.getNotifierPollInterval();
      retryIntervalFactor = conf.getNotifierRetryIntervalFactor();
      retryCountMax = conf.getNotifierRetryMax();
      retryIntervalStart = conf.getNotifierRetryIntervalStart();
      heartbeatInterval = Math.max(conf.getSessionExpiryInterval() / 8, 1);
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

    /**
     * Shutdown the SessionDriver and all of the communication
     */
    public void doShutdown() {
      shutdown = true;
      wakeupThread();
    }

    public SessionRegistrationData getSessionRegistrationData() {
      return sreg;
    }

    /**
     * Wake up the notifier thread to process pending calls
     */
    private synchronized void wakeupThread() {
      this.notify();
    }

    /**
     * Add a call to be sent to the CM
     * @param call the call to be sent
     */
    public void addCall(TBase call) {
      pendingCalls.add(call);
      wakeupThread();
    }

    /**
     * Clear the list of pending calls
     */
    public void clearCalls() {
      pendingCalls.clear();
    }

    /**
     * Initialize the CM client
     * @throws TException
     */
    private void init() throws TException {
      if (transport == null) {
        transport = new TSocket(host, port);
        client = new ClusterManagerService.Client(
            new TBinaryProtocol(transport));
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

    @Override
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
            if (call == null) {
              continue;
            }

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
                    " failed with exception", e);
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


    private void dispatchCall(TBase call)
      throws TException, InvalidSessionHandle {
      if (LOG.isDebugEnabled())
        LOG.debug ("Begin dispatching call: " + call.toString());

      if (call instanceof ClusterManagerService.requestResource_args) {
        ClusterManagerService.requestResource_args args =
          (ClusterManagerService.requestResource_args)call;

        client.requestResource(args.handle, args.requestList);
      } else if (call instanceof ClusterManagerService.releaseResource_args) {
        ClusterManagerService.releaseResource_args args =
          (ClusterManagerService.releaseResource_args)call;

        client.releaseResource(args.handle, args.idList);
      } else if (call instanceof ClusterManagerService.sessionEnd_args) {
        ClusterManagerService.sessionEnd_args args =
          (ClusterManagerService.sessionEnd_args)call;

        client.sessionEnd(args.handle, args.status);
      } else if (call instanceof ClusterManagerService.sessionUpdateInfo_args) {
        ClusterManagerService.sessionUpdateInfo_args args =
          (ClusterManagerService.sessionUpdateInfo_args)call;

        client.sessionUpdateInfo(args.handle, args.info);
      } else if (call instanceof ClusterManagerService.nodeFeedback_args) {
        ClusterManagerService.nodeFeedback_args args =
          (ClusterManagerService.nodeFeedback_args)call;

        client.nodeFeedback(args.handle, args.resourceTypes, args.stats);
      } else {
        throw new RuntimeException("Unknown Class: " +
            call.getClass().getName());
      }

      LOG.debug ("End dispatch call");

    }
  }
}
