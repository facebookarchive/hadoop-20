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
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

import javax.security.auth.login.LoginException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.UnixUserGroupInformation;
import org.apache.hadoop.util.Daemon;
import org.apache.thrift.ProcessFunction;
import org.apache.thrift.TBase;
import org.apache.thrift.TBaseProcessor;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.server.TServer;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportFactory;

import org.apache.hadoop.corona.SessionDriverService.Iface;
import org.apache.hadoop.corona.SessionDriverService.grantResource_args;
import org.apache.hadoop.corona.SessionDriverService.grantResource_result;
import org.apache.hadoop.corona.SessionDriverService.revokeResource_args;
import org.apache.hadoop.corona.SessionDriverService.revokeResource_result;
import org.apache.hadoop.corona.SessionDriverService.processDeadNode_args;
import org.apache.hadoop.corona.SessionDriverService.processDeadNode_result;
import org.apache.hadoop.mapred.CoronaJobTracker;
import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapred.JobInProgress;
import org.apache.hadoop.mapred.Task.Counter;


/**
 * Handles sessions from client to cluster manager.
 */
public class SessionDriver {

  /** Logger */
  public static final Log LOG = LogFactory.getLog(SessionDriver.class);
  
  /**
   * Maximum size of the incoming queue. Thrift calls from the
   * Cluster Manager will block if the queue reaches this size.
   */
  public static final String INCOMING_QUEUE_SIZE =
    "corona.sessiondriver.max.incoming.queue.size";

  /** The max time session driver will wait for threads to die.
   *  In certain cases, corona job client will stuck while session
   *  driver is waiting for CMNotifier to die. 
   */
  private static final long  SESSION_DRIVER_WAIT_INTERVAL = 600000L;

  private volatile boolean running = true;
  /** The configuration of the session */
  private final CoronaConf conf;
  /** The processor for the callback server */
  private final SessionDriverService.Iface iface;
  /** The calls coming in from the Cluster Manager */
  private final LinkedBlockingQueue<TBase> incoming;
  /** Call Processor thread. */
  private Thread incomingCallExecutor;

  /** The id of the underlying session */
  private String sessionId = "";
  /** The pool info that the cluster manager assigns this session to. */
  private PoolInfo poolInfo;
  /** The location of the log for the underlying session */
  private String sessionLog = "";
  /** The info of the underlying session */
  private SessionInfo sessionInfo;
  /** heartBeat info including the last resource requestId and grantId */
  private HeartbeatArgs heartbeatInfo;

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
    incoming = new LinkedBlockingQueue<TBase>(
      conf.getInt(INCOMING_QUEUE_SIZE, 1000));

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
    if (null != conf.get("hive.query.source")) {
      sessionName += " [" + conf.get("hive.query.source")+ "]";
    }
    this.sessionInfo = new SessionInfo();
    this.sessionInfo.setAddress(myAddress);
    this.sessionInfo.setName(sessionName);
    this.sessionInfo.setUserId(userName);
    this.sessionInfo.setPoolInfoStrings(
        PoolInfo.createPoolInfoStrings(conf.getPoolInfo()));
    this.sessionInfo.setPriority(SessionPriority.NORMAL);
    this.sessionInfo.setNoPreempt(false);
    this.heartbeatInfo = new HeartbeatArgs();
    this.heartbeatInfo.requestId = 0;
    this.heartbeatInfo.grantId = 0;

    this.serverThread = new Daemon(new Thread() {
        @Override
        public void run() {
          server.serve();
        }
      });
    this.serverThread.start();

    incomingCallExecutor = new Daemon(new IncomingCallExecutor());
    incomingCallExecutor.setName("Incoming Call Executor");
    incomingCallExecutor.start();

    cmNotifier = new CMNotifierThread(conf, this);
    sessionId = cmNotifier.getSessionId();
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
    args.processor(new SessionDriverServiceProcessor(incoming));
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

  public PoolInfo getPoolInfo() { return poolInfo; }

  public String getSessionLog() { return sessionLog; }
  
  public void setResourceRequest(List<ResourceRequest> requestList) {
    int maxid = 0;
    for (ResourceRequest request: requestList) {
      if (maxid < request.id) {
        maxid = request.id;
      }
    }    
    heartbeatInfo.requestId = maxid;
  }
  
  public void setResourceGrant(List<ResourceGrant> grantList) {
    int maxid = 0;
    for (ResourceGrant grant: grantList) {
      if (maxid < grant.id) {
        maxid = grant.id;
      }
    }
    heartbeatInfo.grantId = maxid;
  }

  public void startSession() throws IOException {
    try {
      cmNotifier.startSession(sessionInfo);
    } catch (InvalidSessionHandle e) {
      throw new IOException(e);
    } catch (TException e) {
      throw new IOException(e);
    }
    cmNotifier.setDaemon(true);
    cmNotifier.start();
    sessionLog = cmNotifier.getSessionRegistrationData()
      .getClusterManagerInfo().getJobHistoryLocation();
    PoolInfoStrings poolInfoStrings =
      cmNotifier.getSessionRegistrationData().getPoolInfoStrings();
    poolInfo = PoolInfo.createPoolInfo(poolInfoStrings);
    sessionInfo.setPoolInfoStrings(poolInfoStrings);
    LOG.info("Session " + sessionId + " job history location " + sessionLog +
      " poolInfo " + poolInfo);
  }

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
   * Stops session acquired by remote JT
   * @param remoteId id of remote JT session
   */
  public void stopRemoteSession(String remoteId) {
    cmNotifier.addCall(new ClusterManagerService.sessionEnd_args(remoteId,
        SessionStatus.TIMED_OUT));
  }

  /**
   * For test purposes. Abort a sessiondriver without sending a sessionEnd()
   * call to the CM. This allows testing for abnormal session termination
   */
  public void abort() {
    LOG.info("Aborting session driver");
    running = false;
    cmNotifier.clearCalls();
    cmNotifier.doShutdown();
    server.stop();
    incomingCallExecutor.interrupt();
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
    running = false;

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

    incomingCallExecutor.interrupt();
  }

  /**
   * Join the underlying threads of SessionDriver
   * @throws InterruptedException
   */
  public void join() throws InterruptedException {
    serverThread.join();
    long start = System.currentTimeMillis();
    cmNotifier.join(SESSION_DRIVER_WAIT_INTERVAL);
    long end = System.currentTimeMillis();
    if (end - start >= SESSION_DRIVER_WAIT_INTERVAL) {
      LOG.warn("Taking more than " + SESSION_DRIVER_WAIT_INTERVAL +
        " for cmNotifier to die");
    }
    incomingCallExecutor.join();
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
  
  public Map<ResourceType, List<Long>> getResourceUsageMap()
  {
    return ((CoronaJobTracker)iface).getResourceUsageMap();
  }
  
  public void incCMClientRetryCounter () {
    if (iface instanceof CoronaJobTracker) {
      Counters jobCounters = ((CoronaJobTracker)iface).getJobCounters();
      if (jobCounters != null) {
        LOG.info("inc retry session counter");
        jobCounters.incrCounter(JobInProgress.Counter.NUM_SESSION_DRIVER_CM_CLIENT_RETRY, 1);
      }
    }
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
    /** The session ID for the session of this driver. */
    private final String sessionId;
    /** The underlying SessionDriver for this notifier thread */
    private final SessionDriver sessionDriver;
    /** The registration data for the session of this driver*/
    private volatile SessionRegistrationData sreg;
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
    /** Required for doing RPC to the ProxyJobTracker */
    private CoronaConf coronaConf;
    /** If sessionHeartbeatV2 is sipported by CM */
    private boolean useHeartbeatV2 = true;
    
    private int readTimeout;
    /**
     * the timeout value used by session driver cm client
     */
    public static final String CM_CLIENT_TIMEOUT = 
        "corona.sessiondriver.cm.client.timeout";
    private static final int SESSION_DRIVER_CM_CLIENT_TIMEOUT = 300000;

    /**
     * Construct a CMNotifier given a Configuration, SessionInfo and for a
     * given SessionDriver
     * @param conf the configuration
     * @param sdriver SessionDriver
     * @throws IOException
     */
    public CMNotifierThread(CoronaConf conf, SessionDriver sdriver)
      throws IOException {
      waitInterval = conf.getNotifierPollInterval();
      retryIntervalFactor = conf.getNotifierRetryIntervalFactor();
      retryCountMax = conf.getNotifierRetryMax();
      retryIntervalStart = conf.getNotifierRetryIntervalStart();
      heartbeatInterval = Math.max(conf.getSessionExpiryInterval() / 8, 1);
      sessionDriver = sdriver;
      readTimeout = conf.getInt(CM_CLIENT_TIMEOUT, SESSION_DRIVER_CM_CLIENT_TIMEOUT);

      String target = conf.getClusterManagerAddress();
      InetSocketAddress address = NetUtils.createSocketAddr(target);
      host = address.getHostName();
      port = address.getPort();
      coronaConf = conf;

      String tempSessionId;
      int numCMConnectRetries = 0;
      while (true) {
        try {
          transport = null;
          LOG.info("Connecting to cluster manager at " + host + ":" + port);
          init();
          tempSessionId = client.getNextSessionId();
          LOG.info("Got session ID " + tempSessionId);
          close();
          break;
        } catch (SafeModeException f) {
          LOG.info("Received a SafeModeException");
          // We do not need to connect to the CM till the Safe Mode flag is
          // set on the PJT.
          ClusterManagerAvailabilityChecker.
            waitWhileClusterManagerInSafeMode(conf);
        } catch (TException e) {
          if (numCMConnectRetries > retryCountMax) {
            throw new IOException(
              "Could not connect to Cluster Manager tried " +
                numCMConnectRetries +
              " times");
          }
          // It is possible that the ClusterManager is down after setting
          // the Safe Mode flag. We should wait until the flag is unset.
          ClusterManagerAvailabilityChecker.
            waitWhileClusterManagerInSafeMode(coronaConf);
          ++numCMConnectRetries;
        }

      }
      sessionId = tempSessionId;
    }

    public void startSession(SessionInfo info)
      throws TException, InvalidSessionHandle, IOException {
      while(true) {
        init();
        try {
          sreg = client.sessionStart(sessionId, info);
          break;
        } catch (SafeModeException e) {
          // Since we have received a SafeModeException, it is likely that
          // the ClusterManager will now go down. So our current thrift
          // client will no longer be useful. Hence, we need to close the
          // current thrift client and create a new one, which will be created
          // in the next iteration
          close();
          ClusterManagerAvailabilityChecker.
            waitWhileClusterManagerInSafeMode(coronaConf);
        }
      }

      LOG.info("Started session " + sessionId);
      close();
    }

    /**
     * Shutdown the SessionDriver and all of the communication
     */
    public void doShutdown() {
      shutdown = true;
      wakeupThread();
    }

    public String getSessionId() {
      return sessionId;
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
        transport = new TFramedTransport(new TSocket(host, port, readTimeout));
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
            LOG.debug("Sending heartbeat for " + sreg.handle + " with (" +
              sessionDriver.heartbeatInfo.requestId + " " +sessionDriver.heartbeatInfo.grantId +")");
            if (useHeartbeatV2) {
              try {      
                sessionDriver.heartbeatInfo.resourceUsages = sessionDriver.getResourceUsageMap();
                
                client.sessionHeartbeatV2(sreg.handle, sessionDriver.heartbeatInfo);
              } catch (org.apache.thrift.TApplicationException e) {
                LOG.info("heartbeatV2 is not suported by CM for session " + sreg.handle);
                useHeartbeatV2 = false;
                client.sessionHeartbeat(sreg.handle);
              }
            }
            else {
              client.sessionHeartbeat(sreg.handle);
            }
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
        } catch (InvalidSessionHandle e) {
          LOG.error("InvalidSession exception - closing CMNotifier", e);
          sessionDriver.setFailed(new IOException(e));
          break;
        } catch (SafeModeException e) {
          LOG.info("Cluster Manager is in Safe Mode");
          try {
            // Since we have received a SafeModeException, it is likely that
            // the ClusterManager will now go down. So our current thrift
            // client will no longer be useful. Hence, we need to close the
            // current thrift client and create a new one, which will be created
            // in the next iteration
            close();
            ClusterManagerAvailabilityChecker.
              waitWhileClusterManagerInSafeMode(coronaConf);
          } catch (IOException ie) {
            LOG.error(ie.getMessage());
          }

        } catch (TException e) {

          LOG.error("Call to CM, numRetry: " + numRetries +
            " failed with exception", e);
          // close the transport/client on any exception
          // will be reopened on next try
          close();
          sessionDriver.incCMClientRetryCounter();
          /**
           * If we don't know if ClusterManager was going for an upgrade,
           * Check with the ProxyJobTracker if the ClusterManager went down
           * after telling it.
           */
          try {
            ClusterManagerAvailabilityChecker.
              waitWhileClusterManagerInSafeMode(coronaConf);
          } catch (IOException ie) {
            LOG.warn("Could not check the Safe Mode flag on PJT");
          }

          if (numRetries > retryCountMax) {
            LOG.error("All retries failed - closing CMNotifier");
            sessionDriver.setFailed(new IOException(e));
            break;
          }

          numRetries++;
          nextDispatchTime = now + currentRetryInterval;
          currentRetryInterval *= retryIntervalFactor;

        }
      } // while (true)
      close();
    } // run()


    private void dispatchCall(TBase call)
      throws TException, InvalidSessionHandle, SafeModeException {
      if (LOG.isDebugEnabled())
        LOG.debug ("Begin dispatching call: " + call.toString());

      if (call instanceof ClusterManagerService.requestResource_args) {
        ClusterManagerService.requestResource_args args =
          (ClusterManagerService.requestResource_args)call;

        client.requestResource(args.handle, args.requestList);
        sessionDriver.setResourceRequest(args.requestList);
      } else if (call instanceof ClusterManagerService.releaseResource_args) {
        ClusterManagerService.releaseResource_args args =
          (ClusterManagerService.releaseResource_args)call;

        client.releaseResource(args.handle, args.idList);
      } else if (call instanceof ClusterManagerService.sessionEnd_args) {
        ClusterManagerService.sessionEnd_args args =
          (ClusterManagerService.sessionEnd_args)call;
        // For job tracker fail over, we will close the remote session
        // but sometimes we will meet the InvalidSessionHandle exception,
        // like remote job tracker has failed in job_end1 or job_end2, so just 
        // catching this exception
        try {
          client.sessionEnd(args.handle, args.status);
        } catch (InvalidSessionHandle e) {
          LOG.warn("sessionEnd get InvalidSessionHandle exception", e);
        }
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

  /**
   * Executes the calls received from the Cluster Manager in the same order
   * as received.
   */
  private class IncomingCallExecutor extends Thread {
    @Override
    public void run() {
      while (running) {
        try {
          TBase call = incoming.take();
          if (call instanceof grantResource_args) {
            grantResource_args args = (grantResource_args) call;
            iface.grantResource(args.handle, args.granted);
            setResourceGrant(args.granted);
          } else if (call instanceof revokeResource_args) {
            revokeResource_args args = (revokeResource_args) call;
            iface.revokeResource(args.handle, args.revoked, args.force);
          } else if (call instanceof processDeadNode_args) {
            processDeadNode_args args = (processDeadNode_args) call;
            iface.processDeadNode(args.handle, args.node);
          } else {
            throw new TException("Unhandled call " + call);
          }
        } catch (InterruptedException e) {
          // Check the running flag.
          continue;
        } catch (TException e) {
          throw new RuntimeException(
            "Unexpected error while processing calls ", e);
        }
      }
    }
  }

  /**
   * A Thrift call processor that simply puts the calls in a queue. This will
   * ensure minimum latency in executing calls from the Cluster Manager.
   */
  public static class SessionDriverServiceProcessor extends TBaseProcessor {
    /**
     * Constructor
     * @param calls The call queue.
     */
    public SessionDriverServiceProcessor(LinkedBlockingQueue<TBase> calls) {
      super(null, getProcessMap(calls));
    }

    /**
     * Constructs the map from function name -> handler.
     * @param calls The call queue.
     * @return The map.
     */
    private static Map<String, ProcessFunction> getProcessMap(
      LinkedBlockingQueue<TBase> calls) {
      Map<String, ProcessFunction> processMap =
        new HashMap<String, ProcessFunction>();
      processMap.put("grantResource", new grantResourceHandler(calls));
      processMap.put("revokeResource", new revokeResourceHandler(calls));
      processMap.put("processDeadNode", new processDeadNodeHandler(calls));
      return processMap;
    }

    /**
     * Handles "grantResource" calls.
     */
    private static class grantResourceHandler
      extends ProcessFunction<Iface, grantResource_args> {
      /** The call queue. */
      private final LinkedBlockingQueue<TBase> calls;

      /**
       * Constructor.
       * @param calls the call queue.
       */
      public grantResourceHandler(LinkedBlockingQueue<TBase> calls) {
        super("grantResource");
        this.calls = calls;
      }

      /**
       * @return empty args.
       */
      public grantResource_args getEmptyArgsInstance() {
        return new grantResource_args();
      }

      protected boolean isOneway() {
        return false;
      }

      /**
       * Call implementation. Just queue up the args.
       * @param unused The unused interface ref.
       * @param args The args
       * @return Empty result
       */
      public grantResource_result getResult(
        Iface unused, grantResource_args args) throws TException {
        try {
          calls.put(args);
        } catch (InterruptedException e) {
          throw new TException(e);
        }
        return new grantResource_result();
      }
    }

    /**
     * Handles "revokeResource" calls.
     */
    private static class revokeResourceHandler
      extends ProcessFunction<Iface, revokeResource_args> {
      /** The call queue. */
      private final LinkedBlockingQueue<TBase> calls;

      /**
       * Constructor.
       * @param calls the call queue.
       */
      public revokeResourceHandler(LinkedBlockingQueue<TBase> calls) {
        super("revokeResource");
        this.calls = calls;
      }

      /**
       * @return empty args.
       */
      public revokeResource_args getEmptyArgsInstance() {
        return new revokeResource_args();
      }

      protected boolean isOneway() {
        return false;
      }

      /**
       * Call implementation. Just queue up the args.
       * @param unused The unused interface ref.
       * @param args The args
       * @return Empty result
       */
      public revokeResource_result getResult(
        Iface unused, revokeResource_args args) throws TException {
        try {
          calls.put(args);
        } catch (InterruptedException e) {
          throw new TException(e);
        }
        return new revokeResource_result();
      }
    }

    private static class processDeadNodeHandler
      extends ProcessFunction<Iface, processDeadNode_args> {
      /** The call queue. */
      private final LinkedBlockingQueue<TBase> calls;

      /**
       * Constructor.
       * @param calls the call queue.
       */
      public processDeadNodeHandler(LinkedBlockingQueue<TBase> calls) {
        super("processDeadNode");
        this.calls = calls;
      }

      /**
       * @return empty args.
       */
      public processDeadNode_args getEmptyArgsInstance() {
        return new processDeadNode_args();
      }

      protected boolean isOneway() {
        return false;
      }

      /**
       * Call implementation. Just queue up the args.
       * @param unused The unused interface ref.
       * @param args The args
       * @return Empty result
       */
      public processDeadNode_result getResult(
        Iface unused, processDeadNode_args args) throws TException {
        try {
          calls.put(args);
        } catch (InterruptedException e) {
          throw new TException(e);
        }
        return new processDeadNode_result();
      }
    }
  }
}
