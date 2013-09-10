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

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.*;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.http.HttpServer;
import org.apache.hadoop.mapred.Clock;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.util.CoronaSerializer;
import org.apache.hadoop.util.HostsFileReader;
import org.apache.thrift.TApplicationException;
import org.apache.thrift.TException;
import org.codehaus.jackson.JsonGenerator;

/**
 * Manager of all the resources of the cluster.
 */
public class ClusterManager implements ClusterManagerService.Iface {
  /** Class logger */
  public static final Log LOG = LogFactory.getLog(ClusterManager.class);
  /** Clock that is used for any general purpose system times. */
  public static Clock clock = new Clock();
  /** 
   * The threshold to control if generating a log to say 
   * somebody try to delete a number of active sessions
   */
  public static final int KILL_SESSIONS_THRESHOLD = 2;

  /** Node manager manages collections of nodes */
  protected NodeManager nodeManager;

  /** Session manager manages collections of sessions */
  protected SessionManager sessionManager;

  /** Sessions history manager */
  protected SessionHistoryManager sessionHistoryManager;

  /** http server */
  protected HttpServer infoServer;

  /** Scheduler service matches free nodes to runnable sessions */
  protected Scheduler scheduler;

  /**
   * The session notifier asynchronously notifies sessions about
   * various events
   */
  protected SessionNotifier sessionNotifier;

  /** Metrics for the cluster manager */
  protected ClusterManagerMetrics metrics;
  /** Configuration */
  protected CoronaConf conf;

  /** Start time to show in UI. */
  protected long startTime;
  /** When was the CM last restarted (either safely or otherwise) */
  protected long lastRestartTime;
  /** Host name to show in UI. */
  protected String hostName;

  /** Legal values for the "type" of a resource request. */
  protected Set<ResourceType> legalTypeSet =
      EnumSet.noneOf(ResourceType.class);

  /** Is the Cluster Manager in Safe Mode */
  protected volatile boolean safeMode;

  /** the thread to restart all the task trackers */
  protected CoronaNodeRestarter nodeRestarter;

  /**
   * Simple constructor for testing help.
   */
  public ClusterManager() { }

  /**
   * Primary constructor.
   *
   * @param conf Configuration to be used
   * @param recoverFromDisk True if we are restarting after going down while
   *                            in Safe Mode
   * @throws IOException
   */
  public ClusterManager(Configuration conf, boolean recoverFromDisk)
    throws IOException {
    this(new CoronaConf(conf), recoverFromDisk);
  }

  /**
   * Constructor for ClusterManager, when it is not specified if we are
   * restarting after persisting the state. In this case we assume the
   * recoverFromDisk flag to be false.
   *
   * @param conf Configuration to be used
   * @throws IOException
   */
  public ClusterManager(Configuration conf) throws IOException {
    this(new CoronaConf(conf), false);
  }

  /**
   * Construct ClusterManager given {@link CoronaConf}
   *
   * @param conf the configuration for the ClusterManager
   * @param recoverFromDisk true if we are restarting after going down while
   *                        in Safe Mode
   * @throws IOException
   */
  public ClusterManager(CoronaConf conf, boolean recoverFromDisk)
    throws IOException {
    this.conf = conf;
    HostsFileReader hostsReader =
      new HostsFileReader(conf.getHostsFile(), conf.getExcludesFile());
    initLegalTypes();
    metrics = new ClusterManagerMetrics(getTypes());

    if (recoverFromDisk) {
      recoverClusterManagerFromDisk(hostsReader);
    } else {
      File stateFile = new File(conf.getCMStateFile());
      if (stateFile.exists()) {
        throw new IOException("State file " + stateFile.getAbsolutePath() +
          " exists, but recoverFromDisk is not set, delete the state file first");
      }
      LOG.info("Starting Cluster Manager with clean state");
      startTime = clock.getTime();
      lastRestartTime = startTime;
      nodeManager = new NodeManager(this, hostsReader);
      nodeManager.setConf(conf);
      sessionManager = new SessionManager(this);
      sessionNotifier = new SessionNotifier(sessionManager, this, metrics);
    }
    sessionManager.setConf(conf);
    sessionNotifier.setConf(conf);

    sessionHistoryManager = new SessionHistoryManager();
    sessionHistoryManager.setConf(conf);

    scheduler = new Scheduler(nodeManager, sessionManager,
        sessionNotifier, getTypes(), metrics, conf);
    scheduler.start();
    metrics.registerUpdater(scheduler, sessionNotifier);

    InetSocketAddress infoSocAddr =
        NetUtils.createSocketAddr(conf.getClusterManagerHttpAddress());
    infoServer =
        new HttpServer("cm", infoSocAddr.getHostName(), infoSocAddr.getPort(),
                       infoSocAddr.getPort() == 0, conf);
    infoServer.setAttribute("cm", this);
    infoServer.start();

    hostName = infoSocAddr.getHostName();

    // We have not completely restored the nodeManager, sessionManager and the
    // sessionNotifier
    if (recoverFromDisk) {
      nodeManager.restoreAfterSafeModeRestart();
      sessionManager.restoreAfterSafeModeRestart();
      sessionNotifier.restoreAfterSafeModeRestart();
    }

    nodeRestarter = new CoronaNodeRestarter(conf, nodeManager);
    nodeRestarter.start();

    setSafeMode(false);
}

  /**
   * This method is used when the ClusterManager is restarting after going down
   * while in Safe Mode. It starts the process of recovering the original
   * CM state by reading back the state in JSON form.
   * @param hostsReader The HostsReader instance
   * @throws IOException
   */
  private void recoverClusterManagerFromDisk(HostsFileReader hostsReader)
    throws IOException {
    LOG.info("Restoring state from " +
      new java.io.File(conf.getCMStateFile()).getAbsolutePath());

    // This will prevent the expireNodes and expireSessions threads from
    // expiring the nodes and sessions respectively
    safeMode = true;
    LOG.info("Safe mode is now: " + (this.safeMode ? "ON" : "OFF"));

    CoronaSerializer coronaSerializer = new CoronaSerializer(conf);

    // Expecting the START_OBJECT token for ClusterManager
    coronaSerializer.readStartObjectToken("ClusterManager");

    coronaSerializer.readField("startTime");
    startTime = coronaSerializer.readValueAs(Long.class);

    coronaSerializer.readField("nodeManager");
    nodeManager = new NodeManager(this, hostsReader, coronaSerializer);
    nodeManager.setConf(conf);

    coronaSerializer.readField("sessionManager");
    sessionManager = new SessionManager(this, coronaSerializer);

    coronaSerializer.readField("sessionNotifier");
    sessionNotifier = new SessionNotifier(sessionManager, this, metrics,
                                          coronaSerializer);

    // Expecting the END_OBJECT token for ClusterManager
    coronaSerializer.readEndObjectToken("ClusterManager");

    lastRestartTime = clock.getTime();
  }

  /**
   * Prepare the legal types allowed based on the resources available
   */
  protected void initLegalTypes() {
    Map<Integer, Map<ResourceType, Integer>> cpuToResourcePartitioning =
      conf.getCpuToResourcePartitioning();

    for (Map.Entry<Integer, Map<ResourceType, Integer>> entry :
          cpuToResourcePartitioning.entrySet()) {
      for (ResourceType type : entry.getValue().keySet()) {
        legalTypeSet.add(type);
      }
    }
    legalTypeSet = Collections.unmodifiableSet(legalTypeSet);
  }

  public ClusterManagerMetrics getMetrics() {
    return metrics;
  }

  public SessionNotifier getSessionNotifier() {
    return sessionNotifier;
  }

  public SessionManager getSessionManager() {
    return sessionManager;
  }

  public NodeManager getNodeManager() {
    return nodeManager;
  }

  public Scheduler getScheduler() {
    return scheduler;
  }

  public Collection<ResourceType> getTypes() {
    return Collections.unmodifiableCollection(legalTypeSet);
  }

  /**
   * This is a helper method which simply checks if the safe mode flag is
   * turned on. If it is, the method which was called, cannot be executed
   * and, a SafeModeException is thrown.
   * @param methodName
   * @throws SafeModeException
   */
  private void checkSafeMode(String methodName) throws SafeModeException {
    if (safeMode) {
      LOG.info(methodName + "() called while ClusterManager is in Safe Mode");
      throw new SafeModeException();
    }
  }

  @Override
  public PoolInfoStrings getActualPoolInfo(ActualPoolInfoArgs actualPoolInfoArgs)
    throws TException, InvalidPoolInfo, SafeModeException {
    checkSafeMode("getActualPoolInfo");
    PoolInfoStrings userSpecifiedPoolInfo = actualPoolInfoArgs.poolInfoString;
    long jobSizeInfo = actualPoolInfoArgs.jobInputSize;
    ConfigManager configManager = getScheduler().getConfigManager();
    PoolInfo poolInfo = PoolInfo.createPoolInfo(userSpecifiedPoolInfo);
    // Get Redirect pool info
    PoolInfo redirectedPoolInfo = configManager.getRedirect(poolInfo, jobSizeInfo);
    // Validate redirected  pool information
    try {
      PoolGroupManager.checkPoolInfoIfStrict(redirectedPoolInfo, configManager, conf);
    } catch (InvalidSessionHandle ex) {
      throw new InvalidPoolInfo(ex.getHandle());
    }
    PoolInfoStrings actualPoolInfo = PoolInfo.createPoolInfoStrings(redirectedPoolInfo);
    return actualPoolInfo;
  }
  
  @Override
  public ActualPoolInfoResponse getActualPoolInfoV2(ActualPoolInfoArgs actualPoolInfoArgs)
      throws InvalidPoolInfo, SafeModeException, TException {
    checkSafeMode("getActualPoolInfoV2");
    PoolInfoStrings userSpecifiedPoolInfo = actualPoolInfoArgs.poolInfoString;
    long jobSizeInfo = actualPoolInfoArgs.jobInputSize;
    ConfigManager configManager = getScheduler().getConfigManager();
    PoolInfo poolInfo = PoolInfo.createPoolInfo(userSpecifiedPoolInfo);
    // Get Redirect pool info
    PoolInfo redirectedPoolInfo = configManager.getRedirect(poolInfo, jobSizeInfo);
    // Validate redirected  pool information
    try {
      PoolGroupManager.checkPoolInfoIfStrict(redirectedPoolInfo, configManager, conf);
    } catch (InvalidSessionHandle ex) {
      throw new InvalidPoolInfo(ex.getHandle());
    }
 
    PoolInfoStrings actualPoolInfo = PoolInfo.createPoolInfoStrings(redirectedPoolInfo);
    ActualPoolInfoResponse actualPoolInfoResponse = new ActualPoolInfoResponse();
    actualPoolInfoResponse.poolInfoString = actualPoolInfo;
    actualPoolInfoResponse.whitelist = configManager.getWhitelist(redirectedPoolInfo);
    return actualPoolInfoResponse;
  }

  @Override
  public String getNextSessionId() throws SafeModeException {
    checkSafeMode("getNextSessionId");
    return sessionManager.getNextSessionId();
  }

  @Override
  public SessionRegistrationData sessionStart(String handle, SessionInfo info)
    throws TException, InvalidSessionHandle, SafeModeException {
    checkSafeMode("sessionStart");
    String sessionLogPath = sessionHistoryManager.getLogPath(handle);
    Session session = sessionManager.addSession(handle, info);
    return new SessionRegistrationData(
      session.getHandle(), new ClusterManagerInfo("", sessionLogPath),
      PoolInfo.createPoolInfoStrings(session.getPoolInfo()));
  }

  @Override
  public void sessionEnd(String handle, SessionStatus status)
    throws TException, InvalidSessionHandle, SafeModeException {
    checkSafeMode("sessionEnd");
    try {
      Session session = sessionManager.getSession(handle);
      InetAddress sessionAddr = session.getAddress();
      LOG.info("sessionEnd called for session: " + handle +
        " on " + sessionAddr.getHost() + ":" + sessionAddr.getPort() +
        " with status: " + status);
      if (status == SessionStatus.TIMED_OUT) {
        if (session.getUrl() != null &&
            session.getUrl().indexOf(handle) < 0) {
          metrics.timeoutRemoteJT(1);
        }
      }
      if (status == SessionStatus.FAILED_JOBTRACKER) {
        metrics.recordCJTFailure();
      }

      Collection<ResourceGrant> canceledGrants =
          sessionManager.deleteSession(handle, status);

      if (canceledGrants == null) {
        return;
      }

      for (ResourceGrant grant: canceledGrants) {
        nodeManager.cancelGrant(grant.nodeName, handle, grant.id);
        metrics.releaseResource(grant.type);
      }

      scheduler.notifyScheduler();
      sessionNotifier.deleteSession(handle);

    } catch (RuntimeException e) {
      LOG.error("Error in sessionEnd of " + handle, e);
      throw new TApplicationException(e.getMessage());
    }
  }

  @Override
  public void sessionUpdateInfo(String handle, SessionInfo info)
      throws TException, InvalidSessionHandle, SafeModeException {
    checkSafeMode("sessionUpdateInfo");
    try {
      LOG.info("sessionUpdateInfo called for session: " + handle +
               " with info: " + info);
      sessionManager.heartbeat(handle);
      sessionManager.updateInfo(handle, info);
    } catch (RuntimeException e) {
      throw new TApplicationException(e.getMessage());
    }
  }

  @Override
  public void sessionHeartbeat(String handle) throws TException,
      InvalidSessionHandle, SafeModeException {
    checkSafeMode("sessionHeartbeat");
    try {
      sessionManager.heartbeat(handle);
    } catch (RuntimeException e) {
      throw new TApplicationException(e.getMessage());
    }
  }

  @Override
  public void sessionHeartbeatV2(String handle, HeartbeatArgs jtInfo) throws TException,
      InvalidSessionHandle, SafeModeException {
    checkSafeMode("sessionHeartbeatV2");
    try {
      Session session = sessionManager.getSession(handle);
      if (!session.checkHeartbeatInfo(jtInfo)) {
        sessionEnd(session.getSessionId(), SessionStatus.FAILED_JOBTRACKER);
      } 
      sessionManager.heartbeatV2(handle, jtInfo);
    } catch (RuntimeException e) {
      throw new TApplicationException(e.getMessage());
    }
  }

  /**
   * Check all the resource requests and ensure that they are legal.
   *
   * @param requestList List of resource requests to check
   * @return True if the resources are legal, false otherwise
   */
  protected boolean checkResourceRequestType(
      List<ResourceRequest> requestList) {
    for (ResourceRequest req: requestList) {
      if (!legalTypeSet.contains(req.type)) {
        return false;
      }
    }
    return true;
  }

  protected boolean checkResourceRequestExcluded(
      List<ResourceRequest> requestList) {
    Set<String> excluded = new HashSet<String>();
    for(ResourceRequest req: requestList) {
      if (req.getExcludeHosts() == null ||
          req.getHosts() == null) {
        continue;
      }
      excluded.clear();
      excluded.addAll(req.getExcludeHosts());
      for (String host : req.getHosts()) {
        if (excluded.contains(host)) {
          return false;
        }
      }
    }
    return true;
  }

  /**
   * Count the resources requested and fail the job if they are above the limit
   *
   * @param requestList List of resource requests to check
   */
  protected void checkResourceRequestLimit(
      List<ResourceRequest> requestList, String handle)
      throws InvalidSessionHandle {
    ConfigManager configManager = getScheduler().getConfigManager();
    Session session = sessionManager.getSession(handle);
    PoolInfo poolInfo = session.getPoolInfo();

    // Only check the resource requests if this pool is configured to not
    // accept more than a fixed number of requests at the same time
    if (!configManager.useRequestMax(poolInfo)) {
      return;
    }

    // Count the resources by type
    ResourceTypeCounter resourceTypeCounter = new ResourceTypeCounter();
    for (ResourceRequest req : requestList) {
      resourceTypeCounter.incr(req.type);
    }

    // No resource type request should exceed the maximum
    for (ResourceType resourceType : ResourceType.values()) {
      if (configManager.getPoolMaximum(poolInfo, resourceType) <
          resourceTypeCounter.getCount(resourceType)) {
        String failureMessage =
            "Session " + handle + " requested " +
                resourceTypeCounter.getCount(resourceType) +
                " resources for resource type " +
                resourceType + " but was only allowed " +
                configManager.getPoolMaximum(poolInfo, resourceType) + ", " +
                "so failing the job";
        LOG.error(failureMessage);
        throw new InvalidSessionHandle(failureMessage);
      }
    }
  }

  @Override
  public void requestResource(String handle, List<ResourceRequest> requestList)
    throws TException, InvalidSessionHandle, SafeModeException {
    checkSafeMode("requestResource");
    try {
      LOG.info ("Request " + requestList.size() +
          " resources from session: " + handle);
      if (!checkResourceRequestType(requestList)) {
        LOG.error ("Bad resource type from session: " + handle);
        throw new TApplicationException("Bad resource type");
      }
      if (!checkResourceRequestExcluded(requestList)) {
        LOG.error("Bad excluded hosts from session: " + handle);
        throw new TApplicationException("Requesting excluded hosts");
      }
      checkResourceRequestLimit(requestList, handle);

      sessionManager.heartbeat(handle);
      sessionManager.getSession(handle).setResourceRequest(requestList);
      List<ResourceRequestInfo> reqInfoList =
          new ArrayList<ResourceRequestInfo>(requestList.size());
      for (ResourceRequest request : requestList) {
        List<String> hosts = request.getHosts();
        List<RequestedNode> requestedNodes = null;
        if (hosts != null && hosts.size() > 0) {
          requestedNodes = new ArrayList<RequestedNode>(hosts.size());
          for (String host : hosts) {
            requestedNodes.add(nodeManager.resolve(host, request.type));
          }
        }
        ResourceRequestInfo info =
          new ResourceRequestInfo(request, requestedNodes);
        reqInfoList.add(info);
      }
      sessionManager.requestResource(handle, reqInfoList);
      for (ResourceRequest req : requestList) {
        metrics.requestResource(req.type);
      }
      scheduler.notifyScheduler();
    } catch (RuntimeException e) {
      e.printStackTrace();
      throw new TApplicationException(e.getMessage());
    }
  }

  @Override
  public void releaseResource(String handle, List<Integer> idList)
    throws TException, InvalidSessionHandle, SafeModeException {
    checkSafeMode("releaseResource");
    try {
      LOG.info("Release " + idList.size() + " resources from session: " +
               handle);
      sessionManager.heartbeat(handle);
      Collection<ResourceGrant> canceledGrants =
        sessionManager.releaseResource(handle, idList);

      if (canceledGrants == null) {
        // LOG.info("No canceled grants for session " + handle);
        return;
      }

      for (ResourceGrant grant: canceledGrants) {
        nodeManager.cancelGrant(grant.nodeName, handle, grant.id);
        metrics.releaseResource(grant.type);
      }

      scheduler.notifyScheduler();
    } catch (RuntimeException e) {
      throw new TApplicationException(e.getMessage());
    }
  }

  @Override
  public NodeHeartbeatResponse nodeHeartbeat(ClusterNodeInfo node)
    throws TException, DisallowedNode, SafeModeException {
    checkSafeMode("nodeHeartbeat");
    //LOG.info("heartbeat from node: " + node.toString());
    if (nodeManager.heartbeat(node)) {
      scheduler.notifyScheduler();
    }
    NodeHeartbeatResponse nodeHeartbeatResponse = new NodeHeartbeatResponse();
    if (nodeRestarter != null && nodeRestarter.checkStatus(node)) {
      nodeHeartbeatResponse.setRestartFlag(true);
    }
    else {
      nodeHeartbeatResponse.setRestartFlag(false);
    }
    return nodeHeartbeatResponse;
  }

  @Override
  public void nodeFeedback(String handle, List<ResourceType> resourceTypes,
      List<NodeUsageReport> reportList)
    throws TException, InvalidSessionHandle, SafeModeException {
    checkSafeMode("nodeFeedback");
    LOG.info("Received feedback from session " + handle);
    nodeManager.nodeFeedback(handle, resourceTypes, reportList);
  }

  @Override
  public void refreshNodes() throws TException, SafeModeException {
    checkSafeMode("refreshNodes");
    try {
      nodeManager.refreshNodes();
    } catch (IOException e) {
      throw new TException(e);
    }
  }

  @Override
  public RestartNodesResponse restartNodes( RestartNodesArgs restartNodesArgs)
    throws TException, SafeModeException {
    checkSafeMode("restartNode");
    LOG.info("Got request to restart all the cluster nodes with batch size " +
      restartNodesArgs.getBatchSize());
    List<ClusterNode> allNodes = nodeManager.getAliveClusterNodes();
    if (allNodes.size() > 0 && nodeRestarter != null){
      nodeRestarter.add(allNodes, restartNodesArgs.isForce(),
        restartNodesArgs.getBatchSize());
    }
    else {
      LOG.info("There is no cluster node to restart");
    }
    RestartNodesResponse restartNodesResponse = new RestartNodesResponse();
    return restartNodesResponse;
  }

  /**
   * Sets the Safe Mode flag on the Cluster Manager, and on the ProxyJobTracker.
   * If we fail to set the flag on the ProxyJobTracker, return false, which
   * signals that setting the flag on the ProxyJobTracker failed. In that case,
   * we should run coronaadmin with the -forceSetSafeModeOnPJT or
   * -forceUnsetSafeModeOnPJT options.
   *
   * If we call this function multiple times, it wouldn't matter, because all
   * operations (apart from resetting of the last heartbeat time) in this
   * function, and in the setClusterManagerSafeModeFlag function in the
   * ProxyJobTracker are idempotent.
   *
   * @param safeMode The value of Safe Mode flag that we want to be set.
   * @return true, if setting the Safe Mode flag succeeded, false otherwise.
   */
  @Override
  public synchronized boolean setSafeMode(boolean safeMode) {
    /**
     * If we are switching off the safe mode, so we need to reset the last
     * heartbeat timestamp for each of the sessions and nodes.
     */
    if (safeMode == false) {
      LOG.info("Resetting the heartbeat times for all sessions");
      sessionManager.resetSessionsLastHeartbeatTime();
      LOG.info("Resetting the heartbeat times for all nodes");
      nodeManager.resetNodesLastHeartbeatTime();
      /**
       * If we are setting the safe mode to false, we should first set it
       * in-memory, before we set it at the CPJT.
       */
      this.safeMode = false;
    }
    try {
      ClusterManagerAvailabilityChecker.getPJTClient(conf).
        setClusterManagerSafeModeFlag(safeMode);
    } catch (IOException e) {
      LOG.info("Exception while setting the safe mode flag in ProxyJobTracker: "
        + e.getMessage());
      return false;
    } catch (TException e) {
      LOG.info("Exception while setting the safe mode flag in ProxyJobTracker: "
        + e.getMessage());
      return false;
    }
    this.safeMode = safeMode;
    LOG.info("Flag successfully set in ProxyJobTracker");
    LOG.info("Safe mode is now: " + (this.safeMode ? "ON" : "OFF"));
    return true;
  }

  /**
   * Get the current safe mode setting.
   */
  public boolean getSafeMode() {
    return safeMode;
  }

  /**
   * This function saves the state of the ClusterManager to disk.
   * @return A boolean. True if saving the state succeeded, false otherwise.
   */
  @Override
  public boolean persistState() {
    if (!safeMode) {
      LOG.info(
        "Cannot persist state because ClusterManager is not in Safe Mode");
      return false;
    }

    try {
      JsonGenerator jsonGenerator = CoronaSerializer.createJsonGenerator(conf);
      jsonGenerator.writeStartObject();

      jsonGenerator.writeFieldName("startTime");
      jsonGenerator.writeNumber(startTime);

      jsonGenerator.writeFieldName("nodeManager");
      nodeManager.write(jsonGenerator);

      jsonGenerator.writeFieldName("sessionManager");
      sessionManager.write(jsonGenerator);

      jsonGenerator.writeFieldName("sessionNotifier");
      sessionNotifier.write(jsonGenerator);

      jsonGenerator.writeEndObject();
      jsonGenerator.close();
    } catch (IOException e) {
      LOG.info("Could not persist the state: ", e);
      return false;
    }
    return true;
  }

  @Override
  public List<RunningSession> getSessions()
    throws TException, SafeModeException {
    checkSafeMode("getSessions");
    List<RunningSession> runningSessions = new LinkedList<RunningSession>();
    Set<String> sessions = sessionManager.getSessions();
    for (String sessionId : sessions) {
      try {
        Session session = sessionManager.getSession(sessionId);

        synchronized (session) {
          RunningSession runningSession =
              new RunningSession(session.getHandle(),
                  session.getName(),
                  session.getUserId(),
                  PoolInfo.createPoolInfoStrings(session.getPoolInfo()));
          runningSession.setDeadline(session.getDeadline());
          runningSession.setPriority(session.getInfo().getPriority());
          Map<ResourceType, Integer> runningResources =
              new EnumMap<ResourceType, Integer>(ResourceType.class);
          for (ResourceType type : ResourceType.values()) {
            runningResources.put(type, session.getGrantCountForType(type));
          }
          runningSession.setRunningResources(runningResources);
          runningSessions.add(runningSession);          
        }
      } catch (InvalidSessionHandle invalidSessionHandle) {
        // This is no big deal, just means that the session has finished
      }
    }
    return runningSessions;
  }

  @Override
  public SessionInfo getSessionInfo(String handle)
    throws TException, InvalidSessionHandle, SafeModeException {
    checkSafeMode("getSessionInfo");
    Session session = sessionManager.getSession(handle);
    return session.getInfo();
  }

  @Override
  public void killSession(String sessionId)
    throws TException, SafeModeException {
    checkSafeMode("killSession");
    try {
      LOG.info("Killing session " + sessionId);
      sessionEnd(sessionId, SessionStatus.KILLED);
    } catch (InvalidSessionHandle e) {
      throw new TException(e);
    }
  }
  
  @Override
  public void killSessions(KillSessionsArgs killSessionsArgs)
      throws SafeModeException, TException {
    StringBuilder msg = new StringBuilder();
    msg.append(killSessionsArgs.who);
    msg.append(" killed session");
    
    int killed = 0;
 
    for (String id: killSessionsArgs.sessionIds) {
      try {
        killSession(id);
        
        ++ killed;
        msg.append(" ");
        msg.append(id);
      } 
      finally {
        if (killed >= KILL_SESSIONS_THRESHOLD) {
          LOG.info(msg);
        }
      }
    } 
  }

  /**
   * This is an internal api called to tell the cluster manager that a
   * a particular node seems dysfunctional and that it should be removed
   * from the cluster.
   *
   * @param nodeName Node to be removed
   */
  public void nodeTimeout(String nodeName) {
    if (nodeRestarter != null) {
      nodeRestarter.delete(nodeName);
    }
    Set<String> sessions = nodeManager.getNodeSessions(nodeName);
    Set<ClusterNode.GrantId> grantsToRevoke = nodeManager.deleteNode(nodeName);
    if (grantsToRevoke == null) {
      return;
    }
    handleRevokedGrants(nodeName, grantsToRevoke);
    handleDeadNode(nodeName, sessions);
    scheduler.notifyScheduler();
  }

  /**
   * This is an internal api called to tell the cluster manager that a
   * particular node is excluded from the cluster.
   *
   * @param nodeName
   *          Node to be removed
   */
  public void nodeDecommisioned(String nodeName) {
    LOG.info("Node decommissioned: " + nodeName);
    // The logic for decommisioning is the same as that for a timeout.
    nodeTimeout(nodeName);
  }

  /**
   * This is an internal api called to tell the cluster manager that a
   * particular type of resource is no longer available on a node.
   *
   * @param nodeName
   *          Name of the node on which the resource is removed.
   * @param type
   *          The type of resource to be removed.
   */
  public void nodeAppRemoved(String nodeName, ResourceType type) {
    Set<String> sessions = nodeManager.getNodeSessions(nodeName);
    Set<ClusterNode.GrantId> grantsToRevoke =
      nodeManager.deleteAppFromNode(nodeName, type);
    if (grantsToRevoke == null) {
      return;
    }
    Set<String> affectedSessions = new HashSet<String>();
    for (String sessionHandle : sessions) {
      try {
        if (sessionManager.getSession(sessionHandle).
            getTypes().contains(type)) {
          affectedSessions.add(sessionHandle);
        }
      } catch (InvalidSessionHandle ex) {
        // ignore
        LOG.warn("Found invalid session: " + sessionHandle
            + " while timing out node: " + nodeName);
      }
    }
    handleDeadNode(nodeName, affectedSessions);
    handleRevokedGrants(nodeName, grantsToRevoke);
    scheduler.notifyScheduler();
  }

  /**
   * Process the grants removed from a node.
   *
   * @param nodeName
   *          The node name.
   * @param grantsToRevoke
   *          The grants to revoke.
   */
  private void handleRevokedGrants(
      String nodeName, Set<ClusterNode.GrantId> grantsToRevoke) {
    for (ClusterNode.GrantId grantId: grantsToRevoke) {
      String sessionHandle = grantId.getSessionId();

      try {
        sessionManager.revokeResource(sessionHandle,
            Collections.singletonList(grantId.getRequestId()));
      } catch (InvalidSessionHandle e) {
        // ignore
        LOG.warn("Found invalid session: " + sessionHandle +
                 " while timing out node: " + nodeName);
      }
    }
  }

  /**
   * All the sessions that had grants on this node should get notified
   * @param nodeName the name of the node that went dead
   */
  private void handleDeadNode(String nodeName, Set<String> sessions) {
    LOG.info("Notify sessions: " + sessions + " about dead node " + nodeName);
    for (String session : sessions) {
      sessionNotifier.notifyDeadNode(session, nodeName);
    }
  }

  public long getStartTime() {
    return startTime;
  }

  /**
   * Returns the last time the CM was restarted either safely, or otherwise.
   * @return Milliseconds since last restart
   */
  public long getLastRestartTime() {
    return lastRestartTime;
  }

  public String getHostName() {
    return hostName;
  }

  
}
