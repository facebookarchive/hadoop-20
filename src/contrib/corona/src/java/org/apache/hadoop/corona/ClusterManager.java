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
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.http.HttpServer;
import org.apache.hadoop.mapred.Clock;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.util.HostsFileReader;
import org.apache.thrift.TApplicationException;
import org.apache.thrift.TException;

/**
 * Manager of all the resources of the cluster.
 */
public class ClusterManager implements ClusterManagerService.Iface {
  /** Class logger */
  public static final Log LOG = LogFactory.getLog(ClusterManager.class);
  /** Clock that is used for any general purpose system times. */
  public static Clock clock = new Clock();

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
  /** Host name to show in UI. */
  protected String hostName;

  /** Legal values for the "type" of a resource request. */
  protected Set<ResourceType> legalTypeSet =
      EnumSet.noneOf(ResourceType.class);

  /**
   * Simple constructor for testing help.
   */
  public ClusterManager() { }

  /**
   * Primary constructor.
   *
   * @param conf Configuration to be used
   * @throws IOException
   */
  public ClusterManager(Configuration conf) throws IOException {
    this(new CoronaConf(conf));
  }

  /**
   * Construct ClusterManager given {@link CoronaConf}
   *
   * @param conf the configuration for the ClusterManager
   * @throws IOException
   */
  public ClusterManager(CoronaConf conf) throws IOException {
    this.conf = conf;
    initLegalTypes();

    metrics = new ClusterManagerMetrics(getTypes());

    sessionManager = new SessionManager(this);
    sessionManager.setConf(conf);

    sessionHistoryManager = new SessionHistoryManager();
    sessionHistoryManager.setConf(conf);
    sessionHistoryManager.initialize();

    HostsFileReader hostsReader =
        new HostsFileReader(conf.getHostsFile(), conf.getExcludesFile());
    nodeManager = new NodeManager(this, hostsReader);
    nodeManager.setConf(conf);

    sessionNotifier = new SessionNotifier(sessionManager, this, metrics);
    sessionNotifier.setConf(conf);

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

    startTime = clock.getTime();
    hostName = infoSocAddr.getHostName();
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

  @Override
  public SessionRegistrationData sessionStart(SessionInfo info)
    throws TException {
    String sessionLogPath = sessionHistoryManager.getCurrentLogPath();
    Session session = sessionManager.addSession(info);
    String pool = scheduler.getPoolName(session);
    return new SessionRegistrationData(
      session.getHandle(), new ClusterManagerInfo("", sessionLogPath), pool);
  }

  @Override
  public void sessionEnd(String handle, SessionStatus status)
    throws TException, InvalidSessionHandle {
    try {
      LOG.info("sessionEnd called for session: " + handle + " with status: " +
               status);

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
      throws TException, InvalidSessionHandle {
    try {
      LOG.info("sessionUpdateInfo called for session: " + handle +
               " with info: " + info);
      sessionManager.updateInfo(handle, info);
    } catch (RuntimeException e) {
      throw new TApplicationException(e.getMessage());
    }
  }

  @Override
  public void sessionHeartbeat(String handle) throws TException,
      InvalidSessionHandle {
    try {
      sessionManager.heartbeat(handle);
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

  @Override
  public void requestResource(String handle, List<ResourceRequest> requestList)
    throws TException, InvalidSessionHandle {
    try {
      LOG.info ("Request " + requestList.size() +
          " resources from session: " + handle);
      if (!checkResourceRequestType(requestList)) {
        LOG.error ("Bad resource type from session: " + handle);
        throw new TApplicationException("Bad resource type");
      }
      if (!checkResourceRequestExcluded(requestList)) {
        LOG.error ("Bad excluded hosts from session: " + handle);
        throw new TApplicationException("Requesting excluded hosts");
      }
      sessionManager.requestResource(handle, requestList);
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
    throws TException, InvalidSessionHandle {
    try {
      LOG.info("Release " + idList.size() + " resources from session: " +
               handle);
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
  public void nodeHeartbeat(ClusterNodeInfo node)
    throws TException, DisallowedNode {
    //LOG.info("heartbeat from node: " + node.toString());
    if (nodeManager.heartbeat(node)) {
      scheduler.notifyScheduler();
    }
  }

  @Override
  public void nodeFeedback(String handle, List<ResourceType> resourceTypes,
      List<NodeUsageReport> reportList)
    throws TException, InvalidSessionHandle {
    LOG.info("Received feedback from session " + handle);
    nodeManager.nodeFeedback(handle, resourceTypes, reportList);
  }

  @Override
  public void refreshNodes() throws TException {
    try {
      nodeManager.refreshNodes();
    } catch (IOException e) {
      throw new TException(e);
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

  public String getHostName() {
    return hostName;
  }
}
