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
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.http.HttpServer;
import org.apache.hadoop.mapred.Clock;
import org.apache.hadoop.net.NetUtils;
import org.apache.thrift.TApplicationException;
import org.apache.thrift.TException;

public class ClusterManager implements ClusterManagerService.Iface {

  public static final Log LOG =
    LogFactory.getLog(ClusterManager.class);

  public static Clock clock = new Clock();

  // node manager manages collections of nodes
  protected NodeManager nodeManager;

  // session manager manages collections of sessions
  protected SessionManager sessionManager;

  // http server
  protected HttpServer infoServer;

  // the scheduler service matches free nodes to runnable sessions
  protected Scheduler scheduler;

  // the session notifier asynchronously notifies sessions about various events
  protected SessionNotifier sessionNotifier;

  protected ClusterManagerMetrics metrics;

  protected CoronaConf conf;

  // a bunch of variables for building UI
  protected long startTime;
  protected String hostName;

  protected Map<String, Object> legalTypes =
    new IdentityHashMap<String, Object> (256);

  protected void initLegalTypes() {
    Map<Integer, Map<String, Integer>> cpuToResourcePartitioning =
      conf.getCpuToResourcePartitioning();

    for(Map.Entry<Integer, Map<String, Integer>> entry:
          cpuToResourcePartitioning.entrySet()) {
      for (String type: entry.getValue().keySet()) {
        legalTypes.put(type.intern(), this);
      }
    }
    legalTypes = Collections.unmodifiableMap(legalTypes);
  }

  public ClusterManager() {
    // provided only to help testing
  }

  public ClusterManager(Configuration conf) throws IOException {
    this(new CoronaConf(conf));
  }

  public ClusterManager(CoronaConf conf) throws IOException {
    this.conf = conf;
    initLegalTypes();

    metrics = new ClusterManagerMetrics(getTypes());

    sessionManager = new SessionManager(this);
    sessionManager.setConf(conf);

    nodeManager = new NodeManager(this);
    nodeManager.setConf(conf);

    sessionNotifier = new SessionNotifier(sessionManager, this, metrics);
    sessionNotifier.setConf(conf);

    scheduler = new Scheduler(nodeManager, sessionManager,
        sessionNotifier, getTypes());
    scheduler.setConf(conf);
    scheduler.start();

    InetSocketAddress infoSocAddr = NetUtils.createSocketAddr(conf.getClusterManagerHttpAddress());
    infoServer = new HttpServer("cm", infoSocAddr.getHostName(), infoSocAddr.getPort(), 
                                infoSocAddr.getPort() == 0, conf);
    infoServer.setAttribute("cm", this);
    infoServer.start();

    startTime = clock.getTime();
    hostName = infoSocAddr.getHostName();
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

  public Collection<String> getTypes() {
    return Collections.unmodifiableCollection(legalTypes.keySet());
  }

  @Override
  public SessionRegistrationData sessionStart(SessionInfo info) throws TException {
    return new SessionRegistrationData(
        sessionManager.addSession(info), new ClusterManagerInfo("", ""));
  }

  @Override
    public void sessionEnd(String handle, SessionStatus status) throws TException, InvalidSessionHandle {
    try {
      LOG.info("sessionEnd called for session: " + handle + " with status: " + status);

      Collection<ResourceGrant> canceledGrants = sessionManager.deleteSession(handle, status);

      if (canceledGrants == null) {
        return;
      }

      for(ResourceGrant grant: canceledGrants) {
        nodeManager.cancelGrant(grant.nodeName, handle, grant.id);
        metrics.releaseResource(grant.type);
      }

      scheduler.notifyScheduler();
      sessionNotifier.deleteSession(handle);

    } catch (RuntimeException e) {
      throw new TApplicationException(e.getMessage());
    }
  }

  @Override
    public void sessionUpdateInfo(String handle, SessionInfo info) throws TException, InvalidSessionHandle {
    try {
      LOG.info("sessionUpdateInfo called for session: " + handle + " with info: " + info);
      sessionManager.updateInfo(handle, info);
    } catch (RuntimeException e) {
      throw new TApplicationException(e.getMessage());
    }
  }

  @Override
  public void sessionHeartbeat(String handle) throws TException, InvalidSessionHandle {
    try {
      sessionManager.heartbeat(handle);
    } catch (RuntimeException e) {
      throw new TApplicationException(e.getMessage());
    }
  }

  /**
   * canonicalize strings used in maps so that we can use cheaper identitymaps
   */
  protected void canonicalizeResourceRequest(List<ResourceRequest> requestList) {
    for(ResourceRequest req: requestList) {
      req.type = req.type.intern();
    }
  }

  protected boolean checkResourceRequestType(
      List<ResourceRequest> requestList) {
    for(ResourceRequest req: requestList) {
      if (legalTypes.get(req.type) == null)
        return false;
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
      canonicalizeResourceRequest(requestList);
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
      LOG.info ("Release " + idList.size() + " resources from session: " + handle);
      Collection<ResourceGrant> canceledGrants =
        sessionManager.releaseResource(handle, idList);

      if (canceledGrants == null) {
        // LOG.info("No canceled grants for session " + handle);
        return;
      }

      for(ResourceGrant grant: canceledGrants) {
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
    throws TException {
    //LOG.info("heartbeat from node: " + node.toString());
    if (nodeManager.heartbeat(node)) 
      scheduler.notifyScheduler();
  }

  /**
   * This is an internal api called to tell the cluster manager that a 
   * a particular node seems dysfunctional and that it should be removed
   * from the cluster
   */
  public void nodeTimeout(String nodeName) {
    Set<ClusterNode.GrantId> grantsToRevoke = nodeManager.deleteNode(nodeName);
    if (grantsToRevoke == null)
      return;

    for(ClusterNode.GrantId grantId: grantsToRevoke) {
      String sessionHandle = grantId.sessionId;

      try {
        List<ResourceGrant> revokedGrants =
          sessionManager.revokeResource(sessionHandle,
                                        Collections.singletonList(grantId.requestId));
        if ((revokedGrants != null) && !revokedGrants.isEmpty()) {
          sessionNotifier.notifyRevokeResource(sessionHandle, revokedGrants, false);
        }
      } catch (InvalidSessionHandle e) {
        // ignore
        LOG.warn("Found invalid session: " + sessionHandle + " while timing out node: " + nodeName);
      }
    }

    scheduler.notifyScheduler();
  }

  public long getStartTime() {
    return startTime;
  }

  public String getHostName() {
    return hostName;
  }
}
