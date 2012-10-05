package org.apache.hadoop.corona;

import java.io.IOException;
import java.util.EnumMap;
import java.util.List;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import junit.framework.TestCase;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.ResourceTracker;
import org.apache.thrift.TException;

public class TestPreemption extends TestCase {
  final static Log LOG = LogFactory.getLog(TestPreemption.class);

  public final static String sessionHost = "localhost";
  public static int getSessionPort(int i) {
    return (7000 + i);
  }

  private Configuration conf;
  private ClusterManagerTestable cm;

  private ClusterNodeInfo nodes [];
  private int numNodes;

  private SessionInfo sessionInfos [];
  private int numSessions;

  private String handles [];
  private Session sessions [];

  protected TopologyCache topologyCache;

  @Override
  protected void setUp() throws IOException {
    conf = new Configuration();
    conf.setBoolean(CoronaConf.CONFIGURED_POOLS_ONLY, false);
    conf.setClass("topology.node.switch.mapping.impl",
                  org.apache.hadoop.net.IPv4AddressTruncationMapping.class,
                  org.apache.hadoop.net.DNSToSwitchMapping.class);
    conf.set(CoronaConf.CPU_TO_RESOURCE_PARTITIONING, TstUtils.std_cpu_to_resource_partitioning);

    topologyCache = new TopologyCache(conf);
    cm = new ClusterManagerTestable(conf);

    numNodes = 10;
    nodes = new ClusterNodeInfo[numNodes];
    Map<ResourceType, String> resourceInfos =
        new EnumMap<ResourceType, String>(ResourceType.class);
    resourceInfos.put(ResourceType.MAP, "");
    resourceInfos.put(ResourceType.REDUCE, "");
    for (int i=0; i<numNodes; i++) {
      nodes[i] = new ClusterNodeInfo(TstUtils.getNodeHost(i),
                                     new InetAddress(TstUtils.getNodeHost(i),
                                                     TstUtils.getNodePort(i)),
                                     TstUtils.std_spec);
      nodes[i].setUsed(TstUtils.free_spec);
      nodes[i].setResourceInfos(resourceInfos);
    }

    numSessions = 3;
    sessionInfos = new SessionInfo [numSessions];
    handles = new String [numSessions];
    sessions =  new Session [numSessions];

    for (int i =0; i<numSessions; i++) {
      sessionInfos[i] = new SessionInfo(new InetAddress(sessionHost, getSessionPort(i)),
                                        "s_" + i, "hadoop");
      sessionInfos[i].setPriority(SessionPriority.NORMAL);
      sessionInfos[i].setPoolId("pool" + i);
    }
  }

  public void testPreemptForMinimum() throws Throwable {
    FakeConfigManager configManager = cm.getConfigManager();
    int s1MinSlots = 60;
    configManager.setMinimum("pool1", ResourceType.MAP, s1MinSlots);
    configManager.setStarvingTimeForMinimum(200L);

    try {
      for (int i=0; i<numSessions; i++) {
        handles[i] = cm.sessionStart(sessionInfos[i]).handle;
        sessions[i] = cm.getSessionManager().getSession(handles[i]);
        TstUtils.reliableSleep(500);
      }
      int [] maps = {800, 100};
      int [] reduces = {800, 100};
      submitRequests(handles[0], maps[0], reduces[0]);
      verifySession(sessions[0], ResourceType.MAP, maps[0], 0);
      verifySession(sessions[0], ResourceType.REDUCE, reduces[0], 0);

      addAllNodes();

      TstUtils.reliableSleep(1000);
      int maxMaps = cm.getNodeManager().getMaxCpuForType(ResourceType.MAP);
      int maxReduces = cm.getNodeManager().getMaxCpuForType(ResourceType.REDUCE);
      verifySession(sessions[0], ResourceType.MAP, maps[0], maxMaps);
      verifySession(sessions[0], ResourceType.REDUCE, reduces[0], maxReduces);

      // Pool1 has a minimum of 60 for M, so it preempts 60 slots
      submitRequests(handles[1], maps[1], reduces[1]);
      TstUtils.reliableSleep(SchedulerForType.PREEMPTION_PERIOD * 2);
      verifySession(sessions[0], ResourceType.MAP, maps[0], maxMaps - s1MinSlots, s1MinSlots);
      verifySession(sessions[0], ResourceType.REDUCE, reduces[0], maxReduces);
      verifySession(sessions[1], ResourceType.MAP, maps[1], s1MinSlots);
      verifySession(sessions[1], ResourceType.REDUCE, reduces[1], 0);

      for (int i = 0; i < numSessions; i++) {
        cm.sessionEnd(handles[i], SessionStatus.SUCCESSFUL);
      }

    } catch (InvalidSessionHandle e) {
      LOG.error("Bad Session Handle");
      assertEquals("Bad Session Handle", null);
    } catch (Throwable t) {
      t.printStackTrace();
      throw t;
    }
  }

  public void testPreemptForShare() throws Throwable {
    FakeConfigManager configManager = cm.getConfigManager();
    configManager.setShareStarvingRatio(0.5);
    configManager.setStarvingTimeForShare(200L);

    try {
      for (int i=0; i<numSessions; i++) {
        handles[i] = cm.sessionStart(sessionInfos[i]).handle;
        sessions[i] = cm.getSessionManager().getSession(handles[i]);
        TstUtils.reliableSleep(500);
      }
      int [] maps = {800, 100};
      int [] reduces = {800, 100};
      submitRequests(handles[0], maps[0], reduces[0]);
      verifySession(sessions[0], ResourceType.MAP, maps[0], 0);
      verifySession(sessions[0], ResourceType.REDUCE, reduces[0], 0);

      addAllNodes();

      TstUtils.reliableSleep(100);
      int maxMaps = cm.getNodeManager().getMaxCpuForType(ResourceType.MAP);
      int maxReduces = cm.getNodeManager().getMaxCpuForType(ResourceType.REDUCE);
      verifySession(sessions[0], ResourceType.MAP, maps[0], maxMaps);
      verifySession(sessions[0], ResourceType.REDUCE, reduces[0], maxReduces);

      // Pool1 will starving for share. So it preempt half of M and R slots
      submitRequests(handles[1], maps[1], reduces[1]);
      TstUtils.reliableSleep(SchedulerForType.PREEMPTION_PERIOD * 2);
      verifySession(sessions[0], ResourceType.MAP, maps[0], maxMaps / 2, maxMaps / 2);
      verifySession(sessions[0], ResourceType.REDUCE, reduces[0], maxReduces / 2, maxReduces / 2);
      verifySession(sessions[1], ResourceType.MAP, maps[1], maxMaps / 2);
      verifySession(sessions[1], ResourceType.REDUCE, reduces[1], maxReduces / 2);

      for (int i = 0; i < numSessions; i++) {
        cm.sessionEnd(handles[i], SessionStatus.SUCCESSFUL);
      }

    } catch (InvalidSessionHandle e) {
      LOG.error("Bad Session Handle");
      assertEquals("Bad Session Handle", null);
    } catch (Throwable t) {
      t.printStackTrace();
      throw t;
    }
  }

  private void submitRequests(String handle, int maps, int reduces)
      throws TException, InvalidSessionHandle {
    List<ResourceRequest> requests =
      TstUtils.createRequests(this.numNodes, maps, reduces);
    cm.requestResource(handle, requests);
  }

  private void verifySession(Session session, ResourceType type,
      int request, int grant, int preempted) {
    synchronized (session) {
      assertEquals(grant, session.getGrantCountForType(type));
      assertEquals(request, session.getRequestCountForType(type));
      assertEquals(request - grant - preempted,
          session.getPendingRequestForType(type).size());
    }
  }

  private void verifySession(Session session, ResourceType type,
      int request, int grant) {
    verifySession(session, type, request, grant, 0);
  }

  private void addSomeNodes(int count) throws TException {
    for (int i=0; i<count; i++) {
      try {
        cm.nodeHeartbeat(nodes[i]);
      } catch (DisallowedNode e) {
        throw new TException(e);
      }
    }
  }

  private void addAllNodes() throws TException {
    addSomeNodes(this.numNodes);
  }

}
