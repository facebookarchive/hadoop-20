package org.apache.hadoop.corona;

import java.io.IOException;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;

import junit.framework.TestCase;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.net.TopologyCache;
import org.apache.thrift.TException;

public class TestResourceCheck extends TestCase {
  final static Log LOG = LogFactory.getLog(TestResourceCheck.class);

  public final static String sessionHost = "localhost";
  public static int getSessionPort(int i) {
    return (7000 + i);
  }

  private Configuration conf;
  private ClusterManagerTestable cm;
  private ClusterNodeInfo nodes [];
  private int numNodes;
  private SessionInfo sessionInfo;

  protected TopologyCache topologyCache;

  protected void setUp(int reservedMemoryMB, int reservedDiskGB)
      throws IOException {
    conf = new Configuration();
    conf.setClass("topology.node.switch.mapping.impl",
                  org.apache.hadoop.net.IPv4AddressTruncationMapping.class,
                  org.apache.hadoop.net.DNSToSwitchMapping.class);
    conf.set(CoronaConf.CPU_TO_RESOURCE_PARTITIONING, TstUtils.std_cpu_to_resource_partitioning);
    conf.setInt(CoronaConf.NODE_RESERVED_MEMORY_MB, reservedMemoryMB);
    conf.setInt(CoronaConf.NODE_RESERVED_DISK_GB, reservedDiskGB);

    topologyCache = new TopologyCache(conf);
    cm = new ClusterManagerTestable(conf);

    numNodes = 10;
    nodes = new ClusterNodeInfo[numNodes];
    Map<ResourceType, String> resourceInfos =
        new EnumMap<ResourceType, String>(ResourceType.class);
    resourceInfos.put(ResourceType.MAP, "");
    resourceInfos.put(ResourceType.REDUCE, "");
    for (int i = 0; i < numNodes; ++i) {
      nodes[i] = new ClusterNodeInfo(TstUtils.getNodeHost(i),
                                     new InetAddress(TstUtils.getNodeHost(i),
                                                     TstUtils.getNodePort(i)),
                                     TstUtils.std_spec);
      nodes[i].setFree(TstUtils.std_spec);
      nodes[i].setResourceInfos(resourceInfos);
    }
    sessionInfo = new SessionInfo(
        new InetAddress(sessionHost, getSessionPort(0)), "s", "hadoop");
    sessionInfo.setPriority(SessionPriority.NORMAL);
    sessionInfo.setPoolInfoStrings(
        PoolInfo.createPoolInfoStrings(PoolGroupManager.DEFAULT_POOL_INFO));
  }

  public void testMemoryCheck() throws Exception {
    // Set a high memory limit
    setUp(TstUtils.std_spec.memoryMB + 1, 0);
    String handle = TstUtils.startSession(cm, sessionInfo);
    Session session = cm.getSessionManager().getSession(handle);
    int maps = 800;
    int reduces = 100;
    submitRequests(handle, maps, reduces);
    verifySession(session, ResourceType.MAP, maps, 0);
    verifySession(session, ResourceType.REDUCE, reduces, 0);

    addAllNodes();

    TstUtils.reliableSleep(100);

    // Nothing granted because of the memory limit
    verifySession(session, ResourceType.MAP, maps, 0);
    verifySession(session, ResourceType.REDUCE, reduces, 0);

    cm.getNodeManager().getResourceLimit().setNodeReservedMemoryMB(
      TstUtils.std_spec.memoryMB - 1);
    // Perform heartbeats.
    addAllNodes();
    TstUtils.reliableSleep(500);
    int maxMaps = cm.getNodeManager().getMaxCpuForType(ResourceType.MAP);
    int maxReduces = cm.getNodeManager().getMaxCpuForType(ResourceType.REDUCE);

    // Granted after changing the limit
    verifySession(session, ResourceType.MAP, maps, maxMaps);
    verifySession(session, ResourceType.REDUCE, reduces, maxReduces);

    cm.sessionEnd(handle, SessionStatus.SUCCESSFUL);
  }

  public void testDiskCheck() throws Exception {
    // Set a high memory limit
    setUp(0, TstUtils.std_spec.diskGB + 1);
    String handle = TstUtils.startSession(cm, sessionInfo);
    Session session = cm.getSessionManager().getSession(handle);
    int maps = 800;
    int reduces = 100;
    submitRequests(handle, maps, reduces);
    verifySession(session, ResourceType.MAP, maps, 0);
    verifySession(session, ResourceType.REDUCE, reduces, 0);

    addAllNodes();

    TstUtils.reliableSleep(100);

    // Nothing granted because of the memory limit
    verifySession(session, ResourceType.MAP, maps, 0);
    verifySession(session, ResourceType.REDUCE, reduces, 0);

    cm.getNodeManager().getResourceLimit().setNodeReservedDiskGB(
      TstUtils.std_spec.diskGB - 1);
    // Perform heartbeats.
    addAllNodes();
    TstUtils.reliableSleep(500);
    int maxMaps = cm.getNodeManager().getMaxCpuForType(ResourceType.MAP);
    int maxReduces = cm.getNodeManager().getMaxCpuForType(ResourceType.REDUCE);

    // Granted after changing the limit
    verifySession(session, ResourceType.MAP, maps, maxMaps);
    verifySession(session, ResourceType.REDUCE, reduces, maxReduces);

    cm.sessionEnd(handle, SessionStatus.SUCCESSFUL);
  }

  public void testResourceUpdate() throws Exception {
    setUp(1, 0);
    addSomeNodes(1);
    ClusterNodeInfo newInfo = new ClusterNodeInfo(nodes[0]);
    // Fully used.
    newInfo.setFree(TstUtils.nothing_free_spec);
    cm.nodeHeartbeat(newInfo);

    String handle = TstUtils.startSession(cm, sessionInfo);
    Session session = cm.getSessionManager().getSession(handle);
    int maps = 80;
    int reduces = 10;
    submitRequests(handle, maps, reduces);

    TstUtils.reliableSleep(100);

    // Nothing granted.
    verifySession(session, ResourceType.MAP, maps, 0);
    verifySession(session, ResourceType.REDUCE, reduces, 0);

    // Node is free
    newInfo = new ClusterNodeInfo(newInfo);
    newInfo.setFree(TstUtils.std_spec);
    cm.nodeHeartbeat(newInfo);

    TstUtils.reliableSleep(500);
    int maxMaps = cm.getNodeManager().getMaxCpuForType(ResourceType.MAP);
    int maxReduces = cm.getNodeManager().getMaxCpuForType(ResourceType.REDUCE);
    verifySession(session, ResourceType.MAP, maps, maxMaps);
    verifySession(session, ResourceType.REDUCE, reduces, maxReduces);
  }

  private void submitRequests(String handle, int maps, int reduces)
      throws TException, InvalidSessionHandle, SafeModeException {
    List<ResourceRequest> requests =
      TstUtils.createRequests(this.numNodes, maps, reduces);
    cm.requestResource(handle, requests);
  }

  private void verifySession(Session session, ResourceType type,
      int request, int grant) {
    synchronized (session) {
      assertEquals(grant, session.getGrantCountForType(type));
      assertEquals(request, session.getRequestCountForType(type));
      assertEquals(request - grant,
          session.getPendingRequestForType(type).size());
    }
  }

  private void addSomeNodes(int count) throws TException {
    for (int i=0; i<count; i++) {
      try {
        cm.nodeHeartbeat(nodes[i]);
      } catch (DisallowedNode e) {
        throw new TException(e);
      } catch (SafeModeException e) {
        LOG.info("Cluster Manager is in Safe Mode");
      }
    }
  }

  private void addAllNodes() throws TException {
    addSomeNodes(this.numNodes);
  }

}
