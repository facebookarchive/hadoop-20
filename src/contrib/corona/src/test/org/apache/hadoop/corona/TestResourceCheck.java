package org.apache.hadoop.corona;

import java.io.IOException;
import java.util.List;

import junit.framework.TestCase;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.ResourceTracker;
import org.apache.thrift.TException;

public class TestResourceCheck extends TestCase {
  final static Log LOG = LogFactory.getLog(TestResourceCheck.class);

  public final static String sessionHost = "localhost";
  public static int getSessionPort(int i) {
    return (7000 + i);
  }

  final static String M = ResourceTracker.RESOURCE_TYPE_MAP;
  final static String R = ResourceTracker.RESOURCE_TYPE_REDUCE;

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
    for (int i = 0; i < numNodes; ++i) {
      nodes[i] = new ClusterNodeInfo(TstUtils.getNodeHost(i),
                                     new InetAddress(TstUtils.getNodeHost(i),
                                                     TstUtils.getNodePort(i)),
                                     TstUtils.std_spec);
      nodes[i].setUsed(TstUtils.free_spec);
    }
    sessionInfo = new SessionInfo(
        new InetAddress(sessionHost, getSessionPort(0)), "s", "hadoop");
    sessionInfo.setPriority(SessionPriority.NORMAL);
  }

  public void testMemoryCheck() throws Exception {
    // Set a high memory limit
    setUp(TstUtils.std_spec.memoryMB + 1, 0);
    String handle = cm.sessionStart(sessionInfo).handle;
    Session session = cm.getSessionManager().getSession(handle);
    int maps = 800;
    int reduces = 100;
    submitRequests(handle, maps, reduces);
    verifySession(session, M, maps, 0);
    verifySession(session, R, reduces, 0);

    addAllNodes();

    TstUtils.reliableSleep(100);

    // Nothing granted because of the memory limit
    verifySession(session, M, maps, 0);
    verifySession(session, R, reduces, 0);

    cm.getNodeManager().setNodeReservedMemoryMB(TstUtils.std_spec.memoryMB - 1);
    TstUtils.reliableSleep(500);
    int maxMaps = cm.getNodeManager().getMaxCpuForType(M);
    int maxReduces = cm.getNodeManager().getMaxCpuForType(R);

    // Granted after changing the limit
    verifySession(session, M, maps, maxMaps);
    verifySession(session, R, reduces, maxReduces);

    cm.sessionEnd(handle, SessionStatus.SUCCESSFUL);
  }

  public void testDiskCheck() throws Exception {
    // Set a high memory limit
    setUp(0, TstUtils.std_spec.diskGB + 1);
    String handle = cm.sessionStart(sessionInfo).handle;
    Session session = cm.getSessionManager().getSession(handle);
    int maps = 800;
    int reduces = 100;
    submitRequests(handle, maps, reduces);
    verifySession(session, M, maps, 0);
    verifySession(session, R, reduces, 0);

    addAllNodes();

    TstUtils.reliableSleep(100);

    // Nothing granted because of the memory limit
    verifySession(session, M, maps, 0);
    verifySession(session, R, reduces, 0);

    cm.getNodeManager().setNodeReservedDiskGB(TstUtils.std_spec.diskGB - 1);
    TstUtils.reliableSleep(500);
    int maxMaps = cm.getNodeManager().getMaxCpuForType(M);
    int maxReduces = cm.getNodeManager().getMaxCpuForType(R);

    // Granted after changing the limit
    verifySession(session, M, maps, maxMaps);
    verifySession(session, R, reduces, maxReduces);

    cm.sessionEnd(handle, SessionStatus.SUCCESSFUL);
  }

  private void submitRequests(String handle, int maps, int reduces)
      throws TException, InvalidSessionHandle {
    List<ResourceRequest> requests =
      TstUtils.createRequests(this.numNodes, maps, reduces);
    cm.requestResource(handle, requests);
  }

  private void verifySession(Session session, String type,
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
      cm.nodeHeartbeat(nodes[i]);
    }
  }

  private void addAllNodes() throws TException {
    addSomeNodes(this.numNodes);
  }

}
