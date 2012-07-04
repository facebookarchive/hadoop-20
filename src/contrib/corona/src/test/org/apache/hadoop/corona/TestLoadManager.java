package org.apache.hadoop.corona;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;

import junit.framework.TestCase;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.ResourceTracker;
import org.apache.hadoop.net.Node;
import org.apache.thrift.TException;

public class TestLoadManager extends TestCase {
  private static final Log LOG = LogFactory.getLog(TestLoadManager.class);
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

  @Override
  protected void setUp() throws IOException {
    conf = new Configuration();
    conf.setClass("topology.node.switch.mapping.impl",
      org.apache.hadoop.net.IPv4AddressTruncationMapping.class,
      org.apache.hadoop.net.DNSToSwitchMapping.class);
    conf.set(CoronaConf.CPU_TO_RESOURCE_PARTITIONING, TstUtils.std_cpu_to_resource_partitioning);

    cm = new ClusterManagerTestable(conf);

    numNodes = 100;
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
      nodes[i].setFree(TstUtils.std_spec);
      nodes[i].setResourceInfos(resourceInfos);
    }

    numSessions = 5;
    sessionInfos = new SessionInfo [numSessions];
    handles = new String [numSessions];
    sessions =  new Session [numSessions];

    for (int i =0; i<numSessions; i++) {
      sessionInfos[i] = new SessionInfo(new InetAddress(sessionHost, getSessionPort(i)),
        "s_" + i, "hadoop");
      sessionInfos[i].setPriority(SessionPriority.NORMAL);
      sessionInfos[i].setPoolInfoStrings(
          PoolInfo.createPoolInfoStrings(PoolGroupManager.DEFAULT_POOL_INFO));
    }
  }

  public void testDistributeLoad() throws Exception {
    LOG.info("Starting testDistributeLoad");
    cm = new ClusterManagerTestable(conf);
    NodeManager nm = cm.getNodeManager();

    int nodeCount = 10;
    addSomeNodes(nodeCount);

    String handle = TstUtils.startSession(cm, sessionInfos[0]);
    Session session = cm.getSessionManager().getSession(handle);

    // Request reducers (no locality requirement).
    // Verify that the grants are distributed evenly across nodes.
    cm.requestResource(handle, TstUtils.createRequests(nodeCount,
      0, nodeCount));
    Thread.sleep(100);

    // Each node should be assigned 1 REDUCE.
    for (ClusterNode node : nm.nameToNode.values()) {
      assertEquals(1, node.getGrantCount(ResourceType.REDUCE));
    }
    LOG.info("Ending testDistributeLoad");
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
}
