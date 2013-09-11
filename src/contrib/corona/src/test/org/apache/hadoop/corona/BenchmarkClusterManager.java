package org.apache.hadoop.corona;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.net.IPv4AddressTruncationMapping;
import org.apache.thrift.TException;

import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * This is a benchmark for the scheduler part of the cluster manager
 * The way it works is it creates a bunch of sessions and adds them to the
 * scheduler. As they are getting scheduled they grants are being handed off to
 * the SessionListener.
 * SessionListeners are threads that periodically return granted resources
 * simulating what happens in the real cluster.
 *
 * This benchmark lets us see how efficient the scheduling part of the cluster
 * manager is when it is bombarded with sessions, requests and releases
 */
public class BenchmarkClusterManager {
  public static final Log LOG = LogFactory.getLog(BenchmarkClusterManager.class);

  public final static String sessionHost = "localhost";

  public static int getSessionPort(int i) {
    return (7000 + i);
  }

  private void printUsage() {
    System.err.println("Usage: BenchmarkClusterManager numNodes numSessions");
    System.err.println("\tnumNodes is the number of nodes to simulate for the cluster");
    System.err.println("\tnumSessions is the number of active sessions to simulate");
  }

  public static void main(String[] args) throws Exception {
    TstUtils.nodesPerRack = 40;
    Configuration conf = new Configuration();
    conf.setClass("topology.node.switch.mapping.impl",
        IPv4AddressTruncationMapping.class,
        org.apache.hadoop.net.DNSToSwitchMapping.class);
    conf.set(CoronaConf.CPU_TO_RESOURCE_PARTITIONING, TstUtils.std_cpu_to_resource_partitioning);

    ClusterManager cm = new ClusterManagerTestable(new CoronaConf(conf), true);
    CallbackSessionNotifier notifier = (CallbackSessionNotifier) cm.getSessionNotifier();
    int numNodes = Integer.parseInt(args[0]);
    ClusterNodeInfo[] nodes = new ClusterNodeInfo[numNodes];
    Map<ResourceType, String> resourceInfos =
        new EnumMap<ResourceType, String>(ResourceType.class);
    resourceInfos.put(ResourceType.MAP, "");
    resourceInfos.put(ResourceType.REDUCE, "");
    for (int i = 0; i < numNodes; i++) {
      nodes[i] = new ClusterNodeInfo(TstUtils.getNodeHost(i),
          new InetAddress(TstUtils.getNodeHost(i),
              TstUtils.getNodePort(i)),
          TstUtils.std_spec);
      nodes[i].setFree(TstUtils.std_spec);
      nodes[i].setResourceInfos(resourceInfos);
    }

    int numSessions = Integer.parseInt(args[1]);
    SessionInfo[] sessionInfos = new SessionInfo[numSessions];
    SessionRunner[] sessionRunners = new SessionRunner[numSessions];
    Thread[] runningSessions = new Thread[numSessions];
    for (int i = 0; i < numSessions; i++) {
      sessionInfos[i] = new SessionInfo(new InetAddress(sessionHost, getSessionPort(i)),
          "s_" + i, "hadoop");
      sessionInfos[i].setPriority(SessionPriority.NORMAL);
      String handle = TstUtils.startSession(cm, sessionInfos[i]);
      SessionRunner runner = new SessionRunner(handle, cm, numNodes, numSessions);
      notifier.addSession(handle, runner);
      Thread t = new Thread(runner);
      t.setName("SessionRunner-" + i);
      runningSessions[i] = t;
    }

    NodeHeartbeater heartbeater = new NodeHeartbeater(cm, nodes);
    // One round of node heartbeats
    heartbeater.run();
    Thread heartbeatThread = new Thread(heartbeater);
    heartbeatThread.setDaemon(true);
    heartbeatThread.start();

    for (Thread t : runningSessions) {
      t.start();
    }
    for (Thread t : runningSessions) {
      t.join();
    }
    // Exit after all sessions are done.
    System.exit(0);
  }

  /**
   * This runnable 'heartbeats' with the CM on behalf of the simulated nodes.
   * So that the cluster manager doesn't try to declare those nodes dead
   */
  private static class NodeHeartbeater implements Runnable {
    ClusterManager cm;
    ClusterNodeInfo[] nodes;

    /**
     * Construct a NodeHeartbeater for a ClusterManager given the nodes
     * information
     * @param cm the cluster manager object to heartbeat with
     * @param nodes the list of node info objects
     */
    private NodeHeartbeater(ClusterManager cm, ClusterNodeInfo[] nodes) {
      this.cm = cm;
      this.nodes = nodes;
    }

    @Override
    public void run() {
      for (int i = 0; i < nodes.length; i++) {
        try {
          cm.nodeHeartbeat(nodes[i]);
        } catch (DisallowedNode dex) {
          LOG.error("Node disallowed ", dex);
        } catch (SafeModeException e) {
          LOG.info("Cluster Manager is in Safe Mode");
        } catch (TException e) {
          LOG.error("Node heartbeat error ", e);
        }
      }
    }
  }

  /**
   * This is a helper class that receives grants and releases them back to
   * cluster manager in batches.
   * This simulates a running session behaviour
   */
  private static class SessionRunner implements SessionListener, Runnable {
    ClusterManager cm;
    String handle;
    SessionInfo session;
    int numNodes;
    int maps = 40000;
    int reducers = 397;
    LinkedBlockingQueue<ResourceGrant> granted = new LinkedBlockingQueue<ResourceGrant>();
    private final List<ResourceRequest> requests;
    private final int delayMsec;

    private SessionRunner(
      String handle, ClusterManager cm, int numNodes, int numSessions) {
      this.cm = cm;
      this.handle = handle;
      this.numNodes = numNodes;
      this.requests = TstUtils.createRequests(numNodes, maps, reducers);
      this.delayMsec = new java.util.Random().nextInt(numSessions);
    }

    @Override
    public void run() {
      try {
        Thread.sleep(delayMsec);
        cm.requestResource(handle, requests);
        List<Integer> toRelease = new ArrayList<Integer>(50);
        while (maps > 0 || reducers > 0) {
          ResourceGrant grant = granted.poll(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
          if (grant.type == ResourceType.MAP) {
            maps--;
          } else {
            reducers--;
          }
          toRelease.add(grant.getId());
          if (toRelease.size() > 40) {
            cm.releaseResource(this.handle, toRelease);
            toRelease = new ArrayList<Integer>(50);
          }
        }

        cm.releaseResource(this.handle, toRelease);

        cm.sessionEnd(this.handle, SessionStatus.SUCCESSFUL);

      } catch (InvalidSessionHandle ex) {
        LOG.error("SessionRunner invalid session ", ex);
      } catch (TException ex) {
        LOG.error("SessionRunner thrift error ", ex);
      } catch (Throwable t) {
        LOG.fatal("Error ", t);
      }
    }

    @Override
    public void notifyGrantResource(List<ResourceGrant> grants) {
      for (ResourceGrant g: grants) {
        granted.offer(g);
      }
      return;
    }
  }
}
