package org.apache.hadoop.mapred;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.HashSet;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.http.HttpServer;
import org.apache.hadoop.ipc.ProtocolSignature;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class DynamicCloudsDaemon implements Tool,
        ClusterBalancerAdminProtocol {

  public static final Log LOG =
          LogFactory.getLog(DynamicCloudsDaemon.class);
  private Configuration conf;
  public final static String CLUSTER_BALANCER_CONF_FILE =
          "cluster-balancer.xml";

  public final static String DEBUG_MODE = "clusterbalancer.debug";
  public final static String CLUSTER_BALANCER_ADDR =
          "clusterbalancer.server";
  public final static String CLUSTER_HTTP_BALANCER_ADDR =
          "clusterbalancer.http.server";
  public final static String BALANCER_CLUSTER_PREFIX =
          "clusterbalancer.cluster.";
  public final static String BALANCER_CLUSTERS_CONF =
          "clusterbalancer.clusters";
  public final static String CLUSTER_MIN_LOAD = ".min_load";
  public final static String CLUSTER_MAX_LOAD = ".max_load";
  public final static String CLUSTER_MIN_NODES = ".min_nodes";
  public final static String CLUSTER_WAIT_FOR_MAPS = ".wait_for_maps";
  public final static String CLUSTER_NOT_TO_MOVE_FILE = ".exclusive_trackers";
  public static boolean DEBUG = false;

  private static String START_TASKTRACKER_COMMAND_21 =
          "bin/hadoop-daemon.sh --script bin/mapred start tasktracker";
  private static String START_TASKTRACKER_COMMAND_20 =
          "bin/hadoop-daemon.sh start tasktracker";
  private static final int POLL_WAIT_TIME = 2000;
  
  private Server clusterDaemonServer;
  private HttpServer infoServer;
  private TTLauncher ttLauncher;
  private Thread launcherThread;

  private volatile boolean shutdown = false;

  // poll clusters every 2 seconds
  private Map<String, Cluster> clusters = new HashMap<String, Cluster>();

  public DynamicCloudsDaemon() {
    conf = new Configuration();
    conf.addResource(CLUSTER_BALANCER_CONF_FILE);

  }

  public void initializeServer() throws IOException {

    String serverAddr = conf.get(CLUSTER_BALANCER_ADDR, "localhost:9143");
    InetSocketAddress addr = NetUtils.createSocketAddr(serverAddr);
    clusterDaemonServer = RPC.getServer(this, addr.getHostName(),
            addr.getPort(), conf);
    clusterDaemonServer.start();

    // Http server
    String infoServerAddr = conf.get(CLUSTER_HTTP_BALANCER_ADDR,
            "localhost:50143");
    InetSocketAddress infoAddr = NetUtils.createSocketAddr(infoServerAddr);
    infoServer = new HttpServer("cb", infoAddr.getHostName(),
            infoAddr.getPort(), infoAddr.getPort() == 0, conf);
    infoServer.setAttribute("cluster.balancer", this);
    infoServer.start();
  }

  public void initializeClusters() throws IOException {
    LOG.info("Initializing Clusters");
    String[] clusterNames = conf.getStrings(BALANCER_CLUSTERS_CONF);
    for (String clusterName : clusterNames) {
      String httpAddr = conf.get(BALANCER_CLUSTER_PREFIX + clusterName);

      int minLoad =
              conf.getInt(BALANCER_CLUSTER_PREFIX +
              clusterName + CLUSTER_MIN_LOAD, 100);
      int maxLoad = conf.getInt(BALANCER_CLUSTER_PREFIX +
              clusterName + CLUSTER_MAX_LOAD, 100);
      boolean waitForMaps = conf.getBoolean(BALANCER_CLUSTER_PREFIX +
              clusterName + CLUSTER_WAIT_FOR_MAPS, false);
      int minNodes = conf.getInt(BALANCER_CLUSTER_PREFIX + clusterName +
              CLUSTER_MIN_NODES, 1);
      String exclusiveTrackers = conf.get(BALANCER_CLUSTER_PREFIX +
              clusterName + CLUSTER_NOT_TO_MOVE_FILE);


      Cluster cluster = new Cluster(httpAddr, minLoad, maxLoad,
              waitForMaps, minNodes, ttLauncher, exclusiveTrackers);
      LOG.info("Created a cluster " + httpAddr);
      cluster.load();
      clusters.put(clusterName, cluster);
    }
  }

  public void initializeLauncher() {
    ttLauncher = new TTLauncher();

    launcherThread = new Thread(ttLauncher);
    launcherThread.start();
  }

  private void registerShutdownHook() {
    Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {

      public void run() {
        try {
          shutdown = true;
          ttLauncher.shutdown();
          launcherThread.join();
        } catch (InterruptedException iex) {
          
        }
      }
    }));
  }

  public int run(String[] args) throws Exception {
    int exitCode = 0;
    // Start the RPC server
    initializeServer();
    initializeLauncher();
    registerShutdownHook();
    initializeClusters();
    DEBUG = conf.getBoolean(DEBUG_MODE, false);
    if (DEBUG) {
      LOG.info("Runnning in DEBUG mode");
    }
    
    Map<Cluster, Integer> consumers = new HashMap<Cluster, Integer>();
    Map<Cluster, Integer> producers = new HashMap<Cluster, Integer>();

    // There are the clusters that can give and take at the same time
    Set<Cluster> specials = new HashSet<Cluster>();

    while (!shutdown) {
      int supply = 0, demand = 0;
      for (Map.Entry<String, Cluster> entry : clusters.entrySet()) {
        Cluster cluster = entry.getValue();
        cluster.poll();

        int spareMachines = cluster.countSpareMachines();
        supply += spareMachines;

        int needsMachines = cluster.countMachineShortage();
        demand += needsMachines;

        int launching = cluster.getWaitingFor();

        // We are waiting for some trackers to come alive -> need less
        needsMachines -= launching;

        if (spareMachines > 0) {
          producers.put(cluster, spareMachines);
        }
        if (needsMachines > 0) {
          consumers.put(cluster, needsMachines);
        }
        if (needsMachines == 1 && spareMachines == 1) {
          specials.add(cluster);
        }
      }

      if (supply > demand) {
        // There are free machines on the clusters and we do not need them all
        // Delete those special from the list of suppliers
        // until the supply == demand
        int diff = supply - demand;
        Iterator<Cluster> iter = specials.iterator();
        while (iter.hasNext()) {
          if (diff == 0) {
            break;
          }
          diff--;
          producers.remove(iter.next());
          iter.remove();
        }
      } else if (demand > supply) {
        int diff = demand - supply;
        Iterator<Cluster> iter = specials.iterator();
        while (iter.hasNext()) {
          if (diff == 0) {
            break;
          }
          diff--;
          consumers.remove(iter.next());
          iter.remove();
        }
      }

      for (Cluster special : specials) {
        // So we do not do useless moves
        producers.remove(special);
        consumers.remove(special);
      }
      // By now producers and consumers cannot share elements
      for (Map.Entry<Cluster, Integer> request : consumers.entrySet()) {
        int clusterDemand = request.getValue();
        Cluster cluster = request.getKey();
        for (Map.Entry<Cluster, Integer> resource :
                producers.entrySet()) {
          if (resource.getValue() > 0) {
            int machines2move = Math.min(clusterDemand, resource.getValue());
            moveMachines(resource.getKey(), cluster, machines2move);
            producers.put(resource.getKey(), resource.getValue() - machines2move);
            clusterDemand -= machines2move;
          }
          if (clusterDemand == 0) {
            break;
          }
        }
      }

      supply = 0;
      demand = 0;
      consumers.clear();
      producers.clear();
      try {
        Thread.sleep(POLL_WAIT_TIME);
      } catch (InterruptedException iex) {
        LOG.error("Sleep interrupted", iex);
      }
    }

    return exitCode;
  }

  public static String getStartCommand(String version) {
    if (version.contains("0.20")) {
      return START_TASKTRACKER_COMMAND_20;
    } else if (version.contains("0.21")) {
      return START_TASKTRACKER_COMMAND_21;
    } else {
      return START_TASKTRACKER_COMMAND_20;
    }
  }

  Set<Entry<String, Cluster>> getRegisteredClusters() {
    return clusters.entrySet();
  }

  public String getCurrentStatus(String clusterName) throws IOException {
    Cluster cluster = clusters.get(clusterName);

    if (cluster == null) {
      throw new IOException("Cluster " + clusterName + " does not exit " +
              "in this configuration");
    }

    return cluster.getStatus();
  }

  public int moveMachines(String from, String to, int numMachines)
          throws IOException {
    Cluster fromCluster = clusters.get(from);
    if (fromCluster == null) {
      throw new IOException("Cluster " + from + " does not exist " +
              "in this configuration");
    }
    Cluster toCluster = clusters.get(to);
    if (toCluster == null) {
      throw new IOException("Cluster " + to + " does not exist " +
              "in this configuration");
    }
    LOG.info("Moving" + numMachines + " machines from cluster " +
            from + " to cluster " + to);
    int launched = moveMachines(fromCluster, toCluster, numMachines);
    // 0 on success and number of machines lost on the way on failure
    return numMachines - launched;
  }

  public int rebalance() throws IOException {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  public long getProtocolVersion(String protocol, long clientVersion)
          throws IOException {
    if (protocol.equals(ClusterBalancerAdminProtocol.class.getName())) {
      return ClusterBalancerAdminProtocol.versionID;
    } else {
      throw new IOException("Unknown protocol for ClusterBalancerDaemon " +
              protocol);
    }
  }

  @Override
  public ProtocolSignature getProtocolSignature(String protocol,
      long clientVersion, int clientMethodsHash) throws IOException {
    return ProtocolSignature.getProtocolSignature(
        this, protocol, clientVersion, clientMethodsHash);
  }
  
  public Configuration getConf() {
    return conf;
  }

  public void setConf(Configuration conf) {
    this.conf = conf;
    Configuration.addDefaultResource(CLUSTER_BALANCER_CONF_FILE);
  }

  /**
   * @param args the command line arguments
   */
  public static void main(String[] args) throws Exception {
    ToolRunner.run(new DynamicCloudsDaemon(), args);
  }

  private int moveMachines(Cluster fromCluster,
          Cluster toCluster, int numMachines) throws IOException {
    if (DEBUG) {
      LOG.info("Move " + numMachines +
                " machines from " + fromCluster.getHostName() +
                " to " + toCluster.getHostName());
      return 0;
    } else {
      LOG.info("Moving " + numMachines + " from cluster " +
              fromCluster.getHostName() + " to cluster " +
              toCluster.getHostName());
      List<TaskTrackerLoadInfo> releasedTrackers =
              fromCluster.releaseTrackers(numMachines);
      LOG.info("Machines released " + releasedTrackers);
      List<TaskTrackerLoadInfo> addedTrackers =
              toCluster.addTrackers(releasedTrackers);
      LOG.info("Machines added " + addedTrackers);

      int launched = toCluster.launchTrackers(addedTrackers);
      LOG.info("Launched " + launched + " machines");
      return launched;
    }
  }
}
