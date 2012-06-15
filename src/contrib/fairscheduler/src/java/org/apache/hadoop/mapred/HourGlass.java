/**
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

package org.apache.hadoop.mapred;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.mapred.protocal.FairSchedulerProtocol;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.StringUtils;


/**
 * Moves slots between two MapReduce clusters which runs TaskTrackers on the
 * same set of machines
 */
public class HourGlass implements Runnable {

  static {
    Configuration.addDefaultResource("hour-glass.xml");
  }

  public final static String SERVERS_KEY = "mapred.hourglass.fairscheduler.servers";
  public final static String WEIGHTS_KEY = "mapred.hourglass.fairscheduler.weights";
  public final static String MAX_MAP_KEY = "mapred.hourglass.map.tasks.maximum";
  public final static String MAX_REDUCE_KEY = "mapred.hourglass.reduce.tasks.maximum";
  public final static String CPU_MAP_KEY = "mapred.hourglass.cpus.to.maptasks";
  public final static String CPU_REDUCE_KEY = "mapred.hourglass.cpus.to.reducetasks";
  public final static String INTERVAL_KEY = "mapred.hourglass.update.interval";
  public final static String SHARE_THRESHOLD_KEY = "mapred.hourglass.share.threshold";

  // The if the share is lower than this threshold, the cluster gets 0 slots
  float shareThreshold = 0.01F;

  public static Log LOG = LogFactory.getLog(HourGlass.class);
  long updateInterval = 10000L;
  volatile boolean running = true;
  Configuration conf;
  Cluster clusters[] = new Cluster[2];

  // Stores the initial maximum slot limit loaded from the conf
  int defaultMaxMapSlots;
  int defaultMaxReduceSlots;

  // Stores the initial #CPU to maximum slot limit loaded from the conf
  Map<Integer, Integer> defaultCpuToMaxMapSlots = null;
  Map<Integer, Integer> defaultCpuToMaxReduceSlots = null;

  final static TaskType MAP_AND_REDUCE[] =
      new TaskType[] {TaskType.MAP, TaskType.REDUCE};

  public HourGlass(Configuration conf) throws IOException {
    this.conf = conf;
    defaultMaxMapSlots = conf.getInt(MAX_MAP_KEY, Integer.MAX_VALUE);
    defaultMaxReduceSlots = conf.getInt(MAX_REDUCE_KEY, Integer.MAX_VALUE);
    defaultCpuToMaxMapSlots = loadCpuToMaxSlots(conf, TaskType.MAP);
    defaultCpuToMaxReduceSlots = loadCpuToMaxSlots(conf, TaskType.REDUCE);
    shareThreshold = conf.getFloat(SHARE_THRESHOLD_KEY, shareThreshold);
    try {
      String config;
      config = conf.get(SERVERS_KEY);
      String addresses[] = config.replaceAll("\\s", "").split(",");
      config = conf.get(WEIGHTS_KEY);
      double weights[] = new double[2];
      String str[] = config.replaceAll("\\s", "").split(",");
      weights[0] = Double.parseDouble(str[0]);
      weights[1] = Double.parseDouble(str[1]);
      if (weights[0] < 0 || weights[1] < 0 ||
          (weights[0] == 0 && weights[1] == 0)) {
        throw new IOException();
      }
      clusters[0] = new Cluster(addresses[0], weights[0], conf);
      clusters[1] = new Cluster(addresses[1], weights[1], conf);
    } catch (Exception e) {
      String msg = "Must assign exactly two server addresses and " +
          "the corresponding positive weights in hour-glass.xml";
      LOG.error(msg);
      throw new IOException(msg);
    }
    updateInterval = conf.getLong(WEIGHTS_KEY, updateInterval);
  }

  public Map<Integer, Integer> loadCpuToMaxSlots(
      Configuration conf, TaskType type) {
    String config = type == TaskType.MAP ?
        conf.get(CPU_MAP_KEY) : conf.get(CPU_REDUCE_KEY);
    Map<Integer, Integer> defaultCpuToMaxSlots =
        new HashMap<Integer, Integer>();
    if (config != null) {
      for (String s : config.replaceAll("\\s", "").split(",")) {
        String pair[] = s.split(":");
        int cpus = Integer.parseInt(pair[0]);
        int tasks = Integer.parseInt(pair[1]);
        LOG.info(String.format(
            "Number of CPUs to tasks. %s CPU : %s %s", cpus, tasks, type));
        defaultCpuToMaxSlots.put(cpus, tasks);
      }
    }
    return defaultCpuToMaxSlots;
  }

  /**
   * Hold the states of one MapReduce cluster
   */
  static class Cluster {
    FairSchedulerProtocol client;
    Map<String, TaskTrackerStatus> taskTrackers =
        new HashMap<String, TaskTrackerStatus>();
    String address;
    double weight;               // Higher weight will get more share
    int runnableMaps;            // Runnable maps on the cluster
    int runnableReduces;         // Runnable reduces on the cluster
    double targetMapShare;       // The share of maps to achieve
    double targetReduceShare;    // The share of reduces to achieve

    Cluster(String address, double weight, Configuration conf)
        throws IOException {
      this.client = createClient(address, conf);
      this.weight = weight;
      this.address = address;
    }

    /**
     * Obtain the cluster information from RPC
     * @throws IOException
     */
    void updateClusterStatus() throws IOException {
      taskTrackers.clear();
      for (TaskTrackerStatus status : client.getTaskTrackerStatus()) {
        String host = NetUtils.normalizeHostName(status.getHost());
        taskTrackers.put(host, status);
      }
      runnableMaps = client.getRunnableTasks(TaskType.MAP);
      runnableReduces = client.getRunnableTasks(TaskType.REDUCE);
      LOG.info(String.format("Update cluster status. " +
          "cluster:%s runnableMaps:%s runnableReduces:%s",
          address, runnableMaps, runnableReduces));
    }

    /**
     * Set the maximum slot of a tasktracker
     * @param tracker The status of the tasktracker to set
     * @param type The type of the task to set
     * @param slots The number of slots to set
     * @throws IOException
     */
    void setFSMaxSlots(TaskTrackerStatus tracker, TaskType type, int slots)
        throws IOException {
      client.setFSMaxSlots(tracker.getTrackerName(), type, slots);
    }

    /**
     * Obtain the maximum slots of a tasktracker of one cluster
     * @param status The status of the tasktracker
     * @param type The type of the task
     * @return The number of slots of the type on the TT
     * @throws IOException
     */
    int getMaxSlots(TaskTrackerStatus status, TaskType type)
        throws IOException {
      return client.getMaxSlots(status, type);
    }
  }

  /**
   * Update the task share of the clusters
   * @param clusters Two clusters with tasktrackers shares same nodes
   */
  static private void updateShares(Cluster clusters[]) {
    assert(clusters.length == 2);
    if (clusters[0].runnableMaps == 0 &&
        clusters[0].runnableMaps == 0 &&
        clusters[1].runnableReduces == 0 &&
        clusters[1].runnableReduces == 0) {
      // Do nothing if both clusters are empty
      return;
    }
    // Update target task shares using runnable tasks and weight
    if (!(clusters[0].runnableMaps == 0 && clusters[1].runnableMaps == 0)) {
      clusters[0].targetMapShare =
        clusters[0].runnableMaps * clusters[0].weight /
        (clusters[0].runnableMaps * clusters[0].weight +
            clusters[1].runnableMaps * clusters[1].weight);
      clusters[1].targetMapShare = 1 - clusters[0].targetMapShare;
    }
    if (!(clusters[0].runnableReduces == 0 &&
          clusters[1].runnableReduces == 0)) {
      clusters[0].targetReduceShare =
        clusters[0].runnableReduces * clusters[0].weight /
        (clusters[0].runnableReduces * clusters[0].weight +
            clusters[1].runnableReduces * clusters[1].weight);
      clusters[1].targetReduceShare = 1 - clusters[0].targetReduceShare;
    }
    for (int i = 0; i < 2; ++i) {
      LOG.info(String.format("Update Shares. " +
          "cluster%s:%s runnableMaps:%s runnableReduces:%s " +
          "weight:%s targetMapShare:%s targetReduceShare:%s", i,
          clusters[i].address,
          clusters[i].weight,
          clusters[i].runnableMaps,
          clusters[i].runnableReduces,
          clusters[i].targetMapShare,
          clusters[i].targetReduceShare));
    }
  }

  /**
   * Keep moving slots between two clusters according to their runnable tasks.
   * These clusters are assumed to run tasktrackers on the same set of machines
   */
  @Override
  public void run() {
    long lastUpdate = -1L;
    // Start balancing the clusters
    while (running) {
      try {
        Thread.sleep(updateInterval / 10);
        long now = JobTracker.getClock().getTime();
        if (now - lastUpdate > updateInterval) {
          lastUpdate = now;
          doMoveSlots(clusters);
        }
      } catch (Exception e) {
        LOG.error("Exception while balancing cluster.", e);
      }
    }
  }

  /**
   * Move slots on each tasktracker between two clusters such that their share
   * of the slots meets the target share.
   * @param clusters Two clusters
   * @throws IOException
   */
  private void doMoveSlots(Cluster clusters[]) throws IOException {
    // Obtain the new status of the clusters
    clusters[0].updateClusterStatus();
    clusters[1].updateClusterStatus();

    // Compute the target shares of the clusters
    updateShares(clusters);

    TaskTrackerStatus taskTrackers[] = new TaskTrackerStatus[2];
    int currentTasks[] = new int[2];
    int maxTasks[] = new int[2];
    int occupiedTasks[] = new int[2];
    double targetShares[] = new double[2];

    Set<String> allTaskTrackers = new HashSet<String>();
    allTaskTrackers.addAll(clusters[0].taskTrackers.keySet());
    allTaskTrackers.addAll(clusters[1].taskTrackers.keySet());
    // Set the slots for each TaskTracker to achieve the target share
    for (String host : allTaskTrackers) {
      boolean inBothClusters = true;
      for (int i = 0; i < 2; ++i) {
        // Check if the host is in both clusters
        if (!clusters[i].taskTrackers.containsKey(host)) {
          inBothClusters = false;
          LOG.warn(String.format(
              "%s is in cluster%s:%s but not int cluster%s:%s",
              1 - i, clusters[1 - i].address,
              i, clusters[i].address));
          TaskTrackerStatus status = clusters[1 - i].taskTrackers.get(host);
          // If it is only in one cluster, this cluster gets all slots
          for (TaskType type : MAP_AND_REDUCE) {
            int totalSlots = getTotalSlots(status, type);
            int maxSlots = clusters[1 - i].getMaxSlots(status, type);
            if (maxSlots < totalSlots) {
              clusters[1 - i].setFSMaxSlots(status, type, totalSlots);
            }
          }
        }
      }
      if (!inBothClusters) {
        continue;
      }
      // Both the clusters have this host
      taskTrackers[0] = clusters[0].taskTrackers.get(host);
      taskTrackers[1] = clusters[1].taskTrackers.get(host);
      for (TaskType type : MAP_AND_REDUCE) {
        int totalSlots = getTotalSlots(taskTrackers[0], type);
        // Compute the free slots from occupiedTasks and maxTasks
        maxTasks[0] = clusters[0].getMaxSlots(taskTrackers[0], type);
        maxTasks[1] = clusters[1].getMaxSlots(taskTrackers[1], type);
        if (type == TaskType.MAP) {
          occupiedTasks[0] = taskTrackers[0].countOccupiedMapSlots();
          occupiedTasks[1] = taskTrackers[1].countOccupiedMapSlots();
          targetShares[0] = clusters[0].targetMapShare;
          targetShares[1] = clusters[1].targetMapShare;
        } else {
          occupiedTasks[0] = taskTrackers[0].countOccupiedReduceSlots();
          occupiedTasks[1] = taskTrackers[1].countOccupiedReduceSlots();
          targetShares[0] = clusters[0].targetReduceShare;
          targetShares[1] = clusters[1].targetReduceShare;
        }
        currentTasks[0] = Math.max(occupiedTasks[0], maxTasks[0]);
        currentTasks[1] = Math.max(occupiedTasks[1], maxTasks[1]);
        int freeSlots = totalSlots - currentTasks[0] - currentTasks[1];
        // Determine where the slots should flow
        int dst = (maxTasks[0] / (0.01 + targetShares[0]) <
                   maxTasks[1] / (0.01 + targetShares[1])) ? 0 : 1;
        int src = 1 - dst;
        // If there are free slots, give them to the destination cluster
        if (freeSlots > 0) {
          clusters[dst].setFSMaxSlots(
              taskTrackers[dst], type, maxTasks[dst] + freeSlots);
          LOG.info(String.format("Increase %s %s for cluster%s on %s. " +
              "maxTasks%s:%s maxTasks%s:%s " +
              "occupiedTasks%s:%s occupiedTasks%s:%s",
              freeSlots, type, dst, host,
              dst, maxTasks[dst],
              src, maxTasks[src],
              dst, occupiedTasks[dst],
              src, occupiedTasks[src]));
        }
        // Release more slots from source cluster if necessary
        int targetSlots = targetShares[src] > shareThreshold ?
            (int)Math.ceil(totalSlots * targetShares[src]) : 0;
        if (maxTasks[src] > targetSlots) {
          clusters[src].setFSMaxSlots(taskTrackers[src], type, targetSlots);
          LOG.info(String.format("Release %s %s for cluster%s on %s. " +
              "maxTasks%s:%s maxTasks%s:%s " +
              "occupiedTasks%s:%s occupiedTasks%s:%s",
              maxTasks[src] - targetSlots, type, src, host,
              src, maxTasks[src],
              dst, maxTasks[dst],
              src, occupiedTasks[src],
              dst, occupiedTasks[dst]));
        }
      }
    }
  }

  /**
   * Stop the loop that balancing the cluster
   */
  public void stop() {
    running = false;
  }

  /**
   * Obtain the two clusters combined total slots of a tasktracker
   */
  private int getTotalSlots(
      TaskTrackerStatus status, TaskType type) {
    Map<Integer, Integer> defaultCpuToMaxSlots = (type == TaskType.MAP) ?
        defaultCpuToMaxMapSlots : defaultCpuToMaxReduceSlots;
    int cpus = status.getResourceStatus().getNumProcessors();
    Integer slots = defaultCpuToMaxSlots.get(cpus);
    if (slots == null) {
      slots = (type == TaskType.MAP) ?
          defaultMaxMapSlots : defaultMaxReduceSlots;
    }
    int taskTrackerSlots = (type == TaskType.MAP) ? status.getMaxMapSlots() :
        status.getMaxReduceSlots();
    return Math.min(slots, taskTrackerSlots);
  }

  /**
   * Create a FariScheduler RPC client
   * @param target The host:port of the RPC server
   * @param conf The configuration
   * @return The FairScheduler client
   * @throws IOException
   */
  private static FairSchedulerProtocol createClient(
      String target, Configuration conf) throws IOException {
    InetSocketAddress addr = NetUtils.createSocketAddr(target);
    UserGroupInformation ugi = UserGroupInformation.getCurrentUGI();
    LOG.info("Connecting to " + addr);
    return (FairSchedulerProtocol) RPC.getProxy(FairSchedulerProtocol.class,
        FairSchedulerProtocol.versionID, addr, ugi, conf,
        NetUtils.getSocketFactory(conf, FairSchedulerProtocol.class));
  }

  /**
   * Start the HourGlass process
   */
  public static void main(String argv[]) {
    StringUtils.startupShutdownMessage(HourGlass.class, argv, LOG);
    try {
      HourGlass hourGlass = new HourGlass(new Configuration());
      hourGlass.run();
    } catch (Throwable e) {
      LOG.fatal(StringUtils.stringifyException(e));
      System.exit(-1);
    }
  }
}
