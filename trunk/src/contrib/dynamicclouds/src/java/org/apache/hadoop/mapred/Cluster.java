package org.apache.hadoop.mapred;

import java.io.File;
import java.io.IOException;
import java.io.BufferedReader;
import java.io.FileReader;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.HashSet;
import org.apache.hadoop.util.Shell.ShellCommandExecutor;

public class Cluster {

  private String hostName;
  private String httpAddress;
  private String version;
  private String hadoopHome;
  private String slavesFile;
  // Whether we can move a node anytime or should wait for all maps to finish
  private boolean waitForMaps;
  private List<TaskTrackerLoadInfo> trackers =
          new ArrayList<TaskTrackerLoadInfo>();
  private Set<String> exclusiveTrackers = new HashSet<String>();
  private int maxLoad;
  private int minLoad;
  private int minNodes;
  // Switches to false the first time we initialize full buffer
  private boolean startup = true;
  private int loadHistoryPos = 0;
  private int[] loadHistory = new int[10];
  TTLauncher ttLauncher;

  public String getHostName() {
    return hostName;
  }

  public String getHttpAddress() {
    return httpAddress;
  }

  public int getMaxLoad() {
    return maxLoad;
  }

  public int getMinLoad() {
    return minLoad;
  }

  public List<TaskTrackerLoadInfo> getTrackers() {
    return trackers;
  }

  public String getVersion() {
    return version;
  }

  public void insertLoad(int load) {
    DynamicCloudsDaemon.LOG.info("Got the new load value for " + hostName + " : " + load);
    loadHistory[loadHistoryPos++] = load;
    // The first time we pass the loadHistory.size - startup is over
    if (startup && loadHistory.length == loadHistoryPos) {
      DynamicCloudsDaemon.LOG.info("Exiting startup mode");
    }
    startup = startup && loadHistory.length != loadHistoryPos;
    loadHistoryPos = loadHistoryPos % loadHistory.length;
  }

  public int getAverageLoad() {
    int total = 0;
    for (int i = 0; i < loadHistory.length; i++) {
      total += loadHistory[i];
    }
    return total / loadHistory.length;
  }

  public Cluster(String httpAddress, int minLoad, int maxLoad,
                  boolean waitForMaps, int minNodes, TTLauncher ttLauncher,
                  String exclusiveTrackers)
                  throws MalformedURLException, IOException {
    super();
    if (minLoad < 0 || minLoad > 100 || maxLoad < 0 || maxLoad > 100) {
      DynamicCloudsDaemon.LOG.error("min load and max load of the cluster " + "have to be integer values from 0 to 100");
      throw new IOException("Failed to initialize cluster " + httpAddress);
    }
    if (minLoad >= maxLoad && maxLoad != 100) {
      DynamicCloudsDaemon.LOG.error("min load of the cluster has to be smaller than the max " + "load of the cluster. The only exception is when they are " + "both 100% that means the cluster can always give " + "machines and only takes machines of noone else needs them");
      throw new IOException("Failed to initialize cluster " + httpAddress);
    }
    URL uri = new URL(httpAddress);
    hostName = uri.getHost();
    if (exclusiveTrackers != null) {
      File file = new File(exclusiveTrackers);
      if (file.exists()) {
        BufferedReader reader =
          new BufferedReader(new FileReader(file));
        String hostName = null;
        while ((hostName = reader.readLine()) != null) {
          this.exclusiveTrackers.add(hostName);
        }
      }
    }
    this.httpAddress = httpAddress;
    this.waitForMaps = waitForMaps;
    this.minLoad = minLoad;
    this.maxLoad = maxLoad;
    this.minNodes = minNodes;
    this.ttLauncher = ttLauncher;
  }

  public void load() throws IOException {
    DynamicCloudsDaemon.LOG.info("Loading initial cluster info");
    Map<String, String> conf = ClusterStatusJSONParser.getClusterConf(httpAddress);
    version = conf.get("version");
    slavesFile = conf.get("slaves.file");
    File file = new File(slavesFile);
    // HADOOP_HOME/conf/slaves parent of parent is hadoop home
    hadoopHome = file.getParentFile().getParent();
  }

  public synchronized void poll() {
    try {
      trackers = ClusterStatusJSONParser.getJobTrackerStatus(httpAddress);
      insertLoad(getCurrentClusterLoad());
    } catch (IOException ex) {
      // The tracker went down - reset the cluster status
      startup = true;
      Arrays.fill(loadHistory, 0, loadHistory.length, 100);
      loadHistoryPos = 0;
      DynamicCloudsDaemon.LOG.error("Error updating cluster " +
              this.getHostName());
    }
  }

  public int getWaitingFor() {
    return ttLauncher.getTasksInQueue(hostName);
  }

  public int countMachineShortage() {
    // It is starting up - too early to tell
    if (startup) {
      return 0;
    }
    if (trackers.size() == 0) {
      // Something bad happened, but the cluster probably needs nodes
      return 1;
    }
    // The cluster is operating at normal load
    if (getAverageLoad() < maxLoad) {
      return 0;
    }
    float deficit = getAverageLoad() / (float) maxLoad - 1;
    if (getAverageLoad() == 100 && maxLoad == 100) {
      // Special case when the cluster is configured to operate
      // fully loaded, but if there are free nodes it can use them
      return 1;
    }
    int machines = (int) Math.ceil(deficit * trackers.size());
    return machines;
  }

  public int countSpareMachines() {
    // The cluster is only starting up
    if (startup) {
      return 0;
    }
    if (waitForMaps && countIdleNodes() == 0) {
      return 0;
    }
    if (trackers.size() - minNodes <= 0) {
      // The cluster is at it's smallest allowed size
      return 0;
    }
    // The cluster is busy and cannot spare nodes
    if (getAverageLoad() > minLoad) {
      return 0;
    }
    float change = 1 - getAverageLoad() / (float) minLoad;
    int spareMachines = (int) Math.ceil(change * trackers.size());
    if (getAverageLoad() == 100 && minLoad == 100 && trackers.size() > 1) {
      // This is a special case: the cluster is fully loaded,
      // but it can still spare nodes since it is OK if it runs at 100% load
      return 1;
    }
    // one machine has to stay.
    // It should really be configurable for each cluster
    return Math.min(spareMachines, trackers.size() - minNodes);
  }

  public int countIdleNodes() {
    int idles = 0;
    for (TaskTrackerLoadInfo ttli : trackers) {
      if (ttli.getTotalMapTasks() == 0) {
        idles++;
      }
    }
    return idles;
  }

  public synchronized int getCurrentClusterLoad() {
    int maps = 0;
    int maxMaps = 0;
    int reduces = 0;
    int maxReduces = 0;
    for (TaskTrackerLoadInfo tracker : trackers) {
      maps += tracker.getRunningMapTasks();
      maxMaps += tracker.getMaxMapTasks();
      reduces += tracker.getRunningReduceTasks();
      maxReduces += tracker.getMaxReduceTasks();
    }
    if (maxMaps == 0 || maxReduces == 0) {
      // The cluster has no trackers. Load is 100%
      // it should get some machines back asap
      return 100;
    }
    int mapLoad = (maps * 100) / maxMaps;
    int reduceLoad = (reduces * 100) / maxReduces;
    return Math.max(mapLoad, reduceLoad);
  }

  public List<TaskTrackerLoadInfo> releaseTrackers(int numTrackers)
          throws IOException {
    List<TaskTrackerLoadInfo> releasedTrackers =
            new ArrayList<TaskTrackerLoadInfo>();
    TaskTrackerLoadInfoIterator iterator = new WastedTimeTTLIIterator();
    iterator.setTrackers(trackers);
    while (releasedTrackers.size() < numTrackers && iterator.hasNext()) {
      TaskTrackerLoadInfo tracker = iterator.next();
      String host = tracker.getTaskTrackerHost();
      if (trackers.contains(host)) {
        continue;
      }
      ShellCommandExecutor removeHostCommand = new ShellCommandExecutor(
              new String[]{"ssh", hostName,
                "cd " + hadoopHome + " && " + "bin/hadoop " +
                TTMover.class.getCanonicalName() + " -remove " + host});
      try {
        removeHostCommand.execute();
        releasedTrackers.add(tracker);
      } catch (IOException ex) {
        DynamicCloudsDaemon.LOG.error("Error removing tracker " +
                tracker.getTaskTrackerName(), ex);
      }
    }
    return releasedTrackers;
  }

  public List<TaskTrackerLoadInfo> addTrackers(List<TaskTrackerLoadInfo> trackers) {
    List<TaskTrackerLoadInfo> trackersAdded = new ArrayList<TaskTrackerLoadInfo>();
    for (TaskTrackerLoadInfo tracker : trackers) {
      String host = tracker.getTaskTrackerHost();
      ShellCommandExecutor addHostCommand = 
          new ShellCommandExecutor(
          new String[]{"ssh", hostName,
          "cd " + hadoopHome + " && " + "bin/hadoop " +
          TTMover.class.getCanonicalName() +
          " -add " + host});
      try {
        addHostCommand.execute();
        trackersAdded.add(tracker);
      } catch (IOException ex) {
        DynamicCloudsDaemon.LOG.error("Error adding tracker " + tracker.getTaskTrackerName(), ex);
      }
    }
    return trackersAdded;
  }

  public int launchTrackers(List<TaskTrackerLoadInfo> trackers) {
    int trackersLaunched = 0;
    for (TaskTrackerLoadInfo tracker : trackers) {
      String host = tracker.getTaskTrackerHost();
      String startCommand = DynamicCloudsDaemon.getStartCommand(version);
      ShellCommandExecutor startTTCommand = 
      new ShellCommandExecutor(
      new String[]{"ssh", host, 
      "cd " + hadoopHome + " && " + startCommand});
      TTLaunchTask task = new TTLaunchTask(startTTCommand, this.hostName);
      ttLauncher.addTTForLaunch(task);
    }
    return trackersLaunched;
  }

  @Override
  public String toString() {
    StringBuilder buffer = new StringBuilder();
    buffer.append("Host :");
    buffer.append(this.hostName);
    buffer.append(" HADOOP_HOME:");
    buffer.append(this.hadoopHome);
    buffer.append("\n");
    return buffer.toString();
  }

  public synchronized String getStatus() {
    int totalMapSlots = 0;
    int totalReduceSlots = 0;
    int runningMaps = 0;
    int runningReduces = 0;
    int allMaps = 0;
    for (TaskTrackerLoadInfo tracker : trackers) {
      runningMaps += tracker.getRunningMapTasks();
      runningReduces += tracker.getRunningReduceTasks();
      allMaps += tracker.getTotalMapTasks();
      totalMapSlots += tracker.getMaxMapTasks();
      totalReduceSlots += tracker.getMaxReduceTasks();
    }
    StringBuilder buffer = new StringBuilder();
    buffer.append("Map Load: ");
    buffer.append(runningMaps).append("/").append(totalMapSlots).append("\n");
    buffer.append("Reduce Load: ");
    buffer.append(runningReduces).append("/").append(totalReduceSlots).append("\n");
    buffer.append("Map tasks finished on the cluster: ").append(allMaps - runningMaps).append("\n");
    return buffer.toString();
  }
}
