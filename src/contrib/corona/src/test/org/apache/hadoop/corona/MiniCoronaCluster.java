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
package org.apache.hadoop.corona;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapred.CoronaJobTracker;
import org.apache.hadoop.mapred.CoronaTaskTracker;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.ProxyJobTracker;
import org.apache.hadoop.mapred.TaskTracker;
import org.apache.hadoop.metrics.ContextFactory;
import org.apache.hadoop.metrics.spi.NoEmitMetricsContext;
import org.apache.hadoop.net.DNSToSwitchMapping;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.net.NetworkTopology;
import org.apache.hadoop.net.StaticMapping;
import org.apache.hadoop.security.UnixUserGroupInformation;

/**
 * This class creates a single-process Map-Reduce cluster for junit testing.
 */
public class MiniCoronaCluster {
  private static final Log LOG = LogFactory.getLog(MiniCoronaCluster.class);

  private final JobConf conf;
  private ClusterManager clusterManager;
  private ClusterManagerServer clusterManagerServer;
  private int clusterManagerPort;
  private List<TaskTrackerRunner> taskTrackerList =
      new ArrayList<TaskTrackerRunner>();
  private List<Thread> taskTrackerThreadList = new ArrayList<Thread>();
  private String namenode;
  private UnixUserGroupInformation ugi;
  private ProxyJobTracker pjt;

  /**
   * Create MiniCoronaCluster with builder pattern.
   * Example:
   * MiniCoronaCluster cluster =
   *    new MiniCoronaCluster.numDir(3).hosts(hosts).build();
   */
  public static class Builder {
    private String namenode = "local";
    private JobConf conf = null;
    private int numTaskTrackers = 1;
    private int numDir = 1;
    private String[] racks = null;
    private String[] hosts = null;
    private UnixUserGroupInformation ugi = null;

    public Builder namenode(String val) {
      this.namenode = val;
      return this;
    }
    public Builder conf(JobConf val) {
      this.conf = val;
      return this;
    }
    public Builder numTaskTrackers(int val) {
      this.numTaskTrackers = val;
      return this;
    }
    public Builder numDir(int val) {
      this.numDir = val;
      return this;
    }
    public Builder racks(String[] val) {
      this.racks = val;
      return this;
    }
    public Builder hosts(String[] val) {
      this.hosts = val;
      return this;
    }
    public MiniCoronaCluster build() throws IOException {
      return new MiniCoronaCluster(this);
    }
  }

  private MiniCoronaCluster(Builder builder) throws IOException {
    ContextFactory.resetFactory();
    setNoEmitMetricsContext();
    if (builder.racks != null && builder.hosts != null) {
      if (builder.racks.length != builder.hosts.length) {
        throw new IllegalArgumentException(
            "The number of hosts and racks must be the same");
      }
    }
    this.conf = builder.conf != null ? builder.conf : new JobConf();
    this.namenode = builder.namenode;
    this.ugi = builder.ugi;
    this.conf.set(CoronaConf.CM_ADDRESS, "localhost:0");
    this.conf.set(CoronaConf.CPU_TO_RESOURCE_PARTITIONING, TstUtils.std_cpu_to_resource_partitioning);
    this.clusterManagerPort = startClusterManager(this.conf);
    configureJobConf(conf, builder.namenode, clusterManagerPort, builder.ugi);
    for (int i = 0; i < builder.numTaskTrackers; ++i) {
      String host = builder.hosts == null ?
          "host" + i + ".foo.com" : builder.hosts[i];
      String rack = builder.racks == null ?
          NetworkTopology.DEFAULT_RACK : builder.racks[i];
      startTaskTracker(host, rack, i, builder.numDir);
    }
    pjt = ProxyJobTracker.startProxyTracker(conf);
    waitTaskTrackers();
  }

  private void setNoEmitMetricsContext() throws IOException {
    ContextFactory factory = ContextFactory.getFactory();
    factory.setAttribute(ClusterManagerMetrics.CONTEXT_NAME + ".class",
        NoEmitMetricsContext.class.getName());
  }

  int startClusterManager(Configuration conf) throws IOException {
    clusterManager = new ClusterManager(conf);
    clusterManagerServer = new ClusterManagerServer(conf, clusterManager);
    clusterManagerServer.start();
    return clusterManagerServer.port;
  }

  public void startTaskTracker(String host, String rack, int idx, int numDir)
      throws IOException {
    if (rack != null) {
      StaticMapping.addNodeToRack(host, rack);
    }
    if (host != null) {
      NetUtils.addStaticResolution(host, "localhost");
    }
    TaskTrackerRunner taskTracker;
    taskTracker = new TaskTrackerRunner(idx, numDir, host, conf);
    addTaskTracker(taskTracker);
  }

  void addTaskTracker(TaskTrackerRunner taskTracker) throws IOException {
    Thread taskTrackerThread = new Thread(taskTracker);
    taskTrackerList.add(taskTracker);
    taskTrackerThreadList.add(taskTrackerThread);
    taskTrackerThread.start();
  }

  public ClusterManager getClusterManager() {
    return clusterManager;
  }

  public TaskTrackerRunner getTaskTrackerRunner(int i) {
    return taskTrackerList.get(i);
  }

  public JobConf createJobConf() {
    return createJobConf(new JobConf());
  }

  public JobConf createJobConf(JobConf conf) {
    if(conf == null) {
      conf = new JobConf();
    }
    configureJobConf(conf, namenode, clusterManagerPort, ugi);
    return conf;
  }

  static void configureJobConf(JobConf conf, String namenode,
      int clusterManagerPort, UnixUserGroupInformation ugi) {
    FileSystem.setDefaultUri(conf, namenode);
    conf.set(CoronaConf.CM_ADDRESS,
               "localhost:" + clusterManagerPort);
    conf.set("mapred.job.tracker", "corona");
    conf.set("mapred.job.tracker.http.address",
                        "127.0.0.1:0");
    conf.setClass("topology.node.switch.mapping.impl",
        StaticMapping.class, DNSToSwitchMapping.class);
    conf.set("mapred.job.tracker.class", CoronaJobTracker.class.getName());
    if (ugi != null) {
      conf.set("mapred.system.dir", "/mapred/system");
      UnixUserGroupInformation.saveToConf(conf,
          UnixUserGroupInformation.UGI_PROPERTY_NAME, ugi);
    }
    // for debugging have all task output sent to the test output
    JobClient.setTaskOutputFilter(conf, JobClient.TaskStatusFilter.ALL);
  }

  /**
   * An inner class to run the corona task tracker.
   */
  static class TaskTrackerRunner implements Runnable {
    volatile CoronaTaskTracker tt;
    int trackerId;
    // the localDirs for this taskTracker
    String[] localDirs;
    volatile boolean isInitialized = false;
    volatile boolean isDead = false;
    int numDir;

    TaskTrackerRunner(int trackerId, int numDir, String hostname,
        JobConf inputConf) throws IOException {
      this.trackerId = trackerId;
      this.numDir = numDir;
      localDirs = new String[numDir];
      JobConf conf = new JobConf(inputConf);
      if (hostname != null) {
        conf.set("slave.host.name", hostname);
      }
      conf.set("mapred.task.tracker.http.address", "0.0.0.0:0");
      conf.set("mapred.task.tracker.report.address", "localhost:0");
      File localDirBase =
        new File(conf.get("mapred.local.dir")).getAbsoluteFile();
      localDirBase.mkdirs();
      StringBuffer localPath = new StringBuffer();
      for(int i=0; i < numDir; ++i) {
        File ttDir = new File(localDirBase,
                              Integer.toString(trackerId) + "_" + 0);
        if (!ttDir.mkdirs()) {
          if (!ttDir.isDirectory()) {
            throw new IOException("Mkdirs failed to create " + ttDir);
          }
        }
        localDirs[i] = ttDir.toString();
        if (i != 0) {
          localPath.append(",");
        }
        localPath.append(localDirs[i]);
      }
      conf.set("mapred.local.dir", localPath.toString());
      LOG.info("mapred.local.dir is " +  localPath);
      try {
        tt = new CoronaTaskTracker(conf);
        isInitialized = true;
      } catch (Throwable e) {
        isDead = true;
        tt = null;
        LOG.error("task tracker " + trackerId + " crashed", e);
      }
    }

    public void run() {
      try {
        if (tt != null) {
          tt.run();
        }
      } catch (Throwable e) {
        isDead = true;
        tt = null;
        LOG.error("task tracker " + trackerId + " crashed", e);
      }
    }

    public String[] getLocalDirs(){
      return localDirs;
    }

    public TaskTracker getTaskTracker() {
      return tt;
    }

    public void shutdown() {
      if (tt != null) {
        try {
          tt.shutdown();
        } catch (Throwable e) {
          LOG.error("task tracker " + trackerId +
              " could not shut down", e);
        }
      }
    }
  }

  public void shutdown() {
    try {
      waitTaskTrackers();
      for (int idx = 0; idx < taskTrackerList.size(); idx++) {
        TaskTrackerRunner taskTracker = taskTrackerList.get(idx);
        Thread taskTrackerThread = taskTrackerThreadList.get(idx);
        taskTracker.shutdown();
        taskTrackerThread.interrupt();
        try {
          taskTrackerThread.join();
        } catch (InterruptedException ex) {
          LOG.error("Problem shutting down task tracker", ex);
        }
      }
    } finally {
      File configDir = new File("build", "minimr");
      File siteFile = new File(configDir, "mapred-site.xml");
      siteFile.delete();
    }
  }

  private void waitTaskTrackers() {
    for (TaskTrackerRunner runner : taskTrackerList) {
      while (!runner.isDead && (!runner.isInitialized || !runner.tt.isIdle())) {
        if (!runner.isInitialized) {
          LOG.info("Waiting for task tracker to start.");
        } else {
          LOG.info("Waiting for task tracker " + runner.tt.getName() +
                   " to be idle.");
        }
        try {
          Thread.sleep(1000);
        } catch (InterruptedException ie) {}
      }
    }
  }
}

