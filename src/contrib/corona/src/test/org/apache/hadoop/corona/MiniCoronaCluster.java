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
import java.net.InetAddress;
import java.net.UnknownHostException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.http.NettyMapOutputHttpServer;
import org.apache.hadoop.mapred.CoronaJobTracker;
import org.apache.hadoop.mapred.CoronaTaskTracker;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.ProxyJobTracker;
import org.apache.hadoop.metrics.ContextFactory;
import org.apache.hadoop.metrics.spi.NoEmitMetricsContext;
import org.apache.hadoop.net.DNSToSwitchMapping;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.net.NetworkTopology;
import org.apache.hadoop.net.StaticMapping;
import org.apache.hadoop.security.UnixUserGroupInformation;
import org.apache.hadoop.util.CoronaFailureEventInjector;

/**
 * This class creates a single-process Map-Reduce cluster for junit testing.
 */
public class MiniCoronaCluster {
  private static final Log LOG = LogFactory.getLog(MiniCoronaCluster.class);

  private final JobConf conf;
  private ClusterManager clusterManager;
  private ClusterManagerServer clusterManagerServer;
  private final int clusterManagerPort;
  private final int proxyJobTrackerPort;
  private final List<TaskTrackerRunner> taskTrackerList =
      new ArrayList<TaskTrackerRunner>();
  private final List<Thread> taskTrackerThreadList = new ArrayList<Thread>();
  private final String namenode;
  private final UnixUserGroupInformation ugi;
  private final ProxyJobTracker pjt;
  private CoronaFailureEventInjector rjtFailureInjector;

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
    private final UnixUserGroupInformation ugi = null;
    // for failure emulator to remote job tracker
    private CoronaFailureEventInjector rjtFailureInjector = null;

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
    public Builder rjtFailureInjector(CoronaFailureEventInjector rjtFailureInjector) {
      this.rjtFailureInjector = rjtFailureInjector;
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
    this.conf.set(CoronaConf.PROXY_JOB_TRACKER_ADDRESS, "localhost:0");
    // if there is any problem with dependencies, please implement
    // getFreePort() in MiniCoronaCluster
    this.conf.set(CoronaConf.PROXY_JOB_TRACKER_THRIFT_ADDRESS, "localhost:"
        + MiniDFSCluster.getFreePort());
    // Because we change to get system dir from ProxyJobTracker,
    // we need to tell the proxy job tracker which sysFs to use
    CoronaConf pjtConf = new CoronaConf(conf);
    FileSystem.setDefaultUri(pjtConf, namenode);
    pjt = ProxyJobTracker.startProxyTracker(pjtConf);
    this.proxyJobTrackerPort = pjt.getRpcPort();
    configureJobConf(conf, builder.namenode, clusterManagerPort,
      proxyJobTrackerPort, builder.ugi, null);
    this.rjtFailureInjector = builder.rjtFailureInjector;
    for (int i = 0; i < builder.numTaskTrackers; ++i) {
      String host = builder.hosts == null ?
          "host" + i + ".foo.com" : builder.hosts[i];
      String rack = builder.racks == null ?
          NetworkTopology.DEFAULT_RACK : builder.racks[i];
      startTaskTracker(host, rack, i, builder.numDir);
    }
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
      try {
        InetAddress addr = InetAddress.getByName(host);
        NetUtils.addStaticResolution(addr.getHostAddress(),"localhost");
      } catch (UnknownHostException e) {
      }
    }
    TaskTrackerRunner taskTracker;
    taskTracker = new TaskTrackerRunner(idx, numDir, host, conf, rjtFailureInjector);
    addTaskTracker(taskTracker);
  }

  void addTaskTracker(TaskTrackerRunner taskTracker) throws IOException {
    Thread taskTrackerThread = new Thread(taskTracker);
    taskTrackerList.add(taskTracker);
    taskTrackerThreadList.add(taskTrackerThread);
    taskTrackerThread.start();
  }

  public int getClusterManagerPort() {
    return clusterManagerPort;
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
    configureJobConf(conf, namenode, clusterManagerPort, proxyJobTrackerPort,
      ugi, this.conf.get(CoronaConf.PROXY_JOB_TRACKER_THRIFT_ADDRESS));
    return conf;
  }

  static void configureJobConf(JobConf conf, String namenode,
      int clusterManagerPort, int proxyJobTrackerPort,
      UnixUserGroupInformation ugi,String proxyJobTrackerThriftAddr) {
    FileSystem.setDefaultUri(conf, namenode);
    conf.set(CoronaConf.CM_ADDRESS,
               "localhost:" + clusterManagerPort);
    conf.set(CoronaConf.PROXY_JOB_TRACKER_ADDRESS,
      "localhost:" + proxyJobTrackerPort);
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
    // We change the logic to get SystemDir by calling thrift API from proxyJobTracker
    // So we need to set this value for job client
    if (proxyJobTrackerThriftAddr != null) {
      conf.set(CoronaConf.PROXY_JOB_TRACKER_THRIFT_ADDRESS, proxyJobTrackerThriftAddr);
    }
    // for debugging have all task output sent to the test output
    JobClient.setTaskOutputFilter(conf, JobClient.TaskStatusFilter.ALL);
  }

  /**
   * An inner class to run the corona task tracker.
   */
  public static class TaskTrackerRunner implements Runnable {
    volatile CoronaTaskTracker tt;
    int trackerId;
    // the localDirs for this taskTracker
    String[] localDirs;
    volatile boolean isInitialized = false;
    volatile boolean isDead = false;
    int numDir;

    TaskTrackerRunner(int trackerId, int numDir, String hostname,
        JobConf inputConf, CoronaFailureEventInjector rjtFailureEventInjector) throws IOException {
      this.trackerId = trackerId;
      this.numDir = numDir;
      localDirs = new String[numDir];
      JobConf conf = new JobConf(inputConf);
      if (hostname != null) {
        conf.set("slave.host.name", hostname);
      }
      conf.set("mapred.task.tracker.http.address", "0.0.0.0:0");
      conf.set("mapred.task.tracker.report.address", "localhost:0");
      conf.setInt(NettyMapOutputHttpServer.MAXIMUM_THREAD_POOL_SIZE, 10);
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
        tt.setJTFailureEventInjector(rjtFailureEventInjector);
        isInitialized = true;
      } catch (Throwable e) {
        isDead = true;
        tt = null;
        LOG.error("task tracker " + trackerId + " crashed", e);
      }
    }

    @Override
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

    public CoronaTaskTracker getTaskTracker() {
      return tt;
    }

    public void shutdown() {
      if (tt != null) {
        try {
          tt.shutdown();
          tt.forceCleanTaskDir();
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
    try {
      pjt.shutdown();
      pjt.join();
    } catch (Exception e) {
      LOG.error("Error during PJT shutdown", e);
    }
    try {
      clusterManagerServer.stopRunning();
      clusterManagerServer.join();
    } catch (Exception e) {
      LOG.error("Error during CM shutdown", e);
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

