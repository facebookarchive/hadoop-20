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
import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.ProtocolSignature;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.security.UnixUserGroupInformation;
import org.apache.hadoop.util.Daemon;
import org.apache.hadoop.util.StringUtils;

/**
 * Cache all the resource utilization information of the {@link UtilizationCollector}
 * locally. This allows us to obtain the information instantly without waiting
 * the response from Collector via RPC call.
 */
public class UtilizationCollectorCached implements UtilizationCollectorProtocol {
  UtilizationCollectorProtocol rpcCollector = null;
  Daemon mirrorDaemon = null;
  volatile boolean running = true;
  private Long RECONNECT_PERIOD = 10000L;
  Configuration conf = null;

  public static final Log LOG =
          LogFactory.getLog("org.apache.hadoop.mapred.resourceutilization");

  // Contains the resource utilization of the cluster
  protected ClusterUtilization clusterUtil = new ClusterUtilization();

  // Contains all the job utilization on the cluster
  protected ConcurrentMap<String, JobUtilization> allJobUtil =
          new ConcurrentHashMap<String, JobUtilization>();

  // Contains all the tasktracker utilization
  protected ConcurrentMap<String, TaskTrackerUtilization> allTaskTrackerUtil =
          new ConcurrentHashMap<String, TaskTrackerUtilization>();

  // How often do we mirror the information from Collector
  Long mirrorPeriod;
  Long DEFAULT_MIRROR_PERIOD = 3000L; // 3 sec

  // Make this object a singleton
  private static UtilizationCollectorCached instance = null;
  public static UtilizationCollectorCached getInstance(Configuration conf) {
    if (instance == null) {
      synchronized (UtilizationCollectorCached.class) {
        if (instance == null) {
          instance = new UtilizationCollectorCached(conf);
        }
      }
    }
    return instance;
  }

  private UtilizationCollectorCached(Configuration conf) {
    conf.addResource("resourceutilization.xml");
    conf.addResource("mapred-site.xml");
    this.conf = conf;
    initialize();
  }

  protected void initialize() {
    // How often do we mirror the information from Collector
    mirrorPeriod = conf.getLong("mapred.resourceutilization.mirrorperiod",
            DEFAULT_MIRROR_PERIOD);
    // Make connection to the Collector
    connect();
    mirrorDaemon = new Daemon(new MirrorRun());
    mirrorDaemon.start();
  }

  // Make connection to the Collector
  protected void connect() {
    LOG.info("Connecting to collector...");
    try {
      conf.setStrings(UnixUserGroupInformation.UGI_PROPERTY_NAME,
                      new String[]{"hadoop", "hadoop"});
      rpcCollector =
            (UtilizationCollectorProtocol) RPC.getProxy(UtilizationCollectorProtocol.class,
                                             UtilizationCollectorProtocol.versionID,
                                             UtilizationCollector.getAddress(conf),
                                             conf);
    } catch (IOException e) {
      LOG.error("Cannot connect to UtilizationCollector server. Retry in " +
               DEFAULT_MIRROR_PERIOD + " milliseconds.");
      return;
    }
    LOG.info("Connection established");
  }

  /**
   * Stop the the daemon which keep getting data from {@link Collection}
   */
  public void terminate() {
    running = false;
    mirrorDaemon.stop();
  }

  /**
   * Periodically getting data from {@link Collection}
   */
  class MirrorRun implements Runnable {
    /**
     */
    @Override
    public void run() {
      while (running) {
        try {
          fetchData();
          Thread.sleep(mirrorPeriod);
        } catch (Exception e) {
          LOG.warn(StringUtils.stringifyException(e));
        }
      }
    }
  }

  /**
   * Mirror data from the Collector
   */
  protected void fetchData() throws IOException {
    // if not connected to the collector, wait for a while then try connecting
    if (rpcCollector == null) {
      try {
        Thread.sleep(RECONNECT_PERIOD);
      } catch (InterruptedException e) {
        // do nothing
      }
      connect();
    }
    try {
      clusterUtil = rpcCollector.getClusterUtilization();
      for (JobUtilization job:
        rpcCollector.getAllRunningJobUtilization()) {
        allJobUtil.put(job.getJobId(), job);
      }
      for (TaskTrackerUtilization tt :
        rpcCollector.getAllTaskTrackerUtilization()) {
        allTaskTrackerUtil.put(tt.getHostName(), tt);
      }
    } catch (Exception e) {
        // When the Collector is down, clear the old data
        clusterUtil = null;
        allJobUtil.clear();
        allTaskTrackerUtil.clear();
        LOG.warn("Error obtaining data from Collector.");
    }
  }

  /**
   * Implement CollectorProtocol methods
   */

  @Override
  public TaskTrackerUtilization getTaskTrackerUtilization(String hostName)
          throws IOException {
    return allTaskTrackerUtil.get(hostName);
  }


  @Override
  public JobUtilization getJobUtilization(String jobId) throws IOException {
    return allJobUtil.get(jobId);
  }

  @Override
  public TaskTrackerUtilization[] getAllTaskTrackerUtilization()
          throws IOException {
    Collection<TaskTrackerUtilization> c = allTaskTrackerUtil.values();
    return 
      (TaskTrackerUtilization[])c.toArray(new TaskTrackerUtilization[c.size()]);
  }

  @Override
  public JobUtilization[] getAllRunningJobUtilization()
          throws IOException {
    Collection<JobUtilization> c = allJobUtil.values();
    return (JobUtilization[])c.toArray(new JobUtilization[c.size()]);
  }

  @Override
  public ClusterUtilization getClusterUtilization() throws IOException {
    return clusterUtil;
  }

  @Override
  public void reportTaskTrackerUtilization(
          TaskTrackerUtilization ttUtil, LocalJobUtilization[] localJobUtil)
                                    throws IOException {
    // This method is not supported
    throw new IOException();
  }

  @Override
  public long getProtocolVersion(String protocol,
                                 long clientVersion) throws IOException {
    if (protocol.equals(UtilizationCollectorProtocol.class.getName())) {
      return UtilizationCollectorProtocol.versionID;
    } else {
      throw new IOException("Unknown protocol to name node: " + protocol);
    }
  }

  @Override
  public ProtocolSignature getProtocolSignature(String protocol,
      long clientVersion, int clientMethodsHash) throws IOException {
    return ProtocolSignature.getProtocolSignature(
        this, protocol, clientVersion, clientMethodsHash);
  }
}
