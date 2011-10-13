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
import java.util.Date;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.ProtocolSignature;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RPC.Server;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.util.Daemon;
import org.apache.hadoop.util.StringUtils;


/**
 * Collects and aggregates the resource utilization information transmitted
 * from TaskTrackers via {@link UtilizationReporter}.
 */
public class UtilizationCollector implements UtilizationCollectorProtocol,
  Configurable {

  public static final Log LOG =
          LogFactory.getLog(UtilizationCollectorProtocol.class);

  // Contains the resource utilization of the cluster
  protected ClusterUtilization clusterUtil = new ClusterUtilization();

  // Contains all the job utilization on the cluster
  protected ConcurrentMap<String, JobUtilization> allJobUtil =
          new ConcurrentHashMap<String, JobUtilization>();

  // Contains the recent reports from all TaskTrackers
  protected ConcurrentMap<String, UtilizationReport> taskTrackerReports =
          new ConcurrentHashMap<String, UtilizationReport>();


  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
    this.conf.addResource("mapred-site.xml");
    this.conf.addResource("mapred-site-custom.xml");
  }

  @Override
  public Configuration getConf() {
    return conf;
  }

  protected class UtilizationReport {
    private Long receivedTime;
    private TaskTrackerUtilization ttUtil;
    private LocalJobUtilization[] jobUtil;

    private long now() {
      return (new Date()).getTime();
    }
    /**
     * Set ttUtil and jobUtil. receivedTime will be set to now
     * @param ttUtil
     * @param jobUtil
     */
    public void setValues(TaskTrackerUtilization ttUtil,
                          LocalJobUtilization[] jobUtil) {
      receivedTime = now();
      this.ttUtil = ttUtil;
      this.jobUtil = jobUtil;
    }

    /**
     * @return the ttUtil
     */
    public TaskTrackerUtilization getTaskTrackerUtilization() {
      if (isExpired()) {
        return null;
      }
      return ttUtil;
    }

    /**
     * @return the localJobUtil
     */
    public LocalJobUtilization[] getJobUtilization() {
      if (isExpired()) {
        return null;
      }
      return jobUtil;
    }
    /**
     * @return Is this report expired?
     */
    public boolean isExpired() {
      if (now() > receivedTime + timeLimit) {
        return true;
      }
      return false;
    }
  }

  // Expired time of the report.
  protected long timeLimit; // millisecond
  static final long DEFAULT_TIME_LIMIT = 10000L; // 10 sec
  // How long do we consider a job is finished after it stops reporting
  protected long stopTimeLimit;
  static final long DEFAULT_STOP_TIME_LIMIT = 300 * 1000L; // 5 min
  // How often do we aggregate the reports
  protected long aggregatePeriod; // millisecond
  static final long DEFAULT_AGGREGATE_SLEEP_TIME = 1000L; // 1 sec;

  /** RPC server */
  private Server server;
  /** RPC server address */
  private InetSocketAddress serverAddress = null;
  /** hadoop configuration */
  private Configuration conf;
  
  protected volatile boolean running = true; // Are we running?

  /** Deamon thread to trigger policies */
  Daemon aggregateDaemon = null;

  /**
   *  Create a non-initialized Collector. This is used for test only
   */
  public UtilizationCollector() {
    // do nothing
  }

  /**
   * Start Collector.
   * @param conf  configuration
   * @throws IOException
   */
  public UtilizationCollector(Configuration conf) throws IOException {
    setConf(conf);
    try {
      initialize(this.conf);
    } catch (IOException e) {
      this.stop();
      throw e;
    } catch (Exception e) {
      this.stop();
      throw new IOException(e);
    }
  }

  protected void initialize(Configuration conf)
    throws IOException {
    InetSocketAddress socAddr = UtilizationCollector.getAddress(conf);
    int handlerCount = conf.getInt(
            "mapred.resourceutilization.handler.count", 10);

    // create rpc server
    this.server = RPC.getServer(this, socAddr.getHostName(), socAddr.getPort(),
                                handlerCount, false, conf);

    // The rpc-server port can be ephemeral... ensure we have the correct info
    this.serverAddress = this.server.getListenerAddress();
    LOG.info("Collector up at: " + this.serverAddress);

    // start RPC server
    this.server.start();

    // How long does the TaskTracker reports expire
    timeLimit = conf.getLong("mapred.resourceutilization.timelimit",
                             DEFAULT_TIME_LIMIT);

    // How long do we consider a job is finished after it stops
    stopTimeLimit = conf.getLong("mapred.resourceutilization.stoptimelimit",
                                 DEFAULT_STOP_TIME_LIMIT);

    // How often do we aggregate the reports
    aggregatePeriod = conf.getLong(
            "mapred.resourceutilization.aggregateperiod",
            DEFAULT_AGGREGATE_SLEEP_TIME);

    // Start the daemon thread to aggregate the TaskTracker reports
    this.aggregateDaemon = new Daemon(new AggregateRun());
    this.aggregateDaemon.start();
  }

  /**
   * Wait for service to finish.
   * (Normally, it runs forever.)
   */
  public void join() {
    try {
      if (server != null) server.join();
      if (aggregateDaemon != null) aggregateDaemon.join();
    } catch (InterruptedException ie) {
      // do nothing
    }
  }
  
  /**
   * Stop all Collector threads and wait for all to finish.
   */
  public void stop() {
    running = false;
    if (server != null) server.stop();
    if (aggregateDaemon != null) aggregateDaemon.interrupt();
  }

  private static InetSocketAddress getAddress(String address) {
    return NetUtils.createSocketAddr(address);
  }

  public static InetSocketAddress getAddress(Configuration conf) {
    // Address of the Collector server
    return getAddress(conf.get("mapred.resourceutilization.server.address",
                               "localhost:30011"));
  }

  /**
   * Aggregate the TaskTracker reports
   */
  protected void aggregateReports() {
    clusterUtil.clear();
    for (String jobId : allJobUtil.keySet()) {
      JobUtilization jobUtil = allJobUtil.get(jobId);
      jobUtil.clear();
      jobUtil.setIsRunning(false);
    }

    for (UtilizationReport report : taskTrackerReports.values()) {
      if (report.isExpired()) {
        continue; // ignore the report if it is older than the timeLimit
      }
      LocalJobUtilization[] localJobUtils = report.getJobUtilization();
      TaskTrackerUtilization ttUtil = report.getTaskTrackerUtilization();

      // Aggregate cluster information
      double cpuUsageGHz = clusterUtil.getCpuUsageGHz();
      double cpuTotalGHz = clusterUtil.getCpuTotalGHz();
      double memUsageGB = clusterUtil.getMemUsageGB();
      double memTotalGB = clusterUtil.getMemTotalGB();
      int numCpu = clusterUtil.getNumCpu();
      int numTaskTrackers = clusterUtil.getNumTaskTrackers();

      cpuUsageGHz += ttUtil.getCpuUsageGHz();
      cpuTotalGHz += ttUtil.getCpuTotalGHz();
      memUsageGB += ttUtil.getMemUsageGB();
      memTotalGB += ttUtil.getMemTotalGB();
      numCpu += ttUtil.getNumCpu();
      numTaskTrackers += 1;

      clusterUtil.setCpuUsageGHz(cpuUsageGHz);
      clusterUtil.setCpuTotalGHz(cpuTotalGHz);
      clusterUtil.setMemUsageGB(memUsageGB);
      clusterUtil.setMemTotalGB(memTotalGB);
      clusterUtil.setNumCpu(numCpu);
      clusterUtil.setNumTaskTrackers(numTaskTrackers);

      // Aggregate Job based information
      if ( localJobUtils == null) {
        continue;
      }

      for (LocalJobUtilization localJobUtil : localJobUtils) {
        String jobId = localJobUtil.getJobId();
        if (jobId == null) {
          continue;
        }

        if (!allJobUtil.containsKey(jobId)) {
          JobUtilization jobUtil = new JobUtilization();
          jobUtil.setJobId(jobId);
          allJobUtil.put(jobId, jobUtil);
        }
        JobUtilization jobUtil = allJobUtil.get(jobId);
        jobUtil.setIsRunning(true);
        double maxCpu = jobUtil.getCpuMaxPercentageOnBox();
        double cpu = jobUtil.getCpuPercentageOnCluster();
        double cpuGigaCycles = jobUtil.getCpuGigaCycles();
        double maxMem = jobUtil.getMemMaxPercentageOnBox();
        double mem = jobUtil.getMemPercentageOnCluster();

        cpu += localJobUtil.getCpuUsageGHz(); // will be normalized later
        mem += localJobUtil.getMemUsageGB(); // will be normalized later
        cpuGigaCycles += localJobUtil.getCpuUsageGHz() *
                         (double)aggregatePeriod * 1e6D / 1e9D;
        // GHz * ms = 1e6. To convert to Giga, divide by 1e9

        double localCpuPercentage =
                localJobUtil.getCpuUsageGHz() / ttUtil.getCpuTotalGHz() * 100;
        double localMemPercentage =
                localJobUtil.getMemUsageGB() / ttUtil.getMemTotalGB() * 100;

        if (maxCpu < localCpuPercentage) {
          maxCpu = localCpuPercentage;
        }
        if (maxMem < localMemPercentage) {
          maxMem = localMemPercentage;
        }
        jobUtil.setCpuMaxPercentageOnBox(maxCpu);
        jobUtil.setCpuGigaCycles(cpuGigaCycles);
        jobUtil.setCpuPercentageOnCluster(cpu); // will be normalized later
        jobUtil.setMemMaxPercentageOnBox(maxMem); // will be normalized later
        jobUtil.setMemPercentageOnCluster(mem);
        if (maxMem > jobUtil.getMemMaxPercentageOnBoxAllTime()) {
          jobUtil.setMemMaxPercentageOnBoxAllTime(maxMem);
        }
      }
    }

    // Normalization and clean up finished jobs
    for (Iterator<String>
            it = allJobUtil.keySet().iterator(); it.hasNext();) {
      String jobId = it.next();
      JobUtilization jobUtil = allJobUtil.get(jobId);
 	 	  if (!jobUtil.getIsRunning()) {
        long stoppedTime = jobUtil.getStoppedTime();
        stoppedTime += aggregatePeriod;
        jobUtil.setStoppedTime(stoppedTime);
        jobUtil.setIsRunning(false);

        if (stoppedTime > stopTimeLimit) {
          // These are finished jobs
          // We may store the information of finished jobs to some place
          double cpuTime = jobUtil.getCpuCumulatedUsageTime() / 1000D;
          double memTime = jobUtil.getMemCumulatedUsageTime() / 1000D;
          double peakMem = jobUtil.getMemMaxPercentageOnBoxAllTime();
          double cpuGigaCycles = jobUtil.getCpuGigaCycles();
          it.remove();
          LOG.info(String.format( 
                "Job done: [JobID,CPU(sec),Mem(sec),Peak Mem(%%),CPU gigacycles]" +
                " = [%s,%f,%f,%f,%f]",
                jobId, cpuTime, memTime, peakMem, cpuGigaCycles));
        }
        continue;
			}

      long runningTime = jobUtil.getRunningTime();
      runningTime += aggregatePeriod; // millisecond to second
      jobUtil.setRunningTime(runningTime);
      jobUtil.setStoppedTime(0);
      jobUtil.setIsRunning(true);

      int numJobs = clusterUtil.getNumRunningJobs();
      numJobs += 1;
      clusterUtil.setNumRunningJobs(numJobs);

      double cpu = jobUtil.getCpuPercentageOnCluster();
      double mem = jobUtil.getMemPercentageOnCluster();
      double cpuTime = jobUtil.getCpuCumulatedUsageTime();
      double memTime = jobUtil.getMemCumulatedUsageTime();
      cpu = cpu / clusterUtil.getCpuTotalGHz() * 100;
      mem = mem / clusterUtil.getMemTotalGB() * 100;
      cpuTime += cpu / 100 * aggregatePeriod;  // in milliseconds
      memTime += mem / 100 * aggregatePeriod;  // in milliseconds
      jobUtil.setCpuPercentageOnCluster(cpu);
      jobUtil.setMemPercentageOnCluster(mem);
      jobUtil.setCpuCumulatedUsageTime(cpuTime);
      jobUtil.setMemCumulatedUsageTime(memTime);
    }
  }

  /**
   * Periodically aggregate trasktracker reports
   */
  class AggregateRun implements Runnable {
    /**
     */
    @Override
    public void run() {
      while (running) {
        try {
          aggregateReports();
          Thread.sleep(aggregatePeriod);
        } catch (Exception e) {
          LOG.error(StringUtils.stringifyException(e));
        }
      }
    }
  }

  /**
   * Implement CollectorProtocol methods
   */

  @Override
  public TaskTrackerUtilization getTaskTrackerUtilization(String hostName)
          throws IOException {
    if (taskTrackerReports.get(hostName) == null) {
      return null;
    }
    return taskTrackerReports.get(hostName).getTaskTrackerUtilization();
  }


  @Override
  public JobUtilization getJobUtilization(String jobId) throws IOException {
    return allJobUtil.get(jobId);
  }

  @Override
  public TaskTrackerUtilization[] getAllTaskTrackerUtilization()
          throws IOException {
    List<TaskTrackerUtilization> result = new LinkedList<TaskTrackerUtilization>();
    for (UtilizationReport report : taskTrackerReports.values()) {
      if (!report.isExpired()) { //remove the expired reports
        result.add(report.getTaskTrackerUtilization());
      }
    }
    return result.toArray(new TaskTrackerUtilization[result.size()]);
  }

  @Override
  public JobUtilization[] getAllRunningJobUtilization()
          throws IOException {
    List<JobUtilization> result = new LinkedList<JobUtilization>();
    for (JobUtilization job : allJobUtil.values()) {
      if (job.getIsRunning()) {
        result.add(job);
      }
    }
    return result.toArray(new JobUtilization[result.size()]);
  }

  @Override
  public ClusterUtilization getClusterUtilization() throws IOException {
    return clusterUtil;
  }

  @Override
  public void reportTaskTrackerUtilization(
          TaskTrackerUtilization ttUtil, LocalJobUtilization[] localJobUtil)
                                    throws IOException {
    UtilizationReport utilReport = new UtilizationReport();
    utilReport.setValues(ttUtil, localJobUtil);
    taskTrackerReports.put(ttUtil.getHostName(), utilReport);
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
  
  /**
   * main program to run on the Collector server
   */
  public static void main(String argv[]) throws Exception {
    StringUtils.startupShutdownMessage(UtilizationCollector.class, argv, LOG);
    try {
      Configuration conf = new Configuration();
      UtilizationCollector collector = new UtilizationCollector(conf);
      if (collector != null) {
        collector.join();
      }
    } catch (Throwable e) {
      LOG.error(StringUtils.stringifyException(e));
      System.exit(-1);
    }
  }
}