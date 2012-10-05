/*
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


import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.URLEncoder;
import java.util.Enumeration;
import java.util.Map;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpMethod;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.httpclient.methods.PostMethod;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.corona.CoronaConf;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.http.HttpServer;
import org.apache.hadoop.ipc.ProtocolSignature;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.metrics.MetricsContext;
import org.apache.hadoop.metrics.MetricsRecord;
import org.apache.hadoop.metrics.MetricsUtil;
import org.apache.hadoop.metrics.Updater;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.util.StringUtils;

/**
 * This is used to proxy HTTP requests to individual Corona Job Tracker web
 * UIs.
 * Also used for aggregating information about jobs such as job counters.
 */
public class ProxyJobTracker implements
  JobHistoryObserver, CoronaJobAggregator, Updater {

  /** Logger. */
  private static final Log LOG = LogFactory.getLog(ProxyJobTracker.class);

  /** Local machine name. */
  private String localMachine;
  /** Http server port. */
  private int localPort;
  /** Clock. */
  private Clock clock = null;
  /** Default clock. */
  private final Clock DEFAULT_CLOCK = new Clock();
  /** Filesystem. */
  private FileSystem fs = null;
  /** The HTTP server. */
  private HttpServer infoServer;
  /** The RPC server. */
  private Server rpcServer;
  /** Start time of the server. */
  private long startTime;
  /** Configuration. */
  private Configuration conf;
  /** Aggregate job counters. */
  private Counters aggregateCounters = new Counters();
  /** Aggregate job stats. */
  private JobStats aggregateJobStats = new JobStats();
  /** Metrics context. */
  private MetricsContext context;
  /** Metrics record. */
  private MetricsRecord metricsRecord;

  @Override
  public void doUpdates(MetricsContext unused) {
    synchronized (aggregateJobStats) {
      // Update metrics with aggregate job stats and reset the aggregate.
      aggregateJobStats.incrementMetricsAndReset(metricsRecord);

      // Now update metrics with the counters and reset the aggregate.
      for (Counters.Group group: aggregateCounters) {
        String groupName = group.getName();
        for (Counter counter : group) {
          String name = groupName + "_" + counter.getName();
          name = name.replaceAll("[^a-zA-Z_]", "_").toLowerCase();
          metricsRecord.incrMetric(name, counter.getValue());
        }
      }
      // Reset the aggregate counters.
      for (Counters.Group g : aggregateCounters) {
        for (Counter c : g) {
          c.setValue(0);
        }
      }
    }
    metricsRecord.update();
  }

  public class ProxyJobTrackerServlet extends HttpServlet {
    @Override
    public void init() throws ServletException {
      LOG.info("Initialized " + this.getClass().getName());
      super.init();
    }

    @Override
    protected void doPost(
      HttpServletRequest request, HttpServletResponse response)
      throws IOException {
      String destination = "";
      try {
        String host = request.getParameter("host");
        String port = request.getParameter("port");
        String path = request.getParameter("path");

        if (host == null || port == null || path == null) {
          response.sendError(HttpServletResponse.SC_BAD_REQUEST,
            "Missing mandatory host and/or port parameters");
          return;
        }
        destination = "http://" + host + ":" + port + "/" + path;
        PostMethod method = new PostMethod(destination);
        for (Enumeration e = request.getParameterNames();
             e.hasMoreElements();) {
          String key = (String) e.nextElement();
          if (key.equals("host") || key.equals("port") || key.equals("path")) {
            continue;
          }
          method.addParameter(key, request.getParameter(key));
        }
        HttpClient httpclient = new HttpClient();
        int statusCode = httpclient.executeMethod(method);
        response.setStatus(statusCode);
        response.setContentType("text/html");
        InputStream is = method.getResponseBodyAsStream();
        int len = 0;
        int bufferSize = 4096;
        byte[] buf = new byte[bufferSize];
        while ((len = is.read(buf)) >= 0) {
          response.getOutputStream().write(buf, 0, len);
        }

        if (statusCode != HttpServletResponse.SC_OK) {
          LOG.warn("Status " + statusCode + " forwarding request to: " +
            destination);
        }

      } catch (IOException e) {
        LOG.warn("Exception forwarding request to: " + destination);
        throw e;
      }
    }

    @Override
    public void doGet(HttpServletRequest request, HttpServletResponse response)
      throws ServletException, IOException {
      StringBuffer sb = null;
      String methodString = null;
      try {
        String host = request.getParameter("host");
        String port = request.getParameter("port");
        String path = request.getParameter("path");
        String jobid = request.getParameter("jobid");
        String jobHistoryFileLocation =
          request.getParameter("jobhistoryfileloc");

        if (host == null || port == null || path == null) {
          response.sendError(HttpServletResponse.SC_BAD_REQUEST,
              "Missing mandatory host and/or port parameters");
          return;
        }
        //check if the job is in the jobHistory
        methodString =  (jobHistoryFileLocation == null) ?
          null :
          urlInJobHistory(jobHistoryFileLocation, jobid);
        //it's not in the job history
        //directly go to the job tracker to retrieve the job information
        //otherwise, directly load the jobhistory page
        if (methodString == null) {
          LOG.info("history file: " + jobHistoryFileLocation +
            " is not in jobhistory");
          sb = new StringBuffer("http://");
          sb.append(host).append(":").append(port).append("/").append(path);

          Map<String, String []> m = request.getParameterMap();
          boolean firstArg = true;

          for (Map.Entry<String, String []> e: m.entrySet()) {
            String key = e.getKey();
            //also ingore the jobhistoryfileloc, only used when the job is done
            //and log is in the job history
            if (key.equals("host") || key.equals("path") ||
              key.equals("port") || key.equals("jobhistoryfileloc")) {
              continue;
            }

            if (firstArg) {
              sb.append('?');
              firstArg = false;
            } else {
              sb.append('&');
            }

            sb.append(e.getKey() + "=" + e.getValue()[0]);
          }
          methodString = sb.toString();
        }
        HttpClient httpclient = new HttpClient();
        HttpMethod method = new GetMethod(methodString);

        int sc = httpclient.executeMethod(method);
        response.setStatus(sc);
        response.setContentType("text/html");
        InputStream is = method.getResponseBodyAsStream();
        int len = 0;
        int bufferSize = 4096;
        byte[] buf = new byte[bufferSize];
        while ((len = is.read(buf)) >= 0) {
          response.getOutputStream().write(buf, 0, len);
        }

        if (sc != HttpServletResponse.SC_OK) {
          LOG.warn("Status " + sc + " forwarding request to: " + methodString);
        }

      } catch (IOException e) {
        LOG.warn("Exception forwarding request to: " + methodString);
        throw e;
      }
    }
  }

  public int getPort() {
    return infoServer.getPort();
  }

  public int getRpcPort() {
    return rpcServer.getListenerAddress().getPort();
  }

  public String getProxyJobTrackerMachine() {
    return localMachine;
  }

  Clock getClock() {
    return clock == null ? DEFAULT_CLOCK : clock;
  }

  public void historyFileCopied(JobID jobid, String historyFile) {
  }

  public ProxyJobTracker(JobConf conf, Clock clock) throws IOException {
    this(conf);
    this.clock = clock;
  }

  public ProxyJobTracker(JobConf conf) throws IOException {
    this.conf = conf;
    fs = FileSystem.get(conf);
    String infoAddr =
      conf.get("mapred.job.tracker.corona.proxyaddr", "0.0.0.0:0");
    InetSocketAddress infoSocAddr = NetUtils.createSocketAddr(infoAddr);
    String infoBindAddress = infoSocAddr.getHostName();
    int port = infoSocAddr.getPort();
    localMachine = infoBindAddress;
    startTime = getClock().getTime();

    CoronaConf coronaConf = new CoronaConf(conf);
    InetSocketAddress rpcSockAddr = NetUtils.createSocketAddr(
      coronaConf.getProxyJobTrackerAddress());
    rpcServer = RPC.getServer(
      this,
      rpcSockAddr.getHostName(),
      rpcSockAddr.getPort(),
      conf.getInt("corona.proxy.job.tracker.handler.count", 10),
      false,
      conf);
    rpcServer.start();

    LOG.info("ProxyJobTracker RPC Server up at " +
      rpcServer.getListenerAddress());

    infoServer = new HttpServer("proxyjt", infoBindAddress, port,
                                port == 0, conf);
    infoServer.setAttribute("proxy.job.tracker", this);
    infoServer.setAttribute("conf", conf);
    infoServer.addServlet("proxy", "/proxy",
                          ProxyJobTrackerServlet.class);
    // initialize history parameters.
    boolean historyInitialized = JobHistory.init(this, conf, this.localMachine,
                                                 this.startTime);
    if (historyInitialized) {
      JobHistory.initDone(conf, fs);
      String historyLogDir =
          JobHistory.getCompletedJobHistoryLocation().toString();
      FileSystem historyFS = new Path(historyLogDir).getFileSystem(conf);
      infoServer.setAttribute("historyLogDir", historyLogDir);
      infoServer.setAttribute("fileSys", historyFS);
    }
    infoServer.start();

    context = MetricsUtil.getContext("mapred");
    metricsRecord = MetricsUtil.createRecord(context, "proxyjobtracker");
    context.registerUpdater(this);
  }

  @Override
  public void reportJobStats(
    String jobId, String pool, JobStats stats, Counters counters) {
    synchronized (aggregateJobStats) {
      aggregateJobStats.accumulate(stats);
      for (JobInProgress.Counter key : JobInProgress.Counter.values()) {
        aggregateCounters.findCounter(key).
          increment(counters.findCounter(key).getValue());
      }
      for (Task.Counter key : Task.Counter.values()) {
        aggregateCounters.findCounter(key).
          increment(counters.findCounter(key).getValue());
      }
      for (Counters.Counter counter :
        counters.getGroup(Task.FILESYSTEM_COUNTER_GROUP)) {
        aggregateCounters.incrCounter(
          Task.FILESYSTEM_COUNTER_GROUP, counter.getName(), counter.getValue());
      }
    }
  }

  @Override
  public long getProtocolVersion(String protocol, long clientVersion)
    throws IOException {
    if (protocol.equals(CoronaJobAggregator.class.getName())) {
      return CoronaJobAggregator.versionID;
    } else {
      throw new IOException("Unknown protocol " + protocol);
    }
  }

  @Override
  public ProtocolSignature getProtocolSignature(
    String protocol,
    long clientVersion,
    int clientMethodsHash) throws IOException {
    return ProtocolSignature.getProtocolSignature(
      this, protocol, clientVersion, clientMethodsHash);
  }

  public void join() throws InterruptedException {
    infoServer.join();
    rpcServer.join();
  }

  public static ProxyJobTracker startProxyTracker(JobConf conf)
    throws IOException {
    ProxyJobTracker result = new ProxyJobTracker(conf);
    return result;
  }

  /**
   * Given the path to the jobHistoryFile, check if the file already exists.
   * 1. If FileNoFoundException is caught, means the job is not yet finished,
   * and there is not job hisotry log file in the done directory
   * 2. If not, it means we get a hit for the jobHistoryFile, directly recover
   * the url to the coronoajobdetailshistory page.
   * @param jobId
   * @return url if the job is done and the jobHistory is in the jobHistory
   * folder, null if the job cannot be found in the jobHistory folder.
   * @throws IOException
   */
  public String urlInJobHistory(
    String jobHistoryFileLocation, String jobId)
    throws IOException {
    try {
      Path p = new Path(jobHistoryFileLocation);
      FileSystem fs = p.getFileSystem(conf);
      fs.getFileStatus(p);
    }  catch (FileNotFoundException e) {
      return null;
    }
    return "http://" + localMachine + ":" + localPort +
      "/coronajobdetailshistory.jsp?jobid=" + jobId +
      "&logFile=" + URLEncoder.encode(jobHistoryFileLocation);
  }

  public static void main(String[] argv) throws IOException {

    StringUtils.startupShutdownMessage(ProxyJobTracker.class, argv, LOG);
    ProxyJobTracker p = startProxyTracker(new JobConf());

    boolean joined = false;
    while (!joined) {
      try {
        p.join();
        joined = true;
      } catch (InterruptedException e) {
        LOG.warn("Ignoring InterruptedException");
      }
    }
  }
}
