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
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.net.SocketException;
import java.net.ServerSocket;
import java.net.URLEncoder;
import java.util.Enumeration;
import java.util.HashMap;
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
import org.apache.hadoop.corona.SessionHistoryManager;
import org.apache.hadoop.corona.TFactoryBasedThreadPoolServer;
import org.apache.hadoop.corona.Utilities;
import org.apache.hadoop.corona.CoronaProxyJobTrackerService;
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
import org.apache.thrift.server.TServer;

/**
 * This is used to proxy HTTP requests to individual Corona Job Tracker web
 * UIs.
 * Also used for aggregating information about jobs such as job counters.
 */
public class ProxyJobTracker implements
  JobHistoryObserver, CoronaJobAggregator, Updater,
  CoronaProxyJobTrackerService.Iface {
  /** Logger. */
  private static final Log LOG = LogFactory.getLog(ProxyJobTracker.class);

  static {
    Utilities.makeProcessExitOnUncaughtException(LOG);
  }

  /** Local machine name. */
  private static String LOCALMACHINE;
  /** Http server port. */
  private static int LOCALPORT;
  /** Configuration. */
  private static CoronaConf conf;
  /** Session History Manager. */
  private static SessionHistoryManager sessionHistoryManager;
  /** Default clock. */
  private static final Clock DEFAULTCLOCK = new Clock();
  /** Clock. */
  private Clock clock = null;
  /** Filesystem. */
  private FileSystem fs = null;
  /** The HTTP server. */
  private HttpServer infoServer;
  /** The RPC server. */
  private Server rpcServer;
  /** Cache expiry. */
  private ExpireUnusedFilesInCache expireUnusedFilesInCache;
  /** Start time of the server. */
  private long startTime;
  /** Aggregate job counters. */
  private Counters aggregateCounters = new Counters();
  /** Aggregate job stats. */
  private JobStats aggregateJobStats = new JobStats();
  /** Job Counters aggregated by pool */
  private Map<String, Counters> poolToJobCounters =
    new HashMap<String, Counters>();
  /** Job Stats aggregated by pool */
  private Map<String, JobStats> poolToJobStats =
    new HashMap<String, JobStats>();
  /** Metrics context. */
  private MetricsContext context;
  /** Metrics record. */
  private MetricsRecord metricsRecord;
  /** Is the Cluster Manager in Safe Mode? */
  private volatile boolean clusterManagerSafeMode;
  /** Metrics Record for pools */
  private Map<String, MetricsRecord> poolToMetricsRecord =
    new HashMap<String, MetricsRecord>();
  /* This is the thrift server thread */
  private TServerThread server;

  /* The thrift server thread class */
  public class TServerThread extends Thread {
    private TServer server;

    public TServerThread(TServer server) {
      this.server = server;
    }

    public void run() {
      try {
        server.serve();
      } catch (Exception e) {
        LOG.info("Got an exception: ", e);
      }
    }
  }

  @Override
  public void doUpdates(MetricsContext unused) {
    synchronized (aggregateJobStats) {
      // Update metrics with aggregate job stats and reset the aggregate.
      aggregateJobStats.incrementMetricsAndReset(metricsRecord);

      incrementMetricsAndReset(metricsRecord, aggregateCounters);

      for (Map.Entry<String, MetricsRecord> entry :
        poolToMetricsRecord.entrySet()) {
        String pool = entry.getKey();

        JobStats poolJobStats = poolToJobStats.get(pool);
        poolJobStats.incrementMetricsAndReset(entry.getValue());

        Counters poolCounters = poolToJobCounters.get(pool);
        incrementMetricsAndReset(entry.getValue(), poolCounters);
      }
    }
  }

  private static void incrementMetricsAndReset(
    MetricsRecord record, Counters counters) {
    // Now update metrics with the counters and reset the aggregate.
    for (Counters.Group group : counters) {
      String groupName = group.getName();
      for (Counter counter : group) {
        String name = groupName + "_" + counter.getName();
        name = name.replaceAll("[^a-zA-Z_]", "_").toLowerCase();
        record.incrMetric(name, counter.getValue());
      }
    }
    // Reset the aggregate counters.
    for (Counters.Group g : counters) {
      for (Counter c : g) {
        c.setValue(0);
      }
    }
    record.update();
  }

  /**
   * Servlet to handle requests.
   */
  public static class ProxyJobTrackerServlet extends HttpServlet {
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
      String host = request.getParameter("host");
      String port = request.getParameter("port");
      String path = request.getParameter("path");
      if (host == null || port == null || path == null) {
        response.sendError(HttpServletResponse.SC_BAD_REQUEST,
          "Missing mandatory host and/or port parameters");
        return;
      }
      try {
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
      } catch (ConnectException ce) {
        handleDeadJobTracker(response, host, port);
      } catch (SocketException se) {
        checkDeadJobTracker(se, response, host, port);
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
      String host = request.getParameter("host");
      String port = request.getParameter("port");
      String path = request.getParameter("path");
      try {
        String jobIDParam = request.getParameter("jobid");
        // Check if the full path is given.
        String jobHistoryFileLocParam =
          request.getParameter("jobhistoryfileloc");
        Path jobHistoryFileLocation = jobHistoryFileLocParam == null ?
          null : new Path(jobHistoryFileLocParam);
        if (jobHistoryFileLocation == null) {
          // If the full path is not given, check the history directory.
          String historyDirParam = request.getParameter("historydir");
          Path historyDir = null;
          if (historyDirParam != null) {
            historyDir = new Path(conf.getSessionsLogDir(), historyDirParam);
          } else if (jobIDParam != null) {
            // Infer the history location from the job id.
            JobID jobID = JobID.forName(jobIDParam);
            String sessionId = jobID.getJtIdentifier();
            historyDir =
              new Path(sessionHistoryManager.getLogPath(sessionId));
          }
          if (historyDir != null) {
            Path doneDir = new Path(historyDir, "done");
            jobHistoryFileLocation = new Path(doneDir, jobIDParam);
          }
        }

        //check if the job is in the jobHistory
        methodString =  (jobHistoryFileLocation == null) ?
          null :
          urlInJobHistory(jobHistoryFileLocation, jobIDParam);
        //it's not in the job history
        //directly go to the job tracker to retrieve the job information
        //otherwise, directly load the jobhistory page
        if (methodString == null) {
          if (host == null || port == null || path == null) {
            response.sendError(HttpServletResponse.SC_BAD_REQUEST,
              "Missing mandatory host and/or port parameters");
            return;
          }
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
      } catch (ConnectException ce) {
        handleDeadJobTracker(response, host, port);
      } catch (SocketException se) {
        checkDeadJobTracker(se, response, host, port);
      } catch (IOException e) {
        LOG.warn("Exception forwarding request to: " + methodString);
        throw e;
      }
    }

    /**
     * Check if the job tracker could be dead based on the exception
     * encountered and provide a helpful message in the response.
     * @param e the exception.
     * @param response the HTTP response
     * @param host The host of the job tracker.
     * @param port the port of the job tracker.
     * @throws IOException
     */
    private void checkDeadJobTracker(
      IOException e,
      HttpServletResponse response,
      String host,
      String port) throws IOException {
      if (e.getMessage().contains("Broken pipe") ||
        e.getMessage().contains("Connection reset")) {
        handleDeadJobTracker(response, host, port);
      } else {
        throw e;
      }
    }

    /**
     * Provide a helpful message in the response after the job tracker has
     * been determined to be dead.
     * @param response the HTTP response
     * @param host The host of the job tracker.
     * @param port the port of the job tracker.
     * @throws IOException
     */
    private void handleDeadJobTracker(
      HttpServletResponse response,
      String host,
      String port) throws IOException {
      String msg =
        "Could not connect to Job Tracker at " + host + ":" + port +
          ". The job may have completed or been killed. Please go to the " +
          "Cluster Manager UI and click on your job again, or retry the " +
          "tracking URL if available.";
      byte[] msgBytes = msg.getBytes();
      response.getOutputStream().write(msgBytes, 0, msgBytes.length);
    }
  }

  public int getPort() {
    return infoServer.getPort();
  }

  public int getRpcPort() {
    return rpcServer.getListenerAddress().getPort();
  }

  public String getProxyJobTrackerMachine() {
    return LOCALMACHINE;
  }

  Clock getClock() {
    return clock == null ? DEFAULTCLOCK : clock;
  }

  public void historyFileCopied(JobID jobid, String historyFile) {
  }

  public ProxyJobTracker(CoronaConf conf) throws IOException {
    this.conf = conf;
    fs = FileSystem.get(conf);
    String infoAddr =
      conf.get("mapred.job.tracker.corona.proxyaddr", "0.0.0.0:0");
    InetSocketAddress infoSocAddr = NetUtils.createSocketAddr(infoAddr);
    String infoBindAddress = infoSocAddr.getHostName();
    int port = infoSocAddr.getPort();
    LOCALMACHINE = infoBindAddress;
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
    JobConf jobConf = new JobConf(conf);
    boolean historyInitialized = JobHistory.init(
      this, jobConf, this.LOCALMACHINE, this.startTime);
    if (historyInitialized) {
      JobHistory.initDone(jobConf, fs);
      String historyLogDir =
          JobHistory.getCompletedJobHistoryLocation().toString();
      FileSystem historyFS = new Path(historyLogDir).getFileSystem(conf);
      infoServer.setAttribute("historyLogDir", historyLogDir);
      infoServer.setAttribute("fileSys", historyFS);
    }
    infoServer.start();
    LOCALPORT = infoServer.getPort();

    context = MetricsUtil.getContext("mapred");
    metricsRecord = MetricsUtil.createRecord(context, "proxyjobtracker");
    context.registerUpdater(this);

    expireUnusedFilesInCache = new ExpireUnusedFilesInCache(
      conf, getClock(), new Path(getSystemDir()), fs);

    sessionHistoryManager = new SessionHistoryManager();
    sessionHistoryManager.setConf(conf);

    try {
      String target = conf.getProxyJobTrackerThriftAddress();
      InetSocketAddress addr = NetUtils.createSocketAddr(target);
      LOG.info("Trying to start the Thrift Server at: " + target);
      ServerSocket serverSocket = new ServerSocket(addr.getPort());
      server = new TServerThread(
        TFactoryBasedThreadPoolServer.createNewServer(
          new CoronaProxyJobTrackerService.Processor(this),
          serverSocket,
          5000));
      server.start();
      LOG.info("Thrift server started on: " + target);
    } catch (IOException e) {
      LOG.info("Exception while starting the Thrift Server on CPJT: ", e);
    }
  }

  @Override
  public void reportJobStats(
    String jobId, String pool, JobStats stats, Counters counters) {
    synchronized (aggregateJobStats) {
      aggregateJobStats.accumulate(stats);
      JobStats poolJobStats = poolToJobStats.get(pool);
      if (poolJobStats == null) {
        poolJobStats = new JobStats();
        poolToJobStats.put(pool, poolJobStats);
      }
      poolJobStats.accumulate(stats);

      accumulateCounters(aggregateCounters, counters);
      Counters poolCounters = poolToJobCounters.get(pool);
      if (poolCounters == null) {
        poolCounters = new Counters();
        poolToJobCounters.put(pool, poolCounters);
      }
      accumulateCounters(poolCounters, counters);

      if (!poolToMetricsRecord.containsKey(pool)) {
        MetricsRecord poolRecord = context.createRecord("pool-" + pool);
        poolToMetricsRecord.put(pool, poolRecord);
      }
    }
  }

  private static void accumulateCounters(
    Counters aggregate, Counters increment) {
    for (JobInProgress.Counter key : JobInProgress.Counter.values()) {
      Counter counter = increment.findCounter(key);
      if (counter != null) {
        aggregate.findCounter(key).increment(counter.getValue());
      }
    }
    for (Task.Counter key : Task.Counter.values()) {
      Counter counter = increment.findCounter(key);
      if (counter != null) {
        aggregate.findCounter(key).increment(counter.getValue());
      }
    }
    for (Counters.Counter counter :
      increment.getGroup(Task.FILESYSTEM_COUNTER_GROUP)) {
      aggregate.incrCounter(
        Task.FILESYSTEM_COUNTER_GROUP, counter.getName(), counter.getValue());
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

  // Used by the CM to tell the CPJT if it's in Safe Mode.
  @Override
  public void setClusterManagerSafeModeFlag(boolean safeMode) {
    clusterManagerSafeMode = safeMode;
    LOG.info("On ProxyJobTracker, clusterManagerSafeModeFlag: " +
      clusterManagerSafeMode);
  }

  // Has the CM gone into Safe Mode and told the CPJT about it?
  @Override
  public boolean getClusterManagerSafeModeFlag() {
    return clusterManagerSafeMode;
  }

  public void join() throws InterruptedException {
    infoServer.join();
    rpcServer.join();
    server.join();
  }

  public static ProxyJobTracker startProxyTracker(CoronaConf conf)
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
  public static String urlInJobHistory(
    Path jobHistoryFileLocation, String jobId)
    throws IOException {
    try {
      FileSystem fs = jobHistoryFileLocation.getFileSystem(conf);
      fs.getFileStatus(jobHistoryFileLocation);
    }  catch (FileNotFoundException e) {
      return null;
    }
    return "http://" + LOCALMACHINE + ":" + LOCALPORT +
      "/coronajobdetailshistory.jsp?jobid=" + jobId +
      "&logFile=" + URLEncoder.encode(jobHistoryFileLocation.toString());
  }

  public String getSystemDir() {
    return CoronaJobTracker.getSystemDir(fs, conf);
  }

  public static void main(String[] argv) throws IOException {

    StringUtils.startupShutdownMessage(ProxyJobTracker.class, argv, LOG);
    ProxyJobTracker p = startProxyTracker(new CoronaConf(new Configuration()));

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
