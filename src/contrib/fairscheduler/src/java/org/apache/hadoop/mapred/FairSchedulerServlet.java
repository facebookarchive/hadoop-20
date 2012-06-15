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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.hadoop.mapred.FairScheduler.JobComparator;
import org.apache.hadoop.mapred.FairScheduler.JobInfo;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.util.StringUtils;

/**
 * Servlet for displaying fair scheduler information, installed at
 * [job tracker URL]/fairscheduler when the {@link FairScheduler} is in use.
 *
 * The main features are viewing each job's task count and fair share, ability
 * to change job priorities and pools from the UI, and ability to switch the
 * scheduler to FIFO mode without restarting the JobTracker if this is required
 * for any reason.
 *
 * There is also an "advanced" view for debugging that can be turned on by
 * going to [job tracker URL]/fairscheduler?advanced.
 */
public class FairSchedulerServlet extends HttpServlet {
  private static final long serialVersionUID = 9104070533067306659L;
  private static final DateFormat DATE_FORMAT =
    new SimpleDateFormat("MMM dd, HH:mm");

  // This object obtain the resource utilization information
  private FairScheduler scheduler;
  private LoadManager loadMgr;
  private JobTracker jobTracker;
  private static long lastId = 0; // Used to generate unique element IDs

  @Override
  public void init() throws ServletException {
    super.init();
    ServletContext servletContext = this.getServletContext();
    this.scheduler = (FairScheduler) servletContext.getAttribute("scheduler");
    this.jobTracker = (JobTracker) scheduler.taskTrackerManager;
    this.loadMgr = scheduler.getLoadManager();
  }

  @Override
  protected void doPost(HttpServletRequest req, HttpServletResponse resp)
      throws ServletException, IOException {
    doGet(req, resp); // Same handler for both GET and POST
  }

  @Override
  public void doGet(HttpServletRequest request, HttpServletResponse response)
      throws ServletException, IOException {
    try {
      doActualGet(request, response);
    } catch (IOException ioe) {
      FairScheduler.LOG.error(
          FairSchedulerServlet.class + " throws exception", ioe);
      throw ioe;
    } catch (ServletException se) {
      FairScheduler.LOG.error(
          FairSchedulerServlet.class + " throws exception", se);
      throw se;
    } catch (Throwable t) {
      FairScheduler.LOG.error(
          FairSchedulerServlet.class + " throws exception", t);
      throw new RuntimeException(t);
    }
  }

  public void doActualGet(HttpServletRequest request, HttpServletResponse response)
      throws ServletException, IOException {
    // Check for user set or pool set filters
    Set<String> userFilterSet = null;
    String userFilter = request.getParameter("users");
    if (userFilter != null) {
      userFilterSet = new HashSet<String>(Arrays.asList(userFilter.split(",")));
    }
    Set<String> poolFilterSet = null;
    String poolFilter = request.getParameter("pools");
    if (poolFilter != null) {
      poolFilterSet = new HashSet<String>(Arrays.asList(poolFilter.split(",")));
    }

    // If the request has a set* param, handle that and redirect to the regular
    // view page so that the user won't resubmit the data if they hit refresh.
    boolean advancedView = request.getParameter("advanced") != null;
    if (request.getParameter("setJobComparator") != null) {
      String newMode = request.getParameter("setJobComparator");
      scheduler.setJobComparator(JobComparator.valueOf(newMode));
      response.sendRedirect("/fairscheduler" + (advancedView ? "?advanced" : ""));
      return;
    }
    if (request.getParameter("setPool") != null) {
      Collection<JobInProgress> runningJobs = getInitedJobs();
      PoolManager poolMgr = null;
      synchronized (scheduler) {
        poolMgr = scheduler.getPoolManager();
      }
      String pool = request.getParameter("setPool");
      String jobId = request.getParameter("jobid");
      for (JobInProgress job: runningJobs) {
        if (job.getProfile().getJobID().toString().equals(jobId)) {
          synchronized(scheduler){
            poolMgr.setPool(job, pool);
            scheduler.infos.get(job).poolName = pool;
          }
          scheduler.update();
          break;
        }
      }
      response.sendRedirect("/fairscheduler" + (advancedView ? "?advanced" : ""));
      return;
    }
    if (request.getParameter("setPriority") != null) {
      Collection<JobInProgress> runningJobs = getInitedJobs();
      JobPriority priority = JobPriority.valueOf(request.getParameter(
          "setPriority"));
      String jobId = request.getParameter("jobid");
      for (JobInProgress job: runningJobs) {
        if (job.getProfile().getJobID().toString().equals(jobId)) {
          job.setPriority(priority);
          scheduler.update();
          break;
        }
      }
      response.sendRedirect("/fairscheduler" + (advancedView ? "?advanced" : ""));
      return;
    }
    if (request.getParameter("setTtThreshold") != null) {
      long newThreshold = Long.parseLong(
              request.getParameter("setTtThreshold")) * 1024;
      ((MemBasedLoadManager)loadMgr).setReservedPhysicalMemoryOnTT(newThreshold);
      response.sendRedirect("/fairscheduler" + (advancedView ? "?advanced" : ""));
      return;
    }
    if (request.getParameter("setLocalityDelayNodeLocal") != null) {
      long delay =
        Long.parseLong(request.getParameter("setLocalityDelayNodeLocal"));
      scheduler.setLocalityDelayNodeLocal(delay);
      response.sendRedirect("/fairscheduler" + (advancedView ? "?advanced" : ""));
      return;
    }
    if (request.getParameter("setLocalityDelayRackLocal") != null) {
      long delay =
        Long.parseLong(request.getParameter("setLocalityDelayRackLocal"));
      scheduler.setLocalityDelayRackLocal(delay);
      response.sendRedirect("/fairscheduler" + (advancedView ? "?advanced" : ""));
      return;
    }
    if (request.getParameter("setMapPerHeartBeat") != null) {
      scheduler.setMapPerHeartBeat(
              Integer.parseInt(request.getParameter("setMapPerHeartBeat")));
      response.sendRedirect("/fairscheduler" + (advancedView ? "?advanced" : ""));
      return;
    }
    if (request.getParameter("setReducePerHeartBeat") != null) {
      scheduler.setReducePerHeartBeat(
              Integer.parseInt(request.getParameter("setReducePerHeartBeat")));
      response.sendRedirect("/fairscheduler" + (advancedView ? "?advanced" : ""));
      return;
    }
    if (request.getParameter("setPreemptionEnabled") != null) {
      scheduler.setPreemptionEnabled(
                "On".equals(request.getParameter("setPreemptionEnabled")) ?
                true : false);
    }
    // Print out the normal response
    response.setContentType("text/html");

    // Because the client may read arbitrarily slow, and we hold locks while
    // the servlet output, we want to write to our own buffer which we know
    // won't block.
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    PrintWriter out = new PrintWriter(baos);
    String hostname = StringUtils.simpleHostname(
        jobTracker.getJobTrackerMachine());
    out.print("<html><head>");
    out.printf("<title>%s Job Scheduler Admininstration</title>\n", hostname);
    out.print("<link rel=\"stylesheet\" type=\"text/css\" " +
        "href=\"/static/hadoop.css\">\n");
    out.print("<link rel=\"stylesheet\" type=\"text/css\" " +
        "href=\"/static/tablesorter/style.css\">\n");
    out.print("<script type=\"text/javascript\" " +
        "src=\"/static/jquery-1.7.1.min.js\"></script>");
    out.print("<script type=\"text/javascript\" " +
        "src=\"/static/tablesorter/jquery.tablesorter.js\"></script>");
    out.print("<script type=\"text/javascript\" " +
        "src=\"/static/tablesorter/jobtablesorter.js\"></script>");
    out.print("<script type=\"text/javascript\">");
    out.print("var prios=['VERY_LOW','LOW','NORMAL','HIGH','VERY_HIGH'];");
    out.print("var pools=[");
    for (String p : scheduler.getPoolManager().getPoolNames()) {
      out.printf("'%s',", p.replace("\n", "\\n").replace("'","\\'"));
    }
    out.printf("];var advanced=%s;</script>\n", advancedView);
    out.print("<script type=\"text/javascript\" "
        + "src=\"/static/dynamic-selector.js\"></script>\n");
    out.print("<script type=\"text/javascript\" "
        + "src=\"/static/tablefilter.js\"></script>\n");
    out.print("<script type=\"text/javascript\">");
    out.print("function init() {\n");
    out.print("  var table=document.getElementById('RunningJobsTable');\n");
    out.print("  var startRow = 2; // Rows 0,1 are header rows\n");
    out.print("  var filteredColumns = ["
        + "new ColumnSearchToggle(0, 'TimeFilterToggle'),"
        + "new ColumnSearchToggle(1, 'JobIDFilterToggle'),"
        + "new ColumnSearchToggle(2, 'UserFilterToggle'),"
        + "new ColumnSearchToggle(3, 'NameFilterToggle'),"
        + "new ColumnSearchToggle(4, 'PoolFilterToggle'),"
        + "new ColumnSearchToggle(5, 'PrioFilterToggle')];\n\n");
    out.print("addTable(table, startRow, filteredColumns);\n");
    out.print("}\nwindow.onload=init;</script>\n");
    out.print("<style type=\"text/css\">"
        + ".fake-link {color: blue; "
          + "text-decoration: underline; "
          + "cursor: pointer;}</style>\n");
    out.print("</head><body>\n");
    out.printf("<h1><a href=\"/jobtracker.jsp\">%s</a> " +
        "Job Scheduler Administration</h1>\n", hostname);
    out.print("<h2><a href=\"fairscheduleradmissioncontrol\">" +
        "Admission Control</a></h2>");
    printFilterInfo(out, poolFilter, userFilter, "fairscheduler");
    showCluster(out, advancedView, jobTracker);
    showPools(out, advancedView, poolFilterSet);
    showJobs(out, advancedView, userFilterSet, poolFilterSet);
    if (advancedView) {
      showAdminFormMemBasedLoadMgr(out, advancedView);
      showAdminFormLocalityDelay(out, advancedView);
      showNumTaskPerHeartBeatOption(out, advancedView);
      showAdminFormJobComparator(out, advancedView);
      showAdminFormPreemption(out, advancedView);
    }
    out.print("</body></html>\n");
    out.close();

    // Flush our buffer to the real servlet output
    OutputStream servletOut = response.getOutputStream();
    baos.writeTo(servletOut);
    servletOut.close();
  }

  /**
   * Print the filter information for pools and users
   * @param out Where the output is dumped
   * @param poolFilter Comma separated list of pools, null for none
   * @param userSet Comma separated list of users, null for none
   * @param showAllLink Link to show everything
   */
  static void printFilterInfo(PrintWriter out,
      String poolFilter, String userFilter, String showAllLink) {
    if (userFilter != null || poolFilter != null) {
      StringBuilder customizedInfo = new StringBuilder("Only showing ");
      if (poolFilter != null) {
        customizedInfo.append("pool(s) " + poolFilter);
      }
      if (userFilter != null) {
        if (customizedInfo.length() != 0) {
          customizedInfo.append(" and ");
        }
        customizedInfo.append("user(s) " + userFilter);
      }
      out.printf("<h3>%s <a href=\"%s\">(show all pools and users)</a></h3>",
          customizedInfo.toString(), showAllLink);
    }
  }

  /**
   * Print a view of pools to the given output writer.
   *
   * @param out All html output goes here.
   * @param advancedView Show advanced view if true
   * @param String poolFilterSet If not null, only show this set's info
   */
  private void showPools(PrintWriter out, boolean advancedView,
      Set<String> poolFilterSet) {
    ResourceReporter reporter = jobTracker.getResourceReporter();
    synchronized (jobTracker) {
      synchronized(scheduler) {
        PoolManager poolManager = scheduler.getPoolManager();
        out.print("<h2>Active Pools</h2>\n");
        out.print("<table border=\"2\" cellpadding=\"5\" cellspacing=\"2\" " +
                  "class=\"tablesorter\">\n");
        out.print("<thead><tr><th>Pool</th><th>Running Jobs</th>" +
            "<th>Preparing Jobs</th>" +
            "<th>Min Maps</th><th>Min Reduces</th>" +
            "<th>Max Maps</th><th>Max Reduces</th>" +
            "<th>Initialized Tasks</th>" +
            "<th>Max Initialized Tasks</th>" +
            "<th>Running/Waiting Maps</th><th>Running/Waiting Reduces</th>" +
            (reporter != null ? "<th>CPU</th><th>Memory</th>" : "") +
            "<th>Map Avg Wait Seccond</th>" +
            "<th>Reduce Avg Wait Second</th>" +
            "</tr></thead><tbody>\n");
        List<Pool> pools = new ArrayList<Pool>(poolManager.getPools());
        Collections.sort(pools, new Comparator<Pool>() {
          public int compare(Pool p1, Pool p2) {
            if (p1.isDefaultPool())
              return 1;
            else if (p2.isDefaultPool())
              return -1;
            else return p1.getName().compareTo(p2.getName());
          }});
        int numActivePools = 0;
        int totalInitedTasks = 0;
        int totalMaxInitedTasks = 0;
        int totalRunningMaps = 0;
        int totalWaitingMaps = 0;
        int totalRunningReduces = 0;
        int totalWaitingReduces = 0;
        int totalMinReduces = 0;
        int totalMaxReduces = 0;
        int totalMinMaps = 0;
        int totalMaxMaps = 0;
        int totalRunningJobs = 0;
        long totalMapWaitTime = 0;
        long totalReduceWaitTime = 0;
        long totalNonConfiguredFirstMapWaitTime = 0;
        long totalNonConfiguredFirstReduceWaitTime = 0;
        long totalJobsInNonConfiguredPools = 0;
        int totalReduceTasks = 0;
        int totalMapTasks = 0;
        int totalPrepareJobs = 0;
        double totalCpu = 0;
        double totalMemory = 0;
        for (Pool pool: pools) {
          String poolName = pool.getName();
          if ((poolFilterSet != null) && !poolFilterSet.contains(poolName)) {
            continue;
          }
          int initedTasks = 0;
          int runningMaps = 0;
          int waitingMaps = 0;
          int runningReduces = 0;
          int waitingReduces = 0;
          long poolMapWaitTime = 0;
          long poolReduceWaitTime = 0;
          int poolMapTasks = 0;
          int poolReduceTasks = 0;
          int poolPrepareJobs = 0;
          long poolFirstMapWaitTime = 0;
          long poolFirstReduceWaitTime = 0;
          boolean isConfiguredPool = pool.isConfiguredPool();
          for (JobInProgress job: pool.getJobs()) {
            if (job.getStatus().getRunState() == JobStatus.PREP) {
              poolPrepareJobs += 1;
            }
            if (isConfiguredPool) {
              totalJobsInNonConfiguredPools++;
              totalNonConfiguredFirstMapWaitTime += job.getFirstMapWaitTime();
              totalNonConfiguredFirstReduceWaitTime += job.getFirstReduceWaitTime();
            }
            JobInfo info = scheduler.infos.get(job);
            if (info != null) {
              initedTasks += info.totalInitedTasks;
              runningMaps += info.runningMaps;
              runningReduces += info.runningReduces;
              waitingMaps += info.neededMaps;
              waitingReduces += info.neededReduces;
              poolMapWaitTime += job.getTotalMapWaitTime();
              poolReduceWaitTime += job.getTotalReduceWaitTime();
              poolMapTasks += job.desiredMaps();
              poolReduceTasks += job.desiredReduces();
            }
          }
          double poolMapAverageWaitTime = 0;
          double poolReduceAverageWaitTime = 0;
          if (poolMapTasks != 0) {
            poolMapAverageWaitTime = (double)poolMapWaitTime / poolMapTasks;
            totalMapWaitTime += poolMapWaitTime;
            totalMapTasks += poolMapTasks;
          }
          if (poolReduceTasks != 0) {
            poolReduceAverageWaitTime = (double)poolReduceWaitTime / poolReduceTasks;
            totalReduceWaitTime += poolReduceWaitTime;
            totalReduceTasks += poolReduceTasks;
          }
          int runningJobs = pool.getJobs().size();
          int minMaps = poolManager.getMinSlots(poolName, TaskType.MAP);
          int minReduces = poolManager.getMinSlots(poolName, TaskType.REDUCE);
          int maxMaps = poolManager.getMaxSlots(poolName, TaskType.MAP);
          int maxReduces = poolManager.getMaxSlots(poolName, TaskType.REDUCE);
          int maxInitedTasks = poolManager.getPoolMaxInitedTasks(poolName);
          totalRunningJobs += runningJobs;
          totalInitedTasks += initedTasks;
          totalRunningMaps += runningMaps;
          totalWaitingMaps += waitingMaps;
          totalRunningReduces += runningReduces;
          totalWaitingReduces += waitingReduces;
          totalMinMaps += minMaps;
          totalMinReduces += minReduces;
          if (runningJobs == 0 && minMaps == 0 && minReduces == 0 &&
              maxMaps == Integer.MAX_VALUE && maxReduces == Integer.MAX_VALUE &&
              initedTasks == 0 && runningMaps == 0 && runningReduces == 0) {
            continue;
          }
          numActivePools++;
          out.print("<tr>\n");
          out.printf("<td>%s</td>\n", poolName);
          out.printf("<td>%s</td>\n", runningJobs);
          out.printf("<td>%s</td>\n", poolPrepareJobs);
          out.printf("<td>%s</td>\n", minMaps);
          out.printf("<td>%s</td>\n", minReduces);
          if (maxMaps == Integer.MAX_VALUE) {
            out.printf("<td>-</td>\n");
          } else {
            out.printf("<td>%s</td>\n", maxMaps);
            totalMaxMaps += maxMaps;
          }
          if (maxReduces == Integer.MAX_VALUE) {
            out.printf("<td>-</td>\n");
          } else {
            out.printf("<td>%s</td>\n", maxReduces);
            totalMaxReduces += maxReduces;
          }
          out.printf("<td>%s</td>\n", initedTasks);
          if (maxInitedTasks == Integer.MAX_VALUE) {
            out.printf("<td>-</td>\n");
          } else {
            out.printf("<td>%s</td>\n", maxInitedTasks);
            totalMaxInitedTasks += maxInitedTasks;
          }
          out.printf("<td>%s/%s</td>\n", runningMaps, waitingMaps);
          out.printf("<td>%s/%s</td>\n", runningReduces, waitingReduces);

          // Compute the CPU and memory usage
          double cpuUsage = 0; // in percentage
          double memoryUsage = 0; // in percentage
          if (reporter != null) {
            for (JobInProgress job : pool.getJobs()) {
              double cpu =
                reporter.getJobCpuPercentageOnCluster(job.getJobID());
              double memory =
                reporter.getJobMemPercentageOnCluster(job.getJobID());
              cpuUsage += cpu != ResourceReporter.UNAVAILABLE ? cpu : 0;
              memoryUsage += memory != ResourceReporter.UNAVAILABLE ?
                             memory : 0;
            }
            out.printf("<td>%.1f%%</td>\n", cpuUsage);
            out.printf("<td>%.1f%%</td>\n", memoryUsage);
          }
          totalCpu += cpuUsage;
          totalMemory += memoryUsage;
          totalPrepareJobs += poolPrepareJobs;
          out.printf("<td>%.1f</td>\n", poolMapAverageWaitTime / 1000D);
          out.printf("<td>%.1f</td>\n", poolReduceAverageWaitTime / 1000D);
          out.print("</tr>\n");
        }
        out.print("<tr>\n");
        out.printf("<td>Total</td>\n");
        out.printf("<td>%s</td>\n", totalRunningJobs);
        out.printf("<td>%s</td>\n", totalPrepareJobs);
        out.printf("<td>%s</td>\n", totalMinMaps);
        out.printf("<td>%s</td>\n", totalMinReduces);
        if (totalMaxMaps == 0) {
          out.printf("<td>-</td>\n");
        } else {
          out.printf("<td>%s</td>\n", totalMaxMaps);
        }
        if (totalMaxReduces == 0) {
          out.printf("<td>-</td>\n");
        } else {
          out.printf("<td>%s</td>\n", totalMaxReduces);
        }
        out.printf("<td>%s</td>\n", totalInitedTasks);
        out.printf("<td>%s</td>\n", totalMaxInitedTasks);
        out.printf("<td>%s/%s</td>\n", totalRunningMaps, totalWaitingMaps);
        out.printf("<td>%s/%s</td>\n",
                   totalRunningReduces, totalWaitingReduces);

        if (reporter != null) {
          out.printf("<td>%.1f%%</td>\n", totalCpu);
          out.printf("<td>%.1f%%</td>\n", totalMemory);
        }
        double mapAverageWaitTime = totalMapTasks == 0 ? 0 :
            (double)totalMapWaitTime / totalMapTasks;
        double reduceAverageWaitTime = totalReduceTasks == 0 ? 0 :
            (double)totalReduceWaitTime / totalReduceTasks;
        out.printf("<td>%.1f</td>\n", mapAverageWaitTime);
        out.printf("<td>%.1f</td>\n", reduceAverageWaitTime);
        out.print("</tr>\n");

        out.print("</tbody></table>\n");
        out.printf("<p>Number of active/total pools : %d/%d</p>",
                   numActivePools, pools.size());
        double nonConfiguredAverageFirstMapWaitTime = totalJobsInNonConfiguredPools == 0 ? 0:
            (double)totalNonConfiguredFirstMapWaitTime / totalJobsInNonConfiguredPools;
        double nonConfiguredAverageFirstReduceWaitTime = totalJobsInNonConfiguredPools == 0 ? 0:
            (double)totalNonConfiguredFirstReduceWaitTime / totalJobsInNonConfiguredPools;
        // Non-configured == ad-hoc.
        out.printf("<p>Average first map wait time in ad-hoc pools: %f</p>", nonConfiguredAverageFirstMapWaitTime);
        out.printf("<p>Average first reduce wait time in ad-hoc pools: %f</p>", nonConfiguredAverageFirstReduceWaitTime);
      }
    }
  }

  /**
   * Print a view of running jobs to the given output writer.
   *
   * @param out All html output goes here.
   * @param advancedView Show advanced view if true
   * @param String userFilterSet If not null, only show this set's info
   * @param String poolFilterSet If not null, only show this set's info
   */
  private void showJobs(PrintWriter out, boolean advancedView,
      Set<String> userFilterSet, Set<String> poolFilterSet) {
    ResourceReporter reporter = jobTracker.getResourceReporter();
    out.print("<h2>Running Jobs</h2>\n");
    out.print("<b>Filter</b> "
        + "<input type=\"text\" onkeyup=\"filterTables(this.value)\" id=\"RunningJobsTableFilter\">"
        + "<input type=\"checkbox\" id=\"TimeFilterToggle\" "
          + "onChange=\"filterTables(inputRJF.value)\" checked>Time "
        + "<input type=\"checkbox\" id=\"JobIDFilterToggle\" "
          + "onChange=\"filterTables(inputRJF.value)\" checked>JobID "
        + "<input type=\"checkbox\" id=\"UserFilterToggle\" "
          + "onChange=\"filterTables(inputRJF.value)\" checked>User "
        + "<input type=\"checkbox\" id=\"NameFilterToggle\" "
          + "onChange=\"filterTables(inputRJF.value)\" checked>Name "
        + "<input type=\"checkbox\" id=\"PoolFilterToggle\" "
          + "onChange=\"filterTables(inputRJF.value)\" checked>Pool "
        + "<input type=\"checkbox\" id=\"PrioFilterToggle\" "
          + "onChange=\"filterTables(inputRJF.value)\" checked>Priority"
        + "<br><br>\n");
    out.print("<script type=\"text/javascript\">var inputRJF = "
      + "document.getElementById('RunningJobsTableFilter');</script>");
    out.print("<table border=\"2\" cellpadding=\"5\" cellspacing=\"2\" "
        + "id=\"RunningJobsTable\" class=\"tablesorter\">\n");
    int colsPerTaskType = advancedView ? 6 : 3;
    out.printf("<thead><tr><th rowspan=2>Submitted</th>" +
        "<th rowspan=2>JobID</th>" +
        "<th rowspan=2>User</th>" +
        "<th rowspan=2>Name</th>" +
        "<th rowspan=2>Pool</th>" +
        "<th rowspan=2>Priority</th>" +
        "<td colspan=%d>Maps</td>" +
        "<td colspan=%d>Reduces</td>" +
        ( reporter != null ?
        "<td colspan=2>CPU</td>" +
        "<td colspan=3>MEM</td>" : ""),
        colsPerTaskType, colsPerTaskType);
    out.print("</tr><tr>\n");
    out.print("<th>Finished</th><th>Running</th><th>Fair Share</th>" +
        (advancedView ? "<th>Weight</th><th>Deficit</th><th>minMaps</th>" : ""));
    out.print("<th>Finished</th><th>Running</th><th>Fair Share</th>" +
        (advancedView ? "<th>Weight</th><th>Deficit</th><th>minReduces</th>" : ""));
    if (reporter != null) {
      out.print("<th>Now</th><th>Cumulated</th>" +
                "<th>Now</th><th>Cumulated</th><th>Max/Node</th>");
    }
    out.print("</tr></thead><tbody>\n");
    synchronized (jobTracker) {
      Collection<JobInProgress> runningJobs = getInitedJobs();
      synchronized (scheduler) {
        for (JobInProgress job: runningJobs) {
          JobProfile profile = job.getProfile();
          JobInfo info = scheduler.infos.get(job);
          if (info == null) { // Job finished, but let's show 0's for info
            info = new JobInfo(0);
          }

          // Filter for user and pool filters
          String userName = profile.getUser();
          String poolName = scheduler.getPoolManager().getPoolName(job);
          if ((userFilterSet != null) && !userFilterSet.contains(userName)) {
            continue;
          }
          if ((poolFilterSet != null) && !poolFilterSet.contains(poolName)) {
            continue;
          }

          out.printf("<tr id=\"%s\">\n", profile.getJobID());
          out.printf("<td>%s</td>\n", DATE_FORMAT.format(
                       new Date(job.getStartTime())));
          out.printf("<td><a href=\"jobdetails.jsp?jobid=%s\">%s</a></td>",
                     profile.getJobID(), profile.getJobID());
          out.printf("<td>%s</td>\n", userName);
          out.printf("<td>%s</td>\n", profile.getJobName());
          out.printf("<td>%s</td>\n", generateSelectForPool(poolName));
          out.printf("<td>%s</td>\n", generateSelectForPrio(
                       job.getPriority().toString()));
          out.printf("<td>%d / %d</td><td>%d</td><td>%f</td>\n",
                     job.finishedMaps(), job.desiredMaps(), info.runningMaps,
                     info.mapFairShare);
          if (advancedView) {
            out.print("<td>" + info.mapWeight + "</td>\n");
            out.printf("<td>%s</td>\n", info.neededMaps > 0 ?
                       (info.mapDeficit / 1000) + "s" : "--");
            out.printf("<td>%d</td>\n", info.minMaps);
          }
          out.printf("<td>%d / %d</td><td>%d</td><td>%f</td>\n",
                     job.finishedReduces(), job.desiredReduces(), info.runningReduces,
                     info.reduceFairShare);
          if (advancedView) {
            out.print("<td>" + info.reduceWeight + "</td>\n");
            out.printf("<td>%s</td>\n", info.neededReduces > 0 ?
                       (info.reduceDeficit / 1000) + "s" : "--");
            out.printf("<td>%d</td>\n", info.minReduces);
          }
          if (reporter != null) {
            JobID jobId = profile.getJobID();
            double cpu = reporter.getJobCpuPercentageOnCluster(jobId);
            double mem = reporter.getJobMemPercentageOnCluster(jobId);
            double cpuTime = reporter.getJobCpuCumulatedUsageTime(jobId);
            double memTime = reporter.getJobMemCumulatedUsageTime(jobId);
            double memMax = reporter.getJobMemMaxPercentageOnBox(jobId);
            if (cpu == ResourceReporter.UNAVAILABLE ||
                mem == ResourceReporter.UNAVAILABLE ||
                cpuTime == ResourceReporter.UNAVAILABLE ||
                memTime == ResourceReporter.UNAVAILABLE ||
                memMax == ResourceReporter.UNAVAILABLE) {
              out.printf("<td>-</td><td>-</td><td>-</td><td>-</td><td>-</td>");
              continue;
            }
            out.printf("<td>%.1f%%</td>\n", cpu);
            out.printf("<td>%.1f sec</td>\n", cpuTime / 1000D);
            out.printf("<td>%.1f%%</td>\n", mem);
            out.printf("<td>%.1f sec</td>\n", memTime / 1000D);
            if (memMax > 50) {
              out.printf("<td><font color=\"red\">%.1f%%</font></td>\n",
                         memMax);
            } else {
              out.printf("<td>%.1f%%</td>\n", memMax);
            }
          }
          out.print("</tr>\n");
        }
      }
    }
    out.print("</tbody></table>\n");
  }

  /**
   * Generate a HTML select control with a given list of choices and a given
   * option selected. When the selection is changed, take the user to the
   * <code>submitUrl</code>. The <code>submitUrl</code> can be made to include
   * the option selected -- the first occurrence of the substring
   * <code>&lt;CHOICE&gt;</code> will be replaced by the option chosen.
   */
  private String generateSelect(Iterable<String> choices,
      String selectedChoice, String submitUrl) {
    StringBuilder html = new StringBuilder();
    String id = "select" + lastId++;
    html.append("<select id=\"" + id + "\" name=\"" + id + "\" " +
        "onchange=\"window.location = '" + submitUrl +
        "'.replace('<CHOICE>', document.getElementById('" + id +
        "').value);\">\n");
    for (String choice: choices) {
      html.append(String.format("<option value=\"%s\"%s>%s</option>\n",
          choice, (choice.equals(selectedChoice) ? " selected" : ""), choice));
    }
    html.append("</select>\n");
    return html.toString();
  }

  private String generateSelectForPool (String selectedChoice) {
    return "<span class=\"fake-link\" id=\"DYN_POOL\">" + selectedChoice + "</span>";
  }

  private String generateSelectForPrio (String selectedChoice) {
    return "<span class=\"fake-link\" id=\"DYN_PRIO\">" + selectedChoice + "</span>";
  }

  /**
   * Print the administration form at the bottom of the page, which currently
   * only includes the button for switching between FIFO and Fair Scheduling.
   */
  private void showAdminFormJobComparator(PrintWriter out, boolean advancedView) {
    out.print("<h2>Scheduling Mode</h2>\n");
    String curMode = scheduler.getJobComparator().toString();
    String advParam = advancedView ? "&advanced" : "";
    out.printf("<p>The scheduler is currently using <b>%s mode</b>.",
        generateSelect((Collection<String>)
            Arrays.asList("FAIR,DEFICIT,FIFO".split(",")),
            curMode, "/fairscheduler?setJobComparator=<CHOICE>" + advParam));
  }

  /**
   * Print the administration form at the bottom of the page, which currently
   * only includes the button for switching between FIFO and Fair Scheduling.
   */
  private void showAdminFormLocalityDelay(PrintWriter out, boolean advancedView) {
    out.print("<h2>Locality Delay</h2>\n");
    String advParam = advancedView ? "&advanced" : "";
    long localityDelayRackLocal = scheduler.getLocalityDelayRackLocal();
    long localityDelayNodeLocal = scheduler.getLocalityDelayNodeLocal();
    Collection<String> possibleDelay = Arrays.asList(
      ("0,1000,2000,3000,4000,5000,10000,15000,20000,25000,30000").split(","));
    out.printf("<p>Mapper NODE locality delay = <b>%s milliseconds</b>.",
        generateSelect(possibleDelay, "" + localityDelayNodeLocal,
            "/fairscheduler?setLocalityDelayNodeLocal=<CHOICE>" + advParam));
    out.printf("<p>Mapper RACK locality delay = <b>%s milliseconds</b>.",
        generateSelect(possibleDelay, "" + localityDelayRackLocal,
            "/fairscheduler?setLocalityDelayRackLocal=<CHOICE>" + advParam));
  }

  /**
   * Print the administration form for preemption
   */
  private void showAdminFormPreemption(PrintWriter out, boolean advancedView) {
    out.print("<h2>Task Preemption</h2>\n");
    String advParam = advancedView ? "&advanced" : "";
    out.print(generateSelect(Arrays.asList("On,Off".split(",")),
              scheduler.isPreemptionEnabled() ? "On" : "Off",
              "/fairscheduler?setPreemptionEnabled=<CHOICE>" + advParam));
  }

  /**
   * Print the administration form for the MemBasedLoadManager
   */
  private void showAdminFormMemBasedLoadMgr(PrintWriter out,
                                            boolean advancedView) {
    if (!(loadMgr instanceof MemBasedLoadManager)) {
      return;
    }
    out.print("<h2>Memory Based Scheduling</h2>\n");
    MemBasedLoadManager memLoadMgr = (MemBasedLoadManager)loadMgr;
    Collection<String> possibleThresholds =
      Arrays.asList(("0,1,2,3,4,5,6,7,8,9,10,1000").split(","));
    long reservedMemGB =
            (long)(memLoadMgr.getReservedPhysicalMemoryOnTT() / 1024D + 0.5);
    out.printf("<p>Reserve %s GB memory on one node.",
               generateSelect(possibleThresholds, "" + reservedMemGB,
                              "/fairscheduler?setTtThreshold=<CHOICE>" +
                              (advancedView ? "&advanced" : "")));
  }

  /**
   * Print the cluster resource utilization
   */
  static void showCluster(
      PrintWriter out, boolean advancedView, JobTracker jobTracker) {
    String cluster = "";
    try {
      cluster = JSPUtil.generateClusterResTable(jobTracker);
      if (cluster.equals("")) {
        return;
      }
    } catch (IOException e) {
      return;
    }
    out.print("<h2>Cluster Resource</h2>\n");
    out.print(cluster);
  }

  /**
   * Print the UI that allows us to change the number of tasks assigned per
   * heartbeat.
   */
  private void showNumTaskPerHeartBeatOption(
          PrintWriter out, boolean advancedView) {

    out.print("<h2>Number of Assigned Tasks Per HeartBeat</h2>\n");
    out.printf("<p>Number of map tasks assigned per heartbeat:%s",
                generateSelect(Arrays.asList("1,2,3,4,5,6,7,8,9,10".split(",")),
                               scheduler.getMapPerHeartBeat() + "",
                               "/fairscheduler?setMapPerHeartBeat=<CHOICE>" +
                               (advancedView ? "&advanced" : "")));

    out.printf("<p>Number of reduce tasks assigned per heartbeat:%s",
               generateSelect(Arrays.asList("1,2,3,4,5,6,7,8,9,10".split(",")),
                              scheduler.getReducePerHeartBeat() + "",
                              "/fairscheduler?setReducePerHeartBeat=<CHOICE>" +
                              (advancedView ? "&advanced" : "")));
  }

  /**
   * Obtained all initialized jobs
   */
  private Collection<JobInProgress> getInitedJobs() {
    Collection<JobInProgress> runningJobs = jobTracker.getRunningJobs();
    for (Iterator<JobInProgress> it = runningJobs.iterator(); it.hasNext();) {
      JobInProgress job = it.next();
      if (!job.inited()) {
        it.remove();
      }
    }
    return runningJobs;
  }
}
