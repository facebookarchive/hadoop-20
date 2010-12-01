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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.hadoop.mapred.FairScheduler.JobInfo;
import org.apache.hadoop.mapred.FairScheduler.JobComparator;
import org.apache.hadoop.util.StringUtils;

/**
 * Servlet for displaying fair scheduler information, installed at
 * [job tracker URL]/scheduler when the {@link FairScheduler} is in use.
 * 
 * The main features are viewing each job's task count and fair share, ability
 * to change job priorities and pools from the UI, and ability to switch the
 * scheduler to FIFO mode without restarting the JobTracker if this is required
 * for any reason.
 * 
 * There is also an "advanced" view for debugging that can be turned on by
 * going to [job tracker URL]/scheduler?advanced.
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
    // If the request has a set* param, handle that and redirect to the regular
    // view page so that the user won't resubmit the data if they hit refresh.
    boolean advancedView = request.getParameter("advanced") != null;
    if (request.getParameter("setJobComparator") != null) {
      String newMode = request.getParameter("setJobComparator");
      scheduler.setJobComparator(JobComparator.fromString(newMode));
      response.sendRedirect("/scheduler" + (advancedView ? "?advanced" : ""));
      return;
    }
    if (request.getParameter("setPool") != null) {
      Collection<JobInProgress> runningJobs = jobTracker.getRunningJobs();
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
      response.sendRedirect("/scheduler" + (advancedView ? "?advanced" : ""));
      return;
    }
    if (request.getParameter("setPriority") != null) {
      Collection<JobInProgress> runningJobs = jobTracker.getRunningJobs();      
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
      response.sendRedirect("/scheduler" + (advancedView ? "?advanced" : ""));
      return;
    }
    if (request.getParameter("setTtThreshold") != null) {
      long newThreshold = Long.parseLong(
              request.getParameter("setTtThreshold")) * 1024;
      ((MemBasedLoadManager)loadMgr).setReservedPhysicalMemoryOnTT(newThreshold);
      response.sendRedirect("/scheduler" + (advancedView ? "?advanced" : ""));
      return;
    }
    if (request.getParameter("setLocalityDelayNodeLocal") != null) {
      long delay =
        Long.parseLong(request.getParameter("setLocalityDelayNodeLocal"));
      scheduler.setLocalityDelayNodeLocal(delay);
      response.sendRedirect("/scheduler" + (advancedView ? "?advanced" : ""));
      return;
    }
    if (request.getParameter("setLocalityDelayRackLocal") != null) {
      long delay =
        Long.parseLong(request.getParameter("setLocalityDelayRackLocal"));
      scheduler.setLocalityDelayRackLocal(delay);
      response.sendRedirect("/scheduler" + (advancedView ? "?advanced" : ""));
      return;
    }
    if (request.getParameter("setForEveryone") != null) {
      ((MemBasedLoadManager)loadMgr).setAffectAllUsers(
                request.getParameter("setForEveryone").equals("On") ?
                true : false);
      response.sendRedirect("/scheduler" + (advancedView ? "?advanced" : ""));
      return;
    }
    if (request.getParameter("setMapPerHeartBeat") != null) {
      scheduler.setMapPerHeartBeat(
              Integer.parseInt(request.getParameter("setMapPerHeartBeat")));
      response.sendRedirect("/scheduler" + (advancedView ? "?advanced" : ""));
      return;
    }
    if (request.getParameter("setReducePerHeartBeat") != null) {
      scheduler.setReducePerHeartBeat(
              Integer.parseInt(request.getParameter("setReducePerHeartBeat")));
      response.sendRedirect("/scheduler" + (advancedView ? "?advanced" : ""));
      return;
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
    out.print("</head><body>\n");
    out.printf("<h1><a href=\"/jobtracker.jsp\">%s</a> " + 
        "Job Scheduler Administration</h1>\n", hostname);
    showCluster(out, advancedView);
    showPools(out, advancedView);
    showJobs(out, advancedView);
    showAdminFormMemBasedLoadMgr(out, advancedView);
    showAdminFormLocalityDelay(out, advancedView);
    showNumTaskPerHeartBeatOption(out, advancedView);
    showAdminFormJobComparator(out, advancedView);
    out.print("</body></html>\n");
    out.close();

    // Flush our buffer to the real servlet output
    OutputStream servletOut = response.getOutputStream();
    baos.writeTo(servletOut);
    servletOut.close();
  }

  /**
   * Print a view of pools to the given output writer.
   */
  private void showPools(PrintWriter out, boolean advancedView) {
    ResourceReporter reporter = jobTracker.getResourceReporter();
    synchronized (jobTracker) {
      synchronized(scheduler) {
        PoolManager poolManager = scheduler.getPoolManager();
        out.print("<h2>Active Pools</h2>\n");
        out.print("<table border=\"2\" cellpadding=\"5\" cellspacing=\"2\">\n");
        out.print("<tr><th>Pool</th><th>Running Jobs</th>" +
            "<th>Min Maps</th><th>Min Reduces</th>" +
            "<th>Max Maps</th><th>Max Reduces</th>" +
            "<th>Running Maps</th><th>Running Reduces</th>" +
            (reporter != null ? "<th>CPU</th><th>Memory</th>" : "") +
            "<th>Avg Wait Time(sec)</th></tr>\n");
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
        for (Pool pool: pools) {
          int runningMaps = 0;
          int runningReduces = 0;
          long[] totalWaitTimeOfTasks = new long[1];
          long[] numTasks = new long[1];
          totalWaitTimeOfTasks[0] = 0;
          numTasks[0] = 0;
          long avgWaitTime = 0;
          long now = System.currentTimeMillis();
          for (JobInProgress job: pool.getJobs()) {
            JobInfo info = scheduler.infos.get(job);
            if (info != null) {
              runningMaps += info.runningMaps;
              runningReduces += info.runningReduces;

              // calculate the wait time of all tasks. Do not use killed tasks.
              // The wait time of reducers is not a precise reflection of the slowness of the
              // cluster because we start scheduling reducers only when a certain percentage
              // of maps are completed.
              calculateWaitTime(job, jobTracker.getMapTaskReports(job.getJobID()),
                                totalWaitTimeOfTasks, numTasks, now);
              calculateWaitTime(job, jobTracker.getReduceTaskReports(job.getJobID()),
                                totalWaitTimeOfTasks, numTasks, now);
              calculateWaitTime(job, jobTracker.getSetupTaskReports(job.getJobID()),
                                totalWaitTimeOfTasks, numTasks, now);
            }
          }
          if (numTasks[0] != 0) {
            avgWaitTime = totalWaitTimeOfTasks[0]/numTasks[0];
          }
          String poolName = pool.getName();
          int runningJobs = pool.getJobs().size();
          int minMaps = poolManager.getAllocation(poolName, TaskType.MAP);
          int minReduces = poolManager.getAllocation(poolName, TaskType.REDUCE);
          int maxMaps = poolManager.getMaxSlots(poolName, TaskType.MAP);
          int maxReduces = poolManager.getMaxSlots(poolName, TaskType.REDUCE);
          if (runningJobs == 0 && minMaps == 0 && minReduces == 0 &&
              maxMaps == Integer.MAX_VALUE && maxReduces == Integer.MAX_VALUE &&
              runningMaps == 0 && runningReduces == 0) {
            continue;
          }
          numActivePools++;
          out.print("<tr>\n");
          out.printf("<td>%s</td>\n", poolName);
          out.printf("<td>%s</td>\n", runningJobs);
          out.printf("<td>%s</td>\n", minMaps);
          out.printf("<td>%s</td>\n", minReduces);
          if (maxMaps == Integer.MAX_VALUE) {
            out.printf("<td>-</td>\n");
          } else {
            out.printf("<td>%s</td>\n", maxMaps);
          }
          if (maxReduces == Integer.MAX_VALUE) {
            out.printf("<td>-</td>\n");
          } else {
            out.printf("<td>%s</td>\n", maxReduces);
          }
          out.printf("<td>%s</td>\n", runningMaps);
          out.printf("<td>%s</td>\n", runningReduces);
          
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
          out.printf("<td>%s</td>\n", avgWaitTime/1000D);
          out.print("</tr>\n");
        }
        out.print("</table>\n");
	      out.printf("<p>Number of active/total pools : %d/%d</p>",
	                 numActivePools, pools.size());
      }
    }
  }

  /**
   * Calculate the total wait time of all tasks
   */
  private void calculateWaitTime(JobInProgress job, TaskReport[] reports, 
                                 long[] totalWaitTimeOfTasks, long numTasks[], long now) {
    long aggr = 0;
    long count = 0;
    for (int i = 0; i < reports.length; i++) {
      TaskReport report = reports[i];
      if (reports[i].getCurrentStatus() == TIPStatus.PENDING) {
        aggr += (now - job.getStartTime());
        count++;
      } else if (reports[i].getCurrentStatus() == TIPStatus.COMPLETE ||
    		     reports[i].getCurrentStatus() == TIPStatus.RUNNING) {
    	aggr += (report.getStartTime() - job.getStartTime());
        count++;
      }
    }
    totalWaitTimeOfTasks[0] += aggr;
    numTasks[0] += count;
  } 

  /**
   * Print a view of running jobs to the given output writer.
   */
  private void showJobs(PrintWriter out, boolean advancedView) {
    ResourceReporter reporter = jobTracker.getResourceReporter();
    out.print("<h2>Running Jobs</h2>\n");
    out.print("<table border=\"2\" cellpadding=\"5\" cellspacing=\"2\">\n");
    int colsPerTaskType = advancedView ? 6 : 3;
    out.printf("<tr><th rowspan=2>Submitted</th>" + 
        "<th rowspan=2>JobID</th>" +
        "<th rowspan=2>User</th>" +
        "<th rowspan=2>Name</th>" +
        "<th rowspan=2>Pool</th>" +
        "<th rowspan=2>Priority</th>" +
        "<th colspan=%d>Maps</th>" +
        "<th colspan=%d>Reduces</th>" +
        ( reporter != null ?
        "<th colspan=2>CPU</th>" +
        "<th colspan=3>MEM</th>" : ""),
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
    out.print("</tr>\n");
    synchronized (jobTracker) {
      Collection<JobInProgress> runningJobs = jobTracker.getRunningJobs();
      synchronized (scheduler) {
        for (JobInProgress job: runningJobs) {
          JobProfile profile = job.getProfile();
          JobInfo info = scheduler.infos.get(job);
          if (info == null) { // Job finished, but let's show 0's for info
            info = new JobInfo();
          }
          out.print("<tr>\n");
          out.printf("<td>%s</td>\n", DATE_FORMAT.format(
                       new Date(job.getStartTime())));
          out.printf("<td><a href=\"jobdetails.jsp?jobid=%s\">%s</a></td>",
                     profile.getJobID(), profile.getJobID());
          out.printf("<td>%s</td>\n", profile.getUser());
          out.printf("<td>%s</td>\n", profile.getJobName());
          out.printf("<td>%s</td>\n", generateSelect(
                       scheduler.getPoolManager().getPoolNames(),
                       scheduler.getPoolManager().getPoolName(job),
                       "/scheduler?setPool=<CHOICE>&jobid=" + profile.getJobID() +
                       (advancedView ? "&advanced" : "")));
          out.printf("<td>%s</td>\n", generateSelect(
                       Arrays.asList(new String[]
                         {"VERY_LOW", "LOW", "NORMAL", "HIGH", "VERY_HIGH"}),
                       job.getPriority().toString(),
                       "/scheduler?setPriority=<CHOICE>&jobid=" + profile.getJobID() +
                       (advancedView ? "&advanced" : "")));
          out.printf("<td>%d / %d</td><td>%d</td><td>%8.1f</td>\n",
                     job.finishedMaps(), job.desiredMaps(), info.runningMaps,
                     info.mapFairShare);
          if (advancedView) {
            out.printf("<td>%8.1f</td>\n", info.mapWeight);
            out.printf("<td>%s</td>\n", info.neededMaps > 0 ?
                       (info.mapDeficit / 1000) + "s" : "--");
            out.printf("<td>%d</td>\n", info.minMaps);
          }
          out.printf("<td>%d / %d</td><td>%d</td><td>%8.1f</td>\n",
                     job.finishedReduces(), job.desiredReduces(), info.runningReduces,
                     info.reduceFairShare);
          if (advancedView) {
            out.printf("<td>%8.1f</td>\n", info.reduceWeight);
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
    out.print("</table>\n");
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
            curMode, "/scheduler?setJobComparator=<CHOICE>" + advParam));
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
            "/scheduler?setLocalityDelayNodeLocal=<CHOICE>" + advParam));
    out.printf("<p>Mapper RACK locality delay = <b>%s milliseconds</b>.",
        generateSelect(possibleDelay, "" + localityDelayRackLocal,
            "/scheduler?setLocalityDelayRackLocal=<CHOICE>" + advParam));
  }

  /**
   * Print the administration form for the MemBasedLoadManager which contains
   * select boxes for selecting thresholds and the affected user groups.
   */
  private void showAdminFormMemBasedLoadMgr(PrintWriter out,
                                            boolean advancedView) {
    if (!(loadMgr instanceof MemBasedLoadManager)) {
      return;
    }
    out.print("<h2>Memory Based Scheduling</h2>\n");
    MemBasedLoadManager memLoadMgr = (MemBasedLoadManager)loadMgr;
    Collection<String> possibleThresholds =
      Arrays.asList(("1,2,3,4,5,6,7,8,9,10,1000").split(","));

    out.printf("<p>Enable memory limiting: %s",
               generateSelect((Collection<String>)Arrays.asList("On,Off".split(",")),
                              memLoadMgr.isAffectAllUsers() ? "On" : "Off",
                              "/scheduler?setForEveryone=<CHOICE>" +
                              (advancedView ? "&advanced" : "")));
    long reservedMemGB = 
            (long)(memLoadMgr.getReservedPhysicalMemoryOnTT() / 1024D + 0.5);
    out.printf("<p>Reserve %s GB memory on one node.",
               generateSelect(possibleThresholds, "" + reservedMemGB,
                              "/scheduler?setTtThreshold=<CHOICE>" +
                              (advancedView ? "&advanced" : "")));
  }

  /**
   * Print the cluster resource utilization
   */
  private void showCluster(
          PrintWriter out, boolean advancedView) {
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
                               "/scheduler?setMapPerHeartBeat=<CHOICE>" +
                               (advancedView ? "&advanced" : "")));

    out.printf("<p>Number of reduce tasks assigned per heartbeat:%s",
               generateSelect(Arrays.asList("1,2,3,4,5,6,7,8,9,10".split(",")),
                              scheduler.getReducePerHeartBeat() + "",
                              "/scheduler?setReducePerHeartBeat=<CHOICE>" +
                              (advancedView ? "&advanced" : "")));
  }
}
