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
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.hadoop.mapred.FairSchedulerMetricsInst.AdmissionControlData;
import org.apache.hadoop.util.StringUtils;

/**
 * Servlet for displaying admission control fair scheduler information,
 * installed at [job tracker URL]/fairscheduleradmissioncontrol when the
 * {@link FairScheduler} is in use.  It is linked off of the
 * [job tracker URL]/fairscheduler page.
 *
 * The main features are viewing jobs that are not yet admitted and why.  This
 * page should generate a table of the waiting jobs and reasons for waiting.
 */
public class FairSchedulerAdmissionControlServlet extends HttpServlet {
  private static final long serialVersionUID = 9104070533067306659L;
  private static final DateFormat DATE_FORMAT =
    new SimpleDateFormat("MMM dd, HH:mm");

  // This object obtain the resource utilization information
  private FairScheduler scheduler;
  private JobTracker jobTracker;

  @Override
  public void init() throws ServletException {
    super.init();
    ServletContext servletContext = this.getServletContext();
    this.scheduler = (FairScheduler) servletContext.getAttribute("scheduler");
    this.jobTracker = (JobTracker) scheduler.taskTrackerManager;
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
          FairSchedulerAdmissionControlServlet.class + " throws exception",
          ioe);
      throw ioe;
    } catch (ServletException se) {
      FairScheduler.LOG.error(
          FairSchedulerAdmissionControlServlet.class + " throws exception",
          se);
      throw se;
    } catch (Throwable t) {
      FairScheduler.LOG.error(
          FairSchedulerAdmissionControlServlet.class + " throws exception",
          t);
      throw new RuntimeException(t);
    }
  }

  public void doActualGet(HttpServletRequest request,
      HttpServletResponse response) throws ServletException, IOException {
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
    out.print("<script type=\"text/javascript\" " +
        "src=\"/static/dynamic-selector.js\"></script>\n");
    out.print("<script type=\"text/javascript\" " +
        "src=\"/static/tablefilter.js\"></script>\n");
    out.print("<script type=\"text/javascript\">");
    out.print("function init() {\n");
    out.print("  var table=document.getElementById('NotAdmittedJobsTable');\n");
    out.print("  var startRow = 1; // Rows 0 is a header row\n");
    out.print("  var filteredColumns = [" +
        "new ColumnSearchToggle(0, 'SubmittedTimeFilterToggle')," +
        "new ColumnSearchToggle(1, 'JobIDFilterToggle')," +
        "new ColumnSearchToggle(2, 'UserFilterToggle')," +
        "new ColumnSearchToggle(4, 'PoolFilterToggle')," +
        "new ColumnSearchToggle(5, 'PrioFilterToggle')];\n\n");
    out.print("addTable(table, startRow, filteredColumns);\n");
    out.print("}\nwindow.onload=init;</script>\n");
    out.print("<style type=\"text/css\">" +
        ".fake-link {color: blue; " +
        "text-decoration: underline; " +
        "cursor: pointer;}</style>\n");
    out.print("</head><body>\n");
    out.printf("<h1><a href=\"/jobtracker.jsp\">%s</a> " +
        "Job Scheduler Administration</h1>\n", hostname);
    AdmissionControlData admissionControlData =
        scheduler.getJobInitializer().getAdmissionControlData();
    out.printf("<b>Cluster-Wide Soft Task Limit :</b> %d<br>",
        admissionControlData.getSoftTaskLimit());
    out.printf("<b>Cluster-Wide Hard Task Limit :</b> %d<br>",
        admissionControlData.getHardTaskLimit());
    out.printf("<b>Cluster-Wide Total Tasks:</b> %d<br>",
        admissionControlData.getTotalTasks());

    if (admissionControlData.getHardTaskLimit() <
        admissionControlData.getTotalTasks()) {
      out.print("<b style=\"color:#FF0000\">" +
          "Cluster is now in hard admission control.  All new jobs will " +
          "have to wait until tasks clear before admission.</b><br>");
    } else if (admissionControlData.getSoftTaskLimit() <
        admissionControlData.getTotalTasks()) {
      out.print("<b style=\"color:#FF6600\">" +
        "Cluster is now in soft admission control.  Only jobs submitted to " +
        "SLA pools that also meet the SLA pool limits will be " +
        "admitted.</b><br>");
    }

    FairSchedulerServlet.printFilterInfo(
        out, poolFilter, userFilter, "fairscheduleradmissioncontrol");
    FairSchedulerServlet.showCluster(out, false, jobTracker);
    showJobsNotAdmitted(out, userFilterSet, poolFilterSet);
    out.print("</body></html>\n");
    out.close();

    // Flush our buffer to the real servlet output
    OutputStream servletOut = response.getOutputStream();
    baos.writeTo(servletOut);
    servletOut.close();
  }

  /**
   * Print a view of not admitted jobs to the given output writer.
   * @param out Where to dump the oiutput
   * @param userFilterSet Only show jobs from these users if not null
   * @param poolFilterSet Only show jobs from these pools if not null
   */
  private void showJobsNotAdmitted(
      PrintWriter out, Set<String> userFilterSet, Set<String> poolFilterSet) {
    out.print("<h2>Not Admitted Jobs</h2>\n");
    out.print("<b>Filter</b> " +
        "<input type=\"text\" onkeyup=\"filterTables(this.value)\" " +
        "id=\"NotAdmittedJobsTableFilter\">" +
        "<input type=\"checkbox\" id=\"SubmittedTimeFilterToggle\" " +
        "onChange=\"filterTables(inputRJF.value)\" checked>Submitted Time " +
        "<input type=\"checkbox\" id=\"JobIDFilterToggle\" " +
        "onChange=\"filterTables(inputRJF.value)\" checked>JobID " +
        "<input type=\"checkbox\" id=\"UserFilterToggle\" " +
        "onChange=\"filterTables(inputRJF.value)\" checked>User " +
        "<input type=\"checkbox\" id=\"PoolFilterToggle\" " +
        "onChange=\"filterTables(inputRJF.value)\" checked>Pool " +
        "<input type=\"checkbox\" id=\"PrioFilterToggle\" " +
        "onChange=\"filterTables(inputRJF.value)\" checked>Priority" +
        "<br><br>\n");
    out.print("<script type=\"text/javascript\">var inputRJF = " +
        "document.getElementById('NotAdmittedJobsTableFilter');</script>");
    out.print("<table border=\"2\" cellpadding=\"5\" cellspacing=\"2\" " +
        "id=\"NotAdmittedJobsTable\" class=\"tablesorter\">\n");
    out.printf("<thead><tr>" +
        "<th>Submitted Time</th>" +
        "<th>JobID</th>" +
        "<th>User</th>" +
        "<th>Pool</th>" +
        "<th>Priority</th>" +
        "<th>Reason</th>" +
        "<th>Job Position</th>" +
        "<th>ETA to Admission (secs)</th>");
    out.print("</tr></thead><tbody>\n");
    Collection<NotAdmittedJobInfo> notAdmittedJobInfos =
        scheduler.getNotAdmittedJobs();
    for (NotAdmittedJobInfo jobInfo : notAdmittedJobInfos) {
      if ((userFilterSet != null) &&
          !userFilterSet.contains(jobInfo.getUser())) {
          continue;
      }
      if ((poolFilterSet != null) &&
          !poolFilterSet.contains(jobInfo.getPool())) {
        continue;
      }

      out.printf("<tr id=\"%s\">\n", jobInfo.getJobName());
      out.printf("<td>%s</td>\n", DATE_FORMAT.format(jobInfo.getStartDate()));
      out.printf("<td><a href=\"jobdetails.jsp?jobid=%s\">%s</a></td>",
          jobInfo.getJobName(), jobInfo.getJobName());
      out.printf("<td>%s</td>\n", jobInfo.getUser());
      out.printf("<td>%s</td>\n", jobInfo.getPool());
      out.printf("<td>%s</td>\n", jobInfo.getPriority());
      out.printf("<td>%s</td>\n", jobInfo.getReason());
      out.printf("<td>%d</td>\n", jobInfo.getHardAdmissionPosition());
      out.printf("<td>%d</td>\n",
          jobInfo.getEstimatedHardAdmissionEntranceSecs());
      out.print("</tr>\n");
    }
    out.print("</tbody></table>\n");
  }
}
