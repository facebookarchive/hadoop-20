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
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.Collection;
import java.util.Date;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.jsp.JspWriter;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapred.JobHistory.JobInfo;
import org.apache.hadoop.mapred.JobTracker.RetireJobInfo;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.util.ServletUtil;
import org.apache.hadoop.util.StringUtils;

public class JSPUtil {
  private static final String PRIVATE_ACTIONS_KEY = "webinterface.private.actions";

  public static final Configuration conf = new Configuration();

  //LRU based cache
  private static final Map<String, JobInfo> jobHistoryCache =
    new LinkedHashMap<String, JobInfo>();

  private static final int CACHE_SIZE =
    conf.getInt("mapred.job.tracker.jobhistory.lru.cache.size", 5);

  private static final Log LOG = LogFactory.getLog(JSPUtil.class);
  /**
   * Method used to process the request from the job page based on the
   * request which it has received. For example like changing priority.
   *
   * @param request HTTP request Object.
   * @param response HTTP response object.
   * @param tracker {@link JobTracker} instance
   * @throws IOException
   */
  public static void processButtons(HttpServletRequest request,
      HttpServletResponse response, JobTracker tracker) throws IOException {

    if (conf.getBoolean(PRIVATE_ACTIONS_KEY, false)
        && request.getParameter("killJobs") != null) {
      String[] jobs = request.getParameterValues("jobCheckBox");
      if (jobs != null) {
        for (String job : jobs) {
          tracker.killJob(JobID.forName(job));
        }
      }
    }

    if (conf.getBoolean(PRIVATE_ACTIONS_KEY, false) &&
          request.getParameter("changeJobPriority") != null) {
      String[] jobs = request.getParameterValues("jobCheckBox");

      if (jobs != null) {
        JobPriority jobPri = JobPriority.valueOf(request
            .getParameter("setJobPriority"));

        for (String job : jobs) {
          tracker.setJobPriority(JobID.forName(job), jobPri);
        }
      }
    }
  }

  /**
   * Method used to generate the Job table for Job pages.
   *
   * @param label display heading to be used in the job table.
   * @param jobs vector of jobs to be displayed in table.
   * @param refresh refresh interval to be used in jobdetails page.
   * @param rowId beginning row id to be used in the table.
   * @return
   * @throws IOException
   */
  public static String generateJobTable(String label, Collection<JobInProgress> jobs
      , int refresh, int rowId) throws IOException {
    boolean isRunning = label.equals("Running");
    boolean isModifiable =
        isRunning && conf.getBoolean(PRIVATE_ACTIONS_KEY, false);
    StringBuffer sb = new StringBuffer();

    sb.append("<table border=\"1\" cellpadding=\"5\" cellspacing=\"0\" class=\"tablesorter\">\n");

    if (jobs.size() > 0) {
      if (isModifiable) {
        sb.append("<thead><form action=\"/jobtracker.jsp\" onsubmit=\"return confirmAction();\" method=\"POST\">");
        sb.append("<tr>");
        sb.append("<td><input type=\"Button\" onclick=\"selectAll()\" " +
        		"value=\"Select All\" id=\"checkEm\"></td>");
        sb.append("<td>");
        sb.append("<input type=\"submit\" name=\"killJobs\" value=\"Kill Selected Jobs\">");
        sb.append("</td>");
        sb.append("<td><nobr>");
        sb.append("<select name=\"setJobPriority\">");

        for (JobPriority prio : JobPriority.values()) {
          sb.append("<option"
              + (JobPriority.NORMAL == prio ? " selected=\"selected\">" : ">")
              + prio + "</option>");
        }

        sb.append("</select>");
        sb.append("<input type=\"submit\" name=\"changeJobPriority\" " +
        		"value=\"Change\">");
        sb.append("</nobr></td>");
        sb.append("<td colspan=\"10\">&nbsp;</td>");
        sb.append("</tr>");
        sb.append("<th>&nbsp;</th>");
      } else {
        sb.append("<thead><tr>");
      }

      int totalMaps = 0;
      int comMaps = 0;
      int totalRunningMaps = 0;
      int totalReduces = 0;
      int comReduces = 0;
      int totalRunningReduces = 0;
      for (Iterator<JobInProgress> it = jobs.iterator(); it.hasNext(); ) {
        JobInProgress job = it.next();
        totalMaps += job.desiredMaps();
        totalReduces += job.desiredReduces();
        comMaps += job.finishedMaps();
        comReduces += job.finishedReduces();
        if (isRunning) {
          totalRunningMaps += job.runningMaps();
          totalRunningReduces += job.runningReduces();
        }
      }

      sb.append("<th><b>Jobid</b></th><th><b>Priority" +
      		"</b></th><th><b>User</b></th>");
      sb.append("<th><b>Name</b></th>");
      sb.append("<th><b>Map % Complete</b></th>");
      sb.append("<th><b>Map Total " + totalMaps + "</b></th>");
      sb.append("<th><b>Maps Completed " + comMaps + "</b></th>");
      if (isRunning) {
        sb.append("<th><b>Maps Running " + totalRunningMaps + "</b></th>");
      }
      sb.append("<th><b>Reduce % Complete</b></th>");
      sb.append("<th><b>Reduce Total " + totalReduces + "</b></th>");
      sb.append("<th><b>Reduces Completed " + comReduces + "</b></th>");
      if (isRunning) {
        sb.append("<th><b>Reduces Running " + totalRunningReduces + "</b></th>");
      }
      sb.append("<th><b>Job Scheduling Information</b></th>");
      sb.append("</tr></thead><tbody>\n");
      for (Iterator<JobInProgress> it = jobs.iterator(); it.hasNext(); ++rowId) {
        JobInProgress job = it.next();
        JobProfile profile = job.getProfile();
        JobStatus status = job.getStatus();
        JobID jobid = profile.getJobID();

        int desiredMaps = job.desiredMaps();
        int desiredReduces = job.desiredReduces();
        int completedMaps = job.finishedMaps();
        int completedReduces = job.finishedReduces();
        String runningMapTableData =
            (isRunning) ? job.runningMaps() + "</td><td>" : "";
        String runningReduceTableData =
            (isRunning) ? job.runningReduces() + "</td><td>" : "";

        String name = profile.getJobName();
        String abbreviatedName
            = (name.length() > 76 ? name.substring(0,76) + "..." : name);

        String jobpri = job.getPriority().toString();
        String schedulingInfo = job.getStatus().getSchedulingInfo();

        if (isModifiable) {
          sb.append("<tr><td><input TYPE=\"checkbox\" " +
          		"onclick=\"checkButtonVerbage()\" " +
          		"name=\"jobCheckBox\" value="
                  + jobid + "></td>");
        } else {
          sb.append("<tr>");
        }

        sb.append("<td id=\"job_" + rowId
            + "\"><a href=\"jobdetails.jsp?jobid=" + jobid + "&refresh="
            + refresh + "\">" + jobid + "</a></td>" + "<td id=\"priority_"
            + rowId + "\">" + jobpri + "</td>" + "<td id=\"user_" + rowId
            + "\">" + profile.getUser() + "</td>" + "<td id=\"name_" + rowId
            + "\">" + ("".equals(abbreviatedName) ? "&nbsp;" : abbreviatedName)
            + "</td>" + "<td>"
            + StringUtils.formatPercent(status.mapProgress(), 2)
            + ServletUtil.percentageGraph(status.mapProgress() * 100, 80)
            + "</td><td>" + desiredMaps + "</td><td>" + completedMaps
            + "</td><td>" + runningMapTableData
            + StringUtils.formatPercent(status.reduceProgress(), 2)
            + ServletUtil.percentageGraph(status.reduceProgress() * 100, 80)
            + "</td><td>" + desiredReduces + "</td><td> " + completedReduces
            + "</td><td>" + runningReduceTableData + schedulingInfo
            + "</td></tr>\n");
      }
      if (isModifiable) {
        sb.append("</form>\n");
      }
      sb.append("</tbody>");
    } else {
      sb.append("<tr><td align=\"center\" colspan=\"8\"><i>none</i>" +
      		"</td></tr>\n");
    }
    sb.append("</table>\n");

    return sb.toString();
  }

  /**
   * Given jobId, resolve the link to jobdetailshistory.jsp
   * @param tracker JobTracker
   * @param jobId JobID
   * @return the link to the page jobdetailshistory.jsp for the job
   */
  public static String getJobDetailsHistoryLink(JobTracker tracker,
                                                String jobId) {
    RetireJobInfo info = tracker.retireJobs.get(JobID.forName(jobId));
    String historyFileUrl = getHistoryFileUrl(info);
    String result =  (historyFileUrl == null ? "" :
              "jobdetailshistory.jsp?jobid=" + jobId + "&logFile=" +
              historyFileUrl);
    return result;
  }

  /**
   * Given jobId, taskid resolve the link to taskdetailshistory.jsp
   * @param tracker JobTracker
   * @param jobId JobID
   * @param tid String
   * @return the link to the page jobdetailshistory.jsp for the job
   */
  public static String getTaskDetailsHistoryLink(JobTracker tracker,
                                                 String jobId,
                                                 String tid) {
    RetireJobInfo info = tracker.retireJobs.get(JobID.forName(jobId));
    String historyFileUrl = getHistoryFileUrl(info);
    String result =  (historyFileUrl == null ? "" :
              "taskdetailshistory.jsp?jobid=" + jobId + "&logFile=" +
              historyFileUrl + "&taskid=" + tid);
    return result;
  }

  /**
   * Obtain history file URL from RetireJobInfo
   * @param info RetireJobInfo
   * @return corresponding history file url, null if cannot creat one
   */
  private static String getHistoryFileUrl(RetireJobInfo info) {
    String historyFile = info.getHistoryFile();
    String historyFileUrl = null;
    if (historyFile != null && !historyFile.equals("")) {
      try {
        historyFileUrl = URLEncoder.encode(info.getHistoryFile(), "UTF-8");
      } catch (UnsupportedEncodingException e) {
        LOG.warn("Can't create history url ", e);
      }
    }
    return historyFileUrl;
  }

  @SuppressWarnings("unchecked")
  public static String generateRetiredJobTable(JobTracker tracker, int rowId)
    throws IOException {

    StringBuffer sb = new StringBuffer();
    sb.append("<table border=\"1\" cellpadding=\"5\" cellspacing=\"0\" class=\"tablesorter\">\n");

    Iterator<RetireJobInfo> iterator =
      tracker.retireJobs.getAll().descendingIterator();
    if (!iterator.hasNext()) {
      sb.append("<tr><th align=\"center\" colspan=\"8\"><i>none</i>" +
      "</th></tr>\n");
    } else {
      sb.append("<thead><tr>");

      sb.append("<th><b>Jobid</b></th>");
      sb.append("<th><b>Priority</b></th>");
      sb.append("<th><b>User</b></th>");
      sb.append("<th><b>Name</b></th>");
      sb.append("<th><b>State</b></th>");
      sb.append("<th><b>Start Time</b></th>");
      sb.append("<th><b>Finish Time</b></th>");
      sb.append("<th><b>Map % Complete</b></th>");
      sb.append("<th><b>Reduce % Complete</b></th>");
      sb.append("<th><b>Job Scheduling Information</b></th>");
      sb.append("</tr></thead><tbody>\n");
      for (int i = 0; i < 100 && iterator.hasNext(); i++) {
        RetireJobInfo info = iterator.next();
        String historyFileUrl = getHistoryFileUrl(info);
        sb.append("<tr>");

        String name = info.profile.getJobName();
        String abbreviatedName
            = (name.length() > 76 ? name.substring(0,76) + "..." : name);

        sb.append(
            "<td id=\"job_" + rowId + "\">" +

              (historyFileUrl == null ? "" :
              "<a href=\"jobdetailshistory.jsp?jobid=" +
              info.status.getJobId() + "&logFile=" + historyFileUrl + "\">") +

              info.status.getJobId() + "</a></td>" +

            "<td id=\"priority_" + rowId + "\">" +
              info.status.getJobPriority().toString() + "</td>" +
            "<td id=\"user_" + rowId + "\">" + info.profile.getUser()
              + "</td>" +
            "<td id=\"name_" + rowId + "\">" + abbreviatedName
              + "</td>" +
            "<td>" + JobStatus.getJobRunState(info.status.getRunState())
              + "</td>" +
            "<td>" + new Date(info.status.getStartTime()) + "</td>" +
            "<td>" + new Date(info.finishTime) + "</td>" +

            "<td>" + StringUtils.formatPercent(info.status.mapProgress(), 2)
            + ServletUtil.percentageGraph(info.status.mapProgress() * 100, 80) +
              "</td>" +

            "<td>" + StringUtils.formatPercent(info.status.reduceProgress(), 2)
            + ServletUtil.percentageGraph(
               info.status.reduceProgress() * 100, 80) +
              "</td>" +

            "<td>" + info.status.getSchedulingInfo() + "</td>" +

            "</tr>\n");
        rowId++;
      }
      sb.append("</tbody>");
    }
    sb.append("</table>\n");
    return sb.toString();
  }
  
  public static void cleanJobInfo(String jobId) {
    synchronized(jobHistoryCache) {
      jobHistoryCache.remove(jobId);
    }
  }

  public static JobInfo getJobInfo(HttpServletRequest request, FileSystem fs)
      throws IOException {
    String jobid = request.getParameter("jobid");
    String logFile = request.getParameter("logFile");
    synchronized(jobHistoryCache) {
      JobInfo jobInfo = jobHistoryCache.remove(jobid);
      if (jobInfo == null) {
        jobInfo = new JobHistory.JobInfo(jobid);
        LOG.info("Loading Job History file "+jobid + ".   Cache size is " +
            jobHistoryCache.size());
        DefaultJobHistoryParser.parseJobTasks( logFile, jobInfo, fs) ;
      }
      jobHistoryCache.put(jobid, jobInfo);
      if (jobHistoryCache.size() > CACHE_SIZE) {
        Iterator<Map.Entry<String, JobInfo>> it =
          jobHistoryCache.entrySet().iterator();
        String removeJobId = it.next().getKey();
        it.remove();
        LOG.info("Job History file removed form cache "+removeJobId);
      }
      return jobInfo;
    }
  }

  @SuppressWarnings("unchecked")
  public static void generateRetiredJobXml(JspWriter out, JobTracker tracker, int rowId)
      throws IOException {

    Iterator<RetireJobInfo> iterator =
      tracker.retireJobs.getAll().descendingIterator();

    for (int i = 0; i < 100 && iterator.hasNext(); i++) {
      RetireJobInfo info = iterator.next();
      JobStatus status = info.status;
      StringBuilder sb = new StringBuilder();
      sb.append("<retired_job rowid=\"" + rowId + "\" jobid=\"" + status.getJobId() + "\">");
      sb.append("<jobid>" + status.getJobId() + "</jobid>");
      sb.append("<history_url>jobdetailshistory.jsp?jobid=" + status.getJobId()
          + "&amp;logFile="
          + URLEncoder.encode(info.getHistoryFile().toString(), "UTF-8")
          + "</history_url>");
      sb.append("<priority>" + status.getJobPriority().toString()
          + "</priority>");
      sb.append("<user>" + info.profile.getUser() + "</user>");
      sb.append("<name>" + info.profile.getJobName() + "</name>");
      sb.append("<run_state>" + JobStatus.getJobRunState(status.getRunState())
          + "</run_state>");
      sb.append("<start_time>" + new Date(status.getStartTime())
          + "</start_time>");
      sb.append("<finish_time>" + new Date(info.finishTime)
          + "</finish_time>");
      sb.append("<map_complete>" + StringUtils.formatPercent(
          status.mapProgress(), 2) + "</map_complete>");
      sb.append("<reduce_complete>" + StringUtils.formatPercent(
          status.reduceProgress(), 2) + "</reduce_complete>");
      sb.append("<scheduling_info>" + status.getSchedulingInfo() + "</scheduling_info>");
      sb.append("</retired_job>\n");
      out.write(sb.toString());
      rowId++;
    }
  }

  /**
   * Method used to generate the cluster resource utilization table
   */
  public static String generateClusterResTable(JobTracker tracker)
    throws IOException {
    ResourceReporter reporter = tracker.getResourceReporter();
    if (reporter == null) {
      return "";
    }
    StringBuffer sb = new StringBuffer();
    sb.append("<table border=\"1\" cellpadding=\"5\" cellspacing=\"0\">\n");
    sb.append("<tr>\n");
    sb.append("<th colspan=3>CPU</th>\n");
    sb.append("<th colspan=3>MEM</th>\n");
    sb.append("<th rowspan=2>Reported</th>\n");
    sb.append("</tr>\n");
    sb.append("<tr>\n");
    sb.append("<th>Total</th><th>Used</th><th>%</th>\n");
    sb.append("<th>Total</th><th>Used</th><th>%</th>\n");
    sb.append("</tr>\n");
    sb.append("<tr>\n");
    sb.append(String.format(
              "<td>%.1f GHz</td><td>%.1f GHz</td><td>%.1f%%</td>\n",
              reporter.getClusterCpuTotalGHz(),
              reporter.getClusterCpuUsageGHz(),
              Math.min(reporter.getClusterCpuUsageGHz() /
                       reporter.getClusterCpuTotalGHz() * 100D, 100D)));
    sb.append(String.format(
              "<td>%.1f GB</td><td>%.1f GB</td><td>%.1f%%</td><td>%d</td>\n",
              reporter.getClusterMemTotalGB(),
              reporter.getClusterMemUsageGB(),
              reporter.getClusterMemUsageGB() /
              reporter.getClusterMemTotalGB() * 100D,
              reporter.getReportedTaskTrackers()));
    sb.append("</tr>\n");
    sb.append("</table>\n");
    return sb.toString();
  }

  /**
   * Method used to generate the Job table for Job pages with resource
   * utilization information obtain from {@link ResourceReporter}.
   *
   * @param label display heading to be used in the job table.
   * @param jobs vector of jobs to be displayed in table.
   * @param refresh refresh interval to be used in jobdetails page.
   * @param rowId beginning row id to be used in the table.
   * @return
   * @throws IOException
   */
  public static String generateJobTableWithResourceInfo(String label,
          Collection<JobInProgress> jobs, int refresh, int rowId,
          JobTracker tracker) throws IOException {
    ResourceReporter reporter = tracker.getResourceReporter();

    if (reporter == null) {
      return generateJobTable(label, jobs, refresh, rowId);
    }
    boolean isRunning = label.equals("Running");
    boolean isModifiable =
        isRunning && conf.getBoolean(PRIVATE_ACTIONS_KEY, false);
    StringBuffer sb = new StringBuffer();

    sb.append("<table border=\"1\" cellpadding=\"5\" cellspacing=\"0\" class=\"tablesorter\">\n");

    if (jobs.size() > 0) {
      if (isModifiable) {
        sb.append("<form action=\"/jobtracker_hmon.jsp\" onsubmit=\"return confirmAction();\" method=\"POST\">");
        sb.append("<thead><tr>");
        sb.append("<td><input type=\"Button\" onclick=\"selectAll()\" " +
        		"value=\"Select All\" id=\"checkEm\"></td>");
        sb.append("<td>");
        sb.append("<input type=\"submit\" name=\"killJobs\" value=\"Kill Selected Jobs\">");
        sb.append("</td>");
        sb.append("<td><nobr>");
        sb.append("<select name=\"setJobPriority\">");

        for (JobPriority prio : JobPriority.values()) {
          sb.append("<option"
              + (JobPriority.NORMAL == prio ? " selected=\"selected\">" : ">")
              + prio + "</option>");
        }

        sb.append("</select>");
        sb.append("<input type=\"submit\" name=\"changeJobPriority\" " +
        		"value=\"Change\">");
        sb.append("</nobr></td>");
        sb.append("<td colspan=\"15\">&nbsp;</td>");
        sb.append("</tr>");
        sb.append("<th>&nbsp;</th>");
      } else {
        sb.append("<thead><tr>");
      }

      int totalMaps = 0;
      int comMaps = 0;
      int totalRunningMaps = 0;
      int totalReduces = 0;
      int comReduces = 0;
      int totalRunningReduces = 0;
      for (Iterator<JobInProgress> it = jobs.iterator(); it.hasNext(); ) {
        JobInProgress job = it.next();
        totalMaps += job.desiredMaps();
        totalReduces += job.desiredReduces();
        comMaps += job.finishedMaps();
        comReduces += job.finishedReduces();
        if (isRunning) {
          totalRunningMaps += job.runningMaps();
          totalRunningReduces += job.runningReduces();
        }
      }
      sb.append("<th><b>Jobid</b></th><th><b>Priority" +
      		"</b></th><th><b>User</b></th>");
      sb.append("<th><b>Name</b></th>");
      sb.append("<th><b>Map % Complete</b></th>");
      sb.append("<th><b>Map Total " + totalMaps + "</b></th>");
      sb.append("<th><b>Maps Completed " + comMaps + "</b></th>");
      if (isRunning) {
        sb.append("<th><b>Maps Running " + totalRunningMaps + "</b></th>");
      }
      sb.append("<th><b>Reduce % Complete</b></th>");
      sb.append("<th><b>Reduce Total " + totalReduces + "</b></th>");
      sb.append("<th><b>Reduces Completed " + comReduces + "</b></th>");
      if (isRunning) {
        sb.append("<th><b>Reduces Running " + totalRunningReduces + "</b></th>");
      }
      sb.append("<th><b>CPU Now</b></th>");
      sb.append("<th><b>CPU Cumulated Cluster-sec</b></th>");
      sb.append("<th><b>MEM Now</b></a></th>");
      sb.append("<th><b>MEM Cumulated Cluster-sec</b></th>");
      sb.append("<th><b>MEM Max/Node</b></th>");
      sb.append("</tr></thead><tbody>\n");
      for (Iterator<JobInProgress> it = jobs.iterator(); it.hasNext(); ++rowId) {
        JobInProgress job = it.next();
        JobProfile profile = job.getProfile();
        JobStatus status = job.getStatus();
        JobID jobid = profile.getJobID();

        int desiredMaps = job.desiredMaps();
        int desiredReduces = job.desiredReduces();
        int completedMaps = job.finishedMaps();
        int completedReduces = job.finishedReduces();
        String runningMapTableData =
            (isRunning) ? job.runningMaps() + "</td><td>" : "";
        String runningReduceTableData =
            (isRunning) ?  "</td><td>" + job.runningReduces() : "";

        String name = profile.getJobName();
        String jobpri = job.getPriority().toString();

        if (isModifiable) {
          sb.append("<tr><td><input TYPE=\"checkbox\" " +
          		"onclick=\"checkButtonVerbage()\" " +
          		"name=\"jobCheckBox\" value="
                  + jobid + "></td>");
        } else {
          sb.append("<tr>");
        }
        String cpu = "-";
        String mem = "-";
        String memMax = "-";
        String cpuCost = "-";
        String memCost = "-";
        if (reporter.getJobCpuCumulatedUsageTime(jobid) !=
            ResourceReporter.UNAVAILABLE) {
          cpu = String.format("%.2f%%",
              reporter.getJobCpuPercentageOnCluster(jobid));
          if (reporter.getJobCpuPercentageOnCluster(jobid) > 50) {
            cpu = "<font color=\"red\">" + cpu + "</font>";
          }
          mem = String.format("%.2f%%",
              reporter.getJobMemPercentageOnCluster(jobid));
          if (reporter.getJobMemPercentageOnCluster(jobid) > 50) {
            mem = "<font color=\"red\">" + mem + "</font>";
          }
          cpuCost = String.format("%.2f",
              reporter.getJobCpuCumulatedUsageTime(jobid) / 1000D);
          memCost = String.format("%.2f",
              reporter.getJobMemCumulatedUsageTime(jobid) / 1000D);
          memMax = String.format("%.2f%%",
              reporter.getJobMemMaxPercentageOnBox(jobid));
          if (reporter.getJobMemMaxPercentageOnBox(jobid) > 50) {
            memMax = "<font color=\"red\">" + memMax + "</font>";
          }
        }

        sb.append("<td id=\"job_" + rowId
            + "\"><a href=\"jobdetails.jsp?jobid=" + jobid + "&refresh="
            + refresh + "\">" + jobid + "</a></td>" + "<td id=\"priority_"
            + rowId + "\">" + jobpri + "</td>" + "<td id=\"user_" + rowId
            + "\">" + profile.getUser() + "</td>" + "<td id=\"name_" + rowId
            + "\">" + ("".equals(name) ? "&nbsp;" : name) + "</td><td>"
            + StringUtils.formatPercent(status.mapProgress(), 2)
            + ServletUtil.percentageGraph(status.mapProgress() * 100, 80)
            + "</td><td>" + desiredMaps + "</td><td>" + completedMaps
            + "</td><td>" + runningMapTableData
            + StringUtils.formatPercent(status.reduceProgress(), 2)
            + ServletUtil.percentageGraph(status.reduceProgress() * 100, 80)
            + "</td><td>" + desiredReduces + "</td><td> " + completedReduces
            + runningReduceTableData
            + "</td><td id=\"cpu_" + rowId + "\">" + cpu + "</td>"
            + "<td id=\"cpuCost_" + rowId + "\">" + cpuCost + "</td>"
            + "<td id=\"mem_" + rowId + "\">" + mem + "</td>"
            + "<td id=\"memCost_" + rowId + "\">" + memCost + "</td>"
            + "<td id=\"memMax_" + rowId + "\">" + memMax + "</td></tr>\n");
      }
      if (isModifiable) {
        sb.append("</form>\n");
      }
      sb.append("</tbody>");
    } else {
      sb.append("<tr><td align=\"center\" colspan=\"8\"><i>none</i>" +
      		"</td></tr>\n");
    }
    sb.append("</table>\n");

    return sb.toString();
  }

  /**
   * Method used to generate the txt based Job table for Job pages.
   *
   * @param jobs vector of jobs to be displayed in table.
   * @param colSeparator the char used to separate columns
   * @param rowSeparator the char used to separate records
   * @return a String contains the table
   * @throws IOException
   */
  public static String generateTxtJobTable(Collection<JobInProgress> jobs,
    JobTracker tracker) throws IOException {
    char colSeparator = '\t';
    char rowSeparator = '\n';

    StringBuffer sb = new StringBuffer();
    sb.append("01.JOBID" + colSeparator +
              "02.START" + colSeparator +
              "03.FINISH" + colSeparator +
              "04.USER" + colSeparator +
              "05.NAME" + colSeparator +
              "06.BLACK_TT" + colSeparator +
              "07.PRIORITY" + colSeparator +
              "08.MAP_TOTAL" + colSeparator +
              "09.MAP_COMPLETE" + colSeparator +
              "10.MAP_RUN" + colSeparator +
              "11.MAP_SPECU" + colSeparator +
              "12.MAP_NONLOC" + colSeparator +
              "13.MAP_KILLED" + colSeparator +
              "14.MAP_FAILED" + colSeparator +
              "15.RED_TOTAL" + colSeparator +
              "16.RED_COMPLETE" + colSeparator +
              "17.RED_RUN" + colSeparator +
              "18.RED_SPECU" + colSeparator +
              "19.RED_KILLED" + colSeparator +
              "20.RED_FAILED" + colSeparator +
              "21.%MEM" + colSeparator +
              "22.%MEM_MAX" + colSeparator +
              "23.%MEM_PEAK" + colSeparator +
              "24.MEM_MS" + colSeparator +
              "25.%CPU" + colSeparator +
              "26.%CPU_MAX" + colSeparator +
              "27.CPU_MS" + rowSeparator);

    if (jobs.size() > 0) {
      for (Iterator<JobInProgress> it = jobs.iterator(); it.hasNext();) {
        JobInProgress job = it.next();
        JobProfile profile = job.getProfile();
        String user = profile.getUser();
        String name = profile.getJobName().
                      replace(' ', '_').replace('\t', '_').replace('\n', '_');
        int desiredMaps = job.desiredMaps();
        int desiredReduces = job.desiredReduces();
        int runningMaps = 0;
        int failedMaps = 0;
        int killedMaps = 0;
        for (TaskInProgress tip: job.getTasks(TaskType.MAP)) {
          if (tip.isRunning()) {
            runningMaps += tip.getActiveTasks().size();
            tip.numKilledTasks();
            failedMaps += tip.numTaskFailures();
            killedMaps += tip.numKilledTasks();
          }
        }
        int runningReduces = 0;
        int failedReduces = 0;
        int killedReduces = 0;
        for (TaskInProgress tip: job.getTasks(TaskType.REDUCE)) {
          if (tip.isRunning()) {
            runningReduces += tip.getActiveTasks().size();
            failedReduces += tip.numTaskFailures();
            killedReduces += tip.numKilledTasks();
          }
        }
        int completedMaps = job.finishedMaps();
        int completedReduces = job.finishedReduces();
        int nonLocalRunningMaps = job.getNonLocalRunningMaps().size();
        long submitTime = job.getStartTime();
        long finishTime = job.getFinishTime();
        String jobpri = job.getPriority().toString();
        JobID jobId = job.getJobID();
        double mem = 0, memMax = 0, memMaxPeak = 0, memCost = 0;
        double cpu = 0, cpuMax = 0, cpuCost = 0;
        ResourceReporter reporter = tracker.getResourceReporter();
        if (reporter != null) {
          mem = reporter.getJobCpuPercentageOnCluster(jobId);
          memMax = reporter.getJobMemMaxPercentageOnBox(jobId);
          memMaxPeak = reporter.getJobMemMaxPercentageOnBoxAllTime(jobId);
          memCost = reporter.getJobMemCumulatedUsageTime(jobId);
          cpu = reporter.getJobCpuPercentageOnCluster(jobId);
          cpuMax = reporter.getJobCpuMaxPercentageOnBox(jobId);
          cpuCost = reporter.getJobCpuCumulatedUsageTime(jobId);
        }
        sb.append(jobId.toString() + colSeparator +
                  submitTime + colSeparator +
                  finishTime + colSeparator +
                  user + colSeparator +
                  name + colSeparator +
                  job.getNoOfBlackListedTrackers() + colSeparator +
                  jobpri + colSeparator +
                  desiredMaps + colSeparator +
                  completedMaps + colSeparator +
                  runningMaps + colSeparator +
                  job.speculativeMapTasks + colSeparator +
                  nonLocalRunningMaps + colSeparator +
                  killedMaps + colSeparator +
                  failedMaps + colSeparator +
                  desiredReduces + colSeparator +
                  completedReduces + colSeparator +
                  runningReduces + colSeparator +
                  job.speculativeReduceTasks + colSeparator +
                  killedReduces + colSeparator +
                  failedReduces + colSeparator +
                  mem + colSeparator +
                  memMax + colSeparator +
                  memMaxPeak + colSeparator +
                  memCost + colSeparator +
                  cpu + colSeparator +
                  cpuMax + colSeparator +
                  cpuCost + rowSeparator);
        }
    }
    return sb.toString();
  }
}
