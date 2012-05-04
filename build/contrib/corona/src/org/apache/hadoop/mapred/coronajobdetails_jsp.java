package org.apache.hadoop.mapred;

import javax.servlet.*;
import javax.servlet.http.*;
import javax.servlet.jsp.*;
import javax.servlet.*;
import javax.servlet.http.*;
import java.io.*;
import java.text.*;
import java.util.*;
import java.text.DecimalFormat;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.util.*;

public final class coronajobdetails_jsp extends org.apache.jasper.runtime.HttpJspBase
    implements org.apache.jasper.runtime.JspSourceDependent {


  private static final String PRIVATE_ACTIONS_KEY 
		= "webinterface.private.actions";

  private String tasksUrl = null;
  private String failUrl = null;
  private String jobUrl = null;
  private String clusterManagerUrl = null;
  private String confUrl = null;
  private boolean hasProxy = false;

  private String getProxyUrl(String proxyPath, String params) {
    return proxyPath + (hasProxy ? "&" : "?") + params;
  }

  private String getTasksUrl(String params) {
    return getProxyUrl(tasksUrl, params);
  }

  private void printTaskSummary(JspWriter out,
                                JobID jobId,
                                String kind,
                                double completePercent,
                                TaskInProgress[] tasks
                               ) throws IOException {
    int totalTasks = tasks.length;
    int runningTasks = 0;
    int finishedTasks = 0;
    int killedTasks = 0;
    int failedTaskAttempts = 0;
    int killedTaskAttempts = 0;
    for(int i=0; i < totalTasks; ++i) {
      TaskInProgress task = tasks[i];
      if (task.isComplete()) {
        finishedTasks += 1;
      } else if (task.isRunning()) {
        runningTasks += 1;
      } else if (task.wasKilled()) {
        killedTasks += 1;
      }
      failedTaskAttempts += task.numTaskFailures();
      killedTaskAttempts += task.numKilledTasks();
    }
    int pendingTasks = totalTasks - runningTasks - killedTasks - finishedTasks; 
    out.print("<tr><th><a href=\"" + getTasksUrl("type="+ kind + "&pagenum=1") + "\">" + kind + 
              "</a></th><td align=\"right\">" + 
              StringUtils.formatPercent(completePercent, 2) +
              ServletUtil.percentageGraph((int)(completePercent * 100), 80) +
              "</td><td align=\"right\">" + 
              totalTasks + 
              "</td><td align=\"right\">" + 
              ((pendingTasks > 0) 
               ? "<a href=\"" + getTasksUrl("type="+ kind + "&pagenum=1" + "&state=pending") +
               "\">" + pendingTasks + "</a>"
               : "0") + 
              "</td><td align=\"right\">" + 
              ((runningTasks > 0) 
               ? "<a href=\"" + getTasksUrl("type="+ kind + "&pagenum=1" + "&state=running") +
               "\">" + runningTasks + "</a>" 
               : "0") + 
              "</td><td align=\"right\">" + 
              ((finishedTasks > 0) 
               ?"<a href=\"" + getTasksUrl("type="+ kind + "&pagenum=1" + "&state=completed") +
               "\">" + finishedTasks + "</a>" 
               : "0") + 
              "</td><td align=\"right\">" + 
              ((killedTasks > 0) 
               ?"<a href=\"" + getTasksUrl("type="+ kind + "&pagenum=1" + "&state=killed") +
               "\">" + killedTasks + "</a>"
               : "0") + 
              "</td><td align=\"right\">" + 
              ((failedTaskAttempts > 0) ? 
               ("<a href=\"" + getProxyUrl(failUrl, "jobid=" + jobId + "&kind=" + kind + "&cause=failed") + "\">" + failedTaskAttempts + 
                   "</a>") : 
                  "0"
                  ) + 
              " / " +
              ((killedTaskAttempts > 0) ? 
               ("<a href=\"" + getProxyUrl(failUrl, "jobid=" + jobId + "&kind=" + kind + "&cause=killed") + "\">" + killedTaskAttempts + 
                   "</a>") : 
                  "0"
                  ) + 
              "</td></tr>\n");
  }

  private void printJobLevelTaskSummary(JspWriter out,
                                JobID jobId,
                                String kind,
                                TaskInProgress[] tasks
                               ) throws IOException {
    int totalTasks = tasks.length;
    int runningTasks = 0;
    int finishedTasks = 0;
    int killedTasks = 0;
    for(int i=0; i < totalTasks; ++i) {
      TaskInProgress task = tasks[i];
      if (task.isComplete()) {
        finishedTasks += 1;
      } else if (task.isRunning()) {
        runningTasks += 1;
      } else if (task.isFailed()) {
        killedTasks += 1;
      }
    }
    int pendingTasks = totalTasks - runningTasks - killedTasks - finishedTasks; 
    out.print(((runningTasks > 0)  
               ? "<a href=\"" + getTasksUrl("type="+ kind + "&pagenum=1" + "&state=running") +
               "\">" + " Running" + 
                 "</a>" 
               : ((pendingTasks > 0) ? " Pending" :
                 ((finishedTasks > 0) 
                  ?"<a href=\"" + getTasksUrl("type="+ kind + "&pagenum=1" + "&state=completed") +
                  "\">" + " Successful"
                 + "</a>" 
               : ((killedTasks > 0) 
                  ?"<a href=\"" + getTasksUrl("type="+ kind + "&pagenum=1" + "&state=killed") +
                  "\">" + " Failed" 
                + "</a>" : "None")))));
  }
  
  private void printConfirm(JspWriter out, JobID jobId) throws IOException{
    String url = jobUrl;
    out.print("<html><head><META http-equiv=\"refresh\" content=\"15;URL="
        + url+"\"></head>"
        + "<body><h3> Are you sure you want to kill " + jobId
        + " ?<h3><br><table border=\"0\"><tr><td width=\"100\">"
        + "<form action=\"" + url + "\" method=\"post\">"
        + "<input type=\"hidden\" name=\"action\" value=\"kill\" />"
        + "<input type=\"submit\" name=\"kill\" value=\"Kill\" />"
        + "</form>"
        + "</td><td width=\"100\"><form method=\"post\" action=\"" + url
        + "\"><input type=\"submit\" value=\"Cancel\" name=\"Cancel\""
        + "/></form></td></tr></table></body></html>");
  }


  private static final JspFactory _jspxFactory = JspFactory.getDefaultFactory();

  private static java.util.Vector _jspx_dependants;

  private org.apache.jasper.runtime.ResourceInjector _jspx_resourceInjector;

  public Object getDependants() {
    return _jspx_dependants;
  }

  public void _jspService(HttpServletRequest request, HttpServletResponse response)
        throws java.io.IOException, ServletException {

    PageContext pageContext = null;
    HttpSession session = null;
    ServletContext application = null;
    ServletConfig config = null;
    JspWriter out = null;
    Object page = this;
    JspWriter _jspx_out = null;
    PageContext _jspx_page_context = null;

    try {
      response.setContentType("text/html; charset=UTF-8");
      pageContext = _jspxFactory.getPageContext(this, request, response,
      			null, true, 8192, true);
      _jspx_page_context = pageContext;
      application = pageContext.getServletContext();
      config = pageContext.getServletConfig();
      session = pageContext.getSession();
      out = pageContext.getOut();
      _jspx_out = out;
      _jspx_resourceInjector = (org.apache.jasper.runtime.ResourceInjector) application.getAttribute("com.sun.appserv.jsp.resource.injector");

      out.write('\n');
      out.write('\n');

  CoronaJobTracker tracker = (CoronaJobTracker) application.getAttribute("job.tracker");

      out.write('\n');
      out.write('\n');

    tasksUrl = tracker.getProxyUrl("coronajobtasks.jsp");
    failUrl = tracker.getProxyUrl("coronajobfailures.jsp");
    jobUrl = tracker.getProxyUrl("coronajobdetails.jsp");
    clusterManagerUrl = tracker.getClusterManagerUrl();
    confUrl = tracker.getProxyUrl("coronajobconf.jsp");
    hasProxy = (tasksUrl.indexOf('?') != -1);

    String refreshParam = request.getParameter("refresh");
    int refresh = 60; // refresh every 60 seconds by default
    if (refreshParam != null) {
        try {
            refresh = Integer.parseInt(refreshParam);
        }
        catch (NumberFormatException ignored) {
        }
    }
    JobID jobId = null;
    CoronaJobInProgress job = (CoronaJobInProgress) tracker.getJob();
    if (job != null) {
      jobId = job.getStatus().getJobID();

      String action = request.getParameter("action");
      if(JSPUtil.conf.getBoolean(PRIVATE_ACTIONS_KEY, false) && 
          "changeprio".equalsIgnoreCase(action) 
          && request.getMethod().equalsIgnoreCase("POST")) {
        tracker.setJobPriority(jobId, request.getParameter("prio"));
      }

      if(JSPUtil.conf.getBoolean(PRIVATE_ACTIONS_KEY, false)) {
          action = request.getParameter("action");
              if(action!=null && action.equalsIgnoreCase("confirm")) {
                printConfirm(out, jobId);
              return;
              }
              else if(action != null && action.equalsIgnoreCase("kill") && 
                  request.getMethod().equalsIgnoreCase("POST")) {
                tracker.killJobFromWebUI(jobId);
              }
      }
    }

      out.write("\n\n<html>\n<head>\n  ");
 
  if (refresh != 0) {
      
      out.write("\n      <meta http-equiv=\"refresh\" content=\"");
      out.print(refresh);
      out.write("\">\n      ");

  }
  
      out.write("\n<title>Hadoop ");
      out.print(jobId);
      out.write(" </title>\n<link rel=\"stylesheet\" type=\"text/css\" href=\"/static/hadoop.css\">\n</head>\n<body>\n\n");
 
    out.println("<h1>Hadoop Corona " + jobId +
      " on <a href=\"" + clusterManagerUrl + "\">" +
      StringUtils.simpleHostname(tracker.getProxyJTAddr()) +
      "</a></h1>");

    if (job == null) {
      out.println("<h2>No running job!</h2>");
      return;
    }
    JobProfile profile = job.getProfile();
    JobStatus status = job.getStatus();
    int runState = status.getRunState();
    int flakyTaskTrackers = job.getNoOfBlackListedTrackers();
    out.print("<b>User:</b> " + profile.getUser() + "<br>\n");
    out.print("<b>Job Name:</b> " + profile.getJobName() + "<br>\n");
    out.print("<b>Job File:</b> <a href=\"" + getProxyUrl(confUrl, "jobid=" + jobId) + "\">" 
              + profile.getJobFile() + "</a><br>\n");
    out.print("<b>Job Setup:</b>");
    printJobLevelTaskSummary(out, jobId, "setup",
                             job.getTasks(TaskType.JOB_SETUP));
    out.print("<br>\n");
    if (runState == JobStatus.RUNNING) {
      out.print("<b>Status:</b> Running<br>\n");
      out.print("<b>Started at:</b> " + new Date(job.getStartTime()) + "<br>\n");
      out.print("<b>Running for:</b> " + StringUtils.formatTimeDiff(
          System.currentTimeMillis(), job.getStartTime()) + "<br>\n");
    } else {
      if (runState == JobStatus.SUCCEEDED) {
        out.print("<b>Status:</b> Succeeded<br>\n");
        out.print("<b>Started at:</b> " + new Date(job.getStartTime()) + "<br>\n");
        out.print("<b>Finished at:</b> " + new Date(job.getFinishTime()) +
                  "<br>\n");
        out.print("<b>Finished in:</b> " + StringUtils.formatTimeDiff(
            job.getFinishTime(), job.getStartTime()) + "<br>\n");
      } else if (runState == JobStatus.FAILED) {
        out.print("<b>Status:</b> Failed<br>\n");
        out.print("<b>Started at:</b> " + new Date(job.getStartTime()) + "<br>\n");
        out.print("<b>Failed at:</b> " + new Date(job.getFinishTime()) +
                  "<br>\n");
        out.print("<b>Failed in:</b> " + StringUtils.formatTimeDiff(
            job.getFinishTime(), job.getStartTime()) + "<br>\n");
      } else if (runState == JobStatus.KILLED) {
        out.print("<b>Status:</b> Killed<br>\n");
        out.print("<b>Started at:</b> " + new Date(job.getStartTime()) + "<br>\n");
        out.print("<b>Killed at:</b> " + new Date(job.getFinishTime()) +
                  "<br>\n");
        out.print("<b>Killed in:</b> " + StringUtils.formatTimeDiff(
            job.getFinishTime(), job.getStartTime()) + "<br>\n");
      }
    }
    out.print("<b>Job Cleanup:</b>");
    printJobLevelTaskSummary(out, jobId, "cleanup",
                             job.getTasks(TaskType.TASK_CLEANUP));
    out.print("<br>\n");
    if (flakyTaskTrackers > 0) {
      out.print("<b>Black-listed TaskTrackers:</b> " + 
          "<a href=\"jobblacklistedtrackers.jsp?jobid=" + jobId + "\">" +
          flakyTaskTrackers + "</a><br>\n");
    }
    if (job.getSchedulingInfo() != null) {
      out.print("<b>Job Scheduling information: </b>" +
          job.getSchedulingInfo().toString() +"\n");
    }
    out.print("<hr>\n");
    out.print("<table border=2 cellpadding=\"5\" cellspacing=\"2\">");
    out.print("<tr><th>Kind</th><th>% Complete</th><th>Num Tasks</th>" +
              "<th>Pending</th><th>Running</th><th>Complete</th>" +
              "<th>Killed</th>" +
              "<th><a href=\"" + getProxyUrl(failUrl, "jobid=" + jobId) + 
              "\">Failed/Killed<br>Task Attempts</a></th></tr>\n");
    printTaskSummary(out, jobId, "map", status.mapProgress(), 
                     job.getTasks(TaskType.MAP));
    printTaskSummary(out, jobId, "reduce", status.reduceProgress(),
                     job.getTasks(TaskType.REDUCE));
    out.print("</table>\n");
    
    
      out.write("\n    <p/>\n    <table border=2 cellpadding=\"5\" cellspacing=\"2\">\n    <tr>\n      <th><br/></th>\n      <th>Counter</th>\n      <th>Map</th>\n      <th>Reduce</th>\n      <th>Total</th>\n    </tr>\n    ");

    Counters mapCounters = job.getMapCounters();
    Counters reduceCounters = job.getReduceCounters();
    Counters totalCounters = job.getCounters();
    
    for (String groupName : totalCounters.getGroupNames()) {
      Counters.Group totalGroup = totalCounters.getGroup(groupName);
      Counters.Group mapGroup = mapCounters.getGroup(groupName);
      Counters.Group reduceGroup = reduceCounters.getGroup(groupName);
      
      Format decimal = new DecimalFormat();
      
      boolean isFirst = true;
      for (Counters.Counter counter : totalGroup) {
        String name = counter.getDisplayName();
        String mapValue = decimal.format(mapGroup.getCounter(name));
        String reduceValue = decimal.format(reduceGroup.getCounter(name));
        String totalValue = decimal.format(counter.getCounter());
        
      out.write("\n        <tr>\n          ");

          if (isFirst) {
            isFirst = false;
            
      out.write("\n            <td rowspan=\"");
      out.print(totalGroup.size());
      out.write('"');
      out.write('>');
      out.print(totalGroup.getDisplayName());
      out.write("</td>\n            ");

          }
          
      out.write("\n          <td>");
      out.print(name);
      out.write("</td>\n          <td align=\"right\">");
      out.print(mapValue);
      out.write("</td>\n          <td align=\"right\">");
      out.print(reduceValue);
      out.write("</td>\n          <td align=\"right\">");
      out.print(totalValue);
      out.write("</td>\n        </tr>\n        ");

      }
    }
    
      out.write("\n    </table>\n\n<hr>\n\n<table border=\"0\"> <tr>\n    \n");
 if(JSPUtil.conf.getBoolean(PRIVATE_ACTIONS_KEY, false) 
    	&& runState == JobStatus.RUNNING) {
      out.print("<br/><a href=\"" +
                getProxyUrl(jobUrl, "action=confirm&jobid=" + jobId) +
                "\"> Kill this job </a>");
    }

      out.write("\n\n<hr>\n\n");

out.println(ServletUtil.htmlFooter());

      out.write('\n');
    } catch (Throwable t) {
      if (!(t instanceof SkipPageException)){
        out = _jspx_out;
        if (out != null && out.getBufferSize() != 0)
          out.clearBuffer();
        if (_jspx_page_context != null) _jspx_page_context.handlePageException(t);
      }
    } finally {
      _jspxFactory.releasePageContext(_jspx_page_context);
    }
  }
}
