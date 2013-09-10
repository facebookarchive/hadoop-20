<%@ page
  contentType="text/html; charset=UTF-8"
  import="javax.servlet.*"
  import="javax.servlet.http.*"
  import="java.io.*"
  import="java.text.*"
  import="java.util.*"
  import="java.text.DecimalFormat"
  import="org.apache.hadoop.mapred.*"
  import="org.apache.hadoop.corona.*"
  import="org.apache.hadoop.mapreduce.TaskType"
  import="org.apache.hadoop.util.*"
%>

<%
  CoronaJobTracker tracker = (CoronaJobTracker) application.getAttribute("job.tracker");
%>
<%!
  private String tasksUrl = null;
  private String resourcesUrl = null;
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
                                TaskInProgress[] tasks,
                                int totalGrants
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
              ((totalGrants > 0)
               ?"<a href=\"" + getProxyUrl(resourcesUrl, "jobid=" + jobId + "&kind=" + kind) +
               "\">" + totalGrants + "</a>"
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

%>
<%
    tasksUrl = tracker.getProxyUrl("coronajobtasks.jsp");
	resourcesUrl = tracker.getProxyUrl("coronajobresources.jsp");
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
      if("changeprio".equalsIgnoreCase(action)
          && request.getMethod().equalsIgnoreCase("POST")) {
        tracker.setJobPriority(jobId, request.getParameter("prio"));
      }

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
%>

<html>
<%
if (job == null) {
  //Check if we're local Jt running remote JT - redirect if so.
  response.sendRedirect(tracker.getRemoteJTUrl());
}
%>
<head>
  <%
  if (refresh != 0) {
      %>
      <meta http-equiv="refresh" content="<%=refresh%>">
      <%
  }
  %>
<title>Hadoop <%=jobId%> </title>
<link rel="stylesheet" type="text/css" href="/static/hadoop.css">
</head>
<body>

<%
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
    int flakyTaskTrackers = tracker.getStats().getNumFaultyTrackers();
    out.print("<b>User:</b> " + profile.getUser() + "<br>\n");
    out.print("<b>Job Name:</b> " + profile.getJobName() + "<br>\n");
    out.print("<b>Job File:</b> <a href=\"" + getProxyUrl(confUrl, "jobid=" + jobId) + "\">"
              + profile.getJobFile() + "</a><br>\n");
    out.print("<b>Job Pool:</b> " + tracker.getPoolInfo() + "<br>\n");
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
    
    
    if (tracker.isStandAlone()) {
      String taskLogUrl = null;
      taskLogUrl = TaskLogServlet.getTaskLogUrl(tracker.getTTHost(),
        						tracker.getTTHttpPort(),
        						tracker.getTid().toString());
      if (taskLogUrl != null) {
        out.print("<b>RemoteJobTrackerLog:</b>");
        String tailFourKBUrl = taskLogUrl + "&start=-4097";
        String tailEightKBUrl = taskLogUrl + "&start=-8193";
        String entireLogUrl = taskLogUrl + "&all=true";
        out.print("<a href=\"" + tailFourKBUrl + "\">Last 4KB</a>  ");
        out.print("<a href=\"" + tailEightKBUrl + "\">Last 8KB</a>  ");
        out.print("<a href=\"" + entireLogUrl + "\">All</a>  ");
        
        String pid = tracker.getPid();
        if (pid != null) {
        	String stackTracingUrl = taskLogUrl + 
        	  "&stacktracing=true&start=-891981&filter=stdout&pid=" + pid;
        	out.print("<a href=\"" + stackTracingUrl + "\">StackTrace</a><br/>\n");
        } else {
        	out.print("<br>\n");
        } 
      }
    }
    
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
              "<th>Pending</th><th>Running</th>" +
              "<th>Granted</th><th>Complete</th>" +
              "<th>Killed</th>" +
              "<th><a href=\"" + getProxyUrl(failUrl, "jobid=" + jobId) +
              "\">Failed/Killed<br>Task Attempts</a></th></tr>\n");
    ResourceUsage resourceUsage = tracker.getResourceUsage();
    printTaskSummary(out, jobId, "map", status.mapProgress(),
                     job.getTasks(TaskType.MAP),
                     resourceUsage.getTotalMapperGrants());
    printTaskSummary(out, jobId, "reduce", status.reduceProgress(),
                     job.getTasks(TaskType.REDUCE),
                     resourceUsage.getTotalReducerGrants());
    out.print("</table>\n");

    %>
    <p/>
    <table border=2 cellpadding="5" cellspacing="2">
    <tr>
      <th><br/></th>
      <th>Counter</th>
      <th>Map</th>
      <th>Reduce</th>
      <th>Total</th>
    </tr>
    <%
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
        %>
        <tr>
          <%
          if (isFirst) {
            isFirst = false;
            %>
            <td rowspan="<%=totalGroup.size()%>"><%=totalGroup.getDisplayName()%></td>
            <%
          }
          %>
          <td><%=name%></td>
          <td align="right"><%=mapValue%></td>
          <td align="right"><%=reduceValue%></td>
          <td align="right"><%=totalValue%></td>
        </tr>
        <%
      }
    }
    %>
    </table>

<hr>

<table border="0"> <tr>

<% if(runState == JobStatus.RUNNING) {
      out.print("<br/><a href=\"" +
                getProxyUrl(jobUrl, "action=confirm&jobid=" + jobId) +
                "\"> Kill this job </a>");
%>
<table border="0"> <tr> <td>
Change priority from <%=job.getPriority()%> to:
<form action="<%=jobUrl%>" method="post">
<input type="hidden" name="action" value="changeprio"/>
<input type="hidden" name="jobid" value="<%=jobId%>"/>
</td><td> <select name="prio"> 
<%
  SessionPriority jobPrio = job.getPriority();
  for (SessionPriority prio : SessionPriority.values()) {
    if(jobPrio != prio) {
      %> <option value=<%=prio%>><%=prio%></option> <%
    }
  }
%>
</select> </td><td><input type="submit" value="Submit"> </form></td></tr> </table>
<%
    }
%>

<hr>

<%
out.println(ServletUtil.htmlFooter());
%>
