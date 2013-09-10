<%@ page
  contentType="text/html; charset=UTF-8"
  import="javax.servlet.*"
  import="javax.servlet.http.*"
  import="java.io.*"
  import="java.lang.String"
  import="java.util.*"
  import="org.apache.hadoop.mapred.*"
  import="org.apache.hadoop.util.*"
  import="java.text.SimpleDateFormat"  
  import="org.apache.hadoop.util.*"
%>
<%!static SimpleDateFormat dateFormat = new SimpleDateFormat(
      "yyyy/MM/dd HH:mm:ss");

  private String detailsUrl = null;
  private String jobUrl = null;
  private String statsUrl = null;
  private boolean hasProxy = false;

  private String getProxyUrl(String proxyPath, String params) {
    return proxyPath + (hasProxy ? "&" : "?") + params;
  }
%>

<%!private void printConfirm(JspWriter out, String jobid, String tipid,
      String taskid, String action) throws IOException {
    String url = getProxyUrl(detailsUrl, "tipid=" + tipid
                             + "&taskid=" + taskid);
    out.print("<html><head><META http-equiv=\"refresh\" content=\"15;URL="
        + url + "\"></head>" + "<body><h3> Are you sure you want to"
        + " kill/fail/speculate "
        + taskid + " ?<h3><br><table border=\"0\"><tr><td width=\"100\">"
        + "<form action=\"" + url + "\" method=\"post\">"
        + "<input type=\"hidden\" name=\"action\" value=\"" + action + "\" />"
        + "<input type=\"submit\" name=\"Kill/Fail/Speculate\""
        + " value=\"Kill/Fail/Speculate\" />"
        + "</form>"
        + "</td><td width=\"100\"><form method=\"post\" action=\"" + url
        + "\"><input type=\"submit\" value=\"Cancel\" name=\"Cancel\""
        + "/></form></td></tr></table></body></html>");
  }%>
<%
    CoronaJobTracker tracker = (CoronaJobTracker) application.getAttribute("job.tracker");
    CoronaJobInProgress job = (CoronaJobInProgress) tracker.getJob();
    JobID jobId = (job != null) ? job.getStatus().getJobID() : null;

    String tipid = request.getParameter("tipid");
    TaskID tipidObj = TaskID.forName(tipid);
    TaskInProgress tip = null;

    if (job != null && tipidObj != null) {
      tip = job.getTaskInProgress(tipidObj);
    }

    String taskid = request.getParameter("taskid");
    TaskAttemptID taskidObj = TaskAttemptID.forName(taskid);

    detailsUrl = tracker.getProxyUrl("coronataskdetails.jsp");
    jobUrl = tracker.getProxyUrl("coronajobdetails.jsp");
    statsUrl = tracker.getProxyUrl("coronataskstats.jsp");
    hasProxy = (detailsUrl.indexOf('?') != -1);

    String action = request.getParameter("action");
    if (action != null) {
      if (action.equalsIgnoreCase("confirm")) {
        String subAction = request.getParameter("subaction");
        if (subAction == null)
          subAction = "fail-task";
        printConfirm(out, jobId.toString(), tipid, taskid, subAction);
        return;
      }
      else if (action.equalsIgnoreCase("kill-task") 
          && request.getMethod().equalsIgnoreCase("POST")) {
        tracker.killTaskFromWebUI(taskidObj, false);
        //redirect again so that refreshing the page will not attempt to rekill the task
        response.sendRedirect(getProxyUrl(detailsUrl, "subaction=kill-task"
                                          + "&tipid=" + tipid));
      }
      else if (action.equalsIgnoreCase("fail-task")
          && request.getMethod().equalsIgnoreCase("POST")) {
        tracker.killTaskFromWebUI(taskidObj, true);
        response.sendRedirect(getProxyUrl(detailsUrl, "subaction=fail-task"
                                          + "&tipid=" + tipid));
      }
      else if (action.equalsIgnoreCase("speculative-task")
          && request.getMethod().equalsIgnoreCase("POST")) {
        if (tip != null) {
          tip.setSpeculativeForced(true);
        }
        response.sendRedirect(getProxyUrl(detailsUrl,
                                          "subaction=speculative-task"
                                          + "&tipid=" + tipid
                                          + "&here=yes"));
      }
    }

    TaskStatus[] ts = null;
    if (tip != null) { 
      ts = tip.getTaskStatuses();
    }
    boolean isCleanupOrSetup = false;
    if ( tip != null) {
      isCleanupOrSetup = tip.isJobCleanupTask();
      if (!isCleanupOrSetup) {
        isCleanupOrSetup = tip.isJobSetupTask();
      }
    }
%>


<html>
<head>
  <link rel="stylesheet" type="text/css" href="/static/hadoop.css">
  <title>Hadoop Task Details</title>
</head>
<body>
<%
   out.println("<h1>Job <a href=\"" +
               jobUrl + "\">" + jobId.toString() + "</a></h1>");
%>

<hr>

<h2>All Task Attempts</h2>
<center>

<table border=2 cellpadding="5" cellspacing="2">
<tr><td align="center">Task Attempts</td><td>Machine</td><td>Status</td><td>Progress</td><td>Start Time</td> 
  <%
   if (ts.length > 0 && !ts[0].getIsMap() && !isCleanupOrSetup) {
   %>
<td>Shuffle Finished</td><td>Sort Finished</td>
  <%
  }
  %>
<td>Finish Time</td><td>Errors</td><td>Task Logs</td><td>Counters</td><td>Actions</td></tr>
  <%
    for (int i = 0; i < ts.length; i++) {
      TaskStatus status = ts[i];
      String taskTrackerName = status.getTaskTracker();
      TaskTrackerStatus taskTracker = tracker.getTaskTrackerStatus(taskTrackerName);
      out.print("<tr><td>" + status.getTaskID() + "</td>");
      String taskAttemptTracker = null;
      String cleanupTrackerName = null;
      TaskTrackerStatus cleanupTracker = null;
      String cleanupAttemptTracker = null;
      boolean hasCleanupAttempt = false;
      if (tip != null && tip.isCleanupAttempt(status.getTaskID())) {
        cleanupTrackerName = tip.machineWhereCleanupRan(status.getTaskID());
        cleanupTracker = tracker.getTaskTrackerStatus(cleanupTrackerName);
        if (cleanupTracker != null) {
          cleanupAttemptTracker = "http://" + cleanupTracker.getHost() + ":"
            + cleanupTracker.getHttpPort();
        }
        hasCleanupAttempt = true;
      }
      out.print("<td>");
      if (hasCleanupAttempt) {
        out.print("Task attempt: ");
      }
      if (taskTracker == null) {
        out.print(taskTrackerName);
      } else {
        taskAttemptTracker = "http://" + taskTracker.getHost() + ":"
          + taskTracker.getHttpPort();
        out.print("<a href=\"" + taskAttemptTracker + "\">"
          + taskTracker.getHost() + "</a>");
      }
      if (hasCleanupAttempt) {
        out.print("<br/>Cleanup Attempt: ");
        if (cleanupAttemptTracker == null ) {
          out.print(cleanupTrackerName);
        } else {
          out.print("<a href=\"" + cleanupAttemptTracker + "\">"
            + cleanupTracker.getHost() + "</a>");
        }
      }
      out.print("</td>");
        out.print("<td>" + status.getRunState() + "</td>");
        out.print("<td>" + StringUtils.formatPercent(status.getProgress(), 2)
          + ServletUtil.percentageGraph(status.getProgress() * 100f, 80) + "</td>");
        out.print("<td>"
          + StringUtils.getFormattedTimeWithDiff(dateFormat, status
          .getStartTime(), 0) + "</td>");
        if (!ts[i].getIsMap() && !isCleanupOrSetup) {
          out.print("<td>"
          + StringUtils.getFormattedTimeWithDiff(dateFormat, status
          .getShuffleFinishTime(), status.getStartTime()) + "</td>");
        out.println("<td>"
          + StringUtils.getFormattedTimeWithDiff(dateFormat, status
          .getSortFinishTime(), status.getShuffleFinishTime())
          + "</td>");
        }
        out.println("<td>"
          + StringUtils.getFormattedTimeWithDiff(dateFormat, status
          .getFinishTime(), status.getStartTime()) + "</td>");

        out.print("<td><pre>");
        String [] failures = tracker.getTaskDiagnostics(status.getTaskID());
        if (failures == null) {
          out.print("&nbsp;");
        } else {
          for(int j = 0 ; j < failures.length ; j++){
            out.print(failures[j]);
            if (j < (failures.length - 1)) {
              out.print("\n-------\n");
            }
          }
        }
        out.print("</pre></td>");
        out.print("<td>");
        String taskLogUrl = null;
        if (taskTracker != null ) {
        	taskLogUrl = TaskLogServlet.getTaskLogUrl(taskTracker.getHost(),
        						String.valueOf(taskTracker.getHttpPort()),
        						status.getTaskID().toString());
      	}
        if (hasCleanupAttempt) {
          out.print("Task attempt: <br/>");
        }
        if (taskLogUrl == null) {
          out.print("n/a");
        } else {
          String tailFourKBUrl = taskLogUrl + "&start=-4097";
          String tailEightKBUrl = taskLogUrl + "&start=-8193";
          String entireLogUrl = taskLogUrl + "&all=true";
          out.print("<a href=\"" + tailFourKBUrl + "\">Last 4KB</a><br/>");
          out.print("<a href=\"" + tailEightKBUrl + "\">Last 8KB</a><br/>");
          out.print("<a href=\"" + entireLogUrl + "\">All</a><br/>");
        }
        if (hasCleanupAttempt) {
          out.print("Cleanup attempt: <br/>");
          taskLogUrl = null;
          if (cleanupTracker != null ) {
        	taskLogUrl = TaskLogServlet.getTaskLogUrl(cleanupTracker.getHost(),
                                String.valueOf(cleanupTracker.getHttpPort()),
                                status.getTaskID().toString());
      	  }
          if (taskLogUrl == null) {
            out.print("n/a");
          } else {
            String tailFourKBUrl = taskLogUrl + "&start=-4097&cleanup=true";
            String tailEightKBUrl = taskLogUrl + "&start=-8193&cleanup=true";
            String entireLogUrl = taskLogUrl + "&all=true&cleanup=true";
            out.print("<a href=\"" + tailFourKBUrl + "\">Last 4KB</a><br/>");
            out.print("<a href=\"" + tailEightKBUrl + "\">Last 8KB</a><br/>");
            out.print("<a href=\"" + entireLogUrl + "\">All</a><br/>");
          }
        }
        out.print("</td><td>" + "<a href=\"" + getProxyUrl(statsUrl, "jobid=" + jobId
          + "&tipid=" + tipid + "&taskid=" + status.getTaskID()) + "\">"
          + ((status.getCounters() != null) ? status.getCounters().size() : 0) + "</a></td>");
        out.print("<td>");
        if (status.getRunState() == TaskStatus.State.RUNNING) {
          out.print("<a href=\"" + getProxyUrl(detailsUrl, "action=confirm"
              + "&subaction=kill-task" + "&jobid=" + jobId + "&tipid="
              + tipid + "&taskid=" + status.getTaskID()) + "\" > Kill </a>");
          out.print("<br><a href=\"" + getProxyUrl(detailsUrl, "action=confirm"
              + "&subaction=fail-task" + "&jobid=" + jobId + "&tipid="
              + tipid + "&taskid=" + status.getTaskID()) + "\" > Fail </a>");
          if (tip != null && !tip.isSpeculativeForced()) {
            out.print("<br><a href=\"" + getProxyUrl(detailsUrl, "action=confirm"
                + "&subaction=speculative-task" + "&jobid=" + jobId + "&tipid="
                + tipid + "&taskid=" + status.getTaskID())
                + "\" > Speculate </a>");
          } else {
            out.print("<br>Speculative");
          }
          if (taskLogUrl != null) {
            String stackTracingUrl = taskLogUrl + "&stacktracing=true&start=-10240&filter=stdout";
            if (hasCleanupAttempt) {
            	stackTracingUrl += "&cleanup=true";
            }
          	out.print("<br><a href=\"" + stackTracingUrl + "\" > StackTrace </a>");
          }
        } else if (status.getRunState() == TaskStatus.State.SUCCEEDED) {
          // Allow failing succeeded tasks.
          out.print("<a href=\"" + getProxyUrl(detailsUrl, "action=confirm"
              + "&subaction=fail-task" + "&jobid=" + jobId + "&tipid="
              + tipid + "&taskid=" + status.getTaskID()) + "\" > Fail </a>");
        } else {
          out.print("<pre>&nbsp;</pre>");
        }
        out.println("</td></tr>");
      }
  %>
</table>
</center>

<%
      if (ts.length > 0 && ts[0].getIsMap() && !isCleanupOrSetup) {
%>
<h3>Input Split Locations</h3>
<table border=2 cellpadding="5" cellspacing="2">
<%
        for (String split: StringUtils.split(tracker.getTip(
                                         tipidObj).getSplitNodes())) {
          out.println("<tr><td>" + split + "</td></tr>");
        }
%>
</table>
<%    
    }
%>

<hr>
<%
  out.println("<a href=\"" +
              getProxyUrl(jobUrl, "jobid=" + jobId.toString()) + 
              "\">Go back to the job</a><br>");
  out.println(ServletUtil.htmlFooter());
%>
