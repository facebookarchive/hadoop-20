<%@ page
  contentType="text/html; charset=UTF-8"
  import="javax.servlet.*"
  import="javax.servlet.http.*"
  import="java.io.*"
  import="java.util.*"
  import="java.text.DecimalFormat"
  import="org.apache.hadoop.mapred.*"
  import="org.apache.hadoop.util.*"
%>
<%
  TaskTracker tracker = (TaskTracker) application.getAttribute("task.tracker");
  String trackerName = tracker.getName();
%>

<html>

<title><%= trackerName %> Task Tracker Status</title>

<body>
<h1><%= trackerName %> Task Tracker Status</h1>
<img src="/static/hadoop-logo.jpg"/><br>
<b>Version:</b> <%= VersionInfo.getVersion()%>,
                r<%= VersionInfo.getRevision()%><br>
<b>Compiled:</b> <%= VersionInfo.getDate()%> by 
                 <%= VersionInfo.getUser()%><br>
Go to <a href="taskcompletionevents.jsp">Task Completion Events</a><br>

<h2>Running tasks</h2>
<center>
<table border=2 cellpadding="5" cellspacing="2">
<tr><td align="center">Task Attempts</td><td>Status</td>
    <td>Progress</td><td>Errors</td></tr>

  <%
     Iterator itr = tracker.getRunningTaskStatuses().iterator();
     while (itr.hasNext()) {
       TaskStatus status = (TaskStatus) itr.next();
       out.print("<tr><td>" + status.getTaskID());
       out.print("</td><td>" + status.getRunState()); 
       out.print("</td><td>" + 
                 StringUtils.formatPercent(status.getProgress(), 2));
       out.print("</td><td><pre>" + status.getDiagnosticInfo() + "</pre></td>");
       out.print("</tr>\n");
     }
  %>
</table>
</center>

<h2>Non-Running Tasks</h2>
<table border=2 cellpadding="5" cellspacing="2">
<tr><td align="center">Task Attempts</td><td>Status</td>
  <%
    for(TaskStatus status: tracker.getNonRunningTasks()) {
      out.print("<tr><td>" + status.getTaskID() + "</td>");
      out.print("<td>" + status.getRunState() + "</td></tr>\n");
    }
  %>
</table>


<h2>Tasks from Running Jobs</h2>
<center>
<table border=2 cellpadding="5" cellspacing="2">
<tr><td align="center">Task Attempts</td><td>Status</td>
    <td>Progress</td><td>Errors</td></tr>

  <%
     itr = tracker.getTasksFromRunningJobs().iterator();
     while (itr.hasNext()) {
       TaskStatus status = (TaskStatus) itr.next();
       out.print("<tr><td>" + status.getTaskID());
       out.print("</td><td>" + status.getRunState()); 
       out.print("</td><td>" + 
                 StringUtils.formatPercent(status.getProgress(), 2));
       out.print("</td><td><pre>" + status.getDiagnosticInfo() + "</pre></td>");
       out.print("</tr>\n");
     }
  %>
</table>
</center>


  <%
    if (tracker.checkCGroupMemoryWatcherEnabled()) {
      CGroupMemoryWatcher.CGroupMemStat memStat= tracker.getCGroupMemStat();
      out.print("<h2>Memory CGroup Status</h2>\n");
      out.print("<table border=2 cellpadding=\"5\" cellspacing=\"2\">\n");
      out.print("<tr><td>JobTracker Max Memory Used</td>" +
                     "<td>JobTracker Current Memory Usage</td>" +
                     "<td>TaskTracker Max Memory Used</td>" +
                     "<td>TaskTracker Current Memory Usage</td></tr>\n");
      out.print("<tr><td>" + memStat.getJTMaxMemoryUsed() + "</td>" +
                    "<td>" + memStat.getJTMemoryUsage() + "</td>" +
                    "<td>" + memStat.getTTMaxMemoryUsed() + "</td>" +
                    "<td>" + memStat.getTTMemoryUsage() + "</td>"); 
      out.print("</table>\n");

      out.print("<h2>TaskTracker Container Memory CGroup Status</h2>\n");
      out.print("<table border=2 cellpadding=\"5\" cellspacing=\"2\">\n");
      out.print("<tr><td>Memory Limit</td><td>Max Memory Used</td>");
      out.print("<td>Current Memory Usage</td><td>Low Memory Threshold</td>");
      out.print("<td>Memory Used by Tasks</tr>\n");
      out.print("<tr><td>" + memStat.getMemoryLimit() + "</td>" +
                    "<td>" + memStat.getMaxMemoryUsed() + "</td>" + 
                    "<td>" + memStat.getMemoryUsage() + "</td>" + 
                    "<td>" + memStat.getLowMemoryThreshold() + "</td>" +
                    "<td>" + memStat.getMemoryUsedByTasks() + "</td></tr>\n");
      out.print("</table>\n");

      out.print("<h2>Memory CGroup Recently Killed Tasks</h2>\n");
      out.print("<table border=2 cellpadding=\"5\" cellspacing=\"2\">\n");
      out.print("<tr><td align=\"center\">Task Attempts</td><td>Memory Used</td>");
      out.print("<td>Started</td><td>Killed</td></tr>\n");
      Iterator ptItr = memStat.getTasks().iterator();
      while (ptItr.hasNext()) {
        CGroupMemoryWatcher.CGroupProcessTreeInfo ptInfo =
          (CGroupMemoryWatcher.CGroupProcessTreeInfo) ptItr.next();
        out.print("<tr><td>" + ptInfo.getTID()+ "</td>");
        out.print("<td>" + ptInfo.getMemoryUsed() + "</td>");
        out.print("<td>" + (new Date(ptInfo.getCreationTime())) + "</td>");
        out.print("<td>" + (new Date(ptInfo.getKillTime())) + "</td></tr>\n");
      }
      out.print("</table>\n");
    }
  %>

<h2>Local Logs</h2>
<a href="/logs/">Log</a> directory

<%
out.println(ServletUtil.htmlFooter());
%>
