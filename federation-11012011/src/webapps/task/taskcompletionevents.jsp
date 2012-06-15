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

<title><%= trackerName %> Task Tracker Completion Events</title>

<body>
<h1><%= trackerName %> Task Tracker Completion Events</h1>
<img src="/static/hadoop-logo.jpg"/><br>
<b>Version:</b> <%= VersionInfo.getVersion()%>,
                r<%= VersionInfo.getRevision()%><br>
<b>Compiled:</b> <%= VersionInfo.getDate()%> by 
                 <%= VersionInfo.getUser()%><br>
Go back to <a href="tasktracker.jsp">Task Tracker</a><br>


<h2>Jobs with Reduce Tasks in Shuffling Phase</h2>
<center>

<%
      List <TaskTracker.FetchStatus> fs = tracker.reducesInShuffle();
      for(TaskTracker.FetchStatus f: fs) {
          out.print("<br>" + "NextId: " + f.fromEventId.get() + "<br>");
          
          TaskCompletionEvent[] events = f.getMapEvents(0, 1000000);
          out.print("<table border=2 cellpadding=\"5\" cellspacing=\"2\">");
          out.print("<tr><td align=\"center\">ID</td><td>Event ID</td><td>Task ID</td><td>ID within Job</td>" +
                    "<td>Type</td><td>Status</td></tr>");

          for(int i=0; i<events.length; i++) {
            TaskCompletionEvent report = events[i];
            String url = report.getTaskTrackerHttp();
            out.print("<tr>");
            out.print("<td><a href=\"" + url + "\">" + i +"</a></td>");
            out.print("<td>" + report.getEventId() + "</td>");
            out.print("<td>" + report.getTaskId() + "</td>");
            out.print("<td>" + report.idWithinJob() + "</td>");
            out.print("<td>" + (report.isMapTask() ? "Map" : "Reduce") + "</td>");
            out.print("<td>" + report.getTaskStatus().name() + "</td>");
          }
          out.print("</table>");
      }      
%>

</center>


<h2>Local Logs</h2>
<a href="/logs/">Log</a> directory

<hr>
<a href="http://lucene.apache.org/hadoop">Hadoop</a>, 2007.<br>
</body>
</html>
