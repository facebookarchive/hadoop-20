<%@ page
  contentType="text/html; charset=UTF-8"
  import="javax.servlet.*"
  import="javax.servlet.http.*"
  import="java.io.*"
  import="java.util.*"
  import="java.text.DecimalFormat"
  import="org.apache.hadoop.mapred.*"
  import="org.apache.hadoop.util.*"
  import="org.apache.hadoop.mapreduce.*"
%>
<%
  JobTracker tracker = (JobTracker) application.getAttribute("job.tracker");
  String trackerName =
           StringUtils.simpleHostname(tracker.getJobTrackerMachine());
  String type = request.getParameter("type");
%>
<%!
  public void generateTaskTrackerTable(JspWriter out,
                                       String type,
                                       JobTracker tracker) throws IOException {
    Collection<TaskTrackerStatus> c;
    if (("blacklisted").equals(type)) {
      out.println("<h2>Blacklisted Task Trackers</h2>");
      c = tracker.blacklistedTaskTrackers();
    } else if (("active").equals(type)) {
      out.println("<h2>Active Task Trackers</h2>");
      c = tracker.activeTaskTrackers();
    } else {
      out.println("<h2>Task Trackers</h2>");
      c = tracker.taskTrackers();
    }
    List<TaskTrackerStatus> tasktrackers = new ArrayList<TaskTrackerStatus>(c);
    Collections.sort(tasktrackers, new Comparator<TaskTrackerStatus>() {
      public int compare(TaskTrackerStatus t1, TaskTrackerStatus t2) {
        return t1.getHost().compareTo(t2.getHost());
      }});
    int noCols = 9 +
      (2 * tracker.getStatistics().collector.DEFAULT_COLLECT_WINDOWS.length);
    if(type.equals("blacklisted")) {
      noCols = noCols + 1;
    }
    if (tasktrackers.size() == 0) {
      out.print("There are currently no known " + type + " Task Trackers.");
    } else {
      out.print("<center>\n");
      out.print("<table border=\"2\" cellpadding=\"5\" cellspacing=\"2\" class=\"tablesorter\">\n");
      out.print("<thead><tr><td align=\"center\" colspan=\""+ noCols +"\"><b>Task Trackers</b></td></tr>\n");
      out.print("<tr><th><b>Name</b></th><th><b>Host</b></th>" +
                "<th><b># running tasks</b></th>" +
                "<th><b>Max Map Tasks</b></th>" +
                "<th><b>Max Reduce Tasks</b></th>" +
                "<th><b>Failures</b></th>" +
                "<th><b>Node Health Status</b></th>" +
                "<th><b>Seconds Since Node Last Healthy</b></th>");
      if(type.equals("blacklisted")) {
      	out.print("<th><b>Reason For blacklisting</b></th>");
      }
      for(StatisticsCollector.TimeWindow window : tracker.getStatistics().
           collector.DEFAULT_COLLECT_WINDOWS) {
         out.println("<th><b>Total Tasks "+window.name+"</b></th>");
         out.println("<th><b>Succeeded Tasks "+window.name+"</b></th>");
       }

      out.print("<th><b>Seconds since heartbeat</b></th></tr></thead><tbody>\n");

      int maxFailures = 0;
      String failureKing = null;
      TaskScheduler scheduler = tracker.getTaskScheduler();
      for (TaskTrackerStatus tt : tasktrackers) {
        long sinceHeartbeat = System.currentTimeMillis() - tt.getLastSeen();
        boolean isHealthy = tt.getHealthStatus().isNodeHealthy();
        long sinceHealthCheck = tt.getHealthStatus().getLastReported();
        String healthString = "";
        if(sinceHealthCheck == 0) {
          healthString = "N/A";
        } else {
          healthString = (isHealthy?"Healthy":"Unhealthy");
          sinceHealthCheck = System.currentTimeMillis() - sinceHealthCheck;
          sinceHealthCheck =  sinceHealthCheck/1000;
        }
        if (sinceHeartbeat > 0) {
          sinceHeartbeat = sinceHeartbeat / 1000;
        }
        int numCurTasks = 0;
        for (Iterator it2 = tt.getTaskReports().iterator(); it2.hasNext(); ) {
          it2.next();
          numCurTasks++;
        }
        int numFailures = tt.getFailures();
        if (numFailures > maxFailures) {
          maxFailures = numFailures;
          failureKing = tt.getTrackerName();
        }
        out.print("<tr><td><a href=\"http://");
        out.print(tt.getHost() + ":" + tt.getHttpPort() + "/\">");
        out.print(tt.getTrackerName() + "</a></td><td>");
        int maxMaps =
         scheduler.getMaxSlots(tt, org.apache.hadoop.mapreduce.TaskType.MAP);
        int maxReduces =
         scheduler.getMaxSlots(tt, org.apache.hadoop.mapreduce.TaskType.REDUCE);
        out.print(tt.getHost() + "</td><td>" + numCurTasks +
                  "</td><td>" + maxMaps +
                  "</td><td>" + maxReduces +
                  "</td><td>" + numFailures +
                  "</td><td>" + healthString +
                  "</td><td>" + sinceHealthCheck);
        if(type.equals("blacklisted")) {
          out.print("</td><td>"
                + "<a href=\"tasktrackerfaultstatus.jsp?host="
                  + tt.getHost() + "\">"
                  + tracker.getReasonsForBlacklisting(tt.getHost())
                + "</a>");
        }
        for(StatisticsCollector.TimeWindow window : tracker.getStatistics().
          collector.DEFAULT_COLLECT_WINDOWS) {
          JobTrackerStatistics.TaskTrackerStat ttStat = tracker.getStatistics().
             getTaskTrackerStat(tt.getTrackerName());
          out.println("</td><td>" + ttStat.totalTasksStat.getValues().
                                get(window).getValue());
          out.println("</td><td>" + ttStat.succeededTasksStat.getValues().
                                get(window).getValue());
        }

        out.print("</td><td>" + sinceHeartbeat + "</td></tr>\n");
      }
      out.print("</tbody></table>\n");
      out.print("</center>\n");
      if (maxFailures > 0) {
        out.print("Highest Failures: " + failureKing + " with " + maxFailures +
                  " failures<br>\n");
      }
    }
  }

  public void generateTableForHostnames(JspWriter out, JobTracker tracker,
                                            String type)
  throws IOException {
    // excluded or dead nodes
    List<String> d = null;
    if ("dead".equals(type)) {
      out.println("<h2>Dead Nodes</h2>");
      d = new ArrayList<String>(tracker.getDeadNodes());
    } else if ("excluded".equals(type)) {
      out.println("<h2>Excluded Nodes</h2>");
      d = new ArrayList<String>(tracker.getExcludedNodes());
    }
    if (d.size() == 0) {
      out.print("There are currently no matching hosts.");
    } else {
      Collections.sort(d);
      out.print("<center>\n");
      out.print("<table border=\"2\" cellpadding=\"5\" cellspacing=\"2\" class=\"tablesorter\">\n");
      out.print("<thead><tr>");
      out.print("<th><b>Host Name</b></th></tr></thead><tbody>\n");
      for (Iterator it = d.iterator(); it.hasNext(); ) {
        String dt = (String)it.next();
        out.print("<td>" + dt + "</td></tr>\n");
      }
      out.print("</tbody></table>\n");
      out.print("</center>\n");
    }
  }
%>

<html>
<head>
<title><%=trackerName%> Hadoop Machine List</title>
<link rel="stylesheet" type="text/css" href="/static/hadoop.css">
<link rel="stylesheet" type="text/css" href="/static/tablesorter/style.css">
<script type="text/javascript" src="/static/jobtracker.js"></script>
<script type="text/javascript" src="/static/jquery-1.7.1.min.js"></script>
<script type="text/javascript" src="/static/tablesorter/jquery.tablesorter.js"></script>
<script type="text/javascript" src="/static/tablesorter/jobtablesorter.js"></script>
</head>
<body>
<h1><a href="jobtracker.jsp"><%=trackerName%></a> Hadoop Machine List</h1>

<%
  if ("excluded".equals(type) || "dead".equals(type)) {
    generateTableForHostnames(out, tracker, type);
  } else {
    generateTaskTrackerTable(out, type, tracker);
  }
%>

<%
out.println(ServletUtil.htmlFooter());
%>
