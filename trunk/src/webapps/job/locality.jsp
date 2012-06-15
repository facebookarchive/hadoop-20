<%@ page
  contentType="text/html; charset=UTF-8"
  import="javax.servlet.*"
  import="javax.servlet.http.*"
  import="java.io.*"
  import="java.util.*"
  import="java.text.DecimalFormat"
  import="org.apache.hadoop.mapred.*"
  import="org.apache.hadoop.util.*"
  import="org.apache.hadoop.fs.FsShell"
%>
<%
  JobTracker tracker = (JobTracker) application.getAttribute("job.tracker");
  ClusterStatus status = tracker.getClusterStatus();
  String trackerName =
           StringUtils.simpleHostname(tracker.getJobTrackerMachine());
%>
<html>
<head>
<title><%= trackerName %> Hadoop Locality Statistics</title>
<link rel="stylesheet" type="text/css" href="/static/hadoop.css">
</head>
<body>
<h1><%= trackerName %> Hadoop Locality Statistics</h1>

<b>State:</b> <%= status.getJobTrackerState() %><br>
<b>Started:</b> <%= new Date(tracker.getStartTime())%><br>
<b>Version:</b> <%= VersionInfo.getVersion()%>,
                r<%= VersionInfo.getRevision()%><br>
<b>Compiled:</b> <%= VersionInfo.getDate()%> by
                 <%= VersionInfo.getUser()%><br>
<b>Identifier:</b> <%= tracker.getTrackerIdentifier()%><br>

<hr>

<%
  Collection<JobInProgress> jobs = new ArrayList<JobInProgress>();
  jobs.addAll(tracker.completedJobs());
  jobs.addAll(tracker.runningJobs());
  jobs.addAll(tracker.failedJobs());
  int dataLocalMaps = 0;
  int rackLocalMaps = 0;
  int totalMaps = 0;
  int totalReduces = 0;
  for (JobInProgress job: jobs) {
    Counters counters = job.getCounters();
    dataLocalMaps += counters.getCounter(JobInProgress.Counter.DATA_LOCAL_MAPS);
    rackLocalMaps += counters.getCounter(JobInProgress.Counter.RACK_LOCAL_MAPS);
    totalMaps += counters.getCounter(JobInProgress.Counter.TOTAL_LAUNCHED_MAPS);
    totalReduces += counters.getCounter(JobInProgress.Counter.TOTAL_LAUNCHED_REDUCES);
  }
  int dataLocalMapPct = totalMaps == 0 ? 0 : (100 * dataLocalMaps) / totalMaps;
  int rackLocalMapPct = totalMaps == 0 ? 0 : (100 * rackLocalMaps) / totalMaps;
  int dataRackLocalMapPct = totalMaps == 0 ? 0 : (100 * (dataLocalMaps + rackLocalMaps)) / totalMaps;
%>
<p>
<b>Data Local Maps:</b> <%=dataLocalMaps%> (<%=dataLocalMapPct%>%) <br>
<b>Rack Local Maps:</b> <%=rackLocalMaps%> (<%=rackLocalMapPct%>%) <br>
<b>Data or Rack Local:</b> <%=dataLocalMaps + rackLocalMaps%> (<%=dataRackLocalMapPct%>%) <br>
<b>Total Maps:</b> <%=totalMaps%> <br>
<b>Total Reduces:</b> <%=totalReduces%> <br>
</p>

<%
out.println(ServletUtil.htmlFooter());
%>
