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
  JobTracker tracker = (JobTracker) application.getAttribute("job.tracker");
  ClusterStatus status = tracker.getClusterStatus();
  String trackerName =
           StringUtils.simpleHostname(tracker.getJobTrackerMachine());
  JobQueueInfo[] queues = tracker.getQueues();
  List<JobInProgress> runningJobs = tracker.getRunningJobs();
  List<JobInProgress> completedJobs = tracker.getCompletedJobs();
  List<JobInProgress> failedJobs = tracker.getFailedJobs();
%>
<%!
  private static DecimalFormat percentFormat = new DecimalFormat("##0.00");

  //  Filter the jobs by a list of jobids
  //  @param jobs  the jobs to be filtered
  //  @param jobIds  the job ids that needs to be retained
  //  @return  filtered jobs
  public List<JobInProgress> filterJobs(List<JobInProgress> jobs, String[] jobIds) {
    List<JobInProgress> filteredJobs = new LinkedList<JobInProgress>();
    Set<String> jobIdSet = new HashSet<String>(Arrays.asList(jobIds));
    for (JobInProgress job : jobs) {
      if (jobIdSet.contains(job.getJobID().toString())) {
        filteredJobs.add(job);
      }
    }
    return filteredJobs;
  }

  public void generateSummaryTable(JspWriter out, ClusterStatus status,
                                   JobTracker tracker) throws IOException {
    String tasksPerNode = status.getTaskTrackers() > 0 ?
      percentFormat.format(((double)(status.getMaxMapTasks() +
               status.getMaxReduceTasks())) / status.getTaskTrackers()) : "-";
    out.print("<table border=\"1\" cellpadding=\"5\" cellspacing=\"0\">\n" +
              "<tr><th colspan=7>Maps</th>" +
              "<th colspan=7>Reduces</th>" +
              "<th colspan=4>Nodes</th>" +
              "<th colspan=4>Jobs</th>" +
              "<th rowspan=2>Avg. Tasks/Node</th></tr>\n");
    out.print("<tr><td>Running</td><td>Speculative</td>" +
              "<td>Waiting</td><td>Complete</td>" +
              "<td>Total</td><td>Capacity</td>" +
              "<td>Idle</td>" +
              "<td>Running</td><td>Speculative</td>" +
              "<td>Waiting</td><td>Complete</td>" +
              "<td>Total</td><td>Capacity</td>" +
              "<td>Idle</td>" +
              "<td>Total</td><td>Blacklisted</td><td>Excluded</td>" +
              "<td>Dead</td>"+
              "<td>Running</td><td>Preparing</td><td>Failed</td><td>Total</td></tr>\n");
    // Compute the total and finished tasks
    int totalMaps = 0;
    int completeMaps = 0;
    int totalReduces = 0;
    int completeReduces = 0;
    int pendingMaps = 0;
    int pendingReduces = 0;
    int runningJobs = tracker.getRunningJobs().size();
    int preparingJobs = tracker.getPreparingJobs().size();
    int failedJobs = tracker.getFailedJobs().size();
    for (JobInProgress job : tracker.getRunningJobs()) {
      totalMaps += job.desiredMaps();
      totalReduces += job.desiredReduces();
      completeMaps += job.finishedMaps();
      completeReduces += job.finishedReduces();
      pendingMaps += job.pendingMaps();
      pendingReduces += job.pendingReduces();
    }
    int runMaps = status.getMapTasks();
    int capMaps = status.getMaxMapTasks();
    int idleMaps = capMaps - runMaps;
    int speculativeMaps = JobInProgress.getTotalSpeculativeMapTasks();
    int runReduces = status.getReduceTasks();
    int capReduces = status.getMaxReduceTasks();
    int idleReduces = capReduces - runReduces;
    int speculativeReduces = JobInProgress.getTotalSpeculativeReduceTasks();
    out.print("<tr><td>" + runMaps + "</td>" +
              "<td>" + speculativeMaps + "</td>" +
              "<td>" + pendingMaps + "</td>" +
              "<td>" + completeMaps + "</td>" +
              "<td>" + totalMaps + "</td>" +
              "<td>" + capMaps + "</td>" +
              "<td>" + idleMaps + "</td>" +
              "<td>" + runReduces + "</td>" +
              "<td>" + speculativeReduces + "</td>" +
              "<td>" + pendingReduces + "</td>" +
              "<td>" + completeReduces + "</td>" +
              "<td>" + totalReduces + "</td>" +
              "<td>" + capReduces + "</td>" +
              "<td>" + idleReduces + "</td>" +
              "<td><a href=\"machines.jsp?type=active\">" +
              status.getTaskTrackers() + "</a></td>" +
              "<td><a href=\"machines.jsp?type=blacklisted\">" +
              status.getBlacklistedTrackers() + "</a></td>" +
              "<td><a href=\"machines.jsp?type=excluded\">" +
              status.getNumExcludedNodes() + "</a></td>" +
              "<td><a href=\"machines.jsp?type=dead\">" +
              tracker.getDeadNodes().size() + "</a></td>" +
              "<td>" + runningJobs + "</td>" +
              "<td>" + preparingJobs + "</td>" +
              "<td>" + failedJobs + "</td>" +
              "<td>" + tracker.getTotalSubmissions() + "</td>" +
              "<td>" + tasksPerNode + "</td></tr></table>\n");
    out.print("<br>");
    out.print(JSPUtil.generateClusterResTable(tracker));
    out.print("<br>");
  }%>


<html>
<head>
<title><%= trackerName %> Hadoop Map/Reduce Administration</title>
<link rel="stylesheet" type="text/css" href="/static/hadoop.css">
<link rel="stylesheet" type="text/css" href="/static/tablesorter/style.css">
<script type="text/javascript" src="/static/jobtracker.js"></script>
<script type="text/javascript" src="/static/jquery-1.7.1.min.js"></script>
<script type="text/javascript" src="/static/tablesorter/jquery.tablesorter.js"></script>
<script type="text/javascript" src="/static/tablesorter/jobtablesorter.js"></script>
</head>
<body onload=applyfilter()>

<% JSPUtil.processButtons(request, response, tracker); %>

<h1><%= trackerName %> Hadoop Map/Reduce Administration</h1>

<div id="quicklinks">
  <a href="#quicklinks" onclick="toggle('quicklinks-list'); return false;">Quick Links</a>
  <ul id="quicklinks-list">
    <li><a href="#running_jobs">Running Jobs</a></li>
    <li><a href="#retired_jobs">Retired Jobs</a></li>
    <li><a href="#local_logs">Local Logs</a></li>
  </ul>
</div>

<b>State:</b> <%= status.getJobTrackerState() %><br>
<b>Started:</b> <%= new Date(tracker.getStartTime())%><br>
<b>Version:</b> <%= VersionInfo.getVersion()%>,
                r<%= VersionInfo.getRevision()%><br>
<b>Compiled:</b> <%= VersionInfo.getDate()%> by
                 <%= VersionInfo.getUser()%><br>
<b>Identifier:</b> <%= tracker.getTrackerIdentifier()%><br>

<hr>
<h2>Cluster Summary (Heap Size is <%= StringUtils.byteDesc(status.getUsedMemory()) %>/<%= StringUtils.byteDesc(status.getMaxMemory()) %>)</h2>
<%
 generateSummaryTable(out, status, tracker);
%>
<hr>
<h2><a href="fairscheduler">Scheduling Information</a></h2>
<hr>
<b>Filter (Jobid, Priority, User, Name)</b>
<%
out.print("<input type=\"text\" id=\"filter\" onkeyup=\"applyfilter()\"");
String filter = request.getParameter("filter");
if (filter == null) {
  out.print(">");
} else {
  out.print("value=\"" + filter + "\">");
}
%>
<br>
<span class="small">Example: 'user:smith 3200' will filter by 'smith' only in the user field and '3200' in all fields</span>
<hr>
<h2 id="running_jobs">Running Jobs</h2>
<%
if (request.getParameter("jobid") == null) {
  out.print(JSPUtil.generateJobTableWithResourceInfo("Running", runningJobs,
            30, 0, tracker));
} else {
  out.print(JSPUtil.generateJobTableWithResourceInfo("Running",
            filterJobs(runningJobs, request.getParameter("jobid").split(",")),
            30, 0, tracker));
}
%>
<hr>

<%
if (completedJobs.size() > 0) {
  out.print("<h2 id=\"completed_jobs\">Completed Jobs</h2>");
  out.print(JSPUtil.generateJobTable("Completed", completedJobs, 0,
    runningJobs.size()));
  out.print("<hr>");
}
%>

<%
if (failedJobs.size() > 0) {
  out.print("<h2 id=\"failed_jobs\">Failed Jobs</h2>");
  out.print(JSPUtil.generateJobTable("Failed", failedJobs, 0,
    (runningJobs.size()+completedJobs.size())));
  out.print("<hr>");
}
%>

<h2 id="retired_jobs">Retired Jobs</h2>
<%=JSPUtil.generateRetiredJobTable(tracker,
  (runningJobs.size()+completedJobs.size()+failedJobs.size()))%>
<hr>

<h2 id="local_logs">Local Logs</h2>
<a href="logs/">Log</a> directory, <a href="jobhistory.jsp">
Job Tracker History</a>

<%
out.println(ServletUtil.htmlFooter());
%>
