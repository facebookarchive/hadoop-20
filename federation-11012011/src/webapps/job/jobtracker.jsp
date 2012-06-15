<%@ page
  contentType="text/html; charset=UTF-8"
  import="javax.servlet.*"
  import="javax.servlet.http.*"
  import="java.io.*"
  import="java.util.*"
  import="java.text.DecimalFormat"
  import="org.apache.hadoop.mapred.*"
  import="org.apache.hadoop.mapreduce.*"
  import="org.apache.hadoop.util.*"
%>
<%
  JobTracker tracker = (JobTracker) application.getAttribute("job.tracker");
  ClusterStatus status = tracker.getClusterStatus();
  ClusterMetrics metrics = tracker.getClusterMetrics();
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
  
  public void generateSummaryTable(JspWriter out, ClusterMetrics metrics,
                                   JobTracker tracker) throws IOException {
    String tasksPerNode = metrics.getTaskTrackerCount() > 0 ?
      percentFormat.format(((double)(metrics.getMapSlotCapacity() +
      metrics.getReduceSlotCapacity())) / metrics.getTaskTrackerCount()):
      "-";
    out.print("<table border=\"1\" cellpadding=\"5\" cellspacing=\"0\">\n"+
              "<tr><th>Running Map Tasks</th><th>Running Reduce Tasks</th>" + 
              "<th>Total Submissions</th>" +
              "<th>Nodes</th>" + 
              "<th>Occupied Map Slots</th><th>Occupied Reduce Slots</th>" + 
              "<th>Reserved Map Slots</th><th>Reserved Reduce Slots</th>" + 
              "<th>Map Task Capacity</th>" +
              "<th>Reduce Task Capacity</th><th>Avg. Tasks/Node</th>" + 
              "<th>Blacklisted Nodes</th>" +
              "<th>Excluded Nodes</th></tr>\n");
    out.print("<tr><td>" + metrics.getRunningMaps() + "</td><td>" +
              metrics.getRunningReduces() + "</td><td>" + 
              metrics.getTotalJobSubmissions() +
              "</td><td><a href=\"machines.jsp?type=active\">" +
              metrics.getTaskTrackerCount() + "</a></td><td>" + 
              metrics.getOccupiedMapSlots() + "</td><td>" +
              metrics.getOccupiedReduceSlots() + "</td><td>" + 
              metrics.getReservedMapSlots() + "</td><td>" +
              metrics.getReservedReduceSlots() + "</td><td>" + 
              metrics.getMapSlotCapacity() +
              "</td><td>" + metrics.getReduceSlotCapacity() +
              "</td><td>" + tasksPerNode +
              "</td><td><a href=\"machines.jsp?type=blacklisted\">" +
              metrics.getBlackListedTaskTrackerCount() + "</a>" +
              "</td><td><a href=\"machines.jsp?type=excluded\">" +
              metrics.getDecommissionedTaskTrackerCount() + "</a>" +
              "</td></tr></table>\n");

    out.print("<br>");
  }%>


<html>
<head>
<title><%= trackerName %> Hadoop Map/Reduce Administration</title>
<link rel="stylesheet" type="text/css" href="/static/hadoop.css">
<script type="text/javascript" src="/static/jobtracker.js"></script>
</head>
<body>

<%
// This will redirect the jobtracker.jsp to jobtracker_hmon.jsp.
// To turn it off, put a file named "turn_off_hmon" in $HADOOP_HOME/conf/
  java.io.File tokenFile = new java.io.File("conf/turn_off_hmon");
  if (!tokenFile.exists()) {
    String redirectURL = "jobtracker_hmon.jsp";
    response.sendRedirect(redirectURL);
  }
%>

<% JSPUtil.processButtons(request, response, tracker); %>

<h1><%= trackerName %> Hadoop Map/Reduce Administration</h1>

<div id="quicklinks">
  <a href="#quicklinks" onclick="toggle('quicklinks-list'); return false;">Quick Links</a>
  <ul id="quicklinks-list">
    <li><a href="#scheduling_info">Scheduling Info</a></li>
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
 generateSummaryTable(out, metrics, tracker); 
%>
<hr>
<h2 id="scheduling_info">Scheduling Information</h2>
<table border="2" cellpadding="5" cellspacing="2">
<thead style="font-weight: bold">
<tr>
<td> Queue Name </td>
<td> Scheduling Information</td>
</tr>
</thead>
<tbody>
<%
for(JobQueueInfo queue: queues) {
  String queueName = queue.getQueueName();
  String schedulingInformation = queue.getSchedulingInfo();
  if(schedulingInformation == null || schedulingInformation.trim().equals("")) {
    schedulingInformation = "NA";
  }
%>
<tr>
<td><a href="jobqueue_details.jsp?queueName=<%=queueName%>"><%=queueName%></a></td>
<td><%=schedulingInformation.replaceAll("\n","<br/>") %>
</td>
</tr>
<%
}
%>
</tbody>
</table>
<hr/>
<b>Filter (Jobid, Priority, User, Name)</b> <input type="text" id="filter" onkeyup="applyfilter()"> <br>
<span class="small">Example: 'user:smith 3200' will filter by 'smith' only in the user field and '3200' in all fields</span>
<hr>

<h2 id="running_jobs">Running Jobs</h2>
<%
if (request.getParameter("jobid") == null) {
  out.print(JSPUtil.generateJobTable("Running", runningJobs, 30, 0));
} else {
  out.print(JSPUtil.generateJobTable("Running", filterJobs(runningJobs, request.getParameter("jobid").split(",")), 30, 0));
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
