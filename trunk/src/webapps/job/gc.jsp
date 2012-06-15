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
  boolean doGc = false;
  double threshold = 100; // default percentage, no throttling

  // is the threshold specified by the client?
  String clientThrehold = request.getParameter("threshold");
  if (clientThrehold != null) {
    try {
      threshold = Integer.parseInt(clientThrehold);
    }
      catch (NumberFormatException ignored) {
    }
  }

%>
<%!
  public void dumpJobInformation(JobTracker tracker) throws IOException {
    List<JobInProgress> runningJobs = tracker.getRunningJobs();
    List<JobInProgress> completedJobs = tracker.getCompletedJobs();
    List<JobInProgress> failedJobs = tracker.getFailedJobs();
    if (completedJobs.size() > 0) {
      tracker.LOG.info("COMPLETED JOBS:---------");
      tracker.LOG.info(JSPUtil.generateJobTable("Completed", completedJobs, 0,
                       runningJobs.size()));
      tracker.LOG.info("<hr>");
    }
    if (failedJobs.size() > 0) {
      tracker.LOG.info("FAILED JOBS:");
      tracker.LOG.info(JSPUtil.generateJobTable("Failed", failedJobs, 0,
                       (runningJobs.size()+completedJobs.size())));
      tracker.LOG.info("<hr>");
    }
    tracker.LOG.info("RETIRED JOBS:");
    tracker.LOG.info(JSPUtil.generateRetiredJobTable(tracker,
                     (runningJobs.size()+completedJobs.size()+failedJobs.size())));
    tracker.LOG.info("--------------<hr>");
  }

  public boolean throttle(JspWriter out, ClusterStatus status,
                          JobTracker tracker, double threshold) throws IOException {
    if (status.getUsedMemory() > (threshold * status.getMaxMemory())/100) {
      tracker.LOG.info("Exceeding configured memory threshold of " +
                        threshold + " percent. " +
                       "Current usage is " + status.getUsedMemory() +
                       " from a configured maximum of " + status.getMaxMemory() + ". " +
                       " Retiring all possible jobs.");
      ReflectionUtils.printThreadInfo(new PrintWriter(System.out), "Throttling new jobs");
      tracker.retireCompletedJobs();
      dumpJobInformation(tracker);
      return true;
    }
    return false;
  }%>

<html>
<head>
<title><%= trackerName %> Hadoop Map/Reduce Throttler</title>
<link rel="stylesheet" type="text/css" href="/static/hadoop.css">
</head>
<body>

<%
    doGc = throttle(out, status, tracker, threshold);
%>

<br>
<dogc><%= doGc ? "true" : "false" %></dogc>
<usedmemory><%= status.getUsedMemory() %></usedmemory>
<maxmemory><%= status.getMaxMemory() %></totalmemory>
<br>

<%
out.println(ServletUtil.htmlFooter());
%>
