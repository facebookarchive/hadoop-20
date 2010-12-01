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
  List<JobInProgress> runningJobs = tracker.getRunningJobs();
  List<JobInProgress> completedJobs = tracker.getCompletedJobs();
  List<JobInProgress> failedJobs = tracker.getFailedJobs();
	String jobStatus = request.getParameter("jobstatus");
	if (jobStatus == null) {
	  out.write(JSPUtil.generateTxtJobTable(runningJobs, tracker));
	} else if (jobStatus.equals("completed")) {
	  out.write(JSPUtil.generateTxtJobTable(completedJobs, tracker));
	} else if (jobStatus.equals("failed")) {
	  out.write(JSPUtil.generateTxtJobTable(failedJobs, tracker));
	} 
%>