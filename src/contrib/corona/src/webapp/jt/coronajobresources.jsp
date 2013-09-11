<%@ page
  contentType="text/html; charset=UTF-8"
  import="javax.servlet.*"
  import="javax.servlet.http.*"
  import="java.io.*"
  import="java.util.*"
  import="org.apache.hadoop.mapred.*"
  import="org.apache.hadoop.util.*"
  import="java.lang.Integer"
%>
<%! 

	private String tasksUrl = null;
	private String jobUrl = null;
	private boolean hasProxy = false;

    private String getProxyUrl(String proxyPath, String params) {
      return proxyPath + (hasProxy ? "&" : "?") + params;
    }
%>
<%
  CoronaJobTracker tracker = (CoronaJobTracker) application.getAttribute("job.tracker");
  CoronaJobInProgress job = (CoronaJobInProgress) tracker.getJob();
  jobUrl = tracker.getProxyUrl("coronajobdetails.jsp");
  tasksUrl = tracker.getProxyUrl("coronajobtasks.jsp");
  hasProxy = (tasksUrl.indexOf('?') != -1);

  String jobId = request.getParameter("jobid");
  if (jobId == null) {
    out.println("<h2>Missing 'jobid'!</h2>");
    return;
  }
  JobID jobIdObj = JobID.forName(jobId);
  String kind = request.getParameter("kind");

  List<ResourceReport> resourceReportList =
      (job != null) ? tracker.getResourceReportList(kind) : null;
%>

<html>
  <head>
    <title>Hadoop <%=kind%> resource list for <%=jobId%> </title>
    <link rel="stylesheet" type="text/css" href="/static/hadoop.css">
  </head>
<body>
<h1>Hadoop <%=kind%> resource list for
<%
  out.print("<a href=\"" + jobUrl + "\">" + jobId + "</a>");

  if (job == null) {
    out.print("<b>Job not found.</b><br>\n");
    return;
  }

  if (resourceReportList.isEmpty()) {
    out.print("<b>No resource reports</b>");
  } else {
    out.print("<hr>");
    out.print("<h2>Resources</h2>");
    out.print("<center>");
    out.print("<table border=2 cellpadding=\"5\" cellspacing=\"2\">");
    out.print("<tr><td align=\"center\">Grant Id</td><td>Task</td></tr>");
    for (ResourceReport report : resourceReportList) {
      out.print("<tr><td>" + report.getGrantId() + "</td><td>" +
                report.getTaskAttemptString() + "</td>");
    }
    out.print("</table>");
    out.print("</center>");
  }
%>

<hr>
<%
  out.println("<a href=\"" + getProxyUrl(jobUrl, "jobid=" + jobId) + "\">Go back to JobDetails</a><br>");
  out.println(ServletUtil.htmlFooter());
%>
