<%@ page
  contentType="text/html; charset=UTF-8"
  import="javax.servlet.*"
  import="javax.servlet.http.*"
  import="java.io.*"
  import="java.util.*"
  import="org.apache.hadoop.mapred.*"
  import="org.apache.hadoop.util.*"
  import="java.lang.Integer"
  import="java.text.SimpleDateFormat"
%>
<%! static SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss") ;

    private String detailsUrl = null;
    private String jobUrl = null;
    private String statsUrl = null;
    private String tasksUrl = null;
    private boolean hasProxy = false;

    private String getProxyUrl(String proxyPath, String params) {
      return proxyPath + (hasProxy ? "&" : "?") + params;
    }
%>
<%
  CoronaJobTracker tracker = (CoronaJobTracker) application.getAttribute("job.tracker");
  CoronaJobInProgress job = (CoronaJobInProgress) tracker.getJob();

  detailsUrl = tracker.getProxyUrl("coronataskdetails.jsp");
  jobUrl = tracker.getProxyUrl("coronajobdetails.jsp");
  statsUrl = tracker.getProxyUrl("coronataskstats.jsp");
  tasksUrl = tracker.getProxyUrl("coronajobtasks.jsp");
  hasProxy = (detailsUrl.indexOf('?') != -1);

  String type = request.getParameter("type");
  String pagenum = request.getParameter("pagenum");
  String state = request.getParameter("state");
  state = (state!=null) ? state : "all";
  int pnum = Integer.parseInt(pagenum);
  int next_page = pnum+1;
  int numperpage = 2000;

  JobID jobId = (job != null) ? job.getStatus().getJobID() : null;
  JobProfile profile = (job != null) ? (job.getProfile()) : null;
  JobStatus status = (job != null) ? (job.getStatus()) : null;
  TaskReport[] reports = null;
  int start_index = (pnum - 1) * numperpage;
  int end_index = start_index + numperpage;
  int report_len = 0;
  if ("map".equals(type)) {
    reports = (job != null) ? tracker.getMapTaskReports(jobId) : null;
  } else if ("reduce".equals(type)) {
    reports = (job != null) ? tracker.getReduceTaskReports(jobId) : null;
  } else if ("cleanup".equals(type)) {
    reports = (job != null) ? tracker.getCleanupTaskReports(jobId) : null;
  } else if ("setup".equals(type)) {
    reports = (job != null) ? tracker.getSetupTaskReports(jobId) : null;
  }
%>

<html>
  <head>
    <title>Hadoop <%=type%> task list for <%=jobId%> </title>
    <link rel="stylesheet" type="text/css" href="/static/hadoop.css">
  </head>
<body>
<h1>Hadoop <%=type%> task list for 
<%
  out.print("<a href=\"" + jobUrl + "\">" + jobId + "</a>");

  if (job == null) {
    out.print("<b>Job not found.</b><br>\n");
    return;
  }
  // Filtering the reports if some filter is specified
  if (!"all".equals(state)) {
    List<TaskReport> filteredReports = new ArrayList<TaskReport>();
    for (int i = 0; i < reports.length; ++i) {
      if (("completed".equals(state) && reports[i].getCurrentStatus() == TIPStatus.COMPLETE) 
          || ("running".equals(state) && reports[i].getCurrentStatus() == TIPStatus.RUNNING) 
          || ("killed".equals(state) && reports[i].getCurrentStatus() == TIPStatus.KILLED) 
          || ("pending".equals(state)  && reports[i].getCurrentStatus() == TIPStatus.PENDING)) {
        filteredReports.add(reports[i]);
      }
    }
    // using filtered reports instead of all the reports
    reports = filteredReports.toArray(new TaskReport[0]);
    filteredReports = null;
  }
  report_len = reports.length;
  
  if (report_len <= start_index) {
    out.print("<b>No such tasks</b>");
  } else {
    out.print("<hr>");
    out.print("<h2>" + Character.toUpperCase(state.charAt(0)) 
              + state.substring(1).toLowerCase() + " Tasks</h2>");
    out.print("<center>");
    out.print("<table border=2 cellpadding=\"5\" cellspacing=\"2\">");
    out.print("<tr><td align=\"center\">Task</td><td>Complete</td><td>Status</td>" +
              "<td>Start Time</td><td>Finish Time</td><td>Errors</td><td>Counters</td></tr>");
    if (end_index > report_len){
        end_index = report_len;
    }
    for (int i = start_index ; i < end_index; i++) {
        TaskReport report = reports[i];
        out.print("<tr><td><a href=\"" + getProxyUrl(detailsUrl, 
                                                     "tipid=" + report.getTaskID()) + "\">"  + 
                  report.getTaskID() + "</a></td>");
         out.print("<td>" + StringUtils.formatPercent(report.getProgress(),2) +
        		   ServletUtil.percentageGraph(report.getProgress() * 100f, 80) + "</td>");
         out.print("<td>"  + report.getState() + "<br/></td>");
         out.println("<td>" + StringUtils.getFormattedTimeWithDiff(dateFormat, report.getStartTime(),0) + "<br/></td>");
         out.println("<td>" + StringUtils.getFormattedTimeWithDiff(dateFormat, 
             report.getFinishTime(), report.getStartTime()) + "<br/></td>");
         String[] diagnostics = report.getDiagnostics();
         out.print("<td><pre>");
         for (int j = 0; j < diagnostics.length ; j++) {
             out.println(diagnostics[j]);
         }
         out.println("</pre><br/></td>");
         out.println("<td>" + 
                     "<a href=\"" +
                     getProxyUrl(statsUrl, "tipid=" + report.getTaskID()) +
                     "\">" + report.getCounters().size() +
                     "</a></td></tr>");
    }
    out.print("</table>");
    out.print("</center>");
  }
  if (end_index < report_len) {
    out.print("<div style=\"text-align:right\">" + 
              "<a href=\"" + getProxyUrl(tasksUrl, "type=" + type +
                                         "&pagenum=" + next_page + "&state=" + state) +
              "\">" + "Next" + "</a></div>");
  }
  if (start_index != 0) {
      out.print("<div style=\"text-align:right\">" + 
                "<a href=\"" +
                getProxyUrl(tasksUrl, "type=" + type +
                            "&pagenum=" + (pnum -1) + "&state=" + state) +
                "\">" + "Prev" + "</a></div>");
  }
%>

<hr>
<%
  out.println("<a href=\"" + getProxyUrl(jobUrl, "jobid=" + jobId) + "\">Go back to JobDetails</a><br>");
  out.println(ServletUtil.htmlFooter());
%>
