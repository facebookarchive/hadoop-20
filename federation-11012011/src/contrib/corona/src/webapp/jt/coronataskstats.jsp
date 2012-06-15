<%@ page
  contentType="text/html; charset=UTF-8"
  import="javax.servlet.*"
  import="javax.servlet.http.*"
  import="java.io.*"
  import="java.lang.String"
  import="java.text.*"
  import="java.util.*"
  import="org.apache.hadoop.mapred.*"
  import="org.apache.hadoop.util.*"
  import="java.text.SimpleDateFormat"  
%>
<%! 
    private static String jobUrl = null;
    private static boolean hasProxy = false;

    private String getProxyUrl(String proxyPath, String params) {
      return proxyPath + (hasProxy ? "&" : "?") + params;
    }
%>

<%
  CoronaJobTracker tracker = (CoronaJobTracker) application.getAttribute("job.tracker");

  if(jobUrl == null) {
    jobUrl = tracker.getProxyUrl("coronajobdetails.jsp");
    hasProxy = (jobUrl.indexOf('?') != -1);
  }

  CoronaJobInProgress job = (CoronaJobInProgress) tracker.getJob();
  JobID jobId = (job != null) ? job.getStatus().getJobID() : null;

  String tipid = request.getParameter("tipid");
  TaskID tipidObj = TaskID.forName(tipid);

  String taskid = request.getParameter("taskid");
  TaskAttemptID taskidObj = TaskAttemptID.forName(taskid);

  Format decimal = new DecimalFormat();
  TaskInProgress tip = tracker.getTip(tipidObj);

  Counters counters;
  if (taskid == null) {
    counters = tip.getCounters();
    taskid = tipid; // for page title etc
  } else {
    TaskStatus taskStatus = tip.getTaskStatus(taskidObj);
    counters = taskStatus.getCounters();
  }
%>

<html>
  <head>
    <title>Counters for <%=taskid%></title>
  </head>
<body>
<h1>Counters for <%=taskid%></h1>

<hr>

<%
  if ( counters == null ) {
%>
    <h3>No counter information found for this task</h3>
<%
  } else {    
%>
    <table>
<%
      for (String groupName : counters.getGroupNames()) {
        Counters.Group group = counters.getGroup(groupName);
        String displayGroupName = group.getDisplayName();
%>
        <tr>
          <td colspan="3"><br/><b><%=displayGroupName%></b></td>
        </tr>
<%
        for (Counters.Counter counter : group) {
          String displayCounterName = counter.getDisplayName();
          long value = counter.getCounter();
%>
          <tr>
            <td width="50"></td>
            <td><%=displayCounterName%></td>
            <td align="right"><%=decimal.format(value)%></td>
          </tr>
<%
        }
      }
%>
    </table>
<%
  }
%>

<hr>
<%
  out.println("<a href=\"" + getProxyUrl(jobUrl, "jobid=" + jobId) + "\">Go back to the job</a><br>");
  out.println(ServletUtil.htmlFooter());
%>
