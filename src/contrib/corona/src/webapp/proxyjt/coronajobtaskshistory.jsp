<%@ page
  contentType="text/html; charset=UTF-8"
  import="javax.servlet.http.*"
  import="java.io.*"
  import="java.util.*"
  import="org.apache.hadoop.mapred.*"
  import="org.apache.hadoop.fs.*"
  import="org.apache.hadoop.util.*"
  import="java.text.SimpleDateFormat"
  import="org.apache.hadoop.mapred.JobHistory.*"
  import="org.apache.hadoop.conf.Configuration"
%>

<%!
  private static SimpleDateFormat dateFormat =
                                    new SimpleDateFormat("yyyy/MM/dd HH:mm:ss") ;
%>

<%
  String jobid = request.getParameter("jobid");
  String logFile = request.getParameter("logFile");
  String encodedLogFileName = JobHistory.JobInfo.encodeJobHistoryFilePath(logFile);
  String taskStatus = request.getParameter("status");
  String taskType = request.getParameter("taskType");

  Path jobFile = new Path(encodedLogFileName);
  FileSystem fs = jobFile.getFileSystem((Configuration) application.getAttribute("conf"));

  JobInfo job = JSPUtil.getJobInfo(request, fs);
  Map<String, JobHistory.Task> tasks = job.getAllTasks();
%>
<html>
<head>
<link rel="stylesheet" type="text/css" href="/static/hadoop.css">
<link rel="stylesheet" type="text/css" href="/static/tablesorter/style.css">
<script type="text/javascript" src="/static/jquery-1.7.1.min.js"></script>
<script type="text/javascript" src="/static/tablesorter/jquery.tablesorter.js"></script>
<script type="text/javascript" src="/static/tablesorter/jobtablesorter.js"></script>
</head>
<body>
<h2><%=taskStatus%> <%=taskType %> task list for <a href="coronajobdetailshistory.jsp?jobid=<%=jobid%>&&logFile=<%=encodedLogFileName%>"><%=jobid %> </a></h2>
<center>
<table border="2" cellpadding="5" cellspacing="2" class="tablesorter">
<thead>
<tr><th>Task Id</th><th>Task Attempts</th><th>Start Time</th>
    <th>Finish Time<br/></th><th>Error</th></tr>
</thead>
<tbody>
<%
  for (JobHistory.Task task : tasks.values()) {
    if (taskType.equals(task.get(Keys.TASK_TYPE))){
      Map <String, TaskAttempt> taskAttempts = task.getTaskAttempts();
      for (JobHistory.TaskAttempt taskAttempt : taskAttempts.values()) {
        if (taskStatus.equals(taskAttempt.get(Keys.TASK_STATUS)) ||
          taskStatus.equals("all")){
          printTask(jobid, encodedLogFileName, taskAttempt, taskAttempts.size(),
                    out);
        }
      }
    }
  }
%>
</tbody>
</table>
<%!
  private void printTask(String jobid, String logFile,
      JobHistory.TaskAttempt attempt, int taskAttempts, JspWriter out)
      throws IOException{
    out.print("<tr>");
    out.print("<td>" + "<a href=\"coronataskdetailshistory.jsp?jobid=" + jobid +
          "&logFile="+ logFile +"&taskid="+attempt.get(Keys.TASKID)+"\">" +
          attempt.get(Keys.TASKID) + "</a></td>");
    out.print("<td>" + taskAttempts + "</td>");
    out.print("<td>" + StringUtils.getFormattedTimeWithDiff(dateFormat,
          attempt.getLong(Keys.START_TIME), 0 ) + "</td>");
    out.print("<td>" + StringUtils.getFormattedTimeWithDiff(dateFormat,
          attempt.getLong(Keys.FINISH_TIME),
          attempt.getLong(Keys.START_TIME) ) + "</td>");
    out.print("<td>" + attempt.get(Keys.ERROR) + "</td>");
    out.print("</tr>");
  }
%>
</center>
</body>
</html>
