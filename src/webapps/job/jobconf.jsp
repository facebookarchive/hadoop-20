<%@ page
  contentType="text/html; charset=UTF-8"
  import="javax.servlet.*"
  import="javax.servlet.http.*"
  import="java.io.*"
  import="java.net.URL"
  import="org.apache.hadoop.mapred.*"
  import="org.apache.hadoop.util.*"
%>


<%
  JobTracker tracker = (JobTracker) application.getAttribute("job.tracker");
  String jobId = request.getParameter("jobid");
  if (jobId == null) {
    out.println("<h2>Missing 'jobid' for fetching job configuration!</h2>");
 	return;
  }
%>
  
<html>

<title>Job Configuration: JobId - <%= jobId %></title>

<body>
<h2>Job Configuration: JobId - <%= jobId %></h2><br>

<%
  JobID jobIdObj = JobID.forName(jobId);
  JobInProgress job = (JobInProgress) tracker.getJob(jobIdObj);
  InputStream jobStream = null;
  JobConf jobConf = job.getJobConf();
  try {
    if (jobConf != null) {
      ByteArrayOutputStream bos = new ByteArrayOutputStream();
      jobConf.writeXml(bos);
      jobStream = new ByteArrayInputStream(bos.toByteArray());
    }
    else {
      String jobFilePath = JobTracker.getLocalJobFilePath(JobID.forName(jobId));
      jobStream = new FileInputStream(jobFilePath);
      jobConf = new JobConf(jobFilePath);
    } 
    XMLUtils.transform(
      jobConf.getConfResourceAsInputStream("webapps/static/jobconf.xsl"),
      jobStream, out);
  } catch (Exception e) {
      out.println("Failed to retreive job configuration for job '" + jobId + "!");
      out.println(e);
  } finally {
    if (jobStream != null) {
      try { 
        jobStream.close(); 
      } catch (IOException e) {}
    }
  }
%>

<br>
<%
out.println(ServletUtil.htmlFooter());
%>
