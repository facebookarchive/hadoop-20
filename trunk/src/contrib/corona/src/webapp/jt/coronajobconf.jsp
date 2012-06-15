<%@ page
  contentType="text/html; charset=UTF-8"
  import="javax.servlet.*"
  import="javax.servlet.http.*"
  import="java.io.*"
  import="java.net.URL"
  import="org.apache.hadoop.mapred.*"
  import="org.apache.hadoop.util.*"
  import="org.apache.hadoop.conf.Configuration"
  import="org.apache.hadoop.io.OutputBuffer"
  import="org.apache.hadoop.io.IOUtils"
%>


<%
  CoronaJobTracker tracker = (CoronaJobTracker) application.getAttribute("job.tracker");
  CoronaJobInProgress job = tracker.getJob();
  JobID jobId = (job != null) ? job.getStatus().getJobID() : null;
  String jobid = request.getParameter("jobid");
  if (job == null) {
    out.println("<h2>No running job!</h2>");
    return;
  }
  if (!jobId.toString().equals(jobid)) {
    out.println("<h2>Running job: " + jobId + " does not match requested job:" + jobid + "!</h2>");
    return;
  }
%>
  
<html>

<title>Job Configuration: JobId - <%= jobid %></title>

<body>
<h2>Job Configuration: JobId - <%= jobid %></h2><br>

<%
  OutputBuffer os = new OutputBuffer();
  try {
    job.getConf().writeXml(os);
    InputStream jobis = new ByteArrayInputStream(os.getData(), 0, os.getLength());
    InputStream xslis = job.getConf().getConfResourceAsInputStream("webapps/static/jobconf.xsl");

    if (xslis == null) {
      out.println("jobconf.xsl not found");
      return;
    }

    XMLUtils.transform(xslis, jobis, out);
  } catch (Exception e) {
    out.println("Unable to display configuration !");
    out.println(e);
    out.println(new String(os.getData()));
  }
%>

<br>
<%
out.println(ServletUtil.htmlFooter());
%>
