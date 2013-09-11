<%@ page
  contentType="text/html; charset=UTF-8"
  import="javax.servlet.*"
  import="javax.servlet.http.*"
  import="java.io.*"
  import="java.util.*"
  import="org.apache.hadoop.raid.*"
  import="org.apache.hadoop.raid.JobMonitor.*"
  import="org.apache.hadoop.raid.StatisticsCollector"
  import="org.apache.hadoop.util.*"
  import="org.apache.hadoop.hdfs.*"
  import="org.apache.hadoop.hdfs.DistributedFileSystem.*"
  import="java.lang.Integer"
  import="java.text.SimpleDateFormat"
%>
<%
  RaidNode raidNode = (RaidNode) application.getAttribute("raidnode");
  String name = raidNode.getHostName();
  name = name.substring(0, name.indexOf(".")).toUpperCase();
%>

<html>
  <head>
    <title><%=name %> Hadoop RaidNode Administration</title>
    <link rel="stylesheet" type="text/css" href="static/hadoop.css">
  </head>
<body>
<h1><%=name %> Hadoop RaidNode Administration</h1>
<b>Started:</b> <%= new Date(raidNode.getStartTime())%><br>
<b>Version:</b> <%= VersionInfo.getVersion()%>,
                r<%= VersionInfo.getRevision()%><br>
<b>Compiled:</b> <%= VersionInfo.getDate()%> by 
                 <%= VersionInfo.getUser()%><br>
<hr>
<% 
out.print("<h2>Raid Progress </h2>");
out.print(raidNode.raidJobsHtmlTable(STATUS.RAIDED));
out.print("<h2>Running Raid Jobs </h2>");
out.print(raidNode.raidJobsHtmlTable(STATUS.RUNNING));
out.print("<h2>Finished Raid Jobs </h2>");
out.print(raidNode.raidJobsHtmlTable(STATUS.FINISHED));
%>
<%
out.println(ServletUtil.htmlFooter());
%>
