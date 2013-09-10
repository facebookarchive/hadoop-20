<%@ page
  import="org.apache.hadoop.hdfs.qjournal.server.JournalNodeJspHelper.QJMStatus"
  import="org.apache.hadoop.hdfs.qjournal.server.JournalNodeJspHelper.StatsDescriptor"
  import="org.apache.hadoop.hdfs.qjournal.server.*"
  import="org.apache.hadoop.util.*"
%>
<%
  JournalNode jn = (JournalNode)application.getAttribute(JournalNodeHttpServer.JN_ATTRIBUTE_KEY);
  JournalNodeJspHelper helper = new JournalNodeJspHelper(jn);
  QJMStatus status = helper.generateQJMStatusReport();
%>
<%@page import="java.net.InetSocketAddress"%><html>
<link rel="stylesheet" type="text/css" href="/static/hadoop.css">
<title>Quorum Journal Manager</title>
<body>
<h1>Quorum Journal Manager: <%= helper.getName()%> </h1>
<%
  out.print("<h3> Journal nodes: </h3>");
  out.print("<div id=\"dfstable\">");
  out.println(JournalNodeJspHelper.getNodeReport(status));
%>
<%
  out.print("<h3> Journals: </h3>");
  out.print("<div id=\"dfstable\">");
  out.println(JournalNodeJspHelper.getJournalReport(status));
%>
<%
  out.println(ServletUtil.htmlFooter());
%>
