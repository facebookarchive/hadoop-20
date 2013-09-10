<%@ page
  contentType="text/html; charset=UTF-8"
  import="javax.servlet.*"
  import="javax.servlet.http.*"
  import="java.io.*"
  import="java.io.InputStreamReader"
  import="java.util.*"
  import="org.apache.hadoop.raid.*"
  import="org.apache.hadoop.util.*"
  import="org.apache.hadoop.hdfs.*"
  import="org.apache.hadoop.hdfs.DistributedFileSystem.*"
%>
<%!
  String error = null;
  final static int FILE_DISPLAY_LIMIT = 500;
  public BufferedReader runFsck(RaidNode raidNode, 
      String dir, boolean listRecoverableFiles) throws Exception {
    try {
      ByteArrayOutputStream bout = new ByteArrayOutputStream();
      PrintStream ps = new PrintStream(bout, true);
      RaidShell shell = new RaidShell(raidNode.getConf(), ps);
      String[] arguments;
      if (listRecoverableFiles) {
        arguments = new String[] {"-fsck", dir, "-listrecoverablefiles"};
      } else {
        arguments = new String[] {"-fsck", dir};
      }
      int res = ToolRunner.run(shell, arguments);
      ByteArrayInputStream bin = new ByteArrayInputStream(bout.toByteArray());
      shell.close();
      return new BufferedReader(new InputStreamReader(bin));
    } catch (Exception e) {
      error = e.getMessage();
      return null;
    }
  }
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
out.print("<h2>Raid Corrupt Files</h2>");
%>

<%
  String dir = request.getParameter("path");
  if (dir == null || dir.length() == 0) {
    dir = "/";
  }
  String recoverableStr = request.getParameter("recoverable");
  int recoverable = 0;
  try {
    if (recoverableStr != null && recoverableStr.length() > 0) {
      recoverable = Integer.valueOf(recoverableStr);
    } 
  } catch (NumberFormatException e) {
    error = e.getMessage();
  }
  BufferedReader reader = runFsck(raidNode, dir, recoverable != 0);
  if (error != null) {
%>
    <%=error%> <br>
<%
  } else {
    String file = null;
    int total = 0;
    while (reader != null && (file = reader.readLine()) != null 
        && total < FILE_DISPLAY_LIMIT) {
      total++;
      out.println(file + "<br>");
    }
%>
    <p>
      <b>Total:</b> At least <%=total%> corrupt file(s).
    </p>
<%
  }
%>
<%
out.println(ServletUtil.htmlFooter());
%>
