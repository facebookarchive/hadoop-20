<%@ page
  contentType="text/html; charset=UTF-8"
  import="javax.servlet.*"
  import="javax.servlet.http.*"
  import="java.io.*"
  import="java.text.*"
  import="java.util.*"
  import="org.apache.hadoop.corona.*"
  import="org.apache.hadoop.util.ServletUtil"
  import="org.apache.hadoop.util.StringUtils"
  import="java.text.SimpleDateFormat"
%>

<%!
	private Session mySession = null;
%>

<%
  ClusterManager cm = (ClusterManager) application.getAttribute("cm");
  NodeManager nm = cm.getNodeManager();
  SessionManager sm = cm.getSessionManager();
  String cmHostName = StringUtils.simpleHostname(cm.getHostName());
  String id = request.getParameter("id");
  mySession = sm.getSession(id);
%>

<html>
<head>
<title><%= cmHostName %> Corona Cluster Manager Job Resources</title>
<link rel="stylesheet" type="text/css" href="/static/hadoop.css">
</head>
<body>

<h1><%= cmHostName %> Resources for session <%=id%></h1>
<b>Started:</b> <%= new Date(cm.getStartTime())%><br>
<hr>

<%
  DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
  if (mySession == null) {
    out.print("<b>Session not found.</b><br>\n");
    return;
  }

  List<GrantReport> grantReportList = null;
  synchronized (mySession) {
    grantReportList = mySession.getGrantReportList();
  }
  if (grantReportList.isEmpty()) {
    out.print("<b>No grant reports</b>");
  } else {
    out.print("<h2>Grants</h2>");
    out.print("<center>");
    out.print("<table border=2 cellpadding=\"5\" cellspacing=\"2\">");
    out.print("<tr><td align=\"center\">Grant Id</td><td>Address</td>" +
              "<td>Type</td><td>Granted Time</td></tr>");
    for (GrantReport report : grantReportList) {
      out.print("<tr><td>" + report.getGrantId() + "</td><td>" +
      	        report.getAddress() + "</td><td>" +
                report.getType() + "</td><td>" +
                dateFormat.format(report.getGrantedTime()) + "</td>");
    }
    out.print("</table>");
    out.print("</center>");
  }
%>

<%
  out.println("<a href=\"cm.jsp\"" +
              "\">Go back to the Cluster Manager</a><br>");
  out.println("</body></html>");
%>
