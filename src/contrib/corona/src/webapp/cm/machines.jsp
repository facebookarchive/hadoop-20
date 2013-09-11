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
%>

<%
  ClusterManager cm = (ClusterManager) application.getAttribute("cm");
  NodeManager nm = cm.getNodeManager();
  String cmHostName = StringUtils.simpleHostname(cm.getHostName());
  String type = request.getParameter("type");
  String resourceType = request.getParameter("resourceType");
%>

<%!
  public void generateHostTable(JspWriter out, NodeManager nm, String type, String resourceType)
        throws IOException {
    out.print("<center>\n");
    Collection<String> nodes = null;
    if ("alive".equals(type)) {
      List<String> aliveNodes = nm.getAliveNodes();
      Collections.sort(aliveNodes);
      nodes = aliveNodes;
      out.println("<h2>Alive Nodes</h2>");
    } else if ("blacklisted".equals(type)) {
      List<String> blacklistedNodes = nm.getFaultManager().getBlacklistedNodes();
      Collections.sort(blacklistedNodes);
      nodes = blacklistedNodes;
      out.println("<h2>Blacklisted Nodes</h2>");
    } else if ("excluded".equals(type)) {
      // Sorting using TreeSet.
      Set<String> excludedNodes = new TreeSet<String>(nm.getExcludedNodes());
      nodes = excludedNodes;
      out.println("<h2>Excluded Nodes</h2>");
    } else if ("all".equals(type)) {
      Set<String> allNodes = new TreeSet<String>(nm.getAllNodes());
      nodes = allNodes;
      out.println("<h2>All Nodes</h2>");
    } else if ("free".equals(type)) {
      List<String> freeNodes = new ArrayList<String>();
      try {
        freeNodes = nm.getFreeNodesForType(ResourceType.valueOf(resourceType));
      } catch (IllegalArgumentException e) {
        throw new IllegalArgumentException(
            "Cannot correctly parse resource type " +
            resourceType + ", must be one of " +
            Arrays.toString(ResourceType.values()));
      }
      Collections.sort(freeNodes);
      nodes = freeNodes;
      out.println("<h2>Free " + resourceType + " Nodes</h2>");
    } else {
      return;
    }
    out.print("<table border=\"2\" cellpadding=\"5\" cellspacing=\"2\">\n");
    out.print("<tr>");
    out.print("<td><b>Host Name</b></td></tr>\n");
    for (String node: nodes) {
      out.print("<td>" + node + "</td></tr>\n");
    }
    out.print("</table>\n");
    out.print("</center>\n");
  }
%>

<html>
<head>
<title><%= cmHostName %> Corona Machine List </title>
<link rel="stylesheet" type="text/css" href="/static/hadoop.css">
<link rel="stylesheet" type="text/css" href="/static/tablesorter/style.css">
<script type="text/javascript" src="/static/jquery-1.7.1.min.js"></script>
<script type="text/javascript" src="/static/tablesorter/jquery.tablesorter.js"></script>
<script type="text/javascript" src="/static/tablesorter/jobtablesorter.js"></script>
</head>
<body>

<%
  if ("alive".equals(type) ||
      "blacklisted".equals(type) ||
      "excluded".equals(type) ||
      "all".equals(type) ||
      "free".equals(type)) {
    generateHostTable(out, nm, type, resourceType);
  } else {
    out.println("Unknown type " + type);
  }
%>

<%
out.println(ServletUtil.htmlFooter());
%>

