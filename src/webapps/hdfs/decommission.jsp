<%@ page
  contentType="text/html; charset=UTF-8"
  import="javax.servlet.*"
  import="javax.servlet.http.*"
  import="java.io.*"
  import="java.util.*"
  import="org.apache.hadoop.fs.*"
  import="org.apache.hadoop.hdfs.*"
  import="org.apache.hadoop.hdfs.server.common.*"
  import="org.apache.hadoop.hdfs.server.namenode.*"
  import="org.apache.hadoop.hdfs.server.namenode.ClusterJspHelper.*"
  import="org.apache.hadoop.util.*"
  import="java.text.DateFormat"
  import="java.lang.Math"
  import="java.net.URLEncoder"
%>
<%!
  long total = 0L;
  long free = 0L;
  long nonDfsUsed = 0L;
  float dfsUsedPercent = 0.0f;
  float dfsRemainingPercent = 0.0f;
  int size = 0;

  int rowNum = 0;
  int colNum = 0;

  String rowTxt() { colNum = 0;
      return "<tr class=\"" + (((rowNum++)%2 == 0)? "rowNormal" : "rowAlt")
          + "\"> "; }
  String colTxt() { return "<td id=\"col" + ++colNum + "\"> "; }
  void counterReset () { colNum = 0; rowNum = 0 ; }

  long diskBytes = 1024 * 1024 * 1024;
  String diskByteStr = "GB";

  String NodeHeaderStr(String name) {
      String ret = "class=header";
      return ret;
  }

  public void generateDecommissioningReport(JspWriter out,
                                     DecommissionStatus dInfo,
                                     HttpServletRequest request)
      throws IOException {
    Map<String, Map<String, String>> statusMap = dInfo.statusMap;
    Set<String> dnSet = statusMap.keySet();
    if (dnSet.size() > 0) {
      out.print("<table border=1 cellspacing=0> <tr class=\"headRow\"> "
                + "<th " + NodeHeaderStr("datanode")
                + "> DataNode <th " + NodeHeaderStr("overallstatus")
                + "> " + ClusterJspHelper.OVERALL_STATUS);
      if (dInfo.nnAddrs != null) {
        for (InetSocketAddress isa : dInfo.nnAddrs) {
          out.print(" <th " + NodeHeaderStr(isa.toString())
                  + "> " + isa.toString());
        }
      }
      for (String dnhost : dnSet) {
        Map<String, String> nnStatus = statusMap.get(dnhost);
        if (nnStatus == null || nnStatus.isEmpty()) {
          continue;
        }
        String overallStatus = nnStatus.get(ClusterJspHelper.OVERALL_STATUS);
        if (overallStatus != null
           && (overallStatus.equals(DecommissionStates.DECOMMISSION_INPROGRESS.toString())
               || overallStatus.equals(DecommissionStates.DECOMMISSIONED.toString())
               || overallStatus.equals(DecommissionStates.PARTIALLY_DECOMMISSIONED
                  .toString())
               || overallStatus.equals(DecommissionStates.UNKNOWN.toString()))) {
          generateDecommissioningData(out, overallStatus, dnhost, nnStatus, dInfo.nnAddrs);
        }
      }
      out.print("</table>\n");
    }
  }

  public void generateDecommissioningData(JspWriter out, String overallStatus, String name,
        Map<String, String> nnStatus, List<InetSocketAddress> nnAddrs) throws IOException {
    if (!name.matches("\\d+\\.\\d+.\\d+\\.\\d+.*"))
      name = name.replaceAll("\\.[^.:]*", "");
    out.print(rowTxt() + "<td class=\"name\"><a title=\"" +
              name + "\">" + name + "</a>");
    out.print("<td class=\"overallstatus\"> "
              + overallStatus);
    for (InetSocketAddress isa : nnAddrs) {
      String status = nnStatus.get(isa.toString());
      out.print("<td class=\"namenodestatus\"> ");
      if (status == null) {
        out.print("");
      } else {
        out.print(status);
      }
    }
    out.print("\n");
  }

%>
<%
  NameNode nn = (NameNode)application.getAttribute("name.node");
  ClusterJspHelper clusterhealthjsp = new ClusterJspHelper(nn);
  DecommissionStatus dInfo = clusterhealthjsp.generateDecommissioningReport();
  dInfo.countDecommissionDatanodes();
%>
<%@page import="java.net.InetSocketAddress"%><html>
<link rel="stylesheet" type="text/css" href="static/hadoop.css">
<title>Hadoop Decommission Status</title>
<body>
<h1>Decommissioning Status</h1>

<div id="dfstable"> <table>
<tr> <td id="col1"><%= DecommissionStates.DECOMMISSIONED.toString() + ":"%><td><%= dInfo.decommissioned %>
<tr> <td id="col1"><%= DecommissionStates.DECOMMISSION_INPROGRESS.toString() + ":" %><td><%= dInfo.decommissioning %>
<tr> <td id="col1"><%= DecommissionStates.PARTIALLY_DECOMMISSIONED.toString() + ":" %><td><%= dInfo.partial %>
</table></div><br>
<%
    generateDecommissioningReport(out, dInfo, request);
%>
<%
  out.println(ServletUtil.htmlFooter());
%>
