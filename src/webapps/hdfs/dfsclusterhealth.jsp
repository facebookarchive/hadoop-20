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
  String spaces(int num) {
    return org.apache.commons.lang.StringUtils.repeat("&nbsp;", num);
  }
  void counterReset () { colNum = 0; rowNum = 0 ; }

  long diskBytes = 1024 * 1024 * 1024;
  String diskByteStr = "GB";

  String NodeHeaderStr(String name) {
      String ret = "class=header";
      return ret;
  }

  public String format(String statName, Object stat) {
    if (statName.equals("Configured Capacity") ||
        statName.equals("DFS Used") ||
        statName.equals("Non DFS Used") ||
        statName.equals("DFS Remaining")) {
      return StringUtils.byteDesc((Long)stat);
    } else if (statName.equals("DFS Used%") ||
        statName.equals("DFS Remaining%")) {
      return StringUtils.limitDecimalTo2((Float)stat) + "%";
    } else {
      return stat.toString();
    }
  }

  public void generateStatsData(JspWriter out,
                                int statNum,
                                String statName,
                                Object totalStat,
                                List<InetSocketAddress> nnAddrs,
                                List<NamenodeStatus> nnList,
                                Map<Integer, Integer> nnMap)
                                throws IOException {
    out.print(rowTxt() + "<td id=\"col1\" class=\"metric\" align=\"center\">"
              + "<a title=\"" + statName + "\"> "
              + spaces(4) + "<b>" + statName + "</b>" + spaces(4) + " </a>");
    out.print("<td class=\"overallstatus\"> "
              + format(statName, totalStat));
    for (int i = 0; i < nnAddrs.size(); i++) {
      out.print("<td class=\"namenodestatus\" align=\"center\">");
      Integer index = nnMap.get(i);
      if (index == null) {
        out.print("-");
        continue;
      }
      Object[] stats = nnList.get(index).getStats();
      out.print(format(statName, stats[statNum]));
    }
    out.print("\n");
  }

  public void generateNameNodeReport(JspWriter out,
                                     ClusterStatus cInfo,
                                     HttpServletRequest request)
                                     throws IOException {
    List<NamenodeStatus> nnList = cInfo.nnList;
    List<InetSocketAddress> nnAddrs = cInfo.nnAddrs;
    if (nnAddrs != null) {
      List<String> nnAddrStrs = new ArrayList<String>();
      //Generate namenode header
      out.print("<table border=1 cellspacing=0>"
                + "<tr class=\"headRow\"> "
                + "<th " + NodeHeaderStr("")
                + "> <th " + NodeHeaderStr("Total")
                + "> Overall");
      for (InetSocketAddress isa : nnAddrs) {
        out.print(" <th " + NodeHeaderStr(isa.toString())
                + "> " + isa.toString());
        nnAddrStrs.add(isa.toString());
      }
      Map<Integer, Integer> nnMaps = new TreeMap<Integer, Integer>();
      for (int i = 0 ; i< nnList.size(); i++) {
        int index = nnAddrStrs.indexOf(nnList.get(i).address);
        if (index != -1) {
          nnMaps.put(index, i);
        }
      }

      String[] statNames = cInfo.getStatsNames();
      Object[] totalStats = cInfo.getStats();

      for (int i = 0; i < statNames.length; i++) {
        generateStatsData(out, i, statNames[i], totalStats[i], nnAddrs, nnList, nnMaps);
      }
      out.print("</table>\n");
      out.print("<hr>");
      out.print("<h3> DataNode Health: </h3>");
      for (int i = 0; i < nnList.size(); i++) {
        generateDataNodeHealthReport(out, nnList.get(i), i);
      }
    } else {
      out.print("There are no namenodes in the cluster");
    }
  }

  public void generateDataNodeHealthReport(JspWriter out,
                                           NamenodeStatus nn,
                                           int nnIndex)
                                           throws IOException {
    String url = "http://" + nn.httpAddress + "/";
    String name = nn.address;
    out.print("<br> <a name=\"NameNodes\" title=\""
            + name + "\" href=\""
            + url + "\">" + " NameNode " + nnIndex + " : " + name + "</a>\n");
    out.print("<div id=\"dfstable\"> <table border=1>\n" +
      rowTxt() + colTxt() +
      "<a href=\"" + url + "dfsnodelist.jsp?whatNodes=LIVE\">Live Nodes</a> " +
      colTxt() + "<b>" + nn.liveDatanodeCount + "</b>" +
      rowTxt() + colTxt() + spaces(4) +
      "<a href=\"" + url + "dfsnodelist.jsp?whatNodes=LIVE&status=NORMAL\">" +
      "In Service</a> " +
      colTxt() + colTxt() + (nn.liveDatanodeCount - nn.liveExcludeCount) +
      rowTxt() + colTxt() + spaces(4) +
      "<a href=\"" + url + "dfsnodelist.jsp?whatNodes=LIVE&status=EXCLUDED\">" +
      "Excluded</a> " +
      colTxt() + colTxt() + nn.liveExcludeCount +
      rowTxt() + colTxt() + spaces(8) +
      "<a href=\"" + url + "dfsnodelist.jsp?whatNodes=LIVE&status=DECOMMISSIONED\">" +
      "Decommission: Completed</a> " +
      colTxt() + colTxt() + colTxt() + nn.liveDecomCount +
      rowTxt() + colTxt() + spaces(8) +
      "<a href=\"" + url + "dfsnodelist.jsp?whatNodes=DECOMMISSIONING\">" +
      "Decommission: In Progress</a> " +
      colTxt() + colTxt() + colTxt() + (nn.liveExcludeCount - nn.liveDecomCount) +
      rowTxt() + colTxt() +
      "<a href=\"" + url + "dfsnodelist.jsp?whatNodes=DEAD\">Dead Nodes</a> " +
      colTxt() + "<b>" + nn.deadDatanodeCount + "</b>" +
      rowTxt() + colTxt() + spaces(4) +
      "<a href=\"" + url + "dfsnodelist.jsp?whatNodes=DEAD&status=EXCLUDED\">" +
      "Excluded</a> " +
      colTxt() + colTxt() + nn.deadExcludeCount +
      rowTxt() + colTxt() + spaces(8) +
      "<a href=\"" + url + "dfsnodelist.jsp?whatNodes=DEAD&status=DECOMMISSIONED\">" +
      "Decommission: Completed</a> " +
      colTxt() + colTxt() + colTxt() + nn.deadDecomCount +
      rowTxt() + colTxt() + spaces(8) +
      "<a href=\"" + url + "dfsnodelist.jsp?whatNodes=DEAD&status=INDECOMMISSIONED\">" +
      "Decommission: Not Completed</a> " +
      colTxt() + colTxt() + colTxt() +
      (nn.deadExcludeCount - nn.deadDecomCount) +
      rowTxt() + colTxt() + spaces(4) +
      "<a class=\"warning\" " +
      "href=\"dfsnodelist.jsp?whatNodes=DEAD&status=ABNORMAL\">" +
      "Not Excluded</a> " +
      colTxt() + colTxt() +
      (nn.deadDatanodeCount - nn.deadExcludeCount) +
      "</table></div><br>\n" );
  }

  void generateNameNodeData(JspWriter out, NamenodeStatus nn) throws IOException {
    String url = nn.httpAddress;
    String name = nn.address;
    out.print(rowTxt() + "<td class=\"name\"><a title=\"" +
        name + "\" href=\"" + "http://" + url + "\">" + name + "</a>");
    out.print("<td class=\"namespaceused\"> "
        + StringUtils.byteDesc(nn.nsUsed)
        + "<td class=\"filesanddirectories\">"
        + nn.filesAndDirectories
        + "<td class=\"blocks\">"
        + nn.blocksCount
        + "<td class=\"missingblocks\">"
        + nn.missingBlocksCount
        + "\n");
  }

%>
<%
  ClusterJspHelper clusterhealthjsp = new ClusterJspHelper();
  ClusterStatus cInfo = clusterhealthjsp.generateClusterHealthReport();
%>
<%@page import="java.net.InetSocketAddress"%><html>
<link rel="stylesheet" type="text/css" href="/static/hadoop.css">
<title>Hadoop Cluster </title>
<body>
<h1>Cluster Summary</h1>
<%
  generateNameNodeReport(out, cInfo, request);
%>
<%
  out.println(ServletUtil.htmlFooter());
%>
