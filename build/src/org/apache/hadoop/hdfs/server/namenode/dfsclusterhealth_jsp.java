package org.apache.hadoop.hdfs.server.namenode;

import javax.servlet.*;
import javax.servlet.http.*;
import javax.servlet.jsp.*;
import javax.servlet.*;
import javax.servlet.http.*;
import java.io.*;
import java.util.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.hdfs.*;
import org.apache.hadoop.hdfs.server.common.*;
import org.apache.hadoop.hdfs.server.namenode.*;
import org.apache.hadoop.hdfs.server.namenode.ClusterJspHelper.*;
import org.apache.hadoop.util.*;
import java.text.DateFormat;
import java.lang.Math;
import java.net.URLEncoder;
import java.net.InetSocketAddress;

public final class dfsclusterhealth_jsp extends org.apache.jasper.runtime.HttpJspBase
    implements org.apache.jasper.runtime.JspSourceDependent {


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
      String statStr = format(statName, stats[statNum]);
      if (!statName.equals("Missing Blocks") || (Long)stats[statNum] == 0) {
        out.print(statStr);
      } else {
        String url = "http://" + nnList.get(index).httpAddress + "/";
        out.print("<a class=\"warning\" href=\"" + url + "corrupt_files.jsp\">"
          + statStr + "</a>");
      }
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
    out.print("<br> <b>" + nn.safeModeText + "</b>\n");
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
      "href=\"" + url + "dfsnodelist.jsp?whatNodes=DEAD&status=ABNORMAL\">" +
      "Not Excluded</a> " +
      colTxt() + colTxt() +
      (nn.deadDatanodeCount - nn.deadExcludeCount) +
      "</table></div><br>\n" );
  }

  private static final JspFactory _jspxFactory = JspFactory.getDefaultFactory();

  private static java.util.Vector _jspx_dependants;

  private org.apache.jasper.runtime.ResourceInjector _jspx_resourceInjector;

  public Object getDependants() {
    return _jspx_dependants;
  }

  public void _jspService(HttpServletRequest request, HttpServletResponse response)
        throws java.io.IOException, ServletException {

    PageContext pageContext = null;
    HttpSession session = null;
    ServletContext application = null;
    ServletConfig config = null;
    JspWriter out = null;
    Object page = this;
    JspWriter _jspx_out = null;
    PageContext _jspx_page_context = null;

    try {
      response.setContentType("text/html; charset=UTF-8");
      pageContext = _jspxFactory.getPageContext(this, request, response,
      			null, true, 8192, true);
      _jspx_page_context = pageContext;
      application = pageContext.getServletContext();
      config = pageContext.getServletConfig();
      session = pageContext.getSession();
      out = pageContext.getOut();
      _jspx_out = out;
      _jspx_resourceInjector = (org.apache.jasper.runtime.ResourceInjector) application.getAttribute("com.sun.appserv.jsp.resource.injector");

      out.write('\n');
      out.write('\n');

  NameNode nn = (NameNode)application.getAttribute("name.node");
  ClusterJspHelper clusterhealthjsp = new ClusterJspHelper(nn);
  ClusterStatus cInfo = clusterhealthjsp.generateClusterHealthReport();

      out.write("\n<html>\n<link rel=\"stylesheet\" type=\"text/css\" href=\"/static/hadoop.css\">\n<title>HDFS Cluster ");
      out.print( ClusterJspHelper.CLUSTER_NAME );
      out.write("</title>\n<body>\n<h1>Cluster ");
      out.print( ClusterJspHelper.CLUSTER_NAME );
      out.write(" Summary</h1>\n");

  generateNameNodeReport(out, cInfo, request);

      out.write('\n');

  out.println(ServletUtil.htmlFooter());

      out.write('\n');
    } catch (Throwable t) {
      if (!(t instanceof SkipPageException)){
        out = _jspx_out;
        if (out != null && out.getBufferSize() != 0)
          out.clearBuffer();
        if (_jspx_page_context != null) _jspx_page_context.handlePageException(t);
      }
    } finally {
      _jspxFactory.releasePageContext(_jspx_page_context);
    }
  }
}
