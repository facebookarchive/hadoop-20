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

public final class decommission_jsp extends org.apache.jasper.runtime.HttpJspBase
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
  DecommissionStatus dInfo = clusterhealthjsp.generateDecommissioningReport();
  dInfo.countDecommissionDatanodes();

      out.write("\n<html>\n<link rel=\"stylesheet\" type=\"text/css\" href=\"/static/hadoop.css\">\n<title>Hadoop Decommission Status</title>\n<body>\n<h1>Decommissioning Status</h1>\n\n<div id=\"dfstable\"> <table>\n<tr> <td id=\"col1\">");
      out.print( DecommissionStates.DECOMMISSIONED.toString() + ":");
      out.write("<td>");
      out.print( dInfo.decommissioned );
      out.write("\n<tr> <td id=\"col1\">");
      out.print( DecommissionStates.DECOMMISSION_INPROGRESS.toString() + ":" );
      out.write("<td>");
      out.print( dInfo.decommissioning );
      out.write("\n<tr> <td id=\"col1\">");
      out.print( DecommissionStates.PARTIALLY_DECOMMISSIONED.toString() + ":" );
      out.write("<td>");
      out.print( dInfo.partial );
      out.write("\n</table></div><br>\n");

    generateDecommissioningReport(out, dInfo, request);

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
