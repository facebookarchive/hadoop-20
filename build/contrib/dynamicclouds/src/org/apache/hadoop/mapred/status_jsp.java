package org.apache.hadoop.mapred;

import javax.servlet.*;
import javax.servlet.http.*;
import javax.servlet.jsp.*;
import javax.servlet.*;
import javax.servlet.http.*;
import java.io.*;
import java.text.*;
import java.util.*;
import java.net.URL;
import java.text.DecimalFormat;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.conf.*;

public final class status_jsp extends org.apache.jasper.runtime.HttpJspBase
    implements org.apache.jasper.runtime.JspSourceDependent {

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
      response.setContentType("text/html");
      pageContext = _jspxFactory.getPageContext(this, request, response,
      			null, true, 8192, true);
      _jspx_page_context = pageContext;
      application = pageContext.getServletContext();
      config = pageContext.getServletConfig();
      session = pageContext.getSession();
      out = pageContext.getOut();
      _jspx_out = out;
      _jspx_resourceInjector = (org.apache.jasper.runtime.ResourceInjector) application.getAttribute("com.sun.appserv.jsp.resource.injector");

      out.write("\n    ");

      DynamicCloudsDaemon cb = 
            (DynamicCloudsDaemon)application.getAttribute("cluster.balancer");
    
      out.write("\n    \n<html>\n  <title> Cluster Balancer Daemon Status </title>\n  <head>\n  </head>\n  <body>\n    <script type=\"text/javascript\">\n      function $(id) {\n        return document.getElementById(id);\n      }\n\n      function switchDisplay (data_id) {\n        if ($(data_id).style.display == 'none') {\n          $(data_id).style.display = 'block';\n        } else {\n          $(data_id).style.display = 'none';\n        }\n      }\n    </script>\n    <h1>Clusters Statuses: </h1>\n    \n    <center>\n");

  int clusterNum = 1;
  for (Map.Entry<String, Cluster> clusterEntry : cb.getRegisteredClusters()) {
    Cluster cluster = clusterEntry.getValue();
    String name = clusterEntry.getKey();
    String host = cluster.getHostName();
    String version = cluster.getVersion();
    String http = cluster.getHttpAddress();

      out.write("\n    <h2>Cluster #");
      out.print(clusterNum++);
      out.write("</h2>\n    <ul style=\"list-style-type: none;\">\n      <li> <b> Name: ");
      out.print(name);
      out.write(" </b>\n      <li> <b> Job Tracker: <a href=\"");
      out.print(http);
      out.write('"');
      out.write('>');
      out.print(host);
      out.write("</a> </b>\n      <li> <b> Version: ");
      out.print(version);
      out.write("\n      <li> <b> Average Load: ");
      out.print(cluster.getAverageLoad());
      out.write("\n    </ul>\n\n    <h3> <a href=\"#\" onclick=\"return switchDisplay('tt_list_");
      out.print(clusterNum);
      out.write("')\"> Task Trackers </a></h3>\n    <div id='tt_list_");
      out.print(clusterNum);
      out.write("' style=\"display:none\">\n      <table border=\"1px\" width=\"80%\" >\n        <tr>\n          <th>TT Host Name</th>\n          <th>Active Status</th>\n          <th>Last Heartbeat</th>\n          <th>Map Slots</th>\n          <th>Reduce Slots</th>\n          <th>Total Maps</th>\n        </tr>\n");

      for (TaskTrackerLoadInfo ttli : cluster.getTrackers()) {
        

      out.write("\n<tr>\n  <td>");
      out.print(ttli.getHostName());
      out.write("</td>\n  <td>");
      out.print(ttli.isActive()?"Active":"Blacklisted");
      out.write("</td>\n  <td>");
      out.print((System.currentTimeMillis() - ttli.getLastSeen())/1000);
      out.write("</td>\n  <td>");
      out.print(ttli.getRunningMapTasks());
      out.write('/');
      out.print(ttli.getMaxMapTasks());
      out.write("</td>\n  <td>");
      out.print(ttli.getRunningReduceTasks());
      out.write('/');
      out.print(ttli.getMaxReduceTasks());
      out.write("</td>\n  <td>");
      out.print(ttli.getTotalMapTasks());
      out.write("</td>\n</tr>\n");

      }

      out.write("\n      </table>\n    </div>\n");

  }

      out.write("\n    </center>\n  </body>    \n</html>\n");
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
