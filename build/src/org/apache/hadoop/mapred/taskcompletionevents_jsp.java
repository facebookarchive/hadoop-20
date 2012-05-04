package org.apache.hadoop.mapred;

import javax.servlet.*;
import javax.servlet.http.*;
import javax.servlet.jsp.*;
import javax.servlet.*;
import javax.servlet.http.*;
import java.io.*;
import java.util.*;
import java.text.DecimalFormat;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public final class taskcompletionevents_jsp extends org.apache.jasper.runtime.HttpJspBase
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

      out.write('\r');
      out.write('\n');

  TaskTracker tracker = (TaskTracker) application.getAttribute("task.tracker");
  String trackerName = tracker.getName();

      out.write("\r\n\r\n<html>\r\n\r\n<title>");
      out.print( trackerName );
      out.write(" Task Tracker Completion Events</title>\r\n\r\n<body>\r\n<h1>");
      out.print( trackerName );
      out.write(" Task Tracker Completion Events</h1>\r\n<img src=\"/static/hadoop-logo.jpg\"/><br>\r\n<b>Version:</b> ");
      out.print( VersionInfo.getVersion());
      out.write(",\r\n                r");
      out.print( VersionInfo.getRevision());
      out.write("<br>\r\n<b>Compiled:</b> ");
      out.print( VersionInfo.getDate());
      out.write(" by \r\n                 ");
      out.print( VersionInfo.getUser());
      out.write("<br>\r\nGo back to <a href=\"tasktracker.jsp\">Task Tracker</a><br>\r\n\r\n\r\n<h2>Jobs with Reduce Tasks in Shuffling Phase</h2>\r\n<center>\r\n\r\n");

      List <TaskTracker.FetchStatus> fs = tracker.reducesInShuffle();
      for(TaskTracker.FetchStatus f: fs) {
          out.print("<br>" + "NextId: " + f.fromEventId.get() + "<br>");
          
          TaskCompletionEvent[] events = f.getMapEvents(0, 1000000);
          out.print("<table border=2 cellpadding=\"5\" cellspacing=\"2\">");
          out.print("<tr><td align=\"center\">ID</td><td>Event ID</td><td>Task ID</td><td>ID within Job</td>" +
                    "<td>Type</td><td>Status</td></tr>");

          for(int i=0; i<events.length; i++) {
            TaskCompletionEvent report = events[i];
            String url = report.getTaskTrackerHttp();
            out.print("<tr>");
            out.print("<td><a href=\"" + url + "\">" + i +"</a></td>");
            out.print("<td>" + report.getEventId() + "</td>");
            out.print("<td>" + report.getTaskId() + "</td>");
            out.print("<td>" + report.idWithinJob() + "</td>");
            out.print("<td>" + (report.isMapTask() ? "Map" : "Reduce") + "</td>");
            out.print("<td>" + report.getTaskStatus().name() + "</td>");
          }
          out.print("</table>");
      }      

      out.write("\r\n\r\n</center>\r\n\r\n\r\n<h2>Local Logs</h2>\r\n<a href=\"/logs/\">Log</a> directory\r\n\r\n<hr>\r\n<a href=\"http://lucene.apache.org/hadoop\">Hadoop</a>, 2007.<br>\r\n</body>\r\n</html>\r\n");
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
