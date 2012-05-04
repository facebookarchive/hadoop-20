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
import org.apache.hadoop.fs.FsShell;

public final class locality_jsp extends org.apache.jasper.runtime.HttpJspBase
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

      out.write('\n');

  JobTracker tracker = (JobTracker) application.getAttribute("job.tracker");
  ClusterStatus status = tracker.getClusterStatus();
  String trackerName =
           StringUtils.simpleHostname(tracker.getJobTrackerMachine());

      out.write("\n<html>\n<head>\n<title>");
      out.print( trackerName );
      out.write(" Hadoop Locality Statistics</title>\n<link rel=\"stylesheet\" type=\"text/css\" href=\"/static/hadoop.css\">\n</head>\n<body>\n<h1>");
      out.print( trackerName );
      out.write(" Hadoop Locality Statistics</h1>\n\n<b>State:</b> ");
      out.print( status.getJobTrackerState() );
      out.write("<br>\n<b>Started:</b> ");
      out.print( new Date(tracker.getStartTime()));
      out.write("<br>\n<b>Version:</b> ");
      out.print( VersionInfo.getVersion());
      out.write(",\n                r");
      out.print( VersionInfo.getRevision());
      out.write("<br>\n<b>Compiled:</b> ");
      out.print( VersionInfo.getDate());
      out.write(" by\n                 ");
      out.print( VersionInfo.getUser());
      out.write("<br>\n<b>Identifier:</b> ");
      out.print( tracker.getTrackerIdentifier());
      out.write("<br>\n\n<hr>\n\n");

  Collection<JobInProgress> jobs = new ArrayList<JobInProgress>();
  jobs.addAll(tracker.completedJobs());
  jobs.addAll(tracker.runningJobs());
  jobs.addAll(tracker.failedJobs());
  int dataLocalMaps = 0;
  int rackLocalMaps = 0;
  int totalMaps = 0;
  int totalReduces = 0;
  for (JobInProgress job: jobs) {
    Counters counters = job.getCounters();
    dataLocalMaps += counters.getCounter(JobInProgress.Counter.DATA_LOCAL_MAPS);
    rackLocalMaps += counters.getCounter(JobInProgress.Counter.RACK_LOCAL_MAPS);
    totalMaps += counters.getCounter(JobInProgress.Counter.TOTAL_LAUNCHED_MAPS);
    totalReduces += counters.getCounter(JobInProgress.Counter.TOTAL_LAUNCHED_REDUCES);
  }
  int dataLocalMapPct = totalMaps == 0 ? 0 : (100 * dataLocalMaps) / totalMaps;
  int rackLocalMapPct = totalMaps == 0 ? 0 : (100 * rackLocalMaps) / totalMaps;
  int dataRackLocalMapPct = totalMaps == 0 ? 0 : (100 * (dataLocalMaps + rackLocalMaps)) / totalMaps;

      out.write("\n<p>\n<b>Data Local Maps:</b> ");
      out.print(dataLocalMaps);
      out.write(' ');
      out.write('(');
      out.print(dataLocalMapPct);
      out.write("%) <br>\n<b>Rack Local Maps:</b> ");
      out.print(rackLocalMaps);
      out.write(' ');
      out.write('(');
      out.print(rackLocalMapPct);
      out.write("%) <br>\n<b>Data or Rack Local:</b> ");
      out.print(dataLocalMaps + rackLocalMaps);
      out.write(' ');
      out.write('(');
      out.print(dataRackLocalMapPct);
      out.write("%) <br>\n<b>Total Maps:</b> ");
      out.print(totalMaps);
      out.write(" <br>\n<b>Total Reduces:</b> ");
      out.print(totalReduces);
      out.write(" <br>\n</p>\n\n");

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
