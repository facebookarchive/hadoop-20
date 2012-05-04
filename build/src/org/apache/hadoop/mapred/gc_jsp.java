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

public final class gc_jsp extends org.apache.jasper.runtime.HttpJspBase
    implements org.apache.jasper.runtime.JspSourceDependent {


  public void dumpJobInformation(JobTracker tracker) throws IOException {
    List<JobInProgress> runningJobs = tracker.getRunningJobs();
    List<JobInProgress> completedJobs = tracker.getCompletedJobs();
    List<JobInProgress> failedJobs = tracker.getFailedJobs();
    if (completedJobs.size() > 0) {
      tracker.LOG.info("COMPLETED JOBS:---------");
      tracker.LOG.info(JSPUtil.generateJobTable("Completed", completedJobs, 0,
                       runningJobs.size()));
      tracker.LOG.info("<hr>");
    }
    if (failedJobs.size() > 0) {
      tracker.LOG.info("FAILED JOBS:");
      tracker.LOG.info(JSPUtil.generateJobTable("Failed", failedJobs, 0,
                       (runningJobs.size()+completedJobs.size())));
      tracker.LOG.info("<hr>");
    }
    tracker.LOG.info("RETIRED JOBS:");
    tracker.LOG.info(JSPUtil.generateRetiredJobTable(tracker,
                     (runningJobs.size()+completedJobs.size()+failedJobs.size())));
    tracker.LOG.info("--------------<hr>");
  }

  public boolean throttle(JspWriter out, ClusterStatus status,
                          JobTracker tracker, double threshold) throws IOException {
    if (status.getUsedMemory() > (threshold * status.getMaxMemory())/100) {
      tracker.LOG.info("Exceeding configured memory threshold of " +
                        threshold + " percent. " +
                       "Current usage is " + status.getUsedMemory() +
                       " from a configured maximum of " + status.getMaxMemory() + ". " +
                       " Retiring all possible jobs.");
      ReflectionUtils.printThreadInfo(new PrintWriter(System.out), "Throttling new jobs");
      tracker.retireCompletedJobs();
      dumpJobInformation(tracker);
      return true;
    }
    return false;
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

  JobTracker tracker = (JobTracker) application.getAttribute("job.tracker");
  ClusterStatus status = tracker.getClusterStatus();
  String trackerName =
           StringUtils.simpleHostname(tracker.getJobTrackerMachine());
  boolean doGc = false;
  double threshold = 100; // default percentage, no throttling

  // is the threshold specified by the client?
  String clientThrehold = request.getParameter("threshold");
  if (clientThrehold != null) {
    try {
      threshold = Integer.parseInt(clientThrehold);
    }
      catch (NumberFormatException ignored) {
    }
  }


      out.write('\n');
      out.write("\n\n<html>\n<head>\n<title>");
      out.print( trackerName );
      out.write(" Hadoop Map/Reduce Throttler</title>\n<link rel=\"stylesheet\" type=\"text/css\" href=\"/static/hadoop.css\">\n</head>\n<body>\n\n");

    doGc = throttle(out, status, tracker, threshold);

      out.write("\n\n<br>\n<dogc>");
      out.print( doGc ? "true" : "false" );
      out.write("</dogc>\n<usedmemory>");
      out.print( status.getUsedMemory() );
      out.write("</usedmemory>\n<maxmemory>");
      out.print( status.getMaxMemory() );
      out.write("</totalmemory>\n<br>\n\n");

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
