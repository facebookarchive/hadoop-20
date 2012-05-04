package org.apache.hadoop.mapred;

import javax.servlet.*;
import javax.servlet.http.*;
import javax.servlet.jsp.*;
import javax.servlet.*;
import javax.servlet.http.*;
import java.io.*;
import java.util.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public final class jobblacklistedtrackers_jsp extends org.apache.jasper.runtime.HttpJspBase
    implements org.apache.jasper.runtime.JspSourceDependent {

       
  private void printBlackListedTrackers(JspWriter out, 
                             JobInProgress job) throws IOException {
    Map<String, List<String>> trackerErrors = job.getTaskTrackerErrors();
    out.print("<table border=2 cellpadding=\"5\" cellspacing=\"2\">");
    out.print("<tr><th>TaskTracker</th><th>No. of Failures</th></tr>\n");
    int maxErrorsPerTracker = job.getJobConf().getMaxTaskFailuresPerTracker();
    for (Map.Entry<String,List<String>> e : trackerErrors.entrySet()) {
      if (e.getValue().size() >= maxErrorsPerTracker) {
        out.print("<tr><td>" + e.getKey() + "</td><td>" + e.getValue().size() + 
            "</td></tr>\n");
      }
    }
    out.print("</table>\n");
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

  JobTracker tracker = (JobTracker) application.getAttribute("job.tracker");
  String trackerName = 
           StringUtils.simpleHostname(tracker.getJobTrackerMachine());

      out.write('\n');
      out.write('\n');
      out.write('\n');

    String jobId = request.getParameter("jobid");
    if (jobId == null) {
  	  out.println("<h2>Missing 'jobid' for fetching black-listed tasktrackers!</h2>");
  	  return;
    }
    
    JobInProgress job = (JobInProgress) tracker.getJob(JobID.forName(jobId));
    if (job == null) {
      out.print("<b>Job " + jobId + " not found.</b><br>\n");
      return;
    }

      out.write("\n\n<html>\n<title>Hadoop ");
      out.print(jobId);
      out.write("'s black-listed tasktrackers</title>\n<body>\n<h1>Hadoop <a href=\"jobdetails.jsp?jobid=");
      out.print(jobId);
      out.write('"');
      out.write('>');
      out.print(jobId);
      out.write("</a> - \nBlack-listed task-trackers</h1>\n\n");
 
    printBlackListedTrackers(out, job); 

      out.write("\n\n<hr>\n<a href=\"jobdetails.jsp?jobid=");
      out.print(jobId);
      out.write("\">Go back to ");
      out.print(jobId);
      out.write("</a><br>\n");

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
