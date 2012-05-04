package org.apache.hadoop.mapred;

import javax.servlet.*;
import javax.servlet.http.*;
import javax.servlet.jsp.*;
import javax.servlet.*;
import javax.servlet.http.*;
import java.io.*;
import java.lang.String;
import java.text.*;
import java.util.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;
import java.text.SimpleDateFormat;

public final class taskstats_jsp extends org.apache.jasper.runtime.HttpJspBase
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
  String trackerName = 
           StringUtils.simpleHostname(tracker.getJobTrackerMachine());
  String jobid = request.getParameter("jobid");
  String tipid = request.getParameter("tipid");
  String taskid = request.getParameter("taskid");
  JobID jobidObj = JobID.forName(jobid);
  TaskID tipidObj = TaskID.forName(tipid);
  TaskAttemptID taskidObj = TaskAttemptID.forName(taskid);
  
  JobInProgress job = (JobInProgress) tracker.getJob(jobidObj);
  
  Format decimal = new DecimalFormat();
  Counters counters;
  if (taskid == null) {
    counters = tracker.getTipCounters(tipidObj);
    taskid = tipid; // for page title etc
  }
  else {
    TaskStatus taskStatus = tracker.getTaskStatus(taskidObj);
    counters = taskStatus.getCounters();
  }

      out.write("\n\n<html>\n  <head>\n    <title>Counters for ");
      out.print(taskid);
      out.write("</title>\n  </head>\n<body>\n<h1>Counters for ");
      out.print(taskid);
      out.write("</h1>\n\n<hr>\n\n");

  if ( counters == null ) {

      out.write("\n    <h3>No counter information found for this task</h3>\n");

  } else {    

      out.write("\n    <table>\n");

      for (String groupName : counters.getGroupNames()) {
        Counters.Group group = counters.getGroup(groupName);
        String displayGroupName = group.getDisplayName();

      out.write("\n        <tr>\n          <td colspan=\"3\"><br/><b>");
      out.print(displayGroupName);
      out.write("</b></td>\n        </tr>\n");

        for (Counters.Counter counter : group) {
          String displayCounterName = counter.getDisplayName();
          long value = counter.getCounter();

      out.write("\n          <tr>\n            <td width=\"50\"></td>\n            <td>");
      out.print(displayCounterName);
      out.write("</td>\n            <td align=\"right\">");
      out.print(decimal.format(value));
      out.write("</td>\n          </tr>\n");

        }
      }

      out.write("\n    </table>\n");

  }

      out.write("\n\n<hr>\n<a href=\"jobdetails.jsp?jobid=");
      out.print(jobid);
      out.write("\">Go back to the job</a><br>\n<a href=\"jobtracker.jsp\">Go back to JobTracker</a><br>\n");

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
