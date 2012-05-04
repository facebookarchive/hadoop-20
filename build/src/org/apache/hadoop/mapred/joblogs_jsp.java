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

public final class joblogs_jsp extends org.apache.jasper.runtime.HttpJspBase
    implements org.apache.jasper.runtime.JspSourceDependent {


  public void generateLogsUrls(JspWriter out,
                               String jobid, JobTracker tracker,
                               JobInProgress job, String type)
                               throws IOException {
    TaskReport[] reports = null;
    JobID jobIdObj = JobID.forName(jobid);
    if ("map".equals(type)){
       reports = (job != null) ? tracker.getMapTaskReports(jobIdObj) : null;
      }
    else{
      reports = (job != null) ? tracker.getReduceTaskReports(jobIdObj) : null;
    }
    if (reports == null) {
      out.print("<b>Job " + jobid + " no reports found.</b><br>\n");
      return;
    }

    for (int i = 0; i < reports.length; i++) {

      // TaskID tipid = report[i].getTaskID(); TOBEUSED for 0.19
      String tipid = reports[i].getTaskId();
      TaskID tipidObj = TaskID.forName(tipid);

      TaskStatus[] ts = tracker.getTaskStatuses(tipidObj);
      for (int j = 0; j < ts.length; j++) {
        TaskStatus status = ts[j];
        String taskTrackerName = status.getTaskTracker();
        TaskTrackerStatus taskTracker = tracker.getTaskTrackerStatus(taskTrackerName);
        if (taskTracker == null) {
          continue;
        }

        String taskAttemptTracker = "http://" + taskTracker.getHost() + ":" +
                                    taskTracker.getHttpPort();
        String taskLogUrl = taskAttemptTracker + "/tasklog?taskid="
                            + status.getTaskID();
        // TOBEUSED for 0.l9
        //String taskLogUrl = TaskLogServlet.getTaskLogUrl(taskTracker.getHost(),
        //                                   String.valueOf(taskTracker.getHttpPort()),
        //                                   status.getTaskID().toString());

        String entireLogUrl = taskLogUrl + "&all=true";
        out.print(entireLogUrl + "\n");
      }
    }
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
  String trackerName =
           StringUtils.simpleHostname(tracker.getJobTrackerMachine());
  String jobid = request.getParameter("jobid");
  JobID jobIdObj = JobID.forName(jobid);
  JobInProgress job = null;
  if (jobid != null) {
    job = (JobInProgress) tracker.getJob(jobIdObj);
  }

      out.write('\n');
      out.write('\n');
      out.write('\n');
generateLogsUrls(out, jobid, tracker, job, "map");
      out.write('\n');
generateLogsUrls(out, jobid, tracker, job, "reduce");
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
