package org.apache.hadoop.mapred;

import javax.servlet.*;
import javax.servlet.http.*;
import javax.servlet.jsp.*;
import java.util.Date;
import java.util.List;
import org.apache.hadoop.mapreduce.server.jobtracker.JobTrackerJspHelper;
import org.apache.hadoop.util.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapred.JSPUtil;

public final class jobtracker_jspx extends org.apache.jasper.runtime.HttpJspBase
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
    ServletContext application = null;
    ServletConfig config = null;
    JspWriter out = null;
    Object page = this;
    JspWriter _jspx_out = null;
    PageContext _jspx_page_context = null;

    try {
      response.setContentType("text/xml; charset=UTF-8");
      pageContext = _jspxFactory.getPageContext(this, request, response,
      			null, false, 8192, true);
      _jspx_page_context = pageContext;
      application = pageContext.getServletContext();
      config = pageContext.getServletConfig();
      out = pageContext.getOut();
      _jspx_out = out;
      _jspx_resourceInjector = (org.apache.jasper.runtime.ResourceInjector) application.getAttribute("com.sun.appserv.jsp.resource.injector");

      out.write("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n");
      out.write("<cluster>");

    response.setHeader("Pragma", "no-cache");
    response.setHeader("Cache-Control", "no-store");
    response.setDateHeader("Expires", -1);
  

    JobTracker tracker = (JobTracker) application.getAttribute("job.tracker");
    String trackerName = StringUtils.simpleHostname(tracker.getJobTrackerMachine());
    JobTrackerJspHelper jspHelper = new JobTrackerJspHelper();

    List<JobInProgress> runningJobs = tracker.getRunningJobs();
    List<JobInProgress> completedJobs = tracker.getCompletedJobs();
    List<JobInProgress> failedJobs = tracker.getFailedJobs();
  
      out.write("<tracker_name>");
      out.print(trackerName);
      out.write("</tracker_name>");
      out.write("<tracker>");
      out.write("<state>");
      out.print(tracker.getClusterStatus().getJobTrackerState());
      out.write("</state>");
      out.write("<started>");
      out.print(new Date(tracker.getStartTime()));
      out.write("</started>");
      out.write("<version>");
      out.print(VersionInfo.getVersion());
      out.write("</version>");
      out.write("<revision>");
      out.print(VersionInfo.getRevision());
      out.write("</revision>");
      out.write("<compiled_at>");
      out.print(VersionInfo.getDate());
      out.write("</compiled_at>");
      out.write("<compiled_by>");
      out.print(VersionInfo.getUser());
      out.write("</compiled_by>");
      out.write("<identifier>");
      out.print(tracker.getTrackerIdentifier());
      out.write("</identifier>");
      out.write("</tracker>");
      out.write("<cluster_summary>");

        jspHelper.generateSummaryTable(out, tracker);
      
      out.write("</cluster_summary>");
      out.write("<running_jobs>");

        jspHelper.generateJobTable(out, "running", runningJobs);
      
      out.write("</running_jobs>");
      out.write("<completed_jobs>");

        jspHelper.generateJobTable(out, "completed", completedJobs);
      
      out.write("</completed_jobs>");
      out.write("<failed_jobs>");

        jspHelper.generateJobTable(out, "failed", failedJobs);
      
      out.write("</failed_jobs>");
      out.write("<retired_jobs>");

        JSPUtil.generateRetiredJobXml(out, tracker,
            runningJobs.size() + completedJobs.size() + failedJobs.size());
      
      out.write("</retired_jobs>");
      out.write("</cluster>");
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
