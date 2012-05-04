package org.apache.hadoop.mapred;

import javax.servlet.*;
import javax.servlet.http.*;
import javax.servlet.jsp.*;
import java.io.*;
import java.util.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.util.*;
import javax.servlet.jsp.*;
import java.text.SimpleDateFormat;
import org.apache.hadoop.mapred.JobHistory.*;

public final class loadhistory_jsp extends org.apache.jasper.runtime.HttpJspBase
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

    PathFilter jobLogFileFilter = new PathFilter() {
      public boolean accept(Path path) {
        return !(path.getName().endsWith(".xml"));
      }
    };
    
    FileSystem fs = (FileSystem) application.getAttribute("fileSys");
    String jobId =  (String)request.getParameter("jobid");
    JobHistory.JobInfo job = (JobHistory.JobInfo)
                               request.getSession().getAttribute("job");
    // if session attribute of JobInfo exists and is of different job's,
    // then remove the attribute
    // if the job has not yet finished, remove the attribute sothat it 
    // gets refreshed.
    boolean isJobComplete = false;
    if (null != job) {
      String jobStatus = job.get(Keys.JOB_STATUS);
      isJobComplete = Values.SUCCESS.name() == jobStatus
                      || Values.FAILED.name() == jobStatus
                      || Values.KILLED.name() == jobStatus;
    }
    if (null != job && 
       (!jobId.equals(job.get(Keys.JOBID)) 
         || !isJobComplete)) {
      // remove jobInfo from session, keep only one job in session at a time
      request.getSession().removeAttribute("job"); 
      job = null ; 
    }
	
    if (null == job) {
      String jobLogFile = (String)request.getParameter("logFile");
      job = new JobHistory.JobInfo(jobId); 
      DefaultJobHistoryParser.parseJobTasks(jobLogFile, job, fs) ; 
      request.getSession().setAttribute("job", job);
      request.getSession().setAttribute("fs", fs);
    }

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
