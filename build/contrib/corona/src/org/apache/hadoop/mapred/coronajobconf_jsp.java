package org.apache.hadoop.mapred;

import javax.servlet.*;
import javax.servlet.http.*;
import javax.servlet.jsp.*;
import javax.servlet.*;
import javax.servlet.http.*;
import java.io.*;
import java.net.URL;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.OutputBuffer;
import org.apache.hadoop.io.IOUtils;

public final class coronajobconf_jsp extends org.apache.jasper.runtime.HttpJspBase
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

      out.write("\n\n\n");

  CoronaJobTracker tracker = (CoronaJobTracker) application.getAttribute("job.tracker");
  CoronaJobInProgress job = tracker.getJob();
  JobID jobId = (job != null) ? job.getStatus().getJobID() : null;
  String jobid = request.getParameter("jobid");
  if (job == null) {
    out.println("<h2>No running job!</h2>");
    return;
  }
  if (!jobId.toString().equals(jobid)) {
    out.println("<h2>Running job: " + jobId + " does not match requested job:" + jobid + "!</h2>");
    return;
  }

      out.write("\n  \n<html>\n\n<title>Job Configuration: JobId - ");
      out.print( jobid );
      out.write("</title>\n\n<body>\n<h2>Job Configuration: JobId - ");
      out.print( jobid );
      out.write("</h2><br>\n\n");

  OutputBuffer os = new OutputBuffer();
  try {
    job.getConf().writeXml(os);
    InputStream jobis = new ByteArrayInputStream(os.getData(), 0, os.getLength());
    InputStream xslis = job.getConf().getConfResourceAsInputStream("webapps/static/jobconf.xsl");

    if (xslis == null) {
      out.println("jobconf.xsl not found");
      return;
    }

    XMLUtils.transform(xslis, jobis, out);
  } catch (Exception e) {
    out.println("Unable to display configuration !");
    out.println(e);
    out.println(new String(os.getData()));
  }

      out.write("\n\n<br>\n");

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
