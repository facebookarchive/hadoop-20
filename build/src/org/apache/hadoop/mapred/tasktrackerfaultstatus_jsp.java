package org.apache.hadoop.mapred;

import javax.servlet.*;
import javax.servlet.http.*;
import javax.servlet.jsp.*;
import javax.servlet.*;
import javax.servlet.http.*;
import java.io.*;
import java.util.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.JobTracker.FaultInfo;
import org.apache.hadoop.mapred.JobTracker.JobFault;
import org.apache.hadoop.util.*;

public final class tasktrackerfaultstatus_jsp extends org.apache.jasper.runtime.HttpJspBase
    implements org.apache.jasper.runtime.JspSourceDependent {


  private static void printTitle(JspWriter out, String hostname, 
                                 boolean blacklisted) 
      throws IOException {
      
    out.print("<h1>" + hostname + " status: ");
    
    if (blacklisted) {
      out.print("<font color='#A00000'>blacklisted</font>");
    } else {
      out.print("<font color='#00A000'>not blacklisted</font>");
    }
    
    out.println("</h1><hr><br>\n\n");
  }
  
  private static void printJobFailureTable(JspWriter out, JobFault[] faults) 
      throws IOException {
    
    out.println("<h2>Cause: <u>job failures</u></h2>");
    out.println("<table id=\"JobFailureTable\" border=2 cellpadding=5>");
    out.println("<tr><th>Job</th><th># Failures</th>"
        + "<th colspan=10000 align=left>Task Failures</th></tr>");
    
    int i = 0;
    //for (Map.Entry<String,List<String>> e : trackerFaults.entrySet()) throws IOException {
    for (JobFault fault : faults) {
      String jobName = fault.job;
      int numFailures = fault.taskExceptions.length;
      
      out.println("<tr class=\"failure\">");
      out.println("<td>" + jobName + "</td><td align=right>" + 
          numFailures + "</td>");
      
      for (String ex : fault.taskExceptions) {
        out.println("<td>");
        out.println("<div class=\"swapable open\">" + 
            getEncodedSummaryLine(ex) + "</div>");
        out.println("<div hidden><pre>" + getEncoding(ex) + 
            "</pre><div class=\"swapable close\"><br>(collapse)</div></div>");
        out.println("</td>");
      }
      out.println("</tr>");
    }
    out.println("</table><br>\n\n");
  }
  
  private static void printMarkedUnhealthy(JspWriter out) throws IOException {
    out.println("<h2>Cause: <u>marked unhealthy</u></h2><br>\n\n");
  }
  
  private static String getEncoding(String text) throws IOException {
    // Replace \n by <br> and make JS-friendly
    return text.replace("\n","<br>").replace("'","\\\'");
  }
  
  private static String getEncodedSummaryLine(String text) throws IOException {
    return getEncoding(text.split("\n")[0]);
  }
  
  public static void printHTMLDocument(JspWriter out, String hostname, 
                                       JobTracker jt)
      throws IOException {
    
    FaultInfo fi = 
        jt.faultyTrackers.getFaultInfo(hostname, false);
    
    boolean blacklisted = ((fi != null) ? fi.isBlacklisted() : false);
    boolean healthy = ((fi != null) ? fi.isHealthy() : true);
    
    printTitle(out, hostname, blacklisted);
    if (blacklisted) {
        printJobFailureTable(out, fi.getFaults());
    }
    if (!healthy) {
      printMarkedUnhealthy(out);
    }
    
    out.flush();
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

  JobTracker jt = (JobTracker) application.getAttribute("job.tracker");
  String jtName = 
           StringUtils.simpleHostname(jt.getJobTrackerMachine());
  String ttName = request.getParameter("host");

      out.write('\n');
      out.write("\n\n<html>\n<head>\n<link rel=\"stylesheet\" type=\"text/css\" href=\"http://mrtest.data.facebook.com:50030/static/hadoop.css\">\n\n<style type=\"text/css\">\npre { font: 11pt consolas; }\ntr.failure { font: 11pt consolas; vertical-align: text-top; }\n.swapable { cursor: pointer; }\n.swapable:hover { text-decoration: underline; }\n.swapable.open { color: #A00000; }\n.swapable.close { font: 8pt times; color: #0000FF; }\n</style>\n\n<script type=\"text/javascript\">\nfunction getSummaryLine (text) {\n  return text.split('<br>')[0];\n}\ndocument.onclick = function(event) { \n   cn = event.target.className; \n   if (cn === 'swapable open') { \n       children = event.target.parentNode.children;\n       for (i in children) {\n           children[i].hidden = !children[i].hidden;\n       }\n   } else if (cn === 'swapable close') {\n       children = event.target.parentNode.parentNode.children;\n       for (i in children) {\n           children[i].hidden = !children[i].hidden;\n       }\n   }\n}\n</script>\n<title>Blacklist status: ");
      out.print(ttName);
      out.write("</title>\n</head>\n<body>\n\n");

    try {
        printHTMLDocument(out, ttName, jt);
    } catch (Exception ex) {
        ex.printStackTrace(new PrintWriter(out));
    }

      out.write("\n\n</body>\n</html>");
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
