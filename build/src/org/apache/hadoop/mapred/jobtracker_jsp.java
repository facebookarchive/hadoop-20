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
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.util.*;

public final class jobtracker_jsp extends org.apache.jasper.runtime.HttpJspBase
    implements org.apache.jasper.runtime.JspSourceDependent {


  private static DecimalFormat percentFormat = new DecimalFormat("##0.00");

  //  Filter the jobs by a list of jobids
  //  @param jobs  the jobs to be filtered
  //  @param jobIds  the job ids that needs to be retained
  //  @return  filtered jobs
  public List<JobInProgress> filterJobs(List<JobInProgress> jobs, String[] jobIds) {
    List<JobInProgress> filteredJobs = new LinkedList<JobInProgress>();
    Set<String> jobIdSet = new HashSet<String>(Arrays.asList(jobIds));
    for (JobInProgress job : jobs) {
      if (jobIdSet.contains(job.getJobID().toString())) {
        filteredJobs.add(job);
      }
    }
    return filteredJobs;
  }
  
  public void generateSummaryTable(JspWriter out, ClusterMetrics metrics,
                                   JobTracker tracker) throws IOException {
    String tasksPerNode = metrics.getTaskTrackerCount() > 0 ?
      percentFormat.format(((double)(metrics.getMapSlotCapacity() +
      metrics.getReduceSlotCapacity())) / metrics.getTaskTrackerCount()):
      "-";
    out.print("<table border=\"1\" cellpadding=\"5\" cellspacing=\"0\">\n"+
              "<tr><th>Running Map Tasks</th><th>Running Reduce Tasks</th>" + 
              "<th>Total Submissions</th>" +
              "<th>Nodes</th>" + 
              "<th>Occupied Map Slots</th><th>Occupied Reduce Slots</th>" + 
              "<th>Reserved Map Slots</th><th>Reserved Reduce Slots</th>" + 
              "<th>Map Task Capacity</th>" +
              "<th>Reduce Task Capacity</th><th>Avg. Tasks/Node</th>" + 
              "<th>Blacklisted Nodes</th>" +
              "<th>Excluded Nodes</th></tr>\n");
    out.print("<tr><td>" + metrics.getRunningMaps() + "</td><td>" +
              metrics.getRunningReduces() + "</td><td>" + 
              metrics.getTotalJobSubmissions() +
              "</td><td><a href=\"machines.jsp?type=active\">" +
              metrics.getTaskTrackerCount() + "</a></td><td>" + 
              metrics.getOccupiedMapSlots() + "</td><td>" +
              metrics.getOccupiedReduceSlots() + "</td><td>" + 
              metrics.getReservedMapSlots() + "</td><td>" +
              metrics.getReservedReduceSlots() + "</td><td>" + 
              metrics.getMapSlotCapacity() +
              "</td><td>" + metrics.getReduceSlotCapacity() +
              "</td><td>" + tasksPerNode +
              "</td><td><a href=\"machines.jsp?type=blacklisted\">" +
              metrics.getBlackListedTaskTrackerCount() + "</a>" +
              "</td><td><a href=\"machines.jsp?type=excluded\">" +
              metrics.getDecommissionedTaskTrackerCount() + "</a>" +
              "</td></tr></table>\n");

    out.print("<br>");
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
  ClusterMetrics metrics = tracker.getClusterMetrics();
  String trackerName = 
           StringUtils.simpleHostname(tracker.getJobTrackerMachine());
  JobQueueInfo[] queues = tracker.getQueues();
  List<JobInProgress> runningJobs = tracker.getRunningJobs();
  List<JobInProgress> completedJobs = tracker.getCompletedJobs();
  List<JobInProgress> failedJobs = tracker.getFailedJobs();

      out.write('\n');
      out.write("\n\n\n<html>\n<head>\n<title>");
      out.print( trackerName );
      out.write(" Hadoop Map/Reduce Administration</title>\n<link rel=\"stylesheet\" type=\"text/css\" href=\"/static/hadoop.css\">\n<script type=\"text/javascript\" src=\"/static/jobtracker.js\"></script>\n</head>\n<body>\n\n");

// This will redirect the jobtracker.jsp to jobtracker_hmon.jsp.
// To turn it off, put a file named "turn_off_hmon" in $HADOOP_HOME/conf/
  java.io.File tokenFile = new java.io.File("conf/turn_off_hmon");
  if (!tokenFile.exists()) {
    String redirectURL = "jobtracker_hmon.jsp";
    response.sendRedirect(redirectURL);
  }

      out.write('\n');
      out.write('\n');
 JSPUtil.processButtons(request, response, tracker); 
      out.write("\n\n<h1>");
      out.print( trackerName );
      out.write(" Hadoop Map/Reduce Administration</h1>\n\n<div id=\"quicklinks\">\n  <a href=\"#quicklinks\" onclick=\"toggle('quicklinks-list'); return false;\">Quick Links</a>\n  <ul id=\"quicklinks-list\">\n    <li><a href=\"#scheduling_info\">Scheduling Info</a></li>\n    <li><a href=\"#running_jobs\">Running Jobs</a></li>\n    <li><a href=\"#retired_jobs\">Retired Jobs</a></li>\n    <li><a href=\"#local_logs\">Local Logs</a></li>\n  </ul>\n</div>\n\n<b>State:</b> ");
      out.print( status.getJobTrackerState() );
      out.write("<br>\n<b>Started:</b> ");
      out.print( new Date(tracker.getStartTime()));
      out.write("<br>\n<b>Version:</b> ");
      out.print( VersionInfo.getVersion());
      out.write(",\n                r");
      out.print( VersionInfo.getRevision());
      out.write("<br>\n<b>Compiled:</b> ");
      out.print( VersionInfo.getDate());
      out.write(" by \n                 ");
      out.print( VersionInfo.getUser());
      out.write("<br>\n<b>Identifier:</b> ");
      out.print( tracker.getTrackerIdentifier());
      out.write("<br>                 \n                   \n<hr>\n<h2>Cluster Summary (Heap Size is ");
      out.print( StringUtils.byteDesc(status.getUsedMemory()) );
      out.write('/');
      out.print( StringUtils.byteDesc(status.getMaxMemory()) );
      out.write(")</h2>\n");
 
 generateSummaryTable(out, metrics, tracker); 

      out.write("\n<hr>\n<h2 id=\"scheduling_info\">Scheduling Information</h2>\n<table border=\"2\" cellpadding=\"5\" cellspacing=\"2\">\n<thead style=\"font-weight: bold\">\n<tr>\n<td> Queue Name </td>\n<td> Scheduling Information</td>\n</tr>\n</thead>\n<tbody>\n");

for(JobQueueInfo queue: queues) {
  String queueName = queue.getQueueName();
  String schedulingInformation = queue.getSchedulingInfo();
  if(schedulingInformation == null || schedulingInformation.trim().equals("")) {
    schedulingInformation = "NA";
  }

      out.write("\n<tr>\n<td><a href=\"jobqueue_details.jsp?queueName=");
      out.print(queueName);
      out.write('"');
      out.write('>');
      out.print(queueName);
      out.write("</a></td>\n<td>");
      out.print(schedulingInformation.replaceAll("\n","<br/>") );
      out.write("\n</td>\n</tr>\n");

}

      out.write("\n</tbody>\n</table>\n<hr/>\n<b>Filter (Jobid, Priority, User, Name)</b> <input type=\"text\" id=\"filter\" onkeyup=\"applyfilter()\"> <br>\n<span class=\"small\">Example: 'user:smith 3200' will filter by 'smith' only in the user field and '3200' in all fields</span>\n<hr>\n\n<h2 id=\"running_jobs\">Running Jobs</h2>\n");

if (request.getParameter("jobid") == null) {
  out.print(JSPUtil.generateJobTable("Running", runningJobs, 30, 0));
} else {
  out.print(JSPUtil.generateJobTable("Running", filterJobs(runningJobs, request.getParameter("jobid").split(",")), 30, 0));
}

      out.write("\n\n<hr>\n\n");

if (completedJobs.size() > 0) {
  out.print("<h2 id=\"completed_jobs\">Completed Jobs</h2>");
  out.print(JSPUtil.generateJobTable("Completed", completedJobs, 0, 
    runningJobs.size()));
  out.print("<hr>");
}

      out.write('\n');
      out.write('\n');

if (failedJobs.size() > 0) {
  out.print("<h2 id=\"failed_jobs\">Failed Jobs</h2>");
  out.print(JSPUtil.generateJobTable("Failed", failedJobs, 0, 
    (runningJobs.size()+completedJobs.size())));
  out.print("<hr>");
}

      out.write("\n\n<h2 id=\"retired_jobs\">Retired Jobs</h2>\n");
      out.print(JSPUtil.generateRetiredJobTable(tracker, 
  (runningJobs.size()+completedJobs.size()+failedJobs.size())));
      out.write("\n<hr>\n\n<h2 id=\"local_logs\">Local Logs</h2>\n<a href=\"logs/\">Log</a> directory, <a href=\"jobhistory.jsp\">\nJob Tracker History</a>\n\n");

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
