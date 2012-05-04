package org.apache.hadoop.mapred;

import javax.servlet.*;
import javax.servlet.http.*;
import javax.servlet.jsp.*;
import javax.servlet.*;
import javax.servlet.http.*;
import java.io.*;
import java.lang.String;
import java.util.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;
import java.text.SimpleDateFormat;
import org.apache.hadoop.util.*;

public final class coronataskdetails_jsp extends org.apache.jasper.runtime.HttpJspBase
    implements org.apache.jasper.runtime.JspSourceDependent {

static SimpleDateFormat dateFormat = new SimpleDateFormat(
      "d-MMM-yyyy HH:mm:ss");

  private static final String PRIVATE_ACTIONS_KEY = "webinterface.private.actions";

  private String detailsUrl = null;
  private String jobUrl = null;
  private String statsUrl = null;
  private boolean hasProxy = false;

  private String getProxyUrl(String proxyPath, String params) {
    return proxyPath + (hasProxy ? "&" : "?") + params;
  }

private void printConfirm(JspWriter out, String jobid, String tipid,
      String taskid, String action) throws IOException {
    String url = getProxyUrl(detailsUrl, "tipid=" + tipid
                             + "&taskid=" + taskid);
    out.print("<html><head><META http-equiv=\"refresh\" content=\"15;URL="
        + url + "\"></head>" + "<body><h3> Are you sure you want to"
        + " kill/fail/speculate "
        + taskid + " ?<h3><br><table border=\"0\"><tr><td width=\"100\">"
        + "<form action=\"" + url + "\" method=\"post\">"
        + "<input type=\"hidden\" name=\"action\" value=\"" + action + "\" />"
        + "<input type=\"submit\" name=\"Kill/Fail/Speculate\""
        + " value=\"Kill/Fail/Speculate\" />"
        + "</form>"
        + "</td><td width=\"100\"><form method=\"post\" action=\"" + url
        + "\"><input type=\"submit\" value=\"Cancel\" name=\"Cancel\""
        + "/></form></td></tr></table></body></html>");
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
      out.write('\n');
      out.write('\n');

    CoronaJobTracker tracker = (CoronaJobTracker) application.getAttribute("job.tracker");
    CoronaJobInProgress job = (CoronaJobInProgress) tracker.getJob();
    JobID jobId = (job != null) ? job.getStatus().getJobID() : null;

    String tipid = request.getParameter("tipid");
    TaskID tipidObj = TaskID.forName(tipid);
    TaskInProgress tip = null;

    if (job != null && tipidObj != null) {
      tip = job.getTaskInProgress(tipidObj);
    }

    String taskid = request.getParameter("taskid");
    TaskAttemptID taskidObj = TaskAttemptID.forName(taskid);

    detailsUrl = tracker.getProxyUrl("coronataskdetails.jsp");
    jobUrl = tracker.getProxyUrl("coronajobdetails.jsp");
    statsUrl = tracker.getProxyUrl("coronataskstats.jsp");
    hasProxy = (detailsUrl.indexOf('?') != -1);

    boolean privateActions = JSPUtil.conf.getBoolean(PRIVATE_ACTIONS_KEY, false);
    if (privateActions) {
      String action = request.getParameter("action");
      if (action != null) {
        if (action.equalsIgnoreCase("confirm")) {
          String subAction = request.getParameter("subaction");
          if (subAction == null)
            subAction = "fail-task";
          printConfirm(out, jobId.toString(), tipid, taskid, subAction);
          return;
        }
        else if (action.equalsIgnoreCase("kill-task") 
            && request.getMethod().equalsIgnoreCase("POST")) {
          tracker.killTask(taskidObj, false);
          //redirect again so that refreshing the page will not attempt to rekill the task
          response.sendRedirect(getProxyUrl(detailsUrl, "subaction=kill-task"
                                            + "&tipid=" + tipid));
        }
        else if (action.equalsIgnoreCase("fail-task")
            && request.getMethod().equalsIgnoreCase("POST")) {
          tracker.killTask(taskidObj, true);
          response.sendRedirect(getProxyUrl(detailsUrl, "subaction=fail-task"
                                            + "&tipid=" + tipid));
        }
        else if (action.equalsIgnoreCase("speculative-task")
            && request.getMethod().equalsIgnoreCase("POST")) {
          if (tip != null) {
            tip.setSpeculativeForced(true);
          }
          response.sendRedirect(getProxyUrl(detailsUrl,
                                            "subaction=speculative-task"
                                            + "&tipid=" + tipid
                                            + "&here=yes"));
        }
      }
    }
    TaskStatus[] ts = null;
    if (tip != null) { 
      ts = tip.getTaskStatuses();
    }
    boolean isCleanupOrSetup = false;
    if ( tip != null) {
      isCleanupOrSetup = tip.isJobCleanupTask();
      if (!isCleanupOrSetup) {
        isCleanupOrSetup = tip.isJobSetupTask();
      }
    }

      out.write("\n\n\n<html>\n<head>\n  <link rel=\"stylesheet\" type=\"text/css\" href=\"/static/hadoop.css\">\n  <title>Hadoop Task Details</title>\n</head>\n<body>\n");

   out.println("<h1>Job <a href=\"" +
               jobUrl + "\">" + jobId.toString() + "</a></h1>");

      out.write("\n\n<hr>\n\n<h2>All Task Attempts</h2>\n<center>\n\n<table border=2 cellpadding=\"5\" cellspacing=\"2\">\n<tr><td align=\"center\">Task Attempts</td><td>Machine</td><td>Status</td><td>Progress</td><td>Start Time</td> \n  ");

   if (ts.length > 0 && !ts[0].getIsMap() && !isCleanupOrSetup) {
   
      out.write("\n<td>Shuffle Finished</td><td>Sort Finished</td>\n  ");

  }
  
      out.write("\n<td>Finish Time</td><td>Errors</td><td>Task Logs</td><td>Counters</td><td>Actions</td></tr>\n  ");

    for (int i = 0; i < ts.length; i++) {
      TaskStatus status = ts[i];
      String taskTrackerName = status.getTaskTracker();
      TaskTrackerStatus taskTracker = tracker.getTaskTrackerStatus(taskTrackerName);
      out.print("<tr><td>" + status.getTaskID() + "</td>");
      String taskAttemptTracker = null;
      String cleanupTrackerName = null;
      TaskTrackerStatus cleanupTracker = null;
      String cleanupAttemptTracker = null;
      boolean hasCleanupAttempt = false;
      if (tip != null && tip.isCleanupAttempt(status.getTaskID())) {
        cleanupTrackerName = tip.machineWhereCleanupRan(status.getTaskID());
        cleanupTracker = tracker.getTaskTrackerStatus(cleanupTrackerName);
        if (cleanupTracker != null) {
          cleanupAttemptTracker = "http://" + cleanupTracker.getHost() + ":"
            + cleanupTracker.getHttpPort();
        }
        hasCleanupAttempt = true;
      }
      out.print("<td>");
      if (hasCleanupAttempt) {
        out.print("Task attempt: ");
      }
      if (taskTracker == null) {
        out.print(taskTrackerName);
      } else {
        taskAttemptTracker = "http://" + taskTracker.getHost() + ":"
          + taskTracker.getHttpPort();
        out.print("<a href=\"" + taskAttemptTracker + "\">"
          + taskTracker.getHost() + "</a>");
      }
      if (hasCleanupAttempt) {
        out.print("<br/>Cleanup Attempt: ");
        if (cleanupAttemptTracker == null ) {
          out.print(cleanupTrackerName);
        } else {
          out.print("<a href=\"" + cleanupAttemptTracker + "\">"
            + cleanupTracker.getHost() + "</a>");
        }
      }
      out.print("</td>");
        out.print("<td>" + status.getRunState() + "</td>");
        out.print("<td>" + StringUtils.formatPercent(status.getProgress(), 2)
          + ServletUtil.percentageGraph(status.getProgress() * 100f, 80) + "</td>");
        out.print("<td>"
          + StringUtils.getFormattedTimeWithDiff(dateFormat, status
          .getStartTime(), 0) + "</td>");
        if (!ts[i].getIsMap() && !isCleanupOrSetup) {
          out.print("<td>"
          + StringUtils.getFormattedTimeWithDiff(dateFormat, status
          .getShuffleFinishTime(), status.getStartTime()) + "</td>");
        out.println("<td>"
          + StringUtils.getFormattedTimeWithDiff(dateFormat, status
          .getSortFinishTime(), status.getShuffleFinishTime())
          + "</td>");
        }
        out.println("<td>"
          + StringUtils.getFormattedTimeWithDiff(dateFormat, status
          .getFinishTime(), status.getStartTime()) + "</td>");

        out.print("<td><pre>");
        String [] failures = tracker.getTaskDiagnostics(status.getTaskID());
        if (failures == null) {
          out.print("&nbsp;");
        } else {
          for(int j = 0 ; j < failures.length ; j++){
            out.print(failures[j]);
            if (j < (failures.length - 1)) {
              out.print("\n-------\n");
            }
          }
        }
        out.print("</pre></td>");
        out.print("<td>");
        String taskLogUrl = null;
        if (taskTracker != null ) {
        	taskLogUrl = TaskLogServlet.getTaskLogUrl(taskTracker.getHost(),
        						String.valueOf(taskTracker.getHttpPort()),
        						status.getTaskID().toString());
      	}
        if (hasCleanupAttempt) {
          out.print("Task attempt: <br/>");
        }
        if (taskLogUrl == null) {
          out.print("n/a");
        } else {
          String tailFourKBUrl = taskLogUrl + "&start=-4097";
          String tailEightKBUrl = taskLogUrl + "&start=-8193";
          String entireLogUrl = taskLogUrl + "&all=true";
          out.print("<a href=\"" + tailFourKBUrl + "\">Last 4KB</a><br/>");
          out.print("<a href=\"" + tailEightKBUrl + "\">Last 8KB</a><br/>");
          out.print("<a href=\"" + entireLogUrl + "\">All</a><br/>");
        }
        if (hasCleanupAttempt) {
          out.print("Cleanup attempt: <br/>");
          taskLogUrl = null;
          if (cleanupTracker != null ) {
        	taskLogUrl = TaskLogServlet.getTaskLogUrl(cleanupTracker.getHost(),
                                String.valueOf(cleanupTracker.getHttpPort()),
                                status.getTaskID().toString());
      	  }
          if (taskLogUrl == null) {
            out.print("n/a");
          } else {
            String tailFourKBUrl = taskLogUrl + "&start=-4097&cleanup=true";
            String tailEightKBUrl = taskLogUrl + "&start=-8193&cleanup=true";
            String entireLogUrl = taskLogUrl + "&all=true&cleanup=true";
            out.print("<a href=\"" + tailFourKBUrl + "\">Last 4KB</a><br/>");
            out.print("<a href=\"" + tailEightKBUrl + "\">Last 8KB</a><br/>");
            out.print("<a href=\"" + entireLogUrl + "\">All</a><br/>");
          }
        }
        out.print("</td><td>" + "<a href=\"" + getProxyUrl(statsUrl, "jobid=" + jobId
          + "&tipid=" + tipid + "&taskid=" + status.getTaskID()) + "\">"
          + ((status.getCounters() != null) ? status.getCounters().size() : 0) + "</a></td>");
        out.print("<td>");
        if (privateActions
          && status.getRunState() == TaskStatus.State.RUNNING) {
          out.print("<a href=\"" + getProxyUrl(detailsUrl, "action=confirm"
              + "&subaction=kill-task" + "&jobid=" + jobId + "&tipid="
              + tipid + "&taskid=" + status.getTaskID()) + "\" > Kill </a>");
          out.print("<br><a href=\"" + getProxyUrl(detailsUrl, "action=confirm"
              + "&subaction=fail-task" + "&jobid=" + jobId + "&tipid="
              + tipid + "&taskid=" + status.getTaskID()) + "\" > Fail </a>");
          if (tip != null && !tip.isSpeculativeForced()) {
            out.print("<br><a href=\"" + getProxyUrl(detailsUrl, "action=confirm"
                + "&subaction=speculative-task" + "&jobid=" + jobId + "&tipid="
                + tipid + "&taskid=" + status.getTaskID())
                + "\" > Speculate </a>");
          } else {
            out.print("<br>Speculative");
          }
        }
        else
          out.print("<pre>&nbsp;</pre>");
        out.println("</td></tr>");
      }
  
      out.write("\n</table>\n</center>\n\n");

      if (ts.length > 0 && ts[0].getIsMap() && !isCleanupOrSetup) {

      out.write("\n<h3>Input Split Locations</h3>\n<table border=2 cellpadding=\"5\" cellspacing=\"2\">\n");

        for (String split: StringUtils.split(tracker.getTip(
                                         tipidObj).getSplitNodes())) {
          out.println("<tr><td>" + split + "</td></tr>");
        }

      out.write("\n</table>\n");
    
    }

      out.write("\n\n<hr>\n");

  out.println("<a href=\"" +
              getProxyUrl(jobUrl, "jobid=" + jobId.toString()) + 
              "\">Go back to the job</a><br>");
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
