package org.apache.hadoop.mapred;

import javax.servlet.*;
import javax.servlet.http.*;
import javax.servlet.jsp.*;
import javax.servlet.*;
import javax.servlet.http.*;
import java.io.*;
import java.text.*;
import java.util.*;
import java.text.DecimalFormat;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.util.*;

public final class jobdetailsjson_jsp extends org.apache.jasper.runtime.HttpJspBase
    implements org.apache.jasper.runtime.JspSourceDependent {


  private void printTaskSummary(JspWriter out,
                                String kind,
                                TaskInProgress[] tasks
                               ) throws IOException {
    int totalTasks = tasks.length;
    int runningTasks = 0;
    int finishedTasks = 0;
    int killedTasks = 0;
    int failedTaskAttempts = 0;
    int killedTaskAttempts = 0;
    long totalTime = 0;
    for(int i=0; i < totalTasks; ++i) {
      TaskInProgress task = tasks[i];
      if (task.isComplete()) {
        finishedTasks += 1;
      } else if (task.isRunning()) {
        runningTasks += 1;
      } else if (task.wasKilled()) {
        killedTasks += 1;
      }
      failedTaskAttempts += task.numTaskFailures();
      killedTaskAttempts += task.numKilledTasks();
      long now = System.currentTimeMillis();
      for(TaskStatus status: task.getTaskStatuses()) {
        long start = status.getStartTime();
        long end = now;
        if (status.getRunState() == TaskStatus.State.SUCCEEDED
            || status.getRunState() == TaskStatus.State.FAILED
            || status.getRunState() == TaskStatus.State.KILLED) {
          end = status.getFinishTime();
        }
        totalTime += end - start;
      }
    }
    int pendingTasks = totalTasks - runningTasks - killedTasks - finishedTasks;
    out.print(String.format("\"total%s\" : %d,\n", kind, totalTasks));
    out.print(String.format("\"running%s\" : %d,\n", kind, runningTasks));
    out.print(String.format("\"finished%s\" : %d,\n", kind, finishedTasks));
    out.print(String.format("\"failed%s\" : %d,\n", kind, failedTaskAttempts));
    out.print(String.format("\"killed%s\" : %d,\n", kind, killedTaskAttempts));
    out.print(String.format("\"timeIn%s\" : %d,\n", kind, totalTime));
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
      response.setContentType("application/json");
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

      out.write('\n');
      out.write('\n');

    String jobId = request.getParameter("jobid");
    JobID jobIdObj = JobID.forName(jobId);
    JobInProgress job = (JobInProgress) tracker.getJob(jobIdObj);
    if (job == null) {
      return;
    }
    JobProfile profile = job.getProfile();
    JobStatus status = job.getStatus();
    int runState = status.getRunState();

      out.write("\n{\n\"jobId\" : \"");
      out.print(jobId);
      out.write("\",\n\"user\" : \"");
      out.print(profile.getUser());
      out.write("\",\n\"name\" : \"");
      out.print(profile.getJobName().replaceAll("\"", "\\\""));
      out.write("\",\n");

  String statusString = "PENDING";
    if (runState == JobStatus.RUNNING) {
        statusString = "RUNNING";
    } else if (runState == JobStatus.SUCCEEDED) {
        statusString = "SUCCEEDED";
    } else if (runState == JobStatus.FAILED) {
      statusString = "FAILED";
    }

      out.write("\n\"status\" : \"");
      out.print(statusString);
      out.write("\",\n\"mapProgress\" : ");
      out.print(status.mapProgress());
      out.write(",\n\"reduceProgress\" : ");
      out.print(status.reduceProgress());
      out.write(',');
      out.write('\n');

    printTaskSummary(out, "Maps", job.getTasks(TaskType.MAP));
    printTaskSummary(out, "Reduces", job.getTasks(TaskType.REDUCE));
    Counters counters = job.getCounters();

      out.write("\n\"hdfsBytesRead\" : ");
      out.print(counters.findCounter(Task.FILESYSTEM_COUNTER_GROUP,"HDFS_BYTES_READ"));
      out.write(",\n\"hdfsBytesWritten\" : ");
      out.print(counters.findCounter(Task.FILESYSTEM_COUNTER_GROUP,"HDFS_BYTES_WRITTEN"));
      out.write(",\n\"hdfsFilesCreated\" : ");
      out.print(counters.findCounter(Task.FILESYSTEM_COUNTER_GROUP,"HDFS_FILES_CREATED"));
      out.write("\n\"localBytesRead\" : ");
      out.print(counters.findCounter(Task.FILESYSTEM_COUNTER_GROUP,"FILE_BYTES_READ"));
      out.write(",\n\"localBytesWritten\" : ");
      out.print(counters.findCounter(Task.FILESYSTEM_COUNTER_GROUP,"FILE_BITES_WRITTEN"));
      out.write(",\n\"mapOutputBytes\" : ");
      out.print(counters.getCounter(Task.Counter.MAP_OUTPUT_BYTES));
      out.write("\n}\n");
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
