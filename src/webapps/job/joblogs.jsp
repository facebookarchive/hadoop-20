<%@ page
  contentType="text/html; charset=UTF-8"
  import="javax.servlet.*"
  import="javax.servlet.http.*"
  import="java.io.*"
  import="java.util.*"
  import="org.apache.hadoop.mapred.*"
  import="org.apache.hadoop.util.*"
%>
<%
  JobTracker tracker = (JobTracker) application.getAttribute("job.tracker");
  String trackerName =
           StringUtils.simpleHostname(tracker.getJobTrackerMachine());
  String jobid = request.getParameter("jobid");
  JobID jobIdObj = JobID.forName(jobid);
  JobInProgress job = null;
  if (jobid != null) {
    job = (JobInProgress) tracker.getJob(jobIdObj);
  }
%>
<%!
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
%>

<%generateLogsUrls(out, jobid, tracker, job, "map");%>
<%generateLogsUrls(out, jobid, tracker, job, "reduce");%>
