<%@ page
  contentType="application/json"
  import="javax.servlet.*"
  import="javax.servlet.http.*"
  import="java.io.*"
  import="java.text.*"
  import="java.util.*"
  import="java.text.DecimalFormat"
  import="org.apache.hadoop.mapred.*"
  import="org.apache.hadoop.mapreduce.TaskType"
  import="org.apache.hadoop.util.*"
%>
<%
  JobTracker tracker = (JobTracker) application.getAttribute("job.tracker");
  String trackerName =
           StringUtils.simpleHostname(tracker.getJobTrackerMachine());
%>
<%!
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
%>
<%
    String jobId = request.getParameter("jobid");
    JobID jobIdObj = JobID.forName(jobId);
    JobInProgress job = (JobInProgress) tracker.getJob(jobIdObj);
    if (job == null) {
      return;
    }
    JobProfile profile = job.getProfile();
    JobStatus status = job.getStatus();
    int runState = status.getRunState();
%>
{
"jobId" : "<%=jobId%>",
"user" : "<%=profile.getUser()%>",
"name" : "<%=profile.getJobName().replaceAll("\"", "\\\"")%>",
<%
  String statusString = "PENDING";
    if (runState == JobStatus.RUNNING) {
        statusString = "RUNNING";
    } else if (runState == JobStatus.SUCCEEDED) {
        statusString = "SUCCEEDED";
    } else if (runState == JobStatus.FAILED) {
      statusString = "FAILED";
    }
%>
"status" : "<%=statusString%>",
"mapProgress" : <%=status.mapProgress()%>,
"reduceProgress" : <%=status.reduceProgress()%>,
<%
    printTaskSummary(out, "Maps", job.getTasks(TaskType.MAP));
    printTaskSummary(out, "Reduces", job.getTasks(TaskType.REDUCE));
    Counters counters = job.getCounters();
%>
"hdfsBytesRead" : <%=counters.findCounter(Task.FILESYSTEM_COUNTER_GROUP,"HDFS_BYTES_READ")%>,
"hdfsBytesWritten" : <%=counters.findCounter(Task.FILESYSTEM_COUNTER_GROUP,"HDFS_BYTES_WRITTEN")%>,
"hdfsFilesCreated" : <%=counters.findCounter(Task.FILESYSTEM_COUNTER_GROUP,"HDFS_FILES_CREATED")%>
"localBytesRead" : <%=counters.findCounter(Task.FILESYSTEM_COUNTER_GROUP,"FILE_BYTES_READ")%>,
"localBytesWritten" : <%=counters.findCounter(Task.FILESYSTEM_COUNTER_GROUP,"FILE_BITES_WRITTEN")%>,
"mapOutputBytes" : <%=counters.getCounter(Task.Counter.MAP_OUTPUT_BYTES)%>
}
