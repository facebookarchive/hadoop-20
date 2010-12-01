<%@ page
  contentType="text/plain; charset=UTF-8"
  import="javax.servlet.*"
  import="javax.servlet.http.*"
  import="java.io.*"
  import="java.util.*"
  import="java.text.DecimalFormat"
  import="org.apache.hadoop.mapred.*"
  import="org.apache.hadoop.util.*"
%>
<%
  JobTracker tracker = (JobTracker) application.getAttribute("job.tracker");
  String trackerName = 
           StringUtils.simpleHostname(tracker.getJobTrackerMachine());
  String type = request.getParameter("type");
%>
<%!
  public void generateTaskTrackerTable(JspWriter out,
                                       String type,
                                       JobTracker tracker) throws IOException {
    Collection<TaskTrackerStatus> c;
    if (("BLACKLISTED").equalsIgnoreCase(type)) {
      c = tracker.blacklistedTaskTrackers();
    } else if (("ACTIVE").equalsIgnoreCase(type)) {
      c = tracker.activeTaskTrackers();
    } else {
      c = tracker.taskTrackers();
    }
    for (TaskTrackerStatus tt : c) {
      out.print(tt.getHost() + "\n");
    }
  }
  public void generateTableForExcludedNodes(JspWriter out, JobTracker tracker) 
  throws IOException {
    // excluded nodes
    for (String tt : tracker.getExcludedNodes()) {
      out.print(tt + "\n");
    }
  }
%>
<%
  if (("EXCLUDED").equalsIgnoreCase(type)) {
    generateTableForExcludedNodes(out, tracker);
  } else {
    generateTaskTrackerTable(out, type, tracker);
  }
%>
