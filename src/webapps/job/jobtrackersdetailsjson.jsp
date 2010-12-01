<%@ page
    contentType="application/json"
    import="javax.servlet.*"
    import="javax.servlet.http.*"
    import="java.io.*"
    import="java.text.*"
    import="java.util.*"
    import="java.net.URL"
    import="java.text.DecimalFormat"
    import="org.apache.hadoop.mapred.*"
    import="org.apache.hadoop.util.*"
    import="org.apache.hadoop.conf.*" %>
    <%
  
      JobTracker tracker = (JobTracker) application.getAttribute("job.tracker");
      if (request.getParameter("jobTrackerConf") != null &&
              request.getParameter("jobTrackerConf").equals("1")) {
        // Only return the paths to hostsFiles
        Configuration conf = new Configuration();
        conf.addResource("mapred-site.xml");
    
        String hostsInclude = conf.get("mapred.hosts","");
        String hostsExclude = conf.get("mapred.hosts.exclude", "");
        String jobTrackerAddress = conf.get("mapred.job.tracker");

        String hostsIncludeAbsolute = (new File(hostsInclude)).getAbsolutePath();
        String hostsExcludeAbsolute = (new File(hostsExclude)).getAbsolutePath();
        String slavesLocation = JobTracker.class.getClassLoader().
                getResource("slaves").getFile();
    %>
    {
    "mapred.hosts":  "<%=hostsIncludeAbsolute%>",
    "mapred.hosts.exclude": "<%=hostsExcludeAbsolute%>",
    "mapred.job.tracker":"<%=jobTrackerAddress%>",
    "slaves.file":"<%=slavesLocation%>",
    "version": "<%=VersionInfo.getVersion()%>"
    <%
      } else if (request.getParameter("status") != null &&
            request.getParameter("status").equals("1")) {
        ClusterStatus fullStatus = tracker.getClusterStatus(true);
        Collection<TaskTrackerStatus> trackers =
                  fullStatus.getTaskTrackersDetails();
        Set<String> activeTrackers =
                  new HashSet<String>(fullStatus.getActiveTrackerNames());
    %>
    {
    <%
        int trackerIndex = 0;
        for (TaskTrackerStatus ttracker : trackers) {
          trackerIndex++;
    %>
    "<%=ttracker.getHost()%>":
    {
    <%
            List<TaskStatus> tasks = ttracker.getTaskReports();
            int runningMaps = 0;
            int runningReduces = 0;
            for (TaskStatus task : tasks) {
              TaskStatus.State state = task.getRunState();
              if (task.getIsMap() &&
                    (state == TaskStatus.State.RUNNING ||
                    state == TaskStatus.State.UNASSIGNED)) {
                runningMaps++;
              } else if (!task.getIsMap() &&
                      (state == TaskStatus.State.RUNNING ||
                      state == TaskStatus.State.UNASSIGNED)) {
                runningReduces++;
              } else if (!task.getIsMap()) {
                System.out.println(state);
              }
            }
    %>
    "active": <%=activeTrackers.contains(ttracker.getTrackerName())%>,
    "last_seen": <%= ttracker.getLastSeen()%>,
    "map_tasks_max": <%= ttracker.getMaxMapSlots()%>,
    "map_tasks_running": <%=runningMaps%>,
    "reduce_tasks_max": <%=ttracker.getMaxReduceSlots()%>,
    "reduce_tasks_running": <%=runningReduces%>,
    
    "tasks":[
    <%
    int taskCount = 1;
    Collection<TaskStatus> extendedTasks =
            fullStatus.getTaskTrackerTasksStatuses(ttracker.getTrackerName());
        for (TaskStatus task : extendedTasks) {
            int jobId = task.getTaskID().getJobID().getId();
            boolean isMap = task.getIsMap();
            TaskStatus.State state = task.getRunState();
            long runningTime = 0;
            if (state == TaskStatus.State.SUCCEEDED) {
              runningTime = task.getFinishTime() - task.getStartTime();
            } else {
              runningTime = System.currentTimeMillis() - task.getStartTime();
            }
    %>
    {
    "job_id" : <%=jobId%>,
    "task_id" : <%=task.getTaskID().getTaskID().getId()%>,
    "attempt" : <%=task.getTaskID().getId()%>,
    "type" : "<%=isMap?"map":"reduce"%>",
    "state" : "<%=state.toString()%>",
    "phase" : "<%=task.getPhase()%>",
    "progress": <%=task.getProgress()%>,
    "start_time": <%=task.getStartTime()%>,
    "running_time": <%=runningTime%>
    }
    
    <%
        if (extendedTasks.size() != taskCount++) {
           %>,<%
        }
            }
    %>
    ]
    }
    <%
          if (trackerIndex != trackers.size()) {
    %>
    ,
    <%
          }
        }
      }
    %>
    }

