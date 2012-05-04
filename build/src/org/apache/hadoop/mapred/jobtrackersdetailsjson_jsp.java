package org.apache.hadoop.mapred;

import javax.servlet.*;
import javax.servlet.http.*;
import javax.servlet.jsp.*;
import javax.servlet.*;
import javax.servlet.http.*;
import java.io.*;
import java.text.*;
import java.util.*;
import java.net.URL;
import java.text.DecimalFormat;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.conf.*;

public final class jobtrackersdetailsjson_jsp extends org.apache.jasper.runtime.HttpJspBase
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

      out.write("\n    ");

  
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
    
      out.write("\n    {\n    \"mapred.hosts\":  \"");
      out.print(hostsIncludeAbsolute);
      out.write("\",\n    \"mapred.hosts.exclude\": \"");
      out.print(hostsExcludeAbsolute);
      out.write("\",\n    \"mapred.job.tracker\":\"");
      out.print(jobTrackerAddress);
      out.write("\",\n    \"slaves.file\":\"");
      out.print(slavesLocation);
      out.write("\",\n    \"version\": \"");
      out.print(VersionInfo.getVersion());
      out.write("\"\n    ");

      } else if (request.getParameter("status") != null &&
            request.getParameter("status").equals("1")) {
        ClusterStatus fullStatus = tracker.getClusterStatus(true);
        Collection<TaskTrackerStatus> trackers =
                  fullStatus.getTaskTrackersDetails();
        Set<String> activeTrackers =
                  new HashSet<String>(fullStatus.getActiveTrackerNames());
    
      out.write("\n    {\n    ");

        int trackerIndex = 0;
        for (TaskTrackerStatus ttracker : trackers) {
          trackerIndex++;
    
      out.write("\n    \"");
      out.print(ttracker.getHost());
      out.write("\":\n    {\n    ");

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
    
      out.write("\n    \"active\": ");
      out.print(activeTrackers.contains(ttracker.getTrackerName()));
      out.write(",\n    \"last_seen\": ");
      out.print( ttracker.getLastSeen());
      out.write(",\n    \"map_tasks_max\": ");
      out.print( ttracker.getMaxMapSlots());
      out.write(",\n    \"map_tasks_running\": ");
      out.print(runningMaps);
      out.write(",\n    \"reduce_tasks_max\": ");
      out.print(ttracker.getMaxReduceSlots());
      out.write(",\n    \"reduce_tasks_running\": ");
      out.print(runningReduces);
      out.write(",\n    \n    \"tasks\":[\n    ");

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
    
      out.write("\n    {\n    \"job_id\" : ");
      out.print(jobId);
      out.write(",\n    \"task_id\" : ");
      out.print(task.getTaskID().getTaskID().getId());
      out.write(",\n    \"attempt\" : ");
      out.print(task.getTaskID().getId());
      out.write(",\n    \"type\" : \"");
      out.print(isMap?"map":"reduce");
      out.write("\",\n    \"state\" : \"");
      out.print(state.toString());
      out.write("\",\n    \"phase\" : \"");
      out.print(task.getPhase());
      out.write("\",\n    \"progress\": ");
      out.print(task.getProgress());
      out.write(",\n    \"start_time\": ");
      out.print(task.getStartTime());
      out.write(",\n    \"running_time\": ");
      out.print(runningTime);
      out.write("\n    }\n    \n    ");

        if (extendedTasks.size() != taskCount++) {
           
      out.write(',');

        }
            }
    
      out.write("\n    ]\n    }\n    ");

          if (trackerIndex != trackers.size()) {
    
      out.write("\n    ,\n    ");

          }
        }
      }
    
      out.write("\n    }\n\n");
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
