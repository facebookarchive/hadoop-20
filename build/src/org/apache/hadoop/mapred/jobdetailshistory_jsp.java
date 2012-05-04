package org.apache.hadoop.mapred;

import javax.servlet.*;
import javax.servlet.http.*;
import javax.servlet.jsp.*;
import javax.servlet.http.*;
import java.io.*;
import java.util.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;
import java.text.*;
import org.apache.hadoop.mapred.JobHistory.*;

public final class jobdetailshistory_jsp extends org.apache.jasper.runtime.HttpJspBase
    implements org.apache.jasper.runtime.JspSourceDependent {

 static SimpleDateFormat dateFormat = new SimpleDateFormat("d-MMM-yyyy HH:mm:ss") ; 
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

    String jobid = request.getParameter("jobid");
    String logFile = request.getParameter("logFile");
	String encodedLogFileName = JobHistory.JobInfo.encodeJobHistoryFilePath(logFile);
	
    Path jobFile = new Path(logFile);
    String[] jobDetails = jobFile.getName().split("_");
    String jobUniqueString = jobDetails[0] + "_" +jobDetails[1] + "_" + jobid ;
	
    FileSystem fs = (FileSystem) application.getAttribute("fileSys");
    JobInfo job = JSPUtil.getJobInfo(request, fs);

      out.write("\n\n<html>\n<head>\n<title>Hadoop Job ");
      out.print(jobid);
      out.write(" on History Viewer</title>\n<link rel=\"stylesheet\" type=\"text/css\" href=\"/static/hadoop.css\">\n</head>\n<body>\n\n<h2>Hadoop Job ");
      out.print(jobid );
      out.write(" on <a href=\"jobhistory.jsp\">History Viewer</a></h2>\n\n<b>User: </b> ");
      out.print(job.get(Keys.USER) );
      out.write("<br/> \n<b>JobName: </b> ");
      out.print(job.get(Keys.JOBNAME) );
      out.write("<br/> \n<b>JobConf: </b> <a href=\"jobconf_history.jsp?jobid=");
      out.print(jobid);
      out.write("&jobLogDir=");
      out.print(new Path(logFile).getParent().toString());
      out.write("&jobUniqueString=");
      out.print(jobUniqueString);
      out.write("\"> \n                 ");
      out.print(job.get(Keys.JOBCONF) );
      out.write("</a><br/> \n<b>Submitted At: </b> ");
      out.print(StringUtils.getFormattedTimeWithDiff(dateFormat, job.getLong(Keys.SUBMIT_TIME), 0 )  );
      out.write("<br/> \n<b>Launched At: </b> ");
      out.print(StringUtils.getFormattedTimeWithDiff(dateFormat, job.getLong(Keys.LAUNCH_TIME), job.getLong(Keys.SUBMIT_TIME)) );
      out.write("<br/>\n<b>Finished At: </b>  ");
      out.print(StringUtils.getFormattedTimeWithDiff(dateFormat, job.getLong(Keys.FINISH_TIME), job.getLong(Keys.LAUNCH_TIME)) );
      out.write("<br/>\n<b>Status: </b> ");
      out.print( ((job.get(Keys.JOB_STATUS) == "")?"Incomplete" :job.get(Keys.JOB_STATUS)) );
      out.write("<br/> \n");

    Map<String, JobHistory.Task> tasks = job.getAllTasks();
    int totalMaps = 0 ; 
    int totalReduces = 0;
    int totalCleanups = 0; 
    int totalSetups = 0; 
    int numFailedMaps = 0; 
    int numKilledMaps = 0;
    int numFailedReduces = 0 ; 
    int numKilledReduces = 0;
    int numFinishedCleanups = 0;
    int numFailedCleanups = 0;
    int numKilledCleanups = 0;
    int numFinishedSetups = 0;
    int numFailedSetups = 0;
    int numKilledSetups = 0;
	
    long mapStarted = 0 ; 
    long mapFinished = 0 ; 
    long reduceStarted = 0 ; 
    long reduceFinished = 0;
    long cleanupStarted = 0;
    long cleanupFinished = 0; 
    long setupStarted = 0;
    long setupFinished = 0; 
        
    Map <String,String> allHosts = new TreeMap<String,String>();
    for (JobHistory.Task task : tasks.values()) {
      Map<String, TaskAttempt> attempts = task.getTaskAttempts();
      allHosts.put(task.get(Keys.HOSTNAME), "");
      for (TaskAttempt attempt : attempts.values()) {
        long startTime = attempt.getLong(Keys.START_TIME) ; 
        long finishTime = attempt.getLong(Keys.FINISH_TIME) ; 
        if (Values.MAP.name().equals(task.get(Keys.TASK_TYPE))){
          if (mapStarted==0 || mapStarted > startTime ) {
            mapStarted = startTime; 
          }
          if (mapFinished < finishTime ) {
            mapFinished = finishTime ; 
          }
          totalMaps++; 
          if (Values.FAILED.name().equals(attempt.get(Keys.TASK_STATUS))) {
            numFailedMaps++; 
          } else if (Values.KILLED.name().equals(attempt.get(Keys.TASK_STATUS))) {
            numKilledMaps++;
          }
        } else if (Values.REDUCE.name().equals(task.get(Keys.TASK_TYPE))) {
          if (reduceStarted==0||reduceStarted > startTime) {
            reduceStarted = startTime ; 
          }
          if (reduceFinished < finishTime) {
            reduceFinished = finishTime; 
          }
          totalReduces++; 
          if (Values.FAILED.name().equals(attempt.get(Keys.TASK_STATUS))) {
            numFailedReduces++;
          } else if (Values.KILLED.name().equals(attempt.get(Keys.TASK_STATUS))) {
            numKilledReduces++;
          }
        } else if (Values.CLEANUP.name().equals(task.get(Keys.TASK_TYPE))) {
          if (cleanupStarted==0||cleanupStarted > startTime) {
            cleanupStarted = startTime ; 
          }
          if (cleanupFinished < finishTime) {
            cleanupFinished = finishTime; 
          }
          totalCleanups++; 
          if (Values.SUCCESS.name().equals(attempt.get(Keys.TASK_STATUS))) {
            numFinishedCleanups++;
          } else if (Values.FAILED.name().equals(attempt.get(Keys.TASK_STATUS))) {
            numFailedCleanups++;
          } else if (Values.KILLED.name().equals(attempt.get(Keys.TASK_STATUS))) {
            numKilledCleanups++;
          } 
        } else if (Values.SETUP.name().equals(task.get(Keys.TASK_TYPE))) {
          if (setupStarted==0||setupStarted > startTime) {
            setupStarted = startTime ; 
          }
          if (setupFinished < finishTime) {
            setupFinished = finishTime; 
          }
          totalSetups++; 
          if (Values.SUCCESS.name().equals(attempt.get(Keys.TASK_STATUS))) {
            numFinishedSetups++;
          } else if (Values.FAILED.name().equals(attempt.get(Keys.TASK_STATUS))) {
            numFailedSetups++;
          } else if (Values.KILLED.name().equals(attempt.get(Keys.TASK_STATUS))) {
            numKilledSetups++;
          }
        }
      }
    }

      out.write("\n<b><a href=\"analysejobhistory.jsp?jobid=");
      out.print(jobid );
      out.write("&logFile=");
      out.print(encodedLogFileName);
      out.write("\">Analyse This Job</a></b> \n<hr/>\n<center>\n<table border=\"2\" cellpadding=\"5\" cellspacing=\"2\">\n<tr>\n<td>Kind</td><td>Total Tasks(successful+failed+killed)</td><td>Successful tasks</td><td>Failed tasks</td><td>Killed tasks</td><td>Start Time</td><td>Finish Time</td>\n</tr>\n<tr>\n<td>Setup</td>\n    <td><a href=\"jobtaskshistory.jsp?jobid=");
      out.print(jobid );
      out.write("&logFile=");
      out.print(encodedLogFileName);
      out.write("&taskType=");
      out.print(Values.SETUP.name() );
      out.write("&status=all\">\n        ");
      out.print(totalSetups);
      out.write("</a></td>\n    <td><a href=\"jobtaskshistory.jsp?jobid=");
      out.print(jobid );
      out.write("&logFile=");
      out.print(encodedLogFileName);
      out.write("&taskType=");
      out.print(Values.SETUP.name() );
      out.write("&status=");
      out.print(Values.SUCCESS );
      out.write("\">\n        ");
      out.print(numFinishedSetups);
      out.write("</a></td>\n    <td><a href=\"jobtaskshistory.jsp?jobid=");
      out.print(jobid );
      out.write("&logFile=");
      out.print(encodedLogFileName);
      out.write("&taskType=");
      out.print(Values.SETUP.name() );
      out.write("&status=");
      out.print(Values.FAILED );
      out.write("\">\n        ");
      out.print(numFailedSetups);
      out.write("</a></td>\n    <td><a href=\"jobtaskshistory.jsp?jobid=");
      out.print(jobid );
      out.write("&logFile=");
      out.print(encodedLogFileName);
      out.write("&taskType=");
      out.print(Values.SETUP.name() );
      out.write("&status=");
      out.print(Values.KILLED );
      out.write("\">\n        ");
      out.print(numKilledSetups);
      out.write("</a></td>  \n    <td>");
      out.print(StringUtils.getFormattedTimeWithDiff(dateFormat, setupStarted, 0) );
      out.write("</td>\n    <td>");
      out.print(StringUtils.getFormattedTimeWithDiff(dateFormat, setupFinished, setupStarted) );
      out.write("</td>\n</tr>\n<tr>\n<td>Map</td>\n    <td><a href=\"jobtaskshistory.jsp?jobid=");
      out.print(jobid );
      out.write("&logFile=");
      out.print(encodedLogFileName);
      out.write("&taskType=");
      out.print(Values.MAP.name() );
      out.write("&status=all\">\n        ");
      out.print(totalMaps );
      out.write("</a></td>\n    <td><a href=\"jobtaskshistory.jsp?jobid=");
      out.print(jobid );
      out.write("&logFile=");
      out.print(encodedLogFileName);
      out.write("&taskType=");
      out.print(Values.MAP.name() );
      out.write("&status=");
      out.print(Values.SUCCESS );
      out.write("\">\n        ");
      out.print(job.getInt(Keys.FINISHED_MAPS) );
      out.write("</a></td>\n    <td><a href=\"jobtaskshistory.jsp?jobid=");
      out.print(jobid );
      out.write("&logFile=");
      out.print(encodedLogFileName);
      out.write("&taskType=");
      out.print(Values.MAP.name() );
      out.write("&status=");
      out.print(Values.FAILED );
      out.write("\">\n        ");
      out.print(numFailedMaps );
      out.write("</a></td>\n    <td><a href=\"jobtaskshistory.jsp?jobid=");
      out.print(jobid );
      out.write("&logFile=");
      out.print(encodedLogFileName);
      out.write("&taskType=");
      out.print(Values.MAP.name() );
      out.write("&status=");
      out.print(Values.KILLED );
      out.write("\">\n        ");
      out.print(numKilledMaps );
      out.write("</a></td>\n    <td>");
      out.print(StringUtils.getFormattedTimeWithDiff(dateFormat, mapStarted, 0) );
      out.write("</td>\n    <td>");
      out.print(StringUtils.getFormattedTimeWithDiff(dateFormat, mapFinished, mapStarted) );
      out.write("</td>\n</tr>\n<tr>\n<td>Reduce</td>\n    <td><a href=\"jobtaskshistory.jsp?jobid=");
      out.print(jobid );
      out.write("&logFile=");
      out.print(encodedLogFileName);
      out.write("&taskType=");
      out.print(Values.REDUCE.name() );
      out.write("&status=all\">\n        ");
      out.print(totalReduces);
      out.write("</a></td>\n    <td><a href=\"jobtaskshistory.jsp?jobid=");
      out.print(jobid );
      out.write("&logFile=");
      out.print(encodedLogFileName);
      out.write("&taskType=");
      out.print(Values.REDUCE.name() );
      out.write("&status=");
      out.print(Values.SUCCESS );
      out.write("\">\n        ");
      out.print(job.getInt(Keys.FINISHED_REDUCES));
      out.write("</a></td>\n    <td><a href=\"jobtaskshistory.jsp?jobid=");
      out.print(jobid );
      out.write("&logFile=");
      out.print(encodedLogFileName);
      out.write("&taskType=");
      out.print(Values.REDUCE.name() );
      out.write("&status=");
      out.print(Values.FAILED );
      out.write("\">\n        ");
      out.print(numFailedReduces);
      out.write("</a></td>\n    <td><a href=\"jobtaskshistory.jsp?jobid=");
      out.print(jobid );
      out.write("&logFile=");
      out.print(encodedLogFileName);
      out.write("&taskType=");
      out.print(Values.REDUCE.name() );
      out.write("&status=");
      out.print(Values.KILLED );
      out.write("\">\n        ");
      out.print(numKilledReduces);
      out.write("</a></td>  \n    <td>");
      out.print(StringUtils.getFormattedTimeWithDiff(dateFormat, reduceStarted, 0) );
      out.write("</td>\n    <td>");
      out.print(StringUtils.getFormattedTimeWithDiff(dateFormat, reduceFinished, reduceStarted) );
      out.write("</td>\n</tr>\n<tr>\n<td>Cleanup</td>\n    <td><a href=\"jobtaskshistory.jsp?jobid=");
      out.print(jobid );
      out.write("&logFile=");
      out.print(encodedLogFileName);
      out.write("&taskType=");
      out.print(Values.CLEANUP.name() );
      out.write("&status=all\">\n        ");
      out.print(totalCleanups);
      out.write("</a></td>\n    <td><a href=\"jobtaskshistory.jsp?jobid=");
      out.print(jobid );
      out.write("&logFile=");
      out.print(encodedLogFileName);
      out.write("&taskType=");
      out.print(Values.CLEANUP.name() );
      out.write("&status=");
      out.print(Values.SUCCESS );
      out.write("\">\n        ");
      out.print(numFinishedCleanups);
      out.write("</a></td>\n    <td><a href=\"jobtaskshistory.jsp?jobid=");
      out.print(jobid );
      out.write("&logFile=");
      out.print(encodedLogFileName);
      out.write("&taskType=");
      out.print(Values.CLEANUP.name() );
      out.write("&status=");
      out.print(Values.FAILED );
      out.write("\">\n        ");
      out.print(numFailedCleanups);
      out.write("</a></td>\n    <td><a href=\"jobtaskshistory.jsp?jobid=");
      out.print(jobid );
      out.write("&logFile=");
      out.print(encodedLogFileName);
      out.write("&taskType=");
      out.print(Values.CLEANUP.name() );
      out.write("&status=");
      out.print(Values.KILLED );
      out.write("\">\n        ");
      out.print(numKilledCleanups);
      out.write("</a></td>  \n    <td>");
      out.print(StringUtils.getFormattedTimeWithDiff(dateFormat, cleanupStarted, 0) );
      out.write("</td>\n    <td>");
      out.print(StringUtils.getFormattedTimeWithDiff(dateFormat, cleanupFinished, cleanupStarted) );
      out.write("</td>\n</tr>\n</table>\n\n<br>\n<br>\n\n<table border=2 cellpadding=\"5\" cellspacing=\"2\">\n  <tr>\n  <th><br/></th>\n  <th>Counter</th>\n  <th>Map</th>\n  <th>Reduce</th>\n  <th>Total</th>\n</tr>\n\n");
  

 Counters totalCounters = 
   Counters.fromEscapedCompactString(job.get(Keys.COUNTERS));
 Counters mapCounters = 
   Counters.fromEscapedCompactString(job.get(Keys.MAP_COUNTERS));
 Counters reduceCounters = 
   Counters.fromEscapedCompactString(job.get(Keys.REDUCE_COUNTERS));

 if (totalCounters != null) {
   for (String groupName : totalCounters.getGroupNames()) {
     Counters.Group totalGroup = totalCounters.getGroup(groupName);
     Counters.Group mapGroup = mapCounters.getGroup(groupName);
     Counters.Group reduceGroup = reduceCounters.getGroup(groupName);
  
     Format decimal = new DecimalFormat();
  
     boolean isFirst = true;
     Iterator<Counters.Counter> ctrItr = totalGroup.iterator();
     while(ctrItr.hasNext()) {
       Counters.Counter counter = ctrItr.next();
       String name = counter.getDisplayName();
       String mapValue = 
         decimal.format(mapGroup.getCounter(name));
       String reduceValue = 
         decimal.format(reduceGroup.getCounter(name));
       String totalValue = decimal.format(counter.getCounter());

      out.write("\n       <tr>\n");

       if (isFirst) {
         isFirst = false;

      out.write("\n         <td rowspan=\"");
      out.print(totalGroup.size());
      out.write('"');
      out.write('>');
      out.print(totalGroup.getDisplayName());
      out.write("</td>\n");

       }

      out.write("\n       <td>");
      out.print(counter.getDisplayName());
      out.write("</td>\n       <td align=\"right\">");
      out.print(mapValue);
      out.write("</td>\n       <td align=\"right\">");
      out.print(reduceValue);
      out.write("</td>\n       <td align=\"right\">");
      out.print(totalValue);
      out.write("</td>\n     </tr>\n");

      }
    }
  }

      out.write("\n</table>\n<br>\n\n<br/>\n ");

    DefaultJobHistoryParser.FailedOnNodesFilter filter = 
                 new DefaultJobHistoryParser.FailedOnNodesFilter();
    JobHistory.parseHistoryFromFS(logFile, filter, fs); 
    Map<String, Set<String>> badNodes = filter.getValues(); 
    if (badNodes.size() > 0) {
 
      out.write("\n<h3>Failed tasks attempts by nodes </h3>\n<table border=\"1\">\n<tr><td>Hostname</td><td>Failed Tasks</td></tr>\n ");
	  
      for (Map.Entry<String, Set<String>> entry : badNodes.entrySet()) {
        String node = entry.getKey();
        Set<String> failedTasks = entry.getValue();

      out.write("\n        <tr>\n        <td>");
      out.print(node );
      out.write("</td>\n        <td>\n");

        for (String t : failedTasks) {

      out.write("\n          <a href=\"taskdetailshistory.jsp?jobid=");
      out.print(jobid);
      out.write("&logFile=");
      out.print(encodedLogFileName);
      out.write("&taskid=");
      out.print(t );
      out.write('"');
      out.write('>');
      out.print(t );
      out.write("</a>,&nbsp;\n");
		  
        }

      out.write("\t\n        </td>\n        </tr>\n");
	  
      }
	}
 
      out.write("\n</table>\n<br/>\n\n ");

    DefaultJobHistoryParser.KilledOnNodesFilter killedFilter =
                 new DefaultJobHistoryParser.KilledOnNodesFilter();
    JobHistory.parseHistoryFromFS(logFile, filter, fs); 
    badNodes = killedFilter.getValues(); 
    if (badNodes.size() > 0) {
 
      out.write("\n<h3>Killed tasks attempts by nodes </h3>\n<table border=\"1\">\n<tr><td>Hostname</td><td>Killed Tasks</td></tr>\n ");
	  
      for (Map.Entry<String, Set<String>> entry : badNodes.entrySet()) {
        String node = entry.getKey();
        Set<String> killedTasks = entry.getValue();

      out.write("\n        <tr>\n        <td>");
      out.print(node );
      out.write("</td>\n        <td>\n");

        for (String t : killedTasks) {

      out.write("\n          <a href=\"taskdetailshistory.jsp?jobid=");
      out.print(jobid);
      out.write("&logFile=");
      out.print(encodedLogFileName);
      out.write("&taskid=");
      out.print(t );
      out.write('"');
      out.write('>');
      out.print(t );
      out.write("</a>,&nbsp;\n");
		  
        }

      out.write("\t\n        </td>\n        </tr>\n");
	  
      }
    }

      out.write("\n</table>\n</center>\n</body></html>\n");
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
