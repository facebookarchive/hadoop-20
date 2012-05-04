package org.apache.hadoop.mapred;

import javax.servlet.*;
import javax.servlet.http.*;
import javax.servlet.jsp.*;
import javax.servlet.http.*;
import java.io.*;
import java.util.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.util.*;
import java.text.SimpleDateFormat;
import org.apache.hadoop.mapred.JobHistory.*;

public final class analysejobhistory_jsp extends org.apache.jasper.runtime.HttpJspBase
    implements org.apache.jasper.runtime.JspSourceDependent {

	private static SimpleDateFormat dateFormat 
                              = new SimpleDateFormat("d/MM HH:mm:ss") ; 

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
      out.write("\n<html><body>\n");

  String jobid = request.getParameter("jobid");
  String logFile = request.getParameter("logFile");
  String encodedLogFileName = JobHistory.JobInfo.encodeJobHistoryFilePath(logFile);
  String numTasks = request.getParameter("numTasks");
  int showTasks = 10 ; 
  if (numTasks != null) {
    showTasks = Integer.parseInt(numTasks);  
  }
  FileSystem fs = (FileSystem) application.getAttribute("fileSys");
  JobInfo job = JSPUtil.getJobInfo(request, fs);

      out.write("\n<h2>Hadoop Job <a href=\"jobdetailshistory.jsp?jobid=");
      out.print(jobid);
      out.write("&&logFile=");
      out.print(encodedLogFileName);
      out.write('"');
      out.write('>');
      out.print(jobid );
      out.write(" </a></h2>\n<b>User : </b> ");
      out.print(job.get(Keys.USER) );
      out.write("<br/> \n<b>JobName : </b> ");
      out.print(job.get(Keys.JOBNAME) );
      out.write("<br/> \n<b>JobConf : </b> ");
      out.print(job.get(Keys.JOBCONF) );
      out.write("<br/> \n<b>Submitted At : </b> ");
      out.print(StringUtils.getFormattedTimeWithDiff(dateFormat, job.getLong(Keys.SUBMIT_TIME), 0 ) );
      out.write("<br/> \n<b>Launched At : </b> ");
      out.print(StringUtils.getFormattedTimeWithDiff(dateFormat, job.getLong(Keys.LAUNCH_TIME), job.getLong(Keys.SUBMIT_TIME)) );
      out.write("<br/>\n<b>Finished At : </b>  ");
      out.print(StringUtils.getFormattedTimeWithDiff(dateFormat, job.getLong(Keys.FINISH_TIME), job.getLong(Keys.LAUNCH_TIME)) );
      out.write("<br/>\n<b>Status : </b> ");
      out.print( ((job.get(Keys.JOB_STATUS) == null)?"Incomplete" :job.get(Keys.JOB_STATUS)) );
      out.write("<br/> \n<hr/>\n<center>\n");

  if (!Values.SUCCESS.name().equals(job.get(Keys.JOB_STATUS))) {
    out.print("<h3>No Analysis available as job did not finish</h3>");
    return;
  }
  Map<String, JobHistory.Task> tasks = job.getAllTasks();
  int finishedMaps = job.getInt(Keys.FINISHED_MAPS)  ;
  int finishedReduces = job.getInt(Keys.FINISHED_REDUCES) ;
  JobHistory.Task [] mapTasks = new JobHistory.Task[finishedMaps]; 
  JobHistory.Task [] reduceTasks = new JobHistory.Task[finishedReduces]; 
  int mapIndex = 0 , reduceIndex=0; 
  long avgMapTime = 0;
  long avgReduceTime = 0;
  long avgShuffleTime = 0;

  for (JobHistory.Task task : tasks.values()) {
    Map<String, TaskAttempt> attempts = task.getTaskAttempts();
    for (JobHistory.TaskAttempt attempt : attempts.values()) {
      if (attempt.get(Keys.TASK_STATUS).equals(Values.SUCCESS.name())) {
        long avgFinishTime = (attempt.getLong(Keys.FINISH_TIME) -
      		                attempt.getLong(Keys.START_TIME));
        if (Values.MAP.name().equals(task.get(Keys.TASK_TYPE))) {
          mapTasks[mapIndex++] = attempt ; 
          avgMapTime += avgFinishTime;
        } else if (Values.REDUCE.name().equals(task.get(Keys.TASK_TYPE))) { 
          reduceTasks[reduceIndex++] = attempt;
          avgShuffleTime += (attempt.getLong(Keys.SHUFFLE_FINISHED) - 
                             attempt.getLong(Keys.START_TIME));
          avgReduceTime += (attempt.getLong(Keys.FINISH_TIME) -
                            attempt.getLong(Keys.SHUFFLE_FINISHED));
        }
        break;
      }
    }
  }
	 
  if (finishedMaps > 0) {
    avgMapTime /= finishedMaps;
  }
  if (finishedReduces > 0) {
    avgReduceTime /= finishedReduces;
    avgShuffleTime /= finishedReduces;
  }
  Comparator<JobHistory.Task> cMap = new Comparator<JobHistory.Task>(){
    public int compare(JobHistory.Task t1, JobHistory.Task t2){
      long l1 = t1.getLong(Keys.FINISH_TIME) - t1.getLong(Keys.START_TIME); 
      long l2 = t2.getLong(Keys.FINISH_TIME) - t2.getLong(Keys.START_TIME);
      return (l2<l1 ? -1 : (l2==l1 ? 0 : 1));
    }
  }; 
  Comparator<JobHistory.Task> cShuffle = new Comparator<JobHistory.Task>(){
    public int compare(JobHistory.Task t1, JobHistory.Task t2){
      long l1 = t1.getLong(Keys.SHUFFLE_FINISHED) - 
                t1.getLong(Keys.START_TIME); 
      long l2 = t2.getLong(Keys.SHUFFLE_FINISHED) - 
                t2.getLong(Keys.START_TIME); 
      return (l2<l1 ? -1 : (l2==l1 ? 0 : 1));
    }
  }; 
  Arrays.sort(mapTasks, cMap);
  JobHistory.Task minMap = mapTasks[mapTasks.length-1] ;

      out.write("\n\n<h3>Time taken by best performing Map task \n<a href=\"taskdetailshistory.jsp?jobid=");
      out.print(jobid);
      out.write("&logFile=");
      out.print(encodedLogFileName);
      out.write("&taskid=");
      out.print(minMap.get(Keys.TASKID));
      out.write("\">\n");
      out.print(minMap.get(Keys.TASKID) );
      out.write("</a> : ");
      out.print(StringUtils.formatTimeDiff(minMap.getLong(Keys.FINISH_TIME), minMap.getLong(Keys.START_TIME) ) );
      out.write("</h3>\n<h3>Average time taken by Map tasks: \n");
      out.print(StringUtils.formatTimeDiff(avgMapTime, 0) );
      out.write("</h3>\n<h3>Worse performing map tasks</h3>\n<table border=\"2\" cellpadding=\"5\" cellspacing=\"2\">\n<tr><td>Task Id</td><td>Time taken</td></tr>\n");

  for (int i=0;i<showTasks && i<mapTasks.length; i++) {

      out.write("\n    <tr>\n    <td><a href=\"taskdetailshistory.jsp?jobid=");
      out.print(jobid);
      out.write("&logFile=");
      out.print(encodedLogFileName);
      out.write("&taskid=");
      out.print(mapTasks[i].get(Keys.TASKID));
      out.write("\">\n        ");
      out.print(mapTasks[i].get(Keys.TASKID) );
      out.write("</a></td>\n    <td>");
      out.print(StringUtils.formatTimeDiff(mapTasks[i].getLong(Keys.FINISH_TIME), mapTasks[i].getLong(Keys.START_TIME)) );
      out.write("</td>\n    </tr>\n");

  }

      out.write("\n</table>\n");
  
  Comparator<JobHistory.Task> cFinishMapRed = 
    new Comparator<JobHistory.Task>() {
    public int compare(JobHistory.Task t1, JobHistory.Task t2){
      long l1 = t1.getLong(Keys.FINISH_TIME); 
      long l2 = t2.getLong(Keys.FINISH_TIME);
      return (l2<l1 ? -1 : (l2==l1 ? 0 : 1));
    }
  };
  Arrays.sort(mapTasks, cFinishMapRed);
  JobHistory.Task lastMap = mapTasks[0] ;

      out.write("\n\n<h3>The last Map task \n<a href=\"taskdetailshistory.jsp?jobid=");
      out.print(jobid);
      out.write("&logFile=");
      out.print(encodedLogFileName);
      out.write("\n&taskid=");
      out.print(lastMap.get(Keys.TASKID));
      out.write('"');
      out.write('>');
      out.print(lastMap.get(Keys.TASKID) );
      out.write("</a> \nfinished at (relative to the Job launch time): \n");
      out.print(StringUtils.getFormattedTimeWithDiff(dateFormat, 
                              lastMap.getLong(Keys.FINISH_TIME), 
                              job.getLong(Keys.LAUNCH_TIME) ) );
      out.write("</h3>\n<hr/>\n\n");

  if (reduceTasks.length <= 0) return;
  Arrays.sort(reduceTasks, cShuffle); 
  JobHistory.Task minShuffle = reduceTasks[reduceTasks.length-1] ;

      out.write("\n<h3>Time taken by best performing shuffle\n<a href=\"taskdetailshistory.jsp?jobid=");
      out.print(jobid);
      out.write("&logFile=");
      out.print(encodedLogFileName);
      out.write("\n&taskid=");
      out.print(minShuffle.get(Keys.TASKID));
      out.write('"');
      out.write('>');
      out.print(minShuffle.get(Keys.TASKID));
      out.write("</a> : \n");
      out.print(StringUtils.formatTimeDiff(minShuffle.getLong(Keys.SHUFFLE_FINISHED), 
                              minShuffle.getLong(Keys.START_TIME) ) );
      out.write("</h3>\n<h3>Average time taken by Shuffle: \n");
      out.print(StringUtils.formatTimeDiff(avgShuffleTime, 0) );
      out.write("</h3>\n<h3>Worse performing Shuffle(s)</h3>\n<table border=\"2\" cellpadding=\"5\" cellspacing=\"2\">\n<tr><td>Task Id</td><td>Time taken</td></tr>\n");

  for (int i=0;i<showTasks && i<reduceTasks.length; i++) {

      out.write("\n    <tr>\n    <td><a href=\"taskdetailshistory.jsp?jobid=");
      out.print(jobid);
      out.write("&logFile=\n");
      out.print(encodedLogFileName);
      out.write("&taskid=");
      out.print(reduceTasks[i].get(Keys.TASKID));
      out.write("\">\n");
      out.print(reduceTasks[i].get(Keys.TASKID) );
      out.write("</a></td>\n    <td>");
      out.print(
           StringUtils.formatTimeDiff(
                       reduceTasks[i].getLong(Keys.SHUFFLE_FINISHED),
                       reduceTasks[i].getLong(Keys.START_TIME)) );
      out.write("\n    </td>\n    </tr>\n");

  }

      out.write("\n</table>\n");
  
  Comparator<JobHistory.Task> cFinishShuffle = 
    new Comparator<JobHistory.Task>() {
    public int compare(JobHistory.Task t1, JobHistory.Task t2){
      long l1 = t1.getLong(Keys.SHUFFLE_FINISHED); 
      long l2 = t2.getLong(Keys.SHUFFLE_FINISHED);
      return (l2<l1 ? -1 : (l2==l1 ? 0 : 1));
    }
  };
  Arrays.sort(reduceTasks, cFinishShuffle);
  JobHistory.Task lastShuffle = reduceTasks[0] ;

      out.write("\n\n<h3>The last Shuffle  \n<a href=\"taskdetailshistory.jsp?jobid=");
      out.print(jobid);
      out.write("&logFile=");
      out.print(encodedLogFileName);
      out.write("\n&taskid=");
      out.print(lastShuffle.get(Keys.TASKID));
      out.write('"');
      out.write('>');
      out.print(lastShuffle.get(Keys.TASKID));
      out.write("\n</a> finished at (relative to the Job launch time): \n");
      out.print(StringUtils.getFormattedTimeWithDiff(dateFormat,
                              lastShuffle.getLong(Keys.SHUFFLE_FINISHED), 
                              job.getLong(Keys.LAUNCH_TIME) ) );
      out.write("</h3>\n\n");

  Comparator<JobHistory.Task> cReduce = new Comparator<JobHistory.Task>(){
    public int compare(JobHistory.Task t1, JobHistory.Task t2){
      long l1 = t1.getLong(Keys.FINISH_TIME) - 
                t1.getLong(Keys.SHUFFLE_FINISHED); 
      long l2 = t2.getLong(Keys.FINISH_TIME) - 
                t2.getLong(Keys.SHUFFLE_FINISHED);
      return (l2<l1 ? -1 : (l2==l1 ? 0 : 1));
    }
  }; 
  Arrays.sort(reduceTasks, cReduce); 
  JobHistory.Task minReduce = reduceTasks[reduceTasks.length-1] ;

      out.write("\n<hr/>\n<h3>Time taken by best performing Reduce task : \n<a href=\"taskdetailshistory.jsp?jobid=");
      out.print(jobid);
      out.write("&logFile=");
      out.print(encodedLogFileName);
      out.write("&taskid=");
      out.print(minReduce.get(Keys.TASKID));
      out.write("\">\n");
      out.print(minReduce.get(Keys.TASKID) );
      out.write("</a> : \n");
      out.print(StringUtils.formatTimeDiff(minReduce.getLong(Keys.FINISH_TIME),
    minReduce.getLong(Keys.SHUFFLE_FINISHED) ) );
      out.write("</h3>\n\n<h3>Average time taken by Reduce tasks: \n");
      out.print(StringUtils.formatTimeDiff(avgReduceTime, 0) );
      out.write("</h3>\n<h3>Worse performing reduce tasks</h3>\n<table border=\"2\" cellpadding=\"5\" cellspacing=\"2\">\n<tr><td>Task Id</td><td>Time taken</td></tr>\n");

  for (int i=0;i<showTasks && i<reduceTasks.length; i++) {

      out.write("\n    <tr>\n    <td><a href=\"taskdetailshistory.jsp?jobid=");
      out.print(jobid);
      out.write("&logFile=");
      out.print(encodedLogFileName);
      out.write("&taskid=");
      out.print(reduceTasks[i].get(Keys.TASKID));
      out.write("\">\n        ");
      out.print(reduceTasks[i].get(Keys.TASKID) );
      out.write("</a></td>\n    <td>");
      out.print(StringUtils.formatTimeDiff(
             reduceTasks[i].getLong(Keys.FINISH_TIME), 
             reduceTasks[i].getLong(Keys.SHUFFLE_FINISHED)) );
      out.write("</td>\n    </tr>\n");

  }

      out.write("\n</table>\n");
  
  Arrays.sort(reduceTasks, cFinishMapRed);
  JobHistory.Task lastReduce = reduceTasks[0] ;

      out.write("\n\n<h3>The last Reduce task \n<a href=\"taskdetailshistory.jsp?jobid=");
      out.print(jobid);
      out.write("&logFile=");
      out.print(encodedLogFileName);
      out.write("\n&taskid=");
      out.print(lastReduce.get(Keys.TASKID));
      out.write('"');
      out.write('>');
      out.print(lastReduce.get(Keys.TASKID));
      out.write("\n</a> finished at (relative to the Job launch time): \n");
      out.print(StringUtils.getFormattedTimeWithDiff(dateFormat,
                              lastReduce.getLong(Keys.FINISH_TIME), 
                              job.getLong(Keys.LAUNCH_TIME) ) );
      out.write("</h3>\n</center>\n</body></html>\n");
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
