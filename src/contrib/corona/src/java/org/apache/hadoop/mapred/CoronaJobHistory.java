/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.mapred;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.mapred.JobHistory.Keys;
import org.apache.hadoop.mapred.JobHistory.LogTask;
import org.apache.hadoop.mapred.JobHistory.MovedFileInfo;
import org.apache.hadoop.mapred.JobHistory.RecordTypes;
import org.apache.hadoop.mapred.JobHistory.Values;
import org.apache.hadoop.util.StringUtils;

/*
 * Restructured JobHistory code for use by Corona
 * - Supports multiple instances at the same time
 * - Produces history files in same format as JobHistory v1
 *
 * Please JobHistory to actually parse/view the history files
 */

public class CoronaJobHistory {

  public static final Log LOG = LogFactory.getLog(CoronaJobHistory.class);

  static final FsPermission HISTORY_DIR_PERMISSION =
    FsPermission.createImmutable((short) 0755); // rwxr-x---
  static final FsPermission HISTORY_FILE_PERMISSION =
    FsPermission.createImmutable((short) 0744); // rwxr-----

  Path logDir;
  FileSystem logDirFs;
  Path doneDir;
  FileSystem doneDirFs;
  Path logFile;
  Path doneFile;
  JobConf conf;
  boolean disableHistory = true;
  long jobHistoryBlockSize = 0;
  CoronaJobHistoryFilesManager fileManager = null;
  JobID jobId;
  ArrayList<PrintWriter> writers = null;

  public CoronaJobHistory(JobConf conf, JobID jobId, String logPath) {
    try {
      this.conf = conf;
      this.jobId = jobId;
      if (logPath == null) {
        logPath = "file:///" + new File
          (System.getProperty("hadoop.log.dir", "/tmp")).getAbsolutePath() +
          File.separator + "history";
      }
      logDir = new Path(logPath);
      logDirFs = logDir.getFileSystem(conf);

      if (!logDirFs.exists(logDir)) {
        LOG.info("Creating history folder at " + logDir);
        if (!logDirFs.mkdirs(logDir, 
            new FsPermission(HISTORY_DIR_PERMISSION))) {
          throw new IOException("Mkdirs failed to create " + logDir.toString());
        }
      }
      conf.set("hadoop.job.history.location", logDir.toString());
      disableHistory = false;

      // set the job history block size (default is 3MB)
      jobHistoryBlockSize =
        conf.getLong("mapred.jobtracker.job.history.block.size",
                     3 * 1024 * 1024);

      doneDir = new Path(logDir, "done");
      doneDirFs = logDirFs;


      if (!doneDirFs.exists(doneDir)) {
        LOG.info("Creating DONE folder at " + doneDir);
        if (! doneDirFs.mkdirs(doneDir,
                               new FsPermission(HISTORY_DIR_PERMISSION))) {
          throw new IOException("Mkdirs failed to create " + doneDir);
        }
      }

      String logFileName = encodeJobHistoryFileName(
          CoronaJobHistoryFilesManager.getHistoryFilename(jobId));
      logFile = new Path(logDir, logFileName);
      doneFile = new Path(doneDir, logFileName);


      // initialize the file manager
      conf.setInt("mapred.jobtracker.historythreads.maximum", 1);
      fileManager = new CoronaJobHistoryFilesManager(conf,
          new JobHistoryObserver() {
            public void historyFileCopied(JobID jobid, String historyFile) {

            }
          }, logDir);

      fileManager.setDoneDir(doneDir);

      // sleeping with the past means tolerating two start methods instead of one
      fileManager.start();
      fileManager.startIOExecutor();

    } catch (IOException e) {
      LOG.error("Failed to initialize JobHistory log file", e);
      disableHistory = true;
    }
  }

  public void shutdown() {
    if (fileManager != null) {
      fileManager.shutdown();
    }
  }

  public boolean isDisabled() {
    return disableHistory;
  }

  public static String encodeJobHistoryFileName(String logFileName)
    throws IOException {
    return JobHistory.JobInfo.encodeJobHistoryFileName(logFileName);
  }

  public String getCompletedJobHistoryPath() {
    return doneFile.toString();
  }

  /**
   * Get the history location for completed jobs
   */
  public Path getCompletedJobHistoryLocation() {
    return doneDir;
  }

  public static String encodeJobHistoryFilePath(String logFile)
    throws IOException {
    return JobHistory.JobInfo.encodeJobHistoryFilePath(logFile);
  }

  private String getJobName() {
    String jobName = ((JobConf) conf).getJobName();
    if (jobName == null || jobName.length() == 0) {
      jobName = "NA";
    }
    return jobName;
  }

  public String getUserName() {
    String user = ((JobConf) conf).getUser();
    if (user == null || user.length() == 0) {
      user = "NA";
    }
    return user;
  }


  private static void closeAndClear(List<PrintWriter> writers) {
    for (PrintWriter out : writers) {
      out.close();
    }
    // By clearning the writers and notify the thread waiting on it, 
    // we will prevent JobHistory.moveToDone() from
    // waiting on writer
    synchronized (writers) {
      writers.clear();
      writers.notifyAll();
    }
  }

  /**
   * Log job submitted event to history. Creates a new file in history
   * for the job. if history file creation fails, it disables history
   * for all other events.
   * @param jobConfPath path to job conf xml file in HDFS.
   * @param submitTime time when job tracker received the job
   * @throws IOException
   */
  public void logSubmitted(String jobConfPath, long submitTime, String jobTrackerId)
    throws IOException {

    if (disableHistory) {
      return;
    }

    // create output stream for logging in hadoop.job.history.location
    int defaultBufferSize =
      logDirFs.getConf().getInt("io.file.buffer.size", 4096);

    try {
      FSDataOutputStream out = null;
      PrintWriter writer = null;
      
      // In case the old JT is still running, but we can't connect to it, we
      // should ensure that it won't write to our (new JT's) job history file.
      if (logDirFs.exists(logFile)) {
        LOG.info("Remove the old history file " + logFile);
        logDirFs.delete(logFile, true);
      }

      out = logDirFs.create(logFile,
                            new FsPermission(HISTORY_FILE_PERMISSION),
                            true,
                            defaultBufferSize,
                            logDirFs.getDefaultReplication(),
                            jobHistoryBlockSize, null);

      writer = new PrintWriter(out);

      fileManager.addWriter(jobId, writer);

      // cache it ...
      fileManager.setHistoryFile(jobId, logFile);

      writers = fileManager.getWriters(jobId);
      if (null != writers) {
        log(writers, RecordTypes.Meta,
            new Keys[] {Keys.VERSION},
            new String[] {String.valueOf(JobHistory.VERSION)});
      }

      String jobName = getJobName();
      String user = getUserName();

      //add to writer as well
      log(writers, RecordTypes.Job,
          new Keys[]{Keys.JOBID, Keys.JOBNAME, Keys.USER,
                       Keys.SUBMIT_TIME, Keys.JOBCONF, Keys.JOBTRACKERID },
          new String[]{jobId.toString(), jobName, user,
                       String.valueOf(submitTime) , jobConfPath, jobTrackerId}
          );

    } catch (IOException e) {
      // Disable history if we have errors other than in the user log.
      disableHistory = true;
    }

    /* Storing the job conf on the log dir */
    Path jobFilePath = new Path(logDir,
        CoronaJobHistoryFilesManager.getConfFilename(jobId));
    fileManager.setConfFile(jobId, jobFilePath);
    FSDataOutputStream jobFileOut = null;
    try {
      if (!logDirFs.exists(jobFilePath)) {
        jobFileOut = logDirFs.create(jobFilePath,
                                     new FsPermission(HISTORY_FILE_PERMISSION),
                                     true,
                                     defaultBufferSize,
                                     logDirFs.getDefaultReplication(),
                                     logDirFs.getDefaultBlockSize(), null);
        conf.writeXml(jobFileOut);
        jobFileOut.close();
      }
    } catch (IOException ioe) {
      LOG.error("Failed to store job conf in the log dir", ioe);
    } finally {
      if (jobFileOut != null) {
        try {
          jobFileOut.close();
        } catch (IOException ie) {
          LOG.info("Failed to close the job configuration file " +
              StringUtils.stringifyException(ie));
        }
      }
    }
  }

  /**
   * Logs launch time of job.
   *
   * @param startTime start time of job.
   * @param totalMaps total maps assigned by jobtracker.
   * @param totalReduces total reduces.
   */
  public void logInited(long startTime, int totalMaps, int totalReduces) {
    if (disableHistory) {
      return;
    }

    if (null != writers) {
      log(writers, RecordTypes.Job,
          new Keys[] {Keys.JOBID, Keys.LAUNCH_TIME, Keys.TOTAL_MAPS,
                      Keys.TOTAL_REDUCES, Keys.JOB_STATUS},
          new String[] {jobId.toString(), String.valueOf(startTime),
                        String.valueOf(totalMaps),
                        String.valueOf(totalReduces),
                        Values.PREP.name()});
    }
  }

  /**
   * Logs job as running
   */
  public void logStarted() {
    if (disableHistory) {
      return;
    }

    if (null != writers) {
      log(writers, RecordTypes.Job,
          new Keys[] {Keys.JOBID, Keys.JOB_STATUS},
          new String[] {jobId.toString(),
                        Values.RUNNING.name()});
    }
  }

  /**
   * Log job finished. closes the job file in history.
   * @param finishTime finish time of job in ms.
   * @param finishedMaps no of maps successfully finished.
   * @param finishedReduces no of reduces finished sucessfully.
   * @param failedMaps no of failed map tasks. (includes killed)
   * @param failedReduces no of failed reduce tasks. (includes killed)
   * @param killedMaps no of killed map tasks.
   * @param killedReduces no of killed reduce tasks.
   * @param counters the counters from the job
   */
  public void logFinished(long finishTime,
                          int finishedMaps, int finishedReduces,
                          int failedMaps, int failedReduces,
                          int killedMaps, int killedReduces,
                          Counters mapCounters,
                          Counters reduceCounters,
                          Counters counters) {
    if (disableHistory) {
      return;
    }

    if (null != writers) {
      log(writers, RecordTypes.Job,
          new Keys[] {Keys.JOBID, Keys.FINISH_TIME,
                      Keys.JOB_STATUS, Keys.FINISHED_MAPS,
                      Keys.FINISHED_REDUCES,
                      Keys.FAILED_MAPS, Keys.FAILED_REDUCES,
                      Keys.KILLED_MAPS, Keys.KILLED_REDUCES,
                      Keys.MAP_COUNTERS, Keys.REDUCE_COUNTERS,
                      Keys.COUNTERS},
          new String[] {jobId.toString(),  Long.toString(finishTime),
                        Values.SUCCESS.name(),
                        String.valueOf(finishedMaps),
                        String.valueOf(finishedReduces),
                        String.valueOf(failedMaps),
                        String.valueOf(failedReduces),
                        String.valueOf(killedMaps),
                        String.valueOf(killedReduces),
                        mapCounters.makeEscapedCompactString(),
                        reduceCounters.makeEscapedCompactString(),
                        counters.makeEscapedCompactString()},
                        true);

      closeAndClear(writers);
    }

    // NOTE: history cleaning stuff deleted from here. We should do that
    // somewhere else!
  }

  /**
   * Logs job failed event. Closes the job history log file.
   * @param timestamp time when job failure was detected in ms.
   * @param finishedMaps no finished map tasks.
   * @param finishedReduces no of finished reduce tasks.
   */
  public void logFailed(long timestamp, int finishedMaps,
                        int finishedReduces, Counters counters) {
    if (disableHistory) {
      return;
    }

    if (null != writers) {
      log(writers, RecordTypes.Job,
          new Keys[] {Keys.JOBID, Keys.FINISH_TIME,
                      Keys.JOB_STATUS, Keys.FINISHED_MAPS,
                      Keys.FINISHED_REDUCES, Keys.COUNTERS},
          new String[] {jobId.toString(),
                        String.valueOf(timestamp),
                        Values.FAILED.name(),
                        String.valueOf(finishedMaps),
                        String.valueOf(finishedReduces),
                        counters.makeEscapedCompactString()},
                        true);
      closeAndClear(writers);
    }
  }


  /**
   * Logs job killed event. Closes the job history log file.
   *
   * @param timestamp
   *          time when job killed was issued in ms.
   * @param finishedMaps
   *          no finished map tasks.
   * @param finishedReduces
   *          no of finished reduce tasks.
   */
  public void logKilled(long timestamp, int finishedMaps,
                        int finishedReduces, Counters counters) {
    if (disableHistory) {
      return;
    }

    if (null != writers) {
      log(writers, RecordTypes.Job,
          new Keys[] {Keys.JOBID,
                      Keys.FINISH_TIME, Keys.JOB_STATUS, Keys.FINISHED_MAPS,
                      Keys.FINISHED_REDUCES, Keys.COUNTERS },
          new String[] {jobId.toString(),
                        String.valueOf(timestamp), Values.KILLED.name(),
                        String.valueOf(finishedMaps),
                        String.valueOf(finishedReduces),
                        counters.makeEscapedCompactString()},
                        true);
      closeAndClear(writers);
    }
  }

  /**
   * Log job's priority.
   * @param priority Jobs priority
   */
  public void logJobPriority(JobID jobid, JobPriority priority) {
    if (disableHistory) {
      return;
    }

    if (null != writers) {
      log(writers, RecordTypes.Job,
          new Keys[] {Keys.JOBID, Keys.JOB_PRIORITY},
          new String[] {jobId.toString(), priority.toString()});
    }
  }

  /**
   * Move the completed job into the completed folder.
   */
  public void markCompleted() throws IOException {
    if (disableHistory) {
      return;
    }
    fileManager.moveToDone(jobId, true, CoronaJobTracker.getMainJobID(jobId));
  }

  /**
   * Log start time of task (TIP).
   * @param taskId task id
   * @param taskType MAP or REDUCE
   * @param startTime startTime of tip.
   */
  public void logTaskStarted(TaskID taskId, String taskType,
                             long startTime, String splitLocations) {
    if (disableHistory) {
      return;
    }

    JobID id = taskId.getJobID();
    if (!this.jobId.equals(id)) {
      throw new RuntimeException("JobId from task: " + id +
                                 " does not match expected: " + jobId);
    }

    if (null != writers) {
      log(writers, RecordTypes.Task,
          new Keys[]{Keys.TASKID, Keys.TASK_TYPE ,
                     Keys.START_TIME, Keys.SPLITS},
          new String[]{taskId.toString(), taskType,
                       String.valueOf(startTime),
                       splitLocations});
    }
  }

  /**
   * Log finish time of task.
   * @param taskId task id
   * @param taskType MAP or REDUCE
   * @param finishTime finish timeof task in ms
   */
  public void logTaskFinished(TaskID taskId, String taskType,
                              long finishTime, Counters counters) {

    if (disableHistory) {
      return;
    }

    JobID id = taskId.getJobID();
    if (!this.jobId.equals(id)) {
      throw new RuntimeException("JobId from task: " + id +
          " does not match expected: " + jobId);
    }

    if (null != writers) {
      log(writers, RecordTypes.Task,
          new Keys[]{Keys.TASKID, Keys.TASK_TYPE,
                     Keys.TASK_STATUS, Keys.FINISH_TIME,
                     Keys.COUNTERS},
          new String[]{ taskId.toString(), taskType, Values.SUCCESS.name(),
                        String.valueOf(finishTime),
                        counters.makeEscapedCompactString()});
    }
  }


  /**
   * Update the finish time of task.
   * @param taskId task id
   * @param finishTime finish time of task in ms
   */
  public void logTaskUpdates(TaskID taskId, long finishTime) {
    if (disableHistory) {
      return;
    }

    JobID id = taskId.getJobID();
    if (!this.jobId.equals(id)) {
      throw new RuntimeException("JobId from task: " + id +
                                 " does not match expected: " + jobId);
    }

    if (null != writers) {
      log(writers, RecordTypes.Task,
          new Keys[]{Keys.TASKID, Keys.FINISH_TIME},
          new String[]{ taskId.toString(),
                        String.valueOf(finishTime)});
    }
  }

  /**
   * Log job failed event.
   * @param taskId task id
   * @param taskType MAP or REDUCE.
   * @param time timestamp when job failed detected.
   * @param error error message for failure.
   */
  public void logTaskFailed(TaskID taskId, String taskType, long time,
      String error) {
    logTaskFailed(taskId, taskType, time, error, null);
  }

  /**
   * Log the task failure
   *
   * @param taskId the task that failed
   * @param taskType the type of the task
   * @param time the time of the failure
   * @param error the error the task failed with
   * @param failedDueToAttempt The attempt that caused the failure, if any
   */
  public void logTaskFailed(TaskID taskId, String taskType, long time,
                            String error,
                            TaskAttemptID failedDueToAttempt) {
    if (disableHistory) {
      return;
    }

    JobID id = taskId.getJobID();
    if (!this.jobId.equals(id)) {
      throw new RuntimeException("JobId from task: " + id +
                                 " does not match expected: " + jobId);
    }

    if (null != writers) {
      String failedAttempt = failedDueToAttempt == null ?
          "" :
          failedDueToAttempt.toString();
      log(writers, RecordTypes.Task,
          new Keys[]{Keys.TASKID, Keys.TASK_TYPE,
                     Keys.TASK_STATUS, Keys.FINISH_TIME,
                     Keys.ERROR, Keys.TASK_ATTEMPT_ID},
          new String[]{ taskId.toString(),  taskType,
                        Values.FAILED.name(),
                        String.valueOf(time) , error,
                        failedAttempt});
    }
  }

  /**
   * Log start time of this map task attempt.
   *
   * @param taskAttemptId task attempt id
   * @param startTime start time of task attempt as reported by task tracker.
   * @param trackerName name of the tracker executing the task attempt.
   * @param httpPort http port of the task tracker executing the task attempt
   * @param taskType Whether the attempt is cleanup or setup or map
   */
  public void logMapTaskStarted(TaskAttemptID taskAttemptId, long startTime,
                                String trackerName, int httpPort,
                                String taskType) {
    if (disableHistory) {
      return;
    }

    JobID id = taskAttemptId.getJobID();
    if (!this.jobId.equals(id)) {
      throw new RuntimeException("JobId from task: " + id +
                                 " does not match expected: " + jobId);
    }

    if (null != writers) {
      log(writers, RecordTypes.MapAttempt,
          new Keys[]{ Keys.TASK_TYPE, Keys.TASKID,
                      Keys.TASK_ATTEMPT_ID, Keys.START_TIME,
                      Keys.TRACKER_NAME, Keys.HTTP_PORT},
          new String[]{taskType,
                       taskAttemptId.getTaskID().toString(),
                       taskAttemptId.toString(),
                       String.valueOf(startTime), trackerName,
                       httpPort == -1 ? "" :
                       String.valueOf(httpPort)});
    }
  }

  /**
   * Log finish time of map task attempt.
   *
   * @param taskAttemptId task attempt id
   * @param finishTime finish time
   * @param hostName host name
   * @param taskType Whether the attempt is cleanup or setup or map
   * @param stateString state string of the task attempt
   * @param counter counters of the task attempt
   */
  public void logMapTaskFinished(TaskAttemptID taskAttemptId,
                                 long finishTime,
                                 String hostName,
                                 String taskType,
                                 String stateString,
                                 Counters counter) {
    if (disableHistory) {
      return;
    }

    JobID id = taskAttemptId.getJobID();
    if (!this.jobId.equals(id)) {
      throw new RuntimeException("JobId from task: " + id +
                                 " does not match expected: " + jobId);
    }
    if (null != writers) {
      log(writers, RecordTypes.MapAttempt,
          new Keys[]{ Keys.TASK_TYPE, Keys.TASKID,
                      Keys.TASK_ATTEMPT_ID, Keys.TASK_STATUS,
                      Keys.FINISH_TIME, Keys.HOSTNAME,
                      Keys.STATE_STRING, Keys.COUNTERS},
          new String[]{taskType,
                       taskAttemptId.getTaskID().toString(),
                       taskAttemptId.toString(),
                       Values.SUCCESS.name(),
                       String.valueOf(finishTime), hostName,
                       stateString,
                       counter.makeEscapedCompactString()});
    }
  }

  /**
   * Log task attempt failed event.
   *
   * @param taskAttemptId task attempt id
   * @param timestamp timestamp
   * @param hostName hostname of this task attempt.
   * @param error error message if any for this task attempt.
   * @param taskType Whether the attempt is cleanup or setup or map
   */
  public void logMapTaskFailed(TaskAttemptID taskAttemptId,
                               long timestamp, String hostName,
                               String error, String taskType) {
    if (disableHistory) {
      return;
    }

    JobID id = taskAttemptId.getJobID();
    if (!this.jobId.equals(id)) {
      throw new RuntimeException("JobId from task: " + id +
                                 " does not match expected: " + jobId);
    }

    if (null != writers) {
      log(writers, RecordTypes.MapAttempt,
          new Keys[]{Keys.TASK_TYPE, Keys.TASKID,
                     Keys.TASK_ATTEMPT_ID, Keys.TASK_STATUS,
                     Keys.FINISH_TIME, Keys.HOSTNAME, Keys.ERROR},
          new String[]{ taskType,
                        taskAttemptId.getTaskID().toString(),
                        taskAttemptId.toString(),
                        Values.FAILED.name(),
                        String.valueOf(timestamp),
                        hostName, error});
    }
  }

  /**
   * Log task attempt killed event.
   *
   * @param taskAttemptId task attempt id
   * @param timestamp timestamp
   * @param hostName hostname of this task attempt.
   * @param error error message if any for this task attempt.
   * @param taskType Whether the attempt is cleanup or setup or map
   */
  public void logMapTaskKilled(TaskAttemptID taskAttemptId,
                               long timestamp, String hostName,
                               String error, String taskType) {

    if (disableHistory) {
      return;
    }

    JobID id = taskAttemptId.getJobID();
    if (!this.jobId.equals(id)) {
      throw new RuntimeException("JobId from task: " + id +
                                 " does not match expected: " + jobId);
    }

    if (null != writers) {
      log(writers, RecordTypes.MapAttempt,
          new Keys[]{Keys.TASK_TYPE, Keys.TASKID,
                     Keys.TASK_ATTEMPT_ID, Keys.TASK_STATUS,
                     Keys.FINISH_TIME, Keys.HOSTNAME,
                     Keys.ERROR},
          new String[]{ taskType,
                        taskAttemptId.getTaskID().toString(),
                        taskAttemptId.toString(),
                        Values.KILLED.name(),
                        String.valueOf(timestamp),
                        hostName, error});
    }
  }


  /**
   * Log start time of  Reduce task attempt.
   *
   * @param taskAttemptId task attempt id
   * @param startTime start time
   * @param trackerName tracker name
   * @param httpPort the http port of the tracker executing the task attempt
   * @param taskType Whether the attempt is cleanup or setup or reduce
   */
  public void logReduceTaskStarted(TaskAttemptID taskAttemptId,
                                   long startTime, String trackerName,
                                   int httpPort,
                                   String taskType) {
    if (disableHistory) {
      return;
    }

    JobID id = taskAttemptId.getJobID();
    if (!this.jobId.equals(id)) {
      throw new RuntimeException("JobId from task: " + id +
                                 " does not match expected: " + jobId);
    }

    if (null != writers) {
      log(writers, RecordTypes.ReduceAttempt,
          new Keys[]{  Keys.TASK_TYPE, Keys.TASKID,
                       Keys.TASK_ATTEMPT_ID, Keys.START_TIME,
                       Keys.TRACKER_NAME, Keys.HTTP_PORT},
          new String[]{taskType,
                       taskAttemptId.getTaskID().toString(),
                       taskAttemptId.toString(),
                       String.valueOf(startTime), trackerName,
                       httpPort == -1 ? "" :
                       String.valueOf(httpPort)});
    }
  }

  /**
   * Log finished event of this task.
   *
   * @param taskAttemptId task attempt id
   * @param shuffleFinished shuffle finish time
   * @param sortFinished sort finish time
   * @param finishTime finish time of task
   * @param hostName host name where task attempt executed
   * @param taskType Whether the attempt is cleanup or setup or reduce
   * @param stateString the state string of the attempt
   * @param counter counters of the attempt
   */
  public void logReduceTaskFinished(TaskAttemptID taskAttemptId,
                                    long shuffleFinished,
                                    long sortFinished, long finishTime,
                                    String hostName, String taskType,
                                    String stateString, Counters counter) {
    if (disableHistory) {
      return;
    }

    JobID id = taskAttemptId.getJobID();
    if (!this.jobId.equals(id)) {
      throw new RuntimeException("JobId from task: " + id +
                                 " does not match expected: " + jobId);
    }

    if (null != writers) {
      log(writers, RecordTypes.ReduceAttempt,
          new Keys[]{ Keys.TASK_TYPE, Keys.TASKID,
                      Keys.TASK_ATTEMPT_ID, Keys.TASK_STATUS,
                      Keys.SHUFFLE_FINISHED, Keys.SORT_FINISHED,
                      Keys.FINISH_TIME, Keys.HOSTNAME,
                      Keys.STATE_STRING, Keys.COUNTERS},
          new String[]{taskType,
                       taskAttemptId.getTaskID().toString(),
                       taskAttemptId.toString(),
                       Values.SUCCESS.name(),
                       String.valueOf(shuffleFinished),
                       String.valueOf(sortFinished),
                       String.valueOf(finishTime), hostName,
                       stateString,
                       counter.makeEscapedCompactString()});
    }
  }

  /**
   * Log failed reduce task attempt.
   *
   * @param taskAttemptId task attempt id
   * @param timestamp time stamp when task failed
   * @param hostName host name of the task attempt.
   * @param error error message of the task.
   * @param taskType Whether the attempt is cleanup or setup or reduce
   */
  public void logReduceTaskFailed(TaskAttemptID taskAttemptId, long timestamp,
                                  String hostName, String error,
                                  String taskType) {
    if (disableHistory) {
      return;
    }

    JobID id = taskAttemptId.getJobID();
    if (!this.jobId.equals(id)) {
      throw new RuntimeException("JobId from task: " + id +
                                 " does not match expected: " + jobId);
    }

    if (null != writers) {
      log(writers, RecordTypes.ReduceAttempt,
          new Keys[]{  Keys.TASK_TYPE, Keys.TASKID,
                       Keys.TASK_ATTEMPT_ID, Keys.TASK_STATUS,
                       Keys.FINISH_TIME, Keys.HOSTNAME,
                       Keys.ERROR },
          new String[]{ taskType,
                        taskAttemptId.getTaskID().toString(),
                        taskAttemptId.toString(),
                        Values.FAILED.name(),
                        String.valueOf(timestamp), hostName, error });
    }
  }

  /**
   * Log killed reduce task attempt.
   *
   * @param taskAttemptId task attempt id
   * @param timestamp time stamp when task failed
   * @param hostName host name of the task attempt.
   * @param error error message of the task.
   * @param taskType Whether the attempt is cleanup or setup or reduce
   */
  public void logReduceTaskKilled(TaskAttemptID taskAttemptId, long timestamp,
                                  String hostName, String error,
                                  String taskType) {
    if (disableHistory) {
      return;
    }

    JobID id = taskAttemptId.getJobID();
    if (!this.jobId.equals(id)) {
      throw new RuntimeException("JobId from task: " + id +
                                 " does not match expected: " + jobId);
    }

    if (null != writers) {
      log(writers, RecordTypes.ReduceAttempt,
          new Keys[]{  Keys.TASK_TYPE, Keys.TASKID,
                       Keys.TASK_ATTEMPT_ID, Keys.TASK_STATUS,
                       Keys.FINISH_TIME, Keys.HOSTNAME,
                       Keys.ERROR },
          new String[]{ taskType,
                        taskAttemptId.getTaskID().toString(),
                        taskAttemptId.toString(),
                        Values.KILLED.name(),
                        String.valueOf(timestamp),
                        hostName, error });
    }
  }

  /**
   * Log a number of keys and values with record. the array length of
   * keys and values should be same.
   * @param writers the writers to send the data to
   * @param recordType type of log event
   * @param keys type of log event
   * @param values type of log event
   */
  private void log(ArrayList<PrintWriter> writers, RecordTypes recordType,
                   Keys[] keys, String[] values) {
    log(writers, recordType, keys, values, false);
  }
  /**
   * Log a number of keys and values with the record. This method allows to do
   * it in a synchronous fashion
   * @param writers the writers to send the data to
   * @param recordType the type to log
   * @param keys keys to log
   * @param values values to log
   * @param sync if true - will block until the data is written
   */
  private void log(ArrayList<PrintWriter> writers, RecordTypes recordType,
        Keys[] keys, String[] values, boolean sync) {
    StringBuffer buf = new StringBuffer(recordType.name());
    buf.append(JobHistory.DELIMITER);
    for (int i = 0; i < keys.length; i++) {
      buf.append(keys[i]);
      buf.append("=\"");
      values[i] = JobHistory.escapeString(values[i]);
      buf.append(values[i]);
      buf.append("\"");
      buf.append(JobHistory.DELIMITER);
    }
    buf.append(JobHistory.LINE_DELIMITER_CHAR);

    for (PrintWriter out : writers) {
      LogTask task = new LogTask(out, buf.toString());
      if (sync) {
        task.run();
      } else {
        fileManager.addWriteTask(task);
      }
    }
  }

}
