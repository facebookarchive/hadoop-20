/**
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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Vector;

public abstract class JobTrackerTraits {

  protected static final TaskReport[] EMPTY_TASK_REPORTS = new TaskReport[0];

  public abstract JobInProgressTraits getJobInProgress(JobID jobid);

  protected TaskReport[] getMapTaskReportsImpl(JobID jobid) {
    return getTaskReportsImpl(jobid, TaskReport.Type.MAP);
  }

  protected TaskReport[] getReduceTaskReportsImpl(JobID jobid) {
    return getTaskReportsImpl(jobid, TaskReport.Type.REDUCE);
  }

  protected TaskReport[] getCleanupTaskReportsImpl(JobID jobid) {
    return getTaskReportsImpl(jobid, TaskReport.Type.CLEANUP);
  }

  protected TaskReport[] getSetupTaskReportsImpl(JobID jobid) {
    return getTaskReportsImpl(jobid, TaskReport.Type.SETUP);
  }

  protected TaskReport[] getTaskReportsImpl(
      JobID jobid, TaskReport.Type type) {
    // JobInProgress.getJobInProgress is protected by JobInProgress.jobs lock
    JobInProgressTraits job = getJobInProgress(jobid);
    if (job == null || !job.inited()) {
      return EMPTY_TASK_REPORTS;
    }
    Vector<TaskInProgress> completeTasks;
    Vector<TaskInProgress> incompleteTasks;
    switch (type) {
      case MAP:
        // Note that JobInProgressTraits.reportTasksInProgress is not protected
        // but JobInProgress.reportTasksInProgress is synchronized
        completeTasks = job.reportTasksInProgress(true, true);
        incompleteTasks = job.reportTasksInProgress(true, false);
        break;
      case REDUCE:
        completeTasks = job.reportTasksInProgress(false, true);
        incompleteTasks = job.reportTasksInProgress(false, false);
        break;
      case SETUP:
        completeTasks = job.reportSetupTIPs(true);
        incompleteTasks = job.reportSetupTIPs(false);
        break;
      case CLEANUP:
        completeTasks = job.reportCleanupTIPs(true);
        incompleteTasks = job.reportCleanupTIPs(false);
        break;
      default:
        throw new IllegalArgumentException();
    }
    List<TaskReport> reports = new ArrayList<TaskReport>();
    for (TaskInProgress tip : completeTasks) {
      reports.add(tip.generateSingleReport());
    }
    for (TaskInProgress tip : incompleteTasks) {
      reports.add(tip.generateSingleReport());
    }
    return reports.toArray(new TaskReport[reports.size()]);
  }


  protected static final String[] EMPTY_TASK_DIAGNOSTICS = new String[0];

  /**
   * Get the diagnostics for a given task
   * @param taskId the id of the task
   * @return an array of the diagnostic messages
   */
  protected String[] getTaskDiagnosticsImpl(TaskAttemptID taskId)
    throws IOException {

    List<String> taskDiagnosticInfo = null;
    JobID jobId = taskId.getJobID();
    TaskID tipId = taskId.getTaskID();
    JobInProgressTraits job = getJobInProgress(jobId);

    if (job != null && job.inited()) {
      TaskInProgress tip = job.getTaskInProgress(tipId);
      if (tip != null) {
        taskDiagnosticInfo = tip.getDiagnosticInfo(taskId);
      }

    }
    return ((taskDiagnosticInfo == null) ? EMPTY_TASK_DIAGNOSTICS :
            taskDiagnosticInfo.toArray(new String[taskDiagnosticInfo.size()]));
  }

  /**
   * Returns specified TaskInProgress, or null.
   */
  public TaskInProgress getTip(TaskID tipid) {
    JobInProgressTraits job = getJobInProgress(tipid.getJobID());
    return (job == null ? null : job.getTaskInProgress(tipid));
  }

}