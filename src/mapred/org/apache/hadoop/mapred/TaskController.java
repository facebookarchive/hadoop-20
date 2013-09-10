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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.CleanupQueue.PathDeletionContext;
import org.apache.hadoop.mapred.JvmManager.JvmEnv;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Shell.ShellCommandExecutor;

/**
 * Controls initialization, finalization and clean up of tasks, and
 * also the launching and killing of task JVMs.
 * 
 * This class defines the API for initializing, finalizing and cleaning
 * up of tasks, as also the launching and killing task JVMs.
 * Subclasses of this class will implement the logic required for
 * performing the actual actions. 
 */
abstract class TaskController implements Configurable {
  /**
   * Wait for the process to be killed for sure if true, otherwise return
   * immediately after the kill is initiated (allows faster scheduling at the
   * risk of overlapping JVMs).
   */
  public static final String WAIT_FOR_CONFIRMED_KILL_KEY =
      "mapred.task.controller.waitForConfirmedKill";
  /**
   * Default is to not wait for a confirmed kill.
   */
  public static final boolean WAIT_FOR_CONFIRMED_DEFAULT = false;
  /**
   * Number of retries to kill.  This is only used if
   * WAIT_FOR_CONFIRMED_KILL_KEY = true.
   */
  public static final String CONFIRMED_KILL_RETRIES_KEY =
      "mapred.task.controller.confirmedKillRetries";
  /**
   * Default number of retries (3)
   */
  public static final int CONFIRMED_KILL_RETRIES_DEFAULT = 3;

  /** Wait for a confirmed kill of the task? */
  private boolean waitForConfirmedKill = false;
  /** Number of retries when doing a confirmed kill */
  private int confirmedKillRetries;

  private Configuration conf;
  
  public static final Log LOG = LogFactory.getLog(TaskController.class);
  
  public Configuration getConf() {
    return conf;
  }
  
  public void setConf(Configuration conf) {
    this.conf = conf;
  }
  
  /**
   * Setup task controller component.  Will be called prior to use.
   */
  void setup() {
    // Cannot set wait for confirmed kill mode if cannot check if task is alive
    if (supportsIsTaskAlive()) {
      waitForConfirmedKill = getConf().getBoolean(WAIT_FOR_CONFIRMED_KILL_KEY,
          WAIT_FOR_CONFIRMED_DEFAULT);
      confirmedKillRetries = getConf().getInt(CONFIRMED_KILL_RETRIES_KEY,
          CONFIRMED_KILL_RETRIES_DEFAULT);
    }
    LOG.info("setup: waitForConfirmedKill=" + waitForConfirmedKill +
        ", confirmedKillRetries=" + confirmedKillRetries);
  }
  
  /**
   * Launch a task JVM
   * 
   * This method defines how a JVM will be launched to run a task.
   * @param context the context associated to the task
   */
  abstract void launchTaskJVM(TaskControllerContext context)
                                      throws IOException;
  
  /**
   * Top level cleanup a task JVM method.
   *
   * The current implementation does the following.
   * <ol>
   * <li>Sends a graceful terminate signal to task JVM allowing its sub-process
   * to cleanup.</li>
   * <li>Waits for stipulated period</li>
   * <li>Sends a forceful kill signal to task JVM, terminating all its
   * sub-process forcefully.</li>
   * </ol>
   **/
  private class DestroyJVMTaskRunnable implements Runnable {
    TaskControllerContext context;
    /**
     * @param context the task for which kill signal has to be sent.
     */
    public DestroyJVMTaskRunnable(TaskControllerContext context) {
      this.context = context;
    }
    @Override
    public void run() {
      terminateTask(context);
      int attempts = 0;
      boolean isTaskConfirmedAlive = false;
      boolean forcefullKillUsed = false;
      do {
        try {
          Thread.sleep(context.sleeptimeBeforeSigkill);
        } catch (InterruptedException e) {
          LOG.warn("Sleep interrupted : " +
              StringUtils.stringifyException(e));
        }
        if (waitForConfirmedKill) {
          isTaskConfirmedAlive = isTaskAlive(context);
          if (!isTaskConfirmedAlive) {
            break;
          }
        }
        killTask(context);
        forcefullKillUsed = true;
        ++attempts;
      } while (waitForConfirmedKill && (attempts < confirmedKillRetries));
      if (waitForConfirmedKill) {
        LOG.info("run: pid = " + context.pid + ", confirmedAlive = " +
            isTaskConfirmedAlive + ", attempts = " + attempts +
            ", forcefullKillUsed = " + forcefullKillUsed);
      }
    }
  }
  
  /**
   * Use DestroyJVMTaskRunnable to kill task JVM asynchronously.  Wait for the
   * confirmed kill if configured so.
   *
   * @param context Task context
   */
  final void destroyTaskJVM(TaskControllerContext context) {
    Thread taskJVMDestroyer = new Thread(new DestroyJVMTaskRunnable(context));
    taskJVMDestroyer.start();
    if (waitForConfirmedKill) {
      try {
        taskJVMDestroyer.join();
      } catch (InterruptedException e) {
        throw new IllegalStateException("destroyTaskJVM: Failed to join " +
            taskJVMDestroyer.getName());
      }
    }
  }
  
  /**
   * Perform initializing actions required before a task can run.
   * 
   * For instance, this method can be used to setup appropriate
   * access permissions for files and directories that will be
   * used by tasks. Tasks use the job cache, log, PID and distributed cache
   * directories and files as part of their functioning. Typically,
   * these files are shared between the daemon and the tasks
   * themselves. So, a TaskController that is launching tasks
   * as different users can implement this method to setup
   * appropriate ownership and permissions for these directories
   * and files.
   */
  abstract void initializeTask(TaskControllerContext context);
  
  
  /**
   * Contains task information required for the task controller.
   */
  static class TaskControllerContext {
    // task being executed
    Task task; 
    // the JVM environment for the task
    JvmEnv env;
    // the Shell executor executing the JVM for this task
    ShellCommandExecutor shExec; 
    // process handle of task JVM
    String pid;
    // waiting time before sending SIGKILL to task JVM after sending SIGTERM
    long sleeptimeBeforeSigkill;

    @Override
    public String toString() {
      return "task=" + task + ",env=" + env + ",shExec=" + shExec +
          ",pid=" + pid + ",sleeptimeBeforeSigkill=" +
          sleeptimeBeforeSigkill;
    }
  }

  /**
   * Contains info related to the path of the file/dir to be deleted. This info
   * is needed by task-controller to build the full path of the file/dir
   */
  static class TaskControllerPathDeletionContext extends PathDeletionContext {
    Task task;
    boolean isWorkDir;
    TaskController taskController;

    /**
     * mapredLocalDir is the base dir under which to-be-deleted taskWorkDir or
     * taskAttemptDir exists. fullPath of taskAttemptDir or taskWorkDir
     * is built using mapredLocalDir, jobId, taskId, etc.
     */
    Path mapredLocalDir;

    public TaskControllerPathDeletionContext(FileSystem fs, Path mapredLocalDir,
        Task task, boolean isWorkDir, TaskController taskController) {
      super(fs, null);
      this.task = task;
      this.isWorkDir = isWorkDir;
      this.taskController = taskController;
      this.mapredLocalDir = mapredLocalDir;
    }

    @Override
    protected String getPathForCleanup() {
      if (fullPath == null) {
        fullPath = buildPathForDeletion();
      }
      return fullPath;
    }

    /**
     * Builds the path of taskAttemptDir OR taskWorkDir based on
     * mapredLocalDir, jobId, taskId, etc
     */
    String buildPathForDeletion() {
      String subDir = TaskTracker.getLocalTaskDir(task.getJobID().toString(),
          task.getTaskID().toString(), task.isTaskCleanupTask());
      if (isWorkDir) {
        subDir = subDir + Path.SEPARATOR + "work";
      }
      return mapredLocalDir.toUri().getPath() + Path.SEPARATOR + subDir;
    }

    /**
     * Makes the path(and its subdirectories recursively) fully deletable by
     * setting proper permissions(777) by task-controller
     */
    @Override
    protected void enablePathForCleanup() throws IOException {
      getPathForCleanup();// allow init of fullPath
      if (fs.exists(new Path(fullPath))) {
        taskController.enableTaskForCleanup(this); 
      }
    }
  }

  /**
   * Method which is called after the job is localized so that task controllers
   * can implement their own job localization logic.
   * 
   * @param tip  Task of job for which localization happens.
   */
  abstract void initializeJob(JobID jobId);
  
  /**
   * Sends a graceful terminate signal to taskJVM and it sub-processes. 
   *   
   * @param context task context
   */
  abstract void terminateTask(TaskControllerContext context);
  
  /**
   * Sends a KILL signal to forcefully terminate the taskJVM and its
   * sub-processes.
   * 
   * @param context task context
   */
  
  abstract void killTask(TaskControllerContext context);

  /**
   * Only some TaskController implementations may support checking if the
   * task is alive.  If we cannot check if the task is alive, then the
   * confirmed kill mode cannot be enabled.
   *
   * @return True if this implementation supports checking if
   *         the task is alive, false otherwise
   */
  abstract boolean supportsIsTaskAlive();

  /**
   * Checks if the task JVM is alive.  May not be supported by all
   * implementations (see supportsIsTaskAlive).
   *
   * @param context Task context
   * @return True if the task is alive, false otherwise
   */
  abstract boolean isTaskAlive(TaskControllerContext context);

  /**
   * Get the current stack trace of this task.
   * 
   * 
   * @param context Task context
   */
  abstract void doStackTrace(TaskControllerContext context);
  
  /**
   * Enable the task for cleanup by changing permissions of the path
   * @param context   path deletion context
   * @throws IOException
   */
  abstract void enableTaskForCleanup(PathDeletionContext context)
      throws IOException;
}
