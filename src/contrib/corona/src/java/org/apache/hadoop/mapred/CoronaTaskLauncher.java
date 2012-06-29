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

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.corona.InetAddress;

/**
 * Sends actions to Corona Task Trackers.
 * There are several threads used for sending the actions so that a single
 * dead task tracker does not block all actions. To preserve the order of
 * operations on a single task (like sending KillTaskAction after
 * LaunchTaskAction), the actions are queued to the same thread by doing
 * taskid % numThreads. We do not need to preserve the order of task-level
 * actions and the KillJobAction, since once we send KillJobAction, the job
 * tracker will shutdown anyway. So any actions sent after that will fail.
 */
public class CoronaTaskLauncher {
  /** Logger. */
  private static final Log LOG = LogFactory.getLog(CoronaTaskLauncher.class);

  /** The workers that send actions to task trackers. */
  private final ActionSender[] workers;
  /** The pool of worker threads that send actions to task trackers. */
  private final Thread[] workerThreads;

  /** The Corona Job Tracker. */
  private final CoronaJobTracker coronaJT;
  /** The expiry logic. */
  private final ExpireTasks expireTasks;

  /** Constructor.
   * @param conf The configuration.
   * @param coronaJT The Corona Job Tracker.
   * @param expireTasks The expiry logic.
   */
  CoronaTaskLauncher(
      Configuration conf,
      CoronaJobTracker coronaJT,
      ExpireTasks expireTasks) {
    this.coronaJT = coronaJT;
    this.expireTasks = expireTasks;
    int numLauncherThreads = conf.getInt(
        "mapred.corona.jobtracker.numtasklauncherthreads", 4);
    workers = new ActionSender[numLauncherThreads];
    workerThreads = new Thread[numLauncherThreads];
    for (int i = 0; i < numLauncherThreads; i++) {
      workers[i] = new ActionSender(i);
      workerThreads[i] = new Thread(workers[i]);
      workerThreads[i].setName("Task Launcher Thread #" + i);
      workerThreads[i].setDaemon(true);
      workerThreads[i].start();
    }
  }

  /**
   * Enqueue an action to kill the job.
   * @param jobId The job identifier.
   * @param allTrackers All trackers to send the kill to.
   */
  @SuppressWarnings("deprecation")
  public void killJob(JobID jobId, Map<String, InetAddress> allTrackers) {
    int workerId = 0;
    for (Map.Entry<String, InetAddress> entry : allTrackers.entrySet()) {
      String trackerName = entry.getKey();
      InetAddress addr = entry.getValue();
      String description = "KillJobAction " + jobId;
      ActionToSend action = new ActionToSend(trackerName, addr,
          new KillJobAction(jobId), description);
      workers[workerId].enqueueAction(action);
      LOG.info("Queueing "  + description + " to worker " + workerId + " " +
        trackerName + "(" + addr.host + ":" + addr.port + ")");
      workerId = (workerId + 1) % workers.length;
    }
  }

  /**
   * Enqueue kill tasks actions.
   * @param trackerName The name of the tracker to send the kill actions to.
   * @param addr The address of the tracker to send the kill actions to.
   * @param killActions The kill actions to send.
   */
  public void killTasks(
      String trackerName, InetAddress addr, List<KillTaskAction> killActions) {
    for (KillTaskAction killAction : killActions) {
      int workerId = workerIdForTask(killAction.getTaskID());
      String description = "KillTaskAction " + killAction.getTaskID();
      LOG.info("Queueing " + description + " to worker " + workerId + " " +
        trackerName + "(" + addr.host + ":" + addr.port + ")");
      workers[workerId].enqueueAction(
          new ActionToSend(trackerName, addr, killAction, description));
    }
  }

  /**
   * Enqueue a commit task action.
   * @param trackerName The name of the tracker to send the commit action to.
   * @param addr The address of the tracker to send the commit action to.
   * @param action The commit action to send.
   */
  public void commitTask(
      String trackerName, InetAddress addr, CommitTaskAction action) {
    int workerId = workerIdForTask(action.getTaskID());
    String description = "KillTaskAction " + action.getTaskID();
    LOG.info("Queueing " + description + " to worker " + workerId + " " +
      trackerName + "(" + addr.host + ":" + addr.port + ")");
    workers[workerId].enqueueAction(new ActionToSend(
        trackerName, addr, action, description));
  }

  /**
   * Remove a launching task.
   * @param attempt The task attempt ID.
   * @return A boolean indicating if an enqueued action was removed.
   */
  @SuppressWarnings("deprecation")
  public boolean removeLaunchingTask(TaskAttemptID attempt) {
    ActionSender designatedWorker = workers[workerIdForTask(attempt)];
    return designatedWorker.removeLaunchingTask(attempt);
  }

  /**
   * Enqueue a launch task action.
   * @param task The task to launch.
   * @param trackerName The name of the tracker to send the task to.
   * @param addr The address of the tracker to send the task to.
   */
  public void launchTask(Task task, String trackerName, InetAddress addr) {
    CoronaSessionInfo info = new CoronaSessionInfo(
      coronaJT.getSessionId(), coronaJT.getJobTrackerAddress());
    LaunchTaskAction action = new LaunchTaskAction(task, info);
    String description = "LaunchTaskAction " + action.getTask().getTaskID();
    ActionToSend actionToSend =
        new ActionToSend(trackerName, addr, action, description);
    int workerId = workerIdForTask(task.getTaskID());
    LOG.info("Queueing " + description +  " to worker " + workerId + " " +
      trackerName + "(" + addr.host + ":" + addr.port + ")");
    workers[workerId].enqueueAction(actionToSend);
  }

  /**
   * Represents an action to send to a task tracker.
   */
  private class ActionToSend {
    /** The host of the tracker. */
    private final String trackerHost;
    /** The name of the tracker. */
    private final String trackerName;
    /** The port of the tracker. */
    private final int port;
    /** The action to send. */
    private final TaskTrackerAction ttAction;
    /** Description for logging. */
    private final String description;
    /** Action creation time */
    private final long ctime = System.currentTimeMillis();

    /** Constructor
     * @param trackerName The name of the tracker.
     * @param addr The address of the tracker.
     * @param action The action to send.
     */
    private ActionToSend(String trackerName, InetAddress addr,
        TaskTrackerAction action, String description) {
      this.trackerName = trackerName;
      this.trackerHost = addr.host;
      this.port = addr.port;
      this.ttAction = action;
      this.description = description;
    }
  }

  /**
   * A worker that sends actions to trackers. All actions for a task are hashed
   * to a single worker.
   */
  private class ActionSender implements Runnable {
    /** The queue of actions. */
    private final List<ActionToSend> workQueue = new LinkedList<ActionToSend>();
    /** The worker identifier. */
    private final int id;
    /** Constructor.
     * @param id The identifier of the worker.
     */
    public ActionSender(int id) {
      this.id = id;
    }
    @Override
    public void run() {
      LOG.info("Starting TaskLauncher thread#" + id);
      while (true) {
        try {
          launchTasks();
        } catch (InterruptedException e) {
          // Ignore, these are daemon threads.
          if (LOG.isDebugEnabled()) {
            LOG.debug("Got InterruptedException while launching a task", e);
          }
        }
      }
    }

    /**
     * Sends a bunch of tasks at a time. This is called repeatedly.
     *
     * @throws InterruptedException
     */
    private void launchTasks() throws InterruptedException {
      List<ActionToSend> actions = new ArrayList<ActionToSend>();
      synchronized (workQueue) {
        while (workQueue.isEmpty()) {
          workQueue.wait();
        }
        actions.addAll(workQueue);
        workQueue.clear();
      }

      for (ActionToSend actionToSend : actions) {
        String trackerName = actionToSend.trackerName;
        if (coronaJT.getTrackerStats().isFaulty(trackerName)) {
          LOG.warn("Not sending " + actionToSend.description +  " to " +
            actionToSend.trackerHost + ":" + actionToSend.port +
                " since previous communication failed");
          coronaJT.processTaskLaunchError(actionToSend.ttAction);
          continue;
        }

        // Fill in the job tracker information.
        CoronaSessionInfo info = new CoronaSessionInfo(
            coronaJT.getSessionId(), coronaJT.getJobTrackerAddress());
        actionToSend.ttAction.setExtensible(info);

        // Get the tracker address.
        String trackerRpcAddress =
          actionToSend.trackerHost + ":" + actionToSend.port;
        try {
          // Start the timer on the task just before making the connection
          // and RPC. If there are any errors after this point, we will reuse
          // the error handling for expired launch tasks.
          if (actionToSend.ttAction instanceof LaunchTaskAction) {
            LaunchTaskAction lta = (LaunchTaskAction) actionToSend.ttAction;
            expireTasks.addNewTask(lta.getTask().getTaskID());
          }
          CoronaTaskTrackerProtocol client = coronaJT.getTaskTrackerClient(
            actionToSend.trackerHost, actionToSend.port);
          client.submitActions(new TaskTrackerAction[]{actionToSend.ttAction});
        } catch (IOException e) {
          LOG.error("Could not send " + actionToSend.description +
                " to " + trackerRpcAddress, e);
          coronaJT.resetTaskTrackerClient(
            actionToSend.trackerHost, actionToSend.port);
          coronaJT.getTrackerStats().recordConnectionError(trackerName);
          coronaJT.processTaskLaunchError(actionToSend.ttAction);
        }
        // Time To Send
        long TTS = System.currentTimeMillis() - actionToSend.ctime;
        LOG.info("Processed " + actionToSend.description + " for " +
            actionToSend.trackerName + " " + TTS + " msec after its creation.");
      }
    }

    /**
     * Remove a task pending launch.
     * @param attempt The task attempt ID.
     * @return A boolean indicating if a pending launch was removed.
     */
    @SuppressWarnings("deprecation")
    boolean removeLaunchingTask(TaskAttemptID attempt) {
      synchronized (workQueue) {
        Iterator<ActionToSend> actionIter = workQueue.iterator();
        while (actionIter.hasNext()) {
          ActionToSend action = actionIter.next();
          if (action.ttAction instanceof LaunchTaskAction &&
              ((LaunchTaskAction) action.ttAction).getTask().
                getTaskID().equals(attempt)) {
            actionIter.remove();
            return true;
          }
        }
      }
      return false;
    }

    /**
     * Enqueue an action to this worker.
     * @param a The action.
     */
    public void enqueueAction(ActionToSend a) {
      synchronized (workQueue) {
        workQueue.add(a);
        workQueue.notify();
      }
    }
  } // Worker

  /**
   * Get the worker ID for a task attempt.
   * We have this function so that all actions for a task attempt go to a
   * single thread. But actions for different attempts of the same task will
   * go to different threads. This is good when a thread gets stuck and the
   * next attempt of the task can go to another thread.
   * @param attemptID The task attempt.
   * @return The ID.
   */
  @SuppressWarnings("deprecation")
  private int workerIdForTask(TaskAttemptID attemptID) {
    int taskNum = attemptID.getTaskID().getId();
    int attemptNum = attemptID.getId();
    return (taskNum + attemptNum)  % workers.length;
  }
}
