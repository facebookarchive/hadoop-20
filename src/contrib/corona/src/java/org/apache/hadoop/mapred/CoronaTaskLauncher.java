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
  private final ExpireLaunchingTasks expireLaunchingTasks;

  /** Constructor.
   * @param conf The configuration.
   * @param coronaJT The Corona Job Tracker.
   * @param expireLaunchingTasks The expiry logic.
   */
  CoronaTaskLauncher(
      Configuration conf,
      CoronaJobTracker coronaJT,
      ExpireLaunchingTasks expireLaunchingTasks) {
    this.coronaJT = coronaJT;
    this.expireLaunchingTasks = expireLaunchingTasks;
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
      LOG.info("Sending kill job to " + trackerName + "(" + addr.host + ":" +
          addr.port + ")");
      ActionToSend action = new ActionToSend(trackerName, addr,
          new KillJobAction(jobId));
      workers[workerId].enqueueAction(action);
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
      workers[workerId].enqueueAction(
          new ActionToSend(trackerName, addr, killAction));
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
    workers[workerId].enqueueAction(new ActionToSend(
        trackerName, addr, action));
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
    LOG.info("Queueing a launch task action for " + trackerName + "(" +
        addr.host + ":" + addr.port + ")");
    CoronaSessionInfo info =
        new CoronaSessionInfo(coronaJT.sessionId, coronaJT.jobTrackerAddress);
    LaunchTaskAction action = new LaunchTaskAction(task, info);
    ActionToSend actionToSend =
        new ActionToSend(trackerName, addr, action);
    int workerId = workerIdForTask(task.getTaskID());
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

    /** Constructor
     * @param trackerName The name of the tracker.
     * @param addr The address of the tracker.
     * @param action The action to send.
     */
    private ActionToSend(String trackerName, InetAddress addr,
        TaskTrackerAction action) {
      this.trackerName = trackerName;
      this.trackerHost = addr.host;
      this.port = addr.port;
      this.ttAction = action;
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
        // Get the tracker address.
        InetSocketAddress trackerRpcAddress =
            new InetSocketAddress(actionToSend.trackerHost, actionToSend.port);

        String trackerName = actionToSend.trackerName;
        if (coronaJT.trackerStats.isFaulty(trackerName)) {
          LOG.info("Not sending " + actionToSend.ttAction.getClass() + " to " +
              actionToSend.trackerHost + " since previous communication " +
              " failed");
          coronaJT.processTaskLaunchError(actionToSend.ttAction);
          continue;
        }

        // Fill in the job tracker information.
        CoronaSessionInfo info = new CoronaSessionInfo(
            coronaJT.sessionId, coronaJT.jobTrackerAddress);
        actionToSend.ttAction.setExtensible(info);

        try {
          // Start the timer on the task just before making the connection
          // and RPC. If there are any errors after this point, we will reuse
          // the error handling for expired launch tasks.
          if (actionToSend.ttAction instanceof LaunchTaskAction) {
            LaunchTaskAction lta = (LaunchTaskAction) actionToSend.ttAction;
            expireLaunchingTasks.addNewTask(lta.getTask().getTaskID());
          }
          CoronaTaskTrackerProtocol client =
              coronaJT.trackerClientCache.getClient(trackerRpcAddress);
          client.submitActions(new TaskTrackerAction[]{actionToSend.ttAction});
        } catch (IOException e) {
          LOG.error("Could not send " + actionToSend.ttAction.getClass() +
              " action to " + actionToSend.trackerHost, e);
          coronaJT.trackerClientCache.resetClient(trackerRpcAddress);
          coronaJT.trackerStats.recordConnectionError(trackerName);
          coronaJT.processTaskLaunchError(actionToSend.ttAction);
        }
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
   * Get the worker ID for a task.
   * @param taskId The task.
   * @return The ID.
   */
  @SuppressWarnings("deprecation")
  private int workerIdForTask(TaskAttemptID taskId) {
    return taskId.getTaskID().getId() % workers.length;
  }
}
