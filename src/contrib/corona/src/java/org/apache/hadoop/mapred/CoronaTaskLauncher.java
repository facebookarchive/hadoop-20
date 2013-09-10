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
import java.util.HashMap;
import java.util.Random;
import java.util.Collections;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.corona.InetAddress;

/**
 * Sends actions to Corona Task Trackers.
 * There are several threads used for sending the actions so that a single
 * dead task tracker does not block all actions. To preserve the order of
 * operations on a single task (like sending KillTaskAction after
 * LaunchTaskAction), the actions are queued to the same queue.
 * We do not need to preserve the order of task-level
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
  /** The pool of list of ActionToSend based on task tracker. */
  private final WorkQueues allWorkQueues;

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
    allWorkQueues = new WorkQueues();
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
    for (Map.Entry<String, InetAddress> entry : allTrackers.entrySet()) {
      String trackerName = entry.getKey();
      InetAddress addr = entry.getValue();
      String description = "KillJobAction " + jobId;
      ActionToSend action = new ActionToSend(trackerName, addr,
          new KillJobAction(jobId), description);
      allWorkQueues.enqueueAction(action);
      LOG.info("Queueing "  + description + " to worker " +
        trackerName + "(" + addr.host + ":" + addr.port + ")");
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
      String description = "KillTaskAction " + killAction.getTaskID();
      LOG.info("Queueing " + description + " to worker " +
        trackerName + "(" + addr.host + ":" + addr.port + ")");
      allWorkQueues.enqueueAction(
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
    String description = "KillTaskAction " + action.getTaskID();
    LOG.info("Queueing " + description + " to worker " +
      trackerName + "(" + addr.host + ":" + addr.port + ")");
    allWorkQueues.enqueueAction(new ActionToSend(
        trackerName, addr, action, description));
  }

  /**
   * Remove a launching task.
   * @param attempt The task attempt ID.
   * @return A boolean indicating if an enqueued action was removed.
   */
  @SuppressWarnings("deprecation")
  public boolean removeLaunchingTask(TaskAttemptID attempt) {
    return allWorkQueues.removeLaunchingTask(attempt);
  }

  /**
   * Enqueue a launch task action.
   * @param task The task to launch.
   * @param trackerName The name of the tracker to send the task to.
   * @param addr The address of the tracker to send the task to.
   */
  public void launchTask(Task task, String trackerName, InetAddress addr) {
    CoronaSessionInfo info = new CoronaSessionInfo(
      coronaJT.getSessionId(), coronaJT.getJobTrackerAddress(),
      coronaJT.getSecondaryTrackerAddress());
    LaunchTaskAction action = new LaunchTaskAction(task, info);
    String description = "LaunchTaskAction " + action.getTask().getTaskID();
    ActionToSend actionToSend =
        new ActionToSend(trackerName, addr, action, description);
    LOG.info("Queueing " + description +  " to worker " +
      trackerName + "(" + addr.host + ":" + addr.port + ")");
    allWorkQueues.enqueueAction(actionToSend);
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
    /** key is used in the WorkQueues */
    private String key;

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
      this.key = this.trackerHost + ":" + this.port;
    }

  }
  
  private class TrackerQueue {
    boolean beingProcessed = false;
    List<ActionToSend> actionQueue = new ArrayList<ActionToSend>();;
  }
  private class WorkQueues {
    private final Map<String, TrackerQueue> trackerQueueMap = 
      new HashMap<String, TrackerQueue>();
    private Random randomGenerator = new Random();
  
    /**
     * Remove a task pending launch.
     * @param attempt The task attempt ID.
     * @return A boolean indicating if a pending launch was removed.
     */
    @SuppressWarnings("deprecation")
    boolean removeLaunchingTask(TaskAttemptID attempt) {
      synchronized (trackerQueueMap) {
        Iterator<TrackerQueue> queueIter = trackerQueueMap.values().iterator();
        while (queueIter.hasNext()) {
          Iterator<ActionToSend>  actIter= queueIter.next().actionQueue.iterator();
          while ( actIter.hasNext()) {
            ActionToSend action = (ActionToSend)actIter.next();
            if (action.ttAction instanceof LaunchTaskAction &&
              ((LaunchTaskAction) action.ttAction).getTask().
                getTaskID().equals(attempt)) {
              actIter.remove();
              return true;
            }
          }
        }
      }
      return false;
    }

    /**
     * Enqueue an action to this tracker.
     * @param a The action.
     */
    void enqueueAction(ActionToSend a) {
      synchronized (trackerQueueMap) {
        TrackerQueue existingQueue = trackerQueueMap.get(a.key);
        if (existingQueue != null) {
          existingQueue.actionQueue.add(a);
        }
        else {
          // no existing work queue for the appInfo
          TrackerQueue newQueue = new TrackerQueue();
          newQueue.actionQueue.add(a);
          trackerQueueMap.put(a.key, newQueue);
        }
        trackerQueueMap.notify();
      }
    }
    
    /**
     *  get a list of AendAction to work on
     *  @param id The threadId id
     *  @param actions  The action list
     */
    void getQueue(int id, List<ActionToSend> actions) throws InterruptedException {
      synchronized(trackerQueueMap){
        if (trackerQueueMap.size() == 0) {
          trackerQueueMap.wait();
        }
        Object[] tmpLists = trackerQueueMap.values().toArray();
        
        // find the starting index for the thread to check.
        // To make sure each tackTracker gets processed, each thread starts 
        // from the entry which maps to its threadid
        int tmpIndex = randomGenerator.nextInt(tmpLists.length);
        
        int checkedQueues = 0;
        while (checkedQueues < tmpLists.length){
          TrackerQueue tmpQueue = (TrackerQueue)tmpLists[tmpIndex];
          if (tmpQueue.actionQueue.size() > 0 && tmpQueue.beingProcessed == false) {
            actions.addAll(tmpQueue.actionQueue);
            tmpQueue.actionQueue.clear();
            tmpQueue.beingProcessed = true;
            return;
          }
          tmpIndex = (tmpIndex +1) % tmpLists.length;
          checkedQueues ++;
        }
        trackerQueueMap.wait();
      }
      return;
    }

    void resetQueueFlag(String key) {
      synchronized (trackerQueueMap) {
        TrackerQueue existingQueue = trackerQueueMap.get(key);
        if (existingQueue != null) {
          existingQueue.beingProcessed = false;
        }
        trackerQueueMap.notify();
      }
    }
  }

  /**
   * A worker that sends actions to trackers. All actions for a task are hashed
   * to a single worker.
   */
  private class ActionSender implements Runnable {
    /** The worker identifier. */
    private final int id;
    private String lastKey = null;
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
        lastKey = null;
        try {
          launchTasks();
        } catch (InterruptedException e) {
          // Ignore, these are daemon threads.
          if (LOG.isDebugEnabled()) {
            LOG.debug("Got InterruptedException while launching a task", e);
          }
        } finally {
          if (lastKey != null) {
            allWorkQueues.resetQueueFlag(lastKey);
          }
        }
      }
    }

    /**
     * Sends a bunch of tasks at a time.
     *
     * @throws InterruptedException
     */
    private void launchTasks() throws InterruptedException {
      List<ActionToSend> actions = new ArrayList<ActionToSend>();
      allWorkQueues.getQueue(id, actions);
      if (actions.size() == 0) {
        return;
      }

      long actionSendStart = System.currentTimeMillis();
      String trackerName = actions.get(0).trackerName;
      String host = actions.get(0).trackerHost;
      int port = actions.get(0).port;
      lastKey = actions.get(0).key;

      if (coronaJT.getTrackerStats().isFaulty(trackerName)) {
        for (ActionToSend actionToSend: actions) {
          LOG.warn("Not sending " + actionToSend.description +  " to " +
            actionToSend.trackerHost + ":" + actionToSend.port +
              " since previous communication failed");
          coronaJT.processTaskLaunchError(actionToSend.ttAction);
        }
        return;
      }

      // Fill in the job tracker information.
      CoronaSessionInfo info = new CoronaSessionInfo(
        coronaJT.getSessionId(), coronaJT.getJobTrackerAddress(),
        coronaJT.getSecondaryTrackerAddress());
      for (ActionToSend actionToSend: actions) {
        actionToSend.ttAction.setExtensible(info);
      }

      // Get the tracker address.
      String trackerRpcAddress = host + ":" + port;
      long setupTime = System.currentTimeMillis();
      long expireTaskTime = 0, getClientTime = 0, submitActionTime = 0;
      try {
        // Start the timer on the task just before making the connection
        // and RPC. If there are any errors after this point, we will reuse
        // the error handling for expired launch tasks.
        for (ActionToSend actionToSend: actions) {
          if (actionToSend.ttAction instanceof LaunchTaskAction) {
            LaunchTaskAction lta = (LaunchTaskAction) actionToSend.ttAction;
            expireTasks.addNewTask(lta.getTask().getTaskID());
          }
        }
        TaskTrackerAction[] actArr = new TaskTrackerAction[actions.size()];
        int index = 0;
        for (ActionToSend actionToSend: actions) {
          assert(actionToSend.trackerHost.equals(host) && actionToSend.port == port);
          actArr[index] = actionToSend.ttAction;
          index++;
        }
        expireTaskTime = System.currentTimeMillis();
        CoronaTaskTrackerProtocol client = coronaJT.getTaskTrackerClient(host, port);
        getClientTime = System.currentTimeMillis();
        client.submitActions(actArr);
        submitActionTime = System.currentTimeMillis();
      } catch (IOException e) {
        for (ActionToSend actionToSend: actions) {
          LOG.error("Could not send " + actionToSend.description +
              " to " + trackerRpcAddress, e);
          coronaJT.resetTaskTrackerClient(
            actionToSend.trackerHost, actionToSend.port);
          coronaJT.getTrackerStats().recordConnectionError(trackerName);
          coronaJT.processTaskLaunchError(actionToSend.ttAction);
        }
      }
      for (ActionToSend actionToSend: actions) {
        // Time To Send
        long TTS = System.currentTimeMillis() - actionToSend.ctime;
        if (TTS > 500) {
          LOG.info("Thread " + id + " processed " + actionToSend.description + " for " +
            actionToSend.trackerName + " " + actionToSend.port + " " +
            TTS + " msec after its creation. Times spent:" +
            " setupTime = " + (setupTime - actionSendStart) +
            " expireTaskTime = " + (expireTaskTime - actionSendStart) +
            " getClientTime = " + (getClientTime - actionSendStart) +
            " submitActionTime = " + (submitActionTime - actionSendStart));
        }
      }
    }
  } // Worker
}
