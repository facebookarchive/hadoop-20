package org.apache.hadoop.mapred;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Random;
import java.util.concurrent.PriorityBlockingQueue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapred.TaskTracker.TaskInProgress;


/**
 * This class accepts tasks to 'run' and finish after a certain time.
 * No actual work is done, the task is only marked as completed after a delay.
 * The advantage of using this class is that no JVM is launched,
 * saving CPU/memory. Used only for when the task tracker is used to simulate
 * load for the job tracker.
 */
public class SimulatedTaskRunner extends Thread{
  public static final Log LOG =
      LogFactory.getLog(SimulatedTaskRunner.class);

  /**
   * Helper class for associating a TIP and the time that the TIP is supposed
   * to finish. Implements comparable so that this can be inserted into a
   * priority queue.
   */
  class TipToFinish implements Comparable<TipToFinish> {

    final TaskInProgress tip;
    final long timeToFinish;
    final TaskUmbilicalProtocol umbilicalProtocol;

    public TipToFinish(
      TaskInProgress tip,
      long timeToFinish,
      TaskUmbilicalProtocol umbilicalProtocol) {
      this.tip = tip;
      this.timeToFinish = timeToFinish;
      this.umbilicalProtocol = umbilicalProtocol;
    }

    public long getTimeToFinish() {
      return timeToFinish;
    }

    public TaskInProgress getTip() {
      return tip;
    }

    @Override
    public int compareTo(TipToFinish o) {
      long otherTimeToFinish = o.timeToFinish;
      if (this.timeToFinish < otherTimeToFinish) {
        return -1;
      } else if (this.timeToFinish > otherTimeToFinish) {
        return 1;
      } else {
        // Must be equal
        return 0;
      }
    }
    @Override
    public String toString() {
      return "<" + tip.getTask().getTaskID().toString() + "," + timeToFinish +
          ">";
    }
    @Override
    public boolean equals(Object o) {
      if (!(o instanceof TipToFinish)) {
        return false;
      }
      TipToFinish ttf = (TipToFinish)o;
      return this.timeToFinish == ttf.timeToFinish;
    }

    /**
     * Finish the tip.
     */
    public void finishTip() {
      LOG.info("Finishing TIP " + tip.getTask().getTaskID() +
        " with status " +
        tip.getStatus() + " and isTaskCleanupTask is : " +
        tip.getTask().isTaskCleanupTask() + " and phase is " +
        tip.getTask().getPhase() + " and finish time " +
        timeToFinish + " at " +
        System.currentTimeMillis());
      try {
        umbilicalProtocol.done(tip.getTask().getTaskID());
        taskTracker.cleanupUmbilical(umbilicalProtocol);
      } catch (IOException e) {
        // This shouldn't happen as done does not really throw an IOE
        LOG.fatal("Error while trying to call done on " +
          tip.getTask().getTaskID(), e);
        System.exit(-1);
      }
      tip.reportTaskFinished(false);
      LOG.debug("After finishing, " + tip.getTask().getTaskID() + " has status " +
        tip.getStatus() + " and isTaskCleanupTask is : " +
        tip.getTask().isTaskCleanupTask() + " and phase is " +
        tip.getTask().getPhase());
    }
  }

  // Upper bound on time that a task should wait before finishing
  private long timeToFinishTask = 0;
  // Random number generator for task finish times
  private Random rand = new Random();
  // Queue of tips to finish. Ordered by finished time. Must be threadsafe.
  private PriorityBlockingQueue<TipToFinish> tipQueue =
      new PriorityBlockingQueue<TipToFinish>();
  // Reference to the tracker for calling done and getting map task completion
  // events
  private TaskTracker taskTracker;
  // A mapping from the (reduce) TIP's to the mapper waiting thread. Used to
  // interrupt the mapper waiting thread in case the task gets killed
  private Map<TaskInProgress, MapperWaitThread> mapperWaitThreadMap =
      Collections.synchronizedMap(
          new HashMap<TaskInProgress, MapperWaitThread>());
  /**
   * @param timeToFinishTask task will finish randomly between 0 and this many
   * miliseconds with a uniform distribution
   * @param t the reference to the TaskTracker for calling completion methods
   */
  public SimulatedTaskRunner(long timeToFinishTask, TaskTracker t) {
    this.taskTracker = t;
    this.timeToFinishTask = timeToFinishTask;
    // If it's not a daemon thread, this might prevent the TT from exiting.
    this.setDaemon(true);
    // Name the thread something like "SimulatedTaskRunner Thread-xyz.."
    this.setName("SimulatedTaskRunner " + this.getName());
  }

  /**
   * The primary public method that should be called to 'run' a task. Handles
   * both map and reduce tasks and marks them as completed after the configured
   * time interval
   * @param tip
   */
  public void launchTask(TaskInProgress tip) throws IOException {
    LOG.info("Launching simulated task " + tip.getTask().getTaskID() +
        " for job " + tip.getTask().getJobID());
    TaskUmbilicalProtocol umbilicalProtocol = taskTracker.getUmbilical(tip);
    // For map tasks, we can just finish the task after some time. Same thing
    // with cleanup tasks, as we don't need to be waiting for mappers to finish
    if (tip.getTask().isMapTask() || tip.getTask().isTaskCleanupTask() ||
      tip.getTask().isJobCleanupTask() || tip.getTask().isJobSetupTask() ) {
      addTipToFinish(tip, umbilicalProtocol);
    } else {
      MapperWaitThread mwt =
          new MapperWaitThread(tip, this, umbilicalProtocol);
      // Save a reference to the mapper wait thread so that we can stop them if
      // the task gets killed
      mapperWaitThreadMap.put(tip, mwt);
      mwt.start();
    }

  }

  /**
   * Add the specified TaskInProgress to the priority queue of tasks to finish.
   * @param tip
   * @param umbilicalProtocol
   */
  protected void addTipToFinish(TaskInProgress tip,
                                TaskUmbilicalProtocol umbilicalProtocol) {
    long currentTime = System.currentTimeMillis();
    long finishTime = currentTime + Math.abs(rand.nextLong()) %
        timeToFinishTask;
    LOG.info("Adding TIP " + tip.getTask().getTaskID() +
        " to finishing queue with start time " +
        currentTime + " and finish time " + finishTime +
        " (" + ((finishTime - currentTime) / 1000.0) + " sec) to thread " +
        getName());
    TipToFinish ttf = new TipToFinish(tip, finishTime, umbilicalProtocol);
    tipQueue.put(ttf);
    // Interrupt the waiting thread. We could put in additional logic to only
    // interrupt when necessary, but probably not worth the complexity.
    this.interrupt();
  }


  /**
   * Continuously looks through the queue of TIP's to mark as finished,
   * finishing and sleeping as necessary. Can be interrupted while it's sleeping
   * if it needs to re-evaluate how long to sleep.
   */
  @Override
  public void run() {
    while (true) {
      // Wait to get a TIP
      TipToFinish ttf = null;
      try {
        LOG.debug("Waiting for a TIP");
        ttf = tipQueue.take();
      } catch (InterruptedException e) {
        LOG.info("Got interrupted exception while waiting to take()");
        continue;
      }
      LOG.debug(" Got a TIP " + ttf.getTip().getTask().getTaskID() +
          " at time " + System.currentTimeMillis() + " with finish time " +
          ttf.getTimeToFinish());
      // Wait until it's time to finish the task. Since the TIP was pulled from
      // the priority queue, this should be the first task in the queue that
      // needs to be finished. If we get interrupted, that means that it's
      // possible that we added a TIP that should finish earlier
      boolean interrupted = false;
      while (true) {
        long currentTime = System.currentTimeMillis();
        if (currentTime < ttf.getTimeToFinish()) {
          try {
            long sleepTime = ttf.getTimeToFinish() - currentTime;
            LOG.debug("Sleeping for " + sleepTime + " ms");
            Thread.sleep(sleepTime);
          } catch (InterruptedException e) {
            LOG.debug("Finisher thread was interrupted", e);
            interrupted = true;
            break;
          }
        } else {
          break;
        }
      }

      // Wait was interrupted, then it could mean that we added a task
      // that needs to finish sooner. Put that task back and start again
      if (interrupted) {
        LOG.info("Putting back TIP " + ttf.getTip().getTask().getTaskID() +
            " for job " + ttf.getTip().getTask().getJobID());
        tipQueue.put(ttf);
        continue;
      }

      // Finish the task
      TaskInProgress tip = ttf.getTip();
      ttf.finishTip();

      // Also clean up the mapper wait thread map for reducers. It should exist
      // for reduce tasks that are not cleanup tasks
      if (!tip.getTask().isMapTask() &&
          !tip.getTask().isTaskCleanupTask() &&
          !tip.getTask().isJobCleanupTask() &&
          !tip.getTask().isJobSetupTask()) {
        if (!mapperWaitThreadMap.containsKey(tip)) {
          throw new RuntimeException("Unable to find mapper wait thread for " +
              tip.getTask().getTaskID() + " job " + tip.getTask().getJobID());
        }
        LOG.debug("Removing mapper wait thread for " +
            tip.getTask().getTaskID() + " job " + tip.getTask().getJobID());
        mapperWaitThreadMap.remove(tip);
      } else if (mapperWaitThreadMap.containsKey(tip)) {
        throw new RuntimeException("Mapper wait thread exists for" +
            tip.getTask().getTaskID() + " job " + tip.getTask().getJobID() +
            " when it shouldn't!");
      }
    }
  }

  /**
   * @param tip the TaskInProgress to remove from the queue of TIP's that need
   * to be finished.
   */
  private void removeFromFinishingQueue(TaskInProgress tip) {
    LOG.debug("Removing " + tip.getTask().getTaskID() +
        " from finishig queue");
    tipQueue.remove(tip);
  }

  /**
   * Called in case the task needs to be killed. Canceling will kill any map
   * wait threads and also remove it from the queue of tasks that should be
   * marked as finished.
   * @param tip the killed TaskInProgress
   */
  public void cancel(TaskInProgress tip) {
    LOG.info("Canceling task "  + tip.getTask().getTaskID() + " of job " +
        tip.getTask().getJobID());
    // Cancel & remove the map completion finish thread for reduce tasks.
    if (!tip.getTask().isMapTask() && !tip.getTask().isTaskCleanupTask()) {
      if (!mapperWaitThreadMap.containsKey(tip)) {
        throw new RuntimeException("Mapper wait thread doesn't exist " +
            "for " + tip.getTask().getTaskID());
      }
      LOG.debug("Interrupting mapper wait thread for " +
          tip.getTask().getTaskID() + " job " +
          tip.getTask().getJobID());
      mapperWaitThreadMap.get(tip).interrupt();
      LOG.debug("Removing mapper wait thread for " +
          tip.getTask().getTaskID() + " job " + tip.getTask().getJobID());
      mapperWaitThreadMap.remove(tip);
    } else {
      LOG.debug(tip.getTask().getTaskID() + " is not a reduce task, so " +
          "not canceling mapper wait thread");
    }
    removeFromFinishingQueue(tip);
  }
}
