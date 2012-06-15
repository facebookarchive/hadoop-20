/**
 *
 */
package org.apache.hadoop.mapred;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapred.TaskTracker.TaskInProgress;

/**
 * A thread that waits for all the mappers for the given reduce task to finish
 * before adding itself to the supplied SimulatedTaskRunner. While
 * waiting, this thread also updates the task status so that they don't get
 * killed due to inactivity.
 */
public class MapperWaitThread extends Thread {
  public static final Log LOG =
      LogFactory.getLog(MapperWaitThread.class);
  private TaskUmbilicalProtocol umbilicalProtocol;
  private SimulatedTaskRunner taskRunner;
  private TaskInProgress tip;

  // Max map completion events to fetch in one go from the tasktracker
  private static final int MAX_EVENTS_TO_FETCH = 10000;
  // Time to wait between fetches.
  private static final int SLEEP_TIME = 2000;

  /**
   * @param tip a reduce task in progress that we should wait for the mappers
   * to finish
   * @param taskRunner the task runner thread that the TIP should be sent
   * to after all the mappers are done.
   * @param umbilicalProtocol The umbilical
   * events
   */
  public MapperWaitThread(TaskInProgress tip,
      SimulatedTaskRunner taskRunner, TaskUmbilicalProtocol umbilicalProtocol) {
    this.taskRunner = taskRunner;
    this.umbilicalProtocol = umbilicalProtocol;
    this.tip = tip;
    this.setName("Map-waiting thread for job: " + tip.getTask().getJobID() +
        " reduce task: " + tip.getTask().getTaskID());
    // Don't want to prevent the TT from shutting down just because of this
    // thread
    this.setDaemon(true);
  }

  /**
   * Updates status / fetches map completion events until it gets them for
   * all the mappers. Adds task to finish afterward.
   */
  @Override
  public void run() {
    try {
      if (tip.getTask().isMapTask()) {
        throw new RuntimeException("Only works for reducers!");
      }

      ReduceTask reduceTask = (ReduceTask) tip.getTask();

      LOG.info("MapperWaitThread started for reduce task " +
          reduceTask.getTaskID());
      int successfulMapCompletions = 0;
      int getFromEventId = 0;

      // Wait for the mappers in a loop
      while (successfulMapCompletions < reduceTask.getNumMaps()) {
        LOG.debug("Job: " + reduceTask.getJobID() + " ReduceTask: " +
            reduceTask.getTaskID() + " Got Successful Maps: " +
            successfulMapCompletions + "/" + reduceTask.getNumMaps());
        try {
          // This gets whether the mappers finished and also the location of the
          // output
          MapTaskCompletionEventsUpdate updates =
              umbilicalProtocol.getMapCompletionEvents(reduceTask.getJobID(), getFromEventId,
                  MAX_EVENTS_TO_FETCH, reduceTask.getTaskID());
          TaskCompletionEvent [] completionEvents =
              updates.getMapTaskCompletionEvents();

          if (updates.shouldReset()) {
            getFromEventId = 0;
            successfulMapCompletions = 0;
          }

          // Increment to get the next set of updates
          LOG.debug("Job: " + reduceTask.getJobID() + " ReduceTask: " +
              reduceTask.getTaskID() + " Got " +
              completionEvents.length + " map task " +
              " completion events");

          getFromEventId += completionEvents.length;

          // Tally up all the successful maps
          for(TaskCompletionEvent t : completionEvents) {
            if (t.getTaskStatus() == TaskCompletionEvent.Status.SUCCEEDED) {
              successfulMapCompletions++;
            }
          }

          // Update the progress of the threads so that they don't get killed
          // for inactivity
          umbilicalProtocol.statusUpdate(tip.getTask().getTaskID(), tip.getStatus());
          // If the thread were interrupted, then it means that we need to stop
          // as the task was killed
          if (Thread.interrupted()) {
            throw new InterruptedException("Generated in loop");
          }
          if (successfulMapCompletions % MAX_EVENTS_TO_FETCH == 0) {
            Thread.sleep(SLEEP_TIME);
          }

        } catch (IOException e) {
          LOG.error("Got an exception while getting map completion events", e);
        } catch (InterruptedException e) {
          LOG.debug("Got an interrupted exception while waiting for mappers  " +
              "for " + tip.getTask().getTaskID() + " job " +
              tip.getTask().getJobID());
          return;
        }
      }

      // All the mappers are done, so we can finish the reduce task
      LOG.info("Job: " + reduceTask.getJobID() + " ReduceTask: " +
          reduceTask.getTaskID() + " All maps finished, adding to task to " +
          "finish");
      taskRunner.addTipToFinish(tip, umbilicalProtocol);
    } finally {
      LOG.info("Exiting mapper wait thread " +
          "for " + tip.getTask().getTaskID() + " job " +
          tip.getTask().getJobID());
    }
  }
}
