package org.apache.hadoop.mapred;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.corona.ResourceGrant;
import org.apache.hadoop.corona.ResourceRequest;
import org.apache.hadoop.corona.SessionDriver;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapred.CoronaStateUpdate.TaskLaunch;
import org.apache.hadoop.mapred.CoronaStateUpdate.TaskStatusUpdate;
import org.apache.hadoop.mapred.CoronaStateUpdate.TaskTimeout;

/**
 * Holds update of remote CoronaJobTracker sent to local one. Used by remote JT
 * to restore its state after failure
 */
@SuppressWarnings("deprecation")
public class CoronaJTState implements Writable {
  /** Logger */
  public static final Log LOG = LogFactory.getLog(CoronaJTState.class);

  /** Updates in the same order as received */
  List<CoronaStateUpdate> updates = new ArrayList<CoronaStateUpdate>();
  /** Session id */
  private String sessionId = "";
  /** The number of remote job tracker failover executed*/
  int restartNum = 0;
  
  public void setRestartNum(int restartNum) {
    this.restartNum = restartNum;
  }
  
  public void setSessionId(String sessionId) {
    this.sessionId = sessionId;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    WritableUtils.writeVInt(out, updates.size());
    for (CoronaStateUpdate update : updates) {
      update.write(out);
    }
    Text.writeString(out, sessionId);
    WritableUtils.writeVInt(out, restartNum);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    updates.clear();
    int size = WritableUtils.readVInt(in);
    for (int i = 0; i < size; ++i) {
      CoronaStateUpdate update = new CoronaStateUpdate();
      update.readFields(in);
      updates.add(update);
    }
    sessionId = Text.readString(in);
    restartNum = WritableUtils.readVInt(in);
  }

  /**
   * Add state update to state
   * @param update update to add
   */
  public void add(CoronaStateUpdate update) {
    updates.add(update);
  }

  /**
   * Prepares saved state for new JT
   * @return CoronaJTState prepared to be consumed by restarting JT
   */
  public CoronaJTState prepare() {
    Collections.sort(updates);
    return this;
  }

  /**
   * Creates pretty report of saved state
   * @return string with report
   */
  public String getPrettyReport(JobID jobId) {
    Map<TaskAttemptID, TaskLaunch> lastLaunch =
        new HashMap<TaskAttemptID, CoronaStateUpdate.TaskLaunch>();
    Map<TaskAttemptID, TaskStatus.State> lastKnownStatus =
        new HashMap<TaskAttemptID, TaskStatus.State>();
    JTFailoverMetrics jtFailoverMetrics = new JTFailoverMetrics();
    
    for (CoronaStateUpdate update : updates) {
      if (update.getTaskLaunch() != null) {
        TaskLaunch launch = update.getTaskLaunch();
        lastLaunch.put(launch.getTaskId(), launch);
      } else if (update.getTaskStatus() != null) {
        TaskStatus status = update.getTaskStatus();
        lastKnownStatus.put(status.getTaskID(), status.getRunState());
        jtFailoverMetrics.update(status);
      }
    }
    StringBuilder result = new StringBuilder();
    result.append("CoronaJTState report");
    if (jobId != null) {
      result.append(" for job ").append(jobId);
    }
    for (CoronaStateUpdate update : updates) {
      TaskLaunch launch = update.getTaskLaunch();
      if (launch != null) {
        result.append("\n").append(launch).append(" last known ");
        result.append(lastKnownStatus.get(launch.getTaskId()));
      }
    }
    if (sessionId != null && !sessionId.isEmpty()) {
      result.append("\n Session id ").append(sessionId);
    }
    result.append("\nThis remoteJobTracker failover totally saved: ");
    result.append("\nmappers ").append(jtFailoverMetrics.savedMappers).
      append(" map cpu ").append(jtFailoverMetrics.savedMapCPU).
      append(" map wallclock ").append(jtFailoverMetrics.savedMapWallclock);
    result.append("\nreducers ").append(jtFailoverMetrics.savedReducers).
      append(" reduce cpu ").append(jtFailoverMetrics.savedReduceCPU).
      append(" reduce wallclock ").append(jtFailoverMetrics.savedReduceWallclock);
    return result.toString();
  }

  @Override
  public String toString() {
    return getPrettyReport(null);
  }
  
  public static class JTFailoverMetrics {
    int savedMappers = 0;
    int savedReducers = 0;
    long savedMapCPU = 0L;
    long savedReduceCPU = 0L;
    long savedMapWallclock = 0L;
    long savedReduceWallclock = 0L;
    int restartNum = 0;
    long fetchStateCost = 0L;
    
    public void update(TaskStatus status) {
      if (status.getRunState() != TaskStatus.State.COMMIT_PENDING &&
          status.getRunState() != TaskStatus.State.SUCCEEDED) {
        return;
      }
      
      if (status.getIsMap()) {
        savedMappers += 1;
        savedMapCPU += 
            status.getCounters().getCounter(Task.Counter.CPU_MILLISECONDS);
        savedMapWallclock += 
            status.getCounters().getCounter(Task.Counter.MAP_TASK_WALLCLOCK);
      } else {
        savedReducers += 1;
        savedReduceCPU += 
            status.getCounters().getCounter(Task.Counter.CPU_MILLISECONDS);
        savedReduceWallclock +=
            status.getCounters().getCounter(Task.Counter.REDUCE_TASK_WALLCLOCK);
      }
    }
  }

  /**
   * This class defines how state updates are sent to local JT from remote one.
   */
  public static class Submitter {
    /** Attempt id of this task tracker */
    private TaskAttemptID jtAttemptId;
    /** Destination where status updates will be saved */
    InterCoronaJobTrackerProtocol localJT;
    /** Information pending processing and sending */
    private LinkedBlockingQueue<CoronaStateUpdate> pendingProcessing;
    /** Indicates whether submitting thread is running */
    private volatile boolean running = true;
    /** Submitting thread */
    private Thread submitterThread;

    /**
     * Creates submitter that discards all state updates
     */
    public Submitter() {
    }

    /**
     * Creates submitter of status updates to given destination.
     * @param localJT destination of submits
     * @param jtAttemptId attempt id of job tracker running this submitter
     */
    public Submitter(InterCoronaJobTrackerProtocol localJT,
        TaskAttemptID jtAttemptId,
        JobConf conf) {
      pendingProcessing = new LinkedBlockingQueue<CoronaStateUpdate>();
      this.localJT = localJT;
      this.jtAttemptId = jtAttemptId;
      submitterThread = new Thread(new AsyncSubmitter(conf));
      submitterThread.start();
    }

    /**
     * Determines whether submitter can send updates to it's destination
     * @return true iff updates can be sent
     */
    public boolean canSubmit() {
      return (localJT != null);
    }

    /**
     * Submits state update to destination. This call can delay sending of
     * update depending on its type.
     * @param launch task launch event
     * @throws IOException
     */
    public void submit(TaskLaunch launch) throws IOException {
      if (localJT == null || launch == null)
        return;
      try {
        // We're sending TaskLaunch updates synchronously
        localJT.pushCoronaJobTrackerStateUpdate(jtAttemptId,
            new CoronaStateUpdate[] { new CoronaStateUpdate(launch) });
        // pendingProcessing.offer(launch);
      } catch (IOException e) {
        LOG.error("Failed to push update, failing submitter", e);
        close();
      }
    }

    /**
     * Submits tracker status update. This call can delay sending of
     * update depending on its type.
     * @param tracker task tracker status to generate update from
     * @throws IOException
     */
    public void submit(TaskTrackerStatus tracker) throws IOException {
      if (localJT == null || tracker == null)
        return;
      pendingProcessing.offer(new CoronaStateUpdate(tracker));
    }

    /**
     * Submits task status update. This call can delay sending of update
     * depending on its type.
     * @param status task status to generate update from
     * @throws IOException
     */
    public void submit(TaskStatus status) throws IOException {
      if (localJT == null || status == null)
        return;
      if (TaskStatus.TERMINATING_STATES.contains(status.getRunState())
          || TaskStatus.State.COMMIT_PENDING.equals(status.getRunState())) {
        pendingProcessing.offer(new CoronaStateUpdate(status));
      }
    }

    /**
     * Submits task status update. This call can delay sending of update
     * depending on its type.
     * @param timeout TaskTimout update to save
     */
    public void submit(TaskTimeout timeout) {
      if (localJT == null || timeout == null)
        return;
      pendingProcessing.offer(new CoronaStateUpdate(timeout));
    }

    /**
     * Closes submitter
     */
    public void close() {
      running = false;
      if (submitterThread != null) {
        submitterThread.interrupt();
        try {
          submitterThread.join();
        } catch (InterruptedException e) {
        }
      }
      jtAttemptId = null;
      localJT = null;
      pendingProcessing = null;
    }

    /**
     * Thread that asynchronously process and submits state updates
     */
    private class AsyncSubmitter implements Runnable {
      /** Max processed pending updates per batch */
      private static final int MAX_BATCH_UPDATES_DEFAULT= 1000;
      /** Keeps track of the most recent tracker info */
      private Map<String, TaskTrackerInfo> trackerToInfo =
          new HashMap<String, TaskTrackerInfo>();
      /** The configure key for RJT to update the state to
       * local job tracker*/
      public static final String MAX_BATCH_UPDATES_SIZE =
          "corona.jt.state.max.batch.updates.size";
      /** The configure key for the wait timeout value when RJT updating 
       * the state before getting the max batch update size in 
       * millis*/
      public static final String MAX_BATCH_UPDATES_WAITTIME =
          "corona.jt.state.batch.update.waittime";
      private static final long MAX_BATCH_UPDATES_WAITTIME_DEFAULT = 1L;
      
      private long batchUpdateTimeout;
      private int maxBatchUpdateSize;
      
      public AsyncSubmitter(JobConf conf) {
        maxBatchUpdateSize = conf.getInt(
            MAX_BATCH_UPDATES_SIZE, MAX_BATCH_UPDATES_DEFAULT);
        batchUpdateTimeout = conf.getLong(
            MAX_BATCH_UPDATES_WAITTIME,
            MAX_BATCH_UPDATES_WAITTIME_DEFAULT);
      }

      @Override
      public void run() {
        List<CoronaStateUpdate> toSend = new ArrayList<CoronaStateUpdate>(
            maxBatchUpdateSize);
        while (running) {
          for (int updates = 0; updates < maxBatchUpdateSize; updates++) {
            CoronaStateUpdate update;
            try {
              if (toSend.isEmpty()) {
                // We're waiting for anything to send
                update = pendingProcessing.take();
              } else {
                // We have things to send, but lets wait for a short time
                // Pushing every update will introduce bigger lag in this thread
                // than this wait, and more updates can get lost (are sync
                // pending)
                update = pendingProcessing.poll(batchUpdateTimeout, TimeUnit.MILLISECONDS);
              }
            } catch (InterruptedException e) {
              // Check running flag, we don't want to loose updates, so this
              // goes through sending code
              break;
            }
            if (update == null) {
              break;
            }
            Object obj = update.get();
            // Classify different objects
            if (obj instanceof TaskLaunch) {
              // Launching task, no preprocessing
              toSend.add(update);
            } else if (obj instanceof TaskTrackerStatus) {
              TaskTrackerStatus tracker = (TaskTrackerStatus) obj;
              String trackerName = tracker.getTrackerName();
              // Send new TaskTrackerInfo update only if has changed
              TaskTrackerInfo info = TaskTrackerInfo.fromStatus(tracker);
              TaskTrackerInfo savedInfo = trackerToInfo.get(trackerName);
              if (savedInfo == null || !savedInfo.equals(info)) {
                trackerToInfo.put(trackerName, info);
                update.set(info);
                toSend.add(update);
              }
            } else if (obj instanceof TaskStatus) {
              TaskStatus report = (TaskStatus) obj;
              // Encapsulate to provide tracker name
              update.set(new TaskStatusUpdate(report));
              toSend.add(update);
            } else if (obj instanceof TaskTimeout) {
              // Timed out running or launching task
              toSend.add(update);
            } else {
              LOG.error("Unknown type of update");
            }
          }
          // Send batch
          if (!toSend.isEmpty()) {
            try {
              localJT.pushCoronaJobTrackerStateUpdate(jtAttemptId, toSend
                  .toArray(CoronaStateUpdate.EMPTY_ARRAY));
              LOG.info("Batch of " + toSend.size() + " updates sent.");
              toSend.clear();
            } catch (IOException e) {
              LOG.error("Failed to push updates", e);
              close();
            }
          }
        }
        LOG.info("AsyncSubmitter exiting.");
      }

    }

  }

  /**
   * Fetches and serves queries for saved state, not designed for concurrent
   * access
   */
  public static class Fetcher {
    /** Id of session saved with this state */
    private String sessionId;
    /** List of updates in the same order as submitted to local JT */
    private List<CoronaStateUpdate> updates;
    /** Maps tracker name to TaskTrackerInfo */
    private Map<String, TaskTrackerInfo> trackerToInfo =
        new HashMap<String, TaskTrackerInfo>();
    /** Clock used for restoring proper timestamps in JT */
    private RestoringClock clock;
    /** The metrics to record the impact of RJT failover*/
    JTFailoverMetrics jtFailoverMetrics =
        new JTFailoverMetrics();
    /** The known trackers **/
    private Set<String> taskLaunchTrackers =
        new HashSet<String>();
    
    /**
     * Creates empty fetcher (which state can't be filled)
     */
    public Fetcher() {
    }

    /**
     * When restoring JT status after restarting, it's possible that we have
     * several task attempts that were using the same grant. Only the most
     * recent task attempt is still using this grant, All finished restored
     * attempts should declare null grant. The last launched attempt for each
     * given grant is the attempt assumed to be running using this grant, rest
     * attempts must declare null grant.
     * @param parent local JT to fetch state from
     * @param jtAttemptId task attempt id of job tracker running this fetcher
     */
    public Fetcher(InterCoronaJobTrackerProtocol parent,
        TaskAttemptID jtAttemptId) {
      CoronaJTState state;
      long startFetchingTime = System.currentTimeMillis();
      
      try {
        state = parent.getCoronaJobTrackerState(jtAttemptId);
      } catch (IOException e) {
        LOG.error("Error when fetching state from parent JT. Proceeding with"
            + " cleared state. ", e);
        close();
        return;
      }
      // State parts
      this.sessionId = state.sessionId;
      this.updates = Collections.unmodifiableList(state.updates);
      this.jtFailoverMetrics.restartNum = state.restartNum;
      
      for (Iterator<CoronaStateUpdate> iter = updates.iterator();
          iter.hasNext();) {
        CoronaStateUpdate update = iter.next();
        // Process task status updates for queries,
        // preserve order for each tracker
        TaskStatus status = update.getTaskStatus();
        if (status != null) {
          jtFailoverMetrics.update(status);
          continue;
        }
        // Set non-existing grants in every attempt, prepare mapping from
        // grant to last attempt that was using this grant
        TaskLaunch launch = update.getTaskLaunch();
        if (launch != null) {
          Integer grant = launch.getGrantId();
          // assign non-existing grant, we will kill all the unfinished tasks
          launch.setGrantId(ResourceTracker.getNoneGrantId());
          taskLaunchTrackers.add(launch.getTrackerName());
          continue;
        }
        // Save tracker info for replaying task status
        TaskTrackerInfo info = update.getTrackerInfo();
        if (info != null) {
          trackerToInfo.put(info.getTrackerName(), info);
          continue;
        }
      }
      trackerToInfo = Collections.unmodifiableMap(trackerToInfo);
      taskLaunchTrackers = Collections.unmodifiableSet(taskLaunchTrackers);
      
      jtFailoverMetrics.fetchStateCost = System.currentTimeMillis() - startFetchingTime;
      LOG.info(jtFailoverMetrics.fetchStateCost + " milliseconds used to do state fetching");
    }

    /**
     * Returns saved session if any
     * @return saved session id or null
     */
    public String getSessionId() {
      if (sessionId == null || sessionId.isEmpty()) {
        return null;
      }
      return sessionId;
    }

    /**
     * Determines whether tasks state has been restored
     * @return true iff tasks state has been restored
     */
    public boolean hasTasksState() {
      return (sessionId != null && updates != null);
    }

    /**
     * Wipes out all state
     */
    public void close() {
      sessionId = null;
      updates = null;
      taskLaunchTrackers = null;
      trackerToInfo = null;
      clock = null;
    }

    /**
     * Restores fetched state updates in the same order that they were saved
     * @param remoteJT JobTracekr to restore state
     */
    public void restoreState(StateRestorer remoteJT) {
      if (!hasTasksState())
        return;
      Clock oldClock = remoteJT.getClock();
      clock = new RestoringClock();
      remoteJT.setClock(clock);
      LOG.info("Begin to restoreState");
      long restoreTime = oldClock.getTime();
      
      for (Iterator<CoronaStateUpdate> iter = updates.iterator();
          iter.hasNext();) {
        CoronaStateUpdate update = iter.next();
        clock.setTimestamp(update.getTimestamp());
        LOG.info("Current timestamp " + update.getTimestamp());
        TaskStatus status = update.getTaskStatus();
        if (status != null) {
          TaskTrackerInfo info = trackerToInfo.get(status.getTaskTracker());
          if (info != null) {
            remoteJT.restoreTaskStatus(status, info);
            LOG.info("Restoring status " + status + " @ " + info);
          } else {
            // it is safe for us to kill more uncertain tasks
            LOG.error("Skipping status " + status + " because of null TaskTracker info");
          }
          continue;
        }
        TaskLaunch launch = update.getTaskLaunch();
        if (launch != null) {
          LOG.info("Restoring launch " + launch);
          remoteJT.restoreTaskLaunch(launch);
          continue;
        }
        TaskTimeout timeout = update.getTaskTimeout();
        if (timeout != null) {
          String trackerName = timeout.getTrackerName();
          LOG.info("Restoring timeout on " + trackerName);
          remoteJT.restoreTaskTimeout(trackerName);
          continue;
        }
      }
      long restoreCost = oldClock.getTime() - restoreTime;
      
      LOG.info("End the restoreState, totally " + restoreCost + "milliseconds used.");
      remoteJT.setClock(oldClock);
    }
    
    /**
     * Returns a set of task trackers that was in use during previous remote JT
     * life
     * @return set of task tracker's names
     */
    public Set<String> getTaskLaunchTrackers() {
      return taskLaunchTrackers;
    }
  }

  /**
   * Clock that allows us to restore time as saved with status updates during
   * restarting process
   */
  public static class RestoringClock extends Clock {
    /** Current manually set timestamp */
    private volatile long timestamp;
    /** Determines whether we're using real or manually set timesamps */
    private volatile boolean useRealTimestamps = false;

    /**
     * Switches to using real timestamps
     */
    public void useRealTimestamps() {
      useRealTimestamps = true;
    }

    /**
     * Sets current timestamp
     * @param timestamp time to set
     */
    public void setTimestamp(long timestamp) {
      useRealTimestamps = false;
      this.timestamp = timestamp;
    }

    @Override
    public long getTime() {
      if (useRealTimestamps) {
        return super.getTime();
      } else {
        return timestamp;
      }
    }
  }

  /**
   * Contract between remote JT and Fetcher defining functions for restoring
   * state
   */
  public interface StateRestorer {

    /**
     * Set clock in object that restores it's state
     * @param clock clock to use
     */
    public void setClock(Clock clock);

    /**
     * Get clock being used in object that restores it's state
     * @return clock used currently by state restorer
     */
    public Clock getClock();

    /**
     * Restores task timeout event for provided task tracker
     * @param trackerName
     */
    public void restoreTaskTimeout(String trackerName);

    /**
     * Restore task launch
     * @param launch a TaskLaunch
     */
    public void restoreTaskLaunch(TaskLaunch launch);

    /**
     * Restore task status update saved from heartbeat report
     * @param status a TaskStatus
     * @param tracker a TaskStatusInfo of tracker that sent this update
     */
    public void restoreTaskStatus(TaskStatus status, TaskTrackerInfo tracker);
  }

}
