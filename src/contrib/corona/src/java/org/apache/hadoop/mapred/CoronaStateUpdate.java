package org.apache.hadoop.mapred;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.net.InetSocketAddress;

import org.apache.hadoop.corona.ResourceGrant;
import org.apache.hadoop.io.GenericWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapred.CoronaSessionInfo.InetSocketAddressWritable;

/**
 * Describes one piece of remote JT state in Corona
 */
@SuppressWarnings("deprecation")
public class CoronaStateUpdate extends GenericWritable implements
    Comparable<CoronaStateUpdate> {
  /** Empty array */
  public static final CoronaStateUpdate[] EMPTY_ARRAY =
      new CoronaStateUpdate[0];
  /** Possible types of updates saved for restoring */
  private static Class[] CLASSES = { TaskStatusUpdate.class, TaskLaunch.class,
      TaskTrackerInfo.class, TaskTrackerStatus.class, MapTaskStatus.class,
      ReduceTaskStatus.class, TaskTimeout.class, };

  @Override
  protected Class[] getTypes() {
    return CLASSES;
  }

  /** Current time clock */
  private static Clock clock = new Clock();
  /** When this update was generated */
  private long timestamp;

  public long getTimestamp() {
    return timestamp;
  }

  /**
   * Returns enclosed object casted on given class, if enclosed object is not
   * instance of given class returns null.
   * @param clazz class to cast on
   * @return casted object or null
   */
  private <T> T get(Class<T> clazz) {
    try {
      return clazz.cast(get());
    } catch (ClassCastException e) {
      return null;
    }
  }

  /** Returns TaskStatus if this update carries one, null otherwise */
  public TaskStatus getTaskStatus() {
    TaskStatusUpdate update = get(TaskStatusUpdate.class);
    if (update == null) {
      return null;
    }
    return update.getStatus();
  }

  /** Returns TaskLaunch if this update carries one, null otherwise */
  public TaskLaunch getTaskLaunch() {
    return get(TaskLaunch.class);
  }

  /** Returns TaskTrackerInfo if this update carries one, null otherwise */
  public TaskTrackerInfo getTrackerInfo() {
    return get(TaskTrackerInfo.class);
  }

  /** Returns TaskTimeout if this update carries one, null otherwise */
  public TaskTimeout getTaskTimeout() {
    return get(TaskTimeout.class);
  }

  /** Writable c'tor */
  public CoronaStateUpdate() {
  }

  /** C'tor that encapsulates value */
  private CoronaStateUpdate(Writable instance) {
    set(instance);
    this.timestamp = clock.getTime();
  }

  /** C'tor that encapsulates value */
  public CoronaStateUpdate(TaskStatusUpdate update) {
    this((Writable) update);
  }

  /** C'tor that encapsulates value */
  public CoronaStateUpdate(TaskLaunch update) {
    this((Writable) update);
  }

  /** C'tor that encapsulates value */
  public CoronaStateUpdate(TaskTrackerInfo update) {
    this((Writable) update);
  }

  /** C'tor that encapsulates value */
  public CoronaStateUpdate(TaskTimeout update) {
    this((Writable) update);
  }

  /**
   * Encapsulates value, this type of update is not to be serialized
   */
  public CoronaStateUpdate(TaskTrackerStatus update) {
    this((Writable) update);
  }

  /**
   * Encapsulates value, this type of update is not to be serialized
   */
  public CoronaStateUpdate(TaskStatus update) {
    this((Writable) update);
  }

  @Override
  public void write(DataOutput out) throws IOException {
    super.write(out);
    WritableUtils.writeVLong(out, timestamp);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    super.readFields(in);
    timestamp = WritableUtils.readVLong(in);
  }

  @Override
  public int compareTo(CoronaStateUpdate o) {
    return Long.valueOf(this.timestamp).compareTo(o.timestamp);
  }

  /** Describes timeout of single task */
  public static class TaskTimeout implements Writable {
    /** Name of task tracker */
    private String trackerName;

    public String getTrackerName() {
      return trackerName;
    }

    /**
     * Writable c'tor
     */
    public TaskTimeout() {
    }

    /**
     * Creates timeout event for given task tracker
     * @param trackerName name of task tracker that timed out during launching
     *        or running task
     */
    public TaskTimeout(String trackerName) {
      this.trackerName = trackerName;
    }

    @Override
    public void write(DataOutput out) throws IOException {
      WritableUtils.writeString(out, trackerName);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      trackerName = WritableUtils.readString(in);
    }

  }

  /** Describes launching single task attempt */
  public static class TaskLaunch implements Writable {
    /** Empty array */
    public static final TaskLaunch[] EMPTY_ARRAY = new TaskLaunch[0];

    /** Task id */
    private TaskAttemptID taskId;
    /** Name of tracker */
    private String trackerName;
    /** Tracker RPC address */
    private InetSocketAddress address;
    /** Grant in use */
    private Integer grantId;

    public Integer getGrantId() {
      return grantId;
    }

    public void setGrantId(Integer grantId) {
      this.grantId = grantId;
    }

    public String getTrackerName() {
      return trackerName;
    }

    public TaskAttemptID getTaskId() {
      return taskId;
    }

    public InetSocketAddress getAddress() {
      return address;
    }

    /** Default c'tor for writable */
    public TaskLaunch() {
      taskId = new TaskAttemptID();
      trackerName = "";
    }

    /**
     * C'tor for normal use
     * @param taskId task attempt id
     * @param trackerName name of tracker
     * @param tip task in progress
     * @param grant grant used for launching this task attempt
     */
    public TaskLaunch(TaskAttemptID taskId, String trackerName,
        InetSocketAddress trackerAddr, TaskInProgress tip,
        ResourceGrant grant) {
      this.taskId = taskId;
      this.trackerName = trackerName;
      this.address = trackerAddr;
      this.grantId = grant.getId();
    }

    @Override
    public void write(DataOutput out) throws IOException {
      taskId.write(out);
      WritableUtils.writeString(out, trackerName);
      InetSocketAddressWritable.writeAddress(out, address);
      WritableUtils.writeVInt(out, grantId);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      this.taskId.readFields(in);
      this.trackerName = WritableUtils.readString(in);
      this.address = InetSocketAddressWritable.readAddress(in);
      this.grantId = WritableUtils.readVInt(in);
    }

    @Override
    public String toString() {
      StringBuilder result = new StringBuilder();
      result.append(taskId.toString()).append(" @ ").append(trackerName);
      result.append(" using grant ").append(grantId);
      return result.toString();
    }

  }

  /** Encapsulates single task status to provide information about tracker */
  public static class TaskStatusUpdate implements Writable {
    /** Real task status object */
    private TaskStatus status;
    /** Tracker name */
    private String trackerName;

    public TaskStatus getStatus() {
      status.setTaskTracker(trackerName);
      return status;
    }

    /**
     * C'tor for writable
     */
    public TaskStatusUpdate() {
    }

    /**
     * Encapsulates status that has tracker name properly set
     * @param status status to encapsulate
     */
    public TaskStatusUpdate(TaskStatus status) {
      this.status = status;
      this.trackerName = status.getTaskTracker();
    }

    @Override
    public void write(DataOutput out) throws IOException {
      TaskStatus.writeTaskStatus(out, status);
      WritableUtils.writeString(out, trackerName);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      status = TaskStatus.readTaskStatus(in);
      trackerName = WritableUtils.readString(in);
    }

  }

}
