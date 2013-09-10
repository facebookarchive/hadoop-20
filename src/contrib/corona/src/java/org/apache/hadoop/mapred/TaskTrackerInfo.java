package org.apache.hadoop.mapred;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

/**
 * Holds information about task tracker obtained from TaskTrackerStatus, that
 * are used in CoronaJobInProgress together with TaskStatus.
 * This class holds a proper subset of information stored in TaskTrackerStatus,
 * the idea is that this class stores data from TaskTrackerStatus that refer
 * to TaskTracker and is used in CoronaJobTracker and is not connected with
 * Tasks running on this TaskTracker.
 * Things that are stored in TaskTrackerStatus and are NOT present here are:
 * - TaskStatus updates for tasks running on TaskTracker
 * - TaskTrackerHealthStatus information
 * - ResourceStatus information
 * - statistic information about number of tasks (maps, reduces, released tasks)
 */
public class TaskTrackerInfo implements Writable {

  /** Name of task tracker */
  private String trackerName;
  /** Host */
  private String host;
  /** Http port */
  private int httpPort;

  public String getTrackerName() {
    return trackerName;
  }

  public String getHost() {
    return host;
  }

  public int getHttpPort() {
    return httpPort;
  }

  /**
   * Writable c'tor
   */
  public TaskTrackerInfo() {
  }

  /**
   * Extracts info from TaskTrackerStatus
   * @param status task tracker status
   */
  private TaskTrackerInfo(TaskTrackerStatus status) {
    trackerName = status.getTrackerName();
    host = status.getHost();
    httpPort = status.getHttpPort();
  }

  /**
   * Builds {@link TaskTrackerInfo} from {@link TaskTrackerStatus}, if provided
   * status is null, then returns null
   * @param status task tracker status
   * @return task tracker info
   */
  public static TaskTrackerInfo fromStatus(TaskTrackerStatus status) {
    if (status == null) {
      return null;
    } else {
      return new TaskTrackerInfo(status);
    }
  }

  @Override
  public boolean equals(Object o) {
    if (o == null || !(o instanceof TaskTrackerInfo))
      return false;
    TaskTrackerInfo info = (TaskTrackerInfo) o;
    return (trackerName.equals(info.trackerName) && host.equals(info.host)
        && httpPort == info.httpPort);
  }

  @Override
  public void write(DataOutput out) throws IOException {
    Text.writeString(out, trackerName);
    Text.writeString(out, host);
    out.writeInt(httpPort);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    this.trackerName = Text.readString(in);
    this.host = Text.readString(in);
    this.httpPort = in.readInt();
  }

  @Override
  public String toString() {
    return trackerName + " http at: " + host + ":" + httpPort;
  }

}
