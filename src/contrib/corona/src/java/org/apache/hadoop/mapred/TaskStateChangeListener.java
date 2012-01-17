package org.apache.hadoop.mapred;

public interface TaskStateChangeListener {
  public void taskStateChange(TaskStatus.State state, TaskInProgress tip,
      TaskAttemptID taskid);
}
