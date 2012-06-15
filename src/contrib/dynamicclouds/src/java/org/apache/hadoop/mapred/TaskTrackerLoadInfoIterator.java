package org.apache.hadoop.mapred;

import java.util.Iterator;
import java.util.List;

public abstract class TaskTrackerLoadInfoIterator
        implements Iterator<TaskTrackerLoadInfo> {

  protected List<TaskTrackerLoadInfo> trackers = null;

  public TaskTrackerLoadInfoIterator() {
  }

  public void setTrackers(List<TaskTrackerLoadInfo> trackers) {
    this.trackers = trackers;
  }
}
