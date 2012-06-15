package org.apache.hadoop.mapred;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalDirAllocator;
import org.apache.hadoop.mapred.TaskTracker.ShuffleServerMetrics;

/**
 * Useful attributes for the Netty HTTP server.
 */
@SuppressWarnings("deprecation")
public class NettyMapOutputAttributes {
  /** Job conf */
  private final JobConf jobConf;
  /** Task tracker object */
  private final TaskTracker taskTracker;
  /** Local file system */
  private final FileSystem localFS;
  /** From maprd.local.dir */
  private final LocalDirAllocator localDirAllocator;
  /** Metrics tracked by the task tracker */
  private final ShuffleServerMetrics shuffleServerMetrics;

  public NettyMapOutputAttributes(
      JobConf jobConf, TaskTracker taskTracker, FileSystem localFS,
      LocalDirAllocator localDirAllocator,
      ShuffleServerMetrics shuffleServerMetrics) {
    this.taskTracker = taskTracker;
    this.localFS = localFS;
    this.localDirAllocator = localDirAllocator;
    this.jobConf = jobConf;
    this.shuffleServerMetrics = shuffleServerMetrics;
  }

  public TaskTracker getTaskTracker() {
    return taskTracker;
  }

  public FileSystem getLocalFS() {
    return localFS;
  }

  public LocalDirAllocator getLocalDirAllocator() {
    return localDirAllocator;
  }

  public JobConf getJobConf() {
    return jobConf;
  }

  public ShuffleServerMetrics getShuffleServerMetrics() {
    return shuffleServerMetrics;
  }
}
