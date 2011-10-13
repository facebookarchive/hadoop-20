package org.apache.hadoop.mapred;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TaskTracker;

public class MultiTaskTracker {

  public static void main(String[] args) throws IOException {
    int numTaskTrackers = Integer.parseInt(args[0]);
    Configuration conf = new Configuration();
    JobConf jConf = new JobConf(conf);
    jConf.set("mapred.task.tracker.http.address", "0.0.0.0:0");
    String[] baseLocalDirs = jConf.getLocalDirs();
    List<String> localDirs = new LinkedList<String>();
    List<TaskTrackerRunner> runners = new ArrayList<TaskTrackerRunner>();
    for (int i = 0; i < numTaskTrackers; i++) {
      localDirs.clear();
      for (String localDir : baseLocalDirs) {
        File baseLocalDir = new File(localDir);
        File localDirFile = new File(baseLocalDir, "TT_" + i);
        localDirFile.mkdirs();
        localDirs.add(localDirFile.getAbsolutePath());
      }
      jConf.setStrings("mapred.local.dir",
              localDirs.toArray(new String[localDirs.size()]));
      TaskTracker tracker = new TaskTracker(jConf);
      TaskTrackerRunner runner = new TaskTrackerRunner(tracker);
      runner.setDaemon(true);
      runners.add(runner);
      runner.start();
    }
    for (TaskTrackerRunner runner : runners) {
      try {
        runner.join();
      } catch (InterruptedException iex) {
      }
    }
  }

  private static class TaskTrackerRunner extends Thread {

    private TaskTracker ttToRun = null;

    public TaskTrackerRunner(TaskTracker tt) {
      super();
      this.ttToRun = tt;
    }

    @Override
    public void run() {
      ttToRun.run();
    }
  }
}
