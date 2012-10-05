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

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;

/**
 * A tool to run multiple CoronaTaskTrackers inside of a single JVM
 */
public class MultiCoronaTaskTracker {

  public static void main(String[] args) throws IOException {
    int numTaskTrackers = Integer.parseInt(args[0]);
    List<TaskTrackerRunner> runners = new ArrayList<TaskTrackerRunner>();
    for (int i = 0; i < numTaskTrackers; i++) {
      Configuration conf = new Configuration();
      JobConf jConf = new JobConf(conf);
      jConf.set("mapred.task.tracker.http.address", "0.0.0.0:0");
      String[] baseLocalDirs = jConf.getLocalDirs();
      List<String> localDirs = new LinkedList<String>();
      localDirs.clear();
      for (String localDir : baseLocalDirs) {
        File baseLocalDir = new File(localDir);
        File localDirFile = new File(baseLocalDir, "TT_" + i);
        localDirFile.mkdirs();
        localDirs.add(localDirFile.getAbsolutePath());
      }
      jConf.setStrings("mapred.local.dir",
              localDirs.toArray(new String[localDirs.size()]));
      CoronaTaskTracker tracker = new CoronaTaskTracker(jConf);
      TaskTrackerRunner runner = new TaskTrackerRunner(tracker);
      runner.setDaemon(true);
      runners.add(runner);
      runner.start();
    }
    for (TaskTrackerRunner runner : runners) {
      try {
        runner.join();
      } catch (InterruptedException iex) {
        iex.printStackTrace();
      }
    }
  }

  /**
   * A wrapper thread that runs a task tracker
   */
  private static class TaskTrackerRunner extends Thread {

    /** TaskTracker object to run in this thread */
    private TaskTracker ttToRun = null;

    /**
     * Construct TaskTrackerRunner for the CoronaTaskTracker
     * @param tt the tracker to run
     */
    TaskTrackerRunner(CoronaTaskTracker tt) {
      super();
      this.ttToRun = tt;
    }

    @Override
    public void run() {
      ttToRun.run();
    }
  }
}

