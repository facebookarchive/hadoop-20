/**
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

import java.util.*;

public abstract class JobInProgressTraits {

  protected TaskInProgress maps[] = new TaskInProgress[0];
  protected TaskInProgress reduces[] = new TaskInProgress[0];
  protected TaskInProgress cleanup[] = new TaskInProgress[0];
  protected TaskInProgress setup[] = new TaskInProgress[0];

  static String convertTrackerNameToHostName(String trackerName) {
    // Ugly!
    // Convert the trackerName to it's host name
    int indexOfColon = trackerName.indexOf(":");
    String trackerHostName = (indexOfColon == -1) ?
      trackerName :
      trackerName.substring(0, indexOfColon);
    return trackerHostName.substring("tracker_".length());
  }

  public abstract boolean inited();

  public abstract JobStatus getStatus();

  public abstract boolean hasSpeculativeMaps();

  public abstract boolean hasSpeculativeReduces();

  abstract DataStatistics getRunningTaskStatistics(boolean isMap);
  
  // Gets statistics for all tasks in a phase
  abstract DataStatistics getRunningTaskStatistics(TaskStatus.Phase phase);

  public abstract float getSlowTaskThreshold();

  public abstract float getStddevMeanRatioMax();

  public abstract int getNumRestarts();

  public abstract String getUser();

  public abstract boolean shouldSpeculateAllRemainingMaps();

  public abstract boolean shouldSpeculateAllRemainingReduces();

  public abstract boolean shouldLogCannotspeculativeMaps();

  public abstract boolean shouldLogCannotspeculativeReduces();

  /**
   * Return a vector of completed TaskInProgress objects
   */
  public Vector<TaskInProgress> reportTasksInProgress(boolean shouldBeMap, boolean shouldBeComplete) {
    Vector<TaskInProgress> results = new Vector<TaskInProgress>();
    TaskInProgress tips[] = null;
    if (shouldBeMap) {
      tips = maps;
    } else {
      tips = reduces;
    }

    for (int i = 0; i < tips.length; i++) {
      if (tips[i].isComplete() == shouldBeComplete) {
        results.add(tips[i]);
      }
    }
    return results;
  }

  /**
   * Return a vector of cleanup TaskInProgress objects
   */
  public Vector<TaskInProgress> reportCleanupTIPs(boolean shouldBeComplete) {

    Vector<TaskInProgress> results = new Vector<TaskInProgress>();
    for (int i = 0; i < cleanup.length; i++) {
      if (cleanup[i].isComplete() == shouldBeComplete) {
        results.add(cleanup[i]);
      }
    }
    return results;
  }

  /**
   * Return a vector of setup TaskInProgress objects
   */
  public Vector<TaskInProgress> reportSetupTIPs(boolean shouldBeComplete) {

    Vector<TaskInProgress> results = new Vector<TaskInProgress>();
    for (int i = 0; i < setup.length; i++) {
      if (setup[i].isComplete() == shouldBeComplete) {
        results.add(setup[i]);
      }
    }
    return results;
  }

  /**
   * Return the TaskInProgress that matches the tipid.
   */
  public TaskInProgress getTaskInProgress(TaskID tipid) {
    if (tipid.isMap()) {
      if (cleanup.length > 0 && tipid.equals(cleanup[0].getTIPId())) { // cleanup map tip
        return cleanup[0];
      }
      if (setup.length > 0 && tipid.equals(setup[0].getTIPId())) { //setup map tip
        return setup[0];
      }
      for (int i = 0; i < maps.length; i++) {
        if (tipid.equals(maps[i].getTIPId())){
          return maps[i];
        }
      }
    } else {
      if (cleanup.length > 0 && tipid.equals(cleanup[1].getTIPId())) { // cleanup reduce tip
        return cleanup[1];
      }
      if (setup.length > 0 && tipid.equals(setup[1].getTIPId())) { //setup reduce tip
        return setup[1];
      }
      for (int i = 0; i < reduces.length; i++) {
        if (tipid.equals(reduces[i].getTIPId())){
          return reduces[i];
        }
      }
    }
    return null;
  }
}
