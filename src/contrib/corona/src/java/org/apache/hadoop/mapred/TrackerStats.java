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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.corona.CoronaConf;
import org.apache.hadoop.corona.NodeUsageReport;

/**
 * Maintains statistics about all the tasks. The Corona Job Tracker can call the
 * record functions to update various statistics. It can find if a tracker is
 * blacklisted or get a summary of the statistics. Note that the all the
 * recording functions require the tracker name, which is obtained through the
 * node name of resource grants. This is different from the tracker host.
 */
public class TrackerStats {
  /** Usage report for a tracker. */
  private final Map<String, NodeUsageReport> usageReports =
      new HashMap<String, NodeUsageReport>();
  /** Max failed connections before consider a tracker faulty for this job. */
  private final int maxFailedConnections;
  /** Max failed tasks before consider a tracker faulty for this job. */
  private final int maxFailures;
  /** The list of trackers that were declared dead */
  private final Set<String> deadTrackers = new HashSet<String>();

  /**
   * Constructor.
   * @param conf The configuration.
   */
  public TrackerStats(Configuration conf) {
    CoronaConf coronaConf = new CoronaConf(conf);
    maxFailedConnections = coronaConf.getMaxFailedConnectionsPerSession();
    maxFailures = coronaConf.getMaxFailuresPerSession();
  }

  /**
   * Check if a tracker is faulty.
   * @param trackerName The name of the tracker.
   * @return A boolean indicating if the tracker is faulty.
   */
  public boolean isFaulty(String trackerName) {
    synchronized (this) {
      NodeUsageReport usageReport = usageReports.get(trackerName);
      return isDeadTracker(trackerName) || (usageReport != null &&
             (usageReport.getNumFailedConnections() > maxFailedConnections ||
              usageReport.getNumFailed() > maxFailures));
    }
  }

  /**
   * Get the number of faulty trackers.
   * @return the number of faulty trackers.
   */
  public int getNumFaultyTrackers() {
    int count = 0;
    synchronized (this) {
      for (String trackerName : usageReports.keySet()) {
        if (isFaulty(trackerName)) {
          count++;
        }
      }
    }
    return count;
  }

  /**
   * This tracker has been declared dead by some external force
   * and we are recording it for future reference
   *
   * @param trackerName the name of the tracker that was declared dead
   */
  public void recordDeadTracker(String trackerName) {
    synchronized (this) {
      deadTrackers.add(trackerName);
    }
  }
  /**
   * Has the tracker been declared dead
   *
   * @param trackerName the name of the tracker to check for deadness
   * @return true if the tracker has been declared dead, false otherwise
   */
  public boolean isDeadTracker(String trackerName) {
    synchronized (this) {
      return deadTrackers.contains(trackerName);
    }
  }

  /**
   * Increment the number of tasks assigned to a tracker.
   * @param trackerName The name of the tracker.
   */
  public void recordTask(String trackerName) {
    synchronized (this) {
      NodeUsageReport usageReport = getReportUnprotected(trackerName);
      usageReport.setNumTotalTasks(usageReport.getNumTotalTasks() + 1);
    }
  }

  /**
   * Increment the number of succeeded tasks on a tracker.
   * @param trackerName The name of the tracker.
   */
  public void recordSucceededTask(String trackerName) {
    synchronized (this) {
      NodeUsageReport usageReport = getReportUnprotected(trackerName);
      usageReport.setNumSucceeded(usageReport.getNumSucceeded() + 1);
    }
  }

  /**
   * Increment the number of killed tasks on a tracker.
   * @param trackerName The name of the tracker.
   */
  public void recordKilledTask(String trackerName) {
    synchronized (this) {
      NodeUsageReport usageReport = getReportUnprotected(trackerName);
      usageReport.setNumKilled(usageReport.getNumKilled() + 1);
    }
  }

  /**
   * Increment the number of failed tasks on a tracker.
   * @param trackerName The name of the tracker.
   */
  public void recordFailedTask(String trackerName) {
    synchronized (this) {
      NodeUsageReport usageReport = getReportUnprotected(trackerName);
      usageReport.setNumFailed(usageReport.getNumFailed() + 1);
    }
  }

  /**
   * Set the number of failed tasks on a tracker to 0.
   */
  public void resetFailedCount() {
    synchronized (this) {
      for (NodeUsageReport report : usageReports.values()) {
        report.setNumFailed(0);
      }
    }
  }

  /**
   * Increment the number of timeouts (expired launch) on a tracker.
   * @param trackerName The name of the tracker.
   */
  public void recordTimeout(String trackerName) {
    synchronized (this) {
      NodeUsageReport usageReport = getReportUnprotected(trackerName);
      usageReport.setNumTimeout(usageReport.getNumTimeout() + 1);
    }
  }

  /**
   * Increment the number of tasks that ran slowly on a tracker.
   * @param trackerName The name of the tracker.
   */
  public void recordSlowTask(String trackerName) {
    synchronized (this) {
      NodeUsageReport usageReport = getReportUnprotected(trackerName);
      usageReport.setNumSlow(usageReport.getNumSlow() + 1);
    }
  }

  /**
   * Increment the number of connection errors encountered on a tracker.
   * @param trackerName The name of the tracker.
   */
  public void recordConnectionError(String trackerName) {
    synchronized (this) {
      NodeUsageReport usageReport = getReportUnprotected(trackerName);
      usageReport
          .setNumFailedConnections(usageReport.getNumFailedConnections() + 1);
    }
  }

  /**
   * Get the usage reports for all trackers.
   * @return A list of {@link NodeUsageReport}.
   */
  public List<NodeUsageReport> getNodeUsageReports() {
    synchronized (this) {
      return new ArrayList<NodeUsageReport>(usageReports.values());
    }
  }

  /**
   * Get the usage report for a tracker.
   * @param trackerName The name of the tracker.
   * @return The {@link NodeUsageReport} for the tracker.
   */
  private NodeUsageReport getReportUnprotected(String trackerName) {
    NodeUsageReport usageReport = usageReports.get(trackerName);
    if (usageReport == null) {
      usageReport = new NodeUsageReport(trackerName, 0, 0, 0, 0, 0, 0, 0);
      usageReports.put(trackerName, usageReport);
    }
    return usageReport;
  }
}
