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
package org.apache.hadoop.hdfs.server.datanode.metrics;

import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.metrics.util.MetricsIntValue;

/**
 * This class is used to track whether background threads are up.
 */
public class DatanodeThreadLivenessReporter {
  public enum BackgroundThread {
    DATA_XCEIVER_SERVER(0),
    BLOCK_SCANNER(1),
    BLOCK_CRC_FLUSHER(2);

    private int threadTypeId;

    private BackgroundThread(int threadTypeId) {
      this.threadTypeId = threadTypeId;
    }

    private int getThreadTypeId() {
      return threadTypeId;
    }
  }
  
  private class ThreadLivenessInfo {
    long timeLastUpdate;
    boolean missingThreadLogged;
    
    private void update() {
      timeLastUpdate = System.currentTimeMillis();
      missingThreadLogged = false;
    }
    
    private boolean ifReportLiveAfter(long timeThreshold) {
      return timeLastUpdate > timeThreshold;
    }
  }

  final private ThreadLivenessInfo[] threadInfo;
  final long timeoutToReport;
  final MetricsIntValue metrics;

  public DatanodeThreadLivenessReporter(long timeoutToReport,
      MetricsIntValue metrics) {
    threadInfo = new ThreadLivenessInfo[BackgroundThread.values().length];
    for (int i = 0; i < threadInfo.length; i++) {
      threadInfo[i] = new ThreadLivenessInfo();
    }
    this.timeoutToReport = timeoutToReport;
    this.metrics = metrics;
  }

  /**
   * Report liveness of one thread and report liveness metrics if asked
   * 
   * @param bt
   *          background thread that is reported to be alive
   * @param reportMetrics
   *          if true, report thread liveness report to metrics, and log threads
   *          missing recent reports.
   */
  public synchronized void reportLiveness(BackgroundThread bt) {
    threadInfo[bt.getThreadTypeId()].update();
    reportMetrics();
    logMissingThreads();
  }

  private synchronized boolean ifAllThreadLive() {
    long deadlineReport = System.currentTimeMillis() - timeoutToReport;
    for (ThreadLivenessInfo t : threadInfo) {
      if (!t.ifReportLiveAfter(deadlineReport)) {
        return false;
      }
    }
    return true;
  }

  private synchronized void reportMetrics() {
    if (metrics != null) {
      metrics.set(ifAllThreadLive() ? 1 : 0);
    }
  }

  private synchronized void logMissingThreads() {
    long deadlineReport = System.currentTimeMillis() - timeoutToReport;
    for (BackgroundThread bt : BackgroundThread.values()) {
      if (!threadInfo[bt.threadTypeId].ifReportLiveAfter(deadlineReport)
          && !threadInfo[bt.threadTypeId].missingThreadLogged) {
        DataNode.LOG.warn("Thread " + bt + " is not up.");
        threadInfo[bt.threadTypeId].missingThreadLogged = true;
      }
    }
  }
}
