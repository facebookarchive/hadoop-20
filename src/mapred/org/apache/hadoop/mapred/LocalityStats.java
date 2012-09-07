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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.net.Node;
import org.apache.hadoop.net.TopologyCache;
import org.apache.hadoop.mapred.JobInProgress.Counter;

import java.util.ArrayList;

/**
 * Record locality information. Can perform the locality computation in a
 * separate thread.
 */
public class LocalityStats implements Runnable {
  /** Logger. */
  private static final Log LOG = LogFactory.getLog(LocalityStats.class);
  /** Topology cache. */
  private final TopologyCache topologyCache;
  /** Max locality level. */
  private final int maxLevel;
  /** Job Counters. */
  private final Counters jobCounters;
  /** Job Statistics. */
  private final JobStats jobStats;
  /** List of records to be used for asynchronous operation. */
  private final ArrayList<Record> localityRecords = new ArrayList<Record>();
  /** In async mode, used to check if we are running. */
  private volatile boolean running = true;

  /**
   * Constructor.
   * @param jobConf Job Configuration.
   * @param maxLevel The maximum locality level.
   * @param counters The job counters to update.
   * @param jobStats The job statistics to update.
   */
  public LocalityStats(
    JobConf jobConf, int maxLevel, Counters counters, JobStats jobStats,
    TopologyCache topologyCache) {
    this.topologyCache = topologyCache;
    this.maxLevel = maxLevel;
    this.jobCounters = counters;
    this.jobStats = jobStats;
  }

  public String getNode(String host) {
    return topologyCache.getNode(host).toString();
  }

  /**
   * Representation of information for asynchronous update.
   */
  private static class Record {
    /** The task. */
    private final TaskInProgress tip;
    /** The task tracker host. */
    private final String host;
    /** The number of bytes processed. */
    private final long inputBytes;

    /**
     * Constructor
     * @param tip The task.
     * @param host The task tracker host.
     * @param inputBytes The number of bytes processed.
     */
    private Record(TaskInProgress tip, String host, long inputBytes) {
      this.tip = tip;
      this.host = host;
      this.inputBytes = inputBytes;
    }
  }

  /**
   * In async mode, stop the thread.
   */
  public void stop() {
    running = false;
  }

  /**
   * Asynchronous update of locality.
   * @param tip The task.
   * @param host The task tracker host.
   * @param inputBytes The number of bytes processed.
   */
  public void record(
    TaskInProgress tip, String host, long inputBytes) {
    synchronized (localityRecords) {
      localityRecords.add(new Record(tip, host, inputBytes));
      localityRecords.notify();
    }
  }

  @Override
  public void run() {
    LOG.info("Starting locality computation thread");
    while (running) {
      Record record = null;
      synchronized (localityRecords) {
        while (localityRecords.isEmpty()) {
          try {
            localityRecords.wait();
          } catch (InterruptedException e) {
            if (!running) {
              return;
            }
          }
        }
        // Remove last element in the array.
        record = localityRecords.remove(localityRecords.size() - 1);
      }
      computeStatistics(record);
    }
    LOG.info("Exiting locality computation thread");
  }

  /**
   * Peform the computation statistics based on a locality record.
   * @param record The locality information.
   */
  private void computeStatistics(Record record) {
    computeStatistics(record.tip, record.host, record.inputBytes);
  }

  /**
   * Peform the computation statistics.
   * @param tip The task.
   * @param host The task tracker host.
   * @param inputBytes The number of bytes processed.
   */
  private void computeStatistics(
    TaskInProgress tip, String host, long inputBytes) {
    int level = this.maxLevel;
    String[] splitLocations = tip.getSplitLocations();
    if (splitLocations.length > 0) {
      Node tracker = topologyCache.getNode(host);
      // find the right level across split locations
      for (String local : splitLocations) {
        Node datanode = topologyCache.getNode(local);
        int newLevel = this.maxLevel;
        if (tracker != null && datanode != null) {
          newLevel = getMatchingLevelForNodes(tracker, datanode, maxLevel);
        }
        if (newLevel < level) {
          level = newLevel;
          if (level == 0) {
            break;
          }
        }
      }
    }
    boolean updateTaskCountOnly = inputBytes < 0;
    switch (level) {
    case 0:
      if (updateTaskCountOnly) {
        LOG.info("Chose data-local task " + tip.getTIPId());
        jobCounters.incrCounter(Counter.DATA_LOCAL_MAPS, 1);
        jobStats.incNumDataLocalMaps();
      } else {
        jobCounters.incrCounter(Counter.LOCAL_MAP_INPUT_BYTES, inputBytes);
        jobStats.incLocalMapInputBytes(inputBytes);
      }
      break;
    case 1:
      if (updateTaskCountOnly) {
        LOG.info("Chose rack-local task " + tip.getTIPId());
        jobCounters.incrCounter(Counter.RACK_LOCAL_MAPS, 1);
        jobStats.incNumRackLocalMaps();
      } else {
        jobCounters.incrCounter(Counter.RACK_MAP_INPUT_BYTES, inputBytes);
        jobStats.incRackMapInputBytes(inputBytes);
      }
      break;
    default:
      LOG.info("Chose non-local task " + tip.getTIPId() + " at level " + level);
      // check if there is any locality
      if (updateTaskCountOnly && level != this.maxLevel) {
        jobCounters.incrCounter(Counter.OTHER_LOCAL_MAPS, 1);
      }
      break;
    }
  }

  public static int getMatchingLevelForNodes(Node n1, Node n2, int maxLevel) {
    int count = 0;
    do {
      if (n1.equals(n2)) {
        return count;
      }
      ++count;
      n1 = n1.getParent();
      n2 = n2.getParent();
    } while (n1 != null && n2 != null);
    return maxLevel;
  }

}
