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
package org.apache.hadoop.hdfs.server.hightidenode;

import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.util.*;
import org.apache.hadoop.fs.Path;
import java.io.*;
import java.util.*;
import java.sql.Time;

/***************************************************
 * PendingReplication does the bookkeeping of all
 * files that are getting replicated.
 *
 * It does the following:
 * 1)  record files that are getting replicated at this instant.
 * 2)  a coarse grain timer to track age of replication request
 * 3)  a thread that periodically identifies replication-requests
 *     that possibly are declared done.
 *
 ***************************************************/
class PendingReplication {
  private Map<Path, PendingInfo> pendingReplications;
  Daemon timerThread = null;
  private volatile boolean fsRunning = true;

  //
  // It might take anywhere between 5 to 10 minutes before
  // a request is timed out.
  //
  private long timeout = 5 * 60 * 1000;
  private long defaultRecheckInterval = 1 * 60 * 1000;

  PendingReplication(long timeoutPeriod) {
    if ( timeoutPeriod > 0 ) {
      this.timeout = timeoutPeriod;
    }
    init();
  }

  PendingReplication() {
    init();
  }

  void init() {
    pendingReplications = new HashMap<Path, PendingInfo>();
    this.timerThread = new Daemon(new PendingReplicationMonitor());
    timerThread.start();
  }

  /**
   * Add a block to the list of pending Replications. Returns true
   * if the filename is added for the first time.
   */
  boolean add(Path filename) {
    synchronized (pendingReplications) {
      PendingInfo found = pendingReplications.get(filename);
      if (found == null) {
        pendingReplications.put(filename, new PendingInfo(filename));
        return true;
      }
      return false;
    }
  }
  /**
   * Remove a block to the list of pending Replications. 
   */
  void remove(Path filename) {
    synchronized (pendingReplications) {
      pendingReplications.remove(filename);
    }
  }

  /**
   * The total number of files that are undergoing replication
   */
  int size() {
    return pendingReplications.size();
  } 

  /**
   * An object that contains information about a file that 
   * is being replicated. It records the timestamp when the 
   * system started replicating the most recent copy of this
   * file.
   */
  static class PendingInfo {
    private long timeStamp;
    private Path filename;

    PendingInfo(Path filename) {
      this.timeStamp = HighTideNode.now();
      this.filename = filename;
    }

    long getTimeStamp() {
      return timeStamp;
    }

    void setTimeStamp() {
      timeStamp = HighTideNode.now();
    }

    Path getFile() {
      return filename;
    }
  }

  /*
   * A periodic thread that scans for blocks that should have finished.
   */
  class PendingReplicationMonitor implements Runnable {
    public void run() {
      while (fsRunning) {
        long period = Math.min(defaultRecheckInterval, timeout);
        try {
          pendingReplicationCheck();
          Thread.sleep(period);
        } catch (InterruptedException ie) {
          HighTideNode.LOG.debug(
                "PendingReplicationMonitor thread received exception. " + ie);
        }
      }
    }

    /**
     * Iterate through all items and detect timed-out items
     */
    void pendingReplicationCheck() {
      synchronized (pendingReplications) {
        Iterator iter = pendingReplications.entrySet().iterator();
        long now = HighTideNode.now();
        HighTideNode.LOG.info("PendingReplicationMonitor checking Q");
        while (iter.hasNext()) {
          Map.Entry entry = (Map.Entry) iter.next();
          PendingInfo pendingBlock = (PendingInfo) entry.getValue();
          if (now > pendingBlock.getTimeStamp() + timeout) {
            HighTideNode.LOG.info(
                "PendingReplicationMonitor purging record for file " + 
                 pendingBlock.getFile());
            iter.remove();
            HighTideNode.getMetrics().fixClearedOut.inc();
          }
        }
      }
    }
  }

  /*
   * Shuts down the pending replication monitor thread.
   * Waits for the thread to exit.
   */
  void stop() {
    fsRunning = false;
    timerThread.interrupt();
    try {
      timerThread.join(3000);
    } catch (InterruptedException ie) {
    }
  }

  /**
   * Iterate through all items and print them.
   */
  void metaSave(PrintWriter out) {
    synchronized (pendingReplications) {
      out.println("Metasave: Blocks being replicated: " +
                  pendingReplications.size());
      Iterator iter = pendingReplications.entrySet().iterator();
      while (iter.hasNext()) {
        Map.Entry entry = (Map.Entry) iter.next();
        PendingInfo pendingBlock = (PendingInfo) entry.getValue();
        Path filename = (Path) entry.getKey();
        out.println(filename + 
                    " StartTime: " + new Time(pendingBlock.timeStamp));
      }
    }
  }
}
