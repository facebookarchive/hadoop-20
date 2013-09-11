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
package org.apache.hadoop.hdfs.server.namenode;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Periodically check whether it is time to leave safe mode. This thread
 * starts when the threshold level is reached.
 */
class SafeModeMonitor implements Runnable {
  /**
   * interval in msec for checking safe mode: {@value}
   */
  private static final long recheckInterval = 1000;

  private final FSNamesystem namesystem;
  private final SafeModeInfo safeMode;
  private static final Log LOG = LogFactory.getLog(SafeModeMonitor.class);

  public SafeModeMonitor(FSNamesystem namesystem, SafeModeInfo safeMode) {
    if (namesystem == null || safeMode == null) {
      throw new IllegalArgumentException("Arguments are null - namesystem: "
          + namesystem + ", safemode: " + safeMode);
    }
    this.namesystem = namesystem;
    this.safeMode = safeMode;
  }

  public void run() {
    try {
      runMonitor();
    } catch (Throwable e) {
      LOG.error("SafeModeMonitor thread exited with exception : ", e);
    }
  }

  private void runMonitor() {
    while (true) {
      try {
        if ((!namesystem.isRunning()) || (safeMode == null)
            || (safeMode.canLeave())) {
          LOG.info("SafeModeMonitor exiting");
          break;
        }
        Thread.sleep(recheckInterval);
      } catch (Exception t) {
        LOG.info("SafeModeMonitor caught exception", t);
      }
    }
    // if we stopped namenode while still in safemode, then exit here
    if (!namesystem.isRunning()) {
      LOG.info("Quitting SafeModeMonitor thread. ");
      return;
    }

    // leave safe mode and stop the monitor
    try {
      namesystem.leaveSafeMode(true);
    } catch (SafeModeException es) { // should never happen
      String msg = "SafeModeMonitor may not run during distributed upgrade.";
      assert false : msg;
      throw new RuntimeException(msg, es);
    } 
  }
}
