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

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hdfs.server.protocol.AutoEditsRollerInterface;
import org.apache.hadoop.hdfs.util.InjectionEvent;
import org.apache.hadoop.util.InjectionHandler;

public class AutomaticEditsRoller implements Runnable {
  public static final Log LOG = LogFactory.getLog(AutomaticEditsRoller.class);

  final AutoEditsRollerInterface namenode;
  long nextRollTime = 0;

  private volatile boolean running = true;
  private Object lock = new Object();
  
  public AutomaticEditsRoller(AutoEditsRollerInterface namenode) {
    this.namenode = namenode;
  }

  public void setNextRollTime(long nextRollTime) {
    LOG.info("Scheduled Automatic Rolling at " + nextRollTime);
    synchronized (lock) {
      this.nextRollTime = nextRollTime;
      lock.notifyAll();
    }
  }

  /**
   * Signal the automatic edits roller thread to exit.
   * We cannot interrupt the thread unless we can allow edits corruption
   * in the last transactions, which is not the case for cases like manual
   * failover.
   */
  public void stop() {
    synchronized (lock) {
      running = false;
      lock.notifyAll();
    }
  }

  @Override
  public void run() {
    if (namenode == null) {
      LOG.warn("Name node passed to AutomaticEditsRoller is NULL.");
      return;
    }
    
    LOG.info("Automatic Edits Roller started...");
    
    try {
      while (running
          && (!namenode.isNamesystemInitialized() || namenode
              .isNamesystemRunning())) {
        // If it is not ready for an automatic roll,
        // wait for a while.
        long timeNow = System.currentTimeMillis();
        try {
          synchronized (lock) {
            if (!namenode.isNamesystemInitialized() || nextRollTime <= 0) {
              lock.wait(1000);
              continue;
            } else if (nextRollTime > timeNow) {
              lock.wait(nextRollTime - timeNow + 1);
              continue;
            }
          }
        } catch (InterruptedException e) {
          LOG.warn("Interrupted", e);
          return;
        }
        // There is a short window between checking the timestamp
        // and actually issuing the roll. There is a small chance
        // that another roll happens in between and the automatic
        // roll happens for an almost empty segment. We are fine
        // with it in this version for simplicity of the codes.

        // Roll Edit Logs
        try {
          // There is a chance that the automatic roll happens
          // in safe mode, so it got lost. We allow it for code
          // simplicity. Safe mode usually is only on for admin
          // operations. We can wait for a roll after the operations
          // if we really care about it.
          nextRollTime = 0;
          namenode.rollEditLogAdmin();
          InjectionHandler
              .processEvent(InjectionEvent.FSEDIT_AFTER_AUTOMATIC_ROLL);
        } catch (IOException e) {
          LOG.warn("Exception when trying to roll edit logs", e);
        }
      }
    } finally {
      LOG.info("Automatic Edits Roller Exiting...");
    }
  }
}
