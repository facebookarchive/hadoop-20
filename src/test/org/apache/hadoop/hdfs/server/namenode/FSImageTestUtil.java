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
import java.util.concurrent.CountDownLatch;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hdfs.util.InjectionEvent;


/**
 * Utility functions for testing fsimage storage.
 */
public abstract class FSImageTestUtil {
  
  final static Log LOG = LogFactory.getLog(FSImageTestUtil.class);
  
  public static class CheckpointTrigger {
    volatile boolean triggerCheckpoint = false;
    volatile boolean checkpointDone = false;
    volatile Exception e;

    private volatile CountDownLatch ckptLatch = new CountDownLatch(0);

    public void checkpointDone(InjectionEvent event, Object... args) {
      if (event == InjectionEvent.STANDBY_EXIT_CHECKPOINT) {
        ckptLatch.countDown();
      }
      if (event == InjectionEvent.STANDBY_EXIT_CHECKPOINT_EXCEPTION) {
        e = (Exception) args[0];
        ckptLatch.countDown();
      }
    }

    public boolean triggerCheckpoint(InjectionEvent event, Object... args) {
      if (event == InjectionEvent.STANDBY_CHECKPOINT_TRIGGER
          && ckptLatch.getCount() > 0) {
        return true;
      }
      return false;
    }

    public void doCheckpoint() throws Exception {
      e = null;
      ckptLatch = new CountDownLatch(1);
      try {
        ckptLatch.await();
      } catch (InterruptedException e) {
        throw new IOException("Interruption received");
      }
      if (e!=null)
        throw e;
    }
  }
}
