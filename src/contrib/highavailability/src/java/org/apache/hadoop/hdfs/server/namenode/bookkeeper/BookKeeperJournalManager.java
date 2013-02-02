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
package org.apache.hadoop.hdfs.server.namenode.bookkeeper;

import com.google.common.annotations.VisibleForTesting;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.util.ZkUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hdfs.server.namenode.JournalManager;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.apache.zookeeper.AsyncCallback.*;

/**
 * Skeleton class that only contains static methods unit tests need to access
 * used to access BookKeeper at this time (these methods will later be needed
 * by the actual {@link JournalManager} implementation).
 */
public class BookKeeperJournalManager {

  private static final Log LOG =
      LogFactory.getLog(BookKeeperJournalManager.class);

  /**
   * Create parent ZNode under which available BookKeeper bookie servers will
   * register themselves. Will create parent ZNodes for that path as well.
   * @see ZkUtils#createFullPathOptimistic(ZooKeeper, String, byte[], List, CreateMode, StringCallback, Object)
   * @param availablePath Full ZooKeeper path for bookies to register
   *                      themselves.
   * @param zooKeeper Fully instantiated ZooKeeper instance.
   * @throws IOException If we are unable to successfully create the path
   *                     during the time specified as the ZooKeeper session
   *                     timeout.
   */
  @VisibleForTesting
  public static void prepareBookKeeperEnv(final String availablePath,
      ZooKeeper zooKeeper) throws IOException {
    final CountDownLatch availablePathLatch =  new CountDownLatch(1);
    StringCallback cb = new StringCallback() {
      @Override
      public void processResult(int rc, String path, Object ctx, String name) {
        if (Code.OK.intValue() == rc || Code.NODEEXISTS.intValue() == rc) {
          availablePathLatch.countDown();
          LOG.info("Successfully created bookie available path:" +
              availablePath);
        } else {
          Code code = Code.get(rc);
          LOG.error("Failed to create available bookie path (" +
              availablePath + ")", KeeperException.create(code, path));
        }
      }
    };
    ZkUtils.createFullPathOptimistic(zooKeeper, availablePath, new byte[0],
        Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT, cb, null);
    try {
      int timeoutMs = zooKeeper.getSessionTimeout();
      if (!availablePathLatch.await(timeoutMs, TimeUnit.MILLISECONDS)) {
        throw new IOException("Couldn't create the bookie available path : " +
            availablePath + ", timed out after " + timeoutMs + " ms.");
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IOException("Interrupted when creating the bookie available " +
          "path: " + availablePath, e);
    }
  }

  @VisibleForTesting
  public LedgerHandle openForReading(long ledgerId) throws IOException {
    return null;
  }
}
