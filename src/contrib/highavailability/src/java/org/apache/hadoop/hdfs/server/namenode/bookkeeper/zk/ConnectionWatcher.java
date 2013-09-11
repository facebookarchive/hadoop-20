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

package org.apache.hadoop.hdfs.server.namenode.bookkeeper.zk;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.apache.hadoop.hdfs.server.namenode.bookkeeper.zk.ZkUtil.interruptedException;

/**
 * Implementation of {@link Watcher} that waits for a connection to be
 * established to ZooKeeper.
 */
public class ConnectionWatcher implements Watcher {

  private static final Log LOG = LogFactory.getLog(ConnectionWatcher.class);

  private final CountDownLatch connectLatch;

  public ConnectionWatcher(CountDownLatch connectLatch) {
    this.connectLatch = connectLatch;
  }

  @Override
  public void process(WatchedEvent event) {
    if (Event.KeeperState.SyncConnected.equals(event.getState())) {
      connectLatch.countDown();
      LOG.info("Connected to ZooKeeper");
    }
  }

  /**
   * Wait until either a specified timeout expires or a connection is
   * created to ZooKeeper.
   * @see {@link CountDownLatch#await(long, TimeUnit)}
   * @param timeoutMillis  Wait this long (in milliseconds) for a connection
   *                       to be established to ZooKeeper.
   * @return True if a connection was established within the specified timeout,
   *         false if a timeout has occurred before a connection to ZooKeeper
   *         could be established.
   * @throws IOException If we interrupted while awaiting for connection to
   *                     ZooKeeper to be established
   */
  public boolean await(long timeoutMillis) throws IOException {
    try {
      return connectLatch.await(timeoutMillis, TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      interruptedException(
          "Interrupted waiting for connection (timeout = " + timeoutMillis +
              "ms.)", e);
      return false;
    }
  }
}
