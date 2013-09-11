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
package org.apache.hadoop.hdfs.server.namenode.bookkeeper.metadata;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hdfs.server.namenode.bookkeeper.BookKeeperJournalManager;
import org.apache.hadoop.hdfs.server.namenode.bookkeeper.metadata.proto.MaxTxIdWritable;
import org.apache.hadoop.hdfs.server.namenode.bookkeeper.metadata.proto.WritableUtil;
import org.apache.hadoop.hdfs.server.namenode.bookkeeper.zk.RecoveringZooKeeper;
import org.apache.hadoop.hdfs.server.namenode.bookkeeper.zk.ZooKeeperIface;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;

import static org.apache.hadoop.hdfs.server.namenode.bookkeeper.zk.ZkUtil.interruptedException;
import static org.apache.hadoop.hdfs.server.namenode.bookkeeper.zk.ZkUtil.keeperException;

/**
 * Used by {@link BookKeeperJournalManager} to indicate the latest known
 * HDFS transaction id (e.g., to prevent starting a log segment with a lower
 * transaction id). The class is thread-safe as all the critical methods
 * are synchronized.
 */
public class MaxTxId {

  private static final Log LOG = LogFactory.getLog(MaxTxId.class);

  private final ZooKeeperIface zooKeeper;
  private final String fullyQualifiedZNode;
  // The writable object is instantiated once and re-used to avoid
  // re-allocating the object on every write
  private final MaxTxIdWritable maxTxIdWritable;
  // Stores the version we've last read from the ZNode to make sure we don't
  // reset the transaction id without reading the current transaction id first
  private Stat lastZNodeStat;

  /**
   * Instantiates a instance, but does not update anything in ZooKeeper
   * @param zooKeeper The appropriate {@link ZooKeeperIface} implementation,
   *                  e.g., {@link RecoveringZooKeeper} for auto-recovery
   * @param fullyQualifiedZNode Fully qualified path to the znode used to
   *                            store the max transaction id
   */
  public MaxTxId(ZooKeeperIface zooKeeper,
      String fullyQualifiedZNode) {
    this.zooKeeper = zooKeeper;
    this.fullyQualifiedZNode = fullyQualifiedZNode;
    this.maxTxIdWritable = new MaxTxIdWritable();
    lastZNodeStat = null;
  }

  /**
   * Store the specified transaction id in the maxTxId ZNode if the
   * specified maxTxId is greater than the existing maxTxId.
   * @param maxTxId Max transaction id to store
   * @throws IOException If there is an unrecoverable error communicating with
   *                     ZooKeeper
   */
  public synchronized void store(long maxTxId) throws IOException {
    long currentMaxTxId = get();
    if (currentMaxTxId < maxTxId) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Resetting maxTxId to " + maxTxId);
      }
      set(maxTxId);
    }
  }

  /**
   * Store the specified transaction in ZooKeeper irrespective of what the
   * current max transaction id is (can be used to reset the max transaction
   * to an older one).
   * @param maxTxId The max transaction id to set
   * @throws StaleVersionException If last max transaction id read by this
   *                               is out of date compared to the version in
   *                               ZooKeeper
   * @throws IOException If there an unrecoverable error communicating with
   *                     ZooKeeper
   */
  public synchronized void set(long maxTxId) throws IOException {
    maxTxIdWritable.set(maxTxId);
    try {
      byte[] data = WritableUtil.writableToByteArray(maxTxIdWritable);
      if (lastZNodeStat != null) {
        try {
          zooKeeper.setData(
              fullyQualifiedZNode, data, lastZNodeStat.getVersion());
        } catch (KeeperException.BadVersionException e) {
          LOG.error(fullyQualifiedZNode + " was updated by another process",
              e);
          throw new StaleVersionException(
              fullyQualifiedZNode + " has been updated by another process");
        }
      } else {
        zooKeeper.create(fullyQualifiedZNode,
            data, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        lastZNodeStat = zooKeeper.exists(fullyQualifiedZNode, false);
      }
    } catch (KeeperException e) {
      LOG.error("Unrecoverable ZooKeeper writing to " +
          fullyQualifiedZNode, e);
      throw new IOException("Unrecoverable writing to " +
          fullyQualifiedZNode, e);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      LOG.error("Interrupted reading from " + fullyQualifiedZNode, e);
      throw new IOException("Interrupted reading from " + fullyQualifiedZNode,
          e);
    }
  }

  /**
   * Get the current max transaction id from ZooKeeper
   * @return The current max transaction id
   * @throws IOException If there is an error talking to ZooKeeper
   */
  public synchronized long get() throws IOException {
    try {
      lastZNodeStat = zooKeeper.exists(fullyQualifiedZNode, false);
      if (lastZNodeStat == null) {
        return -1;
      }
      byte[] data =
          zooKeeper.getData(fullyQualifiedZNode, false, lastZNodeStat);
      WritableUtil.readWritableFromByteArray(data, maxTxIdWritable);
      return maxTxIdWritable.get();
    } catch (KeeperException e) {
      keeperException("Unrecoverable ZooKeeper error reading " +
          fullyQualifiedZNode, e);
      return -1; // This is never reached
    } catch (InterruptedException e) {
      interruptedException("Interrupted reading " +
          fullyQualifiedZNode, e);
      return -1; // This is never reached
    }
  }
}
