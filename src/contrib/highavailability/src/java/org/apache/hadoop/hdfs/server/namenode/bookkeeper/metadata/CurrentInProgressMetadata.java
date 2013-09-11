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
import org.apache.hadoop.hdfs.server.namenode.bookkeeper.metadata.proto.CurrentInProgressMetadataWritable;
import org.apache.hadoop.hdfs.server.namenode.bookkeeper.metadata.proto.WritableUtil;
import org.apache.hadoop.hdfs.server.namenode.bookkeeper.zk.RecoveringZooKeeper;
import org.apache.hadoop.hdfs.server.namenode.bookkeeper.zk.ZooKeeperIface;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.net.InetAddress;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.hadoop.hdfs.server.namenode.bookkeeper.zk.ZkUtil.interruptedException;
import static org.apache.hadoop.hdfs.server.namenode.bookkeeper.zk.ZkUtil.keeperException;

/**
 * Used by {@link BookKeeperJournalMetadataManager} to point to the
 * ledger metadata for the ledger holding the in-progress edit log
 * segment
 */
public class CurrentInProgressMetadata {

  private static final Log LOG = LogFactory.getLog(CurrentInProgressMetadata.class);

  // Keep a thread local Writable for serialization as to avoid allocating
  // a new object each time we write
  private static final ThreadLocal<CurrentInProgressMetadataWritable>
      localWritable = new ThreadLocal<CurrentInProgressMetadataWritable>() {
    @Override
    protected CurrentInProgressMetadataWritable initialValue() {
      return new CurrentInProgressMetadataWritable();
    }
  };

  private final ZooKeeperIface zooKeeper;
  // Full path to znode storing the information about the current in-progress
  // ledger
  private final String fullyQualifiedZNode;
  // The version we've last read from the ZNode to make sure we don't
  // update the in-progress ledger without reading the existing one first
  private final AtomicInteger expectedZNodeVersion;
  // Identifiers the process that last updated the ZNode
  private final String hostname;

  /**
   * Creates and initialized an instance that will point to the metadata about
   * ledger containing the current in-progress edit log segment in a specified
   * ZNode. If the ZNode does not exist, the ZNode will be created and set to
   * null.
   * @param zooKeeper The {@link ZooKeeperIface} implementation. Use
   *                  {@link RecoveringZooKeeper} in order to handle
   *                  transient ZooKeeper issues
   * @param fullyQualifiedZNode The path to the ZNode that will be used
   *                            by this instance to point to the ledger
   *                            metadata for the ledger containing the
   *                            current in-progress log segment
   * @throws IOException
   */
  public CurrentInProgressMetadata(ZooKeeperIface zooKeeper,
      String fullyQualifiedZNode) throws IOException {
    this.zooKeeper = zooKeeper;
    this.fullyQualifiedZNode = fullyQualifiedZNode;
    this.expectedZNodeVersion = new AtomicInteger(-1);
    // Hostname is used as part of the human-readable identifier
    // for the process that last updated the in-progress ZNode
    hostname = InetAddress.getLocalHost().toString();
  }

  public void init() throws IOException {
    boolean alreadyExists = false;
    try {
      // Check whether or not the specified ZNode already exists
      Stat stat = zooKeeper.exists(this.fullyQualifiedZNode,
          false);
      if (stat == null) {
        try { // If the specified ZNode does not exist, create it
          zooKeeper.create(this.fullyQualifiedZNode, null,
              ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
          LOG.info("Created ZNode " + fullyQualifiedZNode);
        } catch (KeeperException.NodeExistsException e) {
          LOG.warn(fullyQualifiedZNode + " created by another process. " +
              "Ignoring " + e);
          alreadyExists = true;
        }
      } else {
        alreadyExists = true;
      }
    } catch (KeeperException e) {
      keeperException("Unrecoverable ZooKeeper error creating " +
          fullyQualifiedZNode, e);
    } catch (InterruptedException e) {
      interruptedException("Interrupted while creating " + fullyQualifiedZNode,
          e);
    }
    if (alreadyExists) {
      // If the znode already exists, read it, print the owner information and
      // update the version number from znode's stat
      CurrentInProgressMetadataWritable owner = localWritable.get();
      if (readAndUpdateVersion(owner)) {
        LOG.info(fullyQualifiedZNode + " held by " + owner.getId() +
            " and points to " + owner.getPath());
      } else {
        LOG.info(fullyQualifiedZNode + " is currently clear.");
      }
    }
  }

  /**
   * Update the data in the ZNode to point to a the ZNode containing the
   * metadata for the ledger containing the current in-progress edit log
   * segment.
   * @param newPath Path to the metadata for the current ledger
   * @throws StaleVersionException If path read is out of date compared to the
   *                               version in ZooKeeper
   * @throws IOException If there is an error talking to ZooKeeper
   */
  public void update(String newPath) throws IOException {
    String id = hostname + Thread.currentThread().getId();
    CurrentInProgressMetadataWritable cip = localWritable.get();
    cip.set(id, newPath);
    byte[] data = WritableUtil.writableToByteArray(cip);
    try {
      zooKeeper.setData(fullyQualifiedZNode, data, expectedZNodeVersion.get());
      if (LOG.isDebugEnabled()) {
        LOG.debug("Set " + fullyQualifiedZNode + " to point to " + newPath);
      }
    } catch (KeeperException.BadVersionException e) {
      // Throw an exception if we try to update without having read the
      // current version
      LOG.error(fullyQualifiedZNode + " has been updated by another process",
          e);
      throw new StaleVersionException(fullyQualifiedZNode +
          "has been updated by another process!");
    } catch (KeeperException e) {
      keeperException("Unrecoverable ZooKeeper error updating " +
          fullyQualifiedZNode, e);
    } catch (InterruptedException e) {
      interruptedException("Interrupted updating " + fullyQualifiedZNode, e);
    }
  }

  /**
   * Read the full path to the ZNode holding the metadata for the ledger
   * containing the current in-progress edit log segment or null if no segment
   * is currently in-progress
   * @return Full path to the ledger's metadata ZNode or null if no
   *         segment is in progress
   * @throws IOException If there is an error talking to ZooKeeper
   */
  public String read() throws IOException {
    CurrentInProgressMetadataWritable cip = localWritable.get();
    if (readAndUpdateVersion(cip)) {
      return cip.getPath();
    } else {
      if (LOG.isDebugEnabled()) {
        LOG.debug(fullyQualifiedZNode + " is currently clear.");
      }
    }
    return null;
  }

  /**
   * Reads the ZNode specified in constructor into an existing
   * {@link CurrentInProgressMetadataWritable} instance and sets expected
   * ZNode version (for next update) the the current version of the
   * specified ZNode
   * @param target The writable instance to read into
   * @return True if there is a log segment is current in-progress, false
   *         otherwise
   * @throws IOException If there is an unrecoverable error talking to
   *                     ZooKeeper
   */
  private boolean readAndUpdateVersion(CurrentInProgressMetadataWritable target)
      throws IOException {
    Stat stat = new Stat();
    try {
      byte[] data = zooKeeper.getData(fullyQualifiedZNode, false, stat);
      expectedZNodeVersion.set(stat.getVersion());
      if (data != null) {
        WritableUtil.readWritableFromByteArray(data, target);
        return true;
      }
    } catch (KeeperException e) {
      keeperException("Unrecoverable ZooKeeper error reading from " +
          fullyQualifiedZNode, e);
    } catch (InterruptedException e) {
      interruptedException("Interrupted reading from " + fullyQualifiedZNode,
          e);
    }
    return false;
  }

  /**
   * Clear out the data in the ZNode specified in the constructor to indicate
   * that no segment is currently in progress. This does not delete the
   * actual ZNode.
   * @throws IOException If there is an unrecoverable error talking to
   *                     ZooKeeper
   */
  public void clear() throws IOException {
    try {
      zooKeeper.setData(fullyQualifiedZNode, null, expectedZNodeVersion.get());
    } catch (KeeperException.BadVersionException e) {
      LOG.error(fullyQualifiedZNode + " has been updated by another process",
          e);
      throw new StaleVersionException(fullyQualifiedZNode +
          "has been updated by another process!");
    } catch (KeeperException e) {
      keeperException("Unrecoverable ZooKeeper error clearing " +
          fullyQualifiedZNode, e);
    } catch (InterruptedException e) {
      interruptedException("Interrupted clearing " + fullyQualifiedZNode, e);
    }
  }

}
