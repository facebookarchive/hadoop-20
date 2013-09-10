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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hdfs.server.namenode.bookkeeper.BookKeeperJournalManager;
import org.apache.hadoop.hdfs.server.namenode.bookkeeper.metadata.proto.EditLogLedgerMetadataWritable;
import org.apache.hadoop.hdfs.server.namenode.bookkeeper.metadata.proto.WritableUtil;
import org.apache.hadoop.hdfs.server.namenode.bookkeeper.zk.RecoveringZooKeeper;
import org.apache.hadoop.hdfs.server.namenode.bookkeeper.zk.ZooKeeperIface;
import org.apache.hadoop.io.Writable;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.TreeSet;

import static org.apache.hadoop.hdfs.server.namenode.bookkeeper.zk.ZkUtil.deleteRecursively;
import static org.apache.hadoop.hdfs.server.namenode.bookkeeper.zk.ZkUtil.interruptedException;
import static org.apache.hadoop.hdfs.server.namenode.bookkeeper.zk.ZkUtil.joinPath;
import static org.apache.hadoop.hdfs.server.namenode.bookkeeper.zk.ZkUtil.keeperException;

/**
 * Since BookKeeper only keeps track of numeric ledger ids, we need to
 * maintain our own mapping of edit log segment information (first and
 * last transaction ids, or whether or not a segment is in progress)
 * in a separate metadata store (currently this store is ZooKeeper).
 * </p>
 * The purpose of this class is to provide a path-based API for managing edit
 * log segment metadata associated with BookKeeper ledgers and to encapsulate
 * ZooKeeper handling (listing ZNode children, reading, writing, and
 * serializing to ZNodes), prevents "leaks" of ZooKeeper exception
 * (e.g., returning null if a ZNode we are trying to read does not exist,
 * and returning false if a ZNode we are trying to create already exists instead
 * of propagating ZooKeeper exceptions upwards) and can
 * (if {@link RecoveringZooKeeper} is passed in as {@link ZooKeeperIface}
 * implementation in the constructor) also handle recovery from ZooKeeper
 * errors such as connection loss.
 */
public class BookKeeperJournalMetadataManager {

  /** Prefix for ZNodes holding metadata for in-progress log segments */
  public static final String BKJM_EDIT_INPROGRESS = "inprogress_";

  /** Suffix for ZNodes holding metadata for corrupt regions */
  @VisibleForTesting
  public static final String BKJM_EDIT_CORRUPT = ".corrupt";

  private static final Log LOG =
      LogFactory.getLog(BookKeeperJournalMetadataManager.class);

  private static final ThreadLocal<EditLogLedgerMetadataWritable>
      localWritable = new ThreadLocal<EditLogLedgerMetadataWritable>(){
    @Override
    protected EditLogLedgerMetadataWritable initialValue() {
      return new EditLogLedgerMetadataWritable();
    }
  };

  // It is up to the implementation of ZooKeeperIface to handle retries
  // and re-connecting to ZooKeeper, e.g., RecoveringZooKeeper
  private final ZooKeeperIface zooKeeper;

  // A common prefix for all BK-related ZNodes. This can usually be set to
  // the name of the HDFS namespace
  private final String zooKeeperParentPath;
  // Information for a ledger would be stored as a child under this ZNode
  private final String ledgerParentPath;

  /**
   * Create a new instance and the required ZooKeeper znodes (if they do not
   * already exist).
   * @param zooKeeper The {@link ZooKeeperIface} implementation to use. This
   *                  instance is responsible for any handling of ZooKeeper
   *                  connection loss
   *                  <b>it is recommended to use {@link RecoveringZooKeeper}</b>
   * @param zooKeeperParentPath The ZooKeeper namespace for all
   *                            {@link BookKeeperJournalManager} related ZNodes.
   *                            This be can be the same as the HDFS namespace.
   * @throws IOException If unrecoverable error when initializing the
   *                     ZooKeeper namespace.
   */
  public BookKeeperJournalMetadataManager(ZooKeeperIface zooKeeper,
      String zooKeeperParentPath) throws IOException {
    this.zooKeeper = zooKeeper;
    this.zooKeeperParentPath = zooKeeperParentPath;
    this.ledgerParentPath =
        joinPath(zooKeeperParentPath, "ledgers");
  }

  public String getLedgerParentPath() {
    return ledgerParentPath;
  }

  /**
   * Create znodes for storing ledger metadata if they have not been
   * created before
   * @throws IOException If there an unrecoverable error talking to ZooKeeper
   */
  public void init() throws IOException {
    try {
      if (zooKeeper.exists(zooKeeperParentPath, false) == null) {
        zooKeeper.create(zooKeeperParentPath, new byte[] { '0' },
            ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        LOG.info("Created ZNode " + zooKeeperParentPath);
      }
      if (zooKeeper.exists(ledgerParentPath, false) == null) {
        zooKeeper.create(ledgerParentPath, new byte[] { '0' },
            ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        LOG.info("Created ZNode" + ledgerParentPath);
      }
    } catch (InterruptedException e) {
      interruptedException("Interrupted ensuring that ZNodes " +
          zooKeeperParentPath + " and " + ledgerParentPath + " exist!", e);
    } catch (KeeperException e) {
      keeperException(
          "Unrecoverable ZooKeeper error ensuring that ZNodes "
              + zooKeeperParentPath + " and " + ledgerParentPath + " exist!",
          e);
    }
  }

  /**
   * Create a name to use for a ZNode holding a ledger's metadata
   * @param e Metadata object that we want to create a name for
   * @return A string containing the name of a ledger's child ZNode
   */
  public static String nameForLedger(EditLogLedgerMetadata e) {
   return nameForLedger(e.getFirstTxId(), e.getLastTxId());
  }

  private static String nameForLedger(long firstTxId, long lastTxId) {
    boolean isInProgress = lastTxId == -1;
    StringBuilder nameBuilder = new StringBuilder();
    nameBuilder.append("ledger-");
    if (isInProgress) {
      nameBuilder.append(BKJM_EDIT_INPROGRESS)
          .append(firstTxId);
    } else {
      nameBuilder.append(firstTxId)
          .append("_")
          .append(lastTxId);
    }
    return nameBuilder.toString();
  }

  public Versioned<EditLogLedgerMetadata> findInProgressLedger(long firstTxId)
      throws IOException {
    String fullyQualifiedInProgressPath =
        fullyQualifiedPathForLedger(nameForLedger(firstTxId, -1));
    // TODO: try using a ThreadLocal here as well. Will test this once unit
    // test is written for this method
    Stat stat = new Stat();
    EditLogLedgerMetadataWritable metadataWritable = readWritableFromZk(
        fullyQualifiedInProgressPath, localWritable.get(), stat);
    return metadataWritable == null ?
        null : Versioned.of(stat.getVersion(), metadataWritable.get());
  }

  /**
   * Return the full ZNode path for a ZNode corresponding to the specified
   * ledger.
   * @param ledgerName The name of the ledger
   * @return Fully qualified ZNode path that may be read from or written to.
   */
  public String fullyQualifiedPathForLedger(String ledgerName) {
    return joinPath(ledgerParentPath, ledgerName);
  }

  /**
   * Return the full ZNode path for a ZNode corresponding to a specific
   * to a specific ledger's metadata.
   * @param e Metadata object that we want to get the full path for
   * @return Full path to the ledger's child ZNode
   */
  public String fullyQualifiedPathForLedger(EditLogLedgerMetadata e) {
    String nameForLedger = nameForLedger(e);
    return fullyQualifiedPathForLedger(nameForLedger);
  }

  /**
   * Removes ledger-related Metadata from BookKeeper. Does not delete
   * the ledger itself.
   * @param ledger The object for the ledger metadata that we want to delete
   *               from BookKeeper
   * @param version The version of the ledger metadata (or -1 to delete any
   *                version). Used as a way to guard against deleting a ledger
   *                metadata that is being updated by another process.
   * @return True if the process successfully deletes the metadata objection,
   *         false if it has already been deleted by another process.
   * @throws IOException If there is an error communicating to ZooKeeper or if
   *                     the metadata object has been modified within ZooKeeper
   *                     by another process (version mis-match).
   */
  public boolean deleteLedgerMetadata(EditLogLedgerMetadata ledger, int version)
      throws IOException {
    String ledgerPath = fullyQualifiedPathForLedger(ledger);
    try {
      zooKeeper.delete(ledgerPath, version);
      return true;
    } catch (KeeperException.NoNodeException e) {
      LOG.warn(ledgerPath + " does not exist. Returning false, ignoring " +
          e);
    } catch (KeeperException.BadVersionException e) {
      keeperException("Unable to delete " + ledgerPath + ", version does not match." +
          " Updated by another process?", e);
    } catch (KeeperException e) {
      keeperException("Unrecoverable ZooKeeper error deleting " + ledgerPath,
          e);
    } catch (InterruptedException e) {
      interruptedException("Interrupted deleting " + ledgerPath, e);
    }
    return false;
  }

  public boolean ledgerExists(String fullyQualifiedPath) throws IOException {
    try {
      return zooKeeper.exists(fullyQualifiedPath, false) != null;
    } catch (KeeperException e) {
      keeperException("Unrecoverable ZooKeeper error checking if " +
          fullyQualifiedPath + " exists!", e);
      return false; // Never reached
    } catch (InterruptedException e) {
      interruptedException("Interrupted checking if " + fullyQualifiedPath +
          " exists!", e);
      return false; // Never reached
    }
  }
  /**
   * Read a {@link Writable} from a specified ZNode path ZooKeeper and
   * (optionally) update its {@link Stat} information (if supplied).
   * The ZNode must either not exist or contain a valid writable: an empty
   * ZNode will result in an IllegalArgumentException being thrown.
   * @param fullyQualifiedPath Full path to the ZNode containing the writable
   * @param writable A newly instantiated instance of the writable. Must not
   *                 be null.
   * @param stat The stat object for ZNode stats or null if none desired.
   * @param <T> Type of the writable (must implement Writable)
   * @return The updated writable instance or null if the specified path does
   *         not exist
   * @throws IOException If there is an unrecoverable error communicating to
   *                     ZooKeeper
   * @throws IllegalArgumentException If the data contained in the specified
   *                                  ZNode is null
   */
  public <T extends Writable> T readWritableFromZk(
      String fullyQualifiedPath, T writable, Stat stat) throws IOException {
    try {
      byte[] data = zooKeeper.getData(fullyQualifiedPath, false, stat);
      if (data == null) {
        LOG.fatal("ZNode " + fullyQualifiedPath + " has no ledger metadata!");
        throw new IOException("ZNode " + fullyQualifiedPath +
            " has no ledger metadata!");
      }
      return WritableUtil.readWritableFromByteArray(data, writable);
    } catch (InterruptedException e) {
      interruptedException("Interrupted reading from " +
          fullyQualifiedPath, e);
      return null; // Should not be reached
    } catch (KeeperException.NoNodeException e) {
      LOG.warn("ZNode " + fullyQualifiedPath +
          " does not exist, returning null! Not re-throwing " + e);
      return null;
    } catch (KeeperException e) {
      keeperException("Unrecoverable ZooKeeper error reading from " +
          fullyQualifiedPath, e);
      return null;
    }
  }

  /**
   * Instantiate an {@link EditLogLedgerMetadata} from a specified ZNode path.
   * @param fullyQualifiedPath Full path to the ZNode containing the ledger
   *                           metadata.
   * @return The edit log ledger metadata or null if the ZNode does not exist
   * @throws IOException If there as an unrecoverable error communicating with
   *                     ZooKeeper
   */
  public EditLogLedgerMetadata readEditLogLedgerMetadata(
      String fullyQualifiedPath) throws IOException {
    EditLogLedgerMetadataWritable writable = localWritable.get();
    writable = readWritableFromZk(fullyQualifiedPath, writable, null);
    return writable == null ? null : writable.get();
  }

  /**
   * Write an {@link EditLogLedgerMetadata} object to a specified ZNode.
   * @param fullyQualifiedPath Full path to the ZNode containing the ledger
   *                           metadata.
   * @param editLogLedgerMetadata The edit log ledger metadata to write.
   * @return True if we wrote successfully to the specified ZNode, or false if
   *         the ZNode already exists.
   * @throws IOException If there is an unrecoverable error communicating with
   *         ZooKeeper.
   */
  public boolean writeEditLogLedgerMetadata(String fullyQualifiedPath,
      EditLogLedgerMetadata editLogLedgerMetadata) throws IOException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Attempting to write " + editLogLedgerMetadata + " to " +
          fullyQualifiedPath);
    }
    EditLogLedgerMetadataWritable writable = localWritable.get();
    writable.set(editLogLedgerMetadata);
    byte[] data = WritableUtil.writableToByteArray(writable);
    try {
      if (zooKeeper.exists(fullyQualifiedPath, false) != null) {
        LOG.warn(fullyQualifiedPath + " already exists. Will not override!");
        return false;
      }
      zooKeeper.create(fullyQualifiedPath, data, ZooDefs.Ids.OPEN_ACL_UNSAFE,
        CreateMode.PERSISTENT);
      return true;
    } catch (InterruptedException e) {
      interruptedException("Interrupted creating " + fullyQualifiedPath,
          e);
      return false; // never reached
    } catch (KeeperException.NodeExistsException e) {
      LOG.warn(fullyQualifiedPath +
          " already exists, returning false! Ignoring " + e);
      return false;
    } catch (KeeperException e) {
      keeperException("Unrecoverable ZooKeeper error creating " +
          fullyQualifiedPath, e);
      return false;
    }
  }

  /**
   * Verify that the specified EditLogLedgerMetadata instance is the same
   * as the EditLogLedgerMetadata object stored in the specified ZNode path.
   * @param metadata The instance to compare against
   * @param fullPathToVerify The path to verify
   * @return True if we are able to successfully verify the metadata
   */
  public boolean verifyEditLogLedgerMetadata(EditLogLedgerMetadata metadata,
      String fullPathToVerify) {
    Preconditions.checkNotNull(metadata);
    try {
      EditLogLedgerMetadata otherMetadata =
          readEditLogLedgerMetadata(fullPathToVerify);
      if (otherMetadata == null) {
        LOG.warn("No metadata found " + fullPathToVerify + "!");
      }
      if (LOG.isTraceEnabled()) {
        LOG.trace("Verifying " + otherMetadata + " read from " +
            fullPathToVerify + " against " + metadata);
      }
      return metadata.equals(otherMetadata);
    } catch (IOException e) {
      LOG.error("Unrecoverable error when verifying " + fullPathToVerify, e);
    }
    return false;
  }

  public void moveAsideCorruptLedger(EditLogLedgerMetadata ledger)
      throws IOException {
    deleteLedgerMetadata(ledger, -1);
    writeEditLogLedgerMetadata(fullyQualifiedPathForLedger(ledger) +
        BKJM_EDIT_CORRUPT, ledger);
  }

  /**
   * List all ledgers in this instance's ZooKeeper namespace.
   * @param includeInProgressLedgers If true, will include in-progress
   *                                 (non-finalized) ledgers
   * @return A list of ledgers ordered (in increasing order) by the first
   *         transaction id, using
   *         {@link EditLogLedgerMetadata#compareTo(EditLogLedgerMetadata)}
   * @throws IOException
   */
  public Collection<EditLogLedgerMetadata> listLedgers(
      boolean includeInProgressLedgers) throws IOException {
    // Use TreeSet to sort ledgers by firstTxId
    TreeSet<EditLogLedgerMetadata> ledgers =
        new TreeSet<EditLogLedgerMetadata>();

    try {
      List<String> ledgerNames = zooKeeper.getChildren(ledgerParentPath,
          false);
      for (String ledgerName : ledgerNames) {
        if (ledgerName.endsWith(BKJM_EDIT_CORRUPT)) {
          continue;
        }
        if (!includeInProgressLedgers && ledgerName.contains(BKJM_EDIT_INPROGRESS)) {
          continue;
        }

        String fullLedgerMetadataPath = fullyQualifiedPathForLedger(ledgerName);
        EditLogLedgerMetadata metadata =
            readEditLogLedgerMetadata(fullLedgerMetadataPath);
        if (metadata != null) {
          if (LOG.isTraceEnabled()) {
            LOG.trace("Read " + metadata + " from " + fullLedgerMetadataPath);
          }
          ledgers.add(metadata);
        } else { // metadata would be returns null iff path doesn't exist
          LOG.warn("ZNode " + fullLedgerMetadataPath +
              " might have been finalized and deleted.");
        }
      }
    } catch (InterruptedException e) {
      interruptedException(
          "Interrupted listing ledgers under " + ledgerParentPath, e);
    } catch (KeeperException e) {
      keeperException("Unrecoverable ZooKeeper error listing ledgers " +
          "under " + ledgerParentPath, e);
    }
    return ledgers;
  }
}
