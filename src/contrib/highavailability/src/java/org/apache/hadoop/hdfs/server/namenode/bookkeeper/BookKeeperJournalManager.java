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
import com.google.common.collect.Lists;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.util.ZkUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.protocol.FSConstants;
import org.apache.hadoop.hdfs.server.common.HdfsConstants;
import org.apache.hadoop.hdfs.server.common.HdfsConstants.StartupOption;
import org.apache.hadoop.hdfs.server.common.HdfsConstants.Transition;
import org.apache.hadoop.hdfs.server.common.StorageInfo;
import org.apache.hadoop.hdfs.server.namenode.EditLogInputStream;
import org.apache.hadoop.hdfs.server.namenode.EditLogOutputStream;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogLoader;
import org.apache.hadoop.hdfs.server.namenode.JournalManager;
import org.apache.hadoop.hdfs.server.namenode.RemoteStorageState;
import org.apache.hadoop.hdfs.server.namenode.bookkeeper.metadata.BookKeeperJournalMetadataManager;
import org.apache.hadoop.hdfs.server.namenode.bookkeeper.metadata.CurrentInProgressMetadata;
import org.apache.hadoop.hdfs.server.namenode.bookkeeper.metadata.EditLogLedgerMetadata;
import org.apache.hadoop.hdfs.server.namenode.bookkeeper.metadata.MaxTxId;
import org.apache.hadoop.hdfs.server.namenode.bookkeeper.metadata.Versioned;
import org.apache.hadoop.hdfs.server.namenode.bookkeeper.metadata.proto.FormatInfoWritable;
import org.apache.hadoop.hdfs.server.namenode.bookkeeper.metadata.proto.WritableUtil;
import org.apache.hadoop.hdfs.server.namenode.bookkeeper.zk.BasicZooKeeper;
import org.apache.hadoop.hdfs.server.namenode.bookkeeper.zk.ConnectionWatcher;
import org.apache.hadoop.hdfs.server.namenode.bookkeeper.zk.RecoveringZooKeeper;
import org.apache.hadoop.hdfs.server.namenode.metrics.NameNodeMetrics;
import org.apache.hadoop.hdfs.server.protocol.NamespaceInfo;
import org.apache.hadoop.hdfs.server.protocol.RemoteEditLog;
import org.apache.hadoop.hdfs.server.protocol.RemoteEditLogManifest;
import org.apache.hadoop.hdfs.util.InjectionEvent;
import org.apache.hadoop.util.InjectionHandler;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.apache.hadoop.hdfs.server.namenode.bookkeeper.BookKeeperJournalConfigKeys.*;
import static org.apache.hadoop.hdfs.server.namenode.bookkeeper.zk.ZkUtil.*;
import static org.apache.zookeeper.AsyncCallback.*;

/**
 * BookKeeper-based JournalManager implementation. This is inspired by
 * Apache's BookKeeperJournalManager, with several core differences:
 * interaction with ZooKeeper goes through {@link RecoveringZooKeeper},
 * custom {@link BookKeeperEditLogInputStream} implementation is used that
 * permits tailing in-progress edits and re-positioning within an ledger-based
 * output stream, and a custom {@link BookKeeperEditLogOutputStream} is used
 * that uses double buffer as used by the standard file journal manager
 * implementation.
 */
public class BookKeeperJournalManager implements JournalManager, LedgerHandleProvider {

  private static final Log LOG =
      LogFactory.getLog(BookKeeperJournalManager.class);

  // Version of the protocol used for serializing and de-serializing data in
  // znodes (i.e., the Writables)

  static final int PROTO_VERSION = -1;

  private final Configuration conf; // Configuration

  private final int quorumSize; // BookKeeper quorum size
  private final int ensembleSize; // BookKeeper cluster size
  private final BookKeeper bookKeeperClient; // BookKeeper client
  private final RecoveringZooKeeper zk;

  private final String digestPw; // BookKeeper digest password
  @VisibleForTesting
  protected final String zkParentPath; // Parent ZNode
  @VisibleForTesting
  protected final String formatInfoPath; // ZNode holding format/namespace information

  // Handles ledger metadata
  final BookKeeperJournalMetadataManager metadataManager;
  private final MaxTxId maxTxId;  // stores max txid

  private final CurrentInProgressMetadata currentInProgressMetadata;

  private boolean initialized = false;
  private LedgerHandle currentInProgressLedger = null; // Current ledger

  @VisibleForTesting
  volatile String currentInProgressPath;

  private volatile NameNodeMetrics metrics = null;

  private long maxSeenTxId = -1;

  private static final ThreadLocal<FormatInfoWritable>
      localFormatInfoWritable = new ThreadLocal<FormatInfoWritable>() {
    @Override
    protected FormatInfoWritable initialValue() {
      return new FormatInfoWritable();
    }
  };

  public BookKeeperJournalManager(Configuration conf, URI uri,
      NamespaceInfo nsInfo, NameNodeMetrics metrics)
      throws IOException {
    this.conf = conf;
    this.metrics = metrics;
    quorumSize = conf.getInt(BKJM_BOOKKEEPER_QUORUM_SIZE,
        BKJM_BOOKKEEPER_QUORUM_SIZE_DEFAULT);
    ensembleSize = conf.getInt(BKJM_BOOKKEEPER_ENSEMBLE_SIZE,
        BKJM_BOOKKEEPER_ENSEMBLE_SIZE_DEFAULT);
    digestPw = conf.get(BKJM_BOOKKEEPER_DIGEST_PW,
        BKJM_BOOKKEEPER_DIGEST_PW_DEFAULT);
    String zkConnect = uri.getAuthority().replace(";", ",");
    zkParentPath = uri.getPath();
    String ledgersAvailablePath = conf.get(
        BKJM_ZK_LEDGERS_AVAILABLE_PATH,
        BKJM_ZK_LEDGERS_AVAILABLE_PATH_DEFAULT);
    formatInfoPath = joinPath(zkParentPath, "version");
    String currentInProgressPath = joinPath(zkParentPath, "CurrentInProgress");
    String maxTxIdPath = joinPath(zkParentPath, "maxtxid");
    int zkSessionTimeoutMs = conf.getInt(BKJM_ZK_SESSION_TIMEOUT,
        BKJM_ZK_SESSION_TIMEOUT_DEFAULT);
    int zkMaxRetries = conf.getInt(BKJM_ZK_MAX_RETRIES,
       BKJM_ZK_MAX_RETRIES_DEFAULT);
    int zkRetryIntervalMs = conf.getInt(BKJM_ZK_RETRY_INTERVAL,
        BKJM_ZK_RETRY_INTERVAL_DEFAULT);
    CountDownLatch connectLatch = new CountDownLatch(1);
    ConnectionWatcher connectionWatcher = new ConnectionWatcher(connectLatch);
    ZooKeeper zooKeeper = new ZooKeeper(zkConnect, zkSessionTimeoutMs,
        connectionWatcher);
    // Use twice session timeout as the connection timeout
    int zkConnectTimeoutMs = zkSessionTimeoutMs * 2;

    if (!connectionWatcher.await(zkConnectTimeoutMs)) {
      throw new IOException("Timed out waiting to connect to " + zkConnect
          + " after " + (zkSessionTimeoutMs * 2) + " ms.");
    }
    prepareBookKeeperEnv(ledgersAvailablePath, zooKeeper);

    try {
      ClientConfiguration clientConf = new ClientConfiguration();
      clientConf.setClientTcpNoDelay(conf.getBoolean(
          BKJM_BOOKKEEPER_CLIENT_TCP_NODELAY,
          BKJM_BOOKKEEPER_CLIENT_TCP_NO_DELAY_DEFAULT));
      clientConf.setThrottleValue(conf.getInt(BKJM_BOOKKEEPER_CLIENT_THROTTLE,
          BKJM_BOOKKEEPER_CLIENT_THROTTLE_DEFAULT));
      bookKeeperClient = new BookKeeper(clientConf, zooKeeper);
    } catch (KeeperException e) {
      keeperException("Unrecoverable ZooKeeper creating BookKeeper client",
          e);
      throw new IllegalStateException(e); // never reached
    } catch (InterruptedException e) {
      interruptedException("Interrupted creating a BookKeeper client", e);
      throw new IllegalStateException(e); // never reached
    }
    zk = new RecoveringZooKeeper(new BasicZooKeeper(zooKeeper), zkMaxRetries,
        zkRetryIntervalMs);
    metadataManager = new BookKeeperJournalMetadataManager(zk, zkParentPath);
    maxTxId = new MaxTxId(zk, maxTxIdPath);
    currentInProgressMetadata = new CurrentInProgressMetadata(zk,
        currentInProgressPath);
    createZkMetadataIfNotExists(nsInfo);
    metadataManager.init();
  }

  public static void bkException(String msg, BKException e) throws IOException {
    LOG.error(msg, e);
    throw new IOException(msg, e);
  }

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

  /**
   * If environment information has yet not been read during the object's life
   * do so and verify that it has been written the expected protocol version.
   * Additionally, the call always refreshes the object's current
   * {@link CurrentInProgressMetadata} information.
   */
  synchronized private void checkEnv() throws IOException {
    if (!initialized) {
      FormatInfoWritable writable = localFormatInfoWritable.get();
      if (metadataManager.readWritableFromZk(formatInfoPath, writable, null) == null) {
        LOG.error("Environment not initialized (format() not called?)");
        throw new IOException(
            "Environment not initialized (format() not called?");
      }
      if (writable.getProtoVersion() != PROTO_VERSION) {
        throw new IllegalStateException("Wrong protocol version! Expected " +
            BKJM_BOOKKEEPER_DIGEST_PW + ", but read " +
            writable.getProtoVersion());
      }
      if (LOG.isDebugEnabled()) {
        LOG.debug("Namespace info read: " + writable.toColonSeparatedString());
      }
    }
    currentInProgressMetadata.init();
    initialized = true;
  }

  @VisibleForTesting
  public LedgerHandle openForReading(long ledgerId) throws IOException {
    try {
      return bookKeeperClient.openLedgerNoRecovery(
          ledgerId, BookKeeper.DigestType.MAC, digestPw.getBytes());
    } catch (InterruptedException e) {
      interruptedException("Interrupted while opening ledger id " + ledgerId +
          " for reading", e);
    } catch (BKException e) {
      bkException("BookKeeper error opening ledger id " + ledgerId +
          " for reading", e);
    }
    return null; // Should not be reached
  }

  @Override
  public void transitionJournal(StorageInfo si, Transition transition,
      StartupOption startOpt) throws IOException {
    if (transition == Transition.FORMAT) {
      deleteMetadataAndLedgers();
      createZkMetadataIfNotExists(si);
      metadataManager.init();
    } else {
      throw new UnsupportedOperationException();
    }
  }

  /**
   * If ZooKeeper metadata is not empty, forcefully delete the metadata
   * and make a best effort attempt at deleting the ledgers. Used by
   * {@link #formatJournal(StorageInfo)}
   * @throws IOException If there is an error talking to BookKeeper or
   *                     ZooKeeper
   */
  private void deleteMetadataAndLedgers() throws IOException {
    try {
      if (hasSomeJournalData()) {
        if (zkPathExists(metadataManager.getLedgerParentPath())) {
          for (EditLogLedgerMetadata ledger : metadataManager.listLedgers(true)) {
            try {
              // Try to delete the individual ledger from BookKeeper
              bookKeeperClient.deleteLedger(ledger.getLedgerId());
            } catch (BKException e) {
              // It is fine if we are unable to delete the ledger, as it will
              // not be read and can then be deleted manually.
              LOG.warn("Unable to delete ledger " + ledger + " from BookKeeper",
                  e);
            } catch (InterruptedException e) {
              interruptedException("Interrupted deleting ledger " + ledger, e);
            }
          }
        }
        deleteRecursively(zk, zkParentPath);
      }
    } catch (IOException e) {
      LOG.error("Error clearing out metadata under " + zkParentPath, e);
      throw e;
    }
  }

  /**
   * If there is no metadata present in ZooKeeper, create and populate the
   * metadata with the right format information
   * @param si The format information to set
   * @throws IOException If there is an error writing to ZooKeeper
   */
  private void createZkMetadataIfNotExists(StorageInfo si) throws IOException {
    try {
      if (!hasSomeJournalData()) {
        try {
          // First create the parent path
          zk.create(zkParentPath, new byte[] { '0' },
              Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

          // Write format/namespace information to ZooKeeper
          FormatInfoWritable writable = localFormatInfoWritable.get();
          writable.set(PROTO_VERSION, si);
          byte[] data = WritableUtil.writableToByteArray(writable);
          zk.create(formatInfoPath, data, Ids.OPEN_ACL_UNSAFE,
              CreateMode.PERSISTENT);
        } catch (KeeperException e) {
          keeperException("Unrecoverable ZooKeeper error initializing " +
              zkParentPath, e);
        } catch (InterruptedException e) {
          interruptedException("Interrupted initializing " + zkParentPath +
              " in ZooKeeper", e);
        }
      }
    } catch (IOException e) {
      LOG.error("Unable to initialize metadata", e);
      throw e;
    }
  }

  /**
   * Check if a path exists in ZooKeeper
   * @param path The ZNode path to check
   * @return True if path exists, false if otherwise
   * @throws IOException If there is an error talking to ZooKeeper
   */
  private boolean zkPathExists(String path) throws IOException {
    try {
      return zk.exists(path, false) != null;
    } catch (KeeperException e) {
      keeperException("Unrecoverable ZooKeeper error checking if " +
          path + " exists", e);
    } catch (InterruptedException e) {
      interruptedException("Interrupted checking if ZooKeeper path " +
          path + " exists", e);
    }
    return false; // Should never be reached
  }

  @Override
  public EditLogOutputStream startLogSegment(long txId) throws IOException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Trying to start a log segment at txId " + txId);
    }

    checkEnv();

    try {
      long currMaxTxId = maxTxId.get();
      if (txId <= currMaxTxId) {
        throw new IOException("Already saw up to txId " + currMaxTxId + "!");
      }

      String existingInProgress = currentInProgressMetadata.read();
      if (existingInProgress != null &&
          metadataManager.ledgerExists(existingInProgress)) {
        throw new IOException(existingInProgress + " already exists, cannot "
            + " start a log segment that is already in progress!");
      }
    } catch (IOException e) {
      LOG.error("Unable to start log segment for txId " + txId, e);
      throw e;
    }

    try {
      // There was an error handling on the last stream, so close it
      if (currentInProgressLedger != null) {
        currentInProgressLedger.close();
      }
      currentInProgressLedger = bookKeeperClient.createLedger(ensembleSize,
          quorumSize, BookKeeper.DigestType.MAC, digestPw.getBytes());
    } catch (BKException e) {
      bkException("BookKeeper error creating ledger for txId " + txId, e);
    } catch (InterruptedException e) {
      interruptedException("Interrupted creating ledger for txId " + txId, e);
    }

    // Create metadata for associated with the edit log segment starting at
    // txId in ZooKeeper
    EditLogLedgerMetadata ledgerMetadata = new EditLogLedgerMetadata(
        FSConstants.LAYOUT_VERSION, currentInProgressLedger.getId(), txId, -1);
    String ledgerFullPath =
        metadataManager.fullyQualifiedPathForLedger(ledgerMetadata);
    metadataManager.writeEditLogLedgerMetadata(ledgerFullPath, ledgerMetadata);
    maxTxId.store(txId);
    currentInProgressMetadata.update(ledgerFullPath);

    // Used by recoverUnfinalizedSegments()
    currentInProgressPath = ledgerFullPath;

    BookKeeperEditLogOutputStream out = new BookKeeperEditLogOutputStream(
        currentInProgressLedger, zkParentPath, metrics);
    out.create(); // Write the ledger header and flush it to BookKeeper

    InjectionHandler.processEvent(InjectionEvent.BKJM_STARTLOGSEGMENT,
        ledgerMetadata);
    return out;
  }

  @Override
  public void finalizeLogSegment(long firstTxId, long lastTxId)
      throws IOException {
    checkEnv();

    try {
      // First, find an in-progress ledger starting at firstTxId
      Versioned<EditLogLedgerMetadata> inProgressMetaAndVersion =
          metadataManager.findInProgressLedger(firstTxId);

      if (inProgressMetaAndVersion == null) {
        throw new IOException(
            "Cannot find metadata for an in-progress ledger with first txId "
                + firstTxId);
      }

      EditLogLedgerMetadata inProgressMeta = inProgressMetaAndVersion.getEntry();

      if (currentInProgressLedger != null) {
        long inProgressLedgerId = currentInProgressLedger.getId();

        if (inProgressMeta.getLedgerId() == inProgressLedgerId) {
          // If the segment is already // If the segment is currently
          // in-progress, then finalize the ledger (this ensures every entry
          // in the ledger committed to the BookKeeper quorum)
          try {
            currentInProgressLedger.close();
          } catch (BKException e) {
            bkException("Unexpected BookKeeper error closing ledger id " +
                inProgressLedgerId, e);
          } catch (InterruptedException e) {
            interruptedException("Interrupted closing ledger id " +
                inProgressLedgerId, e);
          }
          currentInProgressPath = null;
          currentInProgressLedger = null;
        } else { // We can not finalize a ledger that is not in-progress
          throw new IOException("Current in-progress ledger has ledger id (" +
              inProgressLedgerId + ") different from expected ledger id " +
              inProgressMeta.getLedgerId());
        }
      }

      // Set lastTxId in the metadata and persist it to ZooKeeper
      EditLogLedgerMetadata finalizedMeta =
          inProgressMeta.finalizeWithLastTxId(lastTxId);
      String finalizedPath =
          metadataManager.fullyQualifiedPathForLedger(finalizedMeta);
      if (LOG.isDebugEnabled()) {
        LOG.debug("Attempting to finalize metadata " + finalizedMeta +
            " to ZNode " + finalizedPath);
      }
      if (!metadataManager.writeEditLogLedgerMetadata(finalizedPath, finalizedMeta)
          && !metadataManager.verifyEditLogLedgerMetadata(inProgressMeta, finalizedPath)) {
        throw new IOException("Node " + finalizedPath +
            " already exists, but data doesn't match " + finalizedMeta);
      }
      maxTxId.store(lastTxId);


      // Find the ZNode path for the metadata associated with the in-progress
      // version of the ledger
      String lastInProgressPath =
          metadataManager.fullyQualifiedPathForLedger(inProgressMeta);
      String inProgressPathFromCiMeta = currentInProgressMetadata.read();
      if (lastInProgressPath.equals(inProgressPathFromCiMeta)) {
        // If the ZNode path matches the ZNode path for the current in-progress
        // metadata, then clear the current in-progress metadata
        currentInProgressMetadata.clear();
      }

      // Delete the in-progress metadata iff no one else has updated it in
      // the mean while
      if (!metadataManager.deleteLedgerMetadata(inProgressMeta,
          inProgressMetaAndVersion.getVersion())) {
        throw new IOException(
            "Unable to delete in-progress znode " + lastInProgressPath +
                " as it no longer exists (Deleted by another process?)");
      }
    } catch (IOException e) {
      LOG.error("Unable to finalized metadata for segment with firstTxId " +
          firstTxId + ", lastTxId " + lastTxId, e);
      throw e;
    }
  }

  /**
   * An implementation of {@link LedgerHandleProvider} that fences the
   * ledger we are reading from, allowing the ledger to be recovered by
   * BookKeeper as we validate it.
   *
   * @see BookKeeperEditLogInputStream#validateEditLog(LedgerHandleProvider, EditLogLedgerMetadata)
   */
  class FencingLedgerHandleProvider implements LedgerHandleProvider {

    @Override
    public LedgerHandle openForReading(long ledgerId) throws IOException {
      try {
        LOG.info("Opening ledger id " + ledgerId + " for recovery...");

        LedgerHandle lh = bookKeeperClient.openLedger(ledgerId,
            BookKeeper.DigestType.MAC, digestPw.getBytes());

        if (lh.getId() != ledgerId) { // Verify that correct ledger is opened
          throw new IllegalStateException("Ledger id " + lh.getId() +
              " does not match requested ledger id " + ledgerId);
        }

        LOG.info("Opened ledger id " + ledgerId + " for recovery!");
        return lh;
      } catch (BKException e) {
        bkException("BookKeeper error opening ledger id " + ledgerId +
            " for recovery", e);
      } catch (InterruptedException e) {
        interruptedException("Interrupted opening ledger id " + ledgerId +
            "for recovery", e);
      }
      return null;
    }

  }

  @VisibleForTesting
  long validateAndGetEndTxId(EditLogLedgerMetadata ledger) throws IOException {
    return validateAndGetEndTxId(ledger, false);
  }

  long validateAndGetEndTxId(EditLogLedgerMetadata ledger, boolean fence)
      throws IOException {
    FSEditLogLoader.EditLogValidation val;
    if (!fence) {
      val = BookKeeperEditLogInputStream.validateEditLog(this, ledger);
    } else {
      val = BookKeeperEditLogInputStream.validateEditLog(
          new FencingLedgerHandleProvider(), ledger);
    }
    InjectionHandler.processEvent(InjectionEvent.BKJM_VALIDATELOGSEGMENT,
        val);
    if (val.getNumTransactions() == 0) {
      return HdfsConstants.INVALID_TXID; // Ledger is corrupt
    }
    return val.getEndTxId();
  }

  private List<EditLogLedgerMetadata> getLedgers(long fromTxId) throws IOException {
    Collection<EditLogLedgerMetadata> allLedgers =
        metadataManager.listLedgers(true);
    List<EditLogLedgerMetadata> ledgers = new ArrayList<EditLogLedgerMetadata>();
    for (EditLogLedgerMetadata ledger : allLedgers) {
      if (ledger.getLastTxId() != -1 &&
          fromTxId > ledger.getFirstTxId() &&
          fromTxId <= ledger.getLastTxId()) {
        throw new IOException("Asked for fromTxId " + fromTxId +
            " which is in the middle of " + ledger);
      }
      if (fromTxId <= ledger.getFirstTxId()) {
        ledgers.add(ledger);
      }
    }

    return ledgers;
  }

  private long findMaxTransaction() throws IOException {
    List<EditLogLedgerMetadata> ledgers = getLedgers(0);
    synchronized (this) {
      for (EditLogLedgerMetadata ledgerMetadata : ledgers) {
        if (ledgerMetadata.getLastTxId() == -1) {
          maxSeenTxId = Math.max(ledgerMetadata.getFirstTxId(), maxSeenTxId);
        }
        maxSeenTxId = Math.max(ledgerMetadata.getLastTxId(), maxSeenTxId);
      }
    }
    return maxSeenTxId;
  }

  /**
   * For edit log segment that contains transactions with ids earlier than the
   * earliest txid to be retained, remove the ZooKeeper-based metadata and
   * BookKeeper ledgers associated with these segments.
   *
   * @param minTxIdToKeep the earliest txid that must be retained after purging
   *                      old logs
   * @throws IOException If there is an error talking to BookKeeper or
   *                     ZooKeeper
   */
  @Override
  public void purgeLogsOlderThan(long minTxIdToKeep) throws IOException {
    checkEnv();

    Collection<EditLogLedgerMetadata> ledgers =
        metadataManager.listLedgers(false); // Don't list in-progress ledgers

    for (EditLogLedgerMetadata ledger : ledgers) {
      if (ledger.getFirstTxId() < minTxIdToKeep  &&
          ledger.getLastTxId() < minTxIdToKeep) {
        LOG.info("Purging edit log segment: " + ledger);

        // Try to delete the associated ZooKeeper metadata
        if (!metadataManager.deleteLedgerMetadata(ledger, -1)) {
          // It's possible that another process has already deleted the
          // metadata
          LOG.warn(ledger + " has already been purged!");
        } else {
          try {
            // Remove the ledger from BookKeeper itself to reclaim diskspace.
            bookKeeperClient.deleteLedger(ledger.getLedgerId());
          } catch (BKException e) {
            bkException("Unrecoverable error deleting " + ledger +
                " from BookKeeper", e);
          } catch (InterruptedException e) {
            interruptedException("Interrupted deleting " + ledger +
                " from BookKeeper", e);
          }
        }
      }
    }
  }
  
  @Override
  public void setCommittedTxId(long txid, boolean force) {
  }

  @Override
  synchronized public void recoverUnfinalizedSegments() throws IOException {
    checkEnv();

    Collection<EditLogLedgerMetadata> allLedgers =
        metadataManager.listLedgers(true);

    for (EditLogLedgerMetadata ledger : allLedgers) {
      if (ledger.getLastTxId() != -1) {
        continue; // Only un-finalized segments may be recovered
      }

      String ledgerPath = metadataManager.fullyQualifiedPathForLedger(ledger);
      if (currentInProgressPath != null &&
          ledgerPath.equals(currentInProgressPath)) {
        // Do not recover the current in-progress segment
        continue;
      }

      // First open the ledger without fencing in order to check the length
      // of the ledger (to check for any zero-length ledgers that may have
      // been the result of a crash).
      LedgerHandle ledgerHandle = openForReading(ledger.getLedgerId());
      try {
        if (ledgerHandle.getLength() == 0) {
          handleZeroLengthLedger(ledger); // Delete any zero-length ledgers
          continue;
        }
      } finally {
        try {
          ledgerHandle.close();
        } catch (BKException e) {
          bkException("BookKeeper error closing ledger id " +
              ledger.getLedgerId(), e);
        } catch (InterruptedException e) {
          interruptedException("Interrupted closing ledger id " +
          ledger.getLedgerId(), e);
        }
      }

      // Fence the ledger and validate it as it's being recovered by BookKeeper
      long endTxId = validateAndGetEndTxId(ledger, true);

      findMaxTransaction(); // Update maxTxId seen so far by this instance

      if (endTxId == HdfsConstants.INVALID_TXID) {
        LOG.warn(ledger + "(" + ledgerPath + ")" + " cannot be recovered!");
        metadataManager.moveAsideCorruptLedger(ledger);
        continue;
      }

      // Now finalize the ledger
      finalizeLogSegment(ledger.getFirstTxId(), endTxId);
    }
  }

  private void handleZeroLengthLedger(EditLogLedgerMetadata ledger)
      throws IOException {
    LOG.warn("In-progress edit log segment " + ledger + " refers to an " +
       "empty edit log segment. This occurs when NameNode crashes after " +
       "opening a segment, but before writing OP_START_LOG_SEGMENT. Will " +
       "delete the ledger and the metadata.");
    if (maxTxId.get() == ledger.getFirstTxId()) {
      LOG.warn("maxTxId is set to " + ledger.getFirstTxId() + " which is " +
          "belongs to an empty ledger. Resetting to previous maxTxId.");
      maxTxId.set(maxTxId.get() - 1);
    }
    metadataManager.deleteLedgerMetadata(ledger, -1);
    try {
      bookKeeperClient.deleteLedger(ledger.getLedgerId());
    } catch (BKException e) {
      bkException("BookKeeper error deleting empty ledger id " +
          ledger.getLedgerId(), e);
    } catch (InterruptedException e) {
      interruptedException(
          "Interrupted deleting empty ledger id " +
              ledger.getLedgerId(), e);
    }
  }

  @Override
  public RemoteEditLogManifest getEditLogManifest(long fromTxId)
      throws IOException {
    Collection<EditLogLedgerMetadata> ledgers =
        metadataManager.listLedgers(true);
    LOG.info("Ledgers to include in manifest: " + ledgers);

    List<RemoteEditLog> ret = Lists.newArrayListWithCapacity(ledgers.size());

    for (EditLogLedgerMetadata ledger : ledgers) {
      long endTxId = ledger.getLastTxId();
      boolean isInProgress = endTxId == -1;
      if (isInProgress) {
        endTxId = validateAndGetEndTxId(ledger);
      }

      if (endTxId == HdfsConstants.INVALID_TXID) {
        continue;
      }

      if (ledger.getFirstTxId() >= fromTxId) {
        ret.add(new RemoteEditLog(ledger.getFirstTxId(),
            endTxId,
            isInProgress));
      } else if ((fromTxId > ledger.getFirstTxId()) &&
                 (fromTxId <= endTxId)) {
        throw new IOException("Asked for firstTxId " + fromTxId +
            " which is in the middle of ledger " + ledger);
      }
    }

    Collections.sort(ret);
    return new RemoteEditLogManifest(ret, false);
  }

  private void closeBk() throws IOException {
    try {
      bookKeeperClient.close();
    } catch (BKException e) {
      bkException("Error closing BookKeeper client", e);
    } catch (InterruptedException e) {
      interruptedException("Interrupted closing BookKeeper client ", e);
    }
  }

  private void closeZk() throws IOException {
    try {
      zk.close();
    } catch (InterruptedException e) {
      interruptedException("Interrupted closing ZooKeeper client", e);
    }
  }

  @Override
  public void close() throws IOException {
    try {
      closeBk();
    } finally {
      if (!Thread.currentThread().isInterrupted()) {
        closeZk();
      }
    }
  }

  @Override
  public void selectInputStreams(Collection<EditLogInputStream> streams,
      long fromTxId,
      boolean inProgressOk,
      boolean validateInProgressSegments) throws IOException {
    Collection<EditLogLedgerMetadata> allLedgers = getLedgers(fromTxId);
    if (LOG.isDebugEnabled()) {
      LOG.debug(this + ": selecting input streams starting at " + fromTxId +
          (inProgressOk ? " (inProgress ok) " : "(excluding inProgress) " ) +
          "from among " + allLedgers.size() + " candidate ledger(s).");
    }
    addStreamsToCollectionFromLedgers(allLedgers, streams, fromTxId,
        inProgressOk, validateInProgressSegments);
  }

  void addStreamsToCollectionFromLedgers(
      Collection<EditLogLedgerMetadata> allLedgers,
      Collection<EditLogInputStream> streams, long fromTxId,
      boolean inProgressOk, boolean validateInProgressSegments) throws IOException {
    for (EditLogLedgerMetadata ledger : allLedgers) {
      long endTxId = ledger.getLastTxId();
      if (endTxId == -1) {
        if (!inProgressOk) {
          if (LOG.isDebugEnabled()) {
            LOG.debug("Passing over " + ledger + " because it is in progress " +
                " and we are ignoring in-progress logs.");
            continue;
          }
        }
        if (validateInProgressSegments) {
          try {
            endTxId = validateAndGetEndTxId(ledger);
          } catch (IOException e)  {
            LOG.error("Got an IOException while trying to validate header of "
                + ledger + ". Skipping.", e);
            continue;
          }
        } else {
          LOG.info("Skipping validation of edit segment: " + ledger);
        }
      }
      if (endTxId != HdfsConstants.INVALID_TXID && endTxId < fromTxId) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Passing over " + ledger + " because it ends at " +
              endTxId + ", but we only care about transaction as new as " +
              fromTxId);
        }
        continue;
      }
      BookKeeperEditLogInputStream bkelis = new BookKeeperEditLogInputStream(
          this, ledger.getLedgerId(), 0, ledger.getFirstTxId(), endTxId,
          ledger.getLastTxId() == -1);
      bkelis.setJournalManager(this);
      streams.add(bkelis);
    }
  }

  @Override
  public boolean hasSomeJournalData() throws IOException {
    return zkPathExists(zkParentPath);
  }
  
  @Override
  public boolean hasSomeImageData() throws IOException {
    return false;
  }

  @Override
  public String toHTMLString() {
    return "BKJM journal";
  }

  @Override
  public boolean hasImageStorage() {
    return false;
  }

  @Override
  public RemoteStorageState analyzeJournalStorage() {
    // TODO
    return null;
  }
}
