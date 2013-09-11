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
package org.apache.hadoop.hdfs.qjournal.server;

import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.net.URL;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.qjournal.protocol.JournalNotFormattedException;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocol;
import org.apache.hadoop.hdfs.qjournal.protocol.JournalOutOfSyncException;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.GetStorageStateProto;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.NewEpochResponseProto;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.PersistedRecoveryPaxosData;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.PrepareRecoveryResponseProto;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.SegmentStateProto;
import org.apache.hadoop.hdfs.qjournal.protocol.RequestInfo;
import org.apache.hadoop.hdfs.server.common.HdfsConstants;
import org.apache.hadoop.hdfs.server.common.HdfsConstants.StartupOption;
import org.apache.hadoop.hdfs.server.common.HdfsConstants.Transition;
import org.apache.hadoop.hdfs.server.common.StorageErrorReporter;
import org.apache.hadoop.hdfs.server.namenode.EditLogOutputStream;
import org.apache.hadoop.hdfs.server.namenode.FSEditLog;
import org.apache.hadoop.hdfs.server.namenode.FSImageStorageInspector.FSImageFile;
import org.apache.hadoop.hdfs.server.namenode.FileImageManager;
import org.apache.hadoop.hdfs.server.namenode.FileJournalManager;
import org.apache.hadoop.hdfs.server.namenode.FileJournalManager.EditLogFile;
import org.apache.hadoop.hdfs.server.namenode.FSImageTransactionalStorageInspector;
import org.apache.hadoop.hdfs.server.namenode.ImageSet;
import org.apache.hadoop.hdfs.server.namenode.JournalManager;
import org.apache.hadoop.hdfs.server.namenode.TransferFsImage;
import org.apache.hadoop.hdfs.server.protocol.NamespaceInfo;
import org.apache.hadoop.hdfs.server.protocol.RemoteEditLogManifest;
import org.apache.hadoop.hdfs.server.protocol.RemoteImage;
import org.apache.hadoop.hdfs.server.protocol.RemoteImageManifest;
import org.apache.hadoop.hdfs.util.AtomicFileOutputStream;
import org.apache.hadoop.hdfs.util.BestEffortLongFile;
import org.apache.hadoop.hdfs.util.MD5FileUtils;
import org.apache.hadoop.hdfs.util.PersistentLongFile;
import org.apache.hadoop.io.MD5Hash;
import org.apache.hadoop.io.ShortVoid;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.ipc.Server;

import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Range;
import com.google.common.collect.Ranges;

/**
 * A JournalNode can manage journals and optionally images for several clusters
 * at once. Each such journal is entirely independent despite being hosted by
 * the same JVM. 
 */
public class Journal implements Closeable {
  public static final Log LOG = LogFactory.getLog(Journal.class);
  
  private final JournalNode journalNode;


  // Current writing state
  private EditLogOutputStream curSegment;
  private long curSegmentTxId = HdfsConstants.INVALID_TXID;
  private long nextTxId = HdfsConstants.INVALID_TXID;
  private long highestWrittenTxId = 0;
  
  private long currentSegmentWrittenBytes;
  
  // minimum txid of the underlying segments (best effort)
  // updated when we purge logs
  private long minTxid = 0;
  
  private final String journalId;
  
  private final JNStorage journalStorage;
  private final JNStorage imageStorage;
  private final Map<Long, MD5Hash> checkpointImageDigests = new HashMap<Long,MD5Hash>();
  
  private final FileImageManager imageManager;
  private final FileJournalManager fjm;

  private long mostRecentCheckpointTxid = HdfsConstants.INVALID_TXID;

  /**
   * When a new writer comes along, it asks each node to promise
   * to ignore requests from any previous writer, as identified
   * by epoch number. In order to make such a promise, the epoch
   * number of that writer is stored persistently on disk.
   */
  private PersistentLongFile lastPromisedEpoch;

  /**
   * Each IPC that comes from a given client contains a serial number
   * which only increases from the client's perspective. Whenever
   * we switch epochs, we reset this back to -1. Whenever an IPC
   * comes from a client, we ensure that it is strictly higher
   * than any previous IPC. This guards against any bugs in the IPC
   * layer that would re-order IPCs or cause a stale retry from an old
   * request to resurface and confuse things.
   */
  private long currentEpochIpcSerial = -1;
  
  /**
   * The epoch number of the last writer to actually write a transaction.
   * This is used to differentiate log segments after a crash at the very
   * beginning of a segment. See the the 'testNewerVersionOfSegmentWins'
   * test case.
   */
  private PersistentLongFile lastWriterEpoch;
  
  /**
   * Lower-bound on the last committed transaction ID. This is not
   * depended upon for correctness, but acts as a sanity check
   * during the recovery procedures, and as a visibility mark
   * for clients reading in-progress logs.
   */
  private BestEffortLongFile committedTxnId;
  private long lastPersistedCommittedTxId = 0;
  
  private static final String LAST_PROMISED_FILENAME = "last-promised-epoch";
  private static final String LAST_WRITER_EPOCH = "last-writer-epoch";
  private static final String COMMITTED_TXID_FILENAME = "committed-txid";

  private final JournalMetrics metrics;
  
  /**
   * Time threshold for sync calls, beyond which a warning should be logged.
   */
  private static final int WARN_SYNC_MILLIS_THRESHOLD = 1000;

  Journal(File logDir, File imageDir, String journalId,
      StorageErrorReporter errorReporter, JournalNode journalNode) throws IOException {
    this.journalNode = journalNode;
    // initialize storage directories
    journalStorage = new JNStorage(logDir, errorReporter, false, journalNode.getConf());
    imageStorage = new JNStorage(imageDir, errorReporter, true, journalNode.getConf());

    // initialize journal and image managers
    this.fjm = new FileJournalManager(journalStorage.getSingularStorageDir(),
        null, errorReporter);

    this.imageManager = new FileImageManager(
        imageStorage.getStorageDirectory(), imageStorage);
    
    this.journalId = journalId;
    this.metrics = JournalMetrics.create(this);

    refreshCachedData();
     
    EditLogFile latest = scanStorageForLatestEdits();
    if (latest != null) {
      highestWrittenTxId = latest.getLastTxId();
      metrics.setLastWrittenTxId(highestWrittenTxId);
    }
  }

  /**
   * Reload any data that may have been cached. This is necessary
   * when we first load the Journal, but also after any formatting
   * operation, since the cached data is no longer relevant.
   * @throws IOException 
   */
  private synchronized void refreshCachedData() throws IOException {
    IOUtils.closeStream(committedTxnId);
    
    File currentDir = journalStorage.getSingularStorageDir().getCurrentDir();
    this.lastPromisedEpoch = new PersistentLongFile(
        new File(currentDir, LAST_PROMISED_FILENAME), 0);
    this.lastWriterEpoch = new PersistentLongFile(
        new File(currentDir, LAST_WRITER_EPOCH), 0);
    this.committedTxnId = new BestEffortLongFile(
        new File(currentDir, COMMITTED_TXID_FILENAME),
        HdfsConstants.INVALID_TXID);
    metrics.lastWriterEpoch.set(lastWriterEpoch.get());
  }
  
  /**
   * After an upgrade we must ensure that the current directory still holds all
   * epoch, committed txid and paxos files that it had before we did the
   * upgrade.
   */
  private synchronized void copyMetaFilesForUpgrade() throws IOException {
    Configuration conf = new Configuration();
    File currentDir = journalStorage.getSingularStorageDir().getCurrentDir();
    File prevDir = journalStorage.getSingularStorageDir().getPreviousTmp();
    FileSystem fs = FileSystem.getLocal(conf).getRaw();

    FileUtil.copy(new File(prevDir, LAST_PROMISED_FILENAME), fs, new File(
        currentDir, LAST_PROMISED_FILENAME), false, conf);
    FileUtil.copy(new File(prevDir, LAST_WRITER_EPOCH), fs, new File(
        currentDir, LAST_WRITER_EPOCH), false, conf);
    FileUtil.copy(new File(prevDir, COMMITTED_TXID_FILENAME), fs, new File(
        currentDir, COMMITTED_TXID_FILENAME), false, conf);
    FileUtil.copy(new File(prevDir, JNStorage.PAXOS_DIR), fs, new File(
        currentDir, JNStorage.PAXOS_DIR), false, conf);
  }

  /**
   * Scan the local storage directory, and return the segment containing
   * the highest transaction.
   * @return the EditLogFile with the highest transactions, or null
   * if no files exist.
   */
  private synchronized EditLogFile scanStorageForLatestEdits() throws IOException {
    if (!fjm.getStorageDirectory().getCurrentDir().exists()) {
      return null;
    }
    
    LOG.info("Scanning storage " + fjm);
    List<EditLogFile> files = fjm.getLogFiles(0);
    
    while (!files.isEmpty()) {
      EditLogFile latestLog = files.remove(files.size() - 1);
      latestLog.validateLog();
      LOG.info("Latest log is " + latestLog);
      if (latestLog.getLastTxId() == HdfsConstants.INVALID_TXID) {
        // the log contains no transactions
        LOG.warn("Latest log " + latestLog + " has no transactions. " +
            "moving it aside and looking for previous log");
        latestLog.moveAsideEmptyFile();
      } else {
        return latestLog;
      }
    }
    
    LOG.info("No files in " + fjm);
    return null;
  }

  public void transitionJournal(NamespaceInfo nsInfo,
      Transition transition, StartupOption startOpt) throws IOException {
    switch (transition) {
    case UPGRADE:
      doUpgradeJournal(nsInfo);
      break;
    case COMPLETE_UPGRADE:
      completeUpgradeJournal(nsInfo);
      break;
    case RECOVER:
      recoverJournal(startOpt);
      break;
    case FORMAT:
      formatJournal(nsInfo);
      break;
    case ROLLBACK:
      rollbackJournal(nsInfo);
      break;
    case FINALIZE:
      finalizeJournal();
      break;
    }
  }

  public void transitionImage(NamespaceInfo nsInfo,
      Transition transition, StartupOption startOpt) throws IOException {
    switch (transition) {
    case UPGRADE:
      doUpgradeImage(nsInfo);
      break;
    case COMPLETE_UPGRADE:
      completeUpgradeImage(nsInfo);
      break;
    case RECOVER:
      recoverImage(startOpt);
      break;
    case FORMAT:
      formatImage(nsInfo);
      break;
    case ROLLBACK:
      rollbackImage(nsInfo);
      break;
    case FINALIZE:
      finalizeImage();
      break;
    }
  }

  /**
   * Format the local storage with the given namespace.
   */
  private void formatImage(NamespaceInfo nsInfo) throws IOException {
    Preconditions.checkState(nsInfo.getNamespaceID() != 0,
        "can't format with uninitialized namespace info: %s",
        nsInfo.toColonSeparatedString());
    LOG.info("Formatting image " + this.getJournalId() + " with namespace info: (" +
        nsInfo.toColonSeparatedString() + ")");
    imageStorage.backupDirs();
    imageStorage.format(nsInfo);
    // clear obsolete image digests
    checkpointImageDigests.clear();
  }

  /**
   * Finalize upgrade for the local image storage.
   */
  public void finalizeImage() throws IOException {
    LOG.info("Finalizing upgrade for journal " + this.getJournalId());
    imageStorage.finalizeStorage();
  }

  /**
   * Rollback the local image storage with the given namespace.
   */
  public void rollbackImage(NamespaceInfo nsInfo) throws IOException {
    Preconditions.checkState(nsInfo.getLayoutVersion() != 0,
        "can't rollback with uninitialized layout version: %s",
        nsInfo.toColonSeparatedString());
    LOG.info("Rolling back image " + this.getJournalId()
        + " with namespace info: (" + nsInfo.toColonSeparatedString() + ")");
    imageStorage.rollback(nsInfo);
  }

  /**
   * Upgrade the local image storage with the given namespace.
   */
  private void doUpgradeImage(NamespaceInfo nsInfo) throws IOException {
    Preconditions.checkState(nsInfo.getNamespaceID() != 0,
        "can't upgrade with uninitialized namespace info: %s",
        nsInfo.toColonSeparatedString());
    LOG.info("Upgrading image " + this.getJournalId()
        + " with namespace info: (" + nsInfo.toColonSeparatedString() + ")");

    // clear the digest for the most recent image, it might change during
    // upgrade
    checkpointImageDigests.remove(mostRecentCheckpointTxid);

    imageStorage.doUpgrade(nsInfo);
  }

  /**
   * Complete the upgrade for local image storage with the given namespace.
   */
  private void completeUpgradeImage(NamespaceInfo nsInfo) throws IOException {
    Preconditions.checkState(nsInfo.getNamespaceID() != 0,
        "can't upgrade with uninitialized namespace info: %s",
        nsInfo.toColonSeparatedString());
    LOG.info("Completing Upgrading image " + this.getJournalId()
        + " with namespace info: (" + nsInfo.toColonSeparatedString() + ")");
    // Do something about checkpoint image digests.

    imageStorage.completeUpgrade(nsInfo);
  }

  /**
   * Format the local storage with the given namespace.
   */
  private void formatJournal(NamespaceInfo nsInfo) throws IOException {
    Preconditions.checkState(nsInfo.getNamespaceID() != 0,
        "can't format with uninitialized namespace info: %s",
        nsInfo.toColonSeparatedString());
    LOG.info("Formatting journal" + this.getJournalId()
        + " with namespace info: (" + nsInfo.toColonSeparatedString() + ")");
    journalStorage.backupDirs();
    journalStorage.format(nsInfo);
    refreshCachedData();
  }

  /**
   * Complete the upgrade for local journal storage with the given namespace.
   */
  private void completeUpgradeJournal(NamespaceInfo nsInfo) throws IOException {
    Preconditions.checkState(nsInfo.getNamespaceID() != 0,
        "can't upgrade with uninitialized namespace info: %s",
        nsInfo.toColonSeparatedString());
    LOG.info("Completing Upgrading journal" + this.getJournalId()
        + " with namespace info: (" + nsInfo.toColonSeparatedString() + ")");
    journalStorage.completeUpgrade(nsInfo);
  }

  /**
   * Finalize upgrade for the local journal storage.
   */
  public void finalizeJournal() throws IOException {
    LOG.info("Finalizing upgrade for journal " + this.getJournalId());
    journalStorage.finalizeStorage();
  }

  /**
   * Rollback the local journal storage with the given namespace.
   */
  public void rollbackJournal(NamespaceInfo nsInfo) throws IOException {
    Preconditions.checkState(nsInfo.getLayoutVersion() != 0,
        "can't rollback with uninitialized layout version : %s",
        nsInfo.toColonSeparatedString());
    LOG.info("Rolling back journal " + this.getJournalId()
        + " with namespace info: (" + nsInfo.toColonSeparatedString() + ")");
    journalStorage.rollback(nsInfo);

    refreshCachedData();
  }

  /**
   * Upgrade the local journal storage with the given namespace.
   */
  private void doUpgradeJournal(NamespaceInfo nsInfo) throws IOException {
    Preconditions.checkState(nsInfo.getNamespaceID() != 0,
        "can't upgrade with uninitialized namespace info: %s",
        nsInfo.toColonSeparatedString());
    LOG.info("Upgrading journal " + this.getJournalId()
        + " with namespace info: (" + nsInfo.toColonSeparatedString() + ")");
    journalStorage.doUpgrade(nsInfo);

    copyMetaFilesForUpgrade();
    refreshCachedData();
  }

  /**
   * Recover the local journal storage.
   */
  private void recoverJournal(StartupOption startOpt) throws IOException {
    LOG.info("Recovering journal " + this.getJournalId());
    journalStorage.recover(startOpt);
  }

  /**
   * Recover the local image storage.
   */
  private void recoverImage(StartupOption startOpt) throws IOException {
    LOG.info("Recovering image" + this.getJournalId());
    imageStorage.recover(startOpt);
  }

  /**
   * Unlock and release resources.
   */
  @Override // Closeable
  public void close() throws IOException {
    journalStorage.close();
    imageStorage.close();
    
    IOUtils.closeStream(committedTxnId);
  }
  
  public JNStorage getJournalStorage() throws IOException {
    // this function is used by servlets to store and serve edit log segments
    // we should fail immediately if the edits storage is not formatted
    checkJournalStorageFormatted();
    return journalStorage;
  }
  
  
  public JNStorage getImageStorage() throws IOException {
    // this function is used by servlets to store and serve image
    // we should fail immediately if the image storage is not formatted
    checkImageStorageFormatted();
    return imageStorage;
  }
  
  String getJournalId() {
    return journalId;
  }

  /**
   * @return the last epoch which this node has promised not to accept
   * any lower epoch, or 0 if no promises have been made.
   */
  synchronized long getLastPromisedEpoch() throws IOException {
    checkJournalStorageFormatted();
    return lastPromisedEpoch.get();
  }

  synchronized public long getLastWriterEpoch() throws IOException {
    checkJournalStorageFormatted();
    return lastWriterEpoch.get();
  }
  
  synchronized long getCommittedTxnId() throws IOException {
    return committedTxnId.get();
  }
  
  synchronized long getCurrentLagTxns() throws IOException {
    long committed = committedTxnId.get();
    if (committed == 0) {
      return 0;
    }
    
    return Math.max(committed - highestWrittenTxId, 0L);
  }
  
  public synchronized long getHighestWrittenTxId() {
    return highestWrittenTxId;
  }
  
  JournalMetrics getMetrics() {
    return metrics;
  }

  /**
   * Try to create a new epoch for this journal.
   * @param nsInfo the namespace, which is verified for consistency or used to
   * format, if the Journal has not yet been written to.
   * @param epoch the epoch to start
   * @return the status information necessary to begin recovery
   * @throws IOException if the node has already made a promise to another
   * writer with a higher epoch number, if the namespace is inconsistent,
   * or if a disk error occurs.
   */
  synchronized NewEpochResponseProto newEpoch(
      NamespaceInfo nsInfo, long epoch) throws IOException {

    checkJournalStorageFormatted();
    journalStorage.checkConsistentNamespace(nsInfo);
    
    // if we are storing image too, check consistency as well
    if (imageStorage.isFormatted()) {
      imageStorage.checkConsistentNamespace(nsInfo);
    }

    // Check that the new epoch being proposed is in fact newer than
    // any other that we've promised. 
    if (epoch <= getLastPromisedEpoch()) {
      throw new IOException("Proposed epoch " + epoch + " <= last promise " +
          getLastPromisedEpoch());
    }
    
    updateLastPromisedEpoch(epoch);
    abortCurSegment();
    
    NewEpochResponseProto ret = new NewEpochResponseProto();

    EditLogFile latestFile = scanStorageForLatestEdits();

    if (latestFile != null) {
      ret.setLastSegmentTxId(latestFile.getFirstTxId());
    }
    return ret;
  }

  private void updateLastPromisedEpoch(long newEpoch) throws IOException {
    LOG.info("Updating lastPromisedEpoch from " + lastPromisedEpoch.get()
        + " to " + newEpoch + " for client " + Server.getRemoteIp());
    lastPromisedEpoch.set(newEpoch);
    
    // Since we have a new writer, reset the IPC serial - it will start
    // counting again from 0 for this writer.
    currentEpochIpcSerial = -1;
  }

  private void abortCurSegment() throws IOException {
    if (curSegment == null) {
      return;
    }
    
    curSegment.abort();
    curSegment = null;
    curSegmentTxId = HdfsConstants.INVALID_TXID;
    currentSegmentWrittenBytes = 0L;
  }

  /**
   * Write a batch of edits to the journal.
   * {@see QJournalProtocol#journal(RequestInfo, long, long, int, byte[])}
   */
  synchronized ShortVoid journal(RequestInfo reqInfo, 
      long segmentTxId, long firstTxnId, 
      int numTxns, byte[] records) throws IOException {
    checkJournalStorageFormatted();
    checkWriteRequest(reqInfo);

    if (curSegment == null) {
      checkSync(false, "Can't write, no segment open");
    }
    
    if (curSegmentTxId != segmentTxId) {
      // Sanity check: it is possible that the writer will fail IPCs
      // on both the finalize() and then the start() of the next segment.
      // This could cause us to continue writing to an old segment
      // instead of rolling to a new one, which breaks one of the
      // invariants in the design. If it happens, abort the segment
      // and throw an exception.
      JournalOutOfSyncException e = new JournalOutOfSyncException(
          "Writer out of sync: it thinks it is writing segment " + segmentTxId
          + " but current segment is " + curSegmentTxId);
      abortCurSegment();
      throw e;
    }
      
    if (nextTxId != firstTxnId) {
      checkSync(false, "Can't write txid " + firstTxnId
          + " expecting nextTxId=" + nextTxId);
    }
    
    long lastTxnId = firstTxnId + numTxns - 1;
    if (LOG.isTraceEnabled()) {
      LOG.trace("Writing txid " + firstTxnId + "-" + lastTxnId);
    }

    // If the edit has already been marked as committed, we know
    // it has been fsynced on a quorum of other nodes, and we are
    // "catching up" with the rest. Hence we do not need to fsync.
    boolean isLagging = lastTxnId <= committedTxnId.get();
    boolean shouldFsync = !isLagging;
    
    curSegment.writeRaw(records, 0, records.length);
    curSegment.setReadyToFlush();
    long start = System.nanoTime();
    curSegment.flush(shouldFsync);
    long time = DFSUtil.getElapsedTimeMicroSeconds(start);
    currentSegmentWrittenBytes += records.length;
    
    metrics.addSync(time);
    
    if (time / 1000 > WARN_SYNC_MILLIS_THRESHOLD) {
      LOG.warn("Sync of transaction range " + firstTxnId + "-" + lastTxnId + 
               " took " + (time / 1000) + "ms");
    }

    if (isLagging) {
      // This batch of edits has already been committed on a quorum of other
      // nodes. So, we are in "catch up" mode. This gets its own metric.
      metrics.batchesWrittenWhileLagging.inc(1);
    }
    
    metrics.batchesWritten.inc(1);
    metrics.bytesWritten.inc(records.length);
    metrics.txnsWritten.inc(numTxns);
    
    highestWrittenTxId = lastTxnId;
    metrics.setLastWrittenTxId(highestWrittenTxId);
    metrics.setCurrentTxnsLag(getCurrentLagTxns());
    nextTxId = lastTxnId + 1;
    return ShortVoid.instance;
  }

  public void heartbeat(RequestInfo reqInfo) throws IOException {
    checkRequest(reqInfo);
  }
  
  /**
   * Returns true if the given epoch matches the lastPromisedEpoch, false, otherwise.
   */
  boolean checkWriterEpoch(long epoch) throws IOException {
    return epoch == lastPromisedEpoch.get();
  }
  
  /**
   * Ensure that the given request is coming from the correct writer and in-order.
   * @param reqInfo the request info
   * @throws IOException if the request is invalid.
   */
  private synchronized void checkRequest(RequestInfo reqInfo) throws IOException {
    // Invariant 25 from ZAB paper
    if (reqInfo.getEpoch() < lastPromisedEpoch.get()) {
      throw new IOException("IPC's epoch " + reqInfo.getEpoch() +
          " is less than the last promised epoch " +
          lastPromisedEpoch.get());
    } else if (reqInfo.getEpoch() > lastPromisedEpoch.get()) {
      // A newer client has arrived. Fence any previous writers by updating
      // the promise.
      updateLastPromisedEpoch(reqInfo.getEpoch());
    }
    
    // Ensure that the IPCs are arriving in-order as expected.
    if (reqInfo.getIpcSerialNumber() <= currentEpochIpcSerial) {
      checkSync(false,
          "IPC serial %s from client %s was not higher than prior highest " +
          "IPC serial %s", reqInfo.getIpcSerialNumber(),
          Server.getRemoteIp(),
          currentEpochIpcSerial);
    }
    currentEpochIpcSerial = reqInfo.getIpcSerialNumber();

    if (reqInfo.hasCommittedTxId()) {
      if (reqInfo.getCommittedTxId() < committedTxnId.get()) {
        throw new IllegalArgumentException(
            "Client trying to move committed txid backward from "
                + committedTxnId.get() + " to " + reqInfo.getCommittedTxId());
      }
      // persist txid every second, as it is not needed for correctness
      boolean persist = (now() - lastPersistedCommittedTxId) > 1000;
      if (persist) {
        lastPersistedCommittedTxId = now();
      }
      committedTxnId.set(reqInfo.getCommittedTxId(), persist);
    }
  }
  
  static long now() {
    return System.currentTimeMillis();
  }
  
  private synchronized void checkWriteRequest(RequestInfo reqInfo) throws IOException {
    checkRequest(reqInfo);
    
    if (reqInfo.getEpoch() != lastWriterEpoch.get()) {
      throw new IOException("IPC's epoch " + reqInfo.getEpoch() +
          " is not the current writer epoch  " +
          lastWriterEpoch.get());
    }
  }
  
  public synchronized boolean isJournalFormatted() {
    return journalStorage.isFormatted();
  }
  
  public synchronized boolean isImageFormatted() {
    return imageStorage.isFormatted();
  }

  private void checkJournalStorageFormatted()
      throws JournalNotFormattedException {
    if (!journalStorage.isFormatted()) {
      String msg = "Journal edits: " + journalStorage.getSingularStorageDir()
          + " not formatted";
      LOG.error(msg);
      throw new JournalNotFormattedException(msg);
    }
  }

  private void checkImageStorageFormatted() throws JournalNotFormattedException {
    if (!imageStorage.isFormatted()) {
      String msg = "Journal image: " + imageStorage.getSingularStorageDir()
          + " not formatted";
      LOG.error(msg);
      throw new JournalNotFormattedException(msg);
    }
  }
  
  /**
   * Get the transaction id of the currently written segment.
   */
  public synchronized long getCurrentSegmentTxId() {
    return curSegmentTxId;
  }
  
  /**
   * Get upper bound on the size of the currently written segment.
   */
  public synchronized long getValidSizeOfCurrentSegment() {
    return currentSegmentWrittenBytes;
  }

  /**
   * @throws JournalOutOfSyncException if the given expression is not true.
   * The message of the exception is formatted using the 'msg' and
   * 'formatArgs' parameters.
   */
  private void checkSync(boolean expression, String msg,
      Object... formatArgs) throws JournalOutOfSyncException {
    if (!expression) {
      throw new JournalOutOfSyncException(String.format(msg, formatArgs));
    }
  }

  /**
   * @throws AssertionError if the given expression is not true.
   * The message of the exception is formatted using the 'msg' and
   * 'formatArgs' parameters.
   * 
   * This should be used in preference to Java's built-in assert in
   * non-performance-critical paths, where a failure of this invariant
   * might cause the protocol to lose data. 
   */
  private void alwaysAssert(boolean expression, String msg,
      Object... formatArgs) {
    if (!expression) {
      throw new AssertionError(String.format(msg, formatArgs));
    }
  }
  
  /**
   * Start a new segment at the given txid. The previous segment
   * must have already been finalized.
   */
  public synchronized void startLogSegment(RequestInfo reqInfo, long txid)
      throws IOException {
    assert fjm != null;
    checkJournalStorageFormatted();
    checkRequest(reqInfo);
    
    if (curSegment != null) {
      LOG.warn("Client is requesting a new log segment " + txid + 
          " though we are already writing " + curSegment + ". " +
          "Aborting the current segment in order to begin the new one.");
      // The writer may have lost a connection to us and is now
      // re-connecting after the connection came back.
      // We should abort our own old segment.
      abortCurSegment();
    }

    // Paranoid sanity check: we should never overwrite a finalized log file.
    // Additionally, if it's in-progress, it should have at most 1 transaction.
    // This can happen if the writer crashes exactly at the start of a segment.
    EditLogFile existing = fjm.getLogFile(txid);
    if (existing != null) {
      if (!existing.isInProgress()) {
        throw new IllegalStateException("Already have a finalized segment " +
            existing + " beginning at " + txid);
      }
      
      // If it's in-progress, it should only contain one transaction,
      // because the "startLogSegment" transaction is written alone at the
      // start of each segment. 
      existing.validateLog();
      if (existing.getLastTxId() != existing.getFirstTxId()) {
        throw new IllegalStateException("The log file " +
            existing + " seems to contain valid transactions");
      }
    }
    
    long curLastWriterEpoch = lastWriterEpoch.get();
    if (curLastWriterEpoch != reqInfo.getEpoch()) {
      LOG.info("Updating lastWriterEpoch from " + curLastWriterEpoch +
          " to " + reqInfo.getEpoch() + " for client " +
          Server.getRemoteIp());
      lastWriterEpoch.set(reqInfo.getEpoch());
      metrics.lastWriterEpoch.set(reqInfo.getEpoch());
    }

    // The fact that we are starting a segment at this txid indicates
    // that any previous recovery for this same segment was aborted.
    // Otherwise, no writer would have started writing. So, we can
    // remove the record of the older segment here.
    purgePaxosDecision(txid);
    
    curSegment = fjm.startLogSegment(txid);
    curSegmentTxId = txid;
    nextTxId = txid;
    // the layout version has only been written
    // plus OP_INVALID
    currentSegmentWrittenBytes = 5L;
    
    // inform the syncer service that we might have some work to do
    if (journalNode != null) {
      journalNode.addSyncTask(this, curSegmentTxId);
    }
  }
  
  /**
   * Finalize the log segment at the given transaction ID.
   */
  public synchronized void finalizeLogSegment(RequestInfo reqInfo, long startTxId,
      long endTxId) throws IOException {
    checkJournalStorageFormatted();
    checkRequest(reqInfo);
    
    boolean needsValidation = true;

    // Finalizing the log that the writer was just writing.
    if (startTxId == curSegmentTxId) {
      if (curSegment != null) {
        curSegment.close();
        curSegment = null;
        curSegmentTxId = HdfsConstants.INVALID_TXID;
        currentSegmentWrittenBytes = 0L;
      }
      
      checkSync(nextTxId == endTxId + 1,
          "Trying to finalize in-progress log segment %s to end at " +
          "txid %s but only written up to txid %s",
          startTxId, endTxId, nextTxId - 1);
      // No need to validate the edit log if the client is finalizing
      // the log segment that it was just writing to.
      needsValidation = false;
    }
    
    FileJournalManager.EditLogFile elf = fjm.getLogFile(startTxId);
    if (elf == null) {
      throw new JournalOutOfSyncException("No log file to finalize at " +
          "transaction ID " + startTxId);
    }

    if (elf.isInProgress()) {
      if (needsValidation) {
        LOG.info("Validating log segment " + elf.getFile() + " about to be " +
            "finalized");
        elf.validateLog();
  
        checkSync(elf.getLastTxId() == endTxId,
            "Trying to finalize in-progress log segment %s to end at " +
            "txid %s but log %s on disk only contains up to txid %s",
            startTxId, endTxId, elf.getFile(), elf.getLastTxId());
      }
      fjm.finalizeLogSegment(startTxId, endTxId);
    } else {
      Preconditions.checkArgument(endTxId == elf.getLastTxId(),
          "Trying to re-finalize already finalized log " +
              elf + " with different endTxId " + endTxId);
    }

    // Once logs are finalized, a different length will never be decided.
    // During recovery, we treat a finalized segment the same as an accepted
    // recovery. Thus, we no longer need to keep track of the previously-
    // accepted decision. The existence of the finalized log segment is enough.
    purgePaxosDecision(elf.getFirstTxId());
  }
  
  /**
   * @see JournalManager#purgeLogsOlderThan(long)
   */
  public synchronized void purgeLogsOlderThan(RequestInfo reqInfo,
      long minTxIdToKeep) throws IOException {
    checkJournalStorageFormatted();
    checkRequest(reqInfo);
    
    journalStorage.purgeDataOlderThan(minTxIdToKeep);
    if (minTxIdToKeep == FSEditLog.PURGE_ALL_TXID) {
      // When trying to remove all the segments, reset
      // the committed transaction ID too.
      committedTxnId.set(0, true);
      minTxid = 0;
    } else {
      minTxid = minTxIdToKeep;
    }
    if (imageStorage.isFormatted()) {
      imageStorage.purgeDataOlderThan(minTxIdToKeep == 0 ? -1 : minTxIdToKeep);
    }
  }
  
  public synchronized long getMinTxid() {
    return minTxid;
  }
  
  /**
   * Remove the previously-recorded 'accepted recovery' information
   * for a given log segment, once it is no longer necessary. 
   * @param segmentTxId the transaction ID to purge
   * @throws IOException if the file could not be deleted
   */
  private void purgePaxosDecision(long segmentTxId) throws IOException {
    File paxosFile = journalStorage.getPaxosFile(segmentTxId);
    if (paxosFile.exists()) {
      if (!paxosFile.delete()) {
        throw new IOException("Unable to delete paxos file " + paxosFile);
      }
    }
  }
  
  /**
   * Get the list of underlying this journal
   */
  List<EditLogFile> getAllLogFiles() throws IOException {
    return fjm.getLogFiles(HdfsConstants.INVALID_TXID, false);
  }

  /**
   * @see QJournalProtocol#getEditLogManifest(String, long)
   */
  public RemoteEditLogManifest getEditLogManifest(long sinceTxId)
      throws IOException {
    // No need to checkRequest() here - anyone may ask for the list
    // of segments.
    checkJournalStorageFormatted();
    
    RemoteEditLogManifest manifest = fjm.getEditLogManifest(sinceTxId);
    return manifest;
  }

  /**
   * @return the current state of the given segment, or null if the
   * segment does not exist.
   */
  private SegmentStateProto getSegmentInfo(long segmentTxId)
      throws IOException {
    EditLogFile elf = fjm.getLogFile(segmentTxId);
    if (elf == null) {
      return null;
    }
    if (elf.isInProgress()) {
      elf.validateLog();
    }
    if (elf.getLastTxId() == HdfsConstants.INVALID_TXID) {
      LOG.info("Edit log file " + elf + " appears to be empty. " +
          "Moving it aside...");
      elf.moveAsideEmptyFile();
      return null;
    }
    SegmentStateProto ret = new SegmentStateProto(segmentTxId, elf.getLastTxId(), elf.isInProgress());
    LOG.info("getSegmentInfo(" + segmentTxId + "): " + elf + " -> " + ret);
    return ret;
  }

  /**
   * @see QJournalProtocol#prepareRecovery(RequestInfo, long)
   */
  public synchronized PrepareRecoveryResponseProto prepareRecovery(
      RequestInfo reqInfo, long segmentTxId) throws IOException {
    checkJournalStorageFormatted();
    checkRequest(reqInfo);
    
    abortCurSegment();
    
    PrepareRecoveryResponseProto ret = new PrepareRecoveryResponseProto();

    PersistedRecoveryPaxosData previouslyAccepted = getPersistedPaxosData(segmentTxId);
    completeHalfDoneAcceptRecovery(previouslyAccepted);

    SegmentStateProto segInfo = getSegmentInfo(segmentTxId);
    boolean hasFinalizedSegment = segInfo != null && !segInfo.getIsInProgress();

    if (previouslyAccepted != null && !hasFinalizedSegment) {
      ret.setAcceptedInEpoch(previouslyAccepted.getAcceptedInEpoch());   
      ret.setSegmentState(previouslyAccepted.getSegmentState());
    } else {
      if (segInfo != null) {
        ret.setSegmentState(segInfo);
      }
    }
    
    ret.setLastWriterEpoch(lastWriterEpoch.get());
    if (committedTxnId.get() != HdfsConstants.INVALID_TXID) {
      ret.setLastCommittedTxId(committedTxnId.get());
    }
    
    LOG.info("Prepared recovery for segment " + segmentTxId + ": " + ret);
    return ret;
  }
  
  /**
   * @see QJournalProtocol#acceptRecovery(RequestInfo, SegmentStateProto, URL)
   */
  public synchronized void acceptRecovery(RequestInfo reqInfo,
      SegmentStateProto segment, URL fromUrl)
      throws IOException {
    checkJournalStorageFormatted();
    checkRequest(reqInfo);
    
    abortCurSegment();

    long segmentTxId = segment.getStartTxId();

    // Basic sanity checks that the segment is well-formed and contains
    // at least one transaction.
    Preconditions.checkArgument(segment.getEndTxId() > -1 &&
        segment.getEndTxId() >= segmentTxId,
        "bad recovery state for segment %s: %s",
        segmentTxId, segment);
    
    PersistedRecoveryPaxosData oldData = getPersistedPaxosData(segmentTxId);
    PersistedRecoveryPaxosData newData = new PersistedRecoveryPaxosData();
    newData.setAcceptedInEpoch(reqInfo.getEpoch());
    newData.setSegmentState(segment);
    
    // If we previously acted on acceptRecovery() from a higher-numbered writer,
    // this call is out of sync. We should never actually trigger this, since the
    // checkRequest() call above should filter non-increasing epoch numbers.
    if (oldData != null) {
      alwaysAssert(oldData.getAcceptedInEpoch() <= reqInfo.getEpoch(),
          "Bad paxos transition, out-of-order epochs.\nOld: %s\nNew: %s\n",
          oldData, newData);
    }
    
    File syncedFile = null;
    
    SegmentStateProto currentSegment = getSegmentInfo(segmentTxId);
    if (currentSegment == null ||
        currentSegment.getEndTxId() != segment.getEndTxId()) {
      if (currentSegment == null) {
        LOG.info("Synchronizing log " + segment +
            ": no current segment in place");
        
        // Update the highest txid for lag metrics
        highestWrittenTxId = Math.max(segment.getEndTxId(),
            highestWrittenTxId);
        metrics.setLastWrittenTxId(highestWrittenTxId);
      } else {
        LOG.info("Synchronizing log " + segment +
            ": old segment " + currentSegment +
            " is not the right length");
        
        // Paranoid sanity check: if the new log is shorter than the log we
        // currently have, we should not end up discarding any transactions
        // which are already Committed.
        if (txnRange(currentSegment).contains(committedTxnId.get()) &&
            !txnRange(segment).contains(committedTxnId.get())) {
          throw new AssertionError(
              "Cannot replace segment " + currentSegment +
              " with new segment " + segment + 
              ": would discard already-committed txn " +
              committedTxnId.get());
        }
        
        // Another paranoid check: we should not be asked to synchronize a log
        // on top of a finalized segment.
        alwaysAssert(currentSegment.getIsInProgress(),
            "Should never be asked to synchronize a different log on top of an " +
            "already-finalized segment");
        
        // If we're shortening the log, update our highest txid
        // used for lag metrics.
        if (txnRange(currentSegment).contains(highestWrittenTxId)) {
          highestWrittenTxId = segment.getEndTxId();
          metrics.setLastWrittenTxId(highestWrittenTxId);
        }
      }
      syncedFile = syncLog(reqInfo, segment, fromUrl);
      
    } else {
      LOG.info("Skipping download of log " + segment +
          ": already have up-to-date logs");
    }
    
    // This is one of the few places in the protocol where we have a single
    // RPC that results in two distinct actions:
    //
    // - 1) Downloads the new log segment data (above)
    // - 2) Records the new Paxos data about the synchronized segment (below)
    //
    // These need to be treated as a transaction from the perspective
    // of any external process. We do this by treating the persistPaxosData()
    // success as the "commit" of an atomic transaction. If we fail before
    // this point, the downloaded edit log will only exist at a temporary
    // path, and thus not change any externally visible state. If we fail
    // after this point, then any future prepareRecovery() call will see
    // the Paxos data, and by calling completeHalfDoneAcceptRecovery() will
    // roll forward the rename of the referenced log file.
    //
    // See also: HDFS-3955
    //
    // The fault points here are exercised by the randomized fault injection
    // test case to ensure that this atomic "transaction" operates correctly.
    JournalFaultInjector.get().beforePersistPaxosData();
    persistPaxosData(segmentTxId, newData);
    JournalFaultInjector.get().afterPersistPaxosData();

    if (syncedFile != null) {
      FileUtil.replaceFile(syncedFile,
          journalStorage.getInProgressEditLog(segmentTxId));
    }

    LOG.info("Accepted recovery for segment " + segmentTxId + ": " +
        newData);
  }

  private Range<Long> txnRange(SegmentStateProto seg) {
    return Ranges.closed(seg.getStartTxId(), seg.getEndTxId());
  }

  /**
   * Synchronize a log segment from another JournalNode. The log is
   * downloaded from the provided URL into a temporary location on disk,
   * which is named based on the current request's epoch.
   *
   * @return the temporary location of the downloaded file
   */
  File syncLog(RequestInfo reqInfo, final SegmentStateProto segment,
      final URL url) throws IOException {
    long startTxId = segment.getStartTxId();
    long epoch = reqInfo.getEpoch();
    return syncLog(epoch, segment.getStartTxId(), url, segment.toString(),
        journalStorage.getSyncLogTemporaryFile(startTxId, epoch));
  }

  /**
   * Synchronize a log segment from another JournalNode. The log is
   * downloaded from the provided URL into a temporary location on disk
   */
  File syncLog(long stamp, final long startTxId, final URL url, String name, File tmpFile)
      throws IOException {
    final File[] localPaths = new File[] { tmpFile };

    // TODO add security if needed.
    LOG.info("Synchronizing log " + name + " from " + url);
    boolean success = false;
    try {
      TransferFsImage.doGetUrl(
          url,
          ImageSet.convertFilesToStreams(localPaths, journalStorage,
              url.toString()), journalStorage, true);
      assert tmpFile.exists();
      success = true;
    } finally {
      if (!success) {
        if (!tmpFile.delete()) {
          LOG.warn("Failed to delete temporary file " + tmpFile);
        }
      }
    }
    return tmpFile;
  }
  

  /**
   * In the case the node crashes in between downloading a log segment
   * and persisting the associated paxos recovery data, the log segment
   * will be left in its temporary location on disk. Given the paxos data,
   * we can check if this was indeed the case, and &quot;roll forward&quot;
   * the atomic operation.
   * 
   * See the inline comments in
   * {@link #acceptRecovery(RequestInfo, SegmentStateProto, URL)} for more
   * details.
   *
   * @throws IOException if the temporary file is unable to be renamed into
   * place
   */
  private void completeHalfDoneAcceptRecovery(
      PersistedRecoveryPaxosData paxosData) throws IOException {
    if (paxosData == null) {
      return;
    }

    long segmentId = paxosData.getSegmentState().getStartTxId();
    long epoch = paxosData.getAcceptedInEpoch();
    
    File tmp = journalStorage.getSyncLogTemporaryFile(segmentId, epoch);
    
    if (tmp.exists()) {
      File dst = journalStorage.getInProgressEditLog(segmentId);
      LOG.info("Rolling forward previously half-completed synchronization: " +
          tmp + " -> " + dst);
      FileUtil.replaceFile(tmp, dst);
    }
  }

  /**
   * Retrieve the persisted data for recovering the given segment from disk.
   */
  private PersistedRecoveryPaxosData getPersistedPaxosData(long segmentTxId)
      throws IOException {
    File f = journalStorage.getPaxosFile(segmentTxId);
    if (!f.exists()) {
      // Default instance has no fields filled in (they're optional)
      return null;
    }
    
    InputStream in = new FileInputStream(f);
    try {
      PersistedRecoveryPaxosData ret = PersistedRecoveryPaxosData.parseDelimitedFrom(in);
      Preconditions.checkState(ret != null &&
          ret.getSegmentState().getStartTxId() == segmentTxId,
          "Bad persisted data for segment %s: %s",
          segmentTxId, ret);
      return ret;
    } finally {
      IOUtils.closeStream(in);
    }
  }

  /**
   * Persist data for recovering the given segment from disk.
   */
  private void persistPaxosData(long segmentTxId,
      PersistedRecoveryPaxosData newData) throws IOException {
    File f = journalStorage.getPaxosFile(segmentTxId);
    boolean success = false;
    AtomicFileOutputStream fos = new AtomicFileOutputStream(f);
    try {
      newData.writeDelimitedTo(fos);
      fos.write('\n');
      // Write human-readable data after the protobuf. This is only
      // to assist in debugging -- it's not parsed at all.
      OutputStreamWriter writer = new OutputStreamWriter(fos, Charsets.UTF_8);
      
      writer.write(String.valueOf(newData));
      writer.write('\n');
      writer.flush();
      
      fos.flush();
      success = true;
    } finally {
      if (success) {
        IOUtils.closeStream(fos);
      } else {
        fos.abort();
      }
    }
  }
  
  /**
   * Roll the image.
   */
  public void saveDigestAndRenameCheckpointImage(long txid, MD5Hash digest)
      throws IOException {
    MD5Hash storedDigest = checkpointImageDigests.get(txid);
    if (storedDigest == null || !storedDigest.equals(digest)) {
      throw new IOException("Digest of data written: " + storedDigest
          + " does not match requested digest: " + digest + " for txid: "
          + txid + ", journal: " + journalId);
    }
    imageManager.saveDigestAndRenameCheckpointImage(txid, digest);
    checkpointImageDigests.remove(txid);
  }

  synchronized void setCheckpointImageDigest(long txid, MD5Hash imageDigest)
      throws IOException {
    if (checkpointImageDigests.containsKey(txid)) {
      MD5Hash existing = checkpointImageDigests.get(txid);
      if (!existing.equals(imageDigest)) {
        throw new IOException(
            "Trying to set checkpoint image digest for txid: " + txid + "="
                + imageDigest + " existing " + existing + " for txid: " + txid
                + ", journal: " + journalId);
      }
    } else {
      checkpointImageDigests.put(txid, imageDigest);
      mostRecentCheckpointTxid = Math.max(mostRecentCheckpointTxid, txid);
    }
  }
  
  public synchronized void clearCheckpointImageDigests() {
    checkpointImageDigests.clear();
  }
  
  public RemoteImageManifest getImageManifest(long fromTxid) throws IOException {
    checkImageStorageFormatted();
    FSImageTransactionalStorageInspector inspector = new FSImageTransactionalStorageInspector();
    inspector.inspectDirectory(imageStorage.getSingularStorageDir());
    List<RemoteImage> results = Lists.newArrayList();

    for (FSImageFile foundImage : inspector.getFoundImages()) {
      // skip older images
      if (foundImage.getCheckpointTxId() < fromTxid) {
        continue;
      }
      // get md5 if exists
      MD5Hash digest = null;
      try {
        digest = MD5FileUtils.readStoredMd5ForFile(foundImage.getFile());
      } catch (IOException e) {
        LOG.warn("Exception when reading md5 file for image: " + foundImage, e);
      }
      // in case of failure the digest is simply "null"
      results.add(new RemoteImage(foundImage.getCheckpointTxId(), digest));
    }
    
    // sort the images
    Collections.sort(results);
    
    return new RemoteImageManifest(results);
  }

  public FileJournalManager getJournalManager() {
    return fjm;
  }
  
  /**
   * Get storage state for the journal storage directory.
   */
  public GetStorageStateProto analyzeJournalStorage() {
    return new GetStorageStateProto(journalStorage.getStorageState(),
        journalStorage.getStorageInfo());
  }
  
  /**
   * Get storage state for the image storage directory.
   */
  public GetStorageStateProto analyzeImageStorage() {
    return new GetStorageStateProto(imageStorage.getStorageState(),
        imageStorage.getStorageInfo());
  }
}
