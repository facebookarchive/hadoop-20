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
package org.apache.hadoop.hdfs.notifier.server;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.notifier.NamespaceNotification;
import org.apache.hadoop.hdfs.notifier.NotifierConfig;
import org.apache.hadoop.hdfs.notifier.NotifierUtils;
import org.apache.hadoop.hdfs.server.common.HdfsConstants;
import org.apache.hadoop.hdfs.server.common.Storage.StorageDirectory;
import org.apache.hadoop.hdfs.server.common.StorageInfo;
import org.apache.hadoop.hdfs.server.namenode.EditLogInputStream;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOpCodes;
import org.apache.hadoop.hdfs.server.namenode.FileJournalManagerReadOnly;
import org.apache.hadoop.hdfs.server.namenode.JournalManager;
import org.apache.hadoop.hdfs.server.namenode.JournalSet;
import org.apache.hadoop.hdfs.server.namenode.NNStorage;
import org.apache.hadoop.hdfs.server.protocol.RemoteEditLog;
import org.apache.hadoop.hdfs.server.protocol.RemoteEditLogManifest;
import org.apache.hadoop.hdfs.util.InjectionEvent;
import org.apache.hadoop.util.InjectionHandler;

public class ServerLogReaderTransactional implements IServerLogReader {

  public static final Log LOG = LogFactory
      .getLog(ServerLogReaderTransactional.class);

  // handle to the core
  protected IServerCore core;
  protected final Configuration conf;

  // remote journal from which the reader is consuming transactions
  protected JournalManager remoteJournalManager = null;

  // transaction id of the currently consumed log segment
  private long currentSegmentTxId = HdfsConstants.INVALID_TXID;
  private EditLogInputStream currentEditLogInputStream = null;
  private long currentEditLogInputStreamPosition = -1;

  // keep track of when read succeeds or stream is reopen
  private long mostRecentlyReadTransactionTxId = -1;
  private long mostRecentlyReadTransactionTime = -1;

  
  // number of retries when opening input stream
  private final int inputStreamRetries;
  //if we keep getting null when reading, we should reopen the segment
  private long nothingReadCheckTimeout;
  private long errorSleepTimeout;

  public ServerLogReaderTransactional(IServerCore core, URI editsURI)
      throws IOException {
    this.core = core;
    this.conf = core.getConfiguration();
    this.inputStreamRetries = conf.getInt(
        NotifierConfig.LOG_READER_STREAM_RETRIES,
        NotifierConfig.LOG_READER_STREAM_RETRIES_DEFAULT);
    this.nothingReadCheckTimeout = conf.getLong(
        NotifierConfig.LOG_READER_STREAM_TIMEOUT,
        NotifierConfig.LOG_READER_STREAM_TIMEOUT_DEFAULT);
    this.errorSleepTimeout = conf.getLong(
        NotifierConfig.LOG_READER_STREAM_ERROR_SLEEP,
        NotifierConfig.LOG_READER_STREAM_ERROR_SLEEP_DEFAULT);

    if (editsURI != null) {
      if (editsURI.getScheme().equals(NNStorage.LOCAL_URI_SCHEME)) {
        StorageDirectory sd = new NNStorage(new StorageInfo()).new StorageDirectory(
            new File(editsURI.getPath()));
        remoteJournalManager = new FileJournalManagerReadOnly(sd);
      } else {
        throw new IOException("Other journals not supported yet.");
      }
  
      LOG.info("Initializing input stream");
      initialize();
      LOG.info("Initialization completed");
    }
  }

  @Override
  public void run() {
    // this is empty
  }
  
  @Override
  public void close() throws IOException {
    if (currentEditLogInputStream != null) {
      currentEditLogInputStream.close();
    }
  }

  /**
   * Reads from the edit log until it reaches an operation which can be
   * considered a namespace notification (like FILE_ADDED, FILE_CLOSED or
   * NODE_DELETED).
   * 
   * @return the notification object or null if nothing is to be returned at the
   *         moment.
   * @throws IOException
   *           raised when a fatal error occurred.
   */
  @Override
  public NamespaceNotification getNamespaceNotification() throws IOException {
    FSEditLogOp op = null;
    NamespaceNotification notification = null;

    // Keep looping until we reach an operation that can be
    // considered a notification.
    while (true) {
      
      try {
        // if the stream is null, we need to setup next stream
        // if we cannot than this is a fatal failure
        refreshInputStream();

        // get current position in the stream
        currentEditLogInputStreamPosition = currentEditLogInputStream
            .getPosition();
        
        // try reading a transaction
        op = currentEditLogInputStream.readOp();
        InjectionHandler.processEventIO(InjectionEvent.SERVERLOGREADER_READOP);
        if (LOG.isDebugEnabled()) {
          LOG.info("inputStream.readOP() returned " + op);
        }
      } catch (Exception e) {
        // possibly ChecksumException
        LOG.warn("inputStream.readOp() failed", e);
        // try reopening current log segment
        tryReloadingEditLog();
        continue;
      } 

      // for each successful read, we update state
      if (op == null) {
        // we can sleep to wait for new transactions
        sleep(500);
        // nothing in the edit log now
        core.getMetrics().reachedEditLogEnd.inc();
        checkProgress();
        continue;
      }

      if (ServerLogReaderUtil.shouldSkipOp(mostRecentlyReadTransactionTxId, op)){
        updateState(op, false);
        continue;
      }
      // update internal state, and check for progress
      updateState(op, true);

      if (LOG.isDebugEnabled()) {
        LOG.debug("Read operation: " + op + " with txId="
          + (op == null ? "null" : op.getTransactionId()));
      }

      // Test if it can be considered a notification
      notification = ServerLogReaderUtil.createNotification(op);
      if (notification != null) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Emitting " + NotifierUtils.asString(notification));
        }
        core.getMetrics().readNotifications.inc();
        return notification;
      }
    }
  }

  // ////////////////////////////////////////////////////////////////

  /**
   * On error when reading transactions from the stream, try reloading the input
   * stream.
   */
  private void tryReloadingEditLog() throws IOException {
    LOG.info("Segment - trying to reload edit log segment");
    // sleep on error
    sleep(errorSleepTimeout);

    // check if a new segment exists
    checkProgress();

    // reopen current segment
    setupIngestStreamWithRetries(currentSegmentTxId);

    // set the position to last good position
    refreshStreamPosition();
  }
  
  protected void detectJournalManager() throws IOException {
    // do nothing
  }

  /**
   * If we cannot read anything for some time, we can check if new segment
   * exists (this would happen on unclean shutdown). Or trigger reopening
   * current segment.
   */
  private void checkProgress() throws IOException {
    if (now() - mostRecentlyReadTransactionTime > nothingReadCheckTimeout) {
      // check the journal manager
      detectJournalManager();
      
      // we haven't read anything in a while
      if (segmentExists(mostRecentlyReadTransactionTxId + 1) &&
          mostRecentlyReadTransactionTxId != -1) {
        // unclean shutdown happened, we need to switch to next segment
        currentSegmentTxId = mostRecentlyReadTransactionTxId + 1;
        // indicate that we will reopen new stream
        currentEditLogInputStreamPosition = -1;
        
        LOG.warn("Segment - switching to new log segment with txid: "
            + currentSegmentTxId + ". Previous segment was not closed cleanly.");
      } else {
        LOG.warn("Segment - will reopen segment with txid: "
            + currentSegmentTxId + " at position: "
            + currentEditLogInputStreamPosition);
      }
      // in both cases we want to reopen the stream
      currentEditLogInputStream = null;
    }
  }

  /**
   * For each operation read from the stream, check if this is a closing
   * transaction. If so, we are sure we need to move to the next segment.
   * 
   * We also mark that this is the most recent time, we read something valid
   * from the input.
   */
  private void updateState(FSEditLogOp op, boolean checkTxnId) throws IOException {
    InjectionHandler.processEvent(InjectionEvent.SERVERLOGREADER_UPDATE, op);
    
    if (checkTxnId) {
      mostRecentlyReadTransactionTxId = ServerLogReaderUtil.checkTransactionId(
        mostRecentlyReadTransactionTxId, op);
    }
    
    updateStreamPosition();

    // read a valid operation
    core.getMetrics().readOperations.inc();
    mostRecentlyReadTransactionTime = now();

    // current log segment ends normally
    if (op.opCode == FSEditLogOpCodes.OP_END_LOG_SEGMENT) {
      LOG.info("Segment - ending log segment start txid: " + currentSegmentTxId
          + ", end txid: " + op.getTransactionId());
      // move forward with next segment
      currentSegmentTxId = op.getTransactionId() + 1;
      // set the stream to null so the next getNotification()
      // will recreate it
      currentEditLogInputStream = null;
      // indicate that a new stream will be opened
      currentEditLogInputStreamPosition = -1;
    } else if (op.opCode == FSEditLogOpCodes.OP_START_LOG_SEGMENT) {
      LOG.info("Segment - starting log segment start txid: "
          + currentSegmentTxId);
    }
  }

  /**
   * Called to refresh the position in the current input stream to the last
   * ACK'd one.
   * 
   * @throws IOException
   *           if a fatal error occurred.
   */
  private void refreshStreamPosition() throws IOException {
    if (currentEditLogInputStreamPosition != -1) {
      // stream was reopened
      currentEditLogInputStream.refresh(currentEditLogInputStreamPosition,
          mostRecentlyReadTransactionTxId);
    } else {
      // freshly opened stream
      currentEditLogInputStreamPosition = currentEditLogInputStream.getPosition();
    }
  }

  /**
   * Sets the current position of the input stream, after a transaction has been
   * successfully consumed.
   * 
   * @throws IOException
   *           if a fatal error occurred.
   */
  private void updateStreamPosition() throws IOException {
    try {
      currentEditLogInputStreamPosition = currentEditLogInputStream
          .getPosition();
    } catch (IOException e) {
      LOG.error("Failed to get edit log file position", e);
      throw new IOException("updateStreamPosition failed");
    }
  }

  /**
   * On normal activity, when we reach END_LOG_SEGMENT, we will null the stream
   * and at next read we want to instantiate a stream for next segment.
   * 
   * This function repositions the stream to the last correct known position as
   * well.
   */
  private void refreshInputStream() throws IOException {
    // if stream is null, we probably switched to a new
    // segment.
    if (currentEditLogInputStream == null) {
      LOG.info("Segment - setup input stream for txid: " + currentSegmentTxId);
      setupIngestStreamWithRetries(currentSegmentTxId);
      if (currentEditLogInputStreamPosition == -1) {
        // we are opening a fresh segment
        currentEditLogInputStreamPosition = currentEditLogInputStream.getPosition();
      }
    }

    // if we are re-opening stream previously consumed
    // set correct position
    if (currentEditLogInputStreamPosition != -1) {
      currentEditLogInputStream.refresh(currentEditLogInputStreamPosition,
          mostRecentlyReadTransactionTxId);
    }
  }

  /**
   * Initialize first stream. Just for startup, we are extra careful to try 3
   * times, as there is potential race between finding the txid and then setting
   * up the stream.
   */
  protected void initialize() throws IOException {
    for (int i = 0; i < 3; i++) {
      try {
        LOG.info("Detecting current primary node - attempt " + i);
        detectJournalManager();
        
        LOG.info("Finding oldest segment txid - attempt " + i);
        currentSegmentTxId = findOldestLogSegmentTxid();

        LOG.info("Setting up input stream for txid: " + currentSegmentTxId
            + " - attempt " + i);
        setupIngestStreamWithRetries(currentSegmentTxId);
        return;
      } catch (IOException e) {
        LOG.warn("Initialization exception", e);
        if (i == 2) {
          LOG.error("Initialization failed.");
          throw e;
        }
      }
    }
  }

  /**
   * Setup the input stream to be consumed by the reader, with retries on
   * failures.
   */
  private void setupIngestStreamWithRetries(long txid) throws IOException {
    for (int i = 0; i < inputStreamRetries; i++) {
      try {
        setupCurrentEditStream(txid);
        return;
      } catch (IOException e) {
        if (i == inputStreamRetries - 1) {
          throw new IOException("Cannot obtain stream for txid: " + txid, e);
        }
        LOG.info("Error :", e);
      }
      sleep(1000);
      LOG.info("Retrying to get edit input stream for txid: " + txid
          + ", tried: " + (i + 1) + " times");
    }
  }

  /**
   * Setup the input stream to be consumed by the reader. The input stream
   * corresponds to a single segment.
   */
  private void setupCurrentEditStream(long txid) throws IOException {
    // get new stream
    currentEditLogInputStream = JournalSet.getInputStream(remoteJournalManager,
        txid);
    // we just started a new log segment
    currentSegmentTxId = txid;
    // indicate that we successfully reopened the stream
    mostRecentlyReadTransactionTime = now();
  }

  /**
   * Check if a segment of a given txid exists in the underlying storage
   * directory. Whne the reader cannot read any new data, it will
   * periodically check if there was some unclean shutdown, which results in
   * an unfinalized log.
   */
  boolean segmentExists(long txid) throws IOException {
    List<RemoteEditLog> segments = getManifest();
    for (RemoteEditLog segment : segments) {
      if (segment.getStartTxId() == txid) {
        return true;
      }
    }
    return false;
  }

  /**
   * Obtain the transaction id of the newest segment in the underlying storage.
   * Used at startup only.
   */
  long findLatestLogSegmentTxid() throws IOException {
    List<RemoteEditLog> segments = getManifest();
    return (segments.get(segments.size() - 1).getStartTxId());
  }
  
  long findOldestLogSegmentTxid() throws IOException {
    List<RemoteEditLog> segments = getManifest();
    return segments.get(0).getStartTxId();
  }

  /**
   * Get all available log segments present in the underlying storage directory.
   * This function will never return null, or empty list of segments - it will
   * throw exception in this case.
   */
  List<RemoteEditLog> getManifest() throws IOException {
    RemoteEditLogManifest rm = remoteJournalManager.getEditLogManifest(-1);
    if (rm == null || rm.getLogs().size() == 0) {
      throw new IOException("Cannot obtain the list of log segments");
    }
    return rm.getLogs();
  }

  /**
   * Sleep for n milliseconds.
   * Throw IOException when interrupted.
   * @param ms
   * @throws IOException
   */
  protected void sleep(long ms) throws IOException {
    try {
      Thread.sleep(ms);
    } catch (InterruptedException e) {
      LOG.error("Interrupted when sleeping", e);
      Thread.currentThread().interrupt();
      throw new IOException("Received interruption");
    }
  }

  /**
   * Get current time.
   */
  private static long now() {
    return System.currentTimeMillis();
  }
}
