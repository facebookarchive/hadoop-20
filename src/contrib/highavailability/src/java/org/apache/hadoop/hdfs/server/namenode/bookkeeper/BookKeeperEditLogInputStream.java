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
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hdfs.protocol.FSConstants;
import org.apache.hadoop.hdfs.server.common.HdfsConstants;
import org.apache.hadoop.hdfs.server.namenode.EditLogFileInputStream;
import org.apache.hadoop.hdfs.server.namenode.EditLogInputStream;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogLoader;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogLoader.PositionTrackingInputStream;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp.Reader;
import org.apache.hadoop.hdfs.server.namenode.JournalStream.JournalType;
import org.apache.hadoop.hdfs.server.namenode.bookkeeper.metadata.EditLogLedgerMetadata;
import org.apache.hadoop.io.IOUtils;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;

/**
 * An implement of the abstract class {@link EditLogInputStream}, that reads
 * edits from a BookKeeper ledger, which stores edits for a single log
 * segment.
 */
public class BookKeeperEditLogInputStream extends EditLogInputStream {

  private static final Log LOG =
      LogFactory.getLog(BookKeeperEditLogInputStream.class);

  private final LedgerHandleProvider ledgerProvider;

  private final long ledgerId;
  private final long firstTxId;
  // Last transaction id in the segment, or -1 if the segment is in-progress
  private final long lastTxId;

  // The underlying input stream implementation that abstracts away reading
  // from a BookKeeper ledger into a streaming interface as well as "rewinding"
  // back to either the beginning or the last known position
  private BookKeeperJournalInputStream journalInputStream;
  private PositionTrackingInputStream tracker; // Tracks reader position
  @VisibleForTesting
  BufferedInputStream bin; // Buffers reading from journalInputStream
  private Reader reader; // Reads transactions from the stream

  @VisibleForTesting
  int logVersion;
  private final long firstBookKeeperEntry;
  private final boolean inProgress;

  /**
   * Exception indicating that the header of a ledger containing an edit
   * log segment is corrupted. This could be because the header is not
   * present or because the version in the header claims to be a version
   * newer than the version in the running NameNode. Mimics an exception
   * that thrown by {@link EditLogFileInputStream} in similar conditions
   * @see EditLogFileInputStream.LogHeaderCorruptException
   */
  private static class LedgerHeaderCorruptException extends IOException {
    private static final long serialVersionUID = 1L;

    private LedgerHeaderCorruptException(String msg) {
      super(msg);
    }
  }

  /**
   * Opens an EditLogInputStream for a BookKeeper ledger with a specified id,
   * starting from a BookKeeper entry with a specific entry id. Note that the
   * BookKeeper entry id is different from an HDFS transaction id: the entry id
   * is specific to BookKeeper itself and not related to the HDFS transaction
   * id.
   * @param ledgerProvider {@link BookKeeperJournalManager} instance used for
   *                       opening and re-opening ledgers based on the id
   * @param ledgerId BookKeeper specific id of the ledger. This is read from
   *                 ZooKeeper
   * @param firstBookKeeperEntry Entry id of the first BookKeeper entry
   *                             containing edit log segment data
   * @param firstTxId Id of the first HDFS transaction id expected to be
   *                  in the segment stored in the specific ledger
   * @param lastTxId Id of the last transaction id expected to be in the segment
   *                 stored in the specified ledger or -1 if the segment is in
   *                 progress
   * @throws IOException
   */
  public BookKeeperEditLogInputStream(LedgerHandleProvider ledgerProvider,
      long ledgerId,
      long firstBookKeeperEntry,
      long firstTxId,
      long lastTxId,
      boolean inProgress) throws IOException {
    if (firstBookKeeperEntry < 0) {
      throw new IllegalArgumentException("Invalid first BookKeeper entry id to read: " +
          firstBookKeeperEntry + ", cannot be < 0!");
    }
    this.firstBookKeeperEntry = firstBookKeeperEntry;
    this.ledgerProvider = ledgerProvider;
    this.ledgerId = ledgerId;
    this.firstTxId = firstTxId;
    this.lastTxId = lastTxId;
    this.inProgress = inProgress;
  }

  public void init() throws IOException {
    LedgerHandle ledger = ledgerProvider.openForReading(ledgerId);
    if (!isInProgress() && firstBookKeeperEntry > ledger.getLastAddConfirmed()) {
      // ledger.getLastAddConfirmed() returns the last quorum-acknowledged entry
      // id in the ledger. Unless the segment is in-progress, we should throw
      // an exception if this last entry is lower than the first expected entry id.
      throw new IllegalArgumentException(
          "Invalid first BookKeeper entry to read: " + firstBookKeeperEntry +
              " > last confirmed entry id(" + ledger.getLastAddConfirmed() +
              ")");
    }

    journalInputStream = new BookKeeperJournalInputStream(ledger,
        firstBookKeeperEntry);
    bin = new BufferedInputStream(journalInputStream);
    tracker = new PositionTrackingInputStream(bin, 0);
    DataInputStream in = new DataInputStream(tracker);
    try {
      logVersion = readLogVersion(in);
    } catch (EOFException e) {
      throw new LedgerHeaderCorruptException("No header file in the ledger");
    }
    reader = new Reader(in, logVersion);
    LOG.info("Reading from ledger id " + ledgerId +
        ", starting with book keeper entry id " + firstBookKeeperEntry +
        ", log version " + logVersion +
        ", first txn id " +
        firstTxId +  (isInProgress() ?
                          ", in-progress log segment"
                          : (" last txn id " + lastTxId))
        + ".");
  }

  /**
   * Safely reads the log version from the stream. Logic is exactly the same
   * as in the equivalent {@link EditLogFileInputStream} method.
   * @see EditLogFileInputStream#readLogVersion(DataInputStream)
   * @return The log version or 0 if stream is empty
   */
  private static int readLogVersion(DataInputStream in) throws IOException {
    int logVersion = 0;
    in.mark(4);
    // See comments in EditLogFileInputStream as to why readLogVersion is
    // implemented in this way
    boolean available = true;
    try {
      logVersion = in.readByte();
    } catch (EOFException e) {
      available = false;
    }

    if (available) {
      in.reset();
      logVersion = in.readInt();
      if (logVersion < FSConstants.LAYOUT_VERSION) {
        throw new LedgerHeaderCorruptException(
            "Unexpected version of the log segment in the ledger: " + logVersion +
                ". Current version is " + FSConstants.LAYOUT_VERSION + ".");
      }
    }
    return logVersion;
  }

  public static FSEditLogLoader.EditLogValidation validateEditLog(
      LedgerHandleProvider ledgerProvider,
      EditLogLedgerMetadata ledgerMetadata) throws IOException {
    BookKeeperEditLogInputStream in;
    try {
      in = new BookKeeperEditLogInputStream(ledgerProvider,
          ledgerMetadata.getLedgerId(), 0, ledgerMetadata.getFirstTxId(),
          ledgerMetadata.getLastTxId(), ledgerMetadata.getLastTxId() == -1);
    } catch (LedgerHeaderCorruptException e) {
      LOG.warn("Log at ledger id" + ledgerMetadata.getLedgerId() +
          " has no valid header", e);
      return new FSEditLogLoader.EditLogValidation(0,
          HdfsConstants.INVALID_TXID, HdfsConstants.INVALID_TXID, true);
    }

    try {
      return FSEditLogLoader.validateEditLog(in);
    } finally {
      IOUtils.closeStream(in);
    }
  }

  /**
   * Returns true if the segment is in progress from the reader point of view
   * at the time of instantiation.
   */
  @Override
  public boolean isInProgress() {
    return inProgress;
  }

  @Override
  public long getFirstTxId() {
    return firstTxId;
  }

  @Override
  public long getLastTxId() {
    return lastTxId;
  }

  @Override
  public void close() throws IOException {
    checkInitialized();

    // Let JournalInputStream handle cleanup
    journalInputStream.close();
  }

  @Override
  public FSEditLogOp nextOp() throws IOException {
    checkInitialized();

    return reader.readOp(false);
  }

  @Override
  public int getVersion() throws IOException {
    checkInitialized();

    return logVersion;
  }

  @Override
  public long getPosition() throws IOException {
    checkInitialized();

    long pos = tracker.getPos();
    journalInputStream.savePosition(pos);
    return pos;
  }

  @Override
  public void position(long position) throws IOException {
    throw new IOException("position() not supported by " +
        getClass());
  }

  /**
   * Gets the size of the ledger in bytes
   * @return The size of the ledger in bytes
   */
  @Override
  public long length() throws IOException {
    checkInitialized();

    return journalInputStream.getLedgerLength();
  }

  public synchronized void checkInitialized() throws IOException {
    if (journalInputStream == null) {
      init();
    }
  }

  /**
   * Refresh, preferably to a known position
   * @see EditLogInputStream#refresh(long)
   * @see BookKeeperJournalInputStream#position(long)
   */
  @Override
  public void refresh(long position, long skippedUntilTxid) throws IOException {
    checkInitialized();

    if (isInProgress()) {
      // If a ledger is in progress, re-open it for reading in order
      // to determine the correct bounds of the ledger.
      LedgerHandle ledger = ledgerProvider.openForReading(ledgerId);
      journalInputStream.resetLedger(ledger);
    }
    // Try to set the underlying stream to the specified position
    journalInputStream.position(position);
    // Reload the position tracker and log reader to adjust to the newly
    // refreshed position
    bin = new BufferedInputStream(journalInputStream);
    tracker = new PositionTrackingInputStream(bin, position);
    DataInputStream in = new DataInputStream(tracker);
    if (position == 0) { // If we are at the beginning, re-read the version
      logVersion = readLogVersion(in);
    }
    reader = new Reader(in, logVersion);
  }

  @Override
  public String getName() {
    return journalInputStream.getLedgerName();
  }

  @Override
  public long getReadChecksum() {
    return reader.getChecksum();
  }
  
  @Override
  public JournalType getType() {
    return JournalType.EXTERNAL;
  }

  @Override
  public String toString() {
    return "BookKeeperEditLogInputStream{" +
        "ledgerId=" + ledgerId +
        ", firstTxId=" + firstTxId +
        ", lastTxId=" + lastTxId +
        ", journalInputStream=" + journalInputStream +
        ", tracker=" + tracker +
        ", bin=" + bin +
        ", reader=" + reader +
        ", logVersion=" + logVersion +
        '}';
  }
}

