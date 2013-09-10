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

import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.LedgerEntry;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Enumeration;

import static org.apache.hadoop.hdfs.server.namenode.bookkeeper.BookKeeperJournalManager.bkException;
import static org.apache.hadoop.hdfs.server.namenode.bookkeeper.zk.ZkUtil.interruptedException;

/**
 * A {@link InputStream} over a BookKeeper ledger which maps to a specific
 * edit log segment.
 */
public class BookKeeperJournalInputStream extends InputStream {

  private static final Log LOG =
      LogFactory.getLog(BookKeeperJournalInputStream.class);

  // BookKeeper ledger is mutable as the ledger may need to be re-opened
  // by the caller in order to find the true end for tailing
  private LedgerHandle ledger;

  // This is not txId, this is the id of the first ledger entry
  private final long firstLedgerEntryId;

  // Maximum ledger entry id seen so far. In an in-progress edit log
  // stream this changes over time
  private long maxLedgerEntryIdSeen;

  private InputStream entryStream;

  // Keep track of the current state (see the InputStreamState inner
  // class for more detailed information) and the "last known good" state
  // (updated by calling savePosition()).
  private InputStreamState currentStreamState;
  private InputStreamState savedStreamState;

  static class InputStreamState {
    private long offsetInLedger; // How many bytes have we read from this ledger
    private long readerPosition; // How many bytes has the reader read
    private long nextLedgerEntryId; // Next ledger entry id to read
    private int offsetInEntry; // Bytes read from the current ledger entry

    InputStreamState() {
      offsetInLedger = 0;
      offsetInEntry = 0;
    }

    /**
     * Create a copy of another state object. Used to save the current state.
     */
    static InputStreamState copyOf(InputStreamState state) {
      InputStreamState copyState = new InputStreamState();
      copyState.setNextLedgerEntryId(state.getNextLedgerEntryId());
      copyState.setOffsetInEntry(state.getOffsetInEntry());
      copyState.setOffsetInLedger(state.getOffsetInLedger());
      copyState.setReaderPosition(state.getReaderPosition());
      return copyState;
    }

    long getOffsetInLedger() {
      return offsetInLedger;
    }

    void setOffsetInLedger(long offsetInLedger) {
      this.offsetInLedger = offsetInLedger;
    }

    void advanceOffsetInLedger(long numBytes) {
      offsetInLedger += numBytes;
    }

    long getReaderPosition() {
      return readerPosition;
    }

    void setReaderPosition(long readerPosition) {
      this.readerPosition = readerPosition;
    }

    long getNextLedgerEntryId() {
      return nextLedgerEntryId;
    }

    void incrementNextLedgerEntryId() {
      this.nextLedgerEntryId++;
    }

    void setNextLedgerEntryId(long nextLedgerEntryId) {
      this.nextLedgerEntryId = nextLedgerEntryId;
    }

    int getOffsetInEntry() {
      return offsetInEntry;
    }

    void setOffsetInEntry(int offsetInEntry) {
      this.offsetInEntry = offsetInEntry;
    }

    void advanceOffsetInEntry(long numBytes) {
      offsetInEntry += numBytes;
    }
  }

  /**
   * Create an input stream object for a specified BookKeper ledger
   * @param ledger The initial ledger instance
   * @param firstLedgerEntryId First ledger entry id (this is different from
   *                           HDFS transaction id!) to read from the ledger.
   */
  public BookKeeperJournalInputStream(LedgerHandle ledger,
      long firstLedgerEntryId) {
    this.ledger = ledger;
    this.firstLedgerEntryId = firstLedgerEntryId;
    maxLedgerEntryIdSeen = ledger.getLastAddConfirmed();
    currentStreamState = new InputStreamState();
    currentStreamState.setNextLedgerEntryId(firstLedgerEntryId);
  }

  @Override
  public int read() throws IOException {
    byte[] data = new byte[1];
    if (read(data, 0, 1) != 1) {
      return -1;
    }
    return data[0];
  }

  // Once we've reached the end of an entry stream, we want to open
  // a new stream for a new ledger entry
  private InputStream nextEntryStream() throws IOException {
    long nextLedgerEntryId = currentStreamState.getNextLedgerEntryId();
    if (nextLedgerEntryId > maxLedgerEntryIdSeen) {
      updateMaxLedgerEntryIdSeen();
      if (nextLedgerEntryId > maxLedgerEntryIdSeen) {
        // Return null if we've reached the end of the ledger: we can not
        // read beyond the end of the ledger and it is up to the caller to
        // either find the new "tail" of the ledger (if the ledger is in-
        // progress) or open the next ledger (if the ledger is finalized)
        if (LOG.isDebugEnabled()) {
          LOG.debug("Requesting to ledger entryId " + nextLedgerEntryId +
              ", but "+ " maxLedgerEntryIdSeen is " + maxLedgerEntryIdSeen +
              ", ledger length is " + ledger.getLength());
        }
        return null;
      }
    }
    try {
      Enumeration<LedgerEntry> entries =
          ledger.readEntries(nextLedgerEntryId, nextLedgerEntryId);
      currentStreamState.incrementNextLedgerEntryId();
      if (entries.hasMoreElements()) {
        LedgerEntry entry = entries.nextElement();
        if (entries.hasMoreElements()) {
          throw new IllegalStateException("More than one entry retrieved!");
        }
        currentStreamState.setOffsetInEntry(0);
        return entry.getEntryInputStream();
      }
    } catch (BKException e) {
      throw new IOException("Unrecoverable BookKeeper error reading entry " +
          nextLedgerEntryId, e);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IOException("Interrupted reading BookKeeper entry " +
          nextLedgerEntryId, e);
    }
    return null;
  }

  /**
   * Change the underlying ledger object in order to be able to correctly
   * determine the "tail" of the ledger.
   * @param ledger The new ledger object
   */
  public void resetLedger(LedgerHandle ledger) throws IOException {
    this.ledger = ledger;
    updateMaxLedgerEntryIdSeen();
  }

  /**
   * Set <code>maxLedgerEntryIdSeen</code> to the maximum of last confirmed
   * entry-id from a quorum of bookies and last confirmed entry-id from
   * metadata stored in ZooKeeper. The reason is to handle the case of
   * when a ledger becomes finalized mid-flight: in this case last confirmed
   * entry-id that is read from a quorum is no longer reliable, but a reliable
   * last-confirmed entry-id is now available in ZooKeeper metadata which is
   * updated when a ledger is finalized.
   * @throws IOException If there's an error talking to BookKeeper
   *                     or ZooKeeper
   */
  private void updateMaxLedgerEntryIdSeen() throws IOException {
    long lcFromMetadata = ledger.getLastAddConfirmed();
    long lcFromQuorum;
    try {
      lcFromQuorum = ledger.readLastConfirmed();
    } catch (BKException e) {
      bkException("Unable to read last confirmed ledger entry id "
          + "from ledger " + ledger.getId(), e);
      return;
    } catch (InterruptedException e) {
      interruptedException("Interrupted reading last confirmed ledger " +
          "entry id from ledger " + ledger.getId(), e);
      return;
    }
    long newMaxLedgerEntryIdSeen = Math.max(lcFromMetadata, lcFromQuorum);
    if (newMaxLedgerEntryIdSeen > maxLedgerEntryIdSeen) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Resetting maxLedgerEntryIdSeen from " + maxLedgerEntryIdSeen +
            " to " + newMaxLedgerEntryIdSeen);
      }
      maxLedgerEntryIdSeen = newMaxLedgerEntryIdSeen;
    }
  }

  /**
   * Preserve the state associated with the specified reader position
   * (meant for use with {@link #position(long)}
   * @param position The external reader position associated with the
   *                 current ledger state.
   */
  public void savePosition(long position) {
    currentStreamState.setReaderPosition(position);
    savedStreamState = InputStreamState.copyOf(currentStreamState);
  }

  /**
   * "Go back" to the specified reader position by resetting the reader
   * a saved state associated with that position.
   * @param position The reader position we want to go back to
   * @throws IllegalArgumentException If an illegal position is specified
   * @throws IOException If there is an error communicating with BookKeeper
   */
  public void position(long position) throws IOException {
    if (position == 0) {
      currentStreamState.setNextLedgerEntryId(firstLedgerEntryId);
      currentStreamState.setOffsetInEntry(0);
      entryStream = null;
    } else if (savedStreamState == null ||
        position != savedStreamState.getReaderPosition()) {
      // Seek to an arbitrary position through "brute force"
      if (position > Integer.MAX_VALUE) {
        throw new IllegalArgumentException("Asked to position to " + position +
            ", but can only \"brute-force\" skip up" + Integer.MAX_VALUE);
      }
      position(0);
      skip(position, (int) position);
    } else {
      // savedStream != null && position == savedStream.getReaderPosition()
      int bytesToSkip = 0;
      if (savedStreamState.getOffsetInLedger() > position) {
        // Since reading from the input stream is buffered, we usually will
        // read further into the ledger than the reader has actually
        // read into. In this case we will need to find out exactly *what*
        // position within the ledger entry matches with the reader's last
        // known good position.
        long entryStartPosition = savedStreamState.getOffsetInLedger() -
            savedStreamState.getOffsetInEntry();
        bytesToSkip = (int) (position - entryStartPosition);
      } else if (savedStreamState.getOffsetInLedger() < position) {
        throw new IllegalArgumentException("Saved offset in ledger (" +
            savedStreamState.getOffsetInLedger() + ") < position(" + position
            + ")");
      }
      long nextLedgerEntryId = savedStreamState.getNextLedgerEntryId() == firstLedgerEntryId ?
          firstLedgerEntryId : (savedStreamState.getNextLedgerEntryId() - 1);
      currentStreamState.setNextLedgerEntryId(nextLedgerEntryId);
      if (bytesToSkip > 0) {
        entryStream = null;
        skip(position, bytesToSkip);
      } else {
        if (currentStreamState.getNextLedgerEntryId() > 0) {
          currentStreamState.setNextLedgerEntryId(currentStreamState.getNextLedgerEntryId() - 1);
        }
        entryStream = nextEntryStream();
      }
    }
    currentStreamState.setOffsetInLedger(position);
  }

  private void skip(long position, int bytesToSkip) throws IOException {
    // Read further into the ledger such that our position matches the
    // position last consumed by the reader. Discard the data read.
    LOG.info("Attempting to skip " + bytesToSkip + " bytes to get to position "
        + position);
    byte[] data = new byte[bytesToSkip];
    int skipped;
    if ((skipped = read(data, 0, bytesToSkip)) != bytesToSkip) {
      throw new IllegalStateException("Could not skip to position " +
          position + ", tried to read " + bytesToSkip  +
          " but only read " + skipped + " bytes!");
    }
  }

  @Override
  public int read(byte[] buf, int off, int len) throws IOException {
    int bytesRead = readInternal(buf, off, len);
    currentStreamState.advanceOffsetInLedger(bytesRead);
    return bytesRead;
  }

  private int readInternal(byte[] buf, int off, int len) throws IOException {
    if (maxLedgerEntryIdSeen == -1) {
      // If this is an in-progress ledger, find out the true "tail" of the
      // ledger
      maxLedgerEntryIdSeen = ledger.getLastAddConfirmed();
      if (maxLedgerEntryIdSeen == -1) { // Nothing has been added to the ledger
        return 0;
      }
    }

    if (entryStream == null) {
      // If we are the end of the current entry, fetch the next one
      entryStream = nextEntryStream();
      if (entryStream == null) { // We are the end of the ledger
        return 0;
      }
    }

    // The calling classes may want to read a sequence of bytes that is
    // spread across multiple ledger entries. In this case, we will need to
    // in a loop: maintain the number of bytes read so far (the offset into
    // the buffer), when we reach the end of the current ledger entry, use
    // nextEntryStream() to begin reading the next ledger entry
    int bytesReadTotal = 0;
    while (bytesReadTotal < len) {
      int bytesReadLast = entryStream.read(buf, off + bytesReadTotal,
          len - bytesReadTotal);
      if (bytesReadLast == -1) {
        entryStream = nextEntryStream();
        if (entryStream == null) {
          return bytesReadTotal;
        }
      } else {
        currentStreamState.advanceOffsetInEntry(bytesReadLast);
        bytesReadTotal += bytesReadLast;
      }
    }
    return bytesReadTotal;
  }

  public long getLedgerLength() {
    return ledger.getLength();
  }

  public String getLedgerName() {
    return ledger.toString();
  }

  public void close() throws IOException {
    try {
      ledger.close();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IOException("Interrupted during close()", e);
    } catch (BKException e) {
      throw new IOException("BookKeeper error during close()", e);
    }
  }
}


