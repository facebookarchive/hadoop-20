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
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hdfs.server.namenode.EditsDoubleBuffer;

import java.io.IOException;
import java.io.OutputStream;

/**
 * Wrapper OutputStream for that {@link EditsDoubleBuffer} in
 * {@link BookKeeperEditLogOutputStream} will flush to. This is a
 * re-write of BookKeeper's own LedgerOutputStream which ensures that
 * any exception thrown (e.g., if we are unable to confirm that we've
 * written to a quorum of bookies) is propagated back to the caller.
 * <p>
 * A new entry is created whenever any of the write methods are called.
 * Note: it is the responsibility of the class that created the instance
 * to close the underlying ledger.
 *
 * @see org.apache.bookkeeper.streaming.LedgerOutputStream
 */
public class BookKeeperJournalOutputStream extends OutputStream {

  private static final Log LOG = LogFactory.getLog(BookKeeperJournalOutputStream.class);

  private final LedgerHandle ledger; // The underlying ledger

  /**
   * Creates an {@link OutputStream} wrapping a BookKeeper ledger
   * @param ledger The ledger that writes will go to. It is the caller's
   *               responsibility to close the ledger.
   */
  public BookKeeperJournalOutputStream(LedgerHandle ledger) {
    this.ledger = ledger;
  }

  /**
   * Write the buffer to a new entry in a BookKeeper ledger or throw
   * an IOException if we are unable to successfully write to a quorum
   * of bookies
   * @param buf Buffer to write from
   * @param off Offset in the buffer
   * @param len How many bytes to write, starting the offset
   * @throws IOException If we are interrupted while writing to BookKeeper or
   *                     if we are unable to successfully add the entry to
   *                     a quorum of bookies.
   */
  private synchronized void addBookKeeperEntry(byte[] buf, int off, int len)
      throws IOException {
    try {
      ledger.addEntry(buf, off, len);
      if (LOG.isDebugEnabled()) {
        LOG.debug("Last add pushed to ledger " + ledger.getId() + " is " +
            ledger.getLastAddPushed());
        LOG.debug("Last add confirmed to ledger " + ledger.getId() + " is " +
            ledger.getLastAddConfirmed());
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IOException("Interrupted writing to BookKeeper", e);
    } catch (BKException e) {
      throw new IOException("Failed to write to BookKeeper", e);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void write(int i) throws IOException {
    byte oneByte[] = new byte[] { (byte) (i & 0xff) };
    addBookKeeperEntry(oneByte, 0, 1);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void write(byte[] buf) throws IOException {
    addBookKeeperEntry(buf, 0, buf.length);
  }

  /**
   * Write the buffer to a new entry in a BookKeeper ledger.
   * @see OutputStream#write(byte[], int, int)
   * @see #addBookKeeperEntry(byte[], int, int)
   * @throws IOException If there is an error adding the BookKeeper entry.
   */
  @Override
  public void write(byte[] buf, int off, int len) throws IOException {
    addBookKeeperEntry(buf, off, len);
  }
}

