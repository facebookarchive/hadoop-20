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
import org.apache.hadoop.hdfs.protocol.FSConstants;
import org.apache.hadoop.hdfs.server.namenode.EditLogFileOutputStream;
import org.apache.hadoop.hdfs.server.namenode.EditLogOutputStream;
import org.apache.hadoop.hdfs.server.namenode.EditsDoubleBuffer;
import org.apache.hadoop.hdfs.server.namenode.FSEditLog;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.namenode.metrics.NameNodeMetrics;
import org.apache.hadoop.hdfs.util.InjectionEvent;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.metrics.util.MetricsBase;
import org.apache.hadoop.metrics.util.MetricsTimeVaryingRate;
import org.apache.hadoop.util.InjectionHandler;

import java.io.IOException;

// TODO (avf): Move this to hdfs-core

/**
 * An implementation of the abstract class {@link EditLogOutputStream},
 * which stores edits in BookKeeper, a highly-available write-ahead logging
 * service.
 *
 * It a complete re-write of the BookKeeperEditLogOutputStream from the
 * bkjournal contrib project in Apache HDFS trunk: the reason for the
 * rewrite is to maintain internal and external behaviour that is almost
 * identical to the file based implementation.
 *
 * @see EditLogFileOutputStream
 */
public class BookKeeperEditLogOutputStream extends EditLogOutputStream {

  private static final Log LOG =
      LogFactory.getLog(BookKeeperEditLogOutputStream.class);

  private final LedgerHandle ledger; // BookKeeper ledger to store this log segment
  private EditsDoubleBuffer doubleBuf;
  private BookKeeperJournalOutputStream outputStream; // Stream for writing to the ledger

  public BookKeeperEditLogOutputStream(LedgerHandle ledger) throws IOException {
    this(ledger, null, null);
  }

  public BookKeeperEditLogOutputStream(LedgerHandle ledger, String parentPath,
      NameNodeMetrics metrics) throws IOException {
    super();
    this.ledger = ledger;
    doubleBuf = new EditsDoubleBuffer(FSEditLog.sizeFlushBuffer);
    outputStream = new BookKeeperJournalOutputStream(ledger);
    if (metrics != null) { // Metrics is non-null only when used inside name node
      String metricsName = "sync_bk_" + parentPath + "_edit";
      MetricsBase retrMetrics = metrics.registry.get(metricsName);
      if (retrMetrics != null) {
        sync = (MetricsTimeVaryingRate) retrMetrics;
      } else {
        sync = new MetricsTimeVaryingRate(metricsName, metrics.registry,
            "Journal Sync for BookKeeper" + ledger.getId());
      }
    }
  }

  @Override
  public String getName() {
    return getClass().getName();
  }

  @Override
  public void write(FSEditLogOp op) throws IOException {
    doubleBuf.writeOp(op);
  }

  @Override
  public void create() throws IOException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Called create() on " + toString());
    }
    // Write the layout version to the Ledger. This is different from
    // BookKeeperEditLogOutputStream in contrib/bkjournal in
    // Apache trunk, but in line with EditLogFileOutputStream.
    doubleBuf.getCurrentBuf().writeInt(FSConstants.LAYOUT_VERSION);
    setReadyToFlush();
    flush();
  }

  @Override
  public void close() throws IOException {
    if (outputStream == null) {
      throw new IOException("Trying to use aborted output stream!");
    }
    try {
      if (doubleBuf != null) {
        doubleBuf.close();
      }
    } finally {
      try {
        IOUtils.cleanup(FSNamesystem.LOG, outputStream);
        doubleBuf = null;
        outputStream = null;
      } finally {
        // Make sure the ledger is always closed!
        closeLedger();
      }
    }
  }

  @Override
  public boolean shouldForceSync() {
    return doubleBuf.shouldForceSync();
  }

  /**
   * Close the ledger to ensure it can no longer be written to.
   */
  private void closeLedger() throws IOException {
    try {
      ledger.close();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IOException("Interrupted waiting to close ledger", e);
    } catch (BKException e) {
      throw new IOException("Unexpected BookKeeper error closing ledger", e);
    }
  }

  @Override
  public void setReadyToFlush() throws IOException {
    // Since we can not (and do not need to) pre-fill or randomly write to
    // a BookKeeper ledger, we don't need to write the OP_INVALID marker here
    doubleBuf.setReadyToFlush();
  }

  /**
   * Flush the contents of the "ready" buffer to a durable BookKeeper ledger
   * BookKeeper ledger. After this method returns, we are guaranteed to
   * have persisted the log records stored in the ready buffer to a quorum
   * of bookies that store the underlying ledger.
   */
  @Override
  protected void flushAndSync(boolean durable) throws IOException {
    if (outputStream == null) {
      throw new IOException("Trying to use aborted output stream!");
    }
    if (doubleBuf.isFlushed()) {
      return;
    }
    // Under the covers, this invokes a method that does not return until
    // we have successfully persisted the buffer to a quorum of bookies.
    doubleBuf.flushTo(outputStream);
  }

  /**
   * Get the length of the log stream, including the data written to
   * BookKeeper and the buffered data
   * @see EditLogFileOutputStream#length()
   */
  @Override
  public long length() throws IOException {
    return ledger.getLength() + doubleBuf.countBufferedBytes();
  }

  @Override
  public void abort() throws IOException {
    if (outputStream == null) {
      return;
    }
    IOUtils.cleanup(LOG, outputStream);
    outputStream = null;
    closeLedger(); // Close aborted ledgers
  }

  @Override
  public void writeRaw(byte[] bytes, int offset, int length) throws IOException {
    doubleBuf.writeRaw(bytes, offset, length);
  }
  
  @Override
  public void writeRawOp(byte[] bytes, int offset, int length, long txid)
      throws IOException {
    doubleBuf.writeRawOp(bytes, offset, length, txid);
  }
}
