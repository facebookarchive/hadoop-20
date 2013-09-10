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
package org.apache.hadoop.hdfs.qjournal.client;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hdfs.server.namenode.EditLogOutputStream;
import org.apache.hadoop.hdfs.server.namenode.EditsDoubleBuffer;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp;
import org.apache.hadoop.hdfs.server.namenode.metrics.NameNodeMetrics;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.metrics.util.MetricsBase;
import org.apache.hadoop.metrics.util.MetricsTimeVaryingRate;

/**
 * EditLogOutputStream implementation that writes to a quorum of
 * remote journals.
 */
class QuorumOutputStream extends EditLogOutputStream {
  
  static final Log LOG = LogFactory.getLog(QuorumOutputStream.class);
  
  private final AsyncLoggerSet loggers;
  private EditsDoubleBuffer buf;
  private final long segmentTxId;
  private final int writeTimeoutMs;
  private String journalId;

  public QuorumOutputStream(AsyncLoggerSet loggers, long txId,
      int outputBufferCapacity, int writeTimeoutMs, NameNodeMetrics metrics, String journalId)
      throws IOException {
    super();
    this.buf = new EditsDoubleBuffer(outputBufferCapacity);
    this.loggers = loggers;
    this.segmentTxId = txId;
    this.writeTimeoutMs = writeTimeoutMs;
    this.journalId = journalId;
    if (metrics != null) { // Metrics is non-null only when used inside name node
      String metricsName = "sync_qjm_" + journalId + "_edit";
      MetricsBase retrMetrics = metrics.registry.get(metricsName);
      if (retrMetrics != null) {
        sync = (MetricsTimeVaryingRate) retrMetrics;     
      } else {
        sync = new MetricsTimeVaryingRate(metricsName, metrics.registry,
            "Journal Sync for " + journalId);
      }
    }
  }

  @Override
  public void write(FSEditLogOp op) throws IOException {
    buf.writeOp(op);
  }
  
  @Override
  public void writeRaw(byte[] bytes, int offset, int length) throws IOException {
    buf.writeRaw(bytes, offset, length);
  }
  
  @Override
  public void writeRawOp(byte[] bytes, int offset, int length, long txid)
      throws IOException {
    buf.writeRawOp(bytes, offset, length, txid);
  }

  @Override
  public void create() throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void close() throws IOException {
    if (buf != null) {
      buf.close();
      buf = null;
    }
  }

  @Override
  public void abort() throws IOException {
    QuorumJournalManager.LOG.warn("Aborting " + this);
    buf = null;
    close();
  }

  @Override
  public void setReadyToFlush() throws IOException {
    buf.setReadyToFlush();
  }

  @Override
  protected void flushAndSync(boolean durable) throws IOException {
    int numReadyBytes = buf.countReadyBytes();
    if (numReadyBytes > 0) {
      int numReadyTxns = buf.countReadyTxns();
      long firstTxToFlush = buf.getFirstReadyTxId();

      if (numReadyTxns < 0) {
        LOG.warn("There are no ready transaction");
        throw new IllegalStateException("There are no ready transaction");
      }

      // Copy from our double-buffer into a new byte array. This is for
      // two reasons:
      // 1) The IPC code has no way of specifying to send only a slice of
      //    a larger array.
      // 2) because the calls to the underlying nodes are asynchronous, we
      //    need a defensive copy to avoid accidentally mutating the buffer
      //    before it is sent.
      DataOutputBuffer bufToSend = new DataOutputBuffer(numReadyBytes);
      buf.flushTo(bufToSend);

      if (bufToSend.getLength() != numReadyBytes) {
        LOG.warn("Buffer size mismatch");
        throw new IllegalStateException("Buffer size mismatch");
      }

      byte[] data = bufToSend.getData();
      
      if (data.length != bufToSend.getLength()) {
        LOG.warn("Data size mismatch");
        throw new IllegalStateException("Data size mismatch");
      }

      QuorumCall<AsyncLogger, Void> qcall = loggers.sendEdits(
          segmentTxId, firstTxToFlush,
          numReadyTxns, data);
      try {
        loggers.waitForWriteQuorum(qcall, writeTimeoutMs, "sendEdits");
      } catch (IOException e) {
        String msg = "Got IOException when waiting for sendEdits. SegmentTxId: " + segmentTxId +
            ", firstTxToFlush: " + firstTxToFlush + ", numReadyTxns: " + numReadyTxns + ", lengthOfData: " +
            data.length;
        LOG.error(msg, e);
        // re-throw
        throw e;
      }
      
      // Since we successfully wrote this batch, let the loggers know. Any future
      // RPCs will thus let the loggers know of the most recent transaction, even
      // if a logger has fallen behind.
      loggers.setCommittedTxId(firstTxToFlush + numReadyTxns - 1, false);
    }
  }
  
  @Override
  public String toString() {
    return "QuorumOutputStream " + journalId + " starting at txid " + segmentTxId;
  }
  
  @Override
  public boolean shouldForceSync() {
    return buf.shouldForceSync();
  }

  @Override
  public String getName() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public long length() throws IOException {
    // TODO Auto-generated method stub
    return 0;
  }
  
  @Override
  public String generateHtmlReport() {
    StringBuilder sb = new StringBuilder();
    sb.append("Writing segment beginning at txid " + segmentTxId + "<br/>");
    loggers.appendHtmlReport(sb);
    return sb.toString();
  }
}
