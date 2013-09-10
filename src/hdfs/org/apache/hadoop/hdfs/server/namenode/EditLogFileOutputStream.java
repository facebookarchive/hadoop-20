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

package org.apache.hadoop.hdfs.server.namenode;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hdfs.protocol.FSConstants;
import org.apache.hadoop.hdfs.server.namenode.metrics.NameNodeMetrics;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.metrics.util.MetricsBase;
import org.apache.hadoop.metrics.util.MetricsTimeVaryingRate;

import com.google.common.annotations.VisibleForTesting;

/**
 * An implementation of the abstract class {@link EditLogOutputStream}, which
 * stores edits in a local file.
 */
public class EditLogFileOutputStream extends EditLogOutputStream {
  private static Log LOG = LogFactory.getLog(EditLogFileOutputStream.class);

  private File file;
  private FileOutputStream fp; // file stream for storing edit logs
  private FileChannel fc; // channel of the file stream for sync
  private EditsDoubleBuffer doubleBuf;

  static ByteBuffer fill = ByteBuffer.allocateDirect(1024 * 1024); // preallocation, 1MB

  static {
    fill.position(0);
    for (int i = 0; i < fill.capacity(); i++) {
      fill.put(FSEditLogOpCodes.OP_INVALID.getOpCode());
    }
  }

  /**
   * Creates output buffers and file object.
   * 
   * @param name
   *          File name to store edit log
   * @param size
   *          Size of flush buffer
   * @throws IOException
   */
  public EditLogFileOutputStream(File name, NameNodeMetrics metrics) throws IOException {
    super();
    FSNamesystem.LOG.info("Edit Log preallocate size for " + name +   
                          " is " + FSEditLog.preallocateSize + " bytes " +    
                          " and initial size of edits buffer is " +   
                          FSEditLog.sizeFlushBuffer + " bytes." +
                          "Max number of buffered transactions is " +
                          FSEditLog.maxBufferedTransactions);
    file = name;
    doubleBuf = new EditsDoubleBuffer(FSEditLog.sizeFlushBuffer);
    RandomAccessFile rp = new RandomAccessFile(name, "rw");
    fp = new FileOutputStream(rp.getFD()); // open for append
    fc = rp.getChannel();
    fc.position(fc.size());
    if (metrics != null) { // Metrics is non-null only when used inside name node
      String parentDirName = name.getParent();
      String metricsName = "sync_" + parentDirName + "_edit";
      MetricsBase retrMetrics = metrics.registry.get(metricsName);
      if (retrMetrics != null) {
        sync = (MetricsTimeVaryingRate) retrMetrics;     
      } else {
        sync = new MetricsTimeVaryingRate(metricsName, metrics.registry,
            "Journal Sync for " + parentDirName);
      }
    }
  }
  
  @Override
  public String toString() {
    return "EditLogFileOutputStream(" + file + ")";
  }

  @Override
  public void write(FSEditLogOp op) throws IOException {
    doubleBuf.writeOp(op);
  }
  
  /**
   * Write a transaction to the stream. The serialization format is:
   * <ul>
   *   <li>the opcode (byte)</li>
   *   <li>the transaction id (long)</li>
   *   <li>the actual Writables for the transaction</li>
   * </ul>
   * */
  @Override
  public void writeRaw(byte[] bytes, int offset, int length) throws IOException {
    doubleBuf.writeRaw(bytes, offset, length);
  }
  
  @Override
  public void writeRawOp(byte[] bytes, int offset, int length, long txid)
      throws IOException {
    doubleBuf.writeRawOp(bytes, offset, length, txid);
  }

  /**
   * Create empty edits logs file.
   */
  @Override
  public void create() throws IOException {
    fc.truncate(0);
    fc.position(0);
    doubleBuf.getCurrentBuf().writeInt(FSConstants.LAYOUT_VERSION);
    setReadyToFlush();
    flush();
  }

  @Override
  public void close() throws IOException {
    if (fp == null) {
      throw new IOException("Trying to use aborted output stream");
    }

    try {
      // close should have been called after all pending transactions
      // have been flushed & synced.
      // if already closed, just skip
      if (doubleBuf != null) {
        doubleBuf.close();
      }
      
      // remove the last INVALID marker from transaction log.
      if (fc != null && fc.isOpen()) {
        fc.truncate(fc.position());
        fc.close();
      }
      if (fp != null) {
        fp.close();
      }
    } finally {
      IOUtils.cleanup(FSNamesystem.LOG, fc, fp);
      doubleBuf = null;
      fc = null;
      fp = null;
    }
  }
  
  @Override
  public void abort() throws IOException {
    if (fp == null) {
      return;
    }
    IOUtils.cleanup(LOG, fp);
    fp = null;
  }

  /**
   * All data that has been written to the stream so far will be flushed. New
   * data can be still written to the stream while flushing is performed.
   */
  @Override
  public void setReadyToFlush() throws IOException {
    doubleBuf.getCurrentBuf().write(FSEditLogOpCodes.OP_INVALID.getOpCode()); // insert eof marker
    doubleBuf.setReadyToFlush();
  }

  /**
   * Flush ready buffer to persistent store. currentBuffer is not flushed as it
   * accumulates new log records while readyBuffer will be flushed and synced.
   */
  @Override
  protected void flushAndSync(boolean durable) throws IOException {
    if (fp == null) {
      throw new IOException("Trying to use aborted output stream");
    }
    preallocate(); // preallocate file if necessary
    if (doubleBuf.isFlushed()) {
      return;
    }
    doubleBuf.flushTo(fp);
    if (durable) {
      fc.force(false); // metadata updates not needed
    }
    fc.position(fc.position() - 1); // skip back the end-of-file marker
  }
  
  /**
   * @return true if the number of buffered data exceeds the intial buffer size
   */
  @Override
  public boolean shouldForceSync() {
    return doubleBuf.shouldForceSync();
  }
  
  /**
   * Return the size of the current edit log including buffered data.
   */
  @Override
  public long length() throws IOException {
    // file size + size of both buffers
    return fc.size() + doubleBuf.countBufferedBytes();
  }

  // allocate a big chunk of data
  private void preallocate() throws IOException {
    long position = fc.position();
    long triggerSize = Math.max(FSEditLog.preallocateSize / 100, 4096);
    if (position + triggerSize >= fc.size()) {
      if(FSNamesystem.LOG.isDebugEnabled()) {
        FSNamesystem.LOG.debug("Preallocating Edit log, current size "
            + fc.size());
      }
      fill.position(0);
      int written = fc.write(fill, position);
      if(FSNamesystem.LOG.isDebugEnabled()) {
        FSNamesystem.LOG.debug("Edit log size is now " + fc.size() +
            " written " + written + " bytes " + " at offset " + position);
      }
    }
  }

  /**
   * Returns the file associated with this stream.
   */
  File getFile() {
    return file;
  }

  /**
   * @return true if this stream is currently open.
   */
  public boolean isOpen() {
    return fp != null;
  }
  
  @VisibleForTesting
  public void setFileChannelForTesting(FileChannel fc) {
    this.fc = fc;
  }
  
  @VisibleForTesting
  public FileChannel getFileChannelForTesting() {
    return fc;
  }

  @Override
  public String getName() {
    return file.getPath();
  }
}
