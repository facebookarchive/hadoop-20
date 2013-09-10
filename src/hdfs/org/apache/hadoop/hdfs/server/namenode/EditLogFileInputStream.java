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

import java.io.BufferedInputStream;
import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.EOFException;
import java.io.DataInputStream;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hdfs.protocol.FSConstants;
import org.apache.hadoop.hdfs.server.common.HdfsConstants;
import org.apache.hadoop.hdfs.server.common.Storage;
import org.apache.hadoop.hdfs.server.namenode.JournalStream.JournalType;
import org.apache.hadoop.io.IOUtils;

import com.google.common.base.Preconditions;

/**
 * An implementation of the abstract class {@link EditLogInputStream}, which
 * reads edits from a local file.
 */
public class EditLogFileInputStream
    extends EditLogInputStream implements Closeable {
 
  static final Log LOG = LogFactory.getLog(EditLogInputStream.class);
   
  private File file;
  private FileInputStream fStream;
  private FileChannel fc;
  private RandomAccessFile rp;
  final private long firstTxId;
  final private long lastTxId;
  private int logVersion;
  private FSEditLogOp.Reader reader;
  private FSEditLogLoader.PositionTrackingInputStream tracker;
  
  static private enum State {
    UNINIT,
    OPEN,
    CLOSED
  }
  private State state = State.UNINIT;
  private final Object stateLock = new Object();
  
  /**
   * Open an EditLogInputStream for the given file.
   * The file is pretransactional, so has no txids
   * @param name filename to open
   * @throws LogHeaderCorruptException if the header is either missing or
   *         appears to be corrupt/truncated
   * @throws IOException if an actual IO error occurs while reading the
   *         header
   */
  public EditLogFileInputStream(File name)
      throws LogHeaderCorruptException, IOException {
    this(name, HdfsConstants.INVALID_TXID, HdfsConstants.INVALID_TXID, true);
  }

  /**
   * Open an EditLogInputStream for the given file.
   * @param name filename to open
   * @param firstTxId first transaction found in file
   * @param lastTxId last transaction id found in file
   * @throws LogHeaderCorruptException if the header is either missing or
   *         appears to be corrupt/truncated
   * @throws IOException if an actual IO error occurs while reading the
   *         header
   */
  EditLogFileInputStream(File name, long firstTxId, long lastTxId,
      boolean isInProgress)
      throws LogHeaderCorruptException, IOException {
    file = name;
    this.firstTxId = firstTxId;
    this.lastTxId = lastTxId;
    super.setIsInProgress(isInProgress);
  }
  
  private void init() throws LogHeaderCorruptException, IOException {
    synchronized (stateLock) {
      if (state != State.UNINIT) {
        return;
      }
      Preconditions.checkState(state == State.UNINIT);
      BufferedInputStream bin = null;
      DataInputStream in = null;
      try {
        rp = new RandomAccessFile(file, "r");    
        fStream = new FileInputStream(rp.getFD());
        fc = rp.getChannel();
  
        bin = new BufferedInputStream(fStream);  
        tracker = new FSEditLogLoader.PositionTrackingInputStream(bin);  
        in = new DataInputStream(tracker);
  
        try {
          logVersion = readLogVersion(in);
        } catch (EOFException eofe) {
          throw new LogHeaderCorruptException("No header found in log");
        }
        reader = new FSEditLogOp.Reader(in, logVersion);
        state = State.OPEN;
      } finally {
        if (reader == null) {
          IOUtils.cleanup(LOG, in, tracker, bin, rp, fStream);
          state = State.CLOSED;
        }
      }
    }
  }

  @Override
  public long getFirstTxId() {
    return firstTxId;
  }
  
  @Override
  public void refresh(long position, long skippedUntilTxid) throws IOException {
    init();
    fc.position(position);
    BufferedInputStream bin = new BufferedInputStream(fStream);
    tracker = new FSEditLogLoader.PositionTrackingInputStream(bin, position);
    DataInputStream in = new DataInputStream(tracker);
    reader = new FSEditLogOp.Reader(in, logVersion);
  }
  
  @Override
  public void position(long position) throws IOException {
    init();
    fc.position(position);
  }
  
  @Override
  public long getLastTxId() {
    return lastTxId;
  }

  @Override
  public String getName() {
    return file.getPath();
  }

  @Override
  public FSEditLogOp nextOp() throws IOException {
    FSEditLogOp op = null;
    switch (state) {
    case UNINIT:
      try {
        init();
      } catch (Throwable e) {
        LOG.error("caught exception initializing " + this, e);
        throw new IOException("Exception when initializing input stream");
      }
      Preconditions.checkState(state != State.UNINIT);
      return nextOp();
    case OPEN:
      op = reader.readOp(false);
      break;
    case CLOSED:
      break; // return null
    }
    return op;
  }

  @Override
  public FSEditLogOp nextValidOp() {
    try {
      return reader.readOp(true);
    } catch (Throwable e) {
      LOG.error("nextValidOp: got exception while reading " + this, e);
      return null;
    }
  }

  @Override
  public int getVersion() throws IOException {
    init();
    return logVersion;
  }

  @Override
  public void close() throws IOException {
    synchronized (stateLock) {
      if (state == State.OPEN) {
        IOUtils.cleanup(LOG, rp, fStream);
      }
      state = State.CLOSED;
    }
  }

  @Override
  public long length() throws IOException {
    return file.length();
  }
  
  @Override
  public String toString() {
    return getName();
  }

  static FSEditLogLoader.EditLogValidation validateEditLog(File file) throws IOException {
    EditLogFileInputStream in;
    try {
      in = new EditLogFileInputStream(file);
      in.getVersion();
    } catch (LogHeaderCorruptException corrupt) {
      // If it's missing its header, this is equivalent to no transactions
      FSImage.LOG.warn("Log at " + file + " has no valid header",
          corrupt);
      return new FSEditLogLoader.EditLogValidation(0, HdfsConstants.INVALID_TXID, 
                                                   HdfsConstants.INVALID_TXID, true);
    }
    
    try {
      return FSEditLogLoader.validateEditLog(in);
    } finally {
      IOUtils.closeStream(in);
    }
  }

  /**
   * Read the header of fsedit log
   * @param in fsedit stream
   * @return the edit log version number
   * @throws IOException if error occurs
   */
  static int readLogVersion(DataInputStream in) throws IOException,
      LogHeaderCorruptException {
    int logVersion = 0;
    // Read log file version. Could be missing.
    in.mark(4);
    // If edits log is greater than 2G, available method will return negative
    // numbers, so we avoid having to call available
    boolean available = true;
    try {
      logVersion = in.readByte();
    } catch (EOFException e) {
      available = false;
    }

    if (available) {
      in.reset();
      logVersion = in.readInt();
      if (logVersion < FSConstants.LAYOUT_VERSION) { // future version
        throw new LogHeaderCorruptException(
            "Unexpected version of the file system log file: " + logVersion
                + ". Current version = " + FSConstants.LAYOUT_VERSION + ".");
      }
    }
    if (logVersion < FSConstants.LAYOUT_VERSION || // future version
        logVersion > Storage.LAST_UPGRADABLE_LAYOUT_VERSION) { // unsupported
      throw new LogHeaderCorruptException(
          "Unexpected version of the file system log file: "
          + logVersion + ". Current version = "
          + FSConstants.LAYOUT_VERSION + ".");
    }
    return logVersion;
  }
  
  /**
   * Exception indicating that the header of an edits log file is
   * corrupted. This can be because the header is not present,
   * or because the header data is invalid (eg claims to be
   * over a newer version than the running NameNode)
   */
  public static class LogHeaderCorruptException extends IOException {
    private static final long serialVersionUID = 1L;

    private LogHeaderCorruptException(String msg) {
      super(msg);
    }
  }
  
  @Override
  public long getPosition() throws IOException{
    init();
    if (state == State.OPEN) {
      return tracker.getPos();
    } else {
      return 0;
    }
  }
  
  @Override
  public long getReadChecksum() {
    if (state == State.OPEN) {
      return reader.getChecksum();
    } else {
      return 0;
    }
  }
  
  @Override
  public JournalType getType() {
    return JournalType.FILE;
  }
}
