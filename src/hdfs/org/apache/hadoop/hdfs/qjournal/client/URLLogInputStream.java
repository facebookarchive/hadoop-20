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

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.EOFException;
import java.io.DataInputStream;
import java.io.InputStream;
import java.net.URL;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hdfs.protocol.FSConstants;
import org.apache.hadoop.hdfs.server.common.HdfsConstants;
import org.apache.hadoop.hdfs.server.namenode.EditLogInputStream;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogLoader;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp;
import org.apache.hadoop.hdfs.server.namenode.JournalStream.JournalType;
import org.apache.hadoop.hdfs.util.InjectionEvent;
import org.apache.hadoop.util.InjectionHandler;

import com.google.common.base.Joiner;

/**
 * An implementation of the abstract class {@link EditLogInputStream}, which
 * reads edits from a local file.
 */
public class URLLogInputStream extends EditLogInputStream {
 
  static final Log LOG = LogFactory.getLog(URLLogInputStream.class);
  
  private URLLog log;
  private InputStream fStream;
  final private long firstTxId;
  
  private final int logVersion;
  private FSEditLogOp.Reader reader;
  private FSEditLogLoader.PositionTrackingInputStream tracker;
  
  private long lastValidTxId;
  private volatile boolean readLastValidTxId = false;
  
  private boolean disabled = false;
  
  private long lastRefreshLogTime;
  private static final long refreshLogInterval = 10 * 1000; // 10s
  
  // the position to which transactions were returned to the client
  // in general, we might read a valid transaction internally, but beyond
  // maxValidTransaction id - in this situation we return null to the client
  // and DO NOT advance currentPosition
  private long currentPosition = 0;

  URLLogInputStream(AsyncLogger logger, long firstTxId, int httpTimeout)
      throws LogHeaderCorruptException, IOException {
    log = new URLLog(logger, firstTxId, httpTimeout);
    fStream = log.getInputStream(0);
    
    lastValidTxId = log.getLastValidTxId();
    super.setIsInProgress(log.getIsInProgress());

    BufferedInputStream bin = new BufferedInputStream(fStream);  
    tracker = new FSEditLogLoader.PositionTrackingInputStream(bin);  
    DataInputStream in = new DataInputStream(tracker);

    try {
      logVersion = readLogVersion(in);
    } catch (EOFException eofe) {
      throw new LogHeaderCorruptException("No header found in log");
    }
    reader = new FSEditLogOp.Reader(in, logVersion);
    this.firstTxId = firstTxId;
    this.disabled = false;
    // set initial position (version is 4 bytes)
    this.currentPosition = tracker.getPos();
  }

  @Override
  public long getFirstTxId() {
    return firstTxId;
  }
  
  @Override
  public void refresh(long position, long skippedUntilTxid) throws IOException {
    // close previous connection
    try {
      fStream.close();
    } catch (IOException e) {
      // ignore exceptions
      LOG.warn("Error when closing stream: " + this, e);
    }
    
    readLastValidTxId = false;
    
    try {
      fStream = log.getInputStream(position);
    } catch (Exception e) {
      // log only once in a while
      if (now() - lastRefreshLogTime > refreshLogInterval) {
        lastRefreshLogTime = now();
        LOG.info("Refresh at position: " + position
            + " failed, disabling stream: " + getName());
      }
      
      // this log is disabled
      this.disabled = true;
      
      // preserve valid position
      this.currentPosition = position;
      
      // clear out other stuff
      this.tracker = null;
      this.reader = null;
      this.lastValidTxId = HdfsConstants.INVALID_TXID;
      this.readLastValidTxId = true;

      // rethrow the exception
      throw new IOException(e);
    }
    
    lastValidTxId = log.getLastValidTxId();
    super.setIsInProgress(log.getIsInProgress());
    
    BufferedInputStream bin = new BufferedInputStream(fStream);  
    tracker = new FSEditLogLoader.PositionTrackingInputStream(bin, position); 
    DataInputStream in = new DataInputStream(tracker);
    reader = new FSEditLogOp.Reader(in, logVersion);
    
    // set current position
    currentPosition = tracker.getPos();
    
    // stream is up again
    if (disabled) {
      LOG.info("Refresh at position: " + position
          + " succeeded, re-enabling stream: " + getName());
      this.disabled = false;
    }
  }
  
  @Override
  public void position(long position) throws IOException {
    throw new IOException("Not supported");
  }
  
  @Override
  public long getLastTxId() {
    // as far as the reader is concerned this is the last txid of
    // this segment
    return lastValidTxId;
  }

  @Override
  public String getName() {
    URL url = log.getURL();
    String address = url.getAuthority();
    return Joiner.on("").join("[JN: ", address, ", firstTxid: ",
        getFirstTxId(), ", lastValidTxId: ", lastValidTxId, ", inProgress: ",
        isInProgress(), "]");
  }

  @Override
  public FSEditLogOp nextOp() throws IOException {
    InjectionHandler.processEvent(
        InjectionEvent.QJM_URLLOGEDITLOGSTREAM_NEXTOP, log.getURL());
    
    // 1) stream failed at previous refresh, hence we have no valid transactions
    // to serve
    if (this.disabled) {
      throw new IOException("Stream: " + getName() + " is disabled.");
    }
    
    // 2) we previously have served everything that was valid
    if (readLastValidTxId) {
      return null; // do not advance position
    }
    
    // read the operation
    FSEditLogOp op = reader.readOp(false);
    
    // 3) no more transactions (possibly EOF reached)
    if (op == null) {
      readLastValidTxId = true;
      return null; // do not advance position
    }
    
    // 4) we read a valid transaction but beyond lastValidTxId
    if (op.getTransactionId() > lastValidTxId) {
      readLastValidTxId = true;
      LOG.info("Reached the end of valid transactions...");
      return null; // do not advance position
    } 
    
    // we reached the last valid transaction
    if (op.getTransactionId() == lastValidTxId) {
      readLastValidTxId = true;
    }
    
    // 5) we have a valid transaction, advance the position
    currentPosition = tracker.getPos();
    
    return op;
  }

  @Override
  public FSEditLogOp nextValidOp() {
    try {
      FSEditLogOp op = reader.readOp(true);
      currentPosition = tracker.getPos();
      return op;
    } catch (Throwable e) {
      LOG.error("nextValidOp: got exception while reading " + this, e);
      return null;
    }
  }

  @Override
  public int getVersion() throws IOException {
    return logVersion;
  }

  @Override
  public void close() throws IOException {
    fStream.close();
  }

  @Override
  public long length() throws IOException {
    return log.length();
  }
  
  @Override
  public String toString() {
    return getName();
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
    return currentPosition;
  }
  
  @Override
  public long getReadChecksum() {
    return reader.getChecksum();
  }
  
  @Override
  public JournalType getType() {
    return JournalType.FILE;
  }
  
  public boolean isDisabled() {
    return disabled;
  }
  
  private static long now() {
    return System.currentTimeMillis();
  }
}
