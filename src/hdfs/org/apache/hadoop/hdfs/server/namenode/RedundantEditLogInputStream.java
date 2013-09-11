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

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hdfs.server.common.HdfsConstants;
import org.apache.hadoop.hdfs.server.namenode.JournalStream.JournalType;
import org.apache.hadoop.io.IOUtils;

import com.google.common.base.Preconditions;
import com.google.common.primitives.Longs;

/**
 * A merged input stream that handles failover between different edit logs.
 *
 * We will currently try each edit log stream exactly once.  In other words, we
 * don't handle the "ping pong" scenario where different edit logs contain a
 * different subset of the available edits.
 */
public class RedundantEditLogInputStream extends EditLogInputStream {
  public static final Log LOG = LogFactory.getLog(RedundantEditLogInputStream.class.getName());
  private int curIdx;
  private long prevTxId;
  private final EditLogInputStream[] streams;
  
  // for regular startup all streams should be finalized (inProgressOk = false)
  // for tailing streams are usually unfinalized (inProgressOk = true)
  // if this is false, "refresh" cannot be called
  private final boolean inProgressOk;

  /**
   * States that the RedundantEditLogInputStream can be in.
   *
   * <pre>
   *                   start (if no streams)
   *                           |
   *                           V
   * PrematureEOFException  +----------------+
   *        +-------------->| EOF            |<--------------+
   *        |               +----------------+               |
   *        |                                                |
   *        |          start (if there are streams)          |
   *        |                  |                             |
   *        |                  V                             | EOF
   *        |   resync      +----------------+ skipUntil  +---------+
   *        |   +---------->| SKIP_UNTIL     |----------->|  OK     |
   *        |   |           +----------------+            +---------+
   *        |   |                | IOE   ^ fail over to      | IOE
   *        |   |                V       | next stream       |
   * +----------------------+   +----------------+           |
   * | STREAM_FAILED_RESYNC |   | STREAM_FAILED  |<----------+
   * +----------------------+   +----------------+
   *                  ^   Recovery mode    |
   *                  +--------------------+
   * </pre>
   */
  static private enum State {
    /** We need to skip until prevTxId + 1 */
    SKIP_UNTIL,
    /** We're ready to read opcodes out of the current stream */
    OK,
    /** The current stream has failed. */
    STREAM_FAILED,
    /** The current stream has failed, and resync() was called.  */
    STREAM_FAILED_RESYNC,
    /** There are no more opcodes to read from this
     * RedundantEditLogInputStream */
    EOF;
  }

  private State state;
  private IOException prevException;
  
  private long lastRefreshLogTime;
  private static final long refreshLogInterval = 10 * 1000; // 10s
  
  static Comparator<EditLogInputStream> segmentComparator 
    = new Comparator<EditLogInputStream>() {
    @Override
    public int compare(EditLogInputStream a, EditLogInputStream b) {
      if (a.isInProgress() && !b.isInProgress()) {
        // prefer finalized segment b
        return -1;
      }
      if (!a.isInProgress() && b.isInProgress()) {
        // prefer finalized segment a
        return 1;
      }
      // otherwise compare last txid
      return Longs.compare(b.getLastTxId(), a.getLastTxId());
    }
  };

  public RedundantEditLogInputStream(Collection<EditLogInputStream> streams,
      long startTxId, boolean inProgressOk) {
    this.curIdx = 0;
    this.prevTxId = (startTxId == HdfsConstants.INVALID_TXID) ?
      HdfsConstants.INVALID_TXID : (startTxId - 1);
    this.state = (streams.isEmpty()) ? State.EOF : State.SKIP_UNTIL;
    this.prevException = null;
    this.inProgressOk = inProgressOk;
    // EditLogInputStreams in a RedundantEditLogInputStream can't be pre-transactional.
    EditLogInputStream first = null;
    for (EditLogInputStream s : streams) {
      Preconditions.checkArgument(s.getFirstTxId() !=
          HdfsConstants.INVALID_TXID, "invalid first txid in stream: %s", s);
      if (!inProgressOk) { // extra checks for startup
        Preconditions.checkArgument(
            s.getLastTxId() != HdfsConstants.INVALID_TXID,
            "invalid last txid in stream: %s", s);
        Preconditions.checkArgument(!s.isInProgress(),
            "segment should not be inprogress: %s", s);
      }
      if (first == null) {
        first = s;
      } else {
        Preconditions.checkArgument(s.getFirstTxId() == first.getFirstTxId(),
          "All streams in the RedundantEditLogInputStream must have the same " +
          "start transaction ID!  " + first + " had start txId " +
          first.getFirstTxId() + ", but " + s + " had start txId " +
          s.getFirstTxId());
      }
    }

    this.streams = streams.toArray(new EditLogInputStream[streams.size()]);

    // We sort the streams here so that the streams that end later come first.
    Arrays.sort(this.streams, segmentComparator);
    LOG.info("Created stream: " + getName());
  }

  @Override
  public String getName() {
    StringBuilder bld = new StringBuilder();
    String prefix = "Stream set: ";
    for (EditLogInputStream elis : streams) {
      bld.append(prefix);
      bld.append(elis.getName());
      prefix = ", ";
    }
    bld.append(", current stream index: " + curIdx + ", current stream: "
        + streams[curIdx].getName());
    return bld.toString();
  }

  @Override
  public long getFirstTxId() {
    return streams[curIdx].getFirstTxId();
  }

  @Override
  public long getLastTxId() {
    return streams[curIdx].getLastTxId();
  }

  @Override
  public void close() throws IOException {
    IOUtils.cleanup(LOG, streams);
  }

  @Override
  public FSEditLogOp nextValidOp() {
    try {
      if (state == State.STREAM_FAILED) {
        state = State.STREAM_FAILED_RESYNC;
      }
      return nextOp();
    } catch (IOException e) {
      return null;
    }
  }

  @Override
  public FSEditLogOp nextOp() throws IOException {
    /*
     * States that the RedundantEditLogInputStream can be in.
     *
     * <pre>
     *                   start (if no streams)
     *                           |
     *                           V
     * PrematureEOFException  +----------------+
     *        +-------------->| EOF            |<--------------+
     *        |               +----------------+               |
     *        |                                                |
     *        |          start (if there are streams)          |
     *        |                  |                             |
     *        |                  V                             | EOF
     *        |   resync      +----------------+ skipUntil  +---------+
     *        |   +---------->| SKIP_UNTIL     |----------->|  OK     |
     *        |   |           +----------------+            +---------+
     *        |   |                | IOE   ^ fail over to      | IOE
     *        |   |                V       | next stream       |
     * +----------------------+   +----------------+           |
     * | STREAM_FAILED_RESYNC |   | STREAM_FAILED  |<----------+
     * +----------------------+   +----------------+
     *                  ^   Recovery mode    |
     *                  +--------------------+
     * </pre>
     */
    while (true) {
      switch (state) {
      case SKIP_UNTIL:
       try {
          if (prevTxId != HdfsConstants.INVALID_TXID) {
            LOG.info("Fast-forwarding stream '" + streams[curIdx].getName() +
                "' to transaction ID " + (prevTxId + 1));
            streams[curIdx].skipUntil(prevTxId + 1);
          }
          state = State.OK;
        } catch (IOException e) {
          prevException = e;
          state = State.STREAM_FAILED;
          if (LOG.isDebugEnabled()) {
            LOG.debug("Got exception and switch to STREAM_FAILED state. prevTxId:" + prevTxId, e);
          }
        }       
        break;
      case OK:
        try {
          FSEditLogOp op = streams[curIdx].readOp();
          if (op == null) {
            state = State.EOF;
            if (streams[curIdx].getLastTxId() == prevTxId) {
              if (curIdx + 1 < streams.length
                  && streams[curIdx + 1].getLastTxId() > streams[curIdx]
                      .getLastTxId()) {
                throw new IOException(
                    "Reached end of log segment, but we have longer segments available: "
                        + streams[curIdx + 1].getLastTxId() + " vs current: "
                        + streams[curIdx].getLastTxId());
              }
              return null;
            } else {
              throw new PrematureEOFException("got premature end-of-file " +
                  "at txid " + prevTxId + "; expected file to go up to " +
                  streams[curIdx].getLastTxId());
            }
          }
          prevTxId = op.getTransactionId();
          return op;
        } catch (IOException e) {
          prevException = e;
          state = State.STREAM_FAILED;
          if (LOG.isDebugEnabled()) {
            LOG.debug("Got exception and switch to STREAM_FAILED state. prevTxId:" + prevTxId, e);
          }
        }
        break;
      case STREAM_FAILED:
        if (curIdx + 1 == streams.length) {
          throw prevException;
        }
        long oldLast = streams[curIdx].getLastTxId();
        long newLast = streams[curIdx + 1].getLastTxId();
        if (newLast < oldLast) {
          throw new IOException("We encountered an error reading " +
              streams[curIdx].getName() + ".  During automatic edit log " +
              "failover, we noticed that all of the remaining edit log " +
              "streams are shorter than the current one!  The best " +
              "remaining edit log ends at transaction " +
              newLast + ", but we thought we could read up to transaction " +
              oldLast + ".  If you continue, metadata will be lost forever!");
        }
        LOG.error("Got error reading edit log input stream " +
          streams[curIdx].getName() + "; failing over to edit log " +
          streams[curIdx + 1].getName(), prevException);
        curIdx++;
        state = State.SKIP_UNTIL;
        break;
      case STREAM_FAILED_RESYNC:
        if (curIdx + 1 == streams.length) {
          if (prevException instanceof PrematureEOFException) {
            // bypass early EOF check
            state = State.EOF;
          } else {
            streams[curIdx].resync();
            state = State.SKIP_UNTIL;
          }
        } else {
          LOG.error("failing over to edit log " +
              streams[curIdx + 1].getName());
          curIdx++;
          state = State.SKIP_UNTIL;
        }
        break;
      case EOF:
        return null;
      }
    }
  }

  @Override
  public int getVersion() throws IOException {
    return streams[curIdx].getVersion();
  }

  @Override
  public long getPosition() throws IOException {
    return streams[curIdx].getPosition();
  }

  @Override
  public long length() throws IOException {
    return streams[curIdx].length();
  }

  /**
   * Exception thrown when edit log file does not span up to
   * expected transaction id.
   */
  public static final class PrematureEOFException extends IOException {
    private static final long serialVersionUID = 1L;
    PrematureEOFException(String msg) {
      super(msg);
    }
  }

  @Override
  public void position(long position) throws IOException {
    streams[curIdx].position(position);
  }

  @Override
  public void refresh(long position, long skippedUntilTxid) throws IOException {
    if (!inProgressOk) {
      // if we are not in tailing mode, we do not support refresh
      throw new IOException("Refresh cannot be called");
    }
    state = State.OK;
    for (EditLogInputStream elis : streams) {
      try {
        elis.refresh(position, skippedUntilTxid);
      } catch (Exception ex) {
        // ignore this, this is handled separately
      }
    }
    prevTxId = skippedUntilTxid;
    
    // re-sort the segments to read from the freshest one
    Arrays.sort(this.streams, segmentComparator);
    curIdx = 0;
    
    // log every now and then the state of the stream
    if (now() - lastRefreshLogTime > refreshLogInterval) {
      lastRefreshLogTime = now();
      LOG.info("Refresh stream: " + getName());
    }
  }
  
  @Override
  public boolean isInProgress() {
    return streams[curIdx].isInProgress();
  }

  @Override
  public long getReadChecksum() {
    return streams[curIdx].getReadChecksum();
  }
  
  public String toString() {
    return streams[curIdx].toString();
  }
  
  @Override
  public JournalType getType() {
    return streams[curIdx].getType();
  }
  
  private static long now() {
    return System.currentTimeMillis();
  }
}
