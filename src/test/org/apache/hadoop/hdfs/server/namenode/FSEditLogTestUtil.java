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


import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMap;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.IOUtils;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;

import static org.junit.Assert.assertEquals;


/**
 * Utility class for testing {@link EditLogOutputStream} and
 * {@link EditLogInputStream} implementations.
 */
public class FSEditLogTestUtil {

  private static final Log LOG = LogFactory.getLog(FSEditLogTestUtil.class);

  /**
   * Base directory for temporary test data. Populated from the
   * "test.build.data" system property. Default value: <code>/tmp</code>
   */
  private static final String BASE_DATA_DIR =
      System.getProperty("test.build.data", "/tmp");

  private static final LogOpGenerator GENERATOR =
      new LogOpGenerator(1000, 3);

  public static List<FSEditLogOp> getContiguousLogSegment(
      int firstTxId, int lastTxId) {
    return GENERATOR.generateContiguousSegment(firstTxId, lastTxId);
  }

  public static FSEditLogOp getNoOpInstance() {
    return FSEditLogOp.LogSegmentOp.getInstance(
        FSEditLogOpCodes.OP_END_LOG_SEGMENT);
  }

  /**
   * Create a temporary file to use by EditLogFile{Input/Output}Stream which is
   * used to compare against BookKeeperEditLog{Input/Output}Stream
   */
  public static File createTempEditsFile(String testName)
      throws IOException {
    File ret = File.createTempFile(testName, "dat",
        new File(BASE_DATA_DIR));
    LOG.info("Created a temporary file for edits: " + ret.getAbsolutePath());
    return ret;
  }

  /**
   * Sequentially write the same edit log operation to multiple streams.
   * Abort immediately if there is write() method of the stream throws an
   * exception.
   * @param op The edit log operation to write.
   * @param streams The streams to write to.
   * @throws IOException If there is an error writing to any of the streams.
   */
  public static void writeToStreams(FSEditLogOp op,
      EditLogOutputStream... streams) throws IOException {
    for (EditLogOutputStream stream : streams) {
      stream.write(op);
    }
  }

  /**
   * Sequentially set streams ready to flush and then flush them.
   * Abort immediately if either setReadyToFlush() or flush() throws
   * an exception.
   * @param streams The streams to flush
   * @throws IOException If there is an error setting any of the streams
   *                     ready to flush or flushing any of the streams.
   */
  public static void flushStreams(EditLogOutputStream... streams)
      throws IOException {
    for (EditLogOutputStream stream : streams) {
      stream.setReadyToFlush();
      stream.flush();
    }
  }

  /**
   * Try to close multiple streams sequentially. Abort immediately if
   * closing any stream causes an exception
   * @param streams The streams to close
   * @throws IOException If there is an error closing any of the streams.
   */
  public static void closeStreams(EditLogOutputStream... streams)
    throws IOException {
    for (EditLogOutputStream stream : streams) {
      stream.close();
    }
  }

  /**
   * Write the header (using create method), generate a semi random workload
   * using {@link #getContiguousLogSegment(int, int)} and then flush and
   * close the streams.
   * @param firstTxId First transaction id in the workload
   * @param lastTxId Last transaction id in the workload
   * @param streams Streams to write to
   * @throws IOException
   */
  public static void createAndPopulateStreams(int firstTxId, int lastTxId,
      EditLogOutputStream... streams) throws IOException {
    for (EditLogOutputStream stream : streams) {
      stream.create();
    }
    populateStreams(firstTxId, lastTxId, streams);
  }

  public static void populateStreams(int firstTxId, int lastTxId,
      EditLogOutputStream... streams) throws IOException {
    // Semi-randomly generate contiguous transactions starting from firstTxId
    // to lastTxId (inclusive)
    List<FSEditLogOp> ops = getContiguousLogSegment(firstTxId, lastTxId);
    int txId = firstTxId;
    for (FSEditLogOp op : ops) {
      op.setTransactionId(txId++);
      writeToStreams(op, streams);
    }
    flushStreams(streams);
    closeStreams(streams);
  }

  /**
   * Assert that specified {@link EditLogInputStream} instances all contain
   * the same operations in the same order.
   * @param numEdits Number of edit log operations
   * @param streamByName Map of stream name to
   *                     {@link EditLogInputStream} instance
   */
  public static void assertStreamsAreEquivalent(int numEdits,
      Map<String, EditLogInputStream> streamByName) throws IOException {
    LOG.info(" --- Verifying " + numEdits + " edits from " +
        Joiner.on(',').join(streamByName.values()) + " ---");
    for (int i = 1; i <= numEdits; i++) {
      String prevName = null;
      FSEditLogOp prevLogOp = null;
      for (Map.Entry<String, EditLogInputStream> e : streamByName.entrySet()) {
        String name = e.getKey();
        EditLogInputStream stream = e.getValue();
        FSEditLogOp op = stream.readOp();
        if (LOG.isDebugEnabled()) {
          LOG.debug(
              Joiner.on(',').withKeyValueSeparator(" = ").join(
                  ImmutableMap.of("txId", i, "streamName", name, "op", op)));
        }
        if (prevLogOp != null) {
          assertEquals("Operation read from " + prevName + " and " +
              name + " must be equal!", op, prevLogOp);
        }
        prevLogOp = op;
        prevName = name;
      }
    }
  }

  /**
   * Find out how many transactions we can read from a
   * FileJournalManager, starting at a given transaction ID.
   *
   * @param jm              The journal manager
   * @param fromTxId        Transaction ID to start at
   * @param inProgressOk    Should we consider edit logs that are not finalized?
   * @return                The number of transactions
   * @throws IOException
   */
  public static long getNumberOfTransactions(JournalManager jm,
      long fromTxId,
      boolean inProgressOk,
      boolean abortOnGap) throws IOException {
    long numTransactions = 0, txId = fromTxId;
    final PriorityQueue<EditLogInputStream> allStreams =
        new PriorityQueue<EditLogInputStream>(64,
            JournalSet.EDIT_LOG_INPUT_STREAM_COMPARATOR);
    jm.selectInputStreams(allStreams, fromTxId, inProgressOk, true);
    EditLogInputStream elis = null;
    try {
      while ((elis = allStreams.poll()) != null) {
        elis.skipUntil(txId);
        while (true) {
          FSEditLogOp op = elis.readOp();
          if (op == null) {
            break;
          }
          if (abortOnGap && (op.getTransactionId() != txId)) {
            TestFileJournalManager.LOG.info("getNumberOfTransactions: detected gap at txId " +
                fromTxId);
            return numTransactions;
          }
          txId = op.getTransactionId() + 1;
          numTransactions++;
        }
      }
    } finally {
      IOUtils.cleanup(FSEditLogTestUtil.LOG,
          allStreams.toArray(new EditLogInputStream[0]));
      IOUtils.cleanup(FSEditLogTestUtil.LOG, elis);
    }
    return numTransactions;
  }

  public static EditLogInputStream getJournalInputStream(JournalManager jm,
      long txId, boolean inProgressOk) throws IOException {
    final PriorityQueue<EditLogInputStream> allStreams =
        new PriorityQueue<EditLogInputStream>(64,
            JournalSet.EDIT_LOG_INPUT_STREAM_COMPARATOR);
    jm.selectInputStreams(allStreams, txId, inProgressOk, true);
    EditLogInputStream elis = null, ret;
    try {
      while ((elis = allStreams.poll()) != null) {
        if (elis.getFirstTxId() > txId) {
          break;
        }
        if (elis.getLastTxId() < txId) {
          elis.close();
          continue;
        }
        elis.skipUntil(txId);
        ret = elis;
        elis = null;
        return ret;
      }
    } finally {
      IOUtils.cleanup(FSEditLogTestUtil.LOG,  allStreams.toArray(new EditLogInputStream[0]));
      IOUtils.cleanup(FSEditLogTestUtil.LOG,  elis);
    }
    return null;
  }
}
