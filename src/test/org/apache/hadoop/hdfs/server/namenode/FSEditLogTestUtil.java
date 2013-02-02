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


import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.File;
import java.io.IOException;
import java.util.List;

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

}
