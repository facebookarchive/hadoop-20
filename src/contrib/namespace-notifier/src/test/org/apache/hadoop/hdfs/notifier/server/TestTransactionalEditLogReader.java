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
package org.apache.hadoop.hdfs.notifier.server;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ChecksumException;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hdfs.notifier.NamespaceNotification;
import org.apache.hadoop.hdfs.notifier.NotifierConfig;
import org.apache.hadoop.hdfs.server.common.Storage.StorageDirectory;
import org.apache.hadoop.hdfs.server.common.StorageInfo;
import org.apache.hadoop.hdfs.server.common.Util;
import org.apache.hadoop.hdfs.server.namenode.EditLogOutputStream;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp.LogSegmentOp;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOpCodes;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogTestUtil;
import org.apache.hadoop.hdfs.server.namenode.FileJournalManager;
import org.apache.hadoop.hdfs.server.namenode.NNStorage;
import org.apache.hadoop.hdfs.util.InjectionEvent;
import org.apache.hadoop.util.InjectionEventI;
import org.apache.hadoop.util.InjectionHandler;
import org.junit.Test;

public class TestTransactionalEditLogReader {

  private static final Log LOG = LogFactory
      .getLog(TestTransactionalEditLogReader.class);
  private Random r = new Random();

  private File base_dir = new File(System.getProperty("test.build.data",
      "build/test/data"));

  private File editsDir;
  private StorageDirectory storageDirectory;

  private void setUp(String name) throws IOException {
    LOG.info("------ TEST start : " + name);
    FileUtil.fullyDelete(base_dir);
    editsDir = new File(base_dir, "edits");
    storageDirectory = new NNStorage(new StorageInfo(-41, 0, 0)).new StorageDirectory(
        editsDir);
    format(storageDirectory);
  }

  private void format(StorageDirectory sd) throws IOException {
    sd.clearDirectory(); // create currrent dir
    sd.write();
    LOG.info("Storage directory " + sd.getRoot()
        + " has been successfully formatted.");
  }

  private void assertTransactions(List<FSEditLogOp> written,
      List<FSEditLogOp> read) {
    assertEquals(written.size(), read.size());
    for (int i = 0; i < written.size(); i++) {
      assertEquals(written.get(i).getClass(), read.get(i).getClass());
    }
  }

  /**
   * Simple test of well behaved producer consumer.
   */
  @Test
  public void testSimple() throws Exception {
    for (int i = 0; i < 3; i++) {
      // failures - no
      // ending log segment - yes
      runTest(r.nextInt(20) + 1, 10, false, true);
    }
  }

  /**
   * Test unclean shutdown of editlog segment.\ No failures are injected here.
   */
  @Test
  public void testUnclean() throws Exception {
    for (int i = 0; i < 3; i++) {
      // failures - no
      // ending log segment - no
      runTest(r.nextInt(20) + 1, 5, false, false);
    }
  }

  /**
   * Test failures when reading transactions. IOExceptions, ChecksumExceptions,
   * unchecked exceptions.
   */
  @Test
  public void testFailures() throws Exception {
    for (int i = 0; i < 3; i++) {
      // failures - yes
      // ending log segment - yes
      runTest(r.nextInt(20) + 1, 5, true, true);
    }
  }

  /**
   * Test read failure, together with unclean log finalizations.
   */
  @Test
  public void testMess() throws Exception {
    for (int i = 0; i < 3; i++) {
      // failures - yes
      // ending log segment - no
      runTest(r.nextInt(20) + 1, 5, true, true);
    }
  }

  // /////////////////////////////////////////////////////

  private void runTest(final int editsPerFile, final int numSegments,
      final boolean failures, final boolean endLogSegment) throws Exception {
    setUp("testSimple - editsPerFile: " + editsPerFile + ", numSegments: "
        + numSegments + ", failures: " + failures + ", endLogSegment: "
        + endLogSegment);
    TestTransactionalEditLogReaderHandler h = new TestTransactionalEditLogReaderHandler(
        failures);
    InjectionHandler.set(h);

    final List<FSEditLogOp> writtenTransactions = new ArrayList<FSEditLogOp>();
    final int retries = 3;

    Callable<Void> producerThread = new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        int currentTxId = 0;
        try {
          FileJournalManager jm = new FileJournalManager(storageDirectory);
          for (int segment = 0; segment < numSegments; segment++) {
            // start new segment
            int firstTxId = currentTxId;
            EditLogOutputStream out = jm.startLogSegment(currentTxId);

            // starting transaction
            FSEditLogOp startOp = new LogSegmentOp(
                FSEditLogOpCodes.OP_START_LOG_SEGMENT);
            startOp.setTransactionId(firstTxId);
            FSEditLogTestUtil.writeToStreams(startOp, out);
            LOG.info("Written op: " + startOp);
            writtenTransactions.add(startOp);

            currentTxId++;

            // other transactions
            List<FSEditLogOp> transactions = FSEditLogTestUtil
                .getContiguousLogSegment(currentTxId, currentTxId
                    + editsPerFile);

            for (int i = 0; i < editsPerFile; i++) {
              FSEditLogOp op = transactions.get(i);
              op.setTransactionId(currentTxId);
              FSEditLogTestUtil.writeToStreams(op, out);
              writtenTransactions.add(op);
              currentTxId++;
              LOG.info("Written op: " + op);
              if (i % 100 == 0) {
                Thread.sleep(10);
                FSEditLogTestUtil.flushStreams(out);
              }
            }

            // write ending transactions if needed
            if (endLogSegment || (segment == numSegments - 1)) {
              int lastTxId = currentTxId;
              FSEditLogOp endOp = new LogSegmentOp(
                  FSEditLogOpCodes.OP_END_LOG_SEGMENT);
              endOp.setTransactionId(lastTxId);
              FSEditLogTestUtil.writeToStreams(endOp, out);
              LOG.info("Written op: " + endOp);
              writtenTransactions.add(endOp);
              currentTxId++;
            }

            FSEditLogTestUtil.flushStreams(out);
            FSEditLogTestUtil.closeStreams(out);
            jm.finalizeLogSegment(firstTxId, currentTxId - 1);

            // simulate NFS cache delay (reader won't see
            // the new file for 1 second
            if (r.nextBoolean())
              ;
            Thread.sleep(100);
          }
        } finally {
          LOG.info("- DONE -");
        }
        return null;
      }
    };
    Callable<Void> consumerThread = new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        // let the producer progress

        Configuration conf = new Configuration();
        // only this number of retries
        conf.setInt(NotifierConfig.LOG_READER_STREAM_RETRIES, retries);
        // will re-check after 1000ms second for new log segments
        conf.setLong(NotifierConfig.LOG_READER_STREAM_TIMEOUT, 100);
        // will sleep for 100ms after exceptions
        conf.setLong(NotifierConfig.LOG_READER_STREAM_ERROR_SLEEP, 100);
        
        EmptyServerCore core = new EmptyServerCore();
        core.setConfiguration(conf);

        ServerLogReaderTransactional logReader = new ServerLogReaderTransactional(
            core, Util.fileAsURI(editsDir));

        try {
          while (true) {
            NamespaceNotification n = logReader.getNamespaceNotification();
            LOG.info("Got notification: " + n);
          }
        } catch (IOException e) {
          // this will finally fail, because we close last segment
          LOG.info("Exception: " + e.getMessage(), e);
        }
        return null;
      }
    };
    // Allow producer and consumer to run concurrently
    ExecutorService executor = Executors.newFixedThreadPool(2);
    Future<Void> producerFuture = executor.submit(producerThread);
    Future<Void> consumerFuture = executor.submit(consumerThread);

    // Calling a .get() on the future will rethrow any exceptions thrown in
    // the future.
    producerFuture.get();
    consumerFuture.get();

    assertTransactions(writtenTransactions, h.readTransactions);
  }

  class TestTransactionalEditLogReaderHandler extends InjectionHandler {

    List<FSEditLogOp> readTransactions = new ArrayList<FSEditLogOp>();
    boolean failures = false;
    int simulatedFailure = -1;

    void clenup() {
      readTransactions.clear();
    }

    TestTransactionalEditLogReaderHandler(boolean failures) {
      this.failures = failures;
    }

    @Override
    protected void _processEvent(InjectionEventI event, Object... args) {
      if (event == InjectionEvent.SERVERLOGREADER_UPDATE) {
        FSEditLogOp op = (FSEditLogOp) args[0];
        LOG.info("read transaction: " + op.getTransactionId());
        readTransactions.add(op);
      }
    }

    @Override
    protected void _processEventIO(InjectionEventI event, Object... args)
        throws IOException {
      if (event == InjectionEvent.SERVERLOGREADER_READOP && failures) {
        simulatedFailure++;
        if ((simulatedFailure % 3) == 1) {
          LOG.info("Throwing checksum exception");
          throw new ChecksumException("Testing checksum exception...", 0);
        }
        if ((simulatedFailure % 7) == 1) {
          LOG.info("Throwing unchecked exception");
          throw new ArrayIndexOutOfBoundsException(
              "Testing uncecked exception...");
        }
        if ((simulatedFailure % 11) == 1) {
          LOG.info("Throwing IO exception");
          throw new IOException("Testing IO exception...");
        }
      }
    }
  }
}
