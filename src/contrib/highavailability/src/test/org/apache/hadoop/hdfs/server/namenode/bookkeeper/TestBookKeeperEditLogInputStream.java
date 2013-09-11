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

import com.google.common.collect.ImmutableMap;
import junit.framework.AssertionFailedError;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hdfs.protocol.FSConstants;
import org.apache.hadoop.hdfs.server.namenode.EditLogFileInputStream;
import org.apache.hadoop.hdfs.server.namenode.EditLogFileOutputStream;
import org.apache.hadoop.hdfs.server.namenode.FSEditLog;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogTestUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertNull;

public class TestBookKeeperEditLogInputStream extends BookKeeperSetupUtil {

  private static final Log LOG =
      LogFactory.getLog(TestBookKeeperEditLogInputStream.class);

  // Mock BookkeeperJournalManager which implements openForReading() method
  // trivially, allowing BookKeeperEditLogInputStream to be tested before
  // BookKeeperJournalManager is fully implemented
  static class TestLedgerProvider implements LedgerHandleProvider {

    @Override
    public LedgerHandle openForReading(long ledgerId) throws IOException {
      return openLedger(ledgerId, false);
    }
  }

  private static LedgerHandleProvider ledgerProvider;

  private File tempEditsFile = null;
  private int origSizeFlushBuffer;

  @BeforeClass
  public static void setUpStatic() throws Exception {
    BookKeeperSetupUtil.setUpStatic();
    ledgerProvider = new TestLedgerProvider();
  }


  @Before
  public void setUp() throws Exception {
    super.setUp();

    // Remember the size of the flush buffer so that we can reset it
    // in tearDown() in case any of the tests alter it
    origSizeFlushBuffer = FSEditLog.sizeFlushBuffer;

    // Create a temp file to use for comparison of BookKeeper based
    // input stream and the file based input stream
    tempEditsFile = FSEditLogTestUtil.createTempEditsFile(
        "testBookKeeperEditLogInputStream");
  }

  @After
  public void tearDown() throws Exception {
    try {
      super.tearDown();
      if (tempEditsFile != null) {
        if (!tempEditsFile.delete()) {
          LOG.warn("Unable to delete temporary edits file: " +
              tempEditsFile.getAbsolutePath());
        }
      }
    } finally {
      // Reset sizeFlushBuffer between each unit test (in case it has been
      // altered by a unit test to trigger a boundary condition)
      int lastSizeFlushBuffer = FSEditLog.sizeFlushBuffer;
      FSEditLog.sizeFlushBuffer = origSizeFlushBuffer;

      if (lastSizeFlushBuffer != origSizeFlushBuffer) {
        LOG.info("Setting FSEditLog.sizeFlushBuffer back to " +
            origSizeFlushBuffer + " was " + lastSizeFlushBuffer +
            " after last test.");
      }
    }
  }

  private static int[] genRandomNumEdits() {
    int count = 5;
    int maxNumEdits = 1000;
    int ret[] = new int[count];
    Random rng = new Random();
    int prev = 1;
    for (int i = 0; i < count; i++) {
      int curr = prev + rng.nextInt(maxNumEdits);
      ret[i] = prev = curr;
    }
    return ret;
  }

  /**
   * Writes a workload to both a BookKeeperEditLogOutputStream and a
   * file EditLogFileOutputStream, reads the workload in from both using
   * (respectively) BookKeeperEditLogInputStream and EditLogFileInputStream
   * and verifies that the exact same data is read from both.
   */
  @Test
  public void testReadFromClosedLedgerAfterWrite() throws Exception {
    int randomNumEdits[] = genRandomNumEdits();
    for (int numEdits : randomNumEdits) {
      LOG.info("--- Running testReadFromClosedLedgerAfterWrite with " +
          numEdits + " edits ---");
      testReadFromClosedLedgerAfterWriteInner(numEdits);
    }
  }

  private void testReadFromClosedLedgerAfterWriteInner(int numEdits)
    throws Exception {
    LedgerHandle ledgerOut = createLedger();
    long ledgerId = ledgerOut.getId();
    BookKeeperEditLogOutputStream bkEditsOut =
        new BookKeeperEditLogOutputStream(ledgerOut);
    EditLogFileOutputStream fileEditsOut =
        new EditLogFileOutputStream(tempEditsFile, null);

    FSEditLogTestUtil.createAndPopulateStreams(1,
        numEdits, bkEditsOut, fileEditsOut);

    BookKeeperEditLogInputStream bkEditsIn =
        new BookKeeperEditLogInputStream(ledgerProvider,
            ledgerId,
            0,
            1,
            numEdits,
            false);
    EditLogFileInputStream fileEditsIn =
        new EditLogFileInputStream(tempEditsFile);

    assertEquals("Length in bytes must be equal!",
        bkEditsIn.length(), fileEditsIn.length());

    FSEditLogTestUtil.assertStreamsAreEquivalent(numEdits,
        ImmutableMap.of("BookKeeper", bkEditsIn, "File", fileEditsIn));
    assertNull("BookKeeper edit log must end at txid 100", bkEditsIn.readOp());
  }

  /**
   * Refreshes after each read and then verifies that we can still accurately
   * read the next transaction.
   */
  @Test
  @Ignore
  public void testReadAndRefreshAfterEachTransaction() throws Exception {
    int randomNumEdits[] = genRandomNumEdits();
    for (int numEdits : randomNumEdits) {
      LOG.info("--- Running testReadAndRefreshAfterEachTransaction with " +
          numEdits + " edits ---");
      testReadAndRefreshAfterEachTransactionInner(numEdits);
    }
  }

  private void testReadAndRefreshAfterEachTransactionInner(int numEdits)
    throws Exception {
    FSEditLog.sizeFlushBuffer = 100;
    LedgerHandle ledgerOut = createLedger();
    long ledgerId = ledgerOut.getId();
    BookKeeperEditLogOutputStream bkEditsOut =
        new BookKeeperEditLogOutputStream(ledgerOut);
    EditLogFileOutputStream fileEditsOut =
        new EditLogFileOutputStream(tempEditsFile, null);

    FSEditLogTestUtil.createAndPopulateStreams(1,
        numEdits, bkEditsOut, fileEditsOut);

    BookKeeperEditLogInputStream bkEditsIn =
        new BookKeeperEditLogInputStream(ledgerProvider,
            ledgerId,
            0,
            1,
            numEdits,
            false);

    EditLogFileInputStream fileEditsIn =
        new EditLogFileInputStream(tempEditsFile);

    assertEquals("Length in bytes must be equal!",
        bkEditsIn.length(), fileEditsIn.length());

    long lastBkPos = bkEditsIn.getPosition();
    long lastFilePos = fileEditsIn.getPosition();
    for (int i = 1; i <= numEdits; i++) {
      assertEquals("Position in file must be equal position in bk",
          lastBkPos, lastFilePos);
      bkEditsIn.refresh(lastBkPos, -1);
      fileEditsIn.refresh(lastFilePos, -1);
      FSEditLogOp opFromBk = bkEditsIn.readOp();
      FSEditLogOp opFromFile = fileEditsIn.readOp();
      if (LOG.isDebugEnabled()) {
        LOG.debug("txId = " + i + ", " + "opFromBk = " + opFromBk +
            ", opFromFile = " + opFromFile);
      }
      assertEquals(
          "Operation read from file and BookKeeper must be same after refresh",
          opFromBk, opFromFile);
      lastBkPos = bkEditsIn.getPosition();
      lastFilePos = fileEditsIn.getPosition();
    }
    assertNull("BookKeeper edit log must end at last txId", bkEditsIn.readOp());
  }

  /**
   * After each read, save the last known position, re-create the object,
   * and then refresh to the saved position.
   */
  @Test
  @Ignore
  public void testRefreshInAReCreatedInstance() throws Exception {
    int numEdits = 100;
    FSEditLog.sizeFlushBuffer = 100;

    LedgerHandle ledgerOut = createLedger();
    long ledgerId = ledgerOut.getId();
    BookKeeperEditLogOutputStream bkEditsOut =
        new BookKeeperEditLogOutputStream(ledgerOut);
    EditLogFileOutputStream fileEditsOut =
        new EditLogFileOutputStream(tempEditsFile, null);

    FSEditLogTestUtil.createAndPopulateStreams(1,
        numEdits, bkEditsOut, fileEditsOut);

    BookKeeperEditLogInputStream bkEditsIn =
        new BookKeeperEditLogInputStream(ledgerProvider,
            ledgerId,
            0,
            1,
            numEdits,
            false);
    EditLogFileInputStream fileEditsIn =
        new EditLogFileInputStream(tempEditsFile);
    long lastBkPos = bkEditsIn.getPosition();
    long lastFilePos = fileEditsIn.getPosition();
    for (int i = 0; i <= numEdits; i++) {
      assertEquals("Position in file must be equal to position in bk",
          lastBkPos, lastFilePos);
      bkEditsIn.refresh(lastBkPos, -1);
      fileEditsIn.refresh(lastFilePos, -1);
      FSEditLogOp opFromBk = bkEditsIn.readOp();
      FSEditLogOp opFromFile = fileEditsIn.readOp();
      if (LOG.isDebugEnabled()) {
        LOG.debug("txId = " + i + ", " + "opFromBk = " + opFromBk +
            ", opFromFile = " + opFromFile);
      }
      assertEquals(
          "Operation read from file and BookKeeper must be same after refresh",
          opFromBk, opFromFile);
      lastBkPos = bkEditsIn.getPosition();
      lastFilePos = fileEditsIn.getPosition();
      bkEditsIn =
          new BookKeeperEditLogInputStream(ledgerProvider,
              ledgerId,
              0,
              1,
              numEdits,
              false);
      fileEditsIn =
          new EditLogFileInputStream(tempEditsFile);
    }
    bkEditsIn.refresh(lastBkPos, -1);
    assertNull("BookKeeper edit log must end at last txId", bkEditsIn.readOp());
  }

  /**
   * By default, BufferedInputStream (as used by DataInputStream which is used
   * by BookKeeperEditLogInputStream) reads 2048 bytes at a time. Verify that
   * we are able to read and refresh a stream even if the flush buffer
   * is smaller (i.e., each read from BookKeeper ledger spans multiple entries)
   */
  @Test
  public void testReadBufferGreaterThanLedgerEntrySize() throws Exception {
    int randomNumEdits[] = genRandomNumEdits();
    for (int numEdits : randomNumEdits) {
      LOG.info("--- Running testReadBufferGreaterThanLedgerEntrySize with " +
          numEdits + " edits ---");
      testReadBufferGreaterThanLedgerSizeInner(numEdits);
    }
  }

  private void testReadBufferGreaterThanLedgerSizeInner(int numEdits)
      throws Exception {
    LedgerHandle ledgerOut = createLedger();
    long ledgerId = ledgerOut.getId();
    BookKeeperEditLogInputStream bkEditsIn =
        new BookKeeperEditLogInputStream(ledgerProvider,
            ledgerId,
            0,
            1,
            -1,
            true);
    EditLogFileOutputStream fileEditsOut =
        new EditLogFileOutputStream(tempEditsFile, null);
    bkEditsIn.init();
    // Set the edit log buffer flush size smaller than the size of
    // of the buffer in BufferedInputStream in BookKeeperJournalInputStream
    FSEditLog.sizeFlushBuffer = bkEditsIn.bin.available() / 3;
    LOG.info("Set flush buffer size to " + FSEditLog.sizeFlushBuffer);

    BookKeeperEditLogOutputStream bkEditsOut =
        new BookKeeperEditLogOutputStream(ledgerOut);

    FSEditLogTestUtil.createAndPopulateStreams(1, numEdits, bkEditsOut,
        fileEditsOut);

    // Re-try refreshing up to ten times until we are able to refresh
    // successfully to be beginning of the ledger and read the edit log
    // layout version
    int maxTries = 10;
    for (int i = 0; i < maxTries; i++) {
      try {
        bkEditsIn.refresh(0, -1);
        assertEquals("refresh succeeded", bkEditsIn.logVersion,
            FSConstants.LAYOUT_VERSION);
      } catch (AssertionFailedError e) {
        if (i == maxTries) {
          // Fail the unit test rethrowing the assertion failure if we've
          // reached the maximum number of retries
          throw e;
        }
      }
    }
    EditLogFileInputStream fileEditsIn =
        new EditLogFileInputStream(tempEditsFile);
    for (int i = 0; i <= numEdits; i++) {
      FSEditLogOp opFromBk = bkEditsIn.readOp();
      FSEditLogOp opFromFile = fileEditsIn.readOp();
      if (LOG.isDebugEnabled()) {
        LOG.debug("txId = " + i + ", " + "opFromBk = " + opFromBk +
            ", opFromFile = " + opFromFile);
      }
      assertEquals(
          "Operation read from file and BookKeeper must be same after refresh",
          opFromBk, opFromFile);
    }
    assertNull("BookKeeper edit log must end at txid 1000", bkEditsIn.readOp());
  }

  /**
   * Test "tailing" an in-progress ledger that is later finalized, i.e., a
   * typical primary/standby high-availability scenario.
   * Spawns two threads: a consumer (which writes transactions with
   * monotonically increasing transaction ids to the log), and consumer
   * (which keeps reading from the log, refreshing when encountering the end of
   * the log until the producer is shut down). Verifies that transactions have
   * been written in the correct order.
   */
  @Test
  public void testTailing() throws Exception {
    // Unlike the other unit test, numEdits here is constant as this is
    // a longer running test
    final int numEdits = 10000;
    final AtomicBoolean finishedProducing = new AtomicBoolean(false);
    final LedgerHandle ledgerOut = createLedger();
    final long ledgerId = ledgerOut.getId();

    Callable<Void> producerThread = new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        try {
          BookKeeperEditLogOutputStream out =
              new BookKeeperEditLogOutputStream(ledgerOut);
          out.create(); // Writes version to the ledger output stream

          for (int i = 1; i <= numEdits; i++) {
            FSEditLogOp op = FSEditLogTestUtil.getNoOpInstance();
            // Set an increasing transaction id to verify correctness
            op.setTransactionId(i);
            if (LOG.isDebugEnabled()) {
              LOG.debug("Writing " + op);
            }

            FSEditLogTestUtil.writeToStreams(op, out);

            if (i % 1000 == 0) {
              Thread.sleep(500);
              FSEditLogTestUtil.flushStreams(out);
            }
          }
          FSEditLogTestUtil.flushStreams(out);
          FSEditLogTestUtil.closeStreams(out);
        } finally {
          // Let the producer know that we've reached the end.
          finishedProducing.set(true);
        }
        return null;
      }
    };
    Callable<Void> consumerThread = new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        BookKeeperEditLogInputStream in =
            new BookKeeperEditLogInputStream(ledgerProvider,
                ledgerId,
                0,
                1,
                -1,
                true);

        long numOps = 0;
        long maxTxId = -1;
        FSEditLogOp op;
        long lastPos = in.getPosition();
        do {
          op = in.readOp();
          if (op == null) { // If we've reached the end prematurely...
            Thread.sleep(1000);
            LOG.info("Refreshing to " + lastPos);

            in.refresh(lastPos, -1); // Then refresh to last known good position
          } else {
            long txId = op.getTransactionId();
            if (txId > maxTxId) {
              // Standby ingest contains similar logic: transactions
              // with ids lower than what is already read are ignored.
              numOps++;
              maxTxId = txId;
            }

            // Remember the last known safe position that we can refresh to
            lastPos = in.getPosition();
          }
        } while (op != null || !finishedProducing.get());
        Thread.sleep(1000);

        // Once producer is shutdown, scan again from last known good position
        // until the end of the ledger. This mirrors the Ingest logic (last
        // read when being quiesced).
        in.refresh(lastPos, -1);
        do {
          op = in.readOp();
          if (op != null) {
            long txId = op.getTransactionId();
            if (txId > maxTxId) {
              numOps++;
              maxTxId = txId;
            }
          }
        } while (op != null);

        assertEquals("Must have read " + numEdits + " edits", numEdits, numOps);
        assertEquals("Must end at txid = " + numEdits, maxTxId, numEdits);
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
  }
}
