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

import static junit.framework.Assert.assertEquals;
import static org.junit.Assert.*;
import static org.apache.hadoop.hdfs.qjournal.QJMTestUtil.writeRandomSegment;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.qjournal.MiniJournalCluster;
import org.apache.hadoop.hdfs.qjournal.QJMTestUtil;
import org.apache.hadoop.hdfs.qjournal.protocol.JournalConfigKeys;
import org.apache.hadoop.hdfs.server.common.HdfsConstants.Transition;
import org.apache.hadoop.hdfs.server.namenode.EditLogInputStream;
import org.apache.hadoop.hdfs.server.namenode.EditLogOutputStream;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogTestUtil;
import org.apache.hadoop.io.IOUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Lists;

/**
 * Functional tests for QuorumJournalManager. For true unit tests, see
 * {@link TestQuorumJournalManagerUnit}.
 */
public class TestQJMWriteRead {
  private static final Log LOG = LogFactory
      .getLog(TestQuorumJournalManager.class);

  private MiniJournalCluster cluster;
  private Configuration conf;
  private QuorumJournalManager qjm;

  @Before
  public void setup() throws Exception {
    conf = new Configuration();
    // Don't retry connections - it just slows down the tests.
    conf.setInt("ipc.client.connect.max.retries", 0);
    conf.setLong(JournalConfigKeys.DFS_QJOURNAL_CONNECT_TIMEOUT_KEY, 100);

    cluster = new MiniJournalCluster.Builder(conf).build();

    qjm = MiniJournalCluster.createSpyingQJM(conf, cluster);
    qjm.transitionJournal(QJMTestUtil.FAKE_NSINFO, Transition.FORMAT, null);
    qjm.recoverUnfinalizedSegments();
    assertEquals(1, qjm.getLoggerSetForTests().getEpoch());
  }

  @After
  public void shutdown() throws IOException {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @Test
  public void testReaderWhileAnotherWritesFinalized() throws Exception {
    QuorumJournalManager readerQjm = MiniJournalCluster.createSpyingQJM(conf,
        cluster);
    List<EditLogInputStream> streams = Lists.newArrayList();
    readerQjm.selectInputStreams(streams, 0, false, true);
    assertEquals(0, streams.size());
    int numTxns = 10;

    List<FSEditLogOp> txns = new ArrayList<FSEditLogOp>();
    writeRandomSegment(cluster, qjm, 0, numTxns, true, txns);

    readerQjm.selectInputStreams(streams, 0, false, true);
    try {
      assertEquals(1, streams.size());
      // Validate the actual stream contents.
      EditLogInputStream stream = streams.get(0);
      assertEquals(0, stream.getFirstTxId());
      assertEquals(numTxns - 1, stream.getLastTxId());

      verifyEdits(streams, 0, numTxns - 1, txns, false);
      assertNull(stream.readOp());
    } finally {
      IOUtils.cleanup(LOG, streams.toArray(new Closeable[0]));
      streams.clear();
    }
  }

  @Test
  public void testReaderWhileAnotherWritesInProgress() throws Exception {
    QuorumJournalManager readerQjm = MiniJournalCluster.createSpyingQJM(conf,
        cluster);
    List<EditLogInputStream> streams = Lists.newArrayList();
    readerQjm.selectInputStreams(streams, 0, false, true);
    assertEquals(0, streams.size());
    int numTxns = 10;

    List<FSEditLogOp> txns = new ArrayList<FSEditLogOp>();
    writeRandomSegment(cluster, qjm, 0, numTxns, false, txns);

    // allow in-progress stream
    readerQjm.selectInputStreams(streams, 0, true, false);
    try {
      assertEquals(1, streams.size());
      // Validate the actual stream contents.
      EditLogInputStream stream = streams.get(0);
      assertEquals(0, stream.getFirstTxId());

      // for inprogress, we can read only up to second last one
      assertEquals(numTxns - 2, stream.getLastTxId());

      verifyEdits(streams, 0, numTxns - 1, txns, true);
      assertNull(stream.readOp());
    } finally {
      IOUtils.cleanup(LOG, streams.toArray(new Closeable[0]));
      streams.clear();
    }
  }

  // /////////////

  @Test
  public void testTailing() throws Exception {
    // Unlike the other unit test, numEdits here is constant as this is
    // a longer running test
    final int numEdits = 1000;
    final AtomicBoolean finishedProducing = new AtomicBoolean(false);
    final EditLogOutputStream out = qjm.startLogSegment(0);

    Callable<Void> producerThread = new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        try {
          for (int i = 0; i < numEdits; i++) {
            FSEditLogOp op = FSEditLogTestUtil.getNoOpInstance();
            // Set an increasing transaction id to verify correctness
            op.setTransactionId(i);
            if (LOG.isDebugEnabled()) {
              LOG.debug("Writing " + op);
            }

            FSEditLogTestUtil.writeToStreams(op, out);

            if (i % 50 == 0) {
              Thread.sleep(100);
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
        List<EditLogInputStream> streams = Lists.newArrayList();
        qjm.selectInputStreams(streams, 0, true, false);
        EditLogInputStream in = streams.get(0);

        long numOps = 0;
        long maxTxId = -1;
        FSEditLogOp op;
        long lastPos = in.getPosition();
        do {
          op = in.readOp();
          if (op == null) { // If we've reached the end prematurely...
            Thread.sleep(200);
            LOG.info("Refreshing to " + lastPos);

            in.refresh(lastPos, maxTxId); // Then refresh to last known good position
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

        // finalize the segment, so we can read to the end
        qjm.finalizeLogSegment(0, numEdits - 1);

        // Once producer is shutdown, scan again from last known good position
        // until the end of the ledger. This mirrors the Ingest logic (last
        // read when being quiesced).
        in.refresh(lastPos, maxTxId);
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
        assertEquals("Must end at txid = " + (numEdits - 1), numEdits - 1,
            maxTxId);
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

  // /////////////

  public static void verifyEdits(List<EditLogInputStream> streams,
      int firstTxnId, int lastTxnId, List<FSEditLogOp> writtenTxns,
      boolean inProgress) throws IOException {

    Iterator<EditLogInputStream> iter = streams.iterator();
    assertTrue(iter.hasNext());
    EditLogInputStream stream = iter.next();

    long position = stream.getPosition();
    if (inProgress) {
      // we are one transaction behind
      lastTxnId--;
    }

    for (int expected = firstTxnId; expected <= lastTxnId; expected++) {
      if (inProgress) { // otherwise we cannot call refresh
        stream.refresh(position, expected - 1);
      }
      FSEditLogOp op = stream.readOp();
      position = stream.getPosition();

      while (op == null) {
        assertTrue("Expected to find txid " + expected + ", "
            + "but no more streams available to read from", iter.hasNext());
        stream = iter.next();
        op = stream.readOp();
      }

      assertEquals(expected, op.getTransactionId());
      assertEquals(expected, writtenTxns.get(expected).getTransactionId());
      assertEquals(op.opCode, writtenTxns.get(expected).opCode);
    }

    assertNull(stream.readOp());
    assertFalse("Expected no more txns after " + lastTxnId
        + " but more streams are available", iter.hasNext());
  }
}
