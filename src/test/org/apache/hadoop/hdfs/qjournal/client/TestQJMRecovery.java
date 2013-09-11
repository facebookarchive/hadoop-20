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
import static org.apache.hadoop.hdfs.qjournal.QJMTestUtil.JID;
import static org.apache.hadoop.hdfs.qjournal.QJMTestUtil.FAKE_NSINFO;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.qjournal.MiniJournalCluster;
import org.apache.hadoop.hdfs.qjournal.QJMTestUtil;
import org.apache.hadoop.hdfs.qjournal.protocol.JournalConfigKeys;
import org.apache.hadoop.hdfs.qjournal.server.GetJournalManifestServlet;
import org.apache.hadoop.hdfs.qjournal.server.JournalNodeJournalSyncer;
import org.apache.hadoop.hdfs.server.common.HdfsConstants.Transition;
import org.apache.hadoop.hdfs.server.namenode.EditLogOutputStream;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogTestUtil;
import org.apache.hadoop.hdfs.server.namenode.FileJournalManager.EditLogFile;
import org.apache.hadoop.hdfs.server.namenode.NNStorage;
import org.apache.hadoop.hdfs.server.namenode.NNStorage.NameNodeFile;
import org.apache.hadoop.hdfs.server.protocol.NamespaceInfo;
import org.apache.hadoop.hdfs.util.InjectionEvent;
import org.apache.hadoop.util.InjectionEventI;
import org.apache.hadoop.util.InjectionHandler;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import com.google.common.util.concurrent.MoreExecutors;

/**
 * Functional tests for QuorumJournalManager. For true unit tests, see
 * {@link TestQuorumJournalManagerUnit}.
 */
public class TestQJMRecovery {
  private static final Log LOG = LogFactory
      .getLog(TestQuorumJournalManager.class);

  private MiniJournalCluster cluster;
  private Configuration conf;
  private QuorumJournalManager qjm;
  private final int NUM_JOURNALS = 3;
  private final Random r = new Random();

  @Before
  public void setup() throws Exception {
    conf = new Configuration();
    // Don't retry connections - it just slows down the tests.
    conf.setInt("ipc.client.connect.max.retries", 0);
    conf.setLong(JournalConfigKeys.DFS_QJOURNAL_CONNECT_TIMEOUT_KEY, 100);

    cluster = new MiniJournalCluster.Builder(conf)
        .numJournalNodes(NUM_JOURNALS).build();
    qjm = MiniJournalCluster.createSpyingQJM(conf, cluster);
    qjm.transitionJournal(FAKE_NSINFO, Transition.FORMAT, null);
  }

  @After
  public void shutdown() throws IOException {
    if (cluster != null) {
      cluster.shutdown();
    }
    InjectionHandler.clear();
  }

  @Test(timeout=30 * 1000)
  public void testManifest() throws IOException {
    final int numSegments = 20;
    final int txnsPerSegment = 10;

    // take over as the writer
    qjm.recoverUnfinalizedSegments();
    List<FSEditLogOp> txns = new ArrayList<FSEditLogOp>();
    List<SegmentDescriptor> segments = new ArrayList<SegmentDescriptor>();

    for (int i = 0; i < numSegments; i++) {
      long startTxId = i * txnsPerSegment;
      long endTxId = startTxId + txnsPerSegment - 1;
      boolean finalize = r.nextBoolean();

      EditLogOutputStream stm = QJMTestUtil.writeRandomSegment(cluster, qjm, i
          * txnsPerSegment, (txnsPerSegment), finalize, txns);
      SegmentDescriptor sd = new SegmentDescriptor(-1, startTxId,
          finalize ? endTxId : -1, false);
      segments.add(sd);

      // validate manifest for all segments
      validateSegmentManifest(segments, cluster);

      if (!finalize) {
        stm.flush();
        stm.close();
        qjm.finalizeLogSegment(startTxId, endTxId);
        sd.endTxId = endTxId;
      }
      // revalidate after closing
      validateSegmentManifest(segments, cluster);
    }
  }

  /**
   * This test the recovery when a segment fails when it's in progress.
   */
  @Test(timeout = 30 * 1000)
  public void testRecoveryInSegment() throws Exception {
    testRecovery(false);
  }

  /**
   * This test the recovery when a segment is entirely absent. The journal node
   * can be down for multiple rollEditLog.
   */
  @Test(timeout = 30 * 1000)
  public void testRecoveryWithMissingSegments() throws Exception {
    testRecovery(true);
  }

  private void testRecovery(boolean failAtStart) throws Exception {
    TestQJMRecoveryInjectionHandler h = new TestQJMRecoveryInjectionHandler();
    InjectionHandler.set(h);

    // keep track of all expected segments
    Map<Integer, List<SegmentDescriptor>> expectedSegments = new HashMap<Integer, List<SegmentDescriptor>>();
    for (int i = 0; i < NUM_JOURNALS; i++) {
      expectedSegments.put(i, new ArrayList<SegmentDescriptor>());
    }

    final int rollEvryTxns = 5;
    final int numEdits = rollEvryTxns * 20;

    EditLogOutputStream out = null;
    long currentSegmentTxid = 0;
    qjm.recoverUnfinalizedSegments();
    int numRecoveriesToExpect = 0;

    int previouslyFailed = 0;

    // at this point no segment is started
    for (int i = 0; i < numEdits; i++) {

      // roll segment every now and then
      if (i % rollEvryTxns == 0) {
        // choose journal node to fail
        // and a random transaction in this segment to fail at write, or at
        // start log segment

        // in addition, randomly, fail the same journal twice in the row to
        // produce holes
        int journalToFail = r.nextBoolean() ? previouslyFailed : r
            .nextInt(NUM_JOURNALS);

        // set the mode
        h.resetFailureMode(journalToFail, r.nextInt(rollEvryTxns), failAtStart);

        // if recovery works fine, all segments should be available at all
        // journal nodes
        addExpectedFinalizedSegments(NUM_JOURNALS, i, i + rollEvryTxns - 1,
            expectedSegments);

        // we will have extra corrupted segments at the failed node
        // only when segment fails after being successfully created
        if (!failAtStart) {
          expectedSegments.get(h.journalToFail).add(
              new SegmentDescriptor(h.journalToFail, i, -1, true));
        }
        numRecoveriesToExpect++;

        // start log segment
        // this recovers previously failed journals
        out = qjm.startLogSegment(i);
        currentSegmentTxid = i;
      }

      // otherwise write a random operation
      FSEditLogOp op = FSEditLogTestUtil.getNoOpInstance();
      // Set an increasing transaction id to verify correctness
      op.setTransactionId(i);
      FSEditLogTestUtil.writeToStreams(op, out);

      // sync every time to ensure we send transaction by transaction
      FSEditLogTestUtil.flushStreams(out);

      if (i == currentSegmentTxid + rollEvryTxns - 1) {
        // end log segment
        FSEditLogTestUtil.flushStreams(out);
        qjm.finalizeLogSegment(currentSegmentTxid, i);
      }
    }

    // do not fail any of the journals
    h.resetFailureMode(-1, -1, false);

    // start one more segment to trigger recovery
    out = qjm.startLogSegment(numEdits);
    addExpectedFinalizedSegments(NUM_JOURNALS, numEdits, -1, expectedSegments);

    // wait untill all tasks have completed
    h.waitNumRecoveries(numRecoveriesToExpect);

    // validate expected contents of each storage directory
    validateJournalStorage(expectedSegments, cluster);

    FSEditLogTestUtil.flushStreams(out);
    FSEditLogTestUtil.closeStreams(out);

  }

  /**
   * Helper to keep track of all edit log segments
   */
  class SegmentDescriptor {
    int jid;
    long startTxId;
    long endTxId;
    boolean corrupt;

    SegmentDescriptor(int jid, long startTxId, long endTxId, boolean corrupt) {
      this.jid = jid;
      this.startTxId = startTxId;
      this.endTxId = endTxId;
      this.corrupt = corrupt;
    }

    public String toString() {
      return "[" + jid + "," + startTxId + "," + endTxId + "," + corrupt
          + "] name: " + getName();
    }

    String getName() {
      String suffix = corrupt ? ".corrupt" : "";
      String name = (endTxId == -1) ? NNStorage
          .getInProgressEditsFileName(startTxId) : NNStorage
          .getFinalizedEditsFileName(startTxId, endTxId);
      return name + suffix;
    }
  }

  /**
   * Adds correct segments to the list of expected segments
   */
  private void addExpectedFinalizedSegments(int numJournals, long startTxId,
      long endTxId, Map<Integer, List<SegmentDescriptor>> segments) {
    for (int i = 0; i < numJournals; i++) {
      segments.get(i).add(new SegmentDescriptor(i, startTxId, endTxId, false));
    }
  }

  /**
   * Validate the contents of journal node storage.
   */
  private void validateJournalStorage(
      Map<Integer, List<SegmentDescriptor>> segments, MiniJournalCluster cluster) {
    for (Integer jnid : segments.keySet()) {
      // for each journal node

      // these are the expected segments (including failed ones)
      List<SegmentDescriptor> jnExpectedSegments = segments.get(jnid);

      // they should be under this directory
      File currentDir = cluster.getJournalCurrentDir(jnid, JID);
      assertTrue(currentDir.exists());

      // get all files
      File[] files = currentDir.listFiles();
      Set<String> editsFiles = new HashSet<String>();

      // filter edits files
      for (File f : files) {
        if (f.getName().startsWith(NameNodeFile.EDITS.getName())) {
          editsFiles.add(f.getName());
        }
      }

      if (LOG.isDebugEnabled()) {
        LOG.debug("Validating storage for journalnode: " + jnid
            + ", available edit segments: " + editsFiles
            + ", expected segments: " + jnExpectedSegments);
      }

      // assert that we do have what we should
      assertEquals(jnExpectedSegments.size(), editsFiles.size());
      for (SegmentDescriptor sd : jnExpectedSegments) {
        assertTrue(editsFiles.contains(sd.getName()));
      }
    }
  }

  EditLogFile findFile(long startTxId, List<EditLogFile> files) {
    for (EditLogFile elf : files) {
      if (elf.getFirstTxId() == startTxId) {
        return elf;
      }
    }
    return null;
  }

  /**
   * Talk to all journal nodes, get manifest, and compare it ith what we expect
   * to see.
   */
  void validateSegmentManifest(List<SegmentDescriptor> segments,
      MiniJournalCluster cluster) throws MalformedURLException, IOException {
    for (int i = 0; i < cluster.getNumNodes(); i++) {
      String m = DFSUtil.getHTMLContentWithTimeout(
          new URL("http", "localhost", cluster.getHttpPort(i),
              GetJournalManifestServlet.buildPath(JID, 0, FAKE_NSINFO)),
          JournalConfigKeys.DFS_QJOURNAL_HTTP_TIMEOUT_DEFAULT,
          JournalConfigKeys.DFS_QJOURNAL_HTTP_TIMEOUT_DEFAULT);

      // test the method used for converting json -> manifest
      List<EditLogFile> manifest = JournalNodeJournalSyncer
          .convertJsonToListManifest(m);
      assertEquals(segments.size(), manifest.size());

      // for each segment we should have, find the corresponding entry in the
      // manifest
      for (SegmentDescriptor sd : segments) {
        EditLogFile elf = findFile(sd.startTxId, manifest);
        assertNotNull(elf);
        if (sd.endTxId == -1) {
          // manifest should have an entry for inprogress segment
          assertTrue(elf.isInProgress());
        } else {
          // manifest should conatin entry for a finalized segmet
          assertEquals(sd.endTxId, elf.getLastTxId());
          assertFalse(elf.isInProgress());
        }
      }
    }
  }

  class TestQJMRecoveryInjectionHandler extends InjectionHandler {

    volatile int journalToFail = -1;
    volatile int failAtTransaction = -1;
    int transactionsSinceReset = 0;
    volatile boolean failAtStartSegment = false;

    // hom many recoveries were completed (also failed)
    AtomicInteger completedJournalRecoveries = new AtomicInteger(0);

    void resetFailureMode(int journalToFail, int failAtTransaction,
        boolean failAtStartSegment) {
      this.journalToFail = journalToFail;
      this.failAtTransaction = failAtTransaction;
      this.transactionsSinceReset = 0;
      this.failAtStartSegment = failAtStartSegment;

      LOG.info("Will fail jn : "
          + journalToFail
          + " at: "
          + (failAtStartSegment ? " start log segment. "
              : (failAtTransaction + " transaction in this segment.")));
    }

    @Override
    protected void _processEvent(InjectionEventI event, Object... args) {
      if (event == InjectionEvent.QJM_JOURNALNODE_RECOVERY_COMPLETED) {
        completedJournalRecoveries.incrementAndGet();
      }
    }

    // wait until we received num completion events
    // 30 second timeout
    void waitNumRecoveries(int num) {
      long start = now();
      while (completedJournalRecoveries.get() < num && (now() - start < 15000)) {
        DFSTestUtil.waitNMilliSecond(500);
      }
    }

    @Override
    protected void _processEventIO(InjectionEventI event, Object... args)
        throws IOException {
      if (event == InjectionEvent.QJM_JOURNALNODE_JOURNAL
          && !failAtStartSegment) {
        Configuration conf = (Configuration) args[0];
        int jid = conf.getInt(MiniJournalCluster.DFS_JOURNALNODE_TEST_ID, 0);
        if (jid == journalToFail) {
          // this is the journal we need to fail
          if (transactionsSinceReset++ == failAtTransaction) {
            // we reached the transaction at which the failure is supposed to
            // happen - inject failure
            throw new IOException("Simulating failure for jid: " + jid);
          }
        }
      }
      if (event == InjectionEvent.QJM_JOURNALNODE_STARTSEGMENT
          && failAtStartSegment) {
        Configuration conf = (Configuration) args[0];
        int jid = conf.getInt(MiniJournalCluster.DFS_JOURNALNODE_TEST_ID, 0);
        if (jid == journalToFail) {
          throw new IOException("Simulating failure for jid: " + jid);
        }
      }
    }
  }

  static long now() {
    return System.currentTimeMillis();
  }
}
