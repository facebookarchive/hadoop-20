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
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.server.common.HdfsConstants;
import org.apache.hadoop.hdfs.server.common.HdfsConstants.Transition;
import org.apache.hadoop.hdfs.server.namenode.EditLogFileInputStream;
import org.apache.hadoop.hdfs.server.namenode.EditLogFileOutputStream;
import org.apache.hadoop.hdfs.server.namenode.EditLogInputStream;
import org.apache.hadoop.hdfs.server.namenode.EditLogOutputStream;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogLoader;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogTestUtil;
import org.apache.hadoop.hdfs.server.namenode.JournalManager;
import org.apache.hadoop.hdfs.server.namenode.JournalSet;
import org.apache.hadoop.hdfs.server.namenode.bookkeeper.metadata.EditLogLedgerMetadata;
import org.apache.hadoop.hdfs.server.protocol.NamespaceInfo;
import org.apache.hadoop.hdfs.util.InjectionEvent;
import org.apache.hadoop.util.InjectionEventI;
import org.apache.hadoop.util.InjectionHandler;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertFalse;
import static junit.framework.Assert.assertNotNull;
import static junit.framework.Assert.assertTrue;

public class TestBookKeeperJournalManager extends BookKeeperSetupUtil {

  private static final Log LOG =
      LogFactory.getLog(TestBookKeeperJournalManager.class);

  private Configuration conf;
  private BookKeeperJournalManager bkjm;

  /**
   * Initialize a BookKeeperJournalManager instance and format the namespace
   * @param testName Name of the test
   */
  private void setupTest(String testName) throws IOException {
    conf = new Configuration();
    NamespaceInfo nsi = new NamespaceInfo();
    bkjm = new BookKeeperJournalManager(conf, createJournalURI(testName),
        new NamespaceInfo(), null);
    bkjm.transitionJournal(nsi, Transition.FORMAT, null);
  }

  @Test
  public void testHasSomeData() throws Exception {
    setupTest("test-has-some-data");
    assertTrue("hasSomeData() returns true after initialization",
        bkjm.hasSomeJournalData());
    zkDeleteRecursively(bkjm.zkParentPath);
    assertFalse("hasSomeData() returns false when zooKeeper data is empty",
        bkjm.hasSomeJournalData());
  }

  @Test
  public void testGetNumberOfTransactions() throws Exception {
    setupTest("test-get-number-of-transactions");
    EditLogOutputStream eos = bkjm.startLogSegment(1);
    FSEditLogTestUtil.populateStreams(1, 100, eos);
    bkjm.finalizeLogSegment(1, 100);
    assertEquals(
        "getNumberOfTransactions() is correct with a finalized log segment",
        100, FSEditLogTestUtil.getNumberOfTransactions(bkjm, 1, false, true));
    eos = bkjm.startLogSegment(101);
    FSEditLogTestUtil.populateStreams(101, 150, eos);
    assertEquals(
        "getNumberOfTransactions() is correct with an in-progress log segment",
        50, FSEditLogTestUtil.getNumberOfTransactions(bkjm, 101, true, true));
  }

  @Test
  public void testGetInputStreamWithValidation() throws Exception {
    setupTest("test-get-input-stream-with-validation");
    File tempEditsFile = FSEditLogTestUtil.createTempEditsFile(
        "test-get-input-stream-with-validation");
    try {
      TestBKJMInjectionHandler h = new TestBKJMInjectionHandler();
      InjectionHandler.set(h);
      EditLogOutputStream bkeos = bkjm.startLogSegment(1);
      EditLogOutputStream elfos =
          new EditLogFileOutputStream(tempEditsFile, null);
      elfos.create();
      FSEditLogTestUtil.populateStreams(1, 100, bkeos, elfos);
      EditLogInputStream bkeis =
          FSEditLogTestUtil.getJournalInputStream(bkjm, 1, true);
      EditLogInputStream elfis = new EditLogFileInputStream(tempEditsFile);
      Map<String, EditLogInputStream> streamByName =
          ImmutableMap.of("BookKeeper", bkeis, "File", elfis);
      FSEditLogTestUtil.assertStreamsAreEquivalent(100, streamByName);
      assertNotNull("Log was validated", h.logValidation);
      assertEquals("numTrasactions validated correctly",
          100, h.logValidation.getNumTransactions());
      assertEquals("endTxId validated correctly",
          100, h.logValidation.getEndTxId());
    } finally {
      if (!tempEditsFile.delete()) {
        LOG.warn("Unable to delete edits file: " +
            tempEditsFile.getAbsolutePath());
      }
    }
  }

  @Test
  public void testGetInputStreamNoValidationNoCheckLastTxId() throws Exception {
    setupTest("test-get-input-stream-no-validation-no-check-last-txid");
    File tempEditsFile = FSEditLogTestUtil.createTempEditsFile(
        "test-get-input-stream-with-validation");
    try {
      EditLogOutputStream bkeos = bkjm.startLogSegment(1);
      EditLogOutputStream elfos =
          new EditLogFileOutputStream(tempEditsFile, null);
      elfos.create();
      FSEditLogTestUtil.populateStreams(1, 100, bkeos, elfos);
      EditLogInputStream bkeis =
          getJournalInputStreamDontCheckLastTxId(bkjm, 1);
      EditLogInputStream elfis = new EditLogFileInputStream(tempEditsFile);
      Map<String, EditLogInputStream> streamByName =
          ImmutableMap.of("BookKeeper", bkeis, "File", elfis);
      FSEditLogTestUtil.assertStreamsAreEquivalent(100, streamByName);
    } finally {
      if (!tempEditsFile.delete()) {
        LOG.warn("Unable to delete edits file: " +
            tempEditsFile.getAbsolutePath());
      }
    }
  }

  @Test
  public void testRecoverUnfinalizedSegmentsEmptySegment() throws Exception {
    setupTest("test-recover-unfinalized-segments-empty-segment");
    TestBKJMInjectionHandler h = new TestBKJMInjectionHandler();
    InjectionHandler.set(h);
    bkjm.startLogSegment(1);
    assertEquals("Ledger created for segment with firstTxId " + 1,
        h.ledgerForStartedSegment.getFirstTxId(), 1);

    bkjm.currentInProgressPath = null;

    bkjm.recoverUnfinalizedSegments();
    String ledgerPath = bkjm.metadataManager.fullyQualifiedPathForLedger(
        h.ledgerForStartedSegment);
    assertFalse("Zero length ledger was deleted from " + ledgerPath,
        bkjm.metadataManager.ledgerExists(ledgerPath));
  }

  @Test
  public void testRecoverUnfinalizedSegments() throws Exception {
    setupTest("test-recover-unfinalized-segments");
    TestBKJMInjectionHandler h = new TestBKJMInjectionHandler();
    InjectionHandler.set(h);
    EditLogOutputStream eos = bkjm.startLogSegment(1);
    FSEditLogTestUtil.populateStreams(1, 100, eos);

    bkjm.currentInProgressPath = null;
    bkjm.recoverUnfinalizedSegments();

    Collection<EditLogLedgerMetadata> ledgers =
        bkjm.metadataManager.listLedgers(true);
    assertEquals("Only one ledger must remain", 1, ledgers.size());

    EditLogLedgerMetadata ledger = ledgers.iterator().next();
    assertEquals("Ledger must be finalized with correct lastTxId",
        ledger.getLastTxId(), 100);
    assertEquals("Finalized ledger's ledgerId must match created ledger's id",
        ledger.getLedgerId(), h.ledgerForStartedSegment.getLedgerId());
    assertEquals("Finalized ledger's firstTxId", ledger.getFirstTxId(),
        h.ledgerForStartedSegment.getFirstTxId());

  }


  static EditLogInputStream getJournalInputStreamDontCheckLastTxId(
      JournalManager jm, long txId) throws IOException {
    List<EditLogInputStream> streams = new ArrayList<EditLogInputStream>();
    jm.selectInputStreams(streams, txId, true, false);
    if (streams.size() < 1) {
      throw new IOException("Cannot obtain stream for txid: " + txId);
    }
    Collections.sort(streams, JournalSet.EDIT_LOG_INPUT_STREAM_COMPARATOR);

    if (txId == HdfsConstants.INVALID_TXID) {
      return streams.get(0);
    }

    for (EditLogInputStream elis : streams) {
      if (elis.getFirstTxId() == txId) {
        return elis;
      }
    }
    throw new IOException("Cannot obtain stream for txid: " + txId);
  }

  class TestBKJMInjectionHandler extends InjectionHandler {

    EditLogLedgerMetadata ledgerForStartedSegment;
    FSEditLogLoader.EditLogValidation logValidation;

    @Override
    protected void _processEvent(InjectionEventI event, Object... args) {
      if (event == InjectionEvent.BKJM_STARTLOGSEGMENT) {
        ledgerForStartedSegment = (EditLogLedgerMetadata) args[0];
      } else if (event == InjectionEvent.BKJM_VALIDATELOGSEGMENT) {
        logValidation = (FSEditLogLoader.EditLogValidation) args[0];
      }
    }

  }

}
