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

import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BKException.Code;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.streaming.LedgerInputStream;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hdfs.server.namenode.EditLogFileOutputStream;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogTestUtil;
import org.apache.hadoop.hdfs.server.namenode.TestEditLogFileOutputStream;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IOUtils.NullOutputStream;
import org.apache.hadoop.util.StringUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.List;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertTrue;
import static junit.framework.Assert.fail;


public class TestBookKeeperEditLogOutputStream extends BookKeeperSetupUtil {

  private static final Log LOG =
      LogFactory.getLog(TestBookKeeperEditLogOutputStream.class);

  private File tempEditsFile = null;

  @Before
  public void setUp() throws Exception {
    super.setUp();
    tempEditsFile = FSEditLogTestUtil.createTempEditsFile(
        "testBookKeeperEditLogOutputStream");
  }

  @After
  public void tearDown() throws Exception {
    super.tearDown();
    if (tempEditsFile != null) {
      if (!tempEditsFile.delete()) {
        LOG.warn("Unable to delete temporary edits file: " +
            tempEditsFile.getAbsolutePath());
      }
    }
  }

  /**
   * Writes workload to both a BookKeeperEditLogOutputStream and
   * a EditLogFileOutputStream and then verify that they function
   * (nearly) identically.
   */
  @Test
  public void testWrite() throws Exception {
    LedgerHandle ledger = createLedger();
    BookKeeperEditLogOutputStream bkEdits =
        new BookKeeperEditLogOutputStream(ledger);
    EditLogFileOutputStream fileEdits =
        new EditLogFileOutputStream(tempEditsFile, null);

    FSEditLogTestUtil.createAndPopulateStreams(1, 100, bkEdits, fileEdits);

    // Test that after closing both, an EditLogFileOutputStream
    // and a BookKeeperEditLogOutputStream objects return identical
    // length.
    assertEquals("Lengths must match", tempEditsFile.length(),
        ledger.getLength());

    long tempFileCrc32 = IOUtils.copyBytesAndGenerateCRC(
        new FileInputStream(tempEditsFile),
        new NullOutputStream(),
        (int) tempEditsFile.length(),
        false);
    long ledgerCrc32 = IOUtils.copyBytesAndGenerateCRC(
        new LedgerInputStream(ledger),
        new NullOutputStream(),
        (int) ledger.getLength(),
        false);

    // Test that the same data (including a log version) has been written
    // to both BookKeeperEditLogOutputStream and EditLogFileOutputStream
    // by comparing their Crc-32 checksums.
    assertEquals("Crc32 of data in file and in BookKeeper ledger must match",
        tempFileCrc32, ledgerCrc32);
  }

  /**
   * Test that after a BookKeeperEditLogOutputStream we should not be able
   * to mutate the ledger: verify that the ledger is closed, closed ledger
   * can't be written to, and the appropriate exception is bubbled up to
   * the application.
   */
  @Test
  public void testCloseClosesLedger() throws Exception {
    LedgerHandle ledger = createLedger();
    BookKeeperEditLogOutputStream bkOs =
        new BookKeeperEditLogOutputStream(ledger);
    FSEditLogTestUtil.closeStreams(bkOs);
    try {
      // Try writing to a ledger
      ledger.addEntry("a".getBytes(), 0, 1);
      fail("Closing BookKeeperEditLogOutputStream should close the ledger!");
    } catch (BKException e) {
      // Right exception must be bubbled up
      assertEquals("LedgerClosedException should propagate!",
          e.getCode(), Code.LedgerClosedException);
    }
  }

  /**
   * @see TestEditLogFileOutputStream#testEditLogFileOutputStreamCloseClose()
   * @throws IOException
   */
  @Test
  public void testCloseClose() throws IOException {
    LedgerHandle ledger = createLedger();
    BookKeeperEditLogOutputStream bkOs =
        new BookKeeperEditLogOutputStream(ledger);
    bkOs.close();
    try {
      bkOs.close();
    } catch (IOException e) {
      String msg = StringUtils.stringifyException(e);
      assertTrue(msg, msg.contains("Trying to use aborted output stream!"));
    }
  }

  /**
   * @see TestEditLogFileOutputStream#testEditLogFileOutputStreamAbortAbort()
   * @throws IOException
   */
  @Test
  public void testAbortAbort() throws IOException {
    LedgerHandle ledger = createLedger();
    BookKeeperEditLogOutputStream bkOs =
        new BookKeeperEditLogOutputStream(ledger);
    bkOs.abort();
    bkOs.abort();
  }

  /**
   * @see TestEditLogFileOutputStream#testEditLogFileOutputStreamCloseAbort()
   * @throws IOException
   */
  @Test
  public void testCloseAbort() throws IOException {
    LedgerHandle ledger = createLedger();
    BookKeeperEditLogOutputStream bkOs =
        new BookKeeperEditLogOutputStream(ledger);
    bkOs.close();
    bkOs.abort();
  }
}
