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
package org.apache.hadoop.hdfs.server.namenode.bookkeeper.metadata;

import com.google.common.collect.ImmutableSet;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.protocol.FSConstants;
import org.apache.hadoop.hdfs.server.namenode.bookkeeper.BookKeeperSetupUtil;
import org.apache.hadoop.hdfs.server.namenode.bookkeeper.zk.BasicZooKeeper;
import org.apache.hadoop.hdfs.server.namenode.bookkeeper.zk.RecoveringZooKeeper;
import org.apache.hadoop.hdfs.server.namenode.bookkeeper.zk.ZooKeeperIface;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertFalse;
import static junit.framework.Assert.assertNull;
import static junit.framework.Assert.assertTrue;

public class TestBookKeeperJournalMetadataManager extends BookKeeperSetupUtil {

  private static final Log LOG =
      LogFactory.getLog(TestBookKeeperJournalMetadataManager.class);

  // ZooKeeper namespace to use for this test
  private static final String TEST_ZK_PARENT =
      "/testBookKeeperJournalMetadataManager";

  private BookKeeperJournalMetadataManager manager;

  @Before
  public void setUp() throws Exception {
    super.setUp();
    LOG.info("Initializing new BookKeeperJournalMetataManager instance for " +
        TEST_ZK_PARENT);
    manager = new BookKeeperJournalMetadataManager(
        getRecoveringZookeeperClient(), TEST_ZK_PARENT);
    manager.init();
  }

  @After
  public void tearDown() throws Exception {
    super.tearDown();
    if (getRecoveringZookeeperClient() != null) {
      LOG.info("Clearing out " + TEST_ZK_PARENT + " from previous test");
      zkDeleteRecursively(TEST_ZK_PARENT);
    }
  }

  @Test
  public void testReadAndWriteEditLogLedgerMetadata() throws Exception {
    assertNull("Should return null if ZNode does not exist",
        manager.readEditLogLedgerMetadata("/does/not/exist"));

    EditLogLedgerMetadata metadata_1_100 = new EditLogLedgerMetadata(
        FSConstants.LAYOUT_VERSION, 1, 1, 100);

    String fullyQualifiedPath_1_100 =
        manager.fullyQualifiedPathForLedger(metadata_1_100);

    assertTrue("Should return true if ZNode is created",
        manager.writeEditLogLedgerMetadata(fullyQualifiedPath_1_100,
            metadata_1_100));
    assertFalse("Should return false if ZNode already exists",
        manager.writeEditLogLedgerMetadata(fullyQualifiedPath_1_100,
            metadata_1_100));

    assertEquals("Should read be able to correctly read the written metadata",
        manager.readEditLogLedgerMetadata(fullyQualifiedPath_1_100),
        metadata_1_100);
  }

  @Test
  public void testVerifyEditLogLedgerMetadata() throws Exception {
    EditLogLedgerMetadata m0 = new EditLogLedgerMetadata(
        FSConstants.LAYOUT_VERSION, 1, 1, 100);
    EditLogLedgerMetadata m1 = new EditLogLedgerMetadata(
        FSConstants.LAYOUT_VERSION, 2, 101, 200);

    String m0Path = manager.fullyQualifiedPathForLedger(m0);
    String m1Path = manager.fullyQualifiedPathForLedger(m1);
    manager.writeEditLogLedgerMetadata(m0Path, m0);
    manager.writeEditLogLedgerMetadata(m1Path, m1);

    assertTrue(m0 + " should verify under " + m0Path,
        manager.verifyEditLogLedgerMetadata(m0, m0Path));
    assertTrue(m1 + " should verify under " + m1Path,
        manager.verifyEditLogLedgerMetadata(m1, m1Path));

    assertFalse(m0 + " should not verify under " + m1Path,
        manager.verifyEditLogLedgerMetadata(m0, m1Path));
    assertFalse(m1 + " should not verify under" + m0Path,
        manager.verifyEditLogLedgerMetadata(m1, m0Path));

    assertFalse("Non-existent path should not verify!",
        manager.verifyEditLogLedgerMetadata(m0, "/does/not/exist"));
  }

  @Test
  public void testListLedgers() throws Exception {
    Set<EditLogLedgerMetadata> metadata = ImmutableSet.of(
        new EditLogLedgerMetadata(FSConstants.LAYOUT_VERSION, 1, 1, 100),
        new EditLogLedgerMetadata(FSConstants.LAYOUT_VERSION, 2, 101, 200),
        new EditLogLedgerMetadata(FSConstants.LAYOUT_VERSION, 3, 201, -1),
        new EditLogLedgerMetadata(FSConstants.LAYOUT_VERSION - 1, 4, 11, -1),
        new EditLogLedgerMetadata(FSConstants.LAYOUT_VERSION - 1, 5, 2, 10));
    for (EditLogLedgerMetadata e : metadata) {
      String fullLedgerPath = manager.fullyQualifiedPathForLedger(e);
      manager.writeEditLogLedgerMetadata(fullLedgerPath, e);
    }
    Collection<EditLogLedgerMetadata> allLedgers = manager.listLedgers(true);
    assertTrue("listLedgers(true) returns all ledgers and all ledgers once",
        allLedgers.containsAll(metadata) && metadata.containsAll(allLedgers));

    EditLogLedgerMetadata prev = null;
    for (EditLogLedgerMetadata curr : allLedgers) {
      if (prev != null) {
        assertTrue("List must be ordered by firsTxId",
            prev.getFirstTxId() <= curr.getFirstTxId());
      }
      prev = curr;
    }

    Collection<EditLogLedgerMetadata> finalizedOnly =
        manager.listLedgers(false);
    for (EditLogLedgerMetadata e : finalizedOnly) {
      assertFalse("listLedgers(false) does not return in-progress ledgers" + e,
          e.getLastTxId() == -1);
    }
    TreeSet<EditLogLedgerMetadata> expectedFinalizedOnly =
        new TreeSet<EditLogLedgerMetadata>();
    for (EditLogLedgerMetadata e : allLedgers) {
      if (e.getLastTxId() != -1) {
        expectedFinalizedOnly.add(e);
      }
    }
    assertTrue("listLedgers(false) returns all finalized ledgers",
        finalizedOnly.containsAll(expectedFinalizedOnly) &&
            expectedFinalizedOnly.containsAll(finalizedOnly));
  }
}
