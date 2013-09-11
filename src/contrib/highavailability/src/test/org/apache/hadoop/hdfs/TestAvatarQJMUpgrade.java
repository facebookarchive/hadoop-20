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
package org.apache.hadoop.hdfs;

import java.io.IOException;
import java.io.File;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.qjournal.MiniJournalCluster;
import org.apache.hadoop.hdfs.server.common.HdfsConstants.NodeType;
import org.apache.hadoop.hdfs.server.common.HdfsConstants.StartupOption;
import org.apache.hadoop.hdfs.server.common.Storage.StorageDirectory;
import org.apache.hadoop.hdfs.server.namenode.FSImageTestUtil.CheckpointTrigger;
import org.apache.hadoop.hdfs.util.InjectionEvent;
import org.apache.hadoop.util.InjectionHandler;
import org.apache.hadoop.util.InjectionEventI;

import org.junit.After;
import org.junit.AfterClass;
import static org.junit.Assert.*;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestAvatarQJMUpgrade {

  private static MiniAvatarCluster cluster;
  private static Configuration conf;
  private static TestAvatarQJMUpgradeHandler h;

  private static class TestAvatarQJMUpgradeHandler extends InjectionHandler {
    final CheckpointTrigger checkpointTrigger = new CheckpointTrigger();
    Set<StorageDirectory> storageSet = Collections
        .synchronizedSet(new HashSet<StorageDirectory>());
    volatile boolean failBeforeSaveImage = false;
    volatile boolean failAfterSaveImage = false;
    AtomicInteger nFailures = new AtomicInteger(0);
    long[] checksumsAfterRollback = null;

    private long[] checksumCurrent() throws IOException {
      int i = 0;
      long[] checksums = new long[storageSet.size()];
      for (StorageDirectory dir : storageSet) {
        assertTrue(dir.getCurrentDir().exists());
        checksums[i++] = UpgradeUtilities.checksumContents(
            NodeType.JOURNAL_NODE, dir.getCurrentDir(),
            new String[] { "last-promised-epoch" });
      }
      return checksums;
    }


    protected void _processEvent(InjectionEventI event, Object... args) {
      if (event == InjectionEvent.JOURNAL_STORAGE_FORMAT) {
        StorageDirectory sd = (StorageDirectory) args[0];
        if (sd.getCurrentDir().getAbsolutePath().indexOf("zero") > 0) {
          // Only /zero directories are upgraded.
          storageSet.add(sd);
        }
      } else if (event == InjectionEvent.FSIMAGE_ROLLBACK_DONE) {
        try {
          checksumsAfterRollback = checksumCurrent();
        } catch (Exception e) {}
      }
      checkpointTrigger.checkpointDone(event, args);
    }

    protected void _processEventIO(InjectionEventI event, Object... args)
        throws IOException {
      if (event == InjectionEvent.FSIMAGE_UPGRADE_BEFORE_SAVE_IMAGE
          && failBeforeSaveImage) {
        // Simulate only one failure since the upgrade is retried in
        // MiniAvatarCluster using instantiationRetries.
        if (nFailures.incrementAndGet() <= 1) {
          throw new IOException("Simulating failure before save image");
        }
      }
      if (event == InjectionEvent.FSIMAGE_UPGRADE_AFTER_SAVE_IMAGE
          && failAfterSaveImage) {
        // Simulate only one failure since the upgrade is retried in
        // MiniAvatarCluster using instantiationRetries.
        if (nFailures.incrementAndGet() <= 1) {
          throw new IOException("Simulating failure after save image");
        }
      }
    }

    @Override
    protected boolean _falseCondition(InjectionEventI event, Object... args) {
      return checkpointTrigger.triggerCheckpoint(event);
    }

    void doCheckpoint() throws Exception {
      checkpointTrigger.doCheckpoint();
    }
  }

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    MiniAvatarCluster.createAndStartZooKeeper();
  }

  @Before
  public void setUp() throws Exception {
    h = new TestAvatarQJMUpgradeHandler();
    InjectionHandler.set(h);
    conf = new Configuration();
    cluster = new MiniAvatarCluster.Builder(conf).numDataNodes(1).build();
  }

  @After
  public void tearDown() throws Exception {
    cluster.shutDown();
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    MiniAvatarCluster.shutDownZooKeeper();
  }

  private long[] getChecksums() throws Exception {
    DFSTestUtil.createFile(cluster.getFileSystem(), new Path("/test"), 1024,
        (short) 1, 0);
    cluster.shutDownAvatarNodes();
    cluster.shutDownDataNodes();

    // Compute checksums for current directory.
    return h.checksumCurrent();
  }

  private void verifyRollback(long[] checksums, boolean failover,
      long[] expectedChecksums)
      throws Exception {
    checkState(checksums, failover, false, expectedChecksums);
  }

  private void verifyUpgrade(long[] checksums, boolean failover)
      throws Exception {
    checkState(checksums, failover, true, null);
  }

  private void checkState(long[] checksums, boolean failover, boolean previous,
      long[] expectedChecksums)
      throws Exception {
    // Verify checksums for previous directory match with the older current
    // directory.

    int i = 0;
    for (StorageDirectory dir : h.storageSet) {
      System.out.println("Directory : " + dir);
      File dirToProcess = (previous) ? dir.getPreviousDir() : dir.getCurrentDir();
      assertTrue(dirToProcess.exists());
      // We skip last-promised-epoch since it is bumped up just before the
      // upgrade is done when we recover unfinalized segments.
      if (expectedChecksums == null) {
        assertEquals(checksums[i++], UpgradeUtilities.checksumContents(
            NodeType.JOURNAL_NODE, dirToProcess,
            new String[] { "last-promised-epoch" }));
      } else {
        assertEquals(checksums[i], expectedChecksums[i]);
        i++;
      }
    }

    // Ensure we can still write.
    DFSTestUtil.createFile(cluster.getFileSystem(), new Path("/test1"), 1024,
        (short) 1, 0);

    // Ensure we can still do checkpoint.
    h.doCheckpoint();

    // Ensure we can still failover.
    if (failover) {
      cluster.failOver();
    }
  }

  private long[] doUpgrade(boolean failover) throws Exception {
    long[] checksums = getChecksums();

    // Upgrade the cluster.
    cluster = new MiniAvatarCluster.Builder(conf).numDataNodes(1).format(false)
        .startOpt(StartupOption.UPGRADE)
        .setJournalCluster(cluster.getJournalCluster()).build();

    verifyUpgrade(checksums, failover);
    return checksums;
  }

  /**
   * This test just verifies that we can upgrade the storage on journal nodes.
   */
  @Test
  public void testUpgrade() throws Exception {
    doUpgrade(true);
  }

  /**
   * This test simulates the scenario where the upgrade fails before saving
   * image and ensures that the recovery on the journal nodes work correctly.
   */
  @Test
  public void testUpgradeFailureBeforeSaveImage() throws Exception {
    h.failBeforeSaveImage = true;
    doUpgrade(true);
  }

  /**
   * This test simulates the scenario where the upgrade fails after saving image
   * and ensures that the recovery on the journal nodes work correctly.
   */
  @Test
  public void testUpgradeFailureAfterSaveImage() throws Exception {
    h.failAfterSaveImage = true;

    long[] checksums = getChecksums();
    // Upgrade the cluster.
    MiniJournalCluster journalCluster = cluster.getJournalCluster();

    // This upgrade will fail after saving the image.
    try {
      cluster = new MiniAvatarCluster.Builder(conf).numDataNodes(1)
          .format(false).startOpt(StartupOption.UPGRADE)
          .setJournalCluster(journalCluster).instantionRetries(1).build();
      fail("Upgrade did not throw exception");
    } catch (IOException ie) {
      // ignore.
    }

    // This will correctly recover the upgrade directories.
    cluster = new MiniAvatarCluster.Builder(conf).numDataNodes(1).format(false)
        .setJournalCluster(cluster.getJournalCluster()).build();

    verifyUpgrade(checksums, true);
  }

  /**
   * This test verifies that we can finalize the upgrade for journal nodes.
   */
  @Test
  public void testFinalize() throws Exception {
    // Uprade the namenode.
    doUpgrade(false);

    // Now finalize the upgrade.
    cluster.getPrimaryAvatar(0).avatar.finalizeUpgrade();

    // Verify previous directory doesn't exist.
    for (StorageDirectory dir : h.storageSet) {
      assertFalse(dir.getPreviousDir().exists());
      assertFalse(dir.getPreviousTmp().exists());
    }
  }

  /**
   * This test verifies that we can rollback the upgrade for journal nodes.
   */
  @Test
  public void testRollback() throws Exception {
    // Uprade the namenode.
    long[] checksums = doUpgrade(false);

    cluster.shutDownAvatarNodes();
    cluster.shutDownDataNodes();

    // Now rollback the cluster.
    cluster = new MiniAvatarCluster.Builder(conf).numDataNodes(1).format(false)
        .startOpt(StartupOption.ROLLBACK)
        .setJournalCluster(cluster.getJournalCluster()).build();

    assertNotNull(h.checksumsAfterRollback);
    verifyRollback(checksums, true, h.checksumsAfterRollback);
  }

  /**
   * This test verifies that we cannot upgrade twice without rollback/finalize.
   */
  @Test(expected=IOException.class)
  public void upgradeTwice() throws Exception {
    doUpgrade(false);
    doUpgrade(false);
  }
}
