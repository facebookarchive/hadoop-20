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

import java.io.*;
import java.net.URI;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Arrays;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import junit.framework.TestCase;

import org.apache.commons.io.FileUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.impl.Log4JLogger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.*;

import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.FSConstants;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.server.namenode.EditLogFileInputStream;
import org.apache.hadoop.hdfs.server.common.HdfsConstants;
import org.apache.hadoop.hdfs.server.common.Storage.StorageDirectory;
import org.apache.hadoop.hdfs.server.namenode.JournalStream.JournalType;
import org.apache.hadoop.hdfs.server.namenode.NNStorage.NameNodeDirType;
import org.apache.hadoop.hdfs.server.namenode.NNStorage.NameNodeFile;
import org.apache.hadoop.hdfs.server.namenode.NNStorage;
import org.apache.hadoop.hdfs.server.protocol.AutoEditsRollerInterface;

import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.util.Daemon;
import org.apache.hadoop.util.StringUtils;
import org.apache.log4j.Level;

import static org.junit.Assert.*;

import org.mockito.Mockito;
import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;


/**
 * This class tests the creation and validation of a checkpoint.
 */
public class TestEditLog {
  
  static {
    ((Log4JLogger)FSEditLog.LOG).getLogger().setLevel(Level.ALL);
  }
  
  static final Log LOG = LogFactory.getLog(TestEditLog.class);
  
  static final int NUM_DATA_NODES = 0;

  // This test creates NUM_THREADS threads and each thread does
  // 2 * NUM_TRANSACTIONS Transactions concurrently.
  static final int NUM_TRANSACTIONS = 100;
  static final int NUM_THREADS = 100;
  
  static final File TEST_DIR = new File(
    System.getProperty("test.build.data","build/test/data"));

  /** An edits log with 3 edits from 0.20 - the result of
   * a fresh namesystem followed by hadoop fs -touchz /myfile */
  static final byte[] HADOOP20_SOME_EDITS =
    StringUtils.hexStringToByte((
        "ffff ffed 0a00 0000 0000 03fa e100 0000" +
        "0005 0007 2f6d 7966 696c 6500 0133 000d" +
        "3132 3932 3331 3634 3034 3138 3400 0d31" +
        "3239 3233 3136 3430 3431 3834 0009 3133" +
        "3432 3137 3732 3800 0000 0004 746f 6464" +
        "0a73 7570 6572 6772 6f75 7001 a400 1544" +
        "4653 436c 6965 6e74 5f2d 3136 3136 3535" +
        "3738 3931 000b 3137 322e 3239 2e35 2e33" +
        "3209 0000 0005 0007 2f6d 7966 696c 6500" +
        "0133 000d 3132 3932 3331 3634 3034 3138" +
        "3400 0d31 3239 3233 3136 3430 3431 3834" +
        "0009 3133 3432 3137 3732 3800 0000 0004" +
        "746f 6464 0a73 7570 6572 6772 6f75 7001" +
        "a4ff 0000 0000 0000 0000 0000 0000 0000"
    ).replace(" ",""));

  
  static final byte TRAILER_BYTE = FSEditLogOpCodes.OP_INVALID.getOpCode();

  private static final int CHECKPOINT_ON_STARTUP_MIN_TXNS = 100;
  //
  // an object that does a bunch of transactions
  //
  static class Transactions implements Runnable {
    private int threadid;
    FSNamesystem namesystem;
    int numTransactions;
    short replication = 3;
    long blockSize = 64;

    Transactions(FSNamesystem ns, int num, int id) {
      namesystem = ns;
      numTransactions = num;
      threadid = id;
    }

    // add a bunch of transactions.
    public void run() {
      PermissionStatus p = namesystem.createFsOwnerPermissions(
                                          new FsPermission((short)0777));
      FSEditLog editLog = namesystem.getEditLog();

      for (int i = 0; i < numTransactions; i++) {
        INodeFileUnderConstruction inode = new INodeFileUnderConstruction(
            namesystem.dir.allocateNewInodeId(), p, replication, blockSize, 0, "", "", null);
        try {
          editLog.logOpenFile("/filename" + threadid + "-" + i, inode);
          editLog.logCloseFile("/filename" + threadid + "-" + i, inode);
          editLog.logDelete("/filename" + threadid + "-" + i, 0);
          editLog.logSync();
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }
    }
  }

  /**
   * Test case for an empty edit log from a prior version of Hadoop.
   */
  @Test
  public void testPreTxIdEditLogNoEdits() throws Exception {
    FSNamesystem namesys = Mockito.mock(FSNamesystem.class);
    namesys.dir = Mockito.mock(FSDirectory.class);
    int numEdits = testLoad(
        StringUtils.hexStringToByte("ffffffed"), // just version number
        namesys);
    assertEquals(0, numEdits);
  }
  
  /**
   * Test case for loading a very simple edit log from a format
   * prior to the inclusion of edit transaction IDs in the log.
   */
  @Test
  public void testPreTxidEditLogWithEdits() throws Exception {
    Configuration conf = new Configuration();
    MiniDFSCluster cluster = null;

    try {
      cluster = new MiniDFSCluster(conf, 0, true, null);
      cluster.waitActive();
      final FSNamesystem namesystem = cluster.getNameNode().getNamesystem();

      int numEdits = testLoad(HADOOP20_SOME_EDITS, namesystem);
      assertEquals(3, numEdits);
      // Sanity check the edit
      HdfsFileStatus fileInfo = namesystem.getHdfsFileInfo("/myfile");
      assertEquals("supergroup", fileInfo.getGroup());
      assertEquals(3, fileInfo.getReplication());
    } finally {
      if (cluster != null) { cluster.shutdown(); }
    }
  }
  
  private int testLoad(byte[] data, FSNamesystem namesys) throws IOException {
    FSEditLogLoader loader = new FSEditLogLoader(namesys);
    return loader.loadFSEdits(new EditLogByteInputStream(data), 0);
  }

  /**
   * Simple test for writing to and rolling the edit log.
   */
  @Test
  public void testSimpleEditLog() throws IOException {
    // start a cluster 
    Configuration conf = new Configuration();
    MiniDFSCluster cluster = null;
    FileSystem fileSys = null;
    try {
      cluster = new MiniDFSCluster(conf, 0, true, null);
      cluster.waitActive();
      fileSys = cluster.getFileSystem();
      final FSNamesystem namesystem = cluster.getNameNode().getNamesystem();
      FSImage fsimage = namesystem.getFSImage();
      final FSEditLog editLog = fsimage.getEditLog();
      
      assertExistsInStorageDirs(
          cluster, NameNodeDirType.EDITS, 
          NNStorage.getInProgressEditsFileName(0));
      

      editLog.logSetReplication("fakefile", (short) 1);
      editLog.logSync();
      
      editLog.rollEditLog();

      assertExistsInStorageDirs(
          cluster, NameNodeDirType.EDITS,
          NNStorage.getFinalizedEditsFileName(0,2));
      assertExistsInStorageDirs(
          cluster, NameNodeDirType.EDITS,
          NNStorage.getInProgressEditsFileName(3));

      
      editLog.logSetReplication("fakefile", (short) 2);
      editLog.logSync();
      
      {
        // test checking for strea redundancy when choosing streams
        
        // simulate log recovery
        editLog.endCurrentLogSegment(true);     
        long txid = editLog.getCurrentTxId() - 1;
        
        List<EditLogInputStream> editStreams = new ArrayList<EditLogInputStream>();
        // pass -1
        assertFalse(editLog.selectInputStreams(editStreams, 0, txid, -5));
        assertFalse(editLog.selectInputStreams(editStreams, 0, txid, 1));
        // there is 2 streams
        assertFalse(editLog.selectInputStreams(editStreams, 0, txid, 2));
        // this one must return redundancy violation
        assertTrue(editLog.selectInputStreams(editStreams, 0, txid, 3));
      }
      editLog.close(); 
    } finally {
      if(fileSys != null) fileSys.close();
      if(cluster != null) cluster.shutdown();
    }
  }

  /**
   * Tests transaction logging in dfs.
   */
  @Test
  public void testMultiThreadedEditLog() throws IOException {
    testEditLog(2048);
    // force edit buffer to automatically sync on each log of edit log entry
    testEditLog(1);
  }
  
  
  private void assertExistsInStorageDirs(MiniDFSCluster cluster,
      NameNodeDirType dirType,
      String filename) {
    NNStorage storage = cluster.getNameNode().getNamesystem().getFSImage().storage;
    for (StorageDirectory sd : storage.dirIterable(dirType)) {
      File f = new File(sd.getCurrentDir(), filename);
      assertTrue("Expect that " + f + " exists", f.exists());
    }
  }
  
  /**
   * Test edit log with different initial buffer size
   * 
   * @param initialSize initial edit log buffer size
   * @throws IOException
   */
  private void testEditLog(int initialSize) throws IOException {

    // start a cluster 
    Configuration conf = new Configuration();
    MiniDFSCluster cluster = null;
    FileSystem fileSys = null;

    try {
      cluster = new MiniDFSCluster(conf, NUM_DATA_NODES, true, null);
      cluster.waitActive();
      fileSys = cluster.getFileSystem();
      final FSNamesystem namesystem = cluster.getNameNode().getNamesystem();
  
      for (Iterator<File> it = cluster.getNameDirs(0).iterator(); it.hasNext(); ) {
        URI uri = it.next().toURI();
        File dir = new File(uri);
        System.out.println(dir);
      }
  
      FSImage fsimage = namesystem.getFSImage();
  
      // set small size of flush buffer
      FSEditLog.setBufferCapacity(initialSize);
      
      // Roll log so new output buffer size takes effect
      // we should now be writing to edits_inprogress_2
      fsimage.rollEditLog();
    
      // Create threads and make them run transactions concurrently.
      Thread threadId[] = new Thread[NUM_THREADS];
      for (int i = 0; i < NUM_THREADS; i++) {
        Transactions trans = new Transactions(namesystem, NUM_TRANSACTIONS, i);
        threadId[i] = new Thread(trans, "TransactionThread-" + i);
        threadId[i].start();
      }
  
      // wait for all transactions to get over
      for (int i = 0; i < NUM_THREADS; i++) {
        try {
          threadId[i].join();
        } catch (InterruptedException e) {
          i--;      // retry 
        }
      } 
      
      // Roll another time to finalize edits_inprogress_3
      fsimage.rollEditLog();
      
      long expectedTxns = (NUM_THREADS * 3 * NUM_TRANSACTIONS) + 2; // +2 for start/end txns
   
      // Verify that we can read in all the transactions that we have written.
      // If there were any corruptions, it is likely that the reading in
      // of these transactions will throw an exception.
      //
      for (Iterator<StorageDirectory> it = 
              fsimage.storage.dirIterator(NameNodeDirType.EDITS); it.hasNext();) {
        FSEditLogLoader loader = new FSEditLogLoader(namesystem);
        
        File editFile = NNStorage.getFinalizedEditsFile(it.next(), 2,
            2 + expectedTxns - 1);
        assertTrue("Expect " + editFile + " exists", editFile.exists());
        
        System.out.println("Verifying file: " + editFile);
        int numEdits = loader.loadFSEdits(
            new EditLogFileInputStream(editFile), 1);
        int numLeases = namesystem.leaseManager.countLease();
        System.out.println("Number of outstanding leases " + numLeases);
        assertEquals(0, numLeases);
        assertTrue("Verification for " + editFile + " failed. " +
                   "Expected " + expectedTxns + " transactions. "+
                   "Found " + numEdits + " transactions.",
                   numEdits == expectedTxns);
  
      }
    } finally {
      try {
        if(fileSys != null) fileSys.close();
        if(cluster != null) cluster.shutdown();
      } catch (Throwable t) {
        LOG.error("Couldn't shut down cleanly", t);
      }
    }
  }

  private void doLogEdit(ExecutorService exec, final FSEditLog log,
    final String filename) throws Exception
  {
    exec.submit(new Callable<Void>() {
      public Void call() {
        log.logSetReplication(filename, (short)1);
        return null;
      }
    }).get();
  }
  
  private void doCallLogSync(ExecutorService exec, final FSEditLog log)
    throws Exception
  {
    exec.submit(new Callable<Void>() {
      public Void call() throws Exception {
        log.logSync();
        return null;
      }
    }).get();
  }

  private void doCallLogSyncAll(ExecutorService exec, final FSEditLog log)
    throws Exception
  {
    exec.submit(new Callable<Void>() {
      public Void call() throws Exception {
        log.logSyncAll();
        return null;
      }
    }).get();
  }

  @Test
  public void testSyncBatching() throws Exception {
    // start a cluster 
    Configuration conf = new Configuration();
    MiniDFSCluster cluster = null;
    FileSystem fileSys = null;
    ExecutorService threadA = Executors.newSingleThreadExecutor();
    ExecutorService threadB = Executors.newSingleThreadExecutor();
    try {
      cluster = new MiniDFSCluster(conf, NUM_DATA_NODES, true, null);
      cluster.waitActive();
      fileSys = cluster.getFileSystem();
      final FSNamesystem namesystem = cluster.getNameNode().getNamesystem();

      FSImage fsimage = namesystem.getFSImage();
      final FSEditLog editLog = fsimage.getEditLog();

      assertEquals("should start with only the BEGIN_LOG_SEGMENT txn synced",
        0, editLog.getSyncTxId());
      
      // Log an edit from thread A
      doLogEdit(threadA, editLog, "thread-a 1");
      assertEquals("logging edit without syncing should do not affect txid",
        0, editLog.getSyncTxId());

      // Log an edit from thread B
      doLogEdit(threadB, editLog, "thread-b 1");
      assertEquals("logging edit without syncing should do not affect txid",
        0, editLog.getSyncTxId());

      // Now ask to sync edit from B, which should sync both edits.
      doCallLogSync(threadB, editLog);
      assertEquals("logSync from second thread should bump txid up to 2",
        2, editLog.getSyncTxId());

      // Now ask to sync edit from A, which was already batched in - thus
      // it should increment the batch count metric
      doCallLogSync(threadA, editLog);
      assertEquals("logSync from first thread shouldn't change txid",
        2, editLog.getSyncTxId());

    } finally {
      threadA.shutdown();
      threadB.shutdown();
      if(fileSys != null) fileSys.close();
      if(cluster != null) cluster.shutdown();
    }
  }
  
  /**
   * Test what happens with the following sequence:
   *
   *  Thread A writes edit
   *  Thread B calls logSyncAll
   *           calls close() on stream
   *  Thread A calls logSync
   *
   * This sequence is legal and can occur if enterSafeMode() is closely
   * followed by saveNamespace.
   */
  @Test
  public void testBatchedSyncWithClosedLogs() throws Exception {
    // start a cluster 
    Configuration conf = new Configuration();
    MiniDFSCluster cluster = null;
    FileSystem fileSys = null;
    ExecutorService threadA = Executors.newSingleThreadExecutor();
    ExecutorService threadB = Executors.newSingleThreadExecutor();
    try {
      cluster = new MiniDFSCluster(conf, NUM_DATA_NODES, true, null);
      cluster.waitActive();
      fileSys = cluster.getFileSystem();
      final FSNamesystem namesystem = cluster.getNameNode().getNamesystem();

      FSImage fsimage = namesystem.getFSImage();
      final FSEditLog editLog = fsimage.getEditLog();

      // Log an edit from thread A
      doLogEdit(threadA, editLog, "thread-a 1");
      assertEquals("logging edit without syncing should do not affect txid",
        0, editLog.getSyncTxId());

      // logSyncAll in Thread B
      doCallLogSyncAll(threadB, editLog);
      assertEquals("logSyncAll should sync thread A's transaction",
        1, editLog.getSyncTxId());

      // Close edit log
      editLog.close();

      // Ask thread A to finish sync (which should be a no-op)
      doCallLogSync(threadA, editLog);
    } finally {
      threadA.shutdown();
      threadB.shutdown();
      if(fileSys != null) fileSys.close();
      if(cluster != null) cluster.shutdown();
    }
  }
  
  @Test
  public void testEditChecksum() throws Exception {
    // start a cluster 
    Configuration conf = new Configuration();
    MiniDFSCluster cluster = null;
    FileSystem fileSys = null;
    cluster = new MiniDFSCluster(conf, NUM_DATA_NODES, true, null);
    cluster.waitActive();
    fileSys = cluster.getFileSystem();
    final FSNamesystem namesystem = cluster.getNameNode().getNamesystem();

    FSImage fsimage = namesystem.getFSImage();
    final FSEditLog editLog = fsimage.getEditLog();
    fileSys.mkdirs(new Path("/tmp"));
    
    Iterator<StorageDirectory> iter = fsimage.storage.dirIterator(
        NameNodeDirType.EDITS);
    LinkedList<StorageDirectory> sds = new LinkedList<StorageDirectory>();
    while (iter.hasNext()) {
      sds.add(iter.next());
    }
    
    editLog.close();
    cluster.shutdown();

    for (StorageDirectory sd : sds) {
      File editFile = NNStorage.getFinalizedEditsFile(sd, 0, 2);
      assertTrue(editFile.exists());
  
      long fileLen = editFile.length();
      System.out.println("File name: " + editFile + " len: " + fileLen);
      RandomAccessFile rwf = new RandomAccessFile(editFile, "rw");
      rwf.seek(fileLen-4); // seek to checksum bytes
      int b = rwf.readInt();
      rwf.seek(fileLen-4);
      rwf.writeInt(b+1);
      rwf.close();
    }
    
    try {
      cluster = new MiniDFSCluster(conf, NUM_DATA_NODES, false, null);
      fail("should not be able to start");
    } catch (IOException e) {
      // expected
      assertEquals("Cause of exception should be ChecksumException",
          e.getCause().getClass(), IOException.class);
    } finally {
      if (cluster != null)
        cluster.shutdown();
    }
  }

  /**
   * Test what happens if the NN crashes when it has has started but
   * had no transactions written.
   */
  @Test
  public void testCrashRecoveryNoTransactions() throws Exception {
    testCrashRecovery(0);
  }
  
  /**
   * Test what happens if the NN crashes when it has has started and
   * had a few transactions written
   */
  @Test
  public void testCrashRecoveryWithTransactions() throws Exception {
    testCrashRecovery(150);
  }
  
  /**
   * Do a test to make sure the edit log can recover edits even after
   * a non-clean shutdown. This does a simulated crash by copying over
   * the edits directory while the NN is still running, then shutting it
   * down, and restoring that edits directory.
   */
  private void testCrashRecovery(int numTransactions) throws Exception {
    MiniDFSCluster cluster = null;
    Configuration conf = new Configuration();
    conf.setInt("fs.checkpoint.txns",
        CHECKPOINT_ON_STARTUP_MIN_TXNS);
    
    try {
        LOG.info("\n===========================================\n" +
                 "Starting empty cluster");
        
        cluster = new MiniDFSCluster(conf, NUM_DATA_NODES, true, null);
        cluster.waitActive();
        
        FileSystem fs = cluster.getFileSystem();
        for (int i = 0; i < numTransactions; i++) {
          fs.mkdirs(new Path("/test" + i));
        }        
        
        // Directory layout looks like:
        // test/data/dfs/nameN/current/{fsimage_N,edits_...}
        File nameDir = new File(cluster.getNameDirs(0).iterator().next().getPath());
        File dfsDir = nameDir.getParentFile();
        assertEquals(dfsDir.getName(), "dfs"); // make sure we got right dir
        
        LOG.info("Copying data directory aside to a hot backup");
        File backupDir = new File(dfsDir.getParentFile(), "dfs.backup-while-running");
        LOG.info("copying: " + dfsDir + " to: " + backupDir);
        if(backupDir.exists()) {
          FileUtil.fullyDelete(backupDir);
        }
        backupDir.mkdir();
        FileUtils.copyDirectory(dfsDir, backupDir);

        LOG.info("Shutting down cluster #1");
        cluster.shutdown();
        cluster = null;
        
        // Now restore the backup
        FileUtils.deleteDirectory(dfsDir);
        dfsDir.mkdir();
        backupDir.renameTo(dfsDir);
        
        // Directory layout looks like:
        // test/data/dfs/nameN/current/{fsimage_N,edits_...}
        File currentDir = new File(nameDir, "current");

        // We should see the file as in-progress
        File editsFile = new File(currentDir,
            NNStorage.getInProgressEditsFileName(0));
        assertTrue("Edits file " + editsFile + " should exist", editsFile.exists());        
        
        File imageFile = FSImageTestUtil.findNewestImageFile(
            currentDir.getAbsolutePath());
        assertNotNull("No image found in " + nameDir, imageFile);
        assertEquals(NNStorage.getImageFileName(-1), imageFile.getName());
        
        // Try to start a new cluster
        LOG.info("\n===========================================\n" +
        "Starting same cluster after simulated crash");
        cluster = new MiniDFSCluster(conf, NUM_DATA_NODES, false, null);
        cluster.waitActive();
        
        // We should still have the files we wrote prior to the simulated crash
        fs = cluster.getFileSystem();
        for (int i = 0; i < numTransactions; i++) {
          assertTrue(fs.exists(new Path("/test" + i)));
        }

        long expectedTxId;
        if (numTransactions > CHECKPOINT_ON_STARTUP_MIN_TXNS) {
          // It should have saved a checkpoint on startup since there
          // were more unfinalized edits than configured
          expectedTxId = numTransactions;
        } else {
          // otherwise, it shouldn't have made a checkpoint
          expectedTxId = -1;
        }
        imageFile = FSImageTestUtil.findNewestImageFile(
            currentDir.getAbsolutePath());
        assertNotNull("No image found in " + nameDir, imageFile);
        assertEquals(NNStorage.getImageFileName(expectedTxId),
                     imageFile.getName());
        
        // Started successfully. Shut it down and make sure it can restart.
        cluster.shutdown();    
        cluster = null;
        
        cluster = new MiniDFSCluster(conf, NUM_DATA_NODES, false, null);
        cluster.waitActive();
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }
  
  @Test
  public void testCrashRecoveryEmptyLogOneDir() throws Exception {
    doTestCrashRecoveryEmptyLog(false, true, false);
  }
  
  @Test
  public void testCrashRecoveryEmptyLogBothDirs() throws Exception {
    doTestCrashRecoveryEmptyLog(true, true, false);
  }

  @Test
  public void testCrashRecoveryEmptyLogOneDirNoUpdateSeenTxId() 
      throws Exception {
    doTestCrashRecoveryEmptyLog(false, false, true);
  }
  
  @Test
  public void testCrashRecoveryEmptyLogBothDirsNoUpdateSeenTxId()
      throws Exception {
    doTestCrashRecoveryEmptyLog(true, false, true);
  }

  /**
   * Test that the NN handles the corruption properly
   * after it crashes just after creating an edit log
   * (ie before writing START_LOG_SEGMENT). In the case
   * that all logs have this problem, it should mark them
   * as corrupt instead of trying to finalize them.
   * 
   * @param inBothDirs if true, there will be a truncated log in
   * both of the edits directories. If false, the truncated log
   * will only be in one of the directories. In both cases, the
   * NN should fail to start up, because it's aware that txid 3
   * was reached, but unable to find a non-corrupt log starting there.
   * @param updateTransactionIdFile if true update the seen_txid file.
   * If false, the it will not be updated. This will simulate a case 
   * where the NN crashed between creating the new segment and updating
   * seen_txid. 
   */
  private void doTestCrashRecoveryEmptyLog(boolean inBothDirs, 
                                           boolean updateTransactionIdFile,
                                           boolean shouldSucceed) 
      throws Exception {
    // start a cluster 
    Configuration conf = new Configuration();
    MiniDFSCluster cluster = null;
    cluster = new MiniDFSCluster(conf, NUM_DATA_NODES, true, null);
    cluster.shutdown();
    
    Collection<File> editsDirs = cluster.getNameEditsDirs(0);
    for (File f : editsDirs) {
      URI uri = f.toURI();
      File dir = new File(uri.getPath());
      File currentDir = new File(dir, "current");
      // We should start with only the finalized edits_0-1
      GenericTestUtils.assertGlobEquals(currentDir, "edits_.*",
          NNStorage.getFinalizedEditsFileName(0, 1));
      // Make a truncated edits_2_inprogress
      File log = new File(currentDir,
          NNStorage.getInProgressEditsFileName(2));
      NNStorage storage = new NNStorage(conf, 
                                        Collections.<URI>emptyList(),
                                        Lists.newArrayList(uri), null);
      if (updateTransactionIdFile) {
        storage.writeTransactionIdFileToStorage(2, null);
      }
      storage.close();

      new EditLogFileOutputStream(log, null).create();
      if (!inBothDirs) {
        break;
      }
    }
    
    try {
      cluster = new MiniDFSCluster(conf, NUM_DATA_NODES, false, null);
      if (!shouldSucceed) {
        fail("Should not have succeeded in startin cluster");
      }
    } catch (IOException ioe) {
      if (shouldSucceed) {
        LOG.info("Should have succeeded in starting cluster, but failed", ioe);
        throw ioe;
      } else {
        GenericTestUtils.assertExceptionContains(
          "Gap in transactions. Expected to be able to read up until " +
          "at least txid 2 but unable to find any edit logs containing " +
          "txid 2", ioe);
      }
    } finally {
      if (cluster != null)
        cluster.shutdown();
    }
  }

  
  private static class EditLogByteInputStream extends EditLogInputStream {
    private InputStream input;
    private long len;
    private int version;
    private FSEditLogOp.Reader reader = null;
    private FSEditLogLoader.PositionTrackingInputStream tracker = null;

    public EditLogByteInputStream(byte[] data) throws IOException {
      len = data.length;
      input = new ByteArrayInputStream(data);

      BufferedInputStream bin = new BufferedInputStream(input);
      DataInputStream in = new DataInputStream(bin);      
      tracker = new FSEditLogLoader.PositionTrackingInputStream(in);
      in = new DataInputStream(tracker);  
      version = EditLogFileInputStream.readLogVersion(in);           
      reader = new FSEditLogOp.Reader(in, version);
    }
  
    @Override
    public long getFirstTxId() {
      return HdfsConstants.INVALID_TXID;
    }
    
    @Override
    public long getLastTxId() {
      return HdfsConstants.INVALID_TXID;
    }
  
    @Override
    public long length() throws IOException {
      return len;
    }
  
    @Override
    public long getPosition() {
      return tracker.getPos();
    }

    @Override
    public FSEditLogOp nextOp() throws IOException {
      return reader.readOp(false);
    }

    @Override
    public int getVersion() throws IOException {
      return version;
    }

    @Override
    public void close() throws IOException {
      input.close();
    }

    @Override // JournalStream
    public String getName() {
      return "AnonEditLogByteInputStream";
    }
    
    @Override // JournalStream
    public JournalType getType() {
      return JournalType.FILE;
    }

    @Override
    public void position(long position) throws IOException {
    }

    @Override
    public void refresh(long position, long skippedUntilTxid)
        throws IOException {
    }

    @Override
    public long getReadChecksum() {
      return 0;
    }
  }

  @Test
  public void testFailedOpen() throws Exception {
    File logDir = new File(TEST_DIR, "testFailedOpen");
    logDir.mkdirs();
    FSEditLog log = FSImageTestUtil.createStandaloneEditLog(logDir);
    try {
      logDir.setWritable(false);
      log.open();
      fail("Did no throw exception on only having a bad dir");
    } catch (IOException ioe) {
      GenericTestUtils.assertExceptionContains(
          "no journals successfully started", ioe);
    } finally {
      logDir.setWritable(true);
      log.close();
    }
  }

  /**
   * Tests the getEditLogManifest function using mock storage for a number
   * of different situations.
   */
  @Test
  public void testEditLogManifestMocks() throws IOException {
    NNStorage storage;
    FSEditLog log;
    // Simple case - different directories have the same
    // set of logs, with an in-progress one at end
    storage = mockStorageWithEdits(
        "[1,100]|[101,200]|[201,]",
        "[1,100]|[101,200]|[201,]");
    log = createEditLog(storage);;
    assertEquals("[[1,100,true], [101,200,true], [201,-1,false]]",
        log.getEditLogManifest(1).toString());
    assertEquals("[[101,200,true], [201,-1,false]]",
        log.getEditLogManifest(101).toString());

    // Another simple case, different directories have different
    // sets of files
    storage = mockStorageWithEdits(
        "[1,100]|[101,200]",
        "[1,100]|[201,300]|[301,400]"); // nothing starting at 101
    log = createEditLog(storage);;
    assertEquals("[[1,100,true], [101,200,true], [201,300,true], [301,400,true]]",
        log.getEditLogManifest(1).toString());
    
    // Case where one directory has an earlier finalized log, followed
    // by a gap. The returned manifest should start after the gap.
    storage = mockStorageWithEdits(
        "[1,100]|[301,400]", // gap from 101 to 300
        "[301,400]|[401,500]");
    log = createEditLog(storage);;
    assertEquals("[[301,400,true], [401,500,true]]",
        log.getEditLogManifest(1).toString());
    
    // Case where different directories have different length logs
    // starting at the same txid - should pick the longer one
    storage = mockStorageWithEdits(
        "[1,100]|[101,150]", // short log at 101
        "[1,50]|[101,200]"); // short log at 1
    log = createEditLog(storage);;
    assertEquals("[[1,100,true], [101,200,true]]",
        log.getEditLogManifest(1).toString());
    assertEquals("[[101,200,true]]",
        log.getEditLogManifest(101).toString());

    // Case where the first storage has an inprogress while
    // the second has finalised that file (i.e. the first failed
    // recently)
    storage = mockStorageWithEdits(
        "[1,100]|[101,]", 
        "[1,100]|[101,200]"); 
    log = createEditLog(storage);;
    assertEquals("[[1,100,true], [101,200,true]]",
        log.getEditLogManifest(1).toString());
    assertEquals("[[101,200,true]]",
        log.getEditLogManifest(101).toString());
  }
  
  /**
   * Create a mock NNStorage object with several directories, each directory
   * holding edit logs according to a specification. Each directory
   * is specified by a pipe-separated string. For example:
   * <code>[1,100]|[101,200]</code> specifies a directory which
   * includes two finalized segments, one from 1-100, and one from 101-200.
   * The syntax <code>[1,]</code> specifies an in-progress log starting at
   * txid 1.
   */
  private NNStorage mockStorageWithEdits(String... editsDirSpecs) throws IOException {
    List<StorageDirectory> sds = Lists.newArrayList();
    List<URI> uris = Lists.newArrayList();

    NNStorage storage = Mockito.mock(NNStorage.class);
    for (String dirSpec : editsDirSpecs) {
      List<String> files = Lists.newArrayList();
      String[] logSpecs = dirSpec.split("\\|");
      for (String logSpec : logSpecs) {
        Matcher m = Pattern.compile("\\[(\\d+),(\\d+)?\\]").matcher(logSpec);
        assertTrue("bad spec: " + logSpec, m.matches());
        if (m.group(2) == null) {
          files.add(NNStorage.getInProgressEditsFileName(
              Long.valueOf(m.group(1))));
        } else {
          files.add(NNStorage.getFinalizedEditsFileName(
              Long.valueOf(m.group(1)),
              Long.valueOf(m.group(2))));
        }
      }
      StorageDirectory sd = FSImageTestUtil.mockStorageDirectory(
          NameNodeDirType.EDITS, false,
          files.toArray(new String[0]));
      sds.add(sd);
      URI u = URI.create("file:///storage"+ Math.random());
      Mockito.doReturn(sd).when(storage).getStorageDirectory(u);
      uris.add(u);
    }    

    Mockito.doReturn(sds).when(storage).dirIterable(NameNodeDirType.EDITS);
    Mockito.doReturn(uris).when(storage).getEditsDirectories();
    return storage;
  }

  /** 
   * Specification for a failure during #setupEdits
   */
  static class AbortSpec {
    final int roll;
    final int logindex;
    
    /**
     * Construct the failure specification. 
     * @param roll number to fail after. e.g. 1 to fail after the first roll
     * @param loginfo index of journal to fail. 
     */
    AbortSpec(int roll, int logindex) {
      this.roll = roll;
      this.logindex = logindex;
    }
  }

  final static int TXNS_PER_ROLL = 10;  
  final static int TXNS_PER_FAIL = 2;
    
  /**
   * Set up directories for tests. 
   *
   * Each rolled file is 10 txns long. 
   * A failed file is 2 txns long.
   * 
   * @param editUris directories to create edit logs in
   * @param numrolls number of times to roll the edit log during setup
   * @param abortAtRolls Specifications for when to fail, see AbortSpec
   * @throws IOException 
   */
  public static NNStorage setupEdits(List<URI> editUris, int numrolls, 
      AbortSpec... abortAtRolls) throws IOException {
    return setupEdits(editUris, numrolls, true, abortAtRolls);
  }
  
  private static FSEditLog createEditLog(NNStorage storage) throws IOException {
    Configuration conf = new Configuration();
    // Make sure the edits dirs are set in the provided configuration object.
    conf.set(FSConstants.DFS_NAMENODE_EDITS_DIR_KEY,
        org.apache.commons.lang.StringUtils.join(storage.getEditsDirectories(),
            ","));
    return new FSEditLog(conf, null, storage,
        NNStorageConfiguration.getNamespaceDirs(conf),
        NNStorageConfiguration.getNamespaceEditsDirs(conf), null);
  }
  
  public static NNStorage setupEdits(List<URI> editUris, int numrolls, boolean closeOnFinish,
                                     AbortSpec... abortAtRolls)
      throws IOException {
    List<AbortSpec> aborts = new ArrayList<AbortSpec>(Arrays.asList(abortAtRolls));
    NNStorage storage = new NNStorage(new Configuration(),
                                      Collections.<URI>emptyList(),
                                      editUris, null);
    storage.format();
    FSEditLog editlog = createEditLog(storage);;    
    // open the edit log and add two transactions
    // logGenerationStamp is used, simply because it doesn't 
    // require complex arguments.
    editlog.open();
    for (int i = 2; i < TXNS_PER_ROLL; i++) {
      editlog.logGenerationStamp((long)0);
    }
    editlog.logSync();
    
    // Go into edit log rolling loop.
    // On each roll, the abortAtRolls abort specs are 
    // checked to see if an abort is required. If so the 
    // the specified journal is aborted. It will be brought
    // back into rotation automatically by rollEditLog
    for (int i = 0; i < numrolls; i++) {
      editlog.rollEditLog();
      
      editlog.logGenerationStamp((long)i);
      editlog.logSync();

      while (aborts.size() > 0 
             && aborts.get(0).roll == (i+1)) {
        AbortSpec spec = aborts.remove(0);
        editlog.getJournals().get(spec.logindex).abort();
      } 
      
      for (int j = 3; j < TXNS_PER_ROLL; j++) {
        editlog.logGenerationStamp((long)i);
      }
      editlog.logSync();
    }
    if (closeOnFinish) {
      editlog.close();
    }

    FSImageTestUtil.logStorageContents(LOG, storage);
    return storage;
  }

  /** 
   * Test loading an editlog which has had both its storage fail
   * on alternating rolls. Two edit log directories are created.
   * The first on fails on odd rolls, the second on even. Test
   * that we are able to load the entire editlog regardless.
   */
  @Test
  public void testAlternatingJournalFailure() throws IOException {
    File f1 = new File(TEST_DIR + "/alternatingjournaltest0");
    File f2 = new File(TEST_DIR + "/alternatingjournaltest1");

    List<URI> editUris = ImmutableList.of(f1.toURI(), f2.toURI());
    
    NNStorage storage = setupEdits(editUris, 10,
                                   new AbortSpec(1, 0),
                                   new AbortSpec(2, 1),
                                   new AbortSpec(3, 0),
                                   new AbortSpec(4, 1),
                                   new AbortSpec(5, 0),
                                   new AbortSpec(6, 1),
                                   new AbortSpec(7, 0),
                                   new AbortSpec(8, 1),
                                   new AbortSpec(9, 0),
                                   new AbortSpec(10, 1));
    long totaltxnread = 0;
    FSEditLog editlog = createEditLog(storage);;
    long startTxId = 0;
    
    // we need to finalize the inprogress segments (this is done on startup)
    editlog.recoverUnclosedStreams();
    
    List<EditLogInputStream> editStreams = new ArrayList<EditLogInputStream>();
    editlog.selectInputStreams(editStreams, startTxId, TXNS_PER_ROLL*11 - 1, 0);

    for (EditLogInputStream edits : editStreams) {
      FSEditLogLoader.EditLogValidation val = FSEditLogLoader.validateEditLog(edits);
      long read = val.getNumTransactions();
      LOG.info("Loading edits " + edits + " read " + read);
      assertEquals(startTxId, val.getStartTxId());
      startTxId += read;
      totaltxnread += read;
    }

    editlog.close();
    storage.close();
    assertEquals(TXNS_PER_ROLL*11, totaltxnread);    
  }

  /** 
   * Test loading an editlog with gaps. A single editlog directory
   * is set up. On of the edit log files is deleted. This should
   * fail when selecting the input streams as it will not be able 
   * to select enough streams to load up to 4*TXNS_PER_ROLL.
   * There should be 4*TXNS_PER_ROLL transactions as we rolled 3
   * times. 
   */
  @Test
  public void testLoadingWithGaps() throws IOException {
    File f1 = new File(TEST_DIR + "/gaptest0");
    List<URI> editUris = ImmutableList.of(f1.toURI());

    NNStorage storage = setupEdits(editUris, 3);
    
    final long startGapTxId = 1*TXNS_PER_ROLL;
    final long endGapTxId = 2*TXNS_PER_ROLL - 1;

    File[] files = new File(f1, "current").listFiles(new FilenameFilter() {
        public boolean accept(File dir, String name) {
          if (name.startsWith(NNStorage.getFinalizedEditsFileName(startGapTxId, 
                                  endGapTxId))) {
            return true;
          }
          return false;
        }
      });
    assertEquals(1, files.length);
    assertTrue(files[0].delete());
    
    FSEditLog editlog = createEditLog(storage);;
    long startTxId = 0;
    try {
      List<EditLogInputStream> editStreams = new ArrayList<EditLogInputStream>();
      editlog.selectInputStreams(editStreams, startTxId, 4*TXNS_PER_ROLL, 0);
      fail("Should have thrown exception");
    } catch (IOException ioe) {
      GenericTestUtils.assertExceptionContains(
          "Gap in transactions. Expected to be able to read up until " +
          "at least txid 40 but unable to find any edit logs containing " +
          "txid 10", ioe);
    }
  }

  /**
   * Dummy name node for testAutomaticRoll().
   */
  class FakeNameNode implements AutoEditsRollerInterface {
    FSEditLog editlog;
    volatile boolean isRunning = true;

    @Override
    public void rollEditLogAdmin()
        throws IOException {
      editlog.rollEditLog();
    }

    @Override
    public boolean isNamesystemRunning() {
      return isRunning;
    }

    @Override
    public boolean isNamesystemInitialized() {
      return true;
    }
  }

  
  /**
   * Create an FSEditLog and apply an automatic edits roller to it
   * Add some dummy transactions for a couple of seconds and make
   * sure edits are automatically rolled reasonablly nubmer of times.
   * @throws IOException
   * @throws InterruptedException
   */
  @Test
  public void testAutomaticRoll() throws IOException, InterruptedException {
    File f1 = new File(TEST_DIR + "/testAutomaticRoll");
    List<URI> editUris = ImmutableList.of(f1.toURI());

    NNStorage storage = setupEdits(editUris, 3);

    storage.format();
    Configuration conf = new Configuration();
    conf.setLong(FSEditLog.CONF_ROLL_TIMEOUT_MSEC, 200);
    FakeNameNode fakeNN = new FakeNameNode();
    AutomaticEditsRoller aer = new AutomaticEditsRoller(fakeNN);

    FSImage fakeImage = new FSImage(conf);
    FSNamesystem fakeFSN = new FSNamesystem();
    fakeFSN.automaticEditsRoller = aer;
    fakeFSN.automaticEditsRollingThread = new Daemon(aer);
    fakeFSN.automaticEditsRollingThread.start();
    fakeImage.setFSNamesystem(fakeFSN);

    FSEditLog editlog = new FSEditLog(conf, fakeImage, storage, null, editUris,
        null);
    fakeNN.editlog = editlog;
    
    Daemon daemon = new Daemon(aer);
    daemon.start();
    
    editlog.open();
    
    for (int i = 0 ; i < 200; i++) {
      for (int j = 0; j < 5; j++) {
        editlog.logGenerationStamp(0);
        Thread.sleep(1);
      }
      editlog.logSync();
    }
    DFSTestUtil.waitNMilliSecond(200);
    
    File[] files = new File(f1, "current").listFiles(new FilenameFilter() {
        public boolean accept(File dir, String name) {
          if (name.startsWith(NameNodeFile.EDITS.getName())) {
            return true;
          }
          return false;
        }
      });
    assertTrue("Number of files " + files.length, files.length > 5);
    
    editlog.close();
    
    aer.stop();
    
    for (int i = 0; i < 500; i++) {
      if (daemon.isAlive()) {
        break;
      } else if (i == 499) {
        TestCase.fail();
      }
      DFSTestUtil.waitNMilliSecond(10);
    }
  }
}
