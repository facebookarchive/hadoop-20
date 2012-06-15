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

import junit.framework.TestCase;
import java.io.*;
import java.util.Iterator;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.permission.*;

import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.server.namenode.EditLogFileInputStream;
import org.apache.hadoop.hdfs.server.common.Storage.StorageDirectory;

/**
 * This class tests the creation and validation of a checkpoint.
 */
public class TestEditLog2 extends TestCase {
  
  static final Log LOG = LogFactory.getLog(TestEditLog.class);
  
  static final int NUM_DATA_NODES = 0;

  // This test creates NUM_THREADS threads and each thread does
  // 2 * NUM_TRANSACTIONS Transactions concurrently.
  static final int NUM_TRANSACTIONS = 10;
  static final int NUM_THREADS = 10;
  

  //
  // an object that does a bunch of transactions
  //
  static class Transactions implements Runnable {
    FSNamesystem namesystem;
    int numTransactions;
    short replication = 3;
    long blockSize = 64;
    int id;

    Transactions(FSNamesystem ns, int num, int i) {
      namesystem = ns;
      numTransactions = num;
      id = i;
    }

    // add a bunch of transactions.
    public void run() {
      PermissionStatus p = namesystem.createFsOwnerPermissions(
                                          new FsPermission((short)0777));
      FSEditLog editLog = namesystem.getEditLog();

      try {
        for (int i = 0; i < numTransactions; i++) {
          INode inodeDir = new INodeDirectory("/hadoop."+i+"_"+id, p);
          editLog.logMkDir("/hadoop1."+i+"_"+id, inodeDir);
          
          
          INodeFileUnderConstruction inode1 = new INodeFileUnderConstruction(
                              p, replication, blockSize, 0, "", "", null);
          editLog.logOpenFile("/filename1." + i+"_"+id, inode1);
          editLog.logCloseFile("/filename1." + i+"_"+id, inode1);
          
          INodeFileUnderConstruction inode2 = new INodeFileUnderConstruction(
              p, replication, blockSize, 0, "", "", null);
          editLog.logOpenFile("/filename2." + i+"_"+id, inode2);
          editLog.logCloseFile("/filename2." + i+"_"+id, inode2);
          
          editLog.logGenerationStamp(i);
          editLog.logSetOwner("/filename1." + i+"_"+id, "hadoop2", "hadoop2");
          editLog.logSetPermissions("/filename1." + i+"_"+id, p.getPermission());
          editLog.logSetQuota("/hadoop1." + i+"_"+id, 100 + i, 100 + i);
          editLog.logSetReplication("/filename1." + i+"_"+id, (short)2);
          editLog.logTimes("/filename1." + i+"_"+id, i, i);
          editLog.logRename("/hadoop1."+i+"_"+id, "/hadoop2."+i+"_"+id, i);
          
          editLog.logConcat("/filename1." + i+"_"+id, new String[] {"/filename1." + i+"_"+id, "/filename2." + i+"_"+id}, i);
          editLog.logDelete("/filename1." + i+"_"+id, i+1);
          editLog.logDelete("/hadoop1." + i+"_"+id, i+1);
          editLog.logSync();
        }
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  }

  /**
   * Simple test for writing to and rolling the edit log.
   */
  public void testSimpleEditLog() throws IOException {
    // start a cluster 
    Configuration conf = new Configuration();
    MiniDFSCluster cluster = null;
    FileSystem fileSys = null;
    try {
      cluster = new MiniDFSCluster(conf, NUM_DATA_NODES, true, null);
      cluster.waitActive();
      fileSys = cluster.getFileSystem();
      final FSNamesystem namesystem = cluster.getNameNode().getNamesystem();
      FSImage fsimage = namesystem.getFSImage();
      final FSEditLog editLog = fsimage.getEditLog();

      editLog.logSetReplication("fakefile", (short) 1);
      editLog.logSync();
      editLog.rollEditLog();
      editLog.logSetReplication("fakefile", (short) 2);
      editLog.logSync();
      
      editLog.close();
    } finally {
      if(fileSys != null) fileSys.close();
      if(cluster != null) cluster.shutdown();
    }
  }

  /**
   * Tests transaction logging in dfs.
   */
  public void testMultiThreadedEditLog() throws IOException {
    testEditLog(2048);
    // force edit buffer to automatically sync on each log of edit log entry
    testEditLog(1);
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
            
      namesystem.saveNamespace(true, true);  
      FSImage fsimage = namesystem.getFSImage();
      FSEditLog editLog = fsimage.getEditLog();
  
      // set small size of flush buffer
      FSEditLog.setBufferCapacity(initialSize);
      
      // Roll log so new output buffer size takes effect
      fsimage.rollEditLog();
      namesystem.saveNamespace(true, true);
    
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
      
      namesystem.rollEditLog();
      
      
      
      long expectedTxns = (NUM_THREADS * NUM_TRANSACTIONS * 15); 
   
      // Verify that we can read in all the transactions that we have written.
      // If there were any corruptions, it is likely that the reading in
      // of these transactions will throw an exception.
      //
      for (Iterator<StorageDirectory> it = 
              fsimage.dirIterator(); it.hasNext();) {
        StorageDirectory sd = it.next();      
        File editFile = namesystem.getFSImage().getEditFile(sd);
        if(editFile == null)
          continue;
        
        cluster.getNameNode().getFSImage().getEditLog().setStartTransactionId(0);
        
        System.out.println("Verifying file: " + editFile);
        EditLogInputStream is = new EditLogFileInputStream(editFile);
        FSEditLogLoader loader = new FSEditLogLoader(namesystem);
        int numEdits = loader.loadFSEdits(is, namesystem.getEditLog().getCurrentTxId());
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
      public Void call() {
        try {
          log.logSyncAll();
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
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
      
      // Log an edit from thread A
      doLogEdit(threadA, editLog, "thread-a 1");

      // Log an edit from thread B
      doLogEdit(threadB, editLog, "thread-b 1");
      assertEquals("logging edit without syncing should do not affect txid",
        0, editLog.getLastSyncedTxId());

      // Now ask to sync edit from B, which should sync both edits.
      doCallLogSync(threadB, editLog);
      assertEquals("logSync from second thread should bump txid up to 2",
        2, editLog.getLastSyncedTxId());

      // Now ask to sync edit from A, which was already batched in - thus
      // it should increment the batch count metric
      doCallLogSync(threadA, editLog);
      assertEquals("logSync from first thread shouldn't change txid",
        2, editLog.getLastSyncedTxId());

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
        0, editLog.getLastSyncedTxId());

      // logSyncAll in Thread B
      doCallLogSyncAll(threadB, editLog);
      assertEquals("logSyncAll should sync thread A's transaction",
        1, editLog.getLastSyncedTxId());
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
}
