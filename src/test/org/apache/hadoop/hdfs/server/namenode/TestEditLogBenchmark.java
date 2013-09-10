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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.permission.*;

import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.Block;

/**
 * This class tests the creation and validation of a checkpoint.
 */
public class TestEditLogBenchmark extends TestCase {
  
  static final Log LOG = LogFactory.getLog(TestEditLog.class);
  
  static final int NUM_DATA_NODES = 0;

  // This test creates NUM_THREADS threads and each thread does
  // NUM_LOOPS in which it logs each type of transaction
  static final int NUM_LOOPS = 100;
  static final int NUM_THREADS = 10;

  //
  // an object that does a bunch of transactions
  //
  static class Transactions implements Runnable {
    private boolean syncEachTxn;
    private int threadid;
    FSNamesystem namesystem;
    int numLoops;
    short replication = 3;
    long blockSize = 64;

    Transactions(FSNamesystem ns, int num, int id, boolean sync) {
      namesystem = ns;
      numLoops = num;
      threadid = id;
      syncEachTxn = sync;
    }

    // add a bunch of transactions.
    public void run() {
      PermissionStatus p = namesystem.createFsOwnerPermissions(
                                          new FsPermission((short)0777));
      FSEditLog editLog = namesystem.getEditLog();

      for (int i = 0; i < numLoops; i++) {
        INodeFileUnderConstruction inode = new INodeFileUnderConstruction(
            namesystem.dir.allocateNewInodeId(), p, replication, blockSize, 0, "", "", null);
        for(int b = 0; b < 3; b++) {
          Block block = new Block(NUM_THREADS * threadid + b);
          BlocksMap.BlockInfo bi = new BlocksMap.BlockInfo(block, 3);
          try {
            inode.storage.addBlock(bi);
          } catch (IOException ioe) {
            LOG.error("Cannot add block", ioe);
          }
        }
        FsPermission perm = new FsPermission((short)0);
        try {
          String name = "/filename" + threadid + "-" + i;
          editLog.logOpenFile(name, inode); sync();
          editLog.logCloseFile(name, inode); sync();
          editLog.logDelete(name, 0); sync();
          editLog.logSetReplication(name, (short)3); sync();
          editLog.logGenerationStamp(i); sync();
          editLog.logMkDir(name, inode); sync();
          editLog.logRename(name, name, i); sync();
          editLog.logSetOwner(name, "hadoop", "hadoop"); sync();
          editLog.logSetQuota(name, 1, 1); sync();
          editLog.logTimes(name, 0, 0); sync();
          editLog.logSetPermissions(name, perm); sync();
          editLog.logConcat(name, new String[] { name, name, name }, i); 
          editLog.logMerge(name, name, "xor", new int[] { 1, 1, 1 }, i); sync();
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }
    }
    
    private void sync() {
      FSEditLog editLog = namesystem.getEditLog();
      if (syncEachTxn)
        editLog.logSync();
      else
        editLog.logSyncIfNeeded();
    }
  }


  /**
   * Tests transaction logging in dfs.
   */
  public void testEditLog() throws IOException {
    // each txn is synced
    testEditLog(2048, true);
    
    // syncs are performed when needed
    testEditLog(2048, false);
  }
  
  /**
   * Test edit log with different initial buffer size
   * 
   * @param initialSize initial edit log buffer size
   * @throws IOException
   */
  private void testEditLog(int initialSize, boolean syncEachTxn) throws IOException {

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
  
      // set small size of flush buffer
      FSEditLog.setBufferCapacity(initialSize);
      
      // Roll log so new output buffer size takes effect
      // we should now be writing to edits_inprogress_2
      fsimage.rollEditLog();
    
      // Create threads and make them run transactions concurrently.
      Thread.sleep(1000);
      LOG.info("----------------------------- START -----------------  ");
      
      long start = System.currentTimeMillis();
      Thread threadId[] = new Thread[NUM_THREADS];
      for (int i = 0; i < NUM_THREADS; i++) {
        Transactions trans = new Transactions(namesystem, NUM_LOOPS, i,
            syncEachTxn);
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
      long stop = System.currentTimeMillis();
      LOG.info("----------------------------- TIME TAKEN ----------------- : "
          + (stop - start));
      
      // Roll another time to finalize edits_inprogress_3
      fsimage.rollEditLog();
    } catch (InterruptedException e) {
      e.printStackTrace();
    } finally {
      try {
        if(fileSys != null) fileSys.close();
        if(cluster != null) cluster.shutdown();
      } catch (Throwable t) {
        LOG.error("Couldn't shut down cleanly", t);
      }
    }
  }
}
