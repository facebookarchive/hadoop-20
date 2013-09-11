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

import junit.framework.TestCase;
import java.io.*;
import java.util.ArrayList;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.metrics.DFSClientMetrics;
import org.apache.hadoop.hdfs.metrics.DFSQuorumReadMetrics;
import org.apache.hadoop.hdfs.server.common.HdfsConstants;
import org.apache.hadoop.hdfs.server.datanode.SimulatedFSDataset;
import org.apache.hadoop.hdfs.util.InjectionEvent;
import org.apache.hadoop.util.InjectionEventI;
import org.apache.hadoop.util.InjectionHandler;

/**
 * This class tests the DFS positional read functionality in a single node
 * mini-cluster.
 */
public class TestPread extends TestCase {
  static final long seed = 0xDEADBEEFL;
  static final int blockSize = 4096;
  boolean simulatedStorage = false;

  private void writeFile(FileSystem fileSys, Path name) throws IOException {
    int replication = 3;// We need > 1 blocks to test out the quorum reads.
    // create and write a file that contains three blocks of data
    DataOutputStream stm = fileSys.create(name, true, 4096, (short)replication,
                                          (long)blockSize);
    // test empty file open and read
    stm.close();
    FSDataInputStream in = fileSys.open(name);
    byte[] buffer = new byte[(int)(12*blockSize)];
    in.readFully(0, buffer, 0, 0);
    IOException res = null;
    try { // read beyond the end of the file
      in.readFully(0, buffer, 0, 1);
    } catch (IOException e) {
      // should throw an exception
      res = e;
    }
    assertTrue("Error reading beyond file boundary.", res != null);
    in.close();
    if (!fileSys.delete(name, true))
      assertTrue("Cannot delete file", false);
    
    // now create the real file
    stm = fileSys.create(name, true, 4096, (short)replication, (long)blockSize);
    Random rand = new Random(seed);
    rand.nextBytes(buffer);
    stm.write(buffer);
    stm.close();
  }
  
  private void checkAndEraseData(byte[] actual, int from, byte[] expected, String message) {
    for (int idx = 0; idx < actual.length; idx++) {
      assertEquals(message+" byte "+(from+idx)+" differs. expected "+
                        expected[from+idx]+" actual "+actual[idx],
                        actual[idx], expected[from+idx]);
      actual[idx] = 0;
    }
  }
  
  private void doPread(FSDataInputStream stm, long position, byte[] buffer,
                       int offset, int length) throws IOException {
    int nread = 0;
    while (nread < length) {
      int nbytes = stm.read(position+nread, buffer, offset+nread, length-nread);
      assertTrue("Error in pread", nbytes > 0);
      nread += nbytes;
    }
  }
  
  private void pReadFile(FileSystem fileSys, Path name) throws IOException {
    FSDataInputStream stm = fileSys.open(name);
    byte[] expected = new byte[(int)(12*blockSize)];
    if (simulatedStorage) {
      for (int i= 0; i < expected.length; i++) {  
        expected[i] = SimulatedFSDataset.DEFAULT_DATABYTE;
      }
    } else {
      Random rand = new Random(seed);
      rand.nextBytes(expected);
    }
    // do a sanity check. Read first 4K bytes
    byte[] actual = new byte[4096];
    stm.readFully(actual);
    checkAndEraseData(actual, 0, expected, "Read Sanity Test");
    // now do a pread for the first 8K bytes
    actual = new byte[8192];
    doPread(stm, 0L, actual, 0, 8192);
    checkAndEraseData(actual, 0, expected, "Pread Test 1");
    // Now check to see if the normal read returns 4K-8K byte range
    actual = new byte[4096];
    stm.readFully(actual);
    checkAndEraseData(actual, 4096, expected, "Pread Test 2");
    // Now see if we can cross a single block boundary successfully
    // read 4K bytes from blockSize - 2K offset
    stm.readFully(blockSize - 2048, actual, 0, 4096);
    checkAndEraseData(actual, (int)(blockSize-2048), expected, "Pread Test 3");
    // now see if we can cross two block boundaries successfully
    // read blockSize + 4K bytes from blockSize - 2K offset
    actual = new byte[(int)(blockSize+4096)];
    stm.readFully(blockSize - 2048, actual);
    checkAndEraseData(actual, (int)(blockSize-2048), expected, "Pread Test 4");
    // now see if we can cross two block boundaries that are not cached
    // read blockSize + 4K bytes from 10*blockSize - 2K offset
    actual = new byte[(int)(blockSize+4096)];
    stm.readFully(10*blockSize - 2048, actual);
    checkAndEraseData(actual, (int)(10*blockSize-2048), expected, "Pread Test 5");
    // now check that even after all these preads, we can still read
    // bytes 8K-12K
    actual = new byte[4096];
    stm.readFully(actual);
    checkAndEraseData(actual, 8192, expected, "Pread Test 6");
    // done
    stm.close();
    // check block location caching
    stm = fileSys.open(name);
    stm.readFully(1, actual, 0, 4096);
    stm.readFully(4*blockSize, actual, 0, 4096);
    stm.readFully(7*blockSize, actual, 0, 4096);
    actual = new byte[3*4096];
    stm.readFully(0*blockSize, actual, 0, 3*4096);
    checkAndEraseData(actual, 0, expected, "Pread Test 7");
    actual = new byte[8*4096];
    stm.readFully(3*blockSize, actual, 0, 8*4096);
    checkAndEraseData(actual, 3*blockSize, expected, "Pread Test 8");
    // read the tail
    stm.readFully(11*blockSize+blockSize/2, actual, 0, blockSize/2);
    IOException res = null;
    try { // read beyond the end of the file
      stm.readFully(11*blockSize+blockSize/2, actual, 0, blockSize);
    } catch (IOException e) {
      // should throw an exception
      res = e;
    }
    assertTrue("Error reading beyond file boundary.", res != null);
    
    stm.close();
  }

  // test pread can survive datanode restarts
  private void datanodeRestartTest(MiniDFSCluster cluster, FileSystem fileSys,
      Path name) throws IOException {
    // skip this test if using simulated storage since simulated blocks
    // don't survive datanode restarts.
    if (simulatedStorage) {
      return;
    }
    int numBlocks = 1;
    assertTrue(numBlocks <= DFSClient.MAX_BLOCK_ACQUIRE_FAILURES);
    byte[] expected = new byte[numBlocks * blockSize];
    Random rand = new Random(seed);
    rand.nextBytes(expected);
    byte[] actual = new byte[numBlocks * blockSize];
    FSDataInputStream stm = fileSys.open(name);
    // read a block and get block locations cached as a result
    stm.readFully(0, actual);
    checkAndEraseData(actual, 0, expected, "Pread Datanode Restart Setup");
    // restart all datanodes. it is expected that they will
    // restart on different ports, hence, cached block locations
    // will no longer work.
    assertTrue(cluster.restartDataNodes());
    cluster.waitActive();
    // verify the block can be read again using the same InputStream
    // (via re-fetching of block locations from namenode). there is a
    // 3 sec sleep in chooseDataNode(), which can be shortened for
    // this test if configurable.
    stm.readFully(0, actual);
    checkAndEraseData(actual, 0, expected, "Pread Datanode Restart Test");
  }
  
  private void cleanupFile(FileSystem fileSys, Path name) throws IOException {
    assertTrue(fileSys.exists(name));
    assertTrue(fileSys.delete(name, true));
    assertTrue(!fileSys.exists(name));
  }
  
  /**
   * Tests positional read in DFS.
   */
  public void testPreadDFS() throws IOException {
    Configuration conf = new Configuration();
    dfsPreadTest(conf, false); //normal pread
    dfsPreadTest(conf, true); //trigger read code path without transferTo.
  }
  
  /**
   * Tests positional read in DFS, with quorum reads enabled.
   */
  public void testQuorumPreadDFSBasic() throws IOException {
    Configuration conf = new Configuration();
    conf.setInt(HdfsConstants.DFS_DFSCLIENT_QUORUM_READ_THREADPOOL_SIZE, 5);
    conf.setLong(HdfsConstants.DFS_DFSCLIENT_QUORUM_READ_THRESHOLD_MILLIS, 100);
    dfsPreadTest(conf, false); //normal pread
    dfsPreadTest(conf, true); //trigger read code path without transferTo.
  }
  
  public void testMaxOutQuorumPool() throws IOException, InterruptedException, ExecutionException {
    Configuration conf = new Configuration();
    int numQuorumPoolThreads = 5;
    conf.setBoolean("dfs.client.metrics.enable", true);
    conf.setInt(HdfsConstants.DFS_DFSCLIENT_QUORUM_READ_THREADPOOL_SIZE, numQuorumPoolThreads);
    conf.setLong(HdfsConstants.DFS_DFSCLIENT_QUORUM_READ_THRESHOLD_MILLIS, 100);
    
    // Set up the InjectionHandler
    // make preads sleep for 60ms
    InjectionHandler.set(new InjectionHandler() {
      public void _processEvent(InjectionEventI event, Object... args) {
        if(event == InjectionEvent.DFSCLIENT_START_FETCH_FROM_DATANODE) {
          try {
            Thread.sleep(60);
          } catch (InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
          }
        }
      }
    });
    
    MiniDFSCluster cluster = new MiniDFSCluster(conf, 3, true, null);
    DistributedFileSystem fileSys = (DistributedFileSystem)cluster.getFileSystem();
    DFSClient dfsClient = fileSys.getClient();
    DFSQuorumReadMetrics metrics = dfsClient.quorumReadMetrics;
    
    try {
      Path file1 = new Path("quorumReadMaxOut.dat");
      writeFile(fileSys, file1);
      
      // time the pReadFile test
      long t0, t1;
      /* 
       * Basic test. Reads complete within timeout. 
       * Assert that there were no quorum reads.
       * 
       */
      t0 = System.currentTimeMillis();
      pReadFile(fileSys, file1);
      t1 = System.currentTimeMillis();
      long pReadTestTime = t1 - t0;
      // assert that there were no quorum reads. 60ms + delta < 100ms
      assertTrue(metrics.getParallelReadOps() == 0);
      assertTrue(metrics.getParallelReadOpsInCurThread() == 0);
      
      // set the timeout to 50ms;
      /*
       * Reads take longer than timeout. But, only one thread reading.
       * 
       * Assert that there were quorum reads. But, none of the
       * reads had to run in the current thread.
       */
      dfsClient.setQuorumReadTimeout(50); // 50ms
      t0 = System.currentTimeMillis();
      pReadFile(fileSys, file1);
      t1 = System.currentTimeMillis();
      // assert that there were quorum reads
      long pReadTestTimeNew = t1 - t0;
      // assert that there were no quorum reads. 60ms + delta < 100ms
      assertTrue(metrics.getParallelReadOps() > 0);
      assertTrue(metrics.getParallelReadOpsInCurThread() == 0);
      
      /*
       * Multiple threads reading. Reads take longer than timeout.
       * 
       * Assert that there were quorum reads. And that
       * reads had to run in the current thread.
       */
      int factor = 10;
      int numParallelReads = numQuorumPoolThreads * factor;
      long initialReadOpsValue = metrics.getParallelReadOps();
      ExecutorService executor = Executors.newFixedThreadPool(numParallelReads);
      ArrayList<Future<Void>> futures = new ArrayList<Future<Void>>();
      for (int i = 0; i < numParallelReads; i++) {
        futures.add(executor.submit(getPReadFileCallable(fileSys, file1)));
      }
      for (int i = 0; i < numParallelReads; i++) {
        futures.get(i).get();
      }
      assertTrue(metrics.getParallelReadOps() > initialReadOpsValue);
      assertTrue(metrics.getParallelReadOpsInCurThread() > 0);
      
      cleanupFile(fileSys, file1);
    } finally {
      fileSys.close();
      cluster.shutdown();
    }
  }
  
  private Callable<Void> getPReadFileCallable(final FileSystem fileSys, final Path file) {
    return new Callable<Void>() {
      public Void call() throws IOException {
        pReadFile(fileSys, file);
        return null;
      }
    };
  }
  
  private void dfsPreadTest(Configuration conf, boolean disableTransferTo) throws IOException {
    conf.setLong("dfs.block.size", 4096);
    conf.setLong("dfs.read.prefetch.size", 4096);
    if (simulatedStorage) {
      conf.setBoolean("dfs.datanode.simulateddatastorage", true);
    }
    if (disableTransferTo) {
      conf.setBoolean("dfs.datanode.transferTo.allowed", false);
    }
    MiniDFSCluster cluster = new MiniDFSCluster(conf, 3, true, null);
    FileSystem fileSys = cluster.getFileSystem();
    try {
      Path file1 = new Path("preadtest.dat");
      writeFile(fileSys, file1);
      pReadFile(fileSys, file1);
      datanodeRestartTest(cluster, fileSys, file1);
      cleanupFile(fileSys, file1);
    } finally {
      fileSys.close();
      cluster.shutdown();
    }
  }
  
  public void testPreadDFSSimulated() throws IOException {
    simulatedStorage = true;
    testPreadDFS();
    simulatedStorage = false;
  }
  
  /**
   * Tests positional read in LocalFS.
   */
  public void testPreadLocalFS() throws IOException {
    Configuration conf = new Configuration();
    FileSystem fileSys = FileSystem.getLocal(conf);
    try {
      Path file1 = new Path("build/test/data", "preadtest.dat");
      writeFile(fileSys, file1);
      pReadFile(fileSys, file1);
      cleanupFile(fileSys, file1);
    } finally {
      fileSys.close();
    }
  }

  public static void main(String[] args) throws Exception {
    new TestPread().testPreadDFS();
  }
}
