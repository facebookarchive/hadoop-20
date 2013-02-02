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
import java.nio.ByteBuffer;
import java.util.Random;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.server.datanode.SimulatedFSDataset;

/**
 * This class tests the DFS positional scatter-gather read functionality 
 * in a single node mini-cluster.
 */
public class TestScatterGather extends TestCase {
  static final long seed = 0xDEADBEEFL;
  static final int blockSize = 4096;
  boolean simulatedStorage = false;
  boolean shortCircuitRead = false;

  private void writeFile(FileSystem fileSys, Path name) throws IOException {
    // create and write a file that contains three blocks of data
    DataOutputStream stm = fileSys.create(name, true, 4096, (short)1,
                                          (long)blockSize);
    // test empty file open and read
    stm.close();
    FSDataInputStream in = fileSys.open(name);
    byte[] buffer = new byte[(int)(12*blockSize)];
    in.readFully(0, buffer, 0, 0);
    List<ByteBuffer> rlist = in.readFullyScatterGather(0, 0);

    IOException res = null;
    try { // read beyond the end of the file
      in.readFully(0, buffer, 0, 1);
      rlist = in.readFullyScatterGather(0, 1);
    } catch (IOException e) {
      // should throw an exception
      res = e;
    }
    assertTrue("Error reading beyond file boundary.", res != null);
    in.close();
    if (!fileSys.delete(name, true))
      assertTrue("Cannot delete file", false);
    
    // now create the real file
    stm = fileSys.create(name, true, 4096, (short)1, (long)blockSize);
    Random rand = new Random(seed);
    rand.nextBytes(buffer);
    stm.write(buffer);
    stm.close();
  }

  // convert from a List of bytebuffers to a primitive byte array
  private void checkAndEraseData(List<ByteBuffer> actual, int total,
    int from, byte[] expected, String message) {
    int count = 0;
    for (int i = 0; i < actual.size(); i++) {
      count += actual.get(i).remaining();
    }
    assertEquals(message + " expected " + total + " bytes " +
                 " but found only " + count + " bytes.",
                 total, count);
   
    byte[] result = new byte[count];
    int offset = 0;
    for (int i = 0; i < actual.size(); i++) {
      count += actual.get(i).remaining();
      ByteBuffer b = actual.get(i);
      int length = b.remaining();
      b.get(result, offset, length);
      offset += length; 
    }
    checkAndEraseData(result, from, expected, message);
  }
  
  private void checkAndEraseData(byte[] actual, int from, byte[] expected, 
    String message) {
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
    // do a sanity check. pread the first 4K bytes
    List<ByteBuffer> rlist = stm.readFullyScatterGather(0, 4096);
    checkAndEraseData(rlist, 4096, 0, expected, "Read Sanity Test");

    // now do a pread for the first 8K bytes
    byte[] actual = new byte[8192];
    doPread(stm, 0L, actual, 0, 8192);
    checkAndEraseData(actual, 0, expected, "Pread Test 1");

    // Now check to see if the normal read returns 0K-8K byte range
    actual = new byte[8192];
    stm.readFully(actual);
    checkAndEraseData(actual, 0, expected, "Pread Test 2");

    // Now see if we can cross a single block boundary successfully
    // read 4K bytes from blockSize - 2K offset
    rlist = stm.readFullyScatterGather(blockSize - 2048, 4096);
    checkAndEraseData(rlist, 4096, (int)(blockSize-2048), expected, "Pread Test 3");

    // now see if we can cross two block boundaries successfully
    // read blockSize + 4K bytes from blockSize - 2K offset
    int size = (int)(blockSize+4096);
    rlist = stm.readFullyScatterGather(blockSize - 2048, size);
    checkAndEraseData(rlist, size, (int)(blockSize-2048), expected, "Pread Test 4");

    // now see if we can cross two block boundaries that are not cached
    // read blockSize + 4K bytes from 10*blockSize - 2K offset
    size = (int)(blockSize+4096);
    rlist = stm.readFullyScatterGather(10*blockSize - 2048, size);
    checkAndEraseData(rlist, size, (int)(10*blockSize-2048), expected, "Pread Test 5");

    // now check that even after all these preads, we can still read
    // bytes 8K-12K
    actual = new byte[4096];
    stm.readFully(actual);
    checkAndEraseData(actual, 8192, expected, "Pread Test 6");

    // pread beyond the end of the file. It should return the last half block.
    size = blockSize/2;
    rlist = stm.readFullyScatterGather(11*blockSize+size, blockSize);
    checkAndEraseData(rlist, size, (int)(11*blockSize+size), expected, "Pread Test 5");

    IOException res = null;
    try { // normal read beyond the end of the file
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
  
  private void testPreadDFSInternal(boolean inlineChecksum) throws IOException {
    Configuration conf = new Configuration();
    conf.setBoolean("dfs.use.inline.checksum", inlineChecksum);
    conf.setLong("dfs.block.size", 4096);
    conf.setLong("dfs.read.prefetch.size", 4096);
    if (simulatedStorage) {
      conf.setBoolean("dfs.datanode.simulateddatastorage", true);
    }
    if (shortCircuitRead) {
      conf.setBoolean("dfs.read.shortcircuit", true);
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
  
  /**
   * Tests positional read in DFS.
   */
  public void testPreadDFS() throws IOException {
    testPreadDFSInternal(true);
    testPreadDFSInternal(false);
  }

  /**
   * Tests positional read in DFS.
   */
  public void testPreadShortCircuitRead() throws IOException {
    shortCircuitRead = true;
    testPreadDFSInternal(false);
    testPreadDFSInternal(true);
    shortCircuitRead = false;
  }
 
  public static void main(String[] args) throws Exception {
    new TestScatterGather().testPreadDFS();
  }
}
