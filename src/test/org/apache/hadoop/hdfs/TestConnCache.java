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
import java.net.InetSocketAddress;
import java.net.Socket;

import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSClientReadProfilingData;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Matchers;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import static org.mockito.Mockito.spy;

public class TestConnCache {
  static final Log LOG = LogFactory.getLog(TestConnCache.class);

  static final int BLOCK_SIZE = 4096;
  static final int FILE_SIZE = 3 * BLOCK_SIZE;

  static final int NUM_DATANODES = 3;

  static Configuration conf = null;
  static MiniDFSCluster cluster = null;
  static FileSystem fs = null;

  static final Path testFile = new Path("/testConnCache.dat");
  static byte authenticData[] = null;

  static BlockReaderTestUtil util = null;

  /**
   * A mock Answer to remember the BlockReader used.
   * 
   * It verifies that all invocation to DFSInputStream.getBlockReader() use the
   * same socket.
   */
  private class MockGetBlockReader implements Answer<BlockReader> {
    public BlockReader reader = null;
    private Socket sock = null;

    @Override
    public BlockReader answer(InvocationOnMock invocation) throws Throwable {
      BlockReader prevReader = reader;
      reader = (BlockReader) invocation.callRealMethod();
      if (sock == null) {
        sock = reader.dnSock;
      } else if (prevReader != null && prevReader.hasSentStatusCode()) {
        // Can't reuse socket if the previous BlockReader didn't read till EOS.
        assertSame("DFSInputStream should use the same socket", sock,
            reader.dnSock);
      }

      return reader;
    }
  }

  @BeforeClass
  public static void setupCluster() throws Exception {
    util = new BlockReaderTestUtil(NUM_DATANODES);
    cluster = util.getCluster();
    conf = util.getConf();
    fs = cluster.getFileSystem();

    authenticData = util.writeFile(testFile, FILE_SIZE / 1024);
  }

  /**
   * (Optionally) seek to position, read and verify data.
   * 
   * Seek to specified position if pos is non-negative.
   */
  private void pread(DFSInputStream in, long pos, byte[] buffer, int offset,
      int length) throws IOException {
    assertTrue("Test buffer too small", buffer.length >= offset + length);

    if (pos > 0) {
      in.seek(pos);
    }

    LOG.info("Reading from file of size " + in.getFileLength() + " at offset "
        + in.getPos());

    while (length > 0) {
      int cnt = in.read(buffer, offset, length);
      assertTrue("Error in read", cnt > 0);
      offset += cnt;
      length -= cnt;
    }

    // Verify
    for (int i = 0; i < length; ++i) {
      byte actual = buffer[i];
      byte expect = authenticData[(int) pos + i];
      assertEquals("Read data mismatch at file offset " + (pos + i)
          + ". Expects " + expect + "; got " + actual, actual, expect);
    }
  }

  /**
   * Test the SocketCache itself
   */
  @Test
  public void testSocketCache() throws IOException {
    final int CACHE_SIZE = 4;
    SocketCache cache = new SocketCache(CACHE_SIZE);

    // Make a client
    InetSocketAddress nnAddr = new InetSocketAddress("localhost",
        cluster.getNameNodePort());
    DFSClient client = new DFSClient(nnAddr, conf);

    // Find out the DN addr
    LocatedBlock block = client
        .getLocatedBlocks(testFile.toString(), 0, FILE_SIZE).getLocatedBlocks()
        .get(0);
    DataNode dn = util.getDataNode(block);
    InetSocketAddress dnAddr = dn.getSelfAddr();

    // Make some sockets to the DN.
    Socket[] dnSockets = new Socket[CACHE_SIZE];
    for (int i = 0; i < dnSockets.length; i++) {
      dnSockets[i] = client.socketFactory.createSocket(dnAddr.getAddress(),
          dnAddr.getPort());
    }

    // Insert a socket to the NN
    Socket nnSock = new Socket(nnAddr.getAddress(), nnAddr.getPort());
    cache.put(nnSock);
    assertSame("Read the write", nnSock, cache.get(nnAddr));
    cache.put(nnSock);

    // Insert DN socks
    for (Socket dnSock : dnSockets) {
      cache.put(dnSock);
    }

    assertEquals("NN socket evicted", null, cache.get(nnAddr));
    assertTrue("Evicted socket closed", nnSock.isClosed());

    // Look up the DN socks
    for (Socket dnSock : dnSockets) {
      assertEquals("Retrieve cached sockets", dnSock, cache.get(dnAddr));
      dnSock.close();
    }

    assertEquals("Cache is empty", 0, cache.size());
  }

  /**
   * Read a file served entirely from one DN. Seek around and read from
   * different offsets. And verify that they all use the same socket.
   */
  @Test
  public void testReadFromOneDN() throws IOException {
    LOG.info("Starting testReadFromOneDN()");
    DFSClient client = new DFSClient(new InetSocketAddress("localhost",
        cluster.getNameNodePort()), conf);

    DFSInputStream in = spy(client.open(testFile.toString()));
    LOG.info("opended " + testFile.toString());

    byte[] dataBuf = new byte[BLOCK_SIZE];

    MockGetBlockReader answer = new MockGetBlockReader();
    Mockito
        .doAnswer(answer)
        .when(in)
        .getBlockReader(Matchers.anyInt(), Matchers.anyInt(),
            (InetSocketAddress) Matchers.anyObject(), Matchers.anyString(),
            Matchers.anyLong(), Matchers.anyLong(), Matchers.anyLong(),
            Matchers.anyLong(), Matchers.anyInt(), Matchers.anyBoolean(),
            Matchers.anyString(), Matchers.anyLong(),
            Matchers.anyLong(), Matchers.eq(true), 
            (FSClientReadProfilingData) Matchers.anyObject());
    
    // Initial read
    pread(in, 0, dataBuf, 0, dataBuf.length);
    // Read again and verify that the socket is the same.
    pread(in, FILE_SIZE - dataBuf.length, dataBuf, 0, dataBuf.length);
    pread(in, 1024, dataBuf, 0, dataBuf.length);
    pread(in, -1, dataBuf, 0, dataBuf.length);    // No seek; just read
    pread(in, 64, dataBuf, 0, dataBuf.length/2);
    
    in.close();
  }
  
  @AfterClass
  public static void tearDownCluster() throws Exception {
    util.shutdown();
  }
}
