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

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.Random;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.net.NetUtils;

/**
 * A helper class to setup the cluster, and get to BlockReader and DataNode
 * for a block.
 */
public class BlockReaderTestUtil {

  private Configuration conf = null;
  private MiniDFSCluster cluster = null;
  
  /**
   * Setup the cluster
   */
  public BlockReaderTestUtil(int numDataNodes) throws Exception {
    this(numDataNodes, new Configuration());
  }
  
  public BlockReaderTestUtil(int numDataNodes, Configuration conf)
        throws Exception {
    this.conf = conf;
    cluster = new MiniDFSCluster(conf, numDataNodes, true, null);
    cluster.waitActive();
  }
  
  /**
   * Shutdown cluster
   */
  public void shutdown() {
    if (cluster != null) {
      cluster.shutdown();
    }
  }
  
  public MiniDFSCluster getCluster() {
    return cluster;
  }
  
  public Configuration getConf() {
    return conf;
  }
    
  /**
   * Create a file of the given size filled with random data.
   */
  public byte[] writeFile(Path filepath, int sizeKB) 
        throws IOException {
    FileSystem fs = cluster.getFileSystem();
    
    // Write a file with the specified amount of data
    FSDataOutputStream os = fs.create(filepath);
    byte data[] = new byte[1024 * sizeKB];
    new Random().nextBytes(data);
    os.write(data);
    os.close();
    return data;
  }
  
  /**
   * Get the list of blocks for a file.
   */
  public List<LocatedBlock> getFileBlocks(Path filePath, int sizeKB) 
        throws IOException {
    DFSClient dfsclient = getDFSClient();
    return dfsclient.getLocatedBlocks(filePath.toString(), 0, sizeKB * 1024)
        .getLocatedBlocks();
  }
  
  /**
   * Get the DFSClient.
   */
  public DFSClient getDFSClient() throws IOException {
    InetSocketAddress nnAddr = new InetSocketAddress("localhost", cluster.getNameNodePort());
    return new DFSClient(nnAddr, conf);
  }
  
  /**
   * Exercise the BlockReader and read length bytes.
   * 
   * It does not verify the bytes read.
   */
  public void readAndCheckEOS(BlockReader reader, int length, boolean expectEOF) 
          throws IOException {
    byte buf[] = new byte[1024];
    int nRead = 0;
    
    while (nRead < length) {
      DFSClient.LOG.info("So far read " + nRead + "- going to read more.");
      int n = reader.read(buf, 0, buf.length);
      assertTrue(n > 0);
      nRead += n;
    }
    
    if (expectEOF) {
      DFSClient.LOG.info("Done reading, expect EOF for next read.");
      assertEquals(-1, reader.read(buf, 0, buf.length));
    }
  }
  
  /**
   * Get a DataNode that serves our testBlock
   */
  public DataNode getDataNode(LocatedBlock testBlock) {
    DatanodeInfo[] nodes = testBlock.getLocations();
    int ipcport = nodes[0].getIpcPort();
    return cluster.getDataNode(ipcport);
  }
}
