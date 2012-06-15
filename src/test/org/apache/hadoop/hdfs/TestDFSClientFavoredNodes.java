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

import static org.junit.Assert.assertEquals;

import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.FSConstants;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * This test suite ensures that files written with the DFSClient with favored
 * nodes specified will be replicated to the favored nodes.
 */
public class TestDFSClientFavoredNodes {

  private static final int BLOCK_SIZE = 1024;
  private static final int BLOCKS = 10;
  private static final int BUFFER_SIZE = 1024;
  private static final short REPLICATION = 3;
  private static final int BYTES_PER_CHECKSUM = 512;

  private static Configuration conf;
  private static MiniDFSCluster cluster;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    conf = new Configuration();
    conf.setInt("dfs.block.size", BLOCK_SIZE);
    cluster = new MiniDFSCluster(conf, 6, true, null);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    cluster.shutdown();
  }

  @Test
  public void testFavoredNodes() throws Exception {
    // Get the datanodes in the system and choose some favored ones.
    DatanodeInfo[] nodes = cluster.getNameNode().getDatanodeReport(
        FSConstants.DatanodeReportType.ALL);
    InetSocketAddress[] favoredNodes = new InetSocketAddress[REPLICATION];
    for (int i = 0; i < REPLICATION; i++) {
      favoredNodes[i] = new InetSocketAddress(nodes[i].getHost(),
          nodes[i].getPort());
    }
    DatanodeInfo[] favoredNodeInfos = Arrays.copyOfRange(nodes, 0, REPLICATION);
    // Sort the favored nodes for future comparison.
    Arrays.sort(favoredNodeInfos);

    // Write a file, specifying the favored nodes.
    String fileName = "/testFavoredNodes";
    DistributedFileSystem fs = (DistributedFileSystem)cluster.getFileSystem();
    OutputStream out = fs.create(new Path(fileName), FsPermission.getDefault(),
        false, BUFFER_SIZE, REPLICATION, BLOCK_SIZE, BYTES_PER_CHECKSUM, null,
        favoredNodes);
    Random rand = new Random();
    byte[] bytes = new byte[BLOCK_SIZE];
    for (int i = 0; i < BLOCKS; i++) {
      rand.nextBytes(bytes);
      out.write(bytes);
    }
    out.close();

    // Get the locations of every block that was just written, and compare them
    // to the favored nodes.
    LocatedBlocks lbks = cluster.getNameNode().getBlockLocations(fileName, 0,
        Long.MAX_VALUE);
    for (LocatedBlock lbk : lbks.getLocatedBlocks()) {
      DatanodeInfo[] locs = lbk.getLocations();
      // The lists of blocks must be sorted first because nodes are not
      // necessarily listed in the same order (order does not matter anyways).
      // The sorted lists must be equal.
      Arrays.sort(locs);
      for (int i = 0; i < locs.length; i++) {
        assertEquals(locs[i], favoredNodeInfos[i]);
      }
    }
  }
}
