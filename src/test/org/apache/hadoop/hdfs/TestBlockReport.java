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
import java.util.ArrayList;

import junit.framework.TestCase;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.BlockListAsLongs;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.namenode.NameNode;

/*
 * Unit test for block report
 */
public class TestBlockReport extends TestCase {
  public static final Log LOG = LogFactory.getLog(TestBlockReport.class);
  final String DFS_BLOCK_SIZE_KEY = "dfs.block.size";

  /**
   * Verify that the block size has to be the multiple of checksum size
   */
  public void testBadBlockSize() {
    MiniDFSCluster cluster = null;
    boolean gotException = false;
    try {
      Configuration conf = new Configuration();
      final long BLOCK_SIZE = 4 * 1024 * 1024 + 1;
      conf.setLong(DFS_BLOCK_SIZE_KEY, BLOCK_SIZE);
      cluster = new MiniDFSCluster(conf, 5, true, null);
      cluster.waitActive();
      FileSystem fs = cluster.getFileSystem();
      Path file = new Path("/test");
      DFSTestUtil.createFile(fs, file, 33 * 1024 * 1024L, (short)3, 1L);
    } catch (IOException e) {
      gotException = true;
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
    assertEquals(true, gotException);
  }

  /**
   * Verify that the invalid block size in block report will not be accepted
   */
  public void testInvalidBlockReport() throws Exception {
    MiniDFSCluster cluster = null;
    try {
      Configuration conf = new Configuration();
      final long BLOCK_SIZE = 4 * 1024 * 1024;
      final long FILE_SIZE = 33 * 1024 * 1024;
      conf.setLong(DFS_BLOCK_SIZE_KEY, BLOCK_SIZE);
      cluster = new MiniDFSCluster(conf, 5, true, null);
      cluster.waitActive();
      FileSystem fs = cluster.getFileSystem();
      NameNode namenode = cluster.getNameNode();

      Path file = new Path("/test");
      DFSTestUtil.createFile(fs, file, FILE_SIZE, (short)3, 1L);

      LocatedBlocks locations = namenode.getBlockLocations(
          file.toString(), 0, Long.MAX_VALUE);

      LOG.info("Create file with " + locations.locatedBlockCount() +
               " blocks and prefered block size " +
               namenode.getPreferredBlockSize(file.toString()));

      for (int i = 0; i < locations.locatedBlockCount(); i++) {
        Block b = locations.get(i).getBlock();
        if (i == locations.locatedBlockCount() - 1) {
          assertEquals(FILE_SIZE % BLOCK_SIZE, b.getNumBytes());
        } else {
          assertEquals(BLOCK_SIZE, b.getNumBytes());
        }
      }
      assertEquals(BLOCK_SIZE, namenode.getPreferredBlockSize(file.toString()));
      assertEquals(9, locations.locatedBlockCount());

      ArrayList<DataNode> dataNodes = cluster.getDataNodes();
      for (int i = 0; i < dataNodes.size(); i++) {
        Block[] blocksToReport = dataNodes.get(0).data.getBlockReport();
        for (int j = 0; j < blocksToReport.length; j++) {
          Block b = blocksToReport[j];
          // change the block size to be 1 larger than preferred size
          // leave one block reporting the correct size
          b.setNumBytes(BLOCK_SIZE + 1);
        }
        long[] blockReport =
          BlockListAsLongs.convertToArrayLongs(blocksToReport);
        namenode.blockReport(dataNodes.get(i).dnRegistration, blockReport);
      }
      for (int i = 0; i < locations.locatedBlockCount(); i++) {
        Block b = locations.get(i).getBlock();
        // verify that the block size is not changed by the bogus report
        if (i == locations.locatedBlockCount() - 1) {
          assertTrue(b.getNumBytes() <= BLOCK_SIZE);
        } else {
          assertEquals(BLOCK_SIZE, b.getNumBytes());
        }
      }
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

}
