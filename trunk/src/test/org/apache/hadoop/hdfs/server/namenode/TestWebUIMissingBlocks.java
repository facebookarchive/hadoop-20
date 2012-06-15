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

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.server.datanode.DataNode;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mortbay.log.Log;

import static org.junit.Assert.*;

public class TestWebUIMissingBlocks {

  private static MiniDFSCluster cluster;
  private static Configuration conf;
  private static final int BLOCK_SIZE = 1024;
  private static final int BLOCKS = 10;
  private static final int FILE_SIZE = BLOCK_SIZE * BLOCKS;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    conf = new Configuration();
    conf.setInt("dfs.replication.interval", 1); // 1 sec
    conf.setInt("heartbeat.recheck.interval", 200); // 200ms
    conf.setInt("dfs.block.size", BLOCK_SIZE);
    conf.setInt("dfs.heartbeat.interval", 1); // 1 sec
    cluster = new MiniDFSCluster(conf, 3, true, null);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    cluster.shutdown();
  }

  @Test
  public void testDeletedFileMissingBlock() throws IOException {
    FSNamesystem namesystem = cluster.getNameNode().namesystem;
    String fileName = "/testMissingBlock";
    DFSTestUtil.createFile(cluster.getFileSystem(), new Path(fileName),
        (long) FILE_SIZE, (short) 1, (long) 0);
    DatanodeInfo[] locations = namesystem
        .getBlockLocations(fileName, 0, Long.MAX_VALUE).get(0).getLocations();
    int port = locations[0].getPort();
    // Shutdown a datanode to get missing blocks.
    for (DataNode dn : cluster.getDataNodes()) {
      if (dn.getSelfAddr().getPort() == port) {
        dn.shutdown();
      }
    }
    // Wait for missing blocks.
    while (namesystem.getMissingBlocksCount() == 0) {
      try {
        System.out.println("No missing blocks yet");
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        throw new IOException(e);
      }
    }
    assertTrue(namesystem.getMissingBlocksCount() > 0);
    // Delete the file.
    ((DistributedFileSystem) cluster.getFileSystem()).getClient().delete(
        fileName, false);
    // Once we delete the file, there should be no missing blocks.
    assertEquals(0, namesystem.getMissingBlocksCount());
  }
}
