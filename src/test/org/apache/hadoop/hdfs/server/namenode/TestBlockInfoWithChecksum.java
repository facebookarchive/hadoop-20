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

import static org.junit.Assert.assertEquals;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.server.namenode.BlocksMap.BlockInfo;
import org.junit.After;
import org.junit.Test;

public class TestBlockInfoWithChecksum {
  private Configuration conf;
  private MiniDFSCluster cluster;
  private DistributedFileSystem dfs;
  
  @Test
  public void testBlockInfoWithChecksum() throws IOException {
    long blockSize = 1024;
    conf = new Configuration();
    conf.setLong("dfs.block.size", blockSize);
    cluster = new MiniDFSCluster(conf, 2, true, null);
    dfs = (DistributedFileSystem) cluster.getFileSystem();
    FSNamesystem fsn = cluster.getNameNode().getNamesystem();
    
    /**
     * Create some random file with some blocks
     */
    // Test closed inode with block
    long fileoneSize = 8192;
    DFSTestUtil.createFile(dfs, new Path("/testone/file"), fileoneSize, (short) 1, 0);
   
    
    FSDirectory fsDir = fsn.dir;
    
    // Test fileone
    INodeFile fileone = (INodeFile)fsDir.getINode("/testone/file");
    BlockInfo[] blks = fileone.getBlocks();
    assertEquals(fileoneSize / blockSize, blks.length);
    for (BlockInfo block : blks) {
      // TODO(xiaojian): change this once we have block checksum from client
      assertEquals(BlockInfo.NO_BLOCK_CHECKSUM, block.getChecksum());
    }
    
    // Make sure edit log is loaded correctly 
    cluster.restartNameNode();
    cluster.waitActive();
    fsn = cluster.getNameNode().getNamesystem();
    fsDir = fsn.dir;
    
    fileone = (INodeFile)fsDir.getINode("/testone/file");
    blks = fileone.getBlocks();
    assertEquals(fileoneSize / blockSize, blks.length);
    for (BlockInfo block : blks) {
      // TODO(xiaojian): change this once we have block checksum from client
      assertEquals(BlockInfo.NO_BLOCK_CHECKSUM, block.getChecksum());
    }
  }

  @After
  public void tearDown() throws IOException {
    if (cluster != null) {
      cluster.shutdown();
    }
    if (dfs != null) {
      dfs.close();
    }
  }

}
