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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.impl.Log4JLogger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.RaidDFSUtil;
import org.apache.hadoop.hdfs.MiniDFSCluster.DataNodeProperties;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem.NumberReplicas;
import org.apache.hadoop.raid.RaidCodec;
import org.apache.log4j.Level;
import org.junit.Test;

public class TestRaidMissingBlocksQueue {
  static final Log LOG = LogFactory.getLog(TestRaidMissingBlocksQueue.class);
  {
    ((Log4JLogger)NameNode.stateChangeLog).getLogger().setLevel(Level.ALL);
  }
 
  /**
   * corrupt a block in a raided file, and make sure it will be shown in the 
   * Raid missing blocks queue.
   */
  @Test
  public void testCorruptBlocks() throws IOException {
    MiniDFSCluster cluster = null;
    Configuration conf = new Configuration();
    try {
      cluster = new MiniDFSCluster(conf, 3, true, null);
      
      DistributedFileSystem dfs = DFSUtil.convertToDFS(cluster.getFileSystem());
      String filePath = "/test/file1";
      RaidDFSUtil.constructFakeRaidFile(dfs, filePath, RaidCodec.getCodec("rs"));
      
      FileStatus stat = dfs.getFileStatus(new Path(filePath));
      LocatedBlocks blocks = dfs.getClient().
          getLocatedBlocks(filePath, 0, stat.getLen());
      
      Block block = blocks.getLocatedBlocks().get(0).getBlock();
      DFSTestUtil.corruptBlock(block, cluster);
      
      RaidDFSUtil.reportCorruptBlocksToNN(dfs, 
          new LocatedBlock[] {blocks.getLocatedBlocks().get(0)});
      
      final FSNamesystem namesystem = cluster.getNameNode().namesystem;
      assertEquals(1, namesystem.getRaidMissingBlocksCount()); // one raid missing blocks;
      assertEquals(0, namesystem.getMissingBlocksCount());  // zero non-raid missing blocks;
      
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }
  
  /**
   * Take down a datanode to generate raid missing blocks, and then bring it back
   * will restore the missing blocks.
   */
  @Test
  public void testRaidMissingBlocksByTakingDownDataNode() throws IOException, InterruptedException {
    MiniDFSCluster cluster = null;
    Configuration conf = new Configuration();
    try {
      cluster = new MiniDFSCluster(conf, 1, true, null);
      final FSNamesystem namesystem = cluster.getNameNode().namesystem;
      final DistributedFileSystem dfs = DFSUtil.convertToDFS(cluster.getFileSystem());
      String filePath = "/test/file1";
      RaidCodec rsCodec = RaidCodec.getCodec("rs");
      RaidDFSUtil.constructFakeRaidFile(dfs, filePath, rsCodec);
      
      DatanodeDescriptor[] datanodes = (DatanodeDescriptor[])
          namesystem.heartbeats.toArray(
              new DatanodeDescriptor[1]);
      assertEquals(1, datanodes.length);
      
      // shutdown the datanode
      DataNodeProperties dnprop = shutdownDataNode(cluster, datanodes[0]);
      assertEquals(rsCodec.numStripeBlocks, namesystem.getRaidMissingBlocksCount());
      assertEquals(0, namesystem.getMissingBlocksCount()); // zero non-raid missing block
      assertEquals(0, namesystem.getNonCorruptUnderReplicatedBlocks());
      
      // bring up the datanode
      cluster.restartDataNode(dnprop);
      
      // Wait for block report
      LOG.info("wait for its block report to come in");
      NumberReplicas num;
      FileStatus stat = dfs.getFileStatus(new Path(filePath));
      LocatedBlocks blocks = dfs.getClient().
          getLocatedBlocks(filePath, 0, stat.getLen());
      long startTime = System.currentTimeMillis();
      do {
        Thread.sleep(1000);
        int totalCount = 0;
        namesystem.readLock();
        try {
          for (LocatedBlock block : blocks.getLocatedBlocks()) {
            num = namesystem.countNodes(block.getBlock());
            totalCount += num.liveReplicas();
          }
          if (totalCount == rsCodec.numDataBlocks) {
            break;
          } else {
            LOG.info("wait for block report, received total replicas: " + totalCount);
          }
        } finally {
          namesystem.readUnlock();
        }
      } while (System.currentTimeMillis() - startTime < 30000);
      assertEquals(0, namesystem.getRaidMissingBlocksCount());
      assertEquals(0, namesystem.getMissingBlocksCount()); // zero non-raid missing block
      assertEquals(0, namesystem.getNonCorruptUnderReplicatedBlocks());

    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }   
  }
  
  public DataNodeProperties shutdownDataNode(MiniDFSCluster cluster, DatanodeDescriptor datanode) {
    LOG.info("shutdown datanode: " + datanode.getName());
    DataNodeProperties dnprop = cluster.stopDataNode(datanode.getName());
    FSNamesystem namesystem = cluster.getNameNode().namesystem;
    // make sure that NN detects that the datanode is down
    synchronized (namesystem.heartbeats) {
      datanode.setLastUpdate(0); // mark it dead
      namesystem.heartbeatCheck();
    }
    return dnprop;
  }
  
}
