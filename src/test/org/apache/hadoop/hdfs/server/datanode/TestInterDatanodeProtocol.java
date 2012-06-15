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
package org.apache.hadoop.hdfs.server.datanode;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.BlockPathInfo;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlockWithMetaInfo;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.protocol.LocatedBlocksWithMetaInfo;
import org.apache.hadoop.hdfs.server.datanode.DataBlockScanner;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.protocol.BlockMetaDataInfo;
import org.apache.hadoop.hdfs.server.protocol.InterDatanodeProtocol;

/**
 * This tests InterDataNodeProtocol for block handling. 
 */
public class TestInterDatanodeProtocol extends junit.framework.TestCase {
  public static void checkMetaInfo(int namespaceId, Block b, InterDatanodeProtocol idp,
      DataBlockScannerSet scanner) throws IOException {
    BlockMetaDataInfo metainfo = idp.getBlockMetaDataInfo(namespaceId, b);
    assertEquals(b.getBlockId(), metainfo.getBlockId());
    assertEquals(b.getNumBytes(), metainfo.getNumBytes());    
    if (scanner != null) {
      assertEquals(scanner.getLastScanTime(namespaceId, b),
          metainfo.getLastScanTime());
    }
  }

  public static LocatedBlockWithMetaInfo getLastLocatedBlock(
      ClientProtocol namenode, String src
  ) throws IOException {
    //get block info for the last block
    LocatedBlocksWithMetaInfo locations = namenode.openAndFetchMetaInfo (src, 0, Long.MAX_VALUE);
    List<LocatedBlock> blocks = locations.getLocatedBlocks();
    DataNode.LOG.info("blocks.size()=" + blocks.size());
    assertTrue(blocks.size() > 0);

    LocatedBlock blk = blocks.get(blocks.size() - 1);
    return new LocatedBlockWithMetaInfo(blk.getBlock(), blk.getLocations(),
        blk.getStartOffset(), 
        locations.getDataProtocolVersion(), locations.getNamespaceID(),
        locations.getMethodFingerPrint());
  }

  /**
   * The following test first creates a file.
   * It verifies the block information from a datanode.
   * Then, it updates the block with new information and verifies again. 
   */
  public void testBlockMetaDataInfo() throws Exception {
    Configuration conf = new Configuration();
    MiniDFSCluster cluster = null;

    try {
      cluster = new MiniDFSCluster(conf, 3, true, null);
      cluster.waitActive();

      //create a file
      DistributedFileSystem dfs = (DistributedFileSystem)cluster.getFileSystem();
      String filestr = "/foo";
      Path filepath = new Path(filestr);
      DFSTestUtil.createFile(dfs, filepath, 1024L, (short)3, 0L);
      assertTrue(dfs.getClient().exists(filestr));

      //get block info
      LocatedBlockWithMetaInfo locatedblock = getLastLocatedBlock(dfs.getClient().namenode, filestr);
      int namespaceId = locatedblock.getNamespaceID();
      DatanodeInfo[] datanodeinfo = locatedblock.getLocations();
      assertTrue(datanodeinfo.length > 0);

      //connect to a data node
      DataNode datanode = cluster.getDataNode(datanodeinfo[0].getIpcPort());
      InterDatanodeProtocol idp = DataNode.createInterDataNodeProtocolProxy(
          datanodeinfo[0], conf, datanode.socketTimeout);
      assertTrue(datanode != null);
      
      //stop block scanner, so we could compare lastScanTime
      datanode.blockScanner.shutdown();

      //verify BlockMetaDataInfo
      Block b = locatedblock.getBlock();
      InterDatanodeProtocol.LOG.info("b=" + b + ", " + b.getClass());
      checkMetaInfo(namespaceId, b, idp, datanode.blockScanner);

      //verify updateBlock
      Block newblock = new Block(
          b.getBlockId(), b.getNumBytes()/2, b.getGenerationStamp()+1);
      idp.updateBlock(namespaceId, b, newblock, false);
      checkMetaInfo(namespaceId, newblock, idp, datanode.blockScanner);
    }
    finally {
      if (cluster != null) {cluster.shutdown();}
    }
  }
}
