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
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlockWithMetaInfo;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.datanode.FSDataset;
import org.apache.hadoop.hdfs.server.datanode.FSDatasetTestUtil;
import org.apache.hadoop.hdfs.server.datanode.TestInterDatanodeProtocol;
import org.apache.hadoop.hdfs.server.protocol.BlockMetaDataInfo;
import org.apache.hadoop.hdfs.server.protocol.InterDatanodeProtocol;
import org.apache.hadoop.ipc.Client;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.ClientAdapter;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.util.Progressable;

public class TestLeaseRecovery extends junit.framework.TestCase {
  static final int BLOCK_SIZE = 1024;
  static final short REPLICATION_NUM = (short)3;
  private MiniDFSCluster cluster;
  private Configuration conf;

  static void checkMetaInfo(int namespaceId, Block b, InterDatanodeProtocol idp
      ) throws IOException {
    TestInterDatanodeProtocol.checkMetaInfo(namespaceId, b, idp, null);
  }
  
  static int min(Integer... x) {
    int m = x[0];
    for(int i = 1; i < x.length; i++) {
      if (x[i] < m) {
        m = x[i];
      }
    }
    return m;
  }

  @Override
  protected void setUp() throws Exception {
    super.setUp();

    conf = new Configuration();
    conf.setLong("dfs.block.size", BLOCK_SIZE);
    conf.setBoolean("dfs.support.append", true);
    cluster = null;
    cluster = new MiniDFSCluster(conf, 5, true, null);
    cluster.waitActive();
  }

  @Override
  protected void tearDown() throws Exception {
    cluster.shutdown();

    super.tearDown();
  }

  /**
   * test the recoverBlock does not leak clients when creating 
   * InterDatanodeProtocol RPC instances
   */
  public void testForClientLeak() throws Exception {
      Client client = ClientAdapter.getClient(
        conf, NetUtils.getSocketFactory(conf, InterDatanodeProtocol.class)
      );

      DistributedFileSystem fileSystem = (DistributedFileSystem) cluster.getFileSystem();
      int initialRefCount = ClientAdapter.getRefCount(client);
      String filename = "/file1";
      DFSOutputStream out = (DFSOutputStream)
        ((DistributedFileSystem) fileSystem).getClient().create(
          filename, FsPermission.getDefault(), true, (short) 5, 1024,
          new Progressable() {
            @Override
            public void progress() {
            }
          },
          64 * 1024
        );
      out.write(DFSTestUtil.generateSequentialBytes(0, 512));
      out.sync();

      DatanodeInfo[] dataNodeInfos =
        ((DFSOutputStream)out).getPipeline();

      // killing one DN in the pipe and doing a write triggers lease recovery
      // and will result in the refcount being adjusted; if there's a lease
      // in Datanode.recoverBlock(), this will trigger it
      cluster.stopDataNode(dataNodeInfos[0].getName());
      out.write(DFSTestUtil.generateSequentialBytes(0, 512));
      assertEquals(
        "Client refcount leak!",
        initialRefCount - 1,  //-1 since we stop a DN above
        ClientAdapter.getRefCount(client)
      );

      out.close();
  }
  
  public void testBlockSynchronizationInlineChecksum() throws Exception {
    runTestBlockSynchronization(false, true);
  }
  public void testBlockSynchronizationWithZeroBlockInlineChecksum() throws Exception {
    runTestBlockSynchronization(true, true);
  }
  public void testBlockSynchronization() throws Exception {
    runTestBlockSynchronization(false, false);
  }
  public void testBlockSynchronizationWithZeroBlock() throws Exception {
    runTestBlockSynchronization(true, false);
  }


  /**
   * The following test first creates a file with a few blocks.
   * It randomly truncates the replica of the last block stored in each datanode.
   * Finally, it triggers block synchronization to synchronize all stored block.
   * @param forceOneBlockToZero if true, will truncate one block to 0 length
   */
  public void runTestBlockSynchronization(boolean forceOneBlockToZero,
      boolean useInlineChecksum)  throws Exception {
    final int ORG_FILE_SIZE = 3000; 
    for (DataNode dn : cluster.getDataNodes()) {
      dn.useInlineChecksum = useInlineChecksum;
    }

      //create a file
      DistributedFileSystem dfs = (DistributedFileSystem)cluster.getFileSystem();
      String filestr = "/foo";
      Path filepath = new Path(filestr);
      DFSTestUtil.createFile(dfs, filepath, ORG_FILE_SIZE, REPLICATION_NUM, 0L);
      assertTrue(dfs.dfs.exists(filestr));
      DFSTestUtil.waitReplication(dfs, filepath, REPLICATION_NUM);

      //get block info for the last block
      LocatedBlockWithMetaInfo locatedblock = TestInterDatanodeProtocol.getLastLocatedBlock(
          dfs.dfs.namenode, filestr);
      int namespaceId = locatedblock.getNamespaceID();
      DatanodeInfo[] datanodeinfos = locatedblock.getLocations();
      assertEquals(REPLICATION_NUM, datanodeinfos.length);

      //connect to data nodes
      InterDatanodeProtocol[] idps = new InterDatanodeProtocol[REPLICATION_NUM];
      DataNode[] datanodes = new DataNode[REPLICATION_NUM];
      for(int i = 0; i < REPLICATION_NUM; i++) {
      idps[i] = DataNode.createInterDataNodeProtocolProxy(
		datanodeinfos[i], conf, 0);
        datanodes[i] = cluster.getDataNode(datanodeinfos[i].getIpcPort());
        assertTrue(datanodes[i] != null);
      }
      
      //verify BlockMetaDataInfo
      Block lastblock = locatedblock.getBlock();
      DataNode.LOG.info("newblocks=" + lastblock);
      for(int i = 0; i < REPLICATION_NUM; i++) {
        checkMetaInfo(namespaceId, lastblock, idps[i]);
      }

      //setup random block sizes 
      int lastblocksize = ORG_FILE_SIZE % BLOCK_SIZE;
      Integer[] newblocksizes = new Integer[REPLICATION_NUM];
      for(int i = 0; i < REPLICATION_NUM; i++) {
        newblocksizes[i] = AppendTestUtil.nextInt(lastblocksize);
      }
      if (forceOneBlockToZero) {
        newblocksizes[0] = 0;
      }
      DataNode.LOG.info("newblocksizes = " + Arrays.asList(newblocksizes)); 

      //update blocks with random block sizes
      Block[] newblocks = new Block[REPLICATION_NUM];
      for(int i = 0; i < REPLICATION_NUM; i++) {
        DataNode dn = datanodes[i];
      FSDatasetTestUtil.truncateBlock(dn, lastblock, newblocksizes[i],
          namespaceId, useInlineChecksum);
        newblocks[i] = new Block(lastblock.getBlockId(), newblocksizes[i],
            lastblock.getGenerationStamp());
        checkMetaInfo(namespaceId, newblocks[i], idps[i]);
      }

      DataNode.LOG.info("dfs.dfs.clientName=" + dfs.dfs.clientName);
      cluster.getNameNode().append(filestr, dfs.dfs.clientName);

      //block synchronization
      final int primarydatanodeindex = AppendTestUtil.nextInt(datanodes.length);
      DataNode.LOG.info("primarydatanodeindex  =" + primarydatanodeindex);
      DataNode primary = datanodes[primarydatanodeindex];
      DataNode.LOG.info("primary.dnRegistration=" + primary.getDNRegistrationForNS(
          cluster.getNameNode().getNamespaceID()));
    primary.recoverBlocks(namespaceId, new Block[] { lastblock },
        new DatanodeInfo[][] { datanodeinfos },
        System.currentTimeMillis() + 3000).join();

      BlockMetaDataInfo[] updatedmetainfo = new BlockMetaDataInfo[REPLICATION_NUM];
      int minsize = min(newblocksizes);
      long currentGS = cluster.getNameNode().namesystem.getGenerationStamp();
      lastblock.setGenerationStamp(currentGS);
      for(int i = 0; i < REPLICATION_NUM; i++) {
        updatedmetainfo[i] = idps[i].getBlockMetaDataInfo(
            namespaceId, lastblock);
      RPC.stopProxy(idps[i]);
        assertEquals(lastblock.getBlockId(), updatedmetainfo[i].getBlockId());
        assertEquals(minsize, updatedmetainfo[i].getNumBytes());
        assertEquals(currentGS, updatedmetainfo[i].getGenerationStamp());
      }
    }
}
