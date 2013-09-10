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

import org.apache.commons.logging.impl.Log4JLogger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.BlockPathInfo;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.protocol.FSConstants.DatanodeReportType;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.namenode.LeaseManager;
import org.apache.log4j.Level;
import org.junit.Before;

import java.io.IOException;
import java.io.RandomAccessFile;

import junit.framework.TestCase;


/**
 * This class tests the code path of transferring blocks
 */
public class TestTransferBlock extends junit.framework.TestCase {

  {
    ((Log4JLogger) LeaseManager.LOG).getLogger().setLevel(Level.ALL);
    ((Log4JLogger) FSNamesystem.LOG).getLogger().setLevel(Level.ALL);
    ((Log4JLogger) DFSClient.LOG).getLogger().setLevel(Level.ALL);
  }

  static final int blockSize = 8192;
  private MiniDFSCluster cluster;
  private FileSystem fileSystem;


  @Before
  protected void setUp() throws Exception {
    super.setUp();
    final Configuration conf = new Configuration();
    init(conf);
  }

  @Override
  protected void tearDown() throws Exception {
    cluster.shutdown();
    super.tearDown();
  }

  private void init(Configuration conf) throws IOException {
    if (cluster != null) {
      cluster.shutdown();
    }
    cluster = new MiniDFSCluster(conf, 2, true, null);
    cluster.waitClusterUp();
    fileSystem = cluster.getFileSystem();
  }

  public void testTransferZeroChecksumFile() throws IOException {
    for (DataNode dn : cluster.getDataNodes()) {
      dn.useInlineChecksum = false;
    }
    
    // create a new file in the root, write data, do no close
    String filestr = "/testTransferZeroChecksumFile";
    DistributedFileSystem dfs = (DistributedFileSystem) fileSystem;

    DFSTestUtil.createFile(dfs, new Path(filestr), 9L, (short)1, 0L);

    BlockPathInfo blockPathInfo = DFSTestUtil.getBlockPathInfo(filestr,
        cluster, dfs.getClient());
    
    // Delete the checksum file
    RandomAccessFile meta = new RandomAccessFile(blockPathInfo.getMetaPath(), "rw");
    meta.setLength(0);
    meta.close();

    RandomAccessFile block = new RandomAccessFile(blockPathInfo.getBlockPath(), "rw");
    block.setLength(0);
    block.close();
    
    int ns = cluster.getNameNode().getNamespaceID();
    DataNode dnWithBlk = null, dnWithoutBlk = null;
    for (DataNode dn : cluster.getDataNodes()) {
      FSDataset fds = (FSDataset) dn.data;
      DatanodeBlockInfo dbi = fds.getDatanodeBlockInfo(ns, blockPathInfo);
     if (dbi != null) {
        dbi.syncInMemorySize();
        dnWithBlk = dn;
      } else {
        dnWithoutBlk = dn;
      }
    }
    if (dnWithoutBlk == null || dnWithBlk == null) {
      TestCase.fail();
    }
    DatanodeInfo[] list = new DatanodeInfo[1];
    for (DatanodeInfo di : dfs.getClient().datanodeReport(DatanodeReportType.LIVE)) {
      if (dnWithoutBlk.getPort() == di.getPort()) {
        list[0] = di;
        break;
      }
    }
    blockPathInfo.setNumBytes(0);
    dnWithBlk.transferBlocks(ns, new Block[] {blockPathInfo}, new DatanodeInfo[][] {list});
    
    long size = -1;
    for (int i = 0; i < 3; i++) {
      try {
        size = ((FSDataset) dnWithoutBlk.data).getFinalizedBlockLength(ns,
            blockPathInfo);
        if (size == 0) {
          break;
        }
      } catch (IOException ioe) {
      }

      if (i != 2) {
        try {
          Thread.sleep(1000);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      } else {
        TestCase.fail();
      }
    }
    TestCase.assertEquals(0, size);
  }

  public void testTransferZeroChecksumFileInlineChecksum() throws IOException {
    for (DataNode dn : cluster.getDataNodes()) {
      dn.useInlineChecksum = true;
    }
    
    // create a new file in the root, write data, do no close
    String filestr = "/testTransferZeroChecksumFile";
    DistributedFileSystem dfs = (DistributedFileSystem) fileSystem;

    DFSTestUtil.createFile(dfs, new Path(filestr), 9L, (short)1, 0L);
    
    LocatedBlocks locations = cluster.getNameNode().getBlockLocations(filestr, 0,
        Long.MAX_VALUE);
    LocatedBlock locatedblock = locations.getLocatedBlocks().get(0);

    
    int ns = cluster.getNameNode().getNamespaceID();
    DataNode dnWithBlk = null, dnWithoutBlk = null;
    for (DataNode dn : cluster.getDataNodes()) {
      FSDataset fds = (FSDataset) dn.data;
      DatanodeBlockInfo dbi = fds.getDatanodeBlockInfo(ns,
          locatedblock.getBlock());
      
      if (dbi != null) {
        RandomAccessFile block = new RandomAccessFile(
            dbi.getBlockDataFile().file.toString(), "rw");
        block.setLength(0);
        block.close();

        dbi.syncInMemorySize();
        dnWithBlk = dn;
      } else {
        dnWithoutBlk = dn;
      }
    }
    if (dnWithoutBlk == null || dnWithBlk == null) {
      TestCase.fail();
    }
    DatanodeInfo[] list = new DatanodeInfo[1];
    for (DatanodeInfo di : dfs.getClient().datanodeReport(DatanodeReportType.LIVE)) {
      if (dnWithoutBlk.getPort() == di.getPort()) {
        list[0] = di;
        break;
      }
    }
    locatedblock.getBlock().setNumBytes(0);
    dnWithBlk.transferBlocks(ns, new Block[] { locatedblock.getBlock() },
        new DatanodeInfo[][] { list });
    
    long size = -1;
    for (int i = 0; i < 3; i++) {
      try {
        size = ((FSDataset) dnWithoutBlk.data).getFinalizedBlockLength(ns,
            locatedblock.getBlock());
        if (size == 0) {
          break;
        }
      } catch (IOException ioe) {
      }

      if (i != 2) {
        try {
          Thread.sleep(1000);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      } else {
        TestCase.fail();
      }
    }
    TestCase.assertEquals(0, size);
  }

}
