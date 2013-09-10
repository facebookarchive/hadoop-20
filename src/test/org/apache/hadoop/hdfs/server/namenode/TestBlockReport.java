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

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.BlockListAsLongs;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.FSConstants.DatanodeReportType;
import org.apache.hadoop.hdfs.protocol.FSConstants.SafeModeAction;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.namenode.BlocksMap.BlockInfo;
import org.apache.hadoop.hdfs.server.namenode.DatanodeDescriptor;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.util.InjectionEvent;
import org.apache.hadoop.util.InjectionHandler;
import org.apache.hadoop.util.InjectionEventI;
import org.junit.Test;

/*
 * Unit test for block report
 */
public class TestBlockReport {
  public static final Log LOG = LogFactory.getLog(TestBlockReport.class);
  final String DFS_BLOCK_SIZE_KEY = "dfs.block.size";

  /**
   * Verify that the block size has to be the multiple of checksum size
   */
  @Test
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
      DFSTestUtil.createFile(fs, file, 33 * 1024 * 1024L, (short) 3, 1L);
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
  @Test
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
      DFSTestUtil.createFile(fs, file, FILE_SIZE, (short) 3, 1L);

      LocatedBlocks locations = namenode.getBlockLocations(file.toString(), 0,
          Long.MAX_VALUE);

      LOG.info("Create file with " + locations.locatedBlockCount()
          + " blocks and prefered block size "
          + namenode.getPreferredBlockSize(file.toString()));

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
      int nsId = namenode.getNamespaceID();
      for (int i = 0; i < dataNodes.size(); i++) {
        Block[] blocksToReport = dataNodes.get(i).data.getBlockReport(nsId);
        for (int j = 0; j < blocksToReport.length; j++) {
          Block b = blocksToReport[j];
          // change the block size to be 1 larger than preferred size
          // leave one block reporting the correct size
          b.setNumBytes(BLOCK_SIZE + 1);
        }
        long[] blockReport = BlockListAsLongs
            .convertToArrayLongs(blocksToReport);
        namenode.blockReport(dataNodes.get(i).getDNRegistrationForNS(nsId),
            blockReport);
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

  @Test
  public void testInitialBR1Block() throws Exception {
    // only one block in the initial BR from datanode
    testInitialBlockReportsSafemode(1);
  }

  @Test
  public void testInitialBR16Block() throws Exception {
    // 16 blocks in the initial BR from datanode (this many handlers by default)
    testInitialBlockReportsSafemode(16);
  }

  @Test
  public void testInitialBR200Block() throws Exception {
    // 200 blocks in the initial BR from datanode (this many handlers by
    // default)
    testInitialBlockReportsSafemode(200);
  }

  private void testInitialBlockReportsSafemode(int numFiles) throws Exception {
    LOG.info("TEST: test initial block report with " + numFiles
        + " files/blocks");
    MiniDFSCluster cluster = null;
    int numNodes = 3;
    TestBlockReportInjectionHandler h = new TestBlockReportInjectionHandler();
    InjectionHandler.set(h);
    try {
      Configuration conf = new Configuration();
      int numBlocksPerThread = 8;
      conf.setInt("dfs.safemode.fast.br.blocks", numBlocksPerThread);
      cluster = new MiniDFSCluster(conf, numNodes, true, null);
      cluster.waitActive();
      FileSystem fs = cluster.getFileSystem();
      NameNode namenode = cluster.getNameNode();
      LOG.info("Cluster Alive.");

      // create a bunch of files
      for (int i = 0; i < numFiles; i++) {
        Path file = new Path("/filestatus" + i + ".dat");
        DFSTestUtil.createFile(fs, file, 1L, (short) 3, 1L);
      }

      // enter safemode
      namenode.setSafeMode(SafeModeAction.SAFEMODE_ENTER);

      // get datanode info
      DatanodeInfo[] dinfo = namenode.getDatanodeReport(DatanodeReportType.ALL);

      for (int i = 0; i < dinfo.length; i++) {
        DatanodeDescriptor desc = cluster.getNameNode().getNamesystem()
            .getDatanode(dinfo[i]);
        assertEquals(numFiles, desc.numBlocks());
      }

      // save block ids
      HashSet<Long> blocks = new HashSet<Long>();
      DatanodeDescriptor dd = cluster.getNameNode().getNamesystem()
          .getDatanode(dinfo[0]);
      for (Iterator<BlockInfo> iter = dd.getBlockIterator(); iter.hasNext();) {
        blocks.add(iter.next().getBlockId());
      }

      // restart datanodes and remove it from namesystem
      for (int i = 0; i < dinfo.length; i++) {
        namenode.getNamesystem().removeDatanode(dinfo[i]);
        DatanodeDescriptor desc = cluster.getNameNode().getNamesystem()
            .getDatanode(dinfo[i]);
        assertEquals(0, desc.numBlocks());
      }

      h.clearEvents();
      cluster.restartDataNodes();
      long start = System.currentTimeMillis();
      while (!h.processedEvents
          .containsKey(InjectionEvent.FSNAMESYSTEM_BLOCKREPORT_COMPLETED)
          && !(numNodes == h.processedEvents
              .get(InjectionEvent.FSNAMESYSTEM_BLOCKREPORT_COMPLETED))
          && System.currentTimeMillis() - start < 30000) {
        DFSTestUtil.waitNMilliSecond(100);
      }

      // blocks are back
      // we need to get the report again (re-registration)
      dinfo = namenode.getDatanodeReport(DatanodeReportType.ALL);
      for (int i = 0; i < dinfo.length; i++) {
        DatanodeDescriptor desc = cluster.getNameNode().getNamesystem()
            .getDatanode(dinfo[i]);
        // block number matches
        assertEquals(numFiles, desc.numBlocks());
        
        // blocks for this datanode
        HashSet<Long> blocksAfter = new HashSet<Long>();
        for (Iterator<BlockInfo> iter = desc.getBlockIterator(); iter.hasNext();) {
          BlockInfo bi = iter.next();
          blocksAfter.add(bi.getBlockId());
          // datanode must be listed as a location for this block
          assertTrue(0 <= bi.findDatanode(desc));
        }
        // blocks are the same
        assertEquals(blocks, blocksAfter);
      }

      int numShards = Math.min(16,
          ((numFiles + numBlocksPerThread - 1) / numBlocksPerThread));

      if (numShards > 1) {
        // parallel processing
        assertTrue(numNodes * numShards == h.processedEvents
            .get(InjectionEvent.FSNAMESYSTEM_INITIALBR_WORKER));
      } else {
        // direct processing
        assertEquals(null,
            h.processedEvents.get(InjectionEvent.FSNAMESYSTEM_INITIALBR_WORKER));
      }
      assertTrue(numNodes * numFiles == h.processedEvents
          .get(InjectionEvent.FSNAMESYSTEM_ADDSTORED_BLOCK));
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
      InjectionHandler.clear();
    }
  }

  class TestBlockReportInjectionHandler extends InjectionHandler {
    Map<InjectionEventI, Integer> processedEvents = new HashMap<InjectionEventI, Integer>();

    void clearEvents() {
      processedEvents.clear();
    }

    @Override
    protected void _processEvent(InjectionEventI event, Object... args) {
      synchronized (processedEvents) {
        if (processedEvents.containsKey(event)) {
          processedEvents.put(event, processedEvents.get(event) + 1);
        } else {
          processedEvents.put(event, 1);
        }
      }
    }
  }
}
