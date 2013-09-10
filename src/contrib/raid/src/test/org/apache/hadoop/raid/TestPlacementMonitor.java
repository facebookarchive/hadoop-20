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

package org.apache.hadoop.raid;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import junit.framework.Assert;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.impl.Log4JLogger;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.LocatedBlockWithMetaInfo;
import org.apache.hadoop.hdfs.protocol.LocatedBlocksWithMetaInfo;
import org.apache.hadoop.hdfs.protocol.FSConstants.DatanodeReportType;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.raid.PlacementMonitor.BlockAndDatanodeResolver;
import org.apache.hadoop.raid.PlacementMonitor.BlockInfo;
import org.apache.hadoop.raid.protocol.PolicyInfo;
import org.apache.log4j.Level;
import org.junit.Test;

public class TestPlacementMonitor {
  private Configuration conf = null;
  private MiniDFSCluster cluster = null;
  private FileSystem fs = null;
  private PlacementMonitor placementMonitor = null;
  private BlockMover blockMover = null;
  private DatanodeInfo datanodes[] = null;
  private NameNode namenode = null;
  final String[] racks =
      {"/rack1", "/rack1", "/rack1", "/rack1", "/rack1", "/rack1",
      "/rack1", "/rack1", "/rack1", "/rack1", "/rack1", "/rack1"};
  final String[] hosts =
      {"host1.rack1.com", "host2.rack1.com", "host3.rack1.com",
       "host4.rack1.com", "host5.rack1.com", "host6.rack1.com",
       "host7.rack1.com", "host8.rack1.com", "host9.rack1.com",
       "host10.rack1.com", "host11.rack1.com", "host12.rack1.com"};
  final String[] mracks =
    {"/rack1", "/rack2", "/rack3", "/rack4", "/rack5", "/rack6",
     "/rack1", "/rack2", "/rack3", "/rack4", "/rack5", "/rack6"};
  final String[] mhosts =
    {"host1.rack1.com", "host2.rack2.com", "host3.rack3.com",
     "host4.rack4.com", "host5.rack5.com", "host6.rack6.com",
     "host7.rack1.com", "host8.rack2.com", "host9.rack3.com",
     "host10.rack4.com", "host11.rack5.com", "host12.rack6.com"};
  final static Log LOG =
      LogFactory.getLog(TestPlacementMonitor.class);
  private static PolicyInfo rsPolicy;
  private static PolicyInfo xorPolicy;
  
  {
    ((Log4JLogger)PlacementMonitor.LOG).getLogger().setLevel(Level.ALL);
    
    rsPolicy = new PolicyInfo("testrs", conf);
    rsPolicy.setCodecId("rs");
    rsPolicy.setProperty("targetReplication", "1");
    rsPolicy.setProperty("metaReplication", "1");
    
    xorPolicy = new PolicyInfo("testxor", conf);
    xorPolicy.setCodecId("xor");
    xorPolicy.setProperty("targetReplication", "2");
    xorPolicy.setProperty("metaReplication", "2");
    
  }
  private void setupCluster() throws IOException, InterruptedException {
    setupCluster(racks, hosts);
  }
  
  private void setupCluster(String[] racks, String[] hosts) throws IOException,
      InterruptedException {
    setupConf();
    setupCluster(conf, racks, hosts);
  }
  
  private void setupCluster(Configuration conf,
      String[] racks, String[] hosts) throws IOException, InterruptedException {
    // start the cluster with one datanode
    this.conf = conf;
    cluster = new MiniDFSCluster(conf, hosts.length, true, racks, hosts);
    cluster.waitActive();
    fs = cluster.getFileSystem();
    placementMonitor = new PlacementMonitor(conf);
    placementMonitor.start();
    blockMover = placementMonitor.blockMover;
    namenode = cluster.getNameNode();
    datanodes = namenode.getDatanodeReport(DatanodeReportType.LIVE);
    // Wait for Livenodes in clusterInfo to be non-null
    long sTime = System.currentTimeMillis();
    while (System.currentTimeMillis() - sTime < 120000 && blockMover.cluster.liveNodes == null) {
      LOG.info("Waiting for cluster info to add all liveNodes");
      Thread.sleep(1000);
    }
  }

  private void setupConf() throws IOException {
    conf = new Configuration();
    conf.set("dfs.replication.pending.timeout.sec", "2");
    conf.setLong("dfs.blockreport.intervalMsec", 100L);
    conf.setLong("dfs.block.size", 1L);
    Utils.loadTestCodecs(conf, 3, 1, 2, "/raid", "/raidrs");
    conf.setBoolean(PlacementMonitor.SIMULATE_KEY, false);
    conf.setInt("io.bytes.per.checksum", 1);
  }


  /**
   * Test that {@link PlacementMonitor} moves block correctly
   * @throws Exception
   */
  @Test
  public void testMoveBlock() throws Exception {
    setupCluster();
    try {
      Path path = new Path("/dir/file");
      DFSTestUtil.createFile(fs, path, 1, (short)1, 0L);
      DFSTestUtil.waitReplication(fs, path, (short)1);
      FileStatus status = fs.getFileStatus(path);
      LocatedBlocksWithMetaInfo blocks = namenode.openAndFetchMetaInfo(
          path.toString(), 0, status.getLen());
      Assert.assertEquals(1, blocks.getLocatedBlocks().size());
      LocatedBlock block = blocks.getLocatedBlocks().get(0);
      Assert.assertEquals(1, block.getLocations().length);
      DatanodeInfo source = block.getLocations()[0];
      Set<DatanodeInfo> excluded = new HashSet<DatanodeInfo>();
      for (DatanodeInfo d : datanodes) {
        excluded.add(d);
      }
      excluded.remove(source);
      DatanodeInfo target = excluded.iterator().next();
      excluded.add(source);
      excluded.remove(target);
      BlockMover.BlockMoveAction action =
          blockMover.new BlockMoveAction(block, source, excluded, 1,
              blocks.getDataProtocolVersion(), blocks.getNamespaceID());
      LOG.info("Start moving block from " + source + " to " + target);
      action.run();
      LOG.info("Done moving block");
      boolean blockMoved = false;
      long startTime = System.currentTimeMillis();
      while (!blockMoved && System.currentTimeMillis() - startTime < 60000) {
        blocks = namenode.openAndFetchMetaInfo(
            path.toString(), 0, status.getLen());
        block = blocks.getLocatedBlocks().get(0);
        if (block.getLocations().length == 1 &&
            block.getLocations()[0].equals((target))) {
          blockMoved = true;
          break;
        }
        StringBuilder sb = new StringBuilder();
        for (DatanodeInfo dni: block.getLocations()) {
          sb.append(dni);
          sb.append(" ");
        }
        LOG.info(block.getLocations().length + " nodes:" + sb.toString());
        Thread.sleep(1000L);
      }
      Assert.assertTrue(blockMoved);
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
      if (placementMonitor != null) {
        placementMonitor.stop();
      }
    }
  }

  /**
   * Test that the {@link PlacementMonitor.BlockAndDatanodeResolver} works
   * correctly.
   */
  @Test
  public void testBlockAndDatanodeResolver() throws Exception {
    setupCluster();
    try {
      Path src = new Path("/dir/file");
      Path parity = new Path("/raid/dir/file");
      DFSTestUtil.createFile(fs, src, 20, (short)2, 0L);
      DFSTestUtil.createFile(fs, parity, 11, (short)2, 0L);
      DFSTestUtil.waitReplication(fs, src, (short)2);
      DFSTestUtil.waitReplication(fs, parity, (short)2);

      LocatedBlocks srcLbs, parityLbs;
      List<BlockInfo> srcInfos, parityInfos;
      srcLbs = namenode.getBlockLocations(src.toString(), 4, 10);
      srcInfos = placementMonitor.getBlockInfos(fs, src, 4, 10);
      parityLbs = namenode.getBlockLocations(parity.toString(), 3, 7);
      parityInfos = placementMonitor.getBlockInfos(fs, parity, 3, 7);

      Assert.assertEquals(10, srcLbs.getLocatedBlocks().size());
      Assert.assertEquals(7, parityLbs.getLocatedBlocks().size());
      Assert.assertEquals(10, srcInfos.size());
      Assert.assertEquals(7, parityInfos.size());
      
      BlockAndDatanodeResolver resolver =
          new BlockAndDatanodeResolver(src, fs, parity, fs);
      for (int i = 0; i < srcInfos.size(); ++i) {
        LocatedBlock lb = resolver.getLocatedBlock(srcInfos.get(i));
        Assert.assertEquals(srcLbs.get(i).getBlock(), lb.getBlock());
        for (String nodeName : srcInfos.get(i).getNames()) {
          DatanodeInfo node = resolver.getDatanodeInfo(nodeName);
          Assert.assertEquals(node.getName(), nodeName);
        }
      }
      for (int i = 0; i < parityInfos.size(); ++i) {
        LocatedBlock lb = resolver.getLocatedBlock(parityInfos.get(i));
        Assert.assertEquals(parityLbs.get(i).getBlock(), lb.getBlock());
        for (String nodeName : parityInfos.get(i).getNames()) {
          DatanodeInfo node = resolver.getDatanodeInfo(nodeName);
          Assert.assertEquals(node.getName(), nodeName);
        }
      }
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
      if (placementMonitor != null) {
        placementMonitor.stop();
      }
    }
  }

  /**
   * Test that {@link PlacementMonitor} can choose a correct datanode
   * @throws Exception
   */
  @Test
  public void testChooseDatanode() throws Exception {
    setupCluster();
    try {
      Set<DatanodeInfo> excluded = new HashSet<DatanodeInfo>();
      for (int i = 0; i < 3; ++i) {
        excluded.add(datanodes[i]);
      }
      final int NUM_TESTS = 10;
      for (int i = 0; i < NUM_TESTS;) {
        DatanodeInfo target = blockMover.cluster.getNodeOnDifferentRack(excluded);
        if (target == null) {
          continue;
        }
        Assert.assertFalse(excluded.contains(target));
        ++i;
      }
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
      if (placementMonitor != null) {
        placementMonitor.stop();
      }
    }
  }
  
  /**
   * Test that check locatedBlocks will generate the correct move actions
   */
  @Test
  public void testCheckBlockLocationsDifferentRack() throws Exception {
    setupConf();
    this.conf.setBoolean(BlockMover.RAID_TEST_TREAT_NODES_ON_DEFAULT_RACK_KEY, true);
    try {
      setupCluster(this.conf, mracks, mhosts);
      for (DatanodeInfo node : datanodes) {
        LOG.info("node info: " + node);
        LOG.info("node location: " + node.getNetworkLocation());
      }
      FakeExecutorService fakeBlockMover = new FakeExecutorService();
      blockMover.executor = fakeBlockMover;
      DatanodeInfo sourceLocations[] = new DatanodeInfo[] {
          datanodes[0], datanodes[1], datanodes[2], // good
          datanodes[0], datanodes[1], datanodes[1], // bad 1==1
          datanodes[0], datanodes[1], datanodes[2], // good
          datanodes[0], datanodes[1], datanodes[2], // bad 0==0 with parity
          datanodes[0], datanodes[1]  // bad 0==0 with parity
      };
      DatanodeInfo parityLocations[] = new DatanodeInfo[] {
          datanodes[3], datanodes[4], // good
          datanodes[3], datanodes[4], // good
          datanodes[3], datanodes[3], // bad 3==3
          datanodes[0], datanodes[4], // bad 0==0 with parity
          datanodes[0], datanodes[0]  // bad 0==0 with parity
      };
      Path src = new Path("/dir/file");
      FileStatus srcStat = new FileStatus(14, false, 1, 1, 0, src);
      Path parity = new Path("/raid/dir/file");
      FileStatus parityStat = new FileStatus(14, false, 1, 1, 0, parity);
      FakeBlockAndDatanodeResolver resolver = new FakeBlockAndDatanodeResolver();
      long blockId = 0;
      List<LocatedBlockWithMetaInfo> srcBlockList = new LinkedList<LocatedBlockWithMetaInfo>();
      List<BlockInfo> srcInfoList = new LinkedList<BlockInfo>();
      for (DatanodeInfo d : sourceLocations) {
        LocatedBlockWithMetaInfo lb = new LocatedBlockWithMetaInfo(
            new Block(blockId++, 0, 0L), new DatanodeInfo[]{d}, 0L,
            0, 0, 0);
        BlockInfo info = createBlockInfo(srcStat, lb);
        srcBlockList.add(lb);
        srcInfoList.add(info);
        resolver.addBlock(info, lb);
        resolver.addNode(d.getName(), d);
      }
      List<LocatedBlock> parityBlockList = new LinkedList<LocatedBlock>();
      List<BlockInfo> parityInfoList = new LinkedList<BlockInfo>();
      for (DatanodeInfo d : parityLocations) {
        LocatedBlockWithMetaInfo lb = new LocatedBlockWithMetaInfo(
            new Block(blockId++, 0, 0L), new DatanodeInfo[]{d}, 0L,
            0, 0, 0);
        parityBlockList.add(lb);
        BlockInfo info = createBlockInfo(parityStat, lb);
        parityInfoList.add(info);
        resolver.addBlock(info, lb);
        resolver.addNode(d.getName(), d);
      }
      placementMonitor.checkBlockLocations(
          srcInfoList, parityInfoList, Codec.getCodec("rs"), rsPolicy,
          srcStat, resolver);
      for (BlockMover.BlockMoveAction action :
        fakeBlockMover.getSubmittedActions()) {
        LOG.info("Block move:" + action);
      }
      Set<Block> movedBlocks = new HashSet<Block>();
      for (BlockMover.BlockMoveAction action :
          fakeBlockMover.getSubmittedActions()) {
        movedBlocks.add(action.block.getBlock());
      }
      Map<Integer, Long> hist =
          placementMonitor.blockHistograms.get("rs");
      Assert.assertEquals(3, hist.size());
      Assert.assertEquals(15, hist.get(0).longValue());
      Assert.assertEquals(3, hist.get(1).longValue());
      Assert.assertEquals(1, hist.get(2).longValue());
      
      Map<Integer, Long> histRack = 
          placementMonitor.blockHistogramsPerRack.get("rs");
      Assert.assertEquals(3, histRack.size());
      Assert.assertEquals(15, histRack.get(0).longValue());
      Assert.assertEquals(3, histRack.get(1).longValue());
      Assert.assertEquals(1, histRack.get(2).longValue());

      // Note that the first block will stay, and all the modes are on 
      // different racks, so there will only be 5 moves.
      Assert.assertEquals(5, movedBlocks.size());
      Assert.assertTrue(movedBlocks.contains(new Block(5L, 0, 0L)));
      Assert.assertTrue(movedBlocks.contains(new Block(19L, 0, 0L)));
      Assert.assertTrue(movedBlocks.contains(new Block(20L, 0, 0L)));
      Assert.assertTrue(movedBlocks.contains(new Block(22L, 0, 0L)));
      Assert.assertTrue(movedBlocks.contains(new Block(23L, 0, 0L)));
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
      if (placementMonitor != null) {
        placementMonitor.stop();
      }
    }
  }
  
  @Test
  public void testCheckBlockLocationsXOROnDifferentRack() throws Exception {
    setupConf();
    this.conf.setBoolean(BlockMover.RAID_TEST_TREAT_NODES_ON_DEFAULT_RACK_KEY, true);
    try {
      setupCluster(this.conf, mracks, mhosts);
      FakeExecutorService fakeBlockMover = new FakeExecutorService();
      blockMover.executor = fakeBlockMover;
      DatanodeInfo sourceLocations[] = new DatanodeInfo[] {
          datanodes[0], datanodes[1], // block 0
          datanodes[0], datanodes[2], // block 1
          datanodes[0], datanodes[3], // block 2
          
          datanodes[0], datanodes[1], // block 3
          datanodes[0], datanodes[2], // block 4
          datanodes[1], datanodes[2], // block 5
      };
      DatanodeInfo parityLocations[] = new DatanodeInfo[] {
          datanodes[1], datanodes[4], // block 6
          datanodes[0], datanodes[2], // block 7
      };
      Map<String, List<DatanodeInfo>> blockLocations = 
          new HashMap<String, List<DatanodeInfo>> ();
      Path src = new Path("/dir/file");
      FileStatus srcStat = new FileStatus(6, false, 2, 1, 0, src);
      Path parity = new Path("/raid/dir/file");
      FileStatus parityStat = new FileStatus(2, false, 2, 1, 0, parity);
      FakeBlockAndDatanodeResolver resolver = new FakeBlockAndDatanodeResolver();
      long blockId = 0;
      List<LocatedBlockWithMetaInfo> srcBlockList = new LinkedList<LocatedBlockWithMetaInfo>();
      List<BlockInfo> srcInfoList = new LinkedList<BlockInfo>();
      for (int i = 0; i < sourceLocations.length; i+=2) {
        DatanodeInfo d = sourceLocations[i];
        DatanodeInfo d1 = sourceLocations[i+1];
        LocatedBlockWithMetaInfo lb = new LocatedBlockWithMetaInfo(
            new Block(blockId, 0, 0L), new DatanodeInfo[]{d, d1}, 0L,
            0, 0, 0);
        BlockInfo info = createBlockInfo(srcStat, lb);
        srcBlockList.add(lb);
        srcInfoList.add(info);
        resolver.addBlock(info, lb);
        resolver.addNode(d.getName(), d);
        resolver.addNode(d1.getName(), d1);
        
        List<DatanodeInfo> dnList = new ArrayList<DatanodeInfo>();
        dnList.add(d);
        dnList.add(d1);
        blockLocations.put(String.valueOf(blockId), dnList);
        ++ blockId;
      }
      List<LocatedBlock> parityBlockList = new LinkedList<LocatedBlock>();
      List<BlockInfo> parityInfoList = new LinkedList<BlockInfo>();
      for (int i = 0; i < parityLocations.length; i+=2) {
        DatanodeInfo d = parityLocations[i];
        DatanodeInfo d1 = parityLocations[i+1];
        LocatedBlockWithMetaInfo lb = new LocatedBlockWithMetaInfo(
            new Block(blockId, 0, 0L), new DatanodeInfo[]{d, d1}, 0L,
            0, 0, 0);
        parityBlockList.add(lb);
        BlockInfo info = createBlockInfo(parityStat, lb);
        parityInfoList.add(info);
        resolver.addBlock(info, lb);
        resolver.addNode(d.getName(), d);
        resolver.addNode(d1.getName(), d1);
        List<DatanodeInfo> dnList = new ArrayList<DatanodeInfo>();
        dnList.add(d);
        dnList.add(d1);
        blockLocations.put(String.valueOf(blockId), dnList);
        ++ blockId;
      }
      fakeBlockMover.setBlockLocations(blockLocations);
      placementMonitor.checkBlockLocations(
          srcInfoList, parityInfoList, Codec.getCodec("xor"), xorPolicy,
          srcStat, resolver);
      for (BlockMover.BlockMoveAction action :
        fakeBlockMover.getSubmittedActions()) {
        LOG.info("Block move:" + action);
      }
      Set<Block> movedBlocks = new HashSet<Block>();
      for (BlockMover.BlockMoveAction action :
          fakeBlockMover.getSubmittedActions()) {
        movedBlocks.add(action.block.getBlock());
      }
      Map<Integer, Long> hist =
          placementMonitor.blockHistograms.get("xor");
      Assert.assertEquals(3, hist.size());
      Assert.assertEquals(3, hist.get(0).longValue());
      Assert.assertEquals(2, hist.get(1).longValue());
      Assert.assertEquals(3, hist.get(2).longValue());
      
      // since each node is on different rack, the rack histogram should
      // be the same the one for nodes.
      Map<Integer, Long> histRack = 
          placementMonitor.blockHistogramsPerRack.get("xor");
      Assert.assertEquals(3, histRack.size());
      Assert.assertEquals(3, histRack.get(0).longValue());
      Assert.assertEquals(2, histRack.get(1).longValue());
      Assert.assertEquals(3, histRack.get(2).longValue());
      

      // Note that the first block will stay.
      Assert.assertEquals(8, fakeBlockMover.getSubmittedActions().size());
      Assert.assertEquals(6, movedBlocks.size());
      Assert.assertTrue(movedBlocks.contains(new Block(1L, 0, 0L)));
      Assert.assertTrue(movedBlocks.contains(new Block(2L, 0, 0L)));
      Assert.assertTrue(movedBlocks.contains(new Block(4L, 0, 0L)));
      Assert.assertTrue(movedBlocks.contains(new Block(5L, 0, 0L)));
      Assert.assertTrue(movedBlocks.contains(new Block(6L, 0, 0L)));
      Assert.assertTrue(movedBlocks.contains(new Block(7L, 0, 0L)));
      
      fakeBlockMover.clearActions();
      resolver = new FakeBlockAndDatanodeResolver();
      // rebuild the block infos
      srcInfoList = new LinkedList<BlockInfo>();
      for (LocatedBlock srcBlock : srcBlockList) {
        List<DatanodeInfo> replicaNodes = blockLocations.get(
            String.valueOf(srcBlock.getBlock().getBlockId()));
        LocatedBlockWithMetaInfo lb = new LocatedBlockWithMetaInfo(
            new Block(srcBlock.getBlock().getBlockId(), 0, 0L), 
            replicaNodes.toArray(new DatanodeInfo[]{}),
            0L, 0, 0, 0);
        
        BlockInfo info = createBlockInfo(srcStat, lb);
        srcInfoList.add(info);
        resolver.addBlock(info, lb);
        for (DatanodeInfo d : replicaNodes) {
          resolver.addNode(d.getName(), d);
        }
      }
      
      parityInfoList = new LinkedList<BlockInfo>();
      for (LocatedBlock parityBlock : parityBlockList) {
        List<DatanodeInfo> replicaNodes = blockLocations.get(
            String.valueOf(parityBlock.getBlock().getBlockId()));
        LocatedBlockWithMetaInfo lb = new LocatedBlockWithMetaInfo(
            new Block(parityBlock.getBlock().getBlockId(), 0, 0L), 
            replicaNodes.toArray(new DatanodeInfo[]{}),
            0L, 0, 0, 0);
        
        BlockInfo info = createBlockInfo(srcStat, lb);
        parityInfoList.add(info);
        resolver.addBlock(info, lb);
        for (DatanodeInfo d : replicaNodes) {
          resolver.addNode(d.getName(), d);
        }
      }

      // check the block locations again, we should expect no block move actions
      // this time.
      placementMonitor.checkBlockLocations(srcInfoList, parityInfoList, 
          Codec.getCodec("xor"), null, srcStat, resolver);
      assertEquals(0, fakeBlockMover.getSubmittedActions().size());
      
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
      if (placementMonitor != null) {
        placementMonitor.stop();
      }
    }
  }

  @Test
  public void testCheckBlockLocationsXOR() throws Exception {
    try {
      setupCluster();
      FakeExecutorService fakeBlockMover = new FakeExecutorService();
      blockMover.executor = fakeBlockMover;
      DatanodeInfo sourceLocations[] = new DatanodeInfo[] {
          datanodes[0], datanodes[1], 
          datanodes[0], datanodes[2], 
          datanodes[0], datanodes[3], 
          
          datanodes[0], datanodes[1], 
          datanodes[0], datanodes[2], 
          datanodes[1], datanodes[2], 
      };
      DatanodeInfo parityLocations[] = new DatanodeInfo[] {
          datanodes[1], datanodes[4], 
          datanodes[0], datanodes[2],
      };
      Path src = new Path("/dir/file");
      FileStatus srcStat = new FileStatus(6, false, 2, 1, 0, src);
      Path parity = new Path("/raid/dir/file");
      FileStatus parityStat = new FileStatus(2, false, 2, 1, 0, parity);
      FakeBlockAndDatanodeResolver resolver = new FakeBlockAndDatanodeResolver();
      long blockId = 0;
      List<LocatedBlockWithMetaInfo> srcBlockList = new LinkedList<LocatedBlockWithMetaInfo>();
      List<BlockInfo> srcInfoList = new LinkedList<BlockInfo>();
      for (int i = 0; i < sourceLocations.length; i+=2) {
        DatanodeInfo d = sourceLocations[i];
        DatanodeInfo d1 = sourceLocations[i+1];
        LocatedBlockWithMetaInfo lb = new LocatedBlockWithMetaInfo(
            new Block(blockId++, 0, 0L), new DatanodeInfo[]{d, d1}, 0L,
            0, 0, 0);
        BlockInfo info = createBlockInfo(srcStat, lb);
        srcBlockList.add(lb);
        srcInfoList.add(info);
        resolver.addBlock(info, lb);
        resolver.addNode(d.getName(), d);
        resolver.addNode(d1.getName(), d1);
      }
      List<LocatedBlock> parityBlockList = new LinkedList<LocatedBlock>();
      List<BlockInfo> parityInfoList = new LinkedList<BlockInfo>();
      for (int i = 0; i < parityLocations.length; i+=2) {
        DatanodeInfo d = parityLocations[i];
        DatanodeInfo d1 = parityLocations[i+1];
        LocatedBlockWithMetaInfo lb = new LocatedBlockWithMetaInfo(
            new Block(blockId++, 0, 0L), new DatanodeInfo[]{d, d1}, 0L,
            0, 0, 0);
        parityBlockList.add(lb);
        BlockInfo info = createBlockInfo(parityStat, lb);
        parityInfoList.add(info);
        resolver.addBlock(info, lb);
        resolver.addNode(d.getName(), d);
        resolver.addNode(d1.getName(), d1);
      }
      placementMonitor.checkBlockLocations(
          srcInfoList, parityInfoList, Codec.getCodec("xor"), xorPolicy,
          srcStat, resolver);
      for (BlockMover.BlockMoveAction action :
        fakeBlockMover.getSubmittedActions()) {
        LOG.info("Block move:" + action);
      }
      Set<Block> movedBlocks = new HashSet<Block>();
      for (BlockMover.BlockMoveAction action :
          fakeBlockMover.getSubmittedActions()) {
        movedBlocks.add(action.block.getBlock());
      }
      Map<Integer, Long> hist =
          placementMonitor.blockHistograms.get("xor");
      Assert.assertEquals(3, hist.size());
      Assert.assertEquals(3, hist.get(0).longValue());
      Assert.assertEquals(2, hist.get(1).longValue());
      Assert.assertEquals(3, hist.get(2).longValue());
      
      // since all the blocks are on the same rack
      Map<Integer, Long> histRack =
          placementMonitor.blockHistogramsPerRack.get("xor");
      Assert.assertEquals(1, histRack.size());
      Assert.assertEquals(2, histRack.get(7).longValue());

      // Note that the first block will stay, and all the blocks are on 
      // the same rack, so there will be 12 + 4 - 2 moves.
      Assert.assertEquals(14, fakeBlockMover.getSubmittedActions().size());
      Assert.assertEquals(8, movedBlocks.size());
      Assert.assertTrue(movedBlocks.contains(new Block(2L, 0, 0L)));
      Assert.assertTrue(movedBlocks.contains(new Block(3L, 0, 0L)));
      Assert.assertTrue(movedBlocks.contains(new Block(4L, 0, 0L)));
      Assert.assertTrue(movedBlocks.contains(new Block(5L, 0, 0L)));
      Assert.assertTrue(movedBlocks.contains(new Block(7L, 0, 0L)));
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
      if (placementMonitor != null) {
        placementMonitor.stop();
      }
    }
  }
  
  /**
   * Test that no block movement actions are submitted.
   */
  @Test
  public void testNotSubmitBlockMovement() throws Exception {
    LOG.info("Start testNotSubmitBlockMovement.");
    try {
      setupCluster();
      FakeExecutorService fakeBlockMover = new FakeExecutorService();
      blockMover.executor = fakeBlockMover;
      DatanodeInfo sourceLocations[][] = new DatanodeInfo[][] {
          // over-replicated blocks in src 
          new DatanodeInfo[] {datanodes[0]},
          new DatanodeInfo[] {datanodes[0], datanodes[1]},
          new DatanodeInfo[] {datanodes[0], datanodes[2]},
          
          // missing blocks in src
          new DatanodeInfo[] {datanodes[0]},
          new DatanodeInfo[] {datanodes[0]},
          new DatanodeInfo[] {},
          
          // over-replicated blocks in parity
          new DatanodeInfo[] {datanodes[0]},
          new DatanodeInfo[] {datanodes[0]},
          new DatanodeInfo[] {datanodes[0]},
          
          // missing blocks in parity
          new DatanodeInfo[] {datanodes[0]},
          new DatanodeInfo[] {datanodes[0]},
          new DatanodeInfo[] {datanodes[0]}
      };
      DatanodeInfo parityLocations[][] = new DatanodeInfo[][] {
          
          new DatanodeInfo[] {datanodes[0]},
          new DatanodeInfo[] {datanodes[0]},
          
          new DatanodeInfo[] {datanodes[0]},
          new DatanodeInfo[] {datanodes[0]},
          
          new DatanodeInfo[] {datanodes[0], datanodes[1]},
          new DatanodeInfo[] {datanodes[0], datanodes[2]},
          
          new DatanodeInfo[] {datanodes[0]},
          new DatanodeInfo[] {}
      };
      
      Path src = new Path("/dir/file");
      FileStatus srcStat = new FileStatus(12, false, 1, 1, 0, src);
      Path parity = new Path("/raid/dir/file");
      FileStatus parityStat = new FileStatus(8, false, 1, 1, 0, parity);
      FakeBlockAndDatanodeResolver resolver = new FakeBlockAndDatanodeResolver();
      long blockId = 0;
      List<LocatedBlockWithMetaInfo> srcBlockList = new LinkedList<LocatedBlockWithMetaInfo>();
      List<BlockInfo> srcInfoList = new LinkedList<BlockInfo>();
      for (DatanodeInfo[] datanodeInfos : sourceLocations) {
        LocatedBlockWithMetaInfo lb = new LocatedBlockWithMetaInfo(
            new Block(blockId++, 0, 0L), datanodeInfos, 0L,
            0, 0, 0);
        BlockInfo info = createBlockInfo(srcStat, lb);
        srcBlockList.add(lb);
        srcInfoList.add(info);
        resolver.addBlock(info, lb);
        for (DatanodeInfo d : datanodeInfos) {
          resolver.addNode(d.getName(), d);
        }
      }
      List<LocatedBlock> parityBlockList = new LinkedList<LocatedBlock>();
      List<BlockInfo> parityInfoList = new LinkedList<BlockInfo>();
      for (DatanodeInfo[] datanodeInfos : parityLocations) {
        LocatedBlockWithMetaInfo lb = new LocatedBlockWithMetaInfo(
            new Block(blockId++, 0, 0L), datanodeInfos, 0L,
            0, 0, 0);
        parityBlockList.add(lb);
        BlockInfo info = createBlockInfo(parityStat, lb);
        parityInfoList.add(info);
        resolver.addBlock(info, lb);
        for (DatanodeInfo d : datanodeInfos) {
          resolver.addNode(d.getName(), d);
        }
      }
      
      placementMonitor.checkBlockLocations(
          srcInfoList, parityInfoList, Codec.getCodec("rs"), rsPolicy,
          srcStat, resolver);
      for (BlockMover.BlockMoveAction action :
        fakeBlockMover.getSubmittedActions()) {
        LOG.info("Block move:" + action);
      }
      Set<Block> movedBlocks = new HashSet<Block>();
      for (BlockMover.BlockMoveAction action :
          fakeBlockMover.getSubmittedActions()) {
        movedBlocks.add(action.block.getBlock());
      }

      Assert.assertEquals("No block movements should be submitted.", 
          0, movedBlocks.size());
      
      LOG.info("Done testNotSubmitBlockMovement.");
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
      if (placementMonitor != null) {
        placementMonitor.stop();
      }
    }
  }
  
  @Test
  public void testCheckSrcBlocks() throws Exception {
  	try {
  		setupCluster();
  		FakeExecutorService fakeBlockMover = new FakeExecutorService();
  		blockMover.executor = fakeBlockMover;
  		DatanodeInfo srcLocations[] = new DatanodeInfo[] {
  				datanodes[0], datanodes[1], datanodes[2],
  				datanodes[0], datanodes[0], datanodes[0]
  		};
  		
  		Path src = new Path("/dir/file");
  		FileStatus srcStat = new FileStatus(2, false, 3, 1, 0, src);
  		FakeBlockAndDatanodeResolver resolver = new FakeBlockAndDatanodeResolver();
  		long blockId = 0;
  		List<LocatedBlockWithMetaInfo> srcBlockList = new LinkedList<LocatedBlockWithMetaInfo>();
      List<BlockInfo> srcInfoList = new LinkedList<BlockInfo>();
  		for (int i = 0; i < srcLocations.length; i+=3) {
  			LocatedBlockWithMetaInfo lb = new LocatedBlockWithMetaInfo(
  					new Block(blockId++, 0, 0L),
  					new DatanodeInfo[] {srcLocations[i], srcLocations[i+1], srcLocations[i+2]},
  					0L, 0, 0, 0);
  			BlockInfo info = createBlockInfo(srcStat, lb);
  			srcBlockList.add(lb);
  			srcInfoList.add(info);
  			resolver.addBlock(info, lb);
  			resolver.addNode(srcLocations[i].getName(), srcLocations[i]);
  			resolver.addNode(srcLocations[i+1].getName(), srcLocations[i+1]);
  			resolver.addNode(srcLocations[i+2].getName(), srcLocations[i+2]);
  		}
  		
  		placementMonitor.checkSrcBlockLocations(srcInfoList, srcStat, resolver);
  		for (BlockMover.BlockMoveAction action :
        fakeBlockMover.getSubmittedActions()) {
        LOG.info("Block move:" + action);
      }
      Set<Block> movedBlocks = new HashSet<Block>();
      for (BlockMover.BlockMoveAction action :
          fakeBlockMover.getSubmittedActions()) {
        movedBlocks.add(action.block.getBlock());
      }
      
      Assert.assertEquals(2, movedBlocks.size());
      Assert.assertTrue(movedBlocks.contains(new Block(0L, 0, 0L)));
      Assert.assertTrue(movedBlocks.contains(new Block(1L, 0, 0L)));
  	} finally {
  		if (cluster != null) {
  			cluster.shutdown();
  		}
  		
  		if (placementMonitor != null) {
  			placementMonitor.stop();
  		}
  	}
  }
  
  @Test
  public void testCheckSrcBlocksDifferentRack() throws Exception {
  	setupConf();
    this.conf.setBoolean(BlockMover.RAID_TEST_TREAT_NODES_ON_DEFAULT_RACK_KEY, true);
  	try {
  		setupCluster(this.conf, mracks, mhosts);
  		FakeExecutorService fakeBlockMover = new FakeExecutorService();
  		blockMover.executor = fakeBlockMover;
  		DatanodeInfo srcLocations[] = new DatanodeInfo[] {
  				datanodes[0], datanodes[1], datanodes[2],
  				datanodes[0], datanodes[0], datanodes[0]
  		};
  		
  		Path src = new Path("/dir/file");
  		FileStatus srcStat = new FileStatus(2, false, 3, 1, 0, src);
  		FakeBlockAndDatanodeResolver resolver = new FakeBlockAndDatanodeResolver();
  		long blockId = 0;
  		List<LocatedBlockWithMetaInfo> srcBlockList = new LinkedList<LocatedBlockWithMetaInfo>();
      List<BlockInfo> srcInfoList = new LinkedList<BlockInfo>();
  		for (int i = 0; i < srcLocations.length; i+=3) {
  			LocatedBlockWithMetaInfo lb = new LocatedBlockWithMetaInfo(
  					new Block(blockId++, 0, 0L),
  					new DatanodeInfo[] {srcLocations[i], srcLocations[i+1], srcLocations[i+2]},
  					0L, 0, 0, 0);
  			BlockInfo info = createBlockInfo(srcStat, lb);
  			srcBlockList.add(lb);
  			srcInfoList.add(info);
  			resolver.addBlock(info, lb);
  			resolver.addNode(srcLocations[i].getName(), srcLocations[i]);
  			resolver.addNode(srcLocations[i+1].getName(), srcLocations[i+1]);
  			resolver.addNode(srcLocations[i+2].getName(), srcLocations[i+2]);
  		}
  		
  		placementMonitor.checkSrcBlockLocations(srcInfoList, srcStat, resolver);
  		for (BlockMover.BlockMoveAction action :
        fakeBlockMover.getSubmittedActions()) {
        LOG.info("Block move:" + action);
      }
      Set<Block> movedBlocks = new HashSet<Block>();
      for (BlockMover.BlockMoveAction action :
          fakeBlockMover.getSubmittedActions()) {
        movedBlocks.add(action.block.getBlock());
      }
      
      Assert.assertEquals(1, movedBlocks.size());
      Assert.assertTrue(movedBlocks.contains(new Block(1L, 0, 0L)));
  	} finally {
  		if (cluster != null) {
  			cluster.shutdown();
  		}
  		
  		if (placementMonitor != null) {
  			placementMonitor.stop();
  		}
  	}
  }


  /**
   * Test that check locatedBlocks will generate the correct move actions
   */
  @Test
  public void testCheckBlockLocations() throws Exception {
    try {
      setupCluster();
      FakeExecutorService fakeBlockMover = new FakeExecutorService();
      blockMover.executor = fakeBlockMover;
      DatanodeInfo sourceLocations[] = new DatanodeInfo[] {
          datanodes[0], datanodes[1], datanodes[2], // good
          datanodes[0], datanodes[1], datanodes[1], // bad 1==1
          datanodes[0], datanodes[1], datanodes[2], // good
          datanodes[0], datanodes[1], datanodes[2], // bad 0==0 with parity
          datanodes[0], datanodes[1]  // bad 0==0 with parity
      };
      DatanodeInfo parityLocations[] = new DatanodeInfo[] {
          datanodes[3], datanodes[4], // good
          datanodes[3], datanodes[4], // good
          datanodes[3], datanodes[3], // bad 3==3
          datanodes[0], datanodes[4], // bad 0==0 with parity
          datanodes[0], datanodes[0]  // bad 0==0 with parity
      };
      Path src = new Path("/dir/file");
      FileStatus srcStat = new FileStatus(14, false, 1, 1, 0, src);
      Path parity = new Path("/raid/dir/file");
      FileStatus parityStat = new FileStatus(14, false, 1, 1, 0, parity);
      FakeBlockAndDatanodeResolver resolver = new FakeBlockAndDatanodeResolver();
      long blockId = 0;
      List<LocatedBlockWithMetaInfo> srcBlockList = new LinkedList<LocatedBlockWithMetaInfo>();
      List<BlockInfo> srcInfoList = new LinkedList<BlockInfo>();
      for (DatanodeInfo d : sourceLocations) {
        LocatedBlockWithMetaInfo lb = new LocatedBlockWithMetaInfo(
            new Block(blockId++, 0, 0L), new DatanodeInfo[]{d}, 0L,
            0, 0, 0);
        BlockInfo info = createBlockInfo(srcStat, lb);
        srcBlockList.add(lb);
        srcInfoList.add(info);
        resolver.addBlock(info, lb);
        resolver.addNode(d.getName(), d);
      }
      List<LocatedBlock> parityBlockList = new LinkedList<LocatedBlock>();
      List<BlockInfo> parityInfoList = new LinkedList<BlockInfo>();
      for (DatanodeInfo d : parityLocations) {
        LocatedBlockWithMetaInfo lb = new LocatedBlockWithMetaInfo(
            new Block(blockId++, 0, 0L), new DatanodeInfo[]{d}, 0L,
            0, 0, 0);
        parityBlockList.add(lb);
        BlockInfo info = createBlockInfo(parityStat, lb);
        parityInfoList.add(info);
        resolver.addBlock(info, lb);
        resolver.addNode(d.getName(), d);
      }
      placementMonitor.checkBlockLocations(
          srcInfoList, parityInfoList, Codec.getCodec("rs"), rsPolicy,
          srcStat, resolver);
      for (BlockMover.BlockMoveAction action :
        fakeBlockMover.getSubmittedActions()) {
        LOG.info("Block move:" + action);
      }
      Set<Block> movedBlocks = new HashSet<Block>();
      for (BlockMover.BlockMoveAction action :
          fakeBlockMover.getSubmittedActions()) {
        movedBlocks.add(action.block.getBlock());
      }
      Map<Integer, Long> hist =
          placementMonitor.blockHistograms.get("rs");
      Assert.assertEquals(3, hist.size());
      Assert.assertEquals(15, hist.get(0).longValue());
      Assert.assertEquals(3, hist.get(1).longValue());
      Assert.assertEquals(1, hist.get(2).longValue());

      // Note that the first block will stay, and all the blocks are on 
      // the same rack, so there will be 14 + 10 - 5 moves.
      Assert.assertEquals(19, movedBlocks.size());
      Assert.assertTrue(movedBlocks.contains(new Block(5L, 0, 0L)));
      Assert.assertTrue(movedBlocks.contains(new Block(19L, 0, 0L)));
      Assert.assertTrue(movedBlocks.contains(new Block(20L, 0, 0L)));
      Assert.assertTrue(movedBlocks.contains(new Block(22L, 0, 0L)));
      Assert.assertTrue(movedBlocks.contains(new Block(23L, 0, 0L)));
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
      if (placementMonitor != null) {
        placementMonitor.stop();
      }
    }
  }

  private BlockInfo createBlockInfo(FileStatus stat, LocatedBlock b) {
    DatanodeInfo[] locations = b.getLocations();
    String[] hosts = new String[locations.length];
    String[] names = new String[locations.length];
    for (int i = 0; i < locations.length; ++i) {
      DatanodeInfo d = locations[i];
      hosts[i] = d.getHost();
      names[i] = d.getName();
    }
    
    BlockLocation loc = new BlockLocation(
        names, hosts, b.getStartOffset(), b.getBlockSize());
    return new BlockInfo(loc, stat);
  }

  public class FakeBlockAndDatanodeResolver extends
      PlacementMonitor.BlockAndDatanodeResolver {
    Map<BlockInfo, LocatedBlockWithMetaInfo> blockMap = new HashMap<BlockInfo, LocatedBlockWithMetaInfo>();
    Map<String, DatanodeInfo> nodeMap = new HashMap<String, DatanodeInfo>();
    public void addBlock(BlockInfo info, LocatedBlockWithMetaInfo lb) {
      blockMap.put(info, lb);
    }
    public void addNode(String name, DatanodeInfo node) {
      nodeMap.put(name, node);
    }
    @Override
    public LocatedBlockWithMetaInfo getLocatedBlock(BlockInfo blk) throws IOException {
      return blockMap.containsKey(blk)? blockMap.get(blk): null;
    }
    @Override
    public DatanodeInfo getDatanodeInfo(String name) throws IOException {
      return nodeMap.containsKey(name)? nodeMap.get(name): null;
    }
    
    @Override
    public void initialize(Path path, FileSystem fs) {
    }
  }

  public class FakeExecutorService extends ThreadPoolExecutor {
    List<BlockMover.BlockMoveAction> actions;
    Map<String, List<DatanodeInfo>> blockLocations = null;
    public FakeExecutorService() {
      this(1, 1, 1L, TimeUnit.SECONDS, new LinkedBlockingQueue<Runnable>());
    }
    public FakeExecutorService(int corePoolSize, int maximumPoolSize,
        long keepAliveTime, TimeUnit unit, BlockingQueue<Runnable> workQueue) {
      super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue);
      this.actions = new LinkedList<BlockMover.BlockMoveAction>();
    }
    public List<BlockMover.BlockMoveAction> getSubmittedActions() {
      return actions;
    }
    @Override
    public void execute(Runnable action) {
      BlockMover.BlockMoveAction moveAction = (BlockMover.BlockMoveAction)action;
      actions.add(moveAction);
      if (blockLocations != null) {
        long blockId = moveAction.block.getBlock().getBlockId();
        List<DatanodeInfo> info = blockLocations.get(String.valueOf(blockId));
        info.remove(moveAction.source);
        info.add(moveAction.target);
      }
    }
    
    public void clearActions() {
      actions.clear();
    }
    
    public void setBlockLocations(Map<String, List<DatanodeInfo>> blockLocations) {
      this.blockLocations = blockLocations;
    }
  }
}
