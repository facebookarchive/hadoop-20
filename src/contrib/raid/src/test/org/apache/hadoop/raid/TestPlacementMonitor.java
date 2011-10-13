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

import java.io.IOException;
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
import org.apache.hadoop.hdfs.protocol.FSConstants.DatanodeReportType;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.raid.PlacementMonitor.BlockAndDatanodeResolver;
import org.apache.hadoop.raid.PlacementMonitor.BlockInfo;
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
      {"/rack1", "/rack1", "/rack1", "/rack1", "/rack1", "/rack1"};
  final String[] hosts =
      {"host1.rack1.com", "host2.rack1.com", "host3.rack1.com",
       "host4.rack1.com", "host5.rack1.com", "host6.rack1.com"};
  final static Log LOG =
      LogFactory.getLog(TestPlacementMonitor.class);

  private void setupCluster() throws IOException {
    setupConf();
    // start the cluster with one datanode
    cluster = new MiniDFSCluster(conf, 6, true, racks, hosts);
    cluster.waitActive();
    fs = cluster.getFileSystem();
    placementMonitor = new PlacementMonitor(conf);
    placementMonitor.start();
    blockMover = placementMonitor.blockMover;
    namenode = cluster.getNameNode();
    datanodes = namenode.getDatanodeReport(DatanodeReportType.LIVE);
  }

  private void setupConf() throws IOException {
    conf = new Configuration();
    conf.set("dfs.replication.pending.timeout.sec", "2");
    conf.setLong("dfs.blockreport.intervalMsec", 100L);
    conf.setLong("dfs.block.size", 1L);
    conf.set(RaidNode.STRIPE_LENGTH_KEY, "3");
    conf.set(RaidNode.RS_PARITY_LENGTH_KEY, "2");
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
      LocatedBlocks blocks = namenode.getBlockLocations(
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
          blockMover.new BlockMoveAction(block, source, excluded, 1);
      LOG.info("Start moving block from " + source + " to " + target);
      action.run();
      LOG.info("Done moving block");
      boolean blockMoved = false;
      for (int i = 0; i < 100; ++i) {
        blocks = namenode.getBlockLocations(
            path.toString(), 0, status.getLen());
        block = blocks.getLocatedBlocks().get(0);
        if (block.getLocations().length == 1 &&
            block.getLocations()[0].equals((target))) {
          blockMoved = true;
          break;
        }
        Thread.sleep(100L);
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
        DatanodeInfo target = blockMover.cluster.getRandomNode(excluded);
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
  public void testCheckBlockLocations() throws IOException {
    setupCluster();
    try {
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
          datanodes[0], datanodes[4], // bad 0==0 with pairity
          datanodes[0], datanodes[0]  // bad 0==0 with parity
      };
      Path src = new Path("/dir/file");
      Path parity = new Path("/raid/dir/file");
      FakeBlockAndDatanodeResolver resolver = new FakeBlockAndDatanodeResolver();
      long blockId = 0;
      List<LocatedBlock> srcBlockList = new LinkedList<LocatedBlock>();
      List<BlockInfo> srcInfoList = new LinkedList<BlockInfo>();
      for (DatanodeInfo d : sourceLocations) {
        LocatedBlock lb = new LocatedBlock(
            new Block(blockId++, 0, 0L), new DatanodeInfo[]{d});
        BlockInfo info = createBlockInfo(parity, lb);
        srcBlockList.add(lb);
        srcInfoList.add(info);
        resolver.addBlock(info, lb);
        resolver.addNode(d.getName(), d);
      }
      List<LocatedBlock> parityBlockList = new LinkedList<LocatedBlock>();
      List<BlockInfo> parityInfoList = new LinkedList<BlockInfo>();
      for (DatanodeInfo d : parityLocations) {
        LocatedBlock lb = new LocatedBlock(
            new Block(blockId++, 0, 0L), new DatanodeInfo[]{d});
        parityBlockList.add(lb);
        BlockInfo info = createBlockInfo(parity, lb);
        parityInfoList.add(info);
        resolver.addBlock(info, lb);
        resolver.addNode(d.getName(), d);
      }
      FileStatus stat = new FileStatus(14, false, 1, 1, 0, src);
      placementMonitor.checkBlockLocations(
          srcInfoList, parityInfoList, ErasureCodeType.RS, stat, resolver);
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
          placementMonitor.blockHistograms.get(ErasureCodeType.RS);
      Assert.assertEquals(3, hist.size());
      Assert.assertEquals(15, hist.get(0).longValue());
      Assert.assertEquals(3, hist.get(1).longValue());
      Assert.assertEquals(1, hist.get(2).longValue());

      // Note that the first block will stay. So there will be only 5 moves.
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

  private BlockInfo createBlockInfo(Path file, LocatedBlock b) {
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
    return new BlockInfo(loc, file);
  }

  class FakeBlockAndDatanodeResolver extends
      PlacementMonitor.BlockAndDatanodeResolver {
    Map<BlockInfo, LocatedBlock> blockMap = new HashMap<BlockInfo, LocatedBlock>();
    Map<String, DatanodeInfo> nodeMap = new HashMap<String, DatanodeInfo>();
    public void addBlock(BlockInfo info, LocatedBlock lb) {
      blockMap.put(info, lb);
    }
    public void addNode(String name, DatanodeInfo node) {
      nodeMap.put(name, node);
    }
    @Override
    public LocatedBlock getLocatedBlock(BlockInfo blk) throws IOException {
      return blockMap.get(blk);
    }
    @Override
    public DatanodeInfo getDatanodeInfo(String name) throws IOException {
      return nodeMap.get(name);
    }
  }

  class FakeExecutorService extends ThreadPoolExecutor {
    List<BlockMover.BlockMoveAction> actions;
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
      actions.add((BlockMover.BlockMoveAction)action);
    }
  }
}
