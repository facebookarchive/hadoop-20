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
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import junit.framework.Assert;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.server.namenode.BlockPlacementPolicyRaid.CachedFullPathNames;
import org.apache.hadoop.hdfs.server.namenode.BlockPlacementPolicyRaid.CachedLocatedBlocks;
import org.apache.hadoop.hdfs.util.InjectionEvent;
import org.apache.hadoop.net.Node;
import org.apache.hadoop.net.StaticMapping;
import org.apache.hadoop.raid.Codec;
import org.apache.hadoop.raid.Utils;
import org.apache.hadoop.util.InjectionEventI;
import org.apache.hadoop.util.InjectionHandler;
import org.junit.Test;

public class TestBlockPlacementPolicyRaid {
  TestBlockPlacementPolicyRaidInjectionHandler h;
  private Configuration conf = null;
  private MiniDFSCluster cluster = null;
  private FSNamesystem namesystem = null;
  private BlockPlacementPolicyRaid policy = null;
  private FileSystem fs = null;
  String[] rack1 = {"/rack1"};
  String[] rack2 = {"/rack2"};
  String[] host1 = {"host1.rack1.com"};
  String[] host2 = {"host2.rack2.com"};
  String xorPrefix = null;
  String raidTempPrefix = null;
  String raidrsTempPrefix = null;
  String raidrsHarTempPrefix = null;

  final static Log LOG =
      LogFactory.getLog(TestBlockPlacementPolicyRaid.class);

  protected void setupCluster() throws IOException {
    conf = new Configuration();
    conf.setLong("dfs.blockreport.intervalMsec", 1000L);
    conf.set("dfs.replication.pending.timeout.sec", "2");
    conf.setLong("dfs.block.size", 1L);
    conf.set("dfs.block.replicator.classname",
             "org.apache.hadoop.hdfs.server.namenode.BlockPlacementPolicyRaid");
    Utils.loadTestCodecs(conf, 2, 1, 3, "/raid", "/raidrs");
    conf.setInt("io.bytes.per.checksum", 1);
    // start the cluster with one datanode
    cluster = new MiniDFSCluster(conf, 1, true, rack1, host1);
    cluster.waitActive();
    namesystem = cluster.getNameNode().getNamesystem();
    Assert.assertTrue("BlockPlacementPolicy type is not correct.",
        namesystem.replicator instanceof BlockPlacementPolicyRaid);
    policy = (BlockPlacementPolicyRaid) namesystem.replicator;
    fs = cluster.getFileSystem();
    xorPrefix = Codec.getCodec("xor").parityDirectory;
    raidTempPrefix = Codec.getCodec("xor").tmpParityDirectory;
    raidrsTempPrefix = Codec.getCodec("rs").parityDirectory;
    raidrsHarTempPrefix = Codec.getCodec("rs").tmpParityDirectory;
  }

  @Test
  public void test3rdReplicaPlacement() throws Exception {
    conf = new Configuration();
    conf.set("dfs.block.replicator.classname",
    "org.apache.hadoop.hdfs.server.namenode.BlockPlacementPolicyRaid");
    // start the cluster with 3 datanodes and 3 racks
    String[] racks = new String[]{
        "/rack0", "/rack0", "/rack1", "/rack1", "/rack2", "/rack2"};
    String[] hosts = new String[]{
        "host0", "host1", "host2", "host3", "host4", "host5"};
    
    try {
    cluster = new MiniDFSCluster(conf, 6, true, racks, hosts);
    cluster.waitActive();
    
    namesystem = cluster.getNameNode().getNamesystem();
    Assert.assertTrue("BlockPlacementPolicy type is not correct.",
        namesystem.replicator instanceof BlockPlacementPolicyRaid);
    policy = (BlockPlacementPolicyRaid) namesystem.replicator;
    fs = cluster.getFileSystem();
    
    final String filename = "/dir/file1";
    
    // an out-of-cluster client
    DatanodeDescriptor targets[] = policy.chooseTarget(filename, 3, null,
        new ArrayList<DatanodeDescriptor>(), new ArrayList<Node>(), 100L);
    verifyNetworkLocations(targets, 2);
    
    
    // an in-cluster client
    targets = policy.chooseTarget(filename, 3, targets[0],
        new ArrayList<DatanodeDescriptor>(), new ArrayList<Node>(), 100L);
    verifyNetworkLocations(targets, 3);
    } finally {
      if (cluster != null) cluster.shutdown();
    }
  }
  
  private void verifyNetworkLocations(
      DatanodeDescriptor[] locations, int expectedNumOfRacks) {
    HashSet<String> racks = new HashSet<String>();
    for (DatanodeDescriptor loc : locations) {
      racks.add(loc.getNetworkLocation());
    }
    assertEquals(expectedNumOfRacks, racks.size());
  }
  
  @Test
  public void testRefreshPolicy() throws Exception{
    final int THREAD_NUM = 10;
    conf = new Configuration();
    conf.set("dfs.block.replicator.classname",
        "org.apache.hadoop.hdfs.server.namenode.BlockPlacementPolicyDefault");
    conf.setInt("dfs.datanode.handler.count", 1);
    conf.setBoolean("fs.hdfs.impl.disable.cache", true);
    // start the cluster with one datanode
    cluster = new MiniDFSCluster(conf, 1, true, rack1, host1);
    cluster.waitActive();
    namesystem = cluster.getNameNode().getNamesystem();
    
    SimpleClient clients[] = new SimpleClient[THREAD_NUM];
    for (int i = 0; i<THREAD_NUM; i++) {
      clients[i] = new SimpleClient(conf, i);
    }
    for (int i = 0; i<THREAD_NUM; i++) {
      clients[i].start();
    }
    Thread.sleep(3000);
    try {
      namesystem.reconfigurePropertyImpl("dfs.block.replicator.classname",
          "org.apache.hadoop.hdfs.server.namenode.BlockPlacementPolicyRaid");
      Assert.assertTrue("BlockPlacementPolicy type is not correct.", 
          namesystem.replicator instanceof BlockPlacementPolicyRaid);
      // set it back
      namesystem.reconfigurePropertyImpl("dfs.block.replicator.classname",
          "org.apache.hadoop.hdfs.server.namenode.BlockPlacementPolicyDefault");
      Assert.assertTrue("BlockPlacementPolicy type is not correct.", 
          namesystem.replicator instanceof BlockPlacementPolicyDefault);
      Assert.assertFalse("BlockPlacementPolicy type is not correct.", 
          namesystem.replicator instanceof BlockPlacementPolicyRaid);
    } catch (Exception e) {
      LOG.error(e);
    } finally {
      for (int i = 0; i<THREAD_NUM; i++) {
        clients[i].shutdown();
      }
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }
  
  static class SimpleClient extends Thread {
    volatile boolean shouldRun;
    Configuration conf;
    FileSystem fs;
    Path dir;

    SimpleClient(Configuration conf, int threadId) throws Exception {
      shouldRun = true;
      Path p = new Path("/"); 
      fs = FileSystem.get(p.toUri(), conf);
      dir = new Path(p, Integer.toString(threadId));
    }
    
    public void shutdown() {
      shouldRun = false;
    }

    public void run() {
      while (shouldRun) {
        try {
          fs.mkdirs(dir);
        } catch (Exception e) {
          //ignore
        }
      }
    }
  }
  
  /**
   * Test BlockPlacementPolicyRaid.CachedLocatedBlocks and
   * BlockPlacementPolicyRaid.CachedFullPathNames
   * Verify that the results obtained from cache is the same as
   * the results obtained directly
   */
  @Test
  public void testCachedResults() throws IOException {
    setupCluster();
    try {     
      refreshPolicy();
      // test blocks cache
      CachedLocatedBlocks cachedBlocks = new CachedLocatedBlocks(conf);
      String file1 = "/dir/file1";
      String file2 = "/dir/file2";
      DFSTestUtil.createFile(fs, new Path(file1), 3, (short)1, 0L);
      DFSTestUtil.createFile(fs, new Path(file2), 4, (short)1, 0L);

      FSInodeInfo inode1 = null;
      FSInodeInfo inode2 = null;
      namesystem.dir.readLock();
      try {
        inode1 = namesystem.dir.rootDir.getNode(file1);
        inode2 = namesystem.dir.rootDir.getNode(file2);
      } finally {
        namesystem.dir.readUnlock();
      }
      h = new TestBlockPlacementPolicyRaidInjectionHandler();
      InjectionHandler.set(h);
      
      verifyCachedBlocksResult(cachedBlocks, namesystem, file1, (INode)inode1, 0);
      verifyCachedBlocksResult(cachedBlocks, namesystem, file1, (INode)inode1, 1);
      verifyCachedBlocksResult(cachedBlocks, namesystem, file2, (INode)inode2, 1);
      verifyCachedBlocksResult(cachedBlocks, namesystem, file2, (INode)inode2, 2);
      try {
        Thread.sleep(1200L);
      } catch (InterruptedException e) {
      }
      verifyCachedBlocksResult(cachedBlocks, namesystem, file2, (INode)inode2, 3);
      verifyCachedBlocksResult(cachedBlocks, namesystem, file1, (INode)inode1, 4);

      // test full path cache
      CachedFullPathNames cachedFullPathNames = new CachedFullPathNames(conf);
      verifyCachedFullPathNameResult(cachedFullPathNames, inode1, 0);
      verifyCachedFullPathNameResult(cachedFullPathNames, inode1, 1);
      verifyCachedFullPathNameResult(cachedFullPathNames, inode2, 1);
      verifyCachedFullPathNameResult(cachedFullPathNames, inode2, 2);
      try {
        Thread.sleep(1200L);
      } catch (InterruptedException e) {
      }
      verifyCachedFullPathNameResult(cachedFullPathNames, inode2, 3);
      verifyCachedFullPathNameResult(cachedFullPathNames, inode1, 4);
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
      InjectionHandler.clear();
    }
  }

  /**
   * Test the result of getCompanionBlocks() on the unraided files
   */
  @Test
  public void testGetCompanionBLocks() throws IOException {
    setupCluster();
    try {
      String file1 = "/dir/file1";
      String file2 = "/raid/dir/file2";
      String file3 = "/raidrs/dir/file3";
      // Set the policy to default policy to place the block in the default way
      setBlockPlacementPolicy(namesystem, new BlockPlacementPolicyDefault(
          conf, namesystem, namesystem.clusterMap));
      DFSTestUtil.createFile(fs, new Path(file1), 3, (short)1, 0L);
      DFSTestUtil.createFile(fs, new Path(file2), 4, (short)1, 0L);
      DFSTestUtil.createFile(fs, new Path(file3), 8, (short)1, 0L);
      Collection<LocatedBlock> companionBlocks;

      companionBlocks = getCompanionBlocks(
          namesystem, policy, getBlocks(namesystem, file1).get(0).getBlock());
      Assert.assertTrue(companionBlocks == null || companionBlocks.size() == 0);

      companionBlocks = getCompanionBlocks(
          namesystem, policy, getBlocks(namesystem, file1).get(2).getBlock());
      Assert.assertTrue(companionBlocks == null || companionBlocks.size() == 0);

      companionBlocks = getCompanionBlocks(
          namesystem, policy, getBlocks(namesystem, file2).get(0).getBlock());
      Assert.assertEquals(1, companionBlocks.size());

      companionBlocks = getCompanionBlocks(
          namesystem, policy, getBlocks(namesystem, file2).get(3).getBlock());
      Assert.assertEquals(1, companionBlocks.size());

      int rsParityLength = Codec.getCodec("rs").parityLength;
      companionBlocks = getCompanionBlocks(
          namesystem, policy, getBlocks(namesystem, file3).get(0).getBlock());
      Assert.assertEquals(rsParityLength, companionBlocks.size());

      companionBlocks = getCompanionBlocks(
          namesystem, policy, getBlocks(namesystem, file3).get(4).getBlock());
      Assert.assertEquals(rsParityLength, companionBlocks.size());

      companionBlocks = getCompanionBlocks(
          namesystem, policy, getBlocks(namesystem, file3).get(6).getBlock());
      Assert.assertEquals(2, companionBlocks.size());
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  static void setBlockPlacementPolicy(
      FSNamesystem namesystem, BlockPlacementPolicy policy) {
    namesystem.writeLock();
    try {
      namesystem.replicator = policy;
    } finally {
      namesystem.writeUnlock();
    }
  }

  /**
   * Test BlockPlacementPolicyRaid actually deletes the correct replica.
   * Start 2 datanodes and create 1 source file and its parity file.
   * 1) Start host1, create the parity file with replication 1
   * 2) Start host2, create the source file with replication 2
   * 3) Set repliation of source file to 1
   * Verify that the policy should delete the block with more companion blocks.
   */
  @Test
  public void testDeleteReplica() throws IOException {
    setupCluster();
    try {
      // Set the policy to default policy to place the block in the default way
      setBlockPlacementPolicy(namesystem, new BlockPlacementPolicyDefault(
          conf, namesystem, namesystem.clusterMap));
      DatanodeDescriptor datanode1 =
        namesystem.datanodeMap.values().iterator().next();
      String source = "/dir/file";
      String parity = xorPrefix + source;

      final Path parityPath = new Path(parity);
      DFSTestUtil.createFile(fs, parityPath, 3, (short)1, 0L);
      DFSTestUtil.waitReplication(fs, parityPath, (short)1);

      // start one more datanode
      cluster.startDataNodes(conf, 1, true, null, rack2, host2, null);
      DatanodeDescriptor datanode2 = null;
      for (DatanodeDescriptor d : namesystem.datanodeMap.values()) {
        if (!d.getName().equals(datanode1.getName())) {
          datanode2 = d;
        }
      }
      Assert.assertTrue(datanode2 != null);
      cluster.waitActive();
      final Path sourcePath = new Path(source);
      DFSTestUtil.createFile(fs, sourcePath, 5, (short)2, 0L);
      DFSTestUtil.waitReplication(fs, sourcePath, (short)2);

      refreshPolicy();
      Assert.assertEquals(source,
                          policy.getSourceFile(parity, xorPrefix).name);

      List<LocatedBlock> sourceBlocks = getBlocks(namesystem, source);
      List<LocatedBlock> parityBlocks = getBlocks(namesystem, parity);
      Assert.assertEquals(5, sourceBlocks.size());
      Assert.assertEquals(3, parityBlocks.size());

      // verify the result of getCompanionBlocks()
      Collection<LocatedBlock> companionBlocks;
      companionBlocks = getCompanionBlocks(
          namesystem, policy, sourceBlocks.get(0).getBlock());
      verifyCompanionBlocks(companionBlocks, sourceBlocks, parityBlocks,
                            new int[]{0, 1}, new int[]{0});

      companionBlocks = getCompanionBlocks(
          namesystem, policy, sourceBlocks.get(1).getBlock());
      verifyCompanionBlocks(companionBlocks, sourceBlocks, parityBlocks,
                            new int[]{0, 1}, new int[]{0});

      companionBlocks = getCompanionBlocks(
          namesystem, policy, sourceBlocks.get(2).getBlock());
      verifyCompanionBlocks(companionBlocks, sourceBlocks, parityBlocks,
                            new int[]{2, 3}, new int[]{1});

      companionBlocks = getCompanionBlocks(
          namesystem, policy, sourceBlocks.get(3).getBlock());
      verifyCompanionBlocks(companionBlocks, sourceBlocks, parityBlocks,
                            new int[]{2, 3}, new int[]{1});

      companionBlocks = getCompanionBlocks(
          namesystem, policy, sourceBlocks.get(4).getBlock());
      verifyCompanionBlocks(companionBlocks, sourceBlocks, parityBlocks,
                            new int[]{4}, new int[]{2});

      companionBlocks = getCompanionBlocks(
          namesystem, policy, parityBlocks.get(0).getBlock());      
      verifyCompanionBlocks(companionBlocks, sourceBlocks, parityBlocks,
                            new int[]{0, 1}, new int[]{0});

      companionBlocks = getCompanionBlocks(
          namesystem, policy, parityBlocks.get(1).getBlock());
      verifyCompanionBlocks(companionBlocks, sourceBlocks, parityBlocks,
                            new int[]{2, 3}, new int[]{1});

      companionBlocks = getCompanionBlocks(
          namesystem, policy, parityBlocks.get(2).getBlock());
      verifyCompanionBlocks(companionBlocks, sourceBlocks, parityBlocks,
                            new int[]{4}, new int[]{2});

      // Set the policy back to raid policy. We have to create a new object
      // here to clear the block location cache
      refreshPolicy();
      setBlockPlacementPolicy(namesystem, policy);
      // verify policy deletes the correct blocks. companion blocks should be
      // evenly distributed.
      fs.setReplication(sourcePath, (short)1);
      DFSTestUtil.waitReplication(fs, sourcePath, (short)1);
      Map<String, Integer> counters = new HashMap<String, Integer>();
      refreshPolicy();
      for (int i = 0; i < parityBlocks.size(); i++) {
        companionBlocks = getCompanionBlocks(
            namesystem, policy, parityBlocks.get(i).getBlock());

        counters = BlockPlacementPolicyRaid.countCompanionBlocks(
            companionBlocks)[0];
        Assert.assertTrue(counters.get(datanode1.getName()) >= 1 &&
                          counters.get(datanode1.getName()) <= 2);
        Assert.assertTrue(counters.get(datanode1.getName()) +
                          counters.get(datanode2.getName()) ==
                          companionBlocks.size());

        counters = BlockPlacementPolicyRaid.countCompanionBlocks(
            companionBlocks)[1];
        Assert.assertTrue(counters.get(datanode1.getParent().getName()) >= 1 &&
                          counters.get(datanode1.getParent().getName()) <= 2);
        Assert.assertTrue(counters.get(datanode1.getParent().getName()) +
                          counters.get(datanode2.getParent().getName()) ==
                          companionBlocks.size());
      }
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }
  
  @Test
  public void testParentPath() {
    String path = null;

    path = "/foo/bar";
    Assert.assertEquals(new Path(path).getParent().toString(),
        BlockPlacementPolicyRaid.getParentPath(path));

    path = "/foo/bar/";
    Assert.assertEquals(new Path(path).getParent().toString(),
        BlockPlacementPolicyRaid.getParentPath(path));

    path = "/foo";
    Assert.assertEquals(new Path(path).getParent().toString(),
        BlockPlacementPolicyRaid.getParentPath(path));

    path = "/foo/";
    Assert.assertEquals(new Path(path).getParent().toString(),
        BlockPlacementPolicyRaid.getParentPath(path));
  }

  // create a new BlockPlacementPolicyRaid to clear the cache
  private void refreshPolicy() {
      policy = new BlockPlacementPolicyRaid();
      policy.initialize(conf, namesystem, namesystem.clusterMap, null, null, namesystem);
  }

  private void verifyCompanionBlocks(Collection<LocatedBlock> companionBlocks,
      List<LocatedBlock> sourceBlocks, List<LocatedBlock> parityBlocks,
      int[] sourceBlockIndexes, int[] parityBlockIndexes) {
    Set<Block> blockSet = new HashSet<Block>();
    for (LocatedBlock b : companionBlocks) {
      blockSet.add(b.getBlock());
    }
    Assert.assertEquals(sourceBlockIndexes.length + parityBlockIndexes.length,
                        blockSet.size());
    for (int index : sourceBlockIndexes) {
      Assert.assertTrue(blockSet.contains(sourceBlocks.get(index).getBlock()));
    }
    for (int index : parityBlockIndexes) {
      Assert.assertTrue(blockSet.contains(parityBlocks.get(index).getBlock()));
    }
  }
  
  private void verifyCachedFullPathNameResult(
      CachedFullPathNames cachedFullPathNames, FSInodeInfo inode, int cachedReads)
      throws IOException {
    Assert.assertEquals(inode.getFullPathName(),
                        policy.getFullPathName(inode));
    Assert.assertEquals(cachedReads,
        h.events.get(InjectionEvent.BLOCKPLACEMENTPOLICYRAID_CACHED_PATH).intValue());
  }

  private void verifyCachedBlocksResult(CachedLocatedBlocks cachedBlocks,
      FSNamesystem namesystem, String file, INode f, int cachedReads) throws IOException{
    long len = namesystem.getFileInfo(file).getLen();
    List<LocatedBlock> res1 =
        namesystem.getBlockLocations(file, 0L, len).getLocatedBlocks();
    List<LocatedBlock> res2 = policy.getLocatedBlocks(file, f);
    for (int i = 0; i < res1.size(); i++) {
      Assert.assertEquals(res1.get(i).getBlock(), res2.get(i).getBlock());
    }
    Assert.assertEquals(cachedReads,
        h.events.get(InjectionEvent.BLOCKPLACEMENTPOLICYRAID_CACHED_BLOCKS).intValue());
  }


  private Collection<LocatedBlock> getCompanionBlocks(
      FSNamesystem namesystem, BlockPlacementPolicyRaid policy,
      Block block) throws IOException {
    INodeFile inode = namesystem.blocksMap.getINode(block);
    BlockPlacementPolicyRaid.FileInfo info = policy.getFileInfo(inode, inode.getFullPathName());
    return policy.getCompanionBlocks(inode.getFullPathName(), info, block, inode);
  }

  private List<LocatedBlock> getBlocks(FSNamesystem namesystem, String file) 
      throws IOException {
    FileStatus stat = namesystem.getFileInfo(file);
    return namesystem.getBlockLocations(
               file, 0, stat.getLen()).getLocatedBlocks();
  }
  
  class TestBlockPlacementPolicyRaidInjectionHandler extends InjectionHandler {
    Map<InjectionEventI, Integer> events = new HashMap<InjectionEventI, Integer>();
    
    public TestBlockPlacementPolicyRaidInjectionHandler() {
      events.put(InjectionEvent.BLOCKPLACEMENTPOLICYRAID_CACHED_BLOCKS, 0);
      events.put(InjectionEvent.BLOCKPLACEMENTPOLICYRAID_CACHED_PATH, 0);
    }
    @Override
    public void _processEvent(InjectionEventI event, Object... args) {
      if (event == InjectionEvent.BLOCKPLACEMENTPOLICYRAID_CACHED_BLOCKS ||
          event == InjectionEvent.BLOCKPLACEMENTPOLICYRAID_CACHED_PATH) {
        events.put(event, events.get(event) + 1);
      }
    }
  }
}
