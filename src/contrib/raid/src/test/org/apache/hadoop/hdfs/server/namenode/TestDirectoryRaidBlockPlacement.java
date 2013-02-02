package org.apache.hadoop.hdfs.server.namenode;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.TestRaidDfs;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.server.namenode.BlocksMap.BlockInfo;
import org.apache.hadoop.raid.Codec;
import org.apache.hadoop.raid.RaidNode;
import org.apache.hadoop.raid.RaidUtils;
import org.apache.hadoop.raid.TestDirectoryRaidDfs;
import org.apache.hadoop.raid.Utils;
import org.junit.Test;

import junit.framework.Assert;
import junit.framework.TestCase;

public class TestDirectoryRaidBlockPlacement extends TestCase {
  private Configuration conf = null;
  private MiniDFSCluster cluster = null;
  private FSNamesystem namesystem = null;
  private BlockPlacementPolicyRaid policy = null;
  private FileSystem fs = null;
  private static long CAPACITY = 10240000L;

  String[] rack1 = {"/rack1", "/rack1", "/rack1"};
  String[] host1 = {"host1.rack1.com", "host2.rack1.com", "host3.rack1.com"};

  final static Log LOG =
      LogFactory.getLog(TestDirectoryRaidBlockPlacement.class);

  protected void setupCluster(boolean simulated) throws IOException {
    conf = new Configuration();
    conf.setLong("dfs.blockreport.intervalMsec", 1000L);
    conf.set("dfs.replication.pending.timeout.sec", "2");
    conf.setLong("dfs.block.size", 1L);
    conf.set("dfs.block.replicator.classname",
        "org.apache.hadoop.hdfs.server.namenode.BlockPlacementPolicyRaid");
    conf.setLong("hdfs.raid.min.filesize", 1L);
    Utils.loadTestCodecs(conf, 5, 5, 1, 3, "/raid", "/raidrs", false, true);
    conf.setInt("io.bytes.per.checksum", 1);

    if (!simulated) {
      cluster = new MiniDFSCluster(conf, 3, true, rack1, host1);
    } else {
      long[] capacities = new long[]{CAPACITY, CAPACITY, CAPACITY};
      cluster = new MiniDFSCluster(0, conf, 3, true, true, null, 
        rack1, capacities);
    }
    cluster.waitActive();
    namesystem = cluster.getNameNode().getNamesystem();
    Assert.assertTrue("BlockPlacementPolicy type is not correct.",
        namesystem.replicator instanceof BlockPlacementPolicyRaid);
    policy = (BlockPlacementPolicyRaid) namesystem.replicator;
    fs = cluster.getFileSystem();
    TestDirectoryRaidDfs.setupStripeStore(conf, fs);
  }

  protected void closeCluster() throws IOException {
    if (null != cluster) {
      cluster.shutdown();
    }
  }
  
  /*
   * This test start datanodes with simulated mode and keep running
   * chooseReplicaToDelete multiple times to get the average processing time
   * and number of allocated objects
   */
  @Test
  public void testDirXORChooseReplicasToDeletePerformance() throws Exception {
    try {
      setupCluster(true);
      // create test files
      int numFiles = 1000;
      long blockSize = 1024L;
      String parentDir = "/dir/";
      for (int i = 0; i < numFiles; i++) {
        String file = parentDir + "file" + i;
        TestRaidDfs.createTestFile(fs, new Path(file), 3, 1, blockSize);
      }
      LOG.info("Created " + numFiles + " files");
      Codec code = Codec.getCodec("xor");
      FSNamesystem fsNameSys = cluster.getNameNode().namesystem;
      for (DatanodeDescriptor dd: fsNameSys.datanodeMap.values()) {
        LOG.info(dd);
      }
      // create fake parity file
      long numStripes = RaidNode.numStripes(numFiles, code.stripeLength);
      TestRaidDfs.createTestFile(fs, new Path(code.parityDirectory, "dir"), 3,
          (int)numStripes * code.parityLength, blockSize);
      long startTime = System.currentTimeMillis();
      long total = 0L;
      fsNameSys.readLock();
      for (BlocksMap.BlockInfo bi : fsNameSys.blocksMap.getBlocks()) {
        fsNameSys.replicator.chooseReplicaToDelete(bi.getINode(),
            bi, (short)3, fsNameSys.datanodeMap.values(),
            new ArrayList<DatanodeDescriptor>());
        total++;
      }
      fsNameSys.readUnlock();
      LOG.info("Average chooseReplicaToDelete time: " +
          ((double)(System.currentTimeMillis() - startTime) / total));
    } finally {
      closeCluster();
    }
  }

  @Test
  public void testGetCompanionBLocks() throws IOException {

    try {
      setupCluster(false);
      String[] files = new String[] {"/dir/file1", "/dir/file2", "/dir/file3"};
      Codec codec = Codec.getCodec("rs");
      for (String file : files) {
        TestRaidDfs.createTestFile(fs, new Path(file), 3, 2, 8192L);
      }
      FileStatus stat = fs.getFileStatus(new Path("/dir"));
      RaidNode.doRaid(conf, stat, new Path(codec.parityDirectory), codec, 
          new RaidNode.Statistics(), 
          RaidUtils.NULL_PROGRESSABLE, false, 1, 1);       
      
      Collection<LocatedBlock> companionBlocks;
      
      for (int i=0; i<2; i++) {
        for (int j=0; j<2; j++) {
          companionBlocks = getCompanionBlocks(
              namesystem, policy, getBlocks(namesystem, 
                  files[i]).get(j).getBlock());
          Assert.assertEquals(8, companionBlocks.size());
        }
      }
      
      companionBlocks = getCompanionBlocks(
          namesystem, policy, getBlocks(namesystem, 
              files[2]).get(0).getBlock());
      Assert.assertEquals(8, companionBlocks.size());
      
      companionBlocks = getCompanionBlocks(
          namesystem, policy, getBlocks(namesystem, 
              files[2]).get(1).getBlock());
      Assert.assertEquals(4, companionBlocks.size());
      
      String parityFile = "/raidrs/dir";
      
      for (int i=0; i<3; i++) {
        companionBlocks = getCompanionBlocks(
            namesystem, policy, getBlocks(namesystem, 
                parityFile).get(i).getBlock());
        Assert.assertEquals(8, companionBlocks.size());
      }
      
      for (int i=3; i<6; i++) {
        companionBlocks = getCompanionBlocks(
            namesystem, policy, getBlocks(namesystem, 
                parityFile).get(i).getBlock());
        Assert.assertEquals(4, companionBlocks.size());
      }
    } finally {
      closeCluster();
    }
  }
  
  private Collection<LocatedBlock> getCompanionBlocks(
      FSNamesystem namesystem, BlockPlacementPolicyRaid policy,
      Block block) throws IOException {
    INodeFile inode = namesystem.blocksMap.getINode(block);
    BlockPlacementPolicyRaid.FileInfo info = 
        policy.getFileInfo(inode.getFullPathName());
    return policy.getCompanionBlocks(inode.getFullPathName(), info, block, inode);
  }
  
  private List<LocatedBlock> getBlocks(FSNamesystem namesystem, String file) 
      throws IOException {
    FileStatus stat = namesystem.getFileInfo(file);
    return namesystem.getBlockLocations(
               file, 0, stat.getLen()).getLocatedBlocks();
  }

}
