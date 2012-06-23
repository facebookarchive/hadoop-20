package org.apache.hadoop.hdfs.server.namenode;

import java.io.IOException;
import java.util.Collection;
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
import org.apache.hadoop.raid.Codec;
import org.apache.hadoop.raid.RaidNode;
import org.apache.hadoop.raid.RaidUtils;
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

  String[] rack1 = {"/rack1", "/rack1", "/rack1"};
  String[] host1 = {"host1.rack1.com", "host2.rack1.com", "host3.rack1.com"};

  final static Log LOG =
      LogFactory.getLog(TestDirectoryRaidBlockPlacement.class);

  protected void setupCluster() throws IOException {
    conf = new Configuration();
    conf.setLong("dfs.blockreport.intervalMsec", 1000L);
    conf.set("dfs.replication.pending.timeout.sec", "2");
    conf.setLong("dfs.block.size", 1L);
    conf.set("dfs.block.replicator.classname",
        "org.apache.hadoop.hdfs.server.namenode.BlockPlacementPolicyRaid");
    Utils.loadTestCodecs(conf, 5, 5, 1, 3, "/raid", "/raidrs", false, true);
    conf.setInt("io.bytes.per.checksum", 1);

    cluster = new MiniDFSCluster(conf, 3, true, rack1, host1);
    cluster.waitActive();
    namesystem = cluster.getNameNode().getNamesystem();
    Assert.assertTrue("BlockPlacementPolicy type is not correct.",
        namesystem.replicator instanceof BlockPlacementPolicyRaid);
    policy = (BlockPlacementPolicyRaid) namesystem.replicator;
    fs = cluster.getFileSystem();
  }

  protected void closeCluster() throws IOException {
    if (null != cluster) {
      cluster.shutdown();
    }
  }

  @Test
  public void testGetCompanionBLocks() throws IOException {

    try {
      setupCluster();
      String[] files = new String[] {"/dir/file1", "/dir/file2", "/dir/file3"};
      Codec codec = Codec.getCodec("rs");
      for (String file : files) {
        TestRaidDfs.createTestFile(fs, new Path(file), 3, 2, 8192L);
      }
      FileStatus stat = fs.getFileStatus(new Path("/dir"));
      RaidNode.doDirRaid(conf, stat, new Path(codec.parityDirectory), codec, 
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
    return policy.getCompanionBlocks(inode.getFullPathName(), info, block);
  }
  
  private List<LocatedBlock> getBlocks(FSNamesystem namesystem, String file) 
      throws IOException {
    FileStatus stat = namesystem.getFileInfo(file);
    return namesystem.getBlockLocations(
               file, 0, stat.getLen()).getLocatedBlocks();
  }

}
