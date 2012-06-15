package org.apache.hadoop.hdfs.server.datanode;

import java.io.IOException;
import java.util.List;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniAvatarCluster;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.server.namenode.AvatarNode;
import org.junit.AfterClass;
import static org.junit.Assert.*;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestAvatarDataNodeRBW {
  private static Configuration conf;
  private static MiniAvatarCluster cluster;
  private static FileSystem dafs;
  private static int FILE_LEN = 1024 * 20 + 256;
  private static int BLOCKS = 20;
  private static int BLOCK_SIZE = 1024;
  private static Random random = new Random();
  private static byte[] buffer = new byte[FILE_LEN];

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    MiniAvatarCluster.createAndStartZooKeeper();
    conf = new Configuration();
    conf.setInt("dfs.block.size", BLOCK_SIZE);
    conf.setInt("dfs.replication.min", 3);
    conf.setBoolean("dfs.support.append", true);
    cluster = new MiniAvatarCluster(conf, 3, true, null, null);
    dafs = cluster.getFileSystem();
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    dafs.close();
    cluster.shutDown();
    MiniAvatarCluster.shutDownZooKeeper();
  }

  private void createRBWFile(String fileName) throws IOException {
    FSDataOutputStream out = dafs.create(new Path(fileName));
    random.nextBytes(buffer);
    out.write(buffer);
    out.sync();
  }

  @Test
  public void testAvatarDatanodeRBW() throws Exception {
    String fileName = "/testAvatarDatanodeRBW";
    createRBWFile(fileName);
    // Verify we have 1 RBW block.
    AvatarNode avatar = cluster.getPrimaryAvatar(0).avatar;
    LocatedBlocks lbks = avatar.namesystem.getBlockLocations(fileName, 0,
        Long.MAX_VALUE);
    int blocksBefore = lbks.locatedBlockCount();
    for (LocatedBlock lbk : lbks.getLocatedBlocks()) {
      DatanodeInfo[] locs = lbk.getLocations();
      assertNotNull(locs);
      assertTrue(locs.length != 0);
    }

    // Restart avatar nodes.
    cluster.restartAvatarNodes();

    // Verify we have RBWs after restart.
    AvatarNode avatarAfter = cluster.getPrimaryAvatar(0).avatar;
    lbks = avatarAfter.namesystem
        .getBlockLocations(fileName, 0,
        Long.MAX_VALUE);
    long blocksAfter = lbks.locatedBlockCount();

    System.out.println("blocksBefore : " + blocksBefore + " blocksAfter : "
        + blocksAfter);

    assertEquals(blocksBefore, blocksAfter);
    for (LocatedBlock lbk : lbks.getLocatedBlocks()) {
      DatanodeInfo[] locs = lbk.getLocations();
      assertNotNull(locs);
      assertTrue(locs.length != 0);
    }
  }
}
