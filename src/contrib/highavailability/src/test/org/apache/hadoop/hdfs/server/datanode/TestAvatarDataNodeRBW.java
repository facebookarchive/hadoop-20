package org.apache.hadoop.hdfs.server.datanode;

import java.io.IOException;
import java.util.List;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
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
import org.junit.After;
import org.junit.AfterClass;
import static org.junit.Assert.*;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestAvatarDataNodeRBW {
  
  final static Log LOG = LogFactory.getLog(TestAvatarDataNodeRBW.class);
  
  private Configuration conf;
  private MiniAvatarCluster cluster;
  private static FileSystem dafs;
  private static int FILE_LEN = 1024 * 20 + 256;
  private static int BLOCKS = 20;
  private static int BLOCK_SIZE = 1024;
  private static Random random = new Random();
  private static byte[] buffer = new byte[FILE_LEN];
  private static int MAX_WAIT_TIME = 30 * 1000;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    MiniAvatarCluster.createAndStartZooKeeper();
  }

  public void setUp(String name) throws Exception {
    LOG.info("------------------- test: " + name + " START ----------------");
    conf = new Configuration();
    conf.setInt("dfs.block.size", BLOCK_SIZE);
    conf.setInt("dfs.replication.min", 3);
    conf.setBoolean("dfs.support.append", true);
    cluster = new MiniAvatarCluster(conf, 3, true, null, null);
    dafs = cluster.getFileSystem();
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    MiniAvatarCluster.shutDownZooKeeper();
  }

  @After
  public void tearDown() throws Exception {
    dafs.close();
    cluster.shutDown();
  }

  private void createRBWFile(String fileName) throws IOException {
    FSDataOutputStream out = dafs.create(new Path(fileName));
    random.nextBytes(buffer);
    out.write(buffer);
    out.sync();
  }

  private int initializeTest(String testName) throws IOException {
    String fileName = testName;
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
    return blocksBefore;
  }

  private void verifyResults(int blocksBefore, String fileName)
    throws IOException {
    // Verify we have RBWs after restart.
    AvatarNode avatarAfter = cluster.getPrimaryAvatar(0).avatar;
    LocatedBlocks lbks = avatarAfter.namesystem
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

  private boolean blocksReceived(int nBlocks, String fileName) throws IOException {
    AvatarNode avatar = cluster.getPrimaryAvatar(0).avatar;
    LocatedBlocks lbks = avatar.namesystem.getBlockLocations(fileName, 0,
        Long.MAX_VALUE);
    int blocks = lbks.locatedBlockCount();
    if (blocks != nBlocks)
      return false;
    for (LocatedBlock lbk : lbks.getLocatedBlocks()) {
      DatanodeInfo[] locs = lbk.getLocations();
      if (locs == null || locs.length == 0) {
        return false;
      }
    }
    return true;
  }

  @Test
  public void testAvatarDatanodeRBW() throws Exception {
    setUp("testAvatarDatanodeRBW");
    String testCase = "/testAvatarDatanodeRBW";
    int blocksBefore = initializeTest(testCase);
    // Restart avatar nodes.
    cluster.restartAvatarNodes();
    verifyResults(blocksBefore, testCase);
  }

  @Test
  public void testAvatarDatanodeRBWFailover() throws Exception {
    setUp("testAvatarDatanodeRBWFailover");
    String testCase = "/testAvatarDatanodeRBWFailover";
    int blocksBefore = initializeTest(testCase);
    // Perform a failover.
    cluster.failOver();

    long start = System.currentTimeMillis();
    // Wait for standby safe mode to catch up to all blocks.
    while (System.currentTimeMillis() - start <= MAX_WAIT_TIME
        && !blocksReceived(blocksBefore, testCase)) {
      Thread.sleep(1000);
    }
    verifyResults(blocksBefore, testCase);
  }
}
