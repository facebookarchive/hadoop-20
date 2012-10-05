package org.apache.hadoop.hdfs;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.ExecutionException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.tools.FastCopy;

import org.junit.AfterClass;
import static org.junit.Assert.*;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Tests that the FastCopy tool correctly throws an exception when all datanodes
 * for a particular block are down. Ensures we don't end up in an infinite loop.
 */
public class TestFastCopyDeadDataNodes {

  private static MiniDFSCluster cluster;
  private static Configuration conf;
  private static FileSystem fs;
  private static int BLOCK_SIZE = 1024;
  private static int MAX_BLOCKS = 20;
  private static int MAX_FILE_SIZE = MAX_BLOCKS * BLOCK_SIZE;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    conf = new Configuration();
    conf.setInt("ipc.client.connect.max.retries", 3);
    conf.setInt("dfs.block.size", BLOCK_SIZE);
    conf.setInt("dfs.fastcopy.max.datanode.errors", 1);
    conf.setBoolean("dfs.permissions", false);
    cluster = new MiniDFSCluster(conf, 9, true, null);
    fs = cluster.getFileSystem();
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    cluster.shutdown();
  }

  @Test(timeout = 2 * 60 * 1000)
  public void testDeadDatanodes() throws Exception {
    DFSTestUtil util = new DFSTestUtil("testDeadDatanodes", 1, 1, MAX_FILE_SIZE);
    String topDir = "/testDeadDatanodes";
    util.createFiles(fs, topDir);
    FastCopy fastCopy = new FastCopy(conf);

    // Find the locations for the last block of the file.
    String filename = util.getFileNames(topDir)[0];
    LocatedBlocks lbks = cluster.getNameNode().getBlockLocations(filename, 0,
        Long.MAX_VALUE);
    assertNotNull(lbks);
    LocatedBlock lastBlock = lbks.get(lbks.locatedBlockCount() - 1);

    // Shutdown all datanodes that have the last block of the file.
    for (DatanodeInfo dnInfo : lastBlock.getLocations()) {
      InetSocketAddress addr = new InetSocketAddress(dnInfo.getHost(),
          dnInfo.getPort());
      for (int i = 0; i < cluster.getDataNodes().size(); i++) {
        DataNode dn = cluster.getDataNodes().get(i);
        if (dn.getSelfAddr().equals(addr)) {
          cluster.shutdownDataNode(i, true);
        }
      }
    }

    // Now run FastCopy
    try {
      for (String fileName : util.getFileNames(topDir)) {
        fastCopy.copy(fileName, fileName + "dst", (DistributedFileSystem) fs,
            (DistributedFileSystem) fs);
      }

    } catch (ExecutionException e) {
      System.out.println("Expected exception : " + e);
      assertEquals(IOException.class, e.getCause().getClass());
      return;
    } finally {
      fastCopy.shutdown();
    }
    fail("No exception thrown");
  }
}
