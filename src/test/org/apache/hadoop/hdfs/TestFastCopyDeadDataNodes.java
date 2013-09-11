package org.apache.hadoop.hdfs;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.ExecutionException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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
  private static int BLOCK_SIZE = 1024;
  private static int MAX_BLOCKS = 20;
  private static int MAX_FILE_SIZE = MAX_BLOCKS * BLOCK_SIZE;
  private static int NUM_NAME_NODES = 2;
  private static FileSystem[] fileSystems = new FileSystem[NUM_NAME_NODES];

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    conf = new Configuration();
    conf.setInt("ipc.client.connect.max.retries", 3);
    conf.setInt("dfs.block.size", BLOCK_SIZE);
    conf.setInt("dfs.fastcopy.max.datanode.errors", 1);
    conf.setBoolean("dfs.permissions", false);
    cluster = new MiniDFSCluster(conf, 9, true, null, NUM_NAME_NODES);
    
    for (int i = 0; i < NUM_NAME_NODES; i++) {
      fileSystems[i] = cluster.getFileSystem(i);
    }
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    cluster.shutdown();
  }

  @Test(timeout = 2 * 60 * 1000)
  public void testDeadDatanodesSameFS() throws Exception {
    DFSTestUtil util = new DFSTestUtil("testDeadDatanodesSameFS", 1, 1, MAX_FILE_SIZE);
    String topDir = "/testDeadDatanodesSameFS";
    FileSystem fs = fileSystems[0];
    util.createFiles(fs, topDir);
    FastCopy fastCopy = new FastCopy(conf);

    // Find the locations for the last block of the file.
    String filename = util.getFileNames(topDir)[0];
    LocatedBlocks lbks = cluster.getNameNode(0).getBlockLocations(filename, 0,
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
      DistributedFileSystem dfs = (DistributedFileSystem) fs;
      for (String fileName : util.getFileNames(topDir)) {
        fastCopy.copy(fileName, fileName + "dst", dfs, dfs);
        assertTrue(fs.exists(new Path(fileName + "dst")));
        // assert the hard links.
        String[] hardlinks = dfs.getHardLinkedFiles(new Path(fileName));
        for (String hardLink : hardlinks) {
          assertEquals(hardLink, fileName + "dst");
        }
      }

    } catch (ExecutionException e) {
      System.out.println("Expected exception : " + e);
      assertEquals(IOException.class, e.getCause().getClass());
      return;
    } finally {
      fastCopy.shutdown();
    }
  }
  
  @Test(timeout = 2 * 60 * 1000)
  public void testDeadDatanodesCrossFS() throws Exception {
    DFSTestUtil util = new DFSTestUtil("testDeadDatanodesCrossFS", 1, 1, MAX_FILE_SIZE);
    String topDir = "/testDeadDatanodesCrossFS";
    FileSystem fs1 = fileSystems[0];
    FileSystem fs2 = fileSystems[1];
    util.createFiles(fs1, topDir);
    FastCopy fastCopy = new FastCopy(conf);

    // Find the locations for the last block of the file.
    String filename = util.getFileNames(topDir)[0];
    LocatedBlocks lbks = cluster.getNameNode(0).getBlockLocations(filename, 0,
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
        fastCopy.copy(fileName, fileName + "dst", (DistributedFileSystem) fs1, 
            (DistributedFileSystem) fs2);
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
