package org.apache.hadoop.hdfs.server.namenode;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Map;
import java.util.List;
import java.util.concurrent.ExecutionException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.namenode.DatanodeDescriptor;
import org.apache.hadoop.hdfs.server.protocol.BlockCommand;
import org.apache.hadoop.hdfs.tools.FastCopy;

import org.junit.AfterClass;
import static org.junit.Assert.*;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Tests that the FastCopy tool does not incorrectly exclude datanodes.
 */
public class TestFastCopyDeletedBlocks {

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
    cluster = new MiniDFSCluster(conf, 3, true, null);
    fs = cluster.getFileSystem();
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    cluster.shutdown();
  }

  @Test
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

    int namespaceID = cluster.getNameNode().getNamespaceID();
    DataNode dn = cluster.getDataNodes().get(0);
    DatanodeID dnId = dn.getDNRegistrationForNS(namespaceID);
    List <Block> deleteList = new ArrayList <Block> ();
    for(LocatedBlock block : lbks.getLocatedBlocks()) {
      deleteList.add(block.getBlock());
    }

    assertEquals(lbks.locatedBlockCount(),
        dn.getFSDataset().getBlockReport(namespaceID).length);
    DatanodeDescriptor dnDs = cluster.getNameNode().namesystem.getDatanode(dnId);
    dnDs.addBlocksToBeInvalidated(deleteList);

    // Make sure all blocks are deleted.
    while(dn.getFSDataset().getBlockReport(namespaceID).length != 0) {
      Thread.sleep(1000);
    }

    // Now run FastCopy
    try {
      for (String fileName : util.getFileNames(topDir)) {
        fastCopy.copy(fileName, fileName + "dst", (DistributedFileSystem) fs,
            (DistributedFileSystem) fs);
      }
    } finally {
      fastCopy.shutdown();
    }

    // Make sure no errors are reported.
    Map<DatanodeInfo, Integer> dnErrors = fastCopy.getDatanodeErrors();
    assertEquals(0, dnErrors.size());
  }
}
