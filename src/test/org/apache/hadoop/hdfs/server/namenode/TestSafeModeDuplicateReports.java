package org.apache.hadoop.hdfs.server.namenode;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.FSConstants.DatanodeReportType;
import org.apache.hadoop.hdfs.protocol.FSConstants.SafeModeAction;
import org.apache.hadoop.hdfs.server.namenode.DatanodeDescriptor;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.namenode.NameNode;

import org.junit.After;
import static org.junit.Assert.*;
import org.junit.Test;

/**
 * This verifies that when the NameNode is in safemode and it receives
 * duplicated block reports from the same datanode, it still maintains a correct
 * count of safe blocks.
 */
public class TestSafeModeDuplicateReports {

  private static MiniDFSCluster cluster;
  private static Configuration conf;
  private final static int BLOCK_SIZE = 1024;
  private final static int MAX_BLOCKS = 50;
  private final static int MAX_FILE_SIZE = MAX_BLOCKS * BLOCK_SIZE;
  private static FileSystem fs;
  private static final long MAX_WAIT_TIME = 3 * 1000;

  @After
  public void tearDown() throws Exception {
    cluster.shutdown();
  }

  @Test
  public void testSafeModeDuplicateBlocks() throws Exception {
    conf = new Configuration();
    conf.setInt("dfs.block.size", BLOCK_SIZE);
    conf.setLong("dfs.heartbeat.interval", 1);
    cluster = new MiniDFSCluster(conf, 1, true, null);
    fs = cluster.getFileSystem();
    
    // Create data.
    String test = "/testSafeModeDuplicateBlocks";
    DFSTestUtil util = new DFSTestUtil(test, 10, 1, MAX_FILE_SIZE);
    util.createFiles(fs, test);
    fs.close();
    cluster.shutdown();


    // Restart the cluster with NN in manual safemode.
    conf.setLong("dfs.blockreport.intervalMsec", 200);
    cluster = new MiniDFSCluster(conf, 0, false, null);
    NameNode nn = cluster.getNameNode();
    FSNamesystem ns = nn.namesystem;
    nn.setSafeMode(SafeModeAction.SAFEMODE_ENTER);
    cluster.startDataNodes(conf, 1, true, null, null);
    cluster.waitActive();

    long start = System.currentTimeMillis();
    // Wait for atleast 3 full block reports from the datanode.
    while (System.currentTimeMillis() - start <= MAX_WAIT_TIME) {

      // This makes sure we trigger, redudant addStoredBlocks() on the NameNode.
      for (DatanodeInfo dn : ns.datanodeReport(DatanodeReportType.ALL)) {
        ns.unprotectedRemoveDatanode(ns.getDatanode(dn));
      }
      Thread.sleep(200);
    }

    // Verify atleast 3 full block reports took place.
    assertTrue(3 <= NameNode.getNameNodeMetrics().numBlockReport
        .getCurrentIntervalValue());

    // Verify the total number of safe blocks.
    long totalBlocks = ns.getBlocksTotal();
    long safeBlocks = ns.getSafeBlocks();
    assertEquals(totalBlocks, safeBlocks);
  }

}
