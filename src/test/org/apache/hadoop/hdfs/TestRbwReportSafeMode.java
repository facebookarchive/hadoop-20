package org.apache.hadoop.hdfs;

import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSOutputStream;
import org.apache.hadoop.hdfs.protocol.FSConstants.SafeModeAction;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;

import org.junit.AfterClass;
import static org.junit.Assert.*;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestRbwReportSafeMode {

  private static Configuration conf;
  private static MiniDFSCluster cluster;
  private static FileSystem fs;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    conf = new Configuration();
    conf.setInt("dfs.block.size", 1024);
    conf.setFloat("dfs.safemode.threshold.pct", 1.5f);
    cluster = new MiniDFSCluster(conf, 1, true, null, false);
    cluster.getNameNode().setSafeMode(SafeModeAction.SAFEMODE_LEAVE);
    fs = cluster.getFileSystem();
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    fs.close();
    cluster.shutdown();
  }

  private void waitForBlocks() throws Exception {
    FSNamesystem namesystem = cluster.getNameNode().namesystem;
    long totalBlocks = namesystem.getBlocksTotal();
    long safeBlocks = namesystem.getSafeBlocks();
    while (totalBlocks > safeBlocks) {
      System.out.println("Waiting for blocks, total : " + totalBlocks
          + " safe : " + safeBlocks);
      Thread.sleep(1000);
      totalBlocks = namesystem.getBlocksTotal();
      safeBlocks = namesystem.getSafeBlocks();
    }
  }

  @Test
  public void testRBW() throws Exception {
    String fileName = "/testRBW";
    FSDataOutputStream out = fs.create(new Path(fileName));
    // Create RBW.
    byte[] buffer = new byte[1024 * 10 + 100];
    Random r = new Random();
    r.nextBytes(buffer);
    out.write(buffer);
    out.sync();

    cluster.restartNameNode(0, new String[] {}, false);
    ((DFSOutputStream) out.getWrappedStream()).abortForTests();

    // Send multiple RBW reports.
    waitForBlocks();
    cluster.restartDataNodes();
    Thread.sleep(10000);

    System.out.println("Restarts done");
    FSNamesystem namesystem = cluster.getNameNode().namesystem;

    long totalBlocks = namesystem.getBlocksTotal();
    long safeBlocks = namesystem.getSafeBlocks();
    long startTime = System.currentTimeMillis();
    while (totalBlocks != safeBlocks
        && (System.currentTimeMillis() - startTime < 15000)) {
      Thread.sleep(1000);
      System.out.println("Waiting for blocks, Total : " + totalBlocks
          + " Safe : " + safeBlocks);
      totalBlocks = namesystem.getBlocksTotal();
      safeBlocks = namesystem.getSafeBlocks();
    }

    assertEquals(11, totalBlocks);
    assertEquals(totalBlocks, safeBlocks);
    for (DataNode dn : cluster.getDataNodes()) {
      assertEquals(1, dn.data.getBlocksBeingWrittenReport(cluster.getNameNode()
          .getNamespaceID()).length);
    }
  }
}
