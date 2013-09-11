package org.apache.hadoop.hdfs.server.namenode;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;

import org.junit.AfterClass;
import static org.junit.Assert.*;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestEmptyAbandonBlock {

  private static MiniDFSCluster cluster;
  private static Configuration conf;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    conf = new Configuration();
    cluster = new MiniDFSCluster(conf, 3, true, null);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    cluster.shutdown();
  }

  @Test
  public void testAbandon() throws Exception {
    NameNode nn = cluster.getNameNode();
    DistributedFileSystem fs = (DistributedFileSystem) cluster.getFileSystem();
    String fileName = "/testAbandon";
    fs.create(new Path(fileName));
    LocatedBlock lbk = nn.addBlock(fileName, fs.getClient().getClientName());
    INodeFileUnderConstruction cons = (INodeFileUnderConstruction) nn.namesystem.dir
        .getINode(fileName);
    cons.setTargets(null, -1);
    nn.abandonBlock(lbk.getBlock(), fileName, fs.getClient().getClientName());
    assertEquals(0, nn.getBlockLocations(fileName, 0, Long.MAX_VALUE)
        .locatedBlockCount());
  }

}
