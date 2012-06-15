package org.apache.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.FSConstants.SafeModeAction;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.junit.AfterClass;
import static org.junit.Assert.*;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestAbandonBlockEditLog {
  private static Configuration conf;
  private static MiniDFSCluster cluster;
  private static FileSystem fs;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    conf = new Configuration();
    conf.setBoolean("dfs.persist.blocks", true);
    cluster = new MiniDFSCluster(conf, 3, true, null);
    fs = cluster.getFileSystem();
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    cluster.shutdown();
  }

  @Test
  public void testEditLog() throws Exception {
    String src = "/testEditLog";
    String src1 = "/testEditLog1";
    NameNode nn = cluster.getNameNode();
    String clientName = ((DistributedFileSystem) fs).getClient().clientName;
    fs.create(new Path(src));
    for (int i = 0; i < 10; i++) {
      Block b = nn.addBlock(src, clientName).getBlock();
      nn.abandonBlock(b, src, clientName);
    }
    fs.create(new Path(src1));
    nn.addBlock(src1, clientName);
    cluster.restartNameNode(0, new String[] {}, false);
    nn = cluster.getNameNode();
    assertTrue(nn.isInSafeMode());
    nn.setSafeMode(SafeModeAction.SAFEMODE_LEAVE);
  }
}
