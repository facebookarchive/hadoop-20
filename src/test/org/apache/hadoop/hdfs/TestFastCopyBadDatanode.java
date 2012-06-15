package org.apache.hadoop.hdfs;

import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.tools.FastCopy;

import org.junit.AfterClass;
import static org.junit.Assert.*;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestFastCopyBadDatanode {

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
    conf.setInt("dfs.fastcopy.max.datanode.errors", 3);
    cluster = new MiniDFSCluster(conf, 3, true, null);
    fs = cluster.getFileSystem();
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    cluster.shutdown();
  }

  @Test
  public void testBadDatanode() throws Exception {
    DFSTestUtil util = new DFSTestUtil("testBadDatanode", 3, 1, MAX_FILE_SIZE);
    String topDir = "/testBadDatanode";
    util.createFiles(fs, topDir);
    FastCopy fastCopy = new FastCopy(conf);
    cluster.shutdownDataNode(0, true);
    try {
      for (String fileName : util.getFileNames(topDir)) {
        fastCopy.copy(fileName, fileName + "dst", (DistributedFileSystem) fs,
            (DistributedFileSystem) fs);
      }
      Map<DatanodeInfo, Integer> dnErrors = fastCopy.getDatanodeErrors();
      assertEquals(1, dnErrors.size());
      int errors = dnErrors.values().iterator().next();
      assertTrue(errors >= conf.getInt("dfs.fastcopy.max.datanode.errors", 3) + 1);
    } finally {
      fastCopy.shutdown();
    }
  }
}
