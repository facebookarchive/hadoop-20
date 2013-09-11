package org.apache.hadoop.hdfs;

import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.tools.FastCopy;

import org.junit.AfterClass;
import static org.junit.Assert.*;

import org.junit.BeforeClass;
import org.junit.Test;

public class TestFastCopyBadDatanode {

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
    conf.setInt("dfs.fastcopy.max.datanode.errors", 3);
    cluster = new MiniDFSCluster(conf, 3, true, null, NUM_NAME_NODES);
    
    for (int i = 0; i < NUM_NAME_NODES; i++) {
      fileSystems[i] = cluster.getFileSystem(i);
    }
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    cluster.shutdown();
  }

  @Test
  public void testBadDatanodeSameFS() throws Exception {
    DFSTestUtil util = new DFSTestUtil("testBadDatanodeSameFS", 3, 1, MAX_FILE_SIZE);
    String topDir = "/testBadDatanodeSameFS";
    FileSystem fs = fileSystems[0];
    util.createFiles(fs, topDir);
    FastCopy fastCopy = new FastCopy(conf);
    cluster.shutdownDataNode(0, true);
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
    } finally {
      fastCopy.shutdown();
    }
  }
  
  @Test
  public void testBadDatanodeCrossFS() throws Exception {
    DFSTestUtil util = new DFSTestUtil("testBadDatanodeCrossFS", 3, 1, MAX_FILE_SIZE);
    String topDir = "/testBadDatanodeCrossFS";
    FileSystem fs1 = fileSystems[0];
    FileSystem fs2 = fileSystems[1];
    util.createFiles(fs1, topDir);
    FastCopy fastCopy = new FastCopy(conf);
    cluster.shutdownDataNode(0, true);
    try {
      for (String fileName : util.getFileNames(topDir)) {
        fastCopy.copy(fileName, fileName + "dst", (DistributedFileSystem) fs1, 
            (DistributedFileSystem) fs2);
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
