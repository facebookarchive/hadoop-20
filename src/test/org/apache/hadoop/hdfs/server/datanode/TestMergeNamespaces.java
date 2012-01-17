package org.apache.hadoop.hdfs.server.datanode;

import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class TestMergeNamespaces {
  static final Log LOG = LogFactory.getLog(TestMergeNamespaces.class);
  static Configuration conf;
  
  @Before
  public void setUp() throws Exception {
    conf = new Configuration();
  }
  
  public static void createFile(FileSystem fs, Path p, long len) throws Exception {
    DFSTestUtil.createFile(fs, p, len, (short)1, 1L);
    verifyFile(fs, p, len);
  }
  
  public static void verifyFile(FileSystem fs, Path p, long len) throws Exception {
    FileStatus fstat = fs.getFileStatus(p);
    assertEquals(fstat.getLen(), len);
  }
  
  @Test
  public void testMergeNamespaces() throws Exception {
    MiniDFSCluster cluster1 = null;
    MiniDFSCluster cluster2 = null;
    Configuration conf1 = new Configuration(conf);
    conf1.set(MiniDFSCluster.DFS_CLUSTER_ID, Long.toString(System.currentTimeMillis() + 1));
    Configuration conf2 = new Configuration(conf);
    conf2.set(MiniDFSCluster.DFS_CLUSTER_ID, Long.toString(System.currentTimeMillis() + 2));
    
    try {
      LOG.info("Start cluster1 and cluster2");
      cluster1 = new MiniDFSCluster(0, conf1, 1, true, null, 1);
      cluster2 = new MiniDFSCluster(0, conf2, 1, true, null, 2);
      LOG.info("Write data to cluster2 and cluster1");
      Path p = new Path("/testFile");
      createFile(cluster1.getFileSystem(0), p, 512 * 1024L);
      createFile(cluster2.getFileSystem(0), p, 1024 * 1024L);
      createFile(cluster2.getFileSystem(1), p, 1536 * 1024L);
      LOG.info("Add cluster2 to cluster1");
      cluster1.addCluster(cluster2, false);
      verifyFile(cluster1.getFileSystem(0), p, 512 * 1024L);
      verifyFile(cluster1.getFileSystem(1), p, 1024 * 1024L);
      verifyFile(cluster1.getFileSystem(2), p, 1536 * 1024L);
      
      Path p1 = new Path("/testFile1");
      createFile(cluster1.getFileSystem(0), p1, 1536 * 1024L);
      createFile(cluster1.getFileSystem(1), p1, 512 * 1024L);
      createFile(cluster1.getFileSystem(2), p1, 1024 * 1024L);
      
      cluster1.restartDataNodes();
      verifyFile(cluster1.getFileSystem(0), p, 512 * 1024L);
      verifyFile(cluster1.getFileSystem(1), p, 1024 * 1024L);
      verifyFile(cluster1.getFileSystem(2), p, 1536 * 1024L);
      Path p2 = new Path("/testFile2");
      createFile(cluster1.getFileSystem(0), p2, 1024 * 1024L);
      createFile(cluster1.getFileSystem(1), p2, 1536 * 1024L);
      createFile(cluster1.getFileSystem(2), p2, 512 * 1024L);
    } finally {
      cluster1.shutdown();
    }
  }
}
