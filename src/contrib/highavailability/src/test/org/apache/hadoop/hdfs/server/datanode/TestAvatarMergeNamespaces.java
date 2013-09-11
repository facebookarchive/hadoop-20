package org.apache.hadoop.hdfs.server.datanode;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.MiniAvatarCluster;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.fs.Path;

public class TestAvatarMergeNamespaces {
  static final Log LOG = LogFactory.getLog(TestAvatarMergeNamespaces.class);
  static Configuration conf;
  
  @BeforeClass
  public static void setUpClass() throws Exception {
    MiniAvatarCluster.createAndStartZooKeeper();
  }
  
  @Before
  public void setUp() throws Exception {
    conf = new Configuration();
    // populate repl queues on standby (in safe mode)
    conf.setFloat("dfs.namenode.replqueue.threshold-pct", 0f);
    conf.setLong("fs.avatar.standbyfs.initinterval", 1000);
    conf.setLong("fs.avatar.standbyfs.checkinterval", 1000);
  }
  
  @Test
  public void testMergeNamespaces() throws Exception {
    MiniAvatarCluster cluster1 = null;
    MiniAvatarCluster cluster2 = null;
    Configuration conf1 = new Configuration(conf);
    conf1.set(MiniDFSCluster.DFS_CLUSTER_ID, Long.toString(System.currentTimeMillis() + 1));
    Configuration conf2 = new Configuration(conf);
    conf2.set(MiniDFSCluster.DFS_CLUSTER_ID, Long.toString(System.currentTimeMillis() + 2));
    
    try {
      LOG.info("Start cluster1 and cluster2");
      cluster1 = new MiniAvatarCluster.Builder(conf1)
      				.federation(true).enableQJM(false).build();
      cluster2 = new MiniAvatarCluster.Builder(conf2)
      				.numNameNodes(2).federation(true).enableQJM(false).build();
      LOG.info("Write data to cluster2 and cluster1");
      Path p = new Path("/testFile");
      TestMergeNamespaces.createFile(cluster1.getFileSystem(0), p, 512 * 1024L);
      TestMergeNamespaces.createFile(cluster2.getFileSystem(0), p, 1024 * 1024L);
      TestMergeNamespaces.createFile(cluster2.getFileSystem(1), p, 1536 * 1024L);
      LOG.info("Add cluster2 to cluster1");
      cluster1.addCluster(cluster2, false);
      TestMergeNamespaces.verifyFile(cluster1.getFileSystem(0), p, 512 * 1024L);
      TestMergeNamespaces.verifyFile(cluster1.getFileSystem(1), p, 1024 * 1024L);
      TestMergeNamespaces.verifyFile(cluster1.getFileSystem(2), p, 1536 * 1024L);
      
      Path p1 = new Path("/testFile1");
      TestMergeNamespaces.createFile(cluster1.getFileSystem(0), p1, 1536 * 1024L);
      TestMergeNamespaces.createFile(cluster1.getFileSystem(1), p1, 512 * 1024L);
      TestMergeNamespaces.createFile(cluster1.getFileSystem(2), p1, 1024 * 1024L);
      cluster1.restartDataNodes();
      TestMergeNamespaces.verifyFile(cluster1.getFileSystem(0), p, 512 * 1024L);
      TestMergeNamespaces.verifyFile(cluster1.getFileSystem(1), p, 1024 * 1024L);
      TestMergeNamespaces.verifyFile(cluster1.getFileSystem(2), p, 1536 * 1024L);
      Path p2 = new Path("/testFile2");
      TestMergeNamespaces.createFile(cluster1.getFileSystem(0), p2, 1024 * 1024L);
      TestMergeNamespaces.createFile(cluster1.getFileSystem(1), p2, 1536 * 1024L);
      TestMergeNamespaces.createFile(cluster1.getFileSystem(2), p2, 512 * 1024L);
    } finally {
      if (cluster1!= null) cluster1.shutDown();
    }
  }

  @AfterClass
  public static void shutDownClass() throws Exception {
    MiniAvatarCluster.shutDownZooKeeper();
  }
}
