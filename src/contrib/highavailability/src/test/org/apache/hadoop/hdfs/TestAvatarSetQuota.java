package org.apache.hadoop.hdfs;

import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestAvatarSetQuota {

  private static Configuration conf;
  private static MiniAvatarCluster cluster;
  private static FileSystem fs;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    MiniAvatarCluster.createAndStartZooKeeper();
    conf = new Configuration();
    conf.setInt("dfs.block.size", 1024);
    cluster = new MiniAvatarCluster(conf, 3, true, null, null);
    fs = cluster.getFileSystem();
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    cluster.shutDown();
    MiniAvatarCluster.shutDownZooKeeper();
  }

  @Test
  public void testAvatarSetQuota() throws Exception {
    String test = "/testAvatarSetQuota";
    DFSTestUtil util = new DFSTestUtil(test, 10, 10, 1024);
    util.createFiles(fs, test);

    FSDataOutputStream out = fs.create(new Path(test + "/abc"));
    byte[] buffer = new byte[10 * 1024];
    Random r = new Random();
    r.nextBytes(buffer);
    out.write(buffer);
    out.sync();
    ((DistributedFileSystem) fs).setQuota(new Path(test), 5, -1);
    out.close();
    cluster.getStandbyAvatar(0).avatar.quiesceStandby(-1);
  }

}
