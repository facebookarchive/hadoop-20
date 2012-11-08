package org.apache.hadoop.hdfs.server.namenode;

import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniAvatarCluster;
import org.apache.hadoop.hdfs.protocol.AvatarConstants.Avatar;
import org.apache.hadoop.hdfs.server.common.HdfsConstants;

import org.junit.AfterClass;
import static org.junit.Assert.*;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestAvatarLease {

  private static Random random = new Random();
  private static MiniAvatarCluster cluster;
  private static FileSystem fs;
  private static Configuration conf;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    MiniAvatarCluster.createAndStartZooKeeper();
    conf = new Configuration();
    cluster = new MiniAvatarCluster(conf, 3, true, null, null);
    fs = cluster.getFileSystem();
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    cluster.shutDown();
    MiniAvatarCluster.shutDownZooKeeper();
  }

  @Test
  public void testLeaseAfterFailover() throws Exception {
    String fileName = "/testLeaseAfterFailover";
    FSDataOutputStream out = fs.create(new Path(fileName));
    byte[] buffer = new byte[1024];
    random.nextBytes(buffer);
    out.write(buffer);
    out.sync();

    FSNamesystem primary = cluster.getPrimaryAvatar(0).avatar.namesystem;

    // Prevents lease recovery to work.
    cluster.shutDownDataNodes();

    // Expire the lease.
    primary.leaseManager.setLeasePeriod(0, 0);
    primary.leaseManager.checkLeases();
    cluster.killPrimary();
    cluster.restartDataNodes(false);

    AvatarNode standbyAvatar = cluster.getStandbyAvatar(0).avatar;
    cluster.failOver();

    String lease = standbyAvatar.namesystem.leaseManager
        .getLeaseByPath(fileName).getHolder();
    assertEquals(HdfsConstants.NN_RECOVERY_LEASEHOLDER, lease);
  }
}
