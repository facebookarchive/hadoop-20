package org.apache.hadoop.hdfs;

import java.io.IOException;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.server.namenode.AvatarNode;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.junit.After;
import org.junit.AfterClass;
import static org.junit.Assert.*;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestAvatarTxIds {
  private MiniAvatarCluster cluster;
  private Configuration conf;
  private FileSystem fs;
  private Random random = new Random();

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    MiniAvatarCluster.createAndStartZooKeeper();
  }

  @Before
  public void setUp() throws Exception {
    conf = new Configuration();
    conf.setBoolean("fs.ha.retrywrites", true);
    conf.setBoolean("fs.checkpoint.enabled", true);
    cluster = new MiniAvatarCluster(conf, 3, true, null, null);
    fs = cluster.getFileSystem();
  }

  @After
  public void tearDown() throws Exception {
    fs.close();
    cluster.shutDown();
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    MiniAvatarCluster.shutDownZooKeeper();
  }

  public void createEdits(int nEdits) throws IOException {
    for (int i = 0; i < nEdits / 2; i++) {
      // Create file ends up logging two edits to the edit log, one for create
      // file and one for bumping up the generation stamp
      fs.create(new Path("/" + random.nextInt()));
    }
  }

  public long getCurrentTxId(AvatarNode avatar) {
    return avatar.getFSImage().getEditLog().getCurrentTxId();
  }

  @Test
  public void testBasic() throws Exception {
    createEdits(20);
    AvatarNode primary = cluster.getPrimaryAvatar(0).avatar;
    AvatarNode standby = cluster.getStandbyAvatar(0).avatar;
    standby.quiesceStandby(getCurrentTxId(primary)-1);
    assertEquals(20, getCurrentTxId(primary));
    assertEquals(getCurrentTxId(primary), getCurrentTxId(standby));
  }

  @Test
  public void testWithFailover() throws Exception {
    // Create edits before failover.
    createEdits(20);
    AvatarNode primary = cluster.getPrimaryAvatar(0).avatar;
    AvatarNode standby = cluster.getStandbyAvatar(0).avatar;
    assertEquals(20, getCurrentTxId(primary));

    // Perform failover and restart old primary.
    cluster.failOver();
    cluster.restartStandby();

    // Get new instances after failover.
    primary = cluster.getPrimaryAvatar(0).avatar;
    standby = cluster.getStandbyAvatar(0).avatar;
    assertEquals(20, getCurrentTxId(primary));

    // Create some more edits and verify.
    createEdits(20);
    standby.quiesceStandby(getCurrentTxId(primary)-1);
    assertEquals(40, getCurrentTxId(primary));
    assertEquals(getCurrentTxId(primary), getCurrentTxId(standby));
  }

  @Test
  public void testWithFailoverTxIdMismatchHard() throws Exception {
    // Create edits before failover.
    createEdits(20);
    AvatarNode primary = cluster.getPrimaryAvatar(0).avatar;
    AvatarNode standby = cluster.getStandbyAvatar(0).avatar;
    assertEquals(20, getCurrentTxId(primary));

    standby.getFSImage().getEditLog().setStartTransactionId(50);
    assertEquals(50, getCurrentTxId(standby));

    // Perform failover and verify it fails.
    try {
      cluster.failOver();
    } catch (IOException e) {
      System.out.println("Expected exception : " + e);
      return;
    }
    fail("Did not throw exception");
  }

  @Test
  public void testDoubleFailover() throws Exception {
    // Create edits before failover.
    createEdits(20);

    // Perform failover.
    cluster.failOver();
    cluster.restartStandby();
    createEdits(20);

    // Perform second failover.
    cluster.failOver();
    cluster.restartStandby();
    createEdits(20);

    AvatarNode primary = cluster.getPrimaryAvatar(0).avatar;
    AvatarNode standby = cluster.getStandbyAvatar(0).avatar;
    standby.quiesceStandby(getCurrentTxId(primary)-1);
    assertEquals(60, getCurrentTxId(primary));
    assertEquals(getCurrentTxId(primary), getCurrentTxId(standby));
  }

  @Test
  public void testWithStandbyDead() throws Exception {
    createEdits(20);
    cluster.killStandby();
    createEdits(20);
    cluster.restartStandby();
    createEdits(20);
    AvatarNode primary = cluster.getPrimaryAvatar(0).avatar;
    AvatarNode standby = cluster.getStandbyAvatar(0).avatar;
    standby.quiesceStandby(getCurrentTxId(primary)-1);
    assertEquals(60, getCurrentTxId(primary));
    assertEquals(getCurrentTxId(primary), getCurrentTxId(standby));
  }

  @Test
  public void testWithStandbyDeadAfterFailover() throws Exception {
    createEdits(20);
    cluster.failOver();
    createEdits(20);
    cluster.killStandby();
    createEdits(20);
    cluster.restartStandby();
    createEdits(20);

    AvatarNode primary = cluster.getPrimaryAvatar(0).avatar;
    AvatarNode standby = cluster.getStandbyAvatar(0).avatar;
    standby.quiesceStandby(getCurrentTxId(primary)-1);
    assertEquals(80, getCurrentTxId(primary));
    assertEquals(getCurrentTxId(primary), getCurrentTxId(standby));
  }

  @Test
  public void testWithCheckPoints() throws Exception {
    createEdits(20);
    AvatarNode primary = cluster.getPrimaryAvatar(0).avatar;
    AvatarNode standby = cluster.getStandbyAvatar(0).avatar;
    standby.doCheckpoint();
    createEdits(20);
    standby.quiesceStandby(getCurrentTxId(primary)-1);
    assertEquals(40, getCurrentTxId(primary));
    assertEquals(getCurrentTxId(primary), getCurrentTxId(standby));
  }

  @Test
  public void testAcrossRestarts() throws Exception {
    createEdits(20);
    cluster.restartAvatarNodes();
    AvatarNode primary = cluster.getPrimaryAvatar(0).avatar;
    AvatarNode standby = cluster.getStandbyAvatar(0).avatar;
    assertEquals(20, getCurrentTxId(primary));
    assertEquals(getCurrentTxId(primary), getCurrentTxId(standby));
    createEdits(20);
    standby.quiesceStandby(getCurrentTxId(primary)-1);
    assertEquals(40, getCurrentTxId(primary));
    assertEquals(getCurrentTxId(primary), getCurrentTxId(standby));
  }

  @Test
  public void testCheckpointAndRestart() throws Exception {
    createEdits(20);
    AvatarNode primary = cluster.getPrimaryAvatar(0).avatar;
    AvatarNode standby = cluster.getStandbyAvatar(0).avatar;
    standby.doCheckpoint();
    createEdits(20);
    standby.quiesceStandby(getCurrentTxId(primary)-1);
    assertEquals(40, getCurrentTxId(primary));
    assertEquals(getCurrentTxId(primary), getCurrentTxId(standby));

    cluster.restartAvatarNodes();
    primary = cluster.getPrimaryAvatar(0).avatar;
    standby = cluster.getStandbyAvatar(0).avatar;
    createEdits(20);
    standby.quiesceStandby(getCurrentTxId(primary)-1);
    assertEquals(60, getCurrentTxId(primary));
    assertEquals(getCurrentTxId(primary), getCurrentTxId(standby));
  }
}
