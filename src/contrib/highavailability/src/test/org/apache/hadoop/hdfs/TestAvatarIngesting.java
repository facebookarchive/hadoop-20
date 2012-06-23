package org.apache.hadoop.hdfs;

import java.io.IOException;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.server.namenode.AvatarNode;
import org.apache.hadoop.hdfs.server.namenode.CheckpointSignature;
import org.apache.hadoop.hdfs.util.InjectionEvent;
import org.apache.hadoop.hdfs.util.InjectionHandler;
import org.apache.hadoop.fs.ChecksumException;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.junit.After;
import org.junit.AfterClass;
import static org.junit.Assert.*;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestAvatarIngesting {

  final static Log LOG = LogFactory.getLog(TestAvatarCheckpointing.class);

  private MiniAvatarCluster cluster;
  private Configuration conf;
  private FileSystem fs;
  private Random random = new Random();

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    MiniAvatarCluster.createAndStartZooKeeper();
  }

  private void setUp(long ckptPeriod) throws Exception {
    conf = new Configuration();

    conf.setBoolean("fs.ha.retrywrites", true);
    conf.setBoolean("fs.checkpoint.enabled", false);
    conf.setLong("fs.checkpoint.period", 3600);

    cluster = new MiniAvatarCluster(conf, 2, true, null, null);
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

  // ////////////////////////////

  private void testIngestFailure(InjectionEvent event)
      throws Exception {
    LOG.info("TEST Ingest Failure : " + event);
    TestAvatarIngestingHandler h = new TestAvatarIngestingHandler(event);
    InjectionHandler.set(h);
    setUp(3); // simulate interruption, no ckpt failure
    AvatarNode primary = cluster.getPrimaryAvatar(0).avatar;
    AvatarNode standby = cluster.getStandbyAvatar(0).avatar;
    h.setDisabled(false);

    createEdits(20);
    try {
      Thread.sleep(10000);
    } catch (Exception e) {
    }
    h.setDisabled(true);
    standby.quiesceStandby(getCurrentTxId(primary) - 1);
    assertEquals(20, getCurrentTxId(primary));
    assertEquals(getCurrentTxId(primary), getCurrentTxId(standby));
    tearDown();
  }

  /*
   * Simulate exception when reading from edits
   */
  @Test
  public void testIngestFailure() throws Exception {
    testIngestFailure(InjectionEvent.INGEST_READ_OP);
  }

  class TestAvatarIngestingHandler extends InjectionHandler {
    private InjectionEvent synchronizationPoint;
    int simulatedFailure = 0;
    boolean disabled = true;

    public TestAvatarIngestingHandler(InjectionEvent se) {
      synchronizationPoint = se;
    }
    
    public void setDisabled(boolean v){
      disabled = v;
    }

    @Override
    protected void _processEventIO(InjectionEvent event, Object... args)
        throws IOException {
      if (synchronizationPoint == event) {
        if(disabled)
          return;
        LOG.info("PROCESSING EVENT: " + synchronizationPoint + " counter: " + simulatedFailure);
        simulatedFailure++;
        if(event == InjectionEvent.INGEST_READ_OP && ((simulatedFailure % 3) == 1)){
          LOG.info("Throwing checksum exception");   
          throw new ChecksumException("Testing checksum exception...", 0);
        }
        
        if(event == InjectionEvent.INGEST_READ_OP && ((simulatedFailure % 7) == 1)){
          LOG.info("Throwing IO exception");
          throw new IOException("Testing IO exception...");
        }
        
      }
    }
  }
}
