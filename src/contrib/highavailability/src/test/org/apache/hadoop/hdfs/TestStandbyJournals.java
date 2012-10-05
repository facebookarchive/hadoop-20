package org.apache.hadoop.hdfs;

import java.io.IOException;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.server.namenode.AvatarNode;
import org.apache.hadoop.hdfs.server.namenode.FSImageTestUtil.CheckpointTrigger;
import org.apache.hadoop.hdfs.util.InjectionEvent;
import org.apache.hadoop.hdfs.util.InjectionHandler;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.junit.After;
import org.junit.AfterClass;
import static org.junit.Assert.*;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestStandbyJournals {
  
  final static Log LOG = LogFactory.getLog(TestStandbyJournals.class);
  
  private MiniAvatarCluster cluster;
  private Configuration conf;
  private FileSystem fs;
  private Random random = new Random();

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    MiniAvatarCluster.createAndStartZooKeeper();
  }
  
  private void setUp(boolean ckptEnabled, long ckptPeriod) throws Exception {
    conf = new Configuration();
    
    conf.setBoolean("fs.ha.retrywrites", true);
    conf.setBoolean("fs.checkpoint.enabled", ckptEnabled);
    conf.setLong("fs.checkpoint.period", ckptPeriod);
    
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
  
  
  @Test
  public void testStartupJournals() throws Exception {
    LOG.info("TEST: ----> testStartupJournals");
    TestAvatarCheckpointingHandler h = new TestAvatarCheckpointingHandler();
    InjectionHandler.set(h);
    setUp(false, 0);

    AvatarNode standby = cluster.getStandbyAvatar(0).avatar;
    
    // edits.new should not exist after fresh startup
    assertFalse(standby.getFSImage().getEditLog().existsNew());
  }
  

  @Test
  public void testAfterCheckpointJournals() throws Exception {
    LOG.info("TEST: ----> testAfterCheckpointJournals");
    TestAvatarCheckpointingHandler h = new TestAvatarCheckpointingHandler();
    InjectionHandler.set(h);
    setUp(true, 0);
    createEdits(20);
    
    AvatarNode standby = cluster.getStandbyAvatar(0).avatar;
    h.doCheckpoint();
    
    // edits.new should not exist after checkpoint
    assertFalse(standby.getFSImage().getEditLog().existsNew());
  }
  
    
  class TestAvatarCheckpointingHandler extends InjectionHandler {
    private CheckpointTrigger ckptTrigger = new CheckpointTrigger();
    
    @Override
    protected boolean _falseCondition(InjectionEvent event, Object... args) {
      return ckptTrigger.triggerCheckpoint(event); 
    }
    
    @Override 
    protected void _processEvent(InjectionEvent event, Object... args) {
      LOG.debug("processEvent: processing event: " + event);    
      ckptTrigger.checkpointDone(event, args);
    }
    
    void doCheckpoint() throws Exception { 
      ckptTrigger.doCheckpoint();  
    }    
  }
}
