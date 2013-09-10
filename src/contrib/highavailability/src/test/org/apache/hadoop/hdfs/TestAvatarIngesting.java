package org.apache.hadoop.hdfs;

import java.io.IOException;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.impl.Log4JLogger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.qjournal.client.QuorumJournalManager;
import org.apache.hadoop.hdfs.qjournal.server.Journal;
import org.apache.hadoop.hdfs.server.namenode.AvatarNode;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.namenode.FSImageTestUtil.CheckpointTrigger;
import org.apache.hadoop.hdfs.util.InjectionEvent;
import org.apache.hadoop.util.InjectionEventI;
import org.apache.hadoop.util.InjectionHandler;
import org.apache.hadoop.fs.ChecksumException;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Level;

import org.junit.After;
import org.junit.AfterClass;
import static org.junit.Assert.*;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestAvatarIngesting {

  final static Log LOG = LogFactory.getLog(TestAvatarIngesting.class);

  protected MiniAvatarCluster cluster;
  private Configuration conf;
  private FileSystem fs;
  private Random random = new Random();

  {
  	((Log4JLogger)QuorumJournalManager.LOG).getLogger().setLevel(Level.ALL);
  	((Log4JLogger)Journal.LOG).getLogger().setLevel(Level.ALL);
  	((Log4JLogger)FSNamesystem.LOG).getLogger().setLevel(Level.ALL);
  }

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    MiniAvatarCluster.createAndStartZooKeeper();
  }
  

  void setUp(long ckptPeriod, String name, boolean ckptEnabled,
      boolean enableQJM) throws Exception {
    LOG.info("------------------- test: " + name + " START ----------------");
    conf = new Configuration();
    
    conf.setBoolean("fs.ha.retrywrites", true);
    conf.setBoolean("fs.checkpoint.enabled", ckptEnabled);
    conf.setLong("fs.checkpoint.period", 3600);

    cluster = new MiniAvatarCluster.Builder(conf).numDataNodes(2).enableQJM(enableQJM).build();
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

  protected void testIngestFailure(InjectionEvent event, boolean enableQJM)
      throws Exception {
    LOG.info("TEST Ingest Failure : " + event);
    TestAvatarIngestingHandler h = new TestAvatarIngestingHandler(event);
    InjectionHandler.set(h);
    setUp(3, "testIngestFailure: " + event, false, enableQJM); 
          // simulate interruption, no ckpt failure
    AvatarNode primary = cluster.getPrimaryAvatar(0).avatar;
    AvatarNode standby = cluster.getStandbyAvatar(0).avatar;
    h.setDisabled(false);

    createEdits(20);
    h.setDisabled(true);
    
    if (!enableQJM) {
	    standby.quiesceStandby(getCurrentTxId(primary) - 1);
	    // SLS + 20 edits
	    assertEquals(21, getCurrentTxId(primary));
	    assertEquals(getCurrentTxId(primary), getCurrentTxId(standby));
    }
    tearDown();
  }
  
  @Test
  public void testCheckpointWithRestarts() throws Exception {
    TestAvatarIngestingHandler h = new TestAvatarIngestingHandler(
        InjectionEvent.FSEDIT_LOG_WRITE_END_LOG_SEGMENT);
    InjectionHandler.set(h);
    setUp(3, "testCheckpointWithRestarts", true, false);
    // simulate interruption, no ckpt failure
    AvatarNode primary = cluster.getPrimaryAvatar(0).avatar;
    AvatarNode standby = cluster.getStandbyAvatar(0).avatar;
    createEdits(20);
    h.doCheckpoint();
    // 2 for initial checkpoint + 2 for this checkpoint + SLS + 20 edits
    h.disabled = false;
    assertEquals(25, getCurrentTxId(primary));

    cluster.killStandby();
    cluster.killPrimary();
    h.disabled = false;
    cluster.restartAvatarNodes();

    h.doCheckpoint();

    assertEquals(getCurrentTxId(primary), getCurrentTxId(standby));
  }
  
  @Test
  public void testPrimaryRestart() throws Exception {
    // checks whether standby can continue ingesting with finalized segments
    // with no ending transaction
    TestAvatarIngestingHandler h = new TestAvatarIngestingHandler(null);
    InjectionHandler.set(h);
    h.gracefulShutdown = false;
    
    setUp(0, "testPrimaryRestart", false, false);
    // simulate interruption, no ckpt failure
    cluster.killStandby();   
    cluster.killPrimary();
    
    // this will produce finalized segment with no ending marker
    // and a new segment with txid = 1
    cluster.restartAvatarNodes();
    
    // current txid is the one to be written 1 + 1 = 2
    assertEquals(2, getCurrentTxId(cluster.getStandbyAvatar(0).avatar));
  }

  /*
   * Simulate exception when reading from edits
   */
  @Test
  public void testIngestFailureReading() throws Exception {
    testIngestFailure(InjectionEvent.INGEST_READ_OP, false);
  }
  
  /*
   * Simulate edit log transaction mismatch
   * Ingest should be resurrected and consume edits normally
   */
  @Test
  public void testIngestFailureTxidMismatch() throws Exception {
    testIngestFailure(InjectionEvent.INGEST_TXID_CHECK, false);
  }
  
  @Test
  public void testIngestFailureSetupStream() throws Exception {
    InjectionEvent event = InjectionEvent.STANDBY_JOURNAL_GETSTREAM;
    TestAvatarIngestingHandler h = new TestAvatarIngestingHandler(event);
    InjectionHandler.set(h);
    h.setDisabled(false);

    setUp(3, "testIngestFailure: " + event, false, false);
    h.setDisabled(true);

    assertEquals(1, h.exceptions);
  }
  
  @Test
  public void testRecoverState() throws Exception {
    TestAvatarIngestingHandler h = new TestAvatarIngestingHandler(
        InjectionEvent.STANDBY_RECOVER_STATE);
    InjectionHandler.set(h);
    setUp(3, "testRecoverState", true, false);
    // simulate interruption, no ckpt failure
    AvatarNode primary = cluster.getPrimaryAvatar(0).avatar;
    AvatarNode standby = cluster.getStandbyAvatar(0).avatar;
    h.setDisabled(true);

    createEdits(20);
    h.doCheckpoint();
    // 2 for initial checkpoint + 2 for this checkpoint + SLS + 20 edits
    assertEquals(25, getCurrentTxId(primary));
    createEdits(20);
    h.setDisabled(false);

    // sleep to see if the state recovers correctly
    DFSTestUtil.waitNSecond(5);
    h.doCheckpoint();
    standby.quiesceStandby(getCurrentTxId(primary) - 1);

    assertEquals(47, getCurrentTxId(primary));
    assertEquals(getCurrentTxId(primary), getCurrentTxId(standby));
  }
    
  class TestAvatarIngestingHandler extends InjectionHandler {
    boolean gracefulShutdown = true;
    int exceptions = 0;
    private InjectionEvent synchronizationPoint;
    int simulatedFailure = 0;
    boolean disabled = true;
    boolean doneRecovery = false;

    private CheckpointTrigger ckptTrigger = new CheckpointTrigger();

    public TestAvatarIngestingHandler(InjectionEvent se) {
      synchronizationPoint = se;
    }
    
    public void setDisabled(boolean v){
      disabled = v;
    }

    @Override
    protected void _processEventIO(InjectionEventI event, Object... args)
        throws IOException {
      if (synchronizationPoint == event) {
        if(disabled)
          return;
        LOG.info("PROCESSING EVENT: " + event + " counter: " + simulatedFailure);
        simulatedFailure++;
        if (event == InjectionEvent.INGEST_READ_OP
            && synchronizationPoint == event && !disabled) {
          if((simulatedFailure % 3) == 1){
            LOG.info("Throwing checksum exception");   
            throw new ChecksumException("Testing checksum exception...", 0);
          }   
          if ((simulatedFailure % 5) == 1) {
            LOG.info("Throwing unchecked exception");
            throw new ArrayIndexOutOfBoundsException(
                "Testing uncecked exception...");
          }
          if((simulatedFailure % 7) == 1){
            LOG.info("Throwing IO exception");
            throw new IOException("Testing IO exception...");
          }
        }
        if (event == InjectionEvent.STANDBY_JOURNAL_GETSTREAM
            && synchronizationPoint == event && !disabled) {
          if (simulatedFailure < 2) {
            exceptions++;
            LOG.info("Throwing exception when setting up the stream");
            throw new IOException(
                "Testing I/O exception when getting the stream");
          }
        }
      }        
    }
       
    @Override
    protected void _processEvent(InjectionEventI event, Object... args) {
      ckptTrigger.checkpointDone(event, args);
    }
    
    @Override
    protected boolean _falseCondition(InjectionEventI event, Object... args) {
      if (synchronizationPoint == InjectionEvent.INGEST_TXID_CHECK && !disabled
          && event == InjectionEvent.INGEST_TXID_CHECK) {
        return true;
      }
      if (synchronizationPoint == InjectionEvent.STANDBY_RECOVER_STATE
          && !disabled && !doneRecovery
          && event == InjectionEvent.STANDBY_RECOVER_STATE) {
        doneRecovery = true;
        return true;
      }
      return ckptTrigger.triggerCheckpoint(event);
    }
    
    @Override
    protected boolean _trueCondition(InjectionEventI event, Object... args) {
      if (synchronizationPoint == InjectionEvent.FSEDIT_LOG_WRITE_END_LOG_SEGMENT
          && event == InjectionEvent.FSEDIT_LOG_WRITE_END_LOG_SEGMENT
          && !disabled) {
        return false;
      }
      if (event == InjectionEvent.FSEDIT_LOG_WRITE_END_LOG_SEGMENT) {
        return gracefulShutdown;
      }
      return true;
    }
    
    void doCheckpoint() throws IOException {
      ckptTrigger.doCheckpoint();
    }
  }
}
