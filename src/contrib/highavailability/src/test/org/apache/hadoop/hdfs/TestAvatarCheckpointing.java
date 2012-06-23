package org.apache.hadoop.hdfs;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.server.namenode.AvatarNode;
import org.apache.hadoop.hdfs.server.namenode.CheckpointSignature;
import org.apache.hadoop.hdfs.server.namenode.Standby.IngestFile;
import org.apache.hadoop.hdfs.util.InjectionEvent;
import org.apache.hadoop.hdfs.util.InjectionHandler;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.junit.After;
import org.junit.AfterClass;
import static org.junit.Assert.*;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestAvatarCheckpointing {
  
  final static Log LOG = LogFactory.getLog(TestAvatarCheckpointing.class);
  
  private MiniAvatarCluster cluster;
  private Configuration conf;
  private FileSystem fs;
  private Random random = new Random();

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    MiniAvatarCluster.createAndStartZooKeeper();
  }

  private void setUp() throws Exception {
    setUp(3600);
  }
  
  private void setUp(long ckptPeriod) throws Exception {
    conf = new Configuration();
    
    conf.setBoolean("fs.ha.retrywrites", true);
    conf.setBoolean("fs.checkpoint.enabled", true);
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
  
  //////////////////////////////

  @Test
  public void testFailSuccFailQuiesce() throws Exception {
    LOG.info("TEST: ----> testFailCheckpointOnceAndSucceed");
    // fail once
    TestAvatarCheckpointingHandler h = new TestAvatarCheckpointingHandler(null, true);
    InjectionHandler.set(h);
    setUp();
    createEdits(20);
    AvatarNode primary = cluster.getPrimaryAvatar(0).avatar;
    AvatarNode standby = cluster.getStandbyAvatar(0).avatar;
    
    
    try {
      Thread.sleep(3000);
      h.failNextCheckpoint = true;
      standby.doCheckpoint();
      fail("Should get IOException here");
    } catch (Exception e) {  }
    
    // checkpoint failed
    assertNotNull(h.lastSignature);
    
    h.failNextCheckpoint = false;
    standby.doCheckpoint();
    // checkpoint succeeded
    assertNull(h.lastSignature);
    
    h.failNextCheckpoint = true;
    try {
      standby.doCheckpoint();
      fail("Should get IOException here");
    } catch (Exception e) {  }
    // checkpoint failed -> now reading edits.new
    
    
    assertNotNull(h.lastSignature);  
    createEdits(20);
    standby.quiesceStandby(getCurrentTxId(primary)-1);
    assertEquals(40, getCurrentTxId(primary));
    assertEquals(getCurrentTxId(primary), getCurrentTxId(standby));
  }
  
  @Test
  public void testFailCheckpointMultiAndCrash() throws Exception {
    LOG.info("TEST: ----> testFailCheckpointMultiAndCrash");
    TestAvatarCheckpointingHandler h = new TestAvatarCheckpointingHandler(null, true);
    InjectionHandler.set(h);
    setUp();
    createEdits(20);
    AvatarNode standby = cluster.getStandbyAvatar(0).avatar;
    
    try {
      Thread.sleep(3000);
      h.failNextCheckpoint = true;
      standby.doCheckpoint();
      fail("Should get IOException here");
    } catch (IOException e) {  }
    // checkpoint did not succeed
    assertNotNull(h.lastSignature);
    
    try {
      standby.doCheckpoint();
      fail("Should get IOException here");
    } catch (IOException e) {  }
    // checkpoint did not succeed
    assertNotNull(h.lastSignature);
    
    try {
      h.alterSignature = true;
      standby.doCheckpoint();
      fail("Checkpoint should not succeed and throw RuntimeException");
    } catch (Exception e) {
      LOG.info("Expected exception : " + e.toString());
    }
  }
  
  @Test
  public void testFailCheckpointOnceAndRestartStandby() throws Exception {
    LOG.info("TEST: ----> testFailCheckpointOnceAndRestartStandby");
    TestAvatarCheckpointingHandler h = new TestAvatarCheckpointingHandler(null, true);
    InjectionHandler.set(h);
    setUp();
    createEdits(20);
    AvatarNode primary = cluster.getPrimaryAvatar(0).avatar;
    AvatarNode standby = cluster.getStandbyAvatar(0).avatar;
    
    try {
      Thread.sleep(3000);
      h.failNextCheckpoint = true;
      standby.doCheckpoint();
      fail("Should get IOException here");
    } catch (IOException e) {  }
    // checkpoint failed
    assertNotNull(h.lastSignature);
    
    cluster.killStandby(0);
    cluster.restartStandby(0);
    Thread.sleep(2000);
    
    standby = cluster.getStandbyAvatar(0).avatar;
    h.failNextCheckpoint = false;
     
    standby.doCheckpoint();
    // checkpoint succeeded
    assertNull(h.lastSignature);
    
    createEdits(20);
    standby.quiesceStandby(getCurrentTxId(primary)-1);
    assertEquals(40, getCurrentTxId(primary));
    assertEquals(getCurrentTxId(primary), getCurrentTxId(standby));
  }
  
  @Test
  public void testIngestStartFailureAfterSaveNamespace() throws Exception {
    LOG.info("TEST: ----> testIngestStartFailureAfterSaveNamespace");
    TestAvatarCheckpointingHandler h = new TestAvatarCheckpointingHandler(null, false);
    h.simulateEditsNotExists = true;
    
    InjectionHandler.set(h);
    setUp();
    createEdits(20);
    AvatarNode primary = cluster.getPrimaryAvatar(0).avatar;
    AvatarNode standby = cluster.getStandbyAvatar(0).avatar;
    
    Thread.sleep(3000);
    standby.doCheckpoint();
    assertTrue(h.ingestRecreatedAfterFailure);
    h.simulateEditsNotExists = false;
    
    createEdits(20);
    standby.quiesceStandby(getCurrentTxId(primary)-1);
    assertEquals(40, getCurrentTxId(primary));
    assertEquals(getCurrentTxId(primary), getCurrentTxId(standby));
  }
  
  @Test
  public void testFailCheckpointOnCorruptImage() throws Exception {
    LOG.info("TEST: ----> testFailCheckpointOnCorruptImage");
    TestAvatarCheckpointingHandler h = new TestAvatarCheckpointingHandler(
        null, false);
    h.corruptImage = true;
    InjectionHandler.set(h);
    setUp();
    createEdits(20);
    AvatarNode standby = cluster.getStandbyAvatar(0).avatar;
    
    try {
      standby.doCheckpoint();
      fail("Should get IOException here");
    } catch (IOException e) {  }
  }
  
  private void testQuiesceInterruption(InjectionEvent event, boolean scf) throws Exception {
    LOG.info("TEST Quiesce during checkpoint : " + event);
    TestAvatarCheckpointingHandler h = new TestAvatarCheckpointingHandler(event, scf);
    InjectionHandler.set(h);
    setUp(3); //simulate interruption, no ckpt failure   
    AvatarNode primary = cluster.getPrimaryAvatar(0).avatar;
    AvatarNode standby = cluster.getStandbyAvatar(0).avatar;
    
    createEdits(40);
    try {
      Thread.sleep(6000);
    } catch (Exception e) { }

    standby.quiesceStandby(getCurrentTxId(primary)-1);
    assertEquals(40, getCurrentTxId(primary));
    assertEquals(getCurrentTxId(primary), getCurrentTxId(standby));
    tearDown();
  }
  
  /**
   * Invoke standby quiesce at various points of standby state.
   */
  @Test
  public void testQuiescingWhenDoingCheckpoint1() throws Exception{
    testQuiesceInterruption(InjectionEvent.STANDBY_INSTANTIATE_INGEST, false);
  }
  @Test
  public void testQuiescingWhenDoingCheckpoint2() throws Exception{
    testQuiesceInterruption(InjectionEvent.STANDBY_QUIESCE_INGEST, false);
  }
  @Test
  public void testQuiescingWhenDoingCheckpoint3() throws Exception{
    testQuiesceInterruption(InjectionEvent.STANDBY_ENTER_CHECKPOINT, false);
  }
  @Test
  public void testQuiescingWhenDoingCheckpoint4() throws Exception{
    testQuiesceInterruption(InjectionEvent.STANDBY_BEFORE_ROLL_EDIT, false);
  }
  @Test
  public void testQuiescingWhenDoingCheckpoint5() throws Exception{
    testQuiesceInterruption(InjectionEvent.STANDBY_BEFORE_SAVE_NAMESPACE, false);
  }
  @Test
  public void testQuiescingWhenDoingCheckpoint6() throws Exception{
    testQuiesceInterruption(InjectionEvent.STANDBY_BEFORE_PUT_IMAGE, false);
  }
  @Test
  public void testQuiescingWhenDoingCheckpoint7() throws Exception{
    testQuiesceInterruption(InjectionEvent.STANDBY_BEFORE_ROLL_IMAGE, false);
  }
  @Test
  public void testQuiescingBeforeCheckpoint() throws Exception{
    testQuiesceInterruption(InjectionEvent.STANDBY_BEGIN_RUN, false);
  }
  
  
  
  class TestAvatarCheckpointingHandler extends InjectionHandler {
    // specifies where the thread should wait for interruption
    private InjectionEvent synchronizationPoint;
    private volatile boolean quiesceStarted = false;

    private boolean simulateCheckpointFailure = false;
    private boolean failNextCheckpoint = false;

    public boolean alterSignature = false;
    CheckpointSignature lastSignature = null;
    public boolean corruptImage = false;
    
    // for simulateing that edits.new does not exist
    // and that the ingests gets recreated after upload
    public boolean simulateEditsNotExists = false;
    public boolean simulateEditsNotExistsDone = false;
    public boolean ingestRecreatedAfterFailure = false;

    public TestAvatarCheckpointingHandler(InjectionEvent se, boolean scf) {
      synchronizationPoint = se;
      simulateCheckpointFailure = scf;
    }
    
    @Override
    protected boolean _falseCondition(InjectionEvent event, Object... args) {
      if (simulateEditsNotExists 
          && event == InjectionEvent.STANDBY_EDITS_NOT_EXISTS
          && ((IngestFile)args[0]) == IngestFile.EDITS_NEW) {
        LOG.info("Simulate that edits.new does not exist");
        simulateEditsNotExistsDone = true;
        return true;
      }
      return false;
    }
    
    @Override 
    protected void _processEvent(InjectionEvent event, Object... args) {
      LOG.debug("processEvent: processing event: " + event);
      
      if (synchronizationPoint == event) {
        LOG.info("WAITING ON: " + event);
        while (!quiesceStarted);
      }
      if (simulateEditsNotExistsDone && 
          event == InjectionEvent.STANDBY_CREATE_INGEST_RUNLOOP) {
        ingestRecreatedAfterFailure = true;
      }
      if (event == InjectionEvent.STANDBY_INTERRUPT) {
        quiesceStarted = true;
      } 
      if (event == InjectionEvent.STANDBY_ENTER_CHECKPOINT
          && alterSignature) {
        CheckpointSignature c = (CheckpointSignature)args[0];
        if (c!=null)
          c.cTime++;
      }
      if (event == InjectionEvent.STANDBY_EXIT_CHECKPOINT) {
        lastSignature = (CheckpointSignature)args[0];
      }
      if (event == InjectionEvent.STANDBY_BEFORE_PUT_IMAGE && corruptImage) {
        File imageFile = (File)args[0];
        LOG.info("Corrupting image file " + imageFile);
        FileOutputStream out = null;
        try {
          out = new FileOutputStream(imageFile);
          out.getChannel().truncate(4);
        } catch (IOException ioe) {
          throw new RuntimeException(ioe);
        } finally {
          IOUtils.closeStream(out);
        }
      }
    }

    @Override
    protected void _processEventIO(InjectionEvent event, Object... args)
        throws IOException {
      LOG.debug("processEventIO: processing event: " + event);

      if(simulateCheckpointFailure) {
        if (event == InjectionEvent.STANDBY_BEFORE_ROLL_IMAGE
            && failNextCheckpoint) {
          throw new IOException("Simultaing checkpoint failure");
        } else if (event == InjectionEvent.STANDBY_BEGIN_RUN) {
          // delay the main loop forever
          while(!quiesceStarted);
          throw new IOException("Simulating checkpoint failure");
        } 
      } 
      _processEvent(event, args);
    }
  }
}
