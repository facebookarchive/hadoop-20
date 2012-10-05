package org.apache.hadoop.hdfs;

import java.io.IOException;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.server.namenode.AvatarNode;
import org.apache.hadoop.hdfs.util.InjectionEvent;
import org.apache.hadoop.hdfs.util.InjectionHandler;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.junit.After;
import org.junit.AfterClass;
import static org.junit.Assert.*;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestAvatarQuiesce {

  final static Log LOG = LogFactory.getLog(TestAvatarQuiesce.class);

  private MiniAvatarCluster cluster;
  private Configuration conf;
  private FileSystem fs;
  private Random random = new Random();

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    MiniAvatarCluster.createAndStartZooKeeper();
  }

  private void setUp() throws Exception {
    conf = new Configuration();
    conf.setBoolean("fs.ha.retrywrites", true);
    conf.setBoolean("fs.checkpoint.enabled", true);
    conf.setLong("fs.checkpoint.period", 2);

    cluster = new MiniAvatarCluster(conf, 3, true, null, null);
    fs = cluster.getFileSystem();
  }

  @After
  public void tearDown() throws Exception {
    fs.close();
    cluster.shutDown();
    InjectionHandler.clear();
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

  private void testQuiesceWhenSavingNamespace(InjectionEvent event, 
      boolean expectException)
      throws Exception {
    TestAvatarQuiesceHandler h = new TestAvatarQuiesceHandler(event);
    InjectionHandler.set(h);
    setUp();
    // fail once
    AvatarNode primary = cluster.getPrimaryAvatar(0).avatar;
    AvatarNode standby = cluster.getStandbyAvatar(0).avatar;
    
    createEdits(20);

    // wait to the savers are up for some checkpoint
    // and waiting until we interrupt
    try {
      Thread.sleep(10000);
    } catch (Exception e) {
    }
    
    standby.quiesceStandby(getCurrentTxId(primary) - 1);
    assertEquals(20, getCurrentTxId(primary));
    assertEquals(getCurrentTxId(primary), getCurrentTxId(standby));
    if(expectException)
      assertTrue(h.exceptionEvent);
  }

  @Test
  public void testQuiesceWhenSN0() throws Exception {
    // save namespace - initiated
    // current - not moved
    // saver threads - not initiated
    // sn should stop before moving current
    // interruprion comes before sn starts
    testQuiesceWhenSavingNamespace(
        InjectionEvent.FSIMAGE_STARTING_SAVE_NAMESPACE, true);
  }

  @Test
  public void testQuiesceWhenSN1() throws Exception {
    // save namespace - initiated
    // current - moved
    // saver threads - not initiated
    // sn should get InterruptedException when joining saverThreads
    // interruption comes before calling saver.join()
    testQuiesceWhenSavingNamespace(
        InjectionEvent.FSIMAGE_CREATING_SAVER_THREADS, true);
  }

  @Test
  public void testQuiesceWhenSN2() throws Exception {
    // save namespace - initiated
    // current - moved
    // saver threads - initiated
    // sn should get InterruptedException when joining saverThreads
    // interruption comes during calling saver.join()
    testQuiesceWhenSavingNamespace(
        InjectionEvent.FSIMAGE_STARTING_SAVER_THREAD, true);
  }
  
  @Test
  public void testQuiesceWhenSN3() throws Exception {
    // save namespace - completed
    // interruption comes during after SN cleanup - hence no exception is thrown
    testQuiesceWhenSavingNamespace(
        InjectionEvent.FSIMAGE_SN_CLEANUP, false);
  }

  class TestAvatarQuiesceHandler extends InjectionHandler {

    // specifies where the thread should wait for interruption
    private InjectionEvent synchronizationPoint;
    public boolean exceptionEvent = false;
    private volatile boolean receivedCancelRequest = false;

    public TestAvatarQuiesceHandler(InjectionEvent se) {
      synchronizationPoint = se;
    }

    @Override
    protected void _processEvent(InjectionEvent event, Object... args) {
      if (event == InjectionEvent.FSIMAGE_CANCEL_REQUEST_RECEIVED) {
        LOG.info("Injection handler: processing event - " + event);
        receivedCancelRequest = true;
        return;
      }
      if (synchronizationPoint == event) {
        LOG.info("Will wait until save namespace is cancelled - TESTING ONLY : "
            + synchronizationPoint);
        while (!receivedCancelRequest) {
          try {
            Thread.sleep(1000);
          } catch (InterruptedException e) {  }
        }
        LOG.info("FINISHED WAITING - TESTING ONLY : " + synchronizationPoint);
      } else if (event ==InjectionEvent.STANDBY_CANCELLED_EXCEPTION_THROWN) {
        exceptionEvent = true;
      }
    }
  }
}
