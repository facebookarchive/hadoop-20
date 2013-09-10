package org.apache.hadoop.hdfs;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.server.namenode.AvatarNode;
import org.apache.hadoop.hdfs.util.InjectionEvent;
import org.apache.hadoop.security.UnixUserGroupInformation;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.InjectionEventI;
import org.apache.hadoop.util.InjectionHandler;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;

import org.junit.After;
import org.junit.AfterClass;
import static org.junit.Assert.*;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestAvatarShutdown {

  final static Log LOG = LogFactory.getLog(TestAvatarShutdown.class);

  private MiniAvatarCluster cluster;
  private Configuration conf;
  private FileSystem fs;
  private Random random = new Random();
  private List<Path> files = Collections
      .synchronizedList(new ArrayList<Path>());
  
  private static int BLOCK_SIZE = 1024;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    MiniAvatarCluster.createAndStartZooKeeper();
  }

  private void setUp(String name, boolean discardLastBlock) throws Exception {
    LOG.info("------------------- test: " + name + " START ----------------");
    conf = new Configuration();
    conf.setInt("dfs.block.size", BLOCK_SIZE);
    conf.setBoolean("fs.ha.retrywrites", true);
    conf.setBoolean("fs.checkpoint.enabled", true);
    conf.setLong("fs.checkpoint.period", 2);
    conf.setBoolean("dfs.leaserecovery.discardlastblock.ifnosync", discardLastBlock);
    conf.setBoolean("dfs.sync.on.every.addblock", true);

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

  public List<Thread> createEdits(int nEdits) throws IOException {
    List<Thread> threads = new ArrayList<Thread>();
    for (int i = 0; i < nEdits / 2; i++) {
      Thread t = new Thread(new ClientThread());
      t.start();
      threads.add(t);
    }
    return threads;
  }

  public void joinThreads(List<Thread> threads) {
    for (Thread t : threads) {
      try {
        t.interrupt();
        t.join();
      } catch (InterruptedException e) {
        LOG.error("Interrupttion received");
      }
    }
  }

  class ClientThread implements Runnable {

    @Override
    public void run() {
      try {
        Path p = new Path("/" + random.nextInt());
        files.add(p);
        fs.create(p);
      } catch (IOException e) {
        LOG.error("Error when creating a file");
      }
    }
  }
  
  ////////////////////////////////////////////////////////////////////////
  
  /**
   * Basic test to check if the number of blocks obtained 
   * during shutdown is correct.
   */
  @Test
  public void testBasic() throws Exception {
    TestAvatarShutdownHandler h = new TestAvatarShutdownHandler(null);
    InjectionHandler.set(h);
    setUp("testBasic", false);
    for(int i=0; i<10; i++) {
      Path p = new Path("/file"+i);
      DFSTestUtil.createFile(fs, p, 512, (short) 1, 0L);
    }
    DFSTestUtil.waitNSecond(5);
    
    cluster.shutDown();
    LOG.info("Cluster is down");
    assertEquals(10, h.totalBlocks);
  }

  @Test
  public void testOngoingRequestSuceedsDuringShutdown() throws Exception {
    TestAvatarShutdownHandler h = new TestAvatarShutdownHandler(
        InjectionEvent.NAMENODE_AFTER_CREATE_FILE);
    InjectionHandler.set(h);
    setUp("testOngoingRequestSuceedsDuringShutdown", false);

    // start clients
    List<Thread> threads = createEdits(20);
    Thread.sleep(5000);
    
    cluster.shutDownAvatarNodes();
    joinThreads(threads);
    LOG.info("Cluster is down");
    
    // restart nodes in the cluster
    cluster.restartAvatarNodes();
    for (Path p : files) {
      assertTrue(fs.exists(p));
    }
  }
  
  /**
   * Check if the node shuts down cleanly if the lease recovery is
   * in progress, and it is to discard last block of the file under
   * construction.
   */  
  @Test
  public void testLeaseRecoveryShutdownDiscardLastBlock() throws Exception {
    testLeaseRecoveryShutdown(true);
  }
  
  /**
   * Check if the node shuts down cleanly if the lease recovery is
   * in progress, and it is to recover last block of the file under
   * construction.
   */ 
  @Test
  public void testLeaseRecoveryShutdownDoNotDiscardLastBlock() throws Exception {
    testLeaseRecoveryShutdown(false);
  }
  
  private void testLeaseRecoveryShutdown(boolean discardLastBlock)
      throws Exception {
    TestAvatarShutdownHandler h = new TestAvatarShutdownHandler(
        InjectionEvent.LEASEMANAGER_CHECKLEASES);
    InjectionHandler.set(h);
    setUp("testOngoingLeaseRecoveryDuringShutdown", discardLastBlock);

    // set cgi to avoid NPE at NN, since we talk to it directly
    UserGroupInformation.setCurrentUser(UnixUserGroupInformation.login(conf));
    AvatarNode nn = cluster.getPrimaryAvatar(0).avatar;
    FsPermission perm = new FsPermission((short) 0264);
    String clientName = ((DistributedAvatarFileSystem) fs)
        .getClient().getClientName();
    
    // create a file directly and add one block
    nn.create("/test", perm, clientName, true, true, (short) 3, (long) 1024);
    nn.addBlock("/test", clientName);

    assertEquals(1,
        cluster.getPrimaryAvatar(0).avatar.namesystem.getBlocksTotal());
    
    // set lease period to something short
    cluster.getPrimaryAvatar(0).avatar.namesystem.leaseManager.setLeasePeriod(
        1, 1);
    cluster.getPrimaryAvatar(0).avatar.namesystem.lmthread.interrupt();
    
    // wait for the lease manager to kick in
    while(!h.processedEvents.contains(InjectionEvent.LEASEMANAGER_CHECKLEASES))
      DFSTestUtil.waitSecond();
    
    fs.close();
    
    // shutdown the primary, to obtain the block count there
    cluster.killPrimary();
    assertEquals(discardLastBlock ? 0 : 1, h.totalBlocks);
    
    cluster.shutDown();
    LOG.info("Cluster is down");
  }

  class TestAvatarShutdownHandler extends InjectionHandler {

    // specifies where the thread should wait for interruption
    private InjectionEvent synchronizationPoint;
    private volatile boolean stopRPCcalled = false;
    private volatile boolean stopLeaseManagerCalled = false;
    Set<InjectionEventI> processedEvents = Collections
        .synchronizedSet(new HashSet<InjectionEventI>());
    long totalBlocks = -1;

    public TestAvatarShutdownHandler(InjectionEvent se) {
      synchronizationPoint = se;
    }

    @Override
    protected void _processEvent(InjectionEventI event, Object... args) {
      processedEvents.add(event);
      // rpc handlers
      if (synchronizationPoint == event
          && event == InjectionEvent.NAMENODE_AFTER_CREATE_FILE) {
        LOG.info("Will wait until shutdown is called: " + synchronizationPoint);
        while (!stopRPCcalled)
          DFSTestUtil.waitSecond();
        LOG.info("Finished waiting: " + synchronizationPoint);
      } else if (event == InjectionEvent.NAMENODE_STOP_RPC) {
        stopRPCcalled = true;
      // lease recovery
      } else if (synchronizationPoint == event
          && event == InjectionEvent.LEASEMANAGER_CHECKLEASES) {
        LOG.info("Will wait until shutdown is called: " + synchronizationPoint);
        while (!stopLeaseManagerCalled)
          DFSTestUtil.waitSecond();
        LOG.info("Finished waiting: " + synchronizationPoint);
      } else if (event == InjectionEvent.FSNAMESYSTEM_STOP_LEASEMANAGER) {
        stopLeaseManagerCalled = true;
      }
    }

    @Override
    protected void _processEventIO(InjectionEventI event, Object... args)
        throws IOException {
      _processEvent(event, args);
    }
    
    @Override
    protected boolean _falseCondition(InjectionEventI event, Object... args) {
      if (event == InjectionEvent.AVATARNODE_SHUTDOWN) {
        totalBlocks = (Long) args[0];
      }
      if (event == InjectionEvent.LEASEMANAGER_CHECKINTERRUPTION) {
        return true;
      }
      return false;
    }
    
    @Override
    protected boolean _trueCondition(InjectionEventI event, Object... args) {
      if (event == InjectionEvent.LEASEMANAGER_CHECKINTERRUPTION) {
        return false;
      }
      return true;
    }
  }
}
