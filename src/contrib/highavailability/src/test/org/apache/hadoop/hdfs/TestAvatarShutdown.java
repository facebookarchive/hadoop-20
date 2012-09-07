package org.apache.hadoop.hdfs;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.util.InjectionEvent;
import org.apache.hadoop.hdfs.util.InjectionHandler;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

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

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    MiniAvatarCluster.createAndStartZooKeeper();
  }

  private void setUp(String name) throws Exception {
    LOG.info("TEST: " + name);
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

  @Test
  public void testOngoingRequestSuceedsDuringShutdown() throws Exception {
    TestAvatarShutdownHandler h = new TestAvatarShutdownHandler(
        InjectionEvent.NAMENODE_AFTER_CREATE_FILE);
    InjectionHandler.set(h);
    setUp("testOngoingRequestSuceedsDuringShutdown");

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

  class TestAvatarShutdownHandler extends InjectionHandler {

    // specifies where the thread should wait for interruption
    private InjectionEvent synchronizationPoint;
    private volatile boolean shutdownCalled = false;

    public TestAvatarShutdownHandler(InjectionEvent se) {
      synchronizationPoint = se;
    }

    @Override
    protected void _processEvent(InjectionEvent event, Object... args) {
      if (synchronizationPoint == event) {
        LOG.info("Will wait until shutdown is called: " + synchronizationPoint);
        while (!shutdownCalled) {
          try {
            LOG.info("Waiting for shutdown.....");
            Thread.sleep(1000);
          } catch (InterruptedException e) {
          }
        }
        LOG.info("Finished waiting: " + synchronizationPoint);
      } else if (event == InjectionEvent.NAMENODE_STOP_CLIENT_RPC) {
        shutdownCalled = true;
      }
    }

    @Override
    protected void _processEventIO(InjectionEvent event, Object... args) {
      _processEvent(event, args);
    }
  }
}
