package org.apache.hadoop.hdfs.server.datanode;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.MiniAvatarCluster;
import org.apache.hadoop.hdfs.server.datanode.AvatarDataNode.ServicePair;
import org.apache.hadoop.hdfs.util.InjectionEvent;
import org.apache.hadoop.util.InjectionEventI;
import org.apache.hadoop.util.InjectionHandler;

import org.junit.After;
import org.junit.AfterClass;
import static org.junit.Assert.*;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestAvatarDataNodeRestartService {
  protected static MiniAvatarCluster cluster;
  private static boolean pass = true;
  private static boolean injectionDone = false;
  private static Log LOG = LogFactory
      .getLog(TestAvatarDataNodeRestartService.class);
  private static Thread rThread = null;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    MiniAvatarCluster.createAndStartZooKeeper();
  }

  @Before
  public void setUp() throws Exception {
    cluster = new MiniAvatarCluster(new Configuration(), 1, true, null, null);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    MiniAvatarCluster.shutDownZooKeeper();
  }

  @After
  public void tearDown() throws Exception {
    InjectionHandler.clear();
    injectionDone = false;
    cluster.shutDown();
  }

  private static class RestartThread extends Thread {
    private final boolean serviceOne;

    public RestartThread(boolean serviceOne) {
      this.serviceOne = serviceOne;
    }
    public void run() {
      try {
        if (serviceOne) {
          cluster.killPrimary();
        } else {
          cluster.killStandby();
        }
      } catch (Exception e) {
        LOG.error("restart failed", e);
        pass = false;
      }
      if (serviceOne) {
        restartService1();
      } else {
        restartService2();
      }
    }
  }

  private static void waitInjection() {
    try {
      Thread.sleep(10000);
    } catch (InterruptedException ie) {

    }
    injectionDone = true;
  }

  private static class TestRestartServiceHandler extends InjectionHandler {
    private boolean enableOne = true;
    @Override
    public void _processEvent(InjectionEventI event, Object... args) {
      if (event == InjectionEvent.AVATARDATANODE_START_OFFERSERVICE1
          && enableOne) {
        rThread = new RestartThread(true);
        rThread.start();
        waitInjection();
      }
      if (event == InjectionEvent.AVATARDATANODE_START_OFFERSERVICE2
          && !enableOne) {
        rThread = new RestartThread(false);
        rThread.start();
        waitInjection();
      }
    }
  }

  private static ServicePair getPair() {
    AvatarDataNode dn = cluster.getDataNodes().get(0);
    return (ServicePair) dn.getAllNamespaceServices()[0];
  }

  private static void restartService1() {
    try {
      getPair().restartService1();
    } catch (Exception e) {
      LOG.error("restart failed", e);
      pass = false;
    }
  }

  private static void restartService2() {
    try {
      getPair().restartService2();
    } catch (Exception e) {
      LOG.error("restart failed", e);
      pass = false;
    }
  }

  @Test
  public void testDatanodeRestartService1() throws Exception {
    InjectionHandler.set(new TestRestartServiceHandler());
    restartService1();
    while (!injectionDone) {
      Thread.sleep(100);
    }
    Thread.sleep(10000);
    assertTrue(pass);
    ServicePair pair = getPair();
    assertFalse((pair.avatarnode1 == null && pair.offerService1.shouldRun()));
    assertFalse((pair.namenode1 == null && pair.offerService1.shouldRun()));
  }

  @Test
  public void testDatanodeRestartService2() throws Exception {
    ServicePair pair = getPair();
    TestRestartServiceHandler handler = new TestRestartServiceHandler();
    handler.enableOne = false;
    InjectionHandler.set(handler);
    restartService2();
    while (!injectionDone) {
      Thread.sleep(100);
    }
    Thread.sleep(10000);
    assertTrue(pass);
    assertFalse((pair.avatarnode2 == null && pair.offerService2.shouldRun()));
    assertFalse((pair.namenode2 == null && pair.offerService2.shouldRun()));
  }
}
