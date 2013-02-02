package org.apache.hadoop.hdfs.server.datanode;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.MiniAvatarCluster;
import org.apache.hadoop.hdfs.protocol.FSConstants.DatanodeReportType;
import org.apache.hadoop.hdfs.util.InjectionEvent;
import org.apache.hadoop.util.InjectionEventI;
import org.apache.hadoop.util.InjectionHandler;

import org.junit.AfterClass;
import static org.junit.Assert.*;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestAvatarDatanodeNoService {

  private static Configuration conf;
  private static MiniAvatarCluster cluster;
  private static volatile boolean pass = true;
  private static volatile boolean done = false;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    MiniAvatarCluster.createAndStartZooKeeper();
    conf = new Configuration();
    cluster = new MiniAvatarCluster(conf, 1, true, null, null);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    cluster.shutDown();
    MiniAvatarCluster.shutDownZooKeeper();
  }

  private static class TestHandler extends InjectionHandler {
    @Override
    protected void _processEvent(InjectionEventI event, Object... args) {
      if (event == InjectionEvent.AVATARDATANODE_BEFORE_START_OFFERSERVICE1) {
        try {
          if (cluster.getPrimaryAvatar(0) != null) {
            cluster.killPrimary();
            done = true;
          }
        } catch (IOException e) {
          System.out.println("KillPrimary failed : " + e);
          pass = false;
        }
      }
    }

    @Override
    protected void _processEventIO(InjectionEventI event, Object... args)
    throws IOException {
      _processEvent(event, args);
    }
  }

  @Test
  public void testDatanodeNoService() throws Exception {
    cluster.shutDownDataNodes();
    cluster.killStandby();
    cluster.restartStandby();
    InjectionHandler.set(new TestHandler());
    cluster.restartDataNodes(false);
    // Wait for trigger.
    while (!done) {
      System.out.println("Waiting for trigger");
      Thread.sleep(1000);
    }
    int dnReports = cluster.getStandbyAvatar(0).avatar
        .getDatanodeReport(DatanodeReportType.LIVE).length;
    long start = System.currentTimeMillis();
    while (System.currentTimeMillis() - start < 30000 && dnReports != 1) {
      System.out.println("Waiting for dn report");
      Thread.sleep(1000);
      dnReports = cluster.getStandbyAvatar(0).avatar
          .getDatanodeReport(DatanodeReportType.LIVE).length;
    }
    assertEquals(1, dnReports);
    assertTrue(pass);
    assertTrue(done);
  }

}
