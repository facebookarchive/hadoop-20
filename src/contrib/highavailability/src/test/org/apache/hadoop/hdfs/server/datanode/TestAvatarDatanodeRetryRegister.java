package org.apache.hadoop.hdfs.server.datanode;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.MiniAvatarCluster;
import org.apache.hadoop.hdfs.protocol.FSConstants.DatanodeReportType;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.util.InjectionEvent;
import org.apache.hadoop.util.InjectionEventI;
import org.apache.hadoop.util.InjectionHandler;

import static org.junit.Assert.*;
import org.junit.Test;

public class TestAvatarDatanodeRetryRegister {

  private static int registrations = 0;

  private static class TestAvatarDatanodeRetryRegisterHandler extends InjectionHandler {

    @Override
    public void _processEventIO(InjectionEventI event, Object... args)
        throws IOException {
      if (event == InjectionEvent.OFFERSERVICE_BEFORE_REGISTRATION) {
        registrations++;
        throw new IOException("EXCEPTION!!!");
      }
    }
  }

  /**
   * This tests whether when the datanode is having trouble with registration
   * to the namenode, the offerservice threads for the datanode are restarted
   * and the datanode then is correctly able to register to the namenode.
   */
  @Test(timeout=60000)
  public void testAvatarDatanodeRetryRegister() throws Exception {
    Configuration conf = new Configuration();
    conf.setLong("dfs.heartbeat.interval", 1);
    InjectionHandler.set(new TestAvatarDatanodeRetryRegisterHandler());
    MiniAvatarCluster.createAndStartZooKeeper();
    MiniAvatarCluster cluster = new MiniAvatarCluster(conf, 1, true, null, null);
    try {
      FSNamesystem ns = cluster.getPrimaryAvatar(0).avatar.namesystem;
      // These two lines make sure the datanode is marked as dead and is no longer registered.
      ns.setDatanodeDead(ns.getDatanode(cluster.getDataNodes().get(0)
            .getDNRegistrationForNS(cluster.getNamespaceId(0))));
      ns.removeDatanode(cluster.getDataNodes().get(0)
          .getDNRegistrationForNS(cluster.getNamespaceId(0)));
      assertEquals(0,
          ns.getNameNode().getDatanodeReport(DatanodeReportType.LIVE).length);
      int livenodes = ns.getNameNode().getDatanodeReport(DatanodeReportType.LIVE).length;
      // Wait for the datanode to register successfully.
      while (registrations != 1 || livenodes != 1) {
        System.out.println("Waiting for registrations : " + registrations);
        Thread.sleep(1000);
        livenodes = ns.getNameNode().getDatanodeReport(DatanodeReportType.LIVE).length;
      }
      assertEquals(1,
          ns.getNameNode().getDatanodeReport(DatanodeReportType.LIVE).length);
      assertEquals(1, registrations);
    } finally {
      cluster.shutDown();
      MiniAvatarCluster.shutDownZooKeeper();
    }
  }
}
