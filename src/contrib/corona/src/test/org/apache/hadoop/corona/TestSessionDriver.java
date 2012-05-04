package org.apache.hadoop.corona;

import java.io.IOException;
import java.util.Collection;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import junit.framework.TestCase;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.UtilsForTests;

public class TestSessionDriver extends TestCase {
  final static Log LOG = LogFactory.getLog(TestSessionDriver.class);

  int numNodes = 100;
  Configuration conf;
  ClusterManager cm;
  ClusterManagerServer cms;
  ResourceDriver rd;
  SessionDriver driver;
  UtilsForTests.FakeClock myclock;

  class ResourceDriver implements SessionDriverService.Iface {
    public List<ResourceGrant> granted = new ArrayList<ResourceGrant> ();
    public List<ResourceGrant> revoked = new ArrayList<ResourceGrant> ();

    @Override
    public void grantResource(String handle, List<ResourceGrant> granted) {
      LOG.info("Received " + granted.size() + " grants for session: " + handle);
      this.granted.addAll(granted);
    }

    @Override
    public void revokeResource(String handle,
                               List<ResourceGrant> revoked, boolean force) {
      this.revoked.addAll(revoked);
    }
  }

  protected void setUp() throws Exception {
    conf = new Configuration();
    conf.setClass("topology.node.switch.mapping.impl",
                  org.apache.hadoop.net.IPv4AddressTruncationMapping.class,
                  org.apache.hadoop.net.DNSToSwitchMapping.class);
    conf.setInt(CoronaConf.NOTIFIER_RETRY_INTERVAL_START, 0);
    conf.setInt(CoronaConf.NOTIFIER_RETRY_INTERVAL_FACTOR, 1);
    conf.setInt(CoronaConf.NOTIFIER_RETRY_MAX, 3);
    conf.setInt(CoronaConf.NOTIFIER_POLL_INTERVAL, 10);
    conf.set(CoronaConf.CPU_TO_RESOURCE_PARTITIONING, TstUtils.std_cpu_to_resource_partitioning);

    myclock = new UtilsForTests.FakeClock();
    myclock.advance(System.currentTimeMillis());
    ClusterManager.clock = myclock;

    cm = new ClusterManager(conf);
    cms = new ClusterManagerServer(conf, cm);
    cms.start();

    ClusterNodeInfo nodes [];
    nodes = new ClusterNodeInfo[numNodes];
    for (int i=0; i<numNodes; i++) {
      nodes[i] = new ClusterNodeInfo(TstUtils.getNodeHost(i),
                                     new InetAddress(TstUtils.getNodeHost(i),
                                                     TstUtils.getNodePort(i)),
                                     TstUtils.std_spec);
      nodes[i].setUsed(TstUtils.free_spec);
    }
    for (int i=0; i<numNodes; i++) {
      cm.nodeHeartbeat(nodes[i]);
    }

    rd = new ResourceDriver();
    driver = new SessionDriver(conf, rd);
  }

  protected void tearDown() throws InterruptedException {
    if (cms != null) {
      LOG.info("Stopping ClusterManagerServer");
      cms.stopRunning();
      cms.interrupt();
      cms.join();
    }
    if (driver != null) {
      LOG.info("Stopping SessionDriver");
      driver.stop(SessionStatus.SUCCESSFUL);
      driver.join();
    }
  }

  public void testRoundTrip() throws Throwable {
    try {
      LOG.info("Starting testRoundTrip");

      driver.requestResources(TstUtils.createRequests(100, this.numNodes));
      TestClusterManager.reliableSleep(100);

      assertEquals(rd.granted.size(), 100);

      HashSet<Integer> idSet = new HashSet<Integer> (100);
      for (int i=0; i<100; i++)  {
        idSet.add(i);
      }

      for (ResourceGrant g: rd.granted) {
        idSet.remove(g.id);
      }
      assertEquals(idSet.size(), 0);
    } catch (Throwable t) {
      t.printStackTrace();
    }
  }

  public void testSessionFailure() throws Throwable {

    try {
      LOG.info("Starting testSessionFailure");

      // set the socket timeout to a small number so that the server sockets
      // are closed.
      conf.setInt(CoronaConf.CM_SOTIMEOUT, 100);
      ResourceDriver rd2 = new ResourceDriver();
      SessionDriver driver2 = new SessionDriver(conf, rd2);

      // session #1 requests all the resources
      List<ResourceRequest> d1rq = TstUtils.createRequests(800, this.numNodes);
      driver.requestResources(d1rq);
      TestClusterManager.reliableSleep(100);

      // session #2 requests all the resources as well
      driver2.requestResources(TstUtils.createRequests(800, this.numNodes));
      TestClusterManager.reliableSleep(1000);

      // at this time session #1 should have all the resources. release them
      assertEquals(rd.granted.size(), 800);

      // kill session #2
      driver2.abort();
      driver2.join();

      TestClusterManager.reliableSleep(1000);

      // release all resources from session #1
      driver.releaseResources(d1rq);

      // now CM should attempt to keep giving resources to session #2 for ~20 seconds or so

      // session #1 should get the grants shortly as session #2 is timed out
      driver.requestResources(d1rq);
      TestClusterManager.reliableSleep(1000);
      assertEquals(rd.granted.size(), 1600);

      // session #2 should be declared to be TIMEDOUT by now
      Collection<RetiredSession> retiredSessions = cm.getSessionManager().getRetiredSessions();
      synchronized(retiredSessions) {
        assertEquals(retiredSessions.size(), 1);
        for (RetiredSession s: retiredSessions)
          assertEquals(s.status, SessionStatus.TIMED_OUT);
      }

    } catch (Throwable t) {
      t.printStackTrace();
      throw t;
    }
  }

  public void testCMFailureTransient() throws Throwable {
    try {
      LOG.info("Starting testCMFailureTransient");

      // change the retry settings to allow the sessiondriver to
      // retry requests to the CM periodically

      conf.setInt(CoronaConf.NOTIFIER_RETRY_INTERVAL_START, 100);
      conf.setInt(CoronaConf.NOTIFIER_RETRY_INTERVAL_FACTOR, 1);
      conf.setInt(CoronaConf.NOTIFIER_RETRY_MAX, 10);
      conf.setInt(CoronaConf.NOTIFIER_POLL_INTERVAL, 100);

      // new session driver
      rd = new ResourceDriver();
      driver = new SessionDriver(conf, rd);

      // shutdown the clustermanager server. the CM should still be running
      // but will be inaccessible for sometime.
      cms.stopRunning();
      cms.interrupt();
      cms.join();

      List<ResourceRequest> rlist = TstUtils.createRequests(this.numNodes, 800, 0);

      // requests some resources
      driver.requestResources(rlist.subList(0, 400));

      // CM is down, let the driver keep retrying
      TestClusterManager.reliableSleep(300);

      driver.requestResources(rlist.subList(400, 600));

      // start the CM server
      cms = new ClusterManagerServer(conf, cm);
      cms.start();

      driver.requestResources(rlist.subList(600, 800));

      TestClusterManager.reliableSleep(300);

      // at this time session #1 should have all the resources. release them
      assertEquals(rd.granted.size(), 800);

      LOG.info("Stopping testCMFailureTransient");
    } catch (Throwable t) {
      t.printStackTrace();
      throw t;
    }
  }

  public void testCMFailurePermanent() throws Throwable {
    try {
      LOG.info("Starting testCMFailurePermanent");

      // shutdown the clustermanager server.
      cms.stopRunning();
      cms.interrupt();
      cms.join();

      List<ResourceRequest> rlist = TstUtils.createRequests(this.numNodes, 800, 0);

      // requests some resources
      driver.requestResources(rlist.subList(0, 400));

      // these requests will timeout immediately
      TestClusterManager.reliableSleep(100);

      if (driver.getFailed() == null)
        assertEquals("CM failure not detected", null);

      boolean gotFailed = false;
      try {
        driver.requestResources(rlist.subList(400, 800));
      } catch (IOException e) {
        gotFailed = true;
      }

      assertEquals(gotFailed, true);

      LOG.info("Stopping testCMFailurePermanent");
    } catch (Throwable t) {
      t.printStackTrace();
      throw t;
    }
  }
}
