/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.apache.hadoop.mapred;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import junit.framework.TestCase;

/**
 *
 * @author dms
 */
public class TestClustersBalancer extends TestCase {

  public void testClusterBalancer() throws IOException {
    MiniMRCluster mr = null;
    try {
      mr = new MiniMRCluster(2, "file:///", 3);

      int infoPort = mr.getJobTrackerRunner().getJobTrackerInfoPort();

      ClustersBalancer balancer = new ClustersBalancer();
      Map<String, String> conf =
              balancer.getJobTrackerConf("http://localhost:" + infoPort);
      assertTrue(conf.containsKey("mapred.job.tracker"));
      assertTrue(conf.containsKey("slaves.file"));
      assertTrue(conf.containsKey("mapred.hosts"));
      assertTrue(conf.containsKey("mapred.hosts.exclude"));
      assertTrue(conf.containsKey("version"));


      List<TaskTrackerLoadInfo> trackers =
              balancer.getJobTrackerStatus("http://localhost:" + infoPort);
      assertEquals(2, trackers.size());

    } finally {
      if (mr != null) {
        mr.shutdown();
      }
    }
  }
}
