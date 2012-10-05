/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.mapred;

import java.util.List;

import junit.framework.TestCase;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.corona.FaultManager;
import org.apache.hadoop.corona.FaultManager.FaultStatsForType;
import org.apache.hadoop.corona.MiniCoronaCluster;
import org.apache.hadoop.corona.ResourceType;
import org.apache.hadoop.corona.TstUtils;
import org.apache.hadoop.examples.SleepJob;
import org.apache.hadoop.util.ToolRunner;

public class TestMiniCoronaBlacklist extends TestCase {
  private static final Log LOG =
      LogFactory.getLog(TestMiniCoronaBlacklist.class);
  private MiniCoronaCluster corona = null;

  public void testFaultReport() throws Exception {
    LOG.info("Starting testFaultReport");
    // Use only 2 trackers to make sure the faulty tracker gets used.
    corona = new MiniCoronaCluster.Builder().numTaskTrackers(2).build();
    JobConf conf = corona.createJobConf();

    CoronaTaskTracker coronaTT =
      corona.getTaskTrackerRunner(0).getTaskTracker();
    coronaTT.stopActionServer();
    runSleepJob(conf, 10, 10);

    FaultManager fm =
      corona.getClusterManager().getNodeManager().getFaultManager();
    List<FaultManager.FaultStatsForType> faultStats =
        fm.getFaultStats(coronaTT.getName());
    assertTrue(faultStats.size() >= 2);
    for (FaultStatsForType stat : faultStats) {
      if (stat.getType().equals(ResourceType.MAP)
          || stat.getType().equals(ResourceType.REDUCE)) {
        assertEquals(stat.getType() + " Fault not reported to CM ",
            stat.getNumSessionsWithFailedConnections(), 1);
      }
    }
  }

  private void runSleepJob(JobConf conf, int maps, int reduces)
      throws Exception {
    String[] args = { "-m", maps + "", "-r", reduces + "", "-mt", "1", "-rt",
        "1" };
    ToolRunner.run(conf, new SleepJob(), args);
    // This sleep is here to wait for the JobTracker to go down completely
    TstUtils.reliableSleep(1000);
  }
}
