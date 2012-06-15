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

package org.apache.hadoop.corona;

import java.io.IOException;

import junit.framework.Assert;
import junit.framework.TestCase;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.examples.SleepJob;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.CoronaJobInProgress;
import org.apache.hadoop.mapred.CoronaJobTracker;
import org.apache.hadoop.mapred.CoronaTaskTracker;
import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobInProgress;
import org.apache.hadoop.util.ToolRunner;

/**
 * Test task speculative execution in of Mini Corona Map-Reduce Cluster.
 */
public class TestMiniCoronaSpeculativeTask extends TestCase {
  private static final Log LOG =
      LogFactory.getLog(TestMiniCoronaSpeculativeTask.class);
  private static String TEST_ROOT_DIR = new Path(System.getProperty(
		    "test.build.data", "/tmp")).toString().replace(' ', '+');
  private MiniCoronaCluster corona = null;
  
  public void testLastTaskSpeculation() throws Exception {
    corona = new MiniCoronaCluster.Builder().numTaskTrackers(2).build();
    JobConf conf = corona.createJobConf();
    conf.setSpeculativeExecution(true);
    conf.setMapSpeculativeLag(1L);
    conf.setReduceSpeculativeLag(1L);
    conf.setLong(JobInProgress.REFRESH_TIMEOUT, 100L);
    conf.setLong(CoronaTaskTracker.HEART_BEAT_INTERVAL_KEY, 100L);
    conf.setLong(CoronaJobTracker.HEART_BEAT_INTERVAL_KEY, 100L);
    long start = System.currentTimeMillis();
    SleepJob sleepJob = new SleepJob();
    ToolRunner.run(conf, sleepJob,
        new String[]{ "-m", "1", "-r", "1",
                      "-mt", "5000", "-rt", "5000",
                      "-speculation"});
    long end = System.currentTimeMillis();
    verifyLaunchedTasks(sleepJob, 2, 2);
    new ClusterManagerMetricsVerifier(corona.getClusterManager(),
        2, 2, 2, 2, 2, 2, 0, 0).verifyAll();
    LOG.info("Time spent for testOneTaskWithOneTaskTracker:" +
        (end - start));
  }

  @Override
  protected void tearDown() {
    if (corona != null) {
      corona.shutdown();
    }
  }

  private void verifyLaunchedTasks(SleepJob sleepJob, int maps, int reduces)
      throws IOException {
    Counters jobCounters = sleepJob.getRunningJob().getCounters();
    long launchedMaps = jobCounters.findCounter(
        JobInProgress.Counter.TOTAL_LAUNCHED_MAPS).getValue();
    long launchedReduces = jobCounters.findCounter(
        JobInProgress.Counter.TOTAL_LAUNCHED_REDUCES).getValue();
    Assert.assertEquals(maps, launchedMaps);
    Assert.assertEquals(reduces, launchedReduces);
  }

}
