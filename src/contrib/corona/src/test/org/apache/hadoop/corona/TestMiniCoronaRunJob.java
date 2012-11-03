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
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.FutureTask;
import java.util.concurrent.atomic.AtomicBoolean;

import junit.framework.Assert;
import junit.framework.TestCase;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.examples.SleepJob;
import org.apache.hadoop.mapred.CoronaJobTracker;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.util.ToolRunner;

/**
 * A Unit-test to test job execution of Mini Corona Map-Reduce Cluster.
 */
public class TestMiniCoronaRunJob extends TestCase {
  private static final Log LOG =
      LogFactory.getLog(TestMiniCoronaRunJob.class);
  private MiniCoronaCluster corona = null;

  public void testEmptyJob() throws Exception {
    LOG.info("Starting testEmptyJob");
    corona = new MiniCoronaCluster.Builder().numTaskTrackers(1).build();
    JobConf conf = corona.createJobConf();
    long start = System.currentTimeMillis();
    String[] args = {"-m", "0", "-r", "0", "-mt", "1", "-rt", "1", "-nosetup" };
    ToolRunner.run(conf, new SleepJob(), args);
    // This sleep is here to wait for the JobTracker to go down completely
    TstUtils.reliableSleep(1000);
    long end = System.currentTimeMillis();
    LOG.info("Time spent for testEmptyJob:" + (end - start));
  }

  public void testOneTaskWithOneTaskTracker() throws Exception {
    LOG.info("Starting testOneTaskWithOneTaskTracker");
    corona = new MiniCoronaCluster.Builder().numTaskTrackers(1).build();
    JobConf conf = corona.createJobConf();
    long start = System.currentTimeMillis();
    runSleepJob(conf, 1, 1);
    long end = System.currentTimeMillis();
    new ClusterManagerMetricsVerifier(corona.getClusterManager(),
        1, 1, 1, 1, 1, 1, 0, 0).verifyAll();
    LOG.info("Time spent for testOneTaskWithOneTaskTracker:" +
        (end - start));
  }

  public void testOneTaskWithMultipleTaskTracker() throws Exception {
    LOG.info("Starting testOneTaskWithMultipleTaskTracker");
    corona = new MiniCoronaCluster.Builder().numTaskTrackers(4).build();
    JobConf conf = corona.createJobConf();
    long start = System.currentTimeMillis();
    runSleepJob(conf, 1, 1);
    long end = System.currentTimeMillis();
    new ClusterManagerMetricsVerifier(corona.getClusterManager(),
        1, 1, 1, 1, 1, 1, 0, 0).verifyAll();
    LOG.info("Time spent for testOneTaskWithMultipleTaskTracker:" +
        (end - start));
  }

  public void testManyTasksWithManyTaskTracker() throws Exception {
    LOG.info("Starting testManyTasksWithManyTaskTracker");
    corona = new MiniCoronaCluster.Builder().numTaskTrackers(4).build();
    JobConf conf = corona.createJobConf();
    long start = System.currentTimeMillis();
    runSleepJob(conf, 10, 10);
    long end = System.currentTimeMillis();
    new ClusterManagerMetricsVerifier(corona.getClusterManager(),
        10, 10, 10, 10, 10, 10, 0, 0).verifyAll();
    LOG.info("Time spent for testManyTasksWithManyTaskTracker:" +
        (end - start));
  }

  public void testMultipleJobClients() throws Exception {
    LOG.info("Starting testMultipleJobClients");
    corona = new MiniCoronaCluster.Builder().numTaskTrackers(4).build();
    JobConf conf = corona.createJobConf();
    long start = System.currentTimeMillis();
    runMultipleSleepJobs(conf, 10, 10, 5);
    long end = System.currentTimeMillis();
    new ClusterManagerMetricsVerifier(corona.getClusterManager(),
        50, 50, 50, 50, 50, 50, 0, 0).verifyAll();
    LOG.info("Time spent for testMultipleJobClients:" +
        (end - start));
  }

  public void testMemoryLimit() throws Exception {
    LOG.info("Starting testMemoryLimit");
    JobConf conf = new JobConf();
    conf.setInt(CoronaConf.NODE_RESERVED_MEMORY_MB, Integer.MAX_VALUE);
    corona = new MiniCoronaCluster.Builder().conf(conf).numTaskTrackers(2).build();
    final JobConf jobConf = corona.createJobConf();
    long start = System.currentTimeMillis();
    FutureTask<Boolean> task = submitSleepJobFutureTask(jobConf);
    checkTaskNotDone(task, 10);
    NodeManager nm =  corona.getClusterManager().getNodeManager();
    nm.getResourceLimit().setNodeReservedMemoryMB(0);
    Assert.assertTrue(task.get());
    long end = System.currentTimeMillis();
    LOG.info("Task Done. Verifying");
    new ClusterManagerMetricsVerifier(corona.getClusterManager(),
        1, 1, 1, 1, 1, 1, 0, 0).verifyAll();
    LOG.info("Time spent for testMemoryLimit:" +
        (end - start));
  }

  public void testDiskLimit() throws Exception {
    LOG.info("Starting testDiskLimit");
    JobConf conf = new JobConf();
    conf.setInt(CoronaConf.NODE_RESERVED_DISK_GB, Integer.MAX_VALUE);
    corona = new MiniCoronaCluster.Builder().conf(conf).numTaskTrackers(2).build();
    final JobConf jobConf = corona.createJobConf();
    long start = System.currentTimeMillis();
    FutureTask<Boolean> task = submitSleepJobFutureTask(jobConf);
    checkTaskNotDone(task, 10);
    NodeManager nm =  corona.getClusterManager().getNodeManager();
    nm.getResourceLimit().setNodeReservedDiskGB(0);
    Assert.assertTrue(task.get());
    long end = System.currentTimeMillis();
    new ClusterManagerMetricsVerifier(corona.getClusterManager(),
        1, 1, 1, 1, 1, 1, 0, 0).verifyAll();
    LOG.info("Time spent for testDiskLimit:" +
        (end - start));
  }

  private FutureTask<Boolean> submitSleepJobFutureTask(final JobConf conf) {
    FutureTask<Boolean> task = new FutureTask<Boolean>(
        new Callable<Boolean>() {
          public Boolean call() throws IOException {
            try {
              runSleepJob(conf, 1, 1);
            } catch (Exception e) {
              LOG.error("Sleep Job Failed", e);
              return false;
            }
            return true;
          }
        });
    new Thread(task).start();
    return task;
  }

  private void checkTaskNotDone(FutureTask<Boolean> task, int seconds)
      throws Exception {
    for (int i = 0; i < seconds; ++i) {
      if (task.isDone()) {
        // Job should not finish because of the memory limit
        Assert.fail();
      }
      Thread.sleep(1L);
    }
  }

  public void testLocality() throws Exception {
    LOG.info("Starting testOneTaskWithOneTaskTracker");
    String[] racks = "/rack-1,/rack-1,/rack-2,/rack-3".split(",");
    String[] trackers = "tracker-1,tracker-2,tracker-3,tracker-4".split(",");
    String locationsCsv = "tracker-1,tracker-1,tracker-3,tracker-3";
    corona = new MiniCoronaCluster.Builder().
      numTaskTrackers(4).
      racks(racks).
      hosts(trackers).
      build();
    Configuration conf = corona.createJobConf();
    conf.set("mapred.job.tracker", "corona");
    conf.set("mapred.job.tracker.class", CoronaJobTracker.class.getName());
    conf.set("test.locations", locationsCsv);
    Job job = new Job(conf);
    long start = System.currentTimeMillis();
    job.setMapperClass(TstJob.TestMapper.class);
    job.setInputFormatClass(TstJob.TestInputFormat.class);
    job.setOutputFormatClass(NullOutputFormat.class);
    job.setNumReduceTasks(0);
    job.getConfiguration().set("io.sort.record.pct", "0.50");
    job.getConfiguration().set("io.sort.mb", "25");
    boolean success =  job.waitForCompletion(true);
    long end = System.currentTimeMillis();
    new ClusterManagerMetricsVerifier(corona.getClusterManager(),
        4, 0, 4, 0, 4, 0, 0, 0).verifyAll();
    LOG.info("Time spent for testMemoryLimit:" +
        (end - start));
    assertTrue("Job did not succeed", success);
  }

  @Override
  protected void tearDown() {
    if (corona != null) {
      corona.shutdown();
    }
  }

  private void runSleepJob(JobConf conf, int maps, int reduces)
      throws Exception {
    String[] args = {"-m", maps + "",
                     "-r", reduces + "",
                     "-mt", "1",
                     "-rt", "1" };
    ToolRunner.run(conf, new SleepJob(), args);
    // This sleep is here to wait for the JobTracker to go down completely
    TstUtils.reliableSleep(1000);
  }

  private void runMultipleSleepJobs(final JobConf conf,
      final int maps, final int reduces, int numJobs) throws Exception {
    final CountDownLatch startSignal = new CountDownLatch(1);
    final CountDownLatch endSignal = new CountDownLatch(numJobs);
    final AtomicBoolean failed = new AtomicBoolean(false);
    for (int i = 0; i < numJobs; ++i) {
      Runnable action = new Runnable() {
        @Override
        public void run() {
          try {
            startSignal.await();
            runSleepJob(conf, maps, reduces);
            LOG.info("Sleep Job finished");
            endSignal.countDown();
          } catch (Exception e) {
            LOG.error("Exception in running SleepJob", e);
            failed.set(true);
            endSignal.countDown();
          }
        }
      };
      new Thread(action).start();
    }

    // Starting all jobs at the same time
    startSignal.countDown();

    // Waiting for all jobs to finish
    endSignal.await();
    if (failed.get()) {
      fail("Some of the Sleepjobs failed");
    }
  }
}
