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

import junit.framework.TestCase;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.CoronaJobTracker;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;

public class TestMiniCoronaFederatedJT extends TestCase {
  private static final Log LOG = LogFactory.getLog(TestMiniCoronaRunJob.class);
  private MiniCoronaCluster corona = null;

  @Override
  protected void tearDown() {
    if (corona != null) {
      corona.shutdown();
    }
  }

  public void testOneRemoteJT() throws Exception {
    LOG.info("Starting testOneRemoteJT");
    String[] racks = "/rack-1".split(",");
    String[] trackers = "tracker-1".split(",");
    corona = new MiniCoronaCluster.Builder().numTaskTrackers(1).racks(racks)
        .hosts(trackers).build();
    Configuration conf = corona.createJobConf();
    conf.set("mapred.job.tracker", "corona");
    conf.set("mapred.job.tracker.class", CoronaJobTracker.class.getName());
    String locationsCsv = "tracker-1";
    conf.set("test.locations", locationsCsv);
    conf.setBoolean("mapred.coronajobtracker.forceremote", true);
    Job job = new Job(conf);
    job.setMapperClass(TstJob.TestMapper.class);
    job.setInputFormatClass(TstJob.TestInputFormat.class);
    job.setOutputFormatClass(NullOutputFormat.class);
    job.setNumReduceTasks(0);
    job.getConfiguration().set("io.sort.record.pct", "0.50");
    job.getConfiguration().set("io.sort.mb", "25");
    boolean success = job.waitForCompletion(true);
    assertTrue("Job did not succeed", success);
  }

}
