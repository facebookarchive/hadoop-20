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

import java.util.ArrayList;
import java.util.List;

import junit.framework.Assert;
import junit.framework.TestCase;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.examples.SleepJob;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.TaskCompletionEvent;
import org.apache.hadoop.util.ToolRunner;

public class TestCompletionEvents extends TestCase {
  private static final Log LOG =
      LogFactory.getLog(TestMiniCoronaRunJob.class);
  private MiniCoronaCluster corona = null;

  
  /*
   * This unit test runs a sleep job using the Corona with some mappers and reducers
   * and tries to find if the TaskCompletionEvent object is indeed logging all
   * the completion events of the reducers/mappers. This unittest reproduces the
   * bug that the task completion events are not properly logged for the Corona.
   * As of now, this test is commented out, it should be uncommented once
   * this bug is fixed. 
   */
  public void testTaskCompletionEvents() throws Exception {
    LOG.info("Starting SleepJob to test TaskCompletionEvents");
    corona = new MiniCoronaCluster.Builder().numTaskTrackers(1).build();
    JobConf conf = corona.createJobConf();
    
    SleepJob sleepJob = new SleepJob();
    ToolRunner.run(conf, sleepJob, new String[] { "-m", "10", "-r", "10",
        "-mt", "5", "-rt", "5" });
    List<TaskCompletionEvent> events = new ArrayList<TaskCompletionEvent>();
    int index = 0;
    int numReduces = 0;
    int numMaps = 0;
    RunningJob rjob = sleepJob.getRunningJob();
    while (true) {
      TaskCompletionEvent[] someEvents = rjob.getTaskCompletionEvents(index);
      if (someEvents.length == 0) {
        break;
      }
      for (int i = 0; i < someEvents.length; i++) {
        if (someEvents[i].isMapTask()) {
          numMaps++;
        } else {
          numReduces++;
        }
      }
      index += someEvents.length;
    }
    Assert.assertEquals("Num maps is " + numMaps, 10, numMaps);
    Assert.assertTrue("Num reduces is " + numReduces, numReduces >= 10);
  }

  @Override
  protected void tearDown() {
    if (corona != null) {
      corona.shutdown();
    }
  }
}
