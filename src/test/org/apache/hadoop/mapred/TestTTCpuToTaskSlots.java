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

import org.apache.hadoop.util.ResourceCalculatorPlugin;
import org.junit.Assert;
import org.junit.Test;


/**
 * Test the configuration for number of CPU to number of maximum map tasks
 */
public class TestTTCpuToTaskSlots {
  MiniMRCluster miniMRCluster = null;
  TaskTracker taskTracker = null;

  @Test
  public void TestCpuToMapTasksConfig() throws Exception {
    JobConf conf = new JobConf();
    conf.set("mapred.tasktracker.map.tasks.maximum", "3");
    conf.set("mapred.tasktracker.reduce.tasks.maximum", "1");
    // Test with the original settings
    try {
      miniMRCluster =
          new MiniMRCluster(1, "file:///", 3, null, null, conf);
      taskTracker = miniMRCluster.getTaskTrackerRunner(0).getTaskTracker();
      Assert.assertEquals(3, taskTracker.getMaxCurrentMapTasks());
      Assert.assertEquals(1, taskTracker.getMaxCurrentReduceTasks());
    } finally {
      if (miniMRCluster != null) {
        miniMRCluster.shutdown();
      }
    }

    // Test with the # CPU -> mappers settings
    conf.setClass(org.apache.hadoop.mapred.TaskTracker.
        MAPRED_TASKTRACKER_MEMORY_CALCULATOR_PLUGIN_PROPERTY,
        DummyResourceCalculatorPlugin.class, ResourceCalculatorPlugin.class);
    conf.set("mapred.tasktracker.cpus.to.maptasks", "4:6, 8:9, 16:15");
    conf.set("mapred.tasktracker.cpus.to.reducetasks", "4:3, 8:7, 16:12");

    // 4 CPU -> 6 mappers, 3 reducers
    try {
      conf.set(DummyResourceCalculatorPlugin.NUM_PROCESSORS, "4");
      miniMRCluster =
          new MiniMRCluster(1, "file:///", 3, null, null, conf);
      taskTracker = miniMRCluster.getTaskTrackerRunner(0).getTaskTracker();
      Assert.assertEquals(6, taskTracker.getMaxCurrentMapTasks());
      Assert.assertEquals(3, taskTracker.getMaxCurrentReduceTasks());
    } finally {
      if (miniMRCluster != null) {
        miniMRCluster.shutdown();
      }
    }

    // 8 CPU -> 9 mappers, 7 reduces
    try {
      conf.set(DummyResourceCalculatorPlugin.NUM_PROCESSORS, "8");
      miniMRCluster =
          new MiniMRCluster(1, "file:///", 3, null, null, conf);
      taskTracker = miniMRCluster.getTaskTrackerRunner(0).getTaskTracker();
      Assert.assertEquals(9, taskTracker.getMaxCurrentMapTasks());
      Assert.assertEquals(7, taskTracker.getMaxCurrentReduceTasks());
    } finally {
      if (miniMRCluster != null) {
        miniMRCluster.shutdown();
      }
    }

    // 16 CPU -> 15 mappers, 12 reduces
    try {
      conf.set(DummyResourceCalculatorPlugin.NUM_PROCESSORS, "16");
      miniMRCluster =
          new MiniMRCluster(1, "file:///", 3, null, null, conf);
      taskTracker = miniMRCluster.getTaskTrackerRunner(0).getTaskTracker();
      Assert.assertEquals(15, taskTracker.getMaxCurrentMapTasks());
      Assert.assertEquals(12, taskTracker.getMaxCurrentReduceTasks());
    } finally {
      if (miniMRCluster != null) {
        miniMRCluster.shutdown();
      }
    }

    // 11 CPU -> 3 mappers, 1 reduce (back to default)
    try {
      conf.set(DummyResourceCalculatorPlugin.NUM_PROCESSORS, "11");
      miniMRCluster =
          new MiniMRCluster(1, "file:///", 3, null, null, conf);
      taskTracker = miniMRCluster.getTaskTrackerRunner(0).getTaskTracker();
      Assert.assertEquals(3, taskTracker.getMaxCurrentMapTasks());
      Assert.assertEquals(1, taskTracker.getMaxCurrentReduceTasks());
    } finally {
      if (miniMRCluster != null) {
        miniMRCluster.shutdown();
      }
    }
  }
}
