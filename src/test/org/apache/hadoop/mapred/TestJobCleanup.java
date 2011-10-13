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

import java.io.DataOutputStream;
import java.io.File;

import junit.framework.TestCase;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.lib.IdentityMapper;
import org.apache.hadoop.mapred.lib.IdentityReducer;

public class TestJobCleanup extends TestCase {
  private MiniMRCluster cluster = null;
  private JobConf conf = null;
  final private static String TEST_ROOT_DIR =
      new File(System.getProperty("test.build.data", "/tmp") + "/" 
               + "test-job-cleanup").toString();
  
  public void testJobDirctoryCleanup() throws Exception {
    try {
      conf = new JobConf();
      FileSystem fileSys = FileSystem.get(conf);
      fileSys.delete(new Path(TEST_ROOT_DIR), true);
      cluster = new MiniMRCluster(1, "file:///", 1, null, new String[] {"host1"}, conf);
      JobConf jc = cluster.createJobConf();
      jc.setJobName("TestJob");
      Path inDir = new Path(TEST_ROOT_DIR, "test-input");
      Path outDir = new Path(TEST_ROOT_DIR, "test-output");
      String input = "Test\n";
      DataOutputStream file = fileSys.create(new Path(inDir, "part-" + 0));
      file.writeBytes(input);
      file.close();
      FileInputFormat.setInputPaths(jc, inDir);
      FileOutputFormat.setOutputPath(jc, outDir);
      jc.setInputFormat(TextInputFormat.class);
      jc.setOutputKeyClass(LongWritable.class);
      jc.setOutputValueClass(Text.class);
      jc.setMapperClass(IdentityMapper.class);
      jc.setReducerClass(IdentityReducer.class);
      jc.setNumMapTasks(1);
      jc.setNumReduceTasks(1);
      JobClient jobClient = new JobClient(jc);
      RunningJob job = jobClient.submitJob(jc);
      JobID jobId = job.getID();
      job.waitForCompletion();
      cluster.getTaskTrackerRunner(0).getTaskTracker();
      String subdir = TaskTracker.getLocalJobDir(jobId.toString());
      File dir = new File(cluster.getTaskTrackerLocalDir(0) + "/" + subdir);
      assertEquals(null, dir.list());
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }
  
}
