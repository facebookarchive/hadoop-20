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

import java.io.IOException;
import java.util.Arrays;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;
import junit.extensions.TestSetup;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.TestMapCollection.FakeIF;
import org.apache.hadoop.mapred.TestMapCollection.FakeSplit;
import org.apache.hadoop.mapred.lib.IdentityReducer;
import org.apache.hadoop.mapred.TaskCompletionEvent;
import org.apache.hadoop.mapred.TestReduceFetch.MapMB;

public class TestReduceTaskNoMapOutput extends TestCase {

  private static final int NUM_HADOOP_SLAVES = 3;
  static class SinkMapper<K, V>
     extends MapReduceBase implements Mapper<K, V, K, V> {

    public void map(K key, V val,
        OutputCollector<K, V> output, Reporter reporter)
    throws IOException {
      // Don't output anything!
      if (false) output.collect(key, val);
    }
  }

  @SuppressWarnings({ "deprecation", "unchecked" })
  public static TaskCompletionEvent[] runJob(JobConf conf, Class mapperClass,
                  boolean enableNoFetchEmptyMapOutputs) throws Exception {
    conf.setMapperClass(mapperClass);
    conf.setReducerClass(IdentityReducer.class);
    conf.setOutputKeyClass(Text.class);
    conf.setOutputValueClass(Text.class);
    conf.setNumMapTasks(3);
    conf.setNumReduceTasks(1);
    conf.setInputFormat(FakeIF.class);
    conf.setBoolean("mapred.enable.no.fetch.map.outputs", enableNoFetchEmptyMapOutputs);
    FileInputFormat.setInputPaths(conf, new Path("/in"));
    final Path outp = new Path("/out");
    FileOutputFormat.setOutputPath(conf, outp);
    RunningJob job = null;
    
    job = JobClient.runJob(conf);
    assertTrue(job.isSuccessful());
    return job.getTaskCompletionEvents(0);
  }

  public void verifyReduceTaskNoMapOutput(boolean enableNoFetchEmptyMapOutputs,
                Class mapperClass, 
                TaskCompletionEvent.Status expectedStatus) throws Exception {
    MiniDFSCluster dfs = null;
    MiniMRCluster mr = null;
    FileSystem fileSys = null;
    try {
      Configuration conf = new Configuration();
      // Start the mini-MR and mini-DFS clusters
      dfs = new MiniDFSCluster(conf, NUM_HADOOP_SLAVES, true, null);
      fileSys = dfs.getFileSystem();
      mr = new MiniMRCluster(NUM_HADOOP_SLAVES, fileSys.getUri().toString(), 1);

      JobConf jobConf = mr.createJobConf();
      TaskCompletionEvent[] events = runJob(jobConf, mapperClass, enableNoFetchEmptyMapOutputs);
      // Ensure that all mappers set the status as expected
      for (TaskCompletionEvent event: events) {
         if (event.isMapTask()) {
            assertEquals(expectedStatus, event.getTaskStatus());
         }
      }
      
    }
    finally {
      if (dfs != null) { dfs.shutdown(); }
      if (mr != null) { mr.shutdown(); }
    }
  }

  public void testReduceTaskWithoutOutputNoMapOutputProcessingEnabled() throws Exception {
    verifyReduceTaskNoMapOutput(true, SinkMapper.class, TaskCompletionEvent.Status.SUCCEEDED_NO_OUTPUT);
  }

  public void testReduceTaskWithoutOutputNoMapOutputProcessingDisabled() throws Exception {
    verifyReduceTaskNoMapOutput(false, SinkMapper.class, TaskCompletionEvent.Status.SUCCEEDED);
  }

  public void testReduceTaskWithOutputNoMapOutputProcessingEnabled() throws Exception {
    verifyReduceTaskNoMapOutput(true, MapMB.class, TaskCompletionEvent.Status.SUCCEEDED);
  }

  public void testReduceTaskWithOutputNoMapOutputProcessingDisabled() throws Exception {
    verifyReduceTaskNoMapOutput(false, MapMB.class, TaskCompletionEvent.Status.SUCCEEDED);
  }
}   
