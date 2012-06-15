package org.apache.hadoop.mapred;
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

import org.apache.hadoop.hdfs.DirConstant;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

@SuppressWarnings("deprecation")
  public class DirMapper extends MapReduceBase implements
      Mapper<Text, LongWritable, Text, Text>, DirConstant{

    private Configuration conf;

    public void configure(JobConf configuration) {
      conf = configuration;
    }
    
    private class DirThread extends Thread {
      private int id;
      private long ntasks;
      private String name;
      private boolean samedir;
      private FileSystem fs;
      
      public DirThread(Configuration conf, String name, int id,
          long ntasks, boolean samedir) throws IOException{
        this.id = id;
        this.name = name;
        this.ntasks = ntasks;
        this.samedir = samedir;
        this.fs = FileSystem.newInstance(conf);
      }
      
      public void run() {
        try {
          Path basePath = new Path(INPUT);
          if (samedir == false) {
            basePath = new Path(INPUT, name + "/" + id);
          }
          
          for (int i = 0; i < ntasks; i++) {
            Path tmpDir = new Path(basePath, name + "_" + id + "_" + i + "_dir");
            Path tmpDir1 = new Path(basePath, name + "_" + id + "_" + i + "_dir1");
            fs.mkdirs(tmpDir);
            fs.mkdirs(tmpDir1);
            fs.delete(tmpDir, true);
            fs.delete(tmpDir1, true);
          }
        } catch (Exception ioe) {
          System.err.println(ioe.getLocalizedMessage());
          ioe.printStackTrace();
        }
      }

    }

    @Override
    public void map(Text key, LongWritable value,
        OutputCollector<Text, Text> output, Reporter reporter)
        throws IOException{
      // for testing
      String taskID = conf.get("mapred.task.id");
      String name = key.toString() + taskID;

      long ntasks = Long.parseLong(conf.get("dfs.nTasks"));
      boolean samedir = Boolean.parseBoolean(conf.get("dfs.samedir", Boolean.toString(false)));
      long nthreads = value.get();
      // spawn ntasks threads 
      DirThread[] threads = new DirThread[(int)nthreads];
      for (int i=0; i<nthreads; i++) {
        threads[i] = new DirThread(conf, name, i, ntasks, samedir);
        threads[i].start();
      }
      for (DirThread thread : threads) {
        try {
          thread.join();
        } catch (InterruptedException ex) {
        }
      }
      output.collect(
          new Text("1"),
          new Text(String.valueOf(1)));
    }

  }
