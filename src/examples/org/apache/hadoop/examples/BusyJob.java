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
package org.apache.hadoop.examples;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Partitioner;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.lib.NullOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * class for testing CPU CGroup. It creates threads with infinite loop.
 */
public class BusyJob extends SleepJob {  

  private int nThreads = 1;
  
  public class BusyThread extends Thread{
    private boolean running = true;

    @Override
    public void run() {
      int i = 0;
      System.out.println("Starting thread " + getName());
      while (running) {
        i++;
        if (i > 1000000) {
          try {
            Thread.sleep(1000);
          } catch (InterruptedException e) {
          }
          i = 0;
        }
      }
      System.out.println("Ending thread " + getName());
    }

    public void disable(){
      running = false;
    }
  }

  public void map(IntWritable key, IntWritable value,
      OutputCollector<IntWritable, NullWritable> output, Reporter reporter)
      throws IOException {

    BusyThread[] busyThreads = null;
    reporter.setStatus("Creating " + nThreads + " threads");
    System.out.println("Creating " + nThreads + " threads");
    busyThreads = new BusyThread[nThreads];
    for (int i = 0; i < nThreads; i++ ) {
      busyThreads[i] = new BusyThread();
      busyThreads[i].start();
    }
    super.map(key, value, output, reporter);
    try {
      if (busyThreads != null ) {
        for (int i = 0; i < nThreads; i++ ) {
          busyThreads[i].disable();
        }
      }
      if (busyThreads != null ) {
        for (int i = 0; i < nThreads; i++ ) {
          busyThreads[i].join();
        }
      }
    } catch (InterruptedException e) {
    }
  }

  public void reduce(IntWritable key, Iterator<NullWritable> values,
      OutputCollector<NullWritable, NullWritable> output, Reporter reporter)
      throws IOException {
    BusyThread[] busyThreads = null;
    reporter.setStatus("Creating " + nThreads + " threads");
    System.out.println("Creating " + nThreads + " threads");
    busyThreads = new BusyThread[nThreads];
    for (int i = 0; i < nThreads; i++ ) {
      busyThreads[i] = new BusyThread();
      busyThreads[i].start();
    }
    super.reduce(key, values, output, reporter);
    try {
      if (busyThreads != null ) {
        for (int i = 0; i < nThreads; i++ ) {
          busyThreads[i].disable();
        }
      }
      if (busyThreads != null ) {
        for (int i = 0; i < nThreads; i++ ) {
          busyThreads[i].join();
        }
      }
    } catch (InterruptedException e) {
    }
  }

  public void configure(JobConf job) {
    super.configure(job);
    this.nThreads =
      job.getInt("busy.job.no.threads", 1);
    System.out.println("Get nThreads = " + this.nThreads);
  }

  public static void main(String[] args) throws Exception{
    int res = ToolRunner.run(new Configuration(), new BusyJob(), args);
    System.exit(res);
  }

  public int run(int numMapper, int numReducer, long mapSleepTime,
                 long reduceSleepTime, int nThreads) throws IOException {

    JobConf job = setupJobConf(numMapper, numReducer, mapSleepTime, 
                  reduceSleepTime, nThreads);

    rJob = JobClient.runJob(job);
    return 0;
  }

  public JobConf setupJobConf(int numMapper, int numReducer, 
                              long mapSleepTime, long reduceSleepTime,
                              int nThreads) {
    int mapSleepCount = (int)Math.ceil(mapSleepTime / 100);
    int reduceSleepCount = (int)Math.ceil(reduceSleepTime / 100);
    final List<String> EMPTY = Collections.emptyList();
    JobConf job = setupJobConf(
      BusyJob.class,
      numMapper, numReducer,
      mapSleepTime, mapSleepCount,
      reduceSleepTime, reduceSleepCount,
      false, EMPTY, EMPTY, 1, 0,
      new ArrayList<String>(), 1, false, 0);
    System.out.println("nThreads = " + nThreads);
    job.setInt("busy.job.no.threads", nThreads);
    job.setJobName("Busy job");
    return job;
  }

  @Override
  public int run(String[] args) throws Exception {

    if(args.length < 1) {
      System.err.println("BusyJob [-m numMapper] [-r numReducer]" +
          " [-mt mapSleepTime (msec)] [-rt reduceSleepTime (msec)]" +
          " [-thread nThreads]" +
          " ");
      
      ToolRunner.printGenericCommandUsage(System.err);
      return -1;
    }

    int numMapper = 1, numReducer = 1;
    long mapSleepTime = 100, reduceSleepTime = 100;
    int nThreads = 1;
    
    for(int i=0; i < args.length; i++ ) {
      if(args[i].equals("-m")) {
        numMapper = Integer.parseInt(args[++i]);
      }
      else if(args[i].equals("-r")) {
        numReducer = Integer.parseInt(args[++i]);
      }
      else if(args[i].equals("-mt")) {
        mapSleepTime = Long.parseLong(args[++i]);
      }
      else if(args[i].equals("-rt")) {
        reduceSleepTime = Long.parseLong(args[++i]);
      }
      else if (args[i].equals("-thread")) {
        nThreads = Integer.parseInt(args[++i]);
        if (nThreads < 1) {
          System.err.println("Invalid value for " + args[i]);
          System.exit(-1);
        }
      }
      else {
        System.err.println("Invalid option " + args[i]);
        System.exit(-1);
      }
    }
    return run(numMapper, numReducer, mapSleepTime, reduceSleepTime, nThreads);
  }

}
