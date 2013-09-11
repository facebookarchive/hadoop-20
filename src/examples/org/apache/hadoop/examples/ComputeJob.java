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
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
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
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapred.lib.NullOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.util.Shell.ShellCommandExecutor;

public class ComputeJob extends Configured implements Tool,
    Mapper<IntWritable, IntWritable, IntWritable, Text>,
    Reducer<IntWritable, Text, IntWritable, Text> {

  private RunningJob rJob = null;
  private long round;
  private int base;
  private int step;

  public static class EmptySplit implements InputSplit {
    private List<String> hosts = new ArrayList<String>();

    public EmptySplit() {
    }

    public EmptySplit(String host) {
      this.hosts.add(host);
    }

    public EmptySplit(String[] hosts) {
      for (String h : hosts) {
        this.hosts.add(h);
      }
    }

    // No need to write out the hosts as RawSplit handles that for us
    public void write(DataOutput out) throws IOException {
    }

    public void readFields(DataInput in) throws IOException {
    }

    public long getLength() {
      return 0L;
    }

    public String[] getLocations() {
      if (hosts.size() == 0) {
        return new String[0];
      }
      // Hadoop expects a period at the end of the hostnames
      List<String> modifiedHosts = new ArrayList<String>();
      for (String host : this.hosts) {
        modifiedHosts.add(host + ".");
      }

      return modifiedHosts.toArray(new String[0]);
    }
  }

  public static class ComputeInputFormat extends Configured implements
      InputFormat<IntWritable, IntWritable> {
    public InputSplit[] getSplits(JobConf conf, int numSplits) {
      InputSplit[] ret = new InputSplit[numSplits];
      for (int i = 0; i < numSplits; ++i) {
        ret[i] = new EmptySplit(new String[] { "" });
      }
      return ret;
    }

    public RecordReader<IntWritable, IntWritable> getRecordReader(
        InputSplit ignored, JobConf conf, Reporter reporter) throws IOException {
      final int reduce = conf.getNumReduceTasks();
      return new RecordReader<IntWritable, IntWritable>() {
        private boolean first = true;

        public boolean next(IntWritable key, IntWritable value)
            throws IOException {
          if (!first)
            return false;
          first = false;
          key.set(0);
          value.set(reduce);
          return true;
        }

        public IntWritable createKey() {
          return new IntWritable();
        }

        public IntWritable createValue() {
          return new IntWritable();
        }

        public long getPos() throws IOException {
          return (first) ? 0 : 1;
        }

        public void close() throws IOException {
        }

        public float getProgress() throws IOException {
          return (first) ? (float) 0.0 : (float) 1.0;
        }
      };
    }
  }

  static class ComputeRunnable implements Runnable {
    private int round;
    private byte[] data;
    private Random rand;

    public ComputeRunnable(int round) {
      this.round = round;
      this.data = new byte[1024];
      this.rand = new Random();
    }

    public void run() {
      MessageDigest md = null;
      try {
        md = MessageDigest.getInstance("MD5");
      } catch (NoSuchAlgorithmException e) {
        e.printStackTrace();
        return;
      }

      for (int i = 0; i < round; i++) {
        rand.nextBytes(data);
        md.update(data);
      }
    }
  }

  public void map(IntWritable key, IntWritable value,
      OutputCollector<IntWritable, Text> output, Reporter reporter)
      throws IOException {

    int slots = this.base
        * (int) Math.pow(2, (new Random()).nextInt(this.step));
    int each = (int) (this.round / slots);
    Thread[] thread = new Thread[slots];

    long start = System.nanoTime();
    for (int i = 0; i < slots; i++) {
      thread[i] = new Thread(new ComputeRunnable(each));
    }

    for (int i = 0; i < slots; i++) {
      thread[i].start();
    }

    try {
      for (int i = 0; i < slots; i++) {
        thread[i].join();
      }
    } catch (InterruptedException ex) {
      throw (IOException) new IOException("Interrupted while sleeping")
          .initCause(ex);
    }

    long end = System.nanoTime();
    int duration = (int) ((end - start) / 1000000000);

    for (int i = 0; i < value.get(); ++i) {
      output.collect(new IntWritable(slots), new Text(Integer.toString(duration)));
    }
  }

  public void reduce(IntWritable key, Iterator<Text> values,
      OutputCollector<IntWritable, Text> output, Reporter reporter)
      throws IOException {
    int count = 0;
    int sum = 0;
    String list = "";
    while (values.hasNext()) {
      String v = values.next().toString();
      list += v + " ";
      sum += Integer.parseInt(v);
      count++;
    }
    output.collect(key, new Text(String.format("%.2f (%d): %s", (double)sum/count, count, list)));
  }

  public void configure(JobConf job) {
    this.round = job.getLong("compute.job.round", 1024 * 1024 * 2);
    this.base = job.getInt("compute.job.base", 8);
    this.step = job.getInt("compute.job.step", 4);
  }

  public void close() throws IOException {
  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new ComputeJob(), args);
    System.exit(res);
  }

  public int run(int numMapper, long round, int base, int step, String output)
      throws IOException {

    JobConf job = setupJobConf(numMapper, round, base, step, output);

    rJob = JobClient.runJob(job);
    return 0;
  }

  public JobConf setupJobConf(int numMapper, long round, int base, int step, String output) {

    JobConf job = new JobConf(getConf(), ComputeJob.class);
    job.setJobSetupCleanupNeeded(true);
    job.setNumMapTasks(numMapper);
    job.setNumReduceTasks(1);
    job.setMapperClass(ComputeJob.class);
    job.setMapOutputKeyClass(IntWritable.class);
    job.setMapOutputValueClass(Text.class);
    job.setReducerClass(ComputeJob.class);
    job.setOutputFormat(TextOutputFormat.class);
    job.setInputFormat(ComputeInputFormat.class);
    job.setJobName("Compute job");
    FileInputFormat.addInputPath(job, new Path("ignored"));
    Path outputPath = new Path(output);
    try {
      FileSystem dfs = outputPath.getFileSystem(job);
      if(dfs.exists(outputPath))
        dfs.delete(outputPath, true);
    } catch (IOException e) {
      e.printStackTrace();
    }
    FileOutputFormat.setOutputPath(job, outputPath);
    job.setLong("compute.job.round", round);
    job.setLong("compute.job.base", base);
    job.setLong("compute.job.step", step);
    job.setSpeculativeExecution(false);
    return job;
  }

  public int run(String[] args) throws Exception {

    if (args.length < 0) {
      System.err.println("ComputeJob [-m numMapper]" + " [-rd round]"
          + " [-b base]" + " [-s step]" + " [-out output]");

      ToolRunner.printGenericCommandUsage(System.err);
      return -1;
    }

    int numMapper = 1;
    long round = 1024*1024*2;
    int base = 8, step = 4;
    String output = "compute_output";

    for (int i = 0; i < args.length; i++) {
      if (args[i].equals("-m")) {
        numMapper = Integer.parseInt(args[++i]);
      } else if (args[i].equals("-rd")) {
        round = Long.parseLong(args[++i]);
      } else if (args[i].equals("-b")) {
        base = Integer.parseInt(args[++i]);
      } else if (args[i].equals("-s")) {
        step = Integer.parseInt(args[++i]);
      } else {
        System.err.println("Invalid option " + args[i]);
        System.exit(-1);
      }
    }

    return run(numMapper, round, base, step, output);
  }

  public RunningJob getRunningJob() {
    return rJob;
  }
}
