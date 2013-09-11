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
 * Dummy class for testing MR framefork. Sleeps for a defined period 
 * of time in mapper and reducer. Generates fake input for map / reduce 
 * jobs. Note that generated number of input pairs is in the order 
 * of <code>numMappers * mapSleepTime / 100</code>, so the job uses
 * some disk space.
 */
public class SleepJob extends Configured implements Tool,  
             Mapper<IntWritable, IntWritable, IntWritable, NullWritable>,
             Reducer<IntWritable, NullWritable, NullWritable, NullWritable>,
             Partitioner<IntWritable,NullWritable> {

  private static final String SLOW_RATIO = "sleep.job.slow.ratio";
  private static final String SLOW_MAPS = "sleep.job.slow.maps";
  private static final String SLOW_REDUCES = "sleep.job.slow.reduces";
  private static final String HOSTS_FOR_LOCALITY = "sleep.job.hosts";
  private static final String HOSTS_PER_SPLIT = "sleep.job.hosts.per.split";
  
  private long mapSleepDuration = 100;
  private long reduceSleepDuration = 100;
  private int mapSleepCount = 1;
  private int reduceSleepCount = 1;
  private int count = 0;
  private int countersPerTask = 0;
  protected RunningJob rJob = null;

  private static Random generator = new Random();
  
  public int getPartition(IntWritable k, NullWritable v, int numPartitions) {
    return k.get() % numPartitions;
  }
  
  public static class EmptySplit implements InputSplit {
    private List<String> hosts = new ArrayList<String>();
    public EmptySplit() { }
    public EmptySplit(String host) {
      this.hosts.add(host);
    }
    public EmptySplit(String [] hosts) {
      for (String h : hosts) {
        this.hosts.add(h); 
      }
    }
    // No need to write out the hosts as RawSplit handles that for us
    public void write(DataOutput out) throws IOException { }
    public void readFields(DataInput in) throws IOException { }
    public long getLength() { return 0L; }
    public String[] getLocations() {
      if (hosts.size() == 0) {
        return new String [0];
      }
      // Hadoop expects a period at the end of the hostnames
      List<String> modifiedHosts = new ArrayList<String>();
      for (String host : this.hosts) {
        modifiedHosts.add(host + ".");
      }
      
      return modifiedHosts.toArray(new String[0]); 
    }
  }

  public static class SleepInputFormat extends Configured
      implements InputFormat<IntWritable,IntWritable> {
    public InputSplit[] getSplits(JobConf conf, int numSplits) {
      InputSplit[] ret = new InputSplit[numSplits];
      String hostsStr = conf.get(HOSTS_FOR_LOCALITY, "");
      int hostsPerSplit = conf.getInt(HOSTS_PER_SPLIT, 0);
      // If hostsStr is empty, hosts will be [""]
      String [] hosts = hostsStr.split(",");
      
      // Distribute the hosts randomly to the splits
      for (int i = 0; i < numSplits; ++i) {
        Set<String> hostsForSplit = new HashSet<String>();
        for (int j = 0; j < hostsPerSplit; j++) {
          int index = generator.nextInt(hosts.length);
          hostsForSplit.add(hosts[index]);
        }
        ret[i] = new EmptySplit(hostsForSplit.toArray(new String[0]));
      }
      return ret;
    }
    public RecordReader<IntWritable,IntWritable> getRecordReader(
        InputSplit ignored, JobConf conf, Reporter reporter)
        throws IOException {
      final int count = conf.getInt("sleep.job.map.sleep.count", 1);
      if (count < 0) throw new IOException("Invalid map count: " + count);
      final int redcount = conf.getInt("sleep.job.reduce.sleep.count", 1);
      if (redcount < 0)
        throw new IOException("Invalid reduce count: " + redcount);
      final int emitPerMapTask = (redcount * conf.getNumReduceTasks());
    return new RecordReader<IntWritable,IntWritable>() {
        private int records = 0;
        private int emitCount = 0;
        
        public boolean next(IntWritable key, IntWritable value)
            throws IOException {
          key.set(emitCount);
          int emit = emitPerMapTask / count;
          if ((emitPerMapTask) % count > records) {
            ++emit;
          }
          emitCount += emit;
          value.set(emit);
          return records++ < count;
        }
        public IntWritable createKey() { return new IntWritable(); }
        public IntWritable createValue() { return new IntWritable(); }
        public long getPos() throws IOException { return records; }
        public void close() throws IOException { }
        public float getProgress() throws IOException {
          return records / ((float)count);
        }
      };
    }
  }

  private List<String> counterNames = null;
  private List<String> getCounterNames() {
    if (counterNames != null) {
      return counterNames;
    }
    counterNames = new ArrayList<String>();
    for(int i=0; i<this.countersPerTask; i++) {
      String counterName = "C" + i;
      counterNames.add(counterName);
    }
    return counterNames;
  }

  public void map(IntWritable key, IntWritable value,
      OutputCollector<IntWritable, NullWritable> output, Reporter reporter)
      throws IOException {

    List<String> counterNames = getCounterNames();
    for (String counterName : counterNames) {
      reporter.incrCounter("Counters from Mappers", counterName, 1);
    }
    //it is expected that every map processes mapSleepCount number of records. 
    try {
      reporter.setStatus("Sleeping... (" +
          (mapSleepDuration * (mapSleepCount - count)) + ") ms left");
      Thread.sleep(mapSleepDuration);
    }
    catch (InterruptedException ex) {
      throw (IOException)new IOException(
          "Interrupted while sleeping").initCause(ex);
    }
    ++count;
    // output reduceSleepCount * numReduce number of random values, so that
    // each reducer will get reduceSleepCount number of keys.
    int k = key.get();
    for (int i = 0; i < value.get(); ++i) {
      output.collect(new IntWritable(k + i), NullWritable.get());
    }
  }

  public void reduce(IntWritable key, Iterator<NullWritable> values,
      OutputCollector<NullWritable, NullWritable> output, Reporter reporter)
      throws IOException {
    List<String> counterNames = getCounterNames();
    for (String counterName : counterNames) {
      reporter.incrCounter("Counters from Reducers", counterName, 1);
    }
    try {
      reporter.setStatus("Sleeping... (" +
          (reduceSleepDuration * (reduceSleepCount - count)) + ") ms left");
        Thread.sleep(reduceSleepDuration);
      
    }
    catch (InterruptedException ex) {
      throw (IOException)new IOException(
          "Interrupted while sleeping").initCause(ex);
    }
    count++;
  }

  public void configure(JobConf job) {
    this.mapSleepCount =
      job.getInt("sleep.job.map.sleep.count", mapSleepCount);
    this.reduceSleepCount =
      job.getInt("sleep.job.reduce.sleep.count", reduceSleepCount);
    this.mapSleepDuration =
      job.getLong("sleep.job.map.sleep.time" , 100) / mapSleepCount;
    this.reduceSleepDuration =
      job.getLong("sleep.job.reduce.sleep.time" , 100) / reduceSleepCount;
    this.countersPerTask = 
      job.getInt("sleep.job.counters.per.task", 0);
    makeSomeTasksSlower(job);
  }

  private void makeSomeTasksSlower(JobConf job) {
    int id = getTaskId(job);
    int slowRatio = job.getInt(SLOW_RATIO, 1);
    if (isMap(job)) {
      String slowMaps[] = job.getStrings(SLOW_MAPS);
      if (slowMaps != null) {
        for (String s : job.getStrings(SLOW_MAPS)) {
          if (id == Integer.parseInt(s)) {
            System.out.println("Map task:" + id + " is slowed." +
                " slowRatio:" + slowRatio);
            this.mapSleepDuration *= slowRatio;
            return;
          }
        }
      }
    } else {
      String slowReduces[] = job.getStrings(SLOW_REDUCES);
      if (slowReduces != null) {
        for (String s : job.getStrings(SLOW_REDUCES)) {
          if (id == Integer.parseInt(s)) {
            System.out.println("Reduce task:" + id + " is slowed" +
                " slowRatio:" + slowRatio);
            this.reduceSleepDuration *= slowRatio;
            return;
          }
        }
      }
    }
  }

  private boolean isMap(JobConf job) {
    String taskid = job.get("mapred.task.id");
    return taskid.split("_")[3].equals("m");
  }
 
  private int getTaskId(JobConf job) {
    String taskid = job.get("mapred.task.id");
    return Integer.parseInt(taskid.split("_")[4]);
  }

  public void close() throws IOException {
  }

  public static void main(String[] args) throws Exception{
    int res = ToolRunner.run(new Configuration(), new SleepJob(), args);
    System.exit(res);
  }

  public int run(int numMapper, int numReducer, long mapSleepTime,
      int mapSleepCount, long reduceSleepTime,
      int reduceSleepCount, boolean doSpeculation,
      List<String> slowMaps, List<String> slowReduces,
      int slowRatio, int countersPerTask, List<String> hosts, int hostsPerSplit,
      boolean setup, int sortMemory) 
          throws IOException {

    JobConf job = setupJobConf(numMapper, numReducer, mapSleepTime, 
                  mapSleepCount, reduceSleepTime, reduceSleepCount,
                  doSpeculation, slowMaps, slowReduces, slowRatio,
                  countersPerTask, hosts, hostsPerSplit, setup,
                  sortMemory);


    rJob = JobClient.runJob(job);
    return 0;
  }

  public JobConf setupJobConf(int numMapper, int numReducer, 
                                long mapSleepTime, int mapSleepCount, 
                                long reduceSleepTime, int reduceSleepCount) {
    final List<String> EMPTY = Collections.emptyList();
    return setupJobConf(numMapper, numReducer, mapSleepTime, mapSleepCount,

        reduceSleepTime, reduceSleepCount, false, EMPTY, EMPTY, 1, 0,
        new ArrayList<String>(), 1, false, 0);
  }

  public JobConf setupJobConf( int numMapper, int numReducer, 
                                long mapSleepTime, int mapSleepCount, 
                                long reduceSleepTime, int reduceSleepCount,
                                boolean doSpeculation, List<String> slowMaps,
                                List<String> slowReduces, int slowRatio,
                                int countersPerTask, List<String> hosts,
                                int hostsPerSplit, boolean setup,
                                int sortMemory) {
    
    return setupJobConf(SleepJob.class,
                        numMapper, numReducer,
                        mapSleepTime, mapSleepCount,
                        reduceSleepTime, reduceSleepCount,
                        doSpeculation, slowMaps,
                        slowReduces, slowRatio,
                        countersPerTask, hosts,
                        hostsPerSplit, setup, sortMemory);
  }
  @SuppressWarnings({ "deprecation", "unchecked" })
  public JobConf setupJobConf(Class classToSet,
                                int numMapper, int numReducer, 
                                long mapSleepTime, int mapSleepCount, 
                                long reduceSleepTime, int reduceSleepCount,
                                boolean doSpeculation, List<String> slowMaps,
                                List<String> slowReduces, int slowRatio,
                                int countersPerTask, List<String> hosts,
                                int hostsPerSplit, boolean setup,
                                int sortMemory) {
    
    JobConf job = new JobConf(getConf(), classToSet);
    job.setNumMapTasks(numMapper);
    job.setNumReduceTasks(numReducer);
    job.setMapperClass(classToSet);
    job.setMapOutputKeyClass(IntWritable.class);
    job.setMapOutputValueClass(NullWritable.class);
    job.setReducerClass(classToSet);
    job.setOutputFormat(NullOutputFormat.class);
    job.setJobSetupCleanupNeeded(setup);
    job.setInputFormat(SleepInputFormat.class);
    job.setPartitionerClass(SleepJob.class);
    job.setJobName("Sleep job");
    FileInputFormat.addInputPath(job, new Path("ignored"));
    job.setLong("sleep.job.map.sleep.time", mapSleepTime);
    job.setLong("sleep.job.reduce.sleep.time", reduceSleepTime);
    job.setInt("sleep.job.map.sleep.count", mapSleepCount);
    job.setInt("sleep.job.reduce.sleep.count", reduceSleepCount);
    if (sortMemory != 0) { 
      job.setInt("io.sort.mb", sortMemory);
    }
    job.setSpeculativeExecution(doSpeculation);
    job.setInt(SLOW_RATIO, slowRatio);
    job.setStrings(SLOW_MAPS, slowMaps.toArray(new String[slowMaps.size()]));
    job.setStrings(SLOW_REDUCES, slowMaps.toArray(new String[slowReduces.size()]));
    job.setInt("sleep.job.counters.per.task", countersPerTask);
    job.setStrings(HOSTS_FOR_LOCALITY, hosts.toArray(new String[hosts.size()]));
    job.setInt(HOSTS_PER_SPLIT, hostsPerSplit);
    return job;
  }

  public int run(String[] args) throws Exception {

    if(args.length < 1) {
      System.err.println("SleepJob [-m numMapper] [-r numReducer]" +
          " [-mt mapSleepTime (msec)] [-rt reduceSleepTime (msec)]" +
          " [-memory sortMemory(m)]" +
          " [-recordt recordSleepTime (msec)]" +
          " [-slowmaps slowMaps (int separated by ,)]" +
          " [-slowreduces slowReduces (int separated by ,)]" +
          " [-slowratio slowRatio]" + 
          " [-counters numCountersToIncPerRecordPerTask]" +
          " [-nosetup]" +
          " [-hosts hostsToRunMaps (for testing locality. host names" + 
          " separated by ,)]" +
          " [-hostspersplit numHostsPerSplit (for testing locality. number" +
          " of random hosts per split " +
          " ");
      
      ToolRunner.printGenericCommandUsage(System.err);
      return -1;
    }

    int numMapper = 1, numReducer = 1;
    long mapSleepTime = 100, reduceSleepTime = 100, recSleepTime = 100;
    int mapSleepCount = 1, reduceSleepCount = 1;
    int hostsPerSplit = 0;
    List<String> slowMaps = Collections.emptyList();
    List<String> slowReduces = Collections.emptyList();
    int slowRatio = 10;
    boolean setup = true;
    boolean doSpeculation = false;
    List<String> hosts = new ArrayList<String>();
    int countersPerTask = 0;
    int sortMemory = 0;
    
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
      else if (args[i].equals("-memory")) {
        sortMemory = Integer.parseInt(args[++i]);
      }
      else if (args[i].equals("-recordt")) {
        recSleepTime = Long.parseLong(args[++i]);
      }
      else if (args[i].equals("-slowmaps")) {
        doSpeculation = true;
        slowMaps = parseSlowTaskList(args[++i]);
      }
      else if (args[i].equals("-slowreduces")) {
        doSpeculation = true;
        slowReduces = parseSlowTaskList(args[++i]);
      }
      else if (args[i].equals("-slowratio")) {
        doSpeculation = true;
        slowRatio = Integer.parseInt(args[++i]);
      }
      else if (args[i].equals("-hosts")) {
        for (String host : args[++i].split(",")) {
          hosts.add(host);
        }
      }
      else if (args[i].equals("-speculation")) {
        doSpeculation = true;
      }
      else if (args[i].equals("-counters")) {
        // Number of counters to increment per record per task
        countersPerTask = Integer.parseInt(args[++i]);
      }
      else if (args[i].equals("-hostspersplit")) {
        hostsPerSplit = Integer.parseInt(args[++i]);
      } 
      else if (args[i].equals("-nosetup")) {
        setup = false;
      }
      else {
        System.err.println("Invalid option " + args[i]);
        System.exit(-1);
      }
    }
    
    // sleep for *SleepTime duration in Task by recSleepTime per record
    mapSleepCount = (int)Math.ceil(mapSleepTime / ((double)recSleepTime));
    reduceSleepCount = (int)Math.ceil(reduceSleepTime / ((double)recSleepTime));
    
    return run(numMapper, numReducer, mapSleepTime, mapSleepCount,
        reduceSleepTime, reduceSleepCount,
        doSpeculation, slowMaps, slowReduces, slowRatio, countersPerTask, 
        hosts, hostsPerSplit, setup, sortMemory);
  }

  private List<String> parseSlowTaskList(String input) {
    String tasks[] = input.split(",");
    List<String> slowTasks = new ArrayList<String>();
    for (String task : tasks) {
      int id = Integer.parseInt(task);
      slowTasks.add(id + "");
    }
    return slowTasks;
  }

  public RunningJob getRunningJob() {
    return rJob;
  }
}
