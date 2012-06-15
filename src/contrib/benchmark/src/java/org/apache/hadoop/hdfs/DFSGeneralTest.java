package org.apache.hadoop.hdfs;
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

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Date;
import java.util.Random;

import org.apache.hadoop.mapred.GenMapper;
import org.apache.hadoop.mapred.GenReduce;
import org.apache.hadoop.mapred.GenThread;
import org.apache.hadoop.mapred.GenReaderThread;
import org.apache.hadoop.mapred.GenWriterThread;
import org.apache.hadoop.mapred.DatanodeBenThread;
import org.apache.hadoop.mapred.DatanodeBenThread.RUNNING_TYPE;
import org.apache.hadoop.mapred.DatanodeBenThread.DatanodeBenRunTimeConstants;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import java.text.SimpleDateFormat;
import java.text.DateFormat;

@SuppressWarnings("deprecation")
public class DFSGeneralTest extends Configured implements Tool, GeneralConstant{

  private static Configuration fsConfig;
  private long nmaps;
  private long nthreads;
  private int buffersize = GenThread.DEFAULT_BUFFER_SIZE;
  private long datarate = GenThread.DEFAULT_DATA_RATE;
  static final String[] testtypes = {GenWriterThread.TEST_TYPE, 
                                     DatanodeBenThread.TEST_TYPE};
  private static String testtype = null;
  private final static String DEFAULT_USAGE = 
      "USAGE: bin/hadoop hadoop-*-benchmark.jar " + 
      "gentest %s [-nMaps] [-nThreads] [-buffersize] [-workdir] " +
      "[-writerate] [-cleanup] %s\n";
  private String dfs_output = null;
  private String dfs_input = null;
  private String input = null;
  private String output = null;
  private String workdir = null;
  private boolean cleanup = false;
  private Random rb = new Random();
  private static final DateFormat dateFormat =
      new SimpleDateFormat("yyyy-MM-dd-HH-mm-ss-SSS");

  private String uniqueId = (dateFormat.format(new Date())) + "." 
      + rb.nextInt();

  public static void printUsage() {
    System.err.printf(DEFAULT_USAGE, "testtype", "<args...>"); 
    System.err.print("    testtype could be ");
    for (String type: testtypes) {
      System.err.print("\"" + type + "\" ");
    }
    System.err.println();
    System.err.println("    -nMaps [number of machines] Default value = " + NMAPS);
    System.err.println("    -nThreads [number of threads in one machine] Default "
        + "value = " + NTHREADS);
    System.err.println("    -buffersize [X KB buffer] default value = " +
          GenThread.DEFAULT_BUFFER_SIZE);
    System.err.println("    -workdir [working directory] default value = " + 
        INPUT + "[testtype]");
    System.err.println("    -writerate [X KB data allowed to write per " +
        "second] default value = " + GenThread.DEFAULT_DATA_RATE);
    System.err.println("    -cleanup :delete all temp data when test is done.");
    System.err.println();
    for (String type : testtypes) {
      System.err.println("Test " + type + ":");
      printUsage(type, false);
    }
    System.exit(1);
  }
  
  public static void printUsage(String testtype, boolean exitAfterPrint) {
    if (testtype.equals(GenWriterThread.TEST_TYPE)) {
      System.err.printf(DEFAULT_USAGE, testtype, "[-sync] [-roll] " 
                        + "[-maxtime] ");
      System.err.println("    -sync [(sec) sync file once/Xsec] <=0 " +
          "means no sync default value = " + 
          GenWriterThread.DEFAULT_SYNC_INTERVAL_SEC);
      System.err.println("    -roll [(sec) roll file once/Xsec] <=0 " +
          "means no roll, default value = " +
          GenWriterThread.DEFAULT_ROLL_INTERVAL_SEC);
      System.err.println("    -maxtime [(sec) max running time] default " +
          "value = " + GenWriterThread.DEFAULT_MAX_TIME_SEC);
      System.err.println();
    } else if (testtype.equals(DatanodeBenThread.TEST_TYPE)) {
      System.err.printf(DEFAULT_USAGE, testtype, "{[-prepare]} {[-maxtime] " +
          "[-filesize] [-dn] [-pread] [-minfile] [-rep]}");
      System.err.println("    -prepare [generate at least X files per " +
          "datanode in each namespace] default value = " + 
          DatanodeBenThread.DEFAULT_MIN_NUMBER_OF_FILES_PER_DATANODE + 
          " Need to run prepare first before running benchmark");
      System.err.println("    -maxtime [(sec) max running time] default " +
          "value = " + DatanodeBenThread.DEFAULT_MAX_TIME_SEC);
      System.err.println("    -filesize [X MB per file] default value = " +
          DatanodeBenThread.DEFAULT_FILE_SIZE);
      System.err.println("    -dn [Stress test X datanodes] " + 
          "default value = " + DatanodeBenThread.DEFAULT_DATANODE_NUMBER);
      System.err.println("    -pread [read percent: X read and (1-X) write, " +
          "0<=X<=1] default value = " + DatanodeBenThread.DEFAULT_READ_PERCENT);
      System.err.println("    -minfile [choose datanodes with at least X files]" +
          " default value = " + 
          DatanodeBenThread.DEFAULT_MIN_NUMBER_OF_FILES_PER_DATANODE);
      System.err.println("    -rep [X replicas per file] default value = " + 
          DatanodeBenThread.DEFAULT_REPLICATION_NUM);
      System.err.println();
    }
    if (exitAfterPrint) {
      System.exit(1);
    }
  }

  public void control(JobConf fsConfig, String fileName)
      throws IOException {
    String name = fileName;
    FileSystem fs = FileSystem.get(fsConfig);

    SequenceFile.Writer write = null;
    for (int i = 0; i < nmaps; i++) {
      try {
        Path controlFile = new Path(dfs_input, name + i);
        write = SequenceFile.createWriter(fs, fsConfig, controlFile,
            Text.class, Text.class, CompressionType.NONE);
        write.append(new Text(name + i), new Text(workdir));
      } finally {
        if (write != null)
          write.close();
        write = null;
      }
    }
  }
  
  /**
   * Initialize general config 
   */
  private String[] initializeGeneralConf(String[] args, JobConf conf)
      throws IOException {
    nmaps = NMAPS;
    nthreads = NTHREADS;
    buffersize = GenThread.DEFAULT_BUFFER_SIZE;
    datarate = GenThread.DEFAULT_DATA_RATE;
    ArrayList<String> newArgs = new ArrayList<String>();
    for (int i = 1; i < args.length; i++) {
      if (args[i].equals("-nThreads")) nthreads = Long.parseLong(args[++i]);
      else if (args[i].equals("-nMaps")) nmaps = Long.parseLong(args[++i]);
      else if (args[i].equals("-buffersize")) buffersize = Integer.parseInt(args[++i]);
      else if (args[i].equals("-workdir")) workdir = args[++i];
      else if (args[i].equals("-writerate")) datarate = Long.parseLong(args[++i]);
      else if (args[i].equals("-cleanup")) cleanup = true;
      else {
        newArgs.add(args[i]);
      }
    }
    return newArgs.toArray(new String[newArgs.size()]);
  }
  
  /**
   * Generate control files for write test and initialize configure file
   * and return the job config 
   * @param args
   * @return
   */
  private void initializeGenWriterJob(String[] args, JobConf conf) throws IOException{
    long sync = GenWriterThread.DEFAULT_SYNC_INTERVAL_SEC;
    long roll = GenWriterThread.DEFAULT_ROLL_INTERVAL_SEC;
    long maxtime = GenWriterThread.DEFAULT_MAX_TIME_SEC;
    
    for (int i = 0; i < args.length; i++) {
      if (args[i].equals("-sync")) sync = Long.parseLong(args[++i]);
      else if (args[i].equals("-roll")) roll = Long.parseLong(args[++i]);
      else if (args[i].equals("-maxtime")) maxtime = Long.parseLong(args[++i]);
      else {
        printUsage(testtype, true);
      }
    }
    // run the control() to set up for the FileSystem
    control(conf, "testing-" + testtype);
    conf.setLong(GenWriterThread.WRITER_ROLL_INTERVAL_KEY, roll);
    conf.setLong(GenWriterThread.WRITER_SYNC_INTERVAL_KEY, sync);
    conf.setLong(GenWriterThread.MAX_TIME_SEC_KEY, maxtime);
    conf.set(THREAD_CLASS_KEY, "org.apache.hadoop.mapred.GenWriterThread");
    conf.setMapperClass(GenMapper.class);
    conf.setReducerClass(GenReduce.class);
  }
  
  private void initializeDatanodeBenJob(String[] args, JobConf conf) 
      throws IOException {
    if (args[0].equals("-prepare")) {
      if (args.length < 2) {
        printUsage(testtype, true);
      }
      long minFile = DatanodeBenThread.DEFAULT_MIN_NUMBER_OF_FILES_PER_DATANODE;
      minFile = Long.parseLong(args[1]);
      conf.setLong(DatanodeBenThread.MIN_FILE_PER_DATANODE_KEY, minFile);
      conf.setInt(DatanodeBenThread.RUNNING_TYPE_KEY, RUNNING_TYPE.PREPARE.ordinal());
      conf.setLong(DatanodeBenThread.MAX_TIME_SEC_KEY, 3600L);
      conf.setLong(DatanodeBenThread.FILE_SIZE_KEY, 256L);
      conf.setLong(DatanodeBenThread.REPLICATION_KEY, 1L);
      control(conf, "testing-prepare-" + testtype);
    } else {
      long maxtime = DatanodeBenThread.DEFAULT_MAX_TIME_SEC;
      long filesize = DatanodeBenThread.DEFAULT_FILE_SIZE;
      long nDatanode = DatanodeBenThread.DEFAULT_DATANODE_NUMBER;
      float pread = DatanodeBenThread.DEFAULT_READ_PERCENT;
      long minFile = DatanodeBenThread.DEFAULT_MIN_NUMBER_OF_FILES_PER_DATANODE;
      short rep = DatanodeBenThread.DEFAULT_REPLICATION_NUM;
      
      for (int i = 0; i < args.length; i++) {
        if (args[i].equals("-maxtime")) maxtime = Long.parseLong(args[++i]);
        else if (args[i].equals("-filesize")) filesize = 
            Long.parseLong(args[++i]);
        else if (args[i].equals("-dn")) nDatanode = 
            Long.parseLong(args[++i]);
        else if (args[i].equals("-pread")) pread = Float.parseFloat(args[++i]);
        else if (args[i].equals("-minfile")) minFile = Long.parseLong(args[++i]);
        else if (args[i].equals("-rep")) rep = Short.parseShort(args[++i]);
        else {
          printUsage(testtype, true);
        }
      }
      if (pread + 1e-9 < 0.0 || pread - 1e-9 > 1.0) {
        printUsage(testtype, true);
      }
      conf.setLong(DatanodeBenThread.MAX_TIME_SEC_KEY, maxtime);
      conf.setLong(DatanodeBenThread.FILE_SIZE_KEY, filesize);
      conf.setFloat(DatanodeBenThread.READ_PERCENT_KEY, pread);
      conf.setLong(DatanodeBenThread.REPLICATION_KEY, rep);
      List<JobConf> nameNodeConfs = DatanodeBenThread.getNameNodeConfs(conf);
      DatanodeBenThread dbt = new DatanodeBenThread(conf);
      List<DatanodeInfo> victims = dbt.getTestDatanodes(nameNodeConfs,
          workdir, nDatanode, minFile);
      System.out.print("We choose " + victims.size() + " victim datanodes: ");
      String victimStr = "";
      int i = 0;
      for (DatanodeInfo victim: victims) {
        victimStr += victim.getHostName() + ":" + victim.getPort();
        i++;
        if (i < victims.size()) {
          victimStr += ",";
        }
      }
      System.out.println(victimStr);
      conf.set(DatanodeBenThread.VICTIM_DATANODE_KEY, victimStr);
      control(conf, "testing-" + testtype);
    }
    conf.set(THREAD_CLASS_KEY, "org.apache.hadoop.mapred.DatanodeBenThread");
    conf.setMapperClass(GenMapper.class);
    conf.setReducerClass(GenReduce.class);
  }
  
  /*
   * Spawn a map-reduce jobs based on the control files 
   * generated by the writers. 
   */
  private void verifyFiles(FileSystem fs) 
      throws IOException {
    Path inputPath = new Path(input, "filelists");
    Path outputPath = new Path(dfs_output, "verify_results");
    if (!fs.exists(inputPath)) {
      System.out.println("Couldn't find " + inputPath + " Skip verification.");
      return;
    }
    System.out.println("-------------------");
    System.out.println("VERIFY FILES");
    System.out.println("-------------------");
    JobConf conf = new JobConf(fsConfig, DFSGeneralTest.class);
    conf.set(THREAD_CLASS_KEY, "org.apache.hadoop.mapred.GenReaderThread");
    testtype = GenReaderThread.TEST_TYPE;
    conf.set(TEST_TYPE_KEY, testtype);
    conf.setMapperClass(GenMapper.class);
    conf.setReducerClass(GenReduce.class);
    conf.setJobName(getUniqueName("gentest-verify-" + testtype));
    output = getUniqueName(OUTPUT + testtype);
    updateJobConf(conf, inputPath, outputPath);
    long startTime = System.currentTimeMillis();
    JobClient.runJob(conf);
    long endTime = System.currentTimeMillis();
    printResult(fs, new Path(output, "results"), startTime, endTime);
  }
  
  private void printResult(FileSystem fs, Path p, 
      long startTime, long endTime) throws IOException{
    // printout the result
    System.out.println("-------------------");
    System.out.println("RESULT");
    System.out.println("-------------------");
    FSDataInputStream out = null;
    try {
      out = fs.open(p);
      while (true) {
        String temp = out.readLine();
        if (temp == null)
          break;
        System.out.println(temp);
      }
    } finally {
      if (out != null)
        out.close();
    }


    System.out.println("------------------");
    double execTime = (endTime - startTime) / 1000.0;
    String unit = "seconds";
    if (execTime > 60) {
      execTime /= 60.0;
      unit = "mins";
    }
    if (execTime > 60) {
      execTime /= 60.0;
      unit = "hours";
    }
    System.out.println("Time executed :\t" + execTime + " " + unit);
  }
  
  private void updateJobConf(JobConf conf, Path inputPath, Path outputPath) {
    // set specific job config
    conf.setLong(NUMBER_OF_MAPS_KEY, nmaps);
    conf.setLong(NUMBER_OF_THREADS_KEY, nthreads);
    conf.setInt(BUFFER_SIZE_KEY, buffersize);
    conf.setLong(WRITER_DATARATE_KEY, datarate);
    conf.setLong("mapred.task.timeout", Long.MAX_VALUE);
    conf.set(OUTPUT_DIR_KEY, output);
    
    // set the output and input for the map reduce
    FileInputFormat.setInputPaths(conf, inputPath);
    FileOutputFormat.setOutputPath(conf, outputPath);

    conf.setInputFormat(SequenceFileInputFormat.class);
    conf.setOutputKeyClass(Text.class);
    conf.setOutputValueClass(Text.class);
    conf.setNumReduceTasks(1);
    conf.setSpeculativeExecution(false);
  }
  
  // Clean up all directories in all namespaces
  private void cleanUpDirs(Configuration conf) throws IOException {
    List<InetSocketAddress> nameNodeAddrs = 
        DFSUtil.getClientRpcAddresses(conf, null);
    for (InetSocketAddress nnAddr : nameNodeAddrs) {
      Configuration newConf = new Configuration(conf);
      newConf.set(NameNode.DFS_NAMENODE_RPC_ADDRESS_KEY,
          nnAddr.getHostName() + ":" + nnAddr.getPort());
      NameNode.setupDefaultURI(newConf);
      FileSystem fs = FileSystem.get(newConf);
      if (fs.exists(new Path(dfs_output)))
        fs.delete(new Path(dfs_output), true);
      if (fs.exists(new Path(dfs_input)))
        fs.delete(new Path(dfs_input), true);
      if (fs.exists(new Path(input)))
        fs.delete(new Path(input), true);
      if (fs.exists(new Path(output)))
        fs.delete(new Path(output), true);
    }
  }
  
  private String getUniqueName(String prefix) {
    return prefix + "-" + uniqueId;
  }

  @Override
  public int run(String[] args) throws IOException {

    if (args.length < 1) {
      printUsage();
    }
    testtype = args[0];
    if (!Arrays.asList(testtypes).contains(testtype)) {
      System.err.println(testtype + " is not a supported test type");
      printUsage();
    }
    // running the Writting
    fsConfig = new Configuration(getConf());

    dfs_output = getUniqueName(DFS_OUTPUT + testtype); 
    dfs_input = getUniqueName(DFS_INPUT + testtype);
    input = getUniqueName(INPUT + testtype);
    output = getUniqueName(OUTPUT + testtype);
    workdir = input;
    cleanUpDirs(fsConfig);
    
    FileSystem fs = FileSystem.get(fsConfig);
    JobConf conf = new JobConf(fsConfig, DFSGeneralTest.class);
    conf.setJobName(getUniqueName("gentest-" + testtype));
    conf.set(TEST_TYPE_KEY, testtype);
    
    String[] newArgs = initializeGeneralConf(args, conf);
    if (testtype.equals(GenWriterThread.TEST_TYPE)) {
      initializeGenWriterJob(newArgs, conf);
    } else if (testtype.equals(DatanodeBenThread.TEST_TYPE)) {
      initializeDatanodeBenJob(newArgs, conf);
    } else {
      printUsage();
    }
    
    updateJobConf(conf, new Path(dfs_input), new Path(dfs_output, "results"));
    long startTime = System.currentTimeMillis();
    JobClient.runJob(conf);
    long endTime = System.currentTimeMillis();
    printResult(fs, new Path(output, "results"), startTime, endTime);
    verifyFiles(fs);
    //Delete all related files
    if (cleanup)
      cleanUpDirs(fsConfig);
    return 0;
  }

  public static void main(String[] args) throws Exception {
    System.exit(ToolRunner.run(new DFSGeneralTest(), args));
  }

}
