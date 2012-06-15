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
import org.apache.hadoop.mapred.ReadMapper;
import org.apache.hadoop.mapred.DirReduce;
import org.apache.hadoop.mapred.DirMapper;
import org.apache.hadoop.hdfs.Constant;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

@SuppressWarnings("deprecation")
public class DFSDirTest extends Configured implements Tool, DirConstant{

  private static long nmaps;
  private static Configuration fsConfig;
  private static long ntasks;
  private static long nthreads;
  private static boolean samedir = false;

  public static void printUsage() {
    System.out.println("USAGE: bin/hadoop hadoop-*-benchmark.jar dirtest nMaps " +
        "[-nTasks] [-nThreads] [-samedir]"); 
    System.out.println("Default nTasks = " + NSUBTASKS);
    System.out.println("Default nThreads = " + NTHREADS);
    System.out.println("-samedir : all dirs are created under the same directory");
    System.exit(0);
  }

  public void control(Configuration fsConfig, String fileName)
      throws IOException {
    String name = fileName;
    FileSystem fs = FileSystem.get(fsConfig);
    fs.delete(new Path(DFS_INPUT, name), true);

    SequenceFile.Writer write = null;
    for (int i = 0; i < nmaps; i++) {
      try {
        Path controlFile = new Path(DFS_INPUT, name + i);
        write = SequenceFile.createWriter(fs, fsConfig, controlFile,
            Text.class, LongWritable.class, CompressionType.NONE);
        write.append(new Text(name + i), new LongWritable(this.nthreads));
      } finally {
        if (write != null)
          write.close();
        write = null;
      }
    }
  }

  @Override
  public int run(String[] args) throws IOException {

    long startTime = System.currentTimeMillis();
    if (args.length < 1) {
      printUsage();
    }
    nmaps = Long.parseLong(args[0]);
    ntasks = NSUBTASKS;
    nthreads = NTHREADS;
    

    for (int i = 1; i < args.length; i++) {
      if (args[i].equals("-nTasks")) ntasks = Long.parseLong(args[++i]); else
      if (args[i].equals("-nThreads")) nthreads = Long.parseLong(args[++i]); else
      if (args[i].equals("-samedir")) samedir = true; else
        printUsage();
    }

    // running the Writting
    fsConfig = new Configuration(getConf());
    fsConfig.set("dfs.nmaps", String.valueOf(nmaps));
    fsConfig.set("dfs.nTasks", String.valueOf(ntasks));
    fsConfig.set("dfs.samedir", String.valueOf(samedir));
    FileSystem fs = FileSystem.get(fsConfig);

    if (fs.exists(new Path(DFS_OUTPUT)))
      fs.delete(new Path(DFS_OUTPUT), true);
    if (fs.exists(new Path(DFS_INPUT)))
      fs.delete(new Path(DFS_INPUT), true);
    if (fs.exists(new Path(INPUT)))
      fs.delete(new Path(INPUT), true);
    if (fs.exists(new Path(OUTPUT)))
      fs.delete(new Path(OUTPUT), true);
    fs.delete(new Path(TRASH), true);

    // run the control() to set up for the FileSystem
    control(fsConfig, "testing");

    // prepare the for map reduce job
    JobConf conf = new JobConf(fsConfig, DFSDirTest.class);
    conf.setJobName("dirtest-writing");

    // set the output and input for the map reduce
    FileOutputFormat.setOutputPath(conf, new Path(DFS_OUTPUT + "writing"));
    FileInputFormat.setInputPaths(conf, new Path(DFS_INPUT));

    conf.setInputFormat(SequenceFileInputFormat.class);

    conf.setOutputKeyClass(Text.class);
    conf.setOutputValueClass(Text.class);

    conf.setMapperClass(DirMapper.class);
    conf.setReducerClass(DirReduce.class);
    conf.setNumReduceTasks(1);
    conf.setSpeculativeExecution(false);
    JobClient.runJob(conf);

    // printout the result
    System.out.println("-------------------");
    System.out.println("RESULT FOR WRITING");
    System.out.println("-------------------");
    FSDataInputStream out = fs.open(new Path(OUTPUT,"result-writing"));
    while (true) {
      String temp = out.readLine();
      if (temp == null)
        break;
      System.out.println(temp);
    }
    out.close();

    long endTime = System.currentTimeMillis();

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
    return 0;
  }

  public static void main(String[] args) throws Exception {
    System.exit(ToolRunner.run(new DFSDirTest(), args));
  }

}
