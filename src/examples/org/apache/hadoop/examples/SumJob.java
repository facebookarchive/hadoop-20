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

import java.io.IOException;


import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import java.util.Iterator;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class SumJob extends Configured implements Tool  {
  /** The path of the temporary directory for the job */
  static final Path TMP_DIR = new Path(SumJob.class.getSimpleName() + "_TMP");

  /**
   * Mapper class for the SumJob.
   */
  public static class MyMapper
    extends MapReduceBase
    implements Mapper<LongWritable, LongWritable, LongWritable, LongWritable> {

    /**
     * Map task for the job
     *
     * @param key the input key.
     * @param value the input value.
     * @param output collects mapped keys and values.
     * @param reporter facility to report progress.
     * @throws IOException
     */
    public void map(LongWritable key, LongWritable value,
                    OutputCollector<LongWritable, LongWritable> output,
                    Reporter reporter) throws IOException {
      output.collect(key, value);
    }
  }

  /**
   * Reducer class for SumJob
   */
  public static class MyReducer
    extends MapReduceBase
    implements Reducer<LongWritable, LongWritable, LongWritable, LongWritable> {
    private long sum = 0;
    private JobConf conf;

    /** Store job configuration. */
    @Override
    public void configure(JobConf job) {
      conf = job;
    }

    /**
     * Reduce task for the job. It sums up all the numbers in its partitions
     * and writes out the result
     *
     * @param key the key.
     * @param values the list of values to reduce.
     * @param output to collect keys and combined values.
     * @param reporter facility to report progress.
     * @throws IOException
     */
    public void reduce(LongWritable key, Iterator<LongWritable> values,
                       OutputCollector<LongWritable, LongWritable> output,
                       Reporter reporter) throws IOException {
      while (values.hasNext()) {
        long value = values.next().get();
        this.sum = this.sum + value;
      }
      output.collect(key, new LongWritable(sum));
    }
  }

  /**
   * Runs the job to find the sum of integers between 1..N. Also checks if the
   * sum is the same as (N*(N+1))/2
   *
   * @param args command specific arguments.
   * @return A non-zero return value if the job fails
   * @throws Exception
   */
  public int run (String args[]) throws Exception {
    Configuration conf = new Configuration();
    String[] otherArgs = new GenericOptionsParser(conf, args).
                              getRemainingArgs();
    if (otherArgs.length != 2) {
      System.err.println("Usage: sum <numRecords> " +
        "<numReduceTasks>");
      ToolRunner.printGenericCommandUsage(System.err);
      return -1;
    }
    int numRecords = Integer.parseInt(otherArgs[0]);
    int numReduceTasks = Integer.parseInt(otherArgs[1]);

    System.out.println("numRecords: " +  numRecords +
      " numReduceTasks: " + numReduceTasks);

    JobConf jobConf = new JobConf(this.getConf(), SumJob.class);
    final FileSystem fs = FileSystem.get(jobConf);

    if (!fs.mkdirs(TMP_DIR)) {
      throw new IOException("Cannot create input directory " + TMP_DIR);
    }

    final Path inFile = new Path(TMP_DIR, "inp");

    // If the input directory already exists, the Job would assume that the
    // input directory has the right input files, and would not create them
    // again.
    if (!fs.exists(inFile)) {
      SequenceFile.Writer writer = SequenceFile.createWriter(fs, jobConf, inFile,
        LongWritable.class, LongWritable.class);

      for (int i = 1; i <= numRecords; i++) {
        writer.append(
          new LongWritable(i%numReduceTasks ),
          new LongWritable(i));
      }
      writer.close();
    }

    final Path outDir = new Path(TMP_DIR, "out");
    if (fs.exists(outDir)) {
      throw new IOException("Tmp directory " + fs.makeQualified(outDir)
        + " already exists.  Please remove it first.");
    }

    SequenceFileInputFormat.setInputPaths(jobConf, inFile);
    FileOutputFormat.setOutputPath(jobConf, outDir);
    jobConf.setJobName("sum");
    jobConf.setInputFormat(SequenceFileInputFormat.class);
    jobConf.setOutputFormat(SequenceFileOutputFormat.class);
    jobConf.setMapperClass(MyMapper.class);
    jobConf.setCombinerClass(MyReducer.class);
    jobConf.setReducerClass(MyReducer.class);
    jobConf.setOutputKeyClass(LongWritable.class);
    jobConf.setOutputValueClass(LongWritable.class);
    jobConf.setNumReduceTasks(numReduceTasks);

    final long startTime = System.currentTimeMillis();
    JobClient.runJob(jobConf);
    final double duration = (System.currentTimeMillis() - startTime)/1000.0;
    System.out.println("Job Finished in " + duration + " seconds");

    // Read outputs
    Long finalSum = new Long(0);
    // Get the list of all the files which are in the output directory
    FileStatus fileStatuses[] = fs.listStatus(outDir);
    for (FileStatus fileStatus : fileStatuses) {
      // If the file name starts with "part", read it.
      if (fileStatus.getPath().getName().startsWith("part")) {
        Path outFile = fileStatus.getPath();
        LongWritable key = new LongWritable();
        LongWritable sum = new LongWritable();
        SequenceFile.Reader reader = new SequenceFile.Reader(fs, outFile,
                                                              jobConf);
        try {
          reader.next(key, sum);
          finalSum += sum.get();
        } finally {
          reader.close();
        }
      }
    }

    System.err.println("Sum: " + finalSum);
    Long expectedSum = new Long(((long)numRecords * (numRecords + 1)) / 2);
    if (finalSum.equals(expectedSum)) {
      System.err.println("Job Succeeded!");
      return 0;
    }
    System.err.println("Job Failed! Expected Sum: " + expectedSum);
    return 1;
  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new SumJob(), args);
    System.exit(res);
  }
}
