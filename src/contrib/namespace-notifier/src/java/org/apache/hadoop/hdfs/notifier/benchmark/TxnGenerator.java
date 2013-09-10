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
package org.apache.hadoop.hdfs.notifier.benchmark;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.TextInputFormat;

public class TxnGenerator {
  final static Log LOG = LogFactory.getLog(TxnGenerator.class);
  
  public static String TEST_FILE_LABEL = "test.file.label";
  public static String TEST_DIR_LABEL = "test.dir.label";
  
  private static final String NUM_MAPPERS_KEY = "-numtasks";
  private static final String NUM_SUBDIRS_KEY = "-numsubdirs";
  private static final String NUM_FILES_SUB_DIR_KEY = "-numfiles";
  private static final String NUM_ROUND_KEY = "-round";
  private static final String WORKPLACE_KEY = "-workplace";
  
  private static int numMappers = 10;
  private static int numSubDirs = 100;
  private static int numFilesEachSubDirs = 100;
  private static int round = 100;
  private static String workplace = "";
  
  private Configuration conf;
  
  public TxnGenerator(Configuration conf) {
    this.conf = conf;
  }
  
  static class GeneratorMapper implements Mapper<LongWritable, Text, Text, Text> {

    private JobConf jobConf;
    private String workplace;
    
    @Override
    public void configure(JobConf job) {
      this.jobConf = job;
      this.workplace = jobConf.get(TEST_DIR_LABEL);
    }

    @Override
    public void close() throws IOException {
      
    }

    @Override
    public void map(LongWritable key, Text value,
        OutputCollector<Text, Text> output, Reporter reporter)
        throws IOException {
      
      workplace += value.toString();
      Path workPath = new Path(workplace);
      FileSystem fs = workPath.getFileSystem(jobConf);
      long addDirCount = 0;
      long addFileCount = 0;
      long deleteDirCount = 0;
      for (int i = 0; i < round; i++) {
        // each round
        // 1. create numFilesEachSubDirs in the subDirs
        for (int j = 0; j < numSubDirs; j++) {
          String subDir = workplace + "/subdir" + j;
          for (int k = 0; k < numFilesEachSubDirs; k++) {
            String filePath = subDir + "/file" + k;
            Path path = new Path(filePath);
            OutputStream out = fs.create(path);
            out.close();
          }
          reporter.progress();
        }
        addDirCount += numSubDirs;
        addFileCount += numSubDirs * numFilesEachSubDirs;
        
        // 2. delete the sub dirs
        // will skip trash for the path start with "/tmp"
        for (int j = 0; j < numSubDirs; j++) {
          String subDir = workplace + "/subdir" + j;
          fs.delete(new Path(subDir), true);
        }
        deleteDirCount += numSubDirs;
        
        LOG.info("finished Round " + (i+1));
        LOG.info("add dir: " + addDirCount + ", add file: " + addFileCount 
            + ", delete dir: " + deleteDirCount);
        
        reporter.progress();
      }
      fs.close();
    } 
  }
  
  private static JobConf createJobConf(Configuration conf) throws IOException {
    JobConf jobConf = new JobConf(conf);
    String jobName = "transaction_generator";
    jobConf.setJobName(jobName);
    
    String splitDir = workplace + "split/";
    
    jobConf.set(TEST_DIR_LABEL, workplace);
    
    jobConf.setMapSpeculativeExecution(false);
    jobConf.setJarByClass(TxnGenerator.class);
    jobConf.setMapperClass(GeneratorMapper.class);
    jobConf.setInputFormat(TextInputFormat.class);
    
    FileInputFormat.addInputPath(jobConf, new Path(splitDir));
    Random random = new Random();
    FileOutputFormat.setOutputPath(jobConf, new Path(workplace, "output" + random.nextLong()));
    
    jobConf.setNumReduceTasks(0);
    jobConf.setNumMapTasks(numMappers);
    
    createSplitFiles(conf, new Path(splitDir));
    
    return jobConf;
  }
  
  private static void createSplitFiles(Configuration conf, Path splitDir) 
      throws IOException {
    FileSystem fs = splitDir.getFileSystem(conf);
    for (int i = 0; i < numMappers; i++) {
      String mapperDir = "mapper" + i;
      Path path = new Path(splitDir, mapperDir);
      OutputStream os = fs.create(path, true);
      os.write(mapperDir.getBytes());
      os.close();
    }
  }
  
  private void printUsage() {
    System.out.println("NotifierShell -generatetxn -workplace workplace " +
              "[" + NUM_MAPPERS_KEY + " numMappers] " +
              "[" + NUM_SUBDIRS_KEY + " numSubDirs] " + 
              "[" + NUM_FILES_SUB_DIR_KEY + " numFiles] " + 
              "[" + NUM_ROUND_KEY + " numRound]");
  }
  
  public void start(String[] args, int startIndex) throws IOException {
    try {
      while(startIndex < args.length) {
        String cmd = args[startIndex ++];
        if (cmd.equals(NUM_MAPPERS_KEY)) {
          numMappers = Integer.valueOf(args[startIndex ++]);
        } else if (cmd.equals(NUM_SUBDIRS_KEY)) {
          numSubDirs = Integer.valueOf(args[startIndex ++]);
        } else if (cmd.equals(NUM_FILES_SUB_DIR_KEY)) {
          numFilesEachSubDirs = Integer.valueOf(args[startIndex ++]);
        } else if (cmd.equals(NUM_ROUND_KEY)) {
          round = Integer.valueOf(args[startIndex ++]);
        } else if (cmd.equals(WORKPLACE_KEY)) {
          workplace = args[startIndex ++];
        }
      }
    } catch (Exception e) {
      printUsage();
      System.exit(-1);
    }
    
    if (workplace.trim().isEmpty()) {
      printUsage();
      System.exit(-1);
    }
    
    if (!workplace.endsWith(Path.SEPARATOR)) {
      workplace += Path.SEPARATOR;
    }
    
    JobConf jobConf = createJobConf(conf);
    
    JobClient client = new JobClient(jobConf);
    RunningJob runningJob = client.submitJob(jobConf);
    runningJob.waitForCompletion();
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    TxnGenerator generator = new TxnGenerator(conf);
    generator.start(args, 0);
  }
}
