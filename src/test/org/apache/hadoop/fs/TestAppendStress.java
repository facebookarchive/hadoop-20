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
package org.apache.hadoop.fs;

import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.io.InputStream;
import java.util.Date;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.FSConstants;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.util.DataChecksum;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.junit.Test;

public class TestAppendStress extends Configured implements Tool {
  private static final Log LOG = LogFactory.getLog(TestAppendStress.class);
  
  private static final String BASE_FILE_NAME = "test_append_stress";
  private static String TEST_ROOT_DIR = System.getProperty("test.build.data","/benchmarks/TestAppendStress");
  private static Path CONTROL_DIR = new Path(TEST_ROOT_DIR, "io_control");
  private static Path DATA_DIR = new Path(TEST_ROOT_DIR, "io_data");
  private static Path APPEND_DIR = new Path(TEST_ROOT_DIR, "io_append");
  
  private static final String JOB_START_TIME_LABEL = "job_start_time";
  
  private static final int SIZE_RANGE = 1024 * 1024 * 4; // 4MB
  private static final int ROUND_DEFAULT = 100;
  private static final int NUM_FILES_DEFAULT = 1000;
  
  private static int numFiles = NUM_FILES_DEFAULT;
  private static int round = ROUND_DEFAULT;
  
  private static final String USAGE =
      "Usage: " + TestAppendStress.class.getSimpleName() + "[-nFiles N] [-round N]";
  
  @Test
  public void testAppend() throws Exception {
    Configuration fsConfig = new Configuration();
    fsConfig.setBoolean("dfs.support.append", true);
    MiniDFSCluster cluster = null;
    try {
      cluster = new MiniDFSCluster(fsConfig, 2, true, null);
      FileSystem fs = cluster.getFileSystem();
      Path filePath = new Path(DATA_DIR, "file1");
      round = 10;
      assertTrue(doAppendTest(fs, filePath, new Random(), null));
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }
  
  @Override
  public int run(String[] args) throws Exception {
    if (args.length == 0) {
      System.err.println(USAGE);
      return -1;
    }
    
    for (int i = 0; i < args.length; i++) {
      if (args[i].equals("-nFiles")) {
        numFiles = Integer.parseInt(args[++i]);
      } else if (args[i].equals("-round")) {
        round = Integer.parseInt(args[++i]);
      }
    }
    
    LOG.info("nFiles = " + numFiles);
    LOG.info("round = " + round);
    
    
    Configuration conf = getConf();

    try {
      FileSystem fs = FileSystem.get(conf);

      LOG.info("Cleaning up test files");
      fs.delete(new Path(TEST_ROOT_DIR), true);

      createControlFile(fs, numFiles, conf);
      startAppendJob(conf);
    } catch (Exception e) {
      System.err.print(StringUtils.stringifyException(e));
      return -1;
    }

    return 0;
  }
  
  private void startAppendJob(Configuration conf) throws IOException {
    JobConf job = new JobConf(conf, TestAppendStress.class);
    
    job.set(JOB_START_TIME_LABEL, new Date().toString());
    FileInputFormat.setInputPaths(job, CONTROL_DIR);
    FileOutputFormat.setOutputPath(job, APPEND_DIR);
    job.setInputFormat(SequenceFileInputFormat.class);
    
    job.setMapperClass(AppendMapper.class);
    
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    job.setNumReduceTasks(0);
    JobClient.runJob(job);
  }
  
  private static String getFileName(int fIdx) {
    return BASE_FILE_NAME + Integer.toString(fIdx);
  }
  
  
  private static void createControlFile(FileSystem fs,
      int nrFiles,
      Configuration fsConfig
      ) throws IOException {
    fs.delete(CONTROL_DIR, true);

    for(int i=0; i < nrFiles; i++) {
      String name = getFileName(i);
      Path controlFile = new Path(CONTROL_DIR, "in_file_" + name);
      SequenceFile.Writer writer = null;
      try {
        writer = SequenceFile.createWriter(fs, fsConfig, controlFile,
            Text.class, LongWritable.class,
            CompressionType.NONE);
        writer.append(new Text(name), new LongWritable(0));
      } catch(Exception e) {
        throw new IOException(e.getLocalizedMessage());
      } finally {
        if (writer != null)
          writer.close();
        writer = null;
      }
    }
    LOG.info("created control files for: "+nrFiles+" files");
  }

  private static void writeToFile(Random random, FSDataOutputStream out, int len, DataChecksum checksum) 
      throws IOException {
    if (len ==0) {
      return;
    }
    
    LOG.info("Write " + len + " bytes to file.");
    int bufferSize = 1024 * 1024;
    byte[] buffer = new byte[bufferSize];
    int toLen = len;
    while (toLen > 0) {
      random.nextBytes(buffer);
      int numWrite = Math.min(toLen, buffer.length);
      out.write(buffer, 0, numWrite);
      checksum.update(buffer, 0, numWrite);
      toLen -= numWrite;
      
      // randomly do sync or not.
      if (random.nextBoolean()) {
        out.sync();
      }
    }
  }
  
  /**
   * Verify the file length and file crc.
   */
  private static boolean verifyFile(FileSystem fs, Path filePath, 
      int fileLen, DataChecksum checksum) throws IOException {
    FileStatus stat = fs.getFileStatus(filePath);
    if (stat.getLen() != fileLen) {
      return false;
    }
    
    int fileCRC = fs.getFileCrc(filePath); 
       
    LOG.info("Expected checksum: " + (int)checksum.getValue() + ", get: " + fileCRC);
    
    InputStream in = fs.open(filePath);
    DataChecksum newChecksum = DataChecksum.newDataChecksum(FSConstants.CHECKSUM_TYPE, 
        1);
    int toRead = fileLen;
    byte[] buffer = new byte[1024 * 1024];
    while (toRead > 0) {
      int numRead = in.read(buffer);
      newChecksum.update(buffer, 0, numRead);
      toRead -= numRead;
    }
        
    LOG.info("Read CRC: " + (int)newChecksum.getValue());
    return (int)checksum.getValue() == fileCRC && (int)newChecksum.getValue() == fileCRC;
  }

  public static class AppendMapper<T> extends Configured
                implements Mapper<Text, LongWritable, Text, Text> {

    private FileSystem fs;
    private Random random = null;
    private JobConf conf;
    
    public AppendMapper() {}
    
    @Override
    public void configure(JobConf job) {
      conf = job;
      try {
        fs = FileSystem.get(job);
      } catch (IOException e) {
        throw new RuntimeException("Cannot create file system.", e);
      }
    }

    @Override
    public void close() throws IOException {
      
    }

    @Override
    public void map(Text key, LongWritable value,
        OutputCollector<Text, Text> output, Reporter reporter)
        throws IOException {
      String name = key.toString();
      String seedStr = name + conf.get(JOB_START_TIME_LABEL);
      LOG.info("random seed string: " + seedStr);
      random = new Random(seedStr.hashCode());
      Path filePath = new Path(DATA_DIR, name);
      
      if (!doAppendTest(fs, filePath, random, reporter)) {
        throw new RuntimeException("Append operation failed, filePath: " + filePath);
      }
    }
  }
  
  private static boolean doAppendTest(FileSystem fs, Path filePath, Random random, Reporter reporter) 
      throws IOException {
    if (reporter == null) {
      reporter = Reporter.NULL;
    }
    
    FSDataOutputStream out = fs.create(filePath);
    DataChecksum checksum = DataChecksum.newDataChecksum(FSConstants.CHECKSUM_TYPE, 
        1);
    checksum.reset();
    
    int fileLen = 0;
    int len = random.nextInt((int) (SIZE_RANGE + fs.getDefaultBlockSize()));
    fileLen += len;
    writeToFile(random, out, len, checksum);
    out.close();
    
    reporter.progress();
    for (int i = 0; i < round; i++) {
      out = fs.append(filePath);
      
      len = random.nextInt(SIZE_RANGE);
      fileLen += len;
      writeToFile(random, out, len, checksum);
      out.close();
      reporter.progress();
    }
    
    return verifyFile(fs, filePath, fileLen, checksum);
  }
  
  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new TestAppendStress(), args);
    System.exit(res);
  }
}
