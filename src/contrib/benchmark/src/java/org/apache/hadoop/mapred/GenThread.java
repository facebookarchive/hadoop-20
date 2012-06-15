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
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.io.Writer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.ArrayList;
import org.apache.hadoop.hdfs.DistributedRaidFileSystem;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.GeneralConstant;
import org.apache.hadoop.io.Text;

@SuppressWarnings("deprecation")
  public class GenThread extends Thread implements
  GeneralConstant {
    private static final Log LOG = LogFactory.getLog(GenThread.class);
    public static final int DEFAULT_BUFFER_SIZE = 1; //1KB
    public static final long DEFAULT_DATA_RATE = 1; //1KB /sec
    public static final String TEST_TYPE = null; 
    public static final long MEGA = 0x100000;
    public static int test_buffer_size = 10*1024; 
    public class RunTimeConstants {
      public String taskID = null;
      public long nthreads = 0;
      public String output_dir = null;
      public int buffer_size = DEFAULT_BUFFER_SIZE * 1024;
    }
    public long total_processed_files = 0;
    public long total_processed_size = 0;
    public float ioRateMbSec = 0.0f;
    
    protected long files_processed = 0;
    protected long processed_size = 0;
    protected FileSystem fs;
    protected byte[] buffer = null;
    protected Path outputPath = null;
    protected Path inputPath = null;
    protected ArrayList<Exception> errors = new ArrayList<Exception>();
    
    public GenThread() {
      
    }
    
    public GenThread(JobConf conf) {
    }
    
    public GenThread(Configuration conf, Path input, Path output,
        RunTimeConstants rtc) throws IOException {
      this.inputPath = input;
      this.outputPath = output;
      this.fs = FileSystem.newInstance(conf);
      if (fs instanceof DistributedRaidFileSystem) {
        fs = ((DistributedRaidFileSystem)fs).getFileSystem();
      }
      this.buffer = new byte[rtc.buffer_size];
      if (test_buffer_size > rtc.buffer_size) 
        test_buffer_size = rtc.buffer_size;
    }
    
    public GenThread(Configuration conf, Path output, RunTimeConstants rtc)
        throws IOException {
      this(conf, null, output, rtc);
    }
    
    public GenThread[] prepare(JobConf conf, Text key, Text value) throws IOException {
      return this.prepare(conf, key, value, new RunTimeConstants());
    }
    
    public GenThread[] prepare(JobConf conf, Text key, Text value, RunTimeConstants rtc) throws IOException {
      rtc.taskID = conf.get(MAP_TASK_ID_KEY);
      rtc.nthreads = conf.getLong(NUMBER_OF_THREADS_KEY, NTHREADS);
      rtc.buffer_size = conf.getInt(BUFFER_SIZE_KEY, DEFAULT_BUFFER_SIZE) * 1024;
      LOG.info("Buffer size: " + rtc.buffer_size);
      rtc.output_dir = conf.get(OUTPUT_DIR_KEY);
      return null;
    }
    
    public static String getErrorMessage(Exception ex) {
      final Writer result = new StringWriter();
      final PrintWriter printWriter = new PrintWriter(result);
      ex.printStackTrace(printWriter);
      return ex.getMessage() + "\n" + result.toString() + "\n";
    }
    
    public Map<String, String> collectStats(JobConf conf, GenThread[] threads,
        long execTime) throws IOException {
      total_processed_files = 0;
      total_processed_size = 0;
      ArrayList<Exception> errors = new ArrayList<Exception>();
      String errorStr = "";
      for (GenThread thread: threads) {
        total_processed_files += thread.files_processed;
        total_processed_size += thread.processed_size;
        ArrayList<Exception> threadErrors = thread.getErrors();
        if (threadErrors.size() > 0) {
          for (Exception threadError: threadErrors) {
            errors.add(threadError);
            errorStr += getErrorMessage(threadError);
          }
        }
      }
      LOG.info("Number of bytes processed = " + total_processed_size);
      LOG.info("Number of flies processed = " + total_processed_files);
      LOG.info("Number of errors = " + errors.size());
      LOG.info("Exec time = " + execTime);
      ioRateMbSec = (float)total_processed_size * 1000 / (execTime * MEGA);
      LOG.info("IO rate = " + ioRateMbSec);
      Map<String, String> stat = new HashMap<String, String>();
      stat.put("size", String.valueOf(total_processed_size));
      stat.put("files", String.valueOf(total_processed_files));
      stat.put("time", String.valueOf(execTime));
      stat.put("rate", String.valueOf(ioRateMbSec));
      stat.put("errors", String.valueOf(errorStr));
      stat.put("nerrors", String.valueOf(errors.size()));
      return stat;
    }
    
    public ArrayList<Exception> getErrors() {
      return errors;
    }
    
    public void reset() {
    }

    public void analyze(Map<String, String> stat) throws IOException {
    }
    
    public void output(FSDataOutputStream out) throws IOException {
    }
  }
