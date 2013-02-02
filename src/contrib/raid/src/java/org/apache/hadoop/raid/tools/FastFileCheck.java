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

package org.apache.hadoop.raid.tools;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.zip.CRC32;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.raid.Codec;
import org.apache.hadoop.raid.Decoder;
import org.apache.hadoop.raid.LogUtils;
import org.apache.hadoop.raid.Decoder.DecoderInputStream;
import org.apache.hadoop.raid.LogUtils.LOGRESULTS;
import org.apache.hadoop.raid.ParallelStreamReader;
import org.apache.hadoop.raid.ParallelStreamReader.ReadResult;
import org.apache.hadoop.raid.ParityFilePair;
import org.apache.hadoop.raid.RaidUtils;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.SequenceFileRecordReader;
import org.apache.hadoop.mapred.Utils;

public class FastFileCheck {
  final static Log LOG = LogFactory.getLog(
      "org.apache.hadoop.raid.tools.FastFileCheck");
  
  private static final SimpleDateFormat dateForm = new SimpleDateFormat("yyyy-MM-dd-HH-mm");
  private static int DEFAULT_VERIFY_LEN = 64 * 1024; // 64k
  private static int BUFFER_LEN = DEFAULT_VERIFY_LEN;
  private static final String NAME = "fastfilecheck";
  private static final String JOB_DIR_LABEL = NAME + ".job.dir";
  private static final String OP_LIST_LABEL = NAME + ".op.list";
  private static final String OP_COUNT_LABEL = NAME + ".op.count";
  private static final String SOURCE_ONLY_CONF = "sourceonly";
  private static final short OP_LIST_RELICATION = 10;
  private static final int TASKS_PER_JOB = 50;
  private static long filesPerTask = 10;
  private static final int MAX_FILES_PER_TASK = 10000;
  private boolean sourceOnly = false;
  private Configuration conf;
  private static final Random rand = new Random();
  
  enum State{
    GOOD_FILE,
    BAD_FILE,
    NOT_RAIDED,
    UNREADABLE,
    NOT_FOUND
  }
  
  public FastFileCheck(Configuration conf) {
    this.conf = conf;
  }
  
  /**
   * Get the input splits from the operation file list.
   */
  static class FileCheckInputFormat implements InputFormat<Text, Text> {

    @Override
    public InputSplit[] getSplits(JobConf job, int numSplits)
        throws IOException {
      numSplits = TASKS_PER_JOB;
      // get how many records.
      final int totalRecords = job.getInt(OP_COUNT_LABEL, -1);
      final int targetCount = totalRecords / numSplits;
      
      String fileList = job.get(OP_LIST_LABEL, "");
      if (totalRecords < 0 || "".equals(fileList)) {
        throw new RuntimeException("Invalid metadata.");
      }
      
      Path srcs = new Path(fileList);
      FileSystem fs = srcs.getFileSystem(job);
      
      List<FileSplit> splits = new ArrayList<FileSplit>(numSplits);
      Text key = new Text();
      Text value = new Text();
      SequenceFile.Reader in = null;
      long prev = 0L;
      int count = 0;
      
      // split the files to be checked.
      try {
        for (in = new SequenceFile.Reader(fs, srcs, job); in.next(key, value);) {
          long cur = in.getPosition();
          long delta = cur - prev;
          if (++count > targetCount) {
            count = 0;
            splits.add(new FileSplit(srcs, prev, delta, (String[])null));
            prev = cur;
          }
        }
      } finally {
        in.close();
      }
      
      long remaining = fs.getFileStatus(srcs).getLen() - prev;
      if (0 != remaining) {
        splits.add(new FileSplit(srcs, prev, remaining, (String[])null));
      }
      
      return splits.toArray(new FileSplit[splits.size()]);
    }

    @Override
    public RecordReader<Text, Text> getRecordReader(InputSplit split,
        JobConf job, Reporter reporter) throws IOException {
      return new SequenceFileRecordReader<Text, Text>(job, (FileSplit) split);
    }
    
  }
  
  static class FileCheckMapper implements Mapper<Text, Text, Text, Text> {
    private JobConf jobConf;
    private int failCount = 0;
    private int succeedCount = 0;
    private boolean sourceOnly = false;

    @Override
    public void configure(JobConf job) {
      this.jobConf = job;
      this.sourceOnly = job.getBoolean(SOURCE_ONLY_CONF, false);
    }

    @Override
    public void close() throws IOException {
      
    }
    
    private String getCountString() {
      return "Succeeded: " + succeedCount + " Failed: " + failCount;
    }

    @Override
    public void map(Text key, Text value, OutputCollector<Text, Text> output,
        Reporter reporter) throws IOException {
      // run a file operation
      Path p = new Path(key.toString());
      String v;
      try {
        if (sourceOnly) {
          v = processSourceFile(p, reporter, jobConf);
        } else {
          v = processFile(p, reporter, jobConf);
        }
        LOG.info("File: " + p + ", result: " + v);
        output.collect(key, new Text(v));
        reporter.progress();
        ++ succeedCount;
      } catch (InterruptedException e) {
        ++ failCount;
        LOG.warn("Interrupted when processing file: " + p);
        throw new IOException(e);
      } finally {
        reporter.setStatus(getCountString());
      }
    }
    
    /**
     * check a source file.
     */
    String processSourceFile(Path p, Progressable reporter, 
        Configuration conf) throws IOException, InterruptedException {
      LOG.info("Processing Source file: " + p);
      FileSystem fs = p.getFileSystem(conf);
      if (!fs.exists(p)) {
        return State.NOT_FOUND.toString();
      }
      
      Codec codec = Codec.getCodec("rs");
      boolean result = false;
      try {
        result = checkFile(conf, fs, fs, p, null, codec, reporter, true);
      } catch (IOException ex) {
        LOG.warn("Encounter exception when checking file: " + p + 
            ", " + ex.getMessage());
        return State.UNREADABLE.toString();
      }
      
      return result ? State.GOOD_FILE.toString() : State.BAD_FILE.toString();
    }
    
    /**
     * check a single file.
     */
    String processFile(Path p, Progressable reporter, Configuration conf) 
        throws IOException, InterruptedException {
      
      LOG.info("Processing file: " + p);
      FileSystem fs = p.getFileSystem(conf);
      if (!fs.exists(p)) {
        return State.NOT_FOUND.toString();
      }
      FileStatus srcStat = null;
      try {
        srcStat = fs.getFileStatus(p);
      } catch (Exception e) {
        return State.NOT_FOUND.toString();
      }
      boolean result = false;
      boolean raided = false;
      for (Codec codec : Codec.getCodecs()) {
        ParityFilePair pfPair = ParityFilePair.getParityFile(codec, srcStat, conf);
        if (pfPair != null) {
          raided = true;
          Path parityPath = pfPair.getPath();
          try {
            result = checkFile(conf, fs, fs, p, parityPath, codec, reporter
                , false);
          } catch (IOException ex) {
            LOG.warn("Encounter exception when checking the file: " + p, ex);
            LogUtils.logFileCheckMetrics(LOGRESULTS.FAILURE, codec, p, 
                fs, -1, -1, ex, reporter);
            return State.UNREADABLE.toString();
          }
          break;
        }
      }
      
      if (!raided) {
        return State.NOT_RAIDED.toString();
      }
      
      return result ? State.GOOD_FILE.toString() : State.BAD_FILE.toString();
    }
  }
  
  private JobConf createJobConf(Configuration conf) {
    JobConf jobConf = new JobConf(conf);
    String jobName = NAME + "_" + dateForm.format(new Date(System.currentTimeMillis()));
    jobConf.setJobName(jobName);
    jobConf.setMapSpeculativeExecution(false);
    jobConf.setJarByClass(FastFileCheck.class);
    jobConf.setInputFormat(FileCheckInputFormat.class);
    jobConf.setOutputFormat(SequenceFileOutputFormat.class);
    jobConf.setOutputKeyClass(Text.class);
    jobConf.setOutputValueClass(Text.class);
    jobConf.setMapperClass(FileCheckMapper.class);
    jobConf.setNumReduceTasks(0);
    jobConf.setBoolean(SOURCE_ONLY_CONF, sourceOnly);
    
    return jobConf;
  }
  
  /**
   * Check a file.
   */
  public static boolean checkFile(Configuration conf,
      FileSystem srcFs, FileSystem parityFs,
      Path srcPath, Path parityPath, Codec codec, 
      Progressable reporter,
      boolean sourceOnly) 
          throws IOException, InterruptedException {
    FileStatus stat = srcFs.getFileStatus(srcPath);
    long blockSize = stat.getBlockSize();
    long len = stat.getLen();
    
    List<Long> offsets = new ArrayList<Long>();
    // check a small part of each stripe.
    for (int i = 0; i * blockSize < len; i += codec.stripeLength) {
      offsets.add(i * blockSize);
    }
    
    for (long blockOffset : offsets) {
      if (sourceOnly) {
        if (!verifySourceFile(conf, srcFs,stat,
            codec, blockOffset, reporter)) {
          return false;
        }
      }
      else {
        if (!verifyFile(conf, srcFs, parityFs, stat,
            parityPath, codec, blockOffset, reporter)) {
          return false;
        }
      }
    }
    
    return true;
  }
  
  private static boolean verifySourceFile(Configuration conf,
      FileSystem srcFs, FileStatus stat, Codec codec,
      long blockOffset, Progressable reporter)
    throws IOException, InterruptedException {
      Path srcPath = stat.getPath();
      LOG.info("Verify file: " + srcPath + " at offset: " + blockOffset);
      int limit = (int) Math.min(stat.getBlockSize(), DEFAULT_VERIFY_LEN);
      if (reporter == null) {
        reporter = RaidUtils.NULL_PROGRESSABLE;
      }
      
      List<Long> errorOffsets = new ArrayList<Long>();
      // first limit bytes
      errorOffsets.add(blockOffset);
      long left = Math.min(stat.getBlockSize(), stat.getLen() - blockOffset);
      if (left > limit) {
        // last limit bytes
        errorOffsets.add(blockOffset + left - limit);
        // random limit bytes.
        errorOffsets.add(blockOffset + 
            rand.nextInt((int)(left - limit)));
      }
      
      long blockSize = stat.getBlockSize();
      long fileLen = stat.getLen();
      List<InputStream> streamList = new ArrayList<InputStream>();
      List<InputStream> tmpList = new ArrayList<InputStream>();
      try {
        for (long errorOffset : errorOffsets) {
          int k = 0;
          int len = streamList.size();
          tmpList.clear();
          for (int i = 0; i < codec.stripeLength; i++) {
            if (errorOffset + blockSize * i >= fileLen) {
              break;
            }
            
            FSDataInputStream is = null;
            if (k < len) {
              // resue the input stream
              is = (FSDataInputStream) streamList.get(k);
              k++;
            } else {
              is = srcFs.open(srcPath);
              streamList.add(is);
            }
            is.seek(errorOffset + blockSize * i);
            tmpList.add(is);
          }
          
          if (tmpList.size() == 0) {
            continue;
          }
          
          InputStream[] streams = tmpList.toArray(new InputStream[] {});
          ParallelStreamReader reader = null;
          try {
            reader = new ParallelStreamReader(
                reporter, streams, 
                limit, 4, 2, limit);
            reader.start();
            int readNum = 0;
            while (readNum < limit) {
              ReadResult result = reader.getReadResult();
              for (IOException ex : result.ioExceptions) {
                if (ex != null) {
                  LOG.warn("Encounter exception when checking file: " + srcPath + 
                      ", " + ex.getMessage());
                  return false;
                }
              }
              readNum += result.readBufs[0].length;
            }
          } finally {
            if (null != reader) {
              reader.shutdown();
            }
            reporter.progress();
          }
        } 
      } finally {
        if (streamList.size()> 0) {
          RaidUtils.closeStreams(streamList.toArray(new InputStream[]{}));
        }
      }
      return true;
  }
  
  /**
   * Verify the certain offset of a file.
   */
  private static boolean verifyFile(Configuration conf,
      FileSystem srcFs, FileSystem parityFs,
      FileStatus stat, Path parityPath, Codec codec,
      long blockOffset, Progressable reporter) 
    throws IOException, InterruptedException {
    Path srcPath = stat.getPath();
    LOG.info("Verify file: " + srcPath + " at offset: " + blockOffset);
    int limit = (int) Math.min(stat.getBlockSize(), DEFAULT_VERIFY_LEN);
    if (reporter == null) {
      reporter = RaidUtils.NULL_PROGRESSABLE;
    }
    
    // try to decode.
    Decoder decoder = new Decoder(conf, codec);
    if (codec.isDirRaid) {
      decoder.connectToStore(srcPath);
    }
    
    List<Long> errorOffsets = new ArrayList<Long>();
    // first limit bytes
    errorOffsets.add(blockOffset);
    long left = Math.min(stat.getBlockSize(), stat.getLen() - blockOffset);
    if (left > limit) {
      // last limit bytes
      errorOffsets.add(blockOffset + left - limit);
      // random limit bytes.
      errorOffsets.add(blockOffset + 
          rand.nextInt((int)(left - limit)));
    }
 
    byte[] buffer = new byte[limit];
    FSDataInputStream is = srcFs.open(srcPath);
    try {
      for (long errorOffset : errorOffsets) {
        is.seek(errorOffset);
        is.read(buffer);
        // calculate the oldCRC.
        CRC32 oldCrc = new CRC32();
        oldCrc.update(buffer);
        
        CRC32 newCrc = new CRC32();
        DecoderInputStream stream = decoder.new DecoderInputStream(
            RaidUtils.NULL_PROGRESSABLE, limit, stat.getBlockSize(), errorOffset, 
            srcFs, srcPath, parityFs, parityPath, null, null, false);
        try {
          stream.read(buffer);
          newCrc.update(buffer);
          if (oldCrc.getValue() != newCrc.getValue()) {
            LogUtils.logFileCheckMetrics(LOGRESULTS.FAILURE, codec, srcPath, 
                srcFs, errorOffset, limit, null, reporter);
            LOG.error("mismatch crc, old " + oldCrc.getValue() + 
                ", new " + newCrc.getValue() + ", for file: " + srcPath
                + " at offset " + errorOffset + ", read limit " + limit);
            return false;
          }
        } finally {
          reporter.progress();
          if (stream != null) {
            stream.close();
          }
        }
      }
      return true;
    } finally {
      is.close();
    }
  }
 
  private static class JobContext {
    public RunningJob job;
    public JobConf jobConf;
    
    public JobContext(RunningJob job, JobConf jobConf) {
      this.job = job;
      this.jobConf = jobConf;
    }
  }
  
  private List<JobContext> submitJobs(BufferedReader reader, 
          int filesPerJob, Configuration conf) 
      throws IOException {
    List<JobContext> submitted = new ArrayList<JobContext>();
    boolean done = false;
    Random rand = new Random(new Date().getTime());
    filesPerTask = (long) Math.ceil((double)filesPerJob / TASKS_PER_JOB);
    filesPerTask = Math.min(filesPerTask, MAX_FILES_PER_TASK);
    do {
      JobConf jobConf = createJobConf(conf);
      JobClient jobClient = new JobClient(jobConf);
      String randomId = Integer.toString(rand.nextInt(Integer.MAX_VALUE), 36);
      Path jobDir = new Path(jobClient.getSystemDir(), NAME + "_" + randomId);
      jobConf.set(JOB_DIR_LABEL, jobDir.toString());
      Path log = new Path(jobDir, "_logs");
      FileOutputFormat.setOutputPath(jobConf, log);
      LOG.info("log=" + log);
      
      // create operation list
      FileSystem fs = jobDir.getFileSystem(jobConf);
      Path opList = new Path(jobDir, "_" + OP_LIST_LABEL);
      jobConf.set(OP_LIST_LABEL, opList.toString());
      int opCount = 0;
      int synCount = 0;
      SequenceFile.Writer opWriter = null;
      
      try {
        opWriter = SequenceFile.createWriter(fs, jobConf, opList, Text.class, 
            Text.class, SequenceFile.CompressionType.NONE);
        String f = null;
        do {
          f = reader.readLine();
          if (f == null) {
            // no more file
            done = true;
            break;
          }
          opWriter.append(new Text(f), new Text(f));
          opCount ++;
          if (++synCount > filesPerTask) {
            opWriter.sync();
            synCount = 0;
          }
        } while (opCount < filesPerJob);
        
      } finally {
        if (opWriter != null) {
          opWriter.close();
        }
        fs.setReplication(opList, OP_LIST_RELICATION);
      }
      
      jobConf.setInt(OP_COUNT_LABEL, opCount);
      RunningJob job = jobClient.submitJob(jobConf);
      submitted.add(new JobContext(job, jobConf));
    } while (!done);
    
    return submitted;
  }
  
  private void waitForJobs(List<JobContext> submitted, Configuration conf) 
      throws IOException, InterruptedException {
    JobConf jobConf = createJobConf(conf);
    JobClient jobClient = new JobClient(jobConf);
    List<JobContext> running = new ArrayList<JobContext>(submitted);
    while (!running.isEmpty()) {
      Thread.sleep(60000);
      LOG.info("Checking " + running.size() + " running jobs");
      for (Iterator<JobContext> it = running.iterator(); it.hasNext();) {
        Thread.sleep(2000);
        JobContext context = it.next();
        try {
          if (context.job.isComplete()) {
            it.remove();
            LOG.info("Job " + context.job.getID() + " complete. URL: " + 
                    context.job.getTrackingURL());
          } else {
            LOG.info("Job " + context.job.getID() + " still running. URL: " +
                    context.job.getTrackingURL());
          }
        } catch (Exception ex) {
          LOG.error("Hit error while checking job status.", ex);
          it.remove();
          try {
            context.job.killJob();
          } catch (Exception ex2) {
            // ignore the exception.
          }
        }
      }
    }
  }
  
  private void printResult(List<JobContext> submitted, Configuration conf) 
      throws IOException {
    Text key = new Text();
    Text value = new Text();
    
    Map<State, Integer> stateToCountMap = new HashMap<State, Integer>();
    for (State s : State.values()) {
      stateToCountMap.put(s, 0);
    }
    
    for (JobContext context : submitted) {
      Path outputpath = SequenceFileOutputFormat.getOutputPath(context.jobConf);
      FileSystem fs = outputpath.getFileSystem(context.jobConf);
      
      Path dir = SequenceFileOutputFormat.getOutputPath(context.jobConf);
      Path[] names = FileUtil.stat2Paths(fs.listStatus(dir));
      List<Path> resultPart = new ArrayList<Path>(); 
      for (Path name : names) {
        String fileName = name.toUri().getPath();
        int index = fileName.lastIndexOf('/');
        fileName = fileName.substring(index + 1);
        if (fileName.startsWith("part-")) {
          resultPart.add(name);
        }
      }
      names = resultPart.toArray(new Path[] {});
      
      // sort names, so that hash partitioning works
      Arrays.sort(names);
      
      SequenceFile.Reader[] jobOutputs = new SequenceFile.Reader[names.length];
      for (int i = 0; i < names.length; i++) {
        jobOutputs[i] = new SequenceFile.Reader(fs, names[i], conf);
      }
      
      // read ouput of job.
      try {
        for (SequenceFile.Reader r : jobOutputs) {
          while (r.next(key, value)) {
            State state = State.valueOf(value.toString());
            stateToCountMap.put(state, stateToCountMap.get(state) + 1);
            // print the file result to stdout.
            System.out.println(key + " " + value);
          }
        }
      } finally {
        for (SequenceFile.Reader r : jobOutputs) {
          r.close();
        }
      }
    }
    
    // print summary to std error.
    for (State s : State.values()) {
      String output = s + " " + stateToCountMap.get(s);
      System.err.println(output);
    }
  }
  
  private void printUsage() {
    System.err.println(
        "java FastFileCheck [options] [-filesPerJob N] [-sourceOnly] /path/to/inputfile\n");
    ToolRunner.printGenericCommandUsage(System.err);
  }

  public void startFileCheck(String[] args, int startIndex, Configuration conf) 
      throws IOException, InterruptedException {
    JobConf jobConf = createJobConf(conf);
    String inputFile = null;
    int filesPerJob = Integer.MAX_VALUE;
    sourceOnly = false;
    
    for (int i = startIndex; i < args.length; i++) {
      String arg = args[i];
      if (arg.equalsIgnoreCase("-filesPerJob")) {
        i ++;
        filesPerJob = Integer.parseInt(args[i]);
      } else if (arg.equalsIgnoreCase("-sourceOnly")) {
        sourceOnly = true;
      } else {
        inputFile = arg;
      }
    }
    
    InputStream in = 
        inputFile == null ? System.in : new FileInputStream(inputFile);
    BufferedReader reader = new BufferedReader(new InputStreamReader(in));
    List<JobContext> submitted = submitJobs(reader, filesPerJob, conf);
    waitForJobs(submitted, conf);
    printResult(submitted, conf);
  }
}
