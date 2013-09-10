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

package org.apache.hadoop.raid;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.EOFException;
import java.io.FileDescriptor;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.text.SimpleDateFormat;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.ChecksumException;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobID;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.SequenceFileRecordReader;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.util.StringUtils;

/**
 * Utility to check the parity data against source data.
 * It works by trying to compute the parity data of two 1KB chunks in each
 * stripe, and checking against the actual parity data.
 * The input to the utility is a file with the list of
 * files to check. DataFsck kicks off multiple map-only jobs to process the files,
 * waits for the jobs to finish, then prints the results to stdout.
 *
 * Each file is categorized in one of the following states.
 *  DATA_MATCHING_PARITY - file and parity match.
 *  DATA_NOT_MATCHING_PARITY - File and parity do not match.
 *  DATA_UNREADABLE - there was an IOException while reading the file.
 *
 * Options:
 *  -summary : this option prints the count of files in each state instead of a full report.
 *  -filesPerJob : controls the number of files per job.
 *
 * Important options passed through key-value pairs:
 *   -Dmapred.map.tasks=N
 *     start the map-only job with N tasks.
 *
 * Usage:
 * java DataFsck [options] [-summary] [-filesPerJob N] /path/to/input/file
 * The input file should contain the list of files to check. If the input file is "-"
 * the list of files is read from stdin.
 */
public class DataFsck extends Configured implements Tool {
  public static final int CHECKSIZE = 1024;
  private static final SimpleDateFormat dateForm = new SimpleDateFormat("yyyy-MM-dd HH:mm");
  protected static final Log LOG = LogFactory.getLog(DataFsck.class);
  static final String NAME = "datafsck";
  static final String JOB_DIR_LABEL = NAME + ".job.dir";
  static final String OP_LIST_LABEL = NAME + ".op.list";
  static final String OP_COUNT_LABEL = NAME + ".op.count";
  private static final int SYNC_FILE_MAX = 10;
  static final short OP_LIST_REPLICATION = 10; // replication factor of control file
  Configuration conf;

  public enum State {
    DATA_MATCHING_PARITY,
    DATA_NOT_MATCHING_PARITY,
    DATA_UNREADABLE
  }

  static void printUsage() {
    System.err.println(
"java DataFsck [options] [-summary] [-filesPerJob N] /path/to/input/file\n" +
"Utility to check the parity data against source data.\n" +
"The input to the utility is a file with the list of files to check.\n"
);
    ToolRunner.printGenericCommandUsage(System.err);
  }

  public static void main(String[] args) throws Exception {
    org.apache.hadoop.hdfs.DnsMonitorSecurityManager.setTheManager();
    DataFsck dataFsck = new DataFsck(new Configuration());
    int res = ToolRunner.run(dataFsck, args);
    System.exit(res);
  }

  private JobConf createJobConf() {
    JobConf jobConf = new JobConf(getConf());
    String jobName = NAME + " " + dateForm.format(new Date(System.currentTimeMillis()));
    jobConf.setJobName(jobName);
    jobConf.setMapSpeculativeExecution(false);

    jobConf.setJarByClass(DataFsck.class);
    jobConf.setInputFormat(DataFsckInputFormat.class);
    jobConf.setOutputFormat(SequenceFileOutputFormat.class);
    jobConf.setOutputKeyClass(Text.class);
    jobConf.setOutputValueClass(Text.class);

    jobConf.setMapperClass(DataFsckMapper.class);
    jobConf.setNumReduceTasks(0);
    return jobConf;
  }

  private static class JobContext {
    public RunningJob runningJob;
    public JobConf jobConf;
    public JobContext(RunningJob runningJob, JobConf jobConf) {
      this.runningJob = runningJob;
      this.jobConf = jobConf;
    }
  }

  List<JobContext> submitJobs(BufferedReader inputReader, int filesPerJob) throws IOException {
    boolean done = false;
    JobClient jClient = new JobClient(createJobConf());
    List<JobContext> submitted = new ArrayList<JobContext>();
    Random rand = new Random();
    do {
      JobConf jobConf = createJobConf();
      final String randomId = Integer.toString(rand.nextInt(Integer.MAX_VALUE), 36);
      Path jobDir = new Path(jClient.getSystemDir(), NAME + "_" + randomId);
      jobConf.set(JOB_DIR_LABEL, jobDir.toString());
      Path log = new Path(jobDir, "_logs");
      FileOutputFormat.setOutputPath(jobConf, log);
      LOG.info("log=" + log);

      // create operation list
      FileSystem fs = jobDir.getFileSystem(jobConf);
      Path opList = new Path(jobDir, "_" + OP_LIST_LABEL);
      jobConf.set(OP_LIST_LABEL, opList.toString());
      int opCount = 0, synCount = 0;
      SequenceFile.Writer opWriter = null;

      try {
        opWriter = SequenceFile.createWriter(fs, jobConf, opList, Text.class,
            Text.class, SequenceFile.CompressionType.NONE);
        String f = null;
        do {
          f = inputReader.readLine();
          if (f == null) {
            done = true;
            break;
          }
          opWriter.append(new Text(f), new Text(f));
          opCount++;
          if (++synCount > SYNC_FILE_MAX) {
            opWriter.sync();
            synCount = 0;
          }
        } while (opCount < filesPerJob);
      } finally {
        if (opWriter != null) {
          opWriter.close();
        }
        fs.setReplication(opList, OP_LIST_REPLICATION); // increase replication for control file
      }

      jobConf.setInt(OP_COUNT_LABEL, opCount);
      RunningJob rJob = jClient.submitJob(jobConf);
      JobContext ctx = new JobContext(rJob, jobConf);
      submitted.add(ctx);
    } while (!done);

    return submitted;
  }

  void waitForJobs(List<JobContext> submitted) throws IOException, InterruptedException {
    JobClient jClient = new JobClient(createJobConf());
    List<JobContext> running = new ArrayList<JobContext>(submitted);
    while (!running.isEmpty()) {
      Thread.sleep(60000);
      LOG.info("Checking " + running.size() + " running jobs");
      for (Iterator<JobContext> it = running.iterator(); it.hasNext(); ) {
        Thread.sleep(2000);
        JobContext ctx = it.next();
        try {
          if (ctx.runningJob.isComplete()) {
            it.remove();
            LOG.info("Job " + ctx.runningJob.getID() + " complete. URL: " +
              ctx.runningJob.getTrackingURL());
          } else {
            LOG.info("Job " + ctx.runningJob.getID() + " still running. URL: " +
              ctx.runningJob.getTrackingURL());
          }
        } catch (IOException e) {
          LOG.warn("Error while checking job " + ctx.runningJob.getID() +
            ", killing it ", e);
          it.remove();
          try {
            ctx.runningJob.killJob();
          } catch (IOException e2) {
          }
        }
      }
    }
  }

  List<SequenceFile.Reader> getOutputs(List<JobContext> submitted) throws IOException {
    List<SequenceFile.Reader> outputs = new ArrayList<SequenceFile.Reader>();
    for (JobContext ctx: submitted) {
      SequenceFile.Reader[] jobOutputs = SequenceFileOutputFormat.getReaders(
        getConf(),
        SequenceFileOutputFormat.getOutputPath(ctx.jobConf));
      for (SequenceFile.Reader r: jobOutputs) {
        outputs.add(r);
      }
    }
    return outputs;
  }

  void printResult(List<SequenceFile.Reader> outputs, boolean summary)
      throws IOException {
    // Start reading output of job.
    Text key = new Text();
    Text val = new Text();
    Map<State, Integer> stateToCountMap = new HashMap<State, Integer>();
    for (State s: State.values()) {
      stateToCountMap.put(s, 0);
    }
    for (SequenceFile.Reader r: outputs) {
      while (r.next(key, val)) {
        State s = State.valueOf(val.toString());
        stateToCountMap.put(s, 1 + stateToCountMap.get(s));
        if (summary) {
          System.err.println(key + " " + val);
        } else {
          System.out.println(key + " " + val);
        }
      }
    }
    // Print stats.
    for (State s: State.values()) {
      String stat = s + " " + stateToCountMap.get(s);
      if (summary) {
        System.out.println(stat);
      } else {
        System.err.println(stat);
      }
    }
  }

  @Override
  public int run(String args[]) throws Exception {
    String inputFile = null;
    boolean summary = false;
    int filesPerJob = Integer.MAX_VALUE;
    for (int i = 0; i < args.length; i++) {
      String arg = args[i];
      if (arg.equalsIgnoreCase("-summary")) {
        summary = true;
      } else if (arg.equalsIgnoreCase("-h") || arg.equalsIgnoreCase("--help")) {
        printUsage();
        return -1;
      } else if (arg.equalsIgnoreCase("-filesPerJob")) {
        i++;
        if (i == args.length) {
          printUsage();
          return -1;
        }
        filesPerJob = Integer.parseInt(args[i]);
      } else {
        inputFile = arg;
      }
    }
    if (inputFile == null) {
      printUsage();
      return -1;
    }

    InputStream in = inputFile.equals("-") ? System.in : new FileInputStream(inputFile);
    BufferedReader reader = new BufferedReader(new InputStreamReader(in));
    List<JobContext> submitted = submitJobs(reader, filesPerJob);

    waitForJobs(submitted);

    List<SequenceFile.Reader> outputs = getOutputs(submitted);

    printResult(outputs, summary);

    return 0;
  }

  public DataFsck(Configuration conf) {
    super(conf);
    getConf().set(
      "fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
  }

  static class DataFsckInputFormat implements InputFormat<Text, Text> {
    public void validateInput(JobConf job) {
    }

    public InputSplit[] getSplits(JobConf job, int numSplits)
        throws IOException {
      final int srcCount = job.getInt(OP_COUNT_LABEL, -1);
      final int targetcount = srcCount / numSplits;
      String srclist = job.get(OP_LIST_LABEL, "");
      if (srcCount < 0 || "".equals(srclist)) {
        throw new RuntimeException("Invalid metadata: #files(" + srcCount
            + ") listuri(" + srclist + ")");
      }
      Path srcs = new Path(srclist);
      FileSystem fs = srcs.getFileSystem(job);

      List<FileSplit> splits = new ArrayList<FileSplit>(numSplits);

      Text key = new Text();
      Text value = new Text();
      SequenceFile.Reader in = null;
      long prev = 0L;
      int count = 0; // count src
      try {
        for (in = new SequenceFile.Reader(fs, srcs, job); in.next(key, value);) {
          long curr = in.getPosition();
          long delta = curr - prev;
          if (++count > targetcount) {
            count = 0;
            splits.add(new FileSplit(srcs, prev, delta, (String[]) null));
            prev = curr;
          }
        }
      } finally {
        in.close();
      }
      long remaining = fs.getFileStatus(srcs).getLen() - prev;
      if (remaining != 0) {
        splits.add(new FileSplit(srcs, prev, remaining, (String[]) null));
      }
      return splits.toArray(new FileSplit[splits.size()]);
    }

    /** {@inheritDoc} */
    public RecordReader<Text, Text> getRecordReader(InputSplit split,
        JobConf job, Reporter reporter) throws IOException {
      return new SequenceFileRecordReader<Text, Text>(job,
          (FileSplit) split);
    }
  }

  static class DataFsckMapper implements Mapper<Text, Text, Text, Text> {
    private JobConf conf;
    private Reporter reporter = null;
    private int processedCount;

    /** {@inheritDoc} */
    public void configure(JobConf job) {
      this.conf = job;
    }

    /** {@inheritDoc} */
    public void close() throws IOException {
    }

    private String getCountString() {
      return "Processed " + processedCount + " files";
    }

    /** Run a FileOperation */
    public void map(
      Text key, Text val, OutputCollector<Text, Text> out, Reporter reporter)
        throws IOException {
      String file = key.toString();
      State state = null;
      try {
        Path path = new Path(file);
        FileSystem fs = path.getFileSystem(conf);

        FileStatus stat = null;
        stat = fs.getFileStatus(path);
        if (stat == null) {
          state = State.DATA_UNREADABLE;
        } else {
          boolean isCorrupt = checkAgainstParity(fs, stat);
          state = isCorrupt ?
                  State.DATA_NOT_MATCHING_PARITY :
                  State.DATA_MATCHING_PARITY;
        }
      } catch (IOException e) {
        LOG.error("Marking file as unreadable: " + file, e);
        state = State.DATA_UNREADABLE;
      }
      out.collect(key, new Text(state.toString()));
    }

    // Is corrupt?
    boolean checkAgainstParity(FileSystem fs, FileStatus stat) throws IOException {
      Codec code = null;
      ParityFilePair ppair = null;
      for (Codec codec: Codec.getCodecs()) {
        ppair = ParityFilePair.getParityFile(
          codec, stat, conf);
        if (ppair != null) {
          code = codec; 
          break;
        } 
      }
      if (code == null) {
        LOG.info("No parity for " + stat.getPath());
        return false;
      }
      int parityLength = code.parityLength;
      LOG.info("Checking file parity " + stat.getPath() +
        " against parity " + ppair.getPath());

      final long blockSize = stat.getBlockSize();
      int stripeLength = code.stripeLength;
      long stripeBytes = stripeLength * blockSize;
      int numStripes = (int)Math.ceil(stat.getLen() * 1.0 / stripeBytes);

      // Look at all stripes.
      for (int stripeIndex = 0; stripeIndex < numStripes; stripeIndex++) {
        for (boolean lastKB : new boolean[]{true, false}) {
          long shortest = shortestBlockLength(stripeIndex, stat, stripeLength);
          // Optimization - if all blocks are the same size, one check is enough.
          if (!lastKB) {
            if (shortest == blockSize) {
              continue;
            }
          }

          long lastOffsetInBlock = lastKB ? blockSize : shortest;
          if (lastOffsetInBlock < CHECKSIZE) {
            lastOffsetInBlock = CHECKSIZE;
          }
          byte[][] stripeBufs = new byte[stripeLength][];
          for (int i = 0; i < stripeLength; i++) {
            stripeBufs[i] = new byte[CHECKSIZE];
          }
          byte[] parityBuf = new byte[CHECKSIZE];
          byte[] actualParityBuf = new byte[CHECKSIZE];
          // Read CHECKSIZE bytes from all blocks in a stripe and parity.
          computeParity(conf, fs, stat, code, stripeIndex, stripeBufs,
            parityBuf, lastOffsetInBlock);

          readActualParity(ppair, actualParityBuf,
            stripeIndex, parityLength, blockSize, lastOffsetInBlock);

          if (!Arrays.equals(parityBuf, actualParityBuf)) {
            return true;
          }
        }
      }
      // All stripes are good.
      LOG.info("Checking file parity " + stat.getPath() +
        " against parity " + ppair.getPath() + " was OK");
      return false;
    }

    long shortestBlockLength(int stripeIndex, FileStatus stat, int stripeLength) {
      final long blockSize = stat.getBlockSize();
      final long stripeBytes = stripeLength * blockSize;
      int numStripes = (int) Math.ceil(stat.getLen() * 1.0 / stripeBytes);
      if (stripeIndex == numStripes - 1) {
        long remainder = stat.getLen() % blockSize;
        return (remainder == 0) ? blockSize : remainder;
      } else {
        return blockSize;
      }
    }

    void computeParity(Configuration conf, FileSystem fs, FileStatus stat,
          Codec code,
          final int stripeIndex, byte[][] stripeBufs, byte[] parityBuf,
          long lastOffsetInBlock) throws IOException {
      final long blockSize = stat.getBlockSize();
      final long stripeBytes = stripeBufs.length * blockSize;
      final long stripeStartOffset = stripeIndex * stripeBytes;
      final long stripeEndOffset = stripeStartOffset + stripeBytes;
      LOG.info("Checking parity " + stat.getPath() + " with last offset " + lastOffsetInBlock);

      FSDataInputStream[] inputs = new FSDataInputStream[stripeBufs.length];
      try {
        int idx = 0;
        // Loop through the blocks in the stripe
        for (long blockStart = stripeStartOffset;
             blockStart < stripeEndOffset;
             blockStart += blockSize) {
          // First zero out the buffer.
          Arrays.fill(stripeBufs[idx], (byte)0);
          if (blockStart < stat.getLen()) {
            // Block is real, read some bytes from it.
            long readEndOffset = blockStart + lastOffsetInBlock; // readEndOffset > blockStart.
            long readStartOffset = readEndOffset - CHECKSIZE; // readEndOffset > readStartOffset.
            // readStartOffset = blockStart + lastOffsetInBlock - CHECKSIZE, readStartOffset >= blockStartOffset
            // blockStartOffset <= readStartOffset < readEndOffset
            // Check for the case that the readEndOffset is beyond eof.
            long blockEndOffset = Math.min((blockStart + blockSize), stat.getLen());

            if (readStartOffset < blockEndOffset) {
              // blockStart <= readStartOffset < blockEndOffset
              inputs[idx] = fs.open(stat.getPath());
              inputs[idx].seek(readStartOffset);
              int bytesToRead = (int)Math.min(CHECKSIZE, blockEndOffset - readStartOffset);
              IOUtils.readFully(inputs[idx], stripeBufs[idx], 0, bytesToRead);
              // Rest is zeros
            }
          }
          idx++;
        }
        if (code.id.equals("xor")) {
          for (int i = 0; i < CHECKSIZE; i++) {
            parityBuf[i] = 0;
            // For XOR, each byte is XOR of all the stripe bytes.
            for (int j = 0; j < stripeBufs.length; j++) {
              parityBuf[i] = (byte)(parityBuf[i] ^ stripeBufs[j][i]);
            }
          }
        } else if (code.id.equals("rs")) {
          int parityLength = code.parityLength;
          int[] msgbuf = new int[stripeBufs.length];
          int[] codebuf = new int[parityLength];
          ErasureCode rsCode = new ReedSolomonCode(stripeBufs.length, parityLength);
          for (int i = 0; i < CHECKSIZE; i++) {
            for (int j = 0; j < stripeBufs.length; j++) {
              msgbuf[j] = stripeBufs[j][i] & 0x000000FF;
            }
            rsCode.encode(msgbuf, codebuf);
            // Take the first parity byte.
            parityBuf[i] = (byte)codebuf[0];
          }
        }
      } finally {
        for (InputStream stm: inputs) {
          if (stm != null) stm.close();
        }
      }
    }

    void readActualParity(
        ParityFilePair ppair, byte[] actualParityBuf,
        int stripeIndex, int parityLength, long blockSize,
        long lastOffsetInBlock) throws IOException {
      FSDataInputStream parityIn = ppair.getFileSystem().open(ppair.getPath());
      try {
        // Seek to the beginning of parity stripe.
        parityIn.seek(stripeIndex * parityLength * blockSize + lastOffsetInBlock - CHECKSIZE);
        // Parity blocks are always full, so we should be able to read CHECKSIZE bytes.
        IOUtils.readFully(parityIn, actualParityBuf, 0, CHECKSIZE);
      } finally {
        parityIn.close();
      }
    }
  }
}
