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

import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hdfs.GeneralConstant;

import java.io.IOException;
import java.util.Map;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.commons.logging.Log;
import java.util.zip.CRC32; 
import java.util.zip.Checksum;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.CompressionType;

@SuppressWarnings("deprecation")
  public class GenWriterThread extends GenThread implements
    GeneralConstant {
    private static final Log LOG = LogFactory.getLog(GenWriterThread.class);
    public static final String TEST_TYPE = "write"; 
    public static final long DEFAULT_ROLL_INTERVAL_SEC = -1;
    public static final long DEFAULT_SYNC_INTERVAL_SEC = -1;
    public static final long DEFAULT_MAX_TIME_SEC = 60;
    public static final String WRITER_ROLL_INTERVAL_KEY = "writer.roll.interval.sec";
    public static final String WRITER_SYNC_INTERVAL_KEY = "writer.sync.interval.sec";
    public static final String MAX_TIME_SEC_KEY = "max.time.sec";
  
   /**
    * Restricts the maximum rate at which tokens can be claimed and allows the maximum burst size
    * to be bounded.
    */
    public static class TokenBucket {
      private final long maxTokensPerSec;
      private final long maxTokenBurst;
      private long availableTokens = 0;
      private long lastCheckpointSecs = getNowSecs();
      private final Random rb = new Random();
      private static final int SLEEP_INTERVAL = 1000;

      public TokenBucket(long maxTokensPerSec, long maxTokenBurst) {
        this.maxTokensPerSec = maxTokensPerSec;
        this.maxTokenBurst = maxTokenBurst;
      }

      public TokenBucket(long maxTokensPerSec) {
        this(maxTokensPerSec, Long.MAX_VALUE);
      }

      private static long getNowSecs() {
        return System.currentTimeMillis() / 1000;
      }

      private void updateAvailableTokens() {
        long nowSecs = getNowSecs();
        availableTokens += maxTokensPerSec * (nowSecs - lastCheckpointSecs);
        // Bound the maximum number of available tokens
        availableTokens = Math.min(availableTokens, maxTokenBurst);
        lastCheckpointSecs = nowSecs;
      }

      /**
       * This method will block until the requested number of tokens become available
       *
       * @param requestedSize
       * @throws InterruptedException
       */
      public synchronized void getTokens(long requestedSize) throws InterruptedException {
        if (requestedSize > maxTokenBurst) {
          throw new IllegalArgumentException("Cannot request more tokens " +
              "then the max burst size");
        }

        updateAvailableTokens();
        while (requestedSize > availableTokens) {
          // Sleep until the tokens become available
          long tokensStillMissing = requestedSize - availableTokens;
          // Estimate how much time we need to wait to get enough tokens
          long sleepSecs = (long) Math.ceil((double) tokensStillMissing 
              / maxTokensPerSec) * 1000 + rb.nextInt(SLEEP_INTERVAL);
          Thread.sleep(sleepSecs);
          updateAvailableTokens();
        }
        availableTokens -= requestedSize;
        assert(availableTokens >= 0);
      }
    }

    public class GenWriterRunTimeConstants extends RunTimeConstants {
      String input = null;
      long roll_interval = DEFAULT_ROLL_INTERVAL_SEC * 1000;
      long sync_interval = DEFAULT_SYNC_INTERVAL_SEC * 1000;
      long max_time = DEFAULT_MAX_TIME_SEC * 1000;
      long data_rate = DEFAULT_DATA_RATE * 1024;
      String task_name = null;
    }
    
    //For each file, we compute the CRC32 checksum of it.
    //Then we XOR all files' CRC32 checksum to generate a directory 
    //checksum 
    public static class DirectoryChecksum {
      private Checksum fileckm = new CRC32();
      private long dirckm = 0L;
      public long getDirectoryChecksum() {
        return dirckm;
      }
      
      public Checksum getFileChecksum() {
        return fileckm;
      }
      
      public void openFile() {
        fileckm.reset(); 
      }
      
      public void closeFile() {
        dirckm ^= fileckm.getValue();
      }
    }
    
    private DirectoryChecksum dc = new DirectoryChecksum();
    private int id;
    private String name;
    private Random rb = new Random();
    public TokenBucket tb = null;
    public GenWriterRunTimeConstants rtc = null;
    
    public GenWriterThread() {
    }
    
    public GenWriterThread(Configuration conf, Path p, String name, int id, 
        GenWriterRunTimeConstants rtc) throws IOException{
      super(conf, p, rtc);
      this.rtc = rtc;
      this.id = id;
      this.name = name;
      rb.nextBytes(buffer);
      tb = new TokenBucket(rtc.data_rate);
    }
      
    public void run() {
      try {
        fs.mkdirs(outputPath); 
        long endTime = System.currentTimeMillis() + rtc.max_time;
        long lastRollTime = System.currentTimeMillis();
        long lastSyncTime = System.currentTimeMillis();
        long currentId = 0;
        FSDataOutputStream out = null;
        while (System.currentTimeMillis() < endTime) {
          Path fileName = new Path(outputPath, "part" + currentId);
          try { 
            out = fs.create(fileName, (short)3);
            dc.openFile();

            long size = 0;
            while (true) {
              rb.nextBytes(buffer);
              dc.getFileChecksum().update(buffer, 0, rtc.buffer_size);
              tb.getTokens(rtc.buffer_size);
              out.write(buffer, 0, rtc.buffer_size);
              
              size += rtc.buffer_size;
              if (rtc.sync_interval > 0 &&
                  System.currentTimeMillis() - lastSyncTime > rtc.sync_interval) {
                // Sync the file
                out.sync();
                LOG.info("file " + fileName + " is synced");
                lastSyncTime = System.currentTimeMillis() + 
                    rb.nextInt((int)rtc.sync_interval);
              }
              if (System.currentTimeMillis() > endTime ||
                  rtc.roll_interval > 0 &&
                  System.currentTimeMillis() - lastRollTime > rtc.roll_interval) {
                // Roll the file
                out.close();
                out = null;
                currentId++;
                files_processed++;
                processed_size += size;
                LOG.info("file " + fileName + " is closed with " + size + " bytes");
                lastRollTime = System.currentTimeMillis() + 
                    rb.nextInt((int)rtc.roll_interval);
                break;
              }
            }
          } catch (Exception e) {
            LOG.error("Error in writing file: " + fileName, e);
            this.errors.add(e);
          } finally {
            IOUtils.closeStream(out);
            dc.closeFile();
          }
        }
        LOG.info("Checksum of files under dir " + outputPath + " is " + dc.getDirectoryChecksum());
        LOG.info("Thread " + name + "_" + id + " is done.");
      } catch (Exception ioe) {
        LOG.error("Error:", ioe);
        this.errors.add(ioe);
      }
    }
    
    
    /**
     * Each mapper will write one checksum file. 
     * checksum file contains N pairs where N is the number of threads
     * Each pair is has two entries: outputPath and checksum
     * outputPath is the directory of files written by the thread
     * checksum is the CRC checksum of all files under that directory
     * @param name checksum file name
     * @param threads array of writer threads
     * @return checksum file path
     * @throws IOException
     */
    private Path writeChecksumFile(FileSystem fs, String name, 
        GenThread[] threads) throws IOException {
      Path checksumFile = new Path(rtc.output_dir, name + ".checksum");
      SequenceFile.Writer write = null;
      write = SequenceFile.createWriter(fs, fs.getConf(), checksumFile,
          Text.class, Text.class, CompressionType.NONE);
      try {
        for (GenThread rawThread: threads) {
          GenWriterThread thread = (GenWriterThread)rawThread;
          write.append(new Text(thread.outputPath.toString()), 
              new Text(Long.toString(thread.dc.getDirectoryChecksum())));
        } 
      } finally {
        if (write != null)
          write.close();
        write = null;
      }
      return checksumFile;
    }
    
    /**
     * This is used for verification
     * Each mapper writes one control file
     * control file only contains the base directory written by this mapper
     * and the checksum file path so that we could create a Read mapper which
     * scanned the files under the base directory and verify the checksum of 
     * files with the information given in the checksum file. 
     * @param fs 
     * @param outputPath base directory of mapper
     * @param checksumFile location of checksum file
     * @param name name of control file
     * @throws IOException
     */
    private void writeControlFile(FileSystem fs, Path outputPath, 
        Path checksumFile, String name) throws IOException {
      SequenceFile.Writer write = null;
      try {
        Path parentDir = new Path(rtc.input, "filelists");
        if (!fs.exists(parentDir)) {
          fs.mkdirs(parentDir);
        }
        Path controlFile = new Path(parentDir, name);
        write = SequenceFile.createWriter(fs, fs.getConf(), controlFile,
            Text.class, Text.class, CompressionType.NONE);
        write.append(new Text(outputPath.toString()), 
            new Text(checksumFile.toString()));
      } finally {
        if (write != null)
          write.close();
        write = null;
      }
    }
    
    /**
     * Create a number of threads to generate write traffics
     * @param conf
     * @param key name of the mapper 
     * @param value location of data input  
     * @return
     * @throws IOException
     */
    @Override
    public GenThread[] prepare(JobConf conf, Text key, Text value)
        throws IOException {
      this.rtc = new GenWriterRunTimeConstants();
      super.prepare(conf, key, value, rtc);
      rtc.task_name = key.toString() + rtc.taskID;
      rtc.roll_interval = conf.getLong(WRITER_ROLL_INTERVAL_KEY, 
          DEFAULT_ROLL_INTERVAL_SEC) * 1000;
      rtc.sync_interval = conf.getLong(WRITER_SYNC_INTERVAL_KEY, 
          DEFAULT_SYNC_INTERVAL_SEC) * 1000;
      rtc.max_time = conf.getLong(MAX_TIME_SEC_KEY, DEFAULT_MAX_TIME_SEC) * 1000;
      rtc.data_rate = conf.getLong(WRITER_DATARATE_KEY, DEFAULT_DATA_RATE) * 1024;
      rtc.input = value.toString();
      LOG.info("data rate: " + rtc.data_rate);
      GenWriterThread[] threads = new GenWriterThread[(int)rtc.nthreads];
      for (int i=0; i<rtc.nthreads; i++) {
        threads[i] = new GenWriterThread(conf, 
            new Path(new Path(rtc.input, rtc.task_name), 
                rtc.task_name + "_" + i), rtc.task_name, i, rtc);
      }
      return threads;
    }
    
    @Override
    public Map<String, String> collectStats(JobConf conf, 
        GenThread[] threads, long execTime) throws IOException { 
      // write checksum file
      FileSystem fs = FileSystem.newInstance(conf);
      Path checksumFile = writeChecksumFile(fs, rtc.task_name, threads);
      writeControlFile(fs, new Path(rtc.input, rtc.task_name), checksumFile, 
          rtc.task_name);
      return super.collectStats(conf, threads, execTime);
    }
  }
