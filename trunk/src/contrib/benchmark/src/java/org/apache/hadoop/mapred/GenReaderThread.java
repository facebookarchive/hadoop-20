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
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.commons.logging.Log;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapred.GenWriterThread.DirectoryChecksum;

@SuppressWarnings("deprecation")
  public class GenReaderThread extends GenThread implements
      GeneralConstant {
    private static final Log LOG = LogFactory.getLog(GenReaderThread.class);
    public static final String TEST_TYPE = "read"; 
    // for reduce
    private long total_open_files = 0;
    private ArrayList<String> corrupt_dir = null;
    
    // Thread specific variables 
    private long checksum;
    private boolean verifyChecksum = false;
    public long open_files = 0;
    public boolean isCorrupt = false;
    public DirectoryChecksum dc = new DirectoryChecksum();
    public RunTimeConstants rtc = null;
    
    public GenReaderThread() {
    }
    
    public GenReaderThread(Configuration conf, Path p, long checksum,
        boolean verifyChecksum, RunTimeConstants rtc) throws IOException{
      super(conf, p, null, rtc);
      this.rtc = rtc;
      this.checksum = checksum;
      this.verifyChecksum = verifyChecksum;
    }
    
    /**
     * Create a number of threads to generate read traffics
     * @param conf
     * @param key directory of files to read
     * @param value checksum file locaiton 
     * @return
     * @throws IOException
     */
    @Override
    public GenThread[] prepare(JobConf conf, Text key, Text value)
        throws IOException {
      this.rtc = new RunTimeConstants();
      super.prepare(conf, key, value, rtc);
      Path basePath = new Path(key.toString());
      LOG.info("base path is " + basePath);
      Path checksumPath = null;
      FileSystem fs = FileSystem.newInstance(conf);
      if (value.toString().length() != 0) {
        checksumPath = new Path(value.toString());
      }
      HashMap<String, Long> checksumMap = null;
      boolean verifyChecksum = false;
      if (fs.exists(checksumPath)) {
        LOG.info("checksum path is " + checksumPath);
        verifyChecksum = true;
        checksumMap = new HashMap<String, Long>();
        SequenceFile.Reader reader = null;
        try {
          reader = new SequenceFile.Reader(fs, checksumPath, conf);
          Writable dir = (Writable) ReflectionUtils.newInstance(
            reader.getKeyClass(), conf);
          Writable checksum = (Writable) ReflectionUtils.newInstance(
            reader.getValueClass(), conf);
          while(reader.next(dir, checksum)) {
            LOG.info("dir: " + dir.toString() + " checksum: " + checksum);
            checksumMap.put(
                fs.makeQualified(new Path(dir.toString())).toUri().getPath(),
                Long.parseLong(checksum.toString()));
          }
        } catch(Exception e) {
          LOG.error(e);
          throw new IOException(e);
        } finally {
          IOUtils.closeStream(reader);
        }
      }
      
      FileStatus[] baseDirs = fs.listStatus(basePath);
      if (rtc.nthreads != baseDirs.length) {
        throw new IOException("Number of directory under " + basePath + 
            "(" + baseDirs.length + ") doesn't match number of threads " + 
            "(" + rtc.nthreads + ").");
      }
      
      GenReaderThread[] threads = new GenReaderThread[(int)rtc.nthreads];

      for (int i=0; i < rtc.nthreads; i++) {
        long checksum = 0;
        if (verifyChecksum) {
          String basePathStr = baseDirs[i].getPath().toUri().getPath();
          checksum = checksumMap.get(basePathStr);
        }
        threads[i] = new GenReaderThread(conf, baseDirs[i].getPath(), 
            checksum, verifyChecksum, rtc);
      }
      return threads;
    }
    
    @Override
    public Map<String, String> collectStats(JobConf conf, GenThread[] threads, 
        long execTime) throws IOException {
      long total_opened_files = 0;
      String corruptFiles = "";
      for (Thread rawThread: threads) {
        GenReaderThread thread = (GenReaderThread)rawThread;
        total_opened_files += thread.open_files;
        if (thread.isCorrupt) {
          corruptFiles += thread.inputPath.toString() + " ";
        }
      }
      LOG.info("Number of open files = " + total_opened_files);
      Map<String, String> stat = super.collectStats(conf, threads, execTime);
      stat.put("openfiles", String.valueOf(total_opened_files));
      stat.put("corruptdirs", corruptFiles);
      return stat;
    }

    @Override
    public void run() {
      try {
        FileStatus[] files = fs.listStatus(inputPath);
        for (FileStatus file : files) {
          if (verifyChecksum) {
            dc.openFile();
          }
          FSDataInputStream in = null;
          try {
            in = fs.open(file.getPath());
            if (in.isUnderConstruction()) {
              open_files++;
              LOG.info("file " + file.getPath() + " is still open");
              continue;
            }
            int size = 0;
            while (true) {
              size = in.read(buffer, 0, rtc.buffer_size);
              if (size <= 0) {
                break;
              }
              processed_size += size;
              if (verifyChecksum) {
                dc.getFileChecksum().update(buffer, 0, size);
              }
            }
          } catch (Exception e) {
            LOG.error("Error in reading file " + file.getPath(), e);
            this.errors.add(e);
          } finally {
            IOUtils.closeStream(in);
          }
          files_processed++;
          if (verifyChecksum) {
            dc.closeFile();
          }
        }
        LOG.info("Directory " + inputPath + " is scanned with checksum " 
            + dc.getDirectoryChecksum());
        this.isCorrupt = open_files > 0;
        if (verifyChecksum)
          this.isCorrupt = this.isCorrupt || dc.getDirectoryChecksum() != checksum;
      } catch (Exception ioe) {
        LOG.error("Error:", ioe);
        this.errors.add(ioe);
      }
    }
    
    @Override
    public void reset() {
      total_open_files = 0;
      corrupt_dir = new ArrayList<String>();
    }

    @Override
    public void analyze(Map<String, String> stat) throws IOException {
      total_open_files += Long.parseLong(stat.get("openfiles"));
      String[] files = stat.get("corruptdirs").split(" ");
      for (String file : files) {
        if (file != null && file.length() > 0)
          corrupt_dir.add(file);
      }
    }
    
    @Override
    public void output(FSDataOutputStream out) throws IOException {
      out.writeChars("Number of open files:\t\t\t" + total_open_files + "\n");
      out.writeChars("Number of corrupt dirs:\t\t\t" + corrupt_dir.size() + "\n");
      if (corrupt_dir.size() > 0) {
        out.writeChars("-----------------------------\n");
        out.writeChars("Corrupt Dirs:\n");
        out.writeChars("-----------------------------\n");
        for (String file : corrupt_dir) {
          out.writeChars(file + "\n");
        }
      }
    }
  }
