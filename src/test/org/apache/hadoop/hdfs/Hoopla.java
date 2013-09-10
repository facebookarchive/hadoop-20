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
package org.apache.hadoop.hdfs;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Random;
import java.lang.Math;
import java.lang.Integer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.impl.Log4JLogger;
import org.apache.log4j.Level;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.server.datanode.DataNode;

/**
 * Performance benchmark of positional read for HDFS
 */
public class Hoopla extends Configured implements Tool {

  static final Log LOG = LogFactory.getLog(Hoopla.class);
  static final long seed = 0xDEADBEEFL; // seed for random data generation

  // default values for benchmark
  static private Path fileName = new Path("/benchmark/preadBenchmark.dat");
  static private long blockSize =  1024 * 1024; // 1 megabytes
  static private long fileSize = 8 * blockSize;
  static private int numThreads = 100;
  static private int numIterationsPerThread = 10000;
  static private short replication = 1;
  static private boolean doScatterGatherPread = true;
  static private boolean useLocal = true;
  static private boolean verifyChecksum = false;


  // switch off all logging so that the benchmark runs in constant time
  {
    DataNode.LOG.getLogger().setLevel(Level.WARN);
    ((Log4JLogger)DFSClient.LOG).getLogger().setLevel(Level.WARN);
    ((Log4JLogger)DataNode.ClientTraceLog).getLogger().setLevel(Level.WARN);
  }

  public static void printUsage() {
    System.out.println("USAGE: bin/hadoop hadoop-*.jar Hoopla [-noscattergather] [-fileName] " +
                       "[-fileSize] [-blockSize] [-numThreads] [-numIterationsPerThread] " +
                       "[-nolocal] [-verifychecksum]");
    System.exit(0);
  }

  // write specified bytes to file.
  static void writeFile(FSDataOutputStream stm, long fileSize) throws IOException {
    byte[] buffer = new byte[(int)blockSize];
    for (int i = 0; i < fileSize; i++) {
      int remaining = (int)Math.min(fileSize, blockSize);
      stm.write(buffer, 0, remaining);
      fileSize -= remaining;
    }
  }
  
  @Override
  public int run(String[] args) throws IOException {

    long startTime = System.currentTimeMillis();
    for (int i = 0; i < args.length; i++) {
      if (args[i].equals("-noscattergather")) {
        doScatterGatherPread = false;
      } else if (args[i].equals("-nolocal")) {
        useLocal = false;
      } else if (args[i].equals("-verifychecksum")) {
        verifyChecksum = true;
      } else if (args[i].equals("-fileName")) {
        fileName = new Path(args[++i]);
      } else if (args[i].equals("-blocksize")) {
        blockSize = Long.parseLong(args[++i]); 
      } else if (args[i].equals("-numIterationsPerThread")) {
        numIterationsPerThread = Integer.parseInt(args[++i]);
      } else if (args[i].equals("-fileSize")) {
        fileSize = Long.parseLong(args[++i]);
      } else if (args[i].equals("-numThreads")) {
        numThreads = Integer.parseInt(args[++i]); 
      } else {
        printUsage();
      }
    }
    LOG.info("Starting test with scattergather " + doScatterGatherPread +
             " fileName " + fileName +
             " fileSize " + fileSize +
             " blockSize " + blockSize +
             " numIterationsPerThread " + numIterationsPerThread +
             " numThreads " + numThreads);

    // create configuration. Switch off periodic block scanner to reduce
    // jitter. Make the number of threads in the datanode twice as the
    // number of threads in this client benchmark so that the benchmark 
    // is not impacted by unavailabity of server threads.
    Configuration conf = new Configuration();
    conf.setInt("dfs.datanode.scan.period.hours", -1); // disable periodic scanner
    conf.setInt("dfs.datanode.max.xcievers",
      Math.max(100, (int)(numThreads * 1.2))); // datanode threads
    conf.setBoolean("dfs.read.shortcircuit", useLocal);

    // create cluster
    MiniDFSCluster cluster = new MiniDFSCluster(conf, 1, true, null);
    FileSystem fs = cluster.getFileSystem(0);
    fs.setVerifyChecksum(verifyChecksum);

    // create a single large file
    FSDataOutputStream stm = fs.create(fileName, true,
                                       fs.getConf().getInt("io.file.buffer.size", 4096),
                                       (short)replication, (long)blockSize);
    Hoopla.writeFile(stm, fileSize);
    stm.close();

    // check that file is indeed the right size
    if (fs.getFileStatus(fileName).getLen() != fileSize) {
      String msg = "Bad file size, got " + fs.getFileStatus(fileName).getLen() +
                   " expected " + fileSize;
      LOG.warn(msg);
      throw new IOException(msg);
    }

    // open file for reading
    FSDataInputStream fin = fs.open(fileName);

    // Do a first read so that the data is read into cache. Do not measure the time
    // for this read call.
    List<ByteBuffer> rlist = fin.readFullyScatterGather(0, 10000);

    // create all threads
    Putter[] all = new Putter[numThreads];
    for (int i = 0; i < numThreads; i++) {
      all[i] = new Putter(fin, fileName, fileSize, i, numIterationsPerThread);
    }

    // run all threads
    for (int i = 0; i < numThreads; i++) {
      all[i].start();
    }

    // wait for all threads to finish
    long totalTime = 0;
    for (int i = 0; i < numThreads; i++) {
      try {
        all[i].join();
        totalTime += all[i].getElapsedTime();
      } catch (InterruptedException e) {
        LOG.warn("Hoopla encountered InterruptedException." +
                 " Ignoring....", e);
      }
    }
    LOG.info("Hoopla is a grand success! Total time in ms = " + totalTime +
             " ms/read = " + (totalTime/(numIterationsPerThread*numThreads)));
    return 0;
  }

  /**
   * A thread that makes a few pread calls
   */
  public static class Putter extends Thread {

    private FSDataInputStream fin;
    private final Path filename;
    private final int threadNumber;
    private final int numOps;
    private final long fileSize;
    private long elapsedTime;

    public Putter(FSDataInputStream fin, Path filename, long fileSize, int threadNumber, int numOps) {
      this.fin = fin;
      this.filename = filename;
      this.fileSize = fileSize;
      this.threadNumber = threadNumber;
      this.numOps = numOps;
      this.elapsedTime = 0;
      setDaemon(true);
    }

    @Override
    public void run() {
    
      byte[] buffer = new byte[10000];

      // iterate for the specified number of operations
      for (int i = 0; i < numOps; i++) {
        try {
          if (doScatterGatherPread) { 
            long start = System.currentTimeMillis();
            List<ByteBuffer> rlist = fin.readFullyScatterGather(0, 10000);
            elapsedTime += System.currentTimeMillis() - start;
          } else {
            long start = System.currentTimeMillis();
            fin.readFully(0, buffer);
            elapsedTime += System.currentTimeMillis() - start;
          }
        } catch (Throwable e) {
          String msg = "Thread id " + threadNumber + " operation " + i + " failed. " +
                       StringUtils.stringifyException(e);
          LOG.warn(msg);
        }
      }
    }

    // returns the elapsed time of this thread
    long getElapsedTime() {
      return elapsedTime;
    }
  }

  public static void main(String[] args) throws Exception {
    System.exit(ToolRunner.run(new Hoopla(), args));
  }
}
