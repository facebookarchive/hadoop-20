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

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.impl.Log4JLogger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Level;

import static org.junit.Assert.*;

public class LocalReadWritePerf extends Configured implements Tool {
  static final Log LOG = LogFactory.getLog(LocalReadWritePerf.class);

  final static String TEST_DIR = new File(System.getProperty("test.build.data",
      "benchmarks/TestLocalReadWrite")).getAbsolutePath();
  
  {
    ((Log4JLogger)DataNode.ClientTraceLog).getLogger().setLevel(Level.OFF);
  }

  final int NUM_DATANODES = 3;
  final int MAX_BUF_SIZE = 1024 * 1024;
  Random rand = null;

  Configuration conf;
  MiniDFSCluster cluster = null;
  DFSClient client = null;
  FileSystem fs = null;
  
  private int nThreads = 4;
  private int fileSizeKB = 256;
  private int nIterations = 1024;
  private long blockSize = -1;
  private boolean shortCircuit = false;
  private boolean verifyChecksum = true;
  private boolean enableInlineChecksum = false;

  private class TestFileInfo {
    public FSDataInputStream dis;
    public FileInputStream fis;
    public boolean localFile;
    public String filePath;
    public long fileSize;
  }

  private void setupCluster() throws Exception {
    conf = new Configuration();
    new File(TEST_DIR).mkdirs(); // Make sure data directory exists
    conf.setBoolean("dfs.use.inline.checksum", enableInlineChecksum);
    conf.setBoolean("dfs.read.shortcircuit", shortCircuit);
    
    cluster = new MiniDFSCluster(conf, NUM_DATANODES, true, null);
    InetSocketAddress nnAddr = new InetSocketAddress("localhost",
        cluster.getNameNodePort());
    client = new DFSClient(nnAddr, conf);
    rand = new Random(System.currentTimeMillis());
    fs = cluster.getFileSystem();
    
    fs.setVerifyChecksum(verifyChecksum);
    
    if (blockSize <= 0) {
      blockSize = fs.getDefaultBlockSize();
    }
  }

  private void tearDownCluster() throws Exception {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  /**
   * Write a hdfs file with replication factor 1
   */
  private void writeFile(Path filePath, long sizeKB) throws IOException {
    // Write a file with the specified amount of data
    FSDataOutputStream os = fs.create(filePath, true, 
        getConf().getInt("io.file.buffer.size", 4096), 
        (short)1, blockSize);
    long fileSize = sizeKB * 1024;
    int bufSize = (int) Math.min(MAX_BUF_SIZE, fileSize);
    byte[] data = new byte[bufSize];
    long toWrite = fileSize;
    rand.nextBytes(data);
    while (toWrite > 0) {
      int len = (int) Math.min(toWrite, bufSize);
      os.write(data, 0, len);
      toWrite -= len;
    }
    os.sync();
    os.close();
  }
  
  /**
   * Append to a hdfs file.
   */
  private long appendFile(Path filePath, long sizeKB) throws IOException {
    long start = System.nanoTime();
    FSDataOutputStream os = fs.append(filePath);
    long toWrite = sizeKB * 1024;
    int bufSize = (int) Math.min(MAX_BUF_SIZE, toWrite);
    byte[] data = new byte[bufSize];
    rand.nextBytes(data);
    while (toWrite > 0) {
      int len = (int) Math.min(toWrite, bufSize);
      os.write(data, 0, len);
      toWrite -= len;
    }
    long appendTime = System.nanoTime() - start;
    //os.sync();
    os.close();
    
    return appendTime;
  }

  /**
   * write a local dist file
   */
  private void writeLocalFile(File filePath, long sizeKB) throws IOException {
    BufferedOutputStream os = 
        new BufferedOutputStream(new FileOutputStream(filePath));
    long fileSize = sizeKB * 1024;
    int bufSize = (int) Math.min(MAX_BUF_SIZE, fileSize);
    byte[] data = new byte[bufSize];
    long toWrite = fileSize;
    rand.nextBytes(data);
    while (toWrite > 0) {
      int len = (int) Math.min(toWrite, bufSize);
      os.write(data, 0, len);
      toWrite -= len;
    }
    os.flush();
    os.close();
  }

  class WriteWorker extends Thread {
    private TestFileInfo testInfo;
    private long bytesWrite;
    private boolean error;
    private boolean isAppend;

    WriteWorker(TestFileInfo testInfo, int id) {
      this(testInfo, id, false);
    }
    
    WriteWorker(TestFileInfo testInfo, int id, boolean isAppend) {
      super("WriteWorker-" + id);
      this.testInfo = testInfo;
      bytesWrite = 0;
      error = false;
      this.isAppend = isAppend;
    }

    @Override
    public void run() {
      try {
        if (isAppend) {
          FSDataOutputStream out = 
              fs.create(new Path(testInfo.filePath), true,
                  getConf().getInt("io.file.buffer.size", 4096), 
                  (short)3, blockSize);
          out.close();
        }
      } catch (IOException ex) {
        LOG.error(getName() + ": Error while testing write", ex);
        error = true;
        fail(ex.getMessage());
      }
      long appendTime = 0;
      for (int i=0; i < nIterations; i++) {
        try {
          if (testInfo.localFile) {
            writeLocalFile(new File(testInfo.filePath + "_" + i), 
                testInfo.fileSize / 1024);
          } else {
            if (isAppend) {
              appendTime += appendFile(new Path(testInfo.filePath), testInfo.fileSize / 1024);
            } else {
              writeFile(new Path(testInfo.filePath + "_" + i), 
                testInfo.fileSize / 1024);
            }
          }
          bytesWrite += testInfo.fileSize;
        } catch (IOException ex) {
          LOG.error(getName() + ": Error while testing write", ex);
          error = true;
          fail(ex.getMessage());
        }
      }
      if (isAppend) {
        System.out.println("Time spent in append data: " + appendTime);
      }
    }

    public long getBytesWrite() {
      return bytesWrite;
    }

    /**
     * Raising error in a thread doesn't seem to fail the test. So check
     * afterwards.
     */
    public boolean hasError() {
      return error;
    }
  }

  class ReadWorker extends Thread {
    private TestFileInfo testInfo;
    private long bytesRead;
    private boolean error;
    private int nIterations = 1024;
    private int toReadLen;

    ReadWorker(TestFileInfo testInfo, int nIterations, int toReadLen, int id) {
      super("ReadWorker-" + id);
      this.testInfo = testInfo;
      this.nIterations = nIterations;
      bytesRead = 0;
      error = false;
      this.toReadLen = toReadLen;
    }

    /**
     * Randomly do pRead.
     */
    @Override
    public void run() {
      for (int i = 0; i < nIterations; i++) {
        long fileSize = testInfo.fileSize;
        int startOff = rand.nextInt((int) fileSize - toReadLen);
        try {
          pRead(testInfo, startOff, toReadLen);
          bytesRead += toReadLen;
        } catch (Exception ex) {
          LOG.error(getName() + ": Error while testing read at " + startOff
              + " length " + toReadLen, ex);
          error = true;
          fail(ex.getMessage());
        }
      }
    }

    public int getIterations() {
      return nIterations;
    }

    public long getBytesRead() {
      return bytesRead;
    }

    /**
     * Raising error in a thread doesn't seem to fail the test. So check
     * afterwards.
     */
    public boolean hasError() {
      return error;
    }

    /**
     * Positional read.
     */
    private void pRead(TestFileInfo testInfo, int start, int len)
        throws Exception {
      long fileSize = testInfo.fileSize;
      assertTrue("Bad args" + start + " + " + len + " should be < " + fileSize,
          start + len < fileSize);

      if (!testInfo.localFile) {
        byte buf[] = new byte[len];
        FSDataInputStream dis = testInfo.dis;
        int cnt = 0;
        while (cnt < len) {
          cnt += dis.read(start, buf, cnt, buf.length - cnt);
        }
      } else {
        ByteBuffer buffer = ByteBuffer.allocate(len);
        FileInputStream fis = testInfo.fis;
        FileChannel fc = fis.getChannel();
        int cnt = 0;
        while (cnt < len) {
          cnt += fc.read(buffer, start);
        }
      }
    }
  }

  private boolean doReadSameFile(boolean local) throws IOException {

    // read one same file.
    ReadWorker[] workers = new ReadWorker[nThreads];

    TestFileInfo sameInfo = new TestFileInfo();
    sameInfo.localFile = local;

    if (local) {
      sameInfo.filePath = TEST_DIR + "/TestParallelRead.dat0";
      writeLocalFile(new File(sameInfo.filePath),
          fileSizeKB);
      sameInfo.fileSize = fileSizeKB * 1024;
    } else {
      Path filePath = new Path("/TestParallelRead.dat0");
      sameInfo.filePath = filePath.toString();
      writeFile(filePath, fileSizeKB);
      sameInfo.fileSize = fileSizeKB * 1024;
    }
    
    int toReadLen = (int) Math.min(sameInfo.fileSize / 2, 1024 * 1024);

    for (int i = 0; i < nThreads; i++) {
      TestFileInfo testInfo = new TestFileInfo();
      testInfo.localFile = sameInfo.localFile;
      testInfo.filePath = sameInfo.filePath;
      testInfo.fileSize = sameInfo.fileSize;
      if (local) {
        testInfo.fis = new FileInputStream(testInfo.filePath);
      } else {
        testInfo.dis = fs.open(new Path(testInfo.filePath));
      }
      workers[i] = new ReadWorker(testInfo, nIterations, toReadLen, i);
    }

    long startTime = System.currentTimeMillis();
    // start the workers and wait
    for (ReadWorker worker : workers) {
      worker.start();
    }

    for (ReadWorker worker : workers) {
      try {
        worker.join();
      } catch (InterruptedException e) {
      }
    }

    long endTime = System.currentTimeMillis();

    // Cleanup
    for (ReadWorker worker : workers) {
      TestFileInfo testInfo = worker.testInfo;
      if (local) {
        testInfo.fis.close();
      } else {
        testInfo.dis.close();
      }
    }

    // Report
    boolean res = true;
    long totalRead = 0;
    String report = "";
    if (local) {
      report = "--- Local Read Report: ";
    } else {
      report = "--- DFS Read Report: ";
    }
    for (ReadWorker worker : workers) {
      long nread = worker.getBytesRead();
      LOG.info(report + worker.getName() + " read " + nread + " B; "
          + "average " + nread / worker.getIterations() + " B per read;");
      totalRead += nread;
      if (worker.hasError()) {
        res = false;
      }
    }

    double timeTakenSec = (endTime - startTime) / 1000.0;
    long totalReadKB = totalRead / 1024;
    long totalReadOps = nIterations * nThreads;
    System.out.println(report + nThreads + " threads read " + totalReadKB
        + " KB (across " + 1 + " file(s)) in " + timeTakenSec + "s; average "
        + totalReadKB / timeTakenSec + " KB/s; ops per second: " + 
        totalReadOps / timeTakenSec);

    return res;
  }

  private boolean doReadDifferentFiles(boolean local) throws IOException {

    // read one same file.
    int toReadLen = Math.min(fileSizeKB * 1024 / 2, 1024 * 1024);
    
    ReadWorker[] workers = new ReadWorker[nThreads];
    for (int i = 0; i < nThreads; i++) {
      TestFileInfo testInfo = new TestFileInfo();

      if (local) {
        testInfo.localFile = true;
        testInfo.filePath = TEST_DIR + "/TestParallelRead.dat" + i;
        writeLocalFile(new File(testInfo.filePath),
            fileSizeKB);
        testInfo.fis = new FileInputStream(testInfo.filePath);
        testInfo.fileSize = fileSizeKB * 1024;
      } else {
        testInfo.localFile = false;
        Path filePath = new Path("/TestParallelRead.dat" + i);
        testInfo.filePath = filePath.toString();
        writeFile(filePath, fileSizeKB);
        testInfo.dis = fs.open(filePath);
        testInfo.fileSize = fileSizeKB * 1024;
      }

      workers[i] = new ReadWorker(testInfo, nIterations, toReadLen, i);
    }

    long startTime = System.currentTimeMillis();
    // start the workers and wait
    for (ReadWorker worker : workers) {
      worker.start();
    }

    for (ReadWorker worker : workers) {
      try {
        worker.join();
      } catch (InterruptedException e) {
      }
    }

    long endTime = System.currentTimeMillis();

    // Cleanup
    for (ReadWorker worker : workers) {
      TestFileInfo testInfo = worker.testInfo;
      if (local) {
        testInfo.fis.close();
      } else {
        testInfo.dis.close();
      }
    }

    // Report
    boolean res = true;
    long totalRead = 0;
    String report = "";
    if (local) {
      report = "--- Local Read Different files Report: ";
    } else {
      report = "--- DFS Read Different files Report: ";
    }
    for (ReadWorker worker : workers) {
      long nread = worker.getBytesRead();
      LOG.info(report + worker.getName() + " read " + nread + " B; "
          + "average " + nread / worker.getIterations() + " B per read");
      totalRead += nread;
      if (worker.hasError()) {
        res = false;
      }
    }

    double timeTakenSec = (endTime - startTime) / 1000.0;
    long totalReadKB = totalRead / 1024;
    long totalReadOps = nIterations * nThreads;
    System.out.println(report + nThreads + " threads read " + totalReadKB
        + " KB (across " + nThreads + " file(s)) in " + timeTakenSec
        + "s; average " + totalReadKB / timeTakenSec + 
        " KB/s; ops per second: " + 
        totalReadOps / timeTakenSec);

    return res;
  }

  private boolean doWriteFile(boolean local, boolean isAppend)
      throws IOException {
    WriteWorker[] workers = new WriteWorker[nThreads];
    for (int i = 0; i < workers.length; i++) {
      TestFileInfo testInfo = new TestFileInfo();

      if (local) {
        testInfo.localFile = true;
        testInfo.filePath = TEST_DIR + "/TestParallelRead.dat" + i;
        testInfo.fileSize = fileSizeKB * 1024;
      } else {
        testInfo.localFile = false;
        Path filePath = new Path("/TestParallelRead.dat" + i);
        testInfo.filePath = filePath.toString();
        testInfo.fileSize = fileSizeKB * 1024;
      }

      workers[i] = new WriteWorker(testInfo, i, isAppend);
    }

    long startTime = System.currentTimeMillis();
    // start the workers and wait
    for (WriteWorker worker : workers) {
      worker.start();
    }

    for (WriteWorker worker : workers) {
      try {
        worker.join();
      } catch (InterruptedException e) {
      }
    }

    long endTime = System.currentTimeMillis();

    // Report
    boolean res = true;
    long totalWrite = 0;
    String report = "";
    if (local) {
      report = "--- Local Write files Report: ";
    } else {
      report = "--- DFS " + (isAppend ? "Append" : "Write") +" files Report: ";
    }
    for (WriteWorker worker : workers) {
      long nwrite = worker.getBytesWrite();
      LOG.info(report + worker.getName() + " write " + nwrite + " B.");
      totalWrite += nwrite;
      if (worker.hasError()) {
        res = false;
      }
    }

    double timeTakenSec = (endTime - startTime) / 1000.0;
    long totalWriteKB = totalWrite / 1024;
    long totalWriteOps = nThreads * nIterations;
    System.out.println(report + nThreads + " threads write " + totalWriteKB
        + " KB (across " + totalWriteOps
        + " file(s)) in " + timeTakenSec
        + "s; average " + totalWriteKB / timeTakenSec + 
        " KB/s; ops per second: " + 
        totalWriteOps / timeTakenSec);

    return res;
  }

  public static void main(String[] argv) throws Exception {
    Thread.sleep(5000);
    System.exit(ToolRunner.run(new LocalReadWritePerf(), argv));
  }
  
  private void printUsage() {
    System.out
    .println("USAGE: bin/hadoop jar hadoop-*test.jar TestLocalReadWrite \n"
        + "operator: 0 -- read from same file \n "
        + "\t1 -- read from different files \n "
        + "\t2 -- write to different files\n"
        + "\t3 -- append to files\n"
        + "[-n nThreads]   number of reader/writer threads (4 by default)\n"
        + "[-f fileSizeKB]   the size of each test file in KB (256 by default)\n"
        + "[-b blockSizeKB]  the size of the block in KB \n"
        + "[-shortcircuit]    enable short circuit\n"
        + "[-disablechecksum]        disable the checksum verification (enable by default)\n"
        + "[-inlinechecksum]        enable the inline checksum (disabled by defalut)\n"
        + "[-r readIterations]   how many times we will read the file in each thread (1024 by default)");
  }

  @Override
  public int run(String[] args) throws Exception {
    int operator = -1;
    if (args.length < 1) {
      printUsage();
      return -1;
    }
    
    for (int i = 0; i < args.length; i++) {
      if (args[i].equals("-n")) {
        nThreads = Integer.parseInt(args[++i]);
      } else if (args[i].equals("-f")) {
        fileSizeKB = Integer.parseInt(args[++i]);
      } else if (args[i].equals("-b")) {
        blockSize = Long.parseLong(args[++i]) * 1024;
      } else if (args[i].equals("-r")) {
        nIterations = Integer.parseInt(args[++i]);
      } else if (args[i].equals("-shortcircuit")) {
        shortCircuit = true;
      } else if (args[i].equals("-disablechecksum")) {
        verifyChecksum = false;
      } else if (args[i].equals("-inlinechecksum")) {
        enableInlineChecksum = true;
      } else {
        operator = Integer.parseInt(args[i]);
      }
    }
    
    try {
      setupCluster();
      switch (operator) {
      case 0:
        if (!doReadSameFile(false)) {
          System.out.println("check log for errors");
        }
        if (!doReadSameFile( true)) {
          System.out.println("check log for errors");
        }
        break;

      case 1:
        if (!doReadDifferentFiles(false)) {
          System.out.println("check log for errors");
        }
        if (!doReadDifferentFiles(true)) {
          System.out.println("check log for errors");
        }
        break;

      case 2:
        if (!doWriteFile(false, false)) {
          System.out.println("check log for errors");
        }
        if (!doWriteFile(true, false)) {
          System.out.println("check log for errors");
        }
        break;
      case 3:
        if (!doWriteFile(false, true)) {
          System.out.println("check log for errors");
        }
      }
    } finally {
      tearDownCluster();
    }
    return 0;
  }
}
