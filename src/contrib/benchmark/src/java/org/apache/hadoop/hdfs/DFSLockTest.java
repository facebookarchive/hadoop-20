package org.apache.hadoop.hdfs;
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
import java.util.ArrayList;
import java.util.Collections;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

@SuppressWarnings("deprecation")
public class DFSLockTest extends Configured implements Tool{

  private static long ntasks;
  private static long nread;
  private static long nwrite;
  private static long rseed;
  private static long startTime;
  private static final int NSUBTASKS = 10;
  private static final int NTHREADS = 0;
  private static final int DEFAULT_INTERVAL = 10;
  private static final int DEFAULT_RSEED = 10;
  private static final String DEFAULT_WRITEOP = "0,1,2,3";
  private static final String DEFAULT_READOP = "0,1,2";
  private static final Path TESTDIR = new Path("/lockbench");
  private static final FsPermission all = new FsPermission((short)0777);
  private Configuration conf;
  
  private enum THREADTYPE {
    READ, WRITE;
  }
  private static final String[] DEFAULT_READOPS = 
    {"getFileStatus", "getContentSummary", "listStatus"};
  private static final String[] DEFAULT_WRITEOPS = 
    {"mkdirs", "setPermission", "rename", "createEmptyFile"};  
  private static Integer[] writeOps; 
  private static Integer[] readOps;
  
  public static void printUsage() {
    System.out.println("USAGE: bin/hadoop hadoop-*-benchmark.jar dirtest " +
        "[-nTasks] [-nRead] [-nWrite] [-rseed] [-readOP] [-writeOP] "); 
    System.out.println("Default nTasks = " + NSUBTASKS);
    System.out.println("Default nRead = " + NTHREADS);
    System.out.println("Default nWrite = " + NTHREADS);
    System.out.println("Default rseed= " + DEFAULT_RSEED);
    System.out.println("Default readOP= " + DEFAULT_READOP);
    System.out.println("Default writeOP= " + DEFAULT_WRITEOP);
    System.out.print("Read: ");
    for (int i = 0; i < DEFAULT_READOPS.length; i++) {
      System.out.print(i + ":" + DEFAULT_READOPS[i] + " ");
    }
    System.out.println();
    System.out.print("Write: ");
    for (int i = 0; i < DEFAULT_WRITEOPS.length; i++) {
      System.out.print(i + ":" + DEFAULT_WRITEOPS[i] + " ");
    }
    System.out.println();
    System.exit(0);
  }
  
  private class LockThread extends Thread {
    THREADTYPE type; 
    int id = 0;
    long acquiredTime;     // lock acquired time
    long releasedTime = 0; // lock release time
    Path runPath; 
    Path renameablePath;
    private FileSystem fs;
    Random rand;
    int runType;
    
    public LockThread(Configuration conf, THREADTYPE type, int id) 
        throws IOException{
      this.type = type;
      this.id = id;
      this.fs = FileSystem.newInstance(conf);
      this.runPath = new Path(TESTDIR, Integer.toString(id));
      this.renameablePath = new Path(runPath, "rename");
      this.rand = new Random(id * 32 + rseed);
      fs.mkdirs(runPath);
      fs.mkdirs(renameablePath);
    }
    
    public void read() throws Exception{
      // test read lock
      runType = readOps[rand.nextInt(readOps.length)]; 
      switch (runType) {
      case 0:
        FileStatus status = fs.getFileStatus(TESTDIR); break;
      case 1:
        ContentSummary cs = fs.getContentSummary(TESTDIR); break;
      case 2:
        FileStatus[] fss = fs.listStatus(TESTDIR); break;
      }
    }
    
    public void write(int i) throws Exception{
      // test write lock
      Path newPath = new Path(runPath, Integer.toString(i));
      runType = writeOps[rand.nextInt(writeOps.length)]; 
      switch (runType) {
      case 0:
        fs.mkdirs(newPath); break;
      case 1:
        fs.setPermission(runPath, all); break;
      case 2:
        if (fs.rename(this.renameablePath, newPath)) {
          this.renameablePath = newPath; 
        }
        break;
      case 3:
        FSDataOutputStream out = null;
        try {
          out = fs.create(newPath, true);
          out.close();
          out = null;
        } finally {
          IOUtils.closeStream(out);
        }
        break;
      }
    }
    
    public void run() {
      try {
        for (int i = 0; i < ntasks; i++) {
          acquiredTime = System.currentTimeMillis();
          String operationStr = "";
          try {
            switch (type) {
            case READ: 
              read();
              operationStr = DEFAULT_READOPS[runType];
              break;
            case WRITE: 
              write(i);
              operationStr = DEFAULT_WRITEOPS[runType];
              break;
            }
          } catch (Exception ioe) {
            System.err.println(ioe.getLocalizedMessage());
            ioe.printStackTrace();
          }
          releasedTime = System.currentTimeMillis();
          System.out.println(type.name() + ": " + id + "." + operationStr + "." + 
            i + " " + (acquiredTime - startTime) + " to " + (releasedTime - startTime));
        }
      } catch (Exception ioe) {
        System.err.println(ioe.getLocalizedMessage());
        ioe.printStackTrace();
      } finally {
        try {
          fs.close();
        } catch (IOException ie) {
        }
      }
    }
  }
  
  public void test() throws IOException {
    // spawn ntasks threads 
    startTime = System.currentTimeMillis();
    conf = new Configuration(getConf());
    FileSystem fs = FileSystem.newInstance(conf);
    fs.delete(TESTDIR, true);
    ArrayList<LockThread> threads = new ArrayList<LockThread>((int)(nread + nwrite));
    int i;
    int id = 0;
    for (i=0; i < nread; i++, id++) {
      threads.add(new LockThread(conf, THREADTYPE.READ, id));
    }
    for (i=0; i < nwrite; i++, id++) {
      threads.add(new LockThread(conf, THREADTYPE.WRITE, id));
    }
    Collections.shuffle(threads, new Random(rseed));
    for (LockThread thread : threads) {
      thread.start();
    }
    for (LockThread thread : threads) {
      try {
        thread.join();
      } catch (InterruptedException ex) {
      }
    }
  }
  
  @Override
  public int run(String[] args) throws IOException {
    ntasks = NSUBTASKS;
    nread = NTHREADS;
    nwrite = NTHREADS;
    rseed = DEFAULT_RSEED;
    String strWriteOP = DEFAULT_WRITEOP;
    String strReadOP = DEFAULT_READOP;

    for (int i = 0; i < args.length; i++) {
      if (args[i].equals("-nTasks")) ntasks = Long.parseLong(args[++i]); else
      if (args[i].equals("-nRead")) nread = Long.parseLong(args[++i]); else
      if (args[i].equals("-nWrite")) nwrite = Long.parseLong(args[++i]); else
      if (args[i].equals("-rseed")) rseed = Long.parseLong(args[++i]); else
      if (args[i].equals("-readOP")) strReadOP = args[++i]; else
      if (args[i].equals("-writeOP")) strWriteOP = args[++i]; else
        printUsage();
    }
    String[] ops = strReadOP.split(",");
    ArrayList<Integer> opsStr = new ArrayList<Integer>();
    for (String op: ops) {
      int index = Integer.parseInt(op);
      if (index >= 0 && index < DEFAULT_READOPS.length) {
        opsStr.add(index);
      }
    }
    readOps = opsStr.toArray(new Integer[opsStr.size()]);
    ops = strWriteOP.split(",");
    opsStr = new ArrayList<Integer>();
    for (String op: ops) {
      int index = Integer.parseInt(op);
      if (index >= 0 && index < DEFAULT_WRITEOPS.length) {
        opsStr.add(index);
      }
    }
    writeOps = opsStr.toArray(new Integer[opsStr.size()]);
    test();
    return 0;
  }

  public static void main(String[] args) throws Exception {
    System.exit(ToolRunner.run(new DFSLockTest(), args));
  }
}
