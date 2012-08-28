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

import junit.framework.TestCase;
import java.io.*;
import java.util.Random;
import java.net.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.FSConstants.DatanodeReportType;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileStatus;
import org.mortbay.log.Log;

/**
 * This class tests the access time of files.
 * 
 */
public class TestAccessTime extends TestCase {

  static final long seed         = 0xDEADBEEFL;
  static final int  blockSize    = 8192;
  static final int  fileSize     = 16384;
  static final int  numDatanodes = 6;

  Random            myrand       = new Random();
  Path              hostsFile;
  Path              excludeFile;
  Configuration     conf;
  boolean           touchable;
  boolean           changable;

  static void sleep(long ms) {
    try {
      Thread.sleep(ms);
    } catch (InterruptedException e) {
      System.out.println("ms=" + ms + e.getMessage());
    }
  }
  private void createFile(FileSystem fileSys, Path name, int repl,
    boolean overwrite) throws IOException {
    // create and write a file that contains three blocks of data
    FSDataOutputStream stm = fileSys.create(name, overwrite, 
      fileSys.getConf().getInt("io.file.buffer.size", 4096), (short) repl, (long) blockSize);
    byte[] buffer = new byte[fileSize];
    Random rand = new Random(seed);
    rand.nextBytes(buffer);
    sleep(1);
    stm.write(buffer);
    FileStatus stat = fileSys.getFileStatus(name);
    long atime1 = stat.getAccessTime();
    long mtime1 = stat.getModificationTime();
    System.out.println("Creating after write: accessTime = " + atime1
                       + " modTime = " + mtime1);
    sleep(1);
    stm.close();
    stat = fileSys.getFileStatus(name);
    atime1 = stat.getAccessTime();
    mtime1 = stat.getModificationTime();
    System.out.println("Creating after close: accessTime = " + atime1
                       + " modTime = " + mtime1);
  }

  private void appendFile(FileSystem fileSys, Path name, int repl)
    throws IOException {
    // create and write a file that contains three blocks of data
    FSDataOutputStream stm = fileSys.append(name,
      fileSys.getConf().getInt("io.file.buffer.size", 4096));
    byte[] buffer = new byte[fileSize];
    Random rand = new Random(seed);
    rand.nextBytes(buffer);
    sleep(1);
    stm.write(buffer);
    FileStatus stat = fileSys.getFileStatus(name);
    long atime1 = stat.getAccessTime();
    long mtime1 = stat.getModificationTime();
    System.out.println("Appending after write: accessTime = " + atime1
                       + " modTime = " + mtime1);
    sleep(1);
    stm.close();
    stat = fileSys.getFileStatus(name);
    atime1 = stat.getAccessTime();
    mtime1 = stat.getModificationTime();
    System.out.println("Appending after close: accessTime = " + atime1
                       + " modTime = " + mtime1);
  }

  private void cleanupFile(FileSystem fileSys, Path name) throws IOException {
    assertTrue(fileSys.exists(name));
    fileSys.delete(name, true);
    assertTrue(!fileSys.exists(name));
  }

  private void printDatanodeReport(DatanodeInfo[] info) {
    System.out.println("-------------------------------------------------");
    for (int i = 0; i < info.length; i++) {
      System.out.println(info[i].getDatanodeReport());
      System.out.println();
    }
  }

  public void dounit() throws IOException {
    Configuration conf = new Configuration();
    System.out.println("Testing accessTime with touchable = " + touchable
                       + " changable = " + changable);
    if (touchable) {
      conf.set("dfs.access.time.touchable", "true");
    } else {
      conf.set("dfs.access.time.touchable", "false");
    }
    if (changable) {
      conf.set("dfs.access.time.precision", "3600000");
    } else {
      conf.set("dfs.access.time.precision", "0");
    }
    MiniDFSCluster cluster = new MiniDFSCluster(conf, numDatanodes, true, null);
    cluster.waitActive();
    InetSocketAddress addr = new InetSocketAddress("localhost",
      cluster.getNameNodePort());
    DFSClient client = new DFSClient(addr, conf);
    DatanodeInfo[] info = client.datanodeReport(DatanodeReportType.LIVE);
    assertEquals("Number of Datanodes ", numDatanodes, info.length);
    FileSystem fileSys = cluster.getFileSystem();
    int replicas = numDatanodes - 1;
    assertTrue(fileSys instanceof DistributedFileSystem);

    try {
      //
      // create file and record access time of test file
      //
      String testfilename = "test.dat1";
      System.out.println("Creating testdir1 and " + testfilename);
      Path dir1 = new Path("testdir1");
      Path file1 = new Path(dir1, testfilename);

      createFile(fileSys, file1, replicas, false);
      FileStatus stat = fileSys.getFileStatus(file1);
      long atime1 = stat.getAccessTime();
      long mtime1 = stat.getModificationTime();
      System.out.println("The access time after creating the file is " + atime1);
      System.out.println("The mod time after creating the file is " + mtime1);
      stat = fileSys.getFileStatus(dir1);
      long adir1 = stat.getAccessTime();
      System.out.println("The access time for the dir is " + adir1);
      assertTrue(atime1 != 0);

      //
      // append file
      //
      System.out.println("Appending testdir1/" + testfilename);
      appendFile(fileSys, file1, replicas);
      stat = fileSys.getFileStatus(file1);
      long atime2 = stat.getAccessTime();
      long mtime2 = stat.getModificationTime();
      System.out.println("The access time after appending the file is "
                         + atime2);
      System.out.println("The mod time after appending the file is " + mtime2);
      stat = fileSys.getFileStatus(dir1);
      long adir2 = stat.getAccessTime();
      System.out.println("The access time for the dir is " + adir2);
      if (!changable) {
        assertTrue(atime1 == atime2);
      } else {
        assertTrue(atime1 != atime2);
      }
      assertTrue(mtime1 != mtime2);

      //
      // overwrite file
      //
      System.out.println("Overwriting testdir1/" + testfilename);
      createFile(fileSys, file1, replicas, true);
      stat = fileSys.getFileStatus(file1);
      long atime3 = stat.getAccessTime();
      long mtime3 = stat.getModificationTime();
      System.out.println("The access time after overwriting the file is "
                         + atime3);
      System.out.println("The modification time after overwriting the file is "
                         + mtime3);
      stat = fileSys.getFileStatus(dir1);
      long adir3 = stat.getAccessTime();
      System.out.println("The access time for the dir is " + adir3);
      if (!changable) {
        assertTrue(atime1 == atime3);
      } else {
        assertTrue(atime2 != atime3);
      }
      assertTrue(mtime2 != mtime3);

      // setTimes
      System.out.println("setTime for testdir1/" + testfilename);
      long atime4 = atime3 - (24L * 3600L * 1000L);
      long mtime4 = mtime3 - (3600L * 1000L);
      try {
        fileSys.setTimes(file1, mtime4, atime4);
      } catch (org.apache.hadoop.ipc.RemoteException e) {
        assertTrue(touchable == false);
        Log.info("Expected exception from the server for failed touch");
      }
      System.out.println("set modifictaion=" + mtime4 + " accessTime=" + atime4);

      stat = fileSys.getFileStatus(file1);
      long atime5 = stat.getAccessTime();
      long mtime5 = stat.getModificationTime();
      System.out.println("The access time after setting the file is " + atime5);
      System.out.println("The modification time after setting the file is "
                         + mtime5);
      stat = fileSys.getFileStatus(dir1);
      long adir4 = stat.getAccessTime();
      System.out.println("The access time for the dir is " + adir4);
      if (!touchable) {
        assertTrue(atime4 != atime5);
        assertTrue(mtime4 != mtime5);
        assertTrue(atime3 == atime5);
        assertTrue(mtime3 == mtime5);
      } else {
        assertTrue(atime4 == atime5);
        assertTrue(mtime4 == mtime5);
      }

      cleanupFile(fileSys, file1);
      cleanupFile(fileSys, dir1);
    } catch (IOException e) {
      info = client.datanodeReport(DatanodeReportType.ALL);
      printDatanodeReport(info);
      throw e;
    } finally {
      fileSys.close();
      cluster.shutdown();
    }
  }

  /**
   * Tests access time in DFS.
   */
  public void testNoTouchDoChange() throws IOException {
    touchable = false;
    changable = true;
    dounit();
  }

  public void testNoTouchNoChange() throws IOException {
    touchable = false;
    changable = false;
    dounit();
  }

  public void testTouchNoChange() throws IOException {
    touchable = true;
    changable = false;
    dounit();
  }

  public void testTouchChange() throws IOException {
    touchable = true;
    changable = true;
    dounit();
  }
}
