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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.InetSocketAddress;

import org.apache.commons.logging.impl.Log4JLogger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.FSConstants;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.datanode.FSDataset;
import org.apache.hadoop.hdfs.server.datanode.SimulatedFSDataset;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.namenode.LeaseManager;
import org.apache.hadoop.io.IOUtils;
import org.apache.log4j.Level;


/**
 * This class tests that a file need not be closed before its
 * data can be read by another client.
 */
public class TestFileLocalRead extends junit.framework.TestCase {
  static final String DIR = "/" + TestFileLocalRead.class.getSimpleName() + "/";

  {
    //((Log4JLogger)DataNode.LOG).getLogger().setLevel(Level.ALL);
    ((Log4JLogger)LeaseManager.LOG).getLogger().setLevel(Level.ALL);
    ((Log4JLogger)FSNamesystem.LOG).getLogger().setLevel(Level.ALL);
    ((Log4JLogger)DFSClient.LOG).getLogger().setLevel(Level.ALL);
  }

  static final long seed = 0xDEADBEEFL;
  static final int blockSize = 5120;
  static final int numBlocks = 3;
  static final int fileSize = numBlocks * blockSize + 100;
  boolean simulatedStorage = false;

  // creates a file but does not close it
  static FSDataOutputStream createFile(FileSystem fileSys, Path name, int repl)
    throws IOException {
    System.out.println("createFile: Created " + name + " with " + repl + " replica.");
    FSDataOutputStream stm = fileSys.create(name, true,
                                            fileSys.getConf().getInt("io.file.buffer.size", 4096),
                                            (short)repl, (long)blockSize);
    return stm;
  }

  //
  // writes to file but does not close it
  //
  static void writeFile(FSDataOutputStream stm) throws IOException {
    writeFile(stm, fileSize);
  }

  //
  // writes specified bytes to file.
  //
  static void writeFile(FSDataOutputStream stm, int size) throws IOException {
    byte[] buffer = AppendTestUtil.randomBytes(seed, size);
    stm.write(buffer, 0, size);
  }

  //
  // verify that the data written to the full blocks are sane
  // 
  private void checkFile(FileSystem fileSys, Path name, int repl)
    throws IOException {
    boolean done = false;

    // wait till all full blocks are confirmed by the datanodes.
    while (!done) {
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {}
      done = true;
      BlockLocation[] locations = fileSys.getFileBlockLocations(
          fileSys.getFileStatus(name), 0, fileSize);
      if (locations.length < numBlocks) {
        done = false;
        continue;
      }
      for (int idx = 0; idx < locations.length; idx++) {
        if (locations[idx].getHosts().length < repl) {
          done = false;
          break;
        }
      }
    }
    FSDataInputStream stm = fileSys.open(name);
    final byte[] expected;
    if (simulatedStorage) {
      expected = new byte[numBlocks * blockSize];
      for (int i= 0; i < expected.length; i++) {  
        expected[i] = SimulatedFSDataset.DEFAULT_DATABYTE;
      }
    } else {
      expected = AppendTestUtil.randomBytes(seed, numBlocks*blockSize);
    }
    // do a sanity check. Read the file
    byte[] actual = new byte[numBlocks * blockSize];
    System.out.println("Verifying file ");
    stm.readFully(0, actual);
    stm.close();
    checkData(actual, 0, expected, "Read 1");
  }

  static private void checkData(byte[] actual, int from, byte[] expected, String message) {
    for (int idx = 0; idx < actual.length; idx++) {
      assertEquals(message+" byte "+(from+idx)+" differs. expected "+
                   expected[from+idx]+" actual "+actual[idx],
                   expected[from+idx], actual[idx]);
      actual[idx] = 0;
    }
  }

  static void checkFullFile(FileSystem fs, Path name) throws IOException {
    FileStatus stat = fs.getFileStatus(name);
    BlockLocation[] locations = fs.getFileBlockLocations(stat, 0, 
                                                         fileSize);
    for (int idx = 0; idx < locations.length; idx++) {
      String[] hosts = locations[idx].getNames();
      for (int i = 0; i < hosts.length; i++) {
        System.out.print( hosts[i] + " ");
      }
      System.out.println(" off " + locations[idx].getOffset() +
                         " len " + locations[idx].getLength());
    }

    byte[] expected = AppendTestUtil.randomBytes(seed, fileSize);
    FSDataInputStream stm = fs.open(name);
    byte[] actual = new byte[fileSize];
    stm.readFully(0, actual);
    checkData(actual, 0, expected, "Read 2");
    stm.close();
  }

  /**
   * Test that file data can be read by reading the block file
   * directly from the local store.
   */
  public void testFileLocalRead() throws IOException {
    Configuration conf = new Configuration();
    conf.setBoolean("dfs.read.shortcircuit", true);
    conf.setBoolean("dfs.read.shortcircuit.fallbackwhenfail", false);
    conf.setBoolean("dfs.use.inline.checksum", false);

    if (simulatedStorage) {
      conf.setBoolean(SimulatedFSDataset.CONFIG_PROPERTY_SIMULATED, true);
    }
    MiniDFSCluster cluster = new MiniDFSCluster(conf, 1, true, null);
    FileSystem fs = cluster.getFileSystem();
    try {

      //
      // check that / exists
      //
      Path path = new Path("/");
      System.out.println("Path : \"" + path.toString() + "\"");
      System.out.println(fs.getFileStatus(path).isDir()); 
      assertTrue("/ should be a directory", 
                 fs.getFileStatus(path).isDir() == true);

      // 
      // create a new file in home directory. Do not close it.
      //
      Path file1 = new Path("filelocal.dat");
      FSDataOutputStream stm = createFile(fs, file1, 1);

      // write to file
      writeFile(stm);
      stm.close();

      // Make sure a client can read it before it is closed.
      checkFile(fs, file1, 1);

    } finally {
      cluster.shutdown();
    }
  }
}
