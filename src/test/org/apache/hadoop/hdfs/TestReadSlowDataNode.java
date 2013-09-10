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
import java.lang.reflect.Field;
import java.net.*;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.logging.impl.Log4JLogger;
import org.apache.log4j.Level;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSInputChecker;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.HardLink;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.hdfs.BlockReader;
import org.apache.hadoop.hdfs.DFSInputStream;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.datanode.FSDataset;
import org.apache.hadoop.hdfs.server.datanode.FSDatasetInterface;
import org.apache.hadoop.hdfs.server.datanode.SimulatedFSDataset;
import static org.mockito.Mockito.spy;

import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.mortbay.log.Log;

import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Mockito.doAnswer;

/**
 * This class tests the building blocks that are needed to support HDFS appends.
 */
public class TestReadSlowDataNode extends TestCase {
  {
    DataNode.LOG.getLogger().setLevel(Level.ALL);
    ((Log4JLogger) DFSClient.LOG).getLogger().setLevel(Level.ALL);
  }

  static final int blockSize = 2048 * 1024;
  static final int fileSize = blockSize;
  boolean simulatedStorage = false;

  private long seed;
  private byte[] fileContents = null;

  /*
   * creates a file but does not close it
   */
  private FSDataOutputStream createFile(FileSystem fileSys, Path name, int repl)
      throws IOException {
    FSDataOutputStream stm = fileSys.create(name, true, fileSys.getConf()
        .getInt("io.file.buffer.size", 4096), (short) repl, (long) blockSize);
    return stm;
  }

  //
  // writes to file but does not close it
  //
  private void writeFile(FSDataOutputStream stm) throws IOException {
    byte[] buffer = AppendTestUtil.randomBytes(seed, fileSize);
    stm.write(buffer);
  }

  public static DFSInputStream findDFSClientInputStream(FSDataInputStream in)
      throws SecurityException, NoSuchFieldException, IllegalArgumentException,
      IllegalAccessException {
    Field inField = FilterInputStream.class.getDeclaredField("in");
    inField.setAccessible(true);
    return (DFSInputStream) inField.get(in);
  }

  @SuppressWarnings("unchecked")
  public static ConcurrentHashMap<DatanodeInfo, DatanodeInfo> getDeadNodes(
      DFSInputStream in) throws SecurityException, IllegalArgumentException,
      NoSuchFieldException, IllegalAccessException {
    Field deadNodesField = DFSInputStream.class.getDeclaredField("deadNodes");
    deadNodesField.setAccessible(true);
    return (ConcurrentHashMap<DatanodeInfo, DatanodeInfo>) deadNodesField
        .get(in);

  }

  /**
   * Test that copy on write for blocks works correctly
   * 
   * @throws NoSuchFieldException
   * @throws SecurityException
   * @throws IllegalAccessException
   * @throws IllegalArgumentException
   */
  public void testSlowDn() throws IOException, SecurityException,
      NoSuchFieldException, IllegalArgumentException, IllegalAccessException {
    Configuration conf = new Configuration();
    conf.setLong("dfs.bytes.to.check.read.speed", 128 * 1024);
    conf.setLong("dfs.min.read.speed.bps", 1024 * 200);
    conf.setBoolean("dfs.read.switch.for.slow", true);
    
    MiniDFSCluster cluster = new MiniDFSCluster(conf, 2, true, null);
    FileSystem fs = cluster.getFileSystem();
    FSDataInputStream in = null;
    try {
      
      // create a new file, write to it and close it.
      //
      Path file1 = new Path("/filestatus.dat");
      FSDataOutputStream stm = createFile(fs, file1, 2);
      writeFile(stm);
      stm.close();

      in = fs.open(file1);
      in.readByte();

      DFSInputStream dfsClientIn = findDFSClientInputStream(in);      
      Field blockReaderField = DFSInputStream.class.getDeclaredField("blockReader");
      blockReaderField.setAccessible(true);
      BlockReader blockReader = (BlockReader) blockReaderField.get(dfsClientIn);

      blockReader.setArtificialSlowdown(1000);
      blockReader.isReadLocal = false;
      blockReader.isReadRackLocal = false;
      for (int i = 0; i < 1024; i++) {
        in.readByte();
      }

      blockReader.setArtificialSlowdown(0);
      for (int i = 1024; i < fileSize - 1; i++) {
        in.readByte();
      }

      ConcurrentHashMap<DatanodeInfo, DatanodeInfo> deadNodes = getDeadNodes(dfsClientIn);
      TestCase.assertEquals(1, deadNodes.size());
    } finally {
      if (in != null) {
        in.close();
      }
      fs.close();
      cluster.shutdown();
    }
  }
}
