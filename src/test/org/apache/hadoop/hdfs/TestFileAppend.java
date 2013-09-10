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
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.impl.Log4JLogger;
import org.apache.log4j.Level;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.HardLink;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.hdfs.DFSClient.DFSDataInputStream;
import org.apache.hadoop.hdfs.DFSClient.MultiDataOutputStream;
import org.apache.hadoop.hdfs.profiling.DFSWriteProfilingData;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.FSConstants;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.protocol.PacketBlockReceiverProfileData;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.datanode.FSDataset;
import org.apache.hadoop.hdfs.server.datanode.SimulatedFSDataset;
import org.apache.hadoop.hdfs.util.InjectionEvent;
import org.apache.hadoop.util.InjectionEventI;
import org.apache.hadoop.util.InjectionHandler;

/**
 * This class tests the building blocks that are needed to
 * support HDFS appends.
 */
public class TestFileAppend extends TestCase {
  {
    DataNode.LOG.getLogger().setLevel(Level.ALL);
    ((Log4JLogger)DFSClient.LOG).getLogger().setLevel(Level.ALL);
  }

  static final int blockSize = 1024;
  static final int numBlocks = 10;
  static final int fileSize = numBlocks * blockSize + 1;
  boolean simulatedStorage = false;

  private long seed;
  private byte[] fileContents = null;

  //
  // create a buffer that contains the entire test file data.
  //
  private void initBuffer(int size) {
    seed = AppendTestUtil.nextLong();
    fileContents = AppendTestUtil.randomBytes(seed, size);
  }

  /*
   * creates a file but does not close it
   */ 
  private FSDataOutputStream createFile(FileSystem fileSys, Path name, int repl)
    throws IOException {
    FSDataOutputStream stm = fileSys.create(name, true,
                                            fileSys.getConf().getInt("io.file.buffer.size", 4096),
                                            (short)repl, (long)blockSize);
    return stm;
  }

  //
  // writes to file but does not close it
  //
  private void writeFile(FSDataOutputStream stm) throws IOException {
    byte[] buffer = AppendTestUtil.randomBytes(seed, fileSize);
    stm.write(buffer);
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
        System.out.println("Number of blocks found " + locations.length);
        done = false;
        continue;
      }
      for (int idx = 0; idx < numBlocks; idx++) {
        if (locations[idx].getHosts().length < repl) {
          System.out.println("Block index " + idx + " not yet replciated.");
          done = false;
          break;
        }
      }
    }
    checkContent(fileSys, name, numBlocks * blockSize);
  }
  
  private void checkContent(FileSystem fileSys, Path name, int length)
      throws IOException {
    FSDataInputStream stm = fileSys.open(name);
    byte[] expected = new byte[length];
    if (simulatedStorage) {
      for (int i= 0; i < expected.length; i++) {  
        expected[i] = SimulatedFSDataset.DEFAULT_DATABYTE;
      }
    } else {
      for (int i= 0; i < expected.length; i++) {  
        expected[i] = fileContents[i];
      }
    }
    // do a sanity check. Read the file
    byte[] actual = new byte[length];
    stm.readFully(0, actual);
    checkData(actual, 0, expected, "Read 1");
  }

  private void checkFullFile(FileSystem fs, Path name) throws IOException {
    FSDataInputStream stm = fs.open(name);
    byte[] actual = new byte[fileContents.length];
    stm.readFully(0, actual);
    checkData(actual, 0, fileContents, "Read 2");
    stm.close();
  }

  private void checkData(byte[] actual, int from, byte[] expected, String message) {
    for (int idx = 0; idx < actual.length; idx++) {
      assertEquals(message+" byte "+(from+idx)+" differs. expected "+
                   expected[from+idx]+" actual "+actual[idx],
                   expected[from+idx], actual[idx]);
      actual[idx] = 0;
    }
  }


  /**
   * Test that copy on write for blocks works correctly
   */
  public void testCopyOnWrite() throws IOException {
    Configuration conf = new Configuration();
    conf.setBoolean(FSConstants.DFS_USE_INLINE_CHECKSUM_KEY, true);
    if (simulatedStorage) {
      conf.setBoolean(SimulatedFSDataset.CONFIG_PROPERTY_SIMULATED, true);
    }
    conf.setBoolean(FSConstants.FS_OUTPUT_STREAM_AUTO_PRINT_PROFILE, true);
 
    MiniDFSCluster cluster = new MiniDFSCluster(conf, 1, true, null);
    FileSystem fs = cluster.getFileSystem();
    InetSocketAddress addr = new InetSocketAddress("localhost",
                                                   cluster.getNameNodePort());
    int nsId = cluster.getNameNode().getNamespaceID();
    DFSClient client = new DFSClient(addr, conf);
    try {

      // create a new file, write to it and close it.
      //
      Path file1 = new Path("/filestatus.dat");
      FSDataOutputStream stm = createFile(fs, file1, 1);
      writeFile(stm);
      stm.close();

      // Get a handle to the datanode
      DataNode[] dn = cluster.listDataNodes();
      assertTrue("There should be only one datanode but found " + dn.length,
                  dn.length == 1);

      LocatedBlocks locations = client.namenode.getBlockLocations(
                                  file1.toString(), 0, Long.MAX_VALUE);
      List<LocatedBlock> blocks = locations.getLocatedBlocks();
      FSDataset dataset = (FSDataset) dn[0].data;

      //
      // Create hard links for a few of the blocks
      //
      for (int i = 0; i < blocks.size(); i = i + 2) {
        Block b = (Block) blocks.get(i).getBlock();
        FSDataset fsd = (FSDataset) dataset;
        File f = fsd.getFile(nsId, b);
        File link = new File(f.toString() + ".link");
        System.out.println("Creating hardlink for File " + f + 
                           " to " + link);
        HardLink.createHardLink(f, link);
      }

      //
      // Detach all blocks. This should remove hardlinks (if any)
      //
      for (int i = 0; i < blocks.size(); i++) {
        Block b = (Block) blocks.get(i).getBlock();
        System.out.println("testCopyOnWrite detaching block " + b);
        assertTrue("Detaching block " + b + " should have returned true",
                   dataset.detachBlock(nsId, b, 1) == true);
      }

      // Since the blocks were already detached earlier, these calls should
      // return false
      //
      for (int i = 0; i < blocks.size(); i++) {
        Block b = (Block) blocks.get(i).getBlock();
        System.out.println("testCopyOnWrite detaching block " + b);
        assertTrue("Detaching block " + b + " should have returned false",
                   dataset.detachBlock(nsId,b, 1) == false);
      }

    } finally {
      fs.close();
      cluster.shutdown();
    }
  }
  
  public void testPacketBlockReceiverProfileData() throws IOException {
    PacketBlockReceiverProfileData profile = new PacketBlockReceiverProfileData();
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(baos);
    profile.write(dos);
    dos.close();
    DataInputStream dis = new DataInputStream(new ByteArrayInputStream(
        baos.toByteArray()));
    profile.readFields(dis);
    dis.close();
  }

  /**
   * Test a simple flush on a simple HDFS file.
   */
  public void testSimpleFlush() throws IOException {
    testSimpleFlushInternal(true, true);
    testSimpleFlushInternal(true, false);
    testSimpleFlushInternal(false, true);
    testSimpleFlushInternal(false, false);
  }

  
  private void testSimpleFlushInternal(boolean datnodeInlineChecksum,
      boolean clientInlineChecksum) throws IOException {
    Configuration conf = new Configuration();
    conf.setBoolean(FSConstants.DFS_USE_INLINE_CHECKSUM_KEY, datnodeInlineChecksum);
    if (simulatedStorage) {
      conf.setBoolean(SimulatedFSDataset.CONFIG_PROPERTY_SIMULATED, true);
    }
    initBuffer(fileSize);
    MiniDFSCluster cluster = new MiniDFSCluster(0, conf, 3, true, true, true,
        null, null, null, null, true, false, 1, false, false);

    conf.setBoolean(FSConstants.DFS_USE_INLINE_CHECKSUM_KEY,
        clientInlineChecksum);

    cluster.waitActive();
    
    FileSystem fs = cluster.getFileSystem(conf);
    
    try {

      DFSWriteProfilingData profile = new DFSWriteProfilingData();      
      DFSClient.setProfileDataForNextOutputStream(profile);
      
      
      // create a new file.
      Path file1 = new Path("/simpleFlush.dat");
      FSDataOutputStream stm = createFile(fs, file1, 3);
      System.out.println("Created file simpleFlush.dat");

      // write to file
      int mid = fileSize/2;
      try {
        stm.write(fileContents, 0, mid);
        stm.sync();
        if (!datnodeInlineChecksum && clientInlineChecksum) {
          TestCase
              .fail("Client should fail writing to datanode with inline checksum disabled with inline checksum enabled in client side");
        }
      } catch (IOException ioe) {
        if (datnodeInlineChecksum || !clientInlineChecksum) {
          throw ioe;
        } else {
          return;
        }
      }
      System.out.println("Wrote and Flushed first part of file.");

      // write the remainder of the file
      stm.write(fileContents, mid, fileSize - mid);
      System.out.println("Written second part of file");
      stm.sync();
      stm.sync();
      System.out.println("Wrote and Flushed second part of file.");

      // verify that full blocks are sane
      checkFile(fs, file1, 1);

      stm.close();
      System.out.println("Closed file.");

      // verify that entire file is good
      checkFullFile(fs, file1);
      
      System.out.println("Profile: " + profile.toString());

    } catch (IOException e) {
      System.out.println("Exception :" + e);
      throw e; 
    } catch (Throwable e) {
      System.out.println("Throwable :" + e);
      e.printStackTrace();
      throw new IOException("Throwable : " + e);
    } finally {
      fs.close();
      cluster.shutdown();
    }
  }

  /**
   * Test a simple flush on a simple HDFS file.
   */
  public void testSimpleFlushSmallWrite() throws IOException {
    testSimpleFlushSmallWriteInternal(false);
    testSimpleFlushSmallWriteInternal(true);
  }
  
  /**
   * Test a simple flush on a simple HDFS file.
   */
  private void testSimpleFlushSmallWriteInternal(boolean inlineChecksum) throws IOException {
    Configuration conf = new Configuration();
    if (simulatedStorage) {
      conf.setBoolean(SimulatedFSDataset.CONFIG_PROPERTY_SIMULATED, true);
    }
    conf.setBoolean("dfs.use.inline.checksum", inlineChecksum);
    initBuffer(fileSize);
    MiniDFSCluster cluster = new MiniDFSCluster(conf, 1, true, null);
    FileSystem fs = cluster.getFileSystem();
    try {

      // create a new file.
      Path file1 = new Path("/simpleFlushSmallWrite.dat");
      FSDataOutputStream stm = createFile(fs, file1, 1);
      System.out.println("Created file simpleFlush.dat");

      // write to file
      stm.write(fileContents, 0, 1);
      stm.sync();

      stm.write(fileContents, 1, 1);
      stm.sync();

      stm.write(fileContents, 2, 1);
      stm.sync();
      
      stm.close();
      System.out.println("Closed file.");
      checkContent(fs, file1, 3);
      
      stm = fs.append(file1);
      System.out.println("opened file for append.");
      stm.write(fileContents, 3, 1);
      stm.sync();

      stm.write(fileContents, 4, 1);
      stm.sync();

      stm.write(fileContents, 5, 1);
      stm.sync();

      checkContent(fs, file1, 6);

      stm.write(fileContents, 6, 512);
      stm.sync();
      checkContent(fs, file1, 518);

      stm.write(fileContents, 518, 1024);
      stm.sync();
      checkContent(fs, file1, 1542);

      stm.write(fileContents, 1542, 511);
      stm.sync();
      checkContent(fs, file1, 2053);

      stm.write(fileContents, 2053, 513);
      stm.sync();
      checkContent(fs, file1, 2566);
      
      System.out.println("Writing the rest of the data to file");
      stm.write(fileContents, 2566, fileSize - 2566);
      stm.sync();

      stm.close();

      checkFile(fs, file1, 1);

      stm.close();
      System.out.println("Closed file.");

      // verify that entire file is good
      checkFullFile(fs, file1);

    } catch (IOException e) {
      System.out.println("Exception :" + e);
      throw e; 
    } catch (Throwable e) {
      System.out.println("Throwable :" + e);
      e.printStackTrace();
      throw new IOException("Throwable : " + e);
    } finally {
      fs.close();
      cluster.shutdown();
    }
  }

  /**
   * Test that file data can be flushed.
   */
  public void testComplexFlush() throws IOException {
    Configuration conf = new Configuration();
    conf.setBoolean(FSConstants.DFS_USE_INLINE_CHECKSUM_KEY, true);
    if (simulatedStorage) {
      conf.setBoolean(SimulatedFSDataset.CONFIG_PROPERTY_SIMULATED, true);
    }
    initBuffer(fileSize);
    MiniDFSCluster cluster = new MiniDFSCluster(conf, 1, true, null);
    FileSystem fs = cluster.getFileSystem();
    try {

      // create a new file.
      Path file1 = new Path("/complexFlush.dat");
      FSDataOutputStream stm = createFile(fs, file1, 1);
      System.out.println("Created file complexFlush.dat");

      int start = 0;
      for (start = 0; (start + 29) < fileSize; ) {
        stm.write(fileContents, start, 29);
        stm.sync();
        start += 29;
      }
      stm.write(fileContents, start, fileSize-start);

      // verify that full blocks are sane
      checkFile(fs, file1, 1);
      stm.close();

      // verify that entire file is good
      checkFullFile(fs, file1);
    } catch (IOException e) {
      System.out.println("Exception :" + e);
      throw e; 
    } catch (Throwable e) {
      System.out.println("Throwable :" + e);
      e.printStackTrace();
      throw new IOException("Throwable : " + e);
    } finally {
      fs.close();
      cluster.shutdown();
    }
  }
  

  /** This creates a slow writer and check to see
   * if pipeline heartbeats work fine
   */
  public void testPipelineHeartbeat() throws Exception {
    final int DATANODE_NUM = 2;
    final int fileLen = 6;
    Configuration conf = new Configuration();
    conf.setBoolean(FSConstants.DFS_USE_INLINE_CHECKSUM_KEY, true);
    final int timeout = 2000;
    conf.setInt("dfs.socket.timeout",timeout);
    conf.setBoolean(FSConstants.FS_OUTPUT_STREAM_AUTO_PRINT_PROFILE, true);

    final Path p = new Path("/pipelineHeartbeat/foo");
    System.out.println("p=" + p);

    MiniDFSCluster cluster = new MiniDFSCluster(conf, DATANODE_NUM, true, null);
    DistributedFileSystem fs = (DistributedFileSystem)cluster.getFileSystem();

    initBuffer(fileLen);

    try {
      DFSWriteProfilingData profile = new DFSWriteProfilingData();      
      DFSClient.setProfileDataForNextOutputStream(profile);
      
      // create a new file.
      FSDataOutputStream stm = createFile(fs, p, DATANODE_NUM);

      stm.write(fileContents, 0, 1);
      Thread.sleep(timeout);
      stm.sync();
      System.out.println("Wrote 1 byte and hflush " + p);

      // write another byte
      Thread.sleep(timeout);
      stm.write(fileContents, 1, 1);
      stm.sync();

      stm.write(fileContents, 2, 1);
      Thread.sleep(timeout);
      stm.sync();

      stm.write(fileContents, 3, 1);
      Thread.sleep(timeout);
      stm.write(fileContents, 4, 1);
      stm.sync();

      stm.write(fileContents, 5, 1);
      Thread.sleep(timeout);
      stm.close();

      // verify that entire file is good
      checkFullFile(fs, p);
    } finally {
      fs.close();
      cluster.shutdown();
    }
  }

  /**
   * Test a simple flush on a simple HDFS file.
   * @throws InterruptedException 
   * @throws NoSuchFieldException 
   * @throws SecurityException 
   * @throws IllegalAccessException 
   * @throws IllegalArgumentException 
   */
  public void testLocatedBlockExpire() throws IOException,
      InterruptedException, SecurityException, NoSuchFieldException,
      IllegalArgumentException, IllegalAccessException {
    Configuration conf = new Configuration();

    final AtomicInteger invokeCount = new AtomicInteger(0);
    
    InjectionHandler.set(new InjectionHandler() {
      @Override
      protected void _processEventIO(InjectionEventI event, Object... args)
          throws IOException {
        if (event == InjectionEvent.DFSCLIENT_GET_LOCATED_BLOCKS) {
          invokeCount.incrementAndGet();
        }
      }
    });
    
    if (simulatedStorage) {
      conf.setBoolean(SimulatedFSDataset.CONFIG_PROPERTY_SIMULATED, true);
    }
    // Disable background block location renewal thread
    // (it is enabled by default in unit tests)
    conf.setBoolean("dfs.client.block.location.renewal.enabled", false);
    conf.setInt("dfs.client.locatedblock.expire.timeout", 1000);
    conf.setInt("dfs.client.locatedblock.expire.random.timeout", 2);
    conf.setLong("dfs.read.prefetch.size", fileSize - blockSize * 2);
    initBuffer(fileSize);
    MiniDFSCluster cluster = new MiniDFSCluster(conf, 2, true, null);
    FileSystem fs = cluster.getFileSystem();
    try {

      // create a new file.
      Path file1 = new Path("/testLocatedBlockExpire");
      FSDataOutputStream stm = createFile(fs, file1, 2);
      System.out.println("Created file testLocatedBlockExpire");

      // write to file
      stm.write(fileContents, 0, fileSize);
      stm.close();
      System.out.println("Closed file.");

      TestCase.assertEquals(0, invokeCount.get());

      // open the file and remove one datanode from every block
      FSDataInputStream in = fs.open(file1);
      TestCase.assertEquals(1, invokeCount.get());
      
      List<LocatedBlock> lbs = ((DFSDataInputStream)in).getAllBlocks();
      for (LocatedBlock lb : lbs) {
        Field f = lb.getClass().getDeclaredField("locs"); //NoSuchFieldException
        f.setAccessible(true);
        DatanodeInfo[] di = (DatanodeInfo[]) f.get(lb);
        DatanodeInfo[] newDi = new DatanodeInfo[] { di[0] };
        f.set(lb, newDi);
      }
      
      TestCase.assertEquals(2, invokeCount.get());
      
      in.read(fileSize / 4, new byte[fileSize], 0, fileSize / 2);

      TestCase.assertEquals(2, invokeCount.get());

      
      // double check the location size is still 1;
      lbs = ((DFSDataInputStream)in).getAllBlocks();
      for (LocatedBlock lb : lbs) {
        Field f = lb.getClass().getDeclaredField("locs"); //NoSuchFieldException
        f.setAccessible(true);
        DatanodeInfo[] di = (DatanodeInfo[]) f.get(lb);
        TestCase.assertEquals(1, di.length);
      }
      
      // sleep up to the located block expire time
      Thread.sleep(1000);
      // all block locations expire now. Refetch [file_size/2, file_size]
      in.read(fileSize / 2, new byte[fileSize], 0, fileSize / 4 - 1);      
      TestCase.assertEquals(3, invokeCount.get());
      
      Thread.sleep(500);
      // reread within range so no need to refetch
      in.seek(fileSize / 4 * 3 + 1);
      in.read(new byte[fileSize], 0, fileSize / 4 - 2);
      TestCase.assertEquals(3, invokeCount.get());
      // need to refetch as the previous refetch doesn't cover it.
      in.seek(blockSize);
      in.read(new byte[fileSize], 0, fileSize / 4 + blockSize);
      TestCase.assertEquals(4, invokeCount.get());

      Thread.sleep(500);
      // [fileSize-blockSize, fileSize] expired. need to refetch.
      in.read(fileSize - blockSize, new byte[fileSize], 0, blockSize);
      TestCase.assertEquals(5, invokeCount.get());
      in.read(fileSize - blockSize * 2, new byte[fileSize], 0, blockSize);
      TestCase.assertEquals(5, invokeCount.get());

      Thread.sleep(500);
      // All but [fileSize-blockSize, fileSize] expired. Refetch.
      in.read(fileSize / 4, new byte[fileSize], 0, fileSize / 4 - 1);
      TestCase.assertEquals(6, invokeCount.get());

      Thread.sleep(100);
      // prefetch [fileSize/2, fileSize]
      in.seek(fileSize / 2);
      in.read(fileSize / 2, new byte[fileSize], 0, fileSize / 2 - 1);
      TestCase.assertEquals(7, invokeCount.get());

      Thread.sleep(100);
      // need to prefetch [0, prefetchSize]
      in.read(0, new byte[fileSize], 0, fileSize / 2 - 1);
      TestCase.assertEquals(8, invokeCount.get());

      // All blocks' locations should be in cache with two locations (updated from namenodes)
      lbs = ((DFSDataInputStream)in).getAllBlocks();
      for (LocatedBlock lb : lbs) {
        Field f = lb.getClass().getDeclaredField("locs"); //NoSuchFieldException
        f.setAccessible(true);
        DatanodeInfo[] di = (DatanodeInfo[]) f.get(lb);
        TestCase.assertEquals(2, di.length);
      }
      TestCase.assertEquals(8, invokeCount.get());
      
      in.close();
    } catch (IOException e) {
      System.out.println("Exception :" + e);
      throw e; 
    } finally {
      fs.close();
      cluster.shutdown();
    }
  }

  
}
