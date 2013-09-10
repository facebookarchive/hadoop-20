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

import java.io.*;
import java.util.Iterator;
import java.util.Random;
import java.net.*;

import junit.framework.TestCase;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.FSConstants;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.protocol.FSConstants.DatanodeReportType;
import org.apache.hadoop.hdfs.server.datanode.BlockInlineChecksumWriter;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.datanode.SimulatedFSDataset;
import org.apache.hadoop.hdfs.server.namenode.BlockPlacementPolicy;
import org.apache.hadoop.hdfs.server.namenode.BlockPlacementPolicyConfigurable;
import org.apache.hadoop.hdfs.server.namenode.BlockPlacementPolicyDefault;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.BlockLocation;

import static org.junit.Assert.*;
import org.junit.Test;

/**
 * This class tests the replication of a DFS file.
 */
public class TestReplication {
  private static final long seed = 0xDEADBEEFL;
  private static final int blockSize = 8192;
  private static final int fileSize = 16384;
  private static final String racks[] = new String[] {
    "/d1/r1", "/d1/r1", "/d1/r2", "/d1/r2", "/d1/r2", "/d2/r3", "/d2/r3",
    "/d2/r3"
  };
  private static final int numDatanodes = racks.length;
  private static final Log LOG = LogFactory.getLog(
                                       "org.apache.hadoop.hdfs.TestReplication");
  
  private static final String[] racks4 = new String[] { racks[0], racks[1], racks[2], racks[3] };

  private void writeFile(FileSystem fileSys, Path name, int repl)
    throws IOException {
    // create and write a file that contains three blocks of data
    FSDataOutputStream stm = fileSys.create(name, true,
                                            fileSys.getConf().getInt("io.file.buffer.size", 4096),
                                            (short)repl, (long)blockSize);
    byte[] buffer = new byte[fileSize];
    Random rand = new Random(seed);
    rand.nextBytes(buffer);
    stm.write(buffer);
    stm.close();
  }
  
  /* check if there are at least two nodes are on the same rack */
  private void checkFile(FileSystem fileSys, Path name, int repl)
    throws IOException {
    Configuration conf = fileSys.getConf();
    ClientProtocol namenode = DFSClient.createNamenode(conf);
      
    waitForBlockReplication(name.toString(), namenode, 
                            Math.min(numDatanodes, repl), -1);
    
    LocatedBlocks locations = namenode.getBlockLocations(name.toString(),0,
                                                         Long.MAX_VALUE);
    FileStatus stat = fileSys.getFileStatus(name);
    BlockLocation[] blockLocations = fileSys.getFileBlockLocations(stat,0L,
                                                         Long.MAX_VALUE);
    // verify that rack locations match
    assertTrue(blockLocations.length == locations.locatedBlockCount());
    for (int i = 0; i < blockLocations.length; i++) {
      LocatedBlock blk = locations.get(i);
      DatanodeInfo[] datanodes = blk.getLocations();
      String[] topologyPaths = blockLocations[i].getTopologyPaths();
      String[] racks = blockLocations[i].getRacks();
      assertTrue(topologyPaths.length == datanodes.length);
      for (int j = 0; j < topologyPaths.length; j++) {
        boolean found = false;
        String matchedRack = null;
        for (int k = 0; k < racks.length; k++) {
          if (topologyPaths[j].startsWith(racks[k])) {
            found = true;
            matchedRack = racks[k];
            break;
          }
        }
        assertTrue(found);
        assertEquals("Rack info should be equal", matchedRack, racks[j]);
      }
    }

    boolean isOnSameRack = true, isNotOnSameRack = true;
    for (LocatedBlock blk : locations.getLocatedBlocks()) {
      DatanodeInfo[] datanodes = blk.getLocations();
      if (datanodes.length <= 1) break;
      if (datanodes.length == 2) {
        isNotOnSameRack = !(datanodes[0].getNetworkLocation().equals(
                                                                     datanodes[1].getNetworkLocation()));
        break;
      }
      isOnSameRack = false;
      isNotOnSameRack = false;
      for (int i = 0; i < datanodes.length-1; i++) {
        LOG.info("datanode "+ i + ": "+ datanodes[i].getName());
        boolean onRack = false;
        for( int j=i+1; j<datanodes.length; j++) {
           if( datanodes[i].getNetworkLocation().equals(
            datanodes[j].getNetworkLocation()) ) {
             onRack = true;
           }
        }
        if (onRack) {
          isOnSameRack = true;
        }
        if (!onRack) {
          isNotOnSameRack = true;                      
        }
        if (isOnSameRack && isNotOnSameRack) break;
      }
      if (!isOnSameRack || !isNotOnSameRack) break;
    }
    assertTrue(isOnSameRack);
    if (conf.getClass("dfs.block.replicator.classname", null,
        BlockPlacementPolicy.class).equals(
        BlockPlacementPolicyConfigurable.class)
        && repl == 2) {
      // For BlockPlacementPolicyConfigurable we do in rack replication for r =
      // 2.
      assertFalse(isNotOnSameRack);
    } else {
      assertTrue(isNotOnSameRack);
    }
  }
  
  private void cleanupFile(FileSystem fileSys, Path name) throws IOException {
    assertTrue(fileSys.exists(name));
    fileSys.delete(name, true);
    assertTrue(!fileSys.exists(name));
  }
  
  /* 
   * Test if Datanode reports bad blocks during replication request
   */
  @Test
  public void testBadBlockReportOnTransfer() throws Exception {
    Configuration conf = new Configuration();
    FileSystem fs = null;
    DFSClient dfsClient = null;
    LocatedBlocks blocks = null;
    int replicaCount = 0;
    MiniDFSCluster cluster = new MiniDFSCluster(conf, 2, true, null);
    cluster.waitActive();
    fs = cluster.getFileSystem();
    dfsClient = new DFSClient(new InetSocketAddress("localhost",
                              cluster.getNameNodePort()), conf);
  
    // Create file with replication factor of 1
    Path file1 = new Path("/tmp/testBadBlockReportOnTransfer/file1");
    DFSTestUtil.createFile(fs, file1, 1024, (short)1, 0);
    DFSTestUtil.waitReplication(fs, file1, (short)1);
  
    // Corrupt the block belonging to the created file
    Block block = DFSTestUtil.getFirstBlock(fs, file1);
    cluster.corruptBlockOnDataNodes(block);
  
    // Increase replication factor, this should invoke transfer request
    // Receiving datanode fails on checksum and reports it to namenode
    fs.setReplication(file1, (short)2);
  
    // Now get block details and check if the block is corrupt
    blocks = dfsClient.namenode.
              getBlockLocations(file1.toString(), 0, Long.MAX_VALUE);
    while (blocks.get(0).isCorrupt() != true) {
      try {
        LOG.info("Waiting until block is marked as corrupt...");
        Thread.sleep(1000);
      } catch (InterruptedException ie) {
      }
      blocks = dfsClient.namenode.
                getBlockLocations(file1.toString(), 0, Long.MAX_VALUE);
    }
    replicaCount = blocks.get(0).getLocations().length;
    assertTrue(replicaCount == 1);
    cluster.shutdown();
  }
  
  /**
   * Tests replication in DFS.
   */
  private void runReplication(boolean simulated,
      Class<? extends BlockPlacementPolicy> clazz) throws IOException {
    Configuration conf = new Configuration();
    conf.setClass("dfs.block.replicator.classname", clazz,
        BlockPlacementPolicy.class);
    conf.setBoolean("dfs.replication.considerLoad", false);
    if (simulated) {
      conf.setBoolean(SimulatedFSDataset.CONFIG_PROPERTY_SIMULATED, true);
    }    
    MiniDFSCluster cluster = null;
    FileSystem fileSys = null;
    DFSClient client = null;
    try {
      cluster = new MiniDFSCluster(conf, numDatanodes, racks, null,
          true, true);
      cluster.waitActive();
      cluster.getNameNode().namesystem.refreshNodes(conf);
      
      client = new DFSClient(cluster.getNameNode().getNameNodeAddress(), conf);
      DatanodeInfo[] info = client.datanodeReport(DatanodeReportType.LIVE);
      assertEquals("Number of Datanodes ", numDatanodes, info.length);
      fileSys = cluster.getFileSystem();

      Path file1 = new Path("/smallblocktest.dat");
      writeFile(fileSys, file1, 3);
      checkFile(fileSys, file1, 3);
      cleanupFile(fileSys, file1);
      writeFile(fileSys, file1, 10);
      checkFile(fileSys, file1, 10);
      cleanupFile(fileSys, file1);
      writeFile(fileSys, file1, 4);
      checkFile(fileSys, file1, 4);
      cleanupFile(fileSys, file1);
      writeFile(fileSys, file1, 1);
      checkFile(fileSys, file1, 1);
      cleanupFile(fileSys, file1);
      writeFile(fileSys, file1, 2);
      checkFile(fileSys, file1, 2);
      cleanupFile(fileSys, file1);
    } finally {
      if(client != null) {
        client.close();
      }
      if(fileSys != null)
        fileSys.close();
      if (cluster != null)
        cluster.shutdown();
    }
  }


  @Test
  public void testReplicationSimulatedStoragDefault() throws IOException {
    runReplication(true, BlockPlacementPolicyDefault.class);
  }

  @Test
  public void testReplicationDefault() throws IOException {
    runReplication(false, BlockPlacementPolicyDefault.class);
  }

  @Test
  public void testReplicationSimulatedStoragConfigurable() throws IOException {
    runReplication(true, BlockPlacementPolicyConfigurable.class);
  }
  
  @Test
  public void testReplicationConfigurable() throws IOException {
    runReplication(false, BlockPlacementPolicyConfigurable.class);
  }
  
  // Waits for all of the blocks to have expected replication
  private void waitForBlockReplication(String filename, 
                                       ClientProtocol namenode,
                                       int expected, long maxWaitSec) 
                                       throws IOException {
    waitForBlockReplication(filename, namenode, expected, maxWaitSec, false);
  }
  // Waits for all of the blocks to have expected replication
  private void waitForBlockReplication(String filename, 
                                       ClientProtocol namenode,
                                       int expected, long maxWaitSec,
                                       boolean isUnderConstruction) 
                                       throws IOException {
    long start = System.currentTimeMillis();
    
    //wait for all the blocks to be replicated;
    LOG.info("Checking for block replication for " + filename);
    while (true) {
      boolean replOk = true;
      LocatedBlocks blocks = namenode.getBlockLocations(filename, 0, 
                                                        Long.MAX_VALUE);
      
      for (Iterator<LocatedBlock> iter = blocks.getLocatedBlocks().iterator();
           iter.hasNext();) {
        LocatedBlock block = iter.next();
        if (isUnderConstruction && !iter.hasNext()) {
          break;  // do not check the last block
        }
        int actual = block.getLocations().length;
        if ( actual < expected ) {
            LOG.info("Not enough replicas for " + block.getBlock() +
                               " yet. Expecting " + expected + ", got " + 
                               actual + ".");
          replOk = false;
          break;
        }
      }
      
      if (replOk) {
        return;
      }
      
      if (maxWaitSec > 0 && 
          (System.currentTimeMillis() - start) > (maxWaitSec * 1000)) {
        throw new IOException("Timedout while waiting for all blocks to " +
                              " be replicated for " + filename);
      }
      
      try {
        Thread.sleep(500);
      } catch (InterruptedException ignored) {}
    }
  }

  /*
   * This test makes sure that NameNode retries all the available blocks for
   * under replicated blocks.
   *
   * It creates a file with one block and replication of 4. It corrupts two of
   * the blocks and removes one of the replicas. Expected behaviour is that
   * missing replica will be copied from one valid source.
   */
  @Test
  public void testPendingReplicationRetryDefault() throws IOException {
    runPendingReplicationRetry(BlockPlacementPolicyDefault.class);
  }

  @Test
  public void testPendingReplicationRetryConfigurable() throws IOException {
    runPendingReplicationRetry(BlockPlacementPolicyConfigurable.class);
  }
  
  /* This test makes sure that NameNode retries all the available blocks 
   * for under replicated blocks. 
   * 
   * It creates a file with one block and replication of 4. It corrupts 
   * two of the blocks and removes one of the replicas. Expected behaviour is
   * that missing replica will be copied from one valid source.
   */
  private void runPendingReplicationRetry(
      Class<? extends BlockPlacementPolicy> clazz) throws IOException {
    pendingReplicationRetryInternal(false, clazz);
  }

  @Test
  public void testPendingReplicationRetryInlineChecksum() throws IOException {
    pendingReplicationRetryInternal(true, BlockPlacementPolicyDefault.class);
  }
  
  private void pendingReplicationRetryInternal(boolean inlineChecksum,
    Class<? extends BlockPlacementPolicy> clazz)
      throws IOException {
    
    MiniDFSCluster cluster = null;
    int numDataNodes = 4;
    String testFile = "/replication-test-file";
    Path testPath = new Path(testFile);
    
    byte buffer[] = new byte[1024];
    for (int i=0; i<buffer.length; i++) {
      buffer[i] = '1';
    }
    
    try {
      Configuration conf = new Configuration();
      conf.setClass("dfs.block.replicator.classname", clazz,
          BlockPlacementPolicy.class);
      conf.set("dfs.replication", Integer.toString(numDataNodes));
      cluster = new MiniDFSCluster(conf, numDataNodes, racks4, null, true, true);

      cluster.waitActive();
      for (DataNode dn : cluster.getDataNodes()) {
        dn.useInlineChecksum = inlineChecksum;
      }

      DFSClient dfsClient = new DFSClient(new InetSocketAddress("localhost",
                                            cluster.getNameNodePort()),
                                            conf);
      
      OutputStream out = cluster.getFileSystem().create(testPath);
      out.write(buffer);
      out.close();
      
      waitForBlockReplication(testFile, dfsClient.namenode, numDataNodes, -1);

      // get first block of the file.
      Block block = dfsClient.namenode.
                       getBlockLocations(testFile, 0, Long.MAX_VALUE).
                       get(0).getBlock();
      File[] blockFiles = new File[6];
      for (int i=0; i<6; i++) {
        String fileName;
        if (!inlineChecksum) {
          fileName = block.getBlockName();
        } else {
          fileName = BlockInlineChecksumWriter.getInlineChecksumFileName(block,
              FSConstants.CHECKSUM_TYPE, cluster.conf.getInt(
                  "io.bytes.per.checksum",
                  FSConstants.DEFAULT_BYTES_PER_CHECKSUM));
        }
        blockFiles[i] = new File(cluster.getBlockDirectory("data" + (i+1)), fileName);
      }
      cluster.shutdown();
      cluster = null;
      
      //Now mess up some of the replicas.
      //Delete the first and corrupt the next two.
      
      for (int i=0; i<25; i++) {
        buffer[i] = '0';
      }
      
      int fileCount = 0;
      for (int i=0; i<6; i++) {
        File blockFile = blockFiles[i];
        LOG.info("Checking for file " + blockFile);
        
        if (blockFile.exists()) {
          if (fileCount == 0) {
            LOG.info("Deleting file " + blockFile);
            assertTrue(blockFile.delete());
          } else {
            // corrupt it.
            LOG.info("Corrupting file " + blockFile);
            long len = blockFile.length();
            assertTrue(len > 50);
            RandomAccessFile blockOut = new RandomAccessFile(blockFile, "rw");
            try {
              blockOut.seek(len/3);
              blockOut.write(buffer, 0, 25);
            } finally {
              blockOut.close();
            }
          }
          fileCount++;
        }
      }
      assertEquals(3, fileCount);
      
      /* Start the MiniDFSCluster with more datanodes since once a writeBlock
       * to a datanode node fails, same block can not be written to it
       * immediately. In our case some replication attempts will fail.
       */
      
      LOG.info("Restarting minicluster after deleting a replica and corrupting 2 crcs");
      conf = new Configuration();
      conf.setClass("dfs.block.replicator.classname", clazz,
          BlockPlacementPolicy.class);
      // first time format
      conf.set("dfs.replication", Integer.toString(numDataNodes));
      conf.set("dfs.replication.pending.timeout.sec", Integer.toString(2));
      conf.set("dfs.datanode.block.write.timeout.sec", Integer.toString(5));
      conf.set("dfs.safemode.threshold.pct", "0.75f"); // only 3 copies exist
      conf.setBoolean("dfs.use.inline.checksum", !inlineChecksum);
      
      cluster = new MiniDFSCluster(conf, numDataNodes * 2, racks, null, true,
          true, false);
      cluster.waitActive();
      dfsClient = new DFSClient(new InetSocketAddress("localhost",
                                  cluster.getNameNodePort()),
                                  conf);
      
      waitForBlockReplication(testFile, dfsClient.namenode, numDataNodes, -1);
      
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }  
  }

  private void testReplicateLenMismatchedBlockInternal(boolean inlineChecksum)
      throws Exception {
    Configuration conf = new Configuration();
    conf.setBoolean("dfs.use.inline.checksum", inlineChecksum);
    MiniDFSCluster cluster = new MiniDFSCluster(conf, 2, true, null);
    try {
      cluster.waitActive();
      // test truncated block
      changeBlockLen(cluster, -1, inlineChecksum);
      // test extended block
      changeBlockLen(cluster, 1, inlineChecksum);
    } finally {
      cluster.shutdown();
    }
  }

  
  /**
   * Test if replication can detect mismatched length on-disk blocks
   * @throws Exception
   */
  @Test
  public void testReplicateLenMismatchedBlock() throws Exception {
    testReplicateLenMismatchedBlockInternal(false);
  }

  /**
   * Test if replication can detect mismatched length on-disk blocks
   * @throws Exception
   */
  @Test
  public void testReplicateLenMismatchedBlockInlineChecksum() throws Exception {
    testReplicateLenMismatchedBlockInternal(true);
  }
  
  private void changeBlockLen(MiniDFSCluster cluster,
      int lenDelta, boolean isInlineChecksum) throws IOException,
      InterruptedException {
    final Path fileName = new Path("/file1");
    final short REPLICATION_FACTOR = (short)1;
    final FileSystem fs = cluster.getFileSystem();
    final int fileLen = fs.getConf().getInt("io.bytes.per.checksum", 512);
    DFSTestUtil.createFile(fs, fileName, fileLen, REPLICATION_FACTOR, 0);
    DFSTestUtil.waitReplication(fs, fileName, REPLICATION_FACTOR);

    String block;
    if (!isInlineChecksum) {
      block = DFSTestUtil.getFirstBlock(fs, fileName).getBlockName();
    } else {
      block = BlockInlineChecksumWriter.getInlineChecksumFileName(DFSTestUtil
          .getFirstBlock(fs, fileName), FSConstants.CHECKSUM_TYPE, cluster.conf
          .getInt("io.bytes.per.checksum",
              FSConstants.DEFAULT_BYTES_PER_CHECKSUM));
    }

    // Change the length of a replica
    for (int i=0; i<cluster.getDataNodes().size(); i++) {
      if (TestDatanodeBlockScanner.changeReplicaLength(block, i, lenDelta, cluster)) {
        break;
      }
    }

    // increase the file's replication factor
    fs.setReplication(fileName, (short)(REPLICATION_FACTOR+1));

    // block replication triggers corrupt block detection
    DFSClient dfsClient = new DFSClient(new InetSocketAddress("localhost", 
        cluster.getNameNodePort()), fs.getConf());
    LocatedBlocks blocks = dfsClient.namenode.getBlockLocations(
        fileName.toString(), 0, fileLen);
    if (lenDelta < 0) { // replica truncated
    	while (!blocks.get(0).isCorrupt() || 
    			REPLICATION_FACTOR != blocks.get(0).getLocations().length) {
    		Thread.sleep(100);
    		blocks = dfsClient.namenode.getBlockLocations(
    				fileName.toString(), 0, fileLen);
    	}
    } else { // no corruption detected; block replicated
    	while (!blocks.get(0).isCorrupt() && 
    	    REPLICATION_FACTOR +1 != blocks.get(0).getLocations().length) {
    		Thread.sleep(100);
    		blocks = dfsClient.namenode.getBlockLocations(
    				fileName.toString(), 0, fileLen);
    	}
    	LOG.info("Block is " + 
    	         (blocks.get(0).isCorrupt() ? "corrupted" : "healthy"));
    	LOG.info("Replication number: " + blocks.get(0).getLocations().length);
    }
    fs.delete(fileName, true);
  }
  
  /* This test makes sure that the blocks except for last one in an under
   * construction file are replicated.
   * 
   * It creates a file with one block and replication of 4. It corrupts 
   * two of the blocks and removes one of the replicas. Expected behaviour is
   * that missing replica will be copied from one valid source.
   */
  @Test
  public void testBlockReplicationInUCF() throws IOException {
    
    MiniDFSCluster cluster = null;
    short numDataNodes = 3;
    String testFile = "/replication-test-file";
    Path testPath = new Path(testFile);
    
    byte buffer[] = new byte[1024];
    for (int i=0; i<buffer.length; i++) {
      buffer[i] = '1';
    }
    
    try {
      Configuration conf = new Configuration();
      conf.set("dfs.replication", Integer.toString(numDataNodes-1));
      conf.setLong("dfs.block.size", 1024L);

      cluster = new MiniDFSCluster(0, conf, numDataNodes, true,
                                   true, null, null);
      cluster.waitActive();
      DFSClient dfsClient = new DFSClient(new InetSocketAddress("localhost",
                                            cluster.getNameNodePort()),
                                            conf);
      FileSystem fs = cluster.getFileSystem();
      OutputStream out = fs.create(testPath);
      out.write(buffer);
      out.write(buffer);
      
      waitForBlockReplication(testFile, dfsClient.namenode, numDataNodes-1, -1, true);

      // bump this file's replication factor
      fs.setReplication(testPath, numDataNodes);
            
      waitForBlockReplication(testFile, dfsClient.namenode, numDataNodes, 300, true);
      
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }  
  }

  /* 
   * Test if rate cap takes effective
   */
  @Test
  public void testRateCap() throws Exception {
    Configuration conf = new Configuration();
    conf.setLong("dfs.data.transfer.max.bytes.per.sec", 128 * 1024);
    FileSystem fs = null;
    MiniDFSCluster cluster = new MiniDFSCluster(conf, 2, true, null);
    try {
      cluster.waitActive();
      fs = cluster.getFileSystem();

      // Create file with replication factor of 1
      Path file1 = new Path("/tmp/testRateCap");
      DFSTestUtil.createFile(fs, file1, 512 * 1024, (short) 1, 0);
      DFSTestUtil.waitReplication(fs, file1, (short) 1);

      // Make sure replication doesn't finish too fast.
      long startTime = System.currentTimeMillis();
      fs.setReplication(file1, (short) 2);
      DFSTestUtil.waitReplication(fs, file1, (short) 2);
      long endTime = System.currentTimeMillis();
      long length = endTime - startTime;
      System.out.println("Taking " + length + " ms to replicate.");
      TestCase.assertTrue(endTime - startTime > 3700);
    } finally {
      cluster.shutdown();
    }
  }
}
