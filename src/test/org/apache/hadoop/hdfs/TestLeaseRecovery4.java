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
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.net.InetSocketAddress;

import junit.framework.TestCase;

import org.apache.commons.logging.impl.Log4JLogger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.server.protocol.BlockAlreadyCommittedException;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
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
import org.apache.hadoop.util.Progressable;
import org.apache.log4j.Level;


/**
 * This class tests that another updateBlock from the same old block should fail
 */
public class TestLeaseRecovery4 extends junit.framework.TestCase {
  static final String DIR = "/" + TestLeaseRecovery4.class.getSimpleName() + "/";

  {
    //((Log4JLogger)DataNode.LOG).getLogger().setLevel(Level.ALL);
    ((Log4JLogger)LeaseManager.LOG).getLogger().setLevel(Level.ALL);
    ((Log4JLogger)FSNamesystem.LOG).getLogger().setLevel(Level.ALL);
    ((Log4JLogger)DFSClient.LOG).getLogger().setLevel(Level.ALL);
  }

  static final long seed = 0xDEADBEEFL;
  static final int blockSize = 8192;
  static final int numBlocks = 2;
  static final int fileSize = numBlocks * blockSize + 1;
  boolean simulatedStorage = false;
  boolean federation = false;
  static final int numNameNodes = 3;
  
  /**
   * Create a file, write something, and try to updateBlock to a new Block.
   * Then it tries to updateBlock from the old block to a new block, it needs
   * to fail.
   */
  public void testConcurrentLeaseRecovery() throws Exception {
    System.out.println("testConcurrentLeaseRecovery start");
    final int DATANODE_NUM = 3;

    Configuration conf = new Configuration();
    conf.setInt("heartbeat.recheck.interval", 1000);
    conf.setInt("dfs.heartbeat.interval", 1);

    // create cluster
    MiniDFSCluster cluster = new MiniDFSCluster(conf, DATANODE_NUM, true, null);
    DistributedFileSystem dfs = null;
    try {
      cluster.waitActive();
      dfs = (DistributedFileSystem)cluster.getFileSystem();

      // create a new file.
      final String f = "/testConcurrentLeaseRecovery";
      final Path fpath = new Path(f);
      FSDataOutputStream out = TestFileCreation.createFile(dfs, fpath, DATANODE_NUM);
      out.write("something".getBytes());
      out.sync();
      int actualRepl = ((DFSOutputStream)(out.getWrappedStream())).
                        getNumCurrentReplicas();
      assertTrue(f + " should be replicated to " + DATANODE_NUM + " datanodes.",
                 actualRepl == DATANODE_NUM);


      LocatedBlocks locations = dfs.dfs.namenode.getBlockLocations(
          f, 0, Long.MAX_VALUE);
      assertEquals(1, locations.locatedBlockCount());
      LocatedBlock locatedblock = locations.getLocatedBlocks().get(0);
      int nsId = cluster.getNameNode().getNamespaceID();
      for(DatanodeInfo datanodeinfo: locatedblock.getLocations()) {
        DataNode datanode = cluster.getDataNode(datanodeinfo.ipcPort);
        FSDataset dataset = (FSDataset)datanode.data;
        Block b = dataset.getStoredBlock(nsId, locatedblock.getBlock().getBlockId());
        Block newBlock = new Block(b);
        newBlock.setGenerationStamp(6661);
        dataset.updateBlock(nsId, b, newBlock);

        Block newBlock1 = new Block(b);
        newBlock1.setGenerationStamp(6662);
        boolean hitException = false;
        try {
          dataset.updateBlock(nsId, b, newBlock1);
        } catch (IOException e) {
          hitException = true;
        }
        TestCase.assertTrue("Shouldn't allow update block when generation doesn't match", hitException);
        dataset.updateBlock(nsId, newBlock, newBlock1);
      }
    } finally {
      IOUtils.closeStream(dfs);
      cluster.shutdown();
    }
    System.out.println("testLeaseExpireHardLimit successful");
  } 
  /**
   * Create a file, write something, commit the block through namenode and
   * try to triger a block recover. Make sure it fails in the way expected. 
   */
  public void testAlreadyCommittedBlockException() throws Exception {
    System.out.println("testAlreadyCommittedBlockException");
    final int DATANODE_NUM = 3;

    Configuration conf = new Configuration();

    // create cluster
    MiniDFSCluster cluster = new MiniDFSCluster(conf, DATANODE_NUM, true, null);
    DistributedFileSystem dfs = null;
    try {
      cluster.waitActive();
      dfs = (DistributedFileSystem)cluster.getFileSystem();

      // create a new file.
      final String f = "/testAlreadyCommittedBlockException";
      final Path fpath = new Path(f);
      FSDataOutputStream out = TestFileCreation.createFile(dfs, fpath, DATANODE_NUM);
      out.write("something".getBytes());
      out.sync();

      LocatedBlocks locations = dfs.dfs.namenode.getBlockLocations(
          f, 0, Long.MAX_VALUE);
      assertEquals(1, locations.locatedBlockCount());
      LocatedBlock locatedblock = locations.getLocatedBlocks().get(0);

      // Force commit the block
      cluster.getNameNode().commitBlockSynchronization(locatedblock.getBlock(),
          locatedblock.getBlock().getGenerationStamp(),
          locatedblock.getBlockSize(), true, false, new DatanodeID[0]);
      
      // Force block recovery from the ongoing stream
      for(DatanodeInfo datanodeinfo: locatedblock.getLocations()) {
        DataNode datanode = cluster.getDataNode(datanodeinfo.ipcPort);        
        datanode.shutdown();
        break;
      }
      // Close the file and make sure the failure thrown is BlockAlreadyCommittedException 
      try {
        out.close();
        TestCase.fail();
      } catch (BlockAlreadyCommittedException e) {
        TestCase
        .assertTrue(((DFSOutputStream) out.getWrappedStream()).lastException instanceof BlockAlreadyCommittedException);
      }
    } finally {
      IOUtils.closeStream(dfs);
      cluster.shutdown();
    }
    System.out.println("testAlreadyCommittedBlockException successful");
  } 

}
