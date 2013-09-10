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

import java.io.DataOutputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Random;

import junit.framework.TestCase;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.impl.Log4JLogger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.BlockMissingException;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.ChecksumException;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.BlockListAsLongs;
import org.apache.hadoop.hdfs.protocol.FSConstants.SafeModeAction;
import org.apache.hadoop.hdfs.protocol.FSConstants.DatanodeReportType;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.server.common.GenerationStamp;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.log4j.Level;

/**
 * A JUnit test for corrupted file handling.
 */
public class TestBlockReportProcessingTime extends TestCase {
  {
    ((Log4JLogger)NameNode.stateChangeLog).getLogger().setLevel(Level.ALL);
    ((Log4JLogger)FSNamesystem.LOG).getLogger().setLevel(Level.ALL);
    ((Log4JLogger)DFSClient.LOG).getLogger().setLevel(Level.ALL);
    DataNode.LOG.getLogger().setLevel(Level.ALL);
  }
  static Log LOG = ((Log4JLogger)NameNode.stateChangeLog);

  public TestBlockReportProcessingTime(String testName) {
    super(testName);
  }

  protected void setUp() throws Exception {
  }

  protected void tearDown() throws Exception {
  }
  
  /** Test the case when a block report processing at namenode
   * startup time is fast.
   */
  public void testFasterBlockReports() throws Exception {
    MiniDFSCluster cluster = null;
    try {
      Configuration conf = new Configuration();
      cluster = new MiniDFSCluster(conf, 40, true, null);
      cluster.waitActive();
      FileSystem fs = cluster.getFileSystem();
      NameNode namenode = cluster.getNameNode();
      LOG.info("Cluster Alive."); 

      // create a single file with one block.
      Path file1 = new Path("/filestatus.dat");
      final long FILE_LEN = 1L;
      DFSTestUtil.createFile(fs, file1, FILE_LEN, (short)2, 1L);
      LocatedBlocks locations = namenode.getBlockLocations(
                                  file1.toString(), 0, Long.MAX_VALUE);
      assertTrue(locations.locatedBlockCount() == 1);
      Block block = locations.get(0).getBlock();
      long blkid = block.getBlockId();
      long genstamp = block.getGenerationStamp();
      long length = block.getNumBytes();
      
      // put namenode in safemode
      namenode.setSafeMode(SafeModeAction.SAFEMODE_ENTER);
      DatanodeInfo[] dinfo = namenode.getDatanodeReport(DatanodeReportType.ALL);
      LOG.info("Found " + dinfo.length + " number of datanodes.");

      // create artificial block replicas on each datanode
      final int NUMBLOCKS = 1000;
      final int LONGS_PER_BLOCK = 3;
      long tmpblocks[] = new long[NUMBLOCKS * LONGS_PER_BLOCK];
      for (int i = 0; i < NUMBLOCKS; i++) {
        tmpblocks[i * LONGS_PER_BLOCK] = blkid;
        tmpblocks[i * LONGS_PER_BLOCK + 1] = length;
        tmpblocks[i * LONGS_PER_BLOCK + 2] = genstamp;
      }
      BlockListAsLongs blkList = new BlockListAsLongs(tmpblocks);

      // process block report from all machines
      long total = 0;
      for (int i = 0; i < dinfo.length; i++) {
        long start = now();
        namenode.namesystem.processReport(dinfo[i], blkList);
        total += now() - start;
        LOG.info("Processed block report from " + dinfo[i]);
      }
      LOG.info("Average of all block report processing time " +
               " from " + dinfo.length + " datanodes is " +
               (total/dinfo.length) + " milliseconds.");
      
    } finally {
      if (cluster != null) { cluster.shutdown(); }
    }
  }

  static long now() {
    return System.currentTimeMillis();
  }

}
