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

import org.apache.commons.logging.impl.Log4JLogger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.namenode.LeaseManager;
import org.apache.hadoop.io.IOUtils;
import org.apache.log4j.Level;

/**
 * This class tests blockRecovery can throw exception if deadline reaches.
 */
public class TestBlockRecovery extends junit.framework.TestCase {
  static final String DIR = "/" + TestBlockRecovery.class.getSimpleName() + "/";

  {
    // ((Log4JLogger)DataNode.LOG).getLogger().setLevel(Level.ALL);
    ((Log4JLogger) LeaseManager.LOG).getLogger().setLevel(Level.ALL);
    ((Log4JLogger) FSNamesystem.LOG).getLogger().setLevel(Level.ALL);
    ((Log4JLogger) DFSClient.LOG).getLogger().setLevel(Level.ALL);
  }

  static final long seed = 0xDEADBEEFL;
  static final int blockSize = 8192;
  static final int numBlocks = 2;
  static final int fileSize = numBlocks * blockSize + 1;
  boolean simulatedStorage = false;
  boolean federation = false;
  static final int numNameNodes = 3;

  /**
   * Create a file, write something, and try to do a block recovery with
   * deadline is current time
   */
  public void testBlockRecoveryTimeout() throws Exception {
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
      dfs = (DistributedFileSystem) cluster.getFileSystem();

      // create a new file.
      final String f = "/testBlockRecoveryTimeout";
      final Path fpath = new Path(f);
      FSDataOutputStream out = TestFileCreation.createFile(dfs, fpath,
          DATANODE_NUM);
      out.write("something".getBytes());
      out.sync();

      LocatedBlocks locations = dfs.dfs.namenode.getBlockLocations(f, 0,
          Long.MAX_VALUE);
      assertEquals(1, locations.locatedBlockCount());
      LocatedBlock locatedblock = locations.getLocatedBlocks().get(0);
      int nsId = cluster.getNameNode().getNamespaceID();

      for (DatanodeInfo datanodeinfo : locatedblock.getLocations()) {
        DataNode datanode = cluster.getDataNode(datanodeinfo.ipcPort);
        try {
          datanode.recoverBlock(nsId, locatedblock.getBlock(), true,
              locatedblock.getLocations(), System.currentTimeMillis());
          TestCase.fail("recoverBlock didn't throw timeout exception");
        } catch (DataNode.BlockRecoveryTimeoutException e) {

        }
        break;
      }
    } finally {
      IOUtils.closeStream(dfs);
      cluster.shutdown();
    }
    System.out.println("testLeaseExpireHardLimit successful");
  }
}
