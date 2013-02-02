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

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;

import junit.framework.TestCase;

import org.apache.commons.logging.impl.Log4JLogger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSClient.MultiDataOutputStream;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.namenode.LeaseManager;
import org.apache.hadoop.hdfs.util.InjectionEvent;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.InjectionEventI;
import org.apache.hadoop.util.InjectionHandler;
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
    DistributedFileSystem dfs = null;
    MiniDFSCluster cluster = null;
    try {
      cluster = new MiniDFSCluster(conf, DATANODE_NUM, true, null);
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
      if (cluster != null) {
        cluster.shutdown();
      }
    }
    System.out.println("testLeaseExpireHardLimit successful");
  }
  
  public void testReadBlockRecovered() throws Exception {
    System.out.println("testReadBlockRecovered start");
    final int DATANODE_NUM = 3;

    Configuration conf = new Configuration();
    conf.setInt("heartbeat.recheck.interval", 1000);
    conf.setInt("dfs.heartbeat.interval", 1);

    // create cluster
    DistributedFileSystem dfs = null;
    MiniDFSCluster cluster = null;
    try {
      cluster = new MiniDFSCluster(conf, DATANODE_NUM, true, null);
      cluster.waitActive();
      dfs = (DistributedFileSystem) cluster.getFileSystem();

      // create a new file.
      final String f = "/testReadBlockRecovered";
      final Path fpath = new Path(f);
      FSDataOutputStream out = TestFileCreation.createFile(dfs, fpath,
          DATANODE_NUM);
      out.write(new byte[512 * 2]);
      out.sync();

      FSDataInputStream in = dfs.open(fpath);

      in.read(new byte[512]);

      // By closing the pipeline connection, force a block recovery
      InjectionHandler.set(new InjectionHandler() {
        int thrownCount = 0;

        @Override
        protected void _processEventIO(InjectionEventI event, Object... args)
            throws IOException {
          if (event == InjectionEvent.DFSCLIENT_DATASTREAM_AFTER_WAIT
              && thrownCount < 1) {
            thrownCount++;
            MultiDataOutputStream blockStream = (MultiDataOutputStream) args[0];
            blockStream.close();
          }
        }
      });

      out.write(new byte[512 * 2]);
      out.sync();

      InjectionHandler.set(new InjectionHandler() {
        int thrownCount = 0;

        @SuppressWarnings("unchecked")
        @Override
        protected void _processEventIO(InjectionEventI event, Object... args)
            throws IOException {
          if (event == InjectionEvent.DFSCLIENT_READBUFFER_BEFORE
              && thrownCount < 1) {
            // Fail one readBuffer() and put all nodes in dead node list to
            // trigger a refetching of metadata.
            thrownCount++;
            ConcurrentHashMap<DatanodeInfo, DatanodeInfo> deadNodes =
                (ConcurrentHashMap<DatanodeInfo, DatanodeInfo>) args[0];
            DFSLocatedBlocks locatedBlocks = (DFSLocatedBlocks) args[1];

            for (DatanodeInfo dinfo : locatedBlocks.get(0).getLocations()) {
              deadNodes.put(dinfo, dinfo);
            }
            throw new IOException("injected exception");
          } else if (event == InjectionEvent.DFSCLIENT_READBUFFER_AFTER) {
            // Make sure a correct replica, not the out-of-date one is ised.
            // Verifying that by making sure the right metadata is returned.
            BlockReader br = (BlockReader) args[0];
            if (br.blkLenInfoUpdated) {
              TestCase.assertTrue(br.isBlockFinalized);
              TestCase.assertEquals(2048, br.getUpdatedBlockLength());
            }
          }
        }
      });
      out.close();

      in.read(new byte[512]);

      in.close();

    } finally {
      IOUtils.closeStream(dfs);
      if (cluster != null) {
        cluster.shutdown();
      }
    }
    System.out.println("testReadBlockRecovered successful");
  }
}
