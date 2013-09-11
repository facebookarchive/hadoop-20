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
package org.apache.hadoop.hdfs.server.namenode;


import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.FSConstants.DatanodeReportType;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.namenode.DatanodeDescriptor;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.security.UnixUserGroupInformation;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.*;

public class TestDeprioritizeSlowDatanodes {

  private static MiniDFSCluster cluster;
  private static Configuration conf;
  private static final int BLOCK_SIZE = 1024;
  private static final int BLOCKS = 10;
  private static final int FILE_SIZE = BLOCKS * BLOCK_SIZE;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    conf = new Configuration();
    conf.setInt("dfs.block.size", BLOCK_SIZE);
    conf.setLong("dfs.heartbeat.timeout.millis", 10000); // 10 sec
    System.setProperty("test.build.data", "build/test/data1");
    cluster = new MiniDFSCluster(conf, 6, true, new String[] { "/r1", "/r1",
        "/r1", "/r2", "/r2", "/r2" }, new String[] { "h1", "h2", "h3", "h4",
        "h5", "h6" });
    cluster.waitActive(true);
    updateDatanodeMap(cluster);
  }
  
  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    cluster.shutdown();
  }

  private static DatanodeID createDataNodeID(DataNode dn) {
    String ipAddr = dn.getSelfAddr().getAddress().getHostAddress();
    int port = dn.getPort();
    String storageID = dn.getStorageID();

    DatanodeID dnId = new DatanodeID(ipAddr + ":" + port);
    dnId.setStorageID(storageID);
    return dnId;
  }

  /**
   * Does a lot of hacks to change namenode and datanode datastructures to
   * identify datanodes by the machine name rather than the IP address. This is
   * done since we can give each datanode a different hostname in a unit test
   * but not a different ip address.
   * 
   * @param cluster
   *          the {@link MiniDFSCluster} to operate on
   * @throws Exception
   */
  private static void updateDatanodeMap(MiniDFSCluster cluster)
      throws Exception {
    FSNamesystem namesystem = cluster.getNameNode().namesystem;
    for (DataNode node : cluster.getDataNodes()) {
      // Get old descriptor.
      DatanodeID dnId = createDataNodeID(node);
      DatanodeDescriptor dnDs = namesystem.getDatanode(dnId);

      // Create new id and descriptor.
      DatanodeID newId = new DatanodeID(node.getMachineName(),
          dnDs.getStorageID(), dnDs.getInfoPort(), dnDs.getIpcPort());
      DatanodeDescriptor newDS = new DatanodeDescriptor(newId,
          dnDs.getNetworkLocation(), dnDs.getHostName(), dnDs.getCapacity(),
          dnDs.getDfsUsed(), dnDs.getRemaining(), dnDs.getNamespaceUsed(),
          dnDs.getXceiverCount());
      
      newDS.isAlive = true;
      // Overwrite NN maps with new descriptor.
      namesystem.writeLock();
      namesystem.clusterMap.remove(dnDs);
      namesystem.resolveNetworkLocation(newDS);
      namesystem.unprotectedAddDatanode(newDS);
      namesystem.clusterMap.add(newDS);
      namesystem.writeUnlock();
      // Overwrite DN map with new registration.
      node.setRegistrationName(node.getMachineName());
    }
  }
  
  @Test
  public void testDepriotizeSlowDatanodes() throws Exception {
    // Create source file.
    String fileName = "/testDepriotizeSlowDatanodes";
    DFSTestUtil.createFile(cluster.getFileSystem(), new Path(fileName),
        (long) FILE_SIZE, (short) 3, (long) 0);

    // Create RPC connections
    ClientProtocol srcNamenode = DFSClient.createRPCNamenode(
        NameNode.getAddress(cluster.getFileSystem().getUri().getAuthority()),
        conf, UnixUserGroupInformation.login(conf, true), 0).getProxy();

    // Create destination file.
    LocatedBlocks lbksBeforeKill = srcNamenode.getBlockLocations(fileName, 0,
        Long.MAX_VALUE);
    LocatedBlock blkBeforeKill = lbksBeforeKill.get(0); // choose one block
    DatanodeInfo[] locsBeforeKill = blkBeforeKill.getLocations();
    DatanodeInfo toKill = locsBeforeKill[0];
    
    // kill datanode
    Thread.sleep(10000); // 10 sec
    
    LocatedBlocks lbksAfterKill = srcNamenode.getBlockLocations(fileName, 0,
        Long.MAX_VALUE);
    LocatedBlock blkAfterKill = lbksAfterKill.get(0); // choose one block
    DatanodeInfo[] locsAfterKill = blkAfterKill.getLocations();
    
    assert(locsAfterKill.length == locsBeforeKill.length);
    assert(locsAfterKill.length > 1);
    // assert the the killed node is now at the bottom
    assert(locsAfterKill[locsAfterKill.length - 1].equals(toKill));
    
    fileName = "/testDepriotizeSlowDatanodes-2";
    DFSTestUtil.createFile(cluster.getFileSystem(), new Path(fileName),
        (long) FILE_SIZE, (short) 3, (long) 0);
    LocatedBlocks lbks = srcNamenode.getBlockLocations(fileName, 0,
        Long.MAX_VALUE);
    for (LocatedBlock lbk : lbks.getLocatedBlocks()) {
      DatanodeInfo[] locs = lbk.getLocations();
      for (DatanodeInfo dstloc : locs) {
        assert(!dstloc.equals(toKill));
      }
    }
    
    // assert that toKill was still not decommissioned
    // A failed assertion does not signify a bug. It just
    // means that our test may not have tested the correct 
    // behavior because the dataNode was considered dead by NN.
    DatanodeInfo[] deadDataNodes = 
        srcNamenode.getDatanodeReport(DatanodeReportType.DEAD);
    assert(!Arrays.<DatanodeInfo>asList(deadDataNodes).contains(toKill));
  }
}
