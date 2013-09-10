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

public class TestFavoredNodes {

  private static MiniDFSCluster cluster;
  private static Configuration conf;
  private static MiniDFSCluster remoteCluster;
  private static Configuration remoteConf;
  private static final int BLOCK_SIZE = 1024;
  private static final int BLOCKS = 10;
  private static final int FILE_SIZE = BLOCKS * BLOCK_SIZE;
  private static final Random r = new Random();

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    conf = new Configuration();
    conf.setInt("dfs.block.size", BLOCK_SIZE);
    // make heartbeat very infrequent to avoid race condition
    conf.setLong("dfs.heartbeat.interval", 500);
    remoteConf = new Configuration(conf);
    System.setProperty("test.build.data", "build/test/data1");
    cluster = new MiniDFSCluster(conf, 6, true, new String[] { "/r1", "/r1",
        "/r1", "/r2", "/r2", "/r2" }, new String[] { "h1", "h2", "h3", "h4",
        "h5", "h6" });
    cluster.waitActive(true);
    updateDatanodeMap(cluster);

    System.setProperty("test.build.data", "build/test/data2");
    remoteCluster = new MiniDFSCluster(remoteConf, 6, true, new String[] {
        "/r1", "/r1", "/r1", "/r2", "/r2", "/r2" }, new String[] { "h1", "h2",
        "h3", "h4", "h5", "h6" });
    remoteCluster.waitActive(true);
    updateDatanodeMap(remoteCluster);
  }
  
  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    cluster.shutdown();
    remoteCluster.shutdown();
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
  public void testCrossFileSystemAddBlock() throws Exception {
    // Create source file.
    String fileName = "/testCrossFileSystemAddBlock";
    DFSTestUtil.createFile(cluster.getFileSystem(), new Path(fileName),
        (long) FILE_SIZE, (short) 3, (long) 0);

    // Create RPC connections
    ClientProtocol dstNamenode = DFSClient.createRPCNamenode(
        NameNode.getAddress(remoteCluster.getFileSystem().getUri()
            .getAuthority()), remoteConf,
        UnixUserGroupInformation.login(remoteConf, true), 0).getProxy();
    ClientProtocol srcNamenode = DFSClient.createRPCNamenode(
        NameNode.getAddress(cluster.getFileSystem().getUri().getAuthority()),
        conf, UnixUserGroupInformation.login(conf, true), 0).getProxy();

    // Create destination file.
    String dstFile = "/dst" + fileName;
    FileStatus srcStat = cluster.getFileSystem().getFileStatus(
        new Path(fileName));
    String clientName = "testClient";
    dstNamenode.create(dstFile, srcStat.getPermission(), clientName, true,
        true, srcStat.getReplication(), srcStat.getBlockSize());
    FSNamesystem dstNamesystem = remoteCluster.getNameNode().getNamesystem();

    LocatedBlocks lbks = srcNamenode.getBlockLocations(fileName, 0,
        Long.MAX_VALUE);
    for (LocatedBlock lbk : lbks.getLocatedBlocks()) {
      DatanodeInfo[] locs = lbk.getLocations();
      int slice = r.nextInt(locs.length);
      LocatedBlock dstlbk = dstNamenode.addBlock(dstFile, clientName, null,
          Arrays.copyOfRange(locs, 0, slice + 1));
      DatanodeInfo[] dstlocs = dstlbk.getLocations();
      List<String> dstlocHostnames = new ArrayList<String>(dstlocs.length);
      for (DatanodeInfo dstloc : dstlocs) {
        dstlocHostnames.add(dstloc.getHostName());
      }
      assertEquals(conf.getInt("dfs.replication", 3), dstlocs.length);
      for (int i = 0; i <= slice; i++) {
        assertTrue("Expected " + locs[i].getHostName() + " was not found",
            dstlocHostnames.contains(locs[i].getHostName()));
        // Allows us to make the namenode think that these blocks have been
        // successfully written to the datanode, helps us to add the next block
        // without completing the previous block.
        dstNamesystem.blocksMap.addNode(dstlbk.getBlock(),
            dstNamesystem.getDatanode(dstlocs[i]), srcStat.getReplication());
      }
    }
  }

  @Test
  public void testPartiallySpecifiedFavoredNodes() throws Exception {
    // Create source file.
    String fileName = "/testPartiallySpecifiedFavoredNodes";
    DFSTestUtil.createFile(cluster.getFileSystem(), new Path(fileName),
        (long) FILE_SIZE, (short) 3, (long) 0);

    // Create RPC connections
    ClientProtocol srcNamenode = DFSClient.createRPCNamenode(
        NameNode.getAddress(cluster.getFileSystem().getUri().getAuthority()),
        conf, UnixUserGroupInformation.login(conf, true), 0).getProxy();

    // Create destination file.
    String dstFile = "/dst" + fileName;
    FileStatus srcStat = cluster.getFileSystem().getFileStatus(
        new Path(fileName));
    String clientName = "testClient";
    srcNamenode.create(dstFile, srcStat.getPermission(), clientName, true,
        true, srcStat.getReplication(), srcStat.getBlockSize());
    FSNamesystem dstNamesystem = cluster.getNameNode().getNamesystem();

    LocatedBlocks lbks = srcNamenode.getBlockLocations(fileName, 0,
        Long.MAX_VALUE);
    for (LocatedBlock lbk : lbks.getLocatedBlocks()) {
      DatanodeInfo[] locs = lbk.getLocations();

      // Partially-specified nodes have only hostname and port.
      DatanodeInfo[] partialLocs = new DatanodeInfo[locs.length];
      for (int i = 0; i < partialLocs.length; i++) {
        partialLocs[i] = new DatanodeInfo(new DatanodeID(locs[i].getName()));
      }

      int slice = r.nextInt(locs.length);
      LocatedBlock dstlbk = srcNamenode.addBlock(dstFile, clientName, null,
          Arrays.copyOfRange(partialLocs, 0, slice + 1));
      List<DatanodeInfo>dstlocs = Arrays.asList(dstlbk.getLocations());
      assertEquals(conf.getInt("dfs.replication", 3), dstlocs.size());
      for (int i = 0; i <= slice; i++) {
        assertTrue("Expected " + locs[i].getName() + " was not found",
            dstlocs.contains(locs[i]));
        // Allows us to make the namenode think that these blocks have been
        // successfully written to the datanode, helps us to add the next block
        // without completing the previous block.
        dstNamesystem.blocksMap.addNode(dstlbk.getBlock(),
            dstNamesystem.getDatanode(dstlocs.get(i)),
            srcStat.getReplication());
      }
    }
  }
}
