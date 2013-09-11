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
import java.util.List;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.raid.Utils;
import junit.framework.TestCase;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.*;
import java.util.*;

/**
 * This class tests the FaultTolerantBlockPlacementPolicy policy.
 * The policy tries to spread the blocks of a file evenly over all the
 * available racks.
 *
 * This particular test creates a dummy file on a dummy cluster
 * with some number of racks with each rack having some number of hosts.
 * At the end of the test, it checks that all racks have equal number
 * of blocks for a file placed.
 */
public class TestFaultTolerantPlacementPolicy {

  private static MiniDFSCluster cluster;
  private static Configuration conf;
  private static final int BLOCK_SIZE = 1;
  private static final int RS_STRIPE_LEN = 10;
  private static final int RS_PARITY_LEN = 4;
  private static final int XOR_PARITY_LEN = 1;
  private static final int XOR_STRIPE_LEN = 5;
  private static final int NUM_RACKS = RS_STRIPE_LEN+RS_PARITY_LEN+4;
  private static final int BLOCKS_PER_RACK = 2;
  private static final String FILENAME = "testFile";

  @BeforeClass 
  public static void setUpBeforeClass() throws Exception {
    conf = new Configuration();
    conf.setLong("dfs.blockreport.intervalMsec", 1000L);
    conf.set("dfs.replication.pending.timeout.sec", "2");
    conf.setLong("dfs.block.size", BLOCK_SIZE);
    conf.set("dfs.block.replicator.classname",
             "org.apache.hadoop.hdfs.server.namenode.FaultTolerantBlockPlacementPolicy");
    conf.setInt("io.bytes.per.checksum", 1);
    Utils.loadTestCodecs(conf,
                         XOR_STRIPE_LEN,
                         RS_STRIPE_LEN,
                         XOR_PARITY_LEN,
                         RS_PARITY_LEN,
                         "/raid",
                         "/raidrs");
    int numBlocks = NUM_RACKS * BLOCKS_PER_RACK;
    List<String> racks = new ArrayList<String>(NUM_RACKS);
    List<String> hosts = new ArrayList<String>(numBlocks);
    for (int i = 0; i < numBlocks; ++i) {
      String rack = new String("/r" + i%NUM_RACKS);
      String host = new String("h" + i);
      hosts.add(host);
      racks.add(rack);
    }
    cluster = new MiniDFSCluster(conf,
                                 numBlocks,
                                 true,
                                 racks.toArray(new String[numBlocks]),
                                 hosts.toArray(new String[numBlocks]));
    cluster.waitActive(true);
    updateDatanodeMap(cluster);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    cluster.shutdown();
  }

  private void updateRackCount(
    LocatedBlocks blocks,
    int stripeSize,
    HashMap<Integer, Set<String>> stripeToRacks) {
    for (LocatedBlock b : blocks.getLocatedBlocks()) {
      for (DatanodeInfo i : b.getLocations()) {
        String rack = i.getNetworkLocation();
        long stripe = b.getStartOffset()/(stripeSize*BLOCK_SIZE);
        Integer stripeIdx = new Integer((int)stripe);
        Set<String> racks = stripeToRacks.get(stripeIdx);
        if (racks == null) {
          racks = new HashSet<String>();
        }
        assertFalse(racks.contains(rack));
        racks.add(rack);
      }
    }
  }

  private void updateRackCount(LocatedBlocks blocks,
                               HashMap<String, Integer> rackMapCount) {
    for (LocatedBlock b : blocks.getLocatedBlocks()) {
      for (DatanodeInfo i : b.getLocations()) {
        String rack = i.getNetworkLocation();
        Integer count = rackMapCount.get(rack);
        if (count == null) {
          rackMapCount.put(rack, new Integer(1));
        } else {
          int newCount = count.intValue() + 1;
          rackMapCount.put(rack, newCount);
        }
      }
    }
  }

  @Test
  public void testChooseTargetRackBased() throws Exception {
    String fileName = new String("/hay/" + FILENAME);
    DFSTestUtil.createFile(cluster.getFileSystem(),
                           new Path(fileName),
                           (long) RS_STRIPE_LEN*BLOCK_SIZE*BLOCKS_PER_RACK,
                           (short)1, // replication
                           (long)0);
    HashMap<Integer, Set<String>> stripeToRacksXOR =
      new HashMap<Integer, Set<String>>();
    HashMap<Integer, Set<String>> stripeToRacksRS =
      new HashMap<Integer, Set<String>>();
    LocatedBlocks blocks =
      cluster.getNameNode().namesystem.getBlockLocations(fileName,
                                                         0,
                                                         Long.MAX_VALUE);
    updateRackCount(blocks, XOR_STRIPE_LEN, stripeToRacksXOR);
    updateRackCount(blocks, RS_STRIPE_LEN, stripeToRacksRS);

    // Now test creating the corresponding parity file
    fileName = new String("/raidrs/hay/" + FILENAME);
    DFSTestUtil.createFile(cluster.getFileSystem(),
                           new Path(fileName),
                           (long) RS_PARITY_LEN*BLOCK_SIZE*BLOCKS_PER_RACK,
                           (short)1, // replication
                           (long)0);
    blocks =
      cluster.getNameNode().namesystem.getBlockLocations(fileName,
                                                         0,
                                                         Long.MAX_VALUE);
    updateRackCount(blocks, RS_STRIPE_LEN, stripeToRacksRS);

    // Now try creating a xor file.
    fileName = new String("/raid/hay/" + FILENAME);
    DFSTestUtil.createFile(cluster.getFileSystem(),
                           new Path(fileName),
                           (long) XOR_PARITY_LEN*BLOCK_SIZE*BLOCKS_PER_RACK,
                           (short)1, // replication
                           (long)0);
    blocks =
      cluster.getNameNode().namesystem.getBlockLocations(fileName,
                                                         0,
                                                         Long.MAX_VALUE);
    updateRackCount(blocks, XOR_STRIPE_LEN, stripeToRacksXOR);
  }

  // XXX: Copied from TestFavoredNode. Should go to some util
  private static DatanodeID createDataNodeID(DataNode dn) {
    String ipAddr = dn.getSelfAddr().getAddress().getHostAddress();
    int port = dn.getPort();
    String storageID = dn.getStorageID();

    DatanodeID dnId = new DatanodeID(ipAddr + ":" + port);
    dnId.setStorageID(storageID);
    return dnId;
  }

  // XXX: Copied from TestFavoredNode. Should go to some util
  public static void updateDatanodeMap(MiniDFSCluster cluster)
      throws Exception {
    FSNamesystem namesystem = cluster.getNameNode().namesystem;
    for (DataNode node : cluster.getDataNodes()) {
      DatanodeID dnId = createDataNodeID(node);
      DatanodeDescriptor dnDs = namesystem.getDatanode(dnId);
      DatanodeID newId = new DatanodeID(node.getMachineName(),
                                        dnDs.getStorageID(),
                                        dnDs.getInfoPort(),
                                        dnDs.getIpcPort());
      DatanodeDescriptor newDS =
        new DatanodeDescriptor(newId,
                               dnDs.getNetworkLocation(),
                               dnDs.getHostName(),
                               dnDs.getCapacity(),
                               dnDs.getDfsUsed(),
                               dnDs.getRemaining(),
                               dnDs.getNamespaceUsed(),
                               dnDs.getXceiverCount());
      newDS.isAlive = true;
      namesystem.writeLock();
      namesystem.clusterMap.remove(dnDs);
      namesystem.resolveNetworkLocation(newDS);
      namesystem.unprotectedAddDatanode(newDS);
      namesystem.clusterMap.add(newDS);
      namesystem.writeUnlock();
      node.setRegistrationName(node.getMachineName());
    }
  }
}
