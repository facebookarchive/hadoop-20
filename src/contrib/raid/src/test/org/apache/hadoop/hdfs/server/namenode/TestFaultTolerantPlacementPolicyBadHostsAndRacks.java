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
import java.util.HashSet;
import java.util.Set;
import java.util.Map;

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
import org.apache.hadoop.hdfs.protocol.FSConstants;
import org.apache.hadoop.raid.Utils;
import junit.framework.TestCase;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * This class tests the BlockPlacementPolicyF4BadHostsAndRacks policy.
 * The policy tries to spread the blocks of a file evenly over all the
 * available racks with some bad hosts and racks.
 *
 * This is the topology (R0-R9 are racks and h0-h39 are hosts within them):
 *
 * R0   R1   R2   R3   R4   R5   R6   R7   R8   R9
 * -----------------------------------------------
 * h0   h1   h2   h3   h4   h5   h6   h7   h8   h9
 * h10  h11  h12  h13  h14  h15  h16  h17  h18  h19
 * h20  h21  h22  h23  h24  h25  h26  h27  h28  h29
 * h30  h31  h32  h33  h34  h35  h36  h37  h38  h39
 *
 * After we set R0 to R4 bad and h5-h9, h15-h19 bad, the expected
 * placement should happen such that all 40 blocks are equally
 * spread over h25-h29,h35-h39
 *
 * X   X   X   X   X   R5   R6   R7   R8   R9
 * --------------------------------------------
 * h0  h1  h2  h3  h4  x    x    x    x    x
 * h10  h11  h12  h13  x    x    x    x    x
 * h20  h21  h22  h23  h24  h25  h26  h27  h28  h29
 * h30  h31  h32  h33  h34  h35  h36  h37  h38  h39
 *
 * This is the placement that is verified in this test.
 */
public class TestFaultTolerantPlacementPolicyBadHostsAndRacks {

  private static MiniDFSCluster cluster;
  private static Configuration conf;
  private static final int BLOCK_SIZE = 1;
  private static final int STRIPE_SIZE = 10;
  private static final int PARITY_LEN = 4;
  private static final int NUM_RACKS = STRIPE_SIZE;
  private static final int BLOCKS_PER_RACK = 4;
  private static final String FILENAME = "testFile";
  private static Set<String> BAD_RACKS = new HashSet<String>();
  private static Set<String> BAD_HOSTS = new HashSet<String>();

  @BeforeClass 
  public static void setUpBeforeClass() throws Exception {
    conf = new Configuration();
    conf.setLong("dfs.blockreport.intervalMsec", 1000L);
    conf.set("dfs.replication.pending.timeout.sec", "2");
    conf.setLong("dfs.block.size", BLOCK_SIZE);
    conf.set("dfs.block.replicator.classname",
             "org.apache.hadoop.hdfs.server.namenode.FaultTolerantBlockPlacementPolicy");
    conf.setInt("io.bytes.per.checksum", 1);
    Utils.loadTestCodecs(conf, STRIPE_SIZE, 1, PARITY_LEN, "/raid", "/raidrs");
    int numBlocks = NUM_RACKS * BLOCKS_PER_RACK;
    List<String> racks = new ArrayList<String>(NUM_RACKS);

    String badRacks = "";
    for (int i = 0; i < numBlocks; ++i) {
      int rackId = i%NUM_RACKS;
      String rack = new String("/r" + rackId);
      racks.add(rack);
    }
    cluster = new MiniDFSCluster(conf,
                                 numBlocks,
                                 true,
                                 racks.toArray(new String[numBlocks]),
                                 null);
    cluster.waitActive(true);
    TestFaultTolerantPlacementPolicy.updateDatanodeMap(cluster);

    // Now set up the bad hosts and racks such that 50% of the racks
    // are bad and 50% of the hosts in the remaining racks are bad.
    NameNode nn = cluster.getNameNode();
    int i = 0;
    HashMap<String, Set<String>> rackToHost = new HashMap<String, Set<String>>();
    for (DatanodeInfo a : nn.getDatanodeReport(
           FSConstants.DatanodeReportType.ALL)) {
      Set<String> hosts = rackToHost.get(a.getNetworkLocation());
      if (hosts == null) {
        hosts = new HashSet<String>();
        rackToHost.put(a.getNetworkLocation(), hosts);
      }
      hosts.add(a.getName());
    }
    i = 0;
    for (Map.Entry<String, Set<String>> entry : rackToHost.entrySet()) {
      if (i++ < NUM_RACKS/2) {
        BAD_RACKS.add(entry.getKey());
      } else {
        int j = 0;
        int size = entry.getValue().size();
        for (String host : entry.getValue()) {
          if (j++ < size/2) {
            BAD_HOSTS.add(host);
          }
        }
      }
    }
    FaultTolerantBlockPlacementPolicy.setBadHostsAndRacks(
      BAD_RACKS, BAD_HOSTS);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    cluster.shutdown();
  }

  @Test
  public void testChooseTargetRackBasedWithBadRack() throws Exception {
    String fileName = new String("/hay/" + FILENAME);
    DFSTestUtil.createFile(cluster.getFileSystem(),
                           new Path(fileName),
                           (long) STRIPE_SIZE*BLOCK_SIZE*BLOCKS_PER_RACK,
                           (short)1, // replication
                           (long)0);
    HashMap<String, Integer> rackMapCount = new HashMap<String, Integer>();
    LocatedBlocks blocks =
      cluster.getNameNode().namesystem.getBlockLocations(fileName,
                                                         0,
                                                         Long.MAX_VALUE);
    // This means that nothing should exist on any of the bad host or rack.
    // This also means that each host should have 4X the blocks it would
    // have otherwise = 1.
    HashMap<String, Integer> rackCount = new HashMap<String, Integer>();
    HashMap<String, Integer> hostCount = new HashMap<String, Integer>();
    for (LocatedBlock b : blocks.getLocatedBlocks()) {
      for (DatanodeInfo i : b.getLocations()) {
        String rack = i.getNetworkLocation();
        String host = i.getName();
        // Should not get placed in a bad rack or bad host.
        assertFalse(
          TestFaultTolerantPlacementPolicyBadHostsAndRacks.BAD_RACKS.contains(rack));
        assertFalse(
          TestFaultTolerantPlacementPolicyBadHostsAndRacks.BAD_HOSTS.contains(host));
        Integer count = rackCount.get(rack);
        if (count == null) {
          rackCount.put(rack, new Integer(1));
        } else {
          int newCount = count.intValue() + 1;
          rackCount.put(rack, newCount);
        }
        count = hostCount.get(host);
        if (count == null) {
          hostCount.put(host, new Integer(1));
        } else {
          int newCount = count.intValue() + 1;
          hostCount.put(host, newCount);
        }
      }
    }

    for (Integer count: hostCount.values()) {
      // Since the # good hosts is 1/4th, the number of blocks per
      // host should be 4X (=4)
      assertEquals(count.intValue(), 4);
    }
    for (Integer count : rackCount.values()) {
      // Since the number of good racks is 1/2th, the number of blocks
      // per rack should be 2X
      assertEquals(count.intValue(), 2*BLOCKS_PER_RACK);
    }
  }
}
