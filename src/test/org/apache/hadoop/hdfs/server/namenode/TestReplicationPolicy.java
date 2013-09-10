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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.net.NetworkTopology;
import org.apache.hadoop.net.Node;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.protocol.FSConstants;
import org.junit.After;
import org.junit.Before;

import junit.framework.TestCase;

public class TestReplicationPolicy extends TestCase {
  
  final static Log LOG = LogFactory.getLog(TestReplicationPolicy.class);
  
  private static final int BLOCK_SIZE = 1024;
  private static final int NUM_OF_DATANODES = 8;  
  private static final String filename = "/dummyfile.txt";
  
  private Configuration CONF;
  private NetworkTopology cluster;
  private NameNode namenode;
  private BlockPlacementPolicy replicator;
  private DatanodeDescriptor dataNodes[];
     
  private DatanodeDescriptor NODE = 
      new DatanodeDescriptor(new DatanodeID("h7:5020"), "/d2/r4");
  
  @Before
  public void setUp() throws IOException {    
    CONF = new Configuration();
    dataNodes =
        new DatanodeDescriptor[] {
        new DatanodeDescriptor(new DatanodeID("h1:5020"), "/d1/r1"),
        new DatanodeDescriptor(new DatanodeID("h2:5020"), "/d1/r1"),
        new DatanodeDescriptor(new DatanodeID("h3:5020"), "/d1/r2"),
        new DatanodeDescriptor(new DatanodeID("h4:5020"), "/d1/r2"),
        new DatanodeDescriptor(new DatanodeID("h5:5020"), "/d2/r3"),
        new DatanodeDescriptor(new DatanodeID("h6:5020"), "/d2/r3"),
        new DatanodeDescriptor(new DatanodeID("h7:5020"), "/d1/r2"),
        new DatanodeDescriptor(new DatanodeID("h8:5020"), "/d1/r1")
      };
    FileSystem.setDefaultUri(CONF, "hdfs://localhost:0");
    CONF.set("dfs.http.address", "0.0.0.0:0");
    
    NameNode.format(CONF);
    namenode = new NameNode(CONF);
    FSNamesystem fsNamesystem = namenode.getNamesystem();
    replicator = fsNamesystem.replicator;
    cluster = fsNamesystem.clusterMap;
    // construct network topology
    for(int i=0; i<NUM_OF_DATANODES; i++) {
      cluster.add(dataNodes[i]);
    }
    for(int i=0; i<NUM_OF_DATANODES; i++) {
      dataNodes[i].updateHeartbeat(
          2*FSConstants.MIN_BLOCKS_FOR_WRITE*BLOCK_SIZE, 0L,
          2*FSConstants.MIN_BLOCKS_FOR_WRITE*BLOCK_SIZE, 0L, 0);
    }
  }
  
  @After
  public void tearDown() {
    if (namenode != null) {
      namenode.stop();
    }
  }
  
  
  /**
   * In this testcase, client is dataNodes[0]. So the 1st replica should be
   * placed on dataNodes[0], the 2nd replica should be placed on 
   * different rack and third should be placed on different node
   * of rack chosen for 2nd node.
   * The only excpetion is when the <i>numOfReplicas</i> is 2, 
   * the 1st is on dataNodes[0] and the 2nd is on a different rack.
   * @throws Exception
   */
  public void testChooseTarget1() throws Exception {
    dataNodes[0].updateHeartbeat(
        2*FSConstants.MIN_BLOCKS_FOR_WRITE*BLOCK_SIZE, 0L, 
        FSConstants.MIN_BLOCKS_FOR_WRITE*BLOCK_SIZE, 0L, 4); // overloaded

    try{
    DatanodeDescriptor[] targets;
    targets = replicator.chooseTarget(filename,
                                      0, dataNodes[0], BLOCK_SIZE);
    assertEquals(targets.length, 0);
    
    targets = replicator.chooseTarget(filename,
                                      1, dataNodes[0], BLOCK_SIZE);
    assertEquals(targets.length, 1);
    assertEquals(targets[0], dataNodes[0]);
    
    targets = replicator.chooseTarget(filename,
                                      2, dataNodes[0], BLOCK_SIZE);
    assertEquals(targets.length, 2);
    assertEquals(targets[0], dataNodes[0]);
    assertFalse(cluster.isOnSameRack(targets[0], targets[1]));
    
    targets = replicator.chooseTarget(filename,
                                      3, dataNodes[0], BLOCK_SIZE);
    assertEquals(targets.length, 3);
    assertEquals(targets[0], dataNodes[0]);
    assertFalse(cluster.isOnSameRack(targets[0], targets[1]));
    assertTrue(cluster.isOnSameRack(targets[1], targets[2]));

    targets = replicator.chooseTarget(filename,
                                     4, dataNodes[0], BLOCK_SIZE);
    assertEquals(targets.length, 4);
    assertEquals(targets[0], dataNodes[0]);
    assertTrue(cluster.isOnSameRack(targets[1], targets[2]) ||
               cluster.isOnSameRack(targets[2], targets[3]));
    assertFalse(cluster.isOnSameRack(targets[0], targets[2]));
    } finally {
    dataNodes[0].updateHeartbeat(
        2*FSConstants.MIN_BLOCKS_FOR_WRITE*BLOCK_SIZE, 0L,
        FSConstants.MIN_BLOCKS_FOR_WRITE*BLOCK_SIZE, 0L, 0); 
    }
  }
  
  /**
   * Test if the network topology correctly keeps track of racks.
   * Test both adding and removing nodes.
   */
  public void testRacksList() {
    int datanodeCount = 1000;
    Random r = new Random();
    Collection<String> locations = new HashSet<String>();
    for (int i = 0; i < NUM_OF_DATANODES; i++) {
      // remove original datanodes
      cluster.remove(dataNodes[i]);
    }
    // no racks in the topology
    assertEquals(0, cluster.getRacks().size());
    assertEquals(0, cluster.getNumOfRacks());
    
    dataNodes = new DatanodeDescriptor[datanodeCount];
    for (int i = 0; i < datanodeCount; i++) {
      String did = "h" + i + ":5020";
      String loc = "/d" + r.nextInt(20) + "/r" + r.nextInt(20);
      locations.add(loc);
      dataNodes[i] = new DatanodeDescriptor(new DatanodeID(did), loc);
      // add new datanodes
      cluster.add(dataNodes[i]);
      
      // make sure the racks are correct
      List<String> racks = cluster.getRacks();
      for (String rack : racks) {
        assertTrue(locations.contains(rack));
      }
      assertEquals(locations.size(), racks.size());
    }
    
    for (int i = 0; i < datanodeCount; i++) {
      cluster.remove(dataNodes[i]);
      Collection<String> locationsLeft = locationsLeft(i);      
      // make sure the racks are correct
      List<String> racks = cluster.getRacks();
      for (String rack : racks) {
        assertTrue(locationsLeft.contains(rack));
      }
      assertEquals(locationsLeft.size(), racks.size());
    }
  }
  
  /**
   * Test that the list of racks does not contain
   * duplicates.
   */
  public void testRacksListOneRack() {
    for (int i = 0; i < NUM_OF_DATANODES; i++) {
      // remove original datanodes
      cluster.remove(dataNodes[i]);
    }
    assertEquals(0, cluster.getNumOfRacks());
    dataNodes = new DatanodeDescriptor[10];
    for (int i = 0; i < 10; i++) {
      String did = "h" + i + ":5020";
      String loc = "/d0/r0";
      dataNodes[i] = new DatanodeDescriptor(new DatanodeID(did), loc);
      // add new datanodes
      cluster.add(dataNodes[i]);
      
      // make sure we have only one rack
      assertEquals(1, cluster.getNumOfRacks());
      assertEquals(loc, cluster.getRacks().get(0));
    }
  }
 
  /**
   * Return all locations of datanodes starting at 
   * removedUpToIndex + 1 index
   */
  private Collection<String> locationsLeft(int removedUpToIndex) {
    Collection<String> locationsLeft = new HashSet<String>();
    for (int i = removedUpToIndex + 1; i < dataNodes.length; i++) {
      locationsLeft.add(dataNodes[i].getNetworkLocation());
    }
    return locationsLeft;
  }
  
  /**
   * Test NetworkTopology.chooseRack(Set<String>)
   */
  public void testChooseRack() throws Exception {
    // for this test create 100 datanodes
    Random r = new Random();
    Collection<String> locations = new HashSet<String>();
    for (int i = 0; i < NUM_OF_DATANODES; i++) {
      // remove original datanodes
      cluster.remove(dataNodes[i]);
    }
    dataNodes = new DatanodeDescriptor[100];
    for (int i = 0; i < 100; i++) {
      String did = "h" + i + ":5020";
      String loc = "/d" + r.nextInt(5) + "/r" + r.nextInt(5);
      locations.add(loc);
      dataNodes[i] = new DatanodeDescriptor(new DatanodeID(did), loc);
      // add new datanodes
      cluster.add(dataNodes[i]);
    }
    LOG.info("Locations : " + locations.size() + " racks in total");

    int numOfDatanodes = dataNodes.length;
    Set<String> excludedRacks = new HashSet<String>();

    // randomly exclude racks 
    for (int i = 0; i < 20; i++) {
      excludedRacks.clear();
      int numToExclude = r.nextInt(locations.size() - 1) + 1;
      LOG.info("Iteration : " + i + " will exclude: " + numToExclude);
      // exclude a random number of racks
      while (excludedRacks.size() < numToExclude) {
        excludedRacks.add(dataNodes[r.nextInt(numOfDatanodes)].getNetworkLocation());
      }
      LOG.info("Excluded racks: " + excludedRacks);

      String chosenNode = cluster.chooseRack(excludedRacks);

      LOG.info("Chosen node: " + chosenNode + " should not be null");
      // chosen node cannot be in the excluded list
      for (String exluded : excludedRacks) {
        assertFalse(exluded.equals(chosenNode));
      }
      // if the number of locations in the excluded list was less that the
      // total number of location, then we should not get null
      if (locations.size() > excludedRacks.size()) {
        assertNotNull(chosenNode);
      }
    }

    // exclude all nodes, and we should get null
    LOG.info("Excluding all nodes");
    excludedRacks.clear();
    for (int n = 0; n < numOfDatanodes; n++) {
      excludedRacks.add(dataNodes[n].getNetworkLocation());
    }
    String chosenNode = cluster.chooseRack(excludedRacks);
    LOG.info("Chosen node: " + chosenNode + " should be null");
    assertNull(chosenNode);
  }

  /**
   * In this testcase, client is dataNodes[0], but the dataNodes[1] is
   * not allowed to be chosen. So the 1st replica should be
   * placed on dataNodes[0], the 2nd replica should be placed on a different
   * rack, the 3rd should be on same rack as the 2nd replica, and the rest
   * should be placed on a third rack.
   * @throws Exception
   */
  public void testChooseTarget2() throws Exception { 
    List<Node> excludedNodes;
    DatanodeDescriptor[] targets;
    BlockPlacementPolicyDefault replicator1 = (BlockPlacementPolicyDefault)replicator;
    List<DatanodeDescriptor> chosenNodes = new ArrayList<DatanodeDescriptor>();
    
    excludedNodes = new ArrayList<Node>();
    excludedNodes.add(dataNodes[1]); 
    targets = replicator1.chooseTarget(
                                      0, dataNodes[0], chosenNodes, excludedNodes, BLOCK_SIZE);
    assertEquals(targets.length, 0);
    
    excludedNodes.clear();
    chosenNodes.clear();
    excludedNodes.add(dataNodes[1]); 
    targets = replicator1.chooseTarget(
                                      1, dataNodes[0], chosenNodes, excludedNodes, BLOCK_SIZE);
    assertEquals(targets.length, 1);
    assertEquals(targets[0], dataNodes[0]);
    
    excludedNodes.clear();
    chosenNodes.clear();
    excludedNodes.add(dataNodes[1]); 
    targets = replicator1.chooseTarget(
                                      2, dataNodes[0], chosenNodes, excludedNodes, BLOCK_SIZE);
    assertEquals(targets.length, 2);
    assertEquals(targets[0], dataNodes[0]);
    assertFalse(cluster.isOnSameRack(targets[0], targets[1]));
    
    excludedNodes.clear();
    chosenNodes.clear();
    excludedNodes.add(dataNodes[1]); 
    targets = replicator1.chooseTarget(
                                      3, dataNodes[0], chosenNodes, excludedNodes, BLOCK_SIZE);
    assertEquals(targets.length, 3);
    assertEquals(targets[0], dataNodes[0]);
    assertFalse(cluster.isOnSameRack(targets[0], targets[1]));
    assertTrue(cluster.isOnSameRack(targets[1], targets[2]));
    
    excludedNodes.clear();
    chosenNodes.clear();
    excludedNodes.add(dataNodes[1]);
    excludedNodes.add(dataNodes[6]);
    excludedNodes.add(dataNodes[7]);
    targets = replicator1.chooseTarget(
                                      4, dataNodes[0], chosenNodes, excludedNodes, BLOCK_SIZE);
    assertEquals(targets.length, 4);
    assertEquals(targets[0], dataNodes[0]);
    for(int i=1; i<4; i++) {
      assertFalse(cluster.isOnSameRack(targets[0], targets[i]));
    }
    assertTrue(cluster.isOnSameRack(targets[1], targets[2]) ||
               cluster.isOnSameRack(targets[2], targets[3]));
    assertFalse(cluster.isOnSameRack(targets[1], targets[3]));
  }

  /**
   * In this testcase, client is dataNodes[0], but dataNodes[0] is not qualified
   * to be chosen. So the 1st replica should be placed on dataNodes[1], 
   * the 2nd replica should be placed on a different rack,
   * the 3rd replica should be placed on the same rack as the 2nd replica,
   * and the rest should be placed on the third rack.
   * @throws Exception
   */
  public void testChooseTarget3() throws Exception {
    // make data node 0, 6 & 7 to be not qualified to choose
    dataNodes[0].updateHeartbeat(
        2*FSConstants.MIN_BLOCKS_FOR_WRITE*BLOCK_SIZE, 0L,
        (FSConstants.MIN_BLOCKS_FOR_WRITE-1)*BLOCK_SIZE, 0L, 0); // no space
    dataNodes[6].updateHeartbeat(
        2*FSConstants.MIN_BLOCKS_FOR_WRITE*BLOCK_SIZE, 0L,
        (FSConstants.MIN_BLOCKS_FOR_WRITE-1)*BLOCK_SIZE, 0L, 0); // no space
    dataNodes[7].updateHeartbeat(
        2*FSConstants.MIN_BLOCKS_FOR_WRITE*BLOCK_SIZE, 0L,
        (FSConstants.MIN_BLOCKS_FOR_WRITE-1)*BLOCK_SIZE, 0L, 0); // no space
        
    try {
    DatanodeDescriptor[] targets;
    targets = replicator.chooseTarget(filename,
                                      0, dataNodes[0], BLOCK_SIZE);
    assertEquals(targets.length, 0);
    
    targets = replicator.chooseTarget(filename,
                                      1, dataNodes[0], BLOCK_SIZE);
    assertEquals(targets.length, 1);
    assertTrue(targets[0].equals(dataNodes[1]) || targets[0].equals(dataNodes[7]));
    
    targets = replicator.chooseTarget(filename,
                                      2, dataNodes[0], BLOCK_SIZE);
    assertEquals(targets.length, 2);
    assertTrue(targets[0].equals(dataNodes[1]) || targets[0].equals(dataNodes[7]));
    assertFalse(cluster.isOnSameRack(targets[0], targets[1]));
    
    targets = replicator.chooseTarget(filename,
                                      3, dataNodes[0], BLOCK_SIZE);
    assertEquals(targets.length, 3);
    assertTrue(targets[0].equals(dataNodes[1]) || targets[0].equals(dataNodes[7]));
    assertTrue(cluster.isOnSameRack(targets[1], targets[2]));
    assertFalse(cluster.isOnSameRack(targets[0], targets[1]));
    
    targets = replicator.chooseTarget(filename,
                                      4, dataNodes[0], BLOCK_SIZE);
    assertEquals(targets.length, 4);
    assertTrue(targets[0].equals(dataNodes[1]) || targets[0].equals(dataNodes[7]));
    for(int i=1; i<4; i++) {
      assertFalse(cluster.isOnSameRack(targets[0], targets[i]));
    }
    assertTrue(cluster.isOnSameRack(targets[1], targets[2]) ||
               cluster.isOnSameRack(targets[2], targets[3]));
    assertFalse(cluster.isOnSameRack(targets[1], targets[3]));

    } finally {
    dataNodes[0].updateHeartbeat(
        2*FSConstants.MIN_BLOCKS_FOR_WRITE*BLOCK_SIZE, 0L,
        FSConstants.MIN_BLOCKS_FOR_WRITE*BLOCK_SIZE, 0L, 0); 
    dataNodes[6].updateHeartbeat(
        2*FSConstants.MIN_BLOCKS_FOR_WRITE*BLOCK_SIZE, 0L,
        FSConstants.MIN_BLOCKS_FOR_WRITE*BLOCK_SIZE, 0L, 0); 
    dataNodes[7].updateHeartbeat(
        2*FSConstants.MIN_BLOCKS_FOR_WRITE*BLOCK_SIZE, 0L,
        FSConstants.MIN_BLOCKS_FOR_WRITE*BLOCK_SIZE, 0L, 0); 
    }
  }
  
  /**
   * In this testcase, client is dataNodes[4], but none of the nodes on rack 2
   * is qualified to be chosen. So the 1st replica should be placed on either
   * rack 1 or rack 3. 
   * the 2nd replica should be placed on a different rack,
   * the 3rd replica should be placed on the same rack as the 1st replica,
   * @throws Exception
   */
  public void testChoooseTarget4() throws Exception {
    // make data node 4 & 5 to be not qualified to choose: not enough disk space
    for(int i=4; i<6; i++) {
      dataNodes[i].updateHeartbeat(
          2*FSConstants.MIN_BLOCKS_FOR_WRITE*BLOCK_SIZE, 0L,
          (FSConstants.MIN_BLOCKS_FOR_WRITE-1)*BLOCK_SIZE, 0L, 0);
    }
      
    try {
    DatanodeDescriptor[] targets;
    targets = replicator.chooseTarget(filename,
                                      0, dataNodes[0], BLOCK_SIZE);
    assertEquals(targets.length, 0);
    
    targets = replicator.chooseTarget(filename,
                                      1, dataNodes[4], BLOCK_SIZE);
    assertEquals(targets.length, 1);
    assertFalse(cluster.isOnSameRack(targets[0], dataNodes[4]));
    
    targets = replicator.chooseTarget(filename,
                                      2, dataNodes[4], BLOCK_SIZE);
    assertEquals(targets.length, 2);
    assertFalse(cluster.isOnSameRack(targets[0], dataNodes[4]));
    assertFalse(cluster.isOnSameRack(targets[0], targets[1]));
    
    targets = replicator.chooseTarget(filename,
                                      3, dataNodes[4], BLOCK_SIZE);
    assertEquals(targets.length, 3);
    for(int i=0; i<3; i++) {
      assertFalse(cluster.isOnSameRack(targets[i], dataNodes[4]));
    }
    assertTrue(cluster.isOnSameRack(targets[0], targets[1]) ||
               cluster.isOnSameRack(targets[1], targets[2]));
    assertFalse(cluster.isOnSameRack(targets[0], targets[2]));
    } finally {
    for(int i=0; i<2; i++) {
      dataNodes[i].updateHeartbeat(
          2*FSConstants.MIN_BLOCKS_FOR_WRITE*BLOCK_SIZE, 0L,
          FSConstants.MIN_BLOCKS_FOR_WRITE*BLOCK_SIZE, 0L, 0);
    }
    }
  }
  /**
   * In this testcase, client is is a node outside of file system.
   * So the 1st replica can be placed on any node. 
   * the 2nd replica should be placed on a different rack,
   * the 3rd replica should be placed on the same rack as the 2nd replica,
   * @throws Exception
   */
  public void testChooseTarget5() throws Exception {
    DatanodeDescriptor[] targets;
    targets = replicator.chooseTarget(filename,
                                      0, NODE, BLOCK_SIZE);
    assertEquals(targets.length, 0);
    
    targets = replicator.chooseTarget(filename,
                                      1, NODE, BLOCK_SIZE);
    assertEquals(targets.length, 1);
    
    targets = replicator.chooseTarget(filename,
                                      2, NODE, BLOCK_SIZE);
    assertEquals(targets.length, 2);
    assertFalse(cluster.isOnSameRack(targets[0], targets[1]));
    
    targets = replicator.chooseTarget(filename,
                                      3, NODE, BLOCK_SIZE);
    assertEquals(targets.length, 3);
    assertTrue(cluster.isOnSameRack(targets[1], targets[2]));
    assertFalse(cluster.isOnSameRack(targets[0], targets[1]));    
  }
  
  /**
   * This testcase tests re-replication, when dataNodes[0] is already chosen.
   * So the 1st replica can be placed on random rack. 
   * the 2nd replica should be placed on different node by same rack as 
   * the 1st replica. The 3rd replica can be placed randomly.
   * @throws Exception
   */
  public void testRereplicate1() throws Exception {
    List<DatanodeDescriptor> chosenNodes = new ArrayList<DatanodeDescriptor>();
    chosenNodes.add(dataNodes[0]);    
    DatanodeDescriptor[] targets;
    
    targets = replicator.chooseTarget(filename,
                                      0, dataNodes[0], chosenNodes, BLOCK_SIZE);
    assertEquals(targets.length, 0);
    
    targets = replicator.chooseTarget(filename,
                                      1, dataNodes[0], chosenNodes, BLOCK_SIZE);
    assertEquals(targets.length, 1);
    assertFalse(cluster.isOnSameRack(dataNodes[0], targets[0]));
    
    targets = replicator.chooseTarget(filename,
                                      2, dataNodes[0], chosenNodes, BLOCK_SIZE);
    assertEquals(targets.length, 2);
    assertTrue(cluster.isOnSameRack(dataNodes[0], targets[0]));
    assertFalse(cluster.isOnSameRack(targets[0], targets[1]));
    
    targets = replicator.chooseTarget(filename,
                                      3, dataNodes[0], chosenNodes, BLOCK_SIZE);
    assertEquals(targets.length, 3);
    assertTrue(cluster.isOnSameRack(dataNodes[0], targets[0]));
    assertFalse(cluster.isOnSameRack(targets[0], targets[2]));
  }

  /**
   * This testcase tests re-replication, 
   * when dataNodes[0] and dataNodes[1] are already chosen.
   * So the 1st replica should be placed on a different rack than rack 1. 
   * the rest replicas can be placed randomly,
   * @throws Exception
   */
  public void testRereplicate2() throws Exception {
    List<DatanodeDescriptor> chosenNodes = new ArrayList<DatanodeDescriptor>();
    chosenNodes.add(dataNodes[0]);
    chosenNodes.add(dataNodes[1]);

    DatanodeDescriptor[] targets;
    targets = replicator.chooseTarget(filename,
                                      0, dataNodes[0], chosenNodes, BLOCK_SIZE);
    assertEquals(targets.length, 0);
    
    targets = replicator.chooseTarget(filename,
                                      1, dataNodes[0], chosenNodes, BLOCK_SIZE);
    assertEquals(targets.length, 1);
    assertFalse(cluster.isOnSameRack(dataNodes[0], targets[0]));
    
    chosenNodes.add(dataNodes[7]);
    targets = replicator.chooseTarget(filename,
                                      2, dataNodes[0], chosenNodes, BLOCK_SIZE);
    assertEquals(targets.length, 2);
    assertFalse(cluster.isOnSameRack(dataNodes[0], targets[0]));
    assertFalse(cluster.isOnSameRack(dataNodes[0], targets[1]));
  }

  /**
   * This testcase tests re-replication, 
   * when dataNodes[0] and dataNodes[2] are already chosen.
   * So the 1st replica should be placed on the rack that the writer resides. 
   * the rest replicas can be placed randomly,
   * @throws Exception
   */
  public void testRereplicate3() throws Exception {
    List<DatanodeDescriptor> chosenNodes = new ArrayList<DatanodeDescriptor>();
    chosenNodes.add(dataNodes[0]);
    chosenNodes.add(dataNodes[2]);
    
    DatanodeDescriptor[] targets;
    targets = replicator.chooseTarget(filename,
                                      0, dataNodes[0], chosenNodes, BLOCK_SIZE);
    assertEquals(targets.length, 0);
    
    targets = replicator.chooseTarget(filename,
                                      1, dataNodes[0], chosenNodes, BLOCK_SIZE);
    assertEquals(targets.length, 1);
    assertTrue(cluster.isOnSameRack(dataNodes[0], targets[0]));
    assertFalse(cluster.isOnSameRack(dataNodes[2], targets[0]));
    
    targets = replicator.chooseTarget(filename,
                               1, dataNodes[2], chosenNodes, BLOCK_SIZE);
    assertEquals(targets.length, 1);
    assertTrue(cluster.isOnSameRack(dataNodes[2], targets[0]));
    assertFalse(cluster.isOnSameRack(dataNodes[0], targets[0]));

    targets = replicator.chooseTarget(filename,
                                      2, dataNodes[0], chosenNodes, BLOCK_SIZE);
    assertEquals(targets.length, 2);
    assertTrue(cluster.isOnSameRack(dataNodes[0], targets[0]));
    
    targets = replicator.chooseTarget(filename,
                               2, dataNodes[2], chosenNodes, BLOCK_SIZE);
    assertEquals(targets.length, 2);
    assertTrue(cluster.isOnSameRack(dataNodes[2], targets[0]));
  }
  
  /**
   * This testcase tests decommission, when dataNodes[0] is being decommissioned.
   * So the 1st replica can be placed on the same rack. 
   * the 2nd replica should be placed on a different rack as 
   * the 1st replica. The 3rd replica is the same rack as the 2nd replica.
   * @throws Exception
   */
  public void testDecommission1() throws Exception {
    List<DatanodeDescriptor> chosenNodes = new ArrayList<DatanodeDescriptor>();
    dataNodes[0].startDecommission();
    chosenNodes.add(dataNodes[0]);    
    DatanodeDescriptor[] targets;
    try {
    targets = replicator.chooseTarget(filename,
                                      0, dataNodes[0], chosenNodes, BLOCK_SIZE);
    assertEquals(targets.length, 0);
    
    targets = replicator.chooseTarget(filename,
                                      1, dataNodes[0], chosenNodes, BLOCK_SIZE);
    assertEquals(targets.length, 1);
    assertTrue(cluster.isOnSameRack(dataNodes[0], targets[0]));
    
    targets = replicator.chooseTarget(filename,
                                      2, dataNodes[0], chosenNodes, BLOCK_SIZE);
    assertEquals(targets.length, 2);
    assertTrue(cluster.isOnSameRack(dataNodes[0], targets[0]));
    assertFalse(cluster.isOnSameRack(targets[0], targets[1]));
    
    targets = replicator.chooseTarget(filename,
                                      3, dataNodes[0], chosenNodes, BLOCK_SIZE);
    assertEquals(targets.length, 3);
    assertTrue(cluster.isOnSameRack(dataNodes[0], targets[0]));
    assertTrue(cluster.isOnSameRack(dataNodes[0], targets[1]));
    assertFalse(cluster.isOnSameRack(targets[1], targets[2]));
    } finally {
      dataNodes[0].stopDecommission();
    }
  }

  /**
   * This testcase tests decommision, 
   * when dataNodes[0] and dataNodes[1] are already chosen
   * and dataNodes[0] is being decommissioned.
   * So the 1st replica should be placed on a different rack than rack 1. 
   * and the 2nd replica should be placed on the same rack as rack 1.
   * the rest replicas can be placed randomly,
   * @throws Exception
   */
  public void testDecommission2() throws Exception {
    List<DatanodeDescriptor> chosenNodes = new ArrayList<DatanodeDescriptor>();
    dataNodes[0].startDecommission();
    chosenNodes.add(dataNodes[0]);
    chosenNodes.add(dataNodes[1]);

    DatanodeDescriptor[] targets;
    try {
    targets = replicator.chooseTarget(filename,
                                      0, dataNodes[0], chosenNodes, BLOCK_SIZE);
    assertEquals(targets.length, 0);
    
    targets = replicator.chooseTarget(filename,
                                      1, dataNodes[0], chosenNodes, BLOCK_SIZE);
    assertEquals(targets.length, 1);
    assertFalse(cluster.isOnSameRack(dataNodes[0], targets[0]));
    
    targets = replicator.chooseTarget(filename,
                                      2, dataNodes[0], chosenNodes, BLOCK_SIZE);
    assertEquals(targets.length, 2);
    assertTrue(cluster.isOnSameRack(dataNodes[0], targets[0]));
    assertFalse(cluster.isOnSameRack(dataNodes[0], targets[1]));
    } finally {
      dataNodes[0].stopDecommission();
    }
  }

  /**
   * This testcase tests re-replication, 
   * when dataNodes[0] and dataNodes[2] are already chosen.
   * So the 1st replica should be placed on the rack that the writer resides. 
   * the rest replicas can be placed randomly,
   * @throws Exception
   */
  public void testDecommion3() throws Exception {
    List<DatanodeDescriptor> chosenNodes = new ArrayList<DatanodeDescriptor>();
    chosenNodes.add(dataNodes[0]);
    chosenNodes.add(dataNodes[2]);
    
    DatanodeDescriptor[] targets;
    dataNodes[0].startDecommission();
    try {
    targets = replicator.chooseTarget(filename,
                                      0, dataNodes[0], chosenNodes, BLOCK_SIZE);
    assertEquals(targets.length, 0);
    
    targets = replicator.chooseTarget(filename,
                                      1, dataNodes[0], chosenNodes, BLOCK_SIZE);
    assertEquals(targets.length, 1);
    assertTrue(cluster.isOnSameRack(dataNodes[0], targets[0]));
    assertFalse(cluster.isOnSameRack(dataNodes[2], targets[0]));
    
    targets = replicator.chooseTarget(filename,
                               1, dataNodes[2], chosenNodes, BLOCK_SIZE);
    assertEquals(targets.length, 1);
    assertFalse(cluster.isOnSameRack(dataNodes[2], targets[0]));

    targets = replicator.chooseTarget(filename,
                                      2, dataNodes[0], chosenNodes, BLOCK_SIZE);
    assertEquals(targets.length, 2);
    assertTrue(cluster.isOnSameRack(dataNodes[0], targets[0]));
    
    targets = replicator.chooseTarget(filename,
                               2, dataNodes[2], chosenNodes, BLOCK_SIZE);
    assertEquals(targets.length, 2);
    assertTrue(cluster.isOnSameRack(dataNodes[2], targets[0]));
    } finally {
      dataNodes[0].stopDecommission();
    }
  }
  
  /**
   * This testcase tests decommission
   * when dataNodes[0] and dataNodes[2] and dataNodes[3] are already chosen.
   * @throws Exception
   */
  public void testDecommion4() throws Exception {
    List<DatanodeDescriptor> chosenNodes = new ArrayList<DatanodeDescriptor>();
    chosenNodes.add(dataNodes[0]);
    chosenNodes.add(dataNodes[2]);
    chosenNodes.add(dataNodes[3]);
    
    
    DatanodeDescriptor[] targets;
    dataNodes[0].startDecommission();
    try {
      targets = replicator.chooseTarget(filename,
                                      1, dataNodes[0], chosenNodes, BLOCK_SIZE);
      assertEquals(targets.length, 1);
      assertTrue(cluster.isOnSameRack(dataNodes[0], targets[0]));
    } finally {
      dataNodes[0].stopDecommission();
    }
    
    dataNodes[2].startDecommission();
    try {
      targets = replicator.chooseTarget(filename,
          1, dataNodes[2], chosenNodes, BLOCK_SIZE);
      assertEquals(targets.length, 1);
      assertTrue(cluster.isOnSameRack(dataNodes[2], targets[0]));
    } finally {
      dataNodes[0].stopDecommission();
    }
  }

}
