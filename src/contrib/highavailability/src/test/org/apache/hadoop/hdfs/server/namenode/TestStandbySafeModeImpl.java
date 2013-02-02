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
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.server.namenode.StandbySafeMode.SafeModeState;

import static org.junit.Assert.*;
import org.junit.Test;

public class TestStandbySafeModeImpl {
  private static StandbySafeMode safeMode;
  private static Configuration conf;
  private static MyNamesystem namesystem;
  private static final Random random = new Random();
  private static Log LOG = LogFactory.getLog(TestStandbySafeModeImpl.class);

  private class MyNamesystem extends FSNamesystem {
    public long totalBlocks = 0;

    @Override
    public long getBlocksTotal() {
      return totalBlocks;
    }

    @Override
    public boolean isDatanodeDead(DatanodeDescriptor node) {
      return false;
    }
    
    @Override
    public DatanodeDescriptor getDatanode(DatanodeID nodeID) {
      return new DatanodeDescriptor();
    }
    
    @Override
    public void processMisReplicatedBlocks() {
    }

    @Override
    public void writeLock() {
    }

    @Override
    public void writeUnlock() {
    }
  }

  private void setUp(String name) throws Exception {
    LOG.info("------------------- test: " + name + " START ----------------");
    conf = new Configuration();
    namesystem = new MyNamesystem();
    safeMode = new StandbySafeMode(conf, namesystem);
  }


  private static DatanodeID generateRandomDatanodeID() {
    String nodeName = "" + random.nextLong();
    return new DatanodeID(nodeName);
  }

  private static class ModificationThread extends Thread {
    private boolean running = true;

    public void run() {
      while (running) {
        List<DatanodeID> datanodes = new ArrayList<DatanodeID>();
        for (int i = 0; i < 50; i++) {
          DatanodeID node = generateRandomDatanodeID();
          datanodes.add(node);
          safeMode.reportHeartBeat(node);
        }

        for (DatanodeID node : datanodes) {
          if (random.nextBoolean()) {
            safeMode.reportPrimaryCleared(node);
          }
        }
      }
    }

    public void shutdown() {
      running = false;
    }
  }

  @Test
  public void testConcurrentModification() throws Exception {
    setUp("testConcurrentModification");
    namesystem.totalBlocks = 100;
    namesystem.blocksSafe = 100;
    safeMode.setSafeModeStateForTesting(SafeModeState.FAILOVER_IN_PROGRESS);
    List<DatanodeID> datanodes = new ArrayList<DatanodeID>();
    for (int i = 0; i < 100; i++) {
      DatanodeID node = generateRandomDatanodeID();
      datanodes.add(node);
      safeMode.reportHeartBeat(node);
    }
    ModificationThread t = new ModificationThread();
    t.start();
    for (int i = 0; i < 100000; i++) {
      safeMode.canLeave();
    }
    t.shutdown();
  }

  @Test
  public void testBlocks() throws Exception {
    setUp("testBlocks");
    assertTrue(safeMode.isOn());
    assertFalse(safeMode.canLeave());

    namesystem.totalBlocks = 100;
    namesystem.blocksSafe = 100;

    assertFalse(safeMode.canLeave());
    safeMode.setSafeModeStateForTesting(SafeModeState.FAILOVER_IN_PROGRESS);
    assertTrue(safeMode.canLeave());
  }

  @Test
  public void testReports() throws Exception {
    setUp("testReports");
    assertTrue(safeMode.isOn());
    assertFalse(safeMode.canLeave());

    int totalNodes = 100;
    List <DatanodeID> datanodes = new ArrayList<DatanodeID>();
    for (int i = 0; i < totalNodes; i++) {
      DatanodeID node = generateRandomDatanodeID();
      datanodes.add(node);
      safeMode.reportHeartBeat(node);
    }

    for (DatanodeID node : datanodes) {
      safeMode.reportPrimaryCleared(node);
    }

    assertFalse(safeMode.canLeave());
    safeMode.setSafeModeStateForTesting(SafeModeState.FAILOVER_IN_PROGRESS);
    assertTrue(safeMode.canLeave());
  }

  @Test
  public void testEarlyExit() throws Exception {
    setUp("testEarlyExit");
    namesystem.totalBlocks = 100;
    namesystem.blocksSafe = 100;

    try {
      safeMode.leave(false);
    } catch (RuntimeException e) {
      LOG.info("Expected exception", e);
      return;
    }
    fail("Did not throw " + SafeModeException.class);
  }

  @Test
  public void testRandomReports() throws Exception {
    setUp("testRandomReports");
    int totalNodes = 10;
    List <DatanodeID> datanodes = new ArrayList<DatanodeID>();
    for (int i = 0; i < totalNodes; i++) {
      DatanodeID node = generateRandomDatanodeID();
      datanodes.add(node);
    }
    
    assertFalse(safeMode.canLeave());
    safeMode.setSafeModeStateForTesting(SafeModeState.FAILOVER_IN_PROGRESS);

    Set<DatanodeID> expectedR = new HashSet<DatanodeID>();
    Set<DatanodeID> expectedH = new HashSet<DatanodeID>();
    for (DatanodeID node : datanodes) {
      // Add live node.
      if (random.nextBoolean()) {
        safeMode.addLiveNodeForTesting(node);
        expectedH.add(node);
      }

      // Report heartbeat.
      if (random.nextBoolean()) {
        int times = 1; // random.nextInt(3);
        for (int i = 0; i < times; i++) {
          safeMode.reportHeartBeat(node);
          expectedR.add(node);
          expectedH.remove(node);
        }
      }
      // Report primaryClear.
      if (random.nextBoolean()) {
        int times = 1;// random.nextInt(3);
        for (int i = 0; i < times; i++) {
          safeMode.reportPrimaryCleared(node);
          expectedR.remove(node);
        }
      }
    }

    LOG.info("expected : " + expectedR.size() + " actual : "
        + safeMode.getOutStandingReports().size());
    LOG.info("expected : " + expectedH.size() + " actual : "
        + safeMode.getOutStandingHeartbeats().size());
    assertTrue(expectedR.equals(safeMode.getOutStandingReports()));
    assertTrue(expectedH.equals(safeMode.getOutStandingHeartbeats()));
    if (expectedR.size() == 0 && expectedH.size() == 0) {
      assertTrue(safeMode.canLeave());
    } else {
      assertFalse(safeMode.canLeave());
    }
  }

  @Test
  public void testBlocksNotSufficient() throws Exception {
    setUp("testBlocksNotSufficient");
    namesystem.totalBlocks = 100;
    namesystem.blocksSafe = 50;

    int totalNodes = 100;
    List<DatanodeID> datanodes = new ArrayList<DatanodeID>();
    for (int i = 0; i < totalNodes; i++) {
      DatanodeID node = generateRandomDatanodeID();
      datanodes.add(node);
      safeMode.reportHeartBeat(node);
      safeMode.reportPrimaryCleared(node);
    }

    assertFalse(safeMode.canLeave());
    safeMode.setSafeModeStateForTesting(SafeModeState.FAILOVER_IN_PROGRESS);
    assertFalse(safeMode.canLeave());
  }

  @Test
  public void testReportsNotSufficient() throws Exception {
    setUp("testReportsNotSufficient");
    namesystem.totalBlocks = 100;
    namesystem.blocksSafe = 100;

    assertFalse(safeMode.canLeave());
    safeMode.setSafeModeStateForTesting(SafeModeState.FAILOVER_IN_PROGRESS);

    int totalNodes = 100;
    List<DatanodeID> datanodes = new ArrayList<DatanodeID>();
    for (int i = 0; i < totalNodes; i++) {
      DatanodeID node = generateRandomDatanodeID();
      datanodes.add(node);
      safeMode.reportHeartBeat(node);
      if (random.nextBoolean()) {
        safeMode.reportPrimaryCleared(node);
      }
    }

    assertFalse(safeMode.canLeave());
  }

  @Test
  public void testAllSufficient() throws Exception {
    setUp("testAllSufficient");
    namesystem.totalBlocks = 100;
    namesystem.blocksSafe = 100;

    assertFalse(safeMode.canLeave());
    safeMode.setSafeModeStateForTesting(SafeModeState.FAILOVER_IN_PROGRESS);

    int totalNodes = 100;
    List<DatanodeID> datanodes = new ArrayList<DatanodeID>();
    for (int i = 0; i < totalNodes; i++) {
      DatanodeID node = generateRandomDatanodeID();
      datanodes.add(node);
      safeMode.reportHeartBeat(node);
      safeMode.reportPrimaryCleared(node);
    }

    assertTrue(safeMode.canLeave());
  }
  
  @Test
  public void testProcessRBWReports() throws Exception {
    setUp("testProcessRBWReports");
    
    // at startup the safemode is in BEFORE_FAILOVER state
    // we do not process RBW reports
    assertFalse(safeMode.shouldProcessRBWReports());
    
    // in other states we process the RBW reports
    safeMode.setSafeModeStateForTesting(SafeModeState.FAILOVER_IN_PROGRESS);
    assertTrue(safeMode.shouldProcessRBWReports());
    
    safeMode.setSafeModeStateForTesting(SafeModeState.AFTER_FAILOVER);
    assertTrue(safeMode.shouldProcessRBWReports());
    
    // regular namenode safemode always allows RBW reports
    SafeModeInfo nnsm = new NameNodeSafeModeInfo(conf, namesystem);
    assertTrue(nnsm.shouldProcessRBWReports());   
  }
  
  @Test
  public void testInitReplicationQueues() throws Exception {
    setUp("testInitReplicationQueues");
    
    // initializing replication queues not allowed here
    assertFailure(SafeModeState.BEFORE_FAILOVER);
    assertFailure(SafeModeState.AFTER_FAILOVER);
    assertFailure(SafeModeState.LEAVING_SAFEMODE);
    
    // initisalizing only allowed in this state
    safeMode.setSafeModeStateForTesting(SafeModeState.FAILOVER_IN_PROGRESS);
    safeMode.initializeReplicationQueues();
  }
  
  private void assertFailure(SafeModeState state) {
    try {
      safeMode.setSafeModeStateForTesting(state);
      safeMode.initializeReplicationQueues();
    } catch (RuntimeException e) {
      LOG.info("Expected exception: " + e.getMessage());
    }
  }
}
