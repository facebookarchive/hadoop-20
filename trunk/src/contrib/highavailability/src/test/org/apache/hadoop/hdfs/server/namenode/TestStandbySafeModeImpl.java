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
import org.junit.Before;
import org.junit.Test;

public class TestStandbySafeModeImpl {
  private static StandbySafeMode safeMode;
  private static Configuration conf;
  private static MyNamesystem namesystem;
  private static final Random random = new Random();
  private static Log LOG = LogFactory
.getLog(TestStandbySafeModeImpl.class);

  private class MyNamesystem extends FSNamesystem {
    public long totalBlocks = 0;

    @Override
    public long getBlocksTotal() {
      return totalBlocks;
    }
  }

  @Before
  public void setUp() throws Exception {
    conf = new Configuration();
    namesystem = new MyNamesystem();
    safeMode = new StandbySafeMode(conf, namesystem);
  }


  private DatanodeID generateRandomDatanodeID() {
    String nodeName = "" + random.nextLong();
    return new DatanodeID(nodeName);
  }

  @Test
  public void testBlocks() throws Exception {
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
  public void testBlocksNotSufficient() {
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
  public void testReportsNotSufficient() {
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
  public void testAllSufficient() {
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
}
