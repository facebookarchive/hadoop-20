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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.FSConstants.DatanodeReportType;
import org.apache.hadoop.hdfs.protocol.FSConstants.SafeModeAction;
import org.apache.hadoop.hdfs.util.InjectionEvent;
import org.apache.hadoop.util.InjectionEventI;
import org.apache.hadoop.util.InjectionHandler;

import static org.junit.Assert.*;
import org.junit.Test;

/**
 * Tests whether the safe block count is correct despite decommissioning.
 */
public class TestDecommissionSafeBlocks {
  private MiniDFSCluster cluster;
  private int counter = 0;
  private DatanodeDescriptor decom = null;
  private boolean pass = true;

  private class TestDecommissionSafeBlocksHandler extends
      InjectionHandler {

    public boolean processDecom = false;

    protected void _processEvent(InjectionEventI event, Object... args) {
      if (event == InjectionEvent.FSNAMESYSTEM_ADDSTORED_BLOCK) {
        counter++;
        if (!processDecom) {
          return;
        }
        if (counter == 1) {
          decom = (DatanodeDescriptor) (args[0]);
          try {
            cluster.getNameNode().namesystem.startDecommission(decom);
          } catch (Exception e) {
            System.out.println("FAILED : " + e);
            pass = false;
          }
        } else if (counter == 2) {
          try {
            cluster.getNameNode().namesystem.stopDecommission(decom);
          } catch (Exception e) {
            System.out.println("FAILED : " + e);
            pass = false;
          }
        }
      }
    }
  }

  private void waitForCounter() throws Exception {
    long start = System.currentTimeMillis();
    while (counter != 3 && System.currentTimeMillis() - start < 30000) {
      System.out.println("Counter is : " + counter);
      Thread.sleep(1000);
    }
    assertEquals(3, counter);
  }

  private void runTest(Configuration conf) throws Exception {
    counter = 0;
    TestDecommissionSafeBlocksHandler h = new TestDecommissionSafeBlocksHandler();
    InjectionHandler.set(h);
    try {
      cluster = new MiniDFSCluster(conf, 3, true, null);
      DFSTestUtil.createFile(cluster.getFileSystem(), new Path("/abc"),
          1024L, (short) 3, 0);
      waitForCounter();

      h.processDecom = true;
      counter = 0;
      System.out.println("RESTART");
      cluster.shutdown();
      cluster = new MiniDFSCluster(conf, 3, false, null);
      waitForCounter();

      assertEquals(1, cluster.getNameNode().namesystem.getSafeBlocks());
      assertTrue(pass);
    } finally {
      InjectionHandler.clear();
      if (cluster != null)
        cluster.shutdown();
    }
  }

  /**
   * Tests that the safe block count is correct despite a datanode going into
   * decommission and coming back.
   */
  // @Test(timeout = 30000)
  public void testDecommisionSafeBlocks() throws Exception {
    Configuration conf = new Configuration();
    conf.setInt("dfs.block.size", 1024);
    runTest(conf);
  }

  /**
   * Tests that the safe block count is correct despite a datanode going into
   * decommission and coming back with replication queues being initialized
   * during safemode.
   */
  // @Test(timeout = 30000)
  public void testDecommisionSafeBlocksReplThreshold() throws Exception {
    Configuration conf = new Configuration();
    conf.setInt("dfs.block.size", 1024);
    conf.setFloat("dfs.namenode.replqueue.threshold-pct", 0.0f);
    runTest(conf);
  }

  @Test
  /**
   * Verifies that removing a decommissioning datanode decrements the safe block
   * count.
   */
  public void testDecommissionDecrementSafeBlock() throws Exception {
    Configuration conf = new Configuration();
    MiniDFSCluster cluster = new MiniDFSCluster(conf, 1, true, null);
    try {

      // Create a single block on a datanode.
      FSNamesystem ns = cluster.getNameNode().namesystem;
      ArrayList<DatanodeDescriptor> dns = ns
          .getDatanodeListForReport(DatanodeReportType.ALL);
      assertNotNull(dns);
      assertEquals(1, dns.size());
      DFSTestUtil.createFile(cluster.getFileSystem(), new Path("/abc"), 1024L,
          (short) 3, 0);

      // Remove the datanode, enter safemode and restart it.
      ns.removeDatanode(dns.get(0));
      ns.setSafeMode(SafeModeAction.SAFEMODE_ENTER);
      cluster.restartDataNodes();

      // Wait for a block report.
      long start = System.currentTimeMillis();
      while (System.currentTimeMillis() - start < 10000
          && ns.getSafeBlocks() != 1) {
        Thread.sleep(1000);
      }

      // Verify we received the block report.
      assertEquals(1, ns.getSafeBlocks());

      // Remove a decommissioning node and verify the safe block count is
      // decremented.
      ns.startDecommission(dns.get(0));
      assertEquals(1, ns.getSafeBlocks());
      ns.removeDatanode(dns.get(0));
      assertEquals(0, ns.getSafeBlocks());
    } finally {
      cluster.shutdown();
    }
  }
}
