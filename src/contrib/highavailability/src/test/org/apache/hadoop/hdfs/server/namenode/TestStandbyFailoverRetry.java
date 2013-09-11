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
import java.net.InetSocketAddress;
import java.net.Socket;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.server.namenode.AvatarNode;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogLoader;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.FailoverTestUtil;
import org.apache.hadoop.util.InjectionHandler;

import static org.junit.Assert.*;
import org.junit.Test;

/**
 * Test cases to valid Standby behavior when a failover does not succeed.
 */
public class TestStandbyFailoverRetry extends FailoverTestUtil {

  private volatile static boolean restartDone = false;

  private static class WaitForRestart extends Thread {
    private final AvatarNode node;

    public WaitForRestart(AvatarNode node) {
      this.node = node;
    }

    public void run() {
      node.waitForRestart();
      restartDone = true;
    }
  }

  public void setUp(String name) throws Exception {
  	setUp(name, true);
  }
  
  public void setUp(String name, boolean enableQJM) throws Exception {
    super.setUp(name, enableQJM);
    restartDone = false;
    new WaitForRestart(cluster.getPrimaryAvatar(0).avatar).start();
    new WaitForRestart(cluster.getStandbyAvatar(0).avatar).start();
  }

  private void tryConnect(AvatarNode node) throws Exception {
    for (int i = 0; i < 30; i++) {
      InetSocketAddress addr = node.getNameNodeAddress();
      Socket s = new Socket();
      s.connect(addr);
      LOG.info("TryConnect : " + i);
      Thread.sleep(1000);
    }
  }

  @Test
  public void testFailoverWithRestart() throws Exception {
    setUp("testFailoverWithRestart");
    DFSTestUtil.createFile(fs, new Path("/testFailoverWithRestart"), 1024,
        (short) 1, System.currentTimeMillis());
    FailoverTestUtilHandler handler = new FailoverTestUtilHandler();
    handler.simulateShutdownCrash = false;
    InjectionHandler.set(handler);
    AvatarNode node = cluster.getStandbyAvatar(0).avatar;
    cluster.killPrimary();
    cluster.getStandbyAvatar(0).avatar.quiesceForFailover(false);
    while (!handler.waitForRestartTrigger) {
      LOG.info("Waiting for restart");
      Thread.sleep(1000);
    }
    Thread.sleep(5000);
    tryConnect(node);
    cluster.getStandbyAvatar(0).avatar.performFailover();
    tryConnect(node);
  }

  @Test
  public void testFailoverRetryTxIdMisMatch() throws Exception {
    setUp("testFailoverRetryTxIdMisMatch");
    createEditsNotSynced(20);
    fs.close();
    FailoverTestUtilHandler handler = new FailoverTestUtilHandler();
    handler.simulateShutdownCrash = false;
    InjectionHandler.set(handler);
    // Txid will now mismatch.
    cluster.getPrimaryAvatar(0).avatar.namesystem.getEditLog()
        .setLastWrittenTxId(0);
    AvatarNode node = cluster.getStandbyAvatar(0).avatar;
    try {
      cluster.failOver();
      fail("Did not throw exception");
    } catch (IOException ie) {
      while (!handler.waitForRestartTrigger) {
        LOG.info("Waiting for restart");
        Thread.sleep(1000);
      }
      tryConnect(node);
      cluster.failOver(true);
      tryConnect(node);
    }
  }

  @Test
  public void testFailoverRetryBlocksMisMatch() throws Exception {
    setUp("testFailoverRetryBlocksMisMatch");
    int totalBlocks = 50;
    FailoverTestUtilHandler handler = new FailoverTestUtilHandler();
    handler.simulateShutdownCrash = false;
    InjectionHandler.set(handler);
    DFSTestUtil.createFile(fs, new Path("/testFailoverRetryBlocksMisMatch"),
        (long) totalBlocks * 1024, (short) 3, System.currentTimeMillis());
    // This sets the primary number of blocks to 0.
    cluster.getPrimaryAvatar(0).avatar.namesystem.close();
    cluster.getPrimaryAvatar(0).avatar.namesystem.blocksMap.close();
    AvatarNode node = cluster.getStandbyAvatar(0).avatar;
    try {
      cluster.failOver();
      fail("Did not throw exception");
    } catch (Exception e) {
      while (!handler.waitForRestartTrigger) {
        LOG.info("Waiting for restart");
        Thread.sleep(1000);
      }
      tryConnect(node);
      cluster.failOver(true);
      tryConnect(node);
    }
  }

  @Test
  public void testFailoverRetryStandbyQuiesce() throws Exception {
    setUp("testFailoverRetryBlocksMisMatch", false);
    int totalBlocks = 50;
    DFSTestUtil.createFile(fs, new Path("/testFailoverRetryBlocksMisMatch"),
        (long) totalBlocks * 1024, (short) 3, System.currentTimeMillis());
    // This should shutdown the standby.
    cluster.getStandbyAvatar(0).avatar
        .quiesceStandby(FSEditLogLoader.TXID_IGNORE);
    while (!restartDone) {
      LOG.info("Waiting for restart..");
      Thread.sleep(1000);
    }
    try {
      tryConnect(cluster.getStandbyAvatar(0).avatar);
      fail("Did not throw exception");
    } catch (Exception e) {
      LOG.warn("Expected exception : ", e);
    }
  }
}
