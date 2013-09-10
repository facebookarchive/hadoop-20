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

import org.apache.hadoop.hdfs.server.namenode.AvatarNode;
import org.apache.hadoop.hdfs.server.namenode.ZookeeperTxId;
import org.apache.hadoop.util.InjectionHandler;
import org.apache.hadoop.fs.Path;
import org.apache.zookeeper.KeeperException.NoNodeException;

import static org.junit.Assert.*;
import org.junit.Test;

public class TestAvatarSyncLastTxid extends FailoverTestUtil {

  @Test
  public void testBasic() throws Exception {
    setUp("testBasic");
    createEdits(20);
    // fs.close() creates 10 more edits due to close file ops.
    fs.close();
    cluster.shutDown();
    // +3 initial checkpoint +1 shutdown
    verifyState(34);
  }

  @Test
  public void testPrimaryCrash() throws Exception {
    setUp("testPrimaryCrash");
    createEdits(20);
    fs.close();
    InjectionHandler.set(new FailoverTestUtilHandler());
    cluster.getPrimaryAvatar(0).avatar.shutdownAvatar();
    InjectionHandler.clear();
    ZookeeperTxId lastTxId = null;
    try {
      lastTxId = getLastTxid();
    } catch (NoNodeException e) {
      LOG.info("Expected exception", e);
      assertNull(lastTxId);
      return;
    }
    fail("Did not throw : " + NoNodeException.class);
  }

  @Test
  public void testPrimaryCrashWithExistingZkNode() throws Exception {
    setUp("testPrimaryCrashWithExistingZkNode");
    // First do a clean shutdown.
    createEdits(20);
    // fs.close() creates 10 more edits due to close file ops.
    fs.close();
    cluster.shutDown();
    // 3 for initial checkpoint + 1 shutdown
    verifyState(34);

    // Now we have an existing znode with the last transaction id, now do an
    // unclean shutdown and verify session ids don't match.
    cluster = new MiniAvatarCluster(conf, 3, true, null, null);
    fs = cluster.getFileSystem();
    createEdits(20);
    fs.close();
    InjectionHandler.set(new FailoverTestUtilHandler());
    cluster.getPrimaryAvatar(0).avatar.shutdownAvatar();
    InjectionHandler.clear();
    ZookeeperTxId lastTxId = null;
    try {
      lastTxId = getLastTxid();
    } catch (NoNodeException e) {
      LOG.info("Expected exception", e);
      assertNull(lastTxId);
      return;
    }
    fail("Did not throw : " + NoNodeException.class);
  }

  @Test
  public void testWithFailover() throws Exception {
    setUp("testWithFailover");
    createEdits(20);
    cluster.failOver();
    createEdits(20);
    // 20 edits since we are closing 20 files.
    fs.close();
  }

  @Test
  public void testWithDoubleFailover() throws Exception {
    setUp("testWithDoubleFailover");
    // Perform first failover
    createEdits(20);
    cluster.failOver();
    createEdits(20);
    cluster.restartStandby();

    // Perform second failover.
    createEdits(20);
    cluster.failOver();
    createEdits(20);

    // 40 edits since we are closing 40 files.
    fs.close();
  }

  @Test
  public void testFailoverWithPrimaryCrash() throws Exception {
    setUp("testFailoverWithPrimaryCrash");
    createEdits(20);
    fs.close();
    
    InjectionHandler.set(new FailoverTestUtilHandler());
    cluster.killPrimary(0, true);
    InjectionHandler.clear();
    
    try {
      cluster.failOver();
    } catch (Exception e) {
      LOG.info("Expected exception", e);
      return;
    }
    fail("Did not throw exception");
  }

  @Test
  public void testDoubleFailoverWithPrimaryCrash() throws Exception {
    setUp("testDoubleFailoverWithPrimaryCrash");
    // First failover
    createEdits(20);
    cluster.failOver();
    cluster.restartStandby();
    createEdits(20);
    fs.close();

    // Second failover.
    InjectionHandler.set(new FailoverTestUtilHandler());
    cluster.killPrimary(0, true);
    InjectionHandler.clear();
    
    try {
      cluster.failOver();
    } catch (Exception e) {
      LOG.info("Expected exception : ", e);
      return;
    }
    fail("Did not throw : " + NoNodeException.class);
  }

  @Test
  public void testFailoverAfterUnsuccessfulFailover() throws Exception {
    setUp("testFailoverAfterUnsuccessfulFailover");
    createEdits(20);
    fs.close();
    
    InjectionHandler.set(new FailoverTestUtilHandler());
    cluster.killPrimary(0, true);
    InjectionHandler.clear();
    
    try {
      cluster.failOver();
    } catch (Exception e) {
      LOG.info("Expected exception : ", e);
      // Now restart avatarnodes and do a clean failover.
      cluster.restartAvatarNodes();
      fs = cluster.getFileSystem();
      createEdits(20);
      cluster.failOver();
      return;
    }
    fail("Did not throw exception");
  }

  @Test
  public void testEditLogCrash() throws Exception {
    setUp("testEditLogCrash", false);
    FailoverTestUtilHandler h = new FailoverTestUtilHandler();
    h.simulateEditLogCrash = true;
    InjectionHandler.set(h);
    cluster.shutDown();
    conf.setBoolean("fs.ha.retrywrites", true);
    cluster = new MiniAvatarCluster.Builder(conf).numDataNodes(3).enableQJM(false).build();
    fs = cluster.getFileSystem();
    zkClient = new AvatarZooKeeperClient(conf, null);
    createEdits(20);
    // These 20 edits should not be persisted, since edit log is going to crash.
    createEditsNotSynced(20);
    try {
      cluster.failOver();
    } catch (Exception e) {
      LOG.info("Expected exception : ", e);
      // initial checkpoint 3 + 20
      assertEquals(23, cluster.getStandbyAvatar(0).avatar.getLastWrittenTxId() + 1);
      return;
    }
    fail("Did not throw exception");
  }
  
  @Test
  public void testBlocksMisMatch() throws Exception {
    setUp("testBlocksMisMatch", false);
    int totalBlocks = 50;
    DFSTestUtil.createFile(fs, new Path("/testBlocksMisMatch"),
        (long) totalBlocks * 1024, (short) 3, System.currentTimeMillis());
    AvatarNode standby = cluster.getStandbyAvatar(0).avatar;
    standby.quiesceStandby(cluster
        .getPrimaryAvatar(0).avatar.getLastWrittenTxId());
    // This sets the standby number of blocks to 0.
    standby.namesystem.close();
    try {
      cluster.failOver();
    } catch (Exception e) {
      LOG.info("Expected exception : ", e);
      return;
    }
    fail("Did not throw exception");
  }
}
