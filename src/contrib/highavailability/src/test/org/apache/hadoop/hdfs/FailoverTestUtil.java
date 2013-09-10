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

import java.io.IOException;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.server.namenode.AvatarNode;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.server.namenode.ZookeeperTxId;
import org.apache.hadoop.hdfs.util.InjectionEvent;
import org.apache.hadoop.util.InjectionEventI;
import org.apache.hadoop.util.InjectionHandler;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.junit.After;
import static org.junit.Assert.*;

public class FailoverTestUtil {

  protected static MiniAvatarCluster cluster;
  protected static Configuration conf;
  protected static FileSystem fs;
  protected static Random random = new Random();
  protected static AvatarZooKeeperClient zkClient;
  protected static Log LOG = LogFactory.getLog(TestAvatarSyncLastTxid.class);

  public void setUp(String name) throws Exception {
  	setUp(name, true);
  }
  
  public void setUp(String name, boolean enableQJM) throws Exception {
    LOG.info("------------------- test: " + name + " START ----------------");
    MiniAvatarCluster.createAndStartZooKeeper();
    conf = new Configuration();
    conf.setBoolean("fs.checkpoint.enabled", true);
    conf.setBoolean("fs.ha.retrywrites", true);
    conf.setInt("dfs.block.size", 1024);
    cluster = new MiniAvatarCluster.Builder(conf).numDataNodes(3).enableQJM(enableQJM).build();
    fs = cluster.getFileSystem();
    zkClient = new AvatarZooKeeperClient(conf, null);
  }

  @After
  public void tearDown() throws Exception {
    zkClient.shutdown();
    if (cluster != null) {
      cluster.shutDown();
    }
    MiniAvatarCluster.clearZooKeeperData();
    MiniAvatarCluster.shutDownZooKeeper();
    InjectionHandler.clear();
  }

  protected void createEditsNotSynced(int nEdits) throws IOException {
    for (int i = 0; i < nEdits; i++) {
      // This creates unsynced edits by calling generation stamp.
      cluster.getPrimaryAvatar(0).avatar.namesystem
          .nextGenerationStampForTesting();
    }
  }

  protected void createEdits(int nEdits) throws IOException {
    for (int i = 0; i < nEdits / 2; i++) {
      // Create file ends up logging two edits to the edit log, one for create
      // file and one for bumping up the generation stamp
      fs.create(new Path("/" + random.nextInt()));
    }
  }

  protected ZookeeperTxId getLastTxid() throws Exception {
    AvatarNode primaryAvatar = cluster.getPrimaryAvatar(0).avatar;
    String address = primaryAvatar
        .getStartupConf().get(NameNode.DFS_NAMENODE_RPC_ADDRESS_KEY);
    return zkClient.getPrimaryLastTxId(address, false);
  }

  protected long getSessionId() throws Exception {
    AvatarNode primaryAvatar = cluster.getPrimaryAvatar(0).avatar;
    String address = primaryAvatar
        .getStartupConf().get(NameNode.DFS_NAMENODE_RPC_ADDRESS_KEY);
    return zkClient.getPrimarySsId(address, false);
  }

  protected void verifyState(long expectedTxid) throws Exception {
    ZookeeperTxId lastTxId = getLastTxid();
    assertNotNull(lastTxId);
    long sessionId = getSessionId();
    assertEquals(sessionId, lastTxId.getSessionId());
    assertEquals(expectedTxid - 1, lastTxId.getTransactionId());
  }

  public static class FailoverTestUtilHandler extends InjectionHandler {

    public volatile boolean simulateEditLogCrash = false;
    public volatile boolean simulateShutdownCrash = true;
    public volatile boolean standbyThreadCrash = false;
    public volatile boolean waitForRestartTrigger = false;

    @Override
    public boolean _falseCondition(InjectionEventI event, Object... args) {
      if (event == InjectionEvent.AVATARNODE_SHUTDOWN && simulateShutdownCrash) {
        // used to simulate a situation where zk
        // is not updated at primary shutdown
        LOG.info("Skipping the write to ZK");
        return true;
      }
      return false;
    }

    @Override
    public boolean _trueCondition(InjectionEventI event, Object... args) {
      if (event == InjectionEvent.FSNAMESYSTEM_CLOSE_DIRECTORY
          && simulateEditLogCrash) {
        LOG.warn("Simulating edit log crash, not closing edit log cleanly as"
            + "part of shutdown");
        return false;
      }
      return true;
    }

    @Override
    public void _processEvent(InjectionEventI event, Object... args) {
      if (event == InjectionEvent.AVATARNODE_WAIT_FOR_RESTART) {
        waitForRestartTrigger = true;
      }
    }
  }
}
