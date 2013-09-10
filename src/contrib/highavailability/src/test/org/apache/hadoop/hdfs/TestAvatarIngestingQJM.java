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

import static org.junit.Assert.assertEquals;

import org.apache.commons.logging.impl.Log4JLogger;
import org.apache.hadoop.hdfs.qjournal.client.IPCLoggerChannel;
import org.apache.hadoop.hdfs.qjournal.server.Journal;
import org.apache.hadoop.hdfs.qjournal.server.JournalNodeRpcServer;
import org.apache.hadoop.hdfs.server.namenode.AvatarNode;
import org.apache.hadoop.hdfs.util.InjectionEvent;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.util.InjectionHandler;
import org.apache.log4j.Level;
import org.junit.Test;

public class TestAvatarIngestingQJM extends TestAvatarIngesting {
  
  {
    ((Log4JLogger)IPCLoggerChannel.LOG).getLogger().setLevel(Level.ALL);
    ((Log4JLogger)Server.LOG).getLogger().setLevel(Level.ALL);
    ((Log4JLogger)JournalNodeRpcServer.LOG).getLogger().setLevel(Level.ALL);
    ((Log4JLogger)Journal.LOG).getLogger().setLevel(Level.ALL);
  }
  
  
	@Test
  public void testIngestFailureReading() throws Exception {
    testIngestFailure(InjectionEvent.INGEST_READ_OP, true);
  }
	
	@Test
  public void testIngestFailureTxidMismatch() throws Exception {
    testIngestFailure(InjectionEvent.INGEST_TXID_CHECK, true);
  }
	
  @Test
  public void testPrimaryRestart() throws Exception {
    // checks whether standby can continue ingesting with finalized segments
    // with no ending transaction
    TestAvatarIngestingHandler h = new TestAvatarIngestingHandler(null);
    InjectionHandler.set(h);
    h.gracefulShutdown = false;
    
    setUp(0, "testPrimaryRestart", false, false);
    // simulate interruption, no ckpt failure
    cluster.killStandby();   
    cluster.killPrimary();
    
    // this will produce finalized segment with no ending marker
    // and a new segment with txid = 1
    cluster.restartAvatarNodes();
    
    // create 20 edits
    createEdits(20);
    DFSTestUtil.waitNSecond(2);
    
    // current txid is the one to be written 1 + 1 = 2 + 20 transactions
    assertEquals(22, getCurrentTxId(cluster.getStandbyAvatar(0).avatar));
  }
	
  @Test
  public void testCheckpointWithRestarts() throws Exception {
    TestAvatarIngestingHandler h = new TestAvatarIngestingHandler(
        InjectionEvent.FSEDIT_LOG_WRITE_END_LOG_SEGMENT);
    InjectionHandler.set(h);
    setUp(3, "testCheckpointWithRestarts", true, true);
    // simulate interruption, no ckpt failure
    AvatarNode primary = cluster.getPrimaryAvatar(0).avatar;
    AvatarNode standby = cluster.getStandbyAvatar(0).avatar;
    createEdits(20);
    h.doCheckpoint();
    // 2 for initial checkpoint + 2 for this checkpoint + SLS + 20 edits
    h.disabled = false;
    assertEquals(25, getCurrentTxId(primary));

    cluster.killStandby();
    cluster.killPrimary();
    h.disabled = false;
    cluster.restartAvatarNodes();

    h.doCheckpoint();

    assertEquals(getCurrentTxId(primary), getCurrentTxId(standby));
  }
  
  @Test
  public void testIngestFailureSetupStream() throws Exception {
    InjectionEvent event = InjectionEvent.STANDBY_JOURNAL_GETSTREAM;
    TestAvatarIngestingHandler h = new TestAvatarIngestingHandler(event);
    InjectionHandler.set(h);
    h.setDisabled(false);

    setUp(3, "testIngestFailure: " + event, false, true);
    h.setDisabled(true);

    assertEquals(1, h.exceptions);
  }
  
  @Test
  public void testRecoverState() throws Exception {
    TestAvatarIngestingHandler h = new TestAvatarIngestingHandler(
        InjectionEvent.STANDBY_RECOVER_STATE);
    InjectionHandler.set(h);
    setUp(3, "testRecoverState", true, true);
    // simulate interruption, no ckpt failure
    AvatarNode primary = cluster.getPrimaryAvatar(0).avatar;
    AvatarNode standby = cluster.getStandbyAvatar(0).avatar;
    h.setDisabled(true);

    createEdits(20);
    h.doCheckpoint();
    // 2 for initial checkpoint + 2 for this checkpoint + SLS + 20 edits
    assertEquals(25, getCurrentTxId(primary));
    createEdits(20);
    h.setDisabled(false);

    // sleep to see if the state recovers correctly
    DFSTestUtil.waitNSecond(5);
    h.doCheckpoint();
    standby.quiesceStandby(getCurrentTxId(primary) - 1);

    assertEquals(47, getCurrentTxId(primary));
    assertEquals(getCurrentTxId(primary), getCurrentTxId(standby));
  }
}
