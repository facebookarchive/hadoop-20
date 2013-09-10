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

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.junit.Test;
import static org.junit.Assert.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.protocol.FSConstants.SafeModeAction;
import org.apache.hadoop.hdfs.server.namenode.AvatarNode;
import org.apache.hadoop.hdfs.util.InjectionEvent;
import org.apache.hadoop.util.InjectionEventI;
import org.apache.hadoop.util.InjectionHandler;

public class TestAvatarShell extends AvatarSetupUtil {
  public void setUp(boolean federation, String name) throws Exception {
    Configuration conf = new Configuration();
    super.setUp(federation, conf, name);
  }

  @Test
  public void testFailoverWithAvatarShell() throws Exception {
    setUp(false, "testFailoverWithAvatarShell");
    int blocksBefore = blocksInFile();
    TestAvatarShellInjectionHandler h = new TestAvatarShellInjectionHandler();
    InjectionHandler.set(h);
    
    AvatarShell shell = new AvatarShell(conf);
    AvatarZKShell zkshell = new AvatarZKShell(conf);
    assertEquals(0, zkshell.run(new String[] { "-clearZK" }));
    assertEquals(0, shell.run(new String[] { "-zero", "-shutdownAvatar" }));
    // Wait for shutdown thread to finish.
    h.waitForShutdown();
    assertEquals(0, shell.run(new String[] { "-one", "-setAvatar", "primary" }));
    int blocksAfter = blocksInFile();
    assertTrue(blocksBefore == blocksAfter);
  }

  @Test
  public void testFailoverWithAvatarShellNew() throws Exception {
    setUp(false, "testFailoverWithAvatarShellNew");
    int blocksBefore = blocksInFile();

    AvatarShell shell = new AvatarShell(conf);
    assertEquals(0, shell.run(new String[] { "-failover" }));
    int blocksAfter = blocksInFile();
    assertTrue(blocksBefore == blocksAfter);
  }

  @Test
  public void testFailoverWithAvatarShellStandby() throws Exception {
    setUp(false, "testFailoverWithAvatarShellStandby");
    int blocksBefore = blocksInFile();
    cluster.failOver();
    cluster.restartStandby();

    AvatarShell shell = new AvatarShell(conf);
    assertEquals(0, shell.run(new String[] { "-failover" }));
    int blocksAfter = blocksInFile();
    assertTrue(blocksBefore == blocksAfter);
  }

  @Test
  public void testFailoverWithAvatarShellFederation()
      throws Exception {
    setUp(true, "testFailoverWithAvatarShellFederation");
    int blocksBefore = blocksInFile();
    AvatarShell shell = new AvatarShell(conf);
    String nsId = cluster.getNameNode(0).nameserviceId;
    assertEquals(
        0,
        shell.run(new String[] { "-failover", "-service", nsId }));
    int blocksAfter = blocksInFile();
    assertTrue(blocksBefore == blocksAfter);
  }

  @Test
  public void testFailoverWithAvatarShellStandbyFederation() throws Exception {
    setUp(true, "testFailoverWithAvatarShellStandbyFederation");
    int blocksBefore = blocksInFile();
    cluster.failOver(0);
    cluster.restartStandby(0);

    AvatarShell shell = new AvatarShell(conf);
    String nsId = cluster.getNameNode(0).nameserviceId;
    assertEquals(
        0,
        shell.run(new String[] { "-failover", "-service", nsId }));
    int blocksAfter = blocksInFile();
    assertTrue(blocksBefore == blocksAfter);
  }
  
  @Test(timeout=60000)
  public void testPrepareFailover() throws Exception {
    TestAvatarShellInjectionHandler h = new TestAvatarShellInjectionHandler();
    InjectionHandler.set(h);
    setUp(false, "testPrepareFailover");
    try {
      AvatarShell shell = new AvatarShell(conf);
      AvatarNode standbyAvatar = cluster.getStandbyAvatar(0).avatar;
      assertEquals(0, shell.run(new String[] { "-prepfailover" }));   
      assertTrue(standbyAvatar.isInSafeMode());
      while(h.prepareFailover.size() < cluster.getDataNodes().size()) {
        DFSTestUtil.waitNMilliSecond(100);
      }
    } finally {
      InjectionHandler.clear();
    }
  }

  @Test
  public void testAvatarShellLeaveSafeMode() throws Exception {
    setUp(false, "testAvatarShellLeaveSafeMode");
    int blocksBefore = blocksInFile();

    AvatarShell shell = new AvatarShell(conf);
    AvatarNode primaryAvatar = cluster.getPrimaryAvatar(0).avatar;
    primaryAvatar.setSafeMode(SafeModeAction.SAFEMODE_ENTER);
    assertTrue(primaryAvatar.isInSafeMode());
    assertEquals(0, shell.run(new String[] { "-zero", "-safemode", "leave" }));
    assertFalse(primaryAvatar.isInSafeMode());
    assertFalse(cluster.getPrimaryAvatar(0).avatar.isInSafeMode());
    int blocksAfter = blocksInFile();
    assertTrue(blocksBefore == blocksAfter);

  }

  @Test
  public void testAvatarShellLeaveSafeMode1() throws Exception {
    setUp(false, "testAvatarShellLeaveSafeMode1");
    int blocksBefore = blocksInFile();
    cluster.failOver();
    cluster.restartStandby();

    AvatarShell shell = new AvatarShell(conf);
    AvatarNode primaryAvatar = cluster.getPrimaryAvatar(0).avatar;
    primaryAvatar.setSafeMode(SafeModeAction.SAFEMODE_ENTER);
    assertTrue(primaryAvatar.isInSafeMode());
    assertEquals(0, shell.run(new String[] { "-one", "-safemode", "leave" }));
    assertFalse(primaryAvatar.isInSafeMode());
    assertFalse(cluster.getPrimaryAvatar(0).avatar.isInSafeMode());
    int blocksAfter = blocksInFile();
    assertTrue(blocksBefore == blocksAfter);
  }

  @Test
  public void testFailoverWithWaitTxid() throws Exception {
    setUp(false, "testFailoverWithWaitTxid");
    int blocksBefore = blocksInFile();

    AvatarShell shell = new AvatarShell(conf);
    AvatarZKShell zkshell = new AvatarZKShell(conf);
    assertEquals(0, zkshell.run(new String[] { "-clearZK" }));
    assertEquals(0, shell.run(new String[] { "-zero", "-shutdownAvatar" }));
    assertEquals(0, shell.run(new String[] { "-waittxid" }));
    assertEquals(0, shell.run(new String[] { "-one", "-setAvatar", "primary" }));
    int blocksAfter = blocksInFile();
    assertTrue(blocksBefore == blocksAfter);
  }

  @Test
  public void testGetSafeMode() throws Exception {
    setUp(false, "testGetSafeMode");
    AvatarShell shell = new AvatarShell(conf);
    assertEquals(0, shell.run(new String[] { "-zero", "-safemode", "get" }));
    assertEquals(0, shell.run(new String[] { "-one", "-safemode", "get" }));
  }

  public static class ShortTxidWaitAvatarShell extends AvatarShell {

    public ShortTxidWaitAvatarShell(Configuration conf) {
      super(conf);
    }

    @Override
    protected long getMaxWaitTimeForWaitTxid() {
      return 1000 * 15; // 15 seconds.
    }
  }

  @Test
  // avatarshell and avatarzkshell would fail
  // if dfs.federation.nameservices is set in the cluster config, while no -service specified
  public void testMandateSerivceWithAvatarShellFederation()
      throws Exception {
    setUp(true, "testMandateServiceWithAvatarShellFederation");
    AvatarShell shell = new AvatarShell(conf);
    AvatarZKShell zkshell = new AvatarZKShell(conf);
    assertEquals(-1, shell.run(new String[] { "-failover"}));
    assertEquals(-1, zkshell.run(new String[] { "-clearZK" }));
  }

  @Test
  public void testFailoverWithWaitTxidFail() throws Exception {
    setUp(false, "testFailoverWithWaitTxidFail");
    AvatarShell shell = new ShortTxidWaitAvatarShell(conf);
    // This should fail.
    assertEquals(-1, shell.run(new String[] { "-waittxid" }));
  }
  
  class TestAvatarShellInjectionHandler extends InjectionHandler {
    volatile boolean shutdownComplete = false;
    Set<String> prepareFailover = Collections
        .synchronizedSet(new HashSet<String>());
    
    @Override
    public void _processEvent(InjectionEventI event, Object... args) {
      if (event == InjectionEvent.AVATARNODE_SHUTDOWN_COMPLETE) {
        shutdownComplete = true;
      } else if (event == InjectionEvent.OFFERSERVICE_PREPARE_FAILOVER) {
        prepareFailover.add((String) args[0]);
      }
    }
    
    void waitForShutdown() {
      int i = 0;
      while (!shutdownComplete) {
        DFSTestUtil.waitSecond();
        if (++i > 30)
          break;
      }
    }
  }

}
