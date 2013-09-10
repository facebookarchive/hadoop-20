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
import java.util.Collections;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.impl.Log4JLogger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.qjournal.client.QuorumJournalManager;
import org.apache.hadoop.hdfs.qjournal.server.Journal;
import org.apache.hadoop.hdfs.server.namenode.AvatarNode;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.util.InjectionEvent;
import org.apache.hadoop.util.InjectionEventI;
import org.apache.hadoop.util.InjectionHandler;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Level;

import org.junit.After;
import org.junit.AfterClass;
import static org.junit.Assert.*;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestAvatarCheckpointingManual {

  final static Log LOG = LogFactory.getLog(TestAvatarCheckpointing.class);

  protected MiniAvatarCluster cluster;
  protected Configuration conf;
  private FileSystem fs;
  private Random random = new Random();

  {
  	((Log4JLogger)QuorumJournalManager.LOG).getLogger().setLevel(Level.ALL);
  	((Log4JLogger)Journal.LOG).getLogger().setLevel(Level.ALL);
  	((Log4JLogger)FSNamesystem.LOG).getLogger().setLevel(Level.ALL);
  }
  
  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    MiniAvatarCluster.createAndStartZooKeeper();
  }

  protected void setUp(long ckptPeriod, String name, boolean enableQJM) throws Exception {
    LOG.info("------------------- test: " + name + " START ----------------");
    conf = new Configuration();

    conf.setBoolean("fs.ha.retrywrites", true);
    conf.setBoolean("fs.checkpoint.enabled", true);
    conf.setLong("fs.checkpoint.period", ckptPeriod);
    conf.setBoolean("fs.checkpoint.delayed", true);

    // MiniAvatarCluster conf
    conf.setBoolean("fs.checkpoint.wait", false);

    cluster = new MiniAvatarCluster.Builder(conf).numDataNodes(0).enableQJM(enableQJM).build();
    fs = cluster.getFileSystem();
  }

  @After
  public void tearDown() throws Exception {
    if (fs != null) {
      fs.close();
    }
    if (cluster != null) {
      cluster.shutDown();
    }
    InjectionHandler.clear();
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    MiniAvatarCluster.shutDownZooKeeper();
  }

  public void createEdits(int nEdits) throws IOException {
    for (int i = 0; i < nEdits / 2; i++) {
      // Create file ends up logging two edits to the edit log, one for create
      // file and one for bumping up the generation stamp
      fs.create(new Path("/" + random.nextInt()));
    }
  }

  // ////////////////////////////

  @Test
  public void testDeployedCheckpoint() throws Exception {
  	doTestDelayedCheckpoint(false);
  }
  
  protected void doTestDelayedCheckpoint(boolean enableQJM) throws Exception {
    TestAvatarCheckpointingHandler h = new TestAvatarCheckpointingHandler();
    InjectionHandler.set(h);
    setUp(3600, "testDelayedCheckpoint", enableQJM);

    AvatarNode primary = cluster.getPrimaryAvatar(0).avatar;
    AvatarNode standby = cluster.getStandbyAvatar(0).avatar;
    createEdits(40);

    long start = System.currentTimeMillis();
    while (System.currentTimeMillis() - start < 10000) {
      if (h.receivedEvents.contains(InjectionEvent.STANDBY_DELAY_CHECKPOINT)) {
        // checkpoint has been delayed at least once
        break;
      }
    }
    // checkpoint was delayed
    assertTrue(h.receivedEvents
        .contains(InjectionEvent.STANDBY_DELAY_CHECKPOINT));
    // no actual checkpoint happened at startup
    assertFalse(h.receivedEvents
        .contains(InjectionEvent.STANDBY_BEFORE_ROLL_EDIT));

    if (!enableQJM) {
	    standby.quiesceStandby(DFSAvatarTestUtil.getCurrentTxId(primary) - 1);
	    // edits + SLS
	    assertEquals(41, DFSAvatarTestUtil.getCurrentTxId(primary));
    }
  }
  
  protected void runAndAssertCommand(AvatarShell shell, int expectedValue,
      String[] argv) throws Exception {
    assertEquals(expectedValue, shell.run(argv));
  }

  protected void assertCheckpointDone(TestAvatarCheckpointingHandler h) {
    // checkpoint was performed
    assertTrue(h.receivedEvents
        .contains(InjectionEvent.STANDBY_EXIT_CHECKPOINT));
    h.receivedEvents.clear();
  }

  static class TestAvatarCheckpointingHandler extends InjectionHandler {
    public Set<InjectionEventI> receivedEvents = Collections
        .synchronizedSet(new HashSet<InjectionEventI>());

    @Override
    protected void _processEvent(InjectionEventI event, Object... args) {
      receivedEvents.add(event);
    }

    @Override
    protected void _processEventIO(InjectionEventI event, Object... args)
        throws IOException {
      _processEvent(event, args);
    }
  }
}
