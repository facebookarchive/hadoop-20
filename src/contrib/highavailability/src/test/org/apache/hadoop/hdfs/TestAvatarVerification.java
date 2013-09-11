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

import static org.junit.Assert.*;

import java.io.IOException;
import java.lang.reflect.Field;
import java.net.InetAddress;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.server.namenode.FSImageTestUtil.CheckpointTrigger;
import org.apache.hadoop.hdfs.util.InjectionEvent;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.util.InjectionEventI;
import org.apache.hadoop.util.InjectionHandler;

import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestAvatarVerification {

  final static Log LOG = LogFactory.getLog(TestAvatarCheckpointing.class);

  private MiniAvatarCluster cluster;
  private Configuration conf;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    MiniAvatarCluster.createAndStartZooKeeper();
  }

  @After
  public void tearDown() throws Exception {
    if (cluster != null) {
      cluster.shutDown();
    }
    InjectionHandler.clear();
    MiniAvatarCluster.clearAvatarDir();
  }

  @Test
  public void testVerificationAddress() throws Exception {
    LOG.info("------------------- test: testVerificationAddress START ----------------");
    TestAvatarVerificationHandler h = new TestAvatarVerificationHandler();
    InjectionHandler.set(h);
    MiniAvatarCluster.instantiationRetries = 2;

    conf = new Configuration();
    try {
      cluster = new MiniAvatarCluster(conf, 0, true, null, null);
      fail("Instantiation should not succeed");
    } catch (IOException e) {
      // exception during registration
      assertTrue("Should get remote exception here",
          e.getClass().equals(RemoteException.class));
    }
  }
  
  @Test
  public void testVerificationRoll() throws Exception {
    LOG.info("------------------- test: testVerificationRoll START ----------------");
    try {
      conf = new Configuration();
      cluster = new MiniAvatarCluster(conf, 1, true, null, null);

      // wait to separate checkpoints
      DFSTestUtil.waitNMilliSecond(500);
      
      TestAvatarVerificationHandler h = new TestAvatarVerificationHandler();
      InjectionHandler.set(h);

      try {
        h.doCheckpoint();
        fail("Checkpoint should not succeed");
      } catch (IOException e) {
        // exception during roll
        LOG.info("Expected exception", e);
        assertTrue("Should get remote exception here",
            e.getClass().equals(RemoteException.class));
      }
      assertTrue(h.receivedEvents
          .contains(InjectionEvent.STANDBY_EXIT_CHECKPOINT_FAILED_ROLL));
    } finally {
      if (cluster != null) {
        cluster.shutDown();
      }
    }
  }

  class TestAvatarVerificationHandler extends InjectionHandler {
    
    private CheckpointTrigger ckptTrigger = new CheckpointTrigger();
    public Set<InjectionEventI> receivedEvents = Collections
        .synchronizedSet(new HashSet<InjectionEventI>());
    
    @Override
    protected void _processEvent(InjectionEventI event, Object... args) {
      receivedEvents.add(event);
      if (event == InjectionEvent.NAMENODE_VERIFY_CHECKPOINTER) {
        InetAddress addr = (InetAddress) args[0];
        LOG.info("Processing event : " + event + " with address: " + addr);
        try {
          Field f = addr.getClass().getSuperclass().getDeclaredField("address");
          f.setAccessible(true);
          f.set(addr, 0);
          LOG.info("Changed address to : " + addr);
        } catch (Exception e) {
          LOG.info("exception : ", e);
          return;
        }
      }
      ckptTrigger.checkpointDone(event, args);
    }
    
    @Override
    protected boolean _falseCondition(InjectionEventI event, Object... args) {
      return ckptTrigger.triggerCheckpoint(event);
    }
    
    void doCheckpoint() throws Exception { 
      ckptTrigger.doCheckpoint();  
    }
  }
}
