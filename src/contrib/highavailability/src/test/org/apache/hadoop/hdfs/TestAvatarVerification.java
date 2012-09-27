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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.util.InjectionEvent;
import org.apache.hadoop.hdfs.util.InjectionHandler;
import org.apache.hadoop.ipc.RemoteException;

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
  }

  @Test
  public void testVerificationAddress() throws Exception {
    LOG.info("------------------- test: testVerificationAddress START ----------------");
    TestAvatarVerificationHandler h = new TestAvatarVerificationHandler();
    InjectionHandler.set(h);
    MiniAvatarCluster.instantiationRetries = 2;

    conf = new Configuration();
    try {
      cluster = new MiniAvatarCluster(conf, 2, true, null, null);
      fail("Instantiation should not succeed");
    } catch (IOException e) {
      // exception during registration
      assertTrue("Should get remote exception here",
          e.getClass().equals(RemoteException.class));
    }
  }

  class TestAvatarVerificationHandler extends InjectionHandler {
    @Override
    protected void _processEvent(InjectionEvent event, Object... args) {
      if (event == InjectionEvent.NAMENODE_REGISTER) {
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
    }
  }
}
