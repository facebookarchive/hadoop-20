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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.MiniAvatarCluster;
import org.apache.hadoop.hdfs.util.InjectionEvent;
import org.apache.hadoop.util.InjectionEventI;
import org.apache.hadoop.util.InjectionHandler;

import org.junit.After;
import org.junit.AfterClass;
import static org.junit.Assert.*;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestStandbyDataTransferVersion {

  private static MiniAvatarCluster cluster;
  private static Configuration conf;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    MiniAvatarCluster.createAndStartZooKeeper();
  }

  @Before
  public void setup() throws Exception {
    conf = new Configuration();
    cluster = new MiniAvatarCluster(conf, 1, true, null, null);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    MiniAvatarCluster.shutDownZooKeeper();
  }

  @After
  public void tearDown() throws Exception {
    InjectionHandler.clear();
    cluster.shutDown();
  }

  private static class TestHandler extends InjectionHandler {

    @Override
    protected void _processEventIO(InjectionEventI event, Object... args) throws IOException {
      if (event == InjectionEvent.AVATARNODE_RECEIVED_DATA_TRANSFER_VERSION) {
    	  throw new IOException("Incorrect data transfer version");
      }
    }
  }

  @Test
  public void testStandbyNotStarting() throws Exception {
    assertTrue(true);
    boolean pass = false;
    InjectionHandler.set(new TestHandler());
    cluster.killStandby();
    try {
      cluster.restartStandby();
    } catch (IOException e) {
      pass = true;
    }
    assertTrue(pass);
    assertTrue(cluster.getStandbyAvatar(0) == null);
  }

}
