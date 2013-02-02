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
package org.apache.hadoop.hdfs.server.datanode;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.MiniAvatarCluster;
import org.apache.hadoop.hdfs.protocol.FSConstants.DatanodeReportType;
import org.apache.hadoop.hdfs.server.common.IncorrectVersionException;
import org.apache.hadoop.hdfs.util.InjectionEvent;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.util.InjectionEventI;
import org.apache.hadoop.util.InjectionHandler;

import static org.junit.Assert.*;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestAvatarDatanodeVersion {

  private Configuration conf;
  private MiniAvatarCluster cluster;
  private static volatile boolean done = false;

  @Before
  public void setUp() throws Exception {
    MiniAvatarCluster.createAndStartZooKeeper();
    conf = new Configuration();
    done = false;
    cluster = new MiniAvatarCluster(conf, 0, true, null, null);
  }

  @After
  public void tearDown() throws Exception {
    cluster.shutDown();
    MiniAvatarCluster.shutDownZooKeeper();
    InjectionHandler.clear();
  }

  private static class TestHandler extends InjectionHandler {
    private int service = 1;
    TestHandler(int service) {
      this.service = service;
    }
    @Override
    protected void _processEventIO(InjectionEventI event, Object... args)
    throws IOException {
      if (service == 1 && event == InjectionEvent.AVATARDATANODE_BEFORE_START_OFFERSERVICE1) {
        done = true;
        throw new RemoteException(IncorrectVersionException.class.getName(),
            "Unexpected data transfer protocol version");
      }
      if (service == 2 && event == InjectionEvent.AVATARDATANODE_BEFORE_START_OFFERSERVICE2) {
        done = true;
        throw new RemoteException(IncorrectVersionException.class.getName(),
            "Unexpected data transfer protocol version");
      }
    }
  }

  private void waitForDone() { // wait for at most 30s
    long start = System.currentTimeMillis();
    while (System.currentTimeMillis() - start < 300000 && !done) {
      // Wait for trigger.
      System.out.println("Waiting for trigger");
      DFSTestUtil.waitSecond();;
    }
    if (!done) {
      fail("Event not triggered yet");
    }
  }

  /** Test when standby registration throws IncorrectVersion */
  @Test
  public void testDatanodeVersionStandby() throws Exception {
    InjectionHandler.set(new TestHandler(2));
    cluster.startDataNodes(1, null, null, conf);
    waitForDone();
    int dnReports = cluster.getPrimaryAvatar(0).avatar
        .getDatanodeReport(DatanodeReportType.LIVE).length;
    int dnStandbyReports = cluster.getStandbyAvatar(0).avatar
        .getDatanodeReport(DatanodeReportType.LIVE).length;
    long start = System.currentTimeMillis();
    while (System.currentTimeMillis() - start < 10000 && dnReports != 1) {
      System.out.println("Waiting for dn report");
      DFSTestUtil.waitSecond();
      dnReports = cluster.getPrimaryAvatar(0).avatar
          .getDatanodeReport(DatanodeReportType.LIVE).length;
      dnStandbyReports = cluster.getStandbyAvatar(0).avatar
      .getDatanodeReport(DatanodeReportType.LIVE).length;
    }
    assertEquals(1, dnReports);
    assertEquals(0, dnStandbyReports);
    assertEquals(1, cluster.getDataNodes().size());
    assertTrue(cluster.getDataNodes().get(0).isDatanodeUp());
  }

  /* Test when primary registration throws IncorrectVersion */
  @Test
  public void testDatanodeVersionPrimary() throws Exception {
    InjectionHandler.set(new TestHandler(1));
    cluster.startDataNodes(1, null, null, conf);
    waitForDone();

    int dnReports = cluster.getPrimaryAvatar(0).avatar
        .getDatanodeReport(DatanodeReportType.LIVE).length;
    int dnStandbyReports = cluster.getStandbyAvatar(0).avatar
        .getDatanodeReport(DatanodeReportType.LIVE).length;
    long start = System.currentTimeMillis();
    while (System.currentTimeMillis() - start < 10000) {
      System.out.println("Waiting for dn report");
      DFSTestUtil.waitSecond();;
      dnReports = cluster.getPrimaryAvatar(0).avatar
          .getDatanodeReport(DatanodeReportType.LIVE).length;
      dnStandbyReports = cluster.getStandbyAvatar(0).avatar
          .getDatanodeReport(DatanodeReportType.LIVE).length;
    }
    assertEquals(0, dnReports);
    assertEquals(1, dnStandbyReports);
    assertEquals(1, cluster.getDataNodes().size());
    assertFalse(cluster.getDataNodes().get(0).isDatanodeUp());
  }
}
