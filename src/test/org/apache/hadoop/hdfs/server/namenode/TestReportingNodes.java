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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.FSConstants.DatanodeReportType;

import org.junit.After;
import org.junit.Before;
import static org.junit.Assert.*;
import org.junit.Test;

/**
 * This tests whether the reporting nodes counter in the NameNode is correct.
 */
public class TestReportingNodes {
  private Configuration conf;
  private MiniDFSCluster cluster;

  @Before
  public void setUp() throws Exception {
    conf = new Configuration();
    conf.setInt("heartbeat.recheck.interval", 100);
    conf.setInt("dfs.heartbeat.interval", 1);
    conf.setInt("dfs.block.size", 1024);
    cluster = new MiniDFSCluster(conf, 3, true, null);
    DFSTestUtil.createFile(cluster.getFileSystem(), new Path(
        "/TestReportingNodes"),
        (long) 1024 * 10, (short) 3, 0);
  }

  @After
  public void tearDown() throws Exception {
    if (cluster != null)
      cluster.shutdown();
  }

  @Test
  public void testReportingNodesDNShutdown() throws Exception {
    FSNamesystem namesystem = cluster.getNameNode().namesystem;
    waitForNodesReporting(3, namesystem);

    cluster.shutdownDataNode(0, false);

    int live = namesystem.getDatanodeListForReport(DatanodeReportType.LIVE)
        .size();
    long start = System.currentTimeMillis();
    while (live != 2 && System.currentTimeMillis() - start < 30000) {
      live = namesystem.getDatanodeListForReport(DatanodeReportType.LIVE)
          .size();
      System.out.println("Waiting for live : " + live);
      Thread.sleep(1000);
    }
    assertEquals(2, live);

    waitForNodesReporting(2, namesystem);

    cluster.restartDataNode(0);
    waitForNodesReporting(3, namesystem);
  }

  private void waitForNodesReporting(int expectedReportingNodes,
      FSNamesystem namesystem)
      throws Exception {
    long start = System.currentTimeMillis();
    while (expectedReportingNodes != namesystem.getReportingNodes()
        && System.currentTimeMillis() - start < 10000) {
      System.out.println("Waiting for reporting : " + expectedReportingNodes
          + " current : "
          + namesystem.getReportingNodes());
      Thread.sleep(1000);
    }
    assertEquals(expectedReportingNodes, namesystem.getReportingNodes());
  }
}
