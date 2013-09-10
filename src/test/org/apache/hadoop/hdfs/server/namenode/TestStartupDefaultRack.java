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

import static org.junit.Assert.assertEquals;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.FSConstants;
import org.apache.hadoop.hdfs.protocol.FSConstants.DatanodeReportType;
import org.apache.hadoop.net.NetworkTopology;
import org.junit.Test;

public class TestStartupDefaultRack {

  private static Configuration conf;
  private static MiniDFSCluster cluster;

  @Test
  public void testStartup() throws IOException {
    conf = new Configuration();
    conf.setClass("dfs.block.replicator.classname", BlockPlacementPolicyConfigurable.class,
        BlockPlacementPolicy.class);
    conf.set(FSConstants.DFS_HOSTS, "hosts");
    cluster = new MiniDFSCluster(conf, 3,
        new String[] { "/r1", "/r2", NetworkTopology.DEFAULT_RACK }, null, true, false);
    DFSTestUtil util = new DFSTestUtil("/testStartup", 10, 10, 1024);
    util.createFiles(cluster.getFileSystem(), "/");
    util.checkFiles(cluster.getFileSystem(), "/");
    assertEquals(2, cluster.getNameNode().getDatanodeReport(DatanodeReportType.LIVE).length);
    cluster.shutdown();
  }
}
