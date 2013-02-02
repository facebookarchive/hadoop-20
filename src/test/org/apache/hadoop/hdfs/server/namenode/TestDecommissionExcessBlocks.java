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
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.protocol.FSConstants.DatanodeReportType;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;

import static org.junit.Assert.*;
import org.junit.Test;

public class TestDecommissionExcessBlocks {
  private static MiniDFSCluster cluster;
  private static Configuration conf;

  private void waitForReplication(int repl) throws Exception {
    FSNamesystem namesystem = cluster.getNameNode().namesystem;
    boolean flag = false;
    long start = System.currentTimeMillis();
    while (!flag && (System.currentTimeMillis() - start < 30000)) {
      flag = true;
      for (LocatedBlock lbk : namesystem.getBlockLocations("/abc", 0,
          Integer.MAX_VALUE).getLocatedBlocks()) {
        int current = namesystem.countLiveNodes(lbk.getBlock());
        System.out.println("Waiting for " + repl + " replicas for : "
            + lbk.getBlock() + " current : " + current);
        if (repl != current) {
          flag = false;
          Thread.sleep(1000);
          break;
        }
      }
    }
    assertTrue(flag);
  }

  /**
   * This tests that the over replicated blocks number is consistent after a datanode
   * goes into decomission and comes back without going down.
   */
  @Test
  public void testDecommisionExcessBlocks() throws Exception {
    conf = new Configuration();
    conf.setInt("dfs.block.size", 1024);
    cluster = new MiniDFSCluster(conf, 3, true, null);
    try {
      DFSTestUtil.createFile(cluster.getFileSystem(), new Path("/abc"),
          (long) 1024 * 10, (short) 3, 0);
      cluster.startDataNodes(conf, 1, true, null, null);
      FSNamesystem namesystem = cluster.getNameNode().namesystem;

      DatanodeDescriptor dn = null;
      for (DatanodeDescriptor dnn : namesystem.getDatanodeListForReport(
            DatanodeReportType.LIVE)) {
        if (dnn.numBlocks() != 0) {
          dn = dnn;
          break;
        }
      }

      assertNotNull(dn);
      namesystem.startDecommission(dn);
      waitForReplication(3);

      namesystem.stopDecommission(dn);

      waitForReplication(4);

      assertEquals(10, namesystem.overReplicatedBlocks.size());
    } finally {
      cluster.shutdown();
    }
  }
}
