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
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.MiniDFSCluster.DataNodeProperties;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.FSConstants.DatanodeReportType;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.net.NetUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestBlockPlacementPolicyHBase {

  private static final String TARGET_REGION_PREFIX = "/TESTCLUSTER-DC1-HBASE/tableName1/012b45c789012a45e78f01d456e89012";
  private static final int BLOCK_REPORT_INTERVAL = 1000; // milliseconds
  private static final int HEARTBEAT_RECHECK_INTERVAL = 2500; // milliseconds

  private static final long HEARTBEAT_INTERVAL = 1; // seconds
  private static final long BLOCK_SIZE = 64L * 1024L;
  private static final long FILE_LENGTH = 8 * BLOCK_SIZE;

  public static final Log LOG = LogFactory.getLog(BlockPlacementPolicyHBase.class);

  private static short REPLICATION = 3;
  private static final String[] locations = new String[] { "/d1/r1", "/d1/r2", "/d1/r3", "/d1/r3",
      "/d1/r3", "/d1/r3", };

  private static final String[] hFiles = {
      "/TESTCLUSTER-DC1-HBASE/tableName1/012b45c789012a45e78f01d456e89012/CF1/012b412a2a23572f023265fff56e8901",
      "/TESTCLUSTER-DC1-HBASE/tableName1/012b45c789012a45e78f01d456e89012/CF1/012b412a2a237129837915fff56e8901",
      "/TESTCLUSTER-DC1-HBASE/tableName1/012b45c789012a45e78f01d456e89012/CF1/012b412a2a212387193815fff56e8901",
      "/TESTCLUSTER-DC1-HBASE/tableName1/012b45c789012a45e78f01d456e89012/CF2/012b412a2a23572f0232659218302901",
      "/TESTCLUSTER-DC1-HBASE/tableName1/012b45c789012a45e78f01d456e89012/CF2/0112380a2a23572f023265fff56e8901",
      "/TESTCLUSTER-DC1-HBASE/tableName1/012b45c789012a45e78f01d456e89012/CF2/012b41221381072f023265fff56e8901",
      "/TESTCLUSTER-DC1-HBASE/tableName1/01sb4bc789012d45e78f02d4f6e8901f/CF4/012123803923572f023265fff56e8901",
      "/TESTCLUSTER-DC1-HBASE/tableName2/012b41232a23578f0232654256e89012/CF5/012b412a2a235723803365fff56e8901",
      "/TESTCLUSTER-DC1-HBASE/tableName2/012b41232a23578f0232654256e89012/CF5/012b412a2a212387921365fff56e8901",
      "/TESTCLUSTER-DC1-HBASE/tableName2/012b4123223871203789472156e89012/CF6/012b412a2a23572f0232657237832901",
      "/some/other/file1", "/some/other/file2", "/some/other/file3", "/rootFile", };

  private MiniDFSCluster cluster;
  private Configuration conf;
  private DistributedFileSystem fs;
  private FSNamesystem nameSystem;
  private NameNode nameNode;
  private InetSocketAddress[] favoredNodes;
  private String[] favoredHosts;

  @Before
  public void setUp() throws IOException {
    conf = new Configuration();
    conf.setClass("dfs.block.replicator.classname", BlockPlacementPolicyHBase.class,
        BlockPlacementPolicy.class);
    conf.setInt("dfs.replication", REPLICATION);
    conf.setLong("dfs.heartbeat.interval", HEARTBEAT_INTERVAL);
    conf.setInt("heartbeat.recheck.interval", HEARTBEAT_RECHECK_INTERVAL);
    conf.setInt("dfs.blockreport.intervalMsec", BLOCK_REPORT_INTERVAL);
    conf.setLong("dfs.block.size", BLOCK_SIZE);
    conf.setBoolean("dfs.permissions", false);

    // Create hosts file necessary for BlockPlacementPolicyConfigurable.
    File baseDir = MiniDFSCluster.getBaseDirectory(conf);
    baseDir.mkdirs();

    cluster = new MiniDFSCluster(conf, locations.length, locations, null, true, true, true);
    nameNode = cluster.getNameNode();
    nameSystem = nameNode.getNamesystem();
    fs = (DistributedFileSystem) cluster.getFileSystem();
    favoredNodes = new InetSocketAddress[REPLICATION];
    favoredHosts = new String[REPLICATION];
    for (int i = 0; i < REPLICATION; i++) {
      favoredNodes[i] = cluster.getDataNodes().get(i).getSelfAddr();
      favoredHosts[i] = NetUtils.toIpPort(favoredNodes[i]);
    }

    for (String hFile : hFiles) {
      Path p = new Path(hFile);
      if (hFile.startsWith(TARGET_REGION_PREFIX)) {
        DFSTestUtil.createFile(fs, p, FILE_LENGTH, REPLICATION, 1, favoredNodes);
      } else {
        DFSTestUtil.createFile(fs, p, FILE_LENGTH, REPLICATION, 1);
      }
    }
  }

  @Test(timeout = 5 * 60 * 1000)
  public void testAndReportDeletionPolicy() throws IOException, InterruptedException {
    int nodeStop = 0;

    DataNodeProperties dataNode = cluster.stopDataNode(nodeStop);
    waitForDataNodeToDie(nodeStop);
    String situation1 = getSituation("DataNode stopped.");

    cluster.restartDataNode(dataNode);
    waitForDataNodeToBack(nodeStop);
    String situation2 = getSituation("DataNode restored.");

    LOG.info("Test Report.");
    LOG.info("Favor Nodes: " + Arrays.toString(favoredHosts));
    LOG.info("Node Stoped: " + favoredHosts[nodeStop]);
    LOG.info(situation1);
    LOG.info(situation2);
    LOG.info("LOG DONE!");

    for (String hFile : hFiles) {
      if (!hFile.startsWith(TARGET_REGION_PREFIX)) {
        continue;
      }

      List<LocatedBlock> lbs = nameNode.getBlockLocations(hFile, 0, FILE_LENGTH).getLocatedBlocks();
      for (LocatedBlock lb : lbs) {
        assertEquals("Not correctly replicated.", REPLICATION, lb.getLocations().length);
        for (DatanodeInfo dnInfo : lb.getLocations()) {
          assertTrue("A node not part of favored nodes discovered.", Arrays.asList(favoredHosts)
              .contains(dnInfo.getName()));
        }
      }
    }
  }

  @After
  public void tearDown() throws IOException {
    if (fs != null) {
      fs.close();
    }
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  private void waitForDataNodeToBack(int nodeIndex) throws InterruptedException, IOException {
    DatanodeInfo dataNodeInfo = getDataNodeInfo(nodeIndex);
    int sleepStep = 1000;
    // Wait to receive heart beat from the datanode.
    while (true) {
      if (nameSystem.getBlocks(dataNodeInfo, 1).getBlocks().length > 0) {
        break;
      }
      DFSTestUtil.waitNMilliSecond(sleepStep);
    }

    // Wait to process over-replicated blocks
    while (true) {
      nameSystem.readLock();
      try {
        if (nameSystem.overReplicatedBlocks.isEmpty() && nameSystem.excessReplicateMap.isEmpty())
          break;
      } finally {
        nameSystem.readUnlock();
      }
      DFSTestUtil.waitNMilliSecond(sleepStep);
    }
  }

  private DatanodeInfo getDataNodeInfo(int nodeIndex) throws IOException {
    for (DatanodeInfo dataNodeInfo : nameNode.getDatanodeReport(DatanodeReportType.ALL)) {
      if (dataNodeInfo.getName().equals(favoredHosts[nodeIndex])) {
        return dataNodeInfo;
      }
    }
    return null;
  }

  private void waitForDataNodeToDie(int nodeIndex) throws IOException, InterruptedException {
    DatanodeInfo dataNodeInfo = getDataNodeInfo(nodeIndex);
    int sleepStep = 1000;

    // Wait to namenode assigns new datanodes to under replicated blocks
    while (true) {
      boolean assigned = true;
      outer:
      for (String hFile : hFiles) {
        if (!hFile.startsWith(TARGET_REGION_PREFIX)) {
          continue;
        }

        List<LocatedBlock> lbs = nameNode.getBlockLocations(hFile, 0, FILE_LENGTH)
            .getLocatedBlocks();
        for (LocatedBlock lb : lbs) {
          if (lb.getLocations().length != REPLICATION
              || Arrays.asList(lb.getLocations()).contains(dataNodeInfo)) {
            assigned = false;
            break outer;
          }
        }
      }
      if (assigned) {
        break;
      }

      DFSTestUtil.waitNMilliSecond(sleepStep);
    }
  }

  private String getSituation(String header) throws IOException {
    StringBuilder info = new StringBuilder("\n");
    info.append("************************" + header + "**********************************\n");
    for (String hFile : hFiles) {
      List<LocatedBlock> lbs = nameSystem.getBlockLocations(hFile, 0, FILE_LENGTH)
          .getLocatedBlocks();
      info.append("Blocks for file " + hFile + ". This file has " + lbs.size() + " block(s).\n");
      int cnt = 0;
      for (LocatedBlock lb : lbs) {
        info.append("\t\t--Block #" + (++cnt) + " - " + lb.getBlock().getBlockId() + "\n");
        for (DatanodeInfo dn : lb.getLocations()) {
          info.append("\t\t\t\t--DataNode Host:" + dn.getHostName() + "\n");
        }
      }
    }
    info.append("****************************************************************************\n");
    return info.toString();
  }

}
