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
import static org.junit.Assert.assertNull;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSAvatarTestUtil;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.MiniAvatarCluster;
import org.apache.hadoop.hdfs.util.GSet;
import org.apache.hadoop.util.InjectionHandler;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestStandbyINodeId {
  private static MiniAvatarCluster cluster;
  private static Configuration conf;

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
  }
  
  @Before
  public void setUp() throws Exception {
    conf = new Configuration();
    cluster = new MiniAvatarCluster.Builder(conf).numDataNodes(2).enableQJM(false).build();
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    MiniAvatarCluster.shutDownZooKeeper();
  }
  
  @Test
  public void testStandbyInodeId() throws Exception {
    FileSystem fs = cluster.getFileSystem();
    long expectedLastINodeId = INodeId.ROOT_INODE_ID;
    /**
     * Create two files: /testone and /testone/file
     * Before this, we have
     * /
     */
    DFSTestUtil.createFile(fs, new Path("/testone/file"), 1024, (short) 1, 0);
    AvatarNode primaryAvatar = cluster.getPrimaryAvatar(0).avatar;
    AvatarNode standbyAvatar = cluster.getStandbyAvatar(0).avatar;
    FSNamesystem primaryNS = primaryAvatar.namesystem;
    FSNamesystem standbyNS = standbyAvatar.namesystem;

    INode primaryINode = primaryNS.dir.getINode("/testone/file");
    long expectedprimaryINodeId = primaryINode.getId();
    expectedLastINodeId += 2;
    DFSAvatarTestUtil.assertTxnIdSync(primaryAvatar, standbyAvatar);
    assertEquals(expectedLastINodeId, standbyNS.dir.getLastInodeId());
    assertEquals(expectedprimaryINodeId, standbyNS.dir.getINode("/testone/file").getId());
    DFSTestUtil.assertInodemapEquals(primaryNS.dir.getInodeMap(), standbyNS.dir.getInodeMap());
    
    /**
     * Create another two files
     * Before this, we have
     * /
     *  /testone
     *    /file
     */
    DFSTestUtil.createFile(fs, new Path("/testtwo/fileone"), 1024, (short) 1, 0);
    expectedLastINodeId += 2;
    expectedprimaryINodeId += 2;
    DFSAvatarTestUtil.assertTxnIdSync(primaryAvatar, standbyAvatar);
    assertEquals(expectedLastINodeId, standbyNS.dir.getLastInodeId());
    assertEquals(expectedprimaryINodeId, standbyNS.dir.getINode("/testtwo/fileone").getId());
    
    /**
     * delete a file
     * Before this, we have
     * /
     *  /testone
     *    /file
     *  /testtwo
     *    /fileone
     */
    fs.delete(new Path("/testone/file"), true);
    DFSAvatarTestUtil.assertTxnIdSync(primaryAvatar, standbyAvatar);
    assertEquals(expectedLastINodeId, standbyNS.dir.getLastInodeId());
    DFSTestUtil.assertInodemapEquals(primaryNS.dir.getInodeMap(), standbyNS.dir.getInodeMap());
    
    // Restart should not affect
    cluster.restartAvatarNodes();
    primaryAvatar = cluster.getPrimaryAvatar(0).avatar;
    standbyAvatar = cluster.getStandbyAvatar(0).avatar;
    primaryNS = primaryAvatar.namesystem;
    standbyNS = standbyAvatar.namesystem;
    
    /**
     * Before this, we have
     * /
     *  /testone
     *  /testtwo
     *    /fileone
     */
    DFSAvatarTestUtil.assertTxnIdSync(primaryAvatar, standbyAvatar);
    assertEquals(INodeId.ROOT_INODE_ID, standbyNS.dir.getINode("/").getId());
    assertEquals(expectedLastINodeId, standbyNS.dir.getLastInodeId());
    DFSTestUtil.assertInodemapEquals(primaryNS.dir.getInodeMap(), standbyNS.dir.getInodeMap());
    assertEquals(primaryNS.dir.getINode("/testone").getId(), 
        standbyNS.dir.getINode("/testone").getId());
  }
  
  /**
   * Test if standby pick up the correct inode id after failover
   * @throws Exception
   */
  @Test
  public void testFailOverInodeId() throws Exception {
    FileSystem fs = cluster.getFileSystem();
    long expectedLastINodeId = INodeId.ROOT_INODE_ID;
    GSet<INode, INode> expectedINodeMap = null;
    
    DFSTestUtil.createFile(fs, new Path("/testone/file"), 1024, (short) 1, 0);
    FSNamesystem primaryNS = cluster.getPrimaryAvatar(0).avatar.namesystem;
    INode primaryINode = primaryNS.dir.getINode("/testone/file");
    long expectedprimaryINodeId = primaryINode.getId();
    expectedLastINodeId += 2;
    expectedINodeMap = primaryNS.dir.getInodeMap();
    
    cluster.killPrimary();
    cluster.failOver();
    
    // standby become primary after fail over
    primaryNS = cluster.getPrimaryAvatar(0).avatar.namesystem;
    assertNull(cluster.getStandbyAvatar(0));
    assertEquals(expectedLastINodeId, primaryNS.dir.getLastInodeId());
    assertEquals(expectedprimaryINodeId, primaryNS.dir.getINode("/testone/file").getId());
    DFSTestUtil.assertInodemapEquals(expectedINodeMap, primaryNS.dir.getInodeMap());

  }
}
