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
import java.io.File;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.List;

import org.junit.Test;
import org.junit.Before;
import org.junit.After;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.ReconfigurationException;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.FSConstants;
import org.apache.hadoop.net.Node; 

public class TestNameNodeReconfigure {

  public static final Log LOG =
    LogFactory.getLog(TestNameNodeReconfigure.class);

  public static class MockPlacementPolicy extends BlockPlacementPolicyDefault {
    private static AtomicInteger callCounter = null;

    public static synchronized void setCallCounter(AtomicInteger callCounter) {
      MockPlacementPolicy.callCounter = callCounter;
    }

    public static synchronized AtomicInteger getCallCounter() {
      return callCounter;
    }

    @Override
    DatanodeDescriptor[] chooseTarget(int numOfReplicas,
                                      DatanodeDescriptor writer,
                                      List<DatanodeDescriptor> chosenNodes,
                                      List<Node> exlcNodes,
                                      long blocksize) {
      getCallCounter().getAndIncrement();

      return super.chooseTarget(numOfReplicas,
                                writer,
                                chosenNodes,
                                exlcNodes,
                                blocksize);
    }
    
  }

  private MiniDFSCluster cluster;
  private FileSystem fs;

  public void setUp() throws IOException {
    Configuration conf = new Configuration();
    cluster = new MiniDFSCluster(conf, 3, true, null);
    fs = cluster.getFileSystem();
  }

  /**
   * Test that we can change the block placement policy through the
   * reconfigurable API.
   */
  @Test
  public void testChangeBlockPlacementPolicy()
    throws IOException, ReconfigurationException {
    setUp();
    AtomicInteger callCounter = new AtomicInteger(0);
    MockPlacementPolicy.setCallCounter(callCounter);

    DFSTestUtil util = new DFSTestUtil("", 2, 1, 512);

    // write some files with the default block placement policy
    util.createFiles(fs, "/reconfdat1", (short) 3);
    util.waitReplication(fs, "/reconfdat1", (short) 3);

    assertTrue("calls already made to MockPlacementPolicy",
               callCounter.get() == 0);

    // switch over to the mock placement policy
    cluster.getNameNode().reconfigureProperty("dfs.block.replicator.classname",
                                              "org.apache.hadoop.hdfs.server." +
                                              "namenode." +
                                              "TestNameNodeReconfigure$" +
                                              "MockPlacementPolicy");

    // write some files with the mock placement policy
    util.createFiles(fs, "/reconfdat2", (short) 3);
    util.waitReplication(fs, "/reconfdat2", (short) 3);

    int callsMade1 = callCounter.get();
    
    // check that calls were made to mock placement policy
    assertTrue("no calls made to MockPlacementPolicy",
               callsMade1 > 0);
    LOG.info("" + callsMade1 + " calls made to MockPlacementPolicy");

    // now try to change it to a non-existent class
    try {
      cluster.getNameNode().
        reconfigureProperty("dfs.block.replicator.classname",
                            "does.not.exist");
      fail("ReconfigurationException expected");
    } catch (RuntimeException expected) {
      assertTrue("exception should have cause", expected.getCause() != null);
      assertTrue("exception's cause should have cause",
                 expected.getCause().getCause() != null);
      assertTrue("ClassNotFoundException expected but got " +
                 expected.getCause().getCause().getClass().getCanonicalName(),
                 expected.getCause().getCause() instanceof
                 ClassNotFoundException);
    }

    // write some files, they should still go to the mock placemeny policy
    util.createFiles(fs, "/reconfdat3", (short) 3);
    util.waitReplication(fs, "/reconfdat3", (short) 3);

    int callsMade2 = callCounter.get();

    // check that more calls were made to mock placement policy
    assertTrue("no calls made to MockPlacementPolicy",
               callsMade2 > callsMade1);
    LOG.info("" + (callsMade2 - callsMade1) +
      " calls made to MockPlacementPolicy");

    // now revert back to the default policy
    cluster.getNameNode().reconfigureProperty("dfs.block.replicator.classname",
                                              null);

    // write some files with the default block placement policy
    util.createFiles(fs, "/reconfdat4", (short) 3);
    util.waitReplication(fs, "/reconfdat4", (short) 3);

    // make sure that no more calls were made to mock placement policy
    assertTrue("more calls made to MockPlacementPolicy",
               callCounter.get() == callsMade2);

    util.cleanup(fs, "/reconfdat1");
    util.cleanup(fs, "/reconfdat2");
    util.cleanup(fs, "/reconfdat3");
    util.cleanup(fs, "/reconfdat4");
  }


  /**
   * Test that we can modify configuration properties.
   */
  @Test
  public void testReconfigure() throws IOException,
         ReconfigurationException {
    setUp();
    // change properties
    cluster.getNameNode().reconfigureProperty("dfs.heartbeat.interval",
                                              "" + 6);
    cluster.getNameNode().reconfigureProperty("heartbeat.recheck.interval",
                                              "" +  (10 * 60 * 1000));
    cluster.getNameNode().reconfigureProperty("dfs.persist.blocks",
                                              "true");
    cluster.getNameNode().reconfigureProperty("dfs.permissions.audit.log",
                                              "true");

    // try invalid values
    try {
      cluster.getNameNode().reconfigureProperty("dfs.heartbeat.interval",
                                                "text");
      fail("ReconfigurationException expected");
    } catch (ReconfigurationException expected) {
    }
    try {
      cluster.getNameNode().reconfigureProperty("heartbeat.recheck.interval",
                                                "text");
      fail("ReconfigurationException expected");
    } catch (ReconfigurationException expected) {
    }
    try {
      cluster.getNameNode().reconfigureProperty("dfs.persist.blocks",
                                                "text");
      fail("ReconfigurationException expected");
    } catch (ReconfigurationException expected) {
    }
    try {
      cluster.getNameNode().reconfigureProperty("dfs.permissions.audit.log",
                                                "text");
      fail("ReconfigurationException expected");
    } catch (ReconfigurationException expected) {
    }

    // verify change
    assertEquals("dfs.heartbeat.interval has wrong value",
                 6000L, cluster.getNameNode().namesystem.heartbeatInterval);
    assertEquals("heartbeat.recheck.interval has wrong value",
                 10 * 60 * 1000,
                 cluster.getNameNode().namesystem.heartbeatRecheckInterval);
    assertTrue("dfs.persist.blocks has wrong value",
               cluster.getNameNode().namesystem.getPersistBlocks());
    assertTrue("dfs.permissions.audit.log has wrong value",
               cluster.getNameNode().namesystem.getPermissionAuditLog());

    // revert to defaults
    cluster.getNameNode().reconfigureProperty("dfs.heartbeat.interval",
                                              null);
    cluster.getNameNode().reconfigureProperty("heartbeat.recheck.interval",
                                              null);
    cluster.getNameNode().reconfigureProperty("dfs.persist.blocks",
                                              null);
    cluster.getNameNode().reconfigureProperty("dfs.permissions.audit.log",
                                              null);

    // verify defaults
    assertEquals("dfs.heartbeat.interval has wrong value",
                 3000L, cluster.getNameNode().namesystem.heartbeatInterval);
    assertEquals("heartbeat.recheck.interval has wrong value",
                 5 * 60 * 1000,
                 cluster.getNameNode().namesystem.heartbeatRecheckInterval);
    assertFalse("dfs.persist.blocks has wrong value",
                cluster.getNameNode().namesystem.getPersistBlocks());
    assertFalse("dfs.permissions.audit.log has wrong value",
                cluster.getNameNode().namesystem.getPermissionAuditLog());
  }

  /**
   * Test that includes/excludes will be ignored
   * if dfs.ignore.missing.include.files is set 
   */
  @Test
  public void testIncludesExcludesConfigure() throws IOException {
    String inFile = "/tmp/inFileNotExists";
    String exFile = "/tmp/exFileNotExists";
    File include = new File(inFile);
    File exclude = new File(exFile);
    include.delete();
    exclude.delete();
    assertFalse(include.exists());
    assertFalse(exclude.exists());

    Configuration conf = new Configuration();
    conf.set("dfs.hosts.ignoremissing", "true");
    conf.set(FSConstants.DFS_HOSTS, inFile);
    conf.set("dfs.hosts.exclude", exFile);
    cluster = new MiniDFSCluster(conf, 3, true, null);
  }

  @After
  public void shutDown() throws IOException {
    if (fs != null) {
      fs.close();
    }

    if (cluster != null) {
      cluster.shutdown();
    }
  }
}
