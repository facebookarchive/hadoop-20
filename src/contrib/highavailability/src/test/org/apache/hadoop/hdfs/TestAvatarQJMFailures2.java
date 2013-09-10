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

import static org.junit.Assert.fail;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.TestAvatarQJMFailures.TestAvatarQJMFailuresHandler;
import org.apache.hadoop.hdfs.qjournal.MiniJournalCluster;
import org.apache.hadoop.hdfs.server.namenode.FSEditLog;
import org.apache.hadoop.util.InjectionHandler;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestAvatarQJMFailures2 {
 private static final Log LOG = LogFactory.getLog(TestAvatarQJMFailures2.class);
  
  private Configuration conf;
  private MiniAvatarCluster cluster;
  private FileSystem fs;
  private MiniJournalCluster journalCluster;
  
  private TestAvatarQJMFailuresHandler handler;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    MiniAvatarCluster.createAndStartZooKeeper();
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    MiniAvatarCluster.shutDownZooKeeper();
  }

  public void setUp(String name) throws Exception {
    LOG.info("START TEST : " + name);
    
    handler = new TestAvatarQJMFailuresHandler();
    InjectionHandler.set(handler);

    FSEditLog.setRuntimeForTesting(Runtime.getRuntime());
    conf = new Configuration();
    cluster = new MiniAvatarCluster.Builder(conf).numDataNodes(1)
          .enableQJM(true).build();
    fs = cluster.getFileSystem();
    journalCluster = cluster.getJournalCluster();
  }
  
  @After
  public void tearDown() throws Exception {
    if (fs != null)
      fs.close();
    if (cluster != null)
      cluster.shutDown();
    InjectionHandler.clear();
  }

  /**
   * Tests that if a majority of journal nodes fail, the Namenode fails.
   */
  @Test
  public void testMultipleFailures() throws Exception {
    setUp("testMultipleFailures");
    DFSTestUtil.createFile(fs, new Path("/test"), 1024, (short) 1, 0);
    journalCluster.getJournalNode(0).stopAndJoin(0);
    journalCluster.getJournalNode(1).stopAndJoin(0);
    DFSTestUtil.createFile(fs, new Path("/test1"), 1024, (short) 1, 0);

    try {
      handler.doCheckpoint();
      fail("Did not throw IOException");
    } catch (IOException ie) {
      LOG.warn("Expected exception", ie);
    } 
  }
}
