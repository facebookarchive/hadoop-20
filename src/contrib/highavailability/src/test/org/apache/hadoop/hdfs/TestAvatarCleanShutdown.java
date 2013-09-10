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

import java.io.File;
import java.io.FileWriter;
import java.io.PrintWriter;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;

import org.junit.Test;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.AfterClass;

import static org.junit.Assert.*;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.conf.Configuration;
import java.util.Set;
import java.util.HashSet;

import org.apache.hadoop.hdfs.protocol.FSConstants;
import org.apache.hadoop.hdfs.server.datanode.AvatarDataNode;
import org.apache.hadoop.hdfs.util.InjectionEvent;
import org.apache.hadoop.hdfs.MiniAvatarCluster;
import org.apache.hadoop.hdfs.MiniAvatarCluster.DataNodeProperties;
import org.apache.hadoop.hdfs.MiniAvatarCluster.NameNodeInfo;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.InjectionEventI;
import org.apache.hadoop.util.InjectionHandler;

public class TestAvatarCleanShutdown {
  final static Log LOG = LogFactory.getLog(TestAvatarCleanShutdown.class);

  private static final String HOST_FILE_PATH = "/tmp/include_file";
  private String hosts;

  private Configuration conf;
  private MiniAvatarCluster cluster;
  private boolean federation;
  private Set<Thread> oldThreads;

  @BeforeClass
  public static void setUpStatic() throws Exception {
    MiniAvatarCluster.createAndStartZooKeeper();
  }

  public void setUp(String name) throws Exception {
    LOG.info("------------------- test: " + name + ", federation: "
        + federation + " START ----------------");
    oldThreads = new HashSet<Thread>(Thread.getAllStackTraces().keySet());

    conf = new Configuration();
    hosts = HOST_FILE_PATH + "_" + System.currentTimeMillis();
    File f = new File(hosts);
    f.delete();
    f.createNewFile();
    conf.set(FSConstants.DFS_HOSTS, hosts);
    conf.setInt("dfs.datanode.failed.volumes.tolerated", 0);
    if (!federation) {
      cluster = new MiniAvatarCluster.Builder(conf).build();
    } else {
      cluster = new MiniAvatarCluster.Builder(conf)
      		.numNameNodes(2).federation(true).build();
    }
    federation = false;
  }
  
  private void writeWrongIncludeFile(Configuration conf) throws Exception {
    String includeFN = conf.get(FSConstants.DFS_HOSTS, "");
    assertTrue(includeFN.equals(hosts));
    File f = new File(includeFN);
    if (f.exists()) {
      f.delete();
    }
    PrintWriter writer = new PrintWriter(new FileWriter(f, true));
    try {
      writer.println("FAKENAME");
    } finally {
      writer.close();
    }
  }
  
  @Test
  public void testVolumeFailureShutdown() throws Exception {
    setUp("testVolumeFailureShutdown");
    LOG.info("Corrupt the volume");
    DataNodeProperties dnp = cluster.getDataNodeProperties().get(0);
    String[] dataDirs = dnp.conf.getStrings("dfs.data.dir");
    File vol =new File(dataDirs[0], "current");
    try {
      assertEquals("Couldn't chmod local vol", 0,
          FileUtil.chmod(vol.toString(), "444"));
      cluster.restartDataNodes();
    } catch (Exception e) {
      LOG.warn("Expected exception", e);
    } finally {
      FileUtil.chmod(vol.toString(), "755");
      dnp.datanode.waitAndShutdown();
      cluster.shutDownAvatarNodes();
    }
  }

  @Test
  public void testRuntimeDisallowedDatanodeShutdown() throws Exception {
    setUp("testRuntimeDisallowedDatanodeShutdown");
    LOG.info("Update include file to not include datanode");
    NameNodeInfo nni = cluster.getNameNode(0);
    writeWrongIncludeFile(nni.conf);
    LOG.info("RefreshNodes with include file without datanode's hostname.");
    nni.avatars.get(0).avatar.namesystem.refreshNodes(nni.conf);
    nni.avatars.get(1).avatar.namesystem.refreshNodes(nni.conf);
    DataNodeProperties dnProp = cluster.getDataNodeProperties().get(0);
    AvatarDataNode dn = dnProp.datanode;
    dn.waitAndShutdown();
    cluster.shutDownAvatarNodes();
  }
  
  @Test
  public void testStartupDisallowedDatanodeShutdown() throws Exception {
    setUp("testStartupDisallowedDatanodeShutdown");
    LOG.info("Update include file to not include datanode");
    NameNodeInfo nni = cluster.getNameNode(0);
    writeWrongIncludeFile(nni.conf);
    LOG.info("Refresh hostReader internally with include file");
    nni.avatars.get(0).avatar.namesystem.getHostReader().refresh();
    nni.avatars.get(1).avatar.namesystem.getHostReader().refresh();
    LOG.info("Restart the datanode.");
    try {
      cluster.restartDataNodes();
    } catch (Exception e) {
      LOG.warn("Expected exception", e);
    }
    AvatarDataNode dn = cluster.getDataNodes().get(0);
    dn.waitAndShutdown();
    cluster.shutDownAvatarNodes();
  }

  @Test
  public void testNormalShutdown() throws Exception {
    setUp("testNormalShutdown");
    LOG.info("shutdown cluster");
    cluster.shutDown();
  }
  
  @Test
  public void testNormalShutdownFederation() throws Exception {
    federation = true;
    testNormalShutdown();
  }

  @Test
  public void testVolumeFailureShutdownFederation() throws Exception { 
    federation = true;
    testVolumeFailureShutdown();
  }

  @Test
  public void testDataXeiverServerFailureShutdown() throws Exception {
    setUp("testDataXeiverServerFailureShutdown");
    LOG.info("Inject a RuntimeException to DataXeiverServer");
    InjectionHandler.set(new TestAvatarDatanodeShutdownHandler());
    final FileSystem fileSys = cluster.getFileSystem(0);
    FSDataOutputStream out = null;
    try {
      final Path fileName = new Path("/testFile");
      out = TestFileCreation.createFile(
          fileSys, fileName, cluster.getDataNodes().size());
      // trigger the failure
      TestFileCreation.writeFile(out);
    } finally {
      IOUtils.closeStream(out);
      fileSys.close();
      cluster.getDataNodes().get(0).waitAndShutdown();
      cluster.shutDownAvatarNodes();
    }
  }
  
  @Test
  public void testDataXeiverServerFailureShutdownFederation() throws Exception {
    federation = true;
    testDataXeiverServerFailureShutdown();
  }
  
  @After
  public void shutDown() throws Exception {
    if (cluster != null) {
      cluster.shutDown();
    }
    new File(hosts).delete();
    DFSTestThreadUtil.checkRemainingThreads(oldThreads);
  }

  @AfterClass
  public static void shutDownStatic() throws Exception {
    MiniAvatarCluster.shutDownZooKeeper();
  }

  private static class TestAvatarDatanodeShutdownHandler extends InjectionHandler {
    @Override 
    protected void _processEvent(InjectionEventI event, Object... args) {
      LOG.debug("processEvent: processing event: " + event);
      
      if (event == InjectionEvent.AVATARXEIVER_RUNTIME_FAILURE) {
        throw new RuntimeException("Injected failure");
      }
    }
  }

}
