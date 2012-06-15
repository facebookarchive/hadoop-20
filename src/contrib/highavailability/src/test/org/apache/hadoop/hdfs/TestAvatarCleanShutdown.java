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
import java.util.Iterator;
import org.apache.hadoop.fs.FileUtil;

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

import org.apache.hadoop.hdfs.server.datanode.AvatarDataNode;
import org.apache.hadoop.hdfs.MiniAvatarCluster;
import org.apache.hadoop.hdfs.MiniAvatarCluster.DataNodeProperties;
import org.apache.hadoop.hdfs.MiniAvatarCluster.NameNodeInfo;

public class TestAvatarCleanShutdown {
  final static Log LOG = LogFactory.getLog(TestAvatarCleanShutdown.class);

  private static final String HOST_FILE_PATH = "/tmp/include_file";

  private Configuration conf;
  private MiniAvatarCluster cluster;
  private boolean federation;
  private Set<Thread> oldThreads;

  // A list of threads that might hang around (maybe due to a JVM bug).
  private static final String[] excludedThreads = { "SunPKCS11" };

  @BeforeClass
  public static void setUpStatic() throws Exception {
    MiniAvatarCluster.createAndStartZooKeeper();
  }

  public void setUp() throws Exception {
    oldThreads = new HashSet<Thread>(Thread.getAllStackTraces().keySet());

    conf = new Configuration();
    File f = new File(HOST_FILE_PATH);
    if (f.exists()) {
      f.delete();
    }
    conf.set("dfs.hosts", HOST_FILE_PATH);
    conf.setInt("dfs.datanode.failed.volumes.tolerated", 0);
    if (!federation) {
      cluster = new MiniAvatarCluster(conf, 1, true, null, null);
    } else {
      cluster = new MiniAvatarCluster(conf, 1, true, null, null, 2, true);
    }
    federation = false;
  }
  
  private boolean isExcludedThread(Thread th) {
    for (String badThread : excludedThreads) {
      if (th.getName().contains(badThread)) {
        return true;
      }
    }
    return false;
  }

  private void checkRemainingThreads(Set<Thread> old) throws Exception {
    Thread.sleep(5000);
    Set<Thread> threads = Thread.getAllStackTraces().keySet();
    threads.removeAll(old);
    if (threads.size() != 0) {
      LOG.error("Following threads are not clean up:");
      Iterator<Thread> it = threads.iterator();
      while (it.hasNext()) {
        Thread th = it.next();
        if (isExcludedThread(th)) {
          it.remove();
          continue;
        }
        LOG.error("Thread: " + th.getName());
      }
    }
    assertTrue("This is not a clean shutdown", threads.size() == 0);
  }
  
  private void writeWrongIncludeFile(Configuration conf) throws Exception {
    String includeFN = conf.get("dfs.hosts", "");
    assertTrue(includeFN.equals(HOST_FILE_PATH));
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
    setUp();
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
    setUp();
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
    setUp();
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
    setUp();
    LOG.info("shutdown cluster");
    cluster.shutDown();
  }
  
  @Test
  public void testNormalShutdownFederation() throws Exception {
    federation = true;
    testNormalShutdown();
  }
  
  @Test
  public void testRuntimeDisallowedDatanodeShutdownFederation() throws Exception {
    federation = true;
    testRuntimeDisallowedDatanodeShutdown();
  }

  @Test
  public void testStartupDisallowedDatanodeShutdownFederation() throws Exception {
    federation = true;
    testStartupDisallowedDatanodeShutdown();
  }
  
  @Test
  public void testVolumeFailureShutdownFederation() throws Exception { 
    federation = true;
    testVolumeFailureShutdown();
  }

  @After
  public void shutDown() throws Exception {
    checkRemainingThreads(oldThreads);
  }

  @AfterClass
  public static void shutDownStatic() throws Exception {
    MiniAvatarCluster.shutDownZooKeeper();
  }

}
