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
package org.apache.hadoop.raid;
import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.FSConstants;
import org.apache.hadoop.hdfs.server.namenode.JspHelper;
import org.apache.hadoop.hdfs.util.InjectionEvent;
import org.apache.hadoop.util.InjectionEventI;
import org.apache.hadoop.util.InjectionHandler;

import static org.junit.Assert.*;

/* A very simple test to verify namenode UI behaves well when raidnode UI goes 
 * wrong. Namenode web ui will call raidnode webui, which is then processed in
 * CorruptFileCounterServlet. 
 * InjectionHandler.processEventIO(InjectionEvent.RAID_HTTPSERVER_TIMEOUT) is 
 * called and triggers the TestRaidHTTPInjectionHandler code. 
 * TestRaidHTTPInjectionHandler maintains a counter, the first call will sleep 
 * 10 seconds, the second call will throw an exception, 
 * the third call will do nothing and return successfully. 
 */
public class TestRaidUI {
  final static Log LOG = LogFactory.getLog(TestRaidUI.class);
  final static String TEST_DIR = new File(System.getProperty("test.build.data",
      "build/contrib/raid/test/data")).getAbsolutePath();
  final static String CONFIG_FILE = new File(TEST_DIR, 
      "test-raid.xml").getAbsolutePath();
  Configuration conf;
  String namenode = null;
  MiniDFSCluster dfsCluster = null;
  String hftp = null;
  FileSystem fileSys = null;
  RaidNode cnode = null;
  
  class TestRaidHTTPInjectionHandler extends InjectionHandler {
    public int counter = 0;
    @Override
    public void _processEventIO(InjectionEventI event, Object... args)
      throws IOException {
      if (event == InjectionEvent.RAID_HTTPSERVER_TIMEOUT) {
        counter++;
        if (counter == 1) {
          DFSTestUtil.waitNSecond(10);
        } else if (counter == 2) {
          throw new IOException("Error happens!");
        }
      }
    }
  }

  @Test
  public void testRaidUI() throws Exception {
    Configuration localConf = new Configuration(conf);
    cnode = RaidNode.createRaidNode(null, localConf);
    InetSocketAddress infoSocAddr = dfsCluster.getNameNode().getHttpAddress();
    InjectionHandler h = new TestRaidHTTPInjectionHandler();
    InjectionHandler.set(h);
    
    LOG.info("First call will fail with timeout because RaidNode UI will " +  
        "hang for 10 seconds. Check TestRaidHTTPInjectionHandler when "  +
        "counter == 1");
    long stime = System.currentTimeMillis();
    String httpContent = DFSUtil.getHTMLContent(
        new URI("http", null, infoSocAddr.getHostName(), 
            infoSocAddr.getPort(), "/dfshealth.jsp", null, null));
    LOG.info("Output1: " + httpContent);
    long duration = System.currentTimeMillis() - stime;
    long expectTimeout = JspHelper.RAID_UI_CONNECT_TIMEOUT +
        JspHelper.RAID_UI_READ_TIMEOUT;
    assertTrue("Should take less than " + expectTimeout + "ms actual time: " +
        duration, duration < expectTimeout + 1000);
    assertTrue("Should get timeout error", 
        httpContent.contains("Raidnode didn't response"));
    assertFalse("Shouldn't get right result", 
        httpContent.contains("WARNING Corrupt files"));
    
    LOG.info("Second call will fail with error because RaidNode UI throw " + 
        "an IOException. Check TestRaidHTTPInjectionHandler when counter == 2");
    httpContent = DFSUtil.getHTMLContent(
        new URI("http", null, infoSocAddr.getHostName(), 
            infoSocAddr.getPort(), "/dfshealth.jsp", null, null));
    LOG.info("Output2: " + httpContent);
    assertTrue("Should get error", 
        httpContent.contains("Raidnode is unreachable"));
    assertFalse("Shouldn't get right result", 
        httpContent.contains("WARNING Corrupt files"));
    
    LOG.info("Third call will succeed");
    httpContent = DFSUtil.getHTMLContent(
        new URI("http", null, infoSocAddr.getHostName(), 
            infoSocAddr.getPort(), "/dfshealth.jsp", null, null));
    LOG.info("Output3: " + httpContent);
    assertTrue("Should get right result", 
        httpContent.contains("WARNING Corrupt files"));
  }

  @Before
  public void setUp() throws IOException {
    if (System.getProperty("hadoop.log.dir") == null) {
      String base = new File(".").getAbsolutePath();
      System.setProperty("hadoop.log.dir", new Path(base).toString() + "/logs");
    }
    new File(TEST_DIR).mkdirs(); // Make sure data directory exists
    conf = new Configuration();
    conf.set("raid.config.file", CONFIG_FILE);
    // do not use map-reduce cluster for Raiding
    conf.set("raid.classname", "org.apache.hadoop.raid.LocalRaidNode");
    conf.set("raid.server.address", "localhost:" + MiniDFSCluster.getFreePort());
    String raidHttpAddress = "localhost:" + MiniDFSCluster.getFreePort();
    conf.set("mapred.raid.http.address", raidHttpAddress);
    conf.set(FSConstants.DFS_RAIDNODE_HTTP_ADDRESS_KEY, raidHttpAddress);
    Utils.loadTestCodecs(conf, 3, 1, 3, "/destraid", "/destraidrs");
    dfsCluster = new MiniDFSCluster(conf, 1, true, null);
    dfsCluster.waitActive();
    fileSys = dfsCluster.getFileSystem();
    namenode = fileSys.getUri().toString();

    FileSystem.setDefaultUri(conf, namenode);
    ConfigBuilder cb = new ConfigBuilder(CONFIG_FILE);
    cb.persist(); 
  }

  @After
  public void tearDown() throws IOException {
    if (cnode != null) { cnode.stop(); cnode.join(); }
    if (dfsCluster != null) {dfsCluster.shutdown();}
    InjectionHandler.clear();
  }
}
