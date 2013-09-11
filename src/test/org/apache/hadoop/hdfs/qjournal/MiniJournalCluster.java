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
package org.apache.hadoop.hdfs.qjournal;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import java.util.concurrent.ExecutorService;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.qjournal.client.AsyncLogger;
import org.apache.hadoop.hdfs.qjournal.client.IPCLoggerChannel;
import org.apache.hadoop.hdfs.qjournal.client.QuorumJournalManager;
import org.apache.hadoop.hdfs.qjournal.protocol.JournalConfigKeys;
import org.apache.hadoop.hdfs.qjournal.server.JournalNode;
import org.apache.hadoop.hdfs.server.protocol.NamespaceInfo;
import org.mockito.Mockito;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.MoreExecutors;

public class MiniJournalCluster {
  
  public static String DFS_JOURNALNODE_TEST_ID = "qjm.journalnode.id";
  
  private static final String hostname = "localhost:";
  
  public static int getFreeHttpPortAndUpdateConf(Configuration conf,
      boolean updateHosts) {
    // setup http port
    int httpPort = MiniDFSCluster.getFreePort();
    conf.set(JournalConfigKeys.DFS_JOURNALNODE_HTTP_ADDRESS_KEY, "0.0.0.0:"
        + httpPort);
    if (updateHosts) {
      conf.set(JournalConfigKeys.DFS_JOURNALNODE_HOSTS, hostname + httpPort);
    }
    return httpPort;
  }
  
  public static class Builder {
    private String baseDir;
    private int numJournalNodes = 3;
    private boolean format = true;
    private Configuration conf;
    
    public Builder(Configuration conf) {
      this.conf = conf;
    }
    
    public Builder baseDir(String d) {
      this.baseDir = d;
      return this;
    }
    
    public Builder numJournalNodes(int n) {
      this.numJournalNodes = n;
      return this;
    }

    public Builder format(boolean f) {
      this.format = f;
      return this;
    }

    public MiniJournalCluster build() throws IOException {
      return new MiniJournalCluster(this);
    }
  }

  private static final Log LOG = LogFactory.getLog(MiniJournalCluster.class);
  private File baseDir;
  private JournalNode nodes[];
  private InetSocketAddress ipcAddrs[];
  private InetSocketAddress httpAddrs[];
  private int httpPorts[];
  
  private MiniJournalCluster(Builder b) throws IOException {
    
    if (b.baseDir != null) {
      this.baseDir = new File(b.baseDir);
    } else {
      this.baseDir = new File(MiniDFSCluster.getBaseDirectory(new Configuration()),"");
    }
    
    LOG.info("Starting MiniJournalCluster with " +
        b.numJournalNodes + " journal nodes, base_dir: " + baseDir.toString());
    
    nodes = new JournalNode[b.numJournalNodes];
    ipcAddrs = new InetSocketAddress[b.numJournalNodes];
    httpAddrs = new InetSocketAddress[b.numJournalNodes];
    httpPorts = new int[b.numJournalNodes];
    
    // setup hosts key
    String hosts = "";
    for (int i = 0; i < b.numJournalNodes; i++) {
      httpPorts[i] = MiniDFSCluster.getFreePort();
      hosts += hostname + httpPorts[i] + ",";
    }
    hosts = hosts.substring(0, hosts.length() - 1);
    b.conf.set(JournalConfigKeys.DFS_JOURNALNODE_HOSTS, hosts);
    // In test, set buf size to be 2, instead of 20
    b.conf.setInt(JournalConfigKeys.DFS_QJOURNAL_IMAGE_MAX_BUFFERED_CHUNKS_KEY, 2);
    
    for (int i = 0; i < b.numJournalNodes; i++) {
      if (b.format) {
        File dir = getStorageDir(i);
        LOG.debug("Fully deleting JN directory " + dir);
        FileUtil.fullyDelete(dir);
      }
      nodes[i] = new JournalNode();
      nodes[i].setConf(createConfForNode(b, i, httpPorts[i]));
      nodes[i].start();

      ipcAddrs[i] = nodes[i].getBoundIpcAddress();
      httpAddrs[i] = nodes[i].getBoundHttpAddress();
    }
  }

  /**
   * Set up the given Configuration object to point to the set of JournalNodes 
   * in this cluster.
   */
  public URI getQuorumJournalURI(String jid) {
    List<String> addrs = Lists.newArrayList();
    for (InetSocketAddress addr : ipcAddrs) {
      addrs.add("127.0.0.1:" + addr.getPort());
    }
    String addrsVal = Joiner.on(";").join(addrs);
    LOG.debug("Setting logger addresses to: " + addrsVal);
    try {
      return new URI("qjm://" + addrsVal + "/" + jid);
    } catch (URISyntaxException e) {
      throw new AssertionError(e);
    }
  }
  
  /**
   * Get list of the http addresses.
   */
  public List<String> getHttpJournalAddresses() {
    List<String> addrs = Lists.newArrayList();
    for (InetSocketAddress addr : httpAddrs) {
      addrs.add("http://127.0.0.1:" + addr.getPort());
    }
    return addrs;
  }

  /**
   * Start the JournalNodes in the cluster.
   */
  public void start() throws IOException {
    for (JournalNode jn : nodes) {
      jn.start();
    }
  }

  /**
   * Shutdown all of the JournalNodes in the cluster.
   * @throws IOException if one or more nodes failed to stop
   */
  public void shutdown() throws IOException {
    boolean failed = false;
    for (JournalNode jn : nodes) {
      try {
        jn.stopAndJoin(0);
      } catch (Exception e) {
        failed = true;
        LOG.warn("Unable to stop journal node " + jn, e);
      }
    }
    if (failed) {
      throw new IOException("Unable to shut down. Check log for details");
    }
  }

  private Configuration createConfForNode(Builder b, int idx, int httpPort) {
    Configuration conf = new Configuration(b.conf);
    File logDir = getStorageDir(idx);
    conf.setLong("rpc.polling.interval", 100);
    conf.set(JournalConfigKeys.DFS_JOURNALNODE_DIR_KEY, logDir.toString());
    conf.set(JournalConfigKeys.DFS_JOURNALNODE_RPC_ADDRESS_KEY, "0.0.0.0:0");
    conf.set(JournalConfigKeys.DFS_JOURNALNODE_HTTP_ADDRESS_KEY, "0.0.0.0:" + httpPort);
    conf.setInt(DFS_JOURNALNODE_TEST_ID, idx);
    return conf;
  }

  public File getStorageDir(int idx) {
    return new File(baseDir, "journalnode-" + idx).getAbsoluteFile();
  }
  
  public File getJournalCurrentDir(int idx, String jid) {
    return new File(new File(new File(getStorageDir(idx), "edits"), jid),
        "current");
  }

  public JournalNode getJournalNode(int i) {
    return nodes[i];
  }
  
  public int getHttpPort(int i) {
    return httpPorts[i];
  }

  public JournalNode[] getJournalNodes() {
    return nodes;
  }
  
  public void restartJournalNode(int i) throws InterruptedException, IOException {
    Configuration conf = new Configuration(nodes[i].getConf());
    if (nodes[i].isStarted()) {
      nodes[i].stopAndJoin(0);
    }
    
    conf.set(JournalConfigKeys.DFS_JOURNALNODE_RPC_ADDRESS_KEY, "127.0.0.1:" +
        ipcAddrs[i].getPort());
    conf.set(JournalConfigKeys.DFS_JOURNALNODE_HTTP_ADDRESS_KEY, "127.0.0.1:" +
        httpAddrs[i].getPort());
    
    JournalNode jn = new JournalNode();
    jn.setConf(conf);
    jn.start();
  }

  public int getQuorumSize() {
    return nodes.length / 2 + 1;
  }

  public int getNumNodes() {
    return nodes.length;
  }
  
  public static QuorumJournalManager createSpyingQJM(Configuration conf,
      MiniJournalCluster cluster) throws IOException, URISyntaxException {
    AsyncLogger.Factory spyFactory = new AsyncLogger.Factory() {
      @Override
      public AsyncLogger createLogger(Configuration conf, NamespaceInfo nsInfo,
          String journalId, InetSocketAddress addr) {
        AsyncLogger logger = new IPCLoggerChannel(conf, nsInfo, journalId, addr) {
          protected ExecutorService createExecutor() {
            // Don't parallelize calls to the quorum in the tests.
            // This makes the tests more deterministic.
            return MoreExecutors.sameThreadExecutor();
          }
        };
        return Mockito.spy(logger);
      }
    };
    return new QuorumJournalManager(conf, cluster.getQuorumJournalURI(QJMTestUtil.JID),
        QJMTestUtil.FAKE_NSINFO, spyFactory, null, false);
  }

  /**
   * Return the directory inside configured storage
   * dir which corresponds to a given journal. 
   * Edits storage.
   */
  public static File getJournalDir(JournalNode jn, String jid) {
    String dir = jn.getConf().get(JournalConfigKeys.DFS_JOURNALNODE_DIR_KEY,
        JournalConfigKeys.DFS_JOURNALNODE_DIR_DEFAULT);
    Preconditions.checkArgument(jid != null &&
        !jid.isEmpty(),
        "bad journal identifier: %s", jid);
    return new File(new File(new File(dir), "edits"), jid);
  }
 
  /**
   * Return the directory inside our configured storage
   * dir which corresponds to a given journal. 
   * Image storage.
   */
  public static File getImageDir(JournalNode jn, String jid) {
    String dir = jn.getConf().get(JournalConfigKeys.DFS_JOURNALNODE_DIR_KEY,
        JournalConfigKeys.DFS_JOURNALNODE_DIR_DEFAULT);
    Preconditions.checkArgument(jid != null &&
        !jid.isEmpty(),
        "bad journal identifier: %s", jid);
    return new File(new File(new File(dir), "image"), jid);
  }
}
