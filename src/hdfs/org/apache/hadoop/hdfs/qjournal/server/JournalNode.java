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
package org.apache.hadoop.hdfs.qjournal.server;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.FastProtocolHDFS;
import org.apache.hadoop.hdfs.FastWritableHDFS;
import org.apache.hadoop.hdfs.qjournal.client.QuorumJournalManager;
import org.apache.hadoop.hdfs.qjournal.protocol.JournalConfigHelper;
import org.apache.hadoop.hdfs.qjournal.protocol.JournalConfigKeys;
import org.apache.hadoop.hdfs.server.common.StorageErrorReporter;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;

/**
 * The JournalNode is a daemon which allows namenodes using
 * the QuorumJournalManager to log and retrieve edits stored
 * remotely. It is a thin wrapper around a local edit log
 * directory with the addition of facilities to participate
 * in the quorum protocol.
 */
@InterfaceAudience.Private
public class JournalNode implements Tool, Configurable {
  
  static {
    Configuration.addDefaultResource("hdfs-default.xml");
    Configuration.addDefaultResource("hdfs-site.xml");
    Configuration.addDefaultResource("avatar-default.xml");
    Configuration.addDefaultResource("avatar-site.xml");
  }
  
  public static final Log LOG = LogFactory.getLog(JournalNode.class);
  
  private Configuration conf;
  private JournalNodeRpcServer rpcServer;
  private JournalNodeHttpServer httpServer;
  private JournalNodeJournalSyncer journalSyncer;
  private Thread journalSyncerThread;
  private Map<ByteArray, Journal> journalsById = Maps.newHashMap();
  
  // list of all journal nodes (http addresses)
  List<InetSocketAddress> journalNodes;

  private File localDir;
  
  public JournalNodeMetrics metrics;
  
  /**
   * When stopped, the daemon will exit with this code. 
   */
  private int resultCode = 0;
  
  /**
   * Get journals managed by this journal node.
   */
  synchronized Collection<Journal> getJournals() {
    Collection<Journal> journals = new ArrayList<Journal>();
    for (Journal j : journalsById.values()) {
      journals.add(j);
    }
    return journals;
  }

  public synchronized Journal getOrCreateJournal(byte[] jid) throws IOException {
    Journal journal = journalsById.get(new ByteArray(jid));
    if (journal == null) {
      String journalId = QuorumJournalManager.journalIdBytesToString(jid);
      File logDir = getJournalDir(journalId);
      File imgDir = getImageDir(journalId);
      LOG.info("Initializing journal in directory " + logDir);
      journal = new Journal(logDir, imgDir, journalId, new ErrorReporter(), this);
      journalsById.put(new ByteArray(jid), journal);
    }
    return journal;
  }
  
  public synchronized Journal getJournal(byte[] jid) throws IOException {
    return journalsById.get(new ByteArray(jid));
  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
    this.localDir = new File(
        conf.get(JournalConfigKeys.DFS_JOURNALNODE_DIR_KEY,
            JournalConfigKeys.DFS_JOURNALNODE_DIR_DEFAULT).trim());
  }

  private static void validateAndCreateJournalDir(File dir) throws IOException {
    if (!dir.isAbsolute()) {
      throw new IllegalArgumentException(
          "Journal dir '" + dir + "' should be an absolute path");
    }

    if (!dir.exists() && !dir.mkdirs()) {
      throw new IOException("Could not create journal dir '" +
          dir + "'");
    } else if (!dir.isDirectory()) {
      throw new IOException("Journal directory '" + dir + "' is not " +
          "a directory");
    }
    
    if (!dir.canWrite()) {
      throw new IOException("Unable to write to journal dir '" +
          dir + "'");
    }
  }


  @Override
  public Configuration getConf() {
    return conf;
  }

  @Override
  public int run(String[] args) throws Exception {
    start();
    return join();
  }

  /**
   * Start listening for edits via RPC.
   */
  public void start() throws IOException {
    Preconditions.checkState(!isStarted(), "JN already running");
   
    journalNodes = getJournalHttpAddresses(conf);
    // crash the JournalNode if the DFS_JOURNALNODE_HOSTS is not configured.
    if (journalNodes.isEmpty()) {
      String msg = JournalConfigKeys.DFS_JOURNALNODE_HOSTS
          + " is not present in the configuration.";
      LOG.fatal(msg);
      throw new IOException(msg);
    }
    LOG.info("JournalNode hosts: " + journalNodes);
    
    validateAndCreateJournalDir(localDir);
    LOG.info("JournalNode storage: " + localDir.getAbsolutePath());
    
    InetSocketAddress socAddr = JournalNodeRpcServer.getAddress(conf);
    // TODO serverId has to be set correctly
    metrics = new JournalNodeMetrics(conf, socAddr.toString());
    
    httpServer = new JournalNodeHttpServer(conf, this);
    httpServer.start();

    rpcServer = new JournalNodeRpcServer(conf, this);
    rpcServer.start();
    
    journalSyncer = new JournalNodeJournalSyncer(journalNodes,
        httpServer.getAddress(), conf);
    journalSyncerThread = new Thread(journalSyncer, "Thread-JournalSyncer");
    journalSyncerThread.start();
  }

  public boolean isStarted() {
    return rpcServer != null;
  }

  /**
   * @return the address the IPC server is bound to
   */
  public InetSocketAddress getBoundIpcAddress() {
    return rpcServer.getAddress();
  }
  

  public InetSocketAddress getBoundHttpAddress() {
    return httpServer.getAddress();
  }


  /**
   * Stop the daemon with the given status code
   * @param rc the status code with which to exit (non-zero
   * should indicate an error)
   */
  public void stop(int rc) {
    this.resultCode = rc;
    LOG.info("Stopping Journal Node: " + this);
    
    if (rpcServer != null) { 
      rpcServer.stop();
      rpcServer = null;
    }

    if (httpServer != null) {
      try {
        httpServer.stop();
      } catch (IOException ioe) {
        LOG.warn("Unable to stop HTTP server for " + this, ioe);
      }
    }
    
    for (Journal j : journalsById.values()) {
      IOUtils.cleanup(LOG, j);
    }
    
    if (metrics != null) {
      metrics.shutdown();
    }
    
    if (journalSyncer != null) {
      journalSyncer.stop();
    }
  }

  /**
   * Wait for the daemon to exit.
   * @return the result code (non-zero if error)
   */
  int join() throws InterruptedException {
    if (rpcServer != null) {
      rpcServer.join();
    }
    return resultCode;
  }
  
  public void stopAndJoin(int rc) throws InterruptedException {
    stop(rc);
    join();
  }

  /**
   * Return the directory inside our configured storage
   * dir which corresponds to a given journal. 
   * @param jid the journal identifier
   * @return the file, which may or may not exist yet
   */
  private File getJournalDir(String jid) {
    String dir = conf.get(JournalConfigKeys.DFS_JOURNALNODE_DIR_KEY,
        JournalConfigKeys.DFS_JOURNALNODE_DIR_DEFAULT);
    Preconditions.checkArgument(jid != null &&
        !jid.isEmpty(),
        "bad journal identifier: %s", jid);
    return new File(new File(new File(dir), "edits"), jid);
  }
 
  /**
   * Return the directory inside our configured storage
   * dir which corresponds to a given journal. 
   */
  private File getImageDir(String jid) {
    String dir = conf.get(JournalConfigKeys.DFS_JOURNALNODE_DIR_KEY,
        JournalConfigKeys.DFS_JOURNALNODE_DIR_DEFAULT);
    Preconditions.checkArgument(jid != null &&
        !jid.isEmpty(),
        "bad journal identifier: %s", jid);
    return new File(new File(new File(dir), "image"), jid);
  }

  
  private class ErrorReporter implements StorageErrorReporter {
    @Override
    public void reportErrorOnFile(File f) {
      LOG.fatal("Error reported on file " + f + "... exiting",
          new Exception());
      stop(1);
    }
  }

  public static void main(String[] args) throws Exception {
    org.apache.hadoop.hdfs.DnsMonitorSecurityManager.setTheManager();
    StringUtils.startupShutdownMessage(JournalNode.class, args, LOG);
    FastWritableHDFS.init();
    FastProtocolHDFS.init();
    System.exit(ToolRunner.run(new JournalNode(), args));
  }
  
  /**
   * Used as a wrapper class for byte[] used for storing journal identifiers.
   */
  private static final class ByteArray {
    private final byte[] data;

    public ByteArray(byte[] data) {
      this.data = data;
    }

    @Override
    public boolean equals(Object other) {
      if (!(other instanceof ByteArray)) {
        return false;
      }
      return Arrays.equals(data, ((ByteArray) other).data);
    }

    @Override
    public int hashCode() {
      return Arrays.hashCode(data);
    }
  }
  
  /**
   * Get the list of journal addresses to connect.
   * 
   * Consistent with the QuorumJournalManager.getHttpAddresses.
   */
  static List<InetSocketAddress> getJournalHttpAddresses(Configuration conf) {
    String[] hosts = JournalConfigHelper.getJournalHttpHosts(conf);
    List<InetSocketAddress> addrs = new ArrayList<InetSocketAddress>();
    for (String host : hosts) {
      addrs.add(NetUtils.createSocketAddr(host));
    }
    return addrs;
  }
  
  /**
   * Add a new sync task. This is invoked when we start a new segment, we want
   * to make sure that we have all older segments that we worry about.
   */
  public void addSyncTask(Journal journal, long createTxId) {
    journalSyncer.addSyncTask(journal, createTxId);
  }
}
