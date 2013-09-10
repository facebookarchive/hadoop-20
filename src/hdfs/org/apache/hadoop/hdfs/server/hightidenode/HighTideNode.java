 /*
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
package org.apache.hadoop.hdfs.server.hightidenode;

import java.io.IOException;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.LinkedList;
import java.util.Iterator;
import java.util.Arrays;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.HashSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.Date;
import java.text.SimpleDateFormat;
import java.lang.Thread;
import java.lang.InterruptedException;
import java.net.InetSocketAddress;
import java.net.URI;

import org.xml.sax.SAXException;
import javax.xml.parsers.ParserConfigurationException;

import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.ipc.*;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Daemon;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.fs.HarFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;

import org.apache.hadoop.hdfs.protocol.HighTideProtocol;
import org.apache.hadoop.hdfs.protocol.PolicyInfo;
import org.apache.hadoop.hdfs.protocol.PolicyInfo.PathInfo;
import org.apache.hadoop.hdfs.server.hightidenode.metrics.HighTideNodeMetrics;

/**
 * A {@link HighTideNode} that implements 
 */
public class HighTideNode implements HighTideProtocol {

  static{
    Configuration.addDefaultResource("hdfs-default.xml");
    Configuration.addDefaultResource("hdfs-site.xml");
  }
  public static final Log LOG = LogFactory.getLog( "org.apache.hadoop.hightidenode.HighTideNode");
  public static final long SLEEP_TIME = 10000L; // 10 seconds
  public static final int DEFAULT_PORT = 60100;
  public static final String HIGHTIDE_FULLSYNC_INTERVAL = "hightide.fullsync.interval.seconds";
  public static final long HIGHTIDE_FULLSYNC_INTERVAL_DEFAULT = 60 * 60; // 1 hour

  public static final SimpleDateFormat dateForm = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

  /** RPC server */
  private Server server;
  /** RPC server address */
  private InetSocketAddress serverAddress = null;
  /** only used for testing purposes  */
  private boolean stopRequested = false;

  /** Configuration Manager */
  private ConfigManager configMgr;

  /** hadoop configuration */
  private Configuration conf;

  protected boolean initialized;  // Are we initialized?
  protected volatile boolean running; // Are we running?

  /** Deamon thread to find missing blocks */
  Daemon triggerThread = null;

  /** Daemon thread to fix corrupt files */
  public FileFixer fileFixer = null;
  Daemon fileFixerThread = null;

  static HighTideNodeMetrics myMetrics;

  // statistics about replicas fixed
  public static class Statistics {
    long numProcessedBlocks; // total blocks encountered in namespace
    long processedSize;   // disk space occupied by all blocks

    public void clear() {
      numProcessedBlocks = 0;
      processedSize = 0;
    }
    public String toString() {
      String msg = " numProcessedBlocks = " + numProcessedBlocks +
                   " processedSize = " + processedSize;
      return msg;
    }
  }

  // Startup options
  static public enum StartupOption{
    TEST ("-test"),
    REGULAR ("-regular");

    private String name = null;
    private StartupOption(String arg) {this.name = arg;}
    public String getName() {return name;}
  }
  
  /**
   * Start HighTideNode.
   * <p>
   * The hightidenode-node can be started with one of the following startup options:
   * <ul> 
   * <li>{@link StartupOption#REGULAR REGULAR} - normal hightidenode node startup</li>
   * </ul>
   * The option is passed via configuration field: 
   * <tt>fs.hightidenodenode.startup</tt>
   * 
   * The conf will be modified to reflect the actual ports on which 
   * the HighTideNode is up and running if the user passes the port as
   * <code>zero</code> in the conf.
   * 
   * @param conf  confirguration
   * @throws IOException
   */

  HighTideNode(Configuration conf) throws IOException {
    try {
      initialize(conf);
    } catch (IOException e) {
      LOG.error(StringUtils.stringifyException(e));
      this.stop();
      throw e;
    } catch (Exception e) {
      LOG.error(StringUtils.stringifyException(e));
      this.stop();
      throw new IOException(e);
    }
  }

  public long getProtocolVersion(String protocol,
                                 long clientVersion) throws IOException {
    if (protocol.equals(HighTideProtocol.class.getName())) {
      return HighTideProtocol.versionID;
    } else {
      throw new IOException("Unknown protocol to hightide node: " + protocol);
    }
  }

  public ProtocolSignature getProtocolSignature(String protocol,
      long clientVersion, int clientMethodsHash) throws IOException {
    return ProtocolSignature.getProtocolSignature(
        this, protocol, clientVersion, clientMethodsHash);
  }

  /**
   * Wait for service to finish.
   * (Normally, it runs forever.)
   */
  public void join() {
    try {
      if (server != null) server.join();
      if (triggerThread != null) triggerThread.join();
      if (fileFixerThread != null) fileFixerThread.join();
    } catch (InterruptedException ie) {
      // do nothing
    }
  }
  
  /**
   * Stop all HighTideNode threads and wait for all to finish.
   */
  public void stop() {
    if (stopRequested) {
      return;
    }
    stopRequested = true;
    running = false;
    if (server != null) server.stop();
    if (triggerThread != null) triggerThread.interrupt();
    if (fileFixer != null) fileFixer.shutdown();
    if (fileFixerThread != null) fileFixerThread.interrupt();
    if (myMetrics != null) {
      myMetrics.shutdown();
    }
  }

  private static InetSocketAddress getAddress(String address) {
    return NetUtils.createSocketAddr(address);
  }

  public static InetSocketAddress getAddress(Configuration conf) {
    String nodeport = conf.get("hightidenode.server.address");
    if (nodeport == null) {
      nodeport = "localhost:" + DEFAULT_PORT;
    }
    return getAddress(nodeport);
  }

  public InetSocketAddress getListenerAddress() {
    return server.getListenerAddress();
  }
  
  private void initialize(Configuration conf) 
    throws IOException, SAXException, InterruptedException, 
           HighTideConfigurationException,
           ClassNotFoundException, ParserConfigurationException {
    this.conf = conf;
    InetSocketAddress socAddr = HighTideNode.getAddress(conf);
    int handlerCount = conf.getInt("fs.hightidenodenode.handler.count", 10);

    // read in the configuration
    configMgr = new ConfigManager(conf);
    configMgr.reloadConfigsIfNecessary();
    configMgr.startReload();

    // create Metrics object
    myMetrics = new HighTideNodeMetrics(conf, this);

    // create rpc server
    this.server = RPC.getServer(this, socAddr.getAddress().getHostAddress(), socAddr.getPort(),
        handlerCount, false, conf);

    // The rpc-server port can be ephemeral... ensure we have the correct info
    this.serverAddress = this.server.getListenerAddress();
    LOG.info("HighTideNode up at: " + this.serverAddress);

    initialized = true;
    running = true;
    this.server.start(); // start RPC server


    this.fileFixer = new FileFixer(conf);
    this.fileFixerThread = new Daemon(this.fileFixer);
    fileFixer.setPolicyInfo(configMgr.getAllPolicies());
    this.fileFixerThread.start();

   // start the deamon thread to resync if needed
   this.triggerThread = new Daemon(new TriggerMonitor());
   this.triggerThread.start();
  }

  /**
   * Sync up on hightidenode restart
   */
  class TriggerMonitor implements Runnable {

    private Map<String, Long> scanTimes = new HashMap<String, Long>();
    private Map<String, DirectoryTraversal> scanState =
      new HashMap<String, DirectoryTraversal>();

    public void run() {
      while (running) {
        try {
          doFullSync();
        } catch (IOException e) {
          LOG.info("Exception in doFullSync. " + StringUtils.stringifyException(e));
        }

        long sleepTime = conf.getLong(HIGHTIDE_FULLSYNC_INTERVAL,  
                                    HIGHTIDE_FULLSYNC_INTERVAL_DEFAULT) * 1000;
        try {
          Thread.sleep(sleepTime);
        } catch (InterruptedException e) {
          LOG.info("InterrupedException in TriggerMonitor.run.");
        }
      }
    }
  }

  /**
   * Full sync of all policies 
   */
  void doFullSync() throws IOException {
    for (PolicyInfo pinfo:configMgr.getAllPolicies()) {
      doFullSync(pinfo);
    }
  }

  /**
   * Full sync of specified policy
   */
  void doFullSync(PolicyInfo pinfo) throws IOException {
    Path srcPath = pinfo.getSrcPath();
    LOG.info("Starting fullsync of srcPath " + srcPath);

    FileSystem srcFs = srcPath.getFileSystem(pinfo.getConf());
    int srcRepl = Integer.parseInt(pinfo.getProperty("replication"));
    long modTimePeriod = Long.parseLong(pinfo.getProperty("modTimePeriod"));
    long now = HighTideNode.now();
    
    // traverse all files inside the subtree rooted at srcPath
    List<FileStatus> slist = new ArrayList<FileStatus>();
    slist.add(srcFs.getFileStatus(srcPath));
    DirectoryTraversal traverse = new DirectoryTraversal(srcFs, slist);

    while (true) {
      FileStatus sstat = traverse.getNextFile();
      if (sstat == null) {
        break;             // done checking all files
      }

      // we always follow the order of first changing the replication
      // on the destination files before we update the repl factor of
      // the source file. So, it is safe to make the following check
      // and avoid checking the destination files if the source file
      // already is at the specified replication factor.
      if (sstat.getReplication() == srcRepl) {
        continue;
      }
      // if the file has been updated recently, then do not
      // do anything to it
      if (sstat.getModificationTime() +  modTimePeriod > now) {
        continue;
      }

      // find the suffix in the srcPath that is mapped to the destination
      srcPath = sstat.getPath();
      String[] splits = srcPath.toString().split(pinfo.getSrcPath().toString());
      String suffix = splits[1];

      // match each pair of src and destination paths
      boolean match = true;
      for (PathInfo destPathInfo: pinfo.getDestPaths()) {
        Path destPath = new Path(destPathInfo.getPath().toString() + suffix);
        LOG.debug("Comparing " + srcPath + " with " + destPath);

        int destRepl = Integer.parseInt(destPathInfo.getProperty("replication"));
        FileSystem destFs = destPath.getFileSystem(pinfo.getConf());

        FileStatus dstat = null;

        try {
          dstat = destFs.getFileStatus(destPath);
        } catch (java.io.FileNotFoundException e ) {
          match = false;
          continue;                // ok if the destination does not exist
        } catch (IOException e) {
          match = false;
          LOG.info("Unable to locate matching file in destination " +
                   destPath + StringUtils.stringifyException(e) +
                   ". Ignoring...");
        }
        LOG.info("Matching " + srcPath + " with " + destPath);
        HighTideNode.getMetrics().filesMatched.inc();

        if (dstat.getModificationTime() == sstat.getModificationTime() &&
            dstat.getBlockSize() == sstat.getBlockSize() &&
            dstat.getLen() == sstat.getLen()) {

          // first reduce the intended replication on the destination path
          if (dstat.getReplication() > destRepl) {
            HighTideNode.getMetrics().filesChanged.inc();
            long saved = dstat.getLen() * (dstat.getReplication() - destRepl);
            LOG.info("Changing replication of dest " + destPath + 
                     " from " + dstat.getReplication() + " to " + destRepl);
            destFs.setReplication(dstat.getPath(), (short)destRepl);

            saved += HighTideNode.getMetrics().savedSize.get();
            HighTideNode.getMetrics().savedSize.set(saved);
          }
        } else {
          // one destination path does not match the source
          match = false;
          break;
        }
      }
      // if the all the destination paths matched the source, then
      // reduce repl factor of the source
      if (match && sstat.getReplication() > srcRepl) {
        LOG.info("Changing replication of source " + srcPath + 
                 " from " + sstat.getReplication() + " to " + srcRepl);
        HighTideNode.getMetrics().filesChanged.inc();
        long saved = sstat.getLen() * (sstat.getReplication() - srcRepl);
        srcFs.setReplication(sstat.getPath(), (short)srcRepl);

        saved += HighTideNode.getMetrics().savedSize.get();
        HighTideNode.getMetrics().savedSize.set(saved);
      }
    }
    LOG.info("Completed fullsync of srcPath " + srcPath);
  }

  /**
   * Shuts down the HighTideNode
   */
  void shutdown() throws IOException, InterruptedException {
    configMgr.stopReload();    // stop config reloads
    fileFixer.shutdown();     // stop block fixer
    fileFixerThread.interrupt();
    server.stop();             // stop http server
  }

  
  /**
   * Implement HighTideProtocol methods
   */

  /** {@inheritDoc} */
  public PolicyInfo[] getAllPolicies() throws IOException {
    Collection<PolicyInfo> list = configMgr.getAllPolicies();
    return list.toArray(new PolicyInfo[list.size()]);
  }

  /**
   * returns my Metrics object
   */
  public static HighTideNodeMetrics getMetrics() {
    return myMetrics;
  }

  /**
   * Returns current time.
   */
  static long now() {
    return System.currentTimeMillis();
  }

  private static void printUsage() {
    System.err.println("Usage: java HighTideNode ");
  }

  private static StartupOption parseArguments(String args[]) {
    int argsLen = (args == null) ? 0 : args.length;
    StartupOption startOpt = StartupOption.REGULAR;
    for(int i=0; i < argsLen; i++) {
      String cmd = args[i]; // We have to parse command line args in future.
    }
    return startOpt;
  }

  /**
   * Convert command line options to configuration parameters
   */
  private static void setStartupOption(Configuration conf, StartupOption opt) {
    conf.set("fs.hightidenodenode.startup", opt.toString());
  }

  /**
   * Create an instance of the HighTideNode 
   */
  public static HighTideNode createHighTideNode(String argv[],
                                        Configuration conf) throws IOException {
    if (conf == null) {
      conf = new Configuration();
    }
    StartupOption startOpt = parseArguments(argv);
    if (startOpt == null) {
      printUsage();
      return null;
    }
    setStartupOption(conf, startOpt);
    HighTideNode node = new HighTideNode(conf);
    return node;
  }

  /**
   */
  public static void main(String argv[]) throws Exception {
    org.apache.hadoop.hdfs.DnsMonitorSecurityManager.setTheManager();
    try {
      StringUtils.startupShutdownMessage(HighTideNode.class, argv, LOG);
      HighTideNode hightidenode = createHighTideNode(argv, null);
      if (hightidenode != null) {
        hightidenode.join();
      }
    } catch (Throwable e) {
      LOG.error(StringUtils.stringifyException(e));
      System.exit(-1);
    }
  }
}
