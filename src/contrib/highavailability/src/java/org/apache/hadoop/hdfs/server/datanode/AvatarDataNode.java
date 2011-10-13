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
package org.apache.hadoop.hdfs.server.datanode;

import java.io.IOException;
import java.io.File;
import java.io.FileOutputStream;
import java.io.DataOutputStream;
import java.net.InetSocketAddress;
import java.net.SocketTimeoutException;
import java.net.ServerSocket;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.net.ConnectException;
import java.util.Date;
import java.util.Iterator;
import java.util.Collection;
import java.util.AbstractList;
import java.util.ArrayList;
import java.text.SimpleDateFormat;
import java.lang.reflect.Method;

import org.apache.hadoop.ipc.*;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.net.DNS;
import org.apache.hadoop.hdfs.protocol.FSConstants;
import org.apache.hadoop.util.Daemon;
import org.apache.hadoop.util.DiskChecker;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.server.protocol.DatanodeProtocol;
import org.apache.hadoop.hdfs.server.protocol.DatanodeRegistration;
import org.apache.hadoop.hdfs.server.protocol.NamespaceInfo;
import org.apache.hadoop.hdfs.server.protocol.DisallowedDatanodeException;
import org.apache.hadoop.hdfs.server.common.IncorrectVersionException;
import org.apache.hadoop.hdfs.protocol.UnregisteredDatanodeException;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.server.namenode.FileChecksumServlets;
import org.apache.hadoop.hdfs.server.namenode.StreamFile;
import org.apache.hadoop.hdfs.server.common.Storage;
import org.apache.hadoop.hdfs.server.datanode.metrics.DataNodeMetrics;
import org.apache.hadoop.util.DiskChecker.DiskErrorException;
import org.apache.hadoop.hdfs.server.common.HdfsConstants.StartupOption;
import org.apache.hadoop.hdfs.server.common.HdfsConstants;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.authorize.ConfiguredPolicy;
import org.apache.hadoop.security.authorize.PolicyProvider;
import org.apache.hadoop.security.authorize.ServiceAuthorizationManager;
import org.apache.hadoop.hdfs.AvatarZooKeeperClient;
import org.apache.hadoop.hdfs.HDFSPolicyProvider;
import org.apache.hadoop.http.HttpServer;

import org.apache.hadoop.hdfs.protocol.AvatarProtocol;
import org.apache.hadoop.hdfs.server.namenode.AvatarNode;
import org.apache.zookeeper.data.Stat;

/**
 * This is an implementation of the AvatarDataNode, a wrapper
 * for a regular datanode that works with AvatarNode.
 * 
 * The AvatarDataNode is needed to make a vanilla DataNode send
 * block reports to Primary and standby namenodes. The AvatarDataNode
 * does not know which one of the namenodes is primary and which is
 * secondary.
 *
 * Typically, an adminstrator will have to specify the pair of
 * AvatarNodes via fs1.default.name and fs2.default.name
 *
 */

public class AvatarDataNode extends DataNode {

  static {
    Configuration.addDefaultResource("avatar-default.xml");
    Configuration.addDefaultResource("avatar-site.xml");
  }
  public static final Log LOG = LogFactory.getLog(AvatarDataNode.class.getName());

  // public DatanodeProtocol namenode; // override in DataNode class
  InetSocketAddress nameAddr1;
  InetSocketAddress nameAddr2;
  DatanodeProtocol namenode1;
  DatanodeProtocol namenode2;
  AvatarProtocol avatarnode1;
  AvatarProtocol avatarnode2;
  InetSocketAddress avatarAddr1;
  InetSocketAddress avatarAddr2;
  boolean doneRegister1 = false;    // not yet registered with namenode1
  boolean doneRegister2 = false;    // not yet registered with namenode2
  OfferService offerService1;
  OfferService offerService2;
  volatile OfferService primaryOfferService = null;
  volatile boolean shutdown = false;
  Thread of1;
  Thread of2;
  private DataStorage storage;
  public final String dnThreadName;
  private HttpServer infoServer;
  private Thread dataNodeThread;
  Method transferBlockMethod;
  AvatarZooKeeperClient zkClient = null;

  public AvatarDataNode(Configuration conf, AbstractList<File> dataDirs, 
                        String dnThreadName) throws IOException {
    super(conf, dataDirs);

    this.dnThreadName = dnThreadName;

    // access a private member of the base DataNode class
    try { 
      Method[] methods = DataNode.class.getDeclaredMethods();
      for (int i = 0; i < methods.length; i++) {
        if (methods[i].getName().equals("transferBlock")) {
          transferBlockMethod = methods[i];
        }
      }
      if (transferBlockMethod == null) {
        throw new IOException("Unable to find method DataNode.transferBlock.");
      }
      transferBlockMethod.setAccessible(true);
    } catch (java.lang.SecurityException exp) {
      throw new IOException(exp);
    }
  }

  @Override
  void startDataNode(Configuration conf, 
                     AbstractList<File> dataDirs
                     ) throws IOException {
    // use configured nameserver & interface to get local hostname
    if (conf.get("slave.host.name") != null) {
      machineName = conf.get("slave.host.name");   
    }
    if (machineName == null) {
      machineName = DNS.getDefaultHost(
                                     conf.get("dfs.datanode.dns.interface","default"),
                                     conf.get("dfs.datanode.dns.nameserver","default"));
    }
    
    this.socketTimeout =  conf.getInt("dfs.socket.timeout",
                                      HdfsConstants.READ_TIMEOUT);
    this.socketWriteTimeout = conf.getInt("dfs.datanode.socket.write.timeout",
                                          HdfsConstants.WRITE_TIMEOUT);
    /* Based on results on different platforms, we might need set the default 
     * to false on some of them. */
    this.transferToAllowed = conf.getBoolean("dfs.datanode.transferTo.allowed", 
                                             true);
    this.writePacketSize = conf.getInt("dfs.write.packet.size", 64*1024);
    String address = 
      NetUtils.getServerAddress(conf,
                                "dfs.datanode.bindAddress", 
                                "dfs.datanode.port",
                                "dfs.datanode.address");
    InetSocketAddress socAddr = NetUtils.createSocketAddr(address);
    int tmpPort = socAddr.getPort();
    storage = new DataStorage();
    // construct registration
    this.dnRegistration = new DatanodeRegistration(machineName + ":" + tmpPort);

    this.namenode = new DatanodeProtocols(2); // override DataNode.namenode
    nameAddr1 = AvatarDataNode.getNameNodeAddress(getConf(), "fs.default.name0", "dfs.namenode.dn-address0");
    nameAddr2 = AvatarDataNode.getNameNodeAddress(getConf(), "fs.default.name1", "dfs.namenode.dn-address1");
    avatarAddr1 = AvatarDataNode.getAvatarNodeAddress(getConf(), "fs.default.name0");
    avatarAddr2 = AvatarDataNode.getAvatarNodeAddress(getConf(), "fs.default.name1");

    zkClient = new AvatarZooKeeperClient(getConf(), null);

    // get version and id info from the name-node
    NamespaceInfo nsInfo = handshake(true);
    StartupOption startOpt = getStartupOption(conf);
    assert startOpt != null : "Startup option must be set.";
    
    boolean simulatedFSDataset = 
        conf.getBoolean("dfs.datanode.simulateddatastorage", false);
    if (simulatedFSDataset) {
        setNewStorageID(dnRegistration);
        dnRegistration.storageInfo.layoutVersion = FSConstants.LAYOUT_VERSION;
        dnRegistration.storageInfo.namespaceID = nsInfo.namespaceID;
        // it would have been better to pass storage as a parameter to
        // constructor below - need to augment ReflectionUtils used below.
        conf.set("StorageId", dnRegistration.getStorageID());
        try {
          //Equivalent of following (can't do because Simulated is in test dir)
          //  this.data = new SimulatedFSDataset(conf);
          this.data = (FSDatasetInterface) ReflectionUtils.newInstance(
              Class.forName("org.apache.hadoop.hdfs.server.datanode.SimulatedFSDataset"), conf);
        } catch (ClassNotFoundException e) {
          throw new IOException(StringUtils.stringifyException(e));
        }
    } else { // real storage
      // read storage info, lock data dirs and transition fs state if necessary
      storage.recoverTransitionRead(nsInfo, dataDirs, startOpt);
      // adjust
      this.dnRegistration.setStorageInfo(storage);
      // initialize data node internal structure
      this.data = new FSDataset(this, storage, conf);
    }

      
    // find free port
    ServerSocket ss = (socketWriteTimeout > 0) ? 
          ServerSocketChannel.open().socket() : new ServerSocket();
    Server.bind(ss, socAddr, 0);
    ss.setReceiveBufferSize(DEFAULT_DATA_SOCKET_SIZE); 
    // adjust machine name with the actual port
    tmpPort = ss.getLocalPort();
    selfAddr = new InetSocketAddress(ss.getInetAddress().getHostAddress(),
                                     tmpPort);
    this.dnRegistration.setName(machineName + ":" + tmpPort);
    LOG.info("Opened info server at " + tmpPort);
      
    this.threadGroup = new ThreadGroup("dataXceiverServer");
    this.dataXceiverServer = new Daemon(threadGroup, 
        new DataXceiverServer(ss, conf, this));
    this.threadGroup.setDaemon(true); // auto destroy when empty

    this.deletedReportInterval =
      conf.getLong("dfs.blockreport.intervalMsec", BLOCKREPORT_INTERVAL);
    // Calculate the full block report interval
    int fullReportMagnifier = conf.getInt("dfs.fullblockreport.magnifier", 2);
    this.blockReportInterval = fullReportMagnifier * deletedReportInterval;
    this.initialBlockReportDelay = conf.getLong("dfs.blockreport.initialDelay",
                                            BLOCKREPORT_INITIAL_DELAY)* 1000L;
    if (this.initialBlockReportDelay >= blockReportInterval) {
      this.initialBlockReportDelay = 0;
      LOG.info("dfs.blockreport.initialDelay is greater than " +
        "dfs.blockreport.intervalMsec." + " Setting initial delay to 0 msec:");
    }
    this.heartBeatInterval = conf.getLong("dfs.heartbeat.interval", HEARTBEAT_INTERVAL) * 1000L;
    //initialize periodic block scanner
    String reason = null;
    if (conf.getInt("dfs.datanode.scan.period.hours", 0) < 0) {
      reason = "verification is turned off by configuration";
    } else if ( !(data instanceof FSDataset) ) {
      reason = "verifcation is supported only with FSDataset";
    } 
    if ( reason == null ) {
      blockScanner = new DataBlockScanner(this, (FSDataset)data, conf);
    } else {
      LOG.info("Periodic Block Verification is disabled because " +
               reason + ".");
    }

    //create a servlet to serve full-file content
    String infoAddr = 
      NetUtils.getServerAddress(conf, 
                              "dfs.datanode.info.bindAddress", 
                              "dfs.datanode.info.port",
                              "dfs.datanode.http.address");
    InetSocketAddress infoSocAddr = NetUtils.createSocketAddr(infoAddr);
    String infoHost = infoSocAddr.getHostName();
    int tmpInfoPort = infoSocAddr.getPort();
    this.infoServer = new HttpServer("datanode", infoHost, tmpInfoPort,
        tmpInfoPort == 0, conf);
    if (conf.getBoolean("dfs.https.enable", false)) {
      boolean needClientAuth = conf.getBoolean("dfs.https.need.client.auth", false);
      InetSocketAddress secInfoSocAddr = NetUtils.createSocketAddr(conf.get(
          "dfs.datanode.https.address", infoHost + ":" + 0));
      Configuration sslConf = new Configuration(false);
      sslConf.addResource(conf.get("dfs.https.server.keystore.resource",
          "ssl-server.xml"));
      this.infoServer.addSslListener(secInfoSocAddr, sslConf, needClientAuth);
    }
    this.infoServer.addInternalServlet(null, "/streamFile/*", StreamFile.class);
    this.infoServer.addInternalServlet(null, "/getFileChecksum/*",
        FileChecksumServlets.GetServlet.class);
    this.infoServer.setAttribute("datanode.blockScanner", blockScanner);
    this.infoServer.addServlet(null, "/blockScannerReport", 
                               DataBlockScanner.Servlet.class);
    this.infoServer.start();
    // adjust info port
    this.dnRegistration.setInfoPort(this.infoServer.getPort());
    myMetrics = new DataNodeMetrics(conf, dnRegistration.getStorageID());
    
    // set service-level authorization security policy
    if (conf.getBoolean(
          ServiceAuthorizationManager.SERVICE_AUTHORIZATION_CONFIG, false)) {
      PolicyProvider policyProvider = 
        (PolicyProvider)(ReflectionUtils.newInstance(
            conf.getClass(PolicyProvider.POLICY_PROVIDER_CONFIG, 
                HDFSPolicyProvider.class, PolicyProvider.class), 
            conf));
      SecurityUtil.setPolicy(new ConfiguredPolicy(conf, policyProvider));
    }

    //init ipc server
    InetSocketAddress ipcAddr = NetUtils.createSocketAddr(
        conf.get("dfs.datanode.ipc.address"));
    ipcServer = RPC.getServer(this, ipcAddr.getHostName(), ipcAddr.getPort(), 
        conf.getInt("dfs.datanode.handler.count", 3), false, conf);
    ipcServer.start();
    dnRegistration.setIpcPort(ipcServer.getListenerAddress().getPort());

    LOG.info("dnRegistration = " + dnRegistration);
  }

  // connect to both name node if possible. 
  // If doWait is true, then return only when at least one handshake is
  // successful.
  //
  private synchronized NamespaceInfo handshake(boolean startup) throws IOException {
    NamespaceInfo nsInfo = null;
    boolean firstIsPrimary = false;
    do {
      if (startup) {
        // The startup option is used when the datanode is first created
        // We only need to connect to the primary at this point and as soon
        // as possible. So figure out who the primary is from the ZK
        InetSocketAddress addr = DataNode.getNameNodeAddress(getConf());
        String addrStr = addr.getHostName() + ":" + addr.getPort();
        Stat stat = new Stat();
        try {
          String primaryAddress =
            zkClient.getPrimaryAvatarAddress(addrStr, stat, true);
          String firstNNAddress = nameAddr1.getHostName() + ":" +
            nameAddr1.getPort();
          firstIsPrimary = firstNNAddress.equalsIgnoreCase(primaryAddress);
        } catch (Exception ex) {
          LOG.error("Could not get the primary address from ZooKeeper", ex);
        }
      }
      try {
        if ((firstIsPrimary && startup) || !startup) {
          // only try to connect to the first NN if it is not the
          // startup connection or if it is primary on startup
          // This way if it is standby we are not wasting datanode startup time
          if (namenode1 == null) {
            namenode1 = (DatanodeProtocol) 
                             RPC.getProxy(DatanodeProtocol.class,
                               DatanodeProtocol.versionID,
                             nameAddr1, 
                             getConf());
            ((DatanodeProtocols)namenode).setDatanodeProtocol(namenode1, 0);
          }
          if (avatarnode1 == null) {
            avatarnode1 = (AvatarProtocol) 
                             RPC.getProxy(AvatarProtocol.class,
                               AvatarProtocol.versionID,
                             avatarAddr1, 
                             getConf());
          }
          if (startup) {
            nsInfo = handshake(namenode1, nameAddr1);
          }
        }
      } catch(ConnectException se) {  // namenode has not been started
        LOG.info("Server at " + nameAddr1 + " not available yet, Zzzzz...");
      } catch(SocketTimeoutException te) {  // namenode is busy
        LOG.info("Problem connecting to server timeout. " + nameAddr1);
      } catch (IOException ioe) {
        LOG.info("Problem connecting to server. " + nameAddr1, ioe);
      }
      try {
        if ((!firstIsPrimary && startup) || !startup) {
          if (namenode2 == null) {
            namenode2 = (DatanodeProtocol) 
                             RPC.getProxy(DatanodeProtocol.class,
                             DatanodeProtocol.versionID,
                             nameAddr2, 
                             getConf());
            ((DatanodeProtocols)namenode).setDatanodeProtocol(namenode2, 1);
          }
          if (avatarnode2 == null) {
            avatarnode2 = (AvatarProtocol) 
                             RPC.getProxy(AvatarProtocol.class,
                               AvatarProtocol.versionID,
                             avatarAddr2, 
                             getConf());
          }
          if (startup) {
            nsInfo = handshake(namenode2, nameAddr2);
          }
        }
      } catch(ConnectException se) {  // namenode has not been started
        LOG.info("Server at " + nameAddr2 + " not available yet, Zzzzz...");
      } catch(SocketTimeoutException te) {  // namenode is busy
        LOG.info("Problem connecting to server timeout. " + nameAddr2);
      } catch (IOException ioe) {
        LOG.info("Problem connecting to server. " + nameAddr2, ioe);
      }
    } while (startup && nsInfo == null && shouldRun);
    return nsInfo;
  }

  private NamespaceInfo handshake(DatanodeProtocol node,
                                  InetSocketAddress machine) throws IOException {
    NamespaceInfo nsInfo = new NamespaceInfo();
    while (shouldRun) {
      try {
        nsInfo = node.versionRequest();
        break;
      } catch(SocketTimeoutException e) {  // namenode is busy
        LOG.info("Problem connecting to server: " + machine);
        try {
          Thread.sleep(1000);
        } catch (InterruptedException ie) {}
      }
    }
    String errorMsg = null;
    // do not fail on incompatible build version
    if( ! nsInfo.getBuildVersion().equals( Storage.getBuildVersion() )) {
      errorMsg = "Incompatible build versions: namenode BV = " 
        + nsInfo.getBuildVersion() + "; datanode BV = "
        + Storage.getBuildVersion();
      LOG.warn( errorMsg );
    }
    if (FSConstants.LAYOUT_VERSION != nsInfo.getLayoutVersion()) {
      errorMsg = "Data-node and name-node layout versions must be the same."
                  + "Expected: "+ FSConstants.LAYOUT_VERSION + 
                  " actual "+ nsInfo.getLayoutVersion();
      LOG.fatal(errorMsg);
      try {
        node.errorReport(dnRegistration,
                         DatanodeProtocol.NOTIFY, errorMsg );
      } catch( SocketTimeoutException e ) {  // namenode is busy        
        LOG.info("Problem connecting to server: " + machine);
      }
      shutdown();
    }
    return nsInfo;
  }

  /**
   * Returns true if we are able to successfully register with namenode
   */
  boolean register(DatanodeProtocol node, InetSocketAddress machine) 
    throws IOException {
    if (dnRegistration.getStorageID().equals("")) {
      setNewStorageID(dnRegistration);
    }

    DatanodeRegistration tmp = new DatanodeRegistration(dnRegistration.getName());
    tmp.setInfoPort(dnRegistration.getInfoPort());
    tmp.setIpcPort(dnRegistration.getIpcPort());
    tmp.setStorageInfo(storage);

    // reset name to machineName. Mainly for web interface.
    tmp.name = machineName + ":" + dnRegistration.getPort();
    try {
      tmp = node.register(tmp);
      // if we successded registering for the first time, then we update
      // the global registration objct
      if (!doneRegister1 && !doneRegister2) {
        dnRegistration = tmp;
      }
    } catch(SocketTimeoutException e) {  // namenode is busy
      LOG.info("Problem connecting to server: " + machine);
      return false;
    }

    assert ("".equals(storage.getStorageID()) 
            && !"".equals(dnRegistration.getStorageID()))
            || storage.getStorageID().equals(dnRegistration.getStorageID()) :
            "New storageID can be assigned only if data-node is not formatted";
    if (storage.getStorageID().equals("")) {
      storage.setStorageID(dnRegistration.getStorageID());
      storage.writeAll();
      LOG.info("New storage id " + dnRegistration.getStorageID()
          + " is assigned to data-node " + dnRegistration.getName());
    }
    if(! storage.getStorageID().equals(dnRegistration.getStorageID())) {
      throw new IOException("Inconsistent storage IDs. Name-node returned "
          + dnRegistration.getStorageID() 
          + ". Expecting " + storage.getStorageID());
    }
    return  true;
  }
  
  boolean isPrimaryOfferService(OfferService service) {
    return primaryOfferService == service;
  }
  
  void setPrimaryOfferService(OfferService service) {
    this.primaryOfferService = service;
    if (service != null)
      LOG.info("Primary namenode is set to be " + service.avatarnodeAddress);
    else {
      LOG.info("Failover has happened. Stop accessing commands from " +
      		"either namenode until the new primary is completely in" +
      		"sync with all the datanodes");
    }
  }

  @Override
  public void run() {
    LOG.info(dnRegistration + "In AvatarDataNode.run, data = " + data);

    // start dataXceiveServer
    dataXceiverServer.start();

    while (shouldRun && !shutdown) {
      try {

        // try handshaking with any namenode that we have not yet tried
        handshake(false);

        if (namenode1 != null && !doneRegister1 &&
            register(namenode1, nameAddr1)) {
          doneRegister1 = true;
          offerService1 = new OfferService(this, namenode1, nameAddr1, 
                                           avatarnode1, avatarAddr1);
          of1 = new Thread(offerService1, "OfferService1 " + nameAddr1);
          of1.start();
        }
        if (namenode2 != null && !doneRegister2 &&
            register(namenode2, nameAddr2)) {
          doneRegister2 = true;
          offerService2 = new OfferService(this, namenode2, nameAddr2,
                                           avatarnode2, avatarAddr2);
          of2 = new Thread(offerService2, "OfferService2 " + nameAddr2);
          of2.start();
        }

        startDistributedUpgradeIfNeeded();

        // start block scanner
        if (blockScanner != null && blockScannerThread == null &&
            upgradeManager.isUpgradeCompleted()) {
          LOG.info("Starting Periodic block scanner.");
          blockScannerThread = new Daemon(blockScanner);
          blockScannerThread.start();
        }
      } catch (RemoteException re) {
        // If either the primary or standby NN throws these exceptions, this
        // datanode will exit. 
        LOG.error("Exception: ", re);
        String reClass = re.getClassName();
        if (UnregisteredDatanodeException.class.getName().equals(reClass) ||
            DisallowedDatanodeException.class.getName().equals(reClass) ||
            IncorrectVersionException.class.getName().equals(reClass)) {
          LOG.warn("DataNode is shutting down: ", re);
          this.shutdownDN();
        }
      } catch (Exception ex) {
        LOG.error("Exception: ", ex);
      }
      if (shouldRun && !shutdown) {
        try {
          Thread.sleep(5000);
        } catch (InterruptedException ie) {
        }
      }
    }

    LOG.info(dnRegistration + ":Finishing AvatarDataNode in: "+data);
    shutdown();
  }

  /**
   * Notify both namenode(s) that we have received a block
   */
  @Override
  protected void notifyNamenodeReceivedBlock(Block block, String delHint) {
    if (offerService1 != null) {
      offerService1.notifyNamenodeReceivedBlock(block, delHint);
    }
    if (offerService2 != null) {
      offerService2.notifyNamenodeReceivedBlock(block, delHint);
    }
  }

  /**
   * Notify both namenode(s) that we have deleted a block
   */
  @Override
  protected void notifyNamenodeDeletedBlock(Block block) {
    if (offerService1 != null) {
      offerService1.notifyNamenodeDeletedBlock(block);
    }
    if (offerService2 != null) {
      offerService2.notifyNamenodeDeletedBlock(block);
    }
  }

  /**
   * Update received and retry list, when blocks are deleted
   */
  void removeReceivedBlocks(Block[] list) {
    if (offerService1 != null) {
      offerService1.removeReceivedBlocks(list);
    }
    if (offerService2 != null) {
      offerService2.removeReceivedBlocks(list);
    }
  }

  /**
   * Start distributed upgrade if it should be initiated by the data-node.
   */
  private void startDistributedUpgradeIfNeeded() throws IOException {
    UpgradeManagerDatanode um = DataNode.getDataNode().upgradeManager;
    assert um != null : "DataNode.upgradeManager is null.";
    if(!um.getUpgradeState())
      return;
    um.setUpgradeState(false, um.getUpgradeVersion());
    um.startUpgrade();
    return;
  }

  void transferBlocks(Block blocks[], DatanodeInfo xferTargets[][]) {
    for (int i = 0; i < blocks.length; i++) {
      try {
        transferBlockMethod.invoke(this, blocks[i], xferTargets[i]);
      } catch (java.lang.IllegalAccessException ie) {
        LOG.warn("Failed to transfer block " + blocks[i], ie);
      } catch (java.lang.reflect.InvocationTargetException ie) {
        LOG.warn("Failed to transfer block " + blocks[i], ie);
      }
    }
  }

 /**
  * Tells the datanode to start the shutdown process.
  */
  public synchronized void shutdownDN() {
    shutdown = true;
    if (dataNodeThread != null) {
      dataNodeThread.interrupt();
    }
  }
 /**
   * Shut down this instance of the datanode.
   * Returns only after shutdown is complete.
   * This can be called from one of the Offer services thread
   * or from the DataNode main thread.
   */
  @Override
  public synchronized void shutdown() {
   if (of1 != null) {
      offerService1.stop();
      of1.interrupt();
      try {
        of1.join();
      } catch (InterruptedException ie) {
      }
    }
    if (of2 != null) {
      offerService2.stop();
      of2.interrupt();
      try {
        of2.join();
      } catch (InterruptedException ie) {
      }
    }
    if (infoServer != null) {
      try {
        infoServer.stop();
      } catch (Exception e) {
        LOG.warn("Exception shutting down DataNode", e);
      }
    }
    super.shutdown();
    if (storage != null) {
      try {
        this.storage.unlockAll();
      } catch (IOException ie) {
      }
    }
    if (dataNodeThread != null &&
        Thread.currentThread() != dataNodeThread) {
      dataNodeThread.interrupt();
      try {
        dataNodeThread.join();
      } catch (InterruptedException ie) {
      }
    }
  }

  public static void runDatanodeDaemon(AvatarDataNode dn) throws IOException {
    if (dn != null) {
      dn.dataNodeThread = new Thread(dn, dn.dnThreadName);
      dn.dataNodeThread.setDaemon(true); // needed for JUnit testing
      dn.dataNodeThread.start();
    }
  }

  void join() {
    if (dataNodeThread != null) {
      try {
        dataNodeThread.join();
      } catch (InterruptedException e) {}
    }
  }

  DataStorage getStorage() {
    return storage;
  }

  private static void printUsage() {
    System.err.println("Usage: java DataNode");
    System.err.println("           [-rollback]");
  }

  /**
   * Parse and verify command line arguments and set configuration parameters.
   *
   * @return false if passed argements are incorrect
   */
  private static boolean parseArguments(String args[],
                                        Configuration conf) {
    int argsLen = (args == null) ? 0 : args.length;
    StartupOption startOpt = StartupOption.REGULAR;
    for(int i=0; i < argsLen; i++) {
      String cmd = args[i];
      if ("-r".equalsIgnoreCase(cmd) || "--rack".equalsIgnoreCase(cmd)) {
        LOG.error("-r, --rack arguments are not supported anymore. RackID " +
            "resolution is handled by the NameNode.");
        System.exit(-1);
      } else if ("-rollback".equalsIgnoreCase(cmd)) {
        startOpt = StartupOption.ROLLBACK;
      } else if ("-regular".equalsIgnoreCase(cmd)) {
        startOpt = StartupOption.REGULAR;
      } else
        return false;
    }
    setStartupOption(conf, startOpt);
    return true;
  }

  private static void setStartupOption(Configuration conf, StartupOption opt) {
    conf.set("dfs.datanode.startup", opt.toString());
  }

  /**
   * Returns the IP address of the namenode
   */
  static InetSocketAddress getNameNodeAddress(Configuration conf,
                                                      String cname, String cname2) {
    String fs = conf.get(cname);
    String fs2 = conf.get(cname2);
    Configuration newconf = new Configuration(conf);
    newconf.set("fs.default.name", fs);
    if (fs2 != null) {
      newconf.set("dfs.namenode.dn-address", fs2);
    }
    return DataNode.getNameNodeAddress(newconf);
  }

  @Override
  public InetSocketAddress getNameNodeAddr() {
    return NameNode.getAddress(getConf());
  }

  /**
   * Returns the IP:port address of the avatar node
   */
  private static InetSocketAddress getAvatarNodeAddress(Configuration conf,
                                                        String cname) {
    String fs = conf.get(cname);
    Configuration newconf = new Configuration(conf);
    newconf.set("fs.default.name", fs);
    return AvatarNode.getAddress(newconf);
  }

  public static AvatarDataNode makeInstance(String[] dataDirs, Configuration conf)
    throws IOException {
    ArrayList<File> dirs = new ArrayList<File>();
    for (int i = 0; i < dataDirs.length; i++) {
      File data = new File(dataDirs[i]);
      try {
        DiskChecker.checkDir(data);
        dirs.add(data);
      } catch(DiskErrorException e) {
        LOG.warn("Invalid directory in dfs.data.dir: " + e.getMessage());
      }
    }
    if (dirs.size() > 0) {
      String dnThreadName = "AvatarDataNode: [" +
        StringUtils.arrayToString(dataDirs) + "]";
      return new AvatarDataNode(conf, dirs, dnThreadName);
    }
    LOG.error("All directories in dfs.data.dir are invalid.");
    return null;
  }

  /** Instantiate a single datanode object. This must be run by invoking
   *  {@link DataNode#runDatanodeDaemon(DataNode)} subsequently. 
   */
  public static AvatarDataNode instantiateDataNode(String args[],
                                      Configuration conf) throws IOException {
    if (conf == null)
      conf = new Configuration();
    if (!parseArguments(args, conf)) {
      printUsage();
      return null;
    }
    if (conf.get("dfs.network.script") != null) {
      LOG.error("This configuration for rack identification is not supported" +
          " anymore. RackID resolution is handled by the NameNode.");
      System.exit(-1);
    }
    String[] dataDirs = conf.getStrings("dfs.data.dir");
    return makeInstance(dataDirs, conf);
  }

  public static AvatarDataNode createDataNode(String args[],
                                 Configuration conf) throws IOException {
    AvatarDataNode dn = instantiateDataNode(args, conf);
    runDatanodeDaemon(dn);
    return dn;
  }

  public static void main(String argv[]) {
    try {
      StringUtils.startupShutdownMessage(AvatarDataNode.class, argv, LOG);
      AvatarDataNode avatarnode = createDataNode(argv, null);
      if (avatarnode != null)
        avatarnode.join();
    } catch (Throwable e) {
      LOG.error(StringUtils.stringifyException(e));
      System.exit(-1);
    }
  }


}
