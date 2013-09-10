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
package org.apache.hadoop.hdfs.notifier.server;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.AvatarZooKeeperClient;
import org.apache.hadoop.hdfs.CachingAvatarZooKeeperClient;
import org.apache.hadoop.hdfs.qjournal.client.QuorumJournalManager;
import org.apache.hadoop.hdfs.server.common.Storage.StorageDirectory;
import org.apache.hadoop.hdfs.server.common.StorageInfo;
import org.apache.hadoop.hdfs.server.common.Util;
import org.apache.hadoop.hdfs.server.namenode.AvatarNode;
import org.apache.hadoop.hdfs.server.namenode.FileJournalManagerReadOnly;
import org.apache.hadoop.hdfs.server.namenode.JournalManager;
import org.apache.hadoop.hdfs.server.namenode.NNStorage;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.server.protocol.NamespaceInfo;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;

/**
 * This class supports reading the edit logs from HDFS Avatar nodes, so it is 
 * failover-aware, and the HDFS namenode failover would be transparent for 
 * this log reader. 
 * 
 */
public class ServerLogReaderAvatar extends ServerLogReaderTransactional {

  public static final Log LOG = LogFactory
      .getLog(ServerLogReaderAvatar.class);

  // remote journal from which the reader is consuming transactions
  private final JournalManager remoteJournalManagerZero;
  private final JournalManager remoteJournalManagerOne;
  
  /**
   * The location of the edits logs for Avatar zero and one node.
   */
  private final URI editsUriZero;
  private final URI editsUriOne;
  
  /* We will look up the rpc address in zookeeper entity to determine
   *  which node is the current primary node 
   */
  // rpc address of the avatar zero node
  private URI avatarZeroURI;
  // rpc address of the avatar one node
  private URI avatarOneURI;
  
  // the rpc address we read from zookeeper entity.
  private URI primaryURI = null;

  // the name of the zookeeper entity.
  private URI logicalName;
  
  private CachingAvatarZooKeeperClient zk;
  
  private static final int FAILOVER_RETRY_SLEEP = 1000;
  
  static {
    Configuration.addDefaultResource("avatar-default.xml");
    Configuration.addDefaultResource("avatar-site.xml");
  }

  static void throwIOException(String msg, Throwable e) 
      throws IOException {
    LOG.error(msg, e);
    throw new IOException(msg, e);
  }
  
  public ServerLogReaderAvatar(IServerCore core)
      throws IOException {
    
    super(core, null);
    
    // append the service name to each avatar meta directory name
    String serviceName = core.getServiceName();
    if (serviceName != null && !serviceName.equals("")) {
      AvatarNode.adjustMetaDirectoryNames(conf, serviceName);
    } else {
      serviceName = "";
    }
    
    // edits directory uri
    String editsStringZero = core.getConfiguration().get(
        AvatarNode.DFS_SHARED_EDITS_DIR0_KEY);
    String editsStringOne = core.getConfiguration().get(
        AvatarNode.DFS_SHARED_EDITS_DIR1_KEY);
    
    editsUriZero = Util.stringAsURI(editsStringZero);
    editsUriOne = Util.stringAsURI(editsStringOne);

    remoteJournalManagerZero = constructJournalManager(editsUriZero);
    remoteJournalManagerOne = constructJournalManager(editsUriOne);
    
    try {
    	this.logicalName = new URI("hdfs://" + 
          conf.get(NameNode.DFS_NAMENODE_RPC_ADDRESS_KEY + 
          (serviceName.isEmpty() ? "" : "." + serviceName), ""));
    	
      this.avatarZeroURI = addrToURI(conf.get(
                          AvatarNode.DFS_NAMENODE_RPC_ADDRESS0_KEY + 
                          (serviceName.isEmpty() ? "" : "." + serviceName), ""));
      this.avatarOneURI = addrToURI(conf.get(
                          AvatarNode.DFS_NAMENODE_RPC_ADDRESS1_KEY + 
                          (serviceName.isEmpty() ? "" : "." + serviceName), ""));
      
      zk = new CachingAvatarZooKeeperClient(conf, null);

      LOG.info("Initializing input stream");
      initialize();
      LOG.info("Initialization completed");
    } catch (URISyntaxException e) {
      throwIOException(e.getMessage(), e);
    }
  }
  
  private JournalManager constructJournalManager(URI editsUri) 
      throws IOException {
    if (editsUri.getScheme().equals(NNStorage.LOCAL_URI_SCHEME)) {
      StorageDirectory sd = new NNStorage(new StorageInfo()).new StorageDirectory(
          new File(editsUri.getPath()));
      return new FileJournalManagerReadOnly(sd);
    } else if (editsUri.getScheme().equals(QuorumJournalManager.QJM_URI_SCHEME)) {
    	return new QuorumJournalManager(conf, editsUri, 
    					new NamespaceInfo(new StorageInfo()), null, false);
    } else {
      throwIOException("Other journals not supported yet.", null);
    }
    return null;
  }
  
  /**
   * Construct URI from address string. 
   */
  private URI addrToURI(String addrString) throws URISyntaxException {
    if (addrString.startsWith(logicalName.getScheme())) {
      // if the scheme is correct, we just return the new URI(...)
      return new URI(addrString);
    } else {
      // otherwise, we will extract the host and port from the address
      // string, and construct the URI using logicalName's scheme (hdfs://).
      if (addrString.indexOf(":") == -1) {
        // This is not a valid addr string
        return null;
      }
      String fsHost = addrString.substring(0, addrString.indexOf(":"));
      int port = Integer.parseInt(addrString
                                  .substring(addrString.indexOf(":") + 1));
      return new URI(logicalName.getScheme(),
                     logicalName.getUserInfo(),
                     fsHost, port, logicalName.getPath(),
                     logicalName.getQuery(), logicalName.getFragment());
    }
  }

  /**
   * Detect the primary node and the current Journal Manager;
   * @throws IOException
   */
  protected void detectJournalManager() throws IOException {
    int failures = 0;
    do {
      try {
        Stat stat = new Stat();
        String primaryAddr = zk.getPrimaryAvatarAddress(logicalName, 
                                                     stat, true, true);
        if (primaryAddr == null || primaryAddr.trim().isEmpty()) {
          primaryURI = null;
          remoteJournalManager = null;
          LOG.warn("Failover detected, wait for it to finish...");
          failures = 0;
          sleep(FAILOVER_RETRY_SLEEP);
          continue;
        }
        
        primaryURI = addrToURI(primaryAddr);
        
        LOG.info("Read primary URI from zk: " + primaryURI);
        if (primaryURI.equals(avatarZeroURI)) {
          remoteJournalManager = remoteJournalManagerZero;
        } else if (primaryURI.equals(avatarOneURI)) {
          remoteJournalManager = remoteJournalManagerOne;
        } else {
          LOG.warn("Invalid primaryURI: " + primaryURI);
          primaryURI = null;
          remoteJournalManager = null;
          failures = 0;
          sleep(FAILOVER_RETRY_SLEEP);
        }
      
      } catch (KeeperException kex) {
        if (KeeperException.Code.CONNECTIONLOSS == kex.code()
            && failures < AvatarZooKeeperClient.ZK_CONNECTION_RETRIES) {
          failures++;
          // This means there was a failure connecting to zookeeper
          // we should retry since some nodes might be down.
          sleep(FAILOVER_RETRY_SLEEP);
          continue;
        }
        throwIOException(kex.getMessage(), kex);
      } catch (InterruptedException e) {
        throwIOException(e.getMessage(), e);
      } catch (URISyntaxException e) {
        throwIOException(e.getMessage(), e);
      }
    } while (remoteJournalManager == null);
  }
}
