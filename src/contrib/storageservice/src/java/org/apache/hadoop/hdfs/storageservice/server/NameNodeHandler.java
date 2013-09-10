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
package org.apache.hadoop.hdfs.storageservice.server;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.ClientConfigurationUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.FailoverClient;
import org.apache.hadoop.hdfs.FailoverClientHandler;
import org.apache.hadoop.hdfs.FailoverClientHandler.NoRetriesFSCaller;
import org.apache.hadoop.hdfs.protocol.ClientProxyProtocol;
import org.apache.hadoop.hdfs.protocol.ClientProxyRequests.Request;
import org.apache.hadoop.hdfs.storageservice.Response;
import org.apache.hadoop.hdfs.storageservice.StorageServiceConfigKeys;
import org.apache.hadoop.hdfs.storageservice.server.ClientProxyService.ClientProxyCommons;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.VersionedProtocol;
import org.apache.hadoop.net.NetUtils;
import org.apache.zookeeper.data.Stat;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;

/** Handles requests execution for a pair of NameNodes - primary and standby */
final class NameNodeHandler implements Closeable, FailoverClient {
  private static final Log LOG = LogFactory.getLog(NameNodeHandler.class);
  /** {@link ClientProxyService#commons} */
  private final ClientProxyCommons commons;
  /** We're using namenode specific configuration here */
  private final Configuration conf;
  /** Handles actual call execution */
  private final RequestCallableExecutor executor;
  /** A logical name associated with this pair of NameNodes */
  private final URI logicalName;
  /** A name associated with this pair of NameNodes for ZooKeeper lookup */
  @Deprecated
  private final URI zkLogicalName;
  private final FailoverClientHandler failoverHandler;
  /** This is always the low-level RPC client */
  private volatile ClientProxyProtocol namenodeRPC;
  private volatile boolean shuttingDown = false;

  public NameNodeHandler(ClientProxyCommons commons, String nameserviceId) throws IOException,
      URISyntaxException {
    try {
      this.commons = commons;
      this.logicalName = new URI("hdfs", commons.clusterName + "." + nameserviceId, null, null,
          null);
      LOG.info("Looking up configuration for logical name: " + this.logicalName);
      this.conf = ClientConfigurationUtil.mergeConfiguration(logicalName, commons.conf);
      this.zkLogicalName = StorageServiceConfigKeys.translateToOldSchema(conf, nameserviceId);
      LOG.info("Initializing RequestCallableExecutor");
      executor = new RequestCallableExecutor(conf);
      LOG.info("Initializing failover handler with logical name: " + this.zkLogicalName);
      failoverHandler = new FailoverClientHandler(conf, this.zkLogicalName, this);
      initNamenodeRPC();
    } catch (IOException e) {
      LOG.error("Initialization failed for: " + commons.clusterName + "." + nameserviceId, e);
      close();
      throw e;
    }
  }

  public <T> Response<T> submit(Request<T> request) {
    commons.metrics.executorSubmit(request);
    return executor.submit(new RequestCallable<T>(request));
  }

  private void initNamenodeRPC() throws IOException {
    LOG.info("Initializing RPC proxy to NameNode");
    nameNodeDown();
    // create new client
    Stat stat = new Stat();
    boolean firstAttempt = true;
    while (true) {
      try {
        String address = failoverHandler.getPrimaryAvatarAddress(zkLogicalName, stat, true,
            firstAttempt);
        newNamenode(createClientProxyProtocol(conf, address));
        break;
      } catch (Exception e) {
        if (firstAttempt && failoverHandler.isZKCacheEnabled()) {
          firstAttempt = false;
        } else {
          LOG.error("ClientProxyProtocol initialization failed: ", e);
          throw new IOException(e);
        }
      }
    }
  }

  private void closeNamenodeRPC() throws IOException {
    if (failoverHandler != null) {
      failoverHandler.readLock();
      try {
        RPC.stopProxy(namenodeRPC);
        IOUtils.cleanup(null, failoverHandler);
      } finally {
        failoverHandler.readUnlock();
      }
    }
  }

  /** {@link Closeable} */
  @Override
  public void close() throws IOException {
    shuttingDown = true;
    IOUtils.cleanup(null, executor);
    closeNamenodeRPC();
  }

  /** {@link FailoverClient} */
  @Override
  public boolean tryFailover() throws IOException {
    initNamenodeRPC();
    return true;
  }

  @Override
  public boolean isShuttingdown() {
    return shuttingDown;
  }

  @Override
  public boolean isFailoverInProgress() {
    return namenodeRPC == null;
  }

  @Override
  public void nameNodeDown() {
    RPC.stopProxy(this.namenodeRPC);
    this.namenodeRPC = null;
  }

  @Override
  public void newNamenode(VersionedProtocol namenode) {
    this.namenodeRPC = (ClientProxyProtocol) namenode;
  }

  /** Task which handles execution of NameNode call and fallback behaviour in case of failover. */
  class RequestCallable<T> extends NoRetriesFSCaller<T> {
    private final Request<T> request;

    public RequestCallable(Request<T> request) {
      failoverHandler.super();
      this.request = request;
    }

    @Override
    protected T callInternal() throws IOException {
      commons.metrics.executorCall(request);
      commons.metrics.namenodeCalled(request);
      T result = request.call(NameNodeHandler.this.namenodeRPC);
      commons.metrics.namenodeReturned(request);
      return result;
    }
  }

  private static ClientProxyProtocol createClientProxyProtocol(Configuration conf,
      String addressStr) throws IOException {
    String parts[] = addressStr.split(":");
    if (parts.length != 2) {
      throw new IOException("Invalid address : " + addressStr);
    }
    InetSocketAddress address = new InetSocketAddress(parts[0], Integer.parseInt(parts[1]));
    return RPC.getProtocolProxy(ClientProxyProtocol.class, ClientProxyProtocol.versionID, address,
        conf, NetUtils.getSocketFactory(conf, ClientProxyProtocol.class)).getProxy();
  }
}
