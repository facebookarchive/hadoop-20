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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.protocol.ClientProxyRequests.Request;
import org.apache.hadoop.hdfs.storageservice.Response;
import org.apache.hadoop.io.IOUtils;

import java.io.Closeable;
import java.io.IOException;

/**
 * Main class in storage service layer, creates client's endpoints,
 * NameNodes' endpoints and connects them.
 */
public class ClientProxyService implements Closeable {
  private final static Log LOG = LogFactory.getLog(ClientProxyService.class);
  /** Common objects, global configuration, statistics */
  private final ClientProxyCommons commons;
  /** Maintains NameNodes' endpoints and routes calls */
  private final NameNodeRouter namenodeRouter;
  /** Thrift client's endpoint */
  private final TClientProxyProtocolServer thriftClientServer;
  private final ClientProxyProtocolServer hadoopClientServer;

  public ClientProxyService(ClientProxyCommons commons) throws IOException {
    try {
      this.commons = commons;
      // Namenode endpoints
      namenodeRouter = new NameNodeRouter(commons);
      // Thrift clients endpoint
      thriftClientServer = new TClientProxyProtocolServer(commons, this);
      // Hadoop's RPC clients endpoint
      hadoopClientServer = new ClientProxyProtocolServer(commons, this);
    } catch (IOException e) {
      close();
      throw e;
    }
  }

  public ClientProxyCommons getCommons() {
    return commons;
  }

  @Override
  public void close() {
    IOUtils.cleanup(LOG, hadoopClientServer, thriftClientServer, namenodeRouter);
  }

  public <T> T handleRequest(Request<T> request) throws IOException {
    commons.metrics.startProcessing(request);
    Response<T> response = namenodeRouter.resolve(request.getRequestMetaInfo()).submit(request);
    // TODO: return ListenableFuture<T> when swift-service is patched to support that
    commons.metrics.endProcessing(request);
    return response.checkedGet();
  }

  public int getThriftPort() {
    return thriftClientServer.getPort();
  }

  public int getRPCPort() {
    return hadoopClientServer.getPort();
  }

  /** Encapsulates all elements shared across entire client proxy service */
  public static class ClientProxyCommons {
    public final Configuration conf;
    public final ClientProxyMetrics metrics;
    public final String clusterName;

    public ClientProxyCommons(Configuration conf, String clusterName) throws IOException {
      this.conf = conf;
      this.metrics = new ClientProxyMetrics();
      this.clusterName = clusterName;
    }
  }
}
