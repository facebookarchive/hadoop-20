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
import org.apache.hadoop.hdfs.protocol.ClientProxyRequests.RequestMetaInfo;
import org.apache.hadoop.hdfs.protocol.FSConstants;
import org.apache.hadoop.hdfs.storageservice.protocol.NameNodeRoutingException;
import org.apache.hadoop.hdfs.storageservice.server.ClientProxyService.ClientProxyCommons;
import org.apache.hadoop.io.IOUtils;

import java.io.Closeable;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;

/**
 * Multiplexing layer, resolves requests destinations and creates proper NameNodeHandlers.
 * There is one NameNodeRouter per ClientProxyService and many NameNodeHandlers per one
 * NameNodeRouter.
 */
final class NameNodeRouter implements Closeable {
  private static final Log LOG = LogFactory.getLog(NameNodeRouter.class);
  private final ClientProxyCommons commons;
  private final int clusterId;
  private final Map<String, NameNodeHandler> handlers;

  public NameNodeRouter(ClientProxyCommons commons) throws IOException {
    this.commons = commons;
    this.clusterId = commons.conf.getInt(FSConstants.DFS_CLUSTER_ID, RequestMetaInfo.NO_CLUSTER_ID);
    if (this.clusterId == RequestMetaInfo.NO_CLUSTER_ID) {
      String msg = "Cluster ID is not set in configuration.";
      LOG.error(msg);
      throw new IllegalArgumentException(msg);
    }
    handlers = new HashMap<String, NameNodeHandler>();
    try {
      for (String nameserviceId : commons.conf.getStringCollection(
          FSConstants.DFS_FEDERATION_NAMESERVICES)) {
        LOG.info("Initializing NameNodeHandler for clusterId: " + clusterId +
            "nameserviceId: " + nameserviceId);
        handlers.put(nameserviceId, new NameNodeHandler(commons, nameserviceId));
      }
    } catch (URISyntaxException e) {
      LOG.error("Malformed URI", e);
      throw new IOException(e);
    }
  }

  @Override
  public void close() {
    for (NameNodeHandler handler : handlers.values()) {
      IOUtils.cleanup(LOG, handler);
    }
  }

  public NameNodeHandler resolve(RequestMetaInfo metaInfo) throws NameNodeRoutingException {
    final int clusterId = metaInfo.getClusterId();
    if (clusterId == RequestMetaInfo.NO_CLUSTER_ID) {
      String msg = "ClusterId not specified";
      LOG.error(msg);
      throw new NameNodeRoutingException(msg);
    }
    if (clusterId != this.clusterId) {
      String msg = "ClusterId: " + clusterId + " does NOT match " + "expected: " + this.clusterId;
      LOG.error(msg);
      throw new NameNodeRoutingException(msg);
    }
    final String resourceId = metaInfo.getResourceId();
    NameNodeHandler handler = handlers.get(resourceId);
    if (handler == null) {
      throw new NameNodeRoutingException("Not recognized resourceId: " + resourceId);
    }
    return handler;
  }
}
