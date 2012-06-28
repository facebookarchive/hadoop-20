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
package org.apache.hadoop.mapred;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.net.NetUtils;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;

/**
 * Cache of RPC clients to corona task trackers.
 */
class TrackerClientCache {
  /** Logger. */
  private static final Log LOG = LogFactory.getLog(TrackerClientCache.class);
  /** Map of host:port -> rpc client. */
  private Map<String, CoronaTaskTrackerProtocol> trackerClients =
    new HashMap<String, CoronaTaskTrackerProtocol>();
  /** Configuration. */
  private Configuration conf;

  /**
   * Constructor.
   * @param conf Configuration.
   */
  TrackerClientCache(Configuration conf) {
    this.conf = conf;
  }

  /**
   * API to get the RPC client.
   * @param host The host
   * @param port The port
   * @return The RPC client
   * @throws IOException
   */
  public synchronized CoronaTaskTrackerProtocol getClient(
    String host, int port) throws IOException {
    String key = makeKey(host, port);
    CoronaTaskTrackerProtocol client = trackerClients.get(key);
    if (client == null) {
      client = createClient(host, port);
      trackerClients.put(key, client);
    }
    return client;
  }

  /**
   * Remove the RPC client form the cache.
   * @param host The host
   * @param port The port
   */
  public synchronized void resetClient(String host, int port) {
    trackerClients.remove(makeKey(host, port));
  }

  /**
   * Connect to the task tracker and get the RPC client.
   * @param host The host.
   * @param port the port.
   * @return The RPC client.
   * @throws IOException
   */
  private CoronaTaskTrackerProtocol createClient(String host, int port)
    throws IOException {
    String staticHost = NetUtils.getStaticResolution(host);
    InetSocketAddress s = null;
    if (staticHost != null) {
      s = new InetSocketAddress(staticHost, port);
    } else {
      s = new InetSocketAddress(host, port);
    }
    LOG.info("Creating client to " + s.getHostName() + ":" + s.getPort());
    long connectTimeout =
      conf.getLong(CoronaJobTracker.TT_CONNECT_TIMEOUT_MSEC_KEY, 10000L);
    int rpcTimeout =
      conf.getInt(CoronaJobTracker.TT_RPC_TIMEOUT_MSEC_KEY, 60000);
    return RPC.waitForProxy(
      CoronaTaskTrackerProtocol.class,
      CoronaTaskTrackerProtocol.versionID,
      s,
      conf,
      connectTimeout,
      rpcTimeout);
  }

  /**
   * Creates a key from host and port
   * @param host The host
   * @param port The port
   * @return String in the form host:port
   */
  private static String makeKey(String host, int port) {
    return host + ":" + port;
  }
}
