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
package org.apache.hadoop.hdfs.storageservice;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.protocol.FSConstants;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.net.NetUtils;

import java.net.InetSocketAddress;
import java.net.URI;

public class StorageServiceConfigKeys {
  private StorageServiceConfigKeys() {
  }

  private static final String PREFIX = "dfs.storageservice.";

  public static final String PROXY_THRIFT_HOST_KEY = PREFIX + "proxy.thrift.host";
  public static final String PROXY_THRIFT_HOST_DEFAULT = "127.0.0.1";

  public static final String PROXY_THRIFT_PORT_KEY = PREFIX + "proxy.thrift.port";
  public static final short PROXY_THRIFT_PORT_DEFAULT = 5903;

  public static InetSocketAddress getProxyThriftAddress(Configuration conf) {
    return new InetSocketAddress(conf.get(PROXY_THRIFT_HOST_KEY, PROXY_THRIFT_HOST_DEFAULT),
        conf.getInt(PROXY_THRIFT_PORT_KEY, PROXY_THRIFT_PORT_DEFAULT));
  }

  public static final String PROXY_RPC_HOST_KEY = PREFIX + "proxy.rpc.host";
  public static final String PROXY_RPC_HOST_DEFAULT = "127.0.0.1";

  public static final String PROXY_RPC_PORT_KEY = PREFIX + "proxy.rpc.port";
  public static final short PROXY_RPC_PORT_DEFAULT = 5902;

  public static InetSocketAddress getProxyRPCAddress(Configuration conf) {
    return new InetSocketAddress(conf.get(PROXY_RPC_HOST_KEY, PROXY_RPC_HOST_DEFAULT), conf.getInt(
        PROXY_RPC_PORT_KEY, PROXY_RPC_PORT_DEFAULT));
  }

  /** Translates nameserviceId to ZK key in deprecated layout */
  @Deprecated
  public static URI translateToOldSchema(Configuration clusterConf, String nameserviceId) {
    String key = FSConstants.DFS_NAMENODE_RPC_ADDRESS_KEY + "." + nameserviceId;
    String value = clusterConf.get(key);
    if (value == null) {
      throw new IllegalArgumentException(
          "Cannot translate to old schema for nameserviceId: " + nameserviceId);
    }
    InetSocketAddress address = NetUtils.createSocketAddr(value);
    return NameNode.getUri(address);
  }
}
