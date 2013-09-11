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
package org.apache.hadoop.hdfs.qjournal.protocol;

import java.net.InetSocketAddress;
import java.util.Collection;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.net.NetUtils;

public class JournalConfigHelper {
  
  /**
   * Parse the DFS_JOURNALNODE_HOSTS to get the list of Journal Node Hosts
   * 
   * This is used in JournalNode and QuorumJournalManager to talk to the 
   * Http servers of other JournalNodes.
   * 
   * On the server side, the ports of the hosts are specified in the DFS_JOURNALNODE_HTTP_ADDRESS_KEY,
   * so the ports in DFS_JOURNALNODE_HOSTS should be consistent with the port specified in 
   * DFS_JOURNALNODE_HTTP_ADDRESS_KEY, or left empty for DFS_JOURNALNODE_HTTP_PORT_DEFAULT.
   */
  public static String[] getJournalHttpHosts(Configuration conf) {
    Collection<String> hosts = conf.getStringCollection(JournalConfigKeys.DFS_JOURNALNODE_HOSTS);
    int defaultHttpPort = JournalConfigKeys.DFS_JOURNALNODE_HTTP_PORT_DEFAULT;

    String[] httpAddresses = new String[hosts.size()];
    
    int i = 0;
    for (String address : hosts) {
      if (address.indexOf(":") < 0) {
        address += ":" + defaultHttpPort;
      }
      httpAddresses[i++] = address;
    }
    
    return httpAddresses;
  }

  /**
   * Get the address bound by the JournalNode to start the web server.
   * 
   * This returns the server side address, should be consistent with the getJournalHttpHosts.
   */
  public static InetSocketAddress getAddress(Configuration conf) {
    String addr = conf.get(JournalConfigKeys.DFS_JOURNALNODE_HTTP_ADDRESS_KEY,
        JournalConfigKeys.DFS_JOURNALNODE_HTTP_ADDRESS_DEFAULT);
    return NetUtils.createSocketAddr(addr,
        JournalConfigKeys.DFS_JOURNALNODE_HTTP_PORT_DEFAULT);
  }
}
