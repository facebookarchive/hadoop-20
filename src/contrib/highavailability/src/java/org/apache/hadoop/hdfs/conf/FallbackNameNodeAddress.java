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
package org.apache.hadoop.hdfs.conf;

import static org.apache.hadoop.hdfs.protocol.FSConstants.DFS_NAMENODE_HTTP_ADDRESS_KEY;
import static org.apache.hadoop.hdfs.protocol.FSConstants.DFS_NAMENODE_RPC_ADDRESS_KEY;
import static org.apache.hadoop.hdfs.server.namenode.NameNode.DATANODE_PROTOCOL_ADDRESS;
import static org.apache.hadoop.hdfs.conf.AvatarConfigurationKeys.AVATARNODE_PORT;

import java.net.InetSocketAddress;
import java.net.UnknownHostException;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.hdfs.protocol.AvatarConstants.InstanceId;
import org.apache.hadoop.net.NetUtils;

/**
 *  This class performs the fall back behavior of configuration files.
 *
 */
public class FallbackNameNodeAddress extends NameNodeAddress {

  public FallbackNameNodeAddress(InstanceId id, String nameService,
      Configurable parent) {
    super(id, nameService, parent);
    clientProtocol = new ConfigAddressHolder(DFS_NAMENODE_RPC_ADDRESS_KEY,
        parent);
    avatarProtocol = new AddressHolder<InetSocketAddress>(parent) {
      @Override
      public String getKey() {
        return AVATARNODE_PORT;
      }

      @Override
      public InetSocketAddress getAddress() throws UnknownHostException {
        InetSocketAddress clientProtocolAddress = clientProtocol.getAddress();
        return new InetSocketAddress(clientProtocolAddress.getAddress(),
            getConf().getInt(AVATARNODE_PORT,
                clientProtocolAddress.getPort() + 1));
      }
    };
    httpProtocol = new ConfigAddressHolder(DFS_NAMENODE_HTTP_ADDRESS_KEY, parent);
    datanodeProtocol = new ConfigAddressHolder(DATANODE_PROTOCOL_ADDRESS,
        parent);
  }

  /**
   * This address holder represents the current regex of our keys,
   *   
   *   [Some-address-key-name] {0 | 1} . {nameService}
   * 
   *  This class just gets the socket addresses from the config file.
   */
  private class ConfigAddressHolder extends AddressHolder<InetSocketAddress> {
    private final String configKey;

    public ConfigAddressHolder(String configKey, Configurable theParent) {
      super(theParent);
      this.configKey = configKey;
    }

    @Override
    public InetSocketAddress getAddress() throws UnknownHostException {
      return NetUtils.createSocketAddr(getConf().get(getKey()));
    }

    @Override
    public String getKey() {
      return configKey + getInstanceId().getValue() + "." + getServiceName();
    }
  }
}
