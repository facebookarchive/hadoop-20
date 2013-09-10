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

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.net.InetSocketAddress;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.hdfs.protocol.AvatarConstants.InstanceId;
import static org.apache.hadoop.hdfs.conf.AvatarConfigurationKeys.*;

/**
 * This class bundles together all ports and the IP address of an avatarnode
 * 
 */
public class AvatarNodeAddress extends NameNodeAddress {
  private InetAddressHolder nodeAddress;
  private String serviceName;

  protected AvatarNodeAddress(InstanceId id, String nameService,
      Configurable parent) {
    super(id, nameService, parent);
    this.serviceName = nameService;
    this.nodeAddress = new InetAddressHolder(parent, id);
    this.clientProtocol = new AvatarAddressHolder(parent, nodeAddress,
        CLIENT_PROTOCOL_PORT);
    this.avatarProtocol = new AvatarAddressHolder(parent, nodeAddress,
        AVATAR_PROTOCOL_PORT);
    this.datanodeProtocol = new AvatarAddressHolder(parent, nodeAddress,
        DATANODE_PROTOCOL_PORT);
    this.httpProtocol = new AvatarAddressHolder(parent, nodeAddress,
        HTTP_PROTOCOL_PORT);
  }

  /**
   * Gets avatarnode.zero.address
   *      avatarnode.one.address
   *
   */
  private class InetAddressHolder extends AddressHolder<InetAddress> {
    private final InstanceId instanceResponsibleFor;

    public InetAddressHolder(Configurable theParent, InstanceId ofTheNode) {
      super(theParent);
      instanceResponsibleFor = ofTheNode;
    }

    @Override
    public InetAddress getAddress() throws UnknownHostException {
      return InetAddress.getByName(this.getConf().get(getKey()));
    }

    public String getKey() {
      return instanceResponsibleFor.getConfigKey() + "." + serviceName;
    }
  }

  /**
   * Philosophy: Every AddressHolder object should be responsible for extracting
   * one and only one key out of the configuration file.
   * However, to get SocketAddresses to bind to, we need to 
   * two pieces of information, 
   *  1. IP address to bind to
   *  2. Port to bind to
   *  
   *  IP address to bind to is source by InetAddressHolder
   *  Now, this class gets the port and glues it to the IP address to form
   *  an InetSocketAddress object
   *
   *
   */
  private class AvatarAddressHolder extends AddressHolder<InetSocketAddress> {
    private final InetAddressHolder sourceOfAddress;
    private final String keyForPort;

    public AvatarAddressHolder(Configurable parent,
        InetAddressHolder nodeAddress, String portKey) {
      super(parent);
      sourceOfAddress = nodeAddress;
      keyForPort = portKey;
    }

    @Override
    public InetSocketAddress getAddress() throws UnknownHostException {
      return new InetSocketAddress(sourceOfAddress.getAddress(), this.getConf()
          .getInt(getKey(), 0));
    }

    @Override
    public String getKey() {
      return keyForPort + "." + serviceName;
    }
  }
}
