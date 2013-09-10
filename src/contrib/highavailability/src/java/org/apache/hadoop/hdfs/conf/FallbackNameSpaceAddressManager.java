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

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.protocol.AvatarConstants.InstanceId;
import org.apache.hadoop.hdfs.protocol.FSConstants;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.net.NetUtils;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;

public class FallbackNameSpaceAddressManager extends NameSpaceAddressManager {
  private String                      serviceName;
  private static final Log            LOG = LogFactory
                                              .getLog(NameSpaceAddressManager.class
                                                  .getName());
  private final AddressHolder<String> clientProtocolZnodeAddress;
  private final AddressHolder<String> datanodeProtocolZnodeAddress;
  private final AddressHolder<String> httpZnodeAddress;

  private class ConfGetter extends AddressHolder<String> {
    private String key;

    public ConfGetter(Configurable theParent, String configKey) {
      super(theParent);
      key = configKey;
    }

    @Override
    public String getAddress() throws UnknownHostException {
      return getConf().get(getKey());
    }

    @Override
    public String getKey() {
      return key + "." + serviceName;
    }
  }

  public FallbackNameSpaceAddressManager(Configurable parent, String serviceName) {
    super(parent);
    clientProtocolZnodeAddress = new ConfGetter(parent,
        NameNode.DFS_NAMENODE_RPC_ADDRESS_KEY);
    datanodeProtocolZnodeAddress = new ConfGetter(parent,
        NameNode.DATANODE_PROTOCOL_ADDRESS);
    httpZnodeAddress = new ConfGetter(parent,
        FSConstants.DFS_NAMENODE_HTTP_ADDRESS_KEY);
    this.serviceName = serviceName;
  }

  @Override
  public void refreshZookeeperInfo() throws IOException, KeeperException,
      InterruptedException {
    NameNodeAddress zero = getNodeZero();
    NameNodeAddress one = getNodeOne();
    // Using RPC Address to determine which node is primary and using the rest
    // of them as a sanity check
    String zNodeClientProtocol = clientProtocolZnodeAddress.getAddress();
    String primaryClientProtocol = zkClient.getPrimaryAvatarAddress(
        zNodeClientProtocol, new Stat(), false, false);
    InstanceId instanceIdfromZookeeper = null;
    if (primaryClientProtocol == null) {
      // Fail-over in progress.
      instanceIdfromZookeeper = null;
    } else {
      InetSocketAddress clientProtAddress = NetUtils
          .createSocketAddr(primaryClientProtocol);
      if (clientProtAddress.equals(zero.clientProtocol.getAddress())) {
        instanceIdfromZookeeper = InstanceId.NODEZERO;
      } else if (clientProtAddress.equals(one.clientProtocol.getAddress())) {
        instanceIdfromZookeeper = InstanceId.NODEONE;
      } else {
        throw new IllegalArgumentException(
            "Zookeeper client protocol address isn't valid: "
                + primaryClientProtocol);
      }
    }
    String zNodeHttpKey = httpZnodeAddress.getAddress();
    verifyAddress(
        zkClient.getPrimaryAvatarAddress(zNodeHttpKey, new Stat(), false),
        instanceIdfromZookeeper, zero.httpProtocol.getAddress(),
        one.httpProtocol.getAddress());
    String zNodeKeyForDnAddress = datanodeProtocolZnodeAddress.getAddress();
    verifyAddress(zkClient.getPrimaryAvatarAddress(zNodeKeyForDnAddress,
        new Stat(), false), instanceIdfromZookeeper,
        zero.datanodeProtocol.getAddress(), one.datanodeProtocol.getAddress());
    super.setPrimary(instanceIdfromZookeeper);
  }

  private static void verifyAddress(String address, InstanceId fromZk,
      InetSocketAddress zeroAddr, InetSocketAddress oneAddr) {
    InstanceId afterMatchingWithConfig = null;
    if (address == null) {
      afterMatchingWithConfig = null;
    } else {
      InetSocketAddress addressInIp = NetUtils.createSocketAddr(address);
      if (zeroAddr.equals(addressInIp)) {
        afterMatchingWithConfig = InstanceId.NODEZERO;
      } else if (oneAddr.equals(addressInIp)) {
        afterMatchingWithConfig = InstanceId.NODEONE;
      } else {
        DFSUtil
            .throwAndLogIllegalState(
                "Address doesn't match any of the possible candidates from the config file",
                LOG);
      }
    }
    if (afterMatchingWithConfig != fromZk) {
      DFSUtil
          .throwAndLogIllegalState(
              "One of the addresses of Zookeeper isn't in sync with other addresses.",
              LOG);
    }
  }

  @Override
  public void validateConfigFile(Configuration conf) {
    String[] keysOfInterest = new String[] {
        FSConstants.DFS_NAMENODE_HTTP_ADDRESS_KEY,
        NameNode.DATANODE_PROTOCOL_ADDRESS,
        FSConstants.DFS_NAMENODE_RPC_ADDRESS_KEY };
    boolean toThrowTheException = false;
    for (String key : keysOfInterest) {
      if (conf.get(key) == null) {
        LOG.error("Key not found: " + key);
        toThrowTheException = true;
      }
    }
    for (String key : keysOfInterest) {
      for (InstanceId id : InstanceId.values()) {
        String keyWeExpect = key + id.getZookeeeperValue() + "." + serviceName;
        if (conf.get(keyWeExpect) == null) {
          LOG.error("Key not found: " + keyWeExpect);
          toThrowTheException = true;
        }
      }
    }
    if (toThrowTheException) {
      throw new IllegalArgumentException(
          "Essential keys are missing from configuration.");
    }
  }

  @Override
  public String getTxidPath() throws UnknownHostException {
    return clientProtocolZnodeAddress.getAddress();
  }

  @Override
  public String getSsidPath() throws UnknownHostException {
    return getTxidPath();
  }

  public void setPrimary(InstanceId id) throws IOException {
    if (id == null) {
      zkClient.clearPrimary(clientProtocolZnodeAddress.getAddress());
      zkClient.clearPrimary(datanodeProtocolZnodeAddress.getAddress());
      zkClient.clearPrimary(httpZnodeAddress.getAddress());
    } else {
      NameNodeAddress addressesToWrite = null;
      switch (id) {
        case NODEZERO:
          addressesToWrite = getNodeZero();
        case NODEONE:
          addressesToWrite = getNodeOne();
      }
      final boolean toOverWrite = true;
      zkClient.registerPrimary(clientProtocolZnodeAddress.getAddress(),
          NameNode.getHostPortString(addressesToWrite.clientProtocol
              .getAddress()), toOverWrite);
      zkClient.registerPrimary(datanodeProtocolZnodeAddress.getAddress(),
          NameNode.getHostPortString(addressesToWrite.datanodeProtocol
              .getAddress()), toOverWrite);
      zkClient
          .registerPrimary(httpZnodeAddress.getAddress(), NameNode
              .getHostPortString(addressesToWrite.httpProtocol.getAddress()),
              toOverWrite);
    }
    super.setPrimary(id);
  }
}
