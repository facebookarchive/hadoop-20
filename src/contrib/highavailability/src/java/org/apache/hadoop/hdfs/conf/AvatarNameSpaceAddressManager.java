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

import static org.apache.hadoop.hdfs.conf.AvatarConfigurationKeys.AVATAR_PROTOCOL_PORT;
import static org.apache.hadoop.hdfs.conf.AvatarConfigurationKeys.CLIENT_PROTOCOL_PORT;
import static org.apache.hadoop.hdfs.conf.AvatarConfigurationKeys.DATANODE_PROTOCOL_PORT;
import static org.apache.hadoop.hdfs.conf.AvatarConfigurationKeys.DFS_AVATARNODE_ONE_ADDRESS;
import static org.apache.hadoop.hdfs.conf.AvatarConfigurationKeys.DFS_AVATARNODE_ZERO_ADDRESS;
import static org.apache.hadoop.hdfs.conf.AvatarConfigurationKeys.HTTP_PROTOCOL_PORT;
import static org.apache.hadoop.hdfs.protocol.FSConstants.DFS_CLUSTER_NAME;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.protocol.FSConstants;
import org.apache.hadoop.hdfs.protocol.AvatarConstants.InstanceId;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;

/**
 * This class is responsible for reading configuration with addresses and ports
 * as separate for all bindable socket address values.
 * 
 */
public class AvatarNameSpaceAddressManager extends NameSpaceAddressManager {
  private Log LOG = LogFactory.getLog(getClass());
  private static final String[] valuesManaged = new String[] {
      DFS_AVATARNODE_ZERO_ADDRESS, 
      DFS_AVATARNODE_ONE_ADDRESS,
      CLIENT_PROTOCOL_PORT, 
      AVATAR_PROTOCOL_PORT, 
      HTTP_PROTOCOL_PORT,
      DATANODE_PROTOCOL_PORT
  };
  private final String znodePath;

  public AvatarNameSpaceAddressManager(Configurable confg, String serviceName) {
    super(confg);
    znodePath = confg.getConf().get(DFS_CLUSTER_NAME) + "/" + serviceName;
    nodeZero = new AvatarNodeAddress(InstanceId.NODEZERO, serviceName, confg);
    nodeOne = new AvatarNodeAddress(InstanceId.NODEONE, serviceName, confg);
    try {
      // We would like to update the primary and standby references at construction
      // but 
      // not throw an exception here.
      refreshZookeeperInfo();
    } catch (IOException ioe) {
      LOG.error(ioe);
    } catch (KeeperException kpe) {
      LOG.error(kpe);
    } catch (InterruptedException ie) {
      LOG.error(ie);
    }
  }

  @Override
  public void setPrimary(InstanceId id) throws IOException {
    if (id == null) {
      zkClient.clearPrimary(znodePath);
    } else {
      zkClient.registerPrimary(znodePath, id.getZookeeeperValue(), true);
    }
    super.setPrimary(id);
  }

  @Override
  public void refreshZookeeperInfo() throws IOException, KeeperException,
      InterruptedException {
    String primaryNodeFromZk = zkClient.getPrimaryAvatarAddress(znodePath,
        new Stat(), false);
    if (primaryNodeFromZk == null) {
      // Fail over in progress
      super.setPrimary(null);
    } else if (primaryNodeFromZk.equals("zero")) {
      super.setPrimary(InstanceId.NODEZERO);
    } else if (primaryNodeFromZk.equals("one")) {
      super.setPrimary(InstanceId.NODEONE);
    } else {
      // inconsistent state
      DFSUtil.throwAndLogIllegalState("Zookeeper has an invalid value: "
          + primaryNodeFromZk, LOG);
    }
  }

  @Override
  public void validateConfigFile(Configuration conf) {
    Collection<String> allNameServices = DFSUtil.getNameServiceIds(conf);
    List<String> missingKeys = new LinkedList<String>();
    List<String> unWantedKeys = new LinkedList<String>();
    if (conf.get(FSConstants.DFS_CLUSTER_NAME) == null) {
      missingKeys.add(FSConstants.DFS_CLUSTER_NAME);
    }
    // Need ALL the federated keys
    for (String aNameService : allNameServices) {
      for (String value : valuesManaged) {
        String expectedKey = value + "." + aNameService;
        if (conf.get(expectedKey) == null) {
          missingKeys.add(expectedKey);
        }
      }
    }
    // Don't want any of the non-federated keys
    for (String value : valuesManaged) {
      String unexpectedKey = value;
      if (conf.get(unexpectedKey) != null) {
        unWantedKeys.add(unexpectedKey);
      }
    }
    if (missingKeys.isEmpty() && unWantedKeys.isEmpty()) {
      return;
    }
    LOG.error("Add these keys: ");
    for (String aMissingKey : missingKeys) {
      LOG.error("+" + aMissingKey);
    }
    LOG.error("Remove these keys: ");
    for (String anUnwantedKey : unWantedKeys) {
      LOG.error("-" + anUnwantedKey);
    }
    DFSUtil.throwAndLogIllegalState("Invalid Configuration file.", LOG);
  }

  @Override
  public String getTxidPath() throws UnknownHostException {
    return znodePath + ":txid";
  }

  @Override
  public String getSsidPath() throws UnknownHostException {
    return znodePath + ":ssid";
  }
}
