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
package org.apache.hadoop.hdfs.protocol;

import static org.apache.hadoop.hdfs.conf.AvatarConfigurationKeys.DFS_AVATARNODE_ONE_ADDRESS;
import static org.apache.hadoop.hdfs.conf.AvatarConfigurationKeys.DFS_AVATARNODE_ZERO_ADDRESS;

import java.net.InetSocketAddress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.server.namenode.AvatarNodeZkUtil;
import org.apache.hadoop.hdfs.server.namenode.NameNode;

/**
 * Some global definitions for AvatarNode.
 */

public interface AvatarConstants {

  static interface AddressGetter {
    public InetSocketAddress getAddress(Configuration conf);
  }

  /**
   * Various types entries within Zookeeper
   */
  static public enum ZookeeperKey {

    CLIENT_PROTOCOL_ADDRESS("cpa", new AddressGetter() {
      @Override
      public InetSocketAddress getAddress(Configuration conf) {
        return NameNode.getClientProtocolAddress(conf);
      }
    }),

    DATANODE_PROTOCOL_ADDRESS("dnpa", new AddressGetter() {
      @Override
      public InetSocketAddress getAddress(Configuration conf) {
        return NameNode.getDNProtocolAddress(conf);
      }
    }),

    HTTP_SERVER_ADDRESS("hsa", new AddressGetter() {
      @Override
      public InetSocketAddress getAddress(Configuration conf) {
        return NameNode.getHttpServerAddress(conf);
      }
    });

    private final String appendix;
    private final AddressGetter addressGetter;

    private ZookeeperKey(String s, AddressGetter g) {
      appendix = s;
      addressGetter = g;
    }

    public String getIpPortString(Configuration conf) {
      return AvatarNodeZkUtil.toIpPortString(addressGetter.getAddress(conf));
    }

    @Override
    public String toString() {
      return "." + appendix;
    }
  }

  /**
   * Define the various avatars of the NameNode.
   */
  static public enum Avatar {
    ACTIVE("Primary"),
    STANDBY("Standby"),
    UNKNOWN("UnknownAvatar");

    public static Avatar[] avatars = new Avatar[] { ACTIVE, STANDBY };

    private String description = null;
    private Avatar(String arg) {this.description = arg;}

    public String toString() {
      return description;
    }
  }

  /**
   * Define unique names for the instances of the AvatarNode.
   * At present, there can be only two.
   */
  static public enum InstanceId {
    NODEZERO ("FirstNode", 0, "zero", DFS_AVATARNODE_ZERO_ADDRESS),
    NODEONE  ("SecondNode", 1, "one", DFS_AVATARNODE_ONE_ADDRESS);
    private String       description = null;
    private int          val;
    private final String configKey;
    private final String valInWords;

    private InstanceId(String arg, int aValue, String valInWords,
        String configKeyString) {
      this.description = arg;
      val = aValue;
      configKey = configKeyString;
      this.valInWords = valInWords;
    }

    public String toString() {
      return description;
    }

    public int getValue() {
      return val;
    }

    public String getConfigKey() {
      return configKey;
    }

    public String getZookeeeperValue() {
      return valInWords;
    }
  }

  /** Startup options */
  static public enum StartupOption {
    NODEZERO("-zero"),
    NODEONE("-one"),
    SYNC("-sync"),
    ACTIVE("-active"),
    STANDBY("-standby"),
    FORMAT("-format"), // these are namenode options
    FORMATFORCE("-formatforce"),
    REGULAR("-regular"),
    UPGRADE("-upgrade"),
    ROLLBACK("-rollback"),
    FINALIZE("-finalize"),
    IMPORT("-importCheckpoint"),
    FORCE("-forceStartup"),
    SERVICE("-service");

    private String name = null;

    private StartupOption(String arg) {this.name = arg;}
    public String getName() {return name;}

    public Avatar toAvatar() {
      switch(this) {
        case STANDBY:
          return Avatar.STANDBY;
        case ACTIVE:
          return Avatar.ACTIVE;
        default:
          return Avatar.UNKNOWN;
      }
    }
  }
}
