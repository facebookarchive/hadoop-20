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

/**
 * All the configuration keys for Avatar classes.
 * 
 */
public interface AvatarConfigurationKeys {

  public static final String DFS_AVATARNODE_ZERO_ADDRESS = "dfs.avatarnode.zero.address";
  public static final String DFS_AVATARNODE_ONE_ADDRESS = "dfs.avatarnode.one.address";

  public static final String CLIENT_PROTOCOL_PORT = "dfs.avatarnode.clientprotocol.port";
  public static final String AVATAR_PROTOCOL_PORT = "dfs.avatarnode.avatarprotocol.port";
  public static final String HTTP_PROTOCOL_PORT = "dfs.avatarnode.http.port";
  public static final String DATANODE_PROTOCOL_PORT = "dfs.avatarnode.datanodeprotocol.port";

  /**
   * Use for binding the avatarprotocol server on this port.
   */
  @Deprecated
  public static final String AVATARNODE_PORT = "dfs.avatarnode.port";
}
