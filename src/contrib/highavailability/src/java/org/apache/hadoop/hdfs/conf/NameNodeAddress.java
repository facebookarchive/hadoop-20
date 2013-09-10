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

import java.net.InetSocketAddress;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.protocol.AvatarConstants.InstanceId;

/**
 * This class ties down the addresses of a NameNode.
 * 
 */
public abstract class NameNodeAddress {
  public AddressHolder<InetSocketAddress> clientProtocol;
  public AddressHolder<InetSocketAddress> httpProtocol;
  public AddressHolder<InetSocketAddress> avatarProtocol;
  public AddressHolder<InetSocketAddress> datanodeProtocol;

  private Configurable parent;
  private String serviceName;
  private InstanceId ofNodeBeingManaged;

  protected NameNodeAddress(InstanceId id, String nameService,
      Configurable parent) {
    this.parent = parent;
    this.serviceName = nameService;
    this.ofNodeBeingManaged = id;
  }

  protected Configuration getConf() {
    return parent.getConf();
  }

  protected String getServiceName() {
    return serviceName;
  }

  protected InstanceId getInstanceId() {
    return ofNodeBeingManaged;
  }
}
