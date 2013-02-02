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
package org.apache.hadoop.hdfs.server.namenode.bookkeeper.zk;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;

import java.util.List;

/**
 * ZooKeeper interface limited to some basic operations. More ZooKeeper methods
 * may be added to this interface as needed.
 * See ZooKeeper API for documentation.
 * </p>
 * Ported from jcommon-zookeeper:
 * https://github.com/facebook/jcommon/blob/master/zookeeper/src/main/java/com/facebook/zookeeperZooKeeperIface.java
 */
public interface ZooKeeperIface {
  long getSessionId();

  void close() throws InterruptedException;

  String create(String path, byte[] data, List<ACL> acl, CreateMode createMode)
      throws KeeperException, InterruptedException;

  void delete(String path, int version)
      throws InterruptedException, KeeperException;

  Stat exists(String path, Watcher watcher)
      throws KeeperException, InterruptedException;

  Stat exists(String path, boolean watch)
      throws KeeperException, InterruptedException;

  byte[] getData(String path, Watcher watcher, Stat stat)
      throws KeeperException, InterruptedException;

  byte[] getData(String path, boolean watch, Stat stat)
      throws KeeperException, InterruptedException;

  Stat setData(String path, byte[] data, int version)
      throws KeeperException, InterruptedException;

  List<String> getChildren(String path, Watcher watcher)
      throws KeeperException, InterruptedException;

  List<String> getChildren(String path, boolean watch)
      throws KeeperException, InterruptedException;

  ZooKeeper.States getState();
}
