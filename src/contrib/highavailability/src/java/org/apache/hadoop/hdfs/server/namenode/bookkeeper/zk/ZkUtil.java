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


import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.zookeeper.KeeperException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Ported from jcommon-zookeeper, contains utility methods used by
 * {@link RecoveringZooKeeper}
 * https://github.com/facebook/jcommon/blob/master/zookeeper/src/main/java/com/facebook/zookeeper/ZkUtil.java
 */
public class ZkUtil {

  private static final Log LOG = LogFactory.getLog(ZkUtil.class);

  /**
   * Don't allow construction of this class
   */
  private ZkUtil() {
  }

  /**
   * Filters the given node list by the given prefixes.
   * This method is all-inclusive--if any element in the node list starts
   * with any of the given prefixes, then it is included in the result.
   *
   * @param nodes the nodes to filter
   * @param prefixes the prefixes to include in the result
   * @return list of every element that starts with one of the prefixes
   */
  public static List<String> filterByPrefix(
      List<String> nodes, String... prefixes
  ) {
    List<String> lockChildren = new ArrayList<String>();
    for (String child : nodes){
      for (String prefix : prefixes){
        if (child.startsWith(prefix)){
          lockChildren.add(child);
          break;
        }
      }
    }
    return lockChildren;
  }

  public static String joinPath(String parent, String child) {
    return parent + Path.SEPARATOR + child;
  }

  /**
   * Interrupt the thread and then log and re-throw an InterruptedException
   * as an IOException.
   * @param msg The message to log and include when re-throwing
   * @param e The actual exception instances
   * @throws IOException
   */
  public static void interruptedException(String msg, InterruptedException e)
      throws IOException {
    Thread.currentThread().interrupt();
    LOG.error(msg, e);
    throw new IOException(msg, e);
  }

  /**
   * Like {@link #interruptedException(String, InterruptedException)} but
   * handles KeeperException and will not interrupt the thread.
   */
  public static void keeperException(String msg, KeeperException e)
      throws IOException {
    LOG.error(msg, e);
    throw new IOException(msg, e);
  }

  /**
   * Recursively deletes a ZNode (i.e., delete all the ZNode's children and
   * then deletes the ZNode itself). Equivalent to 'rmr' command in
   * ZooKeeper CLI.
   *
   * @param zk ZooKeeper instance
   * @param path Path to delete
   * @throws IOException If there is an error recursively deleting a ZNode
   */
  public static void deleteRecursively(ZooKeeperIface zk, String path)
      throws IOException {
    try {
      List<String> children = zk.getChildren(path, false);
      for (String child : children) {
        deleteRecursively(zk, joinPath(path, child));
      }
      zk.delete(path, -1);
    } catch (KeeperException e) {
      keeperException("Unrecoverable ZooKeeper error deleting " + path, e);
    } catch (InterruptedException e) {
      interruptedException("Interrupted deleting " + path, e);
    }
  }

}