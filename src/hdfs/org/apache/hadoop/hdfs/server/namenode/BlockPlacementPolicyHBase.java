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

package org.apache.hadoop.hdfs.server.namenode;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.server.namenode.BlocksMap.BlockInfo;
import org.apache.hadoop.net.DNSToSwitchMapping;
import org.apache.hadoop.net.NetworkTopology;
import org.apache.hadoop.util.HostsFileReader;

public class BlockPlacementPolicyHBase extends BlockPlacementPolicyConfigurable {

  private static final int ITERATE_THROUGH_BLOCKS_THRESHOLD = 500;
  private static final String MD5_CODE_REGEX = "[[a-f][0-9]]{32}";
  private static final String HBASE_DIRECTORY_REGEX = ".*HBASE";
  private static final String TABLE_NAME_REGEX = "[[a-z][A-Z][0-9]_][[a-z][A-Z][0-9]_\\.\\-]*";
  private static final String REGION_NAME_REGEX = MD5_CODE_REGEX;
  private static final String COLUMN_FAMILY_NAME_REGEX = "[^\\.:][^:]*";
  private static final String HFILE_REGEX = MD5_CODE_REGEX;
  private static final String HBASE_FILE_REGEX = Path.SEPARATOR + HBASE_DIRECTORY_REGEX + Path.SEPARATOR
      + TABLE_NAME_REGEX + Path.SEPARATOR + REGION_NAME_REGEX + Path.SEPARATOR
      + COLUMN_FAMILY_NAME_REGEX + Path.SEPARATOR + HFILE_REGEX; // /*HBASE/TableName/RegionName/ColumnFamily/HFile

  private FSNamesystem nameSystem;

  BlockPlacementPolicyHBase() {
  }

  /** {@inheritDoc} */
  public void initialize(Configuration conf, FSClusterStats stats, NetworkTopology clusterMap,
      HostsFileReader hostsReader, DNSToSwitchMapping dnsToSwitchMapping, FSNamesystem ns) {
    this.nameSystem = ns;
    super.initialize(conf, stats, clusterMap, hostsReader, dnsToSwitchMapping, ns);
  }

  /**
   * For HBase, we try not to delete from datanodes that were originally from
   * the favored nodes, but, since, the original favored nodes are not stored
   * anywhere, our strategy is to iterate through files in the same directory,
   * choose the least frequently used datanode for deletion.
   * However since there might be a large number of datanodes in the same
   * directory, we only iterate through a particular number of blocks (
   * {@link #ITERATE_THROUGH_BLOCKS_THRESHOLD}).
   *
   * Also, since, favored nodes may change over time, we iterate through
   * files in the reverse creation order.
   */
  public DatanodeDescriptor chooseReplicaToDelete(FSInodeInfo inode, Block block,
      short replicationFactor, Collection<DatanodeDescriptor> first,
      Collection<DatanodeDescriptor> second) {

    try {
      if (!(inode instanceof INodeFile) || !inode.getFullPathName().matches(HBASE_FILE_REGEX)) {
        return super.chooseReplicaToDelete(inode, block, replicationFactor, first, second);
      }
    } catch (IOException e) {
      if (NameNode.LOG.isDebugEnabled()) {
        NameNode.LOG.debug("Couldn't get full path name. " + e.getMessage());
      }
      return super.chooseReplicaToDelete(inode, block, replicationFactor, first, second);
    }

    INodeFile inodeFile = (INodeFile) inode;

    INodeDirectory parent = inodeFile.getParent();

    if (parent == null) { // Probably it's renamed or removed
      return super.chooseReplicaToDelete(inodeFile, block, replicationFactor, first, second);
    }

    // A map from datanodes to the number of usages in the same directory.
    HashMap<DatanodeDescriptor, Integer> dataNodeUsage = directoryDataNodeUsage(parent,
        ITERATE_THROUGH_BLOCKS_THRESHOLD);

    // Now pick the datanode with the least usage. In case of equal
    // usages, priority is to pick the one from the first array.
    DatanodeDescriptor minUsageInstance = getMinUsage(first, Integer.MAX_VALUE, null, dataNodeUsage);
    // Get the value of minimum usage from first nodes to compare with second
    // nodes
    int minUsage = (minUsageInstance != null) ? dataNodeUsage.get(minUsageInstance)
        : Integer.MAX_VALUE;
    // To avoid putting all nodes in the same rack:
    if (minUsageInstance == null || second.size() > 1 || nodeOnMultipleRacks(first)) {
      minUsageInstance = getMinUsage(second, minUsage, minUsageInstance, dataNodeUsage);
    }

    return minUsageInstance;
  }

  /**
   * Checks that all the nodes in <code>nodes</code> are from the same rack or
   * not.
   * Result will be false only if all the nodes are from the same rack,
   * otherwise true.
   */
  private boolean nodeOnMultipleRacks(Collection<DatanodeDescriptor> nodes) {
    DatanodeDescriptor previous = null;
    for (DatanodeDescriptor node : nodes) {
      if (previous != null && !previous.getNetworkLocation().equals(node.getNetworkLocation())) {
        return true;
      }
      previous = node;
    }
    return false;
  }

  /**
   * Finds DatanodeDescriptor from the list <code>nodes</code> where its value
   * in <code>usageMap</code> is the minimum, if the minimum value is greater
   * than or equal to <code>minUsageDefault</code>,
   * <code>minUsageInstanceDefault</code> will be returned, otherwise the
   * DatanodeDescriptor with the minimum value will be returned.
   */
  private DatanodeDescriptor getMinUsage(Collection<DatanodeDescriptor> nodes, int minUsageDefault,
      DatanodeDescriptor minUsageInstanceDefault, HashMap<DatanodeDescriptor, Integer> usageMap) {
    for (DatanodeDescriptor dnd : nodes) {
      Integer usage = usageMap.get(dnd);
      if (usage == null) {
        usage = 0;
      }
      if (usage < minUsageDefault) {
        minUsageDefault = usage;
        minUsageInstanceDefault = dnd;
      }
    }
    return minUsageInstanceDefault;
  }

  /**
   * Iterates through files in the directory dir, and counts the number
   * of usages of datanodes for the files in the directory dir.
   * Just iterates through threshold number of blocks.
   */
  private HashMap<DatanodeDescriptor, Integer> directoryDataNodeUsage(INodeDirectory dir,
      int threshold) {
    HashMap<DatanodeDescriptor, Integer> dataNodeUsage = new HashMap<DatanodeDescriptor, Integer>();

    List<INode> children;
    nameSystem.readLock();
    try {
      if (dir.getChildrenRaw() == null) {
        return dataNodeUsage;
      }
      children = new ArrayList<INode>(dir.getChildrenRaw());
      Collections.shuffle(children);

      for (INode node : children) {
        if (!(node instanceof INodeFile)) { // The condition is always false.
          continue;
        }

        INodeFile file = (INodeFile) node;
        BlockInfo[] blocks = file.getBlocks();

        for (BlockInfo block : blocks) {
          if (threshold == 0) {
            return dataNodeUsage;
          }

          int replication = block.numNodes();
          for (int i = 0; i < replication; i++) {
            DatanodeDescriptor datanode = block.getDatanode(i);
            Integer currentUsage = dataNodeUsage.get(datanode);
            dataNodeUsage.put(datanode, currentUsage == null ? 1 : currentUsage + 1);
          }
          threshold--;
        }
      }
    } finally {
      nameSystem.readUnlock();
    }
    return dataNodeUsage;
  }
}
