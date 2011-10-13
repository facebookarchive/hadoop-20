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
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.server.namenode.BlockPlacementPolicyDefault;
import org.apache.hadoop.net.DNSToSwitchMapping;
import org.apache.hadoop.net.NetworkTopology;
import org.apache.hadoop.net.Node;
import org.apache.hadoop.raid.RaidNode;
import org.apache.hadoop.util.HostsFileReader;
import org.apache.hadoop.util.StringUtils;

/**
 * This BlockPlacementPolicy uses a simple heuristic, random placement of
 * the replicas of a newly-created block, for the purpose of spreading out the 
 * group of blocks which used by RAID for recovering each other. 
 * This is important for the availability of the blocks. 
 * 
 * Replication of an existing block continues to use the default placement
 * policy.
 * 
 * This simple block placement policy does not guarantee that
 * blocks on the RAID stripe are on different nodes. However, BlockMonitor
 * will periodically scans the raided files and will fix the placement
 * if it detects violation. 
 * 
 * This class can be used by multiple threads. It has to be thread safe.
 */
public class BlockPlacementPolicyRaid extends BlockPlacementPolicyDefault {
  public static final Log LOG =
    LogFactory.getLog(BlockPlacementPolicyRaid.class);
  Configuration conf;
  private int stripeLength;
  private int xorParityLength;
  private int rsParityLength;
  private String xorPrefix = null;
  private String rsPrefix = null;
  private String raidTempPrefix = null;
  private String raidrsTempPrefix = null;
  private String raidHarTempPrefix = null;
  private String raidrsHarTempPrefix = null;
  private FSNamesystem namesystem = null;

  private CachedLocatedBlocks cachedLocatedBlocks;
  private CachedFullPathNames cachedFullPathNames;

  /** {@inheritDoc} */
  @Override
  public void initialize(Configuration conf,  FSClusterStats stats,
                         NetworkTopology clusterMap, HostsFileReader hostsReader,
                         DNSToSwitchMapping dnsToSwitchMapping, FSNamesystem namesystem) {
    super.initialize(conf, stats, clusterMap, 
                     hostsReader, dnsToSwitchMapping, namesystem);
    this.conf = conf;
    this.namesystem = namesystem;
    this.stripeLength = RaidNode.getStripeLength(conf);
    this.rsParityLength = RaidNode.rsParityLength(conf);
    this.xorParityLength = 1;
    this.cachedLocatedBlocks = new CachedLocatedBlocks(conf, namesystem);
    this.cachedFullPathNames = new CachedFullPathNames(conf, namesystem);
    try {
      this.xorPrefix = RaidNode.xorDestinationPath(conf).toUri().getPath();
      this.rsPrefix = RaidNode.rsDestinationPath(conf).toUri().getPath();
    } catch (IOException e) {
    }
    if (this.xorPrefix == null) {
      this.xorPrefix = RaidNode.DEFAULT_RAID_LOCATION;
    }
    if (this.rsPrefix == null) {
      this.rsPrefix = RaidNode.DEFAULT_RAIDRS_LOCATION;
    }
    this.raidTempPrefix = RaidNode.xorTempPrefix(conf);
    this.raidrsTempPrefix = RaidNode.rsTempPrefix(conf);
    this.raidHarTempPrefix = RaidNode.xorHarTempPrefix(conf);
    this.raidrsHarTempPrefix = RaidNode.rsHarTempPrefix(conf);
  }

  @Override
  public DatanodeDescriptor[] chooseTarget(String srcPath, int numOfReplicas,
      DatanodeDescriptor writer, List<DatanodeDescriptor> chosenNodes,
      long blocksize) {
    try {
      FileType type = getFileType(srcPath);
      if (type == FileType.NOT_RAID) {
        return super.chooseTarget(
            srcPath, numOfReplicas, writer, chosenNodes, blocksize);
      }
      ArrayList<DatanodeDescriptor> results = new ArrayList<DatanodeDescriptor>();
      HashMap<Node, Node> excludedNodes = new HashMap<Node, Node>();
      for (Node node:chosenNodes) {
        excludedNodes.put(node, node);
      }
      chooseRandom(numOfReplicas, "/", excludedNodes, blocksize,
          1, results);
      return results.toArray(new DatanodeDescriptor[results.size()]);
    } catch (Exception e) {
      FSNamesystem.LOG.debug(
        "Error happend when choosing datanode to write:" +
        StringUtils.stringifyException(e));
      return super.chooseTarget(srcPath, numOfReplicas, writer,
                                chosenNodes, blocksize);
    }
  }

  /** {@inheritDoc} */
  @Override
  public DatanodeDescriptor chooseReplicaToDelete(FSInodeInfo inode,
      Block block, short replicationFactor,
      Collection<DatanodeDescriptor> first,
      Collection<DatanodeDescriptor> second) {

    DatanodeDescriptor chosenNode = null;
    try {
      String path = cachedFullPathNames.get(inode);
      FileType type = getFileType(path);
      if (type == FileType.NOT_RAID) {
        return super.chooseReplicaToDelete(
            inode, block, replicationFactor, first, second);
      }
      List<LocatedBlock> companionBlocks =
          getCompanionBlocks(path, type, block);
      if (companionBlocks == null || companionBlocks.size() == 0) {
        // Use the default method if it is not a valid raided or parity file
        return super.chooseReplicaToDelete(
            inode, block, replicationFactor, first, second);
      }
      // Delete from the first collection first
      // This ensures the number of unique rack of this block is not reduced
      Collection<DatanodeDescriptor> all = new HashSet<DatanodeDescriptor>();
      all.addAll(first);
      all.addAll(second);
      chosenNode = chooseReplicaToDelete(companionBlocks, all);
      if (chosenNode != null) {
        return chosenNode;
      }
      return super.chooseReplicaToDelete(
          inode, block, replicationFactor, first, second);
    } catch (Exception e) {
      LOG.debug("Failed to choose the correct replica to delete", e);
      return super.chooseReplicaToDelete(
          inode, block, replicationFactor, first, second);
    }
  }

  /**
   * Obtain the excluded nodes for the current block that is being written
   */
  List<Node> getExcludedNodes(String file, FileType type) throws IOException {
    Set<Node> excluded = new HashSet<Node>();
    Collection<LocatedBlock> blocks = getCompanionBlocks(file, type, null);
    if (blocks != null) {
      for (LocatedBlock b : blocks) {
        for (Node n : b.getLocations()) {
          excluded.add(n);
        }
      }
    }
    return new ArrayList<Node>(excluded);
  }

  private DatanodeDescriptor chooseReplicaToDelete(
      Collection<LocatedBlock> companionBlocks,
      Collection<DatanodeDescriptor> dataNodes) throws IOException {

    if (dataNodes.isEmpty()) {
      return null;
    }
    // Count the number of replicas on each node and rack
    final Map<String, Integer> nodeCompanionBlockCount =
      countCompanionBlocks(companionBlocks, false);
    final Map<String, Integer> rackCompanionBlockCount =
      countCompanionBlocks(companionBlocks, true);

    NodeComparator comparator =
      new NodeComparator(nodeCompanionBlockCount, rackCompanionBlockCount);
    return Collections.max(dataNodes, comparator);
  }

  /**
   * Count how many companion blocks are on each datanode or the each rack
   * @param companionBlocks a collection of all the companion blocks
   * @param doRackCount count the companion blocks on the racks of datanodes
   * @param result the map from node name to the number of companion blocks
   */
  static Map<String, Integer> countCompanionBlocks(
      Collection<LocatedBlock> companionBlocks, boolean doRackCount) {
    Map<String, Integer> result = new HashMap<String, Integer>();
    for (LocatedBlock block : companionBlocks) {
      for (DatanodeInfo d : block.getLocations()) {
        String name = doRackCount ? d.getParent().getName() : d.getName();
        if (result.containsKey(name)) {
          int count = result.get(name) + 1;
          result.put(name, count);
        } else {
          result.put(name, 1);
        }
      }
    }
    return result;
  }

  /**
   * Compares the datanodes based on the number of companion blocks on the same
   * node and rack. If even, compare the remaining space on the datanodes.
   */
  class NodeComparator implements Comparator<DatanodeDescriptor> {
    private Map<String, Integer> nodeBlockCount;
    private Map<String, Integer> rackBlockCount;
    private NodeComparator(Map<String, Integer> nodeBlockCount,
                           Map<String, Integer> rackBlockCount) {
      this.nodeBlockCount = nodeBlockCount;
      this.rackBlockCount = rackBlockCount;
    }
    @Override
    public int compare(DatanodeDescriptor d1, DatanodeDescriptor d2) {
      int res = compareBlockCount(d1, d2, nodeBlockCount);
      if (res != 0) {
        return res;
      }
      res = compareBlockCount(d1.getParent(), d2.getParent(), rackBlockCount);
      if (res != 0) {
        return res;
      }
      if (d1.getRemaining() > d2.getRemaining()) {
        return -1;
      }
      if (d1.getRemaining() < d2.getRemaining()) {
        return 1;
      }
      return 0;
    }
    private int compareBlockCount(Node node1, Node node2,
                                  Map<String, Integer> blockCount) {
      Integer count1 = blockCount.get(node1.getName());
      Integer count2 = blockCount.get(node2.getName());
      count1 = count1 == null ? 0 : count1;
      count2 = count2 == null ? 0 : count2;
      if (count1 > count2) {
        return 1;
      }
      if (count1 < count2) {
        return -1;
      }
      return 0;
    }
  }

  /**
   * Obtain the companion blocks of the give block
   * Companion blocks are defined as the blocks that can help recover each
   * others by using raid decoder.
   * @param path The path of the file contains the block
   * @param type The type of this file
   * @param block The given block
   *              null if it is the block which is currently being written to
   * @return the block locations of companion blocks
   */
  List<LocatedBlock> getCompanionBlocks(String path, FileType type, Block block)
      throws IOException {
    switch (type) {
      case NOT_RAID:
        return Collections.emptyList();
      case XOR_HAR_TEMP_PARITY:
        return getCompanionBlocksForHarParityBlock(
            path, xorParityLength, block);
      case RS_HAR_TEMP_PARITY:
        return getCompanionBlocksForHarParityBlock(
            path, rsParityLength, block);
      case XOR_TEMP_PARITY:
        return getCompanionBlocksForParityBlock(
            getSourceFile(path, raidTempPrefix), path, xorParityLength, block);
      case RS_TEMP_PARITY:
        return getCompanionBlocksForParityBlock(
            getSourceFile(path, raidrsTempPrefix), path, rsParityLength, block);
      case XOR_PARITY:
        return getCompanionBlocksForParityBlock(getSourceFile(path, xorPrefix),
            path, xorParityLength, block);
      case RS_PARITY:
        return getCompanionBlocksForParityBlock(getSourceFile(path, rsPrefix),
            path, rsParityLength, block);
      case XOR_SOURCE:
        return getCompanionBlocksForSourceBlock(
            path, getParityFile(path), xorParityLength, block);
      case RS_SOURCE:
        return getCompanionBlocksForSourceBlock(
            path, getParityFile(path), xorParityLength, block);
    }
    return Collections.emptyList();
  }

  private List<LocatedBlock> getCompanionBlocksForHarParityBlock(
      String parity, int parityLength, Block block)
      throws IOException {
    int blockIndex = getBlockIndex(parity, block);
    // consider only parity file in this case because source file block
    // location is not easy to obtain
    List<LocatedBlock> parityBlocks = cachedLocatedBlocks.get(parity);
    List<LocatedBlock> result = new ArrayList<LocatedBlock>();
    synchronized (parityBlocks) {
      int start = Math.max(0, blockIndex - parityLength + 1);
      int end = Math.min(parityBlocks.size(), blockIndex + parityLength);
      result.addAll(parityBlocks.subList(start, end));
    }
    return result;
  }

  private List<LocatedBlock> getCompanionBlocksForParityBlock(
      String src, String parity, int parityLength, Block block)
      throws IOException {
    int blockIndex = getBlockIndex(parity, block);
    List<LocatedBlock> result = new ArrayList<LocatedBlock>();
    List<LocatedBlock> parityBlocks = cachedLocatedBlocks.get(parity);
    int stripeIndex = blockIndex / parityLength;
    synchronized (parityBlocks) {
      int parityStart = stripeIndex * parityLength;
      int parityEnd = Math.min(parityStart + parityLength,
                               parityBlocks.size());
      // for parity, always consider the neighbor blocks as companion blocks
      if (parityStart < parityBlocks.size()) {
        result.addAll(parityBlocks.subList(parityStart, parityEnd));
      }
    }

    if (src == null) {
      return result;
    }
    List<LocatedBlock> sourceBlocks = cachedLocatedBlocks.get(src);
    synchronized (sourceBlocks) {
      int sourceStart = stripeIndex * stripeLength;
      int sourceEnd = Math.min(sourceStart + stripeLength,
                               sourceBlocks.size());
      if (sourceStart < sourceBlocks.size()) {
        result.addAll(sourceBlocks.subList(sourceStart, sourceEnd));
      }
    }
    return result;
  }

  private List<LocatedBlock> getCompanionBlocksForSourceBlock(
      String src, String parity, int parityLength, Block block)
      throws IOException {
    int blockIndex = getBlockIndex(src, block);
    List<LocatedBlock> result = new ArrayList<LocatedBlock>();
    List<LocatedBlock> sourceBlocks = cachedLocatedBlocks.get(src);
    int stripeIndex = blockIndex / stripeLength;
    synchronized (sourceBlocks) {
      int sourceStart = stripeIndex * stripeLength;
      int sourceEnd = Math.min(sourceStart + stripeLength,
                               sourceBlocks.size());
      if (sourceStart < sourceBlocks.size()) {
        result.addAll(sourceBlocks.subList(sourceStart, sourceEnd));
      }
    }
    if (parity == null) {
      return result;
    }
    List<LocatedBlock> parityBlocks = cachedLocatedBlocks.get(parity);
    synchronized (parityBlocks) {
      int parityStart = stripeIndex * parityLength;
      int parityEnd = Math.min(parityStart + parityLength,
                               parityBlocks.size());
      if (parityStart < parityBlocks.size()) {
        result.addAll(parityBlocks.subList(parityStart, parityEnd));
      }
    }
    return result;
  }

  private int getBlockIndex(String file, Block block) throws IOException {
    List<LocatedBlock> blocks = cachedLocatedBlocks.get(file);
    synchronized (blocks) {
      // null indicates that this block is currently added. Return size()
      // as the index in this case
	    if (block == null) {
	      return blocks.size();
	    }
      for (int i = 0; i < blocks.size(); i++) {
        if (blocks.get(i).getBlock().equals(block)) {
          return i;
        }
      }
    }
    throw new IOException("Cannot locate " + block + " in file " + file);
  }

  /**
   * Cache results for FSInodeInfo.getFullPathName()
   */
  static class CachedFullPathNames {
    private Cache<INodeWithHashCode, String> cacheInternal;
    private FSNamesystem namesystem;

    CachedFullPathNames(final Configuration conf, final FSNamesystem namesystem) {
      this.namesystem = namesystem;
      this.cacheInternal = new Cache<INodeWithHashCode, String>(conf) {  
        @Override
        public String getDirectly(INodeWithHashCode inode) throws IOException {
          namesystem.readLock();
          try {
            return inode.getFullPathName();
          } finally {
            namesystem.readUnlock();
          }
        }
      };
    }
      
    private static class INodeWithHashCode {
      FSInodeInfo inode;
      INodeWithHashCode(FSInodeInfo inode) {
        this.inode = inode;
      }
      @Override
      public boolean equals(Object obj) {
        return inode == obj;
      }
      @Override
      public int hashCode() {
        return System.identityHashCode(inode);
      }
      String getFullPathName() {
        return inode.getFullPathName();
      }
    }

    public String get(FSInodeInfo inode) throws IOException {
      return cacheInternal.get(new INodeWithHashCode(inode));
    }
  }

  /**
   * Cache results for FSNamesystem.getBlockLocations()
   */
  static class CachedLocatedBlocks extends Cache<String, List<LocatedBlock>> {
    private FSNamesystem namesystem;

    CachedLocatedBlocks(Configuration conf, FSNamesystem namesystem) {
      super(conf);
      this.namesystem = namesystem;
    }

    @Override
    public List<LocatedBlock> getDirectly(String file) throws IOException {
      INodeFile inode = namesystem.dir.getFileINode(file);
      // Note that the list is generated. It is not the internal data of inode.
      List<LocatedBlock> result = inode == null ?
          new ArrayList<LocatedBlock>() :
          namesystem.getBlockLocationsInternal(inode, 0, Long.MAX_VALUE,
                                       Integer.MAX_VALUE).getLocatedBlocks();
      if (result == null) {
        result = new ArrayList<LocatedBlock>();
      }
      return result;
    }
  }

  private static abstract class Cache<K, V> {
    private Map<K, ValueWithTime> cache;
    final private long cacheTimeout;
    final private int maxEntries;
    // The timeout is long but the consequence of stale value is not serious
    Cache(Configuration conf) {
      this.cacheTimeout = 
        conf.getLong("raid.blockplacement.cache.timeout", 5000L); // 5 seconds
      this.maxEntries =
        conf.getInt("raid.blockplacement.cache.size", 1000);  // 1000 entries
      Map<K, ValueWithTime> map = new LinkedHashMap<K, ValueWithTime>(
          maxEntries, 0.75f, true) {
        private static final long serialVersionUID = 1L;
          @Override
          protected boolean removeEldestEntry(
            Map.Entry<K, ValueWithTime> eldest) {
            return size() > maxEntries;
          }
        };
      this.cache = Collections.synchronizedMap(map);
    }

    // Note that this method may hold FSNamesystem.readLock() and it may
    // be called inside FSNamesystem.writeLock(). If we make this method
    // synchronized, it will deadlock.
    abstract protected V getDirectly(K key) throws IOException;

    public V get(K key) throws IOException {
      // The method is not synchronized so we may get some stale value here but
      // it's OK.
      ValueWithTime result = cache.get(key);
      long now = System.currentTimeMillis();
      if (result != null &&
          now - result.cachedTime < cacheTimeout) {
        return result.value;
      }
      result = new ValueWithTime();
      result.value = getDirectly(key);
      result.cachedTime = now;
      cache.put(key, result);
      return result.value;
    }
    private class ValueWithTime {
      V value = null;
      long cachedTime = 0L;
    }
  }

  /**
   * Get path for the corresponding source file for a valid parity
   * file. Returns null if it does not exists
   * @param parity the toUri path of the parity file
   * @return the toUri path of the source file
   */
  String getSourceFile(String parity, String prefix) throws IOException {
    if (isHarFile(parity)) {
      return null;
    }
    // remove the prefix
    String src = parity.substring(prefix.length());
    if (namesystem.dir.getInode(src) == null) {
      return null;
    }
    return src;
  }

  /**
   * Get path for the corresponding parity file for a source file.
   * Returns null if it does not exists
   * @param src the toUri path of the source file
   * @return the toUri path of the parity file
   */
  String getParityFile(String src) throws IOException {
    String xorParity = getParityFile(xorPrefix, src);
    if (xorParity != null) {
      return xorParity;
    }
    String rsParity = getParityFile(rsPrefix, src);
    if (rsParity != null) {
      return rsParity;
    }
    return null;
  }

  /**
   * Get path for the parity file. Returns null if it does not exists
   * @param parityPrefix usuall "/raid/" or "/raidrs/"
   * @return the toUri path of the parity file
   */
  private String getParityFile(String parityPrefix, String src)
      throws IOException {
    String parity = parityPrefix + src;
    if (namesystem.dir.getInode(parity) == null) {
      return null;
    }
    return parity;
  }

  private boolean isHarFile(String path) {
    return path.lastIndexOf(RaidNode.HAR_SUFFIX) != -1;
  }

  enum FileType {
    NOT_RAID,
    XOR_HAR_TEMP_PARITY,
    XOR_TEMP_PARITY,
    XOR_PARITY,
    XOR_SOURCE,
    RS_HAR_TEMP_PARITY,
    RS_TEMP_PARITY,
    RS_PARITY,
    RS_SOURCE,
  }

  FileType getFileType(String path) throws IOException {
    if (path.startsWith(raidHarTempPrefix + Path.SEPARATOR)) {
      return FileType.XOR_HAR_TEMP_PARITY;
    }
    if (path.startsWith(raidrsHarTempPrefix + Path.SEPARATOR)) {
      return FileType.RS_HAR_TEMP_PARITY;
    }
    if (path.startsWith(raidTempPrefix + Path.SEPARATOR)) {
      return FileType.XOR_TEMP_PARITY;
    }
    if (path.startsWith(raidrsTempPrefix + Path.SEPARATOR)) {
      return FileType.RS_TEMP_PARITY;
    }
    if (path.startsWith(xorPrefix + Path.SEPARATOR)) {
      return FileType.XOR_PARITY;
    }
    if (path.startsWith(rsPrefix + Path.SEPARATOR)) {
      return FileType.RS_PARITY;
    }
    String parity = getParityFile(path);
    if (parity == null) {
      return FileType.NOT_RAID;
    }
    if (parity.startsWith(xorPrefix + Path.SEPARATOR)) {
      return FileType.XOR_SOURCE;
    }
    if (parity.startsWith(rsPrefix + Path.SEPARATOR)) {
      return FileType.RS_SOURCE;
    }
    return FileType.NOT_RAID;
  }
}
