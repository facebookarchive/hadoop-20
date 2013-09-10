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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.server.namenode.BlockPlacementPolicyDefault;
import org.apache.hadoop.hdfs.server.namenode.BlockPlacementPolicy.NotEnoughReplicasException;
import org.apache.hadoop.hdfs.util.InjectionEvent;
import org.apache.hadoop.net.DNSToSwitchMapping;
import org.apache.hadoop.net.NetworkTopology;
import org.apache.hadoop.net.Node;
import org.apache.hadoop.raid.DirectoryStripeReader.BlockInfo;
import org.apache.hadoop.raid.RaidNode;
import org.apache.hadoop.raid.Codec;
import org.apache.hadoop.util.HostsFileReader;
import org.apache.hadoop.util.InjectionHandler;
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
  private FSNamesystem namesystem = null;

  private CachedLocatedBlocks cachedLocatedBlocks;
  private CachedFullPathNames cachedFullPathNames;
  private long minFileSize = RaidNode.MINIMUM_RAIDABLE_FILESIZE;

  /** {@inheritDoc} */
  @Override
  public void initialize(Configuration conf,  FSClusterStats stats,
                         NetworkTopology clusterMap, HostsFileReader hostsReader,
                         DNSToSwitchMapping dnsToSwitchMapping, FSNamesystem namesystem) {
    super.initialize(conf, stats, clusterMap, 
                     hostsReader, dnsToSwitchMapping, namesystem);
    this.conf = conf;
    this.minFileSize = conf.getLong(RaidNode.MINIMUM_RAIDABLE_FILESIZE_KEY,
        RaidNode.MINIMUM_RAIDABLE_FILESIZE);
    this.namesystem = namesystem;
    this.cachedLocatedBlocks = new CachedLocatedBlocks(conf);
    this.cachedFullPathNames = new CachedFullPathNames(conf);
  }

  @Override
  public DatanodeDescriptor[] chooseTarget(String srcPath, int numOfReplicas,
      DatanodeDescriptor writer, List<DatanodeDescriptor> chosenNodes,
      long blocksize) {
    return chooseTarget(srcPath, numOfReplicas, writer, chosenNodes, null, 
        blocksize);
  }

  @Override
  protected void place3rdReplicaForInClusterWriter(
      HashMap<Node, Node> excludedNodes, long blocksize,
      int maxNodesPerRack,List<DatanodeDescriptor> results
      ) throws NotEnoughReplicasException {
    if (results.size() > 2) {
      return;
    }
    HashSet<String> excludedRacks = new HashSet<String>();
    for (DatanodeDescriptor node : results) {
      String rack = node.getNetworkLocation();
      excludedRacks.add(rack);
    }
    
    do {
      String remoteRack = clusterMap.chooseRack(excludedRacks);
      if (remoteRack == null) { // no more remote rack available
        // choose a node on the rack where the first replica is located
        chooseLocalRack(
            results.get(0), excludedNodes, blocksize, maxNodesPerRack, results);
        return;
      }
      // a remote rack is chosen
      try {
        excludedRacks.add(remoteRack);
        chooseRandom(1, remoteRack, excludedNodes, blocksize, 
            maxNodesPerRack, results);
        return;
      } catch (NotEnoughReplicasException ne) {
        // try again until all remote tracks are exhausted
      }
    } while (true);
  }
    

  @Override
  public DatanodeDescriptor[] chooseTarget(String srcPath, int numOfReplicas,
      DatanodeDescriptor writer, List<DatanodeDescriptor> chosenNodes,
      List<Node> exlcNodes, long blocksize) {
    try {
      FileInfo info = getFileInfo(null, srcPath);
      if (LOG.isDebugEnabled()) {
        LOG.debug("FileType:" + srcPath + " " + info.type.name());
      }
      if (info.type == FileType.NOT_RAID) {
        return super.chooseTarget(
            srcPath, numOfReplicas, writer, chosenNodes, exlcNodes, blocksize);
      }
      ArrayList<DatanodeDescriptor> results = new ArrayList<DatanodeDescriptor>();
      HashMap<Node, Node> excludedNodes = new HashMap<Node, Node>();
      if (exlcNodes != null) {
        for (Node node: exlcNodes) {
          excludedNodes.put(node, node);
        }
      }
      for (Node node:chosenNodes) {
        excludedNodes.put(node, node);
      }
      chooseRandom(numOfReplicas, Path.SEPARATOR, excludedNodes, blocksize,
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
      String path = getFullPathName(inode);
      FileInfo info = getFileInfo(inode, path);
      if (info.type == FileType.NOT_RAID) {
        return super.chooseReplicaToDelete(
            inode, block, replicationFactor, first, second);
      }
      List<LocatedBlock> companionBlocks =
          getCompanionBlocks(path, info, block, inode);
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

  private DatanodeDescriptor chooseReplicaToDelete(
      Collection<LocatedBlock> companionBlocks,
      Collection<DatanodeDescriptor> dataNodes) throws IOException {

    if (dataNodes.isEmpty()) {
      return null;
    }
    // Count the number of replicas on each node and rack
    final Map<String, Integer>[] companionBlockCounts = countCompanionBlocks(companionBlocks);
    final Map<String, Integer> nodeCompanionBlockCount =
        companionBlockCounts[0];
    final Map<String, Integer> rackCompanionBlockCount =
        companionBlockCounts[1];

    NodeComparator comparator =
      new NodeComparator(nodeCompanionBlockCount, rackCompanionBlockCount);
    return Collections.max(dataNodes, comparator);
  }

  /**
   * Count how many companion blocks are on each datanode or the each rack
   * @param companionBlocks a collection of all the companion blocks
   * @param result the map from node name to the number of companion blocks
   * [0] for datanodes [1] for racks
   */
  @SuppressWarnings("unchecked")
  static Map<String, Integer>[] countCompanionBlocks(
      Collection<LocatedBlock> companionBlocks) {
    Map<String, Integer>[] result = new HashMap[2];
    result[0] = new HashMap<String, Integer>();
    result[1] = new HashMap<String, Integer>();
    
    for (LocatedBlock block : companionBlocks) {
      for (DatanodeInfo d : block.getLocations()) {
        // count the companion blocks on the datanodes
        String name = d.getName();
        Integer currentCount = result[0].get(name);
        result[0].put(name, currentCount == null ? 1 : currentCount + 1);
        
        // count the companion blocks on the racks of datanodes
        name = d.getParent().getName();
        currentCount = result[1].get(name);
        result[1].put(name, currentCount == null ? 1 : currentCount + 1);
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
   * @param info The info of this file
   * @param block The given block
   *              null if it is the block which is currently being written to
   * @param inode the inode of the path file 
   * @return the block locations of companion blocks
   */
  List<LocatedBlock> getCompanionBlocks(String path, FileInfo info, Block block, FSInodeInfo inode)
      throws IOException {
    Codec codec = info.codec;
    switch (info.type) {
      case NOT_RAID:
        return Collections.emptyList();
      case HAR_TEMP_PARITY:
        return getCompanionBlocksForHarParityBlock(
            path, codec.parityLength, block, inode);
      case TEMP_PARITY:
        NameWithINode ni = getSourceFile(path, codec.tmpParityDirectory);
        return getCompanionBlocksForParityBlock(
            ni.name,
            path, codec.parityLength, codec.stripeLength, block,
            codec.isDirRaid, ni.inode, inode);
      case PARITY:
        ni = getSourceFile(path, codec.parityDirectory);
        return getCompanionBlocksForParityBlock(
            ni.name,
            path, codec.parityLength, codec.stripeLength, block, 
            codec.isDirRaid, ni.inode, inode);
      case SOURCE:
        return getCompanionBlocksForSourceBlock(
            path,
            info.parityName,
            codec.parityLength, codec.stripeLength, block,
            codec.isDirRaid, inode, info.parityInode);
    }
    return Collections.emptyList();
  }

  private List<LocatedBlock> getCompanionBlocksForHarParityBlock(
      String parity, int parityLength, Block block, FSInodeInfo inode)
      throws IOException {
    int blockIndex = getBlockIndex(parity, block, inode, true);
    List<LocatedBlock> parityBlocks = getLocatedBlocks(parity, inode);
    // consider only parity file in this case because source file block
    // location is not easy to obtain
    List<LocatedBlock> result = new ArrayList<LocatedBlock>();
    int start = Math.max(0, blockIndex - parityLength + 1);
    int end = Math.min(parityBlocks.size(), blockIndex + parityLength);
    result.addAll(parityBlocks.subList(start, end));
    return result;
  }

  private void addCompanionParityBlocks(String parity, INodeFile pinode,
      int stripeIndex, int parityLength, List<LocatedBlock> blocks)
          throws IOException {
    if (pinode == null) 
      return;
    long parityStartOffset = stripeIndex * parityLength *
       pinode.getPreferredBlockSize(); 
    long parityFileSize = namesystem.dir.getFileSize(pinode);
    // for parity, always consider the neighbor blocks as companion blocks
    if (parityStartOffset < parityFileSize) {
      blocks.addAll(getLocatedBlocks(pinode, parityStartOffset,
          parityLength * pinode.getPreferredBlockSize()));
    }
  }

  String getFullPathName(FSInodeInfo inode) throws IOException {
    String path = cachedFullPathNames.get(inode);
    if (path != null) {
      InjectionHandler
          .processEvent(InjectionEvent.BLOCKPLACEMENTPOLICYRAID_CACHED_PATH);
      return path;
    }
    byte[][] names = null;
    namesystem.readLock();
    try {
      names = FSDirectory.getINodeByteArray((INode)inode);
    } finally {
      namesystem.readUnlock();
    }
    path = FSDirectory.getFullPathName(names);
    cachedFullPathNames.put(inode, path);
    return path;   
  }
  
  List<LocatedBlock> getLocatedBlocks(String file, FSInodeInfo f)
      throws IOException {
    List<LocatedBlock> blocks = cachedLocatedBlocks.get(file);
    if (blocks != null) {
      InjectionHandler
          .processEvent(InjectionEvent.BLOCKPLACEMENTPOLICYRAID_CACHED_BLOCKS);
      return blocks;
    }
    // otherwise populate cache
    INodeFile inode = (INodeFile) f;
    // Note that the list is generated. It is not the internal data of inode.
    List<LocatedBlock> result = inode == null ? new ArrayList<LocatedBlock>()
        : namesystem.getBlockLocationsInternal(inode, 0, Long.MAX_VALUE,
            Integer.MAX_VALUE).getLocatedBlocks();
    if (result == null) {
      result = Collections.emptyList();
    } else {
      result = Collections.unmodifiableList(result);
    }
    cachedLocatedBlocks.put(file, result);
    return result;
  }
  
  public List<LocatedBlock> getLocatedBlocks(INodeFile inode, long offset, 
      long length)
      throws IOException {
    // Note that the list is generated. It is not the internal data of inode.
    List<LocatedBlock> result = inode == null ?
        new ArrayList<LocatedBlock>() :
        namesystem.getBlockLocationsInternal(inode, offset, length,
                                     Integer.MAX_VALUE).getLocatedBlocks();
    if (result == null) {
      return Collections.emptyList(); 
    }
    return Collections.unmodifiableList(result);
  }

  private List<LocatedBlock> getCompanionBlocksForParityBlock(
      String src, String parity, int parityLength, int stripeLength, 
      Block block, boolean isDirRaid, FSInodeInfo srcinode, FSInodeInfo pinode)
      throws IOException {
    int blockIndex = getBlockIndex(parity, block, pinode, false);
    int stripeIndex = blockIndex / parityLength;

    List<LocatedBlock> result = new ArrayList<LocatedBlock>();
    addCompanionParityBlocks(parity, (INodeFile)pinode, stripeIndex,
        parityLength, result);
    if (src == null) {
      return result;
    }

    // get the source blocks.
    List<LocatedBlock> sourceBlocks;
    int sourceStart = stripeIndex * stripeLength;
    int sourceEnd = sourceStart + stripeLength;

    if (!isDirRaid) {
      sourceBlocks = getLocatedBlocks(src, srcinode);
    } else {
      sourceBlocks = new ArrayList<LocatedBlock>();
      INode inode = (INode) srcinode;
      INodeDirectory srcNode;
      if (inode.isDirectory()) {
        srcNode = (INodeDirectory) inode;
      } else {
        throw new IOException(
            "The source should be a directory in Dir-Raiding: " + src);
      }

      boolean found = false;
      String srcPath = src + Path.SEPARATOR;
      // look for the stripe 
      namesystem.readLock();
      namesystem.dir.readLock();
      try {
        for (INode child : srcNode.getChildren()) {
          if (child.isDirectory()) {
            throw new IOException("The source is not a leaf directory: " + src
                + ", contains a subdirectory: " + child.getLocalName());
          }
          INodeFile childInode = (INodeFile)child;
          long fileSize = namesystem.dir.getFileSize(childInode);
          // check if we will do dir-raid on this file
          if (fileSize < minFileSize) {
            continue;
          }
          int numBlocks = childInode.getBlocks().length;

          if (numBlocks < sourceStart && !found) {
            sourceStart -= numBlocks; 
            sourceEnd -= numBlocks;
            continue;
          } else {
            String childName = srcPath + child.getLocalName();
            List<LocatedBlock> childBlocks = getLocatedBlocks(childName, child);
            found = true;
            sourceBlocks.addAll(childBlocks);
            if (sourceEnd <= sourceBlocks.size()) {
              break;
            }
          }
        }
      } finally {
        namesystem.dir.readUnlock();
        namesystem.readUnlock();
      }
    }

    sourceEnd = Math.min(sourceEnd,
        sourceBlocks.size());
    if (sourceStart < sourceBlocks.size()) {
      result.addAll(sourceBlocks.subList(sourceStart, sourceEnd));
    }
    return result;
  }

  private List<LocatedBlock> getCompanionBlocksForSourceBlock(
      String src, String parity, int parityLength, int stripeLength, 
      Block block, boolean isDirRaid, FSInodeInfo inode, FSInodeInfo parityInode)
      throws IOException {
    List<LocatedBlock> result = new ArrayList<LocatedBlock>();
    List<LocatedBlock> sourceBlocks = null;
    int blockIndex = getBlockIndex(src, block, inode, true);
    int stripeIndex = 0;
    int sourceStart = 0;
    int sourceEnd = 0;

    if (!isDirRaid) {
      sourceBlocks = getLocatedBlocks(src, inode);
      stripeIndex = blockIndex / stripeLength;
      sourceStart = stripeIndex * stripeLength;
      sourceEnd = Math.min(sourceStart + stripeLength, sourceBlocks.size());
    } else {
      // cache the candidate blocks.
      BlockInfo[] tmpStripe = new BlockInfo[stripeLength];
      for (int i = 0; i < stripeLength; i++) {
        tmpStripe[i] = new BlockInfo(0, 0);
      }
      int curIdx = 0;
      boolean found = false;

      sourceBlocks = new ArrayList<LocatedBlock>();
      byte[][] components = INodeDirectory.getPathComponents(src);
      INodeDirectory srcNode = namesystem.dir.getINode(components).getParent();
      String parentPath = getParentPath(src);
      if (!parentPath.endsWith(Path.SEPARATOR)) {
        parentPath += Path.SEPARATOR;
      }
      
      namesystem.readLock();
      namesystem.dir.readLock();
      try {
        List<INode> children = srcNode.getChildren();
        // look for the stripe
        for (int fid = 0; fid < children.size(); fid++) {
          INode child = children.get(fid);
          if (child.isDirectory()) {
            throw new IOException("The raided-directory is not a leaf directory: "
                + parentPath + 
                ", contains a subdirectory: " + child.getLocalName());
          }
          INodeFile childInode = (INodeFile)child;

          long fileSize = namesystem.dir.getFileSize(childInode);
          // check if we will do dir-raid on this file
          if (fileSize < minFileSize) {
            continue;
          }

          String childName = parentPath + child.getLocalName();
          if (found) {
            if (sourceEnd <= sourceBlocks.size()) {
              break;
            }
            List<LocatedBlock> childBlocks = getLocatedBlocks(childName, childInode);
            sourceBlocks.addAll(childBlocks);
          } else {
            int childBlockSize = childInode.getBlocks().length;

            /** 
             * If we find the target file, we will addAll the
             * cached blocks and the child blocks. 
             * And update the metrics like stripeIndex, sourceStart and sourceEnd.
             * 
             */
            if (childName.equals(src)) {
              found = true;
              List<LocatedBlock> prevChildBlocks = null;
              for (int i=0; i<curIdx; i++) {
                if (i == 0 || tmpStripe[i].fileIdx != tmpStripe[i - 1].fileIdx) {
                  INode prevChildInode = children.get(tmpStripe[i].fileIdx);
                  String prevChildName = parentPath + prevChildInode.getLocalName();
                  prevChildBlocks = getLocatedBlocks(prevChildName, prevChildInode);
                }
                sourceBlocks.add(prevChildBlocks.get(tmpStripe[i].blockId));
              }
              List<LocatedBlock> childBlocks = getLocatedBlocks(childName, childInode);
              sourceBlocks.addAll(childBlocks);
              blockIndex += curIdx;

              stripeIndex += blockIndex / stripeLength;
              sourceStart = (blockIndex / stripeLength) * stripeLength;
              sourceEnd = sourceStart + stripeLength;
            } else {
              /**
               * If not find the target file, we will keep the current stripe
               * in the temp stripe cache.
               */
              /**
               * the childBlockSize is small, and we can fill them into 
               * current temp stripe cache.
               */
              if (curIdx + childBlockSize < stripeLength) {
                for (int i=0; i<childBlockSize; i++, curIdx++) {
                  tmpStripe[curIdx].fileIdx = fid;
                  tmpStripe[curIdx].blockId = i;
                }
              } else {
                /**
                 * The childBlockSize is not small, We need to calculate 
                 * the place in the stripe cache, and copy the current stripe 
                 * into the temp stripe cache.
                 */
                stripeIndex += (curIdx + childBlockSize) / stripeLength;
                int childStart = ((curIdx + childBlockSize) / stripeLength) 
                    * stripeLength - curIdx;
                curIdx = 0;
                for (; childStart<childBlockSize; childStart++,curIdx++) {
                  tmpStripe[curIdx].fileIdx = fid;
                  tmpStripe[curIdx].blockId = childStart;
                }
                curIdx %= stripeLength;
              }
            }
          }
        }
      } finally {
        namesystem.dir.readUnlock();
        namesystem.readUnlock();
      }
      sourceEnd = Math.min(sourceEnd, sourceBlocks.size());
    }

    if (sourceStart < sourceBlocks.size()) {
      for (int i = sourceStart; i < sourceEnd; i++) {
        result.add(sourceBlocks.get(i));
      }
    }
    if (parity == null) {
      return result;
    }
    // add the parity blocks.
    addCompanionParityBlocks(parity, (INodeFile)parityInode,
        stripeIndex, parityLength, result);
    return result;
  }

  private int getBlockIndex(String file, Block block, FSInodeInfo inode,
      boolean cacheResult)
      throws IOException {
    if (cacheResult) {
      List<LocatedBlock> blocks = getLocatedBlocks(file, inode);
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
      throw new IOException("Cannot locate " + block + " in file " + file);
    } else {
      return namesystem.dir.getBlockIndex((INodeFile)inode, block, file);
    }
  }
  
  /**
   * Cache results for FSInodeInfo.getFullPathName()
   */
  static class CachedFullPathNames {
    private Cache<INodeWithHashCode, String> cacheInternal;

    CachedFullPathNames(final Configuration conf) {
      this.cacheInternal = new Cache<INodeWithHashCode, String>(conf);
    }
      
    private static class INodeWithHashCode {
      FSInodeInfo inode;
      INodeWithHashCode(FSInodeInfo inode) {
        this.inode = inode;
      }
      @Override
      public boolean equals(Object obj) {
        if (!(obj instanceof INodeWithHashCode))
          return false;
        return inode == ((INodeWithHashCode)obj).inode;
      }
      @Override
      public int hashCode() {
        return System.identityHashCode(inode);
      }
    }

    public String get(FSInodeInfo inode) throws IOException {
      return cacheInternal.get(new INodeWithHashCode(inode));
    }
    
    public void put(FSInodeInfo inode, String path) {
      cacheInternal.put(new INodeWithHashCode(inode), path);
    }
  }

  /**
   * Cache results for FSNamesystem.getBlockLocations()
   */
  static class CachedLocatedBlocks extends Cache<String, List<LocatedBlock>> {
    CachedLocatedBlocks(Configuration conf) {
      super(conf);
    }
  }

  /**
   * Generic caching class
   */
  private static class Cache<K, V> {
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
          2 * maxEntries, 0.75f, true) {
        private static final long serialVersionUID = 1L;
          @Override
          protected boolean removeEldestEntry(
            Map.Entry<K, ValueWithTime> eldest) {
            return size() > maxEntries;
          }
        };
      this.cache = Collections.synchronizedMap(map);
    }

    public V get(K key) throws IOException {
      // The method is not synchronized so we may get some stale value here but
      // it's OK.
      ValueWithTime result = cache.get(key);
      long now = System.currentTimeMillis();
      if (result != null &&
          now - result.cachedTime < cacheTimeout) {
        return result.value;
      }
      return null;
    }
    
    public void put(K key, V value) {
      ValueWithTime v = new ValueWithTime();
      v.value = value;
      v.cachedTime = System.currentTimeMillis();
      cache.put(key,  v);
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
  NameWithINode getSourceFile(String parity, String prefix) throws IOException {
    if (isHarFile(parity)) {
      return null;
    }
    // remove the prefix
    String src = parity.substring(prefix.length());
    byte[][] components = INodeDirectory.getPathComponents(src);
    INode inode = namesystem.dir.getINode(components);
    return new NameWithINode(src, inode);
  }
  
  class NameWithINode {
    String name;
    INode inode;
    
    public NameWithINode(String name, INode inode) {
      this.name = name;
      this.inode = inode;
    }
  }

  /**
   * Get path for the parity file. Returns null if it does not exists
   * @param codec the codec of the parity file.
   * @return the toUri path of the parity file
   */
  private NameWithINode getParityFile(Codec codec, String src)
      throws IOException {
    String parity;
    if (codec.isDirRaid) {
      String parent = getParentPath(src);      
      parity = codec.parityDirectory + parent;
    } else {
      parity = codec.parityDirectory + src;
    }
    byte[][] components = INodeDirectory.getPathComponents(parity);
    INode parityInode = namesystem.dir.getINode(components);
    if (parityInode == null)
      return null;
    return new NameWithINode(parity, parityInode);
  }
  
  static String getParentPath(String src) {
    int precision = 1;
    if (src.length() > 1 && src.endsWith(Path.SEPARATOR)) {
      precision = 2;
    }
    src = src.substring(0, src.lastIndexOf(Path.SEPARATOR, src.length() - precision));
    if (src.isEmpty())
      src = Path.SEPARATOR;
    return src;
  }

  private boolean isHarFile(String path) {
    return path.lastIndexOf(RaidNode.HAR_SUFFIX) != -1;
  }

  class FileInfo {
    FileInfo(FileType type, Codec codec) {
      this.type = type;
      this.codec = codec;
    }
    
    FileInfo(FileType type, Codec codec, String parityName, INode parityInode)
        throws IOException {
      if (type != FileType.SOURCE) {
        throw new IOException("FileType must be source");
      }
      this.type = type;
      this.codec = codec;
      this.parityInode = parityInode;
      this.parityName = parityName;
    }
    
    final FileType type;
    final Codec codec;
    INode parityInode = null;
    String parityName = null;
  }

  enum FileType {
    NOT_RAID,
    HAR_TEMP_PARITY,
    TEMP_PARITY,
    PARITY,
    SOURCE,
  }

  /**
   * Return raid information about a file, for example
   * if this file is the source file, parity file, or not raid
   * 
   * @param path file name
   * @return raid information
   * @throws IOException
   */
  protected FileInfo getFileInfo(FSInodeInfo srcINode, String path) throws IOException {
    for (Codec c : Codec.getCodecs()) {
      if (path.startsWith(c.tmpHarDirectoryPS)) {
        return new FileInfo(FileType.HAR_TEMP_PARITY, c);
      }
      if (path.startsWith(c.tmpParityDirectoryPS)) {
        return new FileInfo(FileType.TEMP_PARITY, c);
      }
      if (path.startsWith(c.parityDirectoryPS)) {
        return new FileInfo(FileType.PARITY, c);
      }
      NameWithINode ni = getParityFile(c, path);
      if (ni != null) {
        if (c.isDirRaid && srcINode != null && srcINode instanceof INodeFile) {
          INodeFile inf = (INodeFile)srcINode;
          if (inf.getFileSize() < this.minFileSize) {
            // It's too small to be raided
            return new FileInfo(FileType.NOT_RAID, null);
          }
        }
        return new FileInfo(FileType.SOURCE, c, ni.name, ni.inode);
      }
    }
    return new FileInfo(FileType.NOT_RAID, null);
  }
}
