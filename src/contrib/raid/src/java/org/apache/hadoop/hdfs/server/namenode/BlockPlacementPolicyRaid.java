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
import org.apache.hadoop.raid.Codec;
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
    this.cachedLocatedBlocks = new CachedLocatedBlocks(conf, namesystem);
    this.cachedFullPathNames = new CachedFullPathNames(conf, namesystem);
  }

  @Override
  public DatanodeDescriptor[] chooseTarget(String srcPath, int numOfReplicas,
      DatanodeDescriptor writer, List<DatanodeDescriptor> chosenNodes,
      long blocksize) {
    return chooseTarget(srcPath, numOfReplicas, writer, chosenNodes, null, 
        blocksize);
  }
  
  @Override
  public DatanodeDescriptor[] chooseTarget(String srcPath, int numOfReplicas,
      DatanodeDescriptor writer, List<DatanodeDescriptor> chosenNodes,
      List<Node> exlcNodes, long blocksize) {
    try {
      FileInfo info = getFileInfo(srcPath);
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
      FileInfo info = getFileInfo(path);
      if (info.type == FileType.NOT_RAID) {
        return super.chooseReplicaToDelete(
            inode, block, replicationFactor, first, second);
      }
      List<LocatedBlock> companionBlocks =
          getCompanionBlocks(path, info, block);
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
   * @param info The info of this file
   * @param block The given block
   *              null if it is the block which is currently being written to
   * @return the block locations of companion blocks
   */
  List<LocatedBlock> getCompanionBlocks(String path, FileInfo info, Block block)
      throws IOException {
    Codec codec = info.codec;
    switch (info.type) {
      case NOT_RAID:
        return Collections.emptyList();
      case HAR_TEMP_PARITY:
        return getCompanionBlocksForHarParityBlock(
            path, codec.parityLength, block);
      case TEMP_PARITY:
        return getCompanionBlocksForParityBlock(
            getSourceFile(path, codec.tmpParityDirectory),
            path, codec.parityLength, codec.stripeLength, block,
            codec.isDirRaid);
      case PARITY:
        return getCompanionBlocksForParityBlock(
            getSourceFile(path, codec.parityDirectory),
            path, codec.parityLength, codec.stripeLength, block, 
            codec.isDirRaid);
      case SOURCE:
        return getCompanionBlocksForSourceBlock(
            path,
            getParityFile(codec, path),
            codec.parityLength, codec.stripeLength, block,
            codec.isDirRaid);
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
    int start = Math.max(0, blockIndex - parityLength + 1);
    int end = Math.min(parityBlocks.size(), blockIndex + parityLength);
    result.addAll(parityBlocks.subList(start, end));
    return result;
  }
  
  
  /**
   * Check if we will do the directory raid on the file.
   * 
   * @param fileBlocks, all the blocks of the file.
   * @return
   */
  private boolean canDoDirectoryRaid(List<LocatedBlock> fileBlocks) {
    long minFileSize = conf.getLong(RaidNode.MINIMUM_RAIDABLE_FILESIZE_KEY,
        RaidNode.MINIMUM_RAIDABLE_FILESIZE);
    long fileSize = 0;
    for (LocatedBlock block : fileBlocks) {
      fileSize += block.getBlock().getNumBytes();
      if (fileSize >= minFileSize) {
        return true;
      }
    }
    return false;
  }

  private List<LocatedBlock> getCompanionBlocksForParityBlock(
      String src, String parity, int parityLength, int stripeLength, 
      Block block, boolean isDirRaid)
      throws IOException {
    int blockIndex = getBlockIndex(parity, block);
    List<LocatedBlock> result = new ArrayList<LocatedBlock>();
    List<LocatedBlock> parityBlocks = cachedLocatedBlocks.get(parity);
    int stripeIndex = blockIndex / parityLength;
    int parityStart = stripeIndex * parityLength;
    int parityEnd = Math.min(parityStart + parityLength,
        parityBlocks.size());
    // for parity, always consider the neighbor blocks as companion blocks
    if (parityStart < parityBlocks.size()) {
      result.addAll(parityBlocks.subList(parityStart, parityEnd));
    }
    
    if (src == null) {
      return result;
    }
    
    // get the source blocks.
    List<LocatedBlock> sourceBlocks;
    int sourceStart = stripeIndex * stripeLength;
    int sourceEnd = sourceStart + stripeLength;
    
    if (!isDirRaid) { 
      sourceBlocks = cachedLocatedBlocks.get(src);
    } else {
      sourceBlocks = new ArrayList<LocatedBlock>();
      INode inode = namesystem.dir.getInode(src);
      INodeDirectory srcNode;
      if (inode.isDirectory()) {
        srcNode = (INodeDirectory) inode;
      } else {
        throw new IOException(
            "The source should be a directory in Dir-Raiding: " + src);
      }
      
      boolean found = false;
      // look for the stripe 
      for (INode child : srcNode.getChildren()) {
        if (child.isDirectory()) {
          throw new IOException("The source is not a leaf directory: " + src
              + ", contains a subdirectory: " + child.getLocalName());
        }
        
        List<LocatedBlock> childBlocks = 
            cachedLocatedBlocks.get(src + "/" + child.getLocalName());
        
        // check if we will do dir-raid on this file
        if (!canDoDirectoryRaid(childBlocks)) {
          continue;
        }
        
        if (childBlocks.size() < sourceStart && !found) {
          sourceStart -= childBlocks.size();
          sourceEnd -= childBlocks.size();
          continue;
        } else {
          found = true;
          sourceBlocks.addAll(childBlocks);
          if (sourceEnd <= sourceBlocks.size()) {
            break;
          }
        }
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
      Block block, boolean isDirRaid)
      throws IOException {
    int blockIndex = getBlockIndex(src, block);
    List<LocatedBlock> result = new ArrayList<LocatedBlock>();
    List<LocatedBlock> sourceBlocks; 
    
    int stripeIndex = 0;
    int sourceStart = 0;
    int sourceEnd = 0;
    
    if (!isDirRaid) {
      sourceBlocks = cachedLocatedBlocks.get(src);
      stripeIndex = blockIndex / stripeLength;
      sourceStart = stripeIndex * stripeLength;
      sourceEnd = Math.min(sourceStart + stripeLength, sourceBlocks.size());
    } else {
      // cache the candidate blocks.
      LocatedBlock[] tmpStripe = new LocatedBlock[stripeLength];
      int curIdx = 0;
      boolean found = false;
      
      sourceBlocks = new ArrayList<LocatedBlock>();
      INodeDirectory srcNode = namesystem.dir.getInode(src).getParent();
      String srcPath = new Path(src).getParent().toString();
      
      // look for the stripe
      for (INode child : srcNode.getChildren()) {
        if (child.isDirectory()) {
          throw new IOException("The raided-directory is not a leaf directory: "
              + srcPath + 
              ", contains a subdirectory: " + child.getLocalName());
        }
        
        String childName = srcPath + "/" + child.getLocalName();
        List<LocatedBlock> childBlocks = cachedLocatedBlocks.get(childName);
        
        // check if we will do dir-raid on this file
        if (!canDoDirectoryRaid(childBlocks)) {
          continue;
        }
        
        if (found) {
          if (sourceEnd <= sourceBlocks.size()) {
            break;
          }
          sourceBlocks.addAll(childBlocks);
        } else {
          int childBlockSize = childBlocks.size();
          
          /** 
           * If we find the target file, we will addAll the
           * cached blocks and the child blocks. 
           * And update the metrics like stripeIndex, sourceStart and sourceEnd.
           * 
           */
          if (childName.equals(src)) {
            found = true;
            for (int i=0; i<curIdx; i++) {
              sourceBlocks.add(tmpStripe[i]);
            }
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
              for (int i=0; i<childBlockSize; i++) {
                tmpStripe[curIdx++] = childBlocks.get(i);
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
              for (; childStart<childBlockSize;) {
                tmpStripe[curIdx++] = childBlocks.get(childStart++);
              }
              curIdx %= stripeLength;
            }
          }
        }
      }
      sourceEnd = Math.min(sourceEnd, sourceBlocks.size());
    } 
    
    if (sourceStart < sourceBlocks.size()) {
      result.addAll(sourceBlocks.subList(sourceStart, sourceEnd));
    }
      if (parity == null) {
      return result;
    }
    
    // get the parity blocks.
    List<LocatedBlock> parityBlocks = cachedLocatedBlocks.get(parity);
    int parityStart = stripeIndex * parityLength;
    int parityEnd = Math.min(parityStart + parityLength,
        parityBlocks.size());
    if (parityStart < parityBlocks.size()) {
      result.addAll(parityBlocks.subList(parityStart, parityEnd));
    }
    return result;
  }

  private int getBlockIndex(String file, Block block) throws IOException {
    List<LocatedBlock> blocks = cachedLocatedBlocks.get(file);
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
        return Collections.emptyList(); 
      }
      return Collections.unmodifiableList(result);
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
   * Get path for the parity file. Returns null if it does not exists
   * @param codec the codec of the parity file.
   * @return the toUri path of the parity file
   */
  private String getParityFile(Codec codec, String src)
      throws IOException {
    String parity;
    if (codec.isDirRaid) {
      String parent = new Path(src).getParent().toString();
      parity = codec.parityDirectory + parent;
    } else {
      parity = codec.parityDirectory + src;
    }
    
    if (namesystem.dir.getInode(parity) == null) {
      return null;
    }
    return parity;
  }

  private boolean isHarFile(String path) {
    return path.lastIndexOf(RaidNode.HAR_SUFFIX) != -1;
  }

  class FileInfo {
    FileInfo(FileType type, Codec codec) {
      this.type = type;
      this.codec = codec;
    }
    final FileType type;
    final Codec codec;
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
  protected FileInfo getFileInfo(String path) throws IOException {
    for (Codec c : Codec.getCodecs()) {
      if (path.startsWith(c.tmpHarDirectory + Path.SEPARATOR)) {
        return new FileInfo(FileType.HAR_TEMP_PARITY, c);
      }
      if (path.startsWith(c.tmpParityDirectory + Path.SEPARATOR)) {
        return new FileInfo(FileType.TEMP_PARITY, c);
      }
      if (path.startsWith(c.parityDirectory + Path.SEPARATOR)) {
        return new FileInfo(FileType.PARITY, c);
      }
      String parity = getParityFile(c, path);
      if (parity != null) {
        return new FileInfo(FileType.SOURCE, c);
      }
    }
    return new FileInfo(FileType.NOT_RAID, null);
  }
}
