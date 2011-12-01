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

package org.apache.hadoop.raid;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlockWithMetaInfo;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.protocol.LocatedBlocksWithMetaInfo;
import org.apache.hadoop.hdfs.protocol.VersionedLocatedBlocks;
import org.apache.hadoop.util.StringUtils;

/**
 * Monitors and potentially fixes placement of blocks in RAIDed files.
 */
public class PlacementMonitor {
  public static final Log LOG = LogFactory.getLog(PlacementMonitor.class);

  /**
   * Maps number of neighbor blocks to number of blocks
   */
  Map<ErasureCodeType, Map<Integer, Long>> blockHistograms;
  Configuration conf;
  private volatile Map<ErasureCodeType, Map<Integer, Long>> lastBlockHistograms;
  private volatile long lastUpdateStartTime = 0L;
  private volatile long lastUpdateFinishTime = 0L;
  private volatile long lastUpdateUsedTime = 0L;
  public static ThreadLocal<HashMap<String, LocatedFileStatus>> 
      locatedFileStatusCache = new ThreadLocal<HashMap<String, LocatedFileStatus>>() {
        @Override
        protected HashMap<String, LocatedFileStatus> initialValue() {
          return new HashMap<String, LocatedFileStatus>();
        }
      };
  
  RaidNodeMetrics metrics;
  BlockMover blockMover;

  final static String NUM_MOVING_THREADS_KEY = "hdfs.raid.block.move.threads";
  final static String SIMULATE_KEY = "hdfs.raid.block.move.simulate";
  final static String BLOCK_MOVE_QUEUE_LENGTH_KEY = "hdfs.raid.block.move.queue.length";
  final static int DEFAULT_NUM_MOVING_THREADS = 10;
  final static int DEFAULT_BLOCK_MOVE_QUEUE_LENGTH = 30000;
  final static int ALWAYS_SUBMIT_PRIORITY = 3;

  PlacementMonitor(Configuration conf) throws IOException {
    this.conf = conf;
    this.blockHistograms = createEmptyHistograms();
    int numMovingThreads = conf.getInt(
        NUM_MOVING_THREADS_KEY, DEFAULT_NUM_MOVING_THREADS);
    int maxMovingQueueSize = conf.getInt(
        BLOCK_MOVE_QUEUE_LENGTH_KEY, DEFAULT_BLOCK_MOVE_QUEUE_LENGTH);

    boolean simulate = conf.getBoolean(SIMULATE_KEY, true);
    blockMover = new BlockMover(
        numMovingThreads, maxMovingQueueSize, simulate,
        ALWAYS_SUBMIT_PRIORITY, conf);
    this.metrics = RaidNodeMetrics.getInstance(RaidNodeMetrics.DEFAULT_NAMESPACE_ID);
  }

  private Map<ErasureCodeType, Map<Integer, Long>> createEmptyHistograms() {
    Map<ErasureCodeType, Map<Integer, Long>> histo =
        new HashMap<ErasureCodeType, Map<Integer, Long>>();
    for (ErasureCodeType type : ErasureCodeType.values()) {
      histo.put(type, new HashMap<Integer, Long>());
    }
    return new EnumMap<ErasureCodeType, Map<Integer, Long>>(histo);
  }

  public void start() {
    blockMover.start();
  }

  public void stop() {
    blockMover.stop();
  }

  public void startCheckingFiles() {
    lastUpdateStartTime = RaidNode.now();
  }

  public int getMovingQueueSize() {
    return blockMover.getQueueSize();
  }

  public void checkFile(FileSystem srcFs, FileStatus srcFile,
            FileSystem parityFs, Path partFile, HarIndex.IndexEntry entry,
            ErasureCodeType code) throws IOException {
    if (srcFile.getReplication() > 1) {
      // We only check placement for the file with one replica
      return;
    }
    if (srcFs.getUri().equals(parityFs.getUri())) {
      BlockAndDatanodeResolver resolver = new BlockAndDatanodeResolver(
          srcFile.getPath(), srcFs, partFile, parityFs);
      checkBlockLocations(
          getBlockInfos(srcFs, srcFile),
          getBlockInfos(parityFs, partFile, entry.startOffset, entry.length),
          code, srcFile, resolver);
    } else { 
      // TODO: Move blocks in two clusters separately
      LOG.warn("Source and parity are in different file system. " +
          " source:" + srcFs.getUri() + " parity:" + parityFs.getUri() +
          ". Skip.");
    }
  }

  public void checkFile(FileSystem srcFs, FileStatus srcFile,
                        FileSystem parityFs, FileStatus parityFile,
                        ErasureCodeType code)
      throws IOException {
    if (srcFs.equals(parityFs)) {
      BlockAndDatanodeResolver resolver = new BlockAndDatanodeResolver(
          srcFile.getPath(), srcFs, parityFile.getPath(), parityFs);
      checkBlockLocations(
          getBlockInfos(srcFs, srcFile),
          getBlockInfos(parityFs, parityFile),
          code, srcFile, resolver);
    } else {
      // TODO: Move blocks in two clusters separately
      LOG.warn("Source and parity are in different file systems. Skip");
    }
  }

  LocatedFileStatus getLocatedFileStatus(
      FileSystem fs, Path p) throws IOException {
    HashMap<String, LocatedFileStatus> cache = 
        locatedFileStatusCache.get();
    LocatedFileStatus result = cache.get(p.toUri().getPath());
    if (result != null) {
      return result;
    }
    Path parent = p.getParent();
    String parentPath = parent.toUri().getPath();
    //If we already did listlocatedStatus on parent path,
    //it means path p doesn't exist, we don't need to list again 
    if (cache.containsKey(parentPath) && 
        cache.get(parentPath) == null) {
      return null;
    }
    
    RemoteIterator<LocatedFileStatus> iter = fs.listLocatedStatus(parent);
    while (iter.hasNext()) {
      LocatedFileStatus stat = iter.next();
      cache.put(stat.getPath().toUri().getPath(), stat);
    }
    // trick: add parent path to the cache with value = null 
    cache.put(parentPath, null);
    result = cache.get(p.toUri().getPath());
    // This may still return null
    return result;
  }

  static class BlockInfo {
    final BlockLocation blockLocation;
    final Path file;
    BlockInfo(BlockLocation blockLocation, Path file) {
      this.blockLocation = blockLocation;
      this.file = file;
    }
    String[] getNames() {
      try {
        return blockLocation.getNames();
      } catch (IOException e) {
        return new String[]{};
      }
    }
  }

  List<BlockInfo> getBlockInfos(
    FileSystem fs, FileStatus stat) throws IOException {
    return getBlockInfos(
      fs, stat.getPath(), 0, stat.getLen());
  }

  List<BlockInfo> getBlockInfos(
    FileSystem fs, Path path, long start, long length)
      throws IOException {
    LocatedFileStatus stat = getLocatedFileStatus(fs, path);
    List<BlockInfo> result = new ArrayList<BlockInfo>();
    long end = start + length;
    if (stat != null) {
      for (BlockLocation loc : stat.getBlockLocations()) {
        if (loc.getOffset() >= start && loc.getOffset() < end) {
          result.add(new BlockInfo(loc, path));
        }
      }
    }
    return result;
  }

  void checkBlockLocations(List<BlockInfo> srcBlocks,
      List<BlockInfo> parityBlocks, ErasureCodeType code,
      FileStatus srcFile, BlockAndDatanodeResolver resolver) throws IOException {
    if (srcBlocks == null || parityBlocks == null) {
      return;
    }
    int stripeLength = RaidNode.getStripeLength(conf);
    int parityLength = code == ErasureCodeType.XOR ?
        1 : RaidNode.rsParityLength(conf);
    int numBlocks = (int)Math.ceil(1D * srcFile.getLen() /
                                   srcFile.getBlockSize());
    int numStripes = (int)Math.ceil(1D * (numBlocks) / stripeLength);

    Map<String, Integer> nodeToNumBlocks = new HashMap<String, Integer>();
    Set<String> nodesInThisStripe = new HashSet<String>();

    for (int stripeIndex = 0; stripeIndex < numStripes; ++stripeIndex) {

      List<BlockInfo> stripeBlocks = getStripeBlocks(
          stripeIndex, srcBlocks, stripeLength, parityBlocks, parityLength);

      countBlocksOnEachNode(stripeBlocks, nodeToNumBlocks, nodesInThisStripe);

      logBadFile(nodeToNumBlocks, parityLength, srcFile);

      updateBlockPlacementHistogram(nodeToNumBlocks, blockHistograms.get(code));

      submitBlockMoves(
          nodeToNumBlocks, stripeBlocks, nodesInThisStripe, resolver);

    }
  }

  private static void logBadFile(
        Map<String, Integer> nodeToNumBlocks, int parityLength,
        FileStatus srcFile) {
    int max = 0;
    for (Integer n : nodeToNumBlocks.values()) {
      if (max < n) {
        max = n;
      }
    }
    int maxNeighborBlocks = max - 1;
    if (maxNeighborBlocks >= parityLength) {
      LOG.warn("Bad placement found. file:" + srcFile +
          " neighborBlocks:" + maxNeighborBlocks + " parityLength:" + parityLength);
    }
  }

  private static List<BlockInfo> getStripeBlocks(int stripeIndex,
      List<BlockInfo> srcBlocks, int stripeLength,
      List<BlockInfo> parityBlocks, int parityLength) {
    List<BlockInfo> stripeBlocks = new ArrayList<BlockInfo>();
    // Adding source blocks
    int stripeStart = stripeLength * stripeIndex;
    int stripeEnd = Math.min(
        stripeStart + stripeLength, srcBlocks.size());
    if (stripeStart < stripeEnd) {
      stripeBlocks.addAll(
          srcBlocks.subList(stripeStart, stripeEnd));
    }
    // Adding parity blocks
    stripeStart = parityLength * stripeIndex;
    stripeEnd = Math.min(
        stripeStart + parityLength, parityBlocks.size());
    if (stripeStart < stripeEnd) {
      stripeBlocks.addAll(parityBlocks.subList(stripeStart, stripeEnd));
    }
    return stripeBlocks;
  }

  private static void countBlocksOnEachNode(List<BlockInfo> stripeBlocks,
      Map<String, Integer> nodeToNumBlocks,
      Set<String> nodesInThisStripe) throws IOException {
    nodeToNumBlocks.clear();
    nodesInThisStripe.clear();
    for (BlockInfo block : stripeBlocks) {
      for (String node : block.getNames()) {
        Integer n = nodeToNumBlocks.get(node);
        if (n == null) {
          n = 0;
        }
        nodeToNumBlocks.put(node, n + 1);
        nodesInThisStripe.add(node);
      }
    }
  }

  private static void updateBlockPlacementHistogram(
      Map<String, Integer> nodeToNumBlocks,
      Map<Integer, Long> blockHistogram) {
    for (Integer numBlocks : nodeToNumBlocks.values()) {
      Long n = blockHistogram.get(numBlocks - 1);
      if (n == null) {
        n = 0L;
      }
      // Number of neighbor blocks to number of blocks
      blockHistogram.put(numBlocks - 1, n + 1);
    }
  }

  private void submitBlockMoves(Map<String, Integer> nodeToNumBlocks,
      List<BlockInfo> stripeBlocks, Set<String> excludedNodes,
      BlockAndDatanodeResolver resolver) throws IOException {
    // For all the nodes that has more than 2 blocks, find and move the blocks
    // so that there are only one block left on this node.
    for (String node : nodeToNumBlocks.keySet()) {
      int numberOfNeighborBlocks = nodeToNumBlocks.get(node) - 1;
      if (numberOfNeighborBlocks == 0) {
        // Most of the time we will be hitting this
        continue;
      }
      boolean skip = true;
      for (BlockInfo block : stripeBlocks) {
        for (String otherNode : block.getNames()) {
          if (node.equals(otherNode)) {
            if (skip) {
              // leave the first block where it is
              skip = false;
              break;
            }
            int priority = numberOfNeighborBlocks;
            LocatedBlockWithMetaInfo lb = resolver.getLocatedBlock(block);
            DatanodeInfo datanode = resolver.getDatanodeInfo(node);
            Set<DatanodeInfo> excludedDatanodes = new HashSet<DatanodeInfo>();
            for (String name : excludedNodes) {
              excludedDatanodes.add(resolver.getDatanodeInfo(name));
            }
            blockMover.move(lb, datanode, excludedDatanodes, priority,
                lb.getDataProtocolVersion(), lb.getNamespaceID());
            break;
          }
        }
      }
    }
  }

  /**
   * Report the placement histogram to {@link RaidNodeMetrics}. This should only
   * be called right after a complete parity file traversal is done.
   */
  public void clearAndReport() {
    synchronized (metrics) {
      int extra = 0;
      for (Entry<Integer, Long> e :
          blockHistograms.get(ErasureCodeType.RS).entrySet()) {
        if (e.getKey() < metrics.misplacedRs.length - 1) {
          metrics.misplacedRs[e.getKey()].set(e.getValue());
        } else {
          extra += e.getValue();
        }
      }
      metrics.misplacedRs[metrics.misplacedRs.length - 1].set(extra);
      extra = 0;
      for (Entry<Integer, Long> e :
          blockHistograms.get(ErasureCodeType.XOR).entrySet()) {
        if (e.getKey() < metrics.misplacedXor.length - 1) {
          metrics.misplacedXor[e.getKey()].set(e.getValue());
        } else {
          extra += e.getValue();
        }
      }
      metrics.misplacedXor[metrics.misplacedXor.length - 1].set(extra);
    }
    lastBlockHistograms = blockHistograms;
    lastUpdateFinishTime = RaidNode.now();
    lastUpdateUsedTime = lastUpdateFinishTime - lastUpdateStartTime;
    LOG.info("Reporting metrices:\n" + toString());
    blockHistograms = createEmptyHistograms();
  }

  @Override
  public String toString() {
    if (lastBlockHistograms == null) {
      return "Not available";
    }
    String result = "";
    for (ErasureCodeType code : ErasureCodeType.values()) {
      Map<Integer, Long> histo = lastBlockHistograms.get(code);
      result += code + " Blocks\n";
      List<Integer> neighbors = new ArrayList<Integer>();
      neighbors.addAll(histo.keySet());
      Collections.sort(neighbors);
      for (Integer i : neighbors) {
        Long numBlocks = histo.get(i);
        result += i + " co-localted blocks:" + numBlocks + "\n";
      }
    }
    return result;
  }

  public String htmlTable() {
    if (lastBlockHistograms == null) {
      return "Not available";
    }
    int max = computeMaxColocatedBlocks();
    String head = "";
    for (int i = 0; i <= max; ++i) {
      head += JspUtils.td(i + "");
    }
    head = JspUtils.tr(JspUtils.td("CODE") + head);
    String result = head;
    for (ErasureCodeType code : ErasureCodeType.values()) {
      String row = JspUtils.td(code.toString());
      Map<Integer, Long> histo = lastBlockHistograms.get(code);
      for (int i = 0; i <= max; ++i) {
        Long numBlocks = histo.get(i);
        numBlocks = numBlocks == null ? 0 : numBlocks;
        row += JspUtils.td(StringUtils.humanReadableInt(numBlocks));
      }
      row = JspUtils.tr(row);
      result += row;
    }
    return JspUtils.table(result);
  }

  public long lastUpdateTime() {
    return lastUpdateFinishTime;
  }

  public long lastUpdateUsedTime() {
    return lastUpdateUsedTime;
  }

  private int computeMaxColocatedBlocks() {
    int max = 0;
    for (ErasureCodeType code : ErasureCodeType.values()) {
      Map<Integer, Long> histo = lastBlockHistograms.get(code);
      for (Integer i : histo.keySet()) {
        max = Math.max(i, max);
      }
    }
    return max;
  }

  /**
   * Translates {@link BlockLocation} to {@link LocatedBlockLocation} and
   * Datanode host:port to {@link DatanodeInfo}
   */
  static class BlockAndDatanodeResolver {
    final Path src;
    final FileSystem srcFs;
    final Path parity;
    final FileSystem parityFs;

    private boolean inited = false;
    private Map<String, DatanodeInfo> nameToDatanodeInfo = null;
    private Map<Path, Map<Long, LocatedBlockWithMetaInfo>>
      pathAndOffsetToLocatedBlock = null;

    // For test
    BlockAndDatanodeResolver() {
      this.src = null;
      this.srcFs = null;
      this.parity =null;
      this.parityFs = null;
    }

    BlockAndDatanodeResolver(
        Path src, FileSystem srcFs, Path parity, FileSystem parityFs) {
      this.src = src;
      this.srcFs = srcFs;
      this.parity = parity;
      this.parityFs = parityFs;
    }

    public LocatedBlockWithMetaInfo getLocatedBlock(BlockInfo blk) throws IOException {
      checkInitialized();
      Map<Long, LocatedBlockWithMetaInfo> offsetToLocatedBlock =
          pathAndOffsetToLocatedBlock.get(blk.file);
      if (offsetToLocatedBlock != null) {
        LocatedBlockWithMetaInfo lb = offsetToLocatedBlock.get(
            blk.blockLocation.getOffset());
        if (lb != null) {
          return lb;
        }
      }
      // This should not happen
      throw new IOException("Cannot find the " + LocatedBlock.class +
          " for the block in file:" + blk.file +
          " offset:" + blk.blockLocation.getOffset());
    }

    public DatanodeInfo getDatanodeInfo(String name) throws IOException {
      checkInitialized();
      return nameToDatanodeInfo.get(name);
    }

    private void checkInitialized() throws IOException{
      if (inited) {
        return;
      }
      initialize();
      inited = true;
    }
    private void initialize() throws IOException {
      pathAndOffsetToLocatedBlock =
          new HashMap<Path, Map<Long, LocatedBlockWithMetaInfo>>();
      VersionedLocatedBlocks srcLbs = getLocatedBlocks(src, srcFs);
      VersionedLocatedBlocks parityLbs = getLocatedBlocks(parity, parityFs);
      pathAndOffsetToLocatedBlock.put(
          src, createOffsetToLocatedBlockMap(srcLbs));
      pathAndOffsetToLocatedBlock.put(
          parity, createOffsetToLocatedBlockMap(parityLbs));

      nameToDatanodeInfo = new HashMap<String, DatanodeInfo>();
      for (LocatedBlocks lbs : Arrays.asList(srcLbs, parityLbs)) {
        for (LocatedBlock lb : lbs.getLocatedBlocks()) {
          for (DatanodeInfo dn : lb.getLocations()) {
            nameToDatanodeInfo.put(dn.getName(), dn);
          }
        }
      }
    }

    private Map<Long, LocatedBlockWithMetaInfo> createOffsetToLocatedBlockMap(
        VersionedLocatedBlocks lbs) {
      Map<Long, LocatedBlockWithMetaInfo> result =
          new HashMap<Long, LocatedBlockWithMetaInfo>();
      if (lbs instanceof LocatedBlocksWithMetaInfo) {
        LocatedBlocksWithMetaInfo lbsm = (LocatedBlocksWithMetaInfo)lbs;
        for (LocatedBlock lb : lbs.getLocatedBlocks()) {
          result.put(lb.getStartOffset(), new LocatedBlockWithMetaInfo(
              lb.getBlock(), lb.getLocations(), lb.getStartOffset(),
              lbsm.getDataProtocolVersion(), lbsm.getNamespaceID(),
              lbsm.getMethodFingerPrint()));
        }
      } else {
        for (LocatedBlock lb : lbs.getLocatedBlocks()) {
          result.put(lb.getStartOffset(), new LocatedBlockWithMetaInfo(
              lb.getBlock(), lb.getLocations(), lb.getStartOffset(),
              lbs.getDataProtocolVersion(), 0, 0));
        }
      }
      return result;
    }

    private VersionedLocatedBlocks getLocatedBlocks(Path file, FileSystem fs)
        throws IOException {
      if (!(fs instanceof DistributedFileSystem)) {
        throw new IOException("Cannot obtain " + LocatedBlocks.class +
            " from " + fs.getClass().getSimpleName());
      }
      DistributedFileSystem dfs = (DistributedFileSystem) fs;
      if (DFSClient.isMetaInfoSuppoted(dfs.getClient().namenodeProtocolProxy)) {
        LocatedBlocksWithMetaInfo lbwmi = 
        dfs.getClient().namenode.openAndFetchMetaInfo(
            file.toUri().getPath(), 0, Long.MAX_VALUE);
        dfs.getClient().getNewNameNodeIfNeeded(lbwmi.getMethodFingerPrint());
        return lbwmi;
      }
      return dfs.getClient().namenode.open(
          file.toUri().getPath(), 0, Long.MAX_VALUE);
    }
  }
}
