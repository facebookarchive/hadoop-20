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

package org.apache.hadoop.mapred.lib;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.EnumMap;
import java.util.LinkedList;
import java.util.HashSet;
import java.util.List;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.Set;
import java.util.Iterator;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.net.NodeBase;
import org.apache.hadoop.net.NetworkTopology;

import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.RecordReader;
import org.mortbay.util.ajax.JSON;

/**
 * An abstract {@link org.apache.hadoop.mapred.InputFormat} that returns {@link CombineFileSplit}'s
 * in {@link org.apache.hadoop.mapred.InputFormat#getSplits(JobConf, int)} method.
 * Splits are constructed from the files under the input paths.
 * A split cannot have files from different pools.
 * Each split returned may contain blocks from different files.
 * If a maxSplitSize is specified, then blocks on the same node are
 * combined to form a single split. Blocks that are left over are
 * then combined with other blocks in the same rack.
 * If maxSplitSize is not specified, then blocks from the same rack
 * are combined in a single split; no attempt is made to create
 * node-local splits.
 * If the maxSplitSize is equal to the block size, then this class
 * is similar to the default spliting behaviour in Hadoop: each
 * block is a locally processed split.
 * Subclasses implement {@link org.apache.hadoop.mapred.InputFormat#getRecordReader(InputSplit, JobConf, Reporter)}
 * to construct <code>RecordReader</code>'s for <code>CombineFileSplit</code>'s.
 * @see CombineFileSplit
 */
public abstract class CombineFileInputFormat<K, V>
  extends FileInputFormat<K, V> {

  // ability to limit the size of a single split
  private long maxSplitSize = 0;
  private long minSplitSizeNode = 0;
  private long minSplitSizeRack = 0;
  private long maxNumBlocksPerSplit = 0;

  // A pool of input paths filters. A split cannot have blocks from files
  // across multiple pools.
  private ArrayList<MultiPathFilter> pools = new  ArrayList<MultiPathFilter>();

  // mapping from a rack name to the set of Nodes in the rack
  private HashMap<String, Set<String>> rackToNodes =
                            new HashMap<String, Set<String>>();

  // Whether to pass only the path component of the URI to the pool filters
  private boolean poolFilterPathOnly = true;

  // Special log for json metrics (all split stats sent here for easier
  // parsing)
  private static final Log JSON_METRICS_LOG = LogFactory.getLog("JsonMetrics");

  // Map of the stat type to actual stats
  private final EnumMap<SplitType, SplitTypeStats> splitTypeStatsMap =
    new EnumMap<SplitType, SplitTypeStats>(SplitType.class);

  // Split statistics types
  private enum SplitType {
    SINGLE_BLOCK_LOCAL,
    NODE_LOCAL, NODE_LOCAL_LEFTOVER,
    RACK_LOCAL, RACK_LOCAL_LEFTOVER,
    OVERFLOW, OVERFLOW_LEFTOVER, ALL
  }

  /** Are the split types stats valid? */
  private boolean isSplitTypeStatsValid = true;

  /**
   * Get whether the type stats are valid.  Used for testing.
   *
   * @return true if the type stats are valid, false otherwise
   */
  public boolean isTypeStatsValid() {
    return isSplitTypeStatsValid;
  }

  /**
   * Stats associated with a split type
   */
  private class SplitTypeStats {
    private int totalSplitCount = 0;
    private long totalSize = 0;
    private long totalBlockCount = 0;
    private long totalHostCount = 0;

    /**
     * Add a split for this type
     * @param splitSize Size of the split
     * @param hostCount Hosts listed for this split
     * @param blockCount Blocks in this split
     */
    public void addSplit(long splitSize, long hostCount, long blockCount) {
      ++totalSplitCount;
      totalSize += splitSize;
      totalBlockCount += blockCount;
      totalHostCount += hostCount;
    }

    public int getTotalSplitCount() {
      return totalSplitCount;
    }

    public long getTotalSize() {
      return totalSize;
    }

    public long getTotalHostCount() {
      return totalHostCount;
    }

    public long getTotalBlockCount() {
      return totalBlockCount;
    }
  }

  /**
   * Add stats for a split type (i.e node local splits,
   * rack local splits, etc.) and keep a total count.
   * @param splitSize Size of the split
   * @param hostCount Hosts listed for this split
   * @param blockCount Blocks in this split
   */
  private void addStatsForSplitType(
      SplitType splitType, long splitSize, long hostCount, long blockCount) {
    SplitTypeStats splitTypeStats = splitTypeStatsMap.get(splitType);
    if (splitTypeStats == null) {
      splitTypeStats = new SplitTypeStats();
      splitTypeStatsMap.put(splitType, splitTypeStats);
    }
    splitTypeStats.addSplit(splitSize, hostCount, blockCount);

    // Add all splits to the ALL split type
    if (splitType != SplitType.ALL) {
      addStatsForSplitType(SplitType.ALL, splitSize, hostCount, blockCount);
    }
  }

  /**
   * Get stats for every split type as a string
   * @return String of all split type stats
   */
  private String getStatsString() {
    SplitTypeStats allTypeStats = splitTypeStatsMap.get(SplitType.ALL);
    Map<String, Map<String, Number>> statsMapMap =
      new HashMap<String, Map<String, Number>>();
    for (Map.Entry<SplitType, SplitTypeStats> entry :
        splitTypeStatsMap.entrySet()) {
      Map<String, Number> statsMap = new HashMap<String, Number>();
      statsMapMap.put(entry.getKey().toString(), statsMap);

      float percentTotalSplitCount =
        (100f * entry.getValue().getTotalSplitCount()) /
        allTypeStats.getTotalSplitCount();
      float percentTotalSize =
        (100f * entry.getValue().getTotalSize()) /
        allTypeStats.getTotalSize();
      float percentTotalBlockCount =
          (100f * entry.getValue().getTotalBlockCount()) /
          allTypeStats.getTotalBlockCount();
      float averageSizePerSplit =
          ((float) entry.getValue().getTotalSize()) /
          entry.getValue().getTotalSplitCount();
      float averageHostCountPerSplit =
         ((float) entry.getValue().getTotalHostCount()) /
         entry.getValue().getTotalSplitCount();
      float averageBlockCountPerSplit =
          ((float) entry.getValue().getTotalBlockCount()) /
          entry.getValue().getTotalSplitCount();
      statsMap.put("totalSplitCount", entry.getValue().getTotalSplitCount());
      statsMap.put("percentTotalSplitCount", percentTotalSplitCount);
      statsMap.put("totalSize", entry.getValue().getTotalSize());
      statsMap.put("percentTotalSize", percentTotalSize);
      statsMap.put("averageSizePerSplit", averageSizePerSplit);
      statsMap.put("totalHostCount", entry.getValue().getTotalHostCount());
      statsMap.put("averageHostCountPerSplit", averageHostCountPerSplit);
      statsMap.put("totalBlockCount", entry.getValue().getTotalBlockCount());
      statsMap.put("percentTotalBlockCount", percentTotalBlockCount);
      statsMap.put("averageBlockCountPerSplit", averageBlockCountPerSplit);
    }
    return JSON.toString(statsMapMap);
  }

  /**
   * Specify the maximum size (in bytes) of each split. Each split is
   * approximately equal to the specified size.
   */
  protected void setMaxSplitSize(long maxSplitSize) {
    this.maxSplitSize = maxSplitSize;
  }

  /**
   * Specify the maximum number of blocks in each split.
   */
  protected void setMaxNumBlocksPerSplit(long maxNumBlocksPerSplit) {
    this.maxNumBlocksPerSplit = maxNumBlocksPerSplit;
  }

  /**
   * Specify the minimum size (in bytes) of each split per node.
   * This applies to data that is left over after combining data on a single
   * node into splits that are of maximum size specified by maxSplitSize.
   * This leftover data will be combined into its own split if its size
   * exceeds minSplitSizeNode.
   */
  protected void setMinSplitSizeNode(long minSplitSizeNode) {
    this.minSplitSizeNode = minSplitSizeNode;
  }

  /**
   * Specify the minimum size (in bytes) of each split per rack.
   * This applies to data that is left over after combining data on a single
   * rack into splits that are of maximum size specified by maxSplitSize.
   * This leftover data will be combined into its own split if its size
   * exceeds minSplitSizeRack.
   */
  protected void setMinSplitSizeRack(long minSplitSizeRack) {
    this.minSplitSizeRack = minSplitSizeRack;
  }

  /**
   * Create a new pool and add the filters to it.
   * A split cannot have files from different pools.
   */
  protected void createPool(JobConf conf, List<PathFilter> filters) {
    pools.add(new MultiPathFilter(filters));
  }

  /**
   * Create a new pool and add the filters to it.
   * A pathname can satisfy any one of the specified filters.
   * A split cannot have files from different pools.
   */
  protected void createPool(JobConf conf, PathFilter... filters) {
    MultiPathFilter multi = new MultiPathFilter();
    for (PathFilter f: filters) {
      multi.add(f);
    }
    pools.add(multi);
  }

  private CompressionCodecFactory compressionCodecs =
    new CompressionCodecFactory(new JobConf());

  @Override
  protected boolean isSplitable(FileSystem ignored, Path file) {
    return compressionCodecs.getCodec(file) == null;
  }

  /**
   * default constructor
   */
  public CombineFileInputFormat() {
    // Add the all stats, in case of no splits
    splitTypeStatsMap.put(SplitType.ALL, new SplitTypeStats());
  }

  /**
   *
   * @param pathOnly If true, pass only the path component of input paths (i.e.
   * strip out the scheme and authority) to the pool filters
   */
  protected void setPoolFilterPathOnly(boolean pathOnly) {
    poolFilterPathOnly = pathOnly;
  }

  protected boolean getPoolFilterPathOnly() {
    return poolFilterPathOnly;
  }

  @Override
  public InputSplit[] getSplits(JobConf job, int numSplits)
      throws IOException {
    long minSizeNode = 0;
    long minSizeRack = 0;
    long maxSize = 0;
    long maxNumBlocks = 0;

    // the values specified by setxxxSplitSize() takes precedence over the
    // values that might have been specified in the config
    if (minSplitSizeNode != 0) {
      minSizeNode = minSplitSizeNode;
    } else {
      minSizeNode = job.getLong("mapred.min.split.size.per.node", 0);
    }
    if (maxNumBlocksPerSplit != 0) {
      maxNumBlocks = maxNumBlocksPerSplit;
    } else {
      maxNumBlocks = job.getLong("mapred.max.num.blocks.per.split", 0);
    }
    if (minSplitSizeRack != 0) {
      minSizeRack = minSplitSizeRack;
    } else {
      minSizeRack = job.getLong("mapred.min.split.size.per.rack", 0);
    }
    if (maxSplitSize != 0) {
      maxSize = maxSplitSize;
    } else {
      maxSize = job.getLong("mapred.max.split.size", 0);
    }
    if (minSizeNode != 0 && maxSize != 0 && minSizeNode > maxSize) {
      throw new IOException("Minimum split size pernode " + minSizeNode +
                            " cannot be larger than maximum split size " +
                            maxSize);
    }
    if (minSizeRack != 0 && maxSize != 0 && minSizeRack > maxSize) {
      throw new IOException("Minimum split size per rack " + minSizeRack +
                            " cannot be larger than maximum split size " +
                            maxSize);
    }
    if (minSizeRack != 0 && minSizeNode > minSizeRack) {
      throw new IOException("Minimum split size per rack " + minSizeRack +
                            " cannot be smaller than minimum split size per node " +
                            minSizeNode);
    }

    // all the files in input set
    LocatedFileStatus[] stats = listLocatedStatus(job);
    long totalLen = 0;
    for (LocatedFileStatus stat: stats) {
      totalLen += stat.getLen();
    }
    List<CombineFileSplit> splits = new ArrayList<CombineFileSplit>();
    if (stats.length == 0) {
      return splits.toArray(new CombineFileSplit[splits.size()]);
    }

    // Put them into a list for easier removal during iteration
    Collection<LocatedFileStatus> newstats = new LinkedList<LocatedFileStatus>();
    Collections.addAll(newstats, stats);
    stats = null;

    // In one single iteration, process all the paths in a single pool.
    // Processing one pool at a time ensures that a split contains paths
    // from a single pool only.
    for (MultiPathFilter onepool : pools) {
      ArrayList<LocatedFileStatus> myStats = new ArrayList<LocatedFileStatus>();

      // pick one input path. If it matches all the filters in a pool,
      // add it to the output set
      for (Iterator<LocatedFileStatus> iter = newstats.iterator();
           iter.hasNext();) {
        LocatedFileStatus stat = iter.next();
        if (onepool.accept(stat.getPath(), poolFilterPathOnly)) {
          myStats.add(stat); // add it to my output set
          iter.remove();
        }
      }
      // create splits for all files in this pool.
      getMoreSplits(job, myStats,
                    maxSize, minSizeNode, minSizeRack, maxNumBlocks, splits);
    }

    // create splits for all files that are not in any pool.
    getMoreSplits(job, newstats,
                  maxSize, minSizeNode, minSizeRack, maxNumBlocks, splits);

    // free up rackToNodes map
    rackToNodes.clear();
    verifySplits(job, totalLen, splits);

    // Print the stats of the splits to the special json metrics log for easier
    // parsing.  Also, clean up the stats after each getSplits() call since
    // others may call it multiple times (i.e. CombineHiveInputFormat)
    JSON_METRICS_LOG.info(getStatsString());
    splitTypeStatsMap.clear();

    return splits.toArray(new CombineFileSplit[splits.size()]);
  }

  private void verifySplits(
      JobConf conf, long totalLen, List<CombineFileSplit> splits)
      throws IOException {
    if (!conf.getBoolean("mapred.fileinputformat.verifysplits", true)) {
      return;
    }
    long totalSplitLen = 0;
    for (CombineFileSplit split: splits) {
      totalSplitLen += split.getLength();
    }

    if (totalLen != totalSplitLen) {
      throw new IOException("Total length expected is " + totalLen +
        ", but total split length is " + totalSplitLen);
    }

    if (splitTypeStatsMap.get(SplitType.ALL).getTotalSize() != totalSplitLen) {
      LOG.error("Total length expected is " + totalLen +
        ", but total split length according to stats is " +
        splitTypeStatsMap.get(SplitType.ALL).getTotalSize() +
        ", previous isSplitTypeStatsValid = " +  isSplitTypeStatsValid);
      isSplitTypeStatsValid = false;
    }
  }

  /**
   * Comparator to be used with sortBlocksBySize to sort from largest to
   * smallest.
   */
  private class OneBlockInfoSizeComparator implements Comparator<OneBlockInfo> {
    @Override
    public int compare(OneBlockInfo left, OneBlockInfo right) {
      return (int) (right.length - left.length);
    }
  }

  /**
   * Sort the blocks on each node by size, largest to smallest
   *
   * @param nodeToBlocks Map of nodes to all blocks on that node
   */
  private void sortBlocksBySize(Map<String, List<OneBlockInfo>> nodeToBlocks) {
    OneBlockInfoSizeComparator comparator = new OneBlockInfoSizeComparator();
    for (Entry<String, List<OneBlockInfo>> entry : nodeToBlocks.entrySet()) {
      Collections.sort(entry.getValue(), comparator);
    }
  }

  /**
   * Return all the splits in the specified set of paths
   */
  private void getMoreSplits(JobConf job, Collection<LocatedFileStatus> stats,
                             long maxSize, long minSizeNode,
                             long minSizeRack, long maxNumBlocksPerSplit,
                             List<CombineFileSplit> splits)
    throws IOException {

    // all blocks for all the files in input set
    OneFileInfo[] files;

    // mapping from a rack name to the list of blocks it has
    HashMap<String, List<OneBlockInfo>> rackToBlocks =
                              new HashMap<String, List<OneBlockInfo>>();

    // mapping from a block to the nodes on which it has replicas
    HashMap<OneBlockInfo, String[]> blockToNodes =
                              new HashMap<OneBlockInfo, String[]>();

    // mapping from a node to the list of blocks that it contains
    HashMap<String, List<OneBlockInfo>> nodeToBlocks =
                              new HashMap<String, List<OneBlockInfo>>();

    if (stats.isEmpty()) {
      return;
    }
    files = new OneFileInfo[stats.size()];

    // populate all the blocks for all files
    long totLength = 0;
    int fileIndex = 0;
    for (LocatedFileStatus oneStatus : stats) {
      files[fileIndex] = new OneFileInfo(oneStatus, job,
          isSplitable(FileSystem.get(job), oneStatus.getPath()),
          rackToBlocks, blockToNodes, nodeToBlocks, rackToNodes, maxSize);
      totLength += files[fileIndex].getLength();
      fileIndex++;
    }

    // Sort the blocks on each node from biggest to smallest by size to
    // encourage more node-local single block splits
    sortBlocksBySize(nodeToBlocks);

    ArrayList<OneBlockInfo> validBlocks = new ArrayList<OneBlockInfo>();
    Set<String> nodes = new HashSet<String>();
    long curSplitSize = 0;

    // process all nodes and create splits that are local
    // to a node.
    for (Iterator<Map.Entry<String,
         List<OneBlockInfo>>> iter = nodeToBlocks.entrySet().iterator();
         iter.hasNext();) {

      Map.Entry<String, List<OneBlockInfo>> one = iter.next();
      nodes.add(one.getKey());
      List<OneBlockInfo> blocksInNode = one.getValue();

      // for each block, copy it into validBlocks. Delete it from
      // blockToNodes so that the same block does not appear in
      // two different splits.
      for (OneBlockInfo oneblock : blocksInNode) {
        if (blockToNodes.containsKey(oneblock)) {
          validBlocks.add(oneblock);
          blockToNodes.remove(oneblock);
          curSplitSize += oneblock.length;

          // if the accumulated split size exceeds the maximum, then
          // create this split.
          if ((maxSize != 0 && curSplitSize >= maxSize) ||
              (maxNumBlocksPerSplit > 0 && validBlocks.size() >= maxNumBlocksPerSplit)) {
            // create an input split and add it to the splits array
            // if only one block, add all the node replicas
            if (validBlocks.size() == 1) {
              Set<String> blockLocalNodes =
                new HashSet<String>(Arrays.asList(validBlocks.get(0).hosts));
              addCreatedSplit(job, splits, blockLocalNodes, validBlocks);
              addStatsForSplitType(SplitType.SINGLE_BLOCK_LOCAL, curSplitSize,
                                   blockLocalNodes.size(), validBlocks.size());
            } else {
              addCreatedSplit(job, splits, nodes, validBlocks);
              addStatsForSplitType(SplitType.NODE_LOCAL, curSplitSize,
                                   nodes.size(), validBlocks.size());
            }
            curSplitSize = 0;
            validBlocks.clear();
          }
        }
      }
      // if there were any blocks left over and their combined size is
      // larger than minSplitNode, then combine them into one split.
      // Otherwise add them back to the unprocessed pool. It is likely
      // that they will be combined with other blocks from the same rack later on.
      if (minSizeNode != 0 && curSplitSize >= minSizeNode) {
        // create an input split and add it to the splits array
        addCreatedSplit(job, splits, nodes, validBlocks);
        addStatsForSplitType(SplitType.NODE_LOCAL_LEFTOVER, curSplitSize,
                             nodes.size(), validBlocks.size());
      } else {
        for (OneBlockInfo oneblock : validBlocks) {
          blockToNodes.put(oneblock, oneblock.hosts);
        }
      }
      validBlocks.clear();
      nodes.clear();
      curSplitSize = 0;
    }

    // if blocks in a rack are below the specified minimum size, then keep them
    // in 'overflow'. After the processing of all racks is complete, these overflow
    // blocks will be combined into splits.
    ArrayList<OneBlockInfo> overflowBlocks = new ArrayList<OneBlockInfo>();
    Set<String> racks = new HashSet<String>();

    // Process all racks over and over again until there is no more work to do.
    boolean noRacksMadeSplit = false;
    while (blockToNodes.size() > 0) {

      // Create one split for this rack before moving over to the next rack.
      // Come back to this rack after creating a single split for each of the
      // remaining racks.
      // Process one rack location at a time, Combine all possible blocks that
      // reside on this rack as one split. (constrained by minimum and maximum
      // split size).

      // Iterate over all racks.  Add to the overflow blocks only if at least
      // one pass over all the racks was completed without adding any splits
      long splitsAddedOnAllRacks = 0;
      for (Iterator<Map.Entry<String, List<OneBlockInfo>>> iter =
           rackToBlocks.entrySet().iterator(); iter.hasNext();) {

        Map.Entry<String, List<OneBlockInfo>> one = iter.next();
        racks.add(one.getKey());
        List<OneBlockInfo> blocks = one.getValue();

        // for each block, copy it into validBlocks. Delete it from
        // blockToNodes so that the same block does not appear in
        // two different splits.
        boolean createdSplit = false;
        for (OneBlockInfo oneblock : blocks) {
          if (blockToNodes.containsKey(oneblock)) {
            validBlocks.add(oneblock);
            blockToNodes.remove(oneblock);
            curSplitSize += oneblock.length;

            // if the accumulated split size exceeds the maximum, then
            // create this split.
            if ((maxSize != 0 && curSplitSize >= maxSize) ||
                (maxNumBlocksPerSplit > 0 && validBlocks.size() >= maxNumBlocksPerSplit)) {
              // create an input split and add it to the splits array
              addCreatedSplit(job, splits, getHosts(racks), validBlocks);
              addStatsForSplitType(SplitType.RACK_LOCAL, curSplitSize,
                                   getHosts(racks).size(), validBlocks.size());
              createdSplit = true;
              ++splitsAddedOnAllRacks;
              break;
            }
          }
        }

        // if we created a split, then just go to the next rack
        if (createdSplit) {
          curSplitSize = 0;
          validBlocks.clear();
          racks.clear();
          continue;
        }

        if (!validBlocks.isEmpty()) {
          if (minSizeRack != 0 && curSplitSize >= minSizeRack) {
            // if there is a mimimum size specified, then create a single split
            // otherwise, store these blocks into overflow data structure
            addCreatedSplit(job, splits, getHosts(racks), validBlocks);
            addStatsForSplitType(SplitType.RACK_LOCAL_LEFTOVER, curSplitSize,
                                 getHosts(racks).size(), validBlocks.size());
            ++splitsAddedOnAllRacks;
          } else if (!noRacksMadeSplit) {
            // Add the blocks back if a pass on all rack found at least one
            // split or this is the first pass
            for (OneBlockInfo oneblock : validBlocks) {
              blockToNodes.put(oneblock, oneblock.hosts);
            }
          } else {
            // There were a few blocks in this rack that remained to be processed.
            // Keep them in 'overflow' block list. These will be combined later.
            overflowBlocks.addAll(validBlocks);
          }
        }
        curSplitSize = 0;
        validBlocks.clear();
        racks.clear();
      }

      if (splitsAddedOnAllRacks == 0) {
        noRacksMadeSplit = true;
      }
    }

    assert blockToNodes.isEmpty();
    assert curSplitSize == 0;
    assert validBlocks.isEmpty();
    assert racks.isEmpty();

    // Process all overflow blocks
    for (OneBlockInfo oneblock : overflowBlocks) {
      validBlocks.add(oneblock);
      curSplitSize += oneblock.length;

      // This might cause an exiting rack location to be re-added,
      // but it should be OK because racks is a Set.
      for (int i = 0; i < oneblock.racks.length; i++) {
        racks.add(oneblock.racks[i]);
      }

      // if the accumulated split size exceeds the maximum, then
      // create this split.
      if ((maxSize != 0 && curSplitSize >= maxSize) ||
          (maxNumBlocksPerSplit > 0 && validBlocks.size() >= maxNumBlocksPerSplit)) {
        // create an input split and add it to the splits array
        addCreatedSplit(job, splits, getHosts(racks), validBlocks);
        addStatsForSplitType(SplitType.OVERFLOW, curSplitSize,
                             getHosts(racks).size(), validBlocks.size());
        curSplitSize = 0;
        validBlocks.clear();
        racks.clear();
      }
    }

    // Process any remaining blocks, if any.
    if (!validBlocks.isEmpty()) {
      addCreatedSplit(job, splits, getHosts(racks), validBlocks);
      addStatsForSplitType(SplitType.OVERFLOW_LEFTOVER, curSplitSize,
                           getHosts(racks).size(), validBlocks.size());
    }
  }

  /**
   * Create a single split from the list of blocks specified in validBlocks
   * Add this new split into splitList.
   */
  private void addCreatedSplit(JobConf job,
                               List<CombineFileSplit> splitList,
                               Collection<String> locations,
                               ArrayList<OneBlockInfo> validBlocks) {
    // create an input split
    Path[] fl = new Path[validBlocks.size()];
    long[] offset = new long[validBlocks.size()];
    long[] length = new long[validBlocks.size()];
    for (int i = 0; i < validBlocks.size(); i++) {
      fl[i] = validBlocks.get(i).onepath;
      offset[i] = validBlocks.get(i).offset;
      length[i] = validBlocks.get(i).length;
    }

     // add this split to the list that is returned
    CombineFileSplit thissplit = new CombineFileSplit(job, fl, offset,
                      length, locations.toArray(new String[locations.size()]));
    splitList.add(thissplit);
  }

  /**
   * This is not implemented yet.
   */
  public abstract RecordReader<K, V> getRecordReader(InputSplit split,
                                      JobConf job, Reporter reporter)
    throws IOException;

  /**
   * information about one file from the File System
   */
  private static class OneFileInfo {
    private long fileSize;               // size of the file
    private OneBlockInfo[] blocks;       // all blocks in this file

    OneFileInfo(LocatedFileStatus stat, JobConf job,
                boolean isSplitable,
                HashMap<String, List<OneBlockInfo>> rackToBlocks,
                HashMap<OneBlockInfo, String[]> blockToNodes,
                HashMap<String, List<OneBlockInfo>> nodeToBlocks,
                HashMap<String, Set<String>> rackToNodes,
                long maxSize)
                throws IOException {
      this.fileSize = 0;

      // get block locations from file system
      BlockLocation[] locations = stat.getBlockLocations();

      // create a list of all block and their locations
      if (locations == null || locations.length == 0) {
        blocks = new OneBlockInfo[0];
      } else {
        if (!isSplitable) {
          // if the file is not splitable, just create the one block with
          // full file length
          blocks = new OneBlockInfo[1];
          fileSize = stat.getLen();
          blocks[0] = new OneBlockInfo(stat.getPath(), 0, fileSize, locations[0]
              .getHosts(), locations[0].getTopologyPaths());
        } else {
          ArrayList<OneBlockInfo> blocksList = new ArrayList<OneBlockInfo>(locations.length);
          for (int i = 0; i < locations.length; i++) {

            fileSize += locations[i].getLength();

            // each split can be a maximum of maxSize
            long left = locations[i].getLength();
            long myOffset = locations[i].getOffset();
            long myLength = 0;
            while (left > 0) {
              if (maxSize == 0) {
                myLength = left;
              } else {
                if (left > maxSize && left < 2 * maxSize) {
                  // if remainder is between max and 2*max - then
                  // instead of creating splits of size max, left-max we
                  //  create splits of size left/2 and left/2.
                  myLength = left / 2;
                } else {
                  myLength = Math.min(maxSize, left);
                }
              }
              OneBlockInfo oneblock =  new OneBlockInfo(stat.getPath(),
                  myOffset,
                  myLength,
                  locations[i].getHosts(),
                  locations[i].getTopologyPaths());
              left -= myLength;
              myOffset += myLength;

              blocksList.add(oneblock);
            }
          }
          blocks = blocksList.toArray(new OneBlockInfo[blocksList.size()]);
        }

        for (OneBlockInfo oneblock : blocks) {
          // add this block to the block --> node locations map
          blockToNodes.put(oneblock, oneblock.hosts);

          // For blocks that do not have host/rack information,
          // assign to default  rack.
          String[] racks = null;
          if (oneblock.hosts.length == 0) {
            racks = new String[]{NetworkTopology.DEFAULT_RACK};
          } else {
            racks = oneblock.racks;
          }

          // add this block to the rack --> block map
          for (int j = 0; j < racks.length; j++) {
            String rack = racks[j];
            List<OneBlockInfo> blklist = rackToBlocks.get(rack);
            if (blklist == null) {
              blklist = new ArrayList<OneBlockInfo>();
              rackToBlocks.put(rack, blklist);
            }
            blklist.add(oneblock);
            if (!racks[j].equals(NetworkTopology.DEFAULT_RACK)) {
              // Add this host to rackToNodes map
              addHostToRack(rackToNodes, racks[j], oneblock.hosts[j]);
            }
          }

          // add this block to the node --> block map
          for (int j = 0; j < oneblock.hosts.length; j++) {
            String node = oneblock.hosts[j];
            List<OneBlockInfo> blklist = nodeToBlocks.get(node);
            if (blklist == null) {
              blklist = new ArrayList<OneBlockInfo>();
              nodeToBlocks.put(node, blklist);
            }
            blklist.add(oneblock);
          }
        }
      }
    }

    long getLength() {
      return fileSize;
    }

    OneBlockInfo[] getBlocks() {
      return blocks;
    }
  }

  /**
   * information about one block from the File System
   */
  private static class OneBlockInfo {
    Path onepath;                // name of this file
    long offset;                 // offset in file
    long length;                 // length of this block
    String[] hosts;              // nodes on whch this block resides
    String[] racks;              // network topology of hosts

    OneBlockInfo(Path path, long offset, long len,
                 String[] hosts, String[] topologyPaths) {
      this.onepath = path;
      this.offset = offset;
      this.hosts = hosts;
      this.length = len;
      assert (hosts.length == topologyPaths.length ||
              topologyPaths.length == 0);

      // if the file ystem does not have any rack information, then
      // use dummy rack location.
      if (topologyPaths.length == 0) {
        topologyPaths = new String[hosts.length];
        for (int i = 0; i < topologyPaths.length; i++) {
          topologyPaths[i] = (new NodeBase(hosts[i], NetworkTopology.DEFAULT_RACK)).
                                          toString();
        }
      }

      // The topology paths have the host name included as the last
      // component. Strip it.
      this.racks = new String[topologyPaths.length];
      for (int i = 0; i < topologyPaths.length; i++) {
        this.racks[i] = (new NodeBase(topologyPaths[i])).getNetworkLocation();
      }
    }
  }

  private static void addHostToRack(HashMap<String, Set<String>> rackToNodes,
                                    String rack, String host) {
    Set<String> hosts = rackToNodes.get(rack);
    if (hosts == null) {
      hosts = new HashSet<String>();
      rackToNodes.put(rack, hosts);
    }
    hosts.add(host);
  }

  private Set<String> getHosts(Set<String> racks) {
    Set<String> hosts = new HashSet<String>();
    for (String rack : racks) {
      if (rackToNodes.containsKey(rack)) {
        hosts.addAll(rackToNodes.get(rack));
      }
    }
    return hosts;
  }

 /**
   * Accept a path only if any one of filters given in the
   * constructor do.
   */
  private static class MultiPathFilter implements PathFilter {
    private List<PathFilter> filters;

    public MultiPathFilter() {
      this.filters = new ArrayList<PathFilter>();
    }

    public MultiPathFilter(List<PathFilter> filters) {
      this.filters = filters;
    }

    public void add(PathFilter one) {
      filters.add(one);
    }

    public boolean accept(Path path) {
      for (PathFilter filter : filters) {
        if (filter.accept(path)) {
          return true;
        }
      }
      return false;
    }

    /**
     *
     * @param path
     * @param pathOnly whether to strip out the scheme/authority before passing
     * to the constituent filters
     * @return whether the path matches all of the filters
     */
    public boolean accept(Path path, boolean pathOnly) {
      Path pathToCheck = path;
      if (pathOnly) {
        pathToCheck = new Path(path.toUri().getPath());
      }
      return accept(pathToCheck);
    }

    public String toString() {
      StringBuffer buf = new StringBuffer();
      buf.append("[");
      for (PathFilter f: filters) {
        buf.append(f);
        buf.append(",");
      }
      buf.append("]");
      return buf.toString();
    }
  }
}
