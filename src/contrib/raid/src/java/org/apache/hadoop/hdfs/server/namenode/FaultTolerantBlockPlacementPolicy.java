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

import org.apache.commons.logging.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.FSConstants;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.net.DNSToSwitchMapping;
import org.apache.hadoop.net.NetworkTopology;
import org.apache.hadoop.net.Node;
import org.apache.hadoop.net.NodeBase;
import org.apache.hadoop.util.HostsFileReader;
import org.apache.hadoop.raid.Codec;
import org.apache.hadoop.hdfs.server.namenode.BlockPlacementPolicyDefault;

import java.io.IOException;
import java.util.*;
import org.apache.commons.lang.ArrayUtils;

/**
 * This block placement policy tries (best effort) to the following:
 * 
 * If the file is under the staging directory (a specially named directory)
 * then all blocks of the file is kept on the same host. Additionally, all
 * the raid blocks (if any) for the same file is also kept on the same host.
 *
 * If the file is not under the staging directory then blocks are put in such
 * a way that all blocks within the same stripe end up on random hosts in
 * different racks. For example, the 10 data blocks and 4 parity blocks in a
 * stripe should end up in different racks.
 */
public class FaultTolerantBlockPlacementPolicy extends BlockPlacementPolicyRaid {
  private int stripeLen;
  private String stagingDir;
  private String localDir;
  private FSNamesystem namesystem = null;
  private boolean considerLoad;
  private List<Codec> acceptedCodecs = new ArrayList<Codec>();

  private static Set<String> badRacks = new HashSet<String>();
  private static Set<String> badHosts = new HashSet<String>();
  FaultTolerantBlockPlacementPolicy(Configuration conf,
                         FSClusterStats stats,
                               NetworkTopology clusterMap) {
    initialize(conf, stats, clusterMap, null, null, null);
  }

  FaultTolerantBlockPlacementPolicy() {
  }

  /** A function to be used by unit tests only */
  public static void setBadHostsAndRacks(Set<String> racks,
                                         Set<String> hosts) {
    badRacks = racks;
    badHosts = hosts;
  }

  /** {@inheritDoc} */
  public void initialize(Configuration conf,
                         FSClusterStats stats,
                         NetworkTopology clusterMap,
                         HostsFileReader hostsReader,
                         DNSToSwitchMapping dnsToSwitchMapping,
                         FSNamesystem ns) {
    super.initialize(
      conf, stats, clusterMap, hostsReader, dnsToSwitchMapping, ns);
    this.namesystem = ns;
    // Default
    this.stripeLen = 0;
    this.considerLoad = conf.getBoolean("dfs.replication.considerLoad", true);
    FSNamesystem.LOG.info("F4: Block placement will consider load: "
      + this.considerLoad);
    initParityConfigs();
    this.stagingDir = conf.get("dfs.f4.staging", "/staging");
    this.localDir = conf.get("dfs.f4.local", "/local");
  }

  /**
   * This function initializes configuration for the supported parities.
   *
   * Currently, we support RS and XOR. Those two can have different
   * configurations individually. Respective configurations will be used when
   * placing the parity files. There is one exception. The stripe length is
   * calculated based on the maximum of the stripe lengths of the individual
   * parities.
   */
  private void initParityConfigs() {
    Set<String> acceptedCodecIds = new HashSet<String>();
    for (String s : conf.get("dfs.f4.accepted.codecs", "rs,xor").split(",")) {
      acceptedCodecIds.add(s);
    }
    for (Codec c : Codec.getCodecs()) {
      if (acceptedCodecIds.contains(c.id)) {
        FSNamesystem.LOG.info("F4: Parity info."
          + " Id: " + c.id
          + " Parity Length: " + c.parityLength
          + " Parity Stripe Length: " + c.stripeLength
          + " Parity directory: " + c.parityDirectory
          + " Parity temp directory: " + c.tmpParityDirectory);
        acceptedCodecs.add(c);
        if (c.stripeLength > this.stripeLen) {
          // Use the max stripe length
          this.stripeLen = c.stripeLength;
        }
      }
    }
    FSNamesystem.LOG.info("F4: Initialized stripe len to: " + this.stripeLen);
  }

  private Codec getCodec(String fileName) {
    for (Codec c : this.acceptedCodecs) {
      // This should be "/raidrs/" or /"raid/". If any of these two is
      // is present in the file path, we will assume that is the parity type.
      String uniqueSubtringId = c.parityDirectory + "/";
      if (fileName.contains(uniqueSubtringId)) {
        return c;
      }
    }
    Codec c = this.acceptedCodecs.get(0);
    FSNamesystem.LOG.error("F4: Could not find any valid codec for the file: "
     + fileName + ", hence returning the first one: " + c.id);
    return c;
  }

  private String getParityStagingDir(String parityFileName) {
    Codec c  = getCodec(parityFileName);
    return c.parityDirectory + this.stagingDir;
  }

  private boolean isStaging(String fileName) {
    return fileName.startsWith(this.stagingDir) ||
      fileName.startsWith(this.getParityStagingDir(fileName));
  }
  private boolean isLocal(String fileName) {
    return fileName.startsWith(this.localDir);
  }

  @Override
  public DatanodeDescriptor[] chooseTarget(
      String srcPath,
      int numOfReplicas,
      DatanodeDescriptor writer,
      List<DatanodeDescriptor> chosenNodes,
      long blocksize) {
    return chooseTargetF4(
      srcPath, numOfReplicas, writer, chosenNodes, null, blocksize);
  }

  /**
   * This function finds a node where to place a block of a file under the
   * "local" directory. The basic idea is to have as few locations (preferably
   * one, and preferably on the writer node)
   *
   * 1) Choose a node that contains one of the blocks in the blocks argument.
   * 2) If there are multiple such nodes, choose one of them (in some order).
   * 3) If this is the first block, then choose the the writer node.
   * 4) If the writer node is not good, choose a random node within the same
   *    rack as the writer node.
   * 5) If the writer node is null or if all of the above tries fail, then
   *    just choose based on the the parent class's policy.
   *
   * @param fileName       The name of the file for which the block is to be
   *                       placed.
   * @param writer         The writer node.
   * @param blocks         The block locations that are to be used as reference
   *                       for placing the current block. For a data file, it
   *                       is the blocks for that file itself. For a raid file,
   *                       it is the blocks of the source file.
   * @param chosenNodes    @see chooseTarget
   * @param excludedNodes  @see chooseTarget
   * @param blocksize      @see chooseTarget
   */
  private DatanodeDescriptor[] chooseLocalTarget(
      String fileName,
      DatanodeDescriptor writer,
      LocatedBlocks blocks,
      List<Node> excludedNodes,
      List<DatanodeDescriptor> chosenNodes,
      long blocksize) throws IOException, NotEnoughReplicasException {
    // First try the same node as the one where other blocks reside.
    HashMap<String, DatanodeInfo> hostMap =
      new HashMap<String, DatanodeInfo>();
    for (LocatedBlock b : blocks.getLocatedBlocks()) {
      for (DatanodeInfo i : b.getLocations()) {
        hostMap.put(i.getNetworkLocation() + "/" + i.getName(), i);
      }
    }

    for (Map.Entry<String, DatanodeInfo> entry : hostMap.entrySet()) {
      DatanodeDescriptor result = null;
      DatanodeInfo i = entry.getValue();
      result = new DatanodeDescriptor(i,
                                      i.getNetworkLocation(),
                                      i.getHostName(),
                                      i.getCapacity(),
                                      i.getDfsUsed(),
                                      i.getRemaining(),
                                      i.getNamespaceUsed(),
                                      i.getXceiverCount());
      if (this.isGoodTarget(result,
                            blocksize,
                            Integer.MAX_VALUE,
                            this.considerLoad,
                            new ArrayList<DatanodeDescriptor>())) {
          // I dont care about per rack load.
        DatanodeDescriptor[] r = {result};
        return r;
      }
    }
    // Try something in the same rack as the writer.
    if (writer == null) {
      return super.chooseTarget(
        fileName, 1, writer, chosenNodes, excludedNodes, blocksize);
    } else if (this.isGoodTarget(writer,
                                 blocksize,
                                 Integer.MAX_VALUE,
                                 this.considerLoad,
                                 new ArrayList<DatanodeDescriptor>())) {
      DatanodeDescriptor[] r = {writer};
      return r;
    }
    HashMap<Node, Node> exclNodes = new HashMap<Node, Node>();
    for (Node n : excludedNodes) {
      exclNodes.put(n, n);
    }
    List<DatanodeDescriptor> results = new ArrayList<DatanodeDescriptor>();
    chooseRandom(
      1, writer.getNetworkLocation(), exclNodes, blocksize, 1, results);
    return results.toArray(new DatanodeDescriptor[results.size()]);
  }

  /// A helper function that says some hosts are bad based on test config.
  @Override
  protected boolean isGoodTarget(DatanodeDescriptor node,
                                 long blockSize,
                                 int maxPerRack,
                                 boolean considerLoad,
                                 List<DatanodeDescriptor> results) {
    if (badRacks.contains(node.getNetworkLocation()) ||
        badHosts.contains(node.getName())) {
      return false;
    }
    return super.isGoodTarget(
      node, blockSize, maxPerRack, considerLoad, results);
  }

  @Override
  public DatanodeDescriptor[] chooseTarget(
      String srcInode,
      int numOfReplicas,
      DatanodeDescriptor writer,
      List<DatanodeDescriptor> chosenNodes,
      List<Node> excludesNodes,
      long blocksize) {
    return chooseTargetF4(
      srcInode, numOfReplicas, writer, chosenNodes, excludesNodes, blocksize);
  }

  private String getSourceFileFromParity(String fileName,
                                         FileInfo info) 
      throws IOException {
    NameWithINode nameWithINode;
    switch (info.type) {
      case PARITY:
        // We need to support the following cases
        // parity = /raidrs/staging/X, source = /X
        // parity = /raidrs/X, source = /X
        nameWithINode = null;
        if (isStaging(fileName)) {
          nameWithINode = getSourceFile(fileName,
                                        getParityStagingDir(fileName));
        }
        if (nameWithINode == null) {
          Codec c = getCodec(fileName);
          nameWithINode = getSourceFile(fileName, c.parityDirectory);
        }
        return ((nameWithINode ==  null) ? null : nameWithINode.name);
      case TEMP_PARITY:
        Codec c = getCodec(fileName);
        nameWithINode = getSourceFile(fileName, c.tmpParityDirectory);
        return ((nameWithINode ==  null) ? null : nameWithINode.name);
      default:
        FSNamesystem.LOG.error("file type bad");
        return null;
    }
  }


  /**
   * This is the main driver function that dictates block placement.
   *
   * This function figures out the kind of file (staging or not, raid or not)
   * and invokes the appropriate functions
   */
  private DatanodeDescriptor[] chooseTargetF4(
      String fileName,
      int numOfReplicas,
      DatanodeDescriptor writer,
      List<DatanodeDescriptor> chosenNodes,
      List<Node> exclNodes,
      long blocksize) {
    FSNamesystem.LOG.info("F4: F4 policy invoked for file: " + fileName +
      ", with replica count: " + numOfReplicas);
    // If replica>1 then just default back to RAID
    if (numOfReplicas > 1) {
      return super.chooseTarget(
        numOfReplicas, writer, chosenNodes, exclNodes, blocksize);
    }
    FileInfo info;
    LocatedBlocks blocks;
    int blockIndex = -1;
    try {
      blocks = this.namesystem.getBlockLocations(fileName, 0, Long.MAX_VALUE);
      info = getFileInfo(null, fileName);
      blockIndex = blocks.getLocatedBlocks().size();
    } catch (IOException e) {
      FSNamesystem.LOG.error(
        "F4: Error happened when calling getFileInfo/getBlockLocations");
      return super.chooseTarget(
        fileName, numOfReplicas, writer, chosenNodes, exclNodes, blocksize);
    }
    FSNamesystem.LOG.info(
      "F4: The file: " + fileName + " has a type: " + info.type);
    HashMap<String, HashSet<Node>> rackToHosts =
      new HashMap<String, HashSet<Node>>();
    try {

      // First handle the "localdir" case
      if (isLocal(fileName)) {
        return chooseLocalTarget(fileName,
                                 writer,
                                 blocks,
                                 exclNodes,
                                 chosenNodes,
                                 blocksize);
      }

      // For a data file, the locations of its own blocks as the reference
      int stripeIndex = -1;
      String srcFileName = null;
      String parityFileName = null;
      int parityLength = 0;
      int stripeLength = 0;
      switch (info.type) {
        case NOT_RAID:
        case SOURCE:
          srcFileName = fileName;
          parityFileName = null;
          stripeLength = this.stripeLen;
          stripeIndex = blockIndex / stripeLength;
          break;
        case TEMP_PARITY:
        case PARITY:
          srcFileName = getSourceFileFromParity(fileName, info);
          parityFileName = fileName;
          if (srcFileName == null ||
              this.namesystem.getHdfsFileInfo(srcFileName) == null) {
            srcFileName = null;
            FSNamesystem.LOG.error("F4: " + srcFileName + " does not exist");
          }
          Codec c = getCodec(fileName);
          parityLength = c.parityLength;
          stripeLength = c.stripeLength;
          stripeIndex = blockIndex / parityLength;
          break;
        default:
          return super.chooseTarget(
            numOfReplicas, writer, chosenNodes, exclNodes, blocksize);
      }

      rackToHosts = getRackToHostsMapForStripe(srcFileName,
                                               parityFileName,
                                               stripeLength,
                                               parityLength,
                                               stripeIndex);
    } catch (IOException e) {
      FSNamesystem.LOG.error("F4: Error happened when calling "
        + "getParityFile/getSourceFileFromParity");
      return super.chooseTarget(
          numOfReplicas, writer, chosenNodes, exclNodes, blocksize);
    } catch (NotEnoughReplicasException e) {
      FSNamesystem.LOG.error("F4: Error happend when calling "
        + "getCompanionSourceNodes/getSourceFile");
      return super.chooseTarget(
          numOfReplicas, writer, chosenNodes, exclNodes, blocksize);
    }
    return chooseTargetOnNewFailureDomain(fileName,
                                          writer,
                                          chosenNodes,
                                          exclNodes,
                                          rackToHosts,
                                          blocksize);
  }

  // Given a stripe index returns all racks in which the blocks of the stripe
  // reside and the hosts within those racks that host those blocks
  private HashMap<String, HashSet<Node>> getRackToHostsMapForStripe(
      String srcFileName,
      String parityFileName,
      int stripeLen,
      int parityLen,
      int stripeIndex) throws IOException {
    HashMap<String, HashSet<Node>> rackToHosts =
      new HashMap<String, HashSet<Node>>();
    if (srcFileName != null) {
      rackToHosts = getRackToHostsMapForStripe(srcFileName,
                                               stripeIndex,
                                               stripeLen);
    }
    if (parityFileName != null) {
      HashMap<String, HashSet<Node>> rackToHostsForParity =
        getRackToHostsMapForStripe(parityFileName,
                                   stripeIndex,
                                   parityLen);
      for (Map.Entry<String, HashSet<Node>> e :
           rackToHostsForParity.entrySet()) {
        HashSet<Node> nodes = rackToHosts.get(e.getKey());
        if (nodes == null) {
          nodes = new HashSet<Node>();
          rackToHosts.put(e.getKey(), nodes);
        }
        for (Node n : e.getValue()) {
          nodes.add(n);
        }
      }
    }
    for (Map.Entry<String, HashSet<Node>> e : rackToHosts.entrySet()) {
      if (e.getValue().size() > 1) {
        FSNamesystem.LOG.warn("F4: Rack " + e.getKey() +
          " being overused for stripe: " + stripeIndex);
      }
    }
    return rackToHosts;
  }

  private HashMap<String, HashSet<Node>> getRackToHostsMapForStripe(
      String src,
      int stripeIndex,
      int stripeLen) throws IOException {
    int sourceStart = stripeIndex * stripeLen;
    int sourceEnd = sourceStart + stripeLen;
    LocatedBlocks blocks = this.namesystem.getBlockLocations(src,
                                                             0,
                                                             Long.MAX_VALUE);
    List<LocatedBlock> sourceBlocks = blocks.getLocatedBlocks();
    sourceEnd = Math.min(sourceEnd, sourceBlocks.size()); 
    HashMap<String, HashSet<Node>> rackNodes =
      new HashMap<String, HashSet<Node>>();
    if (sourceStart < sourceBlocks.size()) {
      for (LocatedBlock b : sourceBlocks.subList(sourceStart, sourceEnd)) {
        for (Node n : b.getLocations()) {
          String rack = n.getNetworkLocation();
          FSNamesystem.LOG.info("F4: Block info for file: " + src
            + ", offset: " + b.getStartOffset() + ", rack: " + rack);
          HashSet<Node> nodes = rackNodes.get(rack);
          if (nodes == null) {
            nodes = new HashSet<Node>();
            rackNodes.put(rack, nodes);
          }
          nodes.add(n);
        }
      }
    }
    return rackNodes;
  }

  /**
   * This function uses the rackToHosts map (that contains the rack and the
   * corresponding nodes in those racks that contain the relevant blocks).
   *
   * The definition of "relevant blocks" is flexible. It can be used in a
   * variety of contexts. In the F4 placement policy, the relevant blocks
   * are all the peer blocks of the block to be placed. The peer blocks would
   * be all blocks in the raid stripe (data and parity included).
   *
   * It gets the racks that contain the least number of blocks for the stripe.
   * it gets the nodes within those racks and tries one-by-one all such
   * hosts as potential locations for the blocks. The check is based on
   * the host:
   * 1) The host passing the isGoodTarget check.
   * 2) If 1) fails and the "considerLoad" is true, then the same check is
   *    done with considerLoad = false.
   * 3) If 2) fails, then a node is chosen randomly while excluding any hosts
   *    that contain a block in the same stripe as the block to be placed.
   */
  private DatanodeDescriptor[] chooseTargetOnNewFailureDomain(
      String fileName,
      DatanodeDescriptor writer,
      List<DatanodeDescriptor> chosenNodes,
      List<Node> exclNodes,
      HashMap<String, HashSet<Node>> rackToHosts,
      long blockSize) {

    HashMap<Node, Node> excludedNodes = new HashMap<Node, Node>();
    for (String rack : this.clusterMap.getAllRacks()) {
      if (rackToHosts.get(rack) == null) {
        rackToHosts.put(rack, new HashSet<Node>());
      }
    }
    // Get the min occupancy in the racks.
    int minCount = Integer.MAX_VALUE;
    for (Map.Entry<String, HashSet<Node>> entry : rackToHosts.entrySet()) {
      if (entry.getValue().size() < minCount) {
        minCount = entry.getValue().size();
      }
      // DO NOT choose a host that has already been chosen for this stripe.
      for (Node n : entry.getValue()) {
        excludedNodes.put(n, n);
      }
    }

    if (exclNodes != null) {
      for (Node node:exclNodes) {
        excludedNodes.put(node, node);
      }
    }

    HashMap<String, HashSet<Node>> candidateNodesByRacks =
      new HashMap<String, HashSet<Node>>();
    for (Map.Entry<String, HashSet<Node>> entry : rackToHosts.entrySet()) {
      if (entry.getValue().size() == minCount) {
        for (Node n : this.clusterMap.getDatanodesInRack(entry.getKey())) {
          if (excludedNodes.get(n) == null) {
            HashSet<Node> candidateNodes =
              candidateNodesByRacks.get(entry.getKey());
            if (candidateNodes == null) {
              candidateNodes = new HashSet<Node>();
              candidateNodesByRacks.put(entry.getKey(), candidateNodes);
            }
            candidateNodes.add(n);
          }
        }
      }
    }

    List<DatanodeDescriptor> results = new ArrayList<DatanodeDescriptor>();
    if (getGoodNode(candidateNodesByRacks,
                    this.considerLoad,
                    blockSize,
                    results)) {
      return results.toArray(new DatanodeDescriptor[results.size()]);
    }
    if (this.considerLoad) {
      FSNamesystem.LOG.info("F4: Retrying without considering load for file: "
        + fileName);
      if (getGoodNode(candidateNodesByRacks, false, blockSize, results)) {
        return results.toArray(new DatanodeDescriptor[results.size()]);
      }
    }
    FSNamesystem.LOG.error("F4: No datanode in a non-overlapping rack for file:"
      + fileName);
    // Final effort to get something. But it will always try to get something
    // that is not a host that contains a peer block (block in the same stripe)
    // We assume that this step should succeed. In this step all nodes in the
    // cluster are available except for atmost 13 hosts for placement. So it is
    // highly unlikely that this step would fail.
    try {
      super.chooseRandom(
        1, NodeBase.ROOT, excludedNodes, blockSize, 1, results);
      return results.toArray(new DatanodeDescriptor[results.size()]);
    } catch (Exception e) {
      FSNamesystem.LOG.error("F4: Could not find a data node using "
        + "the normal F4 policy. Switching to default of parent");
      return super.chooseTarget(fileName, 1, writer,
                                chosenNodes, null, blockSize);
    }
  }

  private class RackComparator 
      implements Comparator<Map.Entry<String, HashSet<Node>>> {
    public RackComparator(long blockSize) {
      this.blockSize = blockSize;
    }

    public int compare(Map.Entry<String, HashSet<Node>> o1,
                       Map.Entry<String, HashSet<Node>> o2) {
      long ret = 0;
      for (Node node : o1.getValue()) {
        DatanodeDescriptor n = (DatanodeDescriptor)node;
        ret += (n.getRemaining() - (n.getBlocksScheduled() * this.blockSize));
      }
      for (Node node : o2.getValue()) {
        DatanodeDescriptor n = (DatanodeDescriptor)node;
        ret -= (n.getRemaining() - (n.getBlocksScheduled() * this.blockSize));
      }
      return ret == 0 ? 0 : (ret > 0) ? -1 : 1;
    }
    private long blockSize;
  }

  // Helper function to choose less occupied racks first.
  private boolean getGoodNode(
      HashMap<String, HashSet<Node>> candidateNodesByRacks,
      boolean considerLoad,
      long blockSize,
      List<DatanodeDescriptor> results) {
    List<Map.Entry<String, HashSet<Node>>> sorted =
      new ArrayList<Map.Entry<String, HashSet<Node>>>();
    for (Map.Entry<String, HashSet<Node>> entry :
           candidateNodesByRacks.entrySet()) {
      sorted.add(entry);
    }
    Collections.sort(sorted, new RackComparator(blockSize));
    int count = sorted.size() / 4;
    Collections.shuffle(sorted.subList(0, count));
    for (Map.Entry<String, HashSet<Node>> e : sorted) {
      if (getGoodNode(e.getValue(), considerLoad, blockSize, results)) {
        return true;
      }
    }
    return false;
  }
 
  // Helper function to find a good node. Returns true if found.
  private boolean getGoodNode(Set<Node> candidateNodes,
                              boolean considerLoad,
                              long blockSize,
                              List<DatanodeDescriptor> results) {
    List<DatanodeDescriptor> sorted = new ArrayList<DatanodeDescriptor>();
    for (Node n : candidateNodes) {
      sorted.add((DatanodeDescriptor)n);
    }
    final long blocksize = blockSize;
    Collections.sort(sorted, new Comparator<DatanodeDescriptor>() {
      public int compare(DatanodeDescriptor n1, DatanodeDescriptor n2) {
        long ret = (n2.getRemaining() - (n2.getBlocksScheduled() * blocksize)) -
                   (n1.getRemaining() - (n1.getBlocksScheduled() * blocksize));
        return ret == 0 ? 0 : (ret > 0) ? 1 : -1;
      }
    });
    // Also, add some randomness. We are doing so because it seems
    // that if there are many copies scheduled at the same time, namenode
    // does not have the uptodate information. So, we need to add some
    // randomness so that there is not a lot of copies targeted to
    // the same node, which will overload the hosts and may lead to
    // timeouts.
    int count = sorted.size() / 2;
    Collections.shuffle(sorted.subList(0, count));
    for (DatanodeDescriptor n : sorted) {
      if (this.isGoodTarget((DatanodeDescriptor)n,
                            blocksize,
                            1, // MaxTargerPerLoc (per rack)
                            considerLoad,
                            results)) {
        results.add((DatanodeDescriptor)n);
        return true;
      }
    }
    return false;
  }
}
