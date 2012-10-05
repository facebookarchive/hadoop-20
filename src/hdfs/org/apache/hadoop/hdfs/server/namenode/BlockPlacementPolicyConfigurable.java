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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.net.DNSToSwitchMapping;
import org.apache.hadoop.net.NetworkTopology;
import org.apache.hadoop.net.Node;
import org.apache.hadoop.net.NodeBase;
import org.apache.hadoop.util.HostsFileReader;

public class BlockPlacementPolicyConfigurable extends
    BlockPlacementPolicyDefault {

  /**
   * RackRingInfo contains the rack information with respect to the ring it
   * belongs to and the internal machine ring it keeps.
   */
  protected class RackRingInfo {
    public int index;
    public List<String> rackNodes;
    public HashMap<String,Integer> rackNodesMap;

    public Integer findNode(DatanodeDescriptor node) {
      Integer retVal = rackNodesMap.get(node.getHostName());

      if (retVal == null) {
        retVal = rackNodesMap.get(node.getName());
        if (retVal == null) {
          retVal = rackNodesMap.get(node.getHost());
          if (retVal == null) {
            LOG.info("Didn't find " + node.getHostName() +
                " - " + node.getName() + " - " + node.getHost());
          }
        }
      }
      return retVal;
    }
  }

  public static final Log LOG =
    LogFactory.getLog(BlockPlacementPolicyConfigurable.class);

  // a fair rw lock for racks and racksMap.
  ReentrantReadWriteLock rwLock = new ReentrantReadWriteLock(true);
  protected List<String> racks; // Ring of racks
  protected HashMap<String,RackRingInfo> racksMap; //RackRingInfo map

  protected int rackWindow;
  protected int machineWindow;
  Random r = null;

  HostsFileReader hostsReader;
  DNSToSwitchMapping dnsToSwitchMapping;

  BlockPlacementPolicyConfigurable(Configuration conf, FSClusterStats stats,
      NetworkTopology clusterMap, HostsFileReader hostsReader,
      DNSToSwitchMapping dnsToSwitchMapping) {
    initialize(conf, stats, clusterMap, hostsReader, dnsToSwitchMapping, null);
  }

  BlockPlacementPolicyConfigurable() {
  }

  BlockPlacementPolicyConfigurable(long seed) {
    r = new Random(seed);
  }

  private class HashComparator implements Comparator<String> {
    Random rand = new Random();

    public int compare(String o1, String o2) {
      rand.setSeed(o1.hashCode());
      int hc1 = rand.nextInt();
      rand.setSeed(o2.hashCode());
      int hc2 = rand.nextInt();

      if (hc1 < hc2) return -1;
      if (hc1 > hc2) return 1;
      return 0;
    }
  }

  private void readLock() {
    rwLock.readLock().lock();
  }

  private void readUnlock() {
    rwLock.readLock().unlock();
  }

  private void writeLock() {
    rwLock.writeLock().lock();
  }

  private void writeUnlock() {
    rwLock.writeLock().unlock();
  }

  /**
   * The two classes below are used to hash rack and host names when forming
   * the rings. They can be overwritten to implement different strategies
   */
  protected Comparator<String> rackComparator = new HashComparator();
  protected Comparator<String> hostComparator = new HashComparator();

  /** {@inheritDoc} */
  public void initialize(Configuration conf, FSClusterStats stats,
      NetworkTopology clusterMap, HostsFileReader hostsReader,
      DNSToSwitchMapping dnsToSwitchMapping, FSNamesystem ns) {
    super.initialize(conf, stats, clusterMap, hostsReader, dnsToSwitchMapping, ns);
    this.rackWindow = conf.getInt("dfs.replication.rackwindow", 2);
    this.machineWindow = conf.getInt("dfs.replication.machineWindow", 5);
    this.racks = new ArrayList<String>();
    this.hostsReader = hostsReader;
    this.dnsToSwitchMapping = dnsToSwitchMapping;
    hostsUpdated(true);
    if (r == null) {
      r = new Random();
    }
    LOG.info("BlockPlacementPolicyConfigurable initialized");
  }

  /** {@inheritDoc} */
  public void hostsUpdated() {
    hostsUpdated(false);
  }

  public void hostsUpdated(boolean startup) {
    List<String> hostsIn = new ArrayList<String>(hostsReader.getHosts());
    List<String> hostsRacks = dnsToSwitchMapping.resolve(hostsIn);
    HashMap<String,RackRingInfo> tempRacksMap =
      new HashMap<String,RackRingInfo>();
    List<String> tempRacks = new ArrayList<String>();

    int index = hostsRacks.indexOf(NetworkTopology.DEFAULT_RACK);
    if (index != -1) {
      if (!startup) {
        throw new DefaultRackException("Could not resolve rack for : "
            + hostsIn.get(index) + " probably due to a DNS issue");
      } else {
        // We do not want to abort startup, just remove the bad datanode.
        for (int i = 0; i < hostsRacks.size(); i++) {
          if (hostsRacks.get(i).equals(NetworkTopology.DEFAULT_RACK)) {
            LOG.warn("Could not resolve rack for : "
                + hostsIn.get(i) + " probably due to a DNS issue, removing"
                + " the host since we are in startup");
            hostsRacks.remove(i);
            hostsReader.getHosts().remove(hostsIn.get(i));
            hostsIn.remove(i);
            i--;
          }
        }
      }
    }

    for (int i=0; i<hostsIn.size(); i++) {
      String host = hostsIn.get(i);
      String rack = hostsRacks.get(i);

      RackRingInfo rackinfo = tempRacksMap.get(rack);
      if (rackinfo == null) {
        LOG.info("Adding rack:" + rack);
        tempRacks.add(rack);
        rackinfo = new RackRingInfo();
        rackinfo.rackNodes = new ArrayList<String>();
        tempRacksMap.put(rack, rackinfo);
      }
      LOG.info("Adding host:" + host);
      rackinfo.rackNodes.add(host);
    }

    Collections.sort(tempRacks, rackComparator);

    StringBuffer ringRep = new StringBuffer("\nRing Topology:\n");
    for (int i = 0; i < tempRacks.size(); i++) {
      RackRingInfo rackinfo = tempRacksMap.get(tempRacks.get(i));
      rackinfo.index = i;
      List<String> rackNodes = rackinfo.rackNodes;
      HashMap<String,Integer> nodesMap = new HashMap<String,Integer>();
      rackinfo.rackNodesMap = nodesMap;

      ringRep.append("\tRing " + i + ": " + tempRacks.get(i) + "\n");

      Collections.sort(rackNodes, hostComparator);
      for (int j=0; j<rackNodes.size(); j++) {
        ringRep.append("\t\t" + j + ": " + rackNodes.get(j) + "\n");
        nodesMap.put(rackNodes.get(j), j);
      }
    }
    LOG.info(ringRep.toString());
    // Update both datastructures together in a lock.
    writeLock();
    racksMap = tempRacksMap;
    racks = tempRacks;
    writeUnlock();
  }

  /**
   * returns a random integer within a modular window taking into consideration
   * a sorted list of nodes to be excluded.
   */
  protected int randomIntInWindow(int begin, int windowSize, int n,
      Set<Integer> excludeSet) {

    final int size = Math.min(windowSize, n);

    if (size <= 0) {
      return -1;
    }

    int adjustment = 0;
    for (Integer v: excludeSet) {
      int vindex = (v.intValue() - begin + n) % n;
      if (vindex < size) {
        adjustment++; // calculates excluded elements within window
      }
    }

    if (adjustment >= size) {
      return -1;
    }

    int rindex = r.nextInt(size - adjustment); // ith element is chosen

    int iterator = begin;
    for (int i = 0; i <= rindex; i++) {
      while (excludeSet.contains(iterator)) {
        iterator = (iterator + 1) % n;
      }
      if (i != rindex) {
        iterator = (iterator + 1) % n;
      }
    }

    return iterator;
  }

  /**
   * This method is currently used only for re-replication and should be used
   * only for this. If this method is used for normal block placements that
   * would completely break this placement policy.
   */
  @Override
  public DatanodeDescriptor[] chooseTarget(FSInodeInfo srcInode,
      int numOfReplicas,
      DatanodeDescriptor writer, List<DatanodeDescriptor> chosenNodes,
      List<Node> excludesNodes, long blocksize) {
    if (numOfReplicas == 0 || clusterMap.getNumOfLeaves() == 0) {
      return new DatanodeDescriptor[0];
    }

    int[] result = getActualReplicas(numOfReplicas, chosenNodes);
    numOfReplicas = result[0];
    int maxNodesPerRack = result[1];

    HashMap<Node, Node> excludedNodes = new HashMap<Node, Node>();
    List<DatanodeDescriptor> results = new ArrayList<DatanodeDescriptor>(
        chosenNodes.size() + numOfReplicas);

    updateExcludedAndChosen(null, excludedNodes, results, chosenNodes);

    if (!clusterMap.contains(writer)) {
      writer = null;
    }

    DatanodeDescriptor localNode = super.chooseTarget(numOfReplicas, writer,
        excludedNodes, blocksize, maxNodesPerRack, results,
        chosenNodes.isEmpty());

    return this.finalizeTargets(results, chosenNodes, writer, localNode);
  }

  /* choose <i>numOfReplicas</i> from all data nodes */
  @Override
  protected DatanodeDescriptor chooseTarget(int numOfReplicas,
      DatanodeDescriptor writer, HashMap<Node, Node> excludedNodes,
      long blocksize, int maxNodesPerRack, List<DatanodeDescriptor> results,
      boolean newBlock) {

    if (numOfReplicas == 0 || clusterMap.getNumOfLeaves() == 0) {
      return writer;
    }

    int numOfResults = results.size();
    if (writer == null && !newBlock) {
      writer = results.get(0);
    }

    try {
      if (numOfResults == 0) {
        writer = chooseLocalNode(writer, excludedNodes, blocksize,
            maxNodesPerRack, results);
        if (--numOfReplicas == 0) {
          return writer;
        }
      }
      if (numOfResults <= 1) {
        // If we have a replication factor of 2, place both replicas on the
        // same rack.
        if (numOfReplicas == 1) {
          chooseLocalRack(results.get(0), excludedNodes, blocksize,
              maxNodesPerRack, results);
        } else {
          chooseFirstInRemoteRack(results.get(0), excludedNodes, blocksize,
              maxNodesPerRack, results);
        }
        if (--numOfReplicas == 0) {
          return writer;
        }
      }
      chooseRemainingReplicas(numOfReplicas, excludedNodes, blocksize,
            maxNodesPerRack, results);
    } catch (NotEnoughReplicasException e) {
      LOG.warn("Not able to place enough replicas, still in need of "
              + numOfReplicas);
    }
    return writer;
  }

  /**
   * Picks up the first replica stored in a remote rack.
   * @param localMachine local machine that is writing the data
   * @param excludedNodes nodes that should not be considered
   * @param blocksize size of blocks
   * @param maxReplicasPerRack maximum replicas per rack
   * @param results datanodes used for replicas
   * @throws NotEnoughReplicasException
   */
  protected void chooseFirstInRemoteRack(DatanodeDescriptor localMachine,
      HashMap<Node, Node> excludedNodes, long blocksize,
      int maxReplicasPerRack, List<DatanodeDescriptor> results)
      throws NotEnoughReplicasException {
    readLock();
    try {
      RackRingInfo rackInfo = racksMap.get(localMachine.getNetworkLocation());
      assert (rackInfo != null);

      Integer machineId = rackInfo.findNode(localMachine);
      assert (machineId != null);

      if (!chooseRemoteRack(rackInfo.index, rackInfo.index, rackWindow + 1,
          machineId, machineWindow, excludedNodes, blocksize,
          maxReplicasPerRack, results, false)) {
        LOG.info("Couldn't find a Datanode within node group. "
            + "Resorting to default policy.");
        super.chooseRemoteRack(1, localMachine, excludedNodes, blocksize,
            maxReplicasPerRack, results);
      }
    } finally {
      readUnlock();
    }
  }

  /**
   * returns the best possible match of nodes to the first three replicas in
   * in the current replication scheme
   * result[0] = first replica or null
   * result[1] = second replica or null
   * result[2] = third replica or null
   */
  protected DatanodeDescriptor[] findBest(List<DatanodeDescriptor> listOfNodes) {

    DatanodeDescriptor[] result = new DatanodeDescriptor[3];

    result[0] = listOfNodes.isEmpty() ? null : listOfNodes.get(0);
    result[1] = null;
    result[2] = null;

    for (DatanodeDescriptor n : listOfNodes) {
      findBestWithFirst(n, listOfNodes, result);
      if (result[2] != null)
        return result;
    }

    if (result[1] == null && listOfNodes.size() > 1) {
      findBestWithoutFirst(listOfNodes, result);
    }

    return result;
  }

  /**
   * Function that finds the best partial triple including a first replica
   * @param first first replica to be used
   * @param listOfNodes nodes to choose from
   * @param result array that stores results
   */
  private void findBestWithFirst(DatanodeDescriptor first,
      List<DatanodeDescriptor> listOfNodes, DatanodeDescriptor[] result) {
    for (int in2 = 0; in2 < listOfNodes.size(); in2++) {
      DatanodeDescriptor n2 = listOfNodes.get(in2);
      if (!first.equals(n2)) {
        if (result[1] == null && inWindow(first, n2)) {
          result[0] = first;
          result[1] = n2;
        }

        for (int in3 = in2 + 1; in3 < listOfNodes.size(); in3++) {
          DatanodeDescriptor n3 = listOfNodes.get(in3);
          if (!first.equals(n3)
              && inWindow(first, n3, n2)) {
            result[0] = first;
            result[1] = n2;
            result[2] = n3;
            return;
          }
        }
      }
    }
  }

  /**
   * Verifies if testing node is within right windows of first node
   * @param first first node being considered
   * @param testing node we are testing to check if it is within window or not
   * @return We return true if it is successful, and not otherwise
   */
  private boolean inWindow(DatanodeDescriptor first, DatanodeDescriptor testing) {

    readLock();
    try {
      RackRingInfo rackInfo = racksMap.get(first.getNetworkLocation());
      assert (rackInfo != null);

      Integer machineId = rackInfo.findNode(first);
      assert (machineId != null);

      final int rackWindowStart = rackInfo.index;

      final RackRingInfo rackTest = racksMap.get(testing.getNetworkLocation());
      assert (rackTest != null);

      final int rackDist = (rackTest.index - rackWindowStart + racks.size())
          % racks.size();

      if (rackDist < rackWindow + 1 && rackTest.index != rackInfo.index) {
        // inside rack window
        final Integer idFirst = rackInfo.findNode(first);
        assert (idFirst != null);

        final int sizeFirstRack = rackInfo.rackNodes.size();
        final int sizeTestRack = rackTest.rackNodes.size();

        final int start = idFirst * sizeTestRack / sizeFirstRack;
        final Integer idTest = rackTest.findNode(testing);
        assert (idTest != null);

        final int dist = (idTest - start + sizeTestRack) % sizeTestRack;

        if (dist < machineWindow) { // inside machine Window
          return true;
        }
      }

      return false;
    } finally {
      readUnlock();
    }
  }

  /**
   * Verifies if testing nodes are within right windows of first node
   * @param first first node being considered
   * @param testing1 node we are testing to check if it is within window or not
   * @param testing2 node we are testing to check if it is within window or not
   * @return We return true if it is successful, and not otherwise
   */
  private boolean inWindow(DatanodeDescriptor first,
      DatanodeDescriptor testing1, DatanodeDescriptor testing2) {

    readLock();
    try {
      if (!testing1.getNetworkLocation().equals(testing2.getNetworkLocation())) {
        return false;
      }

      RackRingInfo rackInfo = racksMap.get(first.getNetworkLocation());
      assert (rackInfo != null);

      Integer machineId = rackInfo.findNode(first);
      assert (machineId != null);

      final int rackWindowStart = rackInfo.index;

      final RackRingInfo rackTest = racksMap.get(testing1.getNetworkLocation());
      assert (rackTest != null);

      final int rackDist = (rackTest.index - rackWindowStart + racks.size())
          % racks.size();

      if (rackDist < rackWindow + 1 && rackTest.index != rackInfo.index) {
        // inside rack window

        final int rackSize = rackTest.rackNodes.size();
        Integer idN2 = rackTest.findNode(testing1);
        assert (idN2 != null);
        Integer idN3 = rackTest.findNode(testing2);
        assert (idN3 != null);

        final Integer idFirst = rackInfo.findNode(first);
        assert (idFirst != null);

        final int sizeFirstRack = rackInfo.rackNodes.size();

        final int end = idFirst * rackSize / sizeFirstRack;

        // proportional to previous of idFirst
        final int prevIdFirst = (idFirst + sizeFirstRack - 1) % sizeFirstRack;
        int start = (prevIdFirst * rackSize / sizeFirstRack);

        int distPropWindow = (end - start + rackSize) % rackSize;

        if (distPropWindow > 0) {
          start = (start + 1) % rackSize;
          distPropWindow--;
        }

        int distIdN2 = (idN2 - start + rackSize) % rackSize;
        int distIdN3 = (idN3 - start + rackSize) % rackSize;

        int distN3N2 = (idN3 - idN2 + rackSize) % rackSize;
        int distN2N3 = (idN2 - idN3 + rackSize) % rackSize;

        if (distIdN2 <= distPropWindow && distN3N2 < machineWindow)
          return true;

        if (distIdN3 <= distPropWindow && distN2N3 < machineWindow)
          return true;

      }

      return false;
    } finally {
      readUnlock();
    }
  }
  /**
   * Finds best match considering only the remote nodes.
   * @param listOfNodes Datanodes to choose from
   * @param result Array containing results
   */
  private void findBestWithoutFirst(List<DatanodeDescriptor> listOfNodes,
      DatanodeDescriptor[] result) {

    readLock();
    try {
      for (int in2 = 0; in2 < listOfNodes.size(); in2++) {
        DatanodeDescriptor n2 = listOfNodes.get(in2);

        for (int in3 = in2 + 1; in3 < listOfNodes.size(); in3++) {
          DatanodeDescriptor n3 = listOfNodes.get(in3);
          if (n2.getNetworkLocation().equals(n3.getNetworkLocation())) {
            RackRingInfo rackInfo = racksMap.get(n2.getNetworkLocation());
            assert (rackInfo != null);
            final int rackSize = rackInfo.rackNodes.size();
            final Integer idN2 = rackInfo.findNode(n2);
            final Integer idN3 = rackInfo.findNode(n3);

            if (idN2 != null && idN3 != null) {
              int dist = (idN3 - idN2 + rackSize) % rackSize;
              if (dist >= machineWindow) {
                dist = rackSize - dist; // try n2 - n3
              }

              if (dist < machineWindow) {
                result[0] = null;
                result[1] = n2;
                result[2] = n3;
                return;
              }
            }
          }
        }
      }
    } finally {
      readUnlock();
    }
  }

  /**
   * Chooses the third replica, after 2 have been allocated
   * @param excludedNodes Nodes that we should not consider
   * @param blocksize size of blocks
   * @param maxReplicasPerRack maximum number of replicas per rack
   * @param results array containing results
   * @throws NotEnoughReplicasException
   */
  protected void chooseRemainingReplicas (int numOfReplicas,
      HashMap<Node, Node> excludedNodes, long blocksize, int maxReplicasPerRack,
      List<DatanodeDescriptor> results) throws NotEnoughReplicasException {
    readLock();
    try {

      if (numOfReplicas <= 0) {
        return;
      }

      DatanodeDescriptor[] bestmatch = findBest(results);

      if (bestmatch[0] != null) { // there is a first replica: 1,X,X

        excludedNodes.put(bestmatch[0], bestmatch[0]);

        if (bestmatch[1] == null) { // there is no second replica: 1,0,0

          chooseFirstInRemoteRack(bestmatch[0], excludedNodes, blocksize,
              maxReplicasPerRack, results); // pick up second
          numOfReplicas--;

          // now search for the rest recursively
          chooseRemainingReplicas(numOfReplicas, excludedNodes, blocksize,
              maxReplicasPerRack, results);
          return;

        } else if (bestmatch[2] == null) { // no third replica: 1,1,0

          // find the third one
          excludedNodes.put(bestmatch[1], bestmatch[1]);
          RackRingInfo rack0 = racksMap.get(bestmatch[0].getNetworkLocation());
          RackRingInfo rack1 = racksMap.get(bestmatch[1].getNetworkLocation());
          int posR0 = rack0.findNode(bestmatch[0]);
          int firstMachine = posR0 * rack1.rackNodes.size()
              / rack0.rackNodes.size();
          if (!chooseMachine(bestmatch[1].getNetworkLocation(),
              firstMachine, machineWindow,
              excludedNodes, blocksize, maxReplicasPerRack, results)) {
            // if doen't get it in the rack, try at a different one
            LOG.info("Couldn't find 3rd Datanode on the same rack as 2nd. "
                + "Resorting to a different rack in the same node group.");
            chooseFirstInRemoteRack(bestmatch[0], excludedNodes, blocksize,
                maxReplicasPerRack, results);
          }
          numOfReplicas--;

        }

      } else if (bestmatch[1] != null && bestmatch[2] != null) { // 0,1,1

        RackRingInfo rackInfo = racksMap.get(bestmatch[1].getNetworkLocation());
        Integer posN1 = rackInfo.findNode(bestmatch[1]);
        Integer posN2 = rackInfo.findNode(bestmatch[2]);

        if (posN1 != null && posN2 != null) {
          int rackSize = rackInfo.rackNodes.size();
          int diff = (posN2 - posN1 + rackSize) % rackSize;
          if (diff >= machineWindow) {
            Integer tmp = posN1;
            posN1 = posN2;
            posN2 = tmp;
            diff = rackSize - diff;
          }

          int newMachineWindow = machineWindow - diff;

          assert (newMachineWindow > 0);

          if (rackSize - diff < machineWindow) {
            newMachineWindow = rackSize;
          }
          final int firstRack = (rackInfo.index - rackWindow + racks.size())
              % racks.size();
          int machineIdx = (posN1 - newMachineWindow + 1 + rackSize) % rackSize;

          if (chooseRemoteRack(rackInfo.index, firstRack, rackWindow,
              machineIdx, newMachineWindow, excludedNodes, blocksize,
              maxReplicasPerRack, results, true)) {
            numOfReplicas--;
          }
        }
      }

      // get the rest randomly
      if (numOfReplicas > 0) {
        int replicas = results.size();
        if (replicas < 3) {
          LOG.info("Picking up random replicas from default policy after "
              + results.size() + " replicas have been chosen");
        }
        super.chooseRandom(numOfReplicas, NodeBase.ROOT, excludedNodes,
            blocksize, maxReplicasPerRack, results);
      }
    } finally {
      readUnlock();
    }
  }

  /**
   * Picks up a remote machine within defined window
   * @param rackIdx rack the request is coming from and that should be avoided
   * @param firstRack rack that starts window
   * @param rackWindow rack window size
   * @param machineIdx index of first replica within its rack
   * @param windowSize size of the machine window
   * @param excludedNodes list of black listed nodes.
   * @param blocksize size of a block
   * @param maxReplicasPerRack maximum number of replicas per rack
   * @param results List of results
   * @param reverse adjustment when looking forward or backward.
   * @return
   * @throws NotEnoughReplicasException
   */
  protected boolean chooseRemoteRack(int rackIdx, int firstRack, int rackWindow,
      int machineIdx, int windowSize, HashMap<Node, Node> excludedNodes,
      long blocksize, int maxReplicasPerRack, List<DatanodeDescriptor> results,
      boolean reverse) throws NotEnoughReplicasException {
    // randomly choose one node from remote racks

    readLock();
    try {
      HashSet<Integer> excludedRacks = new HashSet<Integer>();
      excludedRacks.add(rackIdx);
      int n = racks.size();
      int currRackSize = racksMap.get(racks.get(rackIdx)).rackNodes.size();
      while (excludedRacks.size() < rackWindow) {

        int newRack = randomIntInWindow(firstRack, rackWindow, n, excludedRacks);
        if (newRack < 0)
          break;

        excludedRacks.add(newRack);

        int newRackSize = racksMap.get(racks.get(newRack)).rackNodes.size();
        int firstMachine = machineIdx * newRackSize / currRackSize;

        int newWindowSize = windowSize;
        if (reverse) {
          firstMachine = ((int) Math.ceil((double) machineIdx * newRackSize
              / currRackSize))
              % newRackSize;

          newWindowSize = Math.max(1, windowSize * newRackSize / currRackSize);
        }

        if (newWindowSize <= 0) {
          continue;
        }

        if (chooseMachine(racks.get(newRack), firstMachine, newWindowSize,
            excludedNodes, blocksize, maxReplicasPerRack, results)) {
          return true;
        }
      }
      return false;
    } finally {
      readUnlock();
    }
  }

  /**
   * Chosses a machine within a window inside a rack
   * @param rack rack to choose from
   * @param firstMachine machine that starts window
   * @param windowSize size of machine window
   * @param excludedNodes nodes to avoid
   * @param blocksize size of a block
   * @param maxReplicasPerRack maximum number of replicas within the same rack
   * @param results list of results
   * @return
   */
  protected boolean chooseMachine(String rack, int firstMachine, int windowSize,
      HashMap<Node, Node> excludedNodes, long blocksize,
      int maxReplicasPerRack, List<DatanodeDescriptor> results) {
    readLock();
    try {

      HashSet<Integer> excludedMachines = new HashSet<Integer>();
      RackRingInfo rackInfo = racksMap.get(rack);
      assert (rackInfo != null);

      int n = rackInfo.rackNodesMap.size();

      List<Node> rackDatanodes = clusterMap.getDatanodesInRack(rack);
      if (rackDatanodes == null) {
        return false;
      }

      while (excludedMachines.size() < windowSize) {
        int newMachine = randomIntInWindow(firstMachine, windowSize, n,
            excludedMachines);

        if (newMachine < 0)
          return false;

        excludedMachines.add(newMachine);

        DatanodeDescriptor chosenNode = null;
        for (Node node : rackDatanodes) {
          DatanodeDescriptor datanode = (DatanodeDescriptor) node;
          Integer idx = rackInfo.findNode(datanode);
          if (idx != null && idx.intValue() == newMachine) {
            chosenNode = datanode;
            break;
          }
        }

        if (chosenNode == null)
          continue;

        Node oldNode = excludedNodes.put(chosenNode, chosenNode);
        if (oldNode == null) { // choosendNode was not in the excluded list
          if (isGoodTarget(chosenNode, blocksize, maxReplicasPerRack, results)) {
            results.add(chosenNode);
            return true;
          }
        }
      }
      return false;
    } finally {
      readUnlock();
    }
  }

  /** {@inheritDoc} */
  public DatanodeDescriptor chooseReplicaToDelete(FSInodeInfo inode,
      Block block, short replicationFactor,
      Collection<DatanodeDescriptor> first,
      Collection<DatanodeDescriptor> second) {

    List<DatanodeDescriptor> nodes = new ArrayList<DatanodeDescriptor>();

    if (first != null) {
      nodes.addAll(first);
    }
    if (second != null) {
      nodes.addAll(second);
    }

    DatanodeDescriptor[] best = findBest(nodes);

    boolean saved_two_racks = false;

    if (best[0] != null && best[1] != null) {
      saved_two_racks = true;
    }

    for (DatanodeDescriptor n : nodes) {
      if (saved_two_racks &&
          !n.equals(best[0]) && !n.equals(best[1]) && !n.equals(best[2])) {
        return n;
      }

      if (!saved_two_racks &&
          (
              (best[0] != null && // different from best[0]'s rack
                  !best[0].getNetworkLocation().equals(n.getNetworkLocation()))

                  ||

              (best[1] != null && // different from best[1]'s rack
                  !best[1].getNetworkLocation().equals(n.getNetworkLocation()))

          ) ) {
        saved_two_racks = true; // just skipped (saved) one machine
                                // in a different rack
      }
    }

    return super.chooseReplicaToDelete(inode, block, replicationFactor,
                                                                first, second);
  }

}
