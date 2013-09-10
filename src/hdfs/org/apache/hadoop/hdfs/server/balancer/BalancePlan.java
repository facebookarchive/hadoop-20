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
package org.apache.hadoop.hdfs.server.balancer;

import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.server.balancer.Balancer.BalancerDatanode;
import org.apache.hadoop.hdfs.server.balancer.Balancer.NodeTask;
import org.apache.hadoop.hdfs.server.balancer.Balancer.Source;
import org.apache.hadoop.hdfs.server.balancer.Balancer.Target;
import org.apache.hadoop.net.NetworkTopology;
import org.apache.hadoop.net.Node;

/** Keeps the plan of pipelines to create between nodes and how much data must be sent */
class BalancePlan {
  protected static final Log LOG = LogFactory.getLog(BalancePlan.class.getName());

  /** Number of bytes to be moved in order to make the cluster balanced. */
  public long bytesLeftToMove;
  public long bytesToMove;

  public NetworkTopology cluster = new NetworkTopology();
  /** Maps datanode's storage ID to itself */
  public Map<String, BalancerDatanode> datanodes = new HashMap<String, BalancerDatanode>();

  /** All nodes that will participate in balancing as sources */
  public Collection<Source> sources = new HashSet<Source>();
  /** All nodes that will participate in balancing as targets */
  public Collection<Target> targets = new HashSet<Target>();

  /** If remaining < lowerRemainingThreshold then DataNode is considered overutilized */
  private double lowerRemainingThreshold;
  /** If remaining > upperRemainingThreshold then DataNode is considered underutilized */
  private double upperRemainingThreshold;
  /** Cluster-wide remaining capacity percentage */
  private double avgRemaining;

  /** Compute balance plan */
  public BalancePlan(Balancer balancer, List<DatanodeInfo> datanodes) {
    if (datanodes == null || datanodes.isEmpty()) {
      throw new IllegalArgumentException("cannot prepare plan for empty cluster");
    }

    avgRemaining = computeAvgRemaining(datanodes);
    lowerRemainingThreshold = Math.max(avgRemaining / 2, avgRemaining - balancer.threshold);
    upperRemainingThreshold = Math.min(PERCENTAGE_BASE, avgRemaining + balancer.threshold);
    if (lowerRemainingThreshold > upperRemainingThreshold) {
      throw new IllegalStateException("lowerThresh > upperThresh");
    }

    LOG.info("balanced range: [ " + lowerRemainingThreshold + ", " + upperRemainingThreshold
        + " ], average remaining: " + avgRemaining);

    long overLoadedBytes = 0L, underLoadedBytes = 0L;
    Bucket clusterBucket = new Bucket();
    Map<Node, Bucket> rackBuckets = new HashMap<Node, Bucket>();
    for (DatanodeInfo datanode : datanodes) {
      // Update network topology
      cluster.add(datanode);
      // Create bucket if none
      assert datanode.getParent() != null : "node outside of any rack";
      Bucket bucket = rackBuckets.get(datanode.getParent());
      if (bucket == null) {
        bucket = new Bucket();
        rackBuckets.put(datanode.getParent(), bucket);
      }
      // Put DataNode into chosen bucket
      BalancerDatanode datanodeS;
      if (getRemaining(datanode) < avgRemaining) {
        // Above average utilized
        datanodeS = balancer.getSource(datanode, avgRemaining);
        bucket.addSource((Source) datanodeS);
        clusterBucket.addSource((Source) datanodeS);
        if (isOverUtilized(datanodeS)) {
          overLoadedBytes += (long) ((lowerRemainingThreshold - datanodeS.getCurrentRemaining())
              * datanodeS.getDatanode().getCapacity() / PERCENTAGE_BASE);
        }
      } else {
        // Below average utilized
        datanodeS = new Target(datanode, avgRemaining);
        bucket.addTarget((Target) datanodeS);
        clusterBucket.addTarget((Target) datanodeS);
        if (isUnderUtilized(datanodeS)) {
          underLoadedBytes += (long) ((datanodeS.getCurrentRemaining() - upperRemainingThreshold)
              * datanodeS.getDatanode().getCapacity() / PERCENTAGE_BASE);
        }
      }
      // Update all DataNodes list
      this.datanodes.put(datanode.getStorageID(), datanodeS);
    }
    bytesLeftToMove = Math.max(overLoadedBytes, underLoadedBytes);

    logImbalancedNodes();

    // Balance each rack bucket separately
    for (Bucket bucket : rackBuckets.values()) {
      double rackAverage = bucket.computeAvgRemaining();
      if (lowerRemainingThreshold <= rackAverage && rackAverage <= upperRemainingThreshold) {
        bucket.updatePlan();
      }
      // If perfectly balanced rack renders only over or underutilized DataNodes
      // we do not bother balancing it
    }
    // Balance cluster-wide afterwards
    clusterBucket.externalUpdate();
    clusterBucket.updatePlan();

    bytesToMove = 0L;
    for (Source src : sources) {
      bytesToMove += src.scheduledSize;
    }

    logPlanOutcome();
  }

  /** Log the over utilized & under utilized nodes */
  private void logImbalancedNodes() {
    if (LOG.isInfoEnabled()) {
      int underUtilized = 0, overUtilized = 0;
      for (BalancerDatanode node : this.datanodes.values()) {
        if (isUnderUtilized(node))
          underUtilized++;
        else if (isOverUtilized(node))
          overUtilized++;
      }
      StringBuilder msg = new StringBuilder();
      msg.append(overUtilized);
      msg.append(" over utilized nodes:");
      for (BalancerDatanode node : this.datanodes.values()) {
        if (isOverUtilized(node)) {
          msg.append(" ");
          msg.append(node.getName());
        }
      }
      LOG.info(msg);
      msg = new StringBuilder();
      msg.append(underUtilized);
      msg.append(" under utilized nodes: ");
      for (BalancerDatanode node : this.datanodes.values()) {
        if (isUnderUtilized(node)) {
          msg.append(" ");
          msg.append(node.getName());
        }
      }
      LOG.info(msg);
    }
  }

  /** Log node utilization after the plan execution */
  private void logPlanOutcome() {
    if (LOG.isInfoEnabled()) {
      LOG.info("Predicted plan outcome: bytesLeftToMove: "
          + bytesLeftToMove + ", bytesToMove: " + bytesToMove);
      for (BalancerDatanode node : this.datanodes.values()) {
        LOG.info(node.getName() + " remaining: " + node.getCurrentRemaining());
      }
    }
  }

  /** Pairs up given nodes in balancing plan */
  private void scheduleTask(Source source, long size, Target target) {
    NodeTask nodeTask = new NodeTask(target, size);
    source.addNodeTask(nodeTask);
    target.addNodeTask(nodeTask);
    sources.add(source);
    targets.add(target);
    LOG.info("scheduled " + size + " bytes : " + source.getName() + " -> " + target.getName());
  }

  /** Determines if the node is overutilized */
  private boolean isOverUtilized(BalancerDatanode datanode) {
    return datanode.getCurrentRemaining() < lowerRemainingThreshold;
  }

  /** Determines if the node is underutilized */
  private boolean isUnderUtilized(BalancerDatanode datanode) {
    return datanode.getCurrentRemaining() > upperRemainingThreshold;
  }

  /** True iff the DataNode was over or underutilized before balancing */
  private boolean wasUrgent(BalancerDatanode datanode) {
    // Note that no node can become urgent during balancing if it was not before
    return datanode.initialRemaining < lowerRemainingThreshold || datanode.initialRemaining > upperRemainingThreshold;
  }

  /** Remaining ratio is expressed in percents */
  static final double PERCENTAGE_BASE = 100.0;

  static double computeAvgRemaining(Iterable<DatanodeInfo> datanodes) {
    long totalCapacity = 0L, totalRemainingSpace = 0L;
    for (DatanodeInfo datanode : datanodes) {
      totalCapacity += datanode.getCapacity();
      totalRemainingSpace += datanode.getRemaining();
    }
    return (double) totalRemainingSpace / totalCapacity * PERCENTAGE_BASE;
  }

  static double getRemaining(DatanodeInfo datanode) {
    return (double) datanode.getRemaining() / datanode.getCapacity() * PERCENTAGE_BASE;
  }

  /** Set of nodes which can interchange data for balancing */
  private class Bucket {
    private PriorityQueue<Source> sources = new PriorityQueue<Source>(10, new SourceComparator());
    private PriorityQueue<Target> targets = new PriorityQueue<Target>(10, new TargetComparator());

    public void addSource(Source node) {
      this.sources.add(node);
    }

    public void addTarget(Target node) {
      this.targets.add(node);
    }

    public double computeAvgRemaining() {
      long totalCapacity = 0L, totalRemainingSpace = 0L;
      for (BalancerDatanode node : sources) {
        totalCapacity += node.getDatanode().getCapacity();
        totalRemainingSpace += node.getDatanode().getRemaining();
      }
      for (BalancerDatanode node : targets) {
        totalCapacity += node.getDatanode().getCapacity();
        totalRemainingSpace += node.getDatanode().getRemaining();
      }
      return ((double) totalRemainingSpace) / totalCapacity * PERCENTAGE_BASE;
    }

    /** Updates the plan with all pairs of nodes from this bucket which need to be connected */
    public void updatePlan() {
      while (!this.sources.isEmpty() && !this.targets.isEmpty()) {
        Source source = this.sources.poll();
        Target target = this.targets.poll();
        if (!wasUrgent(source) && !wasUrgent(target)) {
          // Due to ordering of DataNodes we can skip the rest
          break;
        }
        long size = moveSize(source, target);
        if (size > 0) {
          scheduleTask(source, size, target);
        }
        if (source.getAvailableMoveSize() > 0) {
          this.sources.add(source);
        }
        if (target.getAvailableMoveSize() > 0) {
          this.targets.add(target);
        }
        // Loop termination:
        // In each step we either scheduleTask, therefore decreasing sum (over
        // all nodes) of availableMoveSize, or decrease number of nodes in
        // sources or targets queue, all of them are bounded by 0.
      }
    }

    /** Determines how much data to move between given nodes */
    private long moveSize(Source source, BalancerDatanode target) {
      // TODO balancing concurrency
      return Math.min(source.getAvailableMoveSize(), target.getAvailableMoveSize());
    }

    /** Sort internal queues again in case DataNodes was changed externally */
    public void externalUpdate() {
      // sources and targets might no longer be a proper priority queues
      this.sources = new PriorityQueue<Source>((Collection<Source>) this.sources);
      this.targets = new PriorityQueue<Target>((Collection<Target>) this.targets);
    }

    /**
     * We rely on this ordering in Bucket#updatePlan loop termination condition,
     * additional priorities should be expressed in proper (source/target) comparator below.
     * Because of this condition SourceComparator and TargetComparator are not reverse of each
     * other.
     */
    private abstract class BalancerDatanodeComparator implements Comparator<BalancerDatanode> {
      @Override
      public int compare(BalancerDatanode o1, BalancerDatanode o2) {
        return Boolean.valueOf(wasUrgent(o2)).compareTo(wasUrgent(o1));
      }
    }

    private final class SourceComparator extends BalancerDatanodeComparator {
      @Override
      public int compare(BalancerDatanode o1, BalancerDatanode o2) {
        int ret = super.compare(o1, o2);
        if (ret == 0) {
          ret = Double.valueOf(o1.getCurrentRemaining()).compareTo(o2.getCurrentRemaining());
        }
        // TODO concurrency level can also be taken into consideration
        return ret;
      }
    }

    private final class TargetComparator extends BalancerDatanodeComparator {
      @Override
      public int compare(BalancerDatanode o1, BalancerDatanode o2) {
        int ret = super.compare(o1, o2);
        if (ret == 0) {
          ret = Double.valueOf(o2.getCurrentRemaining()).compareTo(o1.getCurrentRemaining());
        }
        // TODO concurrency level can also be taken into consideration
        return ret;
      }
    }
  }

  /** Prints data distribution based on report from NameNode */
  public static void logDataDistribution(DatanodeInfo[] report) {
    if (LOG.isInfoEnabled()) {
      double avgRemaining = computeAvgRemaining(Arrays.asList(report));
      StringBuilder msg = new StringBuilder("Data distribution report: avgRemaining "
          + avgRemaining);
      for (DatanodeInfo node : report) {
        msg.append("\n").append(node.getName());
        msg.append(" remaining ").append(getRemaining(node));
        msg.append(" raw ").append(node.getRemaining()).append(" / ").append(node.getCapacity());
      }
      LOG.info(msg);
    }
  }
}
