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

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.EOFException;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.text.DateFormat;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.protocol.AlreadyBeingCreatedException;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.protocol.DataTransferProtocol;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.FSConstants;
import org.apache.hadoop.hdfs.protocol.LocatedBlocksWithMetaInfo;
import org.apache.hadoop.hdfs.protocol.FSConstants.DatanodeReportType;
import org.apache.hadoop.hdfs.protocol.ReplaceBlockHeader;
import org.apache.hadoop.hdfs.server.balancer.metrics.BalancerMetrics;
import org.apache.hadoop.hdfs.server.common.HdfsConstants;
import org.apache.hadoop.hdfs.server.common.Util;
import org.apache.hadoop.hdfs.server.namenode.BlockPlacementPolicy;
import org.apache.hadoop.hdfs.server.namenode.BlockPlacementPolicyDefault;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.server.protocol.NamenodeProtocol;
import org.apache.hadoop.hdfs.server.protocol.BlocksWithLocations.BlockWithLocations;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.retry.RetryPolicies;
import org.apache.hadoop.io.retry.RetryPolicy;
import org.apache.hadoop.io.retry.RetryProxy;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.UnixUserGroupInformation;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/** <p>The balancer is a tool that balances disk space usage on an HDFS cluster
 * when some datanodes become full or when new empty nodes join the cluster.
 * The tool is deployed as an application program that can be run by the
 * cluster administrator on a live HDFS cluster while applications
 * adding and deleting files.
 *
 * <p>SYNOPSIS
 * <pre>
 * To start:
 *      bin/start-balancer.sh [-threshold <threshold>]
 *      Example: bin/ start-balancer.sh
 *                     start the balancer with a default threshold of 10%
 *               bin/ start-balancer.sh -threshold 5
 *                     start the balancer with a threshold of 5%
 * To stop:
 *      bin/ stop-balancer.sh
 * </pre>
 *
 * <p>DESCRIPTION
 * <p>The threshold parameter is a fraction in the range of (0%, 100%)
 * with a default value of 10%. The threshold sets a target for whether the cluster
 * is balanced. Let the utilization of the node be equal to the ratio of used space
 * at the node to total capacity of the node, analogically let the utilization
 * of the cluster be equal to the ratio of used space in the cluster to total
 * capacity of the cluster. Let the lower threshold be the maximum of (cluster utilization -
 * threshold) and (cluster utilization / 2), let the upper threshold be the minimum of
 * (cluster utilization + threshold) and 100% (only for consistency).
 * A cluster is balanced iff for each datanode, lower threshold <= node's utilization <= upper
 * threshold. The smaller the threshold, the more balanced a cluster will become.
 * It takes more time to run the balancer for small threshold values.
 * Also for a very small threshold the cluster may not be able to reach the
 * balanced state when applications write and delete files concurrently.
 *
 * <p>The tool moves blocks from highly utilized datanodes to poorly utilized
 * datanodes iteratively. At the end of each iteration, the balancer obtains updated
 * datanodes information from the namenode.
 *
 * <p>A system property that limits the balancer's use of bandwidth is
 * defined in the default configuration file:
 * <pre>
 * <property>
 *   <name>dfs.balance.bandwidthPerSec</name>
 *   <value>1048576</value>
 * <description>  Specifies the maximum bandwidth that each datanode
 * can utilize for the balancing purpose in term of the number of bytes
 * per second. </description>
 * </property>
 * </pre>
 *
 * <p>This property determines the maximum speed at which a block will be
 * moved from one datanode to another. The default value is 1MB/s. The higher
 * the bandwidth, the faster a cluster can reach the balanced state,
 * but with greater competition with application processes. If an
 * administrator changes the value of this property in the configuration
 * file, the change is observed when HDFS is next restarted.
 *
 * <p>MONITERING BALANCER PROGRESS
 * <p>After the balancer is started, an output file name where the balancer
 * progress will be recorded is printed on the screen.  The administrator
 * can monitor the running of the balancer by reading the output file.
 * The output shows the balancer's status iteration by iteration. In each
 * iteration it prints the starting time, the iteration number, the total
 * number of bytes that have been moved in the previous iterations,
 * the total number of bytes that are left to move in order for the cluster
 * to be balanced, and the number of bytes that are being moved in this
 * iteration. Normally "Bytes Already Moved" is increasing while "Bytes Left
 * To Move" is decreasing.
 *
 * <p>Running multiple instances of the balancer in an HDFS cluster is
 * prohibited by the tool.
 *
 * <p>The balancer automatically exits when any of the following five
 * conditions is satisfied:
 * <ol>
 * <li>The cluster is balanced;
 * <li>No block can be moved;
 * <li>No block has been moved for five consecutive iterations;
 * <li>An IOException occurs while communicating with the namenode;
 * <li>Another balancer is running.
 * </ol>
 *
 * <p>Upon exit, a balancer returns an exit code and prints one of the
 * following messages to the output file in corresponding to the above exit
 * reasons:
 * <ol>
 * <li>The cluster is balanced after X iterations. Exiting
 * <li>No block can be moved. Exiting...
 * <li>No block has been moved for 3 iterations. Exiting...
 * <li>Received an IO exception: failure reason. Exiting...
 * <li>Another balancer is running. Exiting...
 * </ol>
 *
 * <p>The administrator can interrupt the execution of the balancer at any
 * time by running the command "stop-balancer.sh" on the machine where the
 * balancer is running.
 */

public class Balancer implements Tool {
  protected static final Log LOG =
      LogFactory.getLog(Balancer.class.getName());

  /** How big chunk of blocks can be fetched at once */
  private long fetchBlocksSize;
  /** Only blocks with no less replicas will be moved */
  private int minBlockReplicas;
  /** Max number of block fetches for single source */
  private int maxFetchCount;

  /** The maximum number of concurrent blocks moves for
   * balancing purpose at a datanode
   */
  public final static int MAX_NUM_CONCURRENT_MOVES = 5;
  public static int maxConcurrentMoves = MAX_NUM_CONCURRENT_MOVES;
  private static long maxIterationTime = 20*60*1000L; //20 mins
  protected Configuration conf;
  double threshold = 10D;

  private InetSocketAddress namenodeAddress;
  protected NamenodeProtocol namenode;
  protected ClientProtocol client;
  protected FileSystem fs;
  private OutputStream out = null;

  private int namespaceId;

  private BalancePlan plan;
  private BalancerMetrics myMetrics;

  private Map<Block, BalancerBlock> globalBlockList
                 = new HashMap<Block, BalancerBlock>();
  private MovedBlocks movedBlocks = new MovedBlocks();

  final static private int MOVER_THREAD_POOL_SIZE = 1000;
  private static int moveThreads = MOVER_THREAD_POOL_SIZE;
  private ExecutorService moverExecutor = null;
  final static private int DISPATCHER_THREAD_POOL_SIZE = 200;
  private ExecutorService dispatcherExecutor = null;

  protected boolean shuttingDown = false;

  /* This class keeps track of a scheduled block move */
  private class PendingBlockMove {
    private BalancerBlock block;
    private Source source;
    private BalancerDatanode proxySource;
    private BalancerDatanode target;
    private Socket sock;

    /** constructor */
    private PendingBlockMove() {
    }

    /* choose a block & a proxy source for this pendingMove
     * whose source & target have already been chosen.
     *
     * Return true if a block and its proxy are chosen; false otherwise
     */
    private boolean chooseBlockAndProxy() {
      // iterate all source's blocks until find a good one
      for (Iterator<BalancerBlock> blocks=
        source.getBlockIterator(); blocks.hasNext();) {
        if (markMovedIfGoodBlock(blocks.next())) {
          blocks.remove();
          return true;
        }
      }
      return false;
    }

    /* Return true if the given block is good for the tentative move;
     * If it is good, add it to the moved list to marked as "Moved".
     * A block is good if
     * 1. it is a good candidate; see isGoodBlockCandidate
     * 2. can find a proxy source that's not busy for this move
     */
    private boolean markMovedIfGoodBlock(BalancerBlock block) {
      synchronized(block) {
        synchronized(movedBlocks) {
          if (isGoodBlockCandidate(source, target, block)) {
            this.block = block;
            if ( chooseProxySource() ) {
              movedBlocks.add(block);
              if (LOG.isDebugEnabled()) {
                LOG.debug("Decided to move block "+ block.getBlockId()
                    +" with a length of "+StringUtils.byteDesc(block.getNumBytes())
                    + " bytes from " + source.getName()
                    + " to " + target.getName()
                    + " using proxy source " + proxySource.getName() );
              }
              return true;
            }
          }
        }
      }
      return false;
    }

    /* Now we find out source, target, and block, we need to find a proxy
     *
     * @return true if a proxy is found; otherwise false
     */
    private boolean chooseProxySource() {
      // check if there is replica which is on the same rack with the target
      for (BalancerDatanode loc : block.getLocations()) {
        if (plan.cluster.isOnSameRack(loc.getDatanode(), target.getDatanode())) {
          if (loc.addPendingBlock(this)) {
            proxySource = loc;
            return true;
          }
        }
      }
      // find out a non-busy replica
      for (BalancerDatanode loc : block.getLocations()) {
        if (loc.addPendingBlock(this)) {
          proxySource = loc;
          return true;
        }
      }
      return false;
    }

    /* Dispatch the block move task to the proxy source & wait for the response
     */
    private void dispatch() {
      sock = new Socket();
      DataOutputStream out = null;
      DataInputStream in = null;
      try {
        sock.connect(NetUtils.createSocketAddr(
            target.datanode.getName()), HdfsConstants.READ_TIMEOUT);
        sock.setKeepAlive(true);
        out = new DataOutputStream( new BufferedOutputStream(
            sock.getOutputStream(), FSConstants.BUFFER_SIZE));
        sendRequest(out);
        in = new DataInputStream( new BufferedInputStream(
            sock.getInputStream(), FSConstants.BUFFER_SIZE));
        receiveResponse(in);
        bytesMoved.inc(block.getNumBytes());
        myMetrics.bytesMoved.inc(block.getNumBytes());
        LOG.info("Moving block " + block.getBlock().getBlockId() + " of size "
            + block.getNumBytes() +
              " from "+ source.getName() + " to " +
              target.getName() + " through " +
              proxySource.getName() +
              " is succeeded." );
      } catch (IOException e) {
        LOG.warn("Error moving block "+block.getBlockId()+
            " from " + source.getName() + " to " +
            target.getName() + " through " +
            proxySource.getName() +
            ": "+e.getMessage());
        if (e instanceof EOFException) {
          LOG.warn("Moving block " + block.getBlockId() +
            " was cancelled because the time exceeded the limit");
        }
      } finally {
        IOUtils.closeStream(out);
        IOUtils.closeStream(in);
        IOUtils.closeSocket(sock);

        proxySource.removePendingBlock(this);
        synchronized(target) {
          target.removePendingBlock(this);
        }

        synchronized (this ) {
          reset();
        }
        synchronized (Balancer.this) {
          Balancer.this.notifyAll();
        }
      }
    }

    /* Send a block replace request to the output stream*/
    private void sendRequest(DataOutputStream out) throws IOException {
      /* Write the header */
      ReplaceBlockHeader replaceBlockHeader = new ReplaceBlockHeader(
          DataTransferProtocol.DATA_TRANSFER_VERSION, namespaceId,
          block.getBlock().getBlockId(), block.getBlock().getGenerationStamp(),
          source.getStorageID(), proxySource.getDatanode());
      replaceBlockHeader.writeVersionAndOpCode(out);
      replaceBlockHeader.write(out);
      out.flush();
    }

    /* Receive a block copy response from the input stream */
    private void receiveResponse(DataInputStream in) throws IOException {
      short status = in.readShort();
      if (status != DataTransferProtocol.OP_STATUS_SUCCESS) {
        throw new IOException("block move is failed");
      }
    }

    /* reset the object */
    private void reset() {
      block = null;
      source = null;
      proxySource = null;
      target = null;
    }

    public void closeSocket() {
      try {
        this.sock.shutdownInput();
      } catch (IOException ex) {
        LOG.error("Error shutting down the socket to cancel block transfer");
      }
    }

    /* start a thread to dispatch the block move */
    private void scheduleBlockMove() {
      moverExecutor.execute(new Runnable() {
        public void run() {
          if (LOG.isDebugEnabled()) {
            LOG.debug("Starting moving "+ block.getBlockId() +
                " from " + proxySource.getName() + " to " + target.getName());
          }
          dispatch();
        }
      });
    }
  }

  /* A class for keeping track of blocks in the Balancer */
  static class BalancerBlock {
    private Block block; // the block
    private List<BalancerDatanode> locations
            = new ArrayList<BalancerDatanode>(3); // its locations

    /* Constructor */
    public BalancerBlock(Block block) {
      this.block = block;
    }

    /* clean block locations */
    public synchronized void clearLocations() {
      locations.clear();
    }

    /* add a location */
    public synchronized void addLocation(BalancerDatanode datanode) {
      if (!locations.contains(datanode)) {
        locations.add(datanode);
      }
    }

    /* Return if the block is located on <code>datanode</code> */
    public synchronized boolean isLocatedOnDatanode(
        BalancerDatanode datanode) {
      return locations.contains(datanode);
    }

    /* Return its locations */
    public synchronized List<BalancerDatanode> getLocations() {
      return locations;
    }

    /* Return the block */
    public Block getBlock() {
      return block;
    }

    /* Return the block id */
    public long getBlockId() {
      return block.getBlockId();
    }

    /* Return the length of the block */
    public long getNumBytes() {
      return block.getNumBytes();
    }

    /** Implements prioritization of blocks according to number of replicas */
    public static class BalancerBlockComparator implements Comparator<BalancerBlock> {
      @Override
      public int compare(BalancerBlock b0, BalancerBlock b1) {
        // Decreasing order by number of locations
        int ret = Integer.valueOf(b1.getLocations().size()).compareTo(b0.getLocations().size());
        // We have to distinguish blocks which are different
        return ret != 0 ? ret : Long.valueOf(b0.getBlockId()).compareTo(b1.getBlockId());
      }
    }
  }

  /* The class represents a desired move of bytes between two nodes
   * and the target.
   * An object of this class is stored in a source node.
   */
  static class NodeTask {
    private BalancerDatanode datanode; //target node
    private long size;  //bytes scheduled to move

    /* constructor */
    public NodeTask(BalancerDatanode datanode, long size) {
      this.datanode = datanode;
      this.size = size;
    }

    /* Get the node */
    private BalancerDatanode getDatanode() {
      return datanode;
    }

    /* Get the number of bytes that need to be moved */
    public long getSize() {
      return size;
    }
  }

  /* A class that keeps track of a datanode in Balancer */
  static abstract class BalancerDatanode implements Writable {
    public final DatanodeInfo datanode;
    public final double initialRemaining;
    protected double currentRemaining = 0.0;
    /** Number of bytes that can be moved */
    protected long freeMoveSize;
    /** Number of connections with this node that can be planned */
    protected int freeConnections;
    //  blocks being moved but not confirmed yet
    private List<PendingBlockMove> pendingBlocks =
      new ArrayList<PendingBlockMove>(maxConcurrentMoves);

    public BalancerDatanode(DatanodeInfo node, double avgRemaining) {
      datanode = node;
      initialRemaining = currentRemaining = BalancePlan.getRemaining(node);
      freeMoveSize = (long) (Math.abs(avgRemaining - currentRemaining) * datanode.getCapacity()
          / BalancePlan.PERCENTAGE_BASE);
      freeConnections = 1; // TODO balancing concurrency
    }

    /** Get the datanode */
    protected DatanodeInfo getDatanode() {
      return datanode;
    }

    /** Get the name of the datanode */
    protected String getName() {
      return datanode.getName();
    }

    /* Get the storage id of the datanode */
    protected String getStorageID() {
      return datanode.getStorageID();
    }

    /** PLAN PREPARATION */
    public int getFreeConnections() {
      return freeConnections;
    }

    public long getAvailableMoveSize() {
      return freeConnections > 0 ? freeMoveSize : 0;
    }

    /** Return remaining as it will be when all planned moves are executed */
    public double getCurrentRemaining() {
      return currentRemaining;
    }

    /** Add a node task */
    public abstract void addNodeTask(NodeTask task);

    /** PLAN EXECUTION */
    /* Check if the node can schedule more blocks to move */
    synchronized private boolean isPendingQNotFull() {
      if ( pendingBlocks.size() < maxConcurrentMoves ) {
        return true;
      }
      return false;
    }

    /* Check if all the dispatched moves are done */
    synchronized private boolean isPendingQEmpty() {
      return pendingBlocks.isEmpty();
    }

    synchronized private void killPending() {
      for (PendingBlockMove pendingBlock : pendingBlocks) {
        pendingBlock.closeSocket();
      }
    }

    /* Add a scheduled block move to the node */
    private synchronized boolean addPendingBlock(
        PendingBlockMove pendingBlock) {
      if (isPendingQNotFull()) {
        return pendingBlocks.add(pendingBlock);
      }
      return false;
    }

    /* Remove a scheduled block move from the node */
    private synchronized boolean  removePendingBlock(
        PendingBlockMove pendingBlock) {
      return pendingBlocks.remove(pendingBlock);
    }

    /** The following two methods support the Writable interface */
    /** Deserialize */
    public void readFields(DataInput in) throws IOException {
      datanode.readFields(in);
    }

    /** Serialize */
    public void write(DataOutput out) throws IOException {
      datanode.write(out);
    }
  }

  /** A node that can be the target of a block move */
  static class Target extends BalancerDatanode {
    public Target(DatanodeInfo node, double avgRemaining) {
      super(node, avgRemaining);
    }

    @Override
    public void addNodeTask(NodeTask task) {
      assert task.datanode != this : "Source and target are the same " + getName();
      freeMoveSize -= task.getSize();
      assert freeMoveSize >= 0 : "freeMoveSize < 0 for " + getName();
      freeConnections--;
      assert freeConnections >= 0 : "freeConnections < 0 for " + getName();
      currentRemaining -= (double) task.getSize() / datanode.getCapacity() * BalancePlan.PERCENTAGE_BASE;
    }
  }

  /** A node that can be the source of a block move */
  class Source extends BalancerDatanode {
    /** How many times we can ask NameNode for blocks */
    private int remainingFetchCount;

    public Source(DatanodeInfo node, double avgRemaining) {
      super(node, avgRemaining);
      freeConnections = 100; // TODO balancing concurrency
    }

    @Override
    public void addNodeTask(NodeTask task) {
      assert task.datanode != this : "Source and target are the same " + getName();
      scheduledSize += task.getSize();
      freeMoveSize -= task.getSize();
      assert freeMoveSize >= 0 : "freeMoveSize < 0 for " + getName();
      freeConnections--;
      assert freeConnections >= 0 : "freeConnections < 0 for " + getName();
      currentRemaining += (double) task.getSize() / datanode.getCapacity() * BalancePlan.PERCENTAGE_BASE;
      nodeTasks.add(task);
    }

    /** PLAN EXECUTION */
    /* A thread that initiates a block move
     * and waits for block move to complete */
    private class BlockMoveDispatcher implements Runnable {
      private long startTime;
      BlockMoveDispatcher(long time) {
        this.startTime = time;
      }
      public void run() {
        dispatchBlocks(startTime);
      }
    }

    private ArrayList<NodeTask> nodeTasks = new ArrayList<NodeTask>(2);
    /* source blocks point to balancerBlocks in the global list because
     * we want to keep one copy of a block in balancer and be aware that
     * the locations are changing over time.
     */
    private Collection<BalancerBlock> srcBlockList =
        new TreeSet<BalancerBlock>(new BalancerBlock.BalancerBlockComparator());
    /** Total size of all scheduled move requests */
    long scheduledSize = 0L;

    /* Return an iterator to this source's blocks */
    private Iterator<BalancerBlock> getBlockIterator() {
      return srcBlockList.iterator();
    }

    /** Fetch new blocks of this source from NameNode and update this source's block list & the
     * global block list */
    private void getBlockList() throws IOException {
      BlockWithLocations[] newBlocks = namenode.getBlocks(datanode, fetchBlocksSize).getBlocks();
      remainingFetchCount--;
      for (BlockWithLocations blk : newBlocks) {
        myMetrics.blocksFetched.inc();
        BalancerBlock block;
        synchronized(globalBlockList) {
          block = globalBlockList.get(blk.getBlock());
          if (block==null) {
            block = new BalancerBlock(blk.getBlock());
            globalBlockList.put(blk.getBlock(), block);
          } else {
            block.clearLocations();
          }

          synchronized (block) {
            // update locations
            for ( String location : blk.getDatanodes() ) {
              BalancerDatanode datanode = plan.datanodes.get(location);
              if (datanode != null) { // not an unknown datanode
                block.addLocation(datanode);
              }
            }
          }
          if (!srcBlockList.contains(block)
              && (block.getLocations().size() >= minBlockReplicas)
              && isGoodBlockCandidate(block)) {
            // filter bad candidates
            srcBlockList.add(block);
          } else {
            myMetrics.blocksFetchedAndDropped.inc();
          }
        }
      }
    }

    /* Decide if the given block is a good candidate to move or not */
    private boolean isGoodBlockCandidate(BalancerBlock block) {
      for (NodeTask nodeTask : nodeTasks) {
        if (Balancer.this.isGoodBlockCandidate(this, nodeTask.datanode, block)) {
          return true;
        }
      }
      return false;
    }

    /* Return a block that's good for the source thread to dispatch immediately
     * The block's source, target, and proxy source are determined too.
     * When choosing proxy and target, source & target throttling
     * has been considered. They are chosen only when they have the capacity
     * to support this block move.
     * The block should be dispatched immediately after this method is returned.
     */
    private PendingBlockMove chooseNextBlockToMove() {
      for ( Iterator<NodeTask> tasks=nodeTasks.iterator(); tasks.hasNext(); ) {
        NodeTask task = tasks.next();
        BalancerDatanode target = task.getDatanode();
        PendingBlockMove pendingBlock = new PendingBlockMove();
        if ( target.addPendingBlock(pendingBlock) ) {
          // target is not busy, so do a tentative block allocation
          pendingBlock.source = this;
          pendingBlock.target = target;
          if ( pendingBlock.chooseBlockAndProxy() ) {
            long blockSize = pendingBlock.block.getNumBytes();
            scheduledSize -= blockSize;
            task.size -= blockSize;
            if (task.size == 0) {
              tasks.remove();
            }
            return pendingBlock;
          } else {
            // cancel the tentative move
            target.removePendingBlock(pendingBlock);
          }
        }
      }
      return null;
    }

    /* iterate all source's blocks to remove moved ones */
    private void filterMovedBlocks() {
      for (Iterator<BalancerBlock> blocks=getBlockIterator();
            blocks.hasNext();) {
        if (movedBlocks.contains(blocks.next())) {
          blocks.remove();
        }
      }
    }

    private static final int SOURCE_BLOCK_LIST_MIN_SIZE=5;
    /* Return if should fetch more blocks from namenode */
    private boolean shouldFetchMoreBlocks() {
      return srcBlockList.size() < SOURCE_BLOCK_LIST_MIN_SIZE && remainingFetchCount > 0;
    }

    /* This method iteratively does the following:
     * it first selects a block to move,
     * then sends a request to the proxy source to start the block move
     * when the source's block list falls below a threshold, it asks
     * the namenode for more blocks.
     * It terminates when it has dispatch enough block move tasks or
     * it has received enough blocks from the namenode, or
     * the elapsed time of the iteration has exceeded the max time limit.
     */
    private void dispatchBlocks(long startTime) {
      remainingFetchCount = Balancer.this.maxFetchCount;
      boolean isTimeUp = false;
      while (!isTimeUp && scheduledSize > 0 &&
          (!srcBlockList.isEmpty() || remainingFetchCount > 0)) {

        // check if time is up or not
        // Even if not sent everything the iteration is over
        if (Util.now()-startTime > maxIterationTime) {
          isTimeUp = true;
          continue;
        }

        PendingBlockMove pendingBlock = chooseNextBlockToMove();
        if (pendingBlock != null) {
          // move the block
          pendingBlock.scheduleBlockMove();
          continue;
        }

        /* Since we can not schedule any block to move,
         * filter any moved blocks from the source block list and
         * check if we should fetch more blocks from the namenode
         */
        filterMovedBlocks(); // filter already moved blocks

        if (shouldFetchMoreBlocks()) {
          // fetch new blocks
          try {
            getBlockList();
            continue;
          } catch (IOException e) {
            LOG.warn(StringUtils.stringifyException(e));
            return;
          }
        }

        /* Now we can not schedule any block to move and there are
         * no new blocks added to the source block list, so we wait.
         */
        try {
          synchronized(Balancer.this) {
            Balancer.this.wait(1000);  // wait for targets/sources to be idle
          }
        } catch (InterruptedException ignored) {
        }
      }
    }
  }

  Source getSource(DatanodeInfo datanode, double avgRemaining) {
    return new Source(datanode, avgRemaining);
  }

  /*
   * Check that this Balancer is compatible with the Block Placement Policy
   * used by the Namenode.
   *
   * In case it is not compatible, throw IllegalArgumentException
   *
   * */
  private void checkReplicationPolicyCompatibility(Configuration conf) {
    if (!(BlockPlacementPolicy.getInstance(conf, null, null, null, null, null)
        instanceof BlockPlacementPolicyDefault)) {
      throw new IllegalArgumentException("Configuration lacks BlockPlacementPolicyDefault");
    }
  }

  /** Default constructor */
  Balancer() {
  }

  /** Construct a balancer from the given configuration */
  Balancer(Configuration conf) {
    setConf(conf);
    checkReplicationPolicyCompatibility(conf);
  }

  /** Construct a balancer from the given configuration and threshold */
  Balancer(Configuration conf, double threshold) {
    setConf(conf);
    checkReplicationPolicyCompatibility(conf);
    this.threshold = threshold;
  }

  /**
   * Run a balancer
   * @param args
   */
  public static void main(String[] args) {
    try {
      System.exit( ToolRunner.run(null, new Balancer(), args) );
    } catch (Throwable e) {
      LOG.error(StringUtils.stringifyException(e));
      System.exit(-1);
    }

  }

  private static void printUsage(Options opts) {
    HelpFormatter formatter = new HelpFormatter();
    formatter.printHelp("balancer", opts);
  }

  /* parse argument to get the threshold */
  private double checkThreshold(int value) {
    double threshold = (double) value;
    try {
      if (threshold < 0 || threshold > 100) {
        throw new NumberFormatException();
      }
      LOG.info("Using a threshold of " + threshold);
    } catch (NumberFormatException e) {
      System.err.println("Expect a double parameter in the range of [0, 100]: "
          + value);
      throw e;
    }
    return threshold;
  }

  protected void initNameNodes() throws IOException {
    this.namenode = createNamenode(namenodeAddress, conf);
    this.client = DFSClient.createNamenode(namenodeAddress, conf);
  }

  protected void initFS() throws IOException {
    this.fs = FileSystem.get(NameNode.getUri(namenodeAddress), conf);
  }

  /* Initialize balancer. It sets the value of the threshold, and
   * builds the communication proxies to
   * namenode as a client and a secondary namenode and retry proxies
   * when connection fails.
   */
  private void init(InetSocketAddress namenodeAddress) throws IOException {
    this.namenodeAddress = namenodeAddress;
    this.myMetrics = new BalancerMetrics(conf, namenodeAddress);
    this.fetchBlocksSize = conf.getLong(BalancerConfigKeys.DFS_BALANCER_FETCH_SIZE_KEY,
        BalancerConfigKeys.DFS_BALANCER_FETCH_SIZE_DEFAULT);
    this.minBlockReplicas = conf.getInt(BalancerConfigKeys.DFS_BALANCER_MIN_REPLICAS_KEY,
        BalancerConfigKeys.DFS_BALANCER_MIN_REPLICAS_DEFAULT);
    this.maxFetchCount = conf.getInt(BalancerConfigKeys.DFS_BALANCER_FETCH_COUNT_KEY,
        BalancerConfigKeys.DFS_BALANCER_FETCH_COUNT_DEFAULT);
    initFS();
    initNameNodes();
    this.moverExecutor = Executors.newFixedThreadPool(moveThreads);
    int dispatchThreads = (int)Math.max(1, moveThreads/maxConcurrentMoves);
    this.dispatcherExecutor = Executors.newFixedThreadPool(dispatchThreads);
    /* Check if there is another balancer running.
     * Exit if there is another one running.
     */
    this.out = checkAndMarkRunningBalancer();
    if (out == null) {
      throw new IOException("Another balancer is running");
    }
    // get namespace id
    LocatedBlocksWithMetaInfo locations = client.openAndFetchMetaInfo(BALANCER_ID_PATH.toString(), 0L, 1L);
    this.namespaceId = locations.getNamespaceID();
  }

  /* Build a NamenodeProtocol connection to the namenode and
   * set up the retry policy */
  protected static NamenodeProtocol createNamenode(InetSocketAddress nameNodeAddr, Configuration conf)
    throws IOException {
    RetryPolicy timeoutPolicy = RetryPolicies.exponentialBackoffRetry(
        5, 200, TimeUnit.MILLISECONDS);
    Map<Class<? extends Exception>,RetryPolicy> exceptionToPolicyMap =
      new HashMap<Class<? extends Exception>, RetryPolicy>();
    RetryPolicy methodPolicy = RetryPolicies.retryByException(
        timeoutPolicy, exceptionToPolicyMap);
    Map<String,RetryPolicy> methodNameToPolicyMap =
        new HashMap<String, RetryPolicy>();
    methodNameToPolicyMap.put("getBlocks", methodPolicy);

    UserGroupInformation ugi;
    try {
      ugi = UnixUserGroupInformation.login(conf);
    } catch (javax.security.auth.login.LoginException e) {
      throw new IOException(StringUtils.stringifyException(e));
    }

    return (NamenodeProtocol) RetryProxy.create(
        NamenodeProtocol.class,
        RPC.getProxy(NamenodeProtocol.class,
            NamenodeProtocol.versionID,
            nameNodeAddr,
            ugi,
            conf,
            NetUtils.getDefaultSocketFactory(conf)),
        methodNameToPolicyMap);
  }

  private void preparePlan() throws IOException {
    ArrayList<DatanodeInfo> report = new ArrayList<DatanodeInfo>();
    for (DatanodeInfo node : client.getDatanodeReport(DatanodeReportType.LIVE)) {
      if (!node.isDecommissioned() && !node.isDecommissionInProgress()) {
        report.add(node);
      }
    }
    plan = new BalancePlan(this, report);
  }

  private static class BytesMoved {
    private long bytesMoved = 0L;;
    private synchronized void inc( long bytes ) {
      bytesMoved += bytes;
    }

    private long get() {
      return bytesMoved;
    }
  };
  private BytesMoved bytesMoved = new BytesMoved();
  private int notChangedIterations = 0;

  /* Start a thread to dispatch block moves for each source.
   * The thread selects blocks to move & sends request to proxy source to
   * initiate block move. The process is flow controlled. Block selection is
   * blocked if there are too many un-confirmed block moves.
   * Return the total number of bytes successfully moved in this iteration.
   */
  private long dispatchBlockMoves() throws InterruptedException {
    long bytesLastMoved = bytesMoved.get();

    Future<?>[] futures = new Future<?>[plan.sources.size()];
    int i=0;
    for (Source source : plan.sources) {
      futures[i++] = dispatcherExecutor.submit(
                        source.new BlockMoveDispatcher(Util.now()));
    }

    // wait for all dispatcher threads to finish
    for (Future<?> future : futures) {
      try {
        future.get();
      } catch (ExecutionException e) {
        LOG.warn("Dispatcher thread failed", e.getCause());
      }
    }

    // wait for all block moving to be done
    waitForMoveCompletion();

    return bytesMoved.get()-bytesLastMoved;
  }

  // The sleeping period before checking if block move is completed again
  static private long blockMoveWaitTime = 30000L;
  // How many blockMoveWait to wait until stopping the move
  private static final int MAX_WAIT_ITERATIONS = 1;

  /** set the sleeping period for block move completion check */
  static void setBlockMoveWaitTime(long time) {
    blockMoveWaitTime = time;
  }

  /* wait for all block move confirmations
   * by checking each target's pendingMove queue
   */
  private void waitForMoveCompletion() {
    boolean shouldWait;
    int waitedIterations = 0;
    do {
      shouldWait = false;
      for (BalancerDatanode target : plan.targets) {
        if (!target.isPendingQEmpty()) {
          shouldWait = true;
        }
      }
      if (shouldWait) {
        try {
          if (waitedIterations > MAX_WAIT_ITERATIONS) {
            for (BalancerDatanode target : plan.targets) {
              target.killPending();
            }
            continue;
          }
          waitedIterations++;
          Thread.sleep(blockMoveWaitTime);
        } catch (InterruptedException ignored) {
        }
      }
    } while (shouldWait);
  }

  /** This window makes sure to keep blocks that have been moved within 1.5 hour.
   * Old window has blocks that are older;
   * Current window has blocks that are more recent;
   * Cleanup method triggers the check if blocks in the old window are
   * more than 1.5 hour old. If yes, purge the old window and then
   * move blocks in current window to old window.
   */
  private static class MovedBlocks {
    private long lastCleanupTime = System.currentTimeMillis();
    private static long winWidth = 5400*1000L; // 1.5 hour
    final private static int CUR_WIN = 0;
    final private static int OLD_WIN = 1;
    final private static int NUM_WINS = 2;
    final private List<HashMap<Block, BalancerBlock>> movedBlocks =
      new ArrayList<HashMap<Block, BalancerBlock>>(NUM_WINS);

    /* initialize the moved blocks collection */
    private MovedBlocks() {
      movedBlocks.add(new HashMap<Block,BalancerBlock>());
      movedBlocks.add(new HashMap<Block,BalancerBlock>());
    }

    public void setWinWidth(Configuration conf) {
      winWidth = conf.getLong(
          "dfs.balancer.movedWinWidth", 5400*1000L);
    }

    /* add a block thus marking a block to be moved */
    synchronized private void add(BalancerBlock block) {
      movedBlocks.get(CUR_WIN).put(block.getBlock(), block);
    }

    /* check if a block is marked as moved */
    synchronized private boolean contains(BalancerBlock block) {
      return contains(block.getBlock());
    }

    /* check if a block is marked as moved */
    synchronized private boolean contains(Block block) {
      return movedBlocks.get(CUR_WIN).containsKey(block) ||
        movedBlocks.get(OLD_WIN).containsKey(block);
    }

    /* remove old blocks */
    synchronized private void cleanup() {
      long curTime = System.currentTimeMillis();
      // check if old win is older than winWidth
      if (lastCleanupTime + winWidth <= curTime) {
        // purge the old window
        movedBlocks.set(OLD_WIN, movedBlocks.get(CUR_WIN));
        movedBlocks.set(CUR_WIN, new HashMap<Block, BalancerBlock>());
        lastCleanupTime = curTime;
      }
    }
  }

  /* Decide if it is OK to move the given block from source to target
   * A block is a good candidate if
   * 1. the block is not in the process of being moved/has not been moved;
   * 2. the block does not have a replica on the target;
   * 3. doing the move does not reduce the number of racks that the block has
   */
  private boolean isGoodBlockCandidate(Source source,
      BalancerDatanode target, BalancerBlock block) {
    // check if the block is moved or not
    if (movedBlocks.contains(block)) {
        return false;
    }
    if (block.isLocatedOnDatanode(target)) {
      return false;
    }

    boolean goodBlock = false;
    if (plan.cluster.isOnSameRack(source.getDatanode(), target.getDatanode())) {
      // good if source and target are on the same rack
      goodBlock = true;
    } else {
      boolean notOnSameRack = true;
      synchronized (block) {
        for (BalancerDatanode loc : block.locations) {
          if (plan.cluster.isOnSameRack(loc.datanode, target.datanode)) {
            notOnSameRack = false;
            break;
          }
        }
      }
      if (notOnSameRack) {
        // good if target is target is not on the same rack as any replica
        goodBlock = true;
      } else {
        // good if source is on the same rack as on of the replicas
        for (BalancerDatanode loc : block.locations) {
          if (loc != source &&
              plan.cluster.isOnSameRack(loc.datanode, source.datanode)) {
            goodBlock = true;
            break;
          }
        }
      }
    }
    return goodBlock;
  }

  /* reset all fields in a balancer preparing for the next iteration */
  private void resetData() {
    this.plan = null;
    cleanGlobalBlockList();
    this.movedBlocks.cleanup();
  }

  /* Remove all blocks from the global block list except for the ones in the
   * moved list.
   */
  private void cleanGlobalBlockList() {
    for (Iterator<Block> globalBlockListIterator=globalBlockList.keySet().iterator();
    globalBlockListIterator.hasNext();) {
      Block block = globalBlockListIterator.next();
      if(!movedBlocks.contains(block)) {
        globalBlockListIterator.remove();
      }
    }
  }

  @SuppressWarnings(value = { "static-access" })
  private Options setupOptions() {
    Options cliOpts = new Options();
    cliOpts.addOption(OptionBuilder.hasArg().hasArgs(1).withDescription(
        "percentage of disk capacity. Default is 10")
        .isRequired(false).create("threshold"));
    cliOpts.addOption(OptionBuilder.hasArg().hasArgs(1).isRequired(false)
        .withDescription("The length of an iteration in minutes. " +
                          "Default is " + maxIterationTime/(60 * 1000)).
        create("iter_len"));
    cliOpts.addOption(OptionBuilder.hasArg().hasArgs(1).isRequired(false)
        .withDescription("The number of blocks to move in parallel to " +
        "one node. Default is " + MAX_NUM_CONCURRENT_MOVES).
        create("node_par_moves"));
    cliOpts.addOption(OptionBuilder.hasArg().hasArgs(1).isRequired(false)
        .withDescription("The number of blocks to move in parallel " +
        "in total for the cluster. Default is " + MOVER_THREAD_POOL_SIZE)
        .create("par_moves"));
    return cliOpts;
  }

  // Exit status
  final public static int SUCCESS = 0;
  final public static int IN_PROGRESS = 1;
  final public static int ALREADY_RUNNING = -1;
  final public static int NO_MOVE_BLOCK = -2;
  final public static int NO_MOVE_PROGRESS = -3;
  final public static int IO_EXCEPTION = -4;
  final public static int ILLEGAL_ARGS = -5;
  final public static int INTERRUPTED = -6;
  final public static int NOT_ALL_BALANCED = -7;

  public int run(String[] args) throws Exception {
    final long startTime = Util.now();
    try {
      checkReplicationPolicyCompatibility(conf);
      final List<InetSocketAddress> namenodes = DFSUtil.getClientRpcAddresses(conf, null);
      parse(args);
      return Balancer.run(namenodes, conf);
    } catch (IOException e) {
      System.out.println(e + ".  Exiting ...");
      return IO_EXCEPTION;
    } catch (InterruptedException e) {
      System.out.println(e + ".  Exiting ...");
      return INTERRUPTED;
    } catch (Exception e) {
      e.printStackTrace();
      return ILLEGAL_ARGS;
    } finally {
      System.out.println("Balancing took " + time2Str(Util.now()-startTime));
    }
  }

  /** parse command line arguments */
  private void parse(String[] args) {
    Options cliOpts = setupOptions();
    BasicParser parser = new BasicParser();
    CommandLine cl = null;
    try {
      try {
       cl = parser.parse(cliOpts, args);
      } catch (ParseException ex) {
        throw new IllegalArgumentException("args = " + Arrays.toString(args));
      }

      int newThreshold = Integer.parseInt(cl.getOptionValue("threshold", "10"));
      int iterationTime = Integer.parseInt(cl.getOptionValue("iter_len",
                                 String.valueOf(maxIterationTime/(60 * 1000))));
      maxConcurrentMoves = Integer.parseInt(cl.getOptionValue("node_par_moves",
                                     String.valueOf(MAX_NUM_CONCURRENT_MOVES)));
      moveThreads = Integer.parseInt(cl.getOptionValue("par_moves",
                                     String.valueOf(MOVER_THREAD_POOL_SIZE)));
      maxIterationTime = iterationTime * 60 * 1000L;

      threshold = checkThreshold(newThreshold);
      System.out.println("Running with threshold of " + threshold
          + " and iteration time of " + maxIterationTime + " milliseconds");
    } catch (RuntimeException e) {
      printUsage(cliOpts);
      throw e;
    }
  }

  private static Balancer getBalancerInstance(Configuration conf)
      throws IOException {
    Class<?> clazz = conf.getClass("dfs.balancer.impl", Balancer.class);
    if (!Balancer.class.isAssignableFrom(clazz)) {
      throw new IOException("Invalid class for balancer : " + clazz);
    }
    return (Balancer) ReflectionUtils.newInstance(clazz, conf);
  }


  /**
   * Balance all namenodes.
   * For each iteration,
   * for each namenode,
   * execute a {@link Balancer} to work through all datanodes once.
   */
  static int run(List<InetSocketAddress> namenodes,
      Configuration conf) throws IOException, InterruptedException {
    final long sleepms = 1000L * 2 * conf.getLong("dfs.heartbeat.interval", 3);
    LOG.info("namenodes = " + namenodes);
    Formatter formatter = new Formatter(System.out);
    System.out.println("Time Stamp               Iteration#  Bytes Already Moved  Bytes Left To Move  Bytes Being Moved  Iterations Left  Seconds Left");

    final List<Balancer> balancers
        = new ArrayList<Balancer>(namenodes.size());
    boolean failNN = false;
    try {
      for(InetSocketAddress isa : namenodes) {
        try{
          Balancer b = Balancer.getBalancerInstance(conf);
          b.init(isa);
          balancers.add(b);
        } catch (IOException e) {
          e.printStackTrace();
          LOG.error("Cannot connect to namenode: " + isa);
          failNN = true;
        }
      }

      boolean done = false;
      for(int iterations = 0; !done && balancers.size() > 0; iterations++) {
        done = true;
        Collections.shuffle(balancers);
        Iterator<Balancer> iter = balancers.iterator();
        while (iter.hasNext()) {
          Balancer b = iter.next();
          b.resetData();
          final int r = b.run(iterations, formatter);
          if (r == IN_PROGRESS) {
            done = false;
          } else if (r != SUCCESS) {
            //Remove this balancer
            b.close();
            LOG.info("Namenode " + b.namenodeAddress + " balancing exits...");
            iter.remove();
            continue;
          }
        }

        if (!done) {
          Thread.sleep(sleepms);
        }
      }
    } finally {
      for(Balancer b : balancers) {
        b.close();
      }
    }
    if (failNN) {
      LOG.warn("Could not initialize all balancers");
      return NOT_ALL_BALANCED;
    }
    return SUCCESS;
  }

  public int run(int iterations, Formatter formatter){
    try {
      preparePlan();
      if (plan.bytesLeftToMove == 0) {
        System.out.println("The cluster is balanced after " + iterations
            + " iterations. Exiting...");
        return SUCCESS;
      } else {
        LOG.info("Need to move " + StringUtils.byteDesc(plan.bytesLeftToMove)
            + " bytes to make the cluster balanced.");
      }
      if (plan.bytesToMove == 0) {
        System.out.println("No block can be moved. Exiting...");
        return NO_MOVE_BLOCK;
      } else {
        LOG.info("Will move " + StringUtils.byteDesc(plan.bytesToMove) + "bytes in this iteration");
      }

      long moved = bytesMoved.get();
      String iterationsLeft = "N/A";
      String timeLeft = "N/A";
      if (iterations != 0 && moved != 0) {
        long bytesPerIteration = moved / iterations;
        long iterLeft = plan.bytesLeftToMove / bytesPerIteration;
        iterationsLeft = String.valueOf(iterLeft );
        long secondsPerIteration = (maxIterationTime + blockMoveWaitTime)/1000;
        long secondsLeft = secondsPerIteration * iterLeft;
        long daysLeft = TimeUnit.SECONDS.toDays(secondsLeft);
        timeLeft = "";
        if (daysLeft > 0) {
          timeLeft = timeLeft + daysLeft + "d ";
        }
        long hoursLeft = TimeUnit.SECONDS.toHours(secondsLeft) -
                         TimeUnit.DAYS.toHours(daysLeft);
        if (hoursLeft > 0) {
          timeLeft = timeLeft + hoursLeft + "h ";
        }
        long minutesLeft = TimeUnit.SECONDS.toMinutes(secondsLeft) -
                           TimeUnit.HOURS.toMinutes(hoursLeft) -
                           TimeUnit.DAYS.toMinutes(daysLeft);
        timeLeft = timeLeft + minutesLeft + "m";

      }

      formatter.format("%-24s %10d  %19s  %18s  %17s  %15s  %12s\n",
          DateFormat.getDateTimeInstance().format(new Date()),
          iterations,
          StringUtils.byteDesc(bytesMoved.get()),
          StringUtils.byteDesc(plan.bytesLeftToMove),
          StringUtils.byteDesc(plan.bytesToMove),
          iterationsLeft,
          timeLeft
          );

      /* For each pair of <source, target>, start a thread that repeatedly
       * decide a block to be moved and its proxy source,
       * then initiates the move until all bytes are moved or no more block
       * available to move.
       * Exit no byte has been moved for 5 consecutive iterations.
       */
      if (dispatchBlockMoves() > 0) {
        notChangedIterations = 0;
      } else {
        notChangedIterations++;
        if (notChangedIterations >= 5) {
          System.out.println(
              "No block has been moved for 5 iterations. Exiting...");
          return NO_MOVE_PROGRESS;
        }
      }

      // clean all lists
      resetData();

      return IN_PROGRESS;
    } catch (IllegalArgumentException ae) {
      ae.printStackTrace();
      return ILLEGAL_ARGS;
    } catch (IOException e) {
      e.printStackTrace();
      System.out.println("Received an IO exception: " + e.getMessage() +
          " . Exiting...");
      return IO_EXCEPTION;
    } catch (InterruptedException e) {
      e.printStackTrace();
      return INTERRUPTED;
    } catch (Exception ex) {
      ex.printStackTrace();
      return ILLEGAL_ARGS;
    } finally {
    }
  }

  /** Close the connection. */
  void close() {
    shuttingDown = true;
    // shutdown thread pools
    dispatcherExecutor.shutdownNow();
    moverExecutor.shutdownNow();
    // close the output file
    IOUtils.closeStream(out);
    if (fs != null) {
      try {
        fs.delete(BALANCER_ID_PATH, true);
      } catch(IOException ignored) {
      }
    }
  }

  private Path BALANCER_ID_PATH = new Path("/system/balancer.id");
  /* The idea for making sure that there is no more than one balancer
   * running in an HDFS is to create a file in the HDFS, writes the IP address
   * of the machine on which the balancer is running to the file, but did not
   * close the file until the balancer exits.
   * This prevents the second balancer from running because it can not
   * creates the file while the first one is running.
   *
   * This method checks if there is any running balancer and
   * if no, mark yes if no.
   * Note that this is an atomic operation.
   *
   * Return null if there is a running balancer; otherwise the output stream
   * to the newly created file.
   */
  private OutputStream checkAndMarkRunningBalancer() throws IOException {
    try {
      DataOutputStream out = fs.create(BALANCER_ID_PATH);
      out. writeBytes(InetAddress.getLocalHost().getHostName());
      out.flush();
      return out;
    } catch(RemoteException e) {
      if(AlreadyBeingCreatedException.class.getName().equals(e.getClassName())){
        return null;
      } else {
        throw e;
      }
    }
  }

  /* Given elaspedTime in ms, return a printable string */
  private static String time2Str(long elapsedTime) {
    String unit;
    double time = elapsedTime;
    if (elapsedTime < 1000) {
      unit = "milliseconds";
    } else if (elapsedTime < 60*1000) {
      unit = "seconds";
      time = time/1000;
    } else if (elapsedTime < 3600*1000) {
      unit = "minutes";
      time = time/(60*1000);
    } else {
      unit = "hours";
      time = time/(3600*1000);
    }

    return time+" "+unit;
  }

  /** return this balancer's configuration */
  public Configuration getConf() {
    return conf;
  }

  /** set this balancer's configuration */
  public void setConf(Configuration conf) {
    this.conf = conf;
    movedBlocks.setWinWidth(conf);
  }
}
