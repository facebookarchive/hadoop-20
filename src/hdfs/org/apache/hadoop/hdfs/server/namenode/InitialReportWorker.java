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
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.BlockListAsLongs;
import org.apache.hadoop.hdfs.server.namenode.BlocksMap.BlockInfo;
import org.apache.hadoop.hdfs.util.InjectionEvent;
import org.apache.hadoop.util.InjectionHandler;

/**
 * Worker class for initial in-safemode block reports. Each worker traverses a
 * part of input block report. For each block it calls addStoredBlock, which
 * only changes stored blocks in the blocks map, the datanode descriptor itself
 * is not changed!! The worker collects all its blocks into a linked list, which
 * is later inserted into the datanode descriptor.
 * 
 * The class has a static method for processing a block report, so it can be
 * directly called from FSNamesystem.
 */
class InitialReportWorker implements Callable<List<Block>> {

  static final Log LOG = LogFactory.getLog(InitialReportWorker.class);

  // parent namesystem
  private final FSNamesystem namesystem;

  private final BlockListAsLongs report;
  private final int startIndex;
  private final int jump;
  private final DatanodeDescriptor node;

  // safe blocks for this worker
  private int localBlocksSafe = 0;
  private int addedBlocks = 0;

  // we keep local linked list of blocks inserted
  // to DN's descriptor by this thread
  // we keep tail for fast final insertion
  private BlockInfo head = null;
  private int headIndex = -1;
  private BlockInfo tail = null;
  private int tailIndex = -1;

  // avatar dependent local retry list
  private final boolean shouldRetryAbsentBlocks;
  private final List<Block> toRetry;

  // currently processed block
  BlockInfo currentStoredBlock = null;
  int currentStoredBlockIndex = -1;

  private InitialReportWorker(BlockListAsLongs report, int startIndex,
      int jump, DatanodeDescriptor node, boolean shouldRetryAbsentBlocks,
      FSNamesystem namesystem) {
    this.report = report;
    this.startIndex = startIndex;
    this.jump = jump;
    this.node = node;
    this.shouldRetryAbsentBlocks = shouldRetryAbsentBlocks;
    this.toRetry = shouldRetryAbsentBlocks ? new LinkedList<Block>() : null;
    this.namesystem = namesystem;
  }

  @Override
  public List<Block> call() throws Exception {
    InjectionHandler.processEvent(InjectionEvent.FSNAMESYSTEM_INITIALBR_WORKER);
    Block iblk = new Block();
    final int numberOfBlocks = report.getNumberOfBlocks();
    for (int i = startIndex; i < numberOfBlocks; i += jump) {
      report.getBlock(iblk, i);
      boolean added = false;

      // clear information about the current stored block
      resetCurrentStoredBlock();

      // add each block to blocks map, and store it in the local list for
      // datanode sescriptor
      try {
        // add location to the blocks map
        added = namesystem.addStoredBlockInternal(iblk, node, null, true, this);

        // store the block locally
        insertCurrentStoredBlockIntoList();

        // check if it needs to be retried
        if (shouldRetryAbsentBlocks && !added
            && namesystem.getNameNode().shouldRetryAbsentBlock(iblk, null)) {
          toRetry.add(new Block(iblk));
        }
      } catch (Exception e) {
        LOG.info("Initial lock report worker got exception:", e);
      }
    }

    // insert the block into the local list for datanode descriptor.
    namesystem.lockParallelBRLock(true);
    try {
      // now insert local list into datanode descriptor
      node.insertIntoList(head, headIndex, tail, tailIndex, addedBlocks);
      // we know we are in safemode
      namesystem.incrementSafeBlockCount(localBlocksSafe);
    } finally {
      namesystem.unlockParallelBRLock(true);
    }

    // blocks to be re-tried
    return toRetry;
  }

  /**
   * Clear information about the local stored block.
   */
  private void resetCurrentStoredBlock() {
    this.currentStoredBlock = null;
    this.currentStoredBlockIndex = -1;
  }

  /**
   * FSNmaesystem.addStoredBlock will inform this worker about the stored block,
   * so it can be later inserted into the local list of blocks.
   */
  void setCurrentStoredBlock(BlockInfo storedBlock, int index) {
    this.currentStoredBlock = storedBlock;
    this.currentStoredBlockIndex = index;
  }

  /**
   * Insert the current stored block into the local list of blocks belonging to
   * the datanode descriptor.
   */
  private void insertCurrentStoredBlockIntoList() {
    // index < 0 - block is already in the DN's list
    if (currentStoredBlock == null || currentStoredBlockIndex < 0)
      return;

    if (head == null) {
      // local list is empty,
      // make head and tail point to the input block
      head = currentStoredBlock;
      headIndex = currentStoredBlockIndex;

      tail = currentStoredBlock;
      tailIndex = currentStoredBlockIndex;

      // for sanity, make sure the block is not pointing to anything
      head.setNext(currentStoredBlockIndex, null);
      head.setPrevious(currentStoredBlockIndex, null);
    } else {
      // connect input block with current head
      head.setPrevious(headIndex, currentStoredBlock);
      currentStoredBlock.setNext(currentStoredBlockIndex, head);

      // stored block is the new head
      head = currentStoredBlock;
      headIndex = currentStoredBlockIndex;
    }
    // increment number of blocks in the local list
    addedBlocks++;

    // clear the current stored block information
    resetCurrentStoredBlock();
  }

  /**
   * Increment local count of safe blocks for this worker
   */
  public void incrementSafeBlockCount(int replication) {
    if (replication == namesystem.getMinReplication()) {
      localBlocksSafe++;
    }
  }

  /**
   * Processes a single initial block reports, by spawning multiple threads to
   * handle insertion to the blocks map. Each thread stores the inserted blocks
   * in a local list, and at the end, the list are concatenated for a single
   * datanode descriptor.
   */
  static void processReport(FSNamesystem namesystem, Collection<Block> toRetry,
      BlockListAsLongs newReport, DatanodeDescriptor node,
      ExecutorService initialBlockReportExecutor) throws IOException {
    // spawn one thread for blocksPerShardBR blocks
    int numShards = Math
        .min(
            namesystem.parallelProcessingThreads,
            ((newReport.getNumberOfBlocks()
                + namesystem.parallelBRblocksPerShard - 1) / namesystem.parallelBRblocksPerShard));
    List<Future<List<Block>>> workers = new ArrayList<Future<List<Block>>>(
        numShards);

    // submit tasks for execution
    for (int i = 0; i < numShards; i++) {
      workers.add(initialBlockReportExecutor.submit(new InitialReportWorker(
          newReport, i, numShards, node, namesystem.getNameNode()
              .shouldRetryAbsentBlocks(), namesystem)));
    }

    // get results and add to retry list if need
    try {
      for (Future<List<Block>> worker : workers) {
        if (namesystem.getNameNode().shouldRetryAbsentBlocks()) {
          toRetry.addAll(worker.get());
        } else {
          worker.get();
        }
      }
    } catch (ExecutionException e) {
      LOG.warn("Parallel report failed", e);
      throw new IOException(e);
    } catch (InterruptedException e) {
      throw new IOException("Interruption", e);
    }
  }
}
