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
package org.apache.hadoop.hdfs.server.datanode;

import java.io.IOException;
import java.io.File;
import java.io.FileOutputStream;
import java.io.DataOutputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.TreeSet;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.text.SimpleDateFormat;

import org.apache.hadoop.ipc.*;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.hdfs.AvatarZooKeeperClient;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.protocol.FSConstants;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.BlockListAsLongs;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.server.protocol.BlockCommand;
import org.apache.hadoop.hdfs.server.protocol.BlockReport;
import org.apache.hadoop.hdfs.server.protocol.DatanodeCommand;
import org.apache.hadoop.hdfs.server.protocol.DatanodeProtocol;
import org.apache.hadoop.hdfs.server.protocol.InterDatanodeProtocol;
import org.apache.hadoop.hdfs.server.protocol.ReceivedBlockInfo;
import org.apache.hadoop.hdfs.server.protocol.UpgradeCommand;
import org.apache.hadoop.hdfs.server.protocol.DatanodeRegistration;
import org.apache.hadoop.hdfs.server.datanode.AvatarDataNode.ServicePair;
import org.apache.hadoop.hdfs.server.datanode.DataNode.BlockRecord;
import org.apache.hadoop.hdfs.server.datanode.DataNode.KeepAliveHeartbeater;
import org.apache.hadoop.hdfs.server.datanode.metrics.DataNodeMetrics;
import org.apache.hadoop.hdfs.protocol.AvatarProtocol;
import org.apache.zookeeper.data.Stat;

public class OfferService implements Runnable {

  public static final Log LOG = LogFactory.getLog(OfferService.class.getName());
  private int numFrequentReports;
  private int freqReportsCoeff;

  private final static int BACKOFF_DELAY = 5 * 60 * 1000;

  long lastHeartbeat = 0;
  volatile boolean shouldRun = true;
  long lastBlockReport = 0;
  long lastDeletedReport = 0;
  boolean resetBlockReportTime = true;
  long blockReceivedRetryInterval;
  int reportsSinceRegister;
  AvatarDataNode anode;
  DatanodeProtocol namenode;
  AvatarProtocol avatarnode;
  InetSocketAddress namenodeAddress;
  InetSocketAddress avatarnodeAddress;
  DatanodeRegistration nsRegistration = null;
  FSDatasetInterface data;
  DataNodeMetrics myMetrics;
  ScheduledExecutorService keepAliveSender = null;
  ScheduledFuture keepAliveRun = null;
  private static final Random R = new Random();
  private int backlogSize; // if we accumulate this many blockReceived, then it is time
                           // to send a block report. Otherwise the receivedBlockList
                           // might exceed our Heap size.
  private LinkedList<Block> receivedAndDeletedRetryBlockList
          = new LinkedList<Block>();
  private LinkedList<Block> receivedAndDeletedBlockList
          = new LinkedList<Block>();

  private int pendingReceivedRequests = 0;

  private long lastBlockReceivedFailed = 0;
  private ServicePair servicePair;
  
  private boolean shouldBackoff = false;
  private boolean firstBlockReportSent = false;

  /**
   * Offer service to the specified namenode
   */
  public OfferService(AvatarDataNode anode, ServicePair servicePair,
		              DatanodeProtocol namenode, InetSocketAddress namenodeAddress,
		              AvatarProtocol avatarnode, InetSocketAddress avatarnodeAddress) {
    numFrequentReports = anode.getConf().getInt(
        "ha.blockreport.frequentReports", 4);
    freqReportsCoeff = anode.getConf()
    .getInt("ha.blockreport.frequent.coeff", 2);
    this.anode = anode;
    this.servicePair = servicePair;
    this.namenode = namenode;
    this.avatarnode = avatarnode;
    this.namenodeAddress = namenodeAddress;
    this.avatarnodeAddress = avatarnodeAddress;
    
    reportsSinceRegister = numFrequentReports;

    nsRegistration = servicePair.nsRegistration;
    data = anode.data;
    myMetrics = anode.myMetrics;
    scheduleBlockReport(anode.initialBlockReportDelay);
    backlogSize = anode.getConf().getInt("dfs.datanode.blockreceived.backlog", 10000);
    blockReceivedRetryInterval = anode.getConf().getInt(
        "dfs.datanode.blockreceived.retry.internval", 10000);
  }

  public void stop() {
    if (keepAliveRun != null) {
      keepAliveRun.cancel(false);
    }
    if (keepAliveSender != null) {
      keepAliveSender.shutdownNow();
    }
    shouldRun = false;
  }

  private boolean isPrimaryService() {
    try {
      Stat stat = new Stat();
      String actual = servicePair.zkClient.getPrimaryAvatarAddress(
          servicePair.defaultAddr, stat, true);
      String offerServiceAddress = this.namenodeAddress.getHostName() + ":"
          + this.namenodeAddress.getPort();
      return actual.equalsIgnoreCase(offerServiceAddress);
    } catch (Exception ex) {
      LOG.error("Could not get the primary from ZooKeeper", ex);
    }
    return false;
  }

  public void run() {
    KeepAliveHeartbeater keepAliveTask = 
        new KeepAliveHeartbeater(namenode, nsRegistration, this.servicePair);
    keepAliveSender = Executors.newSingleThreadScheduledExecutor();
    keepAliveRun = keepAliveSender.scheduleAtFixedRate(keepAliveTask, 0,
                                                       anode.heartBeatInterval,
                                                       TimeUnit.MILLISECONDS);
    while (shouldRun) {
      try {
        if (isPrimaryService()) {
          servicePair.setPrimaryOfferService(this);
        }
        offerService();
      } catch (Exception e) {
        LOG.error("OfferService encountered exception " +
			   StringUtils.stringifyException(e));
      }
    }
  }

  private void setBackoff(boolean value) {
    synchronized (receivedAndDeletedBlockList) {
      this.shouldBackoff = value;
    }
  }
  public void offerService() throws Exception {

    LOG.info("using BLOCKREPORT_INTERVAL of " + anode.blockReportInterval + "msec" + 
       " Initial delay: " + anode.initialBlockReportDelay + "msec");
    LOG.info("using DELETEREPORT_INTERVAL of " + anode.deletedReportInterval
        + "msec");
    LOG.info("using HEARTBEAT_EXPIRE_INTERVAL of " + anode.heartbeatExpireInterval + "msec");

    //
    // Now loop for a long time....
    //
    while (shouldRun) {
      try {

        // If we are falling behind in confirming blockReceived to NN, then
        // we clear the backlog and schedule a block report. This scenario
        // is likely to arise if one of the NN is down for an extended period.
        if (receivedAndDeletedBlockList.size() + receivedAndDeletedRetryBlockList.size()
             > backlogSize) {
          LOG.warn("The backlog of blocks to be confirmed has exceeded the " +
                   " configured maximum of " + backlogSize +
                   " records. Cleaning up and scheduling a block report.");
          synchronized(receivedAndDeletedBlockList) {
            receivedAndDeletedBlockList.clear();
            receivedAndDeletedRetryBlockList.clear();
          }
          scheduleBlockReport(0);
        }

        long startTime = anode.now();

        //
        // Every so often, send heartbeat or block-report
        //
        if (startTime - lastHeartbeat > anode.heartBeatInterval) {
          //
          // All heartbeat messages include following info:
          // -- Datanode name
          // -- data transfer port
          // -- Total capacity
          // -- Bytes remaining
          //
          setBackoff(false);
          lastHeartbeat = startTime;
          DatanodeCommand[] cmds = avatarnode.sendHeartbeatNew(nsRegistration,
                                                         data.getCapacity(),
                                                         data.getDfsUsed(),
                                                         data.getRemaining(),
                                                         //TODO support namespace id in the future?
                                                         data.getDfsUsed(),
                                                         anode.xmitsInProgress.get(),
                                                         anode.getXceiverCount());
          this.servicePair.lastBeingAlive = anode.now();
          LOG.debug("Sent heartbeat at " + this.servicePair.lastBeingAlive);
          myMetrics.heartbeats.inc(anode.now() - startTime);
          //LOG.info("Just sent heartbeat, with name " + localName);
          if (!processCommand(cmds))
            continue;
        }

        // check if there are newly received blocks (pendingReceivedRequeste > 0
        // or if the deletedReportInterval passed.

        if (firstBlockReportSent && !shouldBackoff && (pendingReceivedRequests > 0
            || (startTime - lastDeletedReport > anode.deletedReportInterval))) {

          // check if there are newly received blocks
          Block[] receivedAndDeletedBlockArray = null;
          int numBlocksReceivedAndDeleted = 0;
          int currentPendingRequests = 0;

          synchronized (receivedAndDeletedBlockList) {
            // if the retry list is empty, send the receivedAndDeletedBlockList
            // if it's not empty, check the timer if we can retry
            if (receivedAndDeletedRetryBlockList.isEmpty() ||
                (lastBlockReceivedFailed + blockReceivedRetryInterval < startTime)) {

              // process the retry list first
              // insert all blocks to the front of the block list
              // (we maintain the order in which entries were inserted)
              Iterator<Block> blkIter = receivedAndDeletedRetryBlockList
                  .descendingIterator();
              while (blkIter.hasNext()) {
                receivedAndDeletedBlockList.add(0, blkIter.next());
              }
              receivedAndDeletedRetryBlockList.clear();

              // construct the ACKs array
              lastDeletedReport = startTime;
              numBlocksReceivedAndDeleted = receivedAndDeletedBlockList.size();
              if (numBlocksReceivedAndDeleted > 0) {
                receivedAndDeletedBlockArray = receivedAndDeletedBlockList
                    .toArray(new Block[numBlocksReceivedAndDeleted]);
                receivedAndDeletedBlockList.clear();
                currentPendingRequests = pendingReceivedRequests;
                pendingReceivedRequests = 0;
              }
            }
          }

          // process received + deleted
          // if exception is thrown, add all blocks to the retry list
          if (receivedAndDeletedBlockArray != null) {
            Block[] failed = null;
            try {
              failed = avatarnode.blockReceivedAndDeletedNew(nsRegistration,
                  receivedAndDeletedBlockArray);
            } catch (Exception e) {
              synchronized (receivedAndDeletedBlockList) {
                for (int i = 0; i < receivedAndDeletedBlockArray.length; i++) {
                  receivedAndDeletedBlockList.add(0,
                      receivedAndDeletedBlockArray[i]);
                }
                pendingReceivedRequests += currentPendingRequests;
              }
              throw e;
            }
            processFailedReceivedDeleted(failed, receivedAndDeletedBlockArray);
          }
        }

        // send block report
        if (startTime - lastBlockReport > anode.blockReportInterval) {
          if (shouldBackoff) {
            scheduleBlockReport(BACKOFF_DELAY);
            LOG.info("Backoff blockreport. Will be sent in " +
              (lastBlockReport + anode.blockReportInterval - startTime) + "ms");
          } else {
          //
          // Send latest blockinfo report if timer has expired.
          // Get back a list of local block(s) that are obsolete
          // and can be safely GC'ed.
          //
          long brStartTime = anode.now();
          Block[] bReport = data.getBlockReport(servicePair.namespaceId);
          DatanodeCommand cmd = avatarnode.blockReportNew(nsRegistration,
                  new BlockReport(BlockListAsLongs.convertToArrayLongs(bReport)));
          if (cmd != null && 
              cmd.getAction() == DatanodeProtocols.DNA_BACKOFF) {
            // The Standby is catching up and we need to reschedule
            scheduleBlockReport(BACKOFF_DELAY);
            continue;
          }

          firstBlockReportSent = true;
          long brTime = anode.now() - brStartTime;
          myMetrics.blockReports.inc(brTime);
          LOG.info("BlockReport of " + bReport.length +
              " blocks got processed in " + brTime + " msecs on " +
              namenodeAddress);
          if (reportsSinceRegister < numFrequentReports) {
            // Schedule block reports more frequently for the first 
            // numFrequentReports reports. This helps the standby node
            // get full information about block locations faster
            int frequentReportsInterval = 
              (int) (anode.blockReportInterval / freqReportsCoeff);
            scheduleBlockReport(frequentReportsInterval);
            reportsSinceRegister++;
            LOG.info("Scheduling frequent report #" + reportsSinceRegister);
          } else if (resetBlockReportTime) {
            //
            // If we have sent the first block report, then wait a random
            // time before we start the periodic block reports.
            //
            lastBlockReport = startTime - R.nextInt((int)(anode.blockReportInterval));
            resetBlockReportTime = false;
          } else {

            /* say the last block report was at 8:20:14. The current report 
             * should have started around 9:20:14 (default 1 hour interval). 
             * If current time is :
             *   1) normal like 9:20:18, next report should be at 10:20:14
             *   2) unexpected like 11:35:43, next report should be at 12:20:14
             */
            lastBlockReport += (anode.now() - lastBlockReport) / 
                               anode.blockReportInterval * anode.blockReportInterval;
          }
          processCommand(cmd);
        }
        }

        // start block scanner is moved to the Dataode.run()

        //
        // There is no work to do;  sleep until hearbeat timer elapses, 
        // or work arrives, and then iterate again.
        //
        long waitTime = anode.heartBeatInterval - (System.currentTimeMillis() - lastHeartbeat);
        synchronized(receivedAndDeletedBlockList) {
          if (waitTime > 0 && (shouldBackoff || pendingReceivedRequests == 0) && shouldRun) {
            try {
              receivedAndDeletedBlockList.wait(waitTime);
            } catch (InterruptedException ie) {
            }
          }
        } // synchronized
      } catch(RemoteException re) {
        anode.handleRegistrationError(re);
      } catch (IOException e) {
        LOG.warn(e);
      }
    } // while (shouldRun)
  } // offerService

  private void processFailedReceivedDeleted(Block[] failed,
      Block[] receivedAndDeletedBlockArray) {
    synchronized (receivedAndDeletedBlockList) {

      // Blocks that do not belong to an Inode are saved for
      // retransmisions
      for (int i = 0; i < failed.length; i++) {
        // Insert into retry list.
        LOG.info("Block " + failed[i] + " does not belong to any file "
            + "on namenode " + avatarnodeAddress + " Retry later.");
        receivedAndDeletedRetryBlockList.add(failed[i]);
        lastBlockReceivedFailed = anode.now();
      }
    }
  }

  /**
   * Process an array of datanode commands
   * 
   * @param cmds an array of datanode commands
   * @return true if further processing may be required or false otherwise.
   */
  private boolean processCommand(DatanodeCommand[] cmds) {
    if (cmds != null) {
      boolean isPrimary = this.servicePair.isPrimaryOfferService(this);
      for (DatanodeCommand cmd : cmds) {
        try {
          if (cmd.getAction() != DatanodeProtocol.DNA_REGISTER && !isPrimary) {
            if (isPrimaryService()) {
              // The failover has occured. Need to update the datanode knowledge
              this.servicePair.setPrimaryOfferService(this);
            } else {
              continue;
            }
          } else if (cmd.getAction() == DatanodeProtocol.DNA_REGISTER && 
                        !isPrimaryService()) {
            // Standby issued a DNA_REGISTER. Enter the mode of sending frequent
            // heartbeats
            LOG.info("Registering with Standby. Start frequent block reports");
            reportsSinceRegister = 0;
            if (isPrimary) {
              // This information is out of date
              this.servicePair.setPrimaryOfferService(null);
            }
          }
          if (processCommand(cmd) == false) {
            return false;
          }
        } catch (IOException ioe) {
          LOG.warn("Error processing datanode Command", ioe);
        }
      }
    }
    return true;
  }
  
  /**
   *
   * @param cmd
   * @return true if further processing may be required or false otherwise.
   * @throws IOException
   */
  private boolean processCommand(DatanodeCommand cmd) throws IOException {
    if (cmd == null)
      return true;
    final BlockCommand bcmd = cmd instanceof BlockCommand? (BlockCommand)cmd: null;

    switch(cmd.getAction()) {
    case DatanodeProtocol.DNA_TRANSFER:
      // Send a copy of a block to another datanode
      anode.transferBlocks(servicePair.namespaceId, bcmd.getBlocks(), bcmd.getTargets());
      myMetrics.blocksReplicated.inc(bcmd.getBlocks().length);
      break;
    case DatanodeProtocol.DNA_INVALIDATE:
      //
      // Some local block(s) are obsolete and can be
      // safely garbage-collected.
      //
      Block toDelete[] = bcmd.getBlocks();
      try {
        if (anode.blockScanner != null) {
          //TODO temporary
          anode.blockScanner.deleteBlocks(servicePair.namespaceId, toDelete);
        }
        removeReceivedBlocks(toDelete);
        data.invalidate(servicePair.namespaceId, toDelete);
      } catch(IOException e) {
        anode.checkDiskError();
        throw e;
      }
      myMetrics.blocksRemoved.inc(toDelete.length);
      break;
    case DatanodeProtocol.DNA_SHUTDOWN:
      // shut down the data node
      servicePair.shutdown();
      return false;
    case DatanodeProtocol.DNA_REGISTER:
      // namenode requested a registration - at start or if NN lost contact
      LOG.info("AvatarDatanodeCommand action: DNA_REGISTER");
      if (shouldRun) {
        servicePair.register(namenode, namenodeAddress);
        firstBlockReportSent = false;
        scheduleBlockReport(0);
      }
      break;
    case DatanodeProtocol.DNA_FINALIZE:
      anode.getStorage().finalizeUpgrade();
      break;
    case UpgradeCommand.UC_ACTION_START_UPGRADE:
      // start distributed upgrade here
      servicePair.processUpgradeCommand((UpgradeCommand)cmd);
      break;
    case DatanodeProtocol.DNA_RECOVERBLOCK:
      anode.recoverBlocks(servicePair.namespaceId, bcmd.getBlocks(), bcmd.getTargets());
      break;
    case DatanodeProtocols.DNA_BACKOFF:
      setBackoff(true);
      break;
    default:
      LOG.warn("Unknown DatanodeCommand action: " + cmd.getAction());
    }
    return true;
  }

  /**
   * This methods  arranges for the data node to send the block report at the next heartbeat.
   */
  public void scheduleBlockReport(long delay) {
    if (delay > 0) { // send BR after random delay
      lastBlockReport = System.currentTimeMillis()
                            - ( anode.blockReportInterval - R.nextInt((int)(delay)));
    } else { // send at next heartbeat
      lastBlockReport = lastHeartbeat - anode.blockReportInterval;
    }
    resetBlockReportTime = true; // reset future BRs for randomness
  }

  /**
   * Add a block to the pending received/deleted ACKs.
   * to inform the namenode that we have received a block.
   */
  void notifyNamenodeReceivedBlock(Block block, String delHint) {
    if (block==null) {
      throw new IllegalArgumentException("Block is null");
    }
    if (delHint != null && !delHint.isEmpty()) {
      block = new ReceivedBlockInfo(block, delHint);
    }
    if (isPrimaryService()) { // primary: notify NN immediately
      synchronized (receivedAndDeletedBlockList) {
        receivedAndDeletedBlockList.add(block);
        pendingReceivedRequests++;
        if (!shouldBackoff) {
          receivedAndDeletedBlockList.notifyAll();
        }
      }
    } else { // standby: delay notification by adding to retry list
      synchronized (receivedAndDeletedBlockList) {
        receivedAndDeletedRetryBlockList.add(block);
      }
    }
  }

  /**
   * Add a block to the pending received/deleted ACKs.
   * to inform the namenode that we have deleted a block.
   */
  void notifyNamenodeDeletedBlock(Block block) {
    if (block==null) {
      throw new IllegalArgumentException("Block is null");
    }
    // mark it as a deleted block
    DFSUtil.markAsDeleted(block);
    if (isPrimaryService()) { // primary: notify NN immediately
      synchronized (receivedAndDeletedBlockList) {
        receivedAndDeletedBlockList.add(block);
      }
    } else { //standby: delay notification by adding to the retry list
      synchronized (receivedAndDeletedBlockList) {
        receivedAndDeletedRetryBlockList.add(block);
      }
    }
  }

  /**
   * Remove blocks from blockReceived queues
   */
  void removeReceivedBlocks(Block[] removeList) {
    synchronized(receivedAndDeletedBlockList) {
      ReceivedBlockInfo block = new ReceivedBlockInfo();
      block.setDelHints(ReceivedBlockInfo.WILDCARD_HINT);
      for (Block bi : removeList) {
        block.set(bi.getBlockId(), bi.getNumBytes(), bi.getGenerationStamp());
        while (receivedAndDeletedBlockList.remove(block)) {
          LOG.info("Block deletion command deleted from receivedDeletedBlockList " +
                   bi);
        }
        while (receivedAndDeletedRetryBlockList.remove(block)) {
          LOG.info("Block deletion command deleted from receivedDeletedRetryBlockList " +
                   bi);
        }
      }
    }
  }
  
  void reportBadBlocks(LocatedBlock[] blocks) throws IOException {
    try {
      namenode.reportBadBlocks(blocks);  
    } catch (IOException e){
      /* One common reason is that NameNode could be in safe mode.
       * Should we keep on retrying in that case?
       */
      LOG.warn("Failed to report bad block to namenode : " +
               " Exception : " + StringUtils.stringifyException(e));
      throw e;
    }
  }
    
  /** Block synchronization */
  LocatedBlock syncBlock(
      Block block, List<BlockRecord> syncList,
      boolean closeFile, List<InterDatanodeProtocol> datanodeProxies
  ) 
  throws IOException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("block=" + block + ", (length=" + block.getNumBytes()
          + "), syncList=" + syncList + ", closeFile=" + closeFile);
    }

    //syncList.isEmpty() that all datanodes do not have the block
    //so the block can be deleted.
    if (syncList.isEmpty()) {
      namenode.commitBlockSynchronization(block, 0, 0, closeFile, true,
          DatanodeID.EMPTY_ARRAY);
      return null;
    }

    List<DatanodeID> successList = new ArrayList<DatanodeID>();

    long generationstamp = namenode.nextGenerationStamp(block, closeFile);
    Block newblock = new Block(block.getBlockId(), block.getNumBytes(), generationstamp);

    for(BlockRecord r : syncList) {
      try {
        r.datanode.updateBlock(servicePair.namespaceId, r.info.getBlock(), newblock, closeFile);
        successList.add(r.id);
      } catch (IOException e) {
        InterDatanodeProtocol.LOG.warn("Failed to updateBlock (newblock="
            + newblock + ", datanode=" + r.id + ")", e);
      }
    }

    anode.stopAllProxies(datanodeProxies);

    if (!successList.isEmpty()) {
      DatanodeID[] nlist = successList.toArray(new DatanodeID[successList.size()]);

      namenode.commitBlockSynchronization(block,
          newblock.getGenerationStamp(), newblock.getNumBytes(), closeFile, false,
          nlist);
      DatanodeInfo[] info = new DatanodeInfo[nlist.length];
      for (int i = 0; i < nlist.length; i++) {
        info[i] = new DatanodeInfo(nlist[i]);
      }
      return new LocatedBlock(newblock, info); // success
    }

    //failed
    StringBuilder b = new StringBuilder();
    for(BlockRecord r : syncList) {
      b.append("\n  " + r.id);
    }
    throw new IOException("Cannot recover " + block + ", none of these "
        + syncList.size() + " datanodes success {" + b + "\n}");
  }
}
