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
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.protocol.AvatarProtocol;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.BlockListAsLongs;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.server.datanode.AvatarDataNode.ServicePair;
import org.apache.hadoop.hdfs.server.datanode.DataNode.BlockRecoveryTimeoutException;
import org.apache.hadoop.hdfs.server.datanode.DataNode.KeepAliveHeartbeater;
import org.apache.hadoop.hdfs.server.datanode.metrics.DataNodeMetrics;
import org.apache.hadoop.hdfs.server.protocol.BlockAlreadyCommittedException;
import org.apache.hadoop.hdfs.server.protocol.BlockCommand;
import org.apache.hadoop.hdfs.server.protocol.BlockReport;
import org.apache.hadoop.hdfs.server.protocol.DatanodeCommand;
import org.apache.hadoop.hdfs.server.protocol.DatanodeProtocol;
import org.apache.hadoop.hdfs.server.protocol.DatanodeRegistration;
import org.apache.hadoop.hdfs.server.protocol.IncrementalBlockReport;
import org.apache.hadoop.hdfs.server.protocol.InterDatanodeProtocol;
import org.apache.hadoop.hdfs.server.protocol.ReceivedBlockInfo;
import org.apache.hadoop.hdfs.server.protocol.UpgradeCommand;
import org.apache.hadoop.hdfs.util.InjectionEvent;
import org.apache.hadoop.hdfs.util.LightWeightBitSet;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.util.InjectionHandler;
import org.apache.hadoop.util.StringUtils;
import org.apache.zookeeper.data.Stat;

public class OfferService implements Runnable {

  public static final Log LOG = LogFactory.getLog(OfferService.class.getName());

  long lastHeartbeat = 0;
  volatile boolean shouldRun = true;
  long lastBlockReport = 0;
  long lastDeletedReport = 0;
  boolean resetBlockReportTime = true;
  long blockReceivedRetryInterval;
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
  private LinkedList<Block> receivedAndDeletedBlockList
          = new LinkedList<Block>();

  private int pendingReceivedRequests = 0;

  private long lastBlockReceivedFailed = 0;
  private ServicePair servicePair;
   
  private boolean shouldBackoff = false;
  private boolean firstBlockReportSent = false;

  // Used by the NN to force an incremental block report and not wait for any
  // interval.
  private boolean forceIncrementalReport = false;
  
  // after clear primary is called, we will no longer delay any
  // incremental block reports
  private boolean donotDelayIncrementalBlockReports = false;
  
  // indicate that this offer service received CLEAR PRIMARY
  // command, which means failover is in progress
  private boolean clearPrimaryCommandProcessed = false;

  private final long fullBlockReportDelay;

  /**
   * Offer service to the specified namenode
   */
  public OfferService(AvatarDataNode anode, ServicePair servicePair,
		              DatanodeProtocol namenode, InetSocketAddress namenodeAddress,
		              AvatarProtocol avatarnode, InetSocketAddress avatarnodeAddress) {
    this.anode = anode;
    this.servicePair = servicePair;
    this.namenode = namenode;
    this.avatarnode = avatarnode;
    this.namenodeAddress = namenodeAddress;
    this.avatarnodeAddress = avatarnodeAddress;

    nsRegistration = servicePair.nsRegistration;
    data = anode.data;
    myMetrics = anode.myMetrics;
    scheduleBlockReport(anode.initialBlockReportDelay);
    backlogSize = anode.getConf().getInt("dfs.datanode.blockreceived.backlog", 10000);
    fullBlockReportDelay = anode.getConf().getInt(
        "dfs.datanode.fullblockreport.delay", 5 * 60 * 1000);
    blockReceivedRetryInterval = anode.getConf().getInt(
        "dfs.datanode.blockreceived.retry.internval", 10000);
  }

  public void stop() {
    shouldRun = false;
    if (keepAliveRun != null) {
      keepAliveRun.cancel(true);
    }
    if (keepAliveSender != null) {
      keepAliveSender.shutdownNow();
    }
  }
  
  private boolean isPrimaryServiceCached(){
    return this.servicePair.isPrimaryOfferService(this);
  }

  /**
   * Checks whether we are the primary service.
   * 
   * @return whether we are the primary service
   */
  boolean isPrimaryService() throws InterruptedException {
    return servicePair.isPrimary(namenodeAddress);
  }

  public void run() {
    if (!shouldRun()) 
      return;
    KeepAliveHeartbeater keepAliveTask = 
        new KeepAliveHeartbeater(namenode, nsRegistration, this.servicePair);
    keepAliveSender = Executors.newSingleThreadScheduledExecutor();
    keepAliveRun = keepAliveSender.scheduleAtFixedRate(keepAliveTask, 0,
                                                       anode.heartBeatInterval,
                                                       TimeUnit.MILLISECONDS);
    while (shouldRun()) {
      try {
        if (isPrimaryService()) {
          servicePair.setPrimaryOfferService(this);
        }
        offerService();
      } catch (Exception e) {
        LOG.error("OfferService encountered exception", e);
      }
    }
    stop();
  }

  private void setBackoff(boolean value) {
    synchronized (receivedAndDeletedBlockList) {
      this.shouldBackoff = value;
    }
  }

  public boolean shouldRun() {
    return shouldRun && anode.shouldRun;
  }

  /**
   * Sends an incremental block report to the Namenode.
   * 
   * @param startTime
   *          the time when we started processing the last heartbeat
   * @throws Exception
   *           if there is an error in reporting blocks to the NameNode
   */
  private void sendIncrementalBlockReport(long startTime) throws Exception {

    // check if there are newly received blocks
    Block[] receivedAndDeletedBlockArray = null;
    int numBlocksReceivedAndDeleted = 0;
    int currentPendingRequests = 0;

    synchronized (receivedAndDeletedBlockList) {
      
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
    // process received + deleted
    // if exception is thrown, add all blocks to the retry list
    if (receivedAndDeletedBlockArray != null) {            
      long[] failed = null;
      try {
        IncrementalBlockReport ibr = new IncrementalBlockReport(receivedAndDeletedBlockArray);

        long rpcStartTime = 0;
        if (LOG.isDebugEnabled()) {
          rpcStartTime = System.nanoTime();
          LOG.debug("sending blockReceivedAndDeletedNew "
                    + receivedAndDeletedBlockArray.length +
                    " blocks to " + namenodeAddress);
        }
        failed = avatarnode.blockReceivedAndDeletedNew(nsRegistration, ibr);
        if (LOG.isDebugEnabled()) {
          LOG.debug("finished blockReceivedAndDeletedNew " +
                    "to " + namenodeAddress +
                    " time: " + (System.nanoTime() - rpcStartTime) + " ns");
        }

        boolean isPrimaryCached = isPrimaryServiceCached();
        // if we talk to primary failed must be null
        // if we talk to standby failed shouldn't be null
        if(isPrimaryCached && failed!=null){
          //this should never happen
          //the primary can't switch to standby
          throw new IOException("Primary started acting as standby");
        } else if (!isPrimaryCached && failed == null) {
          String msg = "Received null response from standby for incremental"
              + " block report. ";
          if (clearPrimaryCommandProcessed) {
            LOG.info(msg + "Failover is in progress"
                + " - will not clear primary again");
          } else {
            LOG.info(msg + "Standby is acting as primary. Clearing primary");
            // failover - we need to refresh our knowledge
            this.clearPrimary();
          }
        }
      } catch (Exception e) {
        processFailedBlocks(
            receivedAndDeletedBlockArray, currentPendingRequests);
        throw e;
      }
      if(failed != null && failed.length != 0){
        processFailedReceivedDeleted(failed, receivedAndDeletedBlockArray);
      }
    }
  }

  public void offerService() throws Exception {
    InjectionHandler.processEvent(InjectionEvent.OFFERSERVICE_START,
        anode.getPort(), this.avatarnodeAddress);

    LOG.info("using BLOCKREPORT_INTERVAL of " + anode.blockReportInterval + "msec" + 
       " Initial delay: " + anode.initialBlockReportDelay + "msec for " + namenodeAddress);
    LOG.info("using DELETEREPORT_INTERVAL of " + anode.deletedReportInterval
        + "msec for " + namenodeAddress);
    LOG.info("using HEARTBEAT_EXPIRE_INTERVAL of " + anode.heartbeatExpireInterval 
        + "msec for " + namenodeAddress);

    //
    // Now loop for a long time....
    //
    while (shouldRun()) {
      try {

        // If we are falling behind in confirming blockReceived to NN, then
        // we clear the backlog and schedule a block report. This scenario
        // is likely to arise if one of the NN is down for an extended period.
        long maxSize = Math.max(backlogSize, anode.data.size(this.servicePair.namespaceId));
        if (receivedAndDeletedBlockList.size() > maxSize) {
          LOG.warn("The backlog of blocks to be confirmed has exceeded the " +
                   " maximum of " + maxSize +
                   " records. Scheduling a full block report for " + namenodeAddress);
          scheduleBlockReport(0);
        }

        long startTime = AvatarDataNode.now();

        //
        // Every so often, send heartbeat or block-report
        //
        if ((startTime - lastHeartbeat > anode.heartBeatInterval)
            || InjectionHandler.falseCondition(InjectionEvent.OFFERSERVICE_SCHEDULE_HEARTBEAT)) {
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
                                                         data.getNSUsed(
                                                           this.servicePair.namespaceId),
                                                         anode.xmitsInProgress.get(),
                                                         anode.getXceiverCount());
          long cmdTime = AvatarDataNode.now();
          this.servicePair.lastBeingAlive = cmdTime;
          LOG.debug("Sent heartbeat at " + this.servicePair.lastBeingAlive + " to " + namenodeAddress);
          myMetrics.heartbeats.inc(AvatarDataNode.now() - startTime);
          if (!processCommand(cmds, cmdTime))
            continue;
        }

        // send forced incremental report
        if (this.forceIncrementalReport) {
          LOG.info("Forcing incremental block report for " + namenodeAddress);
          // We want to send a RBW report when a block report has been
          // forced. RBW report might take some time since it scans the
          // disk.
          LOG.info("Generating blocks being written report for " + namenodeAddress);
          anode.sendBlocksBeingWrittenReport(namenode,
              servicePair.namespaceId, nsRegistration);
          LOG.info("Sending incremental block report for " + namenodeAddress);
          sendIncrementalBlockReport(startTime);
          avatarnode.primaryCleared(nsRegistration);
          this.forceIncrementalReport = false;
        }

        // check if there are newly received blocks (pendingReceivedRequeste > 0
        // or if the deletedReportInterval passed.
        // send regular incremental report
        if ((firstBlockReportSent && !shouldBackoff 
            && shouldSendIncrementalReport(startTime))) {                    
          sendIncrementalBlockReport(startTime);
        }

        // send block report
        if (startTime - lastBlockReport > anode.blockReportInterval) {
          if (shouldBackoff && 
              !InjectionHandler.falseCondition(InjectionEvent.OFFERSERVICE_SCHEDULE_BR)) {
            scheduleBlockReport(fullBlockReportDelay);
            LOG.info("Backoff blockreport. Will be sent in " +
              (lastBlockReport + anode.blockReportInterval - startTime) + "ms for " 
                + namenodeAddress);
          } else {
            //
            // Send latest blockinfo report if timer has expired.
            // Get back a list of local block(s) that are obsolete
            // and can be safely GC'ed.
            //
            long brStartTime = AvatarDataNode.now();
            // Clear incremental list before full block report. We need to do
            // this before we compute the entire block report. We need to also
            // capture a snapshot of the list if the full block report gets a
            // BACKOFF.
            List <Block> tempRetryList;
            int tempPendingReceivedRequests;
            synchronized (receivedAndDeletedBlockList) {
              tempRetryList = receivedAndDeletedBlockList;
              tempPendingReceivedRequests = pendingReceivedRequests;
              receivedAndDeletedBlockList = new LinkedList<Block>();
              pendingReceivedRequests = 0;
            }
            LOG.info("Generating block report for " + namenodeAddress);
            Block[] bReport = data.getBlockReport(servicePair.namespaceId);
            DatanodeCommand cmd = avatarnode.blockReportNew(nsRegistration,
                    new BlockReport(BlockListAsLongs.convertToArrayLongs(bReport)));
            if (cmd != null && 
                cmd.getAction() == DatanodeProtocols.DNA_BACKOFF) {
              // We have cleared the retry list, but the block report was not
              // processed due to BACKOFF, add the retry blocks back.
              processFailedBlocks(tempRetryList, tempPendingReceivedRequests);
  
              // The Standby is catching up and we need to reschedule
              scheduleBlockReport(fullBlockReportDelay);
              continue;
            }
  
            firstBlockReportSent = true;
            long brTime = AvatarDataNode.now() - brStartTime;
            myMetrics.blockReports.inc(brTime);
            LOG.info("BlockReport of " + bReport.length +
                " blocks got processed in " + brTime + " msecs on " +
                namenodeAddress);
            if (resetBlockReportTime) {
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
              lastBlockReport += (AvatarDataNode.now() - lastBlockReport) / 
                                 anode.blockReportInterval * anode.blockReportInterval;
            }
            if (cmd != null) {
              processCommand(new DatanodeCommand[] { cmd }, brStartTime);
            }
          }
        }

        // start block scanner is moved to the Dataode.run()

        //
        // There is no work to do;  sleep until hearbeat timer elapses, 
        // or work arrives, and then iterate again.
        //
        long waitTime = anode.heartBeatInterval - (System.currentTimeMillis() - lastHeartbeat);
        synchronized(receivedAndDeletedBlockList) {
          if (waitTime > 0 && (shouldBackoff || pendingReceivedRequests == 0) && shouldRun()) {
            try {
              receivedAndDeletedBlockList.wait(waitTime);
            } catch (InterruptedException ie) {
              throw ie;
            }
          }
        } // synchronized
      } catch(RemoteException re) {
        this.servicePair.handleRegistrationError(re, namenodeAddress);
      } catch (IOException e) {
        LOG.warn(e);
      }
    } // while (shouldRun)
  } // offerService

  /**
   * Checks if an incremental block report should be sent.
   * 
   * @param startTime
   * @return true if the report should be sent
   */
  private boolean shouldSendIncrementalReport(long startTime){
    boolean isPrimary = isPrimaryServiceCached() || 
        donotDelayIncrementalBlockReports;
    boolean deleteIntervalTrigger = 
        (startTime - lastDeletedReport > anode.deletedReportInterval);
    
    // by default the report should be sent if there are any received
    // acks, or the deleteInterval has passed
    boolean sendReportDefault = pendingReceivedRequests > 0
        || deleteIntervalTrigger;
    
    if(isPrimary){
      // if talking to primary, send the report with the default
      // conditions
      return sendReportDefault;
    } else {
      // if talking to standby. send the report ONLY when the 
      // retry interval has passed in addition to the default
      // condidtions
      boolean sendIfStandby = 
          (lastBlockReceivedFailed + blockReceivedRetryInterval < startTime)
          && sendReportDefault;
      return sendIfStandby;
    }
  }
 
  private void processFailedBlocks(List <Block> failed, 
      int failedPendingRequests) {
    processFailedBlocks(failed.toArray(new Block[failed.size()]),
        failedPendingRequests);
  }

  /**
   * Adds blocks of incremental block report back to the 
   * receivedAndDeletedBlockList, when handling an exception
   * 
   * @param failed - list of blocks
   * @param failedPendingRequests - how many of the blocks are received acks.
   */
  private void processFailedBlocks(Block []failed, 
      int failedPendingRequests) {
    synchronized (receivedAndDeletedBlockList) {
      // We are adding to the front of a linked list and hence to preserve
      // order we should add the blocks in the reverse order.
      for (int i = failed.length - 1; i >= 0; i--) {
        receivedAndDeletedBlockList.add(0, failed[i]);
      }
      pendingReceivedRequests += failedPendingRequests;
    }
  }
  
  /**
   * Adds blocks of incremental block report back to the 
   * receivedAndDeletedBlockList when the blocks are to be 
   * retried later (when sending to standby)
   * 
   * @param failed
   */
  private void processFailedReceivedDeleted(long[] failedMap, Block[] sent) {
    synchronized (receivedAndDeletedBlockList) {
      // Blocks that do not belong to an Inode are saved for
      // retransmisions
      for (int i = sent.length - 1 ; i >= 0; i--) {
        if(!LightWeightBitSet.get(failedMap, i)){
          continue;
        }
        // Insert into retry list.
        LOG.info("Block " + sent[i] + " does not belong to any file "
            + "on namenode " + avatarnodeAddress + " Retry later.");
        receivedAndDeletedBlockList.add(0, sent[i]);
        if (!DFSUtil.isDeleted(sent[i])) {
          pendingReceivedRequests++;
        }
      }
      lastBlockReceivedFailed = AvatarDataNode.now();
    }
  }

  private static int[] validStandbyCommands = { 
      DatanodeProtocol.DNA_REGISTER,
      DatanodeProtocols.DNA_CLEARPRIMARY, 
      DatanodeProtocols.DNA_BACKOFF,
      DatanodeProtocols.DNA_RETRY, 
      DatanodeProtocols.DNA_PREPAREFAILOVER
  };

  private static boolean isValidStandbyCommand(DatanodeCommand cmd) {
    for (int validCommand : validStandbyCommands) {
      if (cmd.getAction() == validCommand) {
        return true;
      }
    }
    return false;
  }

  /**
   * Determines whether a failover has happened and accordingly takes the
   * appropriate action.
   * @return whether or not failover happened
   */
  private boolean checkFailover() throws InterruptedException {
    boolean isPrimary = isPrimaryServiceCached();
    if (!isPrimary && isPrimaryService()) {
      this.servicePair.setPrimaryOfferService(this);
      return true;
    }
    return false;
  }

  /**
   * Process an array of datanode commands. This function has logic to check for
   * failover. Any commands should be processed using this function as an
   * entry point.
   * 
   * @param cmds an array of datanode commands
   * @return true if further processing may be required or false otherwise.
   */
  private boolean processCommand(DatanodeCommand[] cmds, long processStartTime)
      throws InterruptedException {
    if (cmds != null) {
      // at each heartbeat the standby offer service will talk to ZK!
      boolean switchedFromStandbyToPrimary = checkFailover();
      for (DatanodeCommand cmd : cmds) {
        try {
          // The datanode has received a register command after the failover, this
          // means that the offerservice thread for the datanode was down for a
          // while and it most probably did not clean up its deletion queue, hence
          // force a cleanup.
          if (switchedFromStandbyToPrimary
              && cmd.getAction() == DatanodeProtocol.DNA_REGISTER) {
            this.clearPrimary();
          }
          
          // The standby service thread is allowed to process only a small set
          // of valid commands.
          if (!isPrimaryServiceCached() && !isValidStandbyCommand(cmd)) {
            LOG.warn("Received an invalid command " + cmd.getAction()
                + " from standby " + this.namenodeAddress);
            continue;
          } 
          if (processCommand(cmd, processStartTime) == false) {
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
   * Process a single command sent by namenode. This function does NOT
   * check for failover and whether the command is a valid primary/standby command.
   * It should only be called from processCommand(DatanodeCommand[]), which has that
   * logic.
   * 
   * @param cmd
   * @return true if further processing may be required or false otherwise.
   * @throws IOException
   */
  private boolean processCommand(DatanodeCommand cmd, long processStartTime)
      throws IOException, InterruptedException {
    if (cmd == null)
      return true;
    final BlockCommand bcmd = cmd instanceof BlockCommand? (BlockCommand)cmd: null;

    boolean retValue = true;
    long startTime = System.currentTimeMillis();

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
        servicePair.removeReceivedBlocks(toDelete);
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
      retValue = false;
      break;
    case DatanodeProtocol.DNA_REGISTER:
      // namenode requested a registration - at start or if NN lost contact
      LOG.info("AvatarDatanodeCommand action: DNA_REGISTER");
      if (shouldRun()) {
        try {
          InjectionHandler
              .processEventIO(InjectionEvent.OFFERSERVICE_BEFORE_REGISTRATION);
          servicePair.register(namenode, namenodeAddress, true);
          firstBlockReportSent = false;
          cancelPrepareFailover();
          scheduleBlockReport(0);
        } catch (IOException e) {
          LOG.warn("Registration failed, will restart offerservice threads..",
              e);
          servicePair.stopService1();
          servicePair.doneRegister1 = false;
          servicePair.stopService2();
          servicePair.doneRegister2 = false;
          throw e;
        }
      }
      break;
    case DatanodeProtocol.DNA_FINALIZE:
      boolean shouldProcessUpgradeCommand = servicePair
          .shouldProcessFinalizeCommand(this);
      InjectionHandler.processEvent(InjectionEvent.OFFERSERVICE_DNAFINALIZE,
          shouldProcessUpgradeCommand, isPrimaryServiceCached());
      if (!shouldProcessUpgradeCommand) {
        LOG.warn("Received finalize upgrade command from: "
            + namenodeAddress
            + ", but the registration "
            + "version of data-node and name-node were not matching. Skipping command.");
      } else {
        LOG.info("Finalize upgrade command received from: " + namenodeAddress);
        anode.getStorage().finalizedUpgrade(servicePair.namespaceId);
      }
      break;
    case UpgradeCommand.UC_ACTION_START_UPGRADE:
      // start distributed upgrade here
      servicePair.processUpgradeCommand((UpgradeCommand)cmd);
      break;
    case DatanodeProtocol.DNA_RECOVERBLOCK:
      anode.recoverBlocks(servicePair.namespaceId, bcmd.getBlocks(),
          bcmd.getTargets(), processStartTime);
      break;
    case DatanodeProtocols.DNA_BACKOFF:
      // We can get a BACKOFF request as a response to a full block report.
      setBackoff(true);
      break;
    case DatanodeProtocols.DNA_CLEARPRIMARY:
      LOG.info("CLEAR PRIMARY requested by : " + this.avatarnodeAddress);
      InjectionHandler
          .processEventIO(InjectionEvent.OFFERSERVICE_BEFORE_CLEARPRIMARY);
      retValue = clearPrimary();
      this.clearPrimaryCommandProcessed = true;
      break;
    case DatanodeProtocols.DNA_PREPAREFAILOVER:
      prepareFailover();
      break;
    case DatanodeProtocols.DNA_RETRY:
      // We will get a RETRY request as a response to only a full block report.
      LOG.info(this.avatarnodeAddress + " has requested the retry of : "
          + bcmd.getBlocks().length + " blocks in response to a full block"
          + " report");
      // Retry the blocks that failed on the Standby.
      processFailedBlocks(bcmd.getBlocks(), bcmd.getBlocks().length);
      break;
    default:
      LOG.warn("Unknown DatanodeCommand action: " + cmd.getAction());
    }

    long endTime = System.currentTimeMillis();
    if (endTime - startTime > 1000) {
      LOG.info("processCommand() took " + (endTime - startTime)
               + " msec to process command " + cmd.getAction()
               + " from " + namenodeAddress);
    } else if (LOG.isDebugEnabled()) {
      LOG.debug("processCommand() took " + (endTime - startTime)
                + " msec to process command " + cmd.getAction()
                + " from " + namenodeAddress);
    }
    return retValue;
  }
  
  /**
   * Take actions in preparation for failover.
   */
  private void prepareFailover() {
    LOG.info("PREPARE FAILOVER requested by : " + this.avatarnodeAddress);
    // we should start sending incremental block reports and block
    // reports normally
    setBackoff(false);
    this.donotDelayIncrementalBlockReports = true;
    InjectionHandler.processEvent(InjectionEvent.OFFERSERVICE_PREPARE_FAILOVER,
        nsRegistration.toString());
  }

  /**
   * Cancel prepare failover behavior if any. This should be called when we
   * re-register, which means that somethign went wrong, and we are reconnecting
   * to a node within the same offerservice.
   */
  private void cancelPrepareFailover() {
    this.donotDelayIncrementalBlockReports = false;
  }

  /**
   * This is clears up the thread heartbeating to the primary Avatar, by
   * restarting it. This makes sure all commands from the primary have been
   * processed by the datanode. This method is used during failover.
   */
  private boolean clearPrimary() throws InterruptedException {
    try {
      if (!isPrimaryServiceCached()) {
        InetSocketAddress addr1 = servicePair.avatarAddr1;
        InetSocketAddress addr2 = servicePair.avatarAddr2;
        if (avatarnodeAddress.equals(addr2)) {
          LOG.info("Restarting service for AvatarNode : " + addr1);
          servicePair.restartService1();
        } else if (avatarnodeAddress.equals(addr1)) {
          LOG.info("Restarting service for AvatarNode : " + addr2);
          servicePair.restartService2();
        } else {
          throw new IOException("Address : " + avatarnodeAddress
              + " does not match any avatar address");
        }
        LOG.info("Finished Processing CLEAR PRIMARY requested by : "
            + this.avatarnodeAddress);
        this.forceIncrementalReport = true;
        this.donotDelayIncrementalBlockReports = true;
      }
      InjectionHandler.processEvent(InjectionEvent.OFFERSERVICE_CLEAR_PRIMARY);
    } catch (IOException e) {
      LOG.error("Exception processing CLEAR PRIMARY", e);
      return false;
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
   * Only used for testing
   */
  public void scheduleBlockReceivedAndDeleted(long delay) {
    if (delay > 0) {
      lastDeletedReport = System.currentTimeMillis()
          - anode.deletedReportInterval + delay;
    } else {
      lastDeletedReport = 0;
    }
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
    synchronized (receivedAndDeletedBlockList) {
      receivedAndDeletedBlockList.add(block);
      pendingReceivedRequests++;
      if (!shouldBackoff) {
        receivedAndDeletedBlockList.notifyAll();
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
    synchronized (receivedAndDeletedBlockList) {
      receivedAndDeletedBlockList.add(block);
    }
  }

  /**
   * Remove blocks from blockReceived queues
   */
  void removeReceivedBlocks(Block[] removeList) {
    long start = AvatarDataNode.now();
    synchronized(receivedAndDeletedBlockList) {
      ReceivedBlockInfo block = new ReceivedBlockInfo();
      block.setDelHints(ReceivedBlockInfo.WILDCARD_HINT);
      for (Block bi : removeList) {
        block.set(bi.getBlockId(), bi.getNumBytes(), bi.getGenerationStamp());
        while (receivedAndDeletedBlockList.remove(block)) {
          LOG.info("Block deletion command deleted from receivedDeletedBlockList " +
                   bi);
        }
      }
    }
    long stop = AvatarDataNode.now();
    LOG.info("Pruning blocks from the received list took " + (stop - start)
        + "ms for: " + removeList.length + "blocks, queue length: "
        + receivedAndDeletedBlockList.size());
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
      boolean closeFile, List<InterDatanodeProtocol> datanodeProxies,
      long deadline) 
  throws IOException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("block=" + block + ", (length=" + block.getNumBytes()
          + "), syncList=" + syncList + ", closeFile=" + closeFile);
    }

    //syncList.isEmpty() that all datanodes do not have the block
    //so the block can be deleted.
    if (syncList.isEmpty()) {
      DataNode.throwIfAfterTime(deadline);
      namenode.commitBlockSynchronization(block, 0, 0, closeFile, true,
          DatanodeID.EMPTY_ARRAY);
      return null;
    }

    List<DatanodeID> successList = new ArrayList<DatanodeID>();

    DataNode.throwIfAfterTime(deadline);
    long generationstamp = -1;
    try {
      generationstamp = namenode.nextGenerationStamp(block, closeFile);
    } catch (RemoteException e) {
      if (e.unwrapRemoteException() instanceof BlockAlreadyCommittedException) {
        throw new BlockAlreadyCommittedException(e);
      } else {
        throw e;
      }
    }

    Block newblock = new Block(block.getBlockId(), block.getNumBytes(), generationstamp);

    for(BlockRecord r : syncList) {
      try {
        DataNode.throwIfAfterTime(deadline);
        r.datanode.updateBlock(servicePair.namespaceId, r.info.getBlock(), newblock, closeFile);
        successList.add(r.id);
      } catch (BlockRecoveryTimeoutException e) {
        throw e;
      } catch (IOException e) {
        InterDatanodeProtocol.LOG.warn("Failed to updateBlock (newblock="
            + newblock + ", datanode=" + r.id + ")", e);
      }
    }

    anode.stopAllProxies(datanodeProxies);

    if (!successList.isEmpty()) {
      DatanodeID[] nlist = successList.toArray(new DatanodeID[successList.size()]);

      DataNode.throwIfAfterTime(deadline);
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
