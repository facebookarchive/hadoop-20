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
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.FSConstants.DatanodeReportType;
import org.apache.hadoop.hdfs.util.InjectionEvent;
import org.apache.hadoop.util.Daemon;
import org.apache.hadoop.util.InjectionHandler;
import org.apache.hadoop.util.StringUtils;

public class StandbySafeMode extends NameNodeSafeModeInfo {
  
  protected static enum SafeModeState {
    BEFORE_FAILOVER ("BeforeFailover"),
    FAILOVER_IN_PROGRESS ("FailoverInProgress"),
    LEAVING_SAFEMODE ("LeavingSafeMode"),
    AFTER_FAILOVER ("AfterFailover");

    private final String name;

    private SafeModeState(String name) {
      this.name = name;
    }

    @Override
    public String toString() {
      return name;
    }
  }

  private final FSNamesystem namesystem;
  private final AvatarNode avatarnode;
  private final Set<DatanodeID> outStandingHeartbeats = Collections
      .synchronizedSet(new HashSet<DatanodeID>());
  private final Set<DatanodeID> outStandingReports = Collections
      .synchronizedSet(new HashSet<DatanodeID>());
  private final Set<DatanodeID> liveDatanodes = Collections
      .synchronizedSet(new HashSet<DatanodeID>());
  private volatile SafeModeState safeModeState;
  private final Log LOG = LogFactory.getLog(StandbySafeMode.class);
  private Daemon safeModeMonitor;
  private final float outStandingReportThreshold;
  
  // for fast failover, we do not wait until all datanodes report
  // primaryCleared, we only care about informing all datanodes
  // that failover is in progress
  private final boolean fastFailover;
  
  // we can manually trigger actions before the actual failover
  private volatile boolean prepareFailover = false;
  
  private long lastStatusReportTime;

  public StandbySafeMode(Configuration conf, FSNamesystem namesystem) {
    super(conf, namesystem);
    if (namesystem == null || conf == null)
      throw new IllegalArgumentException("Namesystem and conf cannot be null");
    this.namesystem = namesystem;
    this.avatarnode = (AvatarNode)namesystem.getNameNode();
    this.outStandingReportThreshold = conf.getFloat(
        "dfs.standbysafemode.outstanding.threshold", 1.0f);
    if (this.outStandingReportThreshold < 0
        || this.outStandingReportThreshold > 1) {
      throw new RuntimeException(
          "Invalid dfs.standbysafemode.outstanding.threshold : "
              + this.outStandingReportThreshold + " should be between [0, 1.0]");
    }
    this.fastFailover = conf.getBoolean("dfs.standbysafemode.fastfailover",
        false);
    
    LOG.info("Standby safemode: outstanding report threshold: "
        + outStandingReportThreshold);
    LOG.info("Standby safemode: fast failover: " + fastFailover);
    this.safeModeState = SafeModeState.BEFORE_FAILOVER;
  }

  /**
   * Processes a register from the datanode. First, we will
   * await a heartbeat, and later for a incremental block
   * report.
   *
   * @param node
   *          the datanode that has reported
   */
  protected void reportRegister(DatanodeID node) {
    if (node != null
        && shouldUpdateNodes()) {
      if (!liveDatanodes.contains(node)) {
        // A new node has checked in, we want to send a ClearPrimary command to
        // it as well.
        outStandingHeartbeats.add(node);
        liveDatanodes.add(node);
      }
    }
  }
  
  /**
   * Check if the outstanding datanode queues shoudl be updated.
   */
  private boolean shouldUpdateNodes() {
    // either failover is still in progress, or we are after
    // but the failover did not clean up the datanodes (fast option)
    return (safeModeState == SafeModeState.FAILOVER_IN_PROGRESS 
        || (fastFailover && safeModeState == SafeModeState.AFTER_FAILOVER));
  }

  /**
   * Processes a heartbeat from the datanode and determines whether we should
   * send a ClearPrimary command to it.
   *
   * @param node
   *          the datanode that has reported
   * @return whether or not we should send a ClearPrimary command to this
   *         datanode
   */
  protected boolean reportHeartBeat(DatanodeID node) {
    if (node != null && shouldUpdateNodes()) {
      reportRegister(node);
      synchronized(this) {
        if (outStandingHeartbeats.remove(node)) {
          outStandingReports.add(node);
          return true;
        }
      }
    }
    return false;
  }

  /**
   * Report that the given datanode has cleared the primary.
   * It is fully aware of the failover, and it has sent the 
   * incremental block report.
   * 
   * @param node
   *          the datanode that has reported
   */
  protected void reportPrimaryCleared(DatanodeID node) {
    if (node != null && shouldUpdateNodes()) {
      if (outStandingReports.remove(node)) {
        LOG.info("Failover: Outstanding reports: " + outStandingReports.size());
      }
    }
  }

  /**
   * Triggers failover processing for safe mode and blocks until we have left
   * safe mode.
   * 
   * @throws IOException
   */
  protected void triggerFailover() throws IOException {
    clearDataStructures();
    
    // stop sending PREPARE_FAILOVER command
    // we are performing failover now
    prepareFailover = false;
    
    for (DatanodeInfo node : namesystem.datanodeReport(DatanodeReportType.LIVE)) {
      liveDatanodes.add(node);
      outStandingHeartbeats.add(node);
    }
    InjectionHandler
        .processEvent(InjectionEvent.STANDBY_ENTER_SAFE_MODE);
    safeModeState = SafeModeState.FAILOVER_IN_PROGRESS;
    InjectionHandler.processEvent(InjectionEvent.STANDBY_FAILOVER_INPROGRESS);
    safeModeMonitor = new Daemon(new SafeModeMonitor(namesystem, this));
    safeModeMonitor.start();
    try {
      safeModeMonitor.join();
    } catch (InterruptedException ie) {
      throw new IOException("triggerSafeMode() interruped()");
    }
    if (safeModeState != SafeModeState.AFTER_FAILOVER) {
      throw new IOException("safeModeState is : " + safeModeState +
          " which does not indicate a successfull exit of safemode");
    }
  }

  private void clearDataStructures() {
    outStandingHeartbeats.clear();
    outStandingReports.clear();
    liveDatanodes.clear();
  }

  private float getDatanodeReportRatio() {
    int liveDatanodesSize = liveDatanodes.size();
    if (liveDatanodesSize != 0) {
      return ((liveDatanodesSize - (outStandingHeartbeats.size() +
              outStandingReports.size())) / (float) liveDatanodesSize);
    }
    return 1;
  }

  @Override
  public String getTurnOffTip() {
    try {
      if (!isOn() || safeModeState == SafeModeState.AFTER_FAILOVER) {
        return "Safe mode is OFF";
      }
      
      long safeBlocks = namesystem.getSafeBlocks();
      long totalBlocks = namesystem.getTotalBlocks();
      
      String reportingNodes = "??";
      try {
        reportingNodes = Integer.toString(namesystem.getReportingNodesUnsafe());
      } catch (Exception e) { /* ignore */ }
  
      String initReplicationQueues = namesystem.isPopulatingReplQueues() 
          ? " Replication queues have been initialized manually. "
          : "";
      
      String prepFailoverState = prepareFailover 
          ? " Processed prepare failover - standby will not checkpoint. "
          : "";
      
      String safeBlockRatioMsg = String.format(
        initReplicationQueues 
        + prepFailoverState
        + "The ratio of reported blocks %.8f has "
              + (!blocksSafe() ? "not " : "")
        + "reached the threshold %.8f. ", namesystem.getSafeBlockRatio(),
          threshold)
        + "Safe blocks = "
        + safeBlocks
        + ", Total blocks = "
        + totalBlocks
        + ", Remaining blocks = "
        + (totalBlocks - safeBlocks)
        + ". "
        + "Reporting nodes = " + reportingNodes + ". ";
  
      if (safeModeState == SafeModeState.BEFORE_FAILOVER) {
        return "This is the STANDBY AVATAR. Safe mode is ON. "
          + safeBlockRatioMsg;
      }
  
      boolean received = this.getDatanodeReportRatio() >=
          this.outStandingReportThreshold;
      
      String ff = fastFailover ? " (Fast failover)" : "";
      
      String datanodeReportMsg = "All datanode reports ratio "
          + getDatanodeReportRatio() + " have " + (!received ? "not " : "")
          + "reached threshold : " + this.outStandingReportThreshold
          + ", <a href=\"/outstandingnodes\"> Outstanding Heartbeats" + " : "
          + outStandingHeartbeats.size() + " Outstanding Reports : "
          + outStandingReports.size() + ff + "</a><br><br>";
      return safeBlockRatioMsg + datanodeReportMsg;
    } catch (Exception e) {
      LOG.warn("Exception when obtaining safemode status", e);
      return "Error when obtaining safemode status. Please refresh." ;
    }
  }

  @Override
  public boolean isManual() {
    return true;
  }

  @Override
  public boolean isOn() {
    return (safeModeState != SafeModeState.AFTER_FAILOVER);
  }
  
  /**
   * Initializes replication queues *without* leaving safemode.
   * This should only be used ONLY through dfsadmin command.
   */
  @Override
  public void initializeReplicationQueues() {
    // acquire writelock first to avoid deadlock
    namesystem.writeLock();
    // we can only initialize replication queues manually 
    // during failover
    try {
      synchronized (this) {
        if (safeModeState != SafeModeState.FAILOVER_IN_PROGRESS &&
            safeModeState != SafeModeState.BEFORE_FAILOVER) {
          throw new RuntimeException(
              "Cannot initialize replication queues since Standby is "
                  + "in state : " + safeModeState);
        }
        super.initializeReplQueues();
      }
    } finally {
      namesystem.writeUnlock();
    }
  }

  @Override
  public void leave(boolean checkForUpgrades) {
    namesystem.writeLock();
    if (safeModeState == SafeModeState.LEAVING_SAFEMODE) {
      // if the same thread is already trying to leave safemode, ignore
      // this request.
      namesystem.writeUnlock();
      return;
    }
    try {
      synchronized (this) {
        if (safeModeState != SafeModeState.FAILOVER_IN_PROGRESS) {
          throw new RuntimeException(
              "Cannot leave safe mode since Standby is in state : " + safeModeState);
        }

        safeModeState = SafeModeState.LEAVING_SAFEMODE;

        // Recount file counts and quota
        namesystem.recount();

        // These datanodes have not reported, we are not sure about their state
        // remove them.
        removeOutStandingDatanodes(fastFailover);
        if (avatarnode.enableTestFramework && 
            avatarnode.enableTestFrameworkFsck) {
          try {
            if (namesystem.isPopulatingReplQueues()) {
              LOG.warn("Failover: Test framework - fsck "
                  + "- queues already initialized");
              avatarnode.setFailoverFsck("Could not obtain fsck.");
            }
            super.initializeReplQueues();
            avatarnode.setFailoverFsck(avatarnode.runFailoverFsck());
          } catch (Exception e) {
            LOG.warn("Exception when running fsck after failover.", e);
            avatarnode
            .setFailoverFsck("Exception when running fsck after failover. "
                + StringUtils.stringifyException(e));
          }
        }

        super.startPostSafeModeProcessing();
        // We need to renew all leases, since client has been renewing leases only
        // on the primary.
        renewAllLeases();
        // if we are in fast failover mode, inform the namesystem to delay processing
        // over-replicated blocks
        delayOverreplicationMonitor();
        safeModeState = SafeModeState.AFTER_FAILOVER;
      }
    } finally {
      if (safeModeState == SafeModeState.LEAVING_SAFEMODE) {
        // We did not exit safemode successfully, change to FAILOVER_INPROGRESS,
        // so that we can probably retry leaving safemode.
        safeModeState = SafeModeState.FAILOVER_IN_PROGRESS;
      }
      namesystem.writeUnlock();
    }
  }
  
  private void delayOverreplicationMonitor() {
    if (fastFailover) {
      long now = AvatarNode.now();
      long delay = 2 * namesystem.getHeartbeatExpireInterval();
      namesystem.delayOverreplicationProcessing(now + delay);
      LOG.info("Failover: Delaying overreplication processing by: "
          + (delay / 1000) + " seconds");
    }
  }
  
  private void renewAllLeases() {
    LOG.info("Failover - renewing all leases");
    // be extra safe and synchronize on the lm
    synchronized (namesystem.leaseManager) {
      for (String holder : namesystem.leaseManager.getLeaseHolders()) {
        try {
          namesystem.leaseManager.renewLease(holder);
        } catch (Exception e) {
          LOG.error("Failover - failed to renew lease for " + holder, e);
        }
      }
    }
  }

  private void setDatanodeDead(DatanodeID node) throws IOException {
    DatanodeDescriptor ds = getDatanode(node);
    if (ds != null) {
      namesystem.setDatanodeDead(ds);
    }
  }
  
  /**
   * Get datanode descriptor from namesystem.
   * Return null for unregistered/dead/error nodes.
   */
  private DatanodeDescriptor getDatanode(DatanodeID node) {
    if (node == null) {
      return null;
    }
    DatanodeDescriptor ds = null;
    try {
      ds = namesystem.getDatanode(node);
    } catch (Exception e) {
      // probably dead on unregistered datanode
      LOG.warn("Failover - caught exception when getting datanode", e);
      return null;
    }
    return ds;
  }

  /**
   * This function as a whole need to be synchronized since it is invoked by
   * leave().
   */
  void removeOutStandingDatanodes(boolean logOutStandingOnly) {
    try {
      removeOutstandingDatanodesInternal(outStandingHeartbeats, logOutStandingOnly);
      removeOutstandingDatanodesInternal(outStandingReports, logOutStandingOnly);
    } catch (Exception e) {
      LOG.warn(
          "Failover - caught exception when removing outstanding datanodes", e);
    }
  }
  
  private void removeOutstandingDatanodesInternal(Set<DatanodeID> nodes,
      boolean logOutStandingOnly) throws IOException {
    synchronized (nodes) {
      for (DatanodeID node : nodes) {
        if (logOutStandingOnly) {
          LOG.info("Failover - outstanding node: " + node
              + " - node is not removed (fast failover)");
        } else {
          try {
            LOG.info("Failover - removing outstanding node: " + node);
            namesystem.removeDatanode(node);
            setDatanodeDead(node);
          } catch (Exception e) {
            LOG.warn(
                "Failover - caught exception when removing outstanding datanode "
                    + node, e);
          }
        }
      }
      if (!logOutStandingOnly) {
        nodes.clear();
      }
    }
  }

  @Override
  public void setManual() {
    // isManual is always true.
  }

  @Override
  public void shutdown() {
    if (safeModeMonitor != null) {
      safeModeMonitor.interrupt();
    }
  }

  private boolean blocksSafe() {
    return namesystem.getSafeBlockRatio() >= threshold;
  }

  private void checkDatanodes() {
    try {
      checkDatanodesInternal(outStandingHeartbeats);
      checkDatanodesInternal(outStandingReports);
    } catch (Exception e) {
      // for sanity catch exception here
      LOG.warn("Failover - caught exception when checking datanodes", e);
    }
  }
  
  private void checkDatanodesInternal(Set<DatanodeID> nodes) {
    synchronized (nodes) {
      for (Iterator<DatanodeID> it = nodes.iterator(); it
          .hasNext();) {
        DatanodeID node = null;
        try {
          node = it.next();
          DatanodeDescriptor dn = getDatanode(node);
          if (dn == null || namesystem.isDatanodeDead(dn)) {
            LOG.info("Failover - removing dead node from safemode:" + node);
            liveDatanodes.remove(dn);
            it.remove();
          }
        } catch (Exception e) {
          LOG.warn("Failover - caught exception when checking datanode " + node, e);
        }
      }
    }  
  }

  /**
   * Checks if the datanode reports have been received
   * @param checkDatanodes whether it should actively remove dead datanodes
   * @return true if the datanode reports have been received
   */
  private synchronized boolean datanodeReportsReceived(boolean checkDatanodes) {
    try {
      boolean received = this.getDatanodeReportRatio() >=
        this.outStandingReportThreshold;
      if (!received && checkDatanodes) {
        checkDatanodes();
        return this.getDatanodeReportRatio() >= this.outStandingReportThreshold;
      }
      return received;
    } catch (Exception e) {
      LOG.warn("Failover - caught exception when checking reports", e);
      return false;
    }
  }

  @Override
  public boolean canLeave() {
    try {
      if(FSNamesystem.now() - lastStatusReportTime > 1000) {
        lastStatusReportTime = FSNamesystem.now();
        LOG.info(this.getTurnOffTip());
      }
      if (safeModeState == SafeModeState.AFTER_FAILOVER ||
          safeModeState == SafeModeState.LEAVING_SAFEMODE) {
        // Already left safemode or in the process of.
        return true;
      }
      return (safeModeState == SafeModeState.FAILOVER_IN_PROGRESS
          && blocksSafe() && datanodeReportsReceived(true));
    } catch (Exception e) {
      LOG.warn("Failover - caught exception when checking safemode", e);
      return false;
    }
  }

  @Override
  public void checkMode() {
    if (canLeave()
        || InjectionHandler
            .falseCondition(InjectionEvent.STANDBY_SAFEMODE_CHECKMODE)) {
      leave(false);
    }
  }

  public String toString() {
    return this.getTurnOffTip();
  }

  public boolean failoverInProgress() {
    return (safeModeState == SafeModeState.FAILOVER_IN_PROGRESS);
  }

  public Set<DatanodeID> getOutStandingHeartbeats() {
    return outStandingHeartbeats;
  }

  public Set<DatanodeID> getOutStandingReports() {
    return outStandingReports;
  }

  protected void setSafeModeStateForTesting(SafeModeState state) {
    safeModeState = state;
  }

  protected void addLiveNodeForTesting(DatanodeID node) {
    this.liveDatanodes.add(node);
    this.outStandingHeartbeats.add(node);
  }
  
  @Override
  public boolean shouldProcessRBWReports() {
    // Primary namenode always processed RBW reports.
    return safeModeState != SafeModeState.BEFORE_FAILOVER;
  }
  
  /**
   * Indicate whether we should prepare for failover.
   */
  void setPrepareFailover(boolean prepareFailover) {
    this.prepareFailover = prepareFailover;
  }
  
  boolean getPrepareFailover() {
    return prepareFailover;
  }
}
