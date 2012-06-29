package org.apache.hadoop.hdfs.server.namenode;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.FSConstants.DatanodeReportType;
import org.apache.hadoop.util.Daemon;
import org.apache.hadoop.util.StringUtils;

public class StandbySafeMode extends NameNodeSafeModeInfo {
  
  protected static enum SafeModeState {
    BEFORE_FAILOVER ("BeforeFailover"),
    FAILOVER_IN_PROGRESS ("FailoverInProgress"),
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
  private Set<DatanodeID> liveDatanodes = Collections
      .synchronizedSet(new HashSet<DatanodeID>());
  private volatile SafeModeState safeModeState;
  private final Log LOG = LogFactory.getLog(StandbySafeMode.class);
  private Daemon safeModeMonitor;

  public StandbySafeMode(Configuration conf, FSNamesystem namesystem) {
    super(conf, namesystem);
    this.namesystem = namesystem;
    this.avatarnode = (AvatarNode)namesystem.getNameNode();
    safeModeState = SafeModeState.BEFORE_FAILOVER;
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
    if (safeModeState == SafeModeState.FAILOVER_IN_PROGRESS) {
      if (!liveDatanodes.contains(node)) {
        // A new node has checked in, we want to send a ClearPrimary command to
        // it as well.
        outStandingHeartbeats.add(node);
        liveDatanodes.add(node);
      }
      synchronized(this) {
        if (outStandingHeartbeats.remove(node)) {
          outStandingReports.add(node);
          return true;
        }
      }
    }
    return false;
  }

  protected void reportPrimaryCleared(DatanodeID node) {
    if (safeModeState == SafeModeState.FAILOVER_IN_PROGRESS) {
      outStandingReports.remove(node);
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
    for (DatanodeInfo node : namesystem.datanodeReport(DatanodeReportType.LIVE)) {
      liveDatanodes.add(node);
      outStandingHeartbeats.add(node);
    }
    safeModeState = SafeModeState.FAILOVER_IN_PROGRESS;
    safeModeMonitor = new Daemon(new SafeModeMonitor(namesystem, this));
    safeModeMonitor.start();
    try {
      safeModeMonitor.join();
    } catch (InterruptedException ie) {
      throw new IOException("triggerSafeMode() interruped()");
    }
    if (safeModeState != SafeModeState.AFTER_FAILOVER) {
      throw new RuntimeException("safeModeState is : " + safeModeState +
          " which does not indicate a successfull exit of safemode");
    }
  }

  private void clearDataStructures() {
    outStandingHeartbeats.clear();
    outStandingReports.clear();
    liveDatanodes.clear();
  }

  @Override
  public synchronized String getTurnOffTip() {
    if (!isOn() || safeModeState == SafeModeState.AFTER_FAILOVER) {
      return "Safe mode is OFF";
    }

    String safeBlockRatioMsg = String.format(
        "The ratio of reported blocks %.8f has "
            + (!blocksSafe() ? "not " : "")
        + "reached the threshold %.8f. ", namesystem.getSafeBlockRatio(),
        threshold)
      + "Safe blocks = "
      + namesystem.getSafeBlocks()
      + ", Total blocks = "
      + namesystem.getTotalBlocks()
      + ", Remaining blocks = "
      + (namesystem.getTotalBlocks() - namesystem.getSafeBlocks())
      + ". "
      + "Reporting nodes = " + namesystem.getReportingNodes() + ". ";

    if (safeModeState == SafeModeState.BEFORE_FAILOVER) {
      return "This is the STANDBY AVATAR. Safe mode is ON. "
        + safeBlockRatioMsg;
    }

    String datanodeReportMsg = "All datanode reports have "
        + (!datanodeReportsReceived() ? "not " : "")
        + "been received, Outstanding Heartbeats : "
        + outStandingHeartbeats.size() + " Outstanding Reports : "
        + outStandingReports.size();
    return safeBlockRatioMsg + datanodeReportMsg;
  }

  @Override
  public boolean isManual() {
    return true;
  }

  @Override
  public boolean isOn() {
    return (safeModeState != SafeModeState.AFTER_FAILOVER);
  }

  @Override
  public synchronized void leave(boolean checkForUpgrades) {
    if (safeModeState == SafeModeState.BEFORE_FAILOVER) {
      throw new RuntimeException(
          "Cannot leave safe mode since Standby is in state : " + safeModeState);
    }
    // Recount file counts and quota
    namesystem.recount();

    // These datanodes have not reported, we are not sure about their state
    // remove them.
    removeOutStandingDatanodes();
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
      } catch (IOException e) {
        avatarnode
            .setFailoverFsck("Exception when running fsck after failover. "
                + StringUtils.stringifyException(e));
      }
    }
    
    super.startPostSafeModeProcessing();
    // We need to renew all leases, since client has been renewing leases only
    // on the primary.
    namesystem.leaseManager.renewAllLeases();
    safeModeState = SafeModeState.AFTER_FAILOVER;
  }

  private void setDatanodeDead(DatanodeID node) throws IOException {
    DatanodeDescriptor ds = namesystem.getDatanode(node);
    if (ds != null) {
      namesystem.setDatanodeDead(ds);
    }
  }

  /**
   * This does not need to be synchronized since it is invoked by leave().
   */
  private void removeOutStandingDatanodes() {
    try {
      for (DatanodeID node : outStandingHeartbeats) {
        namesystem.removeDatanode(node);
        setDatanodeDead(node);
      }

      for (DatanodeID node : outStandingReports) {
        namesystem.removeDatanode(node);
        setDatanodeDead(node);
      }
    } catch (IOException e) {
      throw new RuntimeException("Remove of datanode failed", e);
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

  private synchronized boolean datanodeReportsReceived() {
    return (outStandingHeartbeats.size() == 0 && outStandingReports.size() == 0);
  }

  @Override
  public boolean canLeave() {
    LOG.info(this.getTurnOffTip());
    if (safeModeState == SafeModeState.AFTER_FAILOVER) {
      return true;
    }
    return (blocksSafe() && datanodeReportsReceived() &&
        safeModeState == SafeModeState.FAILOVER_IN_PROGRESS);
  }

  @Override
  public void checkMode() {
    if (canLeave()) {
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

}
