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
import org.apache.hadoop.hdfs.util.InjectionHandler;
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
  private final float outStandingReportThreshold;
  
  private long lastStatusReportTime;

  public StandbySafeMode(Configuration conf, FSNamesystem namesystem) {
    super(conf, namesystem);
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
    InjectionHandler
        .processEvent(InjectionEvent.STANDBY_ENTER_SAFE_MODE);
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

  private float getDatanodeReportRatio() {
    if (liveDatanodes.size() != 0) {
      return ((liveDatanodes.size() - (outStandingHeartbeats.size() +
              outStandingReports.size())) / (float) liveDatanodes.size());
    }
    return 1;
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

    String datanodeReportMsg = "All datanode reports ratio "
        + getDatanodeReportRatio() + " have "
        + (!datanodeReportsReceived() ? "not " : "")
        + "reached threshold : " + this.outStandingReportThreshold
        + ", <a href=\"/outstandingnodes\"> Outstanding Heartbeats"
        + " : " + outStandingHeartbeats.size() + " Outstanding Reports : "
        + outStandingReports.size() + "</a><br><br>";
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
  public void leave(boolean checkForUpgrades) {
    namesystem.writeLock();
    try {
      synchronized (this) {
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
    } finally {
      namesystem.writeUnlock();
    }
  }

  private void setDatanodeDead(DatanodeID node) throws IOException {
    DatanodeDescriptor ds = namesystem.getDatanode(node);
    if (ds != null) {
      namesystem.setDatanodeDead(ds);
    }
  }

  /**
   * This function as a whole need to be synchronized since it is invoked by
   * leave().
   */
  private void removeOutStandingDatanodes() {
    try {
      synchronized (outStandingHeartbeats) {
        for (DatanodeID node : outStandingHeartbeats) {
          namesystem.removeDatanode(node);
          setDatanodeDead(node);
        }
      }

      synchronized (outStandingReports) {
        for (DatanodeID node : outStandingReports) {
          namesystem.removeDatanode(node);
          setDatanodeDead(node);
        }
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

  private void checkDatanodes() {
    try {
      synchronized (outStandingHeartbeats) {
        for (Iterator<DatanodeID> it = outStandingHeartbeats.iterator(); it
            .hasNext();) {
          DatanodeID node = it.next();
          DatanodeDescriptor dn = namesystem.getDatanode(node);
          if (namesystem.isDatanodeDead(dn)) {
            liveDatanodes.remove(dn);
            it.remove();
          }
        }
      }
      synchronized (outStandingReports) {
        for (Iterator<DatanodeID> it = outStandingReports.iterator(); it
            .hasNext();) {
          DatanodeID node = it.next();
          DatanodeDescriptor dn = namesystem.getDatanode(node);
          if (namesystem.isDatanodeDead(dn)) {
            liveDatanodes.remove(dn);
            it.remove();
          }
        }
      }
    } catch (IOException ie) {
      LOG.warn("checkDatanodes() caught : ", ie);
    }
  }

  private synchronized boolean datanodeReportsReceived() {
    boolean received = this.getDatanodeReportRatio() >=
      this.outStandingReportThreshold;
    if (!received) {
      checkDatanodes();
      return this.getDatanodeReportRatio() >= this.outStandingReportThreshold;
    }
    return received;
  }

  @Override
  public boolean canLeave() {
    if(FSNamesystem.now() - lastStatusReportTime > 1000) {
      lastStatusReportTime = FSNamesystem.now();
      LOG.info(this.getTurnOffTip());
    }
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
