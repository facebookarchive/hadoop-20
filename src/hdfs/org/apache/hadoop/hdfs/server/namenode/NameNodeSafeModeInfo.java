package org.apache.hadoop.hdfs.server.namenode;

import java.io.IOException;
import java.util.Date;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Daemon;
import org.apache.hadoop.util.FlushableLogger;
import org.apache.hadoop.util.StringUtils;

/**
 * NameNodeSafeModeInfo contains information related to the safe mode.
 * <p/>
 * An instance of {@link NameNodeSafeModeInfo} is created when the name node
 * enters safe mode.
 * <p/>
 * During name node startup {@link NameNodeSafeModeInfo} counts the number of
 * <em>safe blocks</em>, those that have at least the minimal number of
 * replicas, and calculates the ratio of safe blocks to the total number of
 * blocks in the system, which is the size of {@link FSNamesystem#blocksMap}.
 * When the ratio reaches the {@link #threshold} it starts the
 * {@link SafeModeMonitor} daemon in order to monitor whether the safe mode
 * {@link #extension} is passed. Then it leaves safe mode and destroys itself.
 * <p/>
 * If safe mode is turned on manually then the number of safe blocks is not
 * tracked because the name node is not intended to leave safe mode
 * automatically in the case.
 * 
 * @see ClientProtocol#setSafeMode(FSConstants.SafeModeAction)
 * @see SafeModeMonitor
 */
public class NameNodeSafeModeInfo implements SafeModeInfo {
  // configuration fields
  /**
   * Safe mode threshold condition %.
   */
  protected double threshold;
  /**
   * Safe mode extension after the threshold.
   */
  private long extension;

  /** threshold for populating needed replication queues */
  private double replQueueThreshold;

  // internal fields
  /**
   * Time when threshold was reached.
   * <p/>
   * <br>
   * -1 safe mode is off <br>
   * 0 safe mode is on, but threshold is not reached yet
   */
  volatile private long reached = -1;

  /**
   * time of the last status printout
   */
  private long lastStatusReport = 0;

  private final FSNamesystem namesystem;
  private final NameNode nameNode;
  private Daemon smmthread = null; // SafeModeMonitor thread
  private final static Log LOG = LogFactory.getLog(NameNodeSafeModeInfo.class);
  private final static Log FLOG = FlushableLogger.getLogger(LOG);

  NameNodeSafeModeInfo(FSNamesystem namesystem) {
    this(new Configuration(), namesystem);
  }

  /**
   * Creates SafeModeInfo when the name node enters automatic safe mode at
   * startup.
   * 
   * @param conf
   *          configuration
   */
  NameNodeSafeModeInfo(Configuration conf, FSNamesystem namesystem) {
    this.threshold = conf.getFloat("dfs.safemode.threshold.pct", 0.95f);
    this.extension = conf.getLong("dfs.safemode.extension", 0);
    // default to safe mode threshold
    // (i.e., don't populate queues before leaving safe mode)
    this.replQueueThreshold = conf.getFloat(
        "dfs.namenode.replqueue.threshold-pct", (float) threshold);
    this.namesystem = namesystem;
    this.nameNode = namesystem.getNameNode();
    
    // start initial block report executor
    this.namesystem.setupInitialBlockReportExecutor(false);
    // set fields of fsnamesystem to trigger replication queues initialization
    this.namesystem.setInitializedReplicationQueues(false);
    this.namesystem.blocksSafe = 0;
  }

  @Override
  public boolean isOn() {
    assert isConsistent() : " SafeMode: Inconsistent filesystem state: "
                                  + "Total num of blocks, active blocks, or "
                                  + "total safe blocks don't match.";
    return this.reached >= 0;
  }


  /**
   * Enter safe mode.
   */
  private void enter() {
    this.reached = 0;
  }

  protected boolean needUpgrade(boolean checkForUpgrades) {
    if (checkForUpgrades) {
      // verify whether a distributed upgrade needs to be started
      boolean needUpgrade = false;
      try {
        needUpgrade = namesystem.startDistributedUpgradeIfNeeded();
      } catch (IOException e) {
        FSNamesystem.LOG.error(StringUtils.stringifyException(e));
      }
      if (needUpgrade) {
        setManual();
        return true;
      }
    }
    return false;
  }

  protected void startPostSafeModeProcessing() {
    // initialize replication queues
    initializeReplQueues();
    long timeInSafemode = FSNamesystem.now() - namesystem.systemStart;
    NameNode.stateChangeLog.info("STATE* Leaving safe mode after "
        + timeInSafemode / 1000 + " secs.");
    NameNode.getNameNodeMetrics().safeModeTime.set((int) timeInSafemode);

    if (reached >= 0) {
      NameNode.stateChangeLog.info("STATE* Safe mode is OFF.");
    }
    reached = -1;
    try {
      nameNode.startServerForClientRequests();
    } catch (IOException ex) {
      LOG.fatal("Got exception when starting server for client requests: ", ex);
      nameNode.stop();
    }
    NameNode.stateChangeLog.info("STATE* Network topology has "
        + namesystem.clusterMap.getNumOfRacks() + " racks and "
        + namesystem.clusterMap.getNumOfLeaves() + " datanodes");
    NameNode.stateChangeLog.info("STATE* UnderReplicatedBlocks has "
        + namesystem.getUnderReplicatedBlocks() + " blocks");
  }

  @Override
  public void leave(boolean checkForUpgrades) {
    namesystem.writeLock();
    try {
      if (needUpgrade(checkForUpgrades)) {
        return;
      }

      startPostSafeModeProcessing();
    } finally {
      namesystem.writeUnlock();
    }
  }
  
  /**
   * Initializes replication queues *without* leaving safemode.
   * This should only be used ONLY through dfsadmin command.
   */
  @Override
  public void initializeReplicationQueues() {
    // this function internally holds FSNamesystem.writeLock
    initializeReplQueues();
  }
  
  protected void initializeReplQueues() {
    LOG.info("initializing replication queues");
    namesystem.processMisReplicatedBlocks();
  }

  /**
   * Check whether we have reached the threshold for initializing replication
   * queues.
   */
  private boolean canInitializeReplQueues() {
    return namesystem.getSafeBlockRatio() >= this.replQueueThreshold;
  }

  /**
   * Safe mode can be turned off iff the threshold is reached and the extension
   * time have passed.
   * 
   * @return true if can leave or false otherwise.
   */
  @Override
    public boolean canLeave() {
      if (reached == 0) {
        return false;
      }
      if (namesystem.now() - reached < extension) {
        reportStatus("STATE* Safe mode ON.", false);
        return false;
      }
      return !needEnter();
    }

  /**
   * There is no need to enter safe mode if DFS is empty or {@link #threshold}
   * == 0
   */
  private boolean needEnter() {
    return isManual() || namesystem.getSafeBlockRatio() < threshold;
  }

  @Override
    public void checkMode() {
      if (needEnter()) {
        enter();
        // check if we are ready to initialize replication queues
        if (!isManual() && canInitializeReplQueues() 
            && !namesystem.isPopulatingReplQueues()) {
          initializeReplQueues();
        }
        reportStatus("STATE* Safe mode ON.", false);
        return;
      }
      // the threshold is reached
      if (!isOn() || // safe mode is off
          extension <= 0 || threshold <= 0) { // don't need to wait
        this.leave(true); // leave safe mode
        return;
          }
      if (reached > 0) { // threshold has already been reached before
        reportStatus("STATE* Safe mode ON.", false);
        return;
      }
      // start monitor
      reached = namesystem.now();
      smmthread = new Daemon(new SafeModeMonitor(namesystem, this));
      smmthread.start();
      reportStatus("STATE* Safe mode extension entered.", true);

      // check if we are ready to initialize replication queues
      if (canInitializeReplQueues() && !namesystem.isPopulatingReplQueues()) {
        initializeReplQueues();
      }
    }

  @Override
    public boolean isManual() {
      return extension == Long.MAX_VALUE;
    }

  /**
   * Enter safemode manually.
   * <p/>
   * The {@link #threshold} is set to 1.5 so that it could never be reached.
   * {@link #namesystem.getTotalBlocks()} is set to -1 to indicate that safe
   * mode is manual.
   */
  @Override
    public void setManual() {
      this.threshold = 1.5f; // this threshold can never be reached
      this.extension = Long.MAX_VALUE;
      this.replQueueThreshold = 1.5f; // can never be reached
      this.reached = -1;
      enter();
      reportStatus("STATE* Safe mode is ON.", true);
    }

  @Override
    public String getTurnOffTip() {
      String leaveMsg = "Safe mode will be turned off automatically";
      if (reached < 0) {
        return "Safe mode is OFF.";
      }
      if (isManual()) {
        if (namesystem.getDistributedUpgradeState()) {
          return leaveMsg + " upon completion of "
            + "the distributed upgrade: upgrade progress = "
            + namesystem.getDistributedUpgradeStatus() + "%";
        }
        leaveMsg = "Use \"hadoop dfsadmin -safemode leave\" to turn safe mode off";
      }
      if (namesystem.getTotalBlocks() < 0) {
        return leaveMsg + ".";
      }
      String initReplicationQueues = namesystem.isPopulatingReplQueues() 
          ? " Replication queues have been initialized. "
          : "";
      String safeBlockRatioMsg = String.format(
        initReplicationQueues 
        +"The ratio of reported blocks %.8f has " + (reached == 0 ? "not " : "")
        + "reached the threshold %.8f. ", namesystem.getSafeBlockRatio(),
          threshold)
        + "Safe blocks = "
        + namesystem.getSafeBlocks()
        + ", Total blocks = "
        + namesystem.getTotalBlocks()
        + ", Remaining blocks = "
        + (namesystem.getTotalBlocks() - namesystem.getSafeBlocks())
        + ". "
        + "Reporting nodes = " + namesystem.getReportingNodes() + ". " + leaveMsg;
      if (reached == 0 || isManual()) // threshold is not reached or manual
      {
        return safeBlockRatioMsg + ".";
      }
      // extension period is in progress
      return safeBlockRatioMsg + " in " + Math.abs(reached + extension - namesystem.now())
        / 1000 + " seconds.";
    }

  /**
   * Print status every 20 seconds.
   */
  private void reportStatus(String msg, boolean rightNow) {
    long curTime = FSNamesystem.now();
    if (!rightNow && (curTime - lastStatusReport < 20 * 1000)) {
      return;
    }
    FLOG.info(msg + " \n" + getTurnOffTip());
    lastStatusReport = curTime;
  }

  /**
   * Returns printable state of the class.
   */
  public String toString() {
    String resText = "Current safe block ratio = "
      + namesystem.getSafeBlockRatio() + ". Safe blocks = "
      + namesystem.getSafeBlocks() + ". Total blocks = "
      + namesystem.getTotalBlocks()
      + ". Target threshold = " + threshold + ". Minimal replication = "
      + namesystem.getMinReplication() + ".";
    if (reached > 0) {
      resText += " Threshold was reached " + new Date(reached) + ".";
    }
    return resText;
  }

  /**
   * Checks consistency of the class state. This is costly and currently called
   * only in assert.
   */
  private boolean isConsistent() {
    if (this.reached < 0) {
      return true; // Safemode is off.
    }
    if (namesystem.getTotalBlocks() == -1 && namesystem.getSafeBlocks() == -1) {
      return true; // manual safe mode
    }
    long activeBlocks = namesystem.getBlocksTotal()
      - namesystem.getPendingDeletionBlocks();
    return (namesystem.getTotalBlocks() == activeBlocks)
      || (namesystem.getSafeBlocks() >= 0 && namesystem.getSafeBlocks() <= namesystem
          .getTotalBlocks());
  }

  @Override
    public void shutdown() {
      if (smmthread != null) {
        smmthread.interrupt();
      }
    }

  @Override
  public boolean shouldProcessRBWReports() {
    // Primary namenode always processed RBW reports.
    return true;
  }
}
