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
import java.io.File;
import java.io.InterruptedIOException;
import java.lang.Thread;
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.Date;
import java.util.concurrent.CountDownLatch;
import org.apache.commons.logging.Log;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.net.DNS;
import org.apache.hadoop.util.InjectionHandler;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.hdfs.protocol.FSConstants;
import org.apache.hadoop.hdfs.qjournal.client.QuorumJournalManager;
import org.apache.hadoop.hdfs.server.protocol.NamenodeProtocol;
import org.apache.hadoop.hdfs.server.protocol.NamespaceInfo;
import org.apache.hadoop.hdfs.server.common.Storage.StorageDirectory;
import org.apache.hadoop.hdfs.server.namenode.CheckpointSignature;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.namenode.FSImage;
import org.apache.hadoop.hdfs.server.namenode.AvatarNode;
import org.apache.hadoop.hdfs.server.namenode.NNStorage.StorageLocationType;
import org.apache.hadoop.hdfs.server.namenode.metrics.AvatarNodeMetrics;
import org.apache.hadoop.hdfs.tools.offlineImageViewer.LsImageVisitor;
import org.apache.hadoop.hdfs.tools.offlineImageViewer.OfflineImageViewer;
import org.apache.hadoop.hdfs.util.InjectionEvent;
import org.apache.hadoop.http.HttpServer;

/**
 * This class drives the ingest of transaciton logs from primary.
 * It also implements periodic checkpointing of the  primary namenode.
 */

public class Standby implements Runnable{
  
  // allowed states of the ingest thread
  enum StandbyIngestState {
    NOT_INGESTING, 
    INGESTING_EDITS,
    CHECKPOINTING,
    STANDBY_QUIESCED
  };

  public static final Log LOG = AvatarNode.LOG;
  private static final long CHECKPOINT_DELAY = 10000; // 10 seconds
  public static long CHECKPOINT_SLEEP_BEFORE_RETRY = 30000;
  
  volatile private boolean running;
  
  // standby namenode
  private AvatarNode avatarNode;
  private Configuration confg;        // configuration of local standby namenode
  private Configuration startupConf;  // original configuration of AvatarNode
  private FSImage fsImage;            // fsImage of the current namenode.
  private FSNamesystem fsnamesys;     // fsnamesystem of the local standby namenode
  
  // ingest for consuming transaction log
  private volatile Ingest ingest;         // object that processes transaction logs from primary
  private volatile Thread ingestThread;   // thread that is procesing the transaction log
 
  // primary namenode
  private InetSocketAddress nameNodeAddr;   // remote primary namenode address
  private NamenodeProtocol primaryNamenode; // remote primary namenode
  private HttpServer infoServer;
  private int infoPort;
  
  // checkpointing
  private long checkpointPeriod;        // in seconds
  private long checkpointTxnCount;      // in txns
  private volatile long lastCheckpointTime;
  private long earlyScheduledCheckpointTime = Long.MAX_VALUE;
  private volatile long delayedScheduledCheckpointTime = 0L;
  private volatile boolean checkpointEnabled;
  private volatile CheckpointSignature sig;
  private volatile String checkpointStatus;
  private volatile ImageUploader imageUploader; 
  private volatile CountDownLatch manualCheckpointLatch = new CountDownLatch(0);
  
  // for image upload
  private String fsName;              // local namenode http name
  private final String machineName;   // host name of name node
  
  private long sleepBetweenErrors;
  
  private volatile Thread backgroundThread;  // thread for secondary namenode 

  // journal from which we are ingesting transactions
  private final JournalManager remoteJournal;
    
  // lock to protect state
  private Object ingestStateLock = new Object();  
  protected volatile StandbyIngestState currentIngestState 
    = StandbyIngestState.NOT_INGESTING;
  private volatile long currentSegmentTxId = -1;
  
  // this represents last transaction correctly loaded to namesystem
  // used for recovery, to rewind the state to last known checkpoint, but
  // reload transaction only from the lastCorrectlyLoadedTxId point
  private volatile long lastCorrectlyLoadedTxId = -1;
  
  // indicates to which txid the standby is quescing
  // used for quitting the ingest fast during failover
  volatile private long quiesceToTxid = Long.MAX_VALUE;
  
  // counts how many time standby failed to instantiate ingest
  private int ingestFailures = 0;

  private static final int MAX_INGEST_FAILURES = 10;
  // counts how many consecutive checkpoint failure the standby experienced
  private volatile int checkpointFailures = 0;
  static final int MAX_CHECKPOINT_FAILURES = 5;
  // max timeout for uploading image - 2 hours
  private static final long MAX_CHECKPOINT_UPLOAD_TIMEOUT = 2 * 60 * 60 * 1000;
  
  // maximum number of retires when instantiating ingest stream
  private final int inputStreamRetries;
  
  // image validation 
  private final File tmpImageFileForValidation;
  private Object imageValidatorLock = new Object();
  private ImageValidator imageValidator;
  
  // metrics
  private final AvatarNodeMetrics metrics;

  // The Standby can either be processing transaction logs
  // from the primary namenode or it could be doing a checkpoint to upload a merged
  // fsimage to the primary.
  // The startupConf is the original configuration that was used to start the
  // AvatarNode. It is used by the secondary namenode to talk to the primary.
  // The "conf" is the configuration of the local standby namenode.
  //
  Standby(AvatarNode avatarNode, Configuration startupConf, Configuration conf,
      InetSocketAddress nameNodeAddr, NamenodeProtocol primaryNamenode) 
    throws IOException {
    this.running = true;
    this.avatarNode = avatarNode;
    this.metrics = avatarNode.getAvatarNodeMetrics();
    this.confg = conf;
    this.startupConf = startupConf;
    this.fsImage = avatarNode.getFSImage();
    this.fsnamesys = avatarNode.getNamesystem();
    this.sleepBetweenErrors = startupConf.getInt("hdfs.avatarnode.sleep", 5000);
    this.nameNodeAddr = nameNodeAddr;
    this.primaryNamenode = primaryNamenode;
    
    initSecondary(startupConf); // start webserver for secondary namenode

    this.machineName = DNS.getDefaultIP(conf.get(FSConstants.DFS_NAMENODE_DNS_INTERFACE,"default"));
    LOG.info("machineName=" + machineName);
    
    InetSocketAddress addr = NameNode.getClientProtocolAddress(conf);
    this.tmpImageFileForValidation = new File("/tmp", "hadoop_image."
        + addr.getAddress().getHostAddress() + ":" + addr.getPort());

    URI remoteJournalURI = avatarNode.getRemoteSharedEditsURI(conf);
    if (remoteJournalURI.getScheme().equals(NNStorage.LOCAL_URI_SCHEME)) {
      StorageDirectory remoteJournalStorage = fsImage.storage.new StorageDirectory(
          new File(remoteJournalURI.getPath()));
      remoteJournal = new FileJournalManager(remoteJournalStorage, null, null);
    } else if (remoteJournalURI.getScheme().equals(
        QuorumJournalManager.QJM_URI_SCHEME)) {
      // TODO for now we pass null for NameNodeMetrics
      // once we have shared log we will pass the actual metrics
      remoteJournal = new QuorumJournalManager(conf, remoteJournalURI,
          new NamespaceInfo(fsImage.storage), null, false);
    } else {
      remoteJournal = FSEditLog.createJournal(conf, remoteJournalURI,
          new NamespaceInfo(fsImage.storage), null);
    }
    // we will start ingestion from the txid of the image
    this.currentSegmentTxId = avatarNode.getFSImage().storage
        .getMostRecentCheckpointTxId() + 1;
    this.inputStreamRetries = confg.getInt("dfs.ingest.retries", 30);
    checkpointStatus("No checkpoint initiated");
  } 
     
  NamenodeProtocol getPrimaryNameNode(){ 
    return primaryNamenode;
  }

  private void stopIngest() throws IOException {
    if (ingest != null) {
      ingest.stop();
      try {
        ingestThread.join();
      } catch (InterruptedException ie) {
        throw new IOException(ie);
      }
      ingestThread = null;
      ingest = null;
    }
  }
    
  public void run() {
    backgroundThread = Thread.currentThread();
    while (running) {
      try {
        InjectionHandler.processEventIO(InjectionEvent.STANDBY_BEGIN_RUN);
        // if the checkpoint periodicity or the checkpoint size has
        // exceeded the configured parameters, then also we have to checkpoint
        //
        long now = AvatarNode.now();
        checkAndRecoverState();

        if (shouldScheduleCheckpoint(now)) {

          // schedule an early checkpoint if this current one fails.
          earlyScheduledCheckpointTime = now + CHECKPOINT_DELAY;
          doCheckpoint();
          earlyScheduledCheckpointTime = Long.MAX_VALUE;
          lastCheckpointTime = now;

          InjectionHandler
              .processEvent(InjectionEvent.STANDBY_AFTER_DO_CHECKPOINT);
        }

        // if the checkpoint creation has switched off ingesting, then we restart the
        // ingestion here.
        if (currentIngestState == StandbyIngestState.NOT_INGESTING) {
          // the ingest might have finished but we need to ensure
          // we joined the thread. We do not quiesce the ingest here since that
          // incorrectly informs the ingest to move onto the next segment.
          stopIngest();
          instantiateIngest();
        }
        try {
          synchronized(backgroundThread) {
            backgroundThread.wait(sleepBetweenErrors);
          }
        } catch (InterruptedException e) {
          return;
        }
      } catch (SaveNamespaceCancelledException e) {
        return;
      } catch (FinalizeCheckpointException e) {
        LOG.info("Could not finalize checkpoint - will not kill ingest", e);
        
        // standby is not running any more
        if(!running) 
          return;
        
        // if exception happened during finalization, ingestion completed
        // successfully and now we are consuming next segment
        // do not kill the ingest
        continue;
      } catch (IOException e) {
        LOG.warn("Standby: encounter exception " + StringUtils.stringifyException(e));
        if(!running) // standby is quiescing
          return;
        try {
          synchronized(backgroundThread) {
            backgroundThread.wait(sleepBetweenErrors);
          }
        } catch (InterruptedException e1) {
          // give a change to exit this thread, if necessary
        }
        
        // since we had an error, we have to cleanup the ingest thread
        if (ingest != null) {
          ingest.stop();
          try {
            ingestThread.join();
            LOG.info("Standby: error cleanup Ingest thread exited.");
          } catch (InterruptedException em) {
            String msg = "Standby: error cleanup Ingest thread did not exit. " + em;
            LOG.info(msg);
            throw new RuntimeException(msg);
          }
        }
      } catch (Throwable e) {
        LOG.warn("Standby: encounter exception ", e);
        running = false;
      }
    }
  }
  
  private long getTransactionLag() {
    long lastCheckpoint = avatarNode.getFSImage().storage
        .getMostRecentCheckpointTxId();
    long currentTransaction = avatarNode.getFSImage().getLastAppliedTxId();
    return currentTransaction - lastCheckpoint;
  }

  protected JournalManager getRemoteJournal() {
    return remoteJournal;
  }

  synchronized void shutdown() {
    if (infoServer != null) {
      try {
        LOG.info("Shutting down secondary info server");
        infoServer.stop();
        infoServer = null;
      } catch (Exception ex) {
        LOG.error("Error shutting down infoServer", ex);
      }
    }
    
    if (remoteJournal != null) {
      try {
        LOG.info("Shutting down remote journal manager");
        remoteJournal.close();
      } catch (IOException ex) {
        LOG.error("Error shutting down infoServer", ex);
      }
    }
  }
  

  /**
   * Instantiate/quiesces edit log segments until the requested transaction 
   * id has been reached. When -1 is given, it quiesces all available log segments.
   * Notice that there is a retry logic in ingest instantiation
   */
  private long quiesceIngest(long lastTxId) throws IOException {
  	return quiesceIngest(lastTxId, true);
  }
  
  private long quiesceIngest(long lastTxId, boolean recoverUnfinalizedSegments)
    throws IOException {
    final boolean ignoreLastTxid = lastTxId == FSEditLogLoader.TXID_IGNORE;
    long startSegmentId = currentSegmentTxId;

    LOG.info("Standby: Quiescing ingest - Consuming transactions up to: "
        + lastTxId);

    if (ignoreLastTxid && !(remoteJournal instanceof FileJournalManager)
        && recoverUnfinalizedSegments) {
      // Since we have not yet recovered unfinalized segments, we don't
      // quiesce the ingest thread and instead just join on it.
      stopIngest();
      // we need to make sure to recover unclosed streams
      LOG.info("Standby: Recovering unclosed streams since the journal is non-file");
      remoteJournal.recoverUnfinalizedSegments();
    } else {
      // quiesce current ingest thread
      quiesceIngestWithReprocess();
    }

    // instantiate ingest for needed segments and quiesce
    while (avatarNode.getLastWrittenTxId() < lastTxId || ignoreLastTxid) {
      LOG.info("Standby: Quiescing ingest up to: " + lastTxId
          + ", setting up ingest for txid: " + currentSegmentTxId);
      assertState(StandbyIngestState.NOT_INGESTING);

      try {
        instantiateIngest();
      } catch (IOException e) {
        if (ignoreLastTxid) {
          LOG.warn("Cannot obtain the stream - exiting since the requested txid is " + lastTxId);
          break;
        }       
      }
      quiesceIngestWithReprocess();

      if (ingest.getIngestStatus() && currentSegmentTxId == startSegmentId) {
        // no progress, reached the end
        break;
      }
      startSegmentId = currentSegmentTxId;
    }

    // TODO successful roll can add 2 transactions
    // we need to take care of this
    if (lastTxId != FSEditLogLoader.TXID_IGNORE
        && lastTxId > avatarNode.getLastWrittenTxId()) {
      String msg = "Standby: Quiescing - Standby could not successfully ingest the edits up to: "
          + lastTxId
          + ", last consumed txid: "
          + avatarNode.getLastWrittenTxId();
      LOG.fatal(msg);
      throw new StandbyStateException(msg);
    }
    LOG.info("Standby: Quiescing ingest - Consumed transactions up to: "
        + avatarNode.getLastWrittenTxId());
    return currentSegmentTxId;
  }
  
  /**
   * When ingest consumes the end of segment transaction, it sets the state to
   * not ingesting. This function ensures that the ingest thread exited.
   * 
   * @throws IOException
   */
  private void quiesceIngestWithReprocess() throws IOException {
    if (ingest != null) {
      LOG.info("Standby: Quiescing - quiescing ongoing ingest");
      quiesceIngest();
      reprocessCurrentSegmentIfNeeded(ingest.getIngestStatus());
    }
  }
 
  /**
   * Quiesces the currently running ingest
   */
  private void quiesceIngest() throws IOException {
    InjectionHandler.processEvent(InjectionEvent.STANDBY_QUIESCE_INGEST);
    synchronized (ingestStateLock) {
      assertState(StandbyIngestState.INGESTING_EDITS,
          StandbyIngestState.NOT_INGESTING);
      ingest.quiesce();
    }
    try {
      ingestThread.join();
      currentIngestState = StandbyIngestState.NOT_INGESTING;
      LOG.info("Standby: Quiesce - Ingest thread for segment: "
          + ingest.toString() + " exited.");
    } catch (InterruptedException e) {
      LOG.info("Standby: Quiesce - Ingest thread interrupted.");
      throw new IOException(e.getMessage());
    }
  }
  
  /**
   * Check the correct ingest state for instantiating new ingest
   * (synchronized on ingestStateLock)
   * @return true if the ingest for the requested txid is running
   * @throws IOException when the state is incorrect
   */
  private boolean checkIngestState() throws IOException {
    if (currentIngestState == StandbyIngestState.INGESTING_EDITS) {
      String msg = "";
      if (ingest == null) {
        msg = "Standby: Ingest instantiation failed, the state is "
            + currentIngestState + ", but the ingest is null";
      }
      if (currentSegmentTxId != ingest.getStartTxId()) {
        msg = "Standby: Ingest instantiation failed, trying to instantiate ingest for txid: "
            + currentSegmentTxId
            + ", but there is ingest for txid: "
            + ingest.getStartTxId();
      }
      if (currentIngestState == StandbyIngestState.NOT_INGESTING
          && ingest.isRunning()) {
        msg = "Standby: Ingest instantiation failed, the state is "
            + currentIngestState + " but the ingest is running";
      }
      if (!msg.isEmpty()) {
        LOG.warn(msg);
        throw new IOException(msg);
      }
      return true;
    }
    return false;
  }

  private void checkAndRecoverState() throws Exception {
    if (ingestFailures > MAX_INGEST_FAILURES
        || InjectionHandler
            .falseCondition(InjectionEvent.STANDBY_RECOVER_STATE)) {
      LOG.info("Standby: Recovery - Ingest instantiation failed too many times");
      long lastCorrectCheckpointTxId = avatarNode.getFSImage().storage
          .getMostRecentCheckpointTxId();
      LOG.info("Standby: Recovery - Revert standby state to last checkpointed txid: "
          + lastCorrectCheckpointTxId
          + ", last correctly loaded txid: "
          + getLastCorrectTxId());

      LOG.info("Standby: Recovery - cleaning up ingest thread");
      clearIngestState(lastCorrectCheckpointTxId + 1);
      stopIngest();
      currentIngestState = StandbyIngestState.NOT_INGESTING;

      LOG.info("Standby: Recovery - Purging all local logs");
      FSEditLog editLog = avatarNode.getFSImage().getEditLog();

      // ingest is down, no one should be writing to edit log
      if (editLog.isOpen()) {
        // end current log segment
        avatarNode.getFSImage().getEditLog().endCurrentLogSegment(true);
      }
      // remove all local logs
      editLog.purgeLogsOlderThan(FSEditLog.PURGE_ALL_TXID);
      // reset edit log state to last checkpointed txid
      editLog.resetTxIds(lastCorrectCheckpointTxId);

      setIngestFailures(0);
      LOG.info("Standby: Recovery - Completed");
    }
  }

  private void setIngestFailures(int failures) {
    ingestFailures = failures;
    if (metrics != null) {
      metrics.numIngestFailures.set(failures);
    }
  }
  
  private void setCheckpointFailures(int failures) {
    checkpointFailures = failures;
    if (metrics != null) {
      metrics.numCheckpointFailures.set(failures);
    }
  }
  
  /**
   * Instantiates ingest thread for the current edits segment.
   */
  private void instantiateIngest() throws IOException {
    InjectionHandler.processEvent(InjectionEvent.STANDBY_INSTANTIATE_INGEST);
    try {
      synchronized (ingestStateLock) {
        if (checkIngestState()) {
          LOG.info("Standby: Ingest for txid: " + currentSegmentTxId
              + " is already running");
          return;
        }
        assertState(StandbyIngestState.NOT_INGESTING);
        ingest = new Ingest(this, fsnamesys, confg, currentSegmentTxId);
        ingestThread = new Thread(ingest);
        ingestThread.setName("Ingest_for_" + currentSegmentTxId);
        ingestThread.start();
        currentIngestState = StandbyIngestState.INGESTING_EDITS;
      }
      LOG.info("Standby: Instatiated ingest for txid: " + currentSegmentTxId);
    } catch (IOException e) {
      setIngestFailures(ingestFailures + 1);
      currentIngestState = StandbyIngestState.NOT_INGESTING;
      throw e;
    }
  }

  
  
  /**
   * Processes previously consumed edits segment if needed
   * 
   * @param status of the previous ingest
   */
  private void reprocessCurrentSegmentIfNeeded(boolean status)
      throws IOException {
    if (status) {
      return;
    }
    assertState(StandbyIngestState.NOT_INGESTING);
    LOG.info("Standby: Quiesce - reprocessing edits segment starting at: "
        + currentSegmentTxId);
    instantiateIngest();
    quiesceIngest();

    // verify that the entire transaction log was truly consumed
    // when re-processing, if we fail here, we cannot do anything
    // better than fail
    if (!ingest.getIngestStatus()) {
      String emsg = "Standby: Quiesce could not successfully ingest "
          + "transaction log starting at " + currentSegmentTxId;
      LOG.warn(emsg);
      setIngestFailures(ingestFailures + 1);
      throw new IOException(emsg);
    }
  }
  
  /**
   * Stop the standby, read all edit log segments.
   * @param lastTxId - the last txid to be consumed
   * @throws IOException
   */
  synchronized void quiesce(long lastTxId) throws IOException {
  	quiesce(lastTxId, true);
  }
  
  synchronized void quiesce(long lastTxId, boolean recoverUnfinalizedSegments)
    throws IOException {
    if (currentIngestState == StandbyIngestState.STANDBY_QUIESCED) {
      LOG.info("Standby: Quiescing - already quiesced");
      return; // nothing to do
    }    
    // have to wait for the main thread to exit here
    // first stop the main thread before stopping the ingest thread
    LOG.info("Standby: Quiescing up to txid: " + lastTxId);
    running = false;
    // in case there run() retries to instantiate ingest for next
    // non-existent segment, we indicate that the standby can not
    // expect to find anything with txid higher than lastTxId
    quiesceToTxid = lastTxId;
    InjectionHandler.processEvent(InjectionEvent.STANDBY_QUIESCE_INITIATED);
    
    // cancel saving namespace, and image validation
    fsnamesys.cancelSaveNamespace("Standby: Quiescing - Cancel save namespace");   
    interruptImageValidation();
    
    InjectionHandler.processEvent(InjectionEvent.STANDBY_QUIESCE_INTERRUPT);
   
    try {
      synchronized(backgroundThread) {
        backgroundThread.notifyAll();
      }
      if (backgroundThread != null) {
        backgroundThread.join();
        backgroundThread = null;
      }
    } catch (InterruptedException e) {
      LOG.info("Standby: quiesce interrupted.");
      throw new IOException(e.getMessage());
    } finally {
      // We don't want to cancel further save namespaces done manually.
      fsnamesys.clearCancelSaveNamespace();
    }
    try {
      if (infoServer != null) {
        infoServer.stop();
        infoServer= null;
      }
    } catch (Exception e) {
      LOG.error(StringUtils.stringifyException(e));
    }

    // process all log segments
    quiesceIngest(lastTxId, recoverUnfinalizedSegments);
    clearIngestState(currentSegmentTxId);
    
    // mark quiesce as completed
    LOG.info("Standby: Quiescing - Consumed transactions up to: "
        + getLastCorrectTxId() + ", requested: " + lastTxId);
    currentIngestState = StandbyIngestState.STANDBY_QUIESCED;
  }
  
  protected EditLogInputStream setupIngestStreamWithRetries(long txid)
      throws IOException {
    LOG.info("Standby: setup edit stream for txid: " + txid);
    for (int i = 0; i < inputStreamRetries; i++) {
      try {
        return setupCurrentEditStream(txid);
      } catch (IOException e) {
        // either the number of retires is too high
        // or the standby is quiescing to a lower transaction id
        if (i == inputStreamRetries - 1) {
          throwIOException("Cannot obtain edit stream for txid: " + txid, e);
        }
        if (txid > quiesceToTxid) {
          throwIOException("Standby: Quiesce in progress to txid: "
              + quiesceToTxid + ", aborting creating edit stream for: " + txid,
              e);
        }
        LOG.info("Error :", e);
      }
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        throw new IOException("Standby: - received interruption");
      }
      LOG.info("Standby: - retrying to get edit input stream for txid: " + txid
          + ", tried: " + (i + 1) + " times");
    }
    return null;
  }
  
  private EditLogInputStream setupCurrentEditStream(long txid)
      throws IOException {
    synchronized (ingestStateLock) {
      EditLogInputStream currentEditLogInputStream = JournalSet.getInputStream(
          remoteJournal, txid);
      InjectionHandler.processEventIO(InjectionEvent.STANDBY_JOURNAL_GETSTREAM);
      currentSegmentTxId = txid;
      return currentEditLogInputStream;
    }
  }
  
  /**
   * Set current checkpoint status
   */
  private void checkpointStatus(String st) {
    checkpointStatus = new Date(System.currentTimeMillis()).toString() + ": "
        + st;
  }

  /**
   * Get current checkpoint status.
   * Used for webui.
   */
  protected String getCheckpointStatus() {
    return checkpointStatus;
  }
  
  /**
   * Upload the image to the primary namenode
   */
  private class ImageUploader extends Thread {
    long txid;
    private volatile Exception error = null;
    private volatile boolean succeeded = false;
    private volatile boolean done = false;

    private ImageUploader(long txid) throws IOException {
      this.txid = txid;
    }

    public void run() {
      try {
        InjectionHandler.processEvent(InjectionEvent.STANDBY_UPLOAD_CREATE);
        putFSImage(txid);
        succeeded = true;
      } catch (Exception e) {
        LOG.info("Standby: Checkpointing - Image upload exception: ", e);
        error = e;
      } finally {
        done = true;
      }
    }
  }
  
  /**
   * Should checkpoint be triggered now.
   * Checkpoint is scheduled when:
   * - no checkpoint have been performed so far
   * - the checkpointPeriod time has passed since last checkpoint
   * - there is an early scheduled checkpoint (previous has failed)
   * - the number of uncheckpointed transactions is higher than configured
   * - manual checkpoint has been triggered
   * 
   * @param now current time
   * @return
   */
  private boolean shouldScheduleCheckpoint(long now) {
    fsnamesys.writeLock();
    try {
      if (lastCheckpointTime == 0
          || (lastCheckpointTime + 1000 * checkpointPeriod < now)
          || (earlyScheduledCheckpointTime < now)
          || getTransactionLag() > checkpointTxnCount
          || manualCheckpointLatch.getCount() == 2
          || InjectionHandler
              .falseCondition(InjectionEvent.STANDBY_CHECKPOINT_TRIGGER)) {

        // perform another check to see if the checkpoint has not been delayed
        // through configuration
        if (delayedScheduledCheckpointTime > now) {
          LOG.info("Standby: Checkpointing is delayed - will do checkpoint in: "
              + ((delayedScheduledCheckpointTime - now) / 1000));
          InjectionHandler
              .processEvent(InjectionEvent.STANDBY_DELAY_CHECKPOINT);
          return false;
        }
        countDownManualLatch(2);
        return true;
      }
      return false;
    } finally {
      fsnamesys.writeUnlock();
    }
  }
  
  private void countDownManualLatch(int countDownWhenEqual) {
    if (manualCheckpointLatch.getCount() == countDownWhenEqual) {
      // 2 - checkpoint is triggered manually, we decrease to 1
      // when checking the checkpoint trigger condition
      // 1 - checkpoint was triggered manually, we decrease to 0
      // when checkpoint is done
      manualCheckpointLatch.countDown();
    }
  }
  
  /**
   * Trigger checkpoint. If there is an ongoing scheduled checkpoint, this call
   * will trigger a checkpoint immediately after. The method blocks until the
   * checkpoint is done.
   */
  void triggerCheckpoint(boolean uncompressed) throws IOException {
    String pref = "Standby: Checkpoint - ";
    LOG.info(pref + "triggering checkpoint manually");

    // check error conditions
    if (uncompressed) {
      throwIOException(pref + " uncompressed option not supported", null);
    }
    if (manualCheckpointLatch.getCount() > 0) {
      throwIOException(pref + "Another manual checkpoint is in progress", null);
    }
    
    // set the manual checkpoint latch
    manualCheckpointLatch = new CountDownLatch(2);
    lastCheckpointTime = delayedScheduledCheckpointTime = 0;
    try {
      manualCheckpointLatch.await();
    } catch (InterruptedException e) {
      throwIOException(pref + "interrupted when performing manual checkpoint",
          e);
    }
    
    // check if checkpoint succeeded
    if (checkpointFailures > 0) {
      throwIOException(pref + "manual checkpoint failed", null);
    }
    LOG.info(pref + "manual checkpoint done");
  }
 
  /**
   * writes the in memory image of the local namenode to the fsimage
   * and tnen uploads this image to the primary namenode. The transaction 
   * log on the primary is purged too.
   */
  // DO NOT CHANGE THIS TO PUBLIC
  private void doCheckpoint() throws IOException {
    long start = AvatarNode.now();
    try {
      InjectionHandler.processEvent(InjectionEvent.STANDBY_ENTER_CHECKPOINT, this.sig);
      
      // Tell the remote namenode to start logging transactions in a new edit file
      // Retuns a token that would be used to upload the merged image.
      if (!checkpointEnabled) {
        checkpointStatus("Disabled");
        // This means the Standby is not meant to checkpoint the primary
        LOG.info("Standby: Checkpointing is disabled - return");
        return;
      }
      CheckpointSignature sig = null;
      InjectionHandler.processEvent(InjectionEvent.STANDBY_BEFORE_ROLL_EDIT);
      try {
        LOG.info("Standby: Checkpointing - Roll edits logs of primary namenode "
            + nameNodeAddr);
        checkpointStatus("Edit log rolled on primary");
        synchronized(ingestStateLock) {
          sig = (CheckpointSignature) primaryNamenode.rollEditLog();
        }
      } catch (IOException ex) {
        // In this case we can return since we did not kill the Ingest thread yet
        // Nothing prevents us from doing the next checkpoint attempt
        checkpointStatus("Checkpoint failed");
        LOG.warn("Standby: Checkpointing - roll Edits on the primary node failed.");
        InjectionHandler.processEvent(
            InjectionEvent.STANDBY_EXIT_CHECKPOINT_FAILED_ROLL, ex);
        return;
      }
      
      setLastRollSignature(sig); 
      
      // consume all finalized log segments up to the required txid
      long checkpointTxId = sig.curSegmentTxId - 1;  
      LOG.info("Standby: Checkpointing - checkpoint txid: " + checkpointTxId);
      checkpointStatus("Quiescing ingest");
      quiesceIngest(checkpointTxId);
      
      if (currentSegmentTxId != checkpointTxId + 1) {
        throw new IOException(
            "Standby: Checkpointing txid mismatch - ingest consumed txns up to "
                + (currentSegmentTxId - 1) + " but should have up to "
                + checkpointTxId);
      }
      
      // if everything is fine, the edit log should be closed now
      LOG.info("Standby: Checkpointing - finished quitting ingest thread just before ckpt.");       
      assertState(StandbyIngestState.NOT_INGESTING);
  
      /**
       * From now on Ingest thread needs to know if the checkpoint was started and never finished.
       * This would mean that it doesn't have to read the edits, since they were already processed
       * to the end as a part of a checkpoint. state = StandbyIngestState.CHECKPOINTING
       */
      fsnamesys.writeLock();
      try {      
        InjectionHandler.processEvent(InjectionEvent.STANDBY_BEFORE_SAVE_NAMESPACE);
        currentIngestState = StandbyIngestState.CHECKPOINTING;
    
        // save a checkpoint of the current namespace of the local Namenode
        // We should ideally use fsnamesystem.saveNamespace but that works
        // only if namenode is not in safemode.
        LOG.info("Standby: Checkpointing - save fsimage on local namenode.");
        checkpointStatus("Saving namespace started");
        // by default checkpoints are compressed if configured
        // manual command can override this setting for one checkpoint
        fsnamesys.getFSImage().saveNamespace(false);
        // get the new signature
        sig.mostRecentCheckpointTxId = fsImage.getEditLog().getLastWrittenTxId();
        sig.imageDigest = fsImage.storage.getCheckpointImageDigest(sig.mostRecentCheckpointTxId);
      } catch (SaveNamespaceCancelledException e) {
        InjectionHandler.processEvent(InjectionEvent.STANDBY_CANCELLED_EXCEPTION_THROWN);
        LOG.info("Standby: Checkpointing - cancelled saving namespace");
        fsnamesys.getFSImage().deleteCheckpoint(checkpointTxId);
        throw e;
      } catch (IOException ex) {
        // Standby failed to save fsimage locally. Need to reinitialize
        String msg = "Standby: Checkpointing - failed to checkpoint itself, so " +
        		"no image can be uploaded to the primary. The only course of action " +
        		"is to start from the very beginning by reinitializing AvatarNode";
        LOG.error(msg, ex);
        fsnamesys.getFSImage().deleteCheckpoint(checkpointTxId);
        throw new RuntimeException(msg, ex);
      } finally {
        currentIngestState = StandbyIngestState.NOT_INGESTING;
        fsnamesys.writeUnlock();
      }
      
      // we can start the ingest again for next segment
      instantiateIngest();
      
      try {
        finalizeCheckpoint(sig);
      } catch (IOException ex) {
        LOG.error("Standby: Checkpointing - rolling the fsimage " +
            "on the Primary node failed.", ex);
        throw new FinalizeCheckpointException(ex.toString());
      }
      setCheckpointFailures(0);
      LOG.info("Standby: Checkpointing - checkpoint completed in "
          + ((AvatarNode.now() - start) / 1000) + " s.");
    } catch (IOException e) {
      LOG.error("Standby: Checkpointing - failed to complete the checkpoint: "
          + StringUtils.stringifyException(e));
      checkpointStatus("Checkpoint failed");    
      if (!checkpointEnabled) {
        // we received instruction to prepare for failover
        return;
      }
      handleCheckpointFailure();
      InjectionHandler.processEvent(InjectionEvent.STANDBY_EXIT_CHECKPOINT_EXCEPTION, e);
      throw e;
    } finally {
      countDownManualLatch(1);
      setLastRollSignature(null);
      InjectionHandler.processEvent(InjectionEvent.STANDBY_EXIT_CHECKPOINT, this.sig);
    }
  }
  
  /**
   * If checkpoint fails continuously, we want to abort the standby. We want to
   * avoid the situation in which the standby continuously rolls edit log on the
   * primary without finalizing checkpoint.
   */
  private void handleCheckpointFailure() {
    setCheckpointFailures(checkpointFailures + 1);
    if (checkpointFailures > MAX_CHECKPOINT_FAILURES) {
      LOG.fatal("Standby: Checkpointing - standby failed to checkpoint in "
          + checkpointFailures + " attempts. Aborting");
    } else {
      // We want to give some time for some transition error to recover
      // and DNS caching to expire. This is mainly for small clusters
      // where checkpoint can be very fast. Doesn't hurt if we sleep
      // on large clusters too.
      //
      LOG.info("Sleeping " + CHECKPOINT_SLEEP_BEFORE_RETRY
          + " msecs before retry checkpoints...");
      try {
        Thread.sleep(CHECKPOINT_SLEEP_BEFORE_RETRY);
        return;
      } catch (InterruptedException ie) {
        LOG.warn("Standby: Checkpointing - Thread interrupted"
            + " while sleeping before a retry.", ie);
      }
    }
    FSEditLog.runtime.exit(-1);
  }
  
  /**
   * Creates image upload thread. 
   */
  private void uploadImage(long txid) throws IOException {
    final long start = AvatarNode.now();
    LOG.info("Standby: Checkpointing - Upload fsimage to remote namenode.");
    checkpointStatus("Image upload started");
    
    imageUploader = new ImageUploader(txid);
    imageUploader.start();
    
    // wait for the upload to complete   
    while (running
        && !imageUploader.done
        && AvatarNode.now() - start < MAX_CHECKPOINT_UPLOAD_TIMEOUT) {
      try {
        imageUploader.join(3000);
      } catch (InterruptedException ie) { 
        LOG.error("Reveived interruption when uploading image for txid: "
            + txid);
        Thread.currentThread().interrupt();
        throw (IOException) new InterruptedIOException().initCause(ie);
      } 
    }
    if (!running || !imageUploader.succeeded) {
      InjectionHandler.processEvent(InjectionEvent.STANDBY_UPLOAD_FAIL);
      throw new IOException(
          "Standby: Checkpointing - Image upload failed (time= "
              + (AvatarNode.now() - start) + " ms).", imageUploader.error);
    }
    imageUploader = null;
    LOG.info("Standby: Checkpointing - Upload fsimage to remote namenode DONE.");
    checkpointStatus("Image upload completed");
  }
  
  /**
   * Copy the new fsimage into the NameNode
   */
  private void putFSImage(long txid) throws IOException {
    TransferFsImage.uploadImageFromStorage(fsName, machineName, infoPort,
        fsImage.storage, txid);
  }
  
  private void finalizeCheckpoint(CheckpointSignature sig) 
      throws IOException{

    try {
      File imageFile = fsImage.storage.getFsImageName(
          StorageLocationType.LOCAL, sig.mostRecentCheckpointTxId);
      InjectionHandler.processEvent(InjectionEvent.STANDBY_BEFORE_PUT_IMAGE,
          imageFile);
      
      // start a thread to validate image while uploading the image to primary
      createImageValidation(imageFile);
      
      // copy image to primary namenode
      uploadImage(sig.mostRecentCheckpointTxId);
  
      // check if the image is valid
      checkImageValidation();
      
      // make transaction to primary namenode to switch edit logs
      LOG.info("Standby: Checkpointing - Roll fsimage on primary namenode.");
      InjectionHandler.processEventIO(InjectionEvent.STANDBY_BEFORE_ROLL_IMAGE);
        
      assertState(
          StandbyIngestState.NOT_INGESTING,
          StandbyIngestState.INGESTING_EDITS);
      
      primaryNamenode.rollFsImage(new CheckpointSignature(fsImage));
      setLastRollSignature(null);      
      LOG.info("Standby: Checkpointing - Checkpoint done. New Image Size: "
          + fsImage.getFsImageName(StorageLocationType.LOCAL).length());
      checkpointStatus("Completed");
    } finally {
      interruptImageValidation();
    }
  }
  
  ////////////////////////// IMAGE VALIDATION //////////////////////////
  
  /**
   * Load the image to validate that it is not corrupted
   */
  private class ImageValidator extends Thread {
    private OfflineImageViewer viewer;
    volatile private Throwable error = null;
    volatile private boolean succeeded = false;
    private ImageValidator(File imageFile) throws IOException {
      LOG.info("Validating image file " + imageFile);
      tmpImageFileForValidation.delete();
      LsImageVisitor v = new LsImageVisitor(tmpImageFileForValidation.toString());
      viewer = new OfflineImageViewer(imageFile.toString(), v, true);
    }
    
    public void run() {
      try {
        viewer.go();
        succeeded = true;
      } catch (Throwable e) {
        LOG.info("Standby: Image validation exception: ", e);
        error = e;
      }
    }
  }
  
  /**
   * Checks the status of image validation during checkpoint.
   * @throws IOException
   */
  private void checkImageValidation() throws IOException {
    try {
      imageValidator.join();
    } catch (InterruptedException ie) {
      throw (IOException) new InterruptedIOException().initCause(ie);
    } 
    if (!imageValidator.succeeded) {
      throw new IOException("Image file validation failed",
          imageValidator.error);
    }
  }

  /**
   * Creates image validation thread. 
   * @param imageFile
   * @throws IOException on error, or when standby quiesce was invoked
   */
  private void createImageValidation(File imageFile) throws IOException {
    synchronized (imageValidatorLock) {
      InjectionHandler.processEvent(InjectionEvent.STANDBY_VALIDATE_CREATE);
      if (!running) {
        // fails the checkpoint
        InjectionHandler.processEvent(InjectionEvent.STANDBY_VALIDATE_CREATE_FAIL);
        throw new IOException("Standby: standby is quiescing");
      }
      imageValidator = new ImageValidator(imageFile);
      imageValidator.start();
    }
  }

  /**
   * Interrupts and joins ongoing image validation.
   * @throws IOException
   */
  private void interruptImageValidation() throws IOException {
    synchronized (imageValidatorLock) {
      if (imageValidator != null) {
        imageValidator.interrupt();
        try {
          imageValidator.join();
        } catch (InterruptedException e) {
          throw new InterruptedIOException("Standby: received interruption");
        }
      }
    }
  }
  
  //////////////////////////IMAGE VALIDATION END //////////////////////////

  /**
   * Initialize the webserver so that the primary namenode can fetch
   * transaction logs from standby via http.
   */
  void initSecondary(Configuration conf) throws IOException {

    fsName = AvatarNode.getRemoteNamenodeHttpName(conf,
        avatarNode.getInstanceId());

    // Initialize other scheduling parameters from the configuration
    checkpointEnabled = conf.getBoolean("fs.checkpoint.enabled", false);
    checkpointPeriod = conf.getLong("fs.checkpoint.period", 3600);
    checkpointTxnCount = NNStorageConfiguration.getCheckpointTxnCount(conf);
    delayedScheduledCheckpointTime = conf.getBoolean("fs.checkpoint.delayed",
        false) ? AvatarNode.now() + checkpointPeriod * 1000 : 0;
    

    // initialize the webserver for uploading files.
    String infoAddr = 
      NetUtils.getServerAddress(conf,
                                "dfs.secondary.info.bindAddress",
                                "dfs.secondary.info.port",
                                "dfs.secondary.http.address");
    InetSocketAddress infoSocAddr = NetUtils.createSocketAddr(infoAddr);
    String infoBindIpAddress = infoSocAddr.getAddress().getHostAddress();
    int tmpInfoPort = infoSocAddr.getPort();
    infoServer = new HttpServer("secondary", infoBindIpAddress, tmpInfoPort,
        tmpInfoPort == 0, conf);
    infoServer.setAttribute("name.system.image", fsImage);
    this.infoServer.setAttribute("name.conf", conf);
    infoServer.addInternalServlet("getimage", "/getimage", GetImageServlet.class);
    infoServer.start();
    avatarNode.httpServer.setAttribute("avatar.node", avatarNode);
    avatarNode.httpServer.addInternalServlet("outstandingnodes",
        "/outstandingnodes", OutStandingDatanodesServlet.class);

    // The web-server port can be ephemeral... ensure we have the correct info
    infoPort = infoServer.getPort();
    conf.set("dfs.secondary.http.address", infoBindIpAddress + ":" +infoPort);
    LOG.info("Secondary Web-server up at: " + infoBindIpAddress + ":" +infoPort);
    LOG.warn("Checkpoint Period   :" + checkpointPeriod + " secs " +
             "(" + checkpointPeriod/60 + " min)");
    if (delayedScheduledCheckpointTime > 0) {
      LOG.warn("Standby: Checkpointing will be delayed by: " + checkpointPeriod + " seconds");
    }
    LOG.warn("Log Size Trigger    :" + checkpointTxnCount + " transactions.");
  }
  
  public void setLastRollSignature(CheckpointSignature sig) {
    this.sig = sig;
  }
  
  public CheckpointSignature getLastRollSignature() {
    return this.sig;
  }

  public long getLastCheckpointTime() {
    return lastCheckpointTime;
  }
  
  public boolean fellBehind() {
    try {
      switch (currentIngestState) {
      case INGESTING_EDITS:
        return ingest.isCatchingUp();
      case NOT_INGESTING:
      case CHECKPOINTING:
        return true;
      case STANDBY_QUIESCED:
        return false;
      default:
        LOG.error("Standby: unknown ingest state: " + currentIngestState);
        return true;
      }
    } catch (Exception e) {
      // since this is not synchronized on ingest state lock
      // in the case of any exceptions (e.g., NPE) we return true
      return true;
    }
  }

  /**
   * Tells how far behind the standby is with consuming edits 
   * (only in progress segments).
   */
  public long getLagBytes() {
    return ingest == null ? -1 : this.ingest.getLagBytes();
  }

  /**
   * Sets state for standby upon successful ingestion.
   * @param txid next starting txid
   */
  protected void clearIngestState(long txid) {
    synchronized (ingestStateLock) {
      currentSegmentTxId = txid;
      currentIngestState = StandbyIngestState.NOT_INGESTING;
    }
  }
  
  /**
   * Sets state for standby upon unsuccessful ingestion.
   * Ingest will start from the same transaction id.
   */
  protected void clearIngestState() {
    synchronized (ingestStateLock) {
      currentIngestState = StandbyIngestState.NOT_INGESTING;
    }
  }
  
  /**
   * Assert that the standby is in the expected state
   * 
   * @param expectedStates expected states to be in
   */
  private void assertState(StandbyIngestState... expectedStates)
      throws IOException {
    for (StandbyIngestState s : expectedStates) {
      if (currentIngestState == s)
        return;
    }
    throw new IOException("Standby: illegal state - current: "
        + currentIngestState);
  }
  
  /**
   * Ingest will keep track of the last correctly loaded transaction
   * for recovery. Only updated by ingest, does not need to be synchronized.
   * @param txid the id of the last correctly loaded transaction
   */
  protected void setLastCorrectTxId(long txid) {
    if (txid > lastCorrectlyLoadedTxId)
      lastCorrectlyLoadedTxId = txid;
  }
  
  /**
   * Return the last known transaction that has been
   * successfully loaded.
   */
  protected long getLastCorrectTxId() {
    return lastCorrectlyLoadedTxId;
  }
  
  /**
   * Get the number of failed checkpoints.
   */
  int getNumCheckpointFailures() {
    return checkpointFailures;
  }
  
  private void throwIOException(String msg, Exception e) throws IOException {
    LOG.error(msg, e);
    throw new IOException(msg);
  }
  
  /**
   * No more checkpoints will be performed.
   */
  void disableCheckpoint() {
    checkpointEnabled = false;
  }
}
