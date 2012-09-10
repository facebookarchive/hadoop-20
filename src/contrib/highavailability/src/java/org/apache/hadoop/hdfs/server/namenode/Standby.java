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
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.logging.Log;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.*;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.net.DNS;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.hdfs.server.protocol.NamenodeProtocol;
import org.apache.hadoop.hdfs.server.common.Storage.StorageDirectory;
import org.apache.hadoop.hdfs.server.namenode.CheckpointSignature;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.namenode.FSImage;
import org.apache.hadoop.hdfs.server.namenode.AvatarNode;
import org.apache.hadoop.hdfs.tools.offlineImageViewer.LsImageVisitor;
import org.apache.hadoop.hdfs.tools.offlineImageViewer.OfflineImageViewer;
import org.apache.hadoop.hdfs.util.InjectionEvent;
import org.apache.hadoop.hdfs.util.InjectionHandler;
import org.apache.hadoop.http.HttpServer;

/**
 * This class drives the ingest of transaciton logs from primary.
 * It also implements periodic checkpointing of the  primary namenode.
 */

public class Standby implements Runnable{

  public static final Log LOG = AvatarNode.LOG;
  private static final long CHECKPOINT_DELAY = 10000; // 10 seconds
  private AvatarNode avatarNode;
  private Configuration confg; // configuration of local standby namenode
  private Configuration startupConf; // original configuration of AvatarNode
  private FSImage fsImage; // fsImage of the current namenode.
  private FSNamesystem fsnamesys; // fsnamesystem of the local standby namenode
  volatile private Ingest ingest;   // object that processes transaction logs from primary
  volatile private Thread ingestThread;  // thread that is procesing the transaction log
  volatile private boolean running;
  private final String machineName; // host name of name node

  //
  // These are for the Secondary NameNode.
  //
  private String fsName;                    // local namenode http name
  private InetSocketAddress nameNodeAddr;   // remote primary namenode address
  private NamenodeProtocol primaryNamenode; // remote primary namenode
  private HttpServer infoServer;
  private int infoPort;
  private String infoBindAddress;
  private long checkpointPeriod;        // in seconds
  private long checkpointTxnCount;      // in txns
  private long lastCheckpointTime;
  private long earlyScheduledCheckpointTime = Long.MAX_VALUE;
  private long sleepBetweenErrors;
  private boolean checkpointEnabled;
  volatile private Thread backgroundThread;  // thread for secondary namenode 
  volatile private CheckpointSignature sig;
  private volatile String checkpointStatus;
  
  // allowed states of the ingest thread
  enum StandbyIngestState {
    NOT_INGESTING, 
    INGESTING_EDITS,
    CHECKPOINTING,
    STANDBY_QUIESCED
  };
  
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
  
  //counts how many time standby failed to instantiate ingest
  private int ingestFailures = 0;
  
  // image validation 
  private final File tmpImageFileForValidation;
  
  private final int inputStreamRetries;
  
  private Object imageValidatorLock = new Object();
  private ImageValidator imageValidator;

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
    this.confg = conf;
    this.startupConf = startupConf;
    this.fsImage = avatarNode.getFSImage();
    this.fsnamesys = avatarNode.getNamesystem();
    this.sleepBetweenErrors = startupConf.getInt("hdfs.avatarnode.sleep", 5000);
    this.nameNodeAddr = nameNodeAddr;
    this.primaryNamenode = primaryNamenode;
    
    initSecondary(startupConf); // start webserver for secondary namenode

    this.machineName =
      DNS.getDefaultHost(conf.get("dfs.namenode.dns.interface","default"),
                         conf.get("dfs.namenode.dns.nameserver","default"));
    LOG.info("machineName=" + machineName);
    
    InetSocketAddress addr = NameNode.getAddress(conf);
    this.tmpImageFileForValidation = new File("/tmp", "hadoop_image."
        + addr.getHostName() + ":" + addr.getPort());

    URI remoteJournalURI = avatarNode.getRemoteSharedEditsURI(conf);
    if (remoteJournalURI.getScheme().equals(NNStorage.LOCAL_URI_SCHEME)) {
      StorageDirectory remoteJournalStorage = fsImage.storage.new StorageDirectory(
          new File(remoteJournalURI.getPath()));
      remoteJournal = new FileJournalManager(remoteJournalStorage, null);
    } else {
      remoteJournal = FSEditLog.createJournal(remoteJournalURI, conf);
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

        if (lastCheckpointTime == 0 ||
            (lastCheckpointTime + 1000 * checkpointPeriod < now) ||
            (earlyScheduledCheckpointTime < now) ||
            getTransactionLag() > checkpointTxnCount ||
            InjectionHandler.falseCondition(InjectionEvent.STANDBY_CHECKPOINT_TRIGGER)) {

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
          // we joined the thread
          quiesceIngestWithReprocess();
          instantiateIngest();
        }
        try {
          Thread.sleep(sleepBetweenErrors);
        } catch (InterruptedException e) {
          return;
        }
      } catch (SaveNamespaceCancelledException e) {
        return;
      } catch (IOException e) {
        LOG.warn("Standby: encounter exception " + StringUtils.stringifyException(e));
        if(!running) // standby is quiescing
          return;
        try {
          Thread.sleep(sleepBetweenErrors);
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
    if (!running) {
      return;
    }
    if (infoServer != null) {
      try {
      LOG.info("Shutting down secondary info server");
      infoServer.stop();
      infoServer = null;
      } catch (Exception ex) {
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
    final boolean ignoreLastTxid = lastTxId == FSEditLogLoader.TXID_IGNORE;
    long startSegmentId = currentSegmentTxId;

    LOG.info("Standby: Quiescing ingest - Consuming transactions up to: "
        + lastTxId);

    // join current ingest thread
    quiesceIngestWithReprocess();

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
      throw new IOException(msg);
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
    if (ingestFailures > 10
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
      if (ingest != null) {
        ingest.stop();
        ingestThread.join();
        ingest = null;
        ingestThread = null;
      }
      currentIngestState = StandbyIngestState.NOT_INGESTING;

      LOG.info("Standby: Recovery - Purging all local logs");
      FSEditLog editLog = avatarNode.getFSImage().getEditLog();

      // ingest is down, no one should be writing to edit log
      if (editLog.isOpen()) {
        // end current log segment
        avatarNode.getFSImage().getEditLog().endCurrentLogSegment(true);
      }
      // remove all local logs
      editLog.purgeLogsOlderThan(Long.MAX_VALUE);
      // reset edit log state to last checkpointed txid
      editLog.resetTxIds(lastCorrectCheckpointTxId);

      ingestFailures = 0;
      LOG.info("Standby: Recovery - Completed");
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
        setupIngestStreamWithRetries(currentSegmentTxId);
        ingest = new Ingest(this, fsnamesys, confg, currentSegmentTxId);
        ingestThread = new Thread(ingest);
        ingestThread.start();
        currentIngestState = StandbyIngestState.INGESTING_EDITS;
      }
      LOG.info("Standby: Instatiated ingest for txid: " + currentSegmentTxId);
    } catch (IOException e) {
      ingestFailures++;
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
      ingestFailures++;
      throw new IOException(emsg);
    }
  }
  
  /**
   * Stop the standby, read all edit log segments.
   * @param lastTxId - the last txid to be consumed
   * @throws IOException
   */
  synchronized void quiesce(long lastTxId) throws IOException {
    if (currentIngestState == StandbyIngestState.STANDBY_QUIESCED) {
      LOG.info("Standby: Quiescing - already quiesced");
      return; // nothing to do
    }    
    // have to wait for the main thread to exit here
    // first stop the main thread before stopping the ingest thread
    LOG.info("Standby: Quiescing up to txid: " + lastTxId);
    running = false;
    InjectionHandler.processEvent(InjectionEvent.STANDBY_QUIESCE_INITIATED);
    fsnamesys.cancelSaveNamespace("Standby: Quiescing - Cancel save namespace");    
    interruptImageValidation();
    InjectionHandler.processEvent(InjectionEvent.STANDBY_QUIESCE_INTERRUPT);
   
    try {
      if (backgroundThread != null) {
        backgroundThread.join();
        backgroundThread = null;
      }
    } catch (InterruptedException e) {
      LOG.info("Standby: quiesce interrupted.");
      throw new IOException(e.getMessage());
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
    quiesceIngest(lastTxId);
    clearIngestState(currentSegmentTxId);
    
    // mark quiesce as completed
    LOG.info("Standby: Quiescing - Consumed transactions up to: "
        + getLastCorrectTxId() + ", requested: " + lastTxId);
    currentIngestState = StandbyIngestState.STANDBY_QUIESCED;
  }
  
  protected EditLogInputStream setupIngestStreamWithRetries(long txid)
      throws IOException {
    for (int i = 0; i < inputStreamRetries; i++) {
      try {
        return setupCurrentEditStream(txid);
      } catch (IOException e) {
        if (i == inputStreamRetries - 1) {
          throw new IOException("Cannot obtain stream for txid: " + txid, e);
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
      EditLogInputStream currentEditLogInputStream = remoteJournal
          .getInputStream(txid);
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
   * writes the in memory image of the local namenode to the fsimage
   * and tnen uploads this image to the primary namenode. The transaction 
   * log on the primary is purged too.
   */
  // DO NOT CHANGE THIS TO PUBLIC
  private void doCheckpoint() throws IOException {
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
        fsnamesys.saveNamespace(false, false);
        // get the new signature
        sig.mostRecentCheckpointTxId = fsImage.getEditLog().getLastWrittenTxId();
        sig.imageDigest = fsImage.storage.getCheckpointImageDigest(sig.mostRecentCheckpointTxId);
      } catch (SaveNamespaceCancelledException e) {
        InjectionHandler.processEvent(InjectionEvent.STANDBY_CANCELLED_EXCEPTION_THROWN);
        LOG.info("Standby: Checkpointing - cancelled saving namespace");
        throw e;
      } catch (IOException ex) {
        // Standby failed to save fsimage locally. Need to reinitialize
        String msg = "Standby: Checkpointing - failed to checkpoint itself, so " +
        		"no image can be uploaded to the primary. The only course of action " +
        		"is to start from the very beginning by reinitializing AvatarNode";
        LOG.error(msg, ex);
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
        throw ex;
      }
    } catch (IOException e) {
      LOG.error("Standby: Checkpointing - failed to complete the checkpoint: "
          + StringUtils.stringifyException(e));
      checkpointStatus("Checkpoint failed");
      InjectionHandler.processEvent(InjectionEvent.STANDBY_EXIT_CHECKPOINT_EXCEPTION, e);
      throw e;
    } finally {
      InjectionHandler.processEvent(InjectionEvent.STANDBY_EXIT_CHECKPOINT, this.sig);
      setLastRollSignature(null);
    }
  }

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
  
  private void finalizeCheckpoint(CheckpointSignature sig) 
      throws IOException{

    try {
      File imageFile = fsImage.storage.getFsImageName(sig.mostRecentCheckpointTxId);
      InjectionHandler.processEvent(InjectionEvent.STANDBY_BEFORE_PUT_IMAGE,
          imageFile);
      
      // start a thread to validate image while uploading the image to primary
      createImageValidation(imageFile);
      
      // copy image to primary namenode
      LOG.info("Standby: Checkpointing - Upload fsimage to remote namenode.");
      checkpointStatus("Image upload started");
      putFSImage(sig.mostRecentCheckpointTxId);
  
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
          + fsImage.getFsImageName().length());
      checkpointStatus("Completed");
    } finally {
      interruptImageValidation();
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
    checkpointTxnCount = conf.getLong("fs.checkpoint.txns", 40000);

    // initialize the webserver for uploading files.
    String infoAddr = 
      NetUtils.getServerAddress(conf,
                                "dfs.secondary.info.bindAddress",
                                "dfs.secondary.info.port",
                                "dfs.secondary.http.address");
    InetSocketAddress infoSocAddr = NetUtils.createSocketAddr(infoAddr);
    infoBindAddress = infoSocAddr.getHostName();
    int tmpInfoPort = infoSocAddr.getPort();
    infoServer = new HttpServer("secondary", infoBindAddress, tmpInfoPort,
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
    conf.set("dfs.secondary.http.address", infoBindAddress + ":" +infoPort);
    LOG.info("Secondary Web-server up at: " + infoBindAddress + ":" +infoPort);
    LOG.warn("Checkpoint Period   :" + checkpointPeriod + " secs " +
             "(" + checkpointPeriod/60 + " min)");
    LOG.warn("Log Size Trigger    :" + checkpointTxnCount + " transactions.");
  }

  /**
   * Copy the new fsimage into the NameNode
   */
  private void putFSImage(long txid) throws IOException {
    TransferFsImage.uploadImageFromStorage(fsName, machineName, infoPort,
        fsImage.storage, txid);
  }
  
  public void setLastRollSignature(CheckpointSignature sig) {
    this.sig = sig;
  }
  
  public CheckpointSignature getLastRollSignature() {
    return this.sig;
  }
  
  public boolean fellBehind() {
    synchronized (ingestStateLock) {
      switch (currentIngestState) {
      case INGESTING_EDITS:
        return ingest.isCatchingUp();
      case NOT_INGESTING: 
      case CHECKPOINTING:
        return true;
      case STANDBY_QUIESCED:
        return false;
      default:
        throw new IllegalStateException("Unknown ingest state: "
            + currentIngestState);
      }
    }
  }

  /**
   * Tells how far behind the standby is with consuming edits 
   * (only in progress segments).
   */
  public long getLagBytes() {
    return ingest == null ? -1 : this.ingest.getLagBytes();
  }


  protected void clearIngestState(long txid) {
    synchronized (ingestStateLock) {
      currentSegmentTxId = txid;
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
}
