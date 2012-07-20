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
import java.util.Date;
import java.lang.Thread;
import java.net.InetSocketAddress;

import org.apache.commons.logging.Log;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.*;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.net.DNS;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.hdfs.protocol.FSConstants;
import org.apache.hadoop.hdfs.server.protocol.NamenodeProtocol;
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
  private long checkpointSize;    // size (in MB) of current Edit Log
  private long lastCheckpointTime;
  private long earlyScheduledCheckpointTime = Long.MAX_VALUE;
  private long sleepBetweenErrors;
  private boolean checkpointEnabled;
  volatile private Thread backgroundThread;  // thread for secondary namenode 
  volatile private CheckpointSignature sig;
  private volatile String checkpointStatus;
  
  // two different types of ingested file
  public enum IngestFile { EDITS, EDITS_NEW };
  
  // allowed states of the ingest thread
  enum StandbyIngestState {
    NOT_INGESTING, 
    INGESTING_EDITS,
    QUIESCING_EDITS,
    CHECKPOINTING,
    INGESTING_EDITS_NEW,
    QUIESCING_EDITS_NEW,
    STANDBY_QUIESCED
  };
  
  // currently consumed ingest (edits, or edits.new)
  private volatile File currentIngestFile = null;
  protected volatile StandbyIngestState currentIngestState 
    = StandbyIngestState.NOT_INGESTING;
  protected Object ingestStateLock = new Object();
  private boolean lastFinalizeCheckpointFailed = false;
  
  // names of the edits files
  private final File editsFile;
  private final File editsFileNew;
  
  private final File tmpImageFileForValidation;

  // The Standby can either be processing transaction logs
  // from the primary namenode or it could be doing a checkpoint to upload a merged
  // fsimage to the primary.
  // The startupConf is the original configuration that was used to start the
  // AvatarNode. It is used by the secondary namenode to talk to the primary.
  // The "conf" is the configuration of the local standby namenode.
  //
  Standby(AvatarNode avatarNode, Configuration startupConf, Configuration conf) 
    throws IOException {
    this.running = true;
    this.avatarNode = avatarNode;
    this.confg = conf;
    this.startupConf = startupConf;
    this.fsImage = avatarNode.getFSImage();
    this.fsnamesys = avatarNode.getNamesystem();
    this.sleepBetweenErrors = startupConf.getInt("hdfs.avatarnode.sleep", 5000);
    initSecondary(startupConf); // start webserver for secondary namenode

    this.machineName =
      DNS.getDefaultHost(conf.get("dfs.namenode.dns.interface","default"),
                         conf.get("dfs.namenode.dns.nameserver","default"));
    LOG.info("machineName=" + machineName);
    
    this.editsFile = this.avatarNode.getRemoteEditsFile(conf);
    this.editsFileNew = this.avatarNode.getRemoteEditsFileNew(conf);
    
    InetSocketAddress addr = NameNode.getAddress(conf);
    this.tmpImageFileForValidation = new File("/tmp", 
        "hadoop_image." + addr.getHostName() + ":" + addr.getPort());
    checkpointStatus("No checkpoint initiated");
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
        // Check to see if the primary is somehow checkpointing itself. If so, then 
        // exit the StandbyNode, we cannot have two daemons checkpointing the same
        // namespace at the same time
        if (hasStaleCheckpoint()) {
          backgroundThread = null;
          quiesce(AvatarNode.TXID_IGNORE);
          break;
        }

        if (lastCheckpointTime == 0 ||
            (lastCheckpointTime + 1000 * checkpointPeriod < now) ||
            (earlyScheduledCheckpointTime < now) ||
            avatarNode.editSize(confg) > checkpointSize) {

          // schedule an early checkpoint if this current one fails.
          earlyScheduledCheckpointTime = now + CHECKPOINT_DELAY;
          doCheckpoint();
          earlyScheduledCheckpointTime = Long.MAX_VALUE;
          lastCheckpointTime = now;

          InjectionHandler
              .processEvent(InjectionEvent.STANDBY_AFTER_DO_CHECKPOINT);
          // set the last expected checkpoint time on the primary.
          avatarNode.setStartCheckpointTime(startupConf);
        }

        // if edit and edits.new both exists, then we schedule a checkpoint
        // to occur very soon.
        // Only reschedule checkpoint if it is not scheduled to occur even sooner
        if ((avatarNode.twoEditsFile(startupConf)) &&
                (earlyScheduledCheckpointTime > now + CHECKPOINT_DELAY)) {
          LOG.warn("Standby: edits and edits.new found, scheduling early checkpoint.");
          earlyScheduledCheckpointTime = now + CHECKPOINT_DELAY;
        }

        // if the checkpoint creation has switched off ingesting, then we restart the
        // ingestion here.
        if (ingest == null) {
          InjectionHandler.processEvent(InjectionEvent.STANDBY_CREATE_INGEST_RUNLOOP);
          instantiateIngest(IngestFile.EDITS);
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
          clearIngestState();
        }
      } catch (Throwable e) {
        LOG.warn("Standby: encounter exception ", e);
        running = false;
      }
    }
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
   * Quiesces the ingest for the given file typ
   * 
   * @param type ingest to quiesce
   * @param sig signature for quiescing (checkpointing)
   */
  private void quiesceIngest(IngestFile type, CheckpointSignature sig) 
      throws IOException {  
    File edits; 
    InjectionHandler.processEvent(InjectionEvent.STANDBY_QUIESCE_INGEST);
    synchronized (ingestStateLock) {
      if (type == IngestFile.EDITS) {
        assertState(StandbyIngestState.INGESTING_EDITS,
            StandbyIngestState.QUIESCING_EDITS);
      } else {
        assertState(StandbyIngestState.INGESTING_EDITS_NEW,
            StandbyIngestState.QUIESCING_EDITS_NEW);
      }
      edits = getIngestFile(type);
      currentIngestState = (type == IngestFile.EDITS)
          ? StandbyIngestState.QUIESCING_EDITS
          : StandbyIngestState.QUIESCING_EDITS_NEW;
      ingest.quiesce(sig);       
    } 
    try {
      ingestThread.join();
      currentIngestState = StandbyIngestState.NOT_INGESTING;
      LOG.info("Standby: Quiesce - Ingest thread for " 
          + edits.getName() + " exited.");
    } catch (InterruptedException e) {
      LOG.info("Standby: Quiesce - Ingest thread interrupted.");
      throw new IOException(e.getMessage());
    }
  }
  
  /**
   * Instantiates ingest thread for the given edits file type
   * 
   * @param type (EDITS, EDITS_NEW)
   */
  private void instantiateIngest(IngestFile type)
      throws IOException {
    File edits;
    InjectionHandler.processEvent(InjectionEvent.STANDBY_INSTANTIATE_INGEST);
    synchronized (ingestStateLock) {
      assertState(StandbyIngestState.NOT_INGESTING);
      edits = getIngestFile(type);
      // if the file does not exist, 
      // do not change the state
      if (!edits.exists()
          || InjectionHandler
              .falseCondition(InjectionEvent.STANDBY_EDITS_NOT_EXISTS, type)) {
        return;
      }
      setCurrentIngestFile(edits);
      ingest = new Ingest(this, fsnamesys, confg, edits);
      ingestThread = new Thread(ingest);
      ingestThread.start(); 
      currentIngestState = type == IngestFile.EDITS
          ? StandbyIngestState.INGESTING_EDITS
          : StandbyIngestState.INGESTING_EDITS_NEW;
    } 
    LOG.info("Standby: Instantiated ingest for edits file: " + edits.getName());
  }
  
  /**
   * Processes a given edit file type.
   * Method to be used when quiescing the standby!
   * 
   * @param type (EDITS, EDITS_NEW)
   */
  private int processIngestFileForQuiescing(IngestFile type) 
      throws IOException {
    boolean editsNew = type == IngestFile.EDITS_NEW;
    assertState(StandbyIngestState.NOT_INGESTING,
        editsNew ? StandbyIngestState.INGESTING_EDITS_NEW
            : StandbyIngestState.INGESTING_EDITS,
        editsNew ? StandbyIngestState.QUIESCING_EDITS_NEW
            : StandbyIngestState.QUIESCING_EDITS);

    
    if (ingest == null) {
      instantiateIngest(type);
    }
    quiesceIngest(type, null);
    return ingest.getLogVersion();
  }

  /**
   * Processes previously consumed edits file
   * Method to be used when quiescing the standby!
   * 
   * @param type (EDITS, EDITS_NEW)
   */
  private void reprocessIngestFileForQuiescing(IngestFile type) 
      throws IOException{
    assertState(StandbyIngestState.NOT_INGESTING);
    File edits = getIngestFile(type);
    LOG.info("Standby: Quiesce - reprocessing edits file: " + edits.getName());
    if (!edits.exists()) {
      LOG.warn("Standby: Quiesce - reprocessing edits file - edits file: "
          + edits.getName() + " does not exists.");
      return;
    }
    instantiateIngest(type);
    quiesceIngest(type, null);
    
    // verify that the entire transaction log was truly consumed
    // when re-processing, if we fail here, we cannot do anything
    // better than fail
    if(!ingest.getIngestStatus()){
      String emsg = "Standby: Quiesce could not successfully ingest " 
          + edits.getName() + " transaction log.";
      LOG.warn(emsg);
      throw new IOException(emsg);
    }
    clearIngestState();
  }
  
  //
  // stop checkpointing, read edit and edits.new(if it exists) 
  // into local namenode
  //
  synchronized void quiesce(long lastTxId) throws IOException {
    if (currentIngestState == StandbyIngestState.STANDBY_QUIESCED) {
      LOG.info("Standby: Quiescing - already quiesced");
      return; // nothing to do
    }
    // have to wait for the main thread to exit here
    // first stop the main thread before stopping the ingest thread
    LOG.info("Standby: Quiesce - starting");
    running = false;
    fsnamesys.cancelSaveNamespace("Standby: Quiescing - Cancel save namespace");
    InjectionHandler.processEvent(InjectionEvent.STANDBY_INTERRUPT);
    
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

    int logVersion = 0;
    boolean reprocessEdits = false;
    
    // handle "edits"
    if (!isCurrentEditsNew()) {      
      // assert correct state
      logVersion = processIngestFileForQuiescing(IngestFile.EDITS);
      // reprocess edits if failed
      if (!ingest.getIngestStatus()){
        clearIngestState();
        reprocessIngestFileForQuiescing(IngestFile.EDITS);
      }
      clearIngestState();
      reprocessEdits = true;
    }
    
    // if the transactions don't match,
    // there is most probably edits.new
    if (!transactionsMatch(lastTxId, logVersion)) {
      pollEditsNew(30);
    }
    
    // handle "edits.new"
    if (editsNewExists()) {
      logVersion = processIngestFileForQuiescing(IngestFile.EDITS_NEW);
      clearIngestState();
    } 

    // if for some reason we did not succeed,
    // or the last transaction doesn't match
    // try to re-read both files and replay the logs
    // (skips transactions applied before)
    if ((ingest != null && !ingest.getIngestStatus()) 
        || (!transactionsMatch(lastTxId, logVersion))) {       
      // try to reopen the logs and re-read them
      if (reprocessEdits) { // re-read edits, if needed
        reprocessIngestFileForQuiescing(IngestFile.EDITS);
      }
      reprocessIngestFileForQuiescing(IngestFile.EDITS_NEW);
    } 
    
    // final sanity verification of transaction id's
    if(!transactionsMatch(lastTxId, logVersion)){
      String emsg = "Standby: Quiesce - could not successfully ingest " 
          + " transaction logs. Transaction Mismatch: " + lastTxId 
          + " avatar txid: " + avatarNode.getLastWrittenTxId();
      LOG.warn(emsg);
      throw new IOException(emsg);
    }  
    clearIngestState();
    // mark quiesce as completed
    LOG.info("Standby: Quiesce - completed");
    currentIngestState = StandbyIngestState.STANDBY_QUIESCED;
  }
  
  /**
   * Check if the given transaction is the last one applied
   * 
   * @param lastTxId given last transaction
   * @return true if the given transaction is the last one or should be ignored,
   * or the layout version does not support transaction ids
   */
  private boolean transactionsMatch(long lastTxId, int logVersion){
    return (lastTxId == AvatarNode.TXID_IGNORE) 
        || (logVersion > FSConstants.STORED_TXIDS)
        || (lastTxId == avatarNode.getLastWrittenTxId());
  }

  /**
   * Check to see if the remote namenode is doing its own checkpointing. This can happen 
   * when the remote namenode is restarted. This method returns true if the remote 
   * namenode has done an unexpected checkpoint. This method retrieves the fstime of the
   * remote namenode and matches it against the fstime of the checkpoint when this
   * AvatarNode did a full-sync of the edits log. It also matches the size of
   * both the images. The reason for this is as follows :
   *
   * Just after a checkpoint is done there is small duration of time when the
   * remote and local fstime don't match even for a good checkpoint, but
   * fortunately both the images do match and we should check whether both
   * have the same size. Note that even if this check does not catch a stale
   * checkpoint (in the rare case where both images have the same length but
   * are not the same), our transaction id based verification will definitely
   * catch this issue.
   */
  boolean hasStaleCheckpoint() throws IOException {
    long remotefsTime = avatarNode.readRemoteFstime(startupConf);
    long localfsTime = avatarNode.getStartCheckpointTime();
    long remoteImageSize = avatarNode.getRemoteImageFile(startupConf).length();
    long localImageSize = avatarNode.getAvatarImageFile(startupConf).length();
    if (remotefsTime != localfsTime && remoteImageSize != localImageSize) {
      LOG.warn("Standby: The remote active namenode might have been restarted.");
      LOG.warn("Standby: The fstime of checkpoint from which the Standby was created is " +
               AvatarNode.dateForm.format(new Date(localfsTime)) +
               " but remote fstime is " + 
               AvatarNode.dateForm.format(new Date(remotefsTime)));
      avatarNode.doRestart();
      return true;
    }
    return false;
  }
  
  private void pollEditsNew(int numRetries) throws IOException {
    for (int i = 0; i < numRetries; i++) {
      if (editsNewExists())
        break;
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        throw new IOException("Standby: - received interruption");
      }
      LOG.info("Standby: - retrying to check if edits.new exists... try: "
          + i);
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
  public void doCheckpoint() throws IOException {
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
        sig = (CheckpointSignature) primaryNamenode.rollEditLog();
      } catch (IOException ex) {
        // In this case we can return since we did not kill the Ingest thread yet
        // Nothing prevents us from doing the next checkpoint attempt
        checkpointStatus("Checkpoint failed");
        LOG.warn("Standby: Checkpointing - roll Edits on the primary node failed.");
        return;
      }
      
      if (this.sig != null && this.sig.equals(sig)
          && lastFinalizeCheckpointFailed) {
        // previous checkpoint failed, maybe we have the image saved?
        LOG.info("Standby: Checkpointing - retrying to finalize previous checkpoint");
        try {
          finalizeCheckpoint(sig);
        } catch (IOException ex){
          LOG.error("Standby: Checkpointing - can't finalize previous checkpoing, "
              + "will retry later.");
          lastFinalizeCheckpointFailed = true;
          throw ex;
        }
        return;
      } else if (this.sig != null && lastFinalizeCheckpointFailed) {
        // last checkpoint did not succeed, but the primary has
        // been checkpointed in the meantime
        throw new RuntimeException(
            "Last checkpoint did not succeed, but the signatures do not match. "
                + "The primary was checkpointed in the meantime.");
      }
      setLastRollSignature(sig);   
      
      // Ingest till end of edits log
      if (ingest == null) {
        LOG.info("Standby: Checkpointing - creating ingest thread to process all transactions.");
        instantiateIngest(IngestFile.EDITS);
      }  
      
      checkpointStatus("Quiescing ingest");
      quiesceIngest(IngestFile.EDITS, sig);    
      LOG.info("Standby: Checkpointing - finished quitting ingest thread just before ckpt.");
      
      if (!ingest.getIngestStatus()) {
        checkpointStatus("Re-quiescing ingest");
        // try to reopen the log and re-read it
        instantiateIngest(IngestFile.EDITS);
        quiesceIngest(IngestFile.EDITS, sig);
      }
      
      assertState(StandbyIngestState.NOT_INGESTING);
      
      if (!ingest.getIngestStatus()) {
        clearIngestState();
        String emsg = "Standby: Checkpointing - could not ingest transaction log.";
        emsg += " This is real bad because we do not know how much edits we have consumed.";
        emsg += " It is better to exit the AvatarNode here.";
        LOG.error(emsg);
        throw new RuntimeException(emsg);
      }  
      clearIngestState();
      
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
        // roll transaction logs on local namenode
        LOG.info("Standby: Close editlog on local namenode.");
        fsImage.getEditLog().close();
    
        // save a checkpoint of the current namespace of the local Namenode
        // We should ideally use fsnamesystem.saveNamespace but that works
        // only if namenode is not in safemode.
        LOG.info("Standby: Checkpointing - save fsimage on local namenode.");
        checkpointStatus("Saving namespace started");
        fsnamesys.saveNamespace(false, false);
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
      
      pollEditsNew(30);
      
      // we can start the ingest again for edits.new!!!
      instantiateIngest(IngestFile.EDITS_NEW);
      
      try {
        finalizeCheckpoint(sig);
      } catch (IOException ex) {
        // If the rollFsImage has actually succeeded on the Primary, but
        // returned with the exception on recreation our Ingest will throw
        // a runtime exception and the Avatar will be restarted.
        LOG.error("Standby: Checkpointing - rolling the fsimage " +
            "on the Primary node failed.", ex);
        lastFinalizeCheckpointFailed = true;
        throw ex;
      }
    } catch (IOException e) {
      LOG.error("Standby: Checkpointing - failed to complete the checkpoint: "
          + StringUtils.stringifyException(e));
      checkpointStatus("Checkpoint failed");
      throw e;
    } finally {
      InjectionHandler.processEvent(InjectionEvent.STANDBY_EXIT_CHECKPOINT, this.sig);
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
        error = e;
      }
    }
  }
  
  private void finalizeCheckpoint(CheckpointSignature sig) 
      throws IOException{

    File[] imageFiles = fsImage.getImageFiles();
    if (imageFiles.length == 0) {
      throw new IOException("No good image is left");
    }
    File imageFile = imageFiles[0];
    InjectionHandler.processEvent(InjectionEvent.STANDBY_BEFORE_PUT_IMAGE,
        imageFile);
    
    // start a thread to validate image while uploading the image to primary
    ImageValidator imageValidator = new ImageValidator(imageFile);
    imageValidator.run();
    
    // copy image to primary namenode
    LOG.info("Standby: Checkpointing - Upload fsimage to remote namenode.");
    checkpointStatus("Image upload started");
    putFSImage(sig);

    // check if the image is valid
    try {
      imageValidator.join();
    } catch (InterruptedException ie) {
      throw (IOException)new InterruptedIOException().initCause(ie);
    }
    if (!imageValidator.succeeded) {
      throw new IOException("Image file validation failed", imageValidator.error);
    }
    
    // make transaction to primary namenode to switch edit logs
    LOG.info("Standby: Checkpointing - Roll fsimage on primary namenode.");
    InjectionHandler.processEventIO(InjectionEvent.STANDBY_BEFORE_ROLL_IMAGE);
      
    assertState(
        StandbyIngestState.NOT_INGESTING,
        StandbyIngestState.INGESTING_EDITS_NEW);
    
    // we might concurrently reopen ingested file because of 
    // checksum error
    synchronized (ingestStateLock) {
      boolean editsNewExisted = editsNewExists();
      try {
        primaryNamenode.rollFsImage(new CheckpointSignature(fsImage));
      } catch (IOException e) {
        if (editsNewExisted && !editsNewExists()
            && currentIngestState == StandbyIngestState.INGESTING_EDITS_NEW) {
          // we were ingesting edits.new
          // the roll did not succeed but edits.new does not exist anymore          
          // assume that the roll succeeded   
          LOG.warn("Roll did not succeed but edits.new does not exist!!! - assuming roll succeeded", e);
        } else {
          throw e;
        }
      }
      // after successful roll edits.new is rolled to edits
      // and we should be consuming it
      setCurrentIngestFile(editsFile);
      if (currentIngestState == StandbyIngestState.INGESTING_EDITS_NEW) {
        // 1) We currently consume edits.new - do the swap
        currentIngestState = StandbyIngestState.INGESTING_EDITS;
      } // 2) otherwise we don't consume anything - do not change the state
    }
    setLastRollSignature(null);
    lastFinalizeCheckpointFailed = false;
    
    LOG.info("Standby: Checkpointing - Checkpoint done. New Image Size: "
        + fsImage.getFsImageName().length());
    checkpointStatus("Completed");
  }

  /**
   * Initialize the webserver so that the primary namenode can fetch
   * transaction logs from standby via http.
   */
  void initSecondary(Configuration conf) throws IOException {

    nameNodeAddr = avatarNode.getRemoteNamenodeAddress(conf);
    this.primaryNamenode =
        (NamenodeProtocol) RPC.waitForProxy(NamenodeProtocol.class,
            NamenodeProtocol.versionID, nameNodeAddr, conf);

    fsName = avatarNode.getRemoteNamenodeHttpName(conf);

    // Initialize other scheduling parameters from the configuration
    checkpointEnabled = conf.getBoolean("fs.checkpoint.enabled", false);
    checkpointPeriod = conf.getLong("fs.checkpoint.period", 3600);
    checkpointSize = conf.getLong("fs.checkpoint.size", 4194304);

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

    // The web-server port can be ephemeral... ensure we have the correct info
    infoPort = infoServer.getPort();
    conf.set("dfs.secondary.http.address", infoBindAddress + ":" +infoPort);
    LOG.info("Secondary Web-server up at: " + infoBindAddress + ":" +infoPort);
    LOG.warn("Checkpoint Period   :" + checkpointPeriod + " secs " +
             "(" + checkpointPeriod/60 + " min)");
    LOG.warn("Log Size Trigger    :" + checkpointSize + " bytes " +
             "(" + checkpointSize/1024 + " KB)");
  }

  /**
   * Copy the new fsimage into the NameNode
   */
  private void putFSImage(CheckpointSignature sig) throws IOException {
    String fileid = "putimage=1&port=" + infoPort +
      "&machine=" +
      machineName +
      "&token=" + sig.toString();
    LOG.info("Standby: Posted URL " + fsName + fileid);
    TransferFsImage.getFileClient(fsName, fileid, (File[])null, false);
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
      case QUIESCING_EDITS:
        return editsNewExists() ? true : ingest.catchingUp();
      case INGESTING_EDITS_NEW:
      case QUIESCING_EDITS_NEW:
        return ingest.catchingUp();
      case NOT_INGESTING: 
      case CHECKPOINTING:
        return true;
      case STANDBY_QUIESCED:
        return false;
      default:
        throw new IllegalStateException("Unknown ingest state: "
            + currentIngestFile);
      }
    }
  }

  public long getLagBytes() {
    if (this.ingest == null) {
      if (currentIngestState == StandbyIngestState.CHECKPOINTING) {
        try {
          // If it's checkpointing, the primary is writing to edits.new
          File edits = avatarNode.getRemoteEditsFileNew(startupConf);
          return edits.length();
        } catch (IOException e) {
          LOG.error("Fail to get lagbytes", e);
          return -1;
        }
      } else {
        // two rare cases could come here: quiesce and error, no good value
        // could return
        return -1;
      }
    }
    return this.ingest.getLagBytes();
  }

  private void clearIngestState() {
    synchronized (ingestStateLock) {
      currentIngestState = StandbyIngestState.NOT_INGESTING;
      ingest = null;
      ingestThread = null;
      setCurrentIngestFile(null);
    }
  }
  
  /**
   * Set the edits file that the current ingest thread is processing
   * Set appropriate state to indicate which file is being ingested.
   * 
   * @param file - current ingest file
   */
  private void setCurrentIngestFile(File file) {
    currentIngestFile = file;
  }
  
  /**
   * Get the current ingest file
   */
  public File getCurrentIngestFile() {
    return currentIngestFile;
  }
  
  /**
   * Returns true if we are currently processing edits.new
   * Should be avoided because it compares strings!!!
   * @return true if we are consuming edits.new, false otherwise
   */
  private boolean isCurrentEditsNew() {
    // if nothing set, assume that we need to process edits
    synchronized (ingestStateLock) {
      return currentIngestFile == null ? false : currentIngestFile
          .equals(editsFileNew);
    }
  }
  
  private boolean editsNewExists() { 
    return editsFileNew.exists() || editsFileNew.length() > 0;
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
   * Helper function which returns the ingest file for the given typ
   * @param type type (EDITS, EDITS.NEW)
   * @return the corresponding ingest file
   */
  private File getIngestFile(IngestFile type) {
    return type == IngestFile.EDITS ? editsFile : editsFileNew;
  } 
}
