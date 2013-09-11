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

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.protocol.FSConstants;
import org.apache.hadoop.hdfs.protocol.LayoutVersion;
import org.apache.hadoop.hdfs.protocol.LayoutVersion.Feature;
import org.apache.hadoop.hdfs.server.common.HdfsConstants.Transition;
import org.apache.hadoop.hdfs.server.common.InconsistentFSStateException;
import org.apache.hadoop.hdfs.server.common.Storage;
import org.apache.hadoop.hdfs.server.common.Storage.FormatConfirmable;
import org.apache.hadoop.hdfs.server.common.Storage.StorageDirType;
import org.apache.hadoop.hdfs.server.common.Storage.StorageDirectory;
import org.apache.hadoop.hdfs.server.common.Storage.StorageState;
import org.apache.hadoop.hdfs.server.common.StorageInfo;
import org.apache.hadoop.hdfs.server.common.HdfsConstants.StartupOption;
import org.apache.hadoop.hdfs.server.namenode.NNStorage.NameNodeDirType;
import org.apache.hadoop.hdfs.server.namenode.NNStorage.StorageLocationType;
import org.apache.hadoop.hdfs.server.namenode.ValidateNamespaceDirPolicy.NNStorageLocation;
import org.apache.hadoop.hdfs.server.namenode.metrics.NameNodeMetrics;
import org.apache.hadoop.hdfs.server.protocol.RemoteEditLog;
import org.apache.hadoop.hdfs.server.protocol.RemoteEditLogManifest;
import org.apache.hadoop.hdfs.util.InjectionEvent;
import org.apache.hadoop.hdfs.util.MD5FileUtils;
import org.apache.hadoop.io.MD5Hash;
import org.apache.hadoop.util.FlushableLogger;
import org.apache.hadoop.util.InjectionEventI;
import org.apache.hadoop.util.InjectionHandler;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

/**
 * FSImage handles checkpointing and logging of the namespace edits.
 * 
 */
public class FSImage {

  static final Log LOG = LogFactory.getLog(FSImage.class.getName());
  
  // immediate flush logger
  private static final Log FLOG = FlushableLogger.getLogger(LOG);
  
  NNStorage storage;
  Configuration conf;
  private NNStorageRetentionManager archivalManager;
  
  private final SaveNamespaceContext saveNamespaceContext 
    = new SaveNamespaceContext();
  
  protected FSNamesystem namesystem = null;
  FSEditLog editLog = null;
  ImageSet imageSet = null;
  private boolean isUpgradeFinalized = false;
  
  private NameNodeMetrics metrics = NameNode.getNameNodeMetrics();
  
  // intermediate buffer size for image saving and loading
  public static int LOAD_SAVE_BUFFER_SIZE = 4 * 1024 * 1024; // 4MB
  // chunk size for copying out or into the intermediate buffer
  public static int LOAD_SAVE_CHUNK_SIZE = 512 * 1024; // 512KB

  /**
   * Constructor
   * @param conf Configuration
   */
  FSImage(Configuration conf) throws IOException {
    storage = new NNStorage(new StorageInfo());
    
    this.editLog = new FSEditLog(conf, this, storage,
        NNStorageConfiguration.getNamespaceDirs(conf),
        NNStorageConfiguration.getNamespaceEditsDirs(conf), null);
    
    this.imageSet = new ImageSet(this, null, null, metrics);
    setFSNamesystem(null);
    this.conf = conf;
    archivalManager = new NNStorageRetentionManager(conf, storage, editLog);
  }
  
  /**
   */
  FSImage(Configuration conf, Collection<URI> fsDirs,
      Collection<URI> fsEditsDirs, Map<URI, NNStorageLocation> locationMap)
      throws IOException {
    this.conf = conf;
    storage = new NNStorage(conf, fsDirs, fsEditsDirs, locationMap);
    this.editLog = new FSEditLog(conf, this, storage, fsDirs, fsEditsDirs,
        locationMap);
    this.imageSet = new ImageSet(this, fsDirs, fsEditsDirs, metrics);
    archivalManager = new NNStorageRetentionManager(conf, storage, editLog);
  }

  public boolean failOnTxIdMismatch() {
    if (namesystem == null) {
      return true;
    } else {
      return namesystem.failOnTxIdMismatch();
    }
  }

  protected FSNamesystem getFSNamesystem() {
    return namesystem;
  }
  
  protected void setFSNamesystem(FSNamesystem ns) {
    namesystem = ns;
  }
  
  public long getLastAppliedTxId() {
    return editLog.getLastWrittenTxId();
  }
  
  List<StorageDirectory> getRemovedStorageDirs() {
	  return storage.getRemovedStorageDirs();
  }

  /**
   * Get the MD5 digest of the current image
   * @return the MD5 digest of the current image
   */ 
  MD5Hash getImageDigest(long txid) throws IOException {
    return storage.getCheckpointImageDigest(txid);
  }

  void setImageDigest(long txid, MD5Hash imageDigest) throws IOException {
    this.storage.setCheckpointImageDigest(txid, imageDigest);
  }
  
  private void throwIOException(String msg) throws IOException {
    LOG.error(msg);
    throw new IOException(msg);
  }
  
  private void updateRemoteStates(
      Map<ImageManager, RemoteStorageState> remoteImageStates,
      Map<JournalManager, RemoteStorageState> remoteJournalStates,
      List<ImageManager> nonFileImageManagers,
      List<JournalManager> nonFileJournalManagers)
      throws IOException {
    // / analyze non file storage location

    // List non-file storage
    FLOG.info("Startup: non-file image managers:");
    for (ImageManager im : nonFileImageManagers) {
      RemoteStorageState st = im.analyzeImageStorage();
      FLOG.info("-> Image Manager: " + im + " state: " + st.getStorageState());
      if (st.getStorageState() == StorageState.INCONSISTENT) {
        throwIOException("Image manager has inconsistent state: " + im
            + ", state: " + st.getStorageState());
      }
      remoteImageStates.put(im, st);
    }

    FLOG.info("Startup: non-file journal managers:");
    for (JournalManager jm : nonFileJournalManagers) {
      RemoteStorageState st = jm.analyzeJournalStorage();
      FLOG.info("-> Journal Manager: " + jm + " state: " + st.getStorageState());
      if (st.getStorageState() == StorageState.INCONSISTENT) {
        throwIOException("Journal manager has inconsistent state: " + jm
            + ", state: " + st.getStorageState());
      }
      remoteJournalStates.put(jm, st);
    }
  }

  /**
   * Analyze storage directories.
   * Recover from previous transitions if required. 
   * Perform fs state transition if necessary depending on the namespace info.
   * Read storage info. 
   * 
   * @throws IOException
   * @return true if the image needs to be saved or false otherwise
   */
  public boolean recoverTransitionRead(StartupOption startOpt)
      throws IOException {
    
    FLOG.info("Startup: recovering namenode storage");
    
    assert startOpt != StartupOption.FORMAT : 
      "NameNode formatting should be performed before reading the image";
    
    Collection<File> imageDirs = storage.getImageDirectories();

    // none of the data dirs exist
    if(imageDirs.size() == 0 && startOpt != StartupOption.IMPORT)  
      throw new IOException(
          "All specified directories are not accessible or do not exist.");
    editLog.checkJournals();
    
    storage.setUpgradeManager(namesystem.upgradeManager);
    
    Map<ImageManager, RemoteStorageState> remoteImageStates = Maps.newHashMap();
    Map<JournalManager, RemoteStorageState> remoteJournalStates = Maps.newHashMap();

    List<ImageManager> nonFileImageManagers = getNonFileImageManagers();
    List<JournalManager> nonFileJournalManagers = getNonFileJournalManagers();

    updateRemoteStates(remoteImageStates, remoteJournalStates,
        nonFileImageManagers, nonFileJournalManagers);
    
    // number of non-file storage locations
    int nonFileStorageLocations = nonFileImageManagers.size()
        + nonFileJournalManagers.size();
    
    FLOG.info("Startup: checking storage directory state.");
    // 1. For each data directory calculate its state and 
    // check whether all is consistent before transitioning.
    Map<StorageDirectory, StorageState> dataDirStates = 
             new HashMap<StorageDirectory, StorageState>();
    boolean isFormatted = recoverStorageDirs(startOpt, dataDirStates);
    
    // Recover the non-file storage locations.
    editLog.transitionNonFileJournals(null, false,
        Transition.RECOVER, startOpt);
    imageSet.transitionNonFileImages(null, false, Transition.RECOVER,
        startOpt);

    if (!isFormatted && startOpt != StartupOption.ROLLBACK 
                     && startOpt != StartupOption.IMPORT) {
      for(Entry<StorageDirectory, StorageState> e : dataDirStates.entrySet()) {
        LOG.info("State : " + e.getKey().getCurrentDir() + " state: " +e.getValue());
      }
      throw new IOException("NameNode is not formatted." + dataDirStates);      
    }


    int layoutVersion = storage.getLayoutVersion();
    if (layoutVersion < Storage.LAST_PRE_UPGRADE_LAYOUT_VERSION) {
      NNStorage.checkVersionUpgradable(storage.getLayoutVersion());
    }
    if (startOpt != StartupOption.UPGRADE
        && layoutVersion < Storage.LAST_PRE_UPGRADE_LAYOUT_VERSION
        && layoutVersion != FSConstants.LAYOUT_VERSION) {
      throw new IOException(
          "\nFile system image contains an old layout version " 
          + storage.getLayoutVersion() + ".\nAn upgrade to version "
          + FSConstants.LAYOUT_VERSION + " is required.\n"
          + "Please restart NameNode with -upgrade option.");
    }
       
    editLog.updateNamespaceInfo(storage);

    // check whether distributed upgrade is required and/or should be continued
    storage.verifyDistributedUpgradeProgress(startOpt);

    FLOG.info("Startup: formatting unformatted directories.");
    
    // 2. Format unformatted dirs.
    for (Iterator<StorageDirectory> it = storage.dirIterator(); it.hasNext();) {
      StorageDirectory sd = it.next();
      StorageState curState = dataDirStates.get(sd);
      switch(curState) {
      case NON_EXISTENT:
        throw new IOException(StorageState.NON_EXISTENT + 
                              " state cannot be here");
      case NOT_FORMATTED:        
        LOG.info("Storage directory " + sd.getRoot() + " is not formatted.");
        if (!sd.isEmpty()) {
          LOG.error("Storage directory " + sd.getRoot()
            + " is not empty, and will not be formatted! Exiting.");
          throw new IOException(
            "Storage directory " + sd.getRoot() + " is not empty!");
        }   
        LOG.info("Formatting ...");
        sd.clearDirectory(); // create empty currrent dir
        break;
      default:
        break;
      }
    }
    
    // check non-file images
    for (Entry<ImageManager, RemoteStorageState> e : remoteImageStates
        .entrySet()) {
      checkAllowedNonFileState(e.getValue().getStorageState(), e.getKey());
    }
    // check non-file journals
    for (Entry<JournalManager, RemoteStorageState> e : remoteJournalStates
        .entrySet()) {
      checkAllowedNonFileState(e.getValue().getStorageState(), e.getKey());
    }  

    FLOG.info("Startup: Transitions.");
    // 3. Do transitions
    switch(startOpt) {
    case UPGRADE:
      doUpgrade();
      return false; // upgrade saved image already
    case IMPORT:
      doImportCheckpoint();
      if (nonFileStorageLocations > 0) {
        throwIOException("Import not supported for non-file storage");
      }
      return false; // import checkpoint saved image already
    case ROLLBACK:
      doRollback(remoteImageStates, remoteJournalStates);
      // Update the states since the remote states have changed after rollback.
      updateRemoteStates(remoteImageStates, remoteJournalStates,
          nonFileImageManagers, nonFileJournalManagers);
      InjectionHandler.processEvent(InjectionEvent.FSIMAGE_ROLLBACK_DONE);
      break;
    case REGULAR:
      // just load the image
    }
    
    if (inUpgradeStatus()) {
      namesystem.setUpgradeStartTime(FSNamesystem.now());
    }
    
    // final consistency check for non-file images and journals
    // read version file first
    FSImageStorageInspector inspector = storage.readAndInspectDirs();
    
    FLOG.info("Startup: starting with storage info: " + storage.toColonSeparatedString());
    
    // format unformatted journals and images
    // check if the formatted ones are consistent with local storage
    for (Entry<ImageManager, RemoteStorageState> e : remoteImageStates
        .entrySet()) {
      if (e.getValue().getStorageState() != StorageState.NORMAL) {
        LOG.info("Formatting remote image: " + e.getKey());
        e.getKey().transitionImage(storage, Transition.FORMAT, null);
      } else {
        checkConsistency(e.getValue().getStorageInfo(), storage, true, e.getKey());
      }
    } 
    for (Entry<JournalManager, RemoteStorageState> e : remoteJournalStates
        .entrySet()) {
      if (e.getValue().getStorageState() != StorageState.NORMAL) {
        LOG.info("Formatting remote journal: " + e.getKey());
        e.getKey().transitionJournal(storage, Transition.FORMAT, null);
      } else {
        checkConsistency(e.getValue().getStorageInfo(), storage, true, e.getKey());
      }
    }

    // load the image
    return loadFSImage(inspector);
  }

  /**
   * Check if the remote image/journal storage info is the same as ours
   */
  private void checkConsistency(StorageInfo remote, StorageInfo local,
      boolean image, Object name) throws IOException {
    if (!remote.equals(local)) {
      throwIOException("Remote " + (image ? "image" : "edits")
          + " storage is different than local. Local: ("
          + local.toColonSeparatedString() + "), remote: " + name.toString()
          + " (" + remote.toColonSeparatedString() + ")");
    }
  }
  
  /**
   * Check if remote image/journal storage is in allowed state.
   */
  private void checkAllowedNonFileState(StorageState curState, Object name)
      throws IOException {
    switch (curState) {
    case NON_EXISTENT:
    case NOT_FORMATTED:
    case NORMAL:
      break;
    default:
      throwIOException("ImageManager bad state: " + curState + " for: "
          + name.toString());
    }
  }
  
  /**
   * @return true if Nn is under upgrade.
   */
  private boolean inUpgradeStatus() {
    for (Iterator <StorageDirectory> it = storage.dirIterator(); it.hasNext();) {
      StorageDirectory sd = it.next();
      File preDir = sd.getPreviousDir();
      if (preDir.exists()) {
        return true;
      }
    }
    return false;
  }
  
  /**
   * For each storage directory, performs recovery of incomplete transitions
   * (eg. upgrade, rollback, checkpoint) and inserts the directory's storage
   * state into the dataDirStates map.
   * @param dataDirStates output of storage directory states
   * @return true if there is at least one valid formatted storage directory
   */
  private boolean recoverStorageDirs(StartupOption startOpt,
      Map<StorageDirectory, StorageState> dataDirStates) throws IOException {
    boolean isFormatted = false;
    for (Iterator<StorageDirectory> it = 
                      storage.dirIterator(); it.hasNext();) {
      StorageDirectory sd = it.next();
      StorageState curState;
      try {
        curState = sd.analyzeStorage(startOpt);
        isFormatted |= NNStorage.recoverDirectory(sd, startOpt, curState, true);
      } catch (IOException ioe) {
        sd.unlock();
        throw ioe;
      }
      dataDirStates.put(sd,curState);
    }
    return isFormatted;
  }

  private void doUpgrade() throws IOException {
    namesystem.setUpgradeStartTime(FSNamesystem.now());
    if(storage.getDistributedUpgradeState()) {
      // only distributed upgrade need to continue
      // don't do version upgrade
      FSImageStorageInspector inspector = storage.readAndInspectDirs();
      this.loadFSImage(inspector);
      storage.initializeDistributedUpgrade();
      return;
    }
    // Upgrade is allowed only if there are 
    // no previous fs states in any of the directories
    for (Iterator<StorageDirectory> it = storage.dirIterator(); it.hasNext();) {
      StorageDirectory sd = it.next();
      if (sd.getPreviousDir().exists())
        throw new InconsistentFSStateException(sd.getRoot(),
            "previous fs state should not exist during upgrade. "
            + "Finalize or rollback first.");
    }

    FSImageStorageInspector inspector = storage.readAndInspectDirs();
    // load the latest image
    this.loadFSImage(inspector);
    // clear the digest for the loaded image, it might change during upgrade
    this.storage.clearCheckpointImageDigest(storage
        .getMostRecentCheckpointTxId());

    // Do upgrade for each directory
    long oldCTime = storage.getCTime();
    this.storage.cTime = FSNamesystem.now();  // generate new cTime for the state
    int oldLV = storage.getLayoutVersion();
    this.storage.layoutVersion = FSConstants.LAYOUT_VERSION;
    
    assert !editLog.isOpen() : "Edits log must not be open.";

    List<StorageDirectory> errorSDs =
      Collections.synchronizedList(new ArrayList<StorageDirectory>());
    for (Iterator<StorageDirectory> it = storage.dirIterator(); it.hasNext();) {
      StorageDirectory sd = it.next();
      LOG.info("Starting upgrade of image directory " + sd.getRoot()
               + ".\n   old LV = " + oldLV
               + "; old CTime = " + oldCTime
               + ".\n   new LV = " + storage.getLayoutVersion()
               + "; new CTime = " + storage.getCTime());
      try {
        Storage.upgradeDirectory(sd);
      } catch (Exception e) {
        LOG.error("Failed to move aside pre-upgrade storage " +
            "in image directory " + sd.getRoot(), e);
        errorSDs.add(sd);
        continue;
      }
    }

    // Upgrade non-file directories.
    imageSet.transitionNonFileImages(storage, false, Transition.UPGRADE,
        null);
    editLog.transitionNonFileJournals(storage, false,
        Transition.UPGRADE, null);

    storage.reportErrorsOnDirectories(errorSDs, this);
    errorSDs.clear();

    InjectionHandler
        .processEventIO(InjectionEvent.FSIMAGE_UPGRADE_BEFORE_SAVE_IMAGE);

    saveFSImageInAllDirs(editLog.getLastWrittenTxId(), false);

    for (Iterator<StorageDirectory> it = storage.dirIterator(); it.hasNext();) {
      StorageDirectory sd = it.next();
      try {
        Storage.completeUpgrade(sd);
      } catch (IOException ioe) {
        LOG.error("Unable to rename temp to previous for " + sd.getRoot(), ioe);
        errorSDs.add(sd);
        continue;
      }
      isUpgradeFinalized = false;
      LOG.info("Upgrade of " + sd.getRoot() + " is complete.");
    }

    // Complete the upgrade for non-file directories.
    imageSet.transitionNonFileImages(storage, false,
        Transition.COMPLETE_UPGRADE, null);
    editLog.transitionNonFileJournals(storage, false,
        Transition.COMPLETE_UPGRADE, null);

    storage.reportErrorsOnDirectories(errorSDs, this);
    storage.initializeDistributedUpgrade();
  }

  private void doRollback(
      Map<ImageManager, RemoteStorageState> remoteImageStates,
      Map<JournalManager, RemoteStorageState> remoteJournalStates)
      throws IOException {
    // Rollback is allowed only if there is 
    // a previous fs states in at least one of the storage directories.
    // Directories that don't have previous state do not rollback
    boolean canRollback = false;
    FSImage prevState = new FSImage(conf);
    prevState.storage.layoutVersion = FSConstants.LAYOUT_VERSION;
    for (Iterator<StorageDirectory> it = storage.dirIterator(); it.hasNext();) {
      StorageDirectory sd = it.next();
      canRollback = NNStorage.canRollBack(sd, prevState.storage);
    }

    // Check non-file managers.
    for (RemoteStorageState s : remoteImageStates.values()) {
      StorageState state = s.getStorageState();
      if (state == StorageState.UPGRADE_DONE) {
        canRollback = true;
      }
    }

    // Check non-file managers.
    for (RemoteStorageState s : remoteJournalStates.values()) {
      StorageState state = s.getStorageState();
      if (state == StorageState.UPGRADE_DONE) {
        canRollback = true;
      }
    }

    if (!canRollback)
      throw new IOException("Cannot rollback. None of the storage "
                            + "directories contain previous fs state.");

    // Now that we know all directories are going to be consistent
    // Do rollback for each directory containing previous state
    for (Iterator<StorageDirectory> it = storage.dirIterator(); it.hasNext();) {
      StorageDirectory sd = it.next();
      NNStorage.doRollBack(sd, prevState.storage);
    }

    // Rollback non-file storage locations.
    editLog.transitionNonFileJournals(storage, false, Transition.ROLLBACK,
            null);
    imageSet.transitionNonFileImages(storage, false, Transition.ROLLBACK, null);


    isUpgradeFinalized = true;
    // check whether name-node can start in regular mode
    storage.verifyDistributedUpgradeProgress(StartupOption.REGULAR);
  }

  private void doFinalize(StorageDirectory sd) throws IOException {
    NNStorage.finalize(sd, storage.getLayoutVersion(), storage.getCTime());
    isUpgradeFinalized = true;
  }

  /**
   * Load image from a checkpoint directory and save it into the current one.
   * @throws IOException
   */
  /**
   * Load image from a checkpoint directory and save it into the current one.
   * @param target the NameSystem to import into
   * @throws IOException
   */
  void doImportCheckpoint() throws IOException {
    Collection<URI> checkpointDirs =
      NNStorageConfiguration.getCheckpointDirs(conf, null);
    Collection<URI> checkpointEditsDirs =
        NNStorageConfiguration.getCheckpointEditsDirs(conf, null);

    if (checkpointDirs == null || checkpointDirs.isEmpty()) {
      throw new IOException("Cannot import image from a checkpoint. "
                            + "\"dfs.namenode.checkpoint.dir\" is not set." );
    }
    
    if (checkpointEditsDirs == null || checkpointEditsDirs.isEmpty()) {
      throw new IOException("Cannot import image from a checkpoint. "
                            + "\"dfs.namenode.checkpoint.dir\" is not set." );
    }

    // replace real image with the checkpoint image
    FSImage realImage = namesystem.getFSImage();
    assert realImage == this;
    FSImage ckptImage = new FSImage(conf,
                                    checkpointDirs, checkpointEditsDirs, null);
    ckptImage.setFSNamesystem(namesystem);
    namesystem.dir.fsImage = ckptImage;
    // load from the checkpoint dirs
    try {
      ckptImage.recoverTransitionRead(StartupOption.REGULAR);
    } finally {
      ckptImage.close();
    }
    // return back the real image
    realImage.storage.setStorageInfo(ckptImage.storage);
    realImage.getEditLog().setLastWrittenTxId(ckptImage.getEditLog().getLastWrittenTxId() + 1);

    namesystem.dir.fsImage = realImage;

    // and save it but keep the same checkpointTime
    // parameters
    saveNamespace();
  }

  public void finalizeUpgrade() throws IOException {
    for (Iterator<StorageDirectory> it = storage.dirIterator(); it.hasNext();) {
      doFinalize(it.next());
    }

    // finalize non-file storage locations.
    editLog.transitionNonFileJournals(null, false, Transition.FINALIZE, null);
    imageSet.transitionNonFileImages(null, false, Transition.FINALIZE, null);

    isUpgradeFinalized = true;
    namesystem.setUpgradeStartTime(0);
  }

  boolean isUpgradeFinalized() {
    return isUpgradeFinalized;
  }

  public FSEditLog getEditLog() {
    return editLog;
  }
  
  public List<ImageManager> getImageManagers() {
    return imageSet.getImageManagers();
  }
  
  void openEditLog() throws IOException {
    if (editLog == null) {
      throw new IOException("EditLog must be initialized");
    }
    if (!editLog.isOpen()) {
      editLog.open();
      storage
          .writeTransactionIdFileToStorage(editLog.getCurSegmentTxId(), this);
    }
  };

  /**
   * Choose latest image from one of the directories,
   * load it and merge with the edits from that directory.
   * 
   * @return whether the image should be saved
   * @throws IOException
   */
  boolean loadFSImage(FSImageStorageInspector inspector) throws IOException {
    ImageInputStream iis = null;
    isUpgradeFinalized = inspector.isUpgradeFinalized();

    FSImageStorageInspector.FSImageFile imageFile = inspector.getLatestImage();
    boolean needToSave = inspector.needToSave();
    
    FSImageStorageInspector.FSImageFile nonFileImage = imageSet
        .getLatestImageFromNonFileImageManagers();
    
    boolean loadingNonFileImage = false;
    // image stored in non-file storage is newer, we obtain the input stream here
    
    // recover unclosed streams, so the journals storing image are initialized
    editLog.recoverUnclosedStreams();
    long imageCheckpointTxId;
    
    if (nonFileImage != null
        && (nonFileImage.getCheckpointTxId() > imageFile.getCheckpointTxId() || conf
            .getBoolean("dfs.force.remote.image", false))) {
      // this will contain the digest
      LOG.info("Non-file image is newer/forced.");
      iis = nonFileImage.getImageManager().getImageInputStream(
          nonFileImage.getCheckpointTxId());
      imageCheckpointTxId = nonFileImage.getCheckpointTxId();
      loadingNonFileImage = true;
    } else {
      // the md5 digest will be set later
      iis = new ImageInputStream(imageFile.getCheckpointTxId(),
          new FileInputStream(imageFile.getFile()), null, imageFile.getFile()
              .getAbsolutePath(), imageFile.getFile().length());
      imageCheckpointTxId = imageFile.getCheckpointTxId();
      loadingNonFileImage = false;
    }
    

    Collection<EditLogInputStream> editStreams = new ArrayList<EditLogInputStream>();
    
    // if the recovery failed for any journals, just abort the startup.
    if (editLog.getNumberOfAvailableJournals() != editLog.getNumberOfJournals()) {
      LOG.fatal("Unable to recover unclosed segments for all journals.");
      throw new IOException(
          "Unable to recover unclosed segments for all journals.");
    }
    
    if (LayoutVersion.supports(Feature.TXID_BASED_LAYOUT, getLayoutVersion())) {
      FLOG.info("Load Image: checkpoint txid: " + imageCheckpointTxId
          + " max seen: " + inspector.getMaxSeenTxId());
      needToSave |= editLog.selectInputStreams(editStreams,
          imageCheckpointTxId + 1, inspector.getMaxSeenTxId(),
          editLog.getNumberOfJournals());
    } else {
      FSImagePreTransactionalStorageInspector.getEditLogStreams(editStreams,
          storage, conf);
    }

    FLOG.info("Load Image: planning to load image :\n" + iis);
    for (EditLogInputStream l : editStreams) {
      FLOG.info("Load Image: planning to load edit stream: " + l);
    }

    try {
      if (!loadingNonFileImage) {
        StorageDirectory sdForProperties = imageFile.sd;
        sdForProperties.read();
  
        if (LayoutVersion.supports(Feature.TXID_BASED_LAYOUT, getLayoutVersion())) {
          // For txid-based layout, we should have a .md5 file
          // next to the image file
          loadFSImage(iis, imageFile.getFile());
        } else if (LayoutVersion.supports(Feature.FSIMAGE_CHECKSUM,
            getLayoutVersion())) {
          // In 0.22, we have the checksum stored in the VERSION file.
          String md5 = storage
              .getDeprecatedProperty(NNStorage.MESSAGE_DIGEST_PROPERTY);
          if (md5 == null) {
            throw new InconsistentFSStateException(sdForProperties.getRoot(),
                "Message digest property " + NNStorage.MESSAGE_DIGEST_PROPERTY
                    + " not set for storage directory "
                    + sdForProperties.getRoot());
          }
          iis.setImageDigest(new MD5Hash(md5));
          loadFSImage(iis);
        } else {
          // We don't have any record of the md5sum
          loadFSImage(iis);
        }
      } else {
        if (!LayoutVersion.supports(Feature.TXID_BASED_LAYOUT,
            getLayoutVersion())) {
          throwIOException("Inconsistency: Loading remote image, but the layout does not support txids: "
              + getLayoutVersion());
        }
        // loading non-file
        loadFSImage(iis);
      }
    } catch (IOException ioe) {
      FSEditLog.closeAllStreams(editStreams);
      throw new IOException("Failed to load image from " + 
          (loadingNonFileImage ? nonFileImage : imageFile), ioe);
    }
           
    editLog.setLastWrittenTxId(storage.getMostRecentCheckpointTxId());

    long numLoaded = loadEdits(editStreams);
    needToSave |= needsResaveBasedOnStaleCheckpoint(loadingNonFileImage ? null
        : imageFile.getFile(), numLoaded);            
    return needToSave;
  }
  
  /**
   * @param imageFile
   *          the image file that was loaded (if remote location was loaded that
   *          this is null)
   * @param numEditsLoaded
   *          the number of edits loaded from edits logs
   * @return true if the NameNode should automatically save the namespace when
   *         it is started, due to the latest checkpoint being too old.
   */
  private boolean needsResaveBasedOnStaleCheckpoint(File imageFile,
      long numEditsLoaded) {
    final long checkpointPeriod = conf.getLong("fs.checkpoint.period", 3600);
    final long checkpointTxnCount = NNStorageConfiguration.getCheckpointTxnCount(conf);
    long checkpointAge = System.currentTimeMillis()
        - (imageFile == null ? Long.MAX_VALUE : imageFile.lastModified());
    boolean needToSave = (checkpointAge > checkpointPeriod * 1000)
        || (numEditsLoaded > checkpointTxnCount);
    LOG.info("Load Image: Need to save based on stale checkpoint: "
        + needToSave);
    return needToSave;
  }

  /**
   * Load the image namespace from the given image file, verifying it against
   * the MD5 sum stored in its associated .md5 file.
   */
  protected void loadFSImage(ImageInputStream iis, File imageFile) throws IOException {
    MD5Hash expectedMD5 = MD5FileUtils.readStoredMd5ForFile(imageFile);
    if (expectedMD5 == null) {
      throw new IOException("No MD5 file found corresponding to image file "
          + imageFile);
    }
    iis.setImageDigest(expectedMD5);
    loadFSImage(iis);
  }

  boolean loadFSImage(ImageInputStream iis) throws IOException {
    assert iis != null : "input stream is null";

    FSImageFormat.Loader loader = new FSImageFormat.Loader(
        namesystem.getConf(), namesystem, storage);
    loader.load(iis, null);
    saveNamespaceContext.set(null, loader.getLoadedImageTxId());
    // Check that the image digest we loaded matches up with what
    // we expected
    MD5Hash readImageMd5 = loader.getLoadedImageMd5();
    MD5Hash expectedMd5 = iis.getDigest();
    if (expectedMd5 != null && !expectedMd5.equals(readImageMd5)) {
      throw new IOException("Image file " + iis
          + " is corrupt with MD5 checksum of " + readImageMd5
          + " but expecting " + expectedMd5);
    }
    
    this.setImageDigest(loader.getLoadedImageTxId(), readImageMd5); // set this fsimage's checksum 
    
    storage.setMostRecentCheckpointTxId(loader.getLoadedImageTxId());
    return loader.getNeedToSave();
  }

  /**
   * Return string representing the parent of the given path.
   */
  String getParent(String path) {
    return path.substring(0, path.lastIndexOf(Path.SEPARATOR));
  }
  
  byte[][] getParent(byte[][] path) {
    byte[][] result = new byte[path.length - 1][];
    for (int i = 0; i < result.length; i++) {
      result[i] = new byte[path[i].length];
      System.arraycopy(path[i], 0, result[i], 0, path[i].length);
    }
    return result;
  }

  /**
   * Load the specified list of edit files into the image.
   * @return the txid of the current transaction (next to be loaded
   */
  protected long loadEdits(Iterable<EditLogInputStream> editStreams) 
      throws IOException {

    long lastAppliedTxId = storage.getMostRecentCheckpointTxId();
    int numLoaded = 0;
    FSEditLogLoader loader = new FSEditLogLoader(namesystem);
      
    // Load latest edits
    for (EditLogInputStream editIn : editStreams) {
      FLOG.info("Load Image: Reading edits: " + editIn + " last applied txid#: "
          + lastAppliedTxId);
      numLoaded += loader.loadFSEdits(editIn, lastAppliedTxId);  
      lastAppliedTxId = loader.getLastAppliedTxId();
    }
    editLog.setLastWrittenTxId(lastAppliedTxId);
    FLOG.info("Load Image: Number of edit transactions loaded: "
        + numLoaded + " last applied txid: " + lastAppliedTxId);

    // update the counts
    namesystem.dir.updateCountForINodeWithQuota();    
    return numLoaded;
  }

  // for snapshot
  void saveFSImage(String dest, DataOutputStream fstream) throws IOException {
    saveNamespaceContext.set(namesystem, editLog.getLastWrittenTxId());
    FSImageFormat.Saver saver = new FSImageFormat.Saver(saveNamespaceContext);
    FSImageCompression compression = FSImageCompression.createCompression(
        namesystem.getConf(), false);
    saver
        .save(new FileOutputStream(new File(dest)), compression, fstream, dest);
  }
  
  /**
   * Save the contents of the FS image to the file.
   */
  void saveFSImage(SaveNamespaceContext context, ImageManager im, boolean forceUncompressed)
      throws IOException {
    
    long txid = context.getTxId();
    OutputStream os = im.getCheckpointOutputStream(txid);

    FSImageFormat.Saver saver = new FSImageFormat.Saver(context);
    FSImageCompression compression = FSImageCompression.createCompression(conf, forceUncompressed);
    
    saver.save(os, compression, null, im.toString());
    
    InjectionHandler.processEvent(InjectionEvent.FSIMAGE_SAVED_IMAGE, txid);
    storage.setCheckpointImageDigest(txid, saver.getSavedDigest());
  }
  
  private class FSImageSaver implements Runnable {
    private SaveNamespaceContext context;
    private ImageManager im;
    private boolean forceUncompressed;

    FSImageSaver(SaveNamespaceContext ctx, ImageManager im, boolean forceUncompressed) {
      this.context = ctx;
      this.im = im;
      this.forceUncompressed = forceUncompressed;
    }
    
    public String toString() {
      return "FSImage saver for " + im.toString() + " for txid : "
          + context.getTxId();
    }
    
    public void run() {
      try {
        InjectionHandler
          .processEvent(InjectionEvent.FSIMAGE_STARTING_SAVER_THREAD);
        
        LOG.info(this.toString() + " -- starting");
        saveFSImage(context, im, forceUncompressed);
        im.setImageDisabled(false);
      } catch (SaveNamespaceCancelledException ex) {
        LOG.warn("FSImageSaver: - cancelling operation");
      } catch (IOException ex) {
        LOG.error("Unable to write image: " + this.toString(), ex);
        context
            .reportErrorOnStorageDirectory((im instanceof FileImageManager) ? ((FileJournalManager) im)
                .getStorageDirectory() : null);
        im.setImageDisabled(true);
      }
    }
  }

  /**
   * Save the contents of the FS image
   * and create empty edits.
   */
  public void saveNamespace() throws IOException {
    saveNamespace(false);
  }
  
  /**
   * Save the contents of the FS image to a new image file in each of the
   * current storage directories.
   */
  public synchronized void saveNamespace(boolean forUncompressed) 
      throws IOException {
    
    InjectionHandler
      .processEvent(InjectionEvent.FSIMAGE_STARTING_SAVE_NAMESPACE);
    
    if (editLog == null) {
      throw new IOException("editLog must be initialized");
    }
    storage.attemptRestoreRemovedStorage();

    InjectionHandler
      .processEvent(InjectionEvent.FSIMAGE_STARTING_SAVE_NAMESPACE);
    
    boolean editLogWasOpen = editLog.isOpen();
    
    if (editLogWasOpen) {
      editLog.endCurrentLogSegment(true);
    }
    long imageTxId = editLog.getLastWrittenTxId();
    try {    
      // for testing only - we will wait until interruption comes
      InjectionHandler
          .processEvent(InjectionEvent.FSIMAGE_CREATING_SAVER_THREADS);
      saveFSImageInAllDirs(imageTxId, forUncompressed);
      storage.writeAll();
    } finally {
      if (editLogWasOpen) {
        editLog.startLogSegment(imageTxId + 1, true);
        // Take this opportunity to note the current transaction.
        // Even if the namespace save was cancelled, this marker
        // is only used to determine what transaction ID is required
        // for startup. So, it doesn't hurt to update it unnecessarily.
        storage.writeTransactionIdFileToStorage(imageTxId + 1, this);
      }
      saveNamespaceContext.clear();
    }
    
  }
  
  protected synchronized void saveFSImageInAllDirs(long txid, boolean forceUncompressed)
      throws IOException {    
    if (storage.getNumStorageDirs(NameNodeDirType.IMAGE) == 0) {
      throw new IOException("No image directories available!");
    }
    
    saveNamespaceContext.set(namesystem, txid); 
    
    List<Thread> saveThreads = new ArrayList<Thread>();
    // save images into current
    for (ImageManager im : imageSet.getImageManagers()) {
      FSImageSaver saver = new FSImageSaver(saveNamespaceContext, im,
          forceUncompressed);
      Thread saveThread = new Thread(saver, saver.toString());
      saveThreads.add(saveThread);
      saveThread.start();
    }
    waitForThreads(saveThreads);
    saveThreads.clear();
    storage.reportErrorsOnDirectories(saveNamespaceContext.getErrorSDs(), this);

    // check if we have any image managers left
    imageSet.checkImageManagers();
    
    if (saveNamespaceContext.isCancelled()) {
      deleteCheckpoint(saveNamespaceContext.getTxId());  
      saveNamespaceContext.checkCancelled();
    } 
    // tell all image managers to store md5
    imageSet.saveDigestAndRenameCheckpointImage(txid,
        storage.getCheckpointImageDigest(txid));

    storage.setMostRecentCheckpointTxId(txid);
    
    // Since we now have a new checkpoint, we can clean up some
    // old edit logs and checkpoints.
    purgeOldStorage();
  }
  
  private void waitForThreads(List<Thread> threads) {
    for (Thread thread : threads) {
      while (thread.isAlive()) {
        try {
          thread.join();
        } catch (InterruptedException iex) {
          LOG.error("Caught exception while waiting for thread " +
                    thread.getName() + " to finish. Retrying join");
        }        
      }
    }
  }

  public void format() throws IOException {
    storage.format();
    LOG.info("Format non-file journal managers");
    editLog.transitionNonFileJournals(storage, false,
        Transition.FORMAT, null);
    LOG.info("Format non-file image managers");
    transitionNonFileImages(storage, false, Transition.FORMAT);
    // take over as the writer
    editLog.recoverUnclosedStreams(); 
    saveFSImageInAllDirs(-1, false);
  }
  
  void transitionNonFileImages(StorageInfo nsInfo, boolean checkEmpty,
      Transition transition)
      throws IOException {
    imageSet.transitionNonFileImages(storage, checkEmpty,
        transition, null);
  }
  
  /**
   * Get the list of non-file journal managers.
   */
  List<JournalManager> getNonFileJournalManagers() {
    return editLog.getNonFileJournalManagers();
  }

  /**
   * Get the list of non-file image managers.
   */
  List<ImageManager> getNonFileImageManagers() {
    return imageSet.getNonFileImageManagers();
  }
  
  /**
   * Check whether the storage directories and non-file journals exist.
   * If running in interactive mode, will prompt the user for each
   * directory to allow them to format anyway. Otherwise, returns
   * false, unless 'force' is specified.
   * 
   * @param interactive prompt the user when a dir exists
   * @return true if formatting should proceed
   * @throws IOException if some storage cannot be accessed
   */
  boolean confirmFormat(boolean force, boolean interactive) throws IOException {
    List<FormatConfirmable> confirms = Lists.newArrayList();
    for (StorageDirectory sd : storage.dirIterable(null)) {
      confirms.add(sd);
    }   
    confirms.addAll(editLog.getFormatConfirmables());
    return Storage.confirmFormat(confirms, force, interactive);
  }
  
  /**
   * Deletes the checkpoint file in every storage directory,
   * since the checkpoint was cancelled. Attepmts to remove
   * image/md5/ckptimage files.
   */
  void deleteCheckpoint(long txId) throws IOException {
    for (Iterator<StorageDirectory> it = storage.dirIterator(); it.hasNext();) {
      StorageDirectory sd = it.next();
      // image file
      File imageFile = NNStorage.getImageFile(sd, txId);
      if (imageFile.delete())
        LOG.info("Delete checkpoint: deleted: " + imageFile);
      
      // md5 file
      File imageFileMD5 = MD5FileUtils.getDigestFileForFile(imageFile);
      if (imageFileMD5.delete())
        LOG.info("Delete checkpoint: deleted: " + imageFileMD5);
      
      // image ckpt file
      File imageCkptFile = NNStorage.getCheckpointImageFile(sd, txId);
      if (imageCkptFile.delete())
        LOG.info("Delete checkpoint: deleted: " + imageCkptFile);
    }
  }

  CheckpointSignature rollEditLog() throws IOException {
    getEditLog().rollEditLog();
    
    // clear up image manager states
    imageSet.restoreImageManagers();
    
    // Record this log segment ID in all of the storage directories, so
    // we won't miss this log segment on a restart if the edits directories
    // go missing.
    storage.writeTransactionIdFileToStorage(getEditLog().getCurSegmentTxId(),
        this);
    return new CheckpointSignature(this);
  }

  /**
   * End checkpoint.
   * Validate the current storage info with the given signature.
   * 
   * @param sig to validate the current storage info against
   * @throws IOException if the checkpoint fields are inconsistent
   */
  void rollFSImage(CheckpointSignature sig) throws IOException {
    long start = System.nanoTime();
    sig.validateStorageInfo(this.storage);

    saveDigestAndRenameCheckpointImage(sig.mostRecentCheckpointTxId,
        sig.imageDigest);

    long rollTime = DFSUtil.getElapsedTimeMicroSeconds(start);
    if (metrics != null) {
      metrics.rollFsImageTime.inc(rollTime);
    }
  }
    
  synchronized void checkpointUploadDone(long txid, MD5Hash checkpointImageMd5)
      throws IOException {
    storage.checkpointUploadDone(txid, checkpointImageMd5);
  }
  
  /**
   * This is called by the 2NN after having downloaded an image, and by
   * the NN after having received a new image from the 2NN. It
   * renames the image from fsimage_N.ckpt to fsimage_N and also
   * saves the related .md5 file into place.
   */
  synchronized void saveDigestAndRenameCheckpointImage(
      long txid, MD5Hash digest) throws IOException {
    if (!digest.equals(storage.getCheckpointImageDigest(txid))) {
      throw new IOException(
          "Checkpoint image is corrupt: expecting an MD5 checksum of" +
              digest + " but is " + storage.getCheckpointImageDigest(txid));
    }
       
    imageSet.saveDigestAndRenameCheckpointImage(txid, digest);
    
    // So long as this is the newest image available,
    // advertise it as such to other checkpointers
    // from now on
    storage.setMostRecentCheckpointTxId(txid);
  }
  
  /**
   * Purge any files in the storage directories that are no longer
   * necessary.
   */
  public synchronized void purgeOldStorage() {
    try {
      archivalManager.purgeOldStorage();
    } catch (Exception e) {
      LOG.warn("Unable to purge old storage", e);
    }
  }
  
  void reportErrorsOnImageManager(StorageDirectory badSD) {
    if (imageSet != null) {
      imageSet.reportErrorsOnImageManager(badSD);
    }
  }
  
  void checkImageManagers() throws IOException {
    if (imageSet != null) {
      imageSet.checkImageManagers();
    }
  }
  
  void updateImageMetrics() {
    if (imageSet != null) {
      imageSet.updateImageMetrics();
    }
  }
  
  void close() throws IOException {
    if(editLog != null)
      editLog.close();
    storage.unlockAll();
  }

  /**
   * Return the name of the latest image file.
   * @param type which image should be preferred.
   */
  File getFsImageName(StorageLocationType type) {
    return storage.getFsImageName(type, storage.getMostRecentCheckpointTxId());
  }
  
  /**
   * Returns the txid of the last checkpoint
   */
  public long getLastCheckpointTxId() {
    return storage.getMostRecentCheckpointTxId();
  }
  
  /**
   * Retrieve checkpoint dirs from configuration.
   *
   * @param conf the Configuration
   * @param defaultValue a default value for the attribute, if null
   * @return a Collection of URIs representing the values in 
   * dfs.namenode.checkpoint.dir configuration property
   */
  static Collection<File> getCheckpointDirs(Configuration conf,
      String defaultName) {
    Collection<String> dirNames = conf.getStringCollection("fs.checkpoint.dir");
    if (dirNames.size() == 0 && defaultName != null) {
      dirNames.add(defaultName);
    }
    Collection<File> dirs = new ArrayList<File>(dirNames.size());
    for (String name : dirNames) {
      dirs.add(new File(name));
    }
    return dirs;
  }

  static Collection<File> getCheckpointEditsDirs(Configuration conf,
      String defaultName) {
    Collection<String> dirNames = conf
        .getStringCollection("fs.checkpoint.edits.dir");
    if (dirNames.size() == 0 && defaultName != null) {
      dirNames.add(defaultName);
    }
    Collection<File> dirs = new ArrayList<File>(dirNames.size());
    for (String name : dirNames) {
      dirs.add(new File(name));
    }
    return dirs;
  }

  public int getLayoutVersion() {
    return storage.getLayoutVersion();
  }
  
  public int getNamespaceID() {
    return storage.getNamespaceID();
  }
  
  public void cancelSaveNamespace(String reason) {
    saveNamespaceContext.cancel(reason);
    InjectionHandler.processEvent(InjectionEvent.FSIMAGE_CANCEL_REQUEST_RECEIVED);
  }
  
  public void clearCancelSaveNamespace() {
    saveNamespaceContext.clear();
  }

  protected long getImageTxId() {
    return saveNamespaceContext.getTxId();
  }

  public Iterator<StorageDirectory> dirIterator(StorageDirType dirType) {
    return storage.dirIterator(dirType);
  }

  public Iterator<StorageDirectory> dirIterator() {
    return storage.dirIterator();
  }
  
  static void rollForwardByApplyingLogs(
      RemoteEditLogManifest manifest,
      FSImage dstImage) throws IOException {
    NNStorage dstStorage = dstImage.storage;
  
    List<EditLogInputStream> editsStreams = new ArrayList<EditLogInputStream>();    
    for (RemoteEditLog log : manifest.getLogs()) {
      if (log.inProgress())
        break;
      File f = dstStorage.findFinalizedEditsFile(
          log.getStartTxId(), log.getEndTxId());
      if (log.getStartTxId() > dstImage.getLastAppliedTxId()) {
        editsStreams.add(new EditLogFileInputStream(f, log.getStartTxId(), 
                                                    log.getEndTxId(), false));
       }
    }
    dstImage.loadEdits(editsStreams);
  }
  
  /**
   * Get a list of output streams for writing chekpoint images.
   */
  public List<OutputStream> getCheckpointImageOutputStreams(long imageTxId)
      throws IOException {
    return imageSet.getCheckpointImageOutputStreams(imageTxId);
  }
}
