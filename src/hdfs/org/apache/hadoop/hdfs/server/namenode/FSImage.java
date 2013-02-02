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
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.protocol.FSConstants;
import org.apache.hadoop.hdfs.protocol.LayoutVersion;
import org.apache.hadoop.hdfs.protocol.LayoutVersion.Feature;
import org.apache.hadoop.hdfs.server.common.InconsistentFSStateException;
import org.apache.hadoop.hdfs.server.common.Storage;
import org.apache.hadoop.hdfs.server.common.Storage.FormatConfirmable;
import org.apache.hadoop.hdfs.server.common.Storage.StorageDirType;
import org.apache.hadoop.hdfs.server.common.Storage.StorageDirectory;
import org.apache.hadoop.hdfs.server.common.Storage.StorageState;
import org.apache.hadoop.hdfs.server.common.StorageInfo;
import org.apache.hadoop.hdfs.server.common.HdfsConstants.StartupOption;
import org.apache.hadoop.hdfs.server.namenode.NNStorage.NameNodeDirType;
import org.apache.hadoop.hdfs.server.namenode.NNStorage.NameNodeFile;
import org.apache.hadoop.hdfs.server.namenode.NNStorage.StorageLocationType;
import org.apache.hadoop.hdfs.server.namenode.ValidateNamespaceDirPolicy.NNStorageLocation;
import org.apache.hadoop.hdfs.server.namenode.metrics.NameNodeMetrics;
import org.apache.hadoop.hdfs.server.protocol.RemoteEditLog;
import org.apache.hadoop.hdfs.server.protocol.RemoteEditLogManifest;
import org.apache.hadoop.hdfs.util.InjectionEvent;
import org.apache.hadoop.hdfs.util.MD5FileUtils;
import org.apache.hadoop.io.MD5Hash;
import org.apache.hadoop.util.InjectionHandler;

import com.google.common.collect.Lists;

/**
 * FSImage handles checkpointing and logging of the namespace edits.
 * 
 */
public class FSImage {

  static final Log LOG = LogFactory.getLog(FSImage.class.getName());
  
  NNStorage storage;
  Configuration conf;
  private NNStorageRetentionManager archivalManager;
  
  private final SaveNamespaceContext saveNamespaceContext 
    = new SaveNamespaceContext();
  
  protected FSNamesystem namesystem = null;
  FSEditLog editLog = null;
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
    this.editLog = new FSEditLog(storage);
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
    this.editLog = new FSEditLog(conf, storage, fsEditsDirs, locationMap);
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
  
  /**
   * Analyze storage directories.
   * Recover from previous transitions if required. 
   * Perform fs state transition if necessary depending on the namespace info.
   * Read storage info. 
   * 
   * @param dataDirs
   * @param startOpt startup option
   * @throws IOException
   * @return true if the image needs to be saved or false otherwise
   */
  
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
    assert startOpt != StartupOption.FORMAT : 
      "NameNode formatting should be performed before reading the image";
    
    Collection<File> imageDirs = storage.getImageDirectories();

    // none of the data dirs exist
    if(imageDirs.size() == 0 && startOpt != StartupOption.IMPORT)  
      throw new IOException(
          "All specified directories are not accessible or do not exist.");
    editLog.checkJournals();
    
    storage.setUpgradeManager(namesystem.upgradeManager);
    
    // 1. For each data directory calculate its state and 
    // check whether all is consistent before transitioning.
    Map<StorageDirectory, StorageState> dataDirStates = 
             new HashMap<StorageDirectory, StorageState>();
    boolean isFormatted = recoverStorageDirs(startOpt, dataDirStates);
    
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

    // check whether distributed upgrade is required and/or should be continued
    storage.verifyDistributedUpgradeProgress(startOpt);

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

    // 3. Do transitions
    switch(startOpt) {
    case UPGRADE:
      doUpgrade();
      return false; // upgrade saved image already
    case IMPORT:
      doImportCheckpoint();
      return false; // import checkpoint saved image already
    case ROLLBACK:
      doRollback();
      break;
    case REGULAR:
      // just load the image
    }
    
    if (inUpgradeStatus()) {
      namesystem.setUpgradeStartTime(FSNamesystem.now());
    }
    return loadFSImage();
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
        // sd is locked but not opened
        switch(curState) {
        case NON_EXISTENT:
          // name-node fails if any of the configured storage dirs are missing
          throw new InconsistentFSStateException(sd.getRoot(),
                      "storage directory does not exist or is not accessible.");
        case NOT_FORMATTED:
          break;
        case NORMAL:
          break;
        default:  // recovery is possible
          sd.doRecover(curState);      
        }
        if (curState != StorageState.NOT_FORMATTED 
            && startOpt != StartupOption.ROLLBACK) {
          // read and verify consistency with other directories
          sd.read();
          isFormatted = true;
        }
        if (startOpt == StartupOption.IMPORT && isFormatted)
          // import of a checkpoint is allowed only into empty image directories
          throw new IOException("Cannot import image from a checkpoint. " 
              + " NameNode already contains an image in " + sd.getRoot());
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
      this.loadFSImage();
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

    // load the latest image
    this.loadFSImage();
    // clear the digest for the loaded image, it might change during upgrade
    this.storage.clearCheckpointImageDigest(storage
        .getMostRecentCheckpointTxId());

    // Do upgrade for each directory
    long oldCTime = storage.getCTime();
    this.storage.cTime = FSNamesystem.now();  // generate new cTime for the state
    int oldLV = storage.getLayoutVersion();
    this.storage.layoutVersion = FSConstants.LAYOUT_VERSION;
    
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
        File curDir = sd.getCurrentDir();
        File prevDir = sd.getPreviousDir();
        File tmpDir = sd.getPreviousTmp();
        assert curDir.exists() : "Current directory must exist.";
        assert !prevDir.exists() : "prvious directory must not exist.";
        assert !tmpDir.exists() : "prvious.tmp directory must not exist.";
        assert !editLog.isOpen() : "Edits log must not be open.";

        // rename current to tmp
        NNStorage.rename(curDir, tmpDir);
        
        if (!curDir.mkdir()) {
          throw new IOException("Cannot create directory " + curDir);
        }
      } catch (Exception e) {
        LOG.error("Failed to move aside pre-upgrade storage " +
            "in image directory " + sd.getRoot(), e);
        errorSDs.add(sd);
        continue;
      }
    }
    storage.reportErrorsOnDirectories(errorSDs);
    errorSDs.clear();

    saveFSImageInAllDirs(editLog.getLastWrittenTxId(), false);

    for (Iterator<StorageDirectory> it = storage.dirIterator(); it.hasNext();) {
      StorageDirectory sd = it.next();
      try {
        // Write the version file, since saveFsImage above only makes the
        // fsimage, and the directory is otherwise empty.
        sd.write();
        
        File prevDir = sd.getPreviousDir();
        File tmpDir = sd.getPreviousTmp();
        // rename tmp to previous
        NNStorage.rename(tmpDir, prevDir);
      } catch (IOException ioe) {
        LOG.error("Unable to rename temp to previous for " + sd.getRoot(), ioe);
        errorSDs.add(sd);
        continue;
      }
      isUpgradeFinalized = false;
      LOG.info("Upgrade of " + sd.getRoot() + " is complete.");
    }
    storage.reportErrorsOnDirectories(errorSDs);
    storage.initializeDistributedUpgrade();
  }

  private void doRollback() throws IOException {
    // Rollback is allowed only if there is 
    // a previous fs states in at least one of the storage directories.
    // Directories that don't have previous state do not rollback
    boolean canRollback = false;
    FSImage prevState = new FSImage(conf);
    prevState.storage.layoutVersion = FSConstants.LAYOUT_VERSION;
    for (Iterator<StorageDirectory> it = storage.dirIterator(); it.hasNext();) {
      StorageDirectory sd = it.next();
      File prevDir = sd.getPreviousDir();
      if (!prevDir.exists()) {  // use current directory then
        LOG.info("Storage directory " + sd.getRoot()
                 + " does not contain previous fs state.");
        // read and verify consistency with other directories
        sd.read();
        continue;
      }

      // read and verify consistency of the prev dir
      sd.read(sd.getPreviousVersionFile());

      if (prevState.getLayoutVersion() != FSConstants.LAYOUT_VERSION) {
        throw new IOException(
          "Cannot rollback to storage version " +
          prevState.getLayoutVersion() +
          " using this version of the NameNode, which uses storage version " +
          FSConstants.LAYOUT_VERSION + ". " +
          "Please use the previous version of HDFS to perform the rollback.");
      }
      canRollback = true;
    }
    if (!canRollback)
      throw new IOException("Cannot rollback. None of the storage "
                            + "directories contain previous fs state.");

    // Now that we know all directories are going to be consistent
    // Do rollback for each directory containing previous state
    for (Iterator<StorageDirectory> it = storage.dirIterator(); it.hasNext();) {
      StorageDirectory sd = it.next();
      File prevDir = sd.getPreviousDir();
      if (!prevDir.exists())
        continue;

      LOG.info("Rolling back storage directory " + sd.getRoot()
               + ".\n   new LV = " + prevState.storage.getLayoutVersion()
               + "; new CTime = " + prevState.storage.getCTime());
      File tmpDir = sd.getRemovedTmp();
      assert !tmpDir.exists() : "removed.tmp directory must not exist.";
      // rename current to tmp
      File curDir = sd.getCurrentDir();
      assert curDir.exists() : "Current directory must exist.";
      NNStorage.rename(curDir, tmpDir);
      // rename previous to current
      NNStorage.rename(prevDir, curDir);

      // delete tmp dir
      NNStorage.deleteDir(tmpDir);
      LOG.info("Rollback of " + sd.getRoot()+ " is complete.");
    }
    isUpgradeFinalized = true;
    // check whether name-node can start in regular mode
    storage.verifyDistributedUpgradeProgress(StartupOption.REGULAR);
  }

  private void doFinalize(StorageDirectory sd) throws IOException {
    File prevDir = sd.getPreviousDir();
    if (!prevDir.exists()) { // already discarded
      LOG.info("Directory " + prevDir + " does not exist.");
      LOG.info("Finalize upgrade for " + sd.getRoot()+ " is not required.");
      return;
    }
    LOG.info("Finalizing upgrade for storage directory " 
             + sd.getRoot() + "."
             + (storage.getLayoutVersion()==0 ? "" :
                   "\n   cur LV = " + storage.getLayoutVersion()
                   + "; cur CTime = " + storage.getCTime()));
    assert sd.getCurrentDir().exists() : "Current directory must exist.";
    final File tmpDir = sd.getFinalizedTmp();
    // rename previous to tmp and remove
    NNStorage.rename(prevDir, tmpDir);
    NNStorage.deleteDir(tmpDir);
    isUpgradeFinalized = true;
    LOG.info("Finalize upgrade for " + sd.getRoot()+ " is complete.");
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
    isUpgradeFinalized = true;
    namesystem.setUpgradeStartTime(0);
  }

  boolean isUpgradeFinalized() {
    return isUpgradeFinalized;
  }

  public FSEditLog getEditLog() {
    return editLog;
  }
  
  void openEditLog() throws IOException {
    if (editLog == null) {
      throw new IOException("EditLog must be initialized");
    }
    if (!editLog.isOpen()) {
      editLog.open();
      storage.writeTransactionIdFileToStorage(editLog.getCurSegmentTxId());
    }
  };

  /**
   * Choose latest image from one of the directories,
   * load it and merge with the edits from that directory.
   * 
   * @return whether the image should be saved
   * @throws IOException
   */
  boolean loadFSImage() throws IOException {
    FSImageStorageInspector inspector = storage.readAndInspectDirs();
    isUpgradeFinalized = inspector.isUpgradeFinalized();

    FSImageStorageInspector.FSImageFile imageFile = inspector.getLatestImage();
    boolean needToSave = inspector.needToSave();

    Iterable<EditLogInputStream> editStreams = null;
    editLog.recoverUnclosedStreams();
    
    if (LayoutVersion.supports(Feature.TXID_BASED_LAYOUT, getLayoutVersion())) {
      LOG.info("Load Image: checkpoint txid: " + imageFile.getCheckpointTxId()
          + " max seen: " + inspector.getMaxSeenTxId());
      editStreams = editLog.selectInputStreams(
          imageFile.getCheckpointTxId() + 1, inspector.getMaxSeenTxId());
    } else {
      editStreams = FSImagePreTransactionalStorageInspector.getEditLogStreams(
          storage, conf);
    }

    LOG.info("Load Image: planning to load image :\n" + imageFile);
    for (EditLogInputStream l : editStreams) {
      LOG.info("Load Image: planning to load edit stream: " + l);
    }

    try {
      StorageDirectory sdForProperties = imageFile.sd;
      sdForProperties.read();

      if (LayoutVersion.supports(Feature.TXID_BASED_LAYOUT, getLayoutVersion())) {
        // For txid-based layout, we should have a .md5 file
        // next to the image file
        loadFSImage(imageFile.getFile());
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
        loadFSImage(imageFile.getFile(), new MD5Hash(md5));
      } else {
        // We don't have any record of the md5sum
        loadFSImage(imageFile.getFile(), null);
      }
    } catch (IOException ioe) {
      FSEditLog.closeAllStreams(editStreams);
      throw new IOException("Failed to load image from " + imageFile, ioe);
    }
           
    editLog.setLastWrittenTxId(storage.getMostRecentCheckpointTxId());

    long numLoaded = loadEdits(editStreams);
    needToSave |= needsResaveBasedOnStaleCheckpoint(imageFile.getFile(),
                                                   numLoaded);             
    return needToSave;
  }
  
  /**
   * @param imageFile
   *          the image file that was loaded
   * @param numEditsLoaded
   *          the number of edits loaded from edits logs
   * @return true if the NameNode should automatically save the namespace when
   *         it is started, due to the latest checkpoint being too old.
   */
  private boolean needsResaveBasedOnStaleCheckpoint(File imageFile,
      long numEditsLoaded) {
    final long checkpointPeriod = conf.getLong("fs.checkpoint.period", 3600);
    final long checkpointTxnCount = NNStorageConfiguration.getCheckpointTxnCount(conf);
    long checkpointAge = System.currentTimeMillis() - imageFile.lastModified();
    boolean needToSave = (checkpointAge > checkpointPeriod * 1000)
        || (numEditsLoaded > checkpointTxnCount);
    LOG.info("Load Image: Need to save based on stale checkpoint: " + needToSave);
    return needToSave;
  }

  /**
   * Load the image namespace from the given image file, verifying it against
   * the MD5 sum stored in its associated .md5 file.
   */
  protected void loadFSImage(File imageFile) throws IOException {
    MD5Hash expectedMD5 = MD5FileUtils.readStoredMd5ForFile(imageFile);
    if (expectedMD5 == null) {
      throw new IOException("No MD5 file found corresponding to image file "
          + imageFile);
    }
    loadFSImage(imageFile, expectedMD5);
  }

  boolean loadFSImage(File curFile, MD5Hash expectedMd5) throws IOException {
    assert curFile != null : "curFile is null";

    FSImageFormat.Loader loader = new FSImageFormat.Loader(
        namesystem.getConf(), namesystem, storage);
    loader.load(curFile, null);
    saveNamespaceContext.set(null, loader.getLoadedImageTxId());
    // Check that the image digest we loaded matches up with what
    // we expected
    MD5Hash readImageMd5 = loader.getLoadedImageMd5();
    if (expectedMd5 != null && !expectedMd5.equals(readImageMd5)) {
      throw new IOException("Image file " + curFile
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
      LOG.info("Load Image: Reading edits: " + editIn + " last applied txid#: "
          + lastAppliedTxId);
      numLoaded += loader.loadFSEdits(editIn, lastAppliedTxId);  
      lastAppliedTxId = loader.getLastAppliedTxId();
    }
    editLog.setLastWrittenTxId(lastAppliedTxId);
    LOG.info("Load Image: Number of edit transactions loaded: "
        + numLoaded + " last applied txid: " + lastAppliedTxId);

    // update the counts
    namesystem.dir.updateCountForINodeWithQuota();    
    return numLoaded;
  }

  // for snapshot
  void saveFSImage(String dest, DataOutputStream fstream) throws IOException { 
    saveNamespaceContext.set(namesystem, editLog.getLastWrittenTxId());
    FSImageFormat.Saver saver = new FSImageFormat.Saver(saveNamespaceContext);
    FSImageCompression compression = FSImageCompression.createCompression(namesystem.getConf(), false);
    saver.save(new File(dest), compression, fstream);
  }
  
  /**
   * Save the contents of the FS image to the file.
   */
  void saveFSImage(SaveNamespaceContext context, StorageDirectory sd, boolean forceUncompressed)
      throws IOException {
    LOG.info("Saving image to: "  + sd.getRoot().getAbsolutePath());
    long txid = context.getTxId();
    File newFile = NNStorage.getStorageFile(sd, NameNodeFile.IMAGE_NEW, txid);
    File dstFile = NNStorage.getStorageFile(sd, NameNodeFile.IMAGE, txid);
    
    FSImageFormat.Saver saver = new FSImageFormat.Saver(context);
    FSImageCompression compression = FSImageCompression.createCompression(conf, forceUncompressed);
    saver.save(newFile, compression);
    
    MD5FileUtils.saveMD5File(dstFile, saver.getSavedDigest());
    InjectionHandler.processEvent(InjectionEvent.FSIMAGE_SAVED_IMAGE, txid);
    storage.setCheckpointImageDigest(txid, saver.getSavedDigest());
  }
  
  private class FSImageSaver implements Runnable {
    private SaveNamespaceContext context;
    private StorageDirectory sd;
    private File imageFile;
    private boolean forceUncompressed;
    
    FSImageSaver(SaveNamespaceContext ctx, StorageDirectory sd, boolean forceUncompressed) {
      this(ctx, sd, forceUncompressed, NameNodeFile.IMAGE_NEW);
    }

    FSImageSaver(SaveNamespaceContext ctx, StorageDirectory sd, boolean forceUncompressed, NameNodeFile type) {
      this.context = ctx;
      this.sd = sd;
      this.imageFile = NNStorage.getImageFile(sd, ctx.getTxId());
      this.forceUncompressed = forceUncompressed;
    }
    
    public String toString() {
      return "FSImage saver for " + imageFile.getAbsolutePath();
    }
    
    public void run() {
      try {
        InjectionHandler
          .processEvent(InjectionEvent.FSIMAGE_STARTING_SAVER_THREAD);
        
        saveFSImage(context, sd, forceUncompressed);

      } catch (SaveNamespaceCancelledException ex) {
        LOG.warn("FSImageSaver: - cancelling operation");
      } catch (IOException ex) {
        LOG.error("Unable to write image to " + imageFile.getAbsolutePath(), ex);
        context.reportErrorOnStorageDirectory(sd);
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
        storage.writeTransactionIdFileToStorage(imageTxId + 1);
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
    for (Iterator<StorageDirectory> it
           = storage.dirIterator(NameNodeDirType.IMAGE); it.hasNext();) {
      StorageDirectory sd = it.next();
      FSImageSaver saver = new FSImageSaver(saveNamespaceContext, sd, forceUncompressed);
      Thread saveThread = new Thread(saver, saver.toString());
      saveThreads.add(saveThread);
      saveThread.start();
    }
    waitForThreads(saveThreads);
    saveThreads.clear();
    storage.reportErrorsOnDirectories(saveNamespaceContext.getErrorSDs());

    if (storage.getNumStorageDirs(NameNodeDirType.IMAGE) == 0) {
      throw new IOException(
        "Failed to save in any storage directories while saving namespace.");
    }
    
    if (saveNamespaceContext.isCancelled()) {
      deleteCheckpoint(saveNamespaceContext.getTxId());  
      saveNamespaceContext.checkCancelled();
    } 
    renameCheckpoint(txid, storage);
    
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
    editLog.formatNonFileJournals(storage);
    saveFSImageInAllDirs(-1, false);
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
    // Record this log segment ID in all of the storage directories, so
    // we won't miss this log segment on a restart if the edits directories
    // go missing.
    storage.writeTransactionIdFileToStorage(getEditLog().getCurSegmentTxId());
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
    FSImage.saveDigestAndRenameCheckpointImage(sig.mostRecentCheckpointTxId, sig.imageDigest, storage);
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
  static synchronized void saveDigestAndRenameCheckpointImage(
      long txid, MD5Hash digest, NNStorage storage) throws IOException {
    if (!digest.equals(storage.getCheckpointImageDigest(txid))) {
      throw new IOException(
          "Checkpoint image is corrupt: expecting an MD5 checksum of" +
              digest + " but is " + storage.getCheckpointImageDigest(txid));
    }
       
    renameCheckpoint(txid, storage);
    List<StorageDirectory> badSds = new ArrayList<StorageDirectory>();
    
    for (StorageDirectory sd : storage.dirIterable(NameNodeDirType.IMAGE)) {
      File imageFile = NNStorage.getImageFile(sd, txid);
      try {
        MD5FileUtils.saveMD5File(imageFile, digest);
      } catch (IOException ioe) {
        badSds.add(sd);
      }
    }
    storage.reportErrorsOnDirectories(badSds);
    
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
  
  /**
   * Renames new image
   */
  private static void renameCheckpoint(long txid, NNStorage storage) throws IOException {
    ArrayList<StorageDirectory> al = null;

    for (StorageDirectory sd : storage.dirIterable(NameNodeDirType.IMAGE)) {
      try {
        renameCheckpointInDir(sd, txid);
      } catch (IOException ioe) {
        LOG.warn("Unable to rename checkpoint in " + sd, ioe);
        if (al == null) {
          al = new ArrayList<StorageDirectory>();
        }
        al.add(sd);
      }
    }
    if(al != null) storage.reportErrorsOnDirectories(al);
  }
  
  private static void renameCheckpointInDir(StorageDirectory sd, long txid)
      throws IOException {
    File ckpt = NNStorage.getStorageFile(sd, NameNodeFile.IMAGE_NEW, txid);
    File curFile = NNStorage.getStorageFile(sd, NameNodeFile.IMAGE, txid);
    // renameTo fails on Windows if the destination file 
    // already exists.
    LOG.info("Renaming  " + ckpt.getAbsolutePath() + " to "
        + curFile.getAbsolutePath());
    if (!ckpt.renameTo(curFile)) {
      if (!curFile.delete() || !ckpt.renameTo(curFile)) {
        throw new IOException("renaming  " + ckpt.getAbsolutePath() + " to "  + 
            curFile.getAbsolutePath() + " FAILED");
      }
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
                                                    log.getEndTxId()));
       }
    }
    dstImage.loadEdits(editsStreams);
  }
}
