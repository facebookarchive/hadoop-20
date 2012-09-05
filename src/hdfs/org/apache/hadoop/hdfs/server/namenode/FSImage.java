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

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.protocol.FSConstants;
import org.apache.hadoop.hdfs.server.common.HdfsConstants;
import org.apache.hadoop.hdfs.server.common.InconsistentFSStateException;
import org.apache.hadoop.hdfs.server.common.Storage;
import org.apache.hadoop.hdfs.server.common.Storage.StorageDirType;
import org.apache.hadoop.hdfs.server.common.Storage.StorageDirectory;
import org.apache.hadoop.hdfs.server.common.Storage.StorageState;
import org.apache.hadoop.hdfs.server.common.StorageInfo;
import org.apache.hadoop.hdfs.server.common.HdfsConstants.StartupOption;
import org.apache.hadoop.hdfs.server.namenode.NNStorage.NameNodeDirType;
import org.apache.hadoop.hdfs.server.namenode.NNStorage.NameNodeFile;
import org.apache.hadoop.hdfs.util.DataTransferThrottler;
import org.apache.hadoop.hdfs.util.InjectionEvent;
import org.apache.hadoop.hdfs.util.InjectionHandler;
import org.apache.hadoop.io.MD5Hash;

/**
 * FSImage handles checkpointing and logging of the namespace edits.
 * 
 */
public class FSImage {

  static final Log LOG = LogFactory.getLog(FSImage.class.getName());
  
  NNStorage storage;
  Configuration conf;

  private static final SimpleDateFormat DATE_FORM =
    new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
  
  private final SaveNamespaceContext saveNamespaceContext 
    = new SaveNamespaceContext();

  // checkpoint states
  enum CheckpointStates {
    START(0), 
    ROLLED_EDITS(1), 
    UPLOAD_START(2), 
    UPLOAD_DONE(3); 

    private final int code;

    CheckpointStates(int code) {
      this.code = code;
    }

    public int serialize() {
      return this.code;
    }

    public static CheckpointStates deserialize(int code) {
      switch(code) {
      case 0:
        return CheckpointStates.START;
      case 1:
        return CheckpointStates.ROLLED_EDITS;
      case 2:
        return CheckpointStates.UPLOAD_START;
      case 3:
        return CheckpointStates.UPLOAD_DONE;
      default: // illegal
        return null;
      }
    }
  }
  
  protected FSNamesystem namesystem = null;
  FSEditLog editLog = null;
  private boolean isUpgradeFinalized = false;
  
  /**
   * flag that controls if we try to restore failed storages
   */
  public void setRestoreFailedStorage(boolean val) {
    LOG.info("enabled failed storage replicas restore");
    storage.setRestoreFailedStorage(val);
  }
  
  public boolean getRestoreFailedStorage() {
    return storage.getRestoreFailedStorage();
  }

  DataTransferThrottler imageTransferThrottler = null; // throttle image transfer
  
  /**
   * Can fs-image be rolled?
   */
  volatile CheckpointStates ckptState = FSImage.CheckpointStates.START; 

  /**
   */
  FSImage() throws IOException{
    this((FSNamesystem)null);
  }

  FSImage(FSNamesystem ns) throws IOException {
    storage = new NNStorage(new StorageInfo());
    this.editLog = new FSEditLog(this);
    setFSNamesystem(ns);
  }

  /**
   * Constructor
   * @param conf Configuration
   */
  FSImage(Configuration conf) throws IOException {
    this();
    this.conf = conf;
    long transferBandwidth = conf.getLong(
        HdfsConstants.DFS_IMAGE_TRANSFER_RATE_KEY,
        HdfsConstants.DFS_IMAGE_TRANSFER_RATE_DEFAULT);

    if (transferBandwidth > 0) {
      this.imageTransferThrottler = new DataTransferThrottler(transferBandwidth);
    }
  }
  
  /**
   */
  FSImage(Configuration conf, Collection<File> fsDirs, Collection<File> fsEditsDirs) 
    throws IOException {
    this(conf);
    this.conf = conf;
    storage = new NNStorage(conf, this, fsDirs, fsEditsDirs);
  }

  /**
   * Represents an Image (image and edit file).
   */
  public FSImage(File imageDir) throws IOException {
    this();
    ArrayList<File> dirs = new ArrayList<File>(1);
    ArrayList<File> editsDirs = new ArrayList<File>(1);
    dirs.add(imageDir);
    editsDirs.add(imageDir);
    storage = new NNStorage(new Configuration(), this, dirs, editsDirs);
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
  
  List<StorageDirectory> getRemovedStorageDirs() {
	  return storage.getRemovedStorageDirs();
  }
  
  File getEditFile(StorageDirectory sd) {
    return NNStorage.getStorageFile(sd, NameNodeFile.EDITS);
  }
  
  File getEditNewFile(StorageDirectory sd) {
    return NNStorage.getStorageFile(sd, NameNodeFile.EDITS_NEW);
  }

  File[] getFileNames(NameNodeFile type, NameNodeDirType dirType) {
    ArrayList<File> list = new ArrayList<File>();
    Iterator<StorageDirectory> it = (dirType == null) ? storage.dirIterator() :
                                    storage.dirIterator(dirType);
    for ( ;it.hasNext(); ) {
      list.add(NNStorage.getStorageFile(it.next(), type));
    }
    return list.toArray(new File[list.size()]);
  }

  File[] getImageFiles() {
    return getFileNames(NameNodeFile.IMAGE, NameNodeDirType.IMAGE);
  }

  File[] getEditsFiles() {
    return getFileNames(NameNodeFile.EDITS, NameNodeDirType.EDITS);
  }

  File[] getEditsNewFiles() {
    return getFileNames(NameNodeFile.EDITS_NEW, NameNodeDirType.EDITS);
  }

  File[] getTimeFiles() {
    return getFileNames(NameNodeFile.TIME, null);
  }

  /**
   * Get the MD5 digest of the current image
   * @return the MD5 digest of the current image
   */ 
  MD5Hash getImageDigest() {
    return storage.getImageDigest();
  }

  void setImageDigest(MD5Hash imageDigest) {
    this.storage.setImageDigest(imageDigest);
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
    Collection<File> editsDirs = storage.getEditsDirectories();

    // none of the data dirs exist
    if((imageDirs.size() == 0 || editsDirs.size() == 0) 
                             && startOpt != StartupOption.IMPORT)  
      throw new IOException(
          "All specified directories are not accessible or do not exist.");
    
    storage.setUpgradeManager(namesystem.upgradeManager);
    
    // 1. For each data directory calculate its state and 
    // check whether all is consistent before transitioning.
    Map<StorageDirectory, StorageState> dataDirStates = 
             new HashMap<StorageDirectory, StorageState>();
    boolean isFormatted = recoverStorageDirs(startOpt, dataDirStates);
    
    if (!isFormatted && startOpt != StartupOption.ROLLBACK 
                     && startOpt != StartupOption.IMPORT) {
      throw new IOException("NameNode is not formatted.");      
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

    // Do upgrade for each directory
    long oldCTime = storage.getCTime();
    this.storage.cTime = FSNamesystem.now();  // generate new cTime for the state
    int oldLV = storage.getLayoutVersion();
    this.storage.layoutVersion = FSConstants.LAYOUT_VERSION;
    this.storage.checkpointTime = FSNamesystem.now();
    
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

    saveFSImageInAllDirs(editLog.getCurrentTxId(), false);

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
    editLog.open();
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
    Collection<File> checkpointDirs =
      FSImage.getCheckpointDirs(conf, null);
    Collection<File> checkpointEditsDirs =
      FSImage.getCheckpointEditsDirs(conf, null);

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
                                    checkpointDirs, checkpointEditsDirs);
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
    realImage.getEditLog().setStartTransactionId(ckptImage.getEditLog().getCurrentTxId());

    namesystem.dir.fsImage = realImage;

    // and save it but keep the same checkpointTime
    // parameters
    saveNamespace(false);
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

  /**
   * Record new checkpoint time in order to
   * distinguish healthy directories from the removed ones.
   * If there is an error writing new checkpoint time, the corresponding
   * storage directory is removed from the list.
   */
  void incrementCheckpointTime() {
    this.storage.checkpointTime++;
    
    // Write new checkpoint time in all storage directories
    for(Iterator<StorageDirectory> it =
                            storage.dirIterator(); it.hasNext();) {
      StorageDirectory sd = it.next();
      try {
        storage.writeCheckpointTime(sd);
      } catch(IOException e) {
        // Close any edits stream associated with this dir and remove directory
        LOG.warn("incrementCheckpointTime failed on " + sd.getRoot().getPath() + ";type="+sd.getStorageDirType());
        if (sd.getStorageDirType().isOfType(NameNodeDirType.EDITS))
          editLog.processIOError(sd);

        //add storage to the removed list
        storage.reportErrorsOnDirectory(sd);
        it.remove();
      }
    }
  }

  public FSEditLog getEditLog() {
    return editLog;
  }

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

    Collection<EditLogInputStream> editStreams = null;
    
    editStreams = FSImagePreTransactionalStorageInspector.getEditLogStreams(
        storage, conf);

    LOG.debug("Planning to load image :\n" + imageFile);
    for (EditLogInputStream l : editStreams) {
      LOG.debug("\t Planning to load edit stream: " + l);
    }

    StorageDirectory sdForProperties = imageFile.sd;
    sdForProperties.read();

    loadFSImage(imageFile.getFile());

    long numLoaded = loadEdits(editStreams);
    if (numLoaded > 0) {
      needToSave |= inspector.forceSave();
    }
    return needToSave;
  }

  boolean loadFSImage(File curFile) throws IOException {
    assert curFile != null : "curFile is null";

    FSImageFormat.Loader loader = new FSImageFormat.Loader(namesystem.getConf(), namesystem, storage);
    loader.load(curFile, null);
    saveNamespaceContext.set(null, loader.getLoadedImageTxId());
    MD5Hash readImageMd5 = loader.getLoadedImageMd5();
    
    if (this.storage.newImageDigest) {
      this.setImageDigest(readImageMd5); // set this fsimage's checksum
    } else if (!this.getImageDigest().equals(readImageMd5)) {
      throw new IOException("Image file " + curFile + " is corrupt!");
    }
    
    storage.setMostRecentCheckpointTxId(loader.getLoadedImageTxId());
    return loader.getNeedToSave();
  }
  /**
   * Load in the filesystem imagefrom file. It's a big list of
   * filenames and blocks.  Return whether we should
   * "re-save" and consolidate the edit-logs
   */
  boolean loadFSImage(File curFile, DataInputStream dis) throws IOException {
    assert curFile != null : "curFile is null";

    FSImageFormat.Loader loader = new FSImageFormat.Loader(namesystem.getConf(), namesystem, storage);
    loader.load(curFile, dis);
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

    long currentTxId = storage.getMostRecentCheckpointTxId() + 1;
    int numLoaded = 0;


    FSEditLogLoader loader = new FSEditLogLoader(namesystem);
      
    // Load latest edits
    for (EditLogInputStream editIn : editStreams) {
      LOG.info("Reading " + editIn + " expecting start txid #" + currentTxId);
      numLoaded += loader.loadFSEdits(editIn, currentTxId);  
      currentTxId = loader.getCurrentTxId();
    }
    editLog.setStartTransactionId(currentTxId);
    LOG.info("FSImage loader - Number of edit transactions loaded: "+numLoaded);

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
      this.imageFile = NNStorage.getStorageFile(sd, type);
      this.forceUncompressed = forceUncompressed;
    }
    
    public String toString() {
      return "FSImage saver for " + imageFile.getAbsolutePath();
    }
    
    public void run() {
      try {
        InjectionHandler
            .processEvent(InjectionEvent.FSIMAGE_STARTING_SAVER_THREAD);

        saveCurrent(context, sd, forceUncompressed);

      } catch (SaveNamespaceCancelledException ex) {
        LOG.warn("FSImageSaver: - cancelling operation");
      } catch (IOException ex) {
        LOG.error("Unable to write image to " + imageFile.getAbsolutePath());
        context.reportErrorOnStorageDirectory(sd);
      }
    }
  }

  /**
   * Save the contents of the FS image
   * and create empty edits.
   */
  public void saveNamespace(boolean renewCheckpointTime) throws IOException {
    saveNamespace(false, renewCheckpointTime);
  }

  /**
   * Save the contents of the FS image
   * and create empty edits.
   * If forceUncompressed, the image will be saved uncompressed regardless of
   * the fsimage compression configuration.
   */
  public void saveNamespace(boolean forUncompressed, boolean renewCheckpointTime)
    throws IOException {
    
    InjectionHandler
        .processEvent(InjectionEvent.FSIMAGE_STARTING_SAVE_NAMESPACE);
    try {

      // try to restore all failed edit logs here
      assert editLog != null : "editLog must be initialized";
      storage.attemptRestoreRemovedStorage();
      
      InjectionHandler
        .processEvent(InjectionEvent.FSIMAGE_STARTING_SAVE_NAMESPACE);
      
      editLog.close(); // close all open streams before truncating
      if (renewCheckpointTime)
        this.storage.checkpointTime = FSNamesystem.now();
      // mv current -> lastcheckpoint.tmp
      for (Iterator<StorageDirectory> it = storage.dirIterator(); it.hasNext();) {
        StorageDirectory sd = it.next();
        try {
          moveCurrent(sd);
        } catch (IOException ex) {
          LOG.error("Unable to move current for " + sd.getRoot(), ex);
          storage.reportErrorsOnDirectory(sd);
        }
      }
      
      // for testing only - we will wait until interruption comes
      InjectionHandler
          .processEvent(InjectionEvent.FSIMAGE_CREATING_SAVER_THREADS);
      
      saveFSImageInAllDirs(editLog.getCurrentTxId(), forUncompressed);
      
      // -NOTE-
      // If NN has image-only and edits-only storage directories and fails here
      // the image will have the latest namespace state.
      // During startup the image-only directories will recover by discarding
      // lastcheckpoint.tmp, while
      // the edits-only directories will recover by falling back
      // to the old state contained in their lastcheckpoint.tmp.
      // The edits directories should be discarded during startup because their
      // checkpointTime is older than that of image directories.
      
      // recreate edits in current
      InjectionHandler.processEvent(InjectionEvent.FSIMAGE_SN_CLEANUP);
      for (Iterator<StorageDirectory> it = storage.dirIterator(NameNodeDirType.EDITS);
                                                                it.hasNext();) {
        StorageDirectory sd = it.next();
        try {
          saveCurrent(saveNamespaceContext, sd, forUncompressed);
        } catch (IOException ex) {
          LOG.error("Unable to save edits for " + sd.getRoot(), ex);
          storage.reportErrorOnFile(sd.getRoot());
        }
      }
      
      // mv lastcheckpoint.tmp -> previous.checkpoint
      for (Iterator<StorageDirectory> it = storage.dirIterator(); it.hasNext();) {
        StorageDirectory sd = it.next();
        try {
          moveLastCheckpoint(sd);
        } catch (IOException ex) {
          LOG.error("Unable to move last checkpoint for " + sd.getRoot(), ex);
          storage.reportErrorOnFile(sd.getRoot());
        }
      }
      if (!editLog.isOpen()) editLog.open();
      storage.reportErrorsOnDirectories(saveNamespaceContext.getErrorSDs());
      ckptState = CheckpointStates.UPLOAD_DONE;
    } finally {
      saveNamespaceContext.clear();
    }
  }
  
  protected synchronized void saveFSImageInAllDirs(long txid, boolean forceUncompressed)
      throws IOException {    
    if (storage.getNumStorageDirs(NameNodeDirType.IMAGE) == 0) {
      throw new IOException("No image directories available!");
    }
    
    saveNamespaceContext.set(namesystem, editLog.getLastWrittenTxId()); 
    
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
    

    if (storage.getNumStorageDirs(NameNodeDirType.IMAGE) == 0) {
      throw new IOException(
        "Failed to save in any storage directories while saving namespace.");
    }
    
    if (saveNamespaceContext.isCancelled()) {
      storage.reportErrorsOnDirectories(saveNamespaceContext.getErrorSDs());
      deleteCancelledCheckpoint();
      if (!editLog.isOpen()) editLog.open();  
      saveNamespaceContext.checkCancelled();
    } 
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
  
  /**
   * Save current image and empty journal into {@code current} directory.
   */
  protected void saveCurrent(SaveNamespaceContext ctx, StorageDirectory sd, boolean forceUncompressed)
    throws IOException {
    File curDir = sd.getCurrentDir();
    LOG.info("Saving image to: "  + sd.getRoot().getAbsolutePath());
    NameNodeDirType dirType = (NameNodeDirType) sd.getStorageDirType();
    // save new image or new edits
    if (!curDir.exists() && !curDir.mkdir())
      throw new IOException("Cannot create directory " + curDir);
    if (dirType.isOfType(NameNodeDirType.IMAGE)) { 
      FSImageFormat.Saver saver = new FSImageFormat.Saver(ctx);
      FSImageCompression compression = FSImageCompression.createCompression(namesystem.getConf(), forceUncompressed);
      saver.save(NNStorage.getStorageFile(sd, NameNodeFile.IMAGE), compression);
      this.setImageDigest(saver.getSavedDigest());
    }
    if (dirType.isOfType(NameNodeDirType.EDITS))
      editLog.createEditLogFile(NNStorage.getStorageFile(sd, NameNodeFile.EDITS));
    // write version and time files
    sd.write();
    storage.setMostRecentCheckpointTxId(ctx.getTxId());
  }

  /*
   * Move {@code current} to {@code lastcheckpoint.tmp} and recreate empty
   * {@code current}. {@code current} is moved only if it is well formatted,
   * that is contains VERSION file.
   * 
   * @see org.apache.hadoop.hdfs.server.common.Storage.StorageDirectory#
   * getLastCheckpointTmp()
   * 
   * @see org.apache.hadoop.hdfs.server.common.Storage.StorageDirectory#
   * getPreviousCheckpoint()
   */
  protected void moveCurrent(StorageDirectory sd) throws IOException {
    File curDir = sd.getCurrentDir();
    File tmpCkptDir = sd.getLastCheckpointTmp();
    // mv current -> lastcheckpoint.tmp
    // only if current is formatted - has VERSION file
    if (sd.getVersionFile().exists()) {
      assert curDir.exists() : curDir + " directory must exist.";
      assert !tmpCkptDir.exists() : tmpCkptDir + " directory must not exist.";
      NNStorage.rename(curDir, tmpCkptDir);
    }
    // recreate current
    if (!curDir.exists() && !curDir.mkdir())
      throw new IOException("Cannot create directory " + curDir);
  }
  
  /**
   * Move {@code lastcheckpoint.tmp} to {@code previous.checkpoint}
   * 
   * @see org.apache.hadoop.hdfs.server.common.Storage.StorageDirectory#
   * getPreviousCheckpoint()
   * @see org.apache.hadoop.hdfs.server.common.Storage.StorageDirectory#
   * getLastCheckpointTmp()
   */
  protected void moveLastCheckpoint(StorageDirectory sd) throws IOException {
    File tmpCkptDir = sd.getLastCheckpointTmp();
    File prevCkptDir = sd.getPreviousCheckpoint();
    // remove previous.checkpoint
    if (prevCkptDir.exists())
      NNStorage.deleteDir(prevCkptDir);
    // rename lastcheckpoint.tmp -> previous.checkpoint
    if (tmpCkptDir.exists())
      NNStorage.rename(tmpCkptDir, prevCkptDir);
  }

  /** Create new dfs name directory.  Caution: this destroys all files
   * in this filesystem. */
  void format(StorageDirectory sd) throws IOException {
    saveNamespaceContext.set(namesystem, -1);
    sd.clearDirectory();
    sd.lock();
    try {
      saveCurrent(saveNamespaceContext, sd, false);
    } finally {
      sd.unlock();
    }
    LOG.info("Storage directory " + sd.getRoot()
             + " has been successfully formatted.");
  }

  public void format() throws IOException {
    storage.format();
  }

  /**
   * Moves fsimage.ckpt to fsImage and edits.new to edits
   * Reopens the new edits file.
   * 
   * @param newImageSignature the signature of the new image
   */
  void rollFSImage(CheckpointSignature newImageSignature) throws IOException {
    MD5Hash newImageDigest = newImageSignature.getImageDigest();
    if (!newImageDigest.equals(storage.checkpointImageDigest)) {
      throw new IOException(
          "Checkpoint image is corrupt: expecting an MD5 checksum of" +
          newImageDigest + " but is " + storage.checkpointImageDigest);
    }
    rollFSImage(newImageSignature.getImageDigest(), 
        newImageSignature.mostRecentCheckpointTxId);
  }
  
  private void rollFSImage(MD5Hash newImageDigest, 
      long mostRecentCheckpointTxId)  throws IOException {
    if (ckptState != CheckpointStates.UPLOAD_DONE) {
      throw new IOException("Cannot roll fsImage before rolling edits log.");
    }
    //
    // First, verify that edits.new and fsimage.ckpt exists in all
    // checkpoint directories.
    //
    if (!editLog.existsNew()) {
      throw new IOException("New Edits file does not exist");
    }
    for (Iterator<StorageDirectory> it = 
                       storage.dirIterator(NameNodeDirType.IMAGE); it.hasNext();) {
      StorageDirectory sd = it.next();
      File ckpt = NNStorage.getStorageFile(sd, NameNodeFile.IMAGE_NEW);
      if (!ckpt.exists()) {
        throw new IOException("Checkpoint file " + ckpt +
                              " does not exist");
      }
    }
    editLog.purgeEditLog(); // renamed edits.new to edits
    LOG.debug("rollFSImage after purgeEditLog: storageList=" + storage.listStorageDirectories());
    //
    // Renames new image
    //
    for (Iterator<StorageDirectory> it = 
                       storage.dirIterator(NameNodeDirType.IMAGE); it.hasNext();) {
      StorageDirectory sd = it.next();
      File ckpt = NNStorage.getStorageFile(sd, NameNodeFile.IMAGE_NEW);
      File curFile = NNStorage.getStorageFile(sd, NameNodeFile.IMAGE);
      // renameTo fails on Windows if the destination file 
      // already exists.
      LOG.debug("renaming  " + ckpt.getAbsolutePath() + " to "  + curFile.getAbsolutePath());
      if (!ckpt.renameTo(curFile)) {
        curFile.delete();
        if (!ckpt.renameTo(curFile)) {
          LOG.warn("renaming  " + ckpt.getAbsolutePath() + " to "  + 
              curFile.getAbsolutePath() + " FAILED");
          
          // Close edit stream, if this directory is also used for edits
          if (sd.getStorageDirType().isOfType(NameNodeDirType.EDITS))
            editLog.processIOError(sd);
          
          // add storage to the removed list
          storage.reportErrorsOnDirectory(sd);
          it.remove();
        }
      }
    }

    //
    // Updates the fstime file on all directories (fsimage and edits)
    // and write version file
    //
    this.storage.layoutVersion = FSConstants.LAYOUT_VERSION;
    this.storage.checkpointTime = FSNamesystem.now();
    this.setImageDigest(newImageDigest);
    for (Iterator<StorageDirectory> it = 
                           storage.dirIterator(); it.hasNext();) {
      StorageDirectory sd = it.next();
      // delete old edits if sd is the image only the directory
      if (!sd.getStorageDirType().isOfType(NameNodeDirType.EDITS)) {
        File editsFile = NNStorage.getStorageFile(sd, NameNodeFile.EDITS);
        editsFile.delete();
      }
      // delete old fsimage if sd is the edits only the directory
      if (!sd.getStorageDirType().isOfType(NameNodeDirType.IMAGE)) {
        File imageFile = NNStorage.getStorageFile(sd, NameNodeFile.IMAGE);
        imageFile.delete();
      }
      try {
        sd.write();
      } catch (IOException e) {
        LOG.error("Cannot write file " + sd.getRoot(), e);
        // Close edit stream, if this directory is also used for edits
        if (sd.getStorageDirType().isOfType(NameNodeDirType.EDITS))
          editLog.processIOError(sd);
      //add storage to the removed list
        storage.reportErrorsOnDirectory(sd);
        it.remove();
      }
    }
    ckptState = FSImage.CheckpointStates.START;
    storage.setMostRecentCheckpointTxId(mostRecentCheckpointTxId);
  }
  
  /**
   * Deletes the checkpoint file in every storage directory,
   * since the checkpoint was cancelled. Moves lastcheckpoint.tmp -> current
   */
  private void deleteCancelledCheckpoint() throws IOException {
    ArrayList<StorageDirectory> errorDirs = new ArrayList<StorageDirectory>();
    for (Iterator<StorageDirectory> it = storage.dirIterator(); it.hasNext();) {
      StorageDirectory sd = it.next();
      deleteCancelledChecpointDir(sd, errorDirs);
    }
    storage.reportErrorsOnDirectories(errorDirs);
  }

  /**
   * Deletes the checkpoint file in given storage directory.
   * Moves lastcheckpoint.tmp -> current
   */
  private void deleteCancelledChecpointDir(StorageDirectory sd, 
      Collection<StorageDirectory> errorDirs) throws IOException {
    LOG.info("Reverting checkpoint for : " + sd.getCurrentDir());
    try {
      File tmpCkptDir = sd.getLastCheckpointTmp();
      File curDir = sd.getCurrentDir();
      if (!tmpCkptDir.exists()) {
        LOG.warn("Reverting checkpoint - lastcheckpoint.tmp "
            + "does not exist for storage directory: " + sd);
        errorDirs.add(sd);
        return;
      }
      // remove current
      if (curDir.exists())
        NNStorage.deleteDir(curDir);
      // rename lastcheckpoint.tmp -> current
      NNStorage.rename(tmpCkptDir, curDir);
    } catch (IOException e) {
      LOG.warn("Unable to revert checkpoint for : " + sd.getCurrentDir(), e);
      errorDirs.add(sd);
    }
  }

  CheckpointSignature rollEditLog() throws IOException {
    getEditLog().rollEditLog();
    ckptState = CheckpointStates.ROLLED_EDITS;
    return new CheckpointSignature(this);
  }

  /**
   * This is called just before a new checkpoint is uploaded to the
   * namenode.
   */
  void validateCheckpointUpload(CheckpointSignature sig) throws IOException {
    if (ckptState != CheckpointStates.ROLLED_EDITS) {
      throw new IOException("Namenode is not expecting an new image " +
                             ckptState);
    } 
    // verify token
    long modtime = getEditLog().getFsEditTime();
    if (sig.editsTime != modtime) {
      throw new IOException("Namenode has an edit log with timestamp of " +
                            DATE_FORM.format(new Date(modtime)) +
                            " but new checkpoint was created using editlog " +
                            " with timestamp " + 
                            DATE_FORM.format(new Date(sig.editsTime)) + 
                            ". Checkpoint Aborted.");
    }
    sig.validateStorageInfo(storage);
    ckptState = FSImage.CheckpointStates.UPLOAD_START;
  }

  /**
   * This is called when a checkpoint upload finishes successfully.
   */
  synchronized void checkpointUploadDone(MD5Hash checkpointImageMd5) {
    storage.checkpointImageDigest = checkpointImageMd5;
    ckptState = CheckpointStates.UPLOAD_DONE;
  }

  void close() throws IOException {
    getEditLog().close(true);
    storage.unlockAll();
  }

  /**
   * Return the name of the image file.
   */
  File getFsImageName() {
    return storage.getFsImageName();
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
  
  public File getFsEditName() throws IOException {
    return getEditLog().getFsEditName();
  }
  
  public File getFsEditNewName() throws IOException {
    return getEditLog().getFsEditNewName();
  }

  File getFsTimeName() {
    StorageDirectory sd = null;
    // NameNodeFile.TIME shoul be same on all directories
    for (Iterator<StorageDirectory> it = 
             storage.dirIterator(); it.hasNext();)
      sd = it.next();
    return NNStorage.getStorageFile(sd, NameNodeFile.TIME);
  }

  /**
   * Return the name of the image file that is uploaded by periodic
   * checkpointing.
   */
  File[] getFsImageNameCheckpoint() {
    ArrayList<File> list = new ArrayList<File>();
    for (Iterator<StorageDirectory> it = 
                 storage.dirIterator(NameNodeDirType.IMAGE); it.hasNext();) {
      list.add(NNStorage.getStorageFile(it.next(), NameNodeFile.IMAGE_NEW));
    }
    return list.toArray(new File[list.size()]);
  }
  
  public void cancelSaveNamespace(String reason) {
    saveNamespaceContext.cancel(reason);
    InjectionHandler.processEvent(InjectionEvent.FSIMAGE_CANCEL_REQUEST_RECEIVED);
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
}
