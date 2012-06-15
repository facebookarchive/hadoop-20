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
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.io.RandomAccessFile;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.protocol.FSConstants;
import org.apache.hadoop.hdfs.server.common.HdfsConstants;
import org.apache.hadoop.hdfs.server.common.InconsistentFSStateException;
import org.apache.hadoop.hdfs.server.common.Storage;
import org.apache.hadoop.hdfs.server.common.StorageInfo;
import org.apache.hadoop.hdfs.server.common.UpgradeManager;
import org.apache.hadoop.hdfs.server.common.HdfsConstants.NodeType;
import org.apache.hadoop.hdfs.server.common.HdfsConstants.StartupOption;
import org.apache.hadoop.hdfs.server.namenode.EditLogFileInputStream;
import org.apache.hadoop.hdfs.util.DataTransferThrottler;
import org.apache.hadoop.hdfs.util.InjectionEvent;
import org.apache.hadoop.hdfs.util.InjectionHandler;
import org.apache.hadoop.io.MD5Hash;

/**
 * FSImage handles checkpointing and logging of the namespace edits.
 * 
 */
public class FSImage extends Storage {

  private static final SimpleDateFormat DATE_FORM =
    new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
  
  static final String MESSAGE_DIGEST_PROPERTY = "imageMD5Digest";
  
  private final SaveNamespaceContext saveNamespaceContext 
    = new SaveNamespaceContext();

  //
  // The filenames used for storing the images
  //
  enum NameNodeFile {
    IMAGE     ("fsimage"),
    TIME      ("fstime"),
    EDITS     ("edits"),
    IMAGE_NEW ("fsimage.ckpt"),
    EDITS_NEW ("edits.new");
    
    private String fileName = null;
    private NameNodeFile(String name) {this.fileName = name;}
    String getName() {return fileName;}
  }

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

  /**
   * Implementation of StorageDirType specific to namenode storage
   * A Storage directory could be of type IMAGE which stores only fsimage,
   * or of type EDITS which stores edits or of type IMAGE_AND_EDITS which 
   * stores both fsimage and edits.
   */
  static enum NameNodeDirType implements StorageDirType {
    UNDEFINED,
    IMAGE,
    EDITS,
    IMAGE_AND_EDITS;
    
    public StorageDirType getStorageDirType() {
      return this;
    }
    
    public boolean isOfType(StorageDirType type) {
      if ((this == IMAGE_AND_EDITS) && (type == IMAGE || type == EDITS))
        return true;
      return this == type;
    }
  }
  
  protected FSNamesystem namesystem = null;
  protected long checkpointTime = -1L;
  FSEditLog editLog = null;
  private boolean isUpgradeFinalized = false;
  MD5Hash imageDigest = new MD5Hash();
  private boolean newImageDigest = true;
  MD5Hash checkpointImageDigest = null;
  
  /**
   * flag that controls if we try to restore failed storages
   */
  private boolean restoreFailedStorage = false;
  public void setRestoreFailedStorage(boolean val) {
    LOG.info("enabled failed storage replicas restore");
    restoreFailedStorage=val;
  }
  
  public boolean getRestoreFailedStorage() {
    return restoreFailedStorage;
  }

  /**
   * list of failed (and thus removed) storages
   */
  protected List<StorageDirectory> removedStorageDirs = new ArrayList<StorageDirectory>();
  
  /**
   * Directories for importing an image from a checkpoint.
   */
  private Collection<File> checkpointDirs;
  private Collection<File> checkpointEditsDirs;

  DataTransferThrottler imageTransferThrottler = null; // throttle image transfer
  
  /**
   * Can fs-image be rolled?
   */
  volatile CheckpointStates ckptState = FSImage.CheckpointStates.START; 

  /**
   */
  FSImage() {
    this((FSNamesystem)null);
  }

  FSImage(FSNamesystem ns) {
    super(NodeType.NAME_NODE);
    this.editLog = new FSEditLog(this);
    setFSNamesystem(ns);
  }

  /**
   * Constructor
   * @param conf Configuration
   */
  FSImage(Configuration conf) throws IOException {
    this();
    setCheckpointDirectories(FSImage.getCheckpointDirs(conf, null),
        FSImage.getCheckpointEditsDirs(conf, null));
    long transferBandwidth = conf.getLong(
        HdfsConstants.DFS_IMAGE_TRANSFER_RATE_KEY,
        HdfsConstants.DFS_IMAGE_TRANSFER_RATE_DEFAULT);

    if (transferBandwidth > 0) {
      this.imageTransferThrottler = new DataTransferThrottler(transferBandwidth);
    }
  }
  
  /**
   */
  FSImage(Collection<File> fsDirs, Collection<File> fsEditsDirs) 
    throws IOException {
    this();
    setStorageDirectories(fsDirs, fsEditsDirs);
  }

  public FSImage(StorageInfo storageInfo) {
    super(NodeType.NAME_NODE, storageInfo);
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
    setStorageDirectories(dirs, editsDirs);
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

  void setStorageDirectories(Collection<File> fsNameDirs,
                        Collection<File> fsEditsDirs
                             ) throws IOException {
    this.storageDirs = new ArrayList<StorageDirectory>();
    this.removedStorageDirs = new ArrayList<StorageDirectory>();
   // Add all name dirs with appropriate NameNodeDirType 
    for (File dirName : fsNameDirs) {
      boolean isAlsoEdits = false;
      for (File editsDirName : fsEditsDirs) {
        if (editsDirName.compareTo(dirName) == 0) {
          isAlsoEdits = true;
          fsEditsDirs.remove(editsDirName);
          break;
        }
      }
      NameNodeDirType dirType = (isAlsoEdits) ?
                          NameNodeDirType.IMAGE_AND_EDITS :
                          NameNodeDirType.IMAGE;
      this.addStorageDir(new StorageDirectory(dirName, dirType));
    }
    
    // Add edits dirs if they are different from name dirs
    for (File dirName : fsEditsDirs) {
      this.addStorageDir(new StorageDirectory(dirName, 
                    NameNodeDirType.EDITS));
    }
  }

  void setCheckpointDirectories(Collection<File> dirs,
                                Collection<File> editsDirs) {
    checkpointDirs = dirs;
    checkpointEditsDirs = editsDirs;
  }
  
  static File getImageFile(StorageDirectory sd, NameNodeFile type) {
    return new File(sd.getCurrentDir(), type.getName());
  }
  
  List<StorageDirectory> getRemovedStorageDirs() {
	  return this.removedStorageDirs;
  }
  
  File getEditFile(StorageDirectory sd) {
    return getImageFile(sd, NameNodeFile.EDITS);
  }
  
  File getEditNewFile(StorageDirectory sd) {
    return getImageFile(sd, NameNodeFile.EDITS_NEW);
  }

  File[] getFileNames(NameNodeFile type, NameNodeDirType dirType) {
    ArrayList<File> list = new ArrayList<File>();
    Iterator<StorageDirectory> it = (dirType == null) ? dirIterator() :
                                    dirIterator(dirType);
    for ( ;it.hasNext(); ) {
      list.add(getImageFile(it.next(), type));
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
    return imageDigest;
  }

  void setImageDigest(MD5Hash imageDigest) {
    newImageDigest = false;
    this.imageDigest.set(imageDigest);
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
  boolean recoverTransitionRead(Collection<File> dataDirs,
                             Collection<File> editsDirs,
                                StartupOption startOpt
                                ) throws IOException {
    assert startOpt != StartupOption.FORMAT : 
      "NameNode formatting should be performed before reading the image";
    
    // none of the data dirs exist
    if (dataDirs.size() == 0 || editsDirs.size() == 0)  
      throw new IOException(
        "All specified directories are not accessible or do not exist.");
    
    if(startOpt == StartupOption.IMPORT 
        && (checkpointDirs == null || checkpointDirs.isEmpty()))
      throw new IOException("Cannot import image from a checkpoint. "
                          + "\"fs.checkpoint.dir\" is not set." );

    if(startOpt == StartupOption.IMPORT 
        && (checkpointEditsDirs == null || checkpointEditsDirs.isEmpty()))
      throw new IOException("Cannot import image from a checkpoint. "
                          + "\"fs.checkpoint.edits.dir\" is not set." );
    
    setStorageDirectories(dataDirs, editsDirs);
    // 1. For each data directory calculate its state and 
    // check whether all is consistent before transitioning.
    Map<StorageDirectory, StorageState> dataDirStates = 
             new HashMap<StorageDirectory, StorageState>();
    boolean isFormatted = false;
    for (Iterator<StorageDirectory> it = 
                      dirIterator(); it.hasNext();) {
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
          sd.read(); // read and verify consistency with other directories
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
    
    if (!isFormatted && startOpt != StartupOption.ROLLBACK 
                     && startOpt != StartupOption.IMPORT)
      throw new IOException("NameNode is not formatted.");
    if (layoutVersion < LAST_PRE_UPGRADE_LAYOUT_VERSION) {
      checkVersionUpgradable(layoutVersion);
    }
    if (startOpt != StartupOption.UPGRADE
          && layoutVersion < LAST_PRE_UPGRADE_LAYOUT_VERSION
          && layoutVersion != FSConstants.LAYOUT_VERSION)
        throw new IOException(
                          "\nFile system image contains an old layout version " + layoutVersion
                          + ".\nAn upgrade to version " + FSConstants.LAYOUT_VERSION
                          + " is required.\nPlease restart NameNode with -upgrade option.");
    // check whether distributed upgrade is reguired and/or should be continued
    verifyDistributedUpgradeProgress(startOpt);

    // 2. Format unformatted dirs.
    this.checkpointTime = 0L;
    for (Iterator<StorageDirectory> it = 
                     dirIterator(); it.hasNext();) {
      StorageDirectory sd = it.next();
      StorageState curState = dataDirStates.get(sd);
      switch(curState) {
      case NON_EXISTENT:
        assert false : StorageState.NON_EXISTENT + " state cannot be here";
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
      return true;
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
    for (Iterator <StorageDirectory> it = dirIterator(); it.hasNext();) {
      StorageDirectory sd = it.next();
      File preDir = sd.getPreviousDir();
      if (preDir.exists()) {
        return true;
      }
    }
    return false;
  }

  private void doUpgrade() throws IOException {
    namesystem.setUpgradeStartTime(FSNamesystem.now());
    if(getDistributedUpgradeState()) {
      // only distributed upgrade need to continue
      // don't do version upgrade
      this.loadFSImage();
      initializeDistributedUpgrade();
      return;
    }
    // Upgrade is allowed only if there are 
    // no previous fs states in any of the directories
    for (Iterator<StorageDirectory> it = 
                           dirIterator(); it.hasNext();) {
      StorageDirectory sd = it.next();
      if (sd.getPreviousDir().exists())
        throw new InconsistentFSStateException(sd.getRoot(),
                                               "previous fs state should not exist during upgrade. "
                                               + "Finalize or rollback first.");
    }

    // load the latest image
    this.loadFSImage();

    // Do upgrade for each directory
    long oldCTime = this.getCTime();
    this.cTime = FSNamesystem.now();  // generate new cTime for the state
    int oldLV = this.getLayoutVersion();
    this.layoutVersion = FSConstants.LAYOUT_VERSION;
    this.checkpointTime = FSNamesystem.now();
    List<Thread> savers = new ArrayList<Thread>();
    saveNamespaceContext.set(namesystem, editLog.getLastWrittenTxId());

    for (Iterator<StorageDirectory> it = 
                           dirIterator(); it.hasNext();) {
      StorageDirectory sd = it.next();
      LOG.info("Upgrading image directory " + sd.getRoot()
               + ".\n   old LV = " + oldLV
               + "; old CTime = " + oldCTime
               + ".\n   new LV = " + this.getLayoutVersion()
               + "; new CTime = " + this.getCTime());
      File curDir = sd.getCurrentDir();
      File prevDir = sd.getPreviousDir();
      File tmpDir = sd.getPreviousTmp();
      try {
        assert curDir.exists() : "Current directory must exist.";
        assert !prevDir.exists() : "prvious directory must not exist.";
        assert !tmpDir.exists() : "prvious.tmp directory must not exist.";
        // rename current to tmp
        rename(curDir, tmpDir);
        // save new image
        if (!curDir.mkdir())
          throw new IOException("Cannot create directory " + curDir);
      } catch (Exception e) {
        LOG.error("Error upgrading " + sd.getRoot(), e);
        saveNamespaceContext.reportErrorOnStorageDirectory(sd);
        continue;
      }
    }    
    for (Iterator<StorageDirectory> it = dirIterator(NameNodeDirType.IMAGE); 
                it.hasNext();) {
    StorageDirectory sd = it.next();
      // save image to image directory
      FSImageSaver saver = new FSImageSaver(saveNamespaceContext, sd, false, NameNodeFile.IMAGE);
      Thread saverThread = new Thread(saver, saver.toString());
      savers.add(saverThread);
      saverThread.start();
    }
 
    // wait until all images are saved
    for (Thread saver : savers) {
      try {
        saver.join();
      } catch (InterruptedException iex) {
        LOG.info("Caught exception while waiting for thread " + 
            saver.getName() + " to finish. Retrying join");
        throw (IOException)new InterruptedIOException().initCause(iex);
      }
    }  
    for (Iterator<StorageDirectory> it = dirIterator(NameNodeDirType.EDITS);
                it.hasNext();) {
      // create empty edit log file
      StorageDirectory sd = it.next();
      editLog.createEditLogFile(getImageFile(sd, NameNodeFile.EDITS));
    }

    for (Iterator<StorageDirectory> it = dirIterator(); it.hasNext();) {
      StorageDirectory sd = it.next();
      if (saveNamespaceContext.getErrorSDs().contains(sd)) continue;
      try {
        // write version and time files
        sd.write();
        // rename tmp to previous
        rename(sd.getPreviousTmp(), sd.getPreviousDir());
      } catch (IOException ioe) {
        LOG.error("Error upgrading " + sd.getRoot(), ioe);
        saveNamespaceContext.reportErrorOnStorageDirectory(sd);
        continue;
      }
      isUpgradeFinalized = false;
      LOG.info("Upgrade of " + sd.getRoot() + " is complete.");
    }
    saveNamespaceContext.clear();
    processIOError(saveNamespaceContext.getErrorSDs());
    initializeDistributedUpgrade();
    editLog.open();
    saveNamespaceContext.clear();
  }

  private void doRollback() throws IOException {
    // Rollback is allowed only if there is 
    // a previous fs states in at least one of the storage directories.
    // Directories that don't have previous state do not rollback
    boolean canRollback = false;
    FSImage prevState = new FSImage(getFSNamesystem());
    prevState.layoutVersion = FSConstants.LAYOUT_VERSION;
    for (Iterator<StorageDirectory> it = 
                       dirIterator(); it.hasNext();) {
      StorageDirectory sd = it.next();
      File prevDir = sd.getPreviousDir();
      if (!prevDir.exists()) {  // use current directory then
        LOG.info("Storage directory " + sd.getRoot()
                 + " does not contain previous fs state.");
        sd.read(); // read and verify consistency with other directories
        continue;
      }
      StorageDirectory sdPrev = prevState.new StorageDirectory(sd.getRoot());
      sdPrev.read(sdPrev.getPreviousVersionFile());  // read and verify consistency of the prev dir
      canRollback = true;
    }
    if (!canRollback)
      throw new IOException("Cannot rollback. " 
                            + "None of the storage directories contain previous fs state.");

    // Now that we know all directories are going to be consistent
    // Do rollback for each directory containing previous state
    for (Iterator<StorageDirectory> it = 
                          dirIterator(); it.hasNext();) {
      StorageDirectory sd = it.next();
      File prevDir = sd.getPreviousDir();
      if (!prevDir.exists())
        continue;

      LOG.info("Rolling back storage directory " + sd.getRoot()
               + ".\n   new LV = " + prevState.getLayoutVersion()
               + "; new CTime = " + prevState.getCTime());
      File tmpDir = sd.getRemovedTmp();
      assert !tmpDir.exists() : "removed.tmp directory must not exist.";
      // rename current to tmp
      File curDir = sd.getCurrentDir();
      assert curDir.exists() : "Current directory must exist.";
      rename(curDir, tmpDir);
      // rename previous to current
      rename(prevDir, curDir);

      // delete tmp dir
      deleteDir(tmpDir);
      LOG.info("Rollback of " + sd.getRoot()+ " is complete.");
    }
    isUpgradeFinalized = true;
    // check whether name-node can start in regular mode
    verifyDistributedUpgradeProgress(StartupOption.REGULAR);
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
             + (getLayoutVersion()==0 ? "" :
                   "\n   cur LV = " + this.getLayoutVersion()
                   + "; cur CTime = " + this.getCTime()));
    assert sd.getCurrentDir().exists() : "Current directory must exist.";
    final File tmpDir = sd.getFinalizedTmp();
    // rename previous to tmp and remove
    rename(prevDir, tmpDir);
    deleteDir(tmpDir);
    LOG.info("Finalize upgrade for " + sd.getRoot()+ " is complete.");
  }

  /**
   * Load image from a checkpoint directory and save it into the current one.
   * @throws IOException
   */
  void doImportCheckpoint() throws IOException {
    FSNamesystem fsNamesys = getFSNamesystem();
    FSImage ckptImage = new FSImage(fsNamesys);
    // replace real image with the checkpoint image
    FSImage realImage = fsNamesys.getFSImage();
    assert realImage == this;
    fsNamesys.dir.fsImage = ckptImage;
    // load from the checkpoint dirs
    try {
      ckptImage.recoverTransitionRead(checkpointDirs, checkpointEditsDirs,
                                              StartupOption.REGULAR);
    } finally {
      ckptImage.close();
    }
    // return back the real image
    realImage.setStorageInfo(ckptImage);
    fsNamesys.dir.fsImage = realImage;
    // and save it
    saveNamespace(false);
  }

  void finalizeUpgrade() throws IOException {
    for (Iterator<StorageDirectory> it = 
                          dirIterator(); it.hasNext();) {
      doFinalize(it.next());
    }
    isUpgradeFinalized = true;
    namesystem.setUpgradeStartTime(0);
  }

  boolean isUpgradeFinalized() {
    return isUpgradeFinalized;
  }

  protected void getFields(Properties props, 
                           StorageDirectory sd 
                           ) throws IOException {
    super.getFields(props, sd);
    if (layoutVersion == 0)
      throw new IOException("NameNode directory " 
                            + sd.getRoot() + " is not formatted.");
    String sDUS, sDUV;
    sDUS = props.getProperty("distributedUpgradeState"); 
    sDUV = props.getProperty("distributedUpgradeVersion");
    setDistributedUpgradeState(
        sDUS == null? false : Boolean.parseBoolean(sDUS),
        sDUV == null? getLayoutVersion() : Integer.parseInt(sDUV));
    String sMd5 = props.getProperty(MESSAGE_DIGEST_PROPERTY);
    if (layoutVersion <= -26) {
      if (sMd5 == null) {
        throw new InconsistentFSStateException(sd.getRoot(),
            "file " + STORAGE_FILE_VERSION + " does not have MD5 image digest.");
      }
      this.setImageDigest(new MD5Hash(sMd5));
    } else if (sMd5 != null) {
      throw new InconsistentFSStateException(sd.getRoot(),
          "file " + STORAGE_FILE_VERSION + 
          " has image MD5 digest when version is " + layoutVersion);
    }
    this.checkpointTime = readCheckpointTime(sd);
  }

  long readCheckpointTime(StorageDirectory sd) throws IOException {
    File timeFile = getImageFile(sd, NameNodeFile.TIME);
    long timeStamp = 0L;
    if (timeFile.exists() && timeFile.canRead()) {
      DataInputStream in = new DataInputStream(new FileInputStream(timeFile));
      try {
        timeStamp = in.readLong();
      } finally {
        in.close();
      }
    }
    return timeStamp;
  }

  /**
   * Write last checkpoint time and version file into the storage directory.
   * 
   * The version file should always be written last.
   * Missing or corrupted version file indicates that 
   * the checkpoint is not valid.
   * 
   * @param sd storage directory
   * @throws IOException
   */
  protected void setFields(Properties props, 
                           StorageDirectory sd 
                           ) throws IOException {
    super.setFields(props, sd);
    boolean uState = getDistributedUpgradeState();
    int uVersion = getDistributedUpgradeVersion();
    if(uState && uVersion != getLayoutVersion()) {
      props.setProperty("distributedUpgradeState", Boolean.toString(uState));
      props.setProperty("distributedUpgradeVersion", Integer.toString(uVersion)); 
    }
    if (this.newImageDigest) {
      this.setImageDigest(MD5Hash.digest(
          new FileInputStream(getImageFile(sd, NameNodeFile.IMAGE))));
    }
    props.setProperty(MESSAGE_DIGEST_PROPERTY, 
        this.getImageDigest().toString());
    writeCheckpointTime(sd);
  }

  /**
   * Write last checkpoint time into a separate file.
   * 
   * @param sd
   * @throws IOException
   */
  void writeCheckpointTime(StorageDirectory sd) throws IOException {
    if (checkpointTime < 0L)
      return; // do not write negative time
    File timeFile = getImageFile(sd, NameNodeFile.TIME);
    if (timeFile.exists()) { timeFile.delete(); }
    DataOutputStream out = new DataOutputStream(
                                                new FileOutputStream(timeFile));
    try {
      out.writeLong(checkpointTime);
    } finally {
      out.close();
    }
  }

  /**
   * Record new checkpoint time in order to
   * distinguish healthy directories from the removed ones.
   * If there is an error writing new checkpoint time, the corresponding
   * storage directory is removed from the list.
   */
  void incrementCheckpointTime() {
    this.checkpointTime++;
    
    // Write new checkpoint time in all storage directories
    for(Iterator<StorageDirectory> it =
                          dirIterator(); it.hasNext();) {
      StorageDirectory sd = it.next();
      try {
        writeCheckpointTime(sd);
      } catch(IOException e) {
        // Close any edits stream associated with this dir and remove directory
        LOG.warn("incrementCheckpointTime failed on " + sd.getRoot().getPath() + ";type="+sd.getStorageDirType());
        if (sd.getStorageDirType().isOfType(NameNodeDirType.EDITS))
          editLog.processIOError(sd);

        //add storage to the removed list
        removedStorageDirs.add(sd);
        it.remove();
      }
    }
  }
  
  /**
   * Remove storage directory given directory
   */
  void processIOError(File dirName) {
    for (Iterator<StorageDirectory> it = 
      dirIterator(); it.hasNext();) {
      StorageDirectory sd = it.next();
      if (sd.getRoot().getPath().equals(dirName.getPath())) {
        //add storage to the removed list
        LOG.warn("FSImage:processIOError: removing storage: " + dirName.getPath());
        try {
          sd.unlock(); //try to unlock before removing (in case it is restored)
        } catch (Exception e) {
          LOG.info("Unable to unlock bad storage directory : " +  dirName.getPath());
        }
        removedStorageDirs.add(sd);
        it.remove();
      }
    }
  }

  /**
   * @param sds - array of SDs to process
   */
  void processIOError(List<StorageDirectory> sds) throws IOException {
    synchronized (sds) {
      for (StorageDirectory sd : sds) {
        for (Iterator<StorageDirectory> it = dirIterator(); it.hasNext();) {
          StorageDirectory sd1 = it.next();
          if (sd.equals(sd1)) {
            // add storage to the removed list
            LOG.warn("FSImage:processIOError: removing storage: "
                + sd.getRoot().getPath());
            try {
              sd1.unlock(); // unlock before removing (in case it will be
                            // restored)
            } catch (Exception e) {
              LOG.info("Unable to unlock bad storage directory : " +  sd.getRoot().getPath());
            }
            removedStorageDirs.add(sd1);
            it.remove();
            break;
          }
        }
      }
    }
    if (this.getNumStorageDirs() == 0)
      throw new IOException("No more storage directory left");
  }

  public FSEditLog getEditLog() {
    return editLog;
  }

  public boolean isConversionNeeded(StorageDirectory sd) throws IOException {
    File oldImageDir = new File(sd.getRoot(), "image");
    if (!oldImageDir.exists()) {
      if(sd.getVersionFile().exists())
        throw new InconsistentFSStateException(sd.getRoot(),
            oldImageDir + " does not exist.");
      return false;
    }
    // check the layout version inside the image file
    File oldF = new File(oldImageDir, "fsimage");
    RandomAccessFile oldFile = new RandomAccessFile(oldF, "rws");
    try {
      oldFile.seek(0);
      int odlVersion = oldFile.readInt();
      if (odlVersion < LAST_PRE_UPGRADE_LAYOUT_VERSION)
        return false;
    } finally {
      oldFile.close();
    }
    return true;
  }
  
  //
  // Atomic move sequence, to recover from interrupted checkpoint
  //
  boolean recoverInterruptedCheckpoint(StorageDirectory nameSD,
                                       StorageDirectory editsSD) 
                                       throws IOException {
    boolean needToSave = false;
    File curFile = getImageFile(nameSD, NameNodeFile.IMAGE);
    File ckptFile = getImageFile(nameSD, NameNodeFile.IMAGE_NEW);

    //
    // If we were in the midst of a checkpoint
    //
    if (ckptFile.exists()) {
      needToSave = true;
      if (getImageFile(editsSD, NameNodeFile.EDITS_NEW).exists()) {
        //
        // checkpointing migth have uploaded a new
        // merged image, but we discard it here because we are
        // not sure whether the entire merged image was uploaded
        // before the namenode crashed.
        //
        if (!ckptFile.delete()) {
          throw new IOException("Unable to delete " + ckptFile);
        }
      } else {
        //
        // checkpointing was in progress when the namenode
        // shutdown. The fsimage.ckpt was created and the edits.new
        // file was moved to edits. We complete that checkpoint by
        // moving fsimage.new to fsimage. There is no need to 
        // update the fstime file here. renameTo fails on Windows
        // if the destination file already exists.
        //
        if (!ckptFile.renameTo(curFile)) {
          if (!curFile.delete())
            LOG.warn("Unable to delete dir " + curFile + " before rename");
          if (!ckptFile.renameTo(curFile)) {
            throw new IOException("Unable to rename " + ckptFile +
                                  " to " + curFile);
          }
        }
      }
    }
    return needToSave;
  }

  /**
   * Choose latest image from one of the directories,
   * load it and merge with the edits from that directory.
   * 
   * @return whether the image should be saved
   * @throws IOException
   */
  boolean loadFSImage() throws IOException {
    // Now check all curFiles and see which is the newest
    long latestNameCheckpointTime = Long.MIN_VALUE;
    long latestEditsCheckpointTime = Long.MIN_VALUE;
    StorageDirectory latestNameSD = null;
    StorageDirectory latestEditsSD = null;
    boolean needToSave = false;
    isUpgradeFinalized = true;
    Collection<String> imageDirs = new ArrayList<String>();
    Collection<String> editsDirs = new ArrayList<String>();
    for (Iterator<StorageDirectory> it = dirIterator(); it.hasNext();) {
      StorageDirectory sd = it.next();
      if (!sd.getVersionFile().exists()) {
        needToSave |= true;
        continue; // some of them might have just been formatted
      }
      boolean imageExists = false, editsExists = false;
      if (sd.getStorageDirType().isOfType(NameNodeDirType.IMAGE)) {
        imageExists = getImageFile(sd, NameNodeFile.IMAGE).exists();
        imageDirs.add(sd.getRoot().getCanonicalPath());
      }
      if (sd.getStorageDirType().isOfType(NameNodeDirType.EDITS)) {
        editsExists = getImageFile(sd, NameNodeFile.EDITS).exists();
        editsDirs.add(sd.getRoot().getCanonicalPath());
        needToSave |= getImageFile(sd, NameNodeFile.EDITS_NEW).exists();
      }
      
      checkpointTime = readCheckpointTime(sd);
      if ((checkpointTime != latestNameCheckpointTime &&
            latestNameCheckpointTime != Long.MIN_VALUE) || 
           (checkpointTime != latestEditsCheckpointTime &&
            latestEditsCheckpointTime != Long.MIN_VALUE)) {
        // Force saving of new image if checkpoint time
        // is not same in all of the storage directories.
        needToSave |= true;
      }
      if (sd.getStorageDirType().isOfType(NameNodeDirType.IMAGE) && 
         (latestNameCheckpointTime < checkpointTime) && imageExists) {
        latestNameCheckpointTime = checkpointTime;
        latestNameSD = sd;
      }
      if (sd.getStorageDirType().isOfType(NameNodeDirType.EDITS) && 
           (latestEditsCheckpointTime < checkpointTime) && editsExists) {
        latestEditsCheckpointTime = checkpointTime;
        latestEditsSD = sd;
      }
      if (checkpointTime <= 0L)
        needToSave |= true;
      // set finalized flag
      isUpgradeFinalized = isUpgradeFinalized && !sd.getPreviousDir().exists();
    }

    // We should have at least one image and one edits dirs
    if (latestNameSD == null)
      throw new IOException("Image file is not found in " + imageDirs);
    if (latestEditsSD == null)
      throw new IOException("Edits file is not found in " + editsDirs);

    // Make sure we are loading image and edits from same checkpoint
    if (latestNameCheckpointTime > latestEditsCheckpointTime
        && latestNameSD != latestEditsSD
        && latestNameSD.getStorageDirType() == NameNodeDirType.IMAGE
        && latestEditsSD.getStorageDirType() == NameNodeDirType.EDITS) {
      // This is a rare failure when NN has image-only and edits-only
      // storage directories, and fails right after saving images,
      // in some of the storage directories, but before purging edits.
      // See -NOTE- in saveNamespace().
      LOG.error("This is a rare failure scenario!!!");
      LOG.error("Image checkpoint time " + latestNameCheckpointTime
          + " > edits checkpoint time " + latestEditsCheckpointTime);
      LOG.error("Name-node will treat the image as the latest state of "
          + "the namespace. Old edits will be discarded.");
    } else if (latestNameCheckpointTime != latestEditsCheckpointTime)
      throw new IOException("Inconsistent storage detected, "
          + "image and edits checkpoint times do not match. "
          + "image checkpoint time = " + latestNameCheckpointTime
          + "edits checkpoint time = " + latestEditsCheckpointTime);
    
    // Recover from previous interrrupted checkpoint if any
    needToSave |= recoverInterruptedCheckpoint(latestNameSD, latestEditsSD);

    //
    // Load in bits
    //
    latestNameSD.read();
    //triggers re-save if image version is older
    needToSave |= loadFSImage(getImageFile(latestNameSD, NameNodeFile.IMAGE));

    if (latestNameCheckpointTime > latestEditsCheckpointTime) {
      // the image is already current, discard edits
      needToSave |= true;
    }
    else {
      // latestNameCheckpointTime == latestEditsCheckpointTime
      if(loadFSEdits(latestEditsSD) > 0) {
        // trigger to save the image only if the edit log is bigger than
        // fs.checkpoint.size or last checkpoint was done longer than 
        // fs.checkpoint.period   
        boolean checkpointTimeTrigger = 
            latestNameCheckpointTime
            + namesystem.getConf().getLong("fs.checkpoint.period", 3600) * 1000 
            < FSNamesystem.now();
        boolean checkpointSizeTrigger = 
            getEditFile(latestEditsSD).length()
            > namesystem.getConf().getLong("fs.checkpoint.size", 4194304);
        needToSave |= (checkpointTimeTrigger || checkpointSizeTrigger);
      }        
    }
    return needToSave;
  }

  boolean loadFSImage(File curFile) throws IOException {
    assert curFile != null : "curFile is null";

    FSImageFormat.Loader loader = new FSImageFormat.Loader(namesystem.getConf(), namesystem, this);
    loader.load(curFile, null);
    editLog.setStartTransactionId(loader.getLoadedImageTxId() + 1);
    MD5Hash readImageMd5 = loader.getLoadedImageMd5();
    
    if (this.newImageDigest) {
      this.setImageDigest(readImageMd5); // set this fsimage's checksum
    } else if (!this.getImageDigest().equals(readImageMd5)) {
      throw new IOException("Image file " + curFile + " is corrupt!");
    }
    
    return loader.getNeedToSave();
  }
  /**
   * Load in the filesystem imagefrom file. It's a big list of
   * filenames and blocks.  Return whether we should
   * "re-save" and consolidate the edit-logs
   */
  boolean loadFSImage(File curFile, DataInputStream dis) throws IOException {
    assert curFile != null : "curFile is null";

    FSImageFormat.Loader loader = new FSImageFormat.Loader(namesystem.getConf(), namesystem, this);
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
   * Load and merge edits from two edits files
   * 
   * @param sd storage directory
   * @return number of edits loaded
   * @throws IOException
   */
  int loadFSEdits(StorageDirectory sd) throws IOException {
    int numEdits = 0;
    EditLogFileInputStream edits = 
      new EditLogFileInputStream(getImageFile(sd, NameNodeFile.EDITS));
    
    FSEditLogLoader loader = new FSEditLogLoader(namesystem);
    numEdits += loader.loadFSEdits(edits, editLog.getCurrentTxId());
    edits.close();
    File editsNew = getImageFile(sd, NameNodeFile.EDITS_NEW);
    if (editsNew.exists() && editsNew.length() > 0) {
      edits = new EditLogFileInputStream(editsNew);
      numEdits += loader.loadFSEdits(edits, loader.getCurrentTxId());
      edits.close();
    }
    editLog.setStartTransactionId(loader.getCurrentTxId());
    // update the counts.
    getFSNamesystem().dir.updateCountForINodeWithQuota();    
    return numEdits;
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
      this.imageFile = getImageFile(sd, type);
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
      attemptRestoreRemovedStorage();
  
      saveNamespaceContext.set(namesystem, editLog.getLastWrittenTxId());
      
      InjectionHandler
        .processEvent(InjectionEvent.FSIMAGE_STARTING_SAVE_NAMESPACE);
      
      editLog.close(); // close all open streams before truncating
      if (renewCheckpointTime)
        this.checkpointTime = FSNamesystem.now();
      // mv current -> lastcheckpoint.tmp
      for (Iterator<StorageDirectory> it = dirIterator(); it.hasNext();) {
        StorageDirectory sd = it.next();
        try {
          moveCurrent(sd);
        } catch (IOException ex) {
          LOG.error("Unable to move current for " + sd.getRoot(), ex);
          processIOError(sd.getRoot());
        }
      }
      
      // for testing only - we will wait until interruption comes
      InjectionHandler
          .processEvent(InjectionEvent.FSIMAGE_CREATING_SAVER_THREADS);
      
      // Save image into current using parallel threads for saving
      List<Thread> savers = new ArrayList<Thread>();
      for (Iterator<StorageDirectory> it = dirIterator(NameNodeDirType.IMAGE); 
                                                                it.hasNext();) {
        StorageDirectory sd = it.next();
        FSImageSaver saver = new FSImageSaver(saveNamespaceContext, sd, forUncompressed);
        Thread saverThread = new Thread(saver, saver.toString());
        savers.add(saverThread);
        saverThread.start();
      }

      waitForThreads(savers);       
      savers.clear();
      
      if (saveNamespaceContext.isCancelled()) {
        processIOError(saveNamespaceContext.getErrorSDs());
        deleteCancelledCheckpoint();
        if (!editLog.isOpen()) editLog.open();  
        saveNamespaceContext.checkCancelled();
      } 
      
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
      for (Iterator<StorageDirectory> it = dirIterator(NameNodeDirType.EDITS);
                                                                it.hasNext();) {
        StorageDirectory sd = it.next();
        try {
          saveCurrent(saveNamespaceContext, sd, forUncompressed);
        } catch (IOException ex) {
          LOG.error("Unable to save edits for " + sd.getRoot(), ex);
          processIOError(sd.getRoot());
        }
      }
      
      // mv lastcheckpoint.tmp -> previous.checkpoint
      for (Iterator<StorageDirectory> it = dirIterator(); it.hasNext();) {
        StorageDirectory sd = it.next();
        try {
          moveLastCheckpoint(sd);
        } catch (IOException ex) {
          LOG.error("Unable to move last checkpoint for " + sd.getRoot(), ex);
          processIOError(sd.getRoot());
        }
      }
      if (!editLog.isOpen()) editLog.open();
      processIOError(saveNamespaceContext.getErrorSDs());
      ckptState = CheckpointStates.UPLOAD_DONE;
    } finally {
      saveNamespaceContext.clear();
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
      saver.save(getImageFile(sd, NameNodeFile.IMAGE), compression);
      this.setImageDigest(saver.getSavedDigest());
    }
    if (dirType.isOfType(NameNodeDirType.EDITS))
      editLog.createEditLogFile(getImageFile(sd, NameNodeFile.EDITS));
    // write version and time files
    sd.write();
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
      rename(curDir, tmpCkptDir);
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
      deleteDir(prevCkptDir);
    // rename lastcheckpoint.tmp -> previous.checkpoint
    if (tmpCkptDir.exists())
      rename(tmpCkptDir, prevCkptDir);
  }

  /**
   * Generate new namespaceID.
   * 
   * namespaceID is a persistent attribute of the namespace.
   * It is generated when the namenode is formatted and remains the same
   * during the life cycle of the namenode.
   * When a datanodes register they receive it as the registrationID,
   * which is checked every time the datanode is communicating with the 
   * namenode. Datanodes that do not 'know' the namespaceID are rejected.
   * 
   * @return new namespaceID
   */
  private int newNamespaceID() {
    Random r = new Random();
    r.setSeed(FSNamesystem.now());
    int newID = 0;
    while(newID == 0)
      newID = r.nextInt(0x7FFFFFFF);  // use 31 bits only
    return newID;
  }

  /** Create new dfs name directory.  Caution: this destroys all files
   * in this filesystem. */
  void format(StorageDirectory sd) throws IOException {
    saveNamespaceContext.set(namesystem, -1);
    sd.clearDirectory(); // create currrent dir
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
    this.layoutVersion = FSConstants.LAYOUT_VERSION;
    this.namespaceID = newNamespaceID();
    this.cTime = 0L;
    this.checkpointTime = FSNamesystem.now();
    for (Iterator<StorageDirectory> it = 
                           dirIterator(); it.hasNext();) {
      StorageDirectory sd = it.next();
      format(sd);
    }
  }

  /**
   * Moves fsimage.ckpt to fsImage and edits.new to edits
   * Reopens the new edits file.
   * 
   * @param newImageSignature the signature of the new image
   */
  void rollFSImage(CheckpointSignature newImageSignature) throws IOException {
    MD5Hash newImageDigest = newImageSignature.getImageDigest();
    if (!newImageDigest.equals(checkpointImageDigest)) {
      throw new IOException(
          "Checkpoint image is corrupt: expecting an MD5 checksum of" +
          newImageDigest + " but is " + checkpointImageDigest);
    }
    rollFSImage(newImageSignature.getImageDigest());
  }
  
  private void rollFSImage(MD5Hash newImageDigest)  throws IOException {
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
                       dirIterator(NameNodeDirType.IMAGE); it.hasNext();) {
      StorageDirectory sd = it.next();
      File ckpt = getImageFile(sd, NameNodeFile.IMAGE_NEW);
      if (!ckpt.exists()) {
        throw new IOException("Checkpoint file " + ckpt +
                              " does not exist");
      }
    }
    editLog.purgeEditLog(); // renamed edits.new to edits
    LOG.debug("rollFSImage after purgeEditLog: storageList=" + listStorageDirectories());
    //
    // Renames new image
    //
    for (Iterator<StorageDirectory> it = 
                       dirIterator(NameNodeDirType.IMAGE); it.hasNext();) {
      StorageDirectory sd = it.next();
      File ckpt = getImageFile(sd, NameNodeFile.IMAGE_NEW);
      File curFile = getImageFile(sd, NameNodeFile.IMAGE);
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
          removedStorageDirs.add(sd);
          it.remove();
        }
      }
    }

    //
    // Updates the fstime file on all directories (fsimage and edits)
    // and write version file
    //
    this.layoutVersion = FSConstants.LAYOUT_VERSION;
    this.checkpointTime = FSNamesystem.now();
    this.setImageDigest(newImageDigest);
    for (Iterator<StorageDirectory> it = 
                           dirIterator(); it.hasNext();) {
      StorageDirectory sd = it.next();
      // delete old edits if sd is the image only the directory
      if (!sd.getStorageDirType().isOfType(NameNodeDirType.EDITS)) {
        File editsFile = getImageFile(sd, NameNodeFile.EDITS);
        editsFile.delete();
      }
      // delete old fsimage if sd is the edits only the directory
      if (!sd.getStorageDirType().isOfType(NameNodeDirType.IMAGE)) {
        File imageFile = getImageFile(sd, NameNodeFile.IMAGE);
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
        removedStorageDirs.add(sd);
        it.remove();
      }
    }
    ckptState = FSImage.CheckpointStates.START;
  }
  
  /**
   * Deletes the checkpoint file in every storage directory,
   * since the checkpoint was cancelled. Moves lastcheckpoint.tmp -> current
   */
  private void deleteCancelledCheckpoint() throws IOException {
    ArrayList<StorageDirectory> errorDirs = new ArrayList<StorageDirectory>();
    for (Iterator<StorageDirectory> it = dirIterator(); it.hasNext();) {
      StorageDirectory sd = it.next();
      deleteCancelledChecpointDir(sd, errorDirs);
    }
    processIOError(errorDirs);
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
        deleteDir(curDir);
      // rename lastcheckpoint.tmp -> current
      rename(tmpCkptDir, curDir);
    } catch (IOException e) {
      LOG.warn("Unable to revet checkpoint for : " + sd.getCurrentDir());
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
    sig.validateStorageInfo(this);
    ckptState = FSImage.CheckpointStates.UPLOAD_START;
  }

  /**
   * This is called when a checkpoint upload finishes successfully.
   */
  synchronized void checkpointUploadDone(MD5Hash checkpointImageMd5) {
    checkpointImageDigest = checkpointImageMd5;
    ckptState = CheckpointStates.UPLOAD_DONE;
  }

  void close() throws IOException {
    getEditLog().close(true);
    unlockAll();
  }

  /**
   * Return the name of the image file.
   */
  File getFsImageName() {
    StorageDirectory sd = null;
    for (Iterator<StorageDirectory> it = 
                dirIterator(NameNodeDirType.IMAGE); it.hasNext();) {
      sd = it.next();
      File fsImage = getImageFile(sd, NameNodeFile.IMAGE);
      if (sd.getRoot().canRead() && fsImage.exists()) {
        return fsImage;
      }
    }
    return null;
  }
  
  /**
   * See if any of removed storages iw "writable" again, and can be returned 
   * into service
   */
  void attemptRestoreRemovedStorage() {   
    // if directory is "alive" - copy the images there...
    if(!restoreFailedStorage || removedStorageDirs.size() == 0) 
      return; //nothing to restore
    
    LOG.info("FSImage.attemptRestoreRemovedStorage: check removed(failed) " +
    		"storarge. removedStorages size = " + removedStorageDirs.size());
    for(Iterator<StorageDirectory> it = this.removedStorageDirs.iterator(); it.hasNext();) {
      StorageDirectory sd = it.next();
      File root = sd.getRoot();
      LOG.info("currently disabled dir " + root.getAbsolutePath() + 
          "; type="+sd.getStorageDirType() + ";canwrite="+root.canWrite());
      try {
        
        if(root.exists() && root.canWrite()) { 
          // when we try to restore we just need to remove all the data
          // without saving current in-memory state (which could've changed).
          sd.clearDirectory();
          LOG.info("restoring dir " + sd.getRoot().getAbsolutePath());
          this.addStorageDir(sd); // restore
          it.remove();
        }
      } catch(IOException e) {
        LOG.warn("failed to restore " + sd.getRoot().getAbsolutePath(), e);
      }
    }    
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
             dirIterator(); it.hasNext();)
      sd = it.next();
    return getImageFile(sd, NameNodeFile.TIME);
  }

  /**
   * Return the name of the image file that is uploaded by periodic
   * checkpointing.
   */
  File[] getFsImageNameCheckpoint() {
    ArrayList<File> list = new ArrayList<File>();
    for (Iterator<StorageDirectory> it = 
                 dirIterator(NameNodeDirType.IMAGE); it.hasNext();) {
      list.add(getImageFile(it.next(), NameNodeFile.IMAGE_NEW));
    }
    return list.toArray(new File[list.size()]);
  }

  protected void corruptPreUpgradeStorage(File rootDir) throws IOException {
    File oldImageDir = new File(rootDir, "image");
    if (!oldImageDir.exists())
      if (!oldImageDir.mkdir())
        throw new IOException("Cannot create directory " + oldImageDir);
    File oldImage = new File(oldImageDir, "fsimage");
    if (!oldImage.exists())
      // recreate old image file to let pre-upgrade versions fail
      if (!oldImage.createNewFile())
        throw new IOException("Cannot create file " + oldImage);
    RandomAccessFile oldFile = new RandomAccessFile(oldImage, "rws");
    // write new version into old image file
    try {
      writeCorruptedData(oldFile);
    } finally {
      oldFile.close();
    }
  }

  private boolean getDistributedUpgradeState() {
    FSNamesystem ns = getFSNamesystem();
    return ns == null ? false : ns.getDistributedUpgradeState();
  }

  private int getDistributedUpgradeVersion() {
    FSNamesystem ns = getFSNamesystem();
    return ns == null ? 0 : ns.getDistributedUpgradeVersion();
  }

  private void setDistributedUpgradeState(boolean uState, int uVersion) {
    getFSNamesystem().upgradeManager.setUpgradeState(uState, uVersion);
  }

  private void verifyDistributedUpgradeProgress(StartupOption startOpt
                                                ) throws IOException {
    if(startOpt == StartupOption.ROLLBACK || startOpt == StartupOption.IMPORT)
      return;
    UpgradeManager um = getFSNamesystem().upgradeManager;
    assert um != null : "FSNameSystem.upgradeManager is null.";
    if(startOpt != StartupOption.UPGRADE) {
      if(um.getUpgradeState())
        throw new IOException(
                    "\n   Previous distributed upgrade was not completed. "
                  + "\n   Please restart NameNode with -upgrade option.");
      if(um.getDistributedUpgrades() != null)
        throw new IOException("\n   Distributed upgrade for NameNode version " 
          + um.getUpgradeVersion() + " to current LV " + FSConstants.LAYOUT_VERSION
          + " is required.\n   Please restart NameNode with -upgrade option.");
    }
  }

  private void initializeDistributedUpgrade() throws IOException {
    UpgradeManagerNamenode um = getFSNamesystem().upgradeManager;
    if(! um.initializeUpgrade())
      return;
    // write new upgrade state into disk
    writeAll();
    NameNode.LOG.info("\n   Distributed upgrade for NameNode version " 
        + um.getUpgradeVersion() + " to current LV " 
        + FSConstants.LAYOUT_VERSION + " is initialized.");
  }

  static Collection<File> getCheckpointDirs(Configuration conf,
                                            String defaultName) {
    Collection<String> dirNames = conf.getStringCollection("fs.checkpoint.dir");
    if (dirNames.size() == 0 && defaultName != null) {
      dirNames.add(defaultName);
    }
    Collection<File> dirs = new ArrayList<File>(dirNames.size());
    for(String name : dirNames) {
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
  
  public void cancelSaveNamespace(String reason) {
    saveNamespaceContext.cancel(reason);
    InjectionHandler.processEvent(InjectionEvent.FSIMAGE_CANCEL_REQUEST_RECEIVED);
  }
}
