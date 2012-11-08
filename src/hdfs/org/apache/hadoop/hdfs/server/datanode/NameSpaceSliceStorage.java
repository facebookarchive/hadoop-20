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

package org.apache.hadoop.hdfs.server.datanode;

import java.io.File;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.fs.HardLink;
import org.apache.hadoop.hdfs.protocol.FSConstants;
import org.apache.hadoop.hdfs.server.common.InconsistentFSStateException;
import org.apache.hadoop.hdfs.server.common.IncorrectVersionException;
import org.apache.hadoop.hdfs.server.common.Storage;
import org.apache.hadoop.hdfs.server.common.StorageInfo;
import org.apache.hadoop.hdfs.server.common.HdfsConstants.NodeType;
import org.apache.hadoop.hdfs.server.common.HdfsConstants.StartupOption;
import org.apache.hadoop.hdfs.server.common.Storage.StorageDirectory;
import org.apache.hadoop.hdfs.server.datanode.DataStorage.RollbackThread;
import org.apache.hadoop.hdfs.server.datanode.DataStorage.UpgradeThread;
import org.apache.hadoop.hdfs.server.protocol.NamespaceInfo;
import org.apache.hadoop.util.Daemon;

/**
 * Manages storage for the set of BlockPoolSlices which share a particular 
 * namespace id, on this DataNode.
 * 
 * This class supports the following functionality:
 * <ol>
 * <li> Formatting a new namespace storage</li>
 * <li> Recovering a storage state to a consistent state (if possible></li>
 * <li> Taking a snapshot of the namespace during upgrade</li>
 * <li> Rolling back a namespace to a previous snapshot</li>
 * <li> Finalizing block storage by deletion of a snapshot</li>
 * </ul>
 * 
 * @see Storage
 */
public class NameSpaceSliceStorage extends Storage {
  final static String NS_DIR_PREFIX = "NS-";

  private Map<File, Integer> layoutMap = new HashMap<File, Integer>();

  public NameSpaceSliceStorage() {
    super(NodeType.DATA_NODE);
  }
  
  public NameSpaceSliceStorage(int namespaceID, long cTime) {
    super(NodeType.DATA_NODE, namespaceID, cTime);
  }

  public NameSpaceSliceStorage(int namespaceID, long cTime,
      Map<File, Integer> layoutMap) {
    super(NodeType.DATA_NODE, namespaceID, cTime);
    this.layoutMap = layoutMap;
  }

  /**
   * Analyze storage directories. Recover from previous transitions if required.
   * 
   * @param datanode Datanode to which this storage belongs to
   * @param nsInfo namespace information
   * @param dataDirs storage directories of namespace
   * @param startOpt startup option
   * @throws IOException on error
   */
  void recoverTransitionRead(DataNode datanode, NamespaceInfo nsInfo,
      Collection<File> dataDirs, StartupOption startOpt) throws IOException {
    assert FSConstants.LAYOUT_VERSION == nsInfo.getLayoutVersion() 
        : "Block-pool and name-node layout versions must be the same.";

    // 1. For each Namespace data directory analyze the state and
    // check whether all is consistent before transitioning.
    this.storageDirs = new ArrayList<StorageDirectory>(dataDirs.size());
    ArrayList<StorageState> dataDirStates = new ArrayList<StorageState>(
        dataDirs.size());
    for (Iterator<File> it = dataDirs.iterator(); it.hasNext();) {
      File dataDir = it.next();
      StorageDirectory sd = new StorageDirectory(dataDir, null, false);
      StorageState curState;
      try {
        curState = sd.analyzeStorage(startOpt);
        // sd is locked but not opened
        switch (curState) {
        case NORMAL:
          break;
        case NON_EXISTENT:
          // ignore this storage
          LOG.info("Storage directory " + dataDir + " does not exist.");
          it.remove();
          continue;
        case NOT_FORMATTED: // format
          LOG.info("Storage directory " + dataDir + " is not formatted.");
          if (!sd.isEmpty()) {
            LOG.error("Storage directory " + dataDir
              + " is not empty, and will not be formatted! Exiting.");
            throw new IOException(
              "Storage directory " + dataDir + " is not empty!");
          }
          LOG.info("Formatting ...");
          format(sd, nsInfo);
          break;
        default: // recovery part is common
          sd.doRecover(curState);
        }
      } catch (IOException ioe) {
        sd.unlock();
        throw ioe;
      }
      // add to the storage list. This is inherited from parent class, Storage.
      addStorageDir(sd);
      dataDirStates.add(curState);
    }

    if (dataDirs.size() == 0) // none of the data dirs exist
      throw new IOException(
          "All specified directories are not accessible or do not exist.");

    // 2. Do transitions
    // Each storage directory is treated individually.
    // During startup some of them can upgrade or roll back
    // while others could be up-to-date for the regular startup.
    doTransition(datanode, nsInfo, startOpt);

    // 3. Update all storages. Some of them might have just been formatted.
    this.writeAll();
  }

  /**
   * Format a namespace slice storage. 
   * @param dnCurDir DataStorage current directory
   * @param nsInfo the name space info
   * @throws IOException Signals that an I/O exception has occurred.
   */
  void format(File dnCurDir, NamespaceInfo nsInfo) throws IOException {
    File curNsDir = getNsRoot(namespaceID, dnCurDir);
    StorageDirectory nsSdir = new StorageDirectory(curNsDir);
    format(nsSdir, nsInfo);
  }

  /**
   * Format a namespace slice storage. 
   * @param sd the namespace storage
   * @param nsInfo the name space info
   * @throws IOException Signals that an I/O exception has occurred.
   */
  private void format(StorageDirectory nsSdir, NamespaceInfo nsInfo) throws IOException {
    LOG.info("Formatting namespace " + namespaceID + " directory "
        + nsSdir.getCurrentDir());
    nsSdir.clearDirectory(); // create directory
    File rbwDir = new File(nsSdir.getCurrentDir(), STORAGE_DIR_RBW);
    File finalizedDir = new File(nsSdir.getCurrentDir(), STORAGE_DIR_FINALIZED);
    LOG.info("Creating Directories : " + rbwDir + ", " + finalizedDir);
    if (!rbwDir.mkdirs() || !finalizedDir.mkdirs()) {
      throw new IOException("Cannot create directories : " + rbwDir + ", "
          + finalizedDir);
    }
    this.layoutVersion = FSConstants.LAYOUT_VERSION;
    this.cTime = nsInfo.getCTime();
    this.namespaceID = nsInfo.getNamespaceID();
    this.storageType = NodeType.DATA_NODE;
    nsSdir.write();
  }

  /**
   * Set layoutVersion, namespaceID and blockpoolID into namespace storage
   * VERSION file
   */
  @Override
  protected void setFields(Properties props, StorageDirectory sd)
      throws IOException {
    props.setProperty(NAMESPACE_ID, String.valueOf(namespaceID));
    props.setProperty(CHECK_TIME, String.valueOf(cTime));
    props.setProperty(LAYOUT_VERSION, String.valueOf(layoutVersion));
  }

  /** Validate and set namespace ID */
  private void setNameSpaceID(File storage, String nsid)
      throws InconsistentFSStateException {
    if (nsid == null || nsid.equals("")) {
      throw new InconsistentFSStateException(storage, "file "
          + STORAGE_FILE_VERSION + " is invalid.");
    }
    int newNsId = Integer.parseInt(nsid);
    if (namespaceID > 0 && namespaceID != newNsId) {
      throw new InconsistentFSStateException(storage,
          "Unexepcted namespaceID " + nsid + " . Expected " + namespaceID);
    }
    namespaceID = newNsId;
  }
  
  @Override
  protected void getFields(Properties props, StorageDirectory sd)
      throws IOException {
    setNamespaceID(props, sd);
    setcTime(props, sd);
    
    String snsid = props.getProperty(NAMESPACE_ID);
    setNameSpaceID(sd.getRoot(), snsid);

    String property = props.getProperty(LAYOUT_VERSION);
    int lv;
    if (property == null) {
      Integer topLayout = getTopLevelLayout(sd);
      if (topLayout == null) {
        throw new InconsistentFSStateException(sd.getRoot(),
            "Top level layout and NS level layout do not exist");
      }
      lv = topLayout;
    } else {
      lv = Integer.parseInt(property);
    }
    if (lv < FSConstants.LAYOUT_VERSION) { // future version
      throw new InconsistentFSStateException(sd.getRoot(),
          "has future layout version : " + lv);
    }
    layoutVersion = lv;
  }

  private Integer getTopLevelLayout(StorageDirectory sd)
      throws IOException {
    File topDir = sd.getRoot().getParentFile().getParentFile();
    Integer layoutVersion = layoutMap.get(topDir);
    if (layoutVersion != null && topDir.exists() && topDir.isDirectory()) {
      return layoutVersion;
    }
    return null;
  }

  private boolean isTopLevelUpgraded(StorageDirectory sd) {
    File topDir = sd.getRoot().getParentFile().getParentFile();
    return new File(topDir, STORAGE_DIR_PREVIOUS).exists();
  }

  /**
   * Analyze whether a transition of the NS state is required and perform it if
   * necessary. <br>
   * Rollback if (previousLV >= LAYOUT_VERSION &&
   * LAYOUT_VERSION<=FEDERATION_VERSION) || prevCTime <= namenode.cTime.
   * Upgrade if this.LV > LAYOUT_VERSION || this.cTime < namenode.cTime
   * Regular startup if this.LV = LAYOUT_VERSION && this.cTime = namenode.cTime
   * 
   * @param dn
   *          DataNode to which this storage belongs to
   * @param nsInfo
   *          namespace info
   * @param startOpt
   *          startup option
   * @throws IOException
   */
  private void doTransition(DataNode datanode,
      NamespaceInfo nsInfo, StartupOption startOpt) throws IOException {
    if (startOpt == StartupOption.ROLLBACK)
      doRollback(nsInfo); // rollback if applicable
    
    int numOfDirs = getNumStorageDirs();
    List<StorageDirectory> dirsToUpgrade = new ArrayList<StorageDirectory>(numOfDirs);
    List<StorageInfo> dirsInfo = new ArrayList<StorageInfo>(numOfDirs);
    for(int idx = 0; idx < numOfDirs; idx++) {
      StorageDirectory sd = this.getStorageDir(idx);
      sd.read();
      checkVersionUpgradable(this.layoutVersion);
      assert this.layoutVersion >= FSConstants.LAYOUT_VERSION 
         : "Future version is not allowed";
      if (getNamespaceID() != nsInfo.getNamespaceID()) {
        throw new IOException("Incompatible namespaceIDs in "
            + sd.getRoot().getCanonicalPath() + ": namenode namespaceID = "
            + nsInfo.getNamespaceID() + "; datanode namespaceID = "
            + getNamespaceID());
      }
      if (this.layoutVersion == FSConstants.LAYOUT_VERSION
          && this.cTime == nsInfo.getCTime())
        continue; // regular startup
    
      // verify necessity of a distributed upgrade
      UpgradeManagerDatanode um = 
        datanode.getUpgradeManager(nsInfo.namespaceID);
      verifyDistributedUpgradeProgress(um, nsInfo);

      // upgrade if layout version has not changed and NN has a newer checkpoint
      // if layout version gets updated, a global snapshot has already taken
      // so no need to do a per namespace snapshot
      if (this.layoutVersion > nsInfo.layoutVersion ||
          this.cTime < nsInfo.getCTime()) {
        if (isTopLevelUpgraded(sd)) {
          throw new IOException("Top level directory already upgraded for : " +
              sd.getRoot());
        }
        dirsToUpgrade.add(sd);  // upgrade
        dirsInfo.add(new StorageInfo(this));
        continue;
      }
      // layoutVersion == LAYOUT_VERSION && this.cTime > nsInfo.cTime
      // must shutdown
      if (this.layoutVersion == nsInfo.layoutVersion && 
          this.cTime > nsInfo.cTime) {
        throw new IOException("Datanode state: LV = " + this.getLayoutVersion()
          + " CTime = " + this.getCTime()
          + " is newer than the namespace state: LV = "
          + nsInfo.getLayoutVersion() + " CTime = " + nsInfo.getCTime());
      }
    }
    // Now do upgrade if dirsToUpgrade is not empty
    if (!dirsToUpgrade.isEmpty()) {
      doUpgrade(dirsToUpgrade, dirsInfo, nsInfo);
    }   
  }
  
  /**
   * Move current storage into a backup directory,
   * and hardlink all its blocks into the new current directory.
   */
  private void doUpgrade(List<StorageDirectory> sds,
                 List<StorageInfo> sdsInfo,
                 final NamespaceInfo nsInfo
                 ) throws IOException {
    assert sds.size() == sdsInfo.size();
    UpgradeThread[] upgradeThreads = new UpgradeThread[sds.size()];
    // start to upgrade
    for (int i=0; i<upgradeThreads.length; i++) {
      final StorageDirectory sd = sds.get(i);
      final StorageInfo si = sdsInfo.get(i);
      UpgradeThread thread = new UpgradeThread(sd, si, nsInfo);
      thread.start();
      upgradeThreads[i] = thread;
    }
    // wait for upgrade to be done
    for (UpgradeThread thread : upgradeThreads) {
      try {
        thread.join();
      } catch (InterruptedException e) {
        throw (InterruptedIOException)new InterruptedIOException().initCause(e);
      }
    }
    // check for errors
    for (UpgradeThread thread : upgradeThreads) {
      if (thread.error != null)
        throw new IOException(thread.error);
    }

    // write version file
    this.layoutVersion = FSConstants.LAYOUT_VERSION;
    assert this.namespaceID == nsInfo.getNamespaceID() :
      "Data-node and name-node layout versions must be the same.";
    this.cTime = nsInfo.getCTime();
    for (StorageDirectory sd :sds) {
      sd.write();
      File prevDir = sd.getPreviousDir();
      File tmpDir = sd.getPreviousTmp();
      // rename tmp to previous
      rename(tmpDir, prevDir);
      LOG.info("Upgrade of " + sd.getRoot()+ " is complete.");
    }
  }

  private boolean isGlobalUpgraded(StorageDirectory nsSd) {
    return new File(nsSd.getRoot().getParentFile().getParentFile(), STORAGE_DIR_PREVIOUS).exists();
  }
  
  /**
   * Cleanup the detachDir.
   * 
   * If the directory is not empty report an error; Otherwise remove the
   * directory.
   * 
   * @param detachDir detach directory
   * @throws IOException if the directory is not empty or it can not be removed

  private void cleanupDetachDir(File detachDir) throws IOException {
    if (!LayoutVersion.supports(Feature.APPEND_RBW_DIR, layoutVersion)
        && detachDir.exists() && detachDir.isDirectory()) {

      if (detachDir.list().length != 0) {
        throw new IOException("Detached directory " + detachDir
            + " is not empty. Please manually move each file under this "
            + "directory to the finalized directory if the finalized "
            + "directory tree does not have the file.");
      } else if (!detachDir.delete()) {
        throw new IOException("Cannot remove directory " + detachDir);
      }
    }
  }
  */

  /*
   * Roll back to old snapshot at the namespace level
   * If previous directory exists: 
   * <ol>
   * <li>Rename <SD>/current/<nsid>/current to removed.tmp</li>
   * <li>Rename * <SD>/current/<nsid>/previous to current</li>
   * <li>Remove removed.tmp</li>
   * </ol>
   * 
   * Do nothing if previous directory does not exist.
   * @param nsSd Block pool storage directory at <SD>/current/<nsid>
   */
  void doRollback(NamespaceInfo nsInfo)
      throws IOException {
    int numDirs = getNumStorageDirs();
    RollbackThread[] rollbackThreads = new RollbackThread[numDirs];
    // start to rollback
    for (int i=0; i<numDirs; i++) {
      final StorageDirectory sd = this.getStorageDir(i);
      RollbackThread thread = new RollbackThread(sd, nsInfo, new NameSpaceSliceStorage());
      thread.start();
      rollbackThreads[i] = thread;
    }
    // wait for rollback to be done
    for (RollbackThread thread : rollbackThreads) {
      try {
        thread.join();
      } catch (InterruptedException e) {
        return;
      }
    }
    // check for errors
    for (RollbackThread thread : rollbackThreads) {
      if (thread.error != null)
        throw new IOException(thread.error);
    }
  }
    
  /*
   * Finalize the namespace storage by deleting <NS>/previous directory
   * that holds the snapshot.
   */
  void doFinalize(File dnCurDir) throws IOException {
    File nsRoot = getNsRoot(namespaceID, dnCurDir);
    StorageDirectory nsSd = new StorageDirectory(nsRoot);
    // namespace level previous directory
    File prevDir = nsSd.getPreviousDir();
    if (!prevDir.exists()) {
      return; // already finalized
    }
    final String dataDirPath = nsSd.getRoot().getCanonicalPath();
    LOG.info("Finalizing upgrade for storage directory " + dataDirPath
        + ".\n   cur LV = " + this.getLayoutVersion() + "; cur CTime = "
        + this.getCTime());
    assert nsSd.getCurrentDir().exists() : "Current directory must exist.";
    
    // rename previous to finalized.tmp
    final File tmpDir = nsSd.getFinalizedTmp();
    rename(prevDir, tmpDir);

    // delete finalized.tmp dir in a separate thread
    new Daemon(new Runnable() {
      public void run() {
        try {
          deleteDir(tmpDir);
        } catch (IOException ex) {
          LOG.error("Finalize upgrade for " + dataDirPath + " failed.", ex);
        }
        LOG.info("Finalize upgrade for " + dataDirPath + " is complete.");
      }

      public String toString() {
        return "Finalize " + dataDirPath;
      }
    }).start();
  }

  private void verifyDistributedUpgradeProgress(UpgradeManagerDatanode um,
      NamespaceInfo nsInfo) throws IOException {
    assert um != null : "DataNode.upgradeManager is null.";
    um.setUpgradeState(false, getLayoutVersion());
    um.initializeUpgrade(nsInfo);
  }

  @Override
  public String toString() {
    return super.toString() + ";nsid=" + namespaceID;
  }
  
  /**
   * Get a namespace storage root based on data node storage root
   * @param nsID namespace ID
   * @param dnCurDir data node storage root directory
   * @return root directory for namespace storage
   */
  public static File getNsRoot(int namespaceId, File dnCurDir) {
    return new File(dnCurDir, getNamespaceDataDirName(namespaceId));
  }
  
  public File getNsRoot(File dnCurDir) {
    return new File(dnCurDir, getNamespaceDataDirName(namespaceID));
  }

  public static String getNamespaceDataDirName(int namespaceId) {
    return NS_DIR_PREFIX+String.valueOf(namespaceId);
  }
  public boolean isPreUpgradableLayout(StorageDirectory sd) throws IOException {
    return false;
  }

  @Override
  protected void corruptPreUpgradeStorage(File rootDir) throws IOException {
    // TODO Auto-generated method stub
    
  }

  @Override
  public boolean isConversionNeeded(StorageDirectory sd) throws IOException {
    // TODO Auto-generated method stub
    return false;
  }
}
