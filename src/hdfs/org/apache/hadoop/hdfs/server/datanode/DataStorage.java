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
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileLock;
import java.util.Collection;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.HardLink;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.FSConstants;
import org.apache.hadoop.hdfs.server.common.HdfsConstants.NodeType;
import org.apache.hadoop.hdfs.server.common.HdfsConstants.StartupOption;
import org.apache.hadoop.hdfs.server.common.Storage.StorageState;
import org.apache.hadoop.hdfs.server.common.InconsistentFSStateException;
import org.apache.hadoop.hdfs.server.common.Storage;
import org.apache.hadoop.hdfs.server.common.StorageInfo;
import org.apache.hadoop.hdfs.server.protocol.NamespaceInfo;
import org.apache.hadoop.util.Daemon;
import org.apache.hadoop.util.DiskChecker;
import org.apache.hadoop.io.IOUtils;

/** 
 * Data storage information file.
 * <p>
 * @see Storage
 */
public class DataStorage extends Storage {
  // Constants
  final static String BLOCK_SUBDIR_PREFIX = "subdir";
  final static String BLOCK_FILE_PREFIX = "blk_";
  final static String COPY_FILE_PREFIX = "dncp_";
  final static String STORAGE_DIR_DETACHED = "detach";
  public final static String STORAGE_DIR_TMP = "tmp";
  
  private final static String STORAGE_ID = "storageID";
  
  private String storageID;

  // flag to ensure initialzing storage occurs only once
  private boolean initialized = false;
  
  // NameSpaceStorage is map of <Name Space Id, NameSpaceStorage>
  private Map<Integer, NameSpaceSliceStorage> nsStorageMap
    = new HashMap<Integer, NameSpaceSliceStorage>();

  private final DataNode datanode;

  // Map of top level directory to layout version.
  Map<File, Integer> layoutMap = new HashMap<File, Integer>();

  DataStorage(DataNode datanode) {
    super(NodeType.DATA_NODE);
    storageID = "";
    this.datanode = datanode;
  }
  
  public DataStorage(StorageInfo storageInfo, String strgID, DataNode datanode) {
    super(NodeType.DATA_NODE, storageInfo);
    this.storageID = strgID;
    this.datanode = datanode;
  }

  public NameSpaceSliceStorage getNStorage(int namespaceId) {
    return nsStorageMap.get(namespaceId);
  }
  
  public String getStorageID() {
    return storageID;
  }
  
  void setStorageID(String newStorageID) {
    this.storageID = newStorageID;
  }

  synchronized void createStorageID(int datanodePort) {
    if (storageID != null && !storageID.isEmpty()) {
      return;
    }
    storageID = DataNode.createNewStorageId(datanodePort);
  }
  
  ArrayList<StorageDirectory> analyzeStorageDirs(NamespaceInfo nsInfo,
          Collection<File> dataDirs,
          StartupOption startOpt
          ) throws IOException {
    
    if (storageID == null)
      this.storageID = "";

    if (storageDirs == null) {
      this.storageDirs = new ArrayList<StorageDirectory>(dataDirs.size());
    } else {
      ((ArrayList<StorageDirectory>) storageDirs)
          .ensureCapacity(storageDirs.size() + dataDirs.size());
    }

    ArrayList<StorageDirectory> newDirs = new ArrayList<StorageDirectory>(
        dataDirs.size());
    ArrayList<StorageState> dataDirStates = new ArrayList<StorageState>(dataDirs.size());
    for(Iterator<File> it = dataDirs.iterator(); it.hasNext();) {
      File dataDir = it.next();
      StorageDirectory sd = new StorageDirectory(dataDir);
      StorageState curState;
      try {
        curState = sd.analyzeStorage(startOpt);
        // sd is locked but not opened
        switch(curState) {
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
          default:  // recovery part is common
            sd.doRecover(curState);
        }
      } catch (IOException ioe) {
        try {
          sd.unlock();
        }
        catch (IOException e) {
          LOG.warn("Exception when unlocking storage directory", e);
        }
        LOG.warn("Ignoring storage directory " + dataDir, ioe);
        //continue with other good dirs
        continue;
      }
      // add to the storage list
      addStorageDir(sd);
      newDirs.add(sd);
      dataDirStates.add(curState);
    }
    
    if (dataDirs.size() == 0)  // none of the data dirs exist
        throw new IOException(
                          "All specified directories are not accessible or do not exist.");
    
    return newDirs;
  }
  
  /**
   * Analyze storage directories.
   * Recover from previous transitions if required. 
   * Perform fs state transition if necessary depending on the namespace info.
   * Read storage info. 
   * 
   * @param nsInfo namespace information
   * @param dataDirs array of data storage directories
   * @param startOpt startup option
   * @throws IOException
   */
  synchronized void recoverTransitionRead(DataNode datanode,
                             NamespaceInfo nsInfo,
                             Collection<File> dataDirs,
                             StartupOption startOpt
                             ) throws IOException {
    if (initialized) {
      // DN storage has been initialized, no need to do anything
      return;
    }

    if (FSConstants.LAYOUT_VERSION != nsInfo.getLayoutVersion()) {
      throw new IOException(
          "Data-node and name-node layout versions must be the same. Namenode LV: "
              + nsInfo.getLayoutVersion() + ", current LV: "
              + FSConstants.LAYOUT_VERSION);
    }
    
    // 1. For each data directory calculate its state and 
    // check whether all is consistent before transitioning.
    // Format and recover.
    analyzeStorageDirs(nsInfo, dataDirs, startOpt);

    // 2. Do transitions
    // Each storage directory is treated individually.
    // During startup some of them can upgrade or rollback
    // while others could be uptodate for the regular startup.
    doTransition(storageDirs, nsInfo, startOpt);

    // 3. make sure we have storage id set - if not - generate new one
    createStorageID(datanode.getPort());

    // 4. Update all storages. Some of them might have just been formatted.
    this.writeAll();

    this.initialized = true;
  }
  
  /**
   * merge the data directory from srcDataDirs to dstDataDirs
   * @return true if merge succeeds; false if no merge happens
   */
  
  boolean doMerge(String[] srcDataDirs, Collection<File> dstDataDirs, 
      int namespaceId, NamespaceInfo nsInfo, StartupOption startOpt) 
    throws IOException {
    HashMap<File, File> dirsToMerge = new HashMap<File, File>();
    int i = 0;
    for (Iterator<File> it = dstDataDirs.iterator(); it.hasNext(); i++) {
      File dstDataDir = it.next();
      if (dstDataDir.exists()) {
        continue;
      }
      File srcDataDir = NameSpaceSliceStorage.getNsRoot(
          namespaceId, new File(srcDataDirs[i], STORAGE_DIR_CURRENT));
      if (!srcDataDir.exists() || !srcDataDir.isDirectory()) {
        LOG.info("Source data directory " +
            srcDataDir + " doesn't exist.");
        continue;
      }
      dirsToMerge.put(srcDataDir, dstDataDir);
    }
    if (dirsToMerge.size() == 0)
      //No merge is needed
      return false;
    
    if (dirsToMerge.size() != dstDataDirs.size()) {
      // Last merge succeeds partially
      throw new IOException("Merge fail: not all directories are merged successfully.");
    }
    
    MergeThread[] mergeThreads = new MergeThread[dirsToMerge.size()];
    // start to merge
    i = 0;
    for (Map.Entry<File, File> entry: dirsToMerge.entrySet()) {
      MergeThread thread = new MergeThread(entry.getKey(), entry.getValue(), nsInfo);
      thread.start();
      mergeThreads[i] = thread;
      i++;
    }
    // wait for merge to be done
    for (MergeThread thread : mergeThreads) {
      try {
        thread.join();
      } catch (InterruptedException e) {
        throw (InterruptedIOException)new InterruptedIOException().initCause(e);
      }
    }
    // check for errors
    for (MergeThread thread : mergeThreads) {
      if (thread.error != null)
        throw new IOException(thread.error);
    }
    return true;
  }

  /**
   * recoverTransitionRead for a specific Name Space
   * 
   * @param datanode DataNode
   * @param namespaceId name space Id
   * @param nsInfo Namespace info of namenode corresponding to the Name Space
   * @param dataDirs Storage directories
   * @param startOpt startup option
   * @throws IOException on error
   */
  void recoverTransitionRead(DataNode datanode, int namespaceId, NamespaceInfo nsInfo,
      Collection<File> dataDirs, StartupOption startOpt, String nameserviceId) throws IOException {
    // First ensure datanode level format/snapshot/rollback is completed
    // recoverTransitionRead(datanode, nsInfo, dataDirs, startOpt);
    
    // Create list of storage directories for the Name Space
    Collection<File> nsDataDirs = new ArrayList<File>();
    for(Iterator<File> it = dataDirs.iterator(); it.hasNext();) {
      File dnRoot = it.next();
      File nsRoot = NameSpaceSliceStorage.getNsRoot(
          namespaceId, new File(dnRoot, STORAGE_DIR_CURRENT));
      nsDataDirs.add(nsRoot);
    }
    boolean merged = false;
    String[] mergeDataDirs = nameserviceId == null? null: 
      datanode.getConf().getStrings("dfs.merge.data.dir." + nameserviceId);
    if (startOpt.equals(StartupOption.REGULAR) && mergeDataDirs != null
        && mergeDataDirs.length > 0) {
      assert mergeDataDirs.length == dataDirs.size();
      merged = doMerge(mergeDataDirs, nsDataDirs, namespaceId, nsInfo, startOpt);
    }
    if (!merged) {
      // mkdir for the list of NameSpaceStorage
      makeNameSpaceDataDir(nsDataDirs);
    }
    NameSpaceSliceStorage nsStorage = new NameSpaceSliceStorage(
        namespaceId, this.getCTime(), layoutMap);
    
    nsStorage.recoverTransitionRead(datanode, nsInfo, nsDataDirs, startOpt);
    addNameSpaceStorage(namespaceId, nsStorage);
  }

  /**
   * Create physical directory for Name Spaces on the data node
   * 
   * @param dataDirs
   *          List of data directories
   * @throws IOException on errors
   */
  public static void makeNameSpaceDataDir(Collection<File> dataDirs) throws IOException {
    for (File data : dataDirs) {
      try {
        DiskChecker.checkDir(data);
      } catch ( IOException e ) {
        LOG.warn("Invalid directory in: " + data.getCanonicalPath() + ": "
            + e.getMessage());
      }
    }
  }

  synchronized Collection<StorageDirectory> recoverTransitionAdditionalRead(NamespaceInfo nsInfo,
          Collection<File> dataDirs,
          StartupOption startOpt
          ) throws IOException{
    assert FSConstants.LAYOUT_VERSION == nsInfo.getLayoutVersion() :
        "Data-node and name-node layout versions must be the same.";
    
    // 1. For each data directory calculate its state and 
    // check whether all is consistent before transitioning.
    // Format and recover.
    ArrayList<StorageDirectory> newDirs = analyzeStorageDirs(nsInfo, dataDirs, startOpt);

    // 2. Do transitions
    // Each storage directory is treated individually.
    // During startup some of them can upgrade or rollback
    // while others could be uptodate for the regular startup.
    doTransition(newDirs, nsInfo, startOpt);
    assert this.getLayoutVersion() == nsInfo.getLayoutVersion() :
        "Data-node and name-node layout versions must be the same.";
    assert this.getCTime() == nsInfo.getCTime() :
        "Data-node and name-node CTimes must be the same.";

    // 3. Update all storages. Some of them might have just been formatted.
    if (this.layoutVersion == 0) {
      layoutVersion = FSConstants.LAYOUT_VERSION;
    }
    for (StorageDirectory sd : newDirs) {
      sd.write();
    }

    return newDirs;
  }

  void format(StorageDirectory sd, NamespaceInfo nsInfo) throws IOException {
    sd.clearDirectory(); // create directory
    this.layoutVersion = FSConstants.LAYOUT_VERSION;
    this.namespaceID = nsInfo.getNamespaceID();  // mother namespaceid
    this.cTime = 0;
    // store storageID as it currently is
    sd.write();
  }

  protected void setFields(Properties props, 
                           StorageDirectory sd 
                           ) throws IOException {
    props.setProperty(STORAGE_TYPE, storageType.toString());
    props.setProperty(LAYOUT_VERSION, String.valueOf(layoutVersion));
    props.setProperty(STORAGE_ID, getStorageID());
    // Set NamespaceID in version before federation
    if (layoutVersion > FSConstants.FEDERATION_VERSION) {
      props.setProperty(NAMESPACE_ID, String.valueOf(namespaceID));
      props.setProperty(CHECK_TIME, String.valueOf(cTime));
    }
  }

  protected void getFields(Properties props, 
                           StorageDirectory sd 
                           ) throws IOException {
    setLayoutVersion(props, sd);
    setStorageType(props, sd);

    // Read NamespaceID in version before federation
    if (layoutVersion > FSConstants.FEDERATION_VERSION) {
      setNamespaceID(props, sd);
      setcTime(props, sd);
    }

    String ssid = props.getProperty(STORAGE_ID);
    if (ssid == null ||
        !("".equals(storageID) || "".equals(ssid) ||
          storageID.equals(ssid)))
      throw new InconsistentFSStateException(sd.getRoot(),
          "has incompatible storage Id.");
    if ("".equals(storageID)) // update id only if it was empty
      storageID = ssid;
  }

  public boolean isConversionNeeded(StorageDirectory sd) throws IOException {
    File oldF = new File(sd.getRoot(), "storage");
    if (!oldF.exists())
      return false;
    // check the layout version inside the storage file
    // Lock and Read old storage file
    RandomAccessFile oldFile = new RandomAccessFile(oldF, "rws");
    FileLock oldLock = oldFile.getChannel().tryLock();
    try {
      oldFile.seek(0);
      int oldVersion = oldFile.readInt();
      if (oldVersion < LAST_PRE_UPGRADE_LAYOUT_VERSION)
        return false;
    } finally {
      oldLock.release();
      oldFile.close();
    }
    return true;
  }

  private boolean isNsLevelUpgraded(int namespaceId, StorageDirectory sd) {
    File nsRoot = NameSpaceSliceStorage.getNsRoot(namespaceId, sd.getCurrentDir());
    return new File(nsRoot, STORAGE_DIR_PREVIOUS).exists();
  }

  /**
   * Analyze which and whether a transition of the fs state is required and
   * perform it if necessary.
   * 
   * Rollback if (previousLV >= LAYOUT_VERSION && previousLV >
   * FEDERATION_VERSION)
   * Upgrade if this.LV > LAYOUT_VERSION && this.LV > FEDERATION_VERSION
   * Regular startup if this.LV = LAYOUT_VERSION && this.cTime = namenode.cTime
   * 
   * @param nsInfo
   *          namespace info
   * @param startOpt
   *          startup option
   * @throws IOException
   */
  private void doTransition(List<StorageDirectory> sds,
                             NamespaceInfo nsInfo, 
                             StartupOption startOpt
                             ) throws IOException {
    if (startOpt == StartupOption.ROLLBACK)
      doRollback(nsInfo); // rollback if applicable

    int numOfDirs = sds.size();
    List<StorageDirectory> dirsToUpgrade = new ArrayList<StorageDirectory>(numOfDirs);
    List<StorageInfo> dirsInfo = new ArrayList<StorageInfo>(numOfDirs);
    for (StorageDirectory sd : sds) {
      sd.read();
      layoutMap.put(sd.getRoot(), this.layoutVersion);
      checkVersionUpgradable(this.layoutVersion);
      assert this.layoutVersion >= FSConstants.LAYOUT_VERSION :
        "Future version is not allowed";
      
      boolean federationSupported = 
        this.layoutVersion <= FSConstants.FEDERATION_VERSION;
      // For pre-federation version - validate the namespaceID
      if (!federationSupported && 
          getNamespaceID() != nsInfo.getNamespaceID()) {
        sd.unlock();
        throw new IOException(
            "Incompatible namespaceIDs in " + sd.getRoot().getCanonicalPath()
            + ": namenode namespaceID = " + nsInfo.getNamespaceID() 
            + "; datanode namespaceID = " + getNamespaceID());
      }
      if (this.layoutVersion == FSConstants.LAYOUT_VERSION 
          && this.cTime == nsInfo.getCTime())
        continue; // regular startup
      // verify necessity of a distributed upgrade
      verifyDistributedUpgradeProgress(nsInfo);
      // do a global upgrade iff layout version changes and current layout is
      // older than FEDERATION.
      if (this.layoutVersion > FSConstants.LAYOUT_VERSION
          && this.layoutVersion > FSConstants.FEDERATION_VERSION) {
        if (isNsLevelUpgraded(getNamespaceID(), sd)) {
          throw new IOException("Ns level directory already upgraded for : " +
              sd.getRoot() + " ignoring upgrade");
        }
        dirsToUpgrade.add(sd);  // upgrade
        dirsInfo.add(new StorageInfo(this));
        continue;
      }
      if (this.cTime >= nsInfo.getCTime()) {
        // layoutVersion == LAYOUT_VERSION && this.cTime > nsInfo.cTime
        // must shutdown
        sd.unlock();
        throw new IOException("Datanode state: LV = " + this.getLayoutVersion() 
            + " CTime = " + this.getCTime() 
            + " is newer than the namespace state: LV = "
            + nsInfo.getLayoutVersion() 
            + " CTime = " + nsInfo.getCTime());
      }
    }
    
    // Now do upgrade if dirsToUpgrade is not empty
    if (!dirsToUpgrade.isEmpty()) {
      doUpgrade(dirsToUpgrade, dirsInfo, nsInfo);
    }
  }
  
  /**
   * A thread that merges a data storage directory from 
   * srcDataDir to dstDataDir
   */
  static class MergeThread extends Thread {
    private File srcNSDir;
    private File dstNSDir;
    private NamespaceInfo nsInfo;
    volatile Throwable error = null;
    private static final String STORAGE_DIR_MERGE_TMP = "merge.tmp";
    
    MergeThread(File srcNSDir, File dstNSDir, NamespaceInfo nsInfo) {
      this.srcNSDir = srcNSDir;
      this.dstNSDir = dstNSDir;
      this.nsInfo = nsInfo;
      this.setName("Merging " + srcNSDir + " to " + dstNSDir);
    }
    
    /* check if the directory is merged */
    private boolean isMerged() {
      return dstNSDir.exists();
    }
    
    public void run() {
      try {
        if (isMerged()) {
          return;
        }
        
        LOG.info("Merging storage directory " + srcNSDir + 
            " to " + dstNSDir);
        
        File mergeTmpDir = new File(dstNSDir.getParent(), STORAGE_DIR_MERGE_TMP);
        NameSpaceSliceStorage nsStorage = new NameSpaceSliceStorage(
            nsInfo.getNamespaceID(), nsInfo.getCTime());
        nsStorage.format(mergeTmpDir, nsInfo);
        
        assert srcNSDir.exists() : "Source directory must exist.";
        File mergeTmpNSDir = nsStorage.getNsRoot(mergeTmpDir);
        File srcCurNsDir = new File(srcNSDir, STORAGE_DIR_CURRENT);
        File mergeTmpCurNSDir = new File(mergeTmpNSDir, STORAGE_DIR_CURRENT);
        // hardlink all blocks  
        HardLink hardLink = new HardLink();
        linkBlocks(new File(srcCurNsDir, STORAGE_DIR_FINALIZED),
            new File(mergeTmpCurNSDir, STORAGE_DIR_FINALIZED), 
            nsInfo.getLayoutVersion(), hardLink, true);
        linkBlocks(new File(srcCurNsDir, STORAGE_DIR_RBW),
            new File(mergeTmpCurNSDir, STORAGE_DIR_RBW), 
            nsInfo.getLayoutVersion(), hardLink, true);
        
        // finally rename the tmp dir to dst dir
        if (!mergeTmpNSDir.renameTo(dstNSDir)) {
          throw new IOException("Cannot rename tmp directory " + mergeTmpNSDir + 
              " to dst directory " + dstNSDir);
        }
      } catch (Throwable t) {
        error = t;
      }
    }
  }

  /**
   * A thread that upgrades a data storage directory
   */
  static class UpgradeThread extends Thread {
    private StorageDirectory sd;
    private StorageInfo si;
    private NamespaceInfo nsInfo;
    volatile Throwable error = null;
    private File topCurDir;
    private File[] namespaceDirs;
    
    UpgradeThread(StorageDirectory sd, StorageInfo si, NamespaceInfo nsInfo) {
      this.sd = sd;
      this.si = si;
      this.nsInfo = nsInfo;
      this.topCurDir = sd.getCurrentDir();
      this.namespaceDirs = topCurDir.listFiles(new FilenameFilter() {
        @Override
        public boolean accept(File dir, String file) {
          return file.startsWith(NameSpaceSliceStorage.NS_DIR_PREFIX);
        }
      });
      this.setName("Upgrading " + sd.getRoot());
    }
    
    /** check if any of the namespace directory has a snapshot */
    private boolean isNamespaceUpgraded() {
      for (File namespaceDir : namespaceDirs) {
        if (new File(namespaceDir, STORAGE_DIR_PREVIOUS).exists()) {
          return true;
        }
      }
      return false;
    }
    
    public void run() {
      try {
        if (isNamespaceUpgraded()) {
          /// disallow coexistence of global and per namespace snapshots
          throw new IOException(
              "Local snapshot exists. Please either finalize or rollback first!");
        }

        LOG.info("Upgrading storage directory " + sd.getRoot()
            + ".\n   old LV = " + si.getLayoutVersion()
            + "; old CTime = " + si.getCTime()
            + ".\n   new LV = " + nsInfo.getLayoutVersion()
            + "; new CTime = " + nsInfo.getCTime());
        File curDir = sd.getCurrentDir();
        File prevDir = sd.getPreviousDir();
        // remove prev dir if it exists
        if (prevDir.exists()) {
          deleteDir(prevDir);
        }
        assert curDir.exists() : "Current directory must exist.";
        File tmpDir = sd.getPreviousTmp();
        assert !tmpDir.exists() : "previous.tmp directory must not exist.";
        // rename current to tmp
        rename(curDir, tmpDir);
        
        // hardlink blocks
        upgrade(si.getLayoutVersion(), nsInfo.getLayoutVersion(),
            tmpDir, curDir);
      } catch (Throwable t) {
        error = t;
      }
    }
    
    private void upgrade(int oldLayoutVersion, int curLayoutVersion,
        File tmpDir, File curDir) throws IOException {
      HardLink hardLink = new HardLink();
      if (oldLayoutVersion <= FSConstants.FEDERATION_VERSION) {
        // upgrade from a federation version to a newer federation version
        // link top directory
        linkBlocks(new File(tmpDir, STORAGE_DIR_FINALIZED),
            new File(curDir, STORAGE_DIR_FINALIZED), curLayoutVersion,
            hardLink, true);
        linkBlocks(new File(tmpDir, STORAGE_DIR_RBW),
            new File(curDir, STORAGE_DIR_RBW), curLayoutVersion,
            hardLink, true);
        // link all namespace directories
        for (File namespaceDir : namespaceDirs) {
          File tmpNamespaceCurDir = new File(
              new File(tmpDir, namespaceDir.getName()), STORAGE_DIR_CURRENT);
          File namespaceDirCur = new File(namespaceDir, STORAGE_DIR_CURRENT);
          linkBlocks(new File(tmpNamespaceCurDir, STORAGE_DIR_FINALIZED),
              new File(namespaceDirCur, STORAGE_DIR_FINALIZED), 
              curLayoutVersion, hardLink, true);
          linkBlocks(new File(tmpNamespaceCurDir, STORAGE_DIR_RBW),
              new File(namespaceDirCur, STORAGE_DIR_RBW), 
              curLayoutVersion, hardLink, true);
          //link Version file
          linkBlocks(new File(tmpNamespaceCurDir, STORAGE_FILE_VERSION),
              new File(namespaceDirCur, STORAGE_FILE_VERSION), 
              curLayoutVersion, hardLink, true);
        }
      } else if (oldLayoutVersion <= FSConstants.RBW_LAYOUT_VERSION) {
        // upgrade from RBW layout version to Federation.
        // This is the directory data/current/NS-/
        File curNsDir = NameSpaceSliceStorage.getNsRoot(
            nsInfo.getNamespaceID(), curDir);
        NameSpaceSliceStorage nsStorage = new NameSpaceSliceStorage(
            nsInfo.getNamespaceID(), nsInfo.getCTime());
        nsStorage.format(curDir, nsInfo);

        // Move all blocks to this namespace directory
        // This is the directory data/current/NS-/current.
        File nsCurDir = new File(curNsDir, STORAGE_DIR_CURRENT);
        File curNsDirFinalized = new File(nsCurDir, STORAGE_DIR_FINALIZED);
        File curNsDirRbw = new File(nsCurDir, STORAGE_DIR_RBW);
        linkBlocks(new File(tmpDir, STORAGE_DIR_FINALIZED), curNsDirFinalized,
            curLayoutVersion, hardLink, false);
        linkBlocks(new File(tmpDir, STORAGE_DIR_RBW), curNsDirRbw,
            curLayoutVersion, hardLink, false);
      } else {
        // upgrade pre-rbw version to federation version
        // create the directory for the namespace
        File curNsDir = NameSpaceSliceStorage.getNsRoot(
            nsInfo.getNamespaceID(), curDir);
        NameSpaceSliceStorage nsStorage = new NameSpaceSliceStorage(
            nsInfo.getNamespaceID(), nsInfo.getCTime());
        nsStorage.format(curDir, nsInfo);

        // Move all blocks to this namespace directory
        File nsCurDir = new File(curNsDir, STORAGE_DIR_CURRENT);
        // Move finalized blocks
        File nsDirFinalized = new File(nsCurDir, STORAGE_DIR_FINALIZED);
        linkBlocks(tmpDir, nsDirFinalized, curLayoutVersion, hardLink, true);
        // Move rbw blocks
        File nsDirRbw = new File(nsCurDir, STORAGE_DIR_RBW);
        File oldDirRbw = new File(tmpDir.getParentFile(), OLD_STORAGE_DIR_RBW);
        linkBlocks(oldDirRbw, nsDirRbw, curLayoutVersion, hardLink, true);
      }
      LOG.info("Completed upgrading storage directory " + sd.getRoot() +
          " " + hardLink.linkStats.report());
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

  private void doRollback(NamespaceInfo nsInfo) throws IOException {
    int numDirs = getNumStorageDirs();
    RollbackThread[] rollbackThreads = new RollbackThread[numDirs];
    // start to rollback
    for (int i=0; i<numDirs; i++) {
      final StorageDirectory sd = this.getStorageDir(i);
      RollbackThread thread = new RollbackThread(sd, nsInfo, new DataStorage(
          datanode));
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

  static class RollbackThread extends Thread {
    private StorageDirectory sd;
    private NamespaceInfo nsInfo;
    volatile Throwable error;
    private Storage prevInfo;
    
    RollbackThread(StorageDirectory sd, NamespaceInfo nsInfo, 
        Storage prevInfo) {
      this.sd = sd;
      this.nsInfo = nsInfo;
      this.setName("Rolling back " + sd.getRoot());
      this.prevInfo = prevInfo;
    }

    private boolean canRollBack(boolean globalRollback) {
      if (globalRollback) {
        return (prevInfo.getLayoutVersion() >= FSConstants.LAYOUT_VERSION);
      } else {
        return ((prevInfo.getLayoutVersion() >= FSConstants.LAYOUT_VERSION
            || prevInfo.getCTime() <= nsInfo.getCTime()));
      }
    }
    
    public void run() {
      try {
        File prevDir = sd.getPreviousDir();
        // regular startup if previous dir does not exist
        if (!prevDir.exists()) {
          return;
        }
        StorageDirectory prevSD = prevInfo.new StorageDirectory(sd.getRoot());
        prevSD.read(prevSD.getPreviousVersionFile());

        boolean globalRollback = prevInfo instanceof DataStorage;
        if (!canRollBack(globalRollback))
          throw new InconsistentFSStateException(prevSD.getRoot(),
              "Cannot rollback to a newer state.\nDatanode previous state: LV = " 
              + prevInfo.getLayoutVersion() + " CTime = " + prevInfo.getCTime() 
              + " is newer than the namespace state: LV = "
              + nsInfo.getLayoutVersion() + " CTime = " + nsInfo.getCTime());
        LOG.info("Rolling back storage directory " + sd.getRoot()
            + ".\n   target LV = " + nsInfo.getLayoutVersion()
            + "; target CTime = " + nsInfo.getCTime());
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
        LOG.info("Rollback of " + sd.getRoot() + " is complete.");
      } catch (Throwable t) {
        error = t;
      }
    }
  }
  
  void doFinalize(StorageDirectory sd) throws IOException {
    File prevDir = sd.getPreviousDir();
    if (!prevDir.exists())
      return; // already discarded
    final String dataDirPath = sd.getRoot().getCanonicalPath();
    LOG.info("Finalizing upgrade for storage directory "
             + dataDirPath
             + ".\n   cur LV = " + this.getLayoutVersion()
             + "; cur CTime = " + this.getCTime());
    assert sd.getCurrentDir().exists() : "Current directory must exist.";
    final File tmpDir = sd.getFinalizedTmp();
    // rename previous to tmp
    rename(prevDir, tmpDir);

    // delete tmp dir in a separate thread
    new Daemon(new Runnable() {
        public void run() {
          try {
            deleteDir(tmpDir);
          } catch(IOException ex) {
            LOG.error("Finalize upgrade for " + dataDirPath + " failed.", ex);
          }
          LOG.info("Finalize upgrade for " + dataDirPath + " is complete.");
        }
        public String toString() { return "Finalize " + dataDirPath; }
      }).start();
  }
  
  void finalizeUpgrade() throws IOException {
    for (Iterator<StorageDirectory> it = storageDirs.iterator(); it.hasNext();) {
      doFinalize(it.next());
    }
  }

  void finalizedUpgrade(int namespaceId) throws IOException {
    // To handle finalizing a snapshot taken at datanode level while
    // upgrading to federation, if datanode level snapshot previous exists,
    // then finalize it. Else finalize the corresponding BP.
    for (StorageDirectory sd : storageDirs) {
      File prevDir = sd.getPreviousDir();
      File curDir = sd.getCurrentDir();
      NameSpaceSliceStorage nsStorage = nsStorageMap.get(namespaceId);
      File nsRoot = nsStorage.getNsRoot(namespaceId, curDir);
      StorageDirectory nsSd = new StorageDirectory(nsRoot);
      if (prevDir.exists() && nsSd.getPreviousDir().exists()) {
        throw new IOException("Top level and NS level previous directories"
            + " cannot co-exist");
      }
      if (prevDir.exists()) {
        // data node level storage finalize
        doFinalize(sd);
      } else {
        // Name Space storage finalize using specific namespaceId
        nsStorage.doFinalize(curDir);
      }
    }
  }
  
  static void linkBlocks(File from, File to, int oldLV, HardLink hl, boolean createTo)
  throws IOException {
    if (!from.exists()) {
      LOG.warn(from + " does not exist");
      return;
    }
    if (!from.isDirectory()) {
      if (from.getName().startsWith(COPY_FILE_PREFIX) ||
          from.getName().equals(Storage.STORAGE_FILE_VERSION)) {
        FileInputStream in = new FileInputStream(from);
        FileOutputStream out = new FileOutputStream(to);
        try {
          IOUtils.copyBytes(in, out, 16*1024, true);
          hl.linkStats.countPhysicalFileCopies++;
        } finally {
          IOUtils.closeStream(in);
          IOUtils.closeStream(out);
        }
      } else {
        
        //check if we are upgrading from pre-generation stamp version.
        if (oldLV >= PRE_GENERATIONSTAMP_LAYOUT_VERSION) {
          // Link to the new file name.
          to = new File(convertMetatadataFileName(to.getAbsolutePath()));
        }
        
        HardLink.createHardLink(from, to);
        hl.linkStats.countSingleLinks++;
      }
      return;
    }
    // from is a directory
    hl.linkStats.countDirs++;
    if (createTo && !to.exists() && !to.mkdirs())
      throw new IOException("Cannot create directory " + to);
    
    //If upgrading from old stuff, need to munge the filenames.  That has to
    //be done one file at a time, so hardlink them one at a time (slow).
    if (oldLV >= PRE_GENERATIONSTAMP_LAYOUT_VERSION) {
      String[] blockNames = from.list(new java.io.FilenameFilter() {
        public boolean accept(File dir, String name) {
          return name.startsWith(BLOCK_SUBDIR_PREFIX) 
          || name.startsWith(BLOCK_FILE_PREFIX)
          || name.startsWith(COPY_FILE_PREFIX);
        }
      });
      if (blockNames.length == 0) {
        hl.linkStats.countEmptyDirs++;
      } else {
        for(int i = 0; i < blockNames.length; i++)
          linkBlocks(new File(from, blockNames[i]), 
            new File(to, blockNames[i]), oldLV, hl, true);
      }
    } else {
      //If upgrading from a relatively new version, we only need to create
      //links with the same filename.  This can be done in bulk (much faster).
      String[] blockNames = from.list(new java.io.FilenameFilter() {
        public boolean accept(File dir, String name) {
          return name.startsWith(BLOCK_FILE_PREFIX);
        }
      });
      if (blockNames.length > 0) {
        HardLink.createHardLinkMult(from, blockNames, to);
        hl.linkStats.countMultLinks++;
        hl.linkStats.countFilesMultLinks += blockNames.length;
      } else {
        hl.linkStats.countEmptyDirs++;
      }
      
      //now take care of the rest of the files and subdirectories
      String[] otherNames = from.list(new java.io.FilenameFilter() {
          public boolean accept(File dir, String name) {
            return name.startsWith(BLOCK_SUBDIR_PREFIX) 
              || name.startsWith(COPY_FILE_PREFIX);
          }
        });
      for(int i = 0; i < otherNames.length; i++)
        linkBlocks(new File(from, otherNames[i]), 
            new File(to, otherNames[i]), oldLV, hl, true);
    }
  }

  protected void corruptPreUpgradeStorage(File rootDir) throws IOException {
    File oldF = new File(rootDir, "storage");
    if (oldF.exists())
      return;
    // recreate old storage file to let pre-upgrade versions fail
    if (!oldF.createNewFile())
      throw new IOException("Cannot create file " + oldF);
    RandomAccessFile oldFile = new RandomAccessFile(oldF, "rws");
    // write new version into old storage file
    try {
      writeCorruptedData(oldFile);
    } finally {
      oldFile.close();
    }
  }

  private void verifyDistributedUpgradeProgress(
                  NamespaceInfo nsInfo
                ) throws IOException {
    UpgradeManagerDatanode um = datanode.getUpgradeManager(nsInfo
        .getNamespaceID());
    assert um != null : "DataNode.upgradeManager is null.";
    um.setUpgradeState(false, getLayoutVersion());
    um.initializeUpgrade(nsInfo);
  }

  private static final Pattern PRE_GENSTAMP_META_FILE_PATTERN =
    Pattern.compile("(.*blk_[-]*\\d+)\\.meta$");
  /**
   * This is invoked on target file names when upgrading from pre generation
   * stamp version (version -13) to correct the metatadata file name.
   * @param oldFileName
   * @return the new metadata file name with the default generation stamp.
   */
  private static String convertMetatadataFileName(String oldFileName) {
    Matcher matcher = PRE_GENSTAMP_META_FILE_PATTERN.matcher(oldFileName);
    if (matcher.matches()) {
      //return the current metadata file name
      return BlockWithChecksumFileWriter.getMetaFileName(matcher.group(1),
                                       Block.GRANDFATHER_GENERATION_STAMP);
    }
    return oldFileName;
  }
  
  /** 
   * Add nsStorage into nsStorageMap
   */  
  private void addNameSpaceStorage(int nsID, NameSpaceSliceStorage nsStorage)
      throws IOException {
    if (!this.nsStorageMap.containsKey(nsID)) {
      this.nsStorageMap.put(nsID, nsStorage);
    }   
  }

  synchronized void removeNamespaceStorage(int nsId) {                                      
    nsStorageMap.remove(nsId);
  }
  

  /**
   * Get the data directory name that stores the namespace's blocks
   * @param namespaceId namespace id
   * @return the name of the last component of 
   *         the given namespace's data directory
   */
  String getNameSpaceDataDir(int namespaceId) {
    return NameSpaceSliceStorage.getNamespaceDataDirName(namespaceId);
  }
}
