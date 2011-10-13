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
import java.io.IOException;
import java.io.InterruptedIOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileLock;
import java.util.Collection;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.fs.HardLink;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.FSConstants;
import org.apache.hadoop.hdfs.server.common.HdfsConstants.NodeType;
import org.apache.hadoop.hdfs.server.common.HdfsConstants.StartupOption;
import org.apache.hadoop.hdfs.server.common.InconsistentFSStateException;
import org.apache.hadoop.hdfs.server.common.Storage;
import org.apache.hadoop.hdfs.server.common.StorageInfo;
import org.apache.hadoop.hdfs.server.protocol.NamespaceInfo;
import org.apache.hadoop.util.Daemon;
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
  
  private String storageID;

  DataStorage() {
    super(NodeType.DATA_NODE);
    storageID = "";
  }
  
  DataStorage(int nsID, long cT, String strgID) {
    super(NodeType.DATA_NODE, nsID, cT);
    this.storageID = strgID;
  }
  
  public DataStorage(StorageInfo storageInfo, String strgID) {
    super(NodeType.DATA_NODE, storageInfo);
    this.storageID = strgID;
  }

  public String getStorageID() {
    return storageID;
  }
  
  void setStorageID(String newStorageID) {
    this.storageID = newStorageID;
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
  void recoverTransitionRead(NamespaceInfo nsInfo,
                             Collection<File> dataDirs,
                             StartupOption startOpt
                             ) throws IOException {
    assert FSConstants.LAYOUT_VERSION == nsInfo.getLayoutVersion() :
      "Data-node and name-node layout versions must be the same.";
    
    // 1. For each data directory calculate its state and 
    // check whether all is consistent before transitioning.
    // Format and recover.
    this.storageID = "";
    this.storageDirs = new ArrayList<StorageDirectory>(dataDirs.size());
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
          LOG.info("Formatting ...");
          format(sd, nsInfo);
          break;
        default:  // recovery part is common
          sd.doRecover(curState);
        }
      } catch (IOException ioe) {
        sd.unlock();
        throw ioe;
      }
      // add to the storage list
      addStorageDir(sd);
      dataDirStates.add(curState);
    }

    if (dataDirs.size() == 0)  // none of the data dirs exist
      throw new IOException(
                            "All specified directories are not accessible or do not exist.");

    // 2. Do transitions
    // Each storage directory is treated individually.
    // During startup some of them can upgrade or rollback 
    // while others could be uptodate for the regular startup.
    doTransition(nsInfo, startOpt);
    
    // 3. Update all storages. Some of them might have just been formatted.
    this.writeAll();
  }

  void format(StorageDirectory sd, NamespaceInfo nsInfo) throws IOException {
    sd.clearDirectory(); // create directory
    this.layoutVersion = FSConstants.LAYOUT_VERSION;
    this.namespaceID = nsInfo.getNamespaceID();
    this.cTime = 0;
    // store storageID as it currently is
    sd.write();
  }

  protected void setFields(Properties props, 
                           StorageDirectory sd 
                           ) throws IOException {
    super.setFields(props, sd);
    props.setProperty("storageID", storageID);
  }

  protected void getFields(Properties props, 
                           StorageDirectory sd 
                           ) throws IOException {
    super.getFields(props, sd);
    String ssid = props.getProperty("storageID");
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
  
  /**
   * Analyze which and whether a transition of the fs state is required
   * and perform it if necessary.
   * 
   * Rollback if previousLV >= LAYOUT_VERSION && prevCTime <= namenode.cTime
   * Upgrade if this.LV > LAYOUT_VERSION || this.cTime < namenode.cTime
   * Regular startup if this.LV = LAYOUT_VERSION && this.cTime = namenode.cTime
   * 
   * @param nsInfo  namespace info
   * @param startOpt  startup option
   * @throws IOException
   */
  private void doTransition( NamespaceInfo nsInfo, 
                             StartupOption startOpt
                             ) throws IOException {
    if (startOpt == StartupOption.ROLLBACK)
      doRollback(nsInfo); // rollback if applicable

    int numOfDirs = getNumStorageDirs();
    List<StorageDirectory> dirsToUpgrade = new ArrayList<StorageDirectory>(numOfDirs);
    List<StorageInfo> dirsInfo = new ArrayList<StorageInfo>(numOfDirs);
    for(int idx = 0; idx < numOfDirs; idx++) {
      StorageDirectory sd = this.getStorageDir(idx);
      sd.read();
      checkVersionUpgradable(this.layoutVersion);
      assert this.layoutVersion >= FSConstants.LAYOUT_VERSION :
        "Future version is not allowed";
      if (getNamespaceID() != nsInfo.getNamespaceID())
        throw new IOException(
            "Incompatible namespaceIDs in " + sd.getRoot().getCanonicalPath()
            + ": namenode namespaceID = " + nsInfo.getNamespaceID() 
            + "; datanode namespaceID = " + getNamespaceID());
      if (this.layoutVersion == FSConstants.LAYOUT_VERSION 
          && this.cTime == nsInfo.getCTime())
        continue; // regular startup
      // verify necessity of a distributed upgrade
      verifyDistributedUpgradeProgress(nsInfo);
      if (this.layoutVersion > FSConstants.LAYOUT_VERSION
          || this.cTime < nsInfo.getCTime()) {
        dirsToUpgrade.add(sd);  // upgrade
        dirsInfo.add(new StorageInfo(this));
        continue;
      }
      // layoutVersion == LAYOUT_VERSION && this.cTime > nsInfo.cTime
      // must shutdown
      throw new IOException("Datanode state: LV = " + this.getLayoutVersion() 
          + " CTime = " + this.getCTime() 
          + " is newer than the namespace state: LV = "
          + nsInfo.getLayoutVersion() 
          + " CTime = " + nsInfo.getCTime());
    }
    
    // Now do upgrade if dirsToUpgrade is not empty
    if (!dirsToUpgrade.isEmpty()) {
      doUpgrade(dirsToUpgrade, dirsInfo, nsInfo);
    }
  }

  /**
   * A thread that upgrades a data storage directory
   */
  private static class UpgradeThread extends Thread {
    private StorageDirectory sd;
    private StorageInfo si;
    private NamespaceInfo nsInfo;
    volatile private Throwable error = null;
    
    private UpgradeThread(StorageDirectory sd, StorageInfo si, NamespaceInfo nsInfo) {
      this.sd = sd;
      this.si = si;
      this.nsInfo = nsInfo;
      this.setName("Upgrading " + sd.getRoot());
    }
    public void run() {
      try {
        LOG.info("Upgrading storage directory " + sd.getRoot()
            + ".\n   old LV = " + si.getLayoutVersion()
            + "; old CTime = " + si.getCTime()
            + ".\n   new LV = " + nsInfo.getLayoutVersion()
            + "; new CTime = " + nsInfo.getCTime());
        File curDir = sd.getCurrentDir();
        File prevDir = sd.getPreviousDir();
        assert curDir.exists() : "Current directory must exist.";
        File tmpDir = sd.getPreviousTmp();
        assert !tmpDir.exists() : "previous.tmp directory must not exist.";
        // rename current to tmp
        rename(curDir, tmpDir);
        // hardlink blocks
        HardLink hardLink = new HardLink();
        linkBlocks(tmpDir, curDir, si.getLayoutVersion(), hardLink);
        LOG.info("Completed upgrading storage directory " + sd.getRoot() +
            " " + hardLink.linkStats.report());
      } catch (Throwable t) {
        error = t;
      }
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
    // start to upgrade
    for (int i=0; i<numDirs; i++) {
      final StorageDirectory sd = this.getStorageDir(i);
      RollbackThread thread = new RollbackThread(sd, nsInfo);
      thread.start();
      rollbackThreads[i] = thread;
    }
    // wait for upgrade to be done
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
   * A thread that rolls back a data storage directory
   */
  private static class RollbackThread extends Thread {
    private StorageDirectory sd;
    private NamespaceInfo nsInfo;
    private Throwable error;
    
    private RollbackThread(StorageDirectory sd, NamespaceInfo nsInfo) {
      this.sd = sd;
      this.nsInfo = nsInfo;
      this.setName("Rolling back " + sd.getRoot());
    }
    public void run() {
      try {
        File prevDir = sd.getPreviousDir();
        // regular startup if previous dir does not exist
        if (!prevDir.exists())
          return;
        DataStorage prevInfo = new DataStorage();
        StorageDirectory prevSD = prevInfo.new StorageDirectory(sd.getRoot());
        prevSD.read(prevSD.getPreviousVersionFile());

        // We allow rollback to a state, which is either consistent with
        // the namespace state or can be further upgraded to it.
        if (!(prevInfo.getLayoutVersion() >= FSConstants.LAYOUT_VERSION
            && prevInfo.getCTime() <= nsInfo.getCTime()))  // cannot rollback
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
  
  static void linkBlocks(File from, File to, int oldLV, HardLink hl)
  throws IOException {
    if (!from.isDirectory()) {
      if (from.getName().startsWith(COPY_FILE_PREFIX)) {
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
    if (!to.mkdir())
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
            new File(to, blockNames[i]), oldLV, hl);
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
            new File(to, otherNames[i]), oldLV, hl);
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
    UpgradeManagerDatanode um = DataNode.getDataNode().upgradeManager;
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
      return FSDataset.getMetaFileName(matcher.group(1),
                                       Block.GRANDFATHER_GENERATION_STAMP); 
    }
    return oldFileName;
  }
}
