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
package org.apache.hadoop.hdfs.qjournal.server;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hdfs.server.common.HdfsConstants.NodeType;
import org.apache.hadoop.hdfs.server.common.HdfsConstants.StartupOption;
import org.apache.hadoop.hdfs.server.common.InconsistentFSStateException;
import org.apache.hadoop.hdfs.server.common.Storage;
import org.apache.hadoop.hdfs.server.common.StorageErrorReporter;
import org.apache.hadoop.hdfs.server.common.StorageInfo;
import org.apache.hadoop.hdfs.server.namenode.FileJournalManager;
import org.apache.hadoop.hdfs.server.namenode.NNStorage;
import org.apache.hadoop.hdfs.server.namenode.NNStorage.NameNodeDirType;
import org.apache.hadoop.hdfs.server.namenode.NNStorage.NameNodeFile;
import org.apache.hadoop.hdfs.server.namenode.NNStorageDirectoryRetentionManager;
import org.apache.hadoop.hdfs.server.protocol.NamespaceInfo;
import org.apache.hadoop.hdfs.util.InjectionEvent;
import org.apache.hadoop.util.InjectionEventI;
import org.apache.hadoop.util.InjectionHandler;

import com.google.common.collect.ImmutableList;

/**
 * A {@link Storage} implementation for the {@link JournalNode}.
 * 
 * The JN has a storage directory for each namespace for which it stores
 * metadata. There is only a single directory per JN in the current design.
 * It is used for managing storage directories for both edits and image.
 */
public class JNStorage extends Storage {

  private final StorageDirectory sd;
  private StorageState state;
  private final boolean isImageDir;
  private Configuration conf;
  

  private static final List<Pattern> EDITS_CURRENT_DIR_PURGE_REGEXES =
      ImmutableList.of(
        Pattern.compile("edits_\\d+-(\\d+)"),
        Pattern.compile("edits_inprogress_(\\d+)(?:\\..*)?"));
  
  private static final List<Pattern> IMAGE_CURRENT_DIR_PURGE_REGEXES = 
      ImmutableList.of(
        Pattern.compile(NameNodeFile.IMAGE.getName() + "_(-?\\d+)"),
        Pattern.compile(NameNodeFile.IMAGE_NEW.getName() + "_(-?\\d+)"),
        Pattern.compile(NameNodeFile.IMAGE.getName() + "_(-?\\d+)\\.md5"));
  
  private static final List<Pattern> PAXOS_DIR_PURGE_REGEXES = 
      ImmutableList.of(Pattern.compile("(\\d+)"));

  static final String PAXOS_DIR = "paxos";

  /**
   * @param logDir the path to the directory in which data will be stored
   * @param errorReporter a callback to report errors
   * @throws IOException 
   */
  protected JNStorage(File logDir, StorageErrorReporter errorReporter,
      boolean imageDir, Configuration conf) throws IOException {
    super(NodeType.JOURNAL_NODE);
    this.conf = conf;
    sd = new StorageDirectory(logDir, imageDir ? NameNodeDirType.IMAGE
        : NameNodeDirType.EDITS);
    this.addStorageDir(sd);
    this.isImageDir = imageDir;
    // null for metrics, will be extended
    
    
    analyzeStorage();
  }

  /**
   * Find an edits file spanning the given transaction ID range.
   * If no such file exists, an exception is thrown.
   */
  File findFinalizedEditsFile(long startTxId, long endTxId) throws IOException {
    File ret = new File(sd.getCurrentDir(),
        NNStorage.getFinalizedEditsFileName(startTxId, endTxId));
    if (!ret.exists()) {
      throw new IOException(
          "No edits file for range " + startTxId + "-" + endTxId);
    }
    return ret;
  }

  /**
   * @return the path for an in-progress edits file starting at the given
   * transaction ID. This does not verify existence of the file. 
   */
  File getInProgressEditLog(long startTxId) {
    return new File(sd.getCurrentDir(),
        NNStorage.getInProgressEditsFileName(startTxId));
  }
  
  public File getCheckpointImageFile(long txid) {
    return NNStorage.getCheckpointImageFile(sd, txid);
  }
  
  public File getImageFile(long txid) {
    return NNStorage.getImageFile(sd, txid);
  }
  
  /**
   * @param segmentTxId the first txid of the segment
   * @param epoch the epoch number of the writer which is coordinating
   * recovery
   * @return the temporary path in which an edits log should be stored
   * while it is being downloaded from a remote JournalNode
   */
  File getSyncLogTemporaryFile(long segmentTxId, long epoch) {
    String name = NNStorage.getInProgressEditsFileName(segmentTxId) +
        ".epoch=" + epoch; 
    return new File(sd.getCurrentDir(), name);
  }
  
  /**
   * Get name for temporary file used for log syncing, after a journal node
   * crashed.
   */
  File getSyncLogTemporaryFile(long segmentTxId, long endTxId, long stamp) {
    String name = NNStorage.getFinalizedEditsFileName(segmentTxId, endTxId) +
        ".tmp=" + stamp; 
    return new File(sd.getCurrentDir(), name);
  }
  
  /**
   * Get name for destination file used for log syncing, after a journal node
   * crashed.
   */
  File getSyncLogDestFile(long segmentTxId, long endTxId) {
    String name = NNStorage.getFinalizedEditsFileName(segmentTxId, endTxId); 
    return new File(sd.getCurrentDir(), name);
  }

  /**
   * @return the path for the file which contains persisted data for the
   * paxos-like recovery process for the given log segment.
   */
  File getPaxosFile(long segmentTxId) throws IOException {
    if (isImageDir) {
      throwIOException("Paxos data is not present in image directory");
    }
    return new File(getPaxosDir(), String.valueOf(segmentTxId));
  }
  
  File getPaxosDir() throws IOException {
    if (isImageDir) {
      throwIOException("Paxos data is not present in image directory");
    }
    return new File(sd.getCurrentDir(), PAXOS_DIR);
  }
  
  /**
   * Remove any log files and associated paxos files which are older than
   * the given txid.
   */
  void purgeDataOlderThan(long minTxIdToKeep) throws IOException {
    if (isImageDir) {
      purgeMatching(sd.getCurrentDir(), IMAGE_CURRENT_DIR_PURGE_REGEXES,
          minTxIdToKeep);
    } else {
      purgeMatching(sd.getCurrentDir(), EDITS_CURRENT_DIR_PURGE_REGEXES,
          minTxIdToKeep);
      purgeMatching(getPaxosDir(), PAXOS_DIR_PURGE_REGEXES, minTxIdToKeep);
    }
  }
  
  /**
   * Purge files in the given directory which match any of the set of patterns.
   * The patterns must have a single numeric capture group which determines
   * the associated transaction ID of the file. Only those files for which
   * the transaction ID is less than the <code>minTxIdToKeep</code> parameter
   * are removed.
   */
  private static void purgeMatching(File dir, List<Pattern> patterns,
      long minTxIdToKeep) throws IOException {

    for (File f : FileUtil.listFiles(dir)) {
      if (!f.isFile()) continue;
      
      for (Pattern p : patterns) {
        Matcher matcher = p.matcher(f.getName());
        if (matcher.matches()) {
          // This parsing will always succeed since the group(1) is
          // /\d+/ in the regex itself.
          long txid = Long.valueOf(matcher.group(1));
          if (txid < minTxIdToKeep) {
            LOG.info("Purging no-longer needed file " + txid);
            if (!f.delete()) {
              LOG.warn("Unable to delete no-longer-needed data " +
                  f);
            }
            break;
          }
        }
      }
    }
  }

  /*
   * Allow us to backup the storage directory before formatting.
   */
  void backupDirs() throws IOException {
    File rootDir = sd.getRootDir();
    FileSystem localFs = FileSystem.getLocal(conf).getRaw();
    NNStorageDirectoryRetentionManager.backupFiles(localFs, rootDir, conf);
  }
  
  void format(NamespaceInfo nsInfo) throws IOException {
    setStorageInfo(nsInfo);
    LOG.info("Formatting journal " + sd + " with nsid: " + getNamespaceID());
    InjectionHandler.processEvent(InjectionEvent.JOURNAL_STORAGE_FORMAT, sd);
    // Unlock the directory before formatting, because we will
    // re-analyze it after format(). The analyzeStorage() call
    // below is reponsible for re-locking it. This is a no-op
    // if the storage is not currently locked.
    unlockAll();
    sd.clearDirectory();
    sd.write();
    if (!isImageDir) {
      // paxos data is present in the edit directory
      if (!getPaxosDir().mkdirs()) {
        throw new IOException("Could not create paxos dir: " + getPaxosDir());
      }
    }
    analyzeStorage();
  }

  void finalizeStorage() throws IOException {
    NNStorage.finalize(sd, this.getLayoutVersion(), this.getCTime());
  }

  void rollback(NamespaceInfo nsInfo) throws IOException {
    setStorageInfo(nsInfo);
    LOG.info("Rolling back journal " + sd + " with nsid: " + getNamespaceID());
    if (!NNStorage.canRollBack(sd, this)) {
      throw new IOException("Cannot rollback journal : " + sd);
    }
    NNStorage.doRollBack(sd, this);
  }

  void doUpgrade(NamespaceInfo nsInfo) throws IOException {
    setStorageInfo(nsInfo);
    LOG.info("Upgrading journal " + sd + " with nsid: " + getNamespaceID());
    Storage.upgradeDirectory(sd);
  }
  
  void recover(StartupOption startOpt) throws IOException {
    LOG.info("Recovering journal " + sd + " with nsid: " + getNamespaceID());

    // Unlock the directory before formatting, because we will
    // re-analyze it after format(). The analyzeStorage() call
    // below is reponsible for re-locking it. This is a no-op
    // if the storage is not currently locked.
    unlockAll();
    try {
      StorageState curState = sd.analyzeStorage(startOpt);
      NNStorage.recoverDirectory(sd, startOpt, curState, false);
    } catch (IOException ioe) {
      sd.unlock();
      throw ioe;
    }
  }

  void completeUpgrade(NamespaceInfo nsInfo) throws IOException {
    setStorageInfo(nsInfo);
    LOG.info("Upgrading journal " + sd + " with nsid: " + getNamespaceID());
    Storage.completeUpgrade(sd);
  }

  void analyzeStorage() throws IOException {
    this.state = sd.analyzeStorage(StartupOption.REGULAR);
    if (state == StorageState.NORMAL) {
      sd.read();
    }
  }

  void checkConsistentNamespace(StorageInfo nsInfo)
      throws IOException {
    if (nsInfo.getNamespaceID() != getNamespaceID()) {
      throw new IOException("Incompatible namespaceID for journal " +
          this.sd + ": NameNode has nsId " + nsInfo.getNamespaceID() +
          " but storage has nsId " + getNamespaceID());
    }
  }

  public void close() throws IOException {
    LOG.info("Closing journal storage for " + sd);
    unlockAll();
  }

  public boolean isFormatted() {
    return state == StorageState.NORMAL;
  }

  @Override
  public boolean isConversionNeeded(StorageDirectory sd) throws IOException {
    return false;
  }

  @Override
  protected void corruptPreUpgradeStorage(File rootDir) throws IOException {
    
  }
  
  StorageDirectory getStorageDirectory() {
    return sd;
  }
  
  StorageState getStorageState() {
    return state;
  }
  
  StorageInfo getStorageInfo() {
    return new StorageInfo(getLayoutVersion(), getNamespaceID(), getCTime());
  }
  
  static void throwIOException(String msg) throws IOException {
    LOG.error(msg);
    throw new IOException(msg);
  }
}
