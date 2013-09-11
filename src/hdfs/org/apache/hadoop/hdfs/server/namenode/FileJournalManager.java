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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Comparator;
import java.util.Collections;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hdfs.server.common.HdfsConstants;
import org.apache.hadoop.hdfs.server.common.HdfsConstants.StartupOption;
import org.apache.hadoop.hdfs.server.common.HdfsConstants.Transition;
import org.apache.hadoop.hdfs.server.common.StorageErrorReporter;
import org.apache.hadoop.hdfs.server.common.Storage.StorageDirectory;
import org.apache.hadoop.hdfs.server.common.StorageInfo;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogLoader.EditLogValidation;
import org.apache.hadoop.hdfs.server.namenode.NNStorage.NameNodeFile;
import org.apache.hadoop.hdfs.server.namenode.NNStorageRetentionManager.StoragePurger;
import org.apache.hadoop.hdfs.server.namenode.metrics.NameNodeMetrics;
import org.apache.hadoop.hdfs.server.protocol.RemoteEditLog;
import org.apache.hadoop.hdfs.server.protocol.RemoteEditLogManifest;

import com.google.common.base.Joiner;
import com.google.common.collect.ComparisonChain;
import com.google.common.collect.Lists;

/**
 * Journal manager for the common case of edits files being written
 * to a storage directory.
 * 
 * Note: this class is not thread-safe and should be externally
 * synchronized.
 */
public class FileJournalManager implements JournalManager {
  private static final Log LOG = LogFactory.getLog(FileJournalManager.class);

  private final StorageDirectory sd;
  private final NameNodeMetrics metrics;
  private final StorageErrorReporter errorReporter;

  public static final Pattern EDITS_REGEX = Pattern.compile(
    NameNodeFile.EDITS.getName() + "_(\\d+)-(\\d+)");
  public static final Pattern EDITS_INPROGRESS_REGEX = Pattern.compile(
    NameNodeFile.EDITS_INPROGRESS.getName() + "_(\\d+)");

  private File currentInProgress = null;
  private long maxSeenTransaction = -1;

  StoragePurger purger
    = new NNStorageRetentionManager.DeletionStoragePurger();

  public FileJournalManager(StorageDirectory sd) {
    this(sd, null, null);
  }
  
  public FileJournalManager(StorageDirectory sd, NameNodeMetrics metrics,
      StorageErrorReporter errorReporter) {
    this.sd = sd;
    this.metrics = metrics;
    this.errorReporter = errorReporter;
  }

  @Override 
  public void close() throws IOException {}

  @Override
  synchronized public EditLogOutputStream startLogSegment(long txid) 
      throws IOException {
    try {
      currentInProgress = NNStorage.getInProgressEditsFile(sd, txid);
      EditLogOutputStream stm = new EditLogFileOutputStream(currentInProgress,
          metrics);
      stm.create();
      return stm;
    } catch (IOException e) {
      LOG.warn("Unable to start log segment " + txid + " at "
          + currentInProgress + ": " + e.getLocalizedMessage());
      reportErrorOnFile(currentInProgress);
      throw e;
    }
  }

  @Override
  synchronized public void finalizeLogSegment(long firstTxId, long lastTxId)
      throws IOException {
    File inprogressFile = NNStorage.getInProgressEditsFile(sd, firstTxId);

    File dstFile = NNStorage.getFinalizedEditsFile(
        sd, firstTxId, lastTxId);
      LOG.info("Finalizing edits file " + inprogressFile + " -> " + dstFile);
    
    if(dstFile.exists()) {
        throw new IllegalStateException("Can't finalize edits file " 
            + inprogressFile + " since finalized file " + "already exists");
    }
    if (!inprogressFile.renameTo(dstFile)) {
      reportErrorOnFile(dstFile);
      throw new IOException("Unable to finalize edits file " + inprogressFile);
    }
    if (inprogressFile.equals(currentInProgress)) {
      currentInProgress = null;
    }
  }
  
  void reportErrorOnFile(File f) {
    if (errorReporter != null) {
      errorReporter.reportErrorOnFile(f);
    }
  }

  public StorageDirectory getStorageDirectory() {
    return sd;
  }

  @Override
  public void purgeLogsOlderThan(long minTxIdToKeep)
      throws IOException {
    File[] files = FileUtil.listFiles(sd.getCurrentDir());
    List<EditLogFile> editLogs = 
      FileJournalManager.matchEditLogs(files);
    for (EditLogFile log : editLogs) {
      if (log.getFirstTxId() < minTxIdToKeep &&
          log.getLastTxId() < minTxIdToKeep) {
        LOG.info("Purging log: " + log);
        purger.purgeLog(log);
      }
    }
  }
  
  @Override
  public void setCommittedTxId(final long txid, final boolean force) {
  }

  /**
   * Find all editlog segments starting at or above the given txid.
   * Include inprogress segments. Notice that the segments do not have to be 
   * contiguous. JournalSet handles the holes between segments.
   * 
   * @param fromTxId the txnid which to start looking
   * @return a list of remote edit logs
   * @throws IOException if edit logs cannot be listed.
   */
  public RemoteEditLogManifest getEditLogManifest(long firstTxId) throws IOException {
    File currentDir = sd.getCurrentDir();
    List<EditLogFile> allLogFiles = matchEditLogs(
        FileUtil.listFiles(currentDir));
    if (LOG.isDebugEnabled()) {
      LOG.debug(allLogFiles);
    }
    
    List<RemoteEditLog> ret = new ArrayList<RemoteEditLog>(
        allLogFiles.size());

    for (EditLogFile elf : allLogFiles) {
      if (elf.isCorrupt()) continue;   
      if (elf.getFirstTxId() >= firstTxId) {
        ret.add(new RemoteEditLog(elf.firstTxId, 
            elf.lastTxId, 
            elf.isInProgress));
      } else if ((firstTxId > elf.getFirstTxId()) &&
                 (firstTxId <= elf.getLastTxId())) {
        throw new IOException("Asked for firstTxId " + firstTxId
            + " which is in the middle of file " + elf.file);
      }
    }
    Collections.sort(ret);
    return new RemoteEditLogManifest(ret);
  }

  static List<EditLogFile> matchEditLogs(File[] filesInStorage) {
    List<EditLogFile> ret = new ArrayList<EditLogFile>();
    for (File f : filesInStorage) {
      String name = f.getName();
      // Check for edits
      Matcher editsMatch = EDITS_REGEX.matcher(name);
      if (editsMatch.matches()) {
        try {
          long startTxId = Long.valueOf(editsMatch.group(1));
          long endTxId = Long.valueOf(editsMatch.group(2));
          ret.add(new EditLogFile(f, startTxId, endTxId));
        } catch (NumberFormatException nfe) {
          LOG.error("Edits file " + f + " has improperly formatted " +
                    "transaction ID");
          // skip
        }          
      }
      
      // Check for in-progress edits
      Matcher inProgressEditsMatch = EDITS_INPROGRESS_REGEX.matcher(name);
      if (inProgressEditsMatch.matches()) {
        try {
          long startTxId = Long.valueOf(inProgressEditsMatch.group(1));
          ret.add(
              new EditLogFile(f, startTxId, HdfsConstants.INVALID_TXID, true));
        } catch (NumberFormatException nfe) {
          LOG.error("In-progress edits file " + f + " has improperly " +
                    "formatted transaction ID");
          // skip
        }
      }
    }
    return ret;
  }
  
  @Override
  synchronized public void selectInputStreams(
      Collection<EditLogInputStream> streams, long fromTxId,
      boolean inProgressOk, boolean validateInProgressSegments)
      throws IOException {
    List<EditLogFile> elfs = matchEditLogs(FileUtil.listFiles(sd.getCurrentDir()));
    if (LOG.isDebugEnabled()) {
      LOG.debug(this + ": selecting input streams starting at " + fromTxId
          + (inProgressOk ? " (inProgress ok) " : " (excluding inProgress) ")
          + "from among " + elfs.size() + " candidate file(s)");
    }
    addStreamsToCollectionFromFiles(elfs, streams, fromTxId, inProgressOk,
        validateInProgressSegments);
  }

  void addStreamsToCollectionFromFiles(Collection<EditLogFile> elfs,
      Collection<EditLogInputStream> streams, long fromTxId,
      boolean inProgressOk, boolean validateInProgressSegments)
      throws IOException {
    for (EditLogFile elf : elfs) {
      if (elf.isInProgress()) {
        if (!inProgressOk) {
          LOG.debug("passing over " + elf + " because it is in progress "
              + "and we are ignoring in-progress logs.");
          continue;
        }
        if (validateInProgressSegments) {
          try {
            elf.validateLog();
          } catch (IOException e) {
            LOG.error("got IOException while trying to validate header of "
                + elf + ".  Skipping.", e);
            continue;
          }
        } else {
          LOG.info("Skipping validation of edit segment: " + elf);
        }
      }
      if (elf.lastTxId != HdfsConstants.INVALID_TXID && elf.lastTxId < fromTxId) {
        LOG.info("passing over " + elf + " because it ends at " + elf.lastTxId
            + ", but we only care about transactions " + "as new as "
            + fromTxId);
        continue;
      }
      EditLogFileInputStream elfis = new EditLogFileInputStream(elf.getFile(),
          elf.getFirstTxId(), elf.getLastTxId(), elf.isInProgress());
      elfis.setJournalManager(this);
      streams.add(elfis);
    }
  }

  @Override
  synchronized public void recoverUnfinalizedSegments() throws IOException {
    File currentDir = sd.getCurrentDir();
    List<EditLogFile> allLogFiles = matchEditLogs(currentDir.listFiles());
    for(EditLogFile elf : allLogFiles) {
      LOG.info("Found edit file: " + elf);
    }   
    // make sure journal is aware of max seen transaction before moving corrupt 
    // files aside
    findMaxTransaction();

    for (EditLogFile elf : allLogFiles) {
      if (elf.getFile().equals(currentInProgress)) {
        continue;
      }
      if (elf.isInProgress()) {
        // If the file is zero-length, we likely just crashed after opening the
        // file, but before writing anything to it. Safe to delete it.
        if (elf.getFile().length() == 0) {
          LOG.info("Deleting zero-length edit log file " + elf);
          if (!elf.getFile().delete()) {
            throw new IOException("Unable to delete file " + elf.getFile());
          }
          continue;
        }
        
        elf.validateLog();
        if (elf.isCorrupt()) {
          elf.moveAsideCorruptFile();
          continue;
        }
        finalizeLogSegment(elf.getFirstTxId(), elf.getLastTxId());
      }
    }
  }
  
  public List<EditLogFile> getLogFiles(long fromTxId) throws IOException {
    return getLogFiles(fromTxId, true);
  }

  /**
   * Get all edit log segments
   * 
   * @param fromTxId starting txid
   * @param enforceBoundary should we throw an exception if the requested 
   *        txid is not the starting id of a segment
   */
  public List<EditLogFile> getLogFiles(long fromTxId, boolean enforceBoundary)
      throws IOException {
    File currentDir = sd.getCurrentDir();
    List<EditLogFile> allLogFiles = matchEditLogs(currentDir.listFiles());
    List<EditLogFile> logFiles = new ArrayList<EditLogFile>();
    
    for (EditLogFile elf : allLogFiles) {
      if (enforceBoundary && fromTxId > elf.getFirstTxId()
          && fromTxId <= elf.getLastTxId()) {
        throw new IOException("Asked for fromTxId " + fromTxId
            + " which is in middle of file " + elf.file);
      }
      if (fromTxId <= elf.getFirstTxId()) {
        logFiles.add(elf);
      }
    }
    
    Collections.sort(logFiles, EditLogFile.COMPARE_BY_START_TXID);

    return logFiles;
  }

  /** 
   * Find the maximum transaction in the journal.
   * This gets stored in a member variable, as corrupt edit logs
   * will be moved aside, but we still need to remember their first
   * tranaction id in the case that it was the maximum transaction in
   * the journal.
   */
  private long findMaxTransaction()
      throws IOException {
    for (EditLogFile elf : getLogFiles(0)) {
      if (elf.isInProgress()) {
        maxSeenTransaction = Math.max(elf.getFirstTxId(), maxSeenTransaction);
      }
      maxSeenTransaction = Math.max(elf.getLastTxId(), maxSeenTransaction);
    }
    return maxSeenTransaction;
  }

  @Override
  public String toString() {
    return String.format("file:/%s", sd.getRoot());
  }

  /**
   * Record of an edit log that has been located and had its filename parsed.
   */
  public static class EditLogFile {
    private File file;
    private final long firstTxId;
    private long lastTxId;

    private boolean isCorrupt = false;
    private final boolean isInProgress;

    final static Comparator<EditLogFile> COMPARE_BY_START_TXID 
      = new Comparator<EditLogFile>() {
      public int compare(EditLogFile a, EditLogFile b) {
        return ComparisonChain.start()
        .compare(a.getFirstTxId(), b.getFirstTxId())
        .compare(a.getLastTxId(), b.getLastTxId())
        .result();
      }
    };

    EditLogFile(File file,
        long firstTxId, long lastTxId) {
      this(file, firstTxId, lastTxId, false);
      assert (lastTxId != HdfsConstants.INVALID_TXID)
        && (lastTxId >= firstTxId);
    }
    
    EditLogFile(File file, long firstTxId, 
                long lastTxId, boolean isInProgress) {
      boolean checkTxIds = true;
      checkTxIds &= ((lastTxId == HdfsConstants.INVALID_TXID && isInProgress)
        || (lastTxId != HdfsConstants.INVALID_TXID && lastTxId >= firstTxId));
      checkTxIds &= ((firstTxId > -1) || (firstTxId == HdfsConstants.INVALID_TXID));
      if (!checkTxIds)
        throw new IllegalArgumentException("Illegal transaction ids: "
            + firstTxId + ", " + lastTxId + " in progress: " + isInProgress);
      if(file == null)
        throw new IllegalArgumentException("File can not be NULL");
      
      this.firstTxId = firstTxId;
      this.lastTxId = lastTxId;
      this.file = file;
      this.isInProgress = isInProgress;
    }
    
    public long getFirstTxId() {
      return firstTxId;
    }
    
    public long getLastTxId() {
      return lastTxId;
    }

    /** 
     * Count the number of valid transactions in a log.
     * This will update the lastTxId of the EditLogFile or
     * mark it as corrupt if it is.
     */
    public void validateLog() throws IOException {
      EditLogValidation val = EditLogFileInputStream.validateEditLog(file);
      if (val.getNumTransactions() == 0) {
        markCorrupt();
      } else {
        this.lastTxId = val.getEndTxId();
      }
    }

    public boolean isInProgress() {
      return isInProgress;
    }

    public File getFile() {
      return file;
    }
    
    void markCorrupt() {
      isCorrupt = true;
    }
    
    boolean isCorrupt() {
      return isCorrupt;
    }
    
    public void moveAsideCorruptFile() throws IOException {
      assert isCorrupt;
      renameSelf(".corrupt");
    }

    public void moveAsideEmptyFile() throws IOException {
      assert lastTxId == HdfsConstants.INVALID_TXID;
      renameSelf(".empty");
    }
      
    private void renameSelf(String newSuffix) throws IOException {
      File src = file;
      File dst = new File(src.getParent(), src.getName() + newSuffix);
      boolean success = src.renameTo(dst);
      if (!success) {
        throw new IOException(
          "Couldn't rename log " + src + " to " + dst);
      }
      file = dst;
    }
    
    @Override
    public String toString() {
      return String.format("EditLogFile(file=%s,first=%019d,last=%019d,"
                           +"inProgress=%b,corrupt=%b)", file.toString(),
                           firstTxId, lastTxId, isInProgress(), isCorrupt);
    }
    
    public EditLogFile(String colonSeparated) {
      String[] list = colonSeparated.split(":");
      firstTxId = Long.valueOf(list[0]);
      lastTxId = Long.valueOf(list[1]);
      isInProgress = Boolean.valueOf(list[2]);
      isCorrupt = Boolean.valueOf(list[3]);
    }
    
    public String toColonSeparatedString() {
      Joiner joiner = Joiner.on(":");
      return joiner.join(firstTxId, lastTxId, isInProgress, isCorrupt);
    }
  }
  
  @Override
  public void transitionJournal(StorageInfo si, Transition transition,
      StartupOption startOpt) throws IOException {
    // Transitioning file journals is done by the StorageDirectory
    // format code, since they may share their directory with
    // checkpoints, etc.
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean hasSomeJournalData() {
    // Formatting file journals is done by the StorageDirectory
    // format code, since they may share their directory with
    // checkpoints, etc.
    throw new UnsupportedOperationException();
  }
  
  @Override
  public boolean hasSomeImageData() {
    // Formatting file journals is done by the StorageDirectory
    // format code, since they may share their directory with
    // checkpoints, etc.
    throw new UnsupportedOperationException();
  }
  
  public EditLogFile getLogFile(long startTxId) throws IOException {
    return getLogFile(sd.getCurrentDir(), startTxId);
  }
  
  public static EditLogFile getLogFile(File dir, long startTxId)
      throws IOException {
    List<EditLogFile> files = matchEditLogs(FileUtil.listFiles(dir));
    List<EditLogFile> ret = Lists.newLinkedList();
    for (EditLogFile elf : files) {
      if (elf.getFirstTxId() == startTxId) {
        ret.add(elf);
      }
    }
    
    if (ret.isEmpty()) {
      // no matches
      return null;
    } else if (ret.size() == 1) {
      return ret.get(0);
    } else {
      throw new IllegalStateException("More than one log segment in " + 
          dir + " starting at txid " + startTxId + ": " +
          Joiner.on(", ").join(ret));
    }
  }
  
  @Override
  public String toHTMLString() {
    return this.toString();
  }

  @Override
  public boolean hasImageStorage() {
    // FileJournalManager is not used directly to handle images.
    // We use NNStorage instead
    return false;
  }

  @Override
  public RemoteStorageState analyzeJournalStorage() {
    // this is done directly through storage directory
    throw new UnsupportedOperationException();
  }
}
