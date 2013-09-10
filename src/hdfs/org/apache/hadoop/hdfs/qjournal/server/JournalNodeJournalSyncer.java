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
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.qjournal.protocol.JournalConfigKeys;
import org.apache.hadoop.hdfs.server.common.HdfsConstants;
import org.apache.hadoop.hdfs.server.namenode.FileJournalManager.EditLogFile;
import org.apache.hadoop.hdfs.util.InjectionEvent;
import org.apache.hadoop.util.InjectionHandler;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;

/**
 * This class manages journal segment recovery. If a journal is out of sync, and
 * is re-instantiated to the quorum by calling startLogSegment, we want to see
 * if there are some older segments, which are inProgress, and can be recovered.
 * This will be done by talking to other nodes in the JournalNode set and trying
 * to download missing segments.
 */
public class JournalNodeJournalSyncer implements Runnable {

  private static final Log LOG = LogFactory
      .getLog(JournalNodeJournalSyncer.class);
  private static final String logMsg = "Journal Recovery: ";

  // all journal nodes in this set
  private final List<InetSocketAddress> journalNodes;
  // this journal node
  private final InetSocketAddress journalNode;

  // tasks populated when startLogSegment is called
  private final BlockingQueue<SyncTask> taskQueue; // queued calls

  private volatile boolean running = true;

  public static final ObjectMapper mapper = new ObjectMapper();
  
  private int httpConnectReadTimeoutMs = 0;

  JournalNodeJournalSyncer(List<InetSocketAddress> journalNodes,
      InetSocketAddress journalNode, Configuration conf) {
    this.journalNodes = journalNodes;
    this.journalNode = journalNode;
    this.taskQueue = new ArrayBlockingQueue<SyncTask>(10);
    
    // timeout for getting manifest and reading log segments
    this.httpConnectReadTimeoutMs = conf.getInt(
        JournalConfigKeys.DFS_QJOURNAL_HTTP_TIMEOUT_KEY,
        JournalConfigKeys.DFS_QJOURNAL_HTTP_TIMEOUT_DEFAULT);
  }

  @Override
  public void run() {
    while (running) {
      try {
        SyncTask task = taskQueue.poll(1000, TimeUnit.MILLISECONDS);

        // do work with the task
        recoverSegments(task);
      } catch (Exception e) {
        LOG.info(logMsg + "caugth exception", e);
      }
    }
  }

  boolean prepareRecovery(SyncTask task) throws IOException {
    // nothing to do
    if (task == null) {
      // task is polled with timeout which means it can be null
      return false;
    }

    // get the journal info
    Journal journal = task.journal;

    if (journal.getMinTxid() > task.createTxId) {
      // we have already purged segments newer than the one we try to process
      LOG.info(logMsg + "skipping sync for journal: " + journal.getJournalId()
          + " with txid: " + task.createTxId + " since it is too old.");
      // no work to do
      return false;
    } else if (journal.getMinTxid() == task.createTxId){
      // this is the current segment at startup
      LOG.info(logMsg + "skipping sync for journal: " + journal.getJournalId()
          + " with txid: " + task.createTxId + " since it is the current segment.");
      // no work to do
      return false;
    }

    LOG.info(logMsg + "checking journal segments for journal: "
        + journal.getJournalId());

    // synchronize on journal to avoid any changes in the storage
    synchronized (journal) {

      // current minimum txid above which we need to recover
      final long minTxid = journal.getMinTxid();
      // current segment to which we are writing at the moment -1
      final long maxTxid = journal.getCurrentSegmentTxId() - 1;
      
      // sanity check
      if (maxTxid <= minTxid) {
        LOG.info(logMsg + "skipping sync for journal: " + journal.getJournalId()
            + " with txid: " + task.createTxId + " since minTxId >= maxTxId.");
        // no work to do
        return false;
      }

      // populate the task with this information
      task.setRange(minTxid, maxTxid);

      // get all underlying files
      List<EditLogFile> elfs = journal.getAllLogFiles();

      for (EditLogFile elf : elfs) {
        if (elf.getFirstTxId() > maxTxid) {
          // this is a newer or ongoing segment
          LOG.info(logMsg + "skipping newer/ongoing segment: " + elf);
          continue;
        }
        if (elf.getFirstTxId() < minTxid) {
          // this is already not relevant segment
          // it could have been purged
          LOG.info(logMsg + "skipping old segment: " + elf);
          continue;
        }
        if (!elf.isInProgress()) {
          // this is a finalized segment - no need to recover
          LOG.info(logMsg + "found finalized segment: " + elf);
          task.addValidSegment(elf);
        } else {
          // this is a inprogress segment - we will attempt recovery
          LOG.info(logMsg + "found inprogress segment: " + elf);
          task.addInprogressSegement(elf);
        }
      }
    } // end synchronized

    // is there any work to do?
    return task.hasMissingValidSegments();
  }

  /**
   * Recovers a single segment
   * 
   * @param elf
   *          descriptor of the segment to be recovered
   * @param task
   *          contains journal description
   * @throws IOException
   */
  void recoverSegments(SyncTask task) throws IOException {
    // obtain the list of segments that are valid
    if (!prepareRecovery(task)) {
      return;
    }

    // iterate through all nodes
    for (InetSocketAddress jn : journalNodes) {
      if (isLocalIpAddress(jn.getAddress())
          && jn.getPort() == journalNode.getPort()) {
        // we do not need to talk to ourselves
        continue;
      }

      try {
        // get manifest for log that we care about
        List<EditLogFile> remoteLogFiles = getManifest(jn, task.journal,
            task.recoveryStartTxid);

        // go through all remote segments
        for (EditLogFile relf : remoteLogFiles) {
          recoverSegment(jn, relf, task);
        }

        // if we are done, there is no need to iterate more
        if (!task.hasMissingValidSegments()) {
          LOG.info(logMsg + "recovery finished.");
          break;
        }
      } catch (Exception e) {
        LOG.error(logMsg + "error", e);
        continue;
      } 
    }
  }

  void recoverSegment(InetSocketAddress jn, EditLogFile relf, SyncTask task)
      throws IOException {
    try {
      // we are looking for finalized segments that we do not have
      // we only care about segments within the range

      final long remoteStartTxid = relf.getFirstTxId();
      final long remoteEndTxid = relf.getLastTxId();

      if (remoteStartTxid >= task.recoveryStartTxid
          && remoteStartTxid <= task.recoveryEndTxid
          && remoteEndTxid <= task.recoveryEndTxid && !relf.isInProgress()
          && !task.containsValidSegment(relf)) {

        String name = "[" + remoteStartTxid + " : " + remoteEndTxid + "]";

        LOG.info(logMsg + "attempting recovery for segment " + name
            + " for journal id: " + task.journal.getJournalId());

        // path to download
        String path = GetJournalEditServlet.buildPath(
            task.journal.getJournalId(), remoteStartTxid,
            task.journal.getJournalStorage(), 0);

        // url to download
        URL url = new URL("http", jn.getAddress().getHostAddress(), jn.getPort(),
            path.toString());

        // download temporary file
        File syncedTmpFile = task.journal.getJournalStorage().getSyncLogTemporaryFile(
            relf.getFirstTxId(), relf.getLastTxId(), now());

        // .tmp file will not interfere with storage
        syncedTmpFile = task.journal.syncLog(now(), relf.getFirstTxId(), url,
            name, syncedTmpFile);

        // final destination of the file
        File syncedDestFile = task.journal.getJournalStorage().getSyncLogDestFile(
            relf.getFirstTxId(), relf.getLastTxId());

        if (syncedDestFile == null) {
          throwIOException(logMsg + " Error when recovering log " + relf);
        }

        // synchronized on journal so no files can be changed
        synchronized (task.journal) {
          // move away our local copy of the segment
          EditLogFile localCorruptedFile = null;
          try {
            localCorruptedFile = task.getInprogressSegment(remoteStartTxid);
            if (localCorruptedFile != null) {
              localCorruptedFile.moveAsideCorruptFile();
            }
          } catch (Exception e) {
            LOG.warn(logMsg + "exception when marking segment: "
                + localCorruptedFile + " as corrupt.", e);
          }
          // move tmp file to finalized log segment
          FileUtil.replaceFile(syncedTmpFile, syncedDestFile);
        }

        // add the segment to the list so we do not recover it again
        task.addValidSegment(relf);
        LOG.info(logMsg + "successfully recovered segment " + name
            + " for journal id: " + task.journal.getJournalId());
        InjectionHandler
            .processEvent(InjectionEvent.QJM_JOURNALNODE_RECOVERY_COMPLETED);
      }
    } catch (Exception e) {
      LOG.warn(
          logMsg + "exception when recovering segment: "
              + relf.toColonSeparatedString()
              + " when trying with journal node: " + jn, e);
    }
  }

  /**
   * Fetch manifest from a single given journal node over http.
   */
  private List<EditLogFile> getManifest(InetSocketAddress jn, Journal journal,
      long minTxId) throws IOException {
    String m = DFSUtil.getHTMLContentWithTimeout(
        new URL("http", jn.getAddress().getHostAddress(), jn.getPort(),
            GetJournalManifestServlet.buildPath(journal.getJournalId(),
                minTxId, journal.getJournalStorage())), httpConnectReadTimeoutMs,
        httpConnectReadTimeoutMs);
    return convertJsonToListManifest(m);
  }

  /**
   * Get the map corresponding to the JSON string.
   */
  public static List<EditLogFile> convertJsonToListManifest(String json)
      throws IOException {
    if (json == null || json.isEmpty()) {
      return new ArrayList<EditLogFile>();
    }
    // get the list of strings from the http response
    TypeReference<List<String>> type = new TypeReference<List<String>>() {
    };
    List<String> logFilesDesc = mapper.readValue(json, type);

    // we need to convert the list of strings into edit log files
    List<EditLogFile> logFiles = new ArrayList<EditLogFile>();
    for (String lf : logFilesDesc) {
      logFiles.add(new EditLogFile(lf));
    }
    return logFiles;
  }

  /**
   * Add sync task for the given journal. Done when starting a new segment.
   */
  public void addSyncTask(Journal journal, long createTxId) {
    taskQueue.add(new SyncTask(journal, createTxId));
  }

  /**
   * Stop this service.
   */
  public void stop() {
    running = false;
  }

  /**
   * When startLogSegment is called we create a task for the given journal.
   * ValidSegmentTxid is the current segment, so we are interested in recovering
   * older segments.
   */
  private static class SyncTask {
    final Journal journal;
    // txid when the task was create
    final long createTxId;

    // all valid segments for the range we are interested in
    final List<EditLogFile> validSegments = new ArrayList<EditLogFile>();
    // all in-progress segments within the range we are interested in
    final List<EditLogFile> inprogressSegments = new ArrayList<EditLogFile>();

    // we are interested in this range inclusively
    long recoveryStartTxid = HdfsConstants.INVALID_TXID;
    long recoveryEndTxid = HdfsConstants.INVALID_TXID;

    SyncTask(Journal journal, long createTxId) {
      this.journal = journal;
      this.createTxId = createTxId;
    }

    void checkSegment(boolean inProgress, EditLogFile elf) {
      if (inProgress ^ elf.isInProgress()) {
        throw new IllegalArgumentException("Edit log file: "
            + elf.toColonSeparatedString() + " is " + (inProgress ? "not" : "")
            + " in progress");
      }
    }

    /**
     * Add a valid segment that we have locally.
     */
    void addValidSegment(EditLogFile elf) {
      checkSegment(false, elf);
      validSegments.add(elf);
    }

    /**
     * Check if we have a finalized segment, so we do not have tyo recover it.
     */
    boolean containsValidSegment(EditLogFile elf) {
      if (elf.isInProgress()) {
        // we have only finalized segments
        return false;
      }
      for (EditLogFile e : validSegments) {
        if (elf.getFirstTxId() == e.getFirstTxId()
            && elf.getLastTxId() == e.getLastTxId()) {
          return true;
        }
      }
      return false;
    }

    /**
     * Add inprogress segment that we have locally. This is a corrupted segment.
     * 
     * @param elf
     */
    void addInprogressSegement(EditLogFile elf) {
      checkSegment(true, elf);
      inprogressSegments.add(elf);
    }

    /**
     * Get a local inprogress segment.
     */
    EditLogFile getInprogressSegment(long startTxId) {
      for (EditLogFile e : inprogressSegments) {
        if (e.getFirstTxId() == startTxId) {
          return e;
        }
      }
      return null;
    }

    void setRange(long recoveryStartTxid, long recoveryEndTxid)
        throws IOException {
      // transaction ids must be valid
      if (recoveryStartTxid >= recoveryEndTxid
          || recoveryStartTxid <= HdfsConstants.INVALID_TXID
          || recoveryEndTxid <= HdfsConstants.INVALID_TXID) {
        throwIOException(logMsg + "Illegal start/end transactions: "
            + recoveryStartTxid + " : " + recoveryEndTxid);
      }
      this.recoveryStartTxid = recoveryStartTxid;
      this.recoveryEndTxid = recoveryEndTxid;
    }

    /**
     * Check if the recovery is needed. If we have all segments within this
     * range, then there is no need to do anything.
     */
    boolean hasMissingValidSegments() throws IOException {
      // rangeSet() has not been called
      if (recoveryStartTxid == HdfsConstants.INVALID_TXID
          || recoveryEndTxid == HdfsConstants.INVALID_TXID) {
        throwIOException(logMsg + " task range is not set!");
      }
      // if there is no valid segments, there is something missing
      if (validSegments.isEmpty()) {
        return true;
      }
      // check first and last transaction of the range
      if (recoveryStartTxid != validSegments.get(0).getFirstTxId()
          || recoveryEndTxid != validSegments.get(validSegments.size() - 1)
              .getLastTxId()) {
        return true;
      }
      // check if the valid segments are contiguous
      for (int i = 0; i < validSegments.size() - 1; i++) {
        if (validSegments.get(i).getLastTxId() != validSegments.get(i + 1)
            .getFirstTxId() - 1) {
          return true;
        }
      }
      // valid segments cover the entire range of transactions
      return false;
    }
  }

  /**
   * Checks if the address is local.
   */
  private boolean isLocalIpAddress(InetAddress addr) {
    if (addr.isAnyLocalAddress() || addr.isLoopbackAddress())
      return true;
    try {
      return NetworkInterface.getByInetAddress(addr) != null;
    } catch (SocketException e) {
      return false;
    }
  }

  static long now() {
    return System.currentTimeMillis();
  }

  static void throwIOException(String msg) throws IOException {
    LOG.error(msg);
    throw new IOException(msg);
  }
}
