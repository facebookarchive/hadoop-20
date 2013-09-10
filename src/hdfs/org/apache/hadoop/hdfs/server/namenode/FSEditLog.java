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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.zip.Checksum;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.qjournal.client.QuorumJournalManager;
import org.apache.hadoop.hdfs.server.common.HdfsConstants;
import org.apache.hadoop.hdfs.server.common.HdfsConstants.StartupOption;
import org.apache.hadoop.hdfs.server.common.HdfsConstants.Transition;
import org.apache.hadoop.hdfs.server.common.Storage.FormatConfirmable;
import org.apache.hadoop.hdfs.server.common.Storage.StorageDirectory;
import org.apache.hadoop.hdfs.server.common.StorageInfo;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp.*;
import org.apache.hadoop.hdfs.server.namenode.JournalSet.JournalAndStream;
import org.apache.hadoop.hdfs.server.namenode.NNStorage.StorageLocationType;
import org.apache.hadoop.hdfs.server.namenode.ValidateNamespaceDirPolicy.NNStorageLocation;
import org.apache.hadoop.hdfs.server.namenode.metrics.NameNodeMetrics;
import org.apache.hadoop.hdfs.server.protocol.NamespaceInfo;
import org.apache.hadoop.hdfs.server.protocol.RemoteEditLogManifest;
import org.apache.hadoop.hdfs.util.InjectionEvent;
import org.apache.hadoop.io.*;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.util.InjectionHandler;
import org.apache.hadoop.util.PureJavaCrc32;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.permission.*;

import com.google.common.collect.Lists;

/**
 * FSEditLog maintains a log of the namespace modifications.
 * 
 */
public class FSEditLog {
  
  static final Log LOG = LogFactory.getLog(FSEditLog.class);

  public static final long PURGE_ALL_TXID = Long.MAX_VALUE;
  public static String CONF_ROLL_TIMEOUT_MSEC = "dfs.fsedits.timeout.roll.edits.msec";
  
  public static int sizeFlushBuffer = HdfsConstants.DEFAULT_EDIT_BUFFER_SIZE;
  static long preallocateSize= HdfsConstants.DEFAULT_EDIT_PREALLOCATE_SIZE;
  static long maxBufferedTransactions= HdfsConstants.DEFAULT_MAX_BUFFERED_TRANSACTIONS;
  private final ConcurrentSkipListMap<Long, List<Long>> delayedSyncs = 
    new ConcurrentSkipListMap<Long, List<Long>>();
  private Thread syncThread;
  private SyncThread syncer;
  
  /**
   * State machine for edit log. The log starts in UNITIALIZED state upon
   * construction. Once it's initialized, it is usually in IN_SEGMENT state,
   * indicating that edits may be written. In the middle of a roll, or while
   * saving the namespace, it briefly enters the BETWEEN_LOG_SEGMENTS state,
   * indicating that the previous segment has been closed, but the new one has
   * not yet been opened.
   */
  protected enum State {
    UNINITIALIZED, 
    BETWEEN_LOG_SEGMENTS, 
    IN_SEGMENT, 
    CLOSED;
  }

  protected State state = State.UNINITIALIZED;

  // initialize
  private JournalSet journalSet;
  private EditLogOutputStream editLogStream = null;

  // a monotonically increasing counter that represents transactionIds.
  private long txid = -1;

  // stores the last synced transactionId.
  private long synctxid = -1;

  // the first txid of the log that's currently open for writing.
  // If this value is N, we are currently writing to edits_inprogress_N
  private long curSegmentTxId = HdfsConstants.INVALID_TXID;

  // the time of printing the statistics to the log file.
  private long lastPrintTime;

  // is a sync currently running?
  private volatile boolean isSyncRunning;
    
  // Used to exit in the event of a failure to sync to all journals. It's a
  // member variable so it can be swapped out for testing.
  static volatile Runtime runtime = Runtime.getRuntime();

  // these are statistics counters.
  private long numTransactions;        // number of transactions
  private long numTransactionsBatchedInSync;
  private long totalTimeTransactions;  // total time for all transactions
  private NameNodeMetrics metrics;
  
  private NNStorage storage;  
  private Configuration conf;
  private Collection<URI> editsDirs;
  private long timeoutRollEdits;

  private static ThreadLocal<Checksum> localChecksumForRead = new ThreadLocal<Checksum>() {
    protected Checksum initialValue() {
      return new PureJavaCrc32();
    }
  };

  private static ThreadLocal<Checksum> localChecksumForWrite = new ThreadLocal<Checksum>() {
    protected Checksum initialValue() {
      return new PureJavaCrc32();
    }
  };

  /** Get a thread local checksum for read */
  static Checksum getChecksumForRead() {
    return localChecksumForRead.get();
  }
  
  /** Get a thread local checksum for read */
  static Checksum getChecksumForWrite() {
    return localChecksumForWrite.get();
  }

  /**
   * Sets the current transaction id of the edit log. This is used when we load
   * the FSImage and FSEdits and read the last transaction id from disk and then
   * we continue logging transactions to the edit log from that id onwards.
   * 
   * @param txid
   *          the last transaction id
   */
  public void setLastWrittenTxId(long txid) {
    this.txid = txid;
  }
  
  public void resetTxIds(long txid) throws IOException {
    this.txid = txid;
    this.synctxid = txid;
    this.curSegmentTxId = HdfsConstants.INVALID_TXID;
    this.state = State.BETWEEN_LOG_SEGMENTS;
    
    // Journals need to reset their committed IDs.
    journalSet.setCommittedTxId(txid, true);
  }

  private static class TransactionId {
    public long txid;

    TransactionId(long value) {
      this.txid = value;
    }
  }

  // stores the most current transactionId of this thread.
  private static final ThreadLocal<TransactionId> myTransactionId = new ThreadLocal<TransactionId>() {
    protected synchronized TransactionId initialValue() {
      return new TransactionId(-1L);
    }
  };
  
  /**
   * Constructor for FSEditLog. Underlying journals are constructed, but no
   * streams are opened until open() is called.
   * 
   * @param conf The namenode configuration
   * @param storage Storage object used by namenode
   * @param editsDirs List of journals to use
   * @param locationMap contains information about shared/local/remote locations
   */
  FSEditLog(Configuration conf, FSImage image, NNStorage storage,
      Collection<URI> imageDirs, Collection<URI> editsDirs,
      Map<URI, NNStorageLocation> locationMap) {
    init(conf, image, storage, imageDirs, editsDirs, locationMap);
    timeoutRollEdits = conf.getLong(CONF_ROLL_TIMEOUT_MSEC, 0);
  }
  
  private void init(Configuration conf, FSImage image, NNStorage storage,
      Collection<URI> imageDirs, Collection<URI> editsDirs,
      Map<URI, NNStorageLocation> locationMap) {

    isSyncRunning = false;
    this.conf = conf;
    this.storage = storage;
    metrics = NameNode.getNameNodeMetrics();
    lastPrintTime = FSNamesystem.now();

    // If this list is empty, an error will be thrown on first use
    // of the editlog, as no journals will exist
    this.editsDirs = new ArrayList<URI>(editsDirs);

    journalSet = new JournalSet(conf, image, storage, this.editsDirs.size(),
        metrics);
    
    for (URI u : this.editsDirs) {
      boolean required = NNStorageConfiguration.getRequiredNamespaceEditsDirs(
          conf).contains(u);
      boolean shared = false;
      boolean remote = false;
      if (locationMap != null && locationMap.get(u) != null) {
        shared = locationMap.get(u).type == StorageLocationType.SHARED;       
        remote = locationMap.get(u).type == StorageLocationType.REMOTE;
      }
      if (u.getScheme().equals(NNStorage.LOCAL_URI_SCHEME)) {
        StorageDirectory sd = storage.getStorageDirectory(u);
        if (sd != null) {
          LOG.info("Adding local file journal: " + u + ", required: " + required);
          // port error reporter
          journalSet.add(new FileJournalManager(sd, metrics, null), required, shared,
              remote);
        }
      } else if (u.getScheme().equals(QuorumJournalManager.QJM_URI_SCHEME)) {
        // for now, we only allow the QJM to store images
        boolean hasImageStorage = imageDirs.contains(u);
        try {
          journalSet.add(new QuorumJournalManager(conf, u, new NamespaceInfo(
              storage), metrics, hasImageStorage), required, shared, remote);
        } catch (Exception e) {
          throw new IllegalArgumentException("Unable to construct journal, " + u,
              e);
        }     
      } else {
        LOG.info("Adding journal: " + u + ", required: " + required);
        journalSet.add(createJournal(conf, u, new NamespaceInfo(storage),
            metrics), required, shared, remote);
      }
    }
    if (journalSet.isEmpty()) {
      LOG.error("No edits directories configured!");
    }
    state = State.BETWEEN_LOG_SEGMENTS;
  }

  /**
   * Get the list of URIs the editlog is using for storage
   * 
   * @return collection of URIs in use by the edit log
   */
  Collection<URI> getEditURIs() {
    return editsDirs;
  }
  
  /**
   * Create empty edit log files.
   * Initialize the output stream for logging.
   * 
   * @throws IOException
   */
  synchronized void open() throws IOException {
    if (syncer == null) {
      syncer = new SyncThread();
      syncThread = new Thread(syncer);
      syncThread.start();
    }
    if (state != State.BETWEEN_LOG_SEGMENTS)
      throw new IOException("Bad state: " + state);

    startLogSegment(getLastWrittenTxId() + 1, true);
    if (state != State.IN_SEGMENT)
      throw new IOException("Bad state: " + state);
  }

  synchronized boolean isOpen() {
    return state == State.IN_SEGMENT;
  }

  public synchronized void close() throws IOException {
    if (state == State.CLOSED) {
      LOG.info("Closing log when already closed");
      return;
    }
    if (state == State.IN_SEGMENT) {
      assert editLogStream != null;
      waitForSyncToFinish();
      endCurrentLogSegment(true && InjectionHandler
          .trueCondition(InjectionEvent.FSEDIT_LOG_WRITE_END_LOG_SEGMENT));
    }

    if (syncThread != null) {
      syncer.stop();
      syncThread.interrupt();
    }

    try {
      journalSet.close();
    } catch (IOException ioe) {
      LOG.warn("Error closing journalSet", ioe);    
    }
    state = State.CLOSED;
  }
  
  synchronized void transitionNonFileJournals(StorageInfo nsInfo,
      boolean checkEmpty, Transition transition, StartupOption startOpt)
      throws IOException {
    if (Transition.FORMAT == transition
        && state != State.BETWEEN_LOG_SEGMENTS) {
      throw new IOException("Bad state:" + state);
    }
    journalSet.transitionNonFileJournals(nsInfo, checkEmpty,
        transition, startOpt);
  }

  synchronized List<JournalManager> getNonFileJournalManagers() {
    return journalSet.getNonFileJournalManagers();
  }
  
  synchronized List<FormatConfirmable> getFormatConfirmables()
      throws IOException {
    if (state != State.BETWEEN_LOG_SEGMENTS) {
      throw new IOException("Bad state:" + state);
    }

    List<FormatConfirmable> ret = Lists.newArrayList();
    for (final JournalManager jm : journalSet.getJournalManagers()) {
      // The FJMs are confirmed separately since they are also
      // StorageDirectories
      if (!(jm instanceof FileJournalManager)) {
        ret.add(jm);
      }
    }
    return ret;
  }

  void logEdit(final FSEditLogOp op) {
    synchronized (this) {
      assert state != State.CLOSED;
      // this will increase txid
      long start = beginTransaction();
      op.setTransactionId(txid);

      try {
        if (editLogStream != null) {
          // if stream is null it will be handled in sync
          editLogStream.write(op);
        }
      } catch (IOException ex) {
        LOG.fatal("Could not write to required number of streams", ex);
        runtime.exit(1);
      }
      endTransaction(start);
      // check if it is time to schedule an automatic sync
    }
  }

  /**
   * Check if should automatically sync buffered edits to persistent store
   * 
   * @return true if any of the edit stream says that it should sync
   */
  private boolean shouldForceSync() {
    // if editLogStream is null, just fast fail
    return editLogStream == null ? true : editLogStream.shouldForceSync();
  }

  private long beginTransaction() {
    assert Thread.holdsLock(this);
    // get a new transactionId
    txid++;

    //
    // record the transactionId when new data was written to the edits log
    //
    TransactionId id = myTransactionId.get();
    id.txid = txid;
    // obtain time in nanoseconds 
    // endTransaction will compute time in microseconds 
    return System.nanoTime();

  }

  private void endTransaction(long start) {
    assert Thread.holdsLock(this);

    // update statistics
    numTransactions++;
    long txnTime = DFSUtil.getElapsedTimeMicroSeconds(start);
    totalTimeTransactions += txnTime;
    if (metrics != null) { // Metrics is non-null only when used inside name
                           // node
      metrics.transactions.inc(txnTime);
      metrics.numBufferedTransactions.set((int) (txid - synctxid));
      metrics.currentTxnId.set(txid);
    }

  }

  /**
   * Blocks until all ongoing edits have been synced to disk. This differs from
   * logSync in that it waits for edits that have been written by other threads,
   * not just edits from the calling thread.
   * 
   * NOTE: this should be done while holding the FSNamesystem lock, or else more
   * operations can start writing while this is in progress.
   */
  public void logSyncAll() throws IOException {
    // Record the most recent transaction ID as our own id
    synchronized (this) {
      TransactionId id = myTransactionId.get();
      id.txid = txid;
    }
    // Then make sure we're synced up to this point
    logSync();
  }

  /**
   * if there are too many transactions that are yet to be synced,
   * then sync them. Otherwise, the in-memory buffer that keeps
   * the transactions would grow to be very very big. This can happen
   * when there are a large number of listStatus calls which update
   * the access time of files.
   */
  public void logSyncIfNeeded() {
    boolean doSync = false;
    synchronized (this) {
      if (txid > synctxid + maxBufferedTransactions) {
        FSNamesystem.LOG.info("Out of band log sync triggered " +
                              " because there are " +
                              (txid-synctxid) + 
                              " buffered transactions which " +
                              " is more than the configured limit of " +
                              maxBufferedTransactions);
        doSync = true;
      } 
      if (shouldForceSync()) {
        FSNamesystem.LOG.info("Log sync triggered by the output stream");
        doSync = true;
      }
    }
    if (doSync) {
      logSync();
    }
  }
  
  public void logSync() {
    logSync(true);
  }

  /** 
   * Sync all modifications done by this thread.  
   *  
   * The internal concurrency design of this class is as follows: 
   * - Log items are written synchronized into an in-memory buffer, 
   * and each assigned a transaction ID.  
   * - When a thread (client) would like to sync all of its edits, logSync()  
   * uses a ThreadLocal transaction ID to determine what edit number must 
   * be synced to.  
   * - The isSyncRunning volatile boolean tracks whether a sync is currently  
   * under progress.  
   *  
   * The data is double-buffered within each edit log implementation so that  
   * in-memory writing can occur in parallel with the on-disk writing.  
   *  
   * Each sync occurs in three steps: 
   * 1. synchronized, it swaps the double buffer and sets the isSyncRunning 
   * flag.  
   * 2. unsynchronized, it flushes the data to storage  
   * 3. synchronized, it resets the flag and notifies anyone waiting on the 
   * sync.  
   *  
   * The lack of synchronization on step 2 allows other threads to continue 
   * to write into the memory buffer while the sync is in progress. 
   * Because this step is unsynchronized, actions that need to avoid  
   * concurrency with sync() should be synchronized and also call 
   * waitForSyncToFinish() before assuming they are running alone.  
   */ 
  public void logSync(boolean doWait) {

    long syncStart = 0;    
    boolean thisThreadSuccess = false;
    boolean thisThreadSyncing = false;
    EditLogOutputStream logStream = null;
    try {
      synchronized (this) {

        long mytxid = myTransactionId.get().txid;
        myTransactionId.get().txid = -1L;
        if (mytxid == -1) {
          mytxid = txid;
        }

        printStatistics(false);

        // if somebody is already syncing, then wait
        while (mytxid > synctxid && isSyncRunning) {
          if (!doWait) {
            long delayedId = Server.delayResponse();
            List<Long> responses = delayedSyncs.get(mytxid);
            if (responses == null) {
              responses = new LinkedList<Long>();
              delayedSyncs.put(mytxid, responses);
            }
            responses.add(delayedId);
            return;
          }
          try {
            wait(1000);
          } catch (InterruptedException ie) {
          }
        }

        //
        // If this transaction was already flushed, then nothing to do
        //
        if (mytxid <= synctxid) {
          numTransactionsBatchedInSync++;
          if (metrics != null) // Metrics is non-null only when used inside name
            // node
            metrics.transactionsBatchedInSync.inc();
          return;
        }

        // now, this thread will do the sync
        syncStart = txid;
        isSyncRunning = true;
        thisThreadSyncing = true;

        // swap buffers
        try {
          if (journalSet.isEmpty()) {
            throw new IOException(
                "No journals available to flush, journalset is empty");
          }
          if (editLogStream == null) {
            throw new IOException(
                "No journals available to flush, editlogstream is null");
          }
          editLogStream.setReadyToFlush();          
        } catch (IOException e) {
          LOG.fatal("Could not sync enough journals to persistent storage. "
              + "Unsynced transactions: " + (txid - synctxid), new Exception(e));
          runtime.exit(1);
        }
        // editLogStream may become null,
        // so store a local variable for flush.
        logStream = editLogStream;
      }

      // do the sync
      sync(logStream, syncStart);
      thisThreadSuccess = true;
    } finally {
      synchronized (this) {
        if (thisThreadSyncing) {
          if(thisThreadSuccess) {
            // only set this if the sync succeeded
            synctxid = syncStart;
          }
          // if this thread was syncing, clear isSyncRunning
          isSyncRunning = false;
        }
        this.notifyAll();
      }
    }
    endDelay(syncStart);
  }
  
  private void sync(EditLogOutputStream logStream, long syncStart) {
    // do the sync
    long start = System.nanoTime();
    try {
      if (logStream != null) {
        logStream.flush();
      }
    } catch (IOException ex) {
      synchronized (this) {
        LOG.fatal("Could not sync enough journals to persistent storage. "
            + "Unsynced transactions: " + (txid - synctxid), new Exception());
        runtime.exit(1);
      }
    }
    long elapsed = DFSUtil.getElapsedTimeMicroSeconds(start);
    if (metrics != null) // Metrics is non-null only when used inside name node
      metrics.syncs.inc(elapsed);
  }
  
  private void endDelay(long synced) {
    ConcurrentNavigableMap<Long, List<Long>> syncs = delayedSyncs.headMap(
        synced, true);
    for (Iterator<List<Long>> iter = syncs.values().iterator(); iter.hasNext();) {
      List<Long> responses = iter.next();
      for (Long responseId : responses) {
        try {
          Server.sendDelayedResponse(responseId);
        } catch (IOException ex) {
        }
      }
      iter.remove();
    }
    
  }
  
  private class SyncThread implements Runnable {

    private volatile boolean isRunning = true;

    public void stop() {
      isRunning = false;
    }

    @Override
    public void run() {
      try {
        long syncStart = 0;
        while (isRunning) {
          synchronized (FSEditLog.this) {
            while (isSyncRunning || (isRunning && delayedSyncs.size() == 0)) {
              try {
                FSEditLog.this.wait();
              } catch (InterruptedException iex) {
              }
            }
            if (!isRunning) {
              // Shutting down the edits log
              return;
            }

            // There are delayed transactions waiting to be synced and
            // nobody to sync them
            syncStart = txid;
            isSyncRunning = true;

            
            try {
              if (journalSet.isEmpty()) {
                throw new IOException(
                    "No journals available to flush, journalset is empty");
              }
              if (editLogStream == null) {
                throw new IOException(
                    "No journals available to flush, editlogstream is null");
              }
              editLogStream.flush();
            } catch (IOException ex) {
              synchronized (this) {
                LOG.fatal(
                    "Could not sync enough journals to persistent storage. "
                        + "Unsynced transactions: " + (txid - synctxid),
                    new Exception());
                runtime.exit(1);
              }
            }
          }

          sync(editLogStream, syncStart);
          synchronized (FSEditLog.this) {
            synctxid = syncStart;
            isSyncRunning = false;
            FSEditLog.this.notifyAll();
          }
          endDelay(syncStart);
        }
      } catch (Throwable t) {
        FSNamesystem.LOG.fatal("SyncThread received Runtime exception: ", t);
        Runtime.getRuntime().exit(-1);
      }
    }
    
    public String toString() {
      return "SyncThread";
    }
  }
  
  protected int  checkJournals() throws IOException {
    return journalSet.checkJournals("");
  }
  
  protected void updateNamespaceInfo(StorageInfo si) throws IOException {
    journalSet.updateNamespaceInfo(si);
  }

  //
  // print statistics every 1 minute.
  //
  private void printStatistics(boolean force) {
    long now = FSNamesystem.now();
    if (lastPrintTime + 60000 > now && !force) {
      return;
    }
    lastPrintTime = now;
    StringBuilder buf = new StringBuilder();
    buf.append("Number of transactions: ");
    buf.append(numTransactions);
    buf.append(" Number of transactions batched in Syncs: ");
    buf.append(numTransactionsBatchedInSync);
    buf.append(" Number of syncs: ");
    buf.append(editLogStream != null ? editLogStream.getNumSync() : "null");
    buf.append(" Total time for writing transactions (us): ");
    buf.append(totalTimeTransactions);
    buf.append(" Journal sync times (us): ");
    buf.append(journalSet.getSyncTimes());
    
    FSNamesystem.LOG.info(buf);
  }

  /** 
   * Add open lease record to edit log. 
   * Records the block locations of the last block.
   */
  public void logOpenFile(String path, INodeFileUnderConstruction newNode)  
                   throws IOException {
    AddOp op = AddOp.getInstance();  
    op.set(newNode.getId(),
        path,
        newNode.getReplication(), 
        newNode.getModificationTime(),
        newNode.getAccessTime(),
        newNode.getPreferredBlockSize(),
        newNode.getBlocks(),
        newNode.getPermissionStatus(),
        newNode.getClientName(),
        newNode.getClientMachine());   
    logEdit(op);
  }

  /** 
   * Add close lease record to edit log.
   */
  public void logCloseFile(String path, INodeFile newNode) {
    CloseOp op = CloseOp.getInstance();
    op.set(newNode.getId(),
        path, 
        newNode.getReplication(), 
        newNode.getModificationTime(),
        newNode.getAccessTime(),
        newNode.getPreferredBlockSize(),
        newNode.getBlocks(),
        newNode.getPermissionStatus(),
        null,
        null);
    logEdit(op);
  }
  
  /** 
   * Add append file record to the edit log.
   */
  public void logAppendFile(String path, INodeFileUnderConstruction newNode)  
                   throws IOException {
    AppendOp op = AppendOp.getInstance();  
    op.set(path, 
        newNode.getBlocks(),
        newNode.getClientName(),
        newNode.getClientMachine());   
    logEdit(op);
  }
  
  /** 
   * Add create directory record to edit log
   */
  public void logMkDir(String path, INode newNode) {
    MkdirOp op = MkdirOp.getInstance();
    op.set(newNode.getId(), path, newNode.getModificationTime(), 
        newNode.getPermissionStatus());
    logEdit(op);
  }
  
  /** 
   * Add hardlink record to edit log
   */
  public void logHardLink(String src, String dst, long timestamp) {
    HardLinkOp op = HardLinkOp.getInstance();
    op.set(src, dst, timestamp);
    logEdit(op);
  }
  
  /** 
   * Add rename record to edit log
   */
  public void logRename(String src, String dst, long timestamp) {
    RenameOp op = RenameOp.getInstance();
    op.set(src, dst, timestamp);
    logEdit(op);
  }
  
  /**
   * Add raidFile record to edit log
   */
  public void logRaidFile(String src, String codecId, short expectedSourceRepl,
      long timestamp) {
    //TODO
  }
  
  /** 
   * Add set replication record to edit log
   */
  public void logSetReplication(String src, short replication) {
    SetReplicationOp op = SetReplicationOp.getInstance();
    op.set(src, replication);
    logEdit(op);
  }
  
  /** Add set namespace quota record to edit log
   * 
   * @param src the string representation of the path to a directory
   * @param quota the directory size limit
   */
  public void logSetQuota(String src, long nsQuota, long dsQuota) {
    SetQuotaOp op = SetQuotaOp.getInstance();
    op.set(src, nsQuota, dsQuota);
    logEdit(op);
  }

  /**  Add set permissions record to edit log */
  public void logSetPermissions(String src, FsPermission permissions) {
    SetPermissionsOp op = SetPermissionsOp.getInstance();
    op.set(src, permissions);
    logEdit(op);
  }

  /**  Add set owner record to edit log */
  public void logSetOwner(String src, String username, String groupname) {
    SetOwnerOp op = SetOwnerOp.getInstance();
    op.set(src, username, groupname);
    logEdit(op);
  }

  /**
   * concat(trg,src..) log
   */
  public void logConcat(String trg, String [] srcs, long timestamp) {
    ConcatDeleteOp op = ConcatDeleteOp.getInstance();
    op.set(trg, srcs, timestamp);
    logEdit(op);
  }
  
  /**
   * Merge(parity, source, ...) log
   * It's used for converting old raided files into new format
   * by merging parity file and source file together into one file
   */
  public void logMerge(String parity, String source, String codecId, 
      int[] checksums, long timestamp) {
    MergeOp op = MergeOp.getInstance();
    op.set(parity, source, codecId, checksums, timestamp);
    logEdit(op);
  }
  
  /** 
   * Add delete file record to edit log
   */
  public void logDelete(String src, long timestamp) {
    DeleteOp op = DeleteOp.getInstance();
    op.set(src, timestamp);
    logEdit(op);
  }

  /** 
   * Add generation stamp record to edit log
   */
  public void logGenerationStamp(long genstamp) {
    SetGenstampOp op = SetGenstampOp.getInstance();
    op.set(genstamp);
    logEdit(op);
  }

  /** 
   * Add access time record to edit log
   */
  public void logTimes(String src, long mtime, long atime) {
    TimesOp op = TimesOp.getInstance();
    op.set(src, mtime, atime);
    logEdit(op);
  }

  /**
   * Get all journal streams
   */
  public List<JournalAndStream> getJournals() {
    return journalSet.getAllJournalStreams();
  }

  /**
   * Used only by unit tests.
   */
  public static synchronized void setRuntimeForTesting(Runtime rt) {
    runtime = rt;
  }
  
  /**
   * Return a manifest of what finalized edit logs are available
   */
  public synchronized RemoteEditLogManifest getEditLogManifest(long fromTxId)
      throws IOException {
    return journalSet.getEditLogManifest(fromTxId);
  }
  
  /** 
   * Finalizes the current edit log and opens a new log segment.  
   * @return the transaction id of the BEGIN_LOG_SEGMENT transaction  
   * in the new log.  
   */ 
  synchronized long rollEditLog() throws IOException {
    LOG.info("Rolling edit logs.");
    long start = System.nanoTime();
    
    endCurrentLogSegment(true);
    long nextTxId = getLastWrittenTxId() + 1;
    startLogSegment(nextTxId, true);

    assert curSegmentTxId == nextTxId;
    long rollTime = DFSUtil.getElapsedTimeMicroSeconds(start);
    if (metrics != null) {
      metrics.rollEditLogTime.inc(rollTime);
      metrics.tsLastEditsRoll.set(System.currentTimeMillis());
    }
    return nextTxId;
  }
  
    /** 
     * Start writing to the log segment with the given txid.  
     * Transitions from BETWEEN_LOG_SEGMENTS state to IN_LOG_SEGMENT state.   
     */ 
  synchronized void startLogSegment(final long segmentTxId,
      boolean writeHeaderTxn) throws IOException {
    LOG.info("Starting log segment at " + segmentTxId);
    if (segmentTxId < 0) {
      throw new IOException("Bad txid: " + segmentTxId);
    }
    if (state != State.BETWEEN_LOG_SEGMENTS) {
      throw new IOException("Bad state: " + state);
    }
    if (segmentTxId <= curSegmentTxId) {
      throw new IOException("Cannot start writing to log segment "
          + segmentTxId + " when previous log segment started at "
          + curSegmentTxId);
    }
    if (segmentTxId != txid + 1) {
      throw new IOException("Cannot start log segment at txid " + segmentTxId
          + " when next expected " + (txid + 1));
    }

    numTransactions = totalTimeTransactions = numTransactionsBatchedInSync = 0;

    // TODO no need to link this back to storage anymore!
    // See HDFS-2174.
    storage.attemptRestoreRemovedStorage();
    try {
      editLogStream = journalSet.startLogSegment(segmentTxId);
    } catch (IOException ex) {
      throw new IOException("Unable to start log segment " + segmentTxId
          + ": no journals successfully started.");

    }
    curSegmentTxId = segmentTxId;
    state = State.IN_SEGMENT;
    if (writeHeaderTxn) {
      logEdit(LogSegmentOp.getInstance(FSEditLogOpCodes.OP_START_LOG_SEGMENT));
      logSync();
    }
    
    // force update of journal and image metrics
    journalSet.updateJournalMetrics();
    // If it is configured, we want to schedule an automatic edits roll
    if (timeoutRollEdits > 0) {
      FSNamesystem fsn = this.journalSet.getImage().getFSNamesystem();
      if (fsn != null) {
        // In some test cases fsn is NULL in images. Simply skip the feature.
        AutomaticEditsRoller aer = fsn.automaticEditsRoller;
        if (aer != null) {
          aer.setNextRollTime(System.currentTimeMillis() + timeoutRollEdits);
        } else {
          LOG.warn("Automatic edits roll is enabled but the roller thread "
              + "is not enabled. Should only happen in unit tests.");
        }
      } else {
        LOG.warn("FSNamesystem is NULL in FSEditLog.");
      }
    }
  }
      
  /**
   * Finalize the current log segment. Transitions from IN_SEGMENT state to
   * BETWEEN_LOG_SEGMENTS state.
   */
  synchronized void endCurrentLogSegment(boolean writeEndTxn)
      throws IOException {
    LOG.info("Ending log segment " + curSegmentTxId);
    if (state != State.IN_SEGMENT) {
      throw new IllegalStateException("Bad state: " + state);
    }
    waitForSyncToFinish();
    if (writeEndTxn) {
      logEdit(LogSegmentOp.getInstance(FSEditLogOpCodes.OP_END_LOG_SEGMENT));
    }
    logSyncAll();
    printStatistics(true);
    final long lastTxId = getLastWrittenTxId();

    try {
      journalSet.finalizeLogSegment(curSegmentTxId, lastTxId);
      editLogStream = null;
    } catch (IOException e) {
      // All journals have failed, it will be handled in logSync.
      FSNamesystem.LOG.info("Cannot finalize log segment: " + e.toString());
    }
    state = State.BETWEEN_LOG_SEGMENTS;
  }
    
  /**
   * Archive any log files that are older than the given txid.
   */
  public void purgeLogsOlderThan(final long minTxIdToKeep) {
    synchronized (this) {
      // synchronized to prevent findbugs warning about inconsistent
      // synchronization. This will be JIT-ed out if asserts are
      // off.
      assert curSegmentTxId == HdfsConstants.INVALID_TXID || // on format this
                                                             // is no-op
          minTxIdToKeep <= curSegmentTxId : "cannot purge logs older than txid "
          + minTxIdToKeep + " when current segment starts at " + curSegmentTxId;
      try {
        journalSet.purgeLogsOlderThan(minTxIdToKeep);
      } catch (IOException ex) {
        // All journals have failed, it will be handled in logSync.
      }

    }
  }
  
    /**
     * The actual sync activity happens while not synchronized on this object.
     * Thus, synchronized activities that require that they are not concurrent
     * with file operations should wait for any running sync to finish.
     */
  synchronized void waitForSyncToFinish() {
    while (isSyncRunning) {
      try {
        wait(1000);
      } catch (InterruptedException ie) {
      }
    }
  }
  
  /**
   * Return the txid of the last synced transaction. For test use only
   */
  synchronized long getSyncTxId() {
    return synctxid;
  }

  /**
   * Run recovery on all journals to recover any unclosed segments
   */
  void recoverUnclosedStreams() {
    try {
      journalSet.recoverUnfinalizedSegments();
    } catch (IOException ex) {
      // All journals have failed, it is handled in logSync.
    }
  }
  
  /**
   * Select a list of input streams to load.
   * 
   * @param streams, streams to be returned
   * @param fromTxId first transaction in the selected streams
   * @param toAtLeast the selected streams must contain this transaction
   * @param inProgessOk set to true if in-progress streams are OK
   * 
   * @return true if there the redundancy in no met
   */
  public synchronized boolean selectInputStreams(
      Collection<EditLogInputStream> streams, long fromTxId,
      long toAtLeastTxId, int minRedundancy) throws IOException {
    
    // at this point we should not have any non-finalized segments
    // this function is called at startup, and must be invoked after
    // recovering all in progress segments
    if (journalSet.hasUnfinalizedSegments(fromTxId)) {
      LOG.fatal("All streams should be finalized");
      throw new IOException("All streams should be finalized at startup");
    }
    
    // get all finalized streams
    boolean redundancyViolated = journalSet.selectInputStreams(streams,
        fromTxId, false, false, minRedundancy);
    
    try {
      checkForGaps(streams, fromTxId, toAtLeastTxId, true);
    } catch (IOException e) {
      closeAllStreams(streams);
      throw e;
    }
    return redundancyViolated;
  }
  
  /**
   * Check for gaps in the edit log input stream list.
   * Note: we're assuming that the list is sorted and that txid ranges don't
   * overlap.  This could be done better and with more generality with an
   * interval tree.
   */
  private void checkForGaps(Collection<EditLogInputStream> streams, long fromTxId,
      long toAtLeastTxId, boolean inProgressOk) throws IOException {
    Iterator<EditLogInputStream> iter = streams.iterator();
    long txId = fromTxId;
    while (true) {
      if (txId > toAtLeastTxId)
        return;
      if (!iter.hasNext())
        break;
      EditLogInputStream elis = iter.next();
      if (elis.getFirstTxId() > txId) {
        break;
      }
      long next = elis.getLastTxId();
      if (next == HdfsConstants.INVALID_TXID) {
        if (!inProgressOk) {
          throw new RuntimeException("inProgressOk = false, but "
              + "selectInputStreams returned an in-progress edit "
              + "log input stream (" + elis + ")");
        }
        // We don't know where the in-progress stream ends.
        // It could certainly go all the way up to toAtLeastTxId.
        return;
      }
      txId = next + 1;
    }
    throw new IOException(String.format("Gap in transactions. Expected to "
        + "be able to read up until at least txid %d but unable to find any "
        + "edit logs containing txid %d", toAtLeastTxId, txId));
  }

  /**
   * Close all the streams in a collection
   * 
   * @param streams The list of streams to close
   */
  static void closeAllStreams(Iterable<EditLogInputStream> streams) {
    for (EditLogInputStream s : streams) {
      IOUtils.closeStream(s);
    }
  }
  
  /** 
   * Retrieve the implementation class for a Journal scheme.  
   * @param conf The configuration to retrieve the information from 
   * @param uriScheme The uri scheme to look up.  
   * @return the class of the journal implementation  
   * @throws IllegalArgumentException if no class is configured for uri 
   */ 
  static Class<? extends JournalManager> getJournalClass(Configuration conf,
      String uriScheme) {
    String key = "dfs.name.edits.journal-plugin" + "." + uriScheme;
    Class<? extends JournalManager> clazz = null;
    try {
      clazz = conf.getClass(key, null, JournalManager.class);
    } catch (RuntimeException re) {
      throw new IllegalArgumentException("Invalid class specified for "
          + uriScheme, re);
    }

    if (clazz == null) {
      LOG.warn("No class configured for " + uriScheme + ", " + key
          + " is empty");
      throw new IllegalArgumentException("No class configured for " + uriScheme);
    }
    return clazz;
  }
    
  /**
   * Construct a custom journal manager.
   * The class to construct is taken from the configuration.
   * @param uri Uri to construct
   * @return The constructed journal manager
   * @throws IllegalArgumentException if no class is configured for uri
   */
  public static JournalManager createJournal(Configuration conf, URI uri,
      NamespaceInfo nsInfo, NameNodeMetrics metrics) {
    Class<? extends JournalManager> clazz = getJournalClass(conf,
        uri.getScheme());

    try {
      Constructor<? extends JournalManager> cons = clazz.getConstructor(
          Configuration.class, URI.class, NamespaceInfo.class,
          NameNodeMetrics.class);
      return cons.newInstance(conf, uri, nsInfo, metrics);
    } catch (Exception e) {
      throw new IllegalArgumentException("Unable to construct journal, " + uri,
          e);
    }
  }

  // sets the initial capacity of the flush buffer.
  static void setBufferCapacity(int size) {
    sizeFlushBuffer = size;
  }
  //
  // maximum number of transactions to be buffered in memory
  static void setMaxBufferedTransactions(int num) {
    maxBufferedTransactions = num;
  }

  // sets the preallocate trigger of the edits log.
  static void setPreallocateSize(long size) {
    preallocateSize = size;
  }

  /**
   * Return the transaction ID for the transaction that was written last.
   */
  synchronized long getLastWrittenTxId() {
    return txid;
  }
  
  public synchronized long getCurrentTxId() {
    return txid + 1;
  }
  
  synchronized long getLastSyncedTxId() {
    return synctxid;
  }
  
  /**
   * @return the first transaction ID in the current log segment
   */
  public synchronized long getCurSegmentTxId() {
    assert state == State.IN_SEGMENT : "Bad state: " + state;
    return curSegmentTxId;
  }
  
  /**
   * Get number of journals available
   */
  public int getNumberOfAvailableJournals() throws IOException {
    return checkJournals();
  }
  
  /**
   * Get number of journals (enabled and disabled).
   */
  public int getNumberOfJournals() throws IOException {
    return journalSet.getNumberOfJournals();
  }
  
  /**
   * Check if the shared journal is available
   */
  public boolean isSharedJournalAvailable() throws IOException {
    return journalSet.isSharedJournalAvailable();
  }
  
  public void setTimeoutRollEdits(long timeoutRollEdits) {
    this.timeoutRollEdits = timeoutRollEdits;
  }
  
  /**
   * A class to read in blocks stored in the old format. The only two
   * fields in the block were blockid and length.
   */
  static class BlockTwo implements Writable {
    long blkid;
    long len;

    static {                                      // register a ctor
      WritableFactories.setFactory
        (BlockTwo.class,
         new WritableFactory() {
           public Writable newInstance() { return new BlockTwo(); }
         });
    }


    BlockTwo() {
      blkid = 0;
      len = 0;
    }
    /////////////////////////////////////
    // Writable
    /////////////////////////////////////
    public void write(DataOutput out) throws IOException {
      out.writeLong(blkid);
      out.writeLong(len);
    }

    public void readFields(DataInput in) throws IOException {
      this.blkid = in.readLong();
      this.len = in.readLong();
    }
  }
}
