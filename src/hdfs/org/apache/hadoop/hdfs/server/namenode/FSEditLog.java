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
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.zip.Checksum;

import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.server.common.HdfsConstants;
import org.apache.hadoop.hdfs.server.common.Storage.StorageDirectory;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp.*;
import org.apache.hadoop.hdfs.server.namenode.FSImage.NameNodeDirType;
import org.apache.hadoop.hdfs.server.namenode.metrics.NameNodeMetrics;
import org.apache.hadoop.hdfs.util.Holder;
import org.apache.hadoop.io.*;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.util.PureJavaCrc32;
import org.apache.hadoop.fs.ChecksumException;
import org.apache.hadoop.fs.permission.*;

/**
 * FSEditLog maintains a log of the namespace modifications.
 * 
 */
public class FSEditLog {
  static int sizeFlushBuffer = HdfsConstants.DEFAULT_EDIT_BUFFER_SIZE;
  static long preallocateSize= HdfsConstants.DEFAULT_EDIT_PREALLOCATE_SIZE;
  static long maxBufferedTransactions= HdfsConstants.DEFAULT_MAX_BUFFERED_TRANSACTIONS;
  private final ConcurrentSkipListMap<Long, List<Long>> delayedSyncs = 
    new ConcurrentSkipListMap<Long, List<Long>>();
  private Thread syncThread;
  private SyncThread syncer;

  private ArrayList<EditLogOutputStream> editStreams = null;
  private FSImage fsimage = null;

  // a monotonically increasing counter that represents transactionIds.
  private long txid = 0;

  // stores the last synced transactionId.
  private long synctxid = 0;

  // the time of printing the statistics to the log file.
  private long lastPrintTime;

  // is a sync currently running?
  private volatile boolean isSyncRunning;

  // these are statistics counters.
  private long numTransactions;        // number of transactions
  private long numTransactionsBatchedInSync;
  private long totalTimeTransactions;  // total time for all transactions
  private NameNodeMetrics metrics;
  
  private static ThreadLocal<Checksum> localChecksumForRead =
    new ThreadLocal<Checksum>() {
    protected Checksum initialValue() {
        return new PureJavaCrc32();
    }
  };

  private static ThreadLocal<Checksum> localChecksumForWrite =
    new ThreadLocal<Checksum>() {
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
  public void setStartTransactionId(long txid) {
    this.txid = txid;
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



  FSEditLog(FSImage image) {
    fsimage = image;
    isSyncRunning = false;
    metrics = NameNode.getNameNodeMetrics();
    lastPrintTime = FSNamesystem.now();
  }
  
  private File getEditFile(StorageDirectory sd) {
    return fsimage.getEditFile(sd);
  }
  
  private File getEditNewFile(StorageDirectory sd) {
    return fsimage.getEditNewFile(sd);
  }
  
  private int getNumStorageDirs() {
 int numStorageDirs = 0;
 for (Iterator<StorageDirectory> it = 
       fsimage.dirIterator(NameNodeDirType.EDITS); it.hasNext(); it.next())
   numStorageDirs++;
    return numStorageDirs;
  }
  
  synchronized int getNumEditStreams() {
    return editStreams == null ? 0 : editStreams.size();
  }

  boolean isOpen() {
    return getNumEditStreams() > 0;
  }

  /**
   * Create empty edit log files.
   * Initialize the output stream for logging.
   * 
   * @throws IOException
   */
  public synchronized void open() throws IOException {
    if (syncer == null) {
      syncer = new SyncThread();
      syncThread = new Thread(syncer);
      syncThread.start();
    }
    numTransactions = totalTimeTransactions = numTransactionsBatchedInSync = 0;
    if (editStreams == null)
      editStreams = new ArrayList<EditLogOutputStream>();
    for (Iterator<StorageDirectory> it = 
           fsimage.dirIterator(NameNodeDirType.EDITS); it.hasNext();) {
      StorageDirectory sd = it.next();
      File eFile = getEditFile(sd);
      try {
        EditLogOutputStream eStream = new EditLogFileOutputStream(eFile, metrics);
        editStreams.add(eStream);
      } catch (IOException e) {
        FSNamesystem.LOG.warn("Unable to open edit log file " + eFile);
        // Remove the directory from list of storage directories
        fsimage.removedStorageDirs.add(sd);
        it.remove();
      }
    }
  }

  public synchronized void createEditLogFile(File name) throws IOException {
    EditLogOutputStream eStream = new EditLogFileOutputStream(name, metrics);
    eStream.create();
    eStream.close();
  }
  
  public synchronized void close() throws IOException {
    close(false);
  }

  /**
   * Shutdown the file store.
   */
  public synchronized void close(boolean shutdown) throws IOException {
    while (isSyncRunning) {
      try {
        wait(1000);
      } catch (InterruptedException ie) { 
      }
    }
    
    if (shutdown && syncThread != null) {
      syncer.stop();
      syncThread.interrupt();
    }

    if (editStreams == null) {
      return;
    }
    printStatistics(true);
    numTransactions = totalTimeTransactions = numTransactionsBatchedInSync = 0;

    for (int idx = 0; idx < editStreams.size(); idx++) {
      EditLogOutputStream eStream = editStreams.get(idx);
      try {
        eStream.setReadyToFlush();
        eStream.flush();
        eStream.close();
      } catch (IOException e) {
        FSNamesystem.LOG.warn("FSEditLog:close - failed to close stream " 
            + eStream.getName(), e);
        processIOError(idx);
        idx--;
      }
    }
    editStreams.clear();
  }

  /**
   * If there is an IO Error on any log operations, remove that
   * directory from the list of directories.
   * If no more directories remain, then exit.
   */
  synchronized void processIOError(int index) {
    if (editStreams == null || editStreams.size() <= 1) {
      FSNamesystem.LOG.fatal(
      "Fatal Error : All storage directories are inaccessible."); 
      Runtime.getRuntime().exit(-1);
    }
    assert(index < getNumStorageDirs());
    assert(getNumStorageDirs() == editStreams.size());
    
    EditLogFileOutputStream eStream = (EditLogFileOutputStream)editStreams.get(index);
    File parentStorageDir = ((EditLogFileOutputStream)editStreams
                                      .get(index)).getFile()
                                      .getParentFile().getParentFile();
    
    try {
      eStream.close();
    } catch (Exception e) {}
    
    editStreams.remove(index);
    //
    // Invoke the ioerror routine of the fsimage
    //
    fsimage.processIOError(parentStorageDir);
  }
  
  /**
   * If there is an IO Error on any log operations on storage directory,
   * remove any stream associated with that directory 
   */
  synchronized void processIOError(StorageDirectory sd) {
    // Try to remove stream only if one should exist
    if (!sd.getStorageDirType().isOfType(NameNodeDirType.EDITS))
      return;
    if (editStreams == null || editStreams.size() <= 1) {
      FSNamesystem.LOG.fatal(
          "Fatal Error : All storage directories are inaccessible."); 
      Runtime.getRuntime().exit(-1);
    }
    for (int idx = 0; idx < editStreams.size(); idx++) {
      File parentStorageDir = ((EditLogFileOutputStream)editStreams
                                       .get(idx)).getFile()
                                       .getParentFile().getParentFile();
      if (parentStorageDir.getName().equals(sd.getRoot().getName()))
        editStreams.remove(idx);
 }
  }
  
  /**
   * The specified streams have IO errors. Remove them from logging
   * new transactions.
   */
  private void processIOError(ArrayList<EditLogOutputStream> errorStreams) {
    if (errorStreams == null) {
      return;                       // nothing to do
    }
    for (int idx = 0; idx < errorStreams.size(); idx++) {
      EditLogOutputStream eStream = errorStreams.get(idx);
      int j = 0;
      int numEditStreams = editStreams.size();
      for (j = 0; j < numEditStreams; j++) {
        if (editStreams.get(j) == eStream) {
          break;
        }
      }
      if (j == numEditStreams) {
          FSNamesystem.LOG.error("Unable to find sync log on which " +
                                 " IO error occured. " +
                                 "Fatal Error.");
          Runtime.getRuntime().exit(-1);
      }
      processIOError(j);
    }
    fsimage.incrementCheckpointTime();
  }

  /**
   * check if ANY edits.new log exists
   */
  boolean existsNew() throws IOException {
    for (Iterator<StorageDirectory> it = 
           fsimage.dirIterator(NameNodeDirType.EDITS); it.hasNext();) {
      if (getEditNewFile(it.next()).exists()) { 
        return true;
      }
    }
    return false;
  }  

  /**
   * Validate a transaction's checksum
   */
  static void validateChecksum(boolean supportChecksum, 
      DataInputStream rawStream, Checksum checksum, int tid)
  throws IOException {
    if (supportChecksum) {
      int expectedChecksum = rawStream.readInt();  // read in checksum
      int calculatedChecksum = (int)checksum.getValue();
      if (expectedChecksum != calculatedChecksum) {
        throw new ChecksumException(
            "Transaction " + tid + " is corrupt.", tid);
      }
    }
  }
  
  // a place holder for reading a long
  private static final LongWritable longWritable = new LongWritable();

  /**
   * Write an operation to the edit log. Do not sync to persistent
   * store yet.
   */
  synchronized void logEdit(final FSEditLogOp op) {
    op.setTransactionId(txid);
    assert this.getNumEditStreams() > 0 : "no editlog streams";
    long start = FSNamesystem.now();
    for (int idx = 0; idx < editStreams.size(); idx++) {
      EditLogOutputStream eStream = editStreams.get(idx);
      try {
        eStream.write(op);
      } catch (IOException ie) {
        FSImage.LOG.warn("logEdit: removing "+ eStream.getName(), ie);
        processIOError(idx);         
        // processIOError will remove the idx's stream 
        // from the editStreams collection, so we need to update idx
        idx--; 
      }
    }
    // get a new transactionId
    txid++;

    //
    // record the transactionId when new data was written to the edits log
    //
    TransactionId id = myTransactionId.get();
    id.txid = txid;

    // update statistics
    long end = FSNamesystem.now();
    numTransactions++;
    totalTimeTransactions += (end-start);
    if (metrics != null) { // Metrics is non-null only when used inside name node
      metrics.transactions.inc((end-start));
      metrics.numBufferedTransactions.set((int)(txid-synctxid));
    }
  }

  /**
   * Syncs all pending transactions from all threads.
   */
  synchronized void logSyncAll() throws IOException {
    // stores in the Thread local variable of current threads
    TransactionId id = myTransactionId.get();
    id.txid = txid;
    logSync();
  }

  /**
   * if there are too many transactions that are yet to be synced,
   * then sync them. Otherwise, the in-memory buffer that keeps
   * the transactions would grow to be very very big. This can happen
   * when there are a large number of listStatus calls which update
   * the access time of files.
   */
  public void logSyncIfNeeded() throws IOException {
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
    }
    if (doSync) {
      logSync();
    }
  }
  
  public void logSync() throws IOException {
    logSync(true);
  }

  //
  // Sync all modifications done by this thread.
  //
  public void logSync(boolean doWait) throws IOException {
    
    long syncStart = 0;

    final int numEditStreams;
    synchronized (this) {
      // Fetch the transactionId of this thread. 
      long mytxid = myTransactionId.get().txid;
      myTransactionId.get().txid = -1L;
      if (mytxid == -1) {
        mytxid = txid;
      }
      numEditStreams = editStreams.size();
      assert numEditStreams > 0 : "no editlog streams";
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
        } catch (InterruptedException ie) { }
      }

      //
      // If this transaction was already flushed, then nothing to do
      //
      if (mytxid <= synctxid) {
        numTransactionsBatchedInSync++;
        if (metrics != null) // Metrics is non-null only when used inside name node
          metrics.transactionsBatchedInSync.inc();
        return;
      }
   
      // now, this thread will do the sync
      syncStart = txid;
      isSyncRunning = true;   

      // swap buffers
      for (int idx = 0; idx < numEditStreams; idx++) {
        editStreams.get(idx).setReadyToFlush();
      }
    }
    sync(syncStart);

    synchronized (this) {
       synctxid = syncStart;
       isSyncRunning = false;
       this.notifyAll();
    }
    endDelay(syncStart);
  }
  
  private void sync(long syncStart) {
    ArrayList<EditLogOutputStream> errorStreams = null;
    // do the sync
    long start = FSNamesystem.now();
    final int numEditStreams;
    synchronized (this) {
      numEditStreams = editStreams.size();
      assert numEditStreams > 0: "no editlog streams";
    }
    for (int idx = 0; idx < numEditStreams; idx++) {
      EditLogOutputStream eStream = editStreams.get(idx);
      try {
        eStream.flush();
      } catch (IOException ie) {
        //
        // remember the streams that encountered an error.
        //
        if (errorStreams == null) {
          errorStreams = new ArrayList<EditLogOutputStream>(1);
        }
        errorStreams.add(eStream);
        FSNamesystem.LOG.error("Unable to sync edit log. " +
                               "Fatal Error.", ie);
      }
    }
    long elapsed = FSNamesystem.now() - start;
    if (metrics != null) // Metrics is non-null only when used inside name node
      metrics.syncs.inc(elapsed);

    synchronized (this) {
      processIOError(errorStreams);
    }
  }
  
  private void endDelay(long synced) {
    ConcurrentNavigableMap<Long, List<Long>> syncs = delayedSyncs.headMap(synced, true);
    for (Iterator<List<Long>> iter = syncs.values().iterator();
            iter.hasNext();) {
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
      long syncStart = 0;
      int numEditStreams;
      while (isRunning) {
        synchronized (FSEditLog.this) {
          numEditStreams = editStreams.size();
          assert numEditStreams > 0 : "no editlog streams";

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

          for (int idx = 0; idx < numEditStreams; idx++) {
            try {
              editStreams.get(idx).setReadyToFlush();
            } catch (IOException ex) {
              FSNamesystem.LOG.error(ex);
              isSyncRunning = false;
              continue;
            }
          }

        }
        sync(syncStart);
        synchronized (FSEditLog.this) {
          synctxid = syncStart;
          isSyncRunning = false;
          FSEditLog.this.notifyAll();
        }
        endDelay(syncStart);
      }
    }
    
    public String toString() {
      return "SyncThread";
    }
  }

  //
  // print statistics every 1 minute.
  //
  private void printStatistics(boolean force) {
    long now = FSNamesystem.now();
    if (lastPrintTime + 60000 > now && !force) {
      return;
    }
    if (editStreams == null || editStreams.size()==0) {
      return;
    }
    lastPrintTime = now;
    StringBuilder buf = new StringBuilder();
    buf.append("Number of transactions: ");
    buf.append(numTransactions);
    buf.append(" Total time for transactions(ms): ");
    buf.append(totalTimeTransactions);
    buf.append(" Number of transactions batched in Syncs: ");
    buf.append(numTransactionsBatchedInSync);
    buf.append(" Number of syncs: ");
    buf.append(editStreams.get(0).getNumSync());
    buf.append(" SyncTimes(ms): ");

    int numEditStreams = editStreams.size();
    for (int idx = 0; idx < numEditStreams; idx++) {
      EditLogOutputStream eStream = editStreams.get(idx);
      buf.append(" " + eStream.getName() + ":");
      buf.append(eStream.getTotalSyncTime());
      buf.append(" ");
    }
    FSNamesystem.LOG.info(buf);
  }

  /** 
   * Add open lease record to edit log. 
   * Records the block locations of the last block.
   */
  public void logOpenFile(String path, INodeFileUnderConstruction newNode)  
                   throws IOException {
    AddOp op = AddOp.getInstance();  
    op.set(path, 
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
    op.set(path, 
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
   * Add create directory record to edit log
   */
  public void logMkDir(String path, INode newNode) {
    MkdirOp op = MkdirOp.getInstance();
    op.set(path, newNode.getModificationTime(), 
        newNode.getPermissionStatus());
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
   * Return the size of the current EditLog
   */
  synchronized long getEditLogSize() throws IOException {
    assert(getNumStorageDirs() == editStreams.size());
    long size = 0;
    for (int idx = 0; idx < editStreams.size(); idx++) {
      EditLogOutputStream es = editStreams.get(idx);
      try {
        long curSize = es.length();
        assert (size == 0 || size == curSize) : "All streams must be the same";
        size = curSize;
      } catch (IOException e) {
        FSImage.LOG.warn("getEditLogSize: editstream.length failed. removing editlog (" +
            idx + ") " + es.getName(), e);
        processIOError(idx);
      }
    }
    return size;
  }

  /**
   * Closes the current edit log and opens edits.new. 
   * Returns the lastModified time of the edits log.
   */
  synchronized void rollEditLog() throws IOException {
    //
    // If edits.new already exists in some directory, verify it
    // exists in all directories.
    //
    if (existsNew()) {
      for (Iterator<StorageDirectory> it = 
               fsimage.dirIterator(NameNodeDirType.EDITS); it.hasNext();) {
        File editsNew = getEditNewFile(it.next());
     if (!editsNew.exists()) { 
          throw new IOException("Inconsistent existance of edits.new " +
                                editsNew);
        }
      }
      return; // nothing to do, edits.new exists!
    }

    close();                     // close existing edit log

    fsimage.attemptRestoreRemovedStorage();
    
    //
    // Open edits.new
    //
    boolean failedSd = false;
    for (Iterator<StorageDirectory> it = 
           fsimage.dirIterator(NameNodeDirType.EDITS); it.hasNext();) {
      StorageDirectory sd = it.next();
      try {
        EditLogFileOutputStream eStream = 
             new EditLogFileOutputStream(getEditNewFile(sd), metrics);
        eStream.create();
        editStreams.add(eStream);
      } catch (IOException e) {
        failedSd = true;
        // remove stream and this storage directory from list
        FSImage.LOG.warn("rollEdidLog: removing storage " + sd.getRoot().getPath(), e);
        sd.unlock();
        fsimage.removedStorageDirs.add(sd);
        it.remove();
      }
    }
    if(failedSd)
      fsimage.incrementCheckpointTime();  // update time for the valid ones
  }
  
  /**
   * Removes the old edit log and renamed edits.new as edits.
   * Reopens the edits file.
   */
  synchronized void purgeEditLog() throws IOException {
    //
    // If edits.new does not exists, then return error.
    //
    if (!existsNew()) {
      throw new IOException("Attempt to purge edit log " +
                            "but edits.new does not exist.");
    }
    close();

    //
    // Delete edits and rename edits.new to edits.
    //
    for (Iterator<StorageDirectory> it = 
           fsimage.dirIterator(NameNodeDirType.EDITS); it.hasNext();) {
      StorageDirectory sd = it.next();

      if (!getEditNewFile(sd).renameTo(getEditFile(sd))) {
        //
        // renameTo() fails on Windows if the destination
        // file exists.
        //
        getEditFile(sd).delete();
        if (!getEditNewFile(sd).renameTo(getEditFile(sd))) {
          // Should we also remove from edits
          NameNode.LOG.warn("purgeEditLog: removing failed storage " + sd.getRoot().getPath());
          fsimage.removedStorageDirs.add(sd);
          it.remove(); 
        }
      }
    }
    //
    // Reopen all the edits logs.
    //
    open();
  }

  /**
   * Return the name of the edit file
   */
  synchronized File getFsEditName() throws IOException {
    StorageDirectory sd = null;
    for (Iterator<StorageDirectory> it = 
        fsimage.dirIterator(NameNodeDirType.EDITS); it.hasNext();) {
      sd = it.next();
      File fsEdit = getEditFile(sd);
      if (sd.getRoot().canRead() && fsEdit.exists()) {
        return fsEdit;
      }
    }
    return null;
  }
  
  /**
   * Return the name of the edit.new file
   */
  synchronized File getFsEditNewName() throws IOException {
    StorageDirectory sd = null;
    for (Iterator<StorageDirectory> it = 
        fsimage.dirIterator(NameNodeDirType.EDITS); it.hasNext();) {
      sd = it.next();
      File fsEdit = getEditNewFile(sd);
      if (sd.getRoot().canRead() && fsEdit.exists()) {
        return fsEdit;
      }
    }
    return null;
  }

  /**
   * Returns the timestamp of the edit log
   */
  synchronized long getFsEditTime() {
    Iterator<StorageDirectory> it = fsimage.dirIterator(NameNodeDirType.EDITS);
    if(it.hasNext())
      return getEditFile(it.next()).lastModified();
    return 0;
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
   * Return the current transaction ID for the edit log.
   * This is the transaction ID that would be issued to the next transaction.
   */
  public synchronized long getCurrentTxId() {
    return txid;
  }

  /**
   * Return the transaction ID for the transaction that was written last.
   */
  synchronized long getLastWrittenTxId() {
    return getCurrentTxId() - 1;
  }
  
  synchronized long getLastSyncedTxId() {
    return synctxid;
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

  /** This method is defined for compatibility reason. */
  static private DatanodeDescriptor[] readDatanodeDescriptorArray(DataInput in
      ) throws IOException {
    DatanodeDescriptor[] locations = new DatanodeDescriptor[in.readInt()];
    for (int i = 0; i < locations.length; i++) {
      locations[i] = new DatanodeDescriptor();
      locations[i].readFieldsFromFSEditLog(in);
    }
    return locations;
  }

  static private Block[] readBlocks(DataInputStream in) throws IOException {
    int numBlocks = in.readInt();
    Block[] blocks = new Block[numBlocks];
    for (int i = 0; i < numBlocks; i++) {
      blocks[i] = new Block();
      blocks[i].readFields(in);
    }
    return blocks;
  }
  
  private static void incrOpCount(FSEditLogOpCodes opCode,
      EnumMap<FSEditLogOpCodes, Holder<Integer>> opCounts) {
    Holder<Integer> holder = opCounts.get(opCode);
    if (holder == null) {
      holder = new Holder<Integer>(1);
      opCounts.put(opCode, holder);
    } else {
      holder.held++;
    }
  }
  
  public static void dumpOpCounts(
      EnumMap<FSEditLogOpCodes, Holder<Integer>> opCounts) {
    StringBuilder sb = new StringBuilder();
    sb.append("Summary of operations loaded from edit log:\n  ");
    sb.append(opCounts);
    FSImage.LOG.debug(sb.toString());
  }

}
