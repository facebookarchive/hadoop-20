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

import java.io.IOException;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.DataInputStream;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.lang.Thread;
import java.util.Arrays;
import java.util.Date;
import java.util.EnumMap;
import java.text.SimpleDateFormat;

import java.util.zip.CheckedInputStream;
import java.util.zip.Checksum;

import org.apache.commons.logging.Log;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ChecksumException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.fs.permission.*;
import org.apache.hadoop.hdfs.protocol.FSConstants;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.server.namenode.AvatarNode;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.namenode.FSImage;
import org.apache.hadoop.hdfs.server.namenode.FSEditLog;
import org.apache.hadoop.hdfs.server.namenode.Standby.StandbyIngestState;
import org.apache.hadoop.hdfs.server.common.Storage;
import org.apache.hadoop.hdfs.util.InjectionEvent;
import org.apache.hadoop.hdfs.util.InjectionHandler;
import org.apache.hadoop.hdfs.util.Holder;
import org.apache.hadoop.hdfs.util.InjectionEvent;
import org.apache.hadoop.hdfs.util.InjectionHandler;

/**
 * This class reads transaction logs from the primary's shared device
 * and feeds it to the standby NameNode.
 */

public class Ingest implements Runnable {

  public static final Log LOG = AvatarNode.LOG;
  static final SimpleDateFormat DATE_FORM =
    new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

  private Standby standby;
  private Configuration confg;  // configuration of local namenode
  private File ingestFile;
  volatile private boolean running = true;
  volatile private boolean catchingUp = true;
  volatile private long catchUpLag;
  volatile private boolean lastScan = false; // is this the last scan?
  volatile private boolean reopen = false; // Close and open the file again
  private CheckpointSignature lastSignature;
  private int logVersion;
  volatile private boolean success = false;  // not successfully ingested yet
  EditLogInputStream inputEditLog = null;
  long currentPosition; // current offset in the transaction log
  final FSNamesystem fsNamesys;

  Ingest(Standby standby, FSNamesystem ns, Configuration conf, File edits) 
  throws IOException {
    this.fsNamesys = ns;
    this.standby = standby;
    this.confg = conf;
    this.ingestFile = edits;
    catchUpLag = conf.getLong("avatar.catchup.lag", 2 * 1024 * 1024L);
  }

  public void run() {
    if (standby.currentIngestState == StandbyIngestState.CHECKPOINTING) {
      CheckpointSignature sig = standby.getLastRollSignature();
      if (sig == null) {
        // This should never happen, since if checkpoint is in progress
        // and the Ingest thread is getting recreated there should be a sig
        throw new RuntimeException("Ingest: Weird thing has happened. The " +
        		"checkpoint was in progress, but there is no signature of the " +
        		"fsedits available");
      }
      if (sig.editsTime != ingestFile.lastModified()) {
        // This means that the namenode checkpointed itself in the meanwhile
        throw new RuntimeException("Ingest: The primary node has checkpointed" +
        		" so we cannot proceed. Restarting Avatar is the only option");
      }
      /**
       * If the checkpoint is in progress it means that the edits file was 
       * already read and we cannot read it again from the start. 
       * Just successfuly return.
       */
      success = true;
      return;
    }
    while (running) {
      try {
        // keep ingesting transactions from remote edits log.
        loadFSEdits();
      } catch (Exception e) {
        LOG.warn("Ingest: Exception while processing transactions (" + 
                 running + ") ", e);
        throw new RuntimeException("Ingest: failure", e);
      } finally {
        LOG.warn("Ingest: Processing transactions has a hiccup. " + running);
      }
    }
    success = true;      // successful ingest.
  }

  /**
   * Immediately Stop the ingestion of transaction logs
   */
  void stop() {
    running = false;
  }
  
  public boolean catchingUp() {
    return catchingUp;
  }

  /**
   * Indicate that this is the last pass over the transaction log
   * Verify that file modification time of the edits log matches
   * that of the signature.
   */
  synchronized void quiesce(CheckpointSignature sig) {
    if (sig != null) {
      lastSignature = sig;
    }
    lastScan = true;
  }

  private synchronized CheckpointSignature getLastCheckpointSignature() {
    return this.lastSignature;
  }

  /**
   * Indicate whether ingest was successful or not.
   * Returns true on success, else false.
   */
  boolean getIngestStatus() {
    return success;
  }
  
  /**
   * Get version of the consumed log
   */
  int getLogVersion(){
    return logVersion;
  }

  /**
   * Returns the distance in bytes between the current position inside of the
   * edits log and the length of the edits log
   */ 
  public long getLagBytes() {
    try {
      return this.inputEditLog == null ? -1 :
        (this.inputEditLog.length() - this.inputEditLog.getPosition());
    } catch (IOException ex) {
      LOG.error("Error getting the lag", ex);
      return -1;
    }
  }
  
  /**
   * Load an edit log, and continue applying the changes to the in-memory 
   * structure. This is where we ingest transactions into the standby.
   */
  private int loadFSEdits() throws IOException {
    FSDirectory fsDir = fsNamesys.dir;
    int numEdits = 0;
    long startTime = FSNamesystem.now();
    synchronized (standby.ingestStateLock) {
      ingestFile = standby.getCurrentIngestFile();
      inputEditLog = new EditLogFileInputStream(ingestFile);
    }
    LOG.info("Ingest: Consuming transactions from file " + ingestFile +
        " of size " + ingestFile.length());

    try {
      logVersion = inputEditLog.getVersion();
      assert logVersion <= Storage.LAST_UPGRADABLE_LAYOUT_VERSION :
                            "Unsupported version " + logVersion;
      currentPosition = inputEditLog.getPosition();
      numEdits = ingestFSEdits(); // continue to ingest 
    } finally {
      LOG.info("Ingest: Closing transactions file " + ingestFile);
      // At this time we are done reading the transaction log
      // We need to sync to have on disk status the same as in memory
      fsDir.fsImage.getEditLog().logSync();
      inputEditLog.close();
    }
    LOG.info("Ingest: Edits file " + ingestFile.getName() 
        + " of size " + ingestFile.length() + " edits # " + numEdits 
        + " loaded in " + (FSNamesystem.now()-startTime)/1000 + " seconds.");

    if (logVersion != FSConstants.LAYOUT_VERSION) // other version
      numEdits++; // save this image asap
    return numEdits;
  }

  /**
   * Continue to ingest transaction logs until the currentState is 
   * no longer INGEST. If lastScan is set to true, then we process 
   * till the end of the file and return.
   */
  int ingestFSEdits() throws IOException {
    FSDirectory fsDir = fsNamesys.dir;
    int numEdits = 0;
    
    long recentOpcodeOffsets[] = new long[2];
    Arrays.fill(recentOpcodeOffsets, -1);
    EnumMap<FSEditLogOpCodes, Holder<Integer>> opCounts =
        new EnumMap<FSEditLogOpCodes, Holder<Integer>>(FSEditLogOpCodes.class);
    
    long startTime = FSNamesystem.now();
    boolean error = false;
    boolean quitAfterScan = false;
    long diskTxid = AvatarNode.TXID_IGNORE;
    long logTxid = AvatarNode.TXID_IGNORE;

    FSEditLogOp op = null;

    while (running && !quitAfterScan) {
      // refresh the file to have correct name (for printing logs only)
      // to reopen, must synchronize
      ingestFile = standby.getCurrentIngestFile();
      
      // if the application requested that we make a final pass over 
      // the transaction log, then we remember it here. We close and
      // reopen the file to ensure that we can see all the data in the
      // file, one reason being that NFS has open-to-close cache
      // coherancy and the edit log could be stored in NFS.
      //
      if (reopen || lastScan) {
        inputEditLog.close();
        synchronized (standby.ingestStateLock) {
          ingestFile = standby.getCurrentIngestFile();
          inputEditLog = new EditLogFileInputStream(ingestFile);
        }
        if (lastScan) {
          LOG.info("Ingest: Starting last scan of transaction log " + ingestFile);
          quitAfterScan = true;
        }

        // discard older buffers and start a fresh one.
        inputEditLog.refresh(currentPosition);
        catchingUp = (inputEditLog.length() - inputEditLog.getPosition() > catchUpLag);
        reopen = false;
      }

      //
      // Verify that signature of file matches. This is imporatant in the
      // case when the Primary NN was configured to write transactions to 
      // to devices (local and NFS) and the Primary had encountered errors
      // to the NFS device and has continued writing transactions to its
      // device only. In this case, the rollEditLog() RPC would return the
      // modtime of the edits file of the Primary's local device and will
      // not match with the timestamp of our local log from where we are
      // ingesting.
      //
      CheckpointSignature signature = getLastCheckpointSignature();
      if (signature != null) {
        long localtime = ingestFile.lastModified();
        if (localtime == signature.editsTime) {
          LOG.debug("Ingest: Matched modification time of edits log. ");
        } else if (localtime < signature.editsTime) {
          LOG.info("Ingest: Timestamp of transaction log on local machine is " +
                   localtime +
                   " and on remote namenode is " + signature.editsTime);
          String msg = "Ingest: Timestamp of transaction log on local machine is " + 
                       DATE_FORM.format(new Date(localtime)) +
                       " and on remote namenode is " +
                       DATE_FORM.format(new Date(signature.editsTime));
          LOG.info(msg);
          throw new IOException(msg);
        } else {
          LOG.info("Ingest: Timestamp of transaction log on local machine is " +
                   localtime +
                   " and on remote namenode is " + signature.editsTime);
          String msg = "Ingest: Timestamp of transaction log on localmachine is " + 
                       DATE_FORM.format(new Date(localtime)) +
                       " and on remote namenode is " +
                       DATE_FORM.format(new Date(signature.editsTime)) +
                       ". But this can never happen.";
          LOG.info(msg);
          throw new IOException(msg);
        }
      }

      //
      // Process all existing transactions till end of file
      //
      while (running) {
        currentPosition = inputEditLog.getPosition(); // record the current file offset.
        InjectionHandler.processEvent(InjectionEvent.INGEST_BEFORE_LOAD_EDIT);

        fsNamesys.writeLock();
        try {
          error = false;
          try {
            op = inputEditLog.readOp();
            InjectionHandler.processEventIO(InjectionEvent.INGEST_READ_OP);
            if (op == null) {
              FSNamesystem.LOG.debug("Ingest: Invalid opcode, reached end of log " +
                                     "Number of transactions found " + 
                                     numEdits);
              break; // No more transactions.
            }
            if (logVersion <= FSConstants.STORED_TXIDS) {
              diskTxid = op.txid;
              // Verify transaction ids match.
              logTxid = fsDir.fsImage.getEditLog().getCurrentTxId();
              // Error only when the log contains transactions from the future
              // we allow to process a transaction with smaller txid than local
              // we will simply skip it later after reading from the ingest edits
              if (logTxid < diskTxid) {
                throw new IOException("The transaction id in the edit log : "
                    + diskTxid + " does not match the transaction id inferred"
                    + " from FSIMAGE : " + logTxid);
              }
            } else {
              diskTxid = AvatarNode.TXID_IGNORE;
              logTxid = fsDir.fsImage.getEditLog().getCurrentTxId();
            }
          } catch (EOFException e) {
            break; // No more transactions.
          }
          if(!canApplyTransaction(diskTxid, logTxid))
            continue;
          
          FSEditLogLoader.loadEditRecord(logVersion, 
              inputEditLog, 
              recentOpcodeOffsets, 
              opCounts, 
              fsNamesys, 
              fsDir, 
              numEdits, 
              op);        
          fsDir.fsImage.getEditLog().logEdit(op);
          
          if (inputEditLog.getReadChecksum() != FSEditLog.getChecksumForWrite().getValue()) {
            throw new IOException(
                "Ingest: mismatched r/w checksums for transaction #" + numEdits);
          }
          numEdits++;
          LOG.info("Ingest: Processed transaction from " + standby.getCurrentIngestFile() 
              + " opcode " + op.opCode + " file offset " + currentPosition);
        }
        catch (ChecksumException cex) {
          LOG.info("Checksum error reading the transaction #" + numEdits +
                   " reopening the file");
          reopen = true;
          break;
        }
        catch (IOException e) {
          LOG.info("Encountered error reading transaction", e);
          error = true; // if we haven't reached eof, then error.
          break;
        } finally {
          fsDir.fsImage.getEditLog().logSyncIfNeeded();
          fsNamesys.writeUnlock();
        }
      }
      
   
      // if we failed to read the entire transaction from disk, 
      // then roll back to the offset where there was a last good 
      // read, sleep for sometime for new transaction to
      // appear in the file and then continue;
      //
      if (error || running) {
        // discard older buffers and start a fresh one.
        inputEditLog.refresh(currentPosition);
        catchingUp = (inputEditLog.length() - inputEditLog.getPosition() > catchUpLag);

        if (error) {
          LOG.info("Ingest: Incomplete transaction record at offset " + 
              inputEditLog.getPosition() +
                   " but the file is of size " + inputEditLog.length() + 
                   ". Continuing....");
        }

        if (running && !lastScan) {
          try {
            Thread.sleep(1000); // sleep for a second
          } catch (InterruptedException e) {
            // break out of waiting if we receive an interrupt.
          }
        }
      }
    }
    
    if (error) {
      // Catch Throwable because in the case of a truly corrupt edits log, any
      // sort of error might be thrown (NumberFormat, NullPointer, EOF, etc.)
      StringBuilder sb = new StringBuilder();
      sb.append("Error replaying edit log at offset " + currentPosition);
      if (recentOpcodeOffsets[0] != -1) {
        Arrays.sort(recentOpcodeOffsets);
        sb.append("\nRecent opcode offsets:");
        for (long offset : recentOpcodeOffsets) {
          if (offset != -1) {
            sb.append(' ').append(offset);
          }
        }
      }
      String errorMessage = sb.toString();
      LOG.error(errorMessage);
      // This was the last scan of the file but we could not read a full
      // transaction from disk. If we proceed this will corrupt the image
      throw new IOException("Failed to read the edits log. " + 
            "Incomplete transaction at " + currentPosition);
    }
    LOG.info("Ingest: Edits file " + ingestFile.getName() +
      " numedits " + numEdits +
      " loaded in " + (FSNamesystem.now()-startTime)/1000 + " seconds.");
    
    if (LOG.isDebugEnabled()) {
      FSEditLog.dumpOpCounts(opCounts);
    }

    // If the last Scan was completed, then stop the Ingest thread.
    if (lastScan && quitAfterScan) {
      LOG.info("Ingest: lastScan completed.");
      running = false;
    }
    return numEdits; // total transactions consumed
  }

  private boolean canApplyTransaction(long diskTxid, long localLogTxid) {
    boolean canApply = false;
    // process all transaction if log has no tx id's
    // it is up to the avatarnode to ensure we 
    // do not re-consume the same edits
    if(diskTxid == AvatarNode.TXID_IGNORE){
      canApply = true;
    } else {
      // otherwise we can apply the transaction if the txid match
      canApply = (diskTxid == localLogTxid);
    }
    if (!canApply) {
      LOG.info("Skipping transaction txId= " + diskTxid
          + " the local transaction id is: " + localLogTxid);
    }
    return canApply;
  }

  // a place holder for reading a long
  private static final LongWritable longWritable = new LongWritable();

  /** Read an integer from an input stream */
  private static long readLongWritable(DataInputStream in) throws IOException {
    synchronized (longWritable) {
      longWritable.readFields(in);
      return longWritable.get();
    }
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
