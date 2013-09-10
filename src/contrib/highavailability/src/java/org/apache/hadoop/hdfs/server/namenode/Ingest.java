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
import java.lang.Thread;
import java.util.Arrays;
import java.util.EnumMap;
import java.text.SimpleDateFormat;

import org.apache.commons.logging.Log;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ChecksumException;
import org.apache.hadoop.hdfs.protocol.FSConstants;
import org.apache.hadoop.hdfs.protocol.LayoutVersion;
import org.apache.hadoop.hdfs.protocol.LayoutVersion.Feature;
import org.apache.hadoop.hdfs.server.namenode.AvatarNode;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.namenode.FSEditLog;
import org.apache.hadoop.hdfs.server.namenode.Standby.StandbyIngestState;
import org.apache.hadoop.hdfs.util.InjectionEvent;
import org.apache.hadoop.hdfs.util.Holder;
import org.apache.hadoop.util.InjectionHandler;

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
  volatile private boolean running = true;
  volatile private boolean catchingUp = true;
  volatile private long catchUpLag;
  volatile private boolean lastScan = false; // is this the last scan?
  private int logVersion;
  volatile private boolean success = false;  // not successfully ingested yet
  
  EditLogInputStream inputEditStream = null;
  long currentPosition; // current offset in the transaction log
  final FSNamesystem fsNamesys;
  
  // startTxId is known when the ingest is instantiated
  long startTxId = -1;
  // endTxId is -1 until the ingest completes
  long endTxId = -1;

  Ingest(Standby standby, FSNamesystem ns, Configuration conf, long startTxId) 
      throws IOException {
    this.fsNamesys = ns;
    this.standby = standby;
    this.confg = conf;
    this.startTxId = startTxId;
    catchUpLag = conf.getLong("avatar.catchup.lag", 2 * 1024 * 1024L);
    inputEditStream = standby.setupIngestStreamWithRetries(startTxId);
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
  
  public boolean isCatchingUp() {
    return catchingUp;
  }
  
  /**
   * Checks if the ingest is catching up.
   * If the ingest is consuming finalized segment, it's assumed to be behind.
   * Otherwise, catching up is based on the position of the input stream.
   * @throws IOException
   */
  private void setCatchingUp() throws IOException {
    try {
      if (inputEditStream != null && inputEditStream.isInProgress()) {
        catchingUp = (inputEditStream.length() - inputEditStream.getPosition() > catchUpLag);
      } else {
        catchingUp = true;
      }
    } catch (Exception e) {
      catchingUp = true;
    }
  }

  /**
   * Indicate that this is the last pass over the transaction log
   */
  synchronized void quiesce() {
    lastScan = true;
  }

  /**
   * Indicate whether ingest was successful or not.
   * Returns true on success, else false.
   */
  boolean getIngestStatus() {
    return success;
  }
  
  boolean isRunning() {
    return running;
  }
  
  long getStartTxId() {
    return startTxId;
  }

  /**
   * Returns the distance in bytes between the current position inside of the
   * edits log and the length of the edits log
   */
  public long getLagBytes() {
    try {
      if (inputEditStream != null && inputEditStream.isInProgress()) {
        // for file journals it may happen that we read a segment finalized
        // by primary, but not refreshed by the standby, so length() returns 0
        // hence we take max(-1,lag)
        return Math.max(-1,
            inputEditStream.length() - this.inputEditStream.getPosition());
      }
      return -1;
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
    LOG.info("Ingest: Consuming transactions: " + this.toString());

    try {
      logVersion = inputEditStream.getVersion();
      if (!LayoutVersion.supports(Feature.TXID_BASED_LAYOUT, logVersion))
        throw new RuntimeException("Log version is too old");
      
      currentPosition = inputEditStream.getPosition();
      numEdits = ingestFSEdits(); // continue to ingest 
    } finally {
      LOG.info("Ingest: Closing ingest for segment: " + this.toString());
      // At this time we are done reading the transaction log
      // We need to sync to have on disk status the same as in memory
      // if we saw end segment, we already synced
      if(endTxId == -1 && fsDir.fsImage.getEditLog().isOpen()) {
        fsDir.fsImage.getEditLog().logSync();
      }
      inputEditStream.close();
      standby.clearIngestState();
    }
    LOG.info("Ingest: Edits segment: " + this.toString()
        + " edits # " + numEdits 
        + " loaded in " + (FSNamesystem.now()-startTime)/1000 + " seconds.");

    if (logVersion != FSConstants.LAYOUT_VERSION) // other version
      numEdits++; // save this image asap
    return numEdits;
  }
  
  /**
   * Read a single transaction from the input edit log
   * @param inputEditLog the log to read from
   * @return a single edit log entry, null if EOF
   * @throws IOException on error
   */
  private FSEditLogOp ingestFSEdit(EditLogInputStream inputEditLog)
      throws IOException {
    FSEditLogOp op = null;
    try {
      op = inputEditLog.readOp();
      InjectionHandler.processEventIO(InjectionEvent.INGEST_READ_OP);
    } catch (EOFException e) {
      return null; // No more transactions.
    } catch (IOException e) {
      // rethrow, it's handled in ingestFSEdits()
      throw e;
    } catch (Exception e) {
      // some other problem, maybe unchecked exception
      throw new IOException(e);
    }
    return op;
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
    
    boolean error = false;
    boolean reopen = false;    
    boolean quitAfterScan = false;
    
    long sharedLogTxId = FSEditLogLoader.TXID_IGNORE;
    long localLogTxId = FSEditLogLoader.TXID_IGNORE;

    FSEditLogOp op = null;
    FSEditLog localEditLog = fsDir.fsImage.getEditLog();

    while (running && !quitAfterScan) {      
      // if the application requested that we make a final pass over 
      // the transaction log, then we remember it here. We close and
      // reopen the file to ensure that we can see all the data in the
      // file, one reason being that NFS has open-to-close cache
      // coherancy and the edit log could be stored in NFS.
      //
      if (reopen || lastScan) {
        inputEditStream.close();
        inputEditStream = standby.setupIngestStreamWithRetries(startTxId);
        if (lastScan) {
          // QUIESCE requested by Standby thread
          LOG.info("Ingest: Starting last scan of transaction log: " + this.toString());
          quitAfterScan = true;
        }

        // discard older buffers and start a fresh one.
        inputEditStream.refresh(currentPosition, localEditLog.getLastWrittenTxId());       
        setCatchingUp();
        reopen = false;
      }

      //
      // Process all existing transactions till end of file
      //
      while (running) {
        
        if (lastScan && !quitAfterScan) {
          // Standby thread informed the ingest to quiesce
          // we should refresh the input stream as soon as possible
          // then quitAfterScan will be true
          break;
        }
        
        // record the current file offset.
        currentPosition = inputEditStream.getPosition(); 
        InjectionHandler.processEvent(InjectionEvent.INGEST_BEFORE_LOAD_EDIT);

        fsNamesys.writeLock();
        try {
          error = false;
          op = ingestFSEdit(inputEditStream);
          
          /*
           * In the case of segments recovered on primary namenode startup, we
           * have segments that are finalized (by name), but not containing the
           * ending transaction. Without this check, we will keep looping until
           * the next checkpoint to discover this situation.
           */
          if (!inputEditStream.isInProgress()
              && standby.getLastCorrectTxId() == inputEditStream.getLastTxId()) {
            // this is a correct segment with no end segment transaction
            LOG.info("Ingest: Reached finalized log segment end with no end marker. "
                + this.toString());
            tearDown(localEditLog, false, true);
            break;
          }

          if (op == null) {
            FSNamesystem.LOG.debug("Ingest: Invalid opcode, reached end of log " +
                                   "Number of transactions found " + 
                                   numEdits);
            break; // No more transactions.
          }
          
          sharedLogTxId = op.txid;
          // Verify transaction ids match.
          localLogTxId = localEditLog.getLastWrittenTxId() + 1;
          // Fatal error only when the log contains transactions from the future
          // we allow to process a transaction with smaller txid than local
          // we will simply skip it later after reading from the ingest edits
          if (localLogTxId < sharedLogTxId
              || InjectionHandler
                  .falseCondition(InjectionEvent.INGEST_TXID_CHECK)) {
            String message = "The transaction id in the edit log : "
                + sharedLogTxId
                + " does not match the transaction id inferred"
                + " from FSIMAGE : " + localLogTxId;
            LOG.fatal(message);
            throw new RuntimeException(message);
          }

          // skip previously loaded transactions
          if (!canApplyTransaction(sharedLogTxId, localLogTxId, op))
            continue;
          
          // for recovery, we do not want to re-load transactions,
          // but we want to populate local log with them
          if (shouldLoad(sharedLogTxId)) {
            FSEditLogLoader.loadEditRecord(
                logVersion, 
                inputEditStream,
                recentOpcodeOffsets, 
                opCounts, 
                fsNamesys, 
                fsDir, 
                numEdits, 
                op);
          }     
          
          LOG.info("Ingest: " + this.toString() 
                            + ", size: " + inputEditStream.length()
                            + ", processing transaction at offset: " + currentPosition 
                            + ", txid: " + op.txid
                            + ", opcode: " + op.opCode);
          
          if (op.opCode == FSEditLogOpCodes.OP_START_LOG_SEGMENT) {
            LOG.info("Ingest: Opening log segment: " + this.toString());
            localEditLog.open();
          } else if (op.opCode == FSEditLogOpCodes.OP_END_LOG_SEGMENT) {
            InjectionHandler
                .processEventIO(InjectionEvent.INGEST_CLEAR_STANDBY_STATE);
            LOG.info("Ingest: Closing log segment: " + this.toString());
            tearDown(localEditLog, true, true);
            numEdits++;
            LOG.info("Ingest: Reached log segment end. " + this.toString());
            break;
          } else {
            localEditLog.logEdit(op);
            if (inputEditStream.getReadChecksum() != FSEditLog
                .getChecksumForWrite().getValue()) {
              throw new IOException(
                  "Ingest: mismatched r/w checksums for transaction #"
                      + numEdits);
            }
          }
          
          numEdits++;
          standby.setLastCorrectTxId(op.txid);
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
          if (localEditLog.isOpen()) {
            localEditLog.logSyncIfNeeded();
          }
          fsNamesys.writeUnlock();
        }
      } // end inner while(running) -- all breaks come here
   
      // if we failed to read the entire transaction from disk, 
      // then roll back to the offset where there was a last good 
      // read, sleep for sometime for new transaction to
      // appear in the file and then continue;
      if (error || running) {
        // discard older buffers and start a fresh one.
      	inputEditStream.refresh(currentPosition, localEditLog.getLastWrittenTxId());
        setCatchingUp();

        if (error) {
          LOG.info("Ingest: Incomplete transaction record at offset " + 
              inputEditStream.getPosition() +
                   " but the file is of size " + inputEditStream.length() + 
                   ". Continuing....");
        }

        if (running && !lastScan) {
          try {
            Thread.sleep(100); // sleep for a second
          } catch (InterruptedException e) {
            // break out of waiting if we receive an interrupt.
          }
        }
      }
    } //end outer while(running)
    
    ///////////////////// FINAL ACTIONS /////////////////////
    
    // This was the last scan of the file but we could not read a full
    // transaction from disk. If we proceed this will corrupt the image
    if (error) {
      String errorMessage = FSEditLogLoader.getErrorMessage(recentOpcodeOffsets, currentPosition);
      LOG.error(errorMessage);
      throw new IOException("Failed to read the edits log. " + 
          "Incomplete transaction at " + currentPosition);
    }
    
    // If the last Scan was completed, then stop the Ingest thread.
    if (lastScan && quitAfterScan) {
      LOG.info("Ingest: lastScan completed. " + this.toString());
      running = false;
      if(localEditLog.isOpen()) {
        // quiesced non-finalized segment
        LOG.info("Ingest: Reached non-finalized log segment end. "+ this.toString());
        tearDown(localEditLog, false, localLogTxId != startTxId);
      }
    }
    FSEditLogLoader.dumpOpCounts(opCounts);
    return numEdits; // total transactions consumed
  }
  
  private void tearDown(FSEditLog localEditLog,
      boolean writeEndTxn, boolean updateLastCorrectTxn) throws IOException {
    localEditLog.endCurrentLogSegment(writeEndTxn);
    endTxId = localEditLog.getLastWrittenTxId();
    running = false;
    lastScan = true;
    if (updateLastCorrectTxn) {
      standby.setLastCorrectTxId(endTxId);
    }
    standby.clearIngestState(endTxId + 1);
  }
  
  public String toString() {
    String endStr = (endTxId == -1) ? "in progress" : ("" + endTxId);
    return "Log segment (" + startTxId + ", " + endStr + ")";
  }

  private boolean canApplyTransaction(long sharedLogTxid, long localLogTxid,
      FSEditLogOp op) {
    boolean canApply = false;
    // process all transaction if log has no tx id's
    // it is up to the avatarnode to ensure we 
    // do not re-consume the same edits
    if (sharedLogTxid == FSEditLogLoader.TXID_IGNORE){
      canApply = true;
    } else {
      // otherwise we can apply the transaction if the txid match
      canApply = (sharedLogTxid == localLogTxid);
    }
    if (!canApply) {
      LOG.info("Ingest: skip loading txId: " + sharedLogTxid
          + ", local txid: " + localLogTxid 
          + " txn: " + op);
    }
    return canApply;
  }
  
  /**
   * Used for ingest recovery, where we erase the local edit log, and
   * transactions need to be populated to the local log, but they should be 
   * not loaded into the namespace.
   */
  private boolean shouldLoad(long txid) {
    boolean shouldLoad = txid > standby.getLastCorrectTxId();
    if (!shouldLoad) {
      LOG.info("Ingest: skip loading txId: " + txid
          + " to namesystem, but writing to edit log, last correct txid: " 
          + standby.getLastCorrectTxId());
    }
    return shouldLoad;
  }
}
