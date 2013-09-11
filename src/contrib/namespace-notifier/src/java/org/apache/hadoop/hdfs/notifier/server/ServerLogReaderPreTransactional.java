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
package org.apache.hadoop.hdfs.notifier.server;

import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hdfs.notifier.NamespaceNotification;
import org.apache.hadoop.hdfs.notifier.NotifierUtils;
import org.apache.hadoop.hdfs.server.namenode.EditLogFileInputStream;
import org.apache.hadoop.hdfs.server.namenode.EditLogInputStream;
import org.apache.hadoop.hdfs.server.namenode.EditLogFileInputStream.LogHeaderCorruptException;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp;
import org.apache.hadoop.hdfs.server.namenode.NNStorage.NameNodeFile;


public class ServerLogReaderPreTransactional implements IServerLogReader {
  public static final Log LOG = LogFactory
      .getLog(ServerLogReaderPreTransactional.class);

  // Constants used in the edit log file loading
  private static final int LOG_HEADER_CORRUPT_RETRY_MAX = 10;
  private static final int IO_EXCEPTION_RETRY_MAX = 10;
  private static final int LOG_HEADER_CORRUPT_BASE_SLEEP = 50;
  private static final int IO_EXCEPTION_BASE_SLEEP = 500;
  
  private EditLogInputStream inputStream;  
  private File editsFile, editsNewFile, fsTimeFile;
  
  private IServerCore core;
  
  // The current position in the edit log
  private long editLogFilePosition;

  // A separate thread which checks if the edits file roll process began
  private EditsRollChecker editsRollChecker;
  
  // Set to true when it is detected that the NameNode closed the edits
  // file we have opened. This means that there won't be any more operations
  // written to the edits log.
  private boolean curStreamFinished = false;
  
  // Set to true when we read all the operations from the current stream.
  // Note: This can only be true when curStreamFinished is true.
  private boolean curStreamConsumed = false;
  
  // Set to true when the most recent operation read from the input stream
  // was null.
  //private boolean previousReadIsNull = false;
  
  // Set to true if we did at least one null read after the input stream
  // was finished.
  private boolean readNullAfterStreamFinished = false;
  
  // true if the name of the file from which the current input stream is
  // loaded is 'edits.new', false if 'edits'.
  private boolean openedFromEditsNew = false;
  
  private AtomicLong rollImageCount = new AtomicLong(0);
  
  // Used to ensure the transaction id's we read are consecutive.
  private long expectedTransactionId = -1;
  
  public ServerLogReaderPreTransactional(IServerCore core, URI editsURI) throws IOException {
    File editsDir = new File(NotifierUtils.uriToFile(editsURI), "current");    
    
    this.core = core;
    editsFile = new File(editsDir, NameNodeFile.EDITS.getName());
    editsNewFile = new File(editsDir, NameNodeFile.EDITS_NEW.getName());
    fsTimeFile = new File(editsDir, NameNodeFile.TIME.getName());
    
    // Initial open
    openEditLog();
  }
  
  @Override
  public void run() {
    editsRollChecker = new EditsRollChecker();
    editsRollChecker.run();
  }
  

  /**
   * Reads from the edit log until it reaches an operation which can
   * be considered a namespace notification (like FILE_ADDED, FILE_CLOSED
   * or NODE_DELETED). 
   * 
   * @return the notification object or null if nothing is to be returned
   *         at the moment.
   * @throws IOException raised when a fatal error occurred.
   */
  public NamespaceNotification getNamespaceNotification()
      throws IOException {
    FSEditLogOp op = null;
    NamespaceNotification notification = null;
    
    // Keep looping until we reach an operation that can be
    // considered a notification.
    while (true) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("edits.size=" + editsFile.length() + " editsNew.size=" +
            editsNewFile.length());
      }
      try {
        op = inputStream.readOp();
        if (LOG.isDebugEnabled()) {
          LOG.debug("inputStream.readOP() returned " + op);
        }
      } catch (IOException e) {
        LOG.warn("inputStream.readOp() failed", e);
        tryReloadingEditLog();
        return null;
      } catch (Exception e2) {
        LOG.error("Error reading log operation", e2);
        throw new IOException(e2);
      }
      
      // No operation to read at the moment from the transaction log
      if (op == null) {
        core.getMetrics().reachedEditLogEnd.inc();
        handleNullRead();
        trySwitchingEditLog();
        return null;
      } else {
        core.getMetrics().readOperations.inc();
        readNullAfterStreamFinished = false;
      }
      
      if (LOG.isDebugEnabled()) {
        LOG.debug("Read operation: " + op + " with txId=" +
            op.getTransactionId());
      }
      
      if (ServerLogReaderUtil.shouldSkipOp(expectedTransactionId, op)) {
        updateStreamPosition();
        continue;
      }
      expectedTransactionId = ServerLogReaderUtil.checkTransactionId(
          expectedTransactionId, op);
      updateStreamPosition();
      
      // Test if it can be considered a notification
      notification = ServerLogReaderUtil.createNotification(op);
      if (notification != null) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Emitting " + NotifierUtils.asString(notification));
        }
        core.getMetrics().readNotifications.inc();
        return notification;
      } 
    }
  }
  
  
  /**
   * Called after we read a null operation from the transaction log.
   * @throws IOException when a fatal error occurred.
   */
  private void handleNullRead() throws IOException {
    if (curStreamFinished && readNullAfterStreamFinished) {
      // If we read a null operation after the NameNode closed
      // the stream, then we surely reached the end of the file.
      curStreamConsumed = true;
    } else {
      try {
        // This affects how much we wait after we reached the end of the
        // current stream.
        Thread.sleep(100);
      } catch (InterruptedException e) {
        throw new IOException(e);
      }
    }
    
    if (curStreamFinished)
      readNullAfterStreamFinished = true;
    refreshStreamPosition();
  }
  
  
  /**
   * Sets the current position of the input stream, after a
   * transaction has been successfully consumed.
   * @throws IOException if a fatal error occurred.
   */
  private void updateStreamPosition() throws IOException {
    try {
      editLogFilePosition = inputStream.getPosition();
    } catch (IOException e) {
      LOG.error("Failed to get edit log file position", e);
      throw new IOException("updateStreamPosition failed");
    }
  }
  
  
  /**
   * Called to refresh the position in the current input stream
   * to the last ACK'd one.
   * @throws IOException if a fatal error occurred.
   */
  private void refreshStreamPosition() throws IOException {
    LOG.info("Refreshing the stream at position: " + editLogFilePosition);
    inputStream.refresh(editLogFilePosition, expectedTransactionId);
  }
  
  
  /**
   * Tries to fully reload the edit log.
   * 
   * @throws IOException if a fatal error occurred.
   */
  private void tryReloadingEditLog() throws IOException {
    LOG.info("Trying to reload the edit log ...");
    // The roll image count is 1 after edits.new was renamed to edits
    if (rollImageCount.get() == 1) {
      try {
        LOG.info("Trying to reload the edit log from " +
            editsFile.getAbsolutePath());
        openInputStream(editsFile);
        LOG.info("Successfully reloaded the edit log from " +
            editsFile.getAbsolutePath() + ". Trying to refresh position.");
        refreshStreamPosition();
        LOG.info("Successfully refreshed stream position");
        return;
      } catch (IOException e) {
        LOG.warn("Failed to reload from " + editsFile.getAbsolutePath(), e);
      }
    }
    
    try {
      LOG.info("Trying to reload the edit log from " +
          editsNewFile.getAbsolutePath());
      openInputStream(editsNewFile);
      LOG.info("Successfully reloaded the edit log from " +
          editsNewFile.getAbsolutePath() + ". Trying to refresh position.");
      refreshStreamPosition();
      LOG.info("Successfully refreshed stream position");
      return;
    } catch (IOException e) {
      LOG.error("Failed to reload from " + editsFile.getAbsolutePath(), e);
      throw e;
    }
  }
  
  
  /**
   * Tries to switch from the previous transaction file to the new one.
   * It ensures that no notifications are missed. If there is a possibility
   * that the current transaction log file has more notifications to be read,
   * then it will keep the current stream intact.
   * 
   * @throws IOException if a fatal error occurred.
   */
  private void trySwitchingEditLog() throws IOException {
    if (shouldSwitchEditLog()) {
      curStreamFinished = true;
      if (LOG.isDebugEnabled()) {
        LOG.debug("Should switch edit log. rollImageCount=" + rollImageCount +
            ". curStreamConsumed=" + curStreamConsumed);
      }
      
      if (curStreamConsumed) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Reloading edit log ...");
        }
        openEditLog();
        rollImageCount.decrementAndGet();
      }
    }
  }
  
  
  /**
   * @return true if a new edit log was created and is used and we should
   *         switch to it when we finish consuming the current edit log,
   *         false otherwise.
   */
  private boolean shouldSwitchEditLog() {
    // The rollImageCount is incremented each time edits.new is renamed to
    // edits. Thus:
    // * rollImageCount.get() == 1 && editsNewFile.exists()
    //   This is triggered if edits.new was renamed to edits and a new
    //   edits.new file was created meanwhile which is active.
    //   > This usually happens if the rolling process is lengthy.
    // * rollImageCount.get() == 2
    //   This is triggered if edits.new was renamed to edits once and
    //   a new edits.new file was created meanwhile which was itself
    //   renamed to edits.
    //   > This usually happens if the rolling process is short
    // This first case usually happens if the rolling process is lengthy,
    // while the second case mostly when it's very short. Lengthy and short
    // in this case are relative to the polling time (currently 100ms).
    return (rollImageCount.get() == 1 && editsNewFile.exists()) ||
        rollImageCount.get() == 2;
  }  
  
  /**
   * Tries to load the edit log file: first from 'edits.new', and if it
   * doesen't exist, from 'edits'.
   * 
   * @throws IOException If a fatal error occurred.
   */
  private void openEditLog() throws IOException {
    if (editsNewFile.exists()) {
      LOG.info("Trying to load edit log from 'edits.new'");
      try {
        openInputStream(editsNewFile);
        openedFromEditsNew = true;
        return;
      } catch (IOException e) {
        LOG.info("Failed to load from 'edits.new'. Trying from 'edits'", e);
      }
    }
    
    openInputStream(editsFile);
    openedFromEditsNew = false;
    LOG.info("Edit log loaded from 'edits'");
  }
  

  /**
   * Tries opening the input stream.
   * @param txFile the file from which to load the input stream
   * @throws IOException If the input stream couldn't be loaded. This error
   *                     is fatal.
   */
  private void openInputStream(File txFile) throws IOException {
    int ioExceptionRetryCount = 0, logHeaderCorruptRetryCount = 0;
    
    LOG.info("Trying to load the edit log from " + txFile.getAbsolutePath());
    
    do {
      try {
        inputStream = new EditLogFileInputStream(txFile);
        editLogFilePosition = inputStream.getPosition();
        curStreamConsumed = false;
        curStreamFinished = false;
        readNullAfterStreamFinished = false;
        LOG.info("Successfully loaded the edits log from " +
            txFile.getAbsolutePath());
        break;
        
      } catch (LogHeaderCorruptException e1) {
        if (logHeaderCorruptRetryCount == LOG_HEADER_CORRUPT_RETRY_MAX) {
          LOG.error("Failed to load the edit log. No retries left.", e1);
          throw new IOException("Could not load the edit log");
        }
        logHeaderCorruptRetryCount ++;
        LOG.warn("Failed to load the edit log. Retry " +
            logHeaderCorruptRetryCount + " ...", e1);
        try {
          Thread.sleep(ioExceptionRetryCount * LOG_HEADER_CORRUPT_BASE_SLEEP);
        } catch (InterruptedException e) {
          throw new IOException(e);
        }
        
      } catch (IOException e2) {
        if (ioExceptionRetryCount == IO_EXCEPTION_RETRY_MAX) {
          LOG.error("Failed to load the edit log. No retries left.", e2);
          throw new IOException("Could not load the edit log");
        }
        ioExceptionRetryCount ++;
        LOG.warn("Failed to load the edit log. Retry " +
            ioExceptionRetryCount + " ...", e2);
        try {
          Thread.sleep(ioExceptionRetryCount * IO_EXCEPTION_BASE_SLEEP);
        } catch (InterruptedException e) {
          throw new IOException(e);
        }
      }
      
    } while (true);
  }
  

  private class EditsRollChecker implements Runnable {

    long prevValue, currentValue;
    
    // Special values for the value read from fstime
    private static final int INITIAL_STATE = -1;
    private static final int FSTIME_MISSING = -2;
    
    @Override
    public void run() {
      try {
        prevValue = INITIAL_STATE;
        
        if (!openedFromEditsNew) {
          LOG.debug("Incrementing rollImageCount at start ...");
          rollImageCount.incrementAndGet();
        }
      
        while (!core.shutdownPending()) {
          DataInputStream fsTimeInputStream;
          try {
            fsTimeInputStream = 
                new DataInputStream(new FileInputStream(fsTimeFile));
          } catch (FileNotFoundException e) {
            prevValue = FSTIME_MISSING;
            continue;
          }
          try {
            currentValue = fsTimeInputStream.readLong();
            fsTimeInputStream.close();
            if (prevValue == INITIAL_STATE) {
              prevValue = currentValue;
              continue;
            }
          } catch (IOException e) {
            try {
              fsTimeInputStream.close();
            } catch (IOException e2) {}
            prevValue = FSTIME_MISSING;
            continue;
          }
          
          if (prevValue != currentValue) {
            if (LOG.isDebugEnabled()) {
              LOG.debug("Incrementing rollImageCount");
            }
            prevValue = currentValue;
            rollImageCount.incrementAndGet();
          }
          
          Thread.sleep(100);
        }
      } catch (Exception e) {
        LOG.error("Unhandled exception", e);
      } finally {
        core.shutdown();
      }
    }
  }

  @Override
  public void close() throws IOException {
    if (inputStream != null) {
      inputStream.close();
    }
  }
}