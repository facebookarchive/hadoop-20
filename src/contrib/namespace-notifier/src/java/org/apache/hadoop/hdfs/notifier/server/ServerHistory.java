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

import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hdfs.notifier.EventType;
import org.apache.hadoop.hdfs.notifier.NamespaceEvent;
import org.apache.hadoop.hdfs.notifier.NamespaceNotification;
import org.apache.hadoop.hdfs.notifier.NotifierUtils;
import org.apache.hadoop.hdfs.notifier.TransactionIdTooOldException;

public class ServerHistory implements IServerHistory {
  public static final Log LOG = LogFactory.getLog(ServerHistory.class);
  
  public int loopSleepTime = 100;
  
  // Configuration keys
  public static final String HISTORY_LENGTH = "notifier.history.length";
  public static final String HISTORY_LIMIT = "notifier.history.limit";
  
  // true if in the ramp-up phase
  private volatile boolean rampUp = true;
  
  // The actual history. It's a 2-level tree:
  // * On the top level the tree is split by the event's path
  // * On the 2nd level the tree is split by the event's type
  // The leaf nodes contain a list of transaction id's for the events
  // that occurred in the last historyLength milliseconds.
  ConcurrentMap<String, ConcurrentMap<Byte, List<HistoryTreeEntry>>> history;
  
  // Used for synchronization when parts of the history tree must be removed
  Object historyLock = new Object();
  
  private IServerCore core;
  
  // The length in time over which the history is kept
  private volatile long historyLength;
  
  // The number of queues in the history tree
  volatile long historyQueuesCount = 0;
  
  // The physical limit of the number of items in the history
  private volatile long historyLimit;
  private volatile boolean historyLimitDisabled = false;
  
  // The number of stored notifications
  AtomicLong notificationsCount = new AtomicLong(0);
  
  // The ordered by timestamp history tree entries
  List<HistoryTreeEntry> orderedEntries = 
      new LinkedList<ServerHistory.HistoryTreeEntry>();
  
  public ServerHistory(IServerCore core, boolean initialRampUp)
      throws ConfigurationException {
    this.core = core;
    historyLength = core.getConfiguration().getLong(HISTORY_LENGTH, -1);
    historyLimit = core.getConfiguration().getLong(HISTORY_LIMIT, -1);
    history = new ConcurrentHashMap<String, 
        ConcurrentMap<Byte,List<HistoryTreeEntry>>>();
    rampUp = initialRampUp;
    
    if (historyLength == -1) {
      LOG.error("Missing default configuration: historyLength");
      throw new ConfigurationException("Missing historyLength");
    }
    if (historyLimit == -1) {
      LOG.warn("Starting history without any physical limit ...");
      historyLimitDisabled = true;
    }
  }
  
  @Override
  public void setHistoryLength(long newHistoryLength) {
    historyLength = newHistoryLength;
  }
  
  
  @Override
  public void setHistoryLimit(long newHistoryLimit) {
    if (newHistoryLimit > 0) {
      historyLimitDisabled = false;
    }
    historyLimit = newHistoryLimit;
  }
  
  @Override
  public boolean isRampUp() {
    return rampUp;
  }

  
  private void checkRampUp(long startTime) {
    boolean initialIsRampUp = rampUp;
    
    if (startTime + historyLength < System.currentTimeMillis()) {
      rampUp = false;
    }
    
    if (initialIsRampUp && !rampUp) {
      LOG.info("Server went out of ramp up phase ...");
    }
  }
  
  
  /**
   * Does not guarantee any synchronization mechanisms. (historyLock must be
   * hold when calling it).
   * @param path 
   * @return the number of notifications stored in the history at path
   */
  long getNotificationsCountForPath(String path) {
    long count = 0;
    if (!history.containsKey(path)) {
      return 0;
    }
    for (Byte type : history.get(path).keySet()) {
      count += history.get(path).get(type).size();
    }
    return count;
  }


  /**
   * Checks if there are notifications in our tree which are older than
   * historyLength. It removes does which are older.
   */
  private void cleanUpHistory() {
    Set<String> toBeRemovedPaths = new HashSet<String>();
    long oldestAllowedTimestamp = System.currentTimeMillis() - historyLength;
    int trashedNotifications = 0;
    
    if (LOG.isDebugEnabled()) {
      LOG.debug("Checking old notifications to remove from history tree ...");
    }
    
    synchronized (historyLock) {
      int deletedElements = 0;

      if (!historyLimitDisabled && notificationsCount.get() > historyLimit) {
        LOG.warn("Reached physical limit. Number of stored notifications: " + 
            notificationsCount + ". Clearing ...");
      }
      
      // Delete items which are too old
      for (HistoryTreeEntry entry : orderedEntries) {
        if (entry.timestamp > oldestAllowedTimestamp &&
            (historyLimitDisabled || notificationsCount.get() <= historyLimit)) {
          // We stop removing items if the entry is new enough and
          // if we haven't reached the physical limit (if enabled)
          break;
        }
        
        if (entry.timestamp > oldestAllowedTimestamp) {
          // If we delete a notification because we don't have space left
          trashedNotifications ++;
        }
        
        // entry is the first element in the list which holds it
        entry.backPointer.remove(0);
        deletedElements ++;
        notificationsCount.decrementAndGet();
      }
      orderedEntries.subList(0, deletedElements).clear();
    }

    // Clear lists for paths which don't have any elements left
    for (String path : history.keySet()) {
      synchronized (historyLock) {
        // If we don't have any notifications left for this path, remove it
        if (getNotificationsCountForPath(path) == 0) {
          toBeRemovedPaths.add(path);
        }
      }
    }
    
    // Remove the paths marked for removal
    for (String path : toBeRemovedPaths) {
      synchronized (historyLock) {
        // Make sure no notifications were added meanwhile
        if (getNotificationsCountForPath(path) > 0) {
          continue;
        }
        historyQueuesCount -= history.get(path).size();
        history.remove(path);
      }
    }
    
    core.getMetrics().trashedHistoryNotifications.inc(trashedNotifications);
    core.getMetrics().historySize.set(notificationsCount.get());
    core.getMetrics().historyQueues.set(historyQueuesCount);
  }
  
  
  /**
   * Should be called when the server starts to start recording the history
   * (the namespace operations from the edit log).
   */
  @Override
  public void run() {
    long startTime = System.currentTimeMillis();
    
    try {
      LOG.info("Starting the history thread ...");
      while (!core.shutdownPending()) {
        checkRampUp(startTime);
        cleanUpHistory();
        Thread.sleep(loopSleepTime);
      }
      LOG.info("History thread shutdown.");
    } catch (Exception e) {
      LOG.error("ServerHistory died", e);
    } finally {
      core.shutdown();
    }
  }
  

  /**
   * Called when we should store a notification in the our history.
   * The timestamp used to store it is generated when this method is
   * called.
   * 
   * It doesn't provide ordering if it is called with miss-ordered
   * notifications (e.g. it's called once with a notification with
   * the transaction id "i" and after some time with a notification
   * with the transaction id "j", where j < i).
   * 
   * @param notification The notification to be stored.
   */
  @Override
  public void storeNotification(NamespaceNotification notification) {
    ConcurrentMap<Byte, List<HistoryTreeEntry>> typeMap;
    List<HistoryTreeEntry> historyTreeEntryList;
    String basePath = NotifierUtils.getBasePath(notification),
        additionalPath = NotifierUtils.getAdditionalPath(notification);
    
    synchronized (historyLock) {
      history.putIfAbsent(basePath,
          new ConcurrentHashMap<Byte, List<HistoryTreeEntry>>());
      typeMap = history.get(basePath);
      
      if (!typeMap.containsKey(notification.getType())) {
        typeMap.put(notification.getType(), new LinkedList<HistoryTreeEntry>());
        historyQueuesCount ++;
      }
      historyTreeEntryList = typeMap.get(notification.getType());
      
      if (LOG.isDebugEnabled()) {
        LOG.debug("Storing into history: " + NotifierUtils.asString(notification) +
            " with basePath='" + basePath + "' and additionalPath='" +
            additionalPath +"'");
      }
      
      // Store the notification
      HistoryTreeEntry entry = new HistoryTreeEntry(System.currentTimeMillis(),
          notification.getTxId(), additionalPath);
      entry.backPointer = historyTreeEntryList;
      historyTreeEntryList.add(entry);
      orderedEntries.add(entry);
    }

    notificationsCount.incrementAndGet();
    core.getMetrics().historySize.set(notificationsCount.get());
    core.getMetrics().historyQueues.set(historyQueuesCount);
    
    if (LOG.isDebugEnabled()) {
      LOG.debug("Notification stored.");
    }
  }
  
  /**
   * Checks what notifications are saved in history for the given event and
   * adds those notifications in the given queue. Only the notifications
   * which happened strictly after the edit log operations with the given
   * transaction id are put in the queue.
   * The notifications are put in the queue in the order of their
   * transaction id.
   * 
   * @param event the event for which the notifications should be stored
   *        in the queue.
   * @param txId the given transaction id
   * @param notifications the queue in which the notifications will be placed.
   * 
   * @throws TransactionIdTooOldException raised when we can't guarantee that
   *         we got all notifications that happened after the given
   *         transaction id.
   */
  @Override
  public void addNotificationsToQueue(NamespaceEvent event, long txId,
      Queue<NamespaceNotification> notifications)
          throws TransactionIdTooOldException {
    ConcurrentMap<Byte, List<HistoryTreeEntry>> typeMap;
    List<HistoryTreeEntry> historyTreeEntryList;
    
    if (LOG.isDebugEnabled()) {
      LOG.debug("Got addNotificationsToQueue for: " +
          NotifierUtils.asString(event) + " and txId: " + txId);
    }
    
    synchronized (historyLock) {
      typeMap = history.get(event.getPath());
      if (typeMap == null) {
        throw new TransactionIdTooOldException("No data in history for path " +
            event.getPath());
      }
      
      historyTreeEntryList = typeMap.get(event.getType());
      if (historyTreeEntryList == null) {
        throw new TransactionIdTooOldException("No data in history for type.");
      }
      
      if (historyTreeEntryList.isEmpty()) {
        throw new TransactionIdTooOldException("No events recently.");
      }
      if (historyTreeEntryList.get(0).transactionId > txId) {
        throw new TransactionIdTooOldException("No data in history for txId.");
      }
 
      boolean foundTransaction = false;
      for (HistoryTreeEntry entry : historyTreeEntryList) {
        if (foundTransaction) {
          String notificationPath = event.path;
          if (entry.additionalPath != null && entry.additionalPath.length() > 0) {
            if (event.path.trim().equals("/")) {
              notificationPath += entry.additionalPath;
            } else {
              notificationPath += "/" + entry.additionalPath;
            }
          }
          if (LOG.isDebugEnabled()) {
            LOG.debug("addNotificationsToQueue - adding: [" + notificationPath + ", " +
                EventType.fromByteValue(event.type).name() + "]" +
                " and txId: " + entry.transactionId);
          }
          notifications.add(new NamespaceNotification(notificationPath,
              event.type, entry.transactionId));
        }
        
        if (entry.transactionId == txId) {
          foundTransaction = true;
        }
      }
      
      if (!foundTransaction) {
        // If we got here, there are 2 possibilities:
        // * The client gave us a bad transaction id.
        // * We missed one (or more) transaction(s)
        LOG.error("Potential corrupt history. Got request for: " +
            NotifierUtils.asString(event) + " and txId: " + txId);
        throw new TransactionIdTooOldException(
            "Potentially corrupt server history");
      }
    }
  }

  
  private class HistoryTreeEntry {
    // The time when the entry was added
    long timestamp;
    
    // The Edit Log transaction id associated with this entry
    long transactionId;
    
    // Additional path (if needed by the type)
    String additionalPath;
    
    List<HistoryTreeEntry> backPointer; 
    
    public HistoryTreeEntry(long timestamp, long transactionId,
        String additionalPath) {
      this.timestamp = timestamp;
      this.transactionId = transactionId;
      this.additionalPath = additionalPath;
    }
  }
}
