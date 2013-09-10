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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSUtil;
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
  
  // store the timestamp/transaction_id order of the history entries.
  final ArrayList<HistoryTreeEntry> orderedHistoryList;
  // the root of the history tree.
  final HistoryNode historyTree;

  ReentrantReadWriteLock historyLock = new ReentrantReadWriteLock();
  
  private IServerCore core;
  
  // The length in time over which the history is kept
  private volatile long historyLength;
  
  // The number of queues in the history tree
  volatile long historyQueuesCount = 0;
  
  // The physical limit of the number of items in the history
  private volatile long historyLimit;
  private volatile boolean historyLimitDisabled = false;
  
  // compare by transaction id
  private final HistoryTreeEntryComparatorById comparatorByID = 
      new HistoryTreeEntryComparatorById();
  
  // compare by the timestamp when we store the event.
  private final HistoryTreeEntryComparatorByTS comparatorByTS = 
      new HistoryTreeEntryComparatorByTS();
  
  public ServerHistory(IServerCore core, boolean initialRampUp)
      throws ConfigurationException {
    this.core = core;
    historyLength = core.getConfiguration().getLong(HISTORY_LENGTH, -1);
    historyLimit = core.getConfiguration().getLong(HISTORY_LIMIT, -1);
    historyTree = new HistoryNode("");
    orderedHistoryList = new ArrayList<HistoryTreeEntry>();
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
  
  /**
   * Set the length in time 
   */
  @Override
  public void setHistoryLength(long newHistoryLength) {
    historyLength = newHistoryLength;
  }
  
  /**
   * set the number of transactions kept in history
   */
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
   * Checks if there are notifications in our tree which are older than
   * historyLength. It removes does which are older.
   */
  private void cleanUpHistory() {
    long oldestAllowedTimestamp = System.currentTimeMillis() - historyLength;
    int trashedNotifications = 0;
    
    if (LOG.isDebugEnabled()) {
      LOG.debug("History cleanup: Checking old notifications to remove from history list ...");
    }
    
    HistoryTreeEntry key = new HistoryTreeEntry(oldestAllowedTimestamp, 0, (byte)0);
    int notificationsCount = 0;
    historyLock.writeLock().lock();
    try {
      notificationsCount = orderedHistoryList.size();
      LOG.warn("History cleanup: size of the history before cleanup: " + notificationsCount);

      if (!historyLimitDisabled && notificationsCount > historyLimit) {
        LOG.warn("History cleanup: Reached physical limit. Number of stored notifications: " + 
            notificationsCount + ". Clearing ...");
      }
      
      int index = Collections.binarySearch(orderedHistoryList, key, comparatorByTS);
      int toDeleteByTS = index >= 0 ? index : - (index + 1);
      int toDeleteByLimit = historyLimitDisabled ? 0 : notificationsCount - (int)historyLimit;
      toDeleteByLimit = toDeleteByLimit > 0 ? toDeleteByLimit : 0;
      
      int toDelete = Math.max(toDeleteByTS, toDeleteByLimit);
      
      // Delete items which are too old
      if (toDelete > 0) {
        LOG.warn("History cleanup: number of the history to cleanup: " + toDelete);
        for (int i = 0; i < toDelete; i++) {
          orderedHistoryList.get(i).removeFromTree();
        }
        
        orderedHistoryList.subList(0, toDelete).clear();
        
        if (toDeleteByLimit > toDeleteByTS) {
          // If we delete a notification because we don't have space left
          trashedNotifications ++;
        }
        notificationsCount = orderedHistoryList.size();
        LOG.warn("History cleanup: size of the history after cleanup: " + notificationsCount);
        
        // clean up history tree, remove the node that has no children and
        // no notifications associated with them.
        cleanUpHistoryTree(historyTree);
      }
      
    } finally {
      historyLock.writeLock().unlock();
    }
    
    core.getMetrics().trashedHistoryNotifications.inc(trashedNotifications);
    core.getMetrics().historySize.set(notificationsCount);
    core.getMetrics().historyQueues.set(historyQueuesCount);
  }
  
  /**
   * Clean up the Tree by DFS traversal.
   * 
   * Remove the node that has no children and no notifications associated
   * with them.
   * @param node
   */
  private void cleanUpHistoryTree(HistoryNode node) {
    if (node == null || node.children == null) {
      return;
    }
    
    Iterator<HistoryNode> iterator = node.children.iterator();
    while (iterator.hasNext()) {
      HistoryNode child = iterator.next();
      
      // clean up child
      cleanUpHistoryTree(child);
      
      // clean up current node;
      if (shouldRemoveNode(child)) {
        iterator.remove();
      }
    }
  }
  
  /**
   * Should remove the node from the history tree if both the notifications 
   * and children list are empty.
   * @param node
   * @return
   */
  private boolean shouldRemoveNode(HistoryNode node) {
    if (node == null) {
      return true;
    }
    
    int sizeOfChildren = 0;
    if (node.children != null) {
      sizeOfChildren = node.children.size();
    }
    
    if (sizeOfChildren > 0) {
      return false;
    }
    
    int sizeOfNotifications = 0;
    if (node.notifications != null) {
      for (List<HistoryTreeEntry> notiList : node.notifications.values()) {
        if (notiList != null) {
          sizeOfNotifications += notiList.size();
          if (sizeOfNotifications > 0) {
            return false;
          }
        }
      }
    }
    
    return true;
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
    int notificationsCount = 0;
    historyLock.writeLock().lock();
    try {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Storing into history: " + NotifierUtils.asString(notification));
      }
      
      String[] paths = DFSUtil.split(notification.path, Path.SEPARATOR_CHAR);
      
      long timestamp = System.currentTimeMillis();
      HistoryTreeEntry entry = new HistoryTreeEntry(timestamp, notification.txId, notification.type);
      
      // Store the notification
      HistoryNode node = historyTree;
      for (String path : paths) {
        if (path.trim().length() == 0) {
          continue;
        }
        
        node = node.addOrGetChild(path);
      }
      
      if (node.notifications == null) {
        node.notifications = new HashMap<Byte, List<HistoryTreeEntry>>();
      }
      if (!node.notifications.containsKey(notification.type)) {
        node.notifications.put(notification.type, new LinkedList<HistoryTreeEntry>());
      }
      
      entry.node = node;
      node.notifications.get(notification.type).add(entry);
      
      orderedHistoryList.add(entry);
      notificationsCount = orderedHistoryList.size();
    } finally {
      historyLock.writeLock().unlock();
    }

    core.getMetrics().historySize.set(notificationsCount);
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
    
    if (LOG.isDebugEnabled()) {
      LOG.debug("Got addNotificationsToQueue for: " +
          NotifierUtils.asString(event) + " and txId: " + txId);
    }
    
    historyLock.readLock().lock();
    try {
      if (orderedHistoryList == null || orderedHistoryList.size() == 0) {
        throw new TransactionIdTooOldException("No data in history.");
      }
      
      if (orderedHistoryList.get(0).txnId > txId || 
          orderedHistoryList.get(orderedHistoryList.size() - 1).txnId < txId) {
        throw new TransactionIdTooOldException("No data in history for txId " + txId);
      }
      
      int index = Collections.binarySearch(orderedHistoryList, new HistoryTreeEntry(0, txId, event.type), 
            comparatorByID);
      
      if (index < 0) {
        // If we got here, there are 2 possibilities:
        // * The client gave us a bad transaction id.
        // * We missed one (or more) transaction(s)
        LOG.error("Potential corrupt history. Got request for: " +
            NotifierUtils.asString(event) + " and txId: " + txId);
        throw new TransactionIdTooOldException(
            "Potentially corrupt server history");
      }
      
      String dirFormatPath = event.path;
      if (!dirFormatPath.endsWith(Path.SEPARATOR)) {
        dirFormatPath += Path.SEPARATOR;
      }
      for (int i = index + 1; i < orderedHistoryList.size(); i++) {
        HistoryTreeEntry entry = orderedHistoryList.get(i);
        if (event.type != entry.type) {
          continue;
        }
        
        String entryPath = entry.getFullPath();
        if (entryPath.startsWith(dirFormatPath)) {
          notifications.add(new NamespaceNotification(entryPath, entry.type, entry.txnId));
        }
      }
    } finally {
      historyLock.readLock().unlock();
    }
  }
  
  private class HistoryTreeEntryComparatorById implements Comparator<HistoryTreeEntry> {

    @Override
    public int compare(HistoryTreeEntry o1, HistoryTreeEntry o2) {
      return (int) (o1.txnId - o2.txnId);
    }
  }
  
  private class HistoryTreeEntryComparatorByTS implements Comparator<HistoryTreeEntry> {

    @Override
    public int compare(HistoryTreeEntry o1, HistoryTreeEntry o2) {
      return (int) (o1.timestamp - o2.timestamp);
    }
  }
  
  protected class HistoryNode implements Comparable<String> {
    final String name;
    Map<Byte, List<HistoryTreeEntry>> notifications = null;
    final ArrayList<HistoryNode> children;
    HistoryNode parent = null;
    
    public HistoryNode(String name) {
      this.name = name;
      this.children = new ArrayList<HistoryNode>();
    }
    
    public HistoryNode addOrGetChild(String childName) {
      int index = Collections.binarySearch(children, childName);
      if (index >= 0) {
        return children.get(index);
      }
      
      index = -(index + 1);
      HistoryNode child = new HistoryNode(childName);
      child.parent = this;
      children.add(index, child);
      return child;
    }

    @Override
    public int hashCode() {
      return name.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
      if (obj == null || !(obj instanceof HistoryNode)) {
        return false;
      }
      return this.name.equals(((HistoryNode)obj).name);
    }

    @Override
    public int compareTo(String name2) {
      return this.name.compareTo(name2);
    }
  }
  
  protected class HistoryTreeEntry {
    long timestamp;
    long txnId;
    byte type;
    HistoryNode node;
    
    public HistoryTreeEntry(long timestamp, long txnId, byte type) {
      this.timestamp = timestamp;
      this.txnId = txnId;
      this.type = type;
    }
    
    /**
     * Get the full event path.
     * @return
     */
    public String getFullPath() {
      if (node == null) {
        return null;
      }
      
      StringBuilder sb = new StringBuilder();
      HistoryNode t = node;
      sb.append(t.name);
      while (t.parent != null) {
        t = t.parent;
        sb.insert(0, t.name + Path.SEPARATOR);
      }
      return sb.toString();
    }
    
    public boolean removeFromTree() {
      return node.notifications.get(type).remove(this);
    }
  }
  
}
