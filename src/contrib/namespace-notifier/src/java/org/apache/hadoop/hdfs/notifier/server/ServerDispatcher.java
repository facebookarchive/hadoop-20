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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;

import org.apache.hadoop.hdfs.notifier.ClientHandler;
import org.apache.hadoop.hdfs.notifier.InvalidServerIdException;
import org.apache.hadoop.hdfs.notifier.NamespaceNotification;
import org.apache.hadoop.hdfs.notifier.NotifierUtils;
import org.apache.thrift.TException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class ServerDispatcher implements IServerDispatcher, IServerClientTracker {
  public static final Log LOG = LogFactory.getLog(ServerDispatcher.class);
  public static final String CLIENT_TIMEOUT = "notifier.client.timeout";
  public static final String HEARTBEAT_TIMEOUT = "notifier.heartbeat.timeout";
  public static final String THREAD_POOLS_SIZE = "notifier.dispatcher.pool.size";
  
  // Used to send the notifications to the clients.
  private static ExecutorService dispatcherWorkers;
  private static Integer dispatcherWorkersCount = -1;
  
  // Metric variables
  public static AtomicLong queuedNotificationsCount = new AtomicLong(0);
  
  private IServerCore core;
  
  // The value in milliseconds after which a client is considered down
  private long clientTimeout;
  
  // The value in milliseconds after which we send the client a heartbeat
  private long heartbeatTimeout;
  
  // For each client that failed recently, we keep the timestamp for when
  // it failed. If that timestamp gets older then currentTime - clientTimeout,
  // then the client is removed from this server.
  private Map<Long, Long> clientFailedTime = new HashMap<Long, Long>();
  
  // Mapping of each client to the last sent message
  private Map<Long, Long> clientLastSent = new HashMap<Long, Long>();
  
  // Mapping of each client for the Future objects which give the result
  // for the sendNotifications computation
  private Map<Long, Future<NotificationDispatchingStatus>> clientsFuture =
      new HashMap<Long, Future<NotificationDispatchingStatus>>();
  
  // The value slept at each loop step
  public long loopSleepTime = 10;
  
  // If a client fails to receive our notifications, then that notification
  // will be placed in a queue for the server to dispatch later. This is the
  // set of clients that the current dispatcher should handle.
  Set<Long> assignedClients;
  
  Set<Long> newlyAssignedClients;
  Set<Long> removedClients;
  Object clientModificationsLock = new Object();

  // The ID of this dispatcher thread
  private int id;
  
  public ServerDispatcher(IServerCore core, int id)
      throws ConfigurationException {
    this.core = core;
    this.id = id;
    assignedClients = new HashSet<Long>();
    newlyAssignedClients = new HashSet<Long>();
    removedClients = new HashSet<Long>();
    
    clientTimeout = core.getConfiguration().getLong(CLIENT_TIMEOUT, -1);
    heartbeatTimeout = core.getConfiguration().getLong(HEARTBEAT_TIMEOUT, -1);
    if (clientTimeout == -1) {
      throw new ConfigurationException("Invalid or missing clientTimeout: " +
          clientTimeout);
    }
    if (heartbeatTimeout == -1) {
      throw new ConfigurationException("Invalid or missing heartbeatTimeout: " +
          heartbeatTimeout);
    }
    
    synchronized (dispatcherWorkersCount) {
      if (dispatcherWorkersCount != -1) {
        // Already initialized
        return;
      }
      dispatcherWorkersCount = core.getConfiguration().getInt(
          THREAD_POOLS_SIZE, 0);
      if (dispatcherWorkersCount == 0) {
        throw new ConfigurationException("Invalid or missing " +
            "dispatcherWorkersCount: " + dispatcherWorkersCount);
      }
      
      LOG.info("Initialized dispatcherWorkers with " + dispatcherWorkersCount +
          " workers");
      dispatcherWorkers = Executors.newFixedThreadPool(dispatcherWorkersCount);
    }
  }
  
  
  @Override
  public void run() {
    try {
      LOG.info("Dispatcher " + id + " starting ...");
      while (!core.shutdownPending()) {
        long stepStartTime = System.currentTimeMillis();
   
        // Update the assigned clients
        synchronized (clientModificationsLock) {
          updateClients();
        }
        
        // Send the notifications from the core's client queues
        sendNotifications();
        
        // If we should send any heartbeats
        sendHeartbeats();

        // Check if any clients timed out
        checkClientTimeout();

        // Updates the metrics
        updateMetrics();
        
        // Sleeping at most loopSleepTime milliseconds
        long currentTime = System.currentTimeMillis();
        long sleepTime = stepStartTime + loopSleepTime - currentTime;
        if (sleepTime > 0) {
          Thread.sleep(sleepTime);
        }
      }
    } catch (Exception e) {
      LOG.error("Server Dispatcher " + id + " died", e);
    } finally {
      dispatcherWorkers.shutdown();
      dispatcherWorkersCount = -1;
      core.shutdown();
    }
  }

  
  private void updateMetrics() {
    core.getMetrics().queuedNotifications.set(queuedNotificationsCount.get());
  }
  
  /**
   * Assigns a client to this dispatcher. If a notification fails to be sent
   * to a client, then it will be placed in a queue and the assigned
   * dispatcher for each client will try to re-send notifications from that
   * queue.
   * 
   * @param clientId the id of the assigned client.
   */
  @Override
  public void assignClient(long clientId) {
    LOG.info("Dispatcher " + id + ": Assigning client " + clientId + " ...");
    synchronized (clientModificationsLock) {
      newlyAssignedClients.add(clientId);
    }
  }
  
  
  /**
   * Removes the client (it's not assigned anymore to this dispatcher).
   * 
   * @param clientId the id of the client to be removed.
   */
  @Override
  public void removeClient(long clientId) {
    LOG.info("Dispatcher " + id + ": Removing client " + clientId + " ...");
    synchronized (clientModificationsLock) {
      removedClients.add(clientId);
    }
  }
  
  
  private void sendNotifications() {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Dispatcher " + id + ": sending notifications ...");
    } 
    
    for (Long clientId : assignedClients) {
      Future<NotificationDispatchingStatus> clientFuture =
          clientsFuture.get(clientId);
      NotificationDispatchingStatus status;

      if (!shouldSendNotifications(clientId)) {
        continue;
      }
      
      // Get the results of the last batch notification sending
      // for this client.
      if (clientFuture != null && clientFuture.isDone()) {
        try {
          status = clientFuture.get();
        } catch (InterruptedException e) {
          LOG.error("Dispatcher " + id + ": got interrupted for client " +
              clientId, e);
          core.shutdown();
          return;
        } catch (ExecutionException e) {
          LOG.error("Dispatcher " + id + ": got exception in " +
              "clientFuture.get() for client " + clientId, e);
          core.shutdown();
          return;
        }
        
        clientHandleNotificationFailed(clientId,
            status.lastSentNotificationFailed);
        clientHandleNotificationSuccessful(clientId,
            status.lastSentNotificationSuccessful);
      }
      
      // Submit a new batch notification sending callable for this client
      if (clientFuture == null ||
          (clientFuture != null && clientFuture.isDone())) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Dispatcher " + id + ": submiting for client " + clientId);
        }
        clientFuture = dispatcherWorkers.submit(
            new NotificationsSender(clientId));
        clientsFuture.put(clientId, clientFuture);
      }

    }

    if (LOG.isDebugEnabled()) {
      LOG.debug("Dispatcher " + id + ": done sending notifications.");
    }
  }
  
  
  /**
   * Used for a increasing (exponential) retry timer. If a client is marked
   * as potentially failed, this function will compute if enough time passed
   * since the last time we tried to send the specified client a notification. 
   * @param clientId the client to which we want to send the notification
   * @return true if we should send, false otherwise
   */
  private boolean shouldSendNotifications(long clientId) {
    Long failedTime = clientFailedTime.get(clientId);
    Long lastSent = clientLastSent.get(clientId);
    long prevDiffTime, curDiffTime;
    
    // If it's not marked as failed, we should send right away
    if (failedTime == null) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Dispatcher " + id + ": Client " + clientId +
            " not failed. shouldSendNotification returning true");
      }
      return true;
    }
    
    prevDiffTime = lastSent - failedTime;
    curDiffTime = System.currentTimeMillis() - failedTime;
    if (curDiffTime > 2 * prevDiffTime) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Dispatcher " + id + ": Client " + clientId +
            " failed. shouldSendNotification returning true");
      }
      return true;
    }
    
    if (LOG.isDebugEnabled()) {
      LOG.debug("Dispatcher " + id + ": Client " + clientId +
          " failed. shouldSendNotification returning false");
    }
    return false;
  }
  
  
  private boolean sendNotification(NamespaceNotification notification,
      ClientHandler.Iface clientObj, long clientId) {
    Lock clientLock = core.getClientCommunicationLock(clientId);
    if (LOG.isDebugEnabled()) {
      LOG.debug("Dispatcher " + id + ": sending " +
          NotifierUtils.asString(notification) + " to client " +
          clientId + " ...");
    }

    if (clientLock == null) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Dispatcher " + id + ": client " + clientId + " is deleted" +
            "while sending a notification");
      }
      return false;
    }
    
    try {
      clientLock.lock();
      clientObj.handleNotification(notification, core.getId());
    } catch (InvalidServerIdException e) {
      // The client isn't connected to us anymore
      LOG.info("Dispatcher " + id + ": client " + clientId +
          " rejected the notification", e);
      core.removeClient(clientId);
      return false;
    } catch (TException e) {
      LOG.warn("Dispatcher " + id + " failed sending notification to client" +
          clientId, e);
      core.getMetrics().failedNotifications.inc();
      return false;
    } finally {
      clientLock.unlock();
    }
    
    if (LOG.isDebugEnabled()) {
      LOG.debug("Dispatcher " + id + ": notification sent successfully to " +
          "client " + clientId);
    }
    core.getMetrics().dispatchedNotifications.inc();
    return true;
  }
  
  
  /**
   * Overrides the timeout after which a client is removed.
   * @param timeout the new timeout after which a client is removed. The value
   *        is given in milliseconds.
   */
  @Override
  public void setClientTimeout(long timeout) {
    clientTimeout = timeout;
  }

  
  /**
   * Overrides the timeout after which a heartbeat is sent to the client if
   * no other messages were sent
   * @param timeout the new timeout in milliseconds. 
   */
  @Override
  public void setHeartbeatTimeout(long timeout) {
    heartbeatTimeout = timeout;
  }
  
  
  /**
   * Should be called each time a handleNotification Thrift call to a client
   * failed.
   * @param clientId the id of the client on which the call failed.
   * @param failedTime the time at which sending the notification failed. If
   *        this is -1, then nothing will be updated. 
   */
  @Override
  public void clientHandleNotificationFailed(long clientId, long failedTime) {
    if (failedTime == -1)
      return;
    // We only add it and don't update it because we are interested in
    // keeping track of the first moment it failed
    if (!clientFailedTime.containsKey(clientId)) {
      core.getMetrics().markedClients.inc();
      clientFailedTime.put(clientId, failedTime);
    }
    
    if (LOG.isDebugEnabled()) {
      LOG.debug("Dispatcher " + id + ": Marking client " + clientId +
          " potentially failed at " + failedTime);
    }

    clientLastSent.put(clientId, failedTime);
  }
  
  
  /**
   * Should be called each time a handleNotification Thrift call to a client
   * was successful.
   * @param clientId the id of the client on which the call was successful.
   * @param sentTime the time at which the notification was sent. Nothing
   *        happens if this is -1.
   */
  @Override
  public void clientHandleNotificationSuccessful(long clientId, long sentTime) {
    if (sentTime == -1)
      return;
    clientFailedTime.remove(clientId);
    if (LOG.isDebugEnabled()) {
      LOG.debug("Dispatcher " + id + ": Removing client " + clientId +
          " (if already present) from potentially failed clients at " +
         sentTime);
    }
    
    clientLastSent.put(clientId, sentTime);
  }

  
  private void sendHeartbeats() {
    int sentHeartbeats = 0;
    
    if (LOG.isDebugEnabled()) {
      LOG.debug("Dispatcher " + id + ": Checking heartbeats for " +
          clientLastSent.size() + " clients ...");
    }
    
    for (Long clientId : clientLastSent.keySet()) {
      long currentTime = System.currentTimeMillis();
      
      // Not trying for potentially failed clients
      if (clientFailedTime.containsKey(clientId))
        continue;
      if (LOG.isDebugEnabled()) {
        LOG.debug("Dispatcher " + id + ": Testing for client " + clientId +
            " with currentTime=" + currentTime + " clientLastSent=" +
            clientLastSent.get(clientId) + " heartbeatTimeout=" +
            heartbeatTimeout);
      }
      
      if (currentTime - clientLastSent.get(clientId) > heartbeatTimeout) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Dispatcher " + id + ": Sending heartbeat to " + clientId +
              ". currentTime=" + currentTime + " clientLastSent=" + clientLastSent.get(clientId));
        }
        
        ClientHandler.Iface clientObj = core.getClient(clientId);
        Lock commLock = core.getClientCommunicationLock(clientId);
        if (clientObj == null || commLock == null) {
          continue;
        }

        boolean acquiredLock = false;
        try {
          if (commLock.tryLock()) {
            acquiredLock = true;
            clientObj.heartbeat(core.getId());
          }
        } catch (Exception e) {
          LOG.error("Dispatcher " + id + ": Failed to heartbeat client " +
              clientId, e);
          clientHandleNotificationFailed(clientId, System.currentTimeMillis());
          continue;
        } finally {
          if (acquiredLock) {
            commLock.unlock();
          }
        }
        
        if (acquiredLock) {
          // Even if it failed, we still mark we sent it or we will flood
          // with heartbeats messages after HEARTBEAT_TIMEOUT milliseconds
          // if a client is down.
          clientLastSent.put(clientId, currentTime);
          sentHeartbeats ++;
          if (LOG.isDebugEnabled()) {
            LOG.debug("Dispatcher " + id + ": Done sending heartbeat to " +
                clientId + ". currentTime=" + currentTime +
                " clientLastSent=" + clientLastSent.get(clientId) +
                ". acquiredLock=" + acquiredLock);
        }

        }
      }
    }
    
    core.getMetrics().heartbeats.inc(sentHeartbeats);
    if (LOG.isDebugEnabled()) {
      LOG.debug("Dispatcher " + id + ": Done checking heartbeats ...");
    }
  }
  
  
  /**
   * Must be called with the clientsModificationLock hold.
   */
  private void updateClients() {
    long currentTime = System.currentTimeMillis();
    if (LOG.isDebugEnabled()) {
      LOG.debug("Dispatcher " + id + ": updating clients ...");
    }
    assignedClients.addAll(newlyAssignedClients);
    assignedClients.removeAll(removedClients);
    
    for (Long clientId : newlyAssignedClients) {
      if (!clientLastSent.containsKey(clientId)) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Dispatcher " + id + ": adding client " + clientId +
               " to clientLastSent with timestamp " + currentTime);
        }
        clientLastSent.put(clientId, currentTime);
      }
    }
    for (Long clientId : removedClients) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Dispatcher " + id + ": removing client " + clientId +
             " from clientLastSent and clientFailedTime ");
      }
      clientLastSent.remove(clientId);
      clientFailedTime.remove(clientId);
    }
    
    if (LOG.isDebugEnabled()) {
      LOG.debug("Dispatcher " + id + ": done updating clients.");
    }

    newlyAssignedClients.clear();
    removedClients.clear();
  }
  
  
  private void checkClientTimeout() {
    long currentTime = System.currentTimeMillis();
    
    if (LOG.isDebugEnabled()) {
      LOG.debug("Dispatcher " + id + ": Cleaning failed clients ...");
    }

    for (Long clientId : clientFailedTime.keySet()) {
      Long failedTimestamp = clientFailedTime.get(clientId);
      
      // The client responded and was removed from the clientFailedTime
      // map while we were iterating over it.
      if (failedTimestamp == null) {
        continue;
      }
      
      if (currentTime - failedTimestamp > clientTimeout) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Dispatcher " + id + ": Failed client detected with" +
              " clientId " + clientId);
        }
        core.removeClient(clientId);
        core.getMetrics().failedClients.inc();
      }
    }
  }
  
  
  /**
   * Batch sends a group of notifications for a given client.
   */
  private class NotificationsSender implements
      Callable<NotificationDispatchingStatus> {
    long clientId;
    
    NotificationsSender(long clientId) {
      this.clientId = clientId;
    }
    
    @Override
    public NotificationDispatchingStatus call() throws Exception {
      NotificationDispatchingStatus status =
          new NotificationDispatchingStatus();
    
      long startTime, currentTime, prevTime;
      Queue<NamespaceNotification> queuedNotifications =
          core.getClientNotificationQueue(clientId);
      ClientHandler.Iface clientObj = core.getClient(clientId);
      
      if (queuedNotifications == null || clientObj == null) {
        // We are doing this in the middle of the process of the client
        // being removed. Nothing to do here.
        return status;
      }
      
      if (LOG.isDebugEnabled()) {
        LOG.debug("Dispatcher " + id + ": sending notifications for" 
            + " client " + clientId + " ...");
      }
      
      startTime = System.currentTimeMillis();
      prevTime = startTime;
      while (!queuedNotifications.isEmpty()) {
        NamespaceNotification notification = queuedNotifications.peek();
        boolean sentSuccessfuly = sendNotification(notification, clientObj,
            clientId);
        currentTime = System.currentTimeMillis();
        
        if (sentSuccessfuly) {
          core.getMetrics().dispatchNotificationRate.inc(
              currentTime - prevTime);
          queuedNotificationsCount.decrementAndGet();
          status.lastSentNotificationSuccessful = currentTime;
          queuedNotifications.poll();
        } else {
          status.lastSentNotificationFailed = currentTime;
          break;
        }
        
        // TODO - make this configurable
        if (currentTime - startTime > 3000) {
          if (LOG.isDebugEnabled()) {
            LOG.debug("Dispatcher " + id + ": Sending for client " +
                clientId + " reached time limit");
          }
          break;
        }
        prevTime = currentTime;
      }
      return status;
    }
    
  }
  
  private class NotificationDispatchingStatus {
    long lastSentNotificationSuccessful = -1;
    long lastSentNotificationFailed = -1;
  }
}
