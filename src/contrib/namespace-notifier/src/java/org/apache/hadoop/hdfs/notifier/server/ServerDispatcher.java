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
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.hadoop.hdfs.notifier.InvalidServerIdException;
import org.apache.hadoop.hdfs.notifier.NamespaceNotification;
import org.apache.hadoop.hdfs.notifier.NotifierUtils;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransportException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class ServerDispatcher implements IServerDispatcher, IServerClientTracker {
  public static final Log LOG = LogFactory.getLog(ServerDispatcher.class);
  public static final String CLIENT_TIMEOUT = "notifier.client.timeout";
  public static final String HEARTBEAT_TIMEOUT = "notifier.heartbeat.timeout";
  public static final String THREAD_POOLS_SIZE = "notifier.dispatcher.pool.size";
  
  // Used to send the notifications to the clients.
  private ExecutorService workers;
  
  // Metric variables
  public static AtomicLong queuedNotificationsCount = new AtomicLong(0);
  
  private final IServerCore core;
  
  // The value in milliseconds after which a client is considered down
  private long clientTimeout;
  
  // The value in milliseconds after which we send the client a heartbeat
  private long heartbeatTimeout;
  
  // For each client that failed recently, we keep the timestamp for when
  // it failed. If that timestamp gets older then currentTime - clientTimeout,
  // then the client is removed from this server.
  //private Map<Long, Long> clientFailedTime = new HashMap<Long, Long>();
  
  // Mapping of each client to the last sent message
  // private Map<Long, Long> clientLastSent = new HashMap<Long, Long>();
  
  // Mapping of each client for the Future objects which give the result
  // for the sendNotifications computation
  private Map<Long, Future<DispatchStatus>> clientsFuture =
      new HashMap<Long, Future<DispatchStatus>>();
  
  // The value slept at each loop step
  public long loopSleepTime = 10;
  
  // If a client fails to receive our notifications, then that notification
  // will be placed in a queue for the server to dispatch later. This is the
  // set of clients that the current dispatcher should handle.
  Set<Long> assignedClients = new HashSet<Long>();
  
  Set<Long> newlyAssignedClients = new HashSet<Long>();
  Set<Long> removedClients = new HashSet<Long>();
  Object clientModificationsLock = new Object();

  // The ID of this dispatcher thread
  //private int id;
  
  public ServerDispatcher(IServerCore core)
      throws ConfigurationException {
    int workersCount;
    this.core = core;
    
    clientTimeout = core.getConfiguration().getLong(CLIENT_TIMEOUT, -1);
    heartbeatTimeout = core.getConfiguration().getLong(HEARTBEAT_TIMEOUT, -1);
    workersCount = core.getConfiguration().getInt(THREAD_POOLS_SIZE, -1);
    if (clientTimeout == -1) {
      throw new ConfigurationException("Invalid or missing clientTimeout: " +
          clientTimeout);
    }
    if (heartbeatTimeout == -1) {
      throw new ConfigurationException("Invalid or missing heartbeatTimeout: " +
          heartbeatTimeout);
    }
    if (workersCount == 0) {
      throw new ConfigurationException("Invalid or missing " +
          "dispatcherWorkersCount: " + workersCount);
    }
    LOG.info("Initialized " + workersCount + " workers");
    workers = Executors.newFixedThreadPool(workersCount);
  }
  
  
  @Override
  public void run() {
    try {
      LOG.info("Dispatcher starting ...");
      while (!core.shutdownPending()) {
        long stepStartTime = System.currentTimeMillis();

        // Update the assigned clients
        synchronized (clientModificationsLock) {
          updateClients();
        }
        
        // Send the notifications from the core's client queues
        sendNotifications();

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
      LOG.error("Server Dispatcher died", e);
    } finally {
      workers.shutdown();
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
    LOG.info("Assigning client " + clientId + " ...");
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
    LOG.info("Removing client " + clientId + " ...");
    synchronized (clientModificationsLock) {
      removedClients.add(clientId);
    }
  }
  
  
  private void sendNotifications() {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Sending notifications ...");
    }
    
    List<Long> shuffledClients = new ArrayList<Long>(assignedClients);
    Collections.shuffle(shuffledClients);
    
    for (Long clientId : shuffledClients) {
      Future<DispatchStatus> clientFuture =
          clientsFuture.get(clientId);
      DispatchStatus status;

      if (!shouldSendNotifications(clientId)) {
        continue;
      }

      // Get the results of the last batch notification sending
      // for this client.
      if (clientFuture != null && clientFuture.isDone()) {
        try {
          status = clientFuture.get();
        } catch (InterruptedException e) {
          LOG.error("Got interrupted for client " + clientId, e);
          core.shutdown();
          return;
        } catch (ExecutionException e) {
          LOG.error("got exception in clientFuture.get() for client " +
              clientId, e);
          core.shutdown();
          return;
        }
        
        if (status.action == DispatchStatus.Action.DISPATCH_SUCCESSFUL) {
          handleSuccessfulDispatch(clientId, status.time);
        }
        if (status.action == DispatchStatus.Action.DISPATCH_FAILED) {
          handleFailedDispatch(clientId, status.time);
        }
      }
      
      // Submit a new batch notification sending callable for this client
      if (clientFuture == null ||
          (clientFuture != null && clientFuture.isDone())) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Submiting for client " + clientId);
        }
        try {
          clientFuture = workers.submit(new NotificationsSender(clientId));
        } catch (RejectedExecutionException e) {
          LOG.warn("Failed to submit task", e);
          continue;
        }
        clientsFuture.put(clientId, clientFuture);
      }

    }

    if (LOG.isDebugEnabled()) {
      LOG.debug("Done sending notifications.");
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
    ClientData clientData = core.getClientData(clientId);
    long prevDiffTime, curDiffTime;
    if (clientData == null) {
      return false;
    }
    
    // If it's not marked as failed, we should send right away
    if (clientData.markedAsFailedTime == -1) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Client " + clientId + " not failed.");
      }
      return true;
    }
    
    prevDiffTime = clientData.lastSentTime - clientData.markedAsFailedTime;
    curDiffTime = System.currentTimeMillis() - clientData.markedAsFailedTime;
    if (curDiffTime > 2 * prevDiffTime) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Client " + clientData + " failed (returning true)");
      }
      return true;
    }
    
    if (LOG.isDebugEnabled()) {
      LOG.debug("Client " + clientData + " failed (returning false)");
    }
    return false;
  }
  
  
  private DispatchStatus sendHeartbeat(ClientData clientData) {
    long currentTime = System.currentTimeMillis();
    DispatchStatus status = new DispatchStatus();
    
    status.time = currentTime;
    if (currentTime - clientData.lastSentTime > heartbeatTimeout) {
      try {
        clientData.handler.heartbeat(core.getId());
        if (LOG.isDebugEnabled()) {
          LOG.debug("Successfully sent heartbeat to " + clientData);
        }
        core.getMetrics().heartbeats.inc();
        status.action = DispatchStatus.Action.DISPATCH_SUCCESSFUL;
      } catch (TTransportException e) {
        LOG.warn("Failed to heartbeat " + clientData, e);
        if (e.getType() != TTransportException.TIMED_OUT) {
          core.removeClient(clientData.id);
        }
        status.action = DispatchStatus.Action.DISPATCH_FAILED;
      } catch (InvalidServerIdException e) {
        LOG.warn("Client removed this server " + clientData, e);
        status.action = DispatchStatus.Action.DISPATCH_FAILED;
      } catch (TException e) {
        LOG.warn("Failed to heartbeat " + clientData, e);
        core.removeClient(clientData.id);
        status.action = DispatchStatus.Action.DISPATCH_FAILED;
      } 
    }
    
    return status;
  }
  
  
  private DispatchStatus sendNotification(ClientData clientData,
      NamespaceNotification notification) {
    DispatchStatus status = new DispatchStatus();
    if (LOG.isDebugEnabled()) {
      LOG.debug("Sending " + NotifierUtils.asString(notification) +
          " to client " + clientData + " ...");
    }
    
    status.time = System.currentTimeMillis();
    try {
      clientData.handler.handleNotification(notification, core.getId());
      if (LOG.isDebugEnabled()) {
        LOG.debug("Successfully sent " + NotifierUtils.asString(notification) +
            " to " + clientData);
      }
      core.getMetrics().dispatchedNotifications.inc();
      status.action = DispatchStatus.Action.DISPATCH_SUCCESSFUL;
    } catch (TTransportException e) {
      LOG.warn("Failed to send notification to " + clientData, e);
      if (e.getType() != TTransportException.TIMED_OUT) {
        core.removeClient(clientData.id);
      }
      status.action = DispatchStatus.Action.DISPATCH_FAILED;
      core.getMetrics().failedNotifications.inc();
    } catch (InvalidServerIdException e) {
      LOG.warn("Client removed this server " + clientData, e);
      status.action = DispatchStatus.Action.DISPATCH_FAILED;
    } catch (TException e) {
      LOG.warn("Failed to send notification to " + clientData, e);
      core.removeClient(clientData.id);
      status.action = DispatchStatus.Action.DISPATCH_FAILED;
      core.getMetrics().failedNotifications.inc();
    }
    
    return status;
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
   * Called each time a handleNotification or heartbeat Thrift
   * call fails.
   * @param clientId the id of the client on which the call failed.
   * @param failedTime the time at which the call was made. If
   *        this is -1, then nothing will be updated. 
   */
  @Override
  public void handleFailedDispatch(long clientId, long failedTime) {
    ClientData clientData = core.getClientData(clientId);
    if (failedTime == -1 || clientData == null)
      return;
    
    // We only add it and don't update it because we are interested in
    // keeping track of the first moment it failed
    if (clientData.markedAsFailedTime == -1) {
      clientData.markedAsFailedTime = failedTime;
      LOG.info("Marked client " + clientId + " as failed at " + failedTime);
    }

    clientData.lastSentTime = failedTime;
  }
  
  
  /**
   * Called each time a handleNotification or heartbeat Thrift
   * call is successful.
   * @param clientId the id of the client on which the call was successful.
   * @param sentTime the time at which the call was made. Nothing
   *        happens if this is -1.
   */
  @Override
  public void handleSuccessfulDispatch(long clientId, long sentTime) {
    ClientData clientData = core.getClientData(clientId);
    if (sentTime == -1 || clientData == null)
      return;
    
    clientData.markedAsFailedTime = -1;
    if (clientData.markedAsFailedTime != -1) {
      LOG.info("Unmarking " + clientId + " at " + sentTime);
    }
    
    clientData.lastSentTime = sentTime;
  }
  
  
  /**
   * Must be called with the clientsModificationLock hold.
   */
  private void updateClients() {
    assignedClients.addAll(newlyAssignedClients);
    assignedClients.removeAll(removedClients);
    
    newlyAssignedClients.clear();
    removedClients.clear();
  }
  
  
  private void checkClientTimeout() {
    long currentTime = System.currentTimeMillis();
    if (LOG.isDebugEnabled()) {
      LOG.debug("Cleaning failed clients ...");
    }

    for (Long clientId : assignedClients) {
      ClientData clientData = core.getClientData(clientId);
      if (clientData == null || clientData.markedAsFailedTime == -1) {
        continue;
      }
      
      if (currentTime - clientData.markedAsFailedTime > clientTimeout) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Failed client detected " + clientData);
        }
        core.removeClient(clientId);
        core.getMetrics().failedClients.inc();
      }
    }
  }
  
  
  /**
   * Batch sends a group of notifications for a given client.
   */
  private class NotificationsSender implements Callable<DispatchStatus> {
    long clientId;
    
    NotificationsSender(long clientId) {
      this.clientId = clientId;
    }
    
    @Override
    public DispatchStatus call() throws Exception {
      DispatchStatus status;
      Queue<NamespaceNotification> queuedNotifications =
          core.getClientNotificationQueue(clientId);
      ClientData clientData = core.getClientData(clientId);
      if (clientData == null || queuedNotifications == null) {
        // Client removed meanwhile (or is being removed)
        return new DispatchStatus();
      }
      
      if (queuedNotifications.isEmpty()) {
        status = sendHeartbeat(clientData);
      } else {
        status = sendNotification(clientData, queuedNotifications.peek());
        if (status.action == DispatchStatus.Action.DISPATCH_SUCCESSFUL) {
          queuedNotifications.poll();
        }
      }
      return status;
    }
    
  }
  
  private static class DispatchStatus {
    enum Action {
      NOTHING,
      DISPATCH_SUCCESSFUL,
      DISPATCH_FAILED
    };
    
    Action action = Action.NOTHING;
    long time = -1;
  }
}
