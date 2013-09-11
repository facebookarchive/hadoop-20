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
package org.apache.hadoop.hdfs.notifier;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.thrift.TException;

public class ClientHandlerImpl implements ClientHandler.Iface {
  public static final Log LOG = LogFactory.getLog(ClientHandlerImpl.class);
  
  NamespaceNotifierClient client;
  
  public ClientHandlerImpl(NamespaceNotifierClient client) {
    this.client = client;
  }
  
  
  @Override
  public void handleNotification(NamespaceNotification notification,
      String serverId) throws InvalidServerIdException, TException {
    if (LOG.isDebugEnabled()) {
      LOG.debug(client.listeningPort + ": Received " +
          NotifierUtils.asString(notification) + " from " +
          "server " + serverId);
    }
    
    if (!serverId.equals(client.connectionManager.serverId)) {
      LOG.warn(client.listeningPort + ": Received notification, but not " +
          "connected to server " + serverId +
          ". Answering with InvalidServerIdException");
      throw new InvalidServerIdException();
    }
    
    String eventPath = NotifierUtils.getBasePath(notification);
    List<String> allAncestorPath = NotifierUtils.getAllAncestors(eventPath);
    List<NamespaceEventKey> ancestorKeys = new ArrayList<NamespaceEventKey>();
    for (String ancestorPath : allAncestorPath) {
      NamespaceEventKey eventKey = new NamespaceEventKey(ancestorPath,
          notification.type);
      if (client.watchedEvents.containsKey(eventKey)) {
        ancestorKeys.add(eventKey);
      }
    }
    
    boolean duplicated = false;
    for (NamespaceEventKey eventKey : ancestorKeys) {
      if (client.watchedEvents.get(eventKey) == notification.txId) {
        duplicated = true;
        LOG.warn(client.listeningPort + ": Received duplicate for txId=" +
            notification.txId);
        break;
      }
    }
    if (!duplicated) {
      try {
        client.watcher.handleNamespaceNotification(notification);
      } catch (Exception e) {
        LOG.warn(client.listeningPort +
            ": wather.handleNamespaceNotification failed", e);
        throw new TException(e);
      }
      for (NamespaceEventKey eventKey : ancestorKeys) {
        client.watchedEvents.put(eventKey, notification.txId);
      }
    }
    
    client.connectionManager.tracker.messageReceived();
  }

  
  @Override
  public void heartbeat(String serverId) throws InvalidServerIdException,
      TException {
    LOG.info(client.listeningPort + ": Received heartbeat from server " +
        serverId);
    if (!serverId.equals(client.connectionManager.serverId)) {
      LOG.warn(client.listeningPort + ": Not connected to server " + serverId +
          ". Answering with InvalidServerIdException");
      throw new InvalidServerIdException();
    }
    
    client.connectionManager.tracker.messageReceived();
  }
  
  
  @Override
  public void registerServer(long clientId, String serverId, long token)
      throws InvalidTokenException, TException {
    LOG.info(client.listeningPort + ": registerServer called with clientId=" +
        clientId + " serverId=" + serverId + " token=" + token);
    if (token != client.getCurrentConnectionToken()) {
      LOG.warn(client.listeningPort + ": registerServer called with bad" +
          " token. Expected: " + token);
      throw new InvalidTokenException();
    }
    LOG.info(client.listeningPort + ": Token accepted. Saving client" +
        " and server id ...");
    client.connectionManager.id = clientId;
    client.connectionManager.serverId = serverId;
  }
 
  
}
