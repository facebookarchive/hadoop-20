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

import org.apache.hadoop.hdfs.notifier.ClientConnectionException;
import org.apache.hadoop.hdfs.notifier.ClientNotSubscribedException;
import org.apache.hadoop.hdfs.notifier.InvalidClientIdException;
import org.apache.hadoop.hdfs.notifier.InvalidTokenException;
import org.apache.hadoop.hdfs.notifier.NamespaceEvent;
import org.apache.hadoop.hdfs.notifier.RampUpException;
import org.apache.hadoop.hdfs.notifier.ServerHandler;
import org.apache.hadoop.hdfs.notifier.TransactionIdTooOldException;
import org.apache.thrift.TException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;


public class ServerHandlerImpl implements ServerHandler.Iface {
  public static final Log LOG = LogFactory.getLog(ServerHandlerImpl.class);

  private IServerCore core;
  
  public ServerHandlerImpl(IServerCore core) {
    this.core = core;
  }
  
  @Override
  public void subscribe(long clientId, NamespaceEvent subscribedEvent,
      long txId) throws InvalidClientIdException, TransactionIdTooOldException,
      TException {
    core.getMetrics().subscribeCalls.inc();
    if (!core.isRegistered(clientId)) {
      throw new InvalidClientIdException("Invalid client id: " + clientId);
    }
    core.subscribeClient(clientId, subscribedEvent, txId);
  }

  @Override
  public void unsubscribe(long clientId, NamespaceEvent subscribedEvent)
      throws InvalidClientIdException, TException, ClientNotSubscribedException {
    core.getMetrics().unsubscribeCalls.inc();
    if (!core.isRegistered(clientId)) {
      throw new InvalidClientIdException("Invalid client id: " + clientId);
    }
    
    core.unsubscribeClient(clientId, subscribedEvent);
  }


  @Override
  public void registerClient(String host, int port, long token)
      throws RampUpException, TException, ClientConnectionException {
    core.getMetrics().registerClientCalls.inc();
    if (core.getHistory().isRampUp()) {
      throw new RampUpException("Currently in ramp-up phase.");
    }
    
    // Add the client to our internal data structures
    ClientData clientData;
    long clientId;
    try {
      clientId = core.addClientAndConnect(host, port);
    } catch (Exception e) {
      throw new ClientConnectionException("Failed to connect to client " 
          + " on host " + host + " and port " + port);
    }
    clientData = core.getClientData(clientId);
    
    try {
      clientData.handler.registerServer(clientId, core.getId(), token);
    } catch (InvalidTokenException e) {
      LOG.warn("Client rejected our token. clientId = " + clientId +
          ". token = " + token, e);
      
      // Remove the client from our internal structures
      core.removeClient(clientId);
      
      // Raise an exception here since the client rejected us
      throw new ClientConnectionException("Token rejected");
    }
  }

  @Override
  public void unregisterClient(long clientId) throws InvalidClientIdException,
      TException {
    core.getMetrics().unregisterClientCalls.inc();
    if (!core.removeClient(clientId)) {
      LOG.warn("Invalid request to remove client with id: " + clientId);
      throw new InvalidClientIdException("Invalid client id: " + clientId);
    }
  }

}
