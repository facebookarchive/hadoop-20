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

import java.io.IOException;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.locks.Lock;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.notifier.ClientNotSubscribedException;
import org.apache.hadoop.hdfs.notifier.InvalidClientIdException;
import org.apache.hadoop.hdfs.notifier.NamespaceEvent;
import org.apache.hadoop.hdfs.notifier.NamespaceNotification;
import org.apache.hadoop.hdfs.notifier.ServerHandler;
import org.apache.hadoop.hdfs.notifier.TransactionIdTooOldException;
import org.apache.hadoop.hdfs.notifier.server.metrics.NamespaceNotifierMetrics;
import org.apache.thrift.transport.TTransportException;

public interface IServerCore extends Runnable {

  public void init(IServerLogReader logReader, IServerHistory serverHistory,
      IServerDispatcher dispatcher, ServerHandler.Iface handler);
  
  public NamespaceNotifierMetrics getMetrics();
  
  public void shutdown();
  
  public String getServiceName();
  
  public boolean shutdownPending();
  
  public void join();
  
  public Configuration getConfiguration();
  
  public void handleNotification(NamespaceNotification n);
  
  public long addClientAndConnect(String host, int port)
      throws TTransportException, IOException;
  
  public void addClient(ClientData clientData);
  
  public boolean removeClient(long clientId);
  
  public boolean isRegistered(long clientId);
  
  public ClientData getClientData(long clientId);
  
  public Set<Long> getClientsForNotification(NamespaceNotification n);
  
  public Set<Long> getClients();
  
  public void subscribeClient(long clientId, NamespaceEvent event, long txId) 
      throws TransactionIdTooOldException, InvalidClientIdException;
  
  public void unsubscribeClient(long clientId, NamespaceEvent event) 
      throws ClientNotSubscribedException, InvalidClientIdException;
  
  public String getId();
  
  public Queue<NamespaceNotification> getClientNotificationQueue(long clientId);
  
  public IServerHistory getHistory();
}
