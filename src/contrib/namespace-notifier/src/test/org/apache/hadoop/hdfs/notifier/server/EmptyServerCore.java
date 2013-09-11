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
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.notifier.ClientNotSubscribedException;
import org.apache.hadoop.hdfs.notifier.InvalidClientIdException;
import org.apache.hadoop.hdfs.notifier.NamespaceEvent;
import org.apache.hadoop.hdfs.notifier.NamespaceNotification;
import org.apache.hadoop.hdfs.notifier.TransactionIdTooOldException;
import org.apache.hadoop.hdfs.notifier.ServerHandler.Iface;
import org.apache.hadoop.hdfs.notifier.server.metrics.NamespaceNotifierMetrics;
import org.apache.thrift.transport.TTransportException;


/**
 * Inherited by dummy IServerCore classes in tests to avoid the need
 * of having to write empty method implementations.
 */
class EmptyServerCore implements IServerCore {
  
  Configuration conf = new Configuration();

  AtomicBoolean shouldShutdown = new AtomicBoolean(false);
  NamespaceNotifierMetrics metrics =
      new NamespaceNotifierMetrics(new Configuration(), "");
  
  @Override
  public void run() {}

  @Override
  public void init(IServerLogReader logReader, IServerHistory serverHistory,
      IServerDispatcher dispatcher, Iface handler) {}

  @Override
  public void shutdown() {
    shouldShutdown.set(true);
  }
  
  @Override
  public String getServiceName() {
    return "";
  }
  
  @Override 
  public void join() {
  }

  public void setConfiguration(Configuration conf) {
    this.conf = conf;
  }
  
  @Override
  public Configuration getConfiguration() {
    return conf;
  }

  @Override
  public long addClientAndConnect(String host, int port)
      throws TTransportException, IOException {
    return 0;
  }

  @Override
  public void handleNotification(NamespaceNotification n) {}

  @Override
  public void addClient(ClientData clientData) {}

  @Override
  public boolean removeClient(long clientId) {
    return false;
  }

  @Override
  public boolean isRegistered(long clientId) {
    return false;
  }

  @Override
  public ClientData getClientData(long clientId) {
    return null;
  }

  @Override
  public Set<Long> getClientsForNotification(NamespaceNotification n) {
    return null;
  }

  @Override
  public Set<Long> getClients() {
    return null;
  }

  @Override
  public void subscribeClient(long clientId, NamespaceEvent event, long txId)
      throws TransactionIdTooOldException, InvalidClientIdException {}

  @Override
  public void unsubscribeClient(long clientId, NamespaceEvent event)
      throws ClientNotSubscribedException {}

  @Override
  public String getId() {
    return "";
  }

  @Override
  public Queue<NamespaceNotification> getClientNotificationQueue(long clientId) {
    return null;
  }

  @Override
  public IServerHistory getHistory() {
    return null;
  }
  
  @Override
  public boolean shutdownPending() {
    return shouldShutdown.get();
  }

  @Override
  public NamespaceNotifierMetrics getMetrics() {
    return metrics;
  }

}
