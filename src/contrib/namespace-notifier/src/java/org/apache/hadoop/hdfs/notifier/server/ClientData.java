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

import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.hadoop.hdfs.notifier.ClientHandler;
import org.apache.hadoop.hdfs.notifier.NamespaceNotification;

public class ClientData {
  final String host;
  final int port;
  
  // The asssigned id for this client
  final long id;
  
  // The thrift handler for this client
  final ClientHandler.Iface handler;
  
  // The time on which we sent the client the last notification
  long lastSentTime = System.currentTimeMillis();
  
  // The time on which the client was marked as failed (or -1 if the
  // client isn't marked as failed)
  long markedAsFailedTime = -1;
  
  // The list with all the subscription sets in which this client appears
  // for easier removal
  List<Set<Long>> subscriptions = new LinkedList<Set<Long>>();
  
  // The queue with notifications for this client
  ConcurrentLinkedQueue<NamespaceNotification> queue =
      new ConcurrentLinkedQueue<NamespaceNotification>();
  
  ClientData(long id, ClientHandler.Iface handler, String host, int port) {
    this.id = id;
    this.handler = handler;
    this.host = host;
    this.port = port;
  }
  
  @Override
  public String toString() {
    return "[id=" + id + ";host=" + host + ";port=" + port + "]";
  }
}