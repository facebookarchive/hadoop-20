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
import org.apache.hadoop.hdfs.notifier.NamespaceEvent;
import org.apache.hadoop.hdfs.notifier.RampUpException;
import org.apache.hadoop.hdfs.notifier.ServerHandler;
import org.apache.hadoop.hdfs.notifier.TransactionIdTooOldException;
import org.apache.thrift.TException;

public class EmptyServerHandler implements ServerHandler.Iface {

  @Override
  public void subscribe(long clientId, NamespaceEvent subscribedEvent, long txId)
      throws InvalidClientIdException, TransactionIdTooOldException, TException {}

  @Override
  public void unsubscribe(long clientId, NamespaceEvent subscribedEvent)
      throws InvalidClientIdException, ClientNotSubscribedException, TException {}

  @Override
  public void registerClient(String host, int port, long token)
      throws RampUpException, ClientConnectionException, TException {}

  @Override
  public void unregisterClient(long clientId) throws InvalidClientIdException,
      TException {}
  
}
