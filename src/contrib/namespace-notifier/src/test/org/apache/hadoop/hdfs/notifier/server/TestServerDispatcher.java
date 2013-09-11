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
import java.util.Queue;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;

import junit.framework.Assert;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.notifier.EventType;
import org.apache.hadoop.hdfs.notifier.InvalidServerIdException;
import org.apache.hadoop.hdfs.notifier.NamespaceEvent;
import org.apache.hadoop.hdfs.notifier.NamespaceEventKey;
import org.apache.hadoop.hdfs.notifier.NamespaceNotification;
import org.apache.thrift.TException;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestServerDispatcher {
  static private Logger LOG = LoggerFactory.getLogger(TestServerDispatcher.class);

  static Configuration conf;
  
  @BeforeClass
  public static void initConf() {
    conf = NotifierTestUtil.initGenericConf();
  }
  
  @Test
  public void testBasicDispatch() throws Exception {
    DummyServerCore core = new DummyServerCore();
    ServerDispatcher dispatcher = new ServerDispatcher(core);
    long clientId = 1000;
    DummyClientHandler handler = new DummyClientHandler();
    NamespaceEvent event = new NamespaceEvent("/a",
        EventType.FILE_ADDED.getByteValue());
    Set<Long> subscriptions = new HashSet<Long>();
    Thread dispatcherThread = new Thread(dispatcher);
    
    // Add only one client with one subscription
    subscriptions.add(clientId);
    core.clientQueues.put(clientId,
        new ConcurrentLinkedQueue<NamespaceNotification>());
    core.subscriptions.put(new NamespaceEventKey(event), subscriptions);
    core.clients.put(clientId, new ClientData(clientId, handler, "host", 3000));
    dispatcher.loopSleepTime = 20;
    dispatcher.assignClient(clientId);
    
    // Add a notification and wait for it to be delivered
    dispatcherThread.start();
    core.clientQueues.get(clientId).add(new NamespaceNotification("/a/b",
        EventType.FILE_ADDED.getByteValue(), 10));
    Thread.sleep(250);
    core.shutdown();
    dispatcherThread.join();
    
    // Check what was received
    NamespaceNotification n;
    Assert.assertEquals(1, handler.notificationQueue.size());
    n = handler.notificationQueue.element();
    Assert.assertEquals("/a/b", n.path);
    Assert.assertEquals(EventType.FILE_ADDED.getByteValue(), n.type);
    Assert.assertEquals(10, n.txId);
  }
  
  
  @Test
  public void testDispatchOrder() throws Exception {
    DummyServerCore core = new DummyServerCore();
    ServerDispatcher dispatcher = new ServerDispatcher(core);
    long clientId = 1000;
    DummyClientHandler handler = new DummyClientHandler();
    NamespaceEvent event = new NamespaceEvent("/a",
        EventType.FILE_ADDED.getByteValue());
    NamespaceEvent event2 = new NamespaceEvent("/a/c",
        EventType.FILE_CLOSED.getByteValue());
    Set<Long> subscriptions = new HashSet<Long>();
    Thread dispatcherThread = new Thread(dispatcher);
    
    // Add only one client with one subscription
    subscriptions.add(clientId);
    core.clientQueues.put(clientId,
        new ConcurrentLinkedQueue<NamespaceNotification>());
    core.subscriptions.put(new NamespaceEventKey(event), subscriptions);
    core.subscriptions.put(new NamespaceEventKey(event2), subscriptions);
    core.clients.put(clientId, new ClientData(clientId, handler, "host", 3000));
    dispatcher.loopSleepTime = 20;
    dispatcher.assignClient(clientId);
    
    dispatcherThread.start();
    core.clientQueues.get(clientId).add(new NamespaceNotification("/a/a",
        EventType.FILE_ADDED.getByteValue(), 10));
    core.clientQueues.get(clientId).add(new NamespaceNotification("/a/b",
        EventType.FILE_ADDED.getByteValue(), 20));
    core.clientQueues.get(clientId).add(new NamespaceNotification("/a/c",
        EventType.FILE_ADDED.getByteValue(), 30));
    core.clientQueues.get(clientId).add(new NamespaceNotification("/a/c",
        EventType.FILE_CLOSED.getByteValue(), 40));
    Thread.sleep(250);
    core.shutdown();
    dispatcherThread.join();
    
    // Check what was received
    NamespaceNotification n;
    Assert.assertEquals(4, handler.notificationQueue.size());
    
    n = handler.notificationQueue.poll();
    Assert.assertEquals("/a/a", n.path);
    Assert.assertEquals(EventType.FILE_ADDED.getByteValue(), n.type);
    Assert.assertEquals(10, n.txId);
    n = handler.notificationQueue.poll();
    Assert.assertEquals("/a/b", n.path);
    Assert.assertEquals(EventType.FILE_ADDED.getByteValue(), n.type);
    Assert.assertEquals(20, n.txId);
    n = handler.notificationQueue.poll();
    Assert.assertEquals("/a/c", n.path);
    Assert.assertEquals(EventType.FILE_ADDED.getByteValue(), n.type);
    Assert.assertEquals(30, n.txId);
    n = handler.notificationQueue.poll();
    Assert.assertEquals("/a/c", n.path);
    Assert.assertEquals(EventType.FILE_CLOSED.getByteValue(), n.type);
    Assert.assertEquals(40, n.txId);
  }
  
  
  @Test
  public void testDispatchOrderClientFailing() throws Exception {
    DummyServerCore core = new DummyServerCore();
    ServerDispatcher dispatcher = new ServerDispatcher(core);
    long clientId = 1000;
    DummyClientHandler handler = new DummyClientHandler();
    NamespaceEvent event = new NamespaceEvent("/a",
        EventType.FILE_ADDED.getByteValue());
    NamespaceEvent event2 = new NamespaceEvent("/a/c",
        EventType.FILE_CLOSED.getByteValue());
    Set<Long> subscriptions = new HashSet<Long>();
    Thread dispatcherThread = new Thread(dispatcher);
    
    // Add only one client with one subscription
    subscriptions.add(clientId);
    core.clientQueues.put(clientId,
        new ConcurrentLinkedQueue<NamespaceNotification>());
    core.subscriptions.put(new NamespaceEventKey(event), subscriptions);
    core.subscriptions.put(new NamespaceEventKey(event2), subscriptions);
    core.clients.put(clientId, new ClientData(clientId, handler, "host", 3000));
    dispatcher.assignClient(clientId);
    dispatcher.loopSleepTime = 20;
    
    handler.failChance = 0.8f;
    handler.failChanceDec = 0.1f;
    
    dispatcherThread.start();
    core.clientQueues.get(clientId).add(new NamespaceNotification("/a/a",
        EventType.FILE_ADDED.getByteValue(), 10));
    core.clientQueues.get(clientId).add(new NamespaceNotification("/a/b",
        EventType.FILE_ADDED.getByteValue(), 20));
    core.clientQueues.get(clientId).add(new NamespaceNotification("/a/c",
        EventType.FILE_ADDED.getByteValue(), 30));
    core.clientQueues.get(clientId).add(new NamespaceNotification("/a/c",
        EventType.FILE_CLOSED.getByteValue(), 40));
    Thread.sleep(1000);
    core.shutdown();
    dispatcherThread.join();
    
    // Check what was received
    NamespaceNotification n;
    Assert.assertEquals(4, handler.notificationQueue.size());
    
    n = handler.notificationQueue.poll();
    Assert.assertEquals("/a/a", n.path);
    Assert.assertEquals(EventType.FILE_ADDED.getByteValue(), n.type);
    Assert.assertEquals(10, n.txId);
    n = handler.notificationQueue.poll();
    Assert.assertEquals("/a/b", n.path);
    Assert.assertEquals(EventType.FILE_ADDED.getByteValue(), n.type);
    Assert.assertEquals(20, n.txId);
    n = handler.notificationQueue.poll();
    Assert.assertEquals("/a/c", n.path);
    Assert.assertEquals(EventType.FILE_ADDED.getByteValue(), n.type);
    Assert.assertEquals(30, n.txId);
    n = handler.notificationQueue.poll();
    Assert.assertEquals("/a/c", n.path);
    Assert.assertEquals(EventType.FILE_CLOSED.getByteValue(), n.type);
    Assert.assertEquals(40, n.txId);
  }
  
  
  @Test
  public void testMultipleClients() throws Exception {
    DummyServerCore core = new DummyServerCore();
    ServerDispatcher dispatcher = new ServerDispatcher(core);
    long client1Id = 1000, client2Id = 2000;
    DummyClientHandler handler1 = new DummyClientHandler(),
        handler2 = new DummyClientHandler();
    NamespaceEvent event1 = new NamespaceEvent("/a",
        EventType.FILE_ADDED.getByteValue());
    NamespaceEvent event2 = new NamespaceEvent("/b",
        EventType.FILE_ADDED.getByteValue());
    NamespaceEvent eventCommon = new NamespaceEvent("/c",
        EventType.FILE_ADDED.getByteValue());
    Set<Long> subscriptions = new HashSet<Long>();
    Thread dispatcherThread = new Thread(dispatcher);
    
    // Add only one client with one subscription
    subscriptions.add(client1Id);
    subscriptions.add(client2Id);
    core.clientQueues.put(client1Id,
        new ConcurrentLinkedQueue<NamespaceNotification>());
    core.clientQueues.put(client2Id,
        new ConcurrentLinkedQueue<NamespaceNotification>());
    core.subscriptions.put(new NamespaceEventKey(eventCommon), subscriptions);
    subscriptions = new HashSet<Long>();
    subscriptions.add(client1Id);
    core.subscriptions.put(new NamespaceEventKey(event1), subscriptions);
    subscriptions = new HashSet<Long>();
    subscriptions.add(client2Id);
    core.subscriptions.put(new NamespaceEventKey(event2), subscriptions);
    core.clients.put(client1Id, new ClientData(client1Id, handler1, "host", 3000));
    core.clients.put(client2Id, new ClientData(client2Id, handler2, "host", 3000));
    dispatcher.assignClient(client1Id);
    dispatcher.assignClient(client2Id);
    dispatcher.loopSleepTime = 1;
    
    handler1.failChance = 0.8f;
    handler1.failChanceDec = 0.2f;
    handler2.failChance = 0.8f;
    handler2.failChanceDec = 0.2f;
    
    dispatcherThread.start();
    
    Random generator = new Random();
    String[] basePaths = {"a", "b", "c"};
    Queue<Long> client1ExpectedTxIds = new LinkedList<Long>();
    Queue<Long> client2ExpectedTxIds = new LinkedList<Long>();
    for (long txId = 0; txId < 3000; txId ++) {
      String basePath = basePaths[generator.nextInt(3)];
      
      if (basePath.equals("a") || basePath.equals("c")) {
        core.clientQueues.get(client1Id).add(new NamespaceNotification("/" + basePath +
            "/" + txId, EventType.FILE_ADDED.getByteValue(), txId));
        client1ExpectedTxIds.add(txId);
      }
      if (basePath.equals("b") || basePath.equals("c")) {
        core.clientQueues.get(client2Id).add(new NamespaceNotification("/" + basePath +
            "/" + txId, EventType.FILE_ADDED.getByteValue(), txId));
        client2ExpectedTxIds.add(txId);
      }
    }

    Thread.sleep(2500);
    core.shutdown();
    dispatcherThread.join();
    
    // Check for client 1
    Assert.assertEquals(client1ExpectedTxIds.size(),
        handler1.notificationQueue.size());
    while (!client1ExpectedTxIds.isEmpty()) {
      Long expectedTxId = client1ExpectedTxIds.poll();
      Long receivedTxId = handler1.notificationQueue.poll().txId;
      Assert.assertEquals(expectedTxId, receivedTxId);
    }
    // Check for client 2
    Assert.assertEquals(client2ExpectedTxIds.size(),
        handler2.notificationQueue.size());
    while (!client2ExpectedTxIds.isEmpty()) {
      Long expectedTxId = client2ExpectedTxIds.poll();
      Long receivedTxId = handler2.notificationQueue.poll().txId;
      Assert.assertEquals(expectedTxId, receivedTxId);
    }
  }
  
  
  @Test
  public void testDispatchFailing() throws Exception {
    DummyServerCore core = new DummyServerCore();
    ServerDispatcher dispatcher = new ServerDispatcher(core);
    long clientId = 1000;
    DummyClientHandler handler = new DummyClientHandler();
    NamespaceEvent event = new NamespaceEvent("/a",
        EventType.FILE_ADDED.getByteValue());
    NamespaceEvent event2 = new NamespaceEvent("/a/c",
        EventType.FILE_CLOSED.getByteValue());
    Set<Long> subscriptions = new HashSet<Long>();
    Thread dispatcherThread = new Thread(dispatcher);
    
    // Add only one client with one subscription
    subscriptions.add(clientId);
    core.clientQueues.put(clientId,
        new ConcurrentLinkedQueue<NamespaceNotification>());
    core.subscriptions.put(new NamespaceEventKey(event), subscriptions);
    core.subscriptions.put(new NamespaceEventKey(event2), subscriptions);
    core.clients.put(clientId, new ClientData(clientId, handler, "host", 3000));
    dispatcher.loopSleepTime = 20;
    
    handler.failChance = 1.0f;
    handler.failChanceDec = 1.0f;
    
    dispatcherThread.start();
    core.clientQueues.get(clientId).add(new NamespaceNotification("/a/a",
        EventType.FILE_ADDED.getByteValue(), 10));
    core.clientQueues.get(clientId).add(new NamespaceNotification("/a/b",
        EventType.FILE_ADDED.getByteValue(), 20));
    core.clientQueues.get(clientId).add(new NamespaceNotification("/a/c",
        EventType.FILE_ADDED.getByteValue(), 30));
    core.clientQueues.get(clientId).add(new NamespaceNotification("/a/c",
        EventType.FILE_CLOSED.getByteValue(), 40));
    Thread.sleep(140);
    core.shutdown();
    dispatcherThread.join();
    
    // Since we didn't assigned the client to this dispatcher, we
    // shouldn't receive any notifications
    Assert.assertEquals(0, handler.notificationQueue.size());
  }
  
  
  class DummyServerCore extends EmptyServerCore {
    
    ConcurrentMap<Long, ClientData> clients =
        new ConcurrentHashMap<Long, ClientData>();
    ConcurrentMap<Long, ConcurrentLinkedQueue<NamespaceNotification>> clientQueues =
        new ConcurrentHashMap<Long, ConcurrentLinkedQueue<NamespaceNotification>>();
    ConcurrentMap<NamespaceEventKey, Set<Long>> subscriptions =
        new ConcurrentHashMap<NamespaceEventKey, Set<Long>>();
    
    @Override
    public ClientData getClientData(long clientId) {
      return clients.get(clientId);
    }
    
    public Queue<NamespaceNotification> getClientNotificationQueue(long clientId) {
      return clientQueues.get(clientId);
    }
    
    @Override
    public Set<Long> getClientsForNotification(NamespaceNotification n) {
      Set<Long> clients = subscriptions.get(new NamespaceEventKey(n));
      if (clients == null)
        return new HashSet<Long>();
      return null;
    }
    
    @Override
    public IServerHistory getHistory() {
      return new EmptyServerHistory();
    }
    
    @Override
    public Configuration getConfiguration() {
      return conf;
    }
  }
  
  
  class DummyClientHandler extends EmptyClientHandler {
    
    float failChance = 0.0f;
    float failChanceDec = 0.0f;
    Random generator = new Random();
    
    // The order in which we received the notifications
    ConcurrentLinkedQueue<NamespaceNotification> notificationQueue =
        new ConcurrentLinkedQueue<NamespaceNotification>();
    
    @Override
    public void handleNotification(NamespaceNotification notification,
        String serverId) throws InvalidServerIdException, TException {
      float randomVal = generator.nextFloat();

      if (randomVal < failChance) {
        failChance -= failChanceDec;
        throw new TException("Randomly generated exception");
      }
      else {
        failChance -= failChanceDec;
        notificationQueue.add(notification);
      }
    }
  
  }
}
