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
import java.util.Queue;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;

import junit.framework.Assert;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.notifier.EventType;
import org.apache.hadoop.hdfs.notifier.NamespaceEvent;
import org.apache.hadoop.hdfs.notifier.NamespaceNotification;
import org.apache.hadoop.hdfs.notifier.TransactionIdTooOldException;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestServerCore {
  static private Logger LOG = LoggerFactory.getLogger(TestServerCore.class);

  static Configuration conf;
  
  @BeforeClass
  public static void initConf() {
    Configuration.addDefaultResource("namespace-notifier-server-default.xml");
    Configuration.addDefaultResource("hdfs-default.xml");
    conf = new Configuration();
    conf.addResource("namespace-notifier-server-site.xml");
    conf.addResource("hdfs-site.xml");
    conf.set(ServerCore.SERVER_ID, "42");
  }
  
  @Test
  public void testClientOperations() throws Exception {
    ServerCore core = new ServerCore(conf);
    List<IServerDispatcher> dispatchers = new LinkedList<IServerDispatcher>();
    dispatchers.add(new EmptyServerDispatcher());
    core.init(new EmptyServerLogReader(), new EmptyServerHistory(),
        dispatchers, new EmptyServerHandler());
    long clientId = 1000;
    EmptyClientHandler clientObj = new EmptyClientHandler();
    
    core.addClient(clientId, clientObj);
    Assert.assertTrue(core.isRegistered(clientId));
    Assert.assertEquals(clientObj, core.getClient(clientId));
    Assert.assertEquals(1, core.getClients().size());
    core.removeClient(clientId);
    Assert.assertFalse(core.isRegistered(clientId));
    Assert.assertEquals(0, core.getClients().size());
  }
  

  @Test
  public void testSubscription() throws Exception {
    ServerCore core = new ServerCore(conf);
    List<IServerDispatcher> dispatchers = new LinkedList<IServerDispatcher>();
    dispatchers.add(new EmptyServerDispatcher());
    core.init(new EmptyServerLogReader(), new TestSubscriptionHistory(),
        dispatchers, new EmptyServerHandler());
    long client1Id = 1000, client2Id = 2000;
    EmptyClientHandler client1Obj = new EmptyClientHandler(),
        client2Obj = new EmptyClientHandler();
    NamespaceEvent event = new NamespaceEvent("/a",
        EventType.FILE_ADDED.getByteValue());
    
    core.addClient(client1Id, client1Obj);
    core.addClient(client2Id, client2Obj);
    
    Set<Long> clientsForNotification;
    core.subscribeClient(client1Id, event, -1);
    clientsForNotification = core.getClientsForNotification(
        new NamespaceNotification("/a/b", event.type, 10));
    Assert.assertEquals(1, clientsForNotification.size());
    Assert.assertTrue(clientsForNotification.contains(client1Id));
    
    core.subscribeClient(client2Id, event, -1);
    clientsForNotification = core.getClientsForNotification(
        new NamespaceNotification("/a/b", event.type, 10));
    Assert.assertEquals(2, clientsForNotification.size());
    Assert.assertTrue(clientsForNotification.contains(client1Id));
    Assert.assertTrue(clientsForNotification.contains(client2Id));
    
    core.unsubscribeClient(client1Id, event);
    clientsForNotification = core.getClientsForNotification(
        new NamespaceNotification("/a/b", event.type, 10));
    Assert.assertEquals(1, clientsForNotification.size());
    Assert.assertTrue(clientsForNotification.contains(client2Id));
    
    core.unsubscribeClient(client2Id, event);
    clientsForNotification = core.getClientsForNotification(
        new NamespaceNotification("/a/b", event.type, 10));
    if (clientsForNotification != null)
      Assert.assertEquals(0, clientsForNotification.size());
    
    // Test that TransactionIdTooOldException is thrown
    try {
      core.subscribeClient(client1Id, event, -2);
      // We should get a transaction id too old exception
      Assert.fail();
    } catch (TransactionIdTooOldException e) {}
    
    // Test that the notifications are queued correctly
    core.subscribeClient(client1Id, event, -3);
    Queue<NamespaceNotification> notificationQueue =
        core.getClientNotificationQueue(client1Id);
    Assert.assertEquals(2, notificationQueue.size());
    Assert.assertEquals(20, notificationQueue.poll().txId);
    Assert.assertEquals(30, notificationQueue.poll().txId);
    core.unsubscribeClient(client1Id, event);
    
    core.removeClient(client1Id);
    core.removeClient(client2Id);
  }


  class TestSubscriptionHistory extends EmptyServerHistory {

    @Override
    public void addNotificationsToQueue(NamespaceEvent event, long txId,
        Queue<NamespaceNotification> notifications)
        throws TransactionIdTooOldException {
      // Using -2 to mark a transaction which is too old
      if (txId == -2)
        throw new TransactionIdTooOldException();
      
      // Using -3 to mark we should add these 2 notifications
      if (txId == -3) {
        notifications.add(new NamespaceNotification("/a/b",
            EventType.FILE_ADDED.getByteValue(), 20));
        notifications.add(new NamespaceNotification("/a/c",
            EventType.FILE_ADDED.getByteValue(), 30));
      }
    }
  }
  
  
  @Test
  public void testNotificationHandling() throws Exception {
    ServerCore core = new ServerCore(conf);
    List<IServerDispatcher> dispatchers = new LinkedList<IServerDispatcher>();
    dispatchers.add(new EmptyServerDispatcher());
    TestNotificationHandlingHistory history = new TestNotificationHandlingHistory();
    core.init(new EmptyServerLogReader(), history, dispatchers,
        new EmptyServerHandler());
    long client1Id = 1000, client2Id = 2000;
    EmptyClientHandler client1Obj = new EmptyClientHandler(),
        client2Obj = new EmptyClientHandler();
    NamespaceEvent event1 = new NamespaceEvent("/a",
        EventType.FILE_ADDED.getByteValue());
    NamespaceEvent event2 = new NamespaceEvent("/b",
        EventType.FILE_ADDED.getByteValue());
    NamespaceEvent event3 = new NamespaceEvent("/c",
        EventType.FILE_ADDED.getByteValue());
    long txIdCount = 10;
    
    core.addClient(client1Id, client1Obj);
    core.addClient(client2Id, client2Obj);
    core.subscribeClient(client1Id, event1, -1);
    core.subscribeClient(client2Id, event2, -1);
    core.subscribeClient(client1Id, event3, -1);
    core.subscribeClient(client2Id, event3, -1);

    String[] possiblePaths = {"/a/", "/b/", "/c/"};
    Random generator = new Random();
    Queue<NamespaceNotification> client1ExpectedQueue =
        new LinkedList<NamespaceNotification>();
    Queue<NamespaceNotification> client2ExpectedQueue =
        new LinkedList<NamespaceNotification>();
    Queue<NamespaceNotification> historyExpectedQueue =
        new LinkedList<NamespaceNotification>();
    for (long txId = 0; txId < txIdCount; txId ++) {
      String basePath = possiblePaths[generator.nextInt(3)];
      NamespaceNotification n = new NamespaceNotification(basePath + txId,
          EventType.FILE_ADDED.getByteValue(), txId);
      
      historyExpectedQueue.add(n);
      if (basePath.equals("/a/") || basePath.equals("/c/"))
        client1ExpectedQueue.add(n);
      if (basePath.equals("/b/") || basePath.equals("/c/"))
        client2ExpectedQueue.add(n);
      
      core.handleNotification(n);
    }
    
    Assert.assertEquals(historyExpectedQueue.size(),
        history.notifications.size());
    Assert.assertEquals(client1ExpectedQueue.size(),
        core.getClientNotificationQueue(client1Id).size());
    Assert.assertEquals(client2ExpectedQueue.size(),
        core.getClientNotificationQueue(client2Id).size());

    while (!historyExpectedQueue.isEmpty())
      Assert.assertEquals(historyExpectedQueue.poll(),
          history.notifications.poll());
    while (!client1ExpectedQueue.isEmpty())
      Assert.assertEquals(client1ExpectedQueue.poll(),
          core.getClientNotificationQueue(client1Id).poll());
    while (!client2ExpectedQueue.isEmpty())
      Assert.assertEquals(client2ExpectedQueue.poll(),
          core.getClientNotificationQueue(client2Id).poll());
    
    core.unsubscribeClient(client1Id, event1);
    core.unsubscribeClient(client2Id, event2);
    core.unsubscribeClient(client1Id, event3);
    core.unsubscribeClient(client2Id, event3);
    core.removeClient(client1Id);
    core.removeClient(client2Id);
  }
  
  class TestNotificationHandlingHistory extends EmptyServerHistory {
    
    ConcurrentLinkedQueue<NamespaceNotification> notifications =
        new ConcurrentLinkedQueue<NamespaceNotification>();
    
    @Override
    public void storeNotification(NamespaceNotification notification) {
      notifications.add(notification);
    }
  }

  
}
