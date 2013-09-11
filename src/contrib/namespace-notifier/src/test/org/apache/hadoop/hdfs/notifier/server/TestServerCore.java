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
    conf = NotifierTestUtil.initGenericConf();
  }
  
  @Test
  public void testClientOperations() throws Exception {
    ServerCore core = new ServerCore(conf);
    core.init(new EmptyServerLogReader(), new EmptyServerHistory(),
        new EmptyServerDispatcher(), new EmptyServerHandler());
    long clientId = 1000;
    EmptyClientHandler handler = new EmptyClientHandler();
    
    core.addClient(new ClientData(clientId, handler, "host", 3000));
    Assert.assertTrue(core.isRegistered(clientId));
    Assert.assertEquals(handler, core.getClientData(clientId).handler);
    Assert.assertEquals(1, core.getClients().size());
    core.removeClient(clientId);
    Assert.assertFalse(core.isRegistered(clientId));
    Assert.assertEquals(0, core.getClients().size());
  }
  

  @Test
  public void testSubscription() throws Exception {
    ServerCore core = new ServerCore(conf);
    core.init(new EmptyServerLogReader(), new TestSubscriptionHistory(),
        new EmptyServerDispatcher(), new EmptyServerHandler());
    long id1 = 1000, id2 = 2000;
    EmptyClientHandler handler1 = new EmptyClientHandler(),
        handler2 = new EmptyClientHandler();
    NamespaceEvent event = new NamespaceEvent("/a",
        EventType.FILE_ADDED.getByteValue());
    
    core.addClient(new ClientData(id1, handler1, "host", 3000));
    core.addClient(new ClientData(id2, handler2, "host", 3001));
    
    Set<Long> clientsForNotification;
    core.subscribeClient(id1, event, -1);
    clientsForNotification = core.getClientsForNotification(
        new NamespaceNotification("/a/b", event.type, 10));
    Assert.assertEquals(1, clientsForNotification.size());
    Assert.assertTrue(clientsForNotification.contains(id1));
    
    core.subscribeClient(id2, event, -1);
    clientsForNotification = core.getClientsForNotification(
        new NamespaceNotification("/a/b", event.type, 10));
    Assert.assertEquals(2, clientsForNotification.size());
    Assert.assertTrue(clientsForNotification.contains(id1));
    Assert.assertTrue(clientsForNotification.contains(id2));
    
    core.unsubscribeClient(id1, event);
    clientsForNotification = core.getClientsForNotification(
        new NamespaceNotification("/a/b", event.type, 10));
    Assert.assertEquals(1, clientsForNotification.size());
    Assert.assertTrue(clientsForNotification.contains(id2));
    
    core.unsubscribeClient(id2, event);
    clientsForNotification = core.getClientsForNotification(
        new NamespaceNotification("/a/b", event.type, 10));
    if (clientsForNotification != null)
      Assert.assertEquals(0, clientsForNotification.size());
    
    // Test that TransactionIdTooOldException is thrown
    try {
      core.subscribeClient(id1, event, -2);
      // We should get a transaction id too old exception
      Assert.fail();
    } catch (TransactionIdTooOldException e) {}
    
    // Test that the notifications are queued correctly
    core.subscribeClient(id1, event, -3);
    Queue<NamespaceNotification> notificationQueue =
        core.getClientNotificationQueue(id1);
    Assert.assertEquals(2, notificationQueue.size());
    Assert.assertEquals(20, notificationQueue.poll().txId);
    Assert.assertEquals(30, notificationQueue.poll().txId);
    core.unsubscribeClient(id1, event);
    
    core.removeClient(id1);
    core.removeClient(id2);
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
    TestNotificationHandlingHistory history = new TestNotificationHandlingHistory();
    core.init(new EmptyServerLogReader(), history, new EmptyServerDispatcher(),
        new EmptyServerHandler());
    NamespaceEvent event1 = new NamespaceEvent("/a",
        EventType.FILE_ADDED.getByteValue());
    NamespaceEvent event2 = new NamespaceEvent("/b",
        EventType.FILE_ADDED.getByteValue());
    NamespaceEvent event3 = new NamespaceEvent("/c",
        EventType.FILE_ADDED.getByteValue());
    long txIdCount = 10;
    long id1 = 1000, id2 = 2000;
    EmptyClientHandler handler1 = new EmptyClientHandler(),
        handler2 = new EmptyClientHandler();
    
    core.addClient(new ClientData(id1, handler1, "host", 3000));
    core.addClient(new ClientData(id2, handler2, "host", 3001));
    core.subscribeClient(id1, event1, -1);
    core.subscribeClient(id2, event2, -1);
    core.subscribeClient(id1, event3, -1);
    core.subscribeClient(id2, event3, -1);

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
        core.getClientNotificationQueue(id1).size());
    Assert.assertEquals(client2ExpectedQueue.size(),
        core.getClientNotificationQueue(id2).size());

    while (!historyExpectedQueue.isEmpty())
      Assert.assertEquals(historyExpectedQueue.poll(),
          history.notifications.poll());
    while (!client1ExpectedQueue.isEmpty())
      Assert.assertEquals(client1ExpectedQueue.poll(),
          core.getClientNotificationQueue(id1).poll());
    while (!client2ExpectedQueue.isEmpty())
      Assert.assertEquals(client2ExpectedQueue.poll(),
          core.getClientNotificationQueue(id2).poll());
    
    core.unsubscribeClient(id1, event1);
    core.unsubscribeClient(id2, event2);
    core.unsubscribeClient(id1, event3);
    core.unsubscribeClient(id2, event3);
    core.removeClient(id1);
    core.removeClient(id2);
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
