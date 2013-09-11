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

import java.util.Date;
import java.util.LinkedList;
import java.util.Queue;

import junit.framework.Assert;

import org.apache.commons.logging.impl.Log4JLogger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.notifier.ClientHandlerImpl;
import org.apache.hadoop.hdfs.notifier.EventType;
import org.apache.hadoop.hdfs.notifier.NamespaceEvent;
import org.apache.hadoop.hdfs.notifier.NamespaceNotification;
import org.apache.hadoop.hdfs.notifier.TransactionIdTooOldException;
import org.apache.log4j.Level;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestServerHistory {
  static private Logger LOG = LoggerFactory.getLogger(TestServerHistory.class);

  static Configuration conf;
  
  {
    ((Log4JLogger)ServerLogReaderTransactional.LOG).getLogger().setLevel(Level.ALL);
    ((Log4JLogger)ClientHandlerImpl.LOG).getLogger().setLevel(Level.ALL);
    ((Log4JLogger)ServerHistory.LOG).getLogger().setLevel(Level.ALL);
  }
  
  @BeforeClass
  public static void initConf() {
    conf = NotifierTestUtil.initGenericConf();
  }
  
  
  /**
   * Check that the ramp up phase runs for the (at least) configured
   * amount of time.
   * @throws Exception
   */
  @Test
  public void testRampUp() throws Exception {
    DummyServerCore core = new DummyServerCore();
    ServerHistory history = new ServerHistory(core, true);
    long initialTime = new Date().getTime();
    long historyLength = 100;
    history.setHistoryLength(historyLength);
    
    new Thread(history).start();
    while (history.isRampUp())
      Thread.sleep(1);
    core.shutdown();
    Assert.assertTrue(System.currentTimeMillis() - initialTime > historyLength);
  }

  
  @Test
  public void testBasicQueueNotification() throws Exception {
    // Starting without a ramp-up phase
    DummyServerCore core = new DummyServerCore();
    ServerHistory history = new ServerHistory(core, false);
    long historyLength = 100;
    history.setHistoryLength(historyLength);
    Queue<NamespaceNotification> historyNotifications;
    
    new Thread(history).start();
    
    // Step 1 - test with FILE_ADDED
    history.storeNotification(new NamespaceNotification("/a/b",
        EventType.FILE_ADDED.getByteValue(), 10));
    history.storeNotification(new NamespaceNotification("/a/c",
        EventType.FILE_ADDED.getByteValue(), 11));
    historyNotifications = new LinkedList<NamespaceNotification>();
    history.addNotificationsToQueue(new NamespaceEvent("/a",
        EventType.FILE_ADDED.getByteValue()), 10, historyNotifications);
    Assert.assertEquals(1, historyNotifications.size());
    Assert.assertEquals(11, historyNotifications.peek().txId);
    Assert.assertEquals("/a/c", historyNotifications.peek().path);
    
    // Step 2 - test with FILE_CLOSED
    history.storeNotification(new NamespaceNotification("/a/d",
        EventType.FILE_CLOSED.getByteValue(), 12));
    history.storeNotification(new NamespaceNotification("/a/e",
        EventType.FILE_CLOSED.getByteValue(), 13));
    historyNotifications = new LinkedList<NamespaceNotification>();
    history.addNotificationsToQueue(new NamespaceEvent("/a",
        EventType.FILE_CLOSED.getByteValue()), 12, historyNotifications);
    Assert.assertEquals(1, historyNotifications.size());
    Assert.assertEquals(13, historyNotifications.peek().txId);
    Assert.assertEquals("/a/e", historyNotifications.peek().path);
    
    // test the sub directories
    historyNotifications = new LinkedList<NamespaceNotification>();
    history.addNotificationsToQueue(new NamespaceEvent("/",
        EventType.FILE_ADDED.getByteValue()), 10, historyNotifications);
    Assert.assertEquals(1, historyNotifications.size());
    history.addNotificationsToQueue(new NamespaceEvent("/",
        EventType.FILE_CLOSED.getByteValue()), 10, historyNotifications);
    Assert.assertEquals(3, historyNotifications.size());
    
    core.shutdown();
  }

  @Test
  public void testTransactionIdTooOldDoesentHappen() throws Exception {
    // Starting without a ramp-up phase
    DummyServerCore core = new DummyServerCore();
    ServerHistory history = new ServerHistory(core, false);
    long historyLength = 350;
    history.setHistoryLength(historyLength);
    Queue<NamespaceNotification> historyNotifications;
    
    new Thread(history).start();
    
    // Step 1 - test with FILE_ADDED
    history.storeNotification(new NamespaceNotification("/a/b",
        EventType.FILE_ADDED.getByteValue(), 10));
    history.storeNotification(new NamespaceNotification("/a/c",
        EventType.FILE_ADDED.getByteValue(), 11));
    historyNotifications = new LinkedList<NamespaceNotification>();
    try {
      Thread.sleep(historyLength - 30);
      history.addNotificationsToQueue(new NamespaceEvent("/a",
          EventType.FILE_ADDED.getByteValue()), 10, historyNotifications);
    } catch (TransactionIdTooOldException e) {
      Assert.fail();
    }
    
    core.shutdown();
  }


  @Test
  public void testTransactionIdTooOldDoesHappen() throws Exception {
    // Starting without a ramp-up phase
    DummyServerCore core = new DummyServerCore();
    ServerHistory history = new ServerHistory(core, false);
    long historyLength = 100;
    history.setHistoryLength(historyLength);
    Queue<NamespaceNotification> historyNotifications;
    history.loopSleepTime = 10;
    
    new Thread(history).start();
    
    // Step 1 - test with FILE_ADDED
    history.storeNotification(new NamespaceNotification("/a/b",
        EventType.FILE_ADDED.getByteValue(), 10));
    history.storeNotification(new NamespaceNotification("/a/c",
        EventType.FILE_ADDED.getByteValue(), 11));
    historyNotifications = new LinkedList<NamespaceNotification>();
    try {
      Thread.sleep(historyLength + 50);
      history.addNotificationsToQueue(new NamespaceEvent("/a",
          EventType.FILE_ADDED.getByteValue()), 10, historyNotifications);
      Assert.fail(); // should receive the exception
    } catch (TransactionIdTooOldException e) {}
    
    core.shutdown();
  }
  
  
  @Test
  public void testHistoryMemoryCleanup1() throws Exception {
    // Starting without a ramp-up phase
    DummyServerCore core = new DummyServerCore();
    ServerHistory history = new ServerHistory(core, false);
    long historyLength = 100;
    history.setHistoryLength(historyLength);
    history.loopSleepTime = 10;
    
    new Thread(history).start();
    
    history.storeNotification(new NamespaceNotification("/a/b",
        EventType.FILE_ADDED.getByteValue(), 10));
    history.storeNotification(new NamespaceNotification("/a/c",
        EventType.FILE_ADDED.getByteValue(), 11));
    Thread.sleep(historyLength + 50);
    Assert.assertEquals(0, history.orderedHistoryList.size());
    Assert.assertEquals(0, history.historyTree.children.size());
    core.shutdown();
  }
  
  
  @Test
  public void testHistoryMemoryCleanup2() throws Exception {
    // Starting without a ramp-up phase
    DummyServerCore core = new DummyServerCore();
    ServerHistory history = new ServerHistory(core, false);
    long historyLength = 10000;
    history.setHistoryLength(historyLength);
    history.setHistoryLimit(1000);
    history.loopSleepTime = 10;
    Thread historyThread = new Thread(history);
    
    historyThread.start();
    
    for (int i = 0; i < 1500; i ++) {
      history.storeNotification(new NamespaceNotification("/a/b" + i,
          EventType.FILE_ADDED.getByteValue(), i));
    }
    
    Thread.sleep(500);
    core.shutdown();
    historyThread.join();
    
    Assert.assertEquals(1000, history.orderedHistoryList.size());
  }
  

  @Test
  public void testQueueNotificationAdvanced() throws Exception {
    // Starting without a ramp-up phase
    DummyServerCore core = new DummyServerCore();
    ServerHistory history = new ServerHistory(core, false);
    long historyLength = 10000;
    history.setHistoryLength(historyLength);
    Queue<NamespaceNotification> historyNotifications;
    long txCount = 1001;
    
    new Thread(history).start();
    
    for (long txId = 0; txId < txCount; txId ++) {
      history.storeNotification(new NamespaceNotification("/a/" + txId,
          EventType.FILE_ADDED.getByteValue(), txId));
    }

    // Part 1 - Get all notifications
    historyNotifications = new LinkedList<NamespaceNotification>();
    history.addNotificationsToQueue(new NamespaceEvent("/a",
        EventType.FILE_ADDED.getByteValue()), 0, historyNotifications);
    Assert.assertEquals(1000, historyNotifications.size());
    for (long txId = 1; txId < txCount; txId ++) {
      NamespaceNotification n = historyNotifications.poll();
      Assert.assertEquals(txId, n.txId);
      Assert.assertEquals("/a/" + txId, n.path);
      Assert.assertEquals(EventType.FILE_ADDED.getByteValue(), n.type);
    }
    
    // Part 2 - Get half of the notifications
    historyNotifications = new LinkedList<NamespaceNotification>();
    history.addNotificationsToQueue(new NamespaceEvent("/a",
        EventType.FILE_ADDED.getByteValue()), 500, historyNotifications);
    Assert.assertEquals(500, historyNotifications.size());
    for (long txId = 501; txId < txCount; txId ++) {
      NamespaceNotification n = historyNotifications.poll();
      Assert.assertEquals(txId, n.txId);
      Assert.assertEquals("/a/" + txId, n.path);
      Assert.assertEquals(EventType.FILE_ADDED.getByteValue(), n.type);
    }
    
    core.shutdown();
  }

  
  class DummyServerCore extends EmptyServerCore {

    @Override
    public Configuration getConfiguration() {
      return conf;
    }
  }

}
