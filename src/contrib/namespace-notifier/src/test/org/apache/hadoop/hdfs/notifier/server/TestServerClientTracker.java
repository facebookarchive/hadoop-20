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

import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import junit.framework.Assert;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.notifier.ClientHandler;
import org.apache.hadoop.hdfs.notifier.InvalidServerIdException;
import org.apache.thrift.TException;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestServerClientTracker {
  static private Logger LOG = LoggerFactory.getLogger(TestServerClientTracker.class);

  static Configuration conf;
  
  @BeforeClass
  public static void initConf() {
    Configuration.addDefaultResource("namespace-notifier-server-default.xml");
    Configuration.addDefaultResource("hdfs-default.xml");
    conf = new Configuration();
    conf.addResource("namespace-notifier-server-site.xml");
    conf.addResource("hdfs-site.xml");
    conf.set(ServerDispatcher.THREAD_POOLS_SIZE, "5");
  }

  
  @Test
  public void testClientTimeoutBasic1() throws Exception {
    DummyServerCore core = new DummyServerCore();
    ServerDispatcher clientTracker = new ServerDispatcher(core, 0);
    DummyClientHandler clientHandler = new DummyClientHandler();
    long clientId = 1000;
    core.init(clientTracker);
    
    // Make the client fail so it doesen't respond to heartbeats
    clientHandler.failingClient = true;
    core.clients.put(clientId, clientHandler);
    core.clientLocks.put(clientId, new ReentrantLock());
    clientTracker.setClientTimeout(100);
    clientTracker.setHeartbeatTimeout(1000);
    clientTracker.loopSleepTime = 10;
    clientTracker.assignClient(clientId);
    new Thread(clientTracker).start();
    Thread.sleep(200);
    core.shutdown();
    
    Assert.assertFalse(core.removedClients.contains(clientId));
  }
  
  
  @Test
  public void testClientTimeoutBasic2() throws Exception {
    DummyServerCore core = new DummyServerCore();
    ServerDispatcher clientTracker = new ServerDispatcher(core, 0);
    DummyClientHandler clientHandler = new DummyClientHandler();
    long clientId = 1000;
    core.init(clientTracker);
    
    // Make the client fail so it doesen't respond to heartbeats
    clientHandler.failingClient = true;
    core.clients.put(clientId, clientHandler);
    core.clientLocks.put(clientId, new ReentrantLock());
    clientTracker.setClientTimeout(100);
    clientTracker.setHeartbeatTimeout(1000);
    clientTracker.loopSleepTime = 10;
    clientTracker.assignClient(clientId);
    clientTracker.clientHandleNotificationFailed(clientId,
        System.currentTimeMillis());
    new Thread(clientTracker).start();
    Thread.sleep(300);
    core.shutdown();
    
    Assert.assertTrue(core.removedClients.contains(clientId));
  }
  
  @Test
  public void testClientTimeoutBasic3() throws Exception {
    DummyServerCore core = new DummyServerCore();
    ServerDispatcher clientTracker = new ServerDispatcher(core, 0);
    DummyClientHandler clientHandler = new DummyClientHandler();
    long clientId = 1000;
    core.init(clientTracker);
    
    clientHandler.failingClient = true;
    core.clients.put(clientId, clientHandler);
    core.clientLocks.put(clientId, new ReentrantLock());
    clientTracker.setClientTimeout(100);
    clientTracker.setHeartbeatTimeout(1000);
    clientTracker.loopSleepTime = 10;
    clientTracker.assignClient(clientId);
    clientTracker.clientHandleNotificationFailed(clientId,
        System.currentTimeMillis());
    clientTracker.clientHandleNotificationSuccessful(clientId,
        System.currentTimeMillis());
    new Thread(clientTracker).start();
    Thread.sleep(300);
    core.shutdown();
    
    Assert.assertFalse(core.removedClients.contains(clientId));
  }
  
  
  @Test
  public void testClientHeartbeatBasic1() throws Exception {
    DummyServerCore core = new DummyServerCore();
    ServerDispatcher clientTracker = new ServerDispatcher(core, 0);
    DummyClientHandler clientHandler = new DummyClientHandler();
    long clientId = 1000;
    core.init(clientTracker);
    
    core.clients.put(clientId, clientHandler);
    core.clientLocks.put(clientId, new ReentrantLock());
    clientTracker.setClientTimeout(10000); // to avoid removing it
    clientTracker.setHeartbeatTimeout(100);
    clientTracker.loopSleepTime = 10;
    clientTracker.assignClient(clientId);
    new Thread(clientTracker).start();
    Thread.sleep(350);
    core.shutdown();

    Assert.assertEquals(3, clientHandler.receivedHeartbeats.size());
  }
  
  
  @Test
  public void testClientHeartbeatBasic2() throws Exception {
    DummyServerCore core = new DummyServerCore();
    ServerDispatcher clientTracker = new ServerDispatcher(core, 0);
    DummyClientHandler clientHandler = new DummyClientHandler();
    long clientId = 1000;
    core.init(clientTracker);
    
    core.clients.put(clientId, clientHandler);
    core.clientLocks.put(clientId, new ReentrantLock());
    clientTracker.setClientTimeout(10000); // to avoid removing it
    clientTracker.setHeartbeatTimeout(200);
    clientTracker.loopSleepTime = 10;
    clientTracker.assignClient(clientId);
    new Thread(clientTracker).start();
    Thread.sleep(80);
    clientTracker.clientHandleNotificationSuccessful(clientId,
        System.currentTimeMillis());
    Thread.sleep(80);
    clientTracker.clientHandleNotificationSuccessful(clientId,
        System.currentTimeMillis());
    Thread.sleep(540);
    core.shutdown();

    Assert.assertEquals(2, clientHandler.receivedHeartbeats.size());
  }
  
  
  @Test
  public void testClientHeartbeatBasic3() throws Exception {
    DummyServerCore core = new DummyServerCore();
    ServerDispatcher clientTracker = new ServerDispatcher(core, 0);
    DummyClientHandler clientHandler = new DummyClientHandler();
    long clientId = 1000;
    core.init(clientTracker);
    
    core.clients.put(clientId, clientHandler);
    core.clientLocks.put(clientId, new ReentrantLock());
    clientTracker.setClientTimeout(10000); // to avoid removing it
    clientTracker.setHeartbeatTimeout(200);
    clientTracker.loopSleepTime = 10;
    clientTracker.assignClient(clientId);
    new Thread(clientTracker).start();
    Thread.sleep(80);
    clientTracker.clientHandleNotificationSuccessful(clientId,
        System.currentTimeMillis());
    Thread.sleep(80);
    clientTracker.clientHandleNotificationFailed(clientId,
        System.currentTimeMillis());
    Thread.sleep(540);
    core.shutdown();

    Assert.assertEquals(0, clientHandler.receivedHeartbeats.size());
  }
  
  
  class DummyServerCore extends EmptyServerCore {
    
    ConcurrentMap<Long, DummyClientHandler> clients =
        new ConcurrentHashMap<Long, DummyClientHandler>();
    ConcurrentMap<Long, Lock> clientLocks = 
        new ConcurrentHashMap<Long, Lock>();
    
    Set<Long> removedClients = new HashSet<Long>();
    ServerDispatcher clientTracker;
    
    public void init(ServerDispatcher clientTracker) {
      this.clientTracker = clientTracker;
    }
    
    @Override
    public ClientHandler.Iface getClient(long clientId) {
      return clients.get(clientId);
    }
    
    @Override
    public Lock getClientCommunicationLock(long clientId) {
      return clientLocks.get(clientId);
    }
    
    @Override
    public boolean removeClient(long clientId) {
      synchronized (removedClients) {
        removedClients.add(clientId);
      }
      clients.remove(clientId);
      clientTracker.removeClient(clientId);
      
      return true;
    }
    
    @Override
    public Configuration getConfiguration() {
      return conf;
    }
  }
  
  
  private class DummyClientHandler extends EmptyClientHandler {
    
    public boolean failingClient = false;
    public boolean clientRemovedUs = false;
    
    // A sorted list with the timestamp when the heartbeats were received
    List<Long> receivedHeartbeats = new ArrayList<Long>();
    
    @Override
    public void heartbeat(long serverId) throws InvalidServerIdException,
        TException {
      if (failingClient)
        throw new TException();
      if (clientRemovedUs)
        throw new InvalidServerIdException();
      
      receivedHeartbeats.add(new Date().getTime());
    }
  
  }
 
}
