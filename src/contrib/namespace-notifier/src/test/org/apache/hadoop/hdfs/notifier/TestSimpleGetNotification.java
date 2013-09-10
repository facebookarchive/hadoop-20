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

package org.apache.hadoop.hdfs.notifier;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.util.Random;

import org.apache.log4j.Level;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.impl.Log4JLogger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.MiniAvatarCluster;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.notifier.NamespaceNotifierClient.NotConnectedToServerException;
import org.apache.hadoop.hdfs.notifier.NamespaceNotifierClient.WatchAlreadyPlacedException;
import org.apache.hadoop.hdfs.notifier.server.NotifierTestUtil;
import org.apache.hadoop.hdfs.notifier.server.ServerCore;
import org.apache.hadoop.hdfs.notifier.server.ServerHistory;
import org.apache.hadoop.hdfs.notifier.server.ServerLogReaderTransactional;
import org.apache.hadoop.hdfs.protocol.FSConstants;
import org.apache.hadoop.hdfs.server.namenode.AvatarNode;
import org.apache.thrift.TException;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig.ConfigException;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestSimpleGetNotification {
  
  Configuration conf;
  MiniAvatarCluster cluster;
  FileSystem fileSys;
  String localhostAddr;
  Random random = new Random(System.currentTimeMillis());
  String nameService;
  
  final static String TEST_DIR = new File(System.getProperty("test.build.data",
      "build/test/data")).getAbsolutePath();
  final static Log LOG = LogFactory.getLog(TestSimpleGetNotification.class);
  {
    ((Log4JLogger)ServerLogReaderTransactional.LOG).getLogger().setLevel(Level.ALL);
    ((Log4JLogger)ClientHandlerImpl.LOG).getLogger().setLevel(Level.ALL);
    ((Log4JLogger)ServerHistory.LOG).getLogger().setLevel(Level.ALL);
  }
  
  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    MiniAvatarCluster.createAndStartZooKeeper();
  }
  
  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    MiniAvatarCluster.shutDownZooKeeper();
  }
  
  @Before
  public void setUp() throws IOException, ConfigException, InterruptedException {
    new File(TEST_DIR).mkdirs();

    conf = NotifierTestUtil.initGenericConf();
    String editsLocation = TEST_DIR + "/tmp/hadoop/name";
    conf.set(AvatarNode.DFS_SHARED_EDITS_DIR0_KEY, editsLocation + "0");
    conf.set(AvatarNode.DFS_SHARED_EDITS_DIR1_KEY, editsLocation + "1");
    cluster = new MiniAvatarCluster.Builder(conf).numDataNodes(3).enableQJM(false)
        .federation(true).build();
    
    nameService = conf.get(FSConstants.DFS_FEDERATION_NAMESERVICES);
    fileSys = cluster.getFileSystem();
    localhostAddr = InetAddress.getLocalHost().getHostName(); 
    DFSUtil.setGenericConf(conf, nameService, AvatarNode.DFS_SHARED_EDITS_DIR0_KEY, 
                                              AvatarNode.DFS_SHARED_EDITS_DIR1_KEY);
  }
  
  @After
  public void shutDown() throws IOException, InterruptedException {
    if (cluster != null) {
      cluster.shutDown();
    }
  }
  
  public class MyClientWatcher implements Watcher {
    public volatile boolean connected = false;
    public volatile int numAdd = 0;
    public volatile int numClosed = 0;
    public volatile int numDeleted = 0;
    public volatile int numDirAdded = 0;

    @Override
    public void handleNamespaceNotification(NamespaceNotification notification) {
      LOG.info("received: " + notification.path + ", " + notification.type);
      
      if (notification.type == EventType.FILE_ADDED.getByteValue()) {
        ++ numAdd;
      } else if (notification.type == EventType.FILE_CLOSED.getByteValue()) {
        ++ numClosed;
      } else if (notification.type == EventType.NODE_DELETED.getByteValue()) {
        ++ numDeleted;
      } else if (notification.type == EventType.DIR_ADDED.getByteValue()) {
        ++ numDirAdded;
      }
    }

    @Override
    public void connectionFailed() {
      connected = false;
    }

    @Override
    public void connectionSuccesful() {
      connected = true;
    }
    
    public void resetCounters() {
      numAdd = 0;
      numClosed = 0;
      numDeleted = 0;
    }
    
  }
  
  @Test
  public void testSimpleClient() throws TException, InterruptedException, 
            TransactionIdTooOldException, NotConnectedToServerException,  
            WatchAlreadyPlacedException, IOException {
    ServerCore notifier = null;
    
    try {
      String editPath = conf.get(AvatarNode.DFS_SHARED_EDITS_DIR0_KEY);
      LOG.info("edit path: " + editPath);
      File file = new File(editPath);
      String[] names = file.list();
      for (String name : names) {
        LOG.info("content: " + name);
      }
     
      conf.setLong(NotifierConfig.LOG_READER_STREAM_TIMEOUT, 100);
      
      notifier = ServerCore.createNotifier(conf, nameService);
      
      int port = conf.getInt("notifier.thrift.port", -1);
      int clientPort = MiniDFSCluster.getFreePort();
      LOG.info("port: " + port);
      MyClientWatcher myWatcher = new MyClientWatcher();
      NamespaceNotifierClient myClient = new NamespaceNotifierClient(myWatcher, 
          localhostAddr, port, clientPort);
      Thread clientThread = new Thread(myClient);
      clientThread.start();
      
      // wait for connect
      long start = System.currentTimeMillis();
      while (!myWatcher.connected &&
            System.currentTimeMillis() < start + 2000) {
        Thread.sleep(500);
      }
      
      myClient.placeWatch("/", EventType.FILE_ADDED, -1);
      myClient.placeWatch("/", EventType.FILE_CLOSED, -1);
      myClient.placeWatch("/", EventType.NODE_DELETED, -1);
      
      int numRequests = random.nextInt(50) + 1;
      // issue number of requests.
      for (int i = 0; i < numRequests; i++) {
        FSDataOutputStream out = fileSys.create(new Path("/testfile_" + i));
        out.close();
      }
      
      LOG.info("Start failover");
      cluster.failOver();
      cluster.restartStandby();
      Thread.sleep(100);
      
      for (int i = 0; i < numRequests; i++) {
        fileSys.delete(new Path("/testfile_" + i), false);
      }
      
      start = System.currentTimeMillis();
      while (myWatcher.numDeleted < numRequests && 
          System.currentTimeMillis() < start + 6000) {
        Thread.sleep(500);
      }
      
      assertEquals(numRequests, myWatcher.numAdd);
      assertEquals(numRequests, myWatcher.numClosed);
      assertEquals(numRequests, myWatcher.numDeleted);
      
      // check the events from sub directors.
      myWatcher.resetCounters();
      // issue number of requests.
      for (int i = 0; i < numRequests; i++) {
        FSDataOutputStream out = fileSys.create(new Path("/user/dikang/testfile_" + i));
        out.close();
      }
     
      LOG.info("Start failover");
      cluster.failOver();
      cluster.restartStandby();
      Thread.sleep(100);
      
      for (int i = 0; i < numRequests; i++) {
        fileSys.delete(new Path("/user/dikang/testfile_" + i), false);
      }
      
      start = System.currentTimeMillis();
      while (myWatcher.numDeleted < numRequests && 
          System.currentTimeMillis() < start + 6000) {
        Thread.sleep(500);
      }
      
      assertEquals(numRequests, myWatcher.numAdd);
      assertEquals(numRequests, myWatcher.numClosed);
      assertEquals(numRequests, myWatcher.numDeleted);
      
      
    } finally {
      if (notifier != null) {
        notifier.shutdown();
        notifier.join();
      }
    }
  }
  
  @Test
  public void testDirOps() throws IOException, InterruptedException, 
                            TException, TransactionIdTooOldException, 
                            NotConnectedToServerException, 
                            WatchAlreadyPlacedException {
    ServerCore notifier = null;
    
    try {
      String editPath = conf.get(AvatarNode.DFS_SHARED_EDITS_DIR0_KEY);
      File file = new File(editPath);
      String[] names = file.list();
      for (String name : names) {
        LOG.info("content: " + name);
      }
      
      notifier = ServerCore.createNotifier(conf, nameService);
      
      int port = conf.getInt("notifier.thrift.port", -1);
      int clientPort = MiniDFSCluster.getFreePort();
      LOG.info("port: " + port);
      MyClientWatcher myWatcher = new MyClientWatcher();
      NamespaceNotifierClient myClient = new NamespaceNotifierClient(myWatcher, 
          localhostAddr, port, clientPort);
      Thread clientThread = new Thread(myClient);
      clientThread.start();
      
      // wait for connect
      long start = System.currentTimeMillis();
      while (!myWatcher.connected &&
            System.currentTimeMillis() < start + 2000) {
        Thread.sleep(500);
      }
      
      myClient.placeWatch("/", EventType.NODE_DELETED, -1);
      myClient.placeWatch("/", EventType.DIR_ADDED, -1);
      
      int numRequests = random.nextInt(50) + 1;
      // issue number of requests.
      for (int i = 0; i < numRequests; i++) {
        FSDataOutputStream out = fileSys.create(new Path("/testDir_" + i + "/file"));
        out.close();
      }
      
      for (int i = 0; i < numRequests; i++) {
        fileSys.delete(new Path("/testDir_" + i), true);
      }
      
      start = System.currentTimeMillis();
      while (myWatcher.numDeleted < numRequests && 
          System.currentTimeMillis() < start + 6000) {
        Thread.sleep(500);
      }
      
      assertEquals(numRequests, myWatcher.numDirAdded);
      assertEquals(numRequests, myWatcher.numDeleted);
    } finally {
      if (notifier != null) {
        notifier.shutdown();
        notifier.join();
      }
    }
  }
}
