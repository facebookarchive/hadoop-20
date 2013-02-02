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
import java.util.Random;

import org.apache.log4j.Level;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.impl.Log4JLogger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.notifier.NamespaceNotifierClient.NotConnectedToServerException;
import org.apache.hadoop.hdfs.notifier.NamespaceNotifierClient.WatchAlreadyPlacedException;
import org.apache.hadoop.hdfs.notifier.server.NotifierTestUtil;
import org.apache.hadoop.hdfs.notifier.server.ServerCore;
import org.apache.hadoop.hdfs.notifier.server.ServerLogReaderTransactional;
import org.apache.thrift.TException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestSimpleGetNotification {
  
  Configuration conf;
  MiniDFSCluster cluster;
  FileSystem fileSys;
  String localhostAddr;
  Random random = new Random(System.currentTimeMillis());
  
  final static String TEST_DIR = new File(System.getProperty("test.build.data",
      "build/test/data")).getAbsolutePath();
  final static Log LOG = LogFactory.getLog(TestSimpleGetNotification.class);
  {
    ((Log4JLogger)ServerLogReaderTransactional.LOG).getLogger().setLevel(Level.ALL);
  }
  
  @Before
  public void setUp() throws IOException {
    new File(TEST_DIR).mkdirs();

    conf = NotifierTestUtil.initGenericConf();
    String editsLocation = TEST_DIR + "/tmp/hadoop/name";
    conf.set("dfs.name.edits.dir", editsLocation);
    conf.set(NotifierConfig.NOTIFIER_EDITS_SOURCE, editsLocation);
    conf.setInt(ServerCore.SERVER_ID, 1);
    cluster = new MiniDFSCluster(conf, 1, true, null);
    cluster.waitActive(true);
    
    fileSys = cluster.getFileSystem();
    localhostAddr = cluster.getNameNode().getHttpAddress().getAddress().getHostName();
  }
  
  @After
  public void shutDown() {
    if (cluster != null) {
      cluster.shutdown();
    }
  }
  
  public class MyClientWatcher implements Watcher {
    public volatile boolean connected = false;
    public volatile int numAdd = 0;
    public volatile int numClosed = 0;
    public volatile int numDeleted = 0;

    @Override
    public void handleNamespaceNotification(NamespaceNotification notification) {
      LOG.info("received: " + notification.path + ", " + notification.type);
      
      if (notification.type == EventType.FILE_ADDED.getByteValue()) {
        ++ numAdd;
      } else if (notification.type == EventType.FILE_ADDED.getByteValue()) {
        ++ numClosed;
      } else if (notification.type == EventType.FILE_ADDED.getByteValue()) {
        ++ numDeleted;
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
    
  }
  
  @Test
  public void testSimpleClient() throws IOException, InterruptedException, 
                            TException, TransactionIdTooOldException, 
                            NotConnectedToServerException, 
                            WatchAlreadyPlacedException {
    ServerCore notifier = null;
    
    try {
      String editPath = conf.get("dfs.name.edits.dir");
      File file = new File(editPath);
      String[] names = file.list();
      for (String name : names) {
        LOG.info("content: " + name);
      }
      
      notifier = ServerCore.createNotifier(conf);
      
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
      
      for (int i = 0; i < numRequests; i++) {
        fileSys.delete(new Path("/testfile_" + i), false);
      }
      
      start = System.currentTimeMillis();
      while (myWatcher.numDeleted < numRequests && 
          System.currentTimeMillis() < start + 6000) {
        Thread.sleep(500);
      }
      
      assertEquals(numRequests, myWatcher.numAdd);
      assertEquals(0, myWatcher.numClosed);
      assertEquals(0, myWatcher.numDeleted);
    } finally {
      if (notifier != null) {
        notifier.shutdown();
        notifier.join();
      }
    }
  }
}
