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
package org.apache.hadoop.hdfs;

import java.io.IOException;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.server.namenode.FSImageTestUtil.CheckpointTrigger;
import org.apache.hadoop.util.InjectionEventI;
import org.apache.hadoop.util.InjectionHandler;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestAvatarFileAppend {

  final static Log LOG = LogFactory.getLog(TestAvatarFileAppend.class);

  static final long BLOCK_SIZE = 64 * 1024;
  static final short REPLICATION = 3;
  static final int DATANODE_NUM = 5;
  
  static final Random random = new Random();

  private Configuration conf;
  private int buffersize;
  private MiniAvatarCluster cluster;
  private DistributedFileSystem fs;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    MiniAvatarCluster.createAndStartZooKeeper();
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    MiniAvatarCluster.shutDownZooKeeper();
  }

  @Before
  public void setUp() throws Exception {
    AppendTestUtil.LOG.info("setUp()");
    conf = new Configuration();
    conf.setInt("io.bytes.per.checksum", 512);
    conf.setBoolean("dfs.support.append", true);
    buffersize = conf.getInt("io.file.buffer.size", 4096);
    cluster = new MiniAvatarCluster(conf, 3, true, null, null);
    fs = (DistributedFileSystem) cluster.getFileSystem();
  }

  @After
  public void tearDown() throws Exception {
    AppendTestUtil.LOG.info("tearDown()");
    if (fs != null)
      fs.close();
    if (cluster != null)
      cluster.shutDown();
    InjectionHandler.clear();
  }
  
  public class AppendWorker extends Thread {
    Path p;

    public AppendWorker(Path filePath) {
      this.p = filePath;
    }

    @Override
    public void run() {
      try {
        // Create file and write half block of data. Close file.
        int len1 = (int) BLOCK_SIZE / 2;
        {
          FSDataOutputStream out = fs.create(p, false, buffersize,
              REPLICATION, BLOCK_SIZE);
          AppendTestUtil.write(out, 0, len1);
          out.close();
        }

        for (int j = 0; j < 10; j++) {
          // Reopen file to append. Append 1/4 block of data. Close file.
          final int len2 = (int) BLOCK_SIZE / 4;
          {
            FSDataOutputStream out = fs.append(p);
            AppendTestUtil.write(out, len1, len2);
            out.close();
            len1 += len2;
          }
        }
      } catch (Exception ex) {
        LOG.error(ex);
      }
    }
  }

  /** Append on block boundary. */
  @Test
  public void testTC1() throws Exception {
    TestAvatarAppendInjectionHandler h = new TestAvatarAppendInjectionHandler();
    InjectionHandler.set(h);
    final Path p = new Path("/TC1/foo");
    LOG.info("Path : " + p);

    // a. Create file and write one block of data. Close file.
    final int len1 = (int) BLOCK_SIZE;
    {
      FSDataOutputStream out = fs.create(p, false, buffersize, REPLICATION,
          BLOCK_SIZE);
      AppendTestUtil.write(out, 0, len1);
      out.close();
    }

    // Reopen file to append. Append half block of data. Close file.
    final int len2 = (int) BLOCK_SIZE / 2;
    {
      FSDataOutputStream out = fs.append(p);
      AppendTestUtil.write(out, len1, len2);
      out.close();
    }

    // b. Reopen file and read 1.5 blocks worth of data. Close file.
    AppendTestUtil.check(fs, p, len1 + len2);

    // make sure the standby is working properly
    h.doCheckpoint();
  }

  /** Append on non-block boundary. */
  @Test
  public void testTC2() throws Exception {
    TestAvatarAppendInjectionHandler h = new TestAvatarAppendInjectionHandler();
    InjectionHandler.set(h);
    final Path p = new Path("/TC2/foo");
    LOG.info("Path : " + p);

    // a. Create file with one and a half block of data. Close file.
    final int len1 = (int) (BLOCK_SIZE + BLOCK_SIZE / 2);
    {
      FSDataOutputStream out = fs.create(p, false, buffersize, REPLICATION,
          BLOCK_SIZE);
      AppendTestUtil.write(out, 0, len1);
      out.close();
    }

    // Reopen file to append quarter block of data. Close file.
    final int len2 = (int) BLOCK_SIZE / 4;
    {
      FSDataOutputStream out = fs.append(p);
      AppendTestUtil.write(out, len1, len2);
      out.close();
    }

    // b. Reopen file and read 1.75 blocks of data. Close file.
    AppendTestUtil.check(fs, p, len1 + len2);

    // make sure the standby is working properly
    h.doCheckpoint();
  }
  
  /** Test append random size data */
  @Test
  public void testTC3() throws Exception {
    TestAvatarAppendInjectionHandler h = new TestAvatarAppendInjectionHandler();
    InjectionHandler.set(h);
    final Path p = new Path("/TC3/foo");
    LOG.info("Path : " + p);
    
    final int round = 10;
    final int lenLimit = (int) (10 * BLOCK_SIZE);
    // create file.
    int len = random.nextInt(lenLimit);
    {
      FSDataOutputStream out = fs.create(p, false, buffersize, REPLICATION,
          BLOCK_SIZE);
      AppendTestUtil.write(out, 0, len);
      out.close();
    }
    
    // append 10 rounds
    for (int i = 0; i < round; i++) {
      int appendLen = random.nextInt(lenLimit);
      FSDataOutputStream out = fs.append(p);
      AppendTestUtil.write(out, len, appendLen);
      out.close();
      len += appendLen;
    }
    
    // verify the file.
    AppendTestUtil.check(fs, p, len);
    
    h.doCheckpoint();
  }

  /**
   * 10 Threads to append files at the same time and perform a name-node fail
   * over.
   */
  @Test
  public void testAppendWithFailover() throws Exception {
    int numThreads = 10;
    Thread[] threads = new Thread[numThreads];
    for (int i = 0; i < numThreads; i++) {
      final Path p = new Path("/Append/failover" + i);
      threads[i] = new AppendWorker(p);
      threads[i].run();
    }

    for (int i = 0; i < numThreads; i++) {
      threads[i].join();
    }

    Thread.sleep(1000);
    cluster.failOver();
  }

  class TestAvatarAppendInjectionHandler extends InjectionHandler {
    private CheckpointTrigger ckptTrigger = new CheckpointTrigger();

    @Override
    protected void _processEvent(InjectionEventI event, Object... args) {
      ckptTrigger.checkpointDone(event, args);
    }

    @Override
    protected boolean _falseCondition(InjectionEventI event, Object... args) {
      return ckptTrigger.triggerCheckpoint(event);
    }

    void doCheckpoint() throws IOException {
      ckptTrigger.doCheckpoint();
    }
  }
}
