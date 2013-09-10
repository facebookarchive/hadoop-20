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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import junit.extensions.TestSetup;
import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSClient.DFSDataInputStream;
import org.apache.hadoop.hdfs.DFSClient.MultiDataOutputStream;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.datanode.ReplicaBeingWritten;
import org.apache.hadoop.hdfs.util.InjectionEvent;
import org.apache.hadoop.util.InjectionEventI;
import org.apache.hadoop.util.InjectionHandler;


/** This class implements some of tests posted in HADOOP-2658. */
public class TestAvailableForUnderConstruction extends junit.framework.TestCase {
  static final long BLOCK_SIZE = 64 * 1024;
  static final short REPLICATION = 3;
  static final int DATANODE_NUM = 5;

  private static Configuration conf;
  private static int buffersize;
  private static MiniDFSCluster cluster;
  private static DistributedFileSystem fs;

  public static Test suite() {
    return new TestSetup(new TestSuite(TestAvailableForUnderConstruction.class)) {
      protected void setUp() throws java.lang.Exception {
        AppendTestUtil.LOG.info("setUp()");
        conf = new Configuration();
        conf.setInt("io.bytes.per.checksum", 512);
        conf.setBoolean("dfs.support.append", true);
        conf.setLong("dfs.timeout.get.available.from.datanode", 1000L);
        buffersize = conf.getInt("io.file.buffer.size", 4096);
        cluster = new MiniDFSCluster(conf, DATANODE_NUM, true, null);
        fs = (DistributedFileSystem) cluster.getFileSystem();
      }

      protected void tearDown() throws Exception {
        AppendTestUtil.LOG.info("tearDown()");
        if (fs != null)
          fs.close();
        if (cluster != null)
          cluster.shutdown();
      }
    };
  }

  public void testUpdateAvailable() throws Exception {
    // Fail the test if any node is added to dead nodes.
    // Also verify number of read requests issued to data node is
    // under the correct level.
    final AtomicBoolean needFail = new AtomicBoolean(false);
    final AtomicInteger numReads = new AtomicInteger(0);
    InjectionHandler.set(new InjectionHandler() {
      @Override
      protected void _processEvent(InjectionEventI event, Object... args) {
        if (event == InjectionEvent.DFSCLIENT_BEFORE_ADD_DEADNODES) {
          needFail.set(true);
          try {
            throw new Exception("for call stack");
          } catch (Exception e) {
            e.printStackTrace();
          }
        } else if (event == InjectionEvent.READ_BLOCK_HEAD_BEFORE_WRITE) {
          numReads.incrementAndGet();
        }
      }
    });
    
    final Path p = new Path("/TC1/foo");
    System.out.println("p=" + p);

    final int len1 = (int) BLOCK_SIZE / 4 - 3;
    FSDataOutputStream out = fs.create(p, false, buffersize, REPLICATION,
        BLOCK_SIZE);
    FSDataInputStream in = fs.open(p);
    int available;

    available = in.available();
    TestCase.assertEquals(0, available);

    AppendTestUtil.write(out, 0, len1);
    out.sync();

    available = in.available();
    TestCase.assertEquals(len1, available);
    
    long i = -1;
    for(i++; i < len1; i++) {
      TestCase.assertEquals((byte)i, (byte)in.read());  
    }

    available = in.available();
    TestCase.assertEquals(0, available);

    final int len2 = (int) BLOCK_SIZE / 2 + 3;
    AppendTestUtil.write(out, len1, len2);
    out.sync();
    available = in.available();
    TestCase.assertEquals(len2, available);
    for(; i < len1 + len2; i++) {
      TestCase.assertEquals((byte)i, (byte)in.read());  
    }
    available = in.available();
    TestCase.assertEquals(0, available);

    // test available update until end of the block
    final int len3 = (int) BLOCK_SIZE / 2;
    AppendTestUtil.write(out, len1 + len2, len3);
    out.sync();
    available = in.available();
    TestCase.assertEquals((int) BLOCK_SIZE / 2, available);
    for(; i < len1 + len2 + len3; i++) {
      TestCase.assertEquals(i +" th number is wrong..", (byte)i, (byte)in.read());  
    }
    available = in.available();
    TestCase.assertEquals(0, available);

    final int len4 = (int)BLOCK_SIZE / 4 - 7;
    AppendTestUtil.write(out, len1 + len2 + len3, len4);
    out.sync();
    available = in.available();
    for(; i < len1 + len2 + len3 + len4; i++) {
      TestCase.assertEquals(i +" th number is wrong..", (byte)i, (byte)in.read());  
    }
    
    final int len5 = 2;
    AppendTestUtil.write(out, len1 + len2 + len3 + len4, len5);
    TestCase.assertTrue(in.isUnderConstruction());

    out.sync();

    available = in.available();
    TestCase.assertEquals(len5, available);
    for(; i < len1 + len2 + len3 + len4 + len5; i++) {
      TestCase.assertEquals(i +" th number is wrong..", (byte)i, (byte)in.read());  
    }
    available = in.available();
    TestCase.assertEquals(0, available);

    out.close();

    available = in.available();
    TestCase.assertEquals(0, available);
    TestCase.assertFalse(in.isUnderConstruction());
    
    in.close();

    // b. Reopen file and read 1.5 blocks worth of data. Close file.
    AppendTestUtil.check(fs, p, len1 + len2 + len3 + len4 + len5);
    
    if (needFail.get()) {
      TestCase.fail("Some node is added to dead node, which shouldn't happen.");
    }
    TestCase.assertTrue("Issued more than 16 reads to data nodes, value: " + numReads.get(),
        numReads.get() <= 17);
  }
  
  public void testUpdateAvailableWithBlockRecovery() throws Exception {
    // Fail the test if any node is added to dead nodes.
    final AtomicBoolean needFail = new AtomicBoolean(false);
    InjectionHandler.set(new InjectionHandler() {
      @Override
      protected void _processEvent(InjectionEventI event, Object... args) {
        if (event == InjectionEvent.DFSCLIENT_BEFORE_ADD_DEADNODES) {
          needFail.set(true);
          try {
            throw new Exception("for call stack");
          } catch (Exception e) {
            e.printStackTrace();
          }
        }
      }
    });
    
    final Path p = new Path("/testUpdateAvailableWithBlockRecovery");
    System.out.println("p=" + p);

    final int len1 = (int) BLOCK_SIZE / 4 - 3;
    FSDataOutputStream out = fs.create(p, false, buffersize, REPLICATION,
        BLOCK_SIZE);
    FSDataInputStream in = fs.open(p);

    AppendTestUtil.write(out, 0, len1);
    out.sync();

    // Make sure the input stream select the first datanode to read.
    final DatanodeInfo targetNode = ((DFSOutputStream) (out.getWrappedStream())).nodes[0];
    InjectionHandler.set(new InjectionHandler() {
      @Override
      protected void _processEventIO(InjectionEventI event, Object... args)
          throws IOException {
        if (event == InjectionEvent.DFSCLIENT_BEFORE_BEST_NODE) {
          DatanodeInfo[] nodes = (DatanodeInfo[]) args[0];
          int index = 0;
          for (;index < nodes.length; index++) {
            if (nodes[index].equals(targetNode)) {
              break;
            }
          }
          if (index > 0 && index < nodes.length) {
            DatanodeInfo tempInfo = nodes[0];
            nodes[0] = nodes[index];
            nodes[index] = tempInfo;
          }
        }
      }
    });
    
    int available = in.available();
        
    TestCase.assertEquals(len1, available);
    int i = 0;
    for(; i < len1; i++) {
      TestCase.assertEquals((byte)i, (byte)in.read());  
    }

    // Remove the first datanode out of the pipelinebbccbc
    InjectionHandler.set(new InjectionHandler() {
      int thrownCount = 0;
      @Override
      protected void _processEventIO(InjectionEventI event, Object... args)
          throws IOException {
        if (event == InjectionEvent.DFSCLIENT_DATASTREAM_BEFORE_WRITE
            && thrownCount < 1) {
          thrownCount++;
          MultiDataOutputStream blockStream = (MultiDataOutputStream) args[0];
          blockStream.close();
        }
      }
    });

    final int len2 = (int) BLOCK_SIZE / 4;
    AppendTestUtil.write(out, len1, len2);
    out.sync();

    // After the replica being read is removed from pipeline, DFSInputStream
    // will never able to read new data until timing out and metadata is refetched
    // from namenode. We wait 3 seconds for it to happen.
    for (int j = 0; j < 30; j++) {
      available = in.available();      
      if (available > 0) {
        break;
      } else if (j < 29) {
        System.out.println("Sleeping 100ms for available() returns 0...");
        Thread.sleep(100);
      }
    }
    TestCase.assertEquals(len2, available);
    
    for(; i < len1 + len2; i++) {
      TestCase.assertEquals((byte)i, (byte)in.read());  
    }
    available = in.available();
    TestCase.assertEquals(0, available);

    // test available still works after block recovery
    final int len3 = (int) BLOCK_SIZE / 2;
    AppendTestUtil.write(out, len1 + len2, len3);
    out.sync();
    available = in.available();
    TestCase.assertEquals((int) BLOCK_SIZE / 2, available);
    for(; i < len1 + len2 + len3; i++) {
      TestCase.assertEquals(i +" th number is wrong..", (byte)i, (byte)in.read());  
    }
    available = in.available();
    TestCase.assertEquals(0, available);

    out.close();

    available = in.available();
    TestCase.assertEquals(0, available);
    TestCase.assertFalse(in.isUnderConstruction());
    
    in.close();

    // b. Reopen file and read 1.5 blocks worth of data. Close file.
    AppendTestUtil.check(fs, p, len1 + len2 + len3);
    
    if (needFail.get()) {
      TestCase.fail("Some node is added to dead node, which shouldn't happen.");
    }
  }

  public void testUpdateAvailableWithShrinkedLength() throws Exception {
    final Path p = new Path("/testUpdateAvailableWithShrinkedLength");
    AppendTestUtil.LOG.info("p=" + p);

    final int len1 = (int) BLOCK_SIZE / 4 - 3;
    FSDataOutputStream out = fs.create(p, false, buffersize, REPLICATION,
        BLOCK_SIZE);
    FSDataInputStream in = fs.open(p);

    AppendTestUtil.write(out, 0, len1);
    out.sync();

    int available = in.available();
    
    TestCase.assertEquals(len1, available);
    int i = 0;
    for(; i < len1; i++) {
      TestCase.assertEquals((byte)i, (byte)in.read());  
    }
    
    // Fail bestNodes() 4 out of 5 times, so that when calling available()
    // multiple times, DFSInputStream will start multiple new connections
    // to data nodes and refetch the block length.
    InjectionHandler.set(new InjectionHandler() {
      int failCount = 0;
      @Override
      protected void _processEventIO(InjectionEventI event, Object... args)
          throws IOException {
        if (event == InjectionEvent.DFSCLIENT_BEFORE_BEST_NODE) {
          // Fail once per 5 retries. 
          if (failCount++ % 5 != 3) {
            throw new IOException("Injected IOException.");
          }
        }
      }
    });
    
    // Set visible length of all replicas to be smaller
    int SIZE_TO_SHRINK = 5;
    DFSDataInputStream is = (DFSDataInputStream) in;
    for (DataNode dn : cluster.getDataNodes()) {
      ReplicaBeingWritten rbw = dn.data.getReplicaBeingWritten(
          fs.dfs.getNamespaceId(), is.getCurrentBlock());
      if (rbw != null) {
        rbw.setBytesAcked(len1 - SIZE_TO_SHRINK);
      }
    }

    AppendTestUtil.LOG.info("Checking file available() is 0");
    for (int j = 0; j < 20; j++) {
      TestCase.assertEquals(0, in.available());
    }
    AppendTestUtil.LOG.info("Checking file length");
    TestCase.assertEquals(len1 - SIZE_TO_SHRINK, ((DFSDataInputStream) in).getFileLength());
    InjectionHandler.clear();
    
    // Make sure when new data are coming, the stream can continue.
    final int len2 = (int) BLOCK_SIZE / 4;
    AppendTestUtil.write(out, len1, len2);
    out.sync();
    AppendTestUtil.LOG.info("Extra bytes written and synced.");
    
    // Wait for the available() size is eventually updated.
    for (int j = 0; j < 30; j++) {
      available = in.available();      
      if (available > 0) {
        break;
      } else if (j < 29) {
        AppendTestUtil.LOG.info("Sleeping 100ms for available() returns 0...");
        Thread.sleep(100);
      }
    }
    TestCase.assertEquals(len2, available);
    
    for(; i < len1 + len2; i++) {
      TestCase.assertEquals((byte)i, (byte)in.read());  
    }
    available = in.available();
    TestCase.assertEquals(0, available);

    out.close();
    in.close();

    AppendTestUtil.check(fs, p, len1 + len2);    
  }
}
