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

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Random;

import junit.framework.TestCase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DFSOutputStream;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.BlockPathInfo;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlockWithMetaInfo;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.datanode.FSDataset;
import org.apache.hadoop.hdfs.server.datanode.FSDatasetTestUtil;
import org.apache.hadoop.hdfs.server.datanode.TestInterDatanodeProtocol;
import org.apache.hadoop.hdfs.server.protocol.BlockMetaDataInfo;
import org.apache.hadoop.hdfs.server.protocol.InterDatanodeProtocol;
import org.apache.hadoop.ipc.Client;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.ClientAdapter;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.util.Progressable;

import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.doAnswer;

import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

public class TestCheckDisk extends junit.framework.TestCase {
  static final int BLOCK_SIZE = 1024;
  static final short REPLICATION_NUM = (short) 1;
  private MiniDFSCluster cluster;
  private Configuration conf;

  @Override
  protected void setUp() throws Exception {
    super.setUp();

    conf = new Configuration();
    conf.setLong("dfs.block.size", BLOCK_SIZE);
    conf.setBoolean("dfs.support.append", true);
    conf.setLong("dfs.datnode.checkdisk.mininterval", 2000);
    cluster = new MiniDFSCluster(conf, 1, true, null);
    cluster.waitActive();
  }

  @Override
  protected void tearDown() throws Exception {
    cluster.shutdown();

    super.tearDown();
  }

  public void testParallelCheckDirs() throws Exception {
    final DataNode datanode = cluster.getDataNodes().get(0);
    FSDataset fsDataset = (FSDataset) datanode.data;
    datanode.data = spy(fsDataset);

    final Method checkDiskMethod = DataNode.class.getDeclaredMethod(
        "checkDiskError", Exception.class);
    checkDiskMethod.setAccessible(true);

    doAnswer(new Answer() {
      public Object answer(InvocationOnMock invocation) {
        try {
          Thread.sleep(1000);
        } catch (InterruptedException e) {
          // TODO Auto-generated catch block
          e.printStackTrace();
        }
        return null;
      }
    }).when(datanode.data).checkDataDir();
    
    Thread[] threads = new Thread[30];
    for (int i = 0; i < 30; i++) {
      threads[i] = new Thread() {
        public void run() {
          try {
            checkDiskMethod.invoke(datanode, new Exception("Fake Exception"));
          } catch (IllegalArgumentException e) {
            TestCase.fail("IllegalArgumentException");
          } catch (IllegalAccessException e) {
            TestCase.fail("IllegalAccessException");
          } catch (InvocationTargetException e) {
            TestCase.fail("InvocationTargetException");
          }
        }
      };
    }

    // Parallel 10 checks should only have one launched.
    for (int i = 0; i < 10; i++) {
      threads[i].start();
    }
    for (int i = 0; i < 10; i++) {
      threads[i].join();
    }
    verify(datanode.data, times(1)).checkDataDir();

    // Next checks won't be launched as one recently finishes.
    for (int i = 10; i < 20; i++) {
      threads[i].start();
    }
    for (int i = 10; i < 20; i++) {
      threads[i].join();
    }
    verify(datanode.data, times(1)).checkDataDir();

    // After 2 seconds, another check should be able to run
    Thread.sleep(2000);
    for (int i = 20; i < 30; i++) {
      threads[i].start();
    }
    for (int i = 20; i < 30; i++) {
      threads[i].join();
    }
    verify(datanode.data, times(2)).checkDataDir();
  }
}
