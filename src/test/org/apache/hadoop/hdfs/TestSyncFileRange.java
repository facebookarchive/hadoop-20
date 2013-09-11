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

import java.io.OutputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.WriteOptions;
import org.apache.hadoop.io.nativeio.NativeIO;
import org.apache.hadoop.util.InjectionHandler;
import org.apache.hadoop.util.InjectionEventI;
import org.apache.hadoop.util.InjectionEventCore;

import org.junit.After;
import static org.junit.Assert.*;
import org.junit.Before;
import org.junit.Test;

public class TestSyncFileRange {

  private static MiniDFSCluster cluster;
  private static Configuration conf;
  private static DistributedFileSystem fs;
  private static volatile int syncFileRange = 0;
  private static final int BLOCK_SIZE = 10 * 1024;
  private static final int FILE_LEN = BLOCK_SIZE / 2;
  private static final byte[] buffer = new byte[FILE_LEN];
  private static final int syncFlag = NativeIO.SYNC_FILE_RANGE_WAIT_AFTER
      | NativeIO.SYNC_FILE_RANGE_WRITE;
  private static int syncFlagCount;

  private static class SyncFileRangeHandler extends InjectionHandler {
    protected void _processEvent(InjectionEventI event, Object... args) {
      if (event == InjectionEventCore.NATIVEIO_SYNC_FILE_RANGE) {
        syncFileRange = (Integer) args[0];
        if (syncFileRange == syncFlag) {
          syncFlagCount++;
        }
      }
    }
  }

  public void setUp(int numDatanodes) throws Exception {
    syncFlagCount = 0;
    conf = new Configuration();
    conf.setInt("dfs.datanode.flush_kb", 1);
    cluster = new MiniDFSCluster(conf, numDatanodes, true, null);
    fs = (DistributedFileSystem) cluster.getFileSystem();
    InjectionHandler.set(new SyncFileRangeHandler());
  }

  @After
  public void tearDown() throws Exception {
    cluster.shutdown();
  }

  private OutputStream writeFile(int flags, int replication) throws Exception {
    WriteOptions options = new WriteOptions();
    options.setSyncFileRange(flags);
    OutputStream out = fs.create(new Path("/test1"), null, true, 1024,
        (short) replication, BLOCK_SIZE, 512, null, null, options);
    out.write(buffer, 0, buffer.length);
    ((FSDataOutputStream) out).sync();
    return out;
  }

  private void runTest(int flags) throws Exception {
    OutputStream out = writeFile(flags, 1);
    long start = System.currentTimeMillis();
    while (syncFileRange != flags
        && System.currentTimeMillis() - start < 30000) {
      Thread.sleep(1000);
    }
    assertEquals(flags, syncFileRange);
    out.close();
  }

  @Test
  public void testSyncFileRange() throws Exception {
    setUp(1);
    int a1 = NativeIO.SYNC_FILE_RANGE_WAIT_AFTER;
    int a2 = NativeIO.SYNC_FILE_RANGE_WAIT_BEFORE;
    int a3 = NativeIO.SYNC_FILE_RANGE_WRITE;

    runTest(a1);
    runTest(a2);
    runTest(a3);
    runTest(a1 | a2);
    runTest(a1 | a3);
    runTest(a2 | a3);
  }

  @Test
  public void testSyncFileRangePipeline() throws Exception {
    setUp(3);
    OutputStream out = writeFile(syncFlag, 3);
    long start = System.currentTimeMillis();
    while (syncFlagCount != 3 && System.currentTimeMillis() - start < 30000) {
      Thread.sleep(1000);
    }
    assertEquals(3, syncFlagCount);
    out.close();
  }
}
