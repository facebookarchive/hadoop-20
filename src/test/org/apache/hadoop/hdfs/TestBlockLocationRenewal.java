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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.protocol.FSConstants.DatanodeReportType;
import org.apache.hadoop.hdfs.util.InjectionEvent;
import org.apache.hadoop.util.InjectionHandler;
import org.apache.hadoop.util.InjectionEventI;

import org.junit.After;
import static org.junit.Assert.*;
import org.junit.Before;
import org.junit.Test;

public class TestBlockLocationRenewal {
  private static MiniDFSCluster cluster;
  private static Configuration conf;
  private static DistributedFileSystem fs;
  private static final int BLOCK_SIZE = 1024;
  private static final int BLOCKS = 20;
  private static volatile int blockRenewalDone;
  private static volatile boolean pass = true;

  public void setUp(int minReplication) throws Exception {
    pass = true;
    blockRenewalDone = 0;
    conf = new Configuration();
    conf.setInt("dfsclient.block.location.renewal.interval", 1000);
    conf.setBoolean("dfs.client.block.location.renewal.enabled", true);
    conf.setInt("dfs.block.size", BLOCK_SIZE);
    conf.setLong("dfs.read.prefetch.size", 0);
    conf.setLong("dfs.heartbeat.interval", 1);
    conf.setInt("heartbeat.recheck.interval", 100); // 5 minutes
    conf.setInt("dfs.replication.min", minReplication);
    cluster = new MiniDFSCluster(conf, 3, true, null);
    fs = (DistributedFileSystem) cluster.getFileSystem(conf);
  }

  @After
  public void tearDown() throws Exception {
    fs.close();
    cluster.shutdown();
  }

  private static class RenewalHandler extends InjectionHandler {
    protected void _processEvent(InjectionEventI event, Object... args) {
      if (event == InjectionEvent.DFSCLIENT_BLOCK_RENEWAL_DONE) {
        blockRenewalDone++;
      }
      if (event == InjectionEvent.DFSCLIENT_BLOCK_RENEWAL_EXCEPTION) {
        blockRenewalDone++;
        pass = false;
      }
    }
  }

  @Test
  public void testBasic() throws Exception {
    setUp(1);
    String fileName = "/test";
    Path file = new Path(fileName);
    DFSTestUtil.createFile(fs, file, BLOCKS
        * BLOCK_SIZE, (short) 1, 0);
    DFSInputStream in = fs.dfs.open(fileName);
    // 1 block fetched by default during open.
    assertEquals(1, in.fetchLocatedBlocks().locatedBlockCount());
    byte[] buf = new byte[BLOCK_SIZE * BLOCKS];
    in.read(0, buf, 0, BLOCK_SIZE * 5);
    in.read(BLOCK_SIZE * 6, buf, 0, BLOCK_SIZE * 7);
    in.read(BLOCK_SIZE * 15, buf, 0, BLOCK_SIZE * 3);

    // 15 blocks in cache now
    assertEquals(15, in.fetchLocatedBlocks().locatedBlockCount());

    waitForBlockRenewal();

    // 15 blocks still in cache now
    assertEquals(15, in.fetchLocatedBlocks().locatedBlockCount());
  }

  private void waitForBlockRenewal() throws Exception {
    InjectionHandler.set(new RenewalHandler());

    long start = System.currentTimeMillis();
    while (blockRenewalDone < 2 && System.currentTimeMillis() - start < 30000) {
      Thread.sleep(1000);
    }
    assertTrue(blockRenewalDone >= 2);
  }

  @Test
  public void testDeadDatanode() throws Exception {
    setUp(3);
    String fileName = "/test";
    Path file = new Path(fileName);
    DFSTestUtil.createFile(fs, file, BLOCKS, (short) 3, 0);
    DFSInputStream in = fs.dfs.open(fileName);
    // 1 block fetched by default during open.
    assertEquals(1, in.fetchLocatedBlocks().locatedBlockCount());
    // 3 locations in client cache.
    assertEquals(3, in.fetchLocatedBlocks().getLocatedBlocks().get(0)
        .getLocations().length);

    int live = cluster.getNameNode().getDatanodeReport(DatanodeReportType.LIVE).length;
    assertEquals(3, live);
    cluster.shutdownDataNode(0, false);

    // Wait for datanode to expire.
    long start = System.currentTimeMillis();
    while (live != 2 && System.currentTimeMillis() - start < 30000) {
      Thread.sleep(1000);
      live = cluster.getNameNode().getDatanodeReport(DatanodeReportType.LIVE).length;
    }
    assertEquals(2, live);

    blockRenewalDone = 0;

    waitForBlockRenewal();

    // Dead datanode removed from client cache.
    assertEquals(2, in.fetchLocatedBlocks().getLocatedBlocks().get(0)
        .getLocations().length);
  }

  @Test
  public void deletedFile() throws Exception {
    setUp(1);
    // Create file
    String fileName = "/test";
    Path file = new Path(fileName);
    DFSTestUtil.createFile(fs, file, BLOCK_SIZE, (short) 3, 0);

    // Open input stream to start renewal thread for this file.
    fs.open(file);

    // Delete the file and make sure the renewal thread doesn't hit any exceptions.
    fs.delete(file);
    waitForBlockRenewal();
    assertTrue(pass);
  }

}
