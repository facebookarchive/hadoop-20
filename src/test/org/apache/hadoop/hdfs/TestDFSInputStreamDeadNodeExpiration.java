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

import junit.framework.TestCase;

import org.junit.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;

/**
 * These tests make sure that DFSInputStream retries fetching data from datanode
 * marked dead after the expiration time
 */
public class TestDFSInputStreamDeadNodeExpiration extends TestCase {

  @Test
  public void testDFSInputStreamDeadNodeExpiration() throws IOException {

    Configuration conf = new Configuration();
    MiniDFSCluster cluster = new MiniDFSCluster(conf, 3, true, null);
    cluster.waitActive();
    DistributedFileSystem fs = (DistributedFileSystem)cluster.getFileSystem();
    Path filePath = new Path("/testDFSInputStream");
    FSDataOutputStream out = TestFileCreation.createFile(fs, filePath,
        cluster.getDataNodes().size());
    out.write("something".getBytes());
    out.sync();

    conf.setLong("dfs.deadnodes.expiration.window", 60000);
    conf.setLong("dfs.deadnodes.update.interval", 1000);
    
    DFSClient client = new DFSClient(null, cluster.getNameNode(), conf, null);
    DFSInputStream fileHandle = client.open(filePath.toString());

    // get the preferred datanode for the first block
    LocatedBlocks locations = client.namenode.getBlockLocations(
        filePath.toString(), 0, client.defaultBlockSize);

    // kill a datanode
    DatanodeInfo dataNode = locations.get(0).getLocations()[0];

    MiniDFSCluster.DataNodeProperties dnp = cluster.stopDataNode(dataNode.name);
    byte[] buffer = new byte[1];
    fileHandle.read(0, buffer, 0, 1);

    // Make sure that the data node is present in the deadNodes list
    assertTrue(fileHandle.deadNodes.containsKey(dataNode));

    // enough time to expire the node from the dead node list maintained by
    // DFSInputStream
    try {
      Thread.sleep(fileHandle.expirationTimeWindow + 1);
    } catch (InterruptedException e) {
      fail("Fail to sleep the thread: \n" + e.getMessage());
    }

    // restart the datanode with the same properties
    cluster.restartDataNode(dnp);
    fileHandle.read(0, buffer, 0, 1);

    // Make sure the deadNode List does not contain the restarted data node
    assertTrue(!fileHandle.deadNodes.containsKey(dataNode));

    try {
      fileHandle.close();
    } catch (Exception e) {
      fail("Failed to close the DFSInputStream: \n" +
           e.getMessage());
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }
}
