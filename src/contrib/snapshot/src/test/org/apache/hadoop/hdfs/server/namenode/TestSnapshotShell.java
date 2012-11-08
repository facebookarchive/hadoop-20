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
package org.apache.hadoop.hdfs.server.namenode;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import junit.framework.TestCase;
import java.io.*;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.DFSLocatedBlocks;
import org.apache.hadoop.hdfs.DFSInputStream;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.server.common.Storage.*;
import org.apache.hadoop.hdfs.server.common.StorageInfo;
import org.apache.hadoop.hdfs.server.protocol.SnapshotProtocol;
import org.apache.hadoop.hdfs.server.namenode.SnapshotNode.*;
import org.apache.hadoop.net.NetUtils;

/**
 * Tests the SnapshotShell and SnapshotProtocol
 */
public class TestSnapshotShell extends TestCase {

  private static final Log LOG = LogFactory.getLog(TestSnapshotShell.class);

  private FileSystem dfs;
  private String ssDir;

  public void testSnapshotShell() throws IOException {
    Configuration conf = new Configuration();
    MiniDFSCluster cluster = new MiniDFSCluster(conf, 4, true, null);

    SnapshotNode ssNode = new SnapshotNode(conf);
    SnapshotShell ssShell = new SnapshotShell(conf);
    SnapshotProtocol ssProtocol = ssShell.getSnapshotNode();

    dfs = cluster.getFileSystem();
    ssDir = conf.get("fs.snapshot.dir", "/.SNAPSHOT");
    FSDataOutputStream out;

    // Test -create
    String[] argv = new String[2]; argv[0] = "-create"; argv[1] = "test";
    try {
      assertTrue(ssShell.run(argv) == 0);
    } catch (Exception e) {
      fail();
    }
    assertTrue(dfs.exists(new Path(ssDir + "/" + SnapshotNode.SSNAME + "test")));

    argv = new String[3]; argv[0] = "-create"; 
    argv[1] = "testNoLease"; argv[2] = "-ignoreleases";
    try {
      assertTrue(ssShell.run(argv) == 0);
    } catch (Exception e) {
      fail();
    }
    assertTrue(dfs.exists(new Path(ssDir + "/" + SnapshotNode.SSNAME + "testNoLease")));

    // Test -list 
    argv = new String[1]; argv[0] = "-list";
    try {
      assertTrue(ssShell.run(argv) == 0);
    } catch (Exception e) {
      fail();
    }
    String[] ids = ssProtocol.listSnapshots();
    assertTrue(ids.length == 2);
    assertTrue(!ids[0].equals(ids[1]));
    assertTrue(ids[0].equals("test") || ids[0].equals("testNoLease"));
    assertTrue(ids[1].equals("test") || ids[1].equals("testNoLease"));

    // Test getSnapshotFileStatus
    FileStatus status = ssProtocol.getSnapshotFileStatus("test");
    assertNotNull(status);
    assertTrue(status.getPath().getName().equals(SnapshotNode.SSNAME + "test"));
    status = ssProtocol.getSnapshotFileStatus("testNoLease");
    assertNotNull(status);
    assertTrue(status.getPath().getName().equals(SnapshotNode.SSNAME + "testNoLease"));

    // Test -delete
    argv = new String[2]; argv[0] = "-delete"; argv[1] = "test";
    try {
      assertTrue(ssShell.run(argv) == 0);
      argv[1] = "testNoLease";
      assertTrue(ssShell.run(argv) == 0);
    } catch (Exception e) {
      fail();
    }
    assertFalse(dfs.exists(new Path(ssDir + "/" + SnapshotNode.SSNAME + "test")));
    assertFalse(dfs.exists(new Path(ssDir + "/" + SnapshotNode.SSNAME + "testNoLease")));

    // Test getLocatedBlocks
    Path foo = new Path("/bar/foo");
    Path woot = new Path("/bar/woot");
    out = dfs.create(foo);
    out.writeByte(0);
    out.close();
    out = dfs.create(woot); // under construction
    out.writeByte(0);
    out.sync();

    ssProtocol.createSnapshot("test", true);
    out.close();

    LocatedBlocks[] blocksArr = ssProtocol.getLocatedBlocks("test", "/bar/foo");
    assertTrue(blocksArr.length == 1); // 1 file

    DFSClient client = new DFSClient(conf);
    DFSInputStream stm = client.open("/bar/foo");
    LocatedBlocks locBlks = blocksArr[0];
    DFSLocatedBlocks dfsLocBlks = stm.fetchLocatedBlocks();
    stm.close();

    assertTrue(locBlks.locatedBlockCount() == 1); // one byte so must be one block
    assertTrue(locBlks.locatedBlockCount() == dfsLocBlks.locatedBlockCount());
    assertTrue(locBlks.get(0).getBlock().getBlockId() == 
               dfsLocBlks.get(0).getBlock().getBlockId());
    assertTrue(locBlks.getFileLength() == 1);

    blocksArr = ssProtocol.getLocatedBlocks("test", "/bar/woot");
    assertTrue(blocksArr.length == 1); // 1 file

    stm = client.open("/bar/woot");
    locBlks = blocksArr[0];
    dfsLocBlks = stm.fetchLocatedBlocks();
    stm.close();

    assertTrue(locBlks.locatedBlockCount() == 1); // one byte so must be one block
    assertTrue(locBlks.locatedBlockCount() == dfsLocBlks.locatedBlockCount());
    assertTrue(locBlks.get(0).getBlock().getBlockId() ==
               dfsLocBlks.get(0).getBlock().getBlockId());
    assertTrue(locBlks.getFileLength() == 1);

    blocksArr = ssProtocol.getLocatedBlocks("test", "/bar");
    assertTrue(blocksArr.length == 2); // 2 files, foo and woot
  }
}
