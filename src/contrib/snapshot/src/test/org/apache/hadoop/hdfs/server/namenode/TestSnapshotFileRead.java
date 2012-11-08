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
import org.apache.hadoop.hdfs.SnapshotClient;
import org.apache.hadoop.hdfs.DFSInputStream;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.server.common.Storage.*;
import org.apache.hadoop.hdfs.server.common.StorageInfo;
import org.apache.hadoop.hdfs.server.namenode.SnapshotNode.SnapshotStorage;
import org.apache.hadoop.net.NetUtils;

/**
 * Tests reading files from snapshots
 */
public class TestSnapshotFileRead extends TestCase {

  private static final Log LOG = LogFactory.getLog(TestSnapshotFileRead.class);

  /**
   * Only need to check rename. File can never be deleted
   * because it should always be moved to WaitingRoom which
   * is equivalent to a rename.
   */
  public void testFileRead() throws IOException {
    Configuration conf = new Configuration();
    MiniDFSCluster cluster = new MiniDFSCluster(conf, 4, true, null);

    SnapshotNode ssNode = new SnapshotNode(conf);
    WaitingRoom wr = new WaitingRoom(conf);
    DFSClient client = new DFSClient(conf);
    SnapshotClient ssClient = new SnapshotClient(conf);

    FileSystem dfs = cluster.getFileSystem();
    String ssDir = conf.get("fs.snapshot.dir", "/.SNAPSHOT");

    Path foo = new Path("/foo");
    Path bar = new Path("/bar");

    FSDataOutputStream out;

    out = dfs.create(foo);
    out.write(0);
    out.close();

    out = dfs.create(bar);
    out.write(1);
    out.sync();

    ssNode.createSnapshot("test", true);

    out.write(2);
    out.close();

    wr.moveToWaitingRoom(foo); // delete

    // Current system has foo deleted and bar with length 2
    // test snapshot has foo with length 1 and bar with length 1

    // Checking current file system
    assertTrue(!dfs.exists(foo));
    DFSInputStream in = client.open("/bar");
    assertTrue(in.getFileLength() == 2);
    assertTrue(in.read() == 1);
    assertTrue(in.read() == 2);
    assertTrue(in.read() == -1); //eof

    // Checking test snapshot
    in = ssClient.open("test", "/foo");
    assertTrue(in.getFileLength() == 1);
    assertTrue(in.read() == 0);
    assertTrue(in.read() == -1); //eof
    in = ssClient.open("test", "/bar");
    assertTrue(in.getFileLength() == 1);
    assertTrue(in.read() == 1);
    assertTrue(in.read() == -1); //eof
  }
}
