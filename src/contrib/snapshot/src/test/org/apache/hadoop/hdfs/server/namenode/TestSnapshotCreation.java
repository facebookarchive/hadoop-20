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
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.server.common.Storage.*;
import org.apache.hadoop.hdfs.server.common.StorageInfo;
import org.apache.hadoop.hdfs.server.namenode.SnapshotNode.*;
import org.apache.hadoop.net.NetUtils;

/**
 * Tests the entire Snapshot creation flow.
 */
public class TestSnapshotCreation extends TestCase {

  private static final Log LOG = LogFactory.getLog(TestSnapshotCreation.class);

  private FileSystem dfs;
  private String ssDir;
  private FSNamesystem namesystem;

  public void testSnapshotCreation() throws IOException {
    Configuration conf = new Configuration();
    MiniDFSCluster cluster = new MiniDFSCluster(conf, 4, true, null);

    SnapshotNode ssNode = new SnapshotNode(conf);
    dfs = cluster.getFileSystem();
    ssDir = conf.get("fs.snapshot.dir", "/.SNAPSHOT");
    FSDataOutputStream out;

    Path foo = new Path("/foo");
    Path bar = new Path("/hadoop/bar");

    out = dfs.create(foo);
    out.writeBytes("foo");
    out.close();

    // Keep bar as file under instruction
    out = dfs.create(bar);
    out.writeBytes("bar");
    out.sync(); // sync to flush stream buffer to NN

    ssNode.createSnapshot("leaseNotUpdated_V1", false);
    ssNode.createSnapshot("leaseUpdated_V1", true);

    out.writeBytes("bar");
    out.close();

    dfs.delete(foo);
    System.out.println("WOOT");
    ssNode.createSnapshot("leaseNotUpdated_V2", false);
    ssNode.createSnapshot("leaseUpdated_V2", true);

    // Shutdown ssNode
    ssNode.shutdown();

    INodeFile file;
    FSNamesystem fsNamesys;

    // Verify state of leaseNotUpdated_V1
    file = getINodeFile("leaseNotUpdated_V1", "/foo");
    assertNotNull(file);
    assertTrue(file.computeContentSummary().getLength() == 3);
    file = getINodeFile("leaseNotUpdated_V1", "/hadoop/bar");
    assertNotNull(file);
    assertTrue(file.computeContentSummary().getLength() == 1);
    namesystem.dir.updateCountForINodeWithQuota();
    // /, /foo, /hadoop, /hadoop/bar, /.SNAPSHOT
    assertTrue(namesystem.dir.rootDir.numItemsInTree() == 5);

    // Verify state of leaseUpdated_V1
    file = getINodeFile("leaseUpdated_V1", "/foo");
    assertNotNull(file);
    assertTrue(file.computeContentSummary().getLength() == 3);
    file = getINodeFile("leaseUpdated_V1", "/hadoop/bar");
    assertNotNull(file);
    assertTrue(file.computeContentSummary().getLength() == 3);
    namesystem.dir.updateCountForINodeWithQuota();
    // Adds /tmp, /.SNAPSHOT/dfs_snapshot_leaseNotUpdated_V1
    assertTrue(namesystem.dir.rootDir.numItemsInTree() == 7);

    // Verify state of leaseNotUpdated_V2
    file = getINodeFile("leaseNotUpdated_V2", "/foo");
    assertNull(file);
    file = getINodeFile("leaseNotUpdated_V2", "/hadoop/bar");
    assertNotNull(file);
    assertTrue(file.computeContentSummary().getLength() == 6);
    namesystem.dir.updateCountForINodeWithQuota();
    // Adds /.SNAPSHOT/dfs_snapshot_leaseUpdated_V1, Removes /foo
    assertTrue(namesystem.dir.rootDir.numItemsInTree() == 7);

    // Verify state of leaseUpdated_V2
    file = getINodeFile("leaseUpdated_V2", "/foo");
    assertNull(file);
    file = getINodeFile("leaseUpdated_V2", "/hadoop/bar");
    assertNotNull(file);
    assertTrue(file.computeContentSummary().getLength() == 6);
    namesystem.dir.updateCountForINodeWithQuota();
    // Adds /.SNAPSHOT/dfs_snapshot_leaseNotUpdated_V2
    assertTrue(namesystem.dir.rootDir.numItemsInTree() == 8);
  }

  private INodeFile getINodeFile(String id, String path) throws IOException {
    Configuration conf = new Configuration();
    FSImage fsImage = new FSImage();
    namesystem = new FSNamesystem(fsImage, conf);
    Path ssPath = new Path(ssDir + "/" + SnapshotNode.SSNAME + id);
    FSDataInputStream in = dfs.open(ssPath);
    fsImage.loadFSImage(new File(ssPath.toString()), in);
    INodeFile file = namesystem.dir.getFileINode(path);
    fsImage.close();

    return file;
  }
}
