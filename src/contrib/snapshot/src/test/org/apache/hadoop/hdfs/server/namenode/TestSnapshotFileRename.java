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
import java.net.URI;
import java.net.URISyntaxException;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.server.common.Storage.*;
import org.apache.hadoop.hdfs.server.common.StorageInfo;
import org.apache.hadoop.hdfs.server.common.Util;
import org.apache.hadoop.hdfs.server.namenode.SnapshotNode.SnapshotStorage;
import org.apache.hadoop.net.NetUtils;

/**
 * Test cases when file renamed/deleted during Snapshot
 */
public class TestSnapshotFileRename extends TestCase {

  private static final Log LOG = LogFactory.getLog(TestSnapshotFileRename.class);

  private FileSystem dfs;
  private String ssDir;

  /**
   * Only need to check rename. File can never be deleted
   * because it should always be moved to WaitingRoom which
   * is equivalent to a rename.
   */
  public void testFileDeleteRename() throws IOException {
    Configuration conf = new Configuration();
    MiniDFSCluster cluster = new MiniDFSCluster(conf, 4, true, null);

    String tempDir = conf.get("fs.snapshot.tempdir", "/tmp/snapshot");
    SnapshotNode ssNode = new SnapshotNode(conf);
    SnapshotStorage ssStore = new SnapshotStorage(conf, Util.stringAsURI(tempDir));

    dfs = cluster.getFileSystem();
    ssDir = conf.get("fs.snapshot.dir", "/.SNAPSHOT");

    Path rename = new Path("/hadoop/rename");

    FSDataOutputStream renameOut = dfs.create(rename);
    renameOut.writeByte(0);
    renameOut.sync();

    // Download image, edit files from NN and load them
    ssNode.prepareDownloadDirs();
    ssNode.downloadSnapshotFiles(ssStore);
    ssNode.doMerge(ssStore);

    renameOut.close();

    // Rename file
    assertTrue(dfs.rename(rename, new Path("/hadoop/rename_2")));

    // Complete snapshot (w/ update leases)
    ssNode.updateLeasedFiles(ssStore);
    ssNode.saveSnapshot(ssStore, "test");
    ssNode.shutdown();

    // Check lengths
    INodeFile renameFile = getINodeFile("test", "/hadoop/rename");
    INodeFile renameFile2 = getINodeFile("test", "/hadoop/rename_2");

    assertNotNull(renameFile);
    assertTrue(renameFile.computeContentSummary().getLength() == 1);
    assertNull(renameFile2);
  }

  private INodeFile getINodeFile(String id, String path) throws IOException {
    Configuration conf = new Configuration();
    FSImage fsImage = new FSImage();
    FSNamesystem namesystem = new FSNamesystem(fsImage, conf);
    Path ssPath = new Path(ssDir + "/" + SnapshotNode.SSNAME + id);
    FSDataInputStream in = dfs.open(ssPath);
    fsImage.loadFSImage(new File(ssPath.toString()), in);
    INodeFile file = namesystem.dir.getFileINode(path);
    fsImage.close();

    return file;
  }

}
