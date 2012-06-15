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

import junit.framework.TestCase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.server.namenode.FSImage;
import org.apache.hadoop.hdfs.server.common.Storage.StorageDirectory;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.io.MD5Hash;

import java.util.Collection;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.ArrayList;

import java.io.File;
import java.io.FileInputStream;
import java.net.URI;

/**
 * A JUnit test for checking if restarting DFS preserves integrity.
 * Specifically with FSImage being written in parallel
 */
public class TestParallelImageWrite extends TestCase {
  /** check if DFS remains in proper condition after a restart */
  public void testRestartDFS() throws Exception {
    MiniDFSCluster cluster = null;
    Configuration conf = new Configuration();
    DFSTestUtil files = new DFSTestUtil("TestRestartDFS", 200, 3, 8*1024);

    final String dir = "/srcdat";
    final Path rootpath = new Path("/");
    final Path dirpath = new Path(dir);

    long rootmtime;
    FileStatus rootstatus;
    FileStatus dirstatus;

    try {
      cluster = new MiniDFSCluster(conf, 4, true, null);
      FileSystem fs = cluster.getFileSystem();
      files.createFiles(fs, dir);

      rootmtime = fs.getFileStatus(rootpath).getModificationTime();
      rootstatus = fs.getFileStatus(dirpath);
      dirstatus = fs.getFileStatus(dirpath);

      fs.setOwner(rootpath, rootstatus.getOwner() + "_XXX", null);
      fs.setOwner(dirpath, null, dirstatus.getGroup() + "_XXX");
    } finally {
      if (cluster != null) { cluster.shutdown(); }
    }
    try {
      // Here we restart the MiniDFScluster without formatting namenode
      cluster = new MiniDFSCluster(conf, 4, false, null);
      FileSystem fs = cluster.getFileSystem();
      assertTrue("Filesystem corrupted after restart.",
                 files.checkFiles(fs, dir));

      final FileStatus newrootstatus = fs.getFileStatus(rootpath);
      assertEquals(rootmtime, newrootstatus.getModificationTime());
      assertEquals(rootstatus.getOwner() + "_XXX", newrootstatus.getOwner());
      assertEquals(rootstatus.getGroup(), newrootstatus.getGroup());

      final FileStatus newdirstatus = fs.getFileStatus(dirpath);
      assertEquals(dirstatus.getOwner(), newdirstatus.getOwner());
      assertEquals(dirstatus.getGroup() + "_XXX", newdirstatus.getGroup());
      rootmtime = fs.getFileStatus(rootpath).getModificationTime();
      List<MD5Hash> checksums = new ArrayList<MD5Hash>();
      Collection<File> nameDirs = cluster.getNameDirs();
      for (File nameDir : nameDirs) {
        File current = new File(nameDir, "current/fsimage");
        FileInputStream in = new FileInputStream(current);
        MD5Hash hash = MD5Hash.digest(in);
        
        checksums.add(hash);
      }
      assertTrue("Not enough fsimage copies in MiniDFSCluster " + 
                   "to test parallel write", checksums.size() > 1);
      for (int i = 1; i < checksums.size(); i++) {
        assertEquals(checksums.get(i - 1), checksums.get(i));
      }
      files.cleanup(fs, dir);
    } finally {
      if (cluster != null) { cluster.shutdown(); }
    }
  }
}
