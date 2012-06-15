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

import java.io.IOException;
import java.util.Random;

import org.apache.commons.logging.impl.Log4JLogger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.TestTrash;
import org.apache.hadoop.fs.TrashPolicy;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.TestFileCreation;
import org.apache.log4j.Level;

public class TestFileDeleteWhitelist extends junit.framework.TestCase {
  static final long seed = 0xDEADBEEFL;
  static final int blockSize = 8192;

  {
    ((Log4JLogger)NameNode.stateChangeLog).getLogger().setLevel(Level.ALL);
    ((Log4JLogger)FSNamesystem.LOG).getLogger().setLevel(Level.ALL);
  }

  //
  // creates a zero file.
  //
  private void createFile(FileSystem fileSys, Path name)
    throws IOException {
    FSDataOutputStream stm = fileSys.create(name, true,
                                            fileSys.getConf().getInt("io.file.buffer.size", 4096),
                                            (short)1, (long)blockSize);
    byte[] buffer = new byte[1024];
    Random rand = new Random(seed);
    rand.nextBytes(buffer);
    stm.write(buffer);
    stm.close();
  }

  public void testFileDeleteWhiteList() throws IOException {
    Configuration conf = new Configuration();
    conf.setInt("heartbeat.recheck.interval", 1000);
    conf.setInt("dfs.heartbeat.interval", 1);
    conf.set("dfs.namenode.whitelist.file", "whitelist.conf");
    conf.setLong("dfs.namenode.config.reload.wait", 10000);  // 10 milliseconds
    conf.setLong("dfs.replication.interval", 2);  // seconds

    // create cluster
    MiniDFSCluster cluster = new MiniDFSCluster(conf, 1, true, null);
    FileSystem fs = null;
    try {
      cluster.waitActive();
      fs = cluster.getFileSystem();
      FSNamesystem namesys = cluster.getNameNode().getNamesystem();

      // create file1.
      Path dir = new Path("/foo");
      Path file1 = new Path(dir, "file1");
      createFile(fs, file1);
      System.out.println("testFileCreationDeleteParent: "
          + "Created file " + file1);

      // create file2.
      Path file2 = new Path(dir, "file2");
      createFile(fs, file2);
      System.out.println("testFileCreationDeleteParent: "
          + "Created file " + file2);

      // tell the namenode to never allow deletion of this path
      namesys.neverDeletePaths.add("/foo/file2");

      // this directory shoudl be deleted sucecssfully
      assertTrue(fs.delete(file1, true));
      System.out.println("testFileCreationDeleteParent: " +
                         "Successfulle deleted " + file1);

      // deleting file2 should fail
      boolean success = false;
      try {
        success = fs.delete(file2, true);
      } catch (Exception e) {
      }
      assertFalse(success);
      System.out.println("testFileCreationDeleteParent: " +
                         "Unable to  deleted " + file2 + " as expected");

      // renaming file2 should fail too
      success = false;
      try {
        success = fs.rename(file2, file1);
      } catch (Exception e) {
      }
      assertFalse(success);
      System.out.println("testFileCreationDeleteParent: " +
                         "Unable to rename " + file2 + " as expected");

    } finally {
      fs.close();
      cluster.shutdown();
    }
  }
  
  public void testFileDeleteWithTrash() throws IOException {
    Configuration conf = new Configuration();
    conf.set("fs.trash.interval", "10"); // 10 minute

    // create cluster
    MiniDFSCluster cluster = new MiniDFSCluster(conf, 1, true, null);
    FileSystem fs = null;
    try {
      cluster.waitActive();
      fs = cluster.getFileSystem();

      // create file1.
      Path dir = new Path("/foo");
      Path file1 = new Path(dir, "file1");
      createFile(fs, file1);
      System.out.println("testFileCreationDeleteParent: "
          + "Created file " + file1);
      fs.delete(file1, true);

      // create file2.
      Path file2 = new Path("/tmp", "file2");
      createFile(fs, file2);
      System.out.println("testFileCreationDeleteParent: "
          + "Created file " + file2);
      fs.delete(file2, true);

      TrashPolicy trashPolicy = TrashPolicy.getInstance(conf, fs, fs.getHomeDirectory());
      Path trashRoot = trashPolicy.getCurrentTrashDir();
      TestTrash.checkTrash(fs, trashRoot, file1);
      TestTrash.checkNotInTrash(fs, trashRoot, file2.toString());
    } finally {
      fs.close();
      cluster.shutdown();
    }
  }
}
