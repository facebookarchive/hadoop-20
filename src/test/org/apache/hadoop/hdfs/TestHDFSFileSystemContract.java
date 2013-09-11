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
import java.util.Arrays;
import java.util.Random;

import junit.framework.TestCase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileSystemContractBaseTest;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UnixUserGroupInformation;

public class TestHDFSFileSystemContract extends FileSystemContractBaseTest {
  
  private MiniDFSCluster cluster;
  private String defaultWorkingDirectory;

  @Override
  protected void setUp() throws Exception {
    Configuration conf = new Configuration();
    cluster = new MiniDFSCluster(conf, 2, true, null);
    fs = cluster.getFileSystem();
    defaultWorkingDirectory = "/user/" + 
           UnixUserGroupInformation.login().getUserName();
  }
  
  @Override
  protected void tearDown() throws Exception {
    super.tearDown();
    cluster.shutdown();
  }

  @Override
  protected String getDefaultWorkingDirectory() {
    return defaultWorkingDirectory;
  }

  /**
   * Tested semantics: if file is being written by one client and deleted by
   * other client, the file shall be removed (according to definition of
   * FileSystem.delete()) and writing client shall not be able to recreate file
   * by performing operations on FSDataOutputStream object, that was retrieved
   * before deletion. Moreover second client shall be able to create file with
   * the same path using FileSystem.create() method with unset overwrite flag.
   * @throws Throwable
   */
  public void testDeleteFileBeingWrittenTo() throws Throwable {
    final int blockSize = getBlockSize();
    final int len = blockSize * 2;
    byte[] data = new byte[len];
    (new Random()).nextBytes(data);

    Path path = path("/test/hadoop/file");

    fs.mkdirs(path.getParent());

    FSDataOutputStream out = fs.create(path, false,
        fs.getConf().getInt("io.file.buffer.size", 4096), (short) 1,
        getBlockSize());
    out.write(data, 0, blockSize);
    out.sync();

    assertTrue("File does not exist", fs.exists(path));
    assertEquals("Wrong file length", blockSize,
        fs.getFileStatus(path).getLen());

    FileSystem fs2 = cluster.getUniqueFileSystem();
    assertTrue("Delete failed", fs2.delete(path, false));

    assertFalse("File still exists", fs.exists(path));

    try {
      out.write(data, blockSize, len - blockSize);
      out.close();
      fail("Client wrote another block to deleted file.");
    } catch (IOException e) {
      // expected
    }

    assertFalse("File recreated", fs.exists(path));

    FSDataOutputStream out2 = fs2.create(path, false,
        fs2.getConf().getInt("io.file.buffer.size", 4096), (short) 1,
        getBlockSize());
    out2.write(data, 0, len);
    out2.close();

    FSDataInputStream in = fs.open(path);
    byte[] buf = new byte[len];
    in.readFully(0, buf);
    in.close();

    assertTrue("File content does not match", Arrays.equals(data, buf));
  }

  public void testUniqueFileSystem() throws Throwable {
    FileSystem fs1 = cluster.getUniqueFileSystem();
    FileSystem fs2 = cluster.getUniqueFileSystem();

    try {
      DistributedFileSystem dfs1 = DFSUtil.convertToDFS(fs1);
      DistributedFileSystem dfs2 = DFSUtil.convertToDFS(fs2);
      TestCase.assertFalse(dfs1.equals(dfs2));
      String clientName1 = dfs1.dfs.clientName;
      String clientName2 = dfs2.dfs.clientName;
      TestCase.assertFalse(clientName1.equals(clientName2));
      TestCase.assertFalse(clientName1.split("_")[2].equals(clientName2.split("_")[2]));
    } finally {
      fs1.close();
      fs2.close();
    }
  }
  
}
