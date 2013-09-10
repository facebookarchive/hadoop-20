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

import junit.framework.TestCase;
import java.io.*;
import java.net.URI;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.hdfs.server.namenode.NameNode;

/**
 * This class tests the FileStatus API.
 */
public class TestFileStatus extends TestCase {
  static final long seed = 0xDEADBEEFL;
  static final int blockSize = 8192;
  static final int fileSize = 16384;
  private static final String USER_DIR = "/user/"
      + System.getProperty("user.name");
  
  private void writeFile(FileSystem fileSys, Path name, int repl,
                         int fileSize, int blockSize)
    throws IOException {
    // create and write a file that contains three blocks of data
    FSDataOutputStream stm = fileSys.create(name, true,
                                            fileSys.getConf().getInt("io.file.buffer.size", 4096),
                                            (short)repl, (long)blockSize);
    byte[] buffer = new byte[fileSize];
    Random rand = new Random(seed);
    rand.nextBytes(buffer);
    stm.write(buffer);
    stm.close();
  }

  private void checkFile(FileSystem fileSys, Path name, int repl)
    throws IOException {
    DFSTestUtil.waitReplication(fileSys, name, (short) repl);
  }


  /**
   * Tests various options of DFSShell.
   */
  public void testFileStatus() throws IOException {
    Configuration conf = new Configuration();
    conf.setInt("dfs.ls.limit", 2);
    MiniDFSCluster cluster = new MiniDFSCluster(conf, 1, true, null);
    FileSystem fs = cluster.getFileSystem();
    final DFSClient dfsClient = new DFSClient(NameNode.getClientProtocolAddress(conf), conf);
    try {

      //
      // check that / exists
      //
      Path path = new Path("/");
      System.out.println("Path : \"" + path.toString() + "\"");
      assertTrue("/ should be a directory", 
                 fs.getFileStatus(path).isDir() == true);
      
      // make sure getFileInfo returns null for files which do not exist
      FileStatus fileInfo = dfsClient.getFileInfo("/noSuchFile");
      assertTrue(fileInfo == null);

      // create a file in home directory
      //
      Path file1 = new Path("filestatus.dat");
      writeFile(fs, file1, 1, fileSize, blockSize);
      System.out.println("Created file filestatus.dat with one "
                         + " replicas.");
      checkFile(fs, file1, 1);
      System.out.println("Path : \"" + file1 + "\"");

      // test getFileStatus on a file
      FileStatus status = fs.getFileStatus(file1);
      assertTrue(file1 + " should be a file", 
          status.isDir() == false);
      assertTrue(status.getBlockSize() == blockSize);
      assertTrue(status.getReplication() == 1);
      assertTrue(status.getLen() == fileSize);
      assertEquals(fs.makeQualified(file1).toString(), 
          status.getPath().toString());

      // test listStatus on a file
      FileStatus[] stats = fs.listStatus(file1);
      assertEquals(1, stats.length);
      status = stats[0];
      assertTrue(file1 + " should be a file", 
          status.isDir() == false);
      assertTrue(status.getBlockSize() == blockSize);
      assertTrue(status.getReplication() == 1);
      assertTrue(status.getLen() == fileSize);
      assertEquals(fs.makeQualified(file1).toString(), 
          status.getPath().toString());

      // test file status on a directory
      Path dir = new Path("/test/mkdirs");

      // test listStatus on a non-existent file/directory
      stats = fs.listStatus(dir);
      assertEquals(null, stats);
      try {
      status = fs.getFileStatus(dir);
      assertEquals(null, status);
      fail("Expect to receive a FileNotFoundException");
      } catch (FileNotFoundException e) {
      }
      
      // create the directory
      assertTrue(fs.mkdirs(dir));
      assertTrue(fs.exists(dir));
      System.out.println("Dir : \"" + dir + "\"");

      // test getFileStatus on an empty directory
      status = fs.getFileStatus(dir);
      assertTrue(dir + " should be a directory", status.isDir());
      assertTrue(dir + " should be zero size ", status.getLen() == 0);
      assertTrue(dir + " should have 0 children ", status.getChildrenCount() == 0);
      assertEquals(fs.makeQualified(dir).toString(), 
          status.getPath().toString());
      // test listStatus on an empty directory
      stats = fs.listStatus(dir);
      assertEquals(dir + " should be empty", 0, stats.length);
      assertTrue(dir + " should be zero size ",
                 fs.getContentSummary(dir).getLength() == 0);
      assertTrue(dir + " should be zero size ",
                 fs.getFileStatus(dir).getLen() == 0);
      System.out.println("Dir : \"" + dir + "\"");

      // create another file that is smaller than a block.
      //
      Path file2 = new Path(dir, "filestatus2.dat");
      writeFile(fs, file2, 1, blockSize/4, blockSize);
      System.out.println("Created file filestatus2.dat with one "
                         + " replicas.");
      checkFile(fs, file2, 1);
      System.out.println("Path : \"" + file2 + "\"");

      // verify file attributes
      status = fs.getFileStatus(file2);
      assertTrue(status.getBlockSize() == blockSize);
      assertTrue(status.getReplication() == 1);
      assertTrue(status.getChildrenCount() == -1);
      file2 = fs.makeQualified(file2);
      assertEquals(file2.toString(), status.getPath().toString());

      // create another file in the same directory
      Path file3 = new Path(dir, "filestatus3.dat");
      writeFile(fs, file3, 1, blockSize/4, blockSize);
      System.out.println("Created file filestatus3.dat with one "
                         + " replicas.");
      checkFile(fs, file3, 1);
      file3 = fs.makeQualified(file3);
      
      // verify that the size of the directory increased by the size 
      // of the two files
      assertTrue(dir + " size should be " + (blockSize/2), 
                 blockSize/2 == fs.getContentSummary(dir).getLength());
       
      // test listStatus on a non-empty directory
      stats = fs.listStatus(dir);
      assertEquals(dir + " should have two entries", 2, stats.length);
      assertEquals(file2.toString(), stats[0].getPath().toString());
      assertEquals(file3.toString(), stats[1].getPath().toString());
      
      // test iterative listing
      // now dir has 2 entries, create one more
      Path dir3 = fs.makeQualified(new Path(dir, "dir3"));
      fs.mkdirs(dir3);
      dir3 = fs.makeQualified(dir3);
      stats = fs.listStatus(dir);
      assertEquals(dir + " should have three entries", 3, stats.length);
      assertEquals(dir3.toString(), stats[0].getPath().toString());
      assertEquals(file2.toString(), stats[1].getPath().toString());
      assertEquals(file3.toString(), stats[2].getPath().toString());
      assertTrue(dir3 + " should have 0 entires", stats[0].getChildrenCount() == 0);
      assertTrue(stats[1].getChildrenCount() == -1);
      assertTrue(stats[2].getChildrenCount() == -1);

      // now dir has 3 entries, create two more
      Path dir4 = fs.makeQualified(new Path(dir, "dir4"));
      fs.mkdirs(dir4);
      dir4 = fs.makeQualified(dir4);
      Path dir5 = fs.makeQualified(new Path(dir, "dir5"));
      fs.mkdirs(dir5);
      dir5 = fs.makeQualified(dir5);
      stats = fs.listStatus(dir);
      assertEquals(dir + " should have five entries", 5, stats.length);
      assertEquals(dir3.toString(), stats[0].getPath().toString());
      assertEquals(dir4.toString(), stats[1].getPath().toString());
      assertEquals(dir5.toString(), stats[2].getPath().toString());
      assertEquals(file2.toString(), stats[3].getPath().toString());
      assertEquals(file3.toString(), stats[4].getPath().toString());
      
      // test if dir has 5 children
      status = fs.getFileStatus(dir);
      assertTrue(dir + " should have five entries", status.getChildrenCount() == 5);
      assertTrue(dir + " should be zero size ", status.getLen() == 0);
    } finally {
      fs.close();
      cluster.shutdown();
    }
  }

  /**
   * Using a MiniDFS cluster, create multilevel directory structures and query
   * them using FileUtil.listStatus
   */
  public void testListStatus() throws IOException {
    Configuration conf = new Configuration();
    MiniDFSCluster cluster = null;
    try {
      cluster = new MiniDFSCluster(conf, 2, true, null);
      URI uri = cluster.getFileSystem().getUri();
      FileSystem.LOG.info("uri=" + uri);
      FileSystem fs = FileSystem.get(uri, new Configuration());

      // Test fully populated tree with no empty directories
      createFullyPopulatedTree(fs, USER_DIR, 3);

      // At full depth
      FileStatus[] statuses = FileUtil.listStatus(fs, new Path(USER_DIR), 3);
      validateStatusCount(statuses, 15); // 7 files 8 leaf level directories

      // At partial depth
      statuses = FileUtil.listStatus(fs, new Path(USER_DIR), 2);
      validateStatusCount(statuses, 7); // 3 files 4 leaf level directories

      fs.delete(new Path(USER_DIR), true);

      // Test non-branching tree with empty directories
      createLinearTree(fs, USER_DIR, 3);

      // At partial depth (5 level tree, from /3 to /0/leaf)
      statuses = FileUtil.listStatus(fs, new Path(USER_DIR), 3);
      validateStatusCount(statuses, 4); // 2 files, 2 dirs (one empty)

      fs.delete(new Path(USER_DIR), true);

    } finally {
      if (cluster != null)
        cluster.shutdown();
    }
  }

  private static void validateStatusCount(FileStatus[] statuses, int i) {
    for (FileStatus f : statuses) {
      FileSystem.LOG.info("Found recursive path: " + f.getPath());
    }
    assertEquals(i, statuses.length);
  }
  
  private static void writeFile(FileSystem fs, Path name) throws IOException {
    FSDataOutputStream stm = fs.create(name);
    stm.writeBytes("42\n");
    stm.close();
  }
  
  private static void createFullyPopulatedTree(FileSystem fs, String prefix,
      int depth) throws IOException {
    /*  
    Create tree as 
      /home
        leaf.file
        /1
          leaf.file
          ...
        /0
          leaf.file
          ...
    */ 
    String sPath0 = prefix + "/0";
    String sPath1 = prefix + "/1";

    Path localFile = new Path(prefix + "/leaf.file").makeQualified(fs);

    FileSystem.LOG.info("Creating path " + prefix);
    writeFile(fs, localFile);

    if (depth > 0) {
      createFullyPopulatedTree(fs, sPath0, depth - 1);
      createFullyPopulatedTree(fs, sPath1, depth - 1);
    }
  }
    
  private static void createLinearTree(FileSystem fs, String prefix,
      int depth) throws IOException {
  /*  
    Create tree as 
    ...
      /1
        leaf.file
        empty dir   
        /0
          leaf.file
          empty dir
    */
    
    String sPath = prefix + "/" + Integer.toString(depth);
    Path localFile = new Path(sPath + "/leaf stress_chars_{}[]*.file")
        .makeQualified(fs);
    Path emptyDirPath = new Path(sPath + "/empty dir stress_chars_{}[]*")
        .makeQualified(fs);

    if (!fs.mkdirs(emptyDirPath)) {
      throw new IOException("mkdirs failed to create "
          + emptyDirPath.toString());
    }

    FileSystem.LOG.info("Creating path " + sPath);
    writeFile(fs, localFile);

    if (depth > 0) {
      createLinearTree(fs, sPath, depth - 1);
    }
  }
}
