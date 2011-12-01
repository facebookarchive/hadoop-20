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
package org.apache.hadoop.raid;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;

import junit.framework.TestCase;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;

/**
 * Verifies {@link DirectoryTraversal} retrieves elements under directory tree 
 */
public class TestDirectoryTraversal extends TestCase {
  final static Log LOG = LogFactory.getLog(TestDirectoryTraversal.class);
  final static String TEST_DIR = new File(System.getProperty("test.build.data",
      "build/contrib/raid/test/data")).getAbsolutePath();
  final static int NUM_TESTS = 100;
  final Configuration conf = new Configuration();
  Set<String> dirsCreated = new HashSet<String>();
  Set<String> filesCreated = new HashSet<String>();
  Random rand = new Random();
  int dirAndFileNo = 0;

  public void testLocalFileSystem() throws Exception {
    LOG.info("Start testing local filesystem");
    Path root = new Path(TEST_DIR + Path.SEPARATOR + "dt");
    FileSystem fs = root.getFileSystem(conf);
    verifyDirectoryRetrival(root, fs);
    verifyFileRetrival(root, fs);
  }

  public void testDistributedFileSystem() throws Exception {
    LOG.info("Start testing distributed filesystem");
    MiniDFSCluster dfs = null;
    try {
      dfs = new MiniDFSCluster(conf, 6, true, null);
      dfs.waitActive();
      FileSystem fs = dfs.getFileSystem();
      Path root = new Path(TEST_DIR + Path.SEPARATOR + "dt");
      verifyDirectoryRetrival(root, fs);
      verifyFileRetrival(root, fs);
    } finally {
      if (dfs != null) {
        dfs.shutdown();
      }
    }
  }

  public void verifyDirectoryRetrival(Path root, FileSystem fs) throws Exception {
    for (int i = 0; i < NUM_TESTS; ++i) {
      fs.delete(root, true);
      fs.mkdirs(root);
      fs.deleteOnExit(root);
      try {
        dirsCreated.clear();
        filesCreated.clear();
        createDirectoryTree(root, 5, 5, 0.3, 0.3, fs);
        LOG.info("Directories created:" + dirsCreated.size());
        DirectoryTraversal dt =
            DirectoryTraversal.directoryRetriever(
                Arrays.asList(root), fs, 5, true, true);
        FileStatus dir;
        int dirCount = 0;
        while ((dir = dt.next()) != DirectoryTraversal.FINISH_TOKEN) {
          LOG.info("Get " + dir.getPath().toString().replace(TEST_DIR, ""));
          dirCount += 1;
          String name = getSimpleName(dir);
          assertTrue(dirsCreated.remove(name));
        }
        assertEquals(0, dirsCreated.size());
      } finally {
        fs.delete(root, true);
      }
    }
  }

  public void verifyFileRetrival(Path root, FileSystem fs) throws Exception {
    for (int i = 0; i < NUM_TESTS; ++i) {
      fs.delete(root, true);
      fs.mkdirs(root);
      fs.deleteOnExit(root);
      try {
        dirsCreated.clear();
        filesCreated.clear();
        createDirectoryTree(root, 5, 5, 0.3, 0.3, fs);
        LOG.info("Files created:" + filesCreated.size());
        DirectoryTraversal dt =
          DirectoryTraversal.fileRetriever(Arrays.asList(root), fs, 5, true, true);
        FileStatus file;
        int dirCount = 0;
        while ((file = dt.next()) != DirectoryTraversal.FINISH_TOKEN) {
          LOG.info("Get " + file.getPath().toString().replace(TEST_DIR, ""));
          dirCount += 1;
          String name = getSimpleName(file);
          assertTrue(filesCreated.remove(name));
        }
        assertEquals(0, filesCreated.size());
      } finally {
        fs.delete(root, true);
      }
    }
  }

  private static String getSimpleName(FileStatus dir) {
    String s = dir.getPath().toString();
    int sep = s.lastIndexOf(Path.SEPARATOR);
    return s.substring(sep + 1);
  }

  private void createDirectoryTree(Path root, int maxLevel, int maxSubElements,
      double dirProbability, double fileProbability, FileSystem fs)
      throws IOException {
    if (maxLevel == 0) {
      return;
    }
    for (int i = 0; i < maxSubElements; ++i) {
      if (rand.nextDouble() > dirProbability + fileProbability) {
        continue;
      }
      String name = ++dirAndFileNo + "";
      if (rand.nextDouble() < dirProbability) {
        name = "d" + name;
        dirsCreated.add(name);
        Path dir = new Path(root + Path.SEPARATOR + name);
        fs.mkdirs(dir);
        LOG.info("Created directory " + dir.toString().replace(TEST_DIR, ""));
      } else {
        name = "f" + name;
        filesCreated.add(name);
        Path file = new Path(root + Path.SEPARATOR + name);
        createFile(fs, file);
        LOG.info("Created file " + file.toString().replace(TEST_DIR, ""));
      }
    }
    FileStatus[] dirs = fs.listStatus(root);
    for (int i = 0; i < dirs.length; ++i) {
      if (dirs[i].isDir()) {
        createDirectoryTree(dirs[i].getPath(), maxLevel - 1, maxSubElements,
            dirProbability, fileProbability, fs);
      }
    }
  }

  private static void createFile(FileSystem fs, Path name) throws IOException {
    FSDataOutputStream out = fs.create(name);
    out.close();
  }

}
