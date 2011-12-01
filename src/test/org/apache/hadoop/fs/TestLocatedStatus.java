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
package org.apache.hadoop.fs;

import java.io.IOException;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;

import org.apache.commons.logging.impl.Log4JLogger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Level;

import static org.junit.Assert.*;
import org.junit.Test;
import org.junit.BeforeClass;

/**
 * This class tests the FileStatus API.
 */
public class TestLocatedStatus {
  {
    ((Log4JLogger)FileSystem.LOG).getLogger().setLevel(Level.ALL);
  }

  static final long seed = 0xDEADBEEFL;

  final protected static Configuration conf = new Configuration();
  protected static FileSystem fs;
  protected static boolean isHDFS = false;
  final protected static Path TEST_DIR = getTestDir();
  final private static int FILE_LEN = 10;
  final private static Path FILE1 = new Path(TEST_DIR, "file1");
  final private static Path DIR1 = new Path(TEST_DIR, "dir1");
  final private static Path FILE2 = new Path(DIR1, "file2");
  final private static Path FILE3 = new Path(DIR1, "file3");
  final private static Path FILE4 = new Path(TEST_DIR, "file4");

  protected static Path getTestDir() {
    return new Path(
      System.getProperty("test.build.data","build/test/data/work-dir/localfs"),
      "main_");
  }
  
  @BeforeClass
  public static void testSetUp() throws Exception {
    fs = FileSystem.getLocal(conf);
    fs.delete(TEST_DIR, true);
  }
  
  private static void writeFile(FileSystem fileSys, Path name, int fileSize)
  throws IOException {
    // Create and write a file that contains three blocks of data
    FSDataOutputStream stm = fileSys.create(name);
    byte[] buffer = new byte[fileSize];
    Random rand = new Random(seed);
    rand.nextBytes(buffer);
    stm.write(buffer);
    stm.close();
  }
  
  /** Test when input path is a file */
  @Test
  public void testFile() throws IOException {
    fs.mkdirs(TEST_DIR);
    writeFile(fs, FILE1, FILE_LEN);

    RemoteIterator<LocatedFileStatus> itor = fs.listLocatedStatus(
        FILE1);
    LocatedFileStatus stat = itor.next();
    assertFalse(itor.hasNext());
    assertFalse(stat.isDir());
    assertEquals(FILE_LEN, stat.getLen());
    assertEquals(fs.makeQualified(FILE1), stat.getPath());
    assertEquals(1, stat.getBlockLocations().length);
    assertEquals(-1, stat.getChildrenCount());
    
    fs.delete(FILE1, true);
  }


  /** Test when input path is a directory */
  @Test
  public void testDirectory() throws IOException {
    fs.mkdirs(DIR1);

    // test empty directory
    RemoteIterator<LocatedFileStatus> itor = fs.listLocatedStatus(DIR1);
    assertFalse(itor.hasNext());
    
    // testing directory with 1 file
    writeFile(fs, FILE2, FILE_LEN);    
    itor = fs.listLocatedStatus(DIR1);
    LocatedFileStatus stat = itor.next();
    assertFalse(itor.hasNext());
    assertFalse(stat.isDir());
    assertEquals(FILE_LEN, stat.getLen());
    assertEquals(-1, stat.getChildrenCount());
    assertEquals(fs.makeQualified(FILE2), stat.getPath());
    assertEquals(1, stat.getBlockLocations().length);
    
    // test more complicated directory
    writeFile(fs, FILE1, FILE_LEN);
    writeFile(fs, FILE3, FILE_LEN);
    writeFile(fs, FILE4, FILE_LEN);

    Set<Path> expectedResults = new TreeSet<Path>();
    expectedResults.add(fs.makeQualified(FILE2));
    expectedResults.add(fs.makeQualified(FILE3));
    for (itor = fs.listLocatedStatus(DIR1); itor.hasNext(); ) {
      stat = itor.next();
      assertFalse(stat.isDir());
      assertTrue(expectedResults.remove(stat.getPath()));
    }
    assertTrue(expectedResults.isEmpty());
    
    final Path qualifiedDir1 = fs.makeQualified(DIR1);
    expectedResults.add(qualifiedDir1);
    expectedResults.add(fs.makeQualified(FILE1));
    expectedResults.add(fs.makeQualified(FILE4));
    
    for (itor = fs.listLocatedStatus(TEST_DIR); itor.hasNext(); ) {
      stat = itor.next();
      assertTrue(expectedResults.remove(stat.getPath()));
      if (qualifiedDir1.equals(stat.getPath())) {
        assertTrue(stat.isDir());
        if (isHDFS) {
          assertEquals(2, stat.getChildrenCount());
        }
      } else {
        assertFalse(stat.isDir());
        assertEquals(-1, stat.getChildrenCount());
      }
    }
    assertTrue(expectedResults.isEmpty());
    
    fs.delete(TEST_DIR, true);
  }
}