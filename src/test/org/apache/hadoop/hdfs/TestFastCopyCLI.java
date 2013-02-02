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
import java.net.URLEncoder;

import org.apache.hadoop.conf.Configuration;

import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * 
 * The FileSystem tree for this test is as follows :
 * 
 *              root 
 *             / | | \ 
 *            /  | |  \ 
 *           f1 f2 f3 d1
 *                   /| | \
 *                  / | |  \
 *                 /  | |  | \
 *                d2 f6 f4 f5 d3
 *              / | \         / \
 *             f7 f8 f1      f9 f10
 * 
 * f - file d - directory
 * 
 * This test tries various queries on the filesystem tree to verify CLI behavior
 */
public class TestFastCopyCLI extends FastCopySetupUtil {
  
  private static String srcPrefix;
  
  @BeforeClass
  public static void setUpClass() throws Exception {
    conf = new Configuration();
    remoteConf = new Configuration();
    conf.setBoolean("dfs.datanode.blkcopy.hardlink", false);
    remoteConf.setBoolean("dfs.datanode.blkcopy.hardlink", false);
    FastCopySetupUtil.setUpClass();
    // Each file is prefixed with this.
    srcPrefix = "/testFastCopyShellGlob/";
    generateFilesForGlobTesting(srcPrefix);
  }
  
  @Test
  public void testThreadsCLI() throws Exception {
    testFastCopyShellMultiple(false, new String[] { "-t", "5" });
    testFastCopyShellMultiple(false, new String[] { "--threads", "5" });
  }
  
  @Test
  public void testDirectoryCopyWithWildCard() throws Exception {
    testFastCopyShellGlob(false, new String[] { "d2/f7", "d2/f8", "d2/f1",
        "d3/f9", "d3/f10" }, new String[] { srcPrefix + "d1/d*",
        "/testFastCopyShellGlob/Dst0/" }, srcPrefix + "d1/",
        "/testFastCopyShellGlob/Dst0/", true);
  }

  @Test
  public void testDirectoryCopy() throws Exception {
    testFastCopyShellGlob(false, new String[] { "f4", "f5", "f6", "d2/f7",
        "d2/f8", "d2/f1", "d3/f9", "d3/f10" }, new String[] {
        srcPrefix + "d1/", "/testFastCopyShellGlob/Dst1/" }, srcPrefix + "d1/",
        "/testFastCopyShellGlob/Dst1/d1/", true);
  }
  
  @Test
  public void testWildCardExpansion() throws Exception {
    testFastCopyShellGlob(false, new String[] { "f7", "f8" }, new String[] {
        srcPrefix + "d1/d2/*", "/testFastCopyShellGlob/Dst2/" }, srcPrefix
        + "d1/d2/", "/testFastCopyShellGlob/Dst2/", true);
  }
  
  @Test
  public void testWildCardExpansionWithFiles() throws Exception {
  testFastCopyShellGlob(false, new String[] { "f1", "f2", "f3" },
      new String[] { srcPrefix + "f*", "/testFastCopyShellGlob/Dst3/" },
      srcPrefix, "/testFastCopyShellGlob/Dst3/", true);
  }
  
  @Test
  public void testSingleFileCopy() throws Exception {
    testFastCopyShellGlob(false, new String[] { "f1" }, new String[] {
        srcPrefix + "f1", "/testFastCopyShellGlob/Dst4/f1" }, srcPrefix,
        "/testFastCopyShellGlob/Dst4/", false);
  }
  
  @Test
  public void testSingleFileCopyIntoDirectory() throws Exception {
    testFastCopyShellGlob(false, new String[] { "f1" }, new String[] {
        srcPrefix + "f1", "/testFastCopyShellGlob/Dst5/" }, srcPrefix,
        "/testFastCopyShellGlob/Dst5/", true);
  }
  
  @Test
  public void testSingleFileCopyWithOverWrite() throws Exception {
    testFastCopyShellGlob(false, new String[] { "f1" }, new String[] {
        srcPrefix + "f1", srcPrefix + "d1/d2/f1" }, srcPrefix, srcPrefix
        + "d1/d2/", false);
  }

  @Test
  public void testDstNotDirectory() throws Exception {
    try {
      testFastCopyShellGlob(false, new String[] { "f1" }, new String[] {
        srcPrefix + "d1/", srcPrefix + "f1" }, "", "" 
        , false);
    } catch (IllegalArgumentException e) {
      return;
    }
    fail("Did not throw IllegalArgumentException");
  }

  @Test
  public void testDstDirectoryNonExistent() throws Exception {
    try {
      testFastCopyShellGlob(false, new String[] { "f1" }, new String[] {
        srcPrefix + "d1/", srcPrefix + "d1/d2/", srcPrefix + "dx1/"}, "",
        "", false);
    } catch (IllegalArgumentException e) {
      return;
    }
    fail("Did not throw IllegalArgumentException");
  }

  @Test
  public void testSingleDirRename() throws Exception {
    testFastCopyShellGlob(false, new String[] { "f9", "f10" }, new String[] {
      srcPrefix + "d1/d3/", srcPrefix + "dx3/"}, srcPrefix + "d1/d3/",
      srcPrefix + "dx3/", false);
  }

  
  @Test
  public void srcNonExistent() throws Exception {
    try {
      testFastCopyShellGlob(false, new String[] { "f1" }, new String[] {
        srcPrefix + "d2/", srcPrefix + "dx2/" }, "",
        "", false);
    } catch (IOException e) {
      return;
    }
    fail("Did not throw IOException");
  }

  @Test
  public void testFileWithColon() throws Exception {
    String prefix = "/testFileWithColon/special/";
    String filename = URLEncoder.encode("localhost:50010", "UTF8");
    String srcfilename = prefix + filename;
    String dstprefix = "/dst" + prefix;
    String dstfilename = dstprefix + filename;
    generateRandomFile(FastCopySetupUtil.fs, srcfilename, TMPFILESIZE);
    testFastCopyShellGlob(false, new String[] { filename },
        new String[] { srcfilename , dstfilename }, prefix , dstprefix , false);
  }
  
  private static void generateFilesForGlobTesting(String prefix) throws IOException {
    String[] files = { "f1", "f2", "f3", "d1/f4", "d1/f5", "d1/f6",
        "d1/d2/f7", "d1/d2/f8", "d1/d3/f9", "d1/d3/f10", "d1/d2/f1" };
    for (String file : files) {
      generateRandomFile(FastCopySetupUtil.fs, prefix + file, TMPFILESIZE);
    }
  }
}
