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

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.TestTrash;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * This class tests commands from Trash.
 */
public class TestHDFSTrash extends TestTrash {

  private MiniDFSCluster cluster = null;
  
  @Before
  public void setUp() throws Exception {
    File testDir = new File(TEST_DIR.toUri().getPath());
    FileUtil.fullyDelete(testDir);
    testDir.mkdirs();    
    
    Configuration conf = new Configuration();
    cluster = new MiniDFSCluster(conf, 2, true, null);
    cluster.waitActive();
  }
  
  @After
  public void tearDown() throws Exception {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  /**
   * Tests Trash on HDFS
   */
  @Test
  public void testTrash() throws IOException {
    trashShell(cluster.getFileSystem(), new Path("/"));
  }

  @Test
  public void testNonDefaultFS() throws IOException {
    FileSystem fs = cluster.getFileSystem();
    Configuration conf = fs.getConf();
    conf.set("fs.default.name", fs.getUri().toString());
    trashNonDefaultFS(conf);
  }

  @Test
  public void testTrashEmptier() throws Exception {
    FileSystem fs = cluster.getFileSystem();
    Configuration conf = fs.getConf();
    conf.set("fs.default.name", fs.getUri().toString());
    trashEmptier(fs, conf);
  }
}
