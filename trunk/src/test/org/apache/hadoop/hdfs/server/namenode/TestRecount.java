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

import static org.junit.Assert.assertEquals;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.tools.DFSAdmin;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestRecount {
  private MiniDFSCluster dfscluster;
  private Configuration conf = new Configuration();
  private FileSystem fs;
  private FSNamesystem namesystem;

  @Before
  public void setup() throws IOException {
    dfscluster = new MiniDFSCluster(conf, 0, true, null);
    dfscluster.waitActive();
    fs = dfscluster.getFileSystem();
    namesystem = dfscluster.getNameNode().namesystem;
  }
  
  @After
  public void teardown() throws IOException {
    dfscluster.shutdown();
  }
  
  @Test
  public void testRecount() throws Exception {
    fs.mkdirs(new Path("/user/user1"));
    assertEquals(3, namesystem.getFilesAndDirectoriesTotal());
    namesystem.dir.rootDir.setSpaceConsumed(2, 0);
    assertEquals(2, namesystem.getFilesAndDirectoriesTotal());
    DFSAdmin adm = new DFSAdmin(conf);
    adm.run(new String[]{"-recount"});
    assertEquals(3, namesystem.getFilesAndDirectoriesTotal());
  }
}
