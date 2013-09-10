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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.tools.AvatarDFSck;

import org.junit.AfterClass;
import static org.junit.Assert.*;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;

public class AvatarDFSckTest {

  private static final String FILE_PATH = "/testfile";
  private static final long FILE_LEN = 512L * 1024L;
  private static final long BLOCKS = 16;
  private static final long BLOCK_SIZE = FILE_LEN / BLOCKS;

  private static MiniAvatarCluster cluster;
  private static Configuration conf;
  private static DistributedAvatarFileSystem dafs;
  private static Path path;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    MiniAvatarCluster.createAndStartZooKeeper();
    conf = new Configuration();
    conf.setLong("dfs.block.size", BLOCK_SIZE);
    cluster = new MiniAvatarCluster(conf, 3, true, null, null);
    dafs = cluster.getFileSystem();
    path = new Path(FILE_PATH);
    DFSTestUtil.createFile(dafs, path, FILE_LEN, (short) 1, 0L);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    cluster.shutDown();
    MiniAvatarCluster.shutDownZooKeeper();
  }

  @Test
  public void testDfsckAfterFailover() throws Exception {
    ByteArrayOutputStream os = new ByteArrayOutputStream();
    PrintStream ps = new PrintStream(os);
    new AvatarDFSck(conf, ps).run(new String[] { "/" });
    String expected = os.toString("UTF8");
    cluster.failOver();
    ByteArrayOutputStream os1 = new ByteArrayOutputStream();
    PrintStream ps1 = new PrintStream(os1);
    new AvatarDFSck(conf, ps1).run(new String[] { "/" });
    String actual = os1.toString("UTF8");
    assertEquals(expected, actual);
  }
}
