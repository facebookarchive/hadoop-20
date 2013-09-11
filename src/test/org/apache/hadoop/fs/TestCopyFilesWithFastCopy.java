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

import static org.junit.Assert.*;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.tools.DistCp;
import org.apache.hadoop.util.ToolRunner;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestCopyFilesWithFastCopy extends CopyFilesBase {

  private static MiniDFSCluster cluster;

  private static Configuration conf;

  private static int BLOCK_SIZE = 1024;
  private static int MAX_BLOCKS = 20;
  private static int NUM_NAME_NODES = 2;
  private static FileSystem[] fileSystems = new FileSystem[NUM_NAME_NODES];

  @BeforeClass
  public static void setUpBeforeClass() throws IOException {
    conf = new Configuration();
    final int MAX_IDLE_TIME = 2000; // 2s
    conf.setInt("ipc.client.connection.maxidletime", MAX_IDLE_TIME);
    conf.setInt("dfs.block.size", BLOCK_SIZE);
    conf.setLong("dfs.blockreport.intervalMsec", 0);
    conf.setInt("dfs.heartbeat.interval", 1);
    conf.setInt("heartbeat.recheck.interval", 1000);
    cluster = new MiniDFSCluster(conf, 3, true, null, NUM_NAME_NODES);
  
    for (int i = 0; i < NUM_NAME_NODES; i++) {
      fileSystems[i] = cluster.getFileSystem(i);
    }
  }


  @AfterClass
  public static void tearDownAfterClass() throws IOException {
    cluster.shutdown();
  }

  @Test
  public void testFastCopyNotPreserveRepl() throws Exception {
    String srcDir = "/srcdat";
    FileSystem srcFS = fileSystems[0];
    String srcNameNode = srcFS.getUri().toString();
    String dstDir = "/dstdat";
    FileSystem dstFs = fileSystems[1];
    String dstNameNode = dstFs.getUri().toString();

    assertEquals(3, srcFS.getDefaultReplication());
    assertEquals(3, srcFS.getDefaultReplication());
    MyFile[] files = createFiles(URI.create(srcNameNode), srcDir);
    
    // set the replication
    for (MyFile file : files) {
      srcFS.setReplication(new Path("/srcdat/" + file.getName()), (short)2);
      assertEquals(2, srcFS.getFileStatus(new Path("/srcdat/" + file.getName())).getReplication());
      // make sure the file is replicated
      DFSTestUtil.waitReplication(srcFS, new Path("/srcdat/" + file.getName()), 
                                  (short)2);
    }
    ToolRunner.run(new DistCp(conf), new String[] {
                                     "-usefastcopy",
                                     "-log",
                                     srcNameNode + "/logs",
                                     srcNameNode + srcDir,
                                     dstNameNode + dstDir});

    assertTrue("Source and destination directories do not match.",
        checkFiles(dstFs, dstDir, files));
    assertTrue("Log directory does not exist.",
        srcFS.exists(new Path(srcNameNode+"/logs")));

    // verify the replication
    for (MyFile file : files) {
      assertEquals(2, srcFS.getFileStatus(new Path("/srcdat/" + file.getName())).getReplication());
      assertEquals(3, dstFs.getFileStatus(new Path("/dstdat/" + file.getName())).getReplication());
    }
  }
}
