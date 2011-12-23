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
package org.apache.hadoop.hdfs.server.datanode;

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.ReconfigurationException;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestDataNodeVolumeRefresh {

  final private int block_size = 512;
  MiniDFSCluster cluster = null;
  int blocks_num = 10;
  File dataDir = null;
  
  @Before
  public void setUp() throws Exception {
    Configuration conf = new Configuration();
    conf.setLong("dfs.block.size", block_size);
    cluster = new MiniDFSCluster(conf, 1, true, null);
    cluster.waitActive();
  }
  
  @After
  public void tearDown() throws Exception {
    if (cluster != null) {
      cluster.shutdown();
    }
  }
  
  @Test
  public void testVolumeAddRefresh() throws IOException,ReconfigurationException {
    FileSystem fs = cluster.getFileSystem();
    dataDir = new File(cluster.getDataDirectory());
    System.out.println("==Data dir: is " +  dataDir.getPath());
    
    //make new test dir
    File dir3 = new File(dataDir, "data3");
    dir3.mkdir();
    File dir4 = new File(dataDir, "data4");
    dir4.mkdir();

    //1st DataNode has data1 and data2, so add data3 and data4 to it
    String origDataDir = cluster.getDataNodes().get(0).getConf().get("dfs.data.dir");
    String newDataDir = origDataDir;
    newDataDir = newDataDir + ',' + dataDir.getAbsolutePath() + "/data3";
    newDataDir = newDataDir + ',' + dataDir.getAbsolutePath() + "/data4";
    
    System.out.println("==Reconfigure Data dir from " +  origDataDir + " to " + newDataDir);
    cluster.getDataNodes().get(0).reconfigureProperty("dfs.data.dir", newDataDir);
    
    System.out.println("==Done Reconfiguration");
    System.out.println("==Namespaces:");
    for (int namespaceId: cluster.getDataNodes().get(0).getAllNamespaces()) {
      System.out.println("====" + namespaceId);
    }
    
    //check configuration change successfully
    assertEquals("Do not change dirs to " + newDataDir,
        newDataDir, cluster.getDataNodes().get(0).getConf().get("dfs.data.dir"));
    
    // we use only small number of blocks to avoid creating subdirs in the data dir..
    String filename = "/test.txt";
    Path filePath = new Path(filename);
    
    int filesize = block_size*blocks_num;
    System.out.println("==DFSTestUtil.createFile");
    DFSTestUtil.createFile(fs, filePath, filesize, (short) 1, 1L);
    System.out.println("==DFSTestUtil.waitReplication");
    DFSTestUtil.waitReplication(fs, filePath, (short) 1);
    System.out.println("==file " + filename + "(size " +
        filesize + ") is created and replicated");
    
    int namespaceId =  cluster.getDataNodes().get(0).getAllNamespaces()[0];
    //Verify the data dir has been in service.
    File dir3current = new File(dir3, "current");
    assertTrue("data3 was not formatted successfully, no current dir.",
        dir3current.isDirectory());
    File dir3current2 = new File(new File(dir3current, "NS-" + namespaceId), "current");
    assertTrue("data3NS was not formatted successfully, no current dir.",
        dir3current2.isDirectory());
    
    File dir4current = new File(dir4, "current");
    assertTrue("data4 was not formatted successfully, no current dir.",
        dir4current.isDirectory());
    File dir4current2 = new File(new File(dir4current, "NS-" + namespaceId), "current");
    assertTrue("data4NS was not formatted successfully, no current dir.",
        dir4current2.isDirectory());
  }
}
