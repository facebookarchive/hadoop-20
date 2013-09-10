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
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.FSConstants;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestINodeDirectoryReplaceChild {
  private Configuration conf;
  private MiniDFSCluster cluster;
  private FileSystem fs;
  private DistributedFileSystem dfs;
  
  @Before
  public void setUp() throws IOException {
    conf = new Configuration();
    cluster = new MiniDFSCluster(conf, 2, true, null);
    fs = cluster.getFileSystem();
    assertTrue("Not a HDFS: "+fs.getUri(),
                fs instanceof DistributedFileSystem);
    dfs = (DistributedFileSystem)fs;
  }
  
  @After
  public void tearDown() throws IOException {
    if (cluster != null) {
      cluster.shutdown();
    }
    if (fs != null) {
      fs.close();
    }
  }
  
  /**
   * Test make sure after set quota, all the parent-children relationship are wired up correctly
   */
  @Test
  public void testSetQuota() throws IOException {
    int consFileSpace = 2048;
    FSDirectory fsd = cluster.getNameNode().namesystem.dir;
    Path dir = new Path("/qdir1/qdir2/qdir3");
    assertTrue(dfs.mkdirs(dir));
    dir = new Path("/qdir1/qdir2/qdir4");
    assertTrue(dfs.mkdirs(dir));
    Path quotaDir = new Path("/qdir1/qdir2");
    dfs.setQuota(quotaDir, FSConstants.QUOTA_DONT_SET, 4 * consFileSpace);
    ContentSummary c = dfs.getContentSummary(quotaDir);
    assertEquals(c.getDirectoryCount(), 3);
    assertEquals(c.getSpaceQuota(), 4 * consFileSpace);
    
    INodeDirectory qdir2 = (INodeDirectory)fsd.getINode("/qdir1/qdir2");
    INode qdir3 = fsd.getINode("/qdir1/qdir2/qdir3");
    INode qdir4 = fsd.getINode("/qdir1/qdir2/qdir4");
    
    assertSame(qdir2, qdir3.parent);
    assertSame(qdir2.getChild("qdir3"), qdir3);
    assertSame(qdir2.getChild("qdir4"), qdir4);
  }
  
  @Test
  public void testReplaceChild() throws Exception {
    FSDirectory fsd = cluster.getNameNode().namesystem.dir;
    Path dir = new Path("/qdir1/qdir2/qdir3");
    assertTrue(dfs.mkdirs(dir));
    dir = new Path("/qdir1/qdir2/qdir4");
    assertTrue(dfs.mkdirs(dir));
    INodeDirectory qdir1 = (INodeDirectory)fsd.getINode("/qdir1");
    INodeDirectory qdir2 = (INodeDirectory)fsd.getINode("/qdir1/qdir2");
    INodeDirectoryWithQuota newQdir = new INodeDirectoryWithQuota(1024, 1024, qdir2);
    qdir1.replaceChild(newQdir);
    
    
    INodeDirectory newQdir2 = (INodeDirectory)fsd.getINode("/qdir1/qdir2");
    INodeDirectory qdir3 = (INodeDirectory)fsd.getINode("/qdir1/qdir2/qdir3");
    
    assertSame(newQdir2, qdir3.parent);
    assertSame(newQdir2.getChild("qdir3"), qdir3);
    
    
  }
}
