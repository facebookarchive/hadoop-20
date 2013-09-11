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

import static org.junit.Assert.assertTrue;

import java.net.URI;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CopyFilesBase.MyFile;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.tools.HadoopArchives;
import org.apache.hadoop.util.ToolRunner;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestHadoopArchives {
  static private MiniDFSCluster dfscluster;
  
  final static Log LOG = LogFactory.getLog(TestHadoopArchives.class);

  
  @BeforeClass
  static public void setup() throws Exception {
    dfscluster = new MiniDFSCluster(new Configuration(), 5, true, null);
  }
  
  @AfterClass
  static public void tearDown() throws Exception {
    try {
      if (dfscluster != null) {
        dfscluster.shutdown();
      }
    } catch(Exception e) {
      System.err.println(e);
    }
  }
  
  // Test copy to and from dfs
  @Test
  public void testCopy() throws Exception {
    String localDir = CopyFilesBase.TEST_ROOT_DIR + "/srcdat";
    String localDir2 = CopyFilesBase.TEST_ROOT_DIR + "/srcdat2";
    Configuration conf = new Configuration();
    FileSystem localfs = FileSystem.getLocal(conf);

    MyFile[] myFiles = CopyFilesBase.createFiles(localfs, localDir);
    
    FileSystem fs = dfscluster.getFileSystem();
    Path archivePath = new Path(fs.getHomeDirectory(), "srcdat.har");
    
    {
      // copy from Local to hdfs
      String[] args = { "-copyFromLocal", localDir, archivePath.toString() };
      int ret = ToolRunner.run(new HadoopArchives(conf), args);
      assertTrue("failed test", ret == 0);
  
      URI uri = archivePath.toUri();
      // create appropriate har path
      Path harPath = new Path("har://" + uri.getScheme() + "-" + uri.getAuthority() + uri.getPath());
      
      FileSystem harfs = harPath.getFileSystem(conf);
      CopyFilesBase.checkFiles(harfs, archivePath.toString(), myFiles);
    }
    
    {
      // copy from hdfs to local
      localfs.mkdirs(new Path(localDir2));
      String[] args = { "-copyToLocal", archivePath.toString(), localDir2 };
      int ret = ToolRunner.run(new HadoopArchives(conf), args);
      assertTrue("failed test", ret == 0);
      
      CopyFilesBase.checkFiles(localfs, localDir2, myFiles);
    }

    CopyFilesBase.deldir(localfs, localDir);
    CopyFilesBase.deldir(localfs, localDir2);
    fs.delete(archivePath, true);
  }
  
}
