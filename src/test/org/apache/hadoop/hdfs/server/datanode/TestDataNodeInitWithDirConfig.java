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
import java.io.FileWriter;
import java.io.BufferedWriter;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestDataNodeInitWithDirConfig {

    final private int block_size = 512;
    MiniDFSCluster cluster = null;
    int block_num = 10;
    File dataDir = null;
    Configuration conf = null;

    @Before
    public void setUp() throws Exception {
      conf = new Configuration();
      conf.setLong("dfs.block.size", 512);
      conf.setInt("dfs.datanode.failed.volumes.tolerated", 0);
    }

    @After
    public void tearDown() throws Exception {
      if (cluster != null) {
        cluster.shutdown();
      }
    }

    // Verify that the datanode will start up without any config path value 
    @Test
    public void testNodeInitWithNoDFSDataDirConfPath() throws IOException{
      cluster = new MiniDFSCluster(conf, 1, true, null);
      cluster.waitActive();
      assertTrue(cluster.isDataNodeUp());
    }

    // Verify that the datanode can start up with a null path 
    @Test
    public void testNodeInitWithEmptyDFSDataDirConfPath() throws IOException{
      conf.setStrings("dfs.datadir.confpath", "");
      cluster = new MiniDFSCluster(conf, 1, true, null);
      cluster.waitActive();
      assertTrue(cluster.isDataNodeUp());
    }

    // Verify that the datanode can start up without a correct path 
    @Test
    public void testNodeInitWithWrongDFSDataDirConfPath() throws IOException {
      conf.setStrings("dfs.datadir.confpath", "this/doesnt/exist");
      cluster = new MiniDFSCluster(conf, 1, true, null);
      cluster.waitActive();
      assertTrue(cluster.isDataNodeUp());
    }

    /* Verify that the datanode is up with a true config file with mounts */
    @Test
    public void testNodeInitWithDFSDataDirConfPath() throws IOException {
      File testFile = new File("/tmp/testfile.conf");
      BufferedWriter out = new BufferedWriter(new FileWriter(testFile));
      out.write("/tmp/mnt/dtest0,/tmp/mnt/dtest1");
      out.close();
      // This will delete any temporary directories from previous runs and 
      // make new directories.
      deleteDir(new File("/tmp/mnt/dtest0"));
      File mnt1 = new File("/tmp/mnt/dtest0");
      mnt1.mkdir();
      deleteDir(new File("/tmp/mnt/dtest1"));
      File mnt2 = new File("/tmp/mnt/dtest1");
      mnt2.mkdir();
      conf.setStrings("dfs.data.dir", "");
      conf.setStrings("dfs.datadir.confpath", testFile.getAbsolutePath());
      cluster = new MiniDFSCluster(conf, 1, true, null);
      cluster.waitActive();

      boolean mnt1InUse = true;
      boolean mnt2InUse = true;
      String[] files = mnt1.list();
      System.out.println("The number of files in mnt1 is " + files.length);
      String[] files2 = mnt2.list();
      System.out.println("The number of files in mnt2 is " + files2.length);
      if(files.length == 0) {
        mnt1InUse = false;
      }
      if(files2.length == 0) {
        mnt2InUse = false;
      } 
      assertTrue("mnt1 is not in use currently by MiniDFSCluster", mnt1InUse);
      assertTrue("mnt2 is not in use currently by MiniDFSCluster", mnt2InUse);
    }

    private static boolean deleteDir(File dir) {
      if (dir.isDirectory()) {
        String[] children = dir.list();
        for (int i=0; i<children.length; i++) {
          boolean success = deleteDir(new File(dir, children[i]));
          if (!success) {
            return false;
          }
        }
      }
      // The directory is now empty so delete it
      return dir.delete();
    }
}
