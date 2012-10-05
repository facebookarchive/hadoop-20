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

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.util.Collection;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.util.InjectionEvent;
import org.apache.hadoop.hdfs.util.InjectionHandler;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestNNStorageFailures {

  public static final Log LOG = LogFactory.getLog(TestNNStorageFailures.class);

  private int editsPerformed = 0;
  private MiniDFSCluster cluster;
  private FileSystem fs;
  private Configuration conf;

  @Before
  public void setUpMiniCluster() throws IOException {
    conf = new Configuration();
    File baseDir = MiniDFSCluster.getBaseDirectory(conf);
    File editDir1 = new File(baseDir, "edit1");
    File editDir2 = new File(baseDir, "edit2");
    conf.set("dfs.name.edits.dir",
        editDir1.getPath() + "," + editDir2.getPath());
    conf.set("dfs.secondary.http.address", "0.0.0.0:0");

    cluster = new MiniDFSCluster(conf, 1, true, null);
    cluster.waitActive();
    fs = cluster.getFileSystem();
  }

  @After
  public void shutDownMiniCluster() throws IOException {
    if (fs != null)
      fs.close();
    if (cluster != null)
      cluster.shutdown();
    InjectionHandler.clear();
  }

  /**
   * Do a mutative metadata operation on the file system.
   * 
   * @return true if the operation was successful, false otherwise.
   */
  private boolean doAnEdit() throws IOException {
    return fs.mkdirs(new Path("/tmp", Integer.toString(editsPerformed++)));
  }

  // check if exception is thrown when all image dirs fail
  @Test
  public void testAllImageDirsFailOnRoll() throws IOException {    
    assertTrue(doAnEdit());
    Collection<File> namedirs = cluster.getNameDirs();
    TestNNStorageFailuresInjectionHandler h = new TestNNStorageFailuresInjectionHandler(namedirs);
    InjectionHandler.set(h);
    SecondaryNameNode sn = new SecondaryNameNode(conf);

    try {
      sn.doCheckpoint();
      fail("Should get an exception here");
    } catch (IOException e) {
      LOG.info(e);
      assertTrue(e.toString()
          .contains("No more image storage directories left"));
    } finally {
      for (File f : namedirs) {
        LOG.info("Changing permissions for directory " + f);
        f.setExecutable(true);
      }
      if(sn != null)
        sn.shutdown();
    }
  }
  
  class TestNNStorageFailuresInjectionHandler extends InjectionHandler {
    
    Collection<File> nameDirs;   
    TestNNStorageFailuresInjectionHandler(Collection<File> dirs) {
      nameDirs = dirs;
    }
     
    public void _processEvent(InjectionEvent event, Object... args) {
      if(event == InjectionEvent.FSIMAGE_RENAME) {
        for (File f : nameDirs) {
          LOG.info("Changing permissions for directory " + f);
          f.setExecutable(false);
        }
      }
    }
  }
}
