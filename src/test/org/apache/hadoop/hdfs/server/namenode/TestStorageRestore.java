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


import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Random;
import java.util.Set;

import junit.framework.TestCase;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.server.common.Storage.StorageDirectory;
import org.apache.hadoop.hdfs.server.namenode.JournalSet.JournalAndStream;
import org.mockito.Mockito;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyLong;

import com.google.common.collect.ImmutableSet;

import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.spy;

import static org.apache.hadoop.hdfs.server.namenode.NNStorage.getInProgressEditsFileName;
import static org.apache.hadoop.hdfs.server.namenode.NNStorage.getFinalizedEditsFileName;
import static org.apache.hadoop.hdfs.server.namenode.NNStorage.getImageFileName;


/**
 * Startup and checkpoint tests
 * 
 */
public class TestStorageRestore extends TestCase {
  public static final String NAME_NODE_HOST = "localhost:";
  public static final String NAME_NODE_HTTP_HOST = "0.0.0.0:";
  private static final Log LOG =
    LogFactory.getLog(TestStorageRestore.class.getName());
  private Configuration config;
  private File hdfsDir=null;
  static final long seed = 0xAAAAEEFL;
  static final int blockSize = 4096;
  static final int fileSize = 8192;
  private File path1, path2, path3;
  private MiniDFSCluster cluster;

  private void writeFile(FileSystem fileSys, Path name, int repl)
  throws IOException {
    FSDataOutputStream stm = fileSys.create(name, true,
        fileSys.getConf().getInt("io.file.buffer.size", 4096),
        (short)repl, (long)blockSize);
    byte[] buffer = new byte[fileSize];
    Random rand = new Random(seed);
    rand.nextBytes(buffer);
    stm.write(buffer);
    stm.close();
  }
  
 
  protected void setUp() throws Exception {
    config = new Configuration();
    String baseDir = System.getProperty("test.build.data", "/tmp");
    
    hdfsDir = new File(baseDir, "dfs");
    if ( hdfsDir.exists() && !FileUtil.fullyDelete(hdfsDir) ) {
      throw new IOException("Could not delete hdfs directory '" + hdfsDir + "'");
    }
    
    hdfsDir.mkdir();
    path1 = new File(hdfsDir, "name1");
    path2 = new File(hdfsDir, "name2");
    path3 = new File(hdfsDir, "name3");
    
    path1.mkdir(); path2.mkdir(); path3.mkdir();
    if(!path2.exists() ||  !path3.exists() || !path1.exists()) {
      throw new IOException("Couldn't create dfs.name dirs");
    }
    
    String dfs_name_dir = new String(path1.getPath() + "," + path2.getPath());
    System.out.println("configuring hdfsdir is " + hdfsDir.getAbsolutePath() + 
        "; dfs_name_dir = "+ dfs_name_dir + ";dfs_name_edits_dir(only)=" + path3.getPath());
    
    config.set("dfs.name.dir", dfs_name_dir);
    config.set("dfs.name.edits.dir", dfs_name_dir + "," + path3.getPath());

    config.set("fs.checkpoint.dir",new File(hdfsDir, "secondary").getPath());
 
    FileSystem.setDefaultUri(config, "hdfs://"+NAME_NODE_HOST + "0");
    
    config.set("dfs.secondary.http.address", "0.0.0.0:0");
  }

  /**
   * clean up
   */
  public void tearDown() throws Exception {
    if (hdfsDir.exists() && !FileUtil.fullyDelete(hdfsDir) ) {
      throw new IOException("Could not delete hdfs directory in tearDown '" + hdfsDir + "'");
    }
  }
  
  /**
   * invalidate storage by removing the second and third storage directories
   */
  public void invalidateStorage(FSImage fi, Set<File> filesToInvalidate) throws IOException {
    ArrayList<StorageDirectory> al = new ArrayList<StorageDirectory>(2);
    Iterator<StorageDirectory> it = fi.storage.dirIterator();
    while(it.hasNext()) {
      StorageDirectory sd = it.next();
      if(filesToInvalidate.contains(sd.getRoot())) {
        LOG.info("causing IO error on " + sd.getRoot());
        al.add(sd);
      }
    }
    // simulate an error
    fi.storage.reportErrorsOnDirectories(al, fi);
    
    for (JournalAndStream j : fi.getEditLog().getJournals()) {
      if (j.getManager() instanceof FileJournalManager) {
        FileJournalManager fm = (FileJournalManager)j.getManager();
        if (fm.getStorageDirectory().getRoot().equals(path2)
            || fm.getStorageDirectory().getRoot().equals(path3)) {
          EditLogOutputStream mockStream = spy(j.getCurrentStream());
          j.setCurrentStreamForTests(mockStream);
          doThrow(new IOException("Injected fault: write")).
            when(mockStream).write(Mockito.<FSEditLogOp>anyObject());
          doThrow(new IOException("Injected fault: write")).
            when(mockStream).writeRaw((byte[]) any(), anyInt(), anyInt());
          doThrow(new IOException("Injected fault: write")).
            when(mockStream).writeRawOp((byte[]) any(), anyInt(), anyInt(), anyLong());
        }
      }
    }
  }

  /**
   * test
   */
  private void printStorages(FSImage image) {
    FSImageTestUtil.logStorageContents(LOG, image.storage);
  }  
  
  /**
   * test 
   * 1. create DFS cluster with 3 storage directories - 2 EDITS_IMAGE, 1 EDITS
   * 2. create a cluster and write a file
   * 3. corrupt/disable one storage (or two) by removing
   * 4. run doCheckpoint - it will fail on removed dirs (which
   * will invalidate the storages)
   * 5. write another file
   * 6. check that edits and fsimage differ 
   * 7. run doCheckpoint
   * 8. verify that all the image and edits files are the same.
   */
  public void testStorageRestore() throws Exception {
    int numDatanodes = 0;
    cluster = new MiniDFSCluster(0, config, numDatanodes, true, false, true,  null, null, null, null);
    cluster.waitActive();
    
    SecondaryNameNode secondary = new SecondaryNameNode(config);
    System.out.println("****testStorageRestore: Cluster and SNN started");
    printStorages(cluster.getNameNode().getFSImage());
    
    FileSystem fs = cluster.getFileSystem();
    Path path = new Path("/", "test");
    assertTrue(fs.mkdirs(path));
    
    System.out.println("****testStorageRestore: dir 'test' created, invalidating storage...");
  
    invalidateStorage(cluster.getNameNode().getFSImage(), ImmutableSet.of(path2, path3));
    printStorages(cluster.getNameNode().getFSImage());
    System.out.println("****testStorageRestore: storage invalidated");

    path = new Path("/", "test1");
    assertTrue(fs.mkdirs(path));

    System.out.println("****testStorageRestore: dir 'test1' created");

    // We did another edit, so the still-active directory at 'path1'
    // should now differ from the others
    FSImageTestUtil.assertFileContentsDifferent(2,
        new File(path1, "current/" + getInProgressEditsFileName(0)),
        new File(path2, "current/" + getInProgressEditsFileName(0)),
        new File(path3, "current/" + getInProgressEditsFileName(0)));
    FSImageTestUtil.assertFileContentsSame(
        new File(path2, "current/" + getInProgressEditsFileName(0)),
        new File(path3, "current/" + getInProgressEditsFileName(0)));
        
    System.out.println("****testStorageRestore: checkfiles(false) run");
    
    secondary.doCheckpoint();  ///should enable storage..
    
    // We should have a checkpoint through txid 4 in the two image dirs
    // (txid=4 for BEGIN, mkdir, mkdir, END)
    FSImageTestUtil.assertFileContentsSame(
        new File(path1, "current/" + getImageFileName(3)),
        new File(path2, "current/" + getImageFileName(3)));
    assertFalse("Should not have any image in an edits-only directory",
        new File(path3, "current/" + getImageFileName(3)).exists());

    // Should have finalized logs in the directory that didn't fail
    assertTrue("Should have finalized logs in the directory that didn't fail",
        new File(path1, "current/" + getFinalizedEditsFileName(0,3)).exists());
    // Should not have finalized logs in the failed directories
    assertFalse("Should not have finalized logs in the failed directories",
        new File(path2, "current/" + getFinalizedEditsFileName(0,3)).exists());
    assertFalse("Should not have finalized logs in the failed directories",
        new File(path3, "current/" + getFinalizedEditsFileName(0,3)).exists());
    
    // The new log segment should be in all of the directories.
    FSImageTestUtil.assertFileContentsSame(
        new File(path1, "current/" + getInProgressEditsFileName(4)),
        new File(path2, "current/" + getInProgressEditsFileName(4)),
        new File(path3, "current/" + getInProgressEditsFileName(4)));
    String md5BeforeEdit = FSImageTestUtil.getFileMD5(
        new File(path1, "current/" + getInProgressEditsFileName(4)));
    
    // The original image should still be the previously failed image
    // directory after it got restored, since it's still useful for
    // a recovery!
    FSImageTestUtil.assertFileContentsSame(
            new File(path1, "current/" + getImageFileName(-1)),
            new File(path2, "current/" + getImageFileName(-1)));
    
    // Do another edit to verify that all the logs are active.
    path = new Path("/", "test2");
    assertTrue(fs.mkdirs(path));

    // Logs should be changed by the edit.
    String md5AfterEdit =  FSImageTestUtil.getFileMD5(
        new File(path1, "current/" + getInProgressEditsFileName(4)));
    assertFalse(md5BeforeEdit.equals(md5AfterEdit));

    // And all logs should be changed.
    FSImageTestUtil.assertFileContentsSame(
        new File(path1, "current/" + getInProgressEditsFileName(4)),
        new File(path2, "current/" + getInProgressEditsFileName(4)),
        new File(path3, "current/" + getInProgressEditsFileName(4)));

    secondary.shutdown();
    cluster.shutdown();
    
    // All logs should be finalized by clean shutdown
    FSImageTestUtil.assertFileContentsSame(
        new File(path1, "current/" + getFinalizedEditsFileName(4,6)),
        new File(path2, "current/" + getFinalizedEditsFileName(4,6)),        
        new File(path3, "current/" + getFinalizedEditsFileName(4,6)));
  }
}
