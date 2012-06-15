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
import java.util.Iterator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.FSConstants;
import org.apache.hadoop.hdfs.server.common.Storage.StorageDirectory;
import org.apache.hadoop.hdfs.server.namenode.FSImage.NameNodeDirType;
import org.apache.hadoop.hdfs.server.namenode.FSImage.NameNodeFile;

/**
 * OfflineEditsViewerHelper is a helper class for TestOfflineEditsViewer,
 * it performs NN operations that generate all op codes
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class OfflineEditsViewerHelper {

  private static final Log LOG = 
    LogFactory.getLog(OfflineEditsViewerHelper.class);

    long           blockSize = 512;
    MiniDFSCluster cluster   = null;
    Configuration  conf    = new Configuration();

  /**
   * Generates edits with all op codes and returns the edits filename
   *
   * @param dfsDir DFS directory (where to setup MiniDFS cluster)
   * @param editsFilename where to copy the edits
   */
  public String generateEdits() throws IOException {
    runOperations();
    return getEditsFilename();
  }

  /**
   * Get edits filename
   *
   * @return edits file name for cluster
   */
  private String getEditsFilename() throws IOException {
    FSImage image = cluster.getNameNode().getFSImage();
    // it was set up to only have ONE StorageDirectory
    Iterator<StorageDirectory> it
      = image.dirIterator(NameNodeDirType.EDITS);
    StorageDirectory sd = it.next();
    File ret = FSImage.getImageFile(sd, NameNodeFile.EDITS);
    assert ret.exists() : "expected " + ret + " exists";
    return ret.getAbsolutePath();
  }

  /**
   * Sets up a MiniDFSCluster, configures it to create one edits file,
   * starts DelegationTokenSecretManager (to get security op codes)
   *
   * @param dfsDir DFS directory (where to setup MiniDFS cluster)
   */
  public void startCluster(String dfsDir, boolean simulateEditLogCrash) 
      throws IOException {

    conf = new Configuration();
    conf.set("dfs.secondary.http.address", "0.0.0.0:0");
    conf.setBoolean("dfs.simulate.editlog.crash", simulateEditLogCrash);
    cluster = new MiniDFSCluster(conf, 3, true, null);
    cluster.waitClusterUp();
  }

  /**
   * Shutdown the cluster
   */
  public void shutdownCluster() throws IOException {
    if (cluster != null) {
      cluster.shutdown();
    }
  }
  
  /**
   * Save namespace
   */
  public void saveNS() throws IOException {
    if (cluster != null) {
      cluster.getNameNode().saveNamespace(true, false);
    }
  }
  
  /**
   * Save namespace
   */
  public void closeNS() throws IOException {
    if (cluster != null) {
      cluster.getNameNode().getNamesystem().close();
    }
  }

  /**
   * Run file operations to create edits for all op codes
   * to be tested.
   */
  private void runOperations() throws IOException {

    LOG.info("Creating edits by performing fs operations");
    // no check, if it's not it throws an exception which is what we want
    DistributedFileSystem dfs =
      (DistributedFileSystem)cluster.getFileSystem();
    // OP_ADD 0, OP_SET_GENSTAMP 10
    Path pathFileCreate = new Path("/file_create");
    FSDataOutputStream s = dfs.create(pathFileCreate);
    // OP_CLOSE 9
    s.close();
    // OP_RENAME 1
    Path pathFileMoved = new Path("/file_moved");
    dfs.rename(pathFileCreate, pathFileMoved);
    // OP_DELETE 2
    dfs.delete(pathFileMoved, false);
    // OP_MKDIR 3
    Path pathDirectoryMkdir = new Path("/directory_mkdir");
    dfs.mkdirs(pathDirectoryMkdir);
    // OP_SET_REPLICATION 4
    s = dfs.create(pathFileCreate);
    s.close();
    dfs.setReplication(pathFileCreate, (short)1);
    // OP_SET_PERMISSIONS 7
    Short permission = 0777;
    dfs.setPermission(pathFileCreate, new FsPermission(permission));
    // OP_SET_OWNER 8
    dfs.setOwner(pathFileCreate, new String("newOwner"), null);
    // OP_CLOSE 9 see above
    // OP_SET_GENSTAMP 10 see above
    // OP_SET_NS_QUOTA 11 obsolete
    // OP_CLEAR_NS_QUOTA 12 obsolete
    // OP_TIMES 13
    long mtime = 1285195527000L; // Wed, 22 Sep 2010 22:45:27 GMT
    long atime = mtime;
    dfs.setTimes(pathFileCreate, mtime, atime);
    // OP_SET_QUOTA 14
    dfs.setQuota(pathDirectoryMkdir, 1000L, FSConstants.QUOTA_DONT_SET);
    // OP_CONCAT_DELETE 16
    Path   pathConcatTarget = new Path("/file_concat_target");
    Path[] pathConcatFiles  = new Path[2];
    pathConcatFiles[0]      = new Path("/file_concat_0");
    pathConcatFiles[1]      = new Path("/file_concat_1");

    long  length      = blockSize * 3; // multiple of blocksize for concat
    short replication = 1;
    long  seed        = 1;

    DFSTestUtil.createFile(dfs, pathConcatTarget, length, replication, seed);
    DFSTestUtil.createFile(dfs, pathConcatFiles[0], length, replication, seed);
    DFSTestUtil.createFile(dfs, pathConcatFiles[1], length, replication, seed);
    dfs.concat(pathConcatTarget, pathConcatFiles, false);

    // sync to disk, otherwise we parse partial edits
    cluster.getNameNode().getFSImage().getEditLog().logSync();
    dfs.close();
  }
}
