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

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.spy;

import java.io.File;
import java.io.IOException;
import java.util.Collection;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.FSConstants.SafeModeAction;
import org.apache.hadoop.hdfs.server.common.Storage.StorageDirectory;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

/**
 * Test various failure scenarios during saveNamespace() operation.
 * Cases covered:
 * <ol>
 * <li>Recover from failure while saving into the second storage directory</li>
 * <li>Recover from failure while moving current into lastcheckpoint.tmp</li>
 * <li>Recover from failure while moving lastcheckpoint.tmp into
 * previous.checkpoint</li>
 * <li>Recover from failure while rolling edits file</li>
 * </ol>
 */
public class TestSaveNamespace {
  private static final Log LOG = LogFactory.getLog(TestSaveNamespace.class);

  private static class FaultySaveImage implements Answer<Void> {
    int count = 0;
    boolean exceptionType = true;

    // generate a RuntimeException
    public FaultySaveImage() {
      this.exceptionType = true;
    }

    // generate either a RuntimeException or IOException
    public FaultySaveImage(boolean etype) {
      this.exceptionType = etype;
    }

    public Void answer(InvocationOnMock invocation) throws Throwable {
      Object[] args = invocation.getArguments();
      File f = (File)args[0];

      if (count++ == 1) {
        LOG.info("Injecting fault for file: " + f);
        if (exceptionType) {
          throw new RuntimeException("Injected fault: saveFSImage second time");
        } else {
          throw new IOException("Injected fault: saveFSImage second time");
        }
      }
      LOG.info("Not injecting fault for file: " + f);
      return (Void)invocation.callRealMethod();
    }
  }

  /**
   * Verify that a saveNamespace command brings faulty directories
   * in fs.name.dir and fs.edit.dir back online.
   */
  @Test
  public void testReinsertnamedirsInSavenamespace() throws Exception {
    // create a configuration with the key to restore error
    // directories in fs.name.dir
    Configuration conf = getConf();
    conf.setBoolean("dfs.namenode.name.dir.restore", true);

    MiniDFSCluster cluster = new MiniDFSCluster(conf, 1, true, null);
    cluster.waitActive();
    FSNamesystem fsn = FSNamesystem.getFSNamesystem();
    
    // Replace the FSImage with a spy
    FSImage originalImage = fsn.dir.fsImage;
    FSImage spyImage = spy(originalImage);
    spyImage.setStorageDirectories(
        FSNamesystem.getNamespaceDirs(conf), 
        FSNamesystem.getNamespaceEditsDirs(conf));
    fsn.dir.fsImage = spyImage;

    // inject fault
    // The spy throws a IOException when writing to the second directory
    doAnswer(new FaultySaveImage(false)).
      when(spyImage).saveFSImage((File)anyObject());

    try {
      doAnEdit(fsn, 1);
      fsn.setSafeMode(SafeModeAction.SAFEMODE_ENTER);

      // Save namespace - this  injects a fault and marks one
      // directory as faulty.
      LOG.info("Doing the first savenamespace.");
      fsn.saveNamespace(false);
      LOG.warn("First savenamespace sucessful.");
      assertTrue("Savenamespace should have marked one directory as bad." +
                 " But found " + spyImage.getRemovedStorageDirs().size() +
                 " bad directories.", 
                   spyImage.getRemovedStorageDirs().size() == 1);

      // The next call to savenamespace should try inserting the
      // erroneous directory back to fs.name.dir. This command should
      // be successful.
      LOG.info("Doing the second savenamespace.");
      fsn.saveNamespace(false);
      LOG.warn("Second savenamespace sucessful.");
      assertTrue("Savenamespace should have been successful in removing " +
                 " bad directories from Image."  +
                 " But found " + originalImage.getRemovedStorageDirs().size() +
                 " bad directories.", 
                 originalImage.getRemovedStorageDirs().size() == 0);

      // Now shut down and restart the namesystem
      LOG.info("Shutting down fsimage.");
      originalImage.close();
      fsn.close();      
      fsn = null;
      cluster.shutdown();

      // Start a new namesystem, which should be able to recover
      // the namespace from the previous incarnation.
      LOG.info("Loading new FSmage from disk.");
      cluster = new MiniDFSCluster(conf, 1, false, null);
      cluster.waitActive();
      fsn = FSNamesystem.getFSNamesystem();

      // Make sure the image loaded including our edit.
      LOG.info("Checking reloaded image.");
      checkEditExists(cluster, 1);
      LOG.info("Reloaded image is good.");
    } finally {
      fsn.close();
      cluster.shutdown();
    }
  }

  /**
   * test savenamespace in the middle of a checkpoint
   */
  @Test
  public void testCheckpointWithSavenamespace() throws Exception {
    Configuration conf = getConf();
    MiniDFSCluster cluster = new MiniDFSCluster(conf, 1, true, null);
    cluster.waitActive();
    FSNamesystem fsn = FSNamesystem.getFSNamesystem();

    // Replace the FSImage with a spy
    final FSImage originalImage = fsn.dir.fsImage;

    try {
      doAnEdit(fsn, 1);
      CheckpointSignature sig = fsn.rollEditLog();
      LOG.warn("Checkpoint signature: " + sig);

      // Do another edit
      doAnEdit(fsn, 2);

      // Save namespace
      fsn.saveNamespace(true);

      // try to do a rollFSImage, this should fail because the
      // saveNamespace have already occured after the call to
      // rollFSEdit
      try {
        fsn.rollFSImage();
        assertTrue("The rollFSImage immediately folloing the saveName " +
                   " command should fail. ", false);
      } catch (IOException e) {
        LOG.info("Expected exception while invoking rollFSImage " +
                 " after a successful call to saveNamespace." + e);
      }
        
      // Now shut down and restart the NN
      originalImage.close();
      fsn.close();
      cluster.shutdown();
      fsn = null;

      // Start a new namesystem, which should be able to recover
      // the namespace from the previous incarnation.
      cluster = new MiniDFSCluster(conf, 1, false, null);
      cluster.waitActive();
      fsn = FSNamesystem.getFSNamesystem();

      // Make sure the image loaded including our edits.
      checkEditExists(cluster, 1);
      checkEditExists(cluster, 2);
    } finally {
      if (fsn != null) {
        fsn.close();
        cluster.shutdown();
      }
    }
  }

  private void testSaveWhileEditsRolled(boolean dosafemode, boolean force) throws Exception {
    Configuration conf = getConf();
    MiniDFSCluster cluster = new MiniDFSCluster(conf, 1, true, null);
    cluster.waitActive();
    FSNamesystem fsn = FSNamesystem.getFSNamesystem();

    // Replace the FSImage with a spy
    FSImage originalImage = fsn.dir.fsImage;
    FSImage spyImage = spy(originalImage);
    spyImage.setStorageDirectories(
        FSNamesystem.getNamespaceDirs(conf), 
        FSNamesystem.getNamespaceEditsDirs(conf));
    fsn.dir.fsImage = spyImage;

    try {
      doAnEdit(fsn, 1);
      CheckpointSignature sig = fsn.rollEditLog();
      LOG.warn("Checkpoint signature: " + sig);
      // Do another edit
      doAnEdit(fsn, 2);

      // Save namespace
      if (dosafemode) {
        fsn.setSafeMode(SafeModeAction.SAFEMODE_ENTER);
      }
      fsn.saveNamespace(force);

      // Now shut down and restart the NN
      originalImage.close();
      originalImage = null;
      fsn.close();
      fsn = null;

      // Start a new namesystem, which should be able to recover
      // the namespace from the previous incarnation.
      cluster = new MiniDFSCluster(conf, 1, false, null);
      cluster.waitActive();
      fsn = FSNamesystem.getFSNamesystem();

      // Make sure the image loaded including our edits.
      checkEditExists(cluster, 1);
      checkEditExists(cluster, 2);
    } finally {
      if (originalImage != null) {
        originalImage.close();
      }
      if (fsn != null) {
        fsn.close();
        cluster.shutdown();
      }
    }
  }


  private void doAnEdit(FSNamesystem fsn, int id) throws IOException {
    // Make an edit
    fsn.mkdirs(
      "/test" + id,
      new PermissionStatus("test", "Test",
          new FsPermission((short)0777)));
  }

  private void checkEditExists(MiniDFSCluster cluster, int id) throws IOException {
    // Make sure the image loaded including our edit.
    Collection<File> editsDirs = cluster.getNameEditsDirs();
    int count = 0;
    for (File ed : editsDirs) {
      count++;
      if (count == id) {
        assertTrue(new File(ed, "current/edits").exists());
      }
    }
  }

  private Configuration getConf() throws IOException {
    Configuration conf = new Configuration();
    FileSystem.setDefaultUri(conf, "hdfs://localhost:0");
    conf.set("dfs.namenode.http-address", "0.0.0.0:0");
    conf.set("dfs.namenode.secondary.http-address", "0.0.0.0:0");
    conf.setBoolean("dfs.permissions.enabled", false); 
    return conf;
  }
}
