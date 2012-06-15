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

import static org.mockito.Matchers.anyObject;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.spy;

import java.io.File;
import java.io.IOException;

import junit.framework.TestCase;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.FSConstants.SafeModeAction;
import org.apache.hadoop.hdfs.server.common.Storage.StorageDirectory;
import org.apache.hadoop.hdfs.server.namenode.metrics.NameNodeMetrics;
import org.mockito.Mockito;
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
public class TestSaveNamespace extends TestCase {
  private static final Log LOG = LogFactory.getLog(TestSaveNamespace.class);

  private static class FaultySaveImage implements Answer<Void> {
    int count = 0;
    boolean exceptionType = true;

    public FaultySaveImage() {
      this.exceptionType = true;
    }

    public FaultySaveImage(boolean ex) {
      this.exceptionType = ex;
    }

    public Void answer(InvocationOnMock invocation) throws Throwable {
      Object[] args = invocation.getArguments();
      File f = (File)args[0];

      if (count++ == 1) {
        if (exceptionType) {
        LOG.info("Injecting fault for file: " + f);
          throw new RuntimeException("Injected fault: saveFSImage second time");
        } else {
          throw new IOException("Injected fault: saveFSImage second time");
        }
      }
      LOG.info("Not injecting fault for file: " + f);
      return (Void) invocation.callRealMethod();
    }
  }

  private enum Fault {
    SAVE_FSIMAGE,
    MOVE_CURRENT,
    MOVE_LAST_CHECKPOINT
  };


  /**
   * Verify that a saveNamespace command brings faulty directories
   * in fs.name.dir and fs.edit.dir back online.
   */
  public void testReinsertnamedirsInSavenamespace() throws Exception {
    // create a configuration with the key to restore error
    // directories in fs.name.dir
    Configuration conf = getConf();
    conf.setBoolean("dfs.namenode.name.dir.restore", true);

    MiniDFSCluster cluster = new MiniDFSCluster(conf, 1, true, null);
    cluster.waitActive();
    FSNamesystem fsn = cluster.getNameNode().getNamesystem();

    // Replace the FSImage with a spy
    FSImage originalImage = fsn.dir.fsImage;
    FSImage spyImage = spy(originalImage);
    spyImage.setImageDigest(originalImage.getImageDigest());
    fsn.dir.fsImage = spyImage;
    spyImage.setStorageDirectories(
        FSNamesystem.getNamespaceDirs(conf),
        FSNamesystem.getNamespaceEditsDirs(conf));

    // inject fault
    // The spy throws a IOException when writing to the second directory
    doAnswer(new FaultySaveImage(false)).
      when(spyImage).saveFSImage((File)anyObject(), anyBoolean());

    try {
      doAnEdit(fsn, 1);
      fsn.setSafeMode(SafeModeAction.SAFEMODE_ENTER);

      // Save namespace - this  injects a fault and marks one
      // directory as faulty.
      LOG.info("Doing the first savenamespace.");
      fsn.saveNamespace(false, false);
      LOG.warn("First savenamespace sucessful.");
      assertTrue("Savenamespace should have marked one directory as bad." +
                 " But found " + spyImage.getRemovedStorageDirs().size() +
                 " bad directories.",
                   spyImage.getRemovedStorageDirs().size() == 1);

      // The next call to savenamespace should try inserting the
      // erroneous directory back to fs.name.dir. This command should
      // be successful.
      LOG.info("Doing the second savenamespace.");
      fsn.saveNamespace(false, false);
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
      fsn = cluster.getNameNode().getNamesystem();

      // Make sure the image loaded including our edit.
      LOG.info("Checking reloaded image.");
      checkEditExists(cluster.getNameNode().namesystem, 1);
      LOG.info("Reloaded image is good.");
    } finally {
      fsn.close();
      cluster.shutdown();
    }
  }
  private void saveNamespaceWithInjectedFault(Fault fault) throws IOException {
    Configuration conf = getConf();
    NameNode.myMetrics = new NameNodeMetrics(conf, null);
    NameNode.format(conf);
    NameNode nn = new NameNode(conf);
    FSNamesystem fsn = nn.getNamesystem();

    // Replace the FSImage with a spy
    FSImage originalImage = fsn.dir.fsImage;
    FSImage spyImage = spy(originalImage);
    spyImage.imageDigest = originalImage.imageDigest;
    fsn.dir.fsImage = spyImage;

    // inject fault
    switch(fault) {
    case SAVE_FSIMAGE:
      // The spy throws a RuntimeException when writing to the second directory
      doAnswer(new FaultySaveImage()).
        when(spyImage).saveFSImage((File)anyObject());
      break;
    case MOVE_CURRENT:
      // The spy throws a RuntimeException when calling moveCurrent()
      doThrow(new RuntimeException("Injected fault: moveCurrent")).
        when(spyImage).moveCurrent((StorageDirectory)anyObject());
      break;
    case MOVE_LAST_CHECKPOINT:
      // The spy throws a RuntimeException when calling moveLastCheckpoint()
      doThrow(new RuntimeException("Injected fault: moveLastCheckpoint")).
        when(spyImage).moveLastCheckpoint((StorageDirectory)anyObject());
      break;
    }

    try {
      doAnEdit(fsn, 1);

      // Save namespace - this will fail because we inject a fault.
      fsn.setSafeMode(SafeModeAction.SAFEMODE_ENTER);
      try {
        fsn.saveNamespace(false, false);
      } catch (Exception e) {
        LOG.info("Test caught expected exception", e);
      }

      // Now shut down and restart the namesystem
      nn.stop();
      nn = null;

      // Start a new namesystem, which should be able to recover
      // the namespace from the previous incarnation.
      nn = new NameNode(conf);
      fsn = nn.getNamesystem();

      // Make sure the image loaded including our edit.
      checkEditExists(fsn, 1);
    } finally {
      if (nn != null) {
        nn.stop();
      }
    }
  }

  // @Test
  public void testCrashWhileSavingSecondImage() throws Exception {
    saveNamespaceWithInjectedFault(Fault.SAVE_FSIMAGE);
  }

  // @Test
  public void testCrashWhileMoveCurrent() throws Exception {
    saveNamespaceWithInjectedFault(Fault.MOVE_CURRENT);
  }

  // @Test
  public void testCrashWhileMoveLastCheckpoint() throws Exception {
    saveNamespaceWithInjectedFault(Fault.MOVE_LAST_CHECKPOINT);
  }

  // @Test
  public void testSaveWhileEditsRolled() throws Exception {
    Configuration conf = getConf();
    NameNode.myMetrics = new NameNodeMetrics(conf, null);
    NameNode.format(conf);
    NameNode nn = new NameNode(conf);
    FSNamesystem fsn = nn.getNamesystem();

    // Replace the FSImage with a spy
    final FSImage originalImage = fsn.dir.fsImage;
    FSImage spyImage = spy(originalImage);
    fsn.dir.fsImage = spyImage;

    try {
      doAnEdit(fsn, 1);
      CheckpointSignature sig = fsn.rollEditLog();
      LOG.warn("Checkpoint signature: " + sig);
      // Do another edit
      doAnEdit(fsn, 2);

      // Save namespace
      fsn.setSafeMode(SafeModeAction.SAFEMODE_ENTER);
      fsn.saveNamespace(false, false);

      // Now shut down and restart the NN
      nn.stop();
      nn = null;

      // Start a new namesystem, which should be able to recover
      // the namespace from the previous incarnation.
      nn = new NameNode(conf);
      fsn = nn.getNamesystem();

      // Make sure the image loaded including our edits.
      checkEditExists(fsn, 1);
      checkEditExists(fsn, 2);
    } finally {
      if (nn != null) {
        nn.stop();
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

  private void checkEditExists(FSNamesystem fsn, int id) throws IOException {
    // Make sure the image loaded including our edit.
    assertNotNull(fsn.getFileInfo("/test" + id));
  }

  private Configuration getConf() throws IOException {
    String baseDir = System.getProperty("test.build.data", "build/test/data/dfs/");
    String nameDirs = baseDir + "name1" + "," + baseDir + "name2";
    Configuration conf = new Configuration();
    FileSystem.setDefaultUri(conf, "hdfs://localhost:0");
    conf.set("dfs.http.address", "0.0.0.0:0");
    conf.set("dfs.name.dir", nameDirs);
    conf.set("dfs.name.edits.dir", nameDirs);
    conf.set("dfs.secondary.http.address", "0.0.0.0:0");
    conf.setBoolean("dfs.permissions", false); 
    return conf;
  }
}
