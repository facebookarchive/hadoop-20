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
package org.apache.hadoop.hdfs;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import junit.framework.TestCase;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.protocol.FSConstants;
import org.apache.hadoop.hdfs.server.common.HdfsConstants.NodeType;
import org.apache.hadoop.hdfs.server.common.HdfsConstants.StartupOption;

import static org.apache.hadoop.hdfs.server.common.HdfsConstants.NodeType.NAME_NODE;
import static org.apache.hadoop.hdfs.server.common.HdfsConstants.NodeType.DATA_NODE;

import org.apache.hadoop.hdfs.server.common.HdfsConstants;
import org.apache.hadoop.hdfs.server.common.InconsistentFSStateException;
import org.apache.hadoop.hdfs.server.common.Storage;
import org.apache.hadoop.hdfs.server.common.StorageInfo;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.datanode.NameSpaceSliceStorage;
import org.apache.hadoop.hdfs.server.namenode.FSImageTestUtil;
import org.apache.hadoop.fs.FileUtil;

/**
* This test ensures the appropriate response (successful or failure) from
* the system when the system is upgraded under various storage state and
* version conditions.
*/
public class TestDFSUpgrade extends TestCase {
 
  private static final Log LOG = LogFactory.getLog(
                                                   "org.apache.hadoop.hdfs.TestDFSUpgrade");
  private Configuration conf;
  private int testCounter = 0;
  private MiniDFSCluster cluster = null;
    
  /**
   * Writes an INFO log message containing the parameters.
   */
  void log(String label, int numDirs) {
    LOG.info("============================================================");
    LOG.info("***TEST " + (testCounter++) + "*** " 
             + label + ":"
             + " numDirs="+numDirs);
  }

  void checkResult(NodeType nodeType, String[] baseDirs) throws IOException {
    checkResult(nodeType, baseDirs, 0, false);
  }
  
  void checkResult(NodeType nodeType, String[] baseDirs,
      boolean simulatedPrevious) throws IOException {
    checkResult(nodeType, baseDirs, 0, simulatedPrevious);
  }

  void checkResult(NodeType nodeType, String[] baseDirs, int nnIndex)
      throws IOException {
    checkResult(nodeType, baseDirs, nnIndex, false);
  }

  /**
   * Verify that the current and previous directories exist.  Verify that 
   * previous hasn't been modified by comparing the checksum of all it's
   * containing files with their original checksum.  It is assumed that
   * the server has recovered and upgraded.
   * nsLevelUpgrade specify if the upgrade is at top level or ns level
   * nsLevelUpgrade=true, we search basedir/current/NS-id/previous
   *               =false, we search basedir/previous
   */
  void checkResult(NodeType nodeType, String[] baseDirs, int nnIndex
,
      boolean simulatedPrevious) throws IOException {
    switch (nodeType) {
    case NAME_NODE:
      for (int i = 0; i < baseDirs.length; i++) {
        FSImageTestUtil.assertReasonableNameCurrentDir(new File(baseDirs[i], "current"));
      }
      break;
    case DATA_NODE:
      for (int i = 0; i < baseDirs.length; i++) {
        assertEquals(
                     UpgradeUtilities.checksumContents(
                                                       nodeType, new File(baseDirs[i],"current")),
                     UpgradeUtilities.checksumMasterContents(nodeType));
        File nsBaseDir= NameSpaceSliceStorage.getNsRoot(
            cluster.getNameNode(nnIndex).getNamespaceID(),
            new File(baseDirs[i], "current"));
        assertEquals(
                     UpgradeUtilities.checksumContents(nodeType, new File(nsBaseDir,
                         MiniDFSCluster.FINALIZED_DIR_NAME)), 
                     UpgradeUtilities.checksumDatanodeNSStorageContents(nnIndex));
      }
      break;
    }
    for (int i = 0; i < baseDirs.length; i++) {
      switch (nodeType) {
      case NAME_NODE:
        assertTrue(new File(baseDirs[i],"previous").isDirectory());
        assertEquals(
                     UpgradeUtilities.checksumContents(
                                                       nodeType, new File(baseDirs[i],"previous")),
                     UpgradeUtilities.checksumMasterContents(nodeType));
        break;
      case DATA_NODE:
        File nsBaseDir = null;
        nsBaseDir = NameSpaceSliceStorage.getNsRoot(
            cluster.getNameNode(nnIndex).getNamespaceID(),
            new File(baseDirs[i], "current"));
        // Top level upgrade should not exist.
        assertFalse(new File(baseDirs[i], "previous").isDirectory() && !simulatedPrevious);
        assertTrue(new File(nsBaseDir, "previous").isDirectory());
        assertEquals(UpgradeUtilities.checksumContents(nodeType, new File(
            nsBaseDir, "previous/finalized")),
            UpgradeUtilities.checksumDatanodeNSStorageContents(nnIndex));
      }
    }
  }
 
  /**
   * Attempts to start a NameNode with the given operation.  Starting
   * the NameNode should throw an exception.
   */
  void startNameNodeShouldFail(StartupOption operation) {
    try {
      cluster = new MiniDFSCluster(conf, 0, operation); // should fail
      throw new AssertionError("NameNode should have failed to start");
    } catch (Exception expected) {
      // expected
    }
  }
  
  /**
   * Attempts to start a DataNode with the given operation.  Starting
   * the DataNode should throw an exception.
   */
  void startDataNodeShouldFail(StartupOption operation) {
    try {
      cluster.startDataNodes(conf, 1, false, operation, null); // should fail
      throw new AssertionError("DataNode should have failed to start");
    } catch (Exception expected) {
      // expected
      assertFalse(cluster.isDataNodeUp());
    }
  }

  /*
   * This test attempts to upgrade the datanode from federation 
   * version -35 to upper version 
   * This test is for non-federation cluster with single namenode
   */
  public void testNonFederationClusterUpgradeAfterFederationVersion()
      throws Exception {
    File[] baseDirs;
    UpgradeUtilities.initialize();
    for (int numDirs = 1; numDirs <= 2; numDirs++) {
      conf = new Configuration();
      conf.setInt("dfs.datanode.scan.period.hours", -1);      
      conf = UpgradeUtilities.initializeStorageStateConf(numDirs, conf);
      String[] nameNodeDirs = conf.getStrings("dfs.name.dir");
      String[] dataNodeDirs = conf.getStrings("dfs.data.dir");
      log("DataNode upgrade with federation layout version in current", numDirs);
      UpgradeUtilities.createStorageDirs(NAME_NODE, nameNodeDirs, "current");
      try {
        cluster = new MiniDFSCluster(conf, 0, StartupOption.UPGRADE);
        baseDirs = UpgradeUtilities.createStorageDirs(DATA_NODE, dataNodeDirs, "current");
        UpgradeUtilities.createVersionFile(DATA_NODE, baseDirs,
            new StorageInfo(FSConstants.FEDERATION_VERSION,
                            UpgradeUtilities.getCurrentNamespaceID(cluster),
                            UpgradeUtilities.getCurrentFsscTime(cluster)), 
            cluster.getNameNode().getNamespaceID());
        cluster.startDataNodes(conf, 1, false, StartupOption.REGULAR, null);
        checkResult(DATA_NODE, dataNodeDirs, 0, false);
      } finally {
        if (cluster != null) cluster.shutdown();
        UpgradeUtilities.createEmptyDirs(nameNodeDirs);
        UpgradeUtilities.createEmptyDirs(dataNodeDirs);
      }
    }
  }  
  
  /*
   * This test attempts to upgrade the datanode from federation 
   * version -35 to upper version
   * This test is for federation cluster with 2 namenodes
   */
  public void testFederationClusterUpgradeAfterFederationVersion()
      throws Exception {
    File[] baseDirs;
    Configuration baseConf = new Configuration();
    UpgradeUtilities.initialize(2, baseConf, true);
    for (int numDirs = 1; numDirs <= 2; numDirs++) {
      conf = new Configuration();
      conf.setInt("dfs.datanode.scan.period.hours", -1);
      conf = UpgradeUtilities.initializeStorageStateConf(numDirs, conf);
       String[] nameNodeDirs = conf.getStrings("dfs.name.dir");
      String[] dataNodeDirs = conf.getStrings("dfs.data.dir");
      log("DataNode upgrade with federation layout version in current", numDirs);
      UpgradeUtilities.createFederatedNameNodeStorageDirs(nameNodeDirs);
      conf.set(FSConstants.DFS_FEDERATION_NAMESERVICES, 
          baseConf.get(FSConstants.DFS_FEDERATION_NAMESERVICES));
      try {
        cluster = new MiniDFSCluster(conf, 0, StartupOption.UPGRADE, false, 2);
        baseDirs = UpgradeUtilities.createStorageDirs(DATA_NODE, dataNodeDirs, "current");
        for (int i = 0; i < 2; i++) {
          UpgradeUtilities.createVersionFile(DATA_NODE, baseDirs,
            new StorageInfo(FSConstants.FEDERATION_VERSION,
                            cluster.getNameNode(i).getNamespaceID(),
                            cluster.getNameNode(i).versionRequest().getCTime()),
            cluster.getNameNode(i).getNamespaceID());
        }
        cluster.startDataNodes(conf, 1, false, StartupOption.REGULAR, null);
        for (int i = 0 ;i < 2; i++) {
          checkResult(DATA_NODE, dataNodeDirs, i, false);
        }
      } finally {
        if (cluster != null) cluster.shutdown();
        UpgradeUtilities.createEmptyDirs(nameNodeDirs);
        UpgradeUtilities.createEmptyDirs(dataNodeDirs);
      }
    }
  }

  /*
   * This test attempts to upgrade the datanode from federation version -35 to
   * upper version This test is for federation cluster with 2 namenodes. It
   * changes the layout version and ctime.
   */
  public void testFederationClusterUpgradeAfterFederationVersionWithCTimeChange()
      throws Exception {
    File[] baseDirs;
    Configuration baseConf = new Configuration();
    UpgradeUtilities.initialize(2, baseConf, true);
    for (int numDirs = 1; numDirs <= 2; numDirs++) {
      conf = new Configuration();
      conf.setInt("dfs.datanode.scan.period.hours", -1);
      conf = UpgradeUtilities.initializeStorageStateConf(numDirs, conf);
      String[] nameNodeDirs = conf.getStrings("dfs.name.dir");
      String[] dataNodeDirs = conf.getStrings("dfs.data.dir");
      log("DataNode upgrade with federation layout version in current and ctime change",
          numDirs);
      UpgradeUtilities.createFederatedNameNodeStorageDirs(nameNodeDirs);
      conf.set(FSConstants.DFS_FEDERATION_NAMESERVICES,
          baseConf.get(FSConstants.DFS_FEDERATION_NAMESERVICES));
      try {
        cluster = new MiniDFSCluster(conf, 0, StartupOption.UPGRADE, false, 2);
        baseDirs = UpgradeUtilities.createStorageDirs(DATA_NODE, dataNodeDirs,
            "current");
        for (int i = 0; i < 2; i++) {
          UpgradeUtilities.createVersionFile(DATA_NODE, baseDirs,
              new StorageInfo(FSConstants.FEDERATION_VERSION, cluster
                  .getNameNode(i).getNamespaceID(), cluster.getNameNode(i)
                  .versionRequest().getCTime() - 1), cluster.getNameNode(i)
                  .getNamespaceID());
        }
        cluster.startDataNodes(conf, 1, false, StartupOption.REGULAR, null);

        for (int i = 0; i < 2; i++) {
          checkResult(DATA_NODE, dataNodeDirs, i, false);
        }
      } finally {
        if (cluster != null)
          cluster.shutdown();
        UpgradeUtilities.createEmptyDirs(nameNodeDirs);
        UpgradeUtilities.createEmptyDirs(dataNodeDirs);
      }
    }
  }

  public void testFederationClusterUpgradeAfterFederationVersionWithTopLevelLayout()
      throws Exception {
    File[] baseDirs;
    Configuration baseConf = new Configuration();
    UpgradeUtilities.initialize(2, baseConf, true);
    for (int numDirs = 1; numDirs <= 2; numDirs++) {
      conf = new Configuration();
      conf.setInt("dfs.datanode.scan.period.hours", -1);
      conf = UpgradeUtilities.initializeStorageStateConf(numDirs, conf);
      String[] nameNodeDirs = conf.getStrings("dfs.name.dir");
      String[] dataNodeDirs = conf.getStrings("dfs.data.dir");
      log("DataNode upgrade with federation layout version in current and no ns level layout version",
          numDirs);
      UpgradeUtilities.createFederatedNameNodeStorageDirs(nameNodeDirs);
      conf.set(FSConstants.DFS_FEDERATION_NAMESERVICES,
          baseConf.get(FSConstants.DFS_FEDERATION_NAMESERVICES));
      try {
        cluster = new MiniDFSCluster(conf, 0, StartupOption.UPGRADE, false, 2);
        baseDirs = UpgradeUtilities.createStorageDirs(DATA_NODE, dataNodeDirs,
            "current");
        for (int i = 0; i < 2; i++) {
          UpgradeUtilities.createVersionFile(DATA_NODE, baseDirs,
              new StorageInfo(FSConstants.FEDERATION_VERSION, cluster
                  .getNameNode(i).getNamespaceID(), cluster.getNameNode(i)
                  .versionRequest().getCTime()), cluster.getNameNode(i)
                  .getNamespaceID(), false);
        }
        cluster.startDataNodes(conf, 1, false, StartupOption.REGULAR, null);

        for (int i = 0; i < 2; i++) {
          checkResult(DATA_NODE, dataNodeDirs, i, false);
        }

        // Finalize upgrade.
        for (int i = 0; i < 2; i++) {
          cluster.getNameNode(i).finalizeUpgrade();
        }
        cluster.restartDataNodes();

        // Wait for datanodes to finalize.
        Thread.sleep(10000);

        for (int nnIndex = 0; nnIndex < 2; nnIndex++) {
          for (int i = 0; i < dataNodeDirs.length; i++) {
            File nsBaseDir = NameSpaceSliceStorage.getNsRoot(cluster
                .getNameNode(nnIndex).getNamespaceID(), new File(
                dataNodeDirs[i], "current"));
            assertFalse(new File(nsBaseDir, "previous").exists());
          }
        }
      } finally {
        if (cluster != null)
          cluster.shutdown();
        UpgradeUtilities.createEmptyDirs(nameNodeDirs);
        UpgradeUtilities.createEmptyDirs(dataNodeDirs);
      }
    }
  }

  /**
   * This test attempts to upgrade the NameNode and DataNode under
   * a number of valid and invalid conditions.
   */
  public void testUpgrade() throws Exception {
    File[] baseDirs;
    UpgradeUtilities.initialize();
    for (int numDirs = 1; numDirs <= 2; numDirs++) {
      conf = new Configuration();
      conf.setInt("dfs.datanode.scan.period.hours", -1);      
      conf = UpgradeUtilities.initializeStorageStateConf(numDirs, conf);
      String[] nameNodeDirs = conf.getStrings("dfs.name.dir");
      String[] dataNodeDirs = conf.getStrings("dfs.data.dir");
      
      log("Normal NameNode upgrade", numDirs);
      UpgradeUtilities.createStorageDirs(NAME_NODE, nameNodeDirs, "current");
      cluster = new MiniDFSCluster(conf, 0, StartupOption.UPGRADE);
      checkResult(NAME_NODE, nameNodeDirs);
      cluster.shutdown();
      UpgradeUtilities.createEmptyDirs(nameNodeDirs);
      
      log("Normal DataNode upgrade", numDirs);
      UpgradeUtilities.createStorageDirs(NAME_NODE, nameNodeDirs, "current");
      cluster = new MiniDFSCluster(conf, 0, StartupOption.UPGRADE);
      UpgradeUtilities.createStorageDirs(DATA_NODE, dataNodeDirs, "current");
      cluster.startDataNodes(conf, 1, false, StartupOption.REGULAR, null);
      checkResult(DATA_NODE, dataNodeDirs);
      cluster.shutdown();
      UpgradeUtilities.createEmptyDirs(nameNodeDirs);
      UpgradeUtilities.createEmptyDirs(dataNodeDirs);
      
      log("NameNode upgrade with existing previous dir", numDirs);
      UpgradeUtilities.createStorageDirs(NAME_NODE, nameNodeDirs, "current");
      UpgradeUtilities.createStorageDirs(NAME_NODE, nameNodeDirs, "previous");
      startNameNodeShouldFail(StartupOption.UPGRADE);
      UpgradeUtilities.createEmptyDirs(nameNodeDirs);
      
      log("DataNode upgrade with existing previous dir", numDirs);
      UpgradeUtilities.createStorageDirs(NAME_NODE, nameNodeDirs, "current");
      cluster = new MiniDFSCluster(conf, 0, StartupOption.UPGRADE);
      UpgradeUtilities.createStorageDirs(DATA_NODE, dataNodeDirs, "current");
      UpgradeUtilities.createStorageDirs(DATA_NODE, dataNodeDirs, "previous");
      startDataNodeShouldFail(StartupOption.REGULAR);
      cluster.shutdown();
      UpgradeUtilities.createEmptyDirs(nameNodeDirs);
      UpgradeUtilities.createEmptyDirs(dataNodeDirs);

      log("DataNode upgrade with future stored layout version in current", numDirs);
      UpgradeUtilities.createStorageDirs(NAME_NODE, nameNodeDirs, "current");
      cluster = new MiniDFSCluster(conf, 0, StartupOption.UPGRADE);
      baseDirs = UpgradeUtilities.createStorageDirs(DATA_NODE, dataNodeDirs, "current");
      UpgradeUtilities.createVersionFile(DATA_NODE, baseDirs,
                                         new StorageInfo(Integer.MIN_VALUE,
                                                         UpgradeUtilities.getCurrentNamespaceID(cluster),
                                                         UpgradeUtilities.getCurrentFsscTime(cluster)), cluster.getNameNode().getNamespaceID());
      startDataNodeShouldFail(StartupOption.REGULAR);
      cluster.shutdown();
      UpgradeUtilities.createEmptyDirs(nameNodeDirs);
      UpgradeUtilities.createEmptyDirs(dataNodeDirs);
      
      log("DataNode upgrade with newer fsscTime in current", numDirs);
      UpgradeUtilities.createStorageDirs(NAME_NODE, nameNodeDirs, "current");
      cluster = new MiniDFSCluster(conf, 0, StartupOption.UPGRADE);
      baseDirs = UpgradeUtilities.createStorageDirs(DATA_NODE, dataNodeDirs, "current");
      UpgradeUtilities.createVersionFile(DATA_NODE, baseDirs,
                                         new StorageInfo(UpgradeUtilities.getCurrentLayoutVersion(),
                                                         UpgradeUtilities.getCurrentNamespaceID(cluster),
                                                         Long.MAX_VALUE), cluster.getNameNode().getNamespaceID());
      startDataNodeShouldFail(StartupOption.REGULAR);
      cluster.shutdown();
      UpgradeUtilities.createEmptyDirs(nameNodeDirs);
      UpgradeUtilities.createEmptyDirs(dataNodeDirs);

      log("NameNode upgrade with no edits file", numDirs);
      baseDirs = UpgradeUtilities.createStorageDirs(NAME_NODE, nameNodeDirs, "current");
      deleteStorageFilesWithPrefix(nameNodeDirs, "edits_");
      startNameNodeShouldFail(StartupOption.UPGRADE);
      UpgradeUtilities.createEmptyDirs(nameNodeDirs);
      
      log("NameNode upgrade with no image file", numDirs);
      baseDirs = UpgradeUtilities.createStorageDirs(NAME_NODE, nameNodeDirs, "current");
      deleteStorageFilesWithPrefix(nameNodeDirs, "fsimage_");
      startNameNodeShouldFail(StartupOption.UPGRADE);
      UpgradeUtilities.createEmptyDirs(nameNodeDirs);
      
      log("NameNode upgrade with corrupt version file", numDirs);
      baseDirs = UpgradeUtilities.createStorageDirs(NAME_NODE, nameNodeDirs, "current");
      for (File f : baseDirs) { 
        UpgradeUtilities.corruptFile(new File(f,"VERSION")); 
      }
      startNameNodeShouldFail(StartupOption.UPGRADE);
      UpgradeUtilities.createEmptyDirs(nameNodeDirs);
      
      log("NameNode upgrade with old layout version in current", numDirs);
      baseDirs = UpgradeUtilities.createStorageDirs(NAME_NODE, nameNodeDirs, "current");
      UpgradeUtilities.createVersionFile(NAME_NODE, baseDirs,
                                         new StorageInfo(Storage.LAST_UPGRADABLE_LAYOUT_VERSION + 1,
                                                         UpgradeUtilities.getCurrentNamespaceID(null),
                                                         UpgradeUtilities.getCurrentFsscTime(null)), 0);
      startNameNodeShouldFail(StartupOption.UPGRADE);
      UpgradeUtilities.createEmptyDirs(nameNodeDirs);
      
      log("NameNode upgrade with future layout version in current", numDirs);
      baseDirs = UpgradeUtilities.createStorageDirs(NAME_NODE, nameNodeDirs, "current");
      UpgradeUtilities.createVersionFile(NAME_NODE, baseDirs,
                                         new StorageInfo(Integer.MIN_VALUE,
                                                         UpgradeUtilities.getCurrentNamespaceID(null),
                                                         UpgradeUtilities.getCurrentFsscTime(null)), 0);
      startNameNodeShouldFail(StartupOption.UPGRADE);
      UpgradeUtilities.createEmptyDirs(nameNodeDirs);

      log("Normal Datanode upgrade after datanode format", numDirs);
      UpgradeUtilities.createEmptyDirs(dataNodeDirs);
      baseDirs = UpgradeUtilities.createStorageDirs(NAME_NODE, nameNodeDirs,
          "current");
      UpgradeUtilities.createVersionFile(NAME_NODE, baseDirs,
          new StorageInfo(FSConstants.LAYOUT_VERSION,
            UpgradeUtilities.getCurrentNamespaceID(null),
            0), 0);
      cluster = new MiniDFSCluster(0, conf, 1, false, false, false,
          StartupOption.UPGRADE, null, null, null, false, false, 1, false);
      cluster.shutdown();
      UpgradeUtilities.createEmptyDirs(nameNodeDirs);
    } // end numDir loop
  }
 
  protected void tearDown() throws Exception {
    LOG.info("Shutting down MiniDFSCluster");
    if (cluster != null) cluster.shutdown();
  }
    
  public static void main(String[] args) throws Exception {
    new TestDFSUpgrade().testUpgrade();
  }
  
  private void deleteStorageFilesWithPrefix(String[] nameNodeDirs, String prefix)
  throws Exception {
    for (String baseDirStr : nameNodeDirs) {
      File baseDir = new File(baseDirStr);
      File currentDir = new File(baseDir, "current");
      for (File f : currentDir.listFiles()) {
        if (f.getName().startsWith(prefix)) {
          assertTrue("Deleting " + f, f.delete());
        }
      }
    }
  }
  
}


