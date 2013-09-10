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
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import junit.framework.TestCase;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.server.common.HdfsConstants.NodeType;
import org.apache.hadoop.hdfs.server.common.HdfsConstants.StartupOption;

import static org.apache.hadoop.hdfs.server.common.HdfsConstants.NodeType.NAME_NODE;
import static org.apache.hadoop.hdfs.server.common.HdfsConstants.NodeType.DATA_NODE;

import org.apache.hadoop.hdfs.server.common.StorageInfo;
import org.apache.hadoop.hdfs.server.datanode.NameSpaceSliceStorage;
import org.apache.hadoop.hdfs.server.namenode.FSImageTestUtil;

import com.google.common.collect.Lists;

/**
* This test ensures the appropriate response (successful or failure) from
* the system when the system is rolled back under various storage state and
* version conditions.
*/
public class TestDFSRollback extends TestCase {
 
  private static final Log LOG = LogFactory.getLog(
                                                   "org.apache.hadoop.hdfs.TestDFSRollback");
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
  
  /**
   * Verify that the new current directory is the old previous.  
   * It is assumed that the server has recovered and rolled back.
   */
  void checkResult(NodeType nodeType, String[] baseDirs) throws Exception {
    List<File> curDirs = Lists.newArrayList();
    for (String baseDir : baseDirs) {
      File curDir = new File(baseDir, "current");
      curDirs.add(curDir);
      switch (nodeType) {
      case NAME_NODE:
        FSImageTestUtil.assertReasonableNameCurrentDir(curDir);
        break;
      case DATA_NODE:
        for (int i = 0; i < baseDirs.length; i++) {
          assertEquals(
                       UpgradeUtilities.checksumContents(
                                                         nodeType, new File(baseDirs[i],"current")),
                       UpgradeUtilities.checksumMasterContents(nodeType));
          File nsBaseDir= NameSpaceSliceStorage.getNsRoot(UpgradeUtilities.getCurrentNamespaceID(cluster), new File(baseDirs[i], "current"));
          assertEquals(
                       UpgradeUtilities.checksumContents(nodeType, new File(nsBaseDir,
                           MiniDFSCluster.FINALIZED_DIR_NAME)), 
                       UpgradeUtilities.checksumDatanodeNSStorageContents());
        }
        break;
      }
    }
    
    FSImageTestUtil.assertParallelFilesAreIdentical(
        curDirs, Collections.<String>emptySet());

    for (int i = 0; i < baseDirs.length; i++) {
      assertFalse(new File(baseDirs[i],"previous").isDirectory());
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
 
  /**
   * This test attempts to rollback the NameNode and DataNode under
   * a number of valid and invalid conditions.
   */
  public void testRollback() throws Exception {
    File[] baseDirs;
    UpgradeUtilities.initialize();
    
    for (int numDirs = 1; numDirs <= 2; numDirs++) {
      conf = new Configuration();
      conf.setInt("dfs.datanode.scan.period.hours", -1);      
      conf = UpgradeUtilities.initializeStorageStateConf(numDirs, conf);
      String[] nameNodeDirs = conf.getStrings("dfs.name.dir");
      String[] dataNodeDirs = conf.getStrings("dfs.data.dir");
      
      log("Normal NameNode rollback", numDirs);
      UpgradeUtilities.createStorageDirs(NAME_NODE, nameNodeDirs, "current");
      UpgradeUtilities.createStorageDirs(NAME_NODE, nameNodeDirs, "previous");
      cluster = new MiniDFSCluster(conf, 0, StartupOption.ROLLBACK);
      checkResult(NAME_NODE, nameNodeDirs);
      cluster.shutdown();
      UpgradeUtilities.createEmptyDirs(nameNodeDirs);
      
      log("Normal DataNode rollback", numDirs);
      UpgradeUtilities.createStorageDirs(NAME_NODE, nameNodeDirs, "current");
      UpgradeUtilities.createStorageDirs(NAME_NODE, nameNodeDirs, "previous");
      cluster = new MiniDFSCluster(conf, 0, StartupOption.ROLLBACK);
      UpgradeUtilities.createStorageDirs(DATA_NODE, dataNodeDirs, "current");
      UpgradeUtilities.createStorageDirs(DATA_NODE, dataNodeDirs, "previous");
      cluster.startDataNodes(conf, 1, false, StartupOption.ROLLBACK, null);
      checkResult(DATA_NODE, dataNodeDirs);
      cluster.shutdown();
      UpgradeUtilities.createEmptyDirs(nameNodeDirs);
      UpgradeUtilities.createEmptyDirs(dataNodeDirs);

      log("NameNode rollback without existing previous dir", numDirs);
      UpgradeUtilities.createStorageDirs(NAME_NODE, nameNodeDirs, "current");
      startNameNodeShouldFail(StartupOption.ROLLBACK);
      UpgradeUtilities.createEmptyDirs(nameNodeDirs);
      
      log("DataNode rollback without existing previous dir", numDirs);
      UpgradeUtilities.createStorageDirs(NAME_NODE, nameNodeDirs, "current");
      cluster = new MiniDFSCluster(conf, 0, StartupOption.UPGRADE);
      UpgradeUtilities.createStorageDirs(DATA_NODE, dataNodeDirs, "current");
      cluster.startDataNodes(conf, 1, false, StartupOption.ROLLBACK, null);
      cluster.shutdown();
      UpgradeUtilities.createEmptyDirs(nameNodeDirs);
      UpgradeUtilities.createEmptyDirs(dataNodeDirs);

      log("DataNode rollback with future stored layout version in previous", numDirs);
      UpgradeUtilities.createStorageDirs(NAME_NODE, nameNodeDirs, "current");
      UpgradeUtilities.createStorageDirs(NAME_NODE, nameNodeDirs, "previous");
      cluster = new MiniDFSCluster(conf, 0, StartupOption.ROLLBACK);
      UpgradeUtilities.createStorageDirs(DATA_NODE, dataNodeDirs, "current");
      int namespaceId = cluster.getNameNode().getNamespaceID();
      baseDirs = UpgradeUtilities.createFederatedDatanodeDirs(dataNodeDirs,
          "previous", namespaceId);
      UpgradeUtilities.createFederatedDatanodesVersionFiles(
          baseDirs,
          namespaceId,
          new StorageInfo(Integer.MIN_VALUE, UpgradeUtilities
              .getCurrentNamespaceID(cluster), Long.MAX_VALUE), "previous");
      
      UpgradeUtilities.createVersionFile(DATA_NODE, baseDirs,
                                         new StorageInfo(Integer.MIN_VALUE,
                                                         UpgradeUtilities.getCurrentNamespaceID(cluster),
                                                         UpgradeUtilities.getCurrentFsscTime(cluster)), cluster.getNameNode().getNamespaceID());
      startDataNodeShouldFail(StartupOption.ROLLBACK);
      cluster.shutdown();
      UpgradeUtilities.createEmptyDirs(nameNodeDirs);
      UpgradeUtilities.createEmptyDirs(dataNodeDirs);
      
      log("DataNode rollback with newer fsscTime in previous", numDirs);
      UpgradeUtilities.createStorageDirs(NAME_NODE, nameNodeDirs, "current");
      UpgradeUtilities.createStorageDirs(NAME_NODE, nameNodeDirs, "previous");
      cluster = new MiniDFSCluster(conf, 0, StartupOption.ROLLBACK);
      UpgradeUtilities.createStorageDirs(DATA_NODE, dataNodeDirs, "current");
      baseDirs = UpgradeUtilities.createFederatedDatanodeDirs(dataNodeDirs,
          "previous", namespaceId); 
      UpgradeUtilities.createFederatedDatanodesVersionFiles(baseDirs, namespaceId,
                                         new StorageInfo(UpgradeUtilities.getCurrentLayoutVersion(),
                                                         UpgradeUtilities.getCurrentNamespaceID(cluster),
                                                         Long.MAX_VALUE), "previous");
      startDataNodeShouldFail(StartupOption.ROLLBACK);
      cluster.shutdown();
      UpgradeUtilities.createEmptyDirs(nameNodeDirs);
      UpgradeUtilities.createEmptyDirs(dataNodeDirs);

      log("NameNode rollback with no edits file", numDirs);
      UpgradeUtilities.createStorageDirs(NAME_NODE, nameNodeDirs, "current");
      baseDirs = UpgradeUtilities.createStorageDirs(NAME_NODE, nameNodeDirs, "previous");
      deleteMatchingFiles(baseDirs, "edits.*");
      startNameNodeShouldFail(StartupOption.ROLLBACK);
      UpgradeUtilities.createEmptyDirs(nameNodeDirs);
      
      log("NameNode rollback with no image file", numDirs);
      UpgradeUtilities.createStorageDirs(NAME_NODE, nameNodeDirs, "current");
      baseDirs = UpgradeUtilities.createStorageDirs(NAME_NODE, nameNodeDirs, "previous");
      deleteMatchingFiles(baseDirs, "fsimage_.*");
      startNameNodeShouldFail(StartupOption.ROLLBACK);
      UpgradeUtilities.createEmptyDirs(nameNodeDirs);
      
      log("NameNode rollback with corrupt version file", numDirs);
      UpgradeUtilities.createStorageDirs(NAME_NODE, nameNodeDirs, "current");
      baseDirs = UpgradeUtilities.createStorageDirs(NAME_NODE, nameNodeDirs, "previous");
      for (File f : baseDirs) { 
        UpgradeUtilities.corruptFile(new File(f,"VERSION")); 
      }
      startNameNodeShouldFail(StartupOption.ROLLBACK);
      UpgradeUtilities.createEmptyDirs(nameNodeDirs);
      
      log("NameNode rollback with old layout version in previous", numDirs);
      UpgradeUtilities.createStorageDirs(NAME_NODE, nameNodeDirs, "current");
      baseDirs = UpgradeUtilities.createStorageDirs(NAME_NODE, nameNodeDirs, "previous");
      UpgradeUtilities.createVersionFile(NAME_NODE, baseDirs,
                                         new StorageInfo(1,
                                                         UpgradeUtilities.getCurrentNamespaceID(null),
                                                         UpgradeUtilities.getCurrentFsscTime(null)), 0);
      startNameNodeShouldFail(StartupOption.UPGRADE);
      UpgradeUtilities.createEmptyDirs(nameNodeDirs);
    } // end numDir loop
  }
  
  private void deleteMatchingFiles(File[] baseDirs, String regex) {
    for (File baseDir : baseDirs) {
      for (File f : baseDir.listFiles()) {
        if (f.getName().matches(regex)) {
          f.delete();
        }
      }
    }
  }
 
  protected void tearDown() throws Exception {
    LOG.info("Shutting down MiniDFSCluster");
    if (cluster != null) cluster.shutdown();
  }
  
  public static void main(String[] args) throws Exception {
    new TestDFSRollback().testRollback();
  }
  
}


