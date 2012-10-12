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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.FSConstants.SafeModeAction;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.Test;

import static org.apache.hadoop.hdfs.server.namenode.NNStorage.getInProgressEditsFileName;
import static org.apache.hadoop.hdfs.server.namenode.NNStorage.getFinalizedEditsFileName;
import static org.apache.hadoop.hdfs.server.namenode.NNStorage.getImageFileName;


/**
 * Functional tests for NNStorageRetentionManager. This differs from
 * {@link TestNNStorageRetentionManager} in that the other test suite
 * is only unit/mock-based tests whereas this suite starts miniclusters,
 * etc.
 */
public class TestNNStorageRetentionFunctional {

  private static File TEST_ROOT_DIR =
    new File(System.getProperty("test.build.data"));
  private static Log LOG = LogFactory.getLog(
      TestNNStorageRetentionFunctional.class);

 /**
  * Test case where two directories are configured as NAME_AND_EDITS
  * and one of them fails to save storage. Since the edits and image
  * failure states are decoupled, the failure of image saving should
  * not prevent the purging of logs from that dir.
  */
  @Test
  public void testPurgingWithNameEditsDirAfterFailure()
      throws IOException {
    MiniDFSCluster cluster = null;    
    Configuration conf = new Configuration();

    File sd0 = null;    
    try {
      cluster = new MiniDFSCluster(conf, 0, true, null);
      File[] storageDirs = cluster.getNameDirs().toArray(new File[0]);
      sd0 = storageDirs[0];
      File cd0 = new File(storageDirs[0], "current");
      File cd1 = new File(storageDirs[1], "current");
  
      NameNode nn = cluster.getNameNode();

      doSaveNamespace(nn);
      // special case of an empty image with txid = -1
      LOG.info("After first save, images -1 and 1 should exist in both dirs");
      GenericTestUtils.assertGlobEquals(cd0, "fsimage_(-?\\d+)", 
          getImageFileName(-1), getImageFileName(1));
      GenericTestUtils.assertGlobEquals(cd1, "fsimage_(-?\\d+)",
          getImageFileName(-1), getImageFileName(1));
      GenericTestUtils.assertGlobEquals(cd0, "edits_.*",
          getFinalizedEditsFileName(0, 1),
          getInProgressEditsFileName(2));
      GenericTestUtils.assertGlobEquals(cd1, "edits_.*",
          getFinalizedEditsFileName(0, 1),
          getInProgressEditsFileName(2));
      
      doSaveNamespace(nn);
      LOG.info("After second save, image 0 should be purged, " +
          "and image 4 should exist in both.");
      GenericTestUtils.assertGlobEquals(cd0, "fsimage_\\d*",
          getImageFileName(1), getImageFileName(3));
      GenericTestUtils.assertGlobEquals(cd1, "fsimage_\\d*",
          getImageFileName(1), getImageFileName(3));
      GenericTestUtils.assertGlobEquals(cd0, "edits_.*",
          getFinalizedEditsFileName(2, 3),
          getInProgressEditsFileName(4));
      GenericTestUtils.assertGlobEquals(cd1, "edits_.*",
          getFinalizedEditsFileName(2, 3),
          getInProgressEditsFileName(4));
      
      LOG.info("Failing first storage dir by chmodding it");
      sd0.setExecutable(false);
      doSaveNamespace(nn);      
      LOG.info("Restoring accessibility of first storage dir");      
      sd0.setExecutable(true);

      LOG.info("nothing should have been purged in first storage dir");
      GenericTestUtils.assertGlobEquals(cd0, "fsimage_\\d*",
          getImageFileName(1), getImageFileName(3));
      GenericTestUtils.assertGlobEquals(cd0, "edits_.*",
          getFinalizedEditsFileName(2, 3),
          getInProgressEditsFileName(4));

      LOG.info("fsimage_2 should be purged in second storage dir");
      GenericTestUtils.assertGlobEquals(cd1, "fsimage_\\d*",
          getImageFileName(3), getImageFileName(5));
      GenericTestUtils.assertGlobEquals(cd1, "edits_.*",
          getFinalizedEditsFileName(4, 5),
          getInProgressEditsFileName(6));

      LOG.info("On next save, we should purge logs and images from the failed dir," +
          " , since the image directory is restored.");
      doSaveNamespace(nn);
      GenericTestUtils.assertGlobEquals(cd1, "fsimage_\\d*",
          getImageFileName(5), getImageFileName(7));
      GenericTestUtils.assertGlobEquals(cd1, "edits_.*",
          getFinalizedEditsFileName(6, 7),
          getInProgressEditsFileName(8));
      GenericTestUtils.assertGlobEquals(cd0, "fsimage_\\d*",
          getImageFileName(7));
      GenericTestUtils.assertGlobEquals(cd0, "edits_.*",
          getInProgressEditsFileName(8));
    } finally {
      if (sd0 != null)
        sd0.setExecutable(true);

      LOG.info("Shutting down...");
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  private static void doSaveNamespace(NameNode nn) throws IOException {
    LOG.info("Saving namespace...");
    nn.setSafeMode(SafeModeAction.SAFEMODE_ENTER);
    nn.saveNamespace();
    nn.setSafeMode(SafeModeAction.SAFEMODE_LEAVE);
  }
}
