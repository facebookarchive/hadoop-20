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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.server.common.Storage.StorageDirectory;
import org.apache.hadoop.hdfs.util.InjectionEvent;
import org.apache.hadoop.hdfs.util.MD5FileUtils;
import org.apache.hadoop.util.InjectionEventI;
import org.apache.hadoop.util.InjectionHandler;
import org.junit.After;
import org.junit.Test;

public class TestSaveNamespaceCancel {

  private MiniDFSCluster cluster;
  private Configuration conf;
  private static Log LOG = LogFactory.getLog(TestSaveNamespaceCancel.class);

  public void setUp(String name) throws IOException {
    conf = new Configuration();
    cluster = new MiniDFSCluster(conf, 3, true, null);
  }

  @Test
  public void testSaveNamespaceCancelledCleanup() throws IOException {
    setUp("testSaveNamespaceCancelledCleanup");
    TestSaveNamespaceCancelHandler h = new TestSaveNamespaceCancelHandler();
    InjectionHandler.set(h);

    // first save namespace should succeed
    cluster.getNameNode().saveNamespace(true, false);

    long ckptTxId = cluster.getNameNode().getFSImage().getLastAppliedTxId() + 1;
    h.disabled = false;

    try {
      cluster.getNameNode().saveNamespace(true, false);
      fail("This should fail as we cancel SN");
    } catch (IOException e) {
      LOG.info("Got exception:", e);
    }

    // assert that all files for checkpoints have been deleted
    for (Iterator<StorageDirectory> it = cluster.getNameNode().getFSImage().storage
        .dirIterator(); it.hasNext();) {
      StorageDirectory sd = it.next();
      File image = NNStorage.getImageFile(sd, ckptTxId);
      assertFalse(image.exists());
      assertFalse(NNStorage.getCheckpointImageFile(sd, ckptTxId).exists());
      assertFalse(MD5FileUtils.getDigestFileForFile(image).exists());
    }
  }

  @After
  public void tearDown() {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  class TestSaveNamespaceCancelHandler extends InjectionHandler {
    volatile boolean disabled = true;

    @Override
    public void _processEvent(InjectionEventI event, Object... args) {
      if (event == InjectionEvent.FSIMAGE_SAVED_IMAGE && !disabled) {
        cluster.getNameNode().getNamesystem()
            .cancelSaveNamespace("Injected failure");
      }
    }
  }

}
