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

import org.apache.hadoop.hdfs.server.common.HdfsConstants.StartupOption;
import org.apache.hadoop.hdfs.server.common.HdfsConstants.Transition;
import org.apache.hadoop.hdfs.server.namenode.metrics.NameNodeMetrics;
import org.apache.hadoop.hdfs.server.protocol.NamespaceInfo;
import org.junit.Test;

import static org.mockito.Mockito.mock;
import static org.junit.Assert.*;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.server.common.Storage.StorageState;
import org.apache.hadoop.hdfs.server.common.StorageInfo;
import org.apache.hadoop.hdfs.server.protocol.RemoteEditLogManifest;
import org.apache.hadoop.conf.Configuration;

import java.net.URI;
import java.util.Collection;
import java.io.IOException;

public class TestGenericJournalConf {
  /** 
   * Test that an exception is thrown if a journal class doesn't exist
   * in the configuration 
   */
  @Test(expected=IllegalArgumentException.class)
  public void testNotConfigured() throws Exception {
    MiniDFSCluster cluster = null;
    Configuration conf = new Configuration();

    conf.set("dfs.name.edits.dir", "dummy://test");
    try {
      cluster = new MiniDFSCluster(conf, 0, true, null);
      cluster.waitActive();
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  /**
   * Test that an exception is thrown if a journal class doesn't
   * exist in the classloader.
   */
  @Test(expected=IllegalArgumentException.class)
  public void testClassDoesntExist() throws Exception {
    MiniDFSCluster cluster = null;
    Configuration conf = new Configuration();

    conf.set("dfs.name.edits.journal-plugin" + ".dummy",
             "org.apache.hadoop.nonexistent");
    conf.set("dfs.name.edits.dir",
             "dummy://test");

    try {
      cluster = new MiniDFSCluster(conf, 0, true, null);
      cluster.waitActive();
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  /**
   * Test that a implementation of JournalManager without a 
   * (Configuration,URI) constructor throws an exception
   */
  @Test
  public void testBadConstructor() throws Exception {
    MiniDFSCluster cluster = null;
    Configuration conf = new Configuration();
    
    conf.set("dfs.name.edits.journal-plugin" + ".dummy",
        BadConstructorJournalManager.class.getName());
    conf.set("dfs.name.edits.dir",
        "dummy://test");
    
    try {
      cluster = new MiniDFSCluster(conf, 0, true, null);
      cluster.waitActive();
      fail("Should have failed before this point");
    } catch (IllegalArgumentException iae) {
      if (!iae.getMessage().contains("Unable to construct journal")) {
        fail("Should have failed with unable to construct exception");
      }
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  /**
   * Test that a dummy implementation of JournalManager can
   * be initialized on startup
   */
  @Test
  public void testDummyJournalManager() throws Exception {
    MiniDFSCluster cluster = null;
    Configuration conf = new Configuration();

    conf.set("dfs.name.edits.journal-plugin" + ".dummy",
        DummyJournalManager.class.getName());
    conf.set("dfs.name.edits.dir",
        "dummy://test");

    try {
      cluster = new MiniDFSCluster(conf, 0, true, null);
      cluster.waitActive();
      
      assertTrue(DummyJournalManager.shouldPromptCalled);
      assertTrue(DummyJournalManager.formatCalled);
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  public static class DummyJournalManager implements JournalManager {
    
    static boolean formatCalled = false;
    static boolean shouldPromptCalled = false;

    public DummyJournalManager(Configuration conf, URI u, NamespaceInfo nsInfo,
        NameNodeMetrics metrics) { }
    
    @Override
    public EditLogOutputStream startLogSegment(long txId) throws IOException {
      return mock(EditLogOutputStream.class);
    }
    
    @Override
    public void finalizeLogSegment(long firstTxId, long lastTxId)
        throws IOException {
      // noop
    }

    @Override
    public void purgeLogsOlderThan(long minTxIdToKeep)
        throws IOException {}

    @Override
    public void recoverUnfinalizedSegments() throws IOException {}

    @Override
    public void close() throws IOException {}
    
    @Override
    public boolean hasSomeJournalData() throws IOException {
      shouldPromptCalled = true;
      return false;
    }
    
    @Override
    public boolean hasSomeImageData() throws IOException {
      shouldPromptCalled = true;
      return false;
    }

    @Override
    public void transitionJournal(StorageInfo nsInfo, Transition transition,
        StartupOption startOpt) throws IOException {
      if (Transition.FORMAT == transition) {
        formatCalled = true;
      }
    }

    @Override
    public RemoteEditLogManifest getEditLogManifest(long fromTxId)
        throws IOException {
      return null;
    }

    @Override
    public void selectInputStreams(Collection<EditLogInputStream> streams,
        long fromTxId, boolean inProgressOk, boolean validateInProgressSegments)
        throws IOException {
    }

    @Override
    public String toHTMLString() {
      return this.toString();
    }

    @Override
    public void setCommittedTxId(long txid, boolean force) throws IOException {
    }

    @Override
    public boolean hasImageStorage() {
      return false;
    }

    @Override
    public RemoteStorageState analyzeJournalStorage() {
      return new RemoteStorageState(StorageState.NON_EXISTENT, new StorageInfo());
    }
  }

  public static class BadConstructorJournalManager extends DummyJournalManager {
    public BadConstructorJournalManager() {
      super(null, null, null, null);
    }
  }
}
