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

import java.io.IOException;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.MiniAvatarCluster;
import org.apache.hadoop.hdfs.server.namenode.FSEditLog;
import org.apache.hadoop.hdfs.server.namenode.FSImageTestUtil.CheckpointTrigger;
import org.apache.hadoop.hdfs.util.InjectionEvent;
import org.apache.hadoop.util.InjectionEventI;
import org.apache.hadoop.util.InjectionHandler;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.junit.After;
import org.junit.AfterClass;
import static org.junit.Assert.*;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.mockito.Matchers.anyInt;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class TestAvatarCheckpointingFailure {

  final static Log LOG = LogFactory
      .getLog(TestAvatarCheckpointingFailure.class);

  private Runtime runtime;
  private MiniAvatarCluster cluster;
  private Configuration conf;
  private FileSystem fs;
  private Random random = new Random();

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    MiniAvatarCluster.createAndStartZooKeeper();
  }

  private void setUp(String name) throws Exception {
    LOG.info("------------------- test: " + name + " START ----------------");
    conf = new Configuration();
    conf.setLong("fs.checkpoint.period", 3600);

    cluster = new MiniAvatarCluster(conf, 2, true, null, null);
    fs = cluster.getFileSystem();

    // spy the runtime
    runtime = Runtime.getRuntime();
    runtime = spy(runtime);
    doNothing().when(runtime).exit(anyInt());
    FSEditLog.setRuntimeForTesting(runtime);
  }

  @After
  public void tearDown() throws Exception {
    fs.close();
    cluster.shutDown();
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    MiniAvatarCluster.shutDownZooKeeper();
  }

  public void createEdits(int nEdits) throws IOException {
    for (int i = 0; i < nEdits / 2; i++) {
      // Create file ends up logging two edits to the edit log, one for create
      // file and one for bumping up the generation stamp
      fs.create(new Path("/" + random.nextInt()));
    }
  }

  // ////////////////////////////

  @Test
  public void testCheckpointMaxFailures() throws Exception {
    TestAvatarCheckpointingFailureHandler h = new TestAvatarCheckpointingFailureHandler();
    InjectionHandler.set(h);
    setUp("testCheckpointWithRestarts");
    // simulate interruption, no ckpt failure
    createEdits(20);
    h.disabled = false;

    // 11-th failed checkpoint should cause runtime.exit()
    for (int i = 0; i < Standby.MAX_CHECKPOINT_FAILURES + 1; i++) {
      try {
        h.doCheckpoint();
        fail("Checkpoint should not succeed");
      } catch (IOException e) {
        LOG.info("Expected exception", e);
      }
    }
    verify(runtime, times(1)).exit(anyInt());
  }

  class TestAvatarCheckpointingFailureHandler extends InjectionHandler {
    private CheckpointTrigger ckptTrigger = new CheckpointTrigger();
    int checkpointFailureCount = 0;
    volatile boolean disabled = true;

    @Override
    protected void _processEventIO(InjectionEventI event, Object... args)
        throws IOException {
      if (!disabled) {
        if (event == InjectionEvent.STANDBY_BEFORE_ROLL_IMAGE) {
          throw new IOException("Simulating checkpoint failure");
        }
      }
    }

    @Override
    protected void _processEvent(InjectionEventI event, Object... args) {
      if (event == InjectionEvent.STANDBY_EXIT_CHECKPOINT_EXCEPTION) {
        checkpointFailureCount++;
        LOG.info("Chackpoint failures so far: " + checkpointFailureCount);
      }
      ckptTrigger.checkpointDone(event, args);
    }

    @Override
    protected boolean _falseCondition(InjectionEventI event, Object... args) {
      return ckptTrigger.triggerCheckpoint(event);
    }

    void doCheckpoint() throws IOException {
      ckptTrigger.doCheckpoint();
    }
  }
}
