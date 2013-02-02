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

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.hdfs.server.datanode.AvatarDataNode;
import org.apache.hadoop.hdfs.server.protocol.NamespaceInfo;
import org.apache.hadoop.hdfs.util.InjectionEvent;
import org.apache.hadoop.util.InjectionEventI;
import org.apache.hadoop.util.InjectionHandler;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import static org.junit.Assert.*;
import org.junit.Test;

public class TestAvatarUpgrade extends AvatarSetupUtil {

  final static Log LOG = LogFactory.getLog(TestAvatarUpgrade.class);
  private Configuration conf;

  protected static final String FILE_PATH = "/dir1/dir2/myfile";
  private static final int BLK_SIZE = 1024;
  TestAvatarUpgradeInjectionHandler h;

  /**
   * Creates n files and waits for replication.
   */
  private void createFiles(int n, int blocks) throws IOException {
    for (int i = 0; i < n; i++) {
      Path path = new Path(FILE_PATH + i);
      DFSTestUtil.createFile(dafs, path, blocks * BLK_SIZE, (short) 3, 0L);
    }
    for (int i = 0; i < n; i++) {
      Path path = new Path(FILE_PATH + i);
      DFSTestUtil.waitReplication(dafs, path, (short) 3);
    }
  }

  private void setUp(String name, int layoutDelta) throws Exception {
    setUp(name, layoutDelta, true, true);
  }

  /**
   * @param name
   *          test name
   * @param layoutDelta
   *          by how much the NN LV is higher than DN LV
   */
  private void setUp(String name, int layoutDelta, boolean waitForDatanodes,
      boolean createFiles) throws Exception {

    h = new TestAvatarUpgradeInjectionHandler();
    h.clearState();
    h.diff = layoutDelta;
    InjectionHandler.set(h);

    conf = new Configuration();
    conf.setBoolean("fs.checkpoint.enabled", true);
    conf.setBoolean("fs.checkpoint.wait", true);
    conf.setInt("dfs.block.size", BLK_SIZE);
    conf.setLong("dfs.blockreport.intervalMsec", 500);
    conf.setBoolean("fs.datanodes.wait", waitForDatanodes);

    setUp(false, conf, "testBasic", false);

    if (createFiles) {
      // create one file with 10 blocks
      createFiles(1, 10);
    }
  }

  long now() {
    return System.currentTimeMillis();
  }

  void waitAndAssertFinalize(int n, boolean shouldProcess) {
    long start = now();
    while (now() - start < 30000) {
      if (h.processedEvents.get(InjectionEvent.OFFERSERVICE_DNAFINALIZE) >= n) {
        break;
      }
      LOG.info("--- Will sleep waiting for DNA_FINALIZE events");
      DFSTestUtil.waitNSecond(1);
    }
    // we should only be getting DNA_FINALIZE from the primary
    assertFalse(h.receivedFinalizeFromStandby);
    h.assertAllShouldProcess(shouldProcess);
  }

  @Test
  public void testBasic() throws Exception {
    // standard situation, not layout mismatch
    setUp("testBasic", 0);
    // wait for 6 DNA_FINALIZE (2 per datanode)
    waitAndAssertFinalize(6, true);
  }

  @Test
  public void testNNOlderThanDN() throws Exception {
    // NN LV is 1 higher than DN LV (NN is older)
    // this should be ok, but we do not process DNA_FINALIZE commands
    setUp("testNNOlderThanDN", 1);
    // wait for 6 DNA_FINALIZE (2 per datanode)
    // none of the DNA_FINALIZE should be processed
    waitAndAssertFinalize(6, false);

    // now we will reinstantiate matching namenodes
    cluster.shutDownAvatarNodes();

    // layout will be matching
    h.diff = 0;
    h.clearState();

    // restart avatarnodes
    cluster.restartAvatarNodes();

    // wait for 6 DNA_FINALIZE (2 per datanode)
    // DNA_FINALIZE should now be processed by the old datanodes
    waitAndAssertFinalize(6, true);
  }

  @Test
  public void testNNNewerThanDN() throws Exception {
    // NN LV is 1 lower than DN LV (NN is newer)
    // this should shutdown datanodes
    
    // first set up normal cluster
    setUp("testNNNewerThanDN", 0, false, false);

    // now simulate that some datanodes come back
    // with older LV
    h.diff = -1;
    cluster.restartDataNodes(false);

    // wait until all datanodes will shutdown
    long start = now();
    boolean alive = false;
    while (now() - start < 30000) {
      alive = false;
      for (AvatarDataNode dn : cluster.getDataNodes()) {
        alive |= dn.shouldRun();
      }
      if (!alive)
        break;
    }
    assertFalse(alive);
  }

  class TestAvatarUpgradeInjectionHandler extends InjectionHandler {
    // what layout version to pass from FSNamesystem to datanodes
    // wrt to the FSConstants layout number
    int diff = 0;

    // keep track of DNA-FINALIZE and whether the datanodes are processing them
    Map<InjectionEventI, Integer> processedEvents = new HashMap<InjectionEventI, Integer>();
    List<Boolean> shouldProcessList = new ArrayList<Boolean>();
    volatile boolean receivedFinalizeFromStandby = false;

    // assert that all DNA_FINALIZE were either processed or discarded
    private void assertAllShouldProcess(boolean assertion) {
      synchronized (processedEvents) {
        for (Boolean b : shouldProcessList) {
          assertEquals(assertion, b);
        }
      }
    }

    // clear internal state
    private void clearState() {
      synchronized (processedEvents) {
        processedEvents.clear();
        receivedFinalizeFromStandby = false;
        shouldProcessList.clear();
        processedEvents.put(InjectionEvent.OFFERSERVICE_DNAFINALIZE, 0);
      }
    }

    @Override
    protected void _processEvent(InjectionEventI event, Object... args) {
      synchronized (processedEvents) {
        Integer count = processedEvents.get(event) == null ? 0
            : processedEvents.get(event);
        processedEvents.put(event, ++count);

        if (event == InjectionEvent.OFFERSERVICE_DNAFINALIZE) {
          boolean isStandby = !(Boolean) args[1];
          if (isStandby) {
            // this should never happen, we will use it to trigger failure
            receivedFinalizeFromStandby = true;
          }
          // keep track if the DNA_FINALIZE was processed
          boolean shouldProcess = (Boolean) args[0];
          shouldProcessList.add(shouldProcess);
        }
      }

      // simulate layout mismatch
      if (event == InjectionEvent.FSNAMESYSTEM_VERSION_REQUEST) {
        NamespaceInfo ni = (NamespaceInfo) args[0];
        ni.layoutVersion += diff;
      }
    }
  }

}
