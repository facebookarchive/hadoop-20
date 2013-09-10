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

import static org.junit.Assert.assertEquals;

import org.apache.hadoop.hdfs.server.namenode.AvatarNode;
import org.apache.hadoop.util.InjectionHandler;
import org.junit.Test;

public class TestAvatarCheckpointingManualQJM extends
		TestAvatarCheckpointingManual {

	@Test
  public void testDeployedCheckpoint() throws Exception {
  	doTestDelayedCheckpoint(true);
  }
	
	@Test
  public void testStandbySaveNamespace() throws Exception {
    TestAvatarCheckpointingHandler h = new TestAvatarCheckpointingHandler();
    InjectionHandler.set(h);
    setUp(3600, "testStandbySaveNamespace", true);

    AvatarNode primary = cluster.getPrimaryAvatar(0).avatar;
    AvatarNode standby = cluster.getStandbyAvatar(0).avatar;
    createEdits(40);

    // trigger checkpoint on the standby
    AvatarShell shell = new AvatarShell(conf);

    // trigger regular checkpoint
    runAndAssertCommand(shell, 0, new String[] { "-one", "-saveNamespace" });
    assertCheckpointDone(h);

    // trigger regular checkpoint with uncompressed option
    // this will fail, as we do not support uncompressed
    runAndAssertCommand(shell, -1, new String[] { "-one", "-saveNamespace",
        "uncompressed" });

    // trigger regular checkpoint with force option
    runAndAssertCommand(shell, 0, new String[] { "-one", "-saveNamespace",
        "force" });
    assertCheckpointDone(h);

    // trigger regular checkpoint with force, uncompressed option
    // this will fail, as we do not support uncompressed
    runAndAssertCommand(shell, -1, new String[] { "-one", "-saveNamespace",
        "force", "uncompressed" });

    // ///////////////////

    // some incorrect option
    runAndAssertCommand(shell, -1, new String[] { "-one", "-saveNamespace",
        "someOption" });

    standby.quiesceStandby(DFSAvatarTestUtil.getCurrentTxId(primary) - 1);
    // edits + SLS + 2 * 2(for each successful checkpoint)
    assertEquals(45, DFSAvatarTestUtil.getCurrentTxId(primary));
  }
}
