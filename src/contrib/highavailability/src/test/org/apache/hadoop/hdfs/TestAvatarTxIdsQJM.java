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
import static org.junit.Assert.fail;

import java.io.IOException;

import org.apache.hadoop.hdfs.server.namenode.AvatarNode;
import org.junit.Test;

public class TestAvatarTxIdsQJM extends TestAvatarTxIds {
	
	@Test
  public void testWithFailover() throws Exception {
  	doTestWithFailover(true);
  }
	
	@Test
  public void testDoubleFailover() throws Exception {
  	doTestDoubleFailover(true);
  }
	
	@Test
  public void testWithStandbyDead() throws Exception {
  	doTestWithStandbyDead(true);
  }
	
	@Test
  public void testWithStandbyDeadAfterFailover() throws Exception {
  	doTestWithStandbyDeadAfterFailover(true);
  }
	
	@Test
  public void testWithCheckPoints() throws Exception {
  	doTestWithCheckPoints(true);
  }
	
	@Test
  public void testAcrossRestarts() throws Exception {
  	doTestAcrossRestarts(true);
  }

	@Test
	public void testWithFailoverTxIdMismatchHard() throws Exception {
	  setUp("testWithFailoverTxIdMismatchHard", true);
	  // Create edits before failover.
	  createEdits(20);
	  AvatarNode primary = cluster.getPrimaryAvatar(0).avatar;
	  AvatarNode standby = cluster.getStandbyAvatar(0).avatar;

	  // SLS + ELS (first checkpoint with no txns) + SLS + 20 edits
	  assertEquals(23, getCurrentTxId(primary));

	  standby.getFSImage().getEditLog().setLastWrittenTxId(49);
	  assertEquals(50, getCurrentTxId(standby));

	  // Perform failover and verify it fails.
	  try {
	    cluster.failOver();
	  } catch (IOException e) {
	    System.out.println("Expected exception : " + e);
	    return;
	  }
	  fail("Did not throw exception");
	}
}
