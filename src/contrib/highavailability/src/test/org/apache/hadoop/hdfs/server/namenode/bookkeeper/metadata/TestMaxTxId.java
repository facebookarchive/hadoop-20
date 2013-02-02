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
package org.apache.hadoop.hdfs.server.namenode.bookkeeper.metadata;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.server.namenode.bookkeeper.BookKeeperSetupUtil;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static junit.framework.Assert.assertEquals;

public class TestMaxTxId extends BookKeeperSetupUtil {

  private static final String TEST_ZK_PARENT = "/testMaxTxId";

  private static final String FQ_ZNODE_PATH =
      TEST_ZK_PARENT + Path.SEPARATOR + "maxTxId";

  private MaxTxId newMaxTxId() {
    return new MaxTxId(getRecoveringZookeeperClient(), FQ_ZNODE_PATH);
  }

  @Before
  public void setUp() throws Exception {
    super.setUp();
    getRecoveringZookeeperClient().create(TEST_ZK_PARENT, new byte[] {'0'},
        ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
  }

  @After
  public void tearDown() throws Exception {
    super.tearDown();
    if (getRecoveringZookeeperClient() != null) {
      zkDeleteRecursively(TEST_ZK_PARENT);
    }
  }

  // Test initialization
  @Test
  public void testInit() throws Exception {
    MaxTxId maxTxId = newMaxTxId();
    assertEquals("Initial value should be -1", maxTxId.get(), -1);
  }

  // Test storing and reading a value and verify that store will not
  // store a value that is lower than the current value
  @Test
  public void testStore() throws Exception {
    MaxTxId maxTxId = newMaxTxId();
    maxTxId.store(0);
    assertEquals("store() should work", maxTxId.get(), 0);
    maxTxId.store(2);
    assertEquals("store() should work if new maxTxId > current maxTxId",
        maxTxId.get(), 2);
    maxTxId.store(1);
    assertEquals("store() should not work if new maxTxId < current maxTxId",
        maxTxId.get(), 2);
  }

  // Test correct semantics for set
  @Test
  public void testSet() throws Exception {
    MaxTxId maxTxId = newMaxTxId();
    maxTxId.store(2);
    maxTxId.set(1);
    assertEquals("set() should reset to any maxTxId", maxTxId.get(), 1);
  }

  // Test that an exception is thrown if we try to call reset without
  // having read the last transaction id
  @Test(expected = StaleVersionException.class)
  public void testResetWithoutRead() throws Exception {
    MaxTxId maxTxId = newMaxTxId();
    maxTxId.store(2);
    maxTxId.set(3);
    maxTxId.set(1);
  }
}
