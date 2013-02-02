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

import java.io.IOException;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertNull;

public class TestCurrentInProgressMetadata extends BookKeeperSetupUtil {

  private static final String TEST_ZK_PARENT = "/testCurrentInProgress";

  private static final String FQ_ZNODE_PATH =
      TEST_ZK_PARENT + Path.SEPARATOR + "currentInProgress";

  @Before
  public void setUp() throws Exception {
    super.setUp();
    getRecoveringZookeeperClient().create(TEST_ZK_PARENT, new byte[] { '0' },
        ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
  }

  @After
  public void tearDown() throws Exception {
    super.tearDown();
    if (getRecoveringZookeeperClient() != null) {
      zkDeleteRecursively(TEST_ZK_PARENT);
    }
  }

  private CurrentInProgressMetadata getCurrentInProgress() throws IOException {
    CurrentInProgressMetadata cip = new CurrentInProgressMetadata(
        getRecoveringZookeeperClient(), FQ_ZNODE_PATH);
    cip.init();
    return cip;
  }

  // Test initialization
  @Test
  public void testInit() throws Exception {
    CurrentInProgressMetadata cip = getCurrentInProgress();
    assertNull("Current in progress node is clear immediately after creation",
        cip.read());
  }

  // Tests storing a path and then reading it
  @Test
  public void testUpdate() throws Exception {
    CurrentInProgressMetadata cip = getCurrentInProgress();
    cip.update("/foo");
    assertEquals("Update sets correct path", "/foo", cip.read());
    cip.update("/bar");
    assertEquals("Can update an updated path", "/bar", cip.read());
  }

  // Test clearing out the ZNode
  @Test
  public void testClear() throws Exception {
    CurrentInProgressMetadata cip = getCurrentInProgress();
    cip.update("/foo");
    cip.clear();
    assertNull("clear() resets back to null", cip.read());
    cip.update("/bar");
    assertEquals("Can update a path after clear", "/bar", cip.read());
  }

  // Test that an exception is thrown if we try to call update
  // without having read the latest in-progress path
  @Test(expected = StaleVersionException.class)
  public void testUpdateWithoutRead() throws Exception {
    CurrentInProgressMetadata cip = getCurrentInProgress();
    cip.update("/foo");
    cip.read();
    cip.update("/bar");
    cip.update("/shouldFailAtThisPoint");
  }
}
