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

import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.tools.FastCopy;

import static org.junit.Assert.*;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestFastCopyWithoutHardLink extends FastCopySetupUtil {

  @BeforeClass
  public static void setUpClass() throws Exception {
    conf = new Configuration();
    remoteConf = new Configuration();
    conf.setBoolean("dfs.datanode.blkcopy.hardlink", false);
    remoteConf.setBoolean("dfs.datanode.blkcopy.hardlink", false);
    FastCopySetupUtil.setUpClass();
  }

  @Test
  public void testFastCopy() throws Exception {
    super.testFastCopy(false);
  }

  @Test
  public void testFastCopyOldAPI() throws Exception {
    super.testFastCopyOldAPI(false);
  }

  @Test
  public void testFastCopyMultiple() throws Exception {
    super.testFastCopyMultiple(false);
  }

  @Test
  public void testInterFileSystemFastCopy() throws Exception {
    super.testInterFileSystemFastCopy(false);
  }

  @Test
  public void testInterFileSystemFastCopyMultiple() throws Exception {
    super.testInterFileSystemFastCopyMultiple(false);
  }

  @Test
  public void testInterFileSystemFastCopyShellMultiple() throws Exception {
    super.testInterFileSystemFastCopyShellMultiple(false, new String[0]);
  }

  private DatanodeInfo get(String name) {
    return new DatanodeInfo(new DatanodeID(name));
  }

  @Test
  public void testAlignDatanodes() {
    DatanodeInfo[] dst = new DatanodeInfo[] { get("c"), get("a"), get("b") };
    DatanodeInfo[] src = new DatanodeInfo[] { get("a"), get("c"), get("b") };
    FastCopy.alignDatanodes(dst, src);
    assertTrue(Arrays.equals(dst, src));
  }

  @Test
  public void testAlignDatanodes1() {
    DatanodeInfo[] dst = new DatanodeInfo[] { get("c"), get("a"), get("b") };
    DatanodeInfo[] src = new DatanodeInfo[] { get("c"), get("b") };
    FastCopy.alignDatanodes(dst, src);
    assertEquals("c", dst[0].getName(), src[0].getName());
    assertEquals("b", dst[1].getName(), src[1].getName());
  }

  @Test
  public void testAlignDatanodes2() {
    DatanodeInfo[] dst = new DatanodeInfo[] { get("d"), get("a"), get("b") };
    DatanodeInfo[] src = new DatanodeInfo[] { get("c"), get("d"), get("z") };
    FastCopy.alignDatanodes(dst, src);
    assertEquals("d", dst[0].getName(), src[0].getName());
    assertEquals("a", dst[1].getName());
    assertEquals("b", dst[2].getName());
  }
}
