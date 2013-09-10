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

import org.apache.hadoop.conf.Configuration;

import org.junit.BeforeClass;
import org.junit.Test;

public class TestFastCopyWithHardLink extends FastCopySetupUtil {

  @BeforeClass
  public static void setUpClass() throws Exception {
    conf = new Configuration();
    remoteConf = new Configuration();
    conf.setBoolean("dfs.datanode.blkcopy.hardlink", true);
    remoteConf.setBoolean("dfs.datanode.blkcopy.hardlink", true);
    FastCopySetupUtil.setUpClass();
  }

  @Test
  public void testFastCopy() throws Exception {
    super.testFastCopy(true);
  }

  @Test
  public void testFastCopyOldAPI() throws Exception {
    super.testFastCopyOldAPI(true);
  }

  @Test
  public void testFastCopyMultiple() throws Exception {
    super.testFastCopyMultiple(true);
  }

  @Test
  public void testFastCopyShellMultiple() throws Exception {
    super.testFastCopyShellMultiple(true, new String[0]);
  }

  @Test
  public void testInterFileSystemFastCopy() throws Exception {
    super.testInterFileSystemFastCopy(true);
  }

  @Test
  public void testInterFileSystemFastCopyMultiple() throws Exception {
    super.testInterFileSystemFastCopyMultiple(true);
  }

  @Test
  public void testInterFileSystemFastCopyShellMultiple() throws Exception {
    testInterFileSystemFastCopyShellMultiple(true, new String[0]);
  }
}
