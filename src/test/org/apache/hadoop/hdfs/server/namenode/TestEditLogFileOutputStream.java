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

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.DU;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.junit.Test;

public class TestEditLogFileOutputStream {

  @Test
  public void testPreallocation() throws IOException {
    Configuration conf = new Configuration();
    FileSystem.setDefaultUri(conf, "hdfs://localhost:0");
    conf.set("dfs.http.address", "127.0.0.1:0");
    conf.setBoolean("dfs.permissions", false);
    NameNode.format(conf);
    NameNode nn = new NameNode(conf);

    File editLog = nn.getFSImage().getEditLog().getFsEditName();

    assertEquals("Edit log should only be 4 bytes long",
        4, editLog.length());

    /**
     * Remove this check for now. Our internal version of DU is
     * different from a regular DU.
     */
    //assertEquals("Edit log disk space used should be one block",
    //    4096, new DU(editLog, conf).getUsed());

    nn.getNamesystem().mkdirs("/test",
      new PermissionStatus("xjin", null, FsPermission.getDefault()));

    assertEquals("Edit log should be 1MB + 4 bytes long",
        (1024 * 1024) + 4, editLog.length());

    /**
     * Remove this check for now. Our internal version of DU is
     * different from a regular DU.
     */
    // 256 blocks for the 1MB of preallocation space, 1 block for the original
    // 4 bytes
    //assertTrue("Edit log disk space used should be at least 257 blocks",
    //    257 * 4096 <= new DU(editLog, conf).getUsed());
  }

}
