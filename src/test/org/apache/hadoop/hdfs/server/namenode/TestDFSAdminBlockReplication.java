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

import junit.framework.TestCase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.tools.DFSAdmin;
import org.apache.hadoop.hdfs.MiniDFSCluster;

/**
 * This class tests DFSAdmin disable/enable Block Replication.
 */
public class TestDFSAdminBlockReplication extends TestCase {
  private MiniDFSCluster cluster;

  /** test block replication */
  public void testBlockReplication() throws Exception {

    Configuration conf = new Configuration();
    cluster = new MiniDFSCluster(conf, 2, true, null);
    DistributedFileSystem dfs = (DistributedFileSystem)cluster.getFileSystem();
    DFSAdmin admin = new DFSAdmin(conf);
    final int fileLen = dfs.getConf().getInt("io.bytes.per.checksum", 512);
    final short REPLICATION_FACTOR = (short)1;
    try {
      {
        // block replication is disabled by DFSAdmin
        admin.run(new String[] { "-blockReplication", "disable"});
        Path path = new Path("testFile");
        DFSTestUtil.createFile(dfs, path, fileLen, REPLICATION_FACTOR, 0);
        DFSTestUtil.waitReplication(dfs, path, REPLICATION_FACTOR);
        dfs.setReplication(path, (short)(REPLICATION_FACTOR + 100));
    		Thread.sleep(ReplicationConfigKeys.replicationRecheckInterval);

        // since block replication is disabled, underReplicatedBlocksCount should be 0
        assertEquals(0, dfs.getUnderReplicatedBlocksCount());

        // enable block replication
        admin.run(new String[] { "-blockReplication", "enable"});
    		Thread.sleep(ReplicationConfigKeys.replicationRecheckInterval);
        
        // since block replication is enabled, underReplicatedBlocksCount should be 1 
        assertEquals(1, dfs.getUnderReplicatedBlocksCount());
      }
    } finally {
      try { dfs.close(); } catch (IOException e) { };
      cluster.shutdown();
    }
  }
}
